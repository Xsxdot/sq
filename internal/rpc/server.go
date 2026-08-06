// Package rpc 实现 RocketMQ 5.x gRPC 协议适配层（spec 的 rocketmq-adapter）。
//
// 职责：
//   - 实现 pb.MessagingServiceServer，把 proto 语义翻译成 core 调用
//   - Telemetry Settings 协商与（M6）事务回查命令下发
//
// 边界：
//   - 不含任何消息语义逻辑（core 的事）；core 反向不感知本包
//   - M1 未覆盖的 RPC 一律返回 UNIMPLEMENTED（由内嵌的
//     pb.UnimplementedMessagingServiceServer 兜底，本包不逐个写桩）
package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

const brokerName = "sq0" // 单机版固定 broker 名；v2 集群时改为节点标识

// Server 协议适配层，实现 pb.MessagingServiceServer。
// 内嵌 UnimplementedMessagingServiceServer：M1 未实现的 RPC（PullMessage、
// EndTransaction 等 17 个方法中未在本包显式覆盖的部分）自动返回 UNIMPLEMENTED，
// 同时满足 gRPC "must embed Unimplemented...Server" 的前向兼容惯例。
type Server struct {
	pb.UnimplementedMessagingServiceServer
	cfg    *config.Config
	mt     *meta.Meta
	pr     *produce.Producer
	dl     *deliver.Deliverer
	logger *slog.Logger
}

// New 构造协议适配层。pr/dl 由 Task 10（SendMessage/AckMessage 等）、
// Task 11（ReceiveMessage/QueryAssignment）使用；本任务的 4 个 RPC
// 只用到 mt 与 cfg，但构造签名一次定死，后续任务不再改。
func New(cfg *config.Config, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer, logger *slog.Logger) *Server {
	return &Server{cfg: cfg, mt: mt, pr: pr, dl: dl, logger: logger.With("mod", "rpc")}
}

// Register 把 Server 挂载到 gRPC server 上。
func (s *Server) Register(gs *grpc.Server) { pb.RegisterMessagingServiceServer(gs, s) }

// okStatus 构造成功状态。
func okStatus() *pb.Status { return &pb.Status{Code: pb.Code_OK, Message: "ok"} }

// errStatus 按错误码与信息构造失败状态。
func errStatus(code pb.Code, msg string) *pb.Status { return &pb.Status{Code: code, Message: msg} }

// endpoints 返回对外通告地址（QueryRoute/QueryAssignment 共用）。
func (s *Server) endpoints() *pb.Endpoints {
	return &pb.Endpoints{
		Scheme: pb.AddressScheme_IPv4,
		Addresses: []*pb.Address{{
			Host: s.cfg.AdvertiseHost,
			Port: int32(s.cfg.AdvertisePort),
		}},
	}
}

// messageQueues 把 topic 配置展开为路由队列表。
func (s *Server) messageQueues(tc meta.TopicConfig, topic *pb.Resource) []*pb.MessageQueue {
	qs := make([]*pb.MessageQueue, 0, tc.Queues)
	for i := uint32(0); i < tc.Queues; i++ {
		qs = append(qs, &pb.MessageQueue{
			Topic:      topic,
			Id:         int32(i),
			Permission: pb.Permission_READ_WRITE,
			Broker:     &pb.Broker{Name: brokerName, Id: 0, Endpoints: s.endpoints()},
			AcceptMessageTypes: []pb.MessageType{
				pb.MessageType_NORMAL, // M3/M4/M6 时追加 DELAY/FIFO/TRANSACTION
			},
		})
	}
	return qs
}

// QueryRoute 返回 topic 路由。autoCreate 开启时未知 topic 在此自动创建——
// 生产者发送消息前必须先查路由，若这里不建，topic 永远没有机会被创建出来。
//
// EnsureTopic 的失败原因需要按性质分类，不能全部折叠成一个 Code：
// 名字本身不合法（ErrBadName，客户端输入错误）与 topic 未注册且未开自动创建
// （ErrTopicNotFound，同样是客户端可自行处理的情况）分属不同语义；两者之外
// 的失败（如自动创建时的 store 持久化错误）是服务端内部故障，绝不能报成
// 「你的输入非法」——那会让一个本该重试的瞬时错误被客户端当成永久性错误放弃。
func (s *Server) QueryRoute(ctx context.Context, req *pb.QueryRouteRequest) (*pb.QueryRouteResponse, error) {
	name := req.GetTopic().GetName()
	tc, err := s.mt.EnsureTopic(name)
	if err != nil {
		switch {
		case errors.Is(err, meta.ErrBadName):
			s.logger.Warn("QueryRoute 失败：topic 名字非法", "topic", name, "err", err)
			return &pb.QueryRouteResponse{Status: errStatus(pb.Code_ILLEGAL_TOPIC, err.Error())}, nil
		case errors.Is(err, meta.ErrTopicNotFound):
			s.logger.Warn("QueryRoute 失败：topic 不存在", "topic", name, "err", err)
			return &pb.QueryRouteResponse{Status: errStatus(pb.Code_TOPIC_NOT_FOUND, err.Error())}, nil
		default:
			// 已排除掉两类客户端输入错误，剩下的都是服务端内部故障
			// （如自动创建 topic 时的 store 持久化失败），用 Error 级别记录。
			s.logger.Error("QueryRoute 内部错误", "topic", name, "err", err)
			return &pb.QueryRouteResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
		}
	}
	s.logger.Debug("QueryRoute", "topic", name, "queues", tc.Queues)
	return &pb.QueryRouteResponse{Status: okStatus(), MessageQueues: s.messageQueues(tc, req.GetTopic())}, nil
}

// Heartbeat 客户端保活；携带消费组名时顺带注册该组（消费组首次出现即注册，
// 与 topic 的 autoCreate 开关无关）。
//
// 与 QueryRoute 同理：EnsureGroup 失败要按性质分类。ErrBadName（名字非法）
// 才是真正的「ILLEGAL_CONSUMER_GROUP」（该 Code 本身定义为 "Format of
// consumer group is illegal"）；其余失败（如注册时的 store 持久化错误）
// 是服务端内部故障，报成客户端输入错误会误导客户端停止本该重试的请求。
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if g := req.GetGroup().GetName(); g != "" {
		if _, err := s.mt.EnsureGroup(g); err != nil {
			if errors.Is(err, meta.ErrBadName) {
				s.logger.Warn("Heartbeat 注册消费组失败：名字非法", "group", g, "err", err)
				return &pb.HeartbeatResponse{Status: errStatus(pb.Code_ILLEGAL_CONSUMER_GROUP, err.Error())}, nil
			}
			s.logger.Error("Heartbeat 注册消费组内部错误", "group", g, "err", err)
			return &pb.HeartbeatResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
		}
	}
	return &pb.HeartbeatResponse{Status: okStatus()}, nil
}

// Telemetry 双向流。M1 职责：收到 Settings 即原样回发（SDK 启动阶段的握手依赖
// 这个回包才能继续），其余命令（线程栈、消息校验结果等）记日志后忽略。
// M6 会在此下发事务回查命令。
func (s *Server) Telemetry(stream pb.MessagingService_TelemetryServer) error {
	for {
		cmd, err := stream.Recv()
		if err != nil {
			// 客户端断流是正常生命周期的一部分，Debug 即可，不算错误。
			s.logger.Debug("telemetry 流结束", "err", err)
			return nil
		}
		switch c := cmd.GetCommand().(type) {
		case *pb.TelemetryCommand_Settings:
			st := c.Settings
			if err := stream.Send(&pb.TelemetryCommand{
				Status:  okStatus(),
				Command: &pb.TelemetryCommand_Settings{Settings: st},
			}); err != nil {
				return fmt.Errorf("telemetry 回发 settings: %w", err)
			}
			s.logger.Debug("telemetry settings 已协商")
		default:
			s.logger.Debug("telemetry 忽略未处理命令", "type", fmt.Sprintf("%T", c))
		}
	}
}

// NotifyClientTermination 客户端优雅下线通知。M1 无会话状态需要清理，确认即可。
func (s *Server) NotifyClientTermination(ctx context.Context, req *pb.NotifyClientTerminationRequest) (*pb.NotifyClientTerminationResponse, error) {
	return &pb.NotifyClientTerminationResponse{Status: okStatus()}, nil
}
