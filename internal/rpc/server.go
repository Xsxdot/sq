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
	"sync"

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

	// done 由 Shutdown 关闭，用于让没有自然终点的长流（Telemetry）主动收尾。
	// 见 Shutdown 的注释：不给它们一个「该结束了」的信号，grpc.Server 的
	// GracefulStop 会永远等下去。
	done     chan struct{}
	doneOnce sync.Once
}

// New 构造协议适配层。pr/dl 由 Task 10（SendMessage/AckMessage 等）、
// Task 11（ReceiveMessage/QueryAssignment）使用；本任务的 4 个 RPC
// 只用到 mt 与 cfg，但构造签名一次定死，后续任务不再改。
func New(cfg *config.Config, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer, logger *slog.Logger) *Server {
	return &Server{
		cfg: cfg, mt: mt, pr: pr, dl: dl, logger: logger.With("mod", "rpc"),
		done: make(chan struct{}),
	}
}

// Shutdown 通知协议适配层「服务端要停机了」，让没有自然终点的长流主动收尾。
// 幂等，可重复调用；不阻塞，也不负责停 gRPC server（那是调用方的事）。
//
// 为什么必须有这么一个信号：Telemetry 是一条双向长流，服务端 handler 阻塞在
// stream.Recv() 上，只有客户端主动断开才会返回；而 grpc.Server.GracefulStop()
// 的语义是「等所有在途 RPC 结束」——只要还有一个客户端进程活着且没有关掉这条
// 流，GracefulStop 就永远不返回。官方 Go SDK 恰好就是这样：它的
// Client.GracefulStop() 只停自己的后台 goroutine，既不 CloseSend 也不关闭
// ClientConn。Task 13 的实测结果是：只连一个 producer 时 sq 停机耗时约 9.5s
// （靠客户端自己的 GOAWAY 恢复逻辑碰巧断开），再加一个 SimpleConsumer 之后
// 就再也停不下来了，只能靠强制中断兜底。
//
// 正确的做法不是把这个"等不到"的时间调长，而是让服务端自己决定长流何时结束：
// 调用方在 GracefulStop 之前先调用本方法，Telemetry handler 会立即返回、
// 流随之关闭，GracefulStop 就只需要等那些真正有终点的在途 RPC 了。
func (s *Server) Shutdown() {
	s.doneOnce.Do(func() {
		close(s.done)
		s.logger.Info("协议适配层进入停机：通知 telemetry 长流收尾")
	})
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

// Telemetry 双向流。M1 职责：收到客户端上报的 Settings 后下发服务端协商结果
// （SDK 启动阶段的握手依赖这个回包才能继续），其余命令（线程栈、消息校验结果等）
// 记日志后忽略。M6 会在此下发事务回查命令。
//
// 回包不是把客户端的 Settings 原样回显，而是经 negotiateSettings 填入服务端
// 权威字段——原样回显会把客户端的消息体上限改写成 0，导致它此后每一次发送都在
// 本地被拒（详见 settings.go 的包内说明与该函数注释）。
//
// 读循环不能直接阻塞在 stream.Recv() 上：Recv 只有收到数据、流出错或底层
// 连接断开才返回，服务端没有任何办法主动打断它。这条流又恰恰是没有自然终点的
// ——客户端可以一直挂着不发也不关——于是停机时 GracefulStop 会永远等它。
// 因此把 Recv 挪到一个独立 goroutine 里，主循环 select 读取结果与 s.done：
// 收到停机信号就直接返回，由 gRPC 关闭流，读 goroutine 的 Recv 随之出错退出。
func (s *Server) Telemetry(stream pb.MessagingService_TelemetryServer) error {
	// 缓冲 1 + readerDone：handler 先返回时，读 goroutine 手上可能正好有一条
	// 没人接收的结果，靠缓冲位或 readerDone 分支退出，不会永久阻塞在发送上。
	//
	// 另一种情形是 handler 返回时读 goroutine 还阻塞在 stream.Recv() 里，这同样
	// 不会泄漏：handler 一返回，gRPC 就会结束并清理这个流，那次 Recv 随即带错误
	// 返回；此时 defer 已经 close(readerDone)，goroutine 走 readerDone 分支直接
	// 退出（就算它抢到了缓冲位，写完也会因为 err != nil 而返回）。既不会空转，
	// 也不会有第二次 Recv。
	//
	// 需要说明的是这条结论的依据：grpc-go 明确文档化的是**并发**约束
	// （同一个流上不得并发调用 RecvMsg），而本函数从头到尾只有这一个 goroutine
	// 调 Recv，不违反它；"handler 返回后仍在途的 Recv 会被唤醒"则属于 grpc-go
	// 的实现行为（v1.83.0 已确认），并非它承诺的 API 契约。之所以可以依赖：
	// 一旦哪天不成立，后果也仅限于每条已关闭的 telemetry 流残留一个 goroutine，
	// 而 Shutdown 本身是进程停机路径——残留 goroutine 随进程一起消失，
	// 不会累积成运行期泄漏。
	type recvResult struct {
		cmd *pb.TelemetryCommand
		err error
	}
	recvCh := make(chan recvResult, 1)
	readerDone := make(chan struct{})
	defer close(readerDone)
	go func() {
		for {
			cmd, err := stream.Recv()
			select {
			case recvCh <- recvResult{cmd: cmd, err: err}:
			case <-readerDone:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-s.done:
			// 服务端停机：主动结束这条长流，让 GracefulStop 能够收敛。
			s.logger.Debug("telemetry 流随服务端停机收尾")
			return nil
		case r := <-recvCh:
			if r.err != nil {
				// 客户端断流是正常生命周期的一部分，Debug 即可，不算错误。
				s.logger.Debug("telemetry 流结束", "err", r.err)
				return nil
			}
			switch c := r.cmd.GetCommand().(type) {
			case *pb.TelemetryCommand_Settings:
				settings := s.negotiateSettings(c.Settings)
				if err := stream.Send(&pb.TelemetryCommand{
					Status:  okStatus(),
					Command: &pb.TelemetryCommand_Settings{Settings: settings},
				}); err != nil {
					return fmt.Errorf("telemetry 下发 settings: %w", err)
				}
				s.logger.Debug("telemetry settings 已协商",
					"client_type", c.Settings.GetClientType(),
					"pub_sub", fmt.Sprintf("%T", settings.GetPubSub()))
			default:
				s.logger.Debug("telemetry 忽略未处理命令", "type", fmt.Sprintf("%T", c))
			}
		}
	}
}

// NotifyClientTermination 客户端优雅下线通知。M1 无会话状态需要清理，确认即可。
func (s *Server) NotifyClientTermination(ctx context.Context, req *pb.NotifyClientTerminationRequest) (*pb.NotifyClientTerminationResponse, error) {
	return &pb.NotifyClientTerminationResponse{Status: okStatus()}, nil
}
