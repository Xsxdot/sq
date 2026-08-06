// ReceiveMessage/AckMessage/ChangeInvisibleDuration/QueryAssignment：
// POP 消费方向的协议翻译（proto ↔ deliver.Deliverer 调用）。
//
// 职责：
//   - ReceiveMessage：服务端流式取件，core.Message → proto Message，
//     并在此注入自包含的 receipt handle（见 receipt.go）
//   - AckMessage/ChangeInvisibleDuration：解出 handle 后转发给
//     deliver.Deliverer，把 (bool,error) 结果映射为协议 Status
//   - QueryAssignment：单机版路由查询，复用 Task 9 的 messageQueues
//
// 边界：
//   - Tag 过滤 M1 仅接受 "*"（真实过滤属 M2），不实现投递次数上限/DLQ
//   - 不直接操作 store/meta 以外的状态，翻译逻辑之外的业务规则全部在
//     deliver 包（本包不重复实现 attempt 校验、inflight 生命周期等）
package rpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// defaultLongPolling 客户端未指定/未受 deadline 限制时的长轮询上限。
const defaultLongPolling = 20 * time.Second

// longPollWaitMargin 从 gRPC 请求 deadline 推导长轮询时长时预留的安全余量：
// 服务端需要留出时间构造响应、写回流，不能把整个 deadline 都花在等待上，
// 否则响应会在 deadline 之后才发出，被客户端判定为超时。
const longPollWaitMargin = time.Second

// ReceiveMessage POP 取件（服务端流）。流格式：先逐条消息，最后一条 status，
// SDK 依赖这个"消息在前、status 收尾"的顺序识别流结束。
func (s *Server) ReceiveMessage(req *pb.ReceiveMessageRequest, stream pb.MessagingService_ReceiveMessageServer) error {
	group := req.GetGroup().GetName()
	mq := req.GetMessageQueue()
	topic := mq.GetTopic().GetName()
	queueID := uint32(mq.GetId())

	if fe := req.GetFilterExpression(); fe != nil &&
		!(fe.GetType() == pb.FilterType_TAG && fe.GetExpression() == "*") {
		s.logger.Warn("ReceiveMessage 拒绝：不支持的过滤表达式",
			"group", group, "topic", topic, "queue", queueID,
			"filter_type", fe.GetType(), "filter_expr", fe.GetExpression())
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_ILLEGAL_FILTER_EXPRESSION, "M1 仅支持 TAG 过滤表达式 *（M2 支持真实过滤）"),
		}})
	}
	invisible := req.GetInvisibleDuration().AsDuration()
	if invisible <= 0 {
		invisible = time.Minute
	}
	batch := int(req.GetBatchSize())
	if batch <= 0 {
		batch = 16
	}
	wait := s.longPollWait(stream.Context())

	msgs, err := s.dl.Receive(stream.Context(), group, topic, queueID, batch, invisible, wait)
	if err != nil {
		s.logger.Error("ReceiveMessage 失败", "group", group, "topic", topic, "queue", queueID, "err", err)
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error()),
		}})
	}
	for _, m := range msgs {
		if err := stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Message{
			Message: s.toPBMessage(m, group),
		}}); err != nil {
			return err
		}
	}
	s.logger.Debug("ReceiveMessage 完成", "group", group, "topic", topic, "queue", queueID, "count", len(msgs))
	return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
		Status: okStatus(),
	}})
}

// longPollWait 由 gRPC 请求 deadline 推导长轮询时长：deadline 减去安全余量，
// 上限为 defaultLongPolling；没有 deadline 时直接用默认值。
//
// 两个边界都要守住（brief 字面代码在此处有缺陷，这里是修正版）：
//   - 余量不能让结果变负：deadline 很近（< longPollWaitMargin）时，
//     remain-margin 会算出负数，必须钳到 0（不等待、取一次就返回），
//     不能让 time.Now().Add(负数) 悄悄退化成"立即过期"以外的语义，
//     也不能因为条件判断疏漏而回退到 defaultLongPolling——那样会导致
//     内部循环仍按 20s 计划轮询，即使 ctx.Done() 最终会打断它，
//     语义上也已经偏离"deadline 附近就不该再等"的本意。
//   - 结果不能超过 deadline：即便刨去余量后剩余时间仍很长，也不能超过
//     defaultLongPolling 这个平台上限。
func (s *Server) longPollWait(ctx context.Context) time.Duration {
	wait := defaultLongPolling
	dl, ok := ctx.Deadline()
	if !ok {
		return wait
	}
	remain := time.Until(dl) - longPollWaitMargin
	if remain < 0 {
		remain = 0
	}
	if remain < wait {
		wait = remain
	}
	return wait
}

// toPBMessage core.Message → proto（读方向翻译，receipt handle 在此注入）。
// handle 编入 m.DeliveryAttempt——deliver.Receive 已经把首投/重投的正确
// attempt 值填好在这个字段上，这里原样取用即可，无需额外传参。
func (s *Server) toPBMessage(m *core.Message, group string) *pb.Message {
	handle := receiptEncode(group, m.Topic, m.QueueID, m.Offset, m.DeliveryAttempt)
	attempt := m.DeliveryAttempt
	offset := int64(m.Offset)
	sp := &pb.SystemProperties{
		MessageId:       m.ID,
		MessageType:     pb.MessageType_NORMAL,
		Keys:            m.Keys,
		BornTimestamp:   timestamppb.New(time.UnixMilli(m.BornAtMs)),
		StoreTimestamp:  timestamppb.New(time.UnixMilli(m.StoreAtMs)),
		DeliveryAttempt: &attempt,
		ReceiptHandle:   &handle,
		QueueId:         int32(m.QueueID),
		QueueOffset:     &offset, // *int64（不是 *int32），见 task-8-report 的字段核对
	}
	if m.Tag != "" {
		tag := m.Tag
		sp.Tag = &tag
	}
	if m.MessageGroup != "" {
		mg := m.MessageGroup
		sp.MessageGroup = &mg
	}
	return &pb.Message{
		Topic:            &pb.Resource{Name: m.Topic},
		SystemProperties: sp,
		UserProperties:   m.Properties,
		Body:             m.Body,
	}
}

// AckMessage 批量确认。逐条独立处理：handle 解析失败或 ack 落空都不影响其它条目，
// 由每条 entry 各自的 Status 反映结果（整体 Status 固定 OK——协议约定，
// "批量部分失败"通过 entry 级别表达，不是整体失败）。
func (s *Server) AckMessage(ctx context.Context, req *pb.AckMessageRequest) (*pb.AckMessageResponse, error) {
	entries := make([]*pb.AckMessageResultEntry, 0, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		g, topic, q, off, attempt, err := receiptDecode(e.GetReceiptHandle())
		if err != nil {
			// 非法 handle 是客户端问题（篡改/损坏/过期协议版本），Warn 即可，
			// 不是服务端故障。
			s.logger.Warn("ack handle 非法", "handle", e.GetReceiptHandle(), "err", err)
			entries = append(entries, &pb.AckMessageResultEntry{
				Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error()), MessageId: e.GetMessageId(),
			})
			continue
		}
		ok, err := s.dl.Ack(g, topic, q, off, attempt)
		st := okStatus()
		if err != nil {
			s.logger.Error("ack 失败", "group", g, "topic", topic, "queue", q, "offset", off, "attempt", attempt, "err", err)
			st = errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())
		} else if !ok {
			// ack 落空的三种情况——已被 ack 过、已被过期重投覆盖、或本来就是携带
			// 陈旧 attempt 的句柄——deliver.Ack 内部已经把它们统一归约成
			// (false, nil)。这里再往上翻译成协议码时同样不细分：RocketMQ SDK
			// 收到 INVALID_RECEIPT_HANDLE 就是静默丢弃、不重试，这正是我们想要
			// 的行为（重试一个已经不对应任何有效 inflight 记录的 ack 没有意义），
			// 不是需要在 Error 级别报警的服务端故障。
			st = errStatus(pb.Code_INVALID_RECEIPT_HANDLE, "receipt 已失效")
		}
		entries = append(entries, &pb.AckMessageResultEntry{Status: st, MessageId: e.GetMessageId(), ReceiptHandle: e.GetReceiptHandle()})
	}
	return &pb.AckMessageResponse{Status: okStatus(), Entries: entries}, nil
}

// ChangeInvisibleDuration 重设不可见时长，返回原 handle——handle 的内容
// （group/topic/queue/offset/attempt）在改不可见时长后不变（本操作只碰
// InflightState.ExpireAtMs，不碰 Attempts），所以不需要重新编码。
func (s *Server) ChangeInvisibleDuration(ctx context.Context, req *pb.ChangeInvisibleDurationRequest) (*pb.ChangeInvisibleDurationResponse, error) {
	g, topic, q, off, attempt, err := receiptDecode(req.GetReceiptHandle())
	if err != nil {
		s.logger.Warn("改不可见时长 handle 非法", "handle", req.GetReceiptHandle(), "err", err)
		return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error())}, nil
	}
	ok, err := s.dl.ChangeInvisible(g, topic, q, off, attempt, req.GetInvisibleDuration().AsDuration())
	if err != nil {
		s.logger.Error("改不可见时长失败", "group", g, "topic", topic, "queue", q, "offset", off, "attempt", attempt, "err", err)
		return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
	}
	if !ok {
		// 同 AckMessage：落空（已 ack/已重投/陈旧 attempt）是正常的幂等分支，不是错误。
		return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, "receipt 已失效")}, nil
	}
	return &pb.ChangeInvisibleDurationResponse{Status: okStatus(), ReceiptHandle: req.GetReceiptHandle()}, nil
}

// QueryAssignment 单机版：全部队列归属本节点，直接复用 messageQueues。
//
// EnsureTopic 失败原因分类与 QueryRoute 同理（见 server.go 的 QueryRoute
// 注释）：名字非法（ErrBadName）与 topic 不存在且未自动创建（ErrTopicNotFound）
// 是两种不同性质的客户端可处理错误，不能都折叠成 TOPIC_NOT_FOUND；
// 两者之外的失败是服务端内部故障，必须用 Error 级别单独记录，不能让客户端
// 把一个本该重试的瞬时错误误判成"topic 真的不存在"。
func (s *Server) QueryAssignment(ctx context.Context, req *pb.QueryAssignmentRequest) (*pb.QueryAssignmentResponse, error) {
	name := req.GetTopic().GetName()
	tc, err := s.mt.EnsureTopic(name)
	if err != nil {
		switch {
		case errors.Is(err, meta.ErrBadName):
			s.logger.Warn("QueryAssignment 失败：topic 名字非法", "topic", name, "err", err)
			return &pb.QueryAssignmentResponse{Status: errStatus(pb.Code_ILLEGAL_TOPIC, err.Error())}, nil
		case errors.Is(err, meta.ErrTopicNotFound):
			s.logger.Warn("QueryAssignment 失败：topic 不存在", "topic", name, "err", err)
			return &pb.QueryAssignmentResponse{Status: errStatus(pb.Code_TOPIC_NOT_FOUND, err.Error())}, nil
		default:
			s.logger.Error("QueryAssignment 内部错误", "topic", name, "err", err)
			return &pb.QueryAssignmentResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
		}
	}
	qs := s.messageQueues(tc, req.GetTopic())
	asgs := make([]*pb.Assignment, 0, len(qs))
	for _, mq := range qs {
		asgs = append(asgs, &pb.Assignment{MessageQueue: mq})
	}
	s.logger.Debug("QueryAssignment", "topic", name, "group", req.GetGroup().GetName(), "assignments", len(asgs))
	return &pb.QueryAssignmentResponse{Status: okStatus(), Assignments: asgs}, nil
}
