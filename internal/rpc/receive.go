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

// ReceiveMessage POP 取件（服务端流）。流格式：先逐条消息，最后一帧 status。
//
// 关于帧顺序（Task 13 已用官方 SDK 实测，不再是悬案）：官方 Go SDK 并不依赖
// 任何顺序——它把整条流读到 io.EOF 收集成一个切片后才统一处理，Status 帧无论
// 出现在头部还是尾部都会被同样地取用。上游 Apache proxy 是 status 在前，sq
// 选择 status 收尾：它同时也是"服务端已经把本批全部发完"的天然信号，而且
// 消息推送中途失败时不会留下一个"已经宣称 OK 却没发全"的流。两种顺序都在
// test/e2e 里实跑验证过（见 task-13 报告），此处的选择不影响 SDK 兼容性。
//
// 空结果不走这条格式：长轮询到期无消息时只发一帧 MESSAGE_NOT_FOUND status，
// 理由见下方该分支的注释。
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
		// deliver.Receive 的错误不是铁板一块，必须按性质分类（同 QueryAssignment
		// 对 EnsureTopic 错误的分类原则）：
		//   - meta.ErrBadName：group 名字非法，是客户端输入错误。折叠成
		//     INTERNAL_SERVER_ERROR 会让轮询消费者把它当成瞬时故障无限重试，
		//     在轮询频率下形成 Error 级别的日志洪水，而这个错误本身永远不会
		//     通过重试变好。
		//   - context.Canceled/DeadlineExceeded：客户端主动断开（关闭消费者、
		//     rebalance）或者内部长轮询循环的 ctx.Done() 分支被触发，都是
		//     正常的流生命周期事件，不是服务端故障，不能打 Error；这种情况下
		//     stream 的底层 ctx 已经失效，不再尝试 Send 一个 status 帧
		//     （大概率也会失败），直接把 ctx 的错误原样返回给 gRPC 框架，
		//     由它按标准方式结束这个流。
		//   - 其余：真正的服务端内部故障，维持原有 INTERNAL_SERVER_ERROR + Error 日志。
		switch {
		case errors.Is(err, meta.ErrBadName):
			s.logger.Warn("ReceiveMessage 拒绝：消费组名字非法", "group", group, "topic", topic, "queue", queueID, "err", err)
			return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
				Status: errStatus(pb.Code_ILLEGAL_CONSUMER_GROUP, err.Error()),
			}})
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			s.logger.Debug("ReceiveMessage 结束：客户端取消或等待超时", "group", group, "topic", topic, "queue", queueID, "err", err)
			return err
		default:
			s.logger.Error("ReceiveMessage 失败", "group", group, "topic", topic, "queue", queueID, "err", err)
			return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
				Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error()),
			}})
		}
	}
	if len(msgs) == 0 {
		// 长轮询到期仍无消息：必须回 MESSAGE_NOT_FOUND（协议里 40401，doc 写
		// "Message not found from server"），不能回 OK + 零条消息。
		//
		// 依据是官方 SDK 的实际代码而非猜测：process_queue.go 的接收回调里有一条
		// 专门以该错误码为条件的分支
		// （isNoNewMessage := isRpcErr && rpcErr.GetCode() == MESSAGE_NOT_FOUND），
		// 命中时只打一条 Debug 并按流控退避重新发起下一次取件；不命中时走的是
		// Errorf("Exception raised during message reception") 这条真·故障路径。
		// 如果服务端用 OK + 零条来表示"没消息"，push 消费者根本不会进入这条分支，
		// 而是把空结果当成一次成功取件、立刻无退避地再发一次——空队列场景下
		// 客户端侧的流控就此失效。SimpleConsumer 两种码都能工作（它把非 OK 状态
		// 翻译成 error 交给调用方循环处理），所以这条不是"能不能跑通"的问题，
		// 而是与协议参考实现保持一致的问题。
		s.logger.Debug("ReceiveMessage 长轮询到期无消息", "group", group, "topic", topic, "queue", queueID, "wait", wait)
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_MESSAGE_NOT_FOUND, "本次长轮询无可投递消息"),
		}})
	}
	for i, m := range msgs {
		if err := stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Message{
			Message: s.toPBMessage(m, group),
		}}); err != nil {
			// 推送到一半流断了（客户端提前关闭/网络问题），前面已经发出的消息
			// 已经生效（inflight 已写），只是本次响应没能完整送达——这属于
			// brief Step 5 要求覆盖的"错误路径带上下文"，之前这里是唯一
			// 没有记录任何信息就直接返回的分支，排查"客户端说没收全"时无从查起。
			s.logger.Warn("ReceiveMessage 推送消息失败，流可能已被客户端关闭",
				"group", group, "topic", topic, "queue", queueID, "index", i, "total", len(msgs), "err", err)
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
// 每条 entry 各自的 Status 反映该条结果；顶层 Status 由 ackAggregateStatus
// 从这些 entry 状态汇总而来，不是固定 OK（见该函数注释：只看顶层 Status 的
// 客户端——常见 SDK 形状——需要靠这个字段判断"这批是否需要重试"）。
func (s *Server) AckMessage(ctx context.Context, req *pb.AckMessageRequest) (*pb.AckMessageResponse, error) {
	entries := make([]*pb.AckMessageResultEntry, 0, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		g, topic, q, off, attempt, err := receiptDecode(e.GetReceiptHandle())
		if err != nil {
			// 非法 handle 是客户端问题（篡改/损坏/过期协议版本），Warn 即可，
			// 不是服务端故障。handle 是客户端可控的任意长度字符串，日志里只留
			// 截断预览（truncateForLog），不把不受信任的输入原样灌进日志。
			s.logger.Warn("ack handle 非法", "handle", truncateForLog(e.GetReceiptHandle()), "err", err)
			entries = append(entries, &pb.AckMessageResultEntry{
				// ReceiptHandle 必须回填：MessageId 在客户端请求里可能为空
				// （proto 字段非必填），此时 ReceiptHandle 是客户端把这条失败
				// 结果关联回自己请求里对应 entry 的唯一线索，遗漏它会让
				// 调用方在 MessageId 为空时无法定位是哪条 ack 失败了。
				Status:        errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error()),
				MessageId:     e.GetMessageId(),
				ReceiptHandle: e.GetReceiptHandle(),
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
	return &pb.AckMessageResponse{Status: ackAggregateStatus(entries), Entries: entries}, nil
}

// ackAggregateStatus 由各 entry 的 Status 汇总出批量响应的顶层 Status：
// 全部条目状态码一致就直接复用那个码；出现不一致就返回
// Code_MULTIPLE_RESULTS（生成协议里 30000 号码，doc 明确写"Generic code
// for multiple return results"——这正是"批量结果不全相同"该用的信号）。
//
// 为什么不能像最初实现那样固定顶层 OK：常见的 SDK 只检查顶层
// resp.GetStatus() 就判定整批是否成功，entry 级别的细节它未必会逐条查看。
// 如果一批里有条目因为 store 内部错误真正失败（INTERNAL_SERVER_ERROR），
// 但顶层仍然报 OK，这类客户端会把失败的那条误判为已确认，永远不会重试，
// 消息因此在事实上被跳过确认却又不会被重投——这是比"多打一个错误码"更
// 严重的静默数据问题。
func ackAggregateStatus(entries []*pb.AckMessageResultEntry) *pb.Status {
	if len(entries) == 0 {
		return okStatus()
	}
	code := entries[0].GetStatus().GetCode()
	for _, e := range entries[1:] {
		if e.GetStatus().GetCode() != code {
			return errStatus(pb.Code_MULTIPLE_RESULTS, "批量确认结果不一致，请检查各 entry 的 status")
		}
	}
	if code == pb.Code_OK {
		return okStatus()
	}
	return errStatus(code, entries[0].GetStatus().GetMessage())
}

// ChangeInvisibleDuration 重设不可见时长，返回原 handle——handle 的内容
// （group/topic/queue/offset/attempt）在改不可见时长后不变（本操作只碰
// InflightState.ExpireAtMs，不碰 Attempts），所以不需要重新编码。
func (s *Server) ChangeInvisibleDuration(ctx context.Context, req *pb.ChangeInvisibleDurationRequest) (*pb.ChangeInvisibleDurationResponse, error) {
	g, topic, q, off, attempt, err := receiptDecode(req.GetReceiptHandle())
	if err != nil {
		s.logger.Warn("改不可见时长 handle 非法", "handle", truncateForLog(req.GetReceiptHandle()), "err", err)
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

// logPreviewLen 日志里展示的 receipt handle 前缀长度上限。
const logPreviewLen = 32

// truncateForLog 截断字符串用于日志展示。receipt handle 是客户端可控的
// 任意长度字符串——一个篡改或异常的客户端可以喂进任意大小的值——错误日志
// 不能把这种不受信任的输入原样、无界地写进去，否则等于把日志系统暴露成
// 一个客户端可控的写入面（日志量放大、潜在的日志注入）。只按字节截断即可：
// handle 是 base64 编码，全部落在 ASCII 范围内，不存在截断到多字节字符
// 中间导致乱码的问题。
func truncateForLog(s string) string {
	if len(s) <= logPreviewLen {
		return s
	}
	return s[:logPreviewLen] + "...(truncated)"
}
