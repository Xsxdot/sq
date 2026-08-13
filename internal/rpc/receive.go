// ReceiveMessage/AckMessage/ChangeInvisibleDuration/QueryAssignment：
// POP 消费方向的协议翻译（proto ↔ deliver.Deliverer 调用）。
//
// 职责：
//   - ReceiveMessage：服务端流式取件，core.Message → proto Message，
//     并在此注入自包含的 receipt handle（见 receipt.go）
//   - AckMessage/ChangeInvisibleDuration：解出 handle 后转发给
//     deliver.Deliverer，把 (bool,error) 结果映射为协议 Status
//   - QueryAssignment：路由查询，复用 QueryRoute 的 messageQueues（集群档
//     经 RouteView 把队列指向各 leader，行为与 QueryRoute 一致）
//
// 边界：
//   - Tag 过滤支持 "*" / 单 tag / "a || b"，SQL92 属性过滤计划 v1.1
//   - 不直接操作 store/meta 以外的状态，翻译逻辑之外的业务规则全部在
//     deliver 包（本包不重复实现 attempt 校验、inflight 生命周期等）
package rpc

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/replication"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// defaultLongPolling 客户端未指定/未受 deadline 限制时的长轮询上限。
const defaultLongPolling = 20 * time.Second

// longPollWaitMargin 从 gRPC 请求 deadline 推导长轮询时长时预留的安全余量：
// 服务端需要留出时间构造响应、写回流，不能把整个 deadline 都花在等待上，
// 否则响应会在 deadline 之后才发出，被客户端判定为超时。
const longPollWaitMargin = time.Second

// filterKindOf 把协议过滤类型映射为 deliver 的种类。
// 第二个返回值为 false 表示本版本不支持该类型，调用方须回
// ILLEGAL_FILTER_EXPRESSION——不能默默当成不过滤，那会让本应被筛掉的
// 消息全量投给消费者。
func filterKindOf(t pb.FilterType) (deliver.FilterKind, bool) {
	switch t {
	case pb.FilterType_TAG:
		return deliver.FilterTag, true
	case pb.FilterType_SQL:
		return deliver.FilterSQL92, true
	default:
		return 0, false
	}
}

// ReceiveMessage POP 取件（服务端流）。流格式：先逐条消息，最后一帧 status。
//
// 关于帧顺序（已用官方 SDK 实测，不是推测）：官方 Go SDK 并不依赖任何顺序——
// 它把整条流读到 io.EOF 收集成一个切片后才统一处理，Status 帧无论出现在头部
// 还是尾部都会被同样地取用。上游 Apache proxy 是 status 在前，sq 选择 status
// 收尾：它同时也是"服务端已经把本批全部发完"的天然信号，而且消息推送中途失败
// 时不会留下一个"已经宣称 OK 却没发全"的流。两种顺序都在 test/e2e 里用真实
// SDK 实跑验证过，此处的选择不影响 SDK 兼容性。
//
// 空结果不走这条格式：长轮询到期无消息时只发一帧 MESSAGE_NOT_FOUND status，
// 理由见下方该分支的注释。
func (s *Server) ReceiveMessage(req *pb.ReceiveMessageRequest, stream pb.MessagingService_ReceiveMessageServer) error {
	group := req.GetGroup().GetName()
	mq := req.GetMessageQueue()
	topic := mq.GetTopic().GetName()
	queueID := uint32(mq.GetId())

	// 入口快速失败：本节点不是该队列 leader 时，长轮询没有任何意义——
	// follower 上 deliver.Receive 会照常等待，20s 后回 MESSAGE_NOT_FOUND，
	// 消费者停在一条死路由上毫无线索（leader 已迁走的场景），直到 rebalance
	// 或人工干预。HA_NOT_AVAILABLE（选码论证见 QueryRoute 的分支注释）让
	// SDK 立即换节点重试。
	if !s.rv.SelfIsLeader(topic, queueID) {
		s.logger.Debug("ReceiveMessage 快速失败：本节点非该队列 leader", "group", group, "topic", topic, "queue", queueID)
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_HA_NOT_AVAILABLE, "本节点不是该队列当前 leader，请向 leader 节点重试"),
		}})
	}

	// 过滤表达式解析：协议枚举在此映射为 deliver 自己的种类
	// （core 不 import pb 是既有架构约束）。未带 FilterExpression 时用
	// AllPass，不传 nil——deliver 侧不接受 nil Filter。
	filter := deliver.AllPass
	if fe := req.GetFilterExpression(); fe != nil {
		kind, ok := filterKindOf(fe.GetType())
		if !ok {
			s.logger.Warn("不支持的过滤类型", "group", group, "topic", topic, "type", fe.GetType())
			return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
				Status: errStatus(pb.Code_ILLEGAL_FILTER_EXPRESSION,
					fmt.Sprintf("不支持的过滤类型 %v", fe.GetType())),
			}})
		}
		f, err := deliver.ParseFilter(kind, fe.GetExpression())
		if err != nil {
			s.logger.Warn("过滤表达式非法", "group", group, "topic", topic,
				"kind", fe.GetType(), "expr", fe.GetExpression(), "err", err)
			return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
				Status: errStatus(pb.Code_ILLEGAL_FILTER_EXPRESSION, err.Error()),
			}})
		}
		filter = f
	}
	// topic 存在性与队列边界必须在进入 deliver 前挡住（spec 鉴权收尾 §5）。
	// 用只读 GetTopic 而非 EnsureTopic：消费动作不应创建 topic。
	// 为什么这里可以硬拒而不影响正常 SDK：SDK 总是先 QueryRoute（那里在
	// auto_create 开启时会建 topic）再 Receive，走到这里 topic 必已存在；
	// 能命中该分支的是绕过路由的手写客户端或已被删除的 topic。
	tc, ok := s.mt.GetTopic(topic)
	if !ok {
		s.logger.Warn("ReceiveMessage 拒绝：topic 不存在", "group", group, "topic", topic, "queue", queueID)
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_TOPIC_NOT_FOUND, fmt.Sprintf("topic %q 不存在", topic)),
		}})
	}
	if queueID >= tc.Queues {
		s.logger.Warn("ReceiveMessage 拒绝：queueID 越界", "group", group, "topic", topic, "queue", queueID, "queues", tc.Queues)
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_BAD_REQUEST, fmt.Sprintf("queueID %d 越界（topic %q 共 %d 队列）", queueID, topic, tc.Queues)),
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

	msgs, err := s.dl.Receive(stream.Context(), group, topic, queueID, batch, invisible, wait, filter)
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
		//   - replication.ErrNotLeader：长轮询期间领导权迁移（入口探针放行
		//     后 leader 换手，EnsureGroup/receiveOnce 的提案被拒）——「问错
		//     节点」不是服务端故障，映射 HA_NOT_AVAILABLE 让 SDK 换节点重试
		//     （选码论证同 QueryRoute 分支注释）。Debug 级别：迁移窗口内
		//     会被高频触发。
		//   - 其余：真正的服务端内部故障，维持原有 INTERNAL_SERVER_ERROR + Error 日志。
		switch {
		case errors.Is(err, meta.ErrBadName):
			s.logger.Warn("ReceiveMessage 拒绝：消费组名字非法", "group", group, "topic", topic, "queue", queueID, "err", err)
			return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
				Status: errStatus(pb.Code_ILLEGAL_CONSUMER_GROUP, err.Error()),
			}})
		case errors.Is(err, replication.ErrNotLeader):
			s.logger.Debug("ReceiveMessage 失败：本节点非该组 leader（长轮询期间领导权迁移）", "group", group, "topic", topic, "queue", queueID, "err", err)
			return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
				Status: errStatus(pb.Code_HA_NOT_AVAILABLE, err.Error()),
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
			Message: s.toPBMessage(m, group, invisible),
		}}); err != nil {
			// 推送到一半流断了（客户端提前关闭/网络问题），前面已经发出的消息
			// 已经生效（inflight 已写），只是本次响应没能完整送达。这条日志必须
			// 带上 index/total：没有它，排查"客户端说没收全"时只知道流断了，
			// 不知道断在第几条，也就无从判断有多少条消息会靠不可见超时重投回来。
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
// 两个边界都要守住：
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

// crc32Checksum 按官方 SDK 的校验式计算消息体 CRC32 校验和：无前导零的大写
// 十六进制。格式必须与 SDK v5.1.4 message.go 的
// `strings.ToUpper(strconv.FormatInt(int64(crc32.ChecksumIEEE(body)), 16))`
// 逐字符一致——差一个前导零或大小写，消费端就会把消息标记为 corrupted。
func crc32Checksum(body []byte) string {
	return strings.ToUpper(strconv.FormatInt(int64(crc32.ChecksumIEEE(body)), 16))
}

// toPBMessage core.Message → proto（读方向翻译，receipt handle 在此注入）。
// handle 编入 m.DeliveryAttempt——deliver.Receive 已经把首投/重投的正确
// attempt 值填好在这个字段上，这里原样取用即可，无需额外传参。
//
// 本函数必须回填 toCoreMessage 收下的每一个透传字段：写方向存了、读方向不发，
// 效果与压根没存一样，只是更难发现（盘上数据看着是对的）。改动其中一侧时
// 请同时改另一侧，两者的类型映射集中在 sysprops.go。
// DeliverAtMs 也在此回填（类型 + DeliveryTimestamp 两个字段）；MessageGroup 非空时类型回填 FIFO（M4）。
//
// 透传之上有两条"缺失兜底"（只在生产者什么都没声明时生效，不覆盖已有值）：
// digest 缺失补算 CRC32、encoding 未声明归一化 IDENTITY，理由见各自的行内注释。
func (s *Server) toPBMessage(m *core.Message, group string, invisible time.Duration) *pb.Message {
	handle := receiptEncode(s.handleSecret, group, m.Topic, m.QueueID, m.Offset, m.DeliveryAttempt)
	attempt := m.DeliveryAttempt
	offset := int64(m.Offset)
	// 重投消息（attempt>=2）的实际不可见时长是 max(客户端要求, 退避下限)：
	// receiveOnce 的过期语义是 exp=now+max(invisible,backoff)（deliver.go），
	// 这里用同一公式（deliver.RetryBackoff）回填，否则 SDK 依 InvisibleDuration
	// 换算出的可见时间点会早于服务端实际。首投无退避概念，保持客户端值。
	eff := invisible
	// 顺序消息除外（M4）：deliver 对顺序重投不设退避下限（卡队头要的是快速
	// 原地重投，spec §5 流程 6 的退避仅限非顺序），这里的回填判据必须与
	// deliver 侧（receiveOnce 的 !r.ordered）保持一致，否则 SDK 依
	// InvisibleDuration 换算的可见时间点晚于服务端实际。
	if m.DeliveryAttempt >= 2 && m.MessageGroup == "" {
		if bo := deliver.RetryBackoff(m.DeliveryAttempt); bo > eff {
			eff = bo
		}
	}
	// 盘上的 token 只可能来自 bodyEncodingToCore，正常不会认不出来；真认不出来
	// 说明数据被外部改写或两个方向被改得不再对称，宁可发一个"未声明"也要留日志。
	enc, known := bodyEncodingToPB(m.BodyEncoding)
	if !known {
		s.logger.Warn("投递消息时遇到无法映射回协议的 body_encoding，按未声明下发",
			"topic", m.Topic, "queue", m.QueueID, "offset", m.Offset,
			"msg_id", m.ID, "body_encoding", string(m.BodyEncoding))
	}
	// 生产者未声明编码时归一化为 IDENTITY 下发：sq 存的就是原始字节，IDENTITY
	// 是这个事实的如实陈述；而下发 UNSPECIFIED 会让官方 SDK 消费端每条消息打一条
	// "unsupported message encoding algorithm" ERROR（message.go 的 default 分支）。
	// 判据必须是 m.BodyEncoding（盘上真的没声明），不能是 enc==UNSPECIFIED：
	// 后者会把上面 !known 分支（盘上存着不认识的 token，Body 可能是某种未知
	// 压缩格式）也一并伪装成 IDENTITY，从"如实说不知道"变成"主动说谎"。
	if m.BodyEncoding == core.BodyEncodingUnspecified {
		enc = pb.Encoding_IDENTITY
	}
	// 生产者没带 digest 时（官方 Go SDK v5.1.4 的生产者就从不设置它）补算 CRC32：
	// 消费端对 digest 类型 UNSPECIFIED 的每条消息都会刷一条 WARN，补算之后消费端
	// 校验通过、日志干净。这不与"digest 是端到端的事、sq 不重算"（sysprops.go）
	// 矛盾：生产者声明过的 digest 依旧原样透传、绝不覆盖——兜底只在端到端链路
	// 本来就是空白的时候补一段服务端到消费端的完整性校验，聊胜于无。
	digest := digestToPB(m.BodyDigest)
	if digest == nil {
		digest = &pb.Digest{Type: pb.DigestType_CRC32, Checksum: crc32Checksum(m.Body)}
	}
	// 投递时如实回填消息类型：延时看 DeliverAtMs、顺序看 MessageGroup。
	// 写方向已拒绝两者组合，这里的优先级只为对脏数据保持确定性；
	// DLQ 消息的 MessageGroup 在 moveToDLQ 时已清空，回 NORMAL，符合
	// "死信不再参与顺序"的语义。
	mtype := pb.MessageType_NORMAL
	switch {
	case m.DeliverAtMs > 0:
		mtype = pb.MessageType_DELAY
	case m.MessageGroup != "":
		mtype = pb.MessageType_FIFO
	}
	sp := &pb.SystemProperties{
		MessageId:         m.ID,
		MessageType:       mtype,
		Keys:              m.Keys,
		BodyEncoding:      enc,
		BodyDigest:        digest,
		BornTimestamp:     timestamppb.New(time.UnixMilli(m.BornAtMs)),
		BornHost:          m.BornHost,
		StoreTimestamp:    timestamppb.New(time.UnixMilli(m.StoreAtMs)),
		DeliveryAttempt:   &attempt,
		ReceiptHandle:     &handle,
		QueueId:           int32(m.QueueID),
		InvisibleDuration: durationpb.New(eff),
		// QueueOffset 在生成代码里是 *int64（不是 *int32，也不是值类型），
		// 上面的 offset 局部变量就是为了取地址而存在的。
		QueueOffset: &offset,
	}
	if m.Tag != "" {
		tag := m.Tag
		sp.Tag = &tag
	}
	if m.DeliverAtMs > 0 {
		sp.DeliveryTimestamp = timestamppb.New(time.UnixMilli(m.DeliverAtMs))
	}
	if m.MessageGroup != "" {
		mg := m.MessageGroup
		sp.MessageGroup = &mg
	}
	if m.TraceContext != "" {
		tc := m.TraceContext
		sp.TraceContext = &tc
	}
	return &pb.Message{
		Topic:            &pb.Resource{Name: m.Topic},
		SystemProperties: sp,
		UserProperties:   m.Properties,
		Body:             m.Body,
	}
}

// AckMessage 批量确认。两遍处理：第一遍逐条解码 handle（失败的当场生成
// per-entry 失败结果，不影响其它条目）；第二遍把解码成功的按
// (group,topic,queue) 分组，每组一次 deliver.AckBatch——同队列多条合成
// 单个 Pebble Batch 一次 fsync（官方 SDK 批量 ack 即刻受益）。
//
// per-entry 语义与逐条时代完全一致：落空（已 ack/已重投/陈旧 attempt）
// 翻译成 INVALID_RECEIPT_HANDLE（SDK 收到即静默丢弃、不重试，正是想要的）；
// 存储故障时该组全部 INTERNAL_SERVER_ERROR。响应 entries 严格保持请求顺序。
// 顶层 Status 仍由 ackAggregateStatus 聚合（规则与理由见该函数注释）。
func (s *Server) AckMessage(ctx context.Context, req *pb.AckMessageRequest) (*pb.AckMessageResponse, error) {
	type ackSlot struct {
		idx     int
		offset  uint64
		attempt int32
		e       *pb.AckMessageEntry
	}
	type ackKey struct {
		group string
		topic string
		q     uint32
	}
	entries := make([]*pb.AckMessageResultEntry, len(req.GetEntries()))
	groups := make(map[ackKey][]ackSlot)
	var order []ackKey // 按首次出现序处理各组，行为确定可测
	for i, e := range req.GetEntries() {
		g, topic, q, off, attempt, err := receiptDecode(s.handleSecret, e.GetReceiptHandle())
		if err != nil {
			// 非法 handle 是客户端问题（篡改/损坏/过期协议版本），Warn 即可，
			// 不是服务端故障。handle 是客户端可控的任意长度字符串，日志里只留
			// 截断预览（truncateForLog），不把不受信任的输入原样灌进日志。
			s.logger.Warn("ack handle 非法", "handle", truncateForLog(e.GetReceiptHandle()), "err", err)
			entries[i] = &pb.AckMessageResultEntry{
				// ReceiptHandle 必须回填：MessageId 在客户端请求里可能为空
				// （proto 字段非必填），此时 ReceiptHandle 是客户端把这条失败
				// 结果关联回自己请求里对应 entry 的唯一线索。
				Status:        errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error()),
				MessageId:     e.GetMessageId(),
				ReceiptHandle: e.GetReceiptHandle(),
			}
			continue
		}
		k := ackKey{group: g, topic: topic, q: q}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], ackSlot{idx: i, offset: off, attempt: attempt, e: e})
	}
	for _, k := range order {
		slots := groups[k]
		acks := make([]deliver.AckEntry, len(slots))
		for j, sl := range slots {
			acks[j] = deliver.AckEntry{Offset: sl.offset, Attempt: sl.attempt}
		}
		results, err := s.dl.AckBatch(ctx, k.group, k.topic, k.q, acks)
		if err != nil {
			// ErrNotLeader（本节点不再是该组 leader，如 leader 刚迁移）与
			// 存储故障性质不同：前者是「问错节点」，映射 HA_NOT_AVAILABLE
			// 让 SDK 换节点重试（选码论证见 QueryRoute 分支注释）；后者才是
			// 真·服务端故障。Debug 级别：leader 迁移窗口内会被高频触发。
			if errors.Is(err, replication.ErrNotLeader) {
				s.logger.Debug("批量 ack 失败：本节点非该组 leader", "group", k.group, "topic", k.topic,
					"queue", k.q, "count", len(slots), "err", err)
				for _, sl := range slots {
					entries[sl.idx] = &pb.AckMessageResultEntry{
						Status:        errStatus(pb.Code_HA_NOT_AVAILABLE, err.Error()),
						MessageId:     sl.e.GetMessageId(),
						ReceiptHandle: sl.e.GetReceiptHandle(),
					}
				}
				continue
			}
			// 存储故障：该组整体失败（AckBatch 单 Batch 原子，不存在部分生效），
			// 客户端对这些 entry 重试即可
			s.logger.Error("批量 ack 失败", "group", k.group, "topic", k.topic,
				"queue", k.q, "count", len(slots), "err", err)
			for _, sl := range slots {
				entries[sl.idx] = &pb.AckMessageResultEntry{
					Status:        errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error()),
					MessageId:     sl.e.GetMessageId(),
					ReceiptHandle: sl.e.GetReceiptHandle(),
				}
			}
			continue
		}
		for j, sl := range slots {
			st := okStatus()
			if !results[j].OK {
				// 落空的三种情况已在 deliver 层归约成 OK=false，翻译规则与
				// 逐条时代一致（理由见旧实现注释：SDK 收到该码即静默丢弃）
				st = errStatus(pb.Code_INVALID_RECEIPT_HANDLE, "receipt 已失效")
			}
			entries[sl.idx] = &pb.AckMessageResultEntry{
				Status: st, MessageId: sl.e.GetMessageId(), ReceiptHandle: sl.e.GetReceiptHandle(),
			}
		}
	}
	s.logger.Debug("AckMessage 处理完成", "entries", len(entries), "groups", len(order))
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
	g, topic, q, off, attempt, err := receiptDecode(s.handleSecret, req.GetReceiptHandle())
	if err != nil {
		s.logger.Warn("改不可见时长 handle 非法", "handle", truncateForLog(req.GetReceiptHandle()), "err", err)
		return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error())}, nil
	}
	ok, err := s.dl.ChangeInvisible(ctx, g, topic, q, off, attempt, req.GetInvisibleDuration().AsDuration())
	if err != nil {
		// ErrNotLeader 与存储故障分性质映射，理由同 AckMessage 的批量失败分支
		if errors.Is(err, replication.ErrNotLeader) {
			s.logger.Debug("改不可见时长失败：本节点非该组 leader", "group", g, "topic", topic,
				"queue", q, "offset", off, "attempt", attempt, "err", err)
			return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_HA_NOT_AVAILABLE, err.Error())}, nil
		}
		s.logger.Error("改不可见时长失败", "group", g, "topic", topic, "queue", q, "offset", off, "attempt", attempt, "err", err)
		return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
	}
	if !ok {
		// 同 AckMessage：落空（已 ack/已重投/陈旧 attempt）是正常的幂等分支，不是错误。
		return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, "receipt 已失效")}, nil
	}
	return &pb.ChangeInvisibleDurationResponse{Status: okStatus(), ReceiptHandle: req.GetReceiptHandle()}, nil
}

// QueryAssignment 全部队列经 RouteView 指向其 leader（单机形态即本节点）。
// EnsureTopic 失败按性质分类，规则见 server.go 的 topicErrStatus；队列
// leader 未知（选举窗口）时整包 HA_NOT_AVAILABLE，行为与 QueryRoute 一致。
func (s *Server) QueryAssignment(ctx context.Context, req *pb.QueryAssignmentRequest) (*pb.QueryAssignmentResponse, error) {
	name := req.GetTopic().GetName()
	tc, err := s.mt.EnsureTopic(ctx, name)
	if err != nil {
		return &pb.QueryAssignmentResponse{Status: s.topicErrStatus("QueryAssignment", name, err)}, nil
	}
	qs, err := s.messageQueues(tc, req.GetTopic())
	if err != nil {
		s.logger.Warn("QueryAssignment 拒答：部分队列所属组尚无 leader（选举窗口）", "topic", name,
			"group", req.GetGroup().GetName(), "err", err)
		return &pb.QueryAssignmentResponse{Status: errStatus(pb.Code_HA_NOT_AVAILABLE, err.Error())}, nil
	}
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
