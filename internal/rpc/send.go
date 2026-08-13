// SendMessage 相关：proto Message ↔ core.Message 的写方向翻译。
// 边界：NORMAL、DELAY（M3）、FIFO（M4）与 TRANSACTION（M6）。
package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// SendMessage 批量写入。
//
// 三遍处理（第零遍 + 两遍），不能边校验边写：先在"第零遍"做只读的 topic 级
// 预检——批内 topic 必须一致、名字必须合法、未开自动创建时 topic 必须已存在，
// 任何一条不满足整批直接拒、零落盘；再在"第一遍"把整批消息逐条翻译并校验
// （toCoreMessage），任一条不合法就直接返回该失败状态、不写入任何一条；
// 只有整批全部通过校验，才进入第二遍逐条 Append。
//
// 为什么不能像"翻译一条、校验一条、立刻 Append 一条"那样做：假设批内第 1 条
// 合法、第 2 条不合法，若边验边写，第 1 条会在第 2 条校验失败之前就已经真正
// 落盘，但响应仍然告诉客户端"整批失败"。客户端据此判断整批未生效，而这类
// 校验失败（消息类型/内容非法）本身不可重试——重发同一请求只会在同一位置
// 再次失败——于是第 1 条就永久卡在"服务端已持久化但客户端认为未发送成功"
// 的不一致状态，且没有任何机制能让客户端发现并处理它。两遍处理保证了
// "整批任一条失败即整体失败"这句话在字面上和效果上一致：失败就是真的什么
// 都没写。
func (s *Server) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	// 入口快速失败：本节点对 topic 的任一队列组都不是 leader（整机处在
	// 选举窗口或分区）时，整批消息的提案都注定被拒——与其等批次构造 +
	// 编解码完成后才在提案处失败，不如入口就拒，省一次批次构造。
	//
	// 为什么必须查「任一队列组」而不是用队列 0 作代理：多组集群里
	// topic 的队列摊布在多个组上（GroupForQueue 入盘映射），本节点
	// 可能是队列 1/3/5 所在组的 leader 却不是队列 0 所在组的 leader——
	// 只查队列 0 会把能服务的节点误拒（SDK 拿到 HA_NOT_AVAILABLE 后
	// 隔离该端点、重试预算烧在错误的候选上，三节点 e2e 实测发送失败）。
	// 探针只做「本节点明确不是写主人」的粗筛，单组误判（探针放行但
	// propose 的 rr 选到别组队列）由 propose 的 ErrNotLeader 映射兜底。
	if msgs := req.GetMessages(); len(msgs) > 0 {
		if topic := msgs[0].GetTopic().GetName(); !s.leadsAnyQueueGroup(topic) {
			s.logger.Debug("SendMessage 快速失败：本节点非该 topic 任一队列组 leader", "topic", topic)
			return &pb.SendMessageResponse{Status: errStatus(pb.Code_HA_NOT_AVAILABLE,
				"本节点不是该 topic 队列的当前 leader，请向 leader 节点重试")}, nil
		}
	}
	// 磁盘水位拒写保读（spec §7）：只拦生产者写入，消费链路（Receive/Ack）不受影响
	if s.writeBlocked != nil && s.writeBlocked.Load() {
		s.logger.Warn("磁盘水位超限，拒绝写入", "messages", len(req.GetMessages()))
		return &pb.SendMessageResponse{Status: errStatus(pb.Code_FORBIDDEN,
			"磁盘使用率超过水位线，暂时拒写（保读）")}, nil
	}
	// 第零遍：topic 级只读预检（spec 鉴权收尾 §6 / backlog B6）。
	// 必须完全无副作用——EnsureTopic（会创建 topic）绝不能出现在这里，
	// 否则"整批任一条失败即什么都没发生"的承诺被打破。
	if len(req.GetMessages()) > 0 {
		topic := req.GetMessages()[0].GetTopic().GetName()
		for _, pm := range req.GetMessages()[1:] {
			if pm.GetTopic().GetName() != topic {
				// 官方 Go/Java SDK 客户端侧即拒绝混 topic 批次（producer.go:276），
				// 能到这里的只有手写客户端；若不拒绝，第二遍逐条 Append 会在部分
				// 落盘后因 topic 错误返回不可重试错误，已落盘条目成为幽灵消息
				s.logger.Warn("SendMessage 拒绝：批内 topic 不一致", "topic", topic, "other", pm.GetTopic().GetName())
				return &pb.SendMessageResponse{Status: errStatus(pb.Code_BAD_REQUEST,
					"批内所有消息的 topic 必须一致")}, nil
			}
		}
		if err := meta.ValidateName(topic); err != nil {
			s.logger.Warn("SendMessage 拒绝：topic 名字非法", "topic", topic, "err", err)
			return &pb.SendMessageResponse{Status: errStatus(pb.Code_ILLEGAL_TOPIC, err.Error())}, nil
		}
		if !s.cfg.AutoCreateTopic {
			if _, ok := s.mt.GetTopic(topic); !ok {
				s.logger.Warn("SendMessage 拒绝：topic 不存在且未开启自动创建", "topic", topic)
				return &pb.SendMessageResponse{Status: errStatus(pb.Code_TOPIC_NOT_FOUND,
					fmt.Sprintf("topic %q 不存在（auto_create_topic 已关闭）", topic))}, nil
			}
		}
	}
	// 第一遍：只翻译 + 校验，不做任何持久化。
	msgs := make([]*core.Message, 0, len(req.GetMessages()))
	for _, pm := range req.GetMessages() {
		m, st := s.toCoreMessage(pm)
		if st != nil {
			s.logger.Warn("SendMessage 拒绝", "topic", pm.GetTopic().GetName(), "reason", st.GetMessage())
			return &pb.SendMessageResponse{Status: st}, nil
		}
		msgs = append(msgs, m)
	}

	// 第二遍：整批校验通过后才真正写入。
	//
	// 快路径：多条且全部为普通消息 → AppendBatch 整批同队列、单 Pebble Batch、
	// 一次 fsync、整批原子。官方 SDK 的 batch send 只会产生这种批（客户端侧
	// 已禁止批内混入事务/延时/FIFO）；含特殊消息的多条请求走下方逐条回退
	// 路径，行为与历史版本完全一致。
	if batchable(msgs) {
		stored, err := s.pr.AppendBatch(ctx, msgs)
		if err != nil {
			s.logger.Warn("SendMessage 批量写入失败", "topic", msgs[0].Topic, "count", len(msgs), "err", err)
			return &pb.SendMessageResponse{
				Status: s.topicErrStatus("SendMessage", msgs[0].Topic, err, "batch_count", len(msgs)),
			}, nil
		}
		entries := make([]*pb.SendResultEntry, 0, len(stored))
		for _, m := range stored {
			entries = append(entries, &pb.SendResultEntry{
				Status:    okStatus(),
				MessageId: m.ID,
				Offset:    int64(m.Offset),
			})
		}
		s.logger.Debug("SendMessage 批量写入完成", "topic", msgs[0].Topic, "count", len(stored),
			"queue", stored[0].QueueID, "first_offset", stored[0].Offset)
		return &pb.SendMessageResponse{Status: okStatus(), Entries: entries}, nil
	}
	//
	// 注意：以下 at-least-once 论证仅适用于逐条回退路径；批量快路径整批原子，
	// 不存在部分落盘。这里仍可能在写到第 N 条时 Append 失败（store 内部故障），
	// 此时前面 N-1 条已经真正落盘且无法撤回——没有跨消息的原子写入机制。但
	// 这与上面两遍处理要解决的问题不同：这是运行时 I/O 故障，不是"客户端输入
	// 非法"，属于 MQ 客户端本就要容忍的 at-least-once 场景（收到失败状态后
	// 重试，服务端凭 msgId 或消费端幂等处理去重），因此仍然整批返回该错误，
	// 不额外引入多消息原子写入的复杂度。
	entries := make([]*pb.SendResultEntry, 0, len(msgs))
	for _, m := range msgs {
		var stored *core.Message
		var txID string
		var err error
		switch {
		case m.Transactional:
			// 半消息进暂存区：无队列无 offset（entry.Offset 回 0），
			// TransactionId 必须回填——SDK 的 transactionImpl 靠它发起
			// Commit/RollBack，漏了它整个事务 API 在客户端侧无法收尾
			stored, txID, err = s.tx.Stage(ctx, m)
		case m.DeliverAtMs > 0:
			// 延时消息进暂存区（未分配 offset，entry 里 Offset 回 0——SDK 的
			// SendReceipt 只消费 MessageId，offset 字段对延时场景无意义）
			stored, _, _, err = s.pr.AppendDelay(ctx, m)
		default:
			stored, err = s.pr.Append(ctx, m)
		}
		if err != nil {
			// Append 内部会调用 meta.EnsureTopic，因此它的失败同样分"客户端输入
			// 错误"与"服务端内部故障"两类，必须按 topicErrStatus 的规则分开
			// （详见该函数注释）。写路径上这一点比读路径更要紧：关掉
			// auto_create_topic 的部署里，往未建的 topic 发消息本是运维配置问题，
			// 若折叠成 INTERNAL_SERVER_ERROR，客户端会按 sq 自己下发的退避策略
			// 把三次重试全部烧掉，最后报一个"服务端内部错误"，而服务端这边则为
			// 一台完全健康的 broker 打三条 Error 日志。
			return &pb.SendMessageResponse{
				Status: s.topicErrStatus("SendMessage", m.Topic, err, "msg_id", m.ID),
			}, nil
		}
		entries = append(entries, &pb.SendResultEntry{
			Status:        okStatus(),
			MessageId:     stored.ID,
			Offset:        int64(stored.Offset),
			TransactionId: txID, // 非事务消息为空串，proto3 省略
		})
	}
	return &pb.SendMessageResponse{Status: okStatus(), Entries: entries}, nil
}

// toCoreMessage 翻译并校验一条 proto 消息。返回非 nil status 表示拒绝，
// 此时不产生任何副作用（不触碰 store/meta）。
func (s *Server) toCoreMessage(pm *pb.Message) (*core.Message, *pb.Status) {
	sp := pm.GetSystemProperties()
	var delayAt int64
	var transactional bool
	switch sp.GetMessageType() {
	case pb.MessageType_NORMAL:
		// SDK 只要带 deliveryTimestamp 就自动标 DELAY 类型，标 NORMAL 却带
		// 到期时间的只可能是行为异常的客户端。静默忽略该时间戳等于把"延时"
		// 悄悄变成"立即投递"，必须显式拒绝。
		if sp.GetDeliveryTimestamp() != nil {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"NORMAL 消息不应携带 delivery_timestamp")
		}
		// 同理（M4）：SDK 设了 messageGroup 就自动标 FIFO，标 NORMAL 却带组
		// 意味着这条消息会在 deliver 侧获得顺序锁语义而发送端不自知——拒绝。
		if sp.GetMessageGroup() != "" {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"NORMAL 消息不应携带 message_group")
		}
	case pb.MessageType_FIFO:
		// 顺序消息（M4）：MessageGroup 是顺序语义的全部依据（hash 定队列 +
		// 消费端顺序锁），缺了它"顺序"无从谈起，用协议专用码报错。
		if sp.GetMessageGroup() == "" {
			return nil, errStatus(pb.Code_ILLEGAL_MESSAGE_GROUP, "FIFO 消息缺少 message_group")
		}
		if sp.GetDeliveryTimestamp() != nil {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"FIFO 消息不应携带 delivery_timestamp")
		}
	case pb.MessageType_DELAY:
		if sp.GetDeliveryTimestamp() == nil {
			return nil, errStatus(pb.Code_ILLEGAL_DELIVERY_TIME, "DELAY 消息缺少 delivery_timestamp")
		}
		// 延时与顺序不可组合（M4）：SDK 两者都设时按组判定标 FIFO（上面的
		// 分支拒绝），裸客户端标 DELAY 带组同样拒绝——到期搬运经 Append
		// 重新入队，无法承诺组内相对顺序。
		if sp.GetMessageGroup() != "" {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"DELAY 消息不应携带 message_group")
		}
		delayAt = sp.GetDeliveryTimestamp().AsTime().UnixMilli()
		// 时间戳存在但落在非正区间（1970 epoch 或零值 time.Time）时不能静默
		// 降级成 NORMAL：DeliverAtMs 停在 0 会让 SendMessage 的路由门
		// m.DeliverAtMs>0 走不到 AppendDelay，消息被当普通消息落盘，类型与
		// 时间戳回读双双丢失。钳到 1ms 保证路由进门，已过期由 AppendDelay
		// 的直通逻辑立即投递，DELAY 语义原样保留。
		if delayAt <= 0 {
			delayAt = 1
		}
	case pb.MessageType_TRANSACTION:
		// 事务不可与延时/顺序组合（RocketMQ 语义）：半消息的可见时机由
		// EndTransaction 决定，delivery_timestamp 无处安放；提交时经正常
		// 写入路径重新入队，无法承诺组内相对顺序
		if sp.GetDeliveryTimestamp() != nil {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"TRANSACTION 消息不应携带 delivery_timestamp")
		}
		if sp.GetMessageGroup() != "" {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"TRANSACTION 消息不应携带 message_group")
		}
		transactional = true
	default:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
			fmt.Sprintf("未知消息类型 %v", sp.GetMessageType()))
	}
	if len(pm.GetBody()) == 0 {
		return nil, errStatus(pb.Code_MESSAGE_BODY_EMPTY, "消息体为空")
	}
	// 上限校验必须放在这里（第一遍），不能只依赖 produce.Append 的同款检查：
	// body 超限是客户端输入错误，与空 body 同性质，理应在第一遍就被拦下，
	// 否则批内一条超限消息会重新打开"前面消息已落盘、批量状态却报整体失败"
	// 的口子（两遍处理本来就是为了堵住这个口子）。同时超限要映射成
	// MESSAGE_BODY_TOO_LARGE 而不是走到 Append 报错后被折叠成
	// INTERNAL_SERVER_ERROR——后者语义是"服务端故障，可重试"，会让客户端
	// 对着一条永远不可能成功的超限消息反复重试。
	// 复用 produce.MaxBodySize 而不是重复写 4*1024*1024：上下限只能有一个
	// 出处，否则两处未来可能改出不一致的上限。
	if len(pm.GetBody()) > produce.MaxBodySize {
		return nil, errStatus(pb.Code_MESSAGE_BODY_TOO_LARGE,
			fmt.Sprintf("消息体过大: %d（上限 %d）", len(pm.GetBody()), produce.MaxBodySize))
	}
	born := time.Now().UnixMilli()
	if ts := sp.GetBornTimestamp(); ts != nil {
		born = ts.AsTime().UnixMilli()
	}
	// 编码方式必须收下并落盘：sq 只存字节、不解压，若这里丢掉 body_encoding，
	// 投递时就只能回一个"未声明"，消费端拿到压缩过的 Body 却以为是明文——
	// 交给应用的就是一段乱码，而且没有任何一层会报错。未知编码无法落盘
	// （core 存不下自己不认识的 token），至少要留一条日志说明丢了什么。
	enc, known := bodyEncodingToCore(sp.GetBodyEncoding())
	if !known {
		s.logger.Warn("SendMessage 收到本版本不认识的 body_encoding，将按未声明落盘",
			"topic", pm.GetTopic().GetName(), "msg_id", sp.GetMessageId(),
			"body_encoding", sp.GetBodyEncoding())
	}
	return &core.Message{
		ID:           sp.GetMessageId(), // 客户端生成的 msgId，保留以便端到端对账
		Topic:        pm.GetTopic().GetName(),
		Tag:          sp.GetTag(),
		Keys:         sp.GetKeys(),
		MessageGroup: sp.GetMessageGroup(),
		Properties:   pm.GetUserProperties(),
		Body:         pm.GetBody(),
		BodyEncoding: enc,
		BodyDigest:   digestToCore(sp.GetBodyDigest()),
		BornAtMs:     born,
		BornHost:     sp.GetBornHost(),
		// DELAY 消息才有值；任意未来时间都接受（spec：任意秒级延时，不设上限），
		// 已过期的时间戳由 AppendDelay 直通立即投递
		DeliverAtMs: delayAt,
		// TRANSACTION 消息才有值：SendMessage 据此分流到 txn.Stage 暂存
		// （半消息不落 msg/，对消费者不可见，提交时才移入）
		Transactional: transactional,
		// TraceContext 是 W3C traceparent 之类的链路上下文：不带回去，
		// 分布式追踪就在 sq 这一跳断掉，且断得悄无声息。
		TraceContext: sp.GetTraceContext(),
	}, nil
}

// batchable 判断一批消息可否走 AppendBatch 快路径：多条、且全部为普通消息。
// 事务/延时消息各有独立暂存路径；FIFO 消息的队列由 MessageGroup 哈希决定，
// 与整批轮询选队冲突——三者任一出现即回退逐条处理，保证历史行为不变。
func batchable(msgs []*core.Message) bool {
	if len(msgs) < 2 {
		return false
	}
	for _, m := range msgs {
		if m.Transactional || m.DeliverAtMs > 0 || m.MessageGroup != "" {
			return false
		}
	}
	return true
}
