// SendMessage 相关：proto Message ↔ core.Message 的写方向翻译。
// 边界：仅 NORMAL 类型（M3 延时 / M4 FIFO 属性 / M6 事务在各自里程碑打开）。
package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/produce"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// SendMessage 批量写入。
//
// 两遍处理，不能边校验边写：先把整批消息逐条翻译并校验（toCoreMessage），
// 任一条不合法就直接返回该失败状态、不写入任何一条；只有整批全部通过校验，
// 才进入第二遍逐条 Append。
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
	// 注意：这里仍可能在写到第 N 条时 Append 失败（store 内部故障），此时前面
	// N-1 条已经真正落盘且无法撤回——没有跨消息的原子写入机制。但这与上面
	// 两遍处理要解决的问题不同：这是运行时 I/O 故障，不是"客户端输入非法"，
	// 属于 MQ 客户端本就要容忍的 at-least-once 场景（收到失败状态后重试，
	// 服务端凭 msgId 或消费端幂等处理去重），因此仍然整批返回该错误，
	// 不额外引入多消息原子写入的复杂度。
	entries := make([]*pb.SendResultEntry, 0, len(msgs))
	for _, m := range msgs {
		stored, err := s.pr.Append(m)
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
			Status:    okStatus(),
			MessageId: stored.ID,
			Offset:    int64(stored.Offset),
		})
	}
	return &pb.SendMessageResponse{Status: okStatus(), Entries: entries}, nil
}

// toCoreMessage 翻译并校验一条 proto 消息。返回非 nil status 表示拒绝，
// 此时不产生任何副作用（不触碰 store/meta）。
func (s *Server) toCoreMessage(pm *pb.Message) (*core.Message, *pb.Status) {
	sp := pm.GetSystemProperties()
	switch sp.GetMessageType() {
	case pb.MessageType_NORMAL:
	case pb.MessageType_FIFO:
		// FIFO 消息的写入路径与 NORMAL 相同（MessageGroup 定队列已在 produce 实现），
		// 但消费端顺序锁是 M4——为避免"能发不能保序"的假象，M1 一并拒绝。
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "顺序消息将在 M4 支持")
	case pb.MessageType_DELAY:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "延时消息将在 M3 支持")
	case pb.MessageType_TRANSACTION:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "事务消息将在 M6 支持")
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
		// TraceContext 是 W3C traceparent 之类的链路上下文：不带回去，
		// 分布式追踪就在 sq 这一跳断掉，且断得悄无声息。
		TraceContext: sp.GetTraceContext(),
	}, nil
}
