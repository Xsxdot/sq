// send_test.go 验证 rpc.Server.SendMessage 的写路径行为。
//
// 职责：
//   - 覆盖普通消息正常写入、M1 拒绝 DELAY/空 body/超限 body 四条基础用例
//   - 覆盖「整批任一条失败即整体失败，且失败前已翻译通过的消息不得被持久化」：
//     验证与写入必须分两遍，不能边验边写；覆盖类型非法（DELAY）与 body 超限
//     两种触发方式，因为超限校验是后来才加进第一遍的拦截项，必须单独证明它
//     同样不会漏到第二遍才被 Append 拦下
//   - 覆盖超限 body 精确映射到 Code_MESSAGE_BODY_TOO_LARGE（不是被 Append
//     报错折叠成 Code_INTERNAL_SERVER_ERROR）
//   - 覆盖 topic 相关失败的错误分类：关闭自动创建时发往未建 topic 必须报
//     TOPIC_NOT_FOUND、名字非法必须报 ILLEGAL_TOPIC
//
// 边界：
//   - 只测协议适配层 SendMessage 的行为，不重复测 produce.Append 内部逻辑
//   - "某条消息有没有真的落盘"一律从 deliver.Deliverer.Receive 验证而不是走
//     ReceiveMessage RPC：要证明的是存储侧的事实，绕开协议层能让断言失败时
//     少一层嫌疑对象
package rpc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xushixin/sq/internal/core/produce"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
	"github.com/xushixin/sq/internal/store"
)

// bigMsgSize 超限 body 测试专用的 gRPC server 端 MaxRecvMsgSize。
//
// produce.MaxBodySize 恰好等于 gRPC 默认的 4MB MaxRecvMsgSize；一条刚好
// 超过 produce.MaxBodySize 1 字节的请求体，加上 Topic/SystemProperties 等
// 字段的 proto 编码开销，整条 gRPC 消息的序列化大小会略微超过这个默认上限，
// 于是请求在到达 SendMessage 的应用层校验之前，就先被 gRPC 传输层以
// ResourceExhausted 拒绝——测试永远走不到本次要验证的 MESSAGE_BODY_TOO_LARGE
// 分支。这里把测试自己起的 gRPC server 的 MaxRecvMsgSize 调大到明显超过
// produce.MaxBodySize，只影响这两个专用测试的连接，不改变 server.go 的
// 生产配置（生产环境该给多大的默认值超出本次修复范围）。
const bigMsgSize = 2 * produce.MaxBodySize

// TestSendMessageNormal 验证一条合法 NORMAL 消息写入成功，返回的 entry 带非空 msgId。
func TestSendMessageNormal(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "orders"},
			SystemProperties: &pb.SystemProperties{
				MessageId:   "0102030405060708090A0B0C0D0E0F10",
				MessageType: pb.MessageType_NORMAL,
				Tag:         strPtr("created"),
			},
			Body: []byte("hello"),
		}},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status: %v", resp.GetStatus())
	}
	if len(resp.GetEntries()) != 1 || resp.GetEntries()[0].GetMessageId() == "" {
		t.Fatalf("entries: %v", resp.GetEntries())
	}
}

// TestSendMessageRejectsDelayInM1 验证 M1 拒绝 DELAY 类型消息（延时消息属 M3）。
func TestSendMessageRejectsDelayInM1(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "orders"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_DELAY},
			Body:             []byte("x"),
		}},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() == pb.Code_OK {
		t.Fatal("M1 应拒绝延时消息")
	}
}

// TestSendMessageRejectsEmptyBody 验证空 body 消息被拒绝。
func TestSendMessageRejectsEmptyBody(t *testing.T) {
	c := newTestClient(t)
	resp, _ := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "orders"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
		}},
	})
	if resp.GetStatus().GetCode() == pb.Code_OK {
		t.Fatal("空 body 应被拒绝")
	}
}

// TestSendMessageRejectsBatchAtomically 锁定批量写入的原子语义：批内第一条
// 合法、第二条非法（DELAY）时，返回非 OK 状态，且第一条不得被持久化。
//
// 如果实现是"边校验边写"，第一条会先被 produce.Append 真正落盘，随后第二条
// 校验失败才返回整体失败——客户端收到"整批失败"的状态后无法区分哪些消息其实
// 已经写入，而这类客户端校验失败本身不可重试（重发同样的请求只会再次在同一
// 位置失败），于是第一条消息就永久卡在"服务端已存但客户端认为未发送成功"的
// 状态。这里直接从消费引擎读，验证 store 里确实没有这条消息，证明实现是
// "先整批校验、全部通过才写"。
func TestSendMessageRejectsBatchAtomically(t *testing.T) {
	env := newTestEnv(t, true)
	c, dl := env.client, env.dl
	const topic = "orders-batch"
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{
			{
				Topic:            &pb.Resource{Name: topic},
				SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
				Body:             []byte("first-valid"),
			},
			{
				Topic:            &pb.Resource{Name: topic},
				SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_DELAY},
				Body:             []byte("second-invalid"),
			},
		},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() == pb.Code_OK {
		t.Fatal("批内含非法消息，整批应失败")
	}
	if len(resp.GetEntries()) != 0 {
		t.Fatalf("整批失败时不应返回任何 entry: %v", resp.GetEntries())
	}

	// 直接从消费引擎取该 topic，证明"first-valid"没有被真正写入。
	msgs, err := dl.Receive(context.Background(), "g-batch-check", topic, 0, 10, time.Second, 0, nil)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("批量校验失败后不应有消息被持久化，实际取到 %d 条", len(msgs))
	}
}

// TestSendMessageRejectsOversizedBody 验证超过 produce.MaxBodySize 的单条消息
// 被第一遍校验拦下，返回 Code_MESSAGE_BODY_TOO_LARGE——而不是被放过去，
// 撞到 produce.Append 的同款检查后被折叠成 Code_INTERNAL_SERVER_ERROR。
// 后者语义是"服务端故障，可重试"，会诱导客户端对一条永远不可能成功的
// 超限消息反复重试；这是本次修复要堵住的具体协议码差异。
func TestSendMessageRejectsOversizedBody(t *testing.T) {
	// 用 bigMsgSize 覆盖 gRPC server 端默认 MaxRecvMsgSize，否则这条刻意超限
	// 的请求在到达 SendMessage 之前就会被 gRPC 传输层拒绝（见 bigMsgSize 注释）。
	c := newTestEnv(t, true, grpc.MaxRecvMsgSize(bigMsgSize)).client
	oversized := make([]byte, produce.MaxBodySize+1)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "orders"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			Body:             oversized,
		}},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_MESSAGE_BODY_TOO_LARGE {
		t.Fatalf("status: 期望 MESSAGE_BODY_TOO_LARGE，得到 %v", resp.GetStatus())
	}
}

// TestSendMessageRejectsBatchWithOversizedBodyAtomically 是
// TestSendMessageRejectsBatchAtomically 的姊妹用例，触发方式换成 body 超限
// 而不是类型非法：批内第一条合法（100 字节 NORMAL）、第二条超限
// （produce.MaxBodySize+1 字节 NORMAL）。超限校验是后来才加进第一遍的拦截项，
// 必须单独证明它同样卡在第一遍，不会让第一条先被 Append 写入盘上——否则
// "第一条已持久化，响应却报整体失败"的口子会以 body 超限的形式重新打开。
// 证明方式与既有的类型非法用例一致：SendMessage 后直接用
// deliver.Deliverer.Receive 消费该 topic，断言取到 0 条。
func TestSendMessageRejectsBatchWithOversizedBodyAtomically(t *testing.T) {
	// 同上：用 bigMsgSize 覆盖默认 MaxRecvMsgSize，让超限请求能真正到达
	// SendMessage 的应用层校验。
	env := newTestEnv(t, true, grpc.MaxRecvMsgSize(bigMsgSize))
	c, dl := env.client, env.dl
	const topic = "orders-batch-oversized"
	oversized := make([]byte, produce.MaxBodySize+1)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{
			{
				Topic:            &pb.Resource{Name: topic},
				SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
				Body:             make([]byte, 100),
			},
			{
				Topic:            &pb.Resource{Name: topic},
				SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
				Body:             oversized,
			},
		},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() == pb.Code_OK {
		t.Fatal("批内含超限消息，整批应失败")
	}
	if len(resp.GetEntries()) != 0 {
		t.Fatalf("整批失败时不应返回任何 entry: %v", resp.GetEntries())
	}

	msgs, err := dl.Receive(context.Background(), "g-batch-oversized-check", topic, 0, 10, time.Second, 0, nil)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("批量校验失败后不应有消息被持久化，实际取到 %d 条", len(msgs))
	}
}

// TestSendMessageAutoCreateDisabledReturnsTopicNotFound 锁定写路径的错误分类：
// 关闭 auto_create_topic（README 里推荐的生产姿态）后，往一个名字合法但未注册的
// topic 发消息，必须报 TOPIC_NOT_FOUND。
//
// 这是一个真实场景：生产者缓存了旧路由，或者 topic 干脆忘了建。若折叠成
// INTERNAL_SERVER_ERROR，该码的语义是"服务端坏了，重试我"——客户端会按 sq 自己
// 在 settings.go 里下发的退避策略把三次重试全部烧掉，最后报一个"服务端内部错误"，
// 而真实原因只是没建 topic；服务端这边则为一台完全健康的 broker 打三条 Error 日志。
func TestSendMessageAutoCreateDisabledReturnsTopicNotFound(t *testing.T) {
	c := newTestEnv(t, false).client
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "never-provisioned"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			Body:             []byte("hello"),
		}},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_TOPIC_NOT_FOUND {
		t.Fatalf("status: 期望 TOPIC_NOT_FOUND，得到 %v", resp.GetStatus())
	}
}

// TestSendMessageMalformedTopicReturnsIllegalTopic 与上一条互补：名字本身不合法
// （含 '/'）时必须报 ILLEGAL_TOPIC，而不是 TOPIC_NOT_FOUND 或
// INTERNAL_SERVER_ERROR。三者对客户端是三种不同的处置建议——改名字、去建 topic、
// 重试——不能混为一谈。注意这条与 autoCreate 无关：名字校验发生在自动创建之前。
func TestSendMessageMalformedTopicReturnsIllegalTopic(t *testing.T) {
	c := newTestClient(t) // autoCreate=true，证明名字校验优先于自动创建
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "bad/name"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			Body:             []byte("hello"),
		}},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_ILLEGAL_TOPIC {
		t.Fatalf("status: 期望 ILLEGAL_TOPIC，得到 %v", resp.GetStatus())
	}
}

// TestSendMessageRejectedWhenDiskBlocked 超水位拒写返回 FORBIDDEN（保读不保写）。
func TestSendMessageRejectedWhenDiskBlocked(t *testing.T) {
	env := newTestEnv(t, true) // server_test.go 既有 fixture，本 task 为其增 blocked 字段
	c, blocked := env.client, env.blocked
	blocked.Store(true)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "dw"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			Body:             []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_FORBIDDEN {
		t.Fatalf("期望 FORBIDDEN，得到 %v %v", resp.GetStatus(), err)
	}
	blocked.Store(false)
	sendOne(t, c, "dw", "x") // 恢复后可写
}

func TestSendDelayMessageGoesToDelayAreaNotDeliverable(t *testing.T) {
	env := newTestEnv(t, true)
	due := time.Now().Add(time.Hour)
	resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "dly"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_DELAY,
				DeliveryTimestamp: timestamppb.New(due),
			},
			Body: []byte("later"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("延时发送应成功: %v %v", resp.GetStatus(), err)
	}
	// 未到期：正常消费链路取不到
	msgs, err := env.dl.Receive(context.Background(), "g", "dly", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("未到期不应可消费: %d %v", len(msgs), err)
	}
	// 盘上 delay/ 恰有一条
	pfx := []byte(store.DelayPrefix)
	n := 0
	env.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil })
	if n != 1 {
		t.Fatalf("delay 条目数 = %d，期望 1", n)
	}
}

func TestSendDelayMissingTimestampRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "dly"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_DELAY},
			Body:             []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_ILLEGAL_DELIVERY_TIME {
		t.Fatalf("期望 ILLEGAL_DELIVERY_TIME，得到 %v %v", resp.GetStatus(), err)
	}
}

func TestSendNormalWithDeliveryTimestampRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "dly"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_NORMAL,
				DeliveryTimestamp: timestamppb.New(time.Now().Add(time.Hour)),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("期望 MESSAGE_PROPERTY_CONFLICT_WITH_TYPE，得到 %v %v", resp.GetStatus(), err)
	}
}

func TestQueryRouteAdvertisesDelayType(t *testing.T) {
	// 守护 SDK 客户端侧校验：ValidateMessageType=true 时 SDK 发送前检查路由的
	// AcceptMessageTypes，缺 DELAY 则延时消息在客户端本地就被拒（producer.go:191）
	c := newTestClient(t)
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{Topic: &pb.Resource{Name: "dly"}})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("QueryRoute: %v %v", resp.GetStatus(), err)
	}
	for _, mq := range resp.GetMessageQueues() {
		hasDelay := false
		for _, mt := range mq.GetAcceptMessageTypes() {
			if mt == pb.MessageType_DELAY {
				hasDelay = true
			}
		}
		if !hasDelay {
			t.Fatalf("队列 %d 未通告 DELAY 类型", mq.GetId())
		}
	}
}

func strPtr(s string) *string { return &s }
