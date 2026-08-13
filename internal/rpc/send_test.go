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
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xushixin/sq/internal/core/deliver"
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
	msgs, err := dl.Receive(context.Background(), "g-batch-check", topic, 0, 10, time.Second, 0, deliver.AllPass)
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

	msgs, err := dl.Receive(context.Background(), "g-batch-oversized-check", topic, 0, 10, time.Second, 0, deliver.AllPass)
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
	msgs, err := env.dl.Receive(context.Background(), "g", "dly", 0, 10, time.Minute, 0, deliver.AllPass)
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

// TestSendDelayEpochTimestampNotDemotedToNormal 锁定 DELAY 消息携带非正到期
// 时间戳（1970 epoch 或零值 time.Time）不被静默降级成 NORMAL：存在性校验只查
// "带了没有"，不查正负；若这里不把 <=0 的到期时间钳到正数，DeliverAtMs 停在 0，
// SendMessage 的路由门 m.DeliverAtMs>0 走不到 AppendDelay，消息就被当普通消息
// 落盘——DELAY 类型与 DeliveryTimestamp 回读双双丢失（而且完全静默）。钳到
// 1ms 后由 AppendDelay 的已过期直通逻辑立即投递，DeliverAtMs 原样保留：普通
// 消息的 DeliverAtMs 恒为 0（types.go），取回的消息 >0 即证明仍是延时语义。
func TestSendDelayEpochTimestampNotDemotedToNormal(t *testing.T) {
	env := newTestEnv(t, true)
	c, dl := env.client, env.dl
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "dly-epoch"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_DELAY,
				DeliveryTimestamp: timestamppb.New(time.UnixMilli(0)),
			},
			Body: []byte("epoch"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("DELAY+epoch 时间戳应被接受（直通立即投递）: %v %v", resp.GetStatus(), err)
	}
	msgs, err := dl.Receive(context.Background(), "g", "dly-epoch", 0, 10, time.Second, 5*time.Second, deliver.AllPass)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("直通投递应可取到 1 条: %d %v", len(msgs), err)
	}
	if msgs[0].DeliverAtMs <= 0 {
		t.Fatalf("DeliverAtMs 应为正（DELAY 语义被保留），实际 %d——消息被降级成了 NORMAL", msgs[0].DeliverAtMs)
	}
}

// delayReq 本文件小 helper：一条延时消息的 SendMessageRequest。
// 构造照既有内联写法（MessageType_DELAY + DeliveryTimestamp）。
func delayReq(topic string, dueMs int64) *pb.SendMessageRequest {
	return &pb.SendMessageRequest{Messages: []*pb.Message{{
		Topic: &pb.Resource{Name: topic},
		SystemProperties: &pb.SystemProperties{
			MessageType:       pb.MessageType_DELAY,
			DeliveryTimestamp: timestamppb.New(time.UnixMilli(dueMs)),
		},
		Body: []byte("x"),
	}}}
}

// 延时消息的 SendResultEntry 必须带回可用的 recall 句柄；普通消息不带。
func TestSendMessageIssuesRecallHandleForDelayOnly(t *testing.T) {
	env := newTestEnv(t, true)
	due := time.Now().Add(time.Hour).UnixMilli()

	resp, err := env.client.SendMessage(context.Background(), delayReq("t-issue", due))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	h := resp.GetEntries()[0].GetRecallHandle()
	if h == "" {
		t.Fatalf("延时消息未签发 recall 句柄")
	}
	// 句柄必须真的能解出这条消息的坐标
	topic, gotDue, _, err := recallDecode(env.srv.handleSecret, h)
	if err != nil {
		t.Fatalf("签发的句柄自己解不开: %v", err)
	}
	if topic != "t-issue" || gotDue != due {
		t.Fatalf("句柄内容不对：topic=%q due=%d（期望 t-issue / %d）", topic, gotDue, due)
	}

	// 普通消息：不签发
	resp2, err := env.client.SendMessage(context.Background(), sendReq("t-issue"))
	if err != nil {
		t.Fatalf("SendMessage(普通): %v", err)
	}
	if got := resp2.GetEntries()[0].GetRecallHandle(); got != "" {
		t.Fatalf("普通消息签发了 recall 句柄：%q", got)
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

// FIFO 消息经全链路投递且遵守顺序锁：发 2 条同组，deliver 一次只吐 1 条
func TestSendFifoMessageOrderedThroughStack(t *testing.T) {
	env := newTestEnv(t, true)
	for _, body := range []string{"f1", "f2"} {
		resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
			Messages: []*pb.Message{{
				Topic: &pb.Resource{Name: "fifo"},
				SystemProperties: &pb.SystemProperties{
					MessageType:  pb.MessageType_FIFO,
					MessageGroup: strPtr("grp-1"),
				},
				Body: []byte(body),
			}},
		})
		if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
			t.Fatalf("FIFO 发送应成功: %v %v", resp.GetStatus(), err)
		}
	}
	// 同组消息落同一队列；顺序锁下首轮只投 f1。队列号由 hash 决定，逐队列探测。
	got := 0
	for q := uint32(0); q < 4; q++ {
		msgs, err := env.dl.Receive(context.Background(), "g", "fifo", q, 10, time.Minute, 0, deliver.AllPass)
		if err != nil {
			t.Fatalf("Receive q%d: %v", q, err)
		}
		got += len(msgs)
		for _, m := range msgs {
			if string(m.Body) != "f1" || m.MessageGroup != "grp-1" {
				t.Fatalf("首轮只应投 f1 且组名保留: %+v", m)
			}
		}
	}
	if got != 1 {
		t.Fatalf("顺序锁下首轮应恰投 1 条，实际 %d", got)
	}
}

func TestSendFifoMissingGroupRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "fifo"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_FIFO},
			Body:             []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_ILLEGAL_MESSAGE_GROUP {
		t.Fatalf("期望 ILLEGAL_MESSAGE_GROUP，得到 %v %v", resp.GetStatus(), err)
	}
}

func TestSendFifoWithDeliveryTimestampRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fifo"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_FIFO,
				MessageGroup:      strPtr("grp"),
				DeliveryTimestamp: timestamppb.New(time.Now().Add(time.Hour)),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("期望 MESSAGE_PROPERTY_CONFLICT_WITH_TYPE，得到 %v %v", resp.GetStatus(), err)
	}
}

// NORMAL/DELAY 携带 message_group 被拒：SDK 只要设了组就自动标 FIFO，
// 标其他类型却带组的只可能是行为异常的客户端；静默收下会让消息悄悄获得/
// 失去顺序语义（与 M3 的 NORMAL+delivery_timestamp 拒绝完全同型）
func TestSendNormalWithMessageGroupRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fifo"},
			SystemProperties: &pb.SystemProperties{
				MessageType:  pb.MessageType_NORMAL,
				MessageGroup: strPtr("grp"),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("期望 MESSAGE_PROPERTY_CONFLICT_WITH_TYPE，得到 %v %v", resp.GetStatus(), err)
	}
}

func TestSendDelayWithMessageGroupRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fifo"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_DELAY,
				MessageGroup:      strPtr("grp"),
				DeliveryTimestamp: timestamppb.New(time.Now().Add(time.Hour)),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("期望 MESSAGE_PROPERTY_CONFLICT_WITH_TYPE，得到 %v %v", resp.GetStatus(), err)
	}
}

// 守护 SDK 客户端侧校验（与 M3 的 DELAY 守护测试同型）：ValidateMessageType=true
// 时 SDK 发送前检查路由的 AcceptMessageTypes，缺 FIFO 则顺序消息在客户端本地
// 就被拒（producer.go:191）
func TestQueryRouteAdvertisesFifoType(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{Topic: &pb.Resource{Name: "fifo"}})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("QueryRoute: %v %v", resp.GetStatus(), err)
	}
	for _, mq := range resp.GetMessageQueues() {
		hasFifo := false
		for _, mt := range mq.GetAcceptMessageTypes() {
			if mt == pb.MessageType_FIFO {
				hasFifo = true
			}
		}
		if !hasFifo {
			t.Fatalf("队列 %d 未通告 FIFO 类型", mq.GetId())
		}
	}
}

// TestSendTransactionStagesHalfMessage 验证 TRANSACTION 消息经 SendMessage
// 走暂存区（M6）：返回 OK、entry 回填 TransactionId（SDK 的 transactionImpl
// 靠它发起 Commit/RollBack，漏了它整个事务 API 在客户端侧无法收尾）、
// 半消息未提交前对消费不可见（deliver 只扫 msg/，half/ 数据取不到）。
func TestSendTransactionStagesHalfMessage(t *testing.T) {
	env := newTestEnv(t, true)
	c, dl, st := env.client, env.dl, env.st
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "t-txn"},
			SystemProperties: &pb.SystemProperties{
				MessageId:   "M1",
				MessageType: pb.MessageType_TRANSACTION,
			},
			Body: []byte("half"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("事务发送应成功: %v %v", resp.GetStatus(), err)
	}
	entry := resp.GetEntries()[0]
	if entry.GetTransactionId() == "" {
		t.Fatal("SendResultEntry 未回填 transaction_id——SDK 的 Commit/Rollback 全靠它")
	}
	// 未提交前不可消费（半消息不可见：deliver 只扫 msg/，half/ 数据取不到）
	msgs, err := dl.Receive(context.Background(), "g", "t-txn", 0, 10, time.Minute, 0, deliver.AllPass)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("未提交的半消息不应可消费: %d %v", len(msgs), err)
	}
	// 盘上 half/ 恰有一条（与延时用例的 delay/ 断言同手法）
	pfx := []byte(store.HalfPrefix)
	n := 0
	st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil })
	if n != 1 {
		t.Fatalf("half 条目数 = %d，期望 1", n)
	}
}

// TestSendTransactionRejectsConflicts 验证 TRANSACTION 与延时/顺序组合被拒
// （RocketMQ 语义：事务不可与延时/顺序组合）：半消息的可见时机由
// EndTransaction 决定，delivery_timestamp 无处安放；提交时经正常写入路径
// 重新入队，无法承诺组内相对顺序。手法对齐既有 NORMAL/DELAY 冲突用例。
func TestSendTransactionRejectsConflicts(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "t-txn"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_TRANSACTION,
				DeliveryTimestamp: timestamppb.New(time.Now().Add(time.Hour)),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("TRANSACTION+delivery_timestamp 应被拒: %v %v", resp.GetStatus(), err)
	}
	resp, err = c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "t-txn"},
			SystemProperties: &pb.SystemProperties{
				MessageType:  pb.MessageType_TRANSACTION,
				MessageGroup: strPtr("grp"),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("TRANSACTION+message_group 应被拒: %v %v", resp.GetStatus(), err)
	}
}

// TestRouteAdvertisesTransaction 守护 SDK 客户端侧校验：ValidateMessageType=true
// 时 SDK 发送前检查路由的 AcceptMessageTypes，缺 TRANSACTION 则事务消息在
// 客户端本地就被拒（与 M3 的 DELAY / M4 的 FIFO 同教训）
func TestRouteAdvertisesTransaction(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{Topic: &pb.Resource{Name: "t-txn"}})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("QueryRoute: %v %v", resp.GetStatus(), err)
	}
	for _, mq := range resp.GetMessageQueues() {
		hasTxn := false
		for _, mt := range mq.GetAcceptMessageTypes() {
			if mt == pb.MessageType_TRANSACTION {
				hasTxn = true
			}
		}
		if !hasTxn {
			t.Fatalf("队列 %d 未通告 TRANSACTION 类型", mq.GetId())
		}
	}
}

func strPtr(s string) *string { return &s }

// sendReq 本文件小 helper：单/多条消息的 SendMessageRequest。
func sendReq(topics ...string) *pb.SendMessageRequest {
	req := &pb.SendMessageRequest{}
	for _, tp := range topics {
		req.Messages = append(req.Messages, &pb.Message{
			Topic:            &pb.Resource{Name: tp},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			Body:             []byte("x"),
		})
	}
	return req
}

// countTopicMsgs 扫 [MsgKey(topic,0,0), MsgKey(topic,queues,0)) 计数（默认 4 队列）。
func countTopicMsgs(t *testing.T, st *store.Store, topic string) int {
	t.Helper()
	n := 0
	err := st.Scan(store.MsgKey(topic, 0, 0), store.MsgKey(topic, 4, 0), 0,
		func(k, v []byte) (bool, error) { n++; return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSendRejectsMixedTopicBatchWithoutPersisting(t *testing.T) {
	env := newTestEnv(t, true)
	resp, err := env.client.SendMessage(context.Background(), sendReq("t-mix-a", "t-mix-b"))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_BAD_REQUEST {
		t.Fatalf("混 topic 批次应拒 BAD_REQUEST，实际 %v", resp.GetStatus())
	}
	// 关键断言：第一条也没落盘（B6 的幽灵消息就是这么产生的）
	if n := countTopicMsgs(t, env.st, "t-mix-a") + countTopicMsgs(t, env.st, "t-mix-b"); n != 0 {
		t.Fatalf("拒绝批次却落盘了 %d 条", n)
	}
}

func TestSendRejectsUnknownTopicWhenAutoCreateOff(t *testing.T) {
	env := newTestEnv(t, false)
	resp, err := env.client.SendMessage(context.Background(), sendReq("t-nocreate"))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_TOPIC_NOT_FOUND {
		t.Fatalf("未建 topic 应拒 TOPIC_NOT_FOUND，实际 %v", resp.GetStatus())
	}
	if n := countTopicMsgs(t, env.st, "t-nocreate"); n != 0 {
		t.Fatalf("拒绝批次却落盘了 %d 条", n)
	}
}

func TestSendRejectsIllegalTopicName(t *testing.T) {
	env := newTestEnv(t, true)
	resp, err := env.client.SendMessage(context.Background(), sendReq("bad/name"))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_ILLEGAL_TOPIC {
		t.Fatalf("非法名应在预检即拒 ILLEGAL_TOPIC，实际 %v", resp.GetStatus())
	}
}

// TestSendMessageBatchFastPath 验证纯普通消息的多条请求走整批落盘：
// 响应 Entries 与请求同序、offset 连续、全部同队列（整批一个 Pebble Batch
// 一次 fsync 的外部可见特征）。
func TestSendMessageBatchFastPath(t *testing.T) {
	c := newTestClient(t)
	req := &pb.SendMessageRequest{}
	for i := 0; i < 3; i++ {
		req.Messages = append(req.Messages, &pb.Message{
			Topic: &pb.Resource{Name: "batch-t"},
			SystemProperties: &pb.SystemProperties{
				MessageId:   fmt.Sprintf("%032X", i+1),
				MessageType: pb.MessageType_NORMAL,
			},
			Body: []byte("hello"),
		})
	}
	resp, err := c.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status: %v", resp.GetStatus())
	}
	entries := resp.GetEntries()
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(entries))
	}
	for i, e := range entries {
		if e.GetStatus().GetCode() != pb.Code_OK || e.GetMessageId() == "" {
			t.Fatalf("entry %d: %v", i, e)
		}
		if i > 0 && e.GetOffset() != entries[0].GetOffset()+int64(i) {
			t.Fatalf("entry %d offset %d 不连续（首条 %d）——整批应落同一队列连续 offset 段",
				i, e.GetOffset(), entries[0].GetOffset())
		}
	}
}

// TestSendMessageBatchWithFifoFallsBack 验证含 FIFO 消息的多条请求回退逐条
// 路径：行为与历史版本一致（各条独立成功），不因批量快路径引入而改变。
func TestSendMessageBatchWithFifoFallsBack(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{
			{
				Topic: &pb.Resource{Name: "mix-t"},
				SystemProperties: &pb.SystemProperties{
					MessageId:   "000000000000000000000000000000A1",
					MessageType: pb.MessageType_NORMAL,
				},
				Body: []byte("plain"),
			},
			{
				Topic: &pb.Resource{Name: "mix-t"},
				SystemProperties: &pb.SystemProperties{
					MessageId:    "000000000000000000000000000000A2",
					MessageType:  pb.MessageType_FIFO,
					MessageGroup: strPtr("g1"),
				},
				Body: []byte("fifo"),
			},
		},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status: %v", resp.GetStatus())
	}
	if len(resp.GetEntries()) != 2 {
		t.Fatalf("entries = %d, want 2", len(resp.GetEntries()))
	}
}
