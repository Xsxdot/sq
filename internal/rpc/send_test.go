// send_test.go 验证 rpc.Server.SendMessage 的写路径行为。
//
// 职责：
//   - 覆盖普通消息正常写入、M1 拒绝 DELAY/空 body/超限 body 四条基础用例
//   - 覆盖「整批任一条失败即整体失败，且失败前已翻译通过的消息不得被持久化」
//     这一由 controller ruling 修正的语义：验证与写入必须分两遍，不能边验边写；
//     覆盖类型非法（DELAY）与 body 超限两种触发方式，因为超限校验是后续补丁
//     加入的第一遍拦截项，必须单独证明它同样不会漏到第二遍才被 Append 拦下
//   - 覆盖超限 body 精确映射到 Code_MESSAGE_BODY_TOO_LARGE（不是被 Append
//     报错折叠成 Code_INTERNAL_SERVER_ERROR）
//
// 边界：
//   - 只测协议适配层 SendMessage 的行为，不重复测 produce.Append 内部逻辑
//   - ReceiveMessage RPC 属 Task 11，尚未落地：批量非持久化的证明改走
//     deliver.Deliverer.Receive（与 Task 11 落地后将复用的同一条消费路径），
//     而不是等 Task 11 完成后再补测试
package rpc

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
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

// newTestClientWithDeliverer 与 server_test.go 的 newTestClient 走同一套全真组件
// 搭建流程（autoCreate=true），但额外把 *deliver.Deliverer 一并返回。
//
// 之所以在本文件重新搭一遍而不是改造 server_test.go 的 newTestClient：那是
// Task 9 的既有测试基座，本任务只新增文件，不代其改动签名影响其他用例；
// 本测试需要绕开尚未实现的 ReceiveMessage RPC（Task 11），直接从消费引擎
// （SendMessage 与未来 ReceiveMessage 共用的同一份 store）验证非持久化，
// 因此需要拿到 dl 引用。
func newTestClientWithDeliverer(t *testing.T) (pb.MessagingServiceClient, *deliver.Deliverer) {
	t.Helper()
	return buildTestServer(t, 0)
}

// buildTestServer 是 newTestClientWithDeliverer 与超限 body 测试共用的搭建逻辑；
// maxRecvMsgSize<=0 时使用 gRPC 默认上限（4MB），否则用 grpc.MaxRecvMsgSize
// 覆盖——仅用于让本文件的超限测试样例能把请求真正送到应用层校验代码。
func buildTestServer(t *testing.T, maxRecvMsgSize int) (pb.MessagingServiceClient, *deliver.Deliverer) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, _ := meta.New(st, true, 4, slog.Default())
	pr := produce.New(st, mt, slog.Default())
	dl := deliver.New(st, mt, pr, slog.Default())
	cfg, _ := config.Load("")
	srv := New(cfg, mt, pr, dl, slog.Default())

	lis := bufconn.Listen(1 << 20)
	var serverOpts []grpc.ServerOption
	if maxRecvMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(maxRecvMsgSize))
	}
	gs := grpc.NewServer(serverOpts...)
	srv.Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewMessagingServiceClient(conn), dl
}

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

// TestSendMessageRejectsBatchAtomically 锁定 controller ruling 的语义：批内第一条
// 合法、第二条非法（DELAY）时，返回非 OK 状态，且第一条不得被持久化。
//
// 如果实现是"边校验边写"（brief 字面代码的写法），第一条会先被 produce.Append
// 真正落盘，随后第二条校验失败才返回整体失败——客户端收到"整批失败"的状态后
// 无法区分哪些消息其实已经写入，而这类客户端校验失败本身不可重试（重发同样的
// 请求只会再次在同一位置失败），于是第一条消息就永久卡在"服务端已存但客户端
// 认为未发送成功"的状态。用直接消费（而非等 Task 11 的 ReceiveMessage RPC）
// 验证 store 里确实没有这条消息，证明实现是"先整批校验、全部通过才写"。
func TestSendMessageRejectsBatchAtomically(t *testing.T) {
	c, dl := newTestClientWithDeliverer(t)
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
	msgs, err := dl.Receive(context.Background(), "g-batch-check", topic, 0, 10, time.Second, 0)
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
	c, _ := buildTestServer(t, bigMsgSize)
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
// （produce.MaxBodySize+1 字节 NORMAL）。超限校验是本次修复新加入第一遍的
// 拦截项，必须单独证明它同样卡在第一遍，不会让第一条先被 Append 写入盘上——
// 否则修复前描述的"第一条已持久化，响应却报整体失败"的口子会以 body 超限
// 的形式重新打开。证明方式与既有的类型非法用例一致：SendMessage 后直接用
// deliver.Deliverer.Receive 消费该 topic，断言取到 0 条。
func TestSendMessageRejectsBatchWithOversizedBodyAtomically(t *testing.T) {
	// 同上：用 bigMsgSize 覆盖默认 MaxRecvMsgSize，让超限请求能真正到达
	// SendMessage 的应用层校验。
	c, dl := buildTestServer(t, bigMsgSize)
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

	msgs, err := dl.Receive(context.Background(), "g-batch-oversized-check", topic, 0, 10, time.Second, 0)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("批量校验失败后不应有消息被持久化，实际取到 %d 条", len(msgs))
	}
}

func strPtr(s string) *string { return &s }
