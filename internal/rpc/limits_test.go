// limits_test.go 验证 MaxGRPCMessageSize 让"消息体上限 4MB"这条 spec 约束
// 在真实 gRPC 传输链路上确实可达。
//
// 职责：
//   - 按 cmd/sq/main.go 装配 grpc.NewServer 的同款 Option
//     （grpc.MaxRecvMsgSize/MaxSendMsgSize(rpc.MaxGRPCMessageSize)）起测试
//     server，证明一条 body 恰好等于 produce.MaxBodySize、且带真实
//     SystemProperties/UserProperties 的 SendMessage 请求不会被传输层拒绝
//   - 这是 Task 10 review 指出的覆盖缺口："body 恰好等于上限"这个边界用例
//     此前没有被任何测试覆盖（已有的 TestSendMessageRejectsOversizedBody
//     只覆盖了 MaxBodySize+1 的失败分支）
//
// 边界：
//   - 只测传输层配置是否放行到应用层，不重复测 SendMessage 的校验逻辑
//     （send_test.go 的职责）
//   - 故意不复用 send_test.go 的 buildTestServer：那个 helper 只调了
//     MaxRecvMsgSize，没有调 MaxSendMsgSize，且用的是任意大的 bigMsgSize
//     （用来绕开限制去测应用层校验），语义与本文件"验证生产配置本身够用"
//     的目的不同，混用会让两边的意图都变得含糊
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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
	"github.com/xushixin/sq/internal/store"
)

// newProdConfiguredClient 按 cmd/sq/main.go 里 grpc.NewServer 的同款 Option
// 起测试 server，与生产装配的传输层配置保持一致（这正是本测试要验证的对象，
// 不能像其它测试那样各自调一个专门放宽/收紧的 MaxRecvMsgSize）。
func newProdConfiguredClient(t *testing.T) pb.MessagingServiceClient {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 4, slog.Default())
	if err != nil {
		t.Fatalf("meta.New: %v", err)
	}
	pr := produce.New(st, mt, slog.Default())
	dl := deliver.New(st, mt, pr, slog.Default())
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	srv := New(cfg, mt, pr, dl, slog.Default())

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(MaxGRPCMessageSize),
	)
	srv.Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(MaxGRPCMessageSize),
		),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewMessagingServiceClient(conn)
}

// TestSendMessageBodyExactlyAtMaxBodySizeSucceeds 证明 spec §7 宣称的
// "消息体上限 4MB"在真实 gRPC 传输链路上确实可达：body 长度恰好等于
// produce.MaxBodySize，外加真实流量会带的 SystemProperties/UserProperties
// （MessageId、Tag、Keys、BornTimestamp、UserProperties），整条请求的
// gRPC 消息大小必然大于 MaxBodySize 本身——如果 server 端仍用 gRPC-go
// 默认的 4MiB MaxRecvMsgSize（数值上等于 MaxBodySize），这条请求会在到达
// SendMessage 应用层校验之前就被传输层以 ResourceExhausted 拒绝，客户端
// 收到的是裸传输层错误而不是 pb.Status。断言 Code_OK 就是断言这种情况
// 没有发生。
func TestSendMessageBodyExactlyAtMaxBodySizeSucceeds(t *testing.T) {
	c := newProdConfiguredClient(t)
	body := make([]byte, produce.MaxBodySize)
	for i := range body {
		body[i] = byte(i) // 非全零，避免压缩类中间件掩盖真实序列化大小（本项目未启用压缩，属防御性写法）
	}

	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "orders-max-body"},
			SystemProperties: &pb.SystemProperties{
				MessageId:     "0102030405060708090A0B0C0D0E0F10",
				MessageType:   pb.MessageType_NORMAL,
				Tag:           strPtr("created"),
				Keys:          []string{"order-123", "region-cn", "priority-high"},
				BornTimestamp: timestamppb.New(time.Now()),
			},
			UserProperties: map[string]string{
				"traceId":    "7f1c9e2a-4b3d-4e5f-8a6b-1234567890ab",
				"source":     "order-service",
				"schemaVer":  "v3",
				"retryCount": "0",
			},
			Body: body,
		}},
	})
	if err != nil {
		t.Fatalf("SendMessage（body 恰好等于 MaxBodySize）不应在传输层失败: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status: 期望 OK，得到 %v", resp.GetStatus())
	}
	if len(resp.GetEntries()) != 1 || resp.GetEntries()[0].GetMessageId() == "" {
		t.Fatalf("entries: %v", resp.GetEntries())
	}
}
