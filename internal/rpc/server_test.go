// server_test.go 验证 rpc.Server 的 gRPC 服务基座。
//
// 职责：
//   - 用 bufconn 起真实 gRPC server + client，覆盖 QueryRoute 自动建 topic、
//     Heartbeat 注册消费组、Telemetry settings 回显三条主链路
//
// 边界：
//   - 只测协议适配层的行为（请求→响应/状态码），不重复测 meta/produce/deliver
//     内部逻辑（那是各自包自己的测试职责）
package rpc

import (
	"context"
	"log/slog"
	"net"
	"testing"

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

// newTestClient 起全真组件 + bufconn gRPC，返回客户端 stub。
func newTestClient(t *testing.T) pb.MessagingServiceClient {
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
	gs := grpc.NewServer()
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
	return pb.NewMessagingServiceClient(conn)
}

// TestQueryRouteAutoCreatesTopic 验证未知 topic 在 autoCreate 开启时被自动建
// 并返回完整路由（队列数、broker endpoints）。
func TestQueryRouteAutoCreatesTopic(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{
		Topic: &pb.Resource{Name: "orders"},
	})
	if err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status: %v", resp.GetStatus())
	}
	if len(resp.GetMessageQueues()) != 4 {
		t.Fatalf("队列数: %d", len(resp.GetMessageQueues()))
	}
	mq := resp.GetMessageQueues()[0]
	if len(mq.GetBroker().GetEndpoints().GetAddresses()) == 0 {
		t.Fatal("缺少 broker endpoints")
	}
}

// TestHeartbeatRegistersGroup 验证带 group 的心跳会顺带注册消费组。
func TestHeartbeatRegistersGroup(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.Heartbeat(context.Background(), &pb.HeartbeatRequest{
		Group: &pb.Resource{Name: "g-hb"},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("Heartbeat: %v %v", resp.GetStatus(), err)
	}
}

// TestTelemetrySettingsEcho 验证客户端发来的 Settings 会被原样回发
// （SDK 启动阶段依赖这个握手完成协商）。
func TestTelemetrySettingsEcho(t *testing.T) {
	c := newTestClient(t)
	stream, err := c.Telemetry(context.Background())
	if err != nil {
		t.Fatalf("Telemetry: %v", err)
	}
	err = stream.Send(&pb.TelemetryCommand{Command: &pb.TelemetryCommand_Settings{
		Settings: &pb.Settings{},
	}})
	if err != nil {
		t.Fatalf("send settings: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if resp.GetSettings() == nil {
		t.Fatalf("期望 settings 回包，得到 %v", resp)
	}
}
