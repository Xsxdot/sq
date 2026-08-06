// server_test.go 验证 rpc.Server 的 gRPC 服务基座。
//
// 职责：
//   - 用 bufconn 起真实 gRPC server + client，覆盖 QueryRoute 自动建 topic、
//     Heartbeat 注册消费组、Telemetry settings 协商三条主链路
//   - 覆盖 QueryRoute/Heartbeat 的错误分类分支：非法名字 vs topic 不存在，
//     确保 Code 枚举没有把不同性质的失败折叠成同一个码
//
// 边界：
//   - 只测协议适配层的行为（请求→响应/状态码），不重复测 meta/produce/deliver
//     内部逻辑（那是各自包自己的测试职责）
package rpc

import (
	"context"
	"io"
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

// newTestClient 起全真组件（autoCreate=true）+ bufconn gRPC，返回客户端 stub。
func newTestClient(t *testing.T) pb.MessagingServiceClient {
	t.Helper()
	return newTestClientWithAutoCreate(t, true)
}

// newTestClientWithAutoCreate 与 newTestClient 相同，但 autoCreate 由调用方指定——
// 用于覆盖 autoCreate=false 时 QueryRoute 对未知合法 topic 返回 TOPIC_NOT_FOUND 的分支，
// 不影响 newTestClient 现有三个用例默认的 autoCreate=true 行为。
func newTestClientWithAutoCreate(t *testing.T, autoCreate bool) pb.MessagingServiceClient {
	t.Helper()
	_, c := newTestServerAndClient(t, autoCreate)
	return c
}

// newTestServerAndClient 与 newTestClientWithAutoCreate 相同，但把 *Server 也交出来——
// 停机行为（Shutdown 让 Telemetry 长流收尾）只能从服务端这一侧触发，
// 光有客户端 stub 测不了。
func newTestServerAndClient(t *testing.T, autoCreate bool) (*Server, pb.MessagingServiceClient) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, _ := meta.New(st, autoCreate, 4, slog.Default())
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
	return srv, pb.NewMessagingServiceClient(conn)
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

// telemetryNegotiate 走一趟 Telemetry 握手：上报 client 的 Settings，返回服务端下发的 Settings。
func telemetryNegotiate(t *testing.T, c pb.MessagingServiceClient, client *pb.Settings) *pb.Settings {
	t.Helper()
	stream, err := c.Telemetry(context.Background())
	if err != nil {
		t.Fatalf("Telemetry: %v", err)
	}
	if err := stream.Send(&pb.TelemetryCommand{Command: &pb.TelemetryCommand_Settings{Settings: client}}); err != nil {
		t.Fatalf("send settings: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if resp.GetSettings() == nil {
		t.Fatalf("期望 settings 回包，得到 %v", resp)
	}
	return resp.GetSettings()
}

// TestTelemetryPublishingSettingsCarryServerLimits 锁定发布端协商结果里那些
// 「必须由服务端填」的字段。
//
// 这条断言的由来是一次真实的互操作故障（Task 13 e2e）：服务端最初把客户端
// 上报的 Settings 原样回发，而官方 Go SDK 的 producerSettings.applySettingsCommand
// 会无条件把回包里的 Publishing.max_body_size 存成自己的本地上限——客户端
// 自报的 Publishing 里没有这个字段（它本来就是服务端字段），于是上限被改写成 0，
// 此后每一次 Send 都在客户端本地直接失败：
//
//	message body size exceeds the threshold, max size=0 bytes
//
// 请求根本到不了服务端，任何只用裸 protobuf stub 的测试都看不见这个问题。
func TestTelemetryPublishingSettingsCarryServerLimits(t *testing.T) {
	c := newTestClient(t)
	ct := pb.ClientType_PRODUCER
	got := telemetryNegotiate(t, c, &pb.Settings{
		ClientType: &ct,
		PubSub: &pb.Settings_Publishing{Publishing: &pb.Publishing{
			Topics: []*pb.Resource{{Name: "t-settings"}},
		}},
	})

	pub, ok := got.GetPubSub().(*pb.Settings_Publishing)
	if !ok {
		t.Fatalf("发布端必须收到 Publishing 分支，实际 %T", got.GetPubSub())
	}
	if pub.Publishing.GetMaxBodySize() != produce.MaxBodySize {
		t.Fatalf("max_body_size 应为 %d，实际 %d（为 0 时 SDK 会把每条消息都判成超限）",
			produce.MaxBodySize, pub.Publishing.GetMaxBodySize())
	}
	if !pub.Publishing.GetValidateMessageType() {
		t.Fatal("validate_message_type 应为 true：路由只通告 NORMAL，客户端应在本地拦下其它类型")
	}
	if len(pub.Publishing.GetTopics()) != 1 || pub.Publishing.GetTopics()[0].GetName() != "t-settings" {
		t.Fatalf("客户端自报的 topics 应原样带回，实际 %v", pub.Publishing.GetTopics())
	}
	bp := got.GetBackoffPolicy()
	if bp.GetMaxAttempts() != backoffMaxAttempts {
		t.Fatalf("backoff max_attempts 应为 %d，实际 %d", backoffMaxAttempts, bp.GetMaxAttempts())
	}
	eb := bp.GetExponentialBackoff()
	if eb == nil {
		t.Fatalf("backoff 策略应为指数退避，实际 %v", bp.GetStrategy())
	}
	if eb.GetInitial().AsDuration() != backoffInitial || eb.GetMax().AsDuration() != backoffMax {
		t.Fatalf("指数退避区间应为 %v→%v，实际 %v→%v",
			backoffInitial, backoffMax, eb.GetInitial().AsDuration(), eb.GetMax().AsDuration())
	}
	if got.GetAccessPoint().GetAddresses() == nil {
		t.Fatal("access_point 应带上服务端通告地址")
	}
}

// TestTelemetrySubscriptionSettingsMatchClientType 锁定订阅端协商结果：PubSub
// 分支必须是 Subscription。官方 SDK 的 simpleConsumerSettings.applySettingsCommand
// 对分支做强类型检查，拿到 Publishing 会直接判为
// "[bug] Issued settings not match with the client type" 并让握手失败。
func TestTelemetrySubscriptionSettingsMatchClientType(t *testing.T) {
	c := newTestClient(t)
	ct := pb.ClientType_SIMPLE_CONSUMER
	got := telemetryNegotiate(t, c, &pb.Settings{
		ClientType: &ct,
		PubSub: &pb.Settings_Subscription{Subscription: &pb.Subscription{
			Group: &pb.Resource{Name: "g-settings"},
		}},
	})

	sub, ok := got.GetPubSub().(*pb.Settings_Subscription)
	if !ok {
		t.Fatalf("订阅端必须收到 Subscription 分支，实际 %T", got.GetPubSub())
	}
	if sub.Subscription.GetGroup().GetName() != "g-settings" {
		t.Fatalf("消费组应原样带回，实际 %v", sub.Subscription.GetGroup())
	}
	if sub.Subscription.GetFifo() {
		t.Fatal("M1 不支持顺序消费，fifo 必须显式下发 false")
	}
	if sub.Subscription.GetLongPollingTimeout().AsDuration() != defaultLongPolling {
		t.Fatalf("长轮询上限应下发服务端真实值 %v，实际 %v",
			defaultLongPolling, sub.Subscription.GetLongPollingTimeout().AsDuration())
	}
}

// TestTelemetrySettingsWithoutPubSubStaysEmpty 客户端没上报 PubSub 时，服务端
// 不得凭空捏造一个分支——协商注定失败，但失败原因应由客户端按自己的规则报出来，
// 服务端替它猜一个分支只会把「客户端漏填」伪装成「服务端类型不匹配」。
func TestTelemetrySettingsWithoutPubSubStaysEmpty(t *testing.T) {
	c := newTestClient(t)
	got := telemetryNegotiate(t, c, &pb.Settings{})
	if got.GetPubSub() != nil {
		t.Fatalf("客户端未上报 PubSub 时回包不应带分支，实际 %T", got.GetPubSub())
	}
}

// TestQueryRouteAutoCreateDisabledReturnsTopicNotFound 验证 autoCreate 关闭时，
// 一个合法但未注册过的 topic 名字应归类为「topic 不存在」而不是「名字非法」——
// 两者是不同性质的失败，Code 不能混用。
func TestQueryRouteAutoCreateDisabledReturnsTopicNotFound(t *testing.T) {
	c := newTestClientWithAutoCreate(t, false)
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{
		Topic: &pb.Resource{Name: "never-created"},
	})
	if err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_TOPIC_NOT_FOUND {
		t.Fatalf("status: 期望 TOPIC_NOT_FOUND，得到 %v", resp.GetStatus())
	}
}

// TestQueryRouteMalformedTopicReturnsIllegalTopic 验证名字本身不合法（含非法字符）
// 时返回 ILLEGAL_TOPIC，而不是被误判为 TOPIC_NOT_FOUND。
func TestQueryRouteMalformedTopicReturnsIllegalTopic(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{
		Topic: &pb.Resource{Name: "bad/name"},
	})
	if err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_ILLEGAL_TOPIC {
		t.Fatalf("status: 期望 ILLEGAL_TOPIC，得到 %v", resp.GetStatus())
	}
}

// TestHeartbeatMalformedGroupReturnsIllegalConsumerGroup 验证消费组名字非法时
// 返回 ILLEGAL_CONSUMER_GROUP（这是该 Code 本身的定义：Format of consumer group
// is illegal），与内部存储故障区分开。
func TestHeartbeatMalformedGroupReturnsIllegalConsumerGroup(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.Heartbeat(context.Background(), &pb.HeartbeatRequest{
		Group: &pb.Resource{Name: "bad/name"},
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_ILLEGAL_CONSUMER_GROUP {
		t.Fatalf("status: 期望 ILLEGAL_CONSUMER_GROUP，得到 %v", resp.GetStatus())
	}
}

// TestShutdownEndsTelemetryStream 锁定 Server.Shutdown 的核心作用：一条已经
// 建立、客户端既不发也不关的 Telemetry 流，必须在服务端调用 Shutdown 后结束。
//
// 这条断言的由来是 Task 13 e2e 实测到的停机故障：Telemetry 是没有自然终点的
// 双向长流，官方 Go SDK 的 Client.GracefulStop() 又不会关闭它，于是
// grpc.Server.GracefulStop() 永远等不到这条流结束——接一个 producer 时 sq
// 停机要 9.5s，再接一个 SimpleConsumer 之后就再也停不下来（实测 30s 上限
// 兜底才退出）。修复后同样场景 0.04s 退出。
//
// 若哪天有人把 Telemetry 的读循环改回裸 stream.Recv()，本用例会挂在
// stream.Recv() 上直到测试超时——这正是我们要防的回归。
func TestShutdownEndsTelemetryStream(t *testing.T) {
	srv, c := newTestServerAndClient(t, true)
	stream, err := c.Telemetry(context.Background())
	if err != nil {
		t.Fatalf("Telemetry: %v", err)
	}
	// 先完成一次握手，确保服务端 handler 确实已经进入读循环。
	ct := pb.ClientType_PRODUCER
	if err := stream.Send(&pb.TelemetryCommand{Command: &pb.TelemetryCommand_Settings{
		Settings: &pb.Settings{ClientType: &ct, PubSub: &pb.Settings_Publishing{Publishing: &pb.Publishing{}}},
	}}); err != nil {
		t.Fatalf("send settings: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("首次协商回包: %v", err)
	}

	// 客户端此后既不发也不关（模拟官方 SDK 停机后仍挂着的那条流）。
	srv.Shutdown()

	done := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		done <- err
	}()
	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("流应以 io.EOF 正常结束，实际 %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown 后 telemetry 流仍未结束——服务端停机会被它无限期挂住")
	}
}
