// server_test.go 验证 rpc.Server 的 gRPC 服务基座，并提供本包全部 gRPC 测试
// 共用的环境搭建 helper（newTestEnv）。
//
// 职责：
//   - newTestEnv：起全真组件 + bufconn 上的真 gRPC server/client
//   - 用它覆盖 QueryRoute 自动建 topic、Heartbeat 注册消费组、
//     Telemetry settings 协商三条主链路
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
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Xsxdot/sq/internal/config"
	"github.com/Xsxdot/sq/internal/core/delay"
	"github.com/Xsxdot/sq/internal/core/deliver"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/core/txn"
	"github.com/Xsxdot/sq/internal/replication"
	pb "github.com/Xsxdot/sq/internal/rpc/pb/apache/rocketmq/v2"
	"github.com/Xsxdot/sq/internal/store"
)

// testEnv 是一套完整的被测环境：真实的 Pebble store（临时目录，测试结束自动
// 清理）、meta/produce/deliver 三个引擎、bufconn 上的真 gRPC server，以及连着
// 它的 client stub。三个字段按测试关心的角度各取所需——
//   - client：绝大多数用例只需要它
//   - srv：停机行为（Shutdown 让 Telemetry 长流收尾）只能从服务端这一侧触发
//   - dl：需要绕开协议层、直接查"盘上到底有没有这条消息"时用
//   - st：需要绕开协议层直接查盘上状态时用——延时用例查 delay/ 前缀
type testEnv struct {
	srv     *Server
	client  pb.MessagingServiceClient
	dl      *deliver.Deliverer
	blocked *atomic.Bool
	st      *store.Store
}

// newTestEnv 起一套测试环境（单机形态：staticRouteView + 真实 Standalone
// 复制后端）。autoCreate 决定未知 topic 是否自动创建；opts 追加
// 到 grpc.NewServer 上，供需要特定传输层配置的用例使用（如放宽 MaxRecvMsgSize
// 好让超限请求真正到达应用层校验，或按生产装配的同款 Option 验证配置本身够用）。
//
// 客户端一侧的收发上限刻意放到最大：本包每个用例断言的都是**服务端**的行为，
// 客户端 stub 只是运输工具。若它自己也带着限制，一条本该由服务端裁决的请求
// 可能在客户端就被拦下，测试挂在一个与被测对象无关的地方。
//
// 全包只此一个搭建入口：曾经三个测试文件各写了一份几乎相同的搭建代码，仅在
// ServerOption 与返回值上有差别，且各自都留了一段"为什么不复用另一份"的注释。
// 到第三份时这个理由已经不成立——差异全部可以用参数表达。
func newTestEnv(t *testing.T, autoCreate bool, opts ...grpc.ServerOption) testEnv {
	return newTestEnvWith(t, autoCreate, nil, nil, opts...)
}

// newTestEnvWith 是 newTestEnv 的全参形态：rv 为 nil 时用 staticRouteView
// （单机形态），rep 为 nil 时用真实 Standalone 复制后端。集群协议面用例
// （路由指向 leader、follower 快速失败、ErrNotLeader 映射）注入两者。
func newTestEnvWith(t *testing.T, autoCreate bool, rv RouteView, rep replication.Replicator, opts ...grpc.ServerOption) testEnv {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	if rep == nil {
		rep = replication.NewStandalone(st)
	}
	rt := replication.StandaloneRouter{}
	mt, err := meta.New(rep, rt, st, autoCreate, 4, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta.New: %v", err)
	}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	dl := deliver.New(rep, rt, st, mt, pr, slog.Default())
	ds := delay.New(rep, rt, st, pr, slog.Default())
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	cfg.AutoCreateTopic = autoCreate // cfg 与 meta 的 autoCreate 必须一致，预检（send.go 第零遍）读的是 cfg
	if rv == nil {
		rv = staticRouteView{cfg: cfg}
	}
	blocked := &atomic.Bool{}
	// txn 管理器与生产装配同参数（30s 首查间隔、15 次上限，见 config 默认值）
	tx := txn.New(rep, rt, st, pr, mt, 30*time.Second, 15, slog.Default())
	srv := New(cfg, rv, mt, pr, dl, ds, tx, blocked, []byte("test-handle-secret"), slog.Default())

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(opts...)
	srv.Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32),
		),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return testEnv{srv: srv, client: pb.NewMessagingServiceClient(conn), dl: dl, blocked: blocked, st: st}
}

// newTestClient 是 newTestEnv 最常用形态的简写：autoCreate=true、无额外
// ServerOption，只要客户端 stub。
func newTestClient(t *testing.T) pb.MessagingServiceClient {
	t.Helper()
	return newTestEnv(t, true).client
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
// 这条断言的由来是一次真实的互操作故障（用官方 SDK 跑 e2e 时暴露）：服务端最初把客户端
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
	// Fifo/ReceiveBatchSize/LongPollingTimeout 都是 proto3 optional（指针字段），
	// 必须先断言指针非 nil 再断言取值：Getter 对 nil 返回零值，光看
	// GetFifo()==false 分不清「显式下发了 false」和「压根没下发」这两种情况，
	// 而后者正是这条用例要防的回归——字段被漏填时 Getter 依然一片祥和。
	if sub.Subscription.Fifo == nil {
		t.Fatal("fifo 必须显式下发（当前为 nil，客户端只能靠默认值猜）")
	}
	if *sub.Subscription.Fifo {
		t.Fatal("M4 起顺序由 broker 端强制（顺序锁），fifo 协商标志待 push 消费流程验证后（M5+）再翻转，当前保持 false")
	}
	if sub.Subscription.ReceiveBatchSize == nil {
		t.Fatal("receive_batch_size 必须显式下发：漏填时 push 消费者拿到的批量大小是 0")
	}
	if got := sub.Subscription.GetReceiveBatchSize(); got != pushReceiveBatchSize {
		t.Fatalf("receive_batch_size 应为 %d，实际 %d", pushReceiveBatchSize, got)
	}
	if sub.Subscription.LongPollingTimeout == nil {
		t.Fatal("long_polling_timeout 必须显式下发（当前为 nil）")
	}
	if got := sub.Subscription.GetLongPollingTimeout().AsDuration(); got != defaultLongPolling {
		t.Fatalf("长轮询上限应下发服务端真实值 %v，实际 %v", defaultLongPolling, got)
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
	c := newTestEnv(t, false).client
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
// 这条断言的由来是一次用官方 SDK 实测到的停机故障：Telemetry 是没有自然终点的
// 双向长流，官方 Go SDK 的 Client.GracefulStop() 又不会关闭它，于是
// grpc.Server.GracefulStop() 永远等不到这条流结束——接一个 producer 时 sq
// 停机要 9.5s，再接一个 SimpleConsumer 之后就再也停不下来（实测 30s 上限
// 兜底才退出）。修复后同样场景 0.04s 退出。
//
// 若哪天有人把 Telemetry 的读循环改回裸 stream.Recv()，本用例会挂在
// stream.Recv() 上直到测试超时——这正是我们要防的回归。
func TestShutdownEndsTelemetryStream(t *testing.T) {
	env := newTestEnv(t, true)
	srv, c := env.srv, env.client
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
