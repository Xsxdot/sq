// routeview_test.go 验证协议面的集群感知行为：路由指向组 leader、follower
// 快速失败、ErrNotLeader 映射与集群档退避参数。
//
// 职责：
//   - 注入假 RouteView：端点按队列指向不同节点，leader 判定可配置
//   - 覆盖 QueryRoute/QueryAssignment 的「队列→leader」展开与 leader 未知
//     （选举窗口）时整包 HA_NOT_AVAILABLE
//   - 覆盖 SendMessage/ReceiveMessage 在 follower 上的入口快速失败
//   - 覆盖 topicErrStatus 对 replication.ErrNotLeader 的 HA_NOT_AVAILABLE 映射
//   - 覆盖集群档退避参数（3s × 5 次）
//
// 边界：
//   - 只测协议适配层的行为（请求→响应/状态码），不重复测 cluster/raft 本身
//     （那是 cluster 包的测试职责）
package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/replication"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
	"github.com/xushixin/sq/internal/store"
)

// fakeRouteView 测试用假 RouteView：端点解析与 leader 判定都可注入。
type fakeRouteView struct {
	queueEndpoint func(topic string, queueID uint32) (host string, port int32, brokerName string, ok bool)
	selfLeader    bool
}

func (f fakeRouteView) QueueEndpoint(topic string, queueID uint32) (string, int32, string, bool) {
	if f.queueEndpoint != nil {
		return f.queueEndpoint(topic, queueID)
	}
	return "", 0, "", false
}

func (f fakeRouteView) SelfIsLeader(string, uint32) bool { return f.selfLeader }
func (f fakeRouteView) MetaIsLeader() bool               { return f.selfLeader }

// errInjectingReplicator 测试用假 Replicator：一切提交返回注入的错误，用于在
// 协议层之下注入 ErrNotLeader（等价于真实集群里 follower 的提案路径）。
type errInjectingReplicator struct{ err error }

func (f errInjectingReplicator) Apply(context.Context, uint32, *store.Batch) error { return f.err }

func (f errInjectingReplicator) ApplyAsync(context.Context, uint32, *store.Batch) (replication.Pending, error) {
	return nil, f.err
}

// TestQueryRoutePointsQueuesAtGroupLeaders 注入假 RouteView：队列 0→节点A、
// 队列 1→节点B → 断言响应里两条 MessageQueue 的 broker.endpoints 各指其主，
// broker.Name 分别为 sq1/sq2。
func TestQueryRoutePointsQueuesAtGroupLeaders(t *testing.T) {
	rv := fakeRouteView{
		queueEndpoint: func(_ string, queueID uint32) (string, int32, string, bool) {
			if queueID == 1 {
				return "10.0.0.2", 8082, "sq2", true
			}
			return "10.0.0.1", 8081, "sq1", true
		},
	}
	c := newTestEnvWith(t, true, rv, nil).client
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{
		Topic: &pb.Resource{Name: "routed"},
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
	mq0 := resp.GetMessageQueues()[0]
	if mq0.GetBroker().GetName() != "sq1" {
		t.Fatalf("队列 0 的 broker 名应为 sq1，实际 %q", mq0.GetBroker().GetName())
	}
	addr0 := mq0.GetBroker().GetEndpoints().GetAddresses()
	if len(addr0) != 1 || addr0[0].GetHost() != "10.0.0.1" || addr0[0].GetPort() != 8081 {
		t.Fatalf("队列 0 应指向节点 A(10.0.0.1:8081)，实际 %v", addr0)
	}
	mq1 := resp.GetMessageQueues()[1]
	if mq1.GetBroker().GetName() != "sq2" {
		t.Fatalf("队列 1 的 broker 名应为 sq2，实际 %q", mq1.GetBroker().GetName())
	}
	addr1 := mq1.GetBroker().GetEndpoints().GetAddresses()
	if len(addr1) != 1 || addr1[0].GetHost() != "10.0.0.2" || addr1[0].GetPort() != 8082 {
		t.Fatalf("队列 1 应指向节点 B(10.0.0.2:8082)，实际 %v", addr1)
	}
}

// TestQueryRouteLeaderUnknownReturnsHANotAvailable 队列 leader 未知（选举
// 窗口）时，QueryRoute 必须整包回 HA_NOT_AVAILABLE——SDK 对非 OK 码的
// 处理是立即重试 + 隔离本端点 + 轮换候选队列，3 次尝试跨 3 个候选队列
// 足以撞上健康 leader（选举完成后的任意节点都能给出完整路由）。
func TestQueryRouteLeaderUnknownReturnsHANotAvailable(t *testing.T) {
	rv := fakeRouteView{
		queueEndpoint: func(string, uint32) (string, int32, string, bool) { return "", 0, "", false },
	}
	c := newTestEnvWith(t, true, rv, nil).client
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{
		Topic: &pb.Resource{Name: "routed-election"},
	})
	if err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_HA_NOT_AVAILABLE {
		t.Fatalf("leader 未知时应回 HA_NOT_AVAILABLE，得到 %v", resp.GetStatus())
	}
}

// TestSendOnNonLeaderReturnsHANotAvailable 假 RouteView SelfIsLeader=false +
// 复制层注入 ErrNotLeader → SendMessage 状态码 == Code_HA_NOT_AVAILABLE。
//
// 两条路径都必须覆盖：入口快速失败（follower 探测）与 propose 报
// ErrNotLeader 的映射（探针放行但实际提交落在非 leader 组的场景——协议层
// 看不到 produce 内部选队结果，这条兜底映射是正确性的保证）。
func TestSendOnNonLeaderReturnsHANotAvailable(t *testing.T) {
	req := func() *pb.SendMessageRequest {
		return &pb.SendMessageRequest{
			Messages: []*pb.Message{{
				Topic:            &pb.Resource{Name: "t-send-nonleader"},
				SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
				Body:             []byte("x"),
			}},
		}
	}
	t.Run("入口快速失败", func(t *testing.T) {
		rv := fakeRouteView{selfLeader: false}
		c := newTestEnvWith(t, true, rv, nil).client
		resp, err := c.SendMessage(context.Background(), req())
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		if resp.GetStatus().GetCode() != pb.Code_HA_NOT_AVAILABLE {
			t.Fatalf("follower 上发送应快速失败为 HA_NOT_AVAILABLE，得到 %v", resp.GetStatus())
		}
	})
	t.Run("propose 报 ErrNotLeader 映射", func(t *testing.T) {
		rv := fakeRouteView{selfLeader: true}
		rep := errInjectingReplicator{
			err: fmt.Errorf("%w: 模拟 follower 提案被拒", replication.ErrNotLeader),
		}
		c := newTestEnvWith(t, true, rv, rep).client
		resp, err := c.SendMessage(context.Background(), req())
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		if resp.GetStatus().GetCode() != pb.Code_HA_NOT_AVAILABLE {
			t.Fatalf("ErrNotLeader 应映射为 HA_NOT_AVAILABLE，得到 %v", resp.GetStatus())
		}
	})
}

// TestReceiveOnNonLeaderFailsFastWithoutLongPoll follower 上 ReceiveMessage
// 必须立即返回 HA_NOT_AVAILABLE，耗时 << 长轮询时长（断言 <1s）。
// 回归场景：没有入口快速失败时，follower 会安静长轮询 20s 后回
// MESSAGE_NOT_FOUND——消费者停在一条死路由上毫无线索。
func TestReceiveOnNonLeaderFailsFastWithoutLongPoll(t *testing.T) {
	rv := fakeRouteView{selfLeader: false}
	c := newTestEnvWith(t, true, rv, nil).client
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group: &pb.Resource{Name: "g-follower"},
		MessageQueue: &pb.MessageQueue{
			Topic: &pb.Resource{Name: "t-follower"},
			Id:    0,
		},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
		BatchSize:         10,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	elapsed := time.Since(start)
	msgs, st := recvAll(t, stream)
	if len(msgs) != 0 {
		t.Fatalf("follower 上不应投递消息，得到 %d 条", len(msgs))
	}
	if st == nil || st.GetCode() != pb.Code_HA_NOT_AVAILABLE {
		t.Fatalf("follower 上 ReceiveMessage 应回 HA_NOT_AVAILABLE，得到 %v", st)
	}
	if elapsed >= time.Second {
		t.Fatalf("follower 上 ReceiveMessage 应快速失败，耗时 %v（>=1s，疑似进入了长轮询）", elapsed)
	}
}

// TestClusterModeBackoffPolicy 集群档退避参数：封顶 3s、最多 5 次。
// 依据：选举窗口实测 1.5s 量级，单机档默认的 1s×3 次恰好可能全部落在
// 窗口内——每次重试都被 HA_NOT_AVAILABLE 弹回，3 次烧完还没等到 leader。
func TestClusterModeBackoffPolicy(t *testing.T) {
	env := newTestEnv(t, true)
	// 模拟集群档配置：config.Load("") 的 cfg 没有 Cluster 段，测试直接注入
	// （ClusterEnabled 只判空，语义与真实集群配置一致）。
	env.srv.cfg.Cluster = &config.ClusterConfig{}
	ct := pb.ClientType_PRODUCER
	got := telemetryNegotiate(t, env.client, &pb.Settings{
		ClientType: &ct,
		PubSub:     &pb.Settings_Publishing{Publishing: &pb.Publishing{}},
	})
	bp := got.GetBackoffPolicy()
	if bp.GetMaxAttempts() != backoffMaxAttemptsCluster {
		t.Fatalf("集群档 backoff max_attempts 应为 %d，实际 %d", backoffMaxAttemptsCluster, bp.GetMaxAttempts())
	}
	eb := bp.GetExponentialBackoff()
	if eb == nil {
		t.Fatalf("backoff 策略应为指数退避，实际 %v", bp.GetStrategy())
	}
	if eb.GetInitial().AsDuration() != backoffInitial || eb.GetMax().AsDuration() != backoffMaxCluster {
		t.Fatalf("集群档指数退避区间应为 %v→%v，实际 %v→%v",
			backoffInitial, backoffMaxCluster, eb.GetInitial().AsDuration(), eb.GetMax().AsDuration())
	}
}
