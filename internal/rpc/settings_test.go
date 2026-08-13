// Settings 协商的分档下发测试。
//
// 职责：钉住「BackoffPolicy 的语义由客户端类型决定」这条不变式——两类客户端
// 的 MaxAttempts 必须分别取自各自的语义来源（发布端是 RPC 重试次数常量，
// 订阅端是消费组的真实 DLQ 判据）。
// 边界：不验证 SDK 侧如何消费这些值（那是 e2e 的事），只验证服务端下发了什么。
package rpc

import (
	"context"
	"log/slog"
	"testing"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/replication"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
	"github.com/xushixin/sq/internal/store"
)

// newSettingsServer 造一个只够跑 negotiateSettings 的最小 Server。
//
// 不复用 newTestEnv 的理由：negotiateSettings 只碰 s.cfg / s.mt / s.logger，
// 而 newTestEnv 会起 gRPC 服务与全套引擎，且把 defaultMaxAttempts 固定成一个
// 值——T3/T4 恰恰需要一个非默认值。这里的差异用参数表达得更直接。
//
// 参数：
//   - defaultMaxAttempts: 同时写进 cfg.DefaultMaxAttempts 与 meta.New 的同名
//     参数。**两者必须同源**：生产环境里它们本来就来自同一个配置项
//     （cmd/sq/main.go 把 cfg.DefaultMaxAttempts 传给 meta.New），夹具里分开
//     设会造出一个现实中不存在的状态，让 T4 的回退断言失去意义。
//   - cluster: true 时把配置切到集群档（验证发布端的集群档取值）
func newSettingsServer(t *testing.T, defaultMaxAttempts int32, cluster bool) *Server {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	rep := replication.NewStandalone(st)
	mt, err := meta.New(rep, replication.StandaloneRouter{}, st, true, 4, defaultMaxAttempts, slog.Default())
	if err != nil {
		t.Fatalf("meta.New: %v", err)
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	cfg.DefaultMaxAttempts = defaultMaxAttempts
	if cluster {
		// 切集群档。判据就是「Cluster 段非 nil」（config.ClusterEnabled() 的
		// 实现即 c.Cluster != nil），所以给一个零值段就够——negotiateSettings
		// 只经 ClusterEnabled() 读它，不碰段内任何字段。
		cfg.Cluster = &config.ClusterConfig{}
	}
	return &Server{cfg: cfg, mt: mt, logger: slog.Default().With("mod", "rpc")}
}

func publishSettings() *pb.Settings {
	ct := pb.ClientType_PRODUCER
	return &pb.Settings{
		ClientType: &ct,
		PubSub:     &pb.Settings_Publishing{Publishing: &pb.Publishing{}},
	}
}

func subscribeSettings(group string) *pb.Settings {
	ct := pb.ClientType_PUSH_CONSUMER
	return &pb.Settings{
		ClientType: &ct,
		PubSub: &pb.Settings_Subscription{Subscription: &pb.Subscription{
			Group: &pb.Resource{Name: group},
		}},
	}
}

func expBackoff(t *testing.T, p *pb.RetryPolicy) *pb.ExponentialBackoff {
	t.Helper()
	if p == nil {
		t.Fatalf("BackoffPolicy 为 nil")
	}
	eb, ok := p.GetStrategy().(*pb.RetryPolicy_ExponentialBackoff)
	if !ok {
		t.Fatalf("策略不是 ExponentialBackoff：%T", p.GetStrategy())
	}
	return eb.ExponentialBackoff
}

// T1 发布端回归：拆分不得改变发布端拿到的任何一个字段。
func TestNegotiateSettingsPublishBackoffUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cluster     bool
		wantMaxMs   int64
		wantAttempt int32
	}{
		{"单机档", false, 1000, 3},
		{"集群档", true, 3000, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSettingsServer(t, 16, tc.cluster)
			out := s.negotiateSettings(publishSettings())
			p := out.GetBackoffPolicy()
			if got := p.GetMaxAttempts(); got != tc.wantAttempt {
				t.Fatalf("MaxAttempts=%d，期望 %d", got, tc.wantAttempt)
			}
			eb := expBackoff(t, p)
			if got := eb.GetInitial().AsDuration().Milliseconds(); got != 100 {
				t.Fatalf("Initial=%dms，期望 100ms", got)
			}
			if got := eb.GetMax().AsDuration().Milliseconds(); got != tc.wantMaxMs {
				t.Fatalf("Max=%dms，期望 %dms", got, tc.wantMaxMs)
			}
			if got := eb.GetMultiplier(); got != 2 {
				t.Fatalf("Multiplier=%v，期望 2", got)
			}
		})
	}
}

// T2【承重】消费端的退避三要素必须与发布端**逐字段相同**。
//
// 这条钉住 spec §3.2 的「刻意保持」。本条目初稿曾主张把它们改成 10s / 5min
// 以「镜像 broker 的 retryBackoff」，那个论据是错的（spec §2.2）：SDK 消费失败
// 走 ChangeInvisibleDuration，broker 原样透传不设下限，改了就是把 push 重投从
// 100ms 拉到 10s、顺序消息最坏队头阻塞 5min。
//
// 所以两个分支里写着同样的三行常量**不是可合并的重复代码**——这条用例就是拦住
// 「顺手统一一下」的那道闸。
func TestNegotiateSettingsSubscribeBackoffMatchesPublish(t *testing.T) {
	s := newSettingsServer(t, 16, false)
	pub := expBackoff(t, s.negotiateSettings(publishSettings()).GetBackoffPolicy())
	sub := expBackoff(t, s.negotiateSettings(subscribeSettings("g-unknown")).GetBackoffPolicy())

	if got, want := sub.GetInitial().AsDuration(), pub.GetInitial().AsDuration(); got != want {
		t.Fatalf("Initial=%v，必须与发布端相同（%v）——见 spec §2.2，改它是行为退化", got, want)
	}
	if got, want := sub.GetMax().AsDuration(), pub.GetMax().AsDuration(); got != want {
		t.Fatalf("Max=%v，必须与发布端相同（%v）——见 spec §2.2", got, want)
	}
	if got, want := sub.GetMultiplier(), pub.GetMultiplier(); got != want {
		t.Fatalf("Multiplier=%v，必须与发布端相同（%v）", got, want)
	}
}

// T3 MaxAttempts 取消费组的真实值。
func TestNegotiateSettingsSubscribeMaxAttemptsFromGroup(t *testing.T) {
	s := newSettingsServer(t, 5, false)
	if _, err := s.mt.EnsureGroup(context.Background(), "g-real"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	out := s.negotiateSettings(subscribeSettings("g-real"))
	if got := out.GetBackoffPolicy().GetMaxAttempts(); got != 5 {
		t.Fatalf("MaxAttempts=%d，期望取组真实值 5", got)
	}
}

// T4 组尚未注册时回退 cfg.DefaultMaxAttempts，且协商不得产生建组副作用。
//
// 夹具传 5 而不是 16：用包常量 meta.DefaultMaxAttempts（=16）实现回退时，
// 这条用例会红——这正是它要挡的错误。
func TestNegotiateSettingsSubscribeFallsBackToConfigDefault(t *testing.T) {
	s := newSettingsServer(t, 5, false)
	out := s.negotiateSettings(subscribeSettings("g-never-created"))
	if got := out.GetBackoffPolicy().GetMaxAttempts(); got != 5 {
		t.Fatalf("MaxAttempts=%d，期望回退 cfg.DefaultMaxAttempts=5（用包常量 %d 就会红）",
			got, meta.DefaultMaxAttempts)
	}
	// 承重：协商是只读的。用了 EnsureGroup 就会在这里露出来。
	if _, ok := s.mt.GetGroup("g-never-created"); ok {
		t.Fatalf("协商把组建出来了——negotiateSettings 必须只读")
	}
}

// T5【承重】两个分支下发的 BackoffPolicy 必须不同。
//
// 这是本条目的核心断言：把赋值合并回公共段是本次改动唯一可能的回退方向，
// 这条用例把它变成红灯。差异点在 MaxAttempts——夹具给 7、组也建出来，
// 于是订阅端 7、发布端 3（单机档常量），必然不等。
func TestNegotiateSettingsBranchesDiffer(t *testing.T) {
	s := newSettingsServer(t, 7, false)
	if _, err := s.mt.EnsureGroup(context.Background(), "g-x"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	pub := s.negotiateSettings(publishSettings()).GetBackoffPolicy()
	sub := s.negotiateSettings(subscribeSettings("g-x")).GetBackoffPolicy()

	if pub.GetMaxAttempts() == sub.GetMaxAttempts() {
		t.Fatalf("两类客户端拿到了相同的 MaxAttempts(%d)：赋值又被合并回公共段了",
			pub.GetMaxAttempts())
	}
}

// T6 客户端未上报可识别的 PubSub 时，PubSub 与 BackoffPolicy 一并留空。
// 这与该分支既有的设计一致：协商注定失败，服务端不凭空捏造一个分支去掩盖它；
// 既然连客户端类型都判不出来，退避策略的语义也就无从选择。
func TestNegotiateSettingsNoPubSubLeavesBackoffEmpty(t *testing.T) {
	s := newSettingsServer(t, 16, false)
	ct := pb.ClientType_CLIENT_TYPE_UNSPECIFIED
	out := s.negotiateSettings(&pb.Settings{ClientType: &ct})
	if out.GetPubSub() != nil {
		t.Fatalf("PubSub 应为空，实际 %T", out.GetPubSub())
	}
	if out.GetBackoffPolicy() != nil {
		t.Fatalf("BackoffPolicy 应为空，实际 %+v", out.GetBackoffPolicy())
	}
}

// T7【承重】组已注册后配置默认值发生漂移时，必须下发组落盘的值。
//
// 这条是本条目唯一能区分『查了组』与『只回退配置默认』的用例：T3/T5 的夹具
// 里两者恒等（EnsureGroup 写的就是 meta.New 收到的 defaultMaxAttempts，而
// 夹具让它与 cfg.DefaultMaxAttempts 同源），所以把实现退化成不查组也能全绿。
// 这里让组先在 9 下注册，再把配置默认改成 5，制造出二者的差异。
func TestNegotiateSettingsSubscribePrefersGroupOverConfigDrift(t *testing.T) {
	s := newSettingsServer(t, 9, false)
	if _, err := s.mt.EnsureGroup(context.Background(), "g-drift"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	// 组已按 9 落盘；此后运维把配置默认值改成 5（重启后 cfg 变、组不变）。
	s.cfg.DefaultMaxAttempts = 5
	if got := s.negotiateSettings(subscribeSettings("g-drift")).GetBackoffPolicy().GetMaxAttempts(); got != 9 {
		t.Fatalf("MaxAttempts=%d，期望组落盘值 9；拿到 5 说明实现没查组、直接回退了 cfg.DefaultMaxAttempts", got)
	}
}
