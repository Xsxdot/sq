// metrics 测试：走真实 produce/deliver 制造状态，断言 Collect 推导值与
// Prometheus 文本输出。
//
// 不用 t.Parallel()：NewRegistry 会设置包级钩子 store.OnApplyObserve，并行测试
// 会互相覆盖该钩子（且本包测试本就 cheap，无并行收益）。
package metrics

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
	"github.com/xushixin/sq/internal/sysinfo"
)

func fixture(t *testing.T) (*store.Store, *meta.Meta, *produce.Producer, *deliver.Deliverer) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(st, mt, slog.Default())
	return st, mt, pr, deliver.New(st, mt, pr, slog.Default())
}

func TestCollectDerivesStats(t *testing.T) {
	st, mt, pr, dl := fixture(t)
	for i := 0; i < 3; i++ {
		if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	// 消费 1 条不 ack：cursor=1、inflight=1、待拉取=2
	if _, err := dl.Receive(context.Background(), "g1", "t1", 0, 1, time.Minute, 0, nil); err != nil {
		t.Fatal(err)
	}
	s, err := Collect(st, mt)
	if err != nil {
		t.Fatal(err)
	}
	if s.Topics != 1 || s.Groups != 1 {
		t.Fatalf("topics/groups 不符: %+v", s)
	}
	if s.Written["t1"] != 3 {
		t.Fatalf("t1 写入总量应为 3: %v", s.Written)
	}
	gt := GroupTopic{Group: "g1", Topic: "t1"}
	if s.Pending[gt] != 2 {
		t.Fatalf("待拉取应为 2: %v", s.Pending)
	}
	if s.Inflight[gt] != 1 {
		t.Fatalf("inflight 应为 1: %v", s.Inflight)
	}
	if s.DelayDepth != 0 {
		t.Fatalf("delay 深度应为 0: %d", s.DelayDepth)
	}
}

func TestRegistryExposesMetrics(t *testing.T) {
	st, mt, pr, _ := fixture(t)
	if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry(st, mt, sysinfo.New(t.TempDir(), 0, &atomic.Bool{}, slog.Default()), slog.Default())
	// Append 走过 store.Apply，直方图应已有样本；写入 counter 应为 1
	got, err := testutil.GatherAndCount(reg, "sq_topic_messages_written_total", "sq_store_apply_duration_seconds")
	if err != nil || got == 0 {
		t.Fatalf("指标缺失: n=%d err=%v", got, err)
	}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP sq_topic_messages_written_total topic 累计写入消息条数（各队列 offset 计数器之和）
# TYPE sq_topic_messages_written_total counter
sq_topic_messages_written_total{topic="t1"} 1
`), "sq_topic_messages_written_total"); err != nil {
		t.Fatalf("written_total 输出不符: %v", err)
	}
}

// TestRegistryExposesDiskAndWriteBlocked 钉住磁盘与拒写状态进入 /metrics。
// 这是 M5 的欠账：拒写会让生产端全挂，却没有任何指标可供告警。
func TestRegistryExposesDiskAndWriteBlocked(t *testing.T) {
	st, mt, _, _ := fixture(t)
	blocked := &atomic.Bool{}
	sys := sysinfo.New(t.TempDir(), 85, blocked, slog.Default())
	reg := NewRegistry(st, mt, sys, slog.Default())

	names := gatherNames(t, reg)
	for _, want := range []string{"sq_write_blocked", "sq_data_dir_bytes", "sq_disk_used_percent", "sq_disk_free_bytes"} {
		if !names[want] {
			t.Fatalf("/metrics 应包含 %s，实际有 %v", want, names)
		}
	}
	if v := gatherValue(t, reg, "sq_write_blocked"); v != 0 {
		t.Fatalf("未拒写时 sq_write_blocked 应为 0，得到 %v", v)
	}
	blocked.Store(true)
	if v := gatherValue(t, reg, "sq_write_blocked"); v != 1 {
		t.Fatalf("拒写时 sq_write_blocked 应为 1，得到 %v", v)
	}
}

// gatherNames 收集 registry 当前产出的全部指标名。
func gatherNames(t *testing.T, reg *prometheus.Registry) map[string]bool {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather 失败: %v", err)
	}
	out := map[string]bool{}
	for _, mf := range mfs {
		out[mf.GetName()] = true
	}
	return out
}

// gatherValue 取一个无标签 gauge 的当前值。
func gatherValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather 失败: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("未找到指标 %s", name)
	return 0
}

// TestSystemMetricsSurviveStoreFailure 钉住系统指标与业务指标的产出顺序。
//
// 业务采集失败时 Collect 会直接 return——如果系统指标挂在它后面，磁盘满导致
// store 写不下去的那一刻，sq_write_blocked 会一起从 /metrics 消失，告警侧看到
// absent() 而不是 1。而那恰恰是最需要这个指标的时刻。
func TestSystemMetricsSurviveStoreFailure(t *testing.T) {
	// 不用 fixture：它注册了 t.Cleanup 关 store，而本用例自己就要提前关掉，
	// pebble 二次 Close 会 panic
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(st, mt, slog.Default())
	if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	blocked := &atomic.Bool{}
	blocked.Store(true)
	sys := sysinfo.New(t.TempDir(), 85, blocked, slog.Default())
	reg := NewRegistry(st, mt, sys, slog.Default())

	// 关掉 store，让业务采集必然失败（Collect 里的 st.Get/Scan 会报 ErrClosed）
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// 这里不能用 gatherNames：Collect 上报的 invalid metric 会让 Gather 返回
	// 非 nil error，但仍带回全部有效的 metric family——要断言的正是「错误发生时
	// 哪些指标还在」，所以只取 mfs、不因 err 提前失败
	mfs, err := reg.Gather()
	if err == nil {
		t.Fatal("store 已关闭，业务采集应报错——用例前提不成立")
	}
	names := map[string]bool{}
	var blockedVal float64
	for _, mf := range mfs {
		names[mf.GetName()] = true
		if mf.GetName() == "sq_write_blocked" {
			blockedVal = mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	if names["sq_topics"] {
		t.Fatal("store 已关闭，业务指标不该产出——用例前提不成立")
	}
	if !names["sq_write_blocked"] {
		t.Fatalf("业务采集失败时 sq_write_blocked 仍必须产出，实际有 %v", names)
	}
	if blockedVal != 1 {
		t.Fatalf("拒写中应为 1，得到 %v", blockedVal)
	}
}
