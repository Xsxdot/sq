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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
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
	reg := NewRegistry(st, mt, slog.Default())
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
