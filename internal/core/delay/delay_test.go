// delay 调度器测试。到期条目无法经 AppendDelay 制造（它对已过期时间直通立即
// 投递），因此直接向 store 写 delay 条目造数据——与 retention_test 注入旧
// StoreAtMs 的手法同理。
package delay

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

type fixture struct {
	st *store.Store
	pr *produce.Producer
	dl *deliver.Deliverer
	sc *Scheduler
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(st, mt, slog.Default())
	dl := deliver.New(st, mt, pr, slog.Default())
	return &fixture{st: st, pr: pr, dl: dl, sc: New(st, pr, slog.Default())}
}

// putDelay 直接向暂存区写一条到期条目（绕过 AppendDelay 的直通逻辑）
func (f *fixture) putDelay(t *testing.T, seq uint64, dueMs int64, m *core.Message) {
	t.Helper()
	m.DeliverAtMs = dueMs
	raw, err := core.EncodeMessage(m)
	if err != nil {
		t.Fatal(err)
	}
	b := f.st.NewBatch()
	b.Set(store.DelayKey(dueMs, seq), raw, nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) delayCount(t *testing.T) int {
	t.Helper()
	n := 0
	pfx := []byte(store.DelayPrefix)
	if err := f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPassMovesDueAndPreservesMessage(t *testing.T) {
	f := newFixture(t)
	past := time.Now().Add(-time.Second).UnixMilli()
	f.putDelay(t, 0, past, &core.Message{ID: "m1", Topic: "t", Body: []byte("hello"),
		Keys: []string{"k1"}, Tag: "tg", BornAtMs: 123})
	moved, err := f.sc.Pass()
	if err != nil || moved != 1 {
		t.Fatalf("Pass: moved=%d err=%v", moved, err)
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatalf("delay 条目未删除: %d", n)
	}
	// 经正常投递链路可消费，DeliverAtMs/Tag/Keys 完整保留
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("到期消息应可投递: %d %v", len(msgs), err)
	}
	got := msgs[0]
	if got.ID != "m1" || string(got.Body) != "hello" || got.DeliverAtMs != past ||
		got.Tag != "tg" || got.BornAtMs != 123 {
		t.Fatalf("消息字段丢失: %+v", got)
	}
	// Keys 索引在移入时由 AppendWith 顺带写入
	kpfx := store.KeyIdxKeyPrefix("t", "k1")
	found := 0
	f.st.Scan(kpfx, store.PrefixUpperBound(kpfx), 0, func(k, v []byte) (bool, error) { found++; return true, nil })
	if found != 1 {
		t.Fatalf("keyidx 未写入: %d", found)
	}
}

func TestPassLeavesNotDueEntries(t *testing.T) {
	f := newFixture(t)
	f.putDelay(t, 0, time.Now().Add(time.Hour).UnixMilli(), &core.Message{ID: "m1", Topic: "t", Body: []byte("x")})
	moved, err := f.sc.Pass()
	if err != nil || moved != 0 {
		t.Fatalf("未到期不应移动: %d %v", moved, err)
	}
	if n := f.delayCount(t); n != 1 {
		t.Fatalf("未到期条目不应消失: %d", n)
	}
}

func TestPassDeletesCorruptEntryInsteadOfWedging(t *testing.T) {
	f := newFixture(t)
	b := f.st.NewBatch()
	b.Set(store.DelayKey(time.Now().Add(-time.Second).UnixMilli(), 0), []byte("not-json"), nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
	moved, err := f.sc.Pass()
	if err != nil || moved != 0 {
		t.Fatalf("坏条目不算移动也不算错: %d %v", moved, err)
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatal("坏条目应被删除（否则每 100ms 重扫一次，永久日志洪水）")
	}
}

func TestPassRespectsBudgetAndDrains(t *testing.T) {
	oldBudget := maxMovePerPass
	maxMovePerPass = 2
	defer func() { maxMovePerPass = oldBudget }()
	f := newFixture(t)
	past := time.Now().Add(-time.Second).UnixMilli()
	for i := uint64(0); i < 5; i++ {
		f.putDelay(t, i, past, &core.Message{ID: string(rune('a'+i)), Topic: "t", Body: []byte("x")})
	}
	// 单趟受预算限制
	moved, err := f.sc.Pass()
	if err != nil || moved != 2 {
		t.Fatalf("首趟应恰好移动预算数: %d %v", moved, err)
	}
	// 连续 Pass 可排空
	total := moved
	for total < 5 {
		n, err := f.sc.Pass()
		if err != nil || n == 0 {
			t.Fatalf("排空中断: n=%d total=%d err=%v", n, total, err)
		}
		total += n
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatalf("排空后仍剩 %d 条", n)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.sc.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未在 ctx 取消后退出")
	}
}
