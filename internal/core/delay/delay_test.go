// delay 调度器测试。到期条目无法经 AppendDelay 制造（它对已过期时间直通立即
// 投递），因此直接向 store 写 delay 条目造数据——与 retention_test 注入旧
// StoreAtMs 的手法同理。
package delay

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
)

// fixedLeaderRouter 可编程 Router：IsLeader 按字段返回（leader-only 门控单测用）。
// 不实现 Forwarder——门控拦截后转发分支不可达，nil fwd 不会解引用。
type fixedLeaderRouter struct{ leader bool }

func (r fixedLeaderRouter) GroupForQueue(string, uint32) uint32       { return 0 }
func (r fixedLeaderRouter) MetaGroup() uint32                         { return 0 }
func (r fixedLeaderRouter) IsLeader(uint32) bool                      { return r.leader }
func (r fixedLeaderRouter) ReadBarrier(context.Context, uint32) error { return nil }

// flipRouter 可在测试中翻转 IsLeader 的 Router（Run 门控测试用）。
type flipRouter struct{ leader atomic.Bool }

func (r *flipRouter) GroupForQueue(string, uint32) uint32       { return 0 }
func (r *flipRouter) MetaGroup() uint32                         { return 0 }
func (r *flipRouter) IsLeader(uint32) bool                      { return r.leader.Load() }
func (r *flipRouter) ReadBarrier(context.Context, uint32) error { return nil }

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
	mt, err := meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	dl := deliver.New(rep, rt, st, mt, pr, slog.Default())
	return &fixture{st: st, pr: pr, dl: dl, sc: New(rep, rt, st, pr, slog.Default())}
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
	b.Set(store.DelayKey(dueMs, seq), raw)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

// msgCount 统计 msg/ 区的消息条数（两段式重放用例断言目标队列条数用）。
func (f *fixture) msgCount(t *testing.T) int {
	t.Helper()
	n := 0
	pfx := []byte("msg/")
	if err := f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	return n
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
	moved, err := f.sc.Pass(context.Background())
	if err != nil || moved != 1 {
		t.Fatalf("Pass: moved=%d err=%v", moved, err)
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatalf("delay 条目未删除: %d", n)
	}
	// 经正常投递链路可消费，DeliverAtMs/Tag/Keys 完整保留
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, deliver.AllPass)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("到期消息应可投递: %d %v", len(msgs), err)
	}
	got := msgs[0]
	if got.ID != "m1" || string(got.Body) != "hello" || got.DeliverAtMs != past ||
		got.Tag != "tg" || got.BornAtMs != 123 {
		t.Fatalf("消息字段丢失: %+v", got)
	}
	// Keys 索引在移入时由 Append 顺带写入
	kpfx := store.KeyIdxKeyPrefix("t", "k1")
	found := 0
	f.st.Scan(kpfx, store.PrefixUpperBound(kpfx), 0, func(k, v []byte) (bool, error) { found++; return true, nil })
	if found != 1 {
		t.Fatalf("keyidx 未写入: %d", found)
	}
}

// TestDelayMoveRedeliversOnCrashBetweenPhases 两段式移入的崩溃窗口重放语义：
// 第一段（消息入 msg/）落盘后、第二段（删 delay 条目）前崩溃——条目残留。
// 下一趟 Pass 重搬：目标队列出现两条同 ID 消息（at-least-once 允许的重复），
// 但绝不出现「条目没了消息也没了」（丢失）。afterAppendHook 仅测试注入。
func TestDelayMoveRedeliversOnCrashBetweenPhases(t *testing.T) {
	f := newFixture(t)
	past := time.Now().Add(-time.Second).UnixMilli()
	f.putDelay(t, 0, past, &core.Message{ID: "m1", Topic: "t", Body: []byte("x")})
	// 钩子在 Append 成功后 panic，模拟两段之间的进程崩溃
	f.sc.afterAppendHook = func() { panic("simulated crash between phases") }
	func() {
		defer func() { recover() }()
		f.sc.Pass(context.Background())
	}()
	// 崩溃后：目标消息已在（第一段已提交），delay 条目也在（第二段未执行）
	if n := f.delayCount(t); n != 1 {
		t.Fatalf("崩溃后 delay 条目应残留: %d", n)
	}
	if n := f.msgCount(t); n != 1 {
		t.Fatalf("崩溃后目标消息应已在: %d", n)
	}
	// 下一趟完整 Pass：重搬 → 重复消息，条目清空
	f.sc.afterAppendHook = nil
	moved, err := f.sc.Pass(context.Background())
	if err != nil || moved != 1 {
		t.Fatalf("重放 Pass: moved=%d err=%v", moved, err)
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatalf("重放后 delay 条目应清空: %d", n)
	}
	if n := f.msgCount(t); n != 2 {
		t.Fatalf("重放后目标队列应为 2 条（at-least-once 重复）: %d", n)
	}
}

func TestPassLeavesNotDueEntries(t *testing.T) {
	f := newFixture(t)
	f.putDelay(t, 0, time.Now().Add(time.Hour).UnixMilli(), &core.Message{ID: "m1", Topic: "t", Body: []byte("x")})
	moved, err := f.sc.Pass(context.Background())
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
	b.Set(store.DelayKey(time.Now().Add(-time.Second).UnixMilli(), 0), []byte("not-json"))
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
	moved, err := f.sc.Pass(context.Background())
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
		f.putDelay(t, i, past, &core.Message{ID: string(rune('a' + i)), Topic: "t", Body: []byte("x")})
	}
	// 单趟受预算限制
	moved, err := f.sc.Pass(context.Background())
	if err != nil || moved != 2 {
		t.Fatalf("首趟应恰好移动预算数: %d %v", moved, err)
	}
	// 连续 Pass 可排空
	total := moved
	for total < 5 {
		n, err := f.sc.Pass(context.Background())
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

// TestPassGatedByMetaLeadership 非 meta leader 时 Pass 有到期条目也不搬
// （返回 0，条目保留）；恒 true 照搬——delay/ 键族归 meta 组，搬移必须
// leader-only（非 leader 的第二段删条目提案会被拒）。
func TestPassGatedByMetaLeadership(t *testing.T) {
	f := newFixture(t)
	past := time.Now().Add(-time.Second).UnixMilli()
	f.putDelay(t, 0, past, &core.Message{ID: "m1", Topic: "t", Body: []byte("x")})
	rep := replication.NewStandalone(f.st)
	// 恒 false：有到期条目也不搬
	sc := New(rep, fixedLeaderRouter{leader: false}, f.st, f.pr, slog.Default())
	moved, err := sc.Pass(context.Background())
	if err != nil || moved != 0 {
		t.Fatalf("非 leader Pass: moved=%d err=%v; want 0,nil", moved, err)
	}
	if n := f.delayCount(t); n != 1 {
		t.Fatalf("非 leader 时到期条目不应被搬走: %d", n)
	}
	// 恒 true：照搬
	sc = New(rep, fixedLeaderRouter{leader: true}, f.st, f.pr, slog.Default())
	moved, err = sc.Pass(context.Background())
	if err != nil || moved != 1 {
		t.Fatalf("leader Pass: moved=%d err=%v; want 1,nil", moved, err)
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatalf("leader 时到期条目应被搬走: %d", n)
	}
}

// TestRunGateSkipsWhileNotLeader Run 的门控是「跳趟等 tick」而非退出循环：
// 非 leader 期间到期条目不动；拿到 meta leadership 后下趟即搬；失去后
// 新到期条目又停下（leader 可能随时轮到自己，退出即永久停摆）。
func TestRunGateSkipsWhileNotLeader(t *testing.T) {
	f := newFixture(t)
	past := time.Now().Add(-time.Second).UnixMilli()
	f.putDelay(t, 0, past, &core.Message{ID: "m1", Topic: "t", Body: []byte("x")})
	old := scanInterval
	scanInterval = 30 * time.Millisecond
	defer func() { scanInterval = old }()
	rt := &flipRouter{} // 默认非 leader
	sc := New(replication.NewStandalone(f.st), rt, f.st, f.pr, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sc.Run(ctx); close(done) }()
	time.Sleep(120 * time.Millisecond) // 4 个 tick：非 leader 期间不搬
	if n := f.delayCount(t); n != 1 {
		t.Fatalf("非 leader 期间条目不应被搬: %d", n)
	}
	rt.leader.Store(true) // 拿到 meta leadership → 下趟照搬
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && f.delayCount(t) != 0 {
		time.Sleep(30 * time.Millisecond)
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatal("成为 leader 后到期条目未被搬走")
	}
	rt.leader.Store(false) // 失去 leadership → 新到期条目不再被搬
	f.putDelay(t, 1, past, &core.Message{ID: "m2", Topic: "t", Body: []byte("x")})
	time.Sleep(120 * time.Millisecond)
	if n := f.delayCount(t); n != 1 {
		t.Fatalf("失去 leadership 后新到期条目不应被搬: %d", n)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未在 ctx 取消后退出")
	}
}
