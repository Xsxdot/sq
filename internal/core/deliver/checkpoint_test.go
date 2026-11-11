// 不变式单测：逐次复制的串行化与失败失效（U4）、epoch 装载/失效（U6）（卡 SQ4.1）。
//
// 只测承重规则：
//   - U4：同队列至多一个消费状态提案在途——前序在途时后续调用只改内存、不
//     构造新差量；前序失败 → 内存态丢弃、挂起的后续调用以可重试错误失败、
//     下次访问从盘重装；任一时刻盘上 cursor 之下未确认的 offset 都有 inflight
//   - U6：epoch 变大 → 旧内存态丢弃、从盘重装；持更旧 epoch 的调用拿到
//     ErrNotLeader；提案一律绑定装载时的 epoch
//
// 假 Replicator 只替换「提案何时、是否落盘」：按调用顺序记录提案，测试逐个
// 放行（成功则把批次应用到真实 store，失败则不应用）。
package deliver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

// heldCall 一次被扣住的消费状态提案。
type heldCall struct {
	repr    []byte
	epoch   uint64
	release chan error
	ctx     context.Context
}

// holdRep 包一个真实 Standalone：meta/produce 的提案直通；deliver 的
// ApplyAsyncAt 在 hold 打开时被扣住，等测试放行。
type holdRep struct {
	*replication.Standalone
	st   *store.Store
	hold atomic.Bool
	// holdApply 同步 Apply 也扣住（管理批次迟到用例）；meta 的写不经此路径时才打开
	holdApply atomic.Bool

	mu    sync.Mutex
	calls []*heldCall
}

func (r *holdRep) ApplyAsyncAt(ctx context.Context, g uint32, b *store.Batch, epoch uint64) (replication.Pending, error) {
	if !r.hold.Load() {
		return r.Standalone.ApplyAsyncAt(ctx, g, b, epoch)
	}
	c := &heldCall{repr: append([]byte(nil), b.Repr()...), epoch: epoch, release: make(chan error, 1), ctx: ctx}
	b.Close()
	r.mu.Lock()
	r.calls = append(r.calls, c)
	r.mu.Unlock()
	return heldPending{r: r, c: c}, nil
}

// Apply holdApply 打开时扣住同步提交（等放行或 ctx 结束，同 heldPending）。
func (r *holdRep) Apply(ctx context.Context, g uint32, b *store.Batch) error {
	if !r.holdApply.Load() {
		return r.Standalone.Apply(ctx, g, b)
	}
	c := &heldCall{repr: append([]byte(nil), b.Repr()...), release: make(chan error, 1), ctx: ctx}
	b.Close()
	r.mu.Lock()
	r.calls = append(r.calls, c)
	r.mu.Unlock()
	return heldPending{r: r, c: c}.Wait()
}

// applyLate 把一条已「结局未知」（等待方已超时放弃）的提案迟到落盘——
// 模拟它在组内 FIFO 上仍排在后续提案之前、只是 apply 晚于等待方超时。
func (r *holdRep) applyLate(t *testing.T, i int) {
	t.Helper()
	b, err := r.st.NewBatchFromRepr(r.call(i).repr)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

func (r *holdRep) n() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *holdRep) call(i int) *heldCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[i]
}

type heldPending struct {
	r *holdRep
	c *heldCall
}

func (p heldPending) Wait() error {
	var done <-chan struct{}
	if p.c.ctx != nil {
		done = p.c.ctx.Done()
	}
	select {
	case err := <-p.c.release:
		if err != nil {
			return err
		}
	case <-done:
		return p.c.ctx.Err() // 结局未知：条目可能仍会落盘（见 applyLate）
	}
	b, err := p.r.st.NewBatchFromRepr(p.c.repr)
	if err != nil {
		return err
	}
	return p.r.st.Apply(b)
}

// epochRouter 单机路由 + 可切换的可写门 epoch。
type epochRouter struct {
	replication.StandaloneRouter
	epoch atomic.Uint64
}

func (r *epochRouter) WaitWritable(context.Context, uint32) (uint64, error) {
	return r.epoch.Load(), nil
}

type holdFixture struct {
	st  *store.Store
	rep *holdRep
	rt  *epochRouter
	pr  *produce.Producer
	dl  *Deliverer
}

func newHoldFixture(t *testing.T) *holdFixture {
	t.Helper()
	st, err := store.Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	rep := &holdRep{Standalone: replication.NewStandalone(st), st: st}
	rt := &epochRouter{}
	rt.epoch.Store(1)
	mt, err := meta.New(rep, rt, st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	return &holdFixture{st: st, rep: rep, rt: rt, pr: pr, dl: New(rep, rt, st, mt, pr, slog.Default())}
}

func (f *holdFixture) send(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := f.pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
}

type recvResult struct {
	msgs []*core.Message
	err  error
}

func (f *holdFixture) recvAsync(n int) chan recvResult {
	ch := make(chan recvResult, 1)
	go func() {
		m, err := f.dl.Receive(context.Background(), "g", "t", 0, n, time.Minute, 0, AllPass)
		ch <- recvResult{m, err}
	}()
	return ch
}

// waitUntil 轮询条件（上限 5s）。
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("等待超时：%s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// assertNoUncoveredOffsets I1 在盘上的形态：没有任何 ack 的场景下，盘上
// cursor 之下每个 offset 都必须有 inflight（否则该消息永不再投 = 丢失）。
func assertNoUncoveredOffsets(t *testing.T, st *store.Store) {
	t.Helper()
	c, inf := diskView(t, st, "g", "t", 0)
	for off := uint64(0); off < c; off++ {
		if _, ok := inf[off]; !ok {
			t.Fatalf("盘上 cursor=%d 越过了无 inflight 的 offset %d（丢消息）", c, off)
		}
	}
}

// TestPerOpOneProposalInFlightAndFailureDrops U4：前序在途时后续取件只改内存、
// 不构造新差量；前序失败 → 两次取件都失败、内存态丢弃、盘上不越界；从盘重装后
// 重新投出同一批 offset（attempt=1）。
func TestPerOpOneProposalInFlightAndFailureDrops(t *testing.T) {
	f := newHoldFixture(t)
	f.send(t, 4)
	f.rep.hold.Store(true)
	r1 := f.recvAsync(2)
	waitUntil(t, "首个提案入队", func() bool { return f.rep.n() == 1 })
	r2 := f.recvAsync(2)
	// r2 已在内存里投出 offset 2、3（镜像 4 条在外），但前序在途，不得提案
	waitUntil(t, "r2 改完内存", func() bool { n, _ := f.dl.InflightCount("g", "t", 0); return n == 4 })
	time.Sleep(20 * time.Millisecond)
	if n := f.rep.n(); n != 1 {
		t.Fatalf("前序在途时构造了新差量：提案数=%d，期望 1", n)
	}
	// 前序失败（不落盘）
	f.rep.call(0).release <- errors.New("注入：提案失败")
	// 若实现违反「至多一个在途」，后续差量已构造：放它成功，让盘上暴露后果
	time.Sleep(20 * time.Millisecond)
	for i := 1; i < f.rep.n(); i++ {
		f.rep.call(i).release <- nil
	}
	for _, ch := range []chan recvResult{r1, r2} {
		res := <-ch
		if res.err == nil {
			t.Fatalf("前序失败后取件应失败，得到 %d 条", len(res.msgs))
		}
	}
	assertNoUncoveredOffsets(t, f.st)
	if _, ok := f.dl.InflightCount("g", "t", 0); ok {
		t.Fatal("前序失败后内存态应已丢弃")
	}
	// 从盘重装：盘上无任何消费状态，重新从 offset 0 首投
	f.rep.hold.Store(false)
	got, err := f.dl.Receive(context.Background(), "g", "t", 0, 4, time.Minute, 0, AllPass)
	if err != nil || len(got) != 4 || got[0].Offset != 0 || got[0].DeliveryAttempt != 1 {
		t.Fatalf("重装后应从 0 首投 4 条: n=%d err=%v", len(got), err)
	}
	assertNoUncoveredOffsets(t, f.st)
}

// TestPerOpChainedProposalAfterSuccess U4：前序成功后才构造挂起的下一个差量，
// 两次取件都成功，盘上 = 内存。
func TestPerOpChainedProposalAfterSuccess(t *testing.T) {
	f := newHoldFixture(t)
	f.send(t, 4)
	f.rep.hold.Store(true)
	r1 := f.recvAsync(2)
	waitUntil(t, "首个提案入队", func() bool { return f.rep.n() == 1 })
	r2 := f.recvAsync(2)
	waitUntil(t, "r2 改完内存", func() bool { n, _ := f.dl.InflightCount("g", "t", 0); return n == 4 })
	f.rep.call(0).release <- nil
	waitUntil(t, "前序成功后构造下一个差量", func() bool { return f.rep.n() == 2 })
	f.rep.call(1).release <- nil
	for _, ch := range []chan recvResult{r1, r2} {
		if res := <-ch; res.err != nil || len(res.msgs) != 2 {
			t.Fatalf("取件应成功 2 条: n=%d err=%v", len(res.msgs), res.err)
		}
	}
	c, inf := diskView(t, f.st, "g", "t", 0)
	if c != 4 || len(inf) != 4 {
		t.Fatalf("盘上应为 cursor=4、4 条 inflight: cursor=%d n=%d", c, len(inf))
	}
}

// TestEpochChangeReloadsAndStaleEpochRejected U6。
func TestEpochChangeReloadsAndStaleEpochRejected(t *testing.T) {
	f := newHoldFixture(t)
	f.send(t, 12)
	f.rep.hold.Store(true)
	r1 := f.recvAsync(1)
	waitUntil(t, "提案入队", func() bool { return f.rep.n() == 1 })
	if e := f.rep.call(0).epoch; e != 1 {
		t.Fatalf("提案应绑定装载 epoch 1，得到 %d", e)
	}
	f.rep.call(0).release <- nil
	if res := <-r1; res.err != nil || len(res.msgs) != 1 {
		t.Fatalf("首投: %v", res.err)
	}
	// 他人任期内的写（本节点失主期间）：盘上 cursor 推到 10、inflight 清空
	b := f.st.NewBatch()
	b.Set(store.CursorKey("g", "t", 0), store.PutU64(10))
	b.Delete(store.InflightKey("g", "t", 0, 0))
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
	// 重新当选（epoch 2）：内存态必须丢弃重装，从 10 投
	f.rt.epoch.Store(2)
	r2 := f.recvAsync(1)
	waitUntil(t, "epoch 2 提案入队", func() bool { return f.rep.n() == 2 })
	if e := f.rep.call(1).epoch; e != 2 {
		t.Fatalf("重装后提案应绑定 epoch 2，得到 %d", e)
	}
	f.rep.call(1).release <- nil
	res := <-r2
	if res.err != nil || len(res.msgs) != 1 || res.msgs[0].Offset != 10 {
		t.Fatalf("换届后应从盘重装、投 offset 10: %+v err=%v", res.msgs, res.err)
	}
	// 持更旧 epoch 的调用：ErrNotLeader
	f.rt.epoch.Store(1)
	if _, err := f.dl.Ack(context.Background(), "g", "t", 0, 10, 1); !errors.Is(err, replication.ErrNotLeader) {
		t.Fatalf("旧 epoch 调用应得 ErrNotLeader，得到 %v", err)
	}
}

// TestResetCursorNotOverwrittenByInflightDelta S6：重置位点时同队列有旧差量
// 在途——旧差量失败（属已失效代号）不得把重置后的内存态丢掉；重置的全量批次
// 落盘后盘上 cursor = 重置值、inflight 为空，下一次取件从新位点投。
func TestResetCursorNotOverwrittenByInflightDelta(t *testing.T) {
	f := newHoldFixture(t)
	f.send(t, 8)
	f.rep.hold.Store(true)
	r1 := f.recvAsync(2)
	waitUntil(t, "取件提案入队", func() bool { return f.rep.n() == 1 })
	resetDone := make(chan error, 1)
	go func() { resetDone <- f.dl.ResetCursor(context.Background(), "g", "t", 0, 5) }()
	waitUntil(t, "重置提案入队", func() bool { return f.rep.n() == 2 })
	// 重置持队列锁等自己的提案：旧差量的完成回调要等它放锁，故两个提案都
	// 先放行再收结果（真实复制里两者的落盘互不依赖队列锁）
	f.rep.call(0).release <- errors.New("注入：旧差量失败")
	f.rep.call(1).release <- nil
	if err := <-resetDone; err != nil {
		t.Fatalf("重置: %v", err)
	}
	if res := <-r1; res.err == nil {
		t.Fatal("旧差量失败，取件应报错")
	}
	if _, ok := f.dl.InflightCount("g", "t", 0); !ok {
		t.Fatal("旧代号差量失败不应丢弃重置后的内存态")
	}
	c, inf := diskView(t, f.st, "g", "t", 0)
	if c != 5 || len(inf) != 0 {
		t.Fatalf("重置后盘上应 cursor=5、无 inflight: cursor=%d n=%d", c, len(inf))
	}
	f.rep.hold.Store(false)
	got, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, AllPass)
	if err != nil || len(got) != 1 || got[0].Offset != 5 || got[0].DeliveryAttempt != 1 {
		t.Fatalf("重置后下一次取件应投 offset 5 首投: %+v %v", got, err)
	}
}

// —— 检查点模式（段 B）——

// newCkFixture 单机检查点模式 fixture（间隔 iv；Start 已调用，Cleanup 里 Stop）。
func newCkFixture(t *testing.T, iv time.Duration) *fixture {
	t.Helper()
	f := newFixture(t)
	f.dl = New(f.dl.rep, f.dl.rt, f.st, f.dl.mt, f.pr, slog.Default(), WithCheckpointInterval(iv))
	f.dl.Start(context.Background())
	t.Cleanup(func() { f.dl.Stop(context.Background()) })
	return f
}

// assertDiskEqualsMem 盘上本队列 = 内存态（cursor + inflight 全字段）。
func assertDiskEqualsMem(t *testing.T, d *Deliverer, st *store.Store, group, topic string, q uint32) {
	t.Helper()
	qs := d.queue(group, topic, q)
	qs.mu.Lock()
	mc, mm := memView(qs)
	loaded := qs.loaded
	qs.mu.Unlock()
	if !loaded {
		t.Fatal("队列内存态未装载，无从比较")
	}
	dc, dm := diskView(t, st, group, topic, q)
	if dc != mc || len(dm) != len(mm) {
		t.Fatalf("盘上 != 内存: disk cursor=%d n=%d, mem cursor=%d n=%d", dc, len(dm), mc, len(mm))
	}
	for off, s := range mm {
		if dm[off] != s {
			t.Fatalf("offset %d 盘上 %+v != 内存 %+v", off, dm[off], s)
		}
	}
}

// TestCheckpointStopWritesFinalCheckpoint U5：检查点模式下变更只进内存；Stop
// 后盘上 = 停机前内存；Stop 后的调用返回 ErrNotLeader。
func TestCheckpointStopWritesFinalCheckpoint(t *testing.T) {
	f := newCkFixture(t, time.Hour) // 循环不会触发：落盘只来自 Stop
	for i := 0; i < 5; i++ {
		f.send(t, "t", fmt.Sprintf("m%d", i))
	}
	got, err := f.dl.Receive(context.Background(), "g", "t", 0, 3, time.Minute, 0, AllPass)
	if err != nil || len(got) != 3 {
		t.Fatalf("取件: %d %v", len(got), err)
	}
	if ok, err := f.dl.Ack(context.Background(), "g", "t", 0, got[1].Offset, got[1].DeliveryAttempt); !ok || err != nil {
		t.Fatalf("ack: %v %v", ok, err)
	}
	if c, inf := diskView(t, f.st, "g", "t", 0); c != 0 || len(inf) != 0 {
		t.Fatalf("检查点前盘上不应有消费状态: cursor=%d n=%d", c, len(inf))
	}
	if err := f.dl.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertDiskEqualsMem(t, f.dl, f.st, "g", "t", 0)
	if c, inf := diskView(t, f.st, "g", "t", 0); c != 3 || len(inf) != 2 {
		t.Fatalf("收尾检查点后盘上应为 cursor=3、2 条 inflight: cursor=%d n=%d", c, len(inf))
	}
	if _, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, AllPass); !errors.Is(err, replication.ErrNotLeader) {
		t.Fatalf("Stop 后取件应得 ErrNotLeader: %v", err)
	}
}

// TestCheckpointLoopPersists 后台循环按间隔把脏队列整体落盘。
func TestCheckpointLoopPersists(t *testing.T) {
	f := newCkFixture(t, 20*time.Millisecond)
	for i := 0; i < 4; i++ {
		f.send(t, "t", "x")
	}
	got, err := f.dl.Receive(context.Background(), "g", "t", 0, 4, time.Minute, 0, AllPass)
	if err != nil || len(got) != 4 {
		t.Fatalf("取件: %d %v", len(got), err)
	}
	f.dl.Ack(context.Background(), "g", "t", 0, got[0].Offset, 1)
	waitUntil(t, "检查点落盘", func() bool {
		c, inf := diskView(t, f.st, "g", "t", 0)
		return c == 4 && len(inf) == 3
	})
	assertDiskEqualsMem(t, f.dl, f.st, "g", "t", 0)
}

// TestCheckpointFailureKeepsMemoryAndRetries 检查点批次失败：内存态保留、
// 队列重新登记，下一轮整体重写后盘上 = 内存（全量替换不依赖前序批次）。
func TestCheckpointFailureKeepsMemoryAndRetries(t *testing.T) {
	f := newHoldFixture(t)
	f.dl = New(f.rep, f.rt, f.st, f.dl.mt, f.pr, slog.Default(), WithCheckpointInterval(time.Hour))
	f.send(t, 3)
	got, err := f.dl.Receive(context.Background(), "g", "t", 0, 3, time.Minute, 0, AllPass)
	if err != nil || len(got) != 3 {
		t.Fatalf("取件: %d %v", len(got), err)
	}
	f.rep.hold.Store(true)
	done := make(chan error, 1)
	go func() { _, err := f.dl.checkpointRound(context.Background(), nil, false); done <- err }()
	waitUntil(t, "检查点批次入队", func() bool { return f.rep.n() == 1 })
	f.rep.call(0).release <- errors.New("注入：检查点失败")
	if err := <-done; err == nil {
		t.Fatal("检查点失败应返回错误")
	}
	if n, ok := f.dl.InflightCount("g", "t", 0); !ok || n != 3 {
		t.Fatalf("检查点失败不应丢弃内存态: n=%d ok=%v", n, ok)
	}
	f.rep.hold.Store(false)
	if n, err := f.dl.checkpointRound(context.Background(), nil, false); err != nil || n != 1 {
		t.Fatalf("下一轮应重写该队列: n=%d err=%v", n, err)
	}
	assertDiskEqualsMem(t, f.dl, f.st, "g", "t", 0)
}

// TestPrepareTransferFencesAndCheckpoints U7：PrepareTransfer 之前的改动全部
// 进转移前检查点；围栏期间到达的确认阻塞；围栏到期后恢复服务。
func TestPrepareTransferFencesAndCheckpoints(t *testing.T) {
	old := fenceTTL
	fenceTTL = 300 * time.Millisecond
	t.Cleanup(func() { fenceTTL = old })
	f := newCkFixture(t, time.Hour)
	f.send(t, "t", "a")
	f.send(t, "t", "b")
	got, err := f.dl.Receive(context.Background(), "g", "t", 0, 2, time.Minute, 0, AllPass)
	if err != nil || len(got) != 2 {
		t.Fatalf("取件: %d %v", len(got), err)
	}
	g := f.dl.rt.GroupForQueue("t", 0)
	start := time.Now()
	if err := f.dl.PrepareTransfer(context.Background(), g); err != nil {
		t.Fatalf("PrepareTransfer: %v", err)
	}
	if c, inf := diskView(t, f.st, "g", "t", 0); c != 2 || len(inf) != 2 {
		t.Fatalf("围栏前的取件应全部进转移前检查点: cursor=%d n=%d", c, len(inf))
	}
	ackDone := make(chan time.Time, 1)
	go func() {
		f.dl.Ack(context.Background(), "g", "t", 0, got[0].Offset, 1)
		ackDone <- time.Now()
	}()
	select {
	case <-ackDone:
		t.Fatal("围栏期间确认应阻塞")
	case <-time.After(100 * time.Millisecond):
	}
	at := <-ackDone
	if w := at.Sub(start); w < 250*time.Millisecond {
		t.Fatalf("确认应等到围栏到期（~300ms）才执行，实际 %v", w)
	}
}

// TestPrepareTransferFailureLiftsFence 转移前检查点失败：返回错误并撤围栏，
// 调用立即恢复。
func TestPrepareTransferFailureLiftsFence(t *testing.T) {
	f := newHoldFixture(t)
	f.dl = New(f.rep, f.rt, f.st, f.dl.mt, f.pr, slog.Default(), WithCheckpointInterval(time.Hour))
	f.send(t, 1)
	if got, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, AllPass); err != nil || len(got) != 1 {
		t.Fatalf("取件: %v", err)
	}
	f.rep.hold.Store(true)
	done := make(chan error, 1)
	go func() { done <- f.dl.PrepareTransfer(context.Background(), 0) }()
	waitUntil(t, "检查点批次入队", func() bool { return f.rep.n() == 1 })
	f.rep.call(0).release <- errors.New("注入：失败")
	if err := <-done; err == nil {
		t.Fatal("应返回错误")
	}
	if f.dl.fenced(0) {
		t.Fatal("失败后围栏应已撤除")
	}
}

// —— 结局未知后的重装（审查 I-1）——

// lateFixture 逐次模式 + 短提案超时：先投出 offset 0、1 并落盘。
func lateFixture(t *testing.T) *holdFixture {
	t.Helper()
	old := consumeProposeTimeout
	consumeProposeTimeout = 100 * time.Millisecond
	t.Cleanup(func() { consumeProposeTimeout = old })
	f := newHoldFixture(t)
	f.send(t, 4)
	got, err := f.dl.Receive(context.Background(), "g", "t", 0, 2, time.Minute, 0, AllPass)
	if err != nil || len(got) != 2 {
		t.Fatalf("首取: %d %v", len(got), err)
	}
	return f
}

// lateThenReceive 结局未知的提案 late 之后：内存态已丢弃，再取件（重装后
// 投出 2、3，其提案被扣住）→ late 按组内 FIFO 先落盘 → 再放行取件提案。
// 断言盘上 cursor 之下无未覆盖的 offset（本场景无任何 ack）。
func lateThenReceive(t *testing.T, f *holdFixture, late int) {
	t.Helper()
	f.rep.hold.Store(true)
	r := f.recvAsync(2)
	waitUntil(t, "重装后的取件提案入队", func() bool { return f.rep.n() == late+2 })
	f.rep.applyLate(t, late)
	f.rep.call(late + 1).release <- nil
	if res := <-r; res.err != nil || len(res.msgs) != 2 || res.msgs[0].Offset != 2 {
		t.Fatalf("重装后应投出 offset 2、3: %+v %v", res.msgs, res.err)
	}
	assertNoUncoveredOffsets(t, f.st)
}

// TestLateResetAfterTimeoutKeepsI1 ResetCursor 提案结局未知（等待超时）→ 内存
// 态丢弃 → 同 epoch 内重装再取件 → 重置批次（DeleteRange）迟到落盘：重装后
// 的第一个提案必须整体替换，否则盘上 cursor 越过被迟到批次删掉的 inflight。
func TestLateResetAfterTimeoutKeepsI1(t *testing.T) {
	f := lateFixture(t)
	f.rep.hold.Store(true)
	if err := f.dl.ResetCursor(context.Background(), "g", "t", 0, 0); err == nil {
		t.Fatal("重置提案被扣住，应超时报错")
	}
	lateThenReceive(t, f, 0)
}

// TestLateAdminBatchAfterTimeoutKeepsI1 同族：删组管理批次超时（结局未知）后
// 迟到落盘。
func TestLateAdminBatchAfterTimeoutKeepsI1(t *testing.T) {
	f := lateFixture(t)
	f.rep.holdApply.Store(true)
	b := f.st.NewBatch()
	ck := store.CursorKey("g", "t", 0)
	b.DeleteRange(ck, store.PrefixUpperBound(ck))
	ip := store.InflightPrefix("g", "t", 0)
	b.DeleteRange(ip, store.PrefixUpperBound(ip))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := f.dl.ApplyAdminBatch(ctx, 0, b); err == nil {
		t.Fatal("管理批次被扣住，应超时报错")
	}
	f.rep.holdApply.Store(false)
	lateThenReceive(t, f, 0)
}

// TestStopBoundedByCtxWhenProposalsHang 停机收尾检查点的提案永不提交（失去
// 多数派）时，Stop 须在调用方 ctx 截止时返回，而不是等满单提案上限
// consumeProposeTimeout（10s）——停机预算由调用方 ctx 决定。
func TestStopBoundedByCtxWhenProposalsHang(t *testing.T) {
	f := newHoldFixture(t)
	f.dl = New(f.rep, f.rt, f.st, f.dl.mt, f.pr, slog.Default(), WithCheckpointInterval(time.Hour))
	f.send(t, 2)
	if got, err := f.dl.Receive(context.Background(), "g", "t", 0, 2, time.Minute, 0, AllPass); err != nil || len(got) != 2 {
		t.Fatalf("取件: %d %v", len(got), err)
	}
	f.rep.hold.Store(true) // 此后提案永不放行
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := f.dl.Stop(ctx)
	took := time.Since(start)
	if err == nil {
		t.Fatal("提案未提交时 Stop 应返回错误")
	}
	if took > 2*time.Second {
		t.Fatalf("Stop 应受调用方 ctx（200ms）约束，实际耗时 %v", took)
	}
}
