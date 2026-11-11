// writegate_test.go 可写门（writegate.go，卡 SQ1）的不变式单测：
//   - U1 termStart 识别规则：当选后门记下的首条 index 在 raft 日志里的
//     Term 等于当选任期、前一条 Term 更小；来回转移领导权，epoch 严格递增，
//     失去领导权的一侧门关闭。
//   - U2 门的开/关/超时：新 leader apply 被卡住时等待超时返回 ErrNotLeader；
//     等待中失去领导权立即返回 ErrNotLeader（不等到超时）；放开 apply 后
//     返回当前任期且 applied ≥ termStart。
//   - U4 门的 epoch 即 raft 当前任期：当选后门打开时 WaitWritable 返回的
//     epoch 等于该组 rn.Status().Term，跨换届严格递增（守住 SQ2 leaderTerm()
//     的语义——它是 epoch 的唯一读取入口）。
package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/store"
)

// checkTermStart 核对 gr 的门状态满足识别规则（U1 的承重断言）。
func checkTermStart(t *testing.T, gr *group) gateState {
	t.Helper()
	gs := *gr.gate.Load()
	es := gr.epoch.Load()
	if gs.term != es.term || gs.termStart == 0 {
		t.Fatalf("门状态 %+v 与 epoch term=%d 不符", gs, es.term)
	}
	if st := gr.rn.Status(); st.GetTerm() != gs.term {
		t.Fatalf("门 term=%d 而 raft Status().Term=%d", gs.term, st.GetTerm())
	}
	at, err := gr.mem.Term(gs.termStart)
	if err != nil {
		t.Fatalf("读 Term(%d): %v", gs.termStart, err)
	}
	if at != gs.term {
		t.Fatalf("termStart=%d 处条目 Term=%d，应等于当选任期 %d", gs.termStart, at, gs.term)
	}
	prev, err := gr.mem.Term(gs.termStart - 1)
	if err != nil {
		t.Fatalf("读 Term(%d): %v", gs.termStart-1, err)
	}
	if prev >= gs.term {
		t.Fatalf("termStart-1=%d 处条目 Term=%d，应小于当选任期 %d（termStart 不是本任期首条）", gs.termStart-1, prev, gs.term)
	}
	return gs
}

// TestWriteGateTermStartIdentified U1：termStart 识别规则 + 转移领导权时
// epoch 严格递增、旧 leader 关门。
func TestWriteGateTermStartIdentified(t *testing.T) {
	tc := newTestClusterOpts(t, AckQuorumMem, Options{LeaderBalancerInterval: time.Hour})
	const g = uint32(1)
	lead := tc.leaderOf(t, g)
	gr := tc.mgrs[lead].groups[g]
	waitFor(t, 10*time.Second, func() bool { _, ok := gr.writable(); return ok }, "初始 leader 的可写门未打开")
	gs := checkTermStart(t, gr)
	t.Logf("初始 leader=%d term=%d termStart=%d", lead, gs.term, gs.termStart)

	prevTerm, prevLead := gs.term, lead
	other := uint64(1)
	if other == lead {
		other = 2
	}
	for i, to := range []uint64{other, lead} {
		waitFor(t, 20*time.Second, func() bool {
			if tc.mgrs[to].IsLeader(g) {
				return true
			}
			if cur, ok := tc.mgrs[prevLead].Leader(g); ok {
				tc.mgrs[cur].TransferLeader(g, to)
			}
			return false
		}, fmt.Sprintf("第 %d 次转移：领导权未转到 %d", i+1, to))
		ngr := tc.mgrs[to].groups[g]
		waitFor(t, 10*time.Second, func() bool { _, ok := ngr.writable(); return ok }, "新 leader 的可写门未打开")
		ngs := checkTermStart(t, ngr)
		if ngs.term <= prevTerm {
			t.Fatalf("第 %d 次转移：epoch %d 未严格大于上一任 %d", i+1, ngs.term, prevTerm)
		}
		old := tc.mgrs[prevLead].groups[g]
		waitFor(t, 10*time.Second, func() bool {
			_, ok := old.writable()
			return !ok && *old.gate.Load() == gateState{}
		}, "失去领导权的一侧门未关闭")
		t.Logf("第 %d 次转移：leader=%d term=%d termStart=%d", i+1, to, ngs.term, ngs.termStart)
		prevTerm, prevLead = ngs.term, to
	}
}

// gateApplyBlock 挂在 OnApplied 上的 apply 闸门：armed 后目标节点目标组的
// 第一条非空条目进入钩子即阻塞，直到 release（与 produce 包 S1 同手法）。
type gateApplyBlock struct {
	node    atomic.Uint64 // 钩子在 apply 协程读，测试协程写
	group   uint32
	armed   atomic.Bool
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
	relOnce sync.Once
}

func (b *gateApplyBlock) hook(node uint64, g uint32, repr []byte) {
	if node != b.node.Load() || g != b.group || len(repr) == 0 || !b.armed.Load() {
		return
	}
	b.once.Do(func() { close(b.blocked) })
	<-b.release
}

func (b *gateApplyBlock) open() { b.relOnce.Do(func() { close(b.release) }) }

// TestWriteGateBlocksUntilApplyCatchesUp U2：门的开/关/超时。
func TestWriteGateBlocksUntilApplyCatchesUp(t *testing.T) {
	const g = uint32(1)
	const waitCap = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// 自建三节点（不用 harness：其 OnApplied 收集器 channel 有界，apply
	// 被卡住时会反压别的节点）；不开摊布，领导权只由本测试转移。
	peers := map[uint64]string{}
	lns := map[uint64]net.Listener{}
	for i := uint64(1); i <= 3; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i], peers[i] = ln, ln.Addr().String()
	}
	blk := &gateApplyBlock{group: g, blocked: make(chan struct{}), release: make(chan struct{})}
	mgrs := map[uint64]*Manager{}
	stores := map[uint64]*store.Store{}
	base := t.TempDir()
	for i := uint64(1); i <= 3; i++ {
		st, err := store.Open(fmt.Sprintf("%s/%d", base, i), false, testSlog(t))
		if err != nil {
			t.Fatal(err)
		}
		id := i
		m, err := NewManager(Options{
			NodeID: i, Peers: peers, Listener: lns[i], DataGroups: 3,
			Mode: AckQuorumMem, Store: st, Logger: testSlog(t),
			OnApplied:           func(g uint32, repr []byte) { blk.hook(id, g, repr) },
			WritableWaitTimeout: waitCap,
		})
		if err != nil {
			t.Fatal(err)
		}
		m.Start(context.Background())
		mgrs[i], stores[i] = m, st
	}
	t.Cleanup(func() {
		blk.open()
		for _, m := range mgrs {
			_ = m.StopClean(context.Background())
		}
		for _, m := range mgrs {
			select {
			case <-m.Done():
			case <-time.After(10 * time.Second):
			}
		}
		for _, st := range stores {
			_ = st.Close()
		}
	})
	leaderOf := func() uint64 {
		var lead uint64
		waitFor(t, 30*time.Second, func() bool {
			for id, m := range mgrs {
				if m.IsLeader(g) {
					lead = id
					return true
				}
			}
			return false
		}, "组无 leader")
		return lead
	}
	transferTo := func(to uint64) {
		waitFor(t, 20*time.Second, func() bool {
			if mgrs[to].IsLeader(g) {
				return true
			}
			for _, m := range mgrs {
				if cur, ok := m.Leader(g); ok && cur != to {
					mgrs[cur].TransferLeader(g, to)
					break
				}
			}
			return false
		}, fmt.Sprintf("领导权未转到 %d", to))
	}
	write := func(via uint64, k string) {
		b := stores[via].NewBatch()
		b.Set([]byte("wg/"+k), []byte(k))
		if err := (clusterReplicator{mgrs[via]}).Apply(ctx, g, b); err != nil {
			t.Fatalf("经 %d 写 %s: %v", via, k, err)
		}
	}

	L := leaderOf()
	F := uint64(1)
	if F == L {
		F = 2
	}
	blk.node.Store(F)
	write(L, "warm")
	waitFor(t, 10*time.Second, func() bool { return mgrs[F].AppliedIndex(g) >= mgrs[L].AppliedIndex(g) }, "F 未追平预热")
	blk.armed.Store(true)
	write(L, "m0")
	select {
	case <-blk.blocked:
	case <-ctx.Done():
		t.Fatal("F 的 apply 未进入闸门")
	}
	for i := 1; i <= 3; i++ {
		write(L, fmt.Sprintf("m%d", i))
	}
	lApplied := mgrs[L].AppliedIndex(g)
	waitFor(t, 10*time.Second, func() bool {
		st, _ := mgrs[F].Status(g)
		return st.HardState.GetCommit() >= lApplied
	}, "F 提交位点未追上")
	transferTo(F)
	fgr := mgrs[F].groups[g]
	waitFor(t, 10*time.Second, func() bool { return fgr.gate.Load().termStart != 0 }, "F 未识别本任期首条")
	gs := *fgr.gate.Load()
	t.Logf("F=%d 当选 term=%d termStart=%d applied=%d", F, gs.term, gs.termStart, mgrs[F].AppliedIndex(g))
	if mgrs[F].AppliedIndex(g) >= gs.termStart {
		t.Fatalf("未制造出 applied<termStart 窗口")
	}

	// A. 门关着：等待到上限后返回 ErrNotLeader
	start := time.Now()
	_, err := mgrs[F].WaitWritable(ctx, g)
	el := time.Since(start)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("A：门关着时 WaitWritable 应返回 ErrNotLeader，得到 %v", err)
	}
	if el < waitCap-200*time.Millisecond || el > waitCap+2*time.Second {
		t.Fatalf("A：超时返回耗时 %v，应约等于上限 %v", el, waitCap)
	}
	t.Logf("A：超时 %v 后返回 %v", el.Round(time.Millisecond), err)

	// B. 等待中失去领导权：立即返回 ErrNotLeader（远早于上限）
	type res struct {
		epoch uint64
		err   error
		el    time.Duration
	}
	resCh := make(chan res, 1)
	go func() {
		s := time.Now()
		e, err := mgrs[F].WaitWritable(ctx, g)
		resCh <- res{e, err, time.Since(s)}
	}()
	time.Sleep(200 * time.Millisecond)
	lostAt := time.Now()
	transferTo(L)
	r := <-resCh
	if !errors.Is(r.err, ErrNotLeader) {
		t.Fatalf("B：失去领导权后 WaitWritable 应返回 ErrNotLeader，得到 epoch=%d err=%v", r.epoch, r.err)
	}
	if r.el >= waitCap-200*time.Millisecond {
		t.Fatalf("B：失去领导权后仍等到了上限（%v），应立即返回", r.el)
	}
	t.Logf("B：等待 %v 后返回（转移发起后 %v）: %v", r.el.Round(time.Millisecond),
		time.Since(lostAt).Round(time.Millisecond), r.err)
	if _, ok := fgr.writable(); ok {
		t.Fatal("B：F 已非 leader，门却开着")
	}

	// C. 再当选（apply 仍卡着，门关），等待中放开 apply：返回当前任期且
	//    applied ≥ termStart
	transferTo(F)
	waitFor(t, 10*time.Second, func() bool {
		cur := fgr.gate.Load()
		return cur.termStart != 0 && cur.term > gs.term
	}, "F 再当选后未识别新任期首条")
	gs2 := *fgr.gate.Load()
	go func() {
		s := time.Now()
		e, err := mgrs[F].WaitWritable(ctx, g)
		resCh <- res{e, err, time.Since(s)}
	}()
	time.Sleep(300 * time.Millisecond)
	select {
	case r := <-resCh:
		t.Fatalf("C：apply 未放开时 WaitWritable 已返回 epoch=%d err=%v", r.epoch, r.err)
	default:
	}
	blk.open()
	r = <-resCh
	if r.err != nil {
		t.Fatalf("C：放开 apply 后 WaitWritable 失败: %v", r.err)
	}
	if r.epoch != gs2.term {
		t.Fatalf("C：返回 epoch=%d，应为当前任期 %d", r.epoch, gs2.term)
	}
	if a := mgrs[F].AppliedIndex(g); a < gs2.termStart {
		t.Fatalf("C：放行时 applied=%d < termStart=%d", a, gs2.termStart)
	}
	t.Logf("C：放开后 %v 返回 epoch=%d（termStart=%d）", r.el.Round(time.Millisecond), r.epoch, gs2.termStart)
}

// TestWriteGateEpochEqualsRaftTerm U4：WaitWritable 返回的 epoch == 该组
// raft 当前任期，跨两次换届。
func TestWriteGateEpochEqualsRaftTerm(t *testing.T) {
	tc := newTestClusterOpts(t, AckQuorumMem, Options{LeaderBalancerInterval: time.Hour})
	const g = uint32(2)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lead := tc.leaderOf(t, g)
	check := func(id uint64) uint64 {
		t.Helper()
		var epoch uint64
		waitFor(t, 10*time.Second, func() bool {
			e, err := tc.mgrs[id].WaitWritable(ctx, g)
			if err != nil {
				return false
			}
			epoch = e
			return true
		}, fmt.Sprintf("节点 %d 的可写门未打开", id))
		st, _ := tc.mgrs[id].Status(g)
		if epoch == 0 || epoch != st.GetTerm() {
			t.Fatalf("节点 %d：WaitWritable epoch=%d，raft Status().Term=%d，应相等", id, epoch, st.GetTerm())
		}
		return epoch
	}
	prev := check(lead)
	t.Logf("leader=%d epoch=%d", lead, prev)
	other := uint64(1)
	if other == lead {
		other = 2
	}
	from := lead
	for i, to := range []uint64{other, lead} {
		waitFor(t, 20*time.Second, func() bool {
			if tc.mgrs[to].IsLeader(g) {
				return true
			}
			if cur, ok := tc.mgrs[from].Leader(g); ok {
				tc.mgrs[cur].TransferLeader(g, to)
			}
			return false
		}, fmt.Sprintf("第 %d 次换届：领导权未转到 %d", i+1, to))
		e := check(to)
		if e <= prev {
			t.Fatalf("第 %d 次换届：epoch %d 未严格大于上一任 %d", i+1, e, prev)
		}
		t.Logf("第 %d 次换届：leader=%d epoch=%d", i+1, to, e)
		prev, from = e, to
	}
}
