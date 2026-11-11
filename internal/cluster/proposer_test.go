// proposer_test.go 保序提案队列（proposer.go）的承重不变式单测。
//
// 职责：只覆盖计划 §4.2 列出的承重规则——同组调用顺序 = apply 顺序、
// 入队后任期变化即失败不进 raft（SQ1 F5 硬约束）与 H2、raft 丢弃后同任期
// 在途条目作废（且不株连之后新入队的条目）、条目 ctx 先取消不株连、
// 停机收尾有界、leader 未知时同步/异步分形态。
// 边界：不测队列数据结构本身；三节点场景复用 cluster_test.go 的 harness，
// 单节点场景复用 group_test.go 的装配方式。
package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/batchrepr"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/Xsxdot/sq/internal/store"
)

// propTestGroup 单节点自选举组（AckQuorumMem）+ 可控生命周期 + 可注入
// raft.Node 包装与 OnApplied 钩子。返回组与取消函数（取消后等 done 由
// t.Cleanup 兜底）。
func propTestGroup(t *testing.T, onApplied func(g uint32, repr []byte), wrap func(raft.Node) raft.Node) (*group, *store.Store, context.CancelFunc) {
	t.Helper()
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	storage := raft.NewMemoryStorage()
	gr := newGroup(1, 1, storage, nil, rs, st, func(uint32, []*raftpb.Message) {}, AckQuorumMem, nil, onApplied, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	t.Cleanup(rn.Stop)
	gr.rn = rn
	if wrap != nil {
		gr.rn = wrap(rn)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go gr.run(ctx)
	t.Cleanup(func() {
		select {
		case <-gr.done():
		case <-time.After(5 * time.Second):
			t.Error("group did not shut down within 5s")
		}
	})
	t.Cleanup(cancel)
	waitFor(t, 10*time.Second, gr.isLeader, "单节点组未当选")
	return gr, st, cancel
}

// seqBatch 构造携带序号的提案缓冲（预留提案头 + 一个 key=prefix+seq 的批次）。
func seqBatch(t *testing.T, st *store.Store, prefix string, seq uint64) []byte {
	t.Helper()
	b := st.NewBatch()
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], seq)
	if err := b.Set(key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, ProposalHeaderLen+len(b.Repr()))
	copy(data[ProposalHeaderLen:], b.Repr())
	_ = b.Close()
	return data
}

// seqsIn 从 apply 的批次 repr 里解出 prefix 键携带的序号。
func seqsIn(repr []byte, prefix []byte) []uint64 {
	var out []uint64
	rd := batchrepr.Read(repr)
	if rd == nil {
		return nil
	}
	for {
		_, ukey, _, ok, err := rd.Next()
		if err != nil || !ok {
			return out
		}
		if bytes.HasPrefix(ukey, prefix) && len(ukey) == len(prefix)+8 {
			out = append(out, binary.BigEndian.Uint64(ukey[len(prefix):]))
		}
	}
}

// TestProposerCallOrderIsApplyOrder 不变式 1：同组内，在外部互斥锁下先后
// 调用异步入口的提案，apply 顺序与调用顺序一致（锁外 Wait）。这正是
// produce 在 qs.mu 下分配 offset 并 ApplyAsync 的形态。
func TestProposerCallOrderIsApplyOrder(t *testing.T) {
	prefix := []byte("ord/")
	var mu sync.Mutex
	var applied []uint64
	onApplied := func(_ uint32, repr []byte) {
		s := seqsIn(repr, prefix)
		mu.Lock()
		applied = append(applied, s...)
		mu.Unlock()
	}
	gr, st, _ := propTestGroup(t, onApplied, nil)
	const writers, per = 64, 200
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var callMu sync.Mutex
	var next uint64
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				callMu.Lock()
				next++
				data := seqBatch(t, st, string(prefix), next)
				p := gr.enqueueProposal(ctx, data, false)
				callMu.Unlock()
				if err := p.Wait(); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("提案失败: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(applied) != writers*per {
		t.Fatalf("apply 条数 %d，期望 %d", len(applied), writers*per)
	}
	inv := 0
	for i := 1; i < len(applied); i++ {
		if applied[i] <= applied[i-1] {
			if inv == 0 {
				t.Errorf("首个逆序：apply 序第 %d 条 seq=%d 在 seq=%d 之后", i, applied[i], applied[i-1])
			}
			inv++
		}
	}
	if inv > 0 {
		t.Fatalf("apply 顺序与调用顺序不一致：逆序 %d 处", inv)
	}
}

// TestProposerTermChangeFailsQueuedEntries 不变式 2（SQ1 F5 硬约束）：
// 条目在任期 T 入队（绑定 T），出队前本节点换届后在 T'>T 重新当选——
// 这正是「T 构造、T' 落地」的危险形态。断言：这些条目 Wait 均为
// ErrNotLeader，三个节点的 store 都从未见到它们的载荷；新任期入队的条目
// 照常成功。另断言 H2：当选后 leaderTerm() == rn.Status().GetTerm()。
//
// 手段：测试持有本组提案队列锁 pq.mu——入队方已在锁外读取领导权快照、
// 绑定了任期 T，阻塞在追加上；换届完成后放锁，真实 proposer 处理它们。
func TestProposerTermChangeFailsQueuedEntries(t *testing.T) {
	tc := newTestClusterOpts(t, AckQuorumMem, Options{LeaderBalancerInterval: time.Hour})
	const g = uint32(1)
	lead := tc.leaderOf(t, g)
	var other uint64
	for id := range tc.mgrs {
		if id != lead {
			other = id
			break
		}
	}
	gr := tc.mgrs[lead].groups[g]
	// H2：当选任期的观察值与 raft 内部一致
	waitFor(t, 5*time.Second, func() bool { return gr.leaderTerm() == gr.rn.Status().GetTerm() }, "H2：leaderTerm 未追上 Status().Term")
	termT := gr.leaderTerm()
	if es := gr.epoch.Load(); !es.isSelf || es.term != termT {
		t.Fatalf("领导权快照 = {term %d, isSelf %v}，期望 {%d, true}", es.term, es.isSelf, termT)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const n = 8
	prefix := "tc/"
	gr.pq.mu.Lock()
	locked := true
	defer func() {
		if locked {
			gr.pq.mu.Unlock()
		}
	}()
	pend := make([]*ProposalPending, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pend[i] = gr.enqueueProposal(ctx, seqBatch(t, tc.stores[lead], prefix, uint64(i)), false)
		}(i)
	}
	time.Sleep(200 * time.Millisecond) // 入队方读完快照、阻塞在 pq.mu 上

	// 换届一圈：lead → other → lead，本节点在新任期重新当选
	tc.mgrs[lead].TransferLeader(g, other)
	tc.waitLeader(t, g, other, 20*time.Second)
	tc.mgrs[other].TransferLeader(g, lead)
	tc.waitLeader(t, g, lead, 20*time.Second)
	waitFor(t, 5*time.Second, func() bool {
		es := gr.epoch.Load()
		return es.isSelf && es.term > termT && gr.leaderTerm() == gr.rn.Status().GetTerm()
	}, "本节点未在新任期重新当选，或 H2 不成立")
	termT2 := gr.leaderTerm()

	gr.pq.mu.Unlock()
	locked = false
	wg.Wait()
	for i, p := range pend {
		if !p.bound || p.term != termT {
			t.Fatalf("条目 %d 绑定 {bound %v, term %d}，期望绑定入队时任期 %d", i, p.bound, p.term, termT)
		}
		if err := p.Wait(); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("条目 %d（任期 %d 入队，出队时任期 %d）Wait=%v，期望 ErrNotLeader", i, termT, termT2, err)
		}
	}
	// 新任期入队照常成功；它 apply 之后，若旧条目进过日志也必已 apply
	if err := tc.mgrs[lead].ProposeReservedAsync(ctx, g, seqBatch(t, tc.stores[lead], "tc-new/", 1)).Wait(); err != nil {
		t.Fatalf("新任期提案失败: %v", err)
	}
	newKey := make([]byte, len("tc-new/")+8)
	copy(newKey, "tc-new/")
	binary.BigEndian.PutUint64(newKey[len("tc-new/"):], 1)
	for id, st := range tc.stores {
		waitFor(t, 10*time.Second, func() bool { _, ok, _ := st.Get(newKey); return ok }, fmt.Sprintf("节点 %d 未收到新任期条目", id))
		for i := 0; i < n; i++ {
			key := make([]byte, len(prefix)+8)
			copy(key, prefix)
			binary.BigEndian.PutUint64(key[len(prefix):], uint64(i))
			if _, ok, err := st.Get(key); err != nil || ok {
				t.Fatalf("节点 %d 见到了旧任期条目 %d 的载荷（ok=%v err=%v）——旧任期批次进了新任期日志", id, i, ok, err)
			}
		}
	}
}

// dropFirstNode 包装 raft.Node：第一次 Propose 直接报 ErrProposalDropped
// （模拟 leader 未换届时的 raft 丢弃，例如领导权转移窗口），其余透传。
type dropFirstNode struct {
	raft.Node
	mu      sync.Mutex
	dropped bool
	gate    chan struct{} // 非 nil 时第一次 Propose 先等它关闭（让后续条目入队）
}

func (n *dropFirstNode) Propose(ctx context.Context, data []byte) error {
	n.mu.Lock()
	first := !n.dropped
	n.dropped = true
	n.mu.Unlock()
	if first {
		if n.gate != nil {
			<-n.gate
		}
		return raft.ErrProposalDropped
	}
	return n.Node.Propose(ctx, data)
}

// TestProposerDropVoidsInFlightSameTerm 不变式 3：某条绑定条目被 raft
// 丢弃后，丢弃时已在途的同任期条目全部 ErrNotLeader、未进日志；丢弃之后
// 新入队的同任期条目照常成功（不作废整个任期——转移作罢后仍是同任期
// leader，整任期作废即活性故障）。
//
// 降级说明（计划 §4.2 第 3 条允许）：真实 raft 下能稳定构造的丢弃（转移
// 窗口 leadTransferee 非空）会把后续条目一并丢弃，作废标记有无都红不了；
// 这里用包装的 raft.Node 只丢第一条，其余透传真实 raft。
func TestProposerDropVoidsInFlightSameTerm(t *testing.T) {
	prefix := []byte("dv/")
	var mu sync.Mutex
	seen := map[uint64]bool{}
	onApplied := func(_ uint32, repr []byte) {
		mu.Lock()
		for _, s := range seqsIn(repr, prefix) {
			seen[s] = true
		}
		mu.Unlock()
	}
	gate := make(chan struct{})
	gr, st, _ := propTestGroup(t, onApplied, func(n raft.Node) raft.Node { return &dropFirstNode{Node: n, gate: gate} })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const n = 6
	pend := make([]*ProposalPending, n)
	pend[0] = gr.enqueueProposal(ctx, seqBatch(t, st, string(prefix), 0), false)
	// 第一条已在 proposer 手里（卡在 gate），其余在途入队
	waitFor(t, 5*time.Second, func() bool { gr.pq.mu.Lock(); defer gr.pq.mu.Unlock(); return len(gr.pq.items) == 0 }, "proposer 未取走第一条")
	for i := 1; i < n; i++ {
		pend[i] = gr.enqueueProposal(ctx, seqBatch(t, st, string(prefix), uint64(i)), false)
	}
	close(gate)
	for i, p := range pend {
		if err := p.Wait(); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("在途条目 %d Wait=%v，期望 ErrNotLeader（丢弃后同任期在途条目作废）", i, err)
		}
	}
	// 丢弃之后新入队的同任期条目照常成功（任期未变）
	if err := gr.enqueueProposal(ctx, seqBatch(t, st, string(prefix), 100), false).Wait(); err != nil {
		t.Fatalf("丢弃之后新入队的同任期条目失败: %v（整任期作废 = 活性故障）", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < n; i++ {
		if seen[uint64(i)] {
			t.Fatalf("作废条目 %d 进了日志并被 apply", i)
		}
	}
	if !seen[100] {
		t.Fatal("新条目未被 apply")
	}
}

// TestProposerCanceledEntryDoesNotTaint 不变式 4：条目 ctx 在处理前已取消
// → 不提案、Wait 返回 context.Canceled；后续条目照常成功（单条非领导权
// 失败不株连）。
func TestProposerCanceledEntryDoesNotTaint(t *testing.T) {
	prefix := []byte("cx/")
	var mu sync.Mutex
	seen := map[uint64]bool{}
	onApplied := func(_ uint32, repr []byte) {
		mu.Lock()
		for _, s := range seqsIn(repr, prefix) {
			seen[s] = true
		}
		mu.Unlock()
	}
	gr, st, _ := propTestGroup(t, onApplied, nil)
	dead, kill := context.WithCancel(context.Background())
	kill()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p1 := gr.enqueueProposal(dead, seqBatch(t, st, string(prefix), 1), false)
	p2 := gr.enqueueProposal(ctx, seqBatch(t, st, string(prefix), 2), false)
	if err := p1.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消条目 Wait=%v，期望 context.Canceled", err)
	}
	if err := p2.Wait(); err != nil {
		t.Fatalf("后续条目被株连: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if seen[1] {
		t.Fatal("已取消条目进了日志")
	}
	if !seen[2] {
		t.Fatal("后续条目未 apply")
	}
}

// TestProposerStopDrainsAndRejects 不变式 5：入队后取消组 ctx，所有 Wait
// 在 5s 内返回非 nil；停机后入队经 Wait 立即返回停止错误。
func TestProposerStopDrainsAndRejects(t *testing.T) {
	gr, st, stop := propTestGroup(t, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// 持队列锁让条目在 proposer 处理前堆在队里，再停组
	gr.pq.mu.Lock()
	const n = 16
	pend := make([]*ProposalPending, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pend[i] = gr.enqueueProposal(ctx, seqBatch(t, st, "stop/", uint64(i)), true)
		}(i)
	}
	time.Sleep(100 * time.Millisecond)
	stop()
	gr.pq.mu.Unlock()
	wg.Wait()
	deadline := time.After(5 * time.Second)
	for i, p := range pend {
		done := make(chan error, 1)
		go func() { done <- p.Wait() }()
		select {
		case err := <-done:
			// 放锁与停组竞速：少数条目可能在 proposer 观察到停止前已提案
			// 并 apply（nil）；其余必须是停止错误
			if err != nil && !errors.Is(err, ErrNotLeader) {
				t.Fatalf("条目 %d Wait=%v，期望 nil 或停止错误（ErrNotLeader）", i, err)
			}
		case <-deadline:
			t.Fatalf("条目 %d 的 Wait 在组停止后 5s 内未返回", i)
		}
	}
	<-gr.done()
	start := time.Now()
	err := gr.enqueueProposal(ctx, seqBatch(t, st, "stop/", 99), true).Wait()
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("停机后入队 Wait=%v，期望停止错误（ErrNotLeader）", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("停机后入队 %v 才返回，期望立即", d)
	}
}

// TestProposerLeaderUnknownByForm 不变式 6：leader 永不产生（三成员只起
// 一个）时，异步入口立即 ErrNotLeader；同步入口阻塞到 ctx 超时、返回 ctx 错误。
func TestProposerLeaderUnknownByForm(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	gr := startLoneGroupOfThree(t, 1, rs, st)

	start := time.Now()
	err := gr.enqueueProposal(context.Background(), seqBatch(t, st, "lu/", 1), false).Wait()
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("leader 未知时异步入口 Wait=%v，期望 ErrNotLeader", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("异步入口 %v 才失败，期望立即", d)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start = time.Now()
	err = gr.enqueueProposal(ctx, seqBatch(t, st, "lu/", 2), true).Wait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("leader 未知时同步入口 Wait=%v，期望 context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d < 400*time.Millisecond {
		t.Fatalf("同步入口 %v 就返回，期望阻塞到 ctx 超时", d)
	}
}

// TestProposerLeaderTermAtElection H2：本节点当选、leader 身份对外可见之前
// （OnLeaderChange 钩子内），leaderTerm() 已等于 raft 内部任期——任期绑定
// 读到的是当选任期，而不是 persistPhase 才更新的 lastTerm。fsync 档：append
// 走独立协程，lastTerm 的更新与钩子不在同一线程。
func TestProposerLeaderTermAtElection(t *testing.T) {
	type obs struct{ leaderTerm, raftTerm, epochTerm uint64 }
	got := make(chan obs, 4)
	var gr *group
	hook := func(_ uint32, _ uint64, isSelf bool) {
		if !isSelf {
			return
		}
		got <- obs{gr.leaderTerm(), gr.rn.Status().GetTerm(), gr.epoch.Load().term}
	}
	gr = newTestGroupWithHook(t, hook)
	ctx, cancel := context.WithCancel(context.Background())
	go gr.run(ctx)
	t.Cleanup(func() {
		select {
		case <-gr.done():
		case <-time.After(5 * time.Second):
			t.Error("group did not shut down within 5s")
		}
	})
	t.Cleanup(cancel)
	select {
	case o := <-got:
		if o.leaderTerm == 0 || o.leaderTerm != o.raftTerm {
			t.Fatalf("当选钩子内 leaderTerm()=%d，raft Status().Term=%d——H2 不成立", o.leaderTerm, o.raftTerm)
		}
		waitFor(t, 5*time.Second, gr.isLeader, "未对外可见为 leader")
		if es := gr.epoch.Load(); !es.isSelf || es.term != o.raftTerm {
			t.Fatalf("对外可见为 leader 后领导权快照 = {term %d, isSelf %v}，期望 {%d, true}", es.term, es.isSelf, o.raftTerm)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("单节点组未当选")
	}
}
