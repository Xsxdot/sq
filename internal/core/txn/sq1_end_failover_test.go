// sq1_end_failover_test.go 卡 SQ1 故事级检查 S2：txn.End 在 apply 滞后的新
// meta leader 上不得把「本地读不到 halfidx」判成幂等成功。
//
// 窗口制造同 produce 包 S1：OnApplied 钩子在 F 的 meta 组 apply 协程内阻塞，
// 令 F 的 applied 落后于已提交；Stage 在旧 meta leader L 上成功（客户端已拿到
// txID）后，把 meta 领导权转给 F，经 F 调 End(commit=true)。
//
// 判据：
//   - 门未开（F apply 未追平）期间，End 不得返回 found=false 的成功——那是
//     「客户端收到 commit OK，事务实际未决断、半消息日后被回查丢弃」的假成功；
//   - 放开 apply 后 End 必须返回 found=true，且该消息出现在目标队列。
package txn

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

// sq1ApplyBlock 挂在 OnApplied 上的 apply 闸门：armed 后目标节点目标组的
// 第一条非空条目进入钩子即阻塞，直到 release。
type sq1ApplyBlock struct {
	node    atomic.Uint64
	group   atomic.Uint32
	armed   atomic.Bool
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
	relOnce sync.Once
}

func (b *sq1ApplyBlock) hook(node uint64, g uint32, repr []byte) {
	if node != b.node.Load() || g != b.group.Load() || len(repr) == 0 || !b.armed.Load() {
		return
	}
	b.once.Do(func() { close(b.blocked) })
	<-b.release
}

func (b *sq1ApplyBlock) open() { b.relOnce.Do(func() { close(b.release) }) }

type sq1TxnNode struct {
	id uint64
	st *store.Store
	m  *cluster.Manager
	cl *replication.Cluster
	mt *meta.Meta
	pr *produce.Producer
	tx *Manager
}

func TestSQ1EndOnLaggingMetaLeaderNoFalseSuccess(t *testing.T) {
	const topic = "sq1txn"
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))

	peers := map[uint64]string{}
	lns := map[uint64]net.Listener{}
	for i := uint64(1); i <= 3; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i], peers[i] = ln, ln.Addr().String()
	}
	blk := &sq1ApplyBlock{blocked: make(chan struct{}), release: make(chan struct{})}
	blk.group.Store(cluster.MetaGroup)
	nodes := map[uint64]*sq1TxnNode{}
	base := t.TempDir()
	for i := uint64(1); i <= 3; i++ {
		st, err := store.Open(fmt.Sprintf("%s/%d", base, i), false, lg)
		if err != nil {
			t.Fatal(err)
		}
		id := i
		m, err := cluster.NewManager(cluster.Options{
			NodeID: i, Peers: peers, Listener: lns[i], DataGroups: 3,
			Mode: cluster.AckQuorumMem, Store: st, Logger: lg,
			OnApplied: func(g uint32, repr []byte) { blk.hook(id, g, repr) },
			// 不开摊布循环：领导权只由本测试显式转移
		})
		if err != nil {
			t.Fatal(err)
		}
		m.Start(context.Background())
		nodes[i] = &sq1TxnNode{id: i, st: st, m: m, cl: replication.NewCluster(m)}
	}
	t.Cleanup(func() {
		blk.open()
		for _, n := range nodes {
			_ = n.m.StopClean(context.Background())
		}
		for _, n := range nodes {
			select {
			case <-n.m.Done():
			case <-time.After(10 * time.Second):
			}
			_ = n.st.Close()
		}
	})
	for _, n := range nodes {
		mt, err := meta.New(n.cl, n.cl, n.st, false, 1, 16, lg)
		if err != nil {
			t.Fatal(err)
		}
		n.mt = mt
		n.pr = produce.New(n.cl, n.cl, n.st, mt, lg)
		n.tx = New(n.cl, n.cl, n.st, n.pr, mt, time.Hour, 3, lg)
	}

	leaderOf := func(g uint32) uint64 {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			for _, n := range nodes {
				if n.m.IsLeader(g) {
					return n.id
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("组 %d 30s 内无 leader", g)
		return 0
	}
	transferTo := func(g uint32, to uint64) {
		deadline := time.Now().Add(20 * time.Second)
		for !nodes[to].m.IsLeader(g) {
			if time.Now().After(deadline) {
				t.Fatalf("组 %d 领导权未转到 %d", g, to)
			}
			if cur, ok := nodes[to].m.Leader(g); ok && cur != to {
				nodes[cur].m.TransferLeader(g, to)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitApplied := func(g uint32, F, L uint64, what string) {
		for nodes[F].m.AppliedIndex(g) < nodes[L].m.AppliedIndex(g) {
			if ctx.Err() != nil {
				t.Fatal(what)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// 1. 建单队列 topic，各节点重载 meta
	L := leaderOf(cluster.MetaGroup)
	if _, err := nodes[L].mt.CreateTopic(ctx, topic, 1); err != nil {
		t.Fatalf("建 topic: %v", err)
	}
	for _, n := range nodes {
		for {
			_ = n.mt.Reload()
			if _, ok := n.mt.GetTopic(topic); ok {
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("节点 %d meta 未收敛", n.id)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	var F uint64
	for i := uint64(1); i <= 3; i++ {
		if i != L {
			F = i
			break
		}
	}
	// 目标队列组的领导权预先给 F（且已可写）：End 提交段在 F 本地 Append，
	// 不依赖转发装配；该组 apply 不受闸门影响
	g := nodes[1].m.GroupForQueue(topic, 0)
	transferTo(g, F)
	if _, err := nodes[F].cl.WaitWritable(ctx, g); err != nil {
		t.Fatalf("F 在目标队列组 %d 不可写: %v", g, err)
	}
	blk.node.Store(F)
	t.Logf("meta 旧 leader=%d，apply 滞后节点 F=%d，目标队列组 g=%d", L, F, g)

	// 2. 武装闸门：钩子在批次提交之后触发，故先写一条占位 meta 批次让 F 的
	//    apply 协程卡在它之后，再 Stage（L 本地提案）——Stage 批次已提交、
	//    F 未 apply
	waitApplied(cluster.MetaGroup, F, L, "F 未追平 meta 预热")
	blk.armed.Store(true)
	pb := nodes[L].st.NewBatch()
	pb.Set([]byte("sq1test/placeholder"), []byte("x"))
	if err := nodes[L].cl.Apply(ctx, cluster.MetaGroup, pb); err != nil {
		t.Fatalf("占位 meta 批次: %v", err)
	}
	select {
	case <-blk.blocked:
	case <-ctx.Done():
		t.Fatal("F 的 meta apply 未进入闸门")
	}
	staged, txID, err := nodes[L].tx.Stage(ctx, &core.Message{Topic: topic, Body: []byte("tx-body"), Transactional: true})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	lApplied := nodes[L].m.AppliedIndex(cluster.MetaGroup)
	for {
		st, _ := nodes[F].m.Status(cluster.MetaGroup)
		if st.HardState.GetCommit() >= lApplied {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("F 的 meta 提交位点未追上")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok, _ := nodes[F].st.Get(store.HalfIdxKey(txID)); ok {
		t.Fatal("未制造出窗口：F 本地已能读到 halfidx")
	}

	// 3. meta 领导权转给 F（apply 仍卡着）
	transferTo(cluster.MetaGroup, F)
	t.Logf("F=%d 已当选 meta leader；F.meta.applied=%d < L.applied=%d", F,
		nodes[F].m.AppliedIndex(cluster.MetaGroup), lApplied)

	// 4. 经 F 决断：门未开期间不得返回 found=false 的成功
	type res struct {
		found bool
		err   error
	}
	done := make(chan res, 1)
	go func() {
		found, err := nodes[F].tx.End(ctx, txID, true)
		done <- res{found, err}
	}()
	var early *res
	for obsEnd := time.Now().Add(500 * time.Millisecond); time.Now().Before(obsEnd) && early == nil; {
		select {
		case r := <-done:
			early = &r
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if early != nil && early.err == nil && !early.found {
		t.Fatalf("假成功：apply 未追平时 F 上 End(commit) 返回 found=false err=nil（事务实际未决断）")
	}
	if early != nil {
		t.Logf("门关着期间 End 先行返回 found=%v err=%v（可重试错误，放开后重试）", early.found, early.err)
	} else {
		t.Log("门关着 500ms：End 未返回")
	}

	// 5. 放开 apply：End 必须 found=true（窗口期返回可重试错误的，放开后重试一次）
	blk.open()
	var r res
	if early != nil {
		found, err := nodes[F].tx.End(ctx, txID, true)
		r = res{found, err}
	} else {
		r = <-done
	}
	if r.err != nil || !r.found {
		t.Fatalf("放开后 End(commit) 应 found=true err=nil，得到 found=%v err=%v", r.found, r.err)
	}
	// 消息出现在目标队列（F 是目标队列组 leader，Append 已在 F 本地 apply）
	v, ok, err := nodes[F].st.Get(store.AllocKey(topic, 0))
	if err != nil || !ok {
		t.Fatalf("读目标队列 alloc: ok=%v err=%v", ok, err)
	}
	n := store.GetU64(v)
	var hit bool
	for off := uint64(0); off < n; off++ {
		raw, ok, err := nodes[F].st.Get(store.MsgKey(topic, 0, off))
		if err != nil || !ok {
			continue
		}
		if m, err := core.DecodeMessage(raw); err == nil && m.ID == staged.ID {
			hit = true
			t.Logf("事务消息已入目标队列: q=0 off=%d id=%s", off, m.ID)
		}
	}
	if !hit {
		t.Fatalf("事务消息 %s 未出现在目标队列（alloc=%d）", staged.ID, n)
	}
}
