// sq1_failover_offset_test.go 复现卡 SQ1：切主后新 leader 未等 apply 追平
// 就从本地 pebble 重读 alloc/ 计数器，分配到与「已提交未 apply」尾部重叠
// 的 offset，覆盖已确认（客户端拿到成功应答）的消息。
//
// 判据（直接对着 bug 后果）：
//   - 每条拿到成功应答的消息，按它被告知的 (queue, offset) 在全部节点
//     读回来，ID 必须仍是它自己；
//   - 不存在两条已确认消息被告知同一个 (queue, offset)。
//
// 任一不成立即为红（已确认消息被覆盖 / 同一 offset 被分配两次）。
//
// 窗口制造：只用现成装配钩子 cluster.Options.OnApplied——它在组 apply
// 协程内、批次提交之后同步触发（internal/cluster/group.go applyEntries
// → notifyApplied）。在 follower F 的目标组上让钩子阻塞，即令 F 的
// apply 协程停住而 raft 日志追加与提交照常推进，得到「F 的 applied
// 落后于已提交」的窗口；随后把领导权转给 F，在其当选后、apply 追平前
// 立即经 F 的 Producer 写一条，再放开钩子。
//
// 装配与 cmd/sq/main.go 一致：每节点真实 store + cluster.Manager +
// replication.Cluster + meta.Meta + Producer，OnLeaderChange(isSelf) 同步
// 调 Producer.InvalidateCounters。
package produce

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
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

// sq1Node 是一个进程内节点的全套装配。
type sq1Node struct {
	id  uint64
	st  *store.Store
	m   *cluster.Manager
	cl  *replication.Cluster
	mt  *meta.Meta
	pr  *Producer
	prP atomic.Pointer[Producer]
}

// sq1ApplyGate 挂在 OnApplied 钩子上的 apply 闸门：armed 后，目标节点
// 目标组的第一条非空条目进入钩子即阻塞，直到 release。
type sq1ApplyGate struct {
	node    atomic.Uint64
	group   atomic.Uint32
	armed   atomic.Bool
	blocked chan struct{} // 钩子已阻塞（close 一次）
	release chan struct{}
	once    sync.Once
}

func (g *sq1ApplyGate) hook(node uint64, grp uint32, repr []byte) {
	if node != g.node.Load() || len(repr) == 0 || !g.armed.Load() || grp != g.group.Load() {
		return
	}
	g.once.Do(func() { close(g.blocked) })
	<-g.release
}

func TestSQ1FailoverStaleOffsetOverwritesAckedMessage(t *testing.T) {
	const topic = "sq1topic"
	const nTail = 5 // 「已确认但新 leader 未 apply」的尾部条数
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. 三节点装配
	lns := make([]net.Listener, 0, 3)
	peers := map[uint64]string{}
	for i := uint64(1); i <= 3; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns = append(lns, ln)
		peers[i] = ln.Addr().String()
	}
	gate := &sq1ApplyGate{blocked: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate.release) }) }
	defer releaseGate()

	nodes := map[uint64]*sq1Node{}
	base := t.TempDir()
	for i := uint64(1); i <= 3; i++ {
		n := &sq1Node{id: i}
		st, err := store.Open(fmt.Sprintf("%s/%d", base, i), false, lg)
		if err != nil {
			t.Fatal(err)
		}
		n.st = st
		id := i
		m, err := cluster.NewManager(cluster.Options{
			NodeID: i, Peers: peers, Listener: lns[i-1], DataGroups: 3,
			Mode: cluster.AckQuorumMem, Store: st, Logger: lg,
			// 与 main.go:243-256 同：本节点当选即同步失效计数器缓存
			OnLeaderChange: func(g uint32, leader uint64, isSelf bool) {
				if !isSelf {
					return
				}
				if p := n.prP.Load(); p != nil {
					p.InvalidateCounters()
				}
			},
			OnApplied: func(g uint32, repr []byte) { gate.hook(id, g, repr) },
			// 不开摊布循环：领导权只由本测试显式转移
		})
		if err != nil {
			t.Fatal(err)
		}
		m.Start(context.Background())
		n.m = m
		n.cl = replication.NewCluster(m)
		nodes[i] = n
	}
	t.Cleanup(func() {
		releaseGate()
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
		n.pr = New(n.cl, n.cl, n.st, mt, lg)
		n.prP.Store(n.pr)
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

	// 2. 建单队列 topic（meta leader 本地提案），各节点重载 meta
	metaLead := leaderOf(cluster.MetaGroup)
	if _, err := nodes[metaLead].mt.CreateTopic(ctx, topic, 1); err != nil {
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
	g := nodes[1].m.GroupForQueue(topic, 0)
	L := leaderOf(g)
	var F uint64
	for i := uint64(1); i <= 3; i++ {
		if i != L {
			F = i
			break
		}
	}
	gate.node.Store(F)
	gate.group.Store(g)
	t.Logf("数据组 g=%d 旧 leader=%d，apply 滞后节点 F=%d", g, L, F)

	type acked struct {
		id  string
		q   uint32
		off uint64
		via uint64
	}
	var acks []acked
	appendVia := func(id uint64, body string) {
		m, err := nodes[id].pr.Append(ctx, &core.Message{Topic: topic, Body: []byte(body)})
		if err != nil {
			t.Fatalf("经节点 %d Append %s: %v", id, body, err)
		}
		acks = append(acks, acked{id: m.ID, q: m.QueueID, off: m.Offset, via: id})
	}

	// 3. 预热：L 上写一条，确认组内复制通
	appendVia(L, "warm")
	// 等 F 把 warm 也 apply 掉，闸门才会确定地卡在 M0 上（否则卡在 warm）
	for nodes[F].m.AppliedIndex(g) < nodes[L].m.AppliedIndex(g) {
		if ctx.Err() != nil {
			t.Fatal("F 未追平预热写入")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 4. 武装闸门：M0 进 F 的 apply 后 F 的 apply 协程停在钩子里
	gate.armed.Store(true)
	appendVia(L, "m0")
	select {
	case <-gate.blocked:
	case <-ctx.Done():
		t.Fatal("F 的 apply 未进入闸门")
	}
	// 5. 尾部：全部在 L 上拿到成功应答（L apply = 多数派已提交）
	for i := 1; i <= nTail; i++ {
		appendVia(L, fmt.Sprintf("m%d", i))
	}
	lApplied := nodes[L].m.AppliedIndex(g)
	// F 的提交位点追到 L 的 applied（心跳/追加携带 commit）
	for {
		st, _ := nodes[F].m.Status(g)
		if st.HardState.GetCommit() >= lApplied {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("F 提交位点未追上")
		}
		time.Sleep(10 * time.Millisecond)
	}
	fst, _ := nodes[F].m.Status(g)
	t.Logf("窗口: L.applied=%d F.commit=%d F.applied=%d", lApplied, fst.HardState.GetCommit(), nodes[F].m.AppliedIndex(g))
	if nodes[F].m.AppliedIndex(g) >= lApplied {
		t.Fatalf("未制造出 applied<committed 窗口")
	}

	// 6. 领导权转给 F
	for !nodes[F].m.IsLeader(g) {
		if cur, ok := nodes[L].m.Leader(g); ok {
			nodes[cur].m.TransferLeader(g, F)
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && !nodes[F].m.IsLeader(g) {
			time.Sleep(10 * time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatal("领导权未转到 F")
		}
	}
	t.Logf("F=%d 已当选；此刻 F.applied=%d（尾部未 apply）", F, nodes[F].m.AppliedIndex(g))
	// 等新任期 no-op 提交并稳定，取基线
	var c0 uint64
	for stable := 0; stable < 10; {
		st, _ := nodes[F].m.Status(g)
		if st.HardState.GetCommit() == c0 {
			stable++
		} else {
			c0, stable = st.HardState.GetCommit(), 0
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 7. apply 追平前立即经 F 写入（F 的 propose 等本地 apply，故异步发起）
	type res struct {
		m   *core.Message
		err error
	}
	done := make(chan res, 1)
	go func() {
		m, err := nodes[F].pr.Append(ctx, &core.Message{Topic: topic, Body: []byte("x-after-failover")})
		done <- res{m, err}
	}()
	// 新写入已进日志且被多数派提交（不依赖 F 的 apply）
	for {
		st, _ := nodes[F].m.Status(g)
		if st.HardState.GetCommit() > c0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("F 上的新写入未提交")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 8. 放开 apply，等结果与全节点追平
	releaseGate()
	r := <-done
	if r.err != nil {
		t.Fatalf("经新 leader F Append 失败: %v", r.err)
	}
	acks = append(acks, acked{id: r.m.ID, q: r.m.QueueID, off: r.m.Offset, via: F})
	for {
		a := nodes[F].m.AppliedIndex(g)
		if nodes[1].m.AppliedIndex(g) == a && nodes[2].m.AppliedIndex(g) == a && nodes[3].m.AppliedIndex(g) == a {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("三节点 apply 未追平")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 9. 判据
	for _, a := range acks {
		t.Logf("已确认: id=%s q=%d off=%d via=%d", a.id, a.q, a.off, a.via)
	}
	seen := map[[2]uint64]string{}
	for _, a := range acks {
		k := [2]uint64{uint64(a.q), a.off}
		if prev, dup := seen[k]; dup {
			t.Errorf("同一 offset 被分配两次: q=%d off=%d 先给 %s 又给 %s", a.q, a.off, prev, a.id)
		}
		seen[k] = a.id
	}
	for _, a := range acks {
		for _, id := range []uint64{1, 2, 3} {
			raw, ok, err := nodes[id].st.Get(store.MsgKey(topic, a.q, a.off))
			if err != nil || !ok {
				t.Errorf("节点 %d 读不到已确认消息 %s (q=%d off=%d): ok=%v err=%v", id, a.id, a.q, a.off, ok, err)
				continue
			}
			got, err := core.DecodeMessage(raw)
			if err != nil {
				t.Errorf("节点 %d 解码 q=%d off=%d: %v", id, a.q, a.off, err)
				continue
			}
			if got.ID != a.id {
				t.Errorf("已确认消息被覆盖: 节点 %d q=%d off=%d 原消息 %s(via %d) 现为 %s(body=%q)",
					id, a.q, a.off, a.id, a.via, got.ID, got.Body)
			}
		}
	}
}
