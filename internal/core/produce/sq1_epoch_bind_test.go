// sq1_epoch_bind_test.go 卡 SQ1 计划修订 R1 的故事级检查：提案必须绑定
// 「过可写门时拿到的 epoch」，而不是入队那一刻的任期。
//
// 窗口（确定性注入）：L 在任期 e 内写过（offset 缓存属 e）；下一次 Append
// 过门拿到 e 之后、入队之前（afterGateHook），领导权转给 X，X 写入若干条
// 并拿到确认，再转回 L（任期 > e，L 的门在新任期重新打开）。随后 L 的那次
// Append 继续：缓存 epoch 仍是 e，会分配到 X 已确认的 offset。
//
// 判据：
//   - 这次 Append 必须以 ErrNotLeader 失败（提案按 e 绑定，入队时任期已变）；
//   - 每条已确认消息在三节点按其 (q, off) 读回仍是它自己，无重复 offset；
//   - 随后重试 Append 成功，offset 严格大于全部已确认 offset。
package produce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

func TestSQ1ProposalBoundToGateEpoch(t *testing.T) {
	const topic = "sq1bind"
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
	nodes := map[uint64]*sq1Node{}
	base := t.TempDir()
	for i := uint64(1); i <= 3; i++ {
		st, err := store.Open(fmt.Sprintf("%s/%d", base, i), false, lg)
		if err != nil {
			t.Fatal(err)
		}
		m, err := cluster.NewManager(cluster.Options{
			NodeID: i, Peers: peers, Listener: lns[i], DataGroups: 3,
			Mode: cluster.AckQuorumMem, Store: st, Logger: lg,
			// 不开摊布循环：领导权只由本测试显式转移
		})
		if err != nil {
			t.Fatal(err)
		}
		m.Start(context.Background())
		nodes[i] = &sq1Node{id: i, st: st, m: m, cl: replication.NewCluster(m)}
	}
	t.Cleanup(func() {
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
				t.Errorf("组 %d 领导权未转到 %d", g, to)
				return
			}
			if cur, ok := nodes[to].m.Leader(g); ok && cur != to {
				nodes[cur].m.TransferLeader(g, to)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

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
	X := uint64(1)
	if X == L {
		X = 2
	}

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
			t.Errorf("经节点 %d Append %s: %v", id, body, err)
			return
		}
		acks = append(acks, acked{id: m.ID, q: m.QueueID, off: m.Offset, via: id})
	}

	// 1. L 在任期 e 内写两条：offset 缓存装载并属 e
	appendVia(L, "a0")
	appendVia(L, "a1")
	if t.Failed() {
		t.FailNow()
	}

	// 2. 下一次经 L 的 Append：过门后、入队前，领导权 L→X→L，X 期间写 3 条
	var gateEpoch, newTerm uint64
	nodes[L].pr.afterGateHook = func(hg uint32, epoch uint64) {
		nodes[L].pr.afterGateHook = nil // 只注入一次（同一 goroutine 内，无竞争）
		gateEpoch = epoch
		transferTo(g, X)
		for i := 0; i < 3; i++ {
			appendVia(X, fmt.Sprintf("x%d", i))
		}
		transferTo(g, L)
		e2, err := nodes[L].cl.WaitWritable(ctx, g)
		if err != nil {
			t.Errorf("L 复任后门未打开: %v", err)
		}
		newTerm = e2
	}
	stale, err := nodes[L].pr.Append(ctx, &core.Message{Topic: topic, Body: []byte("stale-after-flip")})
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("门 epoch=%d，L 复任后任期=%d", gateEpoch, newTerm)
	if newTerm <= gateEpoch {
		t.Fatalf("未制造出换届：复任任期 %d 不大于门 epoch %d", newTerm, gateEpoch)
	}
	if err == nil {
		t.Errorf("门 epoch 已过期的 Append 却成功: q=%d off=%d（提案被绑到新任期）", stale.QueueID, stale.Offset)
		acks = append(acks, acked{id: stale.ID, q: stale.QueueID, off: stale.Offset, via: L})
	} else if !errors.Is(err, replication.ErrNotLeader) {
		t.Errorf("门 epoch 已过期的 Append 应返回 ErrNotLeader，得到 %v", err)
	} else {
		t.Logf("门 epoch 过期的 Append 被拒: %v", err)
	}

	// 3. 重试成功，offset 严格大于全部已确认 offset
	retry, err := nodes[L].pr.Append(ctx, &core.Message{Topic: topic, Body: []byte("retry")})
	if err != nil {
		t.Fatalf("重试 Append: %v", err)
	}
	for _, a := range acks {
		if a.q == retry.QueueID && retry.Offset <= a.off {
			t.Errorf("重试分配的 offset=%d 不大于已确认 offset=%d（%s）", retry.Offset, a.off, a.id)
		}
	}
	acks = append(acks, acked{id: retry.ID, q: retry.QueueID, off: retry.Offset, via: L})
	for {
		a := nodes[L].m.AppliedIndex(g)
		if nodes[1].m.AppliedIndex(g) == a && nodes[2].m.AppliedIndex(g) == a && nodes[3].m.AppliedIndex(g) == a {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("三节点 apply 未追平")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 4. 判据：无重复 offset；每条已确认消息在三节点读回仍是它自己
	seen := map[[2]uint64]string{}
	for _, a := range acks {
		t.Logf("已确认: id=%s q=%d off=%d via=%d", a.id, a.q, a.off, a.via)
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
