// sq1_end_epoch_test.go 卡 SQ1 修订 R1.1（审查 M-1）故事级检查：txn.End 的
// 「读不到 halfidx 即幂等成功」判定必须绑定过门时的 epoch。
//
// 窗口（确定性注入，复用 S2 夹具）：L 是 meta leader，End 过门拿到 epoch e
// 之后、读 halfidx 之前（afterGateHook），meta 领导权 L→X；X 上写入一笔事务的
// half 两键（与 Stage 同键同值）并已确认，而 L 的 meta apply 被卡住；再转回 L
// （任期 > e，L 复任但 apply 滞后、门在新任期未开）。随后 L 的 End 读不到
// halfidx——仅复查 IsLeader 会把它判成幂等成功（假成功）。
//
// 判据：这次 End 必须 ErrNotLeader，不得返回 found=false 的成功；放开 apply
// 后重试 End 必须 found=true，且该事务消息出现在目标队列。
package txn

import (
	"context"
	"encoding/json"
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
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

func TestSQ1EndBindsGateEpochAcrossFlip(t *testing.T) {
	const topic = "sq1txnflip"
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
	transferTo := func(g uint32, to uint64) bool {
		deadline := time.Now().Add(20 * time.Second)
		for !nodes[to].m.IsLeader(g) {
			if time.Now().After(deadline) {
				t.Errorf("组 %d 领导权未转到 %d", g, to)
				return false
			}
			if cur, ok := nodes[to].m.Leader(g); ok && cur != to {
				nodes[cur].m.TransferLeader(g, to)
			}
			time.Sleep(20 * time.Millisecond)
		}
		return true
	}

	// 1. 建 topic；meta 与目标队列组的领导权都在 L（目标组已可写）
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
	X := uint64(1)
	if X == L {
		X = 2
	}
	g := nodes[1].m.GroupForQueue(topic, 0)
	if !transferTo(g, L) {
		t.FailNow()
	}
	if _, err := nodes[L].cl.WaitWritable(ctx, g); err != nil {
		t.Fatalf("L 在目标队列组 %d 不可写: %v", g, err)
	}
	blk.node.Store(L)

	// 事务半消息（与 Stage 同键同值，txID 由测试指定以便先发起 End）
	txID := core.NewMessageID()
	msg := &core.Message{ID: core.NewMessageID(), Topic: topic, Body: []byte("tx-flip"), Transactional: true,
		BornAtMs: time.Now().UnixMilli(), StoreAtMs: time.Now().UnixMilli()}
	raw, err := core.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	next := time.Now().Add(time.Hour).UnixMilli()
	ref, _ := json.Marshal(&HalfRef{NextCheckMs: next})

	// 2. End 过门后、读 halfidx 前：meta L→X，X 写 half 两键（L apply 卡住），再 X→L
	var gateEpoch, flipTerm uint64
	nodes[L].tx.afterGateHook = func() {
		nodes[L].tx.afterGateHook = nil
		gateEpoch, _ = nodes[L].cl.WritableEpoch(cluster.MetaGroup)
		if !transferTo(cluster.MetaGroup, X) {
			return
		}
		blk.armed.Store(true)
		pb := nodes[X].st.NewBatch()
		pb.Set([]byte("sq1test/placeholder"), []byte("x"))
		if err := nodes[X].cl.Apply(ctx, cluster.MetaGroup, pb); err != nil {
			t.Errorf("占位 meta 批次: %v", err)
			return
		}
		select {
		case <-blk.blocked:
		case <-ctx.Done():
			t.Error("L 的 meta apply 未进入闸门")
			return
		}
		b := nodes[X].st.NewBatch()
		b.Set(store.HalfKey(next, txID), raw)
		b.Set(store.HalfIdxKey(txID), ref)
		if err := nodes[X].cl.Apply(ctx, cluster.MetaGroup, b); err != nil {
			t.Errorf("X 上写 half 两键: %v", err)
			return
		}
		xApplied := nodes[X].m.AppliedIndex(cluster.MetaGroup)
		for {
			st, _ := nodes[L].m.Status(cluster.MetaGroup)
			if st.HardState.GetCommit() >= xApplied {
				break
			}
			if ctx.Err() != nil {
				t.Error("L 的 meta 提交位点未追上")
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !transferTo(cluster.MetaGroup, L) {
			return
		}
		st, _ := nodes[L].m.Status(cluster.MetaGroup)
		flipTerm = st.GetTerm()
	}
	found, err := nodes[L].tx.End(ctx, txID, true)
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("门 epoch=%d，L 复任 meta 任期=%d，L.meta.applied=%d", gateEpoch, flipTerm,
		nodes[L].m.AppliedIndex(cluster.MetaGroup))
	if flipTerm <= gateEpoch {
		t.Fatalf("未制造出换届：复任任期 %d 不大于门 epoch %d", flipTerm, gateEpoch)
	}
	if _, ok, _ := nodes[L].st.Get(store.HalfIdxKey(txID)); ok {
		t.Fatal("未制造出窗口：L 本地已能读到 halfidx")
	}
	if err == nil && !found {
		t.Fatalf("假成功：门 epoch 已过期、L apply 滞后时 End(commit) 返回 found=false err=nil（事务实际未决断）")
	}
	if !errors.Is(err, replication.ErrNotLeader) {
		t.Fatalf("End 应返回 ErrNotLeader，得到 found=%v err=%v", found, err)
	}
	t.Logf("门 epoch 过期的 End 被拒: %v", err)

	// 3. 放开 apply 后重试：found=true，消息入目标队列
	blk.open()
	found, err = nodes[L].tx.End(ctx, txID, true)
	if err != nil || !found {
		t.Fatalf("放开后 End(commit) 应 found=true err=nil，得到 found=%v err=%v", found, err)
	}
	v, ok, err := nodes[L].st.Get(store.AllocKey(topic, 0))
	if err != nil || !ok {
		t.Fatalf("读目标队列 alloc: ok=%v err=%v", ok, err)
	}
	var hit bool
	for off := uint64(0); off < store.GetU64(v); off++ {
		mraw, ok, err := nodes[L].st.Get(store.MsgKey(topic, 0, off))
		if err != nil || !ok {
			continue
		}
		if m, err := core.DecodeMessage(mraw); err == nil && m.ID == msg.ID {
			hit = true
			t.Logf("事务消息已入目标队列: q=0 off=%d id=%s", off, m.ID)
		}
	}
	if !hit {
		t.Fatalf("事务消息 %s 未出现在目标队列", msg.ID)
	}
}
