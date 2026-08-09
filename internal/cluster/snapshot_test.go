// snapshot_test.go 验证快照发送侧机件：groupStorage 现场生成描述符的
// 一致性不变量、注册表按 TTL 回收超时视图。
//
// 职责：
//   - Snapshot 声称的 index 与 ReadView 看到的状态必须同一时刻
//   - 注册表视图超时未拉完必须被 GC 回收（长期持有阻止 Pebble 回收旧版本）
//
// 边界：不覆盖传输与控制通道的分块拉取（Task 5/Task 6）；
// openClusterTestStore/testSlog 复用 raftstore_test.go。
package cluster

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// TestGroupStorageSnapshotIsConsistentWithIndex 快照的核心不变量：
// 描述符声称的 index 与 ReadView 看到的状态必须同一时刻——
// 先取视图后读 applied 会让快照声称的 index 高于内容（静默丢数据）。
func TestGroupStorageSnapshotIsConsistentWithIndex(t *testing.T) {
	st := openClusterTestStore(t)
	mem := raft.NewMemoryStorage()
	// MemoryStorage 要求条目从 1 起连续（brief 原案只 Append index=5
	// 会在「missing log entry」处 panic）：铺满 1..5、term 全 2，
	// 断言只关心 Term(5)=2 与 LastIndex=5，语义不变
	var ents []*raftpb.Entry
	for i := uint64(1); i <= 5; i++ {
		idx, term := i, uint64(2)
		ents = append(ents, &raftpb.Entry{Index: &idx, Term: &term})
	}
	if err := mem.Append(ents); err != nil {
		t.Fatal(err)
	}
	var applied atomic.Uint64
	applied.Store(5)
	reg := newSnapRegistry(st, time.Minute, testSlog(t))
	var cs atomic.Pointer[raftpb.ConfState]
	cs.Store(&raftpb.ConfState{Voters: []uint64{1, 2, 3}})

	// applied 与 FSM 的对应关系：写入 k=5 后 applied=5
	b := st.NewBatch()
	if err := b.Set([]byte("msg/t/0/5"), []byte("v5")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}

	// applyMu 是组 apply 路径与 Snapshot 共享的锁（Task 4 设计：由 group
	// 持有并传入），这里用测试侧独立的锁验证 Snapshot 的取位点语义。
	var mu sync.Mutex
	gs := newGroupStorage(1, mem, reg, &applied, &cs, 7 /*selfID*/, &mu, testSlog(t))
	snap, err := gs.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Metadata.GetIndex() != 5 || snap.Metadata.GetTerm() != 2 {
		t.Fatalf("快照元数据 = index %d term %d; want 5/2", snap.Metadata.GetIndex(), snap.Metadata.GetTerm())
	}
	if len(snap.Metadata.GetConfState().GetVoters()) != 3 {
		t.Fatalf("快照必须带当前成员表: %+v", snap.Metadata.GetConfState())
	}
	d, err := decodeSnapDescriptor(snap.Data)
	if err != nil {
		t.Fatalf("描述符解码: %v", err)
	}
	if d.Index != 5 || d.Leader != 7 {
		t.Fatalf("描述符 = %+v; want index 5 leader 7", d)
	}

	// 生成之后的写入不得进入这份快照
	b2 := st.NewBatch()
	if err := b2.Set([]byte("msg/t/0/6"), []byte("v6")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b2); err != nil {
		t.Fatal(err)
	}
	view, ok := reg.Get(d.ID)
	if !ok {
		t.Fatal("注册表里应有该视图")
	}
	if _, found, _ := view.Get([]byte("msg/t/0/6")); found {
		t.Fatal("快照视图看见了生成之后的写入——一致性被破坏")
	}
	reg.Release(d.ID)
	if _, ok := reg.Get(d.ID); ok {
		t.Fatal("Release 之后不得再取到")
	}
}

// TestSnapRegistryGCReclaimsStaleViews 视图长期不关会撑大磁盘：
// 超时未被拉完的必须被 GC 回收。
func TestSnapRegistryGCReclaimsStaleViews(t *testing.T) {
	st := openClusterTestStore(t)
	reg := newSnapRegistry(st, 50*time.Millisecond, testSlog(t))
	id, _ := reg.Create(1, 10)
	base := time.Now()
	if n := reg.GCOnce(base); n != 0 {
		t.Fatalf("未超时不得回收，回收了 %d", n)
	}
	if n := reg.GCOnce(base.Add(time.Second)); n != 1 {
		t.Fatalf("超时应回收 1 个，回收了 %d", n)
	}
	if _, ok := reg.Get(id); ok {
		t.Fatal("回收后不得再取到")
	}
}
