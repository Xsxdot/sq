// snapshot_test.go 验证快照发送侧机件：groupStorage 现场生成描述符的
// 一致性不变量、注册表借用计数与按 TTL 回收超时视图。
//
// 职责：
//   - Snapshot 声称的 index 与 ReadView 看到的状态必须同一时刻
//   - 注册表视图超时未拉完必须被 GC 回收（长期持有阻止 Pebble 回收旧版本）
//   - 借出中的视图 GC 不得回收（Close 会炸正在扫描的借用者）
//   - 借出续期：活跃拉取的视图 TTL 不因创建久远而失效（防大库传输活锁）
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
	reg.Put(d.ID)
	// 归还后条目仍可借出（多块续拉语义：块间窗口内不注销），
	// TTL 回收后才不可得
	reg.GCOnce(time.Now().Add(time.Hour))
	if _, ok := reg.Get(d.ID); ok {
		t.Fatal("回收后不得再取到")
	}
}

// TestSnapRegistryGCReclaimsStaleViews 视图长期不关会撑大磁盘：
// 超时未被拉完的必须被 GC 回收。
func TestSnapRegistryGCReclaimsStaleViews(t *testing.T) {
	st := openClusterTestStore(t)
	reg := newSnapRegistry(st, 50*time.Millisecond, testSlog(t))
	id := reg.Create(1, 10)
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

// TestSnapRegistryBorrowSurvivesGC 借出中的视图 GC 不得回收：
// 即使远超 TTL，只要借用未归还（refs>0）就跳过——Close 正在被扫描
// 的视图会让 pebble 对已关快照建迭代器直接 panic（无 recover 即进程
// 死亡），借出是一次分块扫描、短命，顺延到下一轮 GC 即可。
func TestSnapRegistryBorrowSurvivesGC(t *testing.T) {
	st := openClusterTestStore(t)
	reg := newSnapRegistry(st, 50*time.Millisecond, testSlog(t))
	clock := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	reg.now = func() time.Time { return clock }

	id := reg.Create(1, 10) // created = clock
	view, ok := reg.Get(id)
	if !ok {
		t.Fatal("应能借出视图")
	}
	_ = view
	clock = clock.Add(time.Minute) // 远超 TTL（且 Get 已把 created 续期到 borrow 时刻）
	if n := reg.GCOnce(clock); n != 0 {
		t.Fatalf("借出中的视图不得被回收，回收了 %d", n)
	}
	reg.Put(id) // 归还：无借用了，且已超 TTL，下一轮 GC 应回收
	if n := reg.GCOnce(clock); n != 1 {
		t.Fatalf("归还后超 TTL 应回收 1 个，回收了 %d", n)
	}
	if _, ok := reg.Get(id); ok {
		t.Fatal("回收后不得再借出")
	}
}

// TestSnapRegistryBorrowRenewsTTL 借出续期：每次 Get 刷新 created，
// 活跃拉取中的视图 TTL 不因「创建于久远时刻」而失效。
//
// 这是旧「Get 不续期」语义的回归防线：大库传输整体耗时超过固定 TTL
// 时，视图会在传输中途被回收 → 对端拿未知 snapID → 重试新快照 → 再
// 回收 → 活锁。续期后每条拉取都刷新基线，只有「整段 TTL 无拉取」的
// 视图才被回收。
func TestSnapRegistryBorrowRenewsTTL(t *testing.T) {
	st := openClusterTestStore(t)
	reg := newSnapRegistry(st, time.Minute, testSlog(t))
	clock := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	reg.now = func() time.Time { return clock }

	id := reg.Create(1, 10) // created = clock
	clock = clock.Add(30 * time.Second)
	if _, ok := reg.Get(id); !ok { // +0.5*ttl 借出：created 续期为 clock
		t.Fatal("应能借出视图")
	}
	if n := reg.GCOnce(clock); n != 0 {
		t.Fatalf("借出中未过期不得回收，回收了 %d", n)
	}
	reg.Put(id) // 归还；created 仍是续期后的 +0.5*ttl
	// 相对初始 created 已 1.0*ttl——若无续期这里就该被回收（活锁源头）；
	// 续期把判活基线推迟了 0.5*ttl，这里仍存活
	clock = clock.Add(30 * time.Second)
	if n := reg.GCOnce(clock); n != 0 {
		t.Fatalf("续期后未过期不得回收，回收了 %d", n)
	}
	// 相对续期基线已 1.0*ttl（初始 +1.5*ttl）：无借用、已过期 → 回收
	clock = clock.Add(31 * time.Second)
	if n := reg.GCOnce(clock); n != 1 {
		t.Fatalf("续期基线过期后应回收 1 个，回收了 %d", n)
	}
}

// TestSnapRegistryConcurrentBorrowGC 并发借出/归还/GC/注销不 panic
// 且不出现「借出中的视图被 Close」：-race 下全速轰炸（极短 TTL 让
// 每轮 GC 都命中过期窗口），借出者借出后立即读视图，读到「已关闭」
// 错误即失败。
func TestSnapRegistryConcurrentBorrowGC(t *testing.T) {
	st := openClusterTestStore(t)
	reg := newSnapRegistry(st, time.Nanosecond, testSlog(t))
	const workers, ops = 4, 1000
	ids := make([]uint64, workers)
	for i := 0; i < workers; i++ {
		ids[i] = reg.Create(uint32(i), uint64(i))
	}
	var wg sync.WaitGroup
	var closedBorrowed atomic.Int32 // 借出视图被 Close 的次数（应恒为 0）
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				view, ok := reg.Get(id)
				if ok {
					// 借用窗口内读视图：若 GCOnce/Release 曾 Close 它，
					// 读操作返回「视图已关闭」错误
					if _, _, err := view.Get([]byte("no/such/key")); err != nil {
						closedBorrowed.Add(1)
					}
					reg.Put(id)
				}
				if j%7 == 0 {
					// 独立于借用的注销尝试：refs>0 时跳过（借用者仍在
					// 扫描，Close 会炸它），refs==0 时移除
					reg.Release(id)
				}
				reg.GCOnce(time.Now())
			}
		}(ids[i])
	}
	wg.Wait()
	if n := closedBorrowed.Load(); n != 0 {
		t.Fatalf("借出中的视图被关闭 %d 次", n)
	}
	// 收尾：远未来 now 一次全量回收后，任何 id 都不可再借出
	reg.GCOnce(time.Now().Add(time.Hour))
	for _, id := range ids {
		if _, ok := reg.Get(id); ok {
			t.Fatalf("全量回收后 snapID %d 仍可借出", id)
		}
	}
}
