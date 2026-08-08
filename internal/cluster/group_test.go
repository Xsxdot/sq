// group_test.go 验证单组运行体的最小闭环与超时清理。
//
// 职责：单节点自选举 + propose apply 全链路（applied 与 FSM 同批原子）、
// 无 quorum 时 propose 按 ctx 超时返回且 waiter 不残留。
// 边界：不覆盖多节点选举与消息语义（Task 3/Task 5 范围）；
// openClusterTestStore/testSlog/testWriter 复用 raftstore_test.go。
package cluster

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/xushixin/sq/internal/store"
)

// startSingleNodeGroup 启动一个自选举单节点组（成员表仅自身），
// 组运行于测试生命周期内，t.Cleanup 负责取消并等待 run 退出。
// send 回调为空函数：单节点无外发需求。
func startSingleNodeGroup(t *testing.T, g uint32, rs *raftStore, st *store.Store, mode AckMode) *group {
	t.Helper()
	storage := raft.NewMemoryStorage()
	rn := raft.StartNode(raftConfig(1, storage), []raft.Peer{{ID: 1}})
	ctx, cancel := context.WithCancel(context.Background())
	gr := newGroup(g, rn, storage, rs, st, func(uint32, []*raftpb.Message) {}, mode, testSlog(t))
	go gr.run(ctx)
	// 先注册等 done、后注册 cancel：LIFO 保证 cancel 先于等待执行，
	// 否则等待会在取消前启动，组永远不会退出。
	t.Cleanup(func() {
		select {
		case <-gr.done():
		case <-time.After(5 * time.Second):
			t.Error("group did not shut down within 5s")
		}
	})
	t.Cleanup(cancel)
	return gr
}

// startLoneGroupOfThree 成员表为 {1,2,3} 但只启动节点 1：永无 quorum，
// 选举与提交都无法完成——超时类测试的地基。
func startLoneGroupOfThree(t *testing.T, g uint32, rs *raftStore, st *store.Store) *group {
	t.Helper()
	storage := raft.NewMemoryStorage()
	rn := raft.StartNode(raftConfig(1, storage), []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	ctx, cancel := context.WithCancel(context.Background())
	gr := newGroup(g, rn, storage, rs, st, func(uint32, []*raftpb.Message) {}, AckQuorumFsync, testSlog(t))
	go gr.run(ctx)
	t.Cleanup(func() {
		select {
		case <-gr.done():
		case <-time.After(5 * time.Second):
			t.Error("group did not shut down within 5s")
		}
	})
	t.Cleanup(cancel)
	return gr
}

// mustApplied 是 rs.Applied 的 fatal 包装：测试断言磁盘 applied 位点用。
func mustApplied(t *testing.T, rs *raftStore, g uint32) uint64 {
	t.Helper()
	idx, err := rs.Applied(g)
	if err != nil {
		t.Fatalf("rs.Applied(%d): %v", g, err)
	}
	return idx
}

// TestGroupSingleNodeProposeApply 单节点单组最小闭环：propose 的批次
// 字节 apply 进共享 store，且 applied_index 与 FSM 写同批落盘（spec §5
// 原子性承诺的直接断言）。
func TestGroupSingleNodeProposeApply(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	gr := startSingleNodeGroup(t, 0, rs, st, AckQuorumFsync) // helper 见下
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b := st.NewBatch()
	if err := b.Set([]byte("meta/topic/demo"), []byte("payload")); err != nil {
		t.Fatal(err)
	}
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil { // 提案路径只取字节，原批次弃用
		t.Fatal(err)
	}
	if err := gr.propose(ctx, repr); err != nil {
		t.Fatal(err)
	}
	// propose 返回即本节点已 apply：立即可读，且 applied_index 已持久化
	if _, ok, _ := st.Get([]byte("meta/topic/demo")); !ok {
		t.Fatal("propose 返回后 FSM 键不可读——读己之写被破坏")
	}
	if idx, err := rs.Applied(0); err != nil || idx == 0 {
		t.Fatalf("applied_index = %d, %v; want >0", idx, err)
	}
	if idx, gidx := mustApplied(t, rs, 0), gr.appliedIndex(); idx != gidx {
		t.Fatalf("盘上 applied %d != 内存 applied %d（必须同批原子）", idx, gidx)
	}
}

// TestGroupProposeCtxTimeout 无 quorum（三成员只起一个）时 propose 按
// ctx 超时返回，waiter 被清理（泄漏检测靠后续 -race 与 map 断言）。
func TestGroupProposeCtxTimeout(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	gr := startLoneGroupOfThree(t, 0, rs, st) // 成员表 {1,2,3} 只起节点 1：永无 quorum
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := gr.propose(ctx, []byte("x")); err == nil {
		t.Fatal("无 quorum 的 propose 应超时报错，得到 nil")
	}
	gr.mu.Lock()
	n := len(gr.propWaiters)
	gr.mu.Unlock()
	if n != 0 {
		t.Fatalf("超时后 propWaiters 残留 %d 个 waiter", n)
	}
}
