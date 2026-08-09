// group_test.go 验证单组运行体的最小闭环与超时清理。
//
// 职责：单节点自选举 + propose apply 全链路（applied 与 FSM 同批原子）、
// 无 quorum 时 propose 按 ctx 超时返回且 waiter 不残留。
// 边界：不覆盖多节点选举与消息语义（Task 3/Task 5 范围）；
// openClusterTestStore/testSlog/testWriter 复用 raftstore_test.go。
package cluster

import (
	"context"
	"encoding/binary"
	"sync/atomic"
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
	gr := newGroup(g, 1, storage, nil, rs, st, func(uint32, []*raftpb.Message) {}, mode, nil, nil, testSlog(t))
	// raft 节点在 newGroup 之后装配（Config.Storage 用包了快照生成器
	// 的 gr.stg，见 newGroup 注释），回填 gr.rn 后启动
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	ctx, cancel := context.WithCancel(context.Background())
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
	gr := newGroup(g, 1, storage, nil, rs, st, func(uint32, []*raftpb.Message) {}, AckQuorumFsync, nil, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	gr.rn = rn
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

// newTestGroupWithHook 构造一个注入 OnLeaderChange 钩子的单节点自选举组
// （成员表仅自身，同 startSingleNodeGroup 的装配方式），但不启动 run
// 循环——钩子闭包通常捕获尚未赋值的 gr 变量，run 必须由调用方在
// gr 赋值之后再启动（钩子触发前 gr 必然已赋值，无竞争）。
func newTestGroupWithHook(t *testing.T, hook func(g uint32, leader uint64, isSelf bool)) *group {
	t.Helper()
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	storage := raft.NewMemoryStorage()
	gr := newGroup(0, 1, storage, nil, rs, st, func(uint32, []*raftpb.Message) {}, AckQuorumFsync, hook, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	return gr
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

// TestProposalWaiterScopedToProposer 终审 R4：waiter id 是每节点独立
// 计数器，跨节点可能撞车——条目头带提案者身份，只有本节点提案才
// 唤醒 waiter（别节点条目 = 丢失提案的 id 恰好撞上，不得假成功）；
// 且 nextID 以时间戳做种子（重启不回零，双保险）。
func TestProposalWaiterScopedToProposer(t *testing.T) {
	self := uint64(7)
	other := uint64(9)
	payload := []byte("payload")
	mk := func(proposer, id uint64) []byte {
		data := make([]byte, 16+len(payload))
		binary.BigEndian.PutUint64(data[:8], proposer)
		binary.BigEndian.PutUint64(data[8:16], id)
		copy(data[16:], payload)
		return data
	}
	if id, ok := proposalWaiter(mk(other, 42), self); ok {
		t.Fatalf("别节点条目（proposer=%d）应 ok=false，得到 id=%d", other, id)
	}
	if id, ok := proposalWaiter(mk(self, 42), self); !ok || id != 42 {
		t.Fatalf("本节点条目 = id %d, ok %v; want 42, true", id, ok)
	}
	if id, ok := proposalWaiter([]byte{0x01, 0x02}, self); ok {
		t.Fatalf("不足 16B 的条目应 ok=false，得到 id=%d", id)
	}
	// nextID 时间戳种子：newGroup 后计数器必须远离 0（重启回零是
	// 跨节点碰撞的第二条路径，由种子 + 提案者校验双保险覆盖）
	storage := raft.NewMemoryStorage()
	gr := newGroup(0, 1, storage, nil, nil, nil, func(uint32, []*raftpb.Message) {}, AckQuorumFsync, nil, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	if gr.nextID.Load() == 0 {
		t.Fatal("nextID 应为时间戳种子（非零）——重启后计数器不得回零")
	}
}

// TestCCWaiterInfoProposerScoped 终审 R4：成员变更的 waiter 通知同样
// 按提案者作用域——Context 携带 [提案者][waiter id]，只有本节点发起的
// ConfChange 才唤醒 ccWaiters（跨节点 id 碰撞不得假成功）。
func TestCCWaiterInfoProposerScoped(t *testing.T) {
	self := uint64(7)
	other := uint64(9)
	mk := func(proposer, id uint64) *raftpb.ConfChangeV2 {
		ctx := make([]byte, 16)
		binary.BigEndian.PutUint64(ctx[:8], proposer)
		binary.BigEndian.PutUint64(ctx[8:16], id)
		return &raftpb.ConfChangeV2{Context: ctx}
	}
	if id, ok := ccWaiterInfo(mk(other, 42), self); ok {
		t.Fatalf("别节点 Context 应 ok=false，得到 id=%d", id)
	}
	if id, ok := ccWaiterInfo(mk(self, 42), self); !ok || id != 42 {
		t.Fatalf("本节点 Context = id %d, ok %v; want 42, true", id, ok)
	}
	if id, ok := ccWaiterInfo(&raftpb.ConfChangeV2{}, self); ok {
		t.Fatalf("nil Context 应 ok=false，得到 id=%d", id)
	}
}

// TestGroupStepAfterDoneDoesNotBlock 终审 R4：组退出后 step 必须立即
// 返回（丢弃消息）——传输读 goroutine 由整条连接共享，阻塞在某一组
// 的 step 会拖死同连接上其余所有组的消息投递。
func TestGroupStepAfterDoneDoesNotBlock(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	storage := raft.NewMemoryStorage()
	gr := newGroup(0, 1, storage, nil, rs, st, func(uint32, []*raftpb.Message) {}, AckQuorumFsync, nil, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	ctx, cancel := context.WithCancel(context.Background())
	go gr.run(ctx)
	cancel()
	select {
	case <-gr.done():
	case <-time.After(5 * time.Second):
		t.Fatal("组未在 5s 内退出")
	}
	from, to := uint64(1), uint64(1)
	typ := raftpb.MsgHeartbeat
	msg := &raftpb.Message{Type: &typ, From: &from, To: &to}
	done := make(chan struct{})
	go func() {
		gr.step(msg) // 组已退出：必须立即返回，不阻塞传输读循环
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("step 在组退出后阻塞——违反不阻塞契约")
	}
}

// TestLeaderVisibleOnlyAfterHookCompletes batch③ 评审 m1：
// gr.lead.Store 早于 notifyLeaderChange，中间还隔着一条日志——
// 这段窗口里 IsLeader 已放行而计数器缓存尚未失效，并发 Append
// 会用陈旧 offset 覆写已 quorum 提交的消息。
// 屏障要求：钩子跑完之前 IsLeader 必须仍为 false。
func TestLeaderVisibleOnlyAfterHookCompletes(t *testing.T) {
	var sawLeaderInsideHook atomic.Bool
	var gr *group
	hook := func(g uint32, leader uint64, isSelf bool) {
		if !isSelf {
			return
		}
		// 钩子执行期间，对外可见的 leader 身份必须还没生效
		if gr.isLeader() {
			sawLeaderInsideHook.Store(true)
		}
	}
	gr = newTestGroupWithHook(t, hook) // 辅助：单节点组 + 注入钩子
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	go gr.run(ctx)
	// 清理顺序同 startSingleNodeGroup：先注册等 done、后注册 cancel
	// （LIFO 先 cancel 后等待）。必须等 run 完全退出——否则 run 可能
	// 仍在 apply 时，openClusterTestStore 的关库清理已执行（pebble: closed）。
	t.Cleanup(func() {
		select {
		case <-gr.done():
		case <-time.After(5 * time.Second):
			t.Error("group did not shut down within 5s")
		}
	})
	t.Cleanup(cancel)
	waitFor(t, 5*time.Second, gr.isLeader, "单节点组未当选")
	if sawLeaderInsideHook.Load() {
		t.Fatal("钩子执行期间 IsLeader 已返回 true——写屏障失效，"+
			"并发 Append 会在计数器失效前拿到陈旧 offset")
	}
}
