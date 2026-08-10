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
	"errors"
	"fmt"
	"strings"
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
// 参数用 testing.TB：测试与基准共用（BenchmarkProposeQuorumFsync 复用）。
func startSingleNodeGroup(t testing.TB, g uint32, rs *raftStore, st *store.Store, mode AckMode) *group {
	t.Helper()
	storage := raft.NewMemoryStorage()
	gr := newGroup(g, 1, storage, nil, rs, st, func(uint32, []*raftpb.Message) {}, mode, nil, nil, testSlog(t))
	// raft 节点在 newGroup 之后装配（Config.Storage 用包了快照生成器
	// 的 gr.stg，见 newGroup 注释），回填 gr.rn 后启动
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	// raft.StartNode 起了 raft 节点 goroutine（node.run），组退出不会
	// 自动停它——-race 下是稳定的 goroutine 泄漏，必须显式 Stop
	// （Stop 幂等、阻塞到该 goroutine 退出，见 node.Stop 注释）。
	// 注册序（LIFO 执行）：cancel → 等 run 退出 → rn.Stop。
	t.Cleanup(rn.Stop)
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
func startLoneGroupOfThree(t testing.TB, g uint32, rs *raftStore, st *store.Store) *group {
	t.Helper()
	storage := raft.NewMemoryStorage()
	gr := newGroup(g, 1, storage, nil, rs, st, func(uint32, []*raftpb.Message) {}, AckQuorumFsync, nil, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	gr.rn = rn
	// 同 startSingleNodeGroup：raft 节点 goroutine 必须显式 Stop
	t.Cleanup(rn.Stop)
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
	// 同 startSingleNodeGroup：raft 节点 goroutine 必须显式 Stop。
	// run 由调用方启动；本 helper 先注册（LIFO 后执行），停节点一定
	// 发生在调用方注册的「cancel → 等 run 退出」之后。
	t.Cleanup(rn.Stop)
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
	defer rn.Stop() // StartNode 起了 raft 节点 goroutine，本测试不跑 run，必须显式停
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
	defer rn.Stop() // StartNode 起了 raft 节点 goroutine，必须显式停
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
		t.Fatal("钩子执行期间 IsLeader 已返回 true——写屏障失效，" +
			"并发 Append 会在计数器失效前拿到陈旧 offset")
	}
}

// TestInstallSnapshotFailurePanicsAndKeepsMarker（I1 修复）快照安装失败
// 必须 fail-stop panic，且安装中标记仍留在盘上：
//
//   - 为什么 panic 而不是「Advance 放弃本轮等 raft 重发」：Advance 把
//     MsgStorageAppendResp（携带快照）步进给 raft 内核，内核的
//     appliedSnap（stableSnapTo + appliedTo）把快照标记为已持久化已
//     应用，raft 不会重发 MsgSnap——静默续跑 = 内存（MemoryStorage 未
//     更新）/磁盘（半截数据）/raft（自以为已到快照位点）三方分叉，
//     永久卡死。panic 让进程死亡、由上层重启接管（与 Persist/applyEntry
//     同策略）；重启时 buildGroup 的标记检查据此清空重来。
//   - 为什么标记必然在盘上：installSnapshot 第 2 步 MarkInstalling
//     （Sync）先于任何数据写入，本测试在拉块（第 4 步）注入必败回调，
//     失败发生在删标记的收口批次（第 5 步）之前——标记必须仍在。
//   - 为什么直接调 handleReady 而非走 run 循环：panic 发生在 run
//     goroutine 内时测试进程无法 recover（goroutine 的 panic 不可跨
//     协程捕获）；handleReady 是 panic 的抛出处，直接在测试协程调用
//     即可捕获断言，生产行为不变（run 循环原样抛出）。
//
// 断言：panic 确实发生（且是安装第 4 步的拉块错误）、标记在位点
// 100 仍在盘上、内存侧未推进（mem 无快照、applied 仍为 0）——三方
// 分叉里「raft 已标记快照应用」的一面因 panic 而未发生。
func TestInstallSnapshotFailurePanicsAndKeepsMarker(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	storage := raft.NewMemoryStorage()
	gr := newGroup(0, 1, storage, nil, rs, st, func(uint32, []*raftpb.Message) {}, AckQuorumFsync, nil, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	// StartNode 起了 raft 节点 goroutine：keepTicking 的 Tick 需要活节点，
	// 本测试不跑 run，必须显式停（Stop 幂等、阻塞到 goroutine 退出）。
	defer rn.Stop()
	// 注入拉块必败的 control 回调（模拟对端不可达）：安装第 4 步第一块
	// 即失败，走 fail-stop panic 路径。
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		return nil, errors.New("注入的拉块失败（模拟对端不可达）")
	}

	idx, tm := uint64(100), uint64(2)
	snap := &raftpb.Snapshot{
		Data: encodeSnapDescriptor(snapDescriptor{ID: 7, Leader: 1, Index: idx}),
		Metadata: &raftpb.SnapshotMetadata{
			Index: &idx, Term: &tm, ConfState: &raftpb.ConfState{},
		},
	}
	rd := raft.Ready{Snapshot: snap}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		gr.handleReady(context.Background(), rd)
	}()
	if recovered == nil {
		t.Fatal("快照安装失败必须 panic（fail-stop）——raft 已把快照视为持久化，静默续跑是内存/磁盘/raft 三方分叉")
	}
	if msg := fmt.Sprint(recovered); !strings.Contains(msg, "快照安装第 4 步 拉块失败") {
		t.Fatalf("panic 载荷应是安装第 4 步的拉块错误，得到: %v", recovered)
	}
	// 标记必须在盘上（第 2 步先于任何数据写入，失败必在删标记之前）——
	// 重启的 buildGroup 检查据此清空重来
	meta, ok, err := rs.LoadInstalling(0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("安装失败后安装中标记必须仍在盘上（重启清空重来的依据）")
	}
	if meta.GetIndex() != idx {
		t.Fatalf("标记位点 %d != 快照位点 %d", meta.GetIndex(), idx)
	}
	// 内存侧不得推进：mem 未被 ApplySnapshot、applied 仍为 0——「raft
	// 自以为到快照位点」的分叉面必须未发生（panic 在 Advance 之前）
	if f, err := gr.mem.FirstIndex(); err != nil || f != 1 {
		t.Fatalf("安装失败后 mem.FirstIndex 应为 1（mem 未应用快照）: %d err=%v", f, err)
	}
	if gr.applied.Load() != 0 {
		t.Fatalf("安装失败后 applied 应为 0，得到 %d", gr.applied.Load())
	}
}

// BenchmarkProposeQuorumFsync 度量 quorum-fsync 档下单节点组的串行提案
// 吞吐（batch④ Task 9 MustSync 改动的基准证据）。
//
// 为什么要用这个形态：每个提案在单节点 leader 上产生两个 Ready 轮次——
// 条目轮（新条目，raft 要求同步落盘）与随后的 commit-only HardState 轮
// （Advance 内自 ack 触发 maybeCommit，提交位点推进但 term/vote/条目都
// 没变）。旧判定「有条目或有 HardState 就刷」在 commit 轮白刷一次盘；
// raft 的 MustSync 判定（raft.MustSync：ents!=0 || term/vote 变化）在
// 该轮为 false，可省掉这次 fsync。改前/改后数字对比记入 commit message。
func BenchmarkProposeQuorumFsync(b *testing.B) {
	st := openClusterTestStore(b)
	rs := newRaftStore(st, testSlog(b))
	gr := startSingleNodeGroup(b, 0, rs, st, AckQuorumFsync)
	deadline := time.Now().Add(5 * time.Second)
	for !gr.isLeader() {
		if time.Now().After(deadline) {
			b.Fatal("单节点组未当选")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 提案载荷复用同一份批次字节：真实路径每条消息一个批次，基准只关心
	// 持久化/提案往返成本，同键覆写让 FSM 应用保持最廉价形态
	batch := st.NewBatch()
	if err := batch.Set([]byte("bench/q/0/m"), []byte("payload")); err != nil {
		b.Fatal(err)
	}
	repr := append([]byte(nil), batch.Repr()...)
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := gr.propose(context.Background(), repr); err != nil {
			b.Fatal(err)
		}
	}
}

// newPullTestGroup 造一个只用于拉块路径的组：不起 raft 节点（
// pullSnapshotChunks 不碰 rn），control 由调用方注入。
func newPullTestGroup(t *testing.T) *group {
	t.Helper()
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	return newGroup(0, 1, raft.NewMemoryStorage(), nil, rs, st,
		func(uint32, []*raftpb.Message) {}, AckQuorumMem, nil, nil, testSlog(t))
}

// snapFetchResp 拼一个 OpFetchSnapshot 响应：[1B done][4B BE 游标长]
// [游标][块字节]（与 decodeSnapFetchResp 配对）。
func snapFetchResp(done bool, next []byte, pairs []kv) []byte {
	resp := make([]byte, 5)
	if done {
		resp[0] = 1
	}
	binary.BigEndian.PutUint32(resp[1:5], uint32(len(next)))
	resp = append(resp, next...)
	return append(resp, encodeChunk(pairs)...)
}

// TestPullSnapshotRejectsNonAdvancingCursor 拉块护栏之一（I4）：游标必须
// 严格前进。发送侧枚举单调性一旦被破坏（键族表写错、对端版本不一致、
// 恶意对端），症状是同一批键反复重发、循环永不 done——batch④ 首轮评审
// 的 C1 正是以「死循环」而非「报错」的面目出现。校验把静默死循环变成
// 一条明确的协议错误。
func TestPullSnapshotRejectsNonAdvancingCursor(t *testing.T) {
	gr := newPullTestGroup(t)
	calls := 0
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		calls++
		// 两块都停在同一个游标上：第二块即触发校验
		return snapFetchResp(false, []byte("msg/aaa"), nil), nil
	}
	err := gr.pullSnapshotChunks(context.Background(), snapDescriptor{ID: 1, Leader: 1, Index: 10})
	if err == nil {
		t.Fatal("游标原地不动必须报错，否则是静默死循环")
	}
	if !strings.Contains(err.Error(), "游标未前进") {
		t.Fatalf("错误应指向游标未前进，得到: %v", err)
	}
	if calls != 2 {
		t.Fatalf("应在第 2 块即中止，实际拉了 %d 块", calls)
	}
}

// TestPullSnapshotChunkCapAborts 拉块护栏之二（I4）：块数上限兜住
// 「每块都推进一点点但永远拉不完」。上限临时调小以覆盖守卫本身
// （生产值 65536，按 4MiB 分块相当于 256GiB 单组状态）。
func TestPullSnapshotChunkCapAborts(t *testing.T) {
	old := maxSnapshotChunks
	maxSnapshotChunks = 3
	defer func() { maxSnapshotChunks = old }()

	gr := newPullTestGroup(t)
	calls := 0
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		calls++
		// 游标严格前进（不触发单调性校验），但永不 done
		return snapFetchResp(false, []byte(fmt.Sprintf("msg/%08d", calls)), nil), nil
	}
	err := gr.pullSnapshotChunks(context.Background(), snapDescriptor{ID: 1, Leader: 1, Index: 10})
	if err == nil {
		t.Fatal("永不 done 的对端必须被块数上限中止")
	}
	if !strings.Contains(err.Error(), "块数超上限") {
		t.Fatalf("错误应指向块数上限，得到: %v", err)
	}
	if calls != 3 {
		t.Fatalf("上限 3 块：应拉满 3 块后中止，实际 %d", calls)
	}
}

// TestPullSnapshotByteCapAborts 拉块护栏之三（I4）：累计字节上限兜住
// 「块数不多但每块巨大」——块大小由发送侧决定，接收侧不能假设它守规矩，
// 否则坏对端可以无界地往本节点磁盘写。上限临时调小以覆盖守卫本身。
func TestPullSnapshotByteCapAborts(t *testing.T) {
	old := maxSnapshotBytes
	maxSnapshotBytes = 64
	defer func() { maxSnapshotBytes = old }()

	gr := newPullTestGroup(t)
	calls := 0
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		calls++
		k := []byte(fmt.Sprintf("msg/%08d", calls))
		// 每块一对键值：8B 双长度头 + 键 + 值，几块即越过 64B 上限
		return snapFetchResp(false, k, []kv{{k: k, v: make([]byte, 16)}}), nil
	}
	err := gr.pullSnapshotChunks(context.Background(), snapDescriptor{ID: 1, Leader: 1, Index: 10})
	if err == nil {
		t.Fatal("超字节上限必须中止")
	}
	if !strings.Contains(err.Error(), "累计字节") {
		t.Fatalf("错误应指向累计字节上限，得到: %v", err)
	}
}

// TestPullSnapshotSucceedsOnDone 反向自证：护栏不误伤正常传输——游标
// 严格前进、对端报 done，拉取正常收口且键已落盘。
func TestPullSnapshotSucceedsOnDone(t *testing.T) {
	gr := newPullTestGroup(t)
	calls := 0
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		calls++
		k := []byte(fmt.Sprintf("msg/%08d", calls))
		return snapFetchResp(calls == 3, k, []kv{{k: k, v: []byte("v")}}), nil
	}
	if err := gr.pullSnapshotChunks(context.Background(), snapDescriptor{ID: 1, Leader: 1, Index: 10}); err != nil {
		t.Fatalf("正常传输不得报错: %v", err)
	}
	if calls != 3 {
		t.Fatalf("应在对端报 done 的第 3 块收口，实际 %d", calls)
	}
	for i := 1; i <= 3; i++ {
		if _, ok, err := gr.st.Get([]byte(fmt.Sprintf("msg/%08d", i))); err != nil || !ok {
			t.Fatalf("第 %d 块的键未落盘 ok=%v err=%v", i, ok, err)
		}
	}
}

// TestStepDropsWhenInstallingAndInboxFull 安装期入站不阻塞（I5）：
// Ready 循环在快照安装期间整段不消费 inbox（安装是分钟级的同步操作），
// 而 leader 的心跳按 tick 持续投递——inbox 一旦填满，阻塞式 step 会把
// 传输层的整条接收协程钉死，该连接上**所有组**的消息一起停摆，安装期
// 从「本组慢」升级成「整节点失联」。安装期改为满则丢：丢的心跳由 raft
// 按 tick 重发兜底，代价远小于连接级停摆。
func TestStepDropsWhenInstallingAndInboxFull(t *testing.T) {
	gr := newPullTestGroup(t)
	gr.installing.Store(true)
	// 灌满 inbox（无 Ready 循环消费）
	filled := 0
	for {
		m := &raftpb.Message{}
		select {
		case gr.inbox <- m:
			filled++
		default:
		}
		if len(gr.inbox) == cap(gr.inbox) {
			break
		}
	}
	// 满了之后 step 必须立刻返回并计数，而不是阻塞
	done := make(chan struct{})
	go func() {
		defer close(done)
		gr.step(&raftpb.Message{})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("安装期 inbox 满时 step 阻塞了——传输层接收协程会被钉死，整条连接的全部组一起停摆")
	}
	if n := gr.installDrops.Load(); n != 1 {
		t.Fatalf("丢弃计数应为 1，得到 %d", n)
	}
	// 非安装期不丢：清位后 step 恢复阻塞语义（此处腾一个位子验证入队成功）
	gr.installing.Store(false)
	<-gr.inbox
	gr.step(&raftpb.Message{})
	if len(gr.inbox) != cap(gr.inbox) {
		t.Fatal("非安装期 step 应入队而非丢弃")
	}
	if n := gr.installDrops.Load(); n != 1 {
		t.Fatalf("非安装期不得再计丢弃，得到 %d", n)
	}
	_ = filled
}

// TestInstallSnapshotRetriesTransientFailure 安装重试（N1 的一半）：
// 第 4 步是分钟级网络操作，一次瞬时错误（对端重启、连接 reset、控制帧
// 超时）不该直接升级成 fail-stop panic + 整组重装。安装流程幂等（标记
// 可重写、wipe 可重跑、块可重拉），重试的代价只是重来一遍。
//
// 注入：前两次尝试的第一块即失败，第三次正常返回 done——安装应成功，
// 且安装中标记已被收口批次删除。
func TestInstallSnapshotRetriesTransientFailure(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	gr := newGroup(0, 1, raft.NewMemoryStorage(), nil, rs, st,
		func(uint32, []*raftpb.Message) {}, AckQuorumMem, nil, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	defer rn.Stop()

	attempts := 0
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		attempts++
		if attempts <= 2 {
			return nil, errors.New("注入的瞬时拉块失败")
		}
		return snapFetchResp(true, nil, []kv{{k: []byte("meta/T"), v: []byte("v")}}), nil
	}

	idx, tm := uint64(100), uint64(2)
	snap := &raftpb.Snapshot{
		Data: encodeSnapDescriptor(snapDescriptor{ID: 7, Leader: 1, Index: idx}),
		Metadata: &raftpb.SnapshotMetadata{
			Index: &idx, Term: &tm, ConfState: &raftpb.ConfState{},
		},
	}
	if err := gr.installSnapshotWithRetry(context.Background(), snap); err != nil {
		t.Fatalf("第 3 次尝试应成功: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("应重试到第 3 次，实际尝试 %d 次", attempts)
	}
	// 成功即收口：安装中标记必须已删（否则重启会白清一次库）
	if _, ok, err := rs.LoadInstalling(0); err != nil || ok {
		t.Fatalf("安装成功后标记必须已删 ok=%v err=%v", ok, err)
	}
	if gr.applied.Load() != idx {
		t.Fatalf("安装成功后 applied 应为 %d，得到 %d", idx, gr.applied.Load())
	}
	// installing 必然已清位、丢弃计数已归零（defer 保证）
	if gr.installing.Load() {
		t.Fatal("安装返回后 installing 必须清位")
	}
}

// TestInstallSnapshotAbortsOnShutdown 停机路径不 panic：ctx 已取消时
// 安装失败是正常停机，不是故障——安装中标记已在盘上，重启清空重来。
// 此时 panic 只会把一次正常停机变成一次崩溃退出。
func TestInstallSnapshotAbortsOnShutdown(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	gr := newGroup(0, 1, raft.NewMemoryStorage(), nil, rs, st,
		func(uint32, []*raftpb.Message) {}, AckQuorumMem, nil, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	defer rn.Stop()

	attempts := 0
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		attempts++
		return nil, errors.New("注入的拉块失败（停机中）")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 停机：第一次失败后即不再重试

	idx, tm := uint64(100), uint64(2)
	snap := &raftpb.Snapshot{
		Data: encodeSnapDescriptor(snapDescriptor{ID: 7, Leader: 1, Index: idx}),
		Metadata: &raftpb.SnapshotMetadata{
			Index: &idx, Term: &tm, ConfState: &raftpb.ConfState{},
		},
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		gr.handleReady(ctx, raft.Ready{Snapshot: snap})
	}()
	if recovered != nil {
		t.Fatalf("停机途中的安装失败不得 panic，得到: %v", recovered)
	}
	if attempts != 1 {
		t.Fatalf("ctx 已取消应立刻停止重试，实际尝试 %d 次", attempts)
	}
	// 标记仍在盘上：重启由 buildGroup 清空重来
	if _, ok, err := rs.LoadInstalling(0); err != nil || !ok {
		t.Fatalf("停机中止后安装中标记必须仍在盘上 ok=%v err=%v", ok, err)
	}
}
