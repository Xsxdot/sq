// cluster_test.go 三节点集成测试：真 TCP 全链路的验收门。
//
// 职责：
//   - testCluster harness：三节点（各自 store + Manager）经 127.0.0.1
//     随机端口的真实 TCP 互联，提供 leader 发现/摘除/收敛轮询原语
//   - 四个端到端场景：复制收敛（全组）、kill-leader 切换续写、
//     follower 提案拒绝、不干净节点 learner 重入
//
// 边界：
//   - 只覆盖三节点、AckQuorumMem 档；单节点/其它档位归 manager_test
//   - learner 重入编排属测试 harness（batch③ 的生产编排按本文件行为
//     规范实现）；本文件是 batch④ 场景测试的 seedbed
//   - 不直接 import internal/replication：它反向 import 本包，内测
//     文件引用即 "import cycle not allowed in test"（编译器拒绝）；
//     场景经 clusterReplicator（同语义薄包装）走同一条 Propose 路径
package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/xushixin/sq/internal/store"
)

// clusterReplicator 是场景测试用的薄复制器，与 replication.Cluster 同
// 语义：取批次物理字节 → 提进 Manager.Propose（组内阻塞到 apply）→
// 回收批次。不直接 import internal/replication 的原因见文件头边界。
//
// 与 replication.Cluster 的刻意差异：本 shim 在 b.Close() 失败时直接
// 报错，那边仅记 Warn 继续——测试从严，batch④ 场景套件不得把它当
// 作该处行为的精确规格。
type clusterReplicator struct{ m *Manager }

// Apply 提交批次：字节先拷贝（Repr 内存归批次所有）再 Close，
// 然后经 Propose 进入指定组。
func (r clusterReplicator) Apply(ctx context.Context, g uint32, b *store.Batch) error {
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil {
		return fmt.Errorf("回收批次: %w", err)
	}
	return r.m.Propose(ctx, g, repr)
}

// TestClusterReplicateAllGroups 三节点起全 4 组；经 Replicator 往 meta 组
// 与全部 3 个数据组各写一键，三个节点的 store 全部可读同值——复制内核
// 端到端（TCP 传输、Ready 契约、盲 apply、applied 原子）一次性验证。
func TestClusterReplicateAllGroups(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for g := uint32(0); g < 4; g++ {
		lead := tc.leaderOf(t, g) // 内部 WaitLeader 语义，超时 Fatal
		r := clusterReplicator{tc.mgrs[lead]}
		b := tc.stores[lead].NewBatch()
		key := fmt.Sprintf("msg/it/g%d", g)
		if err := b.Set([]byte(key), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := r.Apply(ctx, g, b); err != nil {
			t.Fatalf("组 %d apply: %v", g, err)
		}
	}
	tc.waitConverged(t, []string{"msg/it/g0", "msg/it/g1", "msg/it/g2", "msg/it/g3"}, 30*time.Second)
}

// TestClusterKillLeaderWriteContinues 摘除组 1 的 leader 后，幸存两节点
// 重选出新 leader，写入继续成功且在两个存活节点可读——切换语义内核部分。
func TestClusterKillLeaderWriteContinues(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	old := tc.leaderOf(t, 1)
	tc.kill(t, old)
	newLead := tc.leaderOfExcluding(t, 1, old) // 等到 leader ≠ old 且 ≠ 0
	b := tc.stores[newLead].NewBatch()
	_ = b.Set([]byte("msg/it/after-failover"), []byte("v"))
	if err := (clusterReplicator{tc.mgrs[newLead]}).Apply(ctx, 1, b); err != nil {
		t.Fatalf("切换后写入: %v", err)
	}
	tc.waitConvergedOn(t, tc.aliveIDs(), []string{"msg/it/after-failover"}, 30*time.Second)
}

// TestClusterProposeOnFollowerFails follower 上直接 Propose 必须报错——
// batch③ 把这个错误翻译成客户端可重试码，内核先保证它不静默转发不假成功。
func TestClusterProposeOnFollowerFails(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lead := tc.leaderOf(t, 1)
	var follower uint64
	for id := range tc.mgrs {
		if id != lead {
			follower = id
			break
		}
	}
	if err := tc.mgrs[follower].Propose(ctx, 1, []byte("x")); err == nil {
		t.Fatal("follower Propose 应报错，得到 nil")
	}
}

// TestClusterUncleanNodeRejoinsAsLearner 断电节点经 WipeForRejoin +
// harness 编排（存活 leader Remove→AddLearner→追平→AddNode）回归，
// 已 commit 数据一条不丢——spike Task 5 流程在真实存储/传输上的复现。
// 编排自动化是 batch③；本测试同时是那套编排逻辑的行为规范。
func TestClusterUncleanNodeRejoinsAsLearner(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	lead := tc.leaderOf(t, MetaGroup)
	writeN := func(from, n int) { // 经 meta 组写 n 键
		for i := from; i < from+n; i++ {
			b := tc.stores[tc.leaderOf(t, MetaGroup)].NewBatch()
			_ = b.Set([]byte(fmt.Sprintf("meta/topic/t%04d", i)), []byte("v"))
			if err := (clusterReplicator{tc.mgrs[tc.leaderOf(t, MetaGroup)]}).Apply(ctx, MetaGroup, b); err != nil {
				t.Fatalf("写 %d: %v", i, err)
			}
		}
	}
	writeN(0, 100)
	var victim uint64
	for id := range tc.mgrs {
		if id != lead {
			victim = id
			break
		}
	}
	tc.kill(t, victim)                 // 不写标记 = 断电
	writeN(100, 100)                   // 2/3 照常
	tc.rejoinAsLearner(t, ctx, victim) // harness：关旧 store→WipeForRejoin→全组 Remove/AddLearner→新 store+Manager fresh 启动→追平→AddNode
	tc.waitConverged(t, []string{"meta/topic/t0000", "meta/topic/t0099", "meta/topic/t0100", "meta/topic/t0199"}, 60*time.Second)
}

// testCluster 是三节点测试集群：每个节点一条独立数据目录、store 与
// Manager，经 127.0.0.1 随机端口的真实 TCP 互联。
//
// 本类型即 batch④ 场景测试的 seedbed——场景测试直接复用本 harness 的
// leader 发现/摘除/收敛轮询原语，新场景只需写断言部分。
type testCluster struct {
	dirs       map[uint64]string       // 节点 → 数据目录（WipeForRejoin 用）
	stores     map[uint64]*store.Store // 节点 → store（rejoin 后指向新 store）
	mgrs       map[uint64]*Manager     // 节点 → Manager（rejoin 后指向新 Manager）
	peers      map[uint64]string       // 节点 → raft 监听地址（全体，含本节点）
	killed     map[uint64]bool         // 已 kill（模拟断电）的节点
	dataGroups uint32
	mode       AckMode
}

// newTestCluster 起三节点集群（真实 TCP）。
//
// 装配序：先 net.Listen ×3 收集地址再拼 Peers 表——解拨号先有鸡还是
// 先有蛋（传输层按 Peers 拨号，必须拿到全部地址后才能建 Manager），
// 然后逐节点 store.Open(t.TempDir()/id) + NewManager（Listener 注入
// 已建监听）+ Start。
//
// 清理按逆序注册：StopClean 存活节点 → 等全部 Done → 关 store。
// kill 过的节点跳过 StopClean（运行 ctx 已取消）；rejoin 过的节点
// 已重置 killed 标记，按存活节点正常收尾（stores/mgrs 指向新实例）。
func newTestCluster(t *testing.T, mode AckMode) *testCluster {
	t.Helper()
	// 1. 先建三个监听器收集地址，再拼 Peers 表
	lstns := make([]net.Listener, 0, 3)
	peers := make(map[uint64]string, 3)
	for i := uint64(1); i <= 3; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("节点 %d 预建监听: %v", i, err)
		}
		lstns = append(lstns, ln)
		peers[i] = ln.Addr().String()
	}
	tc := &testCluster{
		dirs:       make(map[uint64]string, 3),
		stores:     make(map[uint64]*store.Store, 3),
		mgrs:       make(map[uint64]*Manager, 3),
		peers:      peers,
		killed:     make(map[uint64]bool, 3),
		dataGroups: 3,
		mode:       mode,
	}
	// 2. 逐节点开 store + NewManager + Start（节点目录 = t.TempDir()/id，
	//    WipeForRejoin 只需清节点目录，父目录保留）
	for i := uint64(1); i <= 3; i++ {
		dir := fmt.Sprintf("%s/%d", t.TempDir(), i)
		st, err := store.Open(dir, false, testSlog(t))
		if err != nil {
			t.Fatalf("节点 %d 开 store: %v", i, err)
		}
		m, err := NewManager(Options{
			NodeID:     i,
			Peers:      peers,
			Listener:   lstns[i-1], // 注入预建监听：Peers 地址与监听一一对应
			DataGroups: 3,
			Mode:       mode,
			Store:      st,
			Logger:     testSlog(t),
		})
		if err != nil {
			t.Fatalf("节点 %d NewManager: %v", i, err)
		}
		m.Start(context.Background())
		tc.dirs[i] = dir
		tc.stores[i] = st
		tc.mgrs[i] = m
		t.Logf("节点 %d 已启动: dir=%s addr=%s", i, dir, peers[i])
	}
	// 3. 逆序清理：先停 Manager（存活节点 StopClean）再关 store——
	//    pebble 不关闭直接 RemoveAll 会失败，同序见 rejoinAsLearner
	t.Cleanup(func() {
		for id, m := range tc.mgrs {
			if tc.killed[id] {
				continue // kill 过的节点 ctx 已取消，且可能已 rejoin（旧实例）
			}
			if err := m.StopClean(context.Background()); err != nil {
				t.Logf("清理: 节点 %d StopClean: %v", id, err)
			}
		}
		for id, m := range tc.mgrs {
			select {
			case <-m.Done():
			case <-time.After(10 * time.Second):
				t.Errorf("清理: 节点 %d 未在 10s 内完全退出", id)
			}
		}
		for id, st := range tc.stores {
			// 已关闭的 store（rejoin 流程显式 Close 过）再 Close 会 panic
			// （pebble 语义），清理期吞掉
			func() {
				defer func() { _ = recover() }()
				if err := st.Close(); err != nil {
					t.Logf("清理: 节点 %d store.Close: %v", id, err)
				}
			}()
		}
	})
	return tc
}

// leaderOf 返回组 g 当前 leader 的节点 ID，带 WaitLeader 语义：轮询
// 全部存活节点的 Leader(g) 直到选出，超时 Fatal。
//
// 注意：已 kill 节点被整体跳过——其 lead 原子停在摘除前，会报告陈旧
// leader；同理，leader 取值若是已 kill 节点也不作数。
func (tc *testCluster) leaderOf(t *testing.T, g uint32) uint64 {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for id, m := range tc.mgrs {
			if tc.killed[id] {
				continue
			}
			if lead, ok := m.Leader(g); ok && lead != 0 && !tc.killed[lead] {
				return lead
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("组 %d 在 60s 内未选出 leader", g)
	return 0
}

// leaderOfExcluding 同 leaderOf，但排除指定节点——kill-leader 场景等
// 新 leader 时用（旧 leader 已死，其报告不可信）。
func (tc *testCluster) leaderOfExcluding(t *testing.T, g uint32, exclude uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		for id, m := range tc.mgrs {
			if tc.killed[id] || id == exclude {
				continue
			}
			if lead, ok := m.Leader(g); ok && lead != 0 && lead != exclude && !tc.killed[lead] {
				t.Logf("组 %d 选出新 leader %d（排除 %d）", g, lead, exclude)
				return lead
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("组 %d 在 90s 内未选出新 leader（排除 %d）", g, exclude)
	return 0
}

// kill 模拟节点断电：测试后门 m.kill()（取消运行 ctx，不写干净关机
// 标记）+ 记入 killed 集，并等 Done 确认全部机件（含后台刷盘
// goroutine）完全退出——后续关 store/WipeForRejoin 才能安全操作。
func (tc *testCluster) kill(t *testing.T, id uint64) {
	t.Helper()
	tc.killed[id] = true
	tc.mgrs[id].kill()
	select {
	case <-tc.mgrs[id].Done():
	case <-time.After(10 * time.Second):
		t.Fatalf("节点 %d kill 后未在 10s 内完全退出", id)
	}
	t.Logf("节点 %d 已 kill（模拟断电，未写干净关机标记）", id)
}

// aliveIDs 返回未被 kill 的节点 ID 列表（rejoin 完成的节点视为存活）。
func (tc *testCluster) aliveIDs() []uint64 {
	ids := make([]uint64, 0, len(tc.mgrs))
	for id := range tc.mgrs {
		if !tc.killed[id] {
			ids = append(ids, id)
		}
	}
	return ids
}

// waitConverged 轮询所有存活节点的 store：全部 keys 都存在、非空且
// 值一致（盲 apply 收敛的观测面），超时 Fatal 附各节点缺失键。
func (tc *testCluster) waitConverged(t *testing.T, keys []string, timeout time.Duration) {
	t.Helper()
	tc.waitConvergedOn(t, tc.aliveIDs(), keys, timeout)
}

// waitConvergedOn 同 waitConverged，但只对给定节点集合轮询。
func (tc *testCluster) waitConvergedOn(t *testing.T, ids []uint64, keys []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tc.converged(ids, keys) {
			t.Logf("收敛完成: 节点 %v 上 %d 键全部一致", ids, len(keys))
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	// 超时：逐节点列出缺失键，附排障上下文
	var sb strings.Builder
	fmt.Fprintf(&sb, "节点 %v 上 %d 键未在 %v 内收敛:\n", ids, len(keys), timeout)
	for _, id := range ids {
		var missing []string
		for _, k := range keys {
			_, ok, err := tc.stores[id].Get([]byte(k))
			switch {
			case err != nil:
				missing = append(missing, k+"<err>")
			case !ok:
				missing = append(missing, k)
			}
		}
		fmt.Fprintf(&sb, "  节点 %d: 缺失 %v\n", id, missing)
	}
	t.Fatal(sb.String())
}

// converged 判定给定节点集合上全部 keys 已收敛：每个键在每个节点上
// 都存在、非空且值一致。
func (tc *testCluster) converged(ids []uint64, keys []string) bool {
	for _, k := range keys {
		var want []byte
		for _, id := range ids {
			v, ok, err := tc.stores[id].Get([]byte(k))
			if err != nil || !ok || len(v) == 0 {
				return false
			}
			if want == nil {
				want = v
			} else if !bytes.Equal(want, v) {
				return false
			}
		}
	}
	return true
}

// rejoinAsLearner 把断电节点（已 kill、store 已停）以 learner 身份
// 重新接回集群——spike restartAsLearner 流程（Task 5）在真实存储/
// 传输上的复现，逐步骤 t.Logf 供观测（batch④ 场景测试依赖的观测面）。
//
// 步骤：
//  1. 关旧 store（WipeForRejoin 前置：pebble 持有目录句柄）
//  2. WipeForRejoin 清空状态目录
//  3. 各存活 leader（meta + 全部数据组）Remove 旧 voter → AddLearner
//     ——learner 身份与日志由 leader 的 ConfChange 日志赋予
//  4. 新 store + NewManager fresh 路径启动（断言无 ErrUncleanShutdown）
//  5. 等追平：learner 各组成员位点达到 leader（无新提案时位点即锚点）
//  6. 各 leader AddNode 升级 voter，等到 leader 报告该节点为 voter
//
// 注意：本流程是 batch③ 生产编排的行为规范，编排语义不得私自偏离。
func (tc *testCluster) rejoinAsLearner(t *testing.T, ctx context.Context, victim uint64) {
	t.Helper()
	dir := tc.dirs[victim]
	t.Logf("=== rejoinAsLearner: 节点 %d 断电重入（dir=%s）===", victim, dir)

	// 1. 关旧 store：WipeForRejoin 前置（pebble 文件句柄不关清不掉）
	if err := tc.stores[victim].Close(); err != nil {
		t.Fatalf("节点 %d 关闭旧 store: %v", victim, err)
	}
	t.Logf("节点 %d 旧 store 已关闭", victim)

	// 2. 清空状态目录（含旧 raft 日志——身份由 leader ConfChange 重赋）
	if err := WipeForRejoin(dir); err != nil {
		t.Fatalf("WipeForRejoin(%s): %v", dir, err)
	}
	t.Logf("节点 %d 状态目录已清空", victim)

	// 3. 各存活 leader 先 Remove 旧 voter 再 AddLearner（meta + 全数据组）。
	//    先 Remove 是 raft 前置：同 id 已在成员表（voter）时再 Add 会报错。
	for g := uint32(0); g <= tc.dataGroups; g++ {
		lead := tc.leaderOf(t, g) // 内部跳过已 kill 节点（含 victim）
		ccCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := tc.mgrs[lead].ProposeConfChange(ccCtx, g, raftpb.ConfChangeRemoveNode, victim); err != nil {
			cancel()
			t.Fatalf("组 %d 移除节点 %d: %v", g, victim, err)
		}
		if err := tc.mgrs[lead].ProposeConfChange(ccCtx, g, raftpb.ConfChangeAddLearnerNode, victim); err != nil {
			cancel()
			t.Fatalf("组 %d 添加 learner %d: %v", g, victim, err)
		}
		cancel()
		t.Logf("组 %d（leader %d）: 已 Remove→AddLearner 节点 %d", g, lead, victim)
	}

	// 4. 抢占 victim 的监听地址作为新 Manager 的监听器：kill 后 Done
	//    不保证旧传输层看门狗已关监听器（它不在传输层 wg 内），直接重绑
	//    同一地址可能撞 EADDRINUSE；先 net.Listen 抢到端口（拿到即旧
	//    监听器已关的确定性证明），再注入给 NewManager——中间无空窗。
	var ln net.Listener
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		l, err := net.Listen("tcp", tc.peers[victim])
		if err == nil {
			ln = l
			break
		}
		if errors.Is(err, syscall.EADDRINUSE) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		t.Fatalf("节点 %d 重绑监听 %s: %v", victim, tc.peers[victim], err)
	}
	if ln == nil {
		t.Fatalf("节点 %d 旧监听器未在 10s 内关闭，无法重绑 %s", victim, tc.peers[victim])
	}
	st, err := store.Open(dir, false, testSlog(t))
	if err != nil {
		t.Fatalf("节点 %d 重开 store: %v", victim, err)
	}
	m, err := NewManager(Options{
		NodeID:     victim,
		Peers:      tc.peers,
		Listener:   ln,
		DataGroups: tc.dataGroups,
		Mode:       tc.mode,
		Store:      st,
		Logger:     testSlog(t),
	})
	if errors.Is(err, ErrUncleanShutdown) {
		t.Fatalf("节点 %d WipeForRejoin 后仍报 ErrUncleanShutdown——fresh 路径判定错误", victim)
	}
	if err != nil {
		t.Fatalf("节点 %d NewManager(fresh): %v", victim, err)
	}
	m.Start(context.Background())
	tc.stores[victim] = st
	tc.mgrs[victim] = m
	t.Logf("节点 %d 已以空存储 fresh 路径启动（身份由 leader 的 ConfChange 日志赋予）", victim)

	// 5. 等追平：learner 各组成员位点达到 leader（leader 无新提案时
	//    位点即对账锚点，spike waitCaughtUp 的 Manager 版）
	for g := uint32(0); g <= tc.dataGroups; g++ {
		lead := tc.leaderOf(t, g)
		tc.waitCaughtUp(t, g, tc.mgrs[lead], m, 60*time.Second)
		t.Logf("组 %d: learner 已追平 leader %d", g, lead)
	}

	// 6. 各 leader AddNode 升级 voter，等到 leader 报告该节点为 voter
	for g := uint32(0); g <= tc.dataGroups; g++ {
		lead := tc.leaderOf(t, g)
		ccCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := tc.mgrs[lead].ProposeConfChange(ccCtx, g, raftpb.ConfChangeAddNode, victim); err != nil {
			cancel()
			t.Fatalf("组 %d 升级节点 %d 为 voter: %v", g, victim, err)
		}
		cancel()
		tc.waitVoter(t, g, victim, 30*time.Second)
	}
	tc.killed[victim] = false // 恢复存活：清理期按正常节点 StopClean
	t.Logf("=== rejoinAsLearner 完成: 节点 %d 恢复 voter，三节点成员表复原 ===", victim)
}

// waitCaughtUp 轮询等待 learner 在组 g 的成员位点追平 leader。
// 两组数同一条日志，leader 无新提案时位点即收敛锚点；超时 Fatal。
func (tc *testCluster) waitCaughtUp(t *testing.T, g uint32, leader, learner *Manager, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lApp := leader.groups[g].appliedIndex()
		vApp := learner.groups[g].appliedIndex()
		if vApp >= lApp {
			t.Logf("组 %d: learner applied=%d 达到 leader applied=%d", g, vApp, lApp)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("组 %d: learner 未在 %v 内追平 leader（learner=%d leader=%d）", g, timeout,
		learner.groups[g].appliedIndex(), leader.groups[g].appliedIndex())
}

// waitVoter 轮询等待 leader 的 Progress 报告 nodeID 已是 voter
// （IsLearner=false）——AddNode 生效的观测面。Progress 只在 leader
// 侧填充，故从 leader 节点的 Status 取。
func (tc *testCluster) waitVoter(t *testing.T, g uint32, nodeID uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, m := range tc.mgrs {
			if tc.killed[id] || !m.IsLeader(g) {
				continue
			}
			prs, ok := m.groups[g].rn.Status().Progress[nodeID]
			if ok && !prs.IsLearner {
				t.Logf("组 %d: leader %d 报告节点 %d 已是 voter", g, id, nodeID)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("组 %d: 节点 %d 未在 %v 内恢复 voter 身份", g, nodeID, timeout)
}
