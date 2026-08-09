// cluster_test.go 三节点集成测试：真 TCP 全链路的验收门。
//
// 职责：
//   - testCluster harness：三节点（各自 store + Manager）经 127.0.0.1
//     随机端口的真实 TCP 互联，提供 leader 发现/摘除/收敛轮询原语
//   - 六个端到端场景：复制收敛（全组）、kill-leader 切换续写、
//     follower 提案拒绝、不干净节点 learner 重入、转发原语
//     （Task 4）线路与收敛、控制通道 RPC
//
// 边界：
//   - 只覆盖三节点、AckQuorumMem 档；单节点/其它档位归 manager_test
//   - learner 重入编排属测试 harness（batch③ 的生产编排按本文件行为
//     规范实现）；本文件是 batch④ 场景测试的 seedbed
//   - 不直接 import internal/replication：它反向 import 本包，内测
//     文件引用即 "import cycle not allowed in test"（编译器拒绝）；
//     场景经 clusterReplicator（同语义薄包装）走同一条 Propose 路径；
//     replication 包自身的转发原语（NewCluster.ForwardAppend 等）由
//     replication_test.go 用 solo Manager 直测
package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/xushixin/sq/internal/core"
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

// shimPending 是 clusterReplicator.ApplyAsync 的返回类型：与
// replication.Pending 同语义（恰好一次 Wait）。
type shimPending chan error

// Wait 读一次结果 channel（恰好一次语义，与 replication.chanPending 同）。
func (p shimPending) Wait() error { return <-p }

// ApplyAsync 与 replication.Cluster.ApplyAsync 同语义（shim 契约见
// clusterReplicator 注释）：拷贝 repr、Close 批次、goroutine 内 Propose，
// Wait 阻塞到本节点 apply 完成。
func (r clusterReplicator) ApplyAsync(ctx context.Context, g uint32, b *store.Batch) (shimPending, error) {
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil {
		return nil, fmt.Errorf("回收批次: %w", err)
	}
	ch := make(chan error, 1)
	go func() { ch <- r.m.Propose(ctx, g, repr) }()
	return shimPending(ch), nil
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

// TestProposeOnFollowerReturnsErrNotLeader follower 上 Propose 必须报
// ErrNotLeader（可被 errors.Is 识别）——协议面据此翻译可重试码。
func TestProposeOnFollowerReturnsErrNotLeader(t *testing.T) {
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
	err := tc.mgrs[follower].Propose(ctx, 1, []byte("x"))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower Propose 应返回 ErrNotLeader，得到: %v", err)
	}
}

// TestOnAppliedHookFires 每个节点 apply 提案后钩子必须携带组号与原始
// repr 触发：leader 写一条 meta 组提案，断言 3 个节点都收到
// (g=0, repr 相同)。
func TestOnAppliedHookFires(t *testing.T) {
	// 注入 OnApplied 钩子启用收集器：harness 把钩子逐节点包裹进
	// appliedChs（见 newTestClusterOpts 注释），raw 钩子为占位，断言走
	// 收集器 channel
	tc := newTestClusterOpts(t, AckQuorumMem, Options{OnApplied: func(g uint32, repr []byte) {}})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lead := tc.leaderOf(t, MetaGroup)
	b := tc.stores[lead].NewBatch()
	key := "meta/topic/hook-fires"
	if err := b.Set([]byte(key), []byte("v")); err != nil {
		t.Fatal(err)
	}
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tc.mgrs[lead].Propose(ctx, MetaGroup, repr); err != nil {
		t.Fatalf("meta 组提案: %v", err)
	}
	for id := range tc.mgrs {
		tc.waitAppliedEvt(t, id, MetaGroup, repr, 30*time.Second)
	}
}

// TestOnLeaderChangeHookFiresOnFailover kill leader 后，存活节点必须收到
// isSelf=true 的 OnLeaderChange 回调：新 leader 在自己当选的 SoftState
// 分支触发（leader 字段为新 leader 自身）。
func TestOnLeaderChangeHookFiresOnFailover(t *testing.T) {
	// 同 OnApplied 收集器用法：注入钩子启用 leaderChs，断言走 channel
	tc := newTestClusterOpts(t, AckQuorumMem, Options{OnLeaderChange: func(g uint32, leader uint64, isSelf bool) {}})
	old := tc.leaderOf(t, 1)
	tc.kill(t, old)
	evt := tc.waitLeaderChangeIsSelf(t, 1, old, 60*time.Second)
	if got := tc.leaderOfExcluding(t, 1, old); got != evt.leader {
		t.Fatalf("回调报告的 leader=%d 与 leaderOfExcluding 报告的 %d 不一致", evt.leader, got)
	}
}

// TestLeaderSpreadConverges 三节点四组，任由初始选举集中，摊布循环应在
// 数个周期内把 leader 分布收敛到 preferred（组 g → sortedPeers[g % n]）。
func TestLeaderSpreadConverges(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem) // harness 注入 balancer 间隔 200ms
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if tc.leadersMatchPreferred(t) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("leader 摊布未收敛到 preferred 分布")
}

// TestLeaderChangeFiresExactlyOncePerGroup 钩子时序断言：摊布收敛后每个
// 组的现任 leader 恰好收到一次 isSelf=true（获得 leadership 时触发一次，
// 不多不漏），其余节点至多一次（初始选举赢家若被摊布转走也触发过）。
// 这是 Task 11 InvalidateCounters 接线（isSelf==true 时失效计数器）的
// 时机前提：多次触发会让计数器被无谓失效。
func TestLeaderChangeFiresExactlyOncePerGroup(t *testing.T) {
	// 注入占位钩子启用收集器（newTestClusterOpts 见 harness 注释）
	tc := newTestClusterOpts(t, AckQuorumMem, Options{OnLeaderChange: func(g uint32, leader uint64, isSelf bool) {}})
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if tc.leadersMatchPreferred(t) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	// 收敛后等最后的钩子事件入队：lead 原子先于钩子更新（handleReady
	// 先 Store 再 notify），最后一场转移的当选事件可能晚于收敛观测
	time.Sleep(time.Second)
	drain := func(ch chan leaderEvt) map[uint32]int {
		m := map[uint32]int{}
		for {
			select {
			case evt := <-ch:
				if evt.isSelf {
					m[evt.g]++
				}
			default:
				return m
			}
		}
	}
	counts := make(map[uint64]map[uint32]int, len(tc.mgrs)) // 节点 → 组 → isSelf=true 计数
	for id, ch := range tc.leaderChs {
		counts[id] = drain(ch)
	}
	for g := uint32(0); g <= tc.dataGroups; g++ {
		pref := tc.preferredOf(g)
		for id := range tc.mgrs {
			got := counts[id][g]
			if id == pref {
				if got != 1 {
					t.Fatalf("组 %d 的 preferred 节点 %d 收到 %d 次 isSelf=true; want 恰好 1", g, id, got)
				}
			} else if got > 1 {
				t.Fatalf("组 %d 节点 %d 收到 %d 次 isSelf=true; want ≤1", g, id, got)
			}
		}
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

// TestClusterAutoRejoin 断电节点调用 cluster.Rejoin 一个入口完成全部编排：
// 存活 leader 收 PrepareJoin 自动 Remove→AddLearner；节点 fresh 启动追平后
// 由 AutoPromoteLearners 循环自动升 voter；最终三节点数据收敛且 victim 是
// 全组 voter。原手工编排测试保留（行为规范不动）。
func TestClusterAutoRejoin(t *testing.T) {
	// AutoPromoteLearners 注入到全节点：升 voter 是 leader 侧循环的活
	// （learner 不 lead 组），victim 自己的 Manager 也带上（无妨）
	tc := newTestClusterOpts(t, AckQuorumMem, Options{AutoPromoteLearners: true})
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
	tc.kill(t, victim) // 不写标记 = 断电
	writeN(100, 100)   // 2/3 照常

	// Rejoin 前置（六步之 1）：调用方负责关旧 store——pebble 持有目录
	// 句柄，不关闭 WipeForRejoin 清不掉（Rejoin 文档注释的调用方职责）
	if err := tc.stores[victim].Close(); err != nil {
		t.Fatalf("节点 %d 关闭旧 store: %v", victim, err)
	}
	m, err := Rejoin(ctx, Options{
		NodeID:                 victim,
		Peers:                  tc.peers,
		DataGroups:             tc.dataGroups,
		Mode:                   tc.mode,
		Logger:                 testSlog(t),
		LeaderBalancerInterval: tc.balancerInterval,
		AutoPromoteLearners:    true,
	}, tc.dirs[victim])
	if err != nil {
		t.Fatalf("Rejoin(%d): %v", victim, err)
	}
	tc.stores[victim] = m.st
	tc.mgrs[victim] = m
	tc.killed[victim] = false // 恢复存活：清理期按正常节点 StopClean
	t.Logf("节点 %d 已经 Rejoin 自动编排恢复（dir=%s）", victim, tc.dirs[victim])

	// 数据收敛：断电前后写入的 200 键全部存在且一致（盲 apply 的观测面）
	tc.waitConverged(t, []string{"meta/topic/t0000", "meta/topic/t0099", "meta/topic/t0100", "meta/topic/t0199"}, 60*time.Second)

	// 自动升 voter：AutoPromoteLearners 循环在追平后把 victim 升回
	// 全组 voter——轮询 Status 断言 victim 在每组成员表的 voters 侧
	for g := uint32(0); g <= tc.dataGroups; g++ {
		tc.waitVoterInConfig(t, g, victim, 60*time.Second)
	}
}

// waitVoterInConfig 轮询全部存活节点的 Status：组 g 的成员表把 nodeID
// 列为 voter（Status.Config.Voters 即 ConfState 的运行时形态，[0] 为
// incoming 侧）。超时 Fatal。
func (tc *testCluster) waitVoterInConfig(t *testing.T, g uint32, nodeID uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, m := range tc.mgrs {
			if tc.killed[id] {
				continue
			}
			st, ok := m.Status(g)
			if !ok {
				continue
			}
			if _, ok := st.Config.Voters[0][nodeID]; ok {
				t.Logf("组 %d: 节点 %d 已是 voter（节点 %d 的成员表）", g, nodeID, id)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("组 %d: 节点 %d 未在 %v 内成为 voter", g, nodeID, timeout)
}

// TestSingleNodeUncleanRestartRejoins 单节点集群（peers 只有自己）断电后
// 调用 cluster.Rejoin 必须快速成功——评审 Critical 1 回归：prepareJoinPoll
// 对唯一 peer 跳过轮询自己，need 集合永不消减，裸走 30s 超时失败，节点
// 拒绝重启（单节点是受支持的形态：config 校验放行 peers:1，sq.example.yaml
// 有文档；而 kill -9 正是 Rejoin 存在意义的场景）。修复后单节点跳过
// PrepareJoin 编排（无对端可编排，Wipe + fresh 启动即完整恢复）。
//
// 断言：Rejoin 在远小于 30s 内成功（超时是修复前的失败形态），节点恢复
// 后重新成为全部组 leader（fresh 启动 + 单节点立即当选）。
func TestSingleNodeUncleanRestartRejoins(t *testing.T) {
	tc := newTestClusterN(t, AckQuorumMem, Options{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// 先写几键证明集群健康（断电前有数据）
	for i := 0; i < 10; i++ {
		b := tc.stores[1].NewBatch()
		_ = b.Set([]byte(fmt.Sprintf("meta/topic/single%04d", i)), []byte("v"))
		if err := (clusterReplicator{tc.mgrs[1]}).Apply(ctx, MetaGroup, b); err != nil {
			t.Fatalf("写 %d: %v", i, err)
		}
	}
	tc.kill(t, 1) // 不写标记 = 断电

	// Rejoin 前置（六步之 1）：调用方负责关旧 store
	if err := tc.stores[1].Close(); err != nil {
		t.Fatalf("节点 1 关闭旧 store: %v", err)
	}
	start := time.Now()
	m, err := Rejoin(ctx, Options{
		NodeID:                 1,
		Peers:                  tc.peers,
		DataGroups:             tc.dataGroups,
		Mode:                   tc.mode,
		Logger:                 testSlog(t),
		LeaderBalancerInterval: tc.balancerInterval,
		AutoPromoteLearners:    true,
	}, tc.dirs[1])
	if err != nil {
		t.Fatalf("Rejoin(单节点): %v", err)
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Fatalf("单节点 Rejoin 耗时 %v，超过 15s——疑似走了 PrepareJoin 30s 超时路径", elapsed)
	}
	t.Logf("单节点 Rejoin 完成（%v，无 PrepareJoin 超时）", time.Since(start).Round(time.Millisecond))
	tc.stores[1] = m.st
	tc.mgrs[1] = m
	tc.killed[1] = false // 恢复存活：清理期按正常节点 StopClean

	// 恢复后重新成为全部组 leader（fresh 单节点即刻当选）
	for g := uint32(0); g <= tc.dataGroups; g++ {
		tc.waitLeader(t, g, 1, 30*time.Second)
	}
	t.Logf("单节点断电恢复完成：全组 leader 复归节点 1")
}

// waitLeader 轮询节点 id 的 Leader(g) 直到报告 lead==nodeID（或超时
// Fatal）。单节点恢复场景用：节点必须重新成为各组 leader 才算恢复。
func (tc *testCluster) waitLeader(t *testing.T, g uint32, nodeID uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if lead, ok := tc.mgrs[nodeID].Leader(g); ok && lead == nodeID {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("组 %d: 节点 %d 未在 %v 内成为 leader", g, nodeID, timeout)
}

// TestControlRoundTrip 节点 A 经 Manager.Control 调 B 的控制通道：
// B 的 ControlHandler 收到 op/payload 并应答，A 取回应答；handler
// 报错时 A 收到带错误文本的 error；未知节点与超大 payload 在发送
// 侧直接拒绝（不发起连接）。
func TestControlRoundTrip(t *testing.T) {
	// 双节点 harness（控制通道是点对点 RPC，无需多数派语义）：
	// B 注入 ControlHandler——op=7 回显 payload、op=8 报错，并把
	// 每次调用推进 got 供断言「B 确实收到了」
	got := make(chan struct {
		op      byte
		payload []byte
	}, 8)
	handler := func(op byte, payload []byte) ([]byte, error) {
		got <- struct {
			op      byte
			payload []byte
		}{op, append([]byte(nil), payload...)}
		switch op {
		case 7:
			return append([]byte(nil), payload...), nil
		case 8:
			return nil, errors.New("handler-boom")
		default:
			return nil, fmt.Errorf("unexpected op %d", op)
		}
	}
	tc := newTestClusterN(t, AckQuorumMem, Options{ControlHandler: handler}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := tc.mgrs[1]

	// 成功路径：A 调 B 得回显，且 B 的 handler 确实收到 op=7 payload="ping"
	resp, err := a.Control(ctx, 2, 7, []byte("ping"))
	if err != nil {
		t.Fatalf("Control 成功路径: %v", err)
	}
	if string(resp) != "ping" {
		t.Fatalf("应答 %q; want %q", resp, "ping")
	}
	evt := <-got
	if evt.op != 7 || string(evt.payload) != "ping" {
		t.Fatalf("B 的 handler 收到 op=%d payload=%q; want op=7 payload=\"ping\"", evt.op, evt.payload)
	}

	// 错误路径：B 的 handler 返回 error，A 侧错误必须带该文本
	if _, err := a.Control(ctx, 2, 8, []byte("x")); err == nil || !strings.Contains(err.Error(), "handler-boom") {
		t.Fatalf("错误路径 err=%v; want 含 handler-boom", err)
	}
	evt = <-got
	if evt.op != 8 {
		t.Fatalf("B 的 handler 收到 op=%d; want 8", evt.op)
	}

	// 未知节点：Peers 表无此节点，发送侧直接报错
	if _, err := a.Control(ctx, 99, 1, nil); err == nil {
		t.Fatal("未知节点 Control 应报错，得到 nil")
	}

	// 超大 payload（16MiB 帧上限）：发送侧直接拒绝，不发起连接
	if _, err := a.Control(ctx, 2, 9, make([]byte, maxFrameLen)); err == nil {
		t.Fatal("超大 payload 应被发送侧拒绝，得到 nil")
	}
}

// TestControlOpRegistry 控制通道 op 注册表是跨节点线协议：帧里只有 1B
// op，取值被未来版本引用——改动即协议不兼容，锁死黄金值。
func TestControlOpRegistry(t *testing.T) {
	if OpForwardAppend != 1 {
		t.Fatalf("OpForwardAppend=%d; want 1（线协议黄金值）", OpForwardAppend)
	}
	if OpForwardApply != 2 {
		t.Fatalf("OpForwardApply=%d; want 2（线协议黄金值）", OpForwardApply)
	}
	if OpPrepareJoin != 3 {
		t.Fatalf("OpPrepareJoin=%d; want 3（线协议黄金值）", OpPrepareJoin)
	}
	if OpFetchSnapshot != 4 {
		t.Fatalf("OpFetchSnapshot=%d; want 4（线协议黄金值）", OpFetchSnapshot)
	}
	if OpSeedState != 5 {
		t.Fatalf("OpSeedState=%d; want 5（线协议黄金值）", OpSeedState)
	}
}

// TestClusterApplyAsyncOnFollowerReturnsErrNotLeader follower 上 ApplyAsync
// 的等待必须带回 ErrNotLeader（错误经 goroutine+channel 到 Wait 不丢失）
// ——协议面据此翻译可重试码。与 TestProposeOnFollowerReturnsErrNotLeader
// 同一条 Propose 快速失败路径，差别在异步拆分的传播面。
func TestClusterApplyAsyncOnFollowerReturnsErrNotLeader(t *testing.T) {
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
	b := tc.stores[follower].NewBatch()
	_ = b.Set([]byte("msg/it/async-follower"), []byte("v"))
	p, err := (clusterReplicator{tc.mgrs[follower]}).ApplyAsync(ctx, 1, b)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower ApplyAsync.Wait 应返回 ErrNotLeader，得到: %v", err)
	}
}

// TestClusterForwardAppendWire 用假 handler 测 ForwardAppend 线路：follower
// 经 Manager.Control 发 op=OpForwardAppend、payload=[4B BE g][EncodeMessage
// 字节] 给 leader；假 handler 校验载荷并回 [4B BE queueID][8B BE offset]，
// 发起方校验响应布局。真 produce 栈接线在 Task 11 e2e 覆盖。
func TestClusterForwardAppendWire(t *testing.T) {
	// 假 produce 栈：校验载荷布局（[4B g][msgRaw]）后回 leader 侧坐标
	got := make(chan struct {
		op      byte
		payload []byte
	}, 4)
	handler := func(op byte, payload []byte) ([]byte, error) {
		got <- struct {
			op      byte
			payload []byte
		}{op, append([]byte(nil), payload...)}
		switch op {
		case OpForwardAppend:
			if len(payload) < 4 {
				return nil, fmt.Errorf("ForwardAppend 载荷过短: %d", len(payload))
			}
			resp := make([]byte, 12)
			binary.BigEndian.PutUint32(resp[:4], 9)
			binary.BigEndian.PutUint64(resp[4:], 1234)
			return resp, nil
		default:
			return nil, fmt.Errorf("unexpected op %d", op)
		}
	}
	tc := newTestClusterN(t, AckQuorumMem, Options{ControlHandler: handler}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lead := tc.leaderOf(t, 1)
	var follower uint64
	for id := range tc.mgrs {
		if id != lead {
			follower = id
			break
		}
	}
	raw, err := core.EncodeMessage(&core.Message{ID: "FWD1", Topic: "orders", Body: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4+len(raw))
	binary.BigEndian.PutUint32(payload[:4], 1)
	copy(payload[4:], raw)
	resp, err := tc.mgrs[follower].Control(ctx, lead, OpForwardAppend, payload)
	if err != nil {
		t.Fatalf("ForwardAppend 线路: %v", err)
	}
	if len(resp) != 12 {
		t.Fatalf("响应 %d B; want 12（[4B queueID][8B offset]）", len(resp))
	}
	if qid := binary.BigEndian.Uint32(resp[:4]); qid != 9 {
		t.Fatalf("queueID=%d; want 9", qid)
	}
	if off := binary.BigEndian.Uint64(resp[4:]); off != 1234 {
		t.Fatalf("offset=%d; want 1234", off)
	}
	evt := <-got
	if evt.op != OpForwardAppend || !bytes.Equal(evt.payload, payload) {
		t.Fatalf("leader handler 收到 op=%d payload=%v; want op=%d payload=%v", evt.op, evt.payload, OpForwardAppend, payload)
	}
}

// TestClusterForwardApplyConverges follower 把构造无关删除批次（纯
// Delete）的 repr 经 op=OpForwardApply 转发给 leader，leader 侧假 produce
// 栈（dispatch goroutine，遵守 handler 不阻塞契约）把 repr 提进本组
// Propose——与 Task 11 的 ControlHandler 装配同形——三节点删除收敛。
func TestClusterForwardApplyConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// 假 produce 栈：收到请求即 dispatch 提案。self 是接收节点（= 发起
	// 方按 Leader(g) 寻址的目标 leader）的 Manager——handler 共享给全部
	// 节点、不知道自己的 node id，测试在选主后经 atomic 注入
	var self atomic.Pointer[Manager]
	proposeErr := make(chan error, 1)
	handler := func(op byte, payload []byte) ([]byte, error) {
		if op != OpForwardApply {
			return nil, fmt.Errorf("unexpected op %d", op)
		}
		if len(payload) < 4 {
			return nil, fmt.Errorf("ForwardApply 载荷过短: %d", len(payload))
		}
		g := binary.BigEndian.Uint32(payload[:4])
		repr := append([]byte(nil), payload[4:]...) // 拷贝：跨 goroutine 持有
		go func() { proposeErr <- self.Load().Propose(ctx, g, repr) }()
		return nil, nil
	}
	tc := newTestClusterOpts(t, AckQuorumMem, Options{ControlHandler: handler})
	lead := tc.leaderOf(t, 1)
	self.Store(tc.mgrs[lead])
	// 先让键在三节点可见（删除才可观测）
	b := tc.stores[lead].NewBatch()
	_ = b.Set([]byte("msg/it/fwd-del"), []byte("v"))
	if err := (clusterReplicator{tc.mgrs[lead]}).Apply(ctx, 1, b); err != nil {
		t.Fatal(err)
	}
	tc.waitConverged(t, []string{"msg/it/fwd-del"}, 30*time.Second)
	// follower 构造纯 Delete 批次并转发给 leader 提案
	var follower uint64
	for id := range tc.mgrs {
		if id != lead {
			follower = id
			break
		}
	}
	del := tc.stores[follower].NewBatch()
	if err := del.Delete([]byte("msg/it/fwd-del")); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4+len(del.Repr()))
	binary.BigEndian.PutUint32(payload[:4], 1)
	copy(payload[4:], del.Repr())
	_ = del.Close() // repr 已取出，批次回收失败不挡转发（同 Cluster.Apply 契约）
	if _, err := tc.mgrs[follower].Control(ctx, lead, OpForwardApply, payload); err != nil {
		t.Fatalf("ForwardApply 线路: %v", err)
	}
	// 提案成功（handler dispatch 的 goroutine 回传）后再等三节点删除收敛
	select {
	case err := <-proposeErr:
		if err != nil {
			t.Fatalf("leader 侧提案: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("leader 侧提案未返回")
	}
	tc.waitAbsent(t, tc.aliveIDs(), []string{"msg/it/fwd-del"}, 30*time.Second)
}

// appliedEvt 是 OnApplied 钩子收集器的事件：组号 + 原始批次 repr。
type appliedEvt struct {
	g    uint32
	repr []byte
}

// leaderEvt 是 OnLeaderChange 钩子收集器的事件：组号 + 新 leader + 是否自身。
type leaderEvt struct {
	g      uint32
	leader uint64
	isSelf bool
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

	// 钩子收集器（newTestClusterOpts 注入对应钩子时才会创建）：钩子签名
	// 不带节点 id，harness 逐节点包裹闭包把事件分流进各节点 channel
	appliedChs map[uint64]chan appliedEvt // 节点 → OnApplied 事件流
	leaderChs  map[uint64]chan leaderEvt  // 节点 → OnLeaderChange 事件流

	balancerInterval time.Duration // 摊布循环周期（harness 注入，见 newTestClusterN）
}

// newTestCluster 起三节点集群（真实 TCP），不带钩子注入。
func newTestCluster(t *testing.T, mode AckMode) *testCluster {
	return newTestClusterN(t, mode, Options{}, 3)
}

// newTestClusterOpts 起三节点集群（真实 TCP），并支持注入装配钩子。
//
// hookOpts 只消费 OnLeaderChange/OnApplied/ControlHandler 三个字段：
// 对应钩子非 nil 时，harness 为每个节点挂上包裹闭包——把事件推进该
// 节点的收集器 channel（appliedChs/leaderChs），再转发给注入的 raw
// 钩子。钩子签名不带节点 id，逐节点 channel 分流是测试断言「三节点
// 都收到」的观测面。
//
// 装配序：先 net.Listen ×n 收集地址再拼 Peers 表——解拨号先有鸡还是
// 先有蛋（传输层按 Peers 拨号，必须拿到全部地址后才能建 Manager），
// 然后逐节点 store.Open(t.TempDir()/id) + NewManager（Listener 注入
// 已建监听）+ Start。
//
// 清理按逆序注册：StopClean 存活节点 → 等全部 Done → 关 store。
// kill 过的节点跳过 StopClean（运行 ctx 已取消）；rejoin 过的节点
// 已重置 killed 标记，按存活节点正常收尾（stores/mgrs 指向新实例）。
func newTestClusterOpts(t *testing.T, mode AckMode, hookOpts Options) *testCluster {
	return newTestClusterN(t, mode, hookOpts, 3)
}

// newTestClusterN 同 newTestClusterOpts，但节点数可配：控制通道等
// 无需多数派语义的测试用双节点即可，避免三节点无谓的选举开销。
func newTestClusterN(t *testing.T, mode AckMode, hookOpts Options, n uint64) *testCluster {
	t.Helper()
	// 摊布循环默认注入 200ms 周期（brief 约定「harness 注入 balancer
	// 间隔 200ms」）：摊布只在「本节点连续 lead 某组 ≥3 个周期」后才
	// 发起转移（见 StartLeaderBalancer 注释）——不会打断测试的读写
	// 窗口，因此全量场景套件统一跑在摊布语义下，TestLeaderSpreadConverges
	// 无需任何特殊装配。
	if hookOpts.LeaderBalancerInterval <= 0 {
		hookOpts.LeaderBalancerInterval = 200 * time.Millisecond
	}
	// 1. 先建 n 个监听器收集地址，再拼 Peers 表
	lstns := make([]net.Listener, 0, n)
	peers := make(map[uint64]string, n)
	for i := uint64(1); i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("节点 %d 预建监听: %v", i, err)
		}
		lstns = append(lstns, ln)
		peers[i] = ln.Addr().String()
	}
	tc := &testCluster{
		dirs:             make(map[uint64]string, n),
		stores:           make(map[uint64]*store.Store, n),
		mgrs:             make(map[uint64]*Manager, n),
		peers:            peers,
		killed:           make(map[uint64]bool, n),
		dataGroups:       3,
		mode:             mode,
		balancerInterval: hookOpts.LeaderBalancerInterval,
	}
	if hookOpts.OnApplied != nil {
		tc.appliedChs = make(map[uint64]chan appliedEvt, n)
	}
	if hookOpts.OnLeaderChange != nil {
		tc.leaderChs = make(map[uint64]chan leaderEvt, n)
	}
	// 2. 逐节点开 store + NewManager + Start（节点目录 = t.TempDir()/id，
	//    WipeForRejoin 只需清节点目录，父目录保留）
	for i := uint64(1); i <= n; i++ {
		dir := fmt.Sprintf("%s/%d", t.TempDir(), i)
		st, err := store.Open(dir, false, testSlog(t))
		if err != nil {
			t.Fatalf("节点 %d 开 store: %v", i, err)
		}
		// 逐节点钩子包裹闭包：事件推进收集器（缓冲 64——正常场景每节点
		// 每次选举/每条提案各 1 条，远低于容量；测试不消费也不阻塞
		// Ready 循环），再转发 raw 钩子
		var onLC func(g uint32, leader uint64, isSelf bool)
		if hookOpts.OnLeaderChange != nil {
			raw := hookOpts.OnLeaderChange
			tc.leaderChs[i] = make(chan leaderEvt, 64)
			onLC = func(g uint32, leader uint64, isSelf bool) {
				tc.leaderChs[i] <- leaderEvt{g: g, leader: leader, isSelf: isSelf}
				raw(g, leader, isSelf)
			}
		}
		var onApplied func(g uint32, repr []byte)
		if hookOpts.OnApplied != nil {
			raw := hookOpts.OnApplied
			tc.appliedChs[i] = make(chan appliedEvt, 64)
			onApplied = func(g uint32, repr []byte) {
				// 拷贝：钩子契约禁止保留 repr（Ready 循环缓冲复用），
				// 收集器跨协程持有必须深拷贝
				repr = append([]byte(nil), repr...)
				tc.appliedChs[i] <- appliedEvt{g: g, repr: repr}
				raw(g, repr)
			}
		}
		m, err := NewManager(Options{
			NodeID:                 i,
			Peers:                  peers,
			Listener:               lstns[i-1], // 注入预建监听：Peers 地址与监听一一对应
			DataGroups:             3,
			Mode:                   mode,
			Store:                  st,
			Logger:                 testSlog(t),
			OnLeaderChange:         onLC,
			OnApplied:              onApplied,
			ControlHandler:         hookOpts.ControlHandler,
			LeaderBalancerInterval: hookOpts.LeaderBalancerInterval,
			AutoPromoteLearners:    hookOpts.AutoPromoteLearners,
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

// preferredOf 返回组 g 的 preferred leader 节点：sortedPeerIDs(peers)[g % n]。
func (tc *testCluster) preferredOf(g uint32) uint64 {
	ids := make([]uint64, 0, len(tc.mgrs))
	for id := range tc.mgrs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids[g%uint32(len(ids))]
}

// leadersMatchPreferred 判定全部组 leader 分布已收敛到 preferred：每组的
// preferred 节点自报为该组 leader（摊布循环收敛的观测面）。
func (tc *testCluster) leadersMatchPreferred(t *testing.T) bool {
	t.Helper()
	for g := uint32(0); g <= tc.dataGroups; g++ {
		pref := tc.preferredOf(g)
		if lead, ok := tc.mgrs[pref].Leader(g); !ok || lead != pref {
			return false
		}
	}
	return true
}

// waitAppliedEvt 轮询指定节点的 OnApplied 收集器，直到收到组 g 上
// repr 匹配的事件（不匹配事件丢弃：startup 空条目等噪声），超时 Fatal。
func (tc *testCluster) waitAppliedEvt(t *testing.T, id uint64, g uint32, repr []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case evt := <-tc.appliedChs[id]:
			if evt.g == g && bytes.Equal(evt.repr, repr) {
				t.Logf("节点 %d 已收到组 %d 的 OnApplied 事件（repr %d B）", id, g, len(repr))
				return
			}
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	t.Fatalf("节点 %d 未在 %v 内收到组 %d 的 OnApplied 事件", id, timeout, g)
}

// waitLeaderChangeIsSelf 轮询全部存活节点的 OnLeaderChange 收集器，直到
// 某节点收到组 g 上 isSelf=true 且 leader≠exclude 的事件（kill-leader 后
// 新 leader 的当选回调），返回该事件。启动期旧 leader 的 isSelf 事件以
// leader==exclude 过滤——回调是「新 leader 已产生」的确定性观测面。
func (tc *testCluster) waitLeaderChangeIsSelf(t *testing.T, g uint32, exclude uint64, timeout time.Duration) leaderEvt {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, ch := range tc.leaderChs {
			if tc.killed[id] {
				continue
			}
			select {
			case evt := <-ch:
				if evt.g == g && evt.isSelf && evt.leader != exclude {
					t.Logf("节点 %d 收到组 %d 当选回调: leader=%d isSelf=true", id, g, evt.leader)
					return evt
				}
			default:
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("组 %d: 存活节点未在 %v 内收到 isSelf=true 的 leader 变更回调（排除旧 leader %d）", g, timeout, exclude)
	return leaderEvt{}
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

// waitAbsent 轮询给定节点集合：全部 keys 均不可读（删除批次收敛的
// 观测面），超时 Fatal 附仍持有键的节点。
func (tc *testCluster) waitAbsent(t *testing.T, ids []uint64, keys []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tc.absent(ids, keys) {
			t.Logf("删除收敛: 节点 %v 上 %d 键全部消失", ids, len(keys))
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	// 超时：逐节点列出仍持有的键，附排障上下文
	var sb strings.Builder
	fmt.Fprintf(&sb, "节点 %v 上 %d 键未在 %v 内删除:\n", ids, len(keys), timeout)
	for _, id := range ids {
		var still []string
		for _, k := range keys {
			_, ok, err := tc.stores[id].Get([]byte(k))
			if err != nil || ok {
				still = append(still, k)
			}
		}
		fmt.Fprintf(&sb, "  节点 %d: 仍有 %v\n", id, still)
	}
	t.Fatal(sb.String())
}

// absent 判定给定节点集合上全部 keys 均已不可读。
func (tc *testCluster) absent(ids []uint64, keys []string) bool {
	for _, k := range keys {
		for _, id := range ids {
			_, ok, err := tc.stores[id].Get([]byte(k))
			if err != nil || ok {
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
		NodeID:                 victim,
		Peers:                  tc.peers,
		Listener:               ln,
		DataGroups:             tc.dataGroups,
		Mode:                   tc.mode,
		Store:                  st,
		Logger:                 testSlog(t),
		LeaderBalancerInterval: tc.balancerInterval,
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

// ---- batch④ 快照链路 harness（startThreeNodeCluster）----

// snapHarness 是快照链路场景测试的三节点 harness：nodes[i] 即节点 i+1
// （index = nodeID-1），三节点经 127.0.0.1 随机端口真实 TCP 互联。
//
// 与 testCluster 的关系：testCluster 以 map[uint64]*Manager 组织、leaderOf
// 返回节点 ID；本 harness 以切片组织、leaderOf 返回 *Manager——快照场景
// 直接操作 Manager 的注册表与控制处理器，切片索引即节点序，读起来更直白。
// 两者并存，既有测试不迁移。后续任务（分区/截断/重入）的 harness 原语
// （partitionOff/healPartition/truncateNow 等）在本类型上扩展——peers 与
// mode 已备好，重入类场景另需 dirs 表（届时按需补充）。
type snapHarness struct {
	nodes  []*Manager        // index = nodeID-1（节点 1..3）
	stores []*store.Store    // 与 nodes 同序（清理用）
	dirs   []string          // 与 nodes 同序：原地重启（关 store → 同目录重开）用
	peers  map[uint64]string // 节点 id → raft 监听地址（分区用例备用）
	mode   AckMode
	retain uint64 // 节点截断保留量（Options.RetainEntries 透传，重启时复刻）
	logs   []*logCapture // 与 nodes 同序：countLog 的检索源（快照路径证据）
}

// logCapture 累积一个节点全部日志的格式化行（countLog 的检索源）。
// 与 testWriter 并存：同一行既进 t.Log（失败可查）也进 capture。
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

// append 记录一行（原子；slog 每条记录一次 Write，见 captureWriter）。
func (c *logCapture) append(line string) {
	c.mu.Lock()
	c.lines = append(c.lines, line)
	c.mu.Unlock()
}

// count 返回包含 needle 的行数（快照路径证据：追平是否走了安装日志）。
func (c *logCapture) count(needle string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, l := range c.lines {
		if strings.Contains(l, needle) {
			n++
		}
	}
	return n
}

// captureWriter 把 slog 的格式化输出同时交给测试输出与 logCapture。
// slog 的 TextHandler 每条记录一次 Write（go1.26 的 commonHandler 实现），
// Write 收到的就是整行——逐行累积即「按行检索」。
type captureWriter struct {
	t   *testing.T
	cap *logCapture
}

func (w captureWriter) Write(p []byte) (int, error) {
	w.cap.append(string(p))
	w.t.Log(string(p))
	return len(p), nil
}

// newCaptureSlog 构造同时写 t.Log 与 logCapture 的 slog（节点级）。
// Manager 与各组共享同一 handler 链（lg.With 派生），快照安装的关键
// 日志一行不落。
func newCaptureSlog(t *testing.T, cap *logCapture) *slog.Logger {
	return slog.New(slog.NewTextHandler(&captureWriter{t: t, cap: cap}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// startThreeNodeCluster 起三节点快照链路 harness（真实 TCP）。
//
// 装配序与 newTestClusterN 相同：先预建 n 个监听收集地址再拼 Peers 表
// （传输层按 Peers 拨号，必须先拿到全部地址），然后逐节点
// store.Open(t.TempDir()/id) + NewManager（Listener 注入已建监听）+ Start。
//
// opts 是 Options 级注入（如 withRetainEntries(8) 逼出快照路径）——
// 每个节点收到同一份注入，Logger 除外（节点级捕获日志器）。
//
// 清理经 t.Cleanup 注册（StopClean → 等 Done → 关 store）；stopAll 是
// 显式停机入口——与 Cleanup 重叠安全（StopClean 幂等：cancel 与关通道
// 幂等、干净关机标记重复写无害）。
func startThreeNodeCluster(t *testing.T, opts ...func(*Options)) *snapHarness {
	t.Helper()
	const n = 3
	lstns := make([]net.Listener, 0, n)
	peers := make(map[uint64]string, n)
	for i := uint64(1); i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("节点 %d 预建监听: %v", i, err)
		}
		lstns = append(lstns, ln)
		peers[i] = ln.Addr().String()
	}
	h := &snapHarness{peers: peers, mode: AckQuorumMem}
	for i := uint64(1); i <= n; i++ {
		dir := fmt.Sprintf("%s/%d", t.TempDir(), i)
		st, err := store.Open(dir, false, testSlog(t))
		if err != nil {
			t.Fatalf("节点 %d 开 store: %v", i, err)
		}
		cap := &logCapture{}
		h.logs = append(h.logs, cap)
		h.dirs = append(h.dirs, dir)
		base := Options{
			NodeID:     i,
			Peers:      peers,
			Listener:   lstns[i-1],
			DataGroups: 3,
			Mode:       h.mode,
			Store:      st,
			Logger:     newCaptureSlog(t, cap),
		}
		for _, o := range opts {
			o(&base)
		}
		h.retain = base.RetainEntries
		m, err := NewManager(base)
		if err != nil {
			t.Fatalf("节点 %d NewManager: %v", i, err)
		}
		m.Start(context.Background())
		h.nodes = append(h.nodes, m)
		h.stores = append(h.stores, st)
	}
	t.Cleanup(func() {
		h.stopAll()
		for _, m := range h.nodes {
			select {
			case <-m.Done():
			case <-time.After(10 * time.Second):
				t.Errorf("清理: 节点未在 10s 内完全退出")
			}
		}
		// 强制回收各节点注册表里的快照视图：测试结束远早于默认 TTL
		// （5min），不回收的话 store.Close 会报「leaked snapshots」噪声；
		// 远未来 now 让 GCOnce 一次全量回收（幂等，Close 安全）。
		for _, m := range h.nodes {
			m.snaps.GCOnce(time.Now().Add(time.Hour))
		}
		for _, st := range h.stores {
			if err := st.Close(); err != nil {
				t.Logf("清理: store.Close: %v", err)
			}
		}
	})
	return h
}

// stopAll 显式停掉全部节点（StopClean）。幂等：t.Cleanup 的兜底清理会
// 再调一次，cancel 幂等、done 通道已关闭、干净标记重复写无害。
func (h *snapHarness) stopAll() {
	for _, m := range h.nodes {
		if err := m.StopClean(context.Background()); err != nil {
			_ = err // 停机错误不吞测试主流程；细节归 Cleanup 日志
		}
	}
}

// stopNodeClean 把单个节点原地干净停机并关闭其 store（重启测试用）。
// 停机 = 全机件退出 + 干净关机标记落盘；关 store 释放 pebble 文件锁，
// 之后才能同目录重开。
func (h *snapHarness) stopNodeClean(t *testing.T, idx int) {
	t.Helper()
	m := h.nodes[idx]
	if err := m.StopClean(context.Background()); err != nil {
		t.Fatalf("节点 %d 干净停机: %v", m.nodeID, err)
	}
	select {
	case <-m.Done():
	case <-time.After(10 * time.Second):
		t.Fatalf("节点 %d 未在 10s 内完全退出", m.nodeID)
	}
	if err := h.stores[idx].Close(); err != nil {
		t.Fatalf("节点 %d 关 store: %v", m.nodeID, err)
	}
}

// startNode 在已停机节点的目录上原地重建节点（同目录重开 store +
// NewManager + Start），并替换 harness 里的节点/store/日志引用。
//
// 为什么监听复用原地址：传输层对端按 Peers 表地址拨号，重启后地址变
// 了对端永远拨不通；同地址重绑依赖 Go 监听默认 SO_REUSEADDR（旧连接
// 断开后端口立即可重绑）。
//
// 注意：新 Manager 的传输层从零开始（无分区状态）——调用方按需自行
// 处理分区语义。opts 是 Options 级注入（如重新注入 RetainEntries）。
func (h *snapHarness) startNode(t *testing.T, idx int, opts ...func(*Options)) *Manager {
	t.Helper()
	nodeID := uint64(idx + 1)
	ln, err := net.Listen("tcp", h.peers[nodeID])
	if err != nil {
		t.Fatalf("节点 %d 重绑监听 %s: %v", nodeID, h.peers[nodeID], err)
	}
	st, err := store.Open(h.dirs[idx], false, testSlog(t))
	if err != nil {
		t.Fatalf("节点 %d 重开 store: %v", nodeID, err)
	}
	cap := &logCapture{}
	base := Options{
		NodeID:        nodeID,
		Peers:         h.peers,
		Listener:      ln,
		DataGroups:    3,
		Mode:          h.mode,
		Store:         st,
		Logger:        newCaptureSlog(t, cap),
		RetainEntries: h.retain,
	}
	for _, o := range opts {
		o(&base)
	}
	m, err := NewManager(base)
	if err != nil {
		t.Fatalf("节点 %d 重启 NewManager: %v", nodeID, err)
	}
	m.Start(context.Background())
	h.nodes[idx] = m
	h.stores[idx] = st
	h.logs[idx] = cap
	return m
}

// restartNodeClean 原地干净重启节点：stopNodeClean + startNode 一次完成。
func (h *snapHarness) restartNodeClean(t *testing.T, idx int, opts ...func(*Options)) *Manager {
	t.Helper()
	h.stopNodeClean(t, idx)
	return h.startNode(t, idx, opts...)
}

// leaderOf 返回组 g 当前 leader 的 Manager，带 WaitLeader 轮询语义
// （任何节点自报 lead 且 lead 在 1..n 即作数，超时 Fatal）。
func (h *snapHarness) leaderOf(t *testing.T, g uint32) *Manager {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range h.nodes {
			if lead, ok := m.Leader(g); ok && lead != 0 && lead <= uint64(len(h.nodes)) {
				return h.nodes[lead-1]
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("组 %d 在 60s 内未选出 leader", g)
	return nil
}

// nonLeaderOf 返回组 g 当前非 leader 的一个节点（快照场景的落后方）。
func (h *snapHarness) nonLeaderOf(t *testing.T, g uint32) *Manager {
	t.Helper()
	leader := h.leaderOf(t, g)
	for _, m := range h.nodes {
		if m != leader {
			return m
		}
	}
	t.Fatalf("组 %d 无非 leader 节点", g)
	return nil
}

// partitionOff 把节点与集群整段隔断（双向丢消息，节点保持存活）。
//
// 实现：该节点自身传输层的分区开关（transport.setPartitioned）——
// 出站消息不入队、入站消息不 deliver，等价于物理网络分区。为什么
// 双向：单向隔断时对端仍能收到落后方的选举消息（其任期不断自增），
// 每次竞选都会把 leader 打回 follower 一轮——200 条写入会被反复打断；
// 双向隔断则落后方在自己孤立的任期里空转，多数派不受干扰。
func (h *snapHarness) partitionOff(victim *Manager) {
	victim.tr.setPartitioned(true)
}

// healPartition 恢复 partitionOff 的隔断（双向恢复投递）。
func (h *snapHarness) healPartition(victim *Manager) {
	victim.tr.setPartitioned(false)
}

// truncateNow 在组长节点上执行一次本地截断（Task 8 的 truncateOnce
// 上线前的测试等价物）：SaveSnapMeta（锚点 = applied-retain，带 term
// 与成员表）→ TruncateLog → mem.Compact，三步与生产的「先锚点后
// 截断」顺序一致——截断之后日志起点越过落后方位点，raft 判定它只能
// 靠 MsgSnap 追平。
func (h *snapHarness) truncateNow(t *testing.T, g uint32) {
	t.Helper()
	leader := h.leaderOf(t, g)
	gr := leader.groups[g]
	applied := gr.appliedIndex()
	retain := leader.retainEntries
	if applied <= retain {
		t.Fatalf("组 %d applied=%d 不足以截断（retain=%d）", g, applied, retain)
	}
	upto := applied - retain
	term, err := gr.mem.Term(upto)
	if err != nil {
		t.Fatalf("组 %d 锚点位 %d 的 term 不可查: %v", g, upto, err)
	}
	idx, tm := upto, term
	meta := &raftpb.SnapshotMetadata{Index: &idx, Term: &tm, ConfState: gr.confState.Load()}
	if err := leader.rs.SaveSnapMeta(g, meta); err != nil {
		t.Fatal(err)
	}
	if err := leader.rs.TruncateLog(g, upto); err != nil {
		t.Fatal(err)
	}
	if err := gr.mem.Compact(upto); err != nil {
		t.Fatal(err)
	}
}

// countLog 统计节点 m 的捕获日志中含 needle 的行数（快照路径证据：
// 「快照安装完成」出现 ≥1 次 = 追平确实走了快照安装）。
func (h *snapHarness) countLog(m *Manager, needle string) int {
	for i, node := range h.nodes {
		if node == m {
			return h.logs[i].count(needle)
		}
	}
	return 0
}

// waitFor 轮询等待条件成立，超时 Fatal（快照场景的收敛判据用）。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(msg)
}

// withRetainEntries 注入截断循环的日志保留量（Options.RetainEntries，
// 默认 10000）：缩小到个位数把落后方逼进快照路径（落后位点落在截断
// 区间内，日志追齐不可能，只能 MsgSnap）。
func withRetainEntries(n uint64) func(*Options) {
	return func(o *Options) { o.RetainEntries = n }
}

// encodeFetchReq 编码 OpFetchSnapshot 请求：[4B BE 组][8B BE snapID]
// [4B BE 游标键长][游标键]，全部大端（与 transport.go op 注册表注释
// 同款布局）。
func encodeFetchReq(g uint32, id uint64, cursor []byte) []byte {
	req := make([]byte, 16+len(cursor))
	binary.BigEndian.PutUint32(req[:4], g)
	binary.BigEndian.PutUint64(req[4:12], id)
	binary.BigEndian.PutUint32(req[12:16], uint32(len(cursor)))
	copy(req[16:], cursor)
	return req
}

// decodeFetchResp 解码 OpFetchSnapshot 响应：[1B 是否结束][4B BE 下一
// 游标键长][下一游标键][块字节]。done=true 后不得再以 next 续拉；
// 坏布局直接 Fatal（测试自证的线协议校验）。
func decodeFetchResp(t *testing.T, resp []byte) (done bool, next []byte, chunk []byte) {
	t.Helper()
	if len(resp) < 5 {
		t.Fatalf("FetchSnapshot 响应 %d B 过短（不足 1B 结束位 + 4B 游标长）", len(resp))
	}
	done = resp[0] == 1
	nl := binary.BigEndian.Uint32(resp[1:5])
	if uint32(len(resp)-5) < nl {
		t.Fatalf("FetchSnapshot 响应游标键长 %d 超出剩余 %d B", nl, len(resp)-5)
	}
	next = resp[5 : 5+int(nl)]
	chunk = resp[5+int(nl):]
	return done, next, chunk
}

// TestFetchSnapshotStreamsGroupState 发送侧端到端：注册一份视图，
// 经控制通道分块拉完，内容与视图一致且不含别组键。
func TestFetchSnapshotStreamsGroupState(t *testing.T) {
	h := startThreeNodeCluster(t) // 既有辅助
	defer h.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 在组 0 写入若干全局键（经 leader 提案，三节点收敛）
	leader := h.leaderOf(t, 0)
	for i := 0; i < 50; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("T%02d", i)), []byte("meta")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}

	id := leader.snaps.Create(0, leader.AppliedIndex(0))
	defer leader.snaps.Release(id)

	got := map[string]string{}
	cursor := []byte(nil)
	for i := 0; ; i++ {
		if i > 200 {
			t.Fatal("拉取不收敛（超过 200 块）")
		}
		resp, err := leader.handleFetchSnapshot(encodeFetchReq(0, id, cursor))
		if err != nil {
			t.Fatalf("第 %d 块: %v", i, err)
		}
		done, next, chunk := decodeFetchResp(t, resp)
		pairs, err := decodeChunk(chunk)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range pairs {
			got[string(p.k)] = string(p.v)
		}
		if done {
			break
		}
		cursor = next
	}
	if len(got) < 50 {
		t.Fatalf("拉到 %d 个键; want ≥50（50 个 topic 元数据）", len(got))
	}
	for k := range got {
		if !strings.HasPrefix(k, "meta/") && !strings.HasPrefix(k, "delay") &&
			!strings.HasPrefix(k, "half") {
			t.Fatalf("组 0 快照混入了非全局键 %q", k)
		}
	}
}

// TestFetchSnapshotRejectsUnknownID 过期/未知 snapID 必须显式报错，
// 不能返回空块让接收方以为「拉完了」（那是静默的空状态安装）。
func TestFetchSnapshotRejectsUnknownID(t *testing.T) {
	h := startThreeNodeCluster(t)
	defer h.stopAll()
	if _, err := h.nodes[0].handleFetchSnapshot(encodeFetchReq(0, 99999, nil)); err == nil {
		t.Fatal("未知 snapID 必须报错")
	}
}

// TestLaggingFollowerCatchesUpBySnapshot 端到端：让一个 follower 停摆、
// leader 写入并截断到它需要的位点之上、follower 回来 —— 必须靠快照追平。
//
// 场景时序（为什么截断在分区期间做）：落后方恢复后 raft 先尝试日志
// 追齐（它的位点还在 leader 的日志区间内时，MsgApp 就够）；只有把
// 日志截断到落后位点之上，「日志追齐不可能、只能 MsgSnap」才成立。
// 追平判据是 FSM 数据（S199 可读）而非日志位点——快照路径的最终
// 验收就是客户端数据完整。
func TestLaggingFollowerCatchesUpBySnapshot(t *testing.T) {
	h := startThreeNodeCluster(t, withRetainEntries(8)) // 极小保留量逼出快照路径
	defer h.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	victim := h.nonLeaderOf(t, 0)
	h.partitionOff(victim) // 既有辅助：断开该节点的传输层

	leader := h.leaderOf(t, 0)
	for i := 0; i < 200; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("S%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	h.truncateNow(t, 0) // 触发一次截断循环

	h.healPartition(victim)
	// 追平判据：victim 上能读到最后一个键，且「快照安装完成」日志已落
	// （安装分两步：数据先落、内存侧 Apply 完成才打 Info，只查数据会把
	// 断言竞态到完成日志之前）。
	waitFor(t, 60*time.Second, func() bool {
		_, ok, _ := victim.Store().Get(store.TopicMetaKey("S199"))
		return ok && h.countLog(victim, "快照安装完成") >= 1
	}, "落后节点未在 60s 内经快照追平（数据可见或「快照安装完成」日志缺失）")
}

// TestInstallingMarkerRestartClearsRaftLog（C1 修复）安装中标记重启必须
// 把本组 raft 日志与 HardState 一并清空：重启后 firstIndex=1，leader
// 的探测一路回退到日志起点，只能全量重放或发快照——两条路径都完整，
// 杜绝「半截状态当完整状态启动」的静默丢段。
//
// 磁盘实况的复现（与 C1 描述逐条对应）：
//   - R1 批先写入并等 victim 追平：此刻 X 的日志覆盖 1..P1（P1=54），
//     FSM 含全部 R1 键
//   - 隔断 X；leader 写 R2 批（55..104）并把日志截断到 96——X 落在
//     截断点之下，只能靠快照追平（对应 C1 的「开始一次快照安装」）
//   - 在 X 上直接制造安装中崩溃痕迹：MarkInstalling（安装第 2 步的
//     标记）+ wipeGroupKeys（第 3 步的 FSM 清空），然后干净关机——
//     模拟滚动重启中途关机的半截状态
//   - X 停机期间在盘上制造 C1 前提「日志被截断在锚点 A1」：
//     SaveSnapMeta(A1 = P1-8) + TruncateLog(A1)，X 的日志只剩
//     A1+1..P1，状态 1..A1 只在 FSM 侧存在过（对应 C1 的「raft log
//     truncated at anchor A」）
//   - 杀掉旧 leader 后重启 X：处理 X 追平的是新 leader，它不知道 X
//     的日志起点，只能靠探测从头摸起——正是 C1 描述里「换人后探测锚
//     在 A+1..P 上撞车、永不发快照」的缺口场景
//
// 断言：
//   - X 重启瞬间（NewManager 返回后、Start 前）日志必须全空：ents 空、
//     HardState 空、无锚点、applied=0、firstIndex=1——这是修复的判定面
//   - X 最终收敛到完整状态：锚点之前的 R1 键（R1_00/R1_10/R1_41，即
//     C1 的静默丢失键）与停机后的 R2 批全部可读
//   - 换人后的集群继续正常服务：新 leader 再写 R3 批，X 追平
//
// 修复前本测试失败于「R1_00 不在 X 上」：旧代码只清 applied 不动日志，
// A1+1..P1 原样保留，新 leader 探测锚在 P1 上撞车、只流式重放 P1+1..，
// X 的 FSM 永久缺 1..A1（R1 键全部丢失）。
func TestInstallingMarkerRestartClearsRaftLog(t *testing.T) {
	h := startThreeNodeCluster(t, withRetainEntries(8))
	defer h.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	victim := h.nonLeaderOf(t, 0)
	vi := int(victim.nodeID - 1)
	leader := h.leaderOf(t, 0)
	// 第三个节点：日志从未被截断，锚点 term 与换人后的选举都靠它
	var other *Manager
	for _, m := range h.nodes {
		if m != victim && m != leader {
			other = m
		}
	}
	if other == nil {
		t.Fatal("测试前提不成立：需要既非 leader 也非 victim 的第三个节点")
	}

	// R1 批（条目 5..54）：victim 追平后其日志覆盖 1..54
	for i := 0; i < 50; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("R1_%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	waitFor(t, 30*time.Second, func() bool {
		_, ok, _ := victim.Store().Get(store.TopicMetaKey("R1_49"))
		return ok
	}, "victim 未追平 R1 批")

	// 隔断 X，记下其位点（隔断期间不再推进），leader 写 R2 批并截断到
	// X 的位点之上——X 只能靠快照追平
	h.partitionOff(victim)
	xApplied := victim.AppliedIndex(0)
	for i := 0; i < 50; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("R2_%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	h.truncateNow(t, 0)

	// 制造安装中崩溃痕迹：标记（安装第 2 步，Sync）+ FSM 清空（第 3 步）
	idx, tm := xApplied, victim.groups[0].currentTerm()
	if err := victim.rs.MarkInstalling(0, &raftpb.SnapshotMetadata{Index: &idx, Term: &tm}); err != nil {
		t.Fatal(err)
	}
	if err := wipeGroupKeys(victim.Store(), 0, victim.dataGroups); err != nil {
		t.Fatal(err)
	}
	h.stopNodeClean(t, vi)

	// 停机期间制造 C1 前提「日志被截断在锚点 A1」：X 的日志只剩
	// A1+1..P1。锚点 index 与截断循环同形（applied - retain），term
	// 取未截断节点日志里的真实 term（leader 的日志已截到 96，查不到）。
	a1 := xApplied - uint64(8)
	a1Term, err := other.groups[0].mem.Term(a1)
	if err != nil {
		t.Fatalf("锚点位 %d 的 term 不可查: %v", a1, err)
	}
	stX, err := store.Open(h.dirs[vi], false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	rsX := newRaftStore(stX, testSlog(t))
	aidx, atm := a1, a1Term
	if err := rsX.SaveSnapMeta(0, &raftpb.SnapshotMetadata{
		Index: &aidx, Term: &atm, ConfState: victim.groups[0].confState.Load(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rsX.TruncateLog(0, a1); err != nil {
		t.Fatal(err)
	}
	if err := stX.Close(); err != nil {
		t.Fatal(err)
	}

	// 换人：杀掉旧 leader（X 停机期间集群无 quorum，X 一重启即可重选）。
	// 这样处理 X 追平的是新 leader——C1 的缺口正发生在「换人后」。
	leader.kill()
	select {
	case <-leader.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("旧 leader 未在 10s 内完全退出")
	}

	// 原地干净重启 X
	x := h.startNode(t, vi)

	// 核心断言（修复的判定面）：重启瞬间日志必须全空——NewManager 返回
	// 后、Start 之前检查：buildGroup 已完成清空，run 循环还没起，无竞态
	hs, ents, snapMeta, err := x.rs.Load(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 || snapMeta != nil || !raft.IsEmptyHardState(hs) {
		t.Fatalf("安装中标记重启后日志应全空: ents=%d 锚点存在=%v HardState空=%v",
			len(ents), snapMeta != nil, raft.IsEmptyHardState(hs))
	}
	if ap, err := x.rs.Applied(0); err != nil || ap != 0 {
		t.Fatalf("安装中标记重启后 applied 应为 0: %d err=%v", ap, err)
	}
	if f, err := x.groups[0].stg.FirstIndex(); err != nil || f != 1 {
		t.Fatalf("安装中标记重启后 firstIndex 应为 1（日志未清空）: %d err=%v", f, err)
	}

	// 收敛断言：X 在新 leader 任期内追平到完整状态。R2_49 是停机前集群
	// 最后写入的键，它可见即全量重放/快照已完成
	waitFor(t, 60*time.Second, func() bool {
		_, ok, _ := x.Store().Get(store.TopicMetaKey("R2_49"))
		return ok
	}, "重启后的 X 未在新 leader 任期内收敛（R2_49 缺失）")
	// C1 静默丢段键：锚点之前的 R1 状态必须完整到达（修复前这些键永久缺失）
	for _, k := range []string{"R1_00", "R1_10", "R1_41", "R1_49"} {
		if _, ok, _ := x.Store().Get(store.TopicMetaKey(k)); !ok {
			t.Fatalf("C1 静默丢段：X 上缺少锚点之前的键 %s——安装中标记重启后日志未被清空", k)
		}
	}

	// 换人后的集群继续服务：新 leader 再写 R3 批，X 追平
	waitFor(t, 60*time.Second, func() bool {
		l, ok := other.Leader(0)
		return ok && l == other.nodeID
	}, "other 未在 60s 内当选为新 leader")
	for i := 0; i < 10; i++ {
		b := other.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("R3_%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := other.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatalf("新 leader 续写 R3: %v", err)
		}
		b.Close()
	}
	waitFor(t, 30*time.Second, func() bool {
		_, ok, _ := x.Store().Get(store.TopicMetaKey("R3_09"))
		return ok
	}, "X 未追平新 leader 的 R3 批")
}

// TestTruncateOnceRespectsLeaderMinMatch（I2 修复后的语义）leader 截断
// 下界的「逃生门」：最慢 follower 冻结在低位的 Match 不得再把整组日志
// 钉死——隔断（死）节点的下界约束必须被打破，截断照常推进。
//
// 两条逃生机制的叠加：
//   - 绝对上限 hardLagCap（10×retain=40）：applied≈102 时 applied-cap=
//     62 > victimMatch，上限把 victim 的约束抬到 62，仅此已保证截断
//     越过冻结 Match；
//   - 存活探测（probePeerAlive）：隔断后探测必失败（控制帧被分区侧
//     丢弃，拨号超时归 net.OpError），victim 被整体排除，upto 直取
//     applied-retain=98。
//
// 修复前本测试断言「upto ≤ minMatch」——修复后那个断言对死节点不再
// 成立（正是本批要解的问题）；活 follower 的性能纪律仍在（健康时
// Match≈applied，不压低 upto、也不触发探测，见 truncateOnce 注释）。
func TestTruncateOnceRespectsLeaderMinMatch(t *testing.T) {
	h := startThreeNodeCluster(t, withRetainEntries(4)) // cap = 10×4 = 40
	defer h.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	leader := h.leaderOf(t, 0)
	victim := h.nonLeaderOf(t, 0)

	// 先等 victim 追平选举后的位点（bootstrap 成员变更 + 新 leader 的
	// 空条目，Match≥2）再隔断：partition 瞬间若 victim 的 ack 还在路上，
	// leader 侧 Match 是未定值（0/2），测试会在冻结参照上抖动。
	waitFor(t, 30*time.Second, func() bool {
		st, _ := leader.Status(0)
		return st.Progress[victim.nodeID].Match >= 2
	}, "victim 未追平选举位点")
	h.partitionOff(victim)
	// 隔断后 leader 侧 Match 不再推进：冻结参照取分区后的首个观测值
	st0, _ := leader.Status(0)
	victimMatch := st0.Progress[victim.nodeID].Match

	for i := 0; i < 100; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("K%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	applied := leader.AppliedIndex(0)
	upto, done := leader.truncateOnce(0)
	if !done {
		t.Fatal("应执行一次截断（死节点的冻结 Match 不得钉死截断）")
	}
	// 逃生门判定：upto 越过冻结 Match。修复前 minMatch 会把截断钉在
	// Match-retain ≤ 0，done 恒 false——`done` 本身即逃生门证据。
	if upto <= victimMatch {
		t.Fatalf("截断到 %d 未越过死节点冻结 Match %d——逃生门失效", upto, victimMatch)
	}
	// 不越界的纪律仍在：任何情况下不得截到 applied-retain 之下
	if upto > applied-4 {
		t.Fatalf("截断到 %d 越过 applied-retain %d——越界截断", upto, applied-4)
	}
	// 探测排除的证据：死节点被探测出不可达，Warn 留痕
	if h.countLog(leader, "存活探测失败") < 1 {
		t.Fatal("死节点未被探测排除（Warn 日志缺失）——探测路径未生效")
	}
	h.healPartition(victim)
}

// TestTruncateOnceExcludesLearnerFromMinMatch learner 不参与截断下界：
// 永不联机的 learner（Match 恒 0）不得把整组截断钉死——learner 是
// 非投票成员，快照追齐是设计内路径（见 truncateOnce 注释的排除规则）。
// 修复前 minMatch 会把 learner 的 Match=0 计入，upto 恒 0、永不截断。
func TestTruncateOnceExcludesLearnerFromMinMatch(t *testing.T) {
	h := startThreeNodeCluster(t, withRetainEntries(2))
	defer h.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	leader := h.leaderOf(t, 0)

	// 添加永不联机的 learner（id=99 不在成员表、传输层无其地址表项：
	// 发给它的消息被静默丢弃，Match 恒 0）。
	if err := leader.ProposeConfChange(ctx, 0, raftpb.ConfChangeAddLearnerNode, 99); err != nil {
		t.Fatalf("提 AddLearner 99: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool {
		st, _ := leader.Status(0)
		pr, ok := st.Progress[99]
		return ok && pr.IsLearner
	}, "learner 99 未进入成员表")
	for i := 0; i < 10; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("L%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	upto, done := leader.truncateOnce(0)
	if !done {
		t.Fatal("learner Match=0 不应钉死截断（learner 不参与下界）")
	}
	if upto == 0 {
		t.Fatal("截断点被 learner 的 Match=0 压到 0——learner 未被排除")
	}
}

// TestTruncateOnceNoopWhenNothingToDrop 保留量之内不该动手
// （周期循环每 30s 触发一次，空转不许写盘、不许刷日志）。
func TestTruncateOnceNoopWhenNothingToDrop(t *testing.T) {
	h := startSingleNodeManager(t, withRetainEntries(10000))
	if _, done := h.m.truncateOnce(0); done {
		t.Fatal("条目数远小于保留量时不应截断")
	}
}
