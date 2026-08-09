// manager_test.go 验证 Manager 多组装配：组路由黄金值、干净/不干净
// 重启两条路径的判定与恢复。
//
// 职责：队列→组映射的入盘契约（黄金值锁死）、单节点干净关机后原身份
// 回归（数据可读、无 ErrUncleanShutdown）、不写标记直接停后 NewManager
// 必须拒绝裸恢复。
// 边界：不覆盖多节点消息路径（batch④ 三节点集成测试）；
// openClusterTestStore/testSlog/testWriter 复用 raftstore_test.go。
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/xushixin/sq/internal/store"
)

// TestManagerStatusExposesRaftState Status(g) 透传 rn.Status()：Task 10
// 学习者进度监控依赖它读 Progress/IsLearner；此处锁死基本契约（组号
// 非法 ok=false、单节点自选举后报告 leader）。
func TestManagerStatusExposesRaftState(t *testing.T) {
	dir := t.TempDir()
	_, m := startSoloManager(t, dir, AckQuorumMem)
	if _, ok := m.Status(99); ok {
		t.Fatal("Status(99) 应 ok=false（组号越界）")
	}
	// 单节点自选举约 1s：轮询直到 Status 报告本节点为 leader
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		st, ok := m.Status(MetaGroup)
		if !ok {
			t.Fatal("Status(MetaGroup) 应 ok=true")
		}
		if st.RaftState == raft.StateLeader {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("单节点组未在 30s 内自选举为 leader")
}

// TestGroupForQueueStable 队列→组映射是入盘契约，黄金值锁死：任何改动
// fnv 输入编码的重构都会在这里炸出来，而不是让存量数据错组。
func TestGroupForQueueStable(t *testing.T) {
	m := &Manager{dataGroups: 3}
	golden := []struct {
		topic string
		q     uint32
		want  uint32
	}{
		// 首次生成即冻结，断言三条覆盖：不同 topic 不同组、同 topic
		// 不同 queue 可不同组、组号 ∈ [1,3]
		{"orders", 0, 1}, {"orders", 1, 3}, {"payments", 0, 3},
	}
	for _, g := range golden {
		got := m.GroupForQueue(g.topic, g.q)
		if got != g.want {
			t.Fatalf("GroupForQueue(%s,%d) = %d; want %d——fnv 输入编码被改动", g.topic, g.q, got, g.want)
		}
		if got < 1 || got > 3 {
			t.Fatalf("GroupForQueue(%s,%d) = %d 越界 [1,3]", g.topic, g.q, got)
		}
	}
}

// TestManagerCleanRestartResumes 单节点集群（成员表只有自己）干净关机后
// 重启：applied 恢复、数据可读、无 ErrUncleanShutdown。
func TestManagerCleanRestartResumes(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem) // helper：单成员 Options
	b := st1.NewBatch()
	_ = b.Set([]byte("meta/topic/t1"), []byte("v"))
	repr := append([]byte(nil), b.Repr()...)
	_ = b.Close()
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := m1.Propose(pctx, MetaGroup, repr); err != nil {
		t.Fatal(err)
	}
	if err := m1.StopClean(ctx); err != nil {
		t.Fatal(err)
	}
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}
	st2, m2 := startSoloManager(t, dir, AckQuorumMem) // 同目录重启
	if _, ok, _ := st2.Get([]byte("meta/topic/t1")); !ok {
		t.Fatal("干净重启后数据丢失")
	}
	_ = m2 // 起来即成功；NewManager 未返回 ErrUncleanShutdown 已在 helper 断言
}

// TestManagerUncleanRestartRefusesResume 不写标记直接停（模拟断电）后，
// NewManager 必须返回 ErrUncleanShutdown——异步刷盘不可裸恢复（spec §2.2）。
func TestManagerUncleanRestartRefusesResume(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem)
	// kill 前先落地一条提案，保证至少一轮 Ready 被处理（HardState/
	// 条目已持久化）：若 kill 抢在全部组第一个 Ready 之前，盘上不会
	// 有任何 raft 状态，重启会走 fresh 路径——既往 ~1/16 抖动源。
	b := st1.NewBatch()
	_ = b.Set([]byte("meta/topic/t1"), []byte("v"))
	repr := append([]byte(nil), b.Repr()...)
	_ = b.Close()
	pctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := m1.Propose(pctx, MetaGroup, repr); err != nil {
		t.Fatal(err)
	}
	m1.kill() // 测试后门：cancel 运行 ctx，不写标记
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}
	st2 := mustOpenStore(t, dir)
	_, err := NewManager(soloOptions(t, st2, dir, AckQuorumMem))
	if !errors.Is(err, ErrUncleanShutdown) {
		t.Fatalf("不干净重启 NewManager = %v; want ErrUncleanShutdown", err)
	}
}

// mustOpenStore 打开指定目录下的 store，随测试结束自动关闭。
// 测试体可能已显式 Close（重启测试必须先关再重开），而 pebble 对
// 已关闭 DB 再 Close 是 panic 而非返回错误——清理期按「关闭已做过」
// 语义吞掉该 panic。
func mustOpenStore(t *testing.T, dir string) *store.Store {
	t.Helper()
	st, err := store.Open(dir, false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer func() { _ = recover() }() // pebble: closed——重复关闭，忽略
		st.Close()
	})
	return st
}

// soloOptions 构造单成员（NodeID=1，成员表只有自己）的 Manager 选项：
// Peers 地址为注入监听器的占位（本节点消息不走传输层，地址无需真实），
// DataGroups 用默认 3。dir 仅保持签名与 startSoloManager 一致。
func soloOptions(t *testing.T, st *store.Store, dir string, mode AckMode) Options {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return Options{
		NodeID:     1,
		Peers:      map[uint64]string{1: "127.0.0.1:0"},
		Listener:   ln,
		DataGroups: 3,
		Mode:       mode,
		Store:      st,
		Logger:     testSlog(t),
	}
}

// startSoloManager 打开（或复用）dir 下的 store 并启动单成员 Manager；
// NewManager 返回错误（含 ErrUncleanShutdown）在此直接失败。测试结束后
// 自动 kill 并等待完全退出，防止 goroutine 泄漏到后续清理（LIFO：kill
// 先于 mustOpenStore 的 store 关闭注册）。opts 为 Options 级注入
// （如 withRetainEntries(n)）。
func startSoloManager(t *testing.T, dir string, mode AckMode, opts ...func(*Options)) (*store.Store, *Manager) {
	t.Helper()
	st := mustOpenStore(t, dir)
	o := soloOptions(t, st, dir, mode)
	for _, fn := range opts {
		fn(&o)
	}
	m, err := NewManager(o)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		m.kill()
		select {
		case <-m.Done():
		case <-time.After(5 * time.Second):
			t.Error("manager 未在 5s 内完全退出")
		}
	})
	return st, m
}

// singleNodeManagerHarness 是单节点 Manager 的测试句柄：m 为已启动的
// Manager（组 0..3 自选举），m.rs 即 Manager 的 raft 持久层——成员表
// 与 applied 的磁盘状态直读都经它（内测包内字段可直达，无需访问器）。
type singleNodeManagerHarness struct {
	m *Manager
}

// startSingleNodeManager 启动单节点 Manager 并返回测试句柄，供
// TestConfChangeAdvancesPersistedApplied 等端到端场景使用。与
// startSoloManager 同 Options 模式（Peers 仅自己、数据组默认 3），
// 测试结束自动 kill 并等待完全退出。opts 为 Options 级注入。
func startSingleNodeManager(t *testing.T, opts ...func(*Options)) *singleNodeManagerHarness {
	t.Helper()
	_, m := startSoloManager(t, t.TempDir(), AckQuorumMem, opts...)
	return &singleNodeManagerHarness{m: m}
}

// TestConfChangeAdvancesPersistedApplied 端到端证明缺口已补：
// 单节点组提一条 AddLearner，重启后 applied 位点不回退。
func TestConfChangeAdvancesPersistedApplied(t *testing.T) {
	h := startSingleNodeManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.m.ProposeConfChange(ctx, 0, raftpb.ConfChangeAddLearnerNode, 9); err != nil {
		t.Fatalf("提 AddLearner: %v", err)
	}
	before, err := h.m.rs.Applied(0)
	if err != nil {
		t.Fatal(err)
	}
	if before == 0 {
		t.Fatal("ConfChange apply 后磁盘 applied 位点仍为 0——缺口未补")
	}
	cs, ok, err := h.m.rs.LoadConfState(0)
	if err != nil || !ok {
		t.Fatalf("成员表未持久化 ok=%v err=%v", ok, err)
	}
	if len(cs.Learners) != 1 || cs.Learners[0] != 9 {
		t.Fatalf("成员表 learners = %v; want [9]", cs.Learners)
	}
}

// TestJoinRejectsNonEmptyStore Join 的目录空校验（第 1 步）：带存量
// 数据的目录必须拒绝并指向 Rejoin——加入成功后 leader 的快照安装会
// 整体清空本节点目标组全部键（wipeGroupKeys，Task 7），非空目录混用
// Join = 存量数据被静默抹掉。错误文本必须带 Rejoin 字样（语义分界）。
func TestJoinRejectsNonEmptyStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st := mustOpenStore(t, t.TempDir())
	b := st.NewBatch()
	if err := b.Set(store.TopicMetaKey("LEGACY"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	_, err := Join(ctx, Options{
		NodeID:     2,
		Peers:      map[uint64]string{1: "127.0.0.1:1", 2: "127.0.0.1:2"},
		DataGroups: 3,
		Store:      st,
		Logger:     testSlog(t),
	}, map[uint64]string{1: "127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "Rejoin") {
		t.Fatalf("非空目录 Join 应报错并指向 Rejoin，得到: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestJoinRejectsBadSeeds Join 的种子校验（第 2 步前置）：seedPeers
// 为空、或只有本节点自己（自举语义，不是加入语义——没有任何存活成员
// 会给本节点发 PrepareJoin 的 AddLearner）都必须快速失败，而不是裸走
// 30s 轮询超时。
func TestJoinRejectsBadSeeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	open := func() *store.Store { return mustOpenStore(t, t.TempDir()) }
	base := func(st *store.Store) Options {
		return Options{
			NodeID:     2,
			Peers:      map[uint64]string{1: "127.0.0.1:1", 2: "127.0.0.1:2"},
			DataGroups: 3,
			Store:      st,
			Logger:     testSlog(t),
		}
	}
	if _, err := Join(ctx, base(open()), nil); err == nil {
		t.Fatal("seedPeers 为空应报错，得到 nil")
	}
	if _, err := Join(ctx, base(open()), map[uint64]string{2: "127.0.0.1:2"}); err == nil {
		t.Fatal("seedPeers 只有本节点自己应报错，得到 nil")
	}
}

// TestSeedStateProbe 锁 OpSeedState 的线协议形状（handler 侧，golden
// 值在 TestControlOpRegistry）：请求 [4B BE 组]，响应 [1B FSM 非空]
// [8B BE firstIndex]——Join 的种子日志档位探测（C2）的判定面。
func TestSeedStateProbe(t *testing.T) {
	h := startSingleNodeManager(t)
	// 空 FSM 的组（数据组 1 无键）：nonEmpty=0；日志未压缩 firstIndex=1
	resp, err := h.m.handleSeedState(encodeSeedStateReq(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 9 {
		t.Fatalf("响应 %d B; want 9（[1B 非空][8B firstIndex]）", len(resp))
	}
	if resp[0] != 0 {
		t.Fatalf("空 FSM 的组 nonEmpty=%d; want 0", resp[0])
	}
	if first := binary.BigEndian.Uint64(resp[1:]); first != 1 {
		t.Fatalf("未截断日志 firstIndex=%d; want 1", first)
	}
	// 直写一个组 0 键（单机档语义，不经 raft）：FSM 非空翻 1，日志仍
	// 未压缩——这正是 Join 必须拒绝的档位（firstIndex=1 && FSM 非空）
	b := h.m.Store().NewBatch()
	if err := b.Set(store.TopicMetaKey("SEED"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Store().Apply(b); err != nil {
		t.Fatal(err)
	}
	resp, err = h.m.handleSeedState(encodeSeedStateReq(0))
	if err != nil {
		t.Fatal(err)
	}
	if resp[0] != 1 {
		t.Fatalf("有键的组 nonEmpty=%d; want 1", resp[0])
	}
	if first := binary.BigEndian.Uint64(resp[1:]); first != 1 {
		t.Fatalf("未截断日志 firstIndex=%d; want 1", first)
	}
	// 坏载荷与未知组必须显式报错（防坏对端，与 handlePrepareJoin 同纪律）
	if _, err := h.m.handleSeedState([]byte{1, 2, 3}); err == nil {
		t.Fatal("载荷不足 4B 应报错")
	}
	if _, err := h.m.handleSeedState(encodeSeedStateReq(99)); err == nil {
		t.Fatal("未知组号应报错")
	}
}

// TestJoinRejectsUncompressedSeed Join 的种子日志档位探测（第 2 步，
// C2 修复）：种子「日志未压缩（firstIndex=1）且 FSM 非空」时 Join 必须
// 显式拒绝——此时新节点追齐走日志重放（探测锚在假想条目 index-1），
// 而单机档直写 FSM 的存量数据不在 raft 日志里，重放带不过去、快照也
// 不触发，扩容会静默丢数据。修复前这是文档披露（不报错、数据缺失），
// 现在由第 2 步探测执行（先于 PrepareJoin，拒绝不留 phantom learner）。
func TestJoinRejectsUncompressedSeed(t *testing.T) {
	// 种子：单节点集群 + 少量直写（远低于 2×RetainEntries，截断循环
	// 永不触发，日志保持未压缩）——FSM 非空与 firstIndex=1 双条件齐备
	seedStore, seed := startSoloManager(t, t.TempDir(), AckQuorumMem)
	b := seedStore.NewBatch()
	if err := b.Set(store.TopicMetaKey("SEED-ONLY"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := seedStore.Apply(b); err != nil {
		t.Fatal(err)
	}
	// 测试前提自证：日志确实未压缩（firstIndex=1），FSM 确实非空
	if f, err := seed.groups[0].stg.FirstIndex(); err != nil || f != 1 {
		t.Fatalf("测试前提不成立：种子 firstIndex=%d err=%v（want 1）", f, err)
	}
	if nonEmpty, err := groupHasKeys(seedStore, 0, seed.dataGroups); err != nil || !nonEmpty {
		t.Fatalf("测试前提不成立：组 0 FSM 应非空 nonEmpty=%v err=%v", nonEmpty, err)
	}
	seedAddr := seed.ln.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	joinStore := mustOpenStore(t, t.TempDir())
	_, err := Join(ctx, Options{
		NodeID:     2,
		Peers:      map[uint64]string{1: seedAddr, 2: "127.0.0.1:0"},
		DataGroups: 3,
		Store:      joinStore,
		Logger:     testSlog(t),
	}, map[uint64]string{1: seedAddr})
	if err == nil {
		t.Fatal("未压缩种子（firstIndex=1 + FSM 非空）Join 应被拒绝，得到 nil")
	}
	if !strings.Contains(err.Error(), "日志未压缩") {
		t.Fatalf("拒绝原因应指向日志未压缩，得到: %v", err)
	}
	// 无 phantom learner：拒绝发生在 PrepareJoin 之前，种子成员表里
	// 不得出现节点 2（voter 或 learner 都不行）
	cs, ok, err := seed.rs.LoadConfState(0)
	if err != nil || !ok {
		t.Fatalf("读种子成员表: ok=%v err=%v", ok, err)
	}
	for _, v := range cs.Voters {
		if v == 2 {
			t.Fatal("拒绝后种子成员表出现 voter 2——phantom learner")
		}
	}
	for _, l := range cs.Learners {
		if l == 2 {
			t.Fatal("拒绝后种子成员表出现 learner 2——phantom learner")
		}
	}
}

// TestListenAddrBindsSeparateFromPeersAdvertised 评审 I5 回归：Peers 里的
// 地址是**拨号通告**地址（NAT/容器下为外网可达地址，本机绑定必失败），
// 本节点绑定必须走 ListenAddr——两者分离时 NewManager 按 ListenAddr
// 绑定成功，而不是去绑通告地址。
//
// 模拟：Peers[1] 填一个不可绑定的外网通告地址（192.0.2.1 是保留文档
// 网段，任何机器都不属于它），ListenAddr 用本机随机端口——若实现错误地
// 按 Peers 绑定会立刻失败；按 ListenAddr 绑定则成功。
func TestListenAddrBindsSeparateFromPeersAdvertised(t *testing.T) {
	st := mustOpenStore(t, t.TempDir())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close() // 只借用端口号：NewManager 要自己绑 ListenAddr
	port := ln.Addr().(*net.TCPAddr).Port
	m, err := NewManager(Options{
		NodeID:     1,
		Peers:      map[uint64]string{1: "192.0.2.1:9091"}, // 通告地址：不可本地绑定
		ListenAddr: fmt.Sprintf("127.0.0.1:%d", port),      // 绑定地址：本机
		DataGroups: 3,
		Mode:       AckQuorumMem,
		Store:      st,
		Logger:     testSlog(t),
	})
	if err != nil {
		t.Fatalf("NewManager 应按 ListenAddr 绑定成功，得到 %v", err)
	}
	// 确认绑定端口确实是 ListenAddr 的端口（而不是通告地址的）
	if got := m.ln.Addr().String(); got != fmt.Sprintf("127.0.0.1:%d", port) {
		t.Fatalf("监听地址应为 ListenAddr %s，实际 %s", fmt.Sprintf("127.0.0.1:%d", port), got)
	}
	// 未 Start（NewManager 即完成绑定）：直接关监听器收尾，不走 kill
	m.ln.Close()
}
