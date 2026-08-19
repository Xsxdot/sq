//go:build e2e

// 进程内集群用例（batch④ Task 10）：直接操作 internal/cluster 的
// Manager 与 store，不启动 broker 进程。
//
// 职责：
//   - TestExpandStandaloneToThreeNodes：单机数据目录以单节点集群启动
//     （spec §7 升级路径），另两个空目录节点经 cluster.Join 加入——
//     靠快照追齐存量数据、leader 侧 AutoPromoteLearners 自动升 voter
//   - TestClusterTruncationKeepsLogBounded：三节点持续写入 5000 条，
//     周期截断（retain=500）把 raft 日志键数压在有界区间内，不许无界增长
//
// 边界：
//   - 进程内形态（Manager 直连）没有 gRPC broker，扩容用例 step ⑥
//     「官方 SDK 消费」退化为「200 条消息键在全部节点可见」——零丢失
//     的存储侧等价证据；SDK 消费路径由 TestThreeNodeClusterE2E 覆盖
//   - 节点监听地址按 nodeID 在进程内固定（addr 记忆化）：peers 表、
//     ListenAddr 与 seedPeers 必须引用同一地址；跨测试串行执行、
//     StopClean 后端口即释放
//   - 扩容用例的种子节点用 5s 摊布周期（harness 惯例是 200ms）：Join
//     的 PrepareJoin 轮询只认 seedPeers，组 leader 一旦被摊布转走且不
//     在种子集合里，该组永远收不齐、30s 超时——慢周期把 leadership
//     留在种子上，保证轮询窗口内全部组都由种子处理
package e2e

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/store"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// 进程内集群节点的监听地址：按 nodeID 记忆化（同一 id 多次调用返回
// 同一地址，peers 表与 ListenAddr 的一致性要求），跨测试稳定（扩容
// 用例里 addr(1) 先被种子绑定、后被 Join 的 seedPeers 引用）。
var (
	clusterAddrMu sync.Mutex
	clusterAddrs  = map[uint64]string{}
)

// addr 返回节点 id 在本测试进程内固定的监听地址（记忆化随机端口）。
func addr(t *testing.T, id uint64) string {
	t.Helper()
	clusterAddrMu.Lock()
	defer clusterAddrMu.Unlock()
	if a, ok := clusterAddrs[id]; ok {
		return a
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("节点 %d 探测监听端口: %v", id, err)
	}
	a := ln.Addr().String()
	ln.Close()
	clusterAddrs[id] = a
	return a
}

// TestExpandStandaloneToThreeNodes 单机→多节点扩容（spec §7）：
// 单机数据目录直接以单节点集群启动，另两个空目录节点靠快照追齐入组。
//
// 为什么新节点必然走快照而非日志重放（step ④ 断言注释）：单机档的
// 200 条是直写 FSM 的（当时还没有 raft），种子的 raft 日志里根本没有
// 这些条目——新节点从空日志加入，日志追齐对它毫无意义，只有快照
// （Task 4 从 leader 的活 store 现场生成）能把存量数据带到新节点。
func TestExpandStandaloneToThreeNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("扩容用例耗时长")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// ① 单机档写入 200 条，正常关停（直写 FSM，无 raft 日志）
	seedDir := t.TempDir()
	writeStandaloneMessages(t, seedDir, "EXPAND", 200)

	// ② 同一目录以单节点集群启动：raft 引导只含自己（BootstrapVoters），
	//    但传输层地址表是**全量**成员（fullPeers）——后加入的节点靠
	//    种子回拨收快照，地址表不全时发给它们的消息会被传输层静默丢弃
	//    （扩容 e2e 抓到的根因）；FSM 存量原样接管。种子日志保留量压到
	//    1（快照兜底的前提，见 ②b 注释）
	fullPeers := map[uint64]string{1: addr(t, 1), 2: addr(t, 2), 3: addr(t, 3)}
	seed := startClusterNode(t, seedDir, 1, fullPeers, withRetainEntries(1), withTruncateInterval(time.Second))
	nodes := []*cluster.Manager{seed}
	waitForMsg(t, 30*time.Second, func() bool { return seed.IsLeader(0) }, "种子节点未当选")

	// ②b 逼出新节点的快照路径：raft 的日志追齐探针锚在「假想条目
	//    index-1」（未压缩日志的 term(0)=0，与空日志 follower 恰好匹配）
	//    ——日志未压缩时新节点走**日志重放**，而单机档的 200 条根本不在
	//    日志里（当时直写 FSM），重放带不过去、快照也永不触发。只有把
	//    种子日志压缩到新节点起点之下（term(0) 报 ErrCompacted），raft
	//    才判定「日志追齐不可能，只能 MsgSnap」。做法：每组合计提一条
	//    marker（applied 越过 2×retain 后截断循环才动手，retain=1 需要
	//    applied≥3），等日志被压到保留量再开始 Join。
	//
	//    观测面迁移：raft 日志条目迁到 seglog 后（<data_dir>/raftlog/<g>/
	//    段文件），Pebble 里的 raft/0/ent/* 键从未存在过——旧 gate
	//    「countEntKeys(...) <= 2」在这个仓库形态下从 t=0 起恒为真
	//    （count 恒为 0），Join 会在截断循环真正跑起来之前就抢跑，正是
	//    本次要修的 e2e 回归根因。改观测锚点 raft/0/snap（SnapshotMetadata
	//    protobuf，SaveSnapMeta 写入，且只由截断/快照路径写）：它的出现
	//    就是 truncateOnceWith 已经推进到 SaveSnapMeta 这一步的直接证据，
	//    与旧观测点是同一竞态剖面——旧 gate 依赖的「ent 键被删」同样发生
	//    在 SaveSnapMeta 之后、mem.Compact 之前的微秒级窗口里（TruncateLog
	//    紧跟 SaveSnapMeta），新 gate 只是把「删除」换成了「锚点落盘」
	//    这个更早、但因果顺序相同的信号。Index>=1 顺带确认截断点已经
	//    越过了 bootstrap 空条目，不是残留的零值。
	seedMarkers(t, ctx, seed, 4)
	waitForMsg(t, 30*time.Second, func() bool {
		meta, ok := groupSnapMeta(t, seed, 0)
		return ok && meta.GetIndex() >= 1
	}, "种子日志未在 30s 内被截断压缩（快照触发的前提不成立）")

	// ③ 两个空目录节点 Join。节点 3 的种子把已加入的节点 2 也带上：
	//    PrepareJoin 由「各组的 leader 处理自己 lead 的组」收并集，若
	//    组 leader 被摊布转出种子集合，该组永远等不到完成——种子覆盖
	//    全部组的 leader 是 Join 的调用方职责（见文件头边界注释）。
	join := func(id uint64, seeds map[uint64]string) {
		n, err := cluster.Join(ctx, nodeOptions(t, t.TempDir(), id, fullPeers), seeds)
		if err != nil {
			t.Fatalf("节点 %d 加入失败: %v", id, err)
		}
		nodes = append(nodes, n)
		t.Cleanup(func() { _ = n.StopClean(context.Background()) })
	}
	join(2, map[uint64]string{1: addr(t, 1)})
	join(3, map[uint64]string{1: addr(t, 1), 2: addr(t, 2)})

	// ④ 全部数据在新节点上可见——走的是快照追齐，不是日志重放：
	//    种子的日志里没有单机导入那一段（当时直写 FSM），新节点从零
	//    开始必然需要快照；TopicMetaKey 归组 0（全局键族），组 0 的快照
	//    带着它装进新节点即「升级数据完整迁移」的证据。
	waitForMsg(t, 120*time.Second, func() bool {
		for _, n := range nodes {
			if _, ok, _ := n.Store().Get(store.TopicMetaKey("EXPAND")); !ok {
				return false
			}
		}
		return true
	}, "新节点未在 120s 内追平（快照未把存量数据带到新节点）")

	// ⑤ 新节点最终升为 voter（leader 侧 AutoPromoteLearners 循环）
	waitForMsg(t, 60*time.Second, func() bool {
		st, ok := seed.Status(0)
		return ok && len(st.Config.Voters[0]) == 3
	}, "learner 未自动升为 voter")

	// ⑥ 消息零丢失：进程内形态无 gRPC broker，SDK 消费退化为「200 条
	//    消息键在三节点全部可见」的存储侧等价证据（SDK 消费路径由
	//    TestThreeNodeClusterE2E 覆盖——那里 270 条全历史对账无丢失）
	assertConsumeAll(t, nodes, "EXPAND", 200)
}

// TestClusterTruncationKeepsLogBounded e2e 截断证据：
// 持续写入下 raft 日志键数必须被压住，不许无界增长。
func TestClusterTruncationKeepsLogBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("长跑用例")
	}
	h := startThreeNodeE2E(t, withRetainEntries(500), withTruncateInterval(2*time.Second))
	defer h.stopAll()
	sendClusterMessages(t, h, 5000)
	// 观测面迁移：旧断言数 raft/0/ent/* 键，seglog 落地后该前缀在 Pebble
	// 里从未出现过（日志条目迁到 <data_dir>/raftlog/<g>/ 段文件），count
	// 恒为 0——「< 3000」不论截断循环是否真的在跑都恒真，测试退化成
	// no-op。改观测截断锚点 raft/0/snap 的 Index：锚点只在 truncateOnceWith
	// 推进保留窗口时前移，若 5000 条写入后锚点仍停在 5000-3000=2000 之下，
	// 说明截断循环没把日志压进 retain=500 的窗口——不可能在日志真的有界
	// 时都推不过 2000，这条边界比 retain=500 松得多，留了与原断言同等的
	// 6x 余量防慢机器抖动。（本模块没有解码 seglog 段文件的依赖，锚点
	// Index 是目前能拿到的、证明力最强的逻辑日志观测面）
	waitForMsg(t, 60*time.Second, func() bool {
		meta, ok := groupSnapMeta(t, h.nodes[0], 0)
		return ok && meta.GetIndex() > 5000-3000
	}, "raft 日志未被截断压缩（快照锚点未推进过界，日志可能无界增长）")
}

// writeStandaloneMessages 以单机档（无 raft）直写 FSM：topic 元数据 +
// n 条消息键（3 个队列轮转——消息键经哈希散布到全部数据组，快照路径
// 的证据覆盖到每个组），Sync 落盘后正常关停。
func writeStandaloneMessages(t *testing.T, dir, topic string, n int) {
	t.Helper()
	st, err := store.Open(dir, false, testSlog())
	if err != nil {
		t.Fatalf("单机档开 store: %v", err)
	}
	b := st.NewBatch()
	if err := b.Set(store.TopicMetaKey(topic), []byte("v")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := b.Set(store.MsgKey(topic, uint32(i%3), uint64(i/3)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("单机档关停: %v", err)
	}
}

// startClusterNode 以单节点集群启动：store.Open + NewManager + Start。
// peers 是**全量**成员表（传输层地址表，后加入的节点靠它回拨收快照），
// raft 引导只含自己（BootstrapVoters——多成员引导的 quorum 不可达，
// 单节点永远选不出 leader）。opts 为 Options 级注入（扩容用例注入
// withRetainEntries(1) + withTruncateInterval——日志压缩是快照触发的
// 前提，见 TestExpandStandaloneToThreeNodes ②b）。5s 摊布周期 +
// AutoPromoteLearners（见文件头边界注释）。清理逆序：StopClean → 关 store。
func startClusterNode(t *testing.T, dir string, id uint64, peers map[uint64]string, opts ...clusterOpt) *cluster.Manager {
	t.Helper()
	st, err := store.Open(dir, false, testSlog())
	if err != nil {
		t.Fatalf("节点 %d 开 store: %v", id, err)
	}
	o := cluster.Options{
		NodeID:                 id,
		Peers:                  peers,
		ListenAddr:             addr(t, id),
		BootstrapVoters:        []uint64{id},
		DataGroups:             3,
		Mode:                   cluster.AckQuorumMem,
		Store:                  st,
		Logger:                 harnessSlog(t),
		LeaderBalancerInterval: 5 * time.Second,
		AutoPromoteLearners:    true,
	}
	for _, fn := range opts {
		fn(&o)
	}
	m, err := cluster.NewManager(o)
	if err != nil {
		st.Close()
		t.Fatalf("节点 %d NewManager: %v", id, err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		if err := m.StopClean(context.Background()); err != nil {
			t.Logf("种子节点清理 StopClean: %v", err)
		}
		_ = st.Close()
	})
	return m
}

// nodeOptions 构造 Join 用的 Options：数据目录先开 store（Join 签名
// 不带 dataDir，目录句柄经 Options.Store 传入——与 Rejoin「调用方
// 持句柄」的职责一致），监听按 addr(id) 绑定。
func nodeOptions(t *testing.T, dir string, id uint64, peers map[uint64]string) cluster.Options {
	t.Helper()
	st, err := store.Open(dir, false, testSlog())
	if err != nil {
		t.Fatalf("节点 %d 开 store: %v", id, err)
	}
	// 清理注册在 Join 之前：LIFO 下后注册的 StopClean 先执行，顺序
	// 恒为 StopClean → 关 store；Join 失败（t.Fatalf）时同样兜底
	t.Cleanup(func() { _ = st.Close() })
	return cluster.Options{
		NodeID:                 id,
		Peers:                  peers,
		ListenAddr:             addr(t, id),
		DataGroups:             3,
		Mode:                   cluster.AckQuorumMem,
		Store:                  st,
		Logger:                 harnessSlog(t),
		LeaderBalancerInterval: 5 * time.Second,
		AutoPromoteLearners:    true,
	}
}

// assertConsumeAll 断言 n 条消息键在全部节点可见——进程内形态的
// 「零丢失」存储侧证据（无 gRPC broker，SDK 消费不可行；SDK 消费
// 路径由 TestThreeNodeClusterE2E 覆盖）。超时逐节点列出缺失数。
func assertConsumeAll(t *testing.T, nodes []*cluster.Manager, topic string, n int) {
	t.Helper()
	key := func(i int) []byte { return store.MsgKey(topic, uint32(i%3), uint64(i/3)) }
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, m := range nodes {
			for i := 0; i < n; i++ {
				if _, ok, _ := m.Store().Get(key(i)); !ok {
					all = false
					break
				}
			}
			if !all {
				break
			}
		}
		if all {
			t.Logf("全部 %d 个节点可见 %d 条消息键（零丢失，存储侧证据）", len(nodes), n)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	for j, m := range nodes {
		missing := 0
		for i := 0; i < n; i++ {
			if _, ok, _ := m.Store().Get(key(i)); !ok {
				missing++
			}
		}
		t.Errorf("节点 %d 缺失 %d/%d 条消息键", j+1, missing, n)
	}
	t.Fatal("消息键未在全部节点追平（快照携带数据不完整）")
}

// harnessSlog 构造写入 t.Log 的 slog（供进程内 harness 的节点使用：
// 失败时快照安装/截断等关键日志可查——testSlog 的 io.Discard 在排障
// 时什么都看不见）。
func harnessSlog(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testLogWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// testLogWriter 把 slog 记录逐行转发给 t.Log。
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// seedMarkers 在种子各组合计提一条 marker（每个提案一条 raft 日志条目）：
// applied 越过 2×retain 后截断循环才动手（truncateOnce 的保留量守卫），
// 种子单独运行 applied=2（引导 + 当选空条目）时永远截不了——marker 是
// 把日志推到可截断位点之上的最小手段。组号 0..g-1 各一条。
func seedMarkers(t *testing.T, ctx context.Context, seed *cluster.Manager, groups uint32) {
	t.Helper()
	for g := uint32(0); g < groups; g++ {
		for {
			b := seed.Store().NewBatch()
			if err := b.Set(store.TopicMetaKey(fmt.Sprintf("SEED-MARK-%d", g)), []byte("v")); err != nil {
				t.Fatal(err)
			}
			repr := append([]byte(nil), b.Repr()...)
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			if err := seed.Propose(ctx, g, repr); err == nil {
				break
			} else if errors.Is(err, cluster.ErrNotLeader) {
				time.Sleep(100 * time.Millisecond) // 各组选举错峰，等本组 leader 就位
				continue
			} else {
				t.Fatalf("组 %d marker 提案: %v", g, err)
			}
		}
	}
}

// groupSnapMeta 读取并解码指定 manager 上组 g 的快照锚点
// （raft/<g>/snap，SnapshotMetadata protobuf）——seglog 落地后日志条目
// 迁出 Pebble（迁到 <data_dir>/raftlog/<g>/ 段文件），锚点是 e2e 测试
// 能在这个仓库形态下观测「截断/快照路径已经跑过」的唯一存留观测面
// （原 countEntKeys 扫的 raft/<g>/ent/* 前缀恒空，已删除）。锚点只由
// SaveSnapMeta 写（截断循环 truncateOnceWith 或安装快照两条路径），
// 不存在返回 ok=false。
func groupSnapMeta(t *testing.T, m *cluster.Manager, g uint32) (*raftpb.SnapshotMetadata, bool) {
	t.Helper()
	data, ok, err := m.Store().Get([]byte(fmt.Sprintf("raft/%d/snap", g)))
	if err != nil {
		t.Fatalf("读组 %d 快照锚点: %v", g, err)
	}
	if !ok {
		return nil, false
	}
	meta := &raftpb.SnapshotMetadata{}
	if err := proto.Unmarshal(data, meta); err != nil {
		t.Fatalf("解码组 %d 快照锚点: %v", g, err)
	}
	return meta, true
}

// clusterOpt 是进程内集群 harness 的 Options 级注入点。
type clusterOpt func(*cluster.Options)

// withRetainEntries 注入截断循环的日志保留量（Options.RetainEntries）。
func withRetainEntries(n uint64) clusterOpt {
	return func(o *cluster.Options) { o.RetainEntries = n }
}

// withTruncateInterval 注入周期截断的执行间隔（Options.TruncateInterval）。
func withTruncateInterval(d time.Duration) clusterOpt {
	return func(o *cluster.Options) { o.TruncateInterval = d }
}

// e2eCluster 是进程内三节点集群 harness（截断用例）：nodes[i] 即节点
// i+1，经 127.0.0.1 随机端口真实 TCP 互联。不注入摊布/自动升 voter
// （截断用例不需要）。
type e2eCluster struct {
	nodes  []*cluster.Manager
	stores []*store.Store
}

// startThreeNodeE2E 起三节点进程内 harness。装配序与 cluster 单元
// harness 相同：先预建监听收集地址再拼 Peers 表（传输层按 Peers 拨号，
// 必须先拿到全部地址），然后逐节点 store.Open + NewManager（注入已
// 建监听）+ Start。清理经 t.Cleanup：StopClean → 等 Done → 关 store。
func startThreeNodeE2E(t *testing.T, opts ...clusterOpt) *e2eCluster {
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
	h := &e2eCluster{}
	for i := uint64(1); i <= n; i++ {
		st, err := store.Open(fmt.Sprintf("%s/%d", t.TempDir(), i), false, testSlog())
		if err != nil {
			t.Fatalf("节点 %d 开 store: %v", i, err)
		}
		o := cluster.Options{
			NodeID:     i,
			Peers:      peers,
			Listener:   lstns[i-1],
			DataGroups: 3,
			Mode:       cluster.AckQuorumMem,
			Store:      st,
			Logger:     harnessSlog(t),
		}
		for _, fn := range opts {
			fn(&o)
		}
		m, err := cluster.NewManager(o)
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
		for _, st := range h.stores {
			if err := st.Close(); err != nil {
				t.Logf("清理: store.Close: %v", err)
			}
		}
	})
	return h
}

// stopAll 显式停掉全部节点（StopClean）。幂等：t.Cleanup 兜底会再调
// 一次（cancel 幂等、done 通道已关、干净标记重复写无害）。
func (h *e2eCluster) stopAll() {
	for _, m := range h.nodes {
		_ = m.StopClean(context.Background())
	}
}

// leaderOf 返回组 g 当前 leader 的 Manager，带 WaitLeader 轮询语义
// （任何节点自报 lead 且 lead 在 1..n 即作数，超时 Fatal）。
func (h *e2eCluster) leaderOf(t *testing.T, g uint32) *cluster.Manager {
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

// sendMessages 经组 0 的 leader 连续提案 n 个批次（每批一条消息键），
// 每个提案阻塞到本节点 apply（读己之写语义）；leader 换手（ErrNotLeader）
// 时重找 leader 重试。整体 180s 时限。
func sendClusterMessages(t *testing.T, h *e2eCluster, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	leader := h.leaderOf(t, 0)
	start := time.Now()
	for i := 0; i < n; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.MsgKey("TRUNC", 0, uint64(i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		// 拷贝后再回收批次：repr 底层内存归批次所有（store.Batch.Repr
		// 注释），raft 库会长期持有日志条目
		repr := append([]byte(nil), b.Repr()...)
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		for {
			if err := leader.Propose(ctx, 0, repr); err == nil {
				break
			} else if errors.Is(err, cluster.ErrNotLeader) {
				leader = h.leaderOf(t, 0)
				continue
			} else {
				t.Fatalf("提案 #%d: %v", i, err)
			}
		}
		if (i+1)%1000 == 0 {
			t.Logf("已写入 %d/%d（%.0f msg/s）", i+1, n, float64(i+1)/time.Since(start).Seconds())
		}
	}
	t.Logf("写入完成 %d 条，耗时 %v（%.0f msg/s）", n, time.Since(start).Round(time.Millisecond), float64(n)/time.Since(start).Seconds())
}

// waitForMsg 轮询等待条件成立，超时 Fatal（带描述；与 cluster 单元
// harness 同款。console_test.go 已有三参 waitFor，故用 Msg 后缀区分）。
func waitForMsg(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
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
