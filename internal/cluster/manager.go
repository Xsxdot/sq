// manager.go 提供多组装配层：全部 raft 组（MetaGroup + 数据组）共享
// 一个 store 库与一条 TCP 传输链路的组装、组路由、生命周期与重启恢复
// 判定。
//
// 职责：
//   - 组装配：全组创建——fresh 路径 StartNode 引导 / 干净关机路径
//     RestartNode 原身份回归（磁盘日志回放进 MemoryStorage）
//   - 组路由：队列→组映射（入盘契约，GroupForQueue）与传输消息按组投递
//   - 生命周期：Start 起全部组 + 传输层 + mem 档后台批量刷盘 + leader
//     摊布循环（Options 注入周期，≤0 不启动）；StopClean 停全部机件后
//     最后写干净关机标记；Done 等完全退出
//   - 重启恢复判定：干净关机标记决定 fresh / 原身份回归 / 拒绝裸恢复
//     （ErrUncleanShutdown，须清空状态以 learner 重入）
//   - learner 重入编排（batch③）：Rejoin 六步入口（关店清目录→
//     PrepareJoin 全组→重建监听→fresh 启动→追平升 voter 交给 leader
//     侧循环）；Manager 自装 PrepareJoin 控制协议 handler；开启
//     AutoPromoteLearners 时自动把追平的 learner 升为 voter
//
// 边界：
//   - 不擅自清空数据目录——WipeForRejoin 是破坏性动作，调用方必须先
//     打日志留痕再经 Rejoin/手动流程编排（启动自愈决策属 main）
//   - 摊布只做「向 preferred 转移」的保守判定（本节点连续 lead ≥3 个
//     周期、preferred 存活且为 voter 才动），不强制——节点挂掉时组
//     停留在现任，恢复后自动回迁
//   - 不接 core 写路径——本层只组装与管理集群机件，队列读写仍走原路径
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/xushixin/sq/internal/store"
)

// MetaGroup 是元数据组的固定组号：成员表引导、全局元数据等与队列无关
// 的提案走该组。数据组号从 1 起，与 GroupForQueue 的返回值域一致。
const MetaGroup uint32 = 0

// ErrUncleanShutdown 表示上次关机未写干净关机标记：异步刷盘（mem 档）
// 下盘上数据可能缺最后一段日志，裸恢复会让 FSM 与多数派分歧，因此
// 拒绝直接恢复——须清空状态、经存活 leader 以 learner 身份重新加入。
var ErrUncleanShutdown = errors.New("cluster: 上次为不干净关机，须以 learner 重入（清空状态后经存活 leader 重新加入）")

// Options 是 NewManager 的装配参数。
//
// 监听地址双语义（I5 修复）：Peers 里的地址是**拨号通告**地址——NAT/容器
// 场景下 raft_addr 是外网可达地址，本节点自己绑定它必然失败（外网地址
// 不落在本机网卡上）。因此绑定与通告拆开：
//   - ListenAddr：本节点**绑定**地址（可为本机网卡/0.0.0.0）；空时回退
//     到 Peers[NodeID]（测试/单机形态，两者相同）
//   - Peers[NodeID]：**拨号通告**地址，传输层永远用它拨对端、也把它
//     写进 peer 表广播；本节点消息短路，不参与自身拨号
type Options struct {
	NodeID     uint64            // 本节点 id（1..3）
	Peers      map[uint64]string // 全体节点 id → raft 监听地址（含本节点；语义=拨号通告地址）
	Listener   net.Listener      // 可选：测试注入已建监听（nil 则按 ListenAddr/Peers[NodeID] 监听）
	ListenAddr string            // 本节点绑定地址（空回退 Peers[NodeID]，见类型注释）
	DataGroups uint32            // 数据组数，默认 3；首启持久化，此后不可变
	Mode       AckMode
	Store      *store.Store
	Logger     *slog.Logger

	// OnLeaderChange/OnApplied 是全组共享的装配钩子（batch③，均可为
	// nil）：在组 Ready 循环内同步触发——钩子不得阻塞（阻塞即卡死该组
	// 全部 tick/消息/apply 处理），重活自行 dispatch 到独立 goroutine；
	// OnApplied 的 repr 参数不得保留引用（Ready 循环结束后缓冲可能被
	// raft 复用），需保留须自行拷贝。钩子 panic 不恢复（注入方 bug）。
	//
	// OnLeaderChange: 组 leader 变更（当选/失联/换人）时触发，leader=0
	// 表示当前无 leader；isSelf 表示本节点是否就是新 leader。
	// OnApplied: 每条普通条目 apply 成功后触发，携带组号与原始批次 repr
	// （空条目 repr 为 nil）。
	OnLeaderChange func(g uint32, leader uint64, isSelf bool)
	OnApplied      func(g uint32, repr []byte)

	// ControlHandler 是控制通道的接收处理器（batch③，可为 nil）：对端
	// 经 Manager.Control 发起短连接 RPC 时，传输层读循环在同一连接上
	// 同步调用它，返回值作为应答帧写回（错误返回作为失败应答带回调用
	// 方）。nil 时对端收到「控制通道未装配」错误帧。
	// 注意：处理在传输层读循环内同步执行，不得阻塞（重活自行 dispatch）。
	ControlHandler func(op byte, payload []byte) ([]byte, error)

	// LeaderBalancerInterval 是确定性 leader 摊布循环的周期（batch③，
	// ≤0 表示不启动）：每周期对本节点 lead 的每个组做一次「向 preferred
	// 节点转移领导权」的判定（策略见 StartLeaderBalancer 注释）。生产由
	// main 注入；测试可注小间隔（cluster 测试 harness 注入 200ms）。
	LeaderBalancerInterval time.Duration

	// AutoPromoteLearners 控制追平后的自动升 voter（batch③，默认 false）：
	// 开启时 Start 起一个与摊布循环共节拍的循环——对本节点 lead 的每个
	// 组，把 Status(g).Progress 中「IsLearner 且 Match 已追平本组日志
	// 末位（lastIndex）」的节点 ProposeConfChange(AddNode) 升为 voter。
	// 「无新提案时位点即锚点」：Match 追上当下 lastIndex 即达标，不追求
	// 绝对静止点，其后 leader 的新提案 learner 照常收。
	// 与手工 ProposeConfChange 并存不冲突：raft 成员变更天然串行
	// （pendingConfIndex 校验），并发提出时后者被静默替换成空条目、
	// 等待超时——本循环下轮 tick 重判，手工调用方自行重试。
	// 注意：升 voter 循环与摊布循环共节拍，依赖 LeaderBalancerInterval
	// 注入（≤0 时两者都不启动）；重入编排主体是 Rejoin。
	AutoPromoteLearners bool

	// RetainEntries 是截断循环的日志保留量（batch④，默认 10000；Task 8
	// 的配置面接管）：落后 follower 的位点一旦落在截断点之下，日志追齐
	// 不可能，只能靠 MsgSnap 快照追平。0 时按 defaultRetainEntries。
	RetainEntries uint64

	// TruncateInterval 是周期截断循环的执行间隔（batch④，默认 30s；0
	// 按 defaultTruncateInterval）：每周期对全部组评估一次日志截断——
	// 保留量之外的已 apply 前缀落锚点后删除（截断点计算见
	// truncateOnce 注释）。生产由 main 注入（config cluster.
	// truncate_interval）。
	TruncateInterval time.Duration

	// SnapshotChunkBytes 是 OpFetchSnapshot 单块的字节预算（batch④，
	// 默认 4MiB；0 按 defaultSnapshotChunkBytes）：emit 累计到该阈值
	// 即提前收口（游标 = 最后一个已发出的键）。必须小于传输层帧上限
	// maxFrameLen（16MiB）——上界由配置面校验保证（见 config.go 的
	// snapshot_chunk_bytes 校验）。
	SnapshotChunkBytes int

	// NoBootstrap 控制 fresh 路径的引导方式（batch④ Task 10，默认
	// false）：false 时以 StartNode(peers) 引导完整成员表（新集群开机、
	// 单节点形态、Rejoin——引导条目与 leader 日志在 (index, term) 上
	// 撞车，探测锚在引导条目上，追齐走日志重放）；true 时空白启动
	// （RestartNode 空存储，无引导条目、无成员表）——本节点被动等待
	// leader 的追齐（未压缩日志 = 重放，压缩日志 = 快照，见 buildGroup
	// fresh 分支注释）。Join 强制置 true：带成员表引导会让空目录节点
	// 的日志与 leader 撞车、快照永不触发，只在 FSM 里的存量数据（单机
	// →集群升级，spec §7）永远追不上（batch④ 扩容 e2e 抓到的根因）。
	NoBootstrap bool

	// BootstrapVoters 限定 fresh 路径的引导成员表（batch④ Task 10，
	// nil 按 Peers 全量引导）：只引导给定子集为 voter，传输层地址表
	// 仍用全量 Peers。单机种子扩容场景（spec §7）用：种子节点的数据
	// 目录以单节点集群启动（raft 引导只含自己，否则多成员 quorum
	// 不可达、永远选不出 leader），但传输层必须预先知道未来成员的
	// 地址（Join 加入的新节点靠种子拨号/种子回拨——地址表不全，
	// 发给新节点的消息会被传输层静默丢弃，快照永远到不了）。
	// 校验：子集必须都在 Peers 表（无地址的引导成员无从拨号）。
	BootstrapVoters []uint64
}

// Manager 是多组装配体：持有全部 raft 组、传输层与恢复判定。
type Manager struct {
	nodeID     uint64
	peers      map[uint64]string
	dataGroups uint32
	mode       AckMode
	st         *store.Store
	rs         *raftStore
	lg         *slog.Logger
	groups     map[uint32]*group
	snaps      *snapRegistry // 快照注册表（Task 4）：全组共享一份，组 Storage.Snapshot() 现场登记 ReadView
	ln         net.Listener  // 本节点监听：NewManager 持有，Start 移交传输层
	tr         *transport    // Start 时装配；send 回调运行时取值，必已就绪

	// 装配钩子（Options 透传，nil 安全）：每个组共享同一份，契约见
	// Options 字段注释（Ready 循环内同步触发，不得阻塞）
	onLeaderChange func(g uint32, leader uint64, isSelf bool)
	onApplied      func(g uint32, repr []byte)

	// controlHandler 是控制通道接收处理器（Options 透传，nil 安全）：
	// 传输层收到 ControlGroup 帧时在读循环内同步调用（transport.control）
	controlHandler func(op byte, payload []byte) ([]byte, error)

	cancel         context.CancelFunc // 运行 ctx 取消句柄（StopClean/kill 用）
	doneCh         chan struct{}      // 全部组 + flusher 完全退出后关闭
	flusherStop    chan struct{}      // 仅 mem 档：通知后台刷盘 goroutine 退出
	flusherDone    chan struct{}      // 仅 mem 档：刷盘 goroutine 退出后关闭
	flusherStopOne sync.Once          // flusherStop 幂等关闭（kill 与 StopClean 都可能触发）

	// 摊布循环（batch③）：leaderBalancerInterval 来自 Options（≤0 不
	// 启动）；runCtx 是 Start 设置的运行上下文——循环随集群停机一并
	// 退出；balancerOnce 保证重复启动只起一个循环。
	leaderBalancerInterval time.Duration
	runCtx                 context.Context
	balancerOnce           sync.Once

	// 自动升 voter（batch③）：autoPromote 来自 Options.AutoPromoteLearners，
	// 循环与摊布循环共节拍（见 promoteLearners 注释）。
	autoPromote bool

	// retainEntries 是截断循环的日志保留量（Options.RetainEntries 透传，
	// 0 按 defaultRetainEntries）：落后 follower 的位点一旦落在截断点
	// 之下就只能靠 MsgSnap 追平（Task 7 的截断前置；Task 8 的
	// truncateOnce 用它算截断点）。
	retainEntries uint64

	// 截断循环（batch④）：truncateInterval 来自 Options（0 按默认 30s）；
	// runCtx 是 Start 设置的运行上下文——循环随集群停机一并退出；
	// truncateLoopOnce 保证重复启动只起一个循环。truncateLoopDone 是
	// 循环退出信号（它写 store，必须被 Done 观察——见 Start 注释）。
	truncateInterval time.Duration
	truncateLoopOnce sync.Once
	truncateLoopDone chan struct{}

	// noBootstrap 来自 Options.NoBootstrap：fresh 路径的引导方式
	// （StartNode 引导成员表 vs RestartNode 空白启动，见 Options 注释）。
	noBootstrap bool

	// snapshotChunkBytes 是快照分块的字节预算（Options.SnapshotChunkBytes
	// 透传，0 按 defaultSnapshotChunkBytes）：handleFetchSnapshot 用它做
	// emit 收口阈值。
	snapshotChunkBytes int

	// prepareJoinMu 串行化本节点的 PrepareJoin 处理（见
	// handlePrepareJoin 注释：并发提成员变更会被 raft 静默替换）。
	prepareJoinMu sync.Mutex
}

// defaultRetainEntries 是 Options.RetainEntries 的默认值（0 即默认）：
// 截断循环保留的日志条目数。生产值由 Task 8 的配置面接管。
const defaultRetainEntries uint64 = 10000

// defaultTruncateInterval 是 Options.TruncateInterval 的默认值（0 即
// 默认）：周期截断循环的执行间隔，30s 一轮 × 全组。
const defaultTruncateInterval = 30 * time.Second

// defaultSnapshotChunkBytes 是 Options.SnapshotChunkBytes 的默认值
// （0 即默认）：快照单块字节预算 4MiB。
const defaultSnapshotChunkBytes = 4 << 20

// NewManager 装配全部 raft 组并按磁盘状态判定恢复路径。
//
// 装配序：
//  1. EnsureGroups 校验/持久化数据组数（首启写入，此后不可变）
//  2. ConsumeCleanShutdown 读取并删除干净关机标记
//  3. 恢复路径三分：
//     - 有标记：各组 Load 回放磁盘日志，RestartNode 原身份回归
//     - 无标记且盘上有 raft 状态（任一组的 HardState 或 applied 非零）：
//     返回 ErrUncleanShutdown——异步刷盘下不得裸恢复
//     - 无标记且无 raft 状态（全新目录）：StartNode 按 Peers 引导
//  4. 装配本节点监听（注入或按 Peers[NodeID]）
//
// 返回的 Manager 未启动；调用方必须调用 Start 后使用。
func NewManager(o Options) (*Manager, error) {
	if o.Store == nil {
		return nil, errors.New("cluster: Options.Store 不能为 nil")
	}
	if o.NodeID == 0 {
		return nil, errors.New("cluster: Options.NodeID 不能为 0")
	}
	addr, ok := o.Peers[o.NodeID]
	if !ok {
		return nil, fmt.Errorf("cluster: Peers 缺少本节点 %d 的监听地址", o.NodeID)
	}
	if o.DataGroups == 0 {
		o.DataGroups = 3
	}
	if o.RetainEntries == 0 {
		o.RetainEntries = defaultRetainEntries
	}
	if o.TruncateInterval == 0 {
		o.TruncateInterval = defaultTruncateInterval
	}
	if o.SnapshotChunkBytes == 0 {
		o.SnapshotChunkBytes = defaultSnapshotChunkBytes
	}
	lg := o.Logger
	if lg == nil {
		lg = slog.Default()
	}
	m := &Manager{
		nodeID:                 o.NodeID,
		peers:                  o.Peers,
		dataGroups:             o.DataGroups,
		mode:                   o.Mode,
		st:                     o.Store,
		lg:                     lg,
		groups:                 make(map[uint32]*group, o.DataGroups+1),
		onLeaderChange:         o.OnLeaderChange,
		onApplied:              o.OnApplied,
		leaderBalancerInterval: o.LeaderBalancerInterval,
		autoPromote:            o.AutoPromoteLearners,
		retainEntries:          o.RetainEntries,
		truncateInterval:       o.TruncateInterval,
		snapshotChunkBytes:     o.SnapshotChunkBytes,
		noBootstrap:            o.NoBootstrap,
		doneCh:                 make(chan struct{}),
	}
	m.rs = newRaftStore(o.Store, lg)
	// 快照注册表：每个 Manager 一份，全组共享（Task 4）。TTL 用默认
	// 常量，生产可调值由 Task 8 的配置面接管。
	m.snaps = newSnapRegistry(o.Store, snapRegistryDefaultTTL, lg)

	// PrepareJoin/FetchSnapshot handler 自装（batch③/④）：在任何注入的
	// ControlHandler 之前包一层——OpPrepareJoin（重入编排协议面）与
	// OpFetchSnapshot（快照分块拉取）都由 Manager 内部处理，其余 op
	// 转发给调用方注入的 handler；未注入时回「控制通道未装配」（保持
	// nil handler 的对端语义，probePeerAlive 依赖该错误形状）。
	//
	// 为什么这里只有 op 3/4 而没有 1/2：OpForwardAppend/OpForwardApply
	// 需要 core 组件（复制器、消息追加与批次重放），由 main 的
	// ControlHandler 装配（见 cmd/sq/main.go 的 controlHandler）；Manager
	// 自装层刻意只处理不依赖 core 的集群内协议——这不是漏接。
	userHandler := o.ControlHandler
	m.controlHandler = func(op byte, payload []byte) ([]byte, error) {
		switch op {
		case OpPrepareJoin:
			return m.handlePrepareJoin(payload)
		case OpFetchSnapshot:
			return m.handleFetchSnapshot(payload)
		}
		if userHandler == nil {
			return nil, errControlUnassembled
		}
		return userHandler(op, payload)
	}

	// 组数契约：首启持久化，此后不可变——组数变即存量数据错组
	if err := m.rs.EnsureGroups(o.DataGroups); err != nil {
		return nil, err
	}
	clean, err := m.rs.ConsumeCleanShutdown()
	if err != nil {
		return nil, err
	}
	hasRaft, err := m.diskHasRaftState()
	if err != nil {
		return nil, err
	}
	recovery := "fresh"
	switch {
	case clean:
		recovery = "clean-resume"
	case hasRaft:
		m.lg.Error("检测到不干净关机，拒绝直接恢复——须清空状态以 learner 重入（先 Close store，再 WipeForRejoin，经存活 leader 的 ConfChange 重新加入）")
		return nil, ErrUncleanShutdown
	}

	// 引导成员表：全量 Peers（fresh 路径 StartNode 按此追加 ConfChange
	// 条目）；Options.BootstrapVoters 非空时只引导给定子集（单机种子
	// 扩容：raft 引导只含自己、传输层地址表仍全量，见 Options 注释）。
	// 干净路径不传成员表，身份由日志回放恢复。
	peers := make([]raft.Peer, 0, len(o.Peers))
	bootIDs := o.BootstrapVoters
	if len(bootIDs) == 0 {
		// 默认路径：map 键天然无重复，直接取确定性排序
		bootIDs = sortedPeerIDs(o.Peers)
	} else {
		// BootstrapVoters 是外部传入的切片，可能含重复 id——去重后按
		// id 升序（引导成员表需要确定性顺序，见 sortedPeerIDs；重复 id
		// 会产生重复的 ConfChange 引导条目，raft 侧行为未定义）
		uniq := make(map[uint64]struct{}, len(bootIDs))
		for _, id := range bootIDs {
			uniq[id] = struct{}{}
		}
		bootIDs = make([]uint64, 0, len(uniq))
		for id := range uniq {
			bootIDs = append(bootIDs, id)
		}
		sort.Slice(bootIDs, func(i, j int) bool { return bootIDs[i] < bootIDs[j] })
	}
	for _, id := range bootIDs {
		if _, ok := o.Peers[id]; !ok {
			return nil, fmt.Errorf("cluster: BootstrapVoters 节点 %d 不在 Peers 表——引导成员必须带地址（传输层拨号用）", id)
		}
		peers = append(peers, raft.Peer{ID: id})
	}
	for g := uint32(0); g <= o.DataGroups; g++ {
		gr, err := m.buildGroup(g, clean, peers)
		if err != nil {
			return nil, err
		}
		m.groups[g] = gr
	}

	// 本节点监听：测试注入的 Listener 直接复用；否则按 ListenAddr 绑定
	//（空回退 Peers[NodeID]，单机/测试形态两者相同；NAT/容器场景
	// ListenAddr 是本机绑定地址、Peers[NodeID] 是外网通告地址，见
	// Options 类型注释）。放在恢复判定之后——ErrUncleanShutdown 路径
	// 不得泄漏监听器。
	if o.Listener != nil {
		m.ln = o.Listener
	} else {
		bind := o.ListenAddr
		if bind == "" {
			bind = addr
		}
		ln, err := net.Listen("tcp", bind)
		if err != nil {
			return nil, fmt.Errorf("cluster: 本节点 %d 监听 %s: %w", o.NodeID, bind, err)
		}
		m.ln = ln
	}

	m.lg.Info("集群管理器初始化", "node", o.NodeID, "groups", m.Groups(), "dataGroups", o.DataGroups, "mode", o.Mode.String(), "recovery", recovery)
	return m, nil
}

// buildGroup 构造单个 raft 组，按恢复路径分叉：
//
//   - clean：rs.Load 读回磁盘日志与快照锚点重建 MemoryStorage（锚点提供
//     {Index, Term, ConfState} 与起始位点，日志被全量截断时也以此恢复；
//     成员表优先读持久化值 LoadConfState，旧目录无该值时回退日志重放
//     合成，见函数内注释），raft.RestartNode 恢复，且
//     raftConfig.Applied = 磁盘 applied（raft 从 applied+1 重投递，
//     配合 group 的跳过逻辑双保险，见下述 seeding 注释）
//   - fresh：raft.StartNode 以引导成员表启动
func (m *Manager) buildGroup(g uint32, clean bool, peers []raft.Peer) (*group, error) {
	storage := raft.NewMemoryStorage()
	applied := uint64(0)
	if clean {
		hs, ents, snapMeta, err := m.rs.Load(g)
		if err != nil {
			return nil, fmt.Errorf("cluster: 组 %d 恢复读取: %w", g, err)
		}
		if snapMeta != nil {
			// 截断过的组：锚点是权威位点与成员表来源——即使日志被全量
			// 截断（len(ents)==0）也必须 ApplySnapshot，否则
			// RestartNode 拿不到 ConfState（InitialState 只认快照）与
			// 任期基线，节点以空成员表启动直接变哑（Task 2 遗留缺口）。
			snapIndex, snapTerm := snapMeta.GetIndex(), snapMeta.GetTerm()
			cs := snapMeta.GetConfState()
			if len(cs.GetVoters()) == 0 {
				// 锚点未带成员表：回退 Task 2 持久化成员表——那是
				// ConfChange apply 时整表落盘的权威来源
				persisted, ok, err := m.rs.LoadConfState(g)
				if err != nil {
					return nil, fmt.Errorf("cluster: 组 %d 读成员表: %w", g, err)
				}
				if !ok {
					// 从未持久化过成员表（旧数据目录首次升级）：日志
					// 若还有条目则重放合成，否则空表——截断过的组
					// 必有持久化成员表，此分支实际不可达
					cs, err = confStateFromEntries(ents, hs.GetCommit())
					if err != nil {
						return nil, fmt.Errorf("cluster: 组 %d 成员表合成: %w", g, err)
					}
					m.lg.Info("成员表由日志重放合成（旧数据目录首次升级）", "g", g,
						"voters", cs.GetVoters(), "learners", cs.GetLearners())
				} else {
					cs = persisted
				}
			}
			snap := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
				Index:     &snapIndex,
				Term:      &snapTerm,
				ConfState: cs,
			}}
			if err := storage.ApplySnapshot(snap); err != nil {
				return nil, fmt.Errorf("cluster: 组 %d 重放快照: %w", g, err)
			}
			if len(ents) > 0 {
				if err := storage.Append(ents); err != nil {
					return nil, fmt.Errorf("cluster: 组 %d 重放条目: %w", g, err)
				}
			}
		} else if len(ents) > 0 {
			first := ents[0]
			snapIndex := first.GetIndex() - 1
			// snapTerm 用首条条目的 term 近似「缺失条目 index-1 的 term」：
			// raft 只在任期比较时用到它，而两条路径都不会触发该比较——
			// 不干净路径空存储启动（snapTerm=0）只与新鲜 leader 的 term(0)
			// 相遇；干净路径 leader 的 Progress 仍保留，追齐从不会回退到
			// index-1 对账（若未来做快照压缩，需按真实 term 补快照，见 B8.2）
			snapTerm := first.GetTerm()
			// 成员表优先取持久化值（Task 2）：截断之后日志前缀已被删，
			// 重放合成不再可能。仅当从未持久化过（batch③ 及更早的数据
			// 目录首次升级到本版本）才回退到日志重放合成。
			cs, ok, err := m.rs.LoadConfState(g)
			if err != nil {
				return nil, fmt.Errorf("cluster: 组 %d 读成员表: %w", g, err)
			}
			if !ok {
				cs, err = confStateFromEntries(ents, hs.GetCommit())
				if err != nil {
					return nil, fmt.Errorf("cluster: 组 %d 成员表合成: %w", g, err)
				}
				m.lg.Info("成员表由日志重放合成（旧数据目录首次升级）", "g", g,
					"voters", cs.GetVoters(), "learners", cs.GetLearners())
			}
			snap := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
				Index:     &snapIndex,
				Term:      &snapTerm,
				ConfState: cs,
			}}
			if err := storage.ApplySnapshot(snap); err != nil {
				return nil, fmt.Errorf("cluster: 组 %d 重放快照: %w", g, err)
			}
			if err := storage.Append(ents); err != nil {
				return nil, fmt.Errorf("cluster: 组 %d 重放条目: %w", g, err)
			}
		}
		if !raft.IsEmptyHardState(hs) {
			if err := storage.SetHardState(hs); err != nil {
				return nil, fmt.Errorf("cluster: 组 %d 重放 HardState: %w", g, err)
			}
		}
		applied, err = m.rs.Applied(g)
		if err != nil {
			return nil, fmt.Errorf("cluster: 组 %d 读 applied: %w", g, err)
		}
		// 安装中标记存在 = 上次快照安装未完成，本组状态是半截的。
		// 清空该组键族让 raft 重新发快照/全量重放——半截状态当完整状态
		// 启动会让本节点向客户端返回缺失的消息（静默丢数据）。
		// 注意：LoadInstalling 必须在 Load 之后——日志回放与标记检查
		// 都要基于同一份磁盘实况，且清空重来要覆盖刚读回的 applied。
		if meta, installing, err := m.rs.LoadInstalling(g); err != nil {
			return nil, err
		} else if installing {
			m.lg.Warn("发现未完成的快照安装，清空该组全部状态重来", "g", g, "index", meta.GetIndex())
			if err := wipeGroupKeys(m.st, g, m.dataGroups); err != nil {
				return nil, fmt.Errorf("cluster: 组 %d 清空重来: %w", g, err)
			}
			// 完整重置契约（C1）：ResetGroupProgress 把 applied、锚点、
			// 标记、全部日志条目、HardState 一并清空（单批 Sync）——只清
			// applied 的旧行为 = 重启后带着半截日志（锚点 A 之后的部分）
			// + HardState 启动，raft 从 A+1 重放进被清空的 FSM，状态
			// 1..A 永久丢失且 raft 不知情；leader 换人后探测锚在 A+1..P
			// 上撞车、永不发快照——C1 静默丢段。
			if err := m.rs.ResetGroupProgress(g); err != nil {
				return nil, err
			}
			applied = 0
			// 上面从旧磁盘状态 seed 的 mem（锚点快照 + 条目 + HardState）
			// 随日志清空一并作废：换全新空存储启动（firstIndex=1），
			// leader 的探测一路回退到日志起点，只能全量重放或发快照，
			// 两条路径都完整——杜绝静默丢段。
			storage = raft.NewMemoryStorage()
		}
	}
	gr := newGroup(g, m.nodeID, storage, m.snaps, m.rs, m.st, m.send, m.mode, m.onLeaderChange, m.onApplied, m.lg)
	// raft 节点在 newGroup 之后装配：Config.Storage 必须用包了快照生成
	// 器的 stg——raft 给落后 follower 发 MsgSnap 时调用的是
	// groupStorage.Snapshot() 的现场生成，而非 MemoryStorage 的预置快照；
	// 而 stg 引用组内 atomics（applied/confState/applyMu），只能在组骨架
	// 就绪后创建（见 newGroup 注释）。
	cfg := raftConfig(m.nodeID, gr.stg)
	var rn raft.Node
	if clean {
		cfg.Applied = applied
		rn = raft.RestartNode(cfg)
	} else if m.noBootstrap {
		// Join 空白启动（Options.NoBootstrap，仅 Join 置位）：空存储 +
		// 空成员表，本节点成为被动 follower，等待 leader 的快照/日志
		// 追齐赋予数据与成员表。
		//
		// 为什么必须空白而不是 StartNode(peers) 引导：StartNode 的
		// Bootstrap 会为每个成员追加一条 term=1 的 ConfChangeAddNode
		// 引导条目（index 1..n）并标记为已提交。leader 对空日志 follower
		// 的探测会一路回退到其 lastIndex（=n），而 leader 日志在 1..n
		// 上的 term 恰也是 1——(index, term) 撞车让锚点探测「匹配」，
		// 追齐走日志重放而不是快照。存量数据若只在 FSM 里（单机→集群
		// 升级，spec §7）不在日志里，重放永远带不过去，快照也永不触发
		// （batch④ 扩容 e2e 抓到的根因）。空白启动下 follower 的日志
		// 没有任何 term 可撞：探测一路回退到日志起点，走「重放或快照」
		// 的 raft 标准判定（见下）。
		//
		// 空白启动的追齐路径分两种（都是 raft 标准语义）：
		//   - leader 日志未压缩：探测回退到假想条目 0（未压缩日志的
		//     term(0) 返回 0 而非越界），锚点匹配 → 全量日志重放——只
		//     携带日志里的状态，FSM 里的存量（单机档数据）到不了
		//   - leader 日志已压缩（截断循环把起点推到新节点之下）：term(0)
		//     报 ErrCompacted → 只能 MsgSnap——快照从 leader 的活 store
		//     现场生成（Task 4），FSM 存量（含单机档数据）整体到达
		// 因此 Join 的完整语义 = 空白启动（消除撞车）+ 调用方确保 leader
		// 日志已压缩到新节点起点之下（扩容 e2e 用 marker + 截断循环
		// 显式制造，见测试注释；生产上单机档运行期间日志自然增长并被
		// 周期截断）。压缩与否只决定「重放还是快照」，空白启动本身是
		// 两者成立的前提。
		rn = raft.RestartNode(cfg)
	} else {
		rn = raft.StartNode(cfg, peers)
	}
	gr.rn = rn
	// 快照接收侧装配（Task 7）：控制回调（拉块 RPC）与数据组数（清空
	// 重来的哈希归属分母）由 Manager 注入——同 rn 的「newGroup 后装配」
	// 模式（stg 依赖组内骨架就绪，control/groups 同理）。
	gr.control = m.Control
	gr.groups = m.dataGroups
	// 重启重放跳过守卫：raft 从 applied+1 重投递，内存 applied 必须先
	// 从磁盘位点填充，否则已 apply 的条目会被重放（Task 4 约定）；
	// fresh 路径 applied=0，Store 无副作用
	gr.applied.Store(applied)
	return gr, nil
}

// send 是组消息外发回调（newGroup 注入）。本节点消息直接就地 step，
// 不进传输层——省一次序列化往返，单成员测试也靠它跑通；其余消息
// 按目标节点交给传输层投递。
//
// 注意：m.tr 在 Start 时装配，本回调只被各组 run 循环（Start 之后
// 才启动）调用，取值时必然已就绪。
func (m *Manager) send(g uint32, msgs []*raftpb.Message) {
	gr := m.groups[g]
	var peerMsgs []*raftpb.Message
	for _, msg := range msgs {
		if msg.GetTo() == m.nodeID {
			gr.step(msg)
		} else {
			peerMsgs = append(peerMsgs, msg)
		}
	}
	if len(peerMsgs) > 0 {
		m.tr.Send(g, peerMsgs)
	}
}

// diskHasRaftState 判定磁盘上是否已存在 raft 日志状态：MetaGroup 的
// applied 位点非零，或任一组的 HardState 存在，都说明本节点曾参与
// 过集群——干净标记缺失时不得裸恢复。
func (m *Manager) diskHasRaftState() (bool, error) {
	app, err := m.rs.Applied(0)
	if err != nil {
		return false, err
	}
	if app != 0 {
		return true, nil
	}
	for g := uint32(0); g <= m.dataGroups; g++ {
		_, ok, err := m.st.Get(hsKey(g))
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// sortedPeerIDs 返回 peers 的 id 升序列表——引导成员表需要确定性顺序
// （map 迭代无序会破坏黄金值语义）。
func sortedPeerIDs(peers map[uint64]string) []uint64 {
	ids := make([]uint64, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Start 启动全部机件：传输层（deliver 按组路由）、各组 run 循环、
// mem 档的后台批量刷盘 goroutine，以及 Done 观察者。
//
// 注意：
//   - ctx 仅作父上下文——实际运行上下文由内部派生，StopClean/kill 取消
//   - 本方法立即返回；完全退出经 Done 观察
func (m *Manager) Start(ctx context.Context) {
	rctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.runCtx = rctx

	// 传输层 peers 不含本节点（本节点消息由 send 短路，见 send 注释）；
	// deliver 按信封组号路由到对应组的 step
	m.tr = newTransport(m.nodeID, m.ln, m.peerAddrs(), func(g uint32, msg *raftpb.Message) {
		if gr, ok := m.groups[g]; ok {
			gr.step(msg)
		} else {
			m.lg.Debug("收到未知组的消息，丢弃", "g", g)
		}
	}, m.controlHandler, m.lg)
	m.tr.Start(rctx)

	for g := uint32(0); g < m.Groups(); g++ {
		go m.groups[g].run(rctx)
	}
	if m.mode == AckQuorumMem {
		m.flusherStop = make(chan struct{})
		m.flusherDone = make(chan struct{})
		go m.flusher()
	}
	// leader 摊布循环（Options 注入周期，≤0 不启动）：纯控制面，独立
	// goroutine，不参与 Done 观察（停机时随 runCtx 取消退出，不阻塞）
	m.StartLeaderBalancer(m.leaderBalancerInterval)
	// 截断循环（Options 注入周期，≤0 不启动；默认 30s）：随 Start 拉起、
	// 随 runCtx 取消退出。与摊布循环不同，它**写 store**（SaveSnapMeta/
	// TruncateLog），必须参与 Done 观察——否则调用方按 Done 关 store 时
	// 循环可能正执行到一半，对已关闭的 pebble 写直接 panic（batch④ 截断
	// e2e 用 2s 周期抓到的停机竞态）。退出只多等一轮当前 truncateOnce
	//（毫秒级），不阻塞停机。
	m.truncateLoopOnce.Do(func() {
		if m.truncateInterval > 0 {
			done := make(chan struct{})
			m.truncateLoopDone = done
			go func() {
				defer close(done)
				m.truncateLoop(m.runCtx, m.truncateInterval)
			}()
		}
	})
	// Done 观察者：全部组、flusher 与截断循环退出后关闭 doneCh
	go func() {
		for _, gr := range m.groups {
			<-gr.done()
		}
		if m.flusherDone != nil {
			<-m.flusherDone
		}
		if m.truncateLoopDone != nil {
			<-m.truncateLoopDone
		}
		close(m.doneCh)
	}()
}

// StartLeaderBalancer 启动确定性 leader 摊布循环：每 interval 对本节点
// lead 的每个组做一次摊布判定（条件与策略见 leaderBalancer）。
//
// Start 内按 Options.LeaderBalancerInterval 自动调用；测试可经 Options
// 注入小间隔。interval<=0 时是 no-op；重复调用只生效一次（sync.Once）。
func (m *Manager) StartLeaderBalancer(interval time.Duration) {
	if interval <= 0 {
		return
	}
	if m.runCtx == nil {
		m.lg.Error("摊布循环要求先 Start 再启动", "interval", interval.String())
		return
	}
	m.balancerOnce.Do(func() { go m.leaderBalancer(m.runCtx, interval) })
}

// leaderBalancer 是摊布循环本体：每 interval 对本节点 lead 的每个组做
// 一次摊布判定（balanceOnce）。ctx 取消即退出。
//
// 摊布策略（确定性、无协调收敛）：组 g 的 preferred leader =
// sortedPeerIDs(peers)[g % len(peers)]。全部节点跑同一公式、只看自己
// lead 的组，不需要任何协调即可收敛到同一分布。
func (m *Manager) leaderBalancer(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// preferred 表一次算好：peers 全量静态（成员变更走 learner 重入
	// 流程，本循环只认启动时的全量成员，不随 ConfChange 动态漂移）
	ids := sortedPeerIDs(m.peers)
	preferred := make(map[uint32]uint64, m.Groups())
	for g := uint32(0); g < m.Groups(); g++ {
		preferred[g] = ids[g%uint32(len(ids))]
	}
	// stableTicks 记录本节点连续 lead 各组的 tick 数（跨周期共享状态，
	// 见 balanceOnce 注释的稳定观察说明）
	stableTicks := make(map[uint32]int, m.Groups())
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for g := uint32(0); g < m.Groups(); g++ {
				// 自动升 voter 与摊布共节拍（AutoPromoteLearners）：
				// 先升后摊——升完的 learner 成为 voter 后，摊布的
				// preferred 转移判定才看得到它（balanceOnce 对 learner
				// 不转移）；本节点不再 lead 的组自然跳过
				if m.autoPromote {
					m.promoteLearners(g)
				}
				m.balanceOnce(g, preferred[g], stableTicks)
			}
		}
	}
}

// balanceOnce 对单个组执行一轮摊布判定。preferred 为该组的目标 leader，
// stableTicks 为稳定观察计数（跨周期共享状态）。
//
// 转移条件（全部满足才发起）：
//   - 本节点是该组当前 leader——只有 leader 有权发起转移，多节点并发
//     跑同一条循环也不会有冲突（follower 的判定天然空转）
//   - 本节点已连续 lead 该组 ≥ leaderStableTicks 个周期：刚当选就转走
//     会把 raft 的「转移期丢弃全部提案」（stepLeader 对 transfer 中的
//     MsgProp 返回 ErrProposalDropped）叠在选举后的写入恢复窗口上，
//     客户端在切换期重试的提案会再吃一次失败；稳定判定把转移推迟到
//     写入静默期之后，也避免选举震荡期的反复转移
//   - preferred 不是本节点（是则已收敛）
//   - preferred 在当前成员表中且是 voter 且 RecentActive（Status(g)
//     取）且经存活探测（probePeerAlive）：RecentActive 在本集群配置下
//     不会随时间衰减（未开 CheckQuorum，raft 只在 CheckQuorum 消息里
//     重置它），死节点会残留 true——必须再探一次存活，否则会把领导权
//     转给已死节点（见 probePeerAlive 注释）
func (m *Manager) balanceOnce(g uint32, preferred uint64, stableTicks map[uint32]int) {
	if !m.IsLeader(g) {
		stableTicks[g] = 0 // 重新当选后从头积累稳定观察
		return
	}
	stableTicks[g]++
	if stableTicks[g] < leaderStableTicks {
		return // 稳定观察不足，本周期不动
	}
	if preferred == m.nodeID {
		return // 本节点已是 preferred，无需转移
	}
	st, ok := m.Status(g)
	if !ok {
		return
	}
	pr, ok := st.Progress[preferred]
	if !ok || pr.IsLearner || !pr.RecentActive {
		// preferred 不在成员表/是 learner/不活跃：保持现任。向死节点
		// 转移会一直挂到 raft 超时中止，白白制造提案丢弃窗口；preferred
		// 恢复后自动回迁
		return
	}
	if !m.probePeerAlive(preferred) {
		m.lg.Debug("摊布：preferred 探测不可达，保持现任", "g", g, "preferred", preferred)
		return
	}
	m.lg.Info("摊布：组领导权转移", "g", g, "from", m.nodeID, "to", preferred, "reason", "摊布")
	m.TransferLeader(g, preferred)
	// 转移后进入退避：raft 的转移中止时限是 1 个 electionTimeout
	// （tickHeartbeat 里 abortLeaderTransfer），失败重试若按原节奏（每
	// interval 一次）会让丢弃窗口连续重叠。退避到负值使下次尝试在
	// 2×leaderStableTicks 个周期之后——长于中止时限，窗口不相交。
	stableTicks[g] = -leaderStableTicks
}

// probePeerAlive 探测节点是否存活：控制通道短连接 RPC——拨号成功即
// 视为存活（对端应答什么无关紧要：即使报「控制通道未装配」也证明它
// 活着）；拨号失败（连接拒绝/超时）视为死亡。
//
// 为什么需要它：RecentActive 在本集群配置下不会衰减（raft 只在
// CheckQuorum 消息里重置它，而 CheckQuorum 未开启），死节点的
// RecentActive 残留 true 会骗过摊布判定——把领导权转给已死节点后，
// raft 的转移挂起期内丢弃全部提案（见 balanceOnce 注释），窗口内所有
// 写入/成员变更都会失败。探测在转移决策点做（每周期每组至多一次），
// 代价可忽略。
func (m *Manager) probePeerAlive(nodeID uint64) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := m.Control(ctx, nodeID, 0, nil)
	if err == nil {
		return true
	}
	// 控制通道协议/装配错误（对端活着但应答失败）与拨号失败区分：
	// 只有拨号失败才算死亡——连接拒绝/超时都是 *net.OpError
	var opErr *net.OpError
	return !errors.As(err, &opErr)
}

// promoteLearners 对组 g 做一轮自动升 voter 判定（AutoPromoteLearners
// 循环，与摊布循环共节拍，见 leaderBalancer）：本节点 lead 时，把
// Status(g).Progress 中 IsLearner 且 Match 已追平本组日志末位
// （storage.LastIndex）的节点 ProposeConfChange(AddNode) 升为 voter。
//
// 「无新提案时位点即锚点」（harness 步骤 5 语义）：Match 追上当下
// lastIndex 即达标，不追求绝对静止点——其后 leader 的新提案 learner
// 照常收，追平窗口不要求 quiescence。
//
// 与手工 ProposeConfChange 并存不冲突：raft 成员变更天然串行
// （pendingConfIndex 校验），并发提出时后者被静默替换成空条目、
// 等待超时——本循环下轮 tick 重判即可，升 voter 是幂等目标态，
// 无需重试计数。
//
// 失败（超时/换 leader/提案丢弃）只记 Warn，下轮 tick 重新判定。
func (m *Manager) promoteLearners(g uint32) {
	gr, ok := m.groups[g]
	if !ok {
		return
	}
	// 先取 lastIndex 锚点再取 Status：Status 里的 Match 只会比锚点
	// 更新——若反过来，锚点后移会让「已追上旧锚点」的 learner 被
	// 误判为追平（少收一两条新条目），提前升 voter
	lastIndex, err := gr.stg.LastIndex()
	if err != nil {
		// MemoryStorage 的 LastIndex 在正常路径不可达错误（无快照时
		// 退化为 0），防御性跳过本轮
		m.lg.Warn("自动升 voter 取日志末位失败，下轮重试", "g", g, "err", err)
		return
	}
	st, ok := m.Status(g)
	if !ok || !m.IsLeader(g) {
		return // 非本节点 lead（含刚转移/失联）：learner 由新 leader 接管
	}
	for id, pr := range st.Progress {
		if !pr.IsLearner || pr.Match < lastIndex {
			continue // 未追平：下轮 tick 再判
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := m.ProposeConfChange(ctx, g, raftpb.ConfChangeAddNode, id)
		cancel()
		if err != nil {
			m.lg.Warn("自动升 voter 失败，下轮重试", "g", g, "node", id, "match", pr.Match, "lastIndex", lastIndex, "err", err)
			continue
		}
		m.lg.Info("自动升 voter", "g", g, "node", id, "match", pr.Match, "lastIndex", lastIndex)
	}
}

// truncateOnce 对一组执行一次截断评估与执行。
//
// 截断点计算（三重下界取最小，再减保留量）：
//   - 本节点 applied：不能截掉还没 apply 的条目；
//   - leader 额外取 min(Progress.Match)：不能截掉最慢 follower 还要的条目。
//     这不是正确性开关（有快照兜底），而是性能纪律——截过头会把一次
//     心跳级追齐放大成整组状态传输（Global Constraints）；
//   - 减 RetainEntries：留一段余量给刚落后一点点的 follower。
//
// 执行顺序固定为「先落锚点（Sync）→ 再删盘上条目 → 最后压内存视图」：
// 任一步崩溃都停在「锚点已落、条目还在」的安全态（多留日志无害，重启
// 时锚点提供起始位点，见 buildGroup 注释）。空转（无事可做）不写盘也
// 不打日志——周期循环 30s 一轮 × 全组，空转刷日志就是噪声。
//
// 返回：实际截断到的位点与是否真的执行了截断（无事可做时 done=false）。
func (m *Manager) truncateOnce(g uint32) (uint64, bool) {
	gr, ok := m.groups[g]
	if !ok {
		return 0, false
	}
	// 成员表与位点同临界区配对——与 Task 4 applyEntry/Snapshot 同一纪律：
	// 锚点 index（源自 applied）与 confState 必须取同一 apply 时刻，否则
	// 会产出「index=N 却携带 N+k 成员表」的截断锚点。锁内只读两个原子，
	// 无等待；mem.Term/SaveSnapMeta 等留在锁外（与 groupStorage.Snapshot
	// 的「锁内取配对、锁外查 term」同构）。
	gr.applyMu.Lock()
	applied := gr.applied.Load()
	cs := gr.confState.Load()
	gr.applyMu.Unlock()
	upto := applied
	leader := false
	if st, ok := m.Status(g); ok && st.RaftState == raft.StateLeader {
		leader = true
		for id, pr := range st.Progress {
			if id != m.nodeID && pr.Match < upto {
				upto = pr.Match
			}
		}
	}
	if upto <= m.retainEntries {
		return 0, false
	}
	upto -= m.retainEntries
	first, err := gr.stg.FirstIndex()
	if err != nil || upto < first {
		return 0, false // 已经截到这儿了，空转
	}
	term, err := gr.stg.Term(upto)
	if err != nil {
		m.lg.Warn("截断放弃：位点 term 不可查", "g", g, "upto", upto, "err", err)
		return 0, false
	}
	idx, tm := upto, term
	meta := &raftpb.SnapshotMetadata{Index: &idx, Term: &tm, ConfState: cs}
	if err := m.rs.SaveSnapMeta(g, meta); err != nil {
		m.lg.Error("截断放弃：锚点落盘失败", "g", g, "upto", upto, "err", err)
		return 0, false
	}
	if err := m.rs.TruncateLog(g, upto); err != nil {
		m.lg.Error("截断放弃：删条目失败", "g", g, "upto", upto, "err", err)
		return 0, false
	}
	if err := gr.mem.Compact(upto); err != nil && !errors.Is(err, raft.ErrCompacted) {
		m.lg.Warn("内存日志视图压缩失败（盘上已截断，下次重启自愈）", "g", g, "upto", upto, "err", err)
	}
	m.lg.Info("日志截断执行", "g", g, "upto", upto, "deleted", upto-first+1, "leader", leader)
	return upto, true
}

// truncateLoop 是截断循环本体：每 interval 对全部组执行一次截断评估
// （truncateOnce，保留量之内空转，见 truncateOnce 注释）。ctx 取消即
// 退出。
//
// 为什么是周期循环而非在写路径里顺带截断：截断是低频后台维护（默认
// 30s 一轮），写路径里每批多做一次 SaveSnapMeta（Sync 落盘）会吃掉
// 组提交延迟；循环把截断与提案路径彻底分离，单点控制节奏，也便于
// 统一从配置面调间隔。
func (m *Manager) truncateLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for g := uint32(0); g < m.Groups(); g++ {
				m.truncateOnce(g)
			}
			// 快照视图注册表 GC：视图不关会阻止 Pebble 回收被覆盖的旧版本
			// （磁盘膨胀），必须周期强制回收；循环节奏即 GC 心跳——默认
			// TTL 5min ≈ 10 个 tick 后视图才被回收。
			m.snaps.GCOnce(time.Now())
		}
	}
}

// leaderStableTicks 是摊布转移前的稳定观察门槛：本节点必须连续 lead
// 该组 ≥3 个周期（2 个完整 interval 的稳定期）才发起转移。为什么需要
// 稳定期见 balanceOnce 注释。
const leaderStableTicks = 3

// peerAddrs 返回传输层用的 peer 地址表：全量 Peers 减去本节点
// （本节点消息短路，不配置也不会被 Send 命中）。
func (m *Manager) peerAddrs() map[uint64]string {
	peers := make(map[uint64]string, len(m.peers))
	for id, addr := range m.peers {
		if id != m.nodeID {
			peers[id] = addr
		}
	}
	return peers
}

// flusher 是 AckQuorumMem 档的后台刷盘 goroutine：每 200ms 提交一个
// 空批次并带 pebble.Sync。空批次不携带数据，但会触发一次 WAL fsync，
// 借 WAL 顺序性把此前所有 NoSync 写入一并刷盘（spec §2.2「后台批量
// fsync」的最简实现）。全组共享一条 WAL，一个 flusher 即覆盖全部组。
func (m *Manager) flusher() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	defer close(m.flusherDone)
	for {
		select {
		case <-m.flusherStop:
			return
		case <-ticker.C:
			if err := m.st.ApplyWith(m.st.NewBatch(), true); err != nil {
				// WAL sync 失败即 pebble 不可恢复态，下一拍所有写都会炸，
				// 这行日志是尸检第一现场
				m.lg.Error("后台批量刷盘失败", "err", err)
			}
		}
	}
}

// StopClean 干净关机：先停全部组与后台刷盘 goroutine，再以干净关机
// 标记（Sync 落盘）收尾。
//
// 标记必须是关机的最后一次同步写：mem 档下组退出窗口内仍可能做
// NoSync 持久化与提案确认。若像旧次序那样先写标记，「标记已落盘但
// acked 尾部未刷」的窗口内断电，重启会误判为干净关机、把已确认却
// 未持久化的数据当既有事实；反过来的新次序里，崩溃只可能发生在
// 标记之前 = 标记缺失 = 走不干净路径（learner 重入），方向安全。
//
// 标记写入失败时记 Error 并按不干净关机处理（下次启动走 learner 重入），
// 但关闭流程不阻塞、照常完成——spike 语义：关机是确定性动作，标记只是
// 下次启动的恢复路径判定依据。
//
// 返回：标记写入失败时返回该错误（关机本身已完成）；成功返回 nil。
func (m *Manager) StopClean(ctx context.Context) error {
	m.cancel()
	// 等全部组退出（run 循环结束）后再停 flusher：退出期的最后写入
	// 仍被周期刷盘覆盖
	for _, gr := range m.groups {
		<-gr.done()
	}
	if m.flusherStop != nil {
		m.flusherStopOne.Do(func() { close(m.flusherStop) })
		<-m.flusherDone
	}
	// 干净关机标记作为最后一次同步写（见方法注释：先写标记会让
	// 「标记在、acked 尾部丢」的窗口被误判为干净关机）
	var markErr error
	if err := m.rs.MarkCleanShutdown(); err != nil {
		markErr = err
		m.lg.Error("写入干净关机标记失败，本次关机按不干净处理", "err", err)
	} else {
		m.lg.Info("干净关机标记已写入")
	}
	return markErr
}

// kill 是测试后门：模拟进程宕机——取消运行 ctx 且不写干净关机标记，
// 同时停止后台刷盘 goroutine（否则 flusher 会在 store 关闭后继续提交）。
// 幂等：重复调用安全（StopClean 已停过时不再重复 close flusherStop）。
func (m *Manager) kill() {
	m.cancel()
	if m.flusherStop != nil {
		m.flusherStopOne.Do(func() { close(m.flusherStop) })
	}
}

// Done 返回一个在全部组与后台刷盘 goroutine 完全退出后关闭的 channel。
func (m *Manager) Done() <-chan struct{} {
	return m.doneCh
}

// Store 返回 Manager 持有的 store 实例。
//
// 为什么需要这个访问器：Rejoin 在编排内部重开 store（Wipe 后 pebble
// 句柄失效，原实例已关闭），返回的 Manager 持有新实例——main 装配
// meta/produce 等 core 组件必须拿这个新实例，而不是自己再开一份
// （同目录重复 Open 会撞 pebble 文件锁）。清理类调用方（测试）此前
// 直接读 m.st，导出后统一走本方法。
func (m *Manager) Store() *store.Store {
	return m.st
}

// GroupForQueue 返回 topic+queueID 归属的数据组号（1..DataGroups）。
//
// 入盘契约，永不可变——变更即存量数据错组，黄金值测试锁死。
// 哈希算法本体在包级函数 groupForQueue（snapshotstream 枚举与
// wipeGroupKeys 复用，无需 Manager 实例）；本方法只注入数据组数。
func (m *Manager) GroupForQueue(topic string, queueID uint32) uint32 {
	return groupForQueue(topic, queueID, m.dataGroups)
}

// Propose 向指定组提交一条提案并阻塞直到它在本节点 apply 完成
// （读己之写语义，见 group.propose）。
func (m *Manager) Propose(ctx context.Context, g uint32, batchRepr []byte) error {
	gr, ok := m.groups[g]
	if !ok {
		return fmt.Errorf("cluster: 未知数据组 %d（有效范围 [0, %d]）", g, m.Groups()-1)
	}
	return gr.propose(ctx, batchRepr)
}

// IsLeader 返回本节点当前是否为指定组的 leader。
func (m *Manager) IsLeader(g uint32) bool {
	gr, ok := m.groups[g]
	return ok && gr.isLeader()
}

// Leader 返回指定组当前 leader 的节点 ID；尚未选举完成时 ok=false。
func (m *Manager) Leader(g uint32) (nodeID uint64, ok bool) {
	gr, has := m.groups[g]
	if !has {
		return 0, false
	}
	id := gr.leader()
	return id, id != 0
}

// AppliedIndex 返回指定组当前已 apply 到 FSM 的最高日志 index。
//
// 快照发送侧的位点锚：snaps.Create(g, index) 登记视图时声明「视图内容
// = 该 index 时刻的 FSM 状态」（见 snapRegistry.Create 注释），调用方
// 先等提案 apply 再注册，二者配对即一致视图。未知组号返回 0（调用方
// 契约：只传有效组号）。
func (m *Manager) AppliedIndex(g uint32) uint64 {
	gr, ok := m.groups[g]
	if !ok {
		return 0
	}
	return gr.appliedIndex()
}

// Control 向指定节点发起一次控制通道 RPC（短连接，一次往返）。
//
// 生命周期：查 Peers 表拿地址 → 拨号 → 写请求帧（[op][payload]，
// 组号 ControlGroup）→ 读响应帧 → 关闭。独立短连接、不复用 raft
// 消息流——raft 流是单向流水（batch② 设计），控制是低频 RPC（join、
// 转发），交织会让低频请求被流水消息排队挤出队头阻塞；短连接也天然
// 免除了生命周期清理。
//
// 参数：
//   - ctx: 控制整个往返的时限——拨号（DialContext）、读写 deadline
//     均取 ctx；无 deadline 时读写不设限（连接关闭即返回错误）
//   - nodeID: 目标节点，必须存在于 Peers 表（否则报错，不发起连接）
//   - op: 控制操作码（0..0x7F，高位 0x80 为响应保留位）
//   - payload: 请求载荷
//
// 返回：
//   - handler 应答数据（成功路径）
//   - 错误信息：未知节点/拨号失败/响应协议错；handler 返回的错误会
//     以「控制调用失败: <文本>」的形式原样带回
//
// 注意：
//   - payload 过大（请求帧超过 16MiB 上限）时发送侧直接拒绝，不拨号
//   - 对端 ControlHandler 为 nil 时返回「控制通道未装配」错误
func (m *Manager) Control(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
	return controlCall(ctx, m.lg, m.peers, nodeID, op, payload)
}

// controlCall 向 nodeID 发起一次控制通道 RPC（短连接，一次往返）。
//
// 是 Manager.Control 与 Rejoin 编排（prepareJoinPoll）的公共实现——
// 线协议唯一实现点，帧布局见 ControlGroup 注释。Manager.Control 的
// 文档注释描述了完整契约；prepareJoinPoll 在节点尚无运行中 Manager
// 时复用同一实现发 PrepareJoin。
func controlCall(ctx context.Context, lg *slog.Logger, peers map[uint64]string, nodeID uint64, op byte, payload []byte) ([]byte, error) {
	addr, ok := peers[nodeID]
	if !ok {
		return nil, fmt.Errorf("cluster: 未知节点 %d（Peers 表无此节点）", nodeID)
	}
	// 发送侧帧长校验：请求帧 = 4B 帧长 + 4B 组号 + 1B op + payload，
	// 超过 maxFrameLen 直接拒绝——对端收帧同样限长，发了也白发。
	if 4+1+len(payload) > maxFrameLen {
		return nil, fmt.Errorf("cluster: 控制请求 payload %d B 超帧上限 %d B", len(payload), maxFrameLen)
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cluster: 控制连接节点 %d（%s）: %w", nodeID, addr, err)
	}
	defer conn.Close()
	if d, ok := ctx.Deadline(); ok {
		// 读写 deadline 均取 ctx：读响应时对端 handler 卡死也不会
		// 无限挂起，整个往返被 ctx 时限圈住
		_ = conn.SetDeadline(d)
	}
	// 请求帧 payload = [op][请求 payload]，复用信封帧编码
	ctrl := append([]byte{op}, payload...)
	if _, err := conn.Write(encodeFrame(nil, ControlGroup, ctrl)); err != nil {
		return nil, fmt.Errorf("cluster: 控制请求写节点 %d: %w", nodeID, err)
	}
	lg.Debug("控制请求已发送", "peer", nodeID, "op", op, "len", len(payload))
	// 响应帧同构：[4B 帧长][4B ControlGroup][1B op(响应位)][1B 状态][应答]
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("cluster: 读控制响应头节点 %d: %w", nodeID, err)
	}
	frameLen := binary.BigEndian.Uint32(header)
	// 最小合法响应帧 6 字节（4B 组号 + 1B op + 1B 状态）：<6 时
	// 下面 body[5] 会越界，坏对端可直接打崩调用方，必须防御。
	if frameLen < 6 || frameLen > maxFrameLen {
		return nil, fmt.Errorf("cluster: 节点 %d 控制响应帧长 %d 非法", nodeID, frameLen)
	}
	body := make([]byte, int(frameLen))
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, fmt.Errorf("cluster: 读控制响应体节点 %d: %w", nodeID, err)
	}
	if g := binary.BigEndian.Uint32(body[:4]); g != ControlGroup {
		return nil, fmt.Errorf("cluster: 节点 %d 控制响应组号 %d 异常（want %d）", nodeID, g, ControlGroup)
	}
	respOp := body[4]
	if respOp&0x80 == 0 {
		// 对端把请求帧回传（未置响应位）属协议错，不当作应答
		return nil, fmt.Errorf("cluster: 节点 %d 控制响应缺响应标记（op=0x%x）", nodeID, respOp)
	}
	status := body[5]
	respPayload := body[6:]
	lg.Debug("控制响应已收到", "peer", nodeID, "op", respOp&^0x80, "len", len(respPayload))
	switch status {
	case 0:
		return respPayload, nil
	case 1:
		// 失败：余下 payload 是对端 handler 的 UTF-8 错误文本，原样带回
		return nil, fmt.Errorf("cluster: 节点 %d 控制调用失败: %s", nodeID, string(respPayload))
	default:
		return nil, fmt.Errorf("cluster: 节点 %d 控制响应状态字节 %d 异常", nodeID, status)
	}
}

// Status 返回指定组的 raft 运行时状态（透传 rn.Status()）；组号非法
// 时 ok=false。学习者进度监控（Task 10）经 Status.Progress[nodeID].
// IsLearner 观测追平进度。
func (m *Manager) Status(g uint32) (raft.Status, bool) {
	gr, ok := m.groups[g]
	if !ok {
		return raft.Status{}, false
	}
	return gr.rn.Status(), true
}

// TransferLeader 请求把指定组的领导权转移给 to 节点。
//
// 摊布原语：异步请求，不等待完成（raft 自行处理交接）；自动摊布策略
// 属 batch③。
func (m *Manager) TransferLeader(g uint32, to uint64) {
	if gr, ok := m.groups[g]; ok {
		gr.rn.TransferLeadership(context.Background(), m.nodeID, to)
	}
}

// Groups 返回总组数：MetaGroup（组 0）+ DataGroups 个数据组。
func (m *Manager) Groups() uint32 {
	return 1 + m.dataGroups
}

// ProposeConfChange 向指定组提出一条成员变更并阻塞直到它被 apply。
//
// 重入编排原语：本批由测试 harness 驱动（Remove 旧 voter → AddLearner
// → 追平 → AddNode）；生产编排属 batch③。
//
// Remove 应用完成后排空被移除节点的传输队列：raft 侧 Progress 已删除
// 不再产生新消息，但队列里变更前的心跳等积压仍携带旧 commit 索引，
// 对端以空/短日志重入（learner 重入流程）收到即触发 raft 库
// "tocommit out of range" panic——Task 7 三节点集成测试抓到的缺口。
// 趁节点离线排空是确定的（离线时无在途写，见 transport.DropPeer）。
func (m *Manager) ProposeConfChange(ctx context.Context, g uint32, typ raftpb.ConfChangeType, nodeID uint64) error {
	gr, ok := m.groups[g]
	if !ok {
		return fmt.Errorf("cluster: 未知数据组 %d（有效范围 [0, %d]）", g, m.Groups()-1)
	}
	if err := gr.proposeConfChange(ctx, typ, nodeID); err != nil {
		return err
	}
	if typ == raftpb.ConfChangeRemoveNode && m.tr != nil {
		m.tr.DropPeer(nodeID)
	}
	return nil
}

// handlePrepareJoin 处理 OpPrepareJoin 控制请求（Manager 自装，见
// NewManager）：payload=[8B BE nodeID]，响应=本节点完成 Remove→
// AddLearner 的组号列表 [4B BE n][n × 4B BE g]。
//
// 对**本节点当前 lead 的每个组**顺序执行 Remove→AddLearner（30s 时限，
// 幂等报错忽略见 rejoinGroupPrep），非本节点 lead 的组不动——请求方
// （Rejoin 编排）轮询全部对端收并集，天然免疫 leader 换手。
//
// 串行化：prepareJoinMu 保证同节点同时只有一个 PrepareJoin 在处理——
// 请求方超时会开新连接重发，两个 handler 并发提成员变更会被 raft 的
// pendingConfIndex 校验静默替换成空条目、等待超时，并集收敛被无谓拖慢。
//
// 注意：本 handler 在传输层读循环内同步执行、逐组阻塞至 apply——只
// 阻塞本连接（控制通道短连接 RPC，一次往返），其余连接与各组 Ready
// 循环不受影响；请求方断开时变更照常完成（幂等，下次轮询重发无害）。
func (m *Manager) handlePrepareJoin(payload []byte) ([]byte, error) {
	if len(payload) != 8 {
		return nil, fmt.Errorf("cluster: PrepareJoin 载荷须为 8B BE nodeID，收到 %d B", len(payload))
	}
	nodeID := binary.BigEndian.Uint64(payload)
	if nodeID == 0 {
		return nil, errors.New("cluster: PrepareJoin 的 nodeID 不能为 0")
	}
	m.prepareJoinMu.Lock()
	defer m.prepareJoinMu.Unlock()
	m.lg.Info("PrepareJoin 请求到达：节点重入编排", "rejoin", nodeID)
	done := make([]uint32, 0, m.Groups())
	for g := uint32(0); g < m.Groups(); g++ {
		if !m.IsLeader(g) {
			continue // 非本节点 lead：请求方轮询其他对端补齐
		}
		if err := m.rejoinGroupPrep(g, nodeID); err != nil {
			// 本轮不标记完成：请求方并集缺该组会重发，幂等补齐。
			// 换 leader 时新 leader 会处理，无需本节点重试。
			m.lg.Warn("PrepareJoin 组编排未完成，本轮不标记", "g", g, "rejoin", nodeID, "err", err)
			continue
		}
		done = append(done, g)
	}
	// 响应布局：[4B BE n][n × 4B BE g]，组号升序（循环天然升序）
	resp := make([]byte, 4+4*len(done))
	binary.BigEndian.PutUint32(resp[:4], uint32(len(done)))
	for i, g := range done {
		binary.BigEndian.PutUint32(resp[4+4*i:], g)
	}
	m.lg.Info("PrepareJoin 编排完成", "rejoin", nodeID, "groups", done)
	return resp, nil
}

// rejoinGroupPrep 对单个组执行 learner 重入的 Remove→AddLearner 两步
// 成员变更（每步 5s 时限），两步都 apply 才算完成。
//
// 时限为什么取 5s 而非更宽松：提案已入日志但未 commit 时若 leader 换人
// （摊布转移/选举），raft 静默截断该条目、waiter 永不通知——propose 只
// 能等 ctx 超时收场（无 leader 变更的早醒机制）。超时太长会让一次换手
// 撞车吃满整个 PrepareJoin 轮询预算（30s），而重入幂等决定了「快速
// 失败 + 请求方重发」是更优的收敛路径：新 leader 收到重发后正常完成。
func (m *Manager) rejoinGroupPrep(g uint32, nodeID uint64) error {
	rmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := m.ProposeConfChange(rmCtx, g, raftpb.ConfChangeRemoveNode, nodeID)
	cancel()
	if err != nil {
		// 幂等重放：请求方轮询重发时，nodeID 可能已不在成员表（上一轮
		// 已完成 Remove，或对端早先已处理过本组）——raft 对移除非成员
		// 的变更静默 no-op，这里按成员表实况判定：不在表即幂等成功，
		// 继续 AddLearner；在表却失败（超时/换 leader）是真错误，上报
		if m.memberHas(g, nodeID) {
			return fmt.Errorf("组 %d 移除节点 %d: %w", g, nodeID, err)
		}
		m.lg.Debug("PrepareJoin 移除幂等跳过", "g", g, "node", nodeID, "err", err)
	}
	addCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.ProposeConfChange(addCtx, g, raftpb.ConfChangeAddLearnerNode, nodeID)
}

// memberHas 判定 nodeID 是否在当前成员表（voter 或 learner，含 joint
// 双侧）——幂等判定与成员实况校验共用。Status.Config 即 ConfState 的
// 运行时形态，全节点填充（不只在 leader 上）。
func (m *Manager) memberHas(g uint32, nodeID uint64) bool {
	st, ok := m.Status(g)
	if !ok {
		return false
	}
	for _, half := range st.Config.Voters {
		if _, ok := half[nodeID]; ok {
			return true
		}
	}
	_, ok = st.Config.Learners[nodeID]
	return ok
}

// snapshotChunkKeys 是单块键数的兜底上限：正常块由字节预算
// （Options.SnapshotChunkBytes）收口，键数预算只在极端小键场景（如
// 全空值）下兜底，防止单块键数无界。scanGroupKeys 的 budget 参数必须
// 为正，故传本常量。
const snapshotChunkKeys = 1 << 20

// errChunkBudgetHit 是 handleFetchSnapshot 的 emit 收口哨兵：块字节数
// 已达字节预算（Options.SnapshotChunkBytes），提前结束本块枚举。必须与真实扫描错误（坏
// 键/迭代器错误）区分——调用方用 errors.Is 判定后按「块满、游标续扫」
// 处理，其余错误原样上抛（快照漏键 = 静默丢数据，不得吞）。
var errChunkBudgetHit = errors.New("cluster: 快照块字节预算已满")

// handleFetchSnapshot 处理 OpFetchSnapshot 控制请求（Manager 自装，
// 见 NewManager 的 controlHandler）：按 snapID 取注册表里钉住的
// ReadView，从游标键续扫组 g 的键族，分块返回。
//
// 请求：[4B BE 组][8B BE snapID][4B BE 游标键长][游标键]
// 响应：[1B 是否结束(1=完)][4B BE 下一游标键长][下一游标键][块字节]
//   - 块字节 = encodeChunk 输出（[4B 键长][键][4B 值长][值] 重复），
//     接收方 decodeChunk 还原后逐键 Store.Apply
//   - done=1 后不得再续拉；done=0 时以「下一游标键」发起下一块请求
//
// 块大小双阈值：字节预算 m.snapshotChunkBytes（默认 4MiB，
// Options.SnapshotChunkBytes 配置）与传输帧上限 maxFrameLen（16MiB，
// transport.go）共同约束一块的体积。emit 累计到字节预算即提前收口
// （块最多 = 预算 + 一对把预算顶穿的键值）；单键值对的上界由 FSM 写
// 路径约束——最大的值是消息体，上限 4MiB（见 core/produce 的
// MaxBodySize，其余键族远小于此），默认预算下最坏一块 ≈ 4MiB + 4MiB
// ≈ 8MiB，离 16MiB 帧上限还有一倍余量（配置面把预算上界钉在 16MiB
// 之下，见 config.go 的 snapshot_chunk_bytes 校验）。即便未来出现超过
// 单块预算的病理键值，本层也只多放进一块、不会突破帧上限；而超帧的
// 响应会被传输层双端拒收（坏帧断连，见 readLoop），不会静默截断。
//
// 注册表借用语义：Get 借出视图并续期（refs+1、刷新 TTL 基线 created），
// 本方法用 defer Put 归还——视图归注册表所有，借出窗口内 GCOnce/Release
// 不会 Close 它（refs>0 跳过），活跃拉取（每块一次 Get）不断续期，
// 传输窗口超过固定 5min 不再中途回收。旧的「Get 不续期」语义在大库
// 传输上活锁：视图被回收 → 对端拿到未知 snapID → 重试新快照 → 再
// 回收。
//
// 注意：
//   - 本 handler 在传输层读循环内同步执行，纯读视图枚举、不阻塞；
//     借用只须覆盖 scanGroupKeys 的扫描窗口，defer Put 顺带覆盖到
//     函数返回
//   - 请求方身份由传输层 controlFrame 的「控制请求处理失败」Warn 补齐
//     （带 remote）——handler 签名不带远端信息，两者在日志里按 snap_id
//     配对
func (m *Manager) handleFetchSnapshot(payload []byte) ([]byte, error) {
	if len(payload) < 16 {
		return nil, fmt.Errorf("cluster: FetchSnapshot 载荷须 ≥16B（组+snapID+游标长），收到 %d B", len(payload))
	}
	g := binary.BigEndian.Uint32(payload[:4])
	id := binary.BigEndian.Uint64(payload[4:12])
	cl := binary.BigEndian.Uint32(payload[12:16])
	if uint32(len(payload)-16) < cl {
		return nil, fmt.Errorf("cluster: FetchSnapshot 游标键长 %d 超出载荷 %d B", cl, len(payload)-16)
	}
	cursor := payload[16 : 16+cl]
	if g > m.dataGroups {
		return nil, fmt.Errorf("cluster: FetchSnapshot 组号 %d 越界（有效范围 [0, %d]）", g, m.dataGroups)
	}
	view, ok := m.snaps.Get(id)
	if !ok {
		// 区分「没生成过」与「已回收」：snapID 单调自增、永不复用，
		// id ≤ nextID 说明曾分配过（已释放或 TTL 回收——过期是常见
		// 原因），否则请求方拿到的是从未存在过的陈旧描述符
		if m.snaps.WasCreated(id) {
			m.lg.Warn("FetchSnapshot 未知 snapID：视图已释放或超时回收（TTL 过期是常见原因，对端应重试新快照）",
				"g", g, "snap_id", id)
		} else {
			m.lg.Warn("FetchSnapshot 未知 snapID：从未生成过（请求方持有陈旧描述符）",
				"g", g, "snap_id", id)
		}
		return nil, fmt.Errorf("cluster: FetchSnapshot 未知 snapID %d（组 %d）", id, g)
	}
	defer m.snaps.Put(id) // 归还借用：视图归注册表所有，借出仅覆盖本次扫描窗口
	// 首块（游标为空）打 Info：一次快照传输的起止是排查追齐耗时的基本单位
	if len(cursor) == 0 {
		m.lg.Info("快照拉取开始", "g", g, "snap_id", id)
	}
	var pairs []kv
	bytes := 0
	emit := func(k, v []byte) error {
		// 键值必须拷贝：扫描回调的底层内存仅回调期间有效（见
		// store.ReadView.Scan 注释），encodeChunk 在扫描结束后才执行，
		// 不能持有引用
		pairs = append(pairs, kv{k: append([]byte(nil), k...), v: append([]byte(nil), v...)})
		bytes += 8 + len(k) + len(v) // 8B = 块格式里的双长度头（见 encodeChunk）
		if bytes >= m.snapshotChunkBytes {
			return errChunkBudgetHit // 提前收口：游标由 scanGroupKeys 置为最后发出的键
		}
		return nil
	}
	next, done, err := scanGroupKeys(view, g, m.dataGroups, cursor, snapshotChunkKeys, emit)
	if errors.Is(err, errChunkBudgetHit) {
		done, err = false, nil // 块满而非扫描错误：按「块满、游标续扫」收口
	}
	if err != nil {
		return nil, fmt.Errorf("cluster: FetchSnapshot 组 %d 枚举: %w", g, err)
	}
	chunk := encodeChunk(pairs)
	resp := make([]byte, 0, 5+len(next)+len(chunk))
	if done {
		resp = append(resp, 1)
	} else {
		resp = append(resp, 0)
	}
	resp = binary.BigEndian.AppendUint32(resp, uint32(len(next)))
	resp = append(resp, next...)
	resp = append(resp, chunk...)
	m.lg.Debug("快照块已发送", "g", g, "snap_id", id, "keys", len(pairs), "bytes", len(chunk), "done", done)
	if done {
		m.lg.Info("快照拉取完成", "g", g, "snap_id", id, "keys", len(pairs))
	}
	return resp, nil
}

// WipeForRejoin 清空整个数据目录，是 learner 重入的前置动作。
//
// 注意：
//   - 必须先 Close store 再调用——pebble 持有目录文件句柄，不关闭
//     直接 RemoveAll 会失败
//   - 调用后需以全新 store+Manager 走 fresh 路径启动，本节点的身份
//     由存活 leader 的 ConfChange（Remove 旧 voter → AddLearner →
//     追平 → AddNode）赋予
func WipeForRejoin(dataDir string) error {
	if err := os.RemoveAll(dataDir); err != nil {
		return fmt.Errorf("cluster WipeForRejoin 清空 %s: %w", dataDir, err)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("cluster WipeForRejoin 重建 %s: %w", dataDir, err)
	}
	return nil
}

// Rejoin 把断电节点以 learner 身份自动接回集群——harness 六步编排
// （cluster_test.go 的 rejoinAsLearner）的生产入口，编排语义与该行为
// 规范对齐，不得私自偏离。
//
// 六步：
//  1. 旧 store 已关闭——调用方职责（pebble 持有目录句柄，不关闭
//     WipeForRejoin 清不掉；本函数不代关，签名约定即此）
//  2. WipeForRejoin 清空数据目录（含旧 raft 日志——身份由存活
//     leader 的 ConfChange 日志重赋）
//  3. 轮询全部对端发 PrepareJoin：每端对**自己当前 lead** 的组做
//     Remove→AddLearner 并返回完成组号，收齐 0..DataGroups 全组完成
//     （30s 总时限；期间 leader 可能换手——重发即可，Remove/AddLearner
//     幂等，见 handlePrepareJoin/rejoinGroupPrep）
//  4. 重建本节点监听（EADDRINUSE 抢占，逻辑与注释同 harness）：kill
//     后 Done 不保证旧传输层看门狗已关监听器，直接重绑同一地址可能
//     撞 EADDRINUSE；先 net.Listen 抢到端口（拿到即旧监听器已关的
//     确定性证明），再注入给 NewManager——中间无空窗
//  5. store.Open + NewManager fresh 路径启动（Wipe 后仍报
//     ErrUncleanShutdown 即判定错误，显式报错）+ Start
//  6. 追平与升 voter 交给 leader 侧 AutoPromoteLearners 自动循环
//     （本函数返回时节点尚是 learner，收敛由集群自动完成）
//
// 幂等性：任一步失败可整体重跑——Wipe 幂等（删了再建）、PrepareJoin
// 幂等（已完成的组重发只是 Remove no-op + AddLearner 幂等）、fresh
// 启动可重试（空目录永远 fresh 判定）。
//
// 注意：
//   - Options.Store 与 Options.Listener 被忽略：本函数从 dataDir 重开
//     store、从 Peers[NodeID] 重建监听，调用方不得传入复用实例
//   - 升 voter 依赖 Options.AutoPromoteLearners + LeaderBalancerInterval
//     （leader 侧注入，见 Options 字段注释）
//   - 本节点自身不自动清目录——Wipe 是破坏性动作，main（Task 11）在
//     检测到 ErrUncleanShutdown 后应先打日志留痕再调用本函数
func Rejoin(ctx context.Context, o Options, dataDir string) (*Manager, error) {
	if o.DataGroups == 0 {
		o.DataGroups = 3 // 与 NewManager 同一默认
	}
	lg := o.Logger
	if lg == nil {
		lg = slog.Default()
	}
	start := time.Now()
	// 第 1 步：旧 store 已关闭（调用方职责，见函数注释）
	lg.Info("重入编排: 第 1 步 旧 store 已关闭", "node", o.NodeID, "dir", dataDir)

	// 第 2 步：清空状态目录（幂等：整体重跑安全）
	if err := WipeForRejoin(dataDir); err != nil {
		return nil, err
	}
	lg.Info("重入编排: 第 2 步 状态目录已清空", "dir", dataDir, "duration", time.Since(start).Round(time.Millisecond))

	// 第 3 步：PrepareJoin 全组编排（30s 总时限，leader 换手重发幂等）
	if err := prepareJoinPoll(ctx, lg, o); err != nil {
		return nil, fmt.Errorf("cluster: 重入编排第 3 步 PrepareJoin: %w", err)
	}

	// 第 4 步：重建本节点监听（EADDRINUSE 抢占，注释同 harness——
	// kill 后旧传输层看门狗关监听器与 Done 不同步，直接重绑同地址
	// 可能撞 EADDRINUSE；抢到端口=旧监听器已关的确定性证明）。
	// 绑定地址语义同 NewManager：ListenAddr 优先（NAT/容器下与
	// Peers[NodeID] 的通告地址分离），空回退 Peers[NodeID]。
	bind := o.ListenAddr
	if bind == "" {
		bind = o.Peers[o.NodeID]
	}
	var ln net.Listener
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		l, err := net.Listen("tcp", bind)
		if err == nil {
			ln = l
			break
		}
		if errors.Is(err, syscall.EADDRINUSE) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return nil, fmt.Errorf("cluster: 重入编排重建监听 %s: %w", bind, err)
	}
	if ln == nil {
		return nil, fmt.Errorf("cluster: 重入编排旧监听器未在 10s 内关闭，无法重绑 %s", bind)
	}
	lg.Info("重入编排: 第 4 步 监听重建完成", "node", o.NodeID, "addr", bind, "duration", time.Since(start).Round(time.Millisecond))

	// 第 5 步：新 store + NewManager fresh 路径启动 + Start
	st, err := store.Open(dataDir, false, lg)
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("cluster: 重入编排重开 store: %w", err)
	}
	o.Store = st
	o.Listener = ln
	m, err := NewManager(o)
	if err != nil {
		st.Close()
		ln.Close()
		if errors.Is(err, ErrUncleanShutdown) {
			// Wipe 后仍判不干净 = fresh 判定被破坏，显式报错而非静默恢复
			return nil, fmt.Errorf("cluster: 重入编排 WipeForRejoin 后仍报 ErrUncleanShutdown: %w", err)
		}
		return nil, err
	}
	m.Start(ctx)

	// 第 6 步：追平与升 voter 交给 leader 侧 AutoPromoteLearners 循环
	lg.Info("重入编排: 第 5/6 步 完成——fresh 启动，追平与升 voter 由 leader 侧自动循环接管",
		"node", o.NodeID, "autoPromote", o.AutoPromoteLearners, "duration", time.Since(start).Round(time.Millisecond))
	return m, nil
}

// prepareJoinPoll 是 Rejoin 的第 3 步：轮询全部对端发 OpPrepareJoin，
// 收齐 0..DataGroups 全组完成的并集（30s 总时限）。
//
// 为什么轮询而非直连已知 leader：各组 leader 由摊布分布在多个节点上、
// 且编排期间可能换手——轮询全部对端、每端只处理自己当前 lead 的组、
// 请求方收并集，天然免疫换手；Remove/AddLearner 幂等使重发安全
// （已完成的组重发只是 no-op）。
//
// 对端不可达（宕机/未起）只记 Warn 不失败：它 lead 的组会由其余对端
// 处理，下轮重试补齐。
//
// 单节点集群（peers 只有自己）特判：没有任何对端可轮询，need 集合
// 永远不会被消减，裸走循环必然 30s 超时失败。对单节点而言 PrepareJoin
// 的 Remove→AddLearner 语义本就没有意义（成员表里只有自己，无从
// 「移除再以 learner 加回」）——Wipe + fresh 启动就是完整的恢复路径，
// 直接返回 nil 跳过编排（三节点 e2e 补的回归：单节点 kill -9 重启
// 必须能自愈，见 manager_test.go）。
func prepareJoinPoll(ctx context.Context, lg *slog.Logger, o Options) error {
	peerIDs := sortedPeerIDs(o.Peers)
	if len(peerIDs) == 1 && peerIDs[0] == o.NodeID {
		lg.Info("PrepareJoin 跳过：单节点集群无对端可编排，Wipe + fresh 启动即完整恢复",
			"node", o.NodeID)
		return nil
	}
	// payload = [8B BE nodeID]
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, o.NodeID)
	need := make(map[uint32]bool, o.DataGroups+1)
	for g := uint32(0); g <= o.DataGroups; g++ {
		need[g] = true
	}
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()
	for {
		for _, peer := range peerIDs {
			if peer == o.NodeID {
				continue // 不轮询自己：本节点尚无运行中的 Manager
			}
			if len(need) == 0 {
				break
			}
			callCtx, cancel := context.WithTimeout(pollCtx, 15*time.Second)
			resp, err := controlCall(callCtx, lg, o.Peers, peer, OpPrepareJoin, payload)
			cancel()
			if err != nil {
				lg.Warn("PrepareJoin 轮询失败，下轮重试", "peer", peer, "err", err)
				continue
			}
			// 响应 = [4B BE n][n × 4B BE g]，长度校验防坏对端
			if len(resp) < 4 || len(resp) != 4+int(binary.BigEndian.Uint32(resp[:4]))*4 {
				lg.Warn("PrepareJoin 响应布局非法，忽略", "peer", peer, "len", len(resp))
				continue
			}
			n := binary.BigEndian.Uint32(resp[:4])
			var groups []uint32
			for i := uint32(0); i < n; i++ {
				g := binary.BigEndian.Uint32(resp[4+4*i:])
				if g > o.DataGroups {
					lg.Warn("PrepareJoin 响应组号越界，忽略", "peer", peer, "g", g)
					continue
				}
				if need[g] {
					delete(need, g)
					groups = append(groups, g)
				}
			}
			if len(groups) > 0 {
				lg.Info("PrepareJoin 对端完成组", "peer", peer, "groups", groups, "remaining", len(need))
			}
		}
		if len(need) == 0 {
			lg.Info("PrepareJoin 全组就绪", "node", o.NodeID, "duration", time.Since(start).Round(time.Millisecond))
			return nil
		}
		select {
		case <-pollCtx.Done():
			missing := make([]uint32, 0, len(need))
			for g := range need {
				missing = append(missing, g)
			}
			sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
			return fmt.Errorf("cluster: PrepareJoin 30s 超时，组 %v 仍未完成（对端已重试多轮）", missing)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// storeHasKeys 判定已打开的 store 是否非空（任何键存在即非空）。
// Join 用它做「数据目录为空」的内容判定——目录句柄由调用方持有，
// Join 不接触文件系统（签名不带 dataDir，见 Join 注释）。
func storeHasKeys(st *store.Store) (bool, error) {
	nonEmpty := false
	err := st.Scan(nil, nil, 1, func(k, v []byte) (bool, error) {
		nonEmpty = true
		return false, nil
	})
	if err != nil {
		return false, err
	}
	return nonEmpty, nil
}

// Join 把全新节点（空数据目录）以 learner 身份加入既有集群——spec §7
// 「单机 → 集群平滑升级」的入口：新节点靠存活 leader 的快照追平存量
// 数据，追平后由 leader 侧 AutoPromoteLearners 循环自动升 voter。
//
// 与 Rejoin 的语义分界（为什么两者必须分开）：
//   - Rejoin（断电重入）：节点**已在**集群成员表里，目录里有旧状态
//     （可能是不干净关机残留），必须先 WipeForRejoin 清空再 fresh 启动
//   - Join（新加入）：节点**不在**成员表里，目录必须本来就是空的——
//     learner 身份由 leader 侧 PrepareJoin 的 Remove→AddLearner 赋予
//
// 为什么 Join 拒绝非空目录：加入成功后 leader 的快照安装会整体清空本
// 节点目标组的全部键（wipeGroupKeys，Task 7）——目录里若有存量数据
// （哪怕是单机档的 FSM 数据），会被静默抹掉。这种混用本质是操作意图
// 错位（该走 Rejoin 的走了 Join），必须显式报错指向 Rejoin，而不是让
// 快照安装把数据悄悄清掉。
//
// 扩容前提（生产必读，运维指引见 B8.3）：新节点靠快照追齐的前提是
// 种子日志已压缩到新节点起点（index 0）之下。日志未压缩时 raft 的
// 追齐探针锚在「假想条目 index-1」（未压缩日志 term(0)=0 与空日志
// follower 恰好匹配），新节点走**日志重放**——而单机档的 FSM 存量
// 数据根本不在 raft 日志里（当时直写 FSM），重放带不过去、快照也不
// 触发：种子写入量不足约 2×RetainEntries（默认 10000 → 约 2 万+ 条）
// 就 Join 的扩容会**静默丢失**单机档存量数据（不是报错，是数据缺失）。
// e2e 用 retain=1 + 多组 marker 把种子日志压过这一前提再 Join
// （cluster_expand_test.go ②b 注释，逐条解释了判定路径）；生产扩容
// 请先让种子写入越过该量、等截断循环把日志压住（约 30s 一轮）再 Join。
//
// 编排（与 Rejoin 的后半程同构，差别只在前置条件）：
//  1. 校验数据目录为空——经 Options.Store 内容判定（零键 = 空）。Join
//     签名不带 dataDir，目录句柄由调用方先 store.Open 后经 Store 传入，
//     与 Rejoin「调用方持句柄」的职责一致；失败时 store 仍归调用方
//  2. 轮询 seedPeers 发 PrepareJoin，收齐 0..DataGroups 全组 AddLearner
//     完成——seedPeers 是**可达种子**子集（不必全量成员表），但全部组
//     的 leader 必须落在种子集合里，否则该组永远等不到完成、30s 超时
//     （调用方职责：种子覆盖全部组的 leader）
//  3. 空白启动（强制 Options.NoBootstrap=true）+ Start：不引导成员表
//     ——带引导的 StartNode 会与 leader 日志 (index, term) 撞车、快照
//     永不触发，存量数据（单机档 FSM）永远追不上（根因与「重放 vs
//     快照」的判定见 buildGroup fresh 分支注释）；空白启动消除撞车，
//     追齐走 raft 标准判定（未压缩日志 = 重放，压缩日志 = MsgSnap）
//  4. 返回；追平与升 voter 由 leader 侧 AutoPromoteLearners 自动完成
//     ——落后到日志之外时有 Task 7 的快照兜底（batch③ 时这条路走不通，
//     正是本批解的问题）
//
// 参数：
//   - ctx: 控制 PrepareJoin 轮询的时限（轮询内部另有 30s 总时限）
//   - o: Options，必须携带已打开的 Store 与完整成员表 Peers（含本节点
//     地址；NewManager 与 peerAddrs 广播都需要）。Listener 可选：nil
//     时按 ListenAddr/Peers[NodeID] 绑定（与 NewManager 同规则）
//   - seedPeers: 本次轮询可达的种子节点（id → 监听地址）子集；空或只
//     有本节点自己（自举语义，非加入语义）直接报错
//
// 返回：
//   - 已启动的 Manager（成功时接管 store 所有权；调用方负责 StopClean）
//   - 错误信息（带步骤名）；失败时 store 仍归调用方（可重试或自行关闭）
func Join(ctx context.Context, o Options, seedPeers map[uint64]string) (*Manager, error) {
	if o.Store == nil {
		return nil, errors.New("cluster: Join 要求 Options.Store 已打开（调用方先 store.Open 数据目录）")
	}
	if o.DataGroups == 0 {
		o.DataGroups = 3 // 与 NewManager 同一默认
	}
	lg := o.Logger
	if lg == nil {
		lg = slog.Default()
	}
	start := time.Now()

	// 第 1 步：数据目录必须为空（零键）。拒绝语义与错误指向见函数注释
	// ——「重入」与「新加入」是两个语义，混用会被快照安装静默清数据。
	nonEmpty, err := storeHasKeys(o.Store)
	if err != nil {
		return nil, fmt.Errorf("cluster: 加入编排第 1 步 校验空目录: %w", err)
	}
	if nonEmpty {
		return nil, errors.New("cluster: Join 拒绝非空数据目录——「新加入」只接受空目录；本节点若曾在此目录运行过（断电重入），请关闭 store 后改用 Rejoin（它会清空目录并重赋身份）；混用会令快照安装静默清掉存量数据")
	}
	lg.Info("加入编排: 第 1 步 数据目录为空校验通过", "node", o.NodeID, "duration", time.Since(start).Round(time.Millisecond))

	// 第 2 步：PrepareJoin 全组编排——只轮询可达种子（o.Peers 是完整
	// 成员表，NewManager 需要；轮询视图单独用 seedPeers 子集，见
	// prepareJoinPoll 的对端枚举语义）。
	if len(seedPeers) == 0 {
		return nil, errors.New("cluster: 加入编排第 2 步: seedPeers 为空——至少需要一个存活成员作为种子")
	}
	if _, onlySelf := seedPeers[o.NodeID]; onlySelf && len(seedPeers) == 1 {
		return nil, errors.New("cluster: 加入编排第 2 步: seedPeers 只有本节点自己——Join 需要至少一个**存活成员**作为种子（单节点自举请直接 NewManager）")
	}
	pollOpts := o
	pollOpts.Peers = seedPeers
	if err := prepareJoinPoll(ctx, lg, pollOpts); err != nil {
		return nil, fmt.Errorf("cluster: 加入编排第 2 步 PrepareJoin: %w", err)
	}
	lg.Info("加入编排: 第 2 步 PrepareJoin 完成",
		"node", o.NodeID, "groups", o.DataGroups+1, "duration", time.Since(start).Round(time.Millisecond))

	// 第 3 步：NewManager 空白启动（NoBootstrap 强制置位，理由见函数
	// 注释与 buildGroup fresh 分支——带成员表引导 = 快照永不触发）+ Start
	o.NoBootstrap = true
	m, err := NewManager(o)
	if err != nil {
		if errors.Is(err, ErrUncleanShutdown) {
			return nil, fmt.Errorf("cluster: 加入编排第 3 步: 空目录仍报 ErrUncleanShutdown: %w", err)
		}
		return nil, fmt.Errorf("cluster: 加入编排第 3 步 NewManager: %w", err)
	}
	m.Start(ctx)

	// 第 4 步：追平与升 voter 交给 leader 侧 AutoPromoteLearners 循环
	// （落后到日志之外时由 Task 7 的快照兜底——正是本批要解的问题）
	lg.Info("加入编排: 第 3/4 步 完成——fresh 启动，追平与升 voter 由 leader 侧自动循环接管",
		"node", o.NodeID, "autoPromote", o.AutoPromoteLearners, "duration", time.Since(start).Round(time.Millisecond))
	return m, nil
}
