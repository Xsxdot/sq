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
//
// 边界：
//   - 不自动编排 learner 重入——原语给全（WipeForRejoin、
//     ProposeConfChange、TransferLeader、GroupForQueue），重入编排
//     属 batch③
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
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"os"
	"sort"
	"sync"
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
// Peers 是全体节点 id → raft 监听地址（含本节点）；本节点地址仅在本
// 节点自身未注入 Listener 时用于监听，不参与传输拨号（本节点消息短路）。
type Options struct {
	NodeID     uint64            // 本节点 id（1..3）
	Peers      map[uint64]string // 全体节点 id → raft 监听地址（含本节点）
	Listener   net.Listener      // 可选：测试注入已建监听（nil 则按 Peers[NodeID] 监听）
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
	ln         net.Listener // 本节点监听：NewManager 持有，Start 移交传输层
	tr         *transport   // Start 时装配；send 回调运行时取值，必已就绪

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
}

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
		controlHandler:         o.ControlHandler,
		leaderBalancerInterval: o.LeaderBalancerInterval,
		doneCh:                 make(chan struct{}),
	}
	m.rs = newRaftStore(o.Store, lg)

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
	// 条目）；干净路径不传成员表，身份由日志回放恢复
	peers := make([]raft.Peer, 0, len(o.Peers))
	for _, id := range sortedPeerIDs(o.Peers) {
		peers = append(peers, raft.Peer{ID: id})
	}
	for g := uint32(0); g <= o.DataGroups; g++ {
		gr, err := m.buildGroup(g, clean, peers)
		if err != nil {
			return nil, err
		}
		m.groups[g] = gr
	}

	// 本节点监听：测试注入的 Listener 直接复用；否则按 Peers[NodeID]
	// 监听。放在恢复判定之后——ErrUncleanShutdown 路径不得泄漏监听器。
	if o.Listener != nil {
		m.ln = o.Listener
	} else {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("cluster: 本节点 %d 监听 %s: %w", o.NodeID, addr, err)
		}
		m.ln = ln
	}

	m.lg.Info("集群管理器初始化", "node", o.NodeID, "groups", m.Groups(), "dataGroups", o.DataGroups, "mode", o.Mode.String(), "recovery", recovery)
	return m, nil
}

// buildGroup 构造单个 raft 组，按恢复路径分叉：
//
//   - clean：rs.Load 全量回放磁盘日志重建 MemoryStorage（合成快照元数据
//     提供 ConfState 与起始位点），raft.RestartNode 恢复，且
//     raftConfig.Applied = 磁盘 applied（raft 从 applied+1 重投递，
//     配合 group 的跳过逻辑双保险，见下述 seeding 注释）
//   - fresh：raft.StartNode 以引导成员表启动
func (m *Manager) buildGroup(g uint32, clean bool, peers []raft.Peer) (*group, error) {
	storage := raft.NewMemoryStorage()
	applied := uint64(0)
	var rn raft.Node
	if clean {
		hs, ents, err := m.rs.Load(g)
		if err != nil {
			return nil, fmt.Errorf("cluster: 组 %d 恢复读取: %w", g, err)
		}
		if len(ents) > 0 {
			first := ents[0]
			snapIndex := first.GetIndex() - 1
			// snapTerm 用首条条目的 term 近似「缺失条目 index-1 的 term」：
			// raft 只在任期比较时用到它，而两条路径都不会触发该比较——
			// 不干净路径空存储启动（snapTerm=0）只与新鲜 leader 的 term(0)
			// 相遇；干净路径 leader 的 Progress 仍保留，追齐从不会回退到
			// index-1 对账（若未来做快照压缩，需按真实 term 补快照，见 B8.2）
			snapTerm := first.GetTerm()
			cs, err := confStateFromEntries(ents, hs.GetCommit())
			if err != nil {
				return nil, fmt.Errorf("cluster: 组 %d 成员表合成: %w", g, err)
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
		cfg := raftConfig(m.nodeID, storage)
		cfg.Applied = applied
		rn = raft.RestartNode(cfg)
	} else {
		rn = raft.StartNode(raftConfig(m.nodeID, storage), peers)
	}
	gr := newGroup(g, rn, storage, m.rs, m.st, m.send, m.mode, m.onLeaderChange, m.onApplied, m.lg)
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
	// Done 观察者：全部组退出且 flusher 停止后关闭 doneCh
	go func() {
		for _, gr := range m.groups {
			<-gr.done()
		}
		if m.flusherDone != nil {
			<-m.flusherDone
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

// GroupForQueue 返回 topic+queueID 归属的数据组号（1..DataGroups）。
//
// 入盘契约，永不可变——变更即存量数据错组，黄金值测试锁死。
// 算法：fnv1a(topic 字节 + 4B 大端 queueID) 对数据组数取模后偏移到
// [1, DataGroups]；MetaGroup（0）不参与映射。
func (m *Manager) GroupForQueue(topic string, queueID uint32) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], queueID)
	_, _ = h.Write(buf[:])
	return 1 + h.Sum32()%m.dataGroups
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
	addr, ok := m.peers[nodeID]
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
	m.lg.Debug("控制请求已发送", "peer", nodeID, "op", op, "len", len(payload))
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
	m.lg.Debug("控制响应已收到", "peer", nodeID, "op", respOp&^0x80, "len", len(respPayload))
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
