// manager.go 提供多组装配层：全部 raft 组（MetaGroup + 数据组）共享
// 一个 store 库与一条 TCP 传输链路的组装、组路由、生命周期与重启恢复
// 判定。
//
// 职责：
//   - 组装配：全组创建——fresh 路径 StartNode 引导 / 干净关机路径
//     RestartNode 原身份回归（磁盘日志回放进 MemoryStorage）
//   - 组路由：队列→组映射（入盘契约，GroupForQueue）与传输消息按组投递
//   - 生命周期：Start 起全部组 + 传输层 + mem 档后台批量刷盘；
//     StopClean 停全部机件后最后写干净关机标记；Done 等完全退出
//   - 重启恢复判定：干净关机标记决定 fresh / 原身份回归 / 拒绝裸恢复
//     （ErrUncleanShutdown，须清空状态以 learner 重入）
//
// 边界：
//   - 不自动编排 learner 重入——原语给全（WipeForRejoin、
//     ProposeConfChange、TransferLeader、GroupForQueue），重入编排
//     属 batch③
//   - 不接 core 写路径——本层只组装与管理集群机件，队列读写仍走原路径
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
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

	cancel         context.CancelFunc // 运行 ctx 取消句柄（StopClean/kill 用）
	doneCh         chan struct{}      // 全部组 + flusher 完全退出后关闭
	flusherStop    chan struct{}      // 仅 mem 档：通知后台刷盘 goroutine 退出
	flusherDone    chan struct{}      // 仅 mem 档：刷盘 goroutine 退出后关闭
	flusherStopOne sync.Once          // flusherStop 幂等关闭（kill 与 StopClean 都可能触发）
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
		nodeID:         o.NodeID,
		peers:          o.Peers,
		dataGroups:     o.DataGroups,
		mode:           o.Mode,
		st:             o.Store,
		lg:             lg,
		groups:         make(map[uint32]*group, o.DataGroups+1),
		onLeaderChange: o.OnLeaderChange,
		onApplied:      o.OnApplied,
		doneCh:         make(chan struct{}),
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

	// 传输层 peers 不含本节点（本节点消息由 send 短路，见 send 注释）；
	// deliver 按信封组号路由到对应组的 step
	m.tr = newTransport(m.nodeID, m.ln, m.peerAddrs(), func(g uint32, msg *raftpb.Message) {
		if gr, ok := m.groups[g]; ok {
			gr.step(msg)
		} else {
			m.lg.Debug("收到未知组的消息，丢弃", "g", g)
		}
	}, m.lg)
	m.tr.Start(rctx)

	for g := uint32(0); g < m.Groups(); g++ {
		go m.groups[g].run(rctx)
	}
	if m.mode == AckQuorumMem {
		m.flusherStop = make(chan struct{})
		m.flusherDone = make(chan struct{})
		go m.flusher()
	}
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
