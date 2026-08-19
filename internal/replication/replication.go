// replication.go 提供写路径的复制抽象：单机后端零开销直通，集群后端
// 把批次字节提进所属 raft 组。
//
// 职责：
//   - 定义 Replicator 接口（Apply/ApplyAsync 双形态）——batch③ 把 core
//     的 34 个写点统一改到该抽象上
//   - Standalone：今天的路径，Apply 直通 store.Apply、ApplyAsync 直通
//     store.ApplyAsync（group commit 合并 fsync 的既有机制），零额外开销
//   - Cluster：把批次物理字节（Repr）提进 Manager.Propose 的所属组；
//     ApplyAsync 把等待拆到 Pending.Wait——集群档的「定序」由 raft 内部
//     批量完成，拆分只为把等待挪出调用方队列锁，与单机同形不同机制
//   - Router：组路由视图——Standalone 恒返回 MetaGroup，Cluster 转 Manager
//   - Forwarder：跨节点转发原语（仅 Cluster 实现）——ForwardAppend 把
//     已编码消息交 g 组 leader 的 produce 栈追加、ForwardApply 把构造
//     无关批次 repr 交 g 组 leader 提案
//
// 边界：
//   - 不做错误翻译：非 leader 等 raft 语义错误原样上抛，翻译成客户端
//     可重试错误码是 batch③ 协议面的事，本层包装会吞掉 raft 语义
//     （例外：转发原语在 leader 未知时的守卫按 ErrNotLeader 包装，属
//     错误分类而非翻译——不包装的话上层无法区分「没 leader」与「RPC
//     失败」，重试无从谈起）
//   - 不做重试：提案/转发失败后是否重试由调用方按协议语义决定
//   - 不做组路由：group 由调用方用 cluster.MetaGroup 或
//     Manager.GroupForQueue 算好传入
//   - Forwarder 仅集群档合法：Standalone 不实现该接口，经接口类型调用
//     即 nil 方法 panic（见接口注释）
//   - 日志豁免：本层是纯粘合，成功路径零日志——可观测性由 group 层
//     leader 变更日志与 store 直方图承担。Standalone 路径（含
//     ApplyAsync 直通）维持零日志；Cluster 后端三处例外：b.Close() 失败
//     记 Warn（字节已取出，不挡提案）、ForwardAppend/ForwardApply 跨
//     节点转发成功记 Debug、失败记 Warn——转发走控制通道短连接 RPC，
//     静默失败无从观测，必须可见。第四处例外是包级函数 ApplyOrForward：
//     它把「leader 判定 + 转发 + 批次回收」合成一个入口，转发分支成功
//     记 Info、失败记 Warn（调用方要求的跨节点可见性，见该函数注释）。
package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sort"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/store"
)

// ErrNotLeader 转发 cluster.ErrNotLeader：协议面（rpc）只依赖本包即可识别
// 「本节点不是该组 leader」的 raft 语义错误，无需直接 import cluster——
// 依赖方向 cluster → replication → rpc，协议层不耦合 raft 细节。
// 错误本身由 cluster 定义与包装，本转发保证 errors.Is 穿透（值相等）。
var ErrNotLeader = cluster.ErrNotLeader

// ClusterView 控制台用的集群拓扑只读快照。
//
// 为什么 DTO 定在 replication 而不是 cluster：依赖方向是
// cluster → replication → 上层，admin 只依赖 replication；把 raft 的
// tracker.Progress 直接漏给 admin 会让协议面耦合 raft 细节。
type ClusterView struct {
	// Enabled=false 表示单机档：前端据此渲染「当前为单机模式」，而不是报错
	Enabled bool        `json:"enabled"`
	SelfID  uint64      `json:"self_id"`
	Nodes   []NodeView  `json:"nodes"`
	Groups  []GroupView `json:"groups"`
}

// NodeView 成员表里的一个节点。
type NodeView struct {
	ID       uint64 `json:"id"`
	RaftAddr string `json:"raft_addr"`
	Self     bool   `json:"self"`
}

// GroupView 一个 raft 组在**本节点视角**下的状态。
//
// Commit 是 raft 提交位点（HardState.Commit，每个节点都有，follower 也有）；
// Applied 是本节点已 apply 到位点。两者之差 commit−applied 即「待 apply」
// ——applier 卡住会立刻显形，且每个节点自己就算得出（不依赖 leader 数据）。
//
// 注意：Peers 只有在本节点是该组 leader 时才有内容——raft 的
// tracker.Progress 只在 leader 上维护，follower 上是空表。前端必须按
// IsLeader 决定要不要渲染这一段，不能把空表当成"没有 peer"。
type GroupView struct {
	ID       uint32             `json:"id"`
	Leader   uint64             `json:"leader"`
	IsLeader bool               `json:"is_leader"`
	Role     string             `json:"role"`
	Applied  uint64             `json:"applied"`
	Commit   uint64             `json:"commit"`
	Term     uint64             `json:"term"`
	Peers    []PeerProgressView `json:"peers"`
}

// PeerProgressView leader 视角下某个 peer 的复制进度。
//
// PendingSnapshot 非零 = 该 peer 正在被发快照。长期非零就是 batch④ 定向
// 台账要解决的「快照卡住」现场，前端应当醒目标记。
type PeerProgressView struct {
	ID              uint64 `json:"id"`
	Match           uint64 `json:"match"`
	Next            uint64 `json:"next"`
	State           string `json:"state"`
	RecentActive    bool   `json:"recent_active"`
	IsLearner       bool   `json:"is_learner"`
	PendingSnapshot uint64 `json:"pending_snapshot"`
}

// Topologer 集群拓扑只读视图来源。单机后端回 Enabled=false 的空视图。
type Topologer interface {
	Topology() ClusterView
}

// Topology 单机档没有集群拓扑：回 Enabled=false，让前端渲染「单机模式」
// 而不是把空数组当成"集群里没有节点"。
func (StandaloneRouter) Topology() ClusterView { return ClusterView{Enabled: false} }

// Topology 汇总本节点视角下的全部组状态。
//
// 注意：Status(g) 的 Progress 只在本节点是该组 leader 时非空（raft 的
// tracker 只在 leader 上维护），follower 上 Peers 恒为空切片。
func (r *Cluster) Topology() ClusterView {
	self := r.m.SelfID()
	v := ClusterView{Enabled: true, SelfID: self}
	for id, addr := range r.m.PeerAddrs() {
		v.Nodes = append(v.Nodes, NodeView{ID: id, RaftAddr: addr, Self: id == self})
	}
	sort.Slice(v.Nodes, func(i, j int) bool { return v.Nodes[i].ID < v.Nodes[j].ID })

	for g := uint32(0); g < r.m.Groups(); g++ {
		st, ok := r.m.Status(g)
		if !ok {
			continue
		}
		leader, _ := r.m.Leader(g)
		// Term/Commit 是 protobuf 指针：nil 时按 0 处理（组刚启动、尚未落过
		// HardState 的形态）
		var term uint64
		if st.HardState != nil && st.HardState.Term != nil {
			term = *st.HardState.Term
		}
		var commit uint64
		if st.HardState != nil && st.HardState.Commit != nil {
			commit = *st.HardState.Commit
		}
		gv := GroupView{
			ID:       g,
			Leader:   leader,
			IsLeader: leader == self,
			Role:     st.RaftState.String(),
			Applied:  r.m.AppliedIndex(g),
			Commit:   commit,
			Term:     term,
			Peers:    make([]PeerProgressView, 0, len(st.Progress)),
		}
		for id, pr := range st.Progress {
			gv.Peers = append(gv.Peers, PeerProgressView{
				ID: id, Match: pr.Match, Next: pr.Next,
				State: pr.State.String(), RecentActive: pr.RecentActive,
				IsLearner: pr.IsLearner, PendingSnapshot: pr.PendingSnapshot,
			})
		}
		sort.Slice(gv.Peers, func(i, j int) bool { return gv.Peers[i].ID < gv.Peers[j].ID })
		v.Groups = append(v.Groups, gv)
	}
	return v
}

// Pending 一次已定序、待确认的复制提交；Wait 语义与 store.Pending 一致。
type Pending interface{ Wait() error }

// Replicator 是写路径的复制抽象：单机后端零开销直通，集群后端把批次
// 字节提进所属 raft 组。group 用 cluster.MetaGroup 或
// Manager.GroupForQueue 的返回值。
type Replicator interface {
	// Apply 原子提交批次。返回时保证：单机后端已按 Store 档位落盘；
	// 集群后端已 quorum 确认且在本节点 apply 完成（读己之写成立）。
	// 成功/失败后的批次归属与 store.Apply 一致：调用方不再持有。
	Apply(ctx context.Context, group uint32, b *store.Batch) error
	// ApplyAsync 定序返回、Wait 等确认——produce/deliver 热路径的锁外等待形态。
	// Standalone: store.ApplyAsync 直通（group commit 合并 fsync 的既有机制）。
	// Cluster: 提案发出即返回，Wait 阻塞到本节点 apply（quorum+本地）完成。
	ApplyAsync(ctx context.Context, group uint32, b *store.Batch) (Pending, error)
}

// Router 组路由视图。单机实现恒返回 0；集群实现转发 Manager。
type Router interface {
	GroupForQueue(topic string, queueID uint32) uint32
	MetaGroup() uint32
	IsLeader(g uint32) bool
	// ReadBarrier 等 g 组的线性一致读屏障：返回 nil 后本地读一定包含了
	// 本次调用发起之前已被确认的全部写。单机后端恒 nil（无复制、无屏障
	// 可言）；集群后端在读屏障关闭时同样恒 nil（零开销）。
	ReadBarrier(ctx context.Context, g uint32) error
}

// Forwarder 跨节点转发原语（仅 Cluster 后端实现；Standalone 上调用属编程错误，panic）。
type Forwarder interface {
	// ForwardAppend 把一条逻辑消息交给 g 组 leader 的 produce 栈追加
	//（offset 分配在 leader 侧发生——leader-only 构造不变量的跨节点延伸）。
	// 返回 leader 侧分配的 queueID/offset。自己就是 leader 时属编程错误（调用方先查 IsLeader）。
	ForwardAppend(ctx context.Context, g uint32, msgRaw []byte) (queueID uint32, offset uint64, err error)
	// ForwardApply 把构造无关批次（纯 Delete/DeleteRange/绝对值 Set，不含任何
	// 计数器分配）的 repr 转发给 g 组 leader 提案。带构造状态的批次禁用本方法。
	ForwardApply(ctx context.Context, g uint32, repr []byte) error
}

// ApplyOrForward 提交构造无关批次（纯 Delete/DeleteRange/绝对值 Set，不含
// 任何计数器分配）到 g 组：本节点是该组 leader 时 rep.Apply 本地提案；否则
// 经 fwd.ForwardApply 转发给 g 组 leader 提案。批次归属：两条路径返回后调用
// 方都不再持有（转发路径内部完成 Repr 拷贝与 Close）。
//
// 为什么把「leader 判定 + 转发 + 批次回收」收在一个函数：转发路径的批次
// 生命周期（Repr 字节归批次所有，先拷贝再 Close）与 leader 判定是同一个
// 决策的两个半场，散落复制会在某一处被单独改错——本函数是跨节点清理类批次
// （txn 决断键删除、adminops 成片清理）的唯一提交入口。
//
// fwd 为 nil（单机装配）时本函数只会命中本地分支——单机 Router 的 IsLeader
// 恒真，转发分支不可达，nil 不会解引用。
//
// 注意：带构造状态的批次（含计数器分配）禁用本方法——跨节点重放计数器分配
// 会让构造语义失效（见 Forwarder.ForwardApply 注释）。
func ApplyOrForward(ctx context.Context, rep Replicator, rt Router, fwd Forwarder,
	g uint32, b *store.Batch, logger *slog.Logger) error {
	if rt.IsLeader(g) {
		return rep.Apply(ctx, g, b)
	}
	// 转发路径：Repr 返回的字节归批次所有，必须拷贝后再 Close（回收内存池）。
	// Close 失败仅记 Warn 不阻断——字节已取出，不阻断转发。
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil {
		logger.Warn("转发前回收批次失败（字节已取出，不阻断转发）", "g", g, "err", err)
	}
	// 跨节点转发是罕见慢路径，成功/失败都必须可见（g + 字节数；leader 侧
	// 坐标对清理类批次无意义，不随载荷返回）
	if err := fwd.ForwardApply(ctx, g, repr); err != nil {
		logger.Warn("转发批次失败", "g", g, "bytes", len(repr), "err", err)
		return err
	}
	logger.Info("跨节点转发批次", "g", g, "bytes", len(repr))
	return nil
}

// Standalone 单机后端：Apply/ApplyAsync 忽略 group，直通 store——今天
// 的生产路径，零额外开销。
type Standalone struct {
	st *store.Store
}

// NewStandalone 构造单机后端。
func NewStandalone(st *store.Store) *Standalone {
	return &Standalone{st: st}
}

// Apply 忽略 group，直通 st.Apply。批次归属与 store.Apply 一致。
func (r *Standalone) Apply(_ context.Context, _ uint32, b *store.Batch) error {
	return r.st.Apply(b)
}

// ApplyAsync 忽略 group，直通 st.ApplyAsync。Wait 语义与 store 完全
// 一致（含 group commit 合并 fsync 的既有机制）；保持零日志（见文件头
// 豁免说明）。
func (r *Standalone) ApplyAsync(_ context.Context, _ uint32, b *store.Batch) (Pending, error) {
	p, err := r.st.ApplyAsync(b)
	return pending{p: p}, err
}

// pending 把 store.Pending（值类型，热路径零额外分配）包装为 Pending
// 接口。接口化会让 Pending 逃逸到堆，但 ApplyAsync 的热路径收益
// （fsync 等待挪出锁外）远大于这一次小分配，单机档直通语义不受影响。
type pending struct {
	p store.Pending
}

// Wait 透传 store.Pending.Wait：恰好调用一次（语义同 store 契约）。
func (p pending) Wait() error { return p.p.Wait() }

// StandaloneRouter 单机路由视图：全部键族都在同一个 store 里，不存在
// 跨组/跨节点语义——GroupForQueue/MetaGroup 恒返回 MetaGroup（0），
// IsLeader 恒真。
type StandaloneRouter struct{}

// GroupForQueue 恒返回 MetaGroup：单机无组路由，全部键族归一组。
func (StandaloneRouter) GroupForQueue(string, uint32) uint32 { return cluster.MetaGroup }

// MetaGroup 返回元数据组号（0）。
func (StandaloneRouter) MetaGroup() uint32 { return cluster.MetaGroup }

// IsLeader 恒真：单机不存在非 leader 节点。
func (StandaloneRouter) IsLeader(uint32) bool { return true }

// ReadBarrier 单机档没有复制，本地读天然线性一致，恒放行。
func (StandaloneRouter) ReadBarrier(context.Context, uint32) error { return nil }

// Cluster 集群后端：Apply/ApplyAsync = m.Propose；兼作 Router（转
// Manager）与 Forwarder（经控制通道跨节点转发）。
type Cluster struct {
	m *cluster.Manager
	l *slog.Logger
}

// NewCluster 构造集群后端。
func NewCluster(m *cluster.Manager) *Cluster {
	return &Cluster{m: m, l: slog.Default().With("mod", "replication")}
}

// reserveProposal 把批次字节拷进「预留 cluster.ProposalHeaderLen 提案头」
// 的新缓冲——单拷贝契约的调用方半边（另一半见 cluster.ProposalHeaderLen
// 与 Manager.ProposeReserved 注释）：前 16B 留白由 group.propose 回填
// 提案者/waiter id，本函数不写它们；缓冲所有权随 ProposeReserved 移交
// raft，构造后本包不再读写。
//
// 这一次拷贝不可省：Repr 返回的底层内存归批次所有（Task 1 约定），
// Close 回收前必须拷出；把「必要的一次拷贝」直接拷进带头缓冲，原先
// group.propose 里的第二次整拷（消息体上限 4MB）就此消除。
func reserveProposal(repr []byte) []byte {
	data := make([]byte, cluster.ProposalHeaderLen+len(repr))
	copy(data[cluster.ProposalHeaderLen:], repr)
	return data
}

// Apply 把批次字节提进所属 raft 组并阻塞到本节点 apply 完成。
//
// 先经 reserveProposal 拷出 Repr 字节（带 16B 保留头，单拷贝契约）再
// Close：Repr 返回的底层内存归批次所有（Task 1 约定），raft 库会长期
// 持有日志条目，批次回收前必须完成拷贝。
// Close 失败仅记 Warn 不阻断提案——字节已取出，Close 只是回收内存池。
// 非 leader 等 Propose 错误原样上抛，不做翻译（见文件头边界说明）。
func (r *Cluster) Apply(ctx context.Context, group uint32, b *store.Batch) error {
	data := reserveProposal(b.Repr())
	if err := b.Close(); err != nil {
		r.l.Warn("复制前回收批次失败（字节已取出，不阻断提案）", "group", group, "err", err)
	}
	return r.m.ProposeReserved(ctx, group, data)
}

// ApplyAsync 拷贝 repr 并立即返回，等待挪到 Pending.Wait——produce 热
// 路径的锁外等待形态。拷贝同 Apply 走 reserveProposal（单拷贝契约）。
//
// 与单机档同形不同机制：集群档的「定序」发生在 raft 内部批量（并发
// 在途提案经 Ready 循环合并推进），这里拆出 goroutine 只为把等待挪出
// 调用方队列锁；Wait 阻塞到本节点 apply（quorum+本地）完成，读己之写
// 语义与 Apply 一致。批次归属同 Apply：字节取出后 Close（失败仅记
// Warn，不阻断提案）。
//
// goroutine 为什么必须保留（勿「优化」成调用方同步 Propose）：调用方
// produce.Append 在持有队列锁 qs.mu 时调用本方法，依赖它立即返回才能
// 把 fsync/quorum 等待挪到锁外；而 rn.Propose 在 leader 未知（选举窗口）
// 时会阻塞到 leader 产生——同步执行会让选举窗口内 qs.mu 被长期占住，
// 是行为回退。rn.Propose 没有非阻塞变体，等待无法拆进 Wait，故保留
// goroutine，本轮只消除第二次整拷。
func (r *Cluster) ApplyAsync(ctx context.Context, group uint32, b *store.Batch) (Pending, error) {
	data := reserveProposal(b.Repr())
	if err := b.Close(); err != nil {
		r.l.Warn("复制前回收批次失败（字节已取出，不阻断提案）", "group", group, "err", err)
	}
	ch := make(chan error, 1)
	go func() { ch <- r.m.ProposeReserved(ctx, group, data) }()
	return chanPending(ch), nil
}

// chanPending 把单结果 channel 包装为 Pending：Wait 恰好读一次（与
// store.Pending 的恰好一次 Wait 语义一致）。缓冲 1 保证 goroutine 永不
// 阻塞——Propose 失败路径同样经 channel 带回，错误不丢失。
type chanPending chan error

// Wait 返回一次提案结果：读 channel（Propose 完成即就绪）。
func (p chanPending) Wait() error { return <-p }

// GroupForQueue 转 Manager 的队列→组映射（入盘契约，见 Manager 注释）。
func (r *Cluster) GroupForQueue(topic string, queueID uint32) uint32 {
	return r.m.GroupForQueue(topic, queueID)
}

// MetaGroup 返回元数据组号（0）。
func (r *Cluster) MetaGroup() uint32 { return cluster.MetaGroup }

// IsLeader 返回本节点是否为指定组的 leader（转 Manager）。
func (r *Cluster) IsLeader(g uint32) bool { return r.m.IsLeader(g) }

// ReadBarrier 转发 Manager.ReadBarrier；读屏障关闭时 Manager 内部即恒 nil。
func (r *Cluster) ReadBarrier(ctx context.Context, g uint32) error {
	return r.m.ReadBarrier(ctx, g)
}

// ForwardAppend 把一条已编码逻辑消息（core.EncodeMessage 字节）交给 g
// 组 leader 的 produce 栈追加——offset 分配发生在 leader 侧，
// leader-only 构造不变量的跨节点延伸。返回 leader 侧分配的
// queueID/offset。
//
// 实现：Leader(g) 找当前 leader → 控制通道 RPC（op=OpForwardAppend，
// payload=[4B BE g][msgRaw]）→ 解析响应 [4B BE queueID][8B BE offset]。
//
// 自己就是 leader 时属编程错误（调用方先查 IsLeader 走本地路径）；
// leader 未知（选举未完成）时按 ErrNotLeader 报错供上层重试。
func (r *Cluster) ForwardAppend(ctx context.Context, g uint32, msgRaw []byte) (queueID uint32, offset uint64, err error) {
	lead, ok := r.m.Leader(g)
	if !ok {
		return 0, 0, fmt.Errorf("%w: 组 %d 尚无 leader（选举未完成，稍后重试）", cluster.ErrNotLeader, g)
	}
	// 目标组随载荷携带：控制通道帧不带业务组号（ControlGroup 是保留
	// 通道号），leader 侧 handler 据此路由到对应 produce 栈
	payload := make([]byte, 4+len(msgRaw))
	binary.BigEndian.PutUint32(payload[:4], g)
	copy(payload[4:], msgRaw)
	resp, err := r.m.Control(ctx, lead, cluster.OpForwardAppend, payload)
	if err != nil {
		r.l.Warn("转发追加失败", "g", g, "leader", lead, "bytes", len(msgRaw), "err", err)
		return 0, 0, err
	}
	// 响应布局 [4B BE queueID][8B BE offset]：长度不符即协议错，防御
	// 坏对端（与 Manager.Control 的响应帧长校验同思路）
	if len(resp) != 12 {
		return 0, 0, fmt.Errorf("replication: ForwardAppend 响应 %d B 非法（want 12）", len(resp))
	}
	queueID = binary.BigEndian.Uint32(resp[:4])
	offset = binary.BigEndian.Uint64(resp[4:])
	r.l.Debug("转发追加成功", "g", g, "leader", lead, "bytes", len(msgRaw), "queueID", queueID, "offset", offset)
	return queueID, offset, nil
}

// ForwardApply 把构造无关批次（纯 Delete/DeleteRange/绝对值 Set，不含
// 任何计数器分配）的 repr 转发给 g 组 leader 提案；带构造状态的批次
// 禁用本方法——跨节点重放计数器分配会让构造语义失效（如双倍扣减）。
//
// 实现：Leader(g) 找当前 leader → 控制通道 RPC（op=OpForwardApply，
// payload=[4B BE g][repr]）。自己就是 leader 时属编程错误（调用方先查
// IsLeader）；leader 未知（选举未完成）时按 ErrNotLeader 报错供上层
// 重试。
func (r *Cluster) ForwardApply(ctx context.Context, g uint32, repr []byte) error {
	lead, ok := r.m.Leader(g)
	if !ok {
		return fmt.Errorf("%w: 组 %d 尚无 leader（选举未完成，稍后重试）", cluster.ErrNotLeader, g)
	}
	payload := make([]byte, 4+len(repr))
	binary.BigEndian.PutUint32(payload[:4], g)
	copy(payload[4:], repr)
	if _, err := r.m.Control(ctx, lead, cluster.OpForwardApply, payload); err != nil {
		r.l.Warn("转发提案失败", "g", g, "leader", lead, "bytes", len(repr), "err", err)
		return err
	}
	r.l.Debug("转发提案成功", "g", g, "leader", lead, "bytes", len(repr))
	return nil
}
