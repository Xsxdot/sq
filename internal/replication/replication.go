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

	"github.com/xushixin/sq/internal/cluster"
	"github.com/xushixin/sq/internal/store"
)

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

// Apply 把批次字节提进所属 raft 组并阻塞到本节点 apply 完成。
//
// 先拷贝 Repr 字节再 Close：Repr 返回的底层内存归批次所有（Task 1
// 约定），raft 库会长期持有日志条目，批次回收前必须完成拷贝。
// Close 失败仅记 Warn 不阻断提案——字节已取出，Close 只是回收内存池。
// 非 leader 等 Propose 错误原样上抛，不做翻译（见文件头边界说明）。
func (r *Cluster) Apply(ctx context.Context, group uint32, b *store.Batch) error {
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil {
		r.l.Warn("复制前回收批次失败（字节已取出，不阻断提案）", "group", group, "err", err)
	}
	return r.m.Propose(ctx, group, repr)
}

// ApplyAsync 拷贝 repr 并立即返回，等待挪到 Pending.Wait——produce 热
// 路径的锁外等待形态。
//
// 与单机档同形不同机制：集群档的「定序」发生在 raft 内部批量（并发
// 在途提案经 Ready 循环合并推进），这里拆出 goroutine 只为把等待挪出
// 调用方队列锁；Wait 阻塞到本节点 apply（quorum+本地）完成，读己之写
// 语义与 Apply 一致。批次归属同 Apply：字节取出后 Close（失败仅记
// Warn，不阻断提案）。
func (r *Cluster) ApplyAsync(ctx context.Context, group uint32, b *store.Batch) (Pending, error) {
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil {
		r.l.Warn("复制前回收批次失败（字节已取出，不阻断提案）", "group", group, "err", err)
	}
	ch := make(chan error, 1)
	go func() { ch <- r.m.Propose(ctx, group, repr) }()
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
