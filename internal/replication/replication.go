// replication.go 提供写路径的复制抽象：单机后端零开销直通，集群后端
// 把批次字节提进所属 raft 组。
//
// 职责：
//   - 定义 Replicator 接口——batch③ 把 core 的 34 个写点统一改到该抽象上
//   - Standalone：今天的路径，Apply 直通 store.Apply，零额外开销
//   - Cluster：把批次物理字节（Repr）提进 Manager.Propose 的所属组
//
// 边界：
//   - 不做错误翻译：非 leader 等 raft 语义错误原样上抛，翻译成客户端
//     可重试错误码是 batch③ 协议面的事，本层包装会吞掉 raft 语义
//   - 不做重试：提案失败后是否重试由调用方按协议语义决定
//   - 不做组路由：group 由调用方用 cluster.MetaGroup 或
//     Manager.GroupForQueue 算好传入
//   - 日志豁免：本层是纯粘合，成功路径零日志——可观测性由 group 层
//     leader 变更日志与 store 直方图承担；仅 Cluster 后端 b.Close() 失败
//     记 Warn（字节已取出，不挡提案）
package replication

import (
	"context"
	"log/slog"

	"github.com/xushixin/sq/internal/cluster"
	"github.com/xushixin/sq/internal/store"
)

// Replicator 是写路径的复制抽象：单机后端零开销直通，集群后端把批次
// 字节提进所属 raft 组。group 用 cluster.MetaGroup 或
// Manager.GroupForQueue 的返回值。
//
// ApplyAsync/Pending 拆分形态（produce 热路径的 group-commit 优化）
// 刻意不进本批接口：等 batch③ 接线 produce 时按实测需要再加（YAGNI）。
type Replicator interface {
	// Apply 原子提交批次。返回时保证：单机后端已按 Store 档位落盘；
	// 集群后端已 quorum 确认且在本节点 apply 完成（读己之写成立）。
	// 成功/失败后的批次归属与 store.Apply 一致：调用方不再持有。
	Apply(ctx context.Context, group uint32, b *store.Batch) error
}

// Standalone 单机后端：Apply 忽略 group，直通 store.Apply——今天
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

// Cluster 集群后端：Apply = m.Propose(ctx, group, b.Repr()) + b.Close()。
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
