// cluster.go 提供测试/基准共用的三节点集群装配。
//
// 职责：固定 3 节点（id 1/2/3）的创建与启动、leader 收敛等待、
// 节点摘除（kill-leader 实验）、整体关闭。
// 边界：不处理成员变更与扩容（spike 固定成员表）；摘除即弃，不提供
// 恢复/重启（Task 5 的重启用例另行实现）；不并发安全——WaitLeader/
// Kill/Leader 由同一测试 goroutine 顺序调用。
package raftshell

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"time"

	"go.etcd.io/raft/v3"
)

// 固定三节点成员表：NewCluster 与各节点的 peers 共用。
var clusterPeerIDs = []uint64{1, 2, 3}

// Cluster 是测试/基准共用的三节点集群装配体。
//
// 职责：负责节点生命周期（创建/启动/摘除/关闭）与 leader 收敛等待，
// 供 Task 3 的 kill-leader 及后续实验复用。
// 边界：不并发安全，调用方需按顺序在单个 goroutine 内使用。
type Cluster struct {
	Nodes   map[uint64]*Node
	Tr      *chanTransport
	cancels map[uint64]context.CancelFunc
	killed  map[uint64]bool // 摘除节点集合：WaitLeader/对账时排除
	lg      *slog.Logger
}

// NewCluster 创建并启动固定三节点（id 1/2/3）集群，各节点日志目录为 dir/<id>。
//
// 参数：
//   - dir: 集群根目录，三个节点各自在子目录 dir/1、dir/2、dir/3 打开 pebble WAL
//   - mode: 刷盘档位，透传给每个节点（见 AckMode）
//   - lg: 结构化日志，nil 时用 slog.Default()
//
// 返回：
//   - 就绪的 *Cluster（三节点均已 Start）
//   - 错误信息（任一节点初始化失败；已打开的 WAL 会被清理）
//
// 注意：调用方在测试结束时必须调用 Shutdown 摘除全部节点，
// 否则节点 goroutine 与 pebble 句柄泄漏。
func NewCluster(dir string, mode AckMode, lg *slog.Logger) (*Cluster, error) {
	if lg == nil {
		lg = slog.Default()
	}
	c := &Cluster{
		Nodes:   make(map[uint64]*Node, len(clusterPeerIDs)),
		Tr:      newChanTransport(),
		cancels: make(map[uint64]context.CancelFunc, len(clusterPeerIDs)),
		killed:  make(map[uint64]bool, len(clusterPeerIDs)),
		lg:      lg,
	}
	peers := make([]raft.Peer, 0, len(clusterPeerIDs))
	for _, id := range clusterPeerIDs {
		peers = append(peers, raft.Peer{ID: id})
	}
	// 先全部 NewNode + register，再统一 Start：避免节点 1 先跑起来时
	// 节点 2/3 尚未注册，启动期消息被 transport 丢弃
	for _, id := range clusterPeerIDs {
		n, err := NewNode(id, peers, filepath.Join(dir, strconv.FormatUint(id, 10)), mode, c.Tr, lg)
		if err != nil {
			// 初始化失败：清理已打开但未启动的 WAL，避免句柄泄漏
			for _, prev := range c.Nodes {
				_ = prev.wal.Close()
			}
			return nil, fmt.Errorf("NewNode(%d): %w", id, err)
		}
		c.Tr.register(id, n)
		c.Nodes[id] = n
	}
	for _, id := range clusterPeerIDs {
		ctx, cancel := context.WithCancel(context.Background())
		c.cancels[id] = cancel
		c.Nodes[id].Start(ctx)
	}
	lg.Info("三节点集群装配完成", "mode", mode.String(), "dir", dir)
	return c, nil
}

// WaitLeader 轮询等待集群选出唯一 leader。
//
// 参数：
//   - timeout: 最长等待时间，超时返回错误
//
// 返回：
//   - 存活且一致的 leader 节点 ID
//   - 超时错误（含存活节点中选不出 leader 的情况）
//
// 注意：摘除（killed）节点被排除在统计之外——其 run 循环已退出，
// LeaderID 停在摘除前的旧值，若计入则在 kill-leader 场景下
// 新旧值永远无法一致。
func (c *Cluster) WaitLeader(timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var lead uint64
		agreed := true
		for id, n := range c.Nodes {
			if c.killed[id] {
				continue
			}
			l := n.LeaderID()
			if l == 0 || (lead != 0 && l != lead) {
				agreed = false
				break
			}
			lead = l
		}
		// 两个独立条件都要满足：
		//  1. 存活节点对 leader 达成一致（kill 后幸存者会带着旧 leader 值短暂一致）
		//  2. 达成一致的 leader 节点本身必须存活（旧 leader 刚被摘除时，
		//     幸存者的 LeaderID 停在旧值，此时两者一致但 leader 已死）
		if agreed && lead != 0 && !c.killed[lead] {
			c.lg.Debug("leader 收敛", "lead", lead)
			return lead, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return 0, fmt.Errorf("WaitLeader 超时：%v 内未选出存活且一致的 leader", timeout)
}

// Kill 摘除节点，模拟进程宕机：取消其 ctx（run 循环退出、WAL 关闭）
// 并从传输层移除（此后投给它的消息直接丢弃）。
//
// 注意：被摘除节点不恢复（无 restore 语义）；摘除后其 LeaderID 停在
// 摘除前的值，调用方应通过 WaitLeader/Leader 而非直接查询该节点。
func (c *Cluster) Kill(id uint64) {
	c.cancels[id]() // 先停节点自身：不再产生出站消息
	c.Tr.kill(id)   // 再摘除传输：投给它的消息直接丢弃
	c.killed[id] = true
	c.lg.Info("节点已摘除", "id", id)
}

// Leader 返回当前声称自己是 leader 的存活节点；尚无 leader 时返回 nil。
// 应与 WaitLeader 成功之后调用，此时必有且仅有一个存活节点声称 leader。
func (c *Cluster) Leader() *Node {
	for id, n := range c.Nodes {
		if c.killed[id] {
			continue
		}
		if n.IsLeader() {
			return n
		}
	}
	return nil
}

// Shutdown 取消全部节点 ctx 并等待各自完全退出（run 循环结束、WAL 关闭）。
//
// 返回：
//   - nil：全部节点在 5s 内退出
//   - error：任一节点超时未退出（含具体节点 ID）
//
// 注意：幂等，可在 Kill 之后调用；测试结束时必须调用，防止
// 节点 goroutine 与 pebble 句柄泄漏到后续测试。
func (c *Cluster) Shutdown() error {
	var errs []error
	for _, cancel := range c.cancels {
		cancel()
	}
	for id, n := range c.Nodes {
		select {
		case <-n.Done():
		case <-time.After(5 * time.Second):
			errs = append(errs, fmt.Errorf("节点 %d 未在 5s 内退出", id))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("Shutdown 失败: %v", errs)
	}
	return nil
}
