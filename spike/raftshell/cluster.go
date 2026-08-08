// cluster.go 提供测试/基准共用的三节点集群装配与节点恢复。
//
// 职责：固定 3 节点（id 1/2/3）的创建与启动、leader 收敛等待、
// 节点摘除（kill-leader 实验）、整体关闭，以及节点重启恢复
// （Restart：干净/不干净两条路径，见 Restart 注释）与 FSM 收敛等待。
// 边界：不处理成员变更与扩容（spike 固定成员表）；learner 重入要求
// 重启时集群内已有非本节点的存活 leader——被摘除（Kill）的 leader 由
// 幸存者选出新 leader 后同样可回归，仅「待重启节点仍是唯一 leader」
// 被拒绝；不并发安全——WaitLeader/Kill/Leader/Restart 由同一测试
// goroutine 顺序调用。
package raftshell

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// 固定三节点成员表：NewCluster 与各节点的 peers 共用。
var clusterPeerIDs = []uint64{1, 2, 3}

// uncleanRestartLogMsg 是不干净关机的排障线索（同时被测试引用）：
// 生产环境搜到这一行，即可断定某节点经历了一次未写干净标记的断电，
// 后续一切行为都按 learner 重入的语义解释。
const uncleanRestartLogMsg = "检测到不干净关机，清空状态以 learner 重入"

// Cluster 是测试/基准共用的三节点集群装配体。
//
// 职责：负责节点生命周期（创建/启动/摘除/重启/关闭）与 leader 收敛
// 等待，供 Task 3 的 kill-leader 及后续实验复用。
// 边界：不并发安全，调用方需按顺序在单个 goroutine 内使用。
type Cluster struct {
	Nodes   map[uint64]*Node
	Tr      *chanTransport
	cancels map[uint64]context.CancelFunc
	killed  map[uint64]bool // 摘除节点集合：WaitLeader/对账时排除
	dir     string          // 集群根目录，节点状态目录为 dir/<id>（Restart 用）
	mode    AckMode         // 刷盘档位（Restart 重建节点时透传）
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
		dir:     dir,
		mode:    mode,
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

// Restart 重启集群中的某个节点，按 WAL 中的干净关机标记走两条路径：
//
//   - 有标记（StopClean 干净关机）：原身份回归——WAL 全量重放进
//     MemoryStorage（成员表 ConfState 由日志重放合成），raft.RestartNode
//     恢复；不删除状态目录、不降级，集群成员表零变更；
//   - 无标记（断电/崩溃等不干净关机）：learner 重入——清空该节点状态
//     目录，leader 先 ProposeConfChange 移除旧 voter，再以
//     AddLearner 重新加入；新节点以空存储启动（身份由 leader 的
//     ConfChange 日志赋予），追平 leader 日志后 Promote（AddNode）
//     升回 voter。恢复全程打 Info 日志（「检测到不干净关机，清空状态
//     以 learner 重入」「追齐完成，升级 voter」是生产排障的关键线索）。
//
// 返回：
//   - nil：节点已恢复，且 learner 路径下已升级为 voter
//   - error：旧节点未退出、WAL/目录操作失败、conf-change 超时等，
//     均带节点与操作上下文
//
// 注意：
//   - 必须先 Kill（或 StopClean）待重启节点并等其完全退出
//   - learner 重入的「leader 检查」只拒绝一种情形：待重启节点此刻
//     仍是集群唯一 leader（没有存活节点能选出新 leader，重入无从
//     谈起）。被 Kill 的 leader 不在此列——幸存者会先选出新 leader，
//     随后正常走 learner 回归；被 StopClean 的 leader 走干净路径，
//     根本不进 learner 分支
//   - 边界：追齐走全量日志重放，spike 不做日志压缩（无 Compact）；
//     快照流式追齐是 B8.2 的范围
func (c *Cluster) Restart(id uint64) error {
	old := c.Nodes[id]
	if old == nil {
		return fmt.Errorf("Restart(%d): 节点不存在", id)
	}
	select {
	case <-old.Done():
	case <-time.After(5 * time.Second):
		return fmt.Errorf("Restart(%d): 旧节点未在 5s 内完全退出（WAL 未关闭，无法安全重开）", id)
	}
	dir := filepath.Join(c.dir, strconv.FormatUint(id, 10))
	// 打开 WAL 读标记：有标记 → 原身份回归；无标记 → learner 重入。
	// 打开动作本身也验证了旧节点确实释放了 pebble 句柄。
	wal, err := openWAL(dir, c.mode, c.lg)
	if err != nil {
		return fmt.Errorf("Restart(%d): 打开 WAL 失败: %w", id, err)
	}
	clean, err := wal.ConsumeCleanShutdown()
	if err != nil {
		_ = wal.Close()
		return fmt.Errorf("Restart(%d): 读取关机标记失败: %w", id, err)
	}
	if clean {
		return c.restartClean(id, wal)
	}
	return c.restartAsLearner(id, wal, dir)
}

// restartClean 干净路径：WAL 全量重放进 MemoryStorage，RestartNode 原身份回归。
func (c *Cluster) restartClean(id uint64, wal *WAL) error {
	hs, ents, err := wal.Load()
	if err != nil {
		_ = wal.Close()
		return fmt.Errorf("Restart(%d): WAL 重放读取失败: %w", id, err)
	}
	storage := raft.NewMemoryStorage()
	if len(ents) > 0 {
		first := ents[0]
		snapIndex := first.GetIndex() - 1
		// snapTerm 用首条条目的 term 近似「缺失条目 index-1 的 term」：
		// raft 只在任期比较时用到它，而两条路径都不会触发该比较——
		// 不干净路径空存储启动（snapTerm=0）只与新鲜 leader 的 term(0)
		// 相遇；干净路径 leader 的 Progress 仍保留，追齐从不会回退到
		// index-1 对账（若未来做快照压缩，需按真实 term 补快照，见 B8.2）
		snapTerm := first.GetTerm()
		snap := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
			Index:     &snapIndex,
			Term:      &snapTerm,
			ConfState: confStateFromEntries(ents),
		}}
		if err := storage.ApplySnapshot(snap); err != nil {
			_ = wal.Close()
			return fmt.Errorf("Restart(%d): 重放快照失败: %w", id, err)
		}
		if err := storage.Append(ents); err != nil {
			_ = wal.Close()
			return fmt.Errorf("Restart(%d): 重放条目失败: %w", id, err)
		}
	}
	if !raft.IsEmptyHardState(hs) {
		if err := storage.SetHardState(hs); err != nil {
			_ = wal.Close()
			return fmt.Errorf("Restart(%d): 重放 HardState 失败: %w", id, err)
		}
	}
	lastIndex := uint64(0)
	if len(ents) > 0 {
		lastIndex = ents[len(ents)-1].GetIndex()
	}
	c.lg.Info("检测到干净关机标记，原身份回归", "id", id, "lastIndex", lastIndex)
	// 重启节点会重新投递全部已 commit 条目（raft applied 指针从零开始），
	// 本 FSM 的计数随之恢复，无需调用方额外对齐
	n := newNode(id, raft.RestartNode(raftConfig(id, storage)), storage, wal, c.Tr, c.mode, c.lg)
	c.install(id, n)
	return nil
}

// restartAsLearner 不干净路径：leader 走 Remove → AddLearner → 追平 →
// AddNode 的完整成员变更往返，被重启节点以空存储重入。
// 注意：任何破坏性操作（关 WAL、清状态目录）都排在 leader 检查之后，
// 确保失败路径上该节点唯一的 WAL 副本原样保留（终审 R2 修复了原实现
// 先清目录后查 leader 的顺序缺陷）。
func (c *Cluster) restartAsLearner(id uint64, wal *WAL, dir string) error {
	c.lg.Info(uncleanRestartLogMsg, "id", id, "dir", dir)
	// 先确认 leader 可用，再动状态目录：waitLeader 失败或待重启节点
	// 仍是唯一 leader 时直接返回，目录原封未动
	lead, err := c.WaitLeader(5 * time.Second)
	if err != nil {
		_ = wal.Close()
		c.lg.Error("等待 leader 失败", "id", id, "err", err)
		return fmt.Errorf("Restart(%d): 等待 leader 失败: %w", id, err)
	}
	// 拒绝条件收窄为「待重启节点仍是当前 leader」：此时没有存活节点
	// 能选出新 leader，learner 重入无从谈起。被 Kill 的 leader 由
	// 幸存者选出新 leader 后走正常 learner 回归（见 Restart 注释）
	if lead == id {
		_ = wal.Close()
		err := fmt.Errorf("Restart(%d): 待重启节点仍是唯一 leader（lead=%d）", id, lead)
		c.lg.Error("待重启节点仍是唯一 leader，拒绝 learner 重入", "id", id, "lead", lead)
		return err
	}
	// 先关掉刚打开的 WAL 再清目录：pebble 持有目录文件句柄，
	// 不关闭直接 RemoveAll 会失败
	if err := wal.Close(); err != nil {
		c.lg.Error("关闭旧 WAL 失败，无法清空状态目录", "id", id, "err", err)
		return fmt.Errorf("Restart(%d): 关闭旧 WAL 失败: %w", id, err)
	}
	if err := os.RemoveAll(dir); err != nil {
		c.lg.Error("清空状态目录失败", "id", id, "dir", dir, "err", err)
		return fmt.Errorf("Restart(%d): 清空状态目录 %s 失败: %w", id, dir, err)
	}
	c.lg.Info("状态目录已清空", "id", id, "dir", dir)
	leader := c.Nodes[lead]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// 1. 从成员表移除旧 voter：清掉 leader 侧对该节点的 Progress
	if err := leader.ProposeConfChange(ctx, raftpb.ConfChangeRemoveNode, id); err != nil {
		c.lg.Error("移除旧 voter 失败", "id", id, "leader", lead, "err", err)
		return fmt.Errorf("Restart(%d): 移除旧 voter: %w", id, err)
	}
	c.lg.Info("已从成员表移除旧节点，准备以 learner 重入", "id", id, "leader", lead)
	// 2. 以 learner 身份重新加入：leader 侧创建该节点的 Progress，
	// 开始向它复制日志（此刻新节点尚未启动，消息先被 transport 丢弃，
	// leader 的心跳重试会兜底）
	if err := leader.ProposeConfChange(ctx, raftpb.ConfChangeAddLearnerNode, id); err != nil {
		c.lg.Error("添加 learner 失败", "id", id, "leader", lead, "err", err)
		return fmt.Errorf("Restart(%d): 添加 learner: %w", id, err)
	}
	c.lg.Info("已以 learner 身份加入成员表，等待追齐", "id", id, "leader", lead)
	// 3. 新节点以空存储启动——身份由 leader 的 ConfChange 日志赋予
	storage := raft.NewMemoryStorage()
	nwal, err := openWAL(dir, c.mode, c.lg)
	if err != nil {
		c.lg.Error("重建 WAL 失败", "id", id, "err", err)
		return fmt.Errorf("Restart(%d): 重建 WAL 失败: %w", id, err)
	}
	n := newNode(id, raft.RestartNode(raftConfig(id, storage)), storage, nwal, c.Tr, c.mode, c.lg)
	c.install(id, n)
	// 4. 追平 leader 日志（AppliedCount 达到 leader 值）。追齐走全量
	// 条目重放：spike 不做日志压缩（无 Compact），快照流式追齐是 B8.2
	if err := c.waitCaughtUp(n, leader, 30*time.Second); err != nil {
		c.lg.Error("learner 追齐超时", "id", id, "leader", lead,
			"learner", n.AppliedCount(), "leaderApplied", leader.AppliedCount(), "err", err)
		return err
	}
	c.lg.Info("追齐完成，升级 voter", "id", id, "applied", n.AppliedCount(), "leader", lead)
	// 5. 提升为 voter：成员表恢复为原始三 voter
	if err := leader.ProposeConfChange(ctx, raftpb.ConfChangeAddNode, id); err != nil {
		c.lg.Error("升级 voter 失败", "id", id, "leader", lead, "err", err)
		return fmt.Errorf("Restart(%d): 升级 voter: %w", id, err)
	}
	c.lg.Info("节点已恢复为 voter，成员表恢复三节点", "id", id)
	return nil
}

// install 把重启后的新节点挂回集群：注册传输（覆盖旧节点指针）、
// 清除摘除标记、替换节点与取消句柄，然后启动 run 循环。
func (c *Cluster) install(id uint64, n *Node) {
	c.Tr.register(id, n)
	c.Tr.restore(id) // 幂等：仅不干净路径 Kill 过需要
	ctx, cancel := context.WithCancel(context.Background())
	c.cancels[id] = cancel
	c.killed[id] = false
	c.Nodes[id] = n
	n.Start(ctx)
	c.lg.Info("节点已重新上线", "id", id)
}

// waitCaughtUp 轮询等待 learner 的 AppliedCount 追平 leader。
// 两节点数同一条日志（leader 无新提案时计数即对账锚点），
// 达到即视为追齐。
func (c *Cluster) waitCaughtUp(learner, leader *Node, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if learner.AppliedCount() >= leader.AppliedCount() {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("Restart(%d): learner 未在 %v 内追平 leader %d（learner=%d leader=%d）",
		learner.id, timeout, leader.id, learner.AppliedCount(), leader.AppliedCount())
}

// WaitConverged 轮询等待所有存活节点（未被 Kill）的 AppliedCount 一致，
// 即 FSM 收敛。
//
// 参数：
//   - timeout: 最长等待时间
//
// 返回：
//   - nil：已收敛
//   - error：超时，错误信息附各节点当前计数（排障上下文）
//
// 注意：存活节点数为 0 时直接报错——空集上「全部相等」是假收敛
// （终审 R2 修复）；存活节点数为 1 时视为已收敛（无对账对象）；
// 摘除节点不参与对账——其计数停在摘除前。
func (c *Cluster) WaitConverged(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var target uint64
		converged := true
		alive := 0
		first := true
		for id, n := range c.Nodes {
			if c.killed[id] {
				continue
			}
			alive++
			app := n.AppliedCount()
			if first {
				target = app
				first = false
				continue
			}
			if app != target {
				converged = false
				break
			}
		}
		if alive == 0 {
			return fmt.Errorf("WaitConverged：无存活节点（timeout %v），无法判定收敛", timeout)
		}
		if converged {
			c.lg.Info("集群 FSM 已收敛", "applied", target)
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	counts := make([]string, 0, len(c.Nodes))
	for id, n := range c.Nodes {
		counts = append(counts, fmt.Sprintf("node%d=%d", id, n.AppliedCount()))
	}
	return fmt.Errorf("WaitConverged 超时：%v 内未收敛（%s）", timeout, strings.Join(counts, " "))
}
