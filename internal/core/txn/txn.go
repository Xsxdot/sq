// Package txn 实现事务消息管理器：半消息暂存、提交/回滚、回查调度
// （spec §3 的 txn 引擎，§5 流程 5）。
//
// 职责：
//   - Stage：TRANSACTION 消息在 produce.Append 之前分流至 half/ 暂存区，
//     half 条目与 halfidx 反查索引同批原子提交
//   - End：commit 经 produce.Append 移入 msg/（第一段），再独立批次删除
//     half 两键（第二段，两段式）；rollback 原子删除两键；txID 不存在时
//     幂等返回 found=false
//   - RunChecker：周期扫描到期半消息，经 Notifier 下发回查、改期重扫，
//     超限丢弃；坏 half 条目/坏 half key 删除止损，不阻塞扫描（Task 4 实现）
//
// 边界：
//   - 不 import 任何 proto/pb（回查命令的协议编码在 rpc 层，经 Notifier 反转）
//   - 不管提交后的消费语义（deliver 的事）；提交后的消息就是普通消息
//   - 崩溃恢复零代码：half/halfidx 全在 Pebble，重启后扫描即恢复；
//     Stage/回滚/改期均为单批原子操作；commit 是两段式（先写 msg/ 后删
//     两键），崩溃窗口重放 = 重复提交 = 重复消息，at-least-once 语义内
//
// 组归属（batch③）：half/ 与 halfidx/ 键族归元数据组（rt.MetaGroup()）——
// 事务暂存区与队列无关，无 GroupForQueue 映射；提交段的两段分别归目标队列
// 组（消息追加）与元数据组（删 half 两键）。EndTransaction 可能落在任意
// 节点：非 leader 组经 fwd 转发，见 End 内分支注释。回查调度器因此是
// leader-only 定时器（Task 8 门控）：只在 meta leader 上跑，非 leader
// 节点整趟跳过等待；获得/失去 meta leadership 各打一条 Info。
package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
)

// HalfRef halfidx/{txID} 的值：半消息当前所在 half/ 键的定位信息与回查计数。
// NextCheckMs 是 half key 里的 8B 时间戳——End 只拿到 txID，必须靠它重建
// half key 才能找到消息本体；Checks 是已排期的回查轮数，超过上限即丢弃。
type HalfRef struct {
	NextCheckMs int64 `json:"next_check_ms"`
	Checks      int   `json:"checks"`
}

// Manager 事务管理器。并发安全：锁按 txID 分片（txnLockShards 片）——
// 「同一 txID 的状态迁移」必须串行（End 与回查改期都会搬移 half 键，交错
// 会把已决断的事务复活成僵尸），但不同 txID 之间毫无共享状态，全局锁徒然
// 让所有事务的 fsync 串行化。同 txID 恒定落在同一分片，互斥保证不变。
type Manager struct {
	rep           replication.Replicator
	rt            replication.Router
	fwd           replication.Forwarder // 跨节点转发（集群档）；单机档 nil——IsLeader 恒真，转发分支不可达
	st            *store.Store
	pr            *produce.Producer
	mt            *meta.Meta
	checkInterval time.Duration
	maxChecks     int
	logger        *slog.Logger

	mus     [txnLockShards]sync.Mutex
	checks  atomic.Uint64 // 累计回查排期次数（/metrics 的 sq_txn_checks_total）
	dropped atomic.Uint64 // 累计超限丢弃条数（/metrics 的 sq_txn_dropped_total）

	// afterAppendHook 测试专用注入钩子（生产恒为 nil）：在 End 提交分支第一段
	// （消息入队）成功后、第二段（删 half 两键）前调用，用于模拟两段之间
	// 进程崩溃。生产代码绝不允许设置。
	afterAppendHook func()
}

// txnLockShards 事务锁分片数。32 片对「低频但可能并发」的事务决断绰绰有余；
// 片内条目哈希碰撞只影响并行度，不影响正确性。
const txnLockShards = 32

// lockFor 返回 txID 对应的分片锁。fnv 与 produce 的 FIFO 选队保持同一哈希家族。
func (t *Manager) lockFor(txID string) *sync.Mutex {
	h := fnv.New32a()
	h.Write([]byte(txID))
	return &t.mus[h.Sum32()%txnLockShards]
}

// New 构造事务管理器。checkInterval/maxChecks 来自 config（已在 Load 校验为正）。
//
// rep/rt 为复制抽象与组路由视图（单机档传 replication.NewStandalone(st) 与
// StandaloneRouter{}，集群档由 main 装配）；fwd 从 rt 断言取得——集群档的
// rt 即 *replication.Cluster（同时实现 Forwarder），单机档的
// StandaloneRouter 不实现 Forwarder，断言得 nil；单机 IsLeader 恒真，转发
// 分支不可达，nil 不会解引用。
func New(rep replication.Replicator, rt replication.Router, st *store.Store,
	pr *produce.Producer, mt *meta.Meta,
	checkInterval time.Duration, maxChecks int, logger *slog.Logger) *Manager {
	fwd, _ := rt.(replication.Forwarder)
	return &Manager{rep: rep, rt: rt, fwd: fwd, st: st, pr: pr, mt: mt,
		checkInterval: checkInterval, maxChecks: maxChecks,
		logger: logger.With("mod", "txn")}
}

// Stage 暂存一条事务半消息，返回（写入后消息, 服务端生成的 txID）。
//
// 半消息不分配队列与 offset（提交移入 msg/ 时才由正常写入路径分配），
// 因此对消费者天然不可见——deliver 只扫 msg/。
// 首次回查排在 now+checkInterval：客户端正常几毫秒内就会 EndTransaction，
// 只有本地事务悬而未决（进程崩溃、网络分区）的孤儿才会活到第一次回查。
func (t *Manager) Stage(ctx context.Context, m *core.Message) (*core.Message, string, error) {
	if len(m.Body) == 0 || len(m.Body) > produce.MaxBodySize {
		return nil, "", fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), produce.MaxBodySize)
	}
	// 与 AppendDelay 同理：错误要在发送端立刻暴露，不能等到提交时才发现
	// topic 不存在、消息无处可去
	if _, err := t.mt.EnsureTopic(ctx, m.Topic); err != nil {
		return nil, "", err
	}
	if m.ID == "" {
		m.ID = core.NewMessageID()
	}
	if m.BornAtMs == 0 {
		m.BornAtMs = time.Now().UnixMilli()
	}
	m.StoreAtMs = time.Now().UnixMilli()
	raw, err := core.EncodeMessage(m)
	if err != nil {
		return nil, "", err
	}
	txID := core.NewMessageID()
	next := time.Now().Add(t.checkInterval).UnixMilli()
	ref, _ := json.Marshal(&HalfRef{NextCheckMs: next, Checks: 0}) // 结构固定无失败路径
	// half 条目与反查索引同批原子提交：崩溃后两键要么都在要么都不在，
	// End 靠 halfidx 定位、调度器靠 half 扫描，任何一键单独存在都是孤儿
	b := t.st.NewBatch()
	b.Set(store.HalfKey(next, txID), raw)
	b.Set(store.HalfIdxKey(txID), ref)
	// half 键族归元数据组（与队列无关，无 GroupForQueue 映射）。
	// 集群档：SendWithTransaction 可能落在任意节点，本节点非 meta
	// leader 时经 fwd.ForwardApply 转发给 meta leader 提案——批次是
	// 构造无关的两个绝对键 Set，跨节点重放无副作用（e2e 实测：SDK 的
	// 重试候选是队列组 leader，meta leader 若恰好不 lead 任何队列组，
	// 纯 rep.Apply 的 ErrNotLeader 会耗尽 SDK 重试预算，事务发送失败）。
	if err := replication.ApplyOrForward(ctx, t.rep, t.rt, t.fwd, t.rt.MetaGroup(), b, t.logger); err != nil {
		return nil, "", fmt.Errorf("写入半消息 %s (topic=%s tx=%s): %w", m.ID, m.Topic, txID, err)
	}
	t.logger.Info("事务半消息已暂存", "topic", m.Topic, "msg_id", m.ID,
		"tx_id", txID, "next_check_ms", next)
	return m, txID, nil
}

// End 决断一条事务：commit=true 原子移入 msg/，false 原子删除。
//
// 返回 found=false 表示 txID 当前不存在半消息——三种正常来源：客户端网络
// 重试（第一次已生效）、回查决断与客户端决断赛跑输的一方、超限已丢弃。
// 三者都不是错误，调用方按幂等成功处理（记 Warn 即可）。
func (t *Manager) End(ctx context.Context, txID string, commit bool) (bool, error) {
	mu := t.lockFor(txID)
	mu.Lock()
	defer mu.Unlock()
	refRaw, ok, err := t.st.Get(store.HalfIdxKey(txID))
	if err != nil {
		return false, fmt.Errorf("读取 halfidx (tx=%s): %w", txID, err)
	}
	if !ok {
		return false, nil
	}
	ref := &HalfRef{}
	if err := json.Unmarshal(refRaw, ref); err != nil {
		return false, fmt.Errorf("解码 halfidx (tx=%s): %w", txID, err)
	}
	halfKey := store.HalfKey(ref.NextCheckMs, txID)
	raw, ok, err := t.st.Get(halfKey)
	if err != nil {
		return false, fmt.Errorf("读取半消息 (tx=%s): %w", txID, err)
	}
	if !ok {
		// 两键同批写入/删除，正常不可能只剩 idx。真走到这里说明数据被外部
		// 改写，删掉孤儿 idx 止损并 Error 留痕（与 delay 清坏条目同理）
		t.logger.Error("halfidx 存在但 half 条目缺失，删除孤儿索引",
			"tx_id", txID, "next_check_ms", ref.NextCheckMs)
		b := t.st.NewBatch()
		b.Delete(store.HalfIdxKey(txID))
		if aerr := replication.ApplyOrForward(ctx, t.rep, t.rt, t.fwd, t.rt.MetaGroup(), b, t.logger); aerr != nil {
			return false, fmt.Errorf("删除孤儿 halfidx (tx=%s): %w", txID, aerr)
		}
		return false, nil
	}
	if !commit {
		b := t.st.NewBatch()
		b.Delete(halfKey)
		b.Delete(store.HalfIdxKey(txID))
		if err := replication.ApplyOrForward(ctx, t.rep, t.rt, t.fwd, t.rt.MetaGroup(), b, t.logger); err != nil {
			return false, fmt.Errorf("回滚删除半消息 (tx=%s): %w", txID, err)
		}
		t.logger.Info("事务已回滚", "tx_id", txID, "checks", ref.Checks)
		return true, nil
	}
	m, err := core.DecodeMessage(raw)
	if err != nil {
		return false, fmt.Errorf("解码半消息 (tx=%s): %w", txID, err)
	}
	// 两段式提交：第一段写目标（msg/，正常分配队列与 offset、写 keyidx、唤醒
	// 长轮询），第二段独立批次删 half 两键。先写后删；崩溃窗口重放 = 重复提交
	// = 重复消息，at-least-once 语义内。次序不得反转（反转 = 丢失）。
	// 既有幂等判定天然兜底：End 先读 halfidx（不存在即已决断），第二段落盘后
	// 一切再次 EndTransaction 均为幂等 no-op；窗口内重试则重复提交一次，
	// 仅此而已——重复有界、不丢失。
	//
	// 集群档分派：EndTransaction 可能落在任意节点。第一段的目标队列组与本节点
	// leader 关系不定——本节点是目标组 leader 时本地 pr.Append（offset 分配在
	// 本节点）；否则经 fwd.ForwardAppend 把消息字节交给目标组 leader 追加
	//（leader-only 构造的跨节点延伸，offset 在 leader 侧分配）。
	// 半消息 m 此刻 QueueID 为零（暂存态未选队），转发组号按
	// rt.GroupForQueue(m.Topic, m.QueueID) 计算——选队由 leader 侧 produce 栈
	// 完成，发起方按此组寻址（错组时 leader 侧 Append 自然报 ErrNotLeader
	// 回传，调用方重试）。
	idxKey := store.HalfIdxKey(txID)
	g := t.rt.GroupForQueue(m.Topic, m.QueueID)
	var stored *core.Message
	if t.rt.IsLeader(g) {
		stored, err = t.pr.Append(ctx, m)
	} else {
		qid, off, ferr := t.fwd.ForwardAppend(ctx, g, raw)
		if ferr != nil {
			return false, fmt.Errorf("转发提交半消息 (tx=%s msg=%s topic=%s g=%d): %w", txID, m.ID, m.Topic, g, ferr)
		}
		// 转发只回坐标：拼出日志所需的存储信息（ID/topic 本就来自半消息本体）
		stored = &core.Message{ID: m.ID, Topic: m.Topic, QueueID: qid, Offset: off}
		t.logger.Info("事务提交消息跨节点转发", "tx_id", txID, "g", g,
			"msg_id", m.ID, "topic", m.Topic, "queue", qid, "offset", off)
	}
	if err != nil {
		return false, fmt.Errorf("提交半消息 (tx=%s msg=%s topic=%s): %w", txID, m.ID, m.Topic, err)
	}
	if t.afterAppendHook != nil {
		t.afterAppendHook()
	}
	// 第二段：独立批次删 half 两键。失败只记 Error 不回滚第一段——消息已提交
	// 入队是既成事实，两键残留 = 重放再次提交 = 重复消息（可接受）；日志带
	// 全坐标，便于按 tx_id/msg_id 在 msg/ 与 half/ 两侧核对。
	// 第二段归元数据组；本节点非 meta leader 时经 fwd.ForwardApply 转发——
	// 批次是构造无关的两个绝对键 Delete，转发安全（跨节点重放无副作用）。
	// 本地/转发分派与批次回收统一走 replication.ApplyOrForward（跨节点清理
	// 类批次的唯一提交入口，见该函数注释）。
	b := t.st.NewBatch()
	b.Delete(halfKey)
	b.Delete(idxKey)
	if err := replication.ApplyOrForward(ctx, t.rep, t.rt, t.fwd, t.rt.MetaGroup(), b, t.logger); err != nil {
		t.logger.Error("半消息已提交入队但 half 键删除失败——重放将重复提交产生重复消息（at-least-once 允许）",
			"tx_id", txID, "msg_id", stored.ID, "topic", stored.Topic,
			"queue", stored.QueueID, "offset", stored.Offset, "err", err)
		return false, fmt.Errorf("删除 half 键 (tx=%s msg=%s topic=%s q=%d off=%d): %w",
			txID, stored.ID, stored.Topic, stored.QueueID, stored.Offset, err)
	}
	t.logger.Info("事务已提交", "tx_id", txID, "msg_id", stored.ID,
		"topic", stored.Topic, "queue", stored.QueueID, "offset", stored.Offset, "checks", ref.Checks)
	return true, nil
}

// ChecksTotal 返回累计回查排期次数（含下发失败的轮次；见 Pass 的注释）。
func (t *Manager) ChecksTotal() uint64 { return t.checks.Load() }

// DroppedTotal 返回累计超限丢弃条数。
func (t *Manager) DroppedTotal() uint64 { return t.dropped.Load() }

// scanInterval 回查扫描间隔。1s 对「几十秒级的回查间隔」精度绰绰有余。
// var 而非 const：测试需注入小值。
var scanInterval = time.Second

// maxCheckPerPass 单趟最多处理条数（预算上界，理由同 delay.maxMovePerPass）。
var maxCheckPerPass = 256

// Notifier 回查命令下发通道。由协议适配层实现（经 Telemetry 流发
// RecoverOrphanedTransactionCommand）；core 经本接口反向解耦，不感知 proto。
// 返回 true 表示命令已写入某个 producer 流（不保证客户端处理成功——
// 客户端的决断最终仍以 EndTransaction 到达）。
type Notifier interface {
	RecoverOrphan(m *core.Message, txID string) bool
}

// RunChecker 阻塞运行回查调度循环，结构与 delay.Scheduler.Run 同构：
// 启动即跑一趟，此后每 scanInterval 一趟，单趟满额立即续趟。ctx 取消即返回。
//
// leader-only 门控：每趟开头先查 meta 组 leadership——非 leader 只等
// tick 不干活（half/ 键族归 meta 组，改期/丢弃都是 meta 组写入），但
// 绝不退出循环：leadership 可能随时轮到自己，退出即永久停摆。
func (t *Manager) RunChecker(ctx context.Context, n Notifier) {
	t.logger.Info("txn 回查调度器启动",
		"scan_interval", scanInterval.String(),
		"check_interval", t.checkInterval.String(), "max_checks", t.maxChecks)
	tk := time.NewTicker(scanInterval)
	defer tk.Stop()
	// metaLeader 记录上一个 tick 的 meta 组 leadership：翻转即「开始/
	// 停止承担调度」的判定面（同 delay.Run 的门控日志约定）。
	metaLeader := false
	for {
		isLeader := t.rt.IsLeader(t.rt.MetaGroup())
		switch {
		case isLeader && !metaLeader:
			t.logger.Info("本节点开始承担 txn 回查调度")
		case !isLeader && metaLeader:
			t.logger.Info("本节点停止承担 txn 回查调度")
		}
		metaLeader = isLeader
		if !isLeader {
			// 门控跳过：每趟都会发生，Debug 级避免刷屏
			t.logger.Debug("非 meta leader，txn 本趟跳过")
			select {
			case <-ctx.Done():
				t.logger.Info("txn 回查调度器退出")
				return
			case <-tk.C:
			}
			continue
		}
		handled, err := t.Pass(ctx, n)
		if err != nil {
			// 单趟失败只记日志不退出：store 瞬时故障恢复后下一趟自然重试
			t.logger.Error("txn 回查趟失败", "err", err)
		} else if handled > 0 {
			t.logger.Info("txn 回查趟完成", "handled", handled)
		}
		if err == nil && handled == maxCheckPerPass {
			continue // 满额=可能还有到期积压，立即续趟
		}
		select {
		case <-ctx.Done():
			t.logger.Info("txn 回查调度器退出")
			return
		case <-tk.C:
		}
	}
}

// Pass 执行一趟到期回查，返回处理条数（下发+改期、超限丢弃、坏条目清理，均计入）。
func (t *Manager) Pass(ctx context.Context, n Notifier) (int, error) {
	// leader-only 门控：half/ 键族归 meta 组，回查的改期/丢弃/坏条目
	// 清理都是 meta 组写入——非 leader 直接跳过整趟（RunChecker 已按
	// 同一条件跳过，直接调用 Pass 也须自守）。
	if !t.rt.IsLeader(t.rt.MetaGroup()) {
		t.logger.Debug("非 meta leader，txn 回查趟跳过")
		return 0, nil
	}
	now := time.Now().UnixMilli()
	// 先收集后处理：Scan 回调里不能开写事务（迭代器与写入交错会破坏迭代），
	// 坏 key 也只能拷贝原始字节、扫描结束后统一批量删除（为什么见下方注释）
	type dueEntry struct {
		txID    string
		raw     []byte
		halfKey []byte // 扫描解析出的 half 键：halfidx 损坏时两键只能靠它重建删除目标
	}
	var dues []dueEntry
	var badKeys [][]byte // ParseHalfKey 拒绝的坏 key，按原始 key 删除
	err := t.st.Scan([]byte(store.HalfPrefix), store.HalfScanUpperBound(now), maxCheckPerPass,
		func(k, v []byte) (bool, error) {
			_, txID, perr := store.ParseHalfKey(k)
			if perr != nil {
				// 坏 key 无法解析出 txID，留着会永远排在到期头部，每趟重报错、
				// 并把其后健康条目饿死（同 delay 删坏条目精神）。为什么不能
				// 在这里直接写：迭代器与写入交错会破坏迭代——所以只拷贝原始
				// key 字节，扫描结束后统一批量删除
				badKeys = append(badKeys, append([]byte(nil), k...))
				return true, nil
			}
			dues = append(dues, dueEntry{txID: txID, raw: append([]byte(nil), v...),
				halfKey: append([]byte(nil), k...)})
			return true, nil
		})
	if err != nil {
		return 0, fmt.Errorf("扫描 half 暂存区: %w", err)
	}
	// 坏 key 批量删除（一个 batch 多个 Delete）后再逐条 Error 留痕。坏 key
	// 解析不出 txID，无法定位它的 halfidx——孤儿 idx 由 End 的既有孤儿清理
	// 兜底，无需在此处理
	if len(badKeys) > 0 {
		b := t.st.NewBatch()
		for _, k := range badKeys {
			b.Delete(k)
		}
		if err := t.rep.Apply(ctx, t.rt.MetaGroup(), b); err != nil {
			return 0, fmt.Errorf("删除坏 half key: %w", err)
		}
		for _, k := range badKeys {
			t.logger.Error("half key 无法解析，删除坏条目", "key", fmt.Sprintf("%q", k))
		}
	}
	handled := len(badKeys)
	for _, d := range dues {
		m, err := core.DecodeMessage(d.raw)
		if err != nil {
			// 坏条目永远无法决断，删除止损并 Error 留痕（同 delay 清坏条目）。
			// 删除目标直接用扫描解析出的 halfKey + txID——坏条目的值已无法解码，
			// half key 只能由扫描回调侧给出；从 idx 重建在 idx 缺失/损坏时会失败，
			// 留下每趟重扫重报的残留窗口（见 dropLocked 注释）
			t.logger.Error("half 条目解码失败，丢弃坏条目", "tx_id", d.txID, "err", err)
			if err := t.dropLocked(ctx, d.txID, d.halfKey); err != nil {
				return handled, err
			}
			handled++
			continue
		}
		send, err := t.checkOne(ctx, d.halfKey, d.txID, m)
		if err != nil {
			// 失败即中断本趟：条目未动，下一趟重扫自然重试
			return handled, err
		}
		handled++
		if send {
			// 下发放在改期之后、锁之外（见 checkOne 注释），这里只负责调用
			if !n.RecoverOrphan(m, d.txID) {
				t.logger.Warn("回查命令无处下发：没有发布该 topic 的在线 producer",
					"tx_id", d.txID, "topic", m.Topic, "msg_id", m.ID)
			}
		}
	}
	return handled, nil
}

// checkOne 处理一条到期半消息：重验存在性→超限丢弃或改期。返回 send=true
// 表示调用方应继续下发回查命令。
//
// halfKey 是扫描回调解析出的 half/ 键：halfidx 正常时旧键从 idx 侧重建即可；
// halfidx 损坏时（JSON 解码失败）重建不出 half key（NextCheckMs 在坏的 idx
// 里），只能靠传入的 halfKey 定位删除止损。
//
// 为什么改期在下发之前、且全程持 mu：客户端的 EndTransaction 随时可能并发
// 到达。改期（搬 half 键）与 End（删 half 键）都以 halfidx 为准且都持 mu，
// 因此任一方总能看到另一方的完整结果；若先下发后改期，客户端可能在两者的
// 间隙完成 End，改期一方再按旧键重写就把已决断的事务复活成僵尸。
// 下发本身是网络操作，放在锁外，避免一个慢客户端拖住全部事务状态迁移。
func (t *Manager) checkOne(ctx context.Context, halfKey []byte, txID string, m *core.Message) (bool, error) {
	mu := t.lockFor(txID)
	mu.Lock()
	defer mu.Unlock()
	refRaw, ok, err := t.st.Get(store.HalfIdxKey(txID))
	if err != nil {
		return false, fmt.Errorf("读取 halfidx (tx=%s): %w", txID, err)
	}
	if !ok {
		// 扫描后、加锁前已被 End 决断——正常赛跑结果，静默跳过即可
		return false, nil
	}
	ref := &HalfRef{}
	if err := json.Unmarshal(refRaw, ref); err != nil {
		// halfidx 损坏：两键都无法从 idx 侧重建删除目标，但扫描回调已成功
		// 解析过本条目 key 的 ms+txID——用「传入的 halfKey + HalfIdxKey(txID)」
		// 同批删除止损，Error 留痕后返回 false,nil 继续本趟：否则坏 idx 会让
		// 每趟在此中断、其后健康条目永久饿死（与 End 的孤儿 idx 清理同构）
		t.logger.Error("halfidx 解码失败，删除坏条目", "tx_id", txID,
			"key", fmt.Sprintf("%q", halfKey), "err", err)
		b := t.st.NewBatch()
		b.Delete(halfKey)
		b.Delete(store.HalfIdxKey(txID))
		if aerr := t.rep.Apply(ctx, t.rt.MetaGroup(), b); aerr != nil {
			return false, fmt.Errorf("删除坏 halfidx 条目 (tx=%s): %w", txID, aerr)
		}
		return false, nil
	}
	oldKey := store.HalfKey(ref.NextCheckMs, txID)
	if ref.Checks >= t.maxChecks {
		// spec §5：回查上限默认 15 次，超限丢弃并记日志。Error 级：这代表
		// 一条业务消息被放弃，运维必须能从日志里找到它
		b := t.st.NewBatch()
		b.Delete(oldKey)
		b.Delete(store.HalfIdxKey(txID))
		if err := t.rep.Apply(ctx, t.rt.MetaGroup(), b); err != nil {
			return false, fmt.Errorf("丢弃超限半消息 (tx=%s): %w", txID, err)
		}
		t.dropped.Add(1)
		t.logger.Error("半消息回查超限，丢弃", "tx_id", txID, "msg_id", m.ID,
			"topic", m.Topic, "checks", ref.Checks, "max_checks", t.maxChecks)
		return false, nil
	}
	// 改期：half 键搬到新回查时间、checks+1，同批原子。无论下发是否成功都
	// 计轮次——producer 永不回来时，半消息也要在 maxChecks 轮后被丢弃，
	// 而不是永远滞留（每轮间隔 checkInterval，丢弃前给了它完整的重连窗口）
	next := time.Now().Add(t.checkInterval).UnixMilli()
	raw, ok, err := t.st.Get(oldKey)
	if err != nil {
		return false, fmt.Errorf("重读 half 条目失败 (tx=%s): %w", txID, err)
	}
	if !ok {
		// half 条目消失但 idx 在：两键同批写删，正常不可达（扫描收集与改期
		// 都持 mu），真发生说明数据被外部改写——与 End 的孤儿 idx 清理同构：
		// Error 留痕 + 删 idx 止损 + 返回 false,nil，不再中断本趟饿死其后条目
		t.logger.Error("halfidx 存在但 half 条目缺失，删除孤儿索引",
			"tx_id", txID, "next_check_ms", ref.NextCheckMs)
		b := t.st.NewBatch()
		b.Delete(store.HalfIdxKey(txID))
		if aerr := t.rep.Apply(ctx, t.rt.MetaGroup(), b); aerr != nil {
			return false, fmt.Errorf("删除孤儿 halfidx (tx=%s): %w", txID, aerr)
		}
		return false, nil
	}
	newRef, _ := json.Marshal(&HalfRef{NextCheckMs: next, Checks: ref.Checks + 1})
	b := t.st.NewBatch()
	b.Delete(oldKey)
	b.Set(store.HalfKey(next, txID), raw)
	b.Set(store.HalfIdxKey(txID), newRef)
	if err := t.rep.Apply(ctx, t.rt.MetaGroup(), b); err != nil {
		return false, fmt.Errorf("半消息改期 (tx=%s): %w", txID, err)
	}
	t.checks.Add(1)
	t.logger.Debug("半消息回查已排期", "tx_id", txID, "msg_id", m.ID,
		"topic", m.Topic, "check_round", ref.Checks+1, "next_check_ms", next)
	return true, nil
}

// dropLocked 按「扫描解析出的 half 键 + halfidx 键」删除坏条目（坏条目清理用）。自行加锁。
// 为什么直接传 halfKey 而不是从 idx 重建：坏条目的值已无法解码，删除只能靠
// half key 定位；若此时 idx 恰好缺失或损坏，从 idx 重建会失败，坏条目将永远
// 留在盘上、每趟重扫重报（da3a330 修掉坏 key 洪水后的同类残留窗口）。
func (t *Manager) dropLocked(ctx context.Context, txID string, halfKey []byte) error {
	mu := t.lockFor(txID)
	mu.Lock()
	defer mu.Unlock()
	b := t.st.NewBatch()
	b.Delete(halfKey)
	b.Delete(store.HalfIdxKey(txID))
	return t.rep.Apply(ctx, t.rt.MetaGroup(), b)
}
