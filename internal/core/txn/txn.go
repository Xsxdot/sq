// Package txn 实现事务消息管理器：半消息暂存、提交/回滚、回查调度
// （spec §3 的 txn 引擎，§5 流程 5）。
//
// 职责：
//   - Stage：TRANSACTION 消息在 produce.Append 之前分流至 half/ 暂存区，
//     half 条目与 halfidx 反查索引同批原子提交
//   - End：commit 经 produce.AppendWith 原子移入 msg/ 并删除两键；
//     rollback 原子删除两键；txID 不存在时幂等返回 found=false
//   - RunChecker：周期扫描到期半消息，经 Notifier 下发回查、改期重扫，
//     超限丢弃（Task 4 实现）
//
// 边界：
//   - 不 import 任何 proto/pb（回查命令的协议编码在 rpc 层，经 Notifier 反转）
//   - 不管提交后的消费语义（deliver 的事）；提交后的消息就是普通消息
//   - 崩溃恢复零代码：half/halfidx 全在 Pebble，重启后扫描即恢复；
//     每次状态迁移都是单批原子操作，不存在两键不一致的中间态
package txn

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// HalfRef halfidx/{txID} 的值：半消息当前所在 half/ 键的定位信息与回查计数。
// NextCheckMs 是 half key 里的 8B 时间戳——End 只拿到 txID，必须靠它重建
// half key 才能找到消息本体；Checks 是已排期的回查轮数，超过上限即丢弃。
type HalfRef struct {
	NextCheckMs int64 `json:"next_check_ms"`
	Checks      int   `json:"checks"`
}

// Manager 事务管理器。并发安全：mu 串行化「同一 txID 的状态迁移」——
// End（客户端决断）与回查调度器的改期都会搬移 half 键，两者交错时后者
// 必须看到前者的结果，否则已提交的事务会被改期逻辑复活成僵尸半消息。
type Manager struct {
	st            *store.Store
	pr            *produce.Producer
	mt            *meta.Meta
	checkInterval time.Duration
	maxChecks     int
	logger        *slog.Logger

	mu      sync.Mutex
	checks  atomic.Uint64 // 累计回查排期次数（/metrics 的 sq_txn_checks_total）
	dropped atomic.Uint64 // 累计超限丢弃条数（/metrics 的 sq_txn_dropped_total）
}

// New 构造事务管理器。checkInterval/maxChecks 来自 config（已在 Load 校验为正）。
func New(st *store.Store, pr *produce.Producer, mt *meta.Meta,
	checkInterval time.Duration, maxChecks int, logger *slog.Logger) *Manager {
	return &Manager{st: st, pr: pr, mt: mt,
		checkInterval: checkInterval, maxChecks: maxChecks,
		logger: logger.With("mod", "txn")}
}

// Stage 暂存一条事务半消息，返回（写入后消息, 服务端生成的 txID）。
//
// 半消息不分配队列与 offset（提交移入 msg/ 时才由正常写入路径分配），
// 因此对消费者天然不可见——deliver 只扫 msg/。
// 首次回查排在 now+checkInterval：客户端正常几毫秒内就会 EndTransaction，
// 只有本地事务悬而未决（进程崩溃、网络分区）的孤儿才会活到第一次回查。
func (t *Manager) Stage(m *core.Message) (*core.Message, string, error) {
	if len(m.Body) == 0 || len(m.Body) > produce.MaxBodySize {
		return nil, "", fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), produce.MaxBodySize)
	}
	// 与 AppendDelay 同理：错误要在发送端立刻暴露，不能等到提交时才发现
	// topic 不存在、消息无处可去
	if _, err := t.mt.EnsureTopic(m.Topic); err != nil {
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
	b.Set(store.HalfKey(next, txID), raw, nil)
	b.Set(store.HalfIdxKey(txID), ref, nil)
	if err := t.st.Apply(b); err != nil {
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
func (t *Manager) End(txID string, commit bool) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
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
		b.Delete(store.HalfIdxKey(txID), nil)
		if aerr := t.st.Apply(b); aerr != nil {
			return false, fmt.Errorf("删除孤儿 halfidx (tx=%s): %w", txID, aerr)
		}
		return false, nil
	}
	if !commit {
		b := t.st.NewBatch()
		b.Delete(halfKey, nil)
		b.Delete(store.HalfIdxKey(txID), nil)
		if err := t.st.Apply(b); err != nil {
			return false, fmt.Errorf("回滚删除半消息 (tx=%s): %w", txID, err)
		}
		t.logger.Info("事务已回滚", "tx_id", txID, "checks", ref.Checks)
		return true, nil
	}
	m, err := core.DecodeMessage(raw)
	if err != nil {
		return false, fmt.Errorf("解码半消息 (tx=%s): %w", txID, err)
	}
	// 写 msg/（正常分配队列与 offset、写 keyidx、唤醒长轮询）+ 删两个 half 键，
	// 同一 Batch 原子提交：与 delay 到期搬运同构，不存在丢失或重复
	idxKey := store.HalfIdxKey(txID)
	stored, err := t.pr.AppendWith(m, func(b *pebble.Batch) {
		b.Delete(halfKey, nil)
		b.Delete(idxKey, nil)
	})
	if err != nil {
		return false, fmt.Errorf("提交半消息 (tx=%s msg=%s topic=%s): %w", txID, m.ID, m.Topic, err)
	}
	t.logger.Info("事务已提交", "tx_id", txID, "msg_id", stored.ID,
		"topic", stored.Topic, "queue", stored.QueueID, "offset", stored.Offset, "checks", ref.Checks)
	return true, nil
}

// ChecksTotal 返回累计回查排期次数（含下发失败的轮次；见 Pass 的注释）。
func (t *Manager) ChecksTotal() uint64 { return t.checks.Load() }

// DroppedTotal 返回累计超限丢弃条数。
func (t *Manager) DroppedTotal() uint64 { return t.dropped.Load() }
