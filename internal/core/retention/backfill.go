// backfill.go 孤儿 key 索引的后台补扫。
//
// 职责：清掉「消息已删、索引残留」的存量孤儿索引——来源是升级前旧逻辑
// （先删消息、另行全扫删索引，单趟有上限）积压的部分，以及滚动升级窗口内
// 旧版本 leader 的清理。全部升级后删写两侧都是消息与索引同批，不再产生孤儿，
// 因此每进程每组补扫完成一次即可。
//
// 边界：
//   - 只扫本节点当前 lead 且尚未完成的组；本节点不 lead 任何未完成组时不发起任何扫描
//   - 孤儿判定 = 嵌入 storeMs < 本 topic 当前 cutoff（廉价预筛）且 msg 键不存在
//     （孤儿定义）；过期但消息仍在（时钟回跳使 purgeQueue 边界停在其前）的索引保留
//   - 限速：每块最多 backfillChunk 个键，块间休眠 backfillPause；ctx 取消在块边界返回
//   - 完成状态只在进程内存，不新增持久化键（见包头注释）
//   - 与 purgeQueue 并发安全：两者删同一索引键都是幂等删除
package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/Xsxdot/sq/internal/store"
)

const (
	// backfillChunk 补扫单块最多扫描的 keyidx 键数。
	backfillChunk = 1000
	// backfillPause 补扫块间休眠：上限约 2 万键/秒，避免与前台读写争 IO。
	backfillPause = 50 * time.Millisecond
)

// scanKeyIdx 本包内一切 keyidx 迭代的唯一入口：扫描并把键数计入 counter。
// 稳态清理不应调用它（passIdxScanned 恒为 0 是 S2 的观测量）。
func (m *Manager) scanKeyIdx(counter interface{ Add(int64) int64 }, lower, upper []byte, limit int,
	fn func(k []byte) (bool, error)) (int, error) {
	n := 0
	err := m.st.Scan(lower, upper, limit, func(k, _ []byte) (bool, error) {
		n++
		return fn(k)
	})
	counter.Add(int64(n))
	return n, err
}

// runBackfill 补扫循环：启动即检查一次，此后每 interval 检查是否有新 lead
// 但未完成的组，有则跑一轮、无则空闲。ctx 取消即返回。
func (m *Manager) runBackfill(ctx context.Context) {
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		if err := m.backfillRound(ctx); err != nil && ctx.Err() == nil {
			m.logger.Error("retention 补扫失败，下个周期重试", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// pendingBackfillGroups 本节点当前 lead 且未完成补扫的组集合（按 topic×queue 枚举）。
func (m *Manager) pendingBackfillGroups() map[uint32]bool {
	pending := map[uint32]bool{}
	for _, tc := range m.mt.Topics() {
		for q := uint32(0); q < tc.Queues; q++ {
			g := m.rt.GroupForQueue(tc.Name, q)
			if !m.backfillDone[g] && m.rt.IsLeader(g) {
				pending[g] = true
			}
		}
	}
	return pending
}

// backfillCand 一条待点查的过期索引（键已拷贝出迭代器内存）。
type backfillCand struct {
	idxKey []byte
	msgKey []byte
	group  uint32
}

// backfillRound 跑一轮补扫：遍历全部 topic 的 keyidx，删除属于待补扫组的孤儿索引。
// 一轮结束后，把「本轮开始时就在列、全程未丢 leader、提交无错误」的组记入已完成集合。
//
// 返回：ctx 取消返回 ctx.Err()（本轮组均不记完成）；扫描/点查错误返回该错误
// （同样不记完成，下个周期重来）。提交失败与丢 leader 只把该组移出本轮，不返回错误。
func (m *Manager) backfillRound(ctx context.Context) error {
	pending := m.pendingBackfillGroups()
	if len(pending) == 0 {
		return nil // 本节点不 lead 任何未完成组：不扫描
	}
	m.logger.Info("retention 补扫开始", "groups", len(pending))
	start := time.Now()
	failed := map[uint32]bool{}
	scanned0, deleted0 := m.backfillScanned.Load(), m.backfillDeleted.Load()
	now := time.Now().UnixMilli()
	for _, tc := range m.mt.Topics() {
		cutoff := now - tc.EffectiveRetention().Milliseconds()
		pfx := store.KeyIdxTopicPrefix(tc.Name)
		lower, upper := pfx, store.PrefixUpperBound(pfx)
		for {
			if err := ctx.Err(); err != nil {
				m.logger.Info("retention 补扫中断", "topic", tc.Name, "err", err)
				return err
			}
			var cands []backfillCand
			var last []byte
			n, err := m.scanKeyIdx(&m.backfillScanned, lower, upper, backfillChunk, func(k []byte) (bool, error) {
				last = append(last[:0], k...)
				_, _, ms, qid, off, perr := store.ParseKeyIdxKey(k)
				if perr != nil {
					return false, perr
				}
				if ms >= cutoff {
					return true, nil
				}
				g := m.rt.GroupForQueue(tc.Name, qid)
				if !pending[g] || failed[g] {
					return true, nil
				}
				cands = append(cands, backfillCand{idxKey: append([]byte(nil), k...),
					msgKey: store.MsgKey(tc.Name, qid, off), group: g})
				return true, nil
			})
			if err != nil {
				return fmt.Errorf("补扫扫描 keyidx %s: %w", tc.Name, err)
			}
			// 点查在迭代结束后进行：不在迭代回调里读写交错
			if err := m.backfillDeleteOrphans(ctx, tc.Name, cands, failed); err != nil {
				return err
			}
			if n < backfillChunk {
				break // 本 topic 扫完
			}
			lower = append(last, 0) // 下一块从上一块最后一个键之后继续
			select {
			case <-ctx.Done():
				m.logger.Info("retention 补扫中断", "topic", tc.Name, "err", ctx.Err())
				return ctx.Err()
			case <-time.After(backfillPause):
			}
		}
	}
	for g := range pending {
		if !failed[g] {
			m.backfillDone[g] = true
		}
	}
	m.backfillDoneGroups.Store(int64(len(m.backfillDone)))
	m.logger.Info("retention 补扫完成", "groups", len(pending), "failed_groups", len(failed),
		"scanned_idx", m.backfillScanned.Load()-scanned0, "deleted_idx", m.backfillDeleted.Load()-deleted0,
		"elapsed", time.Since(start).String())
	return nil
}

// backfillDeleteOrphans 对一块候选逐条点查消息，不存在者按组攒批删除。
// 单组批次索引键字节达 maxIdxDeleteBytes 即先提交（同 purgeQueue 的提案大小约束）。
// 提交前复核 leader：已丢 leader 的组丢弃本块结果并记入 failed（移出本轮）。
func (m *Manager) backfillDeleteOrphans(ctx context.Context, topic string, cands []backfillCand, failed map[uint32]bool) error {
	type acc struct {
		b     *store.Batch
		n     int
		bytes int
	}
	batches := map[uint32]*acc{}
	flush := func(g uint32, a *acc) {
		delete(batches, g)
		if failed[g] || !m.rt.IsLeader(g) {
			a.b.Close() // 未提交而放弃的批次自行回收
			if !failed[g] {
				m.logger.Info("retention 补扫期间丢失 leader，该组移出本轮", "topic", topic, "group", g)
			}
			failed[g] = true
			return
		}
		if err := m.rep.Apply(ctx, g, a.b); err != nil {
			// 失败批次按 store 契约丢给 GC，不 Close
			m.logger.Warn("retention 补扫删除提交失败，该组移出本轮", "topic", topic, "group", g,
				"n", a.n, "err", err)
			failed[g] = true
			return
		}
		m.backfillDeleted.Add(int64(a.n))
	}
	for _, c := range cands {
		if failed[c.group] {
			continue
		}
		_, ok, err := m.st.Get(c.msgKey)
		if err != nil {
			for _, a := range batches {
				a.b.Close()
			}
			return fmt.Errorf("补扫点查 %q: %w", c.msgKey, err)
		}
		if ok {
			continue // 消息仍在（如时钟回跳越过 purgeQueue 边界）：不是孤儿
		}
		a := batches[c.group]
		if a == nil {
			a = &acc{b: m.st.NewBatch()}
			batches[c.group] = a
		}
		if err := a.b.Delete(c.idxKey); err != nil {
			for _, a := range batches {
				a.b.Close()
			}
			return fmt.Errorf("补扫删除入批 %q: %w", c.idxKey, err)
		}
		a.n++
		a.bytes += len(c.idxKey)
		if a.bytes >= maxIdxDeleteBytes {
			flush(c.group, a)
		}
	}
	for g, a := range batches {
		flush(g, a)
	}
	return nil
}
