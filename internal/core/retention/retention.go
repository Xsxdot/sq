// Package retention 实现消息过期清理后台任务（spec §5 流程 7）。
//
// 职责：
//   - 周期扫描全部 topic：按各自 retention 时长清理过期 msg/（DeleteRange）
//     与对应 keyidx/ 条目
//   - 单趟工作量有上限（maxPurgePerQueue），超出留待下趟并记日志（不静默截断）
//
// 边界：
//   - 不清理 cursor/inflight：消费位点扫描天然越过已删区间；指向已删消息的
//     inflight 由 deliver 的孤儿清理兜底（M1 已实现并有用例钉住）
//   - msg 能按 offset 边界 DeleteRange（队列内 StoreAtMs 随 offset 单调）；
//     keyidx 按 key 排序，只能全扫按嵌入 storeMs 逐条删——中小规模可接受，
//     量级上来后的优化（时间副索引）留给真实瓶颈出现时
package retention

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// maxPurgePerQueue 单队列/单索引扫描单趟最多清理条数，防止单趟长时间占用。
const maxPurgePerQueue = 10000

// Manager 过期清理任务。单 goroutine 运行（Run），Pass 可单独调用（测试/未来 Admin 触发）。
type Manager struct {
	st       *store.Store
	mt       *meta.Meta
	interval time.Duration
	logger   *slog.Logger

	// 磁盘水位保护三件套（spec §7 拒写保读）：
	// dataDir 为探测目标（磁盘使用率按它所在的文件系统计算）；
	// watermarkPct<=0 或 writeBlocked 为 nil 时水位检查整体禁用。
	dataDir      string
	watermarkPct int
	writeBlocked *atomic.Bool
}

// New 构造清理任务。
//
// 参数：
//   - interval: 扫描间隔（config.RetentionInterval()）
//   - dataDir/watermarkPct/writeBlocked: 磁盘水位保护三件套；
//     watermarkPct<=0 或 writeBlocked 为 nil 时水位检查禁用
func New(st *store.Store, mt *meta.Meta, interval time.Duration, dataDir string,
	watermarkPct int, writeBlocked *atomic.Bool, logger *slog.Logger) *Manager {
	return &Manager{st: st, mt: mt, interval: interval, dataDir: dataDir,
		watermarkPct: watermarkPct, writeBlocked: writeBlocked, logger: logger.With("mod", "retention")}
}

// Run 阻塞运行清理循环：启动即跑一趟，此后每 interval 一趟；ctx 取消即返回。
// 调用方（main）负责放入独立 goroutine 并在停机时先取消再关 store。
func (m *Manager) Run(ctx context.Context) {
	m.logger.Info("retention 任务启动", "interval", m.interval.String())
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		m.checkDisk()
		if n, err := m.Pass(); err != nil {
			m.logger.Error("retention 清理失败", "err", err)
		} else if n > 0 {
			m.logger.Info("retention 清理完成", "purged_msgs", n)
		}
		select {
		case <-ctx.Done():
			m.logger.Info("retention 任务退出")
			return
		case <-t.C:
		}
	}
}

// checkDisk 探测磁盘用量并更新拒写开关。只在状态翻转时打日志（避免每趟刷屏）。
func (m *Manager) checkDisk() {
	if m.watermarkPct <= 0 || m.writeBlocked == nil {
		return
	}
	used, err := diskUsedPercent(m.dataDir)
	if err != nil {
		m.logger.Warn("磁盘水位检查失败，本趟跳过", "dir", m.dataDir, "err", err)
		return
	}
	blocked := used >= float64(m.watermarkPct)
	if blocked != m.writeBlocked.Load() {
		if blocked {
			m.logger.Error("磁盘使用率超过水位线，进入拒写保读", "used_pct", used, "watermark", m.watermarkPct)
		} else {
			m.logger.Info("磁盘使用率回落，恢复写入", "used_pct", used, "watermark", m.watermarkPct)
		}
		m.writeBlocked.Store(blocked)
	}
}

// Pass 执行一趟全量清理，返回清掉的消息条数（keyidx 条目不计入）。
func (m *Manager) Pass() (int, error) {
	now := time.Now().UnixMilli()
	total := 0
	for _, tc := range m.mt.Topics() {
		cutoff := now - tc.EffectiveRetention().Milliseconds()
		for q := uint32(0); q < tc.Queues; q++ {
			n, err := m.purgeQueue(tc.Name, q, cutoff)
			if err != nil {
				return total, fmt.Errorf("清理 %s/q%d: %w", tc.Name, q, err)
			}
			total += n
		}
		if err := m.purgeKeyIdx(tc.Name, cutoff); err != nil {
			return total, fmt.Errorf("清理 keyidx %s: %w", tc.Name, err)
		}
	}
	return total, nil
}

// purgeQueue 找到 [队首, 首条未过期) 边界并 DeleteRange 整段删除。
// 队列内消息按 offset 追加写入、StoreAtMs 单调不减，扫到首条未过期即可停。
// 注：单调性假设可能被时钟回跳（NTP 校时）短暂打破——被回跳越过停止边界的
// 过期消息会在后续趟次被清掉，只有延迟、不丢消息。
func (m *Manager) purgeQueue(topic string, q uint32, cutoff int64) (int, error) {
	pfx := store.MsgQueuePrefix(topic, q)
	var boundary uint64
	found := 0
	err := m.st.Scan(pfx, store.PrefixUpperBound(pfx), maxPurgePerQueue, func(k, v []byte) (bool, error) {
		msg, err := core.DecodeMessage(v)
		if err != nil {
			return false, fmt.Errorf("解码 %q: %w", k, err)
		}
		if msg.StoreAtMs >= cutoff {
			return false, nil
		}
		boundary = msg.Offset + 1
		found++
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	if found == 0 {
		return 0, nil
	}
	b := m.st.NewBatch()
	// DeleteRange 从 offset 0 起：此前趟次已删的区间为空集，重复覆盖无害
	b.DeleteRange(store.MsgKey(topic, q, 0), store.MsgKey(topic, q, boundary), nil)
	if err := m.st.Apply(b); err != nil {
		return 0, fmt.Errorf("DeleteRange 提交: %w", err)
	}
	if found == maxPurgePerQueue {
		m.logger.Info("retention 达单趟上限，剩余留待下趟", "topic", topic, "queue", q)
	}
	m.logger.Debug("retention 清理队列", "topic", topic, "queue", q, "purged", found, "boundary", boundary)
	return found, nil
}

// purgeKeyIdx 清理 topic 下 storeMs < cutoff 的索引条目（全扫逐删，见文件头边界说明）。
func (m *Manager) purgeKeyIdx(topic string, cutoff int64) error {
	pfx := store.KeyIdxTopicPrefix(topic)
	b := m.st.NewBatch()
	n := 0
	err := m.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		_, _, ms, _, _, perr := store.ParseKeyIdxKey(k)
		if perr != nil {
			return false, perr
		}
		if ms < cutoff {
			b.Delete(k, nil) // Batch 编码时即拷贝 key，回调切片可直接用
			n++
		}
		return n < maxPurgePerQueue, nil
	})
	if err != nil {
		b.Close()
		return err
	}
	if n == 0 {
		b.Close()
		return nil
	}
	if err := m.st.Apply(b); err != nil {
		return fmt.Errorf("索引删除提交: %w", err)
	}
	if n == maxPurgePerQueue {
		// 与 purgeQueue 同样的上限语义：n 触顶说明还有未扫描条目，剩余留待下趟
		m.logger.Info("retention 索引达单趟上限，剩余留待下趟", "topic", topic)
	}
	m.logger.Debug("retention 清理索引", "topic", topic, "purged_idx", n)
	return nil
}
