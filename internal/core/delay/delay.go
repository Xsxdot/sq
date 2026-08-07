// Package delay 实现延时消息调度：扫描 delay/ 暂存区头部，到期条目移入
// 目标 topic 的正常队列（spec §5 流程 3 后半）。
//
// 职责：
//   - 周期（scanInterval）扫描 [DelayPrefix, DelayScanUpperBound(now))，
//     到期条目经 produce.AppendWith 写入 msg/ 并同批原子删除 delay 条目
//   - 单趟预算 maxMovePerPass，满额立即续趟排空积压（不等下个 tick）
//
// 边界：
//   - 不感知协议；不管投递/重试/DLQ——移入 msg/ 后就是普通消息，一切
//     消费语义由 deliver 负责
//   - 崩溃恢复零代码：暂存区在 Pebble，重启后从头扫描即恢复；移入是
//     单批原子操作，不存在"已入 msg/ 但 delay 条目残留"的中间态
//   - 时钟回拨（NTP 校时）只会让扫描上界暂时变小、到期条目晚一点被搬运
//     ——仅延迟投递，不丢失不提前（spec §7 时钟策略）
package delay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// scanInterval 扫描间隔。100ms 即延时精度的上界（spec §5：调度器每 100ms
// 扫头部），对"秒级延时"的承诺足够。var 而非 const：测试需注入小值。
var scanInterval = 100 * time.Millisecond

// maxMovePerPass 单趟最多搬运条数：单趟工作量必须有上界，否则大量同时
// 到期的消息会让一趟扫描长时间占用，期间新写入的 fsync 全部排在后面。
// var 而非 const：测试注入小值验证预算与排空行为。
var maxMovePerPass = 512

// Scheduler 延时消息调度器。单 goroutine 运行（Run），Pass 可单独调用（测试用）。
type Scheduler struct {
	st     *store.Store
	pr     *produce.Producer
	logger *slog.Logger
}

// New 构造调度器。
func New(st *store.Store, pr *produce.Producer, logger *slog.Logger) *Scheduler {
	return &Scheduler{st: st, pr: pr, logger: logger.With("mod", "delay")}
}

// Run 阻塞运行调度循环：启动即跑一趟，此后每 scanInterval 一趟；单趟满额
// （moved==maxMovePerPass）说明还有积压，立即续跑不等 tick。ctx 取消即返回。
// 调用方（main）负责放入独立 goroutine 并在停机时先取消再关 store。
func (s *Scheduler) Run(ctx context.Context) {
	s.logger.Info("delay 调度器启动", "interval", scanInterval.String())
	t := time.NewTicker(scanInterval)
	defer t.Stop()
	for {
		moved, err := s.Pass()
		if err != nil {
			// 单趟失败只记日志不退出：store 瞬时故障恢复后下一趟自然重试，
			// 头部条目还在原地（移入失败不会删除条目）
			s.logger.Error("delay 调度趟失败", "err", err)
		} else if moved > 0 {
			s.logger.Info("延时消息到期移入", "moved", moved)
		}
		if err == nil && moved == maxMovePerPass {
			continue // 满额=可能还有积压，立即续趟
		}
		select {
		case <-ctx.Done():
			s.logger.Info("delay 调度器退出")
			return
		case <-t.C:
		}
	}
}

// Pass 执行一趟到期搬运，返回移入 msg/ 的条数（被清理的坏条目不计入）。
func (s *Scheduler) Pass() (int, error) {
	now := time.Now().UnixMilli()
	// 先收集后搬运：Scan 回调的 k/v 仅回调期间有效，且回调里不能再开写
	// 事务（迭代器与写入交错），必须拷贝出来
	type due struct {
		key []byte
		raw []byte
	}
	var dues []due
	lower := []byte(store.DelayPrefix)
	err := s.st.Scan(lower, store.DelayScanUpperBound(now), maxMovePerPass, func(k, v []byte) (bool, error) {
		dues = append(dues, due{key: append([]byte(nil), k...), raw: append([]byte(nil), v...)})
		return true, nil
	})
	if err != nil {
		return 0, fmt.Errorf("扫描 delay 暂存区: %w", err)
	}
	moved := 0
	for _, d := range dues {
		m, err := core.DecodeMessage(d.raw)
		if err != nil {
			// 坏条目永远无法投递，留着只会每 scanInterval 重扫一次、重报一次
			// ——与 deliver 清理孤儿 inflight 同理，删除止损并 Error 留痕
			s.logger.Error("delay 条目解码失败，删除坏条目", "key", fmt.Sprintf("%q", d.key), "err", err)
			b := s.st.NewBatch()
			b.Delete(d.key, nil)
			if err := s.st.Apply(b); err != nil {
				return moved, fmt.Errorf("删除坏 delay 条目: %w", err)
			}
			continue
		}
		key := d.key
		// 写 msg/（正常分配队列与 offset、写 keyidx、唤醒长轮询）+ 删 delay
		// 条目，同一 Batch 原子提交：崩溃窗口内要么都发生要么都不发生，
		// 不存在丢失或重复投递
		if _, err := s.pr.AppendWith(m, func(b *pebble.Batch) { b.Delete(key, nil) }); err != nil {
			// 失败即中断本趟：条目未删除，下一趟从头重扫自然重试
			return moved, fmt.Errorf("延时消息移入 (msg_id=%s topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
		}
		moved++
		s.logger.Debug("延时消息已移入队列", "msg_id", m.ID, "topic", m.Topic,
			"queue", m.QueueID, "offset", m.Offset, "due_ms", m.DeliverAtMs)
	}
	return moved, nil
}
