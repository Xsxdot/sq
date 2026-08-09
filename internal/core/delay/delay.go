// Package delay 实现延时消息调度：扫描 delay/ 暂存区头部，到期条目移入
// 目标 topic 的正常队列（spec §5 流程 3 后半）。
//
// 职责：
//   - 周期（scanInterval）扫描 [DelayPrefix, DelayScanUpperBound(now))，
//     到期条目经 produce.Append 写入 msg/（第一段），再独立批次删除
//     delay 条目（第二段）
//   - 单趟预算 maxMovePerPass，满额立即续趟排空积压（不等下个 tick）
//
// 边界：
//   - 不感知协议；不管投递/重试/DLQ——移入 msg/ 后就是普通消息，一切
//     消费语义由 deliver 负责
//   - 崩溃恢复零代码：暂存区在 Pebble，重启后从头扫描即恢复；移入是
//     两段式（先写后删），崩溃窗口存在"已入 msg/ 但 delay 条目残留"的
//     中间态 = 重放重复投递，at-least-once 语义内
//   - 时钟回拨（NTP 校时）只会让扫描上界暂时变小、到期条目晚一点被搬运
//     ——仅延迟投递，不丢失不提前（spec §7 时钟策略）
//
// 组归属（batch③）：delay/ 暂存区键族归元数据组（rt.MetaGroup()）——
// 暂存条目未选队，无 GroupForQueue 映射；移入第一段（消息追加）归目标
// 队列组，本节点非目标组 leader 时经 fwd 转发（见 Pass 内分支注释）。
// 因此调度器是 leader-only 定时器（Task 8 门控）：只在 meta leader 上
// 跑，非 leader 节点整趟跳过等待（leadership 可能随时轮到自己，不退出
// 循环）；获得/失去 meta leadership 各打一条 Info，是「延时消息为什么
// 不动了」的第一线索。
package delay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/replication"
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
	rep    replication.Replicator
	rt     replication.Router
	fwd    replication.Forwarder // 跨节点转发（集群档）；单机档 nil——IsLeader 恒真，转发分支不可达
	st     *store.Store
	pr     *produce.Producer
	logger *slog.Logger

	// afterAppendHook 测试专用注入钩子（生产恒为 nil）：在第一段（消息入队）
	// 成功后、第二段（删 delay 条目）前调用，用于模拟两段之间进程崩溃。
	// 生产代码绝不允许设置。
	afterAppendHook func()
}

// New 构造调度器。rep/rt 为复制抽象与组路由视图（单机档传
// replication.NewStandalone(st) 与 StandaloneRouter{}，集群档由 main 装配）；
// fwd 从 rt 断言取得——集群档的 rt 即 *replication.Cluster（同时实现
// Forwarder），单机档的 StandaloneRouter 不实现 Forwarder，断言得 nil；
// 单机 IsLeader 恒真，转发分支不可达，nil 不会解引用。
func New(rep replication.Replicator, rt replication.Router, st *store.Store,
	pr *produce.Producer, logger *slog.Logger) *Scheduler {
	fwd, _ := rt.(replication.Forwarder)
	return &Scheduler{rep: rep, rt: rt, fwd: fwd, st: st, pr: pr,
		logger: logger.With("mod", "delay")}
}

// Run 阻塞运行调度循环：启动即跑一趟，此后每 scanInterval 一趟；单趟满额
// （moved==maxMovePerPass）说明还有积压，立即续跑不等 tick。ctx 取消即返回。
// 调用方（main）负责放入独立 goroutine 并在停机时先取消再关 store。
//
// leader-only 门控：每趟开头先查 meta 组 leadership——非 leader 只等
// tick 不干活（delay/ 键族归 meta 组，非 leader 的写入会被拒），但
// 绝不退出循环：leadership 可能随时轮到自己，退出即永久停摆。
func (s *Scheduler) Run(ctx context.Context) {
	s.logger.Info("delay 调度器启动", "interval", scanInterval.String())
	t := time.NewTicker(scanInterval)
	defer t.Stop()
	// metaLeader 记录上一个 tick 的 meta 组 leadership：翻转即「开始/
	// 停止承担调度」的判定面。单机档 IsLeader 恒真，只在启动时报一次
	// 开始承担——这行 Info 是运维定位「延时消息为什么不动了」的第一
	// 线索（集群档 leader 变更时它会先于症状出现）。
	metaLeader := false
	for {
		isLeader := s.rt.IsLeader(s.rt.MetaGroup())
		switch {
		case isLeader && !metaLeader:
			s.logger.Info("本节点开始承担 delay 调度")
		case !isLeader && metaLeader:
			s.logger.Info("本节点停止承担 delay 调度")
		}
		metaLeader = isLeader
		if !isLeader {
			// 门控跳过：每趟都会发生，Debug 级避免刷屏（Info 刷屏会
			// 淹没真正的调度活动日志）
			s.logger.Debug("非 meta leader，delay 本趟跳过")
			select {
			case <-ctx.Done():
				s.logger.Info("delay 调度器退出")
				return
			case <-t.C:
			}
			continue
		}
		moved, err := s.Pass(ctx)
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
func (s *Scheduler) Pass(ctx context.Context) (int, error) {
	// leader-only 门控：delay/ 键族归 meta 组，只有 meta leader 才能
	// 搬移（第一段写 msg/ 归队列组可经 fwd 转发，但第二段删 delay 条目
	// 归 meta 组必须本节点提案——非 leader 直接跳过整趟）。Run 已按同
	// 一条件跳过本趟，直接调用 Pass（测试/未来 Admin 触发）也须自守。
	if !s.rt.IsLeader(s.rt.MetaGroup()) {
		s.logger.Debug("非 meta leader，delay 趟跳过")
		return 0, nil
	}
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
			b.Delete(d.key)
			// 坏条目归元数据组（delay/ 键族与队列无关）
			if err := s.rep.Apply(ctx, s.rt.MetaGroup(), b); err != nil {
				return moved, fmt.Errorf("删除坏 delay 条目: %w", err)
			}
			continue
		}
		key := d.key
		// 两段式移入：第一段写目标（msg/，正常分配队列与 offset、写 keyidx、
		// 唤醒长轮询），第二段独立批次删 delay 条目。先写后删；崩溃窗口（第一段
		// 落盘后、第二段前）重放 = 重复投递，at-least-once 语义内——条目残留 =
		// 下趟重搬 = 目标队列多一条同 ID 消息，消费端按 ID 幂等即可。次序不得
		// 反转（先删后写 = 崩溃丢消息，绝不允许）。
		//
		// 第一段的目标队列组与本节点 leader 关系不定（调度器在 meta leader 上
		// 跑，但目标队列可能属于别的组）：本节点是目标组 leader 时本地
		// pr.Append（offset 分配在本节点）；否则经 fwd.ForwardAppend 把消息字节
		// 交给目标组 leader 追加（leader-only 构造的跨节点延伸）。
		// 暂存消息此刻 QueueID 为零（未选队），转发组号按
		// rt.GroupForQueue(m.Topic, m.QueueID) 计算——选队由 leader 侧 produce
		// 栈完成，发起方按此组寻址。
		g := s.rt.GroupForQueue(m.Topic, m.QueueID)
		if s.rt.IsLeader(g) {
			if _, err := s.pr.Append(ctx, m); err != nil {
				// 失败即中断本趟：条目未删除，下一趟从头重扫自然重试
				return moved, fmt.Errorf("延时消息移入 (msg_id=%s topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
			}
		} else {
			qid, off, ferr := s.fwd.ForwardAppend(ctx, g, d.raw)
			if ferr != nil {
				// 失败即中断本趟：条目未删除，下一趟从头重扫自然重试
				return moved, fmt.Errorf("转发延时消息移入 (msg_id=%s topic=%s due=%d g=%d): %w", m.ID, m.Topic, m.DeliverAtMs, g, ferr)
			}
			// 转发只回坐标：回填用于第二段日志核对（源条目坐标即 leader 侧分配值）
			m.QueueID = qid
			m.Offset = off
			s.logger.Info("延时消息跨节点转发", "msg_id", m.ID, "topic", m.Topic,
				"queue", qid, "offset", off, "g", g, "due_ms", m.DeliverAtMs)
		}
		if s.afterAppendHook != nil {
			s.afterAppendHook()
		}
		moved++
		// 第二段：独立批次删 delay 条目。失败只记 Error 不回滚第一段——消息已
		// 入队是既成事实，条目残留 = 下趟重搬 = 重复（可接受）；日志把两段坐标
		// 都带上，便于运维按 msg_id 在两边核对。第二段归元数据组——调度器只在
		// meta leader 上跑（Task 8 门控），本节点必是 leader，直接 rep.Apply
		// 无需转发。
		b := s.st.NewBatch()
		b.Delete(key)
		if err := s.rep.Apply(ctx, s.rt.MetaGroup(), b); err != nil {
			s.logger.Error("延时消息已入队但 delay 条目删除失败——条目残留，下趟重搬将产生重复投递（at-least-once 允许）",
				"msg_id", m.ID, "topic", m.Topic, "queue", m.QueueID, "offset", m.Offset,
				"due_ms", m.DeliverAtMs, "delay_key", fmt.Sprintf("%q", key), "err", err)
			return moved, fmt.Errorf("删除 delay 条目 (msg_id=%s topic=%s q=%d off=%d due=%d): %w",
				m.ID, m.Topic, m.QueueID, m.Offset, m.DeliverAtMs, err)
		}
		s.logger.Debug("延时消息已移入队列", "msg_id", m.ID, "topic", m.Topic,
			"queue", m.QueueID, "offset", m.Offset, "due_ms", m.DeliverAtMs)
	}
	return moved, nil
}
