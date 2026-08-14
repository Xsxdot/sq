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
	"errors"
	"fmt"
	"log/slog"
	"sync"
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

	// moveMu 把「单个条目的两段式移入」关进一个临界区，撤回（Recall）取同一
	// 把锁。它保证撤回只能在**条目与条目之间**插入，永远不会落在某个条目的
	// 两段之中。
	//
	// 为什么这把锁不能"优化"掉：两段之间 delay 条目仍在、消息却已经写进
	// msg/（可被消费）。撤回若在此刻按"键还在→删掉→返回成功"处理，就是一个
	// **假成功**——客户端以为撤回了，消费者却已经收到。定时消息的典型用途是
	// 「超时未支付则关单」，假成功意味着订单被误关。
	//
	// 为什么是逐条而不是整趟：一趟最多 maxMovePerPass(512) 条、且可能含跨
	// 节点转发（一次网络往返），整趟持锁会把撤回阻塞到秒级。
	//
	// **这把锁单独不够**：Pass 的 Scan 在锁外，见 moveOne 开头的锁内重读。
	moveMu sync.Mutex
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
		ok, err := s.moveOne(ctx, d.key, d.raw)
		if ok {
			moved++
		}
		if err != nil {
			// 失败即中断本趟。moved 先计后判，语义与抽出前逐字一致：
			// 原代码的 moved++ 就在第二段之前，所以"第一段成功、第二段失败"
			// 时那一条也被计入。
			return moved, err
		}
	}
	return moved, nil
}

// moveOne 搬运单个到期条目，全程持 moveMu（与 Recall 互斥，理由见该字段注释）。
//
// 参数：key/raw 为 Pass 的 Scan 拷贝出的条目键与值。
//
// 返回：
//   - moved: 是否真的移入了一条消息。坏条目被清理、或条目已消失时为 false
//   - err: 失败原因。**moved 可能为 true 而 err 非 nil**（第一段成功、第二段
//     删条目失败）——调用方必须先计数再判错，否则会漏计一条已经入队的消息
func (s *Scheduler) moveOne(ctx context.Context, key, raw []byte) (bool, error) {
	s.moveMu.Lock()
	defer s.moveMu.Unlock()

	// 闸门二（锁内重读）：Pass 的 Scan 在锁外做。拷贝出 key/raw 之后、本函数
	// 拿到锁之前，Recall 可能已经删掉这个条目并向客户端返回了成功。此刻若照着
	// 陈旧的 raw 继续入队，就是撤回的假成功——条目还在不在，只有在锁内重读
	// 才算数。
	//
	// 三道闸门缺一不可（spec §3.1）：
	//   - moveMu 定序删除与搬运
	//   - 锁内重读把「Scan 在锁外」这个事实兜住
	//   - Recall 的 dueMs>now 挡住「第二段删除失败留下的残条目」
	// 只有 moveMu 时下面这条交错原样成立：Recall 持锁过判据 → 调度器无锁
	// Scan 到该条目 → Recall 提交删除并返回成功 → moveOne 照陈旧 raw 入队。
	//
	// raw 仍可直接解码：delay 条目一经写入不再改写，键在则内容不变。
	if _, ok, err := s.st.Get(key); err != nil {
		return false, fmt.Errorf("重读 delay 条目 (key=%q): %w", key, err)
	} else if !ok {
		s.logger.Debug("delay 条目在搬运前已消失（多半是被撤回），本条跳过",
			"key", fmt.Sprintf("%q", key))
		return false, nil
	}

	m, err := core.DecodeMessage(raw)
	if err != nil {
		// 坏条目永远无法投递，留着只会每 scanInterval 重扫一次、重报一次
		// ——与 deliver 清理孤儿 inflight 同理，删除止损并 Error 留痕
		s.logger.Error("delay 条目解码失败，删除坏条目", "key", fmt.Sprintf("%q", key), "err", err)
		b := s.st.NewBatch()
		b.Delete(key)
		// 坏条目归元数据组（delay/ 键族与队列无关）
		if err := s.rep.Apply(ctx, s.rt.MetaGroup(), b); err != nil {
			return false, fmt.Errorf("删除坏 delay 条目: %w", err)
		}
		return false, nil
	}
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
			return false, fmt.Errorf("延时消息移入 (msg_id=%s topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
		}
	} else {
		qid, off, ferr := s.fwd.ForwardAppend(ctx, g, raw)
		if ferr != nil {
			// 失败即中断本趟：条目未删除，下一趟从头重扫自然重试
			return false, fmt.Errorf("转发延时消息移入 (msg_id=%s topic=%s due=%d g=%d): %w", m.ID, m.Topic, m.DeliverAtMs, g, ferr)
		}
		// 转发只回坐标：回填用于第二段日志核对（源条目坐标即 leader 侧分配值）
		m.QueueID = qid
		m.Offset = off
		s.logger.Info("延时消息跨节点转发", "msg_id", m.ID, "topic", m.Topic,
			"queue", qid, "offset", off, "g", g, "due_ms", m.DeliverAtMs)
	}
	// afterAppendHook 必须原样保留在第一段之后、第二段之前：
	// TestDelayMoveRedeliversOnCrashBetweenPhases 靠它注入两段之间的崩溃。
	// 挪位置或漏掉都会让那条既有用例失效。
	if s.afterAppendHook != nil {
		s.afterAppendHook()
	}
	// 第二段：独立批次删 delay 条目。失败只记 Error 不回滚第一段——消息已
	// 入队是既成事实，条目残留 = 下趟重搬 = 重复（可接受）；日志把两段坐标
	// 都带上，便于运维按 msg_id 在两边核对。第二段归元数据组——调度器只在
	// meta leader 上跑，本节点必是 leader，直接 rep.Apply 无需转发。
	b := s.st.NewBatch()
	b.Delete(key)
	if err := s.rep.Apply(ctx, s.rt.MetaGroup(), b); err != nil {
		s.logger.Error("延时消息已入队但 delay 条目删除失败——条目残留，下趟重搬将产生重复投递（at-least-once 允许）",
			"msg_id", m.ID, "topic", m.Topic, "queue", m.QueueID, "offset", m.Offset,
			"due_ms", m.DeliverAtMs, "delay_key", fmt.Sprintf("%q", key), "err", err)
		// 返回 true：消息已入队，这一条必须被 Pass 计入 moved
		return true, fmt.Errorf("删除 delay 条目 (msg_id=%s topic=%s q=%d off=%d due=%d): %w",
			m.ID, m.Topic, m.QueueID, m.Offset, m.DeliverAtMs, err)
	}
	s.logger.Debug("延时消息已移入队列", "msg_id", m.ID, "topic", m.Topic,
		"queue", m.QueueID, "offset", m.Offset, "due_ms", m.DeliverAtMs)
	return true, nil
}

// 撤回失败的三种业务原因。协议层按它们选 Status Code（见 rpc/recall.go）：
// TooLate 与 NotFound 都映射成 MESSAGE_NOT_FOUND（对客户端而言"没赶上"与
// "已经不在了"行为完全相同），TopicMismatch 映射成 BAD_REQUEST。
// 二者在服务端日志里用不同措辞区分——**日志是区分它俩的唯一手段**。
var (
	// ErrRecallTooLate 消息已过投递时间，撤回窗口已关闭。
	ErrRecallTooLate = errors.New("delay: 消息已过投递时间，无法撤回")
	// ErrRecallNotFound 延时条目不存在：已被投递、或已被撤回过。
	ErrRecallNotFound = errors.New("delay: 延时条目不存在（已投递或已撤回）")
	// ErrRecallTopicMismatch 句柄指向的条目属于别的 topic。
	ErrRecallTopicMismatch = errors.New("delay: 条目 topic 与请求 topic 不一致")
)

// Recall 撤回一条尚未到期的延时消息，返回被撤回消息的 ID。
//
// 参数：topic 请求体里的 topic（用于纵深防御校验）；dueMs/seq 来自 recall
// 句柄，二者构成 store.DelayKey。
//
// 返回：被撤回消息的 ID；失败时返回上面三个哨兵错误之一，或
// replication.ErrNotLeader（本节点不是 meta leader）。
//
// 四条判据按序执行，**判据 1 在锁外，判据 2–4 与随后的删除全部在同一次持锁
// 期间完成**：
//
//  1. 本节点是 meta leader（锁外）
//  2. dueMs > now（持锁）—— spec §3.1 的闸门三
//  3. 条目存在（持锁）
//  4. 条目里的消息 topic 与请求 topic 一致（持锁）
//
// **删除必须在持锁期间完成，不能"判定完释放锁再删"**——那样会把假成功窗口
// 原封不动地重新打开：判定说「还没到期、条目还在」，释放锁后调度器插进来把
// 它搬走，撤回的删除落到一个已经被搬走的键上（删除空键不报错），于是返回
// 成功而消息已经投出去了。
//
// 代价是持锁跨越一次复制提交（集群档含 quorum 往返）。这与调度器持锁跨越
// 一次跨节点转发追加是同一量级，且两者都不在消息收发热路径上。
func (s *Scheduler) Recall(ctx context.Context, topic string, dueMs int64, seq uint64) (string, error) {
	// 判据 1（锁外）：撤回必须在 meta leader 上执行。
	//
	// 注意约束的**不是「能不能写」**——raft 的 Propose 在 follower 上会转发给
	// leader，删除提案本身在任何节点都能发起。约束的是「能不能与调度器互斥」：
	// moveMu 是进程内互斥量，而调度器只在 meta leader 上跑。这一点容易看反。
	if !s.rt.IsLeader(s.rt.MetaGroup()) {
		// Debug 而非 Warn：集群档下这是高频路径（SDK 按 topic 路由
		// RecallMessage，并不知道谁是 meta leader），Warn 会刷屏
		s.logger.Debug("非 meta leader，撤回请求拒答",
			"topic", topic, "due_ms", dueMs, "seq", seq)
		return "", replication.ErrNotLeader
	}

	s.moveMu.Lock()
	defer s.moveMu.Unlock()

	// 判据 2（闸门三）：必须尚未到投递时间。它挡的是「第二段删除失败留下的
	// 残条目」——消息已入队而条目仍在，此时条目的 due 必已落在过去。
	if now := time.Now().UnixMilli(); dueMs <= now {
		s.logger.Debug("撤回失败：已过投递时间（消息可能已投递或正在投递）",
			"topic", topic, "due_ms", dueMs, "now_ms", now, "seq", seq)
		return "", ErrRecallTooLate
	}

	key := store.DelayKey(dueMs, seq)
	v, ok, err := s.st.Get(key)
	if err != nil {
		return "", fmt.Errorf("读取 delay 条目 (topic=%s due=%d seq=%d): %w", topic, dueMs, seq, err)
	}
	// 判据 3
	if !ok {
		s.logger.Debug("撤回失败：条目不存在（已被投递或已撤回过）",
			"topic", topic, "due_ms", dueMs, "seq", seq)
		return "", ErrRecallNotFound
	}
	m, err := core.DecodeMessage(v)
	if err != nil {
		return "", fmt.Errorf("解码 delay 条目 (topic=%s due=%d seq=%d): %w", topic, dueMs, seq, err)
	}
	// 判据 4（纵深防御）：句柄已验签，topic 理应一致；不一致说明客户端拿错了
	// 句柄，宁可拒绝也不要跨 topic 删除。
	if m.Topic != topic {
		s.logger.Warn("撤回失败：条目 topic 与请求 topic 不一致，拒绝跨 topic 删除",
			"req_topic", topic, "entry_topic", m.Topic, "due_ms", dueMs, "seq", seq)
		return "", ErrRecallTopicMismatch
	}

	b := s.st.NewBatch()
	b.Delete(key)
	if err := s.rep.Apply(ctx, s.rt.MetaGroup(), b); err != nil {
		return "", fmt.Errorf("删除 delay 条目 (topic=%s msg_id=%s due=%d seq=%d): %w",
			topic, m.ID, dueMs, seq, err)
	}
	// 撤回是有外部影响的状态变更，不能静默
	s.logger.Info("延时消息已撤回", "topic", topic, "msg_id", m.ID, "due_ms", dueMs, "seq", seq)
	return m.ID, nil
}
