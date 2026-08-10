// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责：
//   - Receive：先重投本队列已过期的 inflight，再从 fetch 位点取新消息；
//     新取消息写 inflight 并推进位点（同一 Batch 原子提交）
//   - Ack/ChangeInvisible：按 (group,topic,queue,offset,attempt) 定位 inflight——
//     attempt 是消费者持有句柄的一部分，防止陈旧句柄误伤被重投覆盖的新记录
//     （见类型注释与方法注释的详细说明）
//   - 长轮询：produce.Subscribe 的 close-broadcast + 截止时间
//
// 边界：
//   - 重投是惰性的（Receive 时检查），M1 无后台扫描 goroutine——
//     没有消费者在收时也就没有人需要重投，语义等价而实现最简
//   - 投递次数超上限的消息在重投检查时惰性转入死信 %DLQ%{group}（1 队列）；
//     重投指数退避靠不可见时长下限实现，详见 retryBackoff 注释
//   - Tag 过滤已落地（"*"/"tagA"/"tagA || tagB"），SQL92 属性过滤属 v1.1
//   - 顺序消息（M4）：队列级顺序锁，MessageGroup 非空即顺序；重投无退避、超限入 DLQ 解锁
//
// 位点语义说明（对应 spec §5.2"推进到最小未 ack"的实现形态）：
//
//	cursor 是 fetch 位点，Receive 取出即推进；未 ack 的消息由持久化的
//	inflight 记录兜底（崩溃重启后仍在，过期即重投）。两者合起来等价于
//	"已 ack 前消息不丢"，且比维护最小未 ack 位点简单得多。
//
// 组归属（batch③）：本包全部写点（inflight/ 与 cursor/ 键族）都归「被消费
// 队列」的组——组号统一 rt.GroupForQueue(topic, queueID)，经 rep 提交。
// 同一队列的取件/确认/改不可见共享同组：组内 raft 定序即队列内 FIFO 定序。
package deliver

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
)

// scanBudget 单次取件最多检视的新消息条数。Tag 过滤可能连续跳过大量不匹配
// 消息，必须限制单趟工作量；位点照常推进，下一趟从新位点继续，保证前进性。
// M1 时 Scan 的 limit 用 maxMsgs-len(out)（检视数=投递数）；过滤下两者分离：
// 检视上限用本常量，投递上限由回调内 len(out) < maxMsgs 控制。
const scanBudget = 1024

// 重试退避参数（spec §5 流程 6：非顺序消息重试指数退避，靠重投时的不可见时长实现）。
// 第 n 次投递（n≥2）的不可见时长下限 = base × 2^(n-2)，封顶 max：10s, 20s, 40s … 5min。
// 客户端要求的 invisible 更长时取客户端值。
// var 而非 const：测试需注入小值控制用例时长；运行期只读。
var (
	retryBackoffBase = 10 * time.Second
	retryBackoffMax  = 5 * time.Minute
)

// retryBackoff 第 attempts 次投递的退避下限（attempts >= 2；乘法防溢出走上限）。
func retryBackoff(attempts int32) time.Duration {
	d := retryBackoffBase
	for i := int32(2); i < attempts; i++ {
		d *= 2
		if d >= retryBackoffMax {
			return retryBackoffMax
		}
	}
	if d >= retryBackoffMax {
		return retryBackoffMax
	}
	return d
}

// RetryBackoff 导出包装，rpc 层回填 InvisibleDuration 用同一公式。
// attempts<2 的语义由调用方保证（首投无退避，调用方只在 attempt>=2 时调用）。
func RetryBackoff(attempts int32) time.Duration {
	return retryBackoff(attempts)
}

// Deliverer POP 消费引擎。并发安全：同一队列的取件/确认/改不可见时间全部在
// 该队列的 qmu 临界区内执行（Receive 经 receiveOnce、Ack/AckBatch、
// ChangeInvisible 三者都在方法开头取同一把队列锁），不同队列并行。
// 三者共同读写同一片 inflight 键空间，若只有 Receive 持锁而 Ack/
// ChangeInvisible 不持锁，会出现"重投覆盖后的记录被陈旧 Ack 误删"
// "ChangeInvisible 的读-改-写覆盖掉并发重投写入的新 attempts"等竞态——
// 因此上面这句"并发安全"是有条件的：它成立的前提就是这三个方法统一持队列锁，
// 新增任何直接读写 inflight 的方法时必须一并纳入，否则这句话立刻不再成立。
type Deliverer struct {
	rep    replication.Replicator
	rt     replication.Router
	fwd    replication.Forwarder // 跨节点转发（集群档）；单机档 nil——IsLeader 恒真，转发分支不可达
	st     *store.Store
	mt     *meta.Meta
	pr     *produce.Producer
	logger *slog.Logger

	mu  sync.Mutex
	qmu map[string]*sync.Mutex // "group/topic/qid" -> 队列级锁

	// afterAppendHook 测试专用注入钩子（生产恒为 nil）：在 moveToDLQ 第一段
	// （死信消息写入）成功后、第二段（删源 inflight）前调用，用于模拟两段
	// 之间进程崩溃。生产代码绝不允许设置。
	afterAppendHook func()
}

// New 构造 Deliverer。rep/rt 为复制抽象与组路由视图：单机档传
// replication.NewStandalone(st) 与 StandaloneRouter{}，集群档由 main 装配。
// fwd 从 rt 断言取得——集群档的 rt 即 *replication.Cluster（同时实现
// Forwarder），单机档的 StandaloneRouter 不实现 Forwarder，断言得 nil；
// 单机 IsLeader 恒真，转发分支不可达，nil 不会解引用（与 txn/delay 同款）。
func New(rep replication.Replicator, rt replication.Router, st *store.Store,
	mt *meta.Meta, pr *produce.Producer, logger *slog.Logger) *Deliverer {
	fwd, _ := rt.(replication.Forwarder)
	return &Deliverer{rep: rep, rt: rt, fwd: fwd, st: st, mt: mt, pr: pr,
		logger: logger.With("mod", "deliver"), qmu: map[string]*sync.Mutex{}}
}

// lockQueue 返回 (group,topic,queueID) 对应的队列级锁，不存在则创建。
// 仅在 d.mu 临界区内读写 qmu 这张 map 本身；返回的锁在临界区外由调用方
// 自行 Lock/Unlock——绝不能在持有 d.mu 时去 Lock 队列锁，否则与
// receiveOnce 内部逻辑组合可能出现锁序不一致的死锁风险。
func (d *Deliverer) lockQueue(group, topic string, q uint32) *sync.Mutex {
	k := fmt.Sprintf("%s/%s/%d", group, topic, q)
	d.mu.Lock()
	defer d.mu.Unlock()
	m, ok := d.qmu[k]
	if !ok {
		m = &sync.Mutex{}
		d.qmu[k] = m
	}
	return m
}

// Receive 取一批消息，返回的每条消息 DeliveryAttempt 已填充（首投=1）。
// 先重投本队列已过期的 inflight，再从 fetch 位点取新消息，合计不超过 maxMsgs。
// 空结果时最长等待 wait（长轮询），期间新消息写入会立即唤醒重试；
// wait<=0 时不等待，取一次就返回。
// filter 非 nil 时只投 tag 命中的新消息：不匹配的跳过并推进本组位点，
// 不投递、不占 inflight（对该组永久跳过）。阶段 1 的过期重投不重新过滤。
func (d *Deliverer) Receive(ctx context.Context, group, topic string, queueID uint32, maxMsgs int, invisible, wait time.Duration, filter *TagFilter) ([]*core.Message, error) {
	gc, err := d.mt.EnsureGroup(ctx, group)
	if err != nil {
		return nil, err
	}
	// 读屏障挂在 Receive 入口而不是每次 receiveOnce：一次 Receive 是一次
	// 长轮询批次，屏障成本（一次多数派心跳往返）摊到整批上可以忽略；挂在
	// 内层循环里会让每 100ms 的兜底轮询都付一次往返。
	// 屏障关闭 / 单机档时这里恒 nil，零开销。
	if err := d.rt.ReadBarrier(ctx, d.rt.GroupForQueue(topic, queueID)); err != nil {
		d.logger.Warn("消费读屏障未通过，拒绝投递（避免投出过期数据）",
			"group", group, "topic", topic, "queue", queueID, "err", err)
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		// 先订阅再取件：若取件为空后才订阅，"取件为空 → 新消息写入 → 才订阅"
		// 这个窗口期内的写入会错过 close 广播，导致长轮询白等到超时——
		// 订阅在前，即便取件与写入之间发生竞态，wakeCh 也一定能收到这次唤醒。
		wakeCh := d.pr.Subscribe(topic)
		msgs, err := d.receiveOnce(ctx, group, topic, queueID, maxMsgs, invisible, gc.EffectiveMaxAttempts(), filter)
		if err != nil || len(msgs) > 0 {
			return msgs, err
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return nil, nil
		}
		// 100ms 兜底轮询：wakeCh 唤醒信号只覆盖"新消息写入"这一种情况，
		// 不覆盖"inflight 过期可重投"——过期是时间驱动的，没有事件可订阅，
		// 只能靠轮询发现。
		tick := 100 * time.Millisecond
		if remain < tick {
			tick = remain
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wakeCh:
		case <-time.After(tick):
		}
	}
}

// receiveOnce 单次取件：锁内定序（receiveOnceLocked），锁外等待持久化。
// inflight 与 cursor 落盘之前绝不交件（语义红线 1/3——消费者拿到消息时，
// 它的 inflight 兜底记录必然已持久化，崩溃后该消息仍会被重投而不是丢失）。
// 解锁后同队列的下一次取件/确认立即可进锁定序，与本批共享同一次 fsync——
// 队列内 group commit 在消费路径生效的机制，与 produce 侧完全同款。
func (d *Deliverer) receiveOnce(ctx context.Context, group, topic string, queueID uint32, maxMsgs int, invisible time.Duration, maxAttempts int32, filter *TagFilter) ([]*core.Message, error) {
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	out, pending, applied, err := d.receiveOnceLocked(ctx, group, topic, queueID, maxMsgs, invisible, maxAttempts, filter)
	qlock.Unlock()
	if err != nil || !applied {
		return out, err
	}
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待取件批次持久化 (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	return out, nil
}

// receiveOnceLocked 单次取件的锁内部分：过期 inflight 重投 + 新消息扫描
// （可带 Tag 过滤），合计不超过 maxMsgs；批次组装完成后 ApplyAsync 定序。
// 调用方必须持有该队列的 qlock；fsync 等待由调用方在锁外完成。
//
// 返回：(消息, pending, applied, error)。applied=false 表示本轮无暂存写入
// （批次已 Close 回收），pending 为零值，调用方无需 Wait。
func (d *Deliverer) receiveOnceLocked(ctx context.Context, group, topic string, queueID uint32, maxMsgs int, invisible time.Duration, maxAttempts int32, filter *TagFilter) ([]*core.Message, replication.Pending, bool, error) {
	now := time.Now().UnixMilli()
	expireAt := now + invisible.Milliseconds()
	var out []*core.Message
	b := d.st.NewBatch()
	// staged 标记本次 batch 是否真的有待提交的写入（重投更新 / 新取写入 / 孤儿清理删除）。
	// 决定收尾走 Close（未提交，回收批次）还是 Apply（提交批次）——
	// 批次只有这两条合法终止路径（store.NewBatch 文档），不可两者都做也不可都不做。
	staged := false

	// 阶段 1：重投过期 inflight。k/v 回调内有效，需解析后立即使用
	type redeliver struct {
		offset   uint64
		attempts int32
		ordered  bool
	}
	var reds []redeliver
	// orderedBusy：本队列是否存在未终结的顺序 inflight（顺序锁的内存判据，
	// spec §5 流程 4）。不变式「每队列至多 1 条 Ordered inflight」由阶段 2 的
	// 投递门维护，因此单个 bool 足够，无需计数。
	orderedBusy := false
	pfx := store.InflightPrefix(group, topic, queueID)
	err := d.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		_, _, _, off, err := store.ParseInflightKey(k)
		if err != nil {
			return false, err
		}
		ist, err := core.DecodeInflight(v)
		if err != nil {
			return false, err
		}
		if ist.Ordered {
			orderedBusy = true
		}
		if ist.ExpireAtMs <= now && len(reds) < maxMsgs {
			reds = append(reds, redeliver{offset: off, attempts: ist.Attempts, ordered: ist.Ordered})
		}
		// M1-M3 在收满 maxMsgs 个重投候选后提前停扫；M4 起必须看完整个队列的
		// inflight——顺序锁判据 orderedBusy 需要完整视野，提前停会漏看排在
		// 后面的 Ordered 记录，导致顺序锁形同虚设。代价可控：单队列 inflight
		// 条数以未 ack 消息数为上界，远小于消息总量。
		return true, nil
	})
	if err != nil {
		b.Close() // 未提交，按批次生命周期契约自行回收
		return nil, nil, false, fmt.Errorf("扫描 inflight (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	for _, r := range reds {
		raw, ok, err := d.st.Get(store.MsgKey(topic, queueID, r.offset))
		if err != nil {
			b.Close()
			return nil, nil, false, fmt.Errorf("读取重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, r.offset, err)
		}
		if !ok {
			// 消息已被 retention 清理但 inflight 残留：清掉记录并跳过（M2 起 retention 会同步清理）。
			// 这条 Delete 必须真正提交——见本函数收尾处关于 staged 的说明，否则孤儿
			// 记录永不消失，长轮询每 100ms 轮询一次就会反复扫描、反复打这条 Warn，
			// 形成永久日志洪水。
			d.logger.Warn("inflight 指向的消息不存在，清理孤儿记录", "group", group, "topic", topic, "queue", queueID, "offset", r.offset)
			b.Delete(store.InflightKey(group, topic, queueID, r.offset))
			staged = true
			if r.ordered {
				// 被清理的正是持有顺序锁的记录：锁随记录消失（不变式保证至多一条）
				orderedBusy = false
			}
			continue
		}
		m, err := core.DecodeMessage(raw)
		if err != nil {
			b.Close()
			return nil, nil, false, fmt.Errorf("解码重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, r.offset, err)
		}
		// 投递次数耗尽：转入死信 topic，不再投递。
		// DLQ 写入与源 inflight 删除是两段式（先写后删，独立批次），与本函数的
		// 取件批次 b 相互独立（此消息不进 b，不置 staged）。崩溃窗口内至多重复
		// 转入（at-least-once），死信消费端按消息 ID 幂等即可。
		// 锁语义没有破坏：本方法已持队列锁，Append 拿的是 produce 自己的锁，
		// 两把锁全程单向（deliver → produce），无环即无死锁。
		if r.attempts >= maxAttempts {
			if err := d.moveToDLQ(ctx, group, topic, queueID, r.offset, m, r.attempts, "投递次数耗尽（未在不可见期内 ack）"); err != nil {
				b.Close()
				return nil, nil, false, err
			}
			if r.ordered {
				// 卡住队头的顺序消息已随 inflight 一并移除：顺序锁释放，
				// 本轮阶段 2 即可投出下一条顺序消息（卡住→超限→推进，spec 流程 4/6）
				orderedBusy = false
				d.logger.Info("顺序消息超限入死信，队列解锁", "group", group, "topic", topic,
					"queue", queueID, "offset", r.offset, "msg_id", m.ID)
			}
			continue
		}
		m.DeliveryAttempt = r.attempts + 1
		// 指数退避只作用于非顺序消息（spec §5 流程 6）：顺序消息要的是原地
		// 快速重投（卡队头），退避会把整条队列拖住数分钟——不可见时长直接用
		// 客户端值。协议层 InvisibleDuration 回填用同一判据（receive.go）。
		exp := expireAt
		if !r.ordered {
			if bo := retryBackoff(m.DeliveryAttempt); bo > invisible {
				exp = now + bo.Milliseconds()
			}
		}
		// 重投必须原样保留 Ordered 标记：丢了它，重投后的记录不再被视为
		// 顺序锁占用，下一条顺序消息会与卡在队头的这条并发投递，顺序即破。
		b.Set(store.InflightKey(group, topic, queueID, r.offset),
			core.EncodeInflight(&core.InflightState{ExpireAtMs: exp, Attempts: m.DeliveryAttempt, Ordered: r.ordered}))
		out = append(out, m)
		staged = true
	}

	// 阶段 2：从 fetch 位点取新消息。
	//
	// 仅当 len(out) < maxMsgs 时才进入本阶段，不能只靠 Scan 的 limit 参数收敛：
	// store.Scan 的 limit<=0 语义是"不限"；若阶段 1 已把 out 填满 maxMsgs，
	// maxMsgs-len(out) 算出来正好是 0，传给 Scan 会被当成"不限"，从 cursor 开始
	// 把整条剩余队列都扫出来——远超调用方约定的批量上限，在长轮询兜底轮询
	// （每 100ms 一次）下尤其致命。因此这里整体跳过阶段 2，
	// 而不是传负数或改动 store.Scan 的既有约定。
	cursor := uint64(0)
	if v, ok, err := d.st.Get(store.CursorKey(group, topic, queueID)); err != nil {
		b.Close()
		return nil, nil, false, fmt.Errorf("读取 fetch 位点 (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	} else if ok {
		cursor = store.GetU64(v)
	}
	newCursor := cursor
	if len(out) < maxMsgs {
		lower := store.MsgKey(topic, queueID, cursor)
		upper := store.PrefixUpperBound(store.MsgQueuePrefix(topic, queueID))
		skipped := 0
		err = d.st.Scan(lower, upper, scanBudget, func(k, v []byte) (bool, error) {
			m, err := core.DecodeMessage(v)
			if err != nil {
				return false, err
			}
			// Tag 过滤在顺序锁之前：不匹配的消息（含顺序消息）对本消费组永久
			// 跳过、推进位点（spec §5 流程 2 语义不变）。顺序消息被过滤跳过不
			// 破坏顺序——它对本组从未投递，"已投递消息间"的相对顺序完好。
			// 若把顺序锁放在前面，被锁拦住的不匹配消息会永远堵住位点。
			if !filter.Match(m.Tag) {
				newCursor = m.Offset + 1
				skipped++
				return true, nil
			}
			// 顺序锁（spec §5 流程 4）：队列存在未终结的顺序 inflight 时，
			// 后续顺序消息不投。停止扫描且【不推进 newCursor】——这条消息
			// 未被投递，位点停在它前面，崩溃重启后它仍是下一条候选（卡住
			// 而不跳过的实现根基）。副作用：其后的普通消息一并等待（队头
			// 阻塞，设计决策 3，README 建议顺序消息用专用 topic）。
			if m.MessageGroup != "" && orderedBusy {
				d.logger.Debug("顺序锁阻塞取件", "group", group, "topic", topic,
					"queue", queueID, "blocked_offset", m.Offset)
				return false, nil
			}
			newCursor = m.Offset + 1
			m.DeliveryAttempt = 1
			ordered := m.MessageGroup != ""
			if ordered {
				// 投出即占锁：本轮扫描内的下一条顺序消息就会被上面的判定拦下，
				// 由此维持「每队列至多 1 条 Ordered inflight」不变式
				orderedBusy = true
			}
			b.Set(store.InflightKey(group, topic, queueID, m.Offset),
				core.EncodeInflight(&core.InflightState{ExpireAtMs: expireAt, Attempts: 1, Ordered: ordered}))
			out = append(out, m)
			return len(out) < maxMsgs, nil
		})
		if err != nil {
			b.Close()
			return nil, nil, false, fmt.Errorf("扫描新消息 (topic=%s q=%d cursor=%d): %w", topic, queueID, cursor, err)
		}
		if newCursor > cursor {
			staged = true
		}
		if skipped > 0 {
			d.logger.Debug("Tag 过滤跳过消息", "group", group, "topic", topic, "queue", queueID, "skipped", skipped)
		}
	}

	// 早退判据必须是 staged（是否有暂存写入），不能是 len(out)==0（是否取到消息）：
	// 只要 staged 为 true 就必须 Apply 提交，哪怕 out 本身是空的——比如本轮只清理了
	// 孤儿 inflight、没有可投递的消息。若按 len(out)==0 就 Close，阶段 1 里暂存的
	// 孤儿清理 Delete 会被一并悄悄丢弃：孤儿记录永远留在盘上，下次 Receive 还会
	// 扫到、还会再打一次 Warn。
	//
	// 注意返回值语义：孤儿清理这种"有暂存写入但无可投递消息"的情况，out 仍然
	// 是空的——不能因为提交了批次就伪造出一条消息。Receive 的循环靠
	// len(msgs)>0 判断"是否已有结果可以返回调用方"，空结果会继续长轮询等待，
	// 这正是我们想要的：孤儿清理不算"投递成功"，不该打断长轮询。
	if !staged {
		b.Close()
		return nil, nil, false, nil
	}
	if newCursor > cursor {
		b.Set(store.CursorKey(group, topic, queueID), store.PutU64(newCursor))
	}
	// 批次走复制抽象提交：inflight/cursor 键族归「被消费队列」的组。
	// staged 判据不动——它是「避免空提案洪水」的唯一屏障：本函数在锁内
	// 可能组装出零写入批次（无可投递、无可清理），若不过 staged 就提交，
	// 单机只是多一次 fsync，集群会空耗 raft 提案轮次与日志空间。
	g := d.rt.GroupForQueue(topic, queueID)
	pending, err := d.rep.ApplyAsync(ctx, g, b)
	if err != nil {
		// ApplyAsync 失败的批次按 store 约定弃给 GC
		return nil, nil, false, fmt.Errorf("提交取件批次 (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	if len(out) == 0 {
		// 本轮做了"无投递但有写入"的工作：清理孤儿 inflight（上面那条 Warn
		// 已说明是哪几条），或全部新消息被 Tag 过滤跳过、只推进了本组位点
		// （上方 Debug 日志说明跳过了几条）。单独一句话，不与投递共用消息：
		// 这恰恰是运维最需要读懂的一轮——打成"投递消息 count=0"会让人以为
		// 队列空转，从而忽略掉刚刚发生的数据修复或过滤推进。
		d.logger.Debug("本轮无可投递消息，仅清理了孤儿 inflight 或推进了过滤位点",
			"group", group, "topic", topic, "queue", queueID, "cursor", newCursor)
	} else {
		d.logger.Debug("投递消息已定序", "group", group, "topic", topic, "queue", queueID,
			"count", len(out), "redelivered", len(reds), "cursor", newCursor)
	}
	return out, pending, true, nil
}

// Ack 确认消息。attempt 必须与该 offset 当前持久化的 InflightState.Attempts 一致——
// 消费者持有的 attempt 来自它收到的那次 Receive（core.Message.DeliveryAttempt）。
//
// 为什么要校验 attempt，而不是只按 (group,topic,queue,offset) 删记录：
// 若消费者 A 收到 X（attempt=1）后处理超时，X 会被过期重投给消费者 B（attempt=2，
// 全新的过期时间）。此时 A 迟到的 Ack(X) 若不带 attempt 校验，会直接删掉 B 那条
// 记录——X 从此既无 inflight 兜底、cursor 也已跳过它，一旦 B 处理失败或崩溃，
// X 就永久丢失，直接违反本包头注释声明的"已 ack 前消息不丢"。attempt 不匹配
// 说明这是一个陈旧句柄，语义上等价于"记录已不存在"：幂等返回 (false, nil)，不报错。
//
// inflight 不存在或 attempt 不匹配都返回 (false, nil)，幂等，不算错误。
// 实现即 AckBatch 单条形态，校验/锁/持久化语义完全一致。
func (d *Deliverer) Ack(ctx context.Context, group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error) {
	results, err := d.AckBatch(ctx, group, topic, queueID, []AckEntry{{Offset: offset, Attempt: attempt}})
	if err != nil {
		return false, err
	}
	return results[0].OK, nil
}

// AckEntry 批量确认的单条输入：offset + 消费者持有的 attempt（校验规则见 Ack 注释）。
type AckEntry struct {
	Offset  uint64
	Attempt int32
}

// AckResult 批量确认的单条结果。OK=false 即落空（已 ack / 已重投 / 陈旧
// attempt——三种情况统一归约，与 Ack 返回 (false,nil) 的幂等语义一致）。
type AckResult struct {
	Offset uint64
	OK     bool
}

// AckBatch 单一 (group,topic,queue) 的批量确认：一把队列锁、逐条校验、
// 有效条目的 Delete 合成单个 Batch、一次 fsync。
//
// 参数：entries 至少一条（rpc 层保证；空批直接返回 nil,nil 防御）。
// 返回：与 entries 同序的结果；error 仅存储故障（此时整组失败，任何条目
// 都未确认——单 Batch 原子性保证不存在部分生效）。
//
// 锁与持久化（与 produce/receiveOnce 的拆分提交同款论证）：
//   - 队列锁内完成全部 Get→校验→Delete 暂存与 ApplyAsync（定序 + memtable
//     发布），解锁后同队列的下一个拿锁者读到的 inflight 状态与提交顺序一致；
//   - fsync 等待（Wait）在锁外，同队列多个在途确认由 Pebble commit pipeline
//     合并为一次 fsync——ack 吞吐从 1/fsync延迟 解放为 合并深度/fsync延迟；
//   - Wait 成功前绝不向调用方报告确认成功（语义红线 1）。
func (d *Deliverer) AckBatch(ctx context.Context, group, topic string, queueID uint32, entries []AckEntry) ([]AckResult, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	results := make([]AckResult, len(entries))
	b := d.st.NewBatch()
	staged := 0
	for i, e := range entries {
		results[i] = AckResult{Offset: e.Offset}
		k := store.InflightKey(group, topic, queueID, e.Offset)
		v, ok, err := d.st.Get(k)
		if err != nil {
			qlock.Unlock()
			// 未提交而放弃的批次必须自行 Close 回收（store.NewBatch 契约路径 2）
			b.Close()
			return nil, fmt.Errorf("批量 ack 查询 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, e.Offset, err)
		}
		if !ok {
			// 已 ack 或已重投：幂等落空，不影响其它条目（语义红线 4）
			d.logger.Debug("批量 ack 目标不存在（重复 ack 或已重投）",
				"group", group, "topic", topic, "queue", queueID, "offset", e.Offset)
			continue
		}
		ist, err := core.DecodeInflight(v)
		if err != nil {
			qlock.Unlock()
			b.Close()
			return nil, fmt.Errorf("批量 ack 解码 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, e.Offset, err)
		}
		if ist.Attempts != e.Attempt {
			d.logger.Debug("批量 ack attempt 不匹配（陈旧句柄，已被重投覆盖）",
				"group", group, "topic", topic, "queue", queueID, "offset", e.Offset,
				"want_attempt", ist.Attempts, "got_attempt", e.Attempt)
			continue
		}
		b.Delete(k)
		results[i].OK = true
		staged++
	}
	if staged == 0 {
		qlock.Unlock()
		b.Close() // 全部落空：零写入，批次走自行回收路径
		return results, nil
	}
	// 与 receiveOnceLocked 同组：确认归被消费队列的组，raft 定序保证
	// 同一队列内 ack 与重投的先后一致。
	g := d.rt.GroupForQueue(topic, queueID)
	pending, err := d.rep.ApplyAsync(ctx, g, b)
	if err != nil {
		qlock.Unlock()
		// ApplyAsync 失败的批次按 store 约定弃给 GC，不再 Close
		return nil, fmt.Errorf("批量 ack 提交 (group=%s topic=%s q=%d n=%d): %w", group, topic, queueID, staged, err)
	}
	qlock.Unlock()
	// 锁外等待持久化：fsync 完成前绝不报告确认成功（语义红线 1）
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待批量 ack 持久化 (group=%s topic=%s q=%d n=%d): %w", group, topic, queueID, staged, err)
	}
	d.logger.Debug("消息已确认", "group", group, "topic", topic, "queue", queueID,
		"requested", len(entries), "acked", staged)
	return results, nil
}

// ChangeInvisible 重设不可见截止时间（消费端主动延长/缩短处理时间）。
// attempt 校验规则与 Ack 相同：必须与当前持久化的 Attempts 一致，否则视为
// 陈旧句柄，返回 (false, nil)——理由同 Ack 的注释：本方法也是一次 Get→改→Set
// 的读-改-写，若操作的是已被重投覆盖的旧记录，写回去的 ExpireAtMs 会作用在
// "别人正在处理的新一轮投递"上，等于让消费者延长了一个不属于自己的窗口。
//
// inflight 不存在或 attempt 不匹配都返回 (false, nil)，幂等，不算错误。
// 拆分提交：锁内定序（changeInvisibleLocked），锁外等 fsync——成功返回前
// 新的过期时间必然已持久化（语义红线 1）。
func (d *Deliverer) ChangeInvisible(ctx context.Context, group, topic string, queueID uint32, offset uint64, attempt int32, invisible time.Duration) (bool, error) {
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	pending, ok, err := d.changeInvisibleLocked(ctx, group, topic, queueID, offset, attempt, invisible)
	qlock.Unlock()
	if err != nil || !ok {
		return false, err
	}
	if err := pending.Wait(); err != nil {
		return false, fmt.Errorf("等待改不可见时间持久化 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Debug("已更新不可见时间", "group", group, "topic", topic, "queue", queueID,
		"offset", offset, "attempt", attempt, "invisible_ms", invisible.Milliseconds())
	return true, nil
}

// changeInvisibleLocked 锁内部分：Get→校验→Set→ApplyAsync。调用方必须持队列锁。
// 与 receiveOnce 的过期重投互斥的理由见 wrapper 注释（读-改-写不能与重投交错）。
func (d *Deliverer) changeInvisibleLocked(ctx context.Context, group, topic string, queueID uint32, offset uint64, attempt int32, invisible time.Duration) (replication.Pending, bool, error) {
	k := store.InflightKey(group, topic, queueID, offset)
	v, ok, err := d.st.Get(k)
	if err != nil {
		return nil, false, fmt.Errorf("改不可见时间查询 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if !ok {
		d.logger.Debug("改不可见时间目标不存在（已 ack 或已重投）", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return nil, false, nil
	}
	ist, err := core.DecodeInflight(v)
	if err != nil {
		return nil, false, fmt.Errorf("解码 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if ist.Attempts != attempt {
		d.logger.Debug("改不可见时间 attempt 不匹配（陈旧句柄，已被重投覆盖）",
			"group", group, "topic", topic, "queue", queueID, "offset", offset,
			"want_attempt", ist.Attempts, "got_attempt", attempt)
		return nil, false, nil
	}
	ist.ExpireAtMs = time.Now().Add(invisible).UnixMilli()
	b := d.st.NewBatch()
	b.Set(k, core.EncodeInflight(ist))
	pending, err := d.rep.ApplyAsync(ctx, d.rt.GroupForQueue(topic, queueID), b)
	if err != nil {
		return nil, false, fmt.Errorf("改不可见时间 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	return pending, true, nil
}

// ForwardToDLQ 将一条 inflight 消息显式转入死信 %DLQ%{group}（协议
// ForwardMessageToDeadLetterQueue，spec §6：顺序消息重试超限时客户端显式
// 入 DLQ；对非顺序消息同样可用）。
//
// 参数与校验规则与 Ack 完全一致：attempt 必须与当前持久化的 Attempts 匹配，
// 陈旧句柄（已被重投覆盖）幂等返回 (false, nil)，绝不误伤新一轮投递的记录。
//
// 返回：(true, nil) 已转入（或目标消息已被 retention 清理、孤儿 inflight 已
// 清除——调用方要的"这条消息别再投了"两种情况下都已成立）；(false, nil)
// 目标不存在或句柄陈旧；错误仅在存储故障时返回。
func (d *Deliverer) ForwardToDLQ(ctx context.Context, group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error) {
	// 与 Ack/ChangeInvisible 同理：直接读写 inflight 必须持队列锁（类型注释
	// 声明的并发安全前提），否则与过期重投的读-改-写交错
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	defer qlock.Unlock()

	k := store.InflightKey(group, topic, queueID, offset)
	v, ok, err := d.st.Get(k)
	if err != nil {
		return false, fmt.Errorf("forward 查询 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if !ok {
		d.logger.Debug("forward 目标不存在（已 ack 或已重投）", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return false, nil
	}
	ist, err := core.DecodeInflight(v)
	if err != nil {
		return false, fmt.Errorf("解码 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if ist.Attempts != attempt {
		d.logger.Debug("forward attempt 不匹配（陈旧句柄，已被重投覆盖）",
			"group", group, "topic", topic, "queue", queueID, "offset", offset,
			"want_attempt", ist.Attempts, "got_attempt", attempt)
		return false, nil
	}
	raw, ok, err := d.st.Get(store.MsgKey(topic, queueID, offset))
	if err != nil {
		return false, fmt.Errorf("读取 forward 消息 (topic=%s q=%d off=%d): %w", topic, queueID, offset, err)
	}
	if !ok {
		// 消息已被 retention 清理但 inflight 残留：与 receiveOnce 的孤儿清理
		// 同理删除止损。对调用方视为成功——"这条消息别再投了"已经成立。
		b := d.st.NewBatch()
		b.Delete(k)
		if err := d.rep.Apply(ctx, d.rt.GroupForQueue(topic, queueID), b); err != nil {
			return false, fmt.Errorf("清理孤儿 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
		}
		d.logger.Warn("forward 目标消息已不存在，清理孤儿 inflight", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return true, nil
	}
	m, err := core.DecodeMessage(raw)
	if err != nil {
		return false, fmt.Errorf("解码 forward 消息 (topic=%s q=%d off=%d): %w", topic, queueID, offset, err)
	}
	// moveToDLQ 内部：两段式转入（先写死信、后删源 inflight），并打 Info 日志。
	// attempts/reason 写进死信属性，供控制台回答「试了几次、为什么进来」。
	if err := d.moveToDLQ(ctx, group, topic, queueID, offset, m, attempt, "客户端显式转入死信"); err != nil {
		return false, err
	}
	return true, nil
}

// moveToDLQ 将投递次数耗尽的消息复制入 %DLQ%{group} 并删除源 inflight。
//
// 死信保留原消息 ID/Body/Tag/Keys（ID 不变便于全链路追踪），来源坐标写入
// Properties（sq-origin-topic/queue/offset，控制台溯源与重发用），并附上
// 投递次数与转入原因（sq-dlq-attempts/sq-dlq-reason）——控制台的死信列表
// 要回答的第一个问题就是「试了几次、为什么进来的」，事后从日志里翻这两项
// 成本远高于当场写进属性；MessageGroup 置空——死信不再参与顺序语义。
func (d *Deliverer) moveToDLQ(ctx context.Context, group, topic string, queueID uint32, offset uint64,
	m *core.Message, attempts int32, reason string) error {
	dlqTopic := meta.DLQTopicName(group)
	// 死信 topic 固定 1 队列：量小、顺序无关、控制台浏览简单。CreateTopic 幂等。
	if _, err := d.mt.CreateTopic(ctx, dlqTopic, 1); err != nil {
		return fmt.Errorf("创建 DLQ topic %s: %w", dlqTopic, err)
	}
	props := make(map[string]string, len(m.Properties)+5) // +3 来源坐标 +2 投递次数与原因
	for k, v := range m.Properties {
		props[k] = v
	}
	props["sq-origin-topic"] = topic
	props["sq-origin-queue"] = strconv.FormatUint(uint64(queueID), 10)
	props["sq-origin-offset"] = strconv.FormatUint(offset, 10)
	props["sq-dlq-attempts"] = strconv.FormatInt(int64(attempts), 10)
	props["sq-dlq-reason"] = reason
	dlq := &core.Message{
		ID: m.ID, Topic: dlqTopic, Tag: m.Tag, Keys: m.Keys,
		Properties: props, Body: m.Body, BornAtMs: m.BornAtMs,
	}
	infKey := store.InflightKey(group, topic, queueID, offset)
	// 两段式转入：第一段写死信（d.pr.Append），第二段独立批次删源 inflight。
	// 先写后删；崩溃窗口（死信落盘后、inflight 删除前）重放 = 重复死信条目，
	// at-least-once 语义内——重投扫描会再次把超限消息转入，死信区出现两条
	// 同 ID 条目，死信消费端按消息 ID 幂等即可。次序不得反转（反转 = 丢失）。
	//
	// 集群档分派：死信 topic（%DLQ%{group}）是独立 topic，组号
	// GroupForQueue(dlqTopic, ...) 与被消费队列无关——本节点是死信队列组
	// leader 时本地 pr.Append（offset 分配在本节点）；否则经 fwd.ForwardAppend
	// 把消息字节交给死信组 leader 追加（leader-only 构造的跨节点延伸，与
	// txn.End 第一段同款；死信 topic 固定 1 队列，leader 侧 pr.Append 的
	// 确定性选队结果恒为队列 0，转发寻址按 GroupForQueue(dlqTopic, 0) 即可）。
	// 没有转发路径时（修复前），不 lead 死信组的节点每次 attempts 耗尽都撞
	// ErrNotLeader，该队列消费永久停摆——attempts 耗尽恰是 DLQ 设计场景，
	// 停摆即丢消息。
	dlqG := d.rt.GroupForQueue(dlqTopic, 0)
	var stored *core.Message
	var aerr error
	if d.rt.IsLeader(dlqG) {
		stored, aerr = d.pr.Append(ctx, dlq)
		if aerr == nil {
			// 本地路径日志与转发路径对称（字段对齐）：两条路径的死信
			// 落点都要可追溯，只给转发路径打日志会让本地转入在排查时
			// 显得「没发生」。
			d.logger.Info("死信消息本地入队", "g", dlqG, "msg_id", dlq.ID, "dlq_topic", dlqTopic,
				"origin_topic", topic, "origin_queue", queueID, "origin_offset", offset,
				"queue", stored.QueueID, "offset", stored.Offset)
		}
	} else {
		raw, eerr := core.EncodeMessage(dlq)
		if eerr != nil {
			return fmt.Errorf("编码死信消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, eerr)
		}
		qid, off, ferr := d.fwd.ForwardAppend(ctx, dlqG, raw)
		if ferr != nil {
			return fmt.Errorf("转发转入死信 (group=%s topic=%s q=%d off=%d dlq=%s g=%d): %w",
				group, topic, queueID, offset, dlqTopic, dlqG, ferr)
		}
		// 转发只回坐标：拼出日志所需的存储信息（ID/topic 本就来自死信本体）
		stored = &core.Message{ID: dlq.ID, Topic: dlqTopic, QueueID: qid, Offset: off}
		d.logger.Info("死信消息跨节点转发", "g", dlqG, "msg_id", dlq.ID, "dlq_topic", dlqTopic,
			"origin_topic", topic, "origin_queue", queueID, "origin_offset", offset, "queue", qid, "offset", off)
	}
	if aerr != nil {
		return fmt.Errorf("消息转入 DLQ (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, aerr)
	}
	if d.afterAppendHook != nil {
		d.afterAppendHook()
	}
	// 第二段：独立批次删源 inflight。失败只记 Error 不回滚第一段——死信已
	// 写入是既成事实，inflight 残留 = 下次重投扫描再次转入 = 重复死信条目
	//（可接受）；日志带全坐标，便于按 msg_id 在死信与源队列两侧核对。
	b := d.st.NewBatch()
	b.Delete(infKey)
	// 第二段删源 inflight 归被消费队列的组（与第一段的死信写入不同组——
	// DLQ 是另一 topic，组号随 DLQ 队列；两段本就无跨批原子性，分组不影响
	// 既有 at-least-once 重放语义）。
	if err := d.rep.Apply(ctx, d.rt.GroupForQueue(topic, queueID), b); err != nil {
		d.logger.Error("死信消息已写入但源 inflight 删除失败——重放窗口将产生重复死信条目（at-least-once 允许）",
			"group", group, "msg_id", dlq.ID, "dlq_topic", dlqTopic,
			"origin_topic", topic, "origin_queue", queueID, "origin_offset", offset, "err", err)
		return fmt.Errorf("删除源 inflight (group=%s topic=%s q=%d off=%d msg_id=%s): %w",
			group, topic, queueID, offset, dlq.ID, err)
	}
	d.logger.Info("消息投递超限转入死信", "group", group, "topic", topic, "queue", queueID,
		"offset", offset, "msg_id", m.ID, "dlq_topic", dlqTopic)
	return nil
}

// ResetCursor 重置某队列的消费位点并清空该队列全部 inflight（Admin API 位点
// 重置）。offset 允许任意值：向后 = 重复消费（at-least-once 语义内），向前 =
// 跳过消息。
//
// 必须持队列锁执行：绕开锁直接写 store 会与 receiveOnce/Ack 的读改写竞态，
// 出现"重置后 cursor 又被并发投递推回去"的幽灵回退。清空 inflight 同样必须：
// 残留的 inflight 记录会被阶段 1 当作过期重投候选、又会在 Ack 位点推进时
// 参与空洞计算，与重置后的新 cursor 语义互相打架。
func (d *Deliverer) ResetCursor(ctx context.Context, group, topic string, queueID uint32, offset uint64) error {
	qmu := d.lockQueue(group, topic, queueID)
	qmu.Lock()
	defer qmu.Unlock()
	b := d.st.NewBatch()
	b.Set(store.CursorKey(group, topic, queueID), store.PutU64(offset))
	ip := store.InflightPrefix(group, topic, queueID)
	b.DeleteRange(ip, store.PrefixUpperBound(ip))
	if err := d.rep.Apply(ctx, d.rt.GroupForQueue(topic, queueID), b); err != nil {
		return fmt.Errorf("重置位点 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Info("消费位点已重置", "group", group, "topic", topic, "queue", queueID, "offset", offset)
	return nil
}
