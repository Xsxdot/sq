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
package deliver

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
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
// 该队列的 qmu 临界区内执行（Receive 经 receiveOnce、Ack、ChangeInvisible
// 三者都在方法开头取同一把队列锁），不同队列并行。
// 三者共同读写同一片 inflight 键空间，若只有 Receive 持锁而 Ack/
// ChangeInvisible 不持锁，会出现"重投覆盖后的记录被陈旧 Ack 误删"
// "ChangeInvisible 的读-改-写覆盖掉并发重投写入的新 attempts"等竞态——
// 因此上面这句"并发安全"是有条件的：它成立的前提就是这三个方法统一持队列锁，
// 新增任何直接读写 inflight 的方法时必须一并纳入，否则这句话立刻不再成立。
type Deliverer struct {
	st     *store.Store
	mt     *meta.Meta
	pr     *produce.Producer
	logger *slog.Logger

	mu  sync.Mutex
	qmu map[string]*sync.Mutex // "group/topic/qid" -> 队列级锁
}

// New 构造 Deliverer。
func New(st *store.Store, mt *meta.Meta, pr *produce.Producer, logger *slog.Logger) *Deliverer {
	return &Deliverer{st: st, mt: mt, pr: pr, logger: logger.With("mod", "deliver"), qmu: map[string]*sync.Mutex{}}
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
	gc, err := d.mt.EnsureGroup(group)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		// 先订阅再取件：若取件为空后才订阅，"取件为空 → 新消息写入 → 才订阅"
		// 这个窗口期内的写入会错过 close 广播，导致长轮询白等到超时——
		// 订阅在前，即便取件与写入之间发生竞态，wakeCh 也一定能收到这次唤醒。
		wakeCh := d.pr.Subscribe(topic)
		msgs, err := d.receiveOnce(group, topic, queueID, maxMsgs, invisible, gc.EffectiveMaxAttempts(), filter)
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

// receiveOnce 单次取件：过期 inflight 重投 + 新消息（可带 Tag 过滤），
// 合计不超过 maxMsgs。maxAttempts 为该组的生效投递上限，重投候选
// attempts 达到上限时转入死信 topic，不再投递。
func (d *Deliverer) receiveOnce(group, topic string, queueID uint32, maxMsgs int, invisible time.Duration, maxAttempts int32, filter *TagFilter) ([]*core.Message, error) {
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	defer qlock.Unlock()

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
		return nil, fmt.Errorf("扫描 inflight (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	for _, r := range reds {
		raw, ok, err := d.st.Get(store.MsgKey(topic, queueID, r.offset))
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("读取重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, r.offset, err)
		}
		if !ok {
			// 消息已被 retention 清理但 inflight 残留：清掉记录并跳过（M2 起 retention 会同步清理）。
			// 这条 Delete 必须真正提交——见本函数收尾处关于 staged 的说明，否则孤儿
			// 记录永不消失，长轮询每 100ms 轮询一次就会反复扫描、反复打这条 Warn，
			// 形成永久日志洪水。
			d.logger.Warn("inflight 指向的消息不存在，清理孤儿记录", "group", group, "topic", topic, "queue", queueID, "offset", r.offset)
			b.Delete(store.InflightKey(group, topic, queueID, r.offset), nil)
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
			return nil, fmt.Errorf("解码重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, r.offset, err)
		}
		// 投递次数耗尽：转入死信 topic，不再投递。
		// DLQ 写入与 inflight 删除经 AppendWith 同批原子提交，与本函数的取件批次 b
		// 相互独立（此消息不进 b，不置 staged）。崩溃窗口内至多重复转入
		//（at-least-once），死信消费端按消息 ID 幂等即可。
		// 锁语义没有破坏：本方法已持队列锁，AppendWith 拿的是 produce 自己的锁，
		// 两把锁全程单向（deliver → produce），无环即无死锁。
		if r.attempts >= maxAttempts {
			if err := d.moveToDLQ(group, topic, queueID, r.offset, m); err != nil {
				b.Close()
				return nil, err
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
			core.EncodeInflight(&core.InflightState{ExpireAtMs: exp, Attempts: m.DeliveryAttempt, Ordered: r.ordered}), nil)
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
		return nil, fmt.Errorf("读取 fetch 位点 (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
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
				core.EncodeInflight(&core.InflightState{ExpireAtMs: expireAt, Attempts: 1, Ordered: ordered}), nil)
			out = append(out, m)
			return len(out) < maxMsgs, nil
		})
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("扫描新消息 (topic=%s q=%d cursor=%d): %w", topic, queueID, cursor, err)
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
		return nil, nil
	}
	if newCursor > cursor {
		b.Set(store.CursorKey(group, topic, queueID), store.PutU64(newCursor), nil)
	}
	if err := d.st.Apply(b); err != nil {
		return nil, fmt.Errorf("提交取件批次 (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	if len(out) == 0 {
		// 本轮做了"无投递但有写入"的工作：清理孤儿 inflight（上面那条 Warn
		// 已说明是哪几条），或全部新消息被 Tag 过滤跳过、只推进了本组位点
		// （上方 Debug 日志说明跳过了几条）。单独一句话，不与投递共用消息：
		// 这恰恰是运维最需要读懂的一轮——打成"投递消息 count=0"会让人以为
		// 队列空转，从而忽略掉刚刚发生的数据修复或过滤推进。
		d.logger.Debug("本轮无可投递消息，仅清理了孤儿 inflight 或推进了过滤位点",
			"group", group, "topic", topic, "queue", queueID, "cursor", newCursor)
		return out, nil
	}
	d.logger.Debug("投递消息", "group", group, "topic", topic, "queue", queueID,
		"count", len(out), "redelivered", len(reds), "cursor", newCursor)
	return out, nil
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
func (d *Deliverer) Ack(group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error) {
	// 与 receiveOnce 共享队列锁：Ack 要执行的 Get→校验→Delete 若不与重投互斥，
	// receiveOnce 的过期重投（Get 旧记录→写入新 attempts）可能与本方法的
	// Get→Delete 交错，产生"删掉了重投后新记录"或"看到重投前的旧 attempts"
	// 之类的竞态。持有同一把 qmu 是类型注释里"并发安全"这句话成立的前提。
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	defer qlock.Unlock()

	k := store.InflightKey(group, topic, queueID, offset)
	v, ok, err := d.st.Get(k)
	if err != nil {
		return false, fmt.Errorf("ack 查询 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if !ok {
		d.logger.Debug("ack 目标不存在（重复 ack 或已重投）", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return false, nil
	}
	ist, err := core.DecodeInflight(v)
	if err != nil {
		return false, fmt.Errorf("解码 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if ist.Attempts != attempt {
		d.logger.Debug("ack attempt 不匹配（陈旧句柄，已被重投覆盖）",
			"group", group, "topic", topic, "queue", queueID, "offset", offset,
			"want_attempt", ist.Attempts, "got_attempt", attempt)
		return false, nil
	}
	b := d.st.NewBatch()
	b.Delete(k, nil)
	if err := d.st.Apply(b); err != nil {
		return false, fmt.Errorf("ack (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Debug("消息已确认", "group", group, "topic", topic, "queue", queueID, "offset", offset, "attempt", attempt)
	return true, nil
}

// ChangeInvisible 重设不可见截止时间（消费端主动延长/缩短处理时间）。
// attempt 校验规则与 Ack 相同：必须与当前持久化的 Attempts 一致，否则视为
// 陈旧句柄，返回 (false, nil)——理由同 Ack 的注释：本方法也是一次 Get→改→Set
// 的读-改-写，若操作的是已被重投覆盖的旧记录，写回去的 ExpireAtMs 会作用在
// "别人正在处理的新一轮投递"上，等于让消费者延长了一个不属于自己的窗口。
//
// inflight 不存在或 attempt 不匹配都返回 (false, nil)，幂等，不算错误。
func (d *Deliverer) ChangeInvisible(group, topic string, queueID uint32, offset uint64, attempt int32, invisible time.Duration) (bool, error) {
	// 与 Ack 同理：必须持队列锁再做 Get→改→Set，否则这次读-改-写可能与
	// receiveOnce 的过期重投交错，覆盖掉重投写入的新 Attempts（丢更新），
	// 或在 Ack 的 Delete 与本方法的 Set 之间发生"删除后又被写回"的复活。
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	defer qlock.Unlock()

	k := store.InflightKey(group, topic, queueID, offset)
	v, ok, err := d.st.Get(k)
	if err != nil {
		return false, fmt.Errorf("改不可见时间查询 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if !ok {
		d.logger.Debug("改不可见时间目标不存在（已 ack 或已重投）", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return false, nil
	}
	ist, err := core.DecodeInflight(v)
	if err != nil {
		return false, fmt.Errorf("解码 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if ist.Attempts != attempt {
		d.logger.Debug("改不可见时间 attempt 不匹配（陈旧句柄，已被重投覆盖）",
			"group", group, "topic", topic, "queue", queueID, "offset", offset,
			"want_attempt", ist.Attempts, "got_attempt", attempt)
		return false, nil
	}
	ist.ExpireAtMs = time.Now().Add(invisible).UnixMilli()
	b := d.st.NewBatch()
	b.Set(k, core.EncodeInflight(ist), nil)
	if err := d.st.Apply(b); err != nil {
		return false, fmt.Errorf("改不可见时间 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Debug("已更新不可见时间", "group", group, "topic", topic, "queue", queueID, "offset", offset, "attempt", attempt, "invisible_ms", invisible.Milliseconds())
	return true, nil
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
func (d *Deliverer) ForwardToDLQ(group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error) {
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
		b.Delete(k, nil)
		if err := d.st.Apply(b); err != nil {
			return false, fmt.Errorf("清理孤儿 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
		}
		d.logger.Warn("forward 目标消息已不存在，清理孤儿 inflight", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return true, nil
	}
	m, err := core.DecodeMessage(raw)
	if err != nil {
		return false, fmt.Errorf("解码 forward 消息 (topic=%s q=%d off=%d): %w", topic, queueID, offset, err)
	}
	// moveToDLQ 内部：DLQ 写入与 inflight 删除同批原子提交，并打 Info 日志
	if err := d.moveToDLQ(group, topic, queueID, offset, m); err != nil {
		return false, err
	}
	return true, nil
}

// moveToDLQ 将投递次数耗尽的消息复制入 %DLQ%{group} 并原子删除源 inflight。
//
// 死信保留原消息 ID/Body/Tag/Keys（ID 不变便于全链路追踪），来源坐标写入
// Properties（sq-origin-topic/queue/offset，控制台溯源与重发用）；
// MessageGroup 置空——死信不再参与顺序语义。
func (d *Deliverer) moveToDLQ(group, topic string, queueID uint32, offset uint64, m *core.Message) error {
	dlqTopic := meta.DLQTopicName(group)
	// 死信 topic 固定 1 队列：量小、顺序无关、控制台浏览简单。CreateTopic 幂等。
	if _, err := d.mt.CreateTopic(dlqTopic, 1); err != nil {
		return fmt.Errorf("创建 DLQ topic %s: %w", dlqTopic, err)
	}
	props := make(map[string]string, len(m.Properties)+3)
	for k, v := range m.Properties {
		props[k] = v
	}
	props["sq-origin-topic"] = topic
	props["sq-origin-queue"] = strconv.FormatUint(uint64(queueID), 10)
	props["sq-origin-offset"] = strconv.FormatUint(offset, 10)
	dlq := &core.Message{
		ID: m.ID, Topic: dlqTopic, Tag: m.Tag, Keys: m.Keys,
		Properties: props, Body: m.Body, BornAtMs: m.BornAtMs,
	}
	infKey := store.InflightKey(group, topic, queueID, offset)
	if _, err := d.pr.AppendWith(dlq, func(b *pebble.Batch) { b.Delete(infKey, nil) }); err != nil {
		return fmt.Errorf("消息转入 DLQ (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
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
func (d *Deliverer) ResetCursor(group, topic string, queueID uint32, offset uint64) error {
	qmu := d.lockQueue(group, topic, queueID)
	qmu.Lock()
	defer qmu.Unlock()
	b := d.st.NewBatch()
	b.Set(store.CursorKey(group, topic, queueID), store.PutU64(offset), nil)
	ip := store.InflightPrefix(group, topic, queueID)
	b.DeleteRange(ip, store.PrefixUpperBound(ip), nil)
	if err := d.st.Apply(b); err != nil {
		return fmt.Errorf("重置位点 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Info("消费位点已重置", "group", group, "topic", topic, "queue", queueID, "offset", offset)
	return nil
}
