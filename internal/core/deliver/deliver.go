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
//   - 不管投递次数上限/DLQ（M2）；不管 Tag 过滤（M2，M1 只接受 "*"）
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
	"sync"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// Deliverer POP 消费引擎。并发安全：同一队列的取件/确认/改不可见时间全部在
// 该队列的 qmu 临界区内执行（Receive 经 receiveOnce、Ack、ChangeInvisible
// 三者都在方法开头取同一把队列锁），不同队列并行。
// 三者共同读写同一片 inflight 键空间，若只有 Receive 持锁而 Ack/
// ChangeInvisible 不持锁，会出现"重投覆盖后的记录被陈旧 Ack 误删"
// "ChangeInvisible 的读-改-写覆盖掉并发重投写入的新 attempts"等竞态——
// 这正是本类型曾经的注释里"并发安全"这句话不成立的地方，现在三者统一持锁后才真正成立。
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
func (d *Deliverer) Receive(ctx context.Context, group, topic string, queueID uint32, maxMsgs int, invisible, wait time.Duration) ([]*core.Message, error) {
	if _, err := d.mt.EnsureGroup(group); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		// 先订阅再取件：若取件为空后才订阅，"取件为空 → 新消息写入 → 才订阅"
		// 这个窗口期内的写入会错过 close 广播，导致长轮询白等到超时——
		// 订阅在前，即便取件与写入之间发生竞态，wakeCh 也一定能收到这次唤醒。
		wakeCh := d.pr.Subscribe(topic)
		msgs, err := d.receiveOnce(group, topic, queueID, maxMsgs, invisible)
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

// receiveOnce 单次取件：过期 inflight 重投 + 新消息，合计不超过 maxMsgs。
func (d *Deliverer) receiveOnce(group, topic string, queueID uint32, maxMsgs int, invisible time.Duration) ([]*core.Message, error) {
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
	}
	var reds []redeliver
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
		if ist.ExpireAtMs <= now {
			reds = append(reds, redeliver{offset: off, attempts: ist.Attempts})
		}
		return len(reds)+1 <= maxMsgs, nil
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
			// 这条 Delete 必须真正提交——见收尾处的裁定 2 说明，否则孤儿记录永不消失，
			// 长轮询每 100ms 轮询一次就会反复扫描、反复打这条 Warn，形成永久日志洪水。
			d.logger.Warn("inflight 指向的消息不存在，清理孤儿记录", "group", group, "topic", topic, "queue", queueID, "offset", r.offset)
			b.Delete(store.InflightKey(group, topic, queueID, r.offset), nil)
			staged = true
			continue
		}
		m, err := core.DecodeMessage(raw)
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("解码重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, r.offset, err)
		}
		m.DeliveryAttempt = r.attempts + 1
		b.Set(store.InflightKey(group, topic, queueID, r.offset),
			core.EncodeInflight(&core.InflightState{ExpireAtMs: expireAt, Attempts: m.DeliveryAttempt}), nil)
		out = append(out, m)
		staged = true
	}

	// 阶段 2：从 fetch 位点取新消息。
	//
	// 裁定 1（修正 brief 缺陷）：仅当 len(out) < maxMsgs 时才进入本阶段。
	// store.Scan 的 limit<=0 语义是"不限"；若阶段 1 已把 out 填满 maxMsgs，
	// maxMsgs-len(out) 算出来是 0，若仍然传给 Scan 会被当成"不限"，
	// 从 cursor 开始把整条剩余队列都扫出来——远超调用方约定的批量上限，
	// 在长轮询兜底轮询（每 100ms 一次）下尤其致命。因此这里整体跳过阶段 2，
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
		err = d.st.Scan(lower, upper, maxMsgs-len(out), func(k, v []byte) (bool, error) {
			m, err := core.DecodeMessage(v)
			if err != nil {
				return false, err
			}
			m.DeliveryAttempt = 1
			b.Set(store.InflightKey(group, topic, queueID, m.Offset),
				core.EncodeInflight(&core.InflightState{ExpireAtMs: expireAt, Attempts: 1}), nil)
			out = append(out, m)
			newCursor = m.Offset + 1
			return true, nil
		})
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("扫描新消息 (topic=%s q=%d cursor=%d): %w", topic, queueID, cursor, err)
		}
		if newCursor > cursor {
			staged = true
		}
	}

	// 裁定 2（修正 brief 缺陷）：早退路径只在"确无任何暂存写入"时才 Close 放弃批次；
	// 只要 staged 为 true（哪怕 out 本身是空的——比如本轮只清理了孤儿 inflight，
	// 没有可投递的消息），也必须 Apply 提交。原 brief 的条件是 len(out)==0 就
	// Close，会把阶段 1 里暂存的孤儿清理 Delete 一并悄悄丢弃：孤儿记录永远留在
	// 盘上，下次 Receive 还会扫到、还会再打一次 Warn。
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
