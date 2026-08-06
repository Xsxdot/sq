// Package produce 实现消息写入路径：队列选择、offset 分配、落盘、长轮询唤醒。
//
// 职责：
//   - Append：一次语义写 = 消息体 + alloc 计数器，同一 Batch 原子提交
//   - offset 分配采用持久化计数器（alloc/ key），重启 O(1) 恢复且绝不回退
//   - 长轮询唤醒：按 topic 的 close-broadcast 信号
//
// 边界：
//   - 不判定延时/事务（M3/M6 在 Append 之前分流，本包不感知）
//   - 不做消费可见性判断（deliver 的事）
package produce

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// MaxBodySize 消息体上限（spec §7）。
const MaxBodySize = 4 * 1024 * 1024

// Producer 写入引擎。并发安全。
type Producer struct {
	st     *store.Store
	mt     *meta.Meta
	logger *slog.Logger

	mu     sync.Mutex
	next   map[string]uint64        // "topic/4Bqid" -> 下一 offset（内存缓存，与 alloc/ key 同步）
	rr     map[string]uint32        // topic -> 轮询游标
	wakers map[string]chan struct{} // topic -> 长轮询唤醒信号
}

// New 构造 Producer。next 缓存懒加载（首写某队列时读一次 alloc/ key）。
func New(st *store.Store, mt *meta.Meta, logger *slog.Logger) *Producer {
	return &Producer{
		st: st, mt: mt, logger: logger.With("mod", "produce"),
		next: map[string]uint64{}, rr: map[string]uint32{}, wakers: map[string]chan struct{}{},
	}
}

func qkey(topic string, q uint32) string { return fmt.Sprintf("%s/%d", topic, q) }

// AppendWith 在 Append 基础上，把 extra 组装的额外写操作并入同一原子批次。
//
// 用途：DLQ 转入（写死信消息 + 删源 inflight）、M3 延时转正（写消息 + 删 delay
// 条目）等「消息写入必须与另一处状态变更同生共死」的场景。extra 可为 nil。
//
// 注意：extra 只应操作与本消息无键冲突的 key；extra 内不得再调用本 Producer
// 的任何方法（p.mu 不可重入）。
func (p *Producer) AppendWith(m *core.Message, extra func(b *pebble.Batch)) (*core.Message, error) {
	if len(m.Body) == 0 || len(m.Body) > MaxBodySize {
		return nil, fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), MaxBodySize)
	}
	tc, err := p.mt.EnsureTopic(m.Topic)
	if err != nil {
		return nil, err
	}
	if m.ID == "" {
		m.ID = core.NewMessageID()
	}
	if m.BornAtMs == 0 {
		m.BornAtMs = time.Now().UnixMilli()
	}
	m.StoreAtMs = time.Now().UnixMilli()

	// 关于这把锁的范围——已知的吞吐瓶颈，先记在这里，不在 M1 动它：
	//
	// p.mu 是整个 Producer 唯一的一把锁，覆盖队列选择、offset 分配、store.Apply
	// （fsync 就发生在里面）以及唤醒长轮询。也就是说所有 topic 的所有生产者都在
	// 这一个临界区里排队，任意两次 Apply 永远不可能重叠。
	//
	// 由此产生的直接后果：Pebble 的 group commit 在 sq 上是失效的。它的原理是把
	// 同一时刻并发到达的多个 commit 合并成一次 fsync 摊薄开销，而设计文档正是
	// 以此为依据才敢把"默认同步刷盘"当成可以接受的默认值。既然并发不存在，
	// 也就无从合并——持续写入时的实际形态是每条消息一次 fsync，且全局单线程。
	//
	// 为什么现在不改：正确的做法是把锁按队列拆开，或者在进入 Apply 之前就放锁
	// （offset 一旦分配完，写入本身不需要互斥）。但两者都会改变"offset 分配与
	// 落盘之间是否可能交错"的前提，而 M1 没有任何吞吐量基准，改完既无法证明变快，
	// 也无法证明没有引入乱序。等到真正测吞吐时再连同基准一起做。
	p.mu.Lock()
	defer p.mu.Unlock()
	// 队列选择：MessageGroup 定死队列（顺序语义的根基，M4 复用）；否则轮询
	if m.MessageGroup != "" {
		h := fnv.New32a()
		h.Write([]byte(m.MessageGroup))
		m.QueueID = h.Sum32() % tc.Queues
	} else {
		m.QueueID = p.rr[m.Topic] % tc.Queues
		p.rr[m.Topic]++
	}
	off, err := p.nextOffsetLocked(m.Topic, m.QueueID)
	if err != nil {
		return nil, err
	}
	m.Offset = off
	raw, err := core.EncodeMessage(m)
	if err != nil {
		return nil, err
	}
	// 消息体与 offset 计数器同一 Batch 原子提交：Apply 要么两者都落盘要么都不落盘，
	// 这样崩溃重启后 nextOffsetLocked 读到的计数器与实际已写消息严格一致，
	// 绝不会出现"计数器已推进但消息未落盘"从而导致下次分配的 offset 覆盖旧消息，
	// 也不会出现"消息已落盘但计数器未推进"从而导致 offset 被重复分配。
	b := p.st.NewBatch()
	b.Set(store.MsgKey(m.Topic, m.QueueID, m.Offset), raw, nil)
	b.Set(store.AllocKey(m.Topic, m.QueueID), store.PutU64(off+1), nil)
	// Keys 业务索引与消息同批写入（spec §5 流程 1）：崩溃时索引与消息要么都在要么都不在
	for _, key := range m.Keys {
		if key == "" {
			continue // 空 key 无检索意义（SDK 不会生成，防御脏输入）
		}
		b.Set(store.KeyIdxKey(m.Topic, key, m.StoreAtMs, m.QueueID, m.Offset), nil, nil)
	}
	if extra != nil {
		extra(b)
	}
	if err := p.st.Apply(b); err != nil {
		return nil, fmt.Errorf("写入消息 %s (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}
	// 仅在 Apply 成功后才更新内存缓存：失败的写不能让内存计数器"抢跑"，
	// 否则下次 Append 会因为缓存命中而跳过盘上校验，白白烧掉一个从未真正写出的 offset。
	p.next[qkey(m.Topic, m.QueueID)] = off + 1
	p.wakeLocked(m.Topic)
	p.logger.Debug("消息已写入", "topic", m.Topic, "queue", m.QueueID, "offset", m.Offset, "msg_id", m.ID, "keys", len(m.Keys))
	return m, nil
}

// Append 写入一条普通消息（M1 签名保持不变）。
func (p *Producer) Append(m *core.Message) (*core.Message, error) { return p.AppendWith(m, nil) }

// nextOffsetLocked 取该队列下一 offset。缓存未命中时读盘上 alloc/ 计数器——
// 崩溃后靠它 O(1) 恢复，且因与消息同 Batch 提交，绝不会分配已用过的 offset。
func (p *Producer) nextOffsetLocked(topic string, q uint32) (uint64, error) {
	k := qkey(topic, q)
	if off, ok := p.next[k]; ok {
		return off, nil
	}
	v, ok, err := p.st.Get(store.AllocKey(topic, q))
	if err != nil {
		return 0, fmt.Errorf("读取 offset 计数器 %s: %w", k, err)
	}
	if !ok {
		return 0, nil
	}
	return store.GetU64(v), nil
}

// Subscribe 返回 topic 的唤醒信号：下一次 Append 该 topic 时该 chan 被 close。
// 消费方等待后必须重新 Subscribe（close 是一次性广播）。
func (p *Producer) Subscribe(topic string) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch, ok := p.wakers[topic]
	if !ok {
		ch = make(chan struct{})
		p.wakers[topic] = ch
	}
	return ch
}

// wakeLocked close 当前信号并撤下（下一 Subscribe 换新 chan）。调用方必须已持有 p.mu
// （由 Append 在同一临界区内调用），本方法不再重复加锁。
func (p *Producer) wakeLocked(topic string) {
	if ch, ok := p.wakers[topic]; ok {
		close(ch)
		delete(p.wakers, topic)
	}
}
