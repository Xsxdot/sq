// Package produce 实现消息写入路径：队列选择、offset 分配、落盘、长轮询唤醒。
//
// 职责：
//   - Append：一次语义写 = 消息体 + alloc 计数器，同一 Batch 原子提交
//   - offset 分配采用持久化计数器（alloc/ key），重启 O(1) 恢复且绝不回退
//   - 长轮询唤醒：按 topic 的 close-broadcast 信号
//   - AppendDelay：延时消息写 delay/ 暂存区，seq 计数器同批原子提交
//
// 边界：
//   - 延时判定入口在此（AppendDelay），到期搬运是 delay 包的事；事务（M6）仍在 Append 之前分流
//   - 不做消费可见性判断（deliver 的事）
//   - 锁结构：p.mu 护共享 map；每 (topic,queue) 一把锁护写入；delayMu 护延时暂存
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

// queueState 单个 (topic, queue) 的写入状态。qs.mu 只串行化 offset 分配与
// 批次定序（ApplyAsync 为止）；fsync 等待在锁外，同队列多条在途提交由 Pebble
// commit pipeline 合并为一次 fsync——队列内 group commit 的关键。
type queueState struct {
	mu     sync.Mutex
	next   uint64 // 下一 offset（懒加载自 alloc/ key）
	loaded bool
}

// Producer 写入引擎。并发安全。
type Producer struct {
	st     *store.Store
	mt     *meta.Meta
	logger *slog.Logger

	// mu 只护共享 map（qstates/rr/wakers）——临界区内不再有任何 I/O。
	// 旧实现的单一全局锁跨越 store.Apply（fsync 在内），使所有队列的写入
	// 全局串行、group commit 失效，见 git 历史中原锁注释的完整推导。
	mu      sync.Mutex
	qstates map[string]*queueState
	rr      map[string]uint32
	wakers  map[string]chan struct{}

	// delayMu 护延时暂存区的 seq 分配与落盘（单一全局计数器，天然串行；
	// 独立成锁是为了不与普通写入互相阻塞）。
	delayMu     sync.Mutex
	delayNext   uint64
	delayLoaded bool
}

// New 构造 Producer。offset 缓存懒加载（首写某队列时读一次 alloc/ key）。
func New(st *store.Store, mt *meta.Meta, logger *slog.Logger) *Producer {
	return &Producer{
		st: st, mt: mt, logger: logger.With("mod", "produce"),
		qstates: map[string]*queueState{}, rr: map[string]uint32{}, wakers: map[string]chan struct{}{},
	}
}

func qkey(topic string, q uint32) string { return fmt.Sprintf("%s/%d", topic, q) }

// AppendWith 在 Append 基础上，把 extra 组装的额外写操作并入同一原子批次。
//
// 用途：DLQ 转入（写死信消息 + 删源 inflight）、M3 延时转正（写消息 + 删 delay
// 条目）等「消息写入必须与另一处状态变更同生共死」的场景。extra 可为 nil。
//
// 注意：extra 只应操作与本消息无键冲突的 key；extra 内不得再调用本 Producer
// 的任何方法（p.mu 与 queueState.mu 均不可重入）。
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

	// 段 1（p.mu）：队列选择 + 取/建 queueState。纯内存，不含 I/O。
	p.mu.Lock()
	if m.MessageGroup != "" {
		h := fnv.New32a()
		h.Write([]byte(m.MessageGroup))
		m.QueueID = h.Sum32() % tc.Queues
	} else {
		m.QueueID = p.rr[m.Topic] % tc.Queues
		p.rr[m.Topic]++
	}
	k := qkey(m.Topic, m.QueueID)
	qs, ok := p.qstates[k]
	if !ok {
		qs = &queueState{}
		p.qstates[k] = qs
	}
	p.mu.Unlock()

	// 段 2（qs.mu）：offset 分配 + 编码 + 落盘。同队列串行（offset 顺序 ==
	// 落盘顺序，FIFO 根基），跨队列并行（group commit 合并 fsync）。
	qs.mu.Lock()
	off, err := qs.nextOffsetLocked(p.st, m.Topic, m.QueueID)
	if err != nil {
		qs.mu.Unlock()
		return nil, err
	}
	m.Offset = off
	raw, err := core.EncodeMessage(m)
	if err != nil {
		qs.mu.Unlock()
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
	pending, err := p.st.ApplyAsync(b)
	if err != nil {
		qs.mu.Unlock()
		return nil, fmt.Errorf("写入消息 %s (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}
	// ApplyAsync 成功 == 本条已在 WAL/memtable 中定序。此刻立即推进 offset 缓存
	// 并解锁：同队列后继消息随即进锁定序，与本条一起挂在 commit pipeline 里共享
	// 同一次 fsync——这就是 group commit 在队列内生效的机制（吞吐从 1/fsync延迟
	// 变为 合并深度/fsync延迟）。
	//
	// 为什么敢在 Wait 之前推进 qs.next：若后续 WaitSync 失败，说明 WAL sync 失败、
	// Pebble 已进入不可恢复错误态，之后所有写入都会失败，进程只能重启；重启后
	// 计数器与实际落盘由「同批原子提交」保证严格一致，内存里烧掉的 offset 无害。
	qs.next = off + 1
	qs.loaded = true
	qs.mu.Unlock()

	// 锁外等待持久化：fsync 完成之前绝不返回、绝不唤醒（语义红线 1）。
	// 可见性窗口说明（防止后人误改）：Pebble 的 Commit(Sync) 本就是「先发布可见、
	// 后等 fsync」，拉取型读者在旧实现里同样可能于 fsync 完成前读到本条——本改动
	// 没有扩大该窗口，只是把等待从锁内挪到锁外。
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待消息 %s 持久化 (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}

	// 段 3（p.mu）：唤醒长轮询。必须在持久化成功之后——被唤醒的订阅者读 store
	// 必能看到这条消息。
	p.mu.Lock()
	p.wakeLocked(m.Topic)
	p.mu.Unlock()
	p.logger.Debug("消息已写入", "topic", m.Topic, "queue", m.QueueID, "offset", m.Offset, "msg_id", m.ID, "keys", len(m.Keys))
	return m, nil
}

// AppendDelay 将延时消息写入 delay/ 暂存区（spec §5 流程 3 前半）。
//
// 参数：m.DeliverAtMs 必须 >0（协议层已保证 DELAY 消息带 delivery_timestamp，
// <=0 属编程错误直接报错）。到期时间已过（<=now）时直通 AppendWith 立即投递：
// 语义上"到期的延时消息"就是普通消息，绕道暂存区再被调度器搬回来只是
// 多一次读写放大，结果完全相同。
//
// 返回：写入后的消息。注意暂存态消息没有队列与 offset（m.QueueID/Offset
// 保持零值）——它们在到期移入 msg/ 时才由正常写入路径分配。
//
// 原子性：delay 条目与 seq 计数器同一 Batch 提交，理由与 Append 的
// offset 计数器完全相同（崩溃后计数器与已写条目严格一致，seq 绝不复用）。
func (p *Producer) AppendDelay(m *core.Message) (*core.Message, error) {
	if m.DeliverAtMs <= 0 {
		return nil, fmt.Errorf("AppendDelay 要求 DeliverAtMs>0，得到 %d", m.DeliverAtMs)
	}
	if len(m.Body) == 0 || len(m.Body) > MaxBodySize {
		return nil, fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), MaxBodySize)
	}
	// 写入时就确认 topic 存在（autoCreate 时创建）：错误要在发送端立刻暴露，
	// 不能等到几小时后到期移入时才发现 topic 不存在、消息无处可去。
	if _, err := p.mt.EnsureTopic(m.Topic); err != nil {
		return nil, err
	}
	if m.DeliverAtMs <= time.Now().UnixMilli() {
		return p.AppendWith(m, nil)
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
		return nil, err
	}
	p.delayMu.Lock()
	defer p.delayMu.Unlock()
	seq, err := p.nextDelaySeqLocked()
	if err != nil {
		return nil, err
	}
	b := p.st.NewBatch()
	b.Set(store.DelayKey(m.DeliverAtMs, seq), raw, nil)
	b.Set(store.DelayAllocKey(), store.PutU64(seq+1), nil)
	if err := p.st.Apply(b); err != nil {
		return nil, fmt.Errorf("写入延时消息 %s (topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
	}
	// 与 Append 同理：Apply 成功后才推进内存缓存，失败的写不能烧掉 seq
	p.delayNext = seq + 1
	p.delayLoaded = true
	p.logger.Debug("延时消息已暂存", "topic", m.Topic, "msg_id", m.ID,
		"due_ms", m.DeliverAtMs, "seq", seq)
	return m, nil
}

// nextDelaySeqLocked 取下一延时 seq。缓存未命中时读盘上 delayalloc 计数器，
// 崩溃/重启后 O(1) 恢复。调用方必须持有 p.delayMu。
func (p *Producer) nextDelaySeqLocked() (uint64, error) {
	if p.delayLoaded {
		return p.delayNext, nil
	}
	v, ok, err := p.st.Get(store.DelayAllocKey())
	if err != nil {
		return 0, fmt.Errorf("读取延时 seq 计数器: %w", err)
	}
	if !ok {
		return 0, nil
	}
	return store.GetU64(v), nil
}

// Append 写入一条普通消息（M1 签名保持不变）。
func (p *Producer) Append(m *core.Message) (*core.Message, error) { return p.AppendWith(m, nil) }

// nextOffsetLocked 取该队列下一 offset，懒加载盘上 alloc/ 计数器。
// 调用方必须持有 qs.mu。
func (qs *queueState) nextOffsetLocked(st *store.Store, topic string, q uint32) (uint64, error) {
	if qs.loaded {
		return qs.next, nil
	}
	v, ok, err := st.Get(store.AllocKey(topic, q))
	if err != nil {
		return 0, fmt.Errorf("读取 offset 计数器 %s/%d: %w", topic, q, err)
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
