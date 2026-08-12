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
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/replication"
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
	rep    replication.Replicator
	rt     replication.Router
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
//
// rep/rt 为复制抽象与组路由视图：单机档传 replication.NewStandalone(st)
// 与 StandaloneRouter{}，集群档由 main 装配。
func New(rep replication.Replicator, rt replication.Router, st *store.Store,
	mt *meta.Meta, logger *slog.Logger) *Producer {
	return &Producer{
		st: st, rep: rep, rt: rt, mt: mt, logger: logger.With("mod", "produce"),
		qstates: map[string]*queueState{}, rr: map[string]uint32{}, wakers: map[string]chan struct{}{},
	}
}

// qkey 拼 (topic, queue) 的 map 键。每条消息写入都要拼一次，是热路径——
// 手工拼接绕开 fmt.Sprintf 的反射与临时分配（pprof 实测热点）；键无人解析，
// 仅作 map key，语义与原 Sprintf 完全一致。
func qkey(topic string, q uint32) string {
	return topic + "/" + strconv.FormatUint(uint64(q), 10)
}

// InvalidateCounters 使全部 offset/seq 计数缓存失效（集群档组 leader 变更时
// 由 OnLeaderChange 钩子触发；单机档无变更事件，正常不会调用）。
//
// 为什么必须失效：offset 计数器只在所属组的 leader 上权威——follower 节点
// 的 alloc/ 值可能滞后（复制在途）；本节点重新当选 leader 时，若沿用旧内存
// 值（重新当选前跟随期间可能已落后多笔写），会出现重复分配（内存值高于盘上
// 多数派事实）或跳号（内存值低于多数派事实）。置 loaded=false 后，重新当选
// 后的第一笔写强制从 alloc/ 重读——复制保证盘上是多数派事实，重读即回到
// 权威值。seq（delay）计数器同理。
func (p *Producer) InvalidateCounters() {
	p.mu.Lock()
	for _, qs := range p.qstates {
		qs.mu.Lock()
		qs.loaded = false
		qs.mu.Unlock()
	}
	p.mu.Unlock()
	p.delayMu.Lock()
	p.delayLoaded = false
	p.delayMu.Unlock()
	p.logger.Info("leader 变更，offset 缓存失效")
}

// Append 写入一条普通消息（spec §5 流程 1）：队列选择、offset 分配、消息与
// alloc 计数器 + Keys 索引同一 Batch 原子提交，fsync 完成后唤醒长轮询。
// 本方法是本 Producer 唯一的普通消息追加入口（AppendDelay 为延时专用入口）。
//
// 注意：消息追加只保证本消息语义域内原子；跨语义域的关联写（DLQ 转入、延时
// 转正、事务提交时的来源删除）由调用方以两段式完成（先 Append 写目标，后独立
// 批次删来源）——单机单批原子在集群多 raft 组下不可表达，本方法不再提供
// 跨域注入点。调用方不得在持有本 Producer 任何锁的临界区内调用本方法
// （p.mu 与 queueState.mu 均不可重入）。
func (p *Producer) Append(ctx context.Context, m *core.Message) (*core.Message, error) {
	if len(m.Body) == 0 || len(m.Body) > MaxBodySize {
		return nil, fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), MaxBodySize)
	}
	tc, err := p.mt.EnsureTopic(ctx, m.Topic)
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
		// 普通消息轮询选队。集群档多组摊布下，rr 落在哪条队列决定了提案
		// 进哪个 raft 组——offset 分配是 leader-only 构造（Task 8 不变量），
		// 本节点只该写自己 lead 的组。逐条尝试直到选中本节点 lead 组的
		// 队列；全部不命中（探针放行后理论不可达）返回 ErrNotLeader，
		// 由 SDK 换节点重试（三节点 e2e 实测：盲目 rr 会让单节点反复
		// 提案到自己不 lead 的组，SDK 重试预算 5 次可能耗尽）。
		for i := uint32(0); i < tc.Queues; i++ {
			q := (p.rr[m.Topic] + i) % tc.Queues
			if p.rt.IsLeader(p.rt.GroupForQueue(m.Topic, q)) {
				m.QueueID = q
				break
			}
		}
		p.rr[m.Topic]++
		if !p.rt.IsLeader(p.rt.GroupForQueue(m.Topic, m.QueueID)) {
			p.mu.Unlock()
			return nil, fmt.Errorf("%w: topic %s 本节点未 lead 任何队列组", replication.ErrNotLeader, m.Topic)
		}
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
	// 组号 g 必须在此锁内、offset 分配之后计算——QueueID 到此刻才确定，
	// GroupForQueue 是「队列→组」的入盘映射（Task 4 约定），提前算会错组。
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
	b.Set(store.MsgKey(m.Topic, m.QueueID, m.Offset), raw)
	b.Set(store.AllocKey(m.Topic, m.QueueID), store.PutU64(off+1))
	// Keys 业务索引与消息同批写入（spec §5 流程 1）：崩溃时索引与消息要么都在要么都不在
	for _, key := range m.Keys {
		if key == "" {
			continue // 空 key 无检索意义（SDK 不会生成，防御脏输入）
		}
		b.Set(store.KeyIdxKey(m.Topic, key, m.StoreAtMs, m.QueueID, m.Offset), nil)
	}
	g := p.rt.GroupForQueue(m.Topic, m.QueueID)
	pending, err := p.rep.ApplyAsync(ctx, g, b)
	if err != nil {
		qs.mu.Unlock()
		return nil, fmt.Errorf("写入消息 %s (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}
	// ApplyAsync 成功 == 本条已在 WAL/memtable 中定序。此刻立即推进 offset 缓存
	// 并解锁：同队列后继消息随即进锁定序，与本条一起挂在 commit pipeline 里共享
	// 同一次 fsync——这就是 group commit 在队列内生效的机制（吞吐从 1/fsync延迟
	// 变为 合并深度/fsync延迟）。
	//
	// 为什么敢在 Wait 之前推进 qs.next：内存 offset 缓存烧掉无害——任何
	// 让 Wait 失败的场景都不丢消息：
	//   - 集群档提案失败（ErrNotLeader：提交期间领导权迁移）：offset
	//     缓存的失效只发生在重新当选时——onLeaderChange 的 isSelf==true
	//     分支触发 InvalidateCounters 置 qs.loaded=false（I4 同步失效）；
	//     非 leader 期间提案必然被 ErrNotLeader 拒，不存在「缓存已烧、
	//     却返回成功」的路径；重获 leadership 的窗口已由 group.go 的
	//     顺序屏障关闭（OnLeaderChange 钩子先于 IsLeader 对外可见），
	//     后续读取从 store 重读实际状态，绝不返回烧掉的 offset；
	//   - 集群档 WaitSync 失败 ≠ Pebble 不可恢复：raft 副本仍持数据，
	//     offset 分配是 leader-only 构造，本节点重启以 learner 追平即恢复；
	//   - 单机档 Pebble 真坏（WAL sync 失败 = 不可恢复态）：进程只能重启，
	//     重启后计数器与实际落盘由「同批原子提交」保证严格一致，内存里
	//     烧掉的 offset 无害。
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
// <=0 属编程错误直接报错）。到期时间已过（<=now）时直通 Append 立即投递：
// 语义上"到期的延时消息"就是普通消息，绕道暂存区再被调度器搬回来只是
// 多一次读写放大，结果完全相同。
//
// 返回：写入后的消息。注意暂存态消息没有队列与 offset（m.QueueID/Offset
// 保持零值）——它们在到期移入 msg/ 时才由正常写入路径分配。
//
// 原子性：delay 条目与 seq 计数器同一 Batch 提交，理由与 Append 的
// offset 计数器完全相同（崩溃后计数器与已写条目严格一致，seq 绝不复用）。
func (p *Producer) AppendDelay(ctx context.Context, m *core.Message) (*core.Message, error) {
	if m.DeliverAtMs <= 0 {
		return nil, fmt.Errorf("AppendDelay 要求 DeliverAtMs>0，得到 %d", m.DeliverAtMs)
	}
	if len(m.Body) == 0 || len(m.Body) > MaxBodySize {
		return nil, fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), MaxBodySize)
	}
	// 写入时就确认 topic 存在（autoCreate 时创建）：错误要在发送端立刻暴露，
	// 不能等到几小时后到期移入时才发现 topic 不存在、消息无处可去。
	if _, err := p.mt.EnsureTopic(ctx, m.Topic); err != nil {
		return nil, err
	}
	if m.DeliverAtMs <= time.Now().UnixMilli() {
		return p.Append(ctx, m)
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
	seq, err := p.nextDelaySeqLocked()
	if err != nil {
		p.delayMu.Unlock()
		return nil, err
	}
	b := p.st.NewBatch()
	b.Set(store.DelayKey(m.DeliverAtMs, seq), raw)
	b.Set(store.DelayAllocKey(), store.PutU64(seq+1))
	// 延时暂存区键族归元数据组（delay/ 与队列无关，无 GroupForQueue 映射）
	g := p.rt.MetaGroup()
	pending, err := p.rep.ApplyAsync(ctx, g, b)
	if err != nil {
		p.delayMu.Unlock()
		return nil, fmt.Errorf("写入延时消息 %s (topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
	}
	// 定序成功即推进 seq 缓存并解锁：并发的延时写入随即进锁定序，与本条共享
	// 同一次 fsync（拆分提交，与 Append 的 qs.next 同款）。提前推进的安全性
	// 论证也相同：WaitSync 失败 == WAL sync 失败 == Pebble 不可恢复错误态，
	// 重启后 seq 计数器与条目由同批原子提交保证一致，内存里烧掉的 seq 无害。
	p.delayNext = seq + 1
	p.delayLoaded = true
	p.delayMu.Unlock()

	// 锁外等待持久化：fsync 完成之前绝不确认（语义红线 1）
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待延时消息 %s 持久化 (topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
	}
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

// AppendBatch 将同一 topic 的一批普通消息整批落入同一队列：连续 offset 段
// [off, off+N)、单个 Pebble Batch、一次 fsync，整批原子——要么全部落盘要么
// 全部不落，比逐条 Append 的「第 N 条失败前 N-1 条无法撤回」语义更强。
//
// 参数：msgs 非空；全部同 topic、且均为普通消息（无事务、无延时、无
// MessageGroup）。路由约束由 rpc 层保证，此处再做防御校验：FIFO 消息的
// 队列由 MessageGroup 哈希决定，与整批轮询选队冲突；事务/延时各有独立
// 暂存路径——三者一律报错，调用方应回退逐条处理。
//
// 返回：与入参同序的消息切片，QueueID/Offset/ID/时间戳已回填。
//
// 注意：整批绑定单一队列与 RocketMQ batch 绑定单 MessageQueue 的语义一致；
// 批与批之间仍按轮询换队列，长期负载均衡不受影响。
func (p *Producer) AppendBatch(ctx context.Context, msgs []*core.Message) ([]*core.Message, error) {
	if len(msgs) == 0 {
		return nil, fmt.Errorf("AppendBatch 要求至少一条消息")
	}
	topic := msgs[0].Topic
	for _, m := range msgs {
		if m.Topic != topic {
			return nil, fmt.Errorf("AppendBatch 批内 topic 不一致: %q vs %q", topic, m.Topic)
		}
		if m.Transactional || m.DeliverAtMs > 0 || m.MessageGroup != "" {
			return nil, fmt.Errorf("AppendBatch 仅接受普通消息（批内含事务/延时/FIFO 消息）")
		}
		if len(m.Body) == 0 || len(m.Body) > MaxBodySize {
			return nil, fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), MaxBodySize)
		}
	}
	tc, err := p.mt.EnsureTopic(ctx, topic)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	for _, m := range msgs {
		if m.ID == "" {
			m.ID = core.NewMessageID()
		}
		if m.BornAtMs == 0 {
			m.BornAtMs = now
		}
		m.StoreAtMs = now
	}

	// 段 1（p.mu）：整批一次队列选择——批内同队列是一次 fsync 与整批原子的前提。
	// 集群档多组摊布下必须只选本节点 lead 组的队列（offset 分配是
	// leader-only 构造，理由同 Append 段 1 注释）；全部不命中返回
	// ErrNotLeader 由 SDK 换节点重试。
	p.mu.Lock()
	qid := uint32(0)
	for i := uint32(0); i < tc.Queues; i++ {
		q := (p.rr[topic] + i) % tc.Queues
		if p.rt.IsLeader(p.rt.GroupForQueue(topic, q)) {
			qid = q
			break
		}
	}
	p.rr[topic]++
	if !p.rt.IsLeader(p.rt.GroupForQueue(topic, qid)) {
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: topic %s 本节点未 lead 任何队列组", replication.ErrNotLeader, topic)
	}
	k := qkey(topic, qid)
	qs, ok := p.qstates[k]
	if !ok {
		qs = &queueState{}
		p.qstates[k] = qs
	}
	p.mu.Unlock()

	// 段 2（qs.mu）：连续 offset 段分配 + 编码 + 单批定序。组号 g 同
	// Append：锁内、qid 分配后计算（队列→组映射此时才可判定）。
	qs.mu.Lock()
	off, err := qs.nextOffsetLocked(p.st, topic, qid)
	if err != nil {
		qs.mu.Unlock()
		return nil, err
	}
	b := p.st.NewBatch()
	for i, m := range msgs {
		m.QueueID = qid
		m.Offset = off + uint64(i)
		raw, err := core.EncodeMessage(m)
		if err != nil {
			qs.mu.Unlock()
			// 未提交而放弃的批次必须自行 Close 回收（store.NewBatch 契约路径 2）
			b.Close()
			return nil, fmt.Errorf("编码消息 %s (topic=%s): %w", m.ID, topic, err)
		}
		b.Set(store.MsgKey(topic, qid, m.Offset), raw)
		for _, key := range m.Keys {
			if key == "" {
				continue // 空 key 无检索意义（与 Append 同款防御）
			}
			b.Set(store.KeyIdxKey(topic, key, m.StoreAtMs, qid, m.Offset), nil)
		}
	}
	// alloc 计数器一次写到 off+N，与全部消息同批原子（语义红线 3 的批量形态）
	b.Set(store.AllocKey(topic, qid), store.PutU64(off+uint64(len(msgs))))
	g := p.rt.GroupForQueue(topic, qid)
	pending, err := p.rep.ApplyAsync(ctx, g, b)
	if err != nil {
		qs.mu.Unlock()
		return nil, fmt.Errorf("批量写入 %d 条 (topic=%s q=%d off=%d): %w", len(msgs), topic, qid, off, err)
	}
	// 提前推进的理由与 Append 完全相同（见其注释）
	qs.next = off + uint64(len(msgs))
	qs.loaded = true
	qs.mu.Unlock()

	// 锁外等待持久化：fsync 完成之前绝不返回、绝不唤醒（语义红线 1）
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待批量写入持久化 (topic=%s q=%d off=%d n=%d): %w", topic, qid, off, len(msgs), err)
	}

	p.mu.Lock()
	p.wakeLocked(topic)
	p.mu.Unlock()
	p.logger.Debug("批量消息已写入", "topic", topic, "queue", qid, "first_offset", off, "count", len(msgs))
	return msgs, nil
}

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
