// Package produce 实现消息写入路径：队列选择、offset 分配、落盘、可见性通知。
//
// 职责：
//   - Append：一次语义写 = 消息体 + alloc 计数器，同一 Batch 原子提交
//   - offset 分配采用持久化计数器（alloc/ key），重启 O(1) 恢复且绝不回退
//   - 可见性通知：消息持久化成功后，锁外逐个调用 OnVisible 注册的监听者，
//     报出 (topic, 队列)；按队列唤醒长轮询是监听者（deliver）的事（卡 SQ4.2）
//   - AppendDelay：延时消息写 delay/ 暂存区，seq 计数器同批原子提交
//
// 边界：
//   - 延时判定入口在此（AppendDelay），到期搬运是 delay 包的事；事务（M6）仍在 Append 之前分流
//   - 不做消费可见性判断（deliver 的事）
//   - 锁结构：p.mu 护共享 map；每 (topic,queue) 一把锁护写入；delayMu 护延时暂存
//   - leader-only 构造前置：三个写入口在读计数器前过 Router.WaitWritable
//     （集群档新 leader apply 追平本任期首条前不分配，卡 SQ1），计数器缓存
//     与返回的 epoch 绑定——epoch 变了即从盘重读
package produce

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
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
	// epoch 是 next 所属的可写门 epoch（集群档 = 所属组的 leader 任期；
	// 单机档恒 0）。缓存只在同一 epoch 内有效：本任期内本节点是唯一写者，
	// 内存值即权威；换届期间他人可能写过，必须从盘重读（门已保证盘上
	// 包含本任期首条之前的全部已提交写）。
	epoch uint64
}

// Producer 写入引擎。并发安全。
type Producer struct {
	st     *store.Store
	rep    replication.Replicator
	rt     replication.Router
	mt     *meta.Meta
	logger *slog.Logger

	// mu 只护共享 map（qstates/rr）与监听者注册的写端——临界区内不再有任何 I/O。
	// 旧实现的单一全局锁跨越 store.Apply（fsync 在内），使所有队列的写入
	// 全局串行、group commit 失效，见 git 历史中原锁注释的完整推导。
	mu      sync.Mutex
	qstates map[string]*queueState
	rr      map[string]uint32

	// visible 可见性监听者列表（写时拷贝：OnVisible 持 mu 换整份切片，写入
	// 热路径只做一次原子读，不取 mu）。
	visible atomic.Pointer[[]func(topic string, q uint32)]

	// delayMu 护延时暂存区的 seq 分配与落盘（单一全局计数器，天然串行；
	// 独立成锁是为了不与普通写入互相阻塞）。
	delayMu     sync.Mutex
	delayNext   uint64
	delayLoaded bool
	delayEpoch  uint64 // delayNext 所属的元数据组 epoch，语义同 queueState.epoch

	// afterGateHook 测试专用注入点（生产恒为 nil，同 txn.Manager.afterAppendHook
	// 的约定）：Append 过可写门拿到 epoch 之后、进 qs.mu 之前调用，用于在
	// 「门已放行、提案未入队」之间确定性地注入换届（卡 SQ1 计划修订 R1）。
	afterGateHook func(g uint32, epoch uint64)
}

// New 构造 Producer。offset 缓存懒加载（首写某队列时读一次 alloc/ key）。
//
// rep/rt 为复制抽象与组路由视图：单机档传 replication.NewStandalone(st)
// 与 StandaloneRouter{}，集群档由 main 装配。
func New(rep replication.Replicator, rt replication.Router, st *store.Store,
	mt *meta.Meta, logger *slog.Logger) *Producer {
	return &Producer{
		st: st, rep: rep, rt: rt, mt: mt, logger: logger.With("mod", "produce"),
		qstates: map[string]*queueState{}, rr: map[string]uint32{},
	}
}

// qkey 拼 (topic, queue) 的 map 键。每条消息写入都要拼一次，是热路径——
// 手工拼接绕开 fmt.Sprintf 的反射与临时分配（pprof 实测热点）；键无人解析，
// 仅作 map key，语义与原 Sprintf 完全一致。
func qkey(topic string, q uint32) string {
	return topic + "/" + strconv.FormatUint(uint64(q), 10)
}

// Append 写入一条普通消息（spec §5 流程 1）：队列选择、offset 分配、消息与
// alloc 计数器 + Keys 索引同一 Batch 原子提交，持久化完成后通知可见性监听者。
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

	// 过可写门（不持任何锁等待）：QueueID 已在段 1 确定，g 是「队列→组」
	// 的入盘映射（Task 4 约定）。epoch 绑定段 2 的 offset 缓存。
	g := p.rt.GroupForQueue(m.Topic, m.QueueID)
	epoch, err := p.rt.WaitWritable(ctx, g)
	if err != nil {
		p.logger.Debug("写入被可写门拒绝", "topic", m.Topic, "queue", m.QueueID, "group", g, "err", err)
		return nil, fmt.Errorf("写入消息 %s (topic=%s q=%d): %w", m.ID, m.Topic, m.QueueID, err)
	}
	if p.afterGateHook != nil {
		p.afterGateHook(g, epoch)
	}

	// 段 2（qs.mu）：offset 分配 + 编码 + 落盘。同队列串行（offset 顺序 ==
	// 落盘顺序，FIFO 根基），跨队列并行（group commit 合并 fsync）。
	qs.mu.Lock()
	off, err := qs.nextOffsetLocked(p.st, m.Topic, m.QueueID, epoch)
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
	pending, err := p.rep.ApplyAsyncAt(ctx, g, b, epoch)
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
	//   - 集群档提案失败（ErrNotLeader：提交期间领导权迁移）：缓存带着
	//     本任期 epoch，换届后 WaitWritable 返回新 epoch，首次分配从盘
	//     重读；而门只在 apply 越过新任期首条后才放行——此时盘上已含上
	//     任期全部已提交写，重读值不会与任何已确认消息重叠。同任期内被
	//     丢弃的提案只烧掉 offset（跳号），不重复；
	//   - 提案经 ApplyAsyncAt 按 epoch 绑定任期：门放行后、入队前若本节点
	//     失主又以更高任期复任（期间他人可能写过同一队列），本批是用旧
	//     epoch 缓存构造的，入队/出队核对任期不符即失败，绝不进新任期日志
	//     （卡 SQ1 计划修订 R1）；
	//   - 集群档 WaitSync 失败 ≠ Pebble 不可恢复：raft 副本仍持数据，
	//     offset 分配是 leader-only 构造，本节点重启以 learner 追平即恢复；
	//   - 单机档 Pebble 真坏（WAL sync 失败 = 不可恢复态）：进程只能重启，
	//     重启后计数器与实际落盘由「同批原子提交」保证严格一致，内存里
	//     烧掉的 offset 无害。
	qs.next = off + 1
	qs.loaded = true
	qs.epoch = epoch
	qs.mu.Unlock()

	// 锁外等待持久化：fsync 完成之前绝不返回、绝不唤醒（语义红线 1）。
	// 可见性窗口说明（防止后人误改）：Pebble 的 Commit(Sync) 本就是「先发布可见、
	// 后等 fsync」，拉取型读者在旧实现里同样可能于 fsync 完成前读到本条——本改动
	// 没有扩大该窗口，只是把等待从锁内挪到锁外。
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待消息 %s 持久化 (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}

	// 段 3：通知可见性监听者。必须在持久化成功之后——被唤醒的订阅者读 store
	// 必能看到这条消息（集群档 Wait 返回 = 已在本节点 apply）。
	p.notifyVisible(m.Topic, m.QueueID)
	p.logger.Debug("消息已写入", "topic", m.Topic, "queue", m.QueueID, "offset", m.Offset, "msg_id", m.ID, "keys", len(m.Keys))
	return m, nil
}

// AppendDelay 写入一条延时消息。
//
// 参数：m.DeliverAtMs 必须 > 0。
//
// 返回：
//   - stored: 回填了 ID/时间戳的消息
//   - seq: 该条目在延时暂存区的分配序号，与 DeliverAtMs 一起构成
//     store.DelayKey——撤回（RecallMessage）靠这两个值定位条目
//   - staged: 是否真的进了延时暂存区。**已到期的消息会直通 Append**，
//     此时 staged=false、seq 无意义。调用方必须据此判断能否签发 recall
//     句柄：给一条已经进了队列的消息签句柄，撤回时只会查无此条。
//   - err: 失败原因
//
// 为什么用布尔而不是 seq==0 当"未暂存"的哨兵：延时 seq 从 0 开始分配
// （见 nextDelaySeqLocked：计数器不存在时返回 0），0 是合法 seq。
//
// 为什么把 seq 放进返回值而不是挂到 core.Message 上：core.Message 会被
// EncodeMessage 持久化，加字段等于动存储格式（消息 v1 二进制编码刚落地）；
// 而改签名是编译期可见的——所有调用点会立刻报错，不会留下静默失配。
func (p *Producer) AppendDelay(ctx context.Context, m *core.Message) (*core.Message, uint64, bool, error) {
	if m.DeliverAtMs <= 0 {
		return nil, 0, false, fmt.Errorf("AppendDelay 要求 DeliverAtMs>0，得到 %d", m.DeliverAtMs)
	}
	if len(m.Body) == 0 || len(m.Body) > MaxBodySize {
		return nil, 0, false, fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), MaxBodySize)
	}
	// 写入时就确认 topic 存在（autoCreate 时创建）：错误要在发送端立刻暴露，
	// 不能等到几小时后到期移入时才发现 topic 不存在、消息无处可去。
	if _, err := p.mt.EnsureTopic(ctx, m.Topic); err != nil {
		return nil, 0, false, err
	}
	if m.DeliverAtMs <= time.Now().UnixMilli() {
		// 已到期直通正常追加：消息此刻就进队列，不经暂存区，因此没有 seq、
		// 也不可撤回（staged=false）
		stored, err := p.Append(ctx, m)
		return stored, 0, false, err
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
		return nil, 0, false, err
	}
	// 延时暂存区键族归元数据组（delay/ 与队列无关，无 GroupForQueue 映射）。
	// 过可写门（不持 delayMu 等待），epoch 绑定 seq 缓存。
	g := p.rt.MetaGroup()
	epoch, err := p.rt.WaitWritable(ctx, g)
	if err != nil {
		p.logger.Debug("延时写入被可写门拒绝", "topic", m.Topic, "group", g, "err", err)
		return nil, 0, false, fmt.Errorf("写入延时消息 %s (topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
	}
	p.delayMu.Lock()
	seq, err := p.nextDelaySeqLocked(epoch)
	if err != nil {
		p.delayMu.Unlock()
		return nil, 0, false, err
	}
	b := p.st.NewBatch()
	b.Set(store.DelayKey(m.DeliverAtMs, seq), raw)
	b.Set(store.DelayAllocKey(), store.PutU64(seq+1))
	pending, err := p.rep.ApplyAsyncAt(ctx, g, b, epoch)
	if err != nil {
		p.delayMu.Unlock()
		return nil, 0, false, fmt.Errorf("写入延时消息 %s (topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
	}
	// 定序成功即推进 seq 缓存并解锁：并发的延时写入随即进锁定序，与本条共享
	// 同一次 fsync（拆分提交，与 Append 的 qs.next 同款）。提前推进的安全性
	// 论证也相同：WaitSync 失败 == WAL sync 失败 == Pebble 不可恢复错误态，
	// 重启后 seq 计数器与条目由同批原子提交保证一致，内存里烧掉的 seq 无害。
	p.delayNext = seq + 1
	p.delayLoaded = true
	p.delayEpoch = epoch
	p.delayMu.Unlock()

	// 锁外等待持久化：fsync 完成之前绝不确认（语义红线 1）
	if err := pending.Wait(); err != nil {
		return nil, 0, false, fmt.Errorf("等待延时消息 %s 持久化 (topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
	}
	p.logger.Debug("延时消息已暂存", "topic", m.Topic, "msg_id", m.ID,
		"due_ms", m.DeliverAtMs, "seq", seq)
	return m, seq, true, nil
}

// nextDelaySeqLocked 取下一延时 seq。缓存未命中（未装载或 epoch 不同）
// 时读盘上 delayalloc 计数器，崩溃/重启/换届后 O(1) 恢复。调用方必须持有
// p.delayMu。epoch 语义与过期判定同 queueState.nextOffsetLocked。
func (p *Producer) nextDelaySeqLocked(epoch uint64) (uint64, error) {
	if p.delayLoaded {
		if epoch == p.delayEpoch {
			return p.delayNext, nil
		}
		if epoch < p.delayEpoch {
			return 0, staleEpochErr("delay seq", epoch, p.delayEpoch)
		}
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

	// 过可写门（不持锁等待），理由同 Append。
	g := p.rt.GroupForQueue(topic, qid)
	epoch, err := p.rt.WaitWritable(ctx, g)
	if err != nil {
		p.logger.Debug("批量写入被可写门拒绝", "topic", topic, "queue", qid, "group", g, "err", err)
		return nil, fmt.Errorf("批量写入 %d 条 (topic=%s q=%d): %w", len(msgs), topic, qid, err)
	}

	// 段 2（qs.mu）：连续 offset 段分配 + 编码 + 单批定序。
	qs.mu.Lock()
	off, err := qs.nextOffsetLocked(p.st, topic, qid, epoch)
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
	pending, err := p.rep.ApplyAsyncAt(ctx, g, b, epoch)
	if err != nil {
		qs.mu.Unlock()
		return nil, fmt.Errorf("批量写入 %d 条 (topic=%s q=%d off=%d): %w", len(msgs), topic, qid, off, err)
	}
	// 提前推进的理由与 Append 完全相同（见其注释）
	qs.next = off + uint64(len(msgs))
	qs.loaded = true
	qs.epoch = epoch
	qs.mu.Unlock()

	// 锁外等待持久化：fsync 完成之前绝不返回、绝不唤醒（语义红线 1）
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待批量写入持久化 (topic=%s q=%d off=%d n=%d): %w", topic, qid, off, len(msgs), err)
	}

	p.notifyVisible(topic, qid)
	p.logger.Debug("批量消息已写入", "topic", topic, "queue", qid, "first_offset", off, "count", len(msgs))
	return msgs, nil
}

// nextOffsetLocked 取该队列下一 offset：同 epoch 命中内存缓存，否则读盘上
// alloc/ 计数器。调用方必须持有 qs.mu。
//
// 参数 epoch：调用方刚从 Router.WaitWritable 拿到的可写门 epoch。
//
// 返回错误的情形之一是 epoch 比缓存的旧（ErrNotLeader 包装，可重试）：调用
// 方拿到 epoch 后、进锁前已换届，且新任期已有写者推进了缓存——此时新任期
// 的在途写可能尚未 apply，按旧 epoch 重读盘会与之重叠，绝不能分配。
func (qs *queueState) nextOffsetLocked(st *store.Store, topic string, q uint32, epoch uint64) (uint64, error) {
	if qs.loaded {
		if epoch == qs.epoch {
			return qs.next, nil
		}
		if epoch < qs.epoch {
			return 0, staleEpochErr(topic+"/"+strconv.FormatUint(uint64(q), 10), epoch, qs.epoch)
		}
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

// staleEpochErr 调用方持有的可写门 epoch 已被更新的任期取代（ErrNotLeader
// 包装：客户端重试即拿到新 epoch）。
func staleEpochErr(what string, got, cur uint64) error {
	return fmt.Errorf("%w: %s 计数器缓存已属任期 %d，调用方 epoch %d 已过期", replication.ErrNotLeader, what, cur, got)
}

// OnVisible 注册可见性监听者：此后每次 Append / AppendBatch 持久化成功，以
// 消息实际落入的 (topic, 队列) 调用 fn（批量写入整批一次）。可注册多个，
// fn 为 nil 时忽略。
//
// 注意：fn 在写入方的 goroutine 上、不持本 Producer 任何锁时同步调用，必须
// 快速返回且不得回调本 Producer 的写入口；调用方（如 moveToDLQ）可能持有
// 自己的锁调用 Append，fn 只应取叶子锁。
func (p *Producer) OnVisible(fn func(topic string, q uint32)) {
	if fn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var next []func(string, uint32)
	if cur := p.visible.Load(); cur != nil {
		next = append(next, *cur...)
	}
	next = append(next, fn)
	p.visible.Store(&next)
}

// notifyVisible 逐个调用可见性监听者（调用方不持任何 Producer 锁）。
func (p *Producer) notifyVisible(topic string, q uint32) {
	fns := p.visible.Load()
	if fns == nil {
		return
	}
	for _, fn := range *fns {
		fn(topic, q)
	}
}
