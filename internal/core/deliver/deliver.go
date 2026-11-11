// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责：
//   - Receive：先重投本队列已过期的 inflight，再从 fetch 位点取新消息；
//     新取消息写 inflight 并推进位点（同一提案原子落盘）
//   - Ack/ChangeInvisible：按 (group,topic,queue,offset,attempt) 定位 inflight——
//     attempt 是消费者持有句柄的一部分，防止陈旧句柄误伤被重投覆盖的新记录
//     （见类型注释与方法注释的详细说明）
//   - 长轮询：produce.Subscribe 的 close-broadcast + 截止时间
//
// 消费状态的权威（卡 SQ4.1）：cursor 与 inflight 由队列所属组的可写 leader
// 在内存持有（state.go），全部读写在队列锁内对内存进行，不再读 pebble 的
// cursor/、inflight/ 键族（消息本体 msg/ 照旧读 pebble）；pebble 上的这两族
// 是持久化副本，装载、失效与提交见 checkpoint.go。
//
// 边界：
//   - 重投是惰性的（Receive 时检查到期索引），无后台扫描 goroutine——
//     没有消费者在收时也就没有人需要重投，语义等价而实现最简
//   - 投递次数超上限的消息在重投检查时惰性转入死信 %DLQ%{group}（1 队列）；
//     重投指数退避靠不可见时长下限实现，详见 retryBackoff 注释
//   - Tag 过滤与 SQL92 属性过滤均已落地；跳过计数按原因分桶
//     （tag_miss/sql_false/sql_unknown）供 /metrics 抓取，见 FilterSkipped
//   - 顺序消息（M4）：队列级顺序锁，MessageGroup 非空即顺序；重投无退避、超限入 DLQ 解锁
//
// 位点语义说明（对应 spec §5.2"推进到最小未 ack"的实现形态）：
//
//	cursor 是 fetch 位点，Receive 取出即推进；未 ack 的消息由持久化的
//	inflight 记录兜底（崩溃重启后仍在，过期即重投）。两者合起来等价于
//	"已 ack 前消息不丢"，且比维护最小未 ack 位点简单得多。不变式 I1：盘上
//	cursor 恒与其覆盖区间的 inflight 同批写（批次构造见 state.go）。
//
// 组归属（batch③）：本包全部写点（inflight/ 与 cursor/ 键族）都归「被消费
// 队列」的组——组号统一 rt.GroupForQueue(topic, queueID)，经 rep 提交。
package deliver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
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

// Deliverer POP 消费引擎。
//
// 并发安全：每个 (group,topic,queue) 的消费状态是一个 queueState（state.go），
// 由该队列所属组的可写 leader 在内存持有；取件、确认、改不可见、续租、转死信、
// 重置位点全部在 queueState.mu 内读写内存态（经 acquire 前置取锁），不同队列
// 并行。内存态是权威：锁内改完即对下一个持锁者可见，不存在「提案 apply 前放锁、
// 下一个持锁者从 pebble 读到旧状态」的窗口（SQ3 的根因）。新增任何读写
// inflight/cursor 的方法都必须经 acquire 取同一把锁，否则上面这句不再成立。
//
// 持久化两种模式（checkpoint.go），区别只在「谁、何时」把内存态写成批次：
//   - 逐次复制（WithCheckpointInterval(0) 或不传选项）：每次变更以「cursor +
//     变更的 inflight 同批」差量提案，落盘后才交件/应答；同队列至多一个提案在途
//   - 检查点（间隔 > 0，Start 起后台循环）：变更只改内存即返回；每个间隔把
//     脏队列整体替换落盘一次（DeleteRange inflight 前缀 + 全部 inflight + cursor）
type Deliverer struct {
	rep    replication.Replicator
	rt     replication.Router
	fwd    replication.Forwarder // 跨节点转发（集群档）；单机档 nil——IsLeader 恒真，转发分支不可达
	st     *store.Store
	mt     *meta.Meta
	pr     *produce.Producer
	logger *slog.Logger

	mu  sync.Mutex
	qmu map[string]*queueState // "group/topic/qid" -> 队列消费状态（含队列锁）

	// skipMu 保护 skipped。计数在扫描回调里累加（持队列锁），
	// 而 FilterSkipped 由 metrics 抓取协程调用——两者天然并发。
	// 为避免 metrics 抓取的锁竞争进入投递热路径，回调本身不加锁：
	// 计数先累进本趟扫描的局部 map，扫描结束后才持 skipMu 一次性合并。
	skipMu  sync.Mutex
	skipped map[FilterSkipKey]uint64

	// interval 检查点间隔；0 = 逐次复制模式。构造后只读。
	interval time.Duration
	// maxInFlight 逐次复制模式下每队列在途提案上限（构造后只读）：提交后端
	// 声明前缀有序（replication.PrefixOrderedCommitter）时为
	// maxInFlightPrefix，否则为 1（修订 R1/R3）。
	maxInFlight int

	// ckMu 保护 ckDirty（检查点模式的脏队列集合，叶子锁：可在持队列锁时取）。
	ckMu    sync.Mutex
	ckDirty map[*queueState]struct{}
	// roundMu 串行化检查点轮次（后台循环、停机收尾、计划内转移前检查点）。
	roundMu sync.Mutex
	// closed Stop 之后为真：新的消费调用一律 ErrNotLeader（停机收尾检查点
	// 之后不得再有内存变更）。
	closed atomic.Bool
	// loopCancel/loopDone 检查点后台循环的停止与退出信号（Start 置）。
	loopCancel context.CancelFunc
	loopDone   chan struct{}

	// fenceMu 保护 fences：组 → 围栏截止时刻（计划内转移领导权期间消费调用
	// 阻塞等待，P3）。
	fenceMu sync.Mutex
	fences  map[uint32]time.Time

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
//
// opts 可选（变参尾置，既有调用点不改）：不传即逐次复制模式；main 永远显式
// 传 WithCheckpointInterval(配置值)。检查点模式须调用 Start 起后台循环、
// 停机时调用 Stop 做收尾检查点。
func New(rep replication.Replicator, rt replication.Router, st *store.Store,
	mt *meta.Meta, pr *produce.Producer, logger *slog.Logger, opts ...Option) *Deliverer {
	fwd, _ := rt.(replication.Forwarder)
	d := &Deliverer{rep: rep, rt: rt, fwd: fwd, st: st, mt: mt, pr: pr,
		logger: logger.With("mod", "deliver"), qmu: map[string]*queueState{},
		skipped: map[FilterSkipKey]uint64{}, ckDirty: map[*queueState]struct{}{},
		fences: map[uint32]time.Time{}}
	d.maxInFlight = 1
	if pc, ok := rep.(replication.PrefixOrderedCommitter); ok && pc.PrefixOrderedCommit() {
		d.maxInFlight = maxInFlightPrefix
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// maxInFlightPrefix 前缀有序后端下每队列在途提案上限（计划修订 R3，授权
// [4,64]）：16 消费者打同一队列时 pebble 约 8 次提交合并一次 fsync，16 =
// 2× 该并发留余量；超出部分进 R2 的合并提案，在途批次数有界。
const maxInFlightPrefix = 16

// Option Deliverer 构造选项。
type Option func(*Deliverer)

// WithCheckpointInterval 设消费状态检查点间隔：0（或负值）= 逐次复制模式，
// > 0 = 检查点模式（配置键 consume_checkpoint_interval，卡 SQ4.1）。
func WithCheckpointInterval(iv time.Duration) Option {
	return func(d *Deliverer) {
		if iv < 0 {
			iv = 0
		}
		d.interval = iv
	}
}

// FilterSkipKey 跳过计数的维度：topic + 消费组 + 跳过原因。
type FilterSkipKey struct {
	Topic  string
	Group  string
	Reason string
}

// FilterSkipped 返回跳过计数的快照。
//
// 供 /metrics 抓取期调用（metrics.FilterStats）。返回的是拷贝，
// 调用方可以随意持有；计数只增不减，进程重启归零（counter 语义）。
func (d *Deliverer) FilterSkipped() map[FilterSkipKey]uint64 {
	d.skipMu.Lock()
	defer d.skipMu.Unlock()
	out := make(map[FilterSkipKey]uint64, len(d.skipped))
	for k, v := range d.skipped {
		out[k] = v
	}
	return out
}

// skipReasonOf 把（过滤器类型, 三值结果）映射为计数维度。
//
// 为什么用类型断言而不给 Filter 加 Kind() 方法：Kind 是构造期就确定的
// 元信息，加进接口会让每个实现都要维护一个与自身类型重复的字段；
// 而这里只有两个实现，断言一次即可，接口保持只有 Match 一个方法。
func skipReasonOf(f Filter, r Result) string {
	if _, ok := f.(*TagFilter); ok {
		return "tag_miss"
	}
	if r == ResultUnknown {
		return "sql_unknown"
	}
	return "sql_false"
}

// Receive 取一批消息，返回的每条消息 DeliveryAttempt 已填充（首投=1）。
// 先重投本队列已过期的 inflight，再从 fetch 位点取新消息，合计不超过 maxMsgs。
// 空结果时最长等待 wait（长轮询），期间新消息写入会立即唤醒重试；
// wait<=0 时不等待，取一次就返回。
// 只投 filter.Match 返回 ResultTrue 的新消息；filter 不得为 nil，不过滤时传
// AllPass。不匹配的跳过并推进本组位点，不投递、不占 inflight（对该组永久
// 跳过）。阶段 1 的过期重投不重新过滤。
func (d *Deliverer) Receive(ctx context.Context, group, topic string, queueID uint32, maxMsgs int, invisible, wait time.Duration, filter Filter, opts ...ReceiveOption) ([]*core.Message, error) {
	var ro receiveOpts
	for _, opt := range opts {
		opt(&ro)
	}
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
		msgs, err := d.receiveOnce(ctx, group, topic, queueID, maxMsgs, invisible, gc.EffectiveMaxAttempts(), filter, ro.lease)
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

// receiveOnce 单次取件：前置（可写门 + 队列锁 + 装载）→ 锁内改内存态
// （receiveLocked）→ 变更交给逐次复制提案 → 放锁 → 等落盘后交件。
// inflight 与 cursor 落盘之前绝不交件（语义红线 1/3——消费者拿到消息时，
// 它的 inflight 兜底记录必然已持久化，崩溃后该消息仍会被重投而不是丢失）。
// 放锁后同队列的下一次取件/确认立即可进锁，看到的是本次改后的内存态。
func (d *Deliverer) receiveOnce(ctx context.Context, group, topic string, queueID uint32, maxMsgs int, invisible time.Duration, maxAttempts int32, filter Filter, lease Lease) ([]*core.Message, error) {
	qs, _, err := d.acquire(ctx, group, topic, queueID)
	if err != nil {
		return nil, err
	}
	var (
		out  []*core.Message
		rerr error
		f    *flush
	)
	func() {
		defer qs.mu.Unlock()
		out, rerr = d.receiveLocked(ctx, qs, maxMsgs, invisible, maxAttempts, filter, lease)
		// 出错时 receiveLocked 只留下了已成事实的删除（孤儿清理、已写入死信
		// 的源 inflight），照常提交，避免下一轮重复转死信
		f = d.commitLocked(qs)
	}()
	if f != nil {
		if werr := d.awaitCommit(ctx, f); werr != nil {
			if rerr != nil {
				d.logger.Warn("取件出错后提交已成事实的删除也失败", "group", group, "topic", topic,
					"queue", queueID, "err", werr)
				return nil, rerr
			}
			return nil, fmt.Errorf("等待取件批次持久化 (group=%s topic=%s q=%d): %w", group, topic, queueID, werr)
		}
	}
	if rerr != nil {
		return nil, rerr
	}
	return out, nil
}

// receiveLocked 单次取件的锁内部分（调用方持 qs.mu，内存态已装载）：
// 阶段 1 从到期索引取已到期 inflight——可续租的续租、其余至多 maxMsgs 条
// 重投（判孤儿、判超限转死信、退避、保留 Ordered）；阶段 2 从内存 cursor
// 扫 msg/ 取新消息。全部改动只进内存并记脏，由调用方提交。
//
// 出错时返回 (nil, err)：凡涉及读盘的判定都先做完再改内存（重投与新投递在
// 全部读盘成功后才落到内存），已成事实的删除（孤儿、已写入死信）保留。
func (d *Deliverer) receiveLocked(ctx context.Context, qs *queueState, maxMsgs int, invisible time.Duration, maxAttempts int32, filter Filter, lease Lease) ([]*core.Message, error) {
	group, topic, queueID := qs.group, qs.topic, qs.q
	now := time.Now().UnixMilli()
	expireAt := now + invisible.Milliseconds()

	// 阶段 1：到期 inflight。未启用续租时只需最早的 maxMsgs 条（不可能续租，
	// 多取无用）；启用时看完全部已到期记录——续租不占 maxMsgs 预算，占了会让
	// 「一批慢消息正在续租」白白挤掉本轮本可以投出的新消息。
	var reds []*inflightEntry
	for _, e := range qs.expired(now, maxMsgs, lease.Enabled()) {
		if renewable(lease, &e.st, now) {
			// invisible < 1ms 时 Milliseconds() 为 0，newExp == now，会每个兜底
			// tick（100ms）续租一次——与重投路径的 exp 计算同款性质。
			newExp := now + invisible.Milliseconds()
			if newExp > e.st.RenewUntilMs {
				// 不越过硬上限：到点这一轮把 ExpireAtMs 正好压在上限上，
				// 下一轮判据 2 即为假，自然转入重投
				newExp = e.st.RenewUntilMs
			}
			// 只改到期时刻：Attempts（收据句柄的一部分，改了就作废持有者手里
			// 的句柄）与 Ordered（顺序锁判据）原样保留
			qs.setExpire(e, newExp)
			d.logger.Debug("inflight 自动续租", "group", group, "topic", topic,
				"queue", queueID, "offset", e.off, "owner", e.st.Owner,
				"attempts", e.st.Attempts, "new_expire_at_ms", newExp,
				"budget_remain_ms", e.st.RenewUntilMs-now)
			continue
		}
		if len(reds) >= maxMsgs {
			continue
		}
		// 只在「本可以续租却没续成」时记一笔，区分两种原因——慢消费者排障的
		// 入口。判据 1（本次取件没启用续租）不打：SimpleConsumer 的每一次
		// 正常重投都会命中它，打了就是刷屏。
		if lease.Enabled() && e.st.Owner != "" {
			if e.st.RenewUntilMs <= now {
				d.logger.Info("续租预算已耗尽，转正常重投", "group", group,
					"topic", topic, "queue", queueID, "offset", e.off,
					"owner", e.st.Owner, "attempts", e.st.Attempts,
					"renew_until_ms", e.st.RenewUntilMs)
			} else {
				d.logger.Info("持有者会话已断，立即重投", "group", group,
					"topic", topic, "queue", queueID, "offset", e.off,
					"owner", e.st.Owner, "attempts", e.st.Attempts)
			}
		}
		reds = append(reds, e)
	}

	// 阶段 1.5：重投候选逐条读消息本体。续租不查消息本体（retention 清掉
	// 消息后残留的 inflight 只要持有者仍活就会被续租，孤儿清理推迟到续租
	// 预算耗尽——不引入额外随机读的设计代价，有界）。
	type redo struct {
		off uint64
		m   *core.Message
		st  core.InflightState
	}
	var redos []redo
	for _, e := range reds {
		raw, ok, err := d.st.Get(store.MsgKey(topic, queueID, e.off))
		if err != nil {
			return nil, fmt.Errorf("读取重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, e.off, err)
		}
		if !ok {
			// 消息已被 retention 清理但 inflight 残留：删除记录（随本次提交
			// 落盘，否则长轮询每 100ms 都会再扫到、再打这条 Warn）。被删的若是
			// 持有顺序锁的记录，顺序计数随之减一，锁即释放。
			d.logger.Warn("inflight 指向的消息不存在，清理孤儿记录", "group", group, "topic", topic, "queue", queueID, "offset", e.off)
			qs.remove(e.off)
			continue
		}
		m, err := core.DecodeMessage(raw)
		if err != nil {
			return nil, fmt.Errorf("解码重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, e.off, err)
		}
		// 投递次数耗尽：转入死信 topic，不再投递（moveToDLQ 先同步写死信、
		// 成功后才删内存 inflight）。锁序 deliver → produce 单向，无环。
		if e.st.Attempts >= maxAttempts {
			ordered := e.st.Ordered
			if err := d.moveToDLQ(ctx, qs, e.off, m, e.st.Attempts, "投递次数耗尽（未在不可见期内 ack）"); err != nil {
				return nil, err
			}
			if ordered {
				// 卡住队头的顺序消息已随 inflight 一并移除：顺序锁释放，
				// 本轮阶段 2 即可投出下一条顺序消息（卡住→超限→推进，spec 流程 4/6）
				d.logger.Info("顺序消息超限入死信，队列解锁", "group", group, "topic", topic,
					"queue", queueID, "offset", e.off, "msg_id", m.ID)
			}
			continue
		}
		m.DeliveryAttempt = e.st.Attempts + 1
		// 指数退避只作用于非顺序消息（spec §5 流程 6）：顺序消息要的是原地
		// 快速重投（卡队头），退避会把整条队列拖住数分钟——不可见时长直接用
		// 客户端值。协议层 InvisibleDuration 回填用同一判据（receive.go）。
		exp := expireAt
		if !e.st.Ordered {
			if bo := retryBackoff(m.DeliveryAttempt); bo > invisible {
				exp = now + bo.Milliseconds()
			}
		}
		// 重投必须原样保留 Ordered 标记：丢了它，顺序计数漏算这条，下一条
		// 顺序消息会与卡在队头的这条并发投递，顺序即破。
		nst := core.InflightState{ExpireAtMs: exp, Attempts: m.DeliveryAttempt, Ordered: e.st.Ordered}
		if lease.Enabled() {
			// 重投等于一次全新的投递：归属换成本次轮询方，续租预算重新计时
			// （不继承上一任持有者的 RenewUntilMs）。
			nst.Owner = lease.Owner
			nst.RenewUntilMs = now + lease.MaxRenew.Milliseconds()
		}
		redos = append(redos, redo{off: e.off, m: m, st: nst})
	}

	// 阶段 2：从内存 fetch 位点取新消息。
	//
	// 仅当阶段 1 未填满 maxMsgs 时才进入本阶段：store.Scan 的 limit<=0 语义是
	// "不限"，不能把算出来的 0 传进去（会从 cursor 起把整条剩余队列扫出来）。
	type fresh struct {
		m  *core.Message
		st core.InflightState
	}
	var news []fresh
	cursor := qs.cursor
	newCursor := cursor
	if len(redos) < maxMsgs {
		// 顺序锁判据（spec §5 流程 4）：内存在外顺序计数，本轮投出顺序消息即占锁
		orderedBusy := qs.orderedN > 0
		lower := store.MsgKey(topic, queueID, cursor)
		upper := store.PrefixUpperBound(store.MsgQueuePrefix(topic, queueID))
		// skipReasons 是本趟扫描的局部跳过计数，扫描结束后才一次性合并进
		// d.skipped（回调持队列锁，逐条持 skipMu 会拉长队列锁持有时间）。
		skipReasons := map[string]int{}
		err := d.st.Scan(lower, upper, scanBudget, func(k, v []byte) (bool, error) {
			m, err := core.DecodeMessage(v)
			if err != nil {
				return false, err
			}
			// 过滤在顺序锁之前：不匹配的消息（含顺序消息）对本消费组永久跳过、
			// 推进位点。放在顺序锁之后的话，被锁拦住的不匹配消息会永远堵住位点。
			if r := filter.Match(m); r != ResultTrue {
				newCursor = m.Offset + 1
				skipReasons[skipReasonOf(filter, r)]++
				return true, nil
			}
			// 顺序锁：队列存在未终结的顺序 inflight 时，后续顺序消息不投。停止
			// 扫描且【不推进 newCursor】——这条消息未被投递，位点停在它前面，
			// 它仍是下一条候选（卡住而不跳过）。副作用：其后的普通消息一并等待
			// （队头阻塞，设计决策 3，README 建议顺序消息用专用 topic）。
			if m.MessageGroup != "" && orderedBusy {
				d.logger.Debug("顺序锁阻塞取件", "group", group, "topic", topic,
					"queue", queueID, "blocked_offset", m.Offset)
				return false, nil
			}
			newCursor = m.Offset + 1
			m.DeliveryAttempt = 1
			ordered := m.MessageGroup != ""
			if ordered {
				// 投出即占锁：本轮扫描内的下一条顺序消息就会被上面的判定拦下
				orderedBusy = true
			}
			nst := core.InflightState{ExpireAtMs: expireAt, Attempts: 1, Ordered: ordered}
			if lease.Enabled() {
				// 只有启用续租的取件才在记录上留归属与预算：SimpleConsumer 的
				// 投递不写这两个字段，其 inflight 与改造前逐字节相同
				nst.Owner = lease.Owner
				nst.RenewUntilMs = now + lease.MaxRenew.Milliseconds()
			}
			news = append(news, fresh{m: m, st: nst})
			return len(redos)+len(news) < maxMsgs, nil
		})
		if err != nil {
			return nil, fmt.Errorf("扫描新消息 (topic=%s q=%d cursor=%d): %w", topic, queueID, cursor, err)
		}
		if len(skipReasons) > 0 {
			d.skipMu.Lock()
			for reason, n := range skipReasons {
				d.skipped[FilterSkipKey{Topic: topic, Group: group, Reason: reason}] += uint64(n)
			}
			d.skipMu.Unlock()
			d.logger.Debug("订阅过滤跳过消息", "group", group, "topic", topic, "queue", queueID,
				"tag_miss", skipReasons["tag_miss"],
				"sql_false", skipReasons["sql_false"],
				"sql_unknown", skipReasons["sql_unknown"])
		}
	}

	// 读盘全部成功，落内存：重投在前、新消息在后（交件顺序同旧实现）
	out := make([]*core.Message, 0, len(redos)+len(news))
	for _, r := range redos {
		qs.insert(r.off, r.st)
		out = append(out, r.m)
	}
	for _, n := range news {
		qs.insert(n.m.Offset, n.st)
		out = append(out, n.m)
	}
	qs.setCursor(newCursor)

	if len(out) == 0 {
		if qs.hasDirty() {
			// 「无投递但有变更」：续租、清理孤儿、超限转死信，或全部新消息被
			// 过滤跳过、只推进了位点。单独一句，不与投递共用消息——打成
			// 「投递消息 count=0」会让人以为队列空转。
			d.logger.Debug("本轮无可投递消息，仅续租了 inflight、清理了孤儿或推进了过滤位点",
				"group", group, "topic", topic, "queue", queueID, "cursor", newCursor)
		}
	} else {
		d.logger.Debug("投递消息已定序", "group", group, "topic", topic, "queue", queueID,
			"count", len(out), "redelivered", len(redos), "cursor", newCursor)
	}
	return out, nil
}

// Ack 确认消息。attempt 必须与该 offset 当前 InflightState.Attempts 一致——
// 消费者持有的 attempt 来自它收到的那次 Receive（core.Message.DeliveryAttempt）。
//
// 为什么要校验 attempt，而不是只按 (group,topic,queue,offset) 删记录：
// 若消费者 A 收到 X（attempt=1）后处理超时，X 会被过期重投给消费者 B（attempt=2，
// 全新的过期时间）。此时 A 迟到的 Ack(X) 若不带 attempt 校验，会直接删掉 B 那条
// 记录——X 从此既无 inflight 兜底、cursor 也已跳过它，一旦 B 处理失败或崩溃，
// X 就永久丢失。attempt 不匹配说明这是一个陈旧句柄，语义上等价于"记录已不存在"：
// 幂等返回 (false, nil)，不报错。
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

// AckBatch 单一 (group,topic,queue) 的批量确认：一把队列锁、逐条在内存里
// 校验并删除，有效条目合成一次提案。
//
// 参数：entries 至少一条（rpc 层保证；空批直接返回 nil,nil 防御）。
// 返回：与 entries 同序的结果；error 时任何条目都不算已确认（提案失败则
// 内存态随之失效重装，被删的 inflight 回到盘上状态）。
//
// 落盘前绝不向调用方报告确认成功（语义红线 1）；全部落空不提案。
func (d *Deliverer) AckBatch(ctx context.Context, group, topic string, queueID uint32, entries []AckEntry) ([]AckResult, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	qs, _, err := d.acquire(ctx, group, topic, queueID)
	if err != nil {
		return nil, err
	}
	results := make([]AckResult, len(entries))
	acked := 0
	var f *flush
	func() {
		defer qs.mu.Unlock()
		for i, e := range entries {
			results[i] = AckResult{Offset: e.Offset}
			ent, ok := qs.inflight[e.Offset]
			if !ok {
				// 已 ack 或已重投：幂等落空，不影响其它条目（语义红线 4）
				d.logger.Debug("批量 ack 目标不存在（重复 ack 或已重投）",
					"group", group, "topic", topic, "queue", queueID, "offset", e.Offset)
				continue
			}
			if ent.st.Attempts != e.Attempt {
				d.logger.Debug("批量 ack attempt 不匹配（陈旧句柄，已被重投覆盖）",
					"group", group, "topic", topic, "queue", queueID, "offset", e.Offset,
					"want_attempt", ent.st.Attempts, "got_attempt", e.Attempt)
				continue
			}
			qs.remove(e.Offset)
			results[i].OK = true
			acked++
		}
		if acked > 0 {
			f = d.commitLocked(qs)
		}
	}()
	if f == nil {
		return results, nil
	}
	if err := d.awaitCommit(ctx, f); err != nil {
		return nil, fmt.Errorf("等待批量 ack 持久化 (group=%s topic=%s q=%d n=%d): %w", group, topic, queueID, acked, err)
	}
	d.logger.Debug("消息已确认", "group", group, "topic", topic, "queue", queueID,
		"requested", len(entries), "acked", acked)
	return results, nil
}

// ChangeInvisible 重设不可见截止时间（消费端主动延长/缩短处理时间）。
// attempt 校验规则与 Ack 相同：必须与当前 Attempts 一致，否则视为陈旧句柄，
// 返回 (false, nil)——若改的是已被重投覆盖的旧记录，新的过期时间会作用在
// "别人正在处理的新一轮投递"上。
//
// inflight 不存在或 attempt 不匹配都返回 (false, nil)，幂等，不算错误。
// 成功返回前新的过期时间必然已落盘（语义红线 1）。
func (d *Deliverer) ChangeInvisible(ctx context.Context, group, topic string, queueID uint32, offset uint64, attempt int32, invisible time.Duration) (bool, error) {
	qs, _, err := d.acquire(ctx, group, topic, queueID)
	if err != nil {
		return false, err
	}
	var f *flush
	func() {
		defer qs.mu.Unlock()
		e, ok := qs.inflight[offset]
		if !ok {
			d.logger.Debug("改不可见时间目标不存在（已 ack 或已重投）", "group", group, "topic", topic, "queue", queueID, "offset", offset)
			return
		}
		if e.st.Attempts != attempt {
			d.logger.Debug("改不可见时间 attempt 不匹配（陈旧句柄，已被重投覆盖）",
				"group", group, "topic", topic, "queue", queueID, "offset", offset,
				"want_attempt", e.st.Attempts, "got_attempt", attempt)
			return
		}
		qs.setExpire(e, time.Now().Add(invisible).UnixMilli())
		f = d.commitLocked(qs)
	}()
	if f == nil {
		return false, nil
	}
	if err := d.awaitCommit(ctx, f); err != nil {
		return false, fmt.Errorf("等待改不可见时间持久化 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Debug("已更新不可见时间", "group", group, "topic", topic, "queue", queueID,
		"offset", offset, "attempt", attempt, "invisible_ms", invisible.Milliseconds())
	return true, nil
}

// ForwardToDLQ 将一条 inflight 消息显式转入死信 %DLQ%{group}（协议
// ForwardMessageToDeadLetterQueue，spec §6：顺序消息重试超限时客户端显式
// 入 DLQ；对非顺序消息同样可用）。
//
// 参数与校验规则与 Ack 完全一致：attempt 必须与当前 Attempts 匹配，陈旧
// 句柄幂等返回 (false, nil)，绝不误伤新一轮投递的记录。
//
// 返回：(true, nil) 已转入（或目标消息已被 retention 清理、孤儿 inflight 已
// 清除——"这条消息别再投了"两种情况下都已成立）；(false, nil) 目标不存在或
// 句柄陈旧；错误在存储故障或提交失败时返回。
func (d *Deliverer) ForwardToDLQ(ctx context.Context, group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error) {
	qs, _, err := d.acquire(ctx, group, topic, queueID)
	if err != nil {
		return false, err
	}
	var (
		f    *flush
		ferr error
	)
	// 锁内段用 defer 放锁：moveToDLQ 的测试钩子会在两段之间 panic 模拟崩溃
	func() {
		defer qs.mu.Unlock()
		e, ok := qs.inflight[offset]
		if !ok {
			d.logger.Debug("forward 目标不存在（已 ack 或已重投）", "group", group, "topic", topic, "queue", queueID, "offset", offset)
			return
		}
		if e.st.Attempts != attempt {
			d.logger.Debug("forward attempt 不匹配（陈旧句柄，已被重投覆盖）",
				"group", group, "topic", topic, "queue", queueID, "offset", offset,
				"want_attempt", e.st.Attempts, "got_attempt", attempt)
			return
		}
		raw, ok, err := d.st.Get(store.MsgKey(topic, queueID, offset))
		if err != nil {
			ferr = fmt.Errorf("读取 forward 消息 (topic=%s q=%d off=%d): %w", topic, queueID, offset, err)
			return
		}
		if !ok {
			// 消息已被 retention 清理但 inflight 残留：与取件的孤儿清理同理删除止损
			qs.remove(offset)
			d.logger.Warn("forward 目标消息已不存在，清理孤儿 inflight", "group", group, "topic", topic, "queue", queueID, "offset", offset)
			f = d.commitLocked(qs)
			return
		}
		m, err := core.DecodeMessage(raw)
		if err != nil {
			ferr = fmt.Errorf("解码 forward 消息 (topic=%s q=%d off=%d): %w", topic, queueID, offset, err)
			return
		}
		if err := d.moveToDLQ(ctx, qs, offset, m, attempt, "客户端显式转入死信"); err != nil {
			ferr = err
			return
		}
		f = d.commitLocked(qs)
	}()
	if ferr != nil {
		return false, ferr
	}
	if f == nil {
		return false, nil
	}
	if err := d.awaitCommit(ctx, f); err != nil {
		return false, fmt.Errorf("等待删除源 inflight 持久化 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	return true, nil
}

// moveToDLQ 将消息复制入 %DLQ%{group} 并删除内存里的源 inflight（调用方持
// qs.mu；删除随调用方的提交落盘）。
//
// 死信保留原消息 ID/Body/Tag/Keys（ID 不变便于全链路追踪），来源坐标写入
// Properties（sq-origin-topic/queue/offset，控制台溯源与重发用），并附上
// 投递次数与转入原因（sq-dlq-attempts/sq-dlq-reason）——控制台的死信列表
// 要回答的第一个问题就是「试了几次、为什么进来的」；MessageGroup 置空——
// 死信不再参与顺序语义。死信写入失败时内存不变、返回错误。
func (d *Deliverer) moveToDLQ(ctx context.Context, qs *queueState, offset uint64,
	m *core.Message, attempts int32, reason string) error {
	group, topic, queueID := qs.group, qs.topic, qs.q
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
	// 两段式转入：第一段同步写死信（d.pr.Append / 转发），成功后第二段才删
	// 内存 inflight（随调用方的提案或检查点落盘）。先写后删；崩溃窗口（死信
	// 落盘后、inflight 删除落盘前）重放 = 重复死信条目，at-least-once 语义内
	// ——重投扫描会再次把超限消息转入，死信区出现两条同 ID 条目，死信消费端
	// 按消息 ID 幂等即可。次序不得反转（反转 = 丢失）。
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
	// 第二段：删内存源 inflight（顺序计数随之减一）。落盘随调用方的提交；
	// 提交失败 = 内存态失效重装、inflight 回到盘上 = 下次重投再次转入 =
	// 重复死信条目（at-least-once 允许）。
	qs.remove(offset)
	d.logger.Info("消息投递超限转入死信", "group", group, "topic", topic, "queue", queueID,
		"offset", offset, "msg_id", m.ID, "dlq_topic", dlqTopic)
	return nil
}

// ResetCursor 重置某队列的消费位点并清空该队列全部 inflight（Admin API 位点
// 重置）。offset 允许任意值：向后 = 重复消费（at-least-once 语义内），向前 =
// 跳过消息。
//
// 在队列锁内完成：丢弃旧内存态（挂起的变更以可重试错误失败）→ 置新 cursor、
// 清空 inflight → 全量批次（DeleteRange 前缀 + Set cursor，不依赖盘上基线）
// 按 epoch 提案并同步等待落盘后才放锁——下一次取件必从新位点开始，不会被
// 旧内存态或在途的旧差量覆盖（旧差量在同组提案队列里排在本批之前）。
// 提交失败则内存态失效，下次访问从盘重装。
//
// 超时须重试：返回错误（含等待超时）时重置的结局未知——批次可能仍会迟到
// 落盘，也可能被该队列随后的消费提交覆盖（重装后的第一个提案整体替换，把
// 重装时读到的旧状态写回，见 queueState.needFull）。调用方（Admin API）
// 收到失败应重试直至成功，不能把「已报失败」当成「可能已生效」。
//
// 提案 ctx 不取调用方 ctx（Background + consumeProposeTimeout）：管理请求
// 断开不应让一个已入队的重置提前放弃等待、徒增结局未知。
func (d *Deliverer) ResetCursor(ctx context.Context, group, topic string, queueID uint32, offset uint64) error {
	qs, g, err := d.acquire(ctx, group, topic, queueID)
	if err != nil {
		return err
	}
	defer qs.mu.Unlock()
	epoch := qs.epoch
	d.dropLocked(qs, errors.New("位点重置"))
	qs.init(epoch, offset)
	b := d.st.NewBatch()
	qs.buildFull(b)
	pctx, cancel := context.WithTimeout(context.Background(), consumeProposeTimeout)
	defer cancel()
	p, err := d.rep.ApplyAsyncAt(pctx, g, b, epoch)
	if err == nil {
		err = p.Wait()
	}
	if err != nil {
		d.dropLocked(qs, err)
		return fmt.Errorf("重置位点 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Info("消费位点已重置", "group", group, "topic", topic, "queue", queueID, "offset", offset)
	return nil
}
