// checkpoint.go 队列消费状态的装载、失效与持久化（卡 SQ4.1）。
//
// 职责：
//   - 前置（acquire）：过可写门拿 epoch → 取队列锁 → 内存态未装载或 epoch
//     变大即从 pebble 装载；持有更旧 epoch 的调用返回 ErrNotLeader
//   - 失效（dropLocked）：换届、管理批次、前序提案失败时丢弃内存态，下次
//     访问重装；等待中的后续变更一并以可重试错误失败
//   - 逐次复制（commitLocked / finishFlush）：每次变更以「cursor + 变更的
//     inflight 同批」差量提案，落盘后才交件/应答。同队列至多 maxInFlight 个
//     差量在途（R1 为 1；前缀有序后端放宽，见计划修订 R3）：达到上限期间到达
//     的变更只改内存并挂到「下一次提案」，有名额空出才构造下一个差量；在途
//     提案失败即丢弃内存态，等本队列在途全部了结后从盘重装
//   - 检查点（checkpointRound）：检查点模式下变更只改内存、登记脏队列；后台
//     循环每个间隔对脏队列做一轮整体替换落盘（修订 R1：DeleteRange inflight
//     前缀 + Set 全部 inflight + Set cursor，同批）——任一检查点批次单独落地
//     都自洽，前序批次丢弃或失败无害
//   - 停机（Start/Stop）：Stop 先关门（新调用 ErrNotLeader）再停循环，最后对
//     全部脏队列做收尾检查点——正常停机不产生重复投递
//   - 计划内转移（PrepareTransfer，P1/P3）：置组围栏（到达的调用阻塞等待），
//     对该组全部已装载队列做检查点（逐次模式下等在途提案了结），之后才允许
//     转移领导权——计划内切换不产生重复投递
//   - 管理批次（ApplyAdminBatch）：删消费组等旁路批次经此串行化，先失效再提交
//
// 边界：
//   - 一切消费状态提案经 ApplyAsyncAt 绑定内存态装载时的 epoch：内存态只在
//     该 epoch 内权威，绝不让它的批次进入其他任期的日志（主会话裁决）
//   - 不判定领导权本身（Router.WaitWritable 是唯一来源）；不碰 cluster 内部
//     ——转移前回调由 cluster 的摊布循环经组装点（main）调用 PrepareTransfer
package deliver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

// consumeProposeTimeout 单个消费状态提案的等待上限。提案 ctx 不取调用方
// ctx：一次提案承载多个调用方的变更（在途期间挂上来的），某个调用方放弃
// 不应拖垮其他人；上限防止失去领导权后条目被截断、waiter 永不触发时
// 等待者无限挂住（超时 = 结果未知，按失败失效重装，后果至多重复投递）。
var consumeProposeTimeout = 10 * time.Second // var：测试注入小值制造「结局未知」

// errStateDropped 内存态被丢弃时，挂在「下一次提案」上的变更以它失败。
// 包装 ErrNotLeader：rpc 层映射为可重试错误，客户端重试即走重装后的状态。
var errStateDropped = fmt.Errorf("%w: 队列消费状态已失效", replication.ErrNotLeader)

// errStopped Stop 之后的消费调用（停机收尾检查点之后不得再有内存变更）。
var errStopped = fmt.Errorf("%w: 消费引擎已停止", replication.ErrNotLeader)

// queue 返回 (group,topic,queueID) 的队列状态，不存在则创建（未装载）。
// 只在 d.mu 内读写 map 本身；返回值的 mu 由调用方在 d.mu 之外取。
func (d *Deliverer) queue(group, topic string, q uint32) *queueState {
	// 每次取件与每 100ms 的兜底轮询都要拼一次键，是热路径——手工拼接绕开
	// fmt.Sprintf 的反射与临时分配；键无人解析，仅作 map key。
	k := group + "/" + topic + "/" + strconv.FormatUint(uint64(q), 10)
	d.mu.Lock()
	defer d.mu.Unlock()
	qs, ok := d.qmu[k]
	if !ok {
		qs = newQueueState(group, topic, q)
		d.qmu[k] = qs
	}
	return qs
}

// acquire 公共前置：过可写门 → 取队列锁 → 按 epoch 装载/重装。
//
// 返回：已加锁、已装载、epoch == 本次门 epoch 的队列状态（调用方负责解锁）
// 与队列所属组号。错误：门未开（ErrNotLeader 包装）、本次 epoch 已过时
// （ErrNotLeader 包装）、装载读盘失败、等待在途了结期间 ctx 结束。
//
// 未装载而本队列仍有未了结的提案（任意代号）时，先放锁等它们全部了结再
// 重来一遍（修订 R3：重装读盘必须晚于在途了结，盘上才是它们生效后的状态）。
//
// 注意：WaitWritable 可能阻塞（新 leader 追 apply，上限 3s），必须在取队列
// 锁之前调用。
func (d *Deliverer) acquire(ctx context.Context, group, topic string, q uint32) (*queueState, uint32, error) {
	g := d.rt.GroupForQueue(topic, q)
	qs := d.queue(group, topic, q)
	for {
		e, err := d.lockGated(ctx, qs, g)
		if err != nil {
			d.logger.Debug("消费请求未过可写门", "group", group, "topic", topic, "queue", q, "g", g, "err", err)
			return nil, g, err
		}
		if qs.loaded && qs.epoch > e {
			cur := qs.epoch
			qs.mu.Unlock()
			d.logger.Debug("消费请求持有的 epoch 已过时", "group", group, "topic", topic, "queue", q,
				"epoch", e, "state_epoch", cur)
			return nil, g, fmt.Errorf("%w: 组 %d 本次 epoch %d 早于队列状态 epoch %d", replication.ErrNotLeader, g, e, cur)
		}
		if qs.loaded && qs.epoch == e {
			return qs, g, nil
		}
		if qs.loaded {
			// 换届后重新当选：旧任期内存态里可能有他人任期内已被覆盖的状态，
			// 一律丢弃从盘重装（门开 = 旧任期条目已全部 apply 或被截断）
			d.logger.Info("队列消费状态随换届失效，从盘重装", "group", group, "topic", topic,
				"queue", q, "old_epoch", qs.epoch, "epoch", e)
			d.dropLocked(qs, fmt.Errorf("换届 epoch %d→%d", qs.epoch, e))
		}
		if qs.inFlight > 0 {
			ch := qs.settledLocked()
			n := qs.inFlight
			qs.mu.Unlock()
			d.logger.Debug("重装前等待本队列在途提案了结", "group", group, "topic", topic, "queue", q,
				"in_flight", n)
			select {
			case <-ch:
			case <-ctx.Done():
				return nil, g, ctx.Err()
			}
			continue
		}
		start := time.Now()
		if err := qs.load(d.st, e); err != nil {
			qs.mu.Unlock()
			d.logger.Error("装载队列消费状态失败", "group", group, "topic", topic, "queue", q, "err", err)
			return nil, g, err
		}
		d.logger.Debug("队列消费状态已装载", "group", group, "topic", topic, "queue", q,
			"epoch", e, "cursor", qs.cursor, "inflight", len(qs.inflight),
			"took", time.Since(start).String())
		return qs, g, nil
	}
}

// lockGated 过停机/围栏/可写门后取队列锁，返回门 epoch（成功时已持 qs.mu）。
func (d *Deliverer) lockGated(ctx context.Context, qs *queueState, g uint32) (uint64, error) {
	for {
		if d.closed.Load() {
			return 0, errStopped
		}
		if err := d.waitFence(ctx, g); err != nil {
			return 0, err
		}
		e, err := d.rt.WaitWritable(ctx, g)
		if err != nil {
			return 0, err
		}
		qs.mu.Lock()
		// 锁内复查：停机收尾检查点 / 转移前检查点按队列取锁构造批次，取锁
		// 晚于它们的调用必须看到关门或围栏，否则改动会落在最后一次检查点之后
		if d.closed.Load() {
			qs.mu.Unlock()
			return 0, errStopped
		}
		if !d.fenced(g) {
			return e, nil
		}
		qs.mu.Unlock()
	}
}

// settledLocked 返回「本队列在途归零」的等待点（调用方持 qs.mu 且 inFlight>0）。
func (qs *queueState) settledLocked() <-chan struct{} {
	if qs.settled == nil {
		qs.settled = make(chan struct{})
	}
	return qs.settled
}

// dropLocked 丢弃内存态（调用方持 qs.mu）：代号加一（在途提案完成时不再
// 回写），挂在「下一次提案」上的变更以 errStateDropped 失败，状态置未装载。
func (d *Deliverer) dropLocked(qs *queueState, cause error) {
	qs.gen++
	// inFlight 不清零：旧代号的在途提案仍在跑，重装要等它们了结（修订 R3）
	if f := qs.next; f != nil {
		qs.next = nil
		f.err = fmt.Errorf("%w（%v）", errStateDropped, cause)
		close(f.done)
	}
	qs.clear()
}

// commitLocked 逐次复制：本次调用已改了内存（有脏），把变更交给一次提案。
//
// 调用方持 qs.mu。在途数未达上限 → 立即构造差量并提案（ApplyAsyncAt 绑定
// qs.epoch），起一个 goroutine 等待其落盘；已达上限 → 变更留在脏集里，
// 挂到下一次提案（某个在途提案了结后由 finishFlush 构造）。上限为 1 时即
// R1/R2；前缀有序后端放宽为 maxInFlightPrefix（修订 R3）。
//
// 返回：本次变更所在提案的等待点；调用方放锁后经 awaitCommit 等待。无脏返回 nil。
func (d *Deliverer) commitLocked(qs *queueState) *flush {
	if !qs.hasDirty() {
		return nil
	}
	if d.interval > 0 {
		// 检查点模式：只登记脏队列，变更随下一轮检查点落盘
		if !qs.ckDirty {
			qs.ckDirty = true
			d.ckMu.Lock()
			d.ckDirty[qs] = struct{}{}
			d.ckMu.Unlock()
		}
		return nil
	}
	if qs.next == nil {
		qs.next = &flush{done: make(chan struct{})}
	}
	f := qs.next
	if qs.inFlight < d.maxInFlight {
		d.proposeLocked(qs)
	}
	return f
}

// proposeLocked 把 qs.next 变成在途提案：构造差量、按 epoch 提案、起等待
// goroutine。调用方持 qs.mu 且已确认在途数未达上限、qs.next 非空。
func (d *Deliverer) proposeLocked(qs *queueState) {
	f := qs.next
	qs.next = nil
	f.gen = qs.gen
	qs.inFlight++
	b := d.st.NewBatch()
	if qs.needFull {
		// 装载后的第一个提案：整体替换，压过可能迟到 apply 的旧条目（见 needFull）
		qs.buildFull(b)
		qs.needFull = false
	} else {
		qs.buildDelta(b)
	}
	g := d.rt.GroupForQueue(qs.topic, qs.q)
	ctx, cancel := context.WithTimeout(context.Background(), consumeProposeTimeout)
	p, err := d.rep.ApplyAsyncAt(ctx, g, b, qs.epoch)
	go func() {
		defer cancel()
		if err == nil {
			err = p.Wait()
		}
		d.finishFlush(qs, f, err)
	}()
}

// finishFlush 在途提案了结（在等待 goroutine 里调用）：在途数减一；本代号
// 的失败即丢弃内存态（下一差量的基线未知，只能从盘重装，且重装要等本队列
// 在途全部了结，修订 R3）；有挂起变更且名额空出即构造下一个差量；在途归零
// 时唤醒等待重装/转移的一方。最后唤醒本提案的等待者。
func (d *Deliverer) finishFlush(qs *queueState, f *flush, err error) {
	qs.mu.Lock()
	qs.inFlight--
	switch {
	case qs.gen == f.gen && err != nil:
		d.logger.Warn("消费状态提案失败，丢弃队列内存态（在途全部了结后从盘重装）",
			"group", qs.group, "topic", qs.topic, "queue", qs.q, "epoch", qs.epoch,
			"in_flight", qs.inFlight, "err", err)
		d.dropLocked(qs, err)
	case err != nil:
		d.logger.Debug("已失效代号的消费状态提案失败（内存态已丢弃或已重建，忽略）",
			"group", qs.group, "topic", qs.topic, "queue", qs.q, "err", err)
	}
	// 挂起的变更属当前代号（旧代号了结同样腾出在途名额，如 ResetCursor 之后）
	if qs.loaded && qs.next != nil && qs.inFlight < d.maxInFlight {
		d.proposeLocked(qs)
	}
	if qs.inFlight == 0 && qs.settled != nil {
		close(qs.settled)
		qs.settled = nil
	}
	qs.mu.Unlock()
	f.err = err
	close(f.done)
}

// awaitCommit 等本次变更所在提案落盘。调用方 ctx 结束即返回 ctx 错误（变更
// 仍随提案落盘或失效，与旧实现「Wait 超时、条目可能仍进日志」同语义）。
func (d *Deliverer) awaitCommit(ctx context.Context, f *flush) error {
	select {
	case <-f.done:
		return f.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ApplyAdminBatch 提交一个触及消费状态（cursor/、inflight/ 键族）的管理批次，
// 如删除消费组的成片清理（计划 P2）。本节点须为 g 组 leader（非 leader 由
// 调用方转发给 leader，leader 侧经此入口）。
//
// 做法：解析批次触及的全部队列 → 对其中本节点已有内存态的队列按队列键字典序
// 逐个取锁（不会与其他路径成环：别处至多持一把队列锁）→ 丢弃这些队列的内存态
// → 同步提交 → 放锁。提交成功返回时批次已 apply，此后重装读到的是管理批次之后
// 的状态；在途的旧提案在同组提案队列里排在本批之前，不会把被删的数据写回。
// 本节点没有内存态的队列不建空状态（无可失效）；删除期间仍有消费流量属
// adminops 的「行为未定义」运维契约。
//
// 超时须重试：返回错误（含等待超时）时结局未知——批次可能迟到落盘，也可能被
// 相关队列随后的消费提交覆盖（重装后的第一个提案整体替换，见
// queueState.needFull）。调用方应重试直至成功。
//
// 参数 b 的归属同 Replicator.Apply：调用后不再持有。解析出整组/不可解析的
// 消费键时保守地失效本节点全部队列内存态。
func (d *Deliverer) ApplyAdminBatch(ctx context.Context, g uint32, b *store.Batch) error {
	cqs, whole, err := store.BatchConsumeQueues(b.Repr())
	if err != nil {
		b.Close()
		return fmt.Errorf("解析管理批次 (g=%d): %w", g, err)
	}
	var qss []*queueState
	d.mu.Lock()
	if whole {
		for _, qs := range d.qmu {
			qss = append(qss, qs)
		}
	} else {
		for _, cq := range cqs {
			k := cq.Group + "/" + cq.Topic + "/" + strconv.FormatUint(uint64(cq.QueueID), 10)
			if qs, ok := d.qmu[k]; ok {
				qss = append(qss, qs)
			}
		}
	}
	d.mu.Unlock()
	sort.Slice(qss, func(i, j int) bool {
		a, c := qss[i], qss[j]
		if a.group != c.group {
			return a.group < c.group
		}
		if a.topic != c.topic {
			return a.topic < c.topic
		}
		return a.q < c.q
	})
	for _, qs := range qss {
		qs.mu.Lock()
		d.dropLocked(qs, errors.New("管理批次"))
	}
	err = d.rep.Apply(ctx, g, b)
	for _, qs := range qss {
		qs.mu.Unlock()
	}
	if err != nil {
		d.logger.Warn("消费状态管理批次提交失败（结局未知，须重试；相关队列内存态已失效）", "g", g,
			"queues", len(cqs), "dropped", len(qss), "whole", whole, "err", err)
		return fmt.Errorf("提交消费状态管理批次 (g=%d): %w", g, err)
	}
	d.logger.Info("消费状态管理批次已提交，相关队列内存态已失效", "g", g,
		"queues", len(cqs), "dropped", len(qss), "whole", whole)
	return nil
}

// InflightCount 本节点内存里某队列的在外条数（原子镜像，不取队列锁）。
// ok=false 表示本节点未装载该队列（非 leader 或尚未访问），调用方应回落
// 读 pebble。预留给控制台/指标改读内存（SQ4.9）。
func (d *Deliverer) InflightCount(group, topic string, q uint32) (n int, ok bool) {
	k := group + "/" + topic + "/" + strconv.FormatUint(uint64(q), 10)
	d.mu.Lock()
	qs, has := d.qmu[k]
	d.mu.Unlock()
	if !has || !qs.loadedMirror.Load() {
		return 0, false
	}
	return int(qs.nInflight.Load()), true
}

// fenceTTL 计划内转移围栏的有效期（P3，计划授权 [2s,5s]）：转移通常百毫秒内
// 完成；中止时到期自动恢复服务。var 供测试注入小值。
var fenceTTL = 3 * time.Second

// stopCheckpointRounds 停机收尾最多做几轮检查点（失败的队列会重新登记，
// 上限防止 leader 已失时空转到 ctx 截止）。
const stopCheckpointRounds = 3

// Start 启动检查点后台循环（检查点模式；逐次复制模式为空操作）。ctx 结束或
// Stop 时循环退出。只可调用一次。
func (d *Deliverer) Start(ctx context.Context) {
	if d.interval <= 0 {
		d.logger.Info("消费状态持久化：逐次复制模式（每次变更落盘后才应答）", "max_in_flight_per_queue", d.maxInFlight)
		return
	}
	lctx, cancel := context.WithCancel(ctx)
	d.loopCancel = cancel
	d.loopDone = make(chan struct{})
	d.logger.Info("消费状态持久化：检查点模式", "interval", d.interval.String())
	go func() {
		defer close(d.loopDone)
		t := time.NewTicker(d.interval)
		defer t.Stop()
		for {
			select {
			case <-lctx.Done():
				return
			case <-t.C:
			}
			// 轮次 ctx 不取循环 ctx：Stop 取消循环时，正在进行的一轮照常收尾
			// （提案已入队，半途放弃只会让收尾检查点多做一遍）
			rctx, rcancel := context.WithTimeout(context.Background(), d.roundBudget())
			d.checkpointRound(rctx, nil, false)
			rcancel()
		}
	}()
}

// roundBudget 一轮后台检查点等可写门的预算：一个间隔，至少 1s。
func (d *Deliverer) roundBudget() time.Duration {
	if d.interval < time.Second {
		return time.Second
	}
	return d.interval
}

// Stop 停止消费引擎（正常停机）：关门（此后新调用一律 ErrNotLeader）→ 停
// 检查点循环 → 对全部脏队列做收尾检查点并等待落盘。ctx 为收尾上限（计划
// 授权 [1s,4s]，由调用方给），等循环退出与收尾轮次都受它约束。逐次复制模式只关门。重复调用为空操作。
//
// 调用时机（main）：gRPC 排空之后、Manager.StopClean 与 store.Close 之前。
// 返回收尾检查点未全部落盘时的错误（后果：未落盘的进度在重启后表现为重复投递）。
func (d *Deliverer) Stop(ctx context.Context) error {
	if !d.closed.CompareAndSwap(false, true) {
		return nil
	}
	if d.interval <= 0 {
		d.logger.Info("消费引擎已停止（逐次复制模式，无收尾检查点）")
		return nil
	}
	if d.loopCancel != nil {
		d.loopCancel()
		select {
		case <-d.loopDone:
		case <-ctx.Done():
			// 后台轮次挂在提交不了的提案上（如已失去多数派）：不再等它，收尾
			// 轮次随后在同一 ctx 下取轮次信号量失败即返回错误
			d.logger.Warn("停机：等后台检查点轮次退出超时，继续收尾", "err", ctx.Err())
		}
	}
	start := time.Now()
	var last error
	total := 0
	for i := 0; i < stopCheckpointRounds; i++ {
		n, err := d.checkpointRound(ctx, nil, true)
		total += n
		last = err
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	if last != nil {
		// 失败的队列已重新登记为脏：其条数即未落盘的队列数
		d.ckMu.Lock()
		unflushed := len(d.ckDirty)
		d.ckMu.Unlock()
		// 典型成因：本节点是最后停机的成员，其余节点已离开，本节点仍自认
		// leader（未开 CheckQuorum 不会自降）而提案永远凑不齐多数派——上限
		// 到期即放弃，与 kill -9 同等后果（有界重复），不丢消息。
		d.logger.Error("停机收尾检查点未全部落盘（重启后这部分进度表现为重复投递；超时多见于本节点已失去多数派，如全集群停机时最后一个停的节点）",
			"queues", total, "unflushed_queues", unflushed, "took", time.Since(start).String(), "err", last)
		return last
	}
	d.logger.Info("消费引擎已停止，收尾检查点完成", "queues", total, "took", time.Since(start).String())
	return nil
}

// ckPending 一轮检查点里一个已入队的队列批次。
type ckPending struct {
	qs  *queueState
	gen uint64
	p   replication.Pending
}

// checkpointRound 做一轮检查点：对候选队列逐组过可写门，逐队列「取锁 → 校
// 已装载且 epoch == 门 epoch → 有变更即构造全量批次 → ApplyAsyncAt 绑 epoch」
// → 放锁；全部入队后统一等落盘。
//
// 参数：
//   - only：非 nil 时只处理该组（计划内转移）
//   - all：true 时候选 = 本节点全部已装载队列（停机/转移前：不依赖脏集登记
//     的时序，取锁本身与在改的调用串行）；false 时候选 = 脏队列集合
//
// 门未开或 epoch 已变的组：内存态直接丢弃不写（领导权已失，未落盘的变更属
// 故障后果，表现为重复投递）。批次失败：队列保留内存态并重新登记为脏，下一轮
// 重写（全量替换不依赖盘上基线，重试安全）。
//
// 返回：写出的队列数与首个失败原因。轮与轮串行（roundSem；等候受 ctx 约束，
// ctx 先到期返回其错误、本轮不做）。
func (d *Deliverer) checkpointRound(ctx context.Context, only *uint32, all bool) (int, error) {
	select {
	case d.roundSem <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	defer func() { <-d.roundSem }()
	var cands []*queueState
	if all {
		d.mu.Lock()
		for _, qs := range d.qmu {
			cands = append(cands, qs)
		}
		d.mu.Unlock()
	} else {
		d.ckMu.Lock()
		for qs := range d.ckDirty {
			cands = append(cands, qs)
		}
		d.ckDirty = map[*queueState]struct{}{}
		d.ckMu.Unlock()
	}
	byG := map[uint32][]*queueState{}
	for _, qs := range cands {
		g := d.rt.GroupForQueue(qs.topic, qs.q)
		if only != nil && g != *only {
			if !all {
				d.redirty(qs) // 非本组的脏队列放回集合
			}
			continue
		}
		byG[g] = append(byG[g], qs)
	}
	var pend []ckPending
	var firstErr error
	for g, qss := range byG {
		e, werr := d.rt.WaitWritable(ctx, g)
		for _, qs := range qss {
			qs.mu.Lock()
			qs.ckDirty = false
			if !qs.loaded || !qs.hasDirty() {
				qs.mu.Unlock()
				continue
			}
			if werr != nil || qs.epoch != e {
				cause := werr
				if cause == nil {
					cause = fmt.Errorf("epoch %d→%d", qs.epoch, e)
				}
				d.logger.Warn("检查点时本节点已不可写或已换届，丢弃未落盘的队列内存态",
					"group", qs.group, "topic", qs.topic, "queue", qs.q, "g", g,
					"epoch", qs.epoch, "err", cause)
				d.dropLocked(qs, cause)
				qs.mu.Unlock()
				continue
			}
			b := d.st.NewBatch()
			qs.buildFull(b)
			// 提案 ctx 派生自本轮 ctx：停机/转移前检查点的预算由调用方决定
			// （失去多数派时提案永不提交，不得等满单提案上限）；整批替换，
			// 截止后结局未知无妨——下一轮或重装后的首个提案仍整体替换。
			pctx, cancel := context.WithTimeout(ctx, consumeProposeTimeout)
			p, err := d.rep.ApplyAsyncAt(pctx, g, b, qs.epoch)
			if err != nil {
				p = errPending{err}
			}
			pend = append(pend, ckPending{qs: qs, gen: qs.gen, p: cancelPending{p, cancel}})
			qs.mu.Unlock()
		}
	}
	for _, cp := range pend {
		if err := cp.p.Wait(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			d.logger.Warn("队列检查点提交失败，保留内存态待下一轮重写",
				"group", cp.qs.group, "topic", cp.qs.topic, "queue", cp.qs.q, "err", err)
			cp.qs.mu.Lock()
			if cp.qs.gen == cp.gen && cp.qs.loaded {
				cp.qs.cursorDirty = true // 强制下一轮重写（buildFull 已清脏集）
				if !cp.qs.ckDirty {
					cp.qs.ckDirty = true
					d.redirty(cp.qs)
				}
			}
			cp.qs.mu.Unlock()
		}
	}
	if len(pend) > 0 {
		d.logger.Debug("检查点轮次完成", "queues", len(pend), "groups", len(byG), "err", firstErr)
	}
	return len(pend), firstErr
}

// redirty 把队列放回脏队列集合（ckMu 是叶子锁，可在持队列锁时调用）。
func (d *Deliverer) redirty(qs *queueState) {
	d.ckMu.Lock()
	d.ckDirty[qs] = struct{}{}
	d.ckMu.Unlock()
}

// errPending 入队即失败的提案。
type errPending struct{ err error }

func (p errPending) Wait() error { return p.err }

// cancelPending Wait 后释放提案 ctx。
type cancelPending struct {
	p      replication.Pending
	cancel context.CancelFunc
}

func (p cancelPending) Wait() error {
	defer p.cancel()
	return p.p.Wait()
}

// PrepareTransfer 计划内转移 g 组领导权之前调用（P1，经组装点接到
// cluster.Options.BeforeLeaderTransfer）：置围栏（此后到达该组的消费调用阻塞
// 等待，P3）→ 检查点模式对该组全部已装载队列做检查点、逐次模式等该组在途
// 提案了结 → 返回 nil 后调用方即可转移。失败即撤围栏并返回错误（调用方本
// 周期不转移）。
//
// 围栏在 fenceTTL 后自动失效：转移成功则领导权已失，围栏内等待者拿到
// ErrNotLeader；转移中止则到期恢复服务（记 Warn）。
func (d *Deliverer) PrepareTransfer(ctx context.Context, g uint32) error {
	start := time.Now()
	d.fenceMu.Lock()
	d.fences[g] = start.Add(fenceTTL)
	d.fenceMu.Unlock()
	d.logger.Info("计划内转移领导权：组围栏已置，开始转移前检查点", "g", g, "ttl", fenceTTL.String())
	var err error
	n := 0
	if d.interval > 0 {
		n, err = d.checkpointRound(ctx, &g, true)
	} else {
		n, err = d.awaitInflight(ctx, g)
	}
	if err != nil {
		d.fenceMu.Lock()
		delete(d.fences, g)
		d.fenceMu.Unlock()
		d.logger.Warn("转移前检查点失败，撤围栏（本周期不转移）", "g", g, "err", err)
		return fmt.Errorf("转移前检查点 (g=%d): %w", g, err)
	}
	d.logger.Info("转移前检查点完成，可转移领导权", "g", g, "queues", n,
		"took", time.Since(start).String())
	return nil
}

// awaitInflight 逐次复制模式下等 g 组全部队列的在途提案了结（围栏已置，
// 不会再有新变更）。返回等过的队列数；ctx 结束返回其错误。
func (d *Deliverer) awaitInflight(ctx context.Context, g uint32) (int, error) {
	d.mu.Lock()
	var qss []*queueState
	for _, qs := range d.qmu {
		if d.rt.GroupForQueue(qs.topic, qs.q) == g {
			qss = append(qss, qs)
		}
	}
	d.mu.Unlock()
	n := 0
	for _, qs := range qss {
		qs.mu.Lock()
		if qs.inFlight == 0 {
			qs.mu.Unlock()
			continue
		}
		ch := qs.settledLocked()
		qs.mu.Unlock()
		n++
		select {
		case <-ch:
		case <-ctx.Done():
			return n, ctx.Err()
		}
	}
	return n, nil
}

// fenced 组 g 当前是否处于转移围栏内。围栏到期时顺手清除并记一次 Warn
// （转移被中止，服务恢复）。
func (d *Deliverer) fenced(g uint32) bool {
	d.fenceMu.Lock()
	defer d.fenceMu.Unlock()
	until, ok := d.fences[g]
	if !ok {
		return false
	}
	if time.Now().Before(until) {
		return true
	}
	delete(d.fences, g)
	if d.rt.IsLeader(g) {
		d.logger.Warn("转移围栏到期而领导权仍在本节点（转移未发生或已中止），恢复消费服务", "g", g)
	}
	return false
}

// waitFence 围栏内的调用阻塞等待（P3）：直到围栏解除/到期、领导权已失
// （返回 ErrNotLeader，rpc 映射可重试）或 ctx 结束。无围栏时一次 map 读。
func (d *Deliverer) waitFence(ctx context.Context, g uint32) error {
	if !d.fenced(g) {
		return nil
	}
	start := time.Now()
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for d.fenced(g) {
		if !d.rt.IsLeader(g) {
			d.logger.Debug("围栏等待期间领导权已转走", "g", g, "waited", time.Since(start).String())
			return fmt.Errorf("%w: 组 %d 领导权已转移", replication.ErrNotLeader, g)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	d.logger.Debug("围栏解除，继续消费调用", "g", g, "waited", time.Since(start).String())
	return nil
}
