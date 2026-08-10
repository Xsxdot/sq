// Package cluster 的读屏障实现：raft read-index + apply barrier。
//
// 职责：
//   - 向 raft 索取一个 readIndex（ReadOnlySafe 档：leader 已在当前任期
//     提交过条目 + 刚被多数派心跳确认仍是 leader）
//   - 等本节点 applied 追过该 index 后放行，此时的本地读一定包含了读
//     请求发起之前已确认的全部写（线性一致读）
//   - 把并发读者按「排队成批」合流，一轮 read-index 服务一批等待者
//
// 边界：
//   - 只服务 leader：非 leader 直接返回 ErrNotLeader，不做 follower 读
//     转发（raft 内核会把 MsgReadIndex 转给 leader，但 sq 的读路径目前
//     全是 leader-only，follower 读属 V3 议题）
//   - 不碰 FSM：屏障只负责「什么时候可以读」，读什么由调用方决定
//   - 不做超时策略：超时由调用方的 ctx 承担（配置项在 Manager 层）
package cluster

import (
	"context"
	"encoding/binary"
	"time"

	"go.etcd.io/raft/v3"
)

// readWait 一个在等的读屏障等待者。
//
// index 在读状态回流之前是 0：0 表示「readIndex 还没回来」，notifyRead
// Waiters 必须跳过它——否则 applied 恰好非零就会把还没拿到 readIndex 的
// 等待者提前放行，屏障退化成一次空等待。
type readWait struct {
	index uint64
	ch    chan struct{}
}

// readIndexOnce 走一轮完整的 read-index：索取 readIndex → 等 applied 追平。
//
// 参数：
//   - ctx: 控制整轮等待；超时/取消时摘除 waiter 防泄漏
//
// 返回：
//   - nil：本节点 applied 已追过 readIndex，可以安全读
//   - ErrNotLeader：本节点已知不是 leader（读屏障只在 leader 成立）
//   - ctx.Err()：超时或取消
//
// 注意：
//   - 与 propose 同款的快速失败：已探明 leader 是他人就不进 raft，省掉
//     一次必然超时的等待
//   - RequestCtx 复用提案头布局 [8B 提案者][8B waiter id]，跨节点 id
//     撞车由提案者校验挡住（见 readWaiterInfo）
func (gr *group) readIndexOnce(ctx context.Context) error {
	if lead := gr.leader(); lead != gr.selfID {
		gr.lg.Debug("读屏障被拒：本节点非 leader", "lead", lead)
		return gr.notLeaderErr()
	}
	id := gr.nextID.Add(1)
	w := &readWait{ch: make(chan struct{})}
	gr.mu.Lock()
	gr.readWaiters[id] = w
	gr.mu.Unlock()

	rctx := make([]byte, 16)
	binary.BigEndian.PutUint64(rctx[:8], gr.selfID)
	binary.BigEndian.PutUint64(rctx[8:], id)

	start := time.Now()
	if err := gr.rn.ReadIndex(ctx, rctx); err != nil {
		gr.removeReadWaiter(id)
		gr.lg.Warn("read-index 提交失败", "id", id, "err", err)
		return err
	}
	select {
	case <-w.ch:
		gr.lg.Debug("读屏障放行", "id", id, "read_index", w.index,
			"applied", gr.applied.Load(), "cost", time.Since(start))
		return nil
	case <-ctx.Done():
		gr.removeReadWaiter(id)
		// 等待期间领导权已经易主：读屏障在本节点上再也不会满足，归入
		// ErrNotLeader 让调用方换节点重试，而不是当成不可重试的超时
		if cur := gr.leader(); cur != gr.selfID {
			gr.lg.Debug("读屏障等待期间失去领导权", "id", id, "lead", cur)
			return gr.notLeaderErr()
		}
		gr.lg.Warn("读屏障等待超时", "id", id, "read_index", w.index,
			"applied", gr.applied.Load(), "cost", time.Since(start), "err", ctx.Err())
		return ctx.Err()
	}
}

// stepReadStates 消费本轮 Ready 回流的读状态：填 index，若 applied 已经
// 追过就地放行。在 handleReady 的 apply 之前调用——本轮 apply 之后还会
// 再调一次 notifyReadWaiters，两处配合覆盖「回流早于 apply」与「回流晚于
// apply」两种顺序。
func (gr *group) stepReadStates(rss []raft.ReadState) {
	if len(rss) == 0 {
		return
	}
	applied := gr.applied.Load()
	gr.mu.Lock()
	defer gr.mu.Unlock()
	for _, rs := range rss {
		id, ok := readWaiterInfo(rs.RequestCtx, gr.selfID)
		if !ok {
			// 别的节点发起的读状态（raft 会把 follower 的 MsgReadIndex
			// 转给 leader，leader 侧因此会看到非本节点的 RequestCtx）：
			// 本节点没有对应等待者，跳过即可，不是异常
			continue
		}
		w, ok := gr.readWaiters[id]
		if !ok {
			// 等待者已超时摘除：读状态迟到，丢弃
			continue
		}
		w.index = rs.Index
		if applied >= w.index {
			close(w.ch)
			delete(gr.readWaiters, id)
		}
	}
}

// notifyReadWaiters 放行所有 readIndex 已被 applied 追过的等待者。
// 在 handleReady 的 apply 循环之后调用（applied 刚被推进）。
func (gr *group) notifyReadWaiters(applied uint64) {
	gr.mu.Lock()
	defer gr.mu.Unlock()
	for id, w := range gr.readWaiters {
		// index==0 表示 readIndex 还没回流：此时放行等于没有屏障
		if w.index == 0 || applied < w.index {
			continue
		}
		close(w.ch)
		delete(gr.readWaiters, id)
	}
}

// removeReadWaiter 摘除等待者但不关闭 channel（调用方已放弃等待）。
func (gr *group) removeReadWaiter(id uint64) {
	gr.mu.Lock()
	delete(gr.readWaiters, id)
	gr.mu.Unlock()
}

// readWaiterInfo 从 ReadState.RequestCtx 解出 waiter id。
//
// 返回：
//   - id: waiter id
//   - ok: 长度足够且提案者是本节点时为 true
//
// 注意：ok=false 时绝不可唤醒本地等待者——waiter id 是每节点独立计数器，
// 跨节点必然撞车，裸 id 放行会把别节点的读状态当成自己的屏障满足。
func readWaiterInfo(rctx []byte, self uint64) (id uint64, ok bool) {
	if len(rctx) < 16 {
		return 0, false
	}
	if binary.BigEndian.Uint64(rctx[:8]) != self {
		return 0, false
	}
	return binary.BigEndian.Uint64(rctx[8:16]), true
}

// barrierBatch 一批共享同一轮 read-index 的等待者。
//
// n 只用于测试观察批次规模；生产路径不读它。lastArrival 是最后一名
// 成员入队的时刻——收割（claimBatch）要等它老化满 barrierQuiesce，
// 才能把「同一瞬间到达的并发读者」并进同一批。
type barrierBatch struct {
	done        chan struct{}
	err         error
	n           int
	lastArrival time.Time
}

// barrierQuiesce 是批次收割前的静默窗：最后一名成员入队后再等该时长
// 无人加入，即视为「这一批到齐」，开始跑 read-index。
//
// 为什么需要静默窗：若不等待，收割 goroutine 会在第一个成员入队后立刻
// 把批取走，同一瞬间到达的其余并发读者会被甩进下一批——批次成员变成
// 调度竞争的产物，而不是「同一波到达」。窗的存在让「一批的 read-index
// 调用发生在这批所有成员都已入队之后」从纸面变成可保证。
//
// 代价：每轮 read-index 多等至多 barrierQuiesce（1ms）。读屏障的一轮
// 本来就要等一次多数派心跳往返（百毫秒~秒级），1ms 静默窗完全可忽略。
const barrierQuiesce = time.Millisecond

// readBarrier 等一次读屏障，并发调用按「排队成批」合流。
//
// 参数：
//   - ctx: 本调用方的等待预算；超时只影响本调用方，不影响这一批的其他人
//
// 返回：
//   - nil：可以安全读
//   - ErrNotLeader / ctx.Err() / raft 错误：见 readIndexOnce
//
// 注意：
//   - 一轮在途时到达的等待者排进下一批（见 brRunning 字段注释的红线说明）
//   - 每轮用独立的 barrierTimeout 计时，父 ctx 是组的 lifeCtx——某个调用方
//     放弃不该让整批失败
func (gr *group) readBarrier(ctx context.Context) error {
	gr.brMu.Lock()
	if gr.brNext == nil {
		gr.brNext = &barrierBatch{done: make(chan struct{})}
	}
	b := gr.brNext
	b.n++
	b.lastArrival = time.Now()
	if !gr.brRunning {
		gr.brRunning = true
		go gr.runBarrierRounds()
	}
	gr.brMu.Unlock()

	select {
	case <-b.done:
		return b.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runBarrierRounds 串行驱动读屏障轮次：取走当前排队批 → 跑一轮
// read-index → 唤醒该批 → 若期间又排了新批就继续，否则收摊。
//
// 串行是设计而非限制：在途轮次恒为 1，读 QPS 再高也只有一条 read-index
// 流；等待者数无上限，代价只是多等一轮。
func (gr *group) runBarrierRounds() {
	for {
		b := gr.claimBatch()
		if b == nil {
			gr.brMu.Lock()
			gr.brRunning = false
			gr.brMu.Unlock()
			return
		}

		parent := gr.lifeCtx
		if parent == nil {
			parent = context.Background()
		}
		ctx, cancel := context.WithTimeout(parent, gr.barrierTimeout)
		b.err = gr.readIndexFn(ctx)
		cancel()
		if b.err != nil {
			gr.lg.Warn("读屏障一轮失败", "waiters", b.n, "err", b.err)
		} else {
			gr.lg.Debug("读屏障一轮放行", "waiters", b.n)
		}
		close(b.done)
	}
}

// claimBatch 收割当前排队批；无批可收返回 nil。
//
// 收割前等静默窗（barrierQuiesce）：同一瞬间到达的并发读者若在窗内
// 入队则并入本批——这是「一批的 read-index 调用发生在这批所有成员都
// 已入队之后」的实现机制（见 barrierQuiesce 注释）。
func (gr *group) claimBatch() *barrierBatch {
	for {
		gr.brMu.Lock()
		b := gr.brNext
		if b == nil {
			gr.brMu.Unlock()
			return nil
		}
		idle := time.Since(b.lastArrival)
		if idle < barrierQuiesce {
			gr.brMu.Unlock()
			// 睡到静默窗走完再回来收割；期间新成员入队会刷新
			// lastArrival，重锁后 idle 重新计时，自然并入本批
			time.Sleep(barrierQuiesce - idle)
			continue
		}
		gr.brNext = nil
		gr.brMu.Unlock()
		return b
	}
}

// batchSize 返回当前排队批的等待者数（测试观察用；无排队批时返回 0）。
func (gr *group) batchSize() int {
	gr.brMu.Lock()
	defer gr.brMu.Unlock()
	if gr.brNext == nil {
		return 0
	}
	return gr.brNext.n
}
