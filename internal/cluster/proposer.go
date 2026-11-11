// proposer.go 每组一条保序提案队列：同组提案按调用顺序进入 raft 日志，
// 且入队时绑定的 leader 任期在出队提案前被核对（卡 SQ2）。
//
// 职责：
//   - 提案队列（proposalQueue）：入队在调用方线程完成、非阻塞 O(1)；
//     每组一个 proposer goroutine（随 group.run 启停）严格 FIFO 逐条
//     rn.Propose。调用方 A 的入队返回先于 B 的入队发起，且二者都进了
//     日志，则 A 的 index < B 的——这是 replication.Replicator.ApplyAsync
//     「返回即在同组内定序」契约在集群档的兑现点。
//   - 任期绑定：入队时本节点是 leader 则把条目绑定到 Ready 循环观察到的
//     raft 任期号；proposer 出队时若「本节点已非 leader / 任期已变 / 该
//     任期已作废」则以 ErrNotLeader 失败、绝不提案；提案 ctx 并上「本任期
//     ctx」，失去领导权即取消。某条提案被 raft 丢弃后，丢弃那一刻已在
//     队列里的同任期后续条目一律失败（作废标记）——旧任期构造的批次不进
//     新任期日志（SQ1 计划 F5）。
//     作废只覆盖「丢弃时已在途」的条目，不株连之后新入队的同任期条目：
//     raft 也会在 leader 不换届时丢弃提案（leadTransferee 非空的转移窗口，
//     转移超时作罢后本节点仍是同任期 leader），若整个任期作废，本组在
//     下次换届前所有写都会失败——活性故障。同任期内「N 未进日志、N+1
//     成功」本身是安全的（计划 §4.2 失败语义论证）。
//   - 任期读取入口 leaderTerm()：Ready 循环观察到的当前任期（SQ1 复用）。
//   - 带期望 epoch 的异步入口 enqueueProposalAt（卡 SQ1 计划修订 R1）：任期
//     绑定取调用方给定的 epoch（它过可写门时拿到的那一个），而不是入队那一
//     刻的任期——调用方据该 epoch 读计数器构造批次，门放行后、入队前若本节点
//     失主又以更高任期复任，按入队时刻绑定会让旧 epoch 构造的批次进新任期
//     日志、覆盖期间他人已确认的写。入队时 epoch 不符即失败，出队核对沿用
//     绑定任期的同一条规则。原 enqueueProposal 语义不变。
//   - ProposalPending：一条提案的等待句柄（Wait 恰好调用一次）。
//
// 边界：
//   - 不合并提案（一条入队 = 一个 raft 条目），不改提案头布局；
//   - proposer 不等 apply：只把「已进日志 / 失败」交给等待方，等 apply
//     在调用方的 Wait 里（否则 proposer 串行等 quorum，吞吐退化为 1/RTT）；
//   - 热路径零日志：只有队列深度越过 proposalQueueWarnDepth 时记一条 Warn。
//
// 承重假设（实现前已对照 go.etcd.io/raft/v3@v3.7.0 源码钉死）：
//
//	H1「rn.Propose 同步返回 nil = 条目已按调用顺序追加进 leader 的未稳定
//	日志」：node.Propose（node.go:471-473）走 stepWait → stepWithWaitOption
//	（node.go:514 起）wait=true：消息经 n.propc 投递后阻塞等 pm.result；
//	node.run 单 goroutine 在 `case pm := <-propc`（node.go:386-393）里同步
//	r.Step(m) 后才回写 result；leader 的 stepLeader MsgProp 分支
//	（raft.go:1294-1352）在返回 nil 前同步 r.appendEntry（raft.go:812-834，
//	按 lastIndex+1 起编号）。因此「Propose 先返回者 index 更小」，前提是
//	两次 Propose 不并发——本文件的单 proposer goroutine 保证这一点。
//	propc 在无 leader / 不在成员表时被置 nil（node.go:374/:377/:426），
//	此时 Propose 阻塞到 leader 产生或 ctx 取消。
//
//	H2「Ready 循环看到 SoftState.Lead=self 时，本轮或更早已带出该任期的
//	HardState」：rawnode.go:148-153 在 readyWithoutAccept 内对同一 raft
//	状态计算 SoftState 与 HardState，HardState 只要与上次 accept 的不同即
//	填充，且这段不受 asyncStorageWrites 分支（:163 之后）影响；becomeCandidate
//	（raft.go:908 reset(r.Term+1)）先于 becomeLeader（raft.go:939 reset(r.Term)，
//	不再抬任期）抬任期，所以当选任期的 HardState 要么在候选那轮、要么在
//	当选这轮 Ready 里。proposer_test 断言「当选后 leaderTerm()==Status().Term」。
//
// 残余窗口（计划 R1，已知不闭合）：任期核对与 rn.Propose 之间、以及 Ready
// 循环观察滞后于 node 内部状态之间存在间隙；彻底关闭需 apply 时按条目
// term 校验（改日志格式，计划 D5，不在本卡）。
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.etcd.io/raft/v3"
)

// proposalQueueWarnDepth 队列深度越过它时记一条 Warn（只在越过的那一刻
// 记一次，回落后再越过再记）。4096：调用方每入队一条随后都 Wait，深度
// 天然受并发调用方数约束，稳态远小于此；越过说明 proposer 被阻塞（选举
// 窗口内同步入口的未绑任期条目在队头等 leader）或调用方并发异常。
const proposalQueueWarnDepth = 4096

// 等待句柄的状态机（proposer 与 Wait 之间的交接，CAS 推进）：
//   - pendingQueued   → pendingResolved：proposer 已处理完（成功进日志或失败）
//   - pendingQueued   → pendingAbandoned：Wait 因条目 ctx 结束先行放弃
//
// 两者只有一个能赢；proposer 输了（Wait 已放弃）时负责移除已注册的 waiter，
// 防止 propWaiters 泄漏。
const (
	pendingQueued int32 = iota
	pendingResolved
	pendingAbandoned
)

// epochState 是「本节点对本组的领导权快照」：入队与出队核对读同一个
// 不可变快照，保证「是否 leader」与「任期号」一致（不会读到新任期 + 旧
// 身份的混合态）。
type epochState struct {
	term   uint64
	isSelf bool
	// ctx 是本任期 ctx（仅 isSelf 时非 nil）：领导权变化时由 Ready 循环
	// 取消，在途 rn.Propose 随之解阻塞。父 ctx 为组生命周期 ctx。
	ctx    context.Context
	cancel context.CancelFunc
}

// proposalQueue 每组一条 FIFO 提案队列（零值不可用，由 newGroup 初始化）。
type proposalQueue struct {
	mu     sync.Mutex
	items  []*ProposalPending
	closed bool   // proposer 已退出：此后入队立即失败
	seq    uint64 // 入队序号（单调递增），作废范围的上界依据
	warned bool   // 已为本次越过阈值记过 Warn
	// signal 容量 1：入队后非阻塞投递一次唤醒，proposer 醒来一次取走全部
	signal chan struct{}
}

// ProposalPending 一条已入队提案的等待句柄。
//
// Wait 恰好调用一次（与 replication.Pending / store.Pending 同语义）：
// 返回 nil = 条目已在本节点 apply（读己之写）；否则为失败原因。
type ProposalPending struct {
	gr   *group
	ctx  context.Context
	data []byte

	seq   uint64 // 入队序号（proposalQueue.seq），作废判定用
	bound bool   // 是否绑定任期（入队时本节点是 leader）
	term  uint64 // 绑定的任期号（bound 时有效）
	// leadAtEntry 入队时观察到的 leader：等待超时的错误归类基线（与原
	// propose 同语义——期间 leader 换了人则归 ErrNotLeader）
	leadAtEntry uint64

	state    atomic.Int32
	resolved chan struct{} // proposer 处理完本条后关闭（失败与否）
	err      error         // proposer 写入，resolved 关闭后可读
	id       uint64        // 已注册的 waiter id（err==nil 时有效）
	waitCh   chan struct{} // waiter 通道（err==nil 时有效）
}

// failedPending 返回一个已失败的等待句柄（入队期失败经 Wait 报，计划 D4）。
func failedPending(err error) *ProposalPending {
	p := &ProposalPending{err: err, resolved: make(chan struct{})}
	p.state.Store(pendingResolved)
	close(p.resolved)
	return p
}

// newProposalQueue 构造空队列。
func newProposalQueue() *proposalQueue {
	return &proposalQueue{signal: make(chan struct{}, 1)}
}

// leaderTerm 返回 Ready 循环已观察到的当前 raft 任期号（未观察到任何
// HardState 前为 0）。
//
// 与 currentTerm()（lastTerm）的区别：lastTerm 在 append 阶段 persistPhase
// 才更新，滞后于 SoftState（当选那一轮可能仍是旧值）；本方法在 dispatchReady
// 处理 SoftState **之前**从 rd.HardState 更新，钩子与入队看到的都是当选任期。
// 任期绑定与 SQ1 的「可写 epoch」都必须读它，不得另建第二个任期缓存。
func (gr *group) leaderTerm() uint64 {
	return gr.obsTerm.Load()
}

// observeTerm 在 dispatchReady 处理 SoftState 之前调用：从非空 HardState
// 更新任期缓存（只读，不消费——async 下持久化仍走本地消息）。
//
// 本轮没有 SoftState 变化而任期变了、且本节点仍自认 leader（SoftState 被
// 合并：自身→失去→自身在同一 Ready 之间完成，见计划 R1 注）：立即切换
// 到新任期，旧任期在途条目随之失败。有 SoftState 的轮次由 switchEpoch
// 在钩子之后统一切换，这里不动——否则下台那一轮会短暂出现「新任期 +
// 旧 leader 身份」的快照。
func (gr *group) observeTerm(lifeCtx context.Context, rd *raft.Ready) {
	if rd.HardState != nil {
		if t := rd.HardState.GetTerm(); t != 0 && t != gr.obsTerm.Load() {
			gr.obsTerm.Store(t)
		}
	}
	if rd.SoftState == nil {
		if es := gr.epoch.Load(); es.isSelf && es.term != gr.obsTerm.Load() {
			gr.switchEpoch(lifeCtx, true)
		}
	}
}

// switchEpoch 把领导权快照切换到（当前观察任期, isSelf）：取消旧任期 ctx，
// 是 leader 时新建本任期 ctx。只在 Ready 循环线程调用；SoftState 变化时
// 必须在钩子之后、lead.Store 之前调用（失去领导权时任期 ctx 取消先于撤销
// 对外可见；当选时任期快照先于 leader 身份对外可见）。
func (gr *group) switchEpoch(lifeCtx context.Context, isSelf bool) {
	term := gr.obsTerm.Load()
	old := gr.epoch.Load()
	if old.term == term && old.isSelf == isSelf {
		return
	}
	next := &epochState{term: term, isSelf: isSelf}
	if isSelf {
		next.ctx, next.cancel = context.WithCancel(lifeCtx)
	}
	gr.epoch.Store(next)
	if old.cancel != nil {
		old.cancel()
	}
}

// enqueueProposal 把一条提案入本组队列并返回等待句柄（非阻塞）。
//
// 参数：
//   - ctx: 条目 ctx（出队前已取消则不提案；并入 rn.Propose 的 ctx；Wait 的等待预算）
//   - data: 单拷贝契约缓冲（前 ProposalHeaderLen 字节由 proposer 回填）
//   - sync: true = 同步形态（Apply/Propose）：leader 未知时入队、不绑任期，
//     等 leader 产生后再提案；false = 异步形态（ApplyAsync）：leader 未知
//     直接失败 ErrNotLeader（计划 D1）
//
// 返回：等待句柄；入队期失败（组已停止 / 已知他人是 leader / 异步形态
// leader 未知 / 缓冲缺保留头）也经 Wait 报（计划 D4）。
func (gr *group) enqueueProposal(ctx context.Context, data []byte, sync bool) *ProposalPending {
	// 保留头缺失属编程错误（调用方没按单拷贝契约构造缓冲），fail-stop
	// 直接报错，绝不能把载荷前 16B 当头覆盖掉。
	if len(data) < ProposalHeaderLen {
		return failedPending(fmt.Errorf("cluster: 提案缓冲长度 %d 小于保留头 %d（调用方未按单拷贝契约预留）",
			len(data), ProposalHeaderLen))
	}
	p := &ProposalPending{gr: gr, ctx: ctx, data: data, resolved: make(chan struct{})}
	lead := gr.leader()
	p.leadAtEntry = lead
	if es := gr.epoch.Load(); es.isSelf {
		p.bound, p.term = true, es.term
	} else if lead != 0 && lead != gr.selfID {
		// 已探明 leader 是他人（follower 提案在 DisableProposalForwarding
		// 下必被丢弃）：快速失败让客户端经 Leader(g) 重试。高频（客户端
		// 错发是常态）记 Debug。
		gr.lg.Debug("提案被拒：本节点非 leader", "lead", lead)
		return failedPending(gr.notLeaderErr())
	} else if !sync {
		// leader 未知（选举窗口）的异步形态：调用方是持队列锁的
		// leader-only 构造，用旧任期计数器构造的批次不能等到下一任期再进
		// 日志（计划 D1），直接失败。
		return failedPending(fmt.Errorf("%w: 组 %d 当前无 leader（选举未完成）", ErrNotLeader, gr.g))
	}
	return gr.pushProposal(p)
}

// enqueueProposalAt 异步形态入队，任期绑定为调用方给定的 expected（非阻塞）。
//
// 参数：
//   - ctx/data: 同 enqueueProposal
//   - expected: 调用方过可写门（waitWritable）时拿到的 epoch；其构造批次所读
//     的计数器缓存属于该任期
//
// 返回：等待句柄。本节点非 leader、或当前任期 != expected（换届已发生：
// 过期或超前）时入队即失败 ErrNotLeader，经 Wait 报；入队后出队前任期变化
// 同样失败（proposeOne 第 2 步核对绑定任期）。
func (gr *group) enqueueProposalAt(ctx context.Context, data []byte, expected uint64) *ProposalPending {
	if len(data) < ProposalHeaderLen {
		return failedPending(fmt.Errorf("cluster: 提案缓冲长度 %d 小于保留头 %d（调用方未按单拷贝契约预留）",
			len(data), ProposalHeaderLen))
	}
	es := gr.epoch.Load()
	if !es.isSelf || es.term != expected {
		// 换届后的旧 epoch 提案属预期中的可重试失败（客户端重试即拿到新
		// epoch），记 Debug
		gr.lg.Debug("提案被拒：期望 epoch 与当前任期不符", "expected", expected,
			"term", es.term, "is_self", es.isSelf)
		return failedPending(fmt.Errorf("%w: 组 %d 期望任期 %d，当前任期 %d（leader=%v）",
			ErrNotLeader, gr.g, expected, es.term, es.isSelf))
	}
	p := &ProposalPending{gr: gr, ctx: ctx, data: data, resolved: make(chan struct{}),
		leadAtEntry: gr.leader(), bound: true, term: expected}
	return gr.pushProposal(p)
}

// pushProposal 把已完成任期判定的条目追加进本组队列并唤醒 proposer。
func (gr *group) pushProposal(p *ProposalPending) *ProposalPending {
	q := gr.pq
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return failedPending(gr.stoppedErr())
	}
	q.seq++
	p.seq = q.seq
	q.items = append(q.items, p)
	depth := len(q.items)
	warn := false
	if depth >= proposalQueueWarnDepth && !q.warned {
		q.warned, warn = true, true
	}
	q.mu.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
	if warn {
		gr.lg.Warn("提案队列积压越过阈值（proposer 被阻塞或调用方并发异常）",
			"depth", depth, "threshold", proposalQueueWarnDepth)
	}
	return p
}

// stoppedErr 组已停止的错误（包装 ErrNotLeader：客户端可换节点重试）。
func (gr *group) stoppedErr() error {
	return fmt.Errorf("%w: 组 %d 已停止", ErrNotLeader, gr.g)
}

// runProposer 是本组的 proposer 循环：严格 FIFO 逐条处理队列，直至组
// 生命周期 ctx 取消；退出时关闭队列并让全部未处理条目以停止错误失败。
//
// 由 group.run 纳入 stages WaitGroup 启动，与 Ready 循环同生共死。
func (gr *group) runProposer(ctx context.Context) {
	q := gr.pq
	var void voidMark // 作废标记（某条绑定条目被 raft 丢弃后置位）
	var batch []*ProposalPending
	for {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.closed = true
			rest := q.items
			q.items = nil
			q.mu.Unlock()
			for _, p := range rest {
				gr.resolveProposal(p, gr.stoppedErr())
			}
			if len(rest) > 0 {
				gr.lg.Info("提案队列随组退出，未处理条目以停止错误失败", "count", len(rest))
			}
			return
		case <-q.signal:
		}
		// 一次取走全部就绪条目再逐条提案（为后续合并卡留口子）；复用
		// 上一批的底层数组，稳态零分配
		q.mu.Lock()
		batch, q.items = q.items, batch[:0]
		if len(q.items) < proposalQueueWarnDepth {
			q.warned = false
		}
		q.mu.Unlock()
		for i, p := range batch {
			if ctx.Err() != nil {
				gr.resolveProposal(p, gr.stoppedErr())
			} else {
				gr.proposeOne(ctx, p, &void)
			}
			batch[i] = nil // 释放引用：data 可能很大
		}
	}
}

// voidMark 作废标记：任期 term 内入队序号 ≤ upTo 的绑定条目一律失败。
// proposer 私有（只有 proposer goroutine 读写）。
type voidMark struct {
	term uint64
	upTo uint64
}

// covers 返回条目是否落在作废范围内。
func (v *voidMark) covers(p *ProposalPending) bool {
	return p.bound && v.upTo != 0 && p.term == v.term && p.seq <= v.upTo
}

// mark 把「任期 term、截至当前已入队的全部条目」标记作废。
func (v *voidMark) mark(q *proposalQueue, term uint64) {
	q.mu.Lock()
	v.term, v.upTo = term, q.seq
	q.mu.Unlock()
}

// proposeOne 处理队头一条（proposer 线程）：核对 → 回填头 + 注册 waiter
// → rn.Propose → 结果交给等待方。
func (gr *group) proposeOne(lifeCtx context.Context, p *ProposalPending, void *voidMark) {
	// 1. 条目 ctx 已取消：不提案（单条非领导权失败，不株连后续条目）
	if err := p.ctx.Err(); err != nil {
		gr.resolveProposal(p, err)
		return
	}
	// 2. 绑任期条目：领导权或任期已变、或该任期已作废 → 不提案
	var es *epochState
	if p.bound {
		es = gr.epoch.Load()
		if !es.isSelf || es.term != p.term || void.covers(p) {
			gr.resolveProposal(p, gr.notLeaderErr())
			return
		}
	}
	// 3. 回填提案头（终审 R4 布局：[8B 提案者 nodeID][8B waiter id]，只有
	//    提案者为本节点的条目在 apply 时唤醒 waiter）并注册 waiter
	id := gr.nextID.Add(1)
	ch := make(chan struct{})
	gr.mu.Lock()
	gr.propWaiters[id] = ch
	gr.mu.Unlock()
	binary.BigEndian.PutUint64(p.data[:8], gr.selfID)
	binary.BigEndian.PutUint64(p.data[8:16], id)

	// 提案 ctx = 条目 ctx ∧ 组生命周期 ctx（∧ 绑任期条目的本任期 ctx；
	// 本任期 ctx 是生命周期 ctx 的子 ctx，并上它即同时并上生命周期）
	guard := lifeCtx
	if es != nil {
		guard = es.ctx
	}
	pctx, stop := mergeCtx(p.ctx, guard)
	// 先查一次：ctx 已结束时 node.stepWithWaitOption 的 select 在「投递
	// propc」与「ctx.Done」之间随机选，可能仍把条目送进日志——收窄核对
	// 与提案之间的间隙（R1 残余窗口之外不再引入新的随机性）。
	err := pctx.Err()
	if err == nil {
		err = gr.rn.Propose(pctx, p.data)
	}
	stop()
	p.data = nil // 所有权已移交 raft（或提案失败），句柄不再持有
	if err != nil {
		gr.removeWaiter(gr.propWaiters, id)
		// 4. 失败归类：raft 丢弃或本任期 ctx 取消（失去领导权）→ ErrNotLeader，
		//    绑任期条目把该任期作废；组停止 → 停止错误；其余（条目 ctx）原样
		switch {
		case errors.Is(err, raft.ErrProposalDropped):
			if p.bound {
				void.mark(gr.pq, p.term)
			}
			err = gr.notLeaderErr()
		case lifeCtx.Err() != nil:
			err = gr.stoppedErr()
		case p.ctx.Err() == nil && es != nil && es.ctx.Err() != nil:
			void.mark(gr.pq, p.term)
			err = gr.notLeaderErr()
		}
		gr.resolveProposal(p, err)
		return
	}
	// 5. 已进日志：只交「已进日志」，不等 apply
	p.id, p.waitCh = id, ch
	if !gr.resolveProposal(p, nil) {
		// Wait 已因条目 ctx 结束放弃：没人会等这个 waiter 了，移除防泄漏
		gr.removeWaiter(gr.propWaiters, id)
	}
}

// resolveProposal 写入处理结果并唤醒等待方；返回 false 表示等待方已放弃
// （结果无人接收）。
func (gr *group) resolveProposal(p *ProposalPending, err error) bool {
	p.err = err
	if !p.state.CompareAndSwap(pendingQueued, pendingResolved) {
		return false
	}
	close(p.resolved)
	return true
}

// mergeCtx 返回「a 或 b 任一结束即结束」的 ctx（值取自 a）。b 不会先于
// a 被取消且 a 不可取消时直接返回 b，省掉热路径的分配。
//
// 返回的 stop 必须调用（释放 AfterFunc 注册与子 ctx）。
func mergeCtx(a, b context.Context) (context.Context, func()) {
	if a.Done() == nil {
		return b, func() {}
	}
	ctx, cancel := context.WithCancel(a)
	unhook := context.AfterFunc(b, cancel)
	return ctx, func() { unhook(); cancel() }
}

// Wait 阻塞到提案在本节点 apply 完成或失败（恰好调用一次）。
//
// 两段：先等 proposer 处理完本条（失败即返回该错误）；再等 waiter /
// 条目 ctx / 组退出。超时归类与原 propose 一致：等待期间 leader 已换成
// 他人 → ErrNotLeader（未提交条目会被新 leader 截断），否则 ctx.Err()。
// 组已退出且 waiter 未就绪 → 停止错误（先查 waiter：已 apply 的不得因
// 停机报失败）。
//
// 返回：nil = 已 apply；否则失败原因。
func (p *ProposalPending) Wait() error {
	if p.gr == nil { // failedPending：入队期失败
		return p.err
	}
	gr := p.gr
	select {
	case <-p.resolved:
	case <-p.ctx.Done():
		if p.state.CompareAndSwap(pendingQueued, pendingAbandoned) {
			// proposer 尚未处理本条：放弃等待。proposer 之后会在第 1 步
			// 看到 ctx 已取消而不提案；若它恰在提案中，结果无人接收、
			// waiter 由它移除（条目可能仍进日志，语义同原 propose 的超时）
			return gr.classifyTimeout(p.leadAtEntry, p.ctx.Err())
		}
		<-p.resolved // proposer 已赢得交接，结果马上就绪
	}
	if p.err != nil {
		return p.err
	}
	select {
	case <-p.waitCh:
		return nil
	case <-p.ctx.Done():
		gr.removeWaiter(gr.propWaiters, p.id)
		return gr.classifyTimeout(p.leadAtEntry, p.ctx.Err())
	case <-gr.doneCh:
		select {
		case <-p.waitCh:
			return nil
		default:
		}
		gr.removeWaiter(gr.propWaiters, p.id)
		return gr.stoppedErr()
	}
}

// classifyTimeout 等待超时/取消的错误归类（与原 propose 同语义）：等待
// 期间 leader 已变更为他人 → ErrNotLeader（提案已丢或将被截断，客户端
// 可重试）；leader 未变（如仍是我方但无 quorum）→ 条目仍可能提交，保持
// ctx.Err()。
func (gr *group) classifyTimeout(leadAtEntry uint64, ctxErr error) error {
	if cur := gr.leader(); cur != leadAtEntry && cur != gr.selfID {
		return gr.notLeaderErr()
	}
	gr.lg.Debug("提案等待超时", "err", ctxErr)
	return ctxErr
}
