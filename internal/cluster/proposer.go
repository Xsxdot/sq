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
//     注意：作废只覆盖丢弃时已在队列的同任期条目，**不提供跨调用方的
//     同任期失败前缀性**（丢弃之后新入队的同任期条目可以成功）；需要
//     前缀性的调用方须自行保证（见 SQ4.1 计划修订 R1）。
//   - 合并提案（卡 SQ4.3）：proposer 每次唤醒取走全部已排队条目，把其中
//     连续的、绑同一任期、逐条通过核对的条目切成段，段长 ≥2 时用一条
//     MsgProp（rn.Step）一次交给 raft，段长 1 照走 SQ2 的 proposeOne
//     （rn.Propose）。不为攒批而等待：队列只有 1 条时立即单条提案。段内
//     顺序 = 队列顺序 = 日志顺序；每条 entry 仍各自带提案头、各有一个
//     waiter，条目格式不变。rn.Step 对 MsgProp 不回传丢弃错误，所以每批
//     紧随一次 rn.Status() 作「围栏」判断是否进了日志（承重假设 H3–H6）；
//     围栏判不确定 → 整批 ErrNotLeader 并作废该任期在途条目（同 SQ2 丢弃）。
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
//   - 合并只发生在 proposer 内部（一条入队 = 一个 raft 条目），不改提案头
//     布局、不改条目格式、不改 apply 侧；
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
//	H3「rn.Step(MsgProp) 返回 nil = node.run 已取走该消息，且其 r.Step 在
//	node.run 本轮 select 内同步执行完才进入下一轮」：Node.Step（node.go:475-487）
//	→ step → stepWithWaitOption(wait=false)（node.go:504-506、514-540）；
//	n.propc 无缓冲（node.go:314），`case ch <- pm: if !wait { return nil }`
//	即 node.run 已在 `case pm := <-propc`（node.go:386-393）接收；该分支内
//	同步 r.Step(m) 后才回到 select。wait=false 时 pm.result 为 nil，丢弃
//	错误（ErrProposalDropped）被吞——这正是要加围栏的原因。
//
//	H4「H3 之后发起的 rn.Status() 观察到的状态晚于本批处理」：Status 经
//	n.status 通道请求（node.go:575-583），由 node.run 在**后续**轮次的
//	`case c := <-n.status`（node.go:447-448）同步 getStatus(r)；本批的
//	r.Step 在本轮已完成（H3），故围栏看到的一定是本批之后的状态。
//
//	H5「一条 MsgProp 在 leader 侧要么整批 append、要么整批丢弃」：stepLeader
//	MsgProp（raft.go:1294-1352）的丢弃条件只有三条且都对整条消息生效——
//	本节点不在成员表（Progress[r.id]==nil）、leadTransferee 非空（:1304）、
//	appendEntry 超未提交上限（raft.go:812 起对全批一次
//	increaseUncommittedSize）；本仓 raftConfig 未设 MaxUncommittedEntriesSize，
//	raft.go:313-314 置 noLimit，第三条不可达。非 leader（stepFollower
//	raft.go:1718、stepCandidate :1673）一律丢弃。围栏判据由此得出：
//	「RaftState=Leader ∧ Term=本批任期 ∧ LeadTransferee=0 ∧ Progress 含本
//	节点」→ 已进日志；否则不确定（判为失败，计划 A2）。
//
//	H6「MsgProp 永不出本节点」：raftConfig DisableProposalForwarding=true，
//	stepFollower 丢弃而不转发（raft.go:1722-1724）。线上只有 MsgApp，一条
//	MsgApp 带多条 entry 本来就是常态（MaxSizePerMsg 打包），合并提案不改变
//	任何线上格式，新旧节点混跑互通。
//
// 残余窗口（计划 R1，已知不闭合）：任期核对与 rn.Propose 之间、以及 Ready
// 循环观察滞后于 node 内部状态之间存在间隙；彻底关闭需 apply 时按条目
// term 校验（改日志格式，计划 D5，不在本卡）。批路径同样存在、不放大：
// 围栏只会把「不确定」判为失败。围栏自身的两个窗口（SQ4.3 计划 R2/R3）：
// Step 与 Status 之间恰好一次 tick 中止了转移 → 误判「已进日志」，这些
// 条目等到 ctx 超时按 classifyTimeout 归类；Step 之后转移才开始 → 误判
// 「不确定」而条目实际提交，客户端重试产生重复（既有至少一次语义）。
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
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

	// 合并提案计数（只有 proposer 写；同包测试与退出日志读）：batches =
	// 走批路径（一条 MsgProp ≥2 条 entry）的段数，batchEntries = 这些段内
	// 的条目总数。二者之比即平均批长；单条路径不计入。
	batches      atomic.Uint64
	batchEntries atomic.Uint64
}

// batchProposalMaxBytes 一条合并 MsgProp 的载荷总字节上限，与 raftConfig
// 的 MaxSizePerMsg（1MiB）同值：合并段不超过 raft 单条 MsgApp 的打包上限，
// 避免一个超大批把复制拆成多轮的同时还占着 proposer。单条超限的条目自成
// 一段走单条路径（与 SQ2 行为相同）。
const batchProposalMaxBytes = 1 << 20

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
// 同样失败（出队核对绑定任期：单条走 proposeOne 第 2 步，合并批走 checkBound）。
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
	var seg []*ProposalPending // 合并段缓冲（proposeTaken 复用）
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
			// 合并统计（每组生命周期一条）：平均批长 = batch_entries / batches
			gr.lg.Info("提案 proposer 退出", "batches", q.batches.Load(), "batch_entries", q.batchEntries.Load())
			return
		case <-q.signal:
		}
		// 一次取走全部就绪条目，切段后合并提案；复用上一批的底层数组，
		// 稳态零分配
		q.mu.Lock()
		batch, q.items = q.items, batch[:0]
		if len(q.items) < proposalQueueWarnDepth {
			q.warned = false
		}
		q.mu.Unlock()
		seg = gr.proposeTaken(ctx, batch, seg, &void)
		clear(batch) // 释放引用：data 可能很大
	}
}

// proposeTaken 按合并规则处理 proposer 一次取走的条目（proposer 线程）。
//
// 切段规则（SQ4.3 计划 §2）：只有「绑任期、同一任期、逐条通过 SQ2 第 1、2
// 步核对」的**连续**条目进同一段；遇到未绑任期条目、任期不同、单条超
// batchProposalMaxBytes、或累计字节将超上限即断段；核对失败的条目就地失败
// 并断段（之后的条目另起一段）。所以交给 raft 的每条 MsgProp 内条目在
// 队列里连续，段内顺序即日志顺序。
//
// 为什么「先断段、再核对下一条」：断段即提交前一段，提交可能置作废标记
// （丢弃/围栏不确定），下一条必须在那之后核对才看得到。段非空时核对通过
// 的条目随段一起提交、失败的条目就地失败——二者都与段的命运一致（作废
// 标记只在段提交时变化；任期变化会让整段在 Step/围栏处失败）。
//
// 参数 seg 是可复用的段缓冲（proposer 私有），返回复用后的缓冲。
func (gr *group) proposeTaken(lifeCtx context.Context, taken, seg []*ProposalPending, void *voidMark) []*ProposalPending {
	seg = seg[:0]
	segBytes := 0
	var segEs *epochState
	flush := func() {
		switch len(seg) {
		case 0:
		case 1:
			gr.proposeOne(lifeCtx, seg[0], void)
		default:
			gr.proposeBatch(lifeCtx, seg, segEs, void)
		}
		clear(seg)
		seg, segBytes, segEs = seg[:0], 0, nil
	}
	for _, p := range taken {
		switch {
		case lifeCtx.Err() != nil:
			flush()
			gr.resolveProposal(p, gr.stoppedErr())
			continue
		case !p.bound, len(p.data) > batchProposalMaxBytes:
			// 未绑任期（同步入口在 leader 未知时入队）或单条超限：自成一段
			flush()
			gr.proposeOne(lifeCtx, p, void)
			continue
		case len(seg) > 0 && (p.term != seg[0].term || segBytes+len(p.data) > batchProposalMaxBytes):
			flush()
		}
		es, err := gr.checkBound(p, void)
		if err != nil {
			flush()
			gr.resolveProposal(p, err)
			continue
		}
		if len(seg) == 0 {
			segEs = es
		}
		seg = append(seg, p)
		segBytes += len(p.data)
	}
	flush()
	return seg
}

// checkBound 对一条绑任期条目做 SQ2 第 1、2 步核对（proposer 线程）：
// 条目 ctx 已取消 → 返回 ctx 错误（只失败它自己）；本节点已非 leader /
// 任期已变 / 落在作废范围 → ErrNotLeader。通过时返回当前领导权快照。
func (gr *group) checkBound(p *ProposalPending, void *voidMark) (*epochState, error) {
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	es := gr.epoch.Load()
	if !es.isSelf || es.term != p.term || void.covers(p) {
		return nil, gr.notLeaderErr()
	}
	return es, nil
}

// proposeBatch 把一段（≥2 条、同任期、已逐条通过核对）用一条 MsgProp 交给
// raft（proposer 线程）：逐条回填提案头并注册 waiter → rn.Step → 围栏
// rn.Status → 逐条交结果。
//
// 参数 es 是段首核对时的领导权快照（段内条目同任期，同一任期内本节点的
// 领导权快照唯一）；Step 的 ctx 用本任期 ctx：有 leader 时 Step 只阻塞到
// node.run 接收，ctx 只在 raft 内部已无 leader（propc 为 nil）时起作用，
// 此时本任期 ctx 会被 Ready 循环取消。条目各自的 ctx 不并入（核对时已查；
// 之后才取消的条目可能仍进日志，与 proposeOne 的同类窗口语义相同，由
// Wait 的交接 CAS 收尾）。
//
// 结果对整段生效（计划 §2 结果归类表）：Step 失败且组已停止 → 停止错误；
// Step 失败且本任期 ctx 已取消 → ErrNotLeader + 作废该任期；Step 成功且
// 围栏判已进日志 → 全段「已进日志」；围栏判不确定 → ErrNotLeader + 作废。
func (gr *group) proposeBatch(lifeCtx context.Context, seg []*ProposalPending, es *epochState, void *voidMark) {
	term := seg[0].term
	ents := make([]*raftpb.Entry, len(seg))
	ids := make([]uint64, len(seg))
	chs := make([]chan struct{}, len(seg))
	// 回填提案头（[8B 提案者 nodeID][8B waiter id]）并一次加锁注册全部 waiter
	gr.mu.Lock()
	for i, p := range seg {
		ids[i] = gr.nextID.Add(1)
		chs[i] = make(chan struct{})
		gr.propWaiters[ids[i]] = chs[i]
		binary.BigEndian.PutUint64(p.data[:8], gr.selfID)
		binary.BigEndian.PutUint64(p.data[8:16], ids[i])
		ents[i] = &raftpb.Entry{Data: p.data}
	}
	gr.mu.Unlock()
	gr.pq.batches.Add(1)
	gr.pq.batchEntries.Add(uint64(len(seg)))

	// 先查一次（同 proposeOne）：ctx 已结束时 stepWithWaitOption 的 select
	// 可能仍随机选中投递
	err := es.ctx.Err()
	if err == nil {
		err = gr.rn.Step(es.ctx, &raftpb.Message{Type: raftpb.MsgProp.Enum(), Entries: ents})
	}
	landed := false
	if err == nil {
		// 围栏（H3/H4/H5）：Step 返回 nil 只说明 node.run 收下了本批；
		// 之后的 Status 晚于本批处理，据它判断本批是否进了日志
		st := gr.rn.Status()
		_, member := st.Progress[gr.selfID]
		landed = st.RaftState == raft.StateLeader && st.GetTerm() == term &&
			st.LeadTransferee == raft.None && member
		if !landed {
			// Debug：转移窗口内每个新批都会走到这里（与 enqueueProposal 的
			// 高频拒绝同级），Info 会刷屏
			gr.lg.Debug("合并提案围栏判不确定，整批以 ErrNotLeader 失败并作废该任期在途条目",
				"count", len(seg), "term", term, "raft_state", st.RaftState.String(),
				"raft_term", st.GetTerm(), "lead_transferee", st.LeadTransferee)
		}
	}
	for _, p := range seg {
		p.data = nil // 所有权已移交 raft（appendEntry 已克隆）或提案失败，句柄不再持有
	}
	if landed {
		for i, p := range seg {
			p.id, p.waitCh = ids[i], chs[i]
			if !gr.resolveProposal(p, nil) {
				// Wait 已因条目 ctx 结束放弃：没人会等这个 waiter 了，移除防泄漏
				gr.removeWaiter(gr.propWaiters, ids[i])
			}
		}
		return
	}
	gr.mu.Lock()
	for _, id := range ids {
		delete(gr.propWaiters, id)
	}
	gr.mu.Unlock()
	var fail error
	switch {
	case err == nil:
		// 围栏判不确定（计划 A2）：与 raft 丢弃同语义
		void.mark(gr.pq, term)
		fail = gr.notLeaderErr()
	case lifeCtx.Err() != nil || errors.Is(err, raft.ErrStopped):
		fail = gr.stoppedErr()
	case es.ctx.Err() != nil:
		void.mark(gr.pq, term)
		fail = gr.notLeaderErr()
	default:
		// wait=false 的 Step 只会返回 ctx 错误或 ErrStopped，此分支理论
		// 不可达；按失去领导权保守处理（未确认进日志即不报成功）
		gr.lg.Warn("合并提案 Step 返回意外错误，整批以 ErrNotLeader 失败", "count", len(seg), "term", term, "err", err)
		void.mark(gr.pq, term)
		fail = gr.notLeaderErr()
	}
	for _, p := range seg {
		gr.resolveProposal(p, fail)
	}
}

// voidMark 作废标记：任期 term 内入队序号 ≤ upTo 的绑定条目一律失败。
// proposer 私有（只有 proposer goroutine 读写）。
//
// 作废只覆盖「丢弃那一刻已在队列」的同任期条目，不提供跨调用方的同任期
// 失败前缀性（之后新入队的同任期条目照常提案、可以成功）；需要前缀性的
// 调用方须自行保证（见 SQ4.1 计划修订 R1）。
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
