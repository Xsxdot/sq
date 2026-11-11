// writegate.go 每组一道「可写门」：新 leader 在 apply 追平本任期首条之前，
// 不得基于本地 FSM 做 leader-only 构造（卡 SQ1）。
//
// 问题：raft 选出的新 leader 日志一定包含全部已提交条目，但本地 FSM
// （store）只反映「已 apply」的部分。当选时 applied 可能落后于上任
// leader 已确认的写——此刻读 alloc/ 计数器分配 offset，会与「已提交未
// apply」的尾部重叠，覆盖已确认消息。
//
// 门判定（三条全满足才可写，任一不满足即关）：
//  1. 本节点是本组 leader（epoch.isSelf 且 lead==self）；
//  2. 已识别本任期首条 index（termStart）：becomeLeader 在同一次 Step 内
//     追加一条本任期空条目（raft/v3@v3.7.0 raft.go:958-962），leader 只追加
//     本任期条目——本轮 MsgStorageAppend 中 Term 等于当前任期的第一条即是；
//  3. applied ≥ termStart：条目按 index 顺序 apply，termStart 之前的全部
//     条目（含上任期已提交的尾部）都已进 FSM。
//
// 门带 epoch（= 当前任期号，取自 leaderTerm 同源的 epoch 快照）：调用方据此
// 判断计数器缓存是否属于本任期（任期变了缓存必须从盘重读）。
//
// 识别规则用「Term == 当前任期」而非「Term 变大」：合并 Ready 里可能先
// 出现他人旧任期条目，不能误认。即便取到的不是首条而是本任期后续某条，
// 其 index 只会更大——门多关一会儿，不会误开。
//
// 职责：门状态（gateState）的切换、可写判定、等待者的挂起与唤醒。
// 边界：只读 raft 已给出的事实（SoftState/epoch、本轮 append 条目的
// term/index、applied 位点）；不碰提案路径（proposer.go），不改 IsLeader
// 语义——IsLeader 仍是「路由/快速失败」用的 leader 身份，门是「leader-only
// 构造」用的可写性。
package cluster

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/raft/v3"
)

// defaultWritableWait 是 waitWritable 的默认等待上限（Options.WritableWaitTimeout
// 为 0 时）。3s：常见 apply 积压（毫秒～百毫秒）下对客户端透明；快照安装
// （installMu，分钟级）期间不把 handler 无限挂住，超时走 ErrNotLeader 的
// 可重试路径（rpc 映射 HA_NOT_AVAILABLE）。
const defaultWritableWait = 3 * time.Second

// gateState 门状态快照（不可变，整体替换）：term 任期内已识别的首条
// index。termStart==0 = 本任期首条尚未识别（或本节点非 leader），门关。
type gateState struct {
	term      uint64
	termStart uint64
}

// writable 返回门是否开着及其 epoch（任期号）。非阻塞，热路径可调。
func (gr *group) writable() (uint64, bool) {
	es := gr.epoch.Load()
	if !es.isSelf || gr.lead.Load() != gr.selfID {
		return 0, false
	}
	gs := gr.gate.Load()
	if gs.term != es.term || gs.termStart == 0 || gr.applied.Load() < gs.termStart {
		return 0, false
	}
	return es.term, true
}

// observeTermStart 在 dispatchReady 处理完 SoftState 之后调用（Ready 循环
// 线程）：维护门状态。
//
//   - 本节点非 leader：门状态清零（关门），唤醒等待者让其返回 ErrNotLeader；
//   - 本节点是 leader 且本任期首条未识别：在本轮 MsgStorageAppend 条目里找
//     Term == 当前任期的第一条，找到即记为 termStart；本轮没有（理论上
//     当选那一轮就带着空条目，这里只是不依赖该时序）则记「本任期、未识别」，
//     下一轮继续找。
//
// 任何门状态变化与 SoftState 变化都唤醒等待者（无等待者时只有一次原子读）。
func (gr *group) observeTermStart(rd *raft.Ready) {
	es := gr.epoch.Load()
	cur := gr.gate.Load()
	if !es.isSelf {
		changed := cur.term != 0 || cur.termStart != 0
		if changed {
			gr.gate.Store(&gateState{})
		}
		if changed || rd.SoftState != nil {
			gr.gateKick()
		}
		return
	}
	if cur.term == es.term && cur.termStart != 0 {
		return // 本任期首条已识别，门由 applied 推进决定
	}
	for _, m := range rd.Messages {
		if m.GetTo() != raft.LocalAppendThread {
			continue
		}
		for _, e := range m.GetEntries() {
			if e.GetTerm() == es.term {
				gr.gate.Store(&gateState{term: es.term, termStart: e.GetIndex()})
				gr.lg.Info("本任期首条已识别，可写门待 apply 追平后打开",
					"term", es.term, "term_start", e.GetIndex(), "applied", gr.applied.Load())
				gr.gateKick()
				return
			}
		}
	}
	if cur.term != es.term {
		// 新任期首轮未见本任期条目：记下任期、门保持关闭，下一轮继续识别
		gr.gate.Store(&gateState{term: es.term})
		gr.gateKick()
	} else if rd.SoftState != nil {
		gr.gateKick()
	}
}

// gateKick 唤醒全部门等待者（关闭当前广播 channel 并换新）。applied 推进
// 与门状态变化后调用。无等待者时只做一次原子读——apply 热路径零开销。
//
// 无丢失唤醒的论证：等待者先 gateWaiters+1、再取 channel、再判可写；
// 唤醒方先改状态（applied/gate/epoch 原子写）、再读 gateWaiters。若唤醒方
// 读到 0，则等待者的 +1 晚于该读、从而晚于状态写，其随后的可写判定必见
// 新状态；读到非 0 则 close 的要么是等待者手上的 channel，要么等待者
// 取到的是 close 之后的新 channel（其判定同样晚于状态写）。
func (gr *group) gateKick() {
	if gr.gateWaiters.Load() == 0 {
		return
	}
	gr.gateMu.Lock()
	close(gr.gateCh)
	gr.gateCh = make(chan struct{})
	gr.gateMu.Unlock()
}

// gateChan 返回当前广播 channel（下一次 gateKick 时关闭）。
func (gr *group) gateChan() <-chan struct{} {
	gr.gateMu.Lock()
	defer gr.gateMu.Unlock()
	return gr.gateCh
}

// waitWritable 阻塞到本组可写门打开，返回门的 epoch（当前任期号）。
//
// 参数：
//   - ctx: 调用方等待预算；实际上限 = min(ctx, gr.writableWait)
//
// 返回：
//   - (epoch, nil)：本节点是 leader 且 applied 已越过本任期首条
//   - ErrNotLeader 包装：本节点非 leader（立即返回）、等待中失去领导权
//     （立即返回，不等到超时）、等待超时或 ctx 结束（可重试：客户端经
//     HA_NOT_AVAILABLE 重试）、组已停止
//
// 注意：调用方不得持锁等待（最长阻塞 writableWait）。
func (gr *group) waitWritable(ctx context.Context) (uint64, error) {
	if t, ok := gr.writable(); ok {
		return t, nil // 快路径：稳态 leader，零分配
	}
	if es := gr.epoch.Load(); !es.isSelf {
		return 0, gr.notLeaderErr()
	}
	start := time.Now()
	wctx, cancel := context.WithTimeout(ctx, gr.writableWait)
	defer cancel()
	gr.gateWaiters.Add(1)
	defer gr.gateWaiters.Add(-1)
	for {
		ch := gr.gateChan()
		if t, ok := gr.writable(); ok {
			gr.lg.Debug("可写门已打开，放行等待者", "term", t,
				"waited", time.Since(start).Round(time.Millisecond).String())
			return t, nil
		}
		if es := gr.epoch.Load(); !es.isSelf {
			gr.lg.Debug("等待可写期间失去领导权", "lead", gr.leader())
			return 0, gr.notLeaderErr()
		}
		select {
		case <-ch:
		case <-wctx.Done():
			gs := gr.gate.Load()
			gr.lg.Warn("等待可写门超时（apply 未追平本任期首条）",
				"waited", time.Since(start).Round(time.Millisecond).String(),
				"term", gs.term, "term_start", gs.termStart, "applied", gr.applied.Load(),
				"err", wctx.Err())
			return 0, fmt.Errorf("%w: 组 %d 新任期 apply 未追平（term_start=%d applied=%d）: %w",
				ErrNotLeader, gr.g, gs.termStart, gr.applied.Load(), wctx.Err())
		case <-gr.doneCh:
			return 0, gr.stoppedErr()
		}
	}
}
