// state.go 队列消费状态的内存权威态（卡 SQ4.1）。
//
// 职责：
//   - queueState：一个 (消费组, topic, 队列) 的 cursor、inflight 表、按
//     (到期时刻, offset) 排序的到期索引、在外顺序消息计数、脏集；由该队列
//     所属 raft 组的可写 leader（单机档即本进程）持有，队列锁内读写
//   - 装载：从 pebble 的 cursor/、inflight/ 键族读出检查点建内存态（load）
//   - 批次构造：把内存态写成落盘批次——差量（buildDelta）与全量（buildFull）
//     两种，是不变式 I1「cursor 与其覆盖区间的 inflight 同批写」的唯一出处
//   - 只读镜像：inflight 条数与是否已装载的原子镜像，供不取队列锁的查询
//
// 边界：
//   - 纯数据结构：不做复制、不判领导权、不取锁（调用方持 qs.mu）——何时
//     装载、何时提案、失败如何失效都在 deliver.go / checkpoint.go
//   - 盘上格式不变：键布局与 inflight JSON 编码沿用 store/core 的既有定义
//   - 消息本体（msg/）不在这里，取件照旧读 pebble
package deliver

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/store"
)

// inflightEntry 一条在外未确认消息的内存记录：盘上 InflightState 值 +
// 到期索引里的位置（heap 回填，O(log n) 改键/删除用）。
type inflightEntry struct {
	off uint64
	st  core.InflightState
	idx int
}

// expiryHeap 按 (ExpireAtMs, offset) 排序的最小堆，带位置回填。offset 作
// 第二键让同到期时刻的重投顺序确定（与旧实现按 offset 扫描的顺序一致）。
type expiryHeap []*inflightEntry

func (h expiryHeap) Len() int { return len(h) }
func (h expiryHeap) Less(i, j int) bool {
	if h[i].st.ExpireAtMs != h[j].st.ExpireAtMs {
		return h[i].st.ExpireAtMs < h[j].st.ExpireAtMs
	}
	return h[i].off < h[j].off
}
func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}
func (h *expiryHeap) Push(x any) {
	e := x.(*inflightEntry)
	e.idx = len(*h)
	*h = append(*h, e)
}
func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.idx = -1
	*h = old[:n-1]
	return e
}

// flush 一次逐次复制提案的等待点：done 关闭后 err 即该提案（及其承载的全部
// 内存变更）的落盘结果。见 checkpoint.go。
type flush struct {
	done chan struct{}
	err  error
	gen  uint64 // 提案时队列状态的代号；完成时代号已变则不回写队列状态
}

// queueState 一个 (group, topic, queue) 的消费状态。除原子镜像外全部字段
// 只在 mu 内读写。
type queueState struct {
	mu sync.Mutex

	group, topic string
	q            uint32

	// loaded=false 即无内存态（未装载或已失效），下次访问从 pebble 装载。
	loaded bool
	// epoch 装载时可写门给出的 epoch：内存态只在该 epoch 内权威，提案一律
	// 按它绑定任期（ApplyAsyncAt）。
	epoch uint64
	// gen 状态代号：每次失效（drop）加一。在途提案记下提案时的代号，完成时
	// 代号已变说明内存态已被丢弃重建，不得再回写。
	gen uint64

	cursor   uint64
	inflight map[uint64]*inflightEntry
	exp      expiryHeap
	// orderedN 在外顺序消息条数（>0 即顺序锁占用）。用计数而非布尔：装载
	// 到历史遗留的多条顺序 inflight 时也不会因删掉其中一条而误放锁。
	orderedN int

	// dirty 自上次构造批次以来变更过的 offset（内存里有 → Set，没有 → Delete）。
	dirty       map[uint64]struct{}
	cursorDirty bool
	// needFull 下一次逐次复制提案必须是整体替换批次（buildFull）而非差量：
	// 每次从盘装载后置位（审查 I-1）。丢弃内存态的原因可能是一条结局未知的
	// 提案（超时的重置位点、管理批次、差量），它仍可能排在组内 FIFO 上、晚于
	// 本次装载才 apply；差量叠在它之上会让盘上 cursor 越过被它删掉的 inflight。
	// 整体替换批次排在一切迟到条目之后、单独落地即自洽。
	needFull bool

	// 逐次复制的在途控制（checkpoint.go）：inFlight=本队列尚未了结（Wait 未
	// 返回）的提案数，**不分代号**——丢弃内存态后旧代号的提案仍计数，重装须等
	// 它归零（修订 R3）。上限 Deliverer.maxInFlight（R1：1；前缀有序后端：
	// maxInFlightPrefix）。next=达到上限期间到达的变更等待的下一次提案（R2）。
	inFlight int
	next     *flush
	// settled inFlight 归零时关闭（有人等时才创建）：重装与计划内转移等它。
	settled chan struct{}

	// ckDirty 检查点模式：本队列已登记进 Deliverer 的脏队列集合（避免每次
	// 变更都去碰那张共享 map）。只在 mu 内读写。
	ckDirty bool

	// idxVisits 到期索引被检视的元素累计数（U3 用它核对「未到期 inflight
	// 再多，一次取件对索引的访问也是常数级」）。只在 mu 内写。
	idxVisits uint64

	// 只读镜像：不取锁的查询（InflightCount）读它们。epochMirror 先于
	// loadedMirror 置位，读侧先读 loadedMirror 再读 epochMirror。
	nInflight    atomic.Int64
	loadedMirror atomic.Bool
	epochMirror  atomic.Uint64
}

func newQueueState(group, topic string, q uint32) *queueState {
	return &queueState{group: group, topic: topic, q: q}
}

// clear 清空内存态并标记未装载（不动 gen/inFlight/next/settled，由调用方处理）。
func (qs *queueState) clear() {
	qs.loaded = false
	qs.cursor = 0
	qs.inflight = nil
	qs.exp = nil
	qs.orderedN = 0
	qs.dirty = nil
	qs.cursorDirty = false
	qs.nInflight.Store(0)
	qs.loadedMirror.Store(false)
}

// init 置为「已装载、cursor=c、无 inflight、无脏」的空状态并对外标为已装载
// （= reset + markLoaded；用于不需扫盘的场景，如位点重置）。
func (qs *queueState) init(epoch, c uint64) {
	qs.reset(epoch, c)
	qs.markLoaded()
}

// reset 置为「已装载、cursor=c、无 inflight、无脏」的空状态，但不动只读镜像
// 的装载标志：从盘装载时要等扫盘完成才 markLoaded，InflightCount 不会读到
// 扫了一半的部分值。
func (qs *queueState) reset(epoch, c uint64) {
	qs.clear()
	qs.loaded = true
	qs.epoch = epoch
	qs.epochMirror.Store(epoch)
	qs.cursor = c
	qs.inflight = map[uint64]*inflightEntry{}
	qs.dirty = map[uint64]struct{}{}
	qs.needFull = false
}

// markLoaded 对外（InflightCount）标为已装载。epochMirror 已在 reset 里先置。
func (qs *queueState) markLoaded() { qs.loadedMirror.Store(true) }

// load 从 pebble 装载本队列的检查点：cursor（无键 → 0）+ inflight 前缀全扫
// 解码。O(该队列 inflight)，调用方持 mu。失败时状态保持未装载。
func (qs *queueState) load(st *store.Store, epoch uint64) error {
	c := uint64(0)
	v, ok, err := st.Get(store.CursorKey(qs.group, qs.topic, qs.q))
	if err != nil {
		qs.clear()
		return fmt.Errorf("装载 fetch 位点 (group=%s topic=%s q=%d): %w", qs.group, qs.topic, qs.q, err)
	}
	if ok {
		c = store.GetU64(v)
	}
	// 扫盘期间 nInflight 只是部分值：reset 不标已装载，扫完才 markLoaded
	qs.reset(epoch, c)
	pfx := store.InflightPrefix(qs.group, qs.topic, qs.q)
	err = st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		_, _, _, off, perr := store.ParseInflightKey(k)
		if perr != nil {
			return false, perr
		}
		ist, derr := core.DecodeInflight(v)
		if derr != nil {
			return false, fmt.Errorf("off=%d: %w", off, derr)
		}
		qs.insert(off, *ist)
		return true, nil
	})
	if err != nil {
		qs.clear()
		return fmt.Errorf("装载 inflight (group=%s topic=%s q=%d): %w", qs.group, qs.topic, qs.q, err)
	}
	// 装载不是变更：脏集清空（insert 记过脏）；装载后的第一个提案整体替换
	qs.dirty = map[uint64]struct{}{}
	qs.cursorDirty = false
	qs.needFull = true
	qs.markLoaded()
	return nil
}

// insert 新增或覆盖一条 inflight，维护索引、顺序计数、镜像，并记脏。
func (qs *queueState) insert(off uint64, s core.InflightState) {
	if e, ok := qs.inflight[off]; ok {
		if e.st.Ordered {
			qs.orderedN--
		}
		e.st = s
		heap.Fix(&qs.exp, e.idx)
	} else {
		e = &inflightEntry{off: off, st: s}
		qs.inflight[off] = e
		heap.Push(&qs.exp, e)
		qs.nInflight.Add(1)
	}
	if s.Ordered {
		qs.orderedN++
	}
	qs.dirty[off] = struct{}{}
}

// remove 删除一条 inflight（不存在返回 false），维护索引、顺序计数、镜像，并记脏。
func (qs *queueState) remove(off uint64) bool {
	e, ok := qs.inflight[off]
	if !ok {
		return false
	}
	heap.Remove(&qs.exp, e.idx)
	delete(qs.inflight, off)
	if e.st.Ordered {
		qs.orderedN--
	}
	qs.nInflight.Add(-1)
	qs.dirty[off] = struct{}{}
	return true
}

// setExpire 改一条 inflight 的到期时刻（索引随之改键），记脏。
func (qs *queueState) setExpire(e *inflightEntry, expAt int64) {
	e.st.ExpireAtMs = expAt
	heap.Fix(&qs.exp, e.idx)
	qs.dirty[e.off] = struct{}{}
}

// setCursor 推进/重置 fetch 位点，记脏。
func (qs *queueState) setCursor(c uint64) {
	if c != qs.cursor {
		qs.cursor = c
		qs.cursorDirty = true
	}
}

// expired 按 (到期时刻, offset) 升序返回已到期（ExpireAtMs <= now）的
// inflight；all=false 时至多 max 条（调用方用不上更多时提前停）。不修改
// 状态（弹出后原样放回）。代价 O(k log n)，k 为返回条数——未到期的记录
// 只会看到堆顶一条。
func (qs *queueState) expired(now int64, max int, all bool) []*inflightEntry {
	var out []*inflightEntry
	for qs.exp.Len() > 0 {
		qs.idxVisits++
		top := qs.exp[0]
		if top.st.ExpireAtMs > now || (!all && len(out) >= max) {
			break
		}
		out = append(out, heap.Pop(&qs.exp).(*inflightEntry))
	}
	for _, e := range out {
		heap.Push(&qs.exp, e)
	}
	return out
}

// hasDirty 是否有尚未写进批次的变更。
func (qs *queueState) hasDirty() bool {
	return qs.cursorDirty || len(qs.dirty) > 0
}

// buildDelta 把脏集写成差量批次并清空脏集（不变式 I1 的出处之一）。
//
// 批次 = Set(cursor) + 对每个脏 offset：内存里有 → Set(编码后的 inflight)，
// 没有 → Delete(inflight 键)。非空即恒写 cursor：盘上状态 = 上一批落盘后的
// 状态 + 本批 = 构造时刻的内存态，cursor 推进与它越过区间里新增的 inflight
// 必在同一批。差量的正确性依赖「前序差量按序生效」——由调用方保证同队列
// 至多 maxInFlight 个差量在途（R1 为 1、前一批成功才构造下一批；前缀有序
// 后端放宽，按提交序生效、失败则其后全部失败，见计划修订 R3）。
func (qs *queueState) buildDelta(b *store.Batch) {
	b.Set(store.CursorKey(qs.group, qs.topic, qs.q), store.PutU64(qs.cursor))
	for off := range qs.dirty {
		k := store.InflightKey(qs.group, qs.topic, qs.q, off)
		if e, ok := qs.inflight[off]; ok {
			b.Set(k, core.EncodeInflight(&e.st))
		} else {
			b.Delete(k)
		}
	}
	qs.dirty = map[uint64]struct{}{}
	qs.cursorDirty = false
}

// buildFull 把整份内存态写成全量批次并清空脏集：Set(cursor) +
// DeleteRange(本队列 inflight 前缀) + 全部 inflight Set。不依赖盘上基线，
// 单独落地即自洽（重置位点用；修订 R1 的检查点批次同形）。
func (qs *queueState) buildFull(b *store.Batch) {
	b.Set(store.CursorKey(qs.group, qs.topic, qs.q), store.PutU64(qs.cursor))
	ip := store.InflightPrefix(qs.group, qs.topic, qs.q)
	b.DeleteRange(ip, store.PrefixUpperBound(ip))
	for off, e := range qs.inflight {
		b.Set(store.InflightKey(qs.group, qs.topic, qs.q, off), core.EncodeInflight(&e.st))
	}
	qs.dirty = map[uint64]struct{}{}
	qs.cursorDirty = false
}
