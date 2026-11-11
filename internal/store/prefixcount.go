// prefixcount.go: 登记前缀的精确内存计数（SQ4.9）。
//
// 职责：
//   - 对装配方经 Options.CountPrefixes 登记的键族（如 delay/、half/）维护
//     「本库当前以该前缀开头的键数」，供指标采集零扫描读取（PrefixCount）
//   - Open 时扫一次建立基数；之后每一次经 Store 提交的批次，凡触及登记族，
//     就在计数锁内「逐键查提交前是否存在 → 提交 → 按存在性加减」；批次里有
//     与登记族相交的 DeleteRange（快照安装清空）时，提交后在锁内对该族重数
//
// 边界：
//   - store 不认识任何键族的语义：登记哪些前缀由装配方决定（cmd/sq 经
//     metrics.CountedPrefixes 登记），本文件只提供通用的「按前缀计数」
//   - 不写任何盘上键：计数纯在内存，重启由 Open 重建
//
// 正确性论证（不变式 C1：任意时刻计数 = 对本库该前缀的全扫结果）：
//   - 全覆盖：本仓库对 pebble 的全部键值写入都经 Store 的 ApplyWith/
//     ApplyAsync（Apply 即 ApplyWith），二者都经 commitTracked；SyncWAL 只写
//     LogData，不改变键空间。单机写入、集群 FSM apply（MergeRepr 合批后
//     ApplyWith）、快照安装的清空（DeleteRange 经 Apply）与分块写入
//     （ApplyWith）、raft 日志持久化（raft/ 键族，不触及登记族）全部走这里。
//     **新增任何绕开 Batch 直接用 s.db 改键空间的写入口，必须接 commitTracked，
//     否则计数从此永久漂移。**
//   - 精确：触及登记族的批次「查存在 + 提交 + 加减」整段持计数锁，彼此串行；
//     不触及的批次不取锁、不改变登记族的键，因此锁内的存在性查询（读已提交
//     状态）不会被并发改动。pebble 提交返回时批次已发布可见（commit.go
//     Commit：apply → publish 发生在等待 fsync 之前），下一个持锁者一定看得到
//     上一批的结果。提交失败不加减。
//   - 不持锁等 fsync：sync 提交在计数路径上改用 ApplyNoSyncWait（返回即已
//     发布可见），放锁后再由调用方 SyncWait——语义与 Commit(Sync) 相同。
//   - 基数：Open 时尚无任何写入方，扫一次即精确。
//   - 读失败降级（审查 m-2）：计数路径上的存在性查询或重数失败，不升级为
//     写失败或进程崩溃——计数只是指标的派生物，绝不能让业务写入（集群档即
//     FSM apply）因它停摆。该族标记不可信、提交照常，PrefixCount 返回
//     ok=false（调用方回落全扫）；之后任一次成功的全量重数恢复可信：区间
//     删除触发的重数，或不可信期间触及登记族的提交在锁内的重建尝试（间隔
//     retryEvery，避免读故障期间每次提交都全扫）。
//   - 读侧：PrefixCount 读原子值不取锁；提交与加减之间的瞬间读到旧值，等同
//     「在提交之前读」，对指标无害。
package store

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/batchrepr"
)

// countFamily 一个登记前缀的计数。
type countFamily struct {
	prefix string
	lower  []byte
	upper  []byte // PrefixUpperBound(lower)；nil = 无上界（全 0xFF 前缀）
	n      atomic.Int64
	// trusted=false：计数读失败后不可信，PrefixCount 返回 ok=false，直到一次
	// 成功的全量重数。failedAt 为最近一次失败时刻（计数锁内读写）。
	trusted  atomic.Bool
	failedAt time.Time
}

// prefixCounter 登记族集合与计数锁。Store.pc 为 nil = 未登记任何前缀，
// 提交路径零额外工作。
type prefixCounter struct {
	fams []*countFamily
	mu   sync.Mutex
	// lockTaken 计数锁取用次数（仅测试观测「不触及的批次不取锁」）。
	lockTaken atomic.Uint64
	// retryEvery 不可信期间两次重建尝试的最小间隔（计数锁内全扫，读故障
	// 期间不宜每次提交都做）。
	retryEvery time.Duration
	// readFault 测试注入的计数读错误（生产恒 nil；计数锁内读写）。
	readFault func(key []byte) error
}

// defaultRecountRetry 不可信族的重建尝试间隔缺省值。
const defaultRecountRetry = 30 * time.Second

func (pc *prefixCounter) setReadFault(f func(key []byte) error) {
	pc.mu.Lock()
	pc.readFault = f
	pc.mu.Unlock()
}

func newPrefixCounter(prefixes []string) (*prefixCounter, error) {
	if len(prefixes) == 0 {
		return nil, nil
	}
	pc := &prefixCounter{retryEvery: defaultRecountRetry}
	seen := map[string]bool{}
	for _, p := range prefixes {
		if p == "" {
			return nil, fmt.Errorf("store: 计数前缀不能为空串")
		}
		if seen[p] {
			continue
		}
		seen[p] = true
		lo := []byte(p)
		pc.fams = append(pc.fams, &countFamily{prefix: p, lower: lo, upper: PrefixUpperBound(lo)})
	}
	return pc, nil
}

// rebase 对每个登记族全扫一次重设基数（Open 时调用，尚无写入方）。
func (pc *prefixCounter) rebase(s *Store) error {
	for _, f := range pc.fams {
		n, err := s.scanCount(f)
		if err != nil {
			return err
		}
		f.n.Store(n)
		f.trusted.Store(true)
	}
	return nil
}

// scanCount 对一个登记族全扫计数（只读键，不读值）。
func (s *Store) scanCount(f *countFamily) (int64, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: f.lower, UpperBound: f.upper})
	if err != nil {
		return 0, fmt.Errorf("store 计数前缀 %q 建迭代器: %w", f.prefix, err)
	}
	var n int64
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		it.Close()
		return 0, fmt.Errorf("store 计数前缀 %q 扫描: %w", f.prefix, err)
	}
	if err := it.Close(); err != nil {
		return 0, fmt.Errorf("store 计数前缀 %q 关迭代器: %w", f.prefix, err)
	}
	return n, nil
}

// PrefixCount 返回登记前缀 prefix 当前的键数（不变式：等于对本库该前缀的
// 全扫结果）。不取锁、零扫描。
//
// 返回：ok=false 表示 prefix 未在 Open 时登记（Options.CountPrefixes），或
// 计数因读失败暂不可信（见文件头「读失败降级」），调用方应自行扫描。
func (s *Store) PrefixCount(prefix string) (n int64, ok bool) {
	if s.pc == nil {
		return 0, false
	}
	for _, f := range s.pc.fams {
		if f.prefix == prefix {
			if !f.trusted.Load() {
				return 0, false
			}
			return f.n.Load(), true
		}
	}
	return 0, false
}

// countExists 计数锁内的存在性查询（测试可注入读错误）。
func (s *Store) countExists(key []byte) (bool, error) {
	if f := s.pc.readFault; f != nil {
		if err := f(key); err != nil {
			return false, err
		}
	}
	_, ok, err := s.Get(key)
	return ok, err
}

// recountLocked 计数锁内对一个族全量重数：成功即写入并恢复可信，失败即
// 标记不可信（ERROR 日志带原因）。批次须已发布可见。
func (s *Store) recountLocked(f *countFamily, why string) {
	start := time.Now()
	var n int64
	var err error
	if rf := s.pc.readFault; rf != nil {
		err = rf(f.lower)
	}
	if err == nil {
		n, err = s.scanCount(f)
	}
	if err != nil {
		s.markUntrustedLocked(f, why+"重数失败", err)
		return
	}
	was := f.trusted.Load()
	f.n.Store(n)
	f.trusted.Store(true)
	if !was {
		s.logger.Info("计数前缀重建成功，恢复可信", "prefix", f.prefix, "why", why, "count", n,
			"took", time.Since(start).String())
		return
	}
	s.logger.Info("计数前缀已重数", "prefix", f.prefix, "why", why, "count", n,
		"took", time.Since(start).String())
}

// markUntrustedLocked 标记族不可信（计数锁内）：读数改由调用方全扫，提交不受影响。
func (s *Store) markUntrustedLocked(f *countFamily, why string, err error) {
	f.trusted.Store(false)
	f.failedAt = time.Now()
	s.logger.Error("计数前缀读失败，计数标记为不可信（指标回落全扫，写入不受影响）",
		"prefix", f.prefix, "why", why, "err", err, "retry_every", s.pc.retryEvery.String())
}

// has 键 k 是否属于本族。
func (f *countFamily) has(k []byte) bool { return bytes.HasPrefix(k, f.lower) }

// overlaps 区间 [start, end) 是否与本族区间相交。
func (f *countFamily) overlaps(start, end []byte) bool {
	if f.upper != nil && bytes.Compare(start, f.upper) >= 0 {
		return false
	}
	return bytes.Compare(end, f.lower) > 0
}

// touches 批次是否触及任一登记族（锁外的一次键遍历；坏字节报错）。
func (pc *prefixCounter) touches(repr []byte) (bool, error) {
	r := batchrepr.Read(repr)
	for {
		kind, ukey, value, ok, err := r.Next()
		if err != nil {
			return false, fmt.Errorf("store 计数解析批次: %w", err)
		}
		if !ok {
			return false, nil
		}
		for _, f := range pc.fams {
			switch kind {
			case pebble.InternalKeyKindLogData:
			case pebble.InternalKeyKindRangeDelete:
				if f.overlaps(ukey, value) {
					return true, nil
				}
			case pebble.InternalKeyKindRangeKeySet, pebble.InternalKeyKindRangeKeyUnset,
				pebble.InternalKeyKindRangeKeyDelete:
				// 本仓库不产生范围键；万一出现按「可能触及」保守处理（重数）
				return true, nil
			default:
				if f.has(ukey) {
					return true, nil
				}
			}
		}
	}
}

// commitTracked 提交批次并维护登记族计数——Store 全部键值写入的唯一提交点
// （ApplyWith/ApplyAsync 都经它）。
//
// 参数：sync=本次是否要 fsync；async=调用方自己等 fsync（ApplyAsync 形态）。
// 返回：waitSync=true 表示批次已发布可见、调用方还须 SyncWait 等 fsync
// （计数锁已放）；err 非 nil 时批次按 Apply 约定丢给 GC。
func (s *Store) commitTracked(b *pebble.Batch, sync, async bool) (waitSync bool, err error) {
	pc := s.pc
	if pc != nil {
		hit, terr := pc.touches(b.Repr())
		if terr != nil {
			return false, terr
		}
		if hit {
			return s.commitCounted(b, sync)
		}
	}
	switch {
	case !sync:
		return false, b.Commit(pebble.NoSync)
	case async:
		return true, s.db.ApplyNoSyncWait(b, pebble.Sync)
	default:
		return false, b.Commit(pebble.Sync)
	}
}

// commitCounted 触及登记族的批次：持计数锁提交（不等 fsync）并加减。
func (s *Store) commitCounted(b *pebble.Batch, sync bool) (bool, error) {
	pc := s.pc
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.lockTaken.Add(1)
	deltas, recount, broken, err := s.batchDeltas(b.Repr())
	if err != nil {
		return false, err
	}
	if sync {
		err = s.db.ApplyNoSyncWait(b, pebble.Sync)
	} else {
		err = b.Commit(pebble.NoSync)
	}
	if err != nil {
		return false, err
	}
	// 批次已发布可见、仍持锁：此刻的重数得到的就是本批之后的精确值
	for i, f := range pc.fams {
		switch {
		case recount[i]:
			s.recountLocked(f, "区间删除")
		case broken[i] != nil:
			s.markUntrustedLocked(f, "存在性查询", broken[i])
		case !f.trusted.Load():
			if time.Since(f.failedAt) >= pc.retryEvery {
				s.recountLocked(f, "不可信期间重建")
			}
		default:
			f.n.Add(deltas[i])
		}
	}
	return sync, nil
}

// batchDeltas 在计数锁内按批次顺序算每个登记族的增量：键提交前是否存在
// 先看本批内更早的操作（overlay），再查已提交状态。Set 不存在的键 +1，
// Delete 存在的键 −1，其余 0。与族相交的 DeleteRange（及保守处理的范围键）
// 使该族改为提交后重数，增量作废。存在性查询失败的族记入 broken（该族
// 本批增量作废、提交后标记不可信），不让查询错误变成提交错误；已不可信的
// 族不再查询。err 只在批次字节损坏时返回。
func (s *Store) batchDeltas(repr []byte) (deltas []int64, recount []bool, broken []error, err error) {
	fams := s.pc.fams
	deltas = make([]int64, len(fams))
	recount = make([]bool, len(fams))
	broken = make([]error, len(fams))
	skip := func(i int) bool { return recount[i] || broken[i] != nil || !fams[i].trusted.Load() }
	overlay := map[string]bool{} // 本批内已操作过的键 → 操作后是否存在
	r := batchrepr.Read(repr)
	for {
		kind, ukey, value, ok, nerr := r.Next()
		if nerr != nil {
			return nil, nil, nil, fmt.Errorf("store 计数解析批次: %w", nerr)
		}
		if !ok {
			return deltas, recount, broken, nil
		}
		switch kind {
		case pebble.InternalKeyKindLogData:
			continue
		case pebble.InternalKeyKindRangeDelete:
			for i, f := range fams {
				if f.overlaps(ukey, value) {
					recount[i] = true
				}
			}
			continue
		case pebble.InternalKeyKindRangeKeySet, pebble.InternalKeyKindRangeKeyUnset,
			pebble.InternalKeyKindRangeKeyDelete:
			for i := range fams {
				recount[i] = true
			}
			continue
		}
		var setOp bool
		switch kind {
		case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete, pebble.InternalKeyKindMerge:
			setOp = true
		case pebble.InternalKeyKindDelete, pebble.InternalKeyKindSingleDelete, pebble.InternalKeyKindDeleteSized:
		default:
			// 不认识的点操作：无法判定存在性变化，涉及的族改为重数
			for i, f := range fams {
				if f.has(ukey) {
					recount[i] = true
				}
			}
			continue
		}
		in := -1
		for i, f := range fams {
			if f.has(ukey) && !skip(i) {
				in = i
				break
			}
		}
		if in < 0 {
			continue
		}
		existed, seen := overlay[string(ukey)]
		if !seen {
			var gerr error
			if existed, gerr = s.countExists(ukey); gerr != nil {
				for i, f := range fams {
					if f.has(ukey) && !skip(i) {
						broken[i] = fmt.Errorf("查键 %q: %w", ukey, gerr)
					}
				}
				continue
			}
		}
		overlay[string(ukey)] = setOp
		for i, f := range fams {
			if !f.has(ukey) || skip(i) {
				continue
			}
			switch {
			case setOp && !existed:
				deltas[i]++
			case !setOp && existed:
				deltas[i]--
			}
		}
	}
}
