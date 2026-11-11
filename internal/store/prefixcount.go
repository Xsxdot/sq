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
}

// prefixCounter 登记族集合与计数锁。Store.pc 为 nil = 未登记任何前缀，
// 提交路径零额外工作。
type prefixCounter struct {
	fams []*countFamily
	mu   sync.Mutex
	// lockTaken 计数锁取用次数（仅测试观测「不触及的批次不取锁」）。
	lockTaken atomic.Uint64
}

func newPrefixCounter(prefixes []string) (*prefixCounter, error) {
	if len(prefixes) == 0 {
		return nil, nil
	}
	pc := &prefixCounter{}
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
// 返回：ok=false 表示 prefix 未在 Open 时登记（Options.CountPrefixes），
// 调用方应自行扫描。
func (s *Store) PrefixCount(prefix string) (n int64, ok bool) {
	if s.pc == nil {
		return 0, false
	}
	for _, f := range s.pc.fams {
		if f.prefix == prefix {
			return f.n.Load(), true
		}
	}
	return 0, false
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
	deltas, recount, err := s.batchDeltas(b.Repr())
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
	for i, f := range pc.fams {
		if !recount[i] {
			f.n.Add(deltas[i])
			continue
		}
		// 批次已发布可见、仍持锁：重数得到的就是本批之后的精确值
		start := time.Now()
		n, rerr := s.scanCount(f)
		if rerr != nil {
			// 批次已提交、计数却无法重建：之后读数不可信，不能当作提交失败
			// 返回（调用方会重试一个已生效的批次）。按 Pebble 读失败即库已
			// 不可用处理：记错误并 panic，与集群 apply 失败的停摆策略一致。
			s.logger.Error("区间删除后重数前缀失败，计数不可信", "prefix", f.prefix, "err", rerr)
			panic(fmt.Errorf("store 重数前缀 %q: %w", f.prefix, rerr))
		}
		f.n.Store(n)
		s.logger.Info("区间删除触及计数前缀，已重数", "prefix", f.prefix, "count", n,
			"took", time.Since(start).String())
	}
	return sync, nil
}

// batchDeltas 在计数锁内按批次顺序算每个登记族的增量：键提交前是否存在
// 先看本批内更早的操作（overlay），再查已提交状态。Set 不存在的键 +1，
// Delete 存在的键 −1，其余 0。与族相交的 DeleteRange（及保守处理的范围键）
// 使该族改为提交后重数，增量作废。
func (s *Store) batchDeltas(repr []byte) (deltas []int64, recount []bool, err error) {
	fams := s.pc.fams
	deltas = make([]int64, len(fams))
	recount = make([]bool, len(fams))
	overlay := map[string]bool{} // 本批内已操作过的键 → 操作后是否存在
	r := batchrepr.Read(repr)
	for {
		kind, ukey, value, ok, nerr := r.Next()
		if nerr != nil {
			return nil, nil, fmt.Errorf("store 计数解析批次: %w", nerr)
		}
		if !ok {
			return deltas, recount, nil
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
			if f.has(ukey) && !recount[i] {
				in = i
				break
			}
		}
		if in < 0 {
			continue
		}
		existed, seen := overlay[string(ukey)]
		if !seen {
			_, existed, err = s.Get(ukey)
			if err != nil {
				return nil, nil, err
			}
		}
		overlay[string(ukey)] = setOp
		for i, f := range fams {
			if !f.has(ukey) || recount[i] {
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
