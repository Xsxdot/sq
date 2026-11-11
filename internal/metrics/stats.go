// stats.go: 从 store 状态推导运行指标。
//
// 职责：
//   - Collect：全扫推导（控制台 /admin/overview 的数据源，也是 CollectLive 的
//     对照基准）：topic 写入总量（alloc 计数器）、各组待拉取堆积（alloc-cursor
//     差值）、inflight 计数、延时队列深度、半消息暂存深度
//   - CollectLive：同一份 Stats 的低成本算法（SQ4.9，采样器与 /metrics 用）：
//     delay/half 深度读 store 登记前缀的内存计数（CountedPrefixes 由装配方在
//     Open 时登记），inflight 逐队列读 deliver 内存态（InflightSource），本节点
//     无权威内存态的队列才回落扫该队列的 inflight 前缀
//
// 边界：
//   - Collect 全量扫 cursor/inflight/delay/half 前缀，代价与积压线性；不用于
//     周期采样
//   - CollectLive 仍扫 cursor 前缀（规模 = 组×topic×队列，与积压无关）并逐
//     队列读 alloc；队列集合取自盘上 cursor，只在内存、尚未检查点的队列最多
//     缺席一个检查点间隔
//   - 没有 cursor 记录的 (group, topic) 不出现在 Pending 里：组从未拉取过就
//     没有"它视角的堆积"可言（要看总量有 Written）
//   - 本包所有 store 扫描都经 scan 计数「扫描键数」（未导出，供 S2 观测）
package metrics

import (
	"fmt"
	"sync/atomic"

	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/store"
)

// 扫描键族（扫描键数观测的分桶）。
const (
	famCursor = iota
	famInflight
	famDelay
	famHalf
	famCount
)

// scanned 本包各键族累计扫描键数。只增不减、对生产零语义影响，测试据此
// 判定「采样/抓取不随积压量线性扫描」（SQ4.9 S2）。
var scanned [famCount]atomic.Uint64

func scanKeys(fam int) uint64 { return scanned[fam].Load() }

func scanKeysTotal() uint64 {
	var n uint64
	for i := range scanned {
		n += scanned[i].Load()
	}
	return n
}

// scan 本包唯一的 store 扫描入口：透传 st.Scan，按 fam 累计扫描键数。
func scan(st *store.Store, fam int, lower, upper []byte, fn func(k, v []byte) (bool, error)) error {
	return st.Scan(lower, upper, 0, func(k, v []byte) (bool, error) {
		scanned[fam].Add(1)
		return fn(k, v)
	})
}

// CountedPrefixes 需要 store 维护内存计数的前缀（delay/、half/）。装配方
// （cmd/sq 的 store 选项）在 Open 时登记，CollectLive 才能零扫描读深度。
// half/ 以 '/' 自定界，halfidx/ 不在其内——与 Collect 的扫描口径一致。
func CountedPrefixes() []string {
	return []string{store.DelayPrefix, store.HalfPrefix}
}

// InflightSource 逐队列在外条数的内存视图（*deliver.Deliverer 天然实现）。
// ok=false 表示本节点没有该队列的权威内存态（follower、尚未访问、已换届），
// CollectLive 回落扫该队列的 inflight 前缀。
type InflightSource interface {
	InflightCount(group, topic string, q uint32) (n int, ok bool)
}

// GroupTopic 组×topic 二元标签键。
type GroupTopic struct{ Group, Topic string }

// Stats 一次抓取推导出的全部指标值。
type Stats struct {
	Topics     int
	Groups     int
	DelayDepth int
	HalfDepth  int                   // 半消息暂存条数（扫 half/ 前缀，事务量级瞬时值）
	Written    map[string]uint64     // topic → 累计写入条数
	Pending    map[GroupTopic]uint64 // 已写入未拉取
	Inflight   map[GroupTopic]int    // 已投递未确认
}

// Collect 全扫 store 推导当前指标快照（控制台数据源与 CollectLive 的对照基准）。
func Collect(st *store.Store, mt *meta.Meta) (*Stats, error) {
	s, next, err := collectWritten(st, mt)
	if err != nil {
		return nil, err
	}
	if err := collectPending(st, s, next, nil); err != nil {
		return nil, err
	}
	ip := store.InflightAllPrefix()
	err = scan(st, famInflight, ip, store.PrefixUpperBound(ip), func(k, v []byte) (bool, error) {
		g, tp, _, _, perr := store.ParseInflightKey(k)
		if perr != nil {
			return false, perr
		}
		s.Inflight[GroupTopic{g, tp}]++
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描 inflight: %w", err)
	}
	if s.DelayDepth, err = scanDepth(st, famDelay, store.DelayPrefix); err != nil {
		return nil, err
	}
	// half 扫描与 delay 同款：前缀带 '/' 自定界（PrefixUpperBound 进位到
	// "half0"），halfidx/ 索引天然落在区间外，不会混入深度计数
	if s.HalfDepth, err = scanDepth(st, famHalf, store.HalfPrefix); err != nil {
		return nil, err
	}
	return s, nil
}

// CollectLive 低成本推导与 Collect 相同的 Stats（静置时逐字段相等，SQ4.9 S1）。
//
// 参数：src 为 nil 时 inflight 全部回落前缀扫描（测试/降级）。
// 返回：与 Collect 同形状的快照。
//
// 注意：
//   - delay/half 深度读 st.PrefixCount；前缀未登记（st 打开时没带
//     CountedPrefixes）时回落全扫，结果仍正确、只是代价回到积压线性
//   - 检查点模式下 leader 的 inflight 是内存值，pending 用盘上 cursor，二者
//     最多相差一个检查点间隔的投递量
func CollectLive(st *store.Store, mt *meta.Meta, src InflightSource) (*Stats, error) {
	s, next, err := collectWritten(st, mt)
	if err != nil {
		return nil, err
	}
	var queues []store.ConsumeQueue
	if err := collectPending(st, s, next, &queues); err != nil {
		return nil, err
	}
	for _, cq := range queues {
		n, ok := 0, false
		if src != nil {
			n, ok = src.InflightCount(cq.Group, cq.Topic, cq.QueueID)
		}
		if !ok {
			ip := store.InflightPrefix(cq.Group, cq.Topic, cq.QueueID)
			n = 0
			if err := scan(st, famInflight, ip, store.PrefixUpperBound(ip), func(k, v []byte) (bool, error) {
				n++
				return true, nil
			}); err != nil {
				return nil, fmt.Errorf("扫描 inflight (%s/%s/%d): %w", cq.Group, cq.Topic, cq.QueueID, err)
			}
		}
		if n > 0 {
			s.Inflight[GroupTopic{cq.Group, cq.Topic}] += n
		}
	}
	if s.DelayDepth, err = liveDepth(st, famDelay, store.DelayPrefix); err != nil {
		return nil, err
	}
	if s.HalfDepth, err = liveDepth(st, famHalf, store.HalfPrefix); err != nil {
		return nil, err
	}
	return s, nil
}

// liveDepth 读登记前缀的内存计数；未登记时回落全扫。
func liveDepth(st *store.Store, fam int, prefix string) (int, error) {
	if n, ok := st.PrefixCount(prefix); ok {
		return int(n), nil
	}
	return scanDepth(st, fam, prefix)
}

// scanDepth 全扫一个前缀计条数。
func scanDepth(st *store.Store, fam int, prefix string) (int, error) {
	n := 0
	p := []byte(prefix)
	if err := scan(st, fam, p, store.PrefixUpperBound(p), func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		return 0, fmt.Errorf("扫描 %s: %w", prefix, err)
	}
	return n, nil
}

// collectWritten 推导 topic/组数与各 topic 写入量，返回每队列下一 offset
// （cursor 差值计算复用）。
func collectWritten(st *store.Store, mt *meta.Meta) (*Stats, map[string]map[uint32]uint64, error) {
	s := &Stats{
		Written:  map[string]uint64{},
		Pending:  map[GroupTopic]uint64{},
		Inflight: map[GroupTopic]int{},
	}
	topics := mt.Topics()
	s.Topics = len(topics)
	s.Groups = len(mt.Groups())
	// 每队列下一 offset（= 累计写入量）；cursor 差值计算复用同一份
	next := map[string]map[uint32]uint64{}
	for _, tc := range topics {
		qn := map[uint32]uint64{}
		var sum uint64
		for q := uint32(0); q < tc.Queues; q++ {
			raw, ok, err := st.Get(store.AllocKey(tc.Name, q))
			if err != nil {
				return nil, nil, fmt.Errorf("读 alloc (%s/%d): %w", tc.Name, q, err)
			}
			if ok {
				qn[q] = store.GetU64(raw)
			}
			sum += qn[q]
		}
		next[tc.Name] = qn
		s.Written[tc.Name] = sum
	}
	return s, next, nil
}

// collectPending 扫 cursor 前缀推导 Pending；queues 非 nil 时顺带列出全部
// 有 cursor 的队列（CollectLive 的 inflight 枚举）。
func collectPending(st *store.Store, s *Stats, next map[string]map[uint32]uint64, queues *[]store.ConsumeQueue) error {
	cp := store.CursorPrefix()
	err := scan(st, famCursor, cp, store.PrefixUpperBound(cp), func(k, v []byte) (bool, error) {
		g, tp, q, perr := store.ParseCursorKey(k)
		if perr != nil {
			return false, perr
		}
		if queues != nil {
			*queues = append(*queues, store.ConsumeQueue{Group: g, Topic: tp, QueueID: q})
		}
		cur := store.GetU64(v)
		if n, ok := next[tp][q]; ok && n > cur {
			s.Pending[GroupTopic{g, tp}] += n - cur
		}
		// n <= cur：topic 被删后重建（alloc 归零）或纯粹已消费完，都按 0 堆积处理
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("扫描 cursor: %w", err)
	}
	return nil
}
