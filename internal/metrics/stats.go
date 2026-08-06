// stats.go: 从 store 状态推导运行指标（Collector 与 Admin overview 共用）。
//
// 职责：
//   - 单趟扫描推导：topic 写入总量（alloc 计数器）、各组待拉取堆积
//     （alloc-cursor 差值）、inflight 计数、延时队列深度
//
// 边界：
//   - 全量扫 cursor/inflight/delay 前缀，代价与这三类记录条数成线性——目标量级
//     （单机、5k msg/s）毫秒级；不适合超大 inflight/延时积压场景的高频抓取
//   - 没有 cursor 记录的 (group, topic) 不出现在 Pending 里：组从未拉取过就
//     没有"它视角的堆积"可言（要看总量有 Written）
package metrics

import (
	"fmt"

	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// GroupTopic 组×topic 二元标签键。
type GroupTopic struct{ Group, Topic string }

// Stats 一次抓取推导出的全部指标值。
type Stats struct {
	Topics     int
	Groups     int
	DelayDepth int
	Written    map[string]uint64     // topic → 累计写入条数
	Pending    map[GroupTopic]uint64 // 已写入未拉取
	Inflight   map[GroupTopic]int    // 已投递未确认
}

// Collect 扫描 store 推导当前指标快照。
func Collect(st *store.Store, mt *meta.Meta) (*Stats, error) {
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
				return nil, fmt.Errorf("读 alloc (%s/%d): %w", tc.Name, q, err)
			}
			if ok {
				qn[q] = store.GetU64(raw)
			}
			sum += qn[q]
		}
		next[tc.Name] = qn
		s.Written[tc.Name] = sum
	}
	cp := store.CursorPrefix()
	err := st.Scan(cp, store.PrefixUpperBound(cp), 0, func(k, v []byte) (bool, error) {
		g, tp, q, perr := store.ParseCursorKey(k)
		if perr != nil {
			return false, perr
		}
		cur := store.GetU64(v)
		if n, ok := next[tp][q]; ok && n > cur {
			s.Pending[GroupTopic{g, tp}] += n - cur
		}
		// n <= cur：topic 被删后重建（alloc 归零）或纯粹已消费完，都按 0 堆积处理
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描 cursor: %w", err)
	}
	ip := store.InflightAllPrefix()
	err = st.Scan(ip, store.PrefixUpperBound(ip), 0, func(k, v []byte) (bool, error) {
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
	dp := []byte(store.DelayPrefix)
	err = st.Scan(dp, store.PrefixUpperBound(dp), 0, func(k, v []byte) (bool, error) {
		s.DelayDepth++
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描 delay: %w", err)
	}
	return s, nil
}
