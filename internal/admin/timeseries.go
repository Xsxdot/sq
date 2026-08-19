// timeseries.go: 时序曲线与消费关系总账两个端点（控制台总览页的数据源）。
//
// 职责：
//   - GET /admin/timeseries：按 range 从内存环或 Pebble 取时序点
//   - GET /admin/ledger：一次返回全部「组×topic」消费关系（含队列级明细）
//
// 边界：
//   - 两个端点都依赖采样器；采样器未启用（admin_listen 关闭时不会构造）
//     一律 503 而不是空数据——空数据会被画成平的零线，比报错更误导
//   - ledger 每次请求全扫一遍 cursor 前缀，代价与消费关系数成线性；
//     控制台 5 秒轮询一次，目标量级（单机、几十个组×topic）可接受
package admin

import (
	"net/http"
	"sort"

	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/metrics"
	"github.com/Xsxdot/sq/internal/store"
)

// rangeSpec 一个 range 档位对应的取数方式。
type rangeSpec struct {
	spanMs        int64
	granularityMs int64
	source        string // ring | pebble
}

// ranges 三档时间跨度。1h 走内存环（5 秒粒度，看得见秒级抖动），
// 24h/7d 走 Pebble（1 分钟粒度，取该分钟峰值）。
var ranges = map[string]rangeSpec{
	"1h":  {spanMs: 3600_000, granularityMs: 5_000, source: "ring"},
	"24h": {spanMs: 86_400_000, granularityMs: 60_000, source: "pebble"},
	"7d":  {spanMs: 604_800_000, granularityMs: 60_000, source: "pebble"},
}

// handleTimeseries GET /admin/timeseries?range=1h|24h|7d。
func (s *Server) handleTimeseries(w http.ResponseWriter, r *http.Request) {
	if s.sp == nil {
		s.httpError(w, http.StatusServiceUnavailable, "时序采样器未启用")
		return
	}
	key := r.URL.Query().Get("range")
	if key == "" {
		key = "1h"
	}
	spec, ok := ranges[key]
	if !ok {
		s.httpError(w, http.StatusBadRequest, "range 只接受 1h|24h|7d，得到 %q", key)
		return
	}
	var pts []metrics.Point
	if spec.source == "ring" {
		pts = s.sp.Ring()
	} else {
		latest, ok := s.sp.Latest()
		if !ok {
			// 环空 = 采样器刚起来还没采到第一个点，返回空曲线而不是报错
			pts = []metrics.Point{}
		} else {
			var err error
			if pts, err = s.sp.History(latest.TsMs - spec.spanMs); err != nil {
				s.logger.Error("admin 读时序历史失败", "range", key, "err", err)
				s.httpError(w, http.StatusInternalServerError, "%v", err)
				return
			}
		}
	}
	s.writeJSON(w, http.StatusOK, struct {
		Range         string          `json:"range"`
		GranularityMs int64           `json:"granularity_ms"`
		Source        string          `json:"source"`
		Points        []metrics.Point `json:"points"`
	}{key, spec.granularityMs, spec.source, pts})
}

// ledgerQueue 总账行的队列级明细。
type ledgerQueue struct {
	QueueID    uint32 `json:"queue_id"`
	Cursor     uint64 `json:"cursor"`
	NextOffset uint64 `json:"next_offset"`
	Inflight   int    `json:"inflight"`
}

// ledgerRow 一条消费关系 = 一个组在一个 topic 上的全部读数。
type ledgerRow struct {
	Group         string        `json:"group"`
	Topic         string        `json:"topic"`
	Cursor        uint64        `json:"cursor"`      // 各队列 cursor 之和
	NextOffset    uint64        `json:"next_offset"` // 各队列写入头之和
	Pending       uint64        `json:"pending"`
	Inflight      int           `json:"inflight"`
	DLQ           uint64        `json:"dlq"`             // 该组累计死信（组维度，同组各行相同）
	WrittenQPS    *float64      `json:"written_qps"`     // 该 topic 最近一个采样窗口的写入速率；采样不足时为 null
	LastConsumeMs int64         `json:"last_consume_ms"` // 最近一次观察到位点推进的时刻；未观察到为 0
	Queues        []ledgerQueue `json:"queues"`
}

// handleLedger GET /admin/ledger：一次返回全部消费关系。
//
// 总览表要在一屏里横向比较所有组×topic 的落后量，逐个组去打
// GET /admin/groups/{name} 会变成 1+N 次请求且每 5 秒重来一遍；
// 一个端点返回整张表，前端只需一次请求。
func (s *Server) handleLedger(w http.ResponseWriter, r *http.Request) {
	if s.sp == nil {
		s.httpError(w, http.StatusServiceUnavailable, "时序采样器未启用")
		return
	}
	// 死信按消费组统计：死信 topic 固定 1 队列，其累计写入即该组产生的死信总数。
	// 死信是组维度而不是组×topic 维度——同组各行显示同一个值是刻意的。
	dlqByGroup := map[string]uint64{}
	for _, tc := range s.mt.Topics() {
		if !meta.IsDLQTopic(tc.Name) {
			continue
		}
		group := tc.Name[len(meta.DLQPrefix):]
		var sum uint64
		for q := uint32(0); q < tc.Queues; q++ {
			raw, ok, err := s.st.Get(store.AllocKey(tc.Name, q))
			if err != nil {
				s.logger.Error("admin 读死信 alloc 失败", "topic", tc.Name, "queue", q, "err", err)
				s.httpError(w, http.StatusInternalServerError, "%v", err)
				return
			}
			if ok {
				sum += store.GetU64(raw)
			}
		}
		dlqByGroup[group] = sum
	}

	rows := map[metrics.GroupTopic]*ledgerRow{}
	cp := store.CursorPrefix()
	err := s.st.Scan(cp, store.PrefixUpperBound(cp), 0, func(k, v []byte) (bool, error) {
		g, topic, q, perr := store.ParseCursorKey(k)
		if perr != nil {
			return false, perr
		}
		cur := store.GetU64(v)
		var next uint64
		if raw, ok, gerr := s.st.Get(store.AllocKey(topic, q)); gerr != nil {
			return false, gerr
		} else if ok {
			next = store.GetU64(raw)
		}
		inflight := 0
		ip := store.InflightPrefix(g, topic, q)
		if serr := s.st.Scan(ip, store.PrefixUpperBound(ip), 0, func([]byte, []byte) (bool, error) {
			inflight++
			return true, nil
		}); serr != nil {
			return false, serr
		}
		gt := metrics.GroupTopic{Group: g, Topic: topic}
		row, ok := rows[gt]
		if !ok {
			row = &ledgerRow{Group: g, Topic: topic, DLQ: dlqByGroup[g],
				LastConsumeMs: s.sp.LastConsumeMs(gt)}
			if qps, has := s.sp.TopicQPS(topic); has {
				row.WrittenQPS = &qps
			}
			rows[gt] = row
		}
		row.Cursor += cur
		row.NextOffset += next
		row.Inflight += inflight
		if next > cur {
			row.Pending += next - cur
		}
		row.Queues = append(row.Queues, ledgerQueue{QueueID: q, Cursor: cur,
			NextOffset: next, Inflight: inflight})
		return true, nil
	})
	if err != nil {
		s.logger.Error("admin 总账扫描失败", "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}

	out := make([]ledgerRow, 0, len(rows))
	for _, row := range rows {
		sort.Slice(row.Queues, func(i, j int) bool { return row.Queues[i].QueueID < row.Queues[j].QueueID })
		out = append(out, *row)
	}
	// 按「组，然后 topic」稳定排序：总览表按组聚在一起，同组各行相邻，
	// 组维度的死信列重复出现时读起来才自然
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].Topic < out[j].Topic
	})
	s.writeJSON(w, http.StatusOK, out)
}
