// series.go: 运行指标的时序化——内存环给实时观察，Pebble 给历史回看。
//
// 职责：
//   - 每 5 秒调一次 Collect，把快照压入内存环（保留最近 1 小时）
//   - 每跨过一个整分钟，把该分钟的 12 个样本归约成一条 MinutePoint 落 Pebble
//   - 按 metrics_retention_hours 定期 DeleteRange 掉过期的分钟点
//   - 对外提供 Ring/History/Latest/TopicQPS/LastConsumeMs 五个派生查询
//
// 两层粒度的分工：控制台的 1h 视图读环（5 秒粒度，看得见秒级抖动），
// 24h/7d 视图读 Pebble（1 分钟粒度，7 天约 1 万条、十几 MB）。
//
// 归约规则（关键，改之前先读懂）：
//   - gauge（落后 / 在途 / 延时深度）取该分钟的**最大值**——分钟平均会把
//     尖峰抹平，而排查时要找的恰恰是尖峰。
//   - 累计写入是单调计数器，最大值恒等于分钟末值，因此存末值（语义更清楚），
//     并**额外**存该分钟内 12 个 5 秒窗口速率的峰值 QPSPeak。只存末值差分
//     算出来的是分钟平均 QPS，一样会抹平尖峰。
//
// 边界：
//   - 只做单机进程内的时序；不做聚合、不做告警、不替代 Prometheus。
//     /metrics 仍是对外的正式指标源，本包是控制台的自带存储。
//   - 环在进程内存里，重启即丢；落库的分钟点跨重启保留。
//   - 采样代价 = 一次 Collect（全扫 cursor/inflight/delay 前缀），5 秒一次；
//     超大 inflight 积压场景下应调大 sampleInterval 或关掉落库。
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

const (
	// sampleInterval 采样间隔。5 秒是控制台轮询周期，两者对齐后
	// 页面每次刷新恰好多出一个新点，曲线是平滑推进而不是跳变。
	sampleInterval = 5 * time.Second
	// ringSize 内存环容量 = 1 小时 / 5 秒。
	ringSize = int(time.Hour / sampleInterval)
	// samplesPerMinute 一分钟内的样本数，用于分钟归约。
	samplesPerMinute = int(time.Minute / sampleInterval)
	// expireEvery 每多少次分钟落库做一次过期清理。60 = 每小时一次：
	// DeleteRange 会留下区间墓碑，每分钟做一次纯属浪费，而保留时长本身
	// 是以小时为单位配置的，1 小时的清理精度绰绰有余。
	expireEvery = 60
)

// Sample 一次采样的原始快照（内存环元素）。
type Sample struct {
	TsMs       int64
	Written    uint64 // 全局累计写入（不含死信 topic）
	Pending    uint64 // 全局落后
	DLQ        uint64 // 全部死信 topic 的累计写入之和
	Inflight   int
	DelayDepth int

	TopicWritten map[string]uint64     // 每业务 topic 累计写入
	GTPending    map[GroupTopic]uint64 // 组×topic 落后
	GTInflight   map[GroupTopic]int    // 组×topic 在途
	GTConsumed   map[GroupTopic]uint64 // 组×topic 已消费量（= topic 写入 − 该组落后）
}

// MinutePoint 一分钟的归约结果（落 Pebble 的记录形状）。
type MinutePoint struct {
	TsMs        int64   `json:"ts_ms"`        // 该分钟的起始时刻
	QPSPeak     float64 `json:"qps_peak"`     // 该分钟内 5 秒窗口速率的峰值
	WrittenEnd  uint64  `json:"written_end"`  // 分钟末累计写入
	PendingMax  uint64  `json:"pending_max"`  // 该分钟落后峰值
	DLQEnd      uint64  `json:"dlq_end"`      // 分钟末累计死信
	InflightMax int     `json:"inflight_max"` // 该分钟在途峰值
	DelayMax    int     `json:"delay_max"`    // 该分钟延时深度峰值
}

// Point 对外统一的时序点形状。环与历史两个数据源都归一到它，
// 前端只需要一套渲染逻辑。
type Point struct {
	TsMs       int64   `json:"ts_ms"`
	QPS        float64 `json:"qps"`
	Pending    uint64  `json:"pending"`
	DLQ        uint64  `json:"dlq"`
	Inflight   int     `json:"inflight"`
	DelayDepth int     `json:"delay_depth"`
}

// Sampler 时序采样器。构造后须在独立 goroutine 里 Run。
type Sampler struct {
	st        *store.Store
	mt        *meta.Meta
	retention time.Duration // 0 = 不落库
	logger    *slog.Logger

	mu    sync.RWMutex
	ring  []Sample // 环形缓冲，按 n 递增覆盖
	n     int      // 已压入的样本总数（不是下标）
	flush int      // 已落库的分钟点数，用于 expireEvery 计数

	// lastConsume 每个组×topic 最近一次观察到位点推进的采样时刻。
	// 消费时间没有任何持久化来源——broker 不会为了一个展示列在 ack 热路径上
	// 写盘。这里靠「已消费量是否增长」推断，精度 = 采样间隔（5 秒），
	// 进程重启后归零、直到下一次观察到推进为止。
	lastConsume map[GroupTopic]int64
}

// NewSampler 构造采样器。retention<=0 表示只保留内存环、不落库。
func NewSampler(st *store.Store, mt *meta.Meta, retention time.Duration, logger *slog.Logger) *Sampler {
	return &Sampler{
		st: st, mt: mt, retention: retention,
		logger:      logger.With("mod", "metrics.series"),
		ring:        make([]Sample, ringSize),
		lastConsume: map[GroupTopic]int64{},
	}
}

// Run 阻塞采样直到 ctx 取消（调用方放入 goroutine）。
func (s *Sampler) Run(ctx context.Context) {
	s.logger.Info("时序采样器启动", "interval", sampleInterval,
		"ring_span", time.Duration(ringSize)*sampleInterval, "retention", s.retention)
	t := time.NewTicker(sampleInterval)
	defer t.Stop()
	// 立即采一次：否则控制台在启动后的头 5 秒里拿到空曲线
	s.tick()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("时序采样器停止", "samples", s.n, "minute_points", s.flush)
			return
		case <-t.C:
			s.tick()
		}
	}
}

// tick 采一次样，必要时归约并落库上一分钟。
func (s *Sampler) tick() {
	sm, err := s.collect()
	if err != nil {
		s.logger.Error("时序采样失败", "err", err)
		return
	}
	prevMinute := s.lastMinute()
	s.push(sm)
	minute := sm.TsMs / 60000 * 60000
	// 跨分钟了：上一分钟的样本已经收齐，可以归约落库。
	// 首个样本没有上一分钟（prevMinute==0），跳过。
	if s.retention <= 0 || prevMinute == 0 || minute == prevMinute {
		return
	}
	mp := s.reduceMinute(prevMinute)
	if err := s.persist(mp); err != nil {
		s.logger.Error("时序点落库失败", "ts_ms", mp.TsMs, "err", err)
		return
	}
	s.logger.Debug("时序点已落库", "ts_ms", mp.TsMs, "qps_peak", mp.QPSPeak,
		"pending_max", mp.PendingMax)

	s.mu.Lock()
	s.flush++
	due := s.flush%expireEvery == 0
	s.mu.Unlock()
	if due {
		cutoff := sm.TsMs - s.retention.Milliseconds()
		if err := s.expire(cutoff); err != nil {
			s.logger.Error("时序点过期清理失败", "cutoff_ms", cutoff, "err", err)
		} else {
			s.logger.Info("时序点过期清理完成", "cutoff_ms", cutoff)
		}
	}
}

// collect 调用 Collect 并整理成 Sample。死信 topic 从业务写入量里剔除，
// 单独汇总成 DLQ——把死信算进「写入量」会让一次故障看起来像一次流量高峰。
func (s *Sampler) collect() (Sample, error) {
	st, err := Collect(s.st, s.mt)
	if err != nil {
		return Sample{}, fmt.Errorf("采样 Collect: %w", err)
	}
	sm := Sample{
		TsMs:         time.Now().UnixMilli(),
		DelayDepth:   st.DelayDepth,
		TopicWritten: make(map[string]uint64, len(st.Written)),
		GTPending:    make(map[GroupTopic]uint64, len(st.Pending)),
		GTInflight:   make(map[GroupTopic]int, len(st.Inflight)),
		GTConsumed:   make(map[GroupTopic]uint64, len(st.Pending)),
	}
	for topic, n := range st.Written {
		if meta.IsDLQTopic(topic) {
			sm.DLQ += n
			continue
		}
		sm.TopicWritten[topic] = n
		sm.Written += n
	}
	for gt, n := range st.Pending {
		sm.GTPending[gt] = n
		sm.Pending += n
		// 已消费量 = 该 topic 写入头 − 该组落后。Collect 没有直接给 cursor 之和，
		// 但这个差值恰好等于它，且不需要再扫一遍 cursor 前缀。
		if w, ok := sm.TopicWritten[gt.Topic]; ok && w >= n {
			sm.GTConsumed[gt] = w - n
		}
	}
	for gt, n := range st.Inflight {
		sm.GTInflight[gt] = n
		sm.Inflight += n
	}
	return sm, nil
}

// push 压入一个样本并更新位点推进时刻。
func (s *Sampler) push(sm Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastConsume == nil {
		s.lastConsume = map[GroupTopic]int64{}
	}
	if s.n > 0 {
		prev := s.ring[(s.n-1)%ringSize]
		for gt, c := range sm.GTConsumed {
			// 只在「有前驱且确实增长」时才记：没有前驱就无从判断，
			// 否则进程重启后每个组都会显示成「刚刚消费过」
			if p, ok := prev.GTConsumed[gt]; ok && c > p {
				s.lastConsume[gt] = sm.TsMs
			}
		}
	}
	s.ring[s.n%ringSize] = sm
	s.n++
}

// lastMinute 返回最新样本所属的整分钟时刻；环为空时返回 0。
func (s *Sampler) lastMinute() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.n == 0 {
		return 0
	}
	return s.ring[(s.n-1)%ringSize].TsMs / 60000 * 60000
}

// samples 返回环内样本的时间升序副本。
func (s *Sampler) samples() []Sample {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := s.n
	if n > ringSize {
		n = ringSize
	}
	out := make([]Sample, 0, n)
	for i := s.n - n; i < s.n; i++ {
		out = append(out, s.ring[i%ringSize])
	}
	return out
}

// rate 计算两个相邻样本之间的写入速率（msg/s）。时间倒退或计数器回退
// （topic 被删后重建，alloc 归零）都按 0 处理，不产生负速率。
func rate(prev, cur Sample) float64 {
	dt := float64(cur.TsMs-prev.TsMs) / 1000
	if dt <= 0 || cur.Written < prev.Written {
		return 0
	}
	return float64(cur.Written-prev.Written) / dt
}

// Ring 返回内存环内的时序点（最近 1 小时，5 秒粒度，时间升序）。
func (s *Sampler) Ring() []Point {
	sms := s.samples()
	out := make([]Point, 0, len(sms))
	for i, sm := range sms {
		var q float64
		if i > 0 {
			q = rate(sms[i-1], sm)
		}
		out = append(out, Point{TsMs: sm.TsMs, QPS: q, Pending: sm.Pending,
			DLQ: sm.DLQ, Inflight: sm.Inflight, DelayDepth: sm.DelayDepth})
	}
	return out
}

// Latest 返回最新一个时序点。环为空时第二个返回值为 false。
func (s *Sampler) Latest() (Point, bool) {
	pts := s.Ring()
	if len(pts) == 0 {
		return Point{}, false
	}
	return pts[len(pts)-1], true
}

// TopicQPS 返回某 topic 最近一个采样窗口的写入速率。
// 样本不足两个（刚启动）时第二个返回值为 false。
func (s *Sampler) TopicQPS(topic string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.n < 2 {
		return 0, false
	}
	cur := s.ring[(s.n-1)%ringSize]
	prev := s.ring[(s.n-2)%ringSize]
	dt := float64(cur.TsMs-prev.TsMs) / 1000
	c, p := cur.TopicWritten[topic], prev.TopicWritten[topic]
	if dt <= 0 || c < p {
		return 0, true
	}
	return float64(c-p) / dt, true
}

// LastConsumeMs 返回某组×topic 最近一次观察到位点推进的时刻。
// 从未观察到（刚启动，或该组确实没在消费）时返回 0。
func (s *Sampler) LastConsumeMs(gt GroupTopic) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastConsume[gt]
}

// reduceMinute 把属于 minute 这一分钟的样本归约成一个 MinutePoint。
// 归约规则见文件头注释——gauge 取最大，计数器取末值 + 窗口速率峰值。
func (s *Sampler) reduceMinute(minute int64) MinutePoint {
	mp := MinutePoint{TsMs: minute}
	sms := s.samples()
	var prev *Sample
	for i := range sms {
		sm := sms[i]
		if sm.TsMs/60000*60000 != minute {
			// 不在本分钟内，但仍要留作下一个样本的速率前驱
			prev = &sms[i]
			continue
		}
		if prev != nil {
			if r := rate(*prev, sm); r > mp.QPSPeak {
				mp.QPSPeak = r
			}
		}
		if sm.Pending > mp.PendingMax {
			mp.PendingMax = sm.Pending
		}
		if sm.Inflight > mp.InflightMax {
			mp.InflightMax = sm.Inflight
		}
		if sm.DelayDepth > mp.DelayMax {
			mp.DelayMax = sm.DelayDepth
		}
		mp.WrittenEnd = sm.Written
		mp.DLQEnd = sm.DLQ
		prev = &sms[i]
	}
	return mp
}

// persist 把一个分钟点写入 Pebble。
//
// 复制豁免（batch③ 刻意留痕）：metric/ 是本节点可观测数据，绕过 Replicator
// 直连 store——三节点各采各的，复制会同键互覆（MetricKey 只带 tsMs，无节点
// 维度）；快照追齐（batch④）可能混入他节点历史点，可观测数据可接受，不做过滤。
func (s *Sampler) persist(mp MinutePoint) error {
	raw, err := json.Marshal(mp)
	if err != nil {
		return fmt.Errorf("编码时序点 %d: %w", mp.TsMs, err)
	}
	b := s.st.NewBatch()
	b.Set(store.MetricKey(mp.TsMs), raw)
	if err := s.st.Apply(b); err != nil {
		return fmt.Errorf("提交时序点 %d: %w", mp.TsMs, err)
	}
	return nil
}

// History 读取 fromMs 起的全部分钟点（时间升序）。fromMs<=0 表示不限下界。
func (s *Sampler) History(fromMs int64) ([]Point, error) {
	lower := []byte(store.MetricPrefix)
	if fromMs > 0 {
		lower = store.MetricKey(fromMs)
	}
	upper := store.PrefixUpperBound([]byte(store.MetricPrefix))
	out := []Point{}
	err := s.st.Scan(lower, upper, 0, func(k, v []byte) (bool, error) {
		var mp MinutePoint
		if err := json.Unmarshal(v, &mp); err != nil {
			// 坏条目跳过而不是整趟失败：一条解不开的记录不该让整张历史曲线消失
			s.logger.Warn("时序点解码失败，跳过", "key", fmt.Sprintf("%q", k), "err", err)
			return true, nil
		}
		out = append(out, Point{TsMs: mp.TsMs, QPS: mp.QPSPeak, Pending: mp.PendingMax,
			DLQ: mp.DLQEnd, Inflight: mp.InflightMax, DelayDepth: mp.DelayMax})
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描时序点: %w", err)
	}
	return out, nil
}

// expire 删除 cutoffMs 之前的全部分钟点（与 retention 同样用 DeleteRange，
// 一条区间墓碑顶掉逐条删除）。
//
// 复制豁免同 persist：metric/ 是本节点可观测数据，刻意绕过 Replicator——
// 三节点各采各的，复制会同键互覆；过期删除也只在本地发生。
func (s *Sampler) expire(cutoffMs int64) error {
	b := s.st.NewBatch()
	b.DeleteRange(store.MetricKey(0), store.MetricKey(cutoffMs))
	if err := s.st.Apply(b); err != nil {
		return fmt.Errorf("提交时序过期删除 (cutoff=%d): %w", cutoffMs, err)
	}
	return nil
}
