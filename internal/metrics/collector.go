// collector.go: 把 Collect 的快照适配成 Prometheus Collector，并组装进程级 Registry。
//
// 职责：
//   - 抓取时调用 Collect，翻译为带标签的 gauge/counter
//   - NewRegistry 一站式装配：Go/process 采集器、状态 Collector、
//     store.Apply 耗时直方图（挂接包级钩子）
//
// 边界：
//   - NewRegistry 写 store.OnApplyObserve 包级钩子，进程内只可调用一次
//     （装配期契约，见 store 侧注释）
package metrics

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

var (
	descTopics = prometheus.NewDesc("sq_topics", "已注册 topic 数", nil, nil)
	descGroups = prometheus.NewDesc("sq_groups", "已注册消费组数", nil, nil)
	descDelay  = prometheus.NewDesc("sq_delay_depth", "延时暂存区未到期条数", nil, nil)
	descWrite  = prometheus.NewDesc("sq_topic_messages_written_total",
		"topic 累计写入消息条数（各队列 offset 计数器之和）", []string{"topic"}, nil)
	descPending = prometheus.NewDesc("sq_group_pending_messages",
		"消费组视角已写入未拉取的消息数", []string{"group", "topic"}, nil)
	descInflight = prometheus.NewDesc("sq_group_inflight_messages",
		"已投递未确认的消息数", []string{"group", "topic"}, nil)
)

// Collector 抓取期状态采集器。
type Collector struct {
	st     *store.Store
	mt     *meta.Meta
	logger *slog.Logger
}

// NewCollector 构造状态 Collector。
func NewCollector(st *store.Store, mt *meta.Meta, logger *slog.Logger) *Collector {
	return &Collector{st: st, mt: mt, logger: logger.With("mod", "metrics")}
}

// Describe 实现 prometheus.Collector。
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descTopics
	ch <- descGroups
	ch <- descDelay
	ch <- descWrite
	ch <- descPending
	ch <- descInflight
}

// Collect 实现 prometheus.Collector：每次抓取现算。失败时上报 invalid metric
// （抓取端能看到错误）并记 Error 日志，不让一次 store 故障 panic 掉抓取协程。
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	s, err := Collect(c.st, c.mt)
	if err != nil {
		c.logger.Error("metrics 采集失败", "err", err)
		ch <- prometheus.NewInvalidMetric(descTopics, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(descTopics, prometheus.GaugeValue, float64(s.Topics))
	ch <- prometheus.MustNewConstMetric(descGroups, prometheus.GaugeValue, float64(s.Groups))
	ch <- prometheus.MustNewConstMetric(descDelay, prometheus.GaugeValue, float64(s.DelayDepth))
	for topic, n := range s.Written {
		ch <- prometheus.MustNewConstMetric(descWrite, prometheus.CounterValue, float64(n), topic)
	}
	for gt, n := range s.Pending {
		ch <- prometheus.MustNewConstMetric(descPending, prometheus.GaugeValue, float64(n), gt.Group, gt.Topic)
	}
	for gt, n := range s.Inflight {
		ch <- prometheus.MustNewConstMetric(descInflight, prometheus.GaugeValue, float64(n), gt.Group, gt.Topic)
	}
}

// NewRegistry 组装进程级指标注册表并挂接 store.Apply 耗时直方图。
// 只能在装配阶段调用一次（会写 store.OnApplyObserve 包级钩子）。
func NewRegistry(st *store.Store, mt *meta.Meta, logger *slog.Logger) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(NewCollector(st, mt, logger))
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "sq_store_apply_duration_seconds",
		Help: "store 批次提交耗时（含 fsync）",
		// 0.1ms 起倍增 14 档（~0.82s 封顶）：同步刷盘常态在 0.5~10ms，
		// 尾部预算给磁盘抖动
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
	})
	reg.MustRegister(h)
	store.OnApplyObserve = func(d time.Duration) { h.Observe(d.Seconds()) }
	logger.Info("metrics registry 已装配", "mod", "metrics")
	return reg
}
