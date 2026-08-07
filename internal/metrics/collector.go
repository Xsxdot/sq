// collector.go: 把 Collect 的快照适配成 Prometheus Collector，并组装进程级 Registry。
//
// 职责：
//   - 抓取时调用 Collect，翻译为带标签的 gauge/counter
//   - 产出系统类指标（磁盘用量、数据目录占用、拒写开关），与控制台
//     /admin/system 共用同一个 sysinfo.Reporter
//   - 产出事务回查计数（TxnStats）与客户端连接数（ConnCounter），tx/conns
//     允许为 nil——测试与降级场景跳过对应指标
//   - NewRegistry 一站式装配：Go/process 采集器、状态 Collector、
//     store.Apply 耗时直方图（挂接包级钩子）
//
// 边界：
//   - NewRegistry 写 store.OnApplyObserve 包级钩子，进程内只可调用一次
//     （装配期契约，见 store 侧注释）
//   - 不出内存指标：GoCollector 已提供 go_memstats_*，重复暴露会让告警
//     规则出现两个口径不同的来源
package metrics

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
	"github.com/xushixin/sq/internal/sysinfo"
)

// TxnStats 事务回查统计的只读视图（*txn.Manager 天然实现）。
//
// 边界：只暴露累计计数，不透出回查调度细节；为 nil 时 Collector 跳过
// 事务类指标。
type TxnStats interface {
	ChecksTotal() uint64
	DroppedTotal() uint64
}

// ConnCounter 客户端连接数的只读视图（*rpc.Server 天然实现）。
//
// 口径：已完成 Settings 协商的客户端连接数（即 Telemetry 长流数，
// 见 rpc.Server.ConnectionCount）；为 nil 时 Collector 跳过连接数指标。
type ConnCounter interface {
	ConnectionCount() int
}

var (
	descTopics = prometheus.NewDesc("sq_topics", "已注册 topic 数", nil, nil)
	descGroups = prometheus.NewDesc("sq_groups", "已注册消费组数", nil, nil)
	descDelay  = prometheus.NewDesc("sq_delay_depth", "延时暂存区未到期条数", nil, nil)
	descHalf   = prometheus.NewDesc("sq_half_messages", "半消息暂存区待回查条数", nil, nil)
	descWrite  = prometheus.NewDesc("sq_topic_messages_written_total",
		"topic 累计写入消息条数（各队列 offset 计数器之和）", []string{"topic"}, nil)
	descPending = prometheus.NewDesc("sq_group_pending_messages",
		"消费组视角已写入未拉取的消息数", []string{"group", "topic"}, nil)
	descInflight = prometheus.NewDesc("sq_group_inflight_messages",
		"已投递未确认的消息数", []string{"group", "topic"}, nil)
	descTxnChecks = prometheus.NewDesc("sq_txn_checks_total",
		"事务回查累计排期次数（含下发失败的轮次）", nil, nil)
	descTxnDropped = prometheus.NewDesc("sq_txn_dropped_total",
		"事务回查超限累计丢弃条数", nil, nil)
	descConns = prometheus.NewDesc("sq_connections",
		"已完成 Settings 协商的客户端连接数", nil, nil)
	descDiskUsed = prometheus.NewDesc("sq_disk_used_percent",
		"数据目录所在文件系统已用百分比（与 df 同口径）", nil, nil)
	descDiskFree = prometheus.NewDesc("sq_disk_free_bytes",
		"数据目录所在文件系统的非特权可用字节数", nil, nil)
	descDataDir = prometheus.NewDesc("sq_data_dir_bytes",
		"数据目录占用字节数（TTL 缓存，最多滞后 60s）", nil, nil)
	descBlocked = prometheus.NewDesc("sq_write_blocked",
		"磁盘水位拒写开关：1=拒写保读中，0=正常写入", nil, nil)
)

// Collector 抓取期状态采集器。
type Collector struct {
	st     *store.Store
	mt     *meta.Meta
	sys    *sysinfo.Reporter
	tx     TxnStats
	conns  ConnCounter
	logger *slog.Logger
}

// NewCollector 构造状态 Collector。sys/tx/conns 为 nil 时跳过各自负责的
// 指标组（测试可只关心业务指标；main 装配时恒非 nil）。
func NewCollector(st *store.Store, mt *meta.Meta, sys *sysinfo.Reporter, tx TxnStats, conns ConnCounter, logger *slog.Logger) *Collector {
	return &Collector{st: st, mt: mt, sys: sys, tx: tx, conns: conns, logger: logger.With("mod", "metrics")}
}

// Describe 实现 prometheus.Collector。
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descTopics
	ch <- descGroups
	ch <- descDelay
	ch <- descHalf
	ch <- descWrite
	ch <- descPending
	ch <- descInflight
	ch <- descTxnChecks
	ch <- descTxnDropped
	ch <- descConns
	ch <- descDiskUsed
	ch <- descDiskFree
	ch <- descDataDir
	ch <- descBlocked
}

// Collect 实现 prometheus.Collector：每次抓取现算。业务指标采集失败时上报
// invalid metric（抓取端能看到错误）并记 Error 日志，不让一次 store 故障
// panic 掉抓取协程；系统类指标不受此影响，恒会产出。
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	// 系统指标必须先出：下面的业务采集失败会直接 return，而磁盘满导致 Pebble
	// 写不下去时，恰恰是 sq_write_blocked 最需要被看见的时刻——放在末尾会让
	// 告警侧看到 absent() 而不是 1。
	c.collectSystem(ch)
	s, err := Collect(c.st, c.mt)
	if err != nil {
		c.logger.Error("metrics 采集失败", "err", err)
		ch <- prometheus.NewInvalidMetric(descTopics, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(descTopics, prometheus.GaugeValue, float64(s.Topics))
	ch <- prometheus.MustNewConstMetric(descGroups, prometheus.GaugeValue, float64(s.Groups))
	ch <- prometheus.MustNewConstMetric(descDelay, prometheus.GaugeValue, float64(s.DelayDepth))
	ch <- prometheus.MustNewConstMetric(descHalf, prometheus.GaugeValue, float64(s.HalfDepth))
	for topic, n := range s.Written {
		ch <- prometheus.MustNewConstMetric(descWrite, prometheus.CounterValue, float64(n), topic)
	}
	for gt, n := range s.Pending {
		ch <- prometheus.MustNewConstMetric(descPending, prometheus.GaugeValue, float64(n), gt.Group, gt.Topic)
	}
	for gt, n := range s.Inflight {
		ch <- prometheus.MustNewConstMetric(descInflight, prometheus.GaugeValue, float64(n), gt.Group, gt.Topic)
	}
	// 事务计数与连接数都是纯读的外部依赖（txn.Manager/rpc.Server 的原子计数），
	// 无日志需求；nil 时整体跳过对应指标——absent 比恒 0 对告警更诚实
	if c.tx != nil {
		ch <- prometheus.MustNewConstMetric(descTxnChecks, prometheus.CounterValue, float64(c.tx.ChecksTotal()))
		ch <- prometheus.MustNewConstMetric(descTxnDropped, prometheus.CounterValue, float64(c.tx.DroppedTotal()))
	}
	if c.conns != nil {
		ch <- prometheus.MustNewConstMetric(descConns, prometheus.GaugeValue, float64(c.conns.ConnectionCount()))
	}
}

// collectSystem 产出系统类指标。
//
// 注意：
//   - 这里刻意不出内存指标：GoCollector 已经提供 go_memstats_*，再暴露一份
//     只会让告警规则出现两个口径不同的来源。控制台的 /admin/system 之所以
//     仍给内存，是为了「不接 Prometheus 也能看一眼」这条产品承诺
//   - sq_write_blocked 无论探测成功与否都要出：一个会消失的 gauge 对告警
//     而言比恒为 0 的更危险（absent 与 false 混在一起）
func (c *Collector) collectSystem(ch chan<- prometheus.Metric) {
	if c.sys == nil {
		return
	}
	snap := c.sys.Snapshot()
	blocked := 0.0
	if snap.WriteBlocked {
		blocked = 1
	}
	ch <- prometheus.MustNewConstMetric(descBlocked, prometheus.GaugeValue, blocked)
	if snap.Disk != nil {
		ch <- prometheus.MustNewConstMetric(descDiskUsed, prometheus.GaugeValue, snap.Disk.UsedPercent)
		ch <- prometheus.MustNewConstMetric(descDiskFree, prometheus.GaugeValue, float64(snap.Disk.FreeBytes))
	}
	if snap.DataDirBytes != nil {
		ch <- prometheus.MustNewConstMetric(descDataDir, prometheus.GaugeValue, float64(*snap.DataDirBytes))
	}
}

// NewRegistry 组装进程级指标注册表并挂接 store.Apply 耗时直方图。
// 只能在装配阶段调用一次（会写 store.OnApplyObserve 包级钩子）。
// tx/conns 允许为 nil（测试与降级场景跳过事务/连接指标）。
func NewRegistry(st *store.Store, mt *meta.Meta, sys *sysinfo.Reporter, tx TxnStats, conns ConnCounter, logger *slog.Logger) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(NewCollector(st, mt, sys, tx, conns, logger))
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
