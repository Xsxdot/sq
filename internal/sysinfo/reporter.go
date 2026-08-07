// reporter.go: 运行态快照的产出者，含数据目录大小的 TTL 缓存。
//
// 职责：
//   - 持有采集所需的全部依赖（数据目录、水位线、拒写开关、进程启动时刻）
//   - Snapshot 一次性产出磁盘 / 数据目录 / Go 运行时 / 协程 / 运行时长
//
// 边界：
//   - 不起后台 goroutine：调用方（metrics 抓取、admin 请求）触发采集，
//     昂贵的目录遍历靠 TTL 缓存兜住，不需要额外的生命周期管理
//   - 不做任何判定：是否拒写由 retention 决定并写进 writeBlocked，这里只读
package sysinfo

import (
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// dirSizeTTL 数据目录大小的缓存有效期。
//
// 为什么要缓存：目录大小只能靠递归遍历得到，而控制台每 5 秒轮询一次、
// Prometheus 也在按自己的间隔抓取；每次都遍历一遍，等于把一个诊断读数
// 变成一份持续的 I/O 负担。60 秒意味着这个数最多滞后一分钟 —— 对
// 「数据目录涨到多大了」这类判断完全够用。
const dirSizeTTL = 60 * time.Second

// Reporter 运行态读数的产出者。并发安全。
type Reporter struct {
	dataDir      string
	watermarkPct int
	writeBlocked *atomic.Bool
	startedAt    time.Time
	logger       *slog.Logger

	mu       sync.Mutex
	dirBytes int64
	dirAt    time.Time
	dirOK    bool
}

// New 构造 Reporter。
//
// 参数：
//   - dataDir: 数据目录，既是磁盘探测目标也是占用统计目标
//   - watermarkPct: 拒写水位线，0 表示水位保护关闭（原样报给调用方）
//   - writeBlocked: retention 维护的拒写开关；为 nil 时一律视为未拒写
//
// 注意：
//   - startedAt 取构造时刻。本对象在 main 装配早期创建，与进程启动只差毫秒级
func New(dataDir string, watermarkPct int, writeBlocked *atomic.Bool, logger *slog.Logger) *Reporter {
	r := &Reporter{
		dataDir:      dataDir,
		watermarkPct: watermarkPct,
		writeBlocked: writeBlocked,
		startedAt:    time.Now(),
		logger:       logger.With("mod", "sysinfo"),
	}
	r.logger.Info("sysinfo 采集器就绪",
		"data_dir", dataDir, "watermark_pct", watermarkPct, "dir_size_ttl", dirSizeTTL.String())
	return r
}

// WriteBlocked 返回当前是否处于拒写保读状态。开关未装配时返回 false。
func (r *Reporter) WriteBlocked() bool {
	return r.writeBlocked != nil && r.writeBlocked.Load()
}

// Snapshot 采集一次运行态读数。
//
// 返回：
//   - Snapshot: 磁盘/目录占用采不到时对应字段为 nil（表示「不知道」），
//     运行时读数恒可用 —— 磁盘探测失败不该让整个端点变哑
//
// 注意：
//   - 会调用 runtime.ReadMemStats，有一次极短的 STW。控制台 5 秒一次、
//     Prometheus 按抓取间隔一次，这个频率下开销可忽略
func (r *Reporter) Snapshot() Snapshot {
	s := Snapshot{
		WatermarkPercent: r.watermarkPct,
		WriteBlocked:     r.WriteBlocked(),
		Goroutines:       runtime.NumGoroutine(),
		UptimeSeconds:    int64(time.Since(r.startedAt).Seconds()),
		DataDirBytes:     r.dataDirBytes(),
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.GoHeapInuseBytes = ms.HeapInuse
	s.GoSysBytes = ms.Sys

	if d, err := DiskUsage(r.dataDir); err != nil {
		// Warn 而非 Error：非 unix 平台本就探不到，这是已声明的边界不是故障
		r.logger.Warn("磁盘用量探测失败，本次快照缺磁盘读数", "dir", r.dataDir, "err", err)
	} else {
		s.Disk = &d
	}
	return s
}

// dataDirBytes 返回数据目录占用，带 TTL 缓存；从未成功采到过时返回 nil。
func (r *Reporter) dataDirBytes() *int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dirOK && time.Since(r.dirAt) < dirSizeTTL {
		v := r.dirBytes
		return &v
	}
	n, err := dirSize(r.dataDir)
	if err != nil {
		r.logger.Warn("数据目录占用统计失败", "dir", r.dataDir, "err", err, "has_stale", r.dirOK)
		if r.dirOK {
			// 保留上一次的值：一个滞后的数远比「不知道」有用，
			// 前端也不会因为一次瞬时失败而闪成占位符
			v := r.dirBytes
			return &v
		}
		return nil
	}
	if !r.dirOK || n != r.dirBytes {
		r.logger.Debug("数据目录占用刷新", "dir", r.dataDir, "bytes", n, "prev_bytes", r.dirBytes)
	}
	r.dirBytes, r.dirAt, r.dirOK = n, time.Now(), true
	return &n
}
