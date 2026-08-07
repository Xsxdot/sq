// Package sysinfo 提供进程与宿主机的运行态读数（磁盘用量、数据目录占用、
// Go 运行时内存、协程数、运行时长）。
//
// 职责：
//   - 磁盘探测：数据目录所在文件系统的总量/可用/已用百分比（与 df 同口径）
//   - 数据目录占用统计（递归遍历，带 TTL 缓存，见 reporter.go）
//   - 汇总成一个 Snapshot，供 retention（水位判定）、metrics（Prometheus
//     gauge）、admin（GET /admin/system）三方共用同一份事实
//
// 边界：
//   - 只读不改：本包不做任何拒写决策，拒写开关由 retention 写、本包只读出来报
//   - 不提供进程 RSS：Go 标准库没有可移植的 RSS 读法，强行给一个数会诱导
//     「Go 归还内存有延迟 → RSS 长期高位」被误读成内存泄漏。只给 Go 运行时
//     自己的口径（HeapInuse / Sys），Linux 上的真实 RSS 交给 /metrics 的
//     process 采集器
//   - 磁盘探测仅 unix 可用，其余平台返回错误（与 M2 的既有边界一致）
package sysinfo

import (
	"io/fs"
	"path/filepath"
)

// Disk 数据目录所在文件系统的容量读数。
type Disk struct {
	TotalBytes  uint64  `json:"total_bytes"`
	FreeBytes   uint64  `json:"free_bytes"`
	UsedPercent float64 `json:"used_percent"`
}

// Snapshot 一次采集得到的全部运行态读数。
//
// 指针字段表示「不知道」而不是「是 0」：磁盘探测失败与「磁盘是空的」是两件事，
// 前端据此显示占位符而不是一个误导性的 0（与 admin overview 里 qps 的处理一致）。
type Snapshot struct {
	// Disk 为 nil 表示探测失败或平台不支持
	Disk *Disk `json:"disk"`
	// WatermarkPercent 拒写水位线，0 表示水位保护关闭
	WatermarkPercent int `json:"watermark_percent"`
	// WriteBlocked 当前是否处于拒写保读状态
	WriteBlocked bool `json:"write_blocked"`
	// DataDirBytes 为 nil 表示尚未成功采到过
	DataDirBytes *int64 `json:"data_dir_bytes"`
	// GoHeapInuseBytes / GoSysBytes 是 Go 运行时口径，不是进程 RSS
	GoHeapInuseBytes uint64 `json:"go_heap_inuse_bytes"`
	GoSysBytes       uint64 `json:"go_sys_bytes"`
	Goroutines       int    `json:"goroutines"`
	UptimeSeconds    int64  `json:"uptime_seconds"`
}

// dirSize 递归累加 dir 下所有常规文件的大小，返回字节数。
//
// 注意：
//   - 单个条目读失败会被跳过而不是让整趟失败 —— retention 清理与 Pebble 压实
//     随时可能删掉刚枚举到的文件，那是常态不是故障；数据目录大小本就是个近似值
//   - dir 本身不存在时仍然返回错误（那是配置错误，必须暴露）
func dirSize(dir string) (int64, error) {
	if _, err := filepath.Abs(dir); err != nil {
		return 0, err
	}
	var total int64
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// 根目录本身出错要往上抛，子条目出错跳过
			if p == dir {
				return err
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}
