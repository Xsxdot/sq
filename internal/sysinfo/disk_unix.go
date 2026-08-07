//go:build unix

// 磁盘用量探测（unix：syscall.Statfs，darwin/linux 字段一致）。
package sysinfo

import (
	"errors"
	"syscall"
)

// DiskUsage 返回 dir 所在文件系统的容量读数。
//
// 参数：
//   - dir: 探测目标目录（用它所在的文件系统，不是这个目录本身的大小）
//
// 返回：
//   - Disk: 总量/可用/已用百分比
//   - error: dir 不存在、statfs 失败，或文件系统块数为 0（伪文件系统）
//
// 注意：
//   - 可用量用 Bavail（非特权可用块）计算，与 df 口径一致；用 Bfree 会把
//     只有 root 能动的保留块算成可用，导致水位永远差几个百分点触发不了
func DiskUsage(dir string) (Disk, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return Disk{}, err
	}
	// 块数为 0 的多是伪文件系统，算不出任何有意义的容量。必须返回错误而不是
	// 一组零值：零值会经 Snapshot 变成一份「合法读数」，控制台显示成
	// 「0.0% / 可用 0 B」，正好违背本包「不知道就是 nil，不是 0」的约定。
	if fs.Blocks == 0 {
		return Disk{}, errors.New("文件系统块数为 0，无法计算容量")
	}
	bsize := uint64(fs.Bsize)
	freePct := float64(fs.Bavail) * 100 / float64(fs.Blocks)
	return Disk{
		TotalBytes:  fs.Blocks * bsize,
		FreeBytes:   uint64(fs.Bavail) * bsize,
		UsedPercent: 100 - freePct,
	}, nil
}
