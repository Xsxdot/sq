//go:build unix

// 磁盘用量探测（unix：syscall.Statfs，darwin/linux 字段一致）。
package sysinfo

import "syscall"

// DiskUsage 返回 dir 所在文件系统的容量读数。
//
// 参数：
//   - dir: 探测目标目录（用它所在的文件系统，不是这个目录本身的大小）
//
// 返回：
//   - Disk: 总量/可用/已用百分比
//   - error: dir 不存在或 statfs 失败
//
// 注意：
//   - 可用量用 Bavail（非特权可用块）计算，与 df 口径一致；用 Bfree 会把
//     只有 root 能动的保留块算成可用，导致水位永远差几个百分点触发不了
func DiskUsage(dir string) (Disk, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return Disk{}, err
	}
	if fs.Blocks == 0 {
		return Disk{}, nil
	}
	bsize := uint64(fs.Bsize)
	freePct := float64(fs.Bavail) * 100 / float64(fs.Blocks)
	return Disk{
		TotalBytes:  fs.Blocks * bsize,
		FreeBytes:   uint64(fs.Bavail) * bsize,
		UsedPercent: 100 - freePct,
	}, nil
}
