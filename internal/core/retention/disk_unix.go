//go:build unix

// 磁盘用量探测（unix：syscall.Statfs，darwin/linux 字段一致）。
package retention

import "syscall"

// diskUsedPercent 返回 dir 所在文件系统的已用空间百分比 [0,100]。
func diskUsedPercent(dir string) (float64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return 0, err
	}
	if fs.Blocks == 0 {
		return 0, nil
	}
	// 用 Bavail（非特权可用块）计算，与 df 口径一致
	free := float64(fs.Bavail) * 100 / float64(fs.Blocks)
	return 100 - free, nil
}
