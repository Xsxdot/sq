//go:build !unix

// 非 unix 平台的磁盘探测降级（M2 起的既有边界：水位保护仅支持 unix）。
package sysinfo

import "errors"

// DiskUsage 在非 unix 平台恒返回错误，调用方据此降级为「不知道」。
func DiskUsage(dir string) (Disk, error) {
	return Disk{}, errors.New("当前平台不支持磁盘用量探测")
}
