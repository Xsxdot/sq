//go:build darwin

package cluster

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// machineBootGen 读取本机的机器世代（darwin 实现）。
//
// darwin 没有 boot_id，用内核记录的开机时刻代替：它在一次开机内恒定、
// 重启后必变，语义与 Linux 的 boot_id 等价。精度到微秒，同一台机器两次
// 开机撞上同一微秒不可能。
func machineBootGen() (string, error) {
	tv, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		return "", fmt.Errorf("cluster: 读 sysctl kern.boottime: %w", err)
	}
	return fmt.Sprintf("boottime-%d.%06d", tv.Sec, tv.Usec), nil
}
