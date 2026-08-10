//go:build linux

package cluster

import (
	"fmt"
	"os"
	"strings"
)

// bootIDPath 是 Linux 内核暴露的本次启动唯一标识。每次开机重新生成，
// 开机期间恒定——正是「机器有没有重启过」的权威来源。
//
// 容器语义正好是我们要的：容器内读到的是**宿主机**内核的值。容器重启
// 而宿主没重启 → 值不变 → 判定页缓存完好，正确；容器迁移到另一台宿主
// → 值变了 → 保守处理，也正确。
const bootIDPath = "/proc/sys/kernel/random/boot_id"

// machineBootGen 读取本机的机器世代（Linux 实现）。
func machineBootGen() (string, error) {
	data, err := os.ReadFile(bootIDPath)
	if err != nil {
		return "", fmt.Errorf("cluster: 读 %s: %w", bootIDPath, err)
	}
	v := strings.TrimSpace(string(data))
	if v == "" {
		return "", fmt.Errorf("cluster: %s 内容为空", bootIDPath)
	}
	return v, nil
}
