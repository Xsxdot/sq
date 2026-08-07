//go:build !unix

// 非 unix 平台的磁盘探测降级（M2 边界：水位保护仅支持 unix，其余平台禁用并告警）。
package retention

import "errors"

func diskUsedPercent(dir string) (float64, error) {
	return 0, errors.New("当前平台不支持磁盘水位检查")
}
