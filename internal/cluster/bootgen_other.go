//go:build !linux && !darwin

package cluster

// machineBootGen 在没有已知机器世代来源的平台上恒报「不支持」。
//
// 这不是缺陷而是刻意的保守方向：读不到世代 = 无法证明机器没重启过 =
// 一律按「可能重启过」处理，最坏结果是多走一次重入编排（安全），
// 绝不会误判成「本地日志可信」（危险）。
func machineBootGen() (string, error) {
	return "", errBootGenUnsupported
}
