// bootgen.go 提供「机器世代」（boot generation）的读取。
//
// 职责：
//   - 给出一个在「一次开机期间恒定、机器重启后必变」的标识
//   - 统一 env 覆盖（测试用）与可注入 provider（单测用）两条旁路
//
// 边界：
//   - 不解释这个值的用途，也不做任何比较与判定——判定在 recovery.go
//   - 不落盘：读写 raft/bootgen 键是 raftstore 的事
//   - 平台实现分散在 bootgen_{linux,darwin,other}.go，本文件不含系统调用
//
// 为什么需要它：不干净关机之后，本地 raft 日志可不可信，取决于**页缓存
// 有没有丢**。Pebble 的 NoSync 写入走 write(2) 进页缓存，进程被 kill -9
// 之后数据一条不少；只有机器掉电/内核崩溃才会丢尾巴。机器世代就是「这台
// 机器有没有重启过」的可验证证据。
package cluster

import (
	"errors"
	"log/slog"
	"os"
)

// bootGenOverrideEnv 是机器世代的环境变量覆盖名。
//
// 仅供测试：进程级 e2e 起的是真 broker 进程，注不进 Go 函数，只能靠
// 环境变量模拟「机器重启过」。生产环境设置它会让安全门失效（真重启也
// 被判成没重启），因此一旦生效就打 Error 日志——这条不能只靠文档拦。
const bootGenOverrideEnv = "SQ_BOOTGEN_OVERRIDE"

// errBootGenUnsupported 是不支持机器世代读取的平台返回的错误。
var errBootGenUnsupported = errors.New("cluster: 本平台不支持机器世代读取")

// BootGenFunc 是机器世代的读取函数，供测试注入。
type BootGenFunc func() (string, error)

// resolveBootGen 解析本机当前的机器世代。
//
// 参数：
//   - fn: 注入的读取函数；nil 时用平台实现 machineBootGen
//   - lg: 日志器，用于 env 覆盖告警与读取失败告警
//
// 返回：
//   - 世代字符串与它是否可用
//
// 注意：读不到时返回 ("", false) 而**不是** ("", true)。这个区分是安全
// 关键——若把「读不到」当成一个值为空串的正常世代，那么两次都读不到就
// 会比较相等，进而被判成「机器没重启过、本地日志可信」，安全门当场失效。
func resolveBootGen(fn BootGenFunc, lg *slog.Logger) (string, bool) {
	if v := os.Getenv(bootGenOverrideEnv); v != "" {
		lg.Error("机器世代被环境变量覆盖，仅供测试——生产环境设置它会让不干净关机的安全门失效",
			"env", bootGenOverrideEnv, "value", v)
		return v, true
	}
	if fn == nil {
		fn = machineBootGen
	}
	v, err := fn()
	if err != nil {
		lg.Warn("机器世代读取失败，按「机器可能已重启」保守处理", "err", err)
		return "", false
	}
	if v == "" {
		lg.Warn("机器世代读取到空串，按「机器可能已重启」保守处理")
		return "", false
	}
	return v, true
}
