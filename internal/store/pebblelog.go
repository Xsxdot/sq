// pebblelog.go: Pebble 日志接口到项目 slog 的桥接。
//
// 职责：
//   - 实现 pebble.Logger（Infof/Errorf/Fatalf），把 Pebble 内部日志接入项目
//     统一的 *slog.Logger，取代它默认写 stderr 的 log.Printf 行为
//
// 边界：
//   - 不做采样、不做限流、不解析日志内容：级别映射之外原样转发
//   - 级别映射策略与 Fatalf 的进程终止契约见下方各方法注释
package store

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/cockroachdb/pebble/v2"
)

// 编译期断言：签名一旦与 Pebble 的接口对不上，这里就报错，而不是等到
// pebble.Options{Logger: ...} 那行才暴露。
var _ pebble.Logger = (*pebbleLogger)(nil)

// pebbleLogger 把 pebble.Logger 的三个方法桥到 slog。
// 零值不可用（l 为 nil 会 panic），必须经 newPebbleLogger 构造。
type pebbleLogger struct {
	l *slog.Logger
}

// newPebbleLogger 基于 base 构造适配器，返回值挂到 pebble.Options.Logger 上。
//
// base 通常是已经 With("mod","store") 过的实例；这里再补一对 src=pebble，
// 使 Pebble 自身的输出与 store 包的语义日志在同一份日志里可区分、可过滤。
// base 为 nil 时 slog.With 会 panic——调用方（store.Open）保证非 nil。
func newPebbleLogger(base *slog.Logger) *pebbleLogger {
	return &pebbleLogger{l: base.With("src", "pebble")}
}

// Infof 转发为 Debug 而非 Info。
//
// Pebble 的 Info 级输出是 WAL 扫描、compaction 明细一类的运维细节，每次 open
// 都会打若干行；映射成 Info 只会把「直写 stderr 的刷屏」换成「写进项目日志的
// 刷屏」，问题没解决只是换了出口。需要这些细节时把日志级别调到 debug 即可。
func (p *pebbleLogger) Infof(format string, args ...interface{}) {
	p.l.Debug(fmt.Sprintf(format, args...))
}

// Errorf 一一对应到 slog 的 Error。
func (p *pebbleLogger) Errorf(format string, args ...interface{}) {
	p.l.Error(fmt.Sprintf(format, args...))
}

// Fatalf 必须终止进程，这是 Pebble 的契约：它只在无法安全继续时（如检测到
// 数据损坏）调用，默认实现就是 log.Fatalf（os.Exit(1)）。
//
// 为什么不降级、不 panic：
//   - 降级成一条 Error 后返回，会让本该立刻停下的进程带着已知损坏的状态继续
//     跑，比崩溃危险得多；
//   - 改成 panic 则可能被上层 recover 吞掉，同样破坏「不可继续」的语义。
//
// 先打一条 Error，保证这条致命信息进入项目的日志通道（而不是只留在 stderr），
// 再 os.Exit(1)。
func (p *pebbleLogger) Fatalf(format string, args ...interface{}) {
	p.l.Error(fmt.Sprintf(format, args...), "fatal", true)
	os.Exit(1)
}
