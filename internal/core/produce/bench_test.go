// Package produce 实现消息写入路径：队列选择、offset 分配、落盘、长轮询唤醒。
//
// 职责（基准文件）：
//   - BenchmarkAppendParallel 量化"全局锁跨 fsync 导致 group commit 失效"的写吞吐基线
//
// 边界：
//   - 只跑基准，不包含断言测试
//   - fixture 使用真实 store（sync 写盘），基准量的就是 fsync 合并能力
package produce

import (
	"log/slog"
	"testing"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// 并发写吞吐基准：真实 store（fsync=sync）+ 多队列 topic + RunParallel。
// 意义：量化"全局锁跨 fsync 导致 group commit 失效"（produce.go 锁注释记录的
// 瓶颈）。必须先在旧代码上跑出基线，改锁后复跑对比——没有前后两组数字，
// "变快了"只是主张不是事实。
func BenchmarkAppendParallel(b *testing.B) {
	// fixture 照 newTestProducer（produce_test.go:25）改造：*testing.B、
	// store.Open(dir, true /*syncWrites——基准量的就是 fsync 合并*/, ...)、
	// meta.New(st, true, 16 /*16 队列，给并发留出跨队列并行度*/, 16, ...)
	p, _ := newBenchProducer(b, b.TempDir())
	body := []byte("benchmark-payload-256B........................................")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := &core.Message{Topic: "t-bench", Body: body}
			if _, err := p.Append(m); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// newBenchProducer 基准专用 fixture：sync 写盘（基准量的就是 fsync 合并），
// 16 队列 topic 给并发留出跨队列并行度。
func newBenchProducer(b *testing.B, dir string) (*Producer, *store.Store) {
	b.Helper()
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		b.Fatalf("store: %v", err)
	}
	b.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 16, 16, slog.Default())
	if err != nil {
		b.Fatalf("meta: %v", err)
	}
	return New(st, mt, slog.Default()), st
}
