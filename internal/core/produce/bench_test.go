// Package produce 实现消息写入路径：队列选择、offset 分配、落盘、长轮询唤醒。
//
// 职责（基准文件）：
//   - BenchmarkAppendParallel 量化"全局锁跨 fsync 导致 group commit 失效"的写吞吐基线
//   - BenchmarkAppendQueueSweep 复现吞吐随队列数近似线性放大（spec §1）
//   - BenchmarkAppendBatch32 量化 AppendBatch 批量一次 fsync 的收益
//
// 边界：
//   - 只跑基准，不包含断言测试
//   - fixture 使用真实 store（sync 写盘），基准量的就是 fsync 合并能力
package produce

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

// 并发写吞吐基准：真实 store（fsync=sync）+ 多队列 topic + RunParallel。
// 意义：量化"全局锁跨 fsync 导致 group commit 失效"（produce.go 锁注释记录的
// 瓶颈）。必须先在旧代码上跑出基线，改锁后复跑对比——没有前后两组数字，
// "变快了"只是主张不是事实。
func BenchmarkAppendParallel(b *testing.B) {
	// fixture 照 newTestProducer（produce_test.go:25）改造：*testing.B、
	// store.Open(dir, true /*syncWrites——基准量的就是 fsync 合并*/, ...)、
	// meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 16 /*16 队列，给并发留出跨队列并行度*/, 16, ...)
	p, _ := newBenchProducer(b, b.TempDir())
	// 固定 62B 载荷（命名如实标注），不随改锁调整，保证 Task 8 A/B 对比公平。
	body := []byte("benchmark-payload-62B.........................................")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := &core.Message{Topic: "t-bench", Body: body}
			if _, err := p.Append(context.Background(), m); err != nil {
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
	mt, err := meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 16, 16, slog.Default())
	if err != nil {
		b.Fatalf("meta: %v", err)
	}
	return New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, mt, slog.Default()), st
}

// BenchmarkAppendQueueSweep 固定并发（由 -cpu/GOMAXPROCS 决定）只变队列数，
// 复现「写吞吐随队列数近似线性放大」的结论（spec §1）。改锁前后各跑一轮
// 即可量化 group commit 解锁对低队列数配置的收益。
func BenchmarkAppendQueueSweep(b *testing.B) {
	body := []byte("benchmark-payload-62B.........................................")
	for _, queues := range []uint32{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("q%d", queues), func(b *testing.B) {
			st, err := store.Open(b.TempDir(), true, slog.Default())
			if err != nil {
				b.Fatalf("store: %v", err)
			}
			b.Cleanup(func() { st.Close() })
			mt, err := meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, queues, 16, slog.Default())
			if err != nil {
				b.Fatalf("meta: %v", err)
			}
			p := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, mt, slog.Default())
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := p.Append(context.Background(), &core.Message{Topic: "t-sweep", Body: body}); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkAppendBatch32 批量写入基准：每次迭代 = 一批 32 条一次 fsync。
// 换算 msg/s 时乘 32（ns/op 是「每批」耗时，不是每条）。
func BenchmarkAppendBatch32(b *testing.B) {
	p, _ := newBenchProducer(b, b.TempDir())
	body := []byte("benchmark-payload-62B.........................................")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msgs := make([]*core.Message, 32)
			for i := range msgs {
				msgs[i] = &core.Message{Topic: "t-batch", Body: body}
			}
			if _, err := p.AppendBatch(context.Background(), msgs); err != nil {
				b.Fatal(err)
			}
		}
	})
}
