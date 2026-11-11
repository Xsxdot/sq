// 职责（基准文件，卡 SQ4.1 S4）：检查点模式（间隔 1s，后台循环在跑）下的
// ack 与取件基准，与 bench_test.go 的逐次复制档同形，预置 1k/10k 在外 inflight。
// 单独成文件：bench_test.go 只用现有 API、可原样在旧版本上跑基线。
//
// 边界：只跑基准，不含断言测试。
package deliver

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
)

// toCheckpoint 把基准 Deliverer 换成同一存储上的检查点模式实例并起循环。
func toCheckpoint(b *testing.B, d *Deliverer) *Deliverer {
	b.Helper()
	c := New(d.rep, d.rt, d.st, d.mt, d.pr, slog.Default(), WithCheckpointInterval(time.Second))
	c.Start(context.Background())
	b.Cleanup(func() { c.Stop(context.Background()) })
	return c
}

func BenchmarkAckParallelStandingCheckpoint(b *testing.B) {
	for _, sz := range standingSizes {
		b.Run(sz.name, func(b *testing.B) {
			d := toCheckpoint(b, newBenchDeliverer(b, b.N, true))
			preloadStanding(b, d, "t-ack", sz.n)
			if _, err := d.Ack(context.Background(), "g-bench", "t-ack", 0, 1<<62, 1); err != nil {
				b.Fatalf("预热 ack: %v", err)
			}
			var next atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					off := next.Add(1) - 1
					ok, err := d.Ack(context.Background(), "g-bench", "t-ack", 0, off, 1)
					if err != nil || !ok {
						b.Fatalf("ack off=%d: ok=%v err=%v", off, ok, err)
					}
				}
			})
		})
	}
}

func BenchmarkReceiveStandingInflightCheckpoint(b *testing.B) {
	for _, sz := range standingSizes {
		b.Run(sz.name, func(b *testing.B) {
			d := toCheckpoint(b, newBenchDeliverer(b, 0, false))
			const topic = "t-recv"
			for i := 0; i < b.N; i++ {
				if _, err := d.pr.Append(context.Background(), &core.Message{Topic: topic, Body: []byte("x")}); err != nil {
					b.Fatalf("预写消息: %v", err)
				}
			}
			preloadStanding(b, d, topic, sz.n)
			if _, err := d.Ack(context.Background(), "g-bench", topic, 0, 1<<62, 1); err != nil {
				b.Fatalf("预热: %v", err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msgs, err := d.Receive(context.Background(), "g-bench", topic, 0, 1, time.Hour, 0, AllPass)
				if err != nil || len(msgs) != 1 {
					b.Fatalf("receive #%d: n=%d err=%v", i, len(msgs), err)
				}
			}
		})
	}
}
