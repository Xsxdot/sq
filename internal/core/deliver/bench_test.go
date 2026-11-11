// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责（基准文件）：
//   - BenchmarkAckParallel 量化"队列锁跨 fsync 导致同队列 ack 串行"的确认吞吐基线，
//     以及拆分提交后的收益（改锁前后各跑一轮对比）
//   - BenchmarkAckBatch32 量化 AckBatch 批量一次 fsync 的收益（Task 2 引入）
//   - BenchmarkAckParallelAsync 去掉 fsync 后的 ack 路径开销，主要量 inflight
//     点查（store.Get）的缓存命中情况（SQ4.5 pebble 调参前后对比）
//
// 边界：
//   - 只跑基准，不包含断言测试
//   - 直接预写 inflight 记录构造可 ack 状态，不走 Receive（剥离取件成本，
//     量到的是纯确认路径）
package deliver

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

// newBenchDeliverer 构造单队列 Deliverer，并预写 n 条 attempt=1、过期时间
// 远在将来的 inflight 记录（offset 0..n-1）。syncWrites 决定写盘档位：
// 默认基准量的是 fsync 合并（传 true），Async 变体传 false。
// Ack 只读 inflight 不读消息本体，因此无需预写 msg/ 键。
func newBenchDeliverer(b *testing.B, n int, syncWrites bool) *Deliverer {
	b.Helper()
	st, err := store.Open(b.TempDir(), syncWrites, slog.Default())
	if err != nil {
		b.Fatalf("store: %v", err)
	}
	b.Cleanup(func() { st.Close() })
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	mt, err := meta.New(rep, rt, st, true, 1, 16, slog.Default())
	if err != nil {
		b.Fatalf("meta: %v", err)
	}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	d := New(rep, rt, st, mt, pr, slog.Default())
	exp := time.Now().Add(time.Hour).UnixMilli()
	const chunk = 4096 // 分块提交，避免单个超大 Batch 撑爆内存
	for lo := 0; lo < n; lo += chunk {
		hi := lo + chunk
		if hi > n {
			hi = n
		}
		wb := st.NewBatch()
		for i := lo; i < hi; i++ {
			wb.Set(store.InflightKey("g-bench", "t-ack", 0, uint64(i)),
				core.EncodeInflight(&core.InflightState{ExpireAtMs: exp, Attempts: 1}))
		}
		if err := st.Apply(wb); err != nil {
			b.Fatalf("预写 inflight: %v", err)
		}
	}
	return d
}

// BenchmarkAckParallel 同队列并发逐条 ack。并发度由 -cpu/GOMAXPROCS 决定
// （云服务器验收用 -test.cpu 64）。改锁前（队列锁跨 fsync）此数≈单流 fsync
// 速率；拆分提交后应放大约「合并深度」倍。
func BenchmarkAckParallel(b *testing.B) {
	d := newBenchDeliverer(b, b.N, true)
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
}

// BenchmarkAckParallelAsync 与 BenchmarkAckParallel 同形，但 store 用 async
// 写盘：去掉 fsync 后 ack 的主要耗时是 inflight 点查，用于观察 pebble 缓存/
// 布隆过滤器参数对读路径的影响（SQ4.5）。预写量随 b.N 变化，对比时须用
// 固定 -benchtime Nx 保证前后数据量一致（数据量决定是否超出缓存）。
func BenchmarkAckParallelAsync(b *testing.B) {
	d := newBenchDeliverer(b, b.N, false)
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
}

// BenchmarkAckBatch32 批量确认基准：每次迭代 = 一批 32 条一次 fsync。
// 换算 msg/s 时乘 32（ns/op 是「每批」耗时，不是每条）。
func BenchmarkAckBatch32(b *testing.B) {
	d := newBenchDeliverer(b, b.N*32, true)
	var next atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			base := (next.Add(1) - 1) * 32
			entries := make([]AckEntry, 32)
			for i := range entries {
				entries[i] = AckEntry{Offset: base + uint64(i), Attempt: 1}
			}
			results, err := d.AckBatch(context.Background(), "g-bench", "t-ack", 0, entries)
			if err != nil {
				b.Fatalf("AckBatch: %v", err)
			}
			for _, r := range results {
				if !r.OK {
					b.Fatalf("entry off=%d 落空", r.Offset)
				}
			}
		}
	})
}

// standingSizes S4 的两档在外规模：「ack/取件成本不随 inflight 增长」的
// 可检查形态是 10k 档 ns/op ≤ 1k 档的 1.5 倍（卡 SQ4.1）。
var standingSizes = []struct {
	name string
	n    int
}{{"inflight=1k", 1000}, {"inflight=10k", 10000}}

// preloadStanding 预写 n 条在外不 ack 的 inflight（offset 从 1<<40 起，与
// 被 ack 的 0..b.N-1 不相交），过期时间远在将来。
func preloadStanding(b *testing.B, d *Deliverer, topic string, n int) {
	b.Helper()
	exp := time.Now().Add(time.Hour).UnixMilli()
	wb := d.st.NewBatch()
	for i := 0; i < n; i++ {
		wb.Set(store.InflightKey("g-bench", topic, 0, 1<<40+uint64(i)),
			core.EncodeInflight(&core.InflightState{ExpireAtMs: exp, Attempts: 1}))
	}
	if err := d.st.Apply(wb); err != nil {
		b.Fatalf("预写在外 inflight: %v", err)
	}
}

// BenchmarkAckParallelStanding 同 BenchmarkAckParallel（fsync 档），另预置
// 1k/10k 条在外 inflight。计时前做一次落空 ack 让队列状态完成装载（只用现有
// API，main 上同样可跑，作基线对照）。
func BenchmarkAckParallelStanding(b *testing.B) {
	for _, sz := range standingSizes {
		b.Run(sz.name, func(b *testing.B) {
			d := newBenchDeliverer(b, b.N, true)
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

// BenchmarkReceiveStandingInflight 单流取件（每次 1 条新消息）在预置 1k/10k
// 条在外 inflight 下的成本（store async 档，剥离 fsync）。旧实现每次取件全扫
// 本队列 inflight 并逐条 JSON 解码，成本随在外数线性增长。
func BenchmarkReceiveStandingInflight(b *testing.B) {
	for _, sz := range standingSizes {
		b.Run(sz.name, func(b *testing.B) {
			d := newBenchDeliverer(b, 0, false)
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
