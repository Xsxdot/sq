// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责（基准文件）：
//   - BenchmarkAckParallel 量化"队列锁跨 fsync 导致同队列 ack 串行"的确认吞吐基线，
//     以及拆分提交后的收益（改锁前后各跑一轮对比）
//   - BenchmarkAckBatch32 量化 AckBatch 批量一次 fsync 的收益（Task 2 引入）
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

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
)

// newBenchDeliverer 构造真实 fsync 的单队列 Deliverer，并预写 n 条
// attempt=1、过期时间远在将来的 inflight 记录（offset 0..n-1）。
// Ack 只读 inflight 不读消息本体，因此无需预写 msg/ 键。
func newBenchDeliverer(b *testing.B, n int) *Deliverer {
	b.Helper()
	st, err := store.Open(b.TempDir(), true, slog.Default())
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
	d := newBenchDeliverer(b, b.N)
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
	d := newBenchDeliverer(b, b.N*32)
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
