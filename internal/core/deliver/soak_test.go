// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责（soak 文件）：
//   - TestSoakE2E：边写边消费的端到端长跑，观察消费速率能否持续跟上生产、
//     堆积（produced−acked）是否单调增长（短跑基准量不到 compaction 与
//     inflight 累积的稳态——本文件补这个盲区）
//
// 边界：
//   - 默认跳过（SQ_SOAK=1 启用），绝不进普通 CI 路径
//   - 不做自动阈值断言（不同硬件基线不同）；验收由人工读打点判定：
//     ack_rate 均值 ≥ produce_rate 均值的 80%，backlog 曲线不单调增长（spec §3.7）
package deliver

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
)

// TestSoakE2E 端到端长跑：16 队列 / 64 个 producer / 16 个 consumer（每队列
// 一个，Receive 32 条批量 + AckBatch 整批确认）/ 真实 fsync，每 10s 打点。
//
// 环境变量：
//   - SQ_SOAK=1 启用（否则 Skip）
//   - SQ_SOAK_DURATION 时长（默认 10m）
//   - SQ_SOAK_DIR 数据目录（默认 t.TempDir()——某些机器 /tmp 在 tmpfs 上，
//     量真实磁盘时必须显式指定）
func TestSoakE2E(t *testing.T) {
	if os.Getenv("SQ_SOAK") == "" {
		t.Skip("soak 长跑默认跳过；SQ_SOAK=1 启用（make soak-e2e）")
	}
	dur := 10 * time.Minute
	if v := os.Getenv("SQ_SOAK_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("SQ_SOAK_DURATION 非法: %v", err)
		}
		dur = d
	}
	dir := t.TempDir()
	if v := os.Getenv("SQ_SOAK_DIR"); v != "" {
		dir = v
	}
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	mt, err := meta.New(rep, rt, st, true, 16, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	dl := New(rep, rt, st, mt, pr, slog.Default())
	logger := slog.Default().With("mod", "soak-e2e")
	logger.Info("soak-e2e 开始", "duration", dur.String(), "dir", dir,
		"queues", 16, "producers", 64, "consumers", 16)

	var produced, acked atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	body := make([]byte, 62)

	// 64 个 producer：持续写入
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := pr.Append(context.Background(), &core.Message{Topic: "t-soak-e2e", Body: body}); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				produced.Add(1)
			}
		}()
	}
	// 16 个 consumer：每队列一个，批量取件 + 整批确认（同时压测 AckBatch 路径）
	for q := uint32(0); q < 16; q++ {
		wg.Add(1)
		go func(q uint32) {
			defer wg.Done()
			ctx := context.Background()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// invisible 5 分钟：soak 全程不触发重投，acked 数即唯一确认数
				msgs, err := dl.Receive(ctx, "g-soak", "t-soak-e2e", q, 32, 5*time.Minute, 200*time.Millisecond, AllPass)
				if err != nil {
					t.Errorf("Receive q=%d: %v", q, err)
					return
				}
				if len(msgs) == 0 {
					continue
				}
				entries := make([]AckEntry, len(msgs))
				for i, m := range msgs {
					entries[i] = AckEntry{Offset: m.Offset, Attempt: m.DeliveryAttempt}
				}
				results, err := dl.AckBatch(context.Background(), "g-soak", "t-soak-e2e", q, entries)
				if err != nil {
					t.Errorf("AckBatch q=%d: %v", q, err)
					return
				}
				for _, r := range results {
					if r.OK {
						acked.Add(1)
					}
				}
			}
		}(q)
	}

	start := time.Now()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	var lastP, lastA uint64
	for time.Since(start) < dur {
		<-tick.C
		p, a := produced.Load(), acked.Load()
		// 每 10s 一条打点：验收就是人工读 produce_rate/ack_rate/backlog 的走势
		logger.Info("soak-e2e 打点", "elapsed_s", int(time.Since(start).Seconds()),
			"produce_rate", (p-lastP)/10, "ack_rate", (a-lastA)/10, "backlog", p-a)
		lastP, lastA = p, a
	}
	close(stop)
	wg.Wait()
	p, a := produced.Load(), acked.Load()
	logger.Info("soak-e2e 结束", "produced", p, "acked", a, "backlog", p-a,
		"avg_produce_per_s", p/uint64(dur.Seconds()), "avg_ack_per_s", a/uint64(dur.Seconds()))
}
