// Package produce 实现消息写入路径：队列选择、offset 分配、落盘、长轮询唤醒。
//
// 职责（soak 文件）：
//   - TestSoak：长时间持续写入基准，观察 Pebble compaction 稳态下吞吐是否
//     崩塌（短跑基准触发不了 compaction，数字会虚高——本文件补这个盲区）
//
// 边界：
//   - 默认跳过（SQ_SOAK=1 启用），绝不进普通 CI 路径
//   - 不做自动阈值断言：不同硬件基线不同，自动断言会把慢盘机器变成假失败；
//     验收由人工读打点日志判定（后半程均值 ≥ 前半程 70%，spec §6）
package produce

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
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
)

// TestSoak 持续高写入长跑：16 队列 / 64 并发 / 真实 fsync，每 10s 打点吞吐。
//
// 环境变量：
//   - SQ_SOAK=1 启用（否则 Skip）
//   - SQ_SOAK_DURATION 时长（默认 10m）
//   - SQ_SOAK_DIR 数据目录（默认 t.TempDir()——注意某些机器 /tmp 在 tmpfs 上，
//     量真实磁盘时必须显式指定）
func TestSoak(t *testing.T) {
	if os.Getenv("SQ_SOAK") == "" {
		t.Skip("soak 长跑默认跳过；SQ_SOAK=1 启用（make soak）")
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
	mt, err := meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 16, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	p := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, mt, slog.Default())
	logger := slog.Default().With("mod", "soak")
	logger.Info("soak 开始", "duration", dur.String(), "dir", dir, "queues", 16, "workers", 64)

	var total atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	body := make([]byte, 62)
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
				if _, err := p.Append(context.Background(), &core.Message{Topic: "t-soak", Body: body}); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				total.Add(1)
			}
		}()
	}
	start := time.Now()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	var last uint64
	for time.Since(start) < dur {
		<-tick.C
		cur := total.Load()
		// 每 10s 一条打点：验收就是人工读这串 rate_per_s 的走势
		logger.Info("soak 打点", "elapsed_s", int(time.Since(start).Seconds()),
			"total", cur, "rate_per_s", (cur-last)/10)
		last = cur
	}
	close(stop)
	wg.Wait()
	logger.Info("soak 结束", "total", total.Load(),
		"avg_per_s", total.Load()/uint64(dur.Seconds()))
}
