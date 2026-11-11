// 单机 async 档周期刷盘循环（RunWALSyncLoop）的不变式测试（SQ4.11）。
//
// 职责：
//   - 运行中按周期调用刷盘函数（以真实 Store.SyncWAL 的调用计数为判据）
//   - ctx 取消后循环退出，之后不再刷盘，store 可干净关闭
//   - 刷盘失败不终止循环（下一拍继续尝试）
//
// 边界：
//   - 不测 fsync 本身的落盘效果（pebble 的职责）
package store

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunWALSyncLoopSyncsPeriodicallyAndStopsClean(t *testing.T) {
	s, err := Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var n atomic.Int64
	syncFn := func() error { n.Add(1); return s.SyncWAL() }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { RunWALSyncLoop(ctx, 10*time.Millisecond, syncFn, slog.Default()); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for n.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("5s 内只刷了 %d 次，期望周期刷盘 ≥3 次", n.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ctx 取消后循环未退出")
	}
	after := n.Load()
	time.Sleep(50 * time.Millisecond)
	if got := n.Load(); got != after {
		t.Fatalf("退出后仍在刷盘：%d -> %d", after, got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("停循环后关闭 store: %v", err)
	}
}

func TestRunWALSyncLoopKeepsGoingAfterError(t *testing.T) {
	var n atomic.Int64
	syncFn := func() error { n.Add(1); return errors.New("模拟刷盘失败") }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { RunWALSyncLoop(ctx, 5*time.Millisecond, syncFn, slog.Default()); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for n.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("失败后循环停了：只调用 %d 次", n.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}
