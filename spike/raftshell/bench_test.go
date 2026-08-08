// bench_test.go 两档刷盘的 propose→commit 吞吐基准。
//
// 职责：实测 AckQuorumFsync / AckQuorumMem 两档确认模式的吞吐上限，
// 数据回填 spec §2.3（payload 100B 对齐 sq 基准消息体量级）。
// 边界：bench 专用，不参与功能测试；日志用 DiscardHandler 静默——
// 基准期间每节点每秒数千条日志会淹没测量，此为有意为之（评审 R2）。
package raftshell

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"
)

// 两档刷盘的 propose→commit 吞吐。payload 100B 对齐 sq 基准的消息体量级。
// -cpu 决定并发提案数——raft 单组日志把并发提案合并进批次追加，
// 这里量的就是 spec §2.3 预估的合并效应。
func benchPropose(b *testing.B, mode AckMode) {
	c, err := NewCluster(b.TempDir(), mode, slog.New(slog.DiscardHandler))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := c.WaitLeader(10 * time.Second); err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 100)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// RunParallel 的 worker goroutine 里不能调 Fatal（它不会
			// 停止基准，只会让错误在错误的 goroutine 上冒泡），
			// 失败统一用 Error + return 收场
			lead := c.Leader()
			if lead == nil {
				b.Error("WaitLeader 已成功但 Leader() 为空")
				return
			}
			if err := lead.Propose(ctx, payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkProposeQuorumFsync(b *testing.B) { benchPropose(b, AckQuorumFsync) }
func BenchmarkProposeQuorumMem(b *testing.B)   { benchPropose(b, AckQuorumMem) }
