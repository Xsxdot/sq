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
	if err != nil { b.Fatal(err) }
	if _, err := c.WaitLeader(10 * time.Second); err != nil { b.Fatal(err) }
	payload := bytes.Repeat([]byte("x"), 100)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.Leader().Propose(ctx, payload); err != nil { b.Fatal(err) }
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkProposeQuorumFsync(b *testing.B) { benchPropose(b, AckQuorumFsync) }
func BenchmarkProposeQuorumMem(b *testing.B)   { benchPropose(b, AckQuorumMem) }
