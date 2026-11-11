// Package deliver —— 卡 SQ4.2 故事级检查 S1：顺序消息单队列「收到即 ack」吞吐。
//
// 场景：预写 N 条同 MessageGroup 的顺序消息（同一队列）。消费者分两个
// goroutine 模拟 SDK「处理当前消息时已挂着下一个长轮询」：接收者循环
// Receive，不等 ack 就发下一次；处理者收到即 Ack。顺序锁下，接收者的下一次
// 长轮询必然撞上「队头在外」而空等——唤醒是否在 ack 之后发出，决定它等的
// 是事件还是 100ms 兜底 tick。
//
// 判据（计划 sq4.2 第 4 节 S1）：
//   - 全部 ack、无重复、按 offset 有序
//   - 检查点模式吞吐 ≥ 200 条/秒（单机与集群）
//   - 逐次模式只报吞吐，另以轮次钩子验「被唤醒」起因占投出轮数 ≥ 90%
//
// 装配复用 sq3_cluster_race_test.go 的 {standalone, cluster} × {checkpoint, perop}。
package deliver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
)

const (
	sq42N         = 500
	sq42MinRate   = 200.0 // 条/秒，检查点模式硬阈值
	sq42MinWokeRt = 0.90  // 逐次模式「被唤醒」占投出轮数下限
)

func TestSQ42OrderedAckThroughput(t *testing.T) {
	sq3Each(t, sq42RunOrderedAck)
}

func sq42RunOrderedAck(t *testing.T, s *sq3Stack) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	topic, group := "sq42-s1", "sq42-g1"
	for i := 0; i < sq42N; i++ {
		if _, err := s.pr.Append(ctx, &core.Message{Topic: topic, Body: []byte("x"), MessageGroup: "mg-1"}); err != nil {
			t.Fatalf("预写第 %d 条: %v", i, err)
		}
	}

	// 轮次钩子：统计投出了消息的取件轮，按起因分桶
	var rounds roundCounter
	s.dl.roundHook = rounds.observe

	got := make(chan *core.Message, 64)
	recvErr := make(chan error, 1)
	start := time.Now()
	go func() {
		defer close(got)
		for ctx.Err() == nil {
			msgs, err := s.dl.Receive(ctx, group, topic, 0, 32, 30*time.Second, 5*time.Second, AllPass)
			if err != nil {
				if ctx.Err() == nil {
					recvErr <- err
				}
				return
			}
			for _, m := range msgs {
				select {
				case got <- m:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	var last uint64
	seen := make(map[uint64]bool, sq42N)
	for len(seen) < sq42N {
		var m *core.Message
		select {
		case m = <-got:
		case err := <-recvErr:
			t.Fatalf("Receive: %v", err)
		case <-ctx.Done():
			t.Fatalf("超时：只 ack 了 %d/%d 条", len(seen), sq42N)
		}
		if m == nil {
			t.Fatalf("接收者提前退出：只 ack 了 %d/%d 条", len(seen), sq42N)
		}
		if seen[m.Offset] {
			t.Fatalf("重复投递 offset=%d attempt=%d", m.Offset, m.DeliveryAttempt)
		}
		if len(seen) > 0 && m.Offset <= last {
			t.Fatalf("乱序：offset %d 在 %d 之后投出", m.Offset, last)
		}
		seen[m.Offset] = true
		last = m.Offset
		ok, err := s.dl.Ack(ctx, group, topic, m.QueueID, m.Offset, m.DeliveryAttempt)
		if err != nil || !ok {
			t.Fatalf("Ack offset=%d: ok=%v err=%v", m.Offset, ok, err)
		}
	}
	el := time.Since(start)
	cancel()
	rate := float64(sq42N) / el.Seconds()
	woke, delivered := rounds.snapshot()
	ratio := 0.0
	if delivered > 0 {
		ratio = float64(woke) / float64(delivered)
	}
	t.Logf("[SQ42-S1] %s: %d 条用时 %v，吞吐 %.1f 条/秒；投出轮 %d，其中被唤醒 %d（%.1f%%）",
		t.Name(), sq42N, el.Round(time.Millisecond), rate, delivered, woke, ratio*100)

	if strings.Contains(t.Name(), "checkpoint") {
		if rate < sq42MinRate {
			t.Fatalf("检查点模式吞吐 %.1f 条/秒 < %.0f", rate, sq42MinRate)
		}
		return
	}
	if ratio < sq42MinWokeRt {
		t.Fatalf("逐次模式被唤醒占比 %.1f%% < %.0f%%（投出轮 %d，被唤醒 %d）",
			ratio*100, sq42MinWokeRt*100, delivered, woke)
	}
}
