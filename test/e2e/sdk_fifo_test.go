//go:build e2e

// 官方 Go SDK 顺序消息 e2e：验证 M4 出口标准「顺序锁 + 卡住语义」。
//
// 职责：
//   - 顺序投递：同 MessageGroup 8 条消息严格按发送序到达
//   - 卡住语义：队头消息未 ack 期间只会反复收到它（不可见超时重投），
//     绝不先收到后一条；ack 后放行
//
// 边界：
//   - 不验证多 group 并行吞吐（顺序天然按 group 串行，性能属 spec §10）
//   - ForwardMessageToDeadLetterQueue 由单测覆盖（Go SDK SimpleConsumer 不调用）
package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// newFifoConsumer 构造订阅单 topic 的 SimpleConsumer（本文件专用辅助）
func newFifoConsumer(t *testing.T, endpoint, group, topic string) rmq.SimpleConsumer {
	t.Helper()
	c, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	t.Cleanup(func() { c.GracefulStop() })
	return c
}

// newFifoProducer 构造 producer 并发送 n 条同组顺序消息（body: {prefix}-{i}）
func sendFifoBatch(t *testing.T, endpoint, topic, group, prefix string, n int) {
	t.Helper()
	producer, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := producer.Start(); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	defer producer.GracefulStop()
	for i := 0; i < n; i++ {
		msg := &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("%s-%d", prefix, i))}
		msg.SetMessageGroup(group)
		if _, err := producer.Send(context.Background(), msg); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
}

// TestOfficialGoSDKFIFOOrderedDelivery 同组 8 条消息严格按发送序到达，
// MessageGroup 回读一致，逐条 ack
func TestOfficialGoSDKFIFOOrderedDelivery(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-fifo"
		group = "e2e-fifo-g"
		mg    = "order-key"
		total = 8
	)
	sendFifoBatch(t, endpoint, topic, mg, "fifo", total)
	consumer := newFifoConsumer(t, endpoint, group, topic)
	next := 0
	// 官方 SDK 的 SimpleConsumer.Receive 每次只轮询一个队列（round-robin），
	// 空队列长轮询约 5s 超时；8 条同组消息全部落在同一队列，每条消息的投递
	// 都要等一轮 4 队列循环（约 15s），整体约 110s+。deadline 取 240s 留足
	// 余量——120s 实测差最后一条几秒超时（本机 2/2 复现），这是 SDK 轮询
	// 节奏而非 broker 行为（broker 在 ack 后 100ms 内即可投出下一条）。
	deadline := time.Now().Add(240 * time.Second)
	for time.Now().Before(deadline) && next < total {
		mvs, err := consumer.Receive(context.Background(), 16, 20*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			want := fmt.Sprintf("fifo-%d", next)
			if string(mv.GetBody()) != want {
				t.Fatalf("乱序：期望 %s 收到 %s", want, mv.GetBody())
			}
			if g := mv.GetMessageGroup(); g == nil || *g != mg {
				t.Fatalf("MessageGroup 回读不符: %v", g)
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack %s: %v", want, err)
			}
			next++
		}
	}
	if next != total {
		t.Fatalf("只按序收到 %d/%d 条", next, total)
	}
}

// TestOfficialGoSDKFIFOBlockedUntilAck 卡住语义：first 未 ack 的 20s 窗口内
// 只会收到 first（不可见 5s，期间重投数次、attempt 递增），绝不能见到
// second；ack 最新一次收到的 first 后 second 到达
func TestOfficialGoSDKFIFOBlockedUntilAck(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-fifo-block"
		group = "e2e-fifo-block-g"
		mg    = "block-key"
	)
	sendFifoBatch(t, endpoint, topic, mg, "m", 2) // m-0, m-1
	consumer := newFifoConsumer(t, endpoint, group, topic)

	// 阶段 1：不 ack，观察 20s——收到的每一条都必须是 m-0
	var last *rmq.MessageView
	phase1End := time.Now().Add(20 * time.Second)
	for time.Now().Before(phase1End) {
		mvs, err := consumer.Receive(context.Background(), 16, 5*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != "m-0" {
				t.Fatalf("m-0 未 ack 前收到 %q——顺序锁失效", mv.GetBody())
			}
			last = mv // 只保留最新句柄：旧句柄已被重投覆盖，ack 会被幂等拒绝
		}
	}
	if last == nil {
		t.Fatal("20s 内未收到 m-0")
	}
	// 阶段 2：ack 最新一次的 m-0，m-1 放行
	if err := consumer.Ack(context.Background(), last); err != nil {
		t.Fatalf("Ack m-0: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 20*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != "m-1" {
				t.Fatalf("ack 后期望 m-1，收到 %q", mv.GetBody())
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack m-1: %v", err)
			}
			return
		}
	}
	t.Fatal("ack 后 60s 内未收到 m-1")
}
