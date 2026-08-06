//go:build e2e

// 官方 Go SDK 延时消息 e2e：验证 M3 出口标准「任意秒级延时，重启不丢」。
//
// 职责：
//   - 延时投递：到期前收不到、到期后收到、DeliveryTimestamp 回读一致
//   - 重启恢复：延时消息暂存期间重启 broker，到期后仍被投递
//
// 边界：
//   - 不验证海量到期吞吐（性能基线属 spec §10 长稳测试）
//   - 延时精度按调度间隔 100ms + 长轮询节奏放宽断言，不做毫秒级卡点
package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// newDelayConsumer 构造订阅单 topic 的 SimpleConsumer（本文件专用辅助）。
func newDelayConsumer(t *testing.T, endpoint, group, topic string) rmq.SimpleConsumer {
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

// TestOfficialGoSDKDelayDelivery 延时 6s 的消息：前 3s 收不到，到期后收到，
// 且 MessageView 能读回发送时设置的 DeliveryTimestamp。
func TestOfficialGoSDKDelayDelivery(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-delay"
		group = "e2e-delay-g"
	)
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

	due := time.Now().Add(6 * time.Second)
	msg := &rmq.Message{Topic: topic, Body: []byte("delayed")}
	msg.SetDelayTimestamp(due)
	if _, err := producer.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	consumer := newDelayConsumer(t, endpoint, group, topic)

	// 到期前 3s 窗口内不应收到（留 3s 余量吸收轮询节奏，不贴着 due 卡点）
	notBefore := due.Add(-3 * time.Second)
	for time.Now().Before(notBefore) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			t.Fatalf("到期前收到消息: body=%s（距 due 还有 %v）", mv.GetBody(), time.Until(due))
		}
	}
	// 到期后 60s 内必须收到
	deadline := due.Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != "delayed" {
				t.Fatalf("消息体不符: %s", mv.GetBody())
			}
			if time.Now().Before(due) {
				t.Fatalf("提前投递: 距 due 还有 %v", time.Until(due))
			}
			ts := mv.GetDeliveryTimestamp()
			if ts == nil || ts.UnixMilli() != due.UnixMilli() {
				t.Fatalf("DeliveryTimestamp 回读不符: %v（期望 %v）", ts, due)
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			return
		}
	}
	t.Fatal("到期后 60s 内未收到延时消息")
}

// TestOfficialGoSDKDelayRestartRecovery 延时消息暂存期间重启 broker（M3 出口
// 标准「重启不丢」）：第一代进程收下延时 8s 的消息后立即停机，同一数据目录
// 拉起第二代，到期后消息仍被投递且恰好一次到达（首投 attempt 语义由
// broker 侧单测覆盖，这里只断言到达与内容）。
func TestOfficialGoSDKDelayRestartRecovery(t *testing.T) {
	cfgPath, endpoint := writeBrokerConfig(t)
	dir := filepath.Dir(cfgPath)
	run1Log := filepath.Join(dir, "broker-run1.log")
	run2Log := filepath.Join(dir, "broker-run2.log")
	const (
		topic = "e2e-delay-restart"
		group = "e2e-delay-restart-g"
		body  = "survive-restart"
	)

	// cur 指向当前活着的一代进程；亲手停起，Cleanup 只兜底"用例中途 Fatal
	// 时还有进程活着"的情况。失败时两代日志都展开——重启用例最难排查的
	// 就是"问题出在哪一代"（与 sdk_recovery_test.go 的约定一致）。
	var cur *brokerHandle
	t.Cleanup(func() {
		if cur != nil {
			cur.stop(t)
		}
		if t.Failed() {
			dumpBrokerLog(t, run1Log)
			dumpBrokerLog(t, run2Log)
		}
	})

	// 第一代：发送延时消息后停机
	cur = launchBroker(t, cfgPath, endpoint, run1Log)
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
	due := time.Now().Add(8 * time.Second)
	msg := &rmq.Message{Topic: topic, Body: []byte(body)}
	msg.SetDelayTimestamp(due)
	if _, err := producer.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}
	producer.GracefulStop()
	cur.stop(t)
	cur = nil

	// 第二代：同一数据目录重启，到期后消费到
	cur = launchBroker(t, cfgPath, endpoint, run2Log)
	consumer := newDelayConsumer(t, endpoint, group, topic)
	deadline := due.Add(90 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != body {
				t.Fatalf("消息体不符: %s", mv.GetBody())
			}
			if time.Now().Before(due) {
				t.Fatalf("提前投递: 距 due 还有 %v", time.Until(due))
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			return
		}
	}
	t.Fatal("重启后到期消息未到达——延时消息在重启中丢失")
}
