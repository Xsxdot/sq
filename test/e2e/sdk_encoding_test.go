//go:build e2e

// sdk_encoding_test.go 验证消息编码档位切换的端到端连续性。
//
// 职责：同一份数据目录跨代换档重启后，两代写入的消息都能被消费到，
//       且 Body 逐字节正确（含延时消息这类走独立键前缀的路径）。
// 边界：不测格式内部布局（internal/core 的单测覆盖）；不测集群混版
//       （需要多机，属 spec 的三机验收范围）。
package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"

	"github.com/xushixin/sq/internal/config"
)

// TestMessageEncodingCrossGeneration 锁死两步升级纪律的端到端前提：
// 换档重启后，**旧档写的存量**与**新档写的增量**必须都能正常消费。
//
// 这是单测覆盖不到的一环——单测里两种格式的字节是同一个进程造出来的，
// 而这里第一代进程只会写 JSON、第二代进程只会写二进制，盘上真真正正
// 是混存的，解码分流走的是真实的 Pebble 读路径。
func TestMessageEncodingCrossGeneration(t *testing.T) {
	const topic = "e2e-encoding"
	const group = "e2e-encoding-g"
	const perGen = 3

	cfgPath, endpoint := writeBrokerConfig(t, func(c *config.Config) {
		c.MessageEncoding = "json" // 第一代：历史格式
	})
	dir := filepath.Dir(cfgPath)
	run1Log := filepath.Join(dir, "broker-json.log")
	run2Log := filepath.Join(dir, "broker-binary.log")

	var cur *brokerHandle
	t.Cleanup(func() {
		if cur != nil {
			cur.stop(t)
		}
		if t.Failed() { // 换档用例最难排查的是"哪一代出的问题"，两代日志都展开
			dumpBrokerLog(t, run1Log)
			dumpBrokerLog(t, run2Log)
		}
	})

	// ---- 第一代（json 档）：写 perGen 条 ----
	cur = launchBroker(t, cfgPath, endpoint, run1Log)
	genA := sendMessages(t, endpoint, topic, perGen)
	t.Logf("第一代（json）已写入 %d 条", len(genA))
	cur.stop(t)
	cur = nil

	// ---- 换档为 binary，同一份 data_dir 重启 ----
	rewriteBrokerConfig(t, cfgPath, func(c *config.Config) {
		c.MessageEncoding = "binary"
	})
	cur = launchBroker(t, cfgPath, endpoint, run2Log)
	genB := sendMessages(t, endpoint, topic, perGen)
	t.Logf("第二代（binary）已写入 %d 条", len(genB))

	// ---- 消费：两代写入的消息必须全部收到 ----
	want := make(map[string]bool, perGen*2)
	for _, id := range append(append([]string{}, genA...), genB...) {
		want[id] = false
	}
	consumer := newSimpleConsumer(t, endpoint, group, topic)
	deadline := time.Now().Add(60 * time.Second)
	got := 0
	for got < len(want) && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, recvInvisible)
		if err != nil {
			continue // 空轮询
		}
		for _, mv := range mvs {
			id := mv.GetMessageId()
			if seen, ok := want[id]; ok && !seen {
				want[id] = true
				got++
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack msgId=%s 失败: %v", id, err)
			}
		}
	}
	for id, seen := range want {
		if !seen {
			t.Errorf("消息 %s 未被消费到（换档后存量/增量丢失）", id)
		}
	}
	if t.Failed() {
		t.Fatalf("换档消费不完整：%d/%d", got, len(want))
	}
}

// TestMessageEncodingDelayMessage 延时消息在 binary 档下的往返。
//
// 单独一条：延时消息落在 delay/ 前缀、到期后被改写移入 msg/，是编码
// 出入口之外还会"再读一次再写一次"的路径，容易在格式切换时被漏掉。
func TestMessageEncodingDelayMessage(t *testing.T) {
	const topic = "e2e-encoding-delay"
	const group = "e2e-encoding-delay-g"

	endpoint := startBroker(t, func(c *config.Config) {
		c.MessageEncoding = "binary"
	})

	// 含非 UTF-8 字节：base64 能安全承载它，原始字节段同样必须能
	body := []byte{0x00, 0xFF, 0x7F, 'd', 'e', 'l', 'a', 'y'}

	// producer 内联构造：test/e2e 里没有公共的 newProducer helper，
	// 每个用例各自 rmq.NewProducer（与 sdk_delay_test.go 同形）。
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

	due := time.Now().Add(6 * time.Second) // 与既有延时用例同节奏，不贴着到期卡点
	msg := &rmq.Message{Topic: topic, Body: body}
	msg.SetDelayTimestamp(due)
	res, err := producer.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("发送延时消息失败: %v", err)
	}
	if len(res) == 0 {
		t.Fatal("发送延时消息返回空结果")
	}
	t.Logf("延时消息已发送 msgId=%s due=%s", res[0].MessageID, due.Format(time.RFC3339))

	// 用 newDelayConsumer（sdk_delay_test.go 既有）：它的 AwaitDuration 就是
	// 按延时用例的轮询节奏调好的，复用而不是再造一个。
	consumer := newDelayConsumer(t, endpoint, group, topic)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, recvInvisible)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 返回，属正常
		}
		for _, mv := range mvs {
			if got := mv.GetBody(); string(got) != string(body) {
				t.Fatalf("Body 不一致: %x, 应为 %x", got, body)
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			return // 收到且内容正确，用例达成
		}
	}
	t.Fatal("延时消息到期后 60s 内未被投递")
}
