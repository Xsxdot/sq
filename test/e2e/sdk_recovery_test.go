//go:build e2e

// 官方 rocketmq-clients Go SDK 端到端测试：故障恢复类用例。
//
// 职责：
//   - AckTimeout 重投：收到消息不 ack，不可见窗口过期后必须能再次收到，
//     且 DeliveryAttempt 递增——锁定惰性重投（Receive 时触发）经真实 SDK
//     可观测的行为
//   - broker 重启恢复：发送→消费部分并 ack→优雅停机→同一数据目录重启，
//     未 ack 的消息仍可收到（inflight 持久化）、已 ack 的绝不重复
//     （cursor 持久化）——这是 cursor/inflight 跨进程恢复唯一的一层测试，
//     单测只能在同一 store 实例内验证，盖不住"进程死掉再爬起来"
//
// 边界：
//   - broker 启停基建复用 sdk_test.go 的 writeBrokerConfig/launchBroker/
//     brokerHandle（每用例独立数据目录与端口，不共享 broker）
//   - 只覆盖普通消息；崩溃恢复（SIGKILL 非优雅路径）不在 M1 出口标准内，
//     此处两代进程之间走的是 SIGTERM 优雅停机
package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// recvInvisible 收取消息时申请的不可见窗口。刻意取短：AckTimeout/重启恢复
// 两条用例都要等它过期后触发重投，窗口越短用例越快；又不能短到"消息刚投出、
// 本轮断言还没做完就已过期被并发重投"，3s 与轮询节奏（awaitDuration 2s）
// 同量级，两头都稳。
const recvInvisible = 3 * time.Second

// sendMessages 起一个 Producer 发 n 条消息，返回按发送顺序排列的 msgId。
// 函数返回前 Producer 已优雅停止——重启类用例随后要停 broker，生产者的
// telemetry 长流先行关闭，停机断言测的就纯粹是 broker 自己的收尾能力。
func sendMessages(t *testing.T, endpoint, topic string, n int) []string {
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

	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		recs, err := producer.Send(context.Background(),
			&rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("recovery payload #%d", i))})
		if err != nil {
			t.Fatalf("Send #%d 失败: %v", i, err)
		}
		if len(recs) == 0 {
			t.Fatalf("Send #%d 返回空回执", i)
		}
		t.Logf("已发送 #%d msgId=%s offset=%d", i, recs[0].MessageID, recs[0].Offset)
		ids = append(ids, recs[0].MessageID)
	}
	return ids
}

// newSimpleConsumer 构造并启动一个订阅单 topic（SUB_ALL）的 SimpleConsumer。
// 不注册 Cleanup：重启用例必须在停 broker 之前亲手停掉第一个消费者，
// 生命周期交由调用方显式管理，与 launchBroker 的取舍一致。
func newSimpleConsumer(t *testing.T, endpoint, group, topic string) rmq.SimpleConsumer {
	t.Helper()
	c, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(2*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			topic: rmq.SUB_ALL,
		}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	return c
}

// receiveByID 轮询 SimpleConsumer 直到收到指定 msgId 的消息，超时则 Fatal。
// 途中收到的其它消息原样放过（不 ack）：本包恢复类用例每个 topic 的消息
// 都极少且全部有明确归属，不存在需要"清场"的干扰消息。
func receiveByID(t *testing.T, c rmq.SimpleConsumer, msgID string, invisible, timeout time.Duration) *rmq.MessageView {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for rounds := 1; time.Now().Before(deadline); rounds++ {
		mvs, err := c.Receive(context.Background(), 16, invisible)
		if err != nil {
			// 空轮询（MESSAGE_NOT_FOUND）在 SDK 里以错误形式返回，属正常；但绝不
			// 静默——真实故障（握手失败、路由错）同样走这条分支，不打印的话一次
			// 失败的 run 只表现为"超时没收到"，无从判断卡在哪一步。
			t.Logf("第 %d 轮 Receive 返回错误（空轮询或故障）: %v", rounds, err)
			continue
		}
		for _, mv := range mvs {
			if mv.GetMessageId() == msgID {
				t.Logf("第 %d 轮收到目标消息 msgId=%s attempt=%d", rounds, msgID, mv.GetDeliveryAttempt())
				return mv
			}
			t.Logf("第 %d 轮收到非目标消息 msgId=%s（不 ack）", rounds, mv.GetMessageId())
		}
	}
	t.Fatalf("在 %v 内未收到 msgId=%s", timeout, msgID)
	return nil
}

// TestOfficialGoSDKAckTimeoutRedelivery 锁定不可见超时重投：短窗口收到消息
// 故意不 ack，窗口过期后必须能再次收到同一条消息，且 DeliveryAttempt 递增；
// 最终 ack 成功，链路闭环。
//
// 这条必须是 e2e 而不能只靠 deliver 包单测：重投是惰性的（Receive 时触发，
// 无后台扫描），"SDK 的下一次轮询真的会把过期消息带回来、attempt 真的会经
// SystemProperties 透传到 MessageView"这两段只有真实 SDK 链路能验证。
func TestOfficialGoSDKAckTimeoutRedelivery(t *testing.T) {
	endpoint := startBroker(t)
	const topic = "e2e-acktimeout"
	const group = "e2e-acktimeout-g"

	ids := sendMessages(t, endpoint, topic, 1)
	msgID := ids[0]

	consumer := newSimpleConsumer(t, endpoint, group, topic)
	defer consumer.GracefulStop()

	// 首取：短不可见窗口，收到但故意不 ack。
	first := receiveByID(t, consumer, msgID, recvInvisible, 60*time.Second)
	if a := first.GetDeliveryAttempt(); a != 1 {
		t.Fatalf("首次投递 DeliveryAttempt 应为 1，实际 %d", a)
	}

	// 不 ack，继续轮询：窗口过期后 Receive 触发惰性重投，应再次收到同一条。
	// 这次申请长窗口（30s），确保断言与 ack 期间不会又一次过期重投产生干扰。
	redelivered := receiveByID(t, consumer, msgID, 30*time.Second, 90*time.Second)
	if a := redelivered.GetDeliveryAttempt(); a < 2 {
		t.Fatalf("重投消息 DeliveryAttempt 应 >=2，实际 %d", a)
	}
	if err := consumer.Ack(context.Background(), redelivered); err != nil {
		t.Fatalf("Ack 重投消息失败: %v", err)
	}
}

// TestOfficialGoSDKRestartRecovery 锁定 cursor/inflight 跨进程恢复：
// 发 4 条 → 全部收到、只 ack 其中 3 条 → 优雅停机 → 同一数据目录重启 →
// 重启后必须且只能收到未 ack 的那 1 条（attempt 递增），已 ack 的绝不重现。
//
// 两个断言各钉住一半持久化：
//   - 未 ack 的能回来：inflight 记录落盘了，重启后过期重投仍然生效；
//   - 已 ack 的不回来：fetch cursor 落盘了——若 cursor 重启后归零，
//     消息本体还在 msg/ 键空间里（ack 只删 inflight），4 条会全部以
//     attempt=1 重新投出来，这正是"只收到目标消息"这条断言要抓的回归。
func TestOfficialGoSDKRestartRecovery(t *testing.T) {
	const topic = "e2e-restart"
	const group = "e2e-restart-g"
	const total = 4 // 4 条消息按轮询落在前 4 个队列（0-3），与总队列数无关；重启后按序恢复仍只依赖这 4 条

	cfgPath, endpoint := writeBrokerConfig(t)
	dir := filepath.Dir(cfgPath)
	run1Log := filepath.Join(dir, "broker-run1.log")
	run2Log := filepath.Join(dir, "broker-run2.log")

	// cur 指向当前活着的一代进程；亲手停起，Cleanup 只兜底"用例中途 Fatal
	// 时还有进程活着"的情况。失败时两代日志都展开——重启用例最难排查的
	// 就是"问题出在哪一代"。
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
	cur = launchBroker(t, cfgPath, endpoint, run1Log)

	// ---- 第一代：发 total 条，收齐后只 ack 除 victim 外的其它消息 ----
	ids := sendMessages(t, endpoint, topic, total)
	victim := ids[0]

	consumer1 := newSimpleConsumer(t, endpoint, group, topic)
	seen := make(map[string]bool, total)
	deadline := time.Now().Add(60 * time.Second)
	for rounds := 1; len(seen) < total && time.Now().Before(deadline); rounds++ {
		mvs, err := consumer1.Receive(context.Background(), 16, recvInvisible)
		if err != nil {
			t.Logf("第一代第 %d 轮 Receive 返回错误（空轮询或故障）: %v", rounds, err)
			continue
		}
		for _, mv := range mvs {
			seen[mv.GetMessageId()] = true
			if mv.GetMessageId() == victim {
				// victim 故意不 ack：它的 inflight 记录要活着穿过重启。
				// 短窗口下本循环可能再次收到它（过期重投），同样放过即可。
				continue
			}
			if err := consumer1.Ack(context.Background(), mv); err != nil {
				t.Fatalf("第一代 Ack msgId=%s 失败: %v", mv.GetMessageId(), err)
			}
		}
		t.Logf("第一代第 %d 轮后已见 %d/%d 条", rounds, len(seen), total)
	}
	if len(seen) != total {
		t.Fatalf("第一代未收齐: seen=%d want=%d", len(seen), total)
	}
	// 消费者先于 broker 停止：它的 telemetry 长流先行关闭，下面的停机断言
	// 测的就纯粹是 broker 的收尾能力（与 sendMessages 内停 producer 同理）。
	consumer1.GracefulStop()

	// ---- 重启：优雅停掉第一代（含退出码/停机预算断言），同一配置拉起第二代 ----
	cur.stop(t)
	cur = nil
	cur = launchBroker(t, cfgPath, endpoint, run2Log)

	// ---- 第二代：必须且只能收到 victim ----
	consumer2 := newSimpleConsumer(t, endpoint, group, topic)
	defer consumer2.GracefulStop()

	var recovered *rmq.MessageView
	deadline = time.Now().Add(60 * time.Second)
	for rounds := 1; recovered == nil && time.Now().Before(deadline); rounds++ {
		mvs, err := consumer2.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			t.Logf("第二代第 %d 轮 Receive 返回错误（空轮询或故障）: %v", rounds, err)
			continue
		}
		for _, mv := range mvs {
			// 除 victim 外的任何消息都是回归：要么 cursor 没持久化（已消费的
			// 消息被当新消息重投），要么 ack 没生效（inflight 没删干净）。
			if mv.GetMessageId() != victim {
				t.Fatalf("重启后收到已 ack 的消息 msgId=%s（attempt=%d）：cursor/ack 持久化被打破",
					mv.GetMessageId(), mv.GetDeliveryAttempt())
			}
			recovered = mv
		}
	}
	if recovered == nil {
		t.Fatal("重启后未收到未 ack 的消息：inflight 记录没有跨进程恢复")
	}
	if a := recovered.GetDeliveryAttempt(); a < 2 {
		t.Fatalf("重启后重投的 DeliveryAttempt 应 >=2（第一代已投过一次），实际 %d", a)
	}
	if err := consumer2.Ack(context.Background(), recovered); err != nil {
		t.Fatalf("重启后 Ack msgId=%s 失败: %v", victim, err)
	}

	// 收尾巡检：victim 已 ack，再空转几轮必须一无所获。4 个队列 SimpleConsumer
	// 逐队列轮转，跑 4 轮保证每个队列都被再看过一遍——若这里还能收到任何
	// 消息，说明上面的 ack 又没落住或有别的记录死而复生。
	for rounds := 1; rounds <= 4; rounds++ {
		mvs, err := consumer2.Receive(context.Background(), 16, recvInvisible)
		if err != nil {
			t.Logf("巡检第 %d 轮：空轮询（%v）", rounds, err)
			continue
		}
		for _, mv := range mvs {
			t.Fatalf("巡检第 %d 轮收到本不该存在的消息 msgId=%s attempt=%d",
				rounds, mv.GetMessageId(), mv.GetDeliveryAttempt())
		}
	}
}
