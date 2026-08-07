//go:build e2e

// 官方 Go SDK 事务消息端到端：M6 出口标准（spec §11——半消息 + Telemetry
// 回查全链路）。三条链路：显式提交、显式回滚、孤儿回查（不调 Commit，
// 等 broker 经 Telemetry 下发回查、SDK checker 决断提交）。
//
// 职责：
//   - 提交/回滚：两个事务都在首个回查间隔内由客户端决断完成，提交前
//     半消息不可见、提交后同一 msgId 可消费，回滚后永不可见
//   - 孤儿回查：故意不决断，等 broker 经 Telemetry 下发回查命令、SDK
//     checker 决断 COMMIT 后消息可消费——half→回查→checker→EndTransaction
//     全链路闭环
//
// 边界：
//   - 不测回查超限丢弃（需等 maxChecks×interval，纯服务端逻辑已有单测覆盖）
//   - checker 回调跑在 SDK goroutine 里，一律经 channel 传结果，禁止在
//     回调里直接 t.Fatal/t.Error
package e2e

import (
	"context"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"

	"github.com/xushixin/sq/internal/config"
)

// txnCheckIntervalE2E 半消息回查间隔压到 2s：孤儿用例要在测试超时内等到
// 第一次回查；提交/回滚用例的「首个间隔内完成决断」断言也以它为准。
const txnCheckIntervalE2E = "2s"

// txnCheckIntervalE2EDur 与上面字符串同源的时长（Stage 的首个回查排期 =
// now+interval，回查扫描粒度 1s，本文件的时间断言都按它推导）。
const txnCheckIntervalE2EDur = 2 * time.Second

// newTxnProducer 构造订阅单 topic、带事务回查 checker 的 Producer
// （本文件专用辅助，与 sdk_delay_test.go 的 newDelayConsumer 同模式）。
func newTxnProducer(t *testing.T, endpoint, topic string, checker *rmq.TransactionChecker) rmq.Producer {
	t.Helper()
	opts := []rmq.ProducerOption{rmq.WithTopics(topic)}
	if checker != nil {
		opts = append(opts, rmq.WithTransactionChecker(checker))
	}
	p, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{},
	}, opts...)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	t.Cleanup(func() { p.GracefulStop() })
	return p
}

// newTxnConsumer 构造订阅单 topic 的 SimpleConsumer（本文件专用辅助）。
//
// 与 sdk_delay_test.go 的 newDelayConsumer 同构，但把空轮询节奏压短：
// SDK 的 Receive 请求 deadline = awaitDuration + requestTimeout（默认
// requestTimeout≈365 天，空轮询实际会占满服务端 defaultLongPolling 20s），
// 而本文件两条用例的回查间隔只有 2s——「提交前不可见」「回滚后不可见」
// 的断言必须在一个间隔内完成，等不起 20s 的空轮询。SetRequestTimeout
// 在 SimpleConsumer 的接口上（Consumer 内嵌 isClient），压到 500ms 后
// 空轮询 ≈0.5s 内必返 MESSAGE_NOT_FOUND，且回查未触发前就把断言做完。
func newTxnConsumer(t *testing.T, endpoint, group, topic string) rmq.SimpleConsumer {
	t.Helper()
	c, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	c.SetRequestTimeout(500 * time.Millisecond)
	if err := c.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	t.Cleanup(func() { c.GracefulStop() })
	return c
}

// recvUntil 在 window 内轮询 consumer，返回第一条 body 为 wantBody 的消息
// （未 ack）；窗口耗尽仍未收到返回 nil。收到其它 body 立即 t.Fatalf——
// 本文件每条用例独占 topic，出现串扰只可能是断言写错。
func recvUntil(t *testing.T, consumer rmq.SimpleConsumer, wantBody string, window time.Duration) *rmq.MessageView {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != wantBody {
				t.Fatalf("收到意外消息: body=%s", mv.GetBody())
			}
			return mv
		}
	}
	return nil
}

// TestOfficialGoSDKTransactionCommitAndRollback 显式提交/回滚全链路：
// 提交前半消息不可见（Receive 单轮短等待只得空结果）→ Commit 后同一
// msgId 可消费并 ack；第二条事务 SendWithTransaction 后立即 RollBack →
// 消息永不可见。两条事务都在首个回查间隔（2s）内由客户端决断完毕，
// Telemetry 回查不应触发——checker 一旦被调用即失败。
func TestOfficialGoSDKTransactionCommitAndRollback(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) { c.TxnCheckInterval = txnCheckIntervalE2E })
	const (
		topic = "e2e-txn"
		group = "e2e-txn-g"
	)
	// checker 回调跑在 SDK 的 telemetry goroutine 里，禁止直接 t.Fatal——
	// 这里只把被回查的 msgId 塞进 channel，由测试主 goroutine 收尾断言。
	checkerCalled := make(chan string, 1)
	producer := newTxnProducer(t, endpoint, topic, &rmq.TransactionChecker{
		Check: func(mv *rmq.MessageView) rmq.TransactionResolution {
			t.Logf("checker 被回查调用（不应发生）: msgId=%s", mv.GetMessageId())
			select {
			case checkerCalled <- mv.GetMessageId():
			default: // 缓冲已满说明早已失败，丢弃后续重复回查
			}
			return rmq.COMMIT
		},
	})
	consumer := newTxnConsumer(t, endpoint, group, topic)

	// ---- 事务 1：显式提交 ----
	tx := producer.BeginTransaction()
	recs, err := producer.SendWithTransaction(context.Background(),
		&rmq.Message{Topic: topic, Body: []byte("txn-commit")}, tx)
	if err != nil {
		t.Fatalf("SendWithTransaction: %v", err)
	}
	if len(recs) == 0 || recs[0].MessageID == "" {
		t.Fatalf("SendWithTransaction 返回空回执: %v", recs)
	}
	commitMsgID := recs[0].MessageID
	t.Logf("事务消息已发送: msgId=%s txId=%s", commitMsgID, recs[0].TransactionId)

	// 提交前半消息不可见：单轮 Receive 短等待即得空结果。半消息无队列无
	// offset（deliver 只扫 msg/），本轮 ≈0.5s，必须赶在首个回查排期
	// （now+2s，扫描粒度 1s）之前完成——回查先触发会让 checker 断言误报。
	mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
	if err == nil && len(mvs) > 0 {
		t.Fatalf("提交前消费到了半消息: %d 条（首条 msgId=%s）", len(mvs), mvs[0].GetMessageId())
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("事务提交失败: %v", err)
	}
	mv := recvUntil(t, consumer, "txn-commit", 60*time.Second)
	if mv == nil {
		t.Fatal("提交后 60s 内未收到消息")
	}
	if mv.GetMessageId() != commitMsgID {
		t.Fatalf("消费到的 msgId 与发送回执不一致: got=%s want=%s", mv.GetMessageId(), commitMsgID)
	}
	if err := consumer.Ack(context.Background(), mv); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// ---- 事务 2：显式回滚 ----
	tx2 := producer.BeginTransaction()
	recs2, err := producer.SendWithTransaction(context.Background(),
		&rmq.Message{Topic: topic, Body: []byte("txn-rollback")}, tx2)
	if err != nil {
		t.Fatalf("SendWithTransaction #2: %v", err)
	}
	if len(recs2) == 0 || recs2[0].MessageID == "" {
		t.Fatalf("SendWithTransaction #2 返回空回执: %v", recs2)
	}
	if err := tx2.RollBack(); err != nil {
		t.Fatalf("事务回滚失败: %v", err)
	}
	t.Logf("事务回滚完成: msgId=%s txId=%s", recs2[0].MessageID, recs2[0].TransactionId)

	// 回滚后消息必须永不可见：窗口 = 2×间隔 + 1s（覆盖事务 2 的首个回查
	// 排期 [2s,3s]，也顺带覆盖事务 1 的），期间轮询若收到即失败
	if mv := recvUntil(t, consumer, "txn-rollback", 2*txnCheckIntervalE2EDur+time.Second); mv != nil {
		t.Fatalf("回滚后仍消费到消息: msgId=%s", mv.GetMessageId())
	}

	// 两条事务都应在首个间隔内由客户端决断完毕：这里兜底断言 checker
	// 从未被 Telemetry 回查触发（此刻已越过两个事务各自的首次回查排期）
	select {
	case id := <-checkerCalled:
		t.Fatalf("checker 被回查调用（首个间隔内应已完成决断）: msgId=%s", id)
	default:
	}
}

// TestOfficialGoSDKTransactionOrphanRecovery 孤儿回查闭环：SendWithTransaction
// 后故意不 Commit/RollBack，broker 的首个回查排期（now+2s）到期后经
// Telemetry 下发回查命令 → SDK 的 checker 决断 COMMIT → EndTransaction
// 提交 → 消息可消费。证明 half→回查→checker→EndTransaction→msg/ 全链路
// 贯通，且 msgId 全链路一致（发送回执 == checker 收到的半消息 == 消费到的
// 消息；TransactionId 由 broker 在下发命令时带上、SDK 原样回填 EndTransaction，
// checker 的 MessageView 不暴露它，故以 msgId 对账）。
func TestOfficialGoSDKTransactionOrphanRecovery(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) { c.TxnCheckInterval = txnCheckIntervalE2E })
	const (
		topic = "e2e-txn-orphan"
		group = "e2e-txn-orphan-g"
		body  = "orphan"
	)
	type checkResult struct {
		msgID string
		got   string
	}
	checked := make(chan checkResult, 1)
	producer := newTxnProducer(t, endpoint, topic, &rmq.TransactionChecker{
		Check: func(mv *rmq.MessageView) rmq.TransactionResolution {
			t.Logf("checker 收到回查: msgId=%s body=%s", mv.GetMessageId(), mv.GetBody())
			select {
			case checked <- checkResult{msgID: mv.GetMessageId(), got: string(mv.GetBody())}:
			default: // 缓冲已满（重复回查），丢弃
			}
			return rmq.COMMIT
		},
	})
	consumer := newTxnConsumer(t, endpoint, group, topic)

	recs, err := producer.SendWithTransaction(context.Background(),
		&rmq.Message{Topic: topic, Body: []byte(body)}, producer.BeginTransaction())
	if err != nil {
		t.Fatalf("SendWithTransaction: %v", err)
	}
	if len(recs) == 0 || recs[0].MessageID == "" {
		t.Fatalf("SendWithTransaction 返回空回执: %v", recs)
	}
	msgID := recs[0].MessageID
	t.Logf("孤儿事务消息已发送（故意不 Commit/RollBack）: msgId=%s txId=%s", msgID, recs[0].TransactionId)

	// 首个回查排期 = now+2s，扫描粒度 1s：正常 ~2-3s 内 checker 必被调用。
	// 给满 15s（约 7 个间隔）吸收调度抖动——真正断链时远早于它暴露。
	select {
	case res := <-checked:
		if res.msgID != msgID {
			t.Fatalf("checker 收到的 msgId 与发送回执不一致: got=%s want=%s", res.msgID, msgID)
		}
		if res.got != body {
			t.Fatalf("checker 收到的消息体不符: got=%q want=%q", res.got, body)
		}
		t.Logf("checker 已决断 COMMIT: msgId=%s", res.msgID)
	case <-time.After(15 * time.Second):
		t.Fatal("15s 内 checker 未被调用（回查未下发）——half→Telemetry 链路断裂")
	}

	// checker 的 COMMIT 经 EndTransaction 落盘后消息可消费，msgId 与回执一致
	mv := recvUntil(t, consumer, body, 60*time.Second)
	if mv == nil {
		t.Fatal("回查提交后 60s 内未收到消息")
	}
	if mv.GetMessageId() != msgID {
		t.Fatalf("消费到的 msgId 与发送回执不一致: got=%s want=%s", mv.GetMessageId(), msgID)
	}
	if err := consumer.Ack(context.Background(), mv); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}
