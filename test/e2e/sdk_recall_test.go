//go:build e2e

// RecallMessage e2e：撤回未到期的定时消息。
//
// 职责：
//   - E1 撤回成功后消息永不被投递
//   - E2 已投递的消息撤回失败，且失败不得损坏消息
//
// 边界：
//   - 只测单机档。集群档下撤回只在 meta leader 成立（spec §7 A2 的已知限制），
//     本次刻意不新增跨节点转发原语，因此不写集群用例
package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	sdkpb "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

// newRecallProducer 构造一个已声明 topic 的 producer。
// Recall 的 endpoint 取自 producer 自己的 topic 路由表，所以撤回必须用
// **同一个** producer，换一个会因路由未解析而失败。
func newRecallProducer(t *testing.T, endpoint, topic string) rmq.Producer {
	t.Helper()
	p, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	t.Cleanup(func() { p.GracefulStop() })
	return p
}

// sendDelayed 发一条延时消息，返回 recall 句柄与 message id。
func sendDelayed(t *testing.T, p rmq.Producer, topic, body string, after time.Duration) (string, string) {
	t.Helper()
	msg := &rmq.Message{Topic: topic, Body: []byte(body)}
	msg.SetDelayTimestamp(time.Now().Add(after))
	rs, err := p.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(rs) != 1 {
		t.Fatalf("期望 1 个 SendReceipt，实际 %d", len(rs))
	}
	return rs[0].RecallHandle, rs[0].MessageID
}

// E1 撤回成功：消费端始终收不到该消息。
func TestOfficialGoSDKRecallPreventsDelivery(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-recall"
		group = "e2e-recall-g"
	)
	p := newRecallProducer(t, endpoint, topic)
	// due=now+10s：留出足够时间在投递前完成撤回
	handle, msgID := sendDelayed(t, p, topic, "recall-me", 10*time.Second)
	if handle == "" {
		t.Fatalf("延时消息未带回 recall 句柄")
	}
	if _, err := p.Recall(context.Background(), topic, handle); err != nil {
		t.Fatalf("Recall: %v", err)
	}

	// 观测窗口必须长于 due（10s）——短于它则「撤回成功」与「还没到投递时间」
	// 观测一致，断言就成了假绿。取 15s。
	consumer := newDelayConsumer(t, endpoint, group, topic)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			t.Fatalf("撤回成功却仍收到消息: id=%s body=%q（发送时 id=%s）",
				mv.GetMessageId(), string(mv.GetBody()), msgID)
		}
	}
}

// E2 撤回失败不得损坏消息：已投递的消息撤回返回 MESSAGE_NOT_FOUND，
// 且消费端仍能正常收到它。
func TestOfficialGoSDKRecallAfterDeliveryFails(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-recall-late"
		group = "e2e-recall-late-g"
	)
	p := newRecallProducer(t, endpoint, topic)
	handle, msgID := sendDelayed(t, p, topic, "too-late", 2*time.Second)

	// 先把消息收到手，确保它确已投递（due=2s + 调度器扫描 + 长轮询）
	consumer := newDelayConsumer(t, endpoint, group, topic)
	var gotBody string
	deadline := time.Now().Add(20 * time.Second)
	for gotBody == "" && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if mv.GetMessageId() == msgID {
				gotBody = string(mv.GetBody())
			}
			_ = consumer.Ack(context.Background(), mv)
		}
	}
	// 承重：撤回失败不得损坏消息——消息必须正常收到且内容完整
	if gotBody != "too-late" {
		t.Fatalf("消息未被正常投递或内容不符：%q", gotBody)
	}

	// 已投递之后再撤：必须失败，且码是 MESSAGE_NOT_FOUND
	_, err := p.Recall(context.Background(), topic, handle)
	if err == nil {
		t.Fatalf("已投递的消息撤回竟然成功了")
	}
	var st *rmq.ErrRpcStatus
	if !errors.As(err, &st) {
		t.Fatalf("期望 *rmq.ErrRpcStatus，实际 %T: %v", err, err)
	}
	if st.Code != int32(sdkpb.Code_MESSAGE_NOT_FOUND) {
		t.Fatalf("Code=%d，期望 MESSAGE_NOT_FOUND(%d)", st.Code, sdkpb.Code_MESSAGE_NOT_FOUND)
	}
}
