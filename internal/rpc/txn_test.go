// txn_test.go 验证事务消息的 RPC 层行为：EndTransaction 决断与
// RecoverOrphan 回查命令下发（M6）。
//
// 职责：
//   - 覆盖 EndTransaction 主链路：COMMIT 后消息可收、ROLLBACK 后永不可收
//   - 覆盖幂等语义：未知 txID 回 OK（客户端网络重试/回查赛跑/超限丢弃
//     三种正常来源都不该让客户端把已生效的决断当成失败去重试）
//   - 覆盖决断不能靠猜：resolution 未指定必须回 BAD_REQUEST
//   - 覆盖 RecoverOrphan 经真实 bufconn Telemetry 流下发
//     RecoverOrphanedTransactionCommand，字段与原消息一致
//   - 覆盖无可用 producer 会话时返回 false（调度器据此改期）
//
// 边界：
//   - 不重复测 txn.Manager 内部的半消息状态机（那是 txn 包自己的测试职责），
//     这里只验证协议层把决断正确送达、把回查载荷正确编码
package rpc

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Xsxdot/sq/internal/core"
	pb "github.com/Xsxdot/sq/internal/rpc/pb/apache/rocketmq/v2"
	"github.com/Xsxdot/sq/internal/store"
)

// sendTransaction 发送一条 TRANSACTION 半消息，返回 (msgID, txID)。
func sendTransaction(t *testing.T, c pb.MessagingServiceClient, topic, body string) (string, string) {
	t.Helper()
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: topic},
			SystemProperties: &pb.SystemProperties{
				MessageType: pb.MessageType_TRANSACTION,
			},
			Body: []byte(body),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("send transaction: %v %v", resp.GetStatus(), err)
	}
	entry := resp.GetEntries()[0]
	if entry.GetTransactionId() == "" {
		t.Fatal("事务消息响应必须回填 transaction_id")
	}
	return entry.GetMessageId(), entry.GetTransactionId()
}

// receiveTxn 发起一次短超时 ReceiveMessage（0 余量长轮询，快失败），
// 返回第一条消息（没有则 nil）。四队列轮询用它能快速断言"不可收"。
func receiveTxn(t *testing.T, c pb.MessagingServiceClient, group, topic string, queueID int32) *pb.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:        &pb.Resource{Name: group},
		MessageQueue: &pb.MessageQueue{Topic: &pb.Resource{Name: topic}, Id: queueID},
		FilterExpression: &pb.FilterExpression{
			Type:       pb.FilterType_TAG,
			Expression: "*",
		},
		BatchSize:         10,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	msgs, _ := recvAll(t, stream)
	if len(msgs) == 0 {
		return nil
	}
	return msgs[0]
}

// TestEndTransactionCommitAndRollback 主链路：COMMIT 后半消息变普通消息
// 可被消费；ROLLBACK 后消息消失、永不可收。两次决断响应都必须 OK。
func TestEndTransactionCommitAndRollback(t *testing.T) {
	env := newTestEnv(t, true)
	c := env.client
	const topic = "t-txn"

	msgID, txID := sendTransaction(t, c, topic, "commit me")
	resp, err := c.EndTransaction(context.Background(), &pb.EndTransactionRequest{
		Topic:         &pb.Resource{Name: topic},
		MessageId:     msgID,
		TransactionId: txID,
		Resolution:    pb.TransactionResolution_COMMIT,
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("EndTransaction(COMMIT): %v %v", resp.GetStatus(), err)
	}
	// 提交后消息必须可收：四个队列都轮询（提交时经正常写入路径分配队列）。
	var got *pb.Message
	for q := int32(0); q < 4 && got == nil; q++ {
		got = receiveTxn(t, c, "g-txn", topic, q)
	}
	if got == nil {
		t.Fatal("COMMIT 后消息应可被消费")
	}
	if got.GetSystemProperties().GetMessageId() != msgID || !bytes.Equal(got.GetBody(), []byte("commit me")) {
		t.Fatalf("收到的消息与提交的半消息不一致: %v", got)
	}

	msgID2, txID2 := sendTransaction(t, c, topic, "rollback me")
	resp, err = c.EndTransaction(context.Background(), &pb.EndTransactionRequest{
		Topic:         &pb.Resource{Name: topic},
		MessageId:     msgID2,
		TransactionId: txID2,
		Resolution:    pb.TransactionResolution_ROLLBACK,
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("EndTransaction(ROLLBACK): %v %v", resp.GetStatus(), err)
	}
	// 回滚后消息永不可收：四队列全轮询不到，且盘上 halfidx 键已删除。
	for q := int32(0); q < 4; q++ {
		if m := receiveTxn(t, c, "g-txn", topic, q); m != nil {
			t.Fatalf("ROLLBACK 后不应再收到消息，却收到 %v", m)
		}
	}
	if _, ok, err := env.st.Get(store.HalfIdxKey(txID2)); err != nil || ok {
		t.Fatalf("回滚后 halfidx 应已删除，ok=%v err=%v", ok, err)
	}
}

// TestEndTransactionUnknownTxIDIsOK 幂等：未知 txID 回 OK。SDK 网络重试、
// 回查决断与客户端决断赛跑、超限已丢弃都会走到这里，回错误码会让一次
// 已生效的决断在客户端侧表现为失败、被无谓地重试。
func TestEndTransactionUnknownTxIDIsOK(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.EndTransaction(context.Background(), &pb.EndTransactionRequest{
		Topic:         &pb.Resource{Name: "t-unknown"},
		MessageId:     "m-unknown",
		TransactionId: "no-such-tx",
		Resolution:    pb.TransactionResolution_COMMIT,
	})
	if err != nil {
		t.Fatalf("未知 txID 不应产生传输层错误: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("未知 txID 应回 OK（幂等），实际 %v", resp.GetStatus())
	}
}

// TestEndTransactionRejectsUnspecifiedResolution 决断不能靠猜：resolution
// 未指定时，提交会放出未确认的业务消息、回滚会丢掉已确认的——两个方向
// 都错，只能拒绝。
func TestEndTransactionRejectsUnspecifiedResolution(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.EndTransaction(context.Background(), &pb.EndTransactionRequest{
		Topic:         &pb.Resource{Name: "t-nil-res"},
		MessageId:     "m-nil-res",
		TransactionId: "tx-nil-res",
		Resolution:    pb.TransactionResolution_TRANSACTION_RESOLUTION_UNSPECIFIED,
	})
	if err != nil {
		t.Fatalf("拒绝应走 Status 而非传输层错误: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_BAD_REQUEST {
		t.Fatalf("resolution 未指定应回 BAD_REQUEST，实际 %v", resp.GetStatus())
	}
}

// telemetryRegisterProducer 走一趟真实 Telemetry 握手并完成 producer 会话
// 注册（上报 Publishing topics），返回挂在该流上的客户端。
func telemetryRegisterProducer(t *testing.T, c pb.MessagingServiceClient, topics ...string) pb.MessagingService_TelemetryClient {
	t.Helper()
	stream, err := c.Telemetry(context.Background())
	if err != nil {
		t.Fatalf("Telemetry: %v", err)
	}
	ct := pb.ClientType_PRODUCER
	if err := stream.Send(&pb.TelemetryCommand{Command: &pb.TelemetryCommand_Settings{
		Settings: &pb.Settings{
			ClientType: &ct,
			PubSub: &pb.Settings_Publishing{Publishing: &pb.Publishing{
				Topics: func() []*pb.Resource {
					rs := make([]*pb.Resource, 0, len(topics))
					for _, tp := range topics {
						rs = append(rs, &pb.Resource{Name: tp})
					}
					return rs
				}(),
			}},
		},
	}}); err != nil {
		t.Fatalf("send settings: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil || resp.GetSettings() == nil {
		t.Fatalf("期望 settings 回包完成注册: %v %v", resp, err)
	}
	return stream
}

// TestRecoverOrphanSendsCommandToProducerStream 回查下发主链路：producer
// 会话注册后，RecoverOrphan 应把半消息编码为
// RecoverOrphanedTransactionCommand 写进该会话的 Telemetry 流，客户端
// 流上能收到，且 TransactionId / MessageId / Body / MessageType 与原消息
// 一致（SDK 的 MessageView 靠 MessageType==TRANSACTION 识别半消息回查）。
func TestRecoverOrphanSendsCommandToProducerStream(t *testing.T) {
	env := newTestEnv(t, true)
	stream := telemetryRegisterProducer(t, env.client, "t-orphan")

	m := &core.Message{
		ID:           "msg-orphan-1",
		Topic:        "t-orphan",
		Tag:          "et",
		Keys:         []string{"k-orphan"},
		Properties:   map[string]string{"pk": "pv"},
		Body:         []byte("orphan body"),
		BodyEncoding: core.BodyEncodingIdentity,
		// BodyDigest 故意留空：应触发 CRC32 兜底补算（同 receive.go 的理由）
		BornAtMs:  time.Now().UnixMilli() - 1000,
		BornHost:  "10.0.0.8:99",
		StoreAtMs: time.Now().UnixMilli(),
	}
	if !env.srv.RecoverOrphan(m, "TX9") {
		t.Fatal("有匹配的 producer 会话时 RecoverOrphan 应返回 true")
	}

	cmd, err := stream.Recv()
	if err != nil {
		t.Fatalf("客户端流上应收到回查命令: %v", err)
	}
	orphan := cmd.GetRecoverOrphanedTransactionCommand()
	if orphan == nil {
		t.Fatalf("期望 RecoverOrphanedTransactionCommand，实际 %T", cmd.GetCommand())
	}
	if orphan.GetTransactionId() != "TX9" {
		t.Fatalf("TransactionId 应为 TX9，实际 %q", orphan.GetTransactionId())
	}
	msg := orphan.GetMessage()
	if msg == nil {
		t.Fatal("回查命令缺少 Message")
	}
	sp := msg.GetSystemProperties()
	if sp.GetMessageType() != pb.MessageType_TRANSACTION {
		t.Fatalf("MessageType 应回填 TRANSACTION（SDK 靠它识别回查），实际 %v", sp.GetMessageType())
	}
	if sp.GetMessageId() != m.ID || !bytes.Equal(msg.GetBody(), m.Body) {
		t.Fatalf("MessageId/Body 应与原半消息一致，实际 %q %q", sp.GetMessageId(), msg.GetBody())
	}
	if msg.GetTopic().GetName() != "t-orphan" {
		t.Fatalf("Topic 应为 t-orphan，实际 %q", msg.GetTopic().GetName())
	}
	if got := sp.GetBodyDigest().GetChecksum(); got != crc32Checksum(m.Body) {
		t.Fatalf("未声明的 digest 应兜底补算 CRC32，实际 %q", got)
	}
	if sp.GetBodyEncoding() != pb.Encoding_IDENTITY {
		t.Fatalf("BodyEncoding 应如实回填 IDENTITY，实际 %v", sp.GetBodyEncoding())
	}
	if msg.GetUserProperties()["pk"] != "pv" {
		t.Fatalf("UserProperties 应透传，实际 %v", msg.GetUserProperties())
	}
}

// TestRecoverOrphanNoProducerReturnsFalse 无可用 producer 会话时返回
// false（调度器据此打 Warn 并改期）。包括两种情形：完全没有注册会话，
// 以及只有发布其它 topic 的 producer（pickProducer 严格按 topic 匹配，
// 绝不降级到无关 producer）。
func TestRecoverOrphanNoProducerReturnsFalse(t *testing.T) {
	env := newTestEnv(t, true)

	m := &core.Message{Topic: "t-no-producer", Body: []byte("x")}
	if env.srv.RecoverOrphan(m, "TX9") {
		t.Fatal("无注册会话时 RecoverOrphan 应返回 false")
	}

	// 只有发布其它 topic 的 producer：回查发给没发布过该 topic 的进程，
	// 它的 checker 面对陌生事务多半回 ROLLBACK/UNKNOWN——不能降级。
	telemetryRegisterProducer(t, env.client, "t-other")
	if env.srv.RecoverOrphan(m, "TX9") {
		t.Fatal("topic 不匹配时应返回 false（严格按 topic 匹配）")
	}
}
