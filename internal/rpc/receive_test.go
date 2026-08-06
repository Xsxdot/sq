// receive_test.go 验证 rpc.Server 的 POP 消费方向 RPC：
// ReceiveMessage/AckMessage/ChangeInvisibleDuration/QueryAssignment。
//
// 职责：
//   - 覆盖 Receive→Ack 主链路、QueryAssignment 返回全部队列、receipt handle
//     的编解码往返
//   - 覆盖 Task 7 review 修复的 attempt 令牌语义：一条消息被重投后，用重投前
//     （陈旧 attempt）的 handle 去 Ack 必须失败，且不能影响重投后的新记录——
//     否则会重演"迟到的 Ack 误删新记录，消息永久丢失"的问题
//
// 边界：
//   - 只测协议适配层的行为（请求→响应/状态码/handle 格式），不重复测
//     deliver.Deliverer 内部的 inflight/attempt 校验逻辑（那是 deliver 包自己
//     的测试职责，这里只验证协议层正确透传/映射）
package rpc

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// recvAll 读整个 ReceiveMessage 流，分离消息与末尾 status。
func recvAll(t *testing.T, stream pb.MessagingService_ReceiveMessageClient) ([]*pb.Message, *pb.Status) {
	t.Helper()
	var msgs []*pb.Message
	var st *pb.Status
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return msgs, st
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		switch c := resp.GetContent().(type) {
		case *pb.ReceiveMessageResponse_Message:
			msgs = append(msgs, c.Message)
		case *pb.ReceiveMessageResponse_Status:
			st = c.Status
		}
	}
}

func sendOne(t *testing.T, c pb.MessagingServiceClient, topic, body string) {
	t.Helper()
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: topic},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			Body:             []byte(body),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("send: %v %v", resp.GetStatus(), err)
	}
}

func TestReceiveAckRoundTrip(t *testing.T) {
	c := newTestClient(t)
	sendOne(t, c, "rt", "hello")
	// topic 只有 4 个队列，轮询首条必落 queue 0……但为免脆弱，四个队列都试
	var got *pb.Message
	for q := int32(0); q < 4 && got == nil; q++ {
		stream, err := c.ReceiveMessage(context.Background(), &pb.ReceiveMessageRequest{
			Group: &pb.Resource{Name: "g-rt"},
			MessageQueue: &pb.MessageQueue{
				Topic: &pb.Resource{Name: "rt"}, Id: q,
			},
			FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
			BatchSize:         10,
			InvisibleDuration: durationpb.New(time.Minute),
			AutoRenew:         false,
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		msgs, _ := recvAll(t, stream)
		if len(msgs) > 0 {
			got = msgs[0]
		}
	}
	if got == nil {
		t.Fatal("未收到消息")
	}
	// 注意：GetReceiptHandle() 是 protoc-gen-go 为 proto3 optional 字段生成的
	// getter，返回值类型 string（nil 时返回零值 ""），不是 *string——底层字段
	// SystemProperties.ReceiptHandle 才是 *string。取 handle 一律走 Getter。
	handle := got.GetSystemProperties().GetReceiptHandle()
	if handle == "" {
		t.Fatal("缺少 receipt handle")
	}
	if attempt := got.GetSystemProperties().GetDeliveryAttempt(); attempt != 1 {
		t.Fatalf("首次投递 attempt 应为 1，实际 %d", attempt)
	}
	ackResp, err := c.AckMessage(context.Background(), &pb.AckMessageRequest{
		Group: &pb.Resource{Name: "g-rt"},
		Topic: &pb.Resource{Name: "rt"},
		Entries: []*pb.AckMessageEntry{{
			ReceiptHandle: handle,
			MessageId:     got.GetSystemProperties().GetMessageId(),
		}},
	})
	if err != nil || ackResp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("Ack: %v %v", ackResp.GetStatus(), err)
	}
	if len(ackResp.GetEntries()) != 1 || ackResp.GetEntries()[0].GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("ack entry: %v", ackResp.GetEntries())
	}
}

func TestQueryAssignmentReturnsAllQueues(t *testing.T) {
	c := newTestClient(t)
	sendOne(t, c, "qa", "x") // 触发 topic 创建
	resp, err := c.QueryAssignment(context.Background(), &pb.QueryAssignmentRequest{
		Topic: &pb.Resource{Name: "qa"},
		Group: &pb.Resource{Name: "g-qa"},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("QueryAssignment: %v %v", resp.GetStatus(), err)
	}
	if len(resp.GetAssignments()) != 4 {
		t.Fatalf("assignments: %d", len(resp.GetAssignments()))
	}
}

// TestReceiptRoundTrip 验证 receipt handle 编解码往返，含新增的 attempt 字段。
func TestReceiptRoundTrip(t *testing.T) {
	h := receiptEncode("g", "t", 3, 42, 7)
	g, topic, q, off, attempt, err := receiptDecode(h)
	if err != nil || g != "g" || topic != "t" || q != 3 || off != 42 || attempt != 7 {
		t.Fatalf("receipt round trip: %v %v %v %v %v %v", g, topic, q, off, attempt, err)
	}
	if _, _, _, _, _, err := receiptDecode("garbage!!"); err == nil {
		t.Fatal("非法 handle 应报错")
	}
}

// receiveOne 发起一次 ReceiveMessage，返回收到的第一条消息（没有则 nil）。
// 用有限的 context 超时代替长轮询默认值，避免测试在错误队列上等待过久。
func receiveOne(t *testing.T, c pb.MessagingServiceClient, group, topic string, queueID int32, invisible time.Duration) *pb.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: group},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: topic}, Id: queueID},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
		BatchSize:         10,
		InvisibleDuration: durationpb.New(invisible),
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

// TestAckWithStaleAttemptTokenFailsAndMessageStaysRedeliverable 锁定 Task 7 review
// 修复的核心行为端到端：消息 X 首投给消费者（attempt=1，短不可见窗口），窗口过期后
// 被重投（attempt=2）。用第一次投递的陈旧 handle（attempt=1）去 Ack，必须返回非 OK
// 状态；随后证明消息没有丢——用当前（attempt=2）的 handle 仍能成功 Ack。
//
// 如果协议层没有把 attempt 编进 handle（或 deliver.Ack 不校验它），陈旧 Ack 会
// 直接按 (group,topic,queue,offset) 删掉 attempt=2 的新记录：本测试的第二步
// （用新 handle 确认）就会失败，因为记录已经被陈旧 Ack 误删——这正是本测试要
// 防回归的场景。
func TestAckWithStaleAttemptTokenFailsAndMessageStaysRedeliverable(t *testing.T) {
	c := newTestClient(t)
	const topic = "stale"
	const group = "g-stale"
	sendOne(t, c, topic, "hello")

	// 找到消息落在哪个队列，同时拿到首投（attempt=1）的 handle；
	// 不可见窗口刻意设很短，等下要让它过期触发重投。
	var first *pb.Message
	var queueID int32
	for q := int32(0); q < 4 && first == nil; q++ {
		if m := receiveOne(t, c, group, topic, q, 150*time.Millisecond); m != nil {
			first, queueID = m, q
		}
	}
	if first == nil {
		t.Fatal("未收到消息")
	}
	// GetReceiptHandle() 返回 string（proto3 optional 字段的 getter 语义，
	// 见 TestReceiveAckRoundTrip 的注释），不是 *string。
	firstHandle := first.GetSystemProperties().GetReceiptHandle()
	msgID := first.GetSystemProperties().GetMessageId()
	if firstHandle == "" {
		t.Fatal("缺少 receipt handle")
	}
	if attempt := first.GetSystemProperties().GetDeliveryAttempt(); attempt != 1 {
		t.Fatalf("首次投递 attempt 应为 1，实际 %d", attempt)
	}

	// 等不可见窗口过期，让消息具备被重投的条件。
	time.Sleep(300 * time.Millisecond)

	second := receiveOne(t, c, group, topic, queueID, time.Minute)
	if second == nil {
		t.Fatal("消息应在不可见窗口过期后被重投")
	}
	secondHandle := second.GetSystemProperties().GetReceiptHandle()
	if secondHandle == "" {
		t.Fatal("重投消息缺少 receipt handle")
	}
	if secondHandle == firstHandle {
		t.Fatal("重投后 attempt 变化，handle 理应随之变化")
	}
	if attempt := second.GetSystemProperties().GetDeliveryAttempt(); attempt != 2 {
		t.Fatalf("重投后 attempt 应为 2，实际 %d", attempt)
	}

	// 陈旧 handle（attempt=1）确认：必须返回非 OK（INVALID_RECEIPT_HANDLE）。
	staleAck, err := c.AckMessage(context.Background(), &pb.AckMessageRequest{
		Group: &pb.Resource{Name: group},
		Topic: &pb.Resource{Name: topic},
		Entries: []*pb.AckMessageEntry{{
			ReceiptHandle: firstHandle,
			MessageId:     msgID,
		}},
	})
	if err != nil {
		t.Fatalf("AckMessage(陈旧 handle): %v", err)
	}
	if len(staleAck.GetEntries()) != 1 {
		t.Fatalf("entries: %v", staleAck.GetEntries())
	}
	if staleAck.GetEntries()[0].GetStatus().GetCode() == pb.Code_OK {
		t.Fatal("陈旧 handle 确认不应返回 OK")
	}
	if staleAck.GetEntries()[0].GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE {
		t.Fatalf("陈旧 handle 确认应返回 INVALID_RECEIPT_HANDLE，实际 %v", staleAck.GetEntries()[0].GetStatus())
	}

	// 证明消息没有被误删——用重投后（attempt=2）的当前 handle 仍能成功确认。
	freshAck, err := c.AckMessage(context.Background(), &pb.AckMessageRequest{
		Group: &pb.Resource{Name: group},
		Topic: &pb.Resource{Name: topic},
		Entries: []*pb.AckMessageEntry{{
			ReceiptHandle: secondHandle,
			MessageId:     msgID,
		}},
	})
	if err != nil {
		t.Fatalf("AckMessage(当前 handle): %v", err)
	}
	if len(freshAck.GetEntries()) != 1 || freshAck.GetEntries()[0].GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("当前 handle 确认应成功，实际 %v", freshAck.GetEntries())
	}
}
