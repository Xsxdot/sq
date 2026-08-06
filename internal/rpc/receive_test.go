// receive_test.go 验证 rpc.Server 的 POP 消费方向 RPC：
// ReceiveMessage/AckMessage/ChangeInvisibleDuration/QueryAssignment。
//
// 职责：
//   - 覆盖 Receive→Ack 主链路、QueryAssignment 返回全部队列、receipt handle
//     的编解码往返
//   - 覆盖 Task 7 review 修复的 attempt 令牌语义：一条消息被重投后，用重投前
//     （陈旧 attempt）的 handle 去 Ack 必须失败，且不能影响重投后的新记录——
//     否则会重演"迟到的 Ack 误删新记录，消息永久丢失"的问题
//   - 覆盖协议层错误分类：ReceiveMessage 对非法消费组名字必须报
//     ILLEGAL_CONSUMER_GROUP 而不是 INTERNAL_SERVER_ERROR（同 QueryAssignment
//     对 EnsureTopic 错误的分类原则，不能把客户端输入错误折叠成服务端故障）
//   - 覆盖 AckMessage 顶层 Status 的汇总语义：批量结果不全相同时必须报
//     MULTIPLE_RESULTS，不能固定 OK（只看顶层 Status 的客户端否则会把
//     部分失败的批次误判为全部成功，永远不会重试）
//   - 覆盖 handle 解析失败的 entry 必须回填 ReceiptHandle，供客户端在
//     MessageId 为空时仍能关联回自己请求里的哪个 entry
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
	// topic 只有 4 个队列，轮询首条必落 queue 0……但为免脆弱，四个队列都试。
	// 用 receiveOne（内部 2s 的有限 context）而不是 context.Background()：
	// 没消息的队列会长轮询到 longPollWait 算出的等待时长，用无限 context
	// 会导致每个未命中的队列各自等满 20s，最坏情况把这条本该几十毫秒完成
	// 的测试拖到接近 60s。
	var got *pb.Message
	for q := int32(0); q < 4 && got == nil; q++ {
		got = receiveOne(t, c, "g-rt", "rt", q, time.Minute)
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

// TestReceiveMessageRejectsIllegalConsumerGroup 验证消费组名字非法
// （deliver.Receive 内部 EnsureGroup 报 meta.ErrBadName）时返回
// Code_ILLEGAL_CONSUMER_GROUP——这是客户端可自行处理（改名字重试没用，
// 但也不该无脑重试）的输入错误，不能被折叠成 Code_INTERNAL_SERVER_ERROR。
// 折叠成后者会诱导一个用非法组名轮询的消费者把它当瞬时故障无限重试，
// 在轮询频率下形成 Error 级别的日志洪水，而这个错误永远不会因为重试变好。
func TestReceiveMessageRejectsIllegalConsumerGroup(t *testing.T) {
	c := newTestClient(t)
	stream, err := c.ReceiveMessage(context.Background(), &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: "bad/group"},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: "rt-illegal-group"}, Id: 0},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
		BatchSize:         10,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	_, st := recvAll(t, stream)
	if st.GetCode() != pb.Code_ILLEGAL_CONSUMER_GROUP {
		t.Fatalf("status: 期望 ILLEGAL_CONSUMER_GROUP，得到 %v", st)
	}
}

// TestAckMessageAggregatesMixedResultsAsMultipleResults 验证批量确认里一条
// 成功、一条失败（用已经确认过一次、因而失效的 handle）时，顶层 Status 必须
// 是 Code_MULTIPLE_RESULTS，而不是固定 OK——只检查顶层 Status 的客户端
// （常见 SDK 形状）需要靠这个字段判断"这批是否需要重试"，固定 OK 会让它
// 把失败的那条误判为已确认，永远不会重试。
func TestAckMessageAggregatesMixedResultsAsMultipleResults(t *testing.T) {
	c := newTestClient(t)
	const topic = "ack-mixed"
	const group = "g-ack-mixed"
	sendOne(t, c, topic, "a")
	sendOne(t, c, topic, "b")

	// produce 按 topic 轮询分配队列，两条消息各落在不同队列；把 4 个队列
	// 都试一遍凑够 2 条（凑够即停，不会在没消息的队列上多等）。
	var msgs []*pb.Message
	for q := int32(0); q < 4 && len(msgs) < 2; q++ {
		if m := receiveOne(t, c, group, topic, q, time.Minute); m != nil {
			msgs = append(msgs, m)
		}
	}
	if len(msgs) != 2 {
		t.Fatalf("期望收到 2 条消息，实际 %d", len(msgs))
	}

	// 先把第一条 ack 掉，让它的 handle 变成"已失效"（第二次 ack 会落空）。
	h0 := msgs[0].GetSystemProperties().GetReceiptHandle()
	id0 := msgs[0].GetSystemProperties().GetMessageId()
	pre, err := c.AckMessage(context.Background(), &pb.AckMessageRequest{
		Group: &pb.Resource{Name: group}, Topic: &pb.Resource{Name: topic},
		Entries: []*pb.AckMessageEntry{{ReceiptHandle: h0, MessageId: id0}},
	})
	if err != nil || pre.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("预备 ack 失败: %v %v", pre.GetStatus(), err)
	}

	// 批量确认：entry0 用已失效的 h0（应失败），entry1 用未 ack 过的 h1（应成功）。
	h1 := msgs[1].GetSystemProperties().GetReceiptHandle()
	id1 := msgs[1].GetSystemProperties().GetMessageId()
	resp, err := c.AckMessage(context.Background(), &pb.AckMessageRequest{
		Group: &pb.Resource{Name: group}, Topic: &pb.Resource{Name: topic},
		Entries: []*pb.AckMessageEntry{
			{ReceiptHandle: h0, MessageId: id0},
			{ReceiptHandle: h1, MessageId: id1},
		},
	})
	if err != nil {
		t.Fatalf("AckMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_MULTIPLE_RESULTS {
		t.Fatalf("顶层 status 应为 MULTIPLE_RESULTS，实际 %v", resp.GetStatus())
	}
	if len(resp.GetEntries()) != 2 ||
		resp.GetEntries()[0].GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE ||
		resp.GetEntries()[1].GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("entries: %v", resp.GetEntries())
	}
}

// TestAckMessageMalformedHandleEntryIncludesReceiptHandle 验证 handle 解析
// 失败的 entry 仍然带上原始（虽然非法）ReceiptHandle：MessageId 在请求里
// 可能为空（proto 字段非必填），此时 ReceiptHandle 是客户端把这条失败结果
// 关联回自己请求里对应 entry 的唯一线索，遗漏它会让调用方在 MessageId
// 为空时无法定位是哪条 ack 失败了。顺带验证单条失败时顶层 Status 也如实
// 反映失败（不是被固定成 OK）。
func TestAckMessageMalformedHandleEntryIncludesReceiptHandle(t *testing.T) {
	c := newTestClient(t)
	const badHandle = "not-a-valid-handle!!"
	resp, err := c.AckMessage(context.Background(), &pb.AckMessageRequest{
		Group:   &pb.Resource{Name: "g-malformed"},
		Topic:   &pb.Resource{Name: "malformed"},
		Entries: []*pb.AckMessageEntry{{ReceiptHandle: badHandle}},
	})
	if err != nil {
		t.Fatalf("AckMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE {
		t.Fatalf("顶层 status 应反映唯一 entry 的失败，实际 %v", resp.GetStatus())
	}
	if len(resp.GetEntries()) != 1 {
		t.Fatalf("entries: %v", resp.GetEntries())
	}
	e := resp.GetEntries()[0]
	if e.GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE {
		t.Fatalf("entry status: %v", e.GetStatus())
	}
	if e.GetReceiptHandle() != badHandle {
		t.Fatalf("ReceiptHandle 应回显原始值，实际 %q", e.GetReceiptHandle())
	}
}

// TestReceiveMessageEmptyPollReportsMessageNotFound 锁定长轮询空结果的协议表述：
// 必须是一帧 MESSAGE_NOT_FOUND status，且不带任何消息帧。
//
// 不能回 OK + 零条消息：官方 SDK 的 push 消费者只对 MESSAGE_NOT_FOUND 这一个码
// 有「没有新消息」的识别分支（命中后按流控退避重发取件请求），回 OK 会让它把
// 空结果当成一次成功取件、立刻无退避地再发一次。详见 receive.go 该分支的注释，
// 以及 test/e2e 里用真实 SDK 断言同一行为的用例。
func TestReceiveMessageEmptyPollReportsMessageNotFound(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: "g-empty"},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: "empty-topic"}, Id: 0},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
		BatchSize:         10,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	msgs, st := recvAll(t, stream)
	if len(msgs) != 0 {
		t.Fatalf("空队列不应返回消息，实际 %d 条", len(msgs))
	}
	if st.GetCode() != pb.Code_MESSAGE_NOT_FOUND {
		t.Fatalf("空轮询状态码应为 MESSAGE_NOT_FOUND，实际 %v", st)
	}
}
