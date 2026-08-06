// receive_test.go 验证 rpc.Server 的 POP 消费方向 RPC：
// ReceiveMessage/AckMessage/ChangeInvisibleDuration/QueryAssignment。
//
// 职责：
//   - 覆盖 Receive→Ack 主链路、QueryAssignment 返回全部队列、receipt handle
//     的编解码往返
//   - 覆盖 attempt 令牌语义：一条消息被重投后，用重投前（陈旧 attempt）的
//     handle 去 Ack 必须失败，且不能影响重投后的新记录——否则会重演"迟到的
//     Ack 误删新记录，消息永久丢失"的问题
//   - 覆盖 SystemProperties 透传字段（body_encoding/body_digest/born_host/
//     trace_context）的收发往返：这几个字段 sq 不解释，但少带任何一个都会造成
//     消费端可见的损失（编码丢失 → Body 被误解释；trace_context 丢失 → 链路断开）
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
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xushixin/sq/internal/core"
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

// TestSendReceivePreservesPassthroughSystemProperties 锁定四个"sq 不解释、
// 只负责原样带回"的 SystemProperties 字段能完整走完 发送 → 落盘 → 投递 的往返：
// body_encoding、body_digest、born_host、trace_context。
//
// 这四个字段曾经写方向和读方向都没接，于是被静默丢弃。危害各不相同，但没有
// 一个是无害的：
//   - body_encoding：最严重。sq 只存字节、从不解压，若丢掉这个字段，一个把
//     Body 压缩过再发的生产者（Java SDK 超过压缩阈值就会这么做）在消费端会
//     拿回 ENCODING_UNSPECIFIED——SDK 走不到解压分支，直接把压缩字节交给应用。
//     没有任何一层会报错，是静默的数据损坏。
//   - trace_context：分布式链路在 sq 这一跳断掉，且断得无声无息。
//   - body_digest：消费端失去校验消息体完整性的依据。
//   - born_host：排查"这条消息是谁发的"时失去唯一线索。
//
// 这里刻意用 GZIP 而不是 IDENTITY：IDENTITY 恰好是官方 Go SDK 硬编码的值，
// 用它就分不清"字段真的透传了"还是"某处把它写死成了 IDENTITY"。
func TestSendReceivePreservesPassthroughSystemProperties(t *testing.T) {
	c := newTestClient(t)
	const topic = "passthrough"
	const traceContext = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	const bornHost = "10.0.0.7:54321"
	const checksum = "3610a686"

	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: topic},
			SystemProperties: &pb.SystemProperties{
				MessageType:  pb.MessageType_NORMAL,
				BodyEncoding: pb.Encoding_GZIP,
				BodyDigest:   &pb.Digest{Type: pb.DigestType_CRC32, Checksum: checksum},
				BornHost:     bornHost,
				TraceContext: strPtr(traceContext),
			},
			Body: []byte("compressed-payload"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("SendMessage: %v %v", resp.GetStatus(), err)
	}

	var got *pb.Message
	for q := int32(0); q < 4 && got == nil; q++ {
		got = receiveOne(t, c, "g-passthrough", topic, q, time.Minute)
	}
	if got == nil {
		t.Fatal("未收到消息")
	}
	sp := got.GetSystemProperties()
	if sp.GetBodyEncoding() != pb.Encoding_GZIP {
		t.Fatalf("body_encoding 应原样带回 GZIP，实际 %v（消费端会把压缩字节当明文交给应用）",
			sp.GetBodyEncoding())
	}
	if sp.GetBodyDigest().GetType() != pb.DigestType_CRC32 || sp.GetBodyDigest().GetChecksum() != checksum {
		t.Fatalf("body_digest 应原样带回，实际 %v", sp.GetBodyDigest())
	}
	if sp.GetBornHost() != bornHost {
		t.Fatalf("born_host 应原样带回 %q，实际 %q", bornHost, sp.GetBornHost())
	}
	// TraceContext 是 proto3 optional（*string）：先断言指针非 nil 再比值，
	// 否则 Getter 的零值会把"压根没下发"伪装成"下发了空串"。
	if sp.TraceContext == nil {
		t.Fatal("trace_context 缺失：分布式链路在 sq 这一跳断掉了")
	}
	if sp.GetTraceContext() != traceContext {
		t.Fatalf("trace_context 应原样带回 %q，实际 %q", traceContext, sp.GetTraceContext())
	}
}

// TestReceiveBackfillsDigestAndNormalizesEncodingWhenProducerOmitsThem 锁定
// 投递路径对"生产者什么都没声明"的兜底：digest 缺失时按 SDK 校验式补算 CRC32，
// encoding 未声明时归一化为 IDENTITY。
//
// 为什么需要兜底（而不是继续纯透传）：官方 Go SDK v5.1.4 的生产者
// （publishing_message.go）只设置 Encoding_IDENTITY，**从不设置 BodyDigest**；
// 而它的消费端（message.go fromProtobuf_MessageView2）对 digest 类型为
// UNSPECIFIED 的每条消息都会打一条
// "unsupported message body digest algorithm" WARN。纯透传意味着官方 SDK
// 自产自销的每一条消息都在消费端刷一条 WARN——功能可用（消息不会被标
// corrupted），但生产环境的客户端日志会被刷爆。
//
// 兜底只在"缺失"时生效：已有 digest/encoding 的照旧透传（那是端到端校验的
// 语义所在，由 TestSendReceivePreservesPassthroughSystemProperties 锁定，
// 该测试同时保证 GZIP 不会被这里的归一化改写）。
func TestReceiveBackfillsDigestAndNormalizesEncodingWhenProducerOmitsThem(t *testing.T) {
	c := newTestClient(t)
	const topic = "digest-backfill"
	const body = "no-digest-payload"
	// sendOne 只带 MessageType，不带 digest/encoding——正是官方 Go SDK 生产者
	// 发出的 SystemProperties 形态（encoding 它会设 IDENTITY，这里连 encoding
	// 也不带，顺带覆盖"未声明 encoding 归一化"分支）。
	sendOne(t, c, topic, body)

	var got *pb.Message
	for q := int32(0); q < 4 && got == nil; q++ {
		got = receiveOne(t, c, "g-digest-backfill", topic, q, time.Minute)
	}
	if got == nil {
		t.Fatal("未收到消息")
	}
	sp := got.GetSystemProperties()
	// 期望校验和必须按 SDK message.go 的原式计算（无前导零、大写十六进制），
	// 逐字符一致才不会被消费端标记 corrupted。
	wantChecksum := strings.ToUpper(strconv.FormatInt(int64(crc32.ChecksumIEEE([]byte(body))), 16))
	if sp.GetBodyDigest().GetType() != pb.DigestType_CRC32 {
		t.Fatalf("digest 缺失时应补算 CRC32，实际类型 %v（SDK 消费端会对 UNSPECIFIED 每条消息刷 WARN）",
			sp.GetBodyDigest().GetType())
	}
	if sp.GetBodyDigest().GetChecksum() != wantChecksum {
		t.Fatalf("补算的 CRC32 校验和应为 %q，实际 %q（不一致会被 SDK 标记 corrupted）",
			wantChecksum, sp.GetBodyDigest().GetChecksum())
	}
	if sp.GetBodyEncoding() != pb.Encoding_IDENTITY {
		t.Fatalf("未声明 encoding 应归一化为 IDENTITY，实际 %v", sp.GetBodyEncoding())
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

// TestAckWithStaleAttemptTokenFailsAndMessageStaysRedeliverable 端到端锁定
// attempt 令牌：消息 X 首投给消费者（attempt=1，短不可见窗口），窗口过期后
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

// TestToPBMessageSetsInvisibleDuration 下发消息须回填本次的不可见时长，
// SDK 依此换算消息可见时间点展示/重试。
func TestToPBMessageSetsInvisibleDuration(t *testing.T) {
	s := &Server{}
	msg := s.toPBMessage(&core.Message{ID: "A", Topic: "t", Body: []byte("x"), DeliveryAttempt: 1}, "g", 45*time.Second)
	if got := msg.GetSystemProperties().GetInvisibleDuration().AsDuration(); got != 45*time.Second {
		t.Fatalf("InvisibleDuration: 期望 45s，得到 %v", got)
	}
}

// TestToPBMessageBackfillsRetryBackoffFloorOnRedelivery 重投消息（attempt>=2）下发
// InvisibleDuration 必须反映实际过期语义 max(客户端要求, 退避下限)：
// receiveOnce 里重投的过期时间是 exp=now+max(invisible,backoff)（deliver.go），
// 若这里仍回填客户端原值，SDK 换算出的可见时间点会早于服务端实际，消费端
// 展示/重试节奏与服务端不一致。首投（attempt=1）无退避概念，保持客户端值。
func TestToPBMessageBackfillsRetryBackoffFloorOnRedelivery(t *testing.T) {
	s := &Server{}
	redelivered := s.toPBMessage(&core.Message{ID: "B", Topic: "t", Body: []byte("x"), DeliveryAttempt: 2}, "g", time.Second)
	if got := redelivered.GetSystemProperties().GetInvisibleDuration().AsDuration(); got != 10*time.Second {
		t.Fatalf("重投消息 InvisibleDuration 应回填退避下限 10s，实际 %v", got)
	}
	fresh := s.toPBMessage(&core.Message{ID: "C", Topic: "t", Body: []byte("x"), DeliveryAttempt: 1}, "g", 5*time.Second)
	if got := fresh.GetSystemProperties().GetInvisibleDuration().AsDuration(); got != 5*time.Second {
		t.Fatalf("首投消息 InvisibleDuration 应保持客户端值 5s，实际 %v", got)
	}
}

// TestToPBMessageEchoesDelayTypeAndTimestamp 锁定投递方向的 DELAY 回填：
// 盘上带 DeliverAtMs 的消息投递时，MessageType 必须如实回填为 DELAY、
// DeliveryTimestamp 必须回填为到期时间；普通消息保持 NORMAL 且不带
// DeliveryTimestamp。
func TestToPBMessageEchoesDelayTypeAndTimestamp(t *testing.T) {
	env := newTestEnv(t, true)
	due := time.Now().Add(-time.Second).UnixMilli() // 已过期：发送即直通立即投递
	m := &core.Message{ID: "d1", Topic: "t", Body: []byte("x"), DeliverAtMs: due, DeliveryAttempt: 1}
	pm := env.srv.toPBMessage(m, "g", time.Minute)
	if pm.GetSystemProperties().GetMessageType() != pb.MessageType_DELAY {
		t.Fatalf("延时消息投递类型应为 DELAY，得到 %v", pm.GetSystemProperties().GetMessageType())
	}
	got := pm.GetSystemProperties().GetDeliveryTimestamp()
	if got == nil || got.AsTime().UnixMilli() != due {
		t.Fatalf("DeliveryTimestamp 未回填: %v", got)
	}
	// 普通消息不受影响
	n := &core.Message{ID: "n1", Topic: "t", Body: []byte("y"), DeliveryAttempt: 1}
	pn := env.srv.toPBMessage(n, "g", time.Minute)
	if pn.GetSystemProperties().GetMessageType() != pb.MessageType_NORMAL ||
		pn.GetSystemProperties().GetDeliveryTimestamp() != nil {
		t.Fatal("普通消息不应带 DELAY 类型或 DeliveryTimestamp")
	}
}

// 全链路：过期时间戳的 DELAY 消息发送后直通立即投递，消费端读回自己设置的时间
func TestSendPastDueDelayDeliveredImmediatelyWithTimestampEcho(t *testing.T) {
	env := newTestEnv(t, true)
	due := time.Now().Add(-time.Second)
	resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "dly2"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_DELAY,
				DeliveryTimestamp: timestamppb.New(due),
			},
			Body: []byte("imm"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("发送: %v %v", resp.GetStatus(), err)
	}
	pm := receiveOne(t, env.client, "g", "dly2", 0, time.Minute)
	if pm.GetSystemProperties().GetMessageType() != pb.MessageType_DELAY {
		t.Fatal("投递类型应为 DELAY")
	}
	if ts := pm.GetSystemProperties().GetDeliveryTimestamp(); ts == nil || ts.AsTime().UnixMilli() != due.UnixMilli() {
		t.Fatalf("DeliveryTimestamp 回读不符: %v", ts)
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

// sendTagged 发送带 tag 的消息（Tag 是 *string，需取址）。
func sendTagged(t *testing.T, c pb.MessagingServiceClient, topic, body, tag string) {
	t.Helper()
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: topic},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL, Tag: &tag},
			Body:             []byte(body),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("send: %v %v", resp.GetStatus(), err)
	}
}

// receiveQueue 从指定队列收一次，返回消息（helper：给过滤用例复用）。
// 3s deadline 让空队列长轮询快速返回（服务端 wait=deadline-1s≈2s），
// 否则无 deadline 时默认长轮询 20s，空队列用例会拖慢整个测试。
func receiveQueue(t *testing.T, c pb.MessagingServiceClient, group, topic string, q int32, expr string) []*pb.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: group},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: topic}, Id: q},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: expr},
		BatchSize:         16,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	msgs, _ := recvAll(t, stream)
	return msgs
}

// TestReceiveTagFilter 8 条消息 tagA/tagB 交替，按 tagA 过滤只收 4 条 A；
// 被过滤的 B 已被位点跳过，事后用 "*" 也收不到。
func TestReceiveTagFilter(t *testing.T) {
	c := newTestClient(t)
	for i := 0; i < 8; i++ {
		tag, body := "tagA", "a"
		if i%2 == 1 {
			tag, body = "tagB", "b"
		}
		sendTagged(t, c, "tf", body, tag)
	}
	var got []string
	for q := int32(0); q < 4; q++ {
		for _, m := range receiveQueue(t, c, "g-tf", "tf", q, "tagA") {
			got = append(got, string(m.GetBody()))
		}
	}
	if len(got) != 4 {
		t.Fatalf("tagA 消息数: %d (%v)", len(got), got)
	}
	for _, b := range got {
		if b != "a" {
			t.Fatalf("混入非 tagA 消息: %v", got)
		}
	}
	for q := int32(0); q < 4; q++ {
		if rest := receiveQueue(t, c, "g-tf", "tf", q, "*"); len(rest) != 0 {
			t.Fatalf("被过滤消息不应可再收: %d", len(rest))
		}
	}
}

// TestReceiveRejectsUnsupportedFilter SQL92 与非法 TAG 表达式返回 ILLEGAL_FILTER_EXPRESSION。
func TestReceiveRejectsUnsupportedFilter(t *testing.T) {
	c := newTestClient(t)
	sendOne(t, c, "tf-bad", "x")
	cases := []*pb.FilterExpression{
		{Type: pb.FilterType_SQL, Expression: "a > 1"},
		{Type: pb.FilterType_TAG, Expression: "a ||"},
	}
	for _, fe := range cases {
		stream, err := c.ReceiveMessage(context.Background(), &pb.ReceiveMessageRequest{
			Group:             &pb.Resource{Name: "g-bad"},
			MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: "tf-bad"}, Id: 0},
			FilterExpression:  fe,
			BatchSize:         1,
			InvisibleDuration: durationpb.New(time.Minute),
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		msgs, st := recvAll(t, stream)
		if len(msgs) != 0 || st.GetCode() != pb.Code_ILLEGAL_FILTER_EXPRESSION {
			t.Fatalf("期望 ILLEGAL_FILTER_EXPRESSION，得到 %v (msgs=%d)", st, len(msgs))
		}
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
