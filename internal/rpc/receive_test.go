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

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Xsxdot/sq/internal/config"
	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/deliver"
	pb "github.com/Xsxdot/sq/internal/rpc/pb/apache/rocketmq/v2"
	"github.com/Xsxdot/sq/internal/store"
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
	secret := []byte("test-handle-secret")
	h := receiptEncode(secret, "g", "t", 3, 42, 7)
	g, topic, q, off, attempt, err := receiptDecode(secret, h)
	if err != nil || g != "g" || topic != "t" || q != 3 || off != 42 || attempt != 7 {
		t.Fatalf("receipt round trip: %v %v %v %v %v %v", g, topic, q, off, attempt, err)
	}
	if _, _, _, _, _, err := receiptDecode(secret, "garbage!!"); err == nil {
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
	// 先经 QueryRoute 建 topic：入口校验（Task 3）会以 TOPIC_NOT_FOUND 拒绝
	// 不存在的 topic，本用例要测的是组名校验，topic 必须先存在
	if _, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{Topic: &pb.Resource{Name: "rt-illegal-group"}}); err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
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

// TestReceiveRejectsUnsupportedFilter 非法过滤表达式返回 ILLEGAL_FILTER_EXPRESSION。
// （SQL92 已接线，合法表达式不再走拒绝分支，其协议行为见 TestReceiveSQL92Filter。）
func TestReceiveRejectsUnsupportedFilter(t *testing.T) {
	c := newTestClient(t)
	sendOne(t, c, "tf-bad", "x")
	cases := []*pb.FilterExpression{
		{Type: pb.FilterType_TAG, Expression: "a ||"},
		{Type: pb.FilterType_SQL, Expression: "k = NULL"}, // 构建期语义拒绝（请用 k IS NULL）
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

// sendProps 发送带用户属性的消息（SQL92 属性过滤用例专用辅助）。
func sendProps(t *testing.T, c pb.MessagingServiceClient, topic, body string, props map[string]string) {
	t.Helper()
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: topic},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			UserProperties:   props,
			Body:             []byte(body),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("send: %v %v", resp.GetStatus(), err)
	}
}

// receiveQueueSQL 与 receiveQueue 同款，但用 SQL92 过滤表达式取件。
func receiveQueueSQL(t *testing.T, c pb.MessagingServiceClient, group, topic string, q int32, expr string) []*pb.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: group},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: topic}, Id: q},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_SQL, Expression: expr},
		BatchSize:         16,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	msgs, _ := recvAll(t, stream)
	return msgs
}

// TestReceiveSQL92Filter 协议层走通 SQL92 属性过滤：只投属性命中表达式的
// 消息；不命中的被永久跳过（换全量也收不到）。
func TestReceiveSQL92Filter(t *testing.T) {
	c := newTestClient(t)
	sendProps(t, c, "tsql", "hit", map[string]string{"age": "20"})
	sendProps(t, c, "tsql", "miss", map[string]string{"age": "5"})
	var got []string
	for q := int32(0); q < 4; q++ {
		for _, m := range receiveQueueSQL(t, c, "g-tsql", "tsql", q, "age > 10") {
			got = append(got, string(m.GetBody()))
		}
	}
	if len(got) != 1 || got[0] != "hit" {
		t.Fatalf("SQL92 过滤应只收到 age>10 的消息: %v", got)
	}
	// 被过滤的 miss 已被位点跳过，事后用 "*" 也收不到
	for q := int32(0); q < 4; q++ {
		if rest := receiveQueue(t, c, "g-tsql", "tsql", q, "*"); len(rest) != 0 {
			t.Fatalf("被过滤消息不应可再收: %d", len(rest))
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
	// 先经 QueryRoute 建 topic："空轮询"的语义是 topic 存在但没有消息；
	// 不存在的 topic 会被入口校验（Task 3）以 TOPIC_NOT_FOUND 拒绝，
	// 那不是本用例要锁的协议形状
	if _, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{Topic: &pb.Resource{Name: "empty-topic"}}); err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
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

func TestReceiveRejectsUnknownTopic(t *testing.T) {
	// 从未被 QueryRoute/Send 创建过的 topic 直接 Receive → TOPIC_NOT_FOUND。
	// 正常 SDK 到不了这个分支（它先 QueryRoute，autoCreate 时那里已建 topic），
	// 挡的是绕过路由的手写客户端与已删除 topic
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: "g-nt"},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: "never-created"}, Id: 0},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
		BatchSize:         10,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	_, st := recvAll(t, stream)
	if st.GetCode() != pb.Code_TOPIC_NOT_FOUND {
		t.Fatalf("状态码应为 TOPIC_NOT_FOUND，实际 %v", st)
	}
}

func TestReceiveRejectsOutOfRangeQueue(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// 先经 QueryRoute 自动建 topic（默认 4 队列），再请求越界队列 99
	if _, err := c.QueryRoute(ctx, &pb.QueryRouteRequest{Topic: &pb.Resource{Name: "t-oob"}}); err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: "g-oob"},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: "t-oob"}, Id: 99},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
		BatchSize:         10,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	_, st := recvAll(t, stream)
	if st.GetCode() != pb.Code_BAD_REQUEST {
		t.Fatalf("状态码应为 BAD_REQUEST，实际 %v", st)
	}
}

// 投递方向 FIFO 回填：盘上带 MessageGroup 的消息类型必须回填 FIFO；
// DeliverAtMs 优先（写方向已拒绝两者组合，读方向仍需确定性优先级）
func TestToPBMessageEchoesFifoType(t *testing.T) {
	env := newTestEnv(t, true)
	m := &core.Message{ID: "f1", Topic: "t", Body: []byte("x"), MessageGroup: "grp", DeliveryAttempt: 1}
	pm := env.srv.toPBMessage(m, "g", time.Minute)
	sp := pm.GetSystemProperties()
	if sp.GetMessageType() != pb.MessageType_FIFO || sp.GetMessageGroup() != "grp" {
		t.Fatalf("FIFO 回填不符: type=%v group=%q", sp.GetMessageType(), sp.GetMessageGroup())
	}
	// 组合数据（理论上写方向已拒绝）按 DELAY 优先，保证确定性
	both := &core.Message{ID: "f2", Topic: "t", Body: []byte("x"), MessageGroup: "grp",
		DeliverAtMs: time.Now().UnixMilli(), DeliveryAttempt: 1}
	if env.srv.toPBMessage(both, "g", time.Minute).GetSystemProperties().GetMessageType() != pb.MessageType_DELAY {
		t.Fatal("DeliverAtMs 与 MessageGroup 并存时应按 DELAY 回填")
	}
}

// 顺序消息重投的 InvisibleDuration 回填不套退避下限：deliver 侧对顺序消息
// 不退避（Task 2），协议层若仍按退避公式回填，SDK 换算出的可见时间点会
// 晚于服务端实际，消费端白等
func TestToPBMessageOrderedRedeliveryNoBackoffEcho(t *testing.T) {
	env := newTestEnv(t, true)
	ord := &core.Message{ID: "o1", Topic: "t", Body: []byte("x"), MessageGroup: "grp", DeliveryAttempt: 3}
	pm := env.srv.toPBMessage(ord, "g", time.Second)
	if d := pm.GetSystemProperties().GetInvisibleDuration().AsDuration(); d != time.Second {
		t.Fatalf("顺序重投回填应用客户端值 1s，得到 %v", d)
	}
	// 对照：非顺序重投仍套退避下限（既有行为不回归）
	norm := &core.Message{ID: "n1", Topic: "t", Body: []byte("x"), DeliveryAttempt: 3}
	pm2 := env.srv.toPBMessage(norm, "g", time.Second)
	if d := pm2.GetSystemProperties().GetInvisibleDuration().AsDuration(); d != deliver.RetryBackoff(3) {
		t.Fatalf("非顺序重投应回填退避下限 %v，得到 %v", deliver.RetryBackoff(3), d)
	}
}

// 全链路：FIFO 发送 → 投递 → 消费端读回类型与组名
func TestSendFifoDeliveredWithTypeEcho(t *testing.T) {
	env := newTestEnv(t, true)
	resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fifo2"},
			SystemProperties: &pb.SystemProperties{
				MessageType:  pb.MessageType_FIFO,
				MessageGroup: strPtr("grp-2"),
			},
			Body: []byte("hello"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("发送: %v %v", resp.GetStatus(), err)
	}
	// MessageGroup 经 hash 定队（produce.go），组名 "grp-2" 落哪条队列不固定，
	// 逐队列收取（凑够即停），与包内其它全链路用例的取法一致。
	var pm *pb.Message
	for q := int32(0); q < 4 && pm == nil; q++ {
		pm = receiveOne(t, env.client, "g", "fifo2", q, time.Minute)
	}
	if pm == nil {
		t.Fatal("未收到 FIFO 消息")
	}
	sp := pm.GetSystemProperties()
	if sp.GetMessageType() != pb.MessageType_FIFO || sp.GetMessageGroup() != "grp-2" {
		t.Fatalf("投递回读不符: type=%v group=%q", sp.GetMessageType(), sp.GetMessageGroup())
	}
}

// TestAckMessageBatchSameQueueSingleCommit 验证同队列多 entry 走批量路径：
// 全部成功、响应与请求同序；再次整批 ack 全部落空（INVALID_RECEIPT_HANDLE），
// 证明第一批真正生效且幂等语义不变。
func TestAckMessageBatchSameQueueSingleCommit(t *testing.T) {
	c := newTestClient(t)
	const topic = "ack-batch-q"
	const group = "g-ack-batch"
	// 同一队列凑 3 条：12 条按轮询均匀落在 4 个队列（produce.go 的 rr 分配），
	// 队列 0 恰好 3 条。必须用 receiveQueue 一次收整批而非 receiveOne 逐条收：
	// 批量投递（BatchSize=10）会把队列 0 的 3 条一次全部投出（cursor 越过、
	// 全部标记 inflight），receiveOne 只返回第一条，剩余 2 条在不可见期内
	// 逐条轮询再也拿不到，循环必然以"收不满"失败。
	for i := 0; i < 12; i++ {
		sendOne(t, c, topic, "m")
	}
	msgs := receiveQueue(t, c, group, topic, 0, "*")
	if len(msgs) < 3 {
		t.Fatalf("队列 0 消息数 = %d, want >= 3", len(msgs))
	}
	var handles, ids []string
	for _, m := range msgs[:3] {
		handles = append(handles, m.GetSystemProperties().GetReceiptHandle())
		ids = append(ids, m.GetSystemProperties().GetMessageId())
	}
	req := &pb.AckMessageRequest{Group: &pb.Resource{Name: group}, Topic: &pb.Resource{Name: topic}}
	for i := range handles {
		req.Entries = append(req.Entries, &pb.AckMessageEntry{ReceiptHandle: handles[i], MessageId: ids[i]})
	}
	resp, err := c.AckMessage(context.Background(), req)
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("批量 ack: %v %v", resp.GetStatus(), err)
	}
	if len(resp.GetEntries()) != 3 {
		t.Fatalf("entries = %d, want 3", len(resp.GetEntries()))
	}
	for i, e := range resp.GetEntries() {
		if e.GetStatus().GetCode() != pb.Code_OK || e.GetMessageId() != ids[i] {
			t.Fatalf("entry %d 顺序或状态错误: %v", i, e)
		}
	}
	// 重放同一批：全部应落空（幂等语义与逐条路径一致）
	again, err := c.AckMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("重放: %v", err)
	}
	for i, e := range again.GetEntries() {
		if e.GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE {
			t.Fatalf("重放 entry %d 应 INVALID_RECEIPT_HANDLE: %v", i, e)
		}
	}
}

// TestAckMessageMixedQueuesGrouped 验证跨队列 entries 正确分组：不同队列的
// 消息在同一请求中确认，全部成功且响应保持请求顺序。
func TestAckMessageMixedQueuesGrouped(t *testing.T) {
	c := newTestClient(t)
	const topic = "ack-mix-q"
	const group = "g-ack-mix-q"
	sendOne(t, c, topic, "a")
	sendOne(t, c, topic, "b")
	// 两条消息按轮询落在不同队列，逐队列收齐
	var msgs []*pb.Message
	for q := int32(0); q < 4 && len(msgs) < 2; q++ {
		if m := receiveOne(t, c, group, topic, q, time.Minute); m != nil {
			msgs = append(msgs, m)
		}
	}
	if len(msgs) != 2 {
		t.Fatalf("期望 2 条，实际 %d", len(msgs))
	}
	req := &pb.AckMessageRequest{Group: &pb.Resource{Name: group}, Topic: &pb.Resource{Name: topic}}
	for _, m := range msgs {
		req.Entries = append(req.Entries, &pb.AckMessageEntry{
			ReceiptHandle: m.GetSystemProperties().GetReceiptHandle(),
			MessageId:     m.GetSystemProperties().GetMessageId(),
		})
	}
	resp, err := c.AckMessage(context.Background(), req)
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("跨队列批量 ack: %v %v", resp.GetStatus(), err)
	}
	for i, e := range resp.GetEntries() {
		if e.GetStatus().GetCode() != pb.Code_OK ||
			e.GetMessageId() != msgs[i].GetSystemProperties().GetMessageId() {
			t.Fatalf("entry %d 顺序或状态错误: %v", i, e)
		}
	}
}

// TestLeaseForAutoRenew 锁定 leaseFor 的三开关判据：客户端请求了 auto_renew、
// 服务端配置未关闭、能从 metadata 取到 x-mq-client-id——三个全真才启用租约，
// 任一不满足都退化回零值（deliver 侧视为不启用，固定不可见期）。
//
// 边界语义（与 ReceiveMessage 接线共享）：缺 client-id 头是合法的（手写客户端
// 不带该头），只该享受不到续租，绝不能报错/拒绝请求，所以这里断言「退化而不
// 报错」而非返回错误。
func TestLeaseForAutoRenew(t *testing.T) {
	ss := newSessions()
	base := &config.Config{AutoRenewEnabled: true, AutoRenewMaxDuration: "30s"}
	withID := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(clientIDHeaderKey, "cli-1"))

	t.Run("正常启用", func(t *testing.T) {
		l := leaseFor(withID, base, ss, true)
		if !l.Enabled() {
			t.Fatal("应当启用续租")
		}
		if l.Owner != "cli-1" || l.MaxRenew != 30*time.Second {
			t.Fatalf("租约参数错误: %+v", l)
		}
	})
	t.Run("客户端未请求续租", func(t *testing.T) {
		if leaseFor(withID, base, ss, false).Enabled() {
			t.Fatal("客户端未设 auto_renew 时不应启用")
		}
	})
	t.Run("服务端配置关闭", func(t *testing.T) {
		off := &config.Config{AutoRenewEnabled: false, AutoRenewMaxDuration: "30s"}
		if leaseFor(withID, off, ss, true).Enabled() {
			t.Fatal("配置关闭时不应启用")
		}
	})
	t.Run("缺 client-id 头时退化而不报错", func(t *testing.T) {
		if leaseFor(context.Background(), base, ss, true).Enabled() {
			t.Fatal("无 client-id 头时不应启用")
		}
	})
}

// TestReceiveMessageWiresLeaseIntoInflight 钉住 receive.go 把 opts... 传给
// deliver.Receive 这一步接线：带 x-mq-client-id 头 + AutoRenew=true 的取件，
// 盘上的 inflight 记录必须写入 Owner/RenewUntilMs。
//
// 为什么查盘而不是看响应：响应里的消息没有 Owner 字段，只有持久化的 inflight
// 记录能证明租约真的传到了 deliver.Receive——若有人删掉 opts... 接线，测试
// 侧消息照常收到、响应照常 OK，但 Owner 不会被写，本测试即红。这是 e2e 之外
// 对这条接线的唯一单测覆盖。
func TestReceiveMessageWiresLeaseIntoInflight(t *testing.T) {
	env := newTestEnv(t, true)
	const topic = "lease-wire"
	const group = "g-wire"
	sendOne(t, env.client, topic, "hello")

	// 消息落在哪个队列不定（topic 多队列、sendOne 轮转），仿照
	// TestReceiveAckRoundTrip 逐队列取直到收到一条；每队列用 2s 有限 deadline，
	// 空队列不会长轮询 20s（同 receiveOne 的理由）。
	var got *pb.Message
	var queueID uint32
	for q := int32(0); q < 4 && got == nil; q++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(clientIDHeaderKey, "cli-wire"))
		stream, err := env.client.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
			Group:             &pb.Resource{Name: group},
			MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: topic}, Id: q},
			FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
			BatchSize:         10,
			InvisibleDuration: durationpb.New(time.Minute),
			AutoRenew:         true,
		})
		if err != nil {
			cancel()
			t.Fatalf("ReceiveMessage: %v", err)
		}
		// 必须等流读完整再 cancel：服务端流处理依赖 ctx 存活，提前 cancel 会让
		// deliver.Receive 在长轮询中途被打断、返回 context.Canceled。
		msgs, _ := recvAll(t, stream)
		cancel()
		if len(msgs) == 0 {
			continue
		}
		got, queueID = msgs[0], uint32(q)
	}
	if got == nil {
		t.Fatal("未收到消息")
	}
	// QueueOffset 底层是 *int64（proto 生成代码），Getter 返回解引用后的
	// int64 值，直接用。
	off := got.GetSystemProperties().GetQueueOffset()
	// 直接查盘上的 inflight 记录：响应里没有 Owner 字段，只有这条持久化记录
	// 能证明租约真的经 opts... 传到了 deliver.Receive——删掉接线则 Owner 为空。
	raw, ok, err := env.st.Get(store.InflightKey(group, topic, queueID, uint64(off)))
	if err != nil || !ok {
		t.Fatalf("查盘 inflight 失败: ok=%v err=%v", ok, err)
	}
	inf, err := core.DecodeInflight(raw)
	if err != nil {
		t.Fatalf("解码 inflight: %v", err)
	}
	if inf.Owner != "cli-wire" {
		t.Fatalf("inflight Owner 应为 cli-wire，实际 %q（opts... 接线被删则此处为空）", inf.Owner)
	}
	if inf.RenewUntilMs <= 0 {
		t.Fatalf("inflight RenewUntilMs 应 > 0（租约生效的标记），实际 %d", inf.RenewUntilMs)
	}
}

// recvAllFrames 读整个 ReceiveMessage 流，原样保留帧序。
//
// 与 recvAll 的区别：它不做分类归约。归约之后就看不出帧的**位置**了，而
// 信封层 delivery_timestamp 的判据恰恰是位置（必须是首帧，见
// specs/2026-08-16-receive-delivery-timestamp-design.md 的 D2）。
func recvAllFrames(t *testing.T, stream pb.MessagingService_ReceiveMessageClient) []*pb.ReceiveMessageResponse {
	t.Helper()
	var frames []*pb.ReceiveMessageResponse
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return frames
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		frames = append(frames, resp)
	}
}

// TestReceiveSendsEnvelopeDeliveryTimestampAsFirstFrame 锁定 B13.8 的修复：
// 有消息可投时，响应流的首帧必须是信封层 delivery_timestamp。
//
// 为什么这条值得单独锁：缺这一帧时 Go / Java / C# 三门 SDK 全都若无其事
// （它们只是把值存进变量、容忍 nil），只有官方 Python SDK 在 push 路径上对它
// 无条件做减法，抛 TypeError → 异常被吞 → listener 永不触发。也就是说这条
// 缺失在 Go e2e 里**照不出来**（e2e 用的就是 Go SDK），只能靠这里的位置断言
// 守住——这也是它必须是单测而不是 e2e 用例的原因。
func TestReceiveSendsEnvelopeDeliveryTimestampAsFirstFrame(t *testing.T) {
	c := newTestClient(t)
	const topic = "envts"
	sendOne(t, c, topic, "hello")

	before := time.Now()
	// 只有落到消息所在队列的那次 receive 才会有消息；四个队列都试。
	// 空队列只返回一帧 MESSAGE_NOT_FOUND，用帧数即可判别。
	var frames []*pb.ReceiveMessageResponse
	for q := int32(0); q < 4 && frames == nil; q++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
			Group:             &pb.Resource{Name: "g-envts"},
			MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: topic}, Id: q},
			BatchSize:         8,
			InvisibleDuration: durationpb.New(time.Minute),
		})
		if err != nil {
			cancel()
			t.Fatalf("ReceiveMessage: %v", err)
		}
		got := recvAllFrames(t, stream)
		cancel()
		if len(got) > 1 {
			frames = got
		}
	}
	after := time.Now()
	if len(frames) != 3 {
		t.Fatalf("期望 3 帧（delivery_timestamp + message + status），实际 %d", len(frames))
	}

	ts, ok := frames[0].GetContent().(*pb.ReceiveMessageResponse_DeliveryTimestamp)
	if !ok {
		t.Fatalf("首帧必须是信封层 delivery_timestamp，实际 %T", frames[0].GetContent())
	}
	got := ts.DeliveryTimestamp.AsTime()
	// 取值判据不能只判「非零」：非零可以由一个写死的常量满足。要求它落在本次
	// 调用的真实时间窗内，才能证明取的是「开始投递的那一刻」而不是某个定值。
	if got.Before(before) || got.After(after) {
		t.Fatalf("delivery_timestamp %v 不在本次调用区间 [%v, %v] 内", got, before, after)
	}
	if _, ok := frames[1].GetContent().(*pb.ReceiveMessageResponse_Message); !ok {
		t.Fatalf("第 2 帧应为 message，实际 %T", frames[1].GetContent())
	}
	// 末帧仍是 status：sq 刻意的 status-last 帧序（"本批已全部发完"的终止信号）
	// 不能被这次改动破坏
	if _, ok := frames[2].GetContent().(*pb.ReceiveMessageResponse_Status); !ok {
		t.Fatalf("末帧必须是 status，实际 %T", frames[2].GetContent())
	}
}

// TestReceiveEmptyPollSendsNoDeliveryTimestamp 锁定 spec D1：空长轮询是 sq 的
// 常态热路径（每消费者每队列每 20s 一次），不能因为这次改动被恒定加一帧。
//
// 这与参考实现刻意不同——上游在 finally 里的 onComplete() 无条件发。依据是
// 没有任何已知 SDK 在空响应下读它（Python 的赋值条件带 len(messages) > 0、
// Go 只把它当 fromProtobuf_MessageView2 的入参）。判据与推翻它所需的证据都
// 写在 specs/2026-08-16-receive-delivery-timestamp-design.md 的 D1。
func TestReceiveEmptyPollSendsNoDeliveryTimestamp(t *testing.T) {
	c := newTestClient(t)
	const topic = "envts-empty"
	// 经 QueryRoute 建一个从未发过消息的 topic：任何队列都必然为空，
	// 不依赖发送轮询的落点（那会让用例随队列数变化而脆弱）
	if _, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{Topic: &pb.Resource{Name: topic}}); err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: "g-envts-empty"},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: topic}, Id: 0},
		BatchSize:         8,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	frames := recvAllFrames(t, stream)
	if len(frames) != 1 {
		t.Fatalf("空长轮询应只有一帧 status，实际 %d 帧", len(frames))
	}
	if _, ok := frames[0].GetContent().(*pb.ReceiveMessageResponse_Status); !ok {
		t.Fatalf("空长轮询的唯一一帧应为 status，实际 %T", frames[0].GetContent())
	}
}
