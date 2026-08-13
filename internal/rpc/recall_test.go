package rpc

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// U1 编解码往返。
func TestRecallHandleRoundTrip(t *testing.T) {
	secret := []byte("test-handle-secret")
	h := recallEncode(secret, "t-recall", 1765432100000, 42)
	topic, due, seq, err := recallDecode(secret, h)
	if err != nil {
		t.Fatalf("recallDecode: %v", err)
	}
	if topic != "t-recall" || due != 1765432100000 || seq != 42 {
		t.Fatalf("往返不一致：topic=%q due=%d seq=%d", topic, due, seq)
	}
}

// U2【承重】域分隔：一个**合法的 receipt 句柄**交给 recallDecode 必须验签失败。
//
// 这条用例删掉之后，域分隔的整个论证就失效了，它不是可选断言。
// 没有域分隔时：两类句柄共用同一把密钥，receipt 的负载
// {"g":..,"t":..,"q":..,"o":..,"a":..} 拿去做 recall 解码会**验签通过**，
// 然后 JSON 宽松解码把未知字段丢掉、缺失字段补零，得到
// {t: 该 receipt 的 topic, d: 0, s: 0}——一个签名有效、语义完全错位的伪句柄。
// 域分隔让它在验签这一层（最早的一层）就失败。
func TestRecallDecodeRejectsReceiptHandle(t *testing.T) {
	secret := []byte("test-handle-secret")
	// receiptEncode(secret, group, topic, queueID, offset, attempt)
	rh := receiptEncode(secret, "g1", "t-recall", 0, 7, 1)
	if _, _, _, err := recallDecode(secret, rh); err == nil {
		t.Fatalf("receipt 句柄被 recallDecode 接受了——域分隔失效")
	}
}

// U2b 反向：recall 句柄交给 receiptDecode 也必须失败（签名是对"前缀+负载"
// 算的，receiptDecode 对裸负载算，必然对不上）。
func TestReceiptDecodeRejectsRecallHandle(t *testing.T) {
	secret := []byte("test-handle-secret")
	h := recallEncode(secret, "t-recall", 1765432100000, 42)
	// receiptDecode 返回 (group, topic, queueID, offset, attempt, err)，共 6 个
	if _, _, _, _, _, err := receiptDecode(secret, h); err == nil {
		t.Fatalf("recall 句柄被 receiptDecode 接受了")
	}
}

// U3 篡改与形态非法。
func TestRecallDecodeRejectsTampered(t *testing.T) {
	secret := []byte("test-handle-secret")
	h := recallEncode(secret, "t-recall", 1765432100000, 42)

	// 篡改负载段首字节
	payload, sig, _ := strings.Cut(h, receiptSigSep)
	bad := "A" + payload[1:] + receiptSigSep + sig
	if bad == h {
		t.Fatalf("构造的篡改样本与原句柄相同，用例无效")
	}
	if _, _, _, err := recallDecode(secret, bad); err == nil {
		t.Fatalf("篡改负载后仍验签通过")
	}
	// 换一把密钥
	if _, _, _, err := recallDecode([]byte("other-secret"), h); err == nil {
		t.Fatalf("换密钥后仍验签通过")
	}
	// 缺签名段
	if _, _, _, err := recallDecode(secret, payload); err == nil {
		t.Fatalf("缺签名段仍被接受")
	}
	// 空串
	if _, _, _, err := recallDecode(secret, ""); err == nil {
		t.Fatalf("空句柄仍被接受")
	}
}

// 端到端（走 gRPC stub）：签发 → 撤回 → 幂等拒绝 → 各类非法输入的状态码。
func TestRecallMessageEndToEnd(t *testing.T) {
	env := newTestEnv(t, true)
	due := time.Now().Add(time.Hour).UnixMilli()
	resp, err := env.client.SendMessage(context.Background(), delayReq("t-e2e", due))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	h := resp.GetEntries()[0].GetRecallHandle()
	msgID := resp.GetEntries()[0].GetMessageId()

	// 成功
	rr, err := env.client.RecallMessage(context.Background(), &pb.RecallMessageRequest{
		Topic: &pb.Resource{Name: "t-e2e"}, RecallHandle: h,
	})
	if err != nil {
		t.Fatalf("RecallMessage: %v", err)
	}
	if got := rr.GetStatus().GetCode(); got != pb.Code_OK {
		t.Fatalf("Code=%v，期望 OK（message=%q）", got, rr.GetStatus().GetMessage())
	}
	if rr.GetMessageId() != msgID {
		t.Fatalf("MessageId=%q，期望 %q", rr.GetMessageId(), msgID)
	}

	// 幂等：再撤一次 → MESSAGE_NOT_FOUND
	rr2, err := env.client.RecallMessage(context.Background(), &pb.RecallMessageRequest{
		Topic: &pb.Resource{Name: "t-e2e"}, RecallHandle: h,
	})
	if err != nil {
		t.Fatalf("RecallMessage(重复): %v", err)
	}
	if got := rr2.GetStatus().GetCode(); got != pb.Code_MESSAGE_NOT_FOUND {
		t.Fatalf("重复撤回 Code=%v，期望 MESSAGE_NOT_FOUND", got)
	}

	// 空句柄 → BAD_REQUEST
	rr3, err := env.client.RecallMessage(context.Background(), &pb.RecallMessageRequest{
		Topic: &pb.Resource{Name: "t-e2e"},
	})
	if err != nil {
		t.Fatalf("RecallMessage(空句柄): %v", err)
	}
	if got := rr3.GetStatus().GetCode(); got != pb.Code_BAD_REQUEST {
		t.Fatalf("空句柄 Code=%v，期望 BAD_REQUEST", got)
	}

	// 拿一个合法 receipt 句柄冒充 → BAD_REQUEST（域分隔在协议面的体现）
	rr4, err := env.client.RecallMessage(context.Background(), &pb.RecallMessageRequest{
		Topic:        &pb.Resource{Name: "t-e2e"},
		RecallHandle: receiptEncode(env.srv.handleSecret, "g", "t-e2e", 0, 0, 1),
	})
	if err != nil {
		t.Fatalf("RecallMessage(receipt 冒充): %v", err)
	}
	if got := rr4.GetStatus().GetCode(); got != pb.Code_BAD_REQUEST {
		t.Fatalf("receipt 句柄冒充 Code=%v，期望 BAD_REQUEST", got)
	}

	// 请求 topic 与句柄 topic 不一致 → BAD_REQUEST
	resp2, err := env.client.SendMessage(context.Background(), delayReq("t-e2e-2", due))
	if err != nil {
		t.Fatalf("SendMessage(第二个 topic): %v", err)
	}
	rr5, err := env.client.RecallMessage(context.Background(), &pb.RecallMessageRequest{
		Topic: &pb.Resource{Name: "t-e2e"}, RecallHandle: resp2.GetEntries()[0].GetRecallHandle(),
	})
	if err != nil {
		t.Fatalf("RecallMessage(topic 不一致): %v", err)
	}
	if got := rr5.GetStatus().GetCode(); got != pb.Code_BAD_REQUEST {
		t.Fatalf("topic 不一致 Code=%v，期望 BAD_REQUEST", got)
	}
}
