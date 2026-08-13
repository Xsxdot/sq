package rpc

import (
	"strings"
	"testing"
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
