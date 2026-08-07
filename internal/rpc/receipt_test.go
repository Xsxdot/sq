// receipt_test.go 验证 receipt handle 的签名编解码。
//
// 职责：
//   - 覆盖 encode→decode 往返
//   - 覆盖五种篡改/伪造形态必须全部被拒（验签失败带原因）
//
// 边界：
//   - 只测编解码层，不测 deliver 的 inflight/attempt 语义（那是 receive_test.go
//     与 deliver 包自己的职责）
package rpc

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestReceiptSignRoundtrip(t *testing.T) {
	secret := []byte("test-handle-secret")
	h := receiptEncode(secret, "g1", "t1", 3, 42, 2)
	g, topic, q, off, a, err := receiptDecode(secret, h)
	if err != nil || g != "g1" || topic != "t1" || q != 3 || off != 42 || a != 2 {
		t.Fatalf("往返失败: %v %v %v %v %v %v", g, topic, q, off, a, err)
	}
}

func TestReceiptRejectsTampering(t *testing.T) {
	secret := []byte("test-handle-secret")
	h := receiptEncode(secret, "g1", "t1", 0, 1, 1)
	cases := map[string]string{
		"payload 篡改": "x" + h[1:],
		"签名段篡改":     h[:len(h)-2] + "xx",
		"缺签名段（旧格式）": strings.Split(h, ".")[0],
		"换密钥":       receiptEncode([]byte("other-secret"), "g1", "t1", 0, 1, 1)[:len(h)], // 用错误密钥签的完整 handle
	}
	for name, bad := range cases {
		if _, _, _, _, _, err := receiptDecode(secret, bad); err == nil {
			t.Fatalf("%s: 未被拒绝", name)
		}
	}
	// 伪造攻击本体：自造 payload 不带合法签名，必须被拒
	forged := base64.StdEncoding.EncodeToString([]byte(`{"g":"victim","t":"t1","q":0,"o":1,"a":1}`))
	if _, _, _, _, _, err := receiptDecode(secret, forged); err == nil {
		t.Fatal("无签名伪造 handle 未被拒绝")
	}
}
