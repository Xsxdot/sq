// Package core 定义 sq 的内部消息模型与各引擎共享类型。
//
// 测试：消息编解码与状态序列化的正确性。
package core

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	m := &Message{
		ID: NewMessageID(), Topic: "orders", QueueID: 1, Offset: 9,
		Tag: "created", Keys: []string{"o-1"}, MessageGroup: "",
		Properties: map[string]string{"a": "b"}, Body: []byte{0x00, 0xFF, 0x7F},
		BodyEncoding: BodyEncodingGzip,
		BodyDigest:   &BodyDigest{Type: DigestTypeCRC32, Checksum: "3610a686"},
		BornAtMs:     1000, BornHost: "10.0.0.7:54321", StoreAtMs: 2000,
		TraceContext: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	b, err := EncodeMessage(m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeMessage(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	got.DeliveryAttempt = m.DeliveryAttempt // 非存储字段不参与比较
	if !reflect.DeepEqual(m, got) {
		t.Fatalf("round trip 不一致:\n%+v\n%+v", m, got)
	}
}

// TestDecodeLegacyMessageWithoutPassthroughFields 锁定"旧数据不需要迁移"：
// BodyEncoding/BodyDigest/BornHost/TraceContext 是后加的字段，升级前落盘的
// 消息 JSON 里根本没有这些键。encoding/json 对缺失的键不做任何写入，字段保持
// 零值——所以旧消息可以照常解码、照常投递，只是这四个属性为空。
//
// 这里用一段手写的旧格式 JSON 而不是"构造一个新 Message 再编码"：后者永远
// 会带上当前版本的字段集，证明不了对历史数据的兼容性。
func TestDecodeLegacyMessageWithoutPassthroughFields(t *testing.T) {
	legacy := []byte(`{"id":"ABC","topic":"orders","queue_id":1,"offset":9,` +
		`"tag":"created","body":"AAD/","born_at_ms":1000,"store_at_ms":2000}`)
	m, err := DecodeMessage(legacy)
	if err != nil {
		t.Fatalf("旧格式消息应能直接解码: %v", err)
	}
	if m.ID != "ABC" || m.Topic != "orders" || m.BornAtMs != 1000 {
		t.Fatalf("既有字段解码错误: %+v", m)
	}
	if m.BodyEncoding != BodyEncodingUnspecified || m.BodyDigest != nil ||
		m.BornHost != "" || m.TraceContext != "" {
		t.Fatalf("缺失的新字段应保持零值，实际 %+v", m)
	}
}

// TestEncodeMessageOmitsUnsetPassthroughFields 与上一条互补：未设置这些字段的
// 消息，编码结果里不应出现对应的键（靠 omitempty）。这保证升级不会凭空放大
// 每条消息的落盘体积，也保证新旧版本写出的数据形状一致。
func TestEncodeMessageOmitsUnsetPassthroughFields(t *testing.T) {
	b, err := EncodeMessage(&Message{ID: "ABC", Topic: "orders", Body: []byte("x")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	for _, key := range []string{"body_encoding", "body_digest", "born_host", "trace_context"} {
		if strings.Contains(string(b), key) {
			t.Fatalf("未设置的字段 %q 不应出现在编码结果里: %s", key, b)
		}
	}
}

func TestMessageDeliverAtMsRoundTripAndCompat(t *testing.T) {
	// 新字段往返
	m := &Message{ID: "x", Topic: "t", Body: []byte("b"), DeliverAtMs: 12345}
	raw, err := EncodeMessage(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMessage(raw)
	if err != nil || got.DeliverAtMs != 12345 {
		t.Fatalf("DeliverAtMs 往返失败: %+v %v", got, err)
	}
	// 旧数据兼容：M2 及以前落盘的 JSON 没有 deliver_at_ms 键，解码得零值
	old, err := DecodeMessage([]byte(`{"id":"y","topic":"t","body":"Yg=="}`))
	if err != nil || old.DeliverAtMs != 0 {
		t.Fatalf("旧数据兼容失败: %+v %v", old, err)
	}
	// 零值不产生新键：普通消息编码结果与升级前逐字节一致
	m2 := &Message{ID: "z", Topic: "t", Body: []byte("b")}
	raw2, _ := EncodeMessage(m2)
	if bytes.Contains(raw2, []byte("deliver_at_ms")) {
		t.Fatal("零值 DeliverAtMs 不应出现在 JSON 中")
	}
}

func TestMessageIDShape(t *testing.T) {
	id := NewMessageID()
	if len(id) != 32 {
		t.Fatalf("msgId 长度: %d", len(id))
	}
	if id == NewMessageID() {
		t.Fatal("msgId 重复")
	}
}

func TestInflightRoundTrip(t *testing.T) {
	s := &InflightState{ExpireAtMs: 123456, Attempts: 3}
	got, err := DecodeInflight(EncodeInflight(s))
	if err != nil || !reflect.DeepEqual(s, got) {
		t.Fatalf("round trip: %+v %v", got, err)
	}
}

func TestInflightOrderedRoundTripAndCompat(t *testing.T) {
	// 新字段往返
	raw := EncodeInflight(&InflightState{ExpireAtMs: 100, Attempts: 2, Ordered: true})
	got, err := DecodeInflight(raw)
	if err != nil || !got.Ordered || got.ExpireAtMs != 100 || got.Attempts != 2 {
		t.Fatalf("Ordered 往返失败: %+v %v", got, err)
	}
	// 旧数据兼容：M3 及以前落盘的 inflight JSON 没有 ordered 键，解码得 false
	old, err := DecodeInflight([]byte(`{"expire_at_ms":100,"attempts":1}`))
	if err != nil || old.Ordered {
		t.Fatalf("旧数据兼容失败: %+v %v", old, err)
	}
	// 零值不产生新键：非顺序 inflight 编码结果与升级前逐字节一致
	if bytes.Contains(EncodeInflight(&InflightState{ExpireAtMs: 1, Attempts: 1}), []byte("ordered")) {
		t.Fatal("零值 Ordered 不应出现在 JSON 中")
	}
}

func TestInflightStateOldRecordDecodesAsNonRenewable(t *testing.T) {
	// M1–M4 落盘的旧记录：没有 owner / renew_until_ms 两个键
	old := []byte(`{"expire_at_ms":1700000000000,"attempts":3}`)
	st, err := DecodeInflight(old)
	if err != nil {
		t.Fatalf("解码旧记录: %v", err)
	}
	if st.ExpireAtMs != 1700000000000 || st.Attempts != 3 {
		t.Fatalf("旧字段解码错误: %+v", st)
	}
	// 零值即「不参与续租」，这是零迁移的全部依据
	if st.Owner != "" || st.RenewUntilMs != 0 {
		t.Fatalf("旧记录不应带续租信息: owner=%q renew_until=%d", st.Owner, st.RenewUntilMs)
	}
}

func TestInflightStateOmitsRenewFieldsWhenUnset(t *testing.T) {
	// 不启用续租时编码结果必须与改造前逐字节相同，否则存量部署重启后
	// 每条 inflight 都会因为多出两个键而变长，且旧版本 sq 读不回去
	b := EncodeInflight(&InflightState{ExpireAtMs: 1, Attempts: 1})
	if got := string(b); got != `{"expire_at_ms":1,"attempts":1}` {
		t.Fatalf("未启用续租时的编码结果 = %s，期望不含 owner/renew_until_ms", got)
	}
}

func TestInflightStateRenewFieldsRoundTrip(t *testing.T) {
	want := &InflightState{ExpireAtMs: 10, Attempts: 2, Ordered: true, Owner: "cli-1", RenewUntilMs: 99}
	got, err := DecodeInflight(EncodeInflight(want))
	if err != nil {
		t.Fatalf("往返解码: %v", err)
	}
	if *got != *want {
		t.Fatalf("往返后 = %+v，期望 %+v", got, want)
	}
}
