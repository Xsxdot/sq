// Package core 定义 sq 的内部消息模型与各引擎共享类型。
//
// 测试：消息编解码与状态序列化的正确性。
package core

import (
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
