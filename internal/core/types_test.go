// Package core 定义 sq 的内部消息模型与各引擎共享类型。
//
// 测试：消息编解码与状态序列化的正确性。
package core

import (
	"reflect"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	m := &Message{
		ID: NewMessageID(), Topic: "orders", QueueID: 1, Offset: 9,
		Tag: "created", Keys: []string{"o-1"}, MessageGroup: "",
		Properties: map[string]string{"a": "b"}, Body: []byte{0x00, 0xFF, 0x7F},
		BornAtMs: 1000, StoreAtMs: 2000,
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
