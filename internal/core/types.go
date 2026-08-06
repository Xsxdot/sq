// Package core 定义 sq 的内部消息模型与各引擎共享类型。
//
// 职责：
//   - Message：协议无关的消息内部表示（adapter 负责与 proto 互转）
//   - 存储编解码（当前 JSON，Body 走 base64）
//
// 边界：
//   - 不 import 任何 proto/pb 包（spec 的协议适配层约束）
//   - JSON 编码是 M1 的性能取舍：可读易调试，量级足够；
//     若未来需要可替换为二进制编码，Encode/Decode 是唯一出入口
package core

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Message 消息的内部表示。落盘字段见 json tag；DeliveryAttempt 仅投递时填充。
type Message struct {
	ID              string            `json:"id"`
	Topic           string            `json:"topic"`
	QueueID         uint32            `json:"queue_id"`
	Offset          uint64            `json:"offset"`
	Tag             string            `json:"tag,omitempty"`
	Keys            []string          `json:"keys,omitempty"`
	MessageGroup    string            `json:"message_group,omitempty"`
	Properties      map[string]string `json:"properties,omitempty"`
	Body            []byte            `json:"body"`
	BornAtMs        int64             `json:"born_at_ms"`
	StoreAtMs       int64             `json:"store_at_ms"`
	DeliveryAttempt int32             `json:"-"`
}

// EncodeMessage 序列化消息用于落盘。
func EncodeMessage(m *Message) ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("编码消息 %s: %w", m.ID, err)
	}
	return b, nil
}

// DecodeMessage 反序列化落盘消息。
func DecodeMessage(b []byte) (*Message, error) {
	m := &Message{}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("解码消息: %w", err)
	}
	return m, nil
}

// NewMessageID 生成 32 位大写十六进制消息 ID（16 随机字节）。
func NewMessageID() string {
	b := make([]byte, 16)
	rand.Read(b) // crypto/rand 在受支持平台不会失败
	return strings.ToUpper(hex.EncodeToString(b))
}

// InflightState 已投未确认消息的持久状态（inflight key 的 value）。
type InflightState struct {
	ExpireAtMs int64 `json:"expire_at_ms"` // 不可见截止时间；早于 now 即可重投
	Attempts   int32 `json:"attempts"`     // 已投递次数（首投=1）
}

// EncodeInflight 序列化 inflight 状态。
func EncodeInflight(s *InflightState) []byte {
	b, _ := json.Marshal(s) // 结构固定无失败路径
	return b
}

// DecodeInflight 反序列化 inflight 状态。
func DecodeInflight(b []byte) (*InflightState, error) {
	s := &InflightState{}
	if err := json.Unmarshal(b, s); err != nil {
		return nil, fmt.Errorf("解码 inflight: %w", err)
	}
	return s, nil
}
