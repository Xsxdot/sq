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

// BodyEncoding 消息体的编码/压缩方式。
//
// 用自描述的小写 token 而不是协议里的枚举数值：数值是 RocketMQ 线格式的一部分，
// 一旦落进 sq 自己的存储，就等于把某个协议版本的编号钉死在数据文件里；token
// 直接写在 JSON 里人眼可读，也不会因为协议侧重新编号而错位。sq 自身从不按这个
// 字段解压或改写 Body——它只负责原样收下、原样交还，解释权在生产者与消费者。
type BodyEncoding string

const (
	// BodyEncodingUnspecified 生产者未声明编码方式（零值，落盘时省略该键）。
	BodyEncodingUnspecified BodyEncoding = ""
	// BodyEncodingIdentity 未压缩：Body 即原始字节。
	BodyEncodingIdentity BodyEncoding = "identity"
	// BodyEncodingGzip Body 是 gzip 压缩后的字节，由消费端解压。
	BodyEncodingGzip BodyEncoding = "gzip"
)

// DigestType 消息体校验和算法。取值形态与理由同 BodyEncoding。
type DigestType string

const (
	// DigestTypeUnspecified 未声明校验算法（零值）。
	DigestTypeUnspecified DigestType = ""
	// DigestTypeCRC32 CRC32 校验和。
	DigestTypeCRC32 DigestType = "crc32"
	// DigestTypeMD5 MD5 校验和。
	DigestTypeMD5 DigestType = "md5"
	// DigestTypeSHA1 SHA1 校验和。
	DigestTypeSHA1 DigestType = "sha1"
)

// BodyDigest 生产者算好的消息体校验和。sq 既不验证也不重算，只做透传：
// 校验是端到端的事，中间节点重算反而会掩盖"消息在传输中被改坏"这类问题。
type BodyDigest struct {
	Type     DigestType `json:"type"`
	Checksum string     `json:"checksum"`
}

// Message 消息的内部表示。落盘字段见 json tag；DeliveryAttempt 仅投递时填充。
//
// BodyEncoding/BodyDigest/BornHost/TraceContext/DeliverAtMs 是"sq 不解释、只负责
// 原样带回"的生产者属性：写入时从协议里收下，投递时原样还给消费者。少带任何一个
// 都不是无害的省略——BodyEncoding 丢失会让压缩过的 Body 被消费端当成明文交给应用
// （静默的数据损坏），TraceContext 丢失则让分布式链路在经过 sq 时直接断掉。
//
// 这几个字段全部带 omitempty：本结构升级前落盘的消息 JSON 里没有对应的键，
// encoding/json 对缺失键不做任何写入、保持字段零值，因此旧数据无需迁移即可
// 继续解码；反过来，未设置这些字段的新消息编码结果与升级前逐字节相同。
type Message struct {
	ID           string            `json:"id"`
	Topic        string            `json:"topic"`
	QueueID      uint32            `json:"queue_id"`
	Offset       uint64            `json:"offset"`
	Tag          string            `json:"tag,omitempty"`
	Keys         []string          `json:"keys,omitempty"`
	MessageGroup string            `json:"message_group,omitempty"`
	Properties   map[string]string `json:"properties,omitempty"`
	Body         []byte            `json:"body"`
	BodyEncoding BodyEncoding      `json:"body_encoding,omitempty"`
	BodyDigest   *BodyDigest       `json:"body_digest,omitempty"`
	BornAtMs     int64             `json:"born_at_ms"`
	BornHost     string            `json:"born_host,omitempty"`
	StoreAtMs    int64             `json:"store_at_ms"`
	// DeliverAtMs 延时消息的到期投递时间（UnixMilli）；0 = 普通消息。
	// 移入 msg/ 后仍保留：投递时协议层据此回填 MessageType_DELAY 与
	// DeliveryTimestamp，SDK 消费端才能读到自己当初设置的延时时间。
	DeliverAtMs     int64  `json:"deliver_at_ms,omitempty"`
	TraceContext    string `json:"trace_context,omitempty"`
	DeliveryAttempt int32  `json:"-"`
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
