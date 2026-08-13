// Package core 定义 sq 的内部消息模型与各引擎共享类型。
//
// 职责：
//   - Message：协议无关的消息内部表示（adapter 负责与 proto 互转）
//   - InflightState 等辅助结构及其编解码
//
// 边界：
//   - 不 import 任何 proto/pb 包（spec 的协议适配层约束）
//   - 不含消息的存储编解码——已独立到 encoding.go（两套格式 + 档位开关），
//     本文件只留 Message 模型本身与 InflightState 的编解码
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
	DeliverAtMs  int64  `json:"deliver_at_ms,omitempty"`
	TraceContext string `json:"trace_context,omitempty"`
	// Transactional 事务消息路由标记（M6）：true 时 SendMessage 分流至
	// txn.Stage 而非 produce.Append。不落盘——半消息的身份由所在 half/
	// 前缀表达，提交移入 msg/ 后它就是普通消息。
	Transactional   bool  `json:"-"`
	DeliveryAttempt int32 `json:"-"`
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
	// Ordered 顺序消息标记（M4）：true 表示这条 inflight 对应 MessageGroup 非空
	// 的顺序消息，它的存在即该队列顺序锁被占用——deliver 不变式：每
	// (group,topic,queue) 至多 1 条 Ordered inflight。omitempty：M3 及以前
	// 落盘的旧记录无此键，解码得 false（非顺序），无需迁移。
	Ordered bool `json:"ordered,omitempty"`
	// Owner 本次投递的持有者客户端标识（gRPC metadata 的 x-mq-client-id）。
	// 仅在该次投递启用了自动续租时写入；空串表示不参与续租判定。
	// omitempty：M1–M4 落盘的旧记录无此键，解码得空串，行为与改造前相同，
	// 无需迁移——与 Ordered 当初的处理方式一致。
	Owner string `json:"owner,omitempty"`
	// RenewUntilMs 本次投递允许续租到的绝对时刻（毫秒）。0 表示不续租。
	//
	// 它是**硬上限**而不是目标：续租每次把 ExpireAtMs 推到
	// min(now+不可见期, RenewUntilMs)，越过它就必须走重投。没有这条线，
	// 一个「进程活着但 handler 卡死」的消费者能永久扣住消息，投递次数永不
	// 增长，死信队列永远等不到它——而这类故障恰恰最需要 DLQ 兜底。
	RenewUntilMs int64 `json:"renew_until_ms,omitempty"`
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
