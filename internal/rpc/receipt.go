// receipt handle 编解码。
//
// 职责：
//   - 把 POP 消费的定位信息（group/topic/queue/offset/attempt）编码进一个
//     自包含的字符串 handle，供 AckMessage/ChangeInvisibleDuration 解码使用
//
// 边界：
//   - 只做编解码，不做任何 I/O 或业务校验（那是 deliver.Deliverer 的事）
//
// 为什么 handle 要自包含定位信息、且不能只信请求体：Ack 只信 handle，不信
// 请求体里的 group/topic 字段——这是 RocketMQ pop receipt 的设计思路，避免
// 请求体字段与 handle 实际指向的记录不一致。
//
// 为什么还要把 attempt 揉进 handle（这一条堵的是一条真实的消息丢失路径）：没有
// attempt，消费者 A 收到消息 X（attempt=1）后处理超时，X 会被过期重投给
// 消费者 B（attempt=2，全新的不可见窗口）；此时 A 迟到的 Ack(X) 若只按
// (group,topic,queue,offset) 定位记录，会直接删掉 B 持有的那条新记录——
// X 从此既无 inflight 兜底、cursor 也已跳过它，一旦 B 处理失败或崩溃，
// X 就永久丢失。把 attempt 编进 handle 之后，deliver.Ack/ChangeInvisible
// 才能校验出"这是一个陈旧句柄"并安全地幂等拒绝，而不是盲目地按位置操作。
package rpc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// receipt receipt handle 的明文结构，JSON 编码后 base64 传输。
type receipt struct {
	G string `json:"g"` // 消费组
	T string `json:"t"` // topic
	Q uint32 `json:"q"` // 队列 ID
	O uint64 `json:"o"` // offset
	A int32  `json:"a"` // 投递次数（首投=1），陈旧句柄校验的关键字段
}

// receiptEncode 生成自包含 receipt handle。
func receiptEncode(group, topic string, queueID uint32, offset uint64, attempt int32) string {
	b, _ := json.Marshal(receipt{G: group, T: topic, Q: queueID, O: offset, A: attempt}) // 结构固定无失败路径
	return base64.StdEncoding.EncodeToString(b)
}

// receiptDecode 解析 receipt handle。base64/JSON 任一层非法都返回带上下文的错误，
// 调用方（AckMessage/ChangeInvisibleDuration）统一映射为 Code_INVALID_RECEIPT_HANDLE。
func receiptDecode(s string) (group, topic string, queueID uint32, offset uint64, attempt int32, err error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("receipt handle 非法 base64: %w", err)
	}
	var r receipt
	if err := json.Unmarshal(raw, &r); err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("receipt handle 非法 JSON: %w", err)
	}
	return r.G, r.T, r.Q, r.O, r.A, nil
}
