// recall handle 编解码与 RecallMessage 协议入口。
//
// 职责：
//   - 把「撤回一条延时消息」所需的定位信息（topic/dueMs/seq）编码进一个
//     自包含、带签名的字符串句柄，由 SendMessage 在 SendResultEntry 里签发
//   - RecallMessage 请求到来时验签解码，交给 delay.Scheduler.Recall 兑现
//
// 边界：
//   - 只做编解码与协议翻译，不碰存储、不做时序判定——那四条判据全在
//     delay.Scheduler.Recall 里，且必须在它的互斥量内完成（见该函数注释）
//   - 只支持延时消息：事务半消息与普通消息不签发句柄
//
// 为什么形态要与 receipt.go 逐字对齐（base64(JSON) + "." + base64(HMAC)）：
// 同一个部署里两类句柄并存，形态一致的排查成本最低，且能共用 receiptSigSep
// 与同一把 handleSecret。
package rpc

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Xsxdot/sq/internal/core/delay"
	pb "github.com/Xsxdot/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// recallDomain 是 recall 句柄 HMAC 的域前缀。
//
// **不要"简化"掉它。** recall 与 receipt 共用同一把 handleSecret；没有这个
// 前缀时，一个合法的 receipt 句柄（负载 {"g":..,"t":..,"q":..,"o":..,"a":..}）
// 拿去做 recall 解码会**验签通过**——签名算的是同一把密钥、同一段字节。
// 随后 JSON 宽松解码把未知字段丢掉、缺失字段补零，得到
// {t: 该 receipt 的 topic, d: 0, s: 0}：一个签名有效、语义完全错位的伪句柄。
//
// 它今天会被 Recall 的闸门三（dueMs > now，d=0 恒为过去）挡下——但那意味着
// 句柄的完整性依赖一个**下游**判据没被人改动。域分隔让这种混淆在验签这一层
// （最早的一层）就失败，不依赖下游。
const recallDomain = "sq-recall\x00"

// recallPayload recall 句柄的明文结构，JSON 编码后 base64 传输。
type recallPayload struct {
	T string `json:"t"` // topic
	D int64  `json:"d"` // 投递时间（毫秒）
	S uint64 `json:"s"` // 延时暂存区分配序号
}

// recallSign 计算带域前缀的 HMAC。编解码两侧必须走同一个函数——分别手写
// 两遍是域分隔最容易被改坏的地方。
func recallSign(secret, payload []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(recallDomain))
	mac.Write(payload)
	return mac.Sum(nil)
}

// recallEncode 生成自包含 recall handle：base64(JSON) + "." + base64(HMAC-SHA256)。
//
// 参数：secret 服务端句柄密钥；topic/dueMs/seq 定位延时条目（dueMs+seq 即
// store.DelayKey 的两个分量）。
func recallEncode(secret []byte, topic string, dueMs int64, seq uint64) string {
	b, _ := json.Marshal(recallPayload{T: topic, D: dueMs, S: seq}) // 结构固定无失败路径
	return base64.StdEncoding.EncodeToString(b) + receiptSigSep +
		base64.StdEncoding.EncodeToString(recallSign(secret, b))
}

// recallDecode 解析并验签 recall handle。缺签名段、base64/验签/JSON 任一层
// 非法都返回带上下文的错误，调用方统一映射为 Code_BAD_REQUEST
// （**不是** INVALID_RECEIPT_HANDLE：那是消费侧收据句柄的码，用在这里会把
// 排查引向错误的方向）。
func recallDecode(secret []byte, s string) (topic string, dueMs int64, seq uint64, err error) {
	payload, sig, found := strings.Cut(s, receiptSigSep)
	if !found {
		return "", 0, 0, fmt.Errorf("recall handle 缺少签名段")
	}
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", 0, 0, fmt.Errorf("recall handle 非法 base64: %w", err)
	}
	sigRaw, err := base64.StdEncoding.DecodeString(sig)
	if err != nil {
		return "", 0, 0, fmt.Errorf("recall handle 签名段非法 base64: %w", err)
	}
	// 先验签再解 JSON：未通过验签的字节不值得进一步解析（hmac.Equal 常数时间）
	if !hmac.Equal(sigRaw, recallSign(secret, raw)) {
		return "", 0, 0, fmt.Errorf("recall handle 签名校验失败")
	}
	var p recallPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", 0, 0, fmt.Errorf("recall handle 非法 JSON: %w", err)
	}
	return p.T, p.D, p.S, nil
}

// RecallMessage 撤回一条尚未到期的延时消息。
//
// 参数：req.Topic 与 req.RecallHandle 都必填。句柄由 SendMessage 在
// SendResultEntry.RecallHandle 里签发（只有延时消息才有）。
//
// 返回：始终返回 nil error，失败信息放在 Status 里——与本包其余 handler
// 的约定一致（gRPC error 留给传输层故障）。
//
// 状态码选择：
//   - 句柄任一层非法、topic 不一致 → BAD_REQUEST。**不用
//     INVALID_RECEIPT_HANDLE**：那是消费侧收据句柄的码，用在这里会把排查
//     引向错误的方向
//   - 条目不存在、已过投递时间 → 都是 MESSAGE_NOT_FOUND。对客户端而言
//     「没赶上」与「已经不在了」行为完全相同，区分只会让它多写一条永远走
//     同一分支的代码；服务端日志里两者措辞不同，排查不受影响
//   - 非 meta leader → HA_NOT_AVAILABLE（复用 topicErrStatus，语义
//     「这节点没坏，你该问 leader」）
func (s *Server) RecallMessage(ctx context.Context, req *pb.RecallMessageRequest) (*pb.RecallMessageResponse, error) {
	topic := req.GetTopic().GetName()
	h := req.GetRecallHandle()
	if h == "" {
		s.logger.Warn("RecallMessage 缺少 recall handle", "topic", topic)
		return &pb.RecallMessageResponse{
			Status: errStatus(pb.Code_BAD_REQUEST, "recall handle 为空"),
		}, nil
	}
	hTopic, dueMs, seq, err := recallDecode(s.handleSecret, h)
	if err != nil {
		s.logger.Warn("RecallMessage 句柄非法", "topic", topic, "err", err)
		return &pb.RecallMessageResponse{
			Status: errStatus(pb.Code_BAD_REQUEST, err.Error()),
		}, nil
	}
	// 句柄里的 topic 与请求体的 topic 必须一致。二者都由客户端提供，不一致
	// 说明它拿错了句柄——先在协议层挡一道，delay.Recall 里还会拿**条目里
	// 真实的** topic 再校一次（纵深防御，见该函数判据 4）。
	if hTopic != topic {
		s.logger.Warn("RecallMessage 句柄 topic 与请求 topic 不一致",
			"req_topic", topic, "handle_topic", hTopic)
		return &pb.RecallMessageResponse{
			Status: errStatus(pb.Code_BAD_REQUEST, "recall handle 与请求 topic 不一致"),
		}, nil
	}

	msgID, err := s.ds.Recall(ctx, topic, dueMs, seq)
	switch {
	case err == nil:
		s.logger.Info("RecallMessage 撤回成功", "topic", topic, "msg_id", msgID,
			"due_ms", dueMs, "seq", seq)
		return &pb.RecallMessageResponse{Status: okStatus(), MessageId: msgID}, nil
	case errors.Is(err, delay.ErrRecallNotFound), errors.Is(err, delay.ErrRecallTooLate):
		// 两者同码。delay 侧已按不同措辞各打了一条 Debug，此处不重复记录
		return &pb.RecallMessageResponse{
			Status: errStatus(pb.Code_MESSAGE_NOT_FOUND, err.Error()),
		}, nil
	case errors.Is(err, delay.ErrRecallTopicMismatch):
		return &pb.RecallMessageResponse{
			Status: errStatus(pb.Code_BAD_REQUEST, err.Error()),
		}, nil
	default:
		// 非 leader 与内部故障都交给 topicErrStatus 统一分类与记录
		return &pb.RecallMessageResponse{
			Status: s.topicErrStatus("RecallMessage", topic, err, "due_ms", dueMs, "seq", seq),
		}, nil
	}
}
