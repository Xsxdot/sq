// ForwardMessageToDeadLetterQueue：显式转入死信（spec §6 RPC 表——顺序消息
// 重试超限时由客户端显式入 DLQ；Java PushConsumer 的 FIFO 消费依赖它，
// Go SDK SimpleConsumer 不调用）。
//
// 边界：
//   - 仅按 receipt handle 定位（sq 无 message_id 索引，请求里的 MessageId/
//     DeliveryAttempt 只用于日志），handle 编码见 receipt.go
//   - 转移的原子性（DLQ 写入 + inflight 删除同批）由 deliver.ForwardToDLQ 保证
package rpc

import (
	"context"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// ForwardMessageToDeadLetterQueue 按 receipt handle 将消息转入 %DLQ%{group}。
// 成功返回 OK；handle 无法解析、目标不存在或已被重投覆盖（陈旧句柄）返回
// INVALID_RECEIPT_HANDLE；存储故障返回 INTERNAL_SERVER_ERROR。
func (s *Server) ForwardMessageToDeadLetterQueue(ctx context.Context, req *pb.ForwardMessageToDeadLetterQueueRequest) (*pb.ForwardMessageToDeadLetterQueueResponse, error) {
	g, topic, q, off, attempt, err := receiptDecode(s.handleSecret, req.GetReceiptHandle())
	if err != nil {
		s.logger.Warn("forward 句柄无法解析", "handle", truncateForLog(req.GetReceiptHandle()), "msg_id", req.GetMessageId(), "err", err)
		return &pb.ForwardMessageToDeadLetterQueueResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error())}, nil
	}
	ok, err := s.dl.ForwardToDLQ(g, topic, q, off, attempt)
	if err != nil {
		s.logger.Error("forward 转入死信失败", "group", g, "topic", topic, "queue", q, "offset", off, "err", err)
		return &pb.ForwardMessageToDeadLetterQueueResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
	}
	if !ok {
		// 幂等语义与 Ack 一致：目标不存在/句柄陈旧不算服务端错误，
		// 用协议码告知客户端句柄已失效即可
		return &pb.ForwardMessageToDeadLetterQueueResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, "句柄已失效（已确认或已重投）")}, nil
	}
	s.logger.Info("消息经 forward 显式转入死信", "group", g, "topic", topic, "queue", q, "offset", off, "msg_id", req.GetMessageId())
	return &pb.ForwardMessageToDeadLetterQueueResponse{Status: okStatus()}, nil
}
