// EndTransaction 与事务回查下发：proto 语义 ↔ txn.Manager 的翻译层。
//
// 职责：
//   - EndTransaction RPC：resolution 校验、txn.End 调用、幂等语义翻译
//   - RecoverOrphan：实现 txn.Notifier——把 core.Message 编码为
//     RecoverOrphanedTransactionCommand 并写入一条 producer Telemetry 流
//
// 边界：
//   - 不做事务状态判断（txn.Manager 的事）；不管会话怎么挑（sessions 的事）
package rpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/replication"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// EndTransaction 事务提交/回滚（spec §6）。
//
// 幂等：txID 不存在回 OK——三种正常来源（客户端网络重试、与回查决断赛跑、
// 超限已丢弃）都不该让客户端把一次实际已生效的决断当成失败去重试。
// 不拦磁盘水位：提交是已落盘数据的移位（删 half 写 msg，净增量≈0），
// 拦了只会让半消息滞留、回查空转（与 delay 到期搬运同一取舍）。
func (s *Server) EndTransaction(ctx context.Context, req *pb.EndTransactionRequest) (*pb.EndTransactionResponse, error) {
	txID := req.GetTransactionId()
	var commit bool
	switch req.GetResolution() {
	case pb.TransactionResolution_COMMIT:
		commit = true
	case pb.TransactionResolution_ROLLBACK:
		commit = false
	default:
		// 决断不能靠猜：UNSPECIFIED 提交会放出未确认的业务消息，回滚会
		// 丢掉已确认的——两个方向都错，只能拒绝
		s.logger.Warn("EndTransaction 决断未指定", "tx_id", txID, "msg_id", req.GetMessageId())
		return &pb.EndTransactionResponse{Status: errStatus(pb.Code_BAD_REQUEST,
			"resolution 必须为 COMMIT 或 ROLLBACK")}, nil
	}
	found, err := s.tx.End(ctx, txID, commit)
	if err != nil {
		// ErrNotLeader（本节点不再是元数据组 leader）与事务存储故障分性质
		// 映射：前者是「问错节点」，SDK 据此换节点重试；后者才是真·服务端
		// 故障（选码论证见 QueryRoute 分支注释）。
		if errors.Is(err, replication.ErrNotLeader) {
			s.logger.Debug("EndTransaction 失败：本节点非元数据组 leader", "tx_id", txID,
				"msg_id", req.GetMessageId(), "topic", req.GetTopic().GetName(), "err", err)
			return &pb.EndTransactionResponse{Status: errStatus(pb.Code_HA_NOT_AVAILABLE, err.Error())}, nil
		}
		s.logger.Error("EndTransaction 失败", "tx_id", txID,
			"msg_id", req.GetMessageId(), "topic", req.GetTopic().GetName(),
			"resolution", req.GetResolution(), "err", err)
		return &pb.EndTransactionResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
	}
	if !found {
		s.logger.Warn("EndTransaction 目标事务不存在（幂等按成功处理）",
			"tx_id", txID, "msg_id", req.GetMessageId(), "resolution", req.GetResolution(),
			"source", req.GetSource())
		return &pb.EndTransactionResponse{Status: okStatus()}, nil
	}
	s.logger.Info("EndTransaction 完成", "tx_id", txID, "msg_id", req.GetMessageId(),
		"topic", req.GetTopic().GetName(), "resolution", req.GetResolution(), "source", req.GetSource())
	return &pb.EndTransactionResponse{Status: okStatus()}, nil
}

// RecoverOrphan 实现 txn.Notifier：把到期未决半消息经 Telemetry 流下发给
// 一个发布该 topic 的 producer，由其 TransactionChecker 决断后回 EndTransaction。
// 返回 false 表示没有可用会话或写流失败（调度器改期后下轮再试）。
func (s *Server) RecoverOrphan(m *core.Message, txID string) bool {
	sess := s.sessions.pickProducer(m.Topic)
	if sess == nil {
		return false
	}
	cmd := &pb.TelemetryCommand{
		Status: okStatus(),
		Command: &pb.TelemetryCommand_RecoverOrphanedTransactionCommand{
			RecoverOrphanedTransactionCommand: &pb.RecoverOrphanedTransactionCommand{
				Message:       halfToProtoMessage(m),
				TransactionId: txID,
			},
		},
	}
	if err := sess.send(cmd); err != nil {
		// 流写失败通常意味着客户端已断开而注销尚未发生：Warn 留痕即可，
		// 会话随 handler 退出注销，调度器下轮会挑别的会话
		s.logger.Warn("回查命令写流失败", "tx_id", txID, "topic", m.Topic,
			"msg_id", m.ID, "err", err)
		return false
	}
	s.logger.Info("回查命令已下发", "tx_id", txID, "topic", m.Topic, "msg_id", m.ID)
	return true
}

// halfToProtoMessage 半消息 → 协议消息（回查命令载荷）。
//
// 与 receive.go 的投递构造刻意分开：那边带 ReceiptHandle/InvisibleDuration/
// DeliveryAttempt 等 POP 消费语义字段，回查载荷没有这些概念。类型固定回填
// TRANSACTION——SDK 的 MessageView 靠它识别这是半消息回查。
// digest 兜底补算 CRC32 的理由同 receive.go（SDK 对 UNSPECIFIED 刷 WARN）。
func halfToProtoMessage(m *core.Message) *pb.Message {
	enc, _ := bodyEncodingToPB(m.BodyEncoding)
	if m.BodyEncoding == core.BodyEncodingUnspecified {
		enc = pb.Encoding_IDENTITY
	}
	digest := digestToPB(m.BodyDigest)
	if digest == nil {
		digest = &pb.Digest{Type: pb.DigestType_CRC32, Checksum: crc32Checksum(m.Body)}
	}
	sp := &pb.SystemProperties{
		MessageId:      m.ID,
		MessageType:    pb.MessageType_TRANSACTION,
		Keys:           m.Keys,
		BodyEncoding:   enc,
		BodyDigest:     digest,
		BornTimestamp:  timestamppb.New(time.UnixMilli(m.BornAtMs)),
		BornHost:       m.BornHost,
		StoreTimestamp: timestamppb.New(time.UnixMilli(m.StoreAtMs)),
	}
	if m.Tag != "" {
		tag := m.Tag
		sp.Tag = &tag
	}
	if m.TraceContext != "" {
		tc := m.TraceContext
		sp.TraceContext = &tc
	}
	return &pb.Message{
		Topic:            &pb.Resource{Name: m.Topic},
		SystemProperties: sp,
		UserProperties:   m.Properties,
		Body:             m.Body,
	}
}
