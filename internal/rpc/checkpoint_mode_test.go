// checkpoint_mode_test.go 检查点模式下改不可见 / 转死信的协议面回归（SQ4.2
// 期间发现的 SQ4.1 遗留缺陷）。
//
// 职责：经真实 gRPC 入口，对检查点模式（main 缺省）的 deliverer 调
// ChangeInvisibleDuration 与 ForwardMessageToDeadLetterQueue，成功的变更
// 必须回 Code_OK——缺陷形态是 deliver 以「有无待等提案」判成败，检查点模式
// 下恒无提案，成功也报落空，rpc 映射成 INVALID_RECEIPT_HANDLE。
//
// 边界：只测协议映射与成败判定；变更本身的内存/落盘语义归 deliver 包测试。
package rpc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Xsxdot/sq/internal/core/deliver"
	pb "github.com/Xsxdot/sq/internal/rpc/pb/apache/rocketmq/v2"
)

func TestCheckpointModeChangeInvisibleAndForwardReturnOK(t *testing.T) {
	env := newTestEnvDeliver(t, true, nil, nil, []deliver.Option{deliver.WithCheckpointInterval(time.Second)})
	ctx := context.Background()
	resp, err := env.client.SendMessage(ctx, &pb.SendMessageRequest{Messages: []*pb.Message{{
		Topic: &pb.Resource{Name: "ckm"},
		SystemProperties: &pb.SystemProperties{
			MessageType:  pb.MessageType_FIFO,
			MessageGroup: strPtr("grp"),
		},
		Body: []byte("x"),
	}}})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("发送: %v %v", resp.GetStatus(), err)
	}
	pm := receiveOneAnyQueue(t, env, "g", "ckm", time.Minute)
	handle := pm.GetSystemProperties().GetReceiptHandle()

	cresp, err := env.client.ChangeInvisibleDuration(ctx, &pb.ChangeInvisibleDurationRequest{
		Group:             &pb.Resource{Name: "g"},
		Topic:             &pb.Resource{Name: "ckm"},
		ReceiptHandle:     handle,
		InvisibleDuration: durationpb.New(2 * time.Minute),
		MessageId:         pm.GetSystemProperties().GetMessageId(),
	})
	if err != nil || cresp.GetStatus().GetCode() != pb.Code_OK {
		// Errorf 而非 Fatalf：两条入口各自报告（handle 不随改不可见变化）
		t.Errorf("检查点模式 ChangeInvisibleDuration 应 Code_OK，得 %v（err=%v）", cresp.GetStatus(), err)
	}

	fresp, err := env.client.ForwardMessageToDeadLetterQueue(ctx, &pb.ForwardMessageToDeadLetterQueueRequest{
		Group:         &pb.Resource{Name: "g"},
		Topic:         &pb.Resource{Name: "ckm"},
		ReceiptHandle: handle,
		MessageId:     pm.GetSystemProperties().GetMessageId(),
	})
	if err != nil || fresp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("检查点模式 ForwardMessageToDeadLetterQueue 应 Code_OK，得 %v（err=%v）", fresp.GetStatus(), err)
	}
}
