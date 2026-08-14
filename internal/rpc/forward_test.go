// ForwardMessageToDeadLetterQueue RPC 测试。
//
// 职责（测试文件）：
//   - 验证按 receipt handle 定位并转入 DLQ，成功后同 handle 二次调用失效
//   - 验证非法/陈旧 handle 返回 INVALID_RECEIPT_HANDLE
//
// 边界：
//   - DLQ 转移的原子性与内容由 deliver 单测覆盖，这里只测协议映射
package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core/deliver"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

func TestForwardMessageToDeadLetterQueue(t *testing.T) {
	env := newTestEnv(t, true)
	// 发一条 FIFO 消息并收取，拿到 receipt handle
	resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fwd"},
			SystemProperties: &pb.SystemProperties{
				MessageType:  pb.MessageType_FIFO,
				MessageGroup: strPtr("grp"),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("发送: %v %v", resp.GetStatus(), err)
	}
	pm := receiveOneAnyQueue(t, env, "g", "fwd", time.Minute)
	handle := pm.GetSystemProperties().GetReceiptHandle()
	fresp, err := env.client.ForwardMessageToDeadLetterQueue(context.Background(),
		&pb.ForwardMessageToDeadLetterQueueRequest{
			Group:         &pb.Resource{Name: "g"},
			Topic:         &pb.Resource{Name: "fwd"},
			ReceiptHandle: handle,
			MessageId:     pm.GetSystemProperties().GetMessageId(),
		})
	if err != nil || fresp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("forward 应成功: %v %v", fresp.GetStatus(), err)
	}
	// 同一 handle 再来一次：目标已消失
	fresp2, err := env.client.ForwardMessageToDeadLetterQueue(context.Background(),
		&pb.ForwardMessageToDeadLetterQueueRequest{ReceiptHandle: handle})
	if err != nil || fresp2.GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE {
		t.Fatalf("重复 forward 应 INVALID_RECEIPT_HANDLE: %v %v", fresp2.GetStatus(), err)
	}
}

func TestForwardMessageMalformedHandle(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.ForwardMessageToDeadLetterQueue(context.Background(),
		&pb.ForwardMessageToDeadLetterQueueRequest{ReceiptHandle: "not-a-handle"})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE {
		t.Fatalf("非法 handle 应 INVALID_RECEIPT_HANDLE: %v %v", resp.GetStatus(), err)
	}
}

// receiveOneAnyQueue 逐队列（0..3）尝试收取一条消息（FIFO 消息落在 hash
// 决定的队列，测试无法预知队列号）
func receiveOneAnyQueue(t *testing.T, env testEnv, group, topic string, invisible time.Duration) *pb.Message {
	t.Helper()
	for q := uint32(0); q < 4; q++ {
		msgs, err := env.dl.Receive(context.Background(), group, topic, q, 1, invisible, 0, deliver.AllPass)
		if err != nil {
			t.Fatalf("Receive q%d: %v", q, err)
		}
		if len(msgs) == 1 {
			return env.srv.toPBMessage(msgs[0], group, invisible)
		}
	}
	t.Fatal("全部队列均无消息")
	return nil
}
