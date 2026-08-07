//go:build e2e

// 官方 Go SDK 认证 e2e：AK/SK 配置后，错凭据被拒、对凭据全链路可用（spec §6）。
//
// 职责：
//   - 验证服务端签名校验对真实 SDK 的行为：拒绝路径与放行路径
//
// 边界：
//   - 不测重放窗口（服务端刻意不做，见 internal/rpc/auth.go 边界注释）
package e2e

import (
	"context"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"

	"github.com/xushixin/sq/internal/config"
)

func TestOfficialGoSDKAuth(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) {
		c.AccessKey = "e2e-ak"
		c.SecretKey = "e2e-sk"
	})
	const topic = "e2e-auth"

	// 错误凭据：QueryRoute 在客户端启动路径上就会被拒。SDK 对 gRPC 层错误
	// 可能在 Start 或首次 Send 时暴露，两处都接受，但必须在限时内失败。
	bad, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{AccessKey: "e2e-ak", AccessSecret: "wrong"},
	}, rmq.WithTopics(topic))
	if err == nil {
		if serr := bad.Start(); serr == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, senderr := bad.Send(ctx, &rmq.Message{Topic: topic, Body: []byte("x")})
			cancel()
			bad.GracefulStop()
			if senderr == nil {
				t.Fatal("错误凭据的发送应失败")
			}
			t.Logf("错误凭据在 Send 阶段被拒: %v", senderr)
		} else {
			t.Logf("错误凭据在 Start 阶段被拒: %v", serr)
		}
	} else {
		t.Logf("错误凭据在构造阶段被拒: %v", err)
	}

	// 正确凭据：发送 + 消费 + ack 全链路
	good, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{AccessKey: "e2e-ak", AccessSecret: "e2e-sk"},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := good.Start(); err != nil {
		t.Fatalf("正确凭据 Start 失败: %v", err)
	}
	defer good.GracefulStop()
	if _, err := good.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte("authed")}); err != nil {
		t.Fatalf("正确凭据发送失败: %v", err)
	}
	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: "e2e-auth-g",
		Credentials:   &credentials.SessionCredentials{AccessKey: "e2e-ak", AccessSecret: "e2e-sk"},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	defer consumer.GracefulStop()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询返回 MESSAGE_NOT_FOUND，正常
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != "authed" {
				t.Fatalf("消息体不符: %s", mv.GetBody())
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			return
		}
	}
	t.Fatal("正确凭据 60s 内未消费到消息")
}
