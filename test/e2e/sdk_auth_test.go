//go:build e2e

// 官方 Go SDK 认证 e2e：AK/SK 配置后，错凭据被拒、对凭据全链路可用（spec §6）。
//
// 职责：
//   - 验证服务端签名校验对真实 SDK 的行为：拒绝路径与放行路径
//   - broker 配两条凭据时，非首条凭据（e2e-ak2/e2e-sk2）同样完成
//     发送→接收→ack 全链路，证明鉴权查询命中非首条记录而非只认第一条
//   - 交叉错配（e2e-ak2 + e2e-sk）必须被拒：AK 命中但 SK 不匹配不算数
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

// TestOfficialGoSDKAuth 多凭据鉴权 e2e：
// 错凭据被拒、交叉错配被拒、两条凭据各自走通全链路。
func TestOfficialGoSDKAuth(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) {
		c.Credentials = []config.Credential{
			{Name: "e2e", AccessKey: "e2e-ak", SecretKey: "e2e-sk"},
			{AccessKey: "e2e-ak2", SecretKey: "e2e-sk2"},
		}
	})
	const topic, topic2 = "e2e-auth", "e2e-auth2"

	// 错误凭据：AK 本身就不存在，QueryRoute 在客户端启动路径上就会被拒。
	assertAuthRejected(t, endpoint, topic, "e2e-ak", "wrong")
	// 交叉错配：AK 命中 e2e-ak2，但 SK 是另一条凭据的——AK 命中而 SK 不符
	// 必须按无效凭据拒绝，不能因为 AK 存在就放行。
	assertAuthRejected(t, endpoint, topic, "e2e-ak2", "e2e-sk")

	// 第一条凭据：发送 + 消费 + ack 全链路
	runAuthRoundtrip(t, endpoint, topic, "e2e-auth-g", "e2e-ak", "e2e-sk", "authed")
	// 第二条凭据：同一 broker 上同样走通全链路，证明非首条凭据也被正确命中。
	// 用独立 topic 而非共用 topic：新消费组 fetch 位点从 0 起步（重放该 topic
	// 全量历史，见 internal/core/deliver/deliver.go 阶段 2），共用 topic 会让
	// 第二条凭据的消费组把第一条的消息也收到，body 严格断言必然误伤。
	// 鉴权是连接级、与 topic 无关，故独立 topic 不削弱「命中非首条凭据」的证明力。
	runAuthRoundtrip(t, endpoint, topic2, "e2e-auth-g2", "e2e-ak2", "e2e-sk2", "authed2")
}

// assertAuthRejected 断言给定 AK/SK 必被拒。SDK 对 gRPC 层错误可能在构造、
// Start 或首次 Send 时暴露，三处都接受，但必须在限时内失败。
func assertAuthRejected(t *testing.T, endpoint, topic, ak, sk string) {
	t.Helper()
	bad, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{AccessKey: ak, AccessSecret: sk},
	}, rmq.WithTopics(topic))
	if err == nil {
		if serr := bad.Start(); serr == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, senderr := bad.Send(ctx, &rmq.Message{Topic: topic, Body: []byte("x")})
			cancel()
			bad.GracefulStop()
			if senderr == nil {
				t.Fatalf("凭据 %s/%s 的发送应失败", ak, sk)
			}
			t.Logf("凭据 %s/%s 在 Send 阶段被拒: %v", ak, sk, senderr)
		} else {
			t.Logf("凭据 %s/%s 在 Start 阶段被拒: %v", ak, sk, serr)
		}
	} else {
		t.Logf("凭据 %s/%s 在构造阶段被拒: %v", ak, sk, err)
	}
}

// runAuthRoundtrip 用给定凭据完成发送 + 消费 + ack 全链路：消息体必须与
// wantBody 一致才 ack，60s 内收不到即失败。
func runAuthRoundtrip(t *testing.T, endpoint, topic, group, ak, sk, wantBody string) {
	t.Helper()
	producer, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{AccessKey: ak, AccessSecret: sk},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("凭据 %s NewProducer: %v", ak, err)
	}
	if err := producer.Start(); err != nil {
		t.Fatalf("凭据 %s Start 失败: %v", ak, err)
	}
	defer producer.GracefulStop()
	if _, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(wantBody)}); err != nil {
		t.Fatalf("凭据 %s 发送失败: %v", ak, err)
	}
	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{AccessKey: ak, AccessSecret: sk},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("凭据 %s NewSimpleConsumer: %v", ak, err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("凭据 %s consumer.Start: %v", ak, err)
	}
	defer consumer.GracefulStop()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询返回 MESSAGE_NOT_FOUND，正常
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != wantBody {
				t.Fatalf("凭据 %s 消息体不符: %s", ak, mv.GetBody())
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("凭据 %s Ack: %v", ak, err)
			}
			t.Logf("凭据 %s 全链路完成: 收到并 ack %q", ak, wantBody)
			return
		}
	}
	t.Fatalf("凭据 %s 60s 内未消费到消息", ak)
}
