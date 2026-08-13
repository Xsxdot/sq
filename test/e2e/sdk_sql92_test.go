//go:build e2e

// sdk_sql92_test.go：SQL92 属性过滤的端到端验证。
//
// 职责：用官方 Go SDK 的 SQL92 订阅真实收发，证明服务端过滤对真实客户端
// 生效，且未命中消息对该消费组永久越过位点
//
// 边界：
//   - 不验证语法细节（三值逻辑、类型强转、错误信息全部由
//     internal/core/deliver 的单测覆盖），这里只证明"接线通了、
//     语义在真实链路上成立"
//   - 不验证非法表达式的拒绝：那是 rpc 层单测的事
package e2e

import (
	"context"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// sql92Msg 一条待发消息：body 作为身份标识，props 决定它是否命中。
type sql92Msg struct {
	body  string
	props map[string]string
}

// runSQL92Case 发一批消息、用 SQL92 表达式订阅、断言恰好收到 wantBodies，
// 再断言未命中消息已被位点永久越过。
//
// 参数：
//   - expr: SQL92 过滤表达式
//   - msgs: 待发消息（顺序即发送顺序）
//   - wantBodies: 期望收到的 body 集合
func runSQL92Case(t *testing.T, topic, group, expr string, msgs []sql92Msg, wantBodies []string) {
	t.Helper()
	endpoint := startBroker(t)

	producer, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := producer.Start(); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	defer producer.GracefulStop()

	for _, m := range msgs {
		msg := &rmq.Message{Topic: topic, Body: []byte(m.body)}
		for k, v := range m.props {
			msg.AddProperty(k, v)
		}
		if _, err := producer.Send(context.Background(), msg); err != nil {
			t.Fatalf("Send %s: %v", m.body, err)
		}
	}

	// 必须用 NewFilterExpressionWithType 并显式传 rmq.SQL92。
	// Go SDK 的 FilterExpressionType 里 SQL92 = iota = 0，是该类型的零值；
	// 反倒是 NewFilterExpression() 显式设 TAG。类型参数写错的两个方向都很
	// 隐蔽：漏传 -> 拿到 SQL92（以为在测 TAG，实际在测 SQL），
	// 误用 NewFilterExpression -> 表达式被当 TAG 解析，服务端把整条
	// SQL 串当成一个 tag 名，结果是一条都收不到而不是报错。
	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			topic: rmq.NewFilterExpressionWithType(expr, rmq.SQL92),
		}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	defer consumer.GracefulStop()

	want := map[string]bool{}
	for _, b := range wantBodies {
		want[b] = true
	}
	got := map[string]bool{}
	deadline := time.Now().Add(60 * time.Second)
	for len(got) < len(want) && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			body := string(mv.GetBody())
			if !want[body] {
				t.Fatalf("表达式 %q 收到不该命中的消息: %s", expr, body)
			}
			got[body] = true
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
	}
	if len(got) != len(want) {
		t.Fatalf("表达式 %q 期望收到 %v，实际 %v", expr, wantBodies, got)
	}

	// 连续 4 轮空结果（= e2e broker 的 default_queue_nums）才说明每条队列
	// 都已过滤到位点尽头。
	//
	// 为什么不能只轮询一次就断言为空：SimpleConsumer 每次 Receive 只轮询
	// 一个队列，命中消息收齐时，未命中消息所在的队列可能压根还没被扫过——
	// 那是"还没被跳过"，不是"已被永久越过"，单次空结果证明不了任何事。
	// 这个坑在 TestOfficialGoSDKTagFilter 里已经踩过一次。
	swept := 0
	for swept < 4 && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil || len(mvs) == 0 {
			swept++
			continue
		}
		swept = 0
		for _, mv := range mvs {
			t.Fatalf("扫尾阶段收到额外消息: %s（未命中消息应已永久越过位点）", mv.GetBody())
		}
	}
	if swept < 4 {
		t.Fatalf("表达式 %q 未能在期限内把 4 条队列都轮询到空", expr)
	}
}

// TestOfficialGoSDKSQL92Numeric 数值比较 + BETWEEN。
func TestOfficialGoSDKSQL92Numeric(t *testing.T) {
	runSQL92Case(t, "e2e-sql-num", "e2e-sql-num-g", "age BETWEEN 18 AND 65",
		[]sql92Msg{
			{"a10", map[string]string{"age": "10"}},
			{"a18", map[string]string{"age": "18"}}, // 下界闭区间
			{"a40", map[string]string{"age": "40"}},
			{"a65", map[string]string{"age": "65"}}, // 上界闭区间
			{"a70", map[string]string{"age": "70"}},
		},
		[]string{"a18", "a40", "a65"})
}

// TestOfficialGoSDKSQL92StringIn 字符串 IN。
func TestOfficialGoSDKSQL92StringIn(t *testing.T) {
	runSQL92Case(t, "e2e-sql-in", "e2e-sql-in-g", "region IN ('cn', 'us')",
		[]sql92Msg{
			{"cn", map[string]string{"region": "cn"}},
			{"us", map[string]string{"region": "us"}},
			{"eu", map[string]string{"region": "eu"}},
		},
		[]string{"cn", "us"})
}

// TestOfficialGoSDKSQL92Combined 组合逻辑，顺带验证 AND 优先级高于 OR：
// 表达式等价于 (age > 18 AND region = 'cn') OR vip = TRUE。
// 判别信号是 young-vip 落选——若服务端把优先级搞反成
// age > 18 AND (region = 'cn' OR vip = TRUE)，young-vip 会命中，立即被抓。
// old-us 则是「属性缺失不得放行」的负探针：它缺 vip 属性，三值语义下
// (region = 'cn' OR vip = TRUE) 求值为 UNKNOWN，绝不投递。
func TestOfficialGoSDKSQL92Combined(t *testing.T) {
	runSQL92Case(t, "e2e-sql-comb", "e2e-sql-comb-g",
		"age > 18 AND region = 'cn' OR vip = TRUE",
		[]sql92Msg{
			{"old-cn", map[string]string{"age": "30", "region": "cn"}},
			{"old-us", map[string]string{"age": "30", "region": "us"}},
			{"young-cn", map[string]string{"age": "10", "region": "cn"}},
			{"young-vip", map[string]string{"age": "10", "region": "us", "vip": "true"}},
		},
		[]string{"old-cn", "young-vip"})
}

// TestOfficialGoSDKSQL92MissingProperty 三值逻辑的端到端证据：
// 属性缺失求值为 UNKNOWN，不投递。
//
// 这条是四个用例里最重要的一条——退化成二值逻辑（缺失当 FALSE）时它照样
// 通过，但退化成"缺失当空串再比较"或"比较失败即放行"时它会立刻失败。
func TestOfficialGoSDKSQL92MissingProperty(t *testing.T) {
	runSQL92Case(t, "e2e-sql-null", "e2e-sql-null-g", "age > 18",
		[]sql92Msg{
			{"has-age", map[string]string{"age": "20"}},
			{"no-age", map[string]string{"other": "x"}},
			{"bad-age", map[string]string{"age": "not-a-number"}},
		},
		[]string{"has-age"})
}
