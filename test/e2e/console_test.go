//go:build e2e

// 控制台（M5b 内嵌 Web 控制台）的端到端验收：单二进制内嵌、时序采样、消费总账。
//
// 职责：
//   - TestConsoleServedFromBinary：验证「单二进制启动即见一切」——控制台在
//     admin 端口的根路径上，前端路由回 index.html 而不是 404
//   - TestTimeseriesEndToEnd：验证时序采样器真的在跑——发一批消息后，
//     内存环里应出现非零写入速率
//   - TestLedgerReflectsConsumption：验证总账反映消费关系——发 N 条、消费
//     取到 M 条后，落后量应为 N-M（pending = 写入头 − fetch 位点）
//
// 边界：
//   - 三个用例都走免登录配置（不设 admin_username/admin_password），admin
//     端点直通；登录链路由 TestAdminAPISmoke 单独覆盖，互不干扰
//   - 采样间隔 5 秒、速率需要两个样本差分，时序断言用轮询而非一次性 GET
//   - 不覆盖控制台页面内部逻辑（由 web/src 的 Vitest 单测负责）
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"

	"github.com/xushixin/sq/internal/config"
)

// startBrokerWithAdmin 起一个带 admin HTTP（免登录）的 broker，返回 gRPC 接入点
// 与 admin 基址（即控制台的访问地址）。
//
// 为什么沿用 TestAdminAPISmoke 的「先占端口再选端口」：pickPort 首选固定端口
// preferredPort，探测完即释放，紧接着再选一次还会拿到同一个端口——若 admin 与
// gRPC 都拿到它，先绑定的 admin HTTP 会让 gRPC bind 失败导致 broker 退出。
// 先占住 admin 端口迫使 gRPC 走内核分配分支，保证两个端口不同。
func startBrokerWithAdmin(t *testing.T) (endpoint, adminBase string) {
	t.Helper()
	adminPort := pickPort(t)
	hold, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", adminPort))
	if err != nil {
		t.Fatalf("占住 admin 端口失败: %v", err)
	}
	grpcPort := pickPort(t)
	hold.Close()
	startBroker(t, func(c *config.Config) {
		c.GRPCListen = fmt.Sprintf("127.0.0.1:%d", grpcPort)
		c.AdvertisePort = grpcPort
		c.AdminListen = fmt.Sprintf("127.0.0.1:%d", adminPort)
		// 不设 AdminUsername/AdminPassword：免登录直通，用例不必先走登录
	})
	return fmt.Sprintf("127.0.0.1:%d", grpcPort), fmt.Sprintf("http://127.0.0.1:%d", adminPort)
}

// sendViaSDK 经官方 SDK producer 向 topic 发 n 条消息，返回全部发送回执的 msgId。
// 装配方式与 TestOfficialGoSDKSendAndReceive 一致：空凭证 + WithTopics 预建 topic。
func sendViaSDK(t *testing.T, endpoint, topic string, n int) []string {
	t.Helper()
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
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		msg := &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("console payload #%d", i))}
		recs, err := producer.Send(context.Background(), msg)
		if err != nil {
			t.Fatalf("Send #%d 失败: %v", i, err)
		}
		if len(recs) == 0 {
			t.Fatalf("Send #%d 返回空回执", i)
		}
		ids = append(ids, recs[0].MessageID)
	}
	return ids
}

// consumeFetch 经 SimpleConsumer 逐条 ack 地消费，直到恰好取到 n 条。
//
// 为什么这里只关心「取到」而不关心 ack：sq 的 cursor 是 **fetch 位点**，
// Receive 取出即推进（见 internal/core/deliver 包注释）——总账的 pending
// = 写入头 − cursor，衡量的是「该组还没取到的消息」，与 ack 与否无关
// （未 ack 的消息由 inflight 记录兜底重投，不占 pending）。因此原计划的
// 「ack 20 条 → pending 30」在 sq 语义下应表述为「取到 20 条 → pending 30」。
//
// 为什么每轮只取 4 条：SDK 的 SimpleConsumer 每轮轮询一个队列（round-robin，
// 见 SDK loadBalancer 的 TakeMessageQueue），一轮最多取 maxMsgs 条。
// 50 条消息分布在 4 个队列（约 13/13/12/12），每轮取 4 条恰好 5 轮取完
// 20 条，且必然覆盖全部 4 个队列——总账行只列出该组「已触及」的队列，
// 若某个队列从未被取过，它在总账里不可见，next_offset 也不会计入该队列，
// pending 就永远对不上。取 16 条/轮会在一两个队列就凑满 20，其余队列
// 从未触及，断言必然失败。
func consumeFetch(t *testing.T, endpoint, group, topic string, n int) {
	t.Helper()
	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			topic: rmq.SUB_ALL,
		}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	defer consumer.GracefulStop()

	fetched := 0
	acked := 0
	deadline := time.Now().Add(60 * time.Second)
	for fetched < n && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 4, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			if fetched >= n {
				break
			}
			fetched++
			// ack 只清 inflight，不影响 pending；逐条确认避免被重投的
			// 消息混入后续轮次，把「取到 n 条」这个计数搞乱
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			acked++
		}
	}
	if fetched != n {
		t.Fatalf("应恰好取到 %d 条，实际 %d（acked=%d）", n, fetched, acked)
	}
}

// waitFor 在 duration 内每 500ms 轮询 fn，直到它为 true；超时则 Fatal。
func waitFor(t *testing.T, duration time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("等待条件在截止时间前未满足")
}

// TestConsoleServedFromBinary 验证「单二进制启动即见一切」：不需要任何外部文件，
// 控制台就在 admin 端口的根路径上（未构建时请先 make web）。
func TestConsoleServedFromBinary(t *testing.T) {
	_, adminBase := startBrokerWithAdmin(t)

	res, err := http.Get(adminBase + "/")
	if err != nil {
		t.Fatalf("请求控制台首页: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("控制台首页应 200（未构建请先 make web），得到 %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("首页应是 HTML，得到 %s", ct)
	}

	// 前端路由必须回 index.html 而不是 404
	res2, err := http.Get(adminBase + "/groups/order-svc")
	if err != nil {
		t.Fatalf("请求前端路由: %v", err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusOK {
		t.Fatalf("前端路由应回首页，得到 %d", res2.StatusCode)
	}
}

// TestTimeseriesEndToEnd 验证采样器真的在跑：发一批消息后，时序里应出现非零
// 写入速率（速率需要两个样本差分，第一个样本恒为 0，所以要等一个采样周期）。
func TestTimeseriesEndToEnd(t *testing.T) {
	endpoint, adminBase := startBrokerWithAdmin(t)
	sendViaSDK(t, endpoint, "e2e-ts", 200)

	// 采样间隔 5 秒：发送落在某两次采样之间，下一次采样即出现非零速率。
	// 15 秒足够覆盖「发送恰在采样后立即发生」的最差相位。
	waitFor(t, 15*time.Second, func() bool {
		var ts struct {
			Source string `json:"source"`
			Points []struct {
				QPS float64 `json:"qps"`
			} `json:"points"`
		}
		res, err := http.Get(adminBase + "/admin/timeseries?range=1h")
		if err != nil {
			return false
		}
		defer res.Body.Close()
		if json.NewDecoder(res.Body).Decode(&ts) != nil {
			return false
		}
		if ts.Source != "ring" {
			return false
		}
		for _, p := range ts.Points {
			if p.QPS > 0 {
				return true
			}
		}
		return false
	})
}

// TestLedgerReflectsConsumption 验证总账真的能反映消费关系：发 50 条、消费
// （取到）20 条后，落后量应为 30。
//
// pending = 写入头 − fetch 位点：取件即推进位点（见 consumeFetch 注释），
// 因此取件同步落盘后单次 GET 即可断言，无需轮询。
func TestLedgerReflectsConsumption(t *testing.T) {
	endpoint, adminBase := startBrokerWithAdmin(t)
	sendViaSDK(t, endpoint, "e2e-ledger", 50)
	consumeFetch(t, endpoint, "e2e-ledger-g", "e2e-ledger", 20)

	var rows []struct {
		Group   string `json:"group"`
		Topic   string `json:"topic"`
		Pending uint64 `json:"pending"`
	}
	res, err := http.Get(adminBase + "/admin/ledger")
	if err != nil {
		t.Fatalf("请求总账: %v", err)
	}
	defer res.Body.Close()
	if err := json.NewDecoder(res.Body).Decode(&rows); err != nil {
		t.Fatalf("解析总账: %v", err)
	}
	found := false
	for _, r := range rows {
		if r.Group == "e2e-ledger-g" && r.Topic == "e2e-ledger" {
			found = true
			if r.Pending != 30 {
				t.Fatalf("落后量应为 30，得到 %d", r.Pending)
			}
		}
	}
	if !found {
		t.Fatal("总账里应出现 e2e-ledger-g × e2e-ledger 这条消费关系")
	}
}
