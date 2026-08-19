//go:build e2e

// 官方 Go SDK PushConsumer e2e：B13.2 的验证载体。
//
// 职责：
//   - 覆盖 push 消费路径（callback 驱动）的七条真实链路：基础闭环、长轮询唤醒、
//     消费失败重投、超限转 DLQ、FIFO 顺序不破、不可见期到期重投、AutoRenew 续租
//   - 实证 settings.go 下发的 fifo=false 是正确终态：重试计数与死信判定都归 broker
//
// 边界：
//   - 不覆盖 LitePushConsumer（依赖未实现的 SyncLiteSubscription）
//   - 不覆盖集群档（本文件全部单机 broker）
//   - 不做批量 ReceiveBatchSize 的断言（客户端侧观察不到有意义的差别）
//   - 本套件不是 -race 干净的：竞态成因在 SDK v5.1.4 内部——Start() 读
//     pcSettings.isFifo（push_consumer.go:379）与 telemetry 回调
//     applySettingsCommand 写同一字段（push_consumer_options.go:226）并发，
//     双方都是 SDK goroutine，与本套件代码无关；采集器自身的并发保护已由
//     mutex/atomic 覆盖。跑 -race 会有概率性失败，验证用不带 -race 的命令。
package e2e

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"github.com/Xsxdot/sq/internal/config"
)

// recv 是一次 listener 调用入口处的值快照。
//
// 边界：刻意不持有 *rmq.MessageView 指针——SDK 在回调返回后可能复用/回收该对象，
// 跨 goroutine 持有它读出来的值不可信。
type recv struct {
	id      string
	body    string
	attempt int32
	receipt string // 回执句柄：用例 3 用它判别「broker 重投」还是「客户端本地重投」
	group   string // MessageGroup
}

// pushCollector 把 callback 驱动的消费收敛成测试可断言的结构。
//
// 边界：只做采集与返回值决策，不做任何断言。listener 跑在 SDK 的后台 goroutine
// 上，而 t.Fatalf 只有在测试 goroutine 内调用才有定义的行为——在别处调不会正确
// 终止测试，只会让失败以诡异方式表现。所有断言一律回主 goroutine 做。
type pushCollector struct {
	t *testing.T

	mu  sync.Mutex
	got []recv // 按 listener【入口】到达序追加

	inFlight    atomic.Int32 // 当前并发进行中的 listener 调用数
	maxInFlight atomic.Int32 // 历史峰值，用例 5 的核心断言

	// decide 决定每条消息的消费结果；nil 视为 rmq.SUCCESS。
	decide func(*rmq.MessageView) rmq.ConsumerResult
	// hold 是 listener 内部的停留时长，用于撑开并发重叠的观测窗口。
	hold time.Duration
}

// newCollector 构造一个默认全部 ACK、不停留的采集器。
func newCollector(t *testing.T) *pushCollector {
	return &pushCollector{t: t}
}

// listener 返回可交给 WithPushMessageListener 的监听器。
//
// MessageListener 接口的 consume 方法未导出，包外无法自行实现，
// 只能走 SDK 提供的 FuncMessageListener 适配器。
func (c *pushCollector) listener() *rmq.FuncMessageListener {
	return &rmq.FuncMessageListener{
		Consume: func(mv *rmq.MessageView) rmq.ConsumerResult {
			// 并发峰值必须在【入口】抬起、在【出口】落下。只在出口统计的话，
			// 无论真实并发多少，读到的永远是 1。
			n := c.inFlight.Add(1)
			for {
				old := c.maxInFlight.Load()
				if n <= old || c.maxInFlight.CompareAndSwap(old, n) {
					break
				}
			}
			defer c.inFlight.Add(-1)

			var mg string
			if g := mv.GetMessageGroup(); g != nil {
				mg = *g
			}
			r := recv{
				id:      mv.GetMessageId(),
				body:    string(mv.GetBody()),
				attempt: mv.GetDeliveryAttempt(),
				receipt: mv.GetReceiptHandle(),
				group:   mg,
			}
			// 采集点必须在 hold 之前：用例 5 要证的是「listener 被调用的顺序」。
			// 先 hold 再 append 采到的是【完成序】——完成序乱不代表调用序乱，
			// 那是在测另一件事，且是会假红的那种错。
			c.mu.Lock()
			c.got = append(c.got, r)
			c.mu.Unlock()
			c.t.Logf("listener 入口: body=%s attempt=%d inflight=%d", r.body, r.attempt, n)

			if c.hold > 0 {
				time.Sleep(c.hold)
			}
			res := rmq.SUCCESS
			if c.decide != nil {
				res = c.decide(mv)
			}
			c.t.Logf("listener 出口: body=%s 结果=%s", r.body, consumerResultName(res.Type))
			return res
		},
	}
}

// snapshot 加锁拷贝已采集的记录。
func (c *pushCollector) snapshot() []recv {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]recv, len(c.got))
	copy(out, c.got)
	return out
}

// count 已采集条数。
func (c *pushCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got)
}

// startPushConsumer 建 PushConsumer、Start、注册 cleanup，返回一个幂等的
// stop 函数。
//
// 返回 stop 而不是只返回消费者：用例 6 必须在测试中途显式停 A，而 cleanup 里
// 还会再停一次；sync.Once 包住后重复调用是 no-op。消费者返回值在全部调用点都
// 被丢弃，t.Cleanup(stop) 在函数体内执行，与返回值无关。
func startPushConsumer(t *testing.T, endpoint, group, topic string, c *pushCollector, opts ...rmq.PushConsumerOption) func() {
	t.Helper()
	base := []rmq.PushConsumerOption{
		rmq.WithPushSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
		rmq.WithPushMessageListener(c.listener()),
	}
	pc, err := rmq.NewPushConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	}, append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewPushConsumer(%s): %v", group, err)
	}
	if err := pc.Start(); err != nil {
		t.Fatalf("pushConsumer.Start(%s): %v", group, err)
	}
	t.Logf("PushConsumer 已启动: group=%s topic=%s", group, topic)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			if err := pc.GracefulStop(); err != nil {
				t.Logf("GracefulStop(%s) 返回错误（不判失败）: %v", group, err)
			}
			t.Logf("PushConsumer 已停止: group=%s", group)
		})
	}
	t.Cleanup(stop)
	return stop
}

// waitCount 轮询等到累计采集条数达 n，超时即 Fatalf。
//
// 用轮询而非 channel 唤醒：channel 通知要处理「唤醒丢失」和「多次到达只醒一次」，
// 写错就是间歇性挂；e2e 本身是秒级尺度，50ms 轮询没有隐蔽失败模式。
func waitCount(t *testing.T, c *pushCollector, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if c.count() >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("在 %v 内只采集到 %d/%d 条，已收到: %+v", within, c.count(), n, c.snapshot())
}

// sendPlain 发送若干条普通消息（无 MessageGroup、无 tag）。
func sendPlain(t *testing.T, endpoint, topic string, bodies ...string) {
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
	for _, b := range bodies {
		if _, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(b)}); err != nil {
			t.Fatalf("Send %q: %v", b, err)
		}
	}
	t.Logf("已发送 %d 条到 topic=%s", len(bodies), topic)
}

// bodySet 把采集记录折成 body 集合，便于做集合断言。
func bodySet(rs []recv) map[string]int {
	m := make(map[string]int, len(rs))
	for _, r := range rs {
		m[r.body]++
	}
	return m
}

// consumerResultName 把 SDK 的 ConsumerResultType 折成可读文本，供 listener
// 出口日志使用（SDK 未提供 String()，直接 %d 只能看到 0/1）。
func consumerResultName(rt rmq.ConsumerResultType) string {
	switch rt {
	case rmq.ConsumerResultTypeSuccess:
		return "SUCCESS"
	case rmq.ConsumerResultTypeFailure:
		return "FAILURE"
	case rmq.ConsumerResultTypeSuspend:
		return "SUSPEND"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int8(rt))
	}
}

// TestOfficialGoSDKPushBasicLoop 用例 1：QueryAssignment → 长轮询 Receive →
// listener → Ack 整条链路通，且 Ack 真落地（位点推进）。
func TestOfficialGoSDKPushBasicLoop(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) {
		// 1b 的静默窗口必须覆盖一个完整的不可见期，否则断言假绿：
		// 若 ack 丢失，消息要等不可见期到期才会重投，窗口短于它就观测不到差别，
		// 「已 ack」与「ack 全丢」两个世界给出同样的零调用。
		c.DefaultInvisibleDuration = "10s"
	})
	const (
		topic = "e2e-push-basic"
		group = "e2e-push-basic-g"
	)
	bodies := []string{"p-0", "p-1", "p-2", "p-3", "p-4"}
	sendPlain(t, endpoint, topic, bodies...)

	c := newCollector(t)
	stopA := startPushConsumer(t, endpoint, group, topic, c)
	// 1a：5 条全部到达，body 集合与发送集合相等
	waitCount(t, c, len(bodies), 30*time.Second)
	got := bodySet(c.snapshot())
	for _, b := range bodies {
		if got[b] != 1 {
			t.Fatalf("body %q 收到 %d 次，期望恰好 1 次；全量: %v", b, got[b], got)
		}
	}
	if len(got) != len(bodies) {
		t.Fatalf("收到 %d 种 body，期望 %d 种: %v", len(got), len(bodies), got)
	}

	// 1b：停掉 A 后同组新起 B，15s 窗口内零调用 —— 证明 Ack 真落地，位点推过了。
	//
	// 这里不需要 SQL92 用例那种「N 轮连续空轮询」的手法：那条纪律的成因是
	// SimpleConsumer 每次 Receive 只轮询一个队列，单次为空不代表所有队列都空。
	// PushConsumer 对每个已分配队列各维持一条长轮询，一个持续窗口内的零调用
	// 即覆盖全部队列。
	stopA()
	cB := newCollector(t)
	// B 必须与 A 【同组】：换组会从头重投，那测的是另一件事。
	startPushConsumer(t, endpoint, group, topic, cB)
	time.Sleep(15 * time.Second)
	if n := cB.count(); n != 0 {
		t.Fatalf("Ack 未落地：新消费者在 15s 窗口内收到 %d 条: %+v", n, cB.snapshot())
	}
	t.Logf("用例 1 通过：5 条闭环 + Ack 位点已推进")
}

// TestOfficialGoSDKPushFIFOOrderLock 用例 5：同 MessageGroup 的 20 条消息在
// 4 线程 push 消费下顺序不破。
//
// 这是 settings.go 下发 fifo=false 的承重断言：顺序安全不依赖协商标志，
// 而由 broker 侧的队列级顺序锁保证（每队列至多一条未终结的顺序 inflight）。
func TestOfficialGoSDKPushFIFOOrderLock(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-push-fifo"
		group = "e2e-push-fifo-g"
		mg    = "order-key"
		total = 20
	)
	sendFifoBatch(t, endpoint, topic, mg, "fifo", total)

	c := newCollector(t)
	// hold 必须远小于不可见时长（默认 1m，本用例不改）：若 hold 逼近不可见窗口，
	// broker 会认为该条超时未终结而重投，重投件与仍在跑的那件重叠 →
	// maxInFlight 变 2 → 假红。50ms 相对 1m 有三个数量级余量。
	c.hold = 50 * time.Millisecond
	startPushConsumer(t, endpoint, group, topic, c,
		rmq.WithPushConsumptionThreadCount(4))
	waitCount(t, c, total, 120*time.Second)

	// 断言 A：body 序严格全等于发送序。
	//
	// 用严格全等而不是「递增」：重投会让 body 重复出现，全等能把它照出来。
	// 真跑出间歇性红时正确的修法是调高不可见时长，不是把断言放宽成忽略重复
	// ——那等于把要测的东西测没了。
	snap := c.snapshot()
	if len(snap) != total {
		t.Fatalf("采集到 %d 条，期望恰好 %d 条（多出来的是重投件）: %+v", len(snap), total, snap)
	}
	for i, r := range snap {
		want := fmt.Sprintf("fifo-%d", i)
		if r.body != want {
			t.Fatalf("第 %d 条乱序：期望 %s 收到 %s；全量: %+v", i, want, r.body, snap)
		}
		if r.group != mg {
			t.Fatalf("第 %d 条 MessageGroup 回读不符: %q", i, r.group)
		}
	}

	// 断言 B：并发峰值恒为 1 —— 顺序锁的直接证据，也是本用例真正的证明。
	//
	// 只写断言 A 是不够的：顺序锁正确时客户端手上永远只有一条，4 个线程没有
	// 乱序机会，A 会恒真；锁坏掉时 A 只是【概率性】变红。B 是确定性的——
	// 线程池有 4 个线程、listener 撑住 50ms，只要 broker 任何一次放出两条
	// 同队列顺序消息，重叠必被观测到，maxInFlight 立刻变 2。
	if peak := c.maxInFlight.Load(); peak != 1 {
		t.Fatalf("顺序锁失效：listener 并发峰值 = %d，期望 1", peak)
	}
	t.Logf("用例 5 通过：20 条严格按序，并发峰值 1")
}

// TestOfficialGoSDKPushRetryOwnedByBroker 用例 3：消费失败后由 broker 重投，
// 不是客户端本地循环重投 listener。
//
// 这是 fifo=false 的另一半证据：翻成 true 会让客户端改建 FiFoConsumeService，
// 消费失败转为客户端本地重投，重试计数就不再归 broker 了。
func TestOfficialGoSDKPushRetryOwnedByBroker(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-push-retry"
		group = "e2e-push-retry-g"
		body  = "retry-me"
	)
	sendPlain(t, endpoint, topic, body)

	c := newCollector(t)
	c.decide = func(*rmq.MessageView) rmq.ConsumerResult { return rmq.FAILURE }
	startPushConsumer(t, endpoint, group, topic, c)
	// waitCount 等的是第 2 次投递【到来】的时间：FAILURE 经 nackMessage →
	// changeInvisibleDuration 下发退避（协商策略 initial 100ms / max 1s），
	// 是亚秒级，这条路径不经过 default_invisible_duration。120s 预算留足余量。
	waitCount(t, c, 2, 120*time.Second)

	snap := c.snapshot()
	first, second := snap[0], snap[1]
	// 3a：同一条消息被投了两次
	if first.id != second.id {
		t.Fatalf("两次不是同一条消息: %s vs %s", first.id, second.id)
	}
	if first.body != body || second.body != body {
		t.Fatalf("body 不符: %q / %q", first.body, second.body)
	}
	// 3b：投递次数递增
	if first.attempt != 1 || second.attempt != 2 {
		t.Fatalf("投递次数不符: %d → %d，期望 1 → 2", first.attempt, second.attempt)
	}
	// 3c：判别器 —— 回执句柄必须变。
	//
	// 3b 单独不足以判别：客户端本地重试同样会自增 deliveryAttempt
	// （eraseFifoMessage 里就是 mv.deliveryAttempt += 1），两条路的 attempt 都会涨。
	// 只有句柄能把它们分开——本地重试复用同一个 MessageView 反复喂 listener，
	// 句柄不变；只有真的回了 broker、broker 重新发件，才会拿到编着新 attempt
	// 的新句柄。
	if first.receipt == second.receipt {
		t.Fatalf("回执句柄未变（%q）：说明是客户端本地重投，不是 broker 重投", first.receipt)
	}
	t.Logf("用例 3 通过：attempt 1→2 且句柄已换，重试归 broker")
}

// TestOfficialGoSDKPushDLQOwnedByBroker 用例 4：投递超限后由 broker 转入
// %DLQ%{group}，listener 不再被调用。
//
// 限界（如实记录，不包装）：DLQ 条目本身带不出「谁转的」签名——
// ForwardMessageToDeadLetterQueue（RPC 路径，deliver.go:611）与 broker 自动超限
// 路径最终汇进同一个 moveToDLQ（deliver.go:671），产出完全一致。所以「DLQ 判定
// 归 broker」不是单条断言能证的，它由三件事合起来支撑：4a（listener 停止被调用）
// + 4b（消息确实在 DLQ）+ SDK 侧静态事实（标准消费服务的 eraseMessage 只有
// ack/nack 两条分支，根本没有 forward 调用）。第三条是读代码得来的，不是跑出来的。
func TestOfficialGoSDKPushDLQOwnedByBroker(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) {
		c.DefaultMaxAttempts = 2 // 2 次投递即超限，控制用例时长
	})
	const (
		topic = "e2e-push-dlq"
		group = "e2e-push-dlq-g"
		body  = "push-poison"
	)
	sendPlain(t, endpoint, topic, body)

	c := newCollector(t)
	c.decide = func(*rmq.MessageView) rmq.ConsumerResult { return rmq.FAILURE }
	startPushConsumer(t, endpoint, group, topic, c)
	waitCount(t, c, 2, 120*time.Second)

	// 4a：恰 2 次后不再被调用。
	//
	// 必须用【连续静默窗口】判定，不是「等一会儿看一眼」：看一眼恰好落在两次
	// 投递之间就会误判为已停止。连续 15 轮 × 1s 计数不变才算数。
	// 循环本身已是完整判据：任何一轮 count() != 2 都直接 Fatalf，不存在
	// 「等完 15 轮再回头看」的情况。
	for i := 0; i < 15; i++ {
		time.Sleep(time.Second)
		if c.count() != 2 {
			t.Fatalf("超限后 listener 仍被调用：第 %d 秒计数变为 %d；全量: %+v", i+1, c.count(), c.snapshot())
		}
	}

	// 4b：死信作为普通 topic 从 %DLQ%{group} 被读到。
	//
	// 转入是惰性的（原队列下一次 Receive 触发）——这里不需要像 SimpleConsumer 版
	// 那样手动「戳原 topic」：push 消费者一直挂着长轮询，戳的动作天然在发生。
	dlqTopic := "%DLQ%" + group
	dlqConsumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group + "-reader",
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{dlqTopic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer(DLQ): %v", err)
	}
	if err := dlqConsumer.Start(); err != nil {
		t.Fatalf("dlqConsumer.Start: %v", err)
	}
	defer dlqConsumer.GracefulStop()

	var gotBody string
	deadline := time.Now().Add(120 * time.Second)
	for gotBody == "" && time.Now().Before(deadline) {
		mvs, err := dlqConsumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 返回，属正常
		}
		for _, mv := range mvs {
			gotBody = string(mv.GetBody())
			if props := mv.GetProperties(); props["sq-origin-topic"] != topic {
				t.Fatalf("死信缺少来源属性 sq-origin-topic: %v", props)
			}
			if err := dlqConsumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack 死信: %v", err)
			}
		}
	}
	if gotBody != body {
		t.Fatalf("死信未到达或内容不符: %q", gotBody)
	}
	t.Logf("用例 4 通过：listener 恰 2 次后静默，消息在 %s 中", dlqTopic)
}

// TestOfficialGoSDKPushLongPollingWakeup 用例 2：消息到达即唤醒长轮询，
// 而不是慢周期轮询。
func TestOfficialGoSDKPushLongPollingWakeup(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-push-wakeup"
		group = "e2e-push-wakeup-g"
	)
	c := newCollector(t)
	startPushConsumer(t, endpoint, group, topic, c)

	// producer 提前建好并复用：NewProducer + Start 走路由查询 + telemetry 协商，
	// 耗时不确定，若把它算进唤醒计时，慢一次就假红。
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
	sendBody := func(b string) {
		if _, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(b)}); err != nil {
			t.Fatalf("Send %q: %v", b, err)
		}
	}

	// 探针先行：消费者 Start() 之后要走 Telemetry 协商 + QueryAssignment 才会挂上
	// 长轮询，这段耗时不确定。不用探针而直接「起消费者 → 睡 2s → 发消息 → 计时」，
	// 测的是「协商耗时 + 唤醒耗时」的和，协商慢一点就假红。探针到达后，
	// t0 之后长轮询确定已在挂着，量到的才是纯唤醒延迟。
	sendBody("probe")
	waitCount(t, c, 1, 30*time.Second)

	// 复用同一 producer，只把第二次 Send 圈进计时；Send 本身是几十毫秒级。
	t0 := time.Now()
	sendBody("wakeup")
	waitCount(t, c, 2, 30*time.Second)
	elapsed := time.Since(t0)

	// 阈值 3s：若客户端退化成慢周期轮询，周期上限是 defaultLongPolling = 20s，
	// 唤醒延迟会落在秒级到 20s；3s 能把两者分开，同时对本机 e2e 抖动留足余量。
	if elapsed >= 3*time.Second {
		t.Fatalf("唤醒延迟 %v ≥ 3s：疑似退化成周期轮询而非长轮询", elapsed)
	}
	if snap := c.snapshot(); snap[1].body != "wakeup" {
		t.Fatalf("第 2 条 body = %q，期望 wakeup", snap[1].body)
	}
	t.Logf("用例 2 通过：唤醒延迟 %v", elapsed)
}

// TestOfficialGoSDKPushRedeliverAfterInvisibleExpiry 用例 6：已投递给客户端但
// 尚未 ack 的消息，在 broker 侧不可见期到期后被重新投递。
//
// 为什么不测「GracefulStop 后 inflight 不丢」：SDK 的 simpleThreadPool 收到
// shutdown 信号后会排干任务队列（simple_thread_pool.go:49-54，worker 收到
// shutdown 后 for t := range tp.tasks { t() } 把剩余任务全部执行完），且
// Shutdown() 先 close(tp.tasks) 再 waitGroup.Wait()（simple_thread_pool.go:83-86）
// ——GracefulStop 期间已入队的消息全部会被消费并 ack，构造不出「从未进 listener、
// 也从未被 ack」的缓存。记下这个事实，免得以后有人照着旧标题「GracefulStop 后
// inflight 不丢」再推一遍错的假设。
//
// 真正属于 sq 的语义是这条：消息已投给客户端、尚未 ack，在不可见期到期后被
// 重新投递。单消费者即可测——A、B 同组同时长轮询时重投件落到谁手里是赌概率，
// 不能那么写。
func TestOfficialGoSDKPushRedeliverAfterInvisibleExpiry(t *testing.T) {
	// 5s 不可见时长：不改的话走默认 1m，本用例要干等 60s+。
	endpoint := startBroker(t, func(c *config.Config) {
		c.DefaultInvisibleDuration = "5s"
		// 必须显式关闭自动续租：PushConsumer 每次 ReceiveMessage 都下发
		// auto_renew=true，续租开启时这条消息会在持有者存活期间被一路续下去，
		// 永远走不到重投——本用例就再也观测不到它要测的东西了。
		// 这不是给用例打补丁：用例 6 测的是「不可见期到期兜底」这条路径本身，
		// 它必须在续租关闭的前提下才可见。续租开启时的正确行为由用例 7 覆盖，
		// 两者是一对互补的断言。
		c.AutoRenewEnabled = false
	})
	const (
		topic = "e2e-push-redeliver"
		group = "e2e-push-redeliver-g"
		body  = "redeliver-me"
	)
	sendPlain(t, endpoint, topic, body)

	c := newCollector(t)
	// 等待按投递次数分支，放进 decide 而不是 hold 字段：hold 是无条件 sleep，
	// 放那儿会让第二次投递也睡 8s，用例会滚成无限重投。
	c.decide = func(mv *rmq.MessageView) rmq.ConsumerResult {
		// 首投占住唯一的消费线程 8s，跨过 broker 侧 5s 的不可见期，让这条消息
		// 在「已投递给客户端但尚未 ack」的状态下被重新置为可见。重投件立即
		// 返回，否则每一代都要再等 8s。
		if mv.GetDeliveryAttempt() == 1 {
			time.Sleep(8 * time.Second)
		}
		return rmq.SUCCESS
	}
	startPushConsumer(t, endpoint, group, topic, c,
		rmq.WithPushConsumptionThreadCount(1))
	// 时间线（确定的）：
	//   t=0 首投进 listener，attempt=1，句柄 H1，唯一 worker 被占住；
	//   t=5s broker 侧不可见期到期，消息重新可见，长轮询把它收回来
	//        （attempt=2，句柄 H2），排进线程池等 worker；
	//   t=8s worker 空出：先跑完首投回调 → 用 H1 ack（H1 已失效，sq 会拒，
	//        SDK 日志会有一条 ack 失败，这是预期的，不是用例失败）；随即消费
	//        重投件 → 立即 SUCCESS → 用 H2 ack 成功 → 终止，不再重投。
	waitCount(t, c, 2, 40*time.Second)

	got := c.snapshot()
	// 6a：确实发生了重投。用 >= 而非 ==：worker 被占住期间 sq 可能多重投一代
	// （取决于重新置为可见的扫描粒度），多出来的采集条目是良性的。
	if len(got) < 2 {
		t.Fatalf("未发生重投：只采集到 %d 条: %+v", len(got), got)
	}
	// 6b：是同一条消息的重投，不是新消息。
	if got[1].id != got[0].id {
		t.Fatalf("第 2 条不是第 1 条的重投: %s vs %s", got[1].id, got[0].id)
	}
	// 6c：重投件投递次数递增。用 >=：多重投一代时 attempt 会更高。
	if got[1].attempt < 2 {
		t.Fatalf("重投件 attempt = %d，期望 ≥2", got[1].attempt)
	}
	// 6d【承重】：重投件换新回执句柄。
	//
	// 6d 与用例 3 的 3c 是同源判别器，不是对照：客户端本地重投会复用同一个
	// MessageView 反复喂 listener，句柄不变而 deliveryAttempt 照样自增
	// （eraseFifoMessage 路径，fifo=true 才会建的服务，本分支不走）——所以只断言
	// attempt 递增判别不出重投来自哪一侧，只有换了句柄才证明真的回了 broker。
	//
	// 与用例 3 的区别在触发方式而非归属：用例 3 是 FAILURE 触发显式 nack 后重投，
	// 本用例是完全不 ack、靠不可见期自然到期重投。前者验失败路径的归属，
	// 后者验超时兜底本身还活着。
	if got[1].receipt == got[0].receipt {
		t.Fatalf("重投件句柄未变（%q）：说明是客户端本地重投，不是 broker 重投", got[1].receipt)
	}
	t.Logf("用例 6 通过：不可见期到期后重投，attempt=%d 且句柄已换", got[1].attempt)
}

// TestOfficialGoSDKPushAutoRenewPreventsRedelivery 用例 7：慢 handler 在持有
// 消息期间被自动续租，因而不会被重复投递。
//
// 与用例 6 是一对互补断言：同样的构造（不可见期 5s、单消费线程、首投占住
// 8s），用例 6 关掉续租测「到期兜底还活着」，本用例开着续租测「续租真的挡住
// 了那次重投」。缺了本用例，把续租代码整段删掉用例 6 反而更绿——那样的测试
// 套件对本特性是零覆盖。
func TestOfficialGoSDKPushAutoRenewPreventsRedelivery(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) {
		// 5s 不可见期：不续租的话 t=5s 必然重投（用例 6 已证明这一点），
		// 所以「没有第二次投递」这个观测结果只能由续租解释。
		c.DefaultInvisibleDuration = "5s"
		// 显式写出默认值，让用例不依赖默认值将来是否变动
		c.AutoRenewEnabled = true
		// 预算刻意压到 15s：若续租真的污染了 attempts、t=8s 的 ack 被拒，
		// 消息会被续租到预算耗尽（t≈15s）后必然重投——重投落进下面的观测
		// 窗口，7a 才会红。预算取 10m 时重投要到 t≈600s，观测窗口根本
		// 等不到，7c 就成了恒真断言。
		c.AutoRenewMaxDuration = "15s"
	})
	const (
		topic = "e2e-push-autorenew"
		group = "e2e-push-autorenew-g"
		body  = "renew-me"
	)
	sendPlain(t, endpoint, topic, body)

	c := newCollector(t)
	c.decide = func(mv *rmq.MessageView) rmq.ConsumerResult {
		// 首投占住唯一消费线程 8s，跨过 5s 不可见期。与用例 6 的区别只有
		// broker 侧开着续租——同样按投递次数分支，万一真的发生重投，
		// 重投件立即返回，用例不会滚成无限重投而是直接在断言处红。
		if mv.GetDeliveryAttempt() == 1 {
			time.Sleep(8 * time.Second)
		}
		return rmq.SUCCESS
	}
	startPushConsumer(t, endpoint, group, topic, c,
		rmq.WithPushConsumptionThreadCount(1))

	// 时间线（确定的；waitCount 在 t≈0 即返回——采集点在 listener 入口、
	// hold 之前，count>=1 立即满足，所以 sleep 从 t≈0 起算）：
	//   t≈0   首投进 listener，attempt=1，句柄 H1，唯一 worker 被占住；
	//   t≈5s  broker 侧不可见期到期，长轮询触发扫描——四条判据全真
	//          （本次取件带租约、预算 15s 未耗尽、归属已记、持有者
	//          Telemetry 会话仍活）→ 续租到 t≈10s，不重投；
	//   t≈10s 不可见期再次到期 → 再续租，被硬上限钳到 t≈15s；
	//   t=8s   handler 返回，用**仍然有效**的 H1 ack 成功（续租没动 attempts，
	//          所以句柄没失效）；
	//   t≈15s 预算耗尽点：健康路径早已 ack、inflight 已删，此处无事发生。
	waitCount(t, c, 1, 20*time.Second)

	// 7b【承重】：观测窗口必须越过 t≈15s 的预算耗尽点（25s > 15s）。
	// 若续租改了 attempts，t=8s 的 ack 会被拒、消息续租到预算耗尽后必然
	// 重投（attempt>=2），落进本窗口；健康路径则只有首投一条。窗口短于
	// 预算耗尽点，「ack 成功」与「ack 被拒、消息正被无限续租」两个世界观测
	// 一致，7a 就成了假绿——这是 B13.2 终审抓出用例 1b 假绿的同款陷阱。
	time.Sleep(25 * time.Second)

	got := c.snapshot()
	// 7a【承重】：全窗口只投递过一次。这里必须用 == 而不是 >=：本用例证明的
	// 就是「不多投」，>= 会让断言恒真。若续租污染了 attempts 导致 ack 被拒，
	// 消息会在 t≈15s 预算耗尽后重投，本断言先红。
	if len(got) != 1 {
		t.Fatalf("期望恰好 1 次投递（续租应挡住重投），实际 %d 次: %+v", len(got), got)
	}
	// 7c：续租没有污染投递次数。判别器是「预算 15s 到期后必然重投」——若
	// 续租改了 attempts，t=8s 的 ack 被拒，消息续租到 t≈15s 预算耗尽后必然
	// 重投（attempt>=2）落进观测窗口，7a 会先红；此处断言健康路径下唯一的
	// 那次投递仍是首投。它同时反证句柄未失效：若句柄失效，就不会只有一次
	// attempt=1 的投递。
	if got[0].attempt != 1 {
		t.Fatalf("attempt = %d，期望 1（续租不应改变投递次数）", got[0].attempt)
	}
	t.Logf("用例 7 通过：慢 handler 持有 8s 跨过 5s 不可见期，续租挡住了重投")
}
