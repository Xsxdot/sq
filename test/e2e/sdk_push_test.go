//go:build e2e

// 官方 Go SDK PushConsumer e2e：B13.2 的验证载体。
//
// 职责：
//   - 覆盖 push 消费路径（callback 驱动）的六条真实链路：基础闭环、长轮询唤醒、
//     消费失败重投、超限转 DLQ、FIFO 顺序不破、停机后 inflight 不丢
//   - 实证 settings.go 下发的 fifo=false 是正确终态：重试计数与死信判定都归 broker
//
// 边界：
//   - 不覆盖 LitePushConsumer（依赖未实现的 SyncLiteSubscription）
//   - 不覆盖集群档（本文件全部单机 broker）
//   - 不验证 AutoRenew 续租（sq 侧未实现，已另立 backlog B13.5）
//   - 不做批量 ReceiveBatchSize 的断言（客户端侧观察不到有意义的差别）
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
			c.t.Logf("listener 出口: body=%s 结果=%+v", r.body, res.Type)
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

// startPushConsumer 建 PushConsumer、Start、注册 cleanup，返回消费者与一个
// 幂等的 stop 函数。
//
// 返回 stop 而不是只返回消费者：用例 6 必须在测试中途显式停 A，而 cleanup 里
// 还会再停一次；sync.Once 包住后重复调用是 no-op。
func startPushConsumer(t *testing.T, endpoint, group, topic string, c *pushCollector, opts ...rmq.PushConsumerOption) (rmq.PushConsumer, func()) {
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
	return pc, stop
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

// TestOfficialGoSDKPushBasicLoop 用例 1：QueryAssignment → 长轮询 Receive →
// listener → Ack 整条链路通，且 Ack 真落地（位点推进）。
func TestOfficialGoSDKPushBasicLoop(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-push-basic"
		group = "e2e-push-basic-g"
	)
	bodies := []string{"p-0", "p-1", "p-2", "p-3", "p-4"}
	sendPlain(t, endpoint, topic, bodies...)

	c := newCollector(t)
	_, stopA := startPushConsumer(t, endpoint, group, topic, c)
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
