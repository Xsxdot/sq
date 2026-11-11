// Package deliver —— 卡 SQ4.2 唤醒的不变式单测与故事级检查 S2。
//
// 职责（测试文件）：
//   - W1：先订阅再取件，不丢唤醒（关掉兜底 tick，经轮次钩子在「空取件之后、
//     等待之前」确定性地写入 / ack 释放顺序锁，都必须在 500ms 内唤醒 Receive）
//   - W2（deliver 侧）：死信写入只唤醒死信 topic 的队列 0，不唤醒源队列的其他组
//   - W3：Ack 一类每个方法成功后只唤醒同 (组, topic, 队列)，两种持久化模式各一遍；
//     失败路径不 panic
//   - S2：16 队列 topic 只往队列 0 写，其余队列的等待者「被唤醒」轮数为 0；
//     某组在队列 0 ack 不唤醒同队列的其他组
//
// 边界：观测量只经唤醒表信号与测试轮次钩子（roundHook），不改生产判据。
package deliver

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

// roundCounter S1 用：投出了消息的取件轮数，及其中「被唤醒」起因的轮数。
type roundCounter struct {
	mu                          sync.Mutex
	woke, delivered, tick, frst int
}

func (r *roundCounter) observe(_, _ string, _ uint32, cause roundCause, n int) {
	if n == 0 {
		return
	}
	r.mu.Lock()
	r.delivered++
	switch cause {
	case roundWoken:
		r.woke++
	case roundTick:
		r.tick++
	default:
		r.frst++
	}
	r.mu.Unlock()
}

// causes 投出轮按起因的分布（首轮 / tick），只用于诊断日志。
func (r *roundCounter) causes() (first, tick int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.frst, r.tick
}

func (r *roundCounter) snapshot() (woke, delivered int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.woke, r.delivered
}

// roundTally S2 用：按 (组, 队列) 统计「被唤醒」起因的取件轮数（含空轮）。
type roundTally struct {
	mu sync.Mutex
	m  map[string]int
}

func tallyKey(group string, q uint32) string { return fmt.Sprintf("%s/%d", group, q) }

func (r *roundTally) observe(group, _ string, q uint32, cause roundCause, _ int) {
	if cause != roundWoken {
		return
	}
	r.mu.Lock()
	r.m[tallyKey(group, q)]++
	r.mu.Unlock()
}

func (r *roundTally) get(group string, q uint32) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[tallyKey(group, q)]
}

// wakeFixture 单机栈（不 fsync），可指定组默认 maxAttempts 与持久化模式。
type wakeFixture struct {
	st *store.Store
	mt *meta.Meta
	pr *produce.Producer
	dl *Deliverer
}

func newWakeFixture(t *testing.T, maxAttempts int32, opts ...Option) *wakeFixture {
	t.Helper()
	lg := sq3Logger()
	st, err := store.Open(t.TempDir(), false, lg)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	mt, err := meta.New(rep, rt, st, true, 1, maxAttempts, lg)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(rep, rt, st, mt, lg)
	return &wakeFixture{st: st, mt: mt, pr: pr, dl: sq3Deliverer(t, New(rep, rt, st, mt, pr, lg, opts...))}
}

// woken 非阻塞判断信号是否已 close。
func woken(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// withPollTick 测试期间替换兜底 tick。
func withPollTick(t *testing.T, d time.Duration) {
	old := pollTick
	pollTick = d
	t.Cleanup(func() { pollTick = old })
}

// TestWakeNoLostWakeup W1：关掉兜底 tick，把「取件为空之后、开始等待之前」
// 这个竞态窗口变成每轮必经的路径——轮次钩子（Receive 里紧跟 receiveOnce 的
// 第一条语句）在首轮空取件后同步触发唤醒源（Append / Ack 队头），等待者必须
// 在 500ms 内拿到消息。订阅若挪到 receiveOnce（及钩子）之后，唤醒先于订阅
// 发出即丢失，等满 2s，每轮必红。500 轮各一遍。
func TestWakeNoLostWakeup(t *testing.T) {
	withPollTick(t, time.Hour)
	const iters = 500
	ctx := context.Background()

	// run 每轮起一个 Receive(wait=2s)，钩子在其首轮空取件后同步调 trigger 一次
	run := func(t *testing.T, f *wakeFixture, topic string, trigger func() error, check func(i int, ms []*core.Message)) {
		for i := 0; i < iters; i++ {
			fired := false
			f.dl.roundHook = func(_, _ string, _ uint32, cause roundCause, n int) {
				if cause != roundFirst || n != 0 || fired {
					return
				}
				fired = true
				if err := trigger(); err != nil {
					t.Errorf("第 %d 轮触发唤醒源: %v", i, err)
				}
			}
			start := time.Now()
			ms, err := f.dl.Receive(ctx, "g", topic, 0, 1, 30*time.Second, 2*time.Second, AllPass)
			f.dl.roundHook = nil
			if err != nil {
				t.Fatalf("第 %d 轮 Receive: %v", i, err)
			}
			if !fired {
				t.Fatalf("第 %d 轮：首轮未空取件，竞态窗口未被走到（夹具前置不成立）", i)
			}
			if len(ms) != 1 || time.Since(start) > 500*time.Millisecond {
				t.Fatalf("第 %d 轮：唤醒源已触发，%v 后才返回 %d 条（丢唤醒）", i, time.Since(start), len(ms))
			}
			check(i, ms)
		}
	}

	t.Run("append", func(t *testing.T) {
		f := newWakeFixture(t, 16)
		run(t, f, "w1a", func() error {
			_, err := f.pr.Append(ctx, &core.Message{Topic: "w1a", Body: []byte("x")})
			return err
		}, func(i int, ms []*core.Message) {
			if ok, err := f.dl.Ack(ctx, "g", "w1a", 0, ms[0].Offset, ms[0].DeliveryAttempt); err != nil || !ok {
				t.Fatalf("第 %d 轮 Ack: ok=%v err=%v", i, ok, err)
			}
		})
	})

	t.Run("ordered-ack", func(t *testing.T) {
		f := newWakeFixture(t, 16)
		for i := 0; i < iters+1; i++ {
			if _, err := f.pr.Append(ctx, &core.Message{Topic: "w1o", Body: []byte("x"), MessageGroup: "mg"}); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		head, err := f.dl.Receive(ctx, "g", "w1o", 0, 1, 30*time.Second, 0, AllPass)
		if err != nil || len(head) != 1 {
			t.Fatalf("取队头: %v %d", err, len(head))
		}
		// 队头在外 → 首轮被顺序锁拦下空取件；钩子里 ack 队头释放顺序锁
		run(t, f, "w1o", func() error {
			ok, err := f.dl.Ack(ctx, "g", "w1o", 0, head[0].Offset, head[0].DeliveryAttempt)
			if err == nil && !ok {
				err = fmt.Errorf("ack 队头落空")
			}
			return err
		}, func(i int, ms []*core.Message) {
			if ms[0].Offset != head[0].Offset+1 {
				t.Fatalf("第 %d 轮：期望 offset %d，得 %d", i, head[0].Offset+1, ms[0].Offset)
			}
			head = ms
		})
	})
}

// TestWakeDLQWritesWakeDLQQueueOnly W2（deliver 侧）：显式转死信唤醒死信 topic
// 队列 0 的等待者；源队列上其他组不被唤醒。
func TestWakeDLQWritesWakeDLQQueueOnly(t *testing.T) {
	ctx := context.Background()
	f := newWakeFixture(t, 16)
	if _, err := f.pr.Append(ctx, &core.Message{Topic: "w2", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	ms, err := f.dl.Receive(ctx, "g", "w2", 0, 1, 30*time.Second, 0, AllPass)
	if err != nil || len(ms) != 1 {
		t.Fatalf("Receive: %v %d", err, len(ms))
	}
	dlq := meta.DLQTopicName("g")
	dlqCh := f.dl.wk.subscribe("dlq-consumer", dlq, 0)
	srcOther := f.dl.wk.subscribe("g2", "w2", 0)
	if ok, err := f.dl.ForwardToDLQ(ctx, "g", "w2", 0, ms[0].Offset, ms[0].DeliveryAttempt); err != nil || !ok {
		t.Fatalf("ForwardToDLQ: ok=%v err=%v", ok, err)
	}
	if !woken(dlqCh) {
		t.Fatal("死信写入未唤醒死信 topic 队列 0 的等待者")
	}
	if woken(srcOther) {
		t.Fatal("死信写入唤醒了源队列上的其他组")
	}
}

// TestWakeStateChangeScope W3：每个状态变化方法成功后，同 (组, topic, 队列)
// 的等待者被唤醒；别的组、别的队列不被唤醒。两种持久化模式各一遍。
func TestWakeStateChangeScope(t *testing.T) {
	for _, mode := range sq3Modes {
		t.Run(mode.name, func(t *testing.T) { wakeScopeCases(t, mode.opts) })
	}
}

func wakeScopeCases(t *testing.T, opts []Option) {
	ctx := context.Background()
	const topic = "w3"
	// setup：建 2 队列 topic，向队列 0 写一条并以组 g 取出，返回其句柄
	setup := func(t *testing.T, maxAttempts int32) (*wakeFixture, *core.Message) {
		f := newWakeFixture(t, maxAttempts, opts...)
		if _, err := f.mt.CreateTopic(ctx, topic, 2); err != nil {
			t.Fatal(err)
		}
		var m *core.Message
		for m == nil || m.QueueID != 0 {
			var err error
			if m, err = f.pr.Append(ctx, &core.Message{Topic: topic, Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
		}
		ms, err := f.dl.Receive(ctx, "g", topic, 0, 1, 30*time.Second, 0, AllPass)
		if err != nil || len(ms) != 1 {
			t.Fatalf("Receive: %v %d", err, len(ms))
		}
		return f, ms[0]
	}
	// check：订阅三路等待者后执行 op，断言只有同 (g, topic, 0) 被唤醒
	check := func(t *testing.T, f *wakeFixture, op func() error) {
		same := f.dl.wk.subscribe("g", topic, 0)
		otherGroup := f.dl.wk.subscribe("g2", topic, 0)
		otherQueue := f.dl.wk.subscribe("g", topic, 1)
		if err := op(); err != nil {
			t.Fatalf("op: %v", err)
		}
		if !woken(same) {
			t.Fatal("成功后未唤醒同 (组, topic, 队列) 的等待者")
		}
		if woken(otherGroup) || woken(otherQueue) {
			t.Fatalf("越界唤醒：其他组=%v 其他队列=%v", woken(otherGroup), woken(otherQueue))
		}
	}
	want := func(ok bool, err error) error {
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("返回 ok=false")
		}
		return nil
	}

	t.Run("Ack", func(t *testing.T) {
		f, m := setup(t, 16)
		check(t, f, func() error { return want(f.dl.Ack(ctx, "g", topic, 0, m.Offset, m.DeliveryAttempt)) })
	})
	t.Run("AckBatch", func(t *testing.T) {
		f, m := setup(t, 16)
		check(t, f, func() error {
			rs, err := f.dl.AckBatch(ctx, "g", topic, 0, []AckEntry{{Offset: m.Offset, Attempt: m.DeliveryAttempt}, {Offset: 999, Attempt: 1}})
			if err != nil {
				return err
			}
			return want(rs[0].OK, nil)
		})
	})
	t.Run("ChangeInvisible", func(t *testing.T) {
		f, m := setup(t, 16)
		check(t, f, func() error {
			return want(f.dl.ChangeInvisible(ctx, "g", topic, 0, m.Offset, m.DeliveryAttempt, 0))
		})
	})
	t.Run("ForwardToDLQ", func(t *testing.T) {
		f, m := setup(t, 16)
		check(t, f, func() error { return want(f.dl.ForwardToDLQ(ctx, "g", topic, 0, m.Offset, m.DeliveryAttempt)) })
	})
	t.Run("ReceiveOverLimitToDLQ", func(t *testing.T) {
		f, m := setup(t, 1) // 首投即用尽投递次数
		if ok, err := f.dl.ChangeInvisible(ctx, "g", topic, 0, m.Offset, m.DeliveryAttempt, 0); err != nil || !ok {
			t.Fatalf("置为立即到期: %v %v", ok, err)
		}
		check(t, f, func() error {
			ms, err := f.dl.Receive(ctx, "g", topic, 0, 1, 30*time.Second, 0, AllPass)
			if err == nil && len(ms) != 0 {
				err = fmt.Errorf("超限消息不应再投出，得 %d 条", len(ms))
			}
			return err
		})
	})
	t.Run("ResetCursor", func(t *testing.T) {
		f, _ := setup(t, 16)
		check(t, f, func() error { return f.dl.ResetCursor(ctx, "g", topic, 0, 0) })
	})
	t.Run("ApplyAdminBatch", func(t *testing.T) {
		f, _ := setup(t, 16)
		check(t, f, func() error {
			b := f.st.NewBatch()
			ck := store.CursorKey("g", topic, 0)
			b.DeleteRange(ck, store.PrefixUpperBound(ck))
			ip := store.InflightPrefix("g", topic, 0)
			b.DeleteRange(ip, store.PrefixUpperBound(ip))
			return f.dl.ApplyAdminBatch(ctx, 0, b)
		})
	})
	t.Run("FailurePathsNoPanic", func(t *testing.T) {
		f, m := setup(t, 16)
		stale := m.DeliveryAttempt + 1
		if ok, err := f.dl.Ack(ctx, "g", topic, 0, m.Offset, stale); ok || err != nil {
			t.Fatalf("陈旧 Ack: %v %v", ok, err)
		}
		if ok, err := f.dl.ChangeInvisible(ctx, "g", topic, 0, 12345, 1, 0); ok || err != nil {
			t.Fatalf("未命中 ChangeInvisible: %v %v", ok, err)
		}
		if ok, err := f.dl.ForwardToDLQ(ctx, "g", topic, 0, m.Offset, stale); ok || err != nil {
			t.Fatalf("陈旧 ForwardToDLQ: %v %v", ok, err)
		}
	})
}

// sq42GroupForQueue 找一个 fnv32a % queues == want 的 MessageGroup 串（与
// produce 的选队公式一致）。
func sq42GroupForQueue(t *testing.T, queues, want uint32) string {
	for i := 0; i < 100000; i++ {
		s := fmt.Sprintf("mg-%d", i)
		h := fnv.New32a()
		h.Write([]byte(s))
		if h.Sum32()%queues == want {
			return s
		}
	}
	t.Fatalf("找不到落入队列 %d 的 MessageGroup", want)
	return ""
}

// TestSQ42WakeOnlyTargetQueue S2：一个 topic 16 队列只往队列 0 写，其余 15 个
// 队列的等待者「被唤醒」轮数为 0、队列 0 两组都收到消息；追加段：组 g 在队列 0
// ack，不唤醒同队列的组 g2（计划 P3），正向对照组 g 自己的等待者被唤醒。
func TestSQ42WakeOnlyTargetQueue(t *testing.T) {
	ctx := context.Background()
	const (
		topic  = "sq42-s2"
		queues = 16
		n      = 100
	)
	f := newWakeFixture(t, 16)
	if _, err := f.mt.CreateTopic(ctx, topic, queues); err != nil {
		t.Fatal(err)
	}
	tally := &roundTally{m: map[string]int{}}
	f.dl.roundHook = tally.observe

	type res struct {
		group string
		q     uint32
		ms    []*core.Message
	}
	results := make(chan res, queues+1)
	var wg sync.WaitGroup
	poll := func(group string, q uint32, wait time.Duration) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ms, err := f.dl.Receive(ctx, group, topic, q, 32, 30*time.Second, wait, AllPass)
			if err != nil {
				t.Errorf("Receive %s/%d: %v", group, q, err)
			}
			results <- res{group, q, ms}
		}()
	}
	for q := uint32(1); q < queues; q++ {
		poll("g", q, 3*time.Second)
	}
	poll("g", 0, 3*time.Second)
	poll("g2", 0, 3*time.Second)
	time.Sleep(200 * time.Millisecond) // 等待者全部挂上
	mg := sq42GroupForQueue(t, queues, 0)
	for i := 0; i < n; i++ {
		m, err := f.pr.Append(ctx, &core.Message{Topic: topic, Body: []byte("x"), MessageGroup: mg})
		if err != nil {
			t.Fatal(err)
		}
		if m.QueueID != 0 {
			t.Fatalf("消息落入队列 %d，期望 0", m.QueueID)
		}
	}
	wg.Wait()
	close(results)
	heads := map[string]*core.Message{}
	for r := range results {
		if r.q == 0 {
			if len(r.ms) == 0 {
				t.Fatalf("正向对照失败：组 %s 在队列 0 未收到消息", r.group)
			}
			heads[r.group] = r.ms[0]
		} else if len(r.ms) != 0 {
			t.Fatalf("队列 %d 不应有消息", r.q)
		}
	}
	for q := uint32(1); q < queues; q++ {
		if w := tally.get("g", q); w != 0 {
			t.Fatalf("队列 %d 的等待者被唤醒 %d 轮（只写了队列 0）", q, w)
		}
	}
	t.Logf("[SQ42-S2] 队列 1–15 被唤醒轮数全为 0；队列 0：g=%d g2=%d", tally.get("g", 0), tally.get("g2", 0))

	// 追加段（P3）：两组都持有队头（顺序锁），各挂一个长轮询；g ack 只唤醒 g
	g2Before := tally.get("g2", 0)
	gBefore := tally.get("g", 0)
	var g1Got []*core.Message
	wg.Add(2)
	go func() {
		defer wg.Done()
		ms, err := f.dl.Receive(ctx, "g", topic, 0, 32, 30*time.Second, 2*time.Second, AllPass)
		if err != nil {
			t.Errorf("Receive g: %v", err)
		}
		g1Got = ms
	}()
	go func() {
		defer wg.Done()
		if _, err := f.dl.Receive(ctx, "g2", topic, 0, 32, 30*time.Second, time.Second, AllPass); err != nil {
			t.Errorf("Receive g2: %v", err)
		}
	}()
	time.Sleep(200 * time.Millisecond)
	if ok, err := f.dl.Ack(ctx, "g", topic, 0, heads["g"].Offset, heads["g"].DeliveryAttempt); err != nil || !ok {
		t.Fatalf("Ack g: %v %v", ok, err)
	}
	wg.Wait()
	if len(g1Got) == 0 || tally.get("g", 0) <= gBefore {
		t.Fatalf("正向对照失败：g ack 后其等待者未被唤醒取到后继（得 %d 条，被唤醒 %d→%d）",
			len(g1Got), gBefore, tally.get("g", 0))
	}
	if d := tally.get("g2", 0) - g2Before; d != 0 {
		t.Fatalf("g 在队列 0 ack 唤醒了组 g2 %d 轮（P3：只唤醒同组）", d)
	}
	t.Logf("[SQ42-S2] ack 段：g 被唤醒 %d→%d，g2 被唤醒 %d→%d",
		gBefore, tally.get("g", 0), g2Before, tally.get("g2", 0))
}
