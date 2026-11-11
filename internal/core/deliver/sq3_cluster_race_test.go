// Package deliver —— SQ3 复现回路（红色反馈回路，排查第 1 步）。
//
// 复现对象（卡 SQ3）：集群档消费在提案 apply 前释放队列锁。
// receiveOnce 持队列锁组批次（cursor 与 inflight 都从 pebble 读）后调
// rep.ApplyAsync，随即解锁、锁外 Wait。单机档 ApplyAsync 返回时写入已进
// memtable，下一个持锁者读得到；集群档 replication.Cluster.ApplyAsync 只起
// goroutine 提案，写入要等 raft commit + 本节点 apply 才进 pebble——下一个
// 持锁者可能读到旧 cursor / 看不到在途 inflight。
//
// 判据：
//   - 用例 A（普通消息重复投递）：单队列预写 N 条普通消息，同一消费组 C 个
//     goroutine 并发 Receive（不 Ack，不可见时长 10min，远长于测试时长）。
//     每个 offset 以 DeliveryAttempt==1 被投出的次数必须恰好为 1；>1 即红。
//     重复投递还要求发生在首投后 invisible 到期之前（排除合法过期重投）。
//     任何 DeliveryAttempt!=1 的投递在本场景下都不合法（无过期），也判红。
//     收尾检查丢失：成功写入的 offset 必须全部至少被投出一次。
//   - 用例 B（顺序锁失效）：每轮新建一个单队列 topic，写入若干同
//     MessageGroup 的顺序消息，C 个 goroutine 同时 Receive，不 Ack。
//     不可见时长 10min、无 Ack，整轮至多应投出 1 次顺序消息（同一队列在外
//     未确认的顺序投递至多 1 份）；>1 即红。日志另计「越过队头」轮数
//     （投出 >1 个不同 offset），区分「队头被多个消费者同时持有」与
//     「队头未确认就投出后继」两种形态。
//   - 对照：同一逻辑以单机档（replication.Standalone）运行，应为绿。
//
// 装配：真实 cluster.Manager（单 voter，自选举）+ replication.Cluster +
// meta/produce/deliver，按 cmd/sq/main.go 的集群档装配序与钩子接线；
// 不使用任何假 Replicator，不改生产代码。
package deliver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

const (
	sq3Invisible = 10 * time.Minute
	sq3Consumers = 8
)

// sq3Stack 被测栈：produce + deliver（单机档或集群档）。
type sq3Stack struct {
	pr *produce.Producer
	dl *Deliverer
}

// quietLogger 压低日志噪音（集群/投递 Debug 日志在高并发下会刷屏）。
func sq3Logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(nopWriter{}, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

func newSQ3Standalone(t *testing.T, opts ...Option) *sq3Stack {
	t.Helper()
	lg := sq3Logger()
	st, err := store.Open(t.TempDir(), false, lg)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	mt, err := meta.New(rep, rt, st, true, 1, 16, lg)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(rep, rt, st, mt, lg)
	return &sq3Stack{pr: pr, dl: sq3Deliverer(t, New(rep, rt, st, mt, pr, lg, opts...))}
}

// newSQ3Cluster 单 voter 集群档：装配序与钩子同 cmd/sq/main.go 集群分支。
func newSQ3Cluster(t *testing.T, opts ...Option) *sq3Stack {
	t.Helper()
	lg := sq3Logger()
	st, err := store.Open(t.TempDir(), false, lg)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mtPtr atomic.Pointer[meta.Meta]
		prPtr atomic.Pointer[produce.Producer] // 保留装配形态；SQ1 后无钩子读取
	)
	const dataGroups = 3
	m, err := cluster.NewManager(cluster.Options{
		NodeID:     1,
		Peers:      map[uint64]string{1: ln.Addr().String()},
		Listener:   ln,
		DataGroups: dataGroups,
		Mode:       cluster.AckQuorumMem,
		Store:      st,
		Logger:     lg,
		OnApplied: func(g uint32, repr []byte) {
			if len(repr) == 0 {
				return
			}
			mtv := mtPtr.Load()
			if mtv == nil {
				return
			}
			touches, terr := store.BatchTouchesPrefix(repr, []byte("meta/"))
			if terr != nil || !touches {
				return
			}
			go func() { _ = mtv.Reload() }()
		},
		// SQ1 删除了 InvalidateCounters（计数器缓存改绑可写门 epoch），
		// 原 OnLeaderChange 钩子随之删除；其余装配与判据不变。
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.Start(context.Background())
	// LIFO：先 StopClean 再关 store
	t.Cleanup(func() { st.Close() })
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.StopClean(ctx); err != nil {
			t.Logf("StopClean: %v", err)
		}
		select {
		case <-m.Done():
		case <-time.After(10 * time.Second):
			t.Error("manager 未在 10s 内退出")
		}
	})
	deadline := time.Now().Add(30 * time.Second)
	for g := uint32(0); g <= dataGroups; g++ {
		for {
			if lead, ok := m.Leader(g); ok && lead == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("组 %d 未在 30s 内自选举", g)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	cl := replication.NewCluster(m)
	mt, err := meta.New(cl, cl, st, true, 1, 16, lg)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	mtPtr.Store(mt)
	pr := produce.New(cl, cl, st, mt, lg)
	prPtr.Store(pr)
	return &sq3Stack{pr: pr, dl: sq3Deliverer(t, New(cl, cl, st, mt, pr, lg, opts...))}
}

// sq3Deliverer 起检查点循环（逐次模式为空操作），Cleanup 里 Stop（注册晚于
// 集群/store 的 Cleanup，LIFO 先执行：收尾检查点在 StopClean、store 关闭之前）。
func sq3Deliverer(t *testing.T, d *Deliverer) *Deliverer {
	d.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := d.Stop(ctx); err != nil {
			t.Logf("Deliverer.Stop: %v", err)
		}
	})
	return d
}

// sq3Modes 卡 SQ4.1：两种持久化模式，只改装配，判据一字不改。
var sq3Modes = []struct {
	name string
	opts []Option
}{
	{"checkpoint", []Option{WithCheckpointInterval(time.Second)}},
	{"perop", nil},
}

// sq3Each 对 {standalone, cluster} × {checkpoint, perop} 四种组合各跑一次。
func sq3Each(t *testing.T, run func(t *testing.T, s *sq3Stack)) {
	for _, arch := range []string{"standalone", "cluster"} {
		for _, m := range sq3Modes {
			t.Run(arch+"/"+m.name, func(t *testing.T) {
				var s *sq3Stack
				if arch == "cluster" {
					s = newSQ3Cluster(t, m.opts...)
				} else {
					s = newSQ3Standalone(t, m.opts...)
				}
				run(t, s)
			})
		}
	}
}

type sq3Delivery struct {
	offset  uint64
	attempt int32
	at      time.Time
	worker  int
}

// TestSQ3ConcurrentReceiveDuplicateDelivery 用例 A：普通消息并发取件重复投递。
func TestSQ3ConcurrentReceiveDuplicateDelivery(t *testing.T) {
	sq3Each(t, sq3RunCaseA)
}

func sq3RunCaseA(t *testing.T, s *sq3Stack) {
	const (
		n       = 2000
		maxMsgs = 4
	)
	ctx := context.Background()
	topic, group := "sq3-a", "sq3-ga"
	written := make(map[uint64]bool, n)
	for i := 0; i < n; i++ {
		res, err := s.pr.Append(ctx, &core.Message{Topic: topic, Body: []byte(fmt.Sprintf("m%d", i))})
		if err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
		if res.QueueID != 0 {
			t.Fatalf("消息落在队列 %d，期望单队列 0", res.QueueID)
		}
		written[res.Offset] = true
	}

	var (
		mu    sync.Mutex
		dels  []sq3Delivery
		start = make(chan struct{})
		wg    sync.WaitGroup
		errMu sync.Mutex
		errs  []error
	)
	stopAt := time.Now().Add(40 * time.Second)
	for w := 0; w < sq3Consumers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			empties := 0
			for time.Now().Before(stopAt) && empties < 20 {
				msgs, err := s.dl.Receive(ctx, group, topic, 0, maxMsgs, sq3Invisible, 0, AllPass)
				if err != nil {
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
					return
				}
				if len(msgs) == 0 {
					empties++
					time.Sleep(5 * time.Millisecond)
					continue
				}
				empties = 0
				now := time.Now()
				mu.Lock()
				for _, m := range msgs {
					dels = append(dels, sq3Delivery{offset: m.Offset, attempt: m.DeliveryAttempt, at: now, worker: w})
				}
				mu.Unlock()
			}
		}(w)
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t0)
	for _, e := range errs {
		t.Errorf("Receive 出错: %v", e)
	}

	first := map[uint64]time.Time{}
	count := map[uint64]int{}
	var badAttempt []sq3Delivery
	for _, d := range dels {
		if d.attempt != 1 {
			badAttempt = append(badAttempt, d)
			continue
		}
		count[d.offset]++
		if f, ok := first[d.offset]; !ok || d.at.Before(f) {
			first[d.offset] = d.at
		}
	}
	var dupOffsets []uint64
	extra := 0
	for off, c := range count {
		if c > 1 {
			dupOffsets = append(dupOffsets, off)
			extra += c - 1
		}
	}
	sort.Slice(dupOffsets, func(i, j int) bool { return dupOffsets[i] < dupOffsets[j] })
	// 合法重投排除：所有投递都在首投后 invisible 内（整轮耗时 << 10min）
	var afterExpiry int
	for _, d := range dels {
		if f, ok := first[d.offset]; ok && d.at.Sub(f) >= sq3Invisible {
			afterExpiry++
		}
	}
	var lost []uint64
	for off := range written {
		if count[off] == 0 {
			lost = append(lost, off)
		}
	}
	sort.Slice(lost, func(i, j int) bool { return lost[i] < lost[j] })

	t.Logf("用例A: N=%d C=%d maxMsgs=%d 耗时=%v 投递总数=%d distinct=%d 重复offset数=%d(%.2f%%) 多余投递=%d attempt!=1=%d 过期后投递=%d 丢失=%d",
		n, sq3Consumers, maxMsgs, elapsed.Round(time.Millisecond), len(dels), len(count),
		len(dupOffsets), 100*float64(len(dupOffsets))/float64(n), extra, len(badAttempt), afterExpiry, len(lost))

	if len(lost) > 0 {
		t.Errorf("【消息丢失】%d 条成功写入的消息从未被投递，前若干 offset: %v", len(lost), head(lost, 20))
	}
	if len(badAttempt) > 0 {
		t.Errorf("出现 DeliveryAttempt!=1 的投递 %d 次（无过期场景下不合法），首条: off=%d attempt=%d",
			len(badAttempt), badAttempt[0].offset, badAttempt[0].attempt)
	}
	if len(dupOffsets) > 0 {
		var sample []string
		for _, off := range head(dupOffsets, 10) {
			var ws []int
			for _, d := range dels {
				if d.offset == off && d.attempt == 1 {
					ws = append(ws, d.worker)
				}
			}
			sample = append(sample, fmt.Sprintf("off=%d 次数=%d workers=%v 距首投=%v",
				off, count[off], ws, lastGap(dels, off, first[off])))
		}
		t.Errorf("SQ3 重复投递：%d/%d 个 offset 以 DeliveryAttempt=1 被投出多次（多余 %d 次，均在 invisible=%v 到期前）；样例: %v",
			len(dupOffsets), n, extra, sq3Invisible, sample)
	}
}

func lastGap(dels []sq3Delivery, off uint64, first time.Time) time.Duration {
	var g time.Duration
	for _, d := range dels {
		if d.offset == off && d.at.Sub(first) > g {
			g = d.at.Sub(first)
		}
	}
	return g
}

func head(s []uint64, n int) []uint64 {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// TestSQ3ConcurrentReceiveOrderedLock 用例 B：顺序锁在并发取件下失效。
func TestSQ3ConcurrentReceiveOrderedLock(t *testing.T) {
	sq3Each(t, sq3RunCaseB)
}

func sq3RunCaseB(t *testing.T, s *sq3Stack) {
	const (
		rounds      = 60
		perRound    = 8
		triesPerCon = 5
	)
	ctx := context.Background()
	group := "sq3-gb"
	redRounds := 0
	distinctRounds := 0 // 同轮投出 >1 个不同 offset（真正越过队头的 FIFO 破坏）
	var samples, distinctSamples []string
	totalLost := 0
	for r := 0; r < rounds; r++ {
		topic := fmt.Sprintf("sq3-b-%d", r)
		for i := 0; i < perRound; i++ {
			res, err := s.pr.Append(ctx, &core.Message{Topic: topic, Body: []byte(fmt.Sprintf("o%d", i)), MessageGroup: "mg"})
			if err != nil {
				t.Fatalf("Append: %v", err)
			}
			if res.QueueID != 0 {
				t.Fatalf("顺序消息落在队列 %d，期望 0", res.QueueID)
			}
		}
		// 预热：EnsureGroup 首次可能走提案，放到并发窗口之外
		if _, err := s.dl.mt.EnsureGroup(ctx, group); err != nil {
			t.Fatal(err)
		}
		var (
			mu    sync.Mutex
			got   []sq3Delivery
			wg    sync.WaitGroup
			start = make(chan struct{})
		)
		for w := 0; w < sq3Consumers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for k := 0; k < triesPerCon; k++ {
					msgs, err := s.dl.Receive(ctx, group, topic, 0, 1, sq3Invisible, 0, AllPass)
					if err != nil {
						t.Errorf("Receive: %v", err)
						return
					}
					now := time.Now()
					mu.Lock()
					for _, m := range msgs {
						got = append(got, sq3Delivery{offset: m.Offset, attempt: m.DeliveryAttempt, at: now, worker: w})
					}
					mu.Unlock()
				}
			}(w)
		}
		close(start)
		wg.Wait()
		offs := map[uint64]bool{}
		for _, d := range got {
			offs[d.offset] = true
		}
		var desc []string
		for _, d := range got {
			desc = append(desc, fmt.Sprintf("off=%d att=%d w=%d", d.offset, d.attempt, d.worker))
		}
		if len(offs) > 1 {
			distinctRounds++
			if len(distinctSamples) < 3 {
				distinctSamples = append(distinctSamples, fmt.Sprintf("round=%d %v", r, desc))
			}
		}
		if len(got) > 1 {
			redRounds++
			if len(samples) < 3 {
				samples = append(samples, fmt.Sprintf("round=%d 在外顺序消息=%d %v", r, len(got), desc))
			}
		}
		if len(got) == 0 {
			// 一条都没投出：首条顺序消息不该卡住（无 inflight 时必须可投）
			totalLost++
		}
	}
	t.Logf("用例B: rounds=%d 每轮顺序消息=%d C=%d 每消费者尝试=%d 红轮数=%d(%.1f%%) 其中越过队头(>1个不同offset)轮数=%d 零投递轮=%d",
		rounds, perRound, sq3Consumers, triesPerCon, redRounds, 100*float64(redRounds)/float64(rounds), distinctRounds, totalLost)
	if totalLost > 0 {
		t.Errorf("%d 轮无任何顺序消息被投递（队头无 inflight 却投不出）", totalLost)
	}
	if redRounds > 0 {
		t.Errorf("SQ3 顺序锁失效：%d/%d 轮在无 Ack、未过期时同一队列同时在外 >1 条顺序消息（其中 %d 轮越过队头投出后继 offset）；样例: %v；越过队头样例: %v",
			redRounds, rounds, distinctRounds, samples, distinctSamples)
	}
}
