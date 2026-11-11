// SQ2 复现用例（后果层：漏投）：集群档同队列提案不保序，取件位点越过空洞
// 后跳过的消息永远不会被投递。
//
// 机制：produce.Append 持队列锁分配 offset 后经 replication.Cluster.ApplyAsync
// 每条提案起一个 goroutine 去 rn.Propose，off=N 与 off=N+1 进 raft 的先后
// 不受控。若 N+1 先 apply、N 尚未 apply 时有一次取件，receiveOnce 从 cursor
// 扫描、newCursor = m.Offset+1，位点越过 N；N 随后落地但 cursor 已在其后。
//
// 本文件只用真实组件（单 voter 的 cluster.Manager + replication.Cluster +
// meta/produce/deliver），不加任何测试专用入口。
//
// 判据：写入与取件并发进行；写入全部应答后把队列取尽，每条应答成功的
// offset 都必须至少被投递一次。有成功写入却从未投递的 offset 即红（bug
// 存在 → 测试失败），失败信息列出漏投的具体 offset。
package deliver

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
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
	sq2Topic    = "sq2skip"
	sq2Group    = "sq2g"
	sq2Writers  = 64
	sq2Total    = 20000 // 每轮写入条数（全部落同一队列）
	sq2Rounds   = 30    // 最多轮数；变红即停
	sq2Budget   = 90 * time.Second
	sq2MaxProcs = 32
)

func TestSQ2ClusterReceiveSkipsLateAppliedOffset(t *testing.T) {
	prev := runtime.GOMAXPROCS(sq2MaxProcs)
	defer runtime.GOMAXPROCS(prev)

	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(t.TempDir(), false, lg)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m, err := cluster.NewManager(cluster.Options{
		NodeID:     1,
		Peers:      map[uint64]string{1: ln.Addr().String()},
		Listener:   ln,
		DataGroups: 1,
		Mode:       cluster.AckQuorumMem,
		Store:      st,
		Logger:     lg,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.StopClean(ctx); err != nil {
			t.Logf("StopClean: %v", err)
		}
		_ = st.Close()
	})
	deadline := time.Now().Add(20 * time.Second)
	for g := uint32(0); g < m.Groups(); g++ {
		for !m.IsLeader(g) {
			if time.Now().After(deadline) {
				t.Fatalf("组 %d 未在时限内自选为 leader", g)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	cl := replication.NewCluster(m)
	mt, err := meta.New(cl, cl, st, true, 1, 16, lg)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(cl, cl, st, mt, lg)
	dl := New(cl, cl, st, mt, pr, lg)

	start := time.Now()
	total := 0
	for round := 1; round <= sq2Rounds && time.Since(start) < sq2Budget; round++ {
		topic := fmt.Sprintf("%s-r%d", sq2Topic, round)
		if _, err := mt.CreateTopic(context.Background(), topic, 1); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		n, missing, cursor, recv := sq2SkipRound(t, pr, dl, st, topic)
		total += n
		if len(missing) > 0 {
			show := missing
			if len(show) > 20 {
				show = show[:20]
			}
			t.Logf("参数: writers=%d perRound=%d consumers=1 GOMAXPROCS=%d；第 %d 轮变红，累计写入 %d 条，用时 %v",
				sq2Writers, sq2Total, sq2MaxProcs, round, total, time.Since(start))
			t.Errorf("漏投红：第 %d 轮 %d 条应答成功的消息从未被投递（本轮 Receive %d 次，最终 cursor=%d 已越过它们），前 %d 个 offset: %v",
				round, len(missing), recv, cursor, len(show), show)
			return
		}
	}
	t.Logf("未复现：writers=%d perRound=%d GOMAXPROCS=%d，累计写入 %d 条，用时 %v",
		sq2Writers, sq2Total, sq2MaxProcs, total, time.Since(start))
}

// sq2SkipRound 一轮：并发写 sq2Total 条到 topic 的队列 0，同时一个消费者持续
// Receive+Ack；写入全部应答后取尽队列，返回成功写入数、从未投递的 offset、
// 最终 cursor 与 Receive 调用次数。
func sq2SkipRound(t *testing.T, pr *produce.Producer, dl *Deliverer, st *store.Store, topic string) (int, []uint64, uint64, int64) {
	t.Helper()
	ctx := context.Background()
	var (
		acked   sync.Map // offset -> struct{}：Append 成功应答的 offset
		got     sync.Map // offset -> struct{}：被投递过的 offset
		writing atomic.Bool
		recvN   atomic.Int64
	)
	writing.Store(true)

	// 消费者：写入期间不间断取件 + ack（不可见时长取长值，避免重投干扰判据）
	consumerErr := make(chan error, 1)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		empty := 0
		for {
			msgs, err := dl.Receive(ctx, sq2Group, topic, 0, 32, 10*time.Minute, 0, AllPass)
			if err != nil {
				consumerErr <- err
				return
			}
			recvN.Add(1)
			for _, msg := range msgs {
				got.Store(msg.Offset, struct{}{})
				if _, err := dl.Ack(ctx, sq2Group, topic, 0, msg.Offset, msg.DeliveryAttempt); err != nil {
					consumerErr <- err
					return
				}
			}
			if len(msgs) == 0 && !writing.Load() {
				empty++
				if empty >= 5 { // 写入已全部应答且连续 5 次取空：队列已取尽
					return
				}
				time.Sleep(time.Millisecond)
			} else if len(msgs) > 0 {
				empty = 0
			}
		}
	}()

	var wg sync.WaitGroup
	var appendErrs atomic.Int64
	per := sq2Total / sq2Writers
	for w := 0; w < sq2Writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				msg, err := pr.Append(ctx, &core.Message{Topic: topic, Body: []byte("x")})
				if err != nil {
					appendErrs.Add(1)
					continue
				}
				acked.Store(msg.Offset, struct{}{})
			}
		}()
	}
	wg.Wait()
	writing.Store(false)
	select {
	case <-consumerDone:
	case <-time.After(60 * time.Second):
		t.Fatal("消费者 60s 内未取尽队列")
	}
	select {
	case err := <-consumerErr:
		t.Fatalf("消费者出错: %v", err)
	default:
	}
	if n := appendErrs.Load(); n > 0 {
		t.Fatalf("Append 失败 %d 次", n)
	}

	var missing []uint64
	nAcked := 0
	acked.Range(func(k, _ any) bool {
		nAcked++
		if _, ok := got.Load(k); !ok {
			missing = append(missing, k.(uint64))
		}
		return true
	})
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	cursor := uint64(0)
	if raw, ok, _ := st.Get(store.CursorKey(sq2Group, topic, 0)); ok && len(raw) == 8 {
		cursor = store.GetU64(raw)
	}
	return nAcked, missing, cursor, recvN.Load()
}
