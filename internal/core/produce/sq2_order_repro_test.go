// SQ2 复现用例：集群档同队列提案不保序（排查第 1 步的红色反馈回路）。
//
// 疑点：Append 在持有 qs.mu 时分配 offset 并调 replication.Cluster.ApplyAsync，
// 后者每条提案起一个新 goroutine 去 rn.Propose——真正进入 raft 的先后与
// offset 分配顺序脱钩，同队列 off=N 与 off=N+1 的日志顺序可能颠倒。
//
// 本文件只用真实组件（单 voter 的 cluster.Manager + replication.Cluster +
// produce.Producer + meta.Meta），不加任何测试专用入口；观测点是 Manager 的
// OnApplied 装配钩子（每条已 apply 条目的批次 repr，按 apply 顺序触发）。
//
// 判据：
//   - 第一层（乱序本身，TestSQ2ClusterSameQueueApplyOrder）：从每条已 apply
//     条目里解出目标队列的 alloc/ 计数器值，按 apply 顺序必须严格递增；
//     出现逆序即红（bug 存在 → 测试失败）。
//   - 第二层（后果：计数器倒退，同一测试内逐轮检查）：每轮并发写全部
//     应答成功后，盘上 alloc/ 必须等于「已应答最大 offset + 1」；小于即
//     计数器倒退（切主后 InvalidateCounters 重读会重复分配 offset）。
//
// 竞态是概率性的：参数（并发、每轮条数、轮数）见常量，失败信息里带
// 首个逆序对的具体 offset 与触发轮次。
package produce

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/batchrepr"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

const (
	sq2Topic        = "sq2order"
	sq2Writers      = 64   // 并发写 goroutine 数
	sq2PerRound     = 4000 // 每轮写入条数（全部落同一队列）
	sq2Rounds       = 50   // 最多轮数；第一层或第二层任一变红即停
	sq2MaxProcs     = 32   // 拉高 GOMAXPROCS 放大调度交错
	sq2BudgetPerRun = 90 * time.Second
)

// sq2Recorder 按 apply 顺序记录目标队列 alloc/ 值（OnApplied 在 Ready 循环
// 内同步触发，同组严格按条目序；这里只做轻量解析与追加）。
type sq2Recorder struct {
	allocKey []byte
	mu       sync.Mutex
	seq      []uint64
}

func (r *sq2Recorder) onApplied(_ uint32, repr []byte) {
	rd := batchrepr.Read(repr)
	if rd == nil {
		return
	}
	for {
		_, ukey, val, ok, err := rd.Next()
		if err != nil || !ok {
			return
		}
		if bytes.Equal(ukey, r.allocKey) && len(val) == 8 {
			v := store.GetU64(val)
			r.mu.Lock()
			r.seq = append(r.seq, v)
			r.mu.Unlock()
		}
	}
}

func (r *sq2Recorder) snapshot() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.seq...)
}

// sq2Stack 起单节点（单 voter）集群并装配真实 produce 栈。
func sq2Stack(t *testing.T, rec *sq2Recorder) (*Producer, *store.Store) {
	t.Helper()
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
		OnApplied:  rec.onApplied,
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
	if _, err := mt.CreateTopic(context.Background(), sq2Topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	return New(cl, cl, st, mt, lg), st
}

func TestSQ2ClusterSameQueueApplyOrder(t *testing.T) {
	prev := runtime.GOMAXPROCS(sq2MaxProcs)
	defer runtime.GOMAXPROCS(prev)

	rec := &sq2Recorder{allocKey: store.AllocKey(sq2Topic, 0)}
	p, st := sq2Stack(t, rec)

	start := time.Now()
	checked := 0 // rec.seq 中已检查到的位置
	total, rounds := 0, 0
	l1Rounds, l1Inv, l2Rounds := 0, 0, 0
	var l1First, l2First string
	for round := 1; round <= sq2Rounds; round++ {
		if time.Since(start) > sq2BudgetPerRun {
			break
		}
		var (
			wg     sync.WaitGroup
			mu     sync.Mutex
			maxOff uint64
			anyOK  bool
			errs   []error
		)
		per := sq2PerRound / sq2Writers
		for w := 0; w < sq2Writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < per; i++ {
					m, err := p.Append(context.Background(), &core.Message{Topic: sq2Topic, Body: []byte("x")})
					mu.Lock()
					if err != nil {
						errs = append(errs, err)
					} else {
						if m.QueueID != 0 {
							errs = append(errs, fmt.Errorf("消息落到队列 %d，期望 0", m.QueueID))
						}
						if !anyOK || m.Offset > maxOff {
							maxOff = m.Offset
						}
						anyOK = true
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		total += per * sq2Writers
		if len(errs) > 0 {
			t.Fatalf("第 %d 轮 Append 失败 %d 次，首个: %v", round, len(errs), errs[0])
		}

		// 第一层：apply 顺序下 alloc/ 值严格递增
		seq := rec.snapshot()
		inversions := 0
		var firstA, firstB uint64
		firstIdx := -1
		for i := max(checked, 1); i < len(seq); i++ {
			if seq[i] <= seq[i-1] {
				if firstIdx < 0 {
					firstIdx, firstA, firstB = i, seq[i-1], seq[i]
				}
				inversions++
			}
		}
		checked = len(seq)

		// 第二层：本轮全部应答后，盘上 alloc/ == 已应答最大 offset + 1
		raw, ok, err := st.Get(store.AllocKey(sq2Topic, 0))
		if err != nil || !ok {
			t.Fatalf("第 %d 轮读 alloc/ 失败: ok=%v err=%v", round, ok, err)
		}
		alloc := store.GetU64(raw)

		if firstIdx >= 0 {
			l1Rounds++
			l1Inv += inversions
			if l1First == "" {
				l1First = fmt.Sprintf("第 %d 轮（累计写入 %d 条，用时 %v）apply 序第 %d 项: alloc %d 之后 apply 了 alloc %d，即 offset %d 先于 offset %d 进入 raft 日志",
					round, total, time.Since(start), firstIdx, firstA, firstB, firstA-1, firstB-1)
			}
		}
		if alloc != maxOff+1 {
			l2Rounds++
			if l2First == "" {
				l2First = fmt.Sprintf("第 %d 轮（累计写入 %d 条，用时 %v）全部应答后盘上 alloc/=%d，已应答最大 offset=%d，期望 alloc=%d（倒退 %d）",
					round, total, time.Since(start), alloc, maxOff, maxOff+1, int64(maxOff+1)-int64(alloc))
			}
		}
		rounds = round
		if l1First != "" && l2First != "" {
			break
		}
	}
	t.Logf("参数: writers=%d perRound=%d GOMAXPROCS=%d；跑了 %d 轮，累计写入 %d 条，用时 %v；第一层红 %d 轮（逆序共 %d 处），第二层红 %d 轮",
		sq2Writers, sq2PerRound, sq2MaxProcs, rounds, total, time.Since(start), l1Rounds, l1Inv, l2Rounds)
	if l1First != "" {
		t.Errorf("第一层红（同队列提案 apply 顺序逆序）：首个 %s", l1First)
	}
	if l2First != "" {
		t.Errorf("第二层红（alloc/ 计数器倒退）：首个 %s", l2First)
	}
}
