// txn 状态机测试（spec §10 核心单测第 2 项）。
package txn

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/deliver"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

type fixture struct {
	st  *store.Store
	pr  *produce.Producer
	dl  *deliver.Deliverer
	mt  *meta.Meta
	mgr *Manager
	rep replication.Replicator
	rt  replication.Router
}

func newFixture(t *testing.T, interval time.Duration, maxChecks int) *fixture {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	dl := deliver.New(rep, rt, st, mt, pr, slog.Default())
	return &fixture{st: st, pr: pr, dl: dl, mt: mt, mgr: New(rep, rt, st, pr, mt, interval, maxChecks, slog.Default()), rep: rep, rt: rt}
}

// msgCount 统计 msg/ 区消息条数（两段式重放用例断言目标队列条数用）。
func (f *fixture) msgCount(t *testing.T) int {
	t.Helper()
	n := 0
	pfx := []byte("msg/")
	if err := f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

func (f *fixture) halfCount(t *testing.T) int {
	t.Helper()
	n := 0
	pfx := []byte(store.HalfPrefix)
	if err := f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func msg(topic string) *core.Message {
	return &core.Message{Topic: topic, Body: []byte("hello"), Transactional: true}
}

func TestStageWritesHalfAndIdxAtomically(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	m, txID, err := f.mgr.Stage(context.Background(), msg("t-txn"))
	if err != nil {
		t.Fatal(err)
	}
	if txID == "" || m.ID == "" {
		t.Fatalf("Stage 未生成 txID/msgID: %q %q", txID, m.ID)
	}
	if f.halfCount(t) != 1 {
		t.Fatalf("half 条目数 = %d", f.halfCount(t))
	}
	if _, ok, _ := f.st.Get(store.HalfIdxKey(txID)); !ok {
		t.Fatal("halfidx 未写入")
	}
	// 半消息对消费不可见：msg/ 区必须没有任何条目
	pfx := []byte("msg/")
	n := 0
	f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil })
	if n != 0 {
		t.Fatalf("半消息漏进了 msg/：%d 条", n)
	}
}

func TestCommitMovesToMsgAndCleansHalf(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	_, txID, _ := f.mgr.Stage(context.Background(), msg("t-txn"))
	found, err := f.mgr.End(context.Background(), txID, true)
	if err != nil || !found {
		t.Fatalf("End(commit): found=%v err=%v", found, err)
	}
	if f.halfCount(t) != 0 {
		t.Fatal("commit 后 half 条目残留")
	}
	if _, ok, _ := f.st.Get(store.HalfIdxKey(txID)); ok {
		t.Fatal("commit 后 halfidx 残留")
	}
	// 提交后可正常消费到
	got, err := f.dl.Receive(context.Background(), "g", "t-txn", 0, 1, time.Minute, 0, deliver.AllPass)
	if err != nil || len(got) != 1 {
		t.Fatalf("提交后的消息不可消费: %v %d", err, len(got))
	}
}

func TestRollbackDeletesEverything(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	_, txID, _ := f.mgr.Stage(context.Background(), msg("t-txn"))
	found, err := f.mgr.End(context.Background(), txID, false)
	if err != nil || !found {
		t.Fatalf("End(rollback): found=%v err=%v", found, err)
	}
	if f.halfCount(t) != 0 {
		t.Fatal("rollback 后 half 条目残留")
	}
	got, _ := f.dl.Receive(context.Background(), "g", "t-txn", 0, 1, time.Minute, 0, deliver.AllPass)
	if len(got) != 0 {
		t.Fatal("rollback 的消息被消费到了")
	}
}

func TestEndUnknownTxIDIsIdempotent(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	found, err := f.mgr.End(context.Background(), "NO-SUCH-TX", true)
	if err != nil {
		t.Fatalf("未知 txID 不该报错（幂等）: %v", err)
	}
	if found {
		t.Fatal("未知 txID 不该 found")
	}
}

// nonLeaderRouter 假 Router：报告自己不是任何组 leader（模拟集群档里
// 落在非 meta leader 节点的 EndTransaction）。GroupForQueue/MetaGroup
// 沿用 StandaloneRouter 的恒 MetaGroup 语义——I1 判定只关心 IsLeader。
type nonLeaderRouter struct{ replication.StandaloneRouter }

func (nonLeaderRouter) IsLeader(uint32) bool { return false }

// TestEndUnknownTxOnNonLeaderReturnsErrNotLeader 评审 I1 回归：非 meta
// leader 节点上 End 本地读不到 halfidx 时，不能判幂等成功——本地 FSM
// 可能滞后于多数派（Stage 已提交但未 apply 到本节点），判成功 = 客户端
// 收到 commit OK 但事务实际未决断（假成功）。必须报 ErrNotLeader 让
// rpc 层映射 HA_NOT_AVAILABLE、SDK 重试到 meta leader。
func TestEndUnknownTxOnNonLeaderReturnsErrNotLeader(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	mgr := New(f.rep, nonLeaderRouter{}, f.st, f.pr, f.mt, 30*time.Second, 15, slog.Default())
	_, err := mgr.End(context.Background(), "NO-SUCH-TX", true)
	if err == nil {
		t.Fatal("非 meta leader 上未知 txID 应报 ErrNotLeader（本地读不可决断），得到 nil")
	}
	if !errors.Is(err, replication.ErrNotLeader) {
		t.Fatalf("应包装 replication.ErrNotLeader，得到 %v", err)
	}
}

func TestEndTwiceSecondIsNoop(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	_, txID, _ := f.mgr.Stage(context.Background(), msg("t-txn"))
	f.mgr.End(context.Background(), txID, true)
	found, err := f.mgr.End(context.Background(), txID, true) // SDK 网络重试会走到这里
	if err != nil || found {
		t.Fatalf("重复 commit 应为幂等 no-op: found=%v err=%v", found, err)
	}
	// 消息只有一条，没有被重复投入
	got, _ := f.dl.Receive(context.Background(), "g", "t-txn", 0, 10, time.Minute, 0, deliver.AllPass)
	if len(got) != 1 {
		t.Fatalf("重复 End 导致消息条数 = %d", len(got))
	}
}

// TestTxnCommitRedelivers 两段式提交的崩溃窗口重放：第一段（消息入 msg/）后
// 崩溃，half 两键未删 → 重放 End 再次提交 → 目标队列两条同 ID 消息（重复提交
// = 重复消息，at-least-once 允许）。End 先读后删的幂等判定（halfidx 不存在即
// 已决断）保证重放终止：第二段落盘后，一切再次 EndTransaction 均为幂等 no-op。
func TestTxnCommitRedelivers(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	_, txID, _ := f.mgr.Stage(context.Background(), msg("t-txn"))
	f.mgr.afterAppendHook = func() { panic("simulated crash between phases") }
	func() {
		defer func() { recover() }()
		f.mgr.End(context.Background(), txID, true)
	}()
	// 崩溃后：消息已入队（1 条），half 两键仍在
	if n := f.msgCount(t); n != 1 {
		t.Fatalf("崩溃后消息应已入队: %d", n)
	}
	if f.halfCount(t) != 1 {
		t.Fatalf("崩溃后 half 条目应残留: %d", f.halfCount(t))
	}
	// 重放 End：halfidx 仍在 → 重新提交 → 重复消息，两键清空
	f.mgr.afterAppendHook = nil
	found, err := f.mgr.End(context.Background(), txID, true)
	if err != nil || !found {
		t.Fatalf("重放 End(commit): found=%v err=%v", found, err)
	}
	if n := f.msgCount(t); n != 2 {
		t.Fatalf("重放后目标队列应为 2 条（重复提交 = 重复消息，at-least-once 允许）: %d", n)
	}
	if f.halfCount(t) != 0 {
		t.Fatalf("重放后 half 应清空: %d", f.halfCount(t))
	}
	// 第二段落盘后：再次 End 为幂等 no-op（既有先读后删判定的保护）
	found, err = f.mgr.End(context.Background(), txID, true)
	if err != nil || found {
		t.Fatalf("决断完成后的重复 End 应为幂等 no-op: found=%v err=%v", found, err)
	}
}

// TestEndConcurrentDistinctTx 并发决断互不相同的 txID：分片锁下不同事务
// 并行提交，全部 found=true 且 half 暂存区清零。若分片实现让同 txID 的
// 互斥失效，现有 TestEndTwiceSecondIsNoop 等幂等测试会先暴露。
func TestEndConcurrentDistinctTx(t *testing.T) {
	f := newFixture(t, time.Minute, 15)
	const n = 16
	txIDs := make([]string, n)
	for i := 0; i < n; i++ {
		_, txID, err := f.mgr.Stage(context.Background(), &core.Message{Topic: "tx-cc", Body: []byte("x")})
		if err != nil {
			t.Fatalf("Stage: %v", err)
		}
		txIDs[i] = txID
	}
	var wg sync.WaitGroup
	for _, id := range txIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			found, err := f.mgr.End(context.Background(), id, true)
			if err != nil || !found {
				t.Errorf("End(%s): found=%v err=%v", id, found, err)
			}
		}(id)
	}
	wg.Wait()
	if got := f.halfCount(t); got != 0 {
		t.Fatalf("并发提交后 half 暂存区残留 %d 条", got)
	}
}
