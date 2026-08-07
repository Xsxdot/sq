// txn 状态机测试（spec §10 核心单测第 2 项）。
package txn

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

type fixture struct {
	st  *store.Store
	pr  *produce.Producer
	dl  *deliver.Deliverer
	mgr *Manager
}

func newFixture(t *testing.T, interval time.Duration, maxChecks int) *fixture {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(st, mt, slog.Default())
	dl := deliver.New(st, mt, pr, slog.Default())
	return &fixture{st: st, pr: pr, dl: dl, mgr: New(st, pr, mt, interval, maxChecks, slog.Default())}
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
	m, txID, err := f.mgr.Stage(msg("t-txn"))
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
	_, txID, _ := f.mgr.Stage(msg("t-txn"))
	found, err := f.mgr.End(txID, true)
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
	got, err := f.dl.Receive(context.Background(), "g", "t-txn", 0, 1, time.Minute, 0, nil)
	if err != nil || len(got) != 1 {
		t.Fatalf("提交后的消息不可消费: %v %d", err, len(got))
	}
}

func TestRollbackDeletesEverything(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	_, txID, _ := f.mgr.Stage(msg("t-txn"))
	found, err := f.mgr.End(txID, false)
	if err != nil || !found {
		t.Fatalf("End(rollback): found=%v err=%v", found, err)
	}
	if f.halfCount(t) != 0 {
		t.Fatal("rollback 后 half 条目残留")
	}
	got, _ := f.dl.Receive(context.Background(), "g", "t-txn", 0, 1, time.Minute, 0, nil)
	if len(got) != 0 {
		t.Fatal("rollback 的消息被消费到了")
	}
}

func TestEndUnknownTxIDIsIdempotent(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	found, err := f.mgr.End("NO-SUCH-TX", true)
	if err != nil {
		t.Fatalf("未知 txID 不该报错（幂等）: %v", err)
	}
	if found {
		t.Fatal("未知 txID 不该 found")
	}
}

func TestEndTwiceSecondIsNoop(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	_, txID, _ := f.mgr.Stage(msg("t-txn"))
	f.mgr.End(txID, true)
	found, err := f.mgr.End(txID, true) // SDK 网络重试会走到这里
	if err != nil || found {
		t.Fatalf("重复 commit 应为幂等 no-op: found=%v err=%v", found, err)
	}
	// 消息只有一条，没有被重复投入
	got, _ := f.dl.Receive(context.Background(), "g", "t-txn", 0, 10, time.Minute, 0, nil)
	if len(got) != 1 {
		t.Fatalf("重复 End 导致消息条数 = %d", len(got))
	}
}
