// 回查调度器测试：到期扫描、Notifier 下发、改期与超限丢弃（spec §10 核心单测第 3 项）。
package txn

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/store"
)

// fakeNotifier 记录收到的回查请求，可编程返回值。
type fakeNotifier struct {
	got  []string // 收到的 txID 序列
	send bool     // RecoverOrphan 返回值
}

func (f *fakeNotifier) RecoverOrphan(m *core.Message, txID string) bool {
	f.got = append(f.got, txID)
	return f.send
}

// stageOverdue 暂存一条半消息并把它的下次回查时间改到过去（造到期条目——
// 正常 Stage 首查在 30s 后，测试等不起，手法同 delay_test 直写暂存区）。
func stageOverdue(t *testing.T, f *fixture, topic string) string {
	t.Helper()
	m, txID, err := f.mgr.Stage(msg(topic))
	if err != nil {
		t.Fatal(err)
	}
	_ = m
	rewriteNextCheck(t, f, txID, time.Now().Add(-time.Second).UnixMilli())
	return txID
}

// rewriteNextCheck 把 txID 的 half 条目搬到指定回查时间（保持两键一致）。
func rewriteNextCheck(t *testing.T, f *fixture, txID string, ms int64) {
	t.Helper()
	refRaw, ok, err := f.st.Get(store.HalfIdxKey(txID))
	if err != nil || !ok {
		t.Fatalf("halfidx 缺失: %v", err)
	}
	ref := &HalfRef{}
	mustUnmarshal(t, refRaw, ref)
	raw, ok, err := f.st.Get(store.HalfKey(ref.NextCheckMs, txID))
	if err != nil || !ok {
		t.Fatalf("half 条目缺失: %v", err)
	}
	old := store.HalfKey(ref.NextCheckMs, txID)
	ref.NextCheckMs = ms
	b := f.st.NewBatch()
	b.Delete(old, nil)
	b.Set(store.HalfKey(ms, txID), raw, nil)
	b.Set(store.HalfIdxKey(txID), mustMarshal(t, ref), nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

// mustMarshal 测试 helper：编码失败直接 Fatal。
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// mustUnmarshal 测试 helper：解码失败直接 Fatal。
func mustUnmarshal(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatal(err)
	}
}

func TestPassSendsRecoverAndReschedules(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	txID := stageOverdue(t, f, "t-check")
	n := &fakeNotifier{send: true}
	sent, err := f.mgr.Pass(n)
	if err != nil || sent != 1 {
		t.Fatalf("Pass: sent=%d err=%v", sent, err)
	}
	if len(n.got) != 1 || n.got[0] != txID {
		t.Fatalf("回查未下发: %v", n.got)
	}
	// 改期后 checks+1、NextCheckMs 在未来，且 half 键已搬到新位置
	refRaw, _, _ := f.st.Get(store.HalfIdxKey(txID))
	ref := &HalfRef{}
	mustUnmarshal(t, refRaw, ref)
	if ref.Checks != 1 || ref.NextCheckMs <= time.Now().UnixMilli() {
		t.Fatalf("改期状态错误: %+v", ref)
	}
	if _, ok, _ := f.st.Get(store.HalfKey(ref.NextCheckMs, txID)); !ok {
		t.Fatal("half 条目未搬到新回查时间")
	}
	if f.mgr.ChecksTotal() != 1 {
		t.Fatalf("ChecksTotal = %d", f.mgr.ChecksTotal())
	}
}

func TestPassReschedulesEvenWhenNoProducerOnline(t *testing.T) {
	// 无在线 producer（RecoverOrphan=false）也必须改期计数：否则 producer
	// 永不回来时半消息永远停在同一到期位、每秒被重扫，maxChecks 形同虚设
	f := newFixture(t, 30*time.Second, 15)
	txID := stageOverdue(t, f, "t-check")
	n := &fakeNotifier{send: false}
	if _, err := f.mgr.Pass(n); err != nil {
		t.Fatal(err)
	}
	refRaw, _, _ := f.st.Get(store.HalfIdxKey(txID))
	ref := &HalfRef{}
	mustUnmarshal(t, refRaw, ref)
	if ref.Checks != 1 {
		t.Fatalf("未下发也要计轮次: %+v", ref)
	}
}

func TestPassDropsAfterMaxChecks(t *testing.T) {
	f := newFixture(t, 30*time.Second, 2) // 上限 2 次
	txID := stageOverdue(t, f, "t-drop")
	n := &fakeNotifier{send: true}
	for i := 0; i < 3; i++ {
		if _, err := f.mgr.Pass(n); err != nil {
			t.Fatal(err)
		}
		// 先查再改写：本条已超限丢弃时（halfCount==0）条目已不存在，
		// 不能再按它的旧键做 rewriteNextCheck
		if f.halfCount(t) == 0 {
			break
		}
		rewriteNextCheck(t, f, txID, time.Now().Add(-time.Second).UnixMilli())
	}
	if f.halfCount(t) != 0 {
		t.Fatal("超限半消息未被丢弃")
	}
	if _, ok, _ := f.st.Get(store.HalfIdxKey(txID)); ok {
		t.Fatal("超限丢弃后 halfidx 残留")
	}
	if f.mgr.DroppedTotal() != 1 {
		t.Fatalf("DroppedTotal = %d", f.mgr.DroppedTotal())
	}
}

func TestPassSkipsEntryEndedInBetween(t *testing.T) {
	// 扫描收集与逐条处理之间客户端完成了 End——处理阶段必须重验 halfidx，
	// 不能凭已收集的旧键复活已决断的事务
	f := newFixture(t, 30*time.Second, 15)
	txID := stageOverdue(t, f, "t-race")
	if found, _ := f.mgr.End(txID, true); !found {
		t.Fatal("End 失败")
	}
	n := &fakeNotifier{send: true}
	sent, err := f.mgr.Pass(n)
	if err != nil || sent != 0 || len(n.got) != 0 {
		t.Fatalf("已决断事务被回查: sent=%d got=%v err=%v", sent, n.got, err)
	}
}
