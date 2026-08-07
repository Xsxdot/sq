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

func TestPassDeletesBadHalfKeyAndContinues(t *testing.T) {
	// 坏 key（ParseHalfKey 拒绝）排在到期扫描头部时，旧实现直接中断整趟，
	// 其后健康条目永久饿死（RunChecker 只记日志每秒重扫）。修复后应删坏止损
	// 并继续处理健康条目（同 delay 删坏条目精神）
	f := newFixture(t, 30*time.Second, 15)
	txID := stageOverdue(t, f, "t-badkey")
	// "half/\x00"：前缀后仅 1B（≤8B）必被 ParseHalfKey 拒绝，且字典序排在
	// 任何合法 half 键（8B 大端 ms 首字节 0x00）之前——正好落在到期扫描头部，
	// 复现"坏条目堵死其后全部条目"的形态
	badKey := append([]byte(store.HalfPrefix), 0x00)
	b := f.st.NewBatch()
	b.Set(badKey, []byte("whatever"), nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
	n := &fakeNotifier{send: true}
	sent, err := f.mgr.Pass(n)
	if err != nil || sent != 2 {
		t.Fatalf("Pass: sent=%d err=%v（坏 key 1 条 + 健康 1 条均计入 handled）", sent, err)
	}
	if _, ok, _ := f.st.Get(badKey); ok {
		t.Fatal("坏 key 未被删除（否则每趟重扫重报，永久日志洪水）")
	}
	if f.halfCount(t) != 1 {
		t.Fatalf("half 条目数 = %d（期望只剩健康一条）", f.halfCount(t))
	}
	refRaw, ok, _ := f.st.Get(store.HalfIdxKey(txID))
	if !ok {
		t.Fatal("健康条目 halfidx 缺失")
	}
	ref := &HalfRef{}
	mustUnmarshal(t, refRaw, ref)
	if ref.Checks != 1 || ref.NextCheckMs <= time.Now().UnixMilli() {
		t.Fatalf("健康条目未被正常改期: %+v", ref)
	}
	if len(n.got) != 1 || n.got[0] != txID {
		t.Fatalf("健康条目回查未下发: %v", n.got)
	}
}

func TestPassDeletesCorruptHalfIdxAndContinues(t *testing.T) {
	// halfidx 被外部改写为坏 JSON：旧实现在 checkOne 解码失败即中断本趟。
	// 修复后应靠扫描解析出的 half key 把两键同批删除止损，Pass 不报错
	f := newFixture(t, 30*time.Second, 15)
	txID := stageOverdue(t, f, "t-badidx")
	b := f.st.NewBatch()
	b.Set(store.HalfIdxKey(txID), []byte("not-json"), nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
	n := &fakeNotifier{send: true}
	sent, err := f.mgr.Pass(n)
	if err != nil || sent != 1 {
		t.Fatalf("Pass: sent=%d err=%v（坏 halfidx 条目计入 handled）", sent, err)
	}
	if f.halfCount(t) != 0 {
		t.Fatal("坏 halfidx 的 half 条目未被删除")
	}
	if _, ok, _ := f.st.Get(store.HalfIdxKey(txID)); ok {
		t.Fatal("坏 halfidx 未被删除")
	}
	if len(n.got) != 0 {
		t.Fatalf("坏条目不应触发回查下发: %v", n.got)
	}
}

func TestPassDeletesCorruptValueWithMissingIdx(t *testing.T) {
	// 「坏值 + 坏 idx」双重损坏：旧 dropLocked 从 idx 侧重建 half key 来删，
	// idx 损坏时重建失败，坏 half 条目永远留在盘上、每趟被重扫重报 Error。
	// 修复后删除目标直接用扫描解析出的 halfKey，两键无条件删除
	f := newFixture(t, 30*time.Second, 15)
	txID := stageOverdue(t, f, "t-badval")
	// 先读健康 idx 拿到当前 half key（此后就要把它弄坏，不能再靠它重建）
	refRaw, ok, err := f.st.Get(store.HalfIdxKey(txID))
	if err != nil || !ok {
		t.Fatalf("halfidx 缺失: %v", err)
	}
	ref := &HalfRef{}
	mustUnmarshal(t, refRaw, ref)
	halfKey := store.HalfKey(ref.NextCheckMs, txID)
	// 值覆写成坏字节（DecodeMessage 必败）+ idx 覆写成坏 JSON。选坏 JSON 而非
	// 直接删 idx：与 TestPassDeletesCorruptHalfIdxAndContinues 同一种「idx 在但
	// 不可用」的损坏形态，也同时覆盖了旧实现 json.Unmarshal 失败即不删 half 键
	// 的残留窗口（删 idx 只是它的退化子集）
	b := f.st.NewBatch()
	b.Set(halfKey, []byte("not-a-message"), nil)
	b.Set(store.HalfIdxKey(txID), []byte("not-json"), nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
	n := &fakeNotifier{send: true}
	sent, err := f.mgr.Pass(n)
	if err != nil || sent != 1 {
		t.Fatalf("Pass: sent=%d err=%v（坏条目计入 handled，不报错）", sent, err)
	}
	if f.halfCount(t) != 0 {
		t.Fatal("坏值坏 idx 条目未被彻底清除（否则每趟重扫重报）")
	}
	if _, ok, _ := f.st.Get(store.HalfIdxKey(txID)); ok {
		t.Fatal("坏 halfidx 未被删除")
	}
	if len(n.got) != 0 {
		t.Fatalf("坏条目不应触发回查下发: %v", n.got)
	}
}
