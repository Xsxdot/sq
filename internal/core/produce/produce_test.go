// Package produce 实现消息写入路径：队列选择、offset 分配、落盘、长轮询唤醒。
//
// 职责（测试文件）：
//   - 验证轮询队列选择下 offset 单调递增、且轮询覆盖全部队列
//   - 验证 offset 计数器崩溃/重启后不回退、不复用（alloc/ 持久化的核心保证）
//   - 验证 MessageGroup 落队规则（同 group 固定同一队列）
//   - 验证 Subscribe/Append 的长轮询唤醒信号
//
// 边界：
//   - 仅测试 produce.Producer 及其导出方法的行为
//   - 不测试 store/meta 内部实现（仅作为依赖复用）
package produce

import (
	"log/slog"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

func newTestProducer(t *testing.T, dir string) (*Producer, *store.Store) {
	t.Helper()
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	mt, err := meta.New(st, true, 4, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	return New(st, mt, slog.Default()), st
}

func TestAppendAssignsMonotonicOffsets(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	seen := map[uint32]uint64{} // queueID -> 上一个 offset
	for i := 0; i < 20; i++ {
		m, err := p.Append(&core.Message{Topic: "t1", Body: []byte("x")})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if last, ok := seen[m.QueueID]; ok && m.Offset != last+1 {
			t.Fatalf("队列 %d offset 不连续: %d -> %d", m.QueueID, last, m.Offset)
		}
		seen[m.QueueID] = m.Offset
	}
	if len(seen) != 4 { // 轮询应覆盖全部 4 个队列
		t.Fatalf("轮询未覆盖全部队列: %v", seen)
	}
}

// TestAppendOffsetsSurviveRestart 验证重启后 offset 计数器不回退、不复用。
//
// 断言方式：记录重启前每个队列各自的最大 offset（before），重启后 Append 一条，
// 直接比较该消息所落队列的新 offset 是否等于 before[队列]+1。alloc/ 计数器随
// 消息同 Batch 原子提交并持久化，因此无论轮询游标重启后落在哪个队列，
// 该队列的下一 offset 必然精确等于其重启前最大 offset+1——这是比"只要不是 0"
// 更强的确定性断言，能真正捕捉"offset 回退复用覆盖旧消息"的 bug。
func TestAppendOffsetsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	p, st := newTestProducer(t, dir)
	before := map[uint32]uint64{} // queueID -> 重启前最后一次 offset
	for i := 0; i < 8; i++ {      // 4 队列各写 2 条
		m, err := p.Append(&core.Message{Topic: "t1", Body: []byte("x")})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		before[m.QueueID] = m.Offset
	}
	st.Close()

	p2, st2 := newTestProducer(t, dir)
	defer st2.Close()
	m, err := p2.Append(&core.Message{Topic: "t1", Body: []byte("y")})
	if err != nil {
		t.Fatalf("重启后 Append: %v", err)
	}
	want := before[m.QueueID] + 1
	if m.Offset != want {
		t.Fatalf("offset 重启后未严格递增: 队列 %d 期望 %d 实得 %d（重启前最大 offset=%d）",
			m.QueueID, want, m.Offset, before[m.QueueID])
	}
}

func TestMessageGroupPinsQueue(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	var q uint32
	for i := 0; i < 5; i++ {
		m, err := p.Append(&core.Message{Topic: "t2", Body: []byte("x"), MessageGroup: "user-1"})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if i == 0 {
			q = m.QueueID
		} else if m.QueueID != q {
			t.Fatalf("同 MessageGroup 落入不同队列: %d vs %d", q, m.QueueID)
		}
	}
}

func TestSubscribeWakesOnAppend(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	ch := p.Subscribe("t3")
	if _, err := p.Append(&core.Message{Topic: "t3", Body: []byte("x")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	select {
	case <-ch: // 期望已被 close
	default:
		t.Fatal("Append 未唤醒订阅者")
	}
}

// TestAppendWritesKeyIndex Keys 索引必须与消息同批落盘。
func TestAppendWritesKeyIndex(t *testing.T) {
	pr, st := newTestProducer(t, t.TempDir()) // 本文件既有 fixture（Task 3 已改 4 参 meta.New）
	defer st.Close()
	m, err := pr.Append(&core.Message{Topic: "t", Body: []byte("x"), Keys: []string{"k1", "k2"}})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	for _, key := range []string{"k1", "k2"} {
		pfx := store.KeyIdxKeyPrefix("t", key)
		found := 0
		err := st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
			_, pk, _, q, off, err := store.ParseKeyIdxKey(k)
			if err != nil || pk != key || q != m.QueueID || off != m.Offset {
				t.Fatalf("索引内容不符: %v %v %v %v", pk, q, off, err)
			}
			found++
			return true, nil
		})
		if err != nil || found != 1 {
			t.Fatalf("key %s 索引条数: %d %v", key, found, err)
		}
	}
}

// TestAppendWithExtraAtomic extra 写操作与消息同批提交。
func TestAppendWithExtraAtomic(t *testing.T) {
	pr, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	marker := []byte("test/marker")
	_, err := pr.AppendWith(&core.Message{Topic: "t", Body: []byte("x")},
		func(b *pebble.Batch) { b.Set(marker, []byte("1"), nil) })
	if err != nil {
		t.Fatalf("AppendWith: %v", err)
	}
	if _, ok, _ := st.Get(marker); !ok {
		t.Fatal("extra 写操作未随消息落盘")
	}
}

// sendDelay 构造一条延时消息并 AppendDelay（测试辅助）
func sendDelay(t *testing.T, p *Producer, topic string, body string, dueMs int64) *core.Message {
	t.Helper()
	m, err := p.AppendDelay(&core.Message{Topic: topic, Body: []byte(body), DeliverAtMs: dueMs})
	if err != nil {
		t.Fatalf("AppendDelay: %v", err)
	}
	return m
}

func countPrefix(t *testing.T, st *store.Store, lower, upper []byte) int {
	t.Helper()
	n := 0
	if err := st.Scan(lower, upper, 0, func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return n
}

func TestAppendDelayWritesDelayEntryNotMsg(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	due := time.Now().Add(time.Hour).UnixMilli()
	m := sendDelay(t, p, "t", "later", due)
	if m.ID == "" {
		t.Fatal("应分配消息 ID")
	}
	dpfx := []byte(store.DelayPrefix)
	if n := countPrefix(t, st, dpfx, store.PrefixUpperBound(dpfx)); n != 1 {
		t.Fatalf("delay 条目数 = %d，期望 1", n)
	}
	mpfx := []byte("msg/")
	if n := countPrefix(t, st, mpfx, store.PrefixUpperBound(mpfx)); n != 0 {
		t.Fatalf("msg/ 应为空，实际 %d 条", n)
	}
}

func TestAppendDelayPastDueFallsThroughToImmediate(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	m := sendDelay(t, p, "t", "now", time.Now().Add(-time.Second).UnixMilli())
	// 已过期：直接普通写入，分配了队列与 offset，delay 区为空
	raw, ok, err := st.Get(store.MsgKey("t", m.QueueID, m.Offset))
	if err != nil || !ok {
		t.Fatalf("过期延时消息应立即入 msg/: %v", err)
	}
	got, _ := core.DecodeMessage(raw)
	if got.DeliverAtMs == 0 {
		t.Fatal("直通写入也要保留 DeliverAtMs（投递时回填协议字段用）")
	}
	dpfx := []byte(store.DelayPrefix)
	if n := countPrefix(t, st, dpfx, store.PrefixUpperBound(dpfx)); n != 0 {
		t.Fatal("过期延时消息不应写 delay 条目")
	}
}

func TestAppendDelayRejectsInvalid(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	if _, err := p.AppendDelay(&core.Message{Topic: "t", Body: nil, DeliverAtMs: time.Now().Add(time.Hour).UnixMilli()}); err == nil {
		t.Fatal("空 body 应拒绝")
	}
	if _, err := p.AppendDelay(&core.Message{Topic: "t", Body: []byte("x"), DeliverAtMs: 0}); err == nil {
		t.Fatal("DeliverAtMs<=0 是编程错误，应拒绝")
	}
}

func TestAppendDelaySeqPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	p, st := newTestProducer(t, dir)
	due := time.Now().Add(time.Hour).UnixMilli()
	sendDelay(t, p, "t", "a", due)
	sendDelay(t, p, "t", "b", due)
	st.Close()
	// 重开：seq 计数器从盘上恢复，不与已有条目撞 key
	p2, st2 := newTestProducer(t, dir)
	defer st2.Close()
	sendDelay(t, p2, "t", "c", due)
	dpfx := []byte(store.DelayPrefix)
	if n := countPrefix(t, st2, dpfx, store.PrefixUpperBound(dpfx)); n != 3 {
		t.Fatalf("重启后 delay 条目数 = %d，期望 3（seq 撞 key 会覆盖变 2）", n)
	}
}
