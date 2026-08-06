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
