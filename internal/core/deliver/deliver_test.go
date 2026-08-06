// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责（测试文件）：
//   - 验证 Receive/Ack 基本流程：取件后 ack，ack 后不再重复投递
//   - 验证不可见超时后的惰性重投，且 DeliveryAttempt 正确递增
//   - 验证 Ack 幂等：重复 ack 返回 (false, nil) 而非报错
//   - 验证长轮询：空结果时阻塞等待，新消息写入后立即唤醒返回
//   - 验证不同消费组游标互相独立
//   - 验证阶段 2（新消息扫描）不会因为阶段 1（过期重投）填满 out 而失控扫描整个队列
//     （对应实现里的裁定 1：len(out)>=maxMsgs 时必须跳过阶段 2，store.Scan 的
//     limit<=0 语义是"不限"，若误传 0 会吐出整条剩余队列）
//
// 边界：
//   - 仅测试 deliver.Deliverer 及其导出方法的行为
//   - 不测试 store/meta/produce 内部实现（仅作为依赖复用）
package deliver

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

type fixture struct {
	st *store.Store
	pr *produce.Producer
	dl *Deliverer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, slog.Default()) // 单队列，测试确定性
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(st, mt, slog.Default())
	return &fixture{st: st, pr: pr, dl: New(st, mt, pr, slog.Default())}
}

func (f *fixture) send(t *testing.T, topic, body string) {
	t.Helper()
	if _, err := f.pr.Append(&core.Message{Topic: topic, Body: []byte(body)}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func TestReceiveAckFlow(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	f.send(t, "t", "b")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("Receive: %d %v", len(msgs), err)
	}
	if msgs[0].DeliveryAttempt != 1 {
		t.Fatalf("首投 attempt 应为 1: %d", msgs[0].DeliveryAttempt)
	}
	// ack 后不应再收到
	for _, m := range msgs {
		if ok, err := f.dl.Ack("g", "t", m.QueueID, m.Offset); !ok || err != nil {
			t.Fatalf("Ack: %v %v", ok, err)
		}
	}
	msgs, err = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("ack 后仍收到: %d %v", len(msgs), err)
	}
}

func TestUnackedRedeliveryAfterExpire(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	// 第一次取出，极短不可见时间且不 ack
	msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, 30*time.Millisecond, 0)
	if len(msgs) != 1 {
		t.Fatalf("首取: %d", len(msgs))
	}
	// 未过期期间不可见
	msgs, _ = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if len(msgs) != 0 {
		t.Fatalf("不可见期内重复投递: %d", len(msgs))
	}
	time.Sleep(50 * time.Millisecond)
	// 过期后重投，attempt +1
	msgs, _ = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if len(msgs) != 1 || msgs[0].DeliveryAttempt != 2 {
		t.Fatalf("过期重投: %d attempt=%v", len(msgs), msgs)
	}
}

func TestAckIdempotent(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	ok, err := f.dl.Ack("g", "t", msgs[0].QueueID, msgs[0].Offset)
	if !ok || err != nil {
		t.Fatalf("首次 Ack: %v %v", ok, err)
	}
	ok, err = f.dl.Ack("g", "t", msgs[0].QueueID, msgs[0].Offset)
	if ok || err != nil { // 重复 ack：ok=false 且无错
		t.Fatalf("重复 Ack 应幂等: %v %v", ok, err)
	}
}

func TestLongPollingWakesOnNewMessage(t *testing.T) {
	f := newFixture(t)
	done := make(chan []*core.Message, 1)
	go func() {
		msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 3*time.Second)
		done <- msgs
	}()
	time.Sleep(100 * time.Millisecond) // 让 Receive 先进入等待
	f.send(t, "t", "a")
	select {
	case msgs := <-done:
		if len(msgs) != 1 {
			t.Fatalf("长轮询结果: %d", len(msgs))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("长轮询未被新消息唤醒")
	}
}

func TestTwoGroupsIndependentCursors(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	m1, _ := f.dl.Receive(context.Background(), "g1", "t", 0, 10, time.Minute, 0)
	m2, _ := f.dl.Receive(context.Background(), "g2", "t", 0, 10, time.Minute, 0)
	if len(m1) != 1 || len(m2) != 1 {
		t.Fatalf("两组各自应收到 1 条: %d %d", len(m1), len(m2))
	}
}

// TestRedeliveryFillDoesNotUnboundNewMessageScan 锁定裁定 1：
// 阶段 1（过期重投）填满 maxMsgs 后，阶段 2 必须整体跳过，不能因为
// maxMsgs-len(out)==0 被 store.Scan 当作"不限"扫出整条剩余队列。
//
// 构造：发送 5 条消息（多于 maxMsgs=2）；先用极短不可见时间取 2 条
// （offset 0,1）不 ack，让它们过期；等待过期后再次 Receive(maxMsgs=2)——
// 此时阶段 1 能凑满 2 条重投，若阶段 2 未跳过，会把剩余 3 条新消息
// （offset 2,3,4）一并扫出，返回 5 条，暴露 bug。
func TestRedeliveryFillDoesNotUnboundNewMessageScan(t *testing.T) {
	f := newFixture(t)
	for _, body := range []string{"a", "b", "c", "d", "e"} {
		f.send(t, "t", body)
	}
	// 第一次取件：maxMsgs=2，极短不可见时间，取到 offset 0,1，不 ack
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 2, 30*time.Millisecond, 0)
	if err != nil || len(first) != 2 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	time.Sleep(50 * time.Millisecond) // 等待 inflight 过期
	// 第二次取件：maxMsgs 仍为 2。阶段 1 应能凑满 2 条重投（offset 0,1），
	// 阶段 2 必须被跳过——返回值长度必须恰好是 maxMsgs，不能把剩余 3 条新消息也带出来。
	second, err := f.dl.Receive(context.Background(), "g", "t", 0, 2, time.Minute, 0)
	if err != nil {
		t.Fatalf("次取: %v", err)
	}
	if len(second) != 2 {
		t.Fatalf("阶段 2 未正确跳过，返回 %d 条（应恰为 maxMsgs=2）", len(second))
	}
	for _, m := range second {
		if m.DeliveryAttempt != 2 {
			t.Fatalf("重投消息 attempt 应为 2: offset=%d attempt=%d", m.Offset, m.DeliveryAttempt)
		}
	}
}

// TestOrphanInflightCleanupPersistsAndDoesNotReportDelivery 锁定裁定 2：
// 阶段 1 清理"孤儿" inflight（消息已不在但 inflight 记录还在）时暂存的
// Delete 必须真正提交到盘上，且清理本身不能被当作"投递成功"上报给调用方。
//
// 构造：正常收一条消息但不 ack，让 inflight 记录过期；随后直接从 store
// 删掉消息本体（模拟 retention 清理跑在了 inflight 之前——M1 还没有
// retention，这里手工制造该场景）。此时该 inflight 记录就是"孤儿"：
// 过期但对应消息已不存在。
//
// 期望：
//  1. Receive 返回空结果（孤儿清理不算投递成功，不能打断长轮询语义）
//  2. 孤儿 inflight 记录被真正从盘上删除——直接读 store 验证，
//     而不是只看 Receive 的返回值
//
// 若走了 brief 原有的"len(out)==0 就 Close 丢弃整个 batch"早退路径，
// 这条 Delete 会被悄悄丢弃，孤儿记录永远留在盘上，第 2 项断言会失败。
func TestOrphanInflightCleanupPersistsAndDoesNotReportDelivery(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 30*time.Millisecond, 0)
	if err != nil || len(first) != 1 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	offset := first[0].Offset

	// 模拟消息已被清理：直接删掉消息本体，留下 inflight 记录，制造孤儿。
	b := f.st.NewBatch()
	b.Delete(store.MsgKey("t", 0, offset), nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatalf("模拟删除消息本体: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // 等待 inflight 过期，变成孤儿重投候选

	out, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil {
		t.Fatalf("孤儿清理取件: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("孤儿清理不应上报为投递成功: %d", len(out))
	}
	if _, ok, err := f.st.Get(store.InflightKey("g", "t", 0, offset)); err != nil {
		t.Fatalf("查询 inflight: %v", err)
	} else if ok {
		t.Fatalf("孤儿 inflight 记录应已被真正清理，但仍存在于 store")
	}
}
