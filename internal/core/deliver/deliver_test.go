// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责（测试文件）：
//   - 验证 Receive/Ack 基本流程：取件后 ack，ack 后不再重复投递
//   - 验证不可见超时后的惰性重投，且 DeliveryAttempt 正确递增
//   - 验证 Ack 幂等：重复 ack 返回 (false, nil) 而非报错
//   - 验证 Ack/ChangeInvisible 的 attempt 校验：陈旧句柄（attempt 不匹配，
//     说明目标记录已被过期重投覆盖）幂等拒绝且不删除新记录；attempt 匹配时正常生效
//   - 验证 ChangeInvisible：延长不可见时间真的能压下重投、Attempts 在改写后保持不变、
//     未知 offset 返回 (false, nil) 而非报错
//   - 验证长轮询：空结果时阻塞等待，新消息写入后立即唤醒返回
//   - 验证不同消费组游标互相独立
//   - 验证阶段 2（新消息扫描）不会因为阶段 1（过期重投）填满 out 而失控扫描整个队列
//     （对应实现里的裁定 1：len(out)>=maxMsgs 时必须跳过阶段 2，store.Scan 的
//     limit<=0 语义是"不限"，若误传 0 会吐出整条剩余队列）
//   - 验证阶段 1 的孤儿 inflight 清理会真正提交（裁定 2），且不误报为投递成功
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
		if ok, err := f.dl.Ack("g", "t", m.QueueID, m.Offset, m.DeliveryAttempt); !ok || err != nil {
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
	// 第一次取出，不可见时间设 300ms 且不 ack。
	//
	// 注意（Minor 4 flake 修复）：这里原来用 30ms 不可见 + 紧接着的第二次
	// Receive 断言"仍不可见"，在 -race 或机器负载较高时，两次 Receive
	// 之间的调度延迟就可能逼近甚至超过 30ms，导致消息真的合法过期重投，
	// 断言假失败——不是被测代码的 bug，是测试自己的时间预算太紧。
	// 300ms/400ms 给出足够裕量，其余计时类用例的容错方向本来就是安全的
	// （宁可多等，不会误判"过早重投"），无需一并调整。
	msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, 300*time.Millisecond, 0)
	if len(msgs) != 1 {
		t.Fatalf("首取: %d", len(msgs))
	}
	// 未过期期间不可见
	msgs, _ = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if len(msgs) != 0 {
		t.Fatalf("不可见期内重复投递: %d", len(msgs))
	}
	time.Sleep(400 * time.Millisecond)
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
	ok, err := f.dl.Ack("g", "t", msgs[0].QueueID, msgs[0].Offset, msgs[0].DeliveryAttempt)
	if !ok || err != nil {
		t.Fatalf("首次 Ack: %v %v", ok, err)
	}
	ok, err = f.dl.Ack("g", "t", msgs[0].QueueID, msgs[0].Offset, msgs[0].DeliveryAttempt)
	if ok || err != nil { // 重复 ack：ok=false 且无错（记录已不存在）
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

// TestAckStaleAttemptRejected 锁定 Important 1 的修复：Ack 现在按
// (group,topic,queue,offset,attempt) 定位 inflight 记录。消费者 A 拿到的是
// attempt=1 的句柄，处理超时后消息被过期重投给了下一轮（attempt=2）；A 迟到
// 的 Ack 若不校验 attempt 会误删这条"属于下一轮"的记录，导致消息从此既无
// inflight 兜底也无游标覆盖——一旦接手的一方也失败，消息永久丢失。
//
// 期望：陈旧 attempt 的 Ack 幂等返回 (false,nil)，且不删除记录——用"记录还能
// 在下一次过期后继续被重投出来（attempt 变成 3）"来证明记录确实还在，而不是
// 只看 Ack 的返回值（返回值层面 (false,nil) 和"记录已被删"看起来是一样的）。
func TestAckStaleAttemptRejected(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 200*time.Millisecond, 0)
	if err != nil || len(first) != 1 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	offset, staleAttempt := first[0].Offset, first[0].DeliveryAttempt // attempt=1
	time.Sleep(300 * time.Millisecond)                                // 等待首轮过期

	second, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 200*time.Millisecond, 0)
	if err != nil || len(second) != 1 || second[0].DeliveryAttempt != 2 {
		t.Fatalf("过期重投应得 attempt=2: %d %v", len(second), second)
	}

	// 用陈旧 attempt(1) ack：应视为陈旧句柄，幂等拒绝，不能删掉 attempt=2 的记录
	ok, err := f.dl.Ack("g", "t", 0, offset, staleAttempt)
	if ok || err != nil {
		t.Fatalf("陈旧 attempt 的 ack 应为 (false,nil): %v %v", ok, err)
	}

	// 证明 attempt=2 的记录仍在：等它也过期，应该还能重投出 attempt=3；
	// 若陈旧 ack 误删了记录，这里会因为找不到可重投的 inflight 而拿到 0 条。
	time.Sleep(300 * time.Millisecond)
	third, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil || len(third) != 1 || third[0].DeliveryAttempt != 3 {
		t.Fatalf("陈旧 ack 不应删除记录，记录应仍可重投: %d %v", len(third), third)
	}
}

// TestAckCorrectAttemptSucceeds 锁定 Important 1：携带与当前 inflight 记录一致的
// attempt 时，ack 正常生效并真正删除记录（用"立即重复 ack 幂等失败"侧面证明已删除）。
func TestAckCorrectAttemptSucceeds(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 200*time.Millisecond, 0)
	if err != nil || len(first) != 1 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	time.Sleep(300 * time.Millisecond)
	second, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil || len(second) != 1 || second[0].DeliveryAttempt != 2 {
		t.Fatalf("过期重投应得 attempt=2: %d %v", len(second), second)
	}
	ok, err := f.dl.Ack("g", "t", 0, second[0].Offset, second[0].DeliveryAttempt)
	if !ok || err != nil {
		t.Fatalf("正确 attempt 的 ack 应成功: %v %v", ok, err)
	}
	ok, err = f.dl.Ack("g", "t", 0, second[0].Offset, second[0].DeliveryAttempt)
	if ok || err != nil {
		t.Fatalf("记录已被删除，重复 ack 应幂等失败: %v %v", ok, err)
	}
}

// TestChangeInvisibleExtendsWindow 锁定 Important 3：ChangeInvisible 延长
// 不可见时间后，原本会因超时而过期重投的消息应被压下。这是 Task 11 gRPC
// ChangeInvisible 端点的直接依赖，此前 7 个用例没有一个覆盖过它。
func TestChangeInvisibleExtendsWindow(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 50*time.Millisecond, 0)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("首取: %d %v", len(msgs), err)
	}
	ok, err := f.dl.ChangeInvisible("g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt, time.Minute)
	if !ok || err != nil {
		t.Fatalf("ChangeInvisible: %v %v", ok, err)
	}
	time.Sleep(80 * time.Millisecond) // 超过原 50ms 不可见时间，但远小于新设的 1 分钟
	again, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil || len(again) != 0 {
		t.Fatalf("延长不可见时间后不应重投: %d %v", len(again), err)
	}
}

// TestChangeInvisiblePreservesAttempts 验证 ChangeInvisible 的读-改-写只重写
// ExpireAtMs，不影响已持久化的 Attempts——它不是"重新投递"，不应该让投递计数
// 跳变或倒退。
func TestChangeInvisiblePreservesAttempts(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 200*time.Millisecond, 0)
	if err != nil || len(first) != 1 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	time.Sleep(300 * time.Millisecond)
	second, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil || len(second) != 1 || second[0].DeliveryAttempt != 2 {
		t.Fatalf("过期重投应得 attempt=2: %d %v", len(second), second)
	}
	ok, err := f.dl.ChangeInvisible("g", "t", 0, second[0].Offset, second[0].DeliveryAttempt, 60*time.Millisecond)
	if !ok || err != nil {
		t.Fatalf("ChangeInvisible: %v %v", ok, err)
	}
	time.Sleep(120 * time.Millisecond)
	third, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil || len(third) != 1 || third[0].DeliveryAttempt != 3 {
		t.Fatalf("ChangeInvisible 后 attempts 应在原值上继续递增，不应被改写/重置: %d %v", len(third), third)
	}
}

// TestChangeInvisibleUnknownOffsetReturnsFalse 验证目标 offset 不存在（从未投递，
// 或已被 ack/已被别的 attempt 覆盖）时返回 (false,nil) 而非报错——Task 11 的 gRPC
// 端点要靠这个区分"句柄已失效，正常忽略"与"真正的系统错误"。
func TestChangeInvisibleUnknownOffsetReturnsFalse(t *testing.T) {
	f := newFixture(t)
	ok, err := f.dl.ChangeInvisible("g", "t", 0, 999, 1, time.Minute)
	if ok || err != nil {
		t.Fatalf("未知 offset 应为 (false,nil): %v %v", ok, err)
	}
}
