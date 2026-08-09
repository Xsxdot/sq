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
//   - 验证阶段 2（新消息扫描）不会因为阶段 1（过期重投）填满 out 而失控扫描整个队列：
//     len(out)>=maxMsgs 时必须整体跳过阶段 2，因为 store.Scan 的 limit<=0 语义是
//     "不限"，把算出来的 0 传进去会吐出整条剩余队列
//   - 验证阶段 1 的孤儿 inflight 清理会真正提交（提交判据是"有无暂存写入"而非
//     "有无可投递消息"），且不误报为投递成功
//   - 验证 Tag 过滤：只投匹配 tag 的消息；不匹配的跳过、推进位点、不占 inflight
//     （对该消费组永久跳过，换全量过滤器也收不到）
//   - 验证全部消息不匹配时位点仍须持久化推进（否则每次 Receive 反复扫描同一批
//     不匹配消息，性能退化且永不前进）
//   - 验证重投指数退避（不可见时长下限）与投递次数超限后原子转入 %DLQ%{group}
//
// 边界：
//   - 仅测试 deliver.Deliverer 及其导出方法的行为
//   - 不测试 store/meta/produce 内部实现（仅作为依赖复用）
package deliver

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
)

type fixture struct {
	st *store.Store
	pr *produce.Producer
	dl *Deliverer
}

func newFixture(t *testing.T) *fixture {
	return newFixtureMaxAttempts(t, 16)
}

// newFixtureMaxAttempts 指定组默认 maxAttempts 的 fixture（DLQ 用例用小值控制时长）。
func newFixtureMaxAttempts(t *testing.T, maxAttempts int32) *fixture {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 1, maxAttempts, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	return &fixture{st: st, pr: pr, dl: New(rep, rt, st, mt, pr, slog.Default())}
}

func (f *fixture) send(t *testing.T, topic, body string) {
	t.Helper()
	if _, err := f.pr.Append(context.Background(), &core.Message{Topic: topic, Body: []byte(body)}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func (f *fixture) sendTagged(t *testing.T, topic, body, tag string) {
	t.Helper()
	if _, err := f.pr.Append(context.Background(), &core.Message{Topic: topic, Body: []byte(body), Tag: tag}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// sendGrouped 发送一条顺序消息（MessageGroup 非空，M4 顺序锁用例专用辅助）
func (f *fixture) sendGrouped(t *testing.T, topic, body, group string) {
	t.Helper()
	if _, err := f.pr.Append(context.Background(), &core.Message{Topic: topic, Body: []byte(body), MessageGroup: group}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func TestReceiveAckFlow(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	f.send(t, "t", "b")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("Receive: %d %v", len(msgs), err)
	}
	if msgs[0].DeliveryAttempt != 1 {
		t.Fatalf("首投 attempt 应为 1: %d", msgs[0].DeliveryAttempt)
	}
	// ack 后不应再收到
	for _, m := range msgs {
		if ok, err := f.dl.Ack(context.Background(), "g", "t", m.QueueID, m.Offset, m.DeliveryAttempt); !ok || err != nil {
			t.Fatalf("Ack: %v %v", ok, err)
		}
	}
	msgs, err = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("ack 后仍收到: %d %v", len(msgs), err)
	}
}

func TestUnackedRedeliveryAfterExpire(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	// 第一次取出，不可见时间设 300ms 且不 ack。
	//
	// 时间预算说明（这是一次真实 flake 的修复，别再调小）：不可见时间不能取到
	// 30ms 这个量级——本用例紧接着还要再 Receive 一次并断言"仍不可见"，而在
	// -race 或机器负载较高时，两次 Receive 之间的调度延迟本身就可能逼近甚至超过
	// 30ms，消息于是合法过期重投，断言假失败。这不是被测代码的 bug，是测试自己
	// 的时间预算太紧。300ms/400ms 留出足够裕量。其余计时类用例不用一并调整：
	// 它们的容错方向本来就是安全的（宁可多等，不会误判"过早重投"）。
	msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, 300*time.Millisecond, 0, nil)
	if len(msgs) != 1 {
		t.Fatalf("首取: %d", len(msgs))
	}
	// 未过期期间不可见
	msgs, _ = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if len(msgs) != 0 {
		t.Fatalf("不可见期内重复投递: %d", len(msgs))
	}
	time.Sleep(400 * time.Millisecond)
	// 过期后重投，attempt +1
	msgs, _ = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if len(msgs) != 1 || msgs[0].DeliveryAttempt != 2 {
		t.Fatalf("过期重投: %d attempt=%v", len(msgs), msgs)
	}
}

func TestAckIdempotent(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	ok, err := f.dl.Ack(context.Background(), "g", "t", msgs[0].QueueID, msgs[0].Offset, msgs[0].DeliveryAttempt)
	if !ok || err != nil {
		t.Fatalf("首次 Ack: %v %v", ok, err)
	}
	ok, err = f.dl.Ack(context.Background(), "g", "t", msgs[0].QueueID, msgs[0].Offset, msgs[0].DeliveryAttempt)
	if ok || err != nil { // 重复 ack：ok=false 且无错（记录已不存在）
		t.Fatalf("重复 Ack 应幂等: %v %v", ok, err)
	}
}

func TestLongPollingWakesOnNewMessage(t *testing.T) {
	f := newFixture(t)
	done := make(chan []*core.Message, 1)
	go func() {
		msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 3*time.Second, nil)
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
	m1, _ := f.dl.Receive(context.Background(), "g1", "t", 0, 10, time.Minute, 0, nil)
	m2, _ := f.dl.Receive(context.Background(), "g2", "t", 0, 10, time.Minute, 0, nil)
	if len(m1) != 1 || len(m2) != 1 {
		t.Fatalf("两组各自应收到 1 条: %d %d", len(m1), len(m2))
	}
}

// TestRedeliveryFillDoesNotUnboundNewMessageScan 锁定取件批量上限：
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
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 2, 30*time.Millisecond, 0, nil)
	if err != nil || len(first) != 2 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	time.Sleep(50 * time.Millisecond) // 等待 inflight 过期
	// 第二次取件：maxMsgs 仍为 2。阶段 1 应能凑满 2 条重投（offset 0,1），
	// 阶段 2 必须被跳过——返回值长度必须恰好是 maxMsgs，不能把剩余 3 条新消息也带出来。
	second, err := f.dl.Receive(context.Background(), "g", "t", 0, 2, time.Minute, 0, nil)
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

// TestOrphanInflightCleanupPersistsAndDoesNotReportDelivery 锁定批次提交判据：
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
// 若把早退判据写成"len(out)==0 就 Close 丢弃整个 batch"，这条 Delete 会被
// 悄悄丢弃，孤儿记录永远留在盘上，第 2 项断言会失败。
func TestOrphanInflightCleanupPersistsAndDoesNotReportDelivery(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 30*time.Millisecond, 0, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	offset := first[0].Offset

	// 模拟消息已被清理：直接删掉消息本体，留下 inflight 记录，制造孤儿。
	b := f.st.NewBatch()
	b.Delete(store.MsgKey("t", 0, offset))
	if err := f.st.Apply(b); err != nil {
		t.Fatalf("模拟删除消息本体: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // 等待 inflight 过期，变成孤儿重投候选

	out, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
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

// TestAckStaleAttemptRejected 锁定 Ack 的定位方式：必须按
// (group,topic,queue,offset,attempt) 定位 inflight 记录。消费者 A 拿到的是
// attempt=1 的句柄，处理超时后消息被过期重投给了下一轮（attempt=2）；A 迟到
// 的 Ack 若不校验 attempt 会误删这条"属于下一轮"的记录，导致消息从此既无
// inflight 兜底也无游标覆盖——一旦接手的一方也失败，消息永久丢失。
//
// 期望：陈旧 attempt 的 Ack 幂等返回 (false,nil)，且不删除记录——用"记录还能
// 在下一次过期后继续被重投出来（attempt 变成 3）"来证明记录确实还在，而不是
// 只看 Ack 的返回值（返回值层面 (false,nil) 和"记录已被删"看起来是一样的）。
func TestAckStaleAttemptRejected(t *testing.T) {
	// attempt=2 的重投会受退避下限约束（默认 10s），而本用例的结尾依赖它
	// 在 300ms 内再次过期、重投出 attempt=3 来证明记录未被陈旧 ack 误删。
	// 注入小退避基数绕过该下限（var 供测试注入，见实现注释）。
	oldBase := retryBackoffBase
	retryBackoffBase = 10 * time.Millisecond
	defer func() { retryBackoffBase = oldBase }()

	f := newFixture(t)
	f.send(t, "t", "a")
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 200*time.Millisecond, 0, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	offset, staleAttempt := first[0].Offset, first[0].DeliveryAttempt // attempt=1
	time.Sleep(300 * time.Millisecond)                                // 等待首轮过期

	second, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 200*time.Millisecond, 0, nil)
	if err != nil || len(second) != 1 || second[0].DeliveryAttempt != 2 {
		t.Fatalf("过期重投应得 attempt=2: %d %v", len(second), second)
	}

	// 用陈旧 attempt(1) ack：应视为陈旧句柄，幂等拒绝，不能删掉 attempt=2 的记录
	ok, err := f.dl.Ack(context.Background(), "g", "t", 0, offset, staleAttempt)
	if ok || err != nil {
		t.Fatalf("陈旧 attempt 的 ack 应为 (false,nil): %v %v", ok, err)
	}

	// 证明 attempt=2 的记录仍在：等它也过期，应该还能重投出 attempt=3；
	// 若陈旧 ack 误删了记录，这里会因为找不到可重投的 inflight 而拿到 0 条。
	time.Sleep(300 * time.Millisecond)
	third, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(third) != 1 || third[0].DeliveryAttempt != 3 {
		t.Fatalf("陈旧 ack 不应删除记录，记录应仍可重投: %d %v", len(third), third)
	}
}

// TestAckCorrectAttemptSucceeds 是上一条的正向对照：携带与当前 inflight 记录一致的
// attempt 时，ack 正常生效并真正删除记录（用"立即重复 ack 幂等失败"侧面证明已删除）。
func TestAckCorrectAttemptSucceeds(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 200*time.Millisecond, 0, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	time.Sleep(300 * time.Millisecond)
	second, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(second) != 1 || second[0].DeliveryAttempt != 2 {
		t.Fatalf("过期重投应得 attempt=2: %d %v", len(second), second)
	}
	ok, err := f.dl.Ack(context.Background(), "g", "t", 0, second[0].Offset, second[0].DeliveryAttempt)
	if !ok || err != nil {
		t.Fatalf("正确 attempt 的 ack 应成功: %v %v", ok, err)
	}
	ok, err = f.dl.Ack(context.Background(), "g", "t", 0, second[0].Offset, second[0].DeliveryAttempt)
	if ok || err != nil {
		t.Fatalf("记录已被删除，重复 ack 应幂等失败: %v %v", ok, err)
	}
}

// TestChangeInvisibleExtendsWindow 验证 ChangeInvisible 的核心作用：延长不可见
// 时间后，原本会因超时而过期重投的消息应被压下。协议层的
// ChangeInvisibleDuration 端点直接依赖这条语义——消费者靠它为处理较慢的消息
// 续期，续期失效就意味着消息会在消费者还在处理时被重投给别人。
func TestChangeInvisibleExtendsWindow(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 50*time.Millisecond, 0, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("首取: %d %v", len(msgs), err)
	}
	ok, err := f.dl.ChangeInvisible(context.Background(), "g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt, time.Minute)
	if !ok || err != nil {
		t.Fatalf("ChangeInvisible: %v %v", ok, err)
	}
	time.Sleep(80 * time.Millisecond) // 超过原 50ms 不可见时间，但远小于新设的 1 分钟
	again, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
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
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 200*time.Millisecond, 0, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("首取: %d %v", len(first), err)
	}
	time.Sleep(300 * time.Millisecond)
	second, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(second) != 1 || second[0].DeliveryAttempt != 2 {
		t.Fatalf("过期重投应得 attempt=2: %d %v", len(second), second)
	}
	ok, err := f.dl.ChangeInvisible(context.Background(), "g", "t", 0, second[0].Offset, second[0].DeliveryAttempt, 60*time.Millisecond)
	if !ok || err != nil {
		t.Fatalf("ChangeInvisible: %v %v", ok, err)
	}
	time.Sleep(120 * time.Millisecond)
	third, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(third) != 1 || third[0].DeliveryAttempt != 3 {
		t.Fatalf("ChangeInvisible 后 attempts 应在原值上继续递增，不应被改写/重置: %d %v", len(third), third)
	}
}

// TestChangeInvisibleUnknownOffsetReturnsFalse 验证目标 offset 不存在（从未投递，
// 或已被 ack/已被别的 attempt 覆盖）时返回 (false,nil) 而非报错——协议层要靠
// 这个区分"句柄已失效，正常忽略"与"真正的系统错误"：前者回
// INVALID_RECEIPT_HANDLE 让客户端静默丢弃，后者才是 INTERNAL_SERVER_ERROR。
func TestChangeInvisibleUnknownOffsetReturnsFalse(t *testing.T) {
	f := newFixture(t)
	ok, err := f.dl.ChangeInvisible(context.Background(), "g", "t", 0, 999, 1, time.Minute)
	if ok || err != nil {
		t.Fatalf("未知 offset 应为 (false,nil): %v %v", ok, err)
	}
}

// TestTagFilterDelivery 只投匹配 tag；不匹配的跳过、推进位点、不占 inflight。
func TestTagFilterDelivery(t *testing.T) {
	f := newFixture(t)
	f.sendTagged(t, "t", "a", "tagA") // offset 0
	f.sendTagged(t, "t", "b", "tagB") // offset 1
	f.sendTagged(t, "t", "c", "tagA") // offset 2

	flt, err := ParseTagFilter("tagA")
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, flt)
	if err != nil || len(msgs) != 2 || string(msgs[0].Body) != "a" || string(msgs[1].Body) != "c" {
		t.Fatalf("过滤投递: %d %v", len(msgs), err)
	}
	// b 已被位点跳过：即便换成全量过滤器也收不到（本组永久跳过）
	msgs, err = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("被过滤消息不应再投: %d %v", len(msgs), err)
	}
	// b 不占 inflight
	if _, ok, _ := f.st.Get(store.InflightKey("g", "t", 0, 1)); ok {
		t.Fatal("被过滤消息不应写 inflight")
	}
	// 另一消费组不受影响，能收到全部 3 条
	msgs, err = f.dl.Receive(context.Background(), "g2", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 3 {
		t.Fatalf("其他组应不受过滤影响: %d %v", len(msgs), err)
	}
}

// TestTagFilterAllFilteredAdvancesCursor 全部不匹配时位点仍须持久化推进，
// 否则每次 Receive 重复扫描同一批不匹配消息（性能退化 + 永不前进）。
func TestTagFilterAllFilteredAdvancesCursor(t *testing.T) {
	f := newFixture(t)
	f.sendTagged(t, "t", "x", "tagB")
	flt, _ := ParseTagFilter("tagA")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, flt)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("Receive: %d %v", len(msgs), err)
	}
	v, ok, err := f.st.Get(store.CursorKey("g", "t", 0))
	if err != nil || !ok || store.GetU64(v) != 1 {
		t.Fatalf("位点应推进到 1: ok=%v %v", ok, err)
	}
}

func TestRetryBackoffTable(t *testing.T) {
	cases := map[int32]time.Duration{2: 10 * time.Second, 3: 20 * time.Second, 4: 40 * time.Second, 30: 5 * time.Minute}
	for attempts, want := range cases {
		if got := retryBackoff(attempts); got != want {
			t.Fatalf("retryBackoff(%d) = %v，期望 %v", attempts, got, want)
		}
	}
}

// TestRedeliveryUsesBackoffFloor 重投时不可见时长取 max(客户端要求, 退避下限)。
func TestRedeliveryUsesBackoffFloor(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	// 首投 20ms 不可见，过期
	if msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, 20*time.Millisecond, 0, nil); len(msgs) != 1 {
		t.Fatal("首投失败")
	}
	time.Sleep(40 * time.Millisecond)
	// 第 2 次投递：客户端只要 20ms，但退避下限 10s 生效
	before := time.Now().UnixMilli()
	if msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, 20*time.Millisecond, 0, nil); len(msgs) != 1 {
		t.Fatal("重投失败")
	}
	v, ok, err := f.st.Get(store.InflightKey("g", "t", 0, 0))
	if err != nil || !ok {
		t.Fatalf("inflight 缺失: %v", err)
	}
	st2, err := core.DecodeInflight(v)
	if err != nil {
		t.Fatal(err)
	}
	if st2.ExpireAtMs-before < (10 * time.Second).Milliseconds() {
		t.Fatalf("退避下限未生效: expire 距 now 仅 %dms", st2.ExpireAtMs-before)
	}
}

// TestExhaustedAttemptsGoToDLQ 投递次数耗尽后转入 %DLQ%{group}，原队列不再投递。
func TestExhaustedAttemptsGoToDLQ(t *testing.T) {
	// 缩小退避基数，让第 2 次投递快速过期（var 供测试注入，见实现注释）
	oldBase := retryBackoffBase
	retryBackoffBase = 10 * time.Millisecond
	defer func() { retryBackoffBase = oldBase }()

	f := newFixtureMaxAttempts(t, 2)
	f.send(t, "t", "poison")
	// 第 1、2 次投递均不 ack、任其过期
	for i := 0; i < 2; i++ {
		msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 20*time.Millisecond, 0, nil)
		if err != nil || len(msgs) != 1 {
			t.Fatalf("第 %d 次投递: %d %v", i+1, len(msgs), err)
		}
		time.Sleep(60 * time.Millisecond) // > invisible 与退避的较大者
	}
	// 第 3 次 Receive 触发 DLQ 转入，原队列返回空
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("超限后原队列不应再投: %d %v", len(msgs), err)
	}
	// inflight 已删
	if _, ok, _ := f.st.Get(store.InflightKey("g", "t", 0, 0)); ok {
		t.Fatal("DLQ 转入后 inflight 未删除")
	}
	// 死信可从 %DLQ%g 消费，带来源属性
	dlq, err := f.dl.Receive(context.Background(), "dlq-reader", meta.DLQTopicName("g"), 0, 10, time.Minute, 0, nil)
	if err != nil || len(dlq) != 1 {
		t.Fatalf("DLQ 消费: %d %v", len(dlq), err)
	}
	if string(dlq[0].Body) != "poison" || dlq[0].Properties["sq-origin-topic"] != "t" {
		t.Fatalf("死信内容/来源属性不符: %s %v", dlq[0].Body, dlq[0].Properties)
	}
	// 死信要回答的第一个问题是「试了几次、为什么进来的」：投递次数（maxAttempts=2，
	// 第 2 次投递超限转入）与原因分类必须当场写进属性，而不是事后从日志里翻
	if dlq[0].Properties["sq-dlq-attempts"] != "2" {
		t.Fatalf("死信应带投递次数 2，得到 %q（props=%v）", dlq[0].Properties["sq-dlq-attempts"], dlq[0].Properties)
	}
	if dlq[0].Properties["sq-dlq-reason"] == "" {
		t.Fatalf("死信应带转入原因，得到 %q（props=%v）", dlq[0].Properties["sq-dlq-reason"], dlq[0].Properties)
	}
}

// 顺序消息一次只投一条：未 ack 时后续顺序消息全部被拦，ack 后放行下一条
func TestOrderedDeliversOneAtATime(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	f.sendGrouped(t, "t", "b", "g1")
	f.sendGrouped(t, "t", "c", "g1")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 || string(msgs[0].Body) != "a" {
		t.Fatalf("maxMsgs=10 也只能投第 1 条顺序消息: %d %v", len(msgs), err)
	}
	// 未 ack 期间再取：空（顺序锁占用）
	again, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(again) != 0 {
		t.Fatalf("未 ack 不应投出后续顺序消息: %d %v", len(again), err)
	}
	// ack 后放行下一条
	if _, err := f.dl.Ack(context.Background(), "g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(next) != 1 || string(next[0].Body) != "b" {
		t.Fatalf("ack 后应投第 2 条: %d %v", len(next), err)
	}
}

// 卡住语义：过期重投的还是队头那条（attempt 递增），绝不先投下一条；
// 且重投后顺序锁仍占用（Ordered 标记在重投时被保留）
func TestOrderedStuckOnExpiredRedelivery(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	f.sendGrouped(t, "t", "b", "g1")
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 30*time.Millisecond, 0, nil)
	if err != nil || len(first) != 1 || string(first[0].Body) != "a" {
		t.Fatalf("首投: %d %v", len(first), err)
	}
	time.Sleep(50 * time.Millisecond)
	red, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(red) != 1 || string(red[0].Body) != "a" || red[0].DeliveryAttempt != 2 {
		t.Fatalf("过期后应重投队头 a（attempt=2）而非跳到 b: %+v %v", red, err)
	}
	// 重投后 b 仍被拦（Ordered 标记未在重投中丢失）
	blocked, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("重投后 b 仍应被顺序锁拦住: %d %v", len(blocked), err)
	}
	// ack 重投那次的句柄（attempt=2）后放行 b
	if _, err := f.dl.Ack(context.Background(), "g", "t", 0, red[0].Offset, red[0].DeliveryAttempt); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil {
		t.Fatalf("ack 后取件: %v", err)
	}
	if len(next) != 1 || string(next[0].Body) != "b" {
		t.Fatalf("ack 后应投 b: %d", len(next))
	}
}

// 孤儿清理释放顺序锁：持有锁的顺序消息被 retention 清掉后（消息记录删除、
// inflight 残留），下一条顺序消息可投——五个解锁路径中唯一没直测的一条
func TestOrderedOrphanCleanupReleasesLock(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	f.sendGrouped(t, "t", "b", "g1")
	m, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, 30*time.Millisecond, 0, nil)
	if err != nil || len(m) != 1 || string(m[0].Body) != "a" {
		t.Fatalf("首投: %+v %v", m, err)
	}
	time.Sleep(50 * time.Millisecond) // 让 a 的 inflight 过期，进入重投候选
	// 模拟 retention：消息记录已删，仅 inflight 残留（store 无单条 Delete，
	// 与 TestOrphanInflightCleanupPersistsAndDoesNotReportDelivery 同用批次写法）
	b := f.st.NewBatch()
	b.Delete(store.MsgKey("t", 0, m[0].Offset))
	if err := f.st.Apply(b); err != nil {
		t.Fatalf("Delete msg: %v", err)
	}
	// 本轮 receiveOnce 应清理孤儿记录（Warn）并释放顺序锁，b 放行
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, nil)
	if err != nil || len(next) != 1 || string(next[0].Body) != "b" {
		t.Fatalf("孤儿清理后应投 b: %+v %v", next, err)
	}
}

// 顺序消息重投不设指数退避下限（spec §5 流程 6 退避仅限非顺序消息）：
// 用远小于 retryBackoffBase(10s) 的不可见时长连续重投，attempt 快速递增。
// 若误用退避下限，attempt=3 那次要等 10s，本用例会超时失败。
func TestOrderedRedeliveryNoBackoffFloor(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	m, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, 30*time.Millisecond, 0, nil)
	if err != nil || len(m) != 1 {
		t.Fatalf("首投: %d %v", len(m), err)
	}
	for want := int32(2); want <= 3; want++ {
		time.Sleep(50 * time.Millisecond)
		m, err = f.dl.Receive(context.Background(), "g", "t", 0, 1, 30*time.Millisecond, 0, nil)
		if err != nil || len(m) != 1 || m[0].DeliveryAttempt != want {
			t.Fatalf("顺序重投应立即可得（无退避下限），attempt 期望 %d: %+v %v", want, m, err)
		}
	}
}

// 超限转 DLQ 后队列解锁推进：卡住的消息进 %DLQ%{group}，下一条顺序消息可投
func TestOrderedExhaustedToDLQUnblocksQueue(t *testing.T) {
	f := newFixtureMaxAttempts(t, 2)
	f.sendGrouped(t, "t", "a", "g1")
	f.sendGrouped(t, "t", "b", "g1")
	// attempt 1、2 各一轮，均不 ack、放到过期
	for i := 0; i < 2; i++ {
		m, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, 30*time.Millisecond, 0, nil)
		if err != nil || len(m) != 1 || string(m[0].Body) != "a" {
			t.Fatalf("第 %d 轮应投 a: %+v %v", i+1, m, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// attempts 已达上限：本轮把 a 转 DLQ 并解锁，b 在同轮或下一轮可投
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, nil)
	if err != nil || len(next) != 1 || string(next[0].Body) != "b" {
		t.Fatalf("a 入 DLQ 后应投 b: %+v %v", next, err)
	}
	// DLQ 里恰有 a，且 MessageGroup 已清空（死信不再参与顺序，moveToDLQ 既有行为）
	dlq, err := f.dl.Receive(context.Background(), "g", meta.DLQTopicName("g"), 0, 10, time.Minute, 0, nil)
	if err != nil || len(dlq) != 1 || string(dlq[0].Body) != "a" || dlq[0].MessageGroup != "" {
		t.Fatalf("DLQ 内容不符: %+v %v", dlq, err)
	}
}

// 混发队列的队头阻塞语义（设计决策 3）：顺序消息被投出后，其后的普通消息
// 照常投（顺序锁只拦顺序消息）；被锁拦下的顺序消息则连同其后的一切消息
// 都取不到（位点停在它前面），ack 解锁后继续。
func TestMixedQueueHeadOfLineBlocking(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1") // offset 0
	f.send(t, "t", "n")              // offset 1，普通消息
	f.sendGrouped(t, "t", "c", "g1") // offset 2
	f.send(t, "t", "d")              // offset 3，普通消息，被 c 队头阻塞
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 2 || string(msgs[0].Body) != "a" || string(msgs[1].Body) != "n" {
		t.Fatalf("应投出 a 与 n，c/d 被拦: %+v %v", msgs, err)
	}
	// 位点停在 c（offset 2）之前——崩溃重启后 c 仍是下一条候选
	v, ok, err := f.st.Get(store.CursorKey("g", "t", 0))
	if err != nil || !ok || store.GetU64(v) != 2 {
		t.Fatalf("位点应停在被拦消息处（2）: %v %v", v, err)
	}
	if _, err := f.dl.Ack(context.Background(), "g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt); err != nil {
		t.Fatalf("Ack a: %v", err)
	}
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(next) != 2 || string(next[0].Body) != "c" || string(next[1].Body) != "d" {
		t.Fatalf("解锁后应投 c 与 d: %+v %v", next, err)
	}
}

// Tag 过滤先于顺序锁（设计决策 3）：不匹配的顺序消息永久跳过、推进位点，
// 不会被锁拦成永久堵塞
func TestOrderedTagFilteredStillSkipped(t *testing.T) {
	f := newFixture(t)
	if _, err := f.pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("a"), MessageGroup: "g1", Tag: "keep"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("b"), MessageGroup: "g1", Tag: "drop"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("c"), MessageGroup: "g1", Tag: "keep"}); err != nil {
		t.Fatal(err)
	}
	filter, err := ParseTagFilter("keep")
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, filter)
	if err != nil || len(msgs) != 1 || string(msgs[0].Body) != "a" {
		t.Fatalf("首投 a: %+v %v", msgs, err)
	}
	if _, err := f.dl.Ack(context.Background(), "g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt); err != nil {
		t.Fatal(err)
	}
	// b 不匹配被永久跳过，直接投 c
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, filter)
	if err != nil || len(next) != 1 || string(next[0].Body) != "c" {
		t.Fatalf("b 应被过滤跳过、投出 c: %+v %v", next, err)
	}
}

// countMsgs 统计某 topic 队列的消息条数（两段式重放用例断言死信条数用）。
func countMsgs(t *testing.T, st *store.Store, topic string, q uint32) int {
	t.Helper()
	n := 0
	lo := store.MsgQueuePrefix(topic, q)
	if err := st.Scan(lo, store.PrefixUpperBound(lo), 0, func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

// 显式转入 DLQ：inflight 删除、消息入 %DLQ%{group}、顺序锁释放
func TestForwardToDLQHappyPathUnblocksOrdered(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	f.sendGrouped(t, "t", "b", "g1")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("首投: %d %v", len(msgs), err)
	}
	ok, err := f.dl.ForwardToDLQ(context.Background(), "g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt)
	if err != nil || !ok {
		t.Fatalf("ForwardToDLQ: %v %v", ok, err)
	}
	// 顺序锁已释放：b 可投
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, nil)
	if err != nil || len(next) != 1 || string(next[0].Body) != "b" {
		t.Fatalf("forward 后应投 b: %+v %v", next, err)
	}
	// DLQ 里有 a
	dlq, err := f.dl.Receive(context.Background(), "g", meta.DLQTopicName("g"), 0, 10, time.Minute, 0, nil)
	if err != nil || len(dlq) != 1 || string(dlq[0].Body) != "a" {
		t.Fatalf("DLQ 内容不符: %+v %v", dlq, err)
	}
}

// 陈旧句柄幂等拒绝（语义与 Ack 的 attempt 校验一致）：不存在或 attempt
// 不匹配都返回 (false, nil)，不误伤重投后的新记录
func TestForwardToDLQStaleHandle(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, nil)
	if ok, err := f.dl.ForwardToDLQ(context.Background(), "g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt+1); ok || err != nil {
		t.Fatalf("attempt 不匹配应幂等拒绝: %v %v", ok, err)
	}
	if ok, err := f.dl.ForwardToDLQ(context.Background(), "g", "t", 0, 999, 1); ok || err != nil {
		t.Fatalf("不存在的 offset 应幂等拒绝: %v %v", ok, err)
	}
	// 原 inflight 未被误删：a 仍可 ack
	if ok, err := f.dl.Ack(context.Background(), "g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt); !ok || err != nil {
		t.Fatalf("原记录应完好: %v %v", ok, err)
	}
}

// TestDLQMoveRedelivers 两段式死信转入的崩溃窗口重放（经 ForwardToDLQ 协议
// 路径走 moveToDLQ 原语）：第一段（死信消息写入）后崩溃，源 inflight 未删 →
// 重放再次转入 → 死信区两条同 ID 条目（at-least-once 允许，消费端按 ID 幂等），
// 但绝不出现「inflight 删了死信却没有」（丢失）。
//
// 为什么用 ForwardToDLQ 而非超限重投路径触发：两者走同一 moveToDLQ；forward
// 的队列锁由 defer 释放，panic 恢复后无需手工解锁，崩溃模拟最干净。
func TestDLQMoveRedelivers(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "poison")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("首取: %d %v", len(msgs), err)
	}
	off, att := msgs[0].Offset, msgs[0].DeliveryAttempt
	f.dl.afterAppendHook = func() { panic("simulated crash between phases") }
	func() {
		defer func() { recover() }()
		f.dl.ForwardToDLQ(context.Background(), "g", "t", 0, off, att)
	}()
	// 崩溃后：死信已有 1 条，源 inflight 仍在
	dlqTopic := meta.DLQTopicName("g")
	if n := countMsgs(t, f.st, dlqTopic, 0); n != 1 {
		t.Fatalf("崩溃后死信应已有 1 条: %d", n)
	}
	if _, ok, _ := f.st.Get(store.InflightKey("g", "t", 0, off)); !ok {
		t.Fatal("崩溃后源 inflight 应残留")
	}
	// 重放 ForwardToDLQ：attempt 匹配 → 再次转入 → 死信 2 条、inflight 清空
	f.dl.afterAppendHook = nil
	ok, err := f.dl.ForwardToDLQ(context.Background(), "g", "t", 0, off, att)
	if err != nil || !ok {
		t.Fatalf("重放 ForwardToDLQ: ok=%v err=%v", ok, err)
	}
	if n := countMsgs(t, f.st, dlqTopic, 0); n != 2 {
		t.Fatalf("重放后死信应为 2 条（at-least-once 重复）: %d", n)
	}
	if _, ok, _ := f.st.Get(store.InflightKey("g", "t", 0, off)); ok {
		t.Fatal("重放后源 inflight 应已删除")
	}
}

// TestAckBatchMixedEntries 验证批量确认的 per-entry 语义（语义红线 4）：
// 有效、陈旧 attempt、不存在的 offset 混在一批——逐条独立判定，
// 只有有效条目被删除，落空条目不影响其它条目也不报错。
func TestAckBatchMixedEntries(t *testing.T) {
	f := newFixture(t)
	f.send(t, "ab-t", "m0")
	f.send(t, "ab-t", "m1")
	msgs, err := f.dl.Receive(context.Background(), "g", "ab-t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("receive: %v msgs=%d", err, len(msgs))
	}
	results, err := f.dl.AckBatch(context.Background(), "g", "ab-t", 0, []AckEntry{
		{Offset: msgs[0].Offset, Attempt: msgs[0].DeliveryAttempt}, // 有效
		{Offset: msgs[1].Offset, Attempt: 99},                      // 陈旧 attempt
		{Offset: 9999, Attempt: 1},                                 // 不存在
	})
	if err != nil {
		t.Fatalf("AckBatch: %v", err)
	}
	if len(results) != 3 || !results[0].OK || results[1].OK || results[2].OK {
		t.Fatalf("per-entry 结果错误: %+v", results)
	}
	// 有效条目的 inflight 已删；陈旧条目的 inflight 必须原样保留
	if _, ok, _ := f.st.Get(store.InflightKey("g", "ab-t", 0, msgs[0].Offset)); ok {
		t.Fatal("已确认消息的 inflight 未删除")
	}
	if _, ok, _ := f.st.Get(store.InflightKey("g", "ab-t", 0, msgs[1].Offset)); !ok {
		t.Fatal("陈旧句柄不应误删他人 inflight")
	}
}

// TestAckBatchAllInvalidNoWrite 验证全部落空时不产生任何写入（批次走
// 自行 Close 回收路径），且不报错。
func TestAckBatchAllInvalidNoWrite(t *testing.T) {
	f := newFixture(t)
	f.send(t, "ab2-t", "m")
	msgs, err := f.dl.Receive(context.Background(), "g", "ab2-t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v", err)
	}
	results, err := f.dl.AckBatch(context.Background(), "g", "ab2-t", 0, []AckEntry{
		{Offset: msgs[0].Offset, Attempt: 42}, // 陈旧
		{Offset: 8888, Attempt: 1},            // 不存在
	})
	if err != nil || results[0].OK || results[1].OK {
		t.Fatalf("全落空批应无错且全 false: %v %+v", err, results)
	}
	if _, ok, _ := f.st.Get(store.InflightKey("g", "ab2-t", 0, msgs[0].Offset)); !ok {
		t.Fatal("inflight 不应被触碰")
	}
}

// TestConcurrentReceiveAckNoRace 是拆分提交的核心回归：8 个 worker 并发
// 取件+确认同一队列，验证 (1) 每条消息恰好被投递并确认一次（invisible 足够长，
// 无重投）(2) 结束后 inflight 清零 (3) -race 干净。若拆锁破坏了
// inflight/cursor 读-改-写的互斥（语义红线 2），本测试在 -race 下必然暴露。
func TestConcurrentReceiveAckNoRace(t *testing.T) {
	f := newFixture(t)
	const total = 300
	for i := 0; i < total; i++ {
		f.send(t, "cc-t", "m")
	}
	var acked atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for acked.Load() < total {
				msgs, err := f.dl.Receive(context.Background(), "g", "cc-t", 0, 32, time.Minute, 0, nil)
				if err != nil {
					t.Errorf("Receive: %v", err)
					return
				}
				for _, m := range msgs {
					ok, err := f.dl.Ack(context.Background(), "g", "cc-t", 0, m.Offset, m.DeliveryAttempt)
					if err != nil {
						t.Errorf("Ack off=%d: %v", m.Offset, err)
						return
					}
					if ok {
						acked.Add(1)
					}
				}
				if len(msgs) == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
	if got := acked.Load(); got != total {
		t.Fatalf("确认总数 = %d, want %d（invisible 1 分钟内不应有重投）", got, total)
	}
	pfx := store.InflightPrefix("g", "cc-t", 0)
	n := 0
	if err := f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("全部确认后残留 %d 条 inflight", n)
	}
}
