// readbarrier_test.go 验证读屏障的三条不变量：跨节点 id 校验、回流即放行
// vs 挂起等追平、排队成批合流不复用早于自己的 readIndex。
package cluster

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
)

// testLogger 构造丢弃输出但保留级别过滤的测试日志器（包内测试共享）。
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestReadWaiterInfoRejectsForeignProposer 读状态的 RequestCtx 与提案头
// 同布局：[8B 提案者][8B waiter id]。waiter id 是每节点独立计数器，跨
// 节点必然撞车——提案者不是本节点时必须拒绝，否则别人的读状态会唤醒
// 我方等待者，屏障形同虚设。
func TestReadWaiterInfoRejectsForeignProposer(t *testing.T) {
	rctx := make([]byte, 16)
	binary.BigEndian.PutUint64(rctx[:8], 7)
	binary.BigEndian.PutUint64(rctx[8:], 42)

	if id, ok := readWaiterInfo(rctx, 7); !ok || id != 42 {
		t.Fatalf("本节点提案的读状态应被识别，得到 id=%d ok=%v", id, ok)
	}
	if _, ok := readWaiterInfo(rctx, 8); ok {
		t.Fatal("提案者非本节点的读状态不得识别（跨节点 id 撞车会假放行）")
	}
	if _, ok := readWaiterInfo(rctx[:15], 7); ok {
		t.Fatal("长度不足 16B 的 RequestCtx 不得识别")
	}
}

// TestStepReadStatesFillsIndexAndNotifiesWhenCaughtUp 读状态回流时：
// applied 已追过 → 立即放行；未追上 → 记下 index 挂起，等 apply 推进
// 后由 notifyReadWaiters 放行。
func TestStepReadStatesFillsIndexAndNotifiesWhenCaughtUp(t *testing.T) {
	gr := &group{selfID: 1, readWaiters: map[uint64]*readWait{}, lg: testLogger()}

	// ① applied 已追过：回流即放行
	gr.applied.Store(100)
	done := make(chan struct{})
	gr.readWaiters[1] = &readWait{ch: done}
	gr.stepReadStates([]raft.ReadState{{Index: 90, RequestCtx: readCtxBytes(1, 1)}})
	select {
	case <-done:
	default:
		t.Fatal("applied(100) 已追过 readIndex(90)，应立即放行")
	}
	if len(gr.readWaiters) != 0 {
		t.Fatalf("放行后 waiter 应被摘除，残留 %d 个", len(gr.readWaiters))
	}

	// ② applied 未追上：挂起，等 apply 推进后放行
	pending := make(chan struct{})
	gr.readWaiters[2] = &readWait{ch: pending}
	gr.stepReadStates([]raft.ReadState{{Index: 150, RequestCtx: readCtxBytes(1, 2)}})
	select {
	case <-pending:
		t.Fatal("applied(100) 未追到 readIndex(150)，不应放行")
	default:
	}
	gr.applied.Store(150)
	gr.notifyReadWaiters(150)
	select {
	case <-pending:
	default:
		t.Fatal("applied 追平后应放行")
	}
	if len(gr.readWaiters) != 0 {
		t.Fatalf("放行后 waiter 应被摘除，残留 %d 个", len(gr.readWaiters))
	}
}

// readCtxBytes 构造与提案头同布局的 RequestCtx（测试辅助）。
func readCtxBytes(self, id uint64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], self)
	binary.BigEndian.PutUint64(b[8:], id)
	return b
}

// TestReadBarrierBatchesQueueNextRound 合流的正确性红线：一轮在途时，
// 后到的等待者必须排进下一批，绝不复用当前这一轮的结果——当前轮的
// readIndex 取自后到者发起之前，中间被确认的写它看不到。
func TestReadBarrierBatchesQueueNextRound(t *testing.T) {
	gr := &group{selfID: 1, readWaiters: map[uint64]*readWait{}, lg: testLogger()}
	gr.lifeCtx = context.Background()
	gr.barrierTimeout = time.Second

	rounds := make(chan chan error, 4) // 每轮把自己的「放行开关」交出来
	gr.readIndexFn = func(ctx context.Context) error {
		release := make(chan error, 1)
		rounds <- release
		select {
		case err := <-release:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	first := make(chan error, 1)
	go func() { first <- gr.readBarrier(context.Background()) }()
	r1 := <-rounds // 第一轮已在途

	// 第一轮在途期间到达的等待者：必须触发第二轮，而不是搭第一轮的车
	second := make(chan error, 1)
	go func() { second <- gr.readBarrier(context.Background()) }()
	select {
	case <-rounds:
		t.Fatal("第一轮尚未结束就发起了第二轮（应排队等待）")
	case <-time.After(100 * time.Millisecond):
	}

	r1 <- nil
	if err := <-first; err != nil {
		t.Fatalf("第一轮应成功，得到 %v", err)
	}
	// 第一轮结束后，排队的等待者必须触发一轮**新的** read-index
	select {
	case r2 := <-rounds:
		r2 <- nil
	case <-time.After(2 * time.Second):
		t.Fatal("第一轮结束后未为排队等待者发起第二轮 read-index")
	}
	if err := <-second; err != nil {
		t.Fatalf("第二轮应成功，得到 %v", err)
	}
}

// TestReadBarrierBatchSharesOneRound 同一批（一轮收割之前同时到达）的
// 等待者共享一次 read-index：读 QPS 再高，在途轮次恒为 1。
//
// 为什么用「起跑线 + 首轮已发起的信号」而不是轮询 batchSize：收割
// goroutine 会在静默窗（barrierQuiesce）结束后把批取走，轮询 batchSize
// 撞上「批已取走、新批只有 1 人」的窗口必然读到 1。起跑线让 8 个读者
// 在同一瞬间入队，静默窗保证收割发生在最后一名入队之后；「首轮已发起」
// 信号在第一批被收割后才触发，此时 8 名成员必然都在批内。
func TestReadBarrierBatchSharesOneRound(t *testing.T) {
	gr := &group{selfID: 1, readWaiters: map[uint64]*readWait{}, lg: testLogger()}
	gr.lifeCtx = context.Background()
	gr.barrierTimeout = time.Second

	var calls atomic.Int64
	started := make(chan struct{}) // 第一轮 read-index 已发起（批已收割）
	gate := make(chan struct{})
	gr.readIndexFn = func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-gate
		return nil
	}

	const n = 8
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			<-start
			errs <- gr.readBarrier(context.Background())
		}()
	}
	close(start) // 起跑线：8 个读者在同一瞬间到达
	// 等第一轮发起：静默窗保证此时 8 名成员都已入队
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("第一轮 read-index 未发起")
	}
	close(gate)
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("等待者 %d 应成功，得到 %v", i, err)
		}
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("同一批应只走一次 read-index，实走 %d 次", c)
	}
}
