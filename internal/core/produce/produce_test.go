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
	"context"
	"hash/fnv"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

func newTestProducer(t *testing.T, dir string) (*Producer, *store.Store) {
	t.Helper()
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	mt, err := meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 4, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	return New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, mt, slog.Default()), st
}

func TestAppendAssignsMonotonicOffsets(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	seen := map[uint32]uint64{} // queueID -> 上一个 offset
	for i := 0; i < 20; i++ {
		m, err := p.Append(context.Background(), &core.Message{Topic: "t1", Body: []byte("x")})
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
		m, err := p.Append(context.Background(), &core.Message{Topic: "t1", Body: []byte("x")})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		before[m.QueueID] = m.Offset
	}
	st.Close()

	p2, st2 := newTestProducer(t, dir)
	defer st2.Close()
	m, err := p2.Append(context.Background(), &core.Message{Topic: "t1", Body: []byte("y")})
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
		m, err := p.Append(context.Background(), &core.Message{Topic: "t2", Body: []byte("x"), MessageGroup: "user-1"})
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
	if _, err := p.Append(context.Background(), &core.Message{Topic: "t3", Body: []byte("x")}); err != nil {
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
	m, err := pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("x"), Keys: []string{"k1", "k2"}})
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

func TestAppendConcurrentNoDupNoHole(t *testing.T) {
	// Producer 类型注释声称「并发安全」，此用例在 -race 下钉住它，并断言
	// offset 分配无重复无空洞、alloc 计数器与消息数严格一致。
	// Task 8 改队列粒度锁后本用例必须原样通过（等价重构的证明）。
	p, st := newTestProducer(t, t.TempDir()) // 既有 fixture：sync store + autoCreate(4 队列)
	defer st.Close()
	const goroutines, perG = 8, 50
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perG)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				m := &core.Message{Topic: "t-conc", Body: []byte("x")}
				if _, err := p.Append(context.Background(), m); err != nil {
					errs <- err
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// 逐队列扫描 msg/ 前缀：offset 必须恰为 0..count-1（无重复无空洞），
	// 且 alloc 计数器 == count；四队列 count 总和 == goroutines*perG
	total := 0
	for q := uint32(0); q < 4; q++ {
		count := 0
		prev := int64(-1)
		// 队列区间 = [MsgKey(t,q,0), MsgKey(t,q+1,0))：MsgKey 的 queueID/offset
		// 均为定长大端编码，字节序即数值序，因此该区间恰好覆盖队列 q 的全部消息
		err := st.Scan(store.MsgKey("t-conc", q, 0), store.MsgKey("t-conc", q+1, 0), 0,
			func(k, v []byte) (bool, error) {
				_, _, off, perr := store.ParseMsgKey(k)
				if perr != nil {
					return false, perr
				}
				if int64(off) != prev+1 {
					t.Fatalf("q%d offset 不连续: prev=%d got=%d", q, prev, off)
				}
				prev = int64(off)
				count++
				return true, nil
			})
		if err != nil {
			t.Fatal(err)
		}
		v, ok, _ := st.Get(store.AllocKey("t-conc", q))
		if count > 0 && (!ok || store.GetU64(v) != uint64(count)) {
			t.Fatalf("q%d alloc 计数器与消息数不一致", q)
		}
		total += count
	}
	if total != goroutines*perG {
		t.Fatalf("总量 = %d, 期望 %d", total, goroutines*perG)
	}
}

// sendDelay 构造一条延时消息并 AppendDelay（测试辅助）
func sendDelay(t *testing.T, p *Producer, topic string, body string, dueMs int64) *core.Message {
	t.Helper()
	m, _, _, err := p.AppendDelay(context.Background(), &core.Message{Topic: topic, Body: []byte(body), DeliverAtMs: dueMs})
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
	if _, _, _, err := p.AppendDelay(context.Background(), &core.Message{Topic: "t", Body: nil, DeliverAtMs: time.Now().Add(time.Hour).UnixMilli()}); err == nil {
		t.Fatal("空 body 应拒绝")
	}
	if _, _, _, err := p.AppendDelay(context.Background(), &core.Message{Topic: "t", Body: []byte("x"), DeliverAtMs: 0}); err == nil {
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

// TestAppendConcurrentSameGroupOffsetsContiguous 是 group commit 解锁后的 FIFO
// 回归测试：并发写同一 MessageGroup（固定落同一队列），验证 offset 恰为
// 0..N-1 连续无洞无重、alloc 计数器与消息严格一致。解锁改动若破坏
// 「offset 顺序 == 落盘顺序」的临界区，本测试在 -race 下必然暴露。
func TestAppendConcurrentSameGroupOffsetsContiguous(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	const goroutines, perG = 8, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := p.Append(context.Background(), &core.Message{Topic: "fifo-cc", MessageGroup: "g1", Body: []byte("x")}); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	// 用与实现相同的 fnv 哈希算出该 group 固定落的队列号（fixture 为 4 队列）
	h := fnv.New32a()
	h.Write([]byte("g1"))
	qid := h.Sum32() % 4
	total := uint64(goroutines * perG)
	v, ok, err := st.Get(store.AllocKey("fifo-cc", qid))
	if err != nil || !ok {
		t.Fatalf("alloc 计数器缺失: ok=%v err=%v", ok, err)
	}
	if got := store.GetU64(v); got != total {
		t.Fatalf("alloc 计数器 = %d, want %d", got, total)
	}
	for off := uint64(0); off < total; off++ {
		if _, ok, err := st.Get(store.MsgKey("fifo-cc", qid, off)); err != nil || !ok {
			t.Fatalf("offset %d 消息缺失: ok=%v err=%v", off, ok, err)
		}
	}
}

// TestAppendBatchContiguousSameQueue 验证批量写入核心不变式：整批同队列、
// offset 连续段 [off, off+N)、alloc 计数器一次推进到 off+N、keys 索引齐全。
func TestAppendBatchContiguousSameQueue(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	msgs := make([]*core.Message, 5)
	for i := range msgs {
		msgs[i] = &core.Message{Topic: "tb", Body: []byte("x"), Keys: []string{"k-idx"}}
	}
	stored, err := p.AppendBatch(context.Background(), msgs)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if len(stored) != 5 {
		t.Fatalf("返回 %d 条, want 5", len(stored))
	}
	qid := stored[0].QueueID
	for i, m := range stored {
		if m.QueueID != qid {
			t.Fatalf("第 %d 条队列 %d != 首条队列 %d（整批必须同队列）", i, m.QueueID, qid)
		}
		if m.Offset != stored[0].Offset+uint64(i) {
			t.Fatalf("第 %d 条 offset %d 不连续（首条 %d）", i, m.Offset, stored[0].Offset)
		}
		if m.ID == "" {
			t.Fatalf("第 %d 条未回填 ID", i)
		}
		if _, ok, err := st.Get(store.MsgKey("tb", qid, m.Offset)); err != nil || !ok {
			t.Fatalf("offset %d 消息未落盘: ok=%v err=%v", m.Offset, ok, err)
		}
	}
	v, ok, err := st.Get(store.AllocKey("tb", qid))
	if err != nil || !ok || store.GetU64(v) != stored[4].Offset+1 {
		t.Fatalf("alloc 计数器 = %v ok=%v err=%v, want %d", v, ok, err, stored[4].Offset+1)
	}
}

// TestAppendBatchRejectsSpecialMessages 验证防御校验：事务/延时/FIFO 消息
// 不允许进批量路径（路由约束在 rpc 层，此处防御手写调用方）。
func TestAppendBatchRejectsSpecialMessages(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	cases := []*core.Message{
		{Topic: "tb2", Body: []byte("x"), MessageGroup: "g"},
		{Topic: "tb2", Body: []byte("x"), DeliverAtMs: time.Now().UnixMilli() + 60_000},
		{Topic: "tb2", Body: []byte("x"), Transactional: true},
	}
	for i, special := range cases {
		if _, err := p.AppendBatch(context.Background(), []*core.Message{{Topic: "tb2", Body: []byte("x")}, special}); err == nil {
			t.Fatalf("case %d: 含特殊消息的批应报错", i)
		}
	}
	if _, err := p.AppendBatch(context.Background(), nil); err == nil {
		t.Fatal("空批应报错")
	}
}

// TestAppendDelayConcurrentSeqUnique 并发写延时消息，验证 seq 不重不漏：
// delay 条目总数与 delayalloc 计数器严格一致。拆分提交若破坏 seq 分配的
// 临界区（提前推进逻辑写错），本测试必挂。
func TestAppendDelayConcurrentSeqUnique(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	due := time.Now().Add(time.Hour).UnixMilli()
	const goroutines, perG = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, _, _, err := p.AppendDelay(context.Background(), &core.Message{Topic: "dly-cc", Body: []byte("x"), DeliverAtMs: due}); err != nil {
					t.Errorf("AppendDelay: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	total := uint64(goroutines * perG)
	n := 0
	pfx := []byte(store.DelayPrefix)
	if err := st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if uint64(n) != total {
		t.Fatalf("delay 条目 = %d, want %d（seq 撞号会覆盖变少）", n, total)
	}
	v, ok, err := st.Get(store.DelayAllocKey())
	if err != nil || !ok || store.GetU64(v) != total {
		t.Fatalf("delayalloc = %v ok=%v err=%v, want %d", v, ok, err, total)
	}
}

// TestAppendBatchAtomicOnInvalidBody 验证整批原子性：批内任一条 body 非法时
// 整批拒绝、零落盘（alloc 计数器不存在、无任何消息 key）。
func TestAppendBatchAtomicOnInvalidBody(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	msgs := []*core.Message{
		{Topic: "tb3", Body: []byte("ok")},
		{Topic: "tb3", Body: nil}, // 非法：空 body
	}
	if _, err := p.AppendBatch(context.Background(), msgs); err == nil {
		t.Fatal("含空 body 的批应报错")
	}
	// 4 个队列全部确认零落盘
	for q := uint32(0); q < 4; q++ {
		if _, ok, _ := st.Get(store.AllocKey("tb3", q)); ok {
			t.Fatalf("队列 %d 的 alloc 计数器不应存在（整批应零落盘）", q)
		}
	}
}

// TestAppendDelayReportsSeqAndStaged 钉住新签名的两条语义：
// 真进暂存区时 staged=true 且 seq 是该条目的分配值（据此能拼出 DelayKey）；
// 已到期直通 Append 时 staged=false。
//
// 为什么必须有 staged 这个布尔而不能用 seq==0 当哨兵：延时 seq 从 0 开始
// 分配（nextDelaySeqLocked 在计数器不存在时返回 0），所以 0 是一个**合法**
// 的 seq——空库里第一条延时消息的 seq 就是 0。用 0 当"未暂存"的哨兵会让
// 这条消息永远签不出句柄。
func TestAppendDelayReportsSeqAndStaged(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	due := time.Now().Add(time.Hour).UnixMilli()

	m, seq, staged, err := p.AppendDelay(context.Background(),
		&core.Message{Topic: "dly-seq", Body: []byte("x"), DeliverAtMs: due})
	if err != nil {
		t.Fatalf("AppendDelay: %v", err)
	}
	if !staged {
		t.Fatalf("due 在一小时后，应进暂存区（staged=true）")
	}
	// 空库第一条：seq 必须是 0，且 DelayKey(due, seq) 必须真的存在
	if _, ok, err := st.Get(store.DelayKey(due, seq)); err != nil || !ok {
		t.Fatalf("DelayKey(due=%d, seq=%d) 不存在（ok=%v err=%v）——seq 报错了", due, seq, ok, err)
	}
	if m.ID == "" {
		t.Fatalf("返回消息缺 ID")
	}

	// 已到期：直通 Append，不进暂存区
	past := time.Now().Add(-time.Second).UnixMilli()
	_, _, staged2, err := p.AppendDelay(context.Background(),
		&core.Message{Topic: "dly-seq", Body: []byte("y"), DeliverAtMs: past})
	if err != nil {
		t.Fatalf("AppendDelay(已到期): %v", err)
	}
	if staged2 {
		t.Fatalf("due 已过，应直通 Append（staged=false）")
	}
}
