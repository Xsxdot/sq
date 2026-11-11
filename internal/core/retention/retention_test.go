// retention 清理任务测试：直接向 store 注入带旧 StoreAtMs 的消息制造过期数据
// （produce.Append 总用当前时间，无法造旧数据）。
package retention

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/deliver"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/core/query"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

func newFixture(t *testing.T) (*store.Store, *meta.Meta) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	return st, mt
}

// newFixtureRouted 同 newFixture，额外返回单机复制抽象（retention 构造注入用）。
func newFixtureRouted(t *testing.T) (*store.Store, *meta.Meta, *replication.Standalone, replication.StandaloneRouter) {
	t.Helper()
	st, mt := newFixture(t)
	return st, mt, replication.NewStandalone(st), replication.StandaloneRouter{}
}

// writeMsgAt 以指定 StoreAtMs 直写一条消息（含 alloc 计数器与 keyidx，模拟真实写入）。
func writeMsgAt(t *testing.T, st *store.Store, topic string, offset uint64, storeAt int64, keys ...string) {
	t.Helper()
	m := &core.Message{ID: core.NewMessageID(), Topic: topic, QueueID: 0, Offset: offset,
		Keys: keys, Body: []byte("x"), BornAtMs: storeAt, StoreAtMs: storeAt}
	raw, err := core.EncodeMessage(m)
	if err != nil {
		t.Fatal(err)
	}
	b := st.NewBatch()
	b.Set(store.MsgKey(topic, 0, offset), raw)
	b.Set(store.AllocKey(topic, 0), store.PutU64(offset+1))
	for _, k := range keys {
		b.Set(store.KeyIdxKey(topic, k, storeAt, 0, offset), nil)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

// fixedLeaderRouter 可编程 Router：IsLeader 按字段返回（leader-only 门控单测用）。
type fixedLeaderRouter struct{ leader bool }

func (r fixedLeaderRouter) GroupForQueue(string, uint32) uint32       { return 0 }
func (r fixedLeaderRouter) MetaGroup() uint32                         { return 0 }
func (r fixedLeaderRouter) IsLeader(uint32) bool                      { return r.leader }
func (r fixedLeaderRouter) ReadBarrier(context.Context, uint32) error { return nil }

// TestPassPurgesExpired 过期消息与索引被清，未过期保留。
func TestPassPurgesExpired(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	if _, err := mt.CreateTopic(context.Background(), "t", 1); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli() // 超过默认 3 天
	fresh := time.Now().UnixMilli()
	writeMsgAt(t, st, "t", 0, old, "k-old")
	writeMsgAt(t, st, "t", 1, fresh, "k-new")

	m := New(rep, rt, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())
	n, _, err := m.Pass(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("Pass: %d %v", n, err)
	}
	if _, ok, _ := st.Get(store.MsgKey("t", 0, 0)); ok {
		t.Fatal("过期消息未清理")
	}
	if _, ok, _ := st.Get(store.MsgKey("t", 0, 1)); !ok {
		t.Fatal("未过期消息被误删")
	}
	// 索引：旧删新留
	oldIdx := store.KeyIdxKeyPrefix("t", "k-old")
	gone := true
	st.Scan(oldIdx, store.PrefixUpperBound(oldIdx), 0, func(k, v []byte) (bool, error) { gone = false; return false, nil })
	if !gone {
		t.Fatal("过期索引未清理")
	}
	newIdx := store.KeyIdxKeyPrefix("t", "k-new")
	kept := false
	st.Scan(newIdx, store.PrefixUpperBound(newIdx), 0, func(k, v []byte) (bool, error) { kept = true; return false, nil })
	if !kept {
		t.Fatal("未过期索引被误删")
	}
}

// TestPassIdempotentAndNoExpired 无过期数据时 Pass 是无害空转。
func TestPassIdempotentAndNoExpired(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	mt.CreateTopic(context.Background(), "t", 1)
	writeMsgAt(t, st, "t", 0, time.Now().UnixMilli())
	m := New(rep, rt, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())
	for i := 0; i < 2; i++ {
		if n, _, err := m.Pass(context.Background()); err != nil || n != 0 {
			t.Fatalf("第 %d 次 Pass: %d %v", i+1, n, err)
		}
	}
}

// TestConsumeAfterPurge 清理后消费从位点扫描自然越过已删区间（cursor 无需修正）。
func TestConsumeAfterPurge(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	mt.CreateTopic(context.Background(), "t", 1)
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli()
	writeMsgAt(t, st, "t", 0, old)
	writeMsgAt(t, st, "t", 1, time.Now().UnixMilli())
	if _, _, err := New(rep, rt, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default()).Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	dl := deliver.New(rep, rt, st, mt, pr, slog.Default())
	msgs, err := dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, deliver.AllPass)
	if err != nil || len(msgs) != 1 || msgs[0].Offset != 1 {
		t.Fatalf("清理后消费: %d %v", len(msgs), err)
	}
}

// TestPassSkipsNonLedQueues 非 leader 组队列的过期消息不被清理（返回
// 0 且 processable=0——Run 层「本趟 0 队列可处理」的判定依据）；leader
// 时照常清理。
func TestPassSkipsNonLedQueues(t *testing.T) {
	st, mt, rep, _ := newFixtureRouted(t)
	if _, err := mt.CreateTopic(context.Background(), "t", 1); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli()
	writeMsgAt(t, st, "t", 0, old)
	// 非 leader 路由：队列组不 lead → 过期消息不清理
	m := New(rep, fixedLeaderRouter{leader: false}, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())
	n, proc, err := m.Pass(context.Background())
	if err != nil || n != 0 || proc != 0 {
		t.Fatalf("非 leader Pass: n=%d proc=%d err=%v; want 0,0,nil", n, proc, err)
	}
	if _, ok, _ := st.Get(store.MsgKey("t", 0, 0)); !ok {
		t.Fatal("非 leader 队列的过期消息不应被清理")
	}
	// leader 路由：照常清理
	m = New(rep, fixedLeaderRouter{leader: true}, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())
	n, proc, err = m.Pass(context.Background())
	if err != nil || n != 1 || proc != 1 {
		t.Fatalf("leader Pass: n=%d proc=%d err=%v; want 1,1,nil", n, proc, err)
	}
	if _, ok, _ := st.Get(store.MsgKey("t", 0, 0)); ok {
		t.Fatal("leader 队列的过期消息应被清理")
	}
}

// TestRunStopsOnCancel Run 循环响应 ctx 取消退出（停机路径）。
func TestRunStopsOnCancel(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	m := New(rep, rt, st, mt, time.Hour, t.TempDir(), 0, nil, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未响应取消")
	}
}

// countPrefix 统计前缀下的键数（索引存在性断言用）。
func countPrefix(t *testing.T, st *store.Store, pfx []byte) int {
	t.Helper()
	n := 0
	if err := st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestPurgeDeletesKeyIndexSameBatch S1：经真实 produce.Append 写入带 key 的消息，
// 保留期缩短后一趟 Pass 同时删掉消息与其全部索引；对照 topic 原样保留。
func TestPurgeDeletesKeyIndexSameBatch(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	ctx := context.Background()
	pr := produce.New(rep, rt, st, mt, slog.Default())
	write := func(topic string, keys ...string) {
		t.Helper()
		if _, err := pr.Append(ctx, &core.Message{Topic: topic, Body: []byte("x"), Keys: keys}); err != nil {
			t.Fatal(err)
		}
	}
	write("t", "oid")
	write("t", "oid", "oid") // 同一消息内重复 key
	write("t", "a/b", "oid") // 含 '/' 的 key
	write("t", "solo")
	write("keep", "oid", "a/b") // 对照 topic：默认保留期
	if _, err := mt.UpdateTopicRetention(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	m := New(rep, rt, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())
	n, _, err := m.Pass(ctx)
	if err != nil || n != 4 {
		t.Fatalf("Pass: purged=%d err=%v; want 4", n, err)
	}
	for _, k := range []string{"oid", "a/b", "solo"} {
		got, err := query.ByKey(st, "t", k, 0)
		if err != nil || len(got) != 0 {
			t.Fatalf("ByKey(t,%q) = %d 条 %v; want 0", k, len(got), err)
		}
	}
	if c := countPrefix(t, st, store.KeyIdxTopicPrefix("t")); c != 0 {
		t.Fatalf("topic t 索引残留 %d 条; want 0", c)
	}
	if c := countPrefix(t, st, store.KeyIdxTopicPrefix("keep")); c != 2 {
		t.Fatalf("对照 topic keep 索引 %d 条; want 2", c)
	}
	for _, k := range []string{"oid", "a/b"} {
		if got, err := query.ByKey(st, "keep", k, 0); err != nil || len(got) != 1 {
			t.Fatalf("ByKey(keep,%q) = %d 条 %v; want 1", k, len(got), err)
		}
	}
}

// writeMsgsBatch 单批直写 queue 0 上 [from, from+count) 的消息，keysOf 给出每条的 key
// （大量数据夹具用：逐条 Apply 会被 fsync 拖慢）。
func writeMsgsBatch(t *testing.T, st *store.Store, topic string, from uint64, count int, storeAt int64, keysOf func(i int) []string) {
	t.Helper()
	b := st.NewBatch()
	for i := 0; i < count; i++ {
		off := from + uint64(i)
		keys := keysOf(i)
		m := &core.Message{ID: core.NewMessageID(), Topic: topic, QueueID: 0, Offset: off,
			Keys: keys, Body: []byte("x"), BornAtMs: storeAt, StoreAtMs: storeAt}
		raw, err := core.EncodeMessage(m)
		if err != nil {
			t.Fatal(err)
		}
		b.Set(store.MsgKey(topic, 0, off), raw)
		for _, k := range keys {
			b.Set(store.KeyIdxKey(topic, k, storeAt, 0, off), nil)
		}
	}
	b.Set(store.AllocKey(topic, 0), store.PutU64(from+uint64(count)))
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

// longKey 构造长度为 n 的 key，前缀区分不同 key。
func longKey(prefix string, n int) string {
	return prefix + strings.Repeat("k", n-len(prefix))
}

// TestPurgeBudgetStopsAtMessageBoundary I1：索引删除字节预算只在消息边界截停——
// 每条消息的索引约占预算 1/3，单趟删不完；任一时刻消息与其全部索引同在或同无。
func TestPurgeBudgetStopsAtMessageBoundary(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	ctx := context.Background()
	if _, err := mt.CreateTopic(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
	const N = 7
	keyLen := maxIdxDeleteBytes / 6 // 每条 2 个 key ≈ 预算 1/3
	keysOf := func(i int) []string {
		return []string{longKey(fmt.Sprintf("m%d-a-", i), keyLen), longKey(fmt.Sprintf("m%d-b-", i), keyLen)}
	}
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli()
	writeMsgsBatch(t, st, "t", 0, N, old, keysOf)

	m := New(rep, rt, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())
	check := func() int {
		t.Helper()
		gone := 0
		for i := 0; i < N; i++ {
			_, msgOK, _ := st.Get(store.MsgKey("t", 0, uint64(i)))
			for _, k := range keysOf(i) {
				_, idxOK, _ := st.Get(store.KeyIdxKey("t", k, old, 0, uint64(i)))
				if idxOK != msgOK {
					t.Fatalf("消息 %d 存在=%v 但其索引存在=%v：消息与索引被拆批", i, msgOK, idxOK)
				}
			}
			if !msgOK {
				gone++
			}
		}
		return gone
	}
	total := 0
	for pass := 1; ; pass++ {
		n, _, err := m.Pass(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if pass == 1 && (n == 0 || n >= N) {
			t.Fatalf("首趟删除 %d 条; want 0 < n < %d（预算应截停）", n, N)
		}
		total += n
		if g := check(); g != total {
			t.Fatalf("第 %d 趟后已删 %d 条，累计返回 %d", pass, g, total)
		}
		if total == N {
			break
		}
		if pass > N {
			t.Fatal("多趟后仍未删完：无进展")
		}
	}
}

// TestPurgeSingleMessageOverBudget I2：单条消息的索引字节超预算也必须被删（保证进展）。
func TestPurgeSingleMessageOverBudget(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	ctx := context.Background()
	if _, err := mt.CreateTopic(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli()
	big := longKey("big-", maxIdxDeleteBytes+1)
	writeMsgsBatch(t, st, "t", 0, 1, old, func(int) []string { return []string{big} })
	m := New(rep, rt, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())
	n, _, err := m.Pass(ctx)
	if err != nil || n != 1 {
		t.Fatalf("Pass: purged=%d err=%v; want 1", n, err)
	}
	if _, ok, _ := st.Get(store.KeyIdxKey("t", big, old, 0, 0)); ok {
		t.Fatal("超预算消息的索引未删")
	}
}

// keyIdxReadSpy 包装 *store.Store 的读路径，统计读到 keyidx 前缀的键数
// （Scan 回调交出的键 + Get 的键）。与 retention 自身记账无关：retention 的
// 一切存储读都经 Manager.st，注入本包装后任何 keyidx 读都会被计入。
type keyIdxReadSpy struct {
	*store.Store
	reads atomic.Int64
}

// keyIdxRoot keyidx 键族根前缀（由公开拼键函数推出，避免硬编码）。
var keyIdxRoot = strings.TrimSuffix(string(store.KeyIdxTopicPrefix("x")), "x/")

func (s *keyIdxReadSpy) Scan(lower, upper []byte, limit int, fn func(k, v []byte) (bool, error)) error {
	return s.Store.Scan(lower, upper, limit, func(k, v []byte) (bool, error) {
		if strings.HasPrefix(string(k), keyIdxRoot) {
			s.reads.Add(1)
		}
		return fn(k, v)
	})
}

func (s *keyIdxReadSpy) Get(key []byte) ([]byte, bool, error) {
	if strings.HasPrefix(string(key), keyIdxRoot) {
		s.reads.Add(1)
	}
	return s.Store.Get(key)
}

// spyOn 给 Manager 注入读观测包装并返回它。
func spyOn(m *Manager, st *store.Store) *keyIdxReadSpy {
	spy := &keyIdxReadSpy{Store: st}
	m.st = spy
	return spy
}

// TestSteadyPassScansNoKeyIndex S2：稳态清理不读 keyidx——2 万条未过期带 key
// 消息 + 1 条过期带 key 消息，一趟 Pass 经存储读到的 keyidx 键数为 0（外部包装
// 观测），过期那条的索引已删。
func TestSteadyPassScansNoKeyIndex(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	ctx := context.Background()
	if _, err := mt.CreateTopic(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli()
	writeMsgAt(t, st, "t", 0, old, "k-old")
	const fresh = 20000
	writeMsgsBatch(t, st, "t", 1, fresh, time.Now().UnixMilli(), func(i int) []string {
		return []string{fmt.Sprintf("k-%05d", i)}
	})
	m := New(rep, rt, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())
	spy := spyOn(m, st)
	n, _, err := m.Pass(ctx)
	if err != nil || n != 1 {
		t.Fatalf("Pass: purged=%d err=%v; want 1", n, err)
	}
	if got := spy.reads.Load(); got != 0 {
		t.Fatalf("稳态 Pass 读到 keyidx %d 个键; want 0", got)
	}
	if got := m.passIdxDeleted.Load(); got != 1 {
		t.Fatalf("Pass 同批删除索引 %d 条; want 1", got)
	}
	if c := countPrefix(t, st, store.KeyIdxKeyPrefix("t", "k-old")); c != 0 {
		t.Fatalf("过期消息索引残留 %d 条", c)
	}
	if c := countPrefix(t, st, store.KeyIdxTopicPrefix("t")); c != fresh {
		t.Fatalf("未过期索引 %d 条; want %d", c, fresh)
	}
}

// writeIdx 直写一条索引（不写消息）。
func writeIdx(t *testing.T, st *store.Store, topic, key string, storeAt int64, off uint64) []byte {
	t.Helper()
	k := store.KeyIdxKey(topic, key, storeAt, 0, off)
	b := st.NewBatch()
	b.Set(k, nil)
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	return k
}

// TestBackfillRemovesOrphans S3：补扫删孤儿（过期且消息不在），保留过期但消息仍在
// 与未过期的索引；完成后该组记入已完成集合，再跑一轮不再扫描。
func TestBackfillRemovesOrphans(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	ctx := context.Background()
	if _, err := mt.CreateTopic(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli()
	now := time.Now().UnixMilli()
	var orphans [][]byte
	for i := 0; i < 5; i++ { // (a) 孤儿：过期、无消息
		orphans = append(orphans, writeIdx(t, st, "t", fmt.Sprintf("orphan-%d", i), old, uint64(100+i)))
	}
	writeMsgAt(t, st, "t", 0, old, "rewound") // (b) 过期但消息仍在（模拟时钟回跳）
	writeMsgAt(t, st, "t", 1, now, "live")    // (c) 未过期、消息在

	m := New(rep, rt, st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())
	spy := spyOn(m, st)
	if err := m.backfillRound(ctx); err != nil {
		t.Fatal(err)
	}
	for _, k := range orphans {
		if _, ok, _ := st.Get(k); ok {
			t.Fatalf("孤儿索引 %q 未删", k)
		}
	}
	if _, ok, _ := st.Get(store.KeyIdxKey("t", "rewound", old, 0, 0)); !ok {
		t.Fatal("过期但消息仍在的索引被误删")
	}
	if _, ok, _ := st.Get(store.KeyIdxKey("t", "live", now, 0, 1)); !ok {
		t.Fatal("未过期索引被误删")
	}
	if got := m.backfillDeleted.Load(); got != 5 {
		t.Fatalf("补扫删除 %d 条; want 5", got)
	}
	if !m.backfillDone[0] {
		t.Fatal("补扫完成后组 0 未记入已完成集合")
	}
	reads := spy.reads.Load()
	if reads == 0 {
		t.Fatal("补扫未读到任何 keyidx 键")
	}
	if err := m.backfillRound(ctx); err != nil {
		t.Fatal(err)
	}
	if got := spy.reads.Load(); got != reads {
		t.Fatalf("已完成组再跑一轮仍读 keyidx：%d → %d", reads, got)
	}
}

// TestBackfillSkipsWhenNotLeader I3：非 leader 不发起补扫扫描，孤儿保留。
func TestBackfillSkipsWhenNotLeader(t *testing.T) {
	st, mt, rep, _ := newFixtureRouted(t)
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := mt.CreateTopic(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
	orphan := writeIdx(t, st, "t", "orphan", time.Now().Add(-4*24*time.Hour).UnixMilli(), 7)
	m := New(rep, fixedLeaderRouter{leader: false}, st, mt, 10*time.Millisecond, t.TempDir(), 0, nil, slog.Default())
	spy := spyOn(m, st)
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	time.Sleep(100 * time.Millisecond) // 约 10 个周期
	cancel()
	<-done
	if got := spy.reads.Load(); got != 0 {
		t.Fatalf("非 leader 读到 keyidx %d 个键; want 0", got)
	}
	if _, ok, _ := st.Get(orphan); !ok {
		t.Fatal("非 leader 删了孤儿索引")
	}
}

// TestBackfillInterruptible I4：补扫进行中取消 ctx，Run 在 2s 内返回且该组不记完成。
func TestBackfillInterruptible(t *testing.T) {
	st, mt, rep, rt := newFixtureRouted(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := mt.CreateTopic(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli()
	b := st.NewBatch()
	for i := 0; i < 50000; i++ {
		b.Set(store.KeyIdxKey("t", fmt.Sprintf("o-%05d", i), old, 0, uint64(1000+i)), nil)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	m := New(rep, rt, st, mt, time.Hour, t.TempDir(), 0, nil, slog.Default())
	spy := spyOn(m, st)
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for spy.reads.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("补扫未开始")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	start := time.Now()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("补扫中取消后 Run 未在 2s 内返回")
	}
	if m.backfillDone[0] {
		t.Fatalf("中断的补扫（已读 %d 个 keyidx 键）把组记为已完成", spy.reads.Load())
	}
	if c := countPrefix(t, st, store.KeyIdxTopicPrefix("t")); c == 0 {
		t.Fatal("中断后孤儿已全删——补扫未在块边界中断（用例未覆盖中断路径）")
	}
	t.Logf("取消到返回 %v，已读 %d 个 keyidx 键", time.Since(start), spy.reads.Load())
}
