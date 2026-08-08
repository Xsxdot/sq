// retention 清理任务测试：直接向 store 注入带旧 StoreAtMs 的消息制造过期数据
// （produce.Append 总用当前时间，无法造旧数据）。
package retention

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
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

func (r fixedLeaderRouter) GroupForQueue(string, uint32) uint32 { return 0 }
func (r fixedLeaderRouter) MetaGroup() uint32                   { return 0 }
func (r fixedLeaderRouter) IsLeader(uint32) bool                { return r.leader }

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
	msgs, err := dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
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
