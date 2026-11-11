// epoch_cache_test.go U3：offset/seq 计数器缓存与可写门 epoch 绑定（卡 SQ1）。
//
// 契约（替代原 InvalidateCounters）：同一 epoch 内计数器只从盘装载一次，
// 之后用内存值（leader 是本任期唯一写者，内存即权威）；epoch 变了（换届，
// 期间他人可能写过）首次分配必须从盘重读。反着实现（每次读盘 / 永不重读）
// 都会红。
package produce

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

// epochRouter 单机路由 + 可切换的可写门 epoch。
type epochRouter struct {
	replication.StandaloneRouter
	epoch atomic.Uint64
}

func (r *epochRouter) WaitWritable(context.Context, uint32) (uint64, error) {
	return r.epoch.Load(), nil
}

// setDisk 绕过 Producer 直接改盘上计数器（模拟换届期间他人写入后复制落地）。
func setDisk(t *testing.T, st *store.Store, key []byte, v uint64) {
	t.Helper()
	b := st.NewBatch()
	b.Set(key, store.PutU64(v))
	if err := st.Apply(b); err != nil {
		t.Fatalf("改盘上计数器: %v", err)
	}
}

func TestCounterCacheBoundToEpoch(t *testing.T) {
	const topic = "u3"
	ctx := context.Background()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rt := &epochRouter{}
	rt.epoch.Store(1)
	rep := replication.NewStandalone(st)
	mt, err := meta.New(rep, rt, st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	p := New(rep, rt, st, mt, slog.Default())

	app := func(body string) uint64 {
		m, err := p.Append(ctx, &core.Message{Topic: topic, Body: []byte(body)})
		if err != nil {
			t.Fatalf("Append %s: %v", body, err)
		}
		return m.Offset
	}
	delay := func(body string) uint64 {
		_, seq, staged, err := p.AppendDelay(ctx, &core.Message{Topic: topic, Body: []byte(body),
			DeliverAtMs: time.Now().Add(time.Hour).UnixMilli()})
		if err != nil || !staged {
			t.Fatalf("AppendDelay %s: staged=%v err=%v", body, staged, err)
		}
		return seq
	}

	// offset 计数器
	if off := app("a"); off != 0 {
		t.Fatalf("首条 offset=%d，应为 0", off)
	}
	setDisk(t, st, store.AllocKey(topic, 0), 100)
	if off := app("b"); off != 1 {
		t.Fatalf("同 epoch 第二条 offset=%d，应沿用缓存得 1（不得重读盘）", off)
	}
	rt.epoch.Store(2)
	setDisk(t, st, store.AllocKey(topic, 0), 100)
	if off := app("c"); off != 100 {
		t.Fatalf("epoch 变化后首条 offset=%d，应从盘重读得 100", off)
	}
	if off := app("d"); off != 101 {
		t.Fatalf("epoch 变化后第二条 offset=%d，应为 101", off)
	}

	// 延时 seq 计数器（同契约）
	if seq := delay("da"); seq != 0 {
		t.Fatalf("首条延时 seq=%d，应为 0", seq)
	}
	setDisk(t, st, store.DelayAllocKey(), 50)
	if seq := delay("db"); seq != 1 {
		t.Fatalf("同 epoch 第二条延时 seq=%d，应沿用缓存得 1", seq)
	}
	rt.epoch.Store(3)
	setDisk(t, st, store.DelayAllocKey(), 50)
	if seq := delay("dc"); seq != 50 {
		t.Fatalf("epoch 变化后首条延时 seq=%d，应从盘重读得 50", seq)
	}
}
