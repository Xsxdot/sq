// forwardroute_test.go: ForwardApply 控制处理器的提交分流（卡 SQ4.1 S6）。
//
// 职责：触及消费状态（cursor/、inflight/）的转发批次必须走 deliver 的管理
// 入口（先失效 leader 内存态再提交），其余批次照旧直接提交；管理入口未装配
// 时拒绝消费状态批次。
//
// 边界：只测分流判定；ApplyAdminBatch 自身的失效语义在 deliver/adminops 包测。
package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/Xsxdot/sq/internal/store"
)

type recAdmin struct{ n int }

func (r *recAdmin) ApplyAdminBatch(_ context.Context, _ uint32, b *store.Batch) error {
	r.n++
	return b.Close()
}

func TestRouteForwardApplyConsumeBatchesGoThroughDeliver(t *testing.T) {
	st, err := store.Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	direct := 0
	apply := func(_ context.Context, _ uint32, b *store.Batch) error {
		direct++
		return b.Close()
	}
	ck := store.CursorKey("g", "t", 0)
	ip := store.InflightPrefix("g", "t", 0)
	cases := []struct {
		name       string
		build      func(b *store.Batch)
		wantAdmin  bool
		wantDirect bool
	}{
		{"删组：cursor+inflight 前缀 DeleteRange", func(b *store.Batch) {
			b.DeleteRange(ck, store.PrefixUpperBound(ck))
			b.DeleteRange(ip, store.PrefixUpperBound(ip))
		}, true, false},
		{"单条 inflight 删除", func(b *store.Batch) { b.Delete(store.InflightKey("g", "t", 0, 5)) }, true, false},
		{"非消费键（msg/alloc）", func(b *store.Batch) {
			mp := store.MsgQueuePrefix("t", 0)
			b.DeleteRange(mp, store.PrefixUpperBound(mp))
			b.Delete(store.AllocKey("t", 0))
		}, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			adm := &recAdmin{}
			direct = 0
			b := st.NewBatch()
			c.build(b)
			if err := routeForwardApply(context.Background(), 1, b, apply, adm); err != nil {
				t.Fatal(err)
			}
			if (adm.n == 1) != c.wantAdmin || (direct == 1) != c.wantDirect {
				t.Fatalf("分流错误: admin=%d direct=%d", adm.n, direct)
			}
		})
	}
	// 管理入口未装配：消费状态批次拒绝，不得直接提交
	direct = 0
	b := st.NewBatch()
	b.DeleteRange(ck, store.PrefixUpperBound(ck))
	if err := routeForwardApply(context.Background(), 1, b, apply, nil); err == nil || direct != 0 {
		t.Fatalf("未装配时应拒绝消费状态批次: err=%v direct=%d", err, direct)
	}
}
