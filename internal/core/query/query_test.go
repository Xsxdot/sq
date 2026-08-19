// query 包测试：按业务 key 检索。
package query

import (
	"context"
	"log/slog"
	"testing"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

func newFixture(t *testing.T) (*store.Store, *produce.Producer) {
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
	return st, produce.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, mt, slog.Default())
}

func TestByKeyFindsMessages(t *testing.T) {
	st, pr := newFixture(t)
	for _, body := range []string{"a", "b", "c"} {
		if _, err := pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte(body), Keys: []string{"oid"}}); err != nil {
			t.Fatal(err)
		}
	}
	pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("other"), Keys: []string{"other-key"}})

	msgs, err := ByKey(st, "t", "oid", 0)
	if err != nil || len(msgs) != 3 {
		t.Fatalf("ByKey: %d %v", len(msgs), err)
	}
	for i, want := range []string{"a", "b", "c"} { // storeMs 同毫秒时按 queue/offset 续排，单队列即写入序
		if string(msgs[i].Body) != want {
			t.Fatalf("第 %d 条 body: %s", i, msgs[i].Body)
		}
	}
}

// TestBrowse 按队列 offset 顺序浏览。
func TestBrowse(t *testing.T) {
	st, pr := newFixture(t)
	for i := 0; i < 5; i++ {
		if _, err := pr.Append(context.Background(), &core.Message{Topic: "t1", Body: []byte{byte('0' + i)}}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := Browse(st, "t1", 0, 2, 2)
	if err != nil || len(got) != 2 {
		t.Fatalf("应返回 2 条: %d %v", len(got), err)
	}
	if got[0].Offset != 2 || got[1].Offset != 3 {
		t.Fatalf("offset 应为 2,3: %d,%d", got[0].Offset, got[1].Offset)
	}
	// 越界起点：空结果不报错（控制台翻页到底的正常形态）
	if got, err := Browse(st, "t1", 0, 99, 10); err != nil || len(got) != 0 {
		t.Fatalf("越界应空: %d %v", len(got), err)
	}
}

// TestByKeyNoPrefixCollision 查 "oid" 不得混入 "oid2" 与 "oid/x" 的消息。
func TestByKeyNoPrefixCollision(t *testing.T) {
	st, pr := newFixture(t)
	pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("hit"), Keys: []string{"oid"}})
	pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("miss1"), Keys: []string{"oid2"}})
	pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("miss2"), Keys: []string{"oid/x"}})
	msgs, err := ByKey(st, "t", "oid", 0)
	if err != nil || len(msgs) != 1 || string(msgs[0].Body) != "hit" {
		t.Fatalf("ByKey 精确性: %d %v", len(msgs), err)
	}
}

// TestByKeySkipsPurgedMessage retention 清走消息但索引未清（清理竞态）时跳过不报错。
func TestByKeySkipsPurgedMessage(t *testing.T) {
	st, pr := newFixture(t)
	m, _ := pr.Append(context.Background(), &core.Message{Topic: "t", Body: []byte("x"), Keys: []string{"k"}})
	b := st.NewBatch()
	b.Delete(store.MsgKey("t", m.QueueID, m.Offset))
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	msgs, err := ByKey(st, "t", "k", 0)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("应跳过已清理消息: %d %v", len(msgs), err)
	}
}
