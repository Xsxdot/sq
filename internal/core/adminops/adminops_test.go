// adminops 清理测试：写入真实数据后成片删除，验证目标区间清空且相邻数据无损。
package adminops

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

func fixture(t *testing.T) (*store.Store, *meta.Meta, *produce.Producer, *deliver.Deliverer) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(st, mt, slog.Default())
	return st, mt, pr, deliver.New(st, mt, pr, slog.Default())
}

func countPrefix(t *testing.T, st *store.Store, lower []byte) int {
	t.Helper()
	n := 0
	if err := st.Scan(lower, store.PrefixUpperBound(lower), 0, func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPurgeTopicData(t *testing.T) {
	st, mt, pr, _ := fixture(t)
	for i := 0; i < 3; i++ {
		if _, err := pr.Append(&core.Message{Topic: "del-me", Body: []byte("x"), Keys: []string{"k1"}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pr.Append(&core.Message{Topic: "keep", Body: []byte("y"), Keys: []string{"k1"}}); err != nil {
		t.Fatal(err)
	}
	tc, _ := mt.GetTopic("del-me")
	if err := PurgeTopicData(st, tc, slog.Default()); err != nil {
		t.Fatal(err)
	}
	if n := countPrefix(t, st, store.MsgQueuePrefix("del-me", 0)); n != 0 {
		t.Fatalf("msg/ 应清空，剩 %d", n)
	}
	if n := countPrefix(t, st, store.KeyIdxTopicPrefix("del-me")); n != 0 {
		t.Fatalf("keyidx/ 应清空，剩 %d", n)
	}
	if _, ok, _ := st.Get(store.AllocKey("del-me", 0)); ok {
		t.Fatal("alloc 计数器应删除")
	}
	// 相邻 topic 必须无损
	if n := countPrefix(t, st, store.MsgQueuePrefix("keep", 0)); n != 1 {
		t.Fatalf("keep topic 应剩 1 条，得到 %d", n)
	}
}

func TestPurgeGroupData(t *testing.T) {
	st, _, pr, dl := fixture(t)
	if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	// 真实消费一条：产生 cursor 与 inflight
	if _, err := dl.Receive(context.Background(), "g-del", "t1", 0, 1, time.Minute, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := dl.Receive(context.Background(), "g-keep", "t1", 0, 1, time.Minute, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := PurgeGroupData(st, "g-del", slog.Default()); err != nil {
		t.Fatal(err)
	}
	if n := countPrefix(t, st, store.CursorGroupPrefix("g-del")); n != 0 {
		t.Fatalf("g-del cursor 应清空，剩 %d", n)
	}
	if n := countPrefix(t, st, store.InflightGroupPrefix("g-del")); n != 0 {
		t.Fatalf("g-del inflight 应清空，剩 %d", n)
	}
	if n := countPrefix(t, st, store.CursorGroupPrefix("g-keep")); n != 1 {
		t.Fatalf("g-keep cursor 应无损，得到 %d", n)
	}
}
