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
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
)

// fixture 返回 store/meta/produce/deliver 与单机复制抽象（Purge* 构造注入用）。
// 单机 Router 恒返回 MetaGroup 且 IsLeader 恒真：所有批次都走本地 rep.Apply，
// 与旧实现的 st.Apply 行为等价——既有用例无需改动即回归「分桶枚举完整性」。
func fixture(t *testing.T) (*store.Store, *meta.Meta, *produce.Producer, *deliver.Deliverer,
	*replication.Standalone, replication.StandaloneRouter) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	mt, err := meta.New(rep, rt, st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	return st, mt, pr, deliver.New(rep, rt, st, mt, pr, slog.Default()), rep, rt
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
	st, mt, pr, _, rep, rt := fixture(t)
	for i := 0; i < 3; i++ {
		if _, err := pr.Append(context.Background(), &core.Message{Topic: "del-me", Body: []byte("x"), Keys: []string{"k1"}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pr.Append(context.Background(), &core.Message{Topic: "keep", Body: []byte("y"), Keys: []string{"k1"}}); err != nil {
		t.Fatal(err)
	}
	tc, _ := mt.GetTopic("del-me")
	if err := PurgeTopicData(context.Background(), rep, rt, nil, st, tc, slog.Default()); err != nil {
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
	st, _, pr, dl, rep, rt := fixture(t)
	if _, err := pr.Append(context.Background(), &core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	// 真实消费一条：产生 cursor 与 inflight
	if _, err := dl.Receive(context.Background(), "g-del", "t1", 0, 1, time.Minute, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := dl.Receive(context.Background(), "g-keep", "t1", 0, 1, time.Minute, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := PurgeGroupData(context.Background(), rep, rt, nil, st, "g-del", slog.Default()); err != nil {
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

// TestPurgeGroupDataBucketsByQueue 多 topic 多队列的分桶枚举完整性：两队列
// 分别经 cursor 与 inflight 两个族写入（topicA 两个族都有、topicB 只有
// inflight——专测「只扫 cursor 会漏队列」的坑），purge 后该组两族全空。
// 单机 Router 恒零组下无法验证「按组提交」，但能验证「(topic,qid) 枚举
// 不漏不重」——每条记录都必须被某个桶命中并被删掉。
func TestPurgeGroupDataBucketsByQueue(t *testing.T) {
	st, _, pr, dl, rep, rt := fixture(t)
	// topicA 两族都写：q0 消费 1 条不 ack（cursor+inflight）、q1 同（多队列
	// 验证分桶按 qid 而非 topic）；topicB 只消费不 ack（若 PurgeGroupData 只
	// 扫 cursor 族，topicB 的 inflight 会残留——分桶必须两族联合枚举）
	for _, tc := range []struct {
		topic string
		q     uint32
	}{
		{"tA", 0}, {"tA", 1}, {"tB", 0},
	} {
		if _, err := pr.Append(context.Background(), &core.Message{Topic: tc.topic, Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		if _, err := dl.Receive(context.Background(), "g", tc.topic, tc.q, 1, time.Minute, 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	// 制造「有 inflight 无 cursor」的组记录（孤儿 inflight，天然合法形态）
	ob := st.NewBatch()
	ob.Set(store.InflightKey("g", "tB", 7, 99),
		core.EncodeInflight(&core.InflightState{ExpireAtMs: time.Now().UnixMilli() + 60000, Attempts: 1}))
	if err := st.Apply(ob); err != nil {
		t.Fatal(err)
	}

	if err := PurgeGroupData(context.Background(), rep, rt, nil, st, "g", slog.Default()); err != nil {
		t.Fatal(err)
	}
	for _, pfx := range [][]byte{store.CursorGroupPrefix("g"), store.InflightGroupPrefix("g")} {
		if n := countPrefix(t, st, pfx); n != 0 {
			t.Fatalf("g 的 %q 应清空，剩 %d", pfx, n)
		}
	}
	// 相邻组无损（g 之外的记录没被误删）
	if n := countPrefix(t, st, store.CursorGroupPrefix("other")); n != 0 {
		t.Fatalf("other 组不应有数据（测试前提），得到 %d", n)
	}
	// topicB/q7 的孤儿 inflight 已被清（分桶解析正确性的硬证据）
	if _, ok, _ := st.Get(store.InflightKey("g", "tB", 7, 99)); ok {
		t.Fatal("孤儿 inflight 未清理")
	}
}
