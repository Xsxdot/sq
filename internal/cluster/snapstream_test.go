// snapstream_test.go 验证组键族枚举器与快照分块编解码。
//
// 职责：
//   - 数据组枚举必须逐键判哈希归属（键是散布的，不能按前缀整段搬）
//   - 组 0 必须覆盖全部全局前缀键族、且不含本地键族 metric/
//   - 分块编码往返一致、坏块必须报错
//
// 边界：不覆盖控制通道的分块拉取（Task 6）；
// openClusterTestStore/testSlog 复用 raftstore_test.go。
package cluster

import (
	"testing"

	"github.com/xushixin/sq/internal/store"
)

// groupForQueueOfKey 解析键的 (topic, queueID) 并返回其归属组
// （测试 helper，断言枚举结果没有混入别组键用）。
func groupForQueueOfKey(t *testing.T, k string, groups uint32) uint32 {
	t.Helper()
	topic, qid, _, err := store.ParseMsgKey([]byte(k))
	if err != nil {
		t.Fatalf("解析键 %q: %v", k, err)
	}
	return groupForQueue(topic, qid, groups)
}

// TestScanGroupKeysFiltersByGroup 数据组的键是哈希散布的：
// 枚举必须逐键判归属，绝不能按前缀整段搬（会把别组的数据搬过去）。
func TestScanGroupKeysFiltersByGroup(t *testing.T) {
	st := openClusterTestStore(t)
	const groups = uint32(3)
	// 造 30 个队列的消息键，记录每个键的真实归属组
	want := map[uint32][]string{}
	b := st.NewBatch()
	for q := uint32(0); q < 30; q++ {
		k := store.MsgKey("T", q, 1)
		g := groupForQueue("T", q, groups)
		want[g] = append(want[g], string(k))
		if err := b.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	view := st.NewReadView()
	defer view.Close()

	for g := uint32(1); g < groups; g++ {
		var got []string
		from := []byte(nil)
		for {
			next, done, err := scanGroupKeys(view, g, groups, from, 4, func(k, _ []byte) error {
				got = append(got, string(k))
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if done {
				break
			}
			from = next
		}
		if len(got) != len(want[g]) {
			t.Fatalf("组 %d 枚举 %d 个键; want %d", g, len(got), len(want[g]))
		}
		for _, k := range got {
			if groupForQueueOfKey(t, k, groups) != g {
				t.Fatalf("组 %d 的枚举结果混入了别组的键 %q", g, k)
			}
		}
	}
}

// TestScanGroupKeysGroup0CoversGlobalPrefixes 组 0 的键族是全局连续前缀，
// 一个都不能漏——漏掉 half/ 就是事务状态丢失。
func TestScanGroupKeysGroup0CoversGlobalPrefixes(t *testing.T) {
	st := openClusterTestStore(t)
	keys := [][]byte{
		store.TopicMetaKey("T"), store.GroupMetaKey("G"), store.HandleSecretKey(),
		store.DelayKey(1000, 1), store.DelayAllocKey(),
		store.HalfKey(1000, "tx1"), store.HalfIdxKey("tx1"),
	}
	b := st.NewBatch()
	for _, k := range keys {
		if err := b.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// metric/ 是本地不复制键族（batch③ 归属纪律），必须**不**出现在快照里
	if err := b.Set(store.MetricKey(1000), []byte("m")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	view := st.NewReadView()
	defer view.Close()

	got := map[string]bool{}
	from := []byte(nil)
	for {
		next, done, err := scanGroupKeys(view, 0, 3, from, 2, func(k, _ []byte) error {
			got[string(k)] = true
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		from = next
	}
	for _, k := range keys {
		if !got[string(k)] {
			t.Fatalf("组 0 快照漏掉全局键 %q", k)
		}
	}
	if got[string(store.MetricKey(1000))] {
		t.Fatal("metric/ 是本地不复制键族，不得进快照")
	}
}

func TestChunkRoundTrip(t *testing.T) {
	in := []kv{{k: []byte("a"), v: []byte("1")}, {k: []byte(""), v: nil}, {k: []byte("bb"), v: []byte("222")}}
	out, err := decodeChunk(encodeChunk(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("往返 %d 对; want %d", len(out), len(in))
	}
	for i := range in {
		if string(out[i].k) != string(in[i].k) || string(out[i].v) != string(in[i].v) {
			t.Fatalf("第 %d 对 = (%q,%q); want (%q,%q)", i, out[i].k, out[i].v, in[i].k, in[i].v)
		}
	}
	if _, err := decodeChunk([]byte{0, 0, 0, 9, 'x'}); err == nil {
		t.Fatal("坏块（声称长度超出剩余字节）必须报错")
	}
}
