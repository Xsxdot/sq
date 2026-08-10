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
	"fmt"
	"strings"
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

// TestScanGroupKeysMultiFamilyMultiBlockConverges 多键族 + 多块续扫收敛
// （C1 回归测试）：数据组同时存在 msg/ 与 cursor/ 两个键族，单块
// budget 装不下全部键，跨块续扫必须做到每键恰好一次——不漏、不重、
// 不多，并终止于 done。
//
// 修复前 groupKeyRanges 对数据组按字面序返回区间（msg/ 在首），但
// 字节序是 alloc/ cursor/ inflight/ keyidx/ msg/——「区间按字节序
// 升序」的续扫前提对数据组不成立：从 msg/ 续扫到字节序更小的 cursor/
// 段时游标回跳，cursor/ 整族被跳过（静默漏发）。修复后区间按字节序
// 重排，游标单调推进，跨块必然收敛。
func TestScanGroupKeysMultiFamilyMultiBlockConverges(t *testing.T) {
	const groups = uint32(3)
	const g = uint32(1) // 数据组 1；种子按哈希归属筛选，键天然只进本组
	st := openClusterTestStore(t)

	// 种多键族数据：325 个 msg 键 + 65 个 cursor 键，全部归属组 g。
	// 队列→组是散布哈希，循环扫描队列号只挑归属 == g 的写，凑够数量。
	var want []string
	b := st.NewBatch()
	for q, n := uint32(0), 0; n < 325; q++ {
		if groupForQueue("T", q, groups) != g {
			continue
		}
		k := store.MsgKey("T", q, 1)
		want = append(want, string(k))
		if err := b.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
		n++
	}
	for q, n := uint32(0), 0; n < 65; q++ {
		if groupForQueue("U", q, groups) != g {
			continue
		}
		k := store.CursorKey("cg", "U", q)
		want = append(want, string(k))
		if err := b.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
		n++
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	view := st.NewReadView()
	defer view.Close()

	for _, budget := range []int{100, 4} { // 100：多块常规；4：贴边界极小预算
		t.Run(fmt.Sprintf("budget%d", budget), func(t *testing.T) {
			got := map[string]int{}
			from := []byte(nil)
			for blocks := 0; ; blocks++ {
				next, done, err := scanGroupKeys(view, g, groups, from, budget, func(k, _ []byte) error {
					got[string(k)]++
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
				if blocks == 499 {
					t.Fatal("500 块未收敛到 done：续扫游标不单调推进")
				}
				from = next
			}
			// 不漏：种子键必须全部出现
			for _, k := range want {
				if got[k] == 0 {
					t.Fatalf("漏键 %q（续扫跳段把字节序更小的键族吞了）", k)
				}
			}
			// 不重：每个键恰好出现一次
			for k, n := range got {
				if n > 1 {
					t.Fatalf("键 %q 被重发 %d 次（游标回跳重扫）", k, n)
				}
				kg, perr := keyGroupOf([]byte(k), groups)
				if perr != nil {
					t.Fatalf("组 %d 枚举混入不可解析键 %q: %v", g, k, perr)
				}
				if kg != g {
					t.Fatalf("组 %d 枚举混入别组键 %q（归属 %d）", g, k, kg)
				}
			}
		})
	}
}

// TestAssertRangesDisjointCatchesOverlap 键区间守卫（m3）：断言的是
// 「两两不相交」而非「已升序」——升序由紧邻上游的 sort.Sort 无条件
// 保证，断言 IsSorted 恒真是死代码。真正会被未来的键族前缀改动破坏的
// 是不相交：加进一个与既有键族互为前缀的名字（"msg" vs "msg/"），
// 两段区间重叠，同一个键会被发进两个块，块间互斥崩塌、游标语义失效。
func TestAssertRangesDisjointCatchesOverlap(t *testing.T) {
	mustPanic := func(name string, ranges []keyRange) {
		t.Helper()
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("%s: 应 panic，实际正常返回", name)
			}
			if msg, _ := r.(string); !strings.Contains(msg, "两两不相交") {
				t.Fatalf("%s: panic 文本应指向不相交守卫，得到: %v", name, r)
			}
		}()
		assertRangesDisjoint(7, ranges)
	}
	// 互为前缀的键族："msg" 的上界 "msh" 越过了 "msg/" 的下界
	mustPanic("互为前缀的键族", []keyRange{
		{lower: []byte("msg"), upper: store.PrefixUpperBound([]byte("msg"))},
		{lower: []byte("msg/"), upper: store.PrefixUpperBound([]byte("msg/"))},
	})
	// 空区间（lower ≥ upper）：整段永远扫不出键，对应键族静默丢失
	mustPanic("空区间", []keyRange{
		{lower: []byte("msg/"), upper: []byte("msg/")},
	})

	// 反向自证：真实的两个组都必须通过守卫（守卫不是恒 panic）
	for _, g := range []uint32{MetaGroup, 1} {
		ranges := groupKeyRanges(g)
		if len(ranges) == 0 {
			t.Fatalf("组 %d 区间集不得为空", g)
		}
		assertRangesDisjoint(g, ranges)
	}
}
