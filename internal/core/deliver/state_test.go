// 不变式单测：队列内存态的批次构造（I1）、装载往返、到期索引（卡 SQ4.1 U1–U3）。
//
// 只测承重规则：
//   - U1（I1）：随机变更序列下，任一时刻构造的差量批次落盘后，盘上解码 =
//     内存（cursor + inflight 全字段）——cursor 与其覆盖区间的 inflight 恒同批
//   - U2：装载 → 变更 → 落盘 → 重新装载，两份内存态相等（含索引顺序、顺序计数）
//   - U3：到期索引按 (到期时刻, offset) 出序；改到期后顺序随之变化；未到期的
//     inflight 再多，取到期集对索引的访问也是常数级
package deliver

import (
	"log/slog"
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/store"
)

func openStateStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// diskView 盘上本队列的 cursor 与 inflight（解码后）。
func diskView(t *testing.T, st *store.Store, group, topic string, q uint32) (uint64, map[uint64]core.InflightState) {
	t.Helper()
	c := uint64(0)
	if v, ok, err := st.Get(store.CursorKey(group, topic, q)); err != nil {
		t.Fatal(err)
	} else if ok {
		c = store.GetU64(v)
	}
	m := map[uint64]core.InflightState{}
	pfx := store.InflightPrefix(group, topic, q)
	if err := st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		_, _, _, off, err := store.ParseInflightKey(k)
		if err != nil {
			return false, err
		}
		s, err := core.DecodeInflight(v)
		if err != nil {
			return false, err
		}
		m[off] = *s
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return c, m
}

func memView(qs *queueState) (uint64, map[uint64]core.InflightState) {
	m := map[uint64]core.InflightState{}
	for off, e := range qs.inflight {
		m[off] = e.st
	}
	return qs.cursor, m
}

// checkIndex 核对索引与 map、顺序计数、镜像一致。
func checkIndex(t *testing.T, qs *queueState) {
	t.Helper()
	if qs.exp.Len() != len(qs.inflight) || int(qs.nInflight.Load()) != len(qs.inflight) {
		t.Fatalf("索引 %d / 镜像 %d / map %d 不一致", qs.exp.Len(), qs.nInflight.Load(), len(qs.inflight))
	}
	ord := 0
	for i, e := range qs.exp {
		if e.idx != i || qs.inflight[e.off] != e {
			t.Fatalf("索引位置回填错误: off=%d idx=%d i=%d", e.off, e.idx, i)
		}
		if e.st.Ordered {
			ord++
		}
	}
	if ord != qs.orderedN {
		t.Fatalf("顺序计数 %d != 实际 %d", qs.orderedN, ord)
	}
}

// TestStateDeltaBatchKeepsDiskEqualMemory U1（I1）：随机操作序列（投递推进
// cursor / 确认 / 改不可见 / 续租式改到期 / 重投 / 删除孤儿 / 重置），随机时刻
// 构造差量批次并落盘，每次落盘后盘上 = 内存。
func TestStateDeltaBatchKeepsDiskEqualMemory(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		st := openStateStore(t)
		r := rand.New(rand.NewPCG(seed, 99))
		qs := newQueueState("g", "t", 3)
		qs.init(0, 0)
		applied := 0
		for step := 0; step < 400; step++ {
			switch op := r.IntN(7); op {
			case 0, 1: // 投递新消息：写 inflight 并推进 cursor
				n := 1 + r.IntN(4)
				for i := 0; i < n; i++ {
					qs.insert(qs.cursor, core.InflightState{ExpireAtMs: int64(r.IntN(1000)), Attempts: 1, Ordered: r.IntN(5) == 0})
					qs.setCursor(qs.cursor + 1)
				}
			case 2: // 确认 / 孤儿清理 / 转死信：删除
				for off := range qs.inflight {
					qs.remove(off)
					break
				}
			case 3: // 改不可见 / 续租：改到期
				for _, e := range qs.inflight {
					qs.setExpire(e, int64(r.IntN(1000)))
					break
				}
			case 4: // 重投：attempts+1 覆盖写
				for off, e := range qs.inflight {
					s := e.st
					s.Attempts++
					s.Owner = "c"
					s.RenewUntilMs = 5
					qs.insert(off, s)
					break
				}
			case 5: // 过滤跳过：只推进 cursor
				qs.setCursor(qs.cursor + uint64(r.IntN(3)))
			case 6: // 构造批次并落盘
				if !qs.hasDirty() {
					continue
				}
				b := st.NewBatch()
				qs.buildDelta(b)
				if err := st.Apply(b); err != nil {
					t.Fatal(err)
				}
				applied++
				dc, dm := diskView(t, st, "g", "t", 3)
				mc, mm := memView(qs)
				if dc != mc || !reflect.DeepEqual(dm, mm) {
					t.Fatalf("seed=%d step=%d 落盘后盘上 != 内存: disk cursor=%d n=%d, mem cursor=%d n=%d",
						seed, step, dc, len(dm), mc, len(mm))
				}
				if qs.hasDirty() {
					t.Fatalf("构造后脏集未清空")
				}
			}
			checkIndex(t, qs)
		}
		if applied == 0 {
			t.Fatalf("seed=%d 未发生任何落盘，用例空过", seed)
		}
	}
}

// TestStateFullBatchSelfContained 全量批次不依赖盘上基线：盘上残留任意
// inflight，全量批次落地后盘上 = 内存。
func TestStateFullBatchSelfContained(t *testing.T) {
	st := openStateStore(t)
	junk := st.NewBatch()
	for off := uint64(0); off < 10; off++ {
		junk.Set(store.InflightKey("g", "t", 0, off), core.EncodeInflight(&core.InflightState{ExpireAtMs: 1, Attempts: 9}))
	}
	if err := st.Apply(junk); err != nil {
		t.Fatal(err)
	}
	qs := newQueueState("g", "t", 0)
	qs.init(0, 7)
	qs.insert(12, core.InflightState{ExpireAtMs: 50, Attempts: 1})
	b := st.NewBatch()
	qs.buildFull(b)
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	dc, dm := diskView(t, st, "g", "t", 0)
	mc, mm := memView(qs)
	if dc != mc || !reflect.DeepEqual(dm, mm) {
		t.Fatalf("全量批次后盘上 != 内存: disk=%d %v mem=%d %v", dc, dm, mc, mm)
	}
}

// TestStateLoadRoundTrip U2：装载 → 变更 → 差量落盘 → 重新装载，两份相等。
func TestStateLoadRoundTrip(t *testing.T) {
	st := openStateStore(t)
	a := newQueueState("g", "t", 1)
	if err := a.load(st, 0); err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 50; i++ {
		a.insert(i, core.InflightState{ExpireAtMs: int64(1000 - i*7%13), Attempts: int32(1 + i%3), Ordered: i == 9, Owner: "o", RenewUntilMs: 77})
	}
	a.setCursor(50)
	a.remove(3)
	a.setExpire(a.inflight[4], 5)
	b := st.NewBatch()
	a.buildDelta(b)
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	c := newQueueState("g", "t", 1)
	if err := c.load(st, 0); err != nil {
		t.Fatal(err)
	}
	ac, am := memView(a)
	cc, cm := memView(c)
	if ac != cc || !reflect.DeepEqual(am, cm) || a.orderedN != c.orderedN {
		t.Fatalf("往返不等: cursor %d/%d n %d/%d ordered %d/%d", ac, cc, len(am), len(cm), a.orderedN, c.orderedN)
	}
	checkIndex(t, c)
	if c.hasDirty() {
		t.Fatal("装载不应产生脏集")
	}
	ea, ec := a.expired(1<<40, 0, true), c.expired(1<<40, 0, true)
	if len(ea) != len(ec) {
		t.Fatalf("到期序长度不等 %d/%d", len(ea), len(ec))
	}
	for i := range ea {
		if ea[i].off != ec[i].off {
			t.Fatalf("到期序第 %d 位不等: %d/%d", i, ea[i].off, ec[i].off)
		}
	}
}

// TestStateExpiryOrderAndCost U3：到期序、改到期后重排、未到期规模不影响访问量。
func TestStateExpiryOrderAndCost(t *testing.T) {
	qs := newQueueState("g", "t", 0)
	qs.init(0, 0)
	exps := map[uint64]int64{0: 30, 1: 10, 2: 20, 3: 10, 4: 99}
	for off, e := range exps {
		qs.insert(off, core.InflightState{ExpireAtMs: e, Attempts: 1})
	}
	offs := func(es []*inflightEntry) []uint64 {
		var o []uint64
		for _, e := range es {
			o = append(o, e.off)
		}
		return o
	}
	if got := offs(qs.expired(50, 0, true)); !reflect.DeepEqual(got, []uint64{1, 3, 2, 0}) {
		t.Fatalf("到期序应为 (到期,offset) 升序 [1 3 2 0]，得到 %v", got)
	}
	if got := offs(qs.expired(50, 2, false)); !reflect.DeepEqual(got, []uint64{1, 3}) {
		t.Fatalf("限额 2 应取最早两条 [1 3]，得到 %v", got)
	}
	// 改不可见：offset 1 推到最后
	qs.setExpire(qs.inflight[1], 40)
	if got := offs(qs.expired(50, 0, true)); !reflect.DeepEqual(got, []uint64{3, 2, 0, 1}) {
		t.Fatalf("改到期后应为 [3 2 0 1]，得到 %v", got)
	}
	checkIndex(t, qs)

	// 成本：10k 条未到期 inflight，一次取到期集只看堆顶
	big := newQueueState("g", "t", 1)
	big.init(0, 0)
	for off := uint64(0); off < 10000; off++ {
		big.insert(off, core.InflightState{ExpireAtMs: 1_000_000 + int64(off%977), Attempts: 1})
	}
	before := big.idxVisits
	if got := big.expired(500, 32, false); len(got) != 0 {
		t.Fatalf("未到期不应返回: %d", len(got))
	}
	if v := big.idxVisits - before; v > 2 {
		t.Fatalf("10k 未到期 inflight 时一次取到期集访问索引 %d 次，应为常数级（≤2）", v)
	}
	keys := make([]uint64, 0, len(big.inflight))
	for k := range big.inflight {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	if len(keys) != 10000 {
		t.Fatalf("取到期集不应改变状态: %d", len(keys))
	}
}
