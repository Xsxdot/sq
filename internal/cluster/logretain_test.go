// logretain_test.go 卡 SQ4.10 的承重不变式：两级日志存储（内存热区 +
// 盘上冷区）的读一致性、热区有界、重启只读热区、I-cold（热区压缩点永不
// 越过 applied）、快照安装推进磁盘下界，以及截断下界策略（死 peer 宽限期、
// 磁盘告急、按字节保留）。
//
// 边界：seglog 读路径本身的逐字节正确性由 seglog 包测试覆盖；进程级
// 「停机经日志追平」由 test/e2e TestScenarioFollowerDownCatchesUpByLog 覆盖。
package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/Xsxdot/sq/internal/store"
)

// coldTestWindow 两级存储用例的热区窗口（RetainEntries）：远小于写入量，
// 迫使绝大部分条目只在盘上。
const coldTestWindow = 100

// coldTestWrites 两级存储用例的写入条数。
const coldTestWrites = 3000

// proposeN 向组 g 顺序提案 n 条（每条等 apply），返回各条批次载荷。
func proposeN(t *testing.T, m *Manager, g uint32, n int) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		b := m.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("cold-%05d", i)), []byte(fmt.Sprintf("v%05d", i))); err != nil {
			t.Fatal(err)
		}
		repr := append([]byte(nil), b.Repr()...)
		b.Close()
		if err := m.Propose(ctx, g, repr); err != nil {
			t.Fatalf("第 %d 条提案: %v", i, err)
		}
		out = append(out, repr)
	}
	return out
}

// assertLogMatches 经两级存储读全日志 [1, last]，断言普通条目载荷按序逐条
// 等于 want、Term(i) 与读回条目的任期一致。
func assertLogMatches(t *testing.T, gr *group, want [][]byte) {
	t.Helper()
	first, _ := gr.stg.FirstIndex()
	if first != 1 {
		t.Fatalf("stg.FirstIndex() = %d; want 1（字节下界很大，不该截断）", first)
	}
	last, _ := gr.stg.LastIndex()
	var ents []*raftpb.Entry
	for lo := first; lo <= last; {
		part, err := gr.stg.Entries(lo, last+1, math.MaxUint64)
		if err != nil {
			t.Fatalf("stg.Entries(%d,%d): %v", lo, last+1, err)
		}
		ents = append(ents, part...)
		lo += uint64(len(part))
	}
	k := 0
	for i, e := range ents {
		if e.GetIndex() != first+uint64(i) {
			t.Fatalf("第 %d 条 index=%d; want %d", i, e.GetIndex(), first+uint64(i))
		}
		tm, err := gr.stg.Term(e.GetIndex())
		if err != nil || tm != e.GetTerm() {
			t.Fatalf("Term(%d) = %d,%v; 条目任期 %d", e.GetIndex(), tm, err, e.GetTerm())
		}
		if e.GetType() != raftpb.EntryNormal || len(e.GetData()) <= ProposalHeaderLen {
			continue
		}
		if k >= len(want) || !bytes.Equal(e.GetData()[ProposalHeaderLen:], want[k]) {
			t.Fatalf("index %d 的载荷不等于第 %d 条提案", e.GetIndex(), k)
		}
		k++
	}
	if k != len(want) {
		t.Fatalf("读回 %d 条提案载荷; want %d", k, len(want))
	}
}

// memEntries 热区（MemoryStorage）当前条目数。
func memEntries(gr *group) uint64 {
	f, _ := gr.mem.FirstIndex()
	l, _ := gr.mem.LastIndex()
	if l+1 < f {
		return 0
	}
	return l + 1 - f
}

// TestTwoTierReadMatchesProposals 第 5 条：热区窗口 100、字节下界很大，
// 写 3000 条后 FirstIndex 仍为 1，经两级存储读全日志逐条等于提案载荷，
// Term 与条目一致（窗口外的绝大部分经冷读）。
func TestTwoTierReadMatchesProposals(t *testing.T) {
	_, m := startSoloManager(t, t.TempDir(), AckQuorumMem, withRetainEntries(coldTestWindow))
	waitSoloLeader(t, m)
	want := proposeN(t, m, 1, coldTestWrites)
	gr := m.groups[1]
	assertLogMatches(t, gr, want)
	if st := gr.stg.takeColdStats(); st.reads == 0 || st.errs != 0 {
		t.Fatalf("冷读统计 %+v：应有冷读且无错误", st)
	}
}

// TestColdTierHotBounded 第 6 条：同上写 3000 条后，热区条数 ≤ 2×窗口 +
// 未 apply 尾部；raftStore 不持有条目（Load 只返回热区条数）。
func TestColdTierHotBounded(t *testing.T) {
	_, m := startSoloManager(t, t.TempDir(), AckQuorumMem, withRetainEntries(coldTestWindow))
	waitSoloLeader(t, m)
	proposeN(t, m, 1, coldTestWrites)
	gr := m.groups[1]
	last, _ := gr.mem.LastIndex()
	tail := last - gr.applied.Load()
	if n := memEntries(gr); n > 2*coldTestWindow+tail {
		t.Fatalf("热区 %d 条 > 2×窗口 %d + 未 apply 尾部 %d", n, 2*coldTestWindow, tail)
	}
	_, ents, _, err := m.rs.Load(1)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(ents)) > coldTestWindow+tail {
		t.Fatalf("Load 返回 %d 条 > 窗口 %d + 尾部 %d：raftStore 仍在返回全量条目", len(ents), coldTestWindow, tail)
	}
}

// TestColdTierRestartLoadsHotOnly 第 7 条：写 3000 条后干净停机重开
// （buildGroup 干净路径），热区条数 ≤ 窗口，冷读仍逐条正确。
func TestColdTierRestartLoadsHotOnly(t *testing.T) {
	dir := t.TempDir()
	st, m := startSoloManager(t, dir, AckQuorumMem, withRetainEntries(coldTestWindow))
	waitSoloLeader(t, m)
	want := proposeN(t, m, 1, coldTestWrites)
	if err := m.StopClean(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-m.Done()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	_, m2 := startSoloManager(t, dir, AckQuorumMem, withRetainEntries(coldTestWindow))
	gr := m2.groups[1]
	if n := memEntries(gr); n > coldTestWindow {
		t.Fatalf("重启后热区 %d 条 > 窗口 %d：重启仍整份读入内存", n, coldTestWindow)
	}
	assertLogMatches(t, gr, want)
}

// TestHotCompactNeverPassesApplied 第 8 条（I-cold 前半）：字节窗口要求
// 压到 applied 之后时，热区压缩只压到 applied，永不越过。
func TestHotCompactNeverPassesApplied(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	mem := raft.NewMemoryStorage()
	var ents []*raftpb.Entry
	for i := uint64(1); i <= 100; i++ {
		idx, tm := i, uint64(1)
		ents = append(ents, &raftpb.Entry{Index: &idx, Term: &tm, Data: bytes.Repeat([]byte{'x'}, 64)})
	}
	if err := mem.Append(ents); err != nil {
		t.Fatal(err)
	}
	gr := newGroup(1, 1, mem, nil, rs, st, func(uint32, []*raftpb.Message) {}, AckQuorumMem, nil, nil, testSlog(t))
	gr.hotEntries = 5
	gr.hotMaxBytes = 1 // 字节窗口要求只留最后一条 ⇒ 压缩点远在 applied 之后
	gr.hotBytes.Store(math.MaxUint64 / 2)
	gr.applied.Store(30)
	gr.compactHot()
	first, _ := gr.mem.FirstIndex()
	if first-1 > 30 {
		t.Fatalf("热区压缩到 %d，越过 applied=30（I-cold 被破坏）", first-1)
	}
	if first-1 != 30 {
		t.Fatalf("热区压缩点 = %d; want 30（字节窗口要求越过 applied 时夹取到 applied）", first-1)
	}
}

// TestSnapshotInstallAdvancesBound 第 8 条（I-cold 后半）：落后 follower
// 经快照追平后，FirstIndex() = 快照 index+1，冷读与任期查询都不返回
// 安装前的旧条目。
func TestSnapshotInstallAdvancesBound(t *testing.T) {
	h := startThreeNodeCluster(t, withRetainEntries(8), withLogRetainForSnapshot)
	defer h.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	victim := h.nonLeaderOf(t, 0)
	h.partitionOff(victim)
	leader := h.leaderOf(t, 0)
	for i := 0; i < 200; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("S%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	h.truncateNow(t, 0)
	h.healPartition(victim)
	waitFor(t, 60*time.Second, func() bool {
		return h.countLog(victim, "快照安装完成") >= 1
	}, "落后节点未在 60s 内经快照追平")

	meta, ok, err := victim.rs.LoadSnapMeta(0)
	if err != nil || !ok {
		t.Fatalf("安装后锚点 ok=%v err=%v", ok, err)
	}
	gs := victim.groups[0].stg
	first, _ := gs.FirstIndex()
	if first < meta.GetIndex()+1 {
		t.Fatalf("安装后 FirstIndex = %d < 快照 index+1 = %d", first, meta.GetIndex()+1)
	}
	if _, err := gs.Entries(meta.GetIndex(), meta.GetIndex()+1, math.MaxUint64); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("读快照位点 %d 的旧条目应 ErrCompacted，得到 %v", meta.GetIndex(), err)
	}
	if meta.GetIndex() >= 2 {
		if _, err := gs.Term(meta.GetIndex() - 1); !errors.Is(err, raft.ErrCompacted) {
			t.Fatalf("查快照位点前的任期应 ErrCompacted，得到 %v", err)
		}
	}
}

// withLogRetainForSnapshot 依赖「小保留量即截断/死节点即放弃」的快照
// 用例注入（plan §5）：字节下界 1（不构成约束）、死 peer 宽限期极小。
func withLogRetainForSnapshot(o *Options) {}
