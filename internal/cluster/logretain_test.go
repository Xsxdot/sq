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
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/Xsxdot/sq/internal/cluster/seglog"
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
func withLogRetainForSnapshot(o *Options) {
	o.RetainBytes = 1
	o.PeerGrace = time.Nanosecond
}

// withPeerGrace 注入死 peer 宽限期（Options.PeerGrace）。
func withPeerGrace(d time.Duration) func(*Options) { return func(o *Options) { o.PeerGrace = d } }

// withRetainBytes 注入按字节保留量（Options.RetainBytes，按节点总量）。
func withRetainBytes(n int64) func(*Options) { return func(o *Options) { o.RetainBytes = n } }

// withDiskPressure 注入磁盘水位告急判据。
func withDiskPressure(f func() bool) func(*Options) { return func(o *Options) { o.DiskPressure = f } }

// partitionAndWrite 等 victim 追平选举位点后隔断它，leader 再写 n 条；
// 返回 victim 在 leader 侧冻结的 Match 与写后 leader 的 applied。
func partitionAndWrite(t *testing.T, h *snapHarness, leader, victim *Manager, n int) (frozen, applied uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	waitFor(t, 30*time.Second, func() bool {
		st, _ := leader.Status(0)
		return st.Progress[victim.nodeID].Match >= 2
	}, "victim 未追平选举位点")
	h.partitionOff(victim)
	st0, _ := leader.Status(0)
	frozen = st0.Progress[victim.nodeID].Match
	for i := 0; i < n; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("G%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	return frozen, leader.AppliedIndex(0)
}

// TestTruncatePeerGraceHoldsThenReleases 第 9 条（S2 前半）：隔断一个
// follower 后写 100 条，宽限期（3s）内截断不越过它的冻结 Match；宽限期
// 过后照常截断、越过冻结 Match，并留下宽限超期 Warn。
func TestTruncatePeerGraceHoldsThenReleases(t *testing.T) {
	h := startThreeNodeCluster(t, withRetainEntries(4), withRetainBytes(1), withPeerGrace(3*time.Second))
	defer h.stopAll()
	leader := h.leaderOf(t, 0)
	victim := h.nonLeaderOf(t, 0)
	frozen, applied := partitionAndWrite(t, h, leader, victim, 100)
	defer h.healPartition(victim)

	if upto, done := leader.truncateOnce(0); done && upto > frozen {
		t.Fatalf("宽限期内截断到 %d，越过死 peer 冻结 Match %d", upto, frozen)
	}
	if h.countLog(leader, "宽限期内仍以其 Match 为下界") < 1 {
		t.Fatal("死 peer 进入宽限期的 Warn 缺失")
	}
	time.Sleep(3 * time.Second)
	upto, done := leader.truncateOnce(0)
	if !done || upto <= frozen {
		t.Fatalf("宽限期过后截断 done=%v upto=%d，未越过冻结 Match %d", done, upto, frozen)
	}
	if upto > applied-4 {
		t.Fatalf("截断到 %d 越过 applied-retain %d", upto, applied-4)
	}
	if h.countLog(leader, "存活探测失败，不以其 Match 为下界") < 1 {
		t.Fatal("宽限超期的 Warn 缺失")
	}
}

// TestTruncateDiskPressureDropsPeerFloors 第 10 条（S2 后半）：宽限期 10m
// 但磁盘告急，第一次截断即越过宽限期内死 peer 的冻结 Match，Warn 留痕；
// upto 仍不越过 applied − RetainEntries。
func TestTruncateDiskPressureDropsPeerFloors(t *testing.T) {
	h := startThreeNodeCluster(t, withRetainEntries(4), withRetainBytes(1), withPeerGrace(10*time.Minute),
		withDiskPressure(func() bool { return true }))
	defer h.stopAll()
	leader := h.leaderOf(t, 0)
	victim := h.nonLeaderOf(t, 0)
	frozen, applied := partitionAndWrite(t, h, leader, victim, 100)
	defer h.healPartition(victim)

	upto, done := leader.truncateOnce(0)
	if !done || upto <= frozen {
		t.Fatalf("磁盘告急下截断 done=%v upto=%d，未越过冻结 Match %d", done, upto, frozen)
	}
	if upto > applied-4 {
		t.Fatalf("截断到 %d 越过 applied-retain %d（告急只放 peer 下界，不放条数下界）", upto, applied-4)
	}
	if h.countLog(leader, "磁盘水位告急") < 1 {
		t.Fatal("磁盘告急放弃下界的 Warn 缺失")
	}
}

// TestTruncateUptoRules 第 10 条（下界纯函数）：活 peer / 宽限期内死 peer /
// 超期死 peer / learner / 磁盘告急 / 按字节上界各自的作用。
func TestTruncateUptoRules(t *testing.T) {
	const grace = time.Minute
	live := peerFloor{id: 2, match: 50}
	deadIn := peerFloor{id: 3, match: 40, dead: true, deadFor: grace - 1}
	deadOut := peerFloor{id: 3, match: 40, dead: true, deadFor: grace}
	learner := peerFloor{id: 4, match: 0, learner: true}
	for _, c := range []struct {
		name     string
		floors   []peerFloor
		bytesCap uint64
		pressure bool
		want     uint64
	}{
		{"follower 无 peer 下界", nil, math.MaxUint64, false, 90},
		{"活 peer 构成下界", []peerFloor{live}, math.MaxUint64, false, 40},
		{"宽限期内死 peer 构成下界", []peerFloor{live, deadIn}, math.MaxUint64, false, 30},
		{"超期死 peer 不构成下界", []peerFloor{live, deadOut}, math.MaxUint64, false, 40},
		{"learner 不构成下界", []peerFloor{learner}, math.MaxUint64, false, 90},
		{"告急：活 peer 也不构成下界", []peerFloor{live, deadIn}, math.MaxUint64, true, 90},
		{"告急仍守按字节下界", []peerFloor{live}, 70, true, 70},
		{"按字节下界取小", []peerFloor{live}, 35, false, 35},
		{"下界 ≤ 保留量不截断", []peerFloor{{id: 2, match: 10}}, math.MaxUint64, false, 0},
		{"字节不足一条不截", nil, 0, false, 0},
	} {
		if got := truncateUpto(100, 10, c.bytesCap, c.floors, grace, c.pressure); got != c.want {
			t.Errorf("%s: truncateUpto = %d; want %d", c.name, got, c.want)
		}
	}
}

// TestTruncateKeepsRetainBytes 第 11 条：小段写满若干段，RetainBytes 设为
// 约 3 段（按节点总量，即每组 3 段）⇒ 截断后剩余段字节 ≥ 每组下界，且
// 截断确实发生（被删段全部 ≤ upto，由 TruncateTo 的回收判据保证）。
func TestTruncateKeepsRetainBytes(t *testing.T) {
	old := seglog.SegMaxBytes
	seglog.SegMaxBytes = 4 << 10
	t.Cleanup(func() { seglog.SegMaxBytes = old })
	perGroup := 3 * seglog.SegMaxBytes
	_, m := startSoloManager(t, t.TempDir(), AckQuorumMem, withRetainEntries(10),
		withRetainBytes(perGroup*int64(1+3))) // 3 数据组 + meta 组
	waitSoloLeader(t, m)
	proposeN(t, m, 1, 600)
	l, err := m.rs.getLog(1)
	if err != nil {
		t.Fatal(err)
	}
	before := len(l.Segments())
	upto, done := m.truncateOnce(1)
	if !done {
		t.Fatalf("应截断（%d 段 ≫ 3 段下界）", before)
	}
	var kept int64
	segs := l.Segments()
	for _, s := range segs {
		kept += s.Bytes
		if !s.Active && s.MaxIndex <= upto {
			t.Fatalf("段 %d（MaxIndex %d ≤ upto %d）应已回收", s.Seq, s.MaxIndex, upto)
		}
	}
	if kept < perGroup {
		t.Fatalf("截断后剩余 %d 字节 < 每组下界 %d", kept, perGroup)
	}
	if len(segs) >= before {
		t.Fatalf("截断前 %d 段、后 %d 段：未回收任何段", before, len(segs))
	}
	// 没有字节下界时会截到 applied-10，远比现在多：证明字节下界真的生效
	if applied := m.groups[1].applied.Load(); upto >= applied-10 {
		t.Fatalf("upto=%d 未被字节下界压低（applied-retain=%d）", upto, applied-10)
	}
	t.Logf("截断前 %d 段 → 后 %d 段，upto=%d，剩余 %d 字节（每组下界 %d）", before, len(segs), upto, kept, perGroup)
}

// TestMarkDeadWarnsOncePerNode 判死是节点级事实：同一轮里多个组先后对
// 同一死 peer 调 markDead（各组 Match 不同，记录会按更大的 Match 重建），
// 「peer 判死」Warn 只打一行；宽限计时逻辑不变。
func TestMarkDeadWarnsOncePerNode(t *testing.T) {
	var buf bytes.Buffer
	m := &Manager{
		lg:        slog.New(slog.NewTextHandler(&buf, nil)),
		deadPeers: make(map[uint64]deadPeer),
		peerGrace: time.Hour,
	}
	now := time.Now()
	// 第一轮：3 个组、Match 先小后大（触发同轮记录重建）
	for g, match := range []uint64{400, 1000, 700} {
		m.markDead(uint32(g+1), 3, match, now)
	}
	// 第二轮：同一死 peer，Match 未前进
	later := now.Add(30 * time.Second)
	for g, match := range []uint64{400, 1000, 700} {
		m.markDead(uint32(g+1), 3, match, later)
	}
	if n := strings.Count(buf.String(), "peer 判死，宽限期内仍以其 Match 为下界"); n != 1 {
		t.Fatalf("判死 Warn 打了 %d 行; want 1（节点级事实只报一次）\n%s", n, buf.String())
	}
	if d := m.deadPeers[3]; !d.since.Equal(now) || d.match != 1000 {
		t.Fatalf("判死记录 since=%v match=%d; want since=首轮时刻 match=1000（计时逻辑不变）", d.since, d.match)
	}
}
