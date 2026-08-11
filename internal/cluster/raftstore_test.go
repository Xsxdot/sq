// raftstore_test.go 验证 raft 日志共库持久层的正确性。
//
// 职责：HardState+Entry 单批往返、回退截断、组数校验、成员表按 commit 裁剪。
// 边界：不涉及 TCP 传输与运行体（后续任务）；openClusterTestStore/testSlog
// 为本包测试共享 helper，后续任务的测试文件直接复用。
package cluster

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/xushixin/sq/internal/cluster/seglog"
	"github.com/xushixin/sq/internal/store"
)

// openClusterTestStore 打开临时目录下的测试 store，随测试结束自动关闭。
// 后续任务的测试文件复用本 helper，保证集群层测试共享同一套打开方式。
// 参数用 testing.TB（测试与基准共用；BenchmarkProposeQuorumFsync 复用）。
func openClusterTestStore(t testing.TB) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// testSlog 返回写往测试输出的 slog，便于失败时直接看到节点日志。
// testWriter 移植自 spike/raftshell/node_test.go。
func testSlog(t testing.TB) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t testing.TB }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// TestPersistLoadRoundTrip 两组各写 HardState+条目，Load 逐组读回一致，
// 组间互不串扰（键前缀隔离的承重断言）。
func TestPersistLoadRoundTrip(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	e := func(g uint32, idx, term uint64) *raftpb.Entry { // 测试内构造器
		typ := raftpb.EntryNormal
		return &raftpb.Entry{Index: &idx, Term: &term, Type: &typ, Data: []byte{byte(g)}}
	}
	one, two := uint64(1), uint64(2)
	hs1 := &raftpb.HardState{Term: &two, Commit: &one}
	if err := rs.Persist(0, hs1, []*raftpb.Entry{e(0, 1, 1), e(0, 2, 2)}, true); err != nil {
		t.Fatal(err)
	}
	if err := rs.Persist(1, nil, []*raftpb.Entry{e(1, 1, 1)}, false); err != nil {
		t.Fatal(err)
	}
	hs, ents, _, err := rs.Load(0)
	if err != nil || len(ents) != 2 || hs.GetCommit() != 1 {
		t.Fatalf("组0 Load = hs.commit %d, %d 条, %v; want 1, 2, nil", hs.GetCommit(), len(ents), err)
	}
	if _, ents1, _, _ := rs.Load(1); len(ents1) != 1 || ents1[0].Data[0] != 1 {
		t.Fatalf("组1 被组0 污染或缺失: %v", ents1)
	}
}

// TestPersistTruncatesConflictTail raft 回退覆盖：重写 index=2 后，
// 旧的 index=3 必须消失（否则 Load 读回幽灵条目，选举后状态机分叉）。
func TestPersistTruncatesConflictTail(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	e := func(idx, term uint64) *raftpb.Entry {
		typ := raftpb.EntryNormal
		return &raftpb.Entry{Index: &idx, Term: &term, Type: &typ}
	}
	if err := rs.Persist(0, nil, []*raftpb.Entry{e(1, 1), e(2, 1), e(3, 1)}, true); err != nil {
		t.Fatal(err)
	}
	if err := rs.Persist(0, nil, []*raftpb.Entry{e(2, 2)}, true); err != nil { // 新任期覆盖 2，3 成幽灵
		t.Fatal(err)
	}
	_, ents, _, err := rs.Load(0)
	if err != nil || len(ents) != 2 || ents[1].GetTerm() != 2 {
		t.Fatalf("覆盖后 Load = %d 条 (末条 term %d), %v; want 2 条、term 2", len(ents), ents[len(ents)-1].GetTerm(), err)
	}
}

// TestEnsureGroupsRejectsMismatch 组数是队列→组映射的分母，换组数=数据
// 错组，必须在启动时挡死。
func TestEnsureGroupsRejectsMismatch(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	if err := rs.EnsureGroups(3); err != nil {
		t.Fatal(err)
	}
	if err := rs.EnsureGroups(3); err != nil { // 幂等
		t.Fatal(err)
	}
	if err := rs.EnsureGroups(4); err == nil {
		t.Fatal("组数 3→4 应拒绝启动，得到 nil")
	}
}

// TestConfStateClampsToCommit 终审观察项②：commit 之外的 ConfChange
// 尾巴不得进成员表。
func TestConfStateClampsToCommit(t *testing.T) {
	cc := func(idx uint64, typ raftpb.ConfChangeType, node uint64) *raftpb.Entry {
		etyp := raftpb.EntryConfChange
		c := raftpb.ConfChange{Type: typ.Enum(), NodeId: &node}
		data, _ := proto.Marshal(&c)
		return &raftpb.Entry{Index: &idx, Type: &etyp, Data: data}
	}
	ents := []*raftpb.Entry{
		cc(1, raftpb.ConfChangeAddNode, 1),
		cc(2, raftpb.ConfChangeAddNode, 2),
		cc(3, raftpb.ConfChangeAddNode, 3), // commit=2：这条是未提交尾巴
	}
	cs, err := confStateFromEntries(ents, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs.Voters) != 2 {
		t.Fatalf("voters = %v; want [1 2]（index 3 未提交，不得纳入）", cs.Voters)
	}
}

// TestConfStateRejectsCorruptEntry 终审 R4：损坏的 ConfChange 条目必须
// 拒启报错，而不是静默跳过——跳过的 RemoveNode 会让被移除的 voter
// 残留成员表，比拒启危险（拒启后 operator 走 WipeForRejoin + learner
// 重入即可恢复）。
func TestConfStateRejectsCorruptEntry(t *testing.T) {
	idx := uint64(1)
	etyp := raftpb.EntryConfChange
	ents := []*raftpb.Entry{
		{Index: &idx, Type: &etyp, Data: []byte{0xff, 0xff}},
	}
	if _, err := confStateFromEntries(ents, 1); err == nil {
		t.Fatal("损坏的 ConfChange 条目应拒启报错，得到 nil")
	}
}

// TestConfStateSurvivesRestartWithoutEntries batch④ 前置：截断之后
// ConfChange 条目已不在日志里，重启不可能再靠重放合成成员表——
// 成员表必须自己持久化。
func TestConfStateSurvivesRestartWithoutEntries(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))

	cs := &raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	if err := rs.SaveConfState(7, cs, 42); err != nil {
		t.Fatalf("SaveConfState: %v", err)
	}
	got, ok, err := rs.LoadConfState(7)
	if err != nil || !ok {
		t.Fatalf("LoadConfState ok=%v err=%v", ok, err)
	}
	if len(got.Voters) != 2 || got.Voters[0] != 1 || got.Voters[1] != 2 || len(got.Learners) != 1 || got.Learners[0] != 3 {
		t.Fatalf("成员表 = %+v; want voters[1 2] learners[3]", got)
	}
	// applied 与成员表同批写入：ConfChange 条目 apply 后 applied 位点
	// 必须一起落盘，否则重启会重放该条 ConfChange（batch③ 遗留缺口）
	ap, err := rs.Applied(7)
	if err != nil || ap != 42 {
		t.Fatalf("applied = %d err=%v; want 42", ap, err)
	}
	if _, ok, _ := rs.LoadConfState(8); ok {
		t.Fatal("未写过的组不得返回成员表")
	}
}

// TestTruncateLogKeepsSuffixAndSnapMeta 截断的两条不变量：
// ① 截断点之后的条目一条不少；② 截断点的 {Index,Term} 必须留在
// snap 元数据里——raft 重启要查 FirstIndex-1 的 term，查不到就拒启。
func TestTruncateLogKeepsSuffixAndSnapMeta(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	var ents []*raftpb.Entry
	for i := uint64(1); i <= 10; i++ {
		idx, term := i, uint64(3)
		ents = append(ents, &raftpb.Entry{Index: &idx, Term: &term, Data: []byte{byte(i)}})
	}
	commit := uint64(10)
	if err := rs.Persist(1, &raftpb.HardState{Commit: &commit}, ents, true); err != nil {
		t.Fatal(err)
	}

	idx, term := uint64(6), uint64(3)
	meta := &raftpb.SnapshotMetadata{Index: &idx, Term: &term,
		ConfState: &raftpb.ConfState{Voters: []uint64{1}}}
	if err := rs.SaveSnapMeta(1, meta); err != nil {
		t.Fatal(err)
	}
	if err := rs.TruncateLog(1, 6); err != nil {
		t.Fatal(err)
	}

	_, got, gotMeta, err := rs.Load(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0].GetIndex() != 7 || got[3].GetIndex() != 10 {
		t.Fatalf("截断后条目 = %d 条，首 %d 末 %d; want 4 条 [7,10]",
			len(got), got[0].GetIndex(), got[len(got)-1].GetIndex())
	}
	if gotMeta == nil || gotMeta.GetIndex() != 6 || gotMeta.GetTerm() != 3 {
		t.Fatalf("snap 元数据 = %+v; want index=6 term=3", gotMeta)
	}
	if len(gotMeta.GetConfState().GetVoters()) != 1 {
		t.Fatalf("snap 元数据必须带成员表（重启 InitialState 用）: %+v", gotMeta.GetConfState())
	}
}

// TestTruncateLogRejectsAboveSnapMeta 截断点不得越过已落盘的 snap 元数据：
// 越过意味着 FirstIndex-1 的 term 无处可查，重启必然拒启。
func TestTruncateLogRejectsAboveSnapMeta(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	idx, term := uint64(5), uint64(2)
	if err := rs.SaveSnapMeta(1, &raftpb.SnapshotMetadata{Index: &idx, Term: &term}); err != nil {
		t.Fatal(err)
	}
	if err := rs.TruncateLog(1, 9); err == nil {
		t.Fatal("截断点 9 > snap 元数据 index 5，必须被拒绝")
	}
}

// TestTruncateLogIsIdempotent 重复截断到同一位点无害（周期截断会撞上）。
func TestTruncateLogIsIdempotent(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	idx, term := uint64(3), uint64(1)
	if err := rs.SaveSnapMeta(1, &raftpb.SnapshotMetadata{Index: &idx, Term: &term}); err != nil {
		t.Fatal(err)
	}
	if err := rs.TruncateLog(1, 3); err != nil {
		t.Fatal(err)
	}
	if err := rs.TruncateLog(1, 3); err != nil {
		t.Fatalf("重复截断必须无害: %v", err)
	}
}

// TestBootGenRoundTrip 机器世代的写入→读回→覆盖写。
func TestBootGenRoundTrip(t *testing.T) {
	st := mustOpenStore(t, t.TempDir())
	rs := newRaftStore(st, testSlog(t))

	if _, ok, err := rs.LoadBootGen(); err != nil || ok {
		t.Fatalf("空库 LoadBootGen = (_, %v, %v); want (_, false, nil)", ok, err)
	}
	if err := rs.SaveBootGen("gen-1"); err != nil {
		t.Fatalf("SaveBootGen: %v", err)
	}
	got, ok, err := rs.LoadBootGen()
	if err != nil || !ok || got != "gen-1" {
		t.Fatalf("LoadBootGen = (%q, %v, %v); want (\"gen-1\", true, nil)", got, ok, err)
	}
	// 覆盖写：每次启动都要写当次世代，旧值不得残留
	if err := rs.SaveBootGen("gen-2"); err != nil {
		t.Fatalf("SaveBootGen 覆盖: %v", err)
	}
	got, _, _ = rs.LoadBootGen()
	if got != "gen-2" {
		t.Fatalf("覆盖后 LoadBootGen = %q; want \"gen-2\"", got)
	}
}

// TestRecoverPermitRoundTrip 许可的写入→读回→被 ForceLocalRecover 消费。
func TestRecoverPermitRoundTrip(t *testing.T) {
	st := mustOpenStore(t, t.TempDir())
	rs := newRaftStore(st, testSlog(t))

	if _, ok, err := rs.LoadRecoverPermit(); err != nil || ok {
		t.Fatalf("空库 LoadRecoverPermit = (_, %v, %v); want (_, false, nil)", ok, err)
	}
	want := recoverPermit{GrantedAt: "2026-08-10T20:00:00+08:00", Gen: "gen-b"}
	if err := rs.SaveRecoverPermit(want); err != nil {
		t.Fatalf("SaveRecoverPermit: %v", err)
	}
	got, ok, err := rs.LoadRecoverPermit()
	if err != nil || !ok || got != want {
		t.Fatalf("LoadRecoverPermit = (%+v, %v, %v); want (%+v, true, nil)", got, ok, err, want)
	}
}

// TestForceLocalRecoverBumpsTermAndConsumesPermit 抬 term + 清 vote + 消费许可，
// 三件事必须一起发生。
//
// 抬 term 是防日志分叉的关键：mem 档掉电可能丢掉投票记录，本节点在任期 T
// 投过 A 却忘了，重启后又在 T 投给 B → 同一任期两个 leader → 日志分叉。
// 抬任期在 raft 里永远安全，抬完就不可能在 T 投第二次。
func TestForceLocalRecoverBumpsTermAndConsumesPermit(t *testing.T) {
	st := mustOpenStore(t, t.TempDir())
	rs := newRaftStore(st, testSlog(t))
	const groups = uint32(3)

	// 造出「组 0..3 各有一个带 term/vote 的 HardState」的现场
	for g := uint32(0); g <= groups; g++ {
		term, vote, commit := uint64(7), uint64(2), uint64(11)
		hs := &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
		if err := rs.Persist(g, hs, nil, true); err != nil {
			t.Fatalf("组 %d 造 HardState: %v", g, err)
		}
	}
	if err := rs.SaveRecoverPermit(recoverPermit{GrantedAt: "t", Gen: "gen-b"}); err != nil {
		t.Fatalf("SaveRecoverPermit: %v", err)
	}

	if err := rs.ForceLocalRecover(groups); err != nil {
		t.Fatalf("ForceLocalRecover: %v", err)
	}

	for g := uint32(0); g <= groups; g++ {
		hs, _, _, err := rs.Load(g)
		if err != nil {
			t.Fatalf("组 %d Load: %v", g, err)
		}
		if hs.GetTerm() != 8 {
			t.Fatalf("组 %d term = %d; want 8（抬一位）", g, hs.GetTerm())
		}
		if hs.GetVote() != 0 {
			t.Fatalf("组 %d vote = %d; want 0——投票记录可能已丢，必须清空才不会在同一任期投第二次", g, hs.GetVote())
		}
		if hs.GetCommit() != 11 {
			t.Fatalf("组 %d commit = %d; want 11（commit 位点不该被动）", g, hs.GetCommit())
		}
	}
	if _, ok, _ := rs.LoadRecoverPermit(); ok {
		t.Fatal("许可在 ForceLocalRecover 之后仍然存在——一次性许可必须被消费掉，否则下次不干净关机会被静默复用")
	}
}

// TestForceLocalRecoverBumpsLegacyHardStateOnUnmigratedDisk 复现 finding 1：
// 未迁移磁盘（legacyPending==true，即组的 HardState 仍在 Pebble 的 hsKey
// 里）上做本地恢复抬任期，bumpTermsInto 若像迁移后的组一样把结果写去
// seglog，会被 legacyPending（只看 hsKey/entPrefix 是否存在）继续判定为
// 「未迁移」，此后的 Load 仍旧读 Pebble 里那份没被更新的旧 HardState——
// 这次 bump 就被静默吞掉，term 卡住不动，「不干净关机后禁止同任期二次
// 投票」这条不变量在升级重启的场景下失效，许可却已经被消费掉。
//
// 断言：bump 后 Load 读到的 term 必须 > 造现场时写入的 9；连续两次
// bump（各自配一次新许可）term 必须持续单调递增，证明每次都真正生效，
// 不是恰好第一次巧合读对。
func TestForceLocalRecoverBumpsLegacyHardStateOnUnmigratedDisk(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	const groups = uint32(0) // 只关心组 0，避免其它组（走 seglog 路径）的噪音

	// 手工写旧键族（模拟旧版本升级重启前、尚未迁移的盘）：hsKey(0) 存在
	// 即 legacyPending(0) 判定为 true。
	b := st.NewBatch()
	term, commit := uint64(9), uint64(3)
	hsData, err := proto.Marshal(&raftpb.HardState{Term: &term, Commit: &commit})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Set(hsKey(0), hsData); err != nil {
		b.Close()
		t.Fatal(err)
	}
	if err := st.ApplyWith(b, true); err != nil {
		t.Fatal(err)
	}

	if err := rs.SaveRecoverPermit(recoverPermit{GrantedAt: "t1", Gen: "gen-b"}); err != nil {
		t.Fatalf("SaveRecoverPermit: %v", err)
	}
	if err := rs.ForceLocalRecover(groups); err != nil {
		t.Fatalf("ForceLocalRecover: %v", err)
	}
	hs, _, _, err := rs.Load(0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if hs.GetTerm() <= term {
		t.Fatalf("bump 后 term = %d; want > %d（bump 必须写回未迁移盘的 legacy hsKey，"+
			"否则被 seglog 路径遮蔽，Load 读不到）", hs.GetTerm(), term)
	}

	// 再抬一次（新许可），term 必须继续单调递增——证明不是巧合读对一次。
	prevTerm := hs.GetTerm()
	if err := rs.SaveRecoverPermit(recoverPermit{GrantedAt: "t2", Gen: "gen-b"}); err != nil {
		t.Fatalf("SaveRecoverPermit 第二次: %v", err)
	}
	if err := rs.ForceLocalRecover(groups); err != nil {
		t.Fatalf("ForceLocalRecover 第二次: %v", err)
	}
	hs2, _, _, err := rs.Load(0)
	if err != nil {
		t.Fatalf("Load 第二次: %v", err)
	}
	if hs2.GetTerm() <= prevTerm {
		t.Fatalf("第二次 bump 后 term = %d; want > %d（term 必须持续单调递增）", hs2.GetTerm(), prevTerm)
	}
}

// TestPersistSurvivesReopenViaSeglog Persist 后重建 raftStore（模拟重启）
// 能读回同样内容——seglog 路径的端到端 roundtrip。
func TestPersistSurvivesReopenViaSeglog(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	term, vote, commit := uint64(2), uint64(1), uint64(1)
	hs := &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
	idx, trm, typ := uint64(1), uint64(2), raftpb.EntryNormal
	if err := rs.Persist(1, hs, []*raftpb.Entry{{Index: &idx, Term: &trm, Type: &typ, Data: []byte("x")}}, true); err != nil {
		t.Fatal(err)
	}
	rs.CloseLogs()
	rs2 := newRaftStore(st, testSlog(t))
	gotHS, gotEnts, _, err := rs2.Load(1)
	if err != nil || gotHS.GetTerm() != 2 || len(gotEnts) != 1 || string(gotEnts[0].Data) != "x" {
		t.Fatalf("重开读回 = %v,%v,%v; want term=2, 1 条目", gotHS, gotEnts, err)
	}
}

// TestResetGroupProgressRemovesSeglogDir 组级重置必须把段目录一并删掉
// ——半截段目录重启会被当作有效日志读回，比脏键更危险。
func TestResetGroupProgressRemovesSeglogDir(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	idx, trm := uint64(1), uint64(1)
	if err := rs.Persist(1, nil, []*raftpb.Entry{{Index: &idx, Term: &trm, Data: []byte("x")}}, true); err != nil {
		t.Fatal(err)
	}
	segDir := filepath.Join(st.Dir(), "raftlog", "1")
	if fis, err := os.ReadDir(segDir); err != nil || len(fis) == 0 {
		t.Fatalf("前置失败：段目录应有文件: %v", err)
	}
	if err := rs.ResetGroupProgress(1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segDir); !os.IsNotExist(err) {
		t.Fatalf("重置后段目录应被删除，stat err = %v", err)
	}
	// 重置后 Load 必须是空日志形态
	gotHS, gotEnts, _, err := rs.Load(1)
	if err != nil || len(gotEnts) != 0 || gotHS.GetTerm() != 0 {
		t.Fatalf("重置后 Load = %v,%v,%v; want 空", gotHS, gotEnts, err)
	}
}

// TestForceLocalRecoverBumpsTermViaSeglog 抬 term 走 seglog 后，重开
// raftStore 能读回抬高的 term（原 TestForceLocalRecoverBumpsTermAndConsumesPermit
// 覆盖 Pebble 侧，此测试补 seglog 侧的持久化）。
func TestForceLocalRecoverBumpsTermViaSeglog(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	term := uint64(4)
	if err := rs.Persist(0, &raftpb.HardState{Term: &term}, nil, true); err != nil {
		t.Fatal(err)
	}
	// 授予许可（ForceLocalRecover 前置；照抄现有 TestForceLocalRecover... 的授予段）
	if err := rs.SaveRecoverPermit(recoverPermit{GrantedAt: "2026-08-11T00:00:00Z", Gen: "g1"}); err != nil {
		t.Fatal(err)
	}
	if err := rs.ForceLocalRecover(0); err != nil {
		t.Fatal(err)
	}
	rs.CloseLogs()
	rs2 := newRaftStore(st, testSlog(t))
	gotHS, _, _, err := rs2.Load(0)
	if err != nil || gotHS.GetTerm() <= 4 {
		t.Fatalf("抬 term 后重开读回 term = %d, %v; want > 4", gotHS.GetTerm(), err)
	}
}

// TestLoadFallsBackToLegacyKeys Pebble 里有旧日志键族（迁移前形态）时
// Load 走 legacy 只读回退——恢复判定命令在迁移前必须能读到旧状态。
func TestLoadFallsBackToLegacyKeys(t *testing.T) {
	st := openClusterTestStore(t)
	// 手工写旧键族（模拟旧版本留下的盘）
	b := st.NewBatch()
	term := uint64(5)
	hsData, _ := proto.Marshal(&raftpb.HardState{Term: &term})
	_ = b.Set(hsKey(2), hsData)
	idx, trm := uint64(7), uint64(5)
	entData, _ := proto.Marshal(&raftpb.Entry{Index: &idx, Term: &trm, Data: []byte("legacy")})
	_ = b.Set(entKey(2, 7), entData)
	if err := st.ApplyWith(b, true); err != nil {
		t.Fatal(err)
	}
	rs := newRaftStore(st, testSlog(t))
	gotHS, gotEnts, _, err := rs.Load(2)
	if err != nil || gotHS.GetTerm() != 5 || len(gotEnts) != 1 || string(gotEnts[0].Data) != "legacy" {
		t.Fatalf("legacy 回退读回 = %v,%v,%v; want term=5 + 1 条 legacy", gotHS, gotEnts, err)
	}
}

// TestMigrateLegacyRaftLogToSeglog 旧盘（Pebble 日志键族）启动迁移后：
// seglog 有全部内容、Pebble 旧键清空、再次迁移是空操作（幂等）。
func TestMigrateLegacyRaftLogToSeglog(t *testing.T) {
	st := openClusterTestStore(t)
	// 造旧盘：直接写 Pebble 旧键族（与 TestLoadFallsBackToLegacyKeys 同法）
	b := st.NewBatch()
	term := uint64(3)
	hsData, _ := proto.Marshal(&raftpb.HardState{Term: &term})
	_ = b.Set(hsKey(1), hsData)
	for i := uint64(1); i <= 3; i++ {
		trm := uint64(3)
		entData, _ := proto.Marshal(&raftpb.Entry{Index: &i, Term: &trm, Data: []byte{byte(i)}})
		_ = b.Set(entKey(1, i), entData)
	}
	if err := st.ApplyWith(b, true); err != nil {
		t.Fatal(err)
	}
	rs := newRaftStore(st, testSlog(t))
	if err := rs.migrateLog(1); err != nil {
		t.Fatal(err)
	}
	// Pebble 旧键必须清空
	if _, ok, _ := st.Get(hsKey(1)); ok {
		t.Fatal("迁移后 Pebble hs 键仍在")
	}
	// seglog 读回全部内容
	rs.CloseLogs()
	rs2 := newRaftStore(st, testSlog(t))
	gotHS, gotEnts, _, err := rs2.Load(1)
	if err != nil || gotHS.GetTerm() != 3 || len(gotEnts) != 3 {
		t.Fatalf("迁移后 Load = %v, %d 条, %v; want term=3, 3 条", gotHS, len(gotEnts), err)
	}
	// 幂等：再迁一次空操作不报错
	if err := rs2.migrateLog(1); err != nil {
		t.Fatalf("重复迁移应为空操作: %v", err)
	}
}

// TestMigrateLegacyLargeLogInChunks 大体量 legacy 日志（跨多个迁移分块）
// 迁移后内容零丢失、顺序完整——迁移分块只是内存峰值优化，任何一条
// 条目丢失或乱序都是迁移缺陷。2500 条 > 2×migrateChunkEntries，保证
// 至少跨 3 个分块（含最后一个非满块）。
func TestMigrateLegacyLargeLogInChunks(t *testing.T) {
	st := openClusterTestStore(t)
	const total = 2500
	b := st.NewBatch()
	term := uint64(7)
	hsData, _ := proto.Marshal(&raftpb.HardState{Term: &term})
	if err := b.Set(hsKey(1), hsData); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= total; i++ {
		trm := uint64(7)
		entData, _ := proto.Marshal(&raftpb.Entry{Index: &i, Term: &trm, Data: []byte{byte(i % 251)}})
		if err := b.Set(entKey(1, i), entData); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.ApplyWith(b, true); err != nil {
		t.Fatal(err)
	}
	rs := newRaftStore(st, testSlog(t))
	if err := rs.migrateLog(1); err != nil {
		t.Fatal(err)
	}
	// 重开读回：全量条目升序连续，首尾与内容抽查一致
	rs.CloseLogs()
	rs2 := newRaftStore(st, testSlog(t))
	gotHS, gotEnts, _, err := rs2.Load(1)
	if err != nil || gotHS.GetTerm() != 7 {
		t.Fatalf("迁移后 Load = %v, %v; want term=7", gotHS, err)
	}
	if len(gotEnts) != total {
		t.Fatalf("迁移后条目数 = %d; want %d", len(gotEnts), total)
	}
	for i, e := range gotEnts {
		want := uint64(i + 1)
		if e.GetIndex() != want || e.Data[0] != byte(want%251) {
			t.Fatalf("第 %d 条形态错误: index=%d data=%v; want index=%d data=%d",
				i, e.GetIndex(), e.Data, want, byte(want%251))
		}
	}
}

// TestTruncateLogReclaimsSegmentsPhysically TruncateLog 在锚点先行守卫下
// 必须把被覆盖的已关闭段物理删掉——e2e 层因生产段大小（64MiB）永不轮转
// 只能观测锚点前进，物理回收的证据由本测试在接口层压低 SegMaxBytes 补足。
func TestTruncateLogReclaimsSegmentsPhysically(t *testing.T) {
	old := seglog.SegMaxBytes
	seglog.SegMaxBytes = 64 // 每条 entry 帧即触发轮转，10 条产出多个已关闭段
	defer func() { seglog.SegMaxBytes = old }()
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	for i := uint64(1); i <= 10; i++ {
		trm := uint64(1)
		if err := rs.Persist(1, nil, []*raftpb.Entry{{Index: &i, Term: &trm, Data: []byte("payload")}}, false); err != nil {
			t.Fatal(err)
		}
	}
	segDir := filepath.Join(st.Dir(), "raftlog", "1")
	before, err := os.ReadDir(segDir)
	if err != nil || len(before) < 3 {
		t.Fatalf("前置失败：应轮转出至少 3 段，得到 %d, %v", len(before), err)
	}
	// 锚点先行：TruncateLog 守卫要求 upto ≤ 锚点
	idx, trm := uint64(8), uint64(1)
	if err := rs.SaveSnapMeta(1, &raftpb.SnapshotMetadata{Index: &idx, Term: &trm}); err != nil {
		t.Fatal(err)
	}
	if err := rs.TruncateLog(1, 8); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) >= len(before) {
		t.Fatalf("TruncateLog(8) 未物理删段: before=%d after=%d", len(before), len(after))
	}
	// 回收后重启读回：日志尾（index 9、10 及 active 段内容）必须完好
	rs.CloseLogs()
	rs2 := newRaftStore(st, testSlog(t))
	_, gotEnts, _, err := rs2.Load(1)
	if err != nil || len(gotEnts) == 0 || gotEnts[len(gotEnts)-1].GetIndex() != 10 {
		t.Fatalf("物理回收后日志尾丢失: %d 条, %v", len(gotEnts), err)
	}
}
