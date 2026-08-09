// raftstore_test.go 验证 raft 日志共库持久层的正确性。
//
// 职责：HardState+Entry 单批往返、回退截断、组数校验、成员表按 commit 裁剪。
// 边界：不涉及 TCP 传输与运行体（后续任务）；openClusterTestStore/testSlog
// 为本包测试共享 helper，后续任务的测试文件直接复用。
package cluster

import (
	"log/slog"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/xushixin/sq/internal/store"
)

// openClusterTestStore 打开临时目录下的测试 store，随测试结束自动关闭。
// 后续任务的测试文件复用本 helper，保证集群层测试共享同一套打开方式。
func openClusterTestStore(t *testing.T) *store.Store {
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
func testSlog(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

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
