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
	hs, ents, err := rs.Load(0)
	if err != nil || len(ents) != 2 || hs.GetCommit() != 1 {
		t.Fatalf("组0 Load = hs.commit %d, %d 条, %v; want 1, 2, nil", hs.GetCommit(), len(ents), err)
	}
	if _, ents1, _ := rs.Load(1); len(ents1) != 1 || ents1[0].Data[0] != 1 {
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
	_, ents, err := rs.Load(0)
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
	cs := confStateFromEntries(ents, 2)
	if len(cs.Voters) != 2 {
		t.Fatalf("voters = %v; want [1 2]（index 3 未提交，不得纳入）", cs.Voters)
	}
}
