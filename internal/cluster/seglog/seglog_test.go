// seglog_test.go 验证段日志的追加、重开恢复与损坏处理。
// 职责：roundtrip、HS 取末条、冲突回退重放、torn tail 截断、非末段坏帧拒开。
// 边界：不测 raftStore 集成（Task 5 的 raftstore_test 覆盖）。
package seglog

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"
)

// ent 构造一条测试条目。index/term 用指针字段（protobuf-go v2 生成形态）。
func ent(index, term uint64, data string) *raftpb.Entry {
	return &raftpb.Entry{Index: &index, Term: &term, Data: []byte(data)}
}

// hs 构造一条测试 HardState。
func hs(term, vote, commit uint64) *raftpb.HardState {
	return &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
}

func TestAppendReopenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, gotHS, gotEnts, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if gotHS != nil || len(gotEnts) != 0 {
		t.Fatalf("空目录应恢复出 nil HS + 0 条目，得到 %v, %d", gotHS, len(gotEnts))
	}
	if err := l.Append(hs(1, 1, 0), []*raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b")}, false); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(hs(1, 1, 2), []*raftpb.Entry{ent(3, 1, "c")}, true); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	_, gotHS, gotEnts, err = Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	// HS 取最后一条（commit=2 的那条）；条目 1..3 齐全升序
	if gotHS.GetCommit() != 2 {
		t.Fatalf("恢复 HS.Commit = %d; want 2（取末条）", gotHS.GetCommit())
	}
	if len(gotEnts) != 3 || gotEnts[0].GetIndex() != 1 || gotEnts[2].GetIndex() != 3 ||
		string(gotEnts[2].Data) != "c" {
		t.Fatalf("恢复条目形态错误: %+v", gotEnts)
	}
}

func TestReopenReplaysConflictOverwrite(t *testing.T) {
	dir := t.TempDir()
	l, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	// 先写 1..3（term 1），再模拟换届回退：从 index 2 起以 term 2 重写
	if err := l.Append(nil, []*raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")}, false); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(nil, []*raftpb.Entry{ent(2, 2, "B")}, false); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, gotEnts, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	// 后写的赢：index 3 被逻辑截掉，index 2 是 term 2 的新条目
	if len(gotEnts) != 2 || gotEnts[1].GetIndex() != 2 ||
		gotEnts[1].GetTerm() != 2 || string(gotEnts[1].Data) != "B" {
		t.Fatalf("冲突重放形态错误: %+v", gotEnts)
	}
}

func TestReopenTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	l, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(nil, []*raftpb.Entry{ent(1, 1, "good")}, true); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(nil, []*raftpb.Entry{ent(2, 1, "will-be-torn")}, false); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// 掉电模拟：把末段截掉最后 3 字节，第二条帧变 torn
	seg := filepath.Join(dir, "00000001.seg")
	fi, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(seg, fi.Size()-3); err != nil {
		t.Fatal(err)
	}
	l2, _, gotEnts, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatalf("torn tail 应截断续跑，得到错误 %v", err)
	}
	if len(gotEnts) != 1 || gotEnts[0].GetIndex() != 1 {
		t.Fatalf("torn 后应只剩条目 1，得到 %+v", gotEnts)
	}
	// 截断后必须还能继续追加、再重开完好（文件已物理截到好帧边界）
	if err := l2.Append(nil, []*raftpb.Entry{ent(2, 1, "rewritten")}, true); err != nil {
		t.Fatal(err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, gotEnts, err = Open(dir, slog.Default())
	if err != nil || len(gotEnts) != 2 || string(gotEnts[1].Data) != "rewritten" {
		t.Fatalf("torn 截断后续写再恢复失败: %v, %+v", err, gotEnts)
	}
}

func TestOpenRejectsCorruptNonLastSegment(t *testing.T) {
	dir := t.TempDir()
	// 手工造两段：段 1 好帧 + 尾部坏字节，段 2 存在——坏帧不在末段，必须拒开
	good := appendFrame(nil, recEntry, mustMarshalEntry(t, ent(1, 1, "a")))
	if err := os.WriteFile(filepath.Join(dir, "00000001.seg"), append(good, 0xDE, 0xAD), 0o644); err != nil {
		t.Fatal(err)
	}
	seg2 := appendFrame(nil, recEntry, mustMarshalEntry(t, ent(2, 1, "b")))
	if err := os.WriteFile(filepath.Join(dir, "00000002.seg"), seg2, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := Open(dir, slog.Default()); err == nil {
		t.Fatal("非末段坏帧应拒绝打开（真损坏），得到 nil")
	}
}

// mustMarshalEntry 测试辅助：Entry → protobuf 字节。
func mustMarshalEntry(t *testing.T, e *raftpb.Entry) []byte {
	t.Helper()
	b, err := proto.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
