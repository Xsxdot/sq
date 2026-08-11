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

func TestRotationCarriesHardStateAndRecovers(t *testing.T) {
	old := segMaxBytes
	// 逼近零：每轮 Append 后都轮转。实测本仓库 raftpb 版本下，一条
	// hs(3,1,0)+ent(...,"a") 帧合计 31B，第二轮 ent(...,"b") 再加 16B，
	// 累计 47B；brief 原文给的 64 反而刚好卡在两轮总量之上，永远不触发
	// 轮转（已用独立探针测试验证）。改用 40，落在 (31,47] 区间内，两轮
	// 内必定触发轮转，且不影响后面的断言逻辑。
	segMaxBytes = 40
	defer func() { segMaxBytes = old }()
	dir := t.TempDir()
	l, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(hs(3, 1, 0), []*raftpb.Entry{ent(1, 3, "a")}, false); err != nil {
		t.Fatal(err)
	}
	// 这轮无 HS：轮转后的新段必须自带上一轮 HS 的补写副本
	if err := l.Append(nil, []*raftpb.Entry{ent(2, 3, "b")}, false); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// 至少轮转出两段
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) < 2 {
		t.Fatalf("段数 = %d; want ≥2（segMaxBytes=%d 应触发轮转）", len(segs), segMaxBytes)
	}
	// 删掉第一段模拟截断回收后，HS 仍能从后段恢复（新段首条补写的意义）
	if err := os.Remove(segs[0]); err != nil {
		t.Fatal(err)
	}
	_, gotHS, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if gotHS.GetTerm() != 3 {
		t.Fatalf("删首段后 HS.Term = %d; want 3（轮转补写保住最新 HS）", gotHS.GetTerm())
	}
}

func TestTruncateToDeletesOnlyClosedCoveredSegments(t *testing.T) {
	old := segMaxBytes
	// 同上：ent(i,1,"x") 每条帧实测 16B。brief 原文的 64 会让轮转发生在
	// 第 4 条之后（4*16=64），此时段 1 的 max=4，TruncateTo(3) 找不到
	// max<=3 的已关闭段可删,断言必挂。改用 40，落在 (32,48] 区间，轮转
	// 发生在第 3 条之后（3*16=48），段 1 的 max=3 恰好可被 upto=3 回收。
	segMaxBytes = 40
	defer func() { segMaxBytes = old }()
	dir := t.TempDir()
	l, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := l.Append(nil, []*raftpb.Entry{ent(i, 1, "x")}, false); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err := l.TruncateTo(3); err != nil {
		t.Fatal(err)
	}
	after, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(after) >= len(before) {
		t.Fatalf("TruncateTo(3) 未删任何段: before=%d after=%d", len(before), len(after))
	}
	// 回收后重开：条目 4..5 必须还在（active 段与未覆盖段不删）
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, gotEnts, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if len(gotEnts) == 0 || gotEnts[len(gotEnts)-1].GetIndex() != 5 {
		t.Fatalf("回收后日志尾丢失: %+v", gotEnts)
	}
	for _, e := range gotEnts {
		if e.GetIndex() > 3 {
			continue
		}
	}
}

// TestTruncateToReclaimsHSOnlySegmentAfterReopen 覆盖「纯 HS、零 entry 的
// 已关闭段，在进程重启（Close 后重新 Open）之后仍然可以被 TruncateTo 回收」
// ——Open 的扫描阶段必须给每个已关闭段（哪怕它一条 entry 都没有）在 segMax
// 里占位，否则这类段永远不会出现在 TruncateTo 遍历的 map 里，变成重启后
// 再也删不掉的段。
func TestTruncateToReclaimsHSOnlySegmentAfterReopen(t *testing.T) {
	old := segMaxBytes
	// 单条 hs(1,1,0) 帧实测 15B；阈值设得比它还小，逼着这一轮「只写 HS、
	// 不带 entry」的 Append 单独触发一次轮转——制造出一个纯 HS、零 entry
	// 的已关闭段（seg1），正是本测试要覆盖的场景。
	segMaxBytes = 10
	defer func() { segMaxBytes = old }()
	dir := t.TempDir()
	l, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	// 这一轮只写 HS：seg1 变成纯 HS 段，写完立刻轮转到 seg2
	if err := l.Append(hs(1, 1, 0), nil, false); err != nil {
		t.Fatal(err)
	}
	// 再写一条 entry，让当前活动段里有实际数据（顺带也会再轮转一次，
	// 不影响断言——active 段永远不会被 TruncateTo 删）
	if err := l.Append(nil, []*raftpb.Entry{ent(1, 1, "x")}, false); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(before) < 2 {
		t.Fatalf("段数 = %d; want ≥2（segMaxBytes=%d 应在纯 HS 轮后触发轮转）", len(before), segMaxBytes)
	}
	// 关键步骤：不复用内存里的 l，而是重新 Open——模拟进程重启，验证
	// segMax 是靠 Open 的扫描重建出来的，不是靠内存里没丢过的状态侥幸
	// 蒙对。
	l2, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if err := l2.TruncateTo(0); err != nil {
		t.Fatal(err)
	}
	after, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(after) >= len(before) {
		t.Fatalf("重启重开后 TruncateTo(0) 未删掉纯 HS 段: before=%d after=%d", len(before), len(after))
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
}
