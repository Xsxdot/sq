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
	old := SegMaxBytes
	// 逼近零：每轮 Append 后都轮转。实测本仓库 raftpb 版本下，一条
	// hs(3,1,0)+ent(...,"a") 帧合计 31B，第二轮 ent(...,"b") 再加 16B，
	// 累计 47B；brief 原文给的 64 反而刚好卡在两轮总量之上，永远不触发
	// 轮转（已用独立探针测试验证）。改用 40，落在 (31,47] 区间内，两轮
	// 内必定触发轮转，且不影响后面的断言逻辑。
	SegMaxBytes = 40
	defer func() { SegMaxBytes = old }()
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
		t.Fatalf("段数 = %d; want ≥2（SegMaxBytes=%d 应触发轮转）", len(segs), SegMaxBytes)
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
	old := SegMaxBytes
	// 同上：ent(i,1,"x") 每条帧实测 16B。brief 原文的 64 会让轮转发生在
	// 第 4 条之后（4*16=64），此时段 1 的 max=4，TruncateTo(3) 找不到
	// max<=3 的已关闭段可删,断言必挂。改用 40，落在 (32,48] 区间，轮转
	// 发生在第 3 条之后（3*16=48），段 1 的 max=3 恰好可被 upto=3 回收。
	SegMaxBytes = 40
	defer func() { SegMaxBytes = old }()
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
	old := SegMaxBytes
	// 单条 hs(1,1,0) 帧实测 15B；阈值设得比它还小，逼着这一轮「只写 HS、
	// 不带 entry」的 Append 单独触发一次轮转——制造出一个纯 HS、零 entry
	// 的已关闭段（seg1），正是本测试要覆盖的场景。
	SegMaxBytes = 10
	defer func() { SegMaxBytes = old }()
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
		t.Fatalf("段数 = %d; want ≥2（SegMaxBytes=%d 应在纯 HS 轮后触发轮转）", len(before), SegMaxBytes)
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

// TestReopenActiveSizeIsLogicalEnd 重开后 activeSize 必须等于已写入的有效
// 字节数（逻辑末尾），而不是文件的物理大小。
//
// 现状下两者恰好相等（扫描遇到坏帧会物理截断到好帧边界，Stat 发生在扫描
// 之后），所以本用例现在就该绿——它的价值是**回归锚**：预分配落地后
// 物理大小恒为 SegMaxBytes，届时这条断言是唯一挡住「轮转判定读到 64MB
// 立即轮转」的东西。
func TestReopenActiveSizeIsLogicalEnd(t *testing.T) {
	dir := t.TempDir()
	l, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(hs(1, 1, 0), []*raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b")}, true); err != nil {
		t.Fatal(err)
	}
	written := l.activeSize
	if written == 0 {
		t.Fatal("前置条件不成立：写入后 activeSize 仍为 0")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, _, ents, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.activeSize != written {
		t.Fatalf("重开后 activeSize = %d; want %d（逻辑末尾）", l2.activeSize, written)
	}
	if len(ents) != 2 {
		t.Fatalf("重开后恢复 %d 条目; want 2", len(ents))
	}

	// 续写必须接在逻辑末尾之后，不覆盖已有帧
	if err := l2.Append(nil, []*raftpb.Entry{ent(3, 1, "c")}, true); err != nil {
		t.Fatal(err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, ents, err = Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 3 || ents[2].GetIndex() != 3 || string(ents[2].Data) != "c" {
		t.Fatalf("续写后条目形态错误: %+v", ents)
	}
}

// TestPreallocatedActiveSegmentKeepsLogicalActiveSize 预分配生效时，活动段
// 的物理大小是 SegMaxBytes，而 activeSize 必须仍是逻辑末尾。
//
// 这是 spec §2.3 那条顺序约束的守门人：preallocActive 必须发生在
// activeSize 定下来之后。写反了顺序，activeSize 一开就是 SegMaxBytes，
// 重启即触发轮转。
//
// 平台限制（spec §2.3 已声明）：只在预分配真正生效的平台（Linux）可观测，
// 因此断言以 l.prealloc 为前提；macOS 上本用例只验证「未预分配时物理
// 大小仍等于逻辑大小」，写反顺序照样绿——Linux 侧验收不可跳过。
func TestPreallocatedActiveSegmentKeepsLogicalActiveSize(t *testing.T) {
	old := SegMaxBytes
	SegMaxBytes = 1 << 20 // 1MiB：远大于本用例写入量，确保不触发轮转
	defer func() { SegMaxBytes = old }()

	dir := t.TempDir()
	l, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(hs(1, 1, 0), []*raftpb.Entry{ent(1, 1, "a")}, true); err != nil {
		t.Fatal(err)
	}
	written := l.activeSize
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, _, ents, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if len(ents) != 1 {
		t.Fatalf("重开后恢复 %d 条目; want 1", len(ents))
	}
	if l2.activeSize != written {
		t.Fatalf("重开后 activeSize = %d; want %d（逻辑末尾，不是物理大小）", l2.activeSize, written)
	}

	fi, err := os.Stat(filepath.Join(dir, segName(1)))
	if err != nil {
		t.Fatal(err)
	}
	if l2.prealloc {
		if fi.Size() != SegMaxBytes {
			t.Fatalf("预分配生效但段物理大小 = %d; want %d", fi.Size(), SegMaxBytes)
		}
	} else if fi.Size() != written {
		t.Fatalf("未预分配时段物理大小 = %d; want %d", fi.Size(), written)
	}
}

// TestRotationTruncatesClosedSegmentToLogicalSize 已关闭段绝不能带预分配
// 零尾——非末段遇到坏帧走的是「真损坏，拒绝启动」，零尾会让每次重启都
// 撞上它。轮转关段前必须截回逻辑大小。
//
// 断言方式是端到端的：轮转后重开必须成功且条目齐全。若关段带零尾，Open
// 会在扫描非末段时直接返回错误——这比断言文件大小更贴近真实故障形态，
// 且两个平台都能观测（macOS 上不预分配、天然无零尾，用例退化为对现有
// 轮转路径的回归保护）。
func TestRotationTruncatesClosedSegmentToLogicalSize(t *testing.T) {
	old := SegMaxBytes
	SegMaxBytes = 256 // 小段：几条 entry 即触发轮转
	defer func() { SegMaxBytes = old }()

	dir := t.TempDir()
	l, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	// 写到至少产生两次轮转，确保存在「已关闭段」
	for i := uint64(1); i <= 12; i++ {
		if err := l.Append(hs(1, 1, i-1), []*raftpb.Entry{ent(i, 1, "payload-payload")}, true); err != nil {
			t.Fatalf("第 %d 次 Append 失败: %v", i, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	seqs, err := scanSegSeqs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) < 2 {
		t.Fatalf("前置条件不成立：只有 %d 个段，未发生轮转", len(seqs))
	}

	// 已关闭段（除末段外）的物理大小必须等于其逻辑内容——用「重开成功」
	// 断言，因为零尾会让非末段扫描判定为真损坏
	_, _, ents, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatalf("轮转后重开失败（已关闭段疑似带零尾）: %v", err)
	}
	if len(ents) != 12 {
		t.Fatalf("重开后恢复 %d 条目; want 12", len(ents))
	}
	for i, e := range ents {
		if e.GetIndex() != uint64(i+1) {
			t.Fatalf("条目 %d 的 index = %d; want %d", i, e.GetIndex(), i+1)
		}
	}
}
