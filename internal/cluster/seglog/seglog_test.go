// seglog_test.go 验证段日志的追加、重开恢复与损坏处理。
// 职责：roundtrip、HS 取末条、冲突回退重放、torn tail 截断、非末段坏帧拒开。
// 边界：不测 raftStore 集成（Task 5 的 raftstore_test 覆盖）。
package seglog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"
)

// ent 构造一条测试条目。index/term 用指针字段（protobuf-go v2 生成形态）。
func ent(index, term uint64, data string) *raftpb.Entry {
	return &raftpb.Entry{Index: &index, Term: &term, Data: []byte(data)}
}

// openAll 是 Open + 经读路径读回全部条目：卡 SQ4.10 之前 Open 直接返回
// 全量条目，既有用例的断言按这个四返回值形态写成——经读路径读回同一份
// 内容，断言原样保留，同时顺带覆盖读路径与 Open 重建索引的一致性。
func openAll(dir string, lg *slog.Logger) (*Log, *raftpb.HardState, []*raftpb.Entry, error) {
	l, h, err := Open(dir, lg)
	if err != nil {
		return nil, nil, nil, err
	}
	first, ok := l.FirstIndex()
	if !ok {
		return l, h, nil, nil
	}
	last, _ := l.LastIndex()
	ents, err := l.ReadEntries(first, last+1, math.MaxUint64)
	return l, h, ents, err
}

// hs 构造一条测试 HardState。
func hs(term, vote, commit uint64) *raftpb.HardState {
	return &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
}

func TestAppendReopenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, gotHS, gotEnts, err := openAll(dir, slog.Default())
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
	_, gotHS, gotEnts, err = openAll(dir, slog.Default())
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
	l, _, _, err := openAll(dir, slog.Default())
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
	_, _, gotEnts, err := openAll(dir, slog.Default())
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
	l, _, _, err := openAll(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(nil, []*raftpb.Entry{ent(1, 1, "good")}, true); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(nil, []*raftpb.Entry{ent(2, 1, "will-be-torn")}, false); err != nil {
		t.Fatal(err)
	}
	// 逻辑末尾必须在 Close 之前取自 activeSize，不能事后用 Stat().Size()：
	// 预分配生效时段文件的物理大小恒为 SegMaxBytes，拿它减 3 只会削掉一点
	// 零尾，最后一条帧毫发无损——掉电模拟当场失效，用例变成永远绿的空壳。
	// 这正是 seglog.go 里「凡是需要『写了多少』的地方绝不用 Stat().Size()」
	// 那条不变量在测试侧的同款要求。
	written := l.activeSize
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// 掉电模拟：把末段截到逻辑末尾前 3 字节，第二条帧变 torn
	seg := filepath.Join(dir, "00000001.seg")
	if err := os.Truncate(seg, written-3); err != nil {
		t.Fatal(err)
	}
	l2, _, gotEnts, err := openAll(dir, slog.Default())
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
	_, _, gotEnts, err = openAll(dir, slog.Default())
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
	if _, _, _, err := openAll(dir, slog.Default()); err == nil {
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
	l, _, _, err := openAll(dir, slog.Default())
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
	_, gotHS, _, err := openAll(dir, slog.Default())
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
	l, _, _, err := openAll(dir, slog.Default())
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
	_, _, gotEnts, err := openAll(dir, slog.Default())
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
	l, _, _, err := openAll(dir, slog.Default())
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
	l2, _, _, err := openAll(dir, slog.Default())
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
	l, _, _, err := openAll(dir, slog.Default())
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

	l2, _, ents, err := openAll(dir, slog.Default())
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
	_, _, ents, err = openAll(dir, slog.Default())
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
	l, _, _, err := openAll(dir, slog.Default())
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

	l2, _, ents, err := openAll(dir, slog.Default())
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
	l, _, _, err := openAll(dir, slog.Default())
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
	_, _, ents, err := openAll(dir, slog.Default())
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

// recordHandler 收集 slog 记录，用于断言日志级别。
type recordHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }

// hasMsgAtLevel 是否存在指定级别、消息包含 sub 的记录。
func (h *recordHandler) hasMsgAtLevel(lv slog.Level, sub string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == lv && strings.Contains(r.Message, sub) {
			return true
		}
	}
	return false
}

// TestZeroTailLogsDebugNotWarn 预分配产生的全零尾部必须记 Debug，不能记
// Warn——每次干净重启都打 Warn 会让「检测到 torn tail」这条线上告警彻底
// 失去意义。
func TestZeroTailLogsDebugNotWarn(t *testing.T) {
	old := SegMaxBytes
	SegMaxBytes = 1 << 20
	defer func() { SegMaxBytes = old }()

	dir := t.TempDir()
	l, _, _, err := openAll(dir, slog.Default())
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

	// 无论平台是否支持预分配，都人为补一段全零尾巴，让本用例在两个平台
	// 上都能观测到零尾分支
	f, err := os.OpenFile(filepath.Join(dir, segName(1)), os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(written + 4096); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	h := &recordHandler{}
	l2, _, ents, err := openAll(dir, slog.New(h))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if len(ents) != 1 {
		t.Fatalf("零尾不得影响恢复：得到 %d 条目; want 1", len(ents))
	}
	if l2.activeSize != written {
		t.Fatalf("零尾截断后 activeSize = %d; want %d", l2.activeSize, written)
	}
	if h.hasMsgAtLevel(slog.LevelWarn, "torn tail") {
		t.Fatal("全零尾部被记为 Warn torn tail——干净重启不得触发该告警")
	}
	if !h.hasMsgAtLevel(slog.LevelDebug, "预分配零尾") {
		t.Fatal("全零尾部未记 Debug 预分配零尾")
	}
}

// TestGenuineTornTailStillLogsWarn 非全零的尾部字节仍是真 torn write，
// 必须保留 Warn——本用例守住「不要为了消噪把真告警一起消掉」。
func TestGenuineTornTailStillLogsWarn(t *testing.T) {
	dir := t.TempDir()
	l, _, _, err := openAll(dir, slog.Default())
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

	// 追加半条帧的非零垃圾：模拟掉电时最后一次 write 只落盘一部分
	f, err := os.OpenFile(filepath.Join(dir, segName(1)), os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0x00, 0x00, 0x01, 0xff, 0xde, 0xad}, written); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	h := &recordHandler{}
	l2, _, ents, err := openAll(dir, slog.New(h))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if len(ents) != 1 {
		t.Fatalf("torn tail 截断后得到 %d 条目; want 1", len(ents))
	}
	if !h.hasMsgAtLevel(slog.LevelWarn, "torn tail") {
		t.Fatal("真 torn tail 未记 Warn——告警被消噪消掉了")
	}
}

// ---- 读路径（卡 SQ4.10 冷区，plan §4.2 第 1–4 条）----

// withSegMax 把 SegMaxBytes 临时调成 n（测试结束恢复）。
func withSegMax(t *testing.T, n int64) {
	t.Helper()
	old := SegMaxBytes
	SegMaxBytes = n
	t.Cleanup(func() { SegMaxBytes = old })
}

// payload 第 i 条条目的确定性载荷（size 字节，内容随 i 变化）。
func payload(i uint64, size int) string {
	b := make([]byte, size)
	for k := range b {
		b[k] = byte('a' + (int(i)+k)%26)
	}
	return string(b)
}

// assertRead 读 [lo,hi) 并断言：起于 lo、连续、逐条等于 want[index]、
// 字节上限语义（至少一条；多于一条时总量 ≤ max）。
func assertRead(t *testing.T, l *Log, lo, hi, max uint64, want map[uint64]*raftpb.Entry) {
	t.Helper()
	got, err := l.ReadEntries(lo, hi, max)
	if err != nil {
		t.Fatalf("ReadEntries(%d,%d,%d): %v", lo, hi, max, err)
	}
	if len(got) == 0 {
		t.Fatalf("ReadEntries(%d,%d,%d) 返回 0 条（至少应一条）", lo, hi, max)
	}
	var total uint64
	for k, e := range got {
		if e.GetIndex() != lo+uint64(k) {
			t.Fatalf("第 %d 条 index=%d; want %d", k, e.GetIndex(), lo+uint64(k))
		}
		if !proto.Equal(e, want[e.GetIndex()]) {
			t.Fatalf("index %d 读回 term=%d data=%.16q; want term=%d data=%.16q", e.GetIndex(),
				e.GetTerm(), e.GetData(), want[e.GetIndex()].GetTerm(), want[e.GetIndex()].GetData())
		}
		total += uint64(proto.Size(e))
	}
	if len(got) > 1 && total > max {
		t.Fatalf("ReadEntries(%d,%d,%d) 总量 %d 超上限", lo, hi, max, total)
	}
	if uint64(len(got)) < hi-lo {
		// 短返回只允许因字节上限：再加下一条必超
		next := want[lo+uint64(len(got))]
		if total+uint64(proto.Size(next)) <= max {
			t.Fatalf("ReadEntries(%d,%d,%d) 短返回 %d 条但未触及上限（%d+%d ≤ %d）",
				lo, hi, max, len(got), total, proto.Size(next), max)
		}
	}
}

// TestReadEntriesEqualsAppended 第 1 条：多段（含仅 HardState 轮次、跨检查点
// 的大 run）追加后，任意 [lo,hi) 与字节上限组合读回逐条等于追加值；关闭
// 重开后（Open 重建索引）同样成立。
func TestReadEntriesEqualsAppended(t *testing.T) {
	for _, tc := range []struct {
		name   string
		segMax int64
		n      uint64
		size   int
	}{
		{"小段多轮转", 512, 300, 40},
		{"大 run 跨检查点", 64 << 20, 60, 100 << 10}, // 6MiB 单段 ⇒ 多个 1MiB 检查点
	} {
		t.Run(tc.name, func(t *testing.T) {
			withSegMax(t, tc.segMax)
			dir := t.TempDir()
			l, _, err := Open(dir, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			want := map[uint64]*raftpb.Entry{}
			for i := uint64(1); i <= tc.n; {
				// 轮次形态轮换：仅 HS / HS+条目 / 只条目，批大小 1..4
				switch i % 3 {
				case 0:
					if err := l.Append(hs(1, 1, i-1), nil, false); err != nil {
						t.Fatal(err)
					}
				}
				var batch []*raftpb.Entry
				for k := uint64(0); k < 1+i%4 && i <= tc.n; k++ {
					e := ent(i, 1+i/50, payload(i, tc.size))
					want[i] = e
					batch = append(batch, e)
					i++
				}
				var h *raftpb.HardState
				if i%2 == 0 {
					h = hs(1, 1, i-1)
				}
				if err := l.Append(h, batch, false); err != nil {
					t.Fatal(err)
				}
			}
			check := func(l *Log) {
				t.Helper()
				if f, ok := l.FirstIndex(); !ok || f != 1 {
					t.Fatalf("FirstIndex = %d,%v; want 1", f, ok)
				}
				if la, ok := l.LastIndex(); !ok || la != tc.n {
					t.Fatalf("LastIndex = %d,%v; want %d", la, ok, tc.n)
				}
				entSize := uint64(proto.Size(want[1]))
				for _, max := range []uint64{0, 1, entSize, 3*entSize + 1, math.MaxUint64} {
					for lo := uint64(1); lo <= tc.n; lo += 7 {
						for _, span := range []uint64{1, 2, 13, tc.n} {
							hi := min(lo+span, tc.n+1)
							assertRead(t, l, lo, hi, max, want)
						}
					}
				}
				for i := uint64(1); i <= tc.n; i++ {
					if tm, err := l.Term(i); err != nil || tm != want[i].GetTerm() {
						t.Fatalf("Term(%d) = %d,%v; want %d", i, tm, err, want[i].GetTerm())
					}
				}
			}
			check(l)
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			l2, _, err := Open(dir, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			defer l2.Close()
			check(l2)
		})
	}
}

// TestReadAfterRewriteAcrossSegments 第 2 条：追加 1..100，在跨段位置以新
// 任期改写 60..80 再追加 81..120，读 50..90 得到新版本；任期在 59/60 两侧
// 正确；关闭重开（Open 重建的索引）后同样断言。
func TestReadAfterRewriteAcrossSegments(t *testing.T) {
	withSegMax(t, 600)
	dir := t.TempDir()
	l, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64]*raftpb.Entry{}
	for i := uint64(1); i <= 100; i++ {
		e := ent(i, 1, payload(i, 30))
		want[i] = e
		if err := l.Append(nil, []*raftpb.Entry{e}, false); err != nil {
			t.Fatal(err)
		}
	}
	segsBefore := len(l.Segments())
	var batch []*raftpb.Entry
	for i := uint64(60); i <= 80; i++ {
		e := ent(i, 2, "v2-"+payload(i, 20))
		want[i] = e
		batch = append(batch, e)
	}
	if err := l.Append(hs(2, 2, 59), batch, false); err != nil {
		t.Fatal(err)
	}
	for i := uint64(81); i <= 120; i++ {
		e := ent(i, 2, "v2-"+payload(i, 20))
		want[i] = e
		if err := l.Append(nil, []*raftpb.Entry{e}, false); err != nil {
			t.Fatal(err)
		}
	}
	if segsBefore < 3 {
		t.Fatalf("前置：1..100 只占 %d 段，改写点不跨段", segsBefore)
	}
	check := func(l *Log) {
		t.Helper()
		assertRead(t, l, 50, 91, math.MaxUint64, want)
		assertRead(t, l, 1, 121, math.MaxUint64, want)
		for i, w := range map[uint64]uint64{59: 1, 60: 2, 80: 2, 81: 2, 120: 2, 1: 1} {
			if tm, err := l.Term(i); err != nil || tm != w {
				t.Fatalf("Term(%d) = %d,%v; want %d", i, tm, err, w)
			}
		}
		if _, err := l.Term(121); !errors.Is(err, ErrCompacted) {
			t.Fatalf("Term(121) 越尾应 ErrCompacted，得到 %v", err)
		}
	}
	check(l)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	check(l2)
}

// TestReadAfterTruncateIsCompacted 第 3 条：截断后越界读报 ErrCompacted、
// 不返回已删段内容；TruncateTo 与并发读无数据竞争（-race）、无读到半删
// 文件的错误（读要么 ErrCompacted、要么逐条正确）。
func TestReadAfterTruncateIsCompacted(t *testing.T) {
	withSegMax(t, 400)
	dir := t.TempDir()
	l, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const n = 600
	want := map[uint64]*raftpb.Entry{}
	for i := uint64(1); i <= n; i++ {
		e := ent(i, 1, payload(i, 30))
		want[i] = e
		if err := l.Append(nil, []*raftpb.Entry{e}, false); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var reads, compacted int64
	var mu sync.Mutex
	errs := make(chan error, 8)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			lo := uint64(1 + r)
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := l.ReadEntries(lo, lo+50, math.MaxUint64)
				mu.Lock()
				reads++
				if errors.Is(err, ErrCompacted) {
					compacted++
				}
				mu.Unlock()
				if err != nil && !errors.Is(err, ErrCompacted) {
					errs <- fmt.Errorf("并发读 [%d,%d) 非 compacted 错误: %w", lo, lo+50, err)
					return
				}
				for k, e := range got {
					if e.GetIndex() != lo+uint64(k) || !proto.Equal(e, want[e.GetIndex()]) {
						errs <- fmt.Errorf("并发读 [%d,%d) 第 %d 条内容错误（index %d）", lo, lo+50, k, e.GetIndex())
						return
					}
				}
				lo += 37
				if lo > n-50 {
					lo = uint64(1 + r)
				}
			}
		}(r)
	}
	for upto := uint64(50); upto <= 500; upto += 50 {
		if err := l.TruncateTo(upto); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("并发读 %d 次，其中 ErrCompacted %d 次", reads, compacted)

	first, ok := l.FirstIndex()
	if !ok || first <= 1 || first > 501 {
		t.Fatalf("截断到 500 后 FirstIndex = %d,%v; want (1,501]", first, ok)
	}
	if _, err := l.ReadEntries(first-1, first+5, math.MaxUint64); !errors.Is(err, ErrCompacted) {
		t.Fatalf("读已回收的 %d 应 ErrCompacted，得到 %v", first-1, err)
	}
	if _, err := l.Term(first - 1); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Term(%d) 已回收应 ErrCompacted，得到 %v", first-1, err)
	}
	if _, err := l.ReadEntries(1, 10, math.MaxUint64); !errors.Is(err, ErrCompacted) {
		t.Fatalf("读 [1,10) 应 ErrCompacted，得到 %v", err)
	}
	assertRead(t, l, first, n+1, math.MaxUint64, want)
}

// TestReadStopsAtTornTail 第 4 条：torn tail 重开后读路径止于最后一条好帧。
func TestReadStopsAtTornTail(t *testing.T) {
	dir := t.TempDir()
	l, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64]*raftpb.Entry{}
	for i := uint64(1); i <= 5; i++ {
		e := ent(i, 1, payload(i, 50))
		want[i] = e
		if err := l.Append(nil, []*raftpb.Entry{e}, false); err != nil {
			t.Fatal(err)
		}
	}
	written := l.activeSize
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(dir, "00000001.seg"), written-3); err != nil {
		t.Fatal(err)
	}
	l2, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatalf("torn tail 应截断续跑，得到 %v", err)
	}
	defer l2.Close()
	if last, ok := l2.LastIndex(); !ok || last != 4 {
		t.Fatalf("torn 后 LastIndex = %d,%v; want 4", last, ok)
	}
	assertRead(t, l2, 1, 5, math.MaxUint64, want)
	got, err := l2.ReadEntries(3, 10, math.MaxUint64)
	if err != nil || len(got) != 2 || got[1].GetIndex() != 4 {
		t.Fatalf("读 [3,10) 应止于好帧 4，得到 %d 条 err=%v", len(got), err)
	}
	if _, err := l2.ReadEntries(5, 6, math.MaxUint64); !errors.Is(err, ErrCompacted) {
		t.Fatalf("读 torn 掉的 5 应 ErrCompacted，得到 %v", err)
	}
}

// TestRetainBytesCap 按字节保留的截断上界：从新到旧累加到 ≥ budget 的段
// 都必须保留（upto < 它们最小的 MaxIndex）；总量不够 budget 则一条不截。
func TestRetainBytesCap(t *testing.T) {
	segs := []SegInfo{
		{Seq: 1, Bytes: 100, MaxIndex: 10},
		{Seq: 2, Bytes: 100, MaxIndex: 20},
		{Seq: 3, Bytes: 100, MaxIndex: 30},
		{Seq: 4, Bytes: 50, Active: true},
	}
	for _, c := range []struct {
		budget int64
		want   uint64
	}{
		{0, math.MaxUint64}, {50, math.MaxUint64}, {51, 29}, {150, 29}, {151, 19}, {251, 9}, {350, 9}, {351, 0}, {1 << 40, 0},
	} {
		if got := RetainBytesCap(segs, c.budget); got != c.want {
			t.Errorf("RetainBytesCap(budget=%d) = %d; want %d", c.budget, got, c.want)
		}
	}
}
