# raft 日志共享单 seglog（跨组 group commit）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 v2-b2-seglog 分支上「每组独立段日志」改造为「全组共享单段文件链 + 帧带组号 + sync 领导者 group commit」，收回 quorum-fsync 档高并发被 −47% 的跨组 fsync 摊销。

**Architecture:** seglog 包从单组 `Log` 实例变为多组共享实例（Append/TruncateTo/Reset 带组号），文件布局从 `raftlog/<g>/NNNNNNNN.seg` 变为 `raftlog/NNNNNNNN.seg` 单链；fsync 从持锁内联改为「写段持 mu、刷盘走 syncMu 排队 + 水位搭车」的 group commit；段回收按「段内全部组水位均越过」判定；组重置从删目录退化为逻辑标记帧。raftStore 对外接口签名与语义零变化。

**Tech Stack:** Go 1.24+，`go.etcd.io/raft/v3`（raftpb 用**指针字段**，如 `Index: &idx`），`google.golang.org/protobuf`，CRC32C（Castagnoli）。

**Spec:** `docs/superpowers/specs/2026-08-11-raftlog-shared-seglog-design.md`（本计划的唯一权威；冲突以 spec 为准）

## Global Constraints

- 工作分支：`v2-b2-seglog-shared`（由派发基线自动创建，基线 = `v2-b2-seglog` 的 tip，在现有 seglog 代码上改造）。改动经 handoff 回程同步，**不要**自行 `git push` 到 origin。
- **raftstore_test.go 现有 12 个用例零修改通过**是 Task 4 的硬验收锚；增补用例（`TestMigrateLegacyLargeLogInChunks`、`TestTruncateLogReclaimsSegmentsPhysically`）允许适配共享形态但断言意图不变。
- raftpb 全部字段是指针：构造用 `&raftpb.Entry{Index: &idx, Term: &term}` 形态，读用 `GetIndex()`。
- 日志一律 `slog`（包内已有 `lg *slog.Logger` 注入模式），**禁止** `fmt.Printf`。
- 每个改动的文件保持/更新文件头「职责+边界」注释；新导出符号必须有 doc comment；复杂逻辑写中文「为什么」注释——现有代码的注释密度就是基准线。
- 每个 task 结束：`gofmt -w` 改动文件，`go test ./internal/cluster/... -count=1` 全绿后才 commit。
- commit 尾行：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- `SegMaxBytes` 保持导出变量（测试旋钮），生产默认 64MiB 不变。
- 旧的每组独立布局（`raftlog/` 下纯数字子目录）不做自动迁移：检测到即 fail-stop 拒启（spec §3 守卫）。

---

### Task 1: 帧组号化 + Log 多组化（seglog 包主体重写）

**Files:**
- Modify: `internal/cluster/seglog/frame.go`
- Modify: `internal/cluster/seglog/frame_test.go`
- Modify: `internal/cluster/seglog/seglog.go`
- Modify: `internal/cluster/seglog/seglog_test.go`

**Interfaces:**
- Consumes: 现有 seglog 包（每组独立形态）。
- Produces（后续全部 task 依赖的最终 API 形态，本 task 落地其中大部分）:

```go
// GroupState 是 Open 恢复出的一组状态。
type GroupState struct {
    HS   *raftpb.HardState // 该组最新 HardState；从未写过为 nil
    Ents []*raftpb.Entry   // 「后写的赢」重放后的连续条目序列（升序）
}

func Open(dir string, lg *slog.Logger) (*Log, map[uint32]*GroupState, error)
func (l *Log) Append(g uint32, hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error
func (l *Log) Sync() error
func (l *Log) Close() error
// TruncateTo/Reset 在 Task 2 落地
```

- 帧层（包内私有）：

```go
const (
    recEntry      byte = 1
    recHardState  byte = 2
    recGroupReset byte = 3 // Task 2 使用；本 task 先定义常量与编解码支持
)

// 帧 = [4B len BE][4B CRC32C][1B type][4B group BE][payload]
// len = 1 + 4 + len(payload)；CRC 覆盖 type+group+payload。
func appendFrame(dst []byte, typ byte, g uint32, payload []byte) []byte
func readFrame(buf []byte) (typ byte, g uint32, payload []byte, n int, err error)
```

- [ ] **Step 1: 写帧层失败测试**

改写 `frame_test.go`：roundtrip 带组号、CRC 覆盖组号（篡改组号字节必须判 torn）、reset 记录类型无 payload roundtrip。

```go
func TestFrameRoundTripWithGroup(t *testing.T) {
    buf := appendFrame(nil, recEntry, 7, []byte("payload-A"))
    buf = appendFrame(buf, recHardState, 0, []byte("hs"))
    buf = appendFrame(buf, recGroupReset, 3, nil)

    typ, g, payload, n, err := readFrame(buf)
    if err != nil || typ != recEntry || g != 7 || string(payload) != "payload-A" {
        t.Fatalf("第一帧: typ=%d g=%d payload=%q err=%v", typ, g, payload, err)
    }
    buf = buf[n:]
    typ, g, payload, n, err = readFrame(buf)
    if err != nil || typ != recHardState || g != 0 || string(payload) != "hs" {
        t.Fatalf("第二帧: typ=%d g=%d payload=%q err=%v", typ, g, payload, err)
    }
    buf = buf[n:]
    typ, g, payload, _, err = readFrame(buf)
    if err != nil || typ != recGroupReset || g != 3 || len(payload) != 0 {
        t.Fatalf("第三帧: typ=%d g=%d payload=%q err=%v", typ, g, payload, err)
    }
}

func TestFrameCRCCoversGroup(t *testing.T) {
    buf := appendFrame(nil, recEntry, 7, []byte("payload"))
    // 组号字段是帧体第 2..5 字节（帧头 8B 之后：1B type + 4B group）。
    // 把组号 7 改成 8：CRC 必须失配——组号写花等同帧损坏，
    // 绝不能把 A 组条目错归 B 组。
    buf[frameHeaderLen+1+3] ^= 0x0F
    if _, _, _, _, err := readFrame(buf); err == nil {
        t.Fatal("篡改组号后 readFrame 仍通过——CRC 没有覆盖组号")
    }
}
```

- [ ] **Step 2: 跑帧测试确认失败**

Run: `go test ./internal/cluster/seglog/ -run TestFrame -v`
Expected: FAIL（编译错——appendFrame/readFrame 还没有组号参数）

- [ ] **Step 3: 实现帧层**

`frame.go`：帧体从 `[1B type][payload]` 变为 `[1B type][4B group BE][payload]`。

```go
const (
    recEntry      byte = 1 // payload = raftpb.Entry protobuf
    recHardState  byte = 2 // payload = raftpb.HardState protobuf
    recGroupReset byte = 3 // payload 为空：该组此前全部帧作废（spec §7）
)

// frameHeaderLen = 4B len + 4B CRC。len = 1B type + 4B group + payload 长度。
const frameHeaderLen = 8

// frameBodyMeta = 帧体内 type + group 的固定字节数。
const frameBodyMeta = 5

func appendFrame(dst []byte, typ byte, g uint32, payload []byte) []byte {
    var hdr [frameHeaderLen]byte
    binary.BigEndian.PutUint32(hdr[0:4], uint32(frameBodyMeta+len(payload)))
    var meta [frameBodyMeta]byte
    meta[0] = typ
    binary.BigEndian.PutUint32(meta[1:5], g)
    crc := crc32.Update(0, castagnoli, meta[:])
    crc = crc32.Update(crc, castagnoli, payload)
    binary.BigEndian.PutUint32(hdr[4:8], crc)
    dst = append(dst, hdr[:]...)
    dst = append(dst, meta[:]...)
    return append(dst, payload...)
}

func readFrame(buf []byte) (typ byte, g uint32, payload []byte, n int, err error) {
    if len(buf) < frameHeaderLen {
        return 0, 0, nil, 0, errTornFrame
    }
    ln := binary.BigEndian.Uint32(buf[0:4])
    if ln < frameBodyMeta || ln > maxFrameLen || int(ln) > len(buf)-frameHeaderLen {
        return 0, 0, nil, 0, errTornFrame
    }
    body := buf[frameHeaderLen : frameHeaderLen+int(ln)]
    if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(buf[4:8]) {
        return 0, 0, nil, 0, errTornFrame
    }
    return body[0], binary.BigEndian.Uint32(body[1:5]), body[frameBodyMeta:], frameHeaderLen + int(ln), nil
}
```

- [ ] **Step 4: 写 Log 多组化失败测试**

改写 `seglog_test.go` 的核心用例为多组形态（原单组用例逐个升级，不保留单组 API 假设）。必须覆盖：

```go
// mkEntry 测试辅助：构造带 index/term 的条目（raftpb 指针字段）。
func mkEntry(index, term uint64, data string) *raftpb.Entry {
    return &raftpb.Entry{Index: &index, Term: &term, Data: []byte(data)}
}

func mkHS(term, commit uint64) *raftpb.HardState {
    var vote uint64
    return &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
}

// 多组交错追加，重启扫描按组分流恢复。
func TestOpenDemuxesInterleavedGroups(t *testing.T) {
    dir := t.TempDir()
    lg := slog.New(slog.NewTextHandler(io.Discard, nil))
    l, states, err := Open(dir, lg)
    if err != nil {
        t.Fatal(err)
    }
    if len(states) != 0 {
        t.Fatalf("空目录应无组状态，得到 %d 组", len(states))
    }
    // 组 0 与组 2 交错写入
    if err := l.Append(0, mkHS(1, 0), []*raftpb.Entry{mkEntry(1, 1, "g0-1")}, false); err != nil {
        t.Fatal(err)
    }
    if err := l.Append(2, mkHS(3, 0), []*raftpb.Entry{mkEntry(1, 3, "g2-1"), mkEntry(2, 3, "g2-2")}, false); err != nil {
        t.Fatal(err)
    }
    if err := l.Append(0, nil, []*raftpb.Entry{mkEntry(2, 1, "g0-2")}, true); err != nil {
        t.Fatal(err)
    }
    if err := l.Close(); err != nil {
        t.Fatal(err)
    }

    _, states, err = Open(dir, lg)
    if err != nil {
        t.Fatal(err)
    }
    g0, g2 := states[0], states[2]
    if g0 == nil || g2 == nil {
        t.Fatalf("应恢复组 0 与组 2，得到 %v", states)
    }
    if len(g0.Ents) != 2 || g0.Ents[1].GetIndex() != 2 || string(g0.Ents[1].Data) != "g0-2" {
        t.Fatalf("组 0 条目错: %+v", g0.Ents)
    }
    if len(g2.Ents) != 2 || g2.HS.GetTerm() != 3 {
        t.Fatalf("组 2 状态错: ents=%d term=%d", len(g2.Ents), g2.HS.GetTerm())
    }
}

// 组内冲突回退「后写的赢」，且不串扰其他组。
func TestReplayConflictCutIsPerGroup(t *testing.T) {
    dir := t.TempDir()
    lg := slog.New(slog.NewTextHandler(io.Discard, nil))
    l, _, _ := Open(dir, lg)
    // 组 1 写到 index 5，组 2 写到 index 5
    for i := uint64(1); i <= 5; i++ {
        l.Append(1, nil, []*raftpb.Entry{mkEntry(i, 1, "old")}, false)
        l.Append(2, nil, []*raftpb.Entry{mkEntry(i, 1, "keep")}, false)
    }
    // 组 1 换届回退：从 index 3 重写
    if err := l.Append(1, mkHS(2, 2), []*raftpb.Entry{mkEntry(3, 2, "new3"), mkEntry(4, 2, "new4")}, true); err != nil {
        t.Fatal(err)
    }
    l.Close()

    _, states, err := Open(dir, lg)
    if err != nil {
        t.Fatal(err)
    }
    g1 := states[1]
    if len(g1.Ents) != 4 || g1.Ents[3].GetIndex() != 4 || string(g1.Ents[2].Data) != "new3" {
        t.Fatalf("组 1 冲突裁剪错: %+v", g1.Ents)
    }
    if len(states[2].Ents) != 5 { // 组 2 完全不受组 1 回退影响
        t.Fatalf("组 2 被串扰: %d 条", len(states[2].Ents))
    }
}

// 轮转：新段头部逐组补写 HS；多组交错也能跨段恢复。
func TestRotationCarriesAllGroupsHS(t *testing.T) {
    dir := t.TempDir()
    lg := slog.New(slog.NewTextHandler(io.Discard, nil))
    old := SegMaxBytes
    SegMaxBytes = 128 // 逼小段，快速触发轮转
    defer func() { SegMaxBytes = old }()

    l, _, _ := Open(dir, lg)
    l.Append(0, mkHS(1, 1), []*raftpb.Entry{mkEntry(1, 1, "g0")}, false)
    l.Append(1, mkHS(5, 2), []*raftpb.Entry{mkEntry(1, 5, "g1")}, false)
    // 只有组 1 继续写，写到触发多次轮转
    for i := uint64(2); i <= 20; i++ {
        if err := l.Append(1, nil, []*raftpb.Entry{mkEntry(i, 5, "填充填充填充填充")}, false); err != nil {
            t.Fatal(err)
        }
    }
    l.Close()

    segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
    if len(segs) < 2 {
        t.Fatalf("应发生轮转，只有 %d 段", len(segs))
    }
    _, states, err := Open(dir, lg)
    if err != nil {
        t.Fatal(err)
    }
    // 组 0 的 HS 必须还能恢复——哪怕它只在第一段写过，
    // 轮转补写保证每个新段头部都带全部组的最新 HS。
    if states[0] == nil || states[0].HS.GetTerm() != 1 {
        t.Fatalf("组 0 HS 未被轮转补写带上: %+v", states[0])
    }
    if states[1].HS.GetTerm() != 5 || len(states[1].Ents) != 20 {
        t.Fatalf("组 1 状态错: term=%d ents=%d", states[1].HS.GetTerm(), len(states[1].Ents))
    }
}
```

另外把现有 torn tail / 非末段损坏 / HS-only 轮次 / 目录 fsync 等用例机械升级为多组签名（组号任意固定值），断言意图不变。

- [ ] **Step 5: 跑 Log 测试确认失败**

Run: `go test ./internal/cluster/seglog/ -count=1`
Expected: FAIL（编译错——Open 返回值、Append 签名都变了）

- [ ] **Step 6: 实现 Log 多组化**

`seglog.go` 主体改造（保留 syncDir/mkdirAllSync/scanSegSeqs/轮转屏障骨架，改数据结构与扫描）：

```go
type Log struct {
    mu         sync.Mutex
    dir        string
    lg         *slog.Logger
    active     *os.File
    activeSeq  uint64
    activeSize int64
    // 逐组内存态。组号 → 值；组从未出现即不在 map 里。
    lastIndex map[uint32]uint64            // 各组日志尾
    lastHS    map[uint32]*raftpb.HardState // 各组最新 HS（轮转补写来源）
    activeMax map[uint32]uint64            // 当前活动段内各组已写入的最大 entry index
    // segMax 已关闭段号 → {组 → 段内最大 entry index}。回收判定：段可删 ⟺
    // 对登记表内每个组 g'，marks[g'] >= segMax[段][g']（Task 2）。
    // stale-high 方向安全性与旧实现同（见 TruncateTo 注释）。
    segMax map[uint64]map[uint32]uint64
    buf    []byte
}
```

- `Open`：扫描循环里 `readFrame` 多接一个组号，按组分流到 `map[uint32]*replayState`（包内私有结构 `{hs, ents, lastIndex}`）；组内「后写的赢」线性回退裁剪逻辑照旧、但只作用于该组自己的 ents；`segMax` 累积按组（非末段预置空 map 占位，扫到 entry 帧写 `segMax[seq][g]`）。`recGroupReset` 帧的处理本 task 先落一个分支骨架：清空该组重放状态 + 从已扫 segMax 中移除该组（Task 2 的测试才正式覆盖它，实现顺手写全，避免 Task 2 返工扫描层）。返回 `map[uint32]*GroupState`。
- **旧布局守卫**：`Open` 开头（`scanSegSeqs` 之前）检查 `raftlog/` 下是否有纯数字命名子目录，有则返回错误：`"seglog: 检测到未发布的每组独立布局（raftlog/%s/）——该开发期格式不提供自动迁移，请清空数据目录重入集群（WipeForRejoin/sq recover）或回退旧构建"`。
- `Append(g, hs, ents, sync)`：帧编码带组号；更新 `lastIndex[g]/lastHS[g]/activeMax[g]`；本 task fsync 仍持锁内联（`sync=true` → `l.active.Sync()`），group commit 留给 Task 3。
- `maybeRotate`：HS 补写改为**逐组**（按组号升序遍历 `lastHS`，每组一条 recHardState 帧），全部写完后单次 fsync；`segMax[oldSeq]` = `activeMax` 的拷贝，随后 `activeMax` 置新空 map。回收资格最后授予的顺序纪律与注释原样保留。
- `Sync`/`Close` 签名不变。
- `TruncateTo` 本 task 先改签名为 `TruncateTo(g uint32, upto uint64)` 并留最小实现（只按「该组水位」删仅含该组的段即可编译；完整全组水位逻辑与测试在 Task 2）——**不许留 TODO 注释**，写成正确但保守的版本：仅当段登记表内只有组 g 且 `segMax[seq][g] <= upto` 时删。

- [ ] **Step 7: 跑测试确认通过**

Run: `go test ./internal/cluster/seglog/ -count=1 -v`
Expected: PASS（注意：`internal/cluster` 主包此刻编译失败是预期的——raftstore.go 还在用旧 API，Task 4 处理；本 task 只要求 seglog 子包全绿）

- [ ] **Step 8: 补关键节点日志**

- `Open` 完成日志改为多组汇总：`"seglog: 打开完成", "segments", n, "groups", len(states), "tornTruncated", ...`，另按组 Debug 一行（组号、条目数、commit）；
- 旧布局守卫命中：Error 级、带发现的子目录名；
- 轮转日志加 `"carriedHS", len(lastHS)`（补写了几组）；
- 错误分支全部带段号/偏移/组号上下文（现有纪律延续）。

- [ ] **Step 9: 补注释**

- `seglog.go`/`frame.go` 文件头职责边界改为共享多组形态（「每组一份」的表述全部清理）；
- `Log` 结构体逐字段注释更新（尤其 segMax 的按组语义与 stale-high 论证）；
- 帧格式注释更新（组号进 CRC 的为什么）；
- 逐组 HS 补写的不变式注释（「每组的最新 HS 一定在最新段里」）。

- [ ] **Step 10: gofmt + 提交**

```bash
gofmt -w internal/cluster/seglog/
git add internal/cluster/seglog/
git commit -m "feat(seglog): 帧带组号、Log 多组化——单段链共享全组写入

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: TruncateTo 全组水位回收 + Reset 标记帧

**Files:**
- Modify: `internal/cluster/seglog/seglog.go`
- Modify: `internal/cluster/seglog/seglog_test.go`

**Interfaces:**
- Consumes: Task 1 的多组 Log。
- Produces:

```go
func (l *Log) TruncateTo(g uint32, upto uint64) error // 全组水位版
func (l *Log) Reset(g uint32) error                   // 追加 recGroupReset 帧 + fsync + 内存清空
```

- [ ] **Step 1: 写失败测试**

```go
// 跨组 pin：双组写进同一段，仅一组截断不删段；另一组水位越过后删。
func TestTruncateRequiresAllGroupsWatermark(t *testing.T) {
    dir := t.TempDir()
    lg := slog.New(slog.NewTextHandler(io.Discard, nil))
    old := SegMaxBytes
    SegMaxBytes = 64
    defer func() { SegMaxBytes = old }()

    l, _, _ := Open(dir, lg)
    // 两组交错写满多个段（每条都超过 64B，逐条轮转）
    for i := uint64(1); i <= 4; i++ {
        l.Append(1, nil, []*raftpb.Entry{mkEntry(i, 1, "组1的填充数据组1的填充数据组1的填充数据")}, false)
        l.Append(2, nil, []*raftpb.Entry{mkEntry(i, 1, "组2的填充数据组2的填充数据组2的填充数据")}, false)
    }
    before, _ := filepath.Glob(filepath.Join(dir, "*.seg"))

    // 只有组 1 截断到 4：混有组 2 条目的段必须全部保留
    if err := l.TruncateTo(1, 4); err != nil {
        t.Fatal(err)
    }
    afterOne, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
    if len(afterOne) != len(before) {
        t.Fatalf("组 2 水位未过就删了段: %d -> %d", len(before), len(afterOne))
    }
    // 组 2 也截断到 4：混合段现在可以回收（活动段除外）
    if err := l.TruncateTo(2, 4); err != nil {
        t.Fatal(err)
    }
    afterBoth, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
    if len(afterBoth) >= len(before) {
        t.Fatalf("全组水位越过后段未回收: %d -> %d", len(before), len(afterBoth))
    }
    // 回收后重启，两组剩余状态仍可恢复（HS 补写不变式）
    l.Close()
    if _, _, err := Open(dir, lg); err != nil {
        t.Fatalf("回收后重启失败: %v", err)
    }
}

// Reset：逻辑清空 + 重启后该组无状态 + 其他组不受影响。
func TestResetClearsGroupLogically(t *testing.T) {
    dir := t.TempDir()
    lg := slog.New(slog.NewTextHandler(io.Discard, nil))
    l, _, _ := Open(dir, lg)
    l.Append(1, mkHS(3, 2), []*raftpb.Entry{mkEntry(1, 3, "a"), mkEntry(2, 3, "b")}, true)
    l.Append(2, mkHS(1, 1), []*raftpb.Entry{mkEntry(1, 1, "keep")}, true)

    if err := l.Reset(1); err != nil {
        t.Fatal(err)
    }
    l.Close()

    _, states, err := Open(dir, lg)
    if err != nil {
        t.Fatal(err)
    }
    if states[1] != nil {
        t.Fatalf("组 1 已重置，重启后应无状态，得到 %+v", states[1])
    }
    if states[2] == nil || len(states[2].Ents) != 1 {
        t.Fatalf("组 2 被 Reset(1) 波及: %+v", states[2])
    }
}

// Reset 后重新写入：从零累积，与旧帧无冲突纠缠。
func TestResetThenAppendStartsFresh(t *testing.T) {
    dir := t.TempDir()
    lg := slog.New(slog.NewTextHandler(io.Discard, nil))
    l, _, _ := Open(dir, lg)
    l.Append(1, mkHS(2, 5), []*raftpb.Entry{mkEntry(5, 2, "old5")}, true)
    l.Reset(1)
    // 快照安装后从 snapIndex+1=101 起写——与旧 index 5 有「间隙」，
    // 重放不得把旧帧复活出来填充
    l.Append(1, mkHS(4, 100), []*raftpb.Entry{mkEntry(101, 4, "new101")}, true)
    l.Close()

    _, states, err := Open(dir, lg)
    if err != nil {
        t.Fatal(err)
    }
    g1 := states[1]
    if len(g1.Ents) != 1 || g1.Ents[0].GetIndex() != 101 || g1.HS.GetTerm() != 4 {
        t.Fatalf("Reset 后重写状态错: %+v", g1)
    }
}

// Reset 让「仅含该组帧」的已关闭段立即具备回收资格（空登记表 = 恒可删）。
func TestResetMakesOrphanSegmentsReclaimable(t *testing.T) {
    dir := t.TempDir()
    lg := slog.New(slog.NewTextHandler(io.Discard, nil))
    old := SegMaxBytes
    SegMaxBytes = 64
    defer func() { SegMaxBytes = old }()

    l, _, _ := Open(dir, lg)
    for i := uint64(1); i <= 3; i++ { // 组 1 独占写满几个段
        l.Append(1, nil, []*raftpb.Entry{mkEntry(i, 1, "组1独占的填充数据组1独占的填充数据")}, false)
    }
    before, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
    l.Reset(1)
    // 任意组的一次截断扫描都应把空登记段清走——用一个毫无关系的组触发
    if err := l.TruncateTo(9, 0); err != nil {
        t.Fatal(err)
    }
    after, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
    if len(after) >= len(before) {
        t.Fatalf("Reset 后孤儿段未被回收: %d -> %d", len(before), len(after))
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/seglog/ -run 'TestTruncate|TestReset' -v`
Expected: FAIL（Reset 未定义；TruncateTo 还是 Task 1 的保守版，跨组混合段删不掉）

- [ ] **Step 3: 实现**

```go
// marks 加进 Log 结构体：组 → 截断水位（TruncateTo 抬升，只增不减）。
marks map[uint32]uint64
```

- `TruncateTo(g, upto)`：持 mu，`marks[g] = max(marks[g], upto)`；遍历 `segMax`，段可删 ⟺ 登记表内每个组 g' 满足 `l.marks[g'] >= segMax[seq][g']`（空登记表恒真）；删文件、清登记。日志带删段清单、释放字节；未删任何段且存在被 pin 的段时，Debug 打出「最慢组」（登记表内 `segMax[seq][g'] - marks[g']` 最大的组）——回答「盘怎么还没回收」。
- `Reset(g)`：持 mu，追加 recGroupReset 帧（`appendFrame(l.buf[:0], recGroupReset, g, nil)`）并**立即 `l.active.Sync()`**（低频事件，一次 fsync 换掉「标记悬页缓存+掉电」整类推理，spec §7）；`delete(l.lastIndex, g)`、`delete(l.lastHS, g)`、`delete(l.activeMax, g)`；遍历 `segMax` 逐段 `delete(segMax[seq], g)`。Warn 日志带组号。注意 Reset 也要走 `maybeRotate`（标记帧也占字节）。
- `Open` 扫描的 recGroupReset 分支（Task 1 已写骨架）：确认清空该组 replayState、从 segMax 累积中移除该组；本 task 的测试正式覆盖它。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/seglog/ -count=1`
Expected: PASS

- [ ] **Step 5: 补日志与注释**

- `TruncateTo` 注释重写：全组水位判定的为什么、有界性论证引用（截断循环每 30s 覆盖全部组含 meta 组 0——manager.go `g < m.Groups()` 循环）、stale-high 方向安全性；
- `Reset` 注释：为什么立即 fsync；为什么「标记段先于死帧段被回收」是安全的——重放裁剪规则 + MemoryStorage 按锚点丢弃 + 安装标记重复重置三重兜底（spec §7 崩溃窗口方向）；
- marks 字段注释：水位只在本进程内存、重启后归零，段回收在下一轮截断循环全组跑完后收敛（与旧实现同形态）。

- [ ] **Step 6: gofmt + 提交**

```bash
gofmt -w internal/cluster/seglog/
git add internal/cluster/seglog/
git commit -m "feat(seglog): 全组水位段回收 + Reset 逻辑标记帧

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: group commit——sync 领导者 + 水位搭车

**Files:**
- Modify: `internal/cluster/seglog/seglog.go`
- Modify: `internal/cluster/seglog/seglog_test.go`

**Interfaces:**
- Consumes: Task 1/2 的多组 Log（fsync 仍持锁内联）。
- Produces: `Append(sync=true)` 与 `Sync()` 改走 `syncTo`；外部签名零变化。

- [ ] **Step 1: 写失败测试**

```go
// 并发 sync 追加：全部返回后数据完整、可重放。摊销比例不做单测断言
// （时序敏感，交给三机基准），这里只锁正确性：水位机制不丢任何一组的
// durable 承诺。
func TestConcurrentSyncAppendsAllDurable(t *testing.T) {
    dir := t.TempDir()
    lg := slog.New(slog.NewTextHandler(io.Discard, nil))
    l, _, _ := Open(dir, lg)

    const groups = 4
    const perGroup = 50
    var wg sync.WaitGroup
    errs := make(chan error, groups)
    for g := uint32(0); g < groups; g++ {
        wg.Add(1)
        go func(g uint32) {
            defer wg.Done()
            for i := uint64(1); i <= perGroup; i++ {
                if err := l.Append(g, nil, []*raftpb.Entry{mkEntry(i, 1, "并发写入负载")}, true); err != nil {
                    errs <- fmt.Errorf("组 %d 条目 %d: %w", g, i, err)
                    return
                }
            }
        }(g)
    }
    wg.Wait()
    close(errs)
    for err := range errs {
        t.Fatal(err)
    }
    l.Close()

    _, states, err := Open(dir, lg)
    if err != nil {
        t.Fatal(err)
    }
    for g := uint32(0); g < groups; g++ {
        if states[g] == nil || len(states[g].Ents) != perGroup {
            t.Fatalf("组 %d 恢复条目数 %d != %d", g, len(states[g]), perGroup)
        }
    }
}

// 并发 sync 与轮转共存：小段阈值下高并发追加，ErrClosed 重试路径必须
// 走通（sync 领导者抓到的句柄可能已被轮转关闭）。
func TestConcurrentSyncSurvivesRotation(t *testing.T) {
    dir := t.TempDir()
    lg := slog.New(slog.NewTextHandler(io.Discard, nil))
    old := SegMaxBytes
    SegMaxBytes = 256
    defer func() { SegMaxBytes = old }()

    l, _, _ := Open(dir, lg)
    var wg sync.WaitGroup
    errs := make(chan error, 4)
    for g := uint32(0); g < 4; g++ {
        wg.Add(1)
        go func(g uint32) {
            defer wg.Done()
            for i := uint64(1); i <= 30; i++ {
                if err := l.Append(g, nil, []*raftpb.Entry{mkEntry(i, 1, "轮转窗口内的并发同步写入负载")}, true); err != nil {
                    errs <- err
                    return
                }
            }
        }(g)
    }
    wg.Wait()
    close(errs)
    for err := range errs {
        t.Fatal(err)
    }
    segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
    if len(segs) < 2 {
        t.Fatalf("未触发轮转（段数 %d），本用例没测到目标场景", len(segs))
    }
    l.Close()
    if _, _, err := Open(dir, lg); err != nil {
        t.Fatalf("重启恢复失败: %v", err)
    }
}
```

注意：这两个用例必须能在 `-race` 下通过（Task 6 全量回归会带 -race 跑）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/seglog/ -run TestConcurrentSync -race -v`
Expected: 当前实现（持锁内联 fsync）功能上能过这两个测试——所以先跑一遍确认**基线绿**，然后实现改造后再跑确认**仍绿**。本 task 的「红」体现在结构断言上：改造前 `Append` 在 `mu` 内 fsync，写一个用锁观测的辅助断言不可靠，因此接受「行为测试改造前后都绿、以 review 确认结构」的折衷——这是本计划唯一一处非严格红绿，spec §4 的结构（写段与刷盘分离）是 review 检查点。

- [ ] **Step 3: 实现 group commit**

`Log` 结构体加字段：

```go
written uint64     // 全局单调写入水位（字节计数，跨段不清零），mu 保护
synced  uint64     // 已 durable 的写入水位，mu 保护
syncMu  sync.Mutex // sync 排队点：领导者持有期间做 fsync，后来者排队后搭车
```

- `Append`：写段与内存态更新在 `mu` 内完成并累加 `written`（含轮转），记 `target := l.written` 后**释放 mu**；`sync=true` 时调 `l.syncTo(target)`。
- `syncTo`（spec §4 伪码的忠实实现）：

```go
// syncTo 保证全局写入水位 target 之前的全部字节 durable 后返回。
//
// group commit 结构：并发的 sync 请求在 syncMu 上排队；队首（领导者）
// fsync 当前活动段——fsync 落盘的是「系统调用发起那一刻文件里的全部
// 字节」，把队列里所有已写完的组一次全部转正；后续排队者获得 syncMu
// 后先查水位，发现自己已被覆盖直接返回（搭车）。fsync 在途期间其他组
// 的写段（mu）不受阻——这正是跨组摊销的来源。
//
// 不变量：未 durable 的字节永远只存在于活动段（轮转屏障保证旧段 close
// 前必 fsync），因此领导者只需 fsync 活动段。ErrClosed 重试：领导者抓
// 到的句柄可能已被并发轮转关闭——轮转自身先 fsync 后 close 并把 synced
// 抬到 written，重查水位必然覆盖 target，循环必终止。
func (l *Log) syncTo(target uint64) error {
    l.syncMu.Lock()
    defer l.syncMu.Unlock()
    for {
        l.mu.Lock()
        if l.synced >= target {
            l.mu.Unlock()
            return nil // 搭车：前一位领导者（或轮转）的 fsync 已覆盖我
        }
        f, covered := l.active, l.written
        l.mu.Unlock()
        if f == nil {
            return fmt.Errorf("seglog: 已关闭，拒绝 sync")
        }
        if err := f.Sync(); err != nil {
            if errors.Is(err, os.ErrClosed) {
                continue // 句柄被轮转关闭：轮转已保证落盘，回头重查水位
            }
            return fmt.Errorf("seglog: group commit fsync 失败: %w", err)
        }
        l.mu.Lock()
        if covered > l.synced {
            l.synced = covered
        }
        l.mu.Unlock()
        return nil
    }
}
```

- `maybeRotate` 末尾（回收资格授予前）：`l.synced = l.written`——旧段已 fsync+close、HS 补写已 fsync，此刻全部已写字节 durable（这一行是 ErrClosed 循环终止的依据，注释必须写明）。
- `Sync()`（flusher 入口）：持 mu 读 `written` 后释放，调 `syncTo(written)`。
- `Reset` 的内联 fsync 改走同一条 `syncTo`（先在 mu 内写帧记 target，出 mu 后 syncTo）——统一 durable 路径，避免两套水位记账。
- **锁序纪律**（注释写进两把锁的字段声明处）：`syncMu → mu` 允许，反向禁止；Append 持 mu 期间绝不碰 syncMu。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/seglog/ -count=1 -race`
Expected: PASS（全部用例，含 Task 1/2 的）

- [ ] **Step 5: 补日志与注释**

- sync 领导者每次 fsync 后 Debug：`"seglog: group commit 落盘", "coveredBytes", covered-syncedBefore`（摊销真实发生的现场证据，验证时用）；高频路径必须 Debug 级；
- `written/synced/syncMu` 字段注释、`syncTo` 函数注释按上文实现写全（伪码注释已含）；
- `Append` 注释更新：写段/刷盘两段式结构与 durable 契约（sync=true 返回时本次写入必已落盘——覆盖它的 fsync 未必是自己发起的）。

- [ ] **Step 6: gofmt + 提交**

```bash
gofmt -w internal/cluster/seglog/
git add internal/cluster/seglog/
git commit -m "perf(seglog): sync 领导者 + 水位搭车——跨组 fsync 合并为 group commit

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: raftStore 切换到共享 Log

**Files:**
- Modify: `internal/cluster/raftstore.go`
- Modify: `internal/cluster/raftstore_test.go`（**仅**增补用例的适配；12 个存量用例零修改）

**Interfaces:**
- Consumes: Task 1-3 的共享 `seglog.Log` 完整 API。
- Produces: `raftStore` 全部导出/包内方法签名与语义零变化（`Persist/Load/TruncateLog/ResetGroupProgress/SyncLogs/CloseLogs/migrateLog/bumpTermsInto/...`）。

- [ ] **Step 1: 确认红**

Run: `go build ./internal/cluster/`
Expected: FAIL——raftstore.go 还在用旧 API（`logs map[uint32]*seglog.Log`、`seglog.Open` 旧返回值、`wipeLog` 的 RemoveAll 目录语义）。这就是本 task 的失败起点；存量 12 用例是「绿」的定义。

- [ ] **Step 2: 改造 raftStore**

结构体与路径：

```go
// 字段替换
log       *seglog.Log               // 全组共享；惰性打开（首个 Persist/Load 触发）
recovered map[uint32]*logRecovered  // 语义不变；Open 后一次性填充全部组

// groupLogDir 删除，换成：
// sharedLogDir 返回共享段日志目录：<storeDir>/raftlog。
// 唯一路径推导点：openShared（打开）与 manager.go 的 sharedLogHasFrames
// （只读探测）必须指向同一目录（原 groupLogDir 三处一致性的共享版）。
func sharedLogDir(storeDir string) string {
    return filepath.Join(storeDir, "raftlog")
}
```

- `getLog(g)` → `openShared() (*seglog.Log, error)`：首次调用 `seglog.Open(sharedLogDir(...))`，把返回的 `map[uint32]*GroupState` 逐组灌进 `recovered`；后续直接返回缓存。
- `Persist(g, ...)`：`openShared()` + `l.Append(g, hs, ents, sync)`；recovered 缓存更新逻辑（写时复制裁剪）逐字保留。
- `Load(g)`：legacyPending 分流不变；`openShared()` 后从 `recovered[g]` 取（组不在 map = 空日志形态，返回空 HardState）。
- `TruncateLog(g, upto)`：锚点守卫不变；`l.TruncateTo(g, upto)`；缓存裁剪不变。
- `wipeLog` 删除。两处调用方改造：
  - `ResetGroupProgress`：Pebble 批次（原样）成功后 → `openShared()` + `l.Reset(g)` + `recovered` 中该组清空（`delete(r.recovered, g)`，持 logsMu）。「先 Pebble 后日志」顺序与方向论证注释更新为标记帧版（spec §7）。
  - `migrateLog`：①「清半截」从 `wipeLog(g)` 换成 `openShared()` + `l.Reset(g)` + 清 recovered 缓存该组；②③④ 不变（loadLegacy 只读、分块 Persist、Pebble Sync 删 legacy 键）。幂等锚（legacyPending）不变——注释里说明 Reset 标记就是新的「清半截」，重迁时逻辑清掉上次残留。
- `SyncLogs`：单文件 `l.Sync()`（未打开则 no-op）；「先日志后 FSM WAL」契约注释保留。
- `CloseLogs`：关共享句柄 + recovered 清空；幂等语义保留。
- `bumpTermsInto`：**零改动**（Persist/legacy 分流逻辑与共享化正交，验证编译即可）。
- 文件头注释更新：键布局表中 `raftlog/<g>/` 表述改为 `raftlog/`（共享单链、帧带组号）。

- [ ] **Step 3: 增补用例适配**

`TestMigrateLegacyLargeLogInChunks`（2500 条迁移 roundtrip）与 `TestTruncateLogReclaimsSegmentsPhysically`（物理删段）按共享形态适配：断言意图不变（分块后可读回全量 / 截断后段文件数下降且尾部条目在重启后幸存），路径断言从 `raftlog/<g>/*.seg` 改为 `raftlog/*.seg`。若后者因「其他组 pin」需要构造隔离，在测试内只用单组写入即可成立（单组场景登记表内只有该组，水位越过即删）。

- [ ] **Step 4: 跑测试确认绿**

Run: `go test ./internal/cluster/ -run TestRaftStore -count=1 -v`（以及 raftstore_test 内全部用例的实际名字，跑全文件）
Run: `go test ./internal/cluster/... -count=1`
Expected: PASS，**存量 12 用例 diff 为零**（`git diff internal/cluster/raftstore_test.go` 只能出现增补用例的适配）

- [ ] **Step 5: 补日志与注释**

- `openShared` 打开日志打 Info（组数、各组条目数汇总）；
- `ResetGroupProgress`/`migrateLog` 的顺序取舍注释按标记帧版重写（原注释里「删目录」的表述全部纠正——注释失实是历史教训，见 298eb6d）；
- 逐个导出方法过一遍 doc comment，确认没有残留「每组独立」时代的表述。

- [ ] **Step 6: gofmt + 提交**

```bash
gofmt -w internal/cluster/
git add internal/cluster/raftstore.go internal/cluster/raftstore_test.go
git commit -m "feat(cluster): raftStore 切换共享 seglog——接口零变化，12 用例零修改锚达成

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: manager/recovery 接线与只读探测

**Files:**
- Modify: `internal/cluster/manager.go`（`diskHasRaftState`、`groupHasSegFiles` → 共享版）
- Modify: `internal/cluster/recovery.go`（`inspectGroupState` 三支判定）
- Modify: `internal/cluster/manager_test.go` / `internal/cluster/recovery_test.go`（涉及段文件路径的用例适配）

**Interfaces:**
- Consumes: Task 4 的 raftStore。
- Produces:

```go
// sharedLogHasFrames 只读判定共享段日志是否存在非空段文件（size>0 的
// *.seg）。零副作用：不 MkdirAll、不 O_CREATE。同时承担旧布局探测：
// raftlog/ 下发现纯数字子目录返回错误（与 seglog.Open 的守卫同判据）。
func sharedLogHasFrames(storeDir string) (bool, error)
```

- [ ] **Step 1: 写失败测试**

recovery_test.go / manager_test.go 增补（现有涉及 `raftlog/<g>/` 路径的用例先改路径，跑出红）：

```go
// sq recover 只读契约在共享形态下保持：对全新数据目录跑 InspectRecovery
// 不得在盘上创建 raftlog 目录或任何段文件。
func TestInspectRecoveryCreatesNoSharedLogFiles(t *testing.T) {
    // 构造全新 store 目录 → InspectRecovery → 断言 <dir>/raftlog 不存在。
    // （沿用现有同名意图用例的构造方式，路径断言从 raftlog/<g> 改为 raftlog）
}

// diskHasRaftState 第三支：共享段文件非空即判有状态；0 字节段文件不算。
func TestDiskHasRaftStateSeesSharedSegFiles(t *testing.T) {
    // 手工在 <dir>/raftlog/ 放一个 0 字节 00000001.seg → 判 false；
    // 经 raftStore.Persist 写一条后 → 判 true。
    // （现有 diskHasRaftState 三支用例的共享版改造）
}

// 旧的每组独立布局 fail-stop：raftlog/1/ 子目录存在即拒启，错误信息
// 含清理指引。
func TestPerGroupLayoutRefusesBoot(t *testing.T) {
    // mkdir <dir>/raftlog/1 + 放一个非空 .seg → Manager 启动（或
    // sharedLogHasFrames）报错，错误串含「每组独立布局」。
}
```

（前两个用例以现有对应用例为模板改造——recovery_test.go 已有「交叉验证 diskHasRaftState 判据」的用例群，本 task 是判据的共享化平移，不是新语义。）

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/ -run 'TestInspectRecovery|TestDiskHasRaftState|TestPerGroupLayout' -v`
Expected: FAIL（groupHasSegFiles 还在按组目录探测）

- [ ] **Step 3: 实现**

- `groupHasSegFiles` → `sharedLogHasFrames(storeDir)`：读 `raftlog/` 目录（不存在 → false, nil）；发现纯数字子目录 → 返回旧布局错误；存在 size>0 的 `.seg` → true。
- `diskHasRaftState`：第三支从「逐组 groupHasSegFiles」提出循环外，改为一次 `sharedLogHasFrames`；前两支（legacy hsKey / applied>0）逐组循环不变。三支判据注释同步更新，并检查 recovery.go 侧「同判据交叉验证」的对应实现与注释一起改（历史教训：两处判据必须语义一致，见 recovery.go 注释）。
- `inspectGroupState`：三支结构保留——①legacyPending → loadLegacy；②`sharedLogHasFrames` 为假 → 空日志形态直接返回（碰都不碰）；③为真 → `rs.Load(g)`（允许 Open，torn 截断豁免论证不变）。第四返回值 `hasSeg` 的语义改为「共享日志非空」（全组同值）——它在上游只参与 `HasRaft` 的 OR 判定，全组同值不改变 OR 结果；函数注释明确这一点。
- manager.go 其余引用点（迁移循环、flusher、停机路径）验证编译零改动——raftStore 接口没变，理论上不该有别的触点；有则说明 Task 4 漏了接口面，回去修 Task 4 而不是在这里打补丁。

- [ ] **Step 4: 跑测试确认绿**

Run: `go test ./internal/cluster/... -count=1`
Expected: PASS（含 manager_test / recovery_test / cluster_test 全量）

- [ ] **Step 5: 补日志与注释**

- `sharedLogHasFrames` doc comment（上文接口块已给出）+ 旧布局错误信息复核（含清理指引全文）；
- `inspectGroupState` 的三支注释更新（「组目录」表述清理，hasSeg 语义变化说明）；
- `diskHasRaftState` 判据注释与 recovery.go 交叉验证注释同步。

- [ ] **Step 6: gofmt + 提交**

```bash
gofmt -w internal/cluster/
git add internal/cluster/
git commit -m "feat(cluster): 恢复判定与只读探测切换共享 seglog——判据语义平移，零副作用契约保持

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: 全量回归 + 交叉编译产物

**Files:**
- 无新改动（只跑验证；发现缺陷回到对应 task 修）

- [ ] **Step 1: 包级全量 + race**

```bash
go test ./internal/cluster/... -count=1 -race -timeout 20m
go test ./... -count=1 -timeout 30m
```

Expected: 全绿。已知历史 watch-item：internal/cluster -race 曾有一次未捕获的 flaky——若复现，**必须抓全输出定位**，不许当噪声跳过（这次它有了新的头号嫌疑人：group commit 的并发结构）。

- [ ] **Step 2: e2e（macOS 本机）**

```bash
cd test/e2e && go test -tags e2e -count=1 -timeout 40m ./...
```

Expected: PASS（B10/B11 恢复系列全部在内）

- [ ] **Step 3: 交叉编译产物（Linux 验证与三机基准用，历史教训：不在远端装工具链）**

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o sq.linux.sharedlog ./cmd/sq
cd test/e2e && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -tags e2e -c -o e2e.linux.test .
```

Expected: 两个产物构建成功（Linux e2e 与三机基准复测由主会话在测试机上执行，不在本计划内——产物路径写进完工汇报）。

- [ ] **Step 4: 完工自检（instrumenting-code 清单）**

- [ ] 每个错误分支带上下文（段号/偏移/组号/水位）
- [ ] 成功路径不静默（打开/轮转/回收/重置/迁移/group commit 均有 Info 或 Debug）
- [ ] 无 fmt.Printf
- [ ] 改动文件的文件头注释与实现一致（无「每组独立」残留表述）
- [ ] 导出方法 doc comment 齐全；两把锁的锁序纪律写在字段声明处

- [ ] **Step 5: 完工汇报**

不推送（改动经 handoff 回程同步）。完工汇报里列出：各 task 的 commit 清单、全量测试结果原文（含 -race）、两个交叉编译产物的路径、instrumenting-code 自检清单勾选情况。

---

## Self-Review 记录

- Spec 覆盖：§3 帧/布局/守卫→Task 1+5；§4 group commit→Task 3；§5 轮转→Task 1；§6 回收→Task 2；§7 重置/迁移锚→Task 2+4；§8 扫描/只读探测→Task 1+4+5；§10 测试策略逐条落位；§11 观测性分散在各 task 的日志 step。基准复测（spec §10.4）由主会话在三机上执行，不在本计划内。
- 类型一致性：`GroupState`/`Append(g,...)`/`TruncateTo(g,...)`/`Reset(g)`/`sharedLogDir`/`sharedLogHasFrames` 各 task 引用一致。
- 已知非严格红绿一处（Task 3 Step 2）已显式声明与理由。
