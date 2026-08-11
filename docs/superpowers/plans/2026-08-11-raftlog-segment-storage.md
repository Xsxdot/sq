# raft 日志 segment 存储（B2）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** raft 日志条目 + HardState 退出 Pebble LSM，改存每组独立的自研分段追加文件（seglog），`raftStore` 对外接口零变化，消掉日志双写造成的 compaction churn。

**Architecture:** 新包 `internal/cluster/seglog` 提供单组追加日志（CRC 帧、64MiB 段轮转、启动扫描恢复、按段回收）；`raftStore` 内部换实现并保留 Pebble 旧键族的只读回退（迁移前的恢复判定用）；Manager 启动时做一次性迁移，mem 档 flusher 改为「先刷日志段、再刷 FSM WAL」。设计定案见 `docs/superpowers/specs/2026-08-11-raftlog-segment-storage-design.md`（下称 spec）。

**Tech Stack:** Go 1.26；`hash/crc32`（Castagnoli）；`google.golang.org/protobuf/proto`；`go.etcd.io/raft/v3/raftpb`；现有 `internal/store`（Pebble v2 封装）。

## Global Constraints

- 工作分支：`v2-b2-seglog`，从 `v2-apply-coalesce` 切出（该分支含 apply 合批与 spec）。
- **`raftStore` 对外方法签名一个不改**：`Persist/Load/TruncateLog/SaveConfState/ResetGroupProgress/...` 的调用方（group.go/manager.go/recovery.go）除本计划明列的行外零改动。
- TDD 铁律：每个行为先写失败测试、亲眼看它失败、再实现。测试命令统一 `go test ./internal/... -count=1 -run <Name> -v`。
- 日志规范（instrumenting-code）：关键节点用 `slog`（项目现有模式 `lg.With("mod", ...)`），禁止 `fmt.Printf`；错误分支必须带上下文（组号/段号/偏移/err）。
- 注释规范（用户 CLAUDE.md §2）：新文件顶部写职责+边界；导出方法写参数/返回/注意；复杂逻辑写中文「为什么」。
- 提交规范：每 task 至少一次提交，消息格式仿照仓库近期风格（`feat(cluster): ...`），结尾带 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。
- 已验证前提（不必复验）：Pebble 目录（= data_dir）内放 `raftlog/` 外来子目录，pebble.Open/写入/重开均正常（本会话已用 pebble v2.1.6 实测）。
- 每个 task 完成后跑 `gofmt -l internal/` 确认无未格式化文件（改动文件范围内）。

---

### Task 1: 分支与 `store.Dir()` 访问器

**Files:**
- Modify: `internal/store/store.go`（Store 结构体 + Open + 新增 Dir 方法）
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces: `func (s *Store) Dir() string` — 返回 Open 时传入的目录。后续 Task 5 的 raftStore 用它推导 seglog 根目录（`filepath.Join(st.Dir(), "raftlog")`），从而 `newRaftStore(st, lg)` 签名不用改。

- [ ] **Step 1: 建分支**

```bash
cd /path/to/sq && git checkout v2-apply-coalesce && git checkout -b v2-b2-seglog
```

- [ ] **Step 2: 写失败测试**

在 `internal/store/store_test.go` 末尾追加：

```go
// TestStoreDirReturnsOpenDir Dir 必须返回 Open 时的目录——raftStore 靠它
// 推导 seglog 根目录（raftlog/ 子目录与 Pebble 同住 data_dir，
// WipeForRejoin 整删 data_dir 时才能一并覆盖）。
func TestStoreDirReturnsOpenDir(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir)
	defer s.Close()
	if got := s.Dir(); got != dir {
		t.Fatalf("Dir() = %q; want %q", got, dir)
	}
}
```

- [ ] **Step 3: 跑测试看它失败**

Run: `go test ./internal/store/ -count=1 -run TestStoreDirReturnsOpenDir -v`
Expected: 编译失败 `s.Dir undefined`。

- [ ] **Step 4: 最小实现**

`internal/store/store.go`：`Store` 结构体加字段 `dir string`；`Open` 里构造时带上 `dir: dir`；追加方法：

```go
// Dir 返回本库的磁盘目录（Open 时传入的路径）。集群层用它推导
// raft 日志段文件的根目录（<dir>/raftlog/），使日志文件与 Pebble
// 同住 data_dir——WipeForRejoin 整删 data_dir 时二者一并清空。
func (s *Store) Dir() string { return s.dir }
```

- [ ] **Step 5: 跑测试确认通过；全包回归**

Run: `go test ./internal/store/ -count=1`
Expected: 全 PASS。

- [ ] **Step 6: 提交**

```bash
git add internal/store/store.go internal/store/store_test.go
git commit -m "feat(store): Dir 访问器——raftStore 推导 seglog 根目录的地基"
```

---

### Task 2: seglog 记录帧编解码

**Files:**
- Create: `internal/cluster/seglog/frame.go`
- Test: `internal/cluster/seglog/frame_test.go`

**Interfaces:**
- Produces:
  - `const recEntry byte = 1`、`const recHardState byte = 2`
  - `func appendFrame(dst []byte, typ byte, payload []byte) []byte` — 把一条记录帧追加到 dst 并返回新切片
  - `func readFrame(buf []byte) (typ byte, payload []byte, n int, err error)` — 从 buf 头部解一帧；n 为消费字节数；数据不足/CRC 不符返回 `errTornFrame`（导出为包内 sentinel `var errTornFrame = errors.New(...)`）
- 帧布局（spec §4）：`[4B len BE][4B CRC32C][1B type][payload]`，len = 1+len(payload)，CRC 覆盖 type+payload。

- [ ] **Step 1: 写失败测试**

新建 `internal/cluster/seglog/frame_test.go`：

```go
// frame_test.go 验证记录帧的编解码与损坏判定。
// 职责：roundtrip、多帧顺序解码、torn 判定（长度不足/CRC 不符）。
// 边界：不涉文件 I/O（segment 层的事）。
package seglog

import (
	"bytes"
	"errors"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	buf := appendFrame(nil, recEntry, []byte("payload-a"))
	buf = appendFrame(buf, recHardState, []byte("payload-b"))
	typ, payload, n, err := readFrame(buf)
	if err != nil || typ != recEntry || !bytes.Equal(payload, []byte("payload-a")) {
		t.Fatalf("第一帧 = %d,%q,%v; want recEntry,payload-a,nil", typ, payload, err)
	}
	typ, payload, _, err = readFrame(buf[n:])
	if err != nil || typ != recHardState || !bytes.Equal(payload, []byte("payload-b")) {
		t.Fatalf("第二帧 = %d,%q,%v; want recHardState,payload-b,nil", typ, payload, err)
	}
}

func TestFrameTornDetection(t *testing.T) {
	full := appendFrame(nil, recEntry, []byte("payload"))
	// 截尾模拟掉电 torn write：任何前缀都必须判 torn，不 panic 不误读
	for cut := 0; cut < len(full); cut++ {
		if _, _, _, err := readFrame(full[:cut]); !errors.Is(err, errTornFrame) {
			t.Fatalf("截到 %d 字节应判 errTornFrame，得到 %v", cut, err)
		}
	}
	// CRC 损坏（翻转 payload 一位）同判 torn
	bad := append([]byte(nil), full...)
	bad[len(bad)-1] ^= 0x01
	if _, _, _, err := readFrame(bad); !errors.Is(err, errTornFrame) {
		t.Fatalf("CRC 损坏应判 errTornFrame，得到 %v", err)
	}
}

func TestFrameRejectsInsaneLength(t *testing.T) {
	// 长度字段被写花成天文数字：必须判 torn，不得按 len 分配内存
	buf := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 1}
	if _, _, _, err := readFrame(buf); !errors.Is(err, errTornFrame) {
		t.Fatalf("疯长度应判 errTornFrame，得到 %v", err)
	}
}
```

- [ ] **Step 2: 跑测试看它失败**

Run: `go test ./internal/cluster/seglog/ -count=1 -v`
Expected: 编译失败（包不存在/函数未定义）。

- [ ] **Step 3: 最小实现**

新建 `internal/cluster/seglog/frame.go`：

```go
// Package seglog 提供每 raft 组一份的分段追加日志（B2 spec）。
//
// 职责：
//   - 记录帧编解码（CRC32C 校验，torn write 可判定）
//   - 段文件的追加/轮转/fsync 与启动扫描恢复
//   - 按段回收（截断 = 删整段文件）
//
// 边界：
//   - 不理解 raft 语义之外的内容：只存 Entry/HardState 的 protobuf 字节
//   - 不管锚点/applied/成员表——那些留在 Pebble（spec §3 职责划分）
//   - 运行期不提供随机读：raft 读日志走 MemoryStorage 双记账
package seglog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// 记录类型（帧的 1B type 字段）。
const (
	recEntry     byte = 1 // payload = raftpb.Entry 的 protobuf 字节
	recHardState byte = 2 // payload = raftpb.HardState 的 protobuf 字节
)

// frameHeader = 4B len + 4B CRC。len = 1B type + payload 长度。
const frameHeaderLen = 8

// maxFrameLen 单帧上限：Entry 载荷上限（4MiB 消息体 + 头）远小于此；
// 超过即认定长度字段被写花（torn），拒绝按其分配内存。
const maxFrameLen = 64 << 20

// errTornFrame 表示缓冲区头部不构成一条完整有效帧——尾部截断、CRC
// 不符、长度字段疯掉都归此类。扫描层据此判定「末段 torn tail」。
var errTornFrame = errors.New("seglog: torn frame")

// castagnoli 与 Pebble/iSCSI 同款多项式，硬件加速普遍。
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// appendFrame 把一条记录帧追加到 dst 并返回新切片（append 语义）。
func appendFrame(dst []byte, typ byte, payload []byte) []byte {
	var hdr [frameHeaderLen]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(1+len(payload)))
	crc := crc32.Update(0, castagnoli, []byte{typ})
	crc = crc32.Update(crc, castagnoli, payload)
	binary.BigEndian.PutUint32(hdr[4:8], crc)
	dst = append(dst, hdr[:]...)
	dst = append(dst, typ)
	return append(dst, payload...)
}

// readFrame 从 buf 头部解一帧。
//
// 返回：
//   - typ/payload: 帧内容（payload 是 buf 的子切片，调用方需要长期持有时自行拷贝）
//   - n: 本帧消费的字节数（含头）
//   - err: errTornFrame 表示头部不构成完整有效帧
func readFrame(buf []byte) (typ byte, payload []byte, n int, err error) {
	if len(buf) < frameHeaderLen {
		return 0, nil, 0, errTornFrame
	}
	ln := binary.BigEndian.Uint32(buf[0:4])
	if ln == 0 || ln > maxFrameLen || int(ln) > len(buf)-frameHeaderLen {
		return 0, nil, 0, errTornFrame
	}
	body := buf[frameHeaderLen : frameHeaderLen+int(ln)]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(buf[4:8]) {
		return 0, nil, 0, errTornFrame
	}
	return body[0], body[1:], frameHeaderLen + int(ln), nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/seglog/ -count=1 -v`
Expected: 3 个测试全 PASS。

- [ ] **Step 5: 检查注释覆盖**

确认：包头注释（职责+边界）、`errTornFrame`/`maxFrameLen` 的「为什么」注释、导出概念的 doc comment 均已就位（上面代码已含，核对即可）。

- [ ] **Step 6: 提交**

```bash
git add internal/cluster/seglog/
git commit -m "feat(seglog): 记录帧编解码——CRC32C 校验与 torn write 判定"
```

---

### Task 3: seglog 单段追加与重开恢复

**Files:**
- Create: `internal/cluster/seglog/seglog.go`
- Test: `internal/cluster/seglog/seglog_test.go`

**Interfaces:**
- Produces（后续 Task 4/5 依赖的确切签名）:

```go
type Log struct { ... } // 并发安全（内部互斥）：Persist/TruncateTo/Sync 来自不同 goroutine
func Open(dir string, lg *slog.Logger) (*Log, *raftpb.HardState, []*raftpb.Entry, error)
func (l *Log) Append(hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error
func (l *Log) Sync() error
func (l *Log) Close() error
```

- `Open` 返回恢复出的最新 HardState（没有则 `nil`）与「后写的赢」重放后的连续条目序列（升序）。
- 段文件命名 `%08d.seg`（序号从 1 起）；本 task 只做单段（轮转在 Task 4）。
- `Append` 语义：非空 hs 先写 hardstate 记录 → 逐条 entry 记录 → 一次 `write()` 进内核（页缓存，mem 档持久性等位）→ `sync=true` 时 fsync。

- [ ] **Step 1: 写失败测试**

新建 `internal/cluster/seglog/seglog_test.go`：

```go
// seglog_test.go 验证段日志的追加、重开恢复与损坏处理。
// 职责：roundtrip、HS 取末条、冲突回退重放、torn tail 截断、非末段坏帧拒开。
// 边界：不测 raftStore 集成（Task 5 的 raftstore_test 覆盖）。
package seglog

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

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
```

- [ ] **Step 2: 跑测试看它失败**

Run: `go test ./internal/cluster/seglog/ -count=1 -v`
Expected: 编译失败（Open/Log 未定义）。

- [ ] **Step 3: 实现**

新建 `internal/cluster/seglog/seglog.go`（单段版；`segMaxBytes` 常量与轮转骨架留到 Task 4，本 task `maybeRotate` 可先不存在）：

```go
package seglog

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"
)

// Log 一组的分段追加日志。并发安全：Append（组 run goroutine）、
// Sync（flusher goroutine）、TruncateTo（截断 goroutine）来自不同
// goroutine，内部单互斥串行化——三者频率都低（每轮 Ready / 200ms /
// 30s），锁竞争可忽略。
type Log struct {
	mu        sync.Mutex
	dir       string
	lg        *slog.Logger
	active    *os.File // 当前活动段（始终打开，Append 的写入目标）
	activeSeq uint64   // 活动段序号
	activeSize int64   // 活动段当前字节数（轮转判定）
	lastIndex uint64   // 日志尾 index；0 = 空日志
	lastHS    *raftpb.HardState // 最新已写 HardState（轮转补写用，Task 4）
	segMax    map[uint64]uint64 // 已关闭段号 → 段内最大 entry index（回收判定）
	buf       []byte            // Append 的帧组装缓冲（复用减分配）
}

// segName 段文件名：8 位十进制序号，字典序 = 数值序。
func segName(seq uint64) string { return fmt.Sprintf("%08d.seg", seq) }

// Open 打开（或创建）dir 下的组日志：按序扫描全部段、恢复日志状态、
// 打开末段续写。
//
// 返回：
//   - Log: 就绪的日志（尾部定位在最后一条好帧之后）
//   - hs: 恢复出的最新 HardState；从未写过时为 nil
//   - ents: 「后写的赢」重放后的连续条目序列（升序）；可能为空
//   - err: 非末段坏帧（真损坏，非 torn write）或 I/O 错误
//
// 注意：末段的 torn tail（掉电正常形态）在此被物理截断到好帧边界后
// 继续——绝不静默保留坏字节，否则续写会接在坏帧后面永远读不回。
func Open(dir string, lg *slog.Logger) (*Log, *raftpb.HardState, []*raftpb.Entry, error)

// Append 追加一轮 Ready 的 HardState 与条目。
//
// 参数：
//   - hs: 非空时先写一条 hardstate 记录（帧序保证扫描时 HS 不晚于同轮条目）
//   - ents: 逐条写 entry 记录；空切片合法（仅 HS 轮）
//   - sync: true 时写完 fsync（quorum-fsync 档 MustSync 轮）；false 时
//     只 write() 进内核页缓存（mem 档持久性等位，进程 crash 不丢）
//
// 失败即返回错误，调用方（raftStore.Persist → group.handleReady）按
// fail-stop 处理；本层不重试——写失败后文件偏移状态不可信。
func (l *Log) Append(hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error

// Sync 刷活动段到盘（mem 档 200ms flusher 的批量刷盘入口）。
func (l *Log) Sync() error

// Close 关闭活动段句柄。之后任何 Append/Sync 返回错误。
func (l *Log) Close() error
```

实现要点（执行者按此写实现体，逐条都是行为约束）：

1. `Open`：`os.MkdirAll(dir)`；`os.ReadDir` 收集 `*.seg` 按序号排序；逐段 `os.ReadFile` 全量读入后用 `readFrame` 循环解帧：
   - entry 帧：`proto.Unmarshal` 出 `raftpb.Entry`；**若 `e.GetIndex() <= lastIndex`，先把已收集切片截到 `Index-1` 的位置再 append**（后写的赢；用二分或线性回退均可，条目升序、回退距离短，线性即可）；随后 `lastIndex = e.GetIndex()`；
   - hardstate 帧：覆盖 `hs` 变量（后写的赢）；
   - `errTornFrame`：**仅当是最后一段**时合法——记 Warn 日志（段名、好帧边界偏移、丢弃字节数），`os.Truncate` 到好帧边界，停止扫描；非末段遇 torn → 返回错误 `fmt.Errorf("seglog: 段 %s 偏移 %d 帧损坏且非末段——真损坏，拒绝启动", ...)`；
   - protobuf 解码失败视同帧损坏处理（CRC 过了但内容坏 = 写入方 bug 或位腐，同样按末段/非末段分流）。
   - 扫描中同步维护 `segMax`（每个已关闭段的最大 entry index）。
2. 末段以 `os.OpenFile(..., os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)` 打开为 active；无任何段时创建 `00000001.seg`。
3. `Append`：锁内组装 `l.buf = l.buf[:0]`，hs 非空先 `proto.Marshal` + `appendFrame(recHardState)` 且更新 `l.lastHS`；逐条 entry `appendFrame(recEntry)`；一次 `l.active.Write(l.buf)`；更新 `activeSize`、`lastIndex`（取 ents 末条）；`sync` 时 `l.active.Sync()`。
4. Info 日志：`Open` 完成时打一条（段数、条目数、lastIndex、恢复出的 HS commit、是否发生 torn 截断）——重启排障第一现场。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/seglog/ -count=1 -v`
Expected: Task 2+3 全部测试 PASS。

- [ ] **Step 5: 补非末段坏帧拒开测试（先看失败再修，若实现已覆盖则直接绿）**

```go
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
```

（需要 import `google.golang.org/protobuf/proto`。）
Run: `go test ./internal/cluster/seglog/ -count=1 -run TestOpenRejectsCorruptNonLastSegment -v`

- [ ] **Step 6: 检查日志与注释覆盖**

- Open 成功路径 Info（非静默）；torn 截断 Warn（段名+偏移+丢弃字节）；非末段损坏错误里带段名+偏移。
- 文件头注释、Open/Append/Sync/Close doc comment、「后写的赢」与「末段才容忍 torn」的为什么注释。

- [ ] **Step 7: 提交**

```bash
git add internal/cluster/seglog/
git commit -m "feat(seglog): 单段追加与重开恢复——后写的赢重放、torn tail 截断、非末段损坏拒开"
```

---

### Task 4: 段轮转与按段回收

**Files:**
- Modify: `internal/cluster/seglog/seglog.go`
- Test: `internal/cluster/seglog/seglog_test.go`

**Interfaces:**
- Produces:
  - `func (l *Log) TruncateTo(upto uint64) error` — 删除「段内最大 entry index ≤ upto 且已关闭」的段文件；active 段永不删。
  - 轮转行为：`Append` 后若 `activeSize >= segMaxBytes`（`const segMaxBytes = 64 << 20`）→ close+fsync 旧段 → 创建下一序号段 → **新段首条补写一份 `lastHS`**（若有）。
  - 测试口子：`segMaxBytes` 提为 `var segMaxBytes int64 = 64 << 20`（包内变量，测试可调小；生产不改）。

- [ ] **Step 1: 写失败测试**

```go
func TestRotationCarriesHardStateAndRecovers(t *testing.T) {
	old := segMaxBytes
	segMaxBytes = 64 // 逼近零：每轮 Append 后都轮转
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
		t.Fatalf("段数 = %d; want ≥2（segMaxBytes=64 应触发轮转）", len(segs))
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
	segMaxBytes = 64
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
```

- [ ] **Step 2: 跑测试看它失败**

Run: `go test ./internal/cluster/seglog/ -count=1 -run 'TestRotation|TestTruncateTo' -v`
Expected: FAIL（TruncateTo 未定义 / 轮转不存在段数=1）。

- [ ] **Step 3: 实现轮转与回收**

要点：

1. `maybeRotate`（Append 尾部锁内调用）：`activeSize >= segMaxBytes` → `active.Sync()`（**轮转屏障**：旧段落盘后才开新段，保证坏帧只可能在末段）→ `active.Close()` → `segMax[activeSeq] = 该段最大 entry index` → 创建 `segName(activeSeq+1)` → 若 `lastHS != nil` 立即 `appendFrame(recHardState, marshal(lastHS))` 写入新段（**补写**：保证最新 HS 永远在最新段，回收旧段不会删掉唯一 HS）。
2. `TruncateTo(upto)`：锁内遍历 `segMax`，`max <= upto` 的段 `os.Remove` + 从 map 删除；Info 日志（组目录、删段清单、释放字节数）。**注意**：段内只有 hardstate 记录、无条目的段（max=0）也可回收——但若它是「最新 HS 所在段的前身」也没关系，补写保证了后段有副本。
3. Info 日志：轮转时（旧段号、字节数、新段号）。

- [ ] **Step 4: 跑测试确认通过；全包回归**

Run: `go test ./internal/cluster/seglog/ -count=1 -v`
Expected: 全 PASS。

- [ ] **Step 5: 检查日志与注释**

轮转 Info、回收 Info（删段清单+字节）、`segMaxBytes` 为什么是变量（测试口子）注释、轮转屏障与 HS 补写的「为什么」注释。

- [ ] **Step 6: 提交**

```bash
git add internal/cluster/seglog/
git commit -m "feat(seglog): 段轮转与按段回收——轮转屏障、新段 HS 补写、active 段保护"
```

---

### Task 5: raftStore 切换到 seglog（含 legacy 只读回退）

**Files:**
- Modify: `internal/cluster/raftstore.go`
- Test: `internal/cluster/raftstore_test.go`（现有用例零修改必须全过；新增两个用例）

**Interfaces:**
- Consumes: Task 1 `store.Dir()`；Task 3/4 seglog 全套。
- Produces（manager/recovery 在 Task 6/7 依赖）:
  - `func (r *raftStore) getLog(g uint32) (*seglog.Log, error)` — 惰性 Open `<st.Dir()>/raftlog/<g>/`，缓存句柄与恢复态
  - `func (r *raftStore) SyncLogs() error` — 逐个已打开组日志 Sync（flusher 用）
  - `func (r *raftStore) CloseLogs()` — 关闭全部组日志（Manager 停机用）
  - `func (r *raftStore) legacyPending(g uint32) (bool, error)` — Pebble 里是否还有旧日志键族（`hsKey(g)` 或 `entPrefix(g)` 任一存在）＝迁移未完成
- **签名不变**：`newRaftStore(st, lg)`、`Persist`、`Load`、`TruncateLog`。

- [ ] **Step 1: 结构体与惰性打开（无独立测试，随后续步骤被覆盖）**

`raftStore` 加字段：

```go
type raftStore struct {
	st *store.Store
	lg *slog.Logger

	// seglog 组日志：惰性打开（首个 Persist/Load 触发），Open 恢复出的
	// (hs, ents) 缓存在 recovered 里供 Load 消费。logsMu 只保护两张 map，
	// Log 自身并发安全。
	logsMu    sync.Mutex
	logs      map[uint32]*seglog.Log
	recovered map[uint32]*logRecovered
}

// logRecovered 是 seglog.Open 恢复出的一组日志状态（Load 的数据源）。
type logRecovered struct {
	hs   *raftpb.HardState
	ents []*raftpb.Entry
}
```

`newRaftStore` 初始化两张 map。`getLog(g)`：锁内查缓存；未打开则 `seglog.Open(filepath.Join(r.st.Dir(), "raftlog", strconv.FormatUint(uint64(g), 10)), r.lg)`，句柄与恢复态入缓存。

- [ ] **Step 2: Persist 切换（现有测试做红绿）**

`Persist` 新实现——整个函数体替换为：

```go
func (r *raftStore) Persist(g uint32, hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error {
	l, err := r.getLog(g)
	if err != nil {
		return fmt.Errorf("raftstore Persist 组 %d 打开段日志: %w", g, err)
	}
	// 回退冲突（换届重写尾部）不做物理删除：seglog 追加后写的条目，
	// 重启扫描按「后写的赢」重放（seglog.Open 契约），运行期视图由
	// MemoryStorage 双记账维护——旧实现的「同批删尾」在追加日志形态下
	// 由重放语义替代。
	if err := l.Append(hs, ents, sync); err != nil {
		return fmt.Errorf("raftstore Persist 组 %d: %w", g, err)
	}
	return nil
}
```

- [ ] **Step 3: Load 切换（含 legacy 只读回退）**

`Load` 新实现：

```go
func (r *raftStore) Load(g uint32) (*raftpb.HardState, []*raftpb.Entry, *raftpb.SnapshotMetadata, error) {
	// 迁移前的旧库（Pebble 里还有日志键族）走 legacy 只读路径：恢复
	// 判定命令（sq recover）与迁移步骤自身都要在不写盘的前提下读到
	// 旧状态——迁移由 Manager 启动序显式触发（migrateRaftLogs），
	// 读路径绝不顺手迁移。
	pending, err := r.legacyPending(g)
	if err != nil {
		return nil, nil, nil, err
	}
	if pending {
		return r.loadLegacy(g)
	}
	l, err := r.getLog(g) // 惰性 Open；恢复态已缓存
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raftstore Load 组 %d 打开段日志: %w", g, err)
	}
	_ = l
	r.logsMu.Lock()
	rec := r.recovered[g]
	r.logsMu.Unlock()
	hs := rec.hs
	if hs == nil {
		hs = &raftpb.HardState{} // 调用方契约：从未持久化过时返回空 HardState 非 nil
	}
	snapMeta, _, err := r.LoadSnapMeta(g)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raftstore Load 组 %d 读快照元数据: %w", g, err)
	}
	r.lg.Debug("raft 日志已读回（seglog）", "g", g, "entries", len(rec.ents),
		"commit", hs.GetCommit(), "snap", snapMeta.GetIndex())
	return hs, rec.ents, snapMeta, nil
}
```

`loadLegacy(g)` = 现 `Load` 的原实现整体改名保留（读 `hsKey`/`entPrefix` 扫描/`LoadSnapMeta`）。`legacyPending(g)`：`st.Get(hsKey(g))` 存在即 true；否则 `st.Scan(entPrefix(g), 上界, 1, ...)` 有键即 true。

- [ ] **Step 4: TruncateLog 切换**

锚点守卫原样保留（先 `LoadSnapMeta` 校验 `upto ≤ 锚点`——「先锚点后截断」不变量），删除动作换成：

```go
	l, err := r.getLog(g)
	if err != nil {
		return fmt.Errorf("raftstore TruncateLog 组 %d: %w", g, err)
	}
	if err := l.TruncateTo(upto); err != nil {
		return fmt.Errorf("raftstore TruncateLog 组 %d: %w", g, err)
	}
	r.lg.Info("raft 日志已截断（按段回收）", "g", g, "upto", upto)
```

- [ ] **Step 5: SyncLogs / CloseLogs**

```go
// SyncLogs 逐个已打开组日志刷盘——mem 档 200ms flusher 的日志侧入口。
// 顺序契约（spec §5）：调用方必须先 SyncLogs 再 store.SyncWAL，保证
// 崩溃窗口只出现「日志超前 FSM」（重放即补）的安全方向。
func (r *raftStore) SyncLogs() error {
	r.logsMu.Lock()
	logs := make([]*seglog.Log, 0, len(r.logs))
	for _, l := range r.logs {
		logs = append(logs, l)
	}
	r.logsMu.Unlock()
	for _, l := range logs {
		if err := l.Sync(); err != nil {
			return fmt.Errorf("raftstore SyncLogs: %w", err)
		}
	}
	return nil
}

// CloseLogs 关闭全部组日志句柄（Manager 停机/重入前调用；幂等）。
func (r *raftStore) CloseLogs() { ... 逐个 Close，Error 日志但不返回错——停机路径不因关文件失败中断 ... }
```

- [ ] **Step 6: 跑 raftstore 现有全套测试（关键验收：零修改通过）**

Run: `go test ./internal/cluster/ -count=1 -run 'TestPersist|TestTruncateLog|TestConfState|TestEnsureGroups|TestBootGen|TestRecoverPermit' -v`
Expected: 全 PASS **且 raftstore_test.go 一行未改**。`TestPersistTruncatesConflictTail` 尤其关键——它断言的冲突截尾语义现在由重放实现，接口行为必须不变。
若有个别用例直接断言 Pebble 键（检查确认过没有，但执行时若发现）：改断言为走 `rs.Load` 接口，并在提交信息里注明。

- [ ] **Step 7: 新增两个集成用例**

```go
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
```

先跑看失败（TestPersistSurvivesReopenViaSeglog 在 Step 2-5 完成前写会失败；按顺序执行时它应直接过——**若直接过，须口头确认失败条件成立**：把 Step 2 的 Persist 临时改回旧实现能让它挂，确认后还原。TestLoadFallsBackToLegacyKeys 在 legacyPending 缺失时会读到空）。

- [ ] **Step 8: 检查日志与注释**

Persist/Load/TruncateLog 的 doc comment 更新为 seglog 语义（含「删尾由重放替代」的为什么）；legacy 回退的「为什么只读」注释；SyncLogs 顺序契约注释。

- [ ] **Step 9: 提交**

```bash
git add internal/cluster/raftstore.go internal/cluster/raftstore_test.go
git commit -m "feat(cluster): raftStore 切换 seglog——接口零变化，legacy 键族只读回退"
```

---

### Task 6: HS 特殊写点与组级清理适配

**Files:**
- Modify: `internal/cluster/raftstore.go`（bumpTermsInto/ForceLocalRecover/BumpTermsForLocalResume/ResetGroupProgress）
- Modify: `internal/cluster/manager.go`（diskHasRaftState，约 757-775 行）
- Test: `internal/cluster/raftstore_test.go`

**Interfaces:**
- Consumes: Task 5 的 getLog/legacyPending。
- 行为约束：
  - `ForceLocalRecover`/`BumpTermsForLocalResume`（抬 term 清 vote）：改为**先**逐组经 seglog `Append(bumpedHS, nil, true)`（fsync），**后**在 Pebble 批次里做许可消费等剩余写。原实现是单批原子；拆开后崩溃窗口是「term 已抬、许可未消费」→ 重启再抬一次 term，单调无害（安全方向）。此取舍必须写进注释。
  - `bumpTermsInto(b *store.Batch, ...)` 签名会变（不再写批次）：改名 `bumpTerms(dataGroups uint32) error`，内部对每组 `rs.Load` 取现 HS、term+1、vote 清零、`getLog(g).Append(hs, nil, true)`。调用方（raftstore.go 内部两处）同步改。
  - `ResetGroupProgress(g)`：现有 Pebble 批（applied=0、删锚点/安装标记）保留，其中删 `entPrefix`/`hsKey` 的两条**保留**（legacy 键顺手清）；批提交后**追加**：关闭并从缓存移除该组 Log、`os.RemoveAll(<dir>/raftlog/<g>)`、清 recovered 缓存。删目录失败返回错误（半截段目录会在重启时被当作有效日志读回——比留脏键危险）。
  - `diskHasRaftState`（manager.go）：判定改为三者取或——Pebble `hsKey(g)` 存在（legacy）、Pebble applied>0、`raftlog/<g>` 目录内有 `*.seg` 文件。

- [ ] **Step 1: 写失败测试**

```go
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
```

（`ForceLocalRecover` 现签名 `ForceLocalRecover(dataGroups uint32)`——dataGroups=0 表示只有组 0；以实际签名为准，照现有 `TestForceLocalRecoverBumpsTermAndConsumesPermit` 的调用形态写。）

- [ ] **Step 2: 跑测试看失败** → **Step 3: 实现** → **Step 4: 跑通 + 现有 `TestForceLocalRecoverBumpsTermAndConsumesPermit` 仍须过**

- [ ] **Step 5: diskHasRaftState 适配（由现有 manager/cluster 测试回归覆盖）**

manager.go 判定循环改为：

```go
	// 「曾参与集群」的判据（三者取或）：legacy HardState 键（迁移前的旧
	// 盘）、applied 位点非零（FSM 写过）、组段目录里有段文件（新形态）。
	// 只看任一即可短路——判定只回答有/无，不读内容。
```

实现按注释写；`raftlog/<g>` 目录扫描用 `os.ReadDir` + 后缀判断。

- [ ] **Step 6: 检查日志与注释**（抬 term 拆批的崩溃窗口取舍注释；ResetGroupProgress 删目录失败为什么必须报错）

- [ ] **Step 7: 提交**

```bash
git add internal/cluster/raftstore.go internal/cluster/manager.go internal/cluster/raftstore_test.go
git commit -m "feat(cluster): HS 特殊写点与组级清理走 seglog——抬 term 先日志后许可、重置删段目录"
```

---

### Task 7: Manager 接线——一次性迁移、flusher 顺序、停机关闭

**Files:**
- Modify: `internal/cluster/manager.go`（NewManager 启动序、flusher、StopClean/停机路径）
- Modify: `internal/cluster/raftstore.go`（新增 migrateLog 方法）
- Test: `internal/cluster/manager_test.go`（新增迁移用例）

**Interfaces:**
- Consumes: Task 5/6 全部。
- Produces: `func (r *raftStore) migrateLog(g uint32) error` — 单组一次性迁移。

**迁移语义（spec §6）**：`legacyPending(g)` 为真时——①`os.RemoveAll` 该组 seglog 目录（清半截，幂等锚）；②`loadLegacy(g)` 读旧 hs+ents；③seglog `Append(hs, ents, true)`（先 HS 后条目，fsync）；④Pebble 单批 Sync 删 `hsKey(g)` + `DeleteRange(entPrefix(g))`。幂等：任何一步崩溃，Pebble 旧键还在 → 重启重迁。
**调用点**：NewManager 里 `EnsureGroups` 成功之后、恢复判定（decideRecovery）之后、buildGroup 循环之前，对 0..dataGroups 逐组调用（执行者在 manager.go 里找 `EnsureGroups` 调用与 buildGroup 循环之间插入；恢复判定只读 legacy 键，顺序不能颠倒）。

- [ ] **Step 1: 写失败测试**

```go
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
```

- [ ] **Step 2: 看失败** → **Step 3: 实现 migrateLog + NewManager 接线**

migrateLog 完成时 Info 日志（组号、条目数、耗时）；NewManager 的迁移循环外再打一条汇总（迁了几组；全空则 Debug 一条「无 legacy 日志，跳过迁移」——成功路径不静默）。

- [ ] **Step 4: flusher 顺序**

`Manager.flusher()` 的 tick 分支改为：

```go
		case <-ticker.C:
			// 顺序契约（spec §5）：先刷日志段、再刷 FSM WAL。反序的崩溃
			// 窗口会出现「FSM 声称 applied=N 而日志不认识 N」——那需要
			// 锚点引导才能恢复；正序窗口只有「日志超前 FSM」，重放即补。
			if err := m.rs.SyncLogs(); err != nil {
				m.lg.Error("后台批量刷盘失败（日志段）", "err", err)
				continue // 日志没刷成就不刷 FSM——保住顺序契约
			}
			if err := m.st.SyncWAL(); err != nil {
				m.lg.Error("后台批量刷盘失败", "err", err)
			}
```

- [ ] **Step 5: 停机关闭**

StopClean 与非干净停机路径里、store 关闭/标记写入**之前**加 `m.rs.CloseLogs()`（执行者查 StopClean 与 Manager.Stop 的停机序，放在「全部组已停」之后）。Rejoin 路径（`Rejoin` 函数第 1 步注释「旧 store 已关闭」处）同样由调用方停机时已关——确认 `WipeForRejoin` 前无残留打开句柄（unix 下 unlink 打开中的文件也可删，此处关闭是为不泄漏 fd，不是正确性前提；注释写明）。

- [ ] **Step 6: 全 cluster 包回归**

Run: `go test ./internal/cluster/ -count=1`
Expected: 全 PASS（含 manager_test/cluster_test/recovery_test/snapshot 系列——恢复路径全量回归的第一道）。

- [ ] **Step 7: 检查日志与注释**（迁移 Info/汇总、flusher 顺序注释、CloseLogs 停机位注释）

- [ ] **Step 8: 提交**

```bash
git add internal/cluster/
git commit -m "feat(cluster): Manager 接线 seglog——启动一次性迁移、flusher 先段后 WAL、停机关句柄"
```

---

### Task 8: 全量回归与交叉编译产物

**Files:** 无新改动（只跑验证；发现问题按 TDD 修复后追加提交）

- [ ] **Step 1: 全仓测试 + race**

```bash
go build ./... && go vet ./internal/... && go test ./internal/... -count=1
go test ./internal/store/ ./internal/cluster/ -count=1 -race
```

Expected: 全 PASS，无 race。

- [ ] **Step 2: e2e 本地（macOS/本机可跑的部分）**

```bash
cd test/e2e && go test -tags e2e -count=1 -timeout 30m .
```

Expected: 全 PASS（含 B10/B11 恢复系列、集群进程级用例）。任何失败都按 systematic-debugging 处理：先写复现测试再修。

- [ ] **Step 3: instrumenting-code 完工自检清单**

- [ ] 每个错误分支带上下文+cause；[ ] 外部边界（文件 I/O）前后有日志；[ ] 成功路径不静默（Open/迁移/轮转/回收的 Info）；[ ] 无 fmt.Printf；[ ] 新文件头注释；[ ] 导出方法 doc comment。

- [ ] **Step 4: 交叉编译（产物供三机复测，机器恢复后用）**

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/sq.linux.seglog ./cmd/sq
cd test/e2e && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -tags e2e -o /tmp/e2e.linux.test .
```

Expected: 两个产物生成成功。

- [ ] **Step 5: 收尾提交（若有修复），报告分支就绪**

三机性能复测（spec §9-4：两档吞吐 + pprof compaction 占比对比）不在本计划内——测试机当前失联，机器恢复后由主会话执行。

---

## 计划自检记录（写计划时已跑）

- Spec 覆盖：§3 职责划分→Task 5；§4 格式/轮转→Task 2/3/4；§5 写路径/flusher→Task 5/7；§6 恢复/迁移→Task 3/6/7；§7 截断→Task 4/5；§8 错误处理→各 task 的错误分支约束；§9 测试→Task 8；§10 观测→各 task 日志步骤。§9-4 性能验证显式移出（机器失联）。
- 占位符扫描：无 TBD/TODO；所有测试与关键实现给出真实代码；实现体留「要点清单」的两处（Task 3 Step 3、Task 4 Step 3）均为逐条行为约束而非「适当处理」。
- 类型一致性：`seglog.Open` 四返回值、`Append(hs, ents, sync)`、`TruncateTo(upto)`、`getLog/SyncLogs/CloseLogs/legacyPending/loadLegacy/migrateLog` 在各 task 间已交叉核对。
