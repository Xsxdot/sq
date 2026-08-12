# seglog 段文件预分配 + fdatasync 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 seglog 段文件一次性预分配到 `SegMaxBytes`，写入不再改变文件大小，同步落盘从 `fsync` 降级为 `fdatasync`，把单次同步落盘成本从实测 1.82ms 降到 0.61ms。

**Architecture:** 在 `internal/cluster/seglog` 内加一层平台抽象（`preallocate` / `datasync`，build tag 分 Linux 与其他），把段文件的写入从 `O_APPEND` 改为显式偏移写，新建段时预分配、轮转关段前截回逻辑大小，落盘按「是否已预分配」条件分派。`raftStore` 及以上完全无感。

**Tech Stack:** Go 1.26.1，`golang.org/x/sys/unix`（已是直接依赖，`internal/cluster/bootgen_darwin.go` 有同款 build tag 分文件先例），`go.etcd.io/raft/v3 v3.7.0`。

**Spec:** `docs/superpowers/specs/2026-08-12-seglog-prealloc-fdatasync-design.md`

## Global Constraints

- **崩溃恢复语义零变化。** 帧格式（`frame.go`）一个字节不动；`seglog` 对外接口（`Open`/`Append`/`Sync`/`TruncateTo`/`Close`）签名与语义不动。
- **验收锚：`internal/cluster/raftstore_test.go` 现有 12 个用例零修改通过。** 任何一个需要改动，都说明语义变了，必须停下来说明原因，而不是改测试。
- **不碰 `raftStore` 及以上任何一层**（`internal/cluster/raftstore.go`、`group.go`、`manager.go` 均不在本计划的修改范围内）。
- **预分配是纯性能优化，不是正确性前提。** 文件系统不支持时必须优雅退回 `fsync`，功能不受影响。
- **`fdatasync` 只在预分配真正生效时才安全。** 文件大小仍在增长时它不保证「文件长了」落盘。
- **日志用 `l.lg`（注入的 `*slog.Logger`），禁止 `fmt.Printf`。** 热路径（`Append` 每轮 Ready）零日志，日志只落在 `Open`/轮转/降级这些低频路径。
- **平台可观测性限制**：预分配相关的断言只在 Linux 可观测（macOS `prealloc == false`）。涉及此类断言的用例必须以 `if l.prealloc` 为前提，且 Task 6 的 Linux 验收不可跳过。

---

## File Structure

| 文件 | 动作 | 职责 |
|---|---|---|
| `internal/cluster/seglog/falloc_linux.go` | 新建 | Linux 的 `preallocate`（`unix.Fallocate` mode 0）与 `datasync`（`unix.Fdatasync`） |
| `internal/cluster/seglog/falloc_other.go` | 新建 | 非 Linux：`preallocate` 恒返回 `false`，`datasync` 退回 `File.Sync()` |
| `internal/cluster/seglog/falloc_test.go` | 新建 | 平台抽象层的行为用例（两平台都能跑） |
| `internal/cluster/seglog/seglog.go` | 修改 | 偏移写、`prealloc` 标志、预分配与截回、条件落盘、零尾日志判别 |
| `internal/cluster/seglog/seglog_test.go` | 修改 | 追加新用例（现有 7 个用例零修改） |

---

### Task 1: 平台抽象层（preallocate + datasync）

**Files:**
- Create: `internal/cluster/seglog/falloc_linux.go`
- Create: `internal/cluster/seglog/falloc_other.go`
- Test: `internal/cluster/seglog/falloc_test.go`

**Interfaces:**
- Consumes: 无（本任务是叶子）
- Produces:
  - `func preallocate(f *os.File, size int64) (allocated bool, err error)` — `allocated == false && err == nil` 表示文件系统不支持，调用方须退回 `fsync`
  - `func datasync(f *os.File) error` — 仅在文件已预分配时可用

- [ ] **Step 1: 写失败的测试**

创建 `internal/cluster/seglog/falloc_test.go`：

```go
// falloc_test.go 验证段文件的平台抽象层：预分配与数据同步。
// 职责：预分配成功时文件大小达标、预分配后写入可读回、datasync 不报错。
// 边界：不测 Log 的任何行为（seglog_test.go 覆盖）；不断言具体系统调用
//       ——那是平台实现细节，只断言可观测的契约。
package seglog

import (
	"os"
	"path/filepath"
	"testing"
)

// TestPreallocateReportsCapabilityAndKeepsData 预分配后：若报告成功则文件
// 大小必须达到请求值；无论成功与否，写入的数据都必须能原样读回。
//
// 两平台都跑：Linux 上 allocated=true 走预分配断言，macOS 上
// allocated=false 只走数据完整性断言——后者才是「退回 fsync 也不能坏事」
// 这条约束的守门人。
func TestPreallocateReportsCapabilityAndKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	const size = 1 << 20
	allocated, err := preallocate(f, size)
	if err != nil {
		t.Fatalf("preallocate 返回错误: %v", err)
	}

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if allocated && fi.Size() != size {
		t.Fatalf("预分配报告成功但文件大小 = %d; want %d", fi.Size(), size)
	}
	if !allocated && fi.Size() != 0 {
		t.Fatalf("预分配报告未生效但文件大小 = %d; want 0（不得有副作用）", fi.Size())
	}

	want := []byte("hello seglog")
	if _, err := f.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if err := datasync(f); err != nil {
		t.Fatalf("datasync 返回错误: %v", err)
	}

	got := make([]byte, len(want))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("读回 %q; want %q", got, want)
	}
}

// TestPreallocateIsIdempotentOverExistingData 对已有内容的文件再次预分配
// 不得破坏已写入的字节——Open 重启后补分配走的正是这条路径。
func TestPreallocateIsIdempotentOverExistingData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	want := []byte("existing frame bytes")
	if _, err := f.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := preallocate(f, 1<<20); err != nil {
		t.Fatalf("preallocate 返回错误: %v", err)
	}

	got := make([]byte, len(want))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("预分配后读回 %q; want %q（已有内容被破坏）", got, want)
	}
}
```

- [ ] **Step 2: 运行测试确认它失败**

Run: `go test ./internal/cluster/seglog/ -run TestPreallocate -v`
Expected: FAIL，编译错误 `undefined: preallocate` 与 `undefined: datasync`

- [ ] **Step 3: 写 Linux 实现**

创建 `internal/cluster/seglog/falloc_linux.go`：

```go
//go:build linux

// falloc_linux.go 提供段文件的 Linux 平台能力：预分配与数据同步。
//
// 职责：
//   - 把段文件一次性扩到定长并真实分配磁盘块（fallocate mode 0）
//   - 提供只同步数据、不同步 inode 元数据的落盘（fdatasync）
//
// 边界：
//   - 不理解段/帧语义，只对 *os.File 操作
//   - 不决定「什么时候该用 datasync」——那个前提由调用方（Log）持有
package seglog

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// preallocate 把 f 一次性扩到 size 字节并真实分配磁盘块（Linux fallocate）。
//
// 参数：
//   - f: 已打开的可写文件
//   - size: 目标字节数；已有内容原样保留
//
// 返回：
//   - allocated: true 表示预分配生效，此后写入不再改变文件大小，调用方
//     可以安全地用 datasync 代替 fsync；false + nil error 表示底层文件
//     系统不支持，调用方必须退回 fsync——这不是错误，是能力探测的正常结果
//   - err: 真 I/O 错误（ENOSPC 等）
//
// 注意：
//   - 必须用 mode 0（真扩 st_size），**绝不能用 FALLOC_FL_KEEP_SIZE**。
//     后者保持 st_size 不变，写入时文件大小照样增长、元数据日志照付，
//     整个方案的收益归零。
func preallocate(f *os.File, size int64) (bool, error) {
	err := unix.Fallocate(int(f.Fd()), 0, 0, size)
	if err == nil {
		return true, nil
	}
	// ENOTSUP：文件系统不支持 fallocate（网络文件系统、部分 overlayfs）。
	// EINVAL：部分文件系统对 mode 0 也报这个而不是 ENOTSUP。
	// 两者都归入「不支持」而非错误——功能可以退回 fsync 继续跑。
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EINVAL) {
		return false, nil
	}
	return false, fmt.Errorf("seglog: fallocate %s 到 %d 字节失败: %w", f.Name(), size, err)
}

// datasync 只把数据落盘，不同步 inode 元数据（Linux fdatasync）。
//
// 前提（调用方负责）：文件已预分配，本次写入不改变文件大小。否则「文件
// 长了」这件事可能不落盘，掉电后已写入的尾部字节读不回来。
func datasync(f *os.File) error {
	if err := unix.Fdatasync(int(f.Fd())); err != nil {
		return fmt.Errorf("seglog: fdatasync %s 失败: %w", f.Name(), err)
	}
	return nil
}
```

- [ ] **Step 4: 写非 Linux 实现**

创建 `internal/cluster/seglog/falloc_other.go`：

```go
//go:build !linux

// falloc_other.go 提供段文件平台能力在非 Linux 平台的退化实现。
//
// 职责：
//   - 让 seglog 在非 Linux 平台以「不预分配 + 完整 fsync」的形态正常工作
//
// 边界：
//   - 不尝试用 darwin 的 F_PREALLOCATE 模拟：它只预留磁盘块、不扩 st_size，
//     写入时文件大小照样增长，拿不到「同步落盘不碰元数据」这个收益——
//     为它引入一条只在开发机上跑的独立路径，是纯粹的风险
package seglog

import "os"

// preallocate 在非 Linux 平台不做预分配，恒定返回未生效。
//
// 调用方据此把 prealloc 标志置 false、落盘继续走 File.Sync()，行为与本
// 特性落地之前完全一致。开发与 -race 全量测试跑在 macOS 上，走的就是
// 这条路径。
func preallocate(f *os.File, size int64) (bool, error) { return false, nil }

// datasync 在非 Linux 平台退回完整 fsync。
//
// 正常流程下本函数不会被调用（prealloc == false 时调用方直接走
// File.Sync）。保留实现是为了让两个平台的函数集合完全一致，调用方无需
// 任何条件编译。
func datasync(f *os.File) error { return f.Sync() }
```

- [ ] **Step 5: 运行测试确认通过**

Run: `go test ./internal/cluster/seglog/ -run TestPreallocate -v`
Expected: PASS（macOS 上走 `allocated == false` 分支）

- [ ] **Step 6: 补充日志与注释自检**

本任务是叶子工具函数，**不加运行期日志**——它没有独立的「关键节点」，
调用点的降级日志在 Task 3 加（`preallocActive` 里）。确认：
- 两个新文件都有文件头注释（职责 + 边界）✓
- `preallocate` / `datasync` 都有 doc comment，写明参数、返回、前提 ✓
- `FALLOC_FL_KEEP_SIZE` 为什么不能用、ENOTSUP 为什么不算错误——都是
  「为什么」而非「做了什么」的注释 ✓

- [ ] **Step 7: 提交**

```bash
git add internal/cluster/seglog/falloc_linux.go internal/cluster/seglog/falloc_other.go internal/cluster/seglog/falloc_test.go
git commit -m "feat(seglog): 段文件平台抽象层——预分配与 fdatasync

Linux 走 fallocate(mode 0) + fdatasync；非 Linux 恒定不预分配、退回
File.Sync()。preallocate 用 (bool, error) 而非纯 error 报告能力：文件
系统不支持不是错误，是必须优雅退回 fsync 的正常结果。"
```

---

### Task 2: 偏移写入（去掉 O_APPEND，activeSize 兼作写入偏移）

本任务**行为完全不变**，是纯粹的机制铺垫：把写入从「内核定位 EOF」改成
「按 `activeSize` 显式定位」。没有预分配时两者等价，因此现有全部用例必须
零修改通过——这正是本任务的验收方式。

**Files:**
- Modify: `internal/cluster/seglog/seglog.go`（`Open` 的开段与 `activeSize` 来源、`Append` 的写入、`maybeRotate` 的开段与 HS 补写）

**Interfaces:**
- Consumes: 无
- Produces: `Log.activeSize` 的语义收紧为「活动段已写入的有效字节数，同时是下一次写入的偏移」；`Log.active` 不再带 `O_APPEND`

- [ ] **Step 1: 写失败的测试**

在 `internal/cluster/seglog/seglog_test.go` 末尾追加：

```go
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
```

- [ ] **Step 2: 运行测试**

Run: `go test ./internal/cluster/seglog/ -run TestReopenActiveSizeIsLogicalEnd -v`
Expected: PASS（现状下即为真——本用例是回归锚，不是红灯驱动）

> 这一步刻意不追求「先红后绿」：本任务不改变任何可观测行为，红灯无从制造。
> 真正的红灯锚在 Task 3 Step 2。

- [ ] **Step 3: 让 activeSize 显式来自扫描逻辑末尾**

修改 `internal/cluster/seglog/seglog.go` 的 `Open`。

在变量声明块（`tornInfo string` 那一段，约 `seglog.go:138-144`）里追加：

```go
		// activeLogicalEnd 活动段（= 最后一段）扫描结束时的偏移，即已写入的
		// 有效字节数。它是 activeSize 的唯一来源。
		//
		// 为什么不用 f.Stat().Size()：现状下两者恰好相等（扫描遇到坏帧会
		// 物理截断到好帧边界），但那让「轮转判定的输入」依赖「扫描一定会
		// 物理截断零尾」这个不写在任何地方的副作用。预分配落地后物理大小
		// 恒为 SegMaxBytes，这条隐式依赖一旦被后人改动截断策略就会静默
		// 踩塌轮转。显式取扫描结果，依赖关系从隐含变成明写。
		activeLogicalEnd int64
```

在外层 `for i, seq := range seqs` 循环体的**末尾**（内层 `scan:` 循环结束
之后，闭合大括号之前）追加：

```go
		if isLast {
			activeLogicalEnd = int64(off)
		}
```

删除 `f.Stat()` 那一段（约 `seglog.go:281-285`）——它此后唯一的用途被
`activeLogicalEnd` 取代：

```go
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, nil, fmt.Errorf("seglog: stat 活动段 %s 失败: %w", segName(activeSeq), err)
	}
```

把结构体字面量里的 `activeSize: fi.Size(),` 改为：

```go
		activeSize: activeLogicalEnd,
```

- [ ] **Step 4: 去掉 O_APPEND，改显式偏移写**

`seglog.go:264`（`Open` 开活动段）：

```go
	// 不带 O_APPEND：预分配后 EOF 就是 SegMaxBytes，O_APPEND 会把每次写
	// 都定位到段尾。写入位置改由 activeSize 显式给出（WriteAt），两个
	// 平台走同一条写路径——未预分配时 WriteAt(activeSize) 与顺序追加等价。
	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_CREATE, 0o644)
```

`seglog.go:440`（`maybeRotate` 开新段）作同样修改：

```go
	f, err := os.OpenFile(newPath, os.O_WRONLY|os.O_CREATE, 0o644)
```

`seglog.go:373`（`Append` 的写入）：

```go
	if len(l.buf) > 0 {
		n, err := l.active.WriteAt(l.buf, l.activeSize)
		if err != nil {
			return fmt.Errorf("seglog: 写活动段 %s 偏移 %d 失败: %w", segName(l.activeSeq), l.activeSize, err)
		}
		l.activeSize += int64(n)
	}
```

`seglog.go:468`（`maybeRotate` 的 HS 补写）：

```go
		n, werr := l.active.WriteAt(frame, l.activeSize)
		if werr != nil {
			return fmt.Errorf("seglog: 轮转补写 hardstate 写入新段 %s 偏移 %d 失败: %w",
				segName(newSeq), l.activeSize, werr)
		}
		l.activeSize += int64(n)
```

- [ ] **Step 5: 更新 activeSize 的字段注释**

`seglog.go:38` 的字段注释改为：

```go
	// activeSize 活动段已写入的有效字节数。两个用途：
	//   1. 轮转判定（>= SegMaxBytes 触发）
	//   2. 下一次写入的偏移（WriteAt 的第二参数）
	// 预分配后文件的物理大小恒为 SegMaxBytes，与本字段不再相等——凡是
	// 需要「写了多少」的地方一律用本字段，绝不用 Stat().Size()。
	activeSize int64
```

- [ ] **Step 6: 运行全量测试**

Run: `go test ./internal/cluster/seglog/ -race -v`
Expected: PASS，全部用例（现有 7 个 + Task 1 的 2 个 + 本任务的 1 个）

Run: `go test ./internal/cluster/ -race -run TestPersist -v`
Expected: PASS（raftstore 侧零修改）

- [ ] **Step 7: 补充日志与注释自检**

本任务不新增运行期行为，因此不新增日志。确认：
- `activeSize` 字段注释已说明「为什么不能用 `Stat().Size()`」✓
- 两处 `OpenFile` 都注释了「为什么去掉 `O_APPEND`」✓
- 两处 `WriteAt` 的错误信息都带上了偏移（定位写失败位置的唯一线索）✓

- [ ] **Step 8: 提交**

```bash
git add internal/cluster/seglog/seglog.go internal/cluster/seglog/seglog_test.go
git commit -m "refactor(seglog): 去掉 O_APPEND，activeSize 兼作写入偏移

行为完全不变的机制铺垫：写入从「内核定位 EOF」改为「按 activeSize 显式
定位」，未预分配时两者等价。同时把 activeSize 的来源从 f.Stat().Size()
改为扫描算出的逻辑末尾——现状下两者恰好相等，但那依赖「扫描一定会物理
截断零尾」这个不写在任何地方的副作用，预分配落地后会静默踩塌轮转。

现有 7 个用例零修改通过。"
```

---

### Task 3: 预分配 + 轮转截回 + 条件落盘

**Files:**
- Modify: `internal/cluster/seglog/seglog.go`（`Log` 结构体、`Open`、`Append`、`maybeRotate`、`Sync`）
- Test: `internal/cluster/seglog/seglog_test.go`

**Interfaces:**
- Consumes: Task 1 的 `preallocate(f, size) (bool, error)` 与 `datasync(f) error`；Task 2 的 `activeSize` 偏移语义
- Produces:
  - `Log.prealloc bool` — 活动段是否已预分配（`datasync` 的使用前提）
  - `func (l *Log) preallocActive() error` — 预分配当前活动段并刷新 `prealloc`；调用方须持有 `l.mu`
  - `func (l *Log) syncActive() error` — 按 `prealloc` 条件分派 `datasync` / `File.Sync()`；调用方须持有 `l.mu`

- [ ] **Step 1: 写失败的测试**

在 `internal/cluster/seglog/seglog_test.go` 末尾追加：

```go
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
```

`seglog_test.go` 的 import 需要补 `"os"` 与 `"path/filepath"`（若尚未引入）。

- [ ] **Step 2: 运行测试确认它失败**

Run: `go test ./internal/cluster/seglog/ -run 'TestPreallocatedActiveSegmentKeepsLogicalActiveSize|TestRotationTruncatesClosedSegmentToLogicalSize' -v`
Expected（macOS）：`TestPreallocated...` FAIL，编译错误 `l2.prealloc undefined`
Expected（Linux）：同上

> 编译错误即红灯：`prealloc` 字段尚不存在。

- [ ] **Step 3: 给 Log 加 prealloc 字段与两个辅助方法**

在 `Log` 结构体（`seglog.go:32-51`）的 `buf` 字段之后追加：

```go
	// prealloc 当前活动段是否已预分配到 SegMaxBytes。
	//
	// 它是 datasync 的使用前提，不是性能开关：文件大小仍在增长时
	// fdatasync 不保证「文件长了」这件事落盘，掉电后已写入的尾部字节
	// 读不回来。非 Linux 平台、以及 fallocate 返回 ENOTSUP 的文件系统上
	// 恒为 false，落盘全程走完整 fsync，行为与本特性落地之前一致。
	prealloc bool
```

在 `maybeRotate` 之后、`Sync` 之前插入两个方法：

```go
// preallocActive 把当前活动段预分配到 SegMaxBytes，并刷新 prealloc 标志。
//
// 调用方须持有 l.mu。调用时机有两处：Open 打开活动段之后（且必须在
// activeSize 定下来之后——顺序写反会让 activeSize 变成 SegMaxBytes，
// 重启即触发轮转），以及 maybeRotate 创建新段之后。
//
// 失败语义：文件系统不支持时 prealloc=false、返回 nil，落盘退回 fsync，
// 功能不受影响——预分配是纯性能优化，不是正确性前提。只有真 I/O 错误
// （ENOSPC 等）才上抛。
func (l *Log) preallocActive() error {
	ok, err := preallocate(l.active, SegMaxBytes)
	if err != nil {
		return err
	}
	l.prealloc = ok
	if !ok {
		// 降级是低频且影响性能画像的事实，必须留痕：否则线上看到落盘慢
		// 于预期时，无从判断是盘慢还是根本没走上预分配这条路。
		l.lg.Info("seglog: 预分配未生效，落盘退回完整 fsync",
			"dir", l.dir, "segment", segName(l.activeSeq))
	}
	return nil
}

// syncActive 把活动段落盘。调用方须持有 l.mu。
//
// 已预分配时用 datasync（不同步 inode 元数据，实测单次 1.82ms → 0.61ms）；
// 未预分配时文件大小仍在增长，必须用完整 fsync。
//
// 注意：轮转屏障那次落盘**不能**走本方法——它紧跟在 Truncate 之后，
// 那一刻文件大小刚变，必须完整 fsync（见 maybeRotate）。
func (l *Log) syncActive() error {
	if l.prealloc {
		return datasync(l.active)
	}
	if err := l.active.Sync(); err != nil {
		return fmt.Errorf("seglog: fsync 活动段 %s 失败: %w", segName(l.activeSeq), err)
	}
	return nil
}
```

- [ ] **Step 4: 在 Open 里补分配活动段**

在 `Open` 里，`l := &Log{...}` 结构体构造**之后**（`seglog.go:290-299` 之后、
`commit := uint64(0)` 之前）插入：

```go
	// 活动段补分配。必须在 l 构造完成、activeSize 已定之后——顺序写反
	// 会让 activeSize 取到预分配后的物理大小（SegMaxBytes），重启即触发
	// 轮转（spec §2.3）。
	//
	// 重启后段文件的物理大小已被扫描截回逻辑末尾（零尾走 torn tail 分支
	// 物理截断），这里重新扩回 SegMaxBytes，让重启后的段重新获得「写入
	// 不改变文件大小」这个前提。
	if err := l.preallocActive(); err != nil {
		f.Close()
		return nil, nil, nil, err
	}
```

- [ ] **Step 5: 轮转时截回逻辑大小 + 给新段预分配**

修改 `maybeRotate`。在轮转屏障的 `l.active.Sync()`（`seglog.go:426`）**之前**
插入截断：

```go
	// 关段前把物理大小截回逻辑大小：已关闭段绝不能带预分配零尾——Open
	// 扫描对非末段坏帧的判定是「真损坏，拒绝启动」，零尾会让每一次重启
	// 都撞上它，整组日志永久打不开。
	//
	// 未预分配时物理大小本就等于 oldBytes，Truncate 是空操作，但仍然
	// 无条件执行：让两个平台走同一条代码路径，别留「只在 Linux 上执行
	// 的那几行」这种只在生产环境才跑到的分支。
	if err := l.active.Truncate(oldBytes); err != nil {
		return fmt.Errorf("seglog: 轮转前截断旧段 %s 到 %d 字节失败: %w",
			segName(oldSeq), oldBytes, err)
	}
```

轮转屏障那次 `Sync` 保持 `l.active.Sync()` **不变**，但补注释说明为什么
它不能改成 `syncActive`：

```go
	// 轮转屏障：旧段必须先 fsync 落盘、再关闭，然后才允许开新段。（原有
	// 注释保留）
	//
	// 这里必须是完整 fsync，不能走 syncActive：上一行的 Truncate 刚改过
	// 文件大小，fdatasync 不保证 inode 元数据落盘。
	if err := l.active.Sync(); err != nil {
```

在 `l.activeSize = 0`（`seglog.go:455`）**之后**插入新段预分配：

```go
	// 新段预分配。放在 activeSize 归零之后、HS 补写之前：补写要用
	// WriteAt(l.activeSize)，此刻偏移必须已经是 0。
	if err := l.preallocActive(); err != nil {
		return err
	}
```

> 注意：`preallocActive` 失败时提前 `return`，此时尚未执行
> `l.segMax[oldSeq] = oldMaxIndex`——旧段保持「不可回收」，与既有的
> 轮转失败纪律一致（见该函数末尾注释）。

- [ ] **Step 6: 三处落盘改为条件分派**

`Append` 里的落盘（`seglog.go:393-397`）：

```go
	if sync {
		if err := l.syncActive(); err != nil {
			return err
		}
	}
```

`maybeRotate` 里 HS 补写后的落盘（`seglog.go:479-481`）：

```go
		// 补写帧必须立即落盘，不能等下一次批量刷盘（原有注释保留）。
		// 走 syncActive：此刻新段刚预分配完、写入未改变文件大小。
		if serr := l.syncActive(); serr != nil {
			return fmt.Errorf("seglog: 轮转补写 hardstate 落盘新段 %s 失败: %w", segName(newSeq), serr)
		}
```

`Log.Sync`（`seglog.go:500-511`）：

```go
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active == nil {
		return fmt.Errorf("seglog: 已关闭，拒绝 Sync")
	}
	return l.syncActive()
}
```

- [ ] **Step 7: 运行测试确认通过**

Run: `go test ./internal/cluster/seglog/ -race -v`
Expected: PASS，全部用例

Run: `go test ./internal/cluster/ -race -v`
Expected: PASS，**`raftstore_test.go` 的 12 个用例零修改**（Global Constraints 的验收锚）

- [ ] **Step 8: 补充关键节点日志**

确认以下点已有日志，缺则补上（用 `l.lg`，禁止 `fmt.Printf`）：
- **降级留痕**：`preallocActive` 中 `!ok` 分支的 `Info`（Step 3 已写）——
  这是唯一必须新增的日志：预分配是否生效直接决定性能画像，线上排查
  「落盘怎么这么慢」时，第一条要 grep 的就是它。
- **`Open` 完成**：现有 `lg.Info("seglog: 打开完成", ...)` 追加
  `"prealloc", l.prealloc`，让每次启动都能确认走的是哪条路径。
- **轮转完成**：现有 `lg.Info("seglog: 段轮转完成", ...)` 追加
  `"truncatedTo", oldBytes`，让「关段是否截回逻辑大小」可从日志验证。
- **热路径零日志**：`Append` / `syncActive` 不加任何日志——它们是每轮
  Ready 调用的热路径，加日志会淹没日志系统（既有的热循环规则）。

- [ ] **Step 9: 补充意图注释自检**

- `prealloc` 字段注释说明「它是 datasync 的前提，不是性能开关」✓
- `preallocActive` / `syncActive` 都有 doc comment，写明「调用方须持有
  `l.mu`」与调用时机 ✓
- `Open` 里补分配处注释了顺序约束的**后果**（写反会重启即轮转）✓
- `maybeRotate` 的 `Truncate` 注释了**为什么**（非末段零尾 = 拒绝启动）
  以及为什么无条件执行 ✓
- 轮转屏障处注释了为什么不能用 `syncActive` ✓

- [ ] **Step 10: 提交**

```bash
git add internal/cluster/seglog/seglog.go internal/cluster/seglog/seglog_test.go
git commit -m "feat(seglog): 段文件预分配 + 条件 fdatasync

新建段时 fallocate 到 SegMaxBytes，写入不再改变文件大小，落盘按 prealloc
条件分派 fdatasync / fsync——实测单次 1.82ms → 0.61ms。

三个易错点已锁死：preallocActive 必须在 activeSize 定下来之后（写反则
重启即轮转）；轮转关段前必须 Truncate 回逻辑大小（否则非末段零尾 =
拒绝启动）；轮转屏障那次落盘必须完整 fsync（Truncate 刚改过文件大小）。

raftstore_test.go 12 个用例零修改通过。"
```

---

### Task 4: 零尾与 torn tail 的日志判别

预分配后，每一次干净重启都会在末段撞上零尾并走 torn tail 分支，打出
`Warn "检测到 torn tail"`。运维看到会以为掉过电——**日志一旦开始说谎，
真事故里 `search_logs` 就不可信了**。本任务把「预分配零尾」与「真 torn
write」分开记。

**Files:**
- Modify: `internal/cluster/seglog/seglog.go`（`Open` 的 torn 分支）
- Test: `internal/cluster/seglog/seglog_test.go`

**Interfaces:**
- Consumes: Task 3 的预分配行为
- Produces: `func allZero(b []byte) bool` — 判定切片是否全零

- [ ] **Step 1: 写失败的测试**

在 `internal/cluster/seglog/seglog_test.go` 末尾追加：

```go
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
	l2, _, ents, err := Open(dir, slog.New(h))
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
	l2, _, ents, err := Open(dir, slog.New(h))
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
```

`seglog_test.go` 的 import 需要补 `"context"`、`"strings"`、`"sync"`。

- [ ] **Step 2: 运行测试确认它失败**

Run: `go test ./internal/cluster/seglog/ -run 'TestZeroTailLogsDebugNotWarn|TestGenuineTornTailStillLogsWarn' -v`
Expected: `TestZeroTailLogsDebugNotWarn` FAIL —— 未找到 Debug「预分配零尾」，
且命中了 Warn「torn tail」。`TestGenuineTornTailStillLogsWarn` PASS（现状行为）。

- [ ] **Step 3: 加 allZero 辅助函数**

在 `seglog.go` 的 `scanSegSeqs` 之后追加：

```go
// allZero 判定切片是否全零。
//
// 用途：区分「预分配段尾的零填充」与「掉电导致的真 torn write」。两者都
// 让 readFrame 报坏帧，但前者是每次干净重启的正常形态、后者是需要告警的
// 异常——不分开记，Warn 就会被干净重启淹没成噪声。
//
// 全零并不能百分之百证明是预分配（真 torn write 也可能恰好落在一段零
// 扇区上），但那种情况按「零尾」处理同样安全：截断行为完全一致，只是
// 少一条告警。反方向才危险（把真损坏当零尾静默掉），而非零字节一定
// 走告警分支，那个方向不会误判。
func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: 在 torn 分支里分流**

修改 `Open` 的末段坏帧分支（`seglog.go:179-189`）：

```go
				// 末段坏帧：两种成因，必须分开记。
				//   - 全零：预分配段尾的零填充，每次干净重启的正常形态
				//   - 非全零：掉电时最后一次 write() 只落盘一部分，真 torn write
				// 不分开的话，Warn 会被干净重启淹没成噪声，真事故里就没人信它了。
				//
				// 两种成因的**处理动作完全一致**：物理截断到好帧边界。绝不
				// 静默保留坏字节——续写虽然按 activeSize 偏移覆盖写，但残留
				// 在逻辑末尾之后的字节会让下次扫描再次撞上同一分支。
				discarded := len(data) - off
				zeroTail := allZero(data[off:])
				if err := os.Truncate(path, int64(off)); err != nil {
					return nil, nil, nil, fmt.Errorf("seglog: 截断段 %s 到偏移 %d 失败: %w", name, off, err)
				}
				if zeroTail {
					lg.Debug("seglog: 预分配零尾，截断到逻辑末尾",
						"segment", name, "goodOffset", off, "zeroBytes", discarded)
				} else {
					lg.Warn("seglog: 检测到 torn tail，已截断到好帧边界",
						"segment", name, "goodOffset", off, "discardedBytes", discarded)
					tornInfo = name
				}
				break
```

> `tornInfo` 只在真 torn 时置位：`Open` 末尾那条汇总日志的
> `tornTruncated` 字段因此不再被干净重启点亮。

- [ ] **Step 5: 运行测试确认通过**

Run: `go test ./internal/cluster/seglog/ -race -v`
Expected: PASS，全部用例（含现有 `TestReopenTruncatesTornTail` 零修改）

- [ ] **Step 6: 补充关键节点日志自检**

本任务改的就是日志本身。确认：
- 零尾走 `Debug`，真 torn 走 `Warn`，两条都带 `segment` + `goodOffset` +
  字节数——足以在不复现的情况下判断「截掉了多少、从哪截的」✓
- `tornInfo` 只在真 torn 置位，`Open` 汇总日志的 `tornTruncated` 恢复
  可信 ✓
- 判别只在末段、只在遇到坏帧时做一次，不进热路径 ✓

- [ ] **Step 7: 补充意图注释自检**

- `allZero` 的 doc comment 说明了**为什么需要它**，以及「全零误判的方向
  是安全的、非零不会误判」这个不对称性 ✓
- torn 分支注释说明了「两种成因处理动作一致，只是告警级别不同」✓

- [ ] **Step 8: 提交**

```bash
git add internal/cluster/seglog/seglog.go internal/cluster/seglog/seglog_test.go
git commit -m "fix(seglog): 区分预分配零尾与真 torn tail 的日志级别

预分配后每次干净重启都会在末段撞上零尾并走 torn tail 分支。不分流的话
Warn「检测到 torn tail」会被干净重启淹没成噪声——日志一旦开始说谎，真
事故里 search_logs 就不可信了。

全零尾走 Debug，非零尾保留 Warn；截断动作两者完全一致。tornInfo 只在真
torn 时置位，Open 汇总日志的 tornTruncated 字段恢复可信。"
```

---

### Task 5: 部署文档补容量估算

预分配把每组的磁盘占用下界从「实际写入量」抬到了固定的 `SegMaxBytes`。
默认 3 组 = 192MiB 可忽略，但 `data_groups` 上限 64 时是 4GiB——必须写进
文档，不能让运维在磁盘水位告警时才发现。

**Files:**
- Modify: 部署/运维文档中的容量估算章节（实现时先 `grep -rn "data_groups" docs/` 定位；若不存在容量估算章节，则在 `docs/` 下最贴近部署主题的文档里新增一节）

**Interfaces:**
- Consumes: Task 3 的预分配行为
- Produces: 无代码接口

- [ ] **Step 1: 定位文档落点**

Run: `grep -rn "data_groups\|磁盘水位\|disk_watermark" docs/ --include=*.md`
把命中的文件列出来，选择讲部署/配置/容量的那一份。

- [ ] **Step 2: 写入容量估算**

在选定文档中补一节（措辞按该文档既有风格调整）：

```markdown
### raft 日志段文件的磁盘占用

每个 raft 组的**活动段**会被一次性预分配到 64MiB（`SegMaxBytes`），
无论实际写入多少。这是为了让同步落盘不必同步 inode 元数据——实测单次
1.82ms → 0.61ms。

容量下界 = `data_groups` × 64MiB：

| `data_groups` | 预分配占用 |
|---|---|
| 3（默认） | 192MiB |
| 16 | 1GiB |
| 64（上限） | 4GiB |

这是**下界不是上界**：已关闭但尚未被 `TruncateTo` 回收的段另计。配置
大 `data_groups` 时，磁盘水位阈值需相应留出余量。

非 Linux 平台不预分配，无此占用。
```

- [ ] **Step 3: 提交**

```bash
git add docs/
git commit -m "docs(deploy): 补 raft 日志段文件的预分配容量估算

预分配把每组磁盘占用下界抬到固定 64MiB。默认 3 组 192MiB 可忽略，
data_groups 上限 64 时是 4GiB——不写进文档，运维会在磁盘水位告警时
才发现。"
```

---

### Task 6: 全量回归与三机实测验收

本任务不写代码，只做验收。**它是 Global Constraints 里「验收锚」与
「平台可观测性限制」的兑现处，不可跳过。**

**Files:**
- Create: 实测数据留痕（scratchpad，不入库）

**Interfaces:**
- Consumes: Task 1-4 的全部改动
- Produces: 验收结论（吞吐对照 + 摊销比）

- [ ] **Step 1: macOS 全量回归**

Run: `go test ./... -race`
Expected: PASS。**特别确认 `internal/cluster/raftstore_test.go` 的 12 个
用例零修改通过**——任何一个需要改动都说明语义变了，停下来说明原因，
不要改测试。

- [ ] **Step 2: 交叉编译 Linux 二进制**

```bash
GOOS=linux GOARCH=amd64 go build -o /tmp/sq.linux ./cmd/sq
GOOS=linux GOARCH=amd64 go test -c -o /tmp/seglog.linux.test ./internal/cluster/seglog
GOOS=linux GOARCH=amd64 go test -c -o /tmp/cluster.linux.test ./internal/cluster
GOOS=linux GOARCH=amd64 go test -c -o /tmp/e2e.linux.test ./test/e2e
```

> 绝不在远端装 Go 工具链——本地交叉编译后 scp。

- [ ] **Step 3: Linux 侧单测（预分配路径的唯一可观测处）**

scp 后在测试机上运行：

```bash
./seglog.linux.test -test.v
./cluster.linux.test -test.run 'TestPersist|TestConfChange' -test.v
```

Expected: PASS。**这一步是 `TestPreallocatedActiveSegmentKeepsLogicalActiveSize`
唯一真正生效的地方**——macOS 上 `prealloc == false`，顺序写反了也照样绿
（spec §2.3 的平台可观测性限制）。

- [ ] **Step 4: 三机吞吐对照**

**必须同一批次三台机器，且必须重跑 seglog 基线做同环境对照**——跨批次
的绝对数字不可比，这是 B2 与 sharedlog 两次翻车的共同根因。

对 `seglog 基线` 与 `本方案` 两档，各跑 quorum-fsync 的 conc=16 与
conc=256，**≥3 轮交错**（基线→本方案→基线→…，不要跑完一档再跑另一档）：

```bash
SQ_BENCH=1 SQ_E2E_BROKER=/root/sq.linux ./e2e.linux.test \
  -test.run TestClusterWriteThroughput -test.v -test.timeout=40m
```

报**中位数 + 离散度**（`(max-min)/median`）。离散度 >10% 的点标注为
不可信，不得引用。

- [ ] **Step 5: 核对验收标准**

| 标准 | 判定 |
|---|---|
| `raftstore_test.go` 12 用例零修改通过 | Step 1 |
| macOS `-race` 全量绿 | Step 1 |
| Linux 单测全绿（含预分配顺序锚） | Step 3 |
| quorum-fsync 档吞吐提升 | Step 4 |
| **quorum-mem 档不劣化**（硬底线） | Step 4 |

mem 档若出现劣化，说明 Task 2 的偏移写引入了额外成本，**回查
`WriteAt` 与原 `Write` 的差异**，不要接受「大概是噪声」。

- [ ] **Step 6: 结论留痕**

把原始输出与结论写进 scratchpad（不入库），并在 spec 末尾追加一节
「实测结果」记录中位数、离散度、机器规格与测量日期。

```bash
git add docs/superpowers/specs/2026-08-12-seglog-prealloc-fdatasync-design.md
git commit -m "docs(spec): 补 seglog 预分配的三机实测结果"
```

---

## Self-Review

**1. Spec coverage**

| Spec 章节 | 对应 Task |
|---|---|
| §2.1 预分配（mode 0，禁 KEEP_SIZE） | Task 1 Step 3、Task 3 Step 4/5 |
| §2.2 去 `O_APPEND` 改偏移写 | Task 2 Step 4 |
| §2.3 `activeSize` 来源 + 顺序约束 | Task 2 Step 3、Task 3 Step 1/4 |
| §2.4 轮转关段前 `Truncate` | Task 3 Step 5 |
| §2.5 条件 `fdatasync`（含轮转屏障例外） | Task 3 Step 3/5/6 |
| §3 零尾与 torn tail 的日志判别 | Task 4 全部 |
| §3 `Open` 后补分配 | Task 3 Step 4 |
| §4 平台边界（build tag 分文件） | Task 1 Step 3/4 |
| §5 磁盘占用写进部署文档 | Task 5 |
| §6.1 raftstore 12 用例零修改 | Task 3 Step 7、Task 6 Step 1 |
| §6.2 seglog 新增用例（四类） | Task 2 Step 1、Task 3 Step 1、Task 4 Step 1 |
| §6.3 macOS `-race` 全量绿 | Task 6 Step 1 |
| §6.4 Linux 三机实测 | Task 6 Step 4 |

无遗漏。

**2. Placeholder scan**

无 TBD/TODO；每个代码步骤都给了可直接粘贴的完整代码；无「参照 Task N」
式的省略。

**3. Type consistency**

- `preallocate(f *os.File, size int64) (bool, error)` — Task 1 定义，
  Task 3 `preallocActive` 调用，签名一致
- `datasync(f *os.File) error` — Task 1 定义，Task 3 `syncActive` 调用，
  签名一致
- `Log.prealloc bool` — Task 3 Step 3 定义，Task 3 Step 3/6 与 Task 4
  测试引用，名称一致
- `preallocActive()` / `syncActive()` — Task 3 Step 3 定义，Task 3
  Step 4/5/6 调用，名称一致
- `allZero(b []byte) bool` — Task 4 Step 3 定义，Task 4 Step 4 调用
- `segName(seq uint64) string` / `scanSegSeqs(dir)` — 既有函数，测试中
  按现有签名使用

**4. 已知的可观测性缺口（明写，不藏）**

`TestPreallocatedActiveSegmentKeepsLogicalActiveSize` 对「预分配顺序」的
断言只在 Linux 生效。这不是测试设计缺陷，是平台能力差异的必然结果——
补偿手段是 Task 6 Step 3 把 Linux 单测列为不可跳过的验收项。
