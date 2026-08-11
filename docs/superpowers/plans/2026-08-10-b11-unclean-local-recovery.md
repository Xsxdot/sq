# B11 不干净关机的本地自恢复 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **同时必须遵守 `instrumenting-code`**：本计划每个实现类 task 都带「加日志」与「加注释」两个 step，它们不是可选装饰；缺失即为未完成。

**Goal:** 让不干净关机的节点在能证明本地日志完好时直接以原身份从本地日志恢复，消除「全集群同时硬宕后三节点互相等待、谁都起不来」这一 B10 遗留缺口。

**Architecture:** 在现有 `NewManager` 的三分支恢复判定上引入第四个判据——**机器世代**（Linux `boot_id` / darwin `kern.boottime`），把判定扩为五分支。世代未变即证明页缓存从未丢失，本地日志可信，直接走既有的干净回放路径（mem 档另需抬 term——见 Task 6b，投票记录走的是 NoSync 异步路径，可能没落盘）；`quorum-fsync` 档因每轮 `MustSync` 已落盘，即使机器重启也可直接本地恢复；只有「`quorum-mem` + 机器真重启过」这一格保留拒启，并由新增的 `sq recover` 命令提供一次性、绑定机器世代的运维签字出口。判定逻辑抽成纯函数供 `NewManager` 与 `sq recover` 共用。

**Tech Stack:** Go；`go.etcd.io/raft/v3`；`github.com/cockroachdb/pebble/v2`（经 `internal/store` 唯一写入口）；`golang.org/x/sys/unix`（darwin 世代读取，已是间接依赖）；`google.golang.org/protobuf`。

## Global Constraints

- **Spec 是唯一事实来源**：`docs/superpowers/specs/2026-08-10-unclean-local-recovery-design.md`。与本计划冲突时以 spec 为准。
- ~~**不新增任何刷盘机制**：`Manager.flusher()` 的 200ms 周期 fsync 已存在，mem 档损失面已有界（≤200ms）。不加 `store.SyncWAL`、不加 `syncLoop`、不加 `cluster.sync_interval`。~~
  **订正（08-11，Task 9）**：这条约束建立在一个假前提上——`flusher()` 提交的是**空批次**，被 Pebble 的 `if b.Empty() { return nil }` 短路，从来没 fsync 过，损失面根本没有界。**Task 9 因此新增了 `store.SyncWAL`**。约束改为：**不改刷盘机制的形态**（200ms 周期、启动条件、不做成配置项三者一律不变），只把这个空操作修好。`syncLoop`、`cluster.sync_interval` 仍然不加。
- **不改动写路径**：本批不碰 `Persist`/`applyEntry`/`syncPersist`，写吞吐不受影响。
- **B2 铁律**：一切写经 `store` 唯一写入口（`NewBatch` + `ApplyWith`），不得持有裸 `*pebble.DB`。
- **`raft/bootgen` 只在能启动成功的路径上写**（`clean-resume`/`fresh`/`local-resume`/`local-forced`），**`ErrUncleanShutdown` 分支绝不写**。这是安全门能否成立的关键，Task 5 有专门守卫用例。
- **两条新恢复路径必须复用 `buildGroup(g, clean=true, peers)`**，不得另写回放逻辑——否则会丢掉其中既有的「未完成快照安装」处理（`manager.go:579`）。
- **日志用 `m.lg` / `r.lg`（`slog`），禁止 `fmt.Printf`**；`sq recover` 是 CLI，面向运维的报告用 `fmt.Fprintf(os.Stdout, ...)` 输出，但一切**事件**（授予许可）仍打 slog。
- **注释用中文**，解释「为什么」而非「做了什么」；新文件必须有文件头注释（职责 + 边界），导出函数必须有 doc 注释。
- **提交信息结尾**：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- **不改 `/Users/xushixin/workspace/sq-m5` 与 `sq-m5b`**。
- **一切临时文件、冒烟用的数据目录、编译产物，全部落在任务仓库内（用 `.smoke/`、`.tmp/` 之类跑完即删的目录），绝不写 `/tmp`、`/var` 或任何仓库外路径。** 这不是洁癖：本计划若在 handoff 之类的托管执行环境里跑，仓库外写入会触发 `external_directory` 审批；而当命令是由 subagent（子会话）发起时，审批请求归属的会话不是任务自身会话，托管方产不出工单，**没有人能批准它，任务就静默挂死**——2026-08-10 第一次派发即因此卡了 85 分钟。唯一的例外是本文件 Task 8 Step 3 的 Linux 交叉编译产物，那一步由本地操作者手工执行，不在托管任务范围内。
- 每个 task 结束时跑 `go build ./... && go vet ./...`，全绿才提交。

## 文件结构

| 文件 | 状态 | 职责 |
|---|---|---|
| `internal/cluster/bootgen.go` | 新建 | 机器世代的类型、env 覆盖、可注入 provider 与统一入口 |
| `internal/cluster/bootgen_linux.go` | 新建 | Linux 实现：读 `/proc/sys/kernel/random/boot_id` |
| `internal/cluster/bootgen_darwin.go` | 新建 | darwin 实现：`unix.SysctlTimeval("kern.boottime")` |
| `internal/cluster/bootgen_other.go` | 新建 | 其余平台：恒返回「读不到」 |
| `internal/cluster/bootgen_test.go` | 新建 | Task 1 的测试 |
| `internal/cluster/recovery.go` | 新建 | 五分支恢复判定的纯函数（无 raft、无磁盘依赖） |
| `internal/cluster/recovery_test.go` | 新建 | Task 3 的表驱动测试 |
| `internal/cluster/raftstore.go` | 修改 | `raft/bootgen` 与 `raft/local_recover_permit` 的读写；`ForceLocalRecover` |
| `internal/cluster/raftstore_test.go` | 修改 | Task 2、Task 4 的测试 |
| `internal/cluster/manager.go` | 修改 | `NewManager` 接线五分支；`Options.BootGen` 注入点 |
| `internal/cluster/manager_test.go` | 修改 | Task 5 的分支集成测试与安全门守卫用例 |
| `cmd/sq/recover.go` | 新建 | `sq recover` 子命令：报告渲染与许可授予 |
| `cmd/sq/main.go` | 修改 | 子命令分流；头注释重写 |
| `test/e2e/cluster_scenario_test.go` | 修改 | 改写 1 条、新增 3 条场景用例 |
| `test/e2e/cluster_proc_test.go` | 修改 | `procCluster` 支持注入 `SQ_BOOTGEN_OVERRIDE` |
| `sq.example.yaml` | 修改 | 自愈说明补入五分支与签字流程 |
| `docs/superpowers/backlog.md` | 修改 | B11 状态与验收回填 |

**决策抽成 `recovery.go` 纯函数是刻意的**：`NewManager` 与 `sq recover` 必须给出完全一致的判断，否则会出现「命令说你不用签字、进程说你要签字」这种最伤运维信任的分歧。共用一个函数是唯一可靠的保证，同时让五条分支可以脱离 raft 与磁盘直接单测。

---

### Task 1: 机器世代读取

**Files:**
- Create: `internal/cluster/bootgen.go`
- Create: `internal/cluster/bootgen_linux.go`
- Create: `internal/cluster/bootgen_darwin.go`
- Create: `internal/cluster/bootgen_other.go`
- Test: `internal/cluster/bootgen_test.go`

**Interfaces:**
- Consumes: 无（本 task 是叶子）
- Produces:
  - `type BootGenFunc func() (string, error)`
  - `func machineBootGen() (string, error)`（平台分派，各 build tag 文件各一份实现）
  - `func resolveBootGen(fn BootGenFunc, lg *slog.Logger) (string, bool)`——返回 `(世代, 是否可用)`；`fn` 为 nil 时用 `machineBootGen`；`SQ_BOOTGEN_OVERRIDE` 非空时优先并打 Error 日志
  - `const bootGenOverrideEnv = "SQ_BOOTGEN_OVERRIDE"`

- [ ] **Step 1: 写失败的测试**

新建 `internal/cluster/bootgen_test.go`：

```go
//go:build !windows

package cluster

import (
	"errors"
	"testing"
)

// TestResolveBootGenUsesProvider 注入的 provider 优先于平台实现。
func TestResolveBootGenUsesProvider(t *testing.T) {
	got, ok := resolveBootGen(func() (string, error) { return "gen-a", nil }, testSlog(t))
	if !ok || got != "gen-a" {
		t.Fatalf("resolveBootGen = (%q, %v); want (\"gen-a\", true)", got, ok)
	}
}

// TestResolveBootGenUnavailable provider 报错时必须返回不可用，且**不得**
// 返回空串当作一个正常世代——空串会和另一次「读不到」比较相等，
// 于是「两次都读不到」会被误判成「机器没重启过」，安全门直接失效。
func TestResolveBootGenUnavailable(t *testing.T) {
	got, ok := resolveBootGen(func() (string, error) { return "", errors.New("boom") }, testSlog(t))
	if ok {
		t.Fatalf("resolveBootGen 在 provider 报错时 ok=true（got=%q）——读不到必须判为不可用", got)
	}
}

// TestResolveBootGenEnvOverride 环境变量覆盖生效（进程级 e2e 靠它模拟重启）。
func TestResolveBootGenEnvOverride(t *testing.T) {
	t.Setenv(bootGenOverrideEnv, "gen-forced")
	got, ok := resolveBootGen(func() (string, error) { return "gen-real", nil }, testSlog(t))
	if !ok || got != "gen-forced" {
		t.Fatalf("resolveBootGen = (%q, %v); want (\"gen-forced\", true)", got, ok)
	}
}

// TestMachineBootGenOnThisPlatform 本平台能读出一个非空、可重复的世代。
// Linux/darwin 都应成立；其它平台本用例跳过（machineBootGen 恒报错）。
func TestMachineBootGenOnThisPlatform(t *testing.T) {
	first, err := machineBootGen()
	if err != nil {
		t.Skipf("本平台不支持机器世代读取（这是允许的保守形态）: %v", err)
	}
	if first == "" {
		t.Fatal("machineBootGen 返回空串但无错误——空串不是合法世代")
	}
	second, err := machineBootGen()
	if err != nil {
		t.Fatalf("第二次读取失败: %v", err)
	}
	if first != second {
		t.Fatalf("同一次运行内两次读取不一致：%q vs %q——世代必须在一次开机内稳定", first, second)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/ -run 'BootGen' -v`
Expected: FAIL，`undefined: resolveBootGen`、`undefined: machineBootGen`、`undefined: bootGenOverrideEnv`

- [ ] **Step 3: 写实现**

新建 `internal/cluster/bootgen.go`：

```go
// bootgen.go 提供「机器世代」（boot generation）的读取。
//
// 职责：
//   - 给出一个在「一次开机期间恒定、机器重启后必变」的标识
//   - 统一 env 覆盖（测试用）与可注入 provider（单测用）两条旁路
//
// 边界：
//   - 不解释这个值的用途，也不做任何比较与判定——判定在 recovery.go
//   - 不落盘：读写 raft/bootgen 键是 raftstore 的事
//   - 平台实现分散在 bootgen_{linux,darwin,other}.go，本文件不含系统调用
//
// 为什么需要它：不干净关机之后，本地 raft 日志可不可信，取决于**页缓存
// 有没有丢**。Pebble 的 NoSync 写入走 write(2) 进页缓存，进程被 kill -9
// 之后数据一条不少；只有机器掉电/内核崩溃才会丢尾巴。机器世代就是「这台
// 机器有没有重启过」的可验证证据。
package cluster

import (
	"errors"
	"log/slog"
	"os"
)

// bootGenOverrideEnv 是机器世代的环境变量覆盖名。
//
// 仅供测试：进程级 e2e 起的是真 broker 进程，注不进 Go 函数，只能靠
// 环境变量模拟「机器重启过」。生产环境设置它会让安全门失效（真重启也
// 被判成没重启），因此一旦生效就打 Error 日志——这条不能只靠文档拦。
const bootGenOverrideEnv = "SQ_BOOTGEN_OVERRIDE"

// errBootGenUnsupported 是不支持机器世代读取的平台返回的错误。
var errBootGenUnsupported = errors.New("cluster: 本平台不支持机器世代读取")

// BootGenFunc 是机器世代的读取函数，供测试注入。
type BootGenFunc func() (string, error)

// resolveBootGen 解析本机当前的机器世代。
//
// 参数：
//   - fn: 注入的读取函数；nil 时用平台实现 machineBootGen
//   - lg: 日志器，用于 env 覆盖告警与读取失败告警
//
// 返回：
//   - 世代字符串与它是否可用
//
// 注意：读不到时返回 ("", false) 而**不是** ("", true)。这个区分是安全
// 关键——若把「读不到」当成一个值为空串的正常世代，那么两次都读不到就
// 会比较相等，进而被判成「机器没重启过、本地日志可信」，安全门当场失效。
func resolveBootGen(fn BootGenFunc, lg *slog.Logger) (string, bool) {
	if v := os.Getenv(bootGenOverrideEnv); v != "" {
		lg.Error("机器世代被环境变量覆盖，仅供测试——生产环境设置它会让不干净关机的安全门失效",
			"env", bootGenOverrideEnv, "value", v)
		return v, true
	}
	if fn == nil {
		fn = machineBootGen
	}
	v, err := fn()
	if err != nil {
		lg.Warn("机器世代读取失败，按「机器可能已重启」保守处理", "err", err)
		return "", false
	}
	if v == "" {
		lg.Warn("机器世代读取到空串，按「机器可能已重启」保守处理")
		return "", false
	}
	return v, true
}
```

新建 `internal/cluster/bootgen_linux.go`：

```go
//go:build linux

package cluster

import (
	"fmt"
	"os"
	"strings"
)

// bootIDPath 是 Linux 内核暴露的本次启动唯一标识。每次开机重新生成，
// 开机期间恒定——正是「机器有没有重启过」的权威来源。
//
// 容器语义正好是我们要的：容器内读到的是**宿主机**内核的值。容器重启
// 而宿主没重启 → 值不变 → 判定页缓存完好，正确；容器迁移到另一台宿主
// → 值变了 → 保守处理，也正确。
const bootIDPath = "/proc/sys/kernel/random/boot_id"

// machineBootGen 读取本机的机器世代（Linux 实现）。
func machineBootGen() (string, error) {
	data, err := os.ReadFile(bootIDPath)
	if err != nil {
		return "", fmt.Errorf("cluster: 读 %s: %w", bootIDPath, err)
	}
	v := strings.TrimSpace(string(data))
	if v == "" {
		return "", fmt.Errorf("cluster: %s 内容为空", bootIDPath)
	}
	return v, nil
}
```

新建 `internal/cluster/bootgen_darwin.go`：

```go
//go:build darwin

package cluster

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// machineBootGen 读取本机的机器世代（darwin 实现）。
//
// darwin 没有 boot_id，用内核记录的开机时刻代替：它在一次开机内恒定、
// 重启后必变，语义与 Linux 的 boot_id 等价。精度到微秒，同一台机器两次
// 开机撞上同一微秒不可能。
func machineBootGen() (string, error) {
	tv, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		return "", fmt.Errorf("cluster: 读 sysctl kern.boottime: %w", err)
	}
	return fmt.Sprintf("boottime-%d.%06d", tv.Sec, tv.Usec), nil
}
```

新建 `internal/cluster/bootgen_other.go`：

```go
//go:build !linux && !darwin

package cluster

// machineBootGen 在没有已知机器世代来源的平台上恒报「不支持」。
//
// 这不是缺陷而是刻意的保守方向：读不到世代 = 无法证明机器没重启过 =
// 一律按「可能重启过」处理，最坏结果是多走一次重入编排（安全），
// 绝不会误判成「本地日志可信」（危险）。
func machineBootGen() (string, error) {
	return "", errBootGenUnsupported
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/ -run 'BootGen' -v`
Expected: 4 条全 PASS（`TestMachineBootGenOnThisPlatform` 在 macOS/Linux 上应真跑而非 skip）

- [ ] **Step 5: 加关键节点日志**

本 task 的日志已内联在 Step 3 的实现里，逐条核对存在：

- env 覆盖生效 → `lg.Error`，含变量名与值（**这是安全门被绕过的唯一可观测证据**）
- provider 报错 → `lg.Warn`，含 err
- 读到空串 → `lg.Warn`

不打「成功读取」日志：`resolveBootGen` 在每次启动只调用一次，读到什么值会由 Task 5 的恢复判定日志一并输出，此处再打一行是重复。

- [ ] **Step 6: 加注释**

核对 Step 3 已包含：四个新文件各有文件头/常量注释说明职责与边界；`resolveBootGen`、`machineBootGen`（三份）、`BootGenFunc`、`bootGenOverrideEnv` 均有 doc 注释；`resolveBootGen` 的「读不到 ≠ 空串世代」有专门的「为什么」注释；`bootgen_other.go` 说明保守方向是刻意的。

- [ ] **Step 7: 提交**

```bash
go build ./... && go vet ./...
git add internal/cluster/bootgen.go internal/cluster/bootgen_linux.go internal/cluster/bootgen_darwin.go internal/cluster/bootgen_other.go internal/cluster/bootgen_test.go
git commit -m "$(cat <<'EOF'
feat(cluster): 机器世代读取——不干净关机后判断本地日志可不可信的硬证据

页缓存有没有丢，才是本地 raft 日志可不可信的分界线：Pebble 的 NoSync 走
write(2) 进页缓存，kill -9 之后数据一条不少，只有机器掉电/内核崩溃才丢尾巴。
机器世代（Linux boot_id / darwin kern.boottime）就是「这台机器有没有重启过」
的可验证证据。

读不到时返回 (\"\", false) 而不是空串世代：空串会和另一次「读不到」比较相等，
于是「两次都读不到」被误判成「机器没重启过」，安全门当场失效。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `raft/bootgen` 键的读写

**Files:**
- Modify: `internal/cluster/raftstore.go`（键常量表、键常量、新增两个方法）
- Test: `internal/cluster/raftstore_test.go`

**Interfaces:**
- Consumes: Task 1 的世代字符串（本 task 只当它是不透明字符串）
- Produces:
  - `func (r *raftStore) LoadBootGen() (string, bool, error)`
  - `func (r *raftStore) SaveBootGen(gen string) error`（Sync 落盘）
  - `const bootGenKey = "raft/bootgen"`

- [ ] **Step 1: 写失败的测试**

追加到 `internal/cluster/raftstore_test.go`：

```go
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
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/ -run 'TestBootGenRoundTrip' -v`
Expected: FAIL，`rs.LoadBootGen undefined`

- [ ] **Step 3: 写实现**

在 `internal/cluster/raftstore.go` 的键常量块中，`preRaftKey` 之后加入：

```go
	bootGenKey          = "raft/bootgen"
```

同时在文件头的键布局注释里，`raft/preraft` 那一条之后补：

```
//	raft/bootgen                 → 机器世代（写入时机见 SaveBootGen 注释：
//	                               只在能启动成功的路径上写，拒启分支绝不写）
```

在 `HasPreRaft` 之后追加两个方法：

```go
// LoadBootGen 读取盘上记录的机器世代。
//
// 返回：
//   - 世代字符串、是否存在、错误
//   - 从未写入过时返回 ("", false, nil)——首次以本版本启动的旧数据目录
//     即此形态，恢复判定必须把它当作「世代不可比 = 保守处理」
func (r *raftStore) LoadBootGen() (string, bool, error) {
	data, ok, err := r.st.Get([]byte(bootGenKey))
	if err != nil {
		return "", false, fmt.Errorf("raftstore LoadBootGen: %w", err)
	}
	if !ok {
		return "", false, nil
	}
	return string(data), true, nil
}

// SaveBootGen 写入本次启动的机器世代并 Sync 落盘。
//
// 写入时机是安全关键，调用方必须遵守：**只在决定走一条能启动成功的
// 路径之后才调用**（clean-resume / fresh / local-resume / local-forced），
// ErrUncleanShutdown 分支绝不调用。
//
// 理由：本键的语义是「本数据目录最后一次被运行中的节点写入，发生在哪个
// 机器世代」。若拒启分支也写，序列就变成——机器重启 → 判定需要人工签字
// → 顺手写了新世代 → 重入编排失败拒启 → 运维直接重启进程 → 此时盘上
// 世代已等于当前世代 → 自动判定「机器没重启过、本地日志可信」→ 签字门
// 被静默绕过。整扇安全门会因这一处顺序错误而形同虚设。
func (r *raftStore) SaveBootGen(gen string) error {
	b := r.st.NewBatch()
	if err := b.Set([]byte(bootGenKey), []byte(gen)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveBootGen: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveBootGen: %w", err)
	}
	r.lg.Info("机器世代已记录", "gen", gen)
	return nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/ -run 'TestBootGenRoundTrip' -v`
Expected: PASS

- [ ] **Step 5: 加关键节点日志**

核对 Step 3 已包含：`SaveBootGen` 成功后 `r.lg.Info("机器世代已记录", "gen", gen)`——成功路径不静默，重启排障时能一眼看到「这次记录的是哪个世代」。`LoadBootGen` 不单独打日志：它的结果会随 Task 5 的恢复判定单行日志一并输出，此处重复。

- [ ] **Step 6: 加注释**

核对 Step 3 已包含：键布局注释新增一行；`LoadBootGen` doc 注释说明「不存在」的含义；`SaveBootGen` doc 注释用一整段写清「为什么拒启分支绝不能写」——这段注释是给未来那个想「顺手统一到函数开头写」的人看的。

- [ ] **Step 7: 提交**

```bash
go build ./... && go vet ./...
git add internal/cluster/raftstore.go internal/cluster/raftstore_test.go
git commit -m "$(cat <<'EOF'
feat(cluster): raft/bootgen 键读写——并把「拒启分支绝不写」写进注释

SaveBootGen 的注释里用一整段讲清了它的写入时机为什么是安全关键：若在
ErrUncleanShutdown 分支也写，「拒启 → 运维重启进程 → 世代已相等 → 自动
本地恢复」会让签字门自己开了。这是一处功能测试看不出来的顺序缺陷，只能靠
注释与后续的守卫用例拦。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: 五分支恢复判定纯函数

**Files:**
- Create: `internal/cluster/recovery.go`
- Test: `internal/cluster/recovery_test.go`

**Interfaces:**
- Consumes: `AckMode`（`group.go` 既有：`AckQuorumFsync` / `AckQuorumMem`）
- Produces:
  - `type recoveryPath int`，常量 `pathCleanResume` / `pathFresh` / `pathLocalResume` / `pathLocalForced` / `pathRejoin`
  - `func (p recoveryPath) String() string`
  - `type recoveryInput struct { Clean, HasRaft bool; Mode AckMode; GenNow string; GenNowOK bool; GenStored string; GenStoredOK bool; PermitGen string; PermitOK bool }`
  - `func decideRecovery(in recoveryInput) (recoveryPath, string)`——第二个返回值是给日志与 `sq recover` 报告用的中文理由串

- [ ] **Step 1: 写失败的测试**

新建 `internal/cluster/recovery_test.go`：

```go
package cluster

import "testing"

// TestDecideRecovery 覆盖五条恢复分支的全部判据组合。
//
// 这张表就是 spec §3.2 的判定表本身；改动判定逻辑必须先改这张表。
func TestDecideRecovery(t *testing.T) {
	const genA, genB = "gen-a", "gen-b"
	cases := []struct {
		name string
		in   recoveryInput
		want recoveryPath
	}{
		{
			name: "有干净关机标记：原身份回归，其余判据一概不看",
			in:   recoveryInput{Clean: true, HasRaft: true, Mode: AckQuorumMem, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true},
			want: pathCleanResume,
		},
		{
			name: "全新目录：引导启动",
			in:   recoveryInput{Clean: false, HasRaft: false, Mode: AckQuorumMem, GenNow: genA, GenNowOK: true},
			want: pathFresh,
		},
		{
			name: "不干净 + 世代未变：进程崩溃而已，页缓存完好，本地恢复",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genA, GenNowOK: true, GenStored: genA, GenStoredOK: true},
			want: pathLocalResume,
		},
		{
			name: "不干净 + 世代变了 + fsync 档：每轮 MustSync 已落盘，无需签字",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumFsync, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true},
			want: pathLocalResume,
		},
		{
			name: "不干净 + 世代变了 + mem 档 + 有匹配许可：签字放行",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true, PermitGen: genB, PermitOK: true},
			want: pathLocalForced,
		},
		{
			name: "不干净 + 世代变了 + mem 档 + 无许可：重入编排",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true},
			want: pathRejoin,
		},
		{
			name: "许可绑的是别的世代：等于没有许可（堵死旧许可被后一次事故复用）",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true, PermitGen: genA, PermitOK: true},
			want: pathRejoin,
		},
		{
			name: "当前世代读不到：不可比，保守走重入编排",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNowOK: false, GenStored: genA, GenStoredOK: true},
			want: pathRejoin,
		},
		{
			name: "盘上没记过世代（旧数据目录首次升级）：不可比，保守走重入编排",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genA, GenNowOK: true, GenStoredOK: false},
			want: pathRejoin,
		},
		{
			name: "两侧世代都读不到：绝不能因为都是空串就判成相等",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNowOK: false, GenStoredOK: false},
			want: pathRejoin,
		},
		{
			name: "世代读不到 + fsync 档：档位本身就够，仍可本地恢复",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumFsync, GenNowOK: false, GenStoredOK: false},
			want: pathLocalResume,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, reason := decideRecovery(c.in)
			if got != c.want {
				t.Fatalf("decideRecovery = %v（理由：%s）; want %v", got, reason, c.want)
			}
			if reason == "" {
				t.Fatal("decideRecovery 的理由串为空——它要进日志和 sq recover 的报告，不能没有")
			}
		})
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/ -run 'TestDecideRecovery' -v`
Expected: FAIL，`undefined: recoveryInput`、`undefined: decideRecovery`

- [ ] **Step 3: 写实现**

新建 `internal/cluster/recovery.go`：

```go
// recovery.go 提供不干净关机后的恢复路径判定。
//
// 职责：
//   - 把「盘上状态 + 确认档位 + 机器世代 + 运维许可」四组判据映射到
//     唯一一条恢复路径，并给出可直接进日志/进报告的中文理由
//
// 边界：
//   - 纯函数：不碰 raft、不碰磁盘、不打日志、不读环境变量。四组判据由
//     调用方采集后传入
//   - 不执行恢复：本文件只回答「走哪条路」，怎么走在 manager.go
//
// 为什么要抽成纯函数：NewManager 与 sq recover 必须给出**完全一致**的
// 判断。一旦两处各写一套，迟早出现「命令说你不用签字、进程说你要签字」，
// 那是最伤运维信任的一类分歧。共用一个函数是唯一可靠的保证方式；顺带
// 让五条分支能脱离 raft 与磁盘直接单测（recovery_test.go 的判定表）。
package cluster

// recoveryPath 是不干净关机后的恢复路径。
type recoveryPath int

const (
	// pathCleanResume 上次是优雅停机：回放本地日志，原身份回归
	pathCleanResume recoveryPath = iota
	// pathFresh 全新数据目录：按成员表引导启动
	pathFresh
	// pathLocalResume 不干净关机但本地日志可证完好：等同 clean-resume
	pathLocalResume
	// pathLocalForced 不干净关机且日志可能残缺，但运维已签字放行：
	// 抬 term 后回放本地日志（抬 term 的理由见 manager.go 的调用点）
	pathLocalForced
	// pathRejoin 不干净关机且无法证明日志可信：交给重入编排
	// （求集群接纳→清空→learner 重入；求不到接纳则拒启且数据完好）
	pathRejoin
)

// String 返回路径的短名，用于日志的 recovery 字段。
func (p recoveryPath) String() string {
	switch p {
	case pathCleanResume:
		return "clean-resume"
	case pathFresh:
		return "fresh"
	case pathLocalResume:
		return "local-resume"
	case pathLocalForced:
		return "local-forced"
	case pathRejoin:
		return "rejoin"
	}
	return "unknown"
}

// recoveryInput 是恢复判定的全部输入判据。
//
// 字段两两成对的 OK 位不是冗余：世代「读不到」与世代「是空串」必须能
// 区分开，否则两次都读不到会比较相等，被误判成「机器没重启过」。
type recoveryInput struct {
	Clean   bool    // 干净关机标记是否存在
	HasRaft bool    // 盘上是否已有 raft 状态（任一组 HardState 或 applied 非零）
	Mode    AckMode // 确认档位

	GenNow        string // 本机当前机器世代
	GenNowOK      bool   // 当前世代是否可用（读不到时为 false）
	GenStored     string // 盘上记录的机器世代
	GenStoredOK   bool   // 盘上是否记录过世代（旧数据目录首次升级时为 false）

	PermitGen string // 运维许可绑定的机器世代
	PermitOK  bool   // 是否存在运维许可
}

// sameMachineBoot 判定「本数据目录上次被写入」与「现在」是否属于同一次开机。
//
// 只有两侧世代都可用且相等才为真。任何一侧不可用都返回 false——不可比
// 就是不可信，这是安全方向。
func (in recoveryInput) sameMachineBoot() bool {
	return in.GenNowOK && in.GenStoredOK && in.GenNow == in.GenStored
}

// permitValid 判定运维许可是否对**本次**事故有效。
//
// 许可绑定授予时的机器世代，因此只对运维当时看到的那一次事故有效：
// 机器再重启一次，世代就变了，旧许可自动失效。这条替代了 TTL，
// 因而不依赖时钟。
func (in recoveryInput) permitValid() bool {
	return in.PermitOK && in.GenNowOK && in.PermitGen == in.GenNow
}

// decideRecovery 判定恢复路径。
//
// 参数：
//   - in: 四组判据，采集方式见 recoveryInput 各字段注释
//
// 返回：
//   - 恢复路径
//   - 中文理由串（进 NewManager 的判定日志与 sq recover 的报告；恒非空）
//
// 判定表见 spec §3.2；改这里必须同步改 recovery_test.go 的用例表。
func decideRecovery(in recoveryInput) (recoveryPath, string) {
	switch {
	case in.Clean:
		return pathCleanResume, "上次为干净关机，回放本地日志以原身份回归"
	case !in.HasRaft:
		return pathFresh, "数据目录无 raft 状态，按成员表引导启动"
	case in.sameMachineBoot():
		// 机器没重启过 ⇒ 页缓存从未丢失 ⇒ NoSync 写入也都还在
		return pathLocalResume, "不干净关机，但机器世代未变（进程崩溃、机器未重启），本地日志完整，直接以原身份恢复"
	case in.Mode == AckQuorumFsync:
		// fsync 档跟随 raft.MustSync：条目与 term/vote 每次变更都已落盘，
		// 掉电只可能丢 commit 位点的推进，而那可由 leader 重新告知
		return pathLocalResume, "不干净关机且机器可能已重启，但确认档为 quorum-fsync（条目与投票每次变更均已落盘），本地日志可信，直接以原身份恢复"
	case in.permitValid():
		return pathLocalForced, "不干净关机且机器已重启，quorum-mem 档下日志尾可能残缺；已存在与本次机器世代匹配的运维许可，按签字放行本地恢复"
	case in.PermitOK:
		// 有许可但绑的是别的世代：等于没有
		return pathRejoin, "存在运维许可但绑定的机器世代与本次不符（旧事故的签字不能复用于本次），按不干净关机走重入编排"
	default:
		return pathRejoin, "不干净关机且无法证明本地日志可信（机器世代不可比或已变、确认档为 quorum-mem、无运维许可），走重入编排"
	}
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/ -run 'TestDecideRecovery' -v`
Expected: 11 个子用例全 PASS

- [ ] **Step 5: 加关键节点日志**

本 task **刻意不打任何日志**——`decideRecovery` 是纯函数，日志由调用方（Task 5 的 `NewManager`）在拿到路径与理由后打一行。这不是遗漏：纯函数打日志会让它无法在表驱动测试里被大量调用，也会让同一次判定在 `NewManager` 与 `sq recover` 两处打出两份重复日志。理由串就是本函数交给调用方的「日志素材」。

- [ ] **Step 6: 加注释**

核对 Step 3 已包含：文件头注释写清职责、边界与「为什么抽纯函数」；五个路径常量各有注释；`recoveryInput` 的 OK 位成对设计有「为什么」注释；`sameMachineBoot` / `permitValid` 各有 doc 注释（后者解释了它如何替代 TTL）；`decideRecovery` 指明与测试表的同步义务。

- [ ] **Step 7: 提交**

```bash
go build ./... && go vet ./...
git add internal/cluster/recovery.go internal/cluster/recovery_test.go
git commit -m "$(cat <<'EOF'
feat(cluster): 五分支恢复判定抽成纯函数——NewManager 与 sq recover 必须同口径

判定从三分支扩为五分支：不干净关机时，若机器世代未变（进程崩溃、机器没
重启）则页缓存完好、本地日志完整，直接原身份恢复；quorum-fsync 档即使机器
重启过也可直接恢复（条目与投票每次变更都已落盘）；只有 quorum-mem + 机器
真重启过这一格才需要运维签字，否则走既有重入编排。

抽成纯函数是因为 NewManager 和 sq recover 必须给出完全一致的判断——两处
各写一套，迟早出现「命令说你不用签字、进程说你要签字」。顺带让五条分支能
脱离 raft 与磁盘做表驱动测试（11 个组合）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: 许可键的读写与 `ForceLocalRecover`

**Files:**
- Modify: `internal/cluster/raftstore.go`
- Test: `internal/cluster/raftstore_test.go`

**Interfaces:**
- Consumes: Task 2 的键常量风格
- Produces:
  - `const recoverPermitKey = "raft/local_recover_permit"`
  - `type recoverPermit struct { GrantedAt string; Gen string }`
  - `func (r *raftStore) SaveRecoverPermit(p recoverPermit) error`（Sync）
  - `func (r *raftStore) LoadRecoverPermit() (recoverPermit, bool, error)`
  - `func (r *raftStore) ForceLocalRecover(dataGroups uint32) error`——把 0..dataGroups 每组 HardState 的 Term+1、Vote 清零，并删除许可键，**全部在同一批次内 Sync 提交**

- [ ] **Step 1: 写失败的测试**

追加到 `internal/cluster/raftstore_test.go`：

```go
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
```

同时确认 `raftstore_test.go` 顶部已 import `"go.etcd.io/raft/v3/raftpb"`；若未 import 则补上。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/ -run 'TestRecoverPermit|TestForceLocalRecover' -v`
Expected: FAIL，`undefined: recoverPermit`、`rs.ForceLocalRecover undefined`

- [ ] **Step 3: 写实现**

在 `internal/cluster/raftstore.go` 的键常量块中，`bootGenKey` 之后加入：

```go
	recoverPermitKey    = "raft/local_recover_permit"
```

文件头键布局注释补一行：

```
//	raft/local_recover_permit    → 一次性本地恢复许可（两行文本：授予时间 /
//	                               授予时机器世代；由 ForceLocalRecover 消费）
```

在 `SaveBootGen` 之后追加：

```go
// recoverPermit 是一次性本地恢复许可的内容。
//
// Gen 绑定授予时的机器世代，这是许可的作废机制：机器每重启一次世代就变，
// 旧许可自动失效。用世代而非 TTL，是为了不依赖时钟——签字只对运维当时
// 看到的那一次事故有效。
type recoverPermit struct {
	GrantedAt string // 授予时间（RFC3339，只给人看）
	Gen       string // 授予时的机器世代（作废判据）
}

// SaveRecoverPermit 写入一次性本地恢复许可并 Sync 落盘。
//
// 值编码为两行 UTF-8 文本（第一行授予时间、第二行授予时世代）而非
// protobuf：运维可能要用普通工具直接查看它，可读性远比编码效率重要，
// 而这个值一生只被写一次、读一次。
func (r *raftStore) SaveRecoverPermit(p recoverPermit) error {
	b := r.st.NewBatch()
	val := p.GrantedAt + "\n" + p.Gen
	if err := b.Set([]byte(recoverPermitKey), []byte(val)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveRecoverPermit: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveRecoverPermit: %w", err)
	}
	r.lg.Error("已写入一次性本地恢复许可——下次启动将允许带着可能残缺的本地日志恢复，可能丢失已确认的消息",
		"grantedAt", p.GrantedAt, "gen", p.Gen)
	return nil
}

// LoadRecoverPermit 读取一次性本地恢复许可。
//
// 返回：许可内容、是否存在、错误。格式不合法时按「不存在」处理并记
// Error——一个读不懂的许可绝不能被当成有效签字。
func (r *raftStore) LoadRecoverPermit() (recoverPermit, bool, error) {
	data, ok, err := r.st.Get([]byte(recoverPermitKey))
	if err != nil {
		return recoverPermit{}, false, fmt.Errorf("raftstore LoadRecoverPermit: %w", err)
	}
	if !ok {
		return recoverPermit{}, false, nil
	}
	parts := strings.SplitN(string(data), "\n", 2)
	if len(parts) != 2 || parts[1] == "" {
		r.lg.Error("本地恢复许可格式不合法，按无许可处理", "raw", string(data))
		return recoverPermit{}, false, nil
	}
	return recoverPermit{GrantedAt: parts[0], Gen: parts[1]}, true, nil
}

// ForceLocalRecover 执行签字放行的本地恢复前置：抬全部组的任期、清空
// 投票，并消费掉一次性许可。三件事在同一批次内 Sync 提交。
//
// 参数：
//   - dataGroups: 数据组数；本方法处理组 0（meta 组）到 dataGroups
//
// 为什么必须抬 term：quorum-mem 档掉电可能丢掉投票记录——本节点在任期 T
// 投给过 A，重启后忘了，又在 T 投给 B，于是同一任期出现两个 leader、
// 日志分叉。这比丢数据严重，是损坏。抬任期在 raft 中永远安全（代价只是
// 强制一次重新选举），抬完之后本节点不可能再在 T 投第二次。
//
// 为什么只有这条路径抬：另外三条恢复路径本来就没丢过投票，抬了纯属白白
// 多付一次选举。
//
// 为什么与消费许可同批：两者必须同生共死。若先抬 term 后删许可而中间
// 崩溃，许可会被重复消费；若先删许可后抬 term 而中间崩溃，运维签的字白费
// 且节点仍带着旧任期启动。单批原子提交让这两种半截状态都不存在。
//
// 注意：commit 位点不动——它由日志重放与 leader 重新告知恢复，抬它没有
// 意义且会与真实日志脱节。
func (r *raftStore) ForceLocalRecover(dataGroups uint32) error {
	b := r.st.NewBatch()
	for g := uint32(0); g <= dataGroups; g++ {
		hs := &raftpb.HardState{}
		data, ok, err := r.st.Get(hsKey(g))
		if err != nil {
			b.Close()
			return fmt.Errorf("raftstore ForceLocalRecover 组 %d 读 HardState: %w", g, err)
		}
		if ok {
			if err := proto.Unmarshal(data, hs); err != nil {
				b.Close()
				return fmt.Errorf("raftstore ForceLocalRecover 组 %d 解码 HardState: %w", g, err)
			}
		}
		newTerm := hs.GetTerm() + 1
		var noVote uint64
		hs.Term = &newTerm
		hs.Vote = &noVote
		enc, err := proto.Marshal(hs)
		if err != nil {
			b.Close()
			return fmt.Errorf("raftstore ForceLocalRecover 组 %d 编码 HardState: %w", g, err)
		}
		if err := b.Set(hsKey(g), enc); err != nil {
			b.Close()
			return fmt.Errorf("raftstore ForceLocalRecover 组 %d 写 HardState: %w", g, err)
		}
		r.lg.Error("签字放行本地恢复：任期已抬、投票已清", "g", g, "term", newTerm)
	}
	if err := b.Delete([]byte(recoverPermitKey)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore ForceLocalRecover 删许可: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore ForceLocalRecover: %w", err)
	}
	r.lg.Error("一次性本地恢复许可已消费（本次生效，不再对后续任何一次不干净关机放行）", "groups", dataGroups+1)
	return nil
}
```

在 `raftstore.go` 的 import 块补 `"strings"`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/ -run 'TestRecoverPermit|TestForceLocalRecover' -v`
Expected: 2 条全 PASS

- [ ] **Step 5: 加关键节点日志**

核对 Step 3 已包含，全部 **Error 级**（与 wipe 同规格：破坏性/高风险动作的补偿是日志）：

- 授予许可 → Error，含授予时间与世代，且文案直说「可能丢失已确认的消息」
- 许可格式不合法 → Error，含原始值
- 每组抬 term 成功 → Error，含组号与新任期（**成功路径不静默**：事后审计要能逐组核对）
- 许可被消费 → Error，明说「不再对后续任何一次不干净关机放行」

- [ ] **Step 6: 加注释**

核对 Step 3 已包含：`recoverPermit` 说明「用世代替代 TTL、因而不依赖时钟」；`SaveRecoverPermit` 说明为何用纯文本而非 protobuf；`LoadRecoverPermit` 说明「读不懂的许可绝不当成有效签字」；`ForceLocalRecover` 用三段分别写清「为什么必须抬 term」「为什么只有这条路径抬」「为什么与消费许可同批」，并注明 commit 位点刻意不动。

- [ ] **Step 7: 提交**

```bash
go build ./... && go vet ./...
git add internal/cluster/raftstore.go internal/cluster/raftstore_test.go
git commit -m "$(cat <<'EOF'
feat(cluster): 一次性本地恢复许可 + ForceLocalRecover（抬 term、清投票、消费许可同批）

许可绑定授予时的机器世代而不是 TTL：机器每重启一次世代就变，旧许可自动
失效，因而不依赖时钟——签字只对运维当时看到的那一次事故有效。

抬 term 是这条路径的安全前提：quorum-mem 档掉电可能丢掉投票记录，本节点
在任期 T 投过 A 却忘了、重启后又在 T 投给 B，同一任期两个 leader、日志分叉。
这比丢数据严重，是损坏。抬任期在 raft 里永远安全，代价只是强制一次选举。

抬 term 与消费许可同批提交：分开做的两种崩溃窗口分别导致「许可被重复消费」
和「签的字白费且节点仍带旧任期启动」，单批原子让两者都不存在。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: `NewManager` 接线五分支（含安全门守卫用例）

**Files:**
- Modify: `internal/cluster/manager.go`（`Options` 加字段；`NewManager` 恢复判定段落，现 `manager.go:379-409`）
- Test: `internal/cluster/manager_test.go`

**Interfaces:**
- Consumes: `resolveBootGen`（Task 1）、`LoadBootGen`/`SaveBootGen`（Task 2）、`decideRecovery`/`recoveryInput`/五个路径常量（Task 3）、`LoadRecoverPermit`/`ForceLocalRecover`（Task 4）
- Produces:
  - `Options.BootGen BootGenFunc`（测试注入点，nil 走平台实现）
  - `NewManager` 在 `pathLocalResume` / `pathLocalForced` 下**不再**返回 `ErrUncleanShutdown`，而是以 `clean=true` 语义构造各组

- [ ] **Step 1: 写失败的测试**

追加到 `internal/cluster/manager_test.go`：

```go
// withBootGen 注入固定的机器世代，供恢复分支用例构造「重启过 / 没重启过」。
func withBootGen(gen string) func(*Options) {
	return func(o *Options) { o.BootGen = func() (string, error) { return gen, nil } }
}

// TestUncleanSameBootResumesLocally 世代未变（进程崩溃、机器没重启）时，
// 不干净重启必须能直接以原身份起来——这正是三节点同时 kill -9 后集群
// 能自愈的前提。
func TestUncleanSameBootResumesLocally(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem, withBootGen("gen-a"))
	waitSoloLeader(t, m1)
	m1.kill() // 不写干净关机标记 = 不干净关机
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatalf("关闭 store: %v", err)
	}

	st2 := mustOpenStore(t, dir)
	o := soloOptions(t, st2, dir, AckQuorumMem)
	withBootGen("gen-a")(&o) // 同一次开机
	m2, err := NewManager(o)
	if err != nil {
		t.Fatalf("世代未变的不干净重启应本地恢复，却报错: %v", err)
	}
	m2.kill()
	<-m2.Done()
}

// TestUncleanRebootedMemRequiresPermit mem 档 + 机器重启过 + 无许可：
// 必须仍走 ErrUncleanShutdown（交给重入编排/拒启），不得自作主张恢复。
func TestUncleanRebootedMemRequiresPermit(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem, withBootGen("gen-a"))
	waitSoloLeader(t, m1)
	m1.kill()
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatalf("关闭 store: %v", err)
	}

	st2 := mustOpenStore(t, dir)
	o := soloOptions(t, st2, dir, AckQuorumMem)
	withBootGen("gen-b")(&o) // 机器重启过
	if _, err := NewManager(o); !errors.Is(err, ErrUncleanShutdown) {
		t.Fatalf("NewManager = %v; want ErrUncleanShutdown", err)
	}
}

// TestUncleanRebootedFsyncResumesLocally fsync 档即使机器重启过也能直接
// 本地恢复：条目与 term/vote 每次变更都已随 MustSync 落盘。
func TestUncleanRebootedFsyncResumesLocally(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumFsync, withBootGen("gen-a"))
	waitSoloLeader(t, m1)
	m1.kill()
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatalf("关闭 store: %v", err)
	}

	st2 := mustOpenStore(t, dir)
	o := soloOptions(t, st2, dir, AckQuorumFsync)
	withBootGen("gen-b")(&o)
	m2, err := NewManager(o)
	if err != nil {
		t.Fatalf("fsync 档不干净重启应本地恢复，却报错: %v", err)
	}
	m2.kill()
	<-m2.Done()
}

// TestUncleanShutdownDoesNotWriteBootGen 安全门守卫用例。
//
// 走完拒启分支之后，盘上的机器世代必须**仍是旧值**。若哪天有人把
// SaveBootGen 挪到判定之前「统一处理」，序列就变成：机器重启 → 判定需要
// 签字 → 顺手写了新世代 → 拒启 → 运维重启进程 → 世代已相等 → 自动本地
// 恢复，签字门自己开了。这个缺陷在任何功能测试里都看不出来，只会在某次
// 真实事故中表现为「安全门没起作用」，所以必须有一条用例专门盯着它。
func TestUncleanShutdownDoesNotWriteBootGen(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem, withBootGen("gen-a"))
	waitSoloLeader(t, m1)
	m1.kill()
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatalf("关闭 store: %v", err)
	}

	st2 := mustOpenStore(t, dir)
	o := soloOptions(t, st2, dir, AckQuorumMem)
	withBootGen("gen-b")(&o)
	if _, err := NewManager(o); !errors.Is(err, ErrUncleanShutdown) {
		t.Fatalf("前置不成立，NewManager = %v; want ErrUncleanShutdown", err)
	}
	gen, ok, err := newRaftStore(st2, testSlog(t)).LoadBootGen()
	if err != nil {
		t.Fatalf("LoadBootGen: %v", err)
	}
	if !ok || gen != "gen-a" {
		t.Fatalf("拒启后盘上世代 = (%q, %v); want (\"gen-a\", true)——拒启分支绝不能写世代，"+
			"否则运维一重启进程就会被自动放行，签字门形同虚设", gen, ok)
	}
}

// TestForcedLocalRecoverWithPermit 签字往返：写许可 → 起得来 → term 抬了
// → 许可已被消费（同一份许可不能再放行下一次）。
func TestForcedLocalRecoverWithPermit(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem, withBootGen("gen-a"))
	waitSoloLeader(t, m1)
	m1.kill()
	<-m1.Done()
	rs1 := newRaftStore(st1, testSlog(t))
	beforeHS, _, _, err := rs1.Load(0)
	if err != nil {
		t.Fatalf("读抬 term 前的 HardState: %v", err)
	}
	beforeTerm := beforeHS.GetTerm()
	if err := rs1.SaveRecoverPermit(recoverPermit{GrantedAt: "t", Gen: "gen-b"}); err != nil {
		t.Fatalf("SaveRecoverPermit: %v", err)
	}
	if err := st1.Close(); err != nil {
		t.Fatalf("关闭 store: %v", err)
	}

	st2 := mustOpenStore(t, dir)
	o := soloOptions(t, st2, dir, AckQuorumMem)
	withBootGen("gen-b")(&o)
	m2, err := NewManager(o)
	if err != nil {
		t.Fatalf("有匹配许可时应放行本地恢复，却报错: %v", err)
	}
	rs2 := newRaftStore(st2, testSlog(t))
	afterHS, _, _, err := rs2.Load(0)
	if err != nil {
		t.Fatalf("读抬 term 后的 HardState: %v", err)
	}
	if afterHS.GetTerm() <= beforeTerm {
		t.Fatalf("term = %d，未高于恢复前的 %d——签字放行路径必须抬任期，否则可能在同一任期投第二次票",
			afterHS.GetTerm(), beforeTerm)
	}
	if _, ok, _ := rs2.LoadRecoverPermit(); ok {
		t.Fatal("许可仍在——一次性许可必须在放行时被消费")
	}
	m2.kill()
	<-m2.Done()
}
```

若 `manager_test.go` 中尚无 `waitSoloLeader` 助手，用文件内既有的等待 leader 助手替换该调用（`singleNodeManagerHarness` 附近有等待逻辑）；本步骤不新增助手。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/ -run 'TestUnclean|TestForcedLocalRecover' -v`
Expected: FAIL——`o.BootGen undefined`；且 `TestUncleanSameBootResumesLocally` 报 `ErrUncleanShutdown`（旧行为）

- [ ] **Step 3: 写实现**

在 `internal/cluster/manager.go` 的 `Options` 结构体末尾（`ReadBarrierTimeout` 之后）加入：

```go
	// BootGen 是机器世代的读取函数（测试注入点，nil 走平台实现）。
	// 世代用于判断「本地日志在上次不干净关机后是否仍然完整」——见
	// recovery.go 的判定表与 bootgen.go 的机制说明。
	BootGen BootGenFunc
```

把 `NewManager` 中现有的恢复判定段落（`clean, err := m.rs.ConsumeCleanShutdown()` 起到 `default:` 分支结束）整体替换为：

```go
	clean, err := m.rs.ConsumeCleanShutdown()
	if err != nil {
		return nil, err
	}
	hasRaft, err := m.diskHasRaftState()
	if err != nil {
		return nil, err
	}
	genNow, genNowOK := resolveBootGen(o.BootGen, m.lg)
	genStored, genStoredOK, err := m.rs.LoadBootGen()
	if err != nil {
		return nil, err
	}
	permit, permitOK, err := m.rs.LoadRecoverPermit()
	if err != nil {
		return nil, err
	}
	path, reason := decideRecovery(recoveryInput{
		Clean: clean, HasRaft: hasRaft, Mode: o.Mode,
		GenNow: genNow, GenNowOK: genNowOK,
		GenStored: genStored, GenStoredOK: genStoredOK,
		PermitGen: permit.Gen, PermitOK: permitOK,
	})
	// 判定的全部输入与结论打成一行：重启排障时「为什么走了这条路」
	// 永远不需要猜，也不需要去比对几处分散的日志
	m.lg.Info("恢复路径判定", "recovery", path.String(), "reason", reason,
		"clean", clean, "hasRaft", hasRaft, "mode", o.Mode.String(),
		"bootgenNow", genNow, "bootgenNowOK", genNowOK,
		"bootgenStored", genStored, "bootgenStoredOK", genStoredOK,
		"permit", permitOK)

	// replay 决定各组是否回放本地日志。三条本地恢复路径（干净关机、
	// 世代未变、fsync 档/签字放行）共用同一套回放逻辑——必须复用
	// buildGroup 的 clean 分支，不得另写：那条分支里已有「未完成快照
	// 安装 → 清空该组重来」的处理（见 buildGroup 内 LoadInstalling 段），
	// 另起炉灶会让不干净关机若恰好发生在快照安装中途时，节点带着半截
	// 状态启动并向客户端返回缺失的消息。
	replay := false
	switch path {
	case pathCleanResume, pathLocalResume:
		replay = true
	case pathLocalForced:
		// 抬任期 + 清投票 + 消费许可（同批 Sync）：mem 档掉电可能丢掉
		// 投票记录，不抬任期就可能在同一任期投第二次票、导致日志分叉
		if err := m.rs.ForceLocalRecover(o.DataGroups); err != nil {
			return nil, err
		}
		replay = true
	case pathRejoin:
		m.lg.Error("检测到不干净关机且无法证明本地日志可信，拒绝直接恢复——须清空状态以 learner 重入"+
			"（先 Close store，再 WipeForRejoin，经存活 leader 的 ConfChange 重新加入）；"+
			"若全集群均已硬宕、无人可接纳本节点，可用 `sq recover -config <配置> --grant` 签字放行本地恢复",
			"reason", reason)
		// 此处绝不写 bootgen：写了会让运维重启一次进程就自动放行，
		// 安全门形同虚设（见 SaveBootGen 注释与守卫用例
		// TestUncleanShutdownDoesNotWriteBootGen）
		return nil, ErrUncleanShutdown
	case pathFresh:
		// fresh：本目录一生只经过一次的「首次以集群模式启动」。此刻若
		// store 里已有 FSM 数据，那就是单机档直写进去的存量——它不在
		// 任何 raft 日志里，必须打标记（N2：Join 的拒绝条件要靠它把
		// 「单机升级档」与「全新集群刚写了些数据」区分开，否则后者也被
		// 一刀切拒绝，而它的数据本来就能靠日志重放完整带过去）。
		preRaft, err := storeHasFSMKeys(o.Store)
		if err != nil {
			return nil, fmt.Errorf("cluster: 探测前 raft 期存量数据: %w", err)
		}
		if preRaft {
			if err := m.rs.MarkPreRaft(); err != nil {
				return nil, err
			}
		}
	}

	// 世代只在能启动成功的路径上落盘（拒启分支已在上面 return）。
	// 语义是「本数据目录最后一次被运行中的节点写入，发生在哪个世代」，
	// 顺序写反即安全门失效——理由详见 SaveBootGen 的注释。
	if genNowOK {
		if err := m.rs.SaveBootGen(genNow); err != nil {
			return nil, err
		}
	}
	recovery := path.String()
```

并把随后的 `m.buildGroup(g, clean, peers)` 调用改为 `m.buildGroup(g, replay, peers)`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/ -run 'TestUnclean|TestForcedLocalRecover|TestDecideRecovery|TestBootGen' -v`
Expected: 全 PASS

Run: `go test ./internal/cluster/`
Expected: ok（既有用例不得回归；`TestUncleanRestartRequiresRejoin` 一类旧用例若因新行为失效，按其真实语义改成「mem 档 + 世代变了」的形态，不得直接删除）

- [ ] **Step 5: 加关键节点日志**

核对 Step 3 已包含：

- **恢复路径判定单行 Info**：把 8 个判据与结论一次打全。这是本 task 最重要的一行日志——它让「为什么走了这条路」永远不需要猜。
- `pathRejoin` → Error，且**在错误文案里直接给出 `sq recover` 的完整命令**：运维看到日志的那一刻就知道下一步敲什么，不必去翻文档。
- `pathLocalForced` 的抬 term 与消费许可日志由 Task 4 的 `ForceLocalRecover` 承担，此处不重复打。
- `SaveBootGen` 的成功日志由 Task 2 承担。

- [ ] **Step 6: 加注释**

核对 Step 3 已包含：`Options.BootGen` 字段注释；`replay` 变量上方用一整段写清「为什么必须复用 `buildGroup` 的 clean 分支」并点名半截快照安装的后果；`pathLocalForced` 分支说明抬 term 的理由；`pathRejoin` 分支就地注明「此处绝不写 bootgen」并指向守卫用例；`SaveBootGen` 调用点说明写入时机的语义。

- [ ] **Step 7: 提交**

```bash
go build ./... && go vet ./...
git add internal/cluster/manager.go internal/cluster/manager_test.go
git commit -m "$(cat <<'EOF'
feat(cluster): NewManager 接线五分支恢复——kill -9 之后不再无条件清空数据目录

三节点同时 kill -9 曾经等于集群永久需要人工介入（三个节点各自去找一个不
存在的 leader，谁都起不来），而三份完好的日志就躺在盘上。现在世代未变即
证明页缓存从未丢失，直接以原身份回放本地日志。

两条新路径复用 buildGroup 的 clean 分支而不是另写回放：那条分支里已有
「未完成快照安装 → 清空该组重来」的处理，另起炉灶会让不干净关机若恰好
发生在快照安装中途时，节点带着半截状态启动、向客户端返回缺失的消息。

拒启分支绝不写 bootgen，并配一条守卫用例盯着它：写了的话运维重启一次进程
就会被自动放行，整扇签字门形同虚设——这个缺陷在功能测试里完全看不出来。

拒启日志里直接给出 sq recover 的完整命令，运维看到日志就知道下一步敲什么。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `sq recover` 子命令

**Files:**
- Create: `cmd/sq/recover.go`
- Modify: `cmd/sq/main.go`（`main()` 的子命令分流；头注释重写）

**Interfaces:**
- Consumes: `config.Load`、`store.Open`、`cluster` 包的判定与许可读写
- Produces: `func runRecover(args []string) error`

**前置说明**：`recovery.go` 与许可读写目前是 `cluster` 包内的非导出标识符，本 task 需要在 `cluster` 包中新增一个**导出的**只读入口供 CLI 使用，避免把判定逻辑复制一份到 `cmd/`：

```go
// 追加到 internal/cluster/recovery.go
```

- [ ] **Step 1: 写失败的测试**

追加到 `internal/cluster/recovery_test.go`：

```go
// TestInspectRecoveryReportsPathAndPermitNeed CLI 用的只读入口必须与
// NewManager 得出同一条路径——两处各判一次迟早会出现「命令说你不用签字、
// 进程说你要签字」，那是最伤运维信任的一类分歧。
func TestInspectRecoveryReportsPathAndPermitNeed(t *testing.T) {
	dir := t.TempDir()
	st := mustOpenStore(t, dir)
	rs := newRaftStore(st, testSlog(t))
	if err := rs.EnsureGroups(3); err != nil {
		t.Fatalf("EnsureGroups: %v", err)
	}
	term, vote, commit := uint64(3), uint64(1), uint64(5)
	if err := rs.Persist(0, &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}, nil, true); err != nil {
		t.Fatalf("造 HardState: %v", err)
	}
	if err := rs.SaveBootGen("gen-a"); err != nil {
		t.Fatalf("SaveBootGen: %v", err)
	}

	rep, err := InspectRecovery(st, 3, AckQuorumMem, func() (string, error) { return "gen-b", nil }, testSlog(t))
	if err != nil {
		t.Fatalf("InspectRecovery: %v", err)
	}
	if !rep.NeedsPermit {
		t.Fatal("mem 档 + 世代变了 + 无许可，NeedsPermit 应为 true")
	}
	if rep.Reason == "" {
		t.Fatal("Reason 为空——它是报告要打给运维看的主体")
	}
	if len(rep.Groups) != 4 {
		t.Fatalf("Groups 长度 = %d; want 4（组 0..3）", len(rep.Groups))
	}

	// 世代未变时不需要签字，命令必须明说，而不是闷头写一个永不被消费的许可
	rep2, err := InspectRecovery(st, 3, AckQuorumMem, func() (string, error) { return "gen-a", nil }, testSlog(t))
	if err != nil {
		t.Fatalf("InspectRecovery: %v", err)
	}
	if rep2.NeedsPermit {
		t.Fatal("世代未变时 NeedsPermit 应为 false")
	}
}
```

注意：`InspectRecovery` 不消费干净关机标记（那是 `NewManager` 的副作用），它只读。测试里没写干净关机标记，因此 `Clean=false`。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/ -run 'TestInspectRecovery' -v`
Expected: FAIL，`undefined: InspectRecovery`

- [ ] **Step 3: 写实现（先补 cluster 侧的只读入口）**

追加到 `internal/cluster/recovery.go`：

```go
// GroupReport 是单个 raft 组在恢复报告中的现场数据。
type GroupReport struct {
	Group     uint32 // 组号（0 为 meta 组）
	Applied   uint64 // 已应用位点
	LastIndex uint64 // 日志尾 index（无条目时为 0）
	LastTerm  uint64 // 日志尾 term（无条目时为 0）
	SnapIndex uint64 // 快照锚点 index（从未截断时为 0）
	Term      uint64 // 当前任期
	Commit    uint64 // 提交位点
}

// RecoveryReport 是 sq recover 打给运维看的完整报告。
type RecoveryReport struct {
	Path        string        // 恢复路径短名
	Reason      string        // 中文理由
	NeedsPermit bool          // 是否需要运维签字才能本地恢复
	GenNow      string        // 当前机器世代（读不到时为空）
	GenNowOK    bool          // 当前世代是否可用
	GenStored   string        // 盘上记录的机器世代
	GenStoredOK bool          // 盘上是否记录过
	Mode        AckMode       // 确认档位
	HasPermit   bool          // 是否已存在许可
	PermitGen   string        // 已存在许可绑定的世代
	Groups      []GroupReport // 各组现场
}

// InspectRecovery 只读地采集恢复判据与各组现场，供 sq recover 渲染报告。
//
// 参数：
//   - st: 已打开的 store（调用方负责开关）
//   - dataGroups: 数据组数（组 0..dataGroups 全部纳入报告）
//   - mode: 确认档位（来自配置）
//   - bootGen: 机器世代读取函数（nil 走平台实现）
//   - lg: 日志器
//
// 返回：报告与错误。
//
// 注意：本函数**只读**——不消费干净关机标记、不写机器世代、不动任何键。
// 判定复用 decideRecovery，与 NewManager 同源，因此命令与进程给出的结论
// 恒等一致（见本文件头注释）。
func InspectRecovery(st *store.Store, dataGroups uint32, mode AckMode, bootGen BootGenFunc, lg *slog.Logger) (RecoveryReport, error) {
	rs := newRaftStore(st, lg)
	// 只探标记在不在，不消费它——消费是 NewManager 的副作用，命令不能替它做
	_, hasClean, err := st.Get([]byte(cleanShutdownKey))
	if err != nil {
		return RecoveryReport{}, fmt.Errorf("cluster: 读干净关机标记: %w", err)
	}
	genNow, genNowOK := resolveBootGen(bootGen, lg)
	genStored, genStoredOK, err := rs.LoadBootGen()
	if err != nil {
		return RecoveryReport{}, err
	}
	permit, permitOK, err := rs.LoadRecoverPermit()
	if err != nil {
		return RecoveryReport{}, err
	}

	hasRaft := false
	groups := make([]GroupReport, 0, dataGroups+1)
	for g := uint32(0); g <= dataGroups; g++ {
		hs, ents, snapMeta, err := rs.Load(g)
		if err != nil {
			return RecoveryReport{}, err
		}
		applied, err := rs.Applied(g)
		if err != nil {
			return RecoveryReport{}, err
		}
		gr := GroupReport{
			Group: g, Applied: applied,
			SnapIndex: snapMeta.GetIndex(),
			Term:      hs.GetTerm(), Commit: hs.GetCommit(),
		}
		if n := len(ents); n > 0 {
			gr.LastIndex = ents[n-1].GetIndex()
			gr.LastTerm = ents[n-1].GetTerm()
		}
		if applied != 0 || !raft.IsEmptyHardState(*hs) {
			hasRaft = true
		}
		groups = append(groups, gr)
	}

	path, reason := decideRecovery(recoveryInput{
		Clean: hasClean, HasRaft: hasRaft, Mode: mode,
		GenNow: genNow, GenNowOK: genNowOK,
		GenStored: genStored, GenStoredOK: genStoredOK,
		PermitGen: permit.Gen, PermitOK: permitOK,
	})
	return RecoveryReport{
		Path: path.String(), Reason: reason,
		NeedsPermit: path == pathRejoin,
		GenNow:      genNow, GenNowOK: genNowOK,
		GenStored: genStored, GenStoredOK: genStoredOK,
		Mode:      mode,
		HasPermit: permitOK, PermitGen: permit.Gen,
		Groups: groups,
	}, nil
}

// GrantRecoverPermit 写入一次性本地恢复许可，绑定当前机器世代。
//
// 参数：
//   - st: 已打开的 store
//   - now: 授予时间（调用方传入，便于测试固定时钟）
//   - bootGen: 机器世代读取函数（nil 走平台实现）
//   - lg: 日志器
//
// 世代读不到时拒绝授予：许可靠世代作废，没有世代的许可等于一张永不过期
// 的通行证，比不给还危险。
func GrantRecoverPermit(st *store.Store, now time.Time, bootGen BootGenFunc, lg *slog.Logger) error {
	gen, ok := resolveBootGen(bootGen, lg)
	if !ok {
		return errors.New("cluster: 读不到本机机器世代，拒绝授予本地恢复许可——" +
			"许可靠世代作废，没有世代的许可是一张永不过期的通行证")
	}
	rs := newRaftStore(st, lg)
	return rs.SaveRecoverPermit(recoverPermit{GrantedAt: now.Format(time.RFC3339), Gen: gen})
}
```

`recovery.go` 的 import 补：`"errors"`、`"fmt"`、`"log/slog"`、`"time"`、`"go.etcd.io/raft/v3"`、`"github.com/xushixin/sq/internal/store"`。

新建 `cmd/sq/recover.go`：

```go
// recover.go 实现 `sq recover` 子命令：不干净关机后的现场报告与运维签字。
//
// 职责：
//   - 只读地报告本节点的恢复判定与各组现场，让运维在签字前看得见代价
//   - 在 --grant 时写入一次性本地恢复许可
//
// 边界：
//   - 不启动 broker、不碰网络、不做任何恢复动作——恢复由下次正常启动完成
//   - 判定不自己实现，一律走 cluster.InspectRecovery（与进程同源，
//     否则会出现「命令说你不用签字、进程说你要签字」）
//
// 为什么要有这个命令：quorum-mem 档下机器真掉电后，本地日志尾可能残缺，
// 带着它恢复可能静默丢掉已确认的消息。这个决定必须由人做，而人做决定
// 之前得先看见数字。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/xushixin/sq/internal/cluster"
	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/store"
)

// runRecover 执行 `sq recover` 子命令。
//
// 参数：
//   - args: `recover` 之后的全部参数
//
// 返回：错误（nil 表示成功；调用方据此决定退出码）
func runRecover(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "配置文件路径")
	grant := fs.Bool("grant", false, "写入一次性本地恢复许可（默认只报告不写入）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	config.SetupSlog(cfg.LogLevel)
	logger := slog.Default()
	if !cfg.ClusterEnabled() {
		return fmt.Errorf("本配置未启用集群档，不存在不干净关机的重入编排问题，无需 recover")
	}

	// Pebble 独占锁天然把「服务运行中误签」这条路堵死：broker 还在跑时
	// Open 必然失败。这是白送的一道互斥，只需把提示写清楚。
	st, err := store.Open(cfg.DataDir, cfg.Fsync == "sync", logger)
	if err != nil {
		return fmt.Errorf("打开数据目录 %s 失败（若本节点 broker 仍在运行，请先停止它再执行本命令）: %w", cfg.DataDir, err)
	}
	defer st.Close()

	mode := cluster.AckQuorumMem
	if cfg.Cluster.Ack == "quorum-fsync" {
		mode = cluster.AckQuorumFsync
	}
	rep, err := cluster.InspectRecovery(st, cfg.Cluster.DataGroups, mode, nil, logger)
	if err != nil {
		return err
	}
	printRecoveryReport(os.Stdout, rep)

	if !*grant {
		return nil
	}
	if !rep.NeedsPermit {
		fmt.Fprintln(os.Stdout, "\n本节点不需要许可，直接启动即可自动恢复——未写入任何内容。")
		return nil
	}
	if err := cluster.GrantRecoverPermit(st, time.Now(), nil, logger); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "\n已写入一次性本地恢复许可。现在启动本节点即可带着本地日志恢复；"+
		"该许可只对本次事故有效，机器再重启一次即自动失效。")
	return nil
}

// printRecoveryReport 渲染报告：先结论、再判据、最后各组现场。
//
// 顺序是刻意的——运维最需要的是「我要不要签字、签了可能丢什么」，
// 各组的 index 是给需要深挖的人看的素材，放最后。
func printRecoveryReport(w *os.File, rep cluster.RecoveryReport) {
	fmt.Fprintf(w, "恢复判定：%s\n", rep.Path)
	fmt.Fprintf(w, "  理由：%s\n", rep.Reason)
	if rep.NeedsPermit {
		fmt.Fprintf(w, "  结论：**需要运维签字**。quorum-mem 档的后台刷盘周期为 200ms，\n"+
			"        带着本地日志恢复最多可能丢失最后 200ms 内已确认的写入。\n"+
			"        确认接受后执行：sq recover -config <配置> --grant\n")
	} else {
		fmt.Fprintf(w, "  结论：无需许可，直接启动本节点即可。\n")
	}
	fmt.Fprintf(w, "\n判据：\n")
	fmt.Fprintf(w, "  确认档位     : %s\n", rep.Mode.String())
	fmt.Fprintf(w, "  当前机器世代 : %s\n", genText(rep.GenNow, rep.GenNowOK))
	fmt.Fprintf(w, "  盘上机器世代 : %s\n", genText(rep.GenStored, rep.GenStoredOK))
	if rep.HasPermit {
		fmt.Fprintf(w, "  已有许可     : 是（绑定世代 %s）\n", rep.PermitGen)
	} else {
		fmt.Fprintf(w, "  已有许可     : 否\n")
	}
	fmt.Fprintf(w, "\n各组现场：\n")
	fmt.Fprintf(w, "  %-4s %-10s %-10s %-9s %-10s %-6s %-8s\n", "组", "applied", "日志尾", "尾 term", "快照锚点", "任期", "commit")
	for _, g := range rep.Groups {
		fmt.Fprintf(w, "  %-4d %-10d %-10d %-9d %-10d %-6d %-8d\n",
			g.Group, g.Applied, g.LastIndex, g.LastTerm, g.SnapIndex, g.Term, g.Commit)
	}
}

// genText 把「世代 + 是否可用」渲染成一句人话。
func genText(v string, ok bool) string {
	if !ok {
		return "（读不到——按机器可能已重启保守处理）"
	}
	return v
}
```

在 `cmd/sq/main.go` 的 `main()` 中，`run()` 调用之前加入子命令分流（保持 `sq -config x.yaml` 一字不变）：

```go
	// 子命令分流：只认第一个位置参数，其余一律走原有的 run()。
	// 这样 `sq -config x.yaml` 与既有 systemd 单元完全不受影响。
	if len(os.Args) > 1 && os.Args[1] == "recover" {
		if err := runRecover(os.Args[2:]); err != nil {
			slog.Error("recover 失败", "err", err)
			os.Exit(1)
		}
		return
	}
```

同时把 `cmd/sq/main.go` 头注释中这三行：

```
//   - 不干净关机（ErrUncleanShutdown）的无人值守自愈：打日志留痕后
//     cluster.Rejoin 清空数据目录以 learner 重入——拒启等人工介入违背
//     高可用初衷，破坏性动作的补偿是日志
```

替换为：

```
//   - 不干净关机的分级恢复（B10 + B11）：能证明本地日志完好时（机器世代
//     未变，或 quorum-fsync 档）直接以原身份从本地日志恢复；证明不了时
//     走 cluster.Rejoin——**先求集群接纳、拿到接纳才清空数据目录**，
//     求不到接纳则数据分毫不动、进程拒启，并在日志里给出
//     `sq recover --grant` 的签字出口
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/ -run 'TestInspectRecovery' -v`
Expected: PASS

Run: `go build ./... && go vet ./...`
Expected: 无输出

手工验证报告渲染（用一个不存在集群状态的空目录，确认命令不 panic 且输出可读）。
**临时目录必须落在仓库内**（`.smoke/` 跑完即删，不提交），理由见全局约束：

```bash
mkdir -p .smoke/data && printf 'data_dir: .smoke/data\ncluster:\n  enabled: true\n  node_id: 1\n  peers:\n    1: 127.0.0.1:9081\n' > .smoke/sq.yaml
go run ./cmd/sq recover -config .smoke/sq.yaml
rm -rf .smoke
```
Expected: 打印「恢复判定：fresh」与四行组现场，退出码 0

- [ ] **Step 5: 加关键节点日志**

- 授予许可的 Error 日志由 Task 4 的 `SaveRecoverPermit` 承担，不重复。
- `runRecover` 的失败路径经 `main()` 的 `slog.Error("recover 失败", "err", err)` 落日志。
- 报告本身走 stdout 而非 slog：它是**给人看的命令输出**，不是事件。事件（授予）已经在 slog 里，事后审计看 broker/命令日志即可。这条区分要写进 `recover.go` 的文件头注释。

- [ ] **Step 6: 加注释**

核对 Step 3 已包含：`recover.go` 文件头写清职责、边界与「为什么要有这个命令」；`runRecover`、`printRecoveryReport`、`genText` 各有 doc 注释；Pebble 独占锁那道互斥有就地注释；`printRecoveryReport` 说明字段顺序为何是「先结论后素材」；`InspectRecovery` 注明「只读」；`GrantRecoverPermit` 说明为何世代读不到时拒绝授予；`main.go` 子命令分流处注明「不破坏既有部署」。

- [ ] **Step 7: 提交**

```bash
go build ./... && go vet ./...
git add cmd/sq/recover.go cmd/sq/main.go internal/cluster/recovery.go internal/cluster/recovery_test.go
git commit -m "$(cat <<'EOF'
feat(cmd): sq recover——不干净关机后的现场报告与一次性签字出口

quorum-mem 档下机器真掉电后，本地日志尾可能残缺，带着它恢复可能静默丢掉
已确认的消息。这个决定必须由人做，而人做决定之前得先看见数字：命令默认
只报告不写入，把判据（档位、两侧机器世代）与各组现场（applied、日志尾、
快照锚点、任期、commit）摆出来，并直说「最多可能丢最后 200ms」。

判定不在 cmd 里重写一份，走 cluster.InspectRecovery 与 NewManager 同源——
两处各判一次迟早出现「命令说你不用签字、进程说你要签字」。

世代读不到时拒绝授予许可：许可靠世代作废，没有世代的许可是一张永不过期的
通行证，比不给还危险。

子命令分流只认第一个位置参数，`sq -config x.yaml` 与既有 systemd 单元一字
不受影响。broker 运行中误签这条路由 Pebble 独占锁物理堵死。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6b: mem 档 `local-resume` 也抬 term（实现期订正）

> **这个 task 是 Task 1–6 完成后新增的**，起因是 Task 7 的 e2e 用例实测推翻了原设计的一条前提。改动的是 Task 4 与 Task 5 已提交的代码，不是重做它们。背景见 spec §2.2 与 §3.3 的两段「订正留痕」。

**为什么**：原设计只让 `local-forced` 抬 term，理由是「世代未变 ⇒ 页缓存完好 ⇒ 什么都没丢」。这个理由是错的——Pebble 的 `NoSync` 提交返回时，数据可能还在进程内的 WAL 缓冲里（`WriteRecord` 只 `f.ready.Signal()` 就返回，真正的 `write(2)` 由 flusher goroutine 异步做）。而 mem 档下 HardState（含 Vote）走的正是这条路径。于是 `local-resume` 可能带着一张「投过但没落盘」的选票以原身份复活 → 同一任期投第二次 → 两个 leader → 日志分叉。

**这个洞是 B11 新开的**，不是既有缺陷：B11 之前不干净的节点一律清空重入，带的是全新状态，无从双投票。所以抬 term 是新路径必须自带的安全前置，不是「顺手补个老问题」。

**Files:**
- Modify: `internal/cluster/recovery.go`（新增 `needsTermBump`）
- Modify: `internal/cluster/raftstore.go`（抽出共用的抬 term 批次逻辑，新增 `BumpTermsForLocalResume`）
- Modify: `internal/cluster/manager.go`（`pathLocalResume` 分支按档位决定是否抬）
- Test: `internal/cluster/recovery_test.go`、`internal/cluster/raftstore_test.go`、`internal/cluster/manager_test.go`

**Interfaces:**
- Produces:
  - `func needsTermBump(p recoveryPath, mode AckMode) bool`
  - `func (r *raftStore) BumpTermsForLocalResume(dataGroups uint32) error`（抬 term + 清 vote，**不**碰许可键）
- Changes: `ForceLocalRecover` 内部改为复用同一段抬 term 逻辑，对外签名与语义不变（抬 term + 消费许可，同批 Sync）

- [ ] **Step 1: 写失败的测试**

追加到 `internal/cluster/recovery_test.go`：

```go
// TestNeedsTermBump 抬 term 的适用范围——按 spec §3.3 的表逐格覆盖。
//
// 判据是「投票记录是不是同步落盘的」，不是「机器有没有重启」：
// fsync 档跟随 MustSync，term/vote 每次变更都已 fsync，投票不可能丢；
// mem 档走 NoSync 异步路径，commit 返回时可能还在进程内缓冲。
func TestNeedsTermBump(t *testing.T) {
	cases := []struct {
		path recoveryPath
		mode AckMode
		want bool
	}{
		{pathLocalResume, AckQuorumMem, true},
		{pathLocalResume, AckQuorumFsync, false},
		{pathLocalForced, AckQuorumMem, true},
		{pathCleanResume, AckQuorumMem, false},
		{pathCleanResume, AckQuorumFsync, false},
		{pathFresh, AckQuorumMem, false},
		{pathRejoin, AckQuorumMem, false},
	}
	for _, c := range cases {
		if got := needsTermBump(c.path, c.mode); got != c.want {
			t.Fatalf("needsTermBump(%v, %v) = %v; want %v", c.path, c.mode, got, c.want)
		}
	}
}
```

追加到 `internal/cluster/manager_test.go`：

```go
// TestUncleanSameBootMemBumpsTerm mem 档下世代未变的本地恢复也必须抬 term。
//
// 不抬的后果不是丢数据而是损坏：本节点可能在任期 T 投过票但没落盘，
// 重启后又在 T 投第二次，同一任期两个 leader、日志分叉。
func TestUncleanSameBootMemBumpsTerm(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem, withBootGen("gen-a"))
	waitSoloLeader(t, m1)
	m1.kill()
	<-m1.Done()
	before, _, _, err := newRaftStore(st1, testSlog(t)).Load(0)
	if err != nil {
		t.Fatalf("读恢复前 HardState: %v", err)
	}
	beforeTerm := before.GetTerm()
	if err := st1.Close(); err != nil {
		t.Fatalf("关闭 store: %v", err)
	}

	st2 := mustOpenStore(t, dir)
	o := soloOptions(t, st2, dir, AckQuorumMem)
	withBootGen("gen-a")(&o)
	m2, err := NewManager(o)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	after, _, _, err := newRaftStore(st2, testSlog(t)).Load(0)
	if err != nil {
		t.Fatalf("读恢复后 HardState: %v", err)
	}
	if after.GetTerm() <= beforeTerm {
		t.Fatalf("term = %d，未高于恢复前的 %d——mem 档 local-resume 必须抬任期", after.GetTerm(), beforeTerm)
	}
	if after.GetVote() != 0 {
		t.Fatalf("vote = %d; want 0——可能丢失的投票记录必须清空", after.GetVote())
	}
	m2.kill()
	<-m2.Done()
}

// TestUncleanFsyncLocalResumeKeepsTerm fsync 档不抬 term：投票每次变更都已
// fsync，抬了纯属白白多付一次选举，抬了也说明实现把判据搞错了。
func TestUncleanFsyncLocalResumeKeepsTerm(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumFsync, withBootGen("gen-a"))
	waitSoloLeader(t, m1)
	m1.kill()
	<-m1.Done()
	before, _, _, err := newRaftStore(st1, testSlog(t)).Load(0)
	if err != nil {
		t.Fatalf("读恢复前 HardState: %v", err)
	}
	beforeTerm := before.GetTerm()
	if err := st1.Close(); err != nil {
		t.Fatalf("关闭 store: %v", err)
	}

	st2 := mustOpenStore(t, dir)
	o := soloOptions(t, st2, dir, AckQuorumFsync)
	withBootGen("gen-b")(&o) // 机器重启过，走 fsync 档的 local-resume
	m2, err := NewManager(o)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	after, _, _, err := newRaftStore(st2, testSlog(t)).Load(0)
	if err != nil {
		t.Fatalf("读恢复后 HardState: %v", err)
	}
	if after.GetTerm() != beforeTerm {
		t.Fatalf("term 从 %d 变成了 %d——fsync 档不该抬任期", beforeTerm, after.GetTerm())
	}
	m2.kill()
	<-m2.Done()
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/cluster/ -run 'TestNeedsTermBump|TestUncleanSameBootMemBumpsTerm|TestUncleanFsyncLocalResumeKeepsTerm' -v`
Expected: FAIL——`undefined: needsTermBump`；`TestUncleanSameBootMemBumpsTerm` 报 term 未变（旧行为）

- [ ] **Step 3: 写实现**

`internal/cluster/recovery.go` 追加：

```go
// needsTermBump 判定某条恢复路径是否必须在回放前抬任期、清投票。
//
// 判据是**投票记录是不是同步落盘的**，不是「机器有没有重启」：
//   - fsync 档：syncPersist 跟随 raft.MustSync，term/vote 每次变更都已 fsync，
//     投票不可能丢，抬了纯属白白多付一次选举
//   - mem 档：HardState 走 Pebble 的 NoSync——commit 返回时数据可能还在
//     进程内的 WAL 缓冲里（write(2) 由 flusher goroutine 异步执行），
//     所以任何一次不干净关机后都可能少一张已投出的票
//
// 少一张票的后果是**损坏**而非丢数据：同一任期投第二次 → 两个 leader →
// 日志分叉。抬任期在 raft 中永远安全，代价只是强制一次重新选举。
//
// clean-resume 不需要：MarkCleanShutdown 是 Sync 写，会把此前所有 NoSync
// 写一并刷盘。fresh 无历史。rejoin 会清空状态重入，更无从谈起。
func needsTermBump(p recoveryPath, mode AckMode) bool {
	switch p {
	case pathLocalForced:
		return true
	case pathLocalResume:
		return mode == AckQuorumMem
	default:
		return false
	}
}
```

`internal/cluster/raftstore.go`：把 `ForceLocalRecover` 里逐组抬 term 的循环抽成共用函数，并新增只抬不消费许可的入口：

```go
// bumpTermsInto 把组 0..dataGroups 每组 HardState 的 Term 加 1、Vote 清空，
// 写进给定批次（不提交——提交时机由调用方决定，这样抬 term 才能与消费许可
// 同批原子落盘）。
//
// commit 位点刻意不动：它由日志重放与 leader 重新告知恢复，抬它没有意义
// 且会与真实日志脱节。
func (r *raftStore) bumpTermsInto(b *store.Batch, dataGroups uint32) error {
	for g := uint32(0); g <= dataGroups; g++ {
		hs := &raftpb.HardState{}
		data, ok, err := r.st.Get(hsKey(g))
		if err != nil {
			return fmt.Errorf("组 %d 读 HardState: %w", g, err)
		}
		if ok {
			if err := proto.Unmarshal(data, hs); err != nil {
				return fmt.Errorf("组 %d 解码 HardState: %w", g, err)
			}
		}
		newTerm := hs.GetTerm() + 1
		var noVote uint64
		hs.Term = &newTerm
		hs.Vote = &noVote
		enc, err := proto.Marshal(hs)
		if err != nil {
			return fmt.Errorf("组 %d 编码 HardState: %w", g, err)
		}
		if err := b.Set(hsKey(g), enc); err != nil {
			return fmt.Errorf("组 %d 写 HardState: %w", g, err)
		}
		r.lg.Error("不干净关机后本地恢复：任期已抬、投票已清", "g", g, "term", newTerm)
	}
	return nil
}

// BumpTermsForLocalResume 为 local-resume 抬任期、清投票并 Sync 落盘。
//
// 与 ForceLocalRecover 的区别只有一个：本方法**不碰许可键**。local-resume
// 本来就不需要运维签字（它要么世代未变、要么是 fsync 档），只是 mem 档下
// 投票记录可能没落盘，所以同样要抬任期。见 needsTermBump 的注释。
func (r *raftStore) BumpTermsForLocalResume(dataGroups uint32) error {
	b := r.st.NewBatch()
	if err := r.bumpTermsInto(b, dataGroups); err != nil {
		b.Close()
		return fmt.Errorf("raftstore BumpTermsForLocalResume: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore BumpTermsForLocalResume: %w", err)
	}
	return nil
}
```

`ForceLocalRecover` 改为复用 `bumpTermsInto`，其余不变（仍在同一批次里 `b.Delete(recoverPermitKey)` 后 Sync 提交）。**同时修掉它 doc 注释里那句「为什么只有这条路径抬」**——那个说法已经不成立了，改为指向 `needsTermBump`。

`internal/cluster/manager.go` 的恢复分支改为：

```go
	replay := false
	switch path {
	case pathCleanResume, pathLocalResume:
		replay = true
	case pathLocalForced:
		// 抬任期 + 清投票 + 消费许可，同批 Sync
		if err := m.rs.ForceLocalRecover(o.DataGroups); err != nil {
			return nil, err
		}
		replay = true
	...
	}
	// local-resume 在 mem 档下同样要抬任期：投票记录走 NoSync 异步路径，
	// 可能没落盘（见 needsTermBump）。local-forced 已在上面抬过，不重复。
	if path == pathLocalResume && needsTermBump(path, o.Mode) {
		if err := m.rs.BumpTermsForLocalResume(o.DataGroups); err != nil {
			return nil, err
		}
	}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/cluster/ -run 'TestNeedsTermBump|TestUnclean|TestForcedLocalRecover' -v`
Expected: 全 PASS

Run: `go test ./internal/cluster/`
Expected: ok（既有用例不得回归）

- [ ] **Step 5: 加关键节点日志**

- 逐组抬 term 的 Error 日志已在 `bumpTermsInto` 内（沿用 Task 4 的规格，文案改为对两条路径都成立的「不干净关机后本地恢复」）
- 恢复路径判定的那行 Info 日志已包含 `mode`，与本 task 的分支判据一致，无需新增
- 不为 `needsTermBump` 打日志：它是纯函数，且结论已随抬 term 的 Error 日志体现

- [ ] **Step 6: 加注释**

核对 Step 3 已包含：`needsTermBump` 用一整段写清「判据是投票是否同步落盘，不是机器有没有重启」以及 Pebble NoSync 的异步性；`bumpTermsInto` 说明为何不提交、commit 位点为何不动；`BumpTermsForLocalResume` 说明它与 `ForceLocalRecover` 的唯一区别；`ForceLocalRecover` 里那句过时的「只有这条路径抬」必须改掉；`manager.go` 调用点说明为何 `local-forced` 不重复抬。

- [ ] **Step 7: 提交**

```bash
go build ./... && go vet ./...
git add internal/cluster/recovery.go internal/cluster/raftstore.go internal/cluster/manager.go internal/cluster/recovery_test.go internal/cluster/raftstore_test.go internal/cluster/manager_test.go
git commit -m "$(cat <<'EOF'
fix(cluster): mem 档 local-resume 也抬 term——NoSync 是异步的，投票可能没落盘

原设计只让 local-forced 抬 term，理由是「世代未变 ⇒ 页缓存完好 ⇒ 什么都
没丢」。这个理由是错的：Pebble 的 NoSync 提交返回时数据可能还在进程内的
WAL 缓冲里（WriteRecord 只 Signal 一下就返回，write(2) 由 flusher goroutine
异步做），而 mem 档下 HardState 含 Vote 走的正是这条路径。

于是 local-resume 可能带着一张「投过但没落盘」的选票以原身份复活，在同一
任期投第二次 → 两个 leader → 日志分叉。这个洞是 B11 新开的：此前不干净的
节点一律清空重入，带的是全新状态，无从双投票。

判据因此不是「机器有没有重启」而是「投票记录是不是同步落盘的」——fsync 档
跟随 MustSync，投票不可能丢，抬了白付一次选举；mem 档一律要抬。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: e2e 场景用例

**Files:**
- Modify: `test/e2e/cluster_proc_test.go`（`spawn` 支持注入环境变量；新增 `restartAll`）
- Modify: `test/e2e/cluster_scenario_test.go`（改写 1 条、新增 3 条）

**Interfaces:**
- Consumes: `SQ_BOOTGEN_OVERRIDE`（Task 1）、`sq recover --grant`（Task 6）
- Produces: 无（测试是终点）

- [ ] **Step 1: 给 harness 加环境变量注入**

在 `test/e2e/cluster_proc_test.go` 的 `procCluster` 结构体中加入：

```go
	// env 是逐节点附加的环境变量（形如 "K=V"），随进程重启保持。
	// 场景用例用它注入 SQ_BOOTGEN_OVERRIDE 来模拟「机器重启过」——
	// 进程级 e2e 起的是真 broker 进程，注不进 Go 函数，只能走环境变量。
	env [][]string
```

在 `startProcCluster` 中初始化 `pc.env = make([][]string, n)`；在 `spawn` 的 `cmd := exec.Command(...)` 之后加入：

```go
	if len(pc.env[i]) > 0 {
		cmd.Env = append(os.Environ(), pc.env[i]...)
	}
```

并新增方法：

```go
// setEnv 给第 i 个节点追加环境变量，下次 spawn 生效（重启后保持）。
func (pc *procCluster) setEnv(i int, kv ...string) {
	pc.env[i] = append(pc.env[i], kv...)
}
```

同一文件再加一个整批重启助手——**全集群停机后不能逐台 `restart`**：

```go
// restartAll 整批原地重启全部节点：先全部起进程、再逐个等就绪。
//
// 与 restart 的区别：全集群停机（进程全崩／真掉电后上电）后**必须先把进程
// 全起来、再逐个等就绪**——ready 判定依赖 waitMetaLeader，而 raft 选举要
// quorum；首个节点起来时是 1 of 3，选不出 leader，gRPC 监听不会出现。逐台
// restart 会在第一个节点上等满 brokerStartTimeout 然后 Fatal（用例红在
// harness 上，和被测行为无关）。restartAll 让三个进程几乎同时起，第二、三
// 个节点一就绪多数派立刻成型、选举随即收敛。与 startProcCluster 的装配序
// 是同一个道理（见该函数注释）。
func (pc *procCluster) restartAll(t *testing.T) {
	t.Helper()
	for i := range pc.handles {
		if pc.handles[i].cmd.Process != nil {
			t.Fatalf("节点 %d 仍在运行，restartAll 前必须先 kill/stopGraceful", i+1)
		}
	}
	for i := range pc.handles {
		ep := pc.handles[i].endpoint
		pc.handles[i] = pc.spawn(t, i, ep)
	}
	for i := range pc.handles {
		waitBrokerReady(t, pc.handles[i].endpoint, pc.handles[i].waitDone, pc.handles[i].logPath)
		t.Logf("节点 %d 已重启就绪 pid=%d", i+1, pc.handles[i].cmd.Process.Pid)
	}
}
```

顺带给既有的 `restart` doc 注释补一句适用范围，免得下一个人再踩：

```go
// 只适用于「多数派仍存活」的单点重启：ready 判定依赖 waitMetaLeader 先过，
// 而选主要 quorum——全集群停机后的整批重启请用 restartAll，逐台 restart 会
// 在第一个节点上死锁（详见 restartAll 注释）。
```

- [ ] **Step 2: 改写 `TestScenarioFullPowerLossCannotRecover`**

该用例断言「三节点同时 SIGKILL 后全部拒启」，而 SIGKILL 不重启机器、世代不变，改动后三节点会各自本地恢复。整条替换为：

```go
// TestScenarioFullProcessCrashRecoversLocally 三节点同时 SIGKILL 后重启，
// 断言集群**自愈**：三节点各自以原身份从本地日志恢复、重新选出 leader、
// 恢复可写，且崩溃前的确认集一条不丢。
//
// 这条用例取代了旧的 TestScenarioFullPowerLossCannotRecover。旧用例把
// 「三节点全崩 = 集群永久需要人工介入」固化成了期望行为，而那个行为的
// 前提是错的：SIGKILL 杀的是进程，机器没重启，页缓存还在，三份日志基本
// 完好——它们只是被一条过度保守的规则拦在门外（B11）。
//
// 真掉电（页缓存丢失）的形态由 TestScenarioRebootedMemNeedsPermit 与
// TestScenarioRebootedMemRecoversAfterGrant 覆盖，B10 的顺序红线断言在
// 前者中继续生效。
//
// **kill 前必须静置**：Pebble 的 NoSync 提交返回时，数据可能还在进程内的
// WAL 缓冲里（flusher goroutine 异步写出），所以紧贴 SIGKILL 发出的那一批
// 已确认消息本来就会丢——断言它们不丢等于断言系统从来不具备的性质，用例
// 会真红且红得没有意义。静置 500ms（> flusher() 的 200ms 周期 + 余量）之后
// 全部已确认写入都已随周期 fsync 落盘，零丢失才是可断言的。
func TestScenarioFullProcessCrashRecoversLocally(t *testing.T) {
	pc := startProcCluster(t, 3)
	ledger := sendAndTrack(t, pc.multi(), 120)

	// 让 200ms 周期 fsync 至少跑满一轮，把上面这批确认集变成可断言的
	time.Sleep(500 * time.Millisecond)

	for i := range pc.handles {
		pc.kill(t, i)
	}
	pc.restartAll(t)

	// 集群恢复可写 = 选主成功 = 三份本地日志都被接纳
	ledger.append(t, sendAndTrack(t, pc.multi(), 60))
	ledger.assertNoLoss(t, pc.multi())
	for i := range pc.handles {
		if n := countLogLines(t, []string{pc.handles[i].logPath}, "local-resume"); n == 0 {
			t.Fatalf("节点 %d 未走本地恢复路径——世代未变时不该再清空数据目录重入", i+1)
		}
		if n := countLogLines(t, []string{pc.handles[i].logPath}, "状态目录已清空"); n != 0 {
			t.Fatalf("节点 %d 清空了数据目录（%d 次）——进程崩溃而机器未重启时，本地日志是完整的，清空是纯粹的浪费与风险", i+1, n)
		}
	}
}
```

若 `sendAndTrack` / `ledger.append` / `assertNoLoss` 与本仓既有对账器命名不符，改用 `cluster_scenario_test.go` 中既有的确认集对账助手（同文件其余用例使用的那套），语义一致即可：Send 成功即登记、断言全量被消费、允许重复不允许丢失。

- [ ] **Step 3: 新增「机器重启过 + mem 档 + 无许可」用例（B10 红线搬家）**

追加到 `test/e2e/cluster_scenario_test.go`：

```go
// TestScenarioRebootedMemNeedsPermit 模拟真掉电：三节点 SIGKILL 后把机器
// 世代改成新值再重启，断言三节点**拒启且数据目录保留**。
//
// 这里保管着 B10 的顺序红线断言（原在 TestScenarioFullPowerLossCannotRecover）：
//   - 日志出现「数据目录保持原样未清空」——拒启原因必须是 PrepareJoin 求
//     不到接纳，而不是别的启动失败
//   - 日志**不**出现「状态目录已清空」——清空绝不能发生在取得接纳之前
//
// 若哪天有人把 Rejoin 的第 2/3 步调回去，本用例会红在这里。
func TestScenarioRebootedMemNeedsPermit(t *testing.T) {
	pc := startProcCluster(t, 3)
	sendAndTrack(t, pc.multi(), 60)

	for i := range pc.handles {
		pc.kill(t, i)
	}
	// 世代换新 = 机器重启过 = 页缓存已丢，mem 档下日志尾不可信
	for i := range pc.handles {
		pc.setEnv(i, "SQ_BOOTGEN_OVERRIDE=gen-after-reboot")
	}

	for i := range pc.handles {
		pc.restartUnready(t, i)
		logPath := []string{pc.handles[i].logPath}
		if n := countLogLines(t, logPath, "数据目录保持原样未清空"); n == 0 {
			t.Fatalf("节点 %d 日志里没有「数据目录保持原样未清空」——拒启原因不是 PrepareJoin 求不到接纳，机制对不上", i+1)
		}
		if n := countLogLines(t, logPath, "状态目录已清空"); n != 0 {
			t.Fatalf("节点 %d 在没有 leader 可接纳的情况下清空了数据目录（%d 次）——Rejoin 的第 2/3 步顺序被调回去了，这是数据永久全损的口子", i+1, n)
		}
		entries, err := os.ReadDir(pc.cfgs[i].DataDir)
		if err != nil {
			t.Fatalf("节点 %d 数据目录读不出来: %v", i+1, err)
		}
		if len(entries) == 0 {
			t.Fatalf("节点 %d 数据目录被清空了（0 个条目）——掉电前那份数据是集群仅剩的兜底，拒启的前提是它还在", i+1)
		}
		t.Logf("节点 %d：拒启且数据目录保留（%d 个条目）", i+1, len(entries))
	}
}
```

- [ ] **Step 4: 新增「签字后恢复」用例**

```go
// TestScenarioRebootedMemRecoversAfterGrant 承接上一条：三节点拒启之后，
// 逐台执行 `sq recover --grant` 签字，断言集群起得来、确认集零丢失。
//
// 逐台签字是刻意的（spec §3.5）：每个节点丢的尾巴不一样长，运维应逐台
// 看到各自的代价；真到全集群硬宕，人本来就要逐台去开机。
func TestScenarioRebootedMemRecoversAfterGrant(t *testing.T) {
	pc := startProcCluster(t, 3)
	ledger := sendAndTrack(t, pc.multi(), 90)
	time.Sleep(500 * time.Millisecond) // 理由同上一条用例：等周期 fsync 跑满一轮

	for i := range pc.handles {
		pc.kill(t, i)
	}
	for i := range pc.handles {
		pc.setEnv(i, "SQ_BOOTGEN_OVERRIDE=gen-after-reboot")
	}
	// 逐台签字：环境变量要一并带上，否则命令读到的是真实世代、
	// 写出来的许可绑错了世代，启动时不会被消费
	for i := range pc.handles {
		cmd := exec.Command(brokerBinary, "recover", "-config", pc.cfgPaths[i], "--grant")
		cmd.Env = append(os.Environ(), "SQ_BOOTGEN_OVERRIDE=gen-after-reboot")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("节点 %d 签字失败: %v\n%s", i+1, err, out)
		}
		t.Logf("节点 %d 签字输出：\n%s", i+1, out)
	}
	pc.restartAll(t)
	ledger.append(t, sendAndTrack(t, pc.multi(), 40))
	ledger.assertNoLoss(t, pc.multi())
	for i := range pc.handles {
		if n := countLogLines(t, []string{pc.handles[i].logPath}, "local-forced"); n == 0 {
			t.Fatalf("节点 %d 未走签字放行路径——许可没被消费", i+1)
		}
	}
}
```

- [ ] **Step 5: 新增「fsync 档 + 机器重启过」用例**

```go
// TestScenarioRebootedFsyncResumesLocally fsync 档即使机器重启过也不需要
// 签字：条目与 term/vote 每次变更都随 raft.MustSync 落了盘，掉电只可能丢
// commit 位点的推进，而那可由 leader 重新告知。
func TestScenarioRebootedFsyncResumesLocally(t *testing.T) {
	pc := startProcCluster(t, 3, func(c *config.Config) { c.Cluster.Ack = "quorum-fsync" })
	ledger := sendAndTrack(t, pc.multi(), 60)
	// fsync 档下条目每次 MustSync 都已落盘，理论上无需静置；仍留一小段，
	// 让本用例与前两条保持同一形态，避免日后有人以为这里可以省
	time.Sleep(500 * time.Millisecond)

	for i := range pc.handles {
		pc.kill(t, i)
	}
	for i := range pc.handles {
		pc.setEnv(i, "SQ_BOOTGEN_OVERRIDE=gen-after-reboot")
	}
	pc.restartAll(t)
	ledger.append(t, sendAndTrack(t, pc.multi(), 30))
	ledger.assertNoLoss(t, pc.multi())
}
```

- [ ] **Step 6: 跑测试确认通过**

Run: `go test -tags e2e ./test/e2e/ -run 'TestScenarioFullProcessCrash|TestScenarioRebooted' -v -timeout 30m`
Expected: 4 条全 PASS

Run: `go test -tags e2e ./test/e2e/ -timeout 60m`
Expected: 全量 ok，0 个 FAIL

- [ ] **Step 7: 加注释**

核对 Step 1–5 已包含：`procCluster.env` 字段说明「为什么进程级 e2e 只能走环境变量」；改写用例的注释说清「旧用例的前提哪里错了、真掉电形态搬去了哪里」；红线搬家用例注明「B10 的哨兵在这里」；签字用例说明「逐台签字是刻意的」与「环境变量必须一并带给命令」。

- [ ] **Step 8: 提交**

```bash
git add test/e2e/cluster_proc_test.go test/e2e/cluster_scenario_test.go
git commit -m "$(cat <<'EOF'
test(e2e): 场景用例跟上五分支——旧的「全崩即死」被推翻，B10 红线断言搬到真掉电分支

TestScenarioFullPowerLossCannotRecover 把「三节点全崩 = 集群永久需要人工
介入」固化成了期望行为，而它的前提是错的：SIGKILL 杀的是进程，机器没重启，
页缓存完好，三份日志毫发无伤。改写为 TestScenarioFullProcessCrashRecoversLocally，
断言集群自愈且确认集零丢失。

B10 的两条顺序红线断言（「数据目录保持原样未清空」出现、「状态目录已清空」
不出现）一条不丢，搬到 TestScenarioRebootedMemNeedsPermit——那才是它们现在
真正对应的分支。谁把 Rejoin 的第 2/3 步调回去，照样红在那里。

真掉电靠 SQ_BOOTGEN_OVERRIDE 模拟：进程级 e2e 起的是真 broker 进程，注不进
Go 函数，只能走环境变量。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: 文档与 backlog 回填

**Files:**
- Modify: `sq.example.yaml`
- Modify: `docs/superpowers/backlog.md`

**Interfaces:**
- Consumes: Task 1–7 的最终行为与验收数据
- Produces: 无

- [ ] **Step 1: 更新 `sq.example.yaml` 的自愈说明**

把现有的自愈说明段（B10 时写的那版，以「无人值守自愈（默认行为，无需配置）」开头）整段替换为：

```yaml
# 不干净关机（断电 / kill -9 / OOM / panic）后的分级恢复，默认行为，无需配置：
#
# 1) 机器没重启过（进程崩溃而已）：本地 raft 日志完整——未 fsync 的写入
#    仍在系统页缓存里，一条不少。节点直接以原身份从本地日志恢复，不联系
#    任何人。三节点同时被 kill -9 也能各自起来、重新选主。
#
# 2) 机器重启过，且 ack 为 quorum-fsync：条目与投票每次变更都已落盘，
#    本地日志同样可信，直接以原身份恢复。
#
# 3) 机器重启过，且 ack 为 quorum-mem（默认档）：日志尾可能残缺（后台
#    刷盘周期 200ms，最多丢这么多）。节点先向存活 leader 求得接纳，
#    **拿到接纳之后才清空自己的数据目录**，再以 learner 身份重新加入。
#
# 4) 上一条求不到接纳（多数派不在、无 leader 可批准成员变更，典型是全集群
#    同时硬宕）：数据目录**保持原样不清空**，进程拒启。这不是保守，是因为
#    此刻本地这份数据是集群仅剩的兜底——先清空再去问「能不能重入」，问不到
#    时数据已经没了。
#
#    此时有两条出路：
#      a. 恢复多数派后重启本节点，自动走 3)
#      b. 全集群都起不来时，逐台执行下面的命令看现场并签字放行本地恢复：
#           sq recover -config <配置>            # 只看报告，不写任何东西
#           sq recover -config <配置> --grant    # 签字：接受最多丢 200ms
#         许可是一次性的，且绑定当次机器世代——机器再重启一次即自动失效，
#         不会被下一次事故静默复用。
#
# 如需保留现场排查，在集群启动前先备份数据目录。
```

- [ ] **Step 2: 回填 backlog B11**

把 `docs/superpowers/backlog.md` 中 B11 行的状态改为 `✅ done(已验)`，并填写验收列与变更痕迹列。验收列必须写**实际跑出来的**结果，格式对齐 B8.3 那一行：单测条数、e2e 用例名与耗时、双平台结果。**不得以预期代替实测**——没跑出来的数字一个都不许写。

同时在 B10 行的备注末尾追加一句：「遗留半边由 **B11** 收口（08-10）：能证明本地日志完好时直接原身份恢复，B10 的两条顺序红线断言搬至 `TestScenarioRebootedMemNeedsPermit` 继续生效。」

- [ ] **Step 3: 双平台验收**

Run（macOS）：`go test -race ./... && go test -tags e2e ./test/e2e/ -timeout 60m`
Expected: 全绿

Run（Linux，本地交叉编译后 scp，**不得在远端装 Go 工具链**）：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/sq-linux ./cmd/sq
for p in ./internal/cluster ./internal/store ./internal/config; do
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o "/tmp/$(basename $p).test" "$p"
done
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -tags e2e -c -o /tmp/e2e.test ./test/e2e
```

把二进制 scp 到 Linux 主机后，用 `SQ_E2E_BROKER=<交叉编译的 broker 路径>` 跑 e2e（harness 见 `sdk_test.go:116`），并记录耗时与峰值 RSS（`/usr/bin/time -v`）。
Expected: 全绿，数字回填 backlog

- [ ] **Step 4: 提交**

```bash
git add sq.example.yaml docs/superpowers/backlog.md
git commit -m "$(cat <<'EOF'
docs(b11): 自愈说明改为分级恢复四段式，backlog B11 回填实测验收

sq.example.yaml 的自愈说明此前只讲得清「先求接纳后清空」（B10 那版），
现在按实际的四级分流重写：机器没重启 → 直接本地恢复；机器重启过但是
fsync 档 → 同样直接恢复；mem 档 → 求接纳后清空重入；求不到接纳 → 拒启
且数据完好，并给出 sq recover 的两条命令与「许可绑定机器世代、一次性」
这条关键性质。

B11 回填的验收数字全部来自实跑，双平台。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: 修复 mem 档周期刷盘的空操作（实现期订正，B11-OPEN-1 根因）

**背景**：Task 7 的 e2e 场景用例实跑暴露「全集群 SIGKILL 后稳定丢失尾部十余条
已确认消息」（B11-OPEN-1）。根因定位为 `Manager.flusher()` 提交**空批次** +
`pebble.Sync`，而 Pebble 的 `commitPipeline.Commit` 开头即 `if b.Empty() { return nil }`
——空批次连 WAL 都不写、更不 fsync，**mem 档运行期一次盘都不刷**，spec §2.3 的
「损失面 ≤200ms」从未成立。缺陷由 `0c7aa8c`（B8.2）引入，早于 B11 两天；B11 是
暴露者不是引入者。判据与完整证据见
`docs/superpowers/notes/2026-08-11-b11-grant-path-loss.md` 第 0 节。

**Files:**
- Modify: `internal/store/store.go`（新增 `SyncWAL`）
- Modify: `internal/cluster/manager.go`（`flusher()` 改调 `SyncWAL`）
- Test: `internal/store/store_test.go`
- Test: `test/e2e/cluster_scenario_test.go`（存量断言 + 判别器助手）

**Interfaces:**
- Produces: `func (s *Store) SyncWAL() error`

- [ ] **Step 1: 写失败的用例**

用 `vfs.NewCrashableMem` 的 `CrashClone`（零值配置 = 只保留已 fsync 的字节）做
**真掉电语义**断言，并带反向对照——不加屏障必须真的丢，否则用例失去判别力：

```go
func TestSyncWALPersistsNoSyncWrites(t *testing.T) {
	const n = 200
	survivorsAfterCrash := func(t *testing.T, barrier bool) int {
		fs := vfs.NewCrashableMem()
		db, _ := pebble.Open("/db", &pebble.Options{FS: fs})
		s := &Store{db: db, sync: false, logger: slog.Default()}
		for i := 0; i < n; i++ {
			b := s.NewBatch()
			b.Set([]byte(fmt.Sprintf("k%06d", i)), []byte("v"))
			s.ApplyWith(b, false)
		}
		if barrier {
			if err := s.SyncWAL(); err != nil { t.Fatalf("SyncWAL 失败: %v", err) }
		}
		crashed := fs.CrashClone(vfs.CrashCloneCfg{})
		db.Close()
		db2, _ := pebble.Open("/db", &pebble.Options{FS: crashed})
		defer db2.Close()
		s2 := &Store{db: db2, sync: false, logger: slog.Default()}
		got := 0
		for i := 0; i < n; i++ {
			if _, ok, _ := s2.Get([]byte(fmt.Sprintf("k%06d", i))); ok { got++ }
		}
		return got
	}
	if got := survivorsAfterCrash(t, true); got != n {
		t.Fatalf("SyncWAL 之后掉电，幸存 %d 条，要求 %d 条一条不少", got, n)
	}
	if got := survivorsAfterCrash(t, false); got == n {
		t.Fatalf("无屏障时也一条不丢（%d 条），本用例失去判别力", got)
	}
}
```

- [ ] **Step 2: 先用历史实现跑它，确认它能抓到这个 bug**

先把 `SyncWAL` 写成历史形态 `return s.ApplyWith(s.NewBatch(), true)`。

Run: `go test ./internal/store/ -run TestSyncWALPersistsNoSyncWrites`
Expected: FAIL，`SyncWAL 之后掉电，幸存 0 条，要求 200 条一条不少`

这一步不能跳：用例必须先对着**真实的历史缺陷**红过，才证明它测的是这件事。

- [ ] **Step 3: 实现 `SyncWAL`**

```go
// walBarrier 是 SyncWAL 写进 WAL 的屏障载荷。
var walBarrier = []byte("sq/wal-barrier")

func (s *Store) SyncWAL() error {
	b := s.db.NewBatch()
	if err := b.LogData(walBarrier, nil); err != nil {
		b.Close()
		return fmt.Errorf("store SyncWAL 组装屏障: %w", err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("store SyncWAL: %w", err)
	}
	return b.Close()
}
```

用 `LogData` 而不是写真键：它只进 WAL，不进 memtable/sstable、不被索引，5Hz 的
周期屏障因此不污染键空间、不产生 compaction 压力、不影响任何扫描路径。不走
`ApplyWith`：屏障不是一次 apply，计进 `OnApplyObserve` 会把这条恒定的空提交掺进
fsync 延迟直方图，污染写路径的延迟分布。

- [ ] **Step 4: 跑它确认通过**

Run: `go test ./internal/store/ -run TestSyncWALPersistsNoSyncWrites -v`
Expected: PASS（含反向对照）

- [ ] **Step 5: `flusher()` 改调 `SyncWAL`**

```go
case <-ticker.C:
	if err := m.st.SyncWAL(); err != nil {
		m.lg.Error("后台批量刷盘失败", "err", err)
	}
```

- [ ] **Step 6: 加关键节点日志**

- 失败分支：保留既有 `m.lg.Error("后台批量刷盘失败", "err", err)`——WAL sync 失败
  即 Pebble 不可恢复态，这行是尸检第一现场。
- **成功路径刻意不打日志**，这是本 task 唯一一处偏离「成功路径不静默」的通则，
  必须在代码注释里写明理由：5Hz 的周期动作，即便 Debug 也会淹掉日志；这条
  goroutine 的「在不在跑」由启动处已有的 Info 交代。
- `SyncWAL` 自身不打日志：store 层热路径不打日志是既有边界（见文件头注释）。

- [ ] **Step 7: 加注释**

- `SyncWAL` doc 注释必须写清三件事：为什么**必须**往批次里塞 `LogData`（贴出
  Pebble 的 `if b.Empty() { return nil }` 短路，并点名 B11-OPEN-1）、为什么用
  `LogData` 而不是真键、为什么不走 `ApplyWith`。
- `flusher()` 注释记录历史缺陷与新形态，指向 `SyncWAL` 的注释。
- 用例注释说明 `CrashClone` 为什么是真掉电语义，以及反向对照存在的意义。

- [ ] **Step 8: e2e 存量断言（把判别器固化下来）**

在 `test/e2e/cluster_scenario_test.go` 加 `queueNextOffsets` / `dumpQueueOffsets`
两个助手：读 `GET /admin/topics/{name}` 的 `queues_detail[].next_offset` 求和 =
该节点存里实际有多少条消息。在 `TestScenarioFullProcessCrashRecoversLocally` 的
kill 前与 `restartAll` 后各测一次，断言恢复后每个节点不许比断电前少。

这条断言比消息级对账更早失败、也更能指认病灶——它把「存储真丢了」与「存着但
投递不到」分开。同时删掉两条用例上的 `t.Skip("B11-OPEN-1…")`。

- [ ] **Step 9: 全量验证**

Run:
```bash
go build ./... && go vet ./...
go test ./internal/store/ ./internal/cluster/
cd test/e2e && go test -tags e2e ./ -run TestScenario -v -timeout 60m
```
Expected: 全绿；`TestScenarioFullProcessCrashRecoversLocally` 与
`TestScenarioRebootedMemRecoversAfterGrant` 由红转绿。

- [ ] **Step 10: 回填文档**

spec §2.3 / §3.4 / §8 / §9 的「不新增刷盘机制」「空批次 Sync 生效」口径全部订正
（保留错误留痕，不得抹掉）；notes 补第 0 节根因与修复；backlog 关闭 B11-OPEN-1。

- [ ] **Step 11: 提交**

```bash
git add internal/store/store.go internal/store/store_test.go \
        internal/cluster/manager.go test/e2e/cluster_scenario_test.go
git commit -m "$(cat <<'EOF'
fix(store,cluster): mem 档周期刷盘不再是空操作——空批次被 Pebble 短路，quorum-mem 运行期一次盘都不刷

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## 自审记录

**1. Spec 覆盖**

| spec 条目 | 落在哪个 task |
|---|---|
| §3.1 机器世代（含平台分派、容器语义、写入时机陷阱） | Task 1（读取）+ Task 2（落盘与注释）+ Task 5（写入时机与守卫用例） |
| §3.2 五分支判定表 | Task 3（纯函数 + 11 组表驱动）+ Task 5（接线） |
| §3.3 抬 term 的适用范围（四格表） | Task 4（`local-forced`）+ **Task 6b**（mem 档 `local-resume` 也抬、fsync 档不抬） |
| §3.3b 复用 `buildGroup(clean=true)` | Task 5 的 `replay` 变量与其注释 |
| §3.4 刷盘机制只修好、不改形态 | **Task 9**（原为「不动刷盘机制」，实现期实测推翻，见下） |
| §3.5 签字出口（报告、许可作废、互斥、逐台、留痕、不破坏部署） | Task 4（许可键）+ Task 6（命令） |
| §4 文件结构 | 本计划「文件结构」表逐行对应 |
| §5 键布局 | Task 2（`raft/bootgen`）+ Task 4（`raft/local_recover_permit`） |
| §6 错误处理 | Task 1（读不到）、Task 2（写失败即启动失败）、Task 4（许可格式非法、同批提交）、Task 6（Pebble 独占锁） |
| §7 可观测性 | 各 task 的「加关键节点日志」step |
| §8 测试策略 | Task 3、5（单测）+ **Task 6b**（抬 term 四格逐格单测）+ Task 7（e2e 四条，含「静置 ≥500ms 再 kill」的断言时机纪律）+ **Task 9**（掉电语义单测与 e2e 存量断言——静置纪律的前提本身要有用例守着） |
| §9 文档同步 | Task 6（`main.go` 头注释）+ Task 8（`sq.example.yaml`） |
| §11 验收标准 7 条 + 4b | Task 3/5/7（1–5）、Task 8（6–7）、**Task 6b**（4b 抬 term 适用范围）、**Task 9**（验收 2「静置后零丢失」的真正实现件） |

无遗漏。

> **Task 9 是 Task 7 实跑后补的订正 task**：Task 7 的 e2e 用例暴露「全集群 SIGKILL 后稳定丢失尾部十余条已确认消息」（B11-OPEN-1），根因是 `Manager.flusher()` 的空批次 `Sync` 被 Pebble 短路成空操作，mem 档运行期从不 fsync——spec 原先「§3.4 不动刷盘机制、损失面已有界」的前提整个是假的。spec §2.3/§3.4/§8/§9 已同步订正并留痕（错误留痕不抹）。执行顺序上 **Task 9 必须排在 Task 8 之前**——Task 8 要回填的验收数字依赖 Task 9 修好之后的实跑结果。

> **Task 6b 是 Task 1–6 完成后补的订正 task**：Task 7 的 e2e 实测推翻了 spec 原先「NoSync 提交返回即进页缓存」的前提（Pebble 的 `WriteRecord` 只 `Signal` 一下就返回，`write(2)` 由 flusher goroutine 异步做）。spec §2.2/§3.3/§8/§11 已同步订正并留痕，本表按订正后的 spec 重新核对过。执行顺序上 **Task 6b 必须排在 Task 7 之前**——Task 7 的 mem 档场景断言依赖抬 term 后的行为。

**2. 占位符扫描**：无 TBD/TODO；每个代码 step 都给出了可直接落地的完整代码；测试 step 给出了完整测试函数体而非描述。

**3. 类型一致性**：`BootGenFunc` 在 Task 1 定义，Task 5（`Options.BootGen`）、Task 6（`InspectRecovery`/`GrantRecoverPermit` 参数）使用，签名一致；`recoverPermit{GrantedAt, Gen}` 在 Task 4 定义，Task 5、6 使用一致；`recoveryInput` 字段名在 Task 3 定义，Task 5、6 构造时逐字对应；`recoveryPath` 常量名在 Task 3 定义，Task 5 的 switch、Task 6 的 `NeedsPermit` 判定、Task 6b 的 `needsTermBump` 使用一致；Task 6b 新增的 `bumpTermsInto` 由 `ForceLocalRecover`（Task 4）与 `BumpTermsForLocalResume` 共用，抽取时 `ForceLocalRecover` 的对外签名不变。

**4. 一处需要执行者注意的既有代码依赖**：Task 5 替换的是 `manager.go` 现有的 `switch { case clean: ... case hasRaft: ... default: ... }` 段落（约 379–409 行），其中 `default` 分支的 preRaft 标记逻辑必须原样保留到新的 `case pathFresh` 里——那是 Join 拒绝判据的依据（batch④ N2），丢了会让单机升级档与全新集群无法区分。
