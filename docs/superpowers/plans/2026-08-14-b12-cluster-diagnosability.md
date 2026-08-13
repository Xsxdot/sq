# B12 可诊断性修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 Pebble 绕过项目 logger 直写 stderr 的噪声源，把测试输出纪律固化进 Makefile，并对 `internal/cluster` 的偶发失败做一次如实记账的复现尝试。

**Architecture:** 新增一个 `pebble.Logger` → `*slog.Logger` 的适配器，挂到 `store.Open` 已有的 logger 参数上（签名不变，174 个调用点零改动）；Makefile 的 `test` 目标改为完整落盘 + `pipefail` 保退出码，并新增 `test-cluster` 入口；最后在 CPU 争抢下跑 12 轮 `internal/cluster` 并逐轮记账。

**Tech Stack:** Go 1.x，`log/slog`，`github.com/cockroachdb/pebble/v2 v2.1.6`，GNU Make + bash。

## Global Constraints

- **不修任何 `internal/cluster` 测试用例，不调任何超时常量。** 那条 flaky 用例的名字不可考，盲目改是打补丁。见 spec §2 的 N2。
- **不得声称「B12 已修复」「集群测试已稳定」。** 本次交付的是可诊断性，不是修复。12 轮全绿只能写成「12 轮未复现」。
- 日志一律走 `*slog.Logger`，**禁止 `fmt.Printf` / `println` 作为日志机制**。
- 新建文件必须有文件头注释（职责 + 边界）；导出（及包内非显然的）函数需注释参数、返回、注意事项；非显然分支写「为什么」的中文注释。
- 参考 spec：`docs/superpowers/specs/2026-08-14-b12-cluster-flake-diagnosability-design.md`（**开工前先完整读一遍**，尤其 §5 级别映射与 `Fatalf` 契约、§7 三条待复核假设）。

---

### Task 1: Pebble 日志适配器与装配

**Files:**
- Create: `internal/store/pebblelog.go`
- Modify: `internal/store/store.go`（`Open` 函数，当前在第 45–56 行附近）
- Test: `internal/store/pebblelog_test.go`

**Interfaces:**
- Consumes: `pebble.Logger`（`github.com/cockroachdb/pebble/v2`，是 `base.Logger` 的导出别名，三方法：`Infof(string, ...interface{})` / `Errorf(...)` / `Fatalf(...)`）；`pebble.Options.Logger` 字段。
- Produces: 包内 `newPebbleLogger(base *slog.Logger) *pebbleLogger`。仅 `internal/store` 内部使用，不对外导出。

- [ ] **Step 1: 写失败测试**

创建 `internal/store/pebblelog_test.go`：

```go
package store

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestPebbleLoggerLevelMapping 验证三件事：Infof 降级到 DEBUG、Errorf 对应
// ERROR、格式化参数被正确展开，且都带上 src=pebble 标记。
func TestPebbleLoggerLevelMapping(t *testing.T) {
	var buf bytes.Buffer
	lg := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := newPebbleLogger(lg)

	p.Infof("wal count n=%d", 42)
	p.Errorf("compaction failed: %s", "boom")

	out := buf.String()
	for _, want := range []string{"level=DEBUG", "n=42", "level=ERROR", "compaction failed: boom", "src=pebble"} {
		if !strings.Contains(out, want) {
			t.Fatalf("输出中缺少 %q，实际输出：%s", want, out)
		}
	}
}

// TestPebbleLoggerInfoFilteredAtInfoLevel 是本次改动的核心收益断言：默认的
// info 级别下，Pebble 的 Infof 噪声被完全挡住，不再污染日志。
func TestPebbleLoggerInfoFilteredAtInfoLevel(t *testing.T) {
	var buf bytes.Buffer
	lg := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	newPebbleLogger(lg).Infof("noisy: %s", "Found 0 WALs")
	if buf.Len() != 0 {
		t.Fatalf("info 级别下 Pebble 的 Infof 不应输出，实际：%s", buf.String())
	}
}

// TestOpenWiresPebbleLogger 证明适配器真的挂到了 pebble.Options.Logger 上，
// 而不是写了个没人用的类型。Pebble 在 open.go 的恢复路径上无条件打一行
// "Found %d WALs"（v2.1.6 open.go:383），所以一次空库 Open 就足以触发。
func TestOpenWiresPebbleLogger(t *testing.T) {
	var buf bytes.Buffer
	lg := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	st, err := Open(t.TempDir(), false, lg)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer st.Close()

	out := buf.String()
	if !strings.Contains(out, "src=pebble") {
		t.Fatalf("Open 未把适配器接到 pebble.Options.Logger，输出中没有 src=pebble 的行：%s", out)
	}
	if !strings.Contains(out, "WALs") {
		t.Fatalf("未捕获到 Pebble open 路径的 \"Found N WALs\" 日志：%s", out)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/store/ -run 'TestPebbleLogger|TestOpenWires' -v`
Expected: FAIL — `undefined: newPebbleLogger`（编译错误）

- [ ] **Step 3: 写适配器实现**

创建 `internal/store/pebblelog.go`：

```go
// pebblelog.go: Pebble 日志接口到项目 slog 的桥接。
//
// 职责：
//   - 实现 pebble.Logger（Infof/Errorf/Fatalf），把 Pebble 内部日志接入项目
//     统一的 *slog.Logger，取代它默认写 stderr 的 log.Printf 行为
//
// 边界：
//   - 不做采样、不做限流、不解析日志内容：级别映射之外原样转发
//   - 级别映射策略与 Fatalf 的进程终止契约见下方各方法注释
package store

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/cockroachdb/pebble/v2"
)

// 编译期断言：签名一旦与 Pebble 的接口对不上，这里就报错，而不是等到
// pebble.Options{Logger: ...} 那行才暴露。
var _ pebble.Logger = (*pebbleLogger)(nil)

// pebbleLogger 把 pebble.Logger 的三个方法桥到 slog。
// 零值不可用（l 为 nil 会 panic），必须经 newPebbleLogger 构造。
type pebbleLogger struct {
	l *slog.Logger
}

// newPebbleLogger 基于 base 构造适配器，返回值挂到 pebble.Options.Logger 上。
//
// base 通常是已经 With("mod","store") 过的实例；这里再补一对 src=pebble，
// 使 Pebble 自身的输出与 store 包的语义日志在同一份日志里可区分、可过滤。
// base 为 nil 时 slog.With 会 panic——调用方（store.Open）保证非 nil。
func newPebbleLogger(base *slog.Logger) *pebbleLogger {
	return &pebbleLogger{l: base.With("src", "pebble")}
}

// Infof 转发为 Debug 而非 Info。
//
// Pebble 的 Info 级输出是 WAL 扫描、compaction 明细一类的运维细节，每次 open
// 都会打若干行；映射成 Info 只会把「直写 stderr 的刷屏」换成「写进项目日志的
// 刷屏」，问题没解决只是换了出口。需要这些细节时把日志级别调到 debug 即可。
func (p *pebbleLogger) Infof(format string, args ...interface{}) {
	p.l.Debug(fmt.Sprintf(format, args...))
}

// Errorf 一一对应到 slog 的 Error。
func (p *pebbleLogger) Errorf(format string, args ...interface{}) {
	p.l.Error(fmt.Sprintf(format, args...))
}

// Fatalf 必须终止进程，这是 Pebble 的契约：它只在无法安全继续时（如检测到
// 数据损坏）调用，默认实现就是 log.Fatalf（os.Exit(1)）。
//
// 为什么不降级、不 panic：
//   - 降级成一条 Error 后返回，会让本该立刻停下的进程带着已知损坏的状态继续
//     跑，比崩溃危险得多；
//   - 改成 panic 则可能被上层 recover 吞掉，同样破坏「不可继续」的语义。
//
// 先打一条 Error，保证这条致命信息进入项目的日志通道（而不是只留在 stderr），
// 再 os.Exit(1)。
func (p *pebbleLogger) Fatalf(format string, args ...interface{}) {
	p.l.Error(fmt.Sprintf(format, args...), "fatal", true)
	os.Exit(1)
}
```

- [ ] **Step 4: 在 `store.Open` 里装配**

修改 `internal/store/store.go` 的 `Open`。当前实现：

```go
func Open(dir string, syncWrites bool, logger *slog.Logger) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("打开 Pebble(%s): %w", dir, err)
	}
	// mod 一次性绑在 logger 上（与 meta/produce/deliver/rpc 各包的做法一致），
	// 之后各调用点不再逐条重复这对键值。
	l := logger.With("mod", "store")
	l.Info("store 已打开", "dir", dir, "sync", syncWrites)
	return &Store{db: db, dir: dir, sync: syncWrites, logger: l}, nil
}
```

替换为（注意 `l` 的求值上移到 `pebble.Open` 之前）：

```go
func Open(dir string, syncWrites bool, logger *slog.Logger) (*Store, error) {
	// mod 一次性绑在 logger 上（与 meta/produce/deliver/rpc 各包的做法一致），
	// 之后各调用点不再逐条重复这对键值。
	//
	// 必须在 pebble.Open 之前求值：Options.Logger 要拿它构造适配器。不设
	// Options.Logger 时 Pebble 会退回 DefaultLogger 直写 stderr，绕过项目的
	// 日志通道——那正是 B12 里 FAIL 行被噪声挤掉的成因。
	l := logger.With("mod", "store")

	db, err := pebble.Open(dir, &pebble.Options{Logger: newPebbleLogger(l)})
	if err != nil {
		return nil, fmt.Errorf("打开 Pebble(%s): %w", dir, err)
	}
	l.Info("store 已打开", "dir", dir, "sync", syncWrites)
	return &Store{db: db, dir: dir, sync: syncWrites, logger: l}, nil
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `go test ./internal/store/ -run 'TestPebbleLogger|TestOpenWires' -v`
Expected: 三个用例全 PASS

- [ ] **Step 6: 跑 store 包全量回归**

Run: `go test -count=1 ./internal/store/...`
Expected: ok

- [ ] **Step 7: 日志与注释自检**

对照确认（这些内容在 Step 3/4 的代码里已给出，此步是核对而非补写）：
- `pebblelog.go` 有文件头注释（职责 + 边界）
- `newPebbleLogger` / `Infof` / `Errorf` / `Fatalf` 各有注释；`Infof` 说明了为什么降级到 Debug，`Fatalf` 说明了为什么不降级不 panic
- `store.go` 里 `l` 上移处有「为什么必须在 pebble.Open 之前」的注释
- 全文无 `fmt.Printf` / `println` 作为日志机制（`fmt.Sprintf` 用于格式化，不是日志出口，允许）

- [ ] **Step 8: 提交**

```bash
git add internal/store/pebblelog.go internal/store/pebblelog_test.go internal/store/store.go
git commit -m "fix(store): Pebble 日志接入项目 slog，不再直写 stderr

store.Open 本来就收 *slog.Logger，只是从未交给 Pebble；Options.Logger 为空时
Pebble 退回 DefaultLogger 直写 stderr，绕过项目日志通道。这是 B12 中 FAIL 行
被噪声挤出输出窗口的直接成因。

Infof 映射到 Debug（Pebble 的 Info 是每次 open 都打的运维细节），Errorf 对应
Error，Fatalf 保持 os.Exit(1) 契约。签名不变，174 个调用点零改动。"
```

---

### Task 2: Makefile 测试输出纪律

**Files:**
- Modify: `Makefile`（第 1 行 `.PHONY`、第 17–18 行 `test` 目标）

**Interfaces:**
- Consumes: 无（独立于 Task 1）
- Produces: `make test`（完整输出落 `test-output.log`，退出码保真）、`make test-cluster`（`internal/cluster` 专用入口，落 `test-cluster.log`）

- [ ] **Step 1: 声明 bash 并扩充 .PHONY**

`Makefile` 第 1 行当前是：

```makefile
.PHONY: build build-go web test test-web proto e2e soak soak-e2e
```

替换为：

```makefile
.PHONY: build build-go web test test-cluster test-web proto e2e soak soak-e2e

# set -o pipefail 是 bash 特性，Makefile 默认的 /bin/sh 在部分平台上不支持。
# 测试目标依赖它保住 go test 的退出码，所以整个 Makefile 统一用 bash。
SHELL := /bin/bash
```

- [ ] **Step 2: 改写 `test` 目标并新增 `test-cluster`**

当前：

```makefile
test:
	go test ./...
```

替换为：

```makefile
# 测试输出纪律（B12 教训，别改回去）：
#   ① 完整输出落文件，永不过 tail/head——2026-08-11 一次 `go test ./... | tail -60`
#      让 Pebble 噪声把唯一那行 `--- FAIL: TestXxx` 挤出了窗口，失败用例至今不可考。
#      要截，也先落完整文件再从文件里截。
#   ② set -o pipefail 保住 go test 的退出码——同一条管道当时把退出码换成了
#      tail 的 0，那次失败差点被整个漏掉。
#   ③ -timeout 显式写出（与 Go 默认值相同），让它是个可见的旋钮而非隐式默认。
test:
	set -o pipefail; go test -timeout 10m ./... 2>&1 | tee test-output.log

# internal/cluster 专用入口：该包有一次未复现的偶发失败（B12）。
#   -count=1  防测试缓存给出假绿——追偶发时缓存命中等于没跑。
#   -timeout 5m 该包全量约 72–85s，5m 是约 3.5 倍余量；真挂死时比默认的 10m
#              早 5 分钟触发栈转储。
test-cluster:
	set -o pipefail; go test -timeout 5m -count=1 ./internal/cluster/... 2>&1 | tee test-cluster.log
```

- [ ] **Step 3: 验证退出码保真（正向）**

Run: `make test-cluster; echo "exit=$?"`
Expected: 测试通过时 `exit=0`，且 `test-cluster.log` 存在、内容与终端一致

- [ ] **Step 4: 验证退出码保真（反向，关键）**

这一步证明 `pipefail` 真的生效——只验证机制，不留下改动：

```bash
set -o pipefail; go test -timeout 1m -run 'TestDefinitelyDoesNotExistZZZ' ./internal/store/ 2>&1 | tee /dev/null; echo "exit=$?"
```

Expected: `exit=0`（没有匹配用例不算失败）。再跑一次真失败的形态确认：

```bash
set -o pipefail; go vet ./internal/nonexistentpkg 2>&1 | tee /dev/null; echo "exit=$?"
```

Expected: `exit` 非 0 —— 证明管道没有吞掉左侧的退出码。若得到 `exit=0`，说明
`SHELL := /bin/bash` 没生效或 shell 不支持 pipefail，**停下排查，不要继续**。

- [ ] **Step 5: 确认日志文件不会被提交**

Run: `git status --porcelain | grep -E 'test-output\.log|test-cluster\.log'`
Expected: 无输出（`.gitignore` 第 3 行的 `*.log` 已覆盖）。若有输出，说明
gitignore 未生效，**停下排查**，不要用 `git add -f` 绕过。

- [ ] **Step 6: 提交**

```bash
git add Makefile
git commit -m "build(make): 测试输出落盘 + pipefail 保退出码，新增 test-cluster 入口

B12 教训固化进工具链：完整输出永不过 tail/head（噪声会挤掉 FAIL 行），管道用
pipefail 保住 go test 的退出码（否则失败被静默成 0）。新增 test-cluster 作为
追该包偶发失败的固定入口：-count=1 防缓存假绿，-timeout 5m 收紧。"
```

---

### Task 3: 复现尝试与如实记账

**Files:**
- Create: `docs/superpowers/notes/2026-08-14-b12-repro-log.md`

**Interfaces:**
- Consumes: Task 2 产出的 `make test-cluster`
- Produces: 复现记录文件（供协调者判定 B12 后续走向）

**这个 task 的产出是记录，不是修复。** 无论结果如何都不许改任何 cluster 用例。

- [ ] **Step 1: 建记录文件骨架**

创建 `docs/superpowers/notes/2026-08-14-b12-repro-log.md`：

```markdown
# B12 复现尝试记录（2026-08-14）

**目标**：在 CPU 争抢条件下反复跑 `internal/cluster`，尝试复现 2026-08-11 那次
85.057s 的偶发 FAIL。手法沿用当初唯一复现过的条件组合：冷 GOCACHE + `-count=1`
+ 背景 CPU 满载。

**纪律**：全绿只能记「N 轮未复现」，不得写成「已修复」或「已稳定」。复现到了
记录用例名与完整失败片段，**不当场修**——交回协调者定夺。

**执行机**：<填写 uname -a 与 nproc/sysctl -n hw.ncpu 的输出>

## 逐轮结果

| 轮次 | 耗时 | 结果 | 失败用例 |
|------|------|------|---------|

## 结论

<待填>
```

- [ ] **Step 2: 记录执行机规格**

```bash
uname -a
nproc 2>/dev/null || sysctl -n hw.ncpu
```

把输出填进骨架的「执行机」一行。

- [ ] **Step 3: 跑 12 轮，每轮一条独立命令**

每轮用下面这条（把 `N` 换成 1..12）。背景负载路数取 CPU 核数，跑完即杀：

```bash
CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu); \
for i in $(seq 1 $CORES); do yes > /dev/null & done; \
LOADPIDS=$(jobs -p); \
GOCACHE=$(mktemp -d) go test -timeout 5m -count=1 ./internal/cluster/... > /tmp/b12-round-N.log 2>&1; \
STATUS=$?; kill $LOADPIDS 2>/dev/null; \
echo "round=N status=$STATUS"; \
grep -E '^(--- FAIL|FAIL|ok)' /tmp/b12-round-N.log | head -20
```

要点：
- 用**独立 `GOCACHE`**（`mktemp -d`）制造冷缓存，精确重造当初的失败条件。
- 输出**先落完整文件**再 grep——就是 Task 2 里固化的那条纪律，这里身体力行。
- 背景负载必须在测试结束后 kill，否则会累积到后续轮次。

每轮跑完立刻把一行结果追加进记录文件的表格（轮次 / 耗时 / PASS 或 FAIL /
失败用例名，全绿则写 `—`）。**不要攒到最后一起写**——中途被打断时已跑的轮次
不能白跑。

- [ ] **Step 4: 命中失败时的处置**

若某轮出现 FAIL：
- 把 `/tmp/b12-round-N.log` 里从 `--- FAIL` 起的完整片段（含 `t.Log` 现场上下文，
  Task 1 之后 Pebble 的输出会归属在该用例名下）粘进记录文件。
- **停止后续轮次**，在记录文件的「结论」里写明用例名与观察，然后结束本 task。
- **不修它。** 交回协调者决定单独立项还是并入本次。

- [ ] **Step 5: 写结论**

12 轮全绿时，「结论」一节按这个措辞写（不得加强）：

```markdown
12 轮未复现。条件：冷 GOCACHE + `-count=1` + <CORES> 路 CPU 满载，逐轮耗时
<最小>–<最大>s。累计未复现轮次达 27 次（此前 15 次 + 本次 12 次），但仍**不**
据此判定该偶发已消失——B12 的立项前提正是「重跑绿了不是结论，是又一个数据点」。

本次的实际收益是可诊断性：Pebble 噪声已改由项目 slog 承接（失败用例的 t.Log
里归属可见、通过用例完全静默），`make test` / `make test-cluster` 保住完整输出
与退出码。下一次若再失败，用例名必然留在 `test-cluster.log` 里。
```

命中失败时，「结论」写用例名 + 观察 + 「已交回协调者定夺，本次未修改任何用例」。

- [ ] **Step 6: 提交**

```bash
git add docs/superpowers/notes/2026-08-14-b12-repro-log.md
git commit -m "docs(b12): 复现尝试逐轮记录与结论"
```

---

### Task 4: 全量验收与完工自检

**Files:** 无新增改动（仅验证；如发现问题则回到对应 task 修）

- [ ] **Step 1: 全量测试**

Run: `make test`
Expected: 全绿，退出码 0。**检查 `test-output.log`：确认输出里不再出现裸的
Pebble `Found N WALs` 行**（通过的用例不再打印它——这是本次改动最直观的收益）。

- [ ] **Step 2: 编译检查**

Run: `go build ./... && go vet ./internal/store/...`
Expected: 无输出

- [ ] **Step 3: 确认未越界修改**

Run: `git diff --stat <分支起点>..HEAD`
Expected: 改动仅限于 `internal/store/pebblelog.go`、`internal/store/pebblelog_test.go`、
`internal/store/store.go`、`Makefile`、`docs/superpowers/notes/…`。

**若 `internal/cluster/` 下有任何文件被改动，那是违反 Global Constraints，
必须还原。**

- [ ] **Step 4: instrumenting-code 完工自检**

逐项核对（不通过就回去补）：
- [ ] 每个错误分支都带上下文记了日志 —— 本次新增代码的错误面只有 `Fatalf`，已记 Error + `fatal=true`
- [ ] 外部调用前后有日志 —— `pebble.Open` 前后：前有适配器装配，后有 `store 已打开`（既有）
- [ ] 成功路径不静默 —— `store 已打开` 保留
- [ ] 无 `fmt.Printf` / `println` 作为日志机制
- [ ] 新文件有文件头注释（职责 + 边界）—— `pebblelog.go`
- [ ] 导出/关键函数有注释，非显然分支有「为什么」注释 —— `Infof` 降级理由、`Fatalf` 不降级不 panic 的理由、`store.go` 里 `l` 上移的理由

- [ ] **Step 5: 汇报**

在最终回复里明确写出三件事：
1. Task 3 的复现结论**原文**（未复现就是未复现，不加修饰）。
2. spec §7 的三条待复核假设（A1 Infof→Debug 的行为变更、A2 Fatalf 保持
   `os.Exit(1)`、A3 生产路径一并接入），提醒协调者这是外出期间按最保守分支
   定的，需要人来复核。
3. B12 **不因本次关闭**：偶发失败本身仍待观察。

---

## 自审记录

**Spec 覆盖：**
- spec §4.1 Pebble 接入 slog → Task 1（含 §5 的级别映射表与 `Fatalf` 契约，逐条落进代码注释）
- spec §4.2 Makefile 纪律 → Task 2（`SHELL := /bin/bash`、`pipefail`、`tee`、显式 `-timeout`、`test-cluster`）
- spec §4.3 复现尝试与记账 → Task 3（12 轮、冷 GOCACHE、CPU 满载、逐轮落盘、措辞受限的结论）
- spec §6 测试计划 T1/T2/T3 → Task 1 Step 1 三个用例一一对应
- spec §7 三条待复核假设 → Task 4 Step 5 要求在汇报里原样带出
- spec §9 日志与注释要求 → Task 1 Step 7 + Task 4 Step 4
- spec §2 的 N1/N2/N3 → Global Constraints + Task 4 Step 3 的越界检查

**占位符扫描：** 无 TBD/TODO。Task 3 Step 1 的骨架里有两处 `<待填>` / `<填写…>`，
那是**记录文件的模板占位**（执行时由实测值填入），不是计划本身的占位。

**类型一致性：** `newPebbleLogger(base *slog.Logger) *pebbleLogger` 在 Task 1 的
测试（Step 1）、实现（Step 3）、装配（Step 4）三处签名一致；`pebble.Logger` 已
核实为 v2.1.6 中 `base.Logger` 的导出别名（`logger.go:10`），三方法签名与实现
逐字对齐；`pebble.Options.Logger` 字段类型为 `Logger`（`options.go:875`）。
Task 1 Step 1 的 T3 依赖 Pebble 在 open 路径无条件打 `Found %d WALs`
（v2.1.6 `open.go:383`），已核实为无条件调用，非条件分支。
