# B14 + B15 实现计划：Apply 观测钩子并发安全化 + 本地恢复日志分级

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `store` 的 Apply 观测钩子从「靠顺序纪律保证安全的裸全局」改成 `atomic.Pointer`，
并把本地恢复抬任期的日志按调用方分成 WARN / ERROR 两级。

**Architecture:** 两条改动互不依赖。B14 在 `internal/store` 内换掉钩子的存取方式，
读点收敛到一个包内函数，再迁移三处调用方与两处过时注释；B15 给 `bumpTermsInto`
加一个原因形参，在循环内按原因分流日志级别。行为语义（指标口径、抬任期动作）零变化。

**Tech Stack:** Go 1.26.1，`sync/atomic`（`atomic.Pointer[T]`），`log/slog`，标准 testing。

**Spec:** [2026-08-14-apply-observer-race-and-recovery-log-level-design.md](../specs/2026-08-14-apply-observer-race-and-recovery-log-level-design.md)

## Global Constraints

以下每条对每个 task 都生效，逐字照抄自 spec：

- **基线是 `main`**，不叠在 `verify/final-integration` 或任何未合并分支上。
- **不改**`sq_store_apply_duration_seconds` 的指标语义、桶划分（`ExponentialBuckets(0.0001, 2, 14)`）、
  观测口径（覆盖「提交开始 → 持久化完成」全程；只观测成功提交）。
- **不重构** `cmd/sq/main.go` 的启动编排。修完后 `metrics.NewRegistry` 相对
  `m.Start(runCtx)` 的位置不再影响正确性，这正是本方案的目的。
- **不动** `bumpTermsInto` 抬任期的行为本身（legacy hsKey 分流、逐组 sync 落盘、幂等性）。
  B15 只动日志级别与文案。
- **改动面白名单**，超出即为越界：
  `internal/store/store.go`、`internal/store/store_observer_test.go`（新增）、
  `internal/metrics/collector.go`、`internal/metrics/metrics_test.go`、
  `internal/cluster/raftstore.go`、`internal/cluster/raftstore_test.go`、
  `internal/cluster/group_test.go`、`cmd/sq/main.go`。
- **日志禁止 `fmt.Printf`**，一律用既有 `*slog.Logger`。
- **注释用中文，解释「为什么」不是「做了什么」**。
- **每个判别器都必须做变异验证**：亲手掐掉它保护的实现，确认用例变红，再 `git checkout --`
  还原并确认工作区干净。**若掐掉后用例仍绿，停下来上报 BLOCKED，不许改断言迁就。**

## File Structure

| 文件 | 职责 | 本次改动 |
|---|---|---|
| `internal/store/store.go` | 唯一写入口；持有 Apply 观测钩子 | 钩子改私有 `atomic.Pointer`，新增 3 个函数，3 个读点收敛，契约注释重写 |
| `internal/store/store_observer_test.go`（新建） | 观测钩子的并发安全判别器 | 新增 1 条承重用例 |
| `internal/metrics/collector.go` | 组装 registry 并挂接直方图 | 装钩子改调 `SetApplyObserver` |
| `internal/metrics/metrics_test.go` | metrics 测试 | 更新文件头注释里关于 `t.Parallel()` 的过时理由 |
| `internal/cluster/raftstore.go` | raft 持久化层 | `bumpTermsInto` 加原因形参并分流日志级别 |
| `internal/cluster/raftstore_test.go` | raftstore 测试 | 新增 2 条日志级别判别器 + 捕获型 handler |
| `internal/cluster/group_test.go` | group 测试 | `countApplyCommits` 改用 `SwapApplyObserver` |
| `cmd/sq/main.go` | 启动编排 | 只改注释：那段顺序契约已作废 |

---

### Task 1: store 侧钩子改 atomic.Pointer

**Files:**
- Modify: `internal/store/store.go:28-31`（声明与契约注释）、`:205`、`:299`、`:323`（三个读点）
- Test: `internal/store/store_observer_test.go`（新建）

**Interfaces:**
- Consumes: 无（本 task 是链条起点）
- Produces: 供 Task 2 使用的三个导出/包内符号——
  - `func SetApplyObserver(f func(d time.Duration))`
  - `func SwapApplyObserver(f func(d time.Duration)) func(d time.Duration)`
  - `func observeApply(d time.Duration)`（包内，不导出）
  - 包级变量 `OnApplyObserve` **被删除**，任何外部直接赋值都会编译失败——这是刻意的，
    编译器会替我们找出所有遗漏的调用方。

- [ ] **Step 1: 写失败的判别器**

新建 `internal/store/store_observer_test.go`：

```go
// Apply 观测钩子的并发安全判别器。
//
// 职责：证明「一边跑 Apply 一边换观测钩子」不构成数据竞态。
// 边界：不验证观测值的准确性（切换瞬间漏掉少数样本是允许的，见 SetApplyObserver
//       的文档），只验证并发存取本身安全。
package store

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestApplyObserverConcurrentSetAndRead【承重】写侧与读侧并发。
//
// 这条用例存在的唯一理由：2026-08-14 实测发现集群档下 cluster.Manager.Start
// 拉起的 apply goroutine 会在 metrics 装配之前就开始读这个钩子（backlog B14）。
// 把实现退回裸 `var OnApplyObserve func(time.Duration)`，本用例在 -race 下必须变红；
// 若仍绿，说明用例没有真正制造并发读写，停下来上报，不要改断言迁就。
func TestApplyObserverConcurrentSetAndRead(t *testing.T) {
	s, err := Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	t.Cleanup(func() { SetApplyObserver(nil) })

	const writers = 4
	var observed atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 读侧：多个 goroutine 持续 Apply，每次成功提交都会读一次钩子。
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				b := s.NewBatch()
				if err := b.Set([]byte(fmt.Sprintf("k/%d/%d", w, i)), []byte("v")); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				if err := s.Apply(b); err != nil {
					t.Errorf("Apply: %v", err)
					return
				}
			}
		}(w)
	}

	// 写侧：每轮换一个**新的非 nil** 闭包，指针每轮都变，全程不切 nil；
	// 清 nil 只在循环结束后做一次（那也是生产里唯一的 nil 转换方向）。
	//
	// 为什么不「非 nil ↔ nil 来回切」：并发暴露面并不会因此变大——race
	// detector 看的是「无同步地读写同一变量」，与值是不是 nil 无关——但
	// 那种写法的非 nil 窗口只有相邻两条语句的纳秒级间隙，而读侧要等一次
	// Pebble 提交（微秒级）才读一次钩子。机器一忙，读侧整轮都撞不上非 nil，
	// observed 恒为 0、用例假红。实测：整包跑 11/11 红、单跑 20/20 绿。
	// 下一个人若觉得「来回切更全面」而改回去，缺陷会复发。
	deadline := time.Now().Add(10 * time.Second)
	for rounds := 0; ; rounds++ {
		SetApplyObserver(func(time.Duration) { observed.Add(1) })
		if rounds >= 200 && observed.Load() > 0 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
	}
	SetApplyObserver(nil)
	close(stop)
	wg.Wait()

	// 不断言具体条数：切换瞬间漏样本是允许的。但一条都没观测到，说明
	// 钩子从未真正被调用过，用例失去判别力。
	if observed.Load() == 0 {
		t.Fatal("10 秒内观测计数仍为 0——钩子一次都没被调用，本用例无法证明任何并发性质")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -race -run TestApplyObserverConcurrentSetAndRead ./internal/store/`
Expected: 编译失败，`undefined: SetApplyObserver`

- [ ] **Step 3: 实现 atomic.Pointer 钩子**

`internal/store/store.go`，把 `:28-31` 整段替换（旧契约注释一并删除）：

```go
// onApplyObserve 是 Apply 耗时观测钩子。用 atomic.Pointer 而非裸函数变量：
// 这个钩子由装配阶段的 main goroutine 写、由 raft apply 等后台 goroutine 读，
// 二者的先后没有任何跨文件手段能可靠保证——集群档下 cluster.Manager.Start
// 拉起的 apply goroutine 就跑在 metrics 装配之前（2026-08-14 由带 -race 的
// broker 实测抓到，backlog B14）。原先靠「装配阶段设置一次、之后只读」这条
// 注释约定维系，而它已被一次看起来无害的重排打破。不变量交给类型，不交给
// cmd/sq/main.go 里的语句顺序。
var onApplyObserve atomic.Pointer[func(time.Duration)]

// SetApplyObserver 设置 Apply 耗时观测钩子；传 nil 清除。
//
// 并发安全：可在进程任意时刻调用，读侧看到的要么是旧钩子要么是新钩子。
// 不保证「设置后立刻对所有在途 Apply 生效」——观测是尽力而为的，
// 漏掉切换瞬间的少数样本不影响直方图语义。
func SetApplyObserver(f func(d time.Duration)) {
	if f == nil {
		onApplyObserve.Store(nil)
		return
	}
	onApplyObserve.Store(&f)
}

// SwapApplyObserver 设置新钩子并返回旧钩子，供测试保存/还原现场。
// 传 nil 表示清除并取回旧值；无旧值时返回 nil。
func SwapApplyObserver(f func(d time.Duration)) func(d time.Duration) {
	var next *func(time.Duration)
	if f != nil {
		next = &f
	}
	if old := onApplyObserve.Swap(next); old != nil {
		return *old
	}
	return nil
}

// observeApply 读路径：非 nil 才回调。
// 这里刻意不打日志——它跑在每次成功提交之后，是热路径。
func observeApply(d time.Duration) {
	if p := onApplyObserve.Load(); p != nil {
		(*p)(d)
	}
}
```

import 块补 `"sync/atomic"`。

- [ ] **Step 4: 三个读点收敛**

三处一律替换成单行 `observeApply(...)`，不再各自判空：

`store.go:205`（`ApplyWith` 成功提交后，原 `if OnApplyObserve != nil { OnApplyObserve(time.Since(start)) }`）
→ `observeApply(time.Since(start))`

`store.go:299`（`ApplyAsync` 的 `!s.sync` 分支，同形态）
→ `observeApply(time.Since(start))`

`store.go:323`（`Pending.Wait` 的 `needSync` 分支，原用 `p.start`）
→ `observeApply(time.Since(p.start))`

三处上方原有的口径注释（「只观测成功提交」「观测口径与 Apply 一致」）**保留不动**——
它们说明的是语义，与本次改动无关。

- [ ] **Step 5: 跑测试确认通过**

Run: `go test -race -run TestApplyObserverConcurrentSetAndRead ./internal/store/ -v`
Expected: PASS，且输出中无 `WARNING: DATA RACE`

- [ ] **Step 6: 变异验证（承重，不做不算完）**

临时把三个函数一起改回裸全局形态（`SwapApplyObserver` 也要改，否则引用不到
`onApplyObserve` 会编译不过）：

```go
var onApplyObserveRaw func(time.Duration)

func SetApplyObserver(f func(d time.Duration)) { onApplyObserveRaw = f }

func SwapApplyObserver(f func(d time.Duration)) func(d time.Duration) {
	old := onApplyObserveRaw
	onApplyObserveRaw = f
	return old
}

func observeApply(d time.Duration) {
	if onApplyObserveRaw != nil {
		onApplyObserveRaw(d)
	}
}
```

（`Apply` 走的是 `ApplyWith(b, s.sync)`，所以 `:205` 那个读点在本用例里必然被走到，
读侧的并发压力是真的。）

Run: `go test -race -run TestApplyObserverConcurrentSetAndRead ./internal/store/`
Expected: **FAIL，且输出含 `WARNING: DATA RACE`**

把变异后的失败输出（含 race 报告前 20 行）贴进 ledger 作为取证，然后
`git checkout -- internal/store/store.go` 还原，`git status --porcelain` 确认干净。

**若变异后仍然 PASS：停下来上报 BLOCKED。** 不要改断言、不要加 sleep 迁就。

- [ ] **Step 7: 加关键节点日志**

本 task 的日志决策要写清楚，不是「不加」而是「刻意不加，且说明为什么」：

- `observeApply` **不打任何日志**：它跑在每次成功提交之后，是热路径，
  加日志会把观测本身变成开销。已在 Step 3 的注释里写明。
- `SetApplyObserver` / `SwapApplyObserver` **不打日志**：`store` 包不持有 logger，
  为一次装配动作把 logger 穿进来不值得；装配可见性由调用方负责——
  `metrics.NewRegistry` 已有 `logger.Info("metrics registry 已装配", "mod", "metrics")`
  覆盖这一节点（Task 2 会确认它仍在）。

- [ ] **Step 8: 加注释**

确认以下三处注释已就位（Step 3 已写，此步是自检）：
- `onApplyObserve` 变量头：为什么用 atomic 而不是裸变量，附 B14 的实测出处
- `SetApplyObserver` / `SwapApplyObserver`：并发语义与「不保证立刻生效」的边界
- `observeApply`：为什么热路径不打日志
- 新建的 `store_observer_test.go` 文件头：职责 + 边界（不验证观测值准确性）

- [ ] **Step 9: 跑本包全量**

Run: `go test -race ./internal/store/`
Expected: PASS（`store_test.go`、`store_readview_test.go`、`keys_test.go` 全过）

- [ ] **Step 10: Commit**

```bash
git add internal/store/store.go internal/store/store_observer_test.go
git commit -m "fix(store): Apply 观测钩子改 atomic.Pointer，消除集群档启动期数据竞态"
```

**本次提交后 `internal/cluster` 的测试会编译不过**（`group_test.go` 仍在引用被删掉的
`store.OnApplyObserve`），这是**预期的**，Task 2 Step 1 就是要看到这个失败。

**绝对不要**为了让它编译过而保留 `OnApplyObserve` 作为兼容别名或 deprecated 变量——
留着它就等于留着那条随时会被重新用上的裸全局，本次修复的意义归零。编译器报错
正是我们要的：它替我们把所有遗漏的调用方点出来。

---

### Task 2: 迁移调用方与两处过时注释

**Files:**
- Modify: `internal/metrics/collector.go:190`
- Modify: `internal/metrics/metrics_test.go:4`（文件头注释）
- Modify: `internal/cluster/group_test.go:776-785`（`countApplyCommits`）
- Modify: `cmd/sq/main.go:385-388`（注释）

**Interfaces:**
- Consumes: Task 1 产出的 `store.SetApplyObserver` / `store.SwapApplyObserver`
- Produces: 无新符号。本 task 完成后 `grep -rn "OnApplyObserve" internal/ cmd/` 应零命中
  （注释里作为历史称呼提及不算）。

- [ ] **Step 1: 确认编译失败点**

Run: `go build ./... && go vet ./...`
Expected: 失败，`OnApplyObserve` undefined，命中 `internal/metrics/collector.go`
与 `internal/cluster/group_test.go`（后者需 `go vet` 才暴露）

这一步是刻意的：Task 1 删掉了导出变量，编译器负责把所有遗漏调用方找出来。

- [ ] **Step 2: 迁移 metrics 装配点**

`internal/metrics/collector.go:190`：

```go
	store.SetApplyObserver(func(d time.Duration) { h.Observe(d.Seconds()) })
```

`NewRegistry` 的函数头注释（`collector.go:174-176`）原文说「只能在装配阶段调用一次
（会写 store.OnApplyObserve 包级钩子）」，改为：

```go
// NewRegistry 组装进程级指标注册表并挂接 store.Apply 耗时直方图。
// 进程内只应调用一次：它会通过 store.SetApplyObserver 抢占进程级的 Apply
// 观测钩子，调用两次后一次会静默覆盖前一次的直方图。钩子本身并发安全
// （见 store.SetApplyObserver），因此本函数相对后台 goroutine 的启动顺序
// 不影响正确性——只影响装配完成前那一小段时间的样本会不会被记录。
// tx/conns 允许为 nil（测试与降级场景跳过事务/连接指标）。
```

- [ ] **Step 3: 迁移 cluster 测试的钩子存还原**

`internal/cluster/group_test.go:776-785`，`countApplyCommits` 改为：

```go
// countApplyCommits 临时接管 Apply 观测钩子统计引擎提交次数，
// 测试结束恢复原值。applyEntries 是同步调用，计数无并发。
func countApplyCommits(t *testing.T) *int {
	t.Helper()
	n := new(int)
	old := store.SwapApplyObserver(func(time.Duration) { *n++ })
	t.Cleanup(func() { store.SwapApplyObserver(old) })
	return n
}
```

- [ ] **Step 4: 改掉 main.go 那段作废的契约注释**

`cmd/sq/main.go:385-388` 现有注释断言「metrics registry 必须先于任何后台 goroutine
装配」，这条保证在集群档下从来就没成立过（`m.Start` 在 `:299`）。整段替换为：

```go
	// metrics registry 的装配位置不再承担正确性责任：它通过
	// store.SetApplyObserver 挂钩子，而那个钩子本身是并发安全的
	// （atomic.Pointer，见 store 包）。此处曾要求「必须先于任何后台
	// goroutine 装配」，但集群档下 cluster.Manager.Start（上方 m.Start）
	// 拉起的 apply goroutine 一直跑在这之前，那条契约实际从未成立——
	// 2026-08-14 由带 -race 的 broker 实测抓到（backlog B14）。
	// 唯一的后果是：装配完成之前发生的 Apply 不会被记入直方图。
	// admin_listen 为空 = 不装配（钩子保持 nil，Apply 路径零开销）。
```

- [ ] **Step 5: 更新 metrics_test.go 的过时理由**

`internal/metrics/metrics_test.go:4` 现有注释把「不用 `t.Parallel()`」归因于会设置包级钩子
（暗指数据竞态）。并发写现在已安全，真实理由要换成共享状态互相覆盖：

```go
// 不用 t.Parallel()：NewRegistry 会通过 store.SetApplyObserver 抢占进程级的
// Apply 观测钩子（钩子本身并发安全，但只有一份），并行跑的用例会互相覆盖
// 对方的观测目标，断言就落到别人的直方图上了。
```

- [ ] **Step 6: 确认零残留并跑三包**

```bash
grep -rn "OnApplyObserve" internal/ cmd/    # 只应命中注释里的历史称呼
go test -race ./internal/store/ ./internal/metrics/ ./internal/cluster/
```
Expected: 三包全 PASS

- [ ] **Step 7: 加关键节点日志**

本 task 不新增日志，但要**确认既有装配节点日志仍在**：
`metrics.NewRegistry` 结尾的 `logger.Info("metrics registry 已装配", "mod", "metrics")`
必须保留——它是「钩子已挂上」这一状态变更在日志里的唯一痕迹，删了就没人能
从日志判断直方图从哪一刻开始有数据。

- [ ] **Step 8: 加注释**

Step 2 / 4 / 5 三处注释即本 task 的注释交付物，逐条自检：
- 每处都解释了「为什么」（为什么只调一次、为什么顺序不再重要、为什么不能并行）
- 没有一处是复述代码

- [ ] **Step 9: Commit**

```bash
git add internal/metrics/collector.go internal/metrics/metrics_test.go \
        internal/cluster/group_test.go cmd/sq/main.go
git commit -m "refactor(metrics,cluster,cmd): 迁移到 SetApplyObserver，订正作废的顺序契约注释"
```

---

### Task 3: 本地恢复抬任期日志按调用方分级

**Files:**
- Modify: `internal/cluster/raftstore.go:1155`（`bumpTermsInto` 签名）、`:1191`（日志）、
  `:1217`、`:1239`（两个调用点）
- Test: `internal/cluster/raftstore_test.go`（新增 2 条用例 + 捕获型 handler）

**Interfaces:**
- Consumes: 无（与 Task 1/2 完全独立，可并行审阅）
- Produces: 包内新类型 `termBumpReason` 与两个常量 `bumpLocalResume` / `bumpForcedRecover`；
  `bumpTermsInto` 签名变为 `func (r *raftStore) bumpTermsInto(dataGroups uint32, reason termBumpReason) error`

- [ ] **Step 1: 写失败的判别器**

在 `internal/cluster/raftstore_test.go` 末尾追加：

```go
// captureHandler 收集日志记录的级别与消息，供级别断言使用。
// 只留断言需要的两个字段——不做通用日志断言框架。
type captureHandler struct {
	mu   *sync.Mutex
	recs *[]slog.Record
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, r.Clone())
	return nil
}
func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

// newCaptureLogger 返回 logger 与取记录的闭包。
func newCaptureLogger() (*slog.Logger, func() []slog.Record) {
	var mu sync.Mutex
	recs := new([]slog.Record)
	lg := slog.New(captureHandler{mu: &mu, recs: recs})
	return lg, func() []slog.Record {
		mu.Lock()
		defer mu.Unlock()
		return append([]slog.Record(nil), *recs...)
	}
}

// levelOf 找出第一条包含 substr 的记录的级别；找不到返回 (0,false)。
func levelOf(recs []slog.Record, substr string) (slog.Level, bool) {
	for _, r := range recs {
		if strings.Contains(r.Message, substr) {
			return r.Level, true
		}
	}
	return 0, false
}

// seedHardStates 造出「组 0..dataGroups 各有一个带 term/vote 的 HardState」的现场。
func seedHardStates(t *testing.T, rs *raftStore, dataGroups uint32) {
	t.Helper()
	for g := uint32(0); g <= dataGroups; g++ {
		term, vote, commit := uint64(7), uint64(2), uint64(11)
		hs := &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
		if err := rs.Persist(g, hs, nil, true); err != nil {
			t.Fatalf("组 %d 造 HardState: %v", g, err)
		}
	}
}

// TestLocalResumeBumpLogsWarn【承重】mem 档常规重启的抬任期必须是 WARN。
//
// 这条路径是 manager.go 注释写明的预期动作（mem 档投票走 NoSync 可能未落盘），
// 后果只是多一轮选举。打成 ERROR 会训练运维忽略 ERROR——2026-08-14 跨机集群
// 验证时每次 kill -9 重启都刷 4 条 ERROR，正是这个问题（backlog B15）。
func TestLocalResumeBumpLogsWarn(t *testing.T) {
	lg, dump := newCaptureLogger()
	st := mustOpenStore(t, t.TempDir())
	rs := newRaftStore(st, lg)
	const groups = uint32(3)
	seedHardStates(t, rs, groups)

	if err := rs.BumpTermsForLocalResume(groups); err != nil {
		t.Fatalf("BumpTermsForLocalResume: %v", err)
	}
	lvl, ok := levelOf(dump(), "任期已抬、投票已清")
	if !ok {
		t.Fatal("没找到抬任期日志——这条路径必须留痕，静默抬任期无从排查")
	}
	if lvl != slog.LevelWarn {
		t.Fatalf("级别是 %v，期望 WARN；常规重启打 ERROR 会淹没真正需要关注的告警", lvl)
	}
}

// TestForcedRecoverBumpLogsError【承重】签字放行的抬任期必须保持 ERROR。
//
// 与上一条相反的方向：--grant 意味着运维已接受「可能丢已确认消息」，
// 与它同属许可路径的另外两条日志也都是 Error。一刀切降 WARN 会把真正
// 该喊的这条一起压掉，所以本用例和上一条必须成对存在。
func TestForcedRecoverBumpLogsError(t *testing.T) {
	lg, dump := newCaptureLogger()
	st := mustOpenStore(t, t.TempDir())
	rs := newRaftStore(st, lg)
	const groups = uint32(3)
	seedHardStates(t, rs, groups)
	if err := rs.SaveRecoverPermit(recoverPermit{GrantedAt: "t", Gen: "gen-b"}); err != nil {
		t.Fatalf("SaveRecoverPermit: %v", err)
	}

	if err := rs.ForceLocalRecover(groups); err != nil {
		t.Fatalf("ForceLocalRecover: %v", err)
	}
	lvl, ok := levelOf(dump(), "任期已抬、投票已清")
	if !ok {
		t.Fatal("没找到抬任期日志")
	}
	if lvl != slog.LevelError {
		t.Fatalf("级别是 %v，期望 ERROR；签字放行可能丢已确认消息，不能降级", lvl)
	}
}
```

import 块按需补 `"context"`、`"strings"`、`"sync"`（`log/slog`、`raftpb`、`testing` 已在用）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestLocalResumeBumpLogsWarn|TestForcedRecoverBumpLogsError' ./internal/cluster/ -v`
Expected: `TestLocalResumeBumpLogsWarn` FAIL（级别是 ERROR，期望 WARN）；
`TestForcedRecoverBumpLogsError` PASS（现状本就是 ERROR）

- [ ] **Step 3: 实现分级**

`internal/cluster/raftstore.go`，在 `bumpTermsInto` 之前加类型：

```go
// termBumpReason 区分抬任期的两种来路。它们的严重性差一个数量级，
// 日志级别必须跟着分开：常规重启是预期动作（代价只是多一轮选举），
// 签字放行则意味着运维已接受可能丢已确认消息。
type termBumpReason uint8

const (
	bumpLocalResume   termBumpReason = iota // mem 档常规重启（pathLocalResume）
	bumpForcedRecover                       // sq recover --grant 签字放行
)
```

签名改为 `func (r *raftStore) bumpTermsInto(dataGroups uint32, reason termBumpReason) error`，
循环内 `:1191` 那行日志替换为：

```go
		// 同一个动作、两种严重性：级别与文案都按来路分开，让读日志的人
		// 一眼看出这是常规重启还是签字放行。
		if reason == bumpForcedRecover {
			r.lg.Error("签字放行的本地恢复：任期已抬、投票已清",
				"g", g, "term", newTerm, "legacy", pending)
		} else {
			r.lg.Warn("mem 档本地恢复：任期已抬、投票已清（投票记录走 NoSync 可能未落盘，"+
				"抬任期是预期动作，代价是多一轮选举）",
				"g", g, "term", newTerm, "legacy", pending)
		}
```

两个调用点：`:1217`（`ForceLocalRecover`）传 `bumpForcedRecover`；
`:1239`（`BumpTermsForLocalResume`）传 `bumpLocalResume`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -run 'TestLocalResumeBumpLogsWarn|TestForcedRecoverBumpLogsError' ./internal/cluster/ -v`
Expected: 两条都 PASS

- [ ] **Step 5: 变异验证（承重，不做不算完）**

把 Step 3 的分支临时改回一律 Error（删掉 `if/else`，只留 `r.lg.Error("不干净关机后本地恢复：任期已抬、投票已清", ...)`）。

Run: `go test -run TestLocalResumeBumpLogsWarn ./internal/cluster/`
Expected: **FAIL**，报「级别是 ERROR，期望 WARN」

把失败输出贴进 ledger，然后 `git checkout -- internal/cluster/raftstore.go` 还原，
`git status --porcelain` 确认干净。**若仍 PASS：停下来上报 BLOCKED。**

- [ ] **Step 6: 加关键节点日志**

本 task 的交付物本身就是日志，此步的自检项：
- 两条分支**都**留痕，没有任何一条路径静默抬任期（Step 1 的用例已用
  「没找到抬任期日志」这条断言兜住）
- 两条都保留了既有结构化字段 `g` / `term` / `legacy`，没有为了改文案丢字段
- 文案本身说清了「为什么会走到这里」与「后果是什么」，不是只说「做了什么」

- [ ] **Step 7: 加注释**

- `termBumpReason` 类型头：为什么要分两种来路（严重性差一个数量级）
- 两个常量各自的行尾注释：对应哪条恢复路径
- 日志分支上方：为什么同一个动作要分两种级别
- 两条新用例的文档注释：各自钉住什么、为什么必须成对存在

- [ ] **Step 8: 跑本包全量**

Run: `go test -race ./internal/cluster/`
Expected: PASS。特别确认既有的 `TestForceLocalRecoverBumpsTermAndConsumesPermit`
与 `TestForceLocalRecoverBumpsLegacyHardStateOnUnmigratedDisk` 未被破坏——
它们断言的是抬任期的**行为**，本 task 只动日志，这两条必须原样通过。

- [ ] **Step 9: Commit**

```bash
git add internal/cluster/raftstore.go internal/cluster/raftstore_test.go
git commit -m "fix(cluster): 本地恢复抬任期日志按调用方分级——常规重启 WARN、签字放行 ERROR"
```

---

### Task 4: 全量回归与改动面核验

**Files:** 不改代码，只跑与核验。

**Interfaces:**
- Consumes: Task 1/2/3 的全部产出
- Produces: 交给协调者的验收取证

- [ ] **Step 1: 全量 race**

Run: `go test -race -timeout 20m ./...`
Expected: 全部包 PASS，输出中零 `WARNING: DATA RACE`

- [ ] **Step 2: 改动面越界检查**

Run: `git diff --name-only main...HEAD`
Expected: 只出现 Global Constraints 白名单里的 8 个文件（`store_observer_test.go` 为新增）。
多一个文件都要在 ledger 里写明理由。

- [ ] **Step 3: 残留检查**

Run: `grep -rn "OnApplyObserve" internal/ cmd/`
Expected: 只命中注释中作为历史称呼的提及；无任何代码引用。

- [ ] **Step 4: 构建核验**

Run: `go build ./... && go vet ./...`
Expected: 均无输出

- [ ] **Step 5: 把两次变异验证的取证汇总进 ledger**

Step 6（Task 1）与 Step 5（Task 3）的失败输出各留一段，标明「掐掉什么 → 哪条用例变红 → 已还原且工作区干净」。
这两段是本计划最重要的交付物之一：没有它们，「测试全绿」只证明测试跑过了，
不证明测试有判别力。

---

## 交付边界（协调者会做，实现方不要碰）

带 `-race` 的 broker 跑全量 e2e 属于收尾验证，由协调者在临时机上做，**不在本计划范围**：
它需要 `make web`（否则 `TestConsoleServedFromBinary` 会因 `web/dist` 不入库而独立失败）
与一台能跑 40 分钟的机器。实现方只需保证 Task 4 的四项全过。
