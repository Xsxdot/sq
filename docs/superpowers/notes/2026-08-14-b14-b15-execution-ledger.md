# B14+B15 实现 ledger

> 执行纪律：每 task 完成、每轮修复各追加一行，含 commit 范围。恢复现场以本文件 + git log 为准。
> 本文件作为协调可见的取证载体随分支提交（Task 4 Step 2 的改动面核验以此为白名单外文件作理由）。

## 基线

- 分支：`fix/b14-b15-observer-and-loglevel`
- 基线 commit：`eaf537d`（含 spec/plan/backlog 三个 docs commit，基线 = main）
- 环境：Go 1.26.1，darwin/arm64

---

（task 执行记录从下方开始追加）

## Task 1: store 侧钩子改 atomic.Pointer

**Step 2 判别器先失败确认**：写判别器后、实现前 `go test -race -run TestApplyObserverConcurrentSetAndRead ./internal/store/` 编译失败 `undefined: SetApplyObserver`，符合预期。

**Step 5 通过确认**：实现 atomic.Pointer + 三读点收敛后，`-race` 下该用例 16/16 稳定 PASS、无 DATA RACE。
（途中曾出现 3 次「观测计数为 0」FAIL，发生在机器刚从进程围栏挤掉恢复、负载极高时；负载回落后连续 16 次通过，且变异验证证明写读竞争真实存在，判定为调度时序现象而非判别器失效。观察项记账，不阻断。）

**变异验证取证（承重）**：
- 掐掉什么：`SetApplyObserver` / `SwapApplyObserver` / `observeApply` 三个函数临时改回裸全局 `var onApplyObserveRaw func(time.Duration)`（三个一起改，否则编译不过）。
- 哪条用例变红：`TestApplyObserverConcurrentSetAndRead`，-race 下 FAIL 且含 `WARNING: DATA RACE`。
- 已还原：用备份还原 atomic.Pointer 版本（变异前备份到 temp；不用 git checkout，因 atomic 版本尚未提交），`grep onApplyObserveRaw` 零命中、`grep atomic.Pointer` 命中 2 处，`git status --porcelain` 干净。

race 报告（前 20 行）：

```
WARNING: DATA RACE
Read at 0x00010426bbc8 by goroutine 32:
  github.com/xushixin/sq/internal/store.observeApply()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store.go:46 +0x194
  github.com/xushixin/sq/internal/store.(*Store).ApplyWith()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store.go:223 +0x178
  github.com/xushixin/sq/internal/store.(*Store).Apply()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store.go:287 +0x1c8
  github.com/xushixin/sq/internal/store.TestApplyObserverConcurrentSetAndRead.func2()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store_observer_test.go:52 +0x1cc
  github.com/xushixin/sq/internal/store.TestApplyObserverConcurrentSetAndRead.gowrap2()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store_observer_test.go:57 +0x38

Previous write at 0x00010426bbc8 by goroutine 11:
  github.com/xushixin/sq/internal/store.SetApplyObserver()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store.go:37 +0x3a8
  github.com/xushixin/sq/internal/store.TestApplyObserverConcurrentSetAndRead()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store_observer_test.go:63 +0x344
```

（完整输出存 `/var/folders/xc/hpx9c9w153j7tvphw53lc8qr0000gn/T/opencode/task1-mutation-race.txt`）

**Step 7 日志决策**：`observeApply` 刻意不打日志（热路径，加日志把观测变成开销，注释已写明）；`SetApplyObserver`/`SwapApplyObserver` 不打日志（store 包不持有 logger，装配可见性由调用方 metrics.NewRegistry 的 Info 日志负责）。**刻意不加**，非遗漏。

**Step 9 本包全量 — 发现判别器用例不稳定（已上报裁决，未改代码）**：
- 隔离单测 `-run TestApplyObserverConcurrentSetAndRead`：31/31、20/20 通过（机器负载低时）。
- 全量 `go test -race ./internal/store/`：6/6 确定性 FAIL，报 `store_observer_test.go:72: 观测计数为 0`。
- `-count=2` 隔离跑：1/2 FAIL。
- 根因：写侧 200 轮 `SetApplyObserver(fn)` 紧接 `SetApplyObserver(nil)`，非 nil 窗口是相邻两条语句间的纳秒级间隙；读侧每个 goroutine 每次 Pebble 提交（微秒级）后才读一次钩子。机器负载高时主 goroutine 打完整轮 toggle 循环期间读侧来不及完成任何一次提交后读取，观测计数落 0。用例命中非 nil 窗口纯靠调度运气，正确实现也可能红。
- 判别力本身已由变异验证证明充分（掐掉后必然 DATA RACE），**问题不在判别力，在于用例在正确实现下不稳定**，会拖垮 Task 4 全量回归。
- 处置：按纪律不擅改 plan 逐字指定的用例，上报协调者裁决。

**裁决（审核者，已批准）**：选 B——这是 plan 缺陷，非实现问题。否决「接受现状 + 红了就重跑」：11/11 红的用例会毒化 Task 4 并训练「红了就重跑」。按给定结构修改：写侧每轮换新非 nil 闭包、全程不切 nil，清 nil 只在循环结束后一次；固定 200 轮改为「至少 200 轮且已观测到至少一次，10 秒预算兜底」；末尾 `observed.Load()==0` 断言保留，失败文案补 10 秒预算说明；用例上方写明为何不切 nil 并附实测数字。改完重做 Step 6 变异验证。

**用例修改后实测**：隔离 5/5 PASS、全量 5/5 PASS。

**变异验证（改后重做，承重）**：
- 掐掉什么：`SetApplyObserver` / `SwapApplyObserver` / `observeApply` 三个函数改回裸全局 `onApplyObserveRaw`。
- 哪条用例变红：`TestApplyObserverConcurrentSetAndRead`，-race 下 FAIL 且含 `WARNING: DATA RACE` ×2。
- 已还原：备份还原，`grep onApplyObserveRaw` 零命中，`git status --porcelain` 干净。
- race 报告（前 20 行）：

```
WARNING: DATA RACE
Read at 0x000105573bc8 by goroutine 34:
  github.com/xushixin/sq/internal/store.observeApply()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store.go:46 +0x194
  github.com/xushixin/sq/internal/store.(*Store).ApplyWith()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store.go:223 +0x178
  github.com/xushixin/sq/internal/store.(*Store).Apply()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store.go:287 +0x1c8
  github.com/xushixin/sq/internal/store.TestApplyObserverConcurrentSetAndRead.func2()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store_observer_test.go:62 +0x1cc

Previous write at 0x000105573bc8 by goroutine 11:
  github.com/xushixin/sq/internal/store.SetApplyObserver()
      /Users/sycm/.handoff/worktrees/6864ab0e/internal/store/store.go:37 +0x3b4
  github.com/xushixin/sq/internal/store.TestApplyObserverConcurrentSetAndRead()
```

（完整输出存 `/var/folders/xc/hpx9c9w153j7tvphw53lc8qr0000gn/T/opencode/task1-mutation-race2.txt`）

**Commit**：`8e44325 fix(store): Apply 观测钩子改 atomic.Pointer，消除集群档启动期数据竞态`

**提交后预期编译失败确认**：`go vet ./internal/cluster/` 报 `internal/cluster/group_test.go:781:15: undefined: store.OnApplyObserve`，正是 plan 预言的编译器替我们找遗漏调用方。未保留任何兼容别名。Task 2 Step 1 将再确认。

## Task 2: 迁移调用方与两处过时注释

**Step 1 编译失败确认**：`go build ./...` 报 `internal/metrics/collector.go:190:8: undefined: store.OnApplyObserve`；`go vet ./...` 额外报 `internal/cluster/group_test.go:781:15: undefined: store.OnApplyObserve`（仅 vet 暴露）。与预期一致。

**实现与验证**：collector.go 装配点改 `store.SetApplyObserver`；group_test.go `countApplyCommits` 改 `SwapApplyObserver` 存还原；main.go 作废契约注释替换；metrics_test.go 文件头理由改写。`grep -rn "OnApplyObserve" internal/ cmd/` 仅剩语义性历史称呼（store.go:241/271/348、store_observer_test.go:21）；三包 `-race` 全 PASS。装配日志 `logger.Info("metrics registry 已装配")` 保留。另收尾清零 collector.go:13 文件头一处失实的「写 OnApplyObserve」描述（白名单内，符合 Step 6 零残留目标）。

**审查**：spec 符合性通过 + 代码质量通过（双裁决）。

**Minor（记账，终审 triage）**：collector.go:13-14 括号「进程内只应调用一次（见 store 侧 SetApplyObserver 注释）」指向失准——store.go 的 SetApplyObserver 注释主张「可在进程任意时刻调用」，不为「只调一次」背书；该约束真正来源是 NewRegistry 自身语义。建议改为指向本函数语义或去掉括号。

**Commit**：`f89c51a refactor(metrics,cluster,cmd): 迁移到 SetApplyObserver，订正作废的顺序契约注释`

## Task 3: 本地恢复抬任期日志按调用方分级

**Step 2 判别器先失败确认**：新增两条用例后、实现前 `go test -run 'TestLocalResumeBumpLogsWarn|TestForcedRecoverBumpLogsError' ./internal/cluster/ -v` → `TestLocalResumeBumpLogsWarn` FAIL（级别是 ERROR，期望 WARN）；`TestForcedRecoverBumpLogsError` PASS（现状本就是 ERROR）。与预期一致。

**Step 3 实现**：raftstore.go 新增 `termBumpReason` 类型与 `bumpLocalResume`/`bumpForcedRecover` 两常量；`bumpTermsInto` 加 reason 形参；循环内日志按 `reason == bumpForcedRecover` 分流——Error「签字放行的本地恢复」/ Warn「mem 档本地恢复…抬任期是预期动作，代价是多一轮选举」，两条都保留 `g`/`term`/`legacy` 字段。`ForceLocalRecover` 传 `bumpForcedRecover`，`BumpTermsForLocalResume` 传 `bumpLocalResume`。抬任期行为（legacy hsKey 分流、逐组 Persist 落盘、commit 位点不动、许可消费顺序）零变化。

**Step 4 通过确认**：两条新用例 PASS。

**变异验证取证（承重）**：
- 掐掉什么：把分级分支临时改回一律 Error（删 if/else，只留 `r.lg.Error("不干净关机后本地恢复：任期已抬、投票已清", ...)`），保留 `_ = reason` 防编译失败。
- 哪条用例变红：`TestLocalResumeBumpLogsWarn` FAIL，报 `raftstore_test.go:697: 级别是 ERROR，期望 WARN；常规重启打 ERROR 会淹没真正需要关注的告警`。
- 已还原：变异前备份到 temp（`raftstore.go.task3.bak`），用 `cp` 还原（不用 git checkout，因分级实现尚未提交），备份与还原后文件 diff IDENTICAL，`git status --porcelain` 干净。
- 失败输出存 `/var/folders/xc/hpx9c9w153j7tvphw53lc8qr0000gn/T/opencode/task3-mutation.txt`。
- **若变异后仍 PASS 则 BLOCKED——未发生**。

**Step 8 本包全量**：`go test -race ./internal/cluster/` PASS（80.195s）。既有行为测试 `TestForceLocalRecoverBumpsTermAndConsumesPermit`、`TestForceLocalRecoverBumpsLegacyHardStateOnUnmigratedDisk` 原样通过。

**审查**：spec 符合性不通过（缺口：ledger 缺 Task 3 段，即本段）+ 代码质量通过。补写本段后复查。

**Minor（记账，终审 triage）**：task3-mutation.txt 只记 FAIL 文案未记变异具体改动行（FAIL 消息「级别是 ERROR，期望 WARN」已自证，可接受）；captureHandler 的 WithAttrs/WithGroup 丢弃 attrs/group，注释已声明「不做通用断言框架」，非问题。

**Commit**：`816e46c fix(cluster): 本地恢复抬任期日志按调用方分级——常规重启 WARN、签字放行 ERROR`
