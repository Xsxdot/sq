# B14 + B15 设计：Apply 观测钩子并发安全化 + 本地恢复日志分级

**日期**：2026-08-14
**backlog**：B14（高）、B15（低）
**基线**：`main`（两条缺陷在 main 上原样存在，与七条未合分支无关）

## 1. 背景

2026-08-14 的补验中，用 `go build -race` 构建的真 broker 跑 e2e，挖出两个问题。
单测 `-race`（18/18 包）全绿，没能发现其中任何一个——它们都只在「真 broker 走完整启动
编排、且是集群档」时才暴露。

取证见 [四块补验记录](../notes/2026-08-14-four-gaps-verification.md)。

### 1.1 B14：`store.OnApplyObserve` 的启动期数据竞态

20 次 race 报告全部指向同一个地址、同一对调用栈：

```
Write at 0x24ea568 by main goroutine:
  metrics.NewRegistry()            internal/metrics/collector.go:190
  main.run()                       cmd/sq/main.go:389

Previous read at 0x24ea568 by goroutine 48:
  store.(*Store).ApplyWith()       internal/store/store.go:205
  cluster.(*group).applyEntries()  internal/cluster/group.go:1506
  cluster.(*group).runApply()      internal/cluster/group.go:838
```

竞争对象是包级变量 `store.OnApplyObserve`（`internal/store/store.go:31`）。

**根因不是忘了加锁，是启动顺序违反了代码自己写下的契约。** `cmd/sq/main.go:385-388`
写着「metrics registry 必须先于任何后台 goroutine 装配」，但那段推理只点了 retention/delay
两类 goroutine，漏了集群档的 raft apply goroutine——`m.Start(runCtx)` 在 `main.go:299`
就把它们拉起来了，比 `NewRegistry`（`main.go:389`）早 90 行。单机档没有这条路径，
所以问题只在集群档暴露。

**这条契约守不住，是本设计的出发点。** 它是一条纯靠注释维系的跨文件顺序约定：
`store` 里声明的不变量，要靠 `cmd/sq/main.go` 里几百行之外的语句顺序来保证，
中间任何一次看起来无害的重排都能打破它——事实上已经打破过一次。因此修法不是
「把顺序改对」，而是**把不变量从注释搬进类型**。

**连带影响**：race detector 让 broker 以 `exit status 66` 退出，而 e2e harness 断言
「SIGTERM 后必须干净退出（码 0）」（`cluster_proc_test.go:288`），于是 10 条集群/场景
用例一起判失败。它们不是 10 个独立故障，是这一个 bug 的下游。

### 1.2 B15：正常恢复路径打 ERROR

跨机集群 `kill -9` 后重启，每组一条、共 4 条：

```
level=ERROR msg="不干净关机后本地恢复：任期已抬、投票已清" mod=raftstore g=0 term=3
```

出处 `internal/cluster/raftstore.go:1191`。走的是 `pathLocalResume` + quorum-mem，
是 `manager.go:466` 注释写明的**预期路径**（mem 档投票走 NoSync 可能未落盘，所以抬任期），
后果只是多一轮选举。把 documented 的正常恢复打成 ERROR，会训练运维忽略 ERROR。

**但不能一刀切降级**：`bumpTermsInto` 被两个语义完全不同的调用方共用——

| 调用方 | 语义 | 该用的级别 |
|---|---|---|
| `BumpTermsForLocalResume`（`raftstore.go:1238`，被 `manager.go:467` 调） | mem 档常规重启，无害 | WARN |
| `ForceLocalRecover`（`raftstore.go:1216`，被 `manager.go:434` 调） | `sq recover --grant` 签字放行，**可能丢已确认消息** | ERROR |

两者都只经由 `bumpTermsInto`（`raftstore.go:1155`）落地，调用点分别在 `:1239` 与 `:1217`。

后者周围的 `raftstore.go:1079`、`:1228` 两条同属许可路径的日志都是 Error；
一律降 WARN 会把真正该喊的那条一起压掉。

## 2. 目标与非目标

**目标**

1. `store` 的 Apply 观测钩子在任意时刻读写都不构成数据竞态，且该性质由类型保证，
   不依赖调用方的顺序纪律。
2. 本地恢复抬任期的日志按调用方分级：常规重启 WARN、签字放行 ERROR。
3. 两条都留下能变异验证的自动化判别器。

**非目标**

- 不改 `sq_store_apply_duration_seconds` 的指标语义、桶划分、观测口径。
- 不重构 `cmd/sq/main.go` 的启动编排。修完之后 `NewRegistry` 相对 `m.Start` 的位置
  **不再重要**，这正是本方案要达到的效果。
- 不处理 `internal/cluster` 那条偶发失败（B12 的遗留观察项），两者无关。
- 不追求「消灭全部包级可变状态」。`grep` 确认 `OnApplyObserve` 是此类模式的唯一一个，
  没有同类要顺带清理。

## 3. B14 设计

### 3.1 store 侧 API

`internal/store/store.go`，包级变量改为私有原子指针，对外只暴露两个函数：

```go
// onApplyObserve 是 Apply 耗时观测钩子。用 atomic.Pointer 而非裸函数变量：
// 这个钩子由装配阶段的 main goroutine 写、由 raft apply / produce 等后台
// goroutine 读，二者的先后没有任何跨文件手段能可靠保证——集群档下
// cluster.Manager.Start 拉起的 apply goroutine 就跑在 metrics 装配之前
// （2026-08-14 由带 -race 的 broker 实测抓到）。把不变量交给类型，
// 而不是交给 cmd/sq/main.go 里的语句顺序。
var onApplyObserve atomic.Pointer[func(time.Duration)]

// SetApplyObserver 设置 Apply 耗时观测钩子；传 nil 清除。
//
// 并发安全：可在进程任意时刻调用，读侧看到的要么是旧钩子要么是新钩子，
// 不会看到撕裂值。不保证「设置后立刻对所有在途 Apply 生效」——观测是
// 尽力而为的，漏掉切换瞬间的少数样本不影响直方图语义。
func SetApplyObserver(f func(d time.Duration))

// SwapApplyObserver 设置新钩子并返回旧钩子，供测试保存/还原现场。
// 传 nil 表示清除并取回旧值。
func SwapApplyObserver(f func(d time.Duration)) func(d time.Duration)

// observeApply 包内读路径：Load 后非 nil 才调用。
func observeApply(d time.Duration)
```

实现要点：`atomic.Pointer[T]` 存的是 `*T`，因此 `Set` 内部对入参取地址存指针，
`f == nil` 时存 `nil` 指针；`observeApply` 先 `Load()` 判空再解引用调用。

### 3.2 读点收敛

三个读点全部改调 `observeApply`，不再各自判空：

| 位置 | 现状 |
|---|---|
| `store.go:205` | `ApplyWith` 成功提交后 |
| `store.go:299` | `ApplyAsync` 的 `!s.sync` 分支 |
| `store.go:323` | `Pending.Wait` 的 `needSync` 分支 |

三处的观测口径（覆盖「提交开始 → 持久化完成」全程）与「只观测成功提交」的既有约定
一律不变，只换取值方式。

### 3.3 调用方改动

| 文件 | 改动 |
|---|---|
| `internal/metrics/collector.go:190` | `store.OnApplyObserve = f` → `store.SetApplyObserver(f)` |
| `internal/cluster/group_test.go:781-783` | 存还原改用 `SwapApplyObserver` |
| `internal/store/store.go:28-31` | 旧契约注释（「装配阶段设置一次、之后只读——据此不加锁」）作废，按 3.1 重写 |
| `cmd/sq/main.go:385-388` | **必须改**：那段注释断言的顺序保证已不成立，留着会让下一个人继续信一份错的契约。改为说明钩子自身并发安全、装配位置不再承担正确性责任 |
| `internal/metrics/metrics_test.go:4` | 该处注释说「不用 t.Parallel() 是因为会设置包级钩子」。理由要更新：并发写本身已安全，不能并行的真实原因是多个测试共用同一个进程级钩子会互相覆盖观测目标 |

### 3.4 为什么不选另外两条路

- **只修 main.go 的启动顺序**（把 registry 装配拆两段，钩子在 `m.Start` 前装好）：
  读路径零开销、API 不动，但正确性仍然押在顺序纪律上——而这条纪律今天已被证明守不住。
- **钩子改成 `store.Open` 的入参**：依赖方向最干净，但要改 Store 构造签名，
  所有建 store 的测试都得动，改动面与收益不成比例。

原子 load 的代价放在这条路径上可以忽略：同一次调用里已经有一次 Pebble 批次提交
（同步档还含 fsync），量级是微秒到毫秒，而原子 load 是纳秒级。

## 4. B15 设计

`bumpTermsInto` 增加一个原因形参，日志保留在循环内（保住每组的 `g` 与 `newTerm`）：

```go
// termBumpReason 区分抬任期的两种来路——它们的严重性差一个数量级，
// 日志级别必须跟着分开：常规重启是预期动作，签字放行意味着运维已接受丢数据风险。
type termBumpReason uint8

const (
    bumpLocalResume   termBumpReason = iota // mem 档常规重启（pathLocalResume）
    bumpForcedRecover                       // sq recover --grant 签字放行
)
```

调用点：`BumpTermsForLocalResume` 传 `bumpLocalResume`，`ForceLocalRecover` 传
`bumpForcedRecover`。

日志分流（`raftstore.go:1191` 一处替换为二选一）：

- `bumpLocalResume` → **WARN**，文案：`"mem 档本地恢复：任期已抬、投票已清（投票记录走 NoSync 可能未落盘，抬任期是预期动作，代价是多一轮选举）"`
- `bumpForcedRecover` → **ERROR**，文案：`"签字放行的本地恢复：任期已抬、投票已清"`

两条都保留既有字段 `g` / `term` / `legacy`。

抬任期的行为本身（写回 legacy hsKey 还是走 `r.Persist`、逐组 sync 落盘、幂等性）
一个字不改——本条只动日志级别与文案。

## 5. 测试

### 5.1 B14 判别器（承重）

`internal/store` 新增并发用例：N 个 goroutine 持续调 `Apply`，同时另一个 goroutine
反复 `SetApplyObserver`（在非 nil 与 nil 之间切换）。

**变异验证（必须做，不做不算完）**：把实现退回裸函数全局变量，该用例在 `-race` 下
必须变红。若退回后仍绿，说明用例没有真正制造并发读写，**停下来报告，不许改断言迁就**。

### 5.2 B15 判别器（承重）

用捕获型 `slog.Handler` 收日志，两条用例：

- 走 `BumpTermsForLocalResume` → 断言该条记录的 Level 是 `slog.LevelWarn`
- 走 `ForceLocalRecover` → 断言是 `slog.LevelError`

**变异验证**：把分级改回一律 Error，第一条必须变红。

### 5.3 回归锚

`internal/store`、`internal/metrics`、`internal/cluster` 三包除本设计列明的改动外
零修改通过；`go test -race ./...` 全绿。

## 6. 验收

实现侧（进 plan）：

1. `go test -race ./...` 18/18 包全绿。
2. 5.1、5.2 两个判别器各自完成变异验证并留下取证。
3. `git diff --name-only` 的改动面不超出 §3.3 + §4 + 新增测试文件。

收尾侧（**不进 plan，由协调者在临时机上做**）：

4. `go build -race` 构建 broker，跑全量 e2e。预期 `WARNING: DATA RACE` 归零，
   §1.1 提到的 10 条场景用例 FAIL 一并消失。
   这才是 B14 的完整验收证据——5.1 的单测判别器只能证明钩子自身安全，
   证明不了集群档整条启动路径干净。
   注意 `TestConsoleServedFromBinary` 需要先 `make web`，否则它会因为
   `web/dist` 不入库而独立失败，与本次修复无关。

## 7. 已定事项（不要重新议）

- B14 走 `atomic.Pointer`，不走「只修 main.go 顺序」也不走「改 `store.Open` 签名」——
  理由见 §3.4。
- B15 按调用方分级，不一刀切降 WARN——理由见 §1.2。
- 基线 `main`，不叠在 `verify/final-integration` 或任何未合并分支上。
