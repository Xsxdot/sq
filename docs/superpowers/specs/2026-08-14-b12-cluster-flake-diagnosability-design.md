# B12：`internal/cluster` 偶发失败——可诊断性修复设计

> 关联 backlog：B12
> 日期：2026-08-14

## 1. 背景与问题定义

2026-08-11 合并 `v2-b2-seglog` 前的验收中，`go test ./...` 冷缓存首跑出现
`internal/cluster` FAIL（85.057s）。此后 **15 次真跑无一复现**：独占 4 次
（72–78s）、暖缓存全套 `-count=1` 3 次、独立 GOCACHE 冷缓存全套 2 次（精确
重造原失败条件）、8 路 CPU 满载下独跑 6 次。失败轮比通过轮慢 10%+，形态符合
时序敏感用例在 CPU 争抢下超时。

**关键困境：那次失败具体是哪条用例，已经不可考。** 原因有两条，且都不是
「测试本身写得不好」：

1. 首跑命令写成 `go test ./... | tail -60`。Pebble 每次 open 都往 **stderr**
   打若干行（`Found 0 WALs` 之类），15 个包 × 上百次 `store.Open` 的噪声灌满了
   那 60 行窗口，把 `--- FAIL: TestXxx` 挤出了输出。
2. 同一条管道把退出码换成了 `tail` 的 0——这次失败差点被整个漏掉。

于是 B12 的真实形态不是「有一个已知 bug 待修」，而是**「有一次不可复现的失败，
而当前的工具链保证了下一次失败同样不可诊断」**。

## 2. 目标与非目标

**目标：**

- G1：消除 Pebble 绕过项目 logger 直写 stderr 的噪声源——这是失败信息被淹没
  的直接原因，也是一处既有的可观测性漏洞。
- G2：把「测试输出永不过 `tail`/`head`、退出码不被管道吞掉」这条纪律固化进
  `Makefile`，让下一个人（或下一个 agent）不需要记得它。
- G3：做一次认真的复现尝试，并**如实记账**。

**非目标（明确不做）：**

- N1：**不承诺找到并修复那条 flaky 用例。** 它不可复现、名字不可考。任何
  「我修好了 B12」的声明都是不诚实的。本次的成功判据是「下一次失败必然留下
  用例名」，不是「不再失败」。
- N2：不重写 `internal/cluster` 的任何测试，不调整任何用例的超时常量。在不知道
  是哪条用例的前提下调超时是盲目打补丁。
- N3：不引入新依赖，不改动 raft/cluster 的生产逻辑。

## 3. 根因：Pebble 日志绕过项目 logger

`internal/store/store.go:47`：

```go
db, err := pebble.Open(dir, &pebble.Options{})
```

`Options.Logger` 为空。Pebble v2.1.6 在 `options.go` 的 `EnsureDefaults` 里把它
补成 `DefaultLogger`，而 `internal/base/logger.go` 对 `DefaultLogger` 的说明是
「logs to the Go stdlib logs」——即 `log.Printf`，终点是 **stderr**。

而 `store.Open` 的签名是：

```go
func Open(dir string, syncWrites bool, logger *slog.Logger) (*Store, error)
```

**它本来就收了一个 `*slog.Logger`，只是从来没把它交给 Pebble。** 这让修复变成
一处自包含的改动：不需要改签名，174 个调用点一个都不用动。

修复后的连带效果，正是 G1 想要的：`internal/cluster` 的测试传的是
`testSlog(t)`（`internal/cluster/raftstore_test.go:36`，写 `testWriter{t}` →
`t.Log`）。`t.Log` 的输出**只在用例失败时打印，且缩进归属到该用例名下**。所以
接入之后：

- 通过的用例：Pebble 噪声完全不出现。
- 失败的用例：噪声出现在 `--- FAIL: TestXxx` 之下、明确归属于它——从「淹没
  FAIL 行的干扰」变成「这条失败用例的现场上下文」。

## 4. 方案

三件事，互相独立，按序交付。

### 4.1 Pebble 日志接入项目 slog

新增 `internal/store/pebblelog.go`，实现 Pebble 的 `Logger` 接口
（`Infof(string, ...any)` / `Errorf(string, ...any)` / `Fatalf(string, ...any)`），
把调用转发到传入的 `*slog.Logger`；`store.Open` 里装配：

```go
db, err := pebble.Open(dir, &pebble.Options{Logger: newPebbleLogger(l)})
```

注意装配顺序：`l` 是 `logger.With("mod", "store")` 之后的实例，当前代码在
`pebble.Open` **之后**才求值。实现时需把 `l := logger.With("mod", "store")`
上移到 `pebble.Open` 之前，适配器再额外带上 `"src", "pebble"` 以便与 store
自身的日志区分。

### 4.2 Makefile 测试输出纪律

- 顶部声明 `SHELL := /bin/bash`（`set -o pipefail` 是 bash 特性，Makefile 默认
  的 `/bin/sh` 在部分平台上不支持）。
- `test` 目标改为：完整输出 `tee` 落盘 + `pipefail` 保住 `go test` 的退出码 +
  显式 `-timeout`。
- 新增 `test-cluster` 目标：`-count=1`（防缓存假绿）+ 收紧的 `-timeout 5m`，
  作为往后排查该包的固定入口，也是 4.3 复现尝试的载体。

`*.log` 已在 `.gitignore` 中（第 3 行），无需改动。

`-timeout` 的取值理由：`test` 用 `10m`——与 Go 默认值相同，写出来是为了让它成为
一个**可见的旋钮**而非隐式默认；`test-cluster` 用 `5m`——该包全量跑 72–85s，
5m 是约 3.5 倍余量，真挂死时比默认早 5 分钟触发栈转储。

### 4.3 复现尝试与如实记账

在 CPU 争抢条件下反复跑 `internal/cluster`，12 轮，每轮独立一条命令（每轮约
90–180s）。手法沿用当初唯一复现过的条件组合：冷 GOCACHE + `-count=1` + 背景
CPU 满载。

结果写入 `docs/superpowers/notes/2026-08-14-b12-repro-log.md`，逐轮一行：轮次、
耗时、通过/失败、失败时的用例名。

**记账纪律（承重）：**

- 复现到了 → 记录用例名与完整失败片段，**不当场修**：交回协调者决定是单独立项
  还是并入本次。盲目改一条只见过一次的用例是打补丁。
- 12 轮全绿 → 结论写「12 轮未复现」，**不得写成「已修复」「已稳定」**。这正是
  B12 在 15 次绿之后仍拒绝结案的理由。

## 5. 级别映射与 Fatalf 契约

| Pebble | slog | 理由 |
|---|---|---|
| `Infof` | `Debug` | Pebble 的 Info 是 WAL 扫描、compaction 明细一类的运维细节，逐次 open 都打。映射成 Info 会让生产日志被它刷屏——那正是当前 stderr 噪声的实质，只是换了个出口。需要时把级别调到 debug 即可拿到全部。 |
| `Errorf` | `Error` | 一一对应。 |
| `Fatalf` | `Error` + `os.Exit(1)` | 见下。 |

**`Fatalf` 必须保持进程终止语义。** Pebble 只在无法安全继续时（如检测到数据
损坏）调用它，其默认实现就是 `log.Fatalf`（`os.Exit(1)`）。把它降级成一条
Error 日志会让本该立刻停下的进程带着已知损坏的状态继续跑，这比崩溃危险得多；
改成 `panic` 也不行——`panic` 可被 `recover` 吞掉，同样破坏契约。所以：先用
slog 打一条 Error（保证这条致命信息进入项目的日志通道，而不是只留在 stderr），
再 `os.Exit(1)`。这个取舍必须写成代码注释。

## 6. 测试计划

新增 `internal/store/pebblelog_test.go`：

- **T1 级别映射**：用 `slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))`
  构造 logger，分别调 `Infof` / `Errorf`，断言 buf 中出现 `level=DEBUG` /
  `level=ERROR`，且格式化参数被正确展开（如 `Infof("n=%d", 42)` → 输出含 `n=42`）。
- **T2 级别过滤**：用 `Level: slog.LevelInfo` 的 logger 调 `Infof`，断言 buf 为空
  ——证明 Pebble 的噪声在默认生产级别下确实被挡住了。
- **T3 装配生效**：`store.Open` 传入一个写 buffer 的 Debug 级 logger，断言 open
  之后 buffer 里出现带 `src=pebble` 的行——证明 `Options.Logger` 真的接上了，
  而不是只写了个没人用的适配器。

`Fatalf` 不写单测（它会终止进程，测它需要子进程重启技巧，收益不抵复杂度）；
用注释说明契约即可。

回归：`make test` 全绿；`make test-cluster` 全绿。

## 7. 需要用户复核的假设

以下三项是在用户外出期间按「最保守分支」决定的，需回来时复核：

- **A1（行为变更）**：Pebble 的 `Infof` 映射到 **Debug** 而非 Info。代价是生产
  默认 info 级别下看不到 Pebble 的 compaction/WAL 信息，需要临时调 debug。
  选它的理由见 §5。若希望生产常态可见，改一行即可。
- **A2**：`Fatalf` 保持 `os.Exit(1)`。理由见 §5。
- **A3（范围）**：本次**同时改生产路径**（不是只在测试里静音 Pebble）。理由：
  `store.Open` 本来就收了 logger 却不转发，这本身是既有缺陷；且 CLAUDE.md §2
  禁止绕过项目 logger 的裸输出。只在测试里静音是打补丁，会把同一个洞留在生产。

## 8. 边界与风险

- **风险 R1**：`os.Exit(1)` 出现在库代码里。已限定为仅 `Fatalf` 一处，并与
  Pebble 原有默认行为完全一致——这不是新增的退出路径，是保持既有的那条。
- **风险 R2**：失败用例的 `t.Log` 里现在会多出 Pebble 的 Debug 行。这是**有意
  的**：它们归属明确、只在失败时出现，属于现场上下文而非噪声。
- **风险 R3**：`SHELL := /bin/bash` 要求构建机有 bash。macOS 与主流 Linux 均
  自带；若将来上 Alpine 容器需一并装 bash，届时再议。
- **B12 不会被本次关闭。** 完成后 backlog 状态推进为 `✅ done(已验)` 的是
  「可诊断性修复」这一项，偶发失败本身**另立一行保持待观察**，备注指向本 spec
  与复现记录。

## 9. 日志与注释要求

- `internal/store/pebblelog.go` 需文件头注释：职责（把 Pebble 的日志接口桥到
  项目 slog）+ 边界（不做采样、不做限流；级别映射策略与 `Fatalf` 契约的理由）。
- 适配器的导出/构造函数需说明参数与返回。
- `Fatalf` 的 `os.Exit(1)` 必须有「为什么不降级、不 panic」的中文注释。
- `store.Open` 里 `l` 求值上移的那一行，需注释说明「必须在 `pebble.Open` 之前
  求值，因为 Options.Logger 要用它」——否则后人重排代码时会静默把它移回去。
- Makefile 的 `test` 目标需注释写明 B12 教训：**测试输出永不过 `tail`/`head`，
  要截先落完整文件再截；管道会吞掉退出码，故 `pipefail`。**
