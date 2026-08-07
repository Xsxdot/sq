# 吞吐根因与工程卫生设计（B1 队列粒度锁 + B4 e2e 独立模块 + B5 承重测试）

> M7（B7 epic）第二、三批的紧凑 spec。B1 是唯一有设计含量的项；B4/B5 无设计分叉，
> 记录决定即可。与第一批（鉴权与协议面安全收尾）合并为一个实施计划执行。
>
> 需求源：backlog B1 / B4 / B5；主设计 spec §10（测试策略 5：性能基线 5k msg/s）。

## 1. B1：produce.Append 锁按队列拆分

### 问题

`p.mu` 覆盖队列选择、offset 分配、`store.Apply`（fsync 在内）与长轮询唤醒——所有
topic 的所有生产者在同一临界区排队，任意两次 Apply 永不重叠，Pebble group commit
（把并发 commit 合并成一次 fsync）完全失效。实际形态：每条消息一次 fsync、全局单
线程。produce.go:77 的代码注释已记录此瓶颈并明确「等真正测吞吐时连同基准一起做」。

### 方案：队列粒度锁（已定，弃「Apply 前放锁」）

每个 (topic, queue) 一个 `queueState`：

```go
type queueState struct {
    mu     sync.Mutex
    next   uint64 // 下一 offset（懒加载自 alloc/ key）
    loaded bool
}
```

`Producer` 保留全局 `p.mu`，但只护共享 map（`qstates`、`rr`、`wakers`）与 delay
计数器；`Apply` 移到对应 `queueState.mu` 之下。新 Append 流程：

1. 校验 + `EnsureTopic`（锁外，现状）。
2. `p.mu`：队列选择（rr 轮询 / MessageGroup hash）、取或建 `qstates[topic/q]`。放锁。
3. `qs.mu`：offset 分配（懒加载）、编码、建 batch、`store.Apply`、成功后 `qs.next++`。放锁。
4. `p.mu`：`wakeLocked(topic)`。放锁。

不变量逐条对照：

| 既有不变量 | 如何保持 |
|---|---|
| 消息与 alloc 计数器同批原子提交 | batch 组装不变，仍在同一 Apply |
| Apply 成功才推进内存计数器 | `qs.next` 的更新仍在 Apply 成功之后、同一 `qs.mu` 临界区内 |
| 同队列 offset 顺序 == 落盘顺序（FIFO 根基） | 同队列仍被 `qs.mu` 串行化；不同队列间无顺序承诺（本来就没有） |
| `AppendWith` 的 extra 回调不得重入 Producer | 约束不变，注释同步改为「不得重入（qs.mu/p.mu 均不可重入）」 |
| 唤醒在写入之后 | 步骤 4 在 Apply 成功后执行；订阅者被唤醒后读 store 必能看到消息 |

弃选方案「Apply 前释放锁」：同队列并发 Apply 会把 offset 分配顺序与持久化顺序解耦，
FIFO 语义需要额外机制找回，复杂度高而收益与队列粒度锁相同。

`AppendDelay` 改用独立 `delayMu` 护 seq 分配 + Apply（延时暂存区是单一全局计数器，
天然串行；与普通写互不阻塞即可，不追求更细）。`Subscribe` 不变（`p.mu`）。

### 基准与验收

- 基准先行：`BenchmarkAppendParallel`（produce 包，真实 store、`fsync=sync`、
  `b.RunParallel` 多 goroutine 写多队列）**先在旧代码上提交并记录数字**，再实施
  锁拆分、复跑对比。两组数字都写进提交信息。
- 达标线：改后并发多队列吞吐显著高于改前（group commit 生效的直接证据），且换算
  msg/s ≥ 5k（spec §10 基线；开发机数字，README 声明留给 B7.2 收口）。
- `-race` 全量重跑（B5 的并发 Append 用例正好覆盖新锁结构）。

## 2. B4：test/e2e 拆独立 Go module

官方 SDK 是主模块 direct require，仅在 `e2e` tag 下 import，却把约 20 个间接模块
（google.golang.org/api、opencensus、zap、validator 等）拉进主模块图——任何人
`go install` 都要下载，且都进不了最终二进制。

做法：

- `test/e2e/go.mod`：`module github.com/xushixin/sq/test/e2e`，require 主模块
  （`replace github.com/xushixin/sq => ../..`）+ rocketmq-clients。
- 主模块 `go mod tidy` 后 rocketmq-clients 及其间接依赖全部消失。
- Makefile `e2e` 目标改为 `cd test/e2e && go test -tags e2e -count=1 ./...`。
- 子模块自动脱离根 `go test ./...` / `go vet ./...`（Go 模块边界即测试边界，
  正是想要的效果）。
- 未来场景测试的共享 broker helper（`test/internal/broker`，见场景测试 spec）属
  主模块，e2e 子模块经主模块 require 引用，不受影响。

## 3. B5：两条承重测试

1. **`store.Scan(limit<=0)` 语义锁定**（store 包）：`limit<=0 == 不限量`已被
   deliver 阶段 2 的跳过逻辑直接依赖，却无测试。用例：写 N 条，`limit=0` 与
   `limit=-1` 均返回全部 N 条。
2. **并发 `Append` 的 `-race` 用例**（produce 包）：类型注释声称「并发安全」但
   `-race` 只跑过顺序用例。用例：多 goroutine 并发写同一 topic（覆盖同队列争抢
   与跨队列并行），全部成功后断言 offset 无重复无空洞、alloc 计数器与消息数一致。
   此用例在 B1 改锁前提交（先钉住旧行为，改锁后原样通过才算等价重构）。

## 4. 执行顺序（合并计划内）

B5（旧代码上先钉行为）→ B1 基准（旧代码记数）→ B1 实施（复跑对比）→ B4
（模块拆分放最后，避免拆分期间其他任务的 go.mod 操作互相干扰）。

## 5. 明确不做

- 极限优化（零拷贝、批量聚合写、异步 ack）——spec §12 YAGNI 红线。
- README 性能声明措辞——B7.2 发布打磨统一收口。
- go.work 工作区文件——单人仓库，replace 足够；不引入新的工具面。

## 6. 出口标准

`go test -race ./...`（主模块）全绿；e2e 子模块全量过；基准改前改后两组数字
落档且 ≥5k msg/s；主模块 go.mod 无 rocketmq-clients；`go install ./cmd/sq`
在干净缓存下不再拉 SDK 间接依赖。
