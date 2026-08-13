# B13.6：`RetryPolicy` 双重语义——按客户端类型分别下发

> 关联 backlog：B13.6（属 B13 epic）
> 日期：2026-08-14

## 1. 缺口的真实形态（原判断需要修正）

B13.2 的 brainstorm 把这个问题记成「协议只有一个 `BackoffPolicy` 字段，却要同时
承载 RPC 级与消息级两种语义，字段不够用」。**这个判断不成立。**

翻 Go SDK v5.1.4 的两个读取点：

| 读取点 | 客户端类型 | 用途 |
|---|---|---|
| `producer_options.go:153` `settings.GetBackoffPolicy()` → `producer.go:176` | Producer | **RPC 级**：发送失败重试 |
| `push_consumer_options.go:230` `settings.GetBackoffPolicy()` → `pc.retryPolicy` | PushConsumer | **消息级**：`nackMessage` 算不可见时长、`eraseFifoMessage` 判 DLQ |

两个用途分属**两类客户端**，而 `Settings` 是**逐客户端**协商的——同一个字段在
producer 的 Settings 里和在 consumer 的 Settings 里本来就可以是不同的值。

真正的缺口在 sq 这边：`internal/rpc/settings.go:96` 把
`BackoffPolicy: s.backoffPolicy()` 放在 `switch ps := client.GetPubSub()` 的
**外面**，于是两类客户端拿到逐字节相同的值。这不是协议缺陷，是 sq 少分了一次岔。

## 2. 现状与影响

`backoffPolicy()`（`internal/rpc/settings.go:64`）下发的是 **RPC 级**值：
`initial=100ms`、`multiplier=2`、`max=1s`（集群档 3s）、`maxAttempts=3`（集群档 5）。
对 producer 正确。对 consumer 则是两处失真：

- **`MaxAttempts=3` 与 broker 的真实判定不一致。** broker 侧按消费组的
  `EffectiveMaxAttempts()`（`internal/core/meta/meta.go:92`，默认 16）判 DLQ。
- **退避值与 broker 的真实行为不一致。** SDK 的 `nackMessage` 按这个策略算出
  100ms–1s 的不可见时长请求，而 `internal/rpc/receive.go:250` 会取
  `max(客户端要求, 退避下限)`——客户端的请求被静默覆盖掉。

**当前无用户可见故障**：`fifo=false` 下 SDK 不用 `MaxAttempts` 判 DLQ（只
ack/nack），不可见时长又被 broker 兜住。所以这是语义整洁性问题，不是 bug。

## 3. 方案

把 `BackoffPolicy` 的赋值从 `negotiateSettings` 的公共段挪进两个分支，
`backoffPolicy()` 拆成两个：

- **`publishBackoffPolicy()`**：现有 `backoffPolicy()` 原样保留改名，值不变。
  producer 的行为**零变化**。
- **`subscribeBackoffPolicy(group string)`**：描述 broker 的真实重投行为。

### 3.1 消费端的值不是估的，是 broker 常量导出的

`internal/core/deliver/deliver.go:50-57` 的重投退避是：

> 第 n 次投递（n≥2）的不可见时长下限 = `base × 2^(n-2)`，封顶 `max`
> ——`retryBackoffBase = 10s`，`retryBackoffMax = 5min`。

这与 `ExponentialBackoff{Initial, Max, Multiplier}` **逐字段一一对应**：

| proto 字段 | 值 | 来源 |
|---|---|---|
| `Initial` | `retryBackoffBase`（10s） | deliver 常量 |
| `Max` | `retryBackoffMax`（5min） | deliver 常量 |
| `Multiplier` | 2 | 公式里的 `2^(n-2)` |
| `MaxAttempts` | 该消费组的 `EffectiveMaxAttempts()` | `meta.GetGroup(group)` |

所以下发值不是另起一套需要跟着漂的常量，而是 broker 自己那套的**投影**。

`deliver` 侧 `retryBackoffBase` / `retryBackoffMax` 是**未导出的 `var`**（注释
写明「测试需注入小值控制用例时长」）。新增导出访问器：

```go
// RetryBackoffParams 返回重投退避的三要素（初值、上限、倍率），供协议层
// 下发给消费端。与 RetryBackoff 用同一组变量——两者永远不会漂开。
func RetryBackoffParams() (initial, max time.Duration, multiplier float64)
```

**不要在 rpc 侧另写 10s / 5min 常量。** 那正是这个条目要消灭的东西：同一个事实
在两处各写一遍，改一处忘一处。

### 3.2 组不存在时的回退

`negotiateSettings` 的 Subscription 分支已经有 `ps.Subscription.GetGroup()`。
用 `meta.GetGroup(name)`（只读，不建组）查：

- 查到 → `EffectiveMaxAttempts()`
- 查不到（首次连接、组尚未 EnsureGroup）→ `meta.DefaultMaxAttempts`（16）

**用 `GetGroup` 而不是 `EnsureGroup`**：settings 协商是只读的协议协商，不应有
建组副作用。真正建组由 ReceiveMessage 路径负责。

## 4. 这不改变 `fifo` 的结论，也**不改动那段注释**

> **本节已订正。** 本 spec 初稿写着「B13.2 的三条理由里第二条是『客户端按
> `MaxAttempts=3` 转 DLQ，与 broker 的 16 判定不一致』，本条目消除了它，实现时
> 须把该条划掉」。**这是错的。** 核对链尾 `feat/auto-renew` 上
> `internal/rpc/settings.go` 的实际注释（`git show feat/auto-renew:internal/rpc/settings.go`，
> 全文件 grep `MaxAttempts|死信|DLQ` 只命中退避常量与归属权那条），根本没有
> 「判定不一致」这条理由。按初稿施工会去划掉一条**正确且仍然成立**的理由。

B13.2 定 `fifo=false` 为终态，实际写在注释里的三条理由是：

1. **顺序安全不依赖此标志**——M4 起由队列级顺序锁保证每队列至多一条未终结的
   顺序 inflight，e2e `TestOfficialGoSDKPushFIFOOrderLock` 实测 listener 并发峰值恒为 1；
2. **翻 true 会夺走归属权**——客户端改建 `FiFoConsumeService`，消费失败转为
   客户端本地循环重投 listener、不回 broker，重试计数与 DLQ 判定整个从 broker
   挪到客户端，与 sq 的设计相反；
3. **翻 true 本身不保证被可靠观测**——B13.7 记录的 SDK v5.1.4 内部竞态
   （`Start()` 读 `isFifo` 与 telemetry 回调写同一字段，无同步）。

**本条目一条都不消除。** 下发真实 `MaxAttempts` 只意味着「万一客户端夺走了归属，
它至少会数到正确的次数」——归属权迁移这件事本身（理由 2）纹丝不动，理由 1、3
更是完全无关。

**所以：`fifo` 保持 false，且实现时不要碰 `settings.go` 里那段 `fifo` 注释。**
本条目也不得被读成「现在可以翻了」。

## 5. 测试计划

`internal/rpc/settings_test.go`：

- T1 producer 分支的 `BackoffPolicy` 与改动前**逐字段相同**（回归防护：
  producer 行为必须零变化）。单机档与集群档各断言一次。
- T2 consumer 分支的 `BackoffPolicy`：`Initial=10s`、`Max=5min`、
  `Multiplier=2`，且这三个值**取自 `deliver.RetryBackoffParams()` 而不是测试里
  写死的字面量**——写死就复制了同一个漂移风险进测试。
- T3 `MaxAttempts` 取消费组真实值：建一个 `MaxAttempts=5` 的组，断言下发 5。
- T4 组不存在时回退 `meta.DefaultMaxAttempts`。
- T5 **两个分支的 `BackoffPolicy` 不相等**——这是本条目的核心断言，把
  「又被合并回公共段」这个回退变成红灯。

## 6. 需要用户复核的假设

- **A1（原判修正）**：backlog 原记「协议只有一个字段，承载不了双重语义」不成立。
  `Settings` 逐客户端协商，字段本就够用；缺口在 sq 少分了一次岔。**本条目因此
  从「评估要不要改」变成「有明确修法的小改动」。**
- **A2（行为变更）**：消费端下发值从 `100ms/1s/3` 变成 `10s/5min/组真实值`。
  评估：`nackMessage` 请求的不可见时长从「被 broker 静默覆盖的 100ms–1s」变成
  「与 broker 实际施加的一致」，是修正而非退化；`MaxAttempts` 在 `fifo=false`
  下不被 SDK 使用，无当前影响。**但它确实改变了下发给所有存量消费者的协商值**，
  且是在用户外出期间定的，需复核。
- **A3**：`fifo` 仍为 false（§4）。三条理由**一条都没被本条目消除**，那段注释
  不改动。此处与初稿相反，订正理由见 §4 的引注。

## 7. 日志与注释要求

- `publishBackoffPolicy` / `subscribeBackoffPolicy` 各需注释说明**它服务的是哪
  一类客户端、SDK 拿它做什么**——这正是当初合并成一个函数时丢失的信息。
- `subscribeBackoffPolicy` 需注释写明「值取自 `deliver.RetryBackoffParams()`，
  不要在此处另写常量」及其理由（§3.1）。
- `deliver.RetryBackoffParams` 需导出注释，说明它与 `RetryBackoff` 共用同一组
  变量。
- `settings.go` 中 `fifo` 那段注释**保持原样，一个字都不要改**（§4 订正后的结论；
  本 spec 初稿要求更新它，那是基于对原文的误记）。
- 协商时组查不到而回退默认值 → Debug 一条（含 group 与回退值）：这是「为什么
  这个消费者拿到的是 16 而不是它组里配的值」唯一的排查线索。
- 本改动无错误分支（`GetGroup` 只读不报错），故无新增 Error 日志。
