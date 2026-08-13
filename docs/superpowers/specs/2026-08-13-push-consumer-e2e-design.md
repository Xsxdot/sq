# PushConsumer 真实 e2e 验证设计（B13.2）

**日期**：2026-08-13
**Backlog**：B13.2（属 B13「v1.1：官方 SDK 全兼容收口」）
**状态**：设计已确认，待出实现计划

---

## 1. 背景与目标

sq 的 PushConsumer 消费路径**从未被任何测试覆盖过**。所涉 RPC（`QueryAssignment` / `ReceiveMessage` / `AckMessage` / `ChangeInvisibleDuration` / `ForwardMessageToDeadLetterQueue`）全部已实现，`internal/rpc/settings.go` 也显式下发了 `fifo` / `receiveBatchSize` / `longPollingTimeout`，但 `test/e2e` 下的 40 个用例**清一色是 SimpleConsumer**。也就是说这条路径的可用性目前是「理论通」，不是「验过」。

本条目把它变成「验过」，具体三个目标：

1. **建立 PushConsumer 的 e2e 覆盖**——回调驱动的消费模型与现有「主动 `Receive` 再断言」形态完全不同，需要一套新的 harness。
2. **还掉 `settings.go` 的 `fifo` 欠账**——那句「fifo 协商标志待 push 消费流程验证后（M5+）再翻转」的 TODO 性质注释，本条目给出结论并订正理由。
3. **把 push 路径上的协商差异捞出来**——按既定分流规则处置（见 §9）。

**验证载体：Go SDK v5.1.4**。Java 不在本条目范围。理由：PushConsumer 路径零覆盖，任何基础缺陷 Go 一样能撞出来，而 Go 可复用现有 `test/e2e` 模块、零新工具链；先用最便宜的手段把基础缺陷清掉，再单独决定 Java 是否值得立工程。代价是 Go SDK 不是 RocketMQ 的参考实现，只有 Java 才会暴露的行为差异这轮盖不到——这一条如实记在 §11。

---

## 2. 非目标

- **不做 Java / Python / C# / C++ 验证**。Java 待本条目跑完后单独决策；其余语言属 B13.3。
- **不做集群档验证**。三节点下的 `QueryAssignment` 分配、leader 切换后的 push 消费恢复不在范围。leader 切换已由现有大量场景用例覆盖（simple 路径），push 上再来一遍边际收益低，而集群用例都很慢，会显著拉长 e2e 总时长。
- **不实现 `LitePushConsumer`**。它依赖 sq 未实现的 `SyncLiteSubscription` RPC，撞到就明确回 `UNIMPLEMENTED`。
- **不翻转 `fifo` 标志**。结论是保持 `false`，理由见 §7.1。
- **不实现 `AutoRenew`**。见 §8.1。

---

## 3. 前置事实（已实地核对，非推断）

本节全部结论来自读代码，实现计划可直接引用，不需要重新核对。

### 3.1 `fifo` 标志的真实语义

`internal/rpc/settings.go` 下发的 `Subscription.Fifo` 被客户端读入 `pc.pcSettings.isFifo`（`push_consumer_options.go:226`），并在 `Start()` 时决定建哪种消费服务（`push_consumer.go:379`）：

| 下发值 | 客户端消费服务 | 消费失败后的行为 |
|---|---|---|
| `false`（sq 当前） | `StandardConsumeService` | `eraseMessage` → `nackMessage` → `ChangeInvisibleDuration` 回 broker。**重试计数与 DLQ 判定归 broker** |
| `true` | `FiFoConsumeService` | `eraseFifoMessage` → **客户端本地循环重投 listener**（`consumeWithDuration`），不回 broker；跑满 `maxAttempts` 后由客户端直接调 `ForwardMessageToDeadLetterQueue` |

即：**`fifo` 不是「顺序开关」，是「重试与 DLQ 归属权开关」**。

### 3.2 顺序在 `fifo=false` 下结构性安全

`internal/core/deliver/deliver.go:356-370` 的队列级顺序锁保证：队列存在未终结的顺序 inflight 时后续顺序消息不投，且投出即占锁。因此**每队列至多一条未终结的顺序 inflight**。客户端哪怕开多线程消费池，同一队列手上也永远只有一条，线程池没有乱序的机会。

同 `MessageGroup` 的消息落同一队列——这一点已由现有用例 `TestOfficialGoSDKFIFOOrderedDelivery` 证实（其注释明载「8 条同组消息全部落在同一队列」），且已有 `sendFifoBatch` helper 可复用。

### 3.3 回执句柄编码了 attempt

`internal/rpc/receive.go:366` 的 `receiptDecode(secret, handle)` 解出 `(group, topic, queue, offset, attempt)`。**attempt 是句柄内容的一部分**，所以 broker 每次重投必然发出不同的句柄。这是 §6.2 判别器的依据。

### 3.4 重投退避

`deliver.go:51` 起：非顺序消息第 n 次投递（n≥2）的不可见时长下限 = `retryBackoffBase × 2^(n-2)`，封顶 `retryBackoffMax`，即 10s、20s、40s … 5min；客户端要求的 invisible 更长时取客户端值。

两个变量是**包内未导出**的，且 e2e 以独立进程启动 broker（`buildBroker` + `launchBroker`），无法注入小值。**第 2 次投递至少 10 秒后**，所有涉及重投的用例窗口必须按此计算。

### 3.5 PushConsumer 不设 `InvisibleDuration`，改设 `AutoRenew: true`

`push_consumer.go:734` 的 `WrapReceiveMessageRequest` 只设 `LongPollingTimeout` / `BatchSize` / `AutoRenew: true`，**不设 `InvisibleDuration`**。

sq 侧 `receive.go:110-112`：`invisible <= 0` 时回落 `time.Minute`。行为有定义，但该兜底值**硬编码、不可配**。

`AutoRenew` 在 sq 全仓只出现在生成的 pb 代码里，业务侧零引用——**sq 静默忽略它**。见 §8.1。

### 3.6 `MessageListener` 无法在 SDK 包外实现

`push_consumer_options.go:67` 的 `MessageListener` 接口方法 `consume(*MessageView) ConsumerResult` 是**未导出**的。测试代码只能用导出的壳 `rmq.FuncMessageListener{Consume: func(*rmq.MessageView) rmq.ConsumerResult}`。

### 3.7 可用的观测点

- `MessageView` 导出了 `GetMessageId()` / `GetBody()` / `GetDeliveryAttempt() int32` / `GetReceiptHandle()` / `GetMessageGroup()` / `GetProperties()`。
- `ConsumerResult` 只用导出常量 `rmq.SUCCESS` / `rmq.FAILURE`；`SUSPEND` 仅 `LitePushConsumer` 支持，其余客户端类型会被 SDK 转成 `FAILURE` 并打 Warn。
- PushConsumer 可配项仅 7 个：`WithPushSubscriptionExpressions` / `WithPushAwaitDuration` / `WithPushMaxCacheMessageSizeInBytes` / `WithPushMaxCacheMessageCount` / `WithPushConsumptionThreadCount` / `WithPushMessageListener` / `WithPushEnableFifoConsumeAccelerator`。**没有不可见时长选项**。

---

## 4. 验证矩阵

六条用例，每条都必须能写出真断言；写不出真断言的不进矩阵。

| # | 用例 | 证明什么 | 关键断言 |
|---|---|---|---|
| 1 | 基础闭环 | `QueryAssignment` → 长轮询 `Receive` → listener → `Ack` 整条链路通 | 见 §6.5 |
| 2 | 长轮询到达即唤醒 | 用的是长轮询，不是慢周期轮询 | 见 §6.6 |
| 3 | 消费失败 → broker 重投 | 重试计数归 broker | 见 §6.2 |
| 4 | 超限 → DLQ | DLQ 判定归 broker | 见 §6.3 |
| 5 | FIFO + 并发消费顺序不破 | `settings.go` 的承重断言 | 见 §6.1 |
| 6 | `GracefulStop` 后 inflight 不丢 | 停机语义 | 见 §6.4 |

**明确排除的两条**（曾考虑，砍掉）：

- 「批量 `ReceiveBatchSize` 生效」——从客户端侧观察不到有意义的差别，条数对上不等于批量生效。
- 「`fifo` 协商可观测」——与用例 3、4 完全重复：3 和 4 已用实证方式证明归属权在 broker，而那正是 `fifo=false` 的全部含义。

---

## 5. harness 设计

新增文件 `test/e2e/sdk_push_test.go`（`//go:build e2e`）。broker 启停沿用现有 `startBroker(t, mutate...)`——它本来就每次新起一个进程，天然隔离，不需要新造。

### 5.1 采集器

```go
// pushCollector 把 callback 驱动的消费收敛成测试可断言的结构。
//
// 边界：只做采集与返回值决策，不做任何断言——断言一律回主测试 goroutine，
// 原因见 §5.3 第 1 条。
type pushCollector struct {
    mu   sync.Mutex
    got  []recv // 按 listener【入口】到达序追加

    inFlight    atomic.Int32 // 当前并发进行中的 listener 调用数
    maxInFlight atomic.Int32 // 历史峰值，用例 5 的核心断言

    // handle 由用例注入，决定每条返回 SUCCESS 还是 FAILURE，也可在内部阻塞。
    handle func(*rmq.MessageView) rmq.ConsumerResult
    // hold 每次 listener 调用内部停留的时长，用于撑开并发重叠观测窗口。
    hold time.Duration
}

// recv 是 MessageView 的快照。不留 *MessageView 指针：SDK 在回调返回后可能
// 复用/回收该对象，跨 goroutine 持有它读出来的值不可信。
type recv struct {
    id      string
    body    string
    attempt int32
    handle  string // 回执句柄，用例 3 的判别器
    group   string // MessageGroup
}
```

`listener()` 方法返回 `*rmq.FuncMessageListener`（§3.6：接口无法在包外实现），其 `Consume` 依次做：进并发计数并更新峰值 → 入口处 append 快照 → `hold` 停留 → 退并发计数 → 调 `handle` 取返回值。

### 5.2 三个 helper

| 签名 | 职责 |
|---|---|
| `startPushConsumer(t, endpoint, group, topic string, c *pushCollector, opts ...rmq.PushConsumerOption) rmq.PushConsumer` | 建 + `Start()` + `t.Cleanup(GracefulStop)` |
| `waitCount(t, c *pushCollector, n int, within time.Duration)` | 轮询等到累计条数达 n，超时 `t.Fatalf` |
| `snapshot(c *pushCollector) []recv` | 加锁拷贝 |

### 5.3 四处承重设计

每一条都对应一种具体的写错方式，实现计划必须原样落实：

1. **listener 里绝对不能调 `t.Fatalf`。** 它跑在 SDK 的后台 goroutine 上，而 `Fatalf` 只有在测试 goroutine 内调用才有定义的行为——在别处调不会正确终止测试，只会让失败以诡异方式表现。采集器只采集，断言全部回主 goroutine。

2. **顺序采集点必须在 listener 入口，不能在出口。** 用例 5 要证的是「listener 被调用的顺序」。`hold` 停留必须放在 `append` **之后**；若先停留再 append，采到的是**完成序**——完成序乱不代表调用序乱，那是在测另一件事，且是会假红的那种错。

3. **超时窗口按 §3.4 的退避参数算，不拍脑袋。** 涉及重投的用例（3、4、6）第 2 次投递至少在 10 秒后，窗口写死一个小值必然间歇性红。

4. **`waitCount` 用轮询（50ms 一轮 + deadline），不用 channel 唤醒。** channel 通知要处理「唤醒丢失」和「多次到达只醒一次」，写错就是间歇性挂；e2e 本身是秒级尺度，轮询没有隐蔽失败模式。

---

## 6. 用例构造

本节按「承重程度」排序，不按用例编号排序：§6.1 = 用例 5，§6.2 = 用例 3，§6.3 = 用例 4，§6.4 = 用例 6，§6.5 = 用例 1，§6.6 = 用例 2。

### 6.1 用例 5：FIFO + 并发消费顺序不破

**构造**：复用 `sendFifoBatch(t, endpoint, topic, mg, "fifo", 20)` 发 20 条同 `MessageGroup` 消息（→ 同一队列，§3.2）；消费者配 `WithPushConsumptionThreadCount(4)`；采集器 `hold = 50ms`，`handle` 恒返 `rmq.SUCCESS`。

**断言**：

| 编号 | 内容 | 性质 |
|---|---|---|
| A | `got` 的 body 序**严格全等**于 `fifo-0` … `fifo-19` | 用户可见性质：顺序没破 |
| B | **`maxInFlight == 1`** | 顺序锁的直接证据 |

B 才是真正的证明。**不要只写 A**：顺序锁正确时客户端手上永远只有一条，线程池没有乱序机会，A 会恒真；锁坏掉时 A 也只是**概率性**变红（invocation 顺序的竞态很弱，运气好照样顺序正确，缺陷漏网）。B 则是确定性的——线程池有 4 个线程、listener 撑住 50ms，只要 broker 任何一次放出两条同队列顺序消息，重叠必被观测到，`maxInFlight` 立刻变 2。

**两处边界**：

- **`hold` 必须远小于不可见时长**（§3.5：1 分钟，或按 §7.2 配成的值）。若 `hold` 逼近不可见窗口，broker 会认为该条超时未终结而重投，重投件与仍在跑的那件重叠 → `maxInFlight` 变 2，**假红**。50ms 相对 1 分钟有三个数量级余量；若按 §7.2 把用例的不可见时长调小，须同步复核此余量。
- **断言 A 用严格全等，不用「递增」**。重投会让 body 重复出现，全等能把它照出来。真跑出间歇性红时，正确修法是给这条用例调高不可见时长，**不是**把断言放宽成「忽略重复」——那等于把要测的东西测没了。

**成本**：现有 FIFO 用例要跑约 110s（deadline 240s），因为 SimpleConsumer 每次 `Receive` 只轮询一个队列，每条都要等一轮四队列循环。PushConsumer 对每个已分配队列各维持一条长轮询，broker 在 ack 后 100ms 内即可放下一条——**同样量级的消息数，push 版预计秒级**。

### 6.2 用例 3：消费失败 → broker 重投

**构造**：`handle` 恒返 `rmq.FAILURE`，发 1 条普通消息，窗口按 §3.4 取 ≥30s。

**断言**：

| 编号 | 内容 |
|---|---|
| 3a | 同一 messageId 至少被调用 2 次 |
| 3b | 第 2 次的 `GetDeliveryAttempt() == 2` |
| 3c | **两次的 `GetReceiptHandle()` 不同** |

**3c 是判别器。** 客户端本地重试路径复用同一个 `MessageView` 对象反复喂 listener，句柄不会变；只有真的回了 broker、broker 重新发件，才会拿到编着新 attempt 的新句柄（§3.3）。

**3b 单独不足以判别**：客户端本地重试同样会自增 `deliveryAttempt`（`eraseFifoMessage` 里就是 `mv.deliveryAttempt += 1`），两条路的 attempt 都会涨。只有句柄能把它们分开。

### 6.3 用例 4：超限 → DLQ

**构造**：`startBroker(t, func(c *config.Config) { c.DefaultMaxAttempts = 2 })`（沿用现有 `TestOfficialGoSDKDLQ` 的手法）；`handle` 恒返 `rmq.FAILURE`。

**断言**：

| 编号 | 内容 |
|---|---|
| 4a | listener 恰被调用 2 次后**不再**被调用 |
| 4b | 用 SimpleConsumer 订阅 `%DLQ%{group}` 能读到该条，body 对上 |

**4a 必须用连续静默窗口判定**（连续 N 轮采样计数不变），不是「等一会儿看一眼」——与 SQL92 那条「单次空轮询不能证明位点推过」是同一类陷阱。

**限界（spec 必须如实写，不得包装）**：DLQ 条目本身**带不出「谁转的」签名**——`ForwardToDLQ`（RPC 路径，`deliver.go:611`）与 broker 自动超限路径最终汇进同一个 `moveToDLQ`（`deliver.go:671`），产出完全一致。所以「DLQ 判定归 broker」不是单条 e2e 断言能证的，它由三件事合起来支撑：4a（listener 停止被调用）+ 4b（消息确实在 DLQ）+ **SDK 侧静态事实**（标准消费服务的 `eraseMessage` 只有 ack/nack 两条分支，根本没有 forward 调用）。第三条是读代码得来的，不是测试跑出来的。

### 6.4 用例 6：`GracefulStop` 后 inflight 不丢

**构造**（三步，顺序固定）：

1. broker 以 `default_invisible_duration = 5s` 启动（§7.2）。发 1 条消息，消费者 A 的 `handle` 收到后**永久阻塞**（`select {}` 不返回），因此这条永远不会被 ack。
2. `waitCount(t, cA, 1, 30*time.Second)` 确认 A 确实收到了这一条，再对 A 调 `GracefulStop()`。
3. 起消费者 B（同组、同 topic，`handle` 返 `rmq.SUCCESS`），`waitCount(t, cB, 1, 20*time.Second)`。

**断言**：B 收到该条，body 对上，且 `GetDeliveryAttempt() >= 2`（证明它是重投件，不是新消息）。

**为什么必须永久阻塞而不是「返回前消费者已停」**：后者的时序不可控——`GracefulStop` 与 listener 返回谁先谁后取决于调度，若 listener 抢先返回并 ack 成功，B 就永远等不到，用例间歇性红。永久阻塞把「这条一定没 ack」变成确定事实。`GracefulStop` 不会等这个阻塞的 listener（SDK 侧有超时），进程退出时 goroutine 随之消失，不泄漏。

**成本与依赖**：本用例是 §7.2 配置化改动的唯一动因。若不做该改动，初次投递的不可见时长走 §3.5 的硬编码 1 分钟，本用例要干等 60s+。

### 6.5 用例 1：基础闭环

**构造**：发 5 条普通消息，`handle` 恒返 `rmq.SUCCESS`。

**断言**：

| 编号 | 内容 |
|---|---|
| 1a | `waitCount(t, c, 5, 30*time.Second)` 收全 5 条，body 集合与发送集合相等 |
| 1b | 消费者 `GracefulStop()` 后，同组新起一个消费者 B，在 **15s 窗口内 B 的 listener 零调用** |

1b 证明 Ack 真落地——位点推进了，而不是「收到了但位点没推」。

**这里不需要 SQL92 那种「N 轮连续空轮询」的手法**：那条纪律的成因是 SimpleConsumer 每次 `Receive` 只轮询一个队列，单次为空不代表所有队列都空。PushConsumer 对每个已分配队列各维持一条长轮询，一个持续窗口内的零调用即覆盖全部队列。窗口取 15s（> `defaultLongPolling` 20s 的一半，且远大于 assignment 建立所需时间）。

### 6.6 用例 2：长轮询到达即唤醒

**构造**（探针先行）：

1. 起消费者，`handle` 恒返 `rmq.SUCCESS`。
2. **先发一条探针消息**，`waitCount(t, c, 1, 30*time.Second)` 等它到达。这一步确认 `QueryAssignment` 已完成、长轮询已挂上。
3. 记 `t0 := time.Now()`，发第二条消息，等 `got` 长度变成 2，记该时刻为 `t1`。

**断言**：`t1 - t0 < 3s`。

**为什么必须有探针步骤**：消费者 `Start()` 之后要走 Telemetry 协商 + `QueryAssignment` 才会挂上长轮询，这段耗时不确定。不用探针而直接「起消费者 → 睡 2s → 发消息 → 计时」，测的是「协商耗时 + 唤醒耗时」的和，协商慢一点就假红；有了探针，`t0` 之后长轮询确定已在挂着，量到的才是纯唤醒延迟。

**阈值取 3s 的依据**：若客户端退化成慢周期轮询，周期上限是 `defaultLongPolling = 20s`（§3.7 无相关可配项），唤醒延迟会落在秒级到 20s；3s 能把两者分开，同时对本机 e2e 的抖动留足余量。

---

## 7. 随本条目落地的代码改动

### 7.1 订正 `settings.go` 的 `fifo` 注释

**当前原文**（`internal/rpc/settings.go`，`Settings_Subscription` 分支内）：

```go
// M4 起顺序由 broker 端强制（队列级顺序锁），消费端无需协商关闭；
// fifo 协商标志待 push 消费流程验证后（M5+）再翻转，
// 当前保持显式下发 false（不能留空让客户端去猜）。
```

**问题**：结论（保持 `false`）是对的，但理由不完整，且「待验证后再翻转」暗示翻转是既定方向——**方向反了**。

**改后原文**：

```go
// 显式下发 false，且这是终态，不是待翻转的临时值（B13.2 已验）。
//
// 两条理由，缺一不可：
//  1. 顺序安全不依赖此标志：M4 起队列级顺序锁保证每队列至多一条未终结的
//     顺序 inflight（deliver.go 顺序锁），客户端哪怕开多线程消费池，同一
//     队列手上也永远只有一条，无从乱序。
//  2. 翻成 true 会夺走归属权：客户端会改建 FiFoConsumeService，消费失败
//     转为【客户端本地循环重投 listener】，不回 broker；跑满 maxAttempts 后
//     由客户端直接调 ForwardMessageToDeadLetterQueue。这与 sq 的 broker 端
//     maxAttempts / DLQ 设计正面冲突——现有 DLQ 语义与用例验的都是 broker
//     那套。
//
// 不能留空让客户端去猜。
```

### 7.2 不可见时长兜底值提为配置项

`internal/rpc/receive.go:112` 当前硬编码 `invisible = time.Minute`。提为配置项 `default_invisible_duration`，**默认值仍为 1 分钟**（无语义变化），供 e2e 压缩用例 6 的时长。

按 §9 的分流规则，这属于「小修内含」：一个配置项，无行为变化，且直接服务可测性。

---

## 8. 本条目不修、另立 backlog 条目的发现

### 8.1 sq 静默忽略 `AutoRenew`

PushConsumer 在每次 `ReceiveMessage` 请求上设 `AutoRenew: true`（§3.5），语义是「我正在处理，请在处理期间自动续租不可见期」。sq 全仓对该字段零引用，固定用兜底的 1 分钟。

**后果**：listener 处理超过不可见时长时，broker 会在客户端仍在处理的情况下重投——而 `AutoRenew` 的存在正是为了防这个。sq 是 at-least-once 系统，重复投递在契约内，所以这不是正确性缺陷；但它是**与参考 broker 的行为背离**，慢消费者场景会撞到。

**为何不在本条目修**：实现它要引入租约续期机制（判定「客户端是否仍在处理」并延展不可见期，需挂在 Telemetry 流或心跳上），属架构级，按 §9 分流规则应另立条目走完整 brainstorm。

### 8.2 `RetryPolicy` 的双重语义

`settings.go` 的 `backoffPolicy()` 下发的常量按其自身注释是 **RPC 级**重试策略（`backoffInitial=100ms`、`backoffMax=1s`、`backoffMaxAttempts=3`，注释写「单次请求最多尝试次数」）。但协议只有这一个 `BackoffPolicy` 字段，SDK 把它当作**消息级**重试策略读（`nackMessage` 用它算不可见时长，`eraseFifoMessage` 用它的 `MaxAttempts` 判 DLQ）。

**当前无用户可见影响**：`fifo=false` 下标准路径不使用 `MaxAttempts`（只 ack/nack），而客户端请求的不可见时长（100ms–1s）会被 broker 的 10s 退避下限兜住（§3.4）。

**记录原因**：它是**不翻转 `fifo` 的第二条独立理由**——若翻成 `true`，客户端会按 `MaxAttempts=3` 放弃并转 DLQ，而 broker 侧 `DefaultMaxAttempts` 默认是 16，两边判定不一致。另立条目评估是否应下发与消费组真实 `maxAttempts` 一致的值。

---

## 9. 缺陷分流规则

验证过程中撞出 sq 真缺陷时：

- **协商细节类小修**（下发字段取值、边界判定、注释订正、为可测性做的配置化）——当场改掉，写进同一份实现计划。
- **架构级问题**（如 §8.1 的租约续期）——**停下来**开新 backlog 条目走完整 brainstorm，不在验证条目里就地改架构。

判据：若修复需要引入新机制、跨越模块边界、或改变协议语义，即为架构级。

---

## 10. 验收标准

1. `test/e2e/sdk_push_test.go` 六条用例全绿。用例函数一律以 `TestOfficialGoSDKPush` 起头（沿用现有 `TestOfficialGoSDK*` 命名族），使下列命令能一次跑全：
   ```
   cd test/e2e && go test -tags e2e -count=1 -run 'TestOfficialGoSDKPush' -v
   ```
   **`-tags e2e` 不可省**——漏了会得到 "no test files" 而看起来像跑过了。
2. `go test -race -count=1 ./...` 全绿（§7.2 改了 `internal/rpc` 与 `internal/config`）。
3. `settings.go` 的 `fifo` 注释已按 §7.1 订正。
4. `default_invisible_duration` 配置项已落地，默认 1 分钟，README 配置表补该项。
5. §8 的两项发现已各自登记为 backlog 条目（`💡 idea`），备注含本 spec 的定位信息。

---

## 11. 风险与限界

- **Go SDK 不是参考实现。** 只有 Java 才会暴露的行为差异这轮盖不到。本条目验完后需单独决策 Java 是否立工程——那个决策应以本轮结果为输入（若 Go 就撞出一堆基础缺陷，Java 应推后到缺陷清完之后）。
- **「DLQ 判定归 broker」有一半是静态证据。** 见 §6.3 限界段。不得在验收结论里写成「e2e 证明了归属权在 broker」。
- **重投类用例天然慢。** §3.4 的 10s 退避下限不可注入，用例 3、4 各自需要 30s 量级窗口。这是既定成本，不是可优化项——除非把退避参数也配置化，但那改的是运行期行为，超出本条目范围。
- **`maxInFlight == 1` 依赖「所有消息落同一队列」。** 若将来 `sendFifoBatch` 的路由行为变化（同 `MessageGroup` 不再必然同队列），该断言会失去意义并可能假绿。实现计划里应在用例内额外断言所有 `recv.group` 一致，把这个前提钉住。
