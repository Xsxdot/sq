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

真正的缺口在 sq 这边：`internal/rpc/settings.go` 把
`BackoffPolicy: s.backoffPolicy()` 放在 `switch ps := client.GetPubSub()` 的
**外面**，于是两类客户端拿到逐字节相同的值。这不是协议缺陷，是 sq 少分了一次岔。

## 2. 现状与影响（**本节已订正，范围随之收窄**）

`backoffPolicy()` 下发的是 **RPC 级**值：`initial=100ms`、`multiplier=2`、
`max=1s`（集群档 3s）、`maxAttempts=3`（集群档 5）。对 producer 正确。

### 2.1 唯一成立的失真：`MaxAttempts`

broker 侧按消费组的 `EffectiveMaxAttempts()`（`internal/core/meta/meta.go`）判
DLQ，默认值来自配置项 `DefaultMaxAttempts`。下发给消费端的 `3`（集群档 `5`）与
它无关，是一个**凭空的数字**。

**当前无用户可见故障**：`fifo=false` 下 SDK 不用 `MaxAttempts` 判 DLQ（只
ack/nack，死信全由 broker 判）。所以这是语义整洁性问题，不是 bug。

### 2.2 被推翻的那条：退避值失真

> **本节推翻了本 spec 初稿的核心论据。** 初稿写着「SDK 的 `nackMessage` 按这个
> 策略算出 100ms–1s 的不可见时长请求，而 `internal/rpc/receive.go` 会取
> `max(客户端要求, 退避下限)`——客户端的请求被静默覆盖掉」，并据此主张把消费端
> 的 `Initial/Max` 改成 `10s/5min` 以「与 broker 实际施加的一致」。**这个前提是
> 错的**，核对实现后有两处硬伤：

1. **那条覆盖只发生在 Receive 的回填路径，不发生在 nack 路径。**
   `toPBMessage` 里的 `eff = max(invisible, deliver.RetryBackoff(attempt))` 只在
   投递消息时回填 `InvisibleDuration` 字段。而 SDK 消费失败走的是
   `ChangeInvisibleDuration` RPC → `deliver.ChangeInvisible` →
   `ist.ExpireAtMs = time.Now().Add(invisible)`——**原样透传，一处下限都没有**。
   客户端要多久就是多久。

2. **顺序消息被显式排除。** 回填那条的判据是 `m.DeliveryAttempt >= 2 &&
   m.MessageGroup == ""`，与 deliver 侧 `receiveOnce` 的 `!r.ordered` 对齐；注释
   写明「顺序重投要的是快速原地重投，退避仅限非顺序」。

于是初稿方案的真实后果与它宣称的相反：把消费端 `Initial` 从 100ms 改成 10s，会让
**push 消费失败的重投从 100ms 变成 10s**（因为 nack 请求真的会被原样采纳）；`Max`
改成 5min，则顺序消息在反复失败时最坏会把队列**队头阻塞 5 分钟**（顺序锁下每队列
至多一条未终结 inflight）。这是行为退化，不是修正。

**结论：`Initial` / `Max` / `Multiplier` 三个字段的消费端值维持现状不动。**

## 3. 方案（收窄后）

把 `BackoffPolicy` 的赋值从 `negotiateSettings` 的公共段挪进两个分支，
`backoffPolicy()` 拆成两个：

- **`publishBackoffPolicy()`**：现有 `backoffPolicy()` 原样保留改名，值不变。
  producer 的行为**零变化**。
- **`subscribeBackoffPolicy(group string)`**：**退避三要素与发布端逐字段相同**
  （见 §2.2：改它是行为退化），只有 `MaxAttempts` 换成该消费组的真实值。

也就是说，本条目最终只改一个字段。这不是缩水——`MaxAttempts` 恰恰是唯一一个
「下发值与 broker 真实判定不一致」的字段，另外三个本来就没有不一致。

### 3.1 `MaxAttempts` 的取值与回退

`negotiateSettings` 的 Subscription 分支已经有 `ps.Subscription.GetGroup()`。
用 `mt.GetGroup(name)`（只读，不建组）查：

- 查到 → `EffectiveMaxAttempts()`
- 查不到（首次连接、组尚未 EnsureGroup）→ **`s.cfg.DefaultMaxAttempts`**

**回退值必须取配置，不能用包常量 `meta.DefaultMaxAttempts`。** 后者是 meta 包的
兜底常量（16），而组的实际默认值来自 `meta.New` 收到的 `defaultMaxAttempts` 参数
——它由 `cmd/sq/main.go` 从 `cfg.DefaultMaxAttempts` 传入。用包常量会在用户改过
配置时下发一个谁也没配过的 16。

**用 `GetGroup` 而不是 `EnsureGroup`**：settings 协商是只读的协议协商，不应有
建组副作用。真正建组由 ReceiveMessage 路径负责。

### 3.2 退避三要素为什么要显式重复而不是留空

`subscribeBackoffPolicy` 里把 `Initial/Max/Multiplier` 再写一遍（取与
`publishBackoffPolicy` 相同的常量），而不是只填 `MaxAttempts`：proto3 下缺省的
`ExponentialBackoff` 会让 SDK 拿到 0 值退避（`nackMessage` 立刻重投，打满 broker）。
留空不是「保持现状」，是把现状改成更坏的东西。

这个「刻意保持相同」必须有测试钉住（§5 T2），否则后人看到两个分支写着同样的三行
常量，会以为是可以合并的重复代码——而合并回公共段正是本条目要消灭的东西。

## 4. 这不改变 `fifo` 的结论，也**不改动那段注释**

> **本节已订正。** 本 spec 初稿写着「B13.2 的三条理由里第二条是『客户端按
> `MaxAttempts=3` 转 DLQ，与 broker 的 16 判定不一致』，本条目消除了它，实现时
> 须把该条划掉」。**这是错的。** 核对链尾 `feat/auto-renew` 上
> `internal/rpc/settings.go` 的实际注释，根本没有「判定不一致」这条理由。按初稿
> 施工会去划掉一条**正确且仍然成立**的理由。

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
- T2 **消费端的 `Initial` / `Max` / `Multiplier` 与发布端逐字段相同**——这条钉住
  §3.2 的「刻意保持」，防止后人照初稿的错误论据把它改成 10s/5min。
- T3 `MaxAttempts` 取消费组真实值：建一个 `MaxAttempts=5` 的组，断言下发 5。
- T4 组不存在时回退 `cfg.DefaultMaxAttempts`（**不是包常量**），且协商不得建组。
- T5 **两个分支的 `BackoffPolicy` 不相等**——本条目的核心断言，把「又被合并回
  公共段」这个回退变成红灯。差异点在 `MaxAttempts`（单机档 3 vs 组真实值）。
- T6 客户端未上报可识别 PubSub 时，`PubSub` 与 `BackoffPolicy` 一并留空。

## 6. 需要用户复核的假设

- **A1（原判修正）**：backlog 原记「协议只有一个字段，承载不了双重语义」不成立。
  `Settings` 逐客户端协商，字段本就够用；缺口在 sq 少分了一次岔。**本条目因此
  从「评估要不要改」变成「有明确修法的小改动」。**
- **A2（行为变更，已收窄）**：消费端下发值只有 `MaxAttempts` 变化（`3`/`5` →
  组真实值）。评估：`fifo=false` 下 SDK 不使用该字段，**存量消费者行为零变化**；
  改动的意义是让协商描述诚实。退避三要素维持现值不动。
- **A3**：`fifo` 仍为 false（§4）。三条理由**一条都没被本条目消除**，那段注释
  不改动。此处与初稿相反，订正理由见 §4 的引注。
- **A4（明确不做，且不应再被提起）**：初稿主张的「让消费端 nack 退避镜像 broker
  的 `retryBackoff`（10s/5min）」**基于错误前提，本条目明确不做**。真实情况是
  nack 走 `ChangeInvisibleDuration`、broker 原样透传不设下限（§2.2），照初稿改会
  把 push 重投从 100ms 拉到 10s、顺序消息最坏队头阻塞 5min。若将来确有「让客户端
  退避与 broker 一致」的需求，那是一个**独立的行为变更条目**，需要单独评估重投
  节奏与队头阻塞，不能作为本条目的顺带改动。
  连带后果：`deliver` 侧**不新增** `RetryBackoffParams` 导出访问器——它的唯一用途
  就是喂这个已被否决的方案，`internal/core/deliver` 因此完全不改动。

## 7. 日志与注释要求

- `publishBackoffPolicy` / `subscribeBackoffPolicy` 各需注释说明**它服务的是哪
  一类客户端、SDK 拿它做什么**——这正是当初合并成一个函数时丢失的信息。
- `subscribeBackoffPolicy` 里三个退避要素与发布端相同的那几行，需注释写明
  **这是刻意的、不是可合并的重复**，并点明「改成 10s/5min 会把 push 重投拉到
  10s、顺序消息队头阻塞到 5min」这条理由（§2.2 与 A4）。这是本条目最重要的注释：
  没有它，下一个读到初稿论据的人会把它「修」回去。
- 回退分支需注释写明取的是 `cfg.DefaultMaxAttempts` 而非包常量，及其理由（§3.1）。
- `settings.go` 中 `fifo` 那段注释**保持原样，一个字都不要改**（§4 订正后的结论；
  本 spec 初稿要求更新它，那是基于对原文的误记）。
- 协商时组查不到而回退默认值 → Debug 一条（含 group 与回退值）：这是「为什么
  这个消费者拿到的是默认值而不是它组里配的值」唯一的排查线索。
- 本改动无错误分支（`GetGroup` 只读不报错），故无新增 Error 日志。
