# raft 异步存储写入（AsyncStorageWrites）设计

日期：2026-08-12
状态：**已实测——验收未通过，本方案单独落地会全面劣化，不合入**（2026-08-12 三机实测）

> **实测结论（Task 6，同批机器 main=`1b2e680` vs async=`1f002b3`，轮内交错 3 轮）**
>
> 八格全部劣化，三轮同向、离散度多在 4% 以内：quorum-fsync −6.3%/−48.9%/−27.0%/−25.2%，
> quorum-mem −4.9%/−1.8%/−5.0%/−5.7%（**mem 不得劣化的硬底线未通过**）。
>
> 根因由本文 §8 最能证伪的那条指标直接坐实——用 `/proc/diskstats` 第 19 字段
> 直接数下盘 flush 次数，**fsync 摊薄比不升反降**：C1/256 由 4.11 条/次跌到
> 1.60（×0.39），C3cross/16 由 1.49 跌到 1.01（×0.67）。且劣化幅度与「原本
> batch 有多大」严格正相关：main 摊薄比最高的 C1/256 跌得最狠（−48.9%），
> main 本就几乎没有 batch 的 C1/16 只跌 6.3%。
>
> **机理**：同步路径下「主循环阻塞在 fsync 里」本身就是一次隐式 group commit
> ——阻塞期间新提案在 raft unstable log 里堆积，下一轮 Ready 因此携带一个大
> 批次，一次 fsync 摊到很多条。打开 AsyncStorageWrites 后主循环不再阻塞、
> 立刻回头取下一个 Ready，同样的提案流被切成更多更小的 `MsgStorageAppend`，
> 而 `runAppend` 是**一条一 `Persist`、一条一 fsync**，没有任何合并——
> 流水线是打开了，但它换掉的那个隐式 group commit 没有东西接替。
>
> **本文 §1 的前提因此需要修正**：+69% 的 raft 机制税不是「流水线深度不够」，
> 至少主要不是；它更像是「单节点集群下 batch 规模本就小」。异步化只有在
> **append 阶段自带 group commit** 时才可能为正——即 `runAppend` 批量抽干
> `appendCh`、多条 `MsgStorageAppend` 各自 `Persist(sync=false)` 后合并成
> **一次** `seglog.Log.Sync()`，再统一投递 Responses。该修正未实现、未验证。
>
> 语义面无问题：`test/e2e` 全量零修改通过（`ok 1159.589s`），
> `go test ./... -race` 18 包全绿。劣化是纯性能问题，不是正确性问题。
> 原始数据：会话 scratchpad `attribution/rawB/`（40 份 e2e 输出 + flush 计数）。
前置：`2026-08-08-sq-v2-replication-design.md`（V2 复制设计，本文改的是它 §5 Ready 处理的**驱动方式**，不改任何 raft 语义与确认档语义）
姊妹项：`2026-08-12-seglog-prealloc-fdatasync-design.md`（A，攻单次同步落盘成本；与本文完全解耦，可任意先后落地）

## 1. 背景：归因指向流水线深度，不指向复制

2026-08-12 四档归因测量（S 单机 → C1 单节点集群 → C3-cross 三机 →
C3-colo 单机三进程，全新三机 2 核/1612MB/vda，5 轮交错取轮内配对）：

| ack | conc | raft 机制税 S→C1 | 复制+网络税 C1→C3cross | 共置税 C3cross→C3colo |
|---|---|---|---|---|
| mem | 16 | +21.4% | +8.5% | +50.0% |
| mem | 256 | +21.6% | +5.1% | +49.7% |
| fsync | 16 | **+69.2%** | −12.1% | +52.8% |
| fsync | 256 | **+44.5%** | +54.8% | +51.7% |

**复制+网络税 ≈ 0**（fsync/16 档甚至为 −12%，三机快过单节点集群）。
「三节点比单节点慢」几乎全部来自共置税（恒定 +50%，纯 CPU 争抢，
iowait 仅 0.2%——部署建议，非可修 bug）。**唯一的软件侧大头是 raft
机制税**，fsync 档高达 +69%，这就是本文的目标。

两条交叉验证把它定位到了「流水线深度」而非「fsync 太慢」：

- **C1/fsync/16 = 1263 msg/s**，3 个数据组 ⇒ 盘 conc=3 约 1120 sync/s
  ⇒ **约 1 条消息 1 次 fsync，完全没有摊销**。而同档单机 standalone
  4427 msg/s、盘 conc=1 仅 528 sync/s ⇒ pebble 把 16 个并发写者合成了
  **~8.4 条/fsync**。差别不在盘，在「pebble 会 group commit，raft 日志
  路径不会」。
- **pprof（C3-cross/fsync/256 稳态）：三节点各只用 0.14-0.19 台机器的
  CPU**（采样占空比 38.6%/27.8%/32.2% ÷ 2 核）。fsync 档不是 CPU 瓶颈，
  是**延迟串行化**瓶颈——大家都在等。

> 口径留痕：该批机器第 3→4 轮出现台阶式提速 +31%，**绝对吞吐跨轮离散度
> 15-50%，一律不可引用**，只有轮内配对税率与盘微基准可信。另
> `runSendLoad` 只连 `endpoints[0]` 且客户端与 node1 同机，mem/256 档机器
> A 已 99% 饱和而 B/C 仅 48-60% ⇒ **C3-cross 是下界**。

## 2. 机理：每组流水线深度恒为 1

两处叠加造成的：

1. **`handleReady` 严格串行**：持久化（`group.go:396`，fsync 档带 fsync）
   → 发 Messages（`group.go:417`）→ apply（`group.go:443`）→ `Advance`
   （`group.go:532`）。
2. **`node.run` 在非 async 模式下投出 Ready 后必须等 `advancec` 才产下
   一轮**（`node.go:437-441`）。

于是 leader 做 fsync 的那 1.8ms 里，`MsgApp` 一个字节都发不出去——
follower 连自己的 fsync 都还没开始。一次提案的确认链是：

```
leader fsync (1.8ms) → 网络 → follower fsync (1.8ms) → 网络 → commit
```

**两次 fsync 串行相加，且 leader 那次挡住整条链**。深度为 1 时 raft 也
根本没有机会攒批，「约 1 条消息 1 次 fsync」由此而来。

**raft 契约明确允许 leader 在自己 fsync 完成之前就发出 `MsgApp`**——
leader 只需在 **commit** 前 durable，不需在 **replicate** 前 durable。
`AsyncStorageWrites` 就是为此设计的（`raft.go:151-184`），
`go.etcd.io/raft/v3 v3.7.0` 原生支持，**不需要 fork**——守住
`2026-08-08-sq-v2-replication-design.md` §8.1 定下的「不 fork raft」约束。

## 3. 目标与非目标

**目标**：打开 `AsyncStorageWrites`，把 `handleReady` 从「串行四步」改成
「主循环分发 + 本地 append/apply 异步阶段」。收益有两层：

1. leader 与 follower 的 fsync **并行**，而不是串行相加；
2. 同一组允许多轮 Ready 在途，raft 自然攒出更大的 `Entries` 批——
   **「组内合批」是它的副产品，不需要手写合批逻辑**（这一点是本方案取代
   「手写组内 group commit」的关键理由：后者要延后 `Advance`、要自己兜住
   `MustSync` 契约，风险高且收益不确定）。

**硬约束：raft 语义与确认档语义零变化。** 确认档（`quorum-mem` /
`quorum-fsync`）的持久性等位不变；`MustSync` 判定（`group.go:928`）语义
不变，只是承载它的载体从 `Ready.HardState/Entries` 变成
`MsgStorageAppend`；上层 `manager`/`replication`/恢复判定表零改动。

**非目标**：
- 不改帧格式、不碰 `seglog`（那是姊妹项 A）；
- 不改快照安装流程的语义（形态随分发调整，语义不动）；
- 不改截断、成员变更、读屏障的语义；
- **不做共置优化**——共置税是 CPU 争抢，写部署文档，不写代码。

## 4. 切换策略：彻底切换，不留配置开关

只保留 async 一条路径。理由：`handleReady` 是集群正确性的核心，两套路径
长期共存必然腐化，且会制造「两条路径只测了一条」的虚假安全感。风险靠
e2e + 三机实测兑，不靠开关兑。

代价是回退成本高——回退等于 revert 整个改动。因此本改造**单独成支、单独
验收**，不与其他改动混合。

## 5. 三个正确性要害

### 5.1 local message 绝不能走 `gr.send`

raft 要求：**同一 target 的消息必须可靠、有序处理——不能丢，不能重排**
（`raft.go:163-165`）。而现有 `gr.send` 的契约恰恰相反：transport 发送
**永不阻塞，满则丢，靠 raft 心跳重试兜底**（`group.go:412-413`，Task 3
契约）。

丢一条 `MsgStorageAppend` = 静默丢日志，且 raft 会一直等那条永远不来的
`MsgStorageAppendResp`——组静默卡死，没有任何报错。**这是本改造最容易
踩死的坑**。

分发必须按 `m.To` 严格三分：

| `m.To` | 去向 | 可靠性要求 |
|---|---|---|
| `raft.LocalAppendThread` | 本地 append 通道 | **可靠有序**，满则阻塞，绝不丢 |
| `raft.LocalApplyThread` | 本地 apply 通道 | **可靠有序**，满则阻塞，绝不丢 |
| 其他（真实 peer id） | `gr.send`（现状不变） | 可丢，心跳重试兜底 |

分发处必须有显式注释说明这三行的差别，以及「走错一路的后果是静默卡死
而非报错」。

### 5.2 `mem.Append` 必须先于 Responses 投递

raft 读已 stable 的日志走 `MemoryStorage`（双记账），而「stable」的判定
就是 `MsgStorageAppendResp` 投回的那一刻。今天这两件事在主循环里顺序
执行（`group.go:396` → `:411`），天然有序；async 后它们跨 goroutine，
顺序约束从「代码顺序」退化成「必须显式保证的 happens-before」。

append 阶段的严格顺序：

1. `rs.Persist(...)`（`MsgStorageAppend` 带 Responses 时必须 durable）
2. `mem.SetHardState` / `mem.Append`（双记账）
3. 投递 `m.Responses`

**任何一步提前都会让 raft 读到还不存在的日志。** 该顺序必须在代码里以
注释锁死，并有针对性用例。

### 5.3 durable 要求：append 严、apply 松

- `MsgStorageAppend`：**带 Responses 时，所有写入必须在投递 Responses
  之前 durable**；不带 Responses 时不要求（`raft.go:167-172`）。这正好
  承接现有 `syncPersist`（`group.go:928`）的 `MustSync` 判定——语义不变，
  只是判定输入换了载体。
- `MsgStorageApply`：写入**不要求** durable 即可投递 Responses
  （`raft.go:173-176`）。

这条差别决定了 apply 阶段可以比 append 阶段跑得松，是收益的一部分，
不能因为「保险起见都同步」而抹平。

## 6. 需要重新论证的既有逻辑

`Advance` 消失、apply 移出主循环，以下四处的时序假设全部失效，必须逐条
重走：

| 位置 | 原假设 | 需重新论证的点 |
|---|---|---|
| apply 合批（`applyEntries`，db04fdb） | 主循环内独占执行 | `applyMu` 临界区在独立 goroutine 下的竞争面；合批边界是否仍与 `MsgStorageApply` 的批边界对齐 |
| `ccWaiters` 唤醒（`ccApplied`） | 在 `Advance` 之后通知 | **没有 `Advance` 了**，新的通知时机是什么；跨节点条目 id 碰撞的假成功防护是否仍成立 |
| `SaveConfState` 与段冲刷顺序 | 主循环内顺序保证 | 成员变更用独立批次写成员表 + applied 位点，普通条目段必须先冲刷——异步后如何保序 |
| 快照安装分支（`handleReady` 第 0 步） | 安装期间本组暂停处理普通条目 | 快照现在随 `MsgStorageAppend` 到达，「暂停」的表达方式与 tick 保活如何重构 |

## 7. 风险

- **侵入式改造**：`handleReady` 是集群正确性的核心，改动面覆盖持久化、
  发送、apply、成员变更、快照五条链路。
- **失败模式不友好**：§5.1 的坑一旦踩中，表现是**组静默卡死而非报错**。
  因此分发路径必须有可观测性——local 通道的深度、阻塞时长、
  `MsgStorageAppend`/`Resp` 的配对情况都要能从日志/指标看出来。
- **收益依赖负载形态**：fsync 档收益最大（+69% 的税就在那里），mem 档
  预期收益小得多（+21%，且其中多少来自流水线深度尚未拆开）。
  **mem 档不得劣化**是硬底线。

## 8. 验收标准

1. **语义锚**：现有集群 e2e 用例（`test/e2e/sdk_cluster_test.go` 全量）
   零修改通过；`internal/cluster` 现有单测除时序假设确实变更的以外零修改
   通过——每一处修改都必须在 plan 里单独说明理由。
2. macOS `-race` 全量绿。
3. **三机实测（同批机器，seglog 基线对照）**：quorum-fsync 档
   conc=16/256 各跑 ≥3 轮交错，报中位数 + 离散度。预期 raft 机制税
   （S→C1）从 +69% 显著下降。
4. **mem 档不劣化**（硬底线）。
5. **重跑 C1 的 fsync 摊销比**：`msg/s ÷ (盘同并发档 sync/s)` 应从
   ~1 条/fsync 明显上升——这是「流水线深度真的打开了」的直接证据，
   比吞吐数字更能证伪。
6. 可观测性：local 通道阻塞与 `MsgStorageAppend`/`Resp` 配对异常必须
   有日志，且能在不复现的情况下靠 `search_logs` 定位（§7 第二条的
   兑现方式）。
