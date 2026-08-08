# Produce 写吞吐优化设计：group commit 解锁 + 默认队列数 + AppendBatch + soak 基准

日期：2026-08-08
状态：已评审（用户确认范围与三个关键决策）

## 1. 背景与问题

两轮基准测试（2015 老笔记本 + 入门云服务器 2 vCPU/1.6GB/云盘）得出一致结论：

- **写吞吐由队列数决定，不由 fsync 决定。** 云盘裸 fsync 2.19ms（456 次/s），单队列
  实测 435 msg/s——严丝合缝，因为 `produce.Append` 的每队列锁（`qs.mu`）横跨
  `store.Apply`（含 fsync），同队列写入是一条串行 fsync 链。
- **跨队列 group commit 有效。** 256 队列 + 256 并发在真实 fsync 下 56,990 msg/s，
  而无 fsync 纯软件上限 98,135 msg/s——fsync 被 Pebble commit pipeline 大量合并。
- **默认配置是最要紧的坑。** `default_queue_nums: 4` 让所有自动建 topic 封顶
  ~900 msg/s，与硬件和客户端并发无关。
- **协议层批量入口已存在但被浪费。** `SendMessageRequest` 是 `repeated Message`，
  服务端已做批内同 topic 校验，但第二遍循环逐条 `Append`——N 条消息 N 次 fsync。

经验公式（写入 README）：`吞吐 ≈ min(队列数, 并发) × fsync速率 × 合并系数`。

## 2. 目标与非目标

### 目标（验收基准硬件：云服务器 2 vCPU / fsync 456 次/s）

| 指标 | 现状 | 目标 |
|---|---|---|
| 单队列、64 并发 | 435 msg/s（fsync 封顶） | ≥ 2,000 msg/s |
| 默认配置自动建 topic、64 并发 | ~900 msg/s（4 队列） | ≥ 3,000 msg/s（16 队列） |
| 单连接批量发送（32 条/批） | ~435 msg/s（逐条 fsync） | ≥ 5,000 msg/s |
| 持续 10 分钟高写入 | 未验证 | 吞吐无崩塌（compaction 稳态） |

### 非目标

- async durability 分档（按 topic/请求降低刷盘保证）——语义变化大，独立 spec。
- 消费/投递路径优化。
- `p.mu` 全局唤醒锁分片——等 group commit 落地后 profile 出证据再做。
- soak 基准进 CI / 定期自动跑——临时云服务器会回收，现阶段手动跑。

### 语义红线（全程不变，任何实现取舍不得触碰）

1. 每条消息 **fsync 完成后才 ACK**（掉电不丢已确认消息）。
2. 同队列 **offset 顺序 == 落盘顺序**（FIFO 根基）。
3. 消息体与 offset 计数器（AllocKey）**同一 Batch 原子落盘**（崩溃恢复一致性）。

## 3. 组件一：解锁 group commit（核心，优先落地）

### 现状

`internal/core/produce/produce.go` 段 2：`qs.mu` 从 offset 分配一直握到
`p.st.Apply(b)` 返回，而 Apply 内含 fsync 等待。同队列第 2 条消息要等第 1 条
整个 fsync 完成才能进锁。Pebble 的 group commit 只能合并**跨队列**的并发提交。

### 改法

锁只保护「定序」，「等待落盘」挪到锁外。采用 Pebble 拆分式提交 API
（`DB.ApplyNoSyncWait` + `Batch.SyncWait`，CockroachDB 写 raft log 的生产验证模式）：

**store 层**（`internal/store/store.go`）新增，与现有 `Apply` 并存：

```go
// ApplyAsync 提交批次（写 WAL 缓冲、发布可见、定序）但不等待 fsync；
// 调用方随后必须调 WaitSync 等待持久化。
// s.sync=false（配置关闭刷盘）时退化为一次性 NoSync Commit，WaitSync 直接返回。
func (s *Store) ApplyAsync(b *pebble.Batch) error
func (s *Store) WaitSync(b *pebble.Batch) error
```

**produce 层** `Append` 段 2/3 重排：

```
qs.mu 锁内: 分配 offset → 编码 → 组 Batch → ApplyAsync → qs.next = off+1
解锁
锁外:      WaitSync → 成功后才 wakeLocked（段 3）+ 返回 ACK
```

同队列 N 条在途消息同时挂在 commit pipeline 等 sync，Pebble 合并为一次 fsync；
单队列吞吐从 `1/fsync延迟` 变为 `合并深度/fsync延迟`。

### 关键论证

- **FIFO 不破**：offset 分配与 WAL 定序都在锁内完成，顺序钉死后才解锁。
- **可见性窗口没有扩大**（要写进代码注释防止后人误改）：Pebble 的
  `Commit(Sync)` 本身就是「先发布可见、后等 fsync」，现状下拉取型读者已可能在
  fsync 完成前读到消息。本改动只是把等待挪到锁外，窗口性质不变；长轮询唤醒
  依旧在 fsync 成功之后。
- **WaitSync 失败路径**：此时 `qs.next` 已推进，但 WAL sync 失败意味着 Pebble
  进入不可恢复错误态，后续所有写都会失败；重启后 offset 计数器与实际落盘
  由同批原子性保证严格一致，无害。对该条消息返回错误、不 ACK。
- **Batch 生命周期**：沿用现有约定——失败批次不 Close 丢给 GC（store.go 注释
  已论证）；成功批次在 WaitSync 后按 Pebble 要求处理。
- **观测口径**：`OnApplyObserve` 直方图改为覆盖 ApplyAsync→WaitSync 全程，
  与现在 Apply 全程的口径一致。
- `AppendDelay`/事务 Stage 等其它 5 个 Apply 调用点**不改**——它们不在热路径，
  维持简单的同步 Apply。

## 4. 组件二：默认队列数 4 → 16

- `internal/config/config.go` `DefaultQueueNums: 4` → `16`（一行）。
- 只影响**新自动建**的 topic；存量 topic 队列数持久化在 meta，不受影响。
- 选 16 的理由：云盘上约 3-4k msg/s，覆盖 5k 目标大半；group commit 解锁后
  单队列不再是死刑，16 足够；对消费者端并发要求适中。实测内存不随队列数涨
  （256 队列时 RSS 71.5MB）。
- README 同步：经验公式、`default_queue_nums` 调优指引、高吞吐 topic 建议
  显式建 topic 指定更多队列。

## 5. 组件三：AppendBatch（批量落盘）

### 现状

`internal/rpc/send.go` `SendMessage` 已接受多条消息并校验批内同 topic，
但逐条调 `pr.Append`——N 条 N 次 fsync，且第 N 条失败时前 N-1 条无法撤回
（现注释以 at-least-once 论证）。

### 改法

- **路由**（send.go）：`len(msgs) > 1` 且**全部为普通消息**（无事务、无延时）→
  走新的 `pr.AppendBatch(msgs)`；含事务/延时消息的多条请求维持现有逐条循环。
  官方 SDK 的 batch send 本就只含普通消息，回退路径保证零行为变化。
- **AppendBatch 语义**（produce.go）：整批落**同一队列**（与 RocketMQ batch
  绑定单 MessageQueue 的语义一致）：
  - 一次 round-robin 队列选择（批间仍轮转，长期均衡不受影响）；
  - 一个 `qs.mu`、连续 offset 段 `[off, off+N)`；
  - 一个 Pebble Batch：N 条消息 + 各自 keys 索引 + AllocKey 一次写到 `off+N`；
  - 一次 ApplyAsync/WaitSync——整批一次 fsync。
- **原子性收益**：整批要么全落盘要么全不落，比现状逐条更强；send.go 的
  at-least-once 注释相应简化（批路径不再有"部分成功无法撤回"）。
- 单条请求（`len==1`）走原有 `Append`，路径零变化。

## 6. 组件四：soak 基准入库（手动跑）

- 基准过程遗留的 `bench_qsweep_scratch_test.go` 转正：移入
  `internal/core/produce/`，去 scratch 后缀，补文件头注释，作为队列数/并发
  扫描的标准复现工具。
- 新增 soak 模式：长跑基准每 10 秒用 logger 打点吞吐；`Makefile` 加
  `make soak`（10 分钟，生产形态配置：16 队列 / 64 并发 / 真实 fsync）。
- 观察性验收：后半程平均吞吐 ≥ 前半程 70%；无 >5s 的写停顿（L0 stall）。
  不达标则调 Pebble 参数（MemTableSize、L0 阈值、WALBytesPerSync）另立任务。

## 7. 测试策略

1. **顺序不变式（最关键）**：并发多 goroutine 写单队列，读回验证 offset 连续
   且与落盘顺序一致——group commit 解锁后的 FIFO 回归测试。
2. **AppendBatch 单元测试**：offset 连续性、keys 索引齐全、AllocKey = off+N、
   注入 Apply 失败验证整批原子回退、空批/单条批边界。
3. **回退路由测试**：含延时/事务消息的多条请求仍逐条处理，行为与现状一致。
4. **性能验收**：qsweep 基准在云服务器改动前后对照，按 §2 表格数字验收。
5. **回归**：既有全量单测 + e2e（含 FIFO、recovery、txn、delay）全绿。

## 8. 实施顺序

1. 组件一（store ApplyAsync/WaitSync + Append 解锁）——其余组件的地基。
2. 组件三（AppendBatch，复用组件一的接口）。
3. 组件二（默认队列数 16 + README）。
4. 组件四（soak 基准转正 + 云服务器验收跑）。

每步独立成 commit、独立可验证；1/2/3 完成后在云服务器跑一次 §2 全表验收。
