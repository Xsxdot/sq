# sq — simple queue 设计文档

- 日期：2026-08-06
- 状态：设计已确认，待转入实现计划
- 定位一句话：**RocketMQ 协议兼容、单二进制、无 JVM 的轻量消息队列**

---

## 1. 背景与调研结论

目标：做一个开源消息队列，具备类 RocketMQ 的完整功能（普通消息、顺序消息、延时消息、事务消息），支持主备与集群，不追求高性能高并发，面向中小开发者，易用易部署。

2026-08 调研结论（详细调研数据见调研记录）：

- **「四种消息类型 + 主备/集群 + 轻量易部署」的组合在开源市场是空档。** 分水岭是 RocketMQ 式半消息+回查事务消息，Kafka/RabbitMQ/Pulsar/NATS/NSQ/Redis Streams 均无等价物。
- 现成方案中最优解是 RocketMQ 5.x 自身（功能 100%，但 JVM 结构性重，无官方轻量发行版）；NATS JetStream 是轻量侧最佳（但无事务、分区有序弱）。
- 最接近的先行者是去哪儿 QMQ（普通/延时/事务齐全），但顺序消息从未开源、强依赖 MySQL、2023 年底停止维护。其延时消息（两层时间轮+schedule log）与事务（本地消息表）设计文档是重要架构参考。
- **Go 生态中不存在实现「延时+事务+顺序」的原生 broker，此位置空白。**

因此决策：自研（而非二次打包 RocketMQ 或基于 NATS 补层）。

## 2. 核心决策记录

| 决策点 | 结论 | 理由摘要 |
|---|---|---|
| 路线 | 自研新开源项目 | 市场空档真实存在，Go 生态无先行者 |
| 语言 | Go | 性能非目标故 Rust 优势打折；作者技术栈为 Go；hashicorp/raft、Pebble 等生产级积木现成；目标用户与贡献者池 Go 更大 |
| 协议 | 兼容 RocketMQ 5.x gRPC 协议（apache/rocketmq-apis），内部留协议适配层 | 官方五语言 SDK 零成本可用；事务/顺序/延时语义协议已定义；迁移成本≈零。core 不 import proto，协议可插拔 |
| 版本节奏 | 纵切：v1 单机全功能，v2 主备/集群 | 先交付差异化卖点；复制层放内核稳定后。v1 存储按「可复制的日志」建模防返工 |
| v1 功能优先级 | 普通(含重试/DLQ) → 延时 → 顺序 → 事务 | 作者自用需求：普通/延时/顺序马上要用，事务不急 |
| 存储引擎 | 单一嵌入式 KV（Pebble） | 代码量最小；崩溃恢复交给 Pebble；四种消息自然建模；延时=区间扫描无需时间轮；LSM 写放大在目标量级无关紧要 |
| 消费模型 | RocketMQ 5.x POP 模式 | 服务端分配消息与负载均衡，无需客户端 rebalance；5.x SDK 默认模式 |
| 运维界面 | v1 内嵌 Web 控制台 | 「启动即见一切」与单二进制是同一卖点的两半；RocketMQ 官方 dashboard 走 remoting 协议不可用 |
| 控制台技术栈 | React 18 + TS + Vite + shadcn/ui + Recharts，go:embed | 现代观感；数据全走公开 Admin API，无私有通道 |
| 命名 | 项目/二进制名 sq，全称 "sq — simple queue" | 与 sq.io 数据 CLI 撞名，发布期在意 brew 冲突时再议 |

## 3. 整体架构

单二进制进程，四层，依赖方向严格从上到下：

```
┌─────────────────────────────────────────────────┐
│ 接入层                                           │
│  · gRPC Server（RocketMQ 5.x proto 兼容）        │
│  · HTTP Admin API                                │
│  · 内嵌 Web 控制台（go:embed 静态资源）           │
├─────────────────────────────────────────────────┤
│ 协议适配层                                        │
│  · rocketmq-adapter：proto 语义 ↔ core 内部 API   │
│    core 不 import 任何 proto                      │
├─────────────────────────────────────────────────┤
│ 核心引擎 core                                     │
│  · meta：topic / 订阅组 / 队列元数据               │
│  · produce：写入路径（延时判定、半消息判定）        │
│  · deliver：消费投递（消费组、位点、长轮询、        │
│    不可见超时、顺序锁、Tag 过滤）                  │
│  · delay：延时调度器（扫 delay/ 前缀区间）          │
│  · txn：事务管理器（半消息 + 回查调度）             │
│  · retry：重试与死信（计数、退避、入 DLQ）          │
├─────────────────────────────────────────────────┤
│ 存储层 store                                      │
│  · Pebble 封装 + key 编码 schema 集中定义           │
│  · v2 预留：所有状态变更收敛为 Command 序列，        │
│    v1 直接应用到 Pebble，v2 改为 Raft apply 后应用   │
└─────────────────────────────────────────────────┘
```

两个关键架构约束：

1. **POP 消费模型**：`ReceiveMessage`/`AckMessage`/不可见时间。服务端分配消息、服务端负载均衡。顺序消息用队列级投递锁。
2. **Command 化写路径**：core 所有状态变更（写消息/ack/位点变更/事务提交…）收敛为统一 `Command` 序列。v1 中 Command 直接落 Pebble；v2 将该序列写入 Raft 日志、状态机 apply 后落 Pebble。core 与 store 不因 v2 重构。

## 4. 存储 Key 编码（store 层唯一 schema）

整数字段大端定长编码（字节序 = 数值序，保证区间扫描成立）。

```
meta/topic/{topic}                          → topic 配置（队列数、类型、retention）
meta/group/{group}                          → 订阅组配置（maxAttempts 等）
msg/{topic}/{queueId}/{offset:8B}            → 消息体（含系统属性）
cursor/{group}/{topic}/{queueId}             → 已提交消费位点
inflight/{group}/{topic}/{queueId}/{offset}  → 已投未 ack（不可见截止时间、投递次数）
delay/{到期ms:8B}/{seq}                       → 延时消息暂存（消息体直接存此处）
half/{下次回查ms:8B}/{txId}                   → 事务半消息
halfidx/{txId}                               → 半消息反查索引（Commit/Rollback 用）
keyidx/{topic}/{key}/{到达ms:8B}/{queueId}/{offset} → Keys 业务索引（空值）
```

## 5. 核心数据流

1. **普通发送**：`SendMessage` → adapter → produce 分配 queueId（轮询）与 offset（队列内单调递增）→ 写 `msg/`（Keys 存在时同 WriteBatch 写 `keyidx/`）→ 唤醒长轮询等待者。
2. **消费（POP）**：`ReceiveMessage` → 从 `cursor` 起扫 `msg/`，跳过 `inflight` 中的，服务端 Tag 过滤（不匹配则跳过并推进该消费组视角位点，不投递不占 inflight）→ 写 `inflight`（不可见截止 = now + invisibleTime）→ 返回批次。`AckMessage` → 删 `inflight`、`cursor` 推进到最小未 ack offset（空洞由 inflight 记录补差）。超时未 ack → 后台扫描重新可投，投递次数 +1。
3. **延时消息**：带 deliveryTimestamp → 写 `delay/{到期ms}/` 而非 `msg/`。调度器每 100ms 扫 `delay/` 头部，到期消息移入目标 topic `msg/`（正常分配 offset）并删 delay 条目，同一 WriteBatch 原子完成。
4. **顺序消息**：发送端按 MessageGroup hash 到固定 queueId；消费端队列加顺序锁——存在未 ack inflight 时不投后续消息；失败在队列头部阻塞重投（卡住而不跳过，即 RocketMQ FIFO 语义）。
5. **事务消息**：`SendMessage`(事务标记) → 写 `half/` + `halfidx/`，消费者不可见。`EndTransaction`(commit) → 原子移入 `msg/`；(rollback) → 删除。超时未决 → 回查调度器扫 `half/` 到期项，经 Telemetry 双向流下发 `RecoverOrphanedTransaction` 回查；回查上限默认 15 次，超限丢弃并记日志。
6. **重试/DLQ**：投递次数超订阅组 maxAttempts → 消息复制入自动 topic `%DLQ%{group}`（普通 topic，控制台可查可重发）→ 清 inflight、推进 cursor。非顺序消息重试指数退避（重投时设不可见时间实现）。
7. **消息过期（retention）**：按 topic 配置保留时长（默认 3 天），后台任务用 DeleteRange 清理过期区间的 `msg/` 与对应 `keyidx/`。可选按最大保留字节数从最老丢弃（默认仅按时间）。

RocketMQ 三个键概念的对应：MessageGroup（顺序分组）→ 流程 4；Tag（过滤）→ 流程 2 服务端过滤；Keys（业务索引）→ `keyidx/` + Admin API 按 key 查询（`QueryMessage` 同路径）。

## 6. 协议兼容层范围（apache.rocketmq.v2）

**v1 实现的 11 个 RPC**：

| RPC | 要点 |
|---|---|
| QueryRoute | 单机版恒返回本节点为唯一 broker |
| Heartbeat | 保活 + 消费组注册 |
| SendMessage | 四种消息统一入口，属性区分 |
| ReceiveMessage（服务端流） | POP + TAG 过滤 |
| AckMessage | 确认 |
| ChangeInvisibleDuration | 重试退避依赖 |
| EndTransaction | 事务提交/回滚 |
| Telemetry（双向流） | Settings 协商；回查命令下发通道；客户端指标（v1 仅记日志）。兼容层最复杂项 |
| NotifyClientTermination | 优雅下线 |
| ForwardMessageToDeadLetterQueue | 顺序消息重试超限显式入 DLQ |
| QueryAssignment | 单机版返回全量队列归属本节点 |

**v1 不做**：PullMessage/GetOffset/QueryOffset 一族（PullConsumer）、RecallMessage、SQL92 属性过滤（v1.1）。未实现 RPC 返回 `UNIMPLEMENTED`。

**协议验收标准**：直接以官方 rocketmq-clients 的 Java 与 Go SDK 做集成测试，四种消息 × 收发 × 重试 × 事务回查全链路通过才算协议层完成。官方 SDK 即协议一致性测试套件。

**认证**：可选静态 AK/SK（Signature 头校验），默认关闭。控制台独立简单密码登录。

## 7. 可靠性与错误处理

- **投递语义**：at-least-once，broker 不去重（与 RocketMQ 一致），文档明示业务侧幂等。
- **刷盘**：默认同步（`WriteOptions{Sync: true}`，Pebble group commit 合并 fsync）；`store.fsync = async` 可切异步（明示丢电风险）。默认值取安全侧。
- **崩溃恢复**：全部状态在 Pebble，WAL 回放由 Pebble 完成，不自写恢复代码。重启后 inflight 按不可见截止自然重投；延时/回查调度器扫前缀即恢复；长轮询由客户端重连重建。单一状态、单一事务边界（WriteBatch），不存在双状态对齐问题。
- **协议错误**：严格用 proto `Status` code（TOPIC_NOT_FOUND 等），保证官方 SDK 正确重试/报错。
- **磁盘水位保护**：超阈值（默认 85%）拒写保读，返回 FORBIDDEN。
- **时钟**：延时/回查依赖墙钟，文档要求 NTP；回拨仅导致延迟投递（扫描以到期 ≤ now 为准），不丢失不提前。

## 8. 可观测性（一等公民）

- 结构化日志：消息写入、投递、ack、事务回查、DLQ 转入、调度扫描等关键节点全覆盖；错误必带 topic/group/msgId 上下文。
- Prometheus `/metrics`：收发 QPS、堆积、inflight 数、延时队列深度、回查次数、fsync 延迟。控制台图表消费同一指标源。

## 9. 控制台与 Admin API

- 控制台数据全部来自 HTTP Admin API（REST），Admin API 是公开接口，可脚本化。
- v1 页面：总览（QPS/堆积/连接数）、Topic 管理（增删改、队列数、retention）、消费组（堆积、inflight、位点重置）、消息查询（Keys / msgId / 按 topic 浏览）、DLQ 查看与单条重发、延时队列视图、发送测试消息。

## 10. 测试策略

1. 协议一致性：官方 Java/Go SDK 集成测试（CI 常跑）。
2. 核心单测：key 编码、位点推进（空洞）、顺序锁、延时扫描边界、事务状态机。
3. 崩溃测试：随机时点 kill -9 后重启，校验「已 ack 不重投、已确认发送不丢」两条不变式。
4. 长稳：24h+ 持续收发，盯内存/磁盘曲线。
5. 性能基线声明：单机 5k msg/s 稳定即达标，不做极限优化。

## 11. 里程碑

| 里程碑 | 内容 | 出口标准 |
|---|---|---|
| M1 | store + 普通消息 + gRPC 骨架 | 官方 Go SDK 收发普通消息 |
| M2 | 重试/DLQ、Tag 过滤、Keys 索引、retention | 消费失败链路完整 |
| M3 | 延时消息 | 任意秒级延时，重启不丢 |
| M4 | 顺序消息 | 顺序锁 + 卡住语义，官方 SDK FIFO 用例过 |
| M5 | 控制台 + Admin API + metrics | 日常排查够用 |
| M6 | 事务消息 | 半消息 + Telemetry 回查全链路 |
| M7 | v1.0 发布打磨 | 文档、docker 镜像、systemd 单元、快速开始 |
| v2 | Raft 主备/集群（hashicorp/raft） | 3 节点，单机平滑升级 |

M1–M3 完成即可开始自用（普通+延时），M4 补齐顺序。

## 12. 明确不做（YAGNI 清单）

- 多协议兼容（Kafka/AMQP/MQTT）——定位就是 RocketMQ 协议
- 存算分离 / 分层存储（S3）
- 多租户、命名空间级配额
- 极限性能优化（零拷贝、自研存储格式）
- v1 阶段的任何复制/共识代码（仅留 Command 接口）
