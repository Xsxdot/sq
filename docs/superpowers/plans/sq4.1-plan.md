# SQ4.1 实现计划：消费状态改 leader 内存权威态 + 周期检查点

- 卡：SQ4.1（父卡 SQ4；并入并验收 SQ3）｜分支 `cards/sq4`（基于 main@21ee680）
- 需求：`docs/superpowers/specs/2026-11-11-sq4.1-consume-state-in-memory-design.md`（已批准 2026-11-11）
- 前置（硬依赖，本卡实现前 rebase 到其上）：SQ2（每组 FIFO 提案队列、入队绑任期）、SQ1（`Router.WaitWritable(ctx, g) (epoch, err)`、删 `InvalidateCounters`）。两卡目前只有已拍板计划，代码未落地。
- 级别：**L2（待 P1 拍板确认）**。不改 `replication.Replicator` / `Router` 签名（`WaitWritable` 是 SQ1 新增，复用不算本卡新增）；唯一可能被视为跨子系统契约面变化的是 P1 给 `cluster.Options` 加一个可选钩子。无骨架、无冻结对象；复用契约：`replication.Replicator`（Apply/ApplyAsync，SQ2 补齐「返回即定序」后的版本）、`replication.Router`（含 SQ1 的 `WaitWritable`）、`replication.Forwarder.ForwardApply`（wire 不变）、盘上 `cursor/`、`inflight/` 键与 inflight JSON 值（不变）。
- 状态：**已拍板（2026-11-11，主会话）**——P1–P6 均按推荐：P1 cluster.Options 新增可选钩子 BeforeLeaderTransfer，仅 balanceOnce 转移前调用，按装配点钩子处理、维持 L2；P2 管理批次统一经 deliver.ApplyAdminBatch；P3 围栏期阻塞 ≤3s；P4 S2(c) 取被消费队列 leader 所在 broker SIGTERM 重启；P5 S3 判据 ≤2×R×(I+1s)、逐次复制模式为 0；P6 分段 A（内存态+逐次复制，根治 SQ3）/段 B（检查点+钩子+故障注入 e2e）交付，卡验收以段 B 为准。新增不变式 I2（消费状态提案用生命周期 ctx、失败毒化整组）采纳。实现前置：SQ2、SQ1 落地并 rebase，第 1 步先核 SQ2 同任期失败前缀性，不成立即停下升级
- **修订 R1（2026-11-11，主会话，依 SQ2 独立审查 I-1）**：SQ2 落地后的作废语义只覆盖「丢弃那一刻已在队列里的同任期条目」，**不提供跨调用方的同任期失败前缀性**，原 H1 与 I2 的推导前提不成立。I1 的保证改为不依赖 SQ2 前缀性：
  - **检查点模式（默认）**：每个队列的检查点批次一律**全量替换**——DeleteRange 该队列 inflight 前缀 + Set 当前全部 inflight + Set cursor，同批。任一检查点批次单独落地都自洽（cursor 不会越过盘上缺失的 inflight），前序批次丢弃无害。代价 O(该队列 inflight)/每个检查点间隔，可接受。
  - **逐次复制模式**：差量批次保留，但**每个队列同时至多一个在途消费状态提案**——下一个差量在上一个 `Wait` 成功之后才构造（等待在队列锁外，构造前在锁内确认前序已了结）；前序失败即毒化并从盘重装。由此同队列差量严格前缀有序，不依赖 SQ2。
  - H1 改为核对「SQ2 保证同组调用顺序 = 日志顺序、跨任期条目不进新任期日志」两条（SQ2 已验收）；I2 改写为上述两条机制；U4 补两条用例：①检查点模式下前一批丢弃、后一批落地后从盘重装，cursor 之下的未 ack 消息全部仍在 inflight；②逐次复制模式下前序在途时后续调用不构造新差量。§5「H1 不成立即停下」条款随之按新 H1 判定。

---

## 待拍板岔口

| # | 岔口 | 推荐 | 被否方案与理由 |
|---|---|---|---|
| P1 | 计划内转移领导权（S2(b) 要求零重复）前的全量检查点从哪里挂 | **`cluster.Options` 新增可选钩子 `BeforeLeaderTransfer func(ctx context.Context, g uint32) error`**，只在 `Manager.balanceOnce` 调 `m.TransferLeader` 之前同步调用（摊布循环自己的 goroutine，不在 Ready 循环内，可阻塞；带上限 ctx，建议 2s）；返回错误即本周期不转移。main（组装点）把它接到 `deliver` 的「围栏 + 全组检查点」入口。nil 安全，单机档与现有测试零影响。判定仍为 L2：只是给组装点多一个可选回调，与现有 `OnApplied`/`OnLeaderChange` 同形，无独立工作单元共同依赖，无 wire 变化 | ① 不挂钩，计划内转移按故障处理：重复量与 kill -9 同级，直接违反 S2(b)「零重复」。② 钩子放进 `Manager.TransferLeader` 本身：覆盖全部调用方（含 cluster 包测试与 SQ1 的不变式测试里的 TransferLeader），会让测试里的同步转移多一次回调，改动面更大；生产唯一调用点就是 `balanceOnce`（读数见末节），放在调用点即可。③ 给 `Router` 加方法让 deliver 自己发起/感知转移：改 Router 接口 = 升 L3。④ deliver 轮询 raft `Status().LeadTransferee`：需要穿透 cluster 内部状态，违反穿透禁令，且转移开始时 raft 已在丢提案，来不及检查点 |
| P2 | 删除消费组（`adminops.PurgeGroupData` 直接 DeleteRange 旁路 deliver）如何让 leader 内存态失效 | **改为经 deliver 的「消费状态管理批次」唯一入口**：deliver 新增 `ApplyAdminBatch(ctx, g, b)`（取该批次触及的全部队列锁 → 丢弃这些队列的内存态 → 同步 Apply → 解锁）。本节点是该组 leader 时 adminops 直调；否则沿用现有 `ForwardApply`（wire 不变），由 leader 侧 main 的 `controlHandler`（组装点）识别「批次触及 `cursor/` 或 `inflight/` 键族」改调 `dl.ApplyAdminBatch`。adminops 通过一个定义在 adminops 内的小接口（使用方定义，第九条）拿到 deliver，不 import deliver 具体类型。另：队列枚举改为「pebble 扫描 ∪ meta 全部 topic×队列」，盖住「只在内存、尚未检查点」的队列 | ① deliver 订阅 OnApplied、看到删除批次就失效：挡不住「失效前已用旧内存构造、排在删除提案之后进日志」的检查点把数据写回来（删除提案可能由 follower 转发、随时插入日志；只有 leader 侧队列锁能把两者串起来）。② 新增一个「删除消费组」控制 op：新增跨进程 wire = 升 L3。③ 只允许在 leader 节点删组：集群档各数据组 leader 分散在不同节点，任一节点都删不全——功能回退 |
| P3 | 计划内转移的围栏期间，到达该组的取件/确认/改不可见怎么办 | **阻塞等待**，直到围栏解除（转移中止，TTL 到期，建议 3s）或领导权已失（此时返回 `ErrNotLeader`，rpc 已映射 HA_NOT_AVAILABLE），上限同时受调用方 ctx 约束。与 SQ1 F2「未追平阻塞等待」同一取向 | 立即返回 `ErrNotLeader`：转移通常在百毫秒内完成，SDK 拿到 HA_NOT_AVAILABLE 后重查路由仍指向本节点，白白烧掉重试预算（理由同 SQ1 F2 的被否方案） |
| P4 | S2(c)「消费侧正常重启」的解释 | **被消费队列 leader 所在 broker 进程 SIGTERM 优雅停机后重启**（spec「安全语义」最后一条说的就是 StopClean/SIGTERM 收尾检查点）。消费者客户端自己重启不改变服务端状态，按下文的「重复」定义天然为零，顺带在同一用例里覆盖 | 只测消费者客户端重启：测不到服务端停机检查点这条承重路径 |
| P5 | S3「重复有界」的精确判据 | 检查点模式下 kill -9 leader 后，**检查点丢失型重复**（定义见第 4 节）条数 ≤ `2 × R × (I + 1s)`，R = kill 前 5s 窗口内全体消费者的实测投递速率（条/秒），I = 检查点间隔。逐次复制模式同一用例此值必须 = 0（作对照，证明重复来自检查点而非别处） | 只断言「有重复且不丢」：不能区分有界与失控；按被杀 leader 承载的队列比例折算 R：需要从路由反推队列归属，判据更紧但脆，数字留在日志里供人看即可 |
| P6 | 分段交付 | **两段，同一张卡**：段 A = 内存态 + 逐次复制模式（不加配置键，main 固定逐次）+ 管理面失效，根治 SQ3，可单独合入集成分支；段 B = 检查点模式 + 配置键（缺省 1s）+ P1 钩子 + 故障注入 e2e。卡验收以段 B 完成为准 | 一次交付：检查点模式的风险（P1 钩子、e2e 零重复判据）会把 SQ3 根治一起拖住 |

---

## 1. 交付目标

对应 spec 的必交故事 S1–S6 与不变式 I1，做到：

1. 每个 (消费组, topic, 队列) 的 cursor 与 inflight 由该队列所属 raft 组的**可写 leader**（单机档即本进程）在内存持有；取件、确认、改不可见、续租、转死信、重置位点全部在队列锁内读写内存，不再读 pebble 的 `cursor/`、`inflight/`（消息本体 `msg/` 照旧读 pebble）。
2. 持久化两种模式：检查点模式（`consume_checkpoint_interval > 0`，缺省 1s）与逐次复制模式（`= 0`）；**两种模式用同一个批次构造函数**，区别只在「谁、何时触发」。
3. 装载与失效复用 SQ1 的 epoch：内存态带 epoch 标签，`WaitWritable` 返回的 epoch 不同即丢弃重装；管理面删组/重置位点经 deliver 串行化（P2）。
4. 正常停机与计划内转移领导权之前做全量检查点（P1），计划内切换零重复。
5. SQ3 两条复现用例在 {单机, 集群} × {检查点, 逐次} 四种组合下全绿并连跑 10 次稳定。
6. 给 SQ4.9 预留只读查询（第 2 节「对外只读面」），给 SQ4.2 预留到期索引的「最早到期时刻」内部读点。

不做：inflight 二进制编码；控制台/指标改读内存（SQ4.9）；按队列唤醒（SQ4.2）；topic 删除后旧 cursor 残留（现状问题，不因本卡变化，见第 6 节残余）。

## 2. 改动边界

### 文件集（超出须升级，见第 5 节）

| 文件 | 改什么 |
|---|---|
| `internal/core/deliver/state.go`（新） | 队列内存态 `queueState`、组状态 `groupState`、到期索引、装载（从 pebble）、**批次构造函数**（I1 的唯一出处）、只读查询。文件头写职责/边界 |
| `internal/core/deliver/checkpoint.go`（新） | 检查点循环、`Start/Stop`（含停机全量检查点）、计划内转移入口（围栏 + 全组检查点，P1/P3）、`ApplyAdminBatch`（P2）、失败毒化与重装闸门 |
| `internal/core/deliver/deliver.go` | 全部公开方法改走内存态（第 2 节方法表）；`lockQueue` 返回 `*queueState`；包头注释与 `Deliverer` 类型注释的「并发安全」论证改写；`New` 增加变参选项（检查点间隔），不改既有参数 |
| `internal/core/deliver/lease.go` | 只在 `renewable` 入参类型需要随内存记录调整时改；判据不变 |
| `internal/core/adminops/adminops.go` | `PurgeGroupData` 改经消费状态管理入口（P2）；枚举改为 pebble ∪ meta 拓扑；签名增加一个本包定义的小接口参数 |
| `internal/admin/groups.go` | `handleGroupDelete` 传入 deliver；`ResetCursor` 调用不变 |
| `internal/cluster/manager.go` | **仅 P1 拍板后**：`Options.BeforeLeaderTransfer` 字段与注释；`balanceOnce` 在 `m.TransferLeader` 前调用（nil 跳过、带上限 ctx、出错本周期不转移并记 Warn） |
| `internal/config/config.go` | `consume_checkpoint_interval`：字符串时长，缺省 `"1s"`，`0`/`0s` = 逐次复制，负值或非法 → `Load` 报可读错误；解析访问器（注意 `&Config{}` 字面量与 e2e `yaml.Marshal` 写出空串的情形，空串按缺省 1s） |
| `cmd/sq/main.go` | 构造 `deliver.New(..., deliver.WithCheckpointInterval(cfg...))`；`dl.Start` + 停机 defer（注册在 StopClean defer 之后、admin Shutdown defer 之前，见第 3 节步骤 9 的顺序论证）；`controlHandler` 的 ForwardApply 分支识别消费状态批次改走 `dl.ApplyAdminBatch`；P1 钩子接线；启动日志打出模式 |
| `internal/store/store.go` | 若需要：新增只读批次键遍历辅助（与现有 `BatchTouchesPrefix` 同款解析），供 `ApplyAdminBatch` 找出触及的队列 |
| `sq.example.yaml`、`docs/configuration.md` | 新配置键与「重复投递换吞吐」的取舍说明 |
| 测试 | 见第 4 节：`internal/core/deliver/sq3_cluster_race_test.go`（并入）、`state_test.go`/`checkpoint_test.go`（新）、`bench_test.go`、`internal/config/config_test.go`、`internal/core/adminops/*_test.go`、`test/e2e/consume_failover_test.go`（新）、`test/e2e/sdk_cluster_bench_test.go`（新增消费吞吐基准）、因控制台/指标读检查点值而需改为轮询的既有用例 |

明令不碰：`replication.Replicator`/`Router`/`Forwarder` 接口与实现、`cluster` 内除 P1 以外的任何代码、提案头与日志格式、`cursor/`/`inflight/` 键布局与 inflight JSON 编码、`internal/metrics`、`internal/admin` 的读路径（`groups.go:54,69`、`timeseries.go`）、rpc 层错误映射。

### 真实入口

- 取件：gRPC `ReceiveMessage` → `internal/rpc/receive.go:197` → `Deliverer.Receive` → `receiveOnce`。
- 确认：`AckMessage` → `receive.go:502` → `AckBatch`。改不可见：`receive.go:585` → `ChangeInvisible`。转死信：`internal/rpc/forward.go:29` → `ForwardToDLQ`。
- 管理面：`internal/admin/groups.go:127` → `ResetCursor`；`groups.go:145` → `adminops.PurgeGroupData`。
- 跨节点：`cmd/sq/main.go` `controlHandler` → `handleForwardApply`（:682）。
- 停机：`cmd/sq/main.go` SIGTERM 分支 `srv.Shutdown` → `gracefulStop` → defer 链（LIFO）。
- 计划内转移：`internal/cluster/manager.go:1041` `balanceOnce` → `TransferLeader`（生产唯一调用点）。

### 内存态（第 1 题结论）

- **锁：沿用每队列一把锁**，把 `qmu map[string]*sync.Mutex` 的值换成 `*queueState`（内嵌互斥量 + 状态），键与 `lockQueue` 的拼接保持。不改成更粗或更细的锁：同队列的全部读改写本来就要互斥，现锁粒度正合适；不同队列并行不变。
- `queueState` 字段（命名执行者定）：`loaded`、`epoch`、`cursor`、`inflight map[offset]*条目`（条目 = `core.InflightState` 值 + 到期索引位置）、**到期索引**（按 (ExpireAtMs, offset) 排序、支持 O(log n) 插入/删除/改键的结构，推荐带位置回填的最小堆）、**在外顺序计数** `orderedN`（取代现 `orderedBusy` 扫描判据，`orderedN > 0` 即顺序锁占用；用计数而非布尔，装载时若遇到历史遗留的多条顺序 inflight 也不会误放）、**脏集** `dirty map[offset]struct{}` 与 `cursorDirty`、供只读查询的原子镜像（inflight 条数、cursor）。
- 续租字段：沿用 `core.InflightState` 的 `Owner`、`RenewUntilMs`，判据仍是 `lease.go` 的 `renewable`；续租 = 改 `ExpireAtMs` + 在索引里改键 + 记脏。
- `groupState`（按 raft 组号 g，即 `rt.GroupForQueue(topic, q)`）：当前 epoch、在途提案计数、毒化标记、围栏截止时刻。单机档所有队列都映射到同一个 g（`StandaloneRouter.GroupForQueue` 恒为 MetaGroup）。
- 锁序：`d.mu`（只护 map）→ 围栏/毒化等待（不持任何锁）→ 队列锁 → produce 内部锁（DLQ Append）。多队列同时加锁（`ApplyAdminBatch`、全组检查点）按队列键字典序逐个取，或逐个「取锁-构造-入队-放锁」，不同时持两把。

### 批次构造（I1 的唯一出处）

一个函数：输入一个已加锁的 `queueState`，输出一个批次并清空脏集。批次内容 = `Set(cursor)` + 对脏集中每个 offset：内存里有 → `Set(编码后的 inflight)`，没有 → `Delete(inflight 键)`。批次非空时**恒写 cursor**。由此：盘上状态 = 上一次成功落盘的状态 + 本批差量 = 构造时刻的内存态，cursor 推进与其覆盖区间的 inflight 写入必在同一批。全量变体（重置位点、失败后的兜底）= `Set(cursor)` + `DeleteRange(该队列 inflight 前缀)` + 全部 inflight `Set`。

**前缀不变式 I2（承重，本计划新增）**：同一 raft 组内，deliver 发出的全部消费状态提案按构造顺序 FIFO 入队（SQ2），且一律以 **Deliverer 生命周期 ctx** 提案（不传调用方 ctx——调用方取消会让「N 未进日志、N+1 进了日志」在同一任期内成立，差量批次随即错位、cursor 越过缺失的 inflight = 丢消息）；任一消费状态提案失败 ⇒ 该组毒化：丢弃该组全部队列内存态，在该组本 Deliverer 的在途提案全部了结之前不得重装。由 SQ2 的失败契约（同任期内失败条目之后的同组条目要么同样失败、要么失败条目从未进日志）+ 生命周期 ctx，推出「盘上状态恒等于某个提案前缀之后的状态」，前缀上每批都满足 I1，故 I1 在盘上恒成立。

### 每个公开方法的新行为（第 2 题结论）

公共前置（下表「前置」）：取 g → 围栏等待（P3）→ `rt.WaitWritable(ctx, g)` 得 epoch e → 取队列锁 → 若 `queueState` 未装载或 `epoch < e` 则从 pebble 装载并打标 e；若 `epoch > e` 返回 `ErrNotLeader`（本次调用持有的 epoch 已过时）；若组已毒化且在途未了结，放锁等待后重试（受 ctx 约束）。

| 方法 | 锁内读写内存 | 逐次复制模式 | 检查点模式 |
|---|---|---|---|
| `Receive` / `receiveOnce` | 读屏障照旧挂入口；每轮 `receiveOnce` 走前置。阶段 1：从到期索引按到期序取 `ExpireAtMs ≤ now` 的条目——可续租的续租（不占 maxMsgs），其余至多 maxMsgs 条重投（读 `msg/` 判孤儿、判超限转死信、退避、保留 Ordered）；阶段 2：从内存 cursor 扫 `msg/`，过滤、顺序锁看 `orderedN`，投出即写内存 inflight 并推进 cursor。全部改动记脏 | 锁内构造批次 → `ApplyAsync`（生命周期 ctx）→ 放锁 → `Wait` 成功才交件（现语义） | 锁内改完即放锁交件；不提案 |
| `Ack` / `AckBatch` | 逐条查内存 inflight、校 attempt、删除（同步删索引、顺序计数减一）、记脏 | 同上：构造 → `ApplyAsync` → 放锁 → `Wait` 后报成功；全部落空不提案 | 改完即返回成功 |
| `ChangeInvisible` | 查、校 attempt、改 `ExpireAtMs` 与索引键、记脏 | 构造 → `ApplyAsync` → 放锁 → `Wait` | 改完即返回 |
| `ForwardToDLQ` | 查、校 attempt；读 `msg/`：不存在 → 删内存 inflight（孤儿）；存在 → 死信写入（第 5 题）后删内存 inflight，记脏 | 删除随本次批次 `ApplyAsync`，放锁后 `Wait` | 删除只进内存 |
| `moveToDLQ`（内部） | 第一段死信写入保持同步（本地 `pr.Append` 或 `fwd.ForwardAppend`），成功后第二段只删内存 inflight 并记脏，不再单独 `rep.Apply` | 第二段随调用方（receiveOnce / ForwardToDLQ）的批次一起提案 | 第二段只进内存，随检查点落盘 |
| `ResetCursor` | 前置后：内存置新 cursor、清空 inflight/索引/顺序计数/脏集 | 两模式相同：锁内构造**全量变体**批次，`Apply` 同步等待（生命周期 ctx）后放锁（管理操作保持现同步语义，S6） | 同左 |
| `ApplyAdminBatch`（新） | 找出批次触及的 (组, topic, 队列) → 逐个按序取锁 → 丢弃内存态（置未装载）→ 同步 `Apply` → 放锁；下次访问从 pebble 重装 | 两模式相同 | 同左 |
| `FilterSkipped` | 不变 | — | — |

### 检查点（第 3 题结论）

- **触发者**：每个 Deliverer 一个后台循环（`Start(ctx)` 起、`Stop(ctx)` 停），每个间隔一轮，只处理脏队列（队列首次变脏时登记进一个脏队列集合，叶子锁）。
- **一轮的构造**：按 g 分组；每组先 `WaitWritable`（ctx 上限取一个间隔）得 e，不可写或 epoch 已变 → 该组脏队列直接丢弃内存态（变更随领导权丢失，属故障后果）不写；可写 → 逐队列「取锁 → 校 `epoch == e` 且已装载 → 构造批次 → `ApplyAsync`」→ 放锁，同组多个队列可合并为一个批次（一个队列绝不拆到两个批次），批次大小上限执行者定。
- **轮与轮串行**：本轮全部 `Wait` 返回后才开始下一轮。差量批次的正确性依赖「上一批已落盘」，并发两轮会把差量叠在未知基线上。
- **失败**：任一 `Wait` 失败 → 该组毒化（I2），不重试差量；重装后基线回到盘上状态。
- **切主**：失去领导权后本节点该组的检查点提案在 SQ2 出队时因任期不符失败（或早在 `WaitWritable` 就拒），走毒化；新 leader 在 `WaitWritable` 开门后（applied ≥ 本任期首条，旧任期条目已全部 apply 或被截断）首次访问时装载。
- **正常停机**：`Stop` = 置关闭标记（此后新调用报 `ErrNotLeader`）→ 停循环 → 用一个新的有上限 ctx（建议 10s）对全部脏队列做一轮全量检查点并 `Wait`。main 中它的 defer 必须在 gRPC 排空之后、`Manager.StopClean` 与 `st.Close` 之前执行：在 `deliver.New` 之后注册即满足（StopClean 的 defer 注册得更早，LIFO 后执行；gRPC 排空在 `select` 分支内、早于全部 defer）；admin 的 Shutdown defer 注册更晚、先执行，保证不会在最终检查点之后再来一次重置位点。单机档同一套。
- **计划内转移**（P1）：deliver 提供 `PrepareTransfer(ctx, g) error`：置围栏（截止 = now + TTL）→ 对 g 的全部已装载队列逐个「取锁 → 构造 → `ApplyAsync`」→ 全部 `Wait`；失败即撤围栏并返回错误（摊布本周期不转移）。围栏在锁内复查：围栏置起后、该队列被构造前拿到锁的调用，其改动被本次构造带走；构造之后拿到锁的调用看到围栏，放锁等待。领导权确实转走后 epoch 变化，本节点内存态失效；转移中止则 TTL 到期自动恢复（记 Warn）。

### 装载（第 4 题结论）

- 时机：前置里懒装载——`WaitWritable` 返回 e 后首次访问某队列（或 epoch 变大、或毒化了结后）。不做当选时的全量预装：raft 组下可能有成千上万个 (组,topic,队列)，懒装载把成本摊到真实访问上，且天然只发生在 SQ1 开门之后。
- 读什么：`CursorKey(group, topic, q)`（无 → 0，与现状一致）+ `InflightPrefix(group, topic, q)` 全扫解码，建 map、索引、顺序计数。O(inflight) 一次，在队列锁内。
- 与 SQ1 衔接：不新增 leader 变更钩子；`WaitWritable` 是唯一的「可写 + epoch」来源，比较规则与 SQ1 的 produce 计数器缓存相同。
- 删组失效：P2。

### 第 5 题（moveToDLQ）

保留第一段同步死信写入在队列锁内（与现状同，死信是低频路径，锁内一个复制往返可接受，且「先写死信、后删源」的次序必须保持——反过来即丢消息）；第二段从「锁内第二次同步提交」降为内存删除 + 记脏，逐次模式随本次批次提案。锁内同步提交从两次降为一次。被否：把死信写入挪到锁外（先给记录打「转死信中」标、放锁写死信、再取锁删除）——多一个中间态要进内存模型、装载与检查点，收益只是低频路径少持一次锁。

### 第 6 题（单机档）

同一套代码：`Standalone` 的 `ApplyAsync` 即 pebble 提交管线（调用顺序 = 可见顺序，I2 的 FIFO 天然成立），`StandaloneRouter.WaitWritable` 恒 `(0, nil)`，全部队列同属一个 g。fsync 档位：`fsync: sync` 下检查点批次的 `Wait` 等 fsync，逐次模式与现语义逐字节相同；`fsync: async` 下检查点批次不等 fsync，崩溃窗口 = 检查点间隔 + 未刷 WAL，后果仍只是重复。停机全量检查点之后由 `st.Close` 收尾。

### 对外只读面（第 9 题结论）

- 给 SQ4.9：`(d *Deliverer) InflightCount(group, topic string, q uint32) (n int, ok bool)`——读队列状态的原子镜像，不取队列锁；`ok=false` 表示本节点未装载该队列（非 leader 或尚未访问），调用方据此回落读 pebble。本卡的不变式单测是它的第一个消费者（核对内存计数与 map 一致）。不提前做按组/按 topic 聚合的遍历接口——SQ4.9 有了真实形状再加。
- 给 SQ4.2：不导出任何东西；`queueState` 的到期索引堆顶即「最早到期时刻」，SQ4.2 的按队列唤醒在 `queueState` 上加等待通道即可，本卡只保证这两样都在队列状态里、受同一把锁保护。

## 3. 工作顺序

每步「做 → 检查」，检查不过不进下一步。

**段 A（逐次复制模式，根治 SQ3）**

0. **准备与基线**（SQ1、SQ2 合入前即可做）：
   - 把 SQ3 复现用例从 `agent-af9ad1270a441e9d9` worktree 的**未跟踪文件**拷入本分支，单独提交为红测（它不在任何分支上，见「偏离」）；在 main 基线上跑一次存证（预期集群子测试红）。
   - 在 `test/e2e/sdk_cluster_bench_test.go` 加消费吞吐基准 `TestClusterConsumeThroughput`（SQ_BENCH=1 才跑；预写 N 条 → C 个 SimpleConsumer 收+ack 排空，报 msg/s；三节点 quorum-mem + 单机基线），**这一提交不引用任何新配置项**，以便原样在 main 上跑基线。在 main 与本分支起点各跑一次存数。
   - 跑现有 `BenchmarkAckParallel` 与新增的「预置在外 inflight 1k/10k」变体（变体只依赖现有 API，可在 main 上跑）存基线。
1. **rebase 到 SQ2、SQ1 之上**，核对三条承重假设（任一不成立即停下回报主会话，阻塞后续全部步骤）：
   - **H1（SQ2 失败前缀性）**：读落地后的 SQ2 出队/作废代码与其 `proposer_test.go` 第 2、3 条：同组同任期内，一条以「非调用方取消」原因失败的提案之后的同组提案全部失败、不进日志。这是 I2 的地基。
   - **H2（SQ1 门的形态）**：读 `WaitWritable` 实现：已可写时是否只是原子读（本卡每次取件/确认都调）；本节点是 follower 时是立即 `ErrNotLeader` 还是等满上限（后者会让打到 follower 的 ack 慢 3s——若如此，回报主会话，不自行加快速路径）。写一个 deliver 包内微基准记录其开销。
   - **H3（摊布循环可阻塞）**：确认 `balanceOnce` 只在 `leaderBalancer` goroutine 内执行（出稿时已读 manager.go:964-993），不在 Ready 循环。
   - SQ3 用例随 SQ1 适配：其集群装配里的 `OnLeaderChange → InvalidateCounters` 删除（SQ1 已删该方法），其余判据一字不改。
2. **`state.go`：内存态、装载、到期索引、批次构造**（纯数据结构，不接方法）。检查：不变式单测 U1、U2、U3（第 4 节）绿；对 U1 做变异（构造时漏掉脏集里的 Delete）必须红。
3. **把全部公开方法改走内存态，逐次复制模式**（`New` 不传选项即逐次）。检查：`go build ./... && go vet ./internal/core/...`；deliver 包既有测试全绿；SQ3 四个子测试（单机/集群 × 两个用例）各跑 1 次转绿——**首条故事级检查，绿后立即跑真机首跑项**（第 4 节）。
4. **毒化与 epoch 处理**（I2）。检查：U4、U6 绿；变异：把提案 ctx 换回调用方 ctx，U4 必须红。
5. **管理面**：`ResetCursor` 改内存 + 全量变体；`ApplyAdminBatch`；`PurgeGroupData` 改经入口 + 枚举并集；main 的 ForwardApply 分流。检查：S6 的包级检查绿；`TestAdminAPISmoke` 不回退。
6. SQ3 连跑 10 次（逐次模式）、`-race` 跑 deliver 包一次。段 A 至此可合入集成分支（P6）。

**段 B（检查点模式）**

7. **配置键**与 `WithCheckpointInterval`。检查：S5 的 config 测试绿。
8. **检查点循环 + `Start/Stop`**。检查：U5（停机全量检查点）绿；SQ3 在检查点模式下四个子测试转绿（测试装配里 `Start`、Cleanup 里 `Stop`）。
9. **main 接线**：选项、`Start`、停机 defer 位置（第 2 节论证）、启动日志打出模式。检查：`go build ./cmd/...`；单机进程 SIGTERM 后用 `sq` 自带读路径或直接打开数据目录核对 cursor 等于停机前内存值（可写成 cmd 包测试或 e2e `TestOfficialGoSDKRestartRecovery` 在检查点模式下零重复的附加断言，执行者择一）。
10. **P1 钩子**（拍板后）：cluster 字段与 `balanceOnce` 调用点、deliver `PrepareTransfer` 与围栏、main 接线。检查：U7（围栏）绿；`make test-cluster` 不回退。
11. **故障注入 e2e**（S2/S3）与 S4 基准、集群消费吞吐对比。
12. 文档：`sq.example.yaml`、`docs/configuration.md`；包头与类型注释收口。全量回归（第 4 节）。

## 4. 验收方式

### 「重复」的口径（S2/S3 共用，测试照此计数）

- **丢失**：确认集（Send 成功）中从未被消费到的 msgID。任何模式、任何场景必须为 0。
- **检查点丢失型重复** = 以下两类之和：(i) 某 msgID 的一次投递其 Ack 已返回成功之后，又被投递；(ii) 某 msgID 以 DeliveryAttempt=1 被投递超过一次。消费者处理超时、ack 失败后的正常重投（attempt 递增、此前无成功 ack）**不计入**——那是 at-least-once 的既有语义，与检查点无关。
- 消费者用 SimpleConsumer，不可见时长 60s（排除超时重投干扰），ack 失败（含 HA_NOT_AVAILABLE）重试至成功或 30s 上限；SDK 取 attempt 用 `MessageView.GetDeliveryAttempt()`（rocketmq-clients golang v5.1.4 `message.go:386`）。

### 故事级检查

| 故事 | 命令 | 判据 |
|---|---|---|
| **S1** | `go test ./internal/core/deliver/ -run 'TestSQ3' -count=10 -timeout 60m -v 2>&1 \| tee /tmp/sq4.1-s1.log` | SQ3 用例改为 {standalone, cluster} × {checkpoint, perop} 四子测试（模式只改装配，判据一字不改）：用例 A「重复offset数=0」、用例 B「红轮数=0」，10/10 PASS。变异：把 `receiveOnce` 改回读 pebble 的 cursor，集群子测试必须红 |
| **S2/S3** | `cd test/e2e && go test -tags e2e -count=1 -run 'TestConsumeFailover' -v -timeout 90m 2>&1 \| tee /tmp/sq4.1-e2e.log` | 新文件 `test/e2e/consume_failover_test.go`，复用 `startProcCluster`（mutate 设检查点间隔）、`confirmedSet`、`produceUntil`；全程持续生产 + 多消费者收+ack；两种模式各跑三个场景：**(a) kill9**：kill 承载队列最多的节点 → 35s → 重启 → 排空；**(c) graceful**：SIGTERM 同一类节点 → 10s → 重启；**(b) transfer**：(c) 重启后等摊布把 preferred 组转回（≤ 60s），以存活节点 `broker.log` 出现「摊布：组领导权转移」且时间晚于重启为「转移已发生」的证据，未出现即 FAIL（防空过）。判据：全部场景丢失 = 0；(b)(c) 检查点丢失型重复 = 0（两种模式）；(a) 逐次模式 = 0，检查点模式 ≤ `2 × R × (I + 1s)`（P5），日志打印 R、I、实测值 |
| **S4** | `go test ./internal/core/deliver/ -run '^$' -bench 'BenchmarkAckParallel\|BenchmarkReceiveStandingInflight' -benchtime 2s -count 3 2>&1 \| tee /tmp/sq4.1-bench.log`；`cd test/e2e && SQ_BENCH=1 go test -tags e2e -count=1 -run 'TestClusterWriteThroughput\|TestClusterConsumeThroughput' -v -timeout 60m` | 预置在外 inflight 1k/10k 两档，两种模式；判据：同模式下 10k 档 ns/op ≤ 1k 档的 1.5 倍（「不随 inflight 增长」的可检查形态），并与步骤 0 的 main 基线并列报告；ack 路径剖析（`-cpuprofile`）中无 `store.(*Store).Get` 调用。e2e 吞吐只报数字进卡（spec 不设硬阈值） |
| **S5** | `go test ./internal/config/ -run 'ConsumeCheckpoint' -count=1 -v` | 缺省 1s；`0`、`0s` → 0；`-1s`、`abc` → `Load` 返回错误且错误信息含键名与示例；空串按 1s |
| **S6** | `go test ./internal/core/deliver/ ./internal/core/adminops/ ./internal/admin/ -run 'Reset\|Purge\|DeleteGroup' -count=1 -v`；e2e `TestAdminAPISmoke` | 检查点模式下：先取件使内存 cursor 领先盘上（未检查点）→ `ResetCursor` → 下一次取件从新位点投；再走一轮检查点后盘上 cursor = 重置值、inflight 为空（未被旧内存覆盖）。删组：内存独有队列（从未检查点）也被清；删后跑一轮检查点，盘上该组 `cursor/`、`inflight/` 无键；下一次取件从 0 开始。另一条经 main `handleForwardApply` 分流的测试（cmd 包，构造触及 `cursor/` 的批次，断言走了 deliver 入口，执行者定手段） |

### 不变式单测（只为承重规则）

- **U1（I1）**：随机操作序列（投递/确认/改不可见/续租/重投/转死信孤儿/重置）驱动一个 `queueState`，在随机时刻构造批次、应用到一份「上一次检查点」的 pebble 上，断言：盘上解码 = 内存（cursor + inflight 全字段）；每个使 cursor 推进的批次都含被越过区间里内存仍在外的全部 inflight `Set`。变异：漏 `Delete` / 漏 cursor → 红。
- **U2（往返）**：装载 → 变更 → 构造批次并应用 → 重新装载，两份内存态相等（含索引顺序与顺序计数）。
- **U3（到期顺序与成本）**：不同 `ExpireAtMs` 的 inflight，重投按到期序；改不可见后顺序随之变化；续租不占 maxMsgs；10k 条未到期 inflight 时一次取件对索引的访问次数为常数级（计数钩子或基准断言，执行者定）。
- **U4（I2 毒化）**：假 Replicator（按调用顺序记录、可让指定批次 `Wait` 失败且不应用）：失败之后该组内存态被丢弃，在途了结前访问阻塞，重装后的内存 = 盘上；盘上恒满足 U1 的断言。变异：提案用调用方 ctx 并在测试里取消 → 断言红。
- **U5（停机）**：检查点模式下若干变更后 `Stop`，盘上 = 停机前内存；`Stop` 后调用返回 `ErrNotLeader`。
- **U6（epoch）**：假 Router 切换 epoch：旧内存态被丢弃重装；持旧 epoch 的调用拿到 `ErrNotLeader`；检查点循环不写 epoch 不符的队列。
- **U7（围栏，P1）**：`PrepareTransfer` 期间到达的确认阻塞；围栏前已改的状态全部在其检查点批次里；TTL 到期恢复服务。

### 编译与包测试

- `go build ./... && go vet ./...`
- `set -o pipefail; go test -timeout 15m -count=1 ./internal/... ./cmd/... 2>&1 | tee /tmp/sq4.1-pkgs.log`
- `go test -race -count=1 ./internal/core/deliver/ ./internal/core/adminops/`
- `make test-cluster`

### 真机清单（本机进程级三节点 / 单机进程，`test/e2e`）

1. **首跑项**（步骤 3 首次转绿即跑，逐次模式）：`cd test/e2e && go test -tags e2e -count=1 -run 'TestThreeNodeClusterE2E|TestScenarioKillLeaderHard|TestClusterDLQ|TestScenarioPoisonMessageToDLQ|TestOfficialGoSDKFIFOBlockedUntilAck' -v -timeout 40m 2>&1 | tee /tmp/sq4.1-e2e-first.log`。判据：全 PASS；存 PASS 行与对账行。
2. 段 B 完成后，默认配置（检查点 1s）跑 `make e2e` 全量，全 PASS。预期需要改为轮询的只有「消费后立刻读控制台/指标」的用例（已知 `test/e2e/console_test.go:233` `TestLedgerReflectsConsumption` 断言落后量 = 30），改法限于「≤ 3s 内轮询到期望值」，判据数值不动。
3. S2/S3 命令（上表），两种模式。
4. S4 两条 e2e 吞吐（上表），与步骤 0 基线并列。

须存证据：每条命令末尾 PASS/FAIL 行、S2/S3 的计数行（确认集、消费集、两类重复、R、I）、基准表。

## 5. 决策权限

执行者可自定（交回物「偏离与自主决定」逐条对照本节）：

- 内存数据结构细节（索引实现、map 选型、`qmu` 是否换 `sync.Map`）、文件内函数划分与命名（含 `InflightCount`、`PrepareTransfer`、`ApplyAdminBatch`、`WithCheckpointInterval` 的命名）。
- 检查点批次是按队列一个还是同组合并（一个队列不得跨批）、批次大小上限、脏队列集合的实现。
- 毒化后「等在途了结」的机制；围栏 TTL 在 [2s, 5s]、P1 钩子 ctx 上限在 [1s, 3s]、停机检查点上限在 [5s, 20s] 内取值。
- `New` 不传选项时的缺省模式（推荐逐次，保住既有测试的落盘断言；main 永远显式传配置值）。
- e2e 场景的辅助函数组织、等待时长（在不放宽判据的前提下）、`handleForwardApply` 分流测试的手段。
- 既有测试中断言「两段式 moveToDLQ 的第二次提交」这类**机制**的用例（如 `deliver_test.go:858` 的 `afterAppendHook`）改写为断言语义（死信先于源删除、崩溃后至多重复死信），须在交回物逐条列出。

必须停下升级：

- 第 3 节步骤 1 的 H1/H2/H3 任一不成立。
- 需要改 `replication.Replicator`/`Router`/`Forwarder` 任何签名、新增控制 op 或改 wire、改盘上键布局或 inflight 编码、改提案头。
- P1 未获批准却需要动 `internal/cluster`；或 P1 获批后需要动 `balanceOnce` 以外的 cluster 代码。
- 改 rpc 层错误映射或 SDK 可见的重试设置。
- 任何故事判据需要放宽（S1 判据、S2 丢失 = 0、(b)(c) 零重复、S3 上界），或既有测试需要改断言（第 4 节真机清单第 2 条的「轮询化」除外）。
- 发现除 `adminops.PurgeGroupData` 外还有绕过 deliver 写 `cursor/`/`inflight/` 的生产代码。
- S4 中检查点模式的集群消费吞吐低于逐次模式，或逐次模式低于 main 基线的 90%。

---

## 6. 风险与残余

- **最大风险：I2 依赖 SQ2 尚未落地的失败契约**。若 SQ2 实现后「同任期失败前缀性」只在部分失败原因上成立，差量批次可能叠在缺口上，造成 cursor 越过缺失的 inflight（真丢消息，且只在切主后显现）。对策：步骤 1 H1 读码 + U4 用假 Replicator 钉住本卡侧行为；集群真实路径由 SQ2 的 proposer_test 守。
- **SQ2 作废任期与摊布转移的交互**：raft 在转移期丢提案（`ErrProposalDropped`），SQ2 会把该任期标记作废；若转移随后中止、本节点仍是 leader，该组在本任期内全部提案失败直到下次选举。这是 SQ2 的语义，produce 同样受影响；本卡的围栏让转移期内消费侧不提案，减轻而非消除。执行者在步骤 10 观测到此现象时记录并回报，不在本卡修。
- **检查点与 TOCTOU 残余**：`WaitWritable` 拿到 e 与 SQ2 入队绑任期之间存在微秒级窗口，若期间失去并重新赢得领导权（≥ 一个选举超时，实际不可达），旧 epoch 内存态构造的批次可能进新任期日志。论证其后果只是重复（被写回的只会是更旧的 cursor 与已 ack 的删除），不丢消息；与 SQ1 produce 侧同一残余，记录不修。
- **e2e 零重复判据的脆性**：(b)(c) 依赖「最终检查点在 gRPC 排空后、StopClean 前」与围栏时序；失败时先看 `broker.log` 中最终检查点与围栏日志，再判是实现问题还是口径问题，口径问题回报主会话，不自行放宽。
- **大批次**：单队列积压 10k 条在外时一个批次约 800KB（inflight JSON 约 80B/条），低于但接近 raft `MaxSizePerMsg`（1MB，`group.go:374`）；raft 允许单条超限条目，风险是复制延迟尖刺。记录 S4 中最大批次字节数。
- 残余（不归本卡，入卡备注）：topic 删除不清 `cursor/`，重建同名 topic 后旧 cursor 残留（`adminops.PurgeTopicData` 现状如此，内存态不使之变好或变坏）。

## 7. 工作量估计

生产代码约 900–1200 行（deliver 重写约占六成），测试约 1000 行（含 e2e 两个新文件）。段 A 约 2 个实现会话，段 B 约 2 个实现会话，e2e 稳定性调试另留 1 个。

---

## 读数与证据

- `git -C .../worktrees/sq4 log --oneline -3` → `a8639eb docs(spec): SQ4 spec 已批准` / `f4b5f6e` / `21ee680`
- `git -C /Users/df-zidong/workspace/sq worktree list` → `agent-af9ad1270a441e9d9 21ee680 [worktree-agent-af9ad1270a441e9d9]`；`git -C .../agent-af9ad1270a441e9d9 status --short` → `?? internal/core/deliver/sq3_cluster_race_test.go`（449 行，未提交）
- SQ3 用例集群装配：`sq3_cluster_race_test.go:126-133` `OnLeaderChange` 调 `prv.InvalidateCounters()`（SQ1 删该方法，rebase 后须删此钩子）；`:184`/`:346` 两个用例各跑 standalone/cluster 子测试
- `internal/core/deliver/deliver.go:168-180` `lockQueue`；`:243-253` 放锁后 `pending.Wait()`（SQ3 根因）；`:291-336` 全量扫 inflight + JSON 解码；`:456-461` 读 `CursorKey`；`:631-645` ack 锁内 `st.Get` + 解码；`:787`、`:889`、`:917` 三处同步 `rep.Apply`（孤儿清理、moveToDLQ 第二段、ResetCursor）；`:850-874` 死信本地/转发分派
- `internal/core/adminops/adminops.go:106-148` `PurgeGroupData`：枚举只扫 pebble 的 cursor/inflight 两族，逐队列 `ApplyOrForward` DeleteRange
- `internal/admin/groups.go:127` `s.dl.ResetCursor`；`:145` `adminops.PurgeGroupData`
- `cmd/sq/main.go:243-255` `onLeaderChange` → `InvalidateCounters`；`:288-306` `cluster.Options` 装配；`:358-369` StopClean defer；`:384` `deliver.New`；`:449-475` 后台循环 defer 模式；`:560-567` SIGTERM → `srv.Shutdown` → `gracefulStop`；`:682-700` `handleForwardApply`（g + repr → `cl.Apply`）
- `internal/cluster/manager.go:1041` `balanceOnce` 内 `m.TransferLeader(g, preferred)`；`:964-993` `leaderBalancer` 独立 goroutine；`:1410` `leaderStableTicks = 3`；`cmd/sq/main.go:607` `leaderBalanceInterval = 5s`；`:1490-1530` `StopClean` 不做领导权转移
- `rg "TransferLeader" internal cmd test`（非测试文件）→ 仅 `manager.go:1041`（调用）与 `:1789`（定义）；`internal/admin`、`internal/rpc` 无转移接口
- `internal/replication/replication.go:170-195` `Pending`/`Replicator`/`Router` 接口；`:246-293` `Standalone`/`StandaloneRouter`（`GroupForQueue` 恒 MetaGroup）
- `internal/rpc/receive.go:116` 取件入口 `SelfIsLeader` 快速失败；`:222-226`、`:507-517`、`:588-592` `ErrNotLeader` → `HA_NOT_AVAILABLE`
- `internal/metrics/stats.go:63-91` 指标扫 pebble 的 cursor/inflight（检查点值，SQ4.9 前不变）
- `internal/config/config.go:179-195` `Load` 缺省值；`:222-225` `KnownFields(true)` 严格解析；`:48` `fsync: sync|async`
- `test/e2e/sdk_cluster_bench_test.go:14` 「不测消费侧吞吐」、`:250` `TestClusterWriteThroughput` 只测写 → 需新增消费吞吐基准
- `test/e2e/cluster_proc_test.go:58` `startProcCluster(t, n, mutate...)`；`:96-121` 节点日志 `broker.log`；`:160/176/213` kill/stopGraceful/restart
- `test/e2e/console_test.go:233-262` `TestLedgerReflectsConsumption` 消费后立即断言落后量 = 30
- `internal/cluster/group.go:374` `MaxSizePerMsg: 1 << 20`
- `~/go/pkg/mod/github.com/apache/rocketmq-clients/golang/v5@v5.1.4/message.go:386` `func (msg *MessageView) GetDeliveryAttempt() int32`
- SQ1 计划（`.../worktrees/sq1/docs/superpowers/plans/sq1-plan.md`）F1 `WaitWritable(ctx, g) (epoch uint64, err error)`、F2 阻塞上限 min(ctx,3s)、F3 epoch 标签缓存、F4 deliver 挂门交 SQ3；SQ2 计划（`.../worktrees/sq2/docs/superpowers/plans/sq2-plan.md`）D2 入队绑任期、§4.2 失败语义论证、§6「SQ3 用例预期仍红」
