# 2026-11 性能审计落地：批量 spec（SQ4.2–SQ4.11）

> 状态：**已批准（2026-11-11，用户）**
> 父卡：SQ4（epic）。SQ4.1 另有独立 spec：`2026-11-11-sq4.1-consume-state-in-memory-design.md`
> 来源：2026-11-11 四路只读审计 + 本机基准剖析（基线 main@21ee680）。本文件每节是一张卡的 spec，各卡分别挂附件；L1 卡的节末三行即 plan。

## 级别与施工顺序总表

| 卡 | 级别 | 子系统 | 前置（硬依赖 / 施工排序） |
|---|---|---|---|
| SQ4.2 按队列唤醒 | L2 | deliver + produce | 施工排序：SQ4.1 之后（同文件） |
| SQ4.3 合并提案 | L2 | replication + cluster | 硬依赖：SQ2（保序队列） |
| SQ4.4 刷盘移出锁 | L2 | cluster/seglog | 无 |
| SQ4.5 pebble 调参 | L2 | store + config | 无 |
| SQ4.6 消息 ID | L1 | core | 无 |
| SQ4.7 默认二进制编码 | L1 | config | 无 |
| SQ4.8 保留期清理 | L2 | retention | 无 |
| SQ4.9 指标不全扫 | L2 | metrics + deliver | 硬依赖：SQ4.1（inflight 计数取自内存态） |
| SQ4.10 日志截断保留 | L2 | cluster | 施工排序：SQ1、SQ2 之后（同文件） |
| SQ4.11 单机周期刷盘 | L1 | cmd 装配 + store | 无 |

全部卡的共同红线：**不改盘上与线上格式**（raft 日志条目、pebble 键值、gRPC 协议），滚动升级新旧节点互通；不降低任何现有持久化档位的安全承诺（SQ4.1 的检查点取舍已由用户单独批准）。

---

## SQ4.2 按队列唤醒长轮询，ack / 改不可见 / 转死信后也唤醒（L2）

**问题**（现状读数）：`produce.go:479-497` 的唤醒表按 topic 索引，任一条 Append 关闭该 topic 唯一的 channel，唤醒该 topic **所有队列 × 所有消费组 × 所有消费者**的长轮询，各自跑一轮取件（多数空转）。Ack、改不可见、转死信不唤醒任何人，顺序消息队头被 ack 后，后继只能等 `deliver.go:224` 的 100ms 兜底轮询——单队列顺序消费上限约每秒 10–20 条，与业务处理快慢无关。

**方案**：唤醒按 (topic, queue) 索引，Append 只唤醒对应队列的等待者；Ack / 改不可见 / 转死信 / 重置位点成功后唤醒同队列的等待者。100ms 兜底轮询保留（inflight 到期仍是时间驱动；SQ4.1 有到期索引后可改为按最近到期时间定时，属实现选择）。

**必交故事**
- S1：顺序消息单队列、消费者收到即 ack，持续消费吞吐 ≥ 200 条/秒（现状 ≤ 20）。验证：deliver 包故事级测试，单机与集群档各一。
- S2：一个 topic 16 队列、只往队列 0 写，其余 15 个队列的长轮询等待者不被唤醒。验证：包级测试，以「被唤醒的取件轮数」为观测量。
- S3：长轮询语义不回退——现有 deliver / rpc / e2e 长轮询用例全绿。

**Out of Scope**：消费者跨队列合并等待（永不做，协议按队列取件）。

---

## SQ4.3 集群档合并提案（L2）

**问题**：一次提案 = 一个 raft entry = 给每个 follower 一条 MsgApp、一个 waiter、一次 apply 簿记（`group.go:1619`）。默认 `quorum-mem` 档是 CPU 瓶颈，本机三节点 e2e 并发 64 时 14,696 msg/s。

**方案**：在 SQ2 的每组保序提案队列上，proposer 每次把**已经排队**的提案一次交给 raft（只取已排队的，不为攒批等待，低负载延迟不变）。**不改 raft 日志条目格式**——实现走「一条 MsgProp 带多个 entry」一类不改格式的路径；若 plan 证明只能改条目格式，停下回 spec。各调用方的失败语义与 SQ2 定下的规则一致。

**必交故事**
- S1：集群三节点 e2e `TestClusterWriteThroughput`（quorum-mem，并发 64/256）吞吐较 SQ2 落地后的基线提升，数字进卡（审计推断 10–25%，不设硬阈值；若无提升，卡上记实测并说明原因，不硬凑）。
- S2：SQ2 的保序回归测试保持绿；滚动升级兼容——新旧节点混跑的现有 e2e 用例（plan 指定）全绿。

**Out of Scope**：改 raft 条目格式（本期不做，需两步升级时另卡）；RawNode 自驱改造（审计提及，本期不做，落 roadmap）。

---

## SQ4.4 seglog 后台刷盘移出锁外，各组并行刷（L2）

**问题**：`seglog.go:634-642` 的 `Sync` 持日志锁做 fdatasync；默认档的日志追加内联在各组 raft 主循环（`group.go:531`）且要抢同一把锁；`manager.go:1439` 每 200ms 逐组串行刷。结果每 200ms 每个组的主循环停顿一次刷盘时长（tick、收消息、发 MsgApp 全停），停顿还经共享连接的 inbox 传导到其它组。

**方案**：Sync 在锁内只取活动段句柄与当前长度，锁外刷盘；与段轮转并发时不能出错、也不能漏刷（轮转本身已完整刷旧段）。周期刷盘各组并行。刷盘的覆盖语义不变：一次刷盘覆盖调用前的全部写入。

**必交故事**
- S1：刷盘进行中，同一日志的追加不被阻塞。验证：seglog 包测试（以可观测的并发进展为判据，plan 定具体手段，不得为测试加生产入口）。
- S2：轮转与刷盘并发压测无错误、无数据丢失（重开日志逐条校验）。
- S3：三节点 e2e quorum-mem 写入的 p99 对比基线，数字进卡。

**Out of Scope**：改 200ms 周期或做成可配置（本期不做）。

---

## SQ4.5 pebble 参数可配置并调优默认值（L2）

**问题**：`store.go:93` 只设了 Logger，全用 pebble v2.1.6 默认：block cache 8MB、memtable 4MB、L0 阈值 4/12、compaction 并发 1、无布隆过滤器。剖析：ack 路径点查 65% 时间在 pread（索引块缓存未命中）。

**方案**：新增配置段（用户可见键名由本节定）：

```yaml
storage:
  cache_size: 256MiB        # block cache
  memtable_size: 64MiB
  compaction_concurrency: 0 # 0 = min(4, CPU 核数)
```

并默认开启布隆过滤器、适度提高 L0 阈值（具体值 plan 用基准定）。缺省值要能在 2GB 内存机器上安全运行；配置文档写明内存占用估算。

**必交故事**
- S1：deliver 与 produce 包基准（fsync=async 变体）对比基线：ack 路径 pread 占比显著下降，数字进卡。
- S2：配置缺省、覆盖、非法值（负数、非法单位）启动即拒。config 包测试。
- S3：三节点 e2e 写+消费不回退。

**Out of Scope**：按机器内存自动推算缓存大小（本期不做，落 roadmap）。

---

## SQ4.6 消息 ID 改用廉价随机源（L1）

**问题**：`types.go:97-101` 每条消息 `crypto/rand` 读 16 字节再 hex、再 ToUpper。本机剖析（fsync=async）占写入路径 39% CPU（macOS 上 arc4random 有全局锁；Linux 占比未测）。

**方案**：保留格式（32 位大写十六进制、128 位随机）。随机源改为由加密源播种的快速伪随机（`math/rand/v2` 运行时源一类，每 P 无锁），直接按大写表编码，一次分配。ID 只要求全局唯一、不要求不可预测（不用作凭据——plan 不需要，implement 时 rg 确认无安全用途，若有则停下回报）。

**必交故事**
- S1：格式不变：长度 32、字符集 `[0-9A-F]`；100 万个 ID 无重复；并发生成无竞争（-race）。core 包测试。
- S2：`BenchmarkAppendParallel`（async 变体）中 NewMessageID 占比下降，数字进卡。

**实现（即 plan）**
1. 改 `internal/core/types.go` 的 `NewMessageID`，新增同包测试。
2. 验收：`go test -race ./internal/core/ -run MessageID -count=1` 绿；`go test ./internal/core/produce/ -bench AppendParallel -benchmem` 记前后数字。
3. 全量 `go build ./... && go vet ./...`。

---

## SQ4.7 `message_encoding` 默认改为 binary（L1）

**问题**：默认 JSON 编码使 body 经 base64 膨胀 33%，剖析中 JSON 占写入路径 37% 的分配字节。二进制格式已实现并验收。

**事实**：读方向永远双格式（`encoding.go` 按首字节判别）；二进制解码随 v0.1.0 发布（已核：引入 `msgVersionV1` 的提交是 v0.1.0 的祖先）。故从 v0.1.0 起滚动升级、混跑都安全，已有数据零迁移。

**方案**：配置缺省值 json → binary；显式写 `message_encoding: json` 的部署行为不变。更新配置文档、示例配置、升级说明（「v0.1.0 之前的版本不能与本版本混跑」）。

**必交故事**
- S1：未配置时写入的是二进制格式；显式 json 仍写 json；旧 JSON 数据仍可读。config 与 core 包测试。

**实现（即 plan）**
1. 改 `internal/config/config.go` 缺省值，改 `internal/config/config_test.go` 的 `TestMessageEncodingDefault` 断言为 binary；更新 `sq.example.yaml`、`docs/configuration.md`。
2. 验收：`go test ./internal/config/ ./internal/core/ -count=1` 绿。
3. 全量 `go build ./... && go vet ./...`。

---

## SQ4.8 保留期清理不再全扫 key 索引（L2）

**问题**：`retention.go:218` 的 key 索引清理按 `keyidx/{topic}/{key}/{storeMs}` 前缀扫描，键按业务 key 排序而非时间，没攒满 1 万条过期就扫到前缀末尾——稳态即每 5 分钟全扫一遍整个 topic 的索引，规模随保留期内消息数线性增长。

**方案**：清理消息时（`purgeQueue` 已逐条解码被删消息）顺手按消息的 Keys 与存储时间拼出对应索引键，与消息在同一批次删除；去掉独立的 keyidx 全扫。存量的「孤儿索引」（消息已删、索引残留）由一次性的后台补扫处理，补扫限速、可中断、只在 leader 上跑。

**必交故事**
- S1：消息过期删除后，其 key 索引同批删除；按 key 查询不再返回已删消息。retention 包测试。
- S2：稳态清理不再扫描未过期的 key 索引（以扫描键数为观测量）。
- S3：存量孤儿索引最终被清掉。

**Out of Scope**：改 key 索引的键布局（盘上格式，永不在本批做）。

---

## SQ4.9 指标采集不再周期全扫（L2）

**问题**：`metrics/stats.go:63-108` 每 5 秒（每次 Prometheus 抓取再来一次）全量扫 cursor、inflight、delay、half 前缀，delay 还读出整条消息值。延时积压百万级时每 5 秒读 GB 级数据，抢 IO 和 block cache。

**方案**：inflight 计数取自 SQ4.1 的内存态；delay / half 深度由所在组 leader 在内存维护计数（暂存、搬运、撤回、提交/回滚时增减，切主时从盘重数一次）；Prometheus 抓取改读采样器最近一次结果，不再触发扫描。

**必交故事**
- S1：指标数值与全扫结果一致（包级测试：同一数据集两种算法对比）。
- S2：采样与抓取不再随 delay/half 积压量线性变慢（以扫描键数为观测量）。
- S3：现有指标名、标签不变（Prometheus 用户无感），控制台图表不回退。

---

## SQ4.10 raft 日志截断按字节 / 时间保留，死 peer 宽限期（L2）

**问题**：`manager.go:260` 截断保留 1 万条、每 30s 一次，探活失败的 peer 不再作截断下界。节点下线超过一个截断周期、组前进超 1 万条（5k msg/s 下约 2 秒），回来就只能拉全量快照（整组 FSM，可达 GB 级）。

**方案**：保留下界改为「max(按条数, 按字节, 宽限期内所有 peer 的 match)」：死 peer 在宽限期（默认 10 分钟）内仍作截断下界，超期或磁盘水位告急才放弃它。保留量大于内存窗口时，冷条目从磁盘日志段读，不全部留在内存。

用户可见配置：

```yaml
cluster:
  log_retain_bytes: 1GiB
  log_peer_grace: 10m
```

**必交故事**
- S1：三节点 e2e：持续写入下停一个节点 2 分钟再启动，它通过日志追平、不触发快照安装（以日志 / 指标判定）。真机首跑项。
- S2：宽限期过后或磁盘水位告急时照常截断（包级测试）。
- S3：内存占用不随保留量线性增长（冷条目不在内存）。

**Out of Scope**：快照拉取的流水线化（审计发现 9，本期不做，落 roadmap）。

---

## SQ4.11 单机 `fsync: async` 补周期刷盘（L1）

**问题**：单机档 `fsync: async` 下没有任何周期刷盘（`SyncWAL` 只被集群档 flusher 调用，`manager.go:1467`），掉电丢失窗口没有上界（空闲时可达内核回写周期，30 秒级）。

**方案**：单机档 `fsync: async` 时起一个 200ms 周期的 WAL 刷盘，停机时先停它再关 store。`fsync: sync` 与集群档不变。文档把 async 的丢失窗口写成「掉电最多丢 200ms」。

**必交故事**
- S1：async 档运行中周期刷盘生效、停机干净退出。cmd 或 store 包测试（以 WAL 同步计数或等价可观测量为判据）。

**实现（即 plan）**
1. 在单机档装配处（`cmd/sq/main.go` 的 `store.Open` 之后）挂周期刷盘，复用 `store.(*Store).SyncWAL`（`store.go:283`）；更新 `docs/configuration.md` 中 fsync 档说明。
2. 验收：新增测试 + `go test ./internal/store/ ./cmd/sq/ -count=1` 绿。
3. 全量 `go build ./... && go vet ./...`。

---

## Out of Scope（本批整体）

- 永不做：follower 收到即应答、投票状态不落盘（破坏 raft 安全性）；leader 本地落盘即确认（类 Kafka acks=1，会复用 offset）。
- 本期不做、后续要做（落 `docs/roadmap.md`）：inflight 二进制编码（两步升级）；RawNode 自驱；raft 条目格式级的提案合并；快照拉取流水线化；按机器内存自动推算缓存；`CheckQuorum` 开启与 `MaxCommittedSizePerReady` 调大（审计发现，未列入本批）；控制台改读内存态。

## 读数与证据

- `git merge-base --is-ancestor <引入 msgVersionV1 的提交> v0.1.0 && echo` → `v0.1.0 含二进制解码`
- `grep -n "func (s \*Store) SyncWAL" internal/store/store.go` → `283:func (s *Store) SyncWAL() error {`
- `grep -n "defaultRetainEntries" internal/cluster/manager.go` → `260:const defaultRetainEntries uint64 = 10000`
- 本机剖析原始数据：`/private/tmp/claude-501/-Users-df-zidong--claude-spares-spare-DqhYVb/98ca6811-45c1-4c05-ab6b-dd70b9886a38/scratchpad/prof/`（produce_async：`core.NewMessageID` cum 39.55%；deliver_async：`syscall.pread` cum 65.55%，`store.(*Store).Get` 60.26%）
- e2e 基线：`cluster/quorum-mem/conc=64  14696 msg/s  p50=2.592ms  p99=46.846ms`
