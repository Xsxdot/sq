# SQ4.9 实现计划：指标采集不再周期全扫

- 卡：SQ4.9（父卡 SQ4）｜分支 `cards/sq4`
- 需求：`docs/superpowers/specs/2026-11-11-sq4-perf-batch-design.md`「SQ4.9」节（已批准 2026-11-11）、批次红线（不改盘上与线上格式）、批次 Out of Scope「控制台改读内存态（本期不做）」
- 前置（硬依赖）：SQ4.1。inflight 计数取自 SQ4.1 计划（`docs/superpowers/plans/sq4.1-plan.md`，已拍板）预留的 `(d *Deliverer) InflightCount(group, topic string, q uint32) (n int, ok bool)`，`ok=false` 表示本节点没有装载这个队列，此时回落读 pebble。SQ4.1 又排在 SQ1、SQ2 之后。施工排序：SQ4.5 改 `internal/store/store.go` 的 `Open`，本卡也要碰这个文件（Q1），后实现的一方 rebase。
- 级别：L2。无骨架、无冻结对象。复用的契约：SQ4.1 的 `InflightCount`，Prometheus 指标名与标签（第 4 节逐条列出，不变），盘上键布局（不变）。本卡新增的面：`store` 的一个只读计数方法（Q1）、`metrics.NewSampler`/`NewRegistry`/`NewCollector` 的入参（进程内，调用方是 main 和测试）。
- 状态：**已拍板（2026-11-11，主会话）**——Q1 采纳 store 层计数，附条件：store 只提供通用的「按前缀计数」能力，由装配方（main/metrics）注册 delay/、half/ 前缀，store 不认识 delay/half 语义；spec 方案段已随之修订（用户不可见的实现落点变更，不回用户）。Q2–Q4 按推荐。Q5 不纳入本卡（控制台读快照落 roadmap）

---

## 待拍板岔口

| # | 岔口 | 推荐 | 被否方案与理由 |
|---|---|---|---|
| Q1 | delay / half 深度在哪里、由谁在内存维护 | **在 store 层按键族精确计数，全部节点都维护**。store 对 `delay/`、`half/` 两个键族维护内存条数。`Open` 时扫一次建立基数。之后所有经 store 提交的批次（`ApplyWith`/`Apply`/`ApplyAsync`，单机写入和集群 FSM apply 都走这里），只要触及这两个键族，就在一把计数锁内先逐键查「提交前是否存在」，提交成功后再加减：不存在的键被 Set 记 +1，存在的键被 Delete 记 −1，其余记 0；同一批内同一个键的多次操作按顺序叠加。批次里有 DeleteRange 与计数键族相交时（快照安装 `snapinstall.go:98`），提交后在锁内对该键族重数一次。对外只暴露一个只读读数方法。效果：单机、leader、follower 三种节点的读数都等于本节点的全扫结果，「每个节点报本节点副本视图」的现有语义不变，也不需要切主重数。**与 spec 措辞的差异**：spec 写的是「由所在组 leader 在内存维护计数（暂存、搬运、撤回、提交/回滚时增减，切主时从盘重数一次）」，本方案换了维护者与维护位置，「不再周期全扫、内存计数」的意图不变 | ① **spec 原文方案**：由 delay 与 txn 包在 meta leader 上各自增减，切主时（SQ1 `WaitWritable` 返回的 epoch 变化后的首次采样）从盘重数一次。不可行，原因有二。(a) 漏数：`txn.go:145` 等处的 half 写入走 `ApplyOrForward`，从非 leader 节点经 `ForwardApply` 到达 leader，在 `main.go:698-716` `handleForwardApply` 里直接 `cl.Apply` 原始批次，leader 上的 txn 包代码根本看不到这次写入。(b) follower 没有计数，只能二选一：继续全扫（S2 在 follower 上不成立）或者不再输出这两个指标（Prometheus 用户按节点看到序列消失，S3 不成立）。② 在 cluster 的 `OnApplied` 钩子里数：钩子在提交**之后**才触发（`group.go:1505-1518`），无法判断被删的键提交前是否存在，End 与回查并发对同一个 half 双删时会漂移；单机档又没有这个钩子，要做两套机制。③ 在 meta leader 的提案入口数：提案时查存在性，与 apply 顺序之间有竞态，同样会漂移 |
| Q2 | `InflightCount` 返回 `ok=false` 时怎么办（本节点是 follower，或是 leader 但这个队列还没被访问过） | **回落为该队列的 pebble 前缀计数**（`InflightPrefix(group, topic, q)`）。follower 上看到的是复制过来的检查点状态，最多滞后一个检查点间隔，与 cursor 的滞后同一口径 | 不输出这个队列：follower 节点上 `sq_group_inflight_messages` 序列会消失，Prometheus 侧可感知（S3）。记 0：数值错误 |
| Q3 | 落后量（pending）用哪个 cursor | **仍读盘上的 `cursor/`**（一次前缀扫，规模是组×topic×队列数，与积压量无关）。队列集合也从这次扫描枚举，逐队列调 `InflightCount`。这样不需要新增 deliver 接口 | 读内存 cursor：要给 deliver 新增一个「遍历已装载队列」的接口，而 SQ4.1 明确说过「SQ4.9 有了真实形状再加」。现在的形状不需要它：盘上 cursor 最多滞后一个检查点间隔，spec 也只承诺 inflight 取自内存。代价见第 6 节残余 |
| Q4 | Prometheus 抓取读什么 | **读采样器最近一次的完整快照**：采样器每 5s 把完整的 `*Stats` 存进一个原子指针。还没有快照（启动后首次采样之前）或没有装配采样器时，当场用新的低成本路径算一次 | 每次抓取都现算（低成本路径）：spec 明确要求「抓取改读采样器最近一次结果」。缺点是值最多滞后 5s，Prometheus 常见的抓取间隔 ≥15s，可以接受，文档写明 |
| Q5 | 控制台的全扫（`/admin/overview` 调 `metrics.Collect`、`/admin/ledger`、`/admin/groups/{name}`）要不要顺带改 | **本卡不改**。spec 批次 Out of Scope 写了「控制台改读内存态（本期不做）」。保留原有全扫函数 `metrics.Collect` 不动：控制台继续用它，它同时是 S1 的对照基准 | 建议另行考虑：`handleOverview`（`admin/messages.go:355`）每次请求就是一次跟旧采样完全相同的全扫，控制台总览页每 5s 轮询一次，开着页面就重现了本卡要消除的成本。改成读采样器快照只要改一处调用，与「读内存态」无关。是否纳入，由主会话决定；默认不纳入 |

---

## 1. 交付目标

对应 spec SQ4.9 的 S1–S3，做到下面三点：

1. 采样器（5s）和 Prometheus 抓取都不再扫 `delay/`、`half/` 前缀，也不再整前缀扫 `inflight/`：
   - delay / half 深度读 store 的内存计数（Q1）。
   - inflight 先读 `InflightCount`，`ok=false` 时才回落到该队列的前缀计数（Q2）。
   - cursor 前缀扫与逐队列读 alloc 保留（规模与积压量无关，Q3）。
2. 抓取读采样器最近一次的快照（Q4）。
3. 全部指标名、标签、类型、HELP 文本不变。控制台的时序图表照旧从采样器的环里取数，不回退。

不做：控制台各端点（Q5）；按组维度的内存 cursor；采样间隔与保留期的调整。

## 2. 改动边界

### 文件集（超出须升级，见第 5 节）

| 文件 | 改什么 |
|---|---|
| `internal/store/prefixcount.go`（新） | 计数键族表（`delay/`、`half/`，前缀取 `keys.go` 已导出的 `DelayPrefix`/`HalfPrefix`，与 `stats.go` 现在的扫描口径完全一致：`half/` 不含 `halfidx/`）；计数锁；建基数；批次分析（用 `batchrepr.Reader` 逐条取键，先例 `store.go:212` `BatchTouchesPrefix`）；DeleteRange 相交判定后重数；只读读数方法（命名由执行者定，例如 `KeyCount(prefix)`）。文件头写清职责、边界、并发论证。 |
| `internal/store/store.go` | `Open` 打开 pebble 后建基数。`ApplyWith` 与 `ApplyAsync` 两个提交点接上计数：批次不触及计数键族就零额外工作；触及的话，「查存在 → 提交 → 加减」整段持计数锁。`ApplyAsync` 的 sync 分支在 `ApplyNoSyncWait` 返回时批次已经可见（见末节 pebble 读数），所以锁只需持到它返回，不必等 fsync。本文件只做接线，逻辑放在新文件，以减少和 SQ4.5 的冲突。 |
| `internal/metrics/stats.go` | 保留 `Collect(st, mt)` 原样（全扫，作控制台数据源和 S1 对照基准）。新增低成本采集函数，入参多一个 inflight 数据源接口（本包定义，由 `*deliver.Deliverer` 实现）。本包所有 `st.Scan` 调用统一走一个包内辅助函数，它累计「扫描键数」，未导出，供 S2 观测，Collect 也用它。更新文件头「边界」段。 |
| `internal/metrics/series.go` | `NewSampler` 增加 inflight 数据源参数。`collect` 改用低成本函数。每次采样把完整的 `*Stats` 存入原子指针，并提供读取方法供 Collector 使用。更新文件头里「采样代价 = 一次全扫」这段说明。 |
| `internal/metrics/collector.go` | `Collector` 持有可选的采样器和 inflight 数据源。抓取时，有快照就用快照；没有就当场走低成本函数。`NewCollector`/`NewRegistry` 的入参相应增加（nil 安全，测试可不传）。业务指标的输出代码不变（指标名、标签、类型、顺序都不动）。 |
| `internal/core/deliver/*`（SQ4.1 的 `state.go`） | **只在第 3 节步骤 1 H2 不成立时**才改：让 `InflightCount` 在本节点失去该组领导权、或 epoch 过期时返回 `ok=false`。 |
| `cmd/sq/main.go` | `admin_listen` 分支调整顺序：先建采样器，再建 registry。传入 `dl`（同时作为 FilterStats 和 inflight 数据源）。 |
| `internal/admin/server_test.go` | 跟随构造函数签名改动（:47、:51、:73），不改断言。 |
| `docs/admin-api.md` | `/metrics` 一节写明：业务指标是采样器快照，最多滞后 5s。 |
| 测试 | `internal/store/prefixcount_test.go`（新）、`internal/metrics/live_test.go`（新）、`internal/metrics/metrics_test.go` 跟随签名改动、`test/e2e/metrics_cluster_test.go`（新，真机首跑项） |

明令不碰：`internal/admin/` 下的生产代码（Q5）；`delay`、`txn`、`produce` 包（Q1 不在它们里面计数）；`cluster`、`replication`、`rpc`；盘上键与值的编码；指标定义（`collector.go:58-89` 的 `prometheus.NewDesc` 全部原样保留）。

### 真实入口

- 采样：`cmd/sq/main.go:457-460` `metrics.NewSampler` → `Sampler.Run` → `tick` → `collect`（`series.go:177`）
- 抓取：`GET /metrics`（`admin/server.go:93` `promhttp.HandlerFor(reg)`）→ `Collector.Collect`（`collector.go:125`）
- 计数的维护点：全部 `store.Batch` 提交。单机档的写入方：`produce.AppendDelay`（`produce.go:297`）、`delay.Scheduler` 搬运与撤回（`delay.go:233,281`）、`txn`（`txn.go:137-145,195-205,393,467-533`）。集群档：上面这些写入经 raft 提案后，在每个节点的 FSM apply 里提交（`group.go:1506` `gr.st.ApplyWith(b, false)`）；快照安装经 `snapinstall.go:98` 的 DeleteRange 加批量 Set 提交。

### 计数的正确性论证（承重，执行者须在 `prefixcount.go` 头部写成注释）

- **全覆盖**：本仓库对 pebble 的全部键值写入都经 `Store` 的 `ApplyWith`/`ApplyAsync`（`Apply` 也走 `ApplyWith`）。`SyncWAL` 只写 LogData，不改变键空间（`store.go:254-282` 注释）。所以每一次 `delay/`、`half/` 键的变化都经过计数点。执行者在步骤 1 用 rg 复核，确认没有绕开 `Batch` 的 `s.db` 写入。
- **精确**：「查存在 + 提交 + 加减」整段在计数锁内，触及计数键族的批次因此串行化；不触及的批次不取锁、不受影响。pebble 提交返回时批次已经发布可见（`commit.go:327` apply、`:336` publish，都在 `:340` 等待同步之前），下一个持锁者查存在性一定能看到上一批的结果。提交失败就不加减。
- **基数**：`Open` 时还没有任何写入方，扫一次就是精确基数。DeleteRange 与计数键族相交时提交后在锁内重数，只在快照安装时发生。
- 由此可得：任何时刻，计数等于对本节点 pebble 全扫的结果（不变式 C1）。

## 3. 工作顺序

每一步都是「做 → 检查」，检查不过不进下一步。

0. **基线**（rebase 之前就能做）：用当前代码在 metrics 包写一个基准，测数据集「其他数据固定 + delay 0 / 10 万条 + half 0 / 1 万条」下 `Collect` 的耗时与扫描键数，存进末节，作为 S2 的反面对照。
1. **rebase 到 SQ4.1 之上**，核对三条承重假设，任一不成立就停下回报主会话，除非下面写明可以自行处理：
   - **H1（store 是唯一写入口）**：`rg -n "s\.db\.|\.db\.(Set|Delete|Apply|Ingest)" internal/store` 加上全仓 `rg -n "pebble\.(Open|Batch)" --type go -g '!*_test.go'`，确认除 `SyncWAL` 外没有绕开 `Batch` 的写入。不成立就停下。
   - **H2（InflightCount 在失去领导权后的语义）**：读 SQ4.1 落地的 `InflightCount`。本节点失去该组领导权（或 epoch 已变）而内存态还没被丢弃时，它要返回 `ok=false`，否则前 leader 会一直报一个冻结的旧值。不成立时，执行者**可以**在 deliver 里给 `InflightCount` 补上这个判断：用 `rt.IsLeader(GroupForQueue)` 加 epoch 比较，不阻塞、不调 `WaitWritable`，并在交回物里写明。如果补这个判断需要改 SQ4.1 的其他行为，就停下。
   - **H3（pebble 可见性）**：`~/go/pkg/mod/github.com/cockroachdb/pebble/v2@v2.1.6/commit.go` 的 `commitPipeline.Commit` 里，publish 发生在 noSyncWait 返回之前（出稿时已读，见末节）。如果 go.mod 里 pebble 的版本变了，就重读确认。
2. **store 计数**（`prefixcount.go` + `store.go` 接线）。检查：C1、C2、C3 绿；变异：去掉「提交前查存在」改成盲加减，C1 必须红。
3. **metrics 低成本路径 + 采样器快照 + Collector 读快照 + main 接线**。检查：`go build ./... && go vet ./...`；`go test ./internal/metrics/ ./internal/admin/ ./cmd/... -count=1` 全绿；**S1 绿，这是首条故事级检查，绿后立即跑真机首跑项**。
4. S2、S3。
5. 文档、包头注释收口，全量回归。

## 4. 验收方式

### 故事级检查

| 故事 | 命令 | 判据 |
|---|---|---|
| **S1** | `go test ./internal/metrics/ -run 'TestLiveMatchesFullScan' -count=1 -v 2>&1 \| tee /tmp/sq4.9-s1.log` | 单机装配：store、meta、produce、deliver、`delay.Scheduler`、`txn.Manager`（各包测试 fixture 已有现成装配：`metrics_test.go:30-44`、`delay_test.go:59`、`txn_test.go:44`）。用 20 个随机种子各生成一串操作：多 topic 多队列写入、批量写入；两个以上消费组取件、部分 ack、改不可见、转死信；延时暂存若干条，再撤回一部分，再让一部分到期后跑 `Pass` 搬运；事务 Prepare 若干条，再 commit / rollback 一部分。每个种子跑完后静置，然后比较 `新函数(st, mt, dl)` 与 `Collect(st, mt)` 的 `Stats` 全部字段，要求完全相等。两种 deliver 模式各跑一遍：逐次复制模式，以及检查点模式（间隔 20ms，静置 ≥ 5 个间隔后再比）。**防空过**：用一个包装 `dl` 的数据源统计 `ok=true` 命中次数，必须 > 0；新函数执行期间 `delay/`、`half/` 两个前缀的扫描键数必须为 0。变异：让新函数在 `ok=false` 时记 0，必须红。 |
| **S2** | `go test ./internal/metrics/ -run 'TestSampleCostIndependentOfBacklog' -count=1 -v` | 数据集 A：其他数据固定，delay 10 条、half 10 条。数据集 B：同一份其他数据，delay 10 万条、half 1 万条。以包内「扫描键数」计数器为观测量：① 采样器一次 `collect` 的扫描键数，A 与 B 相等；② 采样器已经采过一次之后，`reg.Gather()` 期间扫描键数增量为 0；③ 正向对照：`Collect` 在 B 上的扫描键数比 A 多 ≥ 10 万（证明这个观测量本身有效）。耗时只记录、不判定。 |
| **S3** | `go test ./internal/metrics/ -run 'TestMetricNamesAndLabelsUnchanged' -count=1 -v`，外加 `go test ./internal/admin/ -count=1` 与真机清单第 2 条 | 装配完整的 registry（采样器、tx、conns、fs、sys 都非 nil），先采一次样，再 `Gather`。断言业务与系统指标族的集合恰好是下面这些（名字 / 类型 / 标签键），HELP 文本与 `collector.go:58-89`、`:203-209` 逐字相同：`sq_topics` gauge 无标签；`sq_groups` gauge 无标签；`sq_delay_depth` gauge 无标签；`sq_half_messages` gauge 无标签；`sq_topic_messages_written_total` counter {topic}；`sq_group_pending_messages` gauge {group, topic}；`sq_group_inflight_messages` gauge {group, topic}；`sq_txn_checks_total` counter 无标签；`sq_txn_dropped_total` counter 无标签；`sq_connections` gauge 无标签；`sq_filter_skipped_total` counter {topic, group, reason}；`sq_disk_used_percent` gauge 无标签；`sq_disk_free_bytes` gauge 无标签；`sq_data_dir_bytes` gauge 无标签；`sq_write_blocked` gauge 无标签；`sq_store_apply_duration_seconds` histogram 无标签。`go_*`、`process_*` 只断言仍然存在。控制台方面：admin 包测试全绿，e2e `TestTimeseriesEndToEnd`、`TestLedgerReflectsConsumption` 全绿（后者按 SQ4.1 的轮询化约定执行） |

### 不变式单测（只为承重规则）

- **C1（计数 = 全扫）**：store 包。随机操作序列：新键 Set、已有键 Set、已有键 Delete、不存在的键 Delete、同一批里先 Delete 后 Set 同一个键（模拟 `txn` 重排期）、与计数键族相交和不相交的 DeleteRange、混入非计数键。提交方式在 `Apply`、`ApplyAsync`（sync 档与 nosync 档）、`ApplyWith` 之间轮换。每次提交后断言两个计数都等于前缀全扫的结果。变异：改成盲加减，必须红。
- **C2（并发）**：8 个 goroutine 并发对重叠的键集合做 C1 的操作，其中两个专门对同一批键重复 Delete（模拟 End 与回查并发双删），用 `-race` 跑。结束后计数等于全扫结果。
- **C3（基数与重开）**：写入若干后 `Close`，再 `Open`，计数等于全扫结果；不触及计数键族的批次不取计数锁（用测试计数或基准断言，手段由执行者定）。
- **C4（快照式 DeleteRange）**：构造一个类似 `snapinstall` 的批次（整段 DeleteRange 计数键族后再 Set 新内容），提交后计数等于全扫结果。

### 编译与包测试

- `go build ./... && go vet ./...`
- `set -o pipefail; go test -timeout 15m -count=1 ./internal/... ./cmd/... 2>&1 | tee /tmp/sq4.9-pkgs.log`
- `go test -race -count=1 ./internal/store/ ./internal/metrics/`
- 写路径回归：`go test ./internal/core/produce/ -run '^$' -bench 'AppendParallel' -benchtime 2s -count 3`，改前改后对比，回退 > 3% 就记进交回物（普通消息批次不触及计数键族，理应只多一次批次遍历）

### 真机清单（本机进程级，`test/e2e`）

1. **首跑项**（步骤 3 S1 转绿后立即跑）：新文件 `test/e2e/metrics_cluster_test.go`，用例 `TestClusterMetricsDepthOnAllNodes`，复用 `startProcCluster`（`cluster_proc_test.go:58`，每个节点自带 `AdminListen`，见 :75）。三节点集群里用 SDK 发 K 条延时 10 分钟的消息，撤回其中 R 条；发 T 条事务消息，只 Prepare 不提交（回查返回 UNKNOWN，具体手段参考 `sdk_txn_test.go:210` 孤儿用例）。轮询 ≤ 15s，判据是**三个节点**的 `/metrics` 中 `sq_delay_depth == K−R`、`sq_half_messages == T`（follower 走 apply 路径计数）。接着 kill -9 meta 组 leader 再重启它，等 ≤ 30s：三个节点的读数仍然等于期望值（覆盖重启后 `Open` 重建基数和切主两条路径）。命令：`cd test/e2e && go test -tags e2e -count=1 -run 'TestClusterMetricsDepthOnAllNodes' -v -timeout 20m 2>&1 | tee /tmp/sq4.9-e2e-first.log`。
2. `cd test/e2e && go test -tags e2e -count=1 -run 'TestAdminAPISmoke|TestTimeseriesEndToEnd|TestLedgerReflectsConsumption|TestConsoleServedFromBinary' -v -timeout 20m`，全部 PASS。
3. 全量 `make e2e`，全部 PASS。
4. 启动成本：在 store 包写一个基准，测 `Open` 一个有 100 万条 delay 键（值 1KB）的库要多久，数字进卡。spec 接受「从盘重数一次」的成本，这里只记录、不判定。

须存证据：每条命令末尾的 PASS/FAIL 行；S1 的种子数与 `ok=true` 命中次数；S2 的扫描键数表（A / B，新旧两种算法）；步骤 0 的基线；首跑项中三个节点各自的读数；`Open` 的耗时。

## 5. 决策权限

执行者可以自己定（交回物「偏离与自主决定」逐条对照本节）：

- `prefixcount.go` 的内部结构、批次分析实现、读数方法与新采集函数的命名、inflight 数据源接口的命名。
- `Collector` 拿采样器快照的方式（方法还是函数值）、没有快照时的回退写法。
- 扫描键数计数器的形态（未导出、对生产零语义影响即可）。
- S1/S2 的数据规模与种子数，在不放宽判据的前提下。
- 第 3 节步骤 1 H2 不成立时，在 `InflightCount` 里补「失去领导权即 `ok=false`」的判断（只能是非阻塞的判断，须在交回物写明）。
- 测试为跟随签名改动做的调整（不改断言）。

必须停下升级：

- 第 3 节步骤 1 的 H1 不成立；H2 的修补需要改 SQ4.1 的其他行为；H3 因版本变化不成立。
- 需要改任何指标名、标签、类型、HELP 文本，或者某个指标在某类节点上消失。
- 需要改控制台生产代码（Q5 默认不纳入，主会话另行裁决的除外）、需要改 delay / txn / produce / cluster / replication。
- 计数需要额外的盘上键（违反红线），或者计数锁需要持到 fsync 完成。
- S1 出现不相等、而原因是「全扫口径本身」造成的（例如 `half/` 前缀边界），这种情况属于口径问题，回报主会话，不自行调整任一侧。
- 写路径基准回退 > 10%。

## 6. 风险与残余

- **最大风险：计数被一条没有登记的写入路径绕过，从此永久漂移**。全覆盖论证依赖「store 是唯一写入口」，由 H1 守住；C1、C2 守住计数逻辑本身；真机首跑项守住集群 apply 和重启两条路径。还有一个隐患：将来有人新增直接用 `s.db` 的写入，计数会悄悄出错。对策：`prefixcount.go` 头部和 `store.go` 类型注释里写明「新增写入口必须接计数」，C1 的随机测试会覆盖到所有现存的提交方法。
- **快照安装时的重数**：DeleteRange 相交时在计数锁内重数，期间本节点所有触及 delay/half 的提交都会阻塞。快照安装发生在 follower 上，meta 组 follower 没有本地的 delay/half 写入方，只有 apply，影响就是 apply 慢一段时间。如果 C4 或真机测下来停顿明显（> 1s），记进卡里。
- **启动多一次扫描**：`Open` 时对 delay/half 扫一次，积压百万条时是秒级的一次性成本（真机清单第 4 条测数）。spec 接受「切主时重数一次」，本方案把这个成本挪到了启动时，切主时反而不用再数。
- **滞后**：抓取值最多滞后 5s（Q4）；pending 用盘上 cursor，inflight 在 leader 上读内存，检查点模式下两者最多相差一个检查点间隔的投递量（Q3）。另外，只存在于内存、还没做过检查点的 (group, topic, queue) 不会出现在枚举里，最多持续一个检查点间隔。三者都在文档里写明，不修。
- **follower 上 inflight 回落仍是前缀计数**：成本与在外条数成正比，与积压量无关。spec S2 只约束 delay/half 积压，这是有意保留，记进卡。
- **控制台仍然全扫**（Q5）：`/admin/overview`、`/admin/ledger`、`/admin/groups/{name}` 不变，记进卡里的残余。roadmap 已有「控制台改读内存态」。

## 7. 工作量估计

生产代码约 300–400 行（store 计数约占一半），测试约 600 行（含 e2e 一个文件），1–2 个实现会话。

---

## 读数与证据

- `internal/metrics/stats.go:36-108` `Collect`：逐 topic×队列 `st.Get(AllocKey)`；`:65` cursor 全前缀扫；`:79` `InflightAllPrefix` 全前缀扫；`:91` `DelayPrefix` 全扫；`:101` `HalfPrefix` 全扫（注释说明 `halfidx/` 落在区间外）
- `internal/metrics/collector.go:125-137` 每次抓取都调 `Collect(c.st, c.mt)`；`:58-89` 15 个 `prometheus.NewDesc`；`:203-209` `sq_store_apply_duration_seconds` 直方图；`:52` FilterStats 接口（`*deliver.Deliverer` 实现），metrics 包已经 import deliver
- `internal/metrics/series.go:40` `sampleInterval = 5 * time.Second`；`:177-180` `collect` 调 `Collect`；`:54-65` `Sample` 字段里没有 Topics / Groups / Half 与死信分 topic 写入量（所以 Q4 要存完整的 `*Stats`，不能只存 Sample）
- `cmd/sq/main.go:449-461`：`admin_listen` 非空才装配，先 `NewRegistry(st, mt, sys, tx, srv, dl, logger)`，后 `NewSampler(st, mt, retention, logger)`；`:400` `deliver.New`
- `internal/admin/messages.go:349-355` `handleOverview` 调 `metrics.Collect`；`internal/admin/timeseries.go:133-150` ledger 扫 cursor 后逐队列扫 inflight；`internal/admin/groups.go:54-69` 同款；spec 批次 Out of Scope：「控制台改读内存态」本期不做
- `internal/cluster/group.go:378` `DisableProposalForwarding: true`（follower 上 Propose 直接报错，每条日志条目都由当任 leader 本地提案）
- `internal/core/txn/txn.go:145,196,205,261` half 相关批次走 `replication.ApplyOrForward`（非 leader 节点转发）；`cmd/sq/main.go:698-716` `handleForwardApply` 把转发来的 repr 原样 `cl.Apply`，leader 侧 txn 包代码看不到
- `internal/core/delay/delay.go:156` `Pass` 只在 meta leader 上跑；`moveOne`/`Recall` 持 `moveMu` 串行（delay.go 第 200 行以后）
- `internal/cluster/group.go:1505-1518` FSM apply：`applyMu` 内 `gr.st.ApplyWith(b, false)`，提交后才逐条 `notifyApplied`
- `internal/cluster/snapinstall.go:91-98` 快照安装对 meta 组键族按连续前缀 DeleteRange
- `internal/store/store.go:212-232` `BatchTouchesPrefix`（`batchrepr.Reader` 逐条取键的先例）；`:237-253` `ApplyWith`；`:311` `Apply` = `ApplyWith(b, s.sync)`；`:334-354` `ApplyAsync`（sync 档走 `ApplyNoSyncWait`）；`:254-282` `SyncWAL` 只写 LogData
- `internal/store/keys.go:32-37` 导出 `DelayPrefix`、`HalfPrefix`
- `~/go/pkg/mod/github.com/cockroachdb/pebble/v2@v2.1.6/commit.go:298` `commitPipeline.Commit`：`:327` `p.env.apply(b, mem)` → `:336` `p.publish(b)` → `:340` `if !noSyncWait { 等待 }`。publish 发生在 noSyncWait 返回之前（H3）
- `docs/superpowers/plans/sq4.5-plan.md:47` SQ4.5 同样改 `internal/store/store.go` 的 `Open`（施工排序）
- `internal/metrics/metrics_test.go:46-77` `TestCollectDerivesStats`、`:79` `TestRegistryExposesMetrics`（`NewRegistry` 不带采样器，Append 后立即断言 written_total = 1，靠 Q4 的「无快照时当场算」保持原样通过）
- `internal/admin/server_test.go:47,51,73` `NewSampler`/`NewRegistry` 装配点
- `test/e2e/cluster_proc_test.go:58,75` `startProcCluster` 每节点设 `AdminListen`；`test/e2e/admin_test.go:117-124` `/metrics` 只断言含 `sq_topic_messages_written_total`
- SQ4.1 计划 第 2 节「对外只读面」：`InflightCount` 读原子镜像、不取锁，`ok=false` 表示本节点未装载（非 leader 或尚未访问），「不提前做按组/按 topic 聚合的遍历接口」
