# SQ4.10 计划：raft 日志截断按字节 / 时间保留，死 peer 宽限期（L2）

> 状态：**已拍板（2026-11-11，主会话）**——B1–B7 按推荐；B3 log_retain_bytes 为按节点总量（文档写明）；B5 删除 hardLagCap、TestTruncateOnceRespectsLeaderMinMatch 断言随 spec 改写。施工排序：SQ1、SQ2、SQ4.4 合入后再开工
> 需求：`docs/superpowers/specs/2026-11-11-sq4-perf-batch-design.md`「SQ4.10」节 + 批次共同红线（不改盘上与线上格式：seglog 段文件与帧格式、Pebble 键、raft 消息均不变；滚动升级互通）
> 施工排序（非硬依赖，同文件）：SQ1、SQ2 合入 `cards/sq4` 之后（`manager.go`/`group.go`）；**另加 SQ4.4 之后**（spec 总表未列，出稿时发现：SQ4.4 改 `seglog.go` 的 `Sync` 锁区间，本卡给 seglog 加读路径，同文件且读路径要遵守 SQ4.4 的锁外刷盘约定）；字节配置解析复用 SQ4.5 建的私有解析函数（SQ4.5 计划：「谁先落地谁建」）
> 级别：L2，不出骨架；规模偏大（seglog 读路径 + 两级存储 + 截断策略），计划内拆三个顺序工作单元，不拆卡
> 冻结对象：无新增跨子系统冻结对象。新增用户可见配置键 `cluster.log_retain_bytes`、`cluster.log_peer_grace`（spec 已定名）。复用契约：`raft.Storage`（go.etcd.io/raft/v3 v3.7.0 storage.go 的六方法与错误约定）、seglog 盘上格式（frame.go，不变）
> 执行形态：本会话 subagent 实现，独立 subagent 审，主会话验收

---

## 待拍板岔口

| # | 岔口 | 推荐 | 被否方案与理由 |
|---|---|---|---|
| B1 | MemoryStorage 内存窗口与磁盘保留量怎么解耦 | **两级存储，改 `groupStorage`（snapshot.go:400 起）**：热区仍是 `raft.MemoryStorage`，只留最近一段窗口；冷区 = 本组 seglog 的新读路径。`FirstIndex()` 返回磁盘下界（锚点 +1，原子量），`Term(i)`/`Entries(lo,hi,max)` 在 `i < 热区起点` 时落到冷区读，其余直通 MemoryStorage；`Snapshot()`/`InitialState()`/`LastIndex()` 语义不变。**承重不变式 I-cold：冷区只含 index ≤ applied 的条目**（已提交 ⇒ 永不被换届改写），热区压缩点永远 ≤ applied | ① **自实现完整 Storage 替换 MemoryStorage**：要重写 `Append` 冲突截断、`ApplySnapshot`、`SetHardState` 与快照安装的内存侧，风险与改动面翻倍，换不来额外能力。② **保持全量在内存**：违反 S3。③ **日志进 Pebble 支持随机读**：改盘上格式，红线禁止 |
| B2 | seglog 冷读怎么定位（现有 seglog 无随机读，seglog.go:9-10 文件头明写「不提供随机读接口」） | **「连续段（run）」稀疏索引**：一个 run = 同一段文件内一串连续追加的条目（起始 index、段号、起始偏移、有效终点）；遇段轮转或换届改写（新条目 index ≤ 当前尾）即新起一个 run，改写点之后的旧 run 有效终点裁到改写点 −1（与 Open 的「后写的赢」同一规则，抽成同一个函数供 Open 扫描与 Append 共用）。run 内每约 1MiB 记一个检查点（index → 偏移），冷读从 ≤lo 的最近检查点顺读。另维护任期变化点表（index 起点 → term）供冷区 `Term(i)`。内存开销 ≈ 段数 + 改写次数 + 保留字节/1MiB 个小结构，每 GiB 约 1k 项 | ① **每条一个偏移（稠密索引）**：每条 8–16B，1GiB 小条目（~200B）即 40–80MB，仍随保留量线性增长，与 S3 精神冲突。② **按需整段扫描、不建索引**：raft 每发一条 MsgApp 调一次 `Entries(lo,hi,1MiB)`，追平 1GiB 要上千次，每次最坏扫 64MiB 段，读放大千倍 |
| B3 | `log_retain_bytes` 的口径 | **按节点总量，均分到各组**：每组字节下界 = `log_retain_bytes / (data_groups + 1)`（+1 是 meta 组），文档写明；默认 1GiB / 默认 3 数据组 ⇒ 每组 256MiB（段粒度 64MiB 向上取整，实际多留不少留） | **按组计**：磁盘常驻随组数线性放大（data_groups 上限 64 ⇒ 65GiB），用户读 `1GiB` 不会想到要乘组数 |
| B4 | 「磁盘水位告急」的判据与来源 | **复用 `sysinfo.DiskUsage(dataDir)` 与 `disk_watermark_percent`（默认 85）同一阈值**：`cluster.Options` 新增注入函数 `DiskPressure func() bool`，main.go 装配为「`DiskUsage` 用量百分比 ≥ 水位」，截断循环每轮（每组评估前一次）调用；水位为 0（关闭）时恒 false。与 retention 拒写看的是同一个函数同一个阈值（retention.go:110-133），控制台/指标/截断三方口径一致 | ① **读 retention 维护的 `writeBlocked` 原子量**：它只按 retention 间隔刷新（分钟级），且在 main.go:410 才创建、晚于 `cluster.NewManager`（main.go:308），装配序不允许。② **另设更低的告急阈值**：新增用户可见旋钮，spec 未列 |
| B5 | 现有「绝对上限」`hardLagCap = 10 × retainEntries`（manager.go:262-270）与活着但落后的 peer 怎么处理 | **删除 hardLagCap**：下界规则改为「活 peer 恒作下界；死 peer 宽限期内作下界、超期放弃；磁盘告急时放弃一切 peer 下界（活的也放），只保留按条数与按字节两条下界」。理由：hardLagCap 是条数口径，5k msg/s 下 10 万条约 20 秒即触发，直接让 S1 不成立；它防的「死节点钉死磁盘」改由宽限期 + 磁盘告急兜住 | 保留 hardLagCap：与 S1 冲突（停机 2 分钟必超 10 万条时照样只能拉快照）。把它改成字节口径：与「按字节保留」概念重叠，用户无法预期两个字节数谁生效 |
| B6 | 热区窗口大小与压缩节奏 | **窗口 = min(`log_retain_entries` 条, 64MiB)（字节上限为内部常量，不对用户暴露）；压缩在 apply 路径触发**：每次 apply 推进后若热区条数或字节 > 2× 窗口，压到「applied − 窗口」（`MemoryStorage.Compact` 每次复制剩余切片，storage.go:268-288，按 2× 触发摊还 O(1)）。热区内存上界 ≈ 2× 窗口 + 未提交尾部 | 沿用 30s 截断循环顺带压缩：两次压缩之间整 30s 的写入全在内存（现状即如此），高写入下远超窗口 |
| B7 | 冷读 I/O 失败怎么回 raft | **映射为 `raft.ErrCompacted` + Warn**：raft 对该 peer 改发快照（raft.go:625-629、646-648），可用性不受损；绝不返回其它错误——raft 对 Storage 的非 ErrCompacted/ErrUnavailable 错误直接 panic（log.go `term()` 末尾、`slice()` :515-520） | fail-stop（panic）：一块坏扇区让 leader 进程退出、整组切主，代价远大于给一个 peer 发快照 |

拍板后逐条回写本节，状态行改「已拍板（日期）」。

---

## 1. 交付目标

对应 spec SQ4.10 的 S1–S3：

1. 截断点（锚点 upto）= 以下三者里**最小**的那个（即保留量取最大）：
   - 按条数：`min(applied, peer 下界) − log_retain_entries`（保持现有「peer 下界再减保留余量」的写法）；
   - 按字节：保证 upto 之后的段总字节 ≥ 每组字节下界（B3），段粒度、只多不少；
   - peer 下界（只在 leader 上有）：learner 除外；活 peer 取 Match；死 peer（存活探测失败）自首次判死起 `log_peer_grace`（默认 10m）内仍取 Match，超期放弃并 Warn；磁盘告急（B4）时放弃全部 peer 下界并 Warn。hardLagCap 删除（B5）。
2. 落后 peer 的 Next 只要 ≥ 磁盘下界，leader 就经冷读（B1/B2）发日志，不发快照。快照只在三种情况发生：peer 落后于磁盘截断点（超宽限/磁盘告急/learner 超截断）、冷读 I/O 失败（B7）、或新加入的空节点落后于截断点。
3. 内存不随保留量增长：热区 ≤ 2× 窗口（B6）；`raftStore` 的 `recovered[g].ents` 全量缓存（raftstore.go:138-141、Persist 末尾 `rec.ents = append(...)`）**删除**——它今天就把截断前的全部条目指针钉在内存里；重启时只把热区读进 MemoryStorage，不再整份日志读进内存。
4. 配置：`cluster.log_retain_bytes`（字节串，IEC 单位，默认 1GiB，下限 64MiB——低于一段无意义），`cluster.log_peer_grace`（Go duration，默认 10m，须 >0）；缺省/覆盖/非法值启动即拒。更新 `docs/configuration.md`（含 B3 口径与「磁盘占用 ≈ 字节下界 + 宽限期内写入量」估算）与 `sq.example.yaml`。
5. 可观测：截断循环每轮若有冷读发生，按组打一行 Info（冷读次数、字节）；死 peer 进入/超出宽限期、磁盘告急放弃下界各一行 Warn（只在状态翻转时打）。

**不做**：快照拉取流水线化（spec Out of Scope）；改段大小/截断间隔缺省；follower 侧感知 peer 进度（follower 只按条数与字节截断，现状同）。

## 2. 改动边界

### 文件集（白名单，超出即升级）

| 工作单元 | 文件 | 改什么 |
|---|---|---|
| A seglog 读路径 | `internal/cluster/seglog/seglog.go` | 新增 run 索引、检查点、任期变化点表的维护（Append、轮转、TruncateTo、Open 扫描共用一套「后写的赢」更新函数）；新增只读方法：按 index 区间读条目（带字节上限）、按 index 查任期、各段字节/最大 index 的只读视图（供按字节下界计算）；`Open` 不再在内存里累积全部条目（返回值改为不含全量条目，热区由调用方按需读）；文件头「不提供随机读接口」改写；读路径用独立只读句柄 `ReadAt`，锁内只取索引快照与活动段已写长度，遵守 SQ4.4 锁外刷盘后的锁纪律 |
| A | `internal/cluster/seglog/seglog_test.go` | §4.2 第 1–4 条 |
| B 两级存储 | `internal/cluster/snapshot.go` | `groupStorage` 两级化（B1）；冷区错误映射（B7）；冷读计数 |
| B | `internal/cluster/raftstore.go` | 删 `logRecovered.ents` 缓存与 Persist/TruncateLog 里对它的维护；`Load` 改为返回 HardState + 锚点 + 「热区起点之后」的条目（经 seglog 读路径）；给 groupStorage 暴露该组冷读入口；`confStateFromEntries` 的旧目录回退分支（manager.go:610/650）若仍需全量条目，经读路径按需读全量（一次性升级路径，允许） |
| B | `internal/cluster/manager.go` | `buildGroup`（:584 起）按两级存储播种：MemoryStorage 以「热区起点 −1」为快照元数据起点（任期取自冷区任期表）、磁盘下界取锚点 +1 |
| B | `internal/cluster/group.go` | apply 推进处触发热区压缩（B6，只读 applied 后调用 `mem.Compact`，不进 raft 主循环）；`installSnapshot` 第 6 步（:1180-1196）在 `applyMu` 内同步更新磁盘下界为 `meta.Index+1` |
| B | `internal/cluster/recovery.go` | 仅随 `Load` 返回值变化做适配（`inspectGroupState` :302 起） |
| C 截断策略 | `internal/cluster/manager.go` | `truncateOnceWith`（:1160 起）按 §1 第 1 条重写下界；删 `hardLagCapFactor`；新增死 peer 首次判死时刻表（Manager 级、只在截断 goroutine 内读写）；`Options` 加 `RetainBytes`、`PeerGrace`、`DiskPressure`（0/nil = 默认/关闭）；截断循环每轮的冷读 Info 行 |
| C | `internal/config/config.go`、`config_test.go` | 两个新键、缺省、校验（字节解析复用 SQ4.5 的私有函数；SQ4.5 未落地则在此建，规格照 SQ4.5 计划第 4 条） |
| C | `cmd/sq/main.go` | `cluster.Options` 装配三项（:288-306），`DiskPressure` 用 `sysinfo.DiskUsage` + `cfg.DiskWatermarkPercent` |
| C | `docs/configuration.md`、`sq.example.yaml` | 新键与口径说明；`log_retain_entries` 说明改为「按条数下界 + 内存热区条数」 |
| 测试 | `internal/cluster/cluster_test.go`、`manager_test.go`、`raftstore_test.go`、`snapshot_test.go`、新增 `internal/cluster/logretain_test.go` | §4.2；既有用例的允许改动见 §5 |
| 测试 | `test/e2e/cluster_expand_test.go`；新增 `test/e2e/cluster_logretain_test.go` | e2e 助手加 `withRetainBytes`/`withPeerGrace` 注入；S1 用例 |

**明令不碰**：seglog 帧格式与段命名（frame.go）、Pebble 键族、`proposer.go`（SQ2/SQ4.3）、SQ1 的可写门逻辑、快照生成/拉取/安装流程本身（除 B 行列出的一处下界更新）、`Persist` 的写入语义。

### 真实入口

- 截断：`Manager.Start` → `truncateLoop`（manager.go:1374）→ `truncateOnceWith` → `SaveSnapMeta` → `TruncateLog` → seglog `TruncateTo`。
- 冷读：raft node.run 的 `maybeSendAppend`（raft.go:618 起）→ `raftLog.term/entries` → `groupStorage.Term/Entries` → seglog 读路径。
- 重启：`NewManager` → `buildGroup` → `raftStore.Load`。
- 快照安装：`appendOnce` → `installSnapshot` 第 6 步。

### 架构方向

全部在 `internal/cluster` 子系统内（seglog 是其下层包，依赖方向 cluster → seglog 不变）；磁盘水位判据以函数注入，cluster 不 import sysinfo。

## 3. 工作顺序

每步「做 → 检查」，不过不进下一步。

0. **前置核对**：`git log --oneline` 确认 SQ1、SQ2、SQ4.4 已合入 `cards/sq4`（三者合并提交均为 HEAD 祖先）；查 config 包是否已有 SQ4.5 的字节解析函数。不满足 → 停，回报主会话。记 `BASE=$(git rev-parse HEAD)`，按 §4.4 前言构建基线二进制。
1. **钉承重事实**（结论带出处写进 snapshot.go `groupStorage` 注释与 seglog.go 文件头）：
   - F1 raft 只在 node.run goroutine 调 Storage；本仓另有截断循环（`stg.FirstIndex/Term`）、`groupStorage.Snapshot`、`promoteLearners`（`stg.LastIndex`）跨 goroutine 调用 ⇒ 两级存储与 seglog 读路径必须并发安全。
   - F2 raft 对 Storage 错误的约定：`ErrCompacted` → 发快照；`ErrUnavailable`/其它错误在 `term()`/`slice()` 里 panic（log.go，见读数 R7）。
   - F3 `raftLog.firstIndex()` 每次都调 `storage.FirstIndex()`（log.go，unstable 无快照时）⇒ 必须 O(1)。
   - F4 新当选 leader 对 peer 的 `Progress.Match` 从 0 起，死 peer 在宽限期内会以 0 钉住截断——确认这是「多留」的安全方向，写进注释。
   - F5 `MemoryStorage.Compact` 的界：`compactIndex > lastIndex` 直接 Panicf（storage.go:275-277）；压缩点必须 ≤ applied（I-cold）。
   任一与本计划描述不符 → 停，回报。
2. **先写 S1 e2e 并在基线上跑红**（§4.4 E0）：证明用例能区分新旧行为。基线不红（未触发快照）→ 调整用例参数（写入量、`log_retain_entries`、截断间隔）直到基线稳定红；调不红 → 停，回报。
3. **工作单元 A**：seglog 读路径 + §4.2 第 1–4 条。检查：`go test ./internal/cluster/seglog/ -count=1 -race` 绿；`go test ./internal/cluster/ -count=1` 不回归（此时上层尚未接读路径，`Open` 返回值变化引起的编译适配允许在 raftstore 先做最小改动：用读路径读回全量，行为等价）。
4. **工作单元 B**：两级存储、删全量缓存、`Load`/`buildGroup` 播种、热区压缩、快照安装下界。检查：§4.2 第 5–8 条绿；`go test ./internal/cluster/ -count=1` 全绿（截断相关既有用例此时行为未变，因截断策略未改）。
5. **工作单元 C**：截断策略、Options、config、main 装配、文档。检查：§4.2 第 9–12 条绿；`go test ./internal/config/ -count=1` 绿。
6. **既有用例适配**（只按 §5 允许清单）：`go test ./internal/cluster/ -count=1` 全绿。
7. **首条故事级检查**：§4.1 S2 行绿 → **立即跑真机首跑项 E1**（S1 用例在新二进制上转绿）。
8. **变异验证**（§4.2 末）与全量（§4.3）、真机清单剩余项（§4.4）。

## 4. 验收方式

### 4.1 故事级检查

| 故事 | 检查 | 判据 |
|---|---|---|
| S1 停机 2 分钟经日志追平、不触发快照 | §4.4 E1 | 见 E1 |
| S2 宽限期过后 / 磁盘告急照常截断 | `go test ./internal/cluster/ -run 'TestTruncate(PeerGrace\|DiskPressure)' -count=1 -v`（§4.2 第 9、10 条） | 均 PASS |
| S3 内存不随保留量线性增长 | `go test ./internal/cluster/ -run 'TestColdTier' -count=1 -v`（§4.2 第 6、7 条） | 均 PASS |

### 4.2 不变式单测（承重规则才写）

seglog 包（`SegMaxBytes` 调小以跨段，沿用现有测试做法）：
1. **随机读 = 追加内容**：多段追加 N 条（含仅 HardState 轮次），任意 `[lo,hi)` 与字节上限组合读回逐字节等于追加值；字节上限下至少返回一条（raft 契约）。
2. **换届改写后读到最终版本**：追加 1..100，在跨段位置以新任期改写 60..80，再追加；读 50..90 得到新版本；任期查询在 59/60 两侧正确。关闭重开后同样断言（Open 重建的索引与运行期一致）。
3. **截断后越界读报 compacted 类错误**、不返回已删段内容；`TruncateTo` 与并发读（`-race`）无数据竞争、无读到半删文件的错误。
4. **torn tail 重开后读路径止于最后一条好帧**。

cluster 包（新增 `logretain_test.go` 为主）：
5. **两级存储读一致**：单节点组（`startSingleNodeGroup`）热区窗口设 100 条、字节下界很大，提案 3000 条后，`gr.stg.FirstIndex()` 仍为 1，`gr.stg.Entries(1, last+1, 无上限)` 逐条等于提案载荷，`Term` 与 MemoryStorage 记录一致。
6. **`TestColdTier…` 热区有界**：同上设置写 3000 条后，MemoryStorage 内条目数 ≤ 2×100 + 未提交尾部，且 `raftStore` 不再持有条目（结构上删除，编译即证；另断言 `Load` 只返回热区条数）。
7. **`TestColdTier…` 重启不整份读入**：上例停组重开（`buildGroup` 干净路径），MemoryStorage 条目数 ≤ 窗口，冷读仍逐条正确。
8. **I-cold**：热区压缩函数被要求压到 > applied 时只压到 applied（或拒绝），永不越过；快照安装后 `FirstIndex()` = 快照 index +1，冷读不返回安装前的旧条目。
9. **`TestTruncatePeerGrace…`**（三节点 `startThreeNodeCluster`，`PeerGrace` 设 3s、`RetainEntries` 4、`RetainBytes` 1）：`partitionOff` 一个 follower 后写 100 条，立即 `truncateOnce` → upto 不越过其冻结 Match（宽限期内钉住）；等 3s 后再 `truncateOnce` → upto 越过其 Match，日志含宽限超期 Warn。
10. **`TestTruncateDiskPressure…`**：同上但 `PeerGrace` 10m、注入 `DiskPressure` 返回 true → 第一次 `truncateOnce` 即越过宽限期内死 peer 的冻结 Match，日志含磁盘告急放弃下界的 Warn，但 upto 仍不越过 `applied − RetainEntries`；另以表驱动方式对「下界计算」纯函数（执行者把下界计算抽成不依赖 Manager 的纯函数）断言：告急时活 peer 的 Match 同样不再构成下界。
11. **按字节下界**：单节点组小段（`SegMaxBytes` 调小）写满若干段，`RetainBytes` 设为约 3 段 → 截断后剩余段字节 ≥ 下界且被删段全部 ≤ upto。
12. **config**：两键缺省值（1GiB / 10m）、覆盖值、非法值（`-1GiB`、`1GB`、`0.5GiB`、`32MiB` 低于下限、`log_peer_grace: -1s`、`0s`）`Load` 报错且含键名。

变异验证（执行者亲手做，结果写交回物）：宽限判定改成「判死即放弃」→ 第 9 条红、E1 红；冷读改为返回 `ErrCompacted` → 第 5 条红、E1 红（退化为快照）；run 索引不处理改写（不裁旧 run）→ 第 2 条红；热区压缩点去掉 ≤applied 的夹取 → 第 8 条红；恢复 `rec.ents` 全量缓存 → 第 6 条红（按 `Load` 返回条数断言）。

### 4.3 全量编译与触及包

```
go build ./... && go vet ./internal/cluster/... ./internal/config/ ./cmd/...
go test ./internal/cluster/... ./internal/config/ ./cmd/... -count=1
go test -race ./internal/cluster/... -count=1
```
收尾 `go test ./... -count=1` 一次作证据。

### 4.4 真机清单（本机三进程 e2e，`test/e2e`；`S` = 执行者会话 scratchpad 下 `sq410/`）

基线二进制：`mkdir -p $S/base && git archive $BASE | tar -x -C $S/base && (cd $S/base && go build -o $S/sq.base ./cmd/sq)`。

新用例 `TestScenarioFollowerDownCatchesUpByLog`（`test/e2e/cluster_logretain_test.go`，用 `startProcCluster`）：
- 配置覆盖：`Cluster.LogRetainEntries=2000`（让热区窗口远小于停机期间的落后量，迫使走冷读）、`Cluster.TruncateInterval=5s`；其余缺省（宽限 10m、字节 1GiB）。
- 选一个「至少领导一个组、且在至少一个组是 follower」的节点（从 `/admin/cluster` 读各组 leader，复用 `admin_test.go` 的 `adminJSON` 及其登录方式），`stopGraceful` 后持续写入（并发 16 的 SDK 生产者），停机时长取 `SQ_E2E_DOWN`（缺省 2m）；用例自检：停机期间每个数据组的 leader commit 前进 > 2000×3（不满足即 Fatal「写入量不足，用例无判别力」）。
- `restart` 该节点，继续写 30s 后停写。
- 判据：① 重启节点 `broker.log` 中**重启之后**的「快照安装开始」行数 = 0；② 三个节点 `broker.log` 在停机至判定结束期间「快照描述符已生成」行数 = 0；③ 60s 内各组 leader 的 `/admin/cluster` 复制进度里该节点 Match ≥ 停写时 leader 的 commit；④ 至少一个 leader 的日志出现本卡新增的冷读 Info 行（证明窗口外的条目确经冷读发出）；⑤ 停写后经 SDK 消费核对：停机前后写入的消息全部可读（复用场景用例的对账器）。
- Linux 上顺带记录三进程 VmHWM（`procCluster.peaks`），macOS 跳过，只记录不判定。

| # | 命令（均在 `test/e2e` 下） | 判据 |
|---|---|---|
| E0（基线红） | `SQ_E2E_BROKER=$S/sq.base go test -tags e2e -count=1 -run TestScenarioFollowerDownCatchesUpByLog -v -timeout 20m` | **必须 FAIL**，且失败原因是判据①或②（基线下死 peer 立即被放弃、截断越过其 Match，只能拉快照）；冷读判据④在基线上因无该日志也会失败，不算判别依据 |
| **E1（首跑项）** | `go test -tags e2e -count=1 -run TestScenarioFollowerDownCatchesUpByLog -v -timeout 20m` | PASS，五条判据全满足；贴判据①②的计数、③的 Match/commit 数值、④的日志行 |
| E2 | `go test -tags e2e -count=1 -run 'TestClusterTruncationKeepsLogBounded\|TestExpandStandaloneToThreeNodes' -v -timeout 20m` | PASS（两者按 §5 加了字节/宽限注入，断言不变） |
| E3 | `go test -tags e2e -count=1 -run 'TestScenario(KillLeaderHard\|RollingRestart\|FullProcessCrashRecoversLocally\|RebootedFsyncResumesLocally)\|TestThreeNodeClusterE2E' -v -timeout 40m` | 全 PASS（重启回放走新 `Load`/播种路径） |
| E4（参考） | 在 linux-01 用交叉编译产物 `SQ_E2E_BROKER=<sq.linux>` 跑 E1，设 `SQ_E2E_DOWN=5m` | 记录结果与 VmHWM；红了带日志回报主会话 |

须存证据：每条命令末尾 PASS/FAIL 行与上述关键数值，贴交回物。

## 5. 决策权限

执行者可自定（交回物逐条列出）：run 索引/检查点/任期表的数据结构与检查点间隔（0.5–4MiB 之间）；seglog 新方法命名与签名（包内，cluster 是唯一消费者）；只读句柄的缓存策略；热区压缩的触发点落在 apply 路径的哪一处（须在 apply goroutine，不得在 raft 主循环）；Info/Warn 文案（新增行，不改既有文案）；死 peer 判死时刻表的存放位置；E1 的写入并发与 `LogRetainEntries` 取值（须保证 E0 基线红）。

既有测试允许的改动（只为保持原意图，断言不改）：
- 依赖「小保留量即截断/即快照」的用例加 `RetainBytes`=1 注入，依赖「死节点立即放弃」的用例再加 `PeerGrace` 极小值注入：`TestLaggingFollowerCatchesUpBySnapshot`（cluster_test.go:1831）、`TestTruncateOnceExcludesLearnerFromMinMatch`、`TestTruncateOnceNoopWhenNothingToDrop`、e2e `TestClusterTruncationKeepsLogBounded`（cluster_expand_test.go:165）、`TestExpandStandaloneToThreeNodes`。
- **`TestTruncateOnceRespectsLeaderMinMatch`（cluster_test.go:2069）的断言被 spec 推翻**（它断言死节点立即放弃、并依赖 hardLagCap 注释）：改为注入极小 `PeerGrace` 后断言原结论（宽限超期即放弃），其「存活探测失败」Warn 文案保留；宽限期内钉住的新行为由第 9 条覆盖。
- `raftstore_test.go` 中直接断言 `rec.ents`/`Load` 返回全量条目的用例按新 `Load` 语义适配（`TestPersistTruncatesConflictTail` 的「同进程 Persist 后 Load 读到最新」意图必须保留，改为经读路径验证）。

必须升级（停下回报主会话）：
- 第 0 步前置不满足；第 1 步任一事实不符；第 2 步基线调不红；
- 需要改盘上格式（段/帧/Pebble 键）或 raft 消息；
- 需要改白名单外文件，或改快照生成/拉取/安装流程本身；
- I-cold 无法保证（例如发现 raft 会读 > applied 的冷区条目）；
- §5 清单之外的既有测试需要改断言；
- E1 红且排查后指向设计（而非实现缺陷）；冷读成为 raft 主循环的明显阻塞（E1 期间写入 p99 较停机前恶化 >5 倍）。

## 6. 与 SQ1 / SQ2 / SQ4.4 / SQ4.5 的接触面

- **SQ1**：同改 `group.go` 的 `installSnapshot`（SQ1 在推进 applied 后唤醒门等待者，:1191 附近；本卡在同一 `applyMu` 临界区更新磁盘下界）与 apply 路径（SQ1 在 `applyOnce` 末尾唤醒；本卡在 apply 推进后判断是否压热区）。二者都是「applied 推进后追加一个动作」，互不依赖顺序；rebase 冲突限于相邻行。SQ1 的 `manager.go` 改动（`WaitWritable`、Options 一项）与 `truncateOnceWith` 不重叠，Options 结构体字段区可能文本冲突。
- **SQ2**：`proposer.go` 不碰；`group.go` 结构体字段区可能文本冲突。
- **SQ4.4**：同改 `seglog.go`。本卡读路径的锁内只取索引快照与活动段已写长度，`ReadAt` 在锁外用独立只读句柄，与 SQ4.4 的「锁外刷盘」不争同一把锁的长持有；轮转关闭写句柄不影响读句柄。
- **SQ4.5**：字节解析函数谁先落地谁建；本卡后落地则直接复用，不另写。
- **SQ4.3**：无文件重叠。

## 7. 风险

- **R1（最大）冷读返回错误内容 ⇒ follower 日志与 leader 分叉**（静默数据错乱）。来源：「后写的赢」改写、段轮转、`TruncateTo` 并发、重开重建索引四者的组合。防线：I-cold 把冷区限定在已 apply（已提交、永不改写）的条目；§4.2 第 1–4 条逐字节比对 + 改写跨段 + 重开一致性；E1 的对账判据⑤。
- **R2 冷读在 raft 主循环里同步做磁盘读**：追平期间每条 MsgApp 一次最多 1MiB 的 `ReadAt`，冷数据不在页缓存时会拖慢该组的 tick/收发。检查点让单次读是顺序小读；E1 记录追平期间写入 p99，恶化 >5 倍即升级（异步预读属后续卡）。
- **R3 宽限期内磁盘占用上升**：死 peer 钉住截断 10 分钟，期间写入量全部留盘；磁盘告急兜底（B4），文档写明估算。
- **R4 新 leader 对死 peer 的 Match 从 0 起**（F4）：宽限期内该组完全不截断，比「停机前的 Match」更保守，只多占盘不丢数据；记入文档。

## 8. 读数与证据

- R1 `sed -n 255,275p internal/cluster/manager.go`（cards/sq4）→ `260 const defaultRetainEntries uint64 = 10000`；`270 const hardLagCapFactor = 10`（注释「最坏日志大小上界钉在 retainEntries + 10×retainEntries」）；`274 const defaultTruncateInterval = 30 * time.Second`
- R2 `sed -n 1150,1245p internal/cluster/manager.go` → `truncateOnceWith`：`hardCap := hardLagCapFactor * m.retainEntries`；`if !alive(id) { m.lg.Warn("截断：peer 存活探测失败，不以其 Match 为下界", ...); continue }`；`upto -= m.retainEntries`；`SaveSnapMeta → TruncateLog → gr.mem.Compact(upto)`（:1233-1241）
- R3 `sed -n 1,12p internal/cluster/seglog/seglog.go` → 文件头「不提供随机读接口：raft 侧读日志走 MemoryStorage 双记账」；`grep -n "^func " seglog.go` → 仅 `Open/Append/maybeRotate/preallocActive/syncActive/Sync/Close/TruncateTo`；`Open` 返回 `[]*raftpb.Entry`（全量重放结果）
- R4 `sed -n 105,145p internal/cluster/raftstore.go` → `logRecovered{hs, ents}`；Persist（:256 起）每轮 `rec.ents = append(...)`，仅 `TruncateLog` 裁剪 ⇒ 截断前全部条目指针常驻
- R5 `sed -n 400,445p internal/cluster/snapshot.go` → `groupStorage` 的 `InitialState/Entries/Term/LastIndex/FirstIndex` 全部直通 `s.mem`；`Snapshot()` 用 `s.mem.Term(index)`
- R6 `grep -n "rs.Load\|mem.Compact\|mem.ApplySnapshot" internal/cluster/*.go | grep -v _test` → manager.go:588（buildGroup）、:1241；group.go:1187（installSnapshot 第 6 步）；recovery.go:302
- R7 raft v3.7.0 `log.go`：`firstIndex()` 每次调 `l.storage.FirstIndex()`、出错 panic；`term()`：`err == ErrCompacted || err == ErrUnavailable → return`，否则 `panic(err)`；`slice()` :515-520 `ErrCompacted` 返回、`ErrUnavailable` 与其它 panic
- R8 raft v3.7.0 `raft.go:618-648` `maybeSendAppend`：`r.raftLog.term(prevIndex)` 出错 → `maybeSendSnapshot`；`entries(pr.Next, r.maxMsgSize)` 出错 → `maybeSendSnapshot`（快照触发条件 = Storage 返回 ErrCompacted）
- R9 raft v3.7.0 `storage.go:268-288` `MemoryStorage.Compact`：`compactIndex <= offset → ErrCompacted`；`> lastIndex → Panicf`；每次新分配切片复制剩余条目
- R10 `grep -n "DiskWatermark" internal/config/config.go` → `85 DiskWatermarkPercent`、`191 默认 85`；`retention.go:110-133 checkDisk` 用 `sysinfo.DiskUsage(m.dataDir)` 与 `watermarkPct`；`cmd/sq/main.go:308 cluster.NewManager` 先于 `:410 writeBlocked := &atomic.Bool{}`
- R11 `grep -n "DataGroups = 3" internal/config/config.go` → `316 cc.DataGroups = 3`（默认数据组数）
- R12 `grep -n "func TestLaggingFollowerCatchesUpBySnapshot\|func TestTruncateOnce" internal/cluster/cluster_test.go` → :1831（`withRetainEntries(8)` + `partitionOff`，依赖 hardLagCap 10×8=80 < 200 条落后或探活失败）、:2069（断言死节点立即放弃、「存活探测失败」Warn）、:2124、:2162；`test/e2e/cluster_expand_test.go:165 TestClusterTruncationKeepsLogBounded`（`withRetainEntries(500)`，以锚点 Index 推进为判据）
- R13 `grep -rn "lg.Info(\"快照" internal/cluster/*.go` → group.go:1144「快照安装开始」、:1198「快照安装完成」（注释：e2e 用作快照路径证据，文案不许改）；snapshot.go:470「快照描述符已生成」；仓内无快照相关指标 ⇒ S1 以日志判定
- R14 `sed -n 15,25p internal/admin/cluster.go` → `GET /admin/cluster`：每组 leader/角色/applied，leader 组另附各 peer 复制进度
- R15 `grep -n "SegMaxBytes" internal/cluster/seglog/seglog.go` → `var SegMaxBytes int64 = 64 << 20`（包变量，测试可调小）
- R16 SQ4.5 计划 `docs/superpowers/plans/sq4.5-plan.md:28` → 字节值「非负整数 + 可选 IEC 单位，拒 KB/MB/GB、小数、负数」，「SQ4.10 的 `log_retain_bytes: 1GiB` 复用同一个，谁先落地谁建」
- R17 SQ1 计划 `.../worktrees/sq1/docs/superpowers/plans/sq1-plan.md:44-45` → 改 `group.go` 的 `dispatchReady`/`applyOnce` 末尾/`installSnapshot` 推 applied 处，`manager.go` 加 `WaitWritable` 与 Options 一项
