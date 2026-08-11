# raft 日志 segment 存储设计（B2）

日期：2026-08-11
状态：已评审（brainstorm 定案）
前置：`2026-08-08-sq-v2-replication-design.md`（V2 复制设计，本文只替换其 §5 中 raft 日志的物理存储，不动任何语义）

## 1. 背景与目标

三机压测（2 核 ×3，同 VPC）+ pprof 归因：quorum-mem 档三台 broker 各有
~48% CPU 烧在 Pebble compaction/flush。根因是消息体每节点进两次 LSM——
raft 日志条目（`raft/<g>/ent/*`，写入 30s 后被周期截断 range-delete，
tombstone 在 compaction 里反复搬运）+ FSM apply（真正要留的数据）。

**目标**：raft 日志条目退出 LSM，改存每组独立的分段追加文件（segment
log）。日志字节从此不进 memtable/L0/不参与 compaction，截断从
range-delete 变成删整段文件——compaction churn 中日志贡献的部分结构性
归零。

**硬约束：语义零变化。** `raftStore` 对外接口（`Persist/Load/
TruncateLog/SaveConfState/ResetGroupProgress/MarkInstalling/...`）签名与
语义一个不动，上层 `group`/`manager`/恢复判定表（recovery.go）零改动。
现状 mem 档「NoSync 进页缓存、进程 crash 不丢、断电丢 ≤200ms」的持久性
等位保持——这是本方案（B2）相对「日志内存化」（方案 A）的核心卖点：
`pathLocalResume` 秒级本地自愈原样保住。

**非目标**：
- 不做日志内存化（方案 A）——B2 落地复测后用 pprof 归因数据（双写
  成本 vs churn 成本拆开）再评估 A 是否还有必要，另行立项；
- 不做 log-is-database（方案 C）——独立里程碑；
- 不改确认档语义、不改快照、不改截断位点计算。

## 2. 评审中已定的三个分叉

| 分叉 | 决定 | 理由 |
|---|---|---|
| 总路线 | B2 先行，A 挂起 | B2 风险最低、恢复路径零语义变化；A 会把进程 crash 降格为断电级事件（两节点同 crash 从自动恢复变成永久卡死等签字），且恢复路径重设计的评审成本按最高档预算（该路径已修过 C1/B10/B11 三个深坑） |
| HardState 位置 | 与条目同进 segment log | etcd WAL 经典设计：一轮 Ready 的条目+HardState 顺序写入后单次 fsync 即全 durable，quorum-fsync 档每轮仍是单次刷盘与现状持平；留 Pebble 则 MustSync 轮要刷两次盘且跨存储顺序需额外论证 |
| 实现来源 / 文件布局 | 自研；每组独立日志 | 需求面窄（追加、CRC 帧、轮转、截尾、删段），几百行可控代码；当初选 etcd/raft 薄壳的理由就是「日志存储自主」（V2 spec §2.1）。每组独立 = 零共享状态、截断独立；代价是 fsync 档多组并发时失去单 WAL group commit 摊销（不同文件可并行重叠，且用户形态 ≤3 个数据组，影响有限） |

## 3. 存储职责划分

新组件 `seglog`：每组一个追加日志，只被 `raftStore` 内部使用。

| 数据 | 现状 | 变更后 |
|---|---|---|
| 日志条目 | Pebble `raft/<g>/ent/*` | segment 文件 `<data_dir>/raftlog/<g>/NNNNNNNN.seg` |
| HardState | Pebble `raft/<g>/hs` | segment 文件（记录类型之一） |
| applied 位点 | Pebble（与 FSM 批原子） | 不动——spec §5 的原子性承诺 |
| 锚点/成员表/安装中标记/干净关机标记/机器世代 | Pebble | 不动（低频小键，无 churn） |

运行期读路径不变的前提（已勘察确认）：raft 库读日志走 MemoryStorage
双记账（group.go 的 `mem.Append`），快照生成只读 FSM ReadView + 内存
term（snapshot.go 的 `groupStorage.Snapshot`）——segment 文件运行期
**永远不被随机读**，只有追加、fsync、启动扫描、删段四个操作，因此
**不需要任何索引结构**。

## 4. 段格式与轮转

- **记录帧**：`[4B len][4B CRC32C][1B type][payload]`，type ∈ {entry,
  hardstate}；payload 分别是 Entry / HardState 的 protobuf 原字节
  （Entry 自带 Index/Term，不外挂）。
- **段文件**：按创建序号命名（`%08d.seg`），写满 64MiB 轮转（常量起步，
  不进配置）。轮转屏障：旧段 close + fsync 成功后才创建新段；**新段
  首条记录固定补写一份当前最新 HardState**——保证最新 HS 永远在最新
  段里，截断删旧段不可能删掉唯一的 HS。
- **raft 冲突回退**（换届后重写日志尾）：不做物理删除，直接追加新条目；
  启动扫描按「后写的赢」重放——遇到 index ≤ 当前尾的条目就逻辑截尾再
  接上（与 MemoryStorage.Append 冲突语义一致，etcd WAL 同款）。已提交
  条目不会被回退（raft 不变量），段回收不受影响。
- **段元数据**：内存里记 `{段号 → 段内最大 entry index}`（启动扫描重建、
  轮转时更新），截断判定用；不落盘、无 manifest。

## 5. 写路径

`Persist(g, hs, ents, sync)` 新实现（调用方 handleReady 零改动）：

1. 非空 HardState → 追加 hardstate 记录；
2. 逐条追加 entry 记录；
3. 每轮结束把用户态缓冲 `write()` 进内核——mem 档到此为止（页缓存持久
   性与现状 Pebble NoSync 等位）；
4. `sync=true`（fsync 档 MustSync 轮）→ fsync 该组 active 段，每轮单次
   刷盘与现状持平。

**mem 档 200ms flusher** 改两步、顺序固定：先逐组 fsync active 段，再
`store.SyncWAL()`（FSM）。日志先于 FSM 落盘是安全方向——崩溃窗口只会
出现「日志超前 FSM」（重放即补），不会出现「FSM 声称 applied=N 而日志
不认识 N」（那需要锚点引导，是方案 A 才有的负担）。

其余写点：`SaveConfState`（Pebble Sync）不动，顺序不变量成立——
ConfChange 条目先经 Persist 落段，SaveConfState 后写，日志恒先行。
grant 路径 `bumpTermsInto`（抬 term）改为向段追加 hardstate 记录 +
fsync，机制同构。

成本对比（mem 档稳态每轮 Ready）：一次 Pebble 批次提交（WAL 编码 +
memtable 写入 + 提交流水线）→ 一次 `write()` 系统调用。

## 6. 启动恢复与迁移

**`Load(g)`**：按段号顺序扫描 `raftlog/<g>/`：

- 逐记录校验 len/CRC；entry 按「后写的赢」重放出连续序列，hardstate 取
  最后一条；
- 尾部损坏只允许出现在最后一段（轮转屏障保证）：末段坏帧 → 从坏帧处
  物理截断文件、继续启动（torn write 是正常掉电形态）；**非末段坏帧**
  → 真损坏，fail-stop 拒启，走既有不干净恢复路径（重同步兜底），绝不
  静默跳过；
- 锚点照旧从 Pebble 读，buildGroup 三分叉逻辑不动。

**一次性迁移**（滚动升级的集群不能清盘）：启动时发现 Pebble 里还有
`raft/<g>/ent/*` → 全读按序写进 segment（先补一条旧 `raft/<g>/hs` 的
hardstate 记录，再逐条 entry）+ fsync → 单批 Sync 删旧键族
（ent + hs）→ 继续正常 Load。幂等锚：以「Pebble 旧键是否还在」为完成
判定——中途崩溃则旧键还在，重启先清掉半截 `raftlog/<g>/` 再重迁。
迁移代码走完本版本周期后可删。

**按组清空**：`ResetGroupProgress` 追加删除该组 segment 目录；
`WipeForRejoin` 清整 data_dir，`raftlog/` 在其下天然覆盖。
`diskHasRaftState` 判定改为「raftlog 目录非空 或 Pebble applied 非零」。

## 7. 截断与回收

`truncateOnce` 位点计算（四重下界）原样保留，仅执行段变化：

- `TruncateLog(g, upto)` → 删除所有「段内最大 entry index ≤ upto 且已
  轮转关闭」的段，active 段永不删；
- 锚点先行不变量保持：`SaveSnapMeta`（Pebble Sync）成功后才删段；
- `mem.Compact` / `reportStalledSnapshots` / `snaps.GCOnce` 不动。

行为差异点名：回收粒度是整段（64MiB），比按条 range-delete 粗——日志
盘上驻留最坏多一个段的余量（64MiB/组），换零 tombstone，可接受。

## 8. 错误处理

- 追加/fsync/轮转失败 = 日志分叉风险，与现状 Persist 失败同级：
  fail-stop panic，进程死亡由上层重启接管（走不干净判定）。
- 扫描期损坏：末段 torn tail 截断续跑；非末段坏帧拒启。两条路径都打足
  上下文（段号、偏移、CRC 期望/实际）——掉电尸检的唯一现场。
- 迁移中途崩溃：幂等重迁，无新状态形态。

## 9. 测试策略

1. **seglog 单元测试**（TDD）：帧 roundtrip、torn tail 截断（写坏尾字节
   模拟掉电）、非末段损坏拒启、轮转 + 新段首条 HS 补写、冲突回退重放、
   按段回收（active 不删、锚点先行）、迁移幂等（半截崩溃重放）。
2. **raftstore_test.go 现有全套零修改通过**——接口不动的可执行证明。
   个别直接断言 Pebble 键形态的用例改为断言接口行为，逐条列进 plan。
3. **恢复路径全量回归**：manager_test / cluster_test / recovery 判定 +
   B10/B11 系列 e2e，双平台（含 4 核 Linux——历史教训：两处用例缺陷只
   在 Linux 上现形）。
4. **性能验证**：三机基准复测两档 + pprof 归因对比（compaction 48% →
   ?）。该数据同时回答「方案 A 还要不要做」。

## 10. 观测性

段轮转（段号、大小）、截断回收（删段清单、释放字节）、启动扫描（段数、
条目数、恢复 HS、torn tail 位置）、迁移（条数、耗时）各打 Info/Warn
日志；fsync 耗时指标钩子留到实测需要时再加。

## 11. 后续决策点（不在本 spec 范围）

- B2 落地复测后，若 compaction 占比仍高（双写的磁盘带宽成本是剩余
  大头），再评估方案 A（mem 档日志内存化）——彼时自有 seglog 层也是
  A 的地基；
- 方案 C（log-is-database）独立立项。
