# seglog 段文件预分配 + fdatasync 设计

日期：2026-08-12
状态：已评审（brainstorm 定案）
前置：`2026-08-11-raftlog-segment-storage-design.md`（B2 seglog，本文只改它的**物理落盘方式**，不动帧格式、不动任何 raft 语义）
姊妹项：`2026-08-12-raft-async-storage-writes-design.md`（B 主线，攻 raft 路径流水线深度；与本文完全解耦，可任意先后落地）

## 1. 背景与目标

2026-08-12 四档归因测量（S 单机 → C1 单节点集群 → C3-cross 三机 → C3-colo
单机三进程，全新三机 2 核/1612MB/vda，5 轮交错取轮内配对）推翻了此前
「三节点慢是因为每节点扛 1 份 leader + 2 份 follower 的活」的判断：

| ack | conc | raft 机制税 S→C1 | 复制+网络税 C1→C3cross | 共置税 C3cross→C3colo |
|---|---|---|---|---|
| mem | 16 | +21.4% | +8.5% | +50.0% |
| mem | 256 | +21.6% | +5.1% | +49.7% |
| fsync | 16 | +69.2% | **−12.1%** | +52.8% |
| fsync | 256 | +44.5% | +54.8% | +51.7% |

**复制+网络税约等于零，个别档位为负**（fsync/16 三机快过单节点集群 12%）；
「三节点比单节点慢」几乎全部来自共置税（恒定 +50%，实测是纯 CPU 争抢，
C3-colo 整机 CPU 99.7%、iowait 仅 0.2%——这是部署建议，不是可修的 bug）。

同批盘微基准给出本文的直接依据：

| 维度 | 结果 |
|---|---|
| **元数据成本** | `fsync` + 增长中的文件 **1.82ms** vs `fdatasync` + 预分配文件 **0.61ms**——**3×** |
| 批量摊销（conc=1） | batch 1→64（256KB）延迟 1.89ms→2.80ms，有效吞吐 2.1→85 MB/s（40×） |
| 并行队列深度 | conc 1→8 换 4.3× 吞吐（效率 54%），单次延迟 1.89→3.13ms |

**每次 fsync 的主要成本是「文件增长引发的元数据同步」**，而不是数据本身
落盘。seglog 当前每次 `Append` 都在扩展段文件大小，于是每一次同步落盘都
额外付一次 inode 元数据日志。

**目标**：段文件一次性预分配到 `SegMaxBytes`，写入不再改变文件大小，同步
落盘从 `fsync` 降级为 `fdatasync`。预期单次同步落盘 1.82ms → 0.61ms。

**硬约束：崩溃恢复语义零变化。** 帧格式（`frame.go`）一个字节不动；
`seglog` 对外接口（`Open/Append/Sync/TruncateTo/Close`）签名与语义不动；
`raftStore` 及以上完全无感。**验收锚：`raftstore_test.go` 现有 12 个用例
零修改通过**（与 B2 立项时同一把尺子）。

**非目标**：
- 不改帧格式、不改 `SegMaxBytes` 语义、不改截断（`TruncateTo` 删整段）；
- 不碰 `raftStore` 及以上任何一层；
- 不做批量摊销——那是姊妹项 B 的地盘（且 B 的合批是 `AsyncStorageWrites`
  的副产品，不需要 seglog 配合）。

## 2. 五个改动点

### 2.1 预分配

新建段时（`Open` 创建首段、`maybeRotate` 开新段）对新 fd 做
`fallocate(fd, 0, 0, SegMaxBytes)`。

**必须用 mode 0（真扩 size），不能用 `FALLOC_FL_KEEP_SIZE`**：后者保持
`st_size` 不变，写入时文件大小照样增长，元数据日志照付，本方案收益归零。

### 2.2 去掉 `O_APPEND`，改显式偏移写

当前两处开段都用 `os.O_WRONLY|os.O_APPEND|os.O_CREATE`
（`seglog.go:264`、`seglog.go:440`）。`O_APPEND` 语义是「每次写都定位到
EOF」——预分配后 EOF 就是 `SegMaxBytes`，第一次 `Append` 会直接写到段尾。

改为：`Log` 维护 `logicalSize`（当前有效字节数），写入走
`WriteAt(buf, logicalSize)`，成功后 `logicalSize += n`。

### 2.3 `activeSize` 的来源改为扫描逻辑末尾

`Open` 目前用 `activeSize: fi.Size()`（`seglog.go:295`）。预分配后
`fi.Size()` 一开就是 `SegMaxBytes`，重启即触发轮转——**这是本方案唯一
会静默破坏轮转的点，必须一并改**。

改为取扫描过程已经算出的活动段逻辑末尾偏移（`off`）。轮转判定
（`activeSize >= SegMaxBytes`）语义因此保持「已写入的有效字节数」不变。

### 2.4 轮转关段前 `Truncate(logicalSize)`

已关闭段绝不能带预分配零尾：非末段遇到坏帧走的是「真损坏，拒绝启动」
（`seglog.go:198`、`seglog.go:237`）。`maybeRotate` 在 `Close` 前先
`Truncate(logicalSize)`，让已关闭段的物理大小等于逻辑大小——这也让
`TruncateTo` 删整段、`segMax` 记账、磁盘占用统计全部维持原样。

### 2.5 `Sync()` → `Fdatasync()`

预分配 + 定长写入后文件大小不再变化，`fdatasync` 保证数据落盘即足够。
三处同步点（`Append` 的 `sync=true`、`maybeRotate` 关段前、`Log.Sync`）
统一改走 `fdatasync`。

## 3. 恢复语义：零尾与 torn tail 必须分开记

预分配的零尾会被扫描判为 torn tail：`readFrame` 拒绝 `ln == 0`
（`frame.go:63`），扫描随即**物理截断**到好帧边界（`seglog.go:183`）。

**语义上这完全正确**——不丢数据，而且 `os.Truncate` 的落点正好就是
§2.3 需要的逻辑末尾，两件事天然对齐。

**但操作性上是个真问题**：现状会让**每一次干净重启都打一条
`Warn "检测到 torn tail"`**，运维看到会以为掉过电。日志一旦开始说谎，
`tail_logs`/`search_logs` 在真事故里就不可信了。

因此扫描到末段坏帧时，增加一次判别：

- **剩余字节全零** ⇒ 记 `Debug "预分配零尾，截断到逻辑末尾"`（正常形态）
- **剩余字节非全零** ⇒ 保留现有 `Warn "检测到 torn tail，已截断到好帧边界"`

判别只在末段、只在遇到坏帧时做一次，不进热路径。

`Open` 完成后，把活动段从 `logicalSize` 补分配回 `SegMaxBytes`（复用
§2.1 的同一个调用），使重启后的段重新获得预分配收益。

## 4. 平台边界

`fallocate` 与 `fdatasync` 都是 Linux 特有。开发与 `-race` 全量测试都在
macOS 上跑，**这条是硬约束，不是兼容性锦上添花**。

按 build tag 分文件：

| 文件 | 平台 | 预分配 | 同步 |
|---|---|---|---|
| `seglog_linux.go` | linux | `unix.Fallocate(fd, 0, 0, size)` | `unix.Fdatasync(fd)` |
| `seglog_other.go` | 其他 | no-op（返回 `false` 表示未预分配） | `File.Sync()` |

**两条路径必须走同一套帧格式、同一套扫描恢复逻辑、同一套偏移写入**——
差异面收敛到「有没有预分配」和「同步系统调用是哪个」两点。否则 macOS 上
跑绿的测试证明不了 Linux 上的正确性。

`seglog_other.go` 上不预分配 ⇒ 不产生零尾 ⇒ §3 的判别自然走不到，行为与
今天完全一致。§2.2 的偏移写入两平台都改（`WriteAt` 在未预分配文件上
等价于顺序追加），避免两套写路径。

## 5. 代价与风险

**磁盘占用**：每组一个 `SegMaxBytes`（64MiB）预分配活动段。默认
`data_groups: 3` ⇒ 192MiB；上限 `data_groups: 64` ⇒ 4GiB。40G 盘可忽略，
但必须写进部署文档的容量估算一节。

**收益依赖磁盘特性**（B2 与 sharedlog 两次翻车的共同教训）：3× 是本批
vda 上的实测。换盘后收益幅度会变，但**方向不会反转**——「不改文件大小
就不用同步元数据」是文件系统的结构性事实，不是这块盘的性格。

**不可比的绝对数字**：本文引用的四档吞吐**绝对值不可引用**（该批机器第
3→4 轮出现台阶式提速 +31%，跨轮离散度 15-50%），只有轮内配对税率与盘
微基准可信。验收时必须**同批机器重跑 seglog 基线做对照**。

## 6. 验收标准

1. `raftstore_test.go` 现有 12 个用例**零修改**通过（语义零变化的锚）。
2. `seglog` 包新增用例覆盖：预分配后重启轮转判定正确（§2.3 的回归锚）、
   零尾走 Debug 而非 Warn（§3）、已关闭段无零尾（§2.4）、torn tail 仍被
   正确识别与截断（§3 的非全零分支）。
3. macOS `-race` 全量绿（`seglog_other.go` 路径）。
4. Linux 三机实测：同批机器上 seglog 基线 vs 本方案，quorum-fsync 档
   conc=16/256 各跑 ≥3 轮交错，报中位数 + 离散度。预期 fsync 档吞吐提升，
   mem 档不劣化（mem 档不走同步落盘，应无差异——若 mem 档出现劣化，说明
   §2.2 的偏移写引入了额外成本，需回查）。
