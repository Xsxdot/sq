# 不干净关机的本地自恢复（B11）设计

日期：2026-08-10
状态：已评审通过（brainstorm 定稿）
关联：[V2 复制层设计](2026-08-08-sq-v2-replication-design.md)（§2.2 刷盘档位、§5 持久性、§8 测试）、backlog **B11**（本 spec 的来源）、backlog **B10**（wipe 顺序 P0，本 spec 保留其红线断言并为它找到真正对应的分支）

---

## 1. 目标与非目标

**目标**：让不干净关机的节点在**可以证明本地日志完好**时直接以原身份从本地日志恢复，而不是无条件清空数据目录去求集群接纳。核心收益是消除 B10 遗留的那半边——全集群同时硬宕后三节点互相等待、谁都起不来，而三份完好的数据就躺在盘上。

**非目标**：

| 非目标 | 理由 |
|---|---|
| 让 mem 档在真掉电后**自动**本地恢复 | 掉电可能丢掉投票记录与日志尾，raft 安全性无法保证。这种恢复必然带数据损失风险，必须由人签字，不能由进程替人做主 |
| 「多数派中日志最长者当选」的引导协议 | backlog B11 原始设想。本 spec 不采用：raft 的选举本来就保证「日志最新者当选」，另造一套引导协议是重复造轮子，且它解决不了「所有节点都丢了同一段」这个真正的损失场景 |
| 单机档（非集群）的恢复语义 | 单机档不走 `cluster.Manager`，`fsync: sync\|async` 是它自己的开关，本 spec 一个字不动它 |
| 跨节点协调的恢复流程 | 本地恢复纯本地决策。节点起来之后的日志对账由 raft 自己完成，不需要任何新的协议面 |

---

## 2. 问题：今天为什么起不来

### 2.1 现状

`NewManager` 的恢复判定是三分支（[manager.go:388](../../internal/cluster/manager.go)）：有干净关机标记 → 回放本地日志原身份回归；无标记但盘上有 raft 状态 → 返回 `ErrUncleanShutdown`；无标记且无状态 → 全新引导。

`ErrUncleanShutdown` 之后，`main` 走 `cluster.Rejoin`：向存活 leader 求得接纳（`PrepareJoin`），拿到接纳后清空数据目录，再以 learner 重入（B10 修复后的顺序）。求不到接纳时数据保持原样、进程拒启。

于是全集群同时硬宕 = 三个节点都在找一个不存在的 leader = 三个都拒启 = **集群永久需要人工介入，尽管数据完好无损**。这是 batch⑤ 场景四实测出来的行为，当时用 `TestScenarioFullPowerLossCannotRecover` 固化了它。

### 2.2 判据错了：分界线不是档位，是「进程死了」还是「机器掉电了」

backlog B11 原文说这件事「只在 fsync 档下成立」。这个判断不准确。真正决定本地日志是否可信的，是**页缓存有没有丢**：

- `syncPersist()` 只在 `mode == AckQuorumFsync && rd.MustSync` 时刷盘（[group.go:926](../../internal/cluster/group.go)）。mem 档从不 fsync。
- 但 Pebble 的 `NoSync` 并不是「没写」，它走 `write(2)` 进 OS 页缓存，只是不 fsync。
- 所以 **kill -9 / OOM / panic / 快照 fail-stop 之后，本地日志是完整的**——进程没了，页缓存还在，重开 store 一条不少。真正会丢尾巴的只有机器掉电、内核崩溃、硬重启。
- 而 e2e 里的「断电模拟」用的是 SIGKILL。也就是说：`TestScenarioFullPowerLossCannotRecover` 固化下来的那个「集群起不来」，实际发生在**三份日志毫发无伤**的前提下。

### 2.3 顺带查出的两处口径不符

**(a) mem 档承诺的后台批量 fsync 不存在。** spec §2.2、[group.go:341](../../internal/cluster/group.go) 的注释、`sq.example.yaml:61` 三处都写着「NoSync 落盘 + Manager 层后台周期批量 fsync 兜底」。全仓搜索确认：生产代码里没有任何 `SyncWAL`/`Flush()`/周期刷盘循环，`AckQuorumMem` 这个常量在非测试代码中零引用。mem 档从启动到关机一次主动 fsync 都不发，数据何时真落盘完全交给 Pebble 的 memtable flush 与内核脏页回写——**不可控、无上界**。

后果：mem 档掉电丢的不是「最后几十毫秒」，而是「上一次 memtable flush 以来的全部」。给 mem 档设计的人工恢复出口，其可信度直接取决于这个数，所以本 spec 把补上这个兜底纳入范围（§3.4）。

**(b) 好消息：mem 档的优雅停机本来就是安全的。** `MarkCleanShutdown` 用 `ApplyWith(b, true)` 写标记，且契约要求它是退出前最后一次同步写（[raftstore.go:511](../../internal/cluster/raftstore.go)）。Pebble 的 Sync 提交会把 WAL 中它之前的全部 NoSync 写一并 fsync。所以 mem 档的暴露面**只有**不干净关机，正常停机一条不丢。这把本 spec 的范围收窄了一圈。

---

## 3. 关键决策记录

### 3.1 判据：机器世代（boot generation）

启动时把当前机器的 boot generation 以 **Sync** 写入 `raft/bootgen`；恢复时把盘上的值与当前值比对。

**相等 ⇒ 这台机器从那次写入至今没有重启过 ⇒ 页缓存从未丢失 ⇒ 本地日志完整。** 这是可验证的事实，不是启发式。

平台实现：

| 平台 | 来源 | 说明 |
|---|---|---|
| Linux | `/proc/sys/kernel/random/boot_id` | 标准内核接口，每次启动重新生成。已在 6.17.0-29-generic 上实测可读 |
| darwin | `unix.SysctlTimeval("kern.boottime")` | `golang.org/x/sys` 已是本项目的间接依赖，不引入新模块 |
| 其它 | 读不到即视为「变了」 | 保守方向：宁可多走一次重入编排，不可误判成「日志可信」 |

**容器语义正好是我们要的**：容器内读到的是宿主机内核的 boot_id。容器重启而宿主没重启 → 世代不变 → 判定「页缓存完好」，正确；容器迁移到另一台宿主 → 世代变了 → 保守处理，也正确。

**写入时机是个陷阱，必须写死**：`raft/bootgen` 只在**决定走一条能启动成功的路径之后**写入（`clean-resume` / `fresh` / `local-resume` / `local-forced`），带 Sync；**`ErrUncleanShutdown` 分支绝不写**。

理由：这个值的语义是「本数据目录最后一次被运行中的节点写入，发生在哪个机器世代」。若在 `ErrUncleanShutdown` 分支也写，则序列变成——机器重启 → 判定要签字 → 顺手写了新世代 → Rejoin 失败拒启 → 运维直接重启进程 → 此时盘上世代 == 当前世代 → **自动走 `local-resume`，签字门被静默绕过**。这条错误顺序会让整个安全门形同虚设，实现与代码注释都必须显式点明。

写入失败即启动失败：判据不可靠时不能带着一个假的「世代未变」继续跑。

### 3.2 恢复判定：三分支扩为五分支

| 盘上状态 | ack 档 | 机器世代 | 许可 | 路径 | 行为 |
|---|---|---|---|---|---|
| 有干净关机标记 | 任意 | 任意 | — | `clean-resume` | 现状不变：回放本地日志，`RestartNode` 原身份回归 |
| 无标记、无 raft 状态 | 任意 | 任意 | — | `fresh` | 现状不变：按 `Peers`/`BootstrapVoters` 引导 |
| 无标记、有 raft 状态 | 任意 | **未变** | — | **`local-resume`** | 新增。等同 `clean-resume`：回放本地日志，原身份 `RestartNode` |
| 无标记、有 raft 状态 | **fsync** | 变了 | — | **`local-resume`** | 新增。同上 |
| 无标记、有 raft 状态 | mem | 变了 | **有** | **`local-forced`** | 新增。消费许可 + 抬 term（§3.3）后回放本地日志 |
| 无标记、有 raft 状态 | mem | 变了 | 无 | `ErrUncleanShutdown` | 现状不变：`main` 走 `Rejoin` 重入编排；编排失败则拒启且数据完好（B10） |

**为什么 fsync 档即使重启过也能直接本地恢复。** `syncPersist` 跟随 `raft.MustSync`，而 MustSync 在「有新条目」或「term/vote 变更」时均为真。raft 安全性依赖的两样东西——日志条目与投票记录——每次变更都已落盘。掉电只可能丢失 commit index 的推进（纯 commit 轮 MustSync 为假），而 commit index 丢失完全无害：raft 会从 leader 重新学到。不存在「已确认的东西被忘记」的可能，所以不需要任何人签字。

**为什么最后一行保留自动 Rejoin 而不是直接拒启。** mem 档掉电但多数派仍存活时，「清空 + 以 learner 从 leader 拉一份干净副本」本来就是最优解——强于带着残缺日志硬起来。只有当它**也失败**（没有任何节点能接纳我 = 全集群都倒了），才落到 B10 的「拒启且数据完好」。**签字出口因此定位极清晰：它只服务于「没有任何人能帮我」这一种局面。**

### 3.3 `local-forced` 必须抬 term

mem 档掉电可能丢失投票记录：本节点在任期 T 投给 A，忘记，重启后又在 T 投给 B → 同一任期两个 leader → 日志分叉。这比丢数据严重，是**损坏**。

解法：`local-forced` 路径在回放之前，把每个组持久化 HardState 的 `Term` 加 1、`Vote` 清空，Sync 落盘。抬任期在 raft 中永远安全（只是强制一次重新选举），抬完之后本节点不可能再在 T 投第二次。

**另外三条恢复路径都不抬**：它们本来就没丢过投票，抬了纯属白白多付一次选举。

### 3.3b 两条新路径复用 `buildGroup(clean=true)`，半截快照安装的处理白送

`local-resume` 与 `local-forced` 都走现有的干净回放路径（`buildGroup` 的 `clean` 分支），因此**自动继承**其中已有的「未完成快照安装」处理（[manager.go:579](../../internal/cluster/manager.go)）：`raft/<g>/snapinstall` 标记存在时清空该组键族 + `ResetGroupProgress` + 换全新空存储启动，让 leader 重发快照或全量重放。

这一条消除了一个本来会致命的洞——不干净关机若恰好发生在快照安装中途，本地恢复会带着半截状态启动、向客户端返回缺失的消息。由于该标记是 `MarkInstalling` 用 Sync 写的，它在 mem 档掉电后同样存在，`local-forced` 也受此保护。**实现时不得为两条新路径另写一套回放逻辑**，否则这份保护会丢。

### 3.4 补上 mem 档的周期 fsync

新增 `store.SyncWAL()`：`db.LogData(nil, pebble.Sync)`——向 WAL 写一条空记录并 fsync，副作用是它之前所有 NoSync 写全部落盘。与 `ApplyWith(b, true)` 是同一机制，只是载荷为空；**不触发 memtable flush**（那是 `Flush()`，重得多），只付一次 fsync。放在 store 层是 B2 铁律的要求：集群层不得持有裸 `*pebble.DB`。

`Manager` 起 `syncLoop`，**仅在 mem 档启动**（fsync 档每轮 Ready 已刷，重复无益），随 `doneCh` 收尾。节奏是「**上一轮完成后再等 interval**」而非固定 tick——慢盘上单次 fsync 可能超过整个 interval（实测存在 579 msg/s 的盘），固定 tick 会让请求堆积成雪崩。

**不改动 ack 语义**：mem 档仍是「多数派内存确认即返回」，客户端不等 fsync。syncLoop 是独立 goroutine，写路径零阻塞。代价不是「每条消息一次 fsync」而是**恒定每秒 2 次**，与吞吐无关。

新增配置 `cluster.sync_interval`，默认 `500ms`，`0` 表示禁用。保留 `0` 是给「只要吞吐、明确不要持久性」的部署一条逃生门——但它必须是显式写下的选择，而不是像今天这样是个没人知道的隐含默认。

### 3.5 签字出口：一次性许可，绑定机器世代

命令形态：

```
sq recover -config /etc/sq/sq.yaml            # 只报告，不写任何东西
sq recover -config /etc/sq/sq.yaml --grant    # 写入一次性许可
```

**为什么用 `-config` 而非 `-data-dir`**：ack 档与 `sync_interval` 都在配置里、报告要用；且少一次手输错路径的机会——签错目录的代价是清掉一份好数据。

**报告内容**分两块。判定块：盘上世代 vs 当前世代、ack 档、`sync_interval`，归结为一句结论（例：「本次不干净关机发生在机器重启之后，mem 档下最多可能丢失最后 500ms 的写入」）。现场块：每组一行的 applied index、日志尾 index/term、快照锚点、成员表——这是运维判断「值不值得签」的原始素材。

**许可的存储与作废**：键 `raft/local_recover_permit`，值是两行 UTF-8 文本——第一行授予时间（RFC3339），第二行授予时的机器世代。用纯文本而非 protobuf：这个值运维可能要用工具直接看，可读性比编码效率重要得多，而它一生只被写一次读一次。消费条件是**当前世代必须等于第二行的世代**，消费即删除（与抬 term 同批，Sync）。

这一条堵死了「半年前签过字、今天又掉一次电、被静默放行」：每次机器重启都是新的世代，旧许可自动失效。不需要 TTL，因而不依赖时钟。**签字只对运维当时看到的那一次事故有效。**

**白送的互斥**：该命令需要打开 store，而 Pebble 是独占锁——broker 运行中 `Open` 直接失败。「服务运行中误签」这条路物理上走不通，只需在报错信息里说清楚。

**不需要签字时明说**：世代未变、或运行在 fsync 档时，`sq recover` 直接告知「本节点不需要许可，直接启动即可自动本地恢复」，而不是闷头写一个永远不会被消费的许可。

**逐台签字**：不做「一台签字、全集群放行」。每个节点丢失的尾巴长度不同，运维应逐台看到各自的代价；且真到全集群硬宕，人本来就要逐台去开机。

**留痕规格对齐 wipe**：授予与消费均打 **Error 级**日志；消费时把整份报告重打进 broker 日志——事后审计只看 broker 日志即可，无需追查谁在哪台机器上敲了什么。

**不破坏现有部署**：`main` 现用 `flag` 只认 `-config`。子命令分流的做法是判断 `os.Args[1] == "recover"`，否则原样走今天的路径。`sq -config x.yaml` 一字不改，systemd 单元不受影响。

---

## 4. 组件与文件结构

| 文件 | 状态 | 职责 |
|---|---|---|
| `internal/cluster/bootgen.go` | 新建 | 机器世代的读取（平台分派）与可注入的 provider；`SQ_BOOTGEN_OVERRIDE` 测试覆盖及其 Error 级告警 |
| `internal/cluster/bootgen_linux.go` / `_darwin.go` / `_other.go` | 新建 | 按 build tag 分派的三份实现 |
| `internal/cluster/recovery.go` | 新建 | 五分支恢复判定的决策函数（纯函数，输入：clean/hasRaft/mode/世代是否变/许可是否有效，输出：路径枚举 + 理由串），供 `NewManager` 与 `sq recover` 共用同一套判据 |
| `internal/cluster/manager.go` | 修改 | `NewManager` 调用 `recovery.go` 的决策；新增 `syncLoop`；`local-forced` 的抬 term |
| `internal/cluster/raftstore.go` | 修改 | `raft/bootgen` 与 `raft/local_recover_permit` 的读写消费；`BumpTerm(g)` |
| `internal/store/store.go` | 修改 | 新增 `SyncWAL()` |
| `internal/config/config.go` | 修改 | 新增 `cluster.sync_interval`，校验与默认值 |
| `cmd/sq/recover.go` | 新建 | `sq recover` 子命令：报告渲染与许可授予 |
| `cmd/sq/main.go` | 修改 | 子命令分流；头注释更新（现有注释仍写着 B10 之前的哲学「拒启等人工介入违背高可用初衷」，已不成立） |

把决策抽成 `recovery.go` 的纯函数是刻意的：`NewManager` 和 `sq recover` 必须给出**完全一致**的判断，否则会出现「命令说你不用签字、进程说你要签字」这种最伤运维信任的分歧。共用一个函数是唯一可靠的保证方式，同时它也让五条分支可以脱离 raft 与磁盘直接单测。

---

## 5. 键布局新增

沿用 `raft/` 前缀（见 raftstore.go 头注释的键布局表）：

```
raft/bootgen                 → 机器世代字符串（仅在能启动成功的路径上 Sync 写入，见 §3.1）
raft/local_recover_permit    → 一次性本地恢复许可（两行 UTF-8：授予时间 RFC3339 / 授予时世代）
```

两者都在 `raft/` 前缀内，因此 `WipeForRejoin` 的整目录清空、`storeHasFSMKeys` 的 FSM 区扫描都不受影响。

---

## 6. 错误处理与失败模式

| 情形 | 行为 |
|---|---|
| 机器世代读不到（不支持的平台、`/proc` 不可读） | 视为「变了」，走保守分支。启动日志 Warn 级说明原因 |
| `raft/bootgen` 写入失败 | 启动失败。判据不可靠时不得带着假的「世代未变」继续跑 |
| 许可解析失败 / 世代不匹配 | 视为无许可，走 `ErrUncleanShutdown`。Error 级日志写明不匹配的两个世代值 |
| 抬 term 写入失败 | 启动失败，许可**不消费**（消费与抬 term 同批，同生共死），运维重试即可 |
| `SyncWAL` 单次失败 | Error 日志 + 连续失败计数；不中断进程（一次 fsync 失败不等于盘死了，但连续失败必须在日志里显眼） |
| `sq recover` 时 broker 仍在运行 | Pebble 独占锁使 `Open` 失败，报错信息明写「请先停止本节点的 broker 进程」 |

---

## 7. 可观测性

遵循 `instrumenting-code`：关键节点、错误分支、成功路径都要能在日志里看见。

- `NewManager` 恢复判定：Info 级单行输出全部判据（`recovery` 路径名、`bootgenStored`、`bootgenNow`、`mode`、`permit`），使得「为什么走了这条路」永远不需要猜。
- `local-forced`：Error 级，含抬 term 前后的值与许可的授予时间戳。
- `local-resume`：Info 级，含各组回放到的 applied 与日志尾。
- syncLoop：每轮 Debug（含耗时），失败 Error 并带连续失败计数。
- `SQ_BOOTGEN_OVERRIDE` 生效：Error 级，明写「机器世代被环境变量覆盖，仅供测试」。

---

## 8. 测试策略

**会被推翻的既有用例**：`TestScenarioFullPowerLossCannotRecover` 断言「三节点同时 SIGKILL 后全部拒启」。SIGKILL 不重启机器、世代不变，改动后三节点会各自本地恢复并重新选主。该用例改名为 `TestScenarioFullProcessCrashRecoversLocally`，断言三节点自动起来、集群恢复可写、确认集零丢失。

**B10 的红线断言一条不丢**，搬到新用例：注入「世代变了」+ mem 档 + 无许可 → 断言拒启、数据目录非空、日志中出现「数据目录保持原样未清空」且**不**出现「状态目录已清空」。顺序倒置的口子照样会在这里红，只是搬到了它现在真正对应的分支上。

**如何在测试中模拟机器重启**：世代读取做成可注入——Go 单测经 `Options` 上的 provider；进程级 e2e 只能经环境变量 `SQ_BOOTGEN_OVERRIDE`（真 broker 进程注不进 Go 函数）。该变量的误用风险由 §7 的 Error 级告警缓解，不能只靠文档。

**单测**：五条恢复分支各一条；许可的世代绑定（世代 X 授予、世代 Y 消费必须被拒）；`local-forced` 后 HardState.Term 确实 +1 且 Vote 清空；`SyncWAL` 的落盘语义；syncLoop 在慢盘下不堆积。

另有一条**守安全门的用例**，单独点名因为它守的是 §3.1 那个会让整扇门失效的错误顺序：走完 `ErrUncleanShutdown` 分支之后，断言盘上 `raft/bootgen` **仍是旧值**。若哪天有人把写入挪到判定之前，本用例会红——否则这个缺陷在功能测试里完全看不出来，只会在某次真实事故中表现为「签字门自己开了」。

**e2e**：①三节点全 SIGKILL → 自动本地恢复零丢失（替换旧用例）②mem + 世代变 + 无许可 → 拒启且数据保留 ③mem + 世代变 + 签字 → 起得来、term 抬了、数据还在 ④fsync 档 + 世代变 → 无需签字直接起来。

**基准**：mem 档加周期 fsync 后的吞吐代价，用现成的 `BenchmarkProposeQuorumMem` 与跨机三档实测回填。预期基本无损（恒定 2 次/秒），**但预期不算数据**，验收必须带实测数字。

---

## 9. 文档与口径同步

本 spec 落地时必须同步修正的三处不实描述（§2.3(a)）：

- V2 spec §2.2 与 §5 中「后台周期批量 fsync 兜底」的措辞 → 改为描述真实的 `cluster.sync_interval` 机制
- [group.go:341](../../internal/cluster/group.go) 的注释
- `sq.example.yaml:61` 的 ack 档说明，并补 `cluster.sync_interval` 条目

外加 `cmd/sq/main.go` 头注释（§4 已列）与 `sq.example.yaml` 中自愈说明的更新——后者在 B10 时已改过一版，本次五分支落地后需再改一版。

---

## 10. 边界与已知限制

- **网络卷**：机器世代判的是「内核没换过」。若数据目录挂在网络卷上、卷被 detach/attach 而机器未重启，页缓存假设不成立。本 spec 不覆盖此场景，文档明写。
- **`SQ_BOOTGEN_OVERRIDE`**：测试设施。生产误用会把安全门焊开，缓解手段是 Error 级告警而非禁止（禁止会让进程级 e2e 无法覆盖这条路径）。
- **mem 档掉电仍可能丢数据**：本 spec 把损失面从「无上界」压到「≤ `sync_interval`」，但不消除它。要零丢失请用 `quorum-fsync`。
- **`local-forced` 强制一次重新选举**：抬 term 的必然代价，全集群恢复时本来就要选举，实际影响为零。

---

## 11. 验收标准

1. 五条恢复分支各有单测覆盖，且 `NewManager` 与 `sq recover` 走同一个决策函数（结构上保证判断一致）。
2. `TestScenarioFullProcessCrashRecoversLocally` 通过：三节点全 SIGKILL 后无人值守恢复、确认集零丢失。
3. B10 红线断言在新用例中通过：mem + 世代变 + 无许可 → 拒启且数据目录非空、两条日志串的出现/不出现均成立。
4. 签字往返通过：`sq recover` 报告 → `--grant` → 启动成功 → term 已抬 → 数据完好；且同一许可在下一次世代变更后不再被消费。
5. 安全门守卫用例通过：走完 `ErrUncleanShutdown` 分支后盘上 `raft/bootgen` 仍是旧值（§3.1 的错误顺序会被这条挡住）。
6. mem 档周期 fsync 的吞吐代价有实测数字回填本 spec 与 backlog，不接受以预期代替。
7. §9 列出的全部文档口径完成同步，`grep` 不再能找到「后台周期批量 fsync 兜底」这类与实现不符的描述。
8. macOS 与 Linux 双平台：主套件 `-race` 全绿 + e2e 全量绿。
