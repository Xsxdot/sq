# raft 日志共享单 seglog 设计（B2 修订：恢复跨组 group commit）

日期：2026-08-11
状态：**已实现、验收未通过、已否决**（2026-08-11 晚。实现见分支 `v2-sharedlog-gc`，不合入主线，保留待命）
前置：`2026-08-11-raftlog-segment-storage-design.md`（B2 原 spec）。本文原拟推翻其 §2 第三行「每组独立日志」的定案，**该推翻已被复测否决——每组独立的定案维持不变**。

> ## 否决结论（2026-08-11 晚，先读这一节）
>
> 共享单链 + sync 领导者 group commit 已按本设计完整实现（代码审阅通过：
> macOS `-race` 全量绿、11 个新用例、`raftstore_test.go` 12 例中 10 例零修改
> + 2 例布局断言随设计平移）。**三机验收未通过，方案否决。**
>
> quorum-fsync 档三轮重复测量（轮次交错，离散度 <3%，非噪声）中位数：
>
> | fsync conc | main | seglog（每组独立） | sharedlog（本设计） |
> |---|---|---|---|
> | 16 | 653 | **1209** | 1032 |
> | 64 | 2112 | **2255** | 1864 |
> | 256 | **2884** | 2618 | 2185 |
>
> **每个并发档都劣于每组独立 15-17%**，与本设计的唯一目的相反。
>
> **机理**：成本不在写路径——mem 档同样全组共用一把 `mu` 串行写，
> sharedlog 反而更优（conc=256：4391 vs seglog 4051 vs main 2582）。
> 成本在 §4 的 `syncMu` 队列：每组独立时 4 个组能**并行**发 4 次 fsync，
> 共享后串成一列。conc=256 的 p50 从 76-83ms 抬到 104-120ms，是排队特征。
> 结论：**这块盘吃并行队列深度，不吃批量摊销**；旧机器上 Pebble 单 WAL 能赢
> 说明那块盘相反。同一改动在两种盘上结论相反。
>
> **更要紧的是 §1 的前提本身站不住**：下表 -47% 的回退取自 2026-08-11 上午
> 那批测试机，该批实例当日已销毁；晚间重拉的三台（标称同为 2 核 x86_64）
> 上，同样的 seglog 相对 main 在 conc=256 只回退 9%，低并发反而大幅领先。
> 新机器上 main 的 fsync(2884) 甚至高于 mem(2582)——每轮真刷盘快过不刷盘，
> 物理上不可能，说明该虚拟盘 fsync 近乎免费。**要修的问题在现有条件下不
> 复现，因此本设计的收益不可证，而它的成本已证。** 这是否决的根本理由，
> 不是「性能没达标」这么简单。
>
> **待命条件**：若将来在真实生产盘上复现出显著的 fsync 档回退，本分支代码
> 已就绪且审阅通过，可直接复活复测；届时应先用 fsync 微基准（单并发 vs
> 多并发延迟）量化该盘的性格，再决定摊销与并行孰优。

## 1. 为什么推翻「每组独立」

B2 原 spec §2 判定「fsync 档失去单 WAL group commit 摊销……影响有限」。三机复测（2 核 ×3，同 VPC，128B×20000，conc 16/64/256）证明该判断不成立：

| 档位 | main（Pebble） | seglog（每组独立） | 差异 |
|---|---|---|---|
| quorum-mem conc=256 | 6350 msg/s | 8464 msg/s | **+33%（B2 目的达成）** |
| quorum-fsync conc=16 | 928 msg/s | 1252 msg/s | +35% |
| quorum-fsync conc=256 | 4589 msg/s | **2432 msg/s** | **−47%，conc=64 起饱和** |

根因：4 个 raft 组（meta 组 0 + 3 数据组）各持独立段文件，fsync 档下各组每轮 Ready 各自 fsync 各自的文件——**不同文件的 fsync 物理上不可合并**，云盘刷盘操作速率成为各组均摊的硬顶。Pebble 单 WAL 时代所有组的写入共享一个文件，一次 fsync 覆盖全部组的并发写入（commit pipeline group commit），这正是 main 能到 4589 的机制。

**修订决定：全部组共享一条段文件链，帧带组号，写入端实现 group commit。** 时机理由：v2-b2-seglog 未合并未发布，此刻改盘上格式零迁移成本；发布后再改需第二套 seglog v1→v2 迁移。

## 2. 修订范围（动什么、不动什么）

**动：**
- 文件布局：`raftlog/<g>/NNNNNNNN.seg`（每组一目录）→ `raftlog/NNNNNNNN.seg`（单链，全组共用）；
- 帧格式：帧体加 4B 组号；新增「组重置」记录类型；
- seglog 包 API：`Log` 从单组实例变为多组共享实例（Append/TruncateTo/Reset 带组号参数）；
- 写入端：fsync 从「持锁内联」改为「sync 领导者 + 搭车」的 group commit 结构（§4）；
- 段回收：从「本组水位判定」变为「段内全部组的水位均越过」（§6）；
- 组重置：从「删组目录」变为「追加重置标记帧」（§7）；
- raftStore：`logs map[uint32]*seglog.Log` → 单个共享 `*seglog.Log`；`wipeLog` 消失，由 `Reset(g)` 替代。

**不动（继承 B2 原 spec）：**
- `raftStore` 对外接口签名与语义零变化——raftstore_test 12 用例零修改通过仍是硬验收锚；
- 锚点/成员表/applied/安装标记/干净关机标记留在 Pebble（§3 职责划分）；
- 帧 CRC32C 校验、torn tail 仅末段合法、非末段坏帧 fail-stop 拒启、轮转屏障（旧段 fsync+close 先于新段创建）、syncDir 目录项落盘纪律；
- 「先锚点后截断」运行期守卫、`SaveConfState`/`bumpTermsInto` 的写点与 legacy 分流逻辑；
- mem 档 200ms flusher「先日志后 FSM WAL」顺序契约（现在只刷 1 个文件，是简化）；
- legacy Pebble → seglog 的一次性迁移形态（幂等锚从「删目录」换成「追加重置标记」，见 §7）；
- `SegMaxBytes = 64MiB`（导出变量，测试旋钮）。

## 3. 帧格式与文件布局

- **段文件**：`<data_dir>/raftlog/%08d.seg`，单链全组共用。`raftlog/` 直接放段文件，不再有组子目录。
- **帧**：`[4B len BE][4B CRC32C][1B type][4B group BE][payload]`。CRC 覆盖 type+group+payload（即 len 之后的全部字节），len = 1+4+len(payload)。组号进 CRC 保护范围——组号被写花等同帧损坏，绝不能把 A 组的条目错归 B 组。
- **记录类型**：
  - `recEntry=1`：payload = raftpb.Entry protobuf；
  - `recHardState=2`：payload = raftpb.HardState protobuf；
  - `recGroupReset=3`：payload 为空（组号已在帧头）。语义：该组在此帧之前的全部帧作废（§7）。
- **旧布局守卫**：Open 与只读探测均检查 `raftlog/` 下是否存在纯数字命名的子目录（每组独立时代的布局）。存在即 fail-stop 拒启，错误信息明示：该布局是未发布的开发期格式，不提供自动迁移，清空数据目录重入集群（WipeForRejoin / sq recover 流程）或回退旧构建。测试机/开发盘是唯一受众。

## 4. 写路径与 group commit（本修订的核心）

`Append(g, hs, ents, sync)` 拆成两段：

1. **写段（持 `mu`）**：编码帧、写活动段、更新该组内存态（lastIndex/lastHS/activeMax）、必要时轮转，记录写完后的全局写入水位 `written`（单调字节计数，跨段**不清零**）；
2. **刷盘（不持 `mu`，仅 `sync=true` 走）**：`syncTo(target)`，target = 步骤 1 结束时的 `written`。

`syncTo` 的 group commit 结构（sync 领导者 + 搭车）：

```
syncTo(target):
  持 syncMu（排队点：并发的 sync 请求在此串行）
  loop:
    持 mu 读 {synced, active, written}
    if synced >= target: return nil        // 搭车：前一位领导者的 fsync 已覆盖我
    f, covered := active, written
    释放 mu
    err := f.Sync()                        // 本次 fsync 覆盖到 covered（≥ target，
                                           // 还顺带覆盖了排在我后面已写完的组）
    if err == nil: 持 mu 把 synced 抬到 covered; return nil
    if errors.Is(err, os.ErrClosed): continue  // 轮转关了旧段：轮转自身先 fsync 后 close，
                                               // synced 必已越过 target，重查水位即返回
    return err                             // 真 I/O 错误：fail-stop（与现状同级）
```

**为什么这就是 group commit**：fsync 在途期间，其他组的 Append 只被挡在 `syncMu` 外，**写段（`mu`）不受阻**——它们写完排队；领导者的 fsync 落盘时覆盖的是「fsync 系统调用发起那一刻文件里的全部字节」，队列里所有已写完的组一次全部转正，队首唤醒后查水位直接搭车返回。吞吐模型回到「fsync 速率 × 在途轮数」（见项目 memory `sq-throughput-scales-with-queue-count`）。

**不变量：未 durable 的字节永远只存在于活动段。** 轮转屏障保证旧段 close 前必 fsync，因此 sync 领导者只需 fsync 当前活动段。轮转完成时（旧段已刷、HS 补写已刷）持 `mu` 把 `synced` 抬到 `written`——这也是 ErrClosed 重试循环能终止的原因。

**锁序**：`syncMu` → `mu`（短暂持有读水位/句柄），反向永不发生（Append 持 `mu` 期间绝不碰 `syncMu`），无死锁环。

mem 档语义不变：`sync=false` 只 write() 进页缓存；200ms flusher 的 `Sync()` 走同一条 `syncTo(written)`。

## 5. 轮转

流程与 B2 相同（旧段 fsync → close → 创建新段 → syncDir → HS 补写 → 最后授予旧段回收资格），两处修订：

- **HS 补写逐组**：新段头部写入**每一个已知最新 HS 的组**各一条 recHardState 帧（而非单组一条），随后单次 fsync。不变式升级为「每组的最新 HS 一定在最新段里」——任意旧段被回收都不影响任何组的 HS 可恢复性。已被 Reset 且此后无新 HS 的组跳过（无 HS 可补）。
- **回收资格登记按组**：`segMax` 从 `map[段号]uint64` 变为 `map[段号]map[组]uint64`，登记值取自 `activeMax`（活动段生命周期内各组实际写入的最大 entry index 的持续记账；HS-only 的组值为 0）。stale-high 安全方向的论证原样成立（偏高只延迟回收，绝不误删活条目）。

## 6. 截断与回收：全组水位

`TruncateTo(g, upto)`：

1. 持 `mu` 抬该组水位 `marks[g] = max(marks[g], upto)`；
2. 扫 `segMax`：段可删 ⟺ **对段内出现过的每一个组 g'，`marks[g'] ≥ segMax[段][g']`**（HS-only 的 0 值恒满足——轮转补写不变式保证删它不丢 HS）；
3. 删文件、清登记，日志打删段清单与释放字节。

**滞留有界性论证（本修订的主要代价，必须成立）**：manager 的截断循环每 30s 对**全部组（`g < Groups()`，含 meta 组 0）**各跑一次 truncateOnce，四重下界位点计算不变。因此任一组的水位至多落后一个循环周期 + 该组保留窗口；一个段最坏被 pin 到「段内最慢组的水位越过它」为止，不存在「永不推进的水位」（组闲置时其位点仍随 applied/锚点评估推进）。最坏盘上驻留从「每组 1 段」变为「若干段被最慢组暂 pin」，有界、可观测（删段日志带清单）。**测试必须覆盖**：双组写入同段，仅一组截断 → 段保留；另一组截断越过 → 段删除。

`ResetGroupProgress` 语义下的死帧回收：Reset(g) 把 g 从全部已关闭段的登记中移除（§7），被移除后若某段登记表为空（段内只有 g 的帧），该段在下次扫描即可回收——空集上的全称量词为真。

## 7. 组重置：从物理删除到逻辑标记

共享文件删不掉单组的帧，`ResetGroupProgress` / 迁移「清半截」改用 **recGroupReset 标记帧**：

- `Reset(g)`：持 `mu` 追加一条 recGroupReset 帧并**立即 fsync**（重置是低频事件，一次 fsync 换掉整类「标记悬在页缓存 + 掉电」的推理负担）；清空该组内存态（lastIndex/lastHS/activeMax），把 g 从全部 `segMax` 登记中移除。
- **重放语义**（Open 扫描与逐帧重建共用一条规则）：扫到 g 的 reset 帧 → 丢弃此前累积的 g 的全部状态（hs 置 nil、ents 清空、lastIndex 归零），并把 g 从已扫段的 segMax 累积中移除；此后 g 的新帧从零重新累积。
- **崩溃窗口方向不变**：`ResetGroupProgress` 仍是「先 Pebble 批次（applied=0/删锚点/删标记，Sync）、后 Reset(g)」。中间崩溃 → 盘面 = 进度已清零 + 旧日志帧仍会重放，与 B2「先 Pebble 后删目录」的窗口同形态，安全方向论证原样成立（多余但无害，快照安装覆盖）。
- **迁移幂等锚替换**：`migrateLog` 的「清半截」从 `wipeLog`（删目录）换成 `Reset(g)`。①Reset ②loadLegacy ③分块 Persist ④Pebble Sync 删 legacy 键——③④间崩溃则 legacyPending 仍真，重启重迁，①的 Reset 逻辑清掉上次半截。判定锚（Pebble 旧键存在与否）不变。

## 8. 启动扫描与只读探测

- **Open**：扫单链，按帧头组号分流到各组的重放状态（每组独立执行「后写的赢」冲突裁剪——文件序即全局写序，组内序天然保序）；返回 `map[组]*GroupState{HS, Ents}`。torn tail 仅末段合法（物理截断续跑）、非末段坏帧 fail-stop——规则不变，且单链比 4 链更简单。
- **raftStore**：单个共享 `Log`，首次 Persist/Load（任意组）触发一次 Open，全部组的恢复态一次性入 `recovered` 缓存。`Load(g)`/`Persist(g)` 的缓存更新逻辑（写时复制、别名约定、撕裂读防护）不变。
- **只读探测**（`sq recover` / InspectRecovery 的零副作用契约）：第三支判定从「组目录有非空段文件」改为「`raftlog/` 下存在 size>0 的 `.seg`」——**共享级**判定：无非空段文件时碰都不碰（不 MkdirAll 不 O_CREATE）；有则允许 Open（torn 截断的「提前做下次启动必做且结果唯一的一步」豁免论证不变）。per-group 的 `hasSeg` 事实改由「Open 后该组恢复态非空（hs≠nil ∨ ents 非空）」导出。`diskHasRaftState` 第三支同步改为共享级检查。

## 9. 并发模型

- 全部组的写段串行在一把 `mu` 上。频率 = 各组 Ready 轮次之和（低），临界区 = memcpy 进页缓存（快）；一条 4MiB 大消息追加会短暂阻塞其他组——Pebble 单 WAL 时代本就如此，非退步。
- `Sync`（flusher）/`TruncateTo`（截断循环）/`Reset`（重置路径）与 Append 的互斥关系同 B2（不同 goroutine、频率低）。
- fsync 档低并发（conc=16）此前的独立文件优势会部分回吐（共享排队等待），预期仍不差于 main（928）——基准复测验证。

## 10. 测试策略

1. **seglog 单元测试**（TDD，多数用例按多组重写）：帧含组号 roundtrip + reset 帧；多组交错追加重放分流；组内冲突回退「后写的赢」；轮转逐组 HS 补写 + fsync + 按组登记；**跨组 pin**（§6 的必测项）；Reset 逻辑清空 + 重放 + 空登记段回收 + Reset 后重新写入；torn tail / 非末段损坏（规则不变，单链版）；旧每组布局守卫拒启；**group commit 并发正确性**（N goroutine 并发 Append(sync=true) 全部返回后数据完整可重放；摊销效果由三机基准验证，不做单测断言）。
2. **raftstore_test 12 用例零修改通过**（接口不变的可执行证明）；已有增补用例（分块迁移、物理删段）适配共享形态。
3. **恢复路径全量回归**：manager/cluster/recovery + B10/B11 e2e，双平台（macOS + 4 核 Linux——历史教训）。
4. **三机基准复测**：验收线——quorum-fsync conc=256 **≥ main 的 4589 msg/s**（追平即达标，超出是预期）；quorum-mem 保持 ≥ +30% 优势；conc=16 fsync 不差于 main 的 928。

## 11. 观测性

继承 B2 §10 全部日志点，增补：sync 领导者每次 fsync 覆盖的字节数与搭车组数（Debug，验证摊销真实发生的现场证据）；Reset 标记追加（Warn，带组号——重置是异常路径事件）；段回收日志带「被哪个组的水位卡住」的最慢组信息（回答「盘怎么还没回收」）。
