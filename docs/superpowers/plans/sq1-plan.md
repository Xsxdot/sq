# SQ1 修复计划：新 leader 追平本任期首条之前不得做 leader-only 构造

- 卡：SQ1（bug，已证实）｜分支 `cards/sq1`（基于 main@21ee680，红测提交 c0483eb）
- 级别：非 L3，无骨架、无冻结对象（复用契约：`replication.Router` / `cluster.Manager` 现行版本，本卡给 Router 新增一个方法，见改动边界）
- 状态：**已拍板（2026-11-11，主会话）**——F1–F4 均按推荐；F5 由主会话写入 SQ2 计划约束（SQ2 出队点核对入队时 epoch，不等即 `ErrNotLeader`），SQ1 rebase 到 SQ2 后执行者须核对该约束已落地，未落地即停下升级；meta check-then-write 发现另立卡 SQ5，不归本卡

---

## 待拍板岔口

| # | 岔口 | 推荐 | 被否方案与理由 |
|---|---|---|---|
| F1 | 门对外的形态 | **新增 `WaitWritable(ctx, g) (epoch uint64, err error)`**：cluster 组内实现，经 `Manager` → `replication.Router` 暴露；`IsLeader` 语义不动 | ①改 `IsLeader` 语义为「已追平」：`IsLeader` 有 30 余处调用方，大部分是路由/快速失败（rpc 入口探针、rr 选队、本地/转发分派、后台循环开关），改语义会让新 leader 在追平前把客户端推去 follower（follower 回 HA_NOT_AVAILABLE），选举后一段时间整组不可写也不可路由，比本 bug 的窗口还长；②只给一个非阻塞 `Writable(g) bool`：调用方要自己轮询，且拿不到任期标识，计数器缓存无法与任期绑定（见 F3） |
| F2 | 未追平时的等待策略 | **阻塞等待，上限 = min(ctx, 3s)**，超时返回 `ErrNotLeader` 包装（rpc 已映射 HA_NOT_AVAILABLE，SDK 可重试） | 立即返回 `ErrNotLeader`：SDK 拿到 HA_NOT_AVAILABLE 后重查路由，路由仍指向本节点（它确实是 leader），在集群档退避（100ms→…→3s，5 次，累计约 4.5s，`internal/rpc/settings.go:43-51`）内反复打回同一节点；apply 积压几百毫秒就会白烧掉前几次重试，且每次都是一次完整 RPC。阻塞等待在常见积压（毫秒～百毫秒）下对客户端完全透明；上限保证快照安装期（`installMu`，分钟级）不把 handler 无限挂住，超时后仍走既有可重试路径 |
| F3 | `InvalidateCounters` 的去留 | **删除**：produce 的 offset/seq 缓存改为「带任期标签」——缓存记录装载时的 epoch，`WaitWritable` 返回的 epoch 与之不同即从盘重读；同时删 `cmd/sq/main.go` 里 `onLeaderChange` 对它的调用（`cluster.Options.OnLeaderChange` 选项本身保留，它是通用钩子） | 保留作双保险：同一不变式两套机制，日后改一处漏一处；且它依赖 SoftState 变化可见，`raft/v3@v3.7.0 rawnode.go:147` 只在 SoftState 与「上次已被接受的 Ready」不同时才上报——失而复得若被合并进同一个 Ready 间隙（leader(T)→leader(T')），钩子根本不触发。任期标签对这条路径同样成立 |
| F4 | 本卡覆盖范围 | **只挡「已确认数据被覆盖/丢失」类站点**：produce 三入口（Append / AppendBatch / AppendDelay）、`txn.End` 的「读不到 halfidx 即幂等成功」判定、`LoadOrCreateHandleSecretReplicated` 的「缺失即生成」判定。只造成重复投递 / 假撤回 / 顺序锁失效的站点（deliver、delay 搬运与撤回、txn 回查）记录交 SQ3，接口已备好，SQ3 直接调 `WaitWritable` 即可 | 一并把 deliver 与后台循环全部挂门：改动面翻倍，且 deliver 的取件/ack 路径要另议「阻塞 vs 拒绝」与长轮询的交互，不属本卡的覆盖类缺陷 |
| F5 | 与 SQ2 提案队列的接触 | **升级给主会话协调**：SQ2 的「每组保序提案队列」若让提案在 `ApplyAsync` 返回后才真正 `rn.Propose`，则队列里的提案可能跨任期落盘（T 任期构造的 offset 落进 T' 任期）。要求 SQ2 的出队点在提案前核对「本组当前 epoch == 入队时 epoch」，不等即以 `ErrNotLeader` 失败；本卡提供 epoch 的取值入口（`group` 内的读方法），不碰 propose | 本卡自己去改 SQ2 的出队点：与 SQ2 改同一函数，必冲突；SQ2 先合，本卡 rebase 后再看具体形态 |

另有一条**不归本卡**、建议新开卡的发现：`meta.EnsureTopic` → `CreateTopic` 以本节点 meta 缓存判「不存在」后发绝对值 Set（`internal/core/meta/meta.go:197-236`），缓存滞后的任意节点（不止新 leader，follower 也会经 ForwardApply）都可能用默认队列数覆盖一个已存在 topic 的配置。这是「凭陈旧缓存做 check-then-write」，leader 门挡不住转发路径，需要 apply 时条件写或在 meta leader 侧过门后再判。

---

## 1. 交付目标

对应卡 SQ1 的唯一故事：**集群档某数据组切主后，新 leader 在 apply 追平本任期首条之前不得基于本地 FSM 做 leader-only 构造；已确认消息不得被覆盖。**

做到：

1. `cluster` 组内有「可写门」：本节点是 leader，且已识别本任期首条 index（当选时追加的空条目，下称 termStart），且 `applied ≥ termStart`，才算可写；门带 epoch（= 当前任期号），任期变化即换 epoch。
2. 经 `Manager.WaitWritable` / `replication.Router.WaitWritable` 暴露；单机档恒立即放行、零开销。
3. produce 三入口在读计数器前过门，计数器缓存与 epoch 绑定；`InvalidateCounters` 删除（F3）。
4. `txn.End`、handle 密钥装载两处覆盖/丢失类站点过门（F4）。
5. 红测 `TestSQ1FailoverStaleOffsetOverwritesAckedMessage` 转绿且连跑 ≥10 次稳定，并加反例断言证明门在起作用。

不做：deliver / delay 搬运与撤回 / txn 回查的挂门（交 SQ3）；meta 缓存 check-then-write（建议新卡）；propose 入口任何改动（避让 SQ2）。

## 2. 改动边界

### 文件集（只动这些；超出须升级，见第 5 节）

| 文件 | 改什么 |
|---|---|
| `internal/cluster/writegate.go`（新） | 可写门：状态字段的读写方法、`waitWritable(ctx)`、等待者唤醒；文件头写职责/边界 |
| `internal/cluster/group.go` | `group` 结构体加门状态字段；`dispatchReady` 里识别 termStart 与失去领导权时关门；`applyOnce` 末尾与 `installSnapshot` 推进 applied 后唤醒门等待者；更新 545-556 处「顺序即屏障」注释（钩子失效计数器已不是屏障，屏障改由门承担） |
| `internal/cluster/manager.go` | `Manager.WaitWritable(ctx, g)`；Options 加门等待上限（默认 3s，测试可调） |
| `internal/replication/replication.go` | `Router` 接口加 `WaitWritable`；`StandaloneRouter`（恒 `(0, nil)`）、`Cluster`（转 Manager）实现。**只加方法，不碰 `Apply`/`ApplyAsync`/`ApplyOrForward`**（SQ2 改这些） |
| `internal/core/produce/produce.go` | Append / AppendBatch / AppendDelay 在段 1（`p.mu`）之后、取 `qs.mu`/`delayMu` 之前调 `WaitWritable(g)` 取 epoch；`queueState` 与 delay 缓存加 epoch 字段，`nextOffsetLocked` / `nextDelaySeqLocked` 按 epoch 判命中；删 `InvalidateCounters`；改写 202-220 行那段安全性论证注释 |
| `internal/core/txn/txn.go` | `End`：本节点是 meta leader 时，读 halfidx 之前先 `WaitWritable(meta)`，失败按 `ErrNotLeader` 返回（rpc 已映射 HA_NOT_AVAILABLE，`internal/rpc/txn.go:50-53`） |
| `internal/rpc/handle_secret.go` | `LoadOrCreateHandleSecretReplicated`：`IsLeader(meta)` 分支先 `WaitWritable(meta)`，通过后**重读一次**密钥，仍缺失才生成 |
| `cmd/sq/main.go` | 删 `onLeaderChange` 中 `InvalidateCounters` 调用及其注释；若该闭包因此为空则不再传 `OnLeaderChange` |
| 测试：`internal/core/produce/sq1_failover_offset_test.go`、5 个 Router 测试桩（`internal/core/retention/retention_test.go:66`、`internal/replication/replication_test.go:242`、`internal/core/txn/checker_test.go:23`、`internal/core/delay/delay_test.go:28,36`）、`internal/cluster/cluster_test.go:217-221` 注释、新增不变式单测 | 见第 4 节 |

### 真实入口

- 写路径：gRPC `SendMessage` → `internal/rpc/send.go:102/148/155` → `Producer.AppendBatch/AppendDelay/Append`；跨节点 `OpForwardAppend` → `cmd/sq/main.go` controlHandler → `pr.Append`；延时搬运 / 事务提交 → `pr.Append`（自动过门）。
- 事务决断：gRPC `EndTransaction` → `internal/rpc/txn.go` → `txn.Manager.End`。
- 门的状态源：`group.dispatchReady`（`internal/cluster/group.go:478`）、`group.applyOnce`（`:859`）、`group.installSnapshot`（`:1191` 推 applied 处）。

### 架构方向

- 门属于复制子系统（cluster），只依赖 raft 已给出的事实（SoftState、本轮 append 条目的 term/index、applied 位点）；core 只经 `replication.Router` 看见它，不 import cluster 内部。
- **termStart 的识别规则**（承重，须不变式单测）：本节点处于 leader 态时，在本轮 `MsgStorageAppend` 的条目里找 **Term 等于本节点当前任期** 的第一条，其 index 即 termStart，epoch = 该 Term。依据：`raft/v3@v3.7.0 raft.go:953-962` `becomeLeader` 在同一次 Step 内追加空条目，leader 只追加本任期条目；用「Term == 当前任期」而非「Term 变大」，挡住合并 Ready 里先出现的他人旧任期条目。失去 leader 态（SoftState lead≠self）即关门（termStart 清零）。门判定 = `lead==self && termStart!=0 && applied≥termStart`，任一不满足即不可写——即便 termStart 晚于 `lead.Store` 可见，也只会多关一会儿，不会误开。
- 门等待者在 applied 推进后、失去领导权时被唤醒（后者返回 `ErrNotLeader`）。唤醒机制（按任期的广播 channel 或等待者表）由执行者定。
- `IsLeader` / `Leader` / `SelfIsLeader` / `MetaIsLeader` 语义不变。

### 调用方语义表（rg 全量，见末节读数）

| 调用点 | 需要的语义 | 本卡处理 |
|---|---|---|
| `rpc/server.go:130,133` 写入口探针；`rpc/receive.go:116` 取件入口；`cmd/sq/clusterview.go:36-54` 路由视图；`replication.go:135` 拓扑展示、`:397,431` 转发寻址；`cmd/sq/main.go:628` 启动等 meta leader | 是 leader（路由/快速失败） | 不动 |
| `produce.go:147,153,391,397` rr 选队 | 是 leader（选队） | 不动；选队后另行过门 |
| `replication.go:225` `ApplyOrForward`；`admin/messages.go:155`、`delay.go:252`、`txn.go:233`、`deliver.go:850` 本地/转发分派 | 是 leader（分派），下游 `pr.Append` 自带门 | 不动 |
| `retention.go:176,268` | 是 leader：滞后视图只会少删（offset 单调分配，未 apply 部分 offset 更大），无覆盖 | 不动 |
| `txn.go:174` 读不到 halfidx 判幂等成功 | **须已追平** | 本卡过门 |
| `rpc/handle_secret.go:71` 缺失即生成 | **须已追平** | 本卡过门 |
| `delay.go:110,156` 搬运循环、`:336` 撤回；`txn.go:310,353` 回查循环；deliver Receive/ack/改不可见（无 IsLeader，靠 rpc 入口） | 须已追平，但后果是重复/假撤回/顺序锁失效 | 记录交 SQ3（第 2 题结论） |
| `cluster/manager.go:1014,1104,1867` 摊布、提升 learner、PrepareJoin | 是 leader；成员变更由 raft `pendingConfIndex` 自守 | 不动（未深查 rejoin 是否读 applied confState，记为残余） |

### 与 SQ2 的接触面

- SQ2 改 `replication.go` 的 `Apply/ApplyAsync`、可能改 `group.propose` 与 `Manager.ProposeReserved`。本卡在 `replication.go` 只**新增**方法（Router 接口一行 + 两个实现），在 `group.go` 只动 `dispatchReady`、`applyOnce`、结构体字段、`installSnapshot` 推 applied 处，**不碰 propose / proposeConfChange / ProposeReserved**。rebase 冲突预期限于 `group` 结构体字段区与 `replication.go` 的 import/接口块。
- 语义接触见 F5：SQ2 队列跨任期出队必须失败。本卡 rebase 到 SQ2 后，执行者先读 SQ2 的出队点，确认它有 epoch 核对；没有则停下升级，不自行补。

## 3. 工作顺序

1. **先验证承重假设 A（termStart 可识别）**：在 cluster 包写不变式单测（三节点 harness，`internal/cluster/cluster_test.go` 已有 `newTestClusterOpts`）：当选后门的 termStart 所指条目 Term 等于当选任期、且其前一条 Term 小于当选任期（经组内 raft 存储的 `Term(termStart)`、`Term(termStart-1)` 核对）；`TransferLeader` 来回两次，epoch 严格递增、每次失去领导权门关闭。失败 → 阻塞全部后续，回本节重审识别规则。
2. **验证承重假设 B（门真的挡住窗口）**：cluster 包单测，用 `Options.OnApplied` 阻塞目标节点 apply（同红测手法），转移领导权给它：`WaitWritable` 在等待上限内返回 `ErrNotLeader`；放开后返回 epoch 且 `AppliedIndex ≥ termStart`。
3. replication 层接口与两实现 + 5 个测试桩补方法，`go build ./... && go vet ./...` 过。
4. produce 接门、epoch 缓存、删 `InvalidateCounters` 与 main 接线；produce 包不变式单测（第 4 节 U3）。
5. 调整红测判据（第 4 节 S1），连跑 10 次全绿；再做变异：临时让 `WaitWritable` 直接放行，S1 必须红，跑完还原（变异不提交）。
6. `txn.End`、handle 密钥两处过门；txn 故事级检查 S2。
7. 全量编译 + 触及包测试 + `make test-cluster`；真机清单首跑项。
8. （rebase 到 SQ2 后）复跑 S1×10、S2、`make test-cluster`，核对 F5。

## 4. 验收方式

### 故事级检查（项目测试环境内跑）

- **S1（主故事）** `go test ./internal/core/produce/ -run TestSQ1FailoverStaleOffsetOverwritesAckedMessage -count=10 -v`，10/10 PASS。红测调整只动第 7 步，判据保持对着「已确认消息不得被覆盖」：
  - 删掉「等 F 的 commit > c0」这一步（门生效后新写入不会在追平前提案，该等待会挂到 60s）。
  - 换成**反例断言**：异步发起经 F 的 Append 后，持续约 500ms（小于门等待上限）观测：Append **未返回**，且 F 的 commit **未越过 c0**（门挡在提案之前，没有任何新条目进日志）。任一不成立即 FAIL——这证明是门在起作用，而不是时序碰巧。
  - 放开 apply 闸门后：Append 必须成功；新消息 offset 必须**严格大于**全部已确认 offset（预期恰为 nTail+2）。
  - 第 9 步原判据一字不改：无重复 offset、每条已确认消息在三节点按其 (q, off) 读回仍是它自己。
  - 变异核对（第 3 节步骤 5）：门直接放行时本测试必须 FAIL。
  - 若 F2 拍板为「立即拒绝」：反例断言改为 Append 返回 `errors.Is(err, replication.ErrNotLeader)` 且 commit 未越过 c0；放开后重试直到成功，其余判据同上。
- **S2（txn.End 覆盖/丢失站点）** 新测试（位置执行者定，复用 S1 的三节点装配手法，可抽成包内测试辅助）：meta 组上 L 为 leader，`Stage` 成功；阻塞 F 在 meta 组的 apply 后把 meta 领导权转给 F，经 F 调 `End(commit=true)`：门未开时不得返回 `found=false` 的成功；放开后必须返回 `found=true`，且该消息出现在目标队列。修复前此测试应为红（先跑一次红存证再修）。

### 不变式单测（只为承重规则）

- **U1** termStart 识别规则（第 3 节步骤 1）。
- **U2** 门的开/关/超时（第 3 节步骤 2），含失去领导权时正在等的调用方立即返回 `ErrNotLeader`（不等到超时）。
- **U3** produce epoch 缓存：用假 Router（epoch 可切换）+ 真 store：同 epoch 连续 Append 不重读盘（在两次之间直接改盘上 alloc，第二次仍用缓存值）；epoch 变化后的首次 Append 从盘重读（取改后的值）。这条替代原 `InvalidateCounters` 的契约，反着写会红。

### 编译与包测试

- `go build ./... && go vet ./...`
- `set -o pipefail; go test -timeout 10m -count=1 ./internal/cluster/... ./internal/replication/... ./internal/core/... ./internal/rpc/... ./cmd/... 2>&1 | tee /tmp/sq1-pkgs.log`（输出落文件，遵守 Makefile 的测试输出纪律）
- `make test-cluster`

### 真机清单（本机进程级三节点，`test/e2e` 既有 harness）

1. **首跑项**：S1 首次转绿即跑 `cd test/e2e && go test -tags e2e -count=1 -run 'TestScenarioKillLeaderHard|TestScenarioRollingRestart' -v ./... 2>&1 | tee /tmp/sq1-e2e-first.log`。判据：两用例 PASS（确认集 ⊆ 消费集，被覆盖的消息会表现为确认集缺失）；记录日志中 `sent/failed` 计数，与同命令在 main@21ee680 上的基线对比，failed 不得显著上升（门的阻塞等待不应把 SDK 重试预算烧穿）。存证：两份日志的 PASS 行与计数行。
2. 全量场景：`make e2e`，全 PASS。
3. 说明：e2e 场景不保证命中「apply 落后时切主」窗口（无法从进程外单独卡住 apply），它是回归护栏；窗口本身的证明靠 S1/S2 的进程内注入。

## 5. 决策权限

执行者可自定（交回物「偏离与自主决定」逐条对照本节说明）：

- 门状态的具体字段与唤醒机制（广播 channel / 等待者表）、文件内私有函数划分。
- 门等待上限的默认值在 [1s, 3s] 内取值，以及它放在 `cluster.Options` 还是包内常量 + 测试钩子。
- produce 中过门调用的精确位置（须满足：不持 `p.mu`/`qs.mu`/`delayMu` 等待；读计数器前已拿到 epoch）；rr 选队是否优先选「已可写」的队列。
- S2 的测试位置与辅助函数抽取方式。
- `cluster_test.go:217-221` 等注释的措辞更新。

必须停下升级：

- 需要改 `IsLeader`/`Leader`/`SelfIsLeader`/`MetaIsLeader` 的语义，或改 `group.propose`/`proposeConfChange`/`Manager.ProposeReserved`/`replication.Apply/ApplyAsync/ApplyOrForward`。
- 需要新增或改变协议错误码（rpc 层映射）、改变 SDK 可见的重试策略（`internal/rpc/settings.go`）。
- 第 3 节步骤 1 或 2 的承重假设被证伪。
- 想把 deliver / delay / txn 回查也挂门（F4 范围外），或想动 meta 的 check-then-write。
- rebase 到 SQ2 后发现其提案队列不做跨任期失败（F5）。
- 红测第 9 步判据需要任何放宽。
- 真机首跑项 failed 计数显著高于 main 基线。

---

## 读数与证据

- `cd sq1 && git log --oneline -3` → `c0483eb test(cluster): SQ1 复现切主后旧 alloc 覆盖已确认消息（红）` / `21ee680 …` / `9e22965 …`
- `go test ./internal/core/produce/ -run TestSQ1FailoverStaleOffsetOverwritesAckedMessage -count=2` → 2/2 `--- FAIL`；`窗口: L.applied=11 F.commit=11 F.applied=6`；`同一 offset 被分配两次: q=0 off=2`；三节点 `已确认消息被覆盖: … q=0 off=2 … 现为 …(body="x-after-failover")`
- `internal/cluster/group.go:545-556`：`gr.notifyLeaderChange(rd.SoftState.Lead)` 先于 `gr.lead.Store(rd.SoftState.Lead)`
- `internal/cluster/group.go:1778-1780`：`isLeader()` = `gr.lead.Load() == gr.selfID`
- `internal/cluster/group.go:1598`：propose 只在 `lead != 0 && lead != gr.selfID` 时快速失败
- `internal/cluster/group.go:1484`：空条目「只随批推进 applied 位点」→ 空条目被 apply 后 applied ≥ 其 index
- `internal/cluster/group.go:1191`：快照安装收口 `gr.applied.Store(idx)`
- `internal/core/produce/produce.go:461-475`：`nextOffsetLocked` 未命中缓存即 `st.Get(store.AllocKey(...))`
- `internal/core/produce/produce.go:322-337`：`nextDelaySeqLocked` 同形态；`AppendDelay`（:261）无 `IsLeader(meta)` 前置检查
- `~/go/pkg/mod/go.etcd.io/raft/v3@v3.7.0/rawnode.go:147`：`if softSt := r.softState(); !softSt.equal(rn.prevSoftSt) { rd.SoftState = softSt }`；`:400-402` acceptReady 才更新 prevSoftSt
- `~/go/pkg/mod/go.etcd.io/raft/v3@v3.7.0/raft.go:958-962`：`becomeLeader` 内 `emptyEnt := &pb.Entry{Data: nil}` → `r.appendEntry(emptyEnt)`
- `rg -n "IsLeader\(|SelfIsLeader|\.Leader\(|AppliedIndex\(|ReadBarrier\(" -g '*.go' -g '!*_test.go'`（cluster 包外）→ handle_secret.go:71；replication.go:135,190,194,225,290,293,379,382,397,431；rpc/server.go:130,133；produce.go:147,153,391,397；rpc/routeview.go:26,28,47,50；cmd/sq/main.go:628；admin/messages.go:155；cmd/sq/clusterview.go:36,48-54；rpc/receive.go:116；delay.go:110,156,252,336；txn.go:174,233,310,353；deliver.go:202,850；retention.go:176,268
- cluster 包内：manager.go:1014（balanceOnce）、:1104（promoteLearners）、:1867（handlePrepareJoin）；readbarrier.go:52,81
- `internal/core/txn/txn.go:166-179`：读不到 halfidx 时，仅「非 meta leader」报 ErrNotLeader，meta leader 直接 `return false, nil`（幂等成功）
- `internal/rpc/handle_secret.go:63-83`：本地读不到密钥且 `rt.IsLeader(cluster.MetaGroup)` 即生成新密钥并 `rep.Apply`
- `internal/core/meta/meta.go:197-236`：`EnsureTopic` 缓存未命中 → `CreateTopic`，`m.topics` 未命中即 `b.Set(store.TopicMetaKey(name), raw)` 经 `ApplyOrForward`
- `internal/core/deliver/deliver.go:456-462`：取件读 `CursorKey` 后写回 `newCursor`（滞后视图 → 位点回退 → 重复投递）
- `internal/core/delay/delay.go:356-380`：撤回读 delay 条目存在即删除并报成功（滞后视图 → 已搬运条目被「撤回成功」）
- `internal/core/retention/retention.go:176-205`：按可见消息定 boundary 后 `DeleteRange [0, boundary)`
- `internal/rpc/settings.go:43-51`：集群档 `backoffMaxCluster = 3s`、`backoffMaxAttemptsCluster = 5`
- `internal/rpc/txn.go:50-53`、`server.go:180`：`errors.Is(err, replication.ErrNotLeader)` → `HA_NOT_AVAILABLE`
- Router 测试桩（需补方法）：`retention_test.go:66`、`replication_test.go:242`、`txn/checker_test.go:23`、`delay_test.go:28,36`
- `Makefile:44-45`：`e2e: cd test/e2e && go test -tags e2e -count=1 ./...`
