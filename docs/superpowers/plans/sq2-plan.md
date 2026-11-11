# SQ2 修复计划：集群档同组提案保序（每组一条 FIFO 提案队列）

- 卡：SQ2（bug，已证实）——集群档同队列提案不保序 → `alloc/` 倒退 + 消费漏投已确认消息
- 分支：`cards/sq2`（基于 main@21ee680；5383c28 = 红色复现用例）
- 状态：**已拍板（2026-11-11，主会话）**——D1–D5 均按推荐；D2 跨任期约束为硬约束（来自 SQ1 计划 F5）；D5（apply 时按条目任期校验）不在本卡，残余 R1 记卡
- 级别：非 L3，不出骨架；无新增冻结对象（见「改动边界 · 契约」）

---

## 待拍板岔口

| # | 岔口 | 推荐 | 被否方案与理由 |
|---|---|---|---|
| D1 | 入队时本节点 **leader 未知**（lead==0，选举窗口）怎么办 | **分两种形态**：`ApplyAsync`（异步形态，调用方均为「持队列锁的 leader-only 构造」）直接失败、报 `ErrNotLeader`；`Apply`/`Manager.Propose`（同步形态）保持现状：不绑任期、排队等 leader 产生后再提案 | 全部维持现状（等 leader）：produce 段 1 判过 IsLeader、段 2 入队前失去领导权的窗口里，用旧任期计数器构造的批次会在**下一任期**才进日志，新任期 `InvalidateCounters` 重读的 alloc 不含它，下一笔写会分配到同一 offset、覆盖它（窄但真实的已确认消息丢失路径，现状同样存在）。全部快速失败：同步形态的启动窗口语义（group.go:1568-1570 注释，group 单测直接依赖 lead==0 时阻塞）会被打破，牵连面超出本卡 |
| D2 | 提案**跨任期**怎么处理（失败语义的核心）——**已由主会话追加为硬约束（来自已拍板 SQ1 计划 F5），本行只剩实现细节待确认** | **按任期绑定**：入队时本节点是 leader 则把条目绑定到当时的 raft **任期号（epoch = term）**；proposer 出队、调 `rn.Propose` 前核对「本组当前任期仍等于入队任期且本节点仍是 leader」，否则以 `ErrNotLeader` 失败、绝不提案；`rn.Propose` 的 ctx 并上「本任期 ctx」，失去领导权即取消；某条提案被 raft 丢弃（`ErrProposalDropped`）后**同任期后续在途条目一律失败**。任期读取入口由本卡在 group 内提供（只读方法，建议名 `leaderTerm()`，SQ1 rebase 后复用），不依赖 SQ1 代码。SQ1 的「可写 epoch」= 同一任期号、在 applied ≥ 本任期首条 index 后开门，produce offset 缓存带 epoch 标签 | 只做 FIFO、失败各自为政：同一任期内「N 失败 N+1 成功」安全（论证见 §验收·失败语义），但跨任期的 N+1 会把旧任期计数器构造的批次带进新任期，与 D1 同一类覆盖风险；失败一条就清空全队列：误伤调用方 ctx 取消这类与领导权无关的单条失败，换不来额外安全性 |
| D3 | 队列是否设上限 | **不设上限**，内存天然受调用方并发约束（每个入队者随后都 Wait；现状每提案一个 goroutine 同样无上限）；深度超阈值记一条 Warn（执行者定阈值，建议 4096） | 有上限 + 满则失败：持锁调用方不能阻塞，满了只能报新错误类，协议面需要新增翻译，且选举窗口内队列积压恰恰是正常现象，会把可等的写变成失败 |
| D4 | 入队期失败（组未知/已停止/已知他人是 leader/D1 的 lead==0）从哪里报 | **一律经 `Pending.Wait()` 报**，`Cluster.ApplyAsync` 保持「从不同步返回错误」的现状契约 | 同步返回：更早失败、produce 不推进 `qs.next`，但改变 `ApplyAsync` 现有调用面契约，且 `TestClusterApplyAsyncOnFollowerReturnsErrNotLeader` 断言 `ApplyAsync` 不报错、错误在 Wait——收益（少烧一个 offset，烧号本身无害）不值这个契约变动 |
| D5 | 残余跨任期窗口（见 §风险 R1）是否在本卡用「apply 时按条目 term 校验」彻底关闭 | **不在本卡做**，记入 SQ1 接触面/后续卡 | 需改提案头（16B→更长）= raft 日志持久格式变更、滚动升级兼容，属不可逆迁移，远超 SQ2 范围 |

---

## 1. 交付目标

修复卡 SQ2 的唯一根因：集群档 `replication.Cluster.ApplyAsync` 每提案起一个 goroutine 调 `rn.Propose`，同组提案进入 raft 日志的先后与调用顺序脱钩。修复后：

1. **定序契约**（与单机档对齐）：同一进程内对同一 raft 组 g，若提案 A 的 `ApplyAsync`/`Apply`/`ProposeReserved` 调用**返回先于**提案 B 的调用发起（happens-before，例如二者都在同一把 `qs.mu` 下先后调用），且 A、B 都进入了 raft 日志，则 A 的日志 index < B 的，从而 A 先于 B apply（同一 apply 批内 MergeRepr 时 B 的写覆盖 A 的写）。范围是**同组全局 FIFO**，天然覆盖同队列、同 meta 计数器。跨组不承诺任何顺序（各组独立日志，本来也无从承诺）。
2. **失败契约**：一条提案失败（不论原因），排在其后的同组提案要么同样失败，要么与它同属一个任期且失败的那条**永不进入日志**——即「已进日志的提案之间不存在可被后来填上的空洞」，且旧任期构造的提案不进入新任期日志（D2；残余窗口见 R1）。
3. 红色回路 `TestSQ2ClusterSameQueueApplyOrder`、`TestSQ2ClusterReceiveSkipsLateAppliedOffset` 转绿且连跑 10 次稳定。
4. 停机（StopClean/kill/ctx 取消）时队列与 proposer goroutine 随组退出，所有等待者有界返回错误，不挂死。

**不做**：多提案合并为一个 entry（后续性能卡）；SQ3（deliver 锁外读旧态）；SQ1（新 leader 未追平即读旧 alloc）；提案头/日志格式变更。

## 2. 改动边界

### 文件集（白名单，超出即越界，须升级）

| 文件 | 改什么 |
|---|---|
| `internal/cluster/proposer.go`（新增） | 每组 FIFO 提案队列 + proposer 循环 + 任期号/本任期 ctx 管理 + 异步 pending 类型 |
| `internal/cluster/group.go` | `group` 结构体加队列/任期号字段；`newGroup` 初始化；`run`（:409 起）把 proposer 纳入 `stages` WaitGroup 同生共死；`propose`（:1587-1648）拆为「入队」+「等待」两半，原同步语义 = 入队 + 等待；`dispatchReady`（:478 起）只加两处调用：处理 SoftState 前更新任期缓存、SoftState 变化时切换本任期 ctx（helper 与只读方法 `leaderTerm()` 放 proposer.go） |
| `internal/cluster/manager.go` | 新增异步提案入口（建议名 `ProposeReservedAsync(ctx, g, data) (*ProposalPending, error)` 或等价，返回值有 `Wait() error`）；`ProposeReserved`/`Propose` 改为走同一队列（入队 + Wait） |
| `internal/replication/replication.go` | `Cluster.ApplyAsync`（:353-360）改调新异步入口、删除 goroutine；`Cluster.Apply`（:329-335）保持走 `ProposeReserved`（其内部已经过队列）；重写 :337-351 与文件头 :9-11 的错误注释（「定序发生在 raft 内部」不成立）；`chanPending` 若无用则删（同步改 `replication_test.go:234` 的类型断言） |
| `internal/cluster/proposer_test.go`（新增） | 承重规则的不变式单测（见 §4.2） |
| `internal/cluster/cluster_test.go` | 仅允许：测试 shim `clusterReplicator.ApplyAsync`（:75-83）改调新异步入口以保持「与 replication.Cluster 同语义」；不改任何断言 |
| `internal/replication/replication_test.go` | 仅允许随 `chanPending` 删除做编译适配 |

**明令不碰**：`internal/core/produce/produce.go`（含 `nextOffsetLocked`）、`internal/core/deliver/deliver.go`、`internal/core/*` 其余调用方、`group.go` 的 `isLeader`/`lead.Store` 时序、`applyEntries`/`applyPhase`、提案头布局 `ProposalHeaderLen`、两份 `sq2_*_test.go`。

### 真实入口

- 写热路径：`produce.Append`（produce.go:197）/`AppendBatch`（:439）/`AppendDelay`（:301，meta 组 delayalloc 计数器，同一病）→ `replication.Cluster.ApplyAsync` → 新异步入口 → 组队列 → proposer → `rn.Propose`。
- 消费路径：`deliver.receiveOnce`（deliver.go:554）/`AckBatch`（:669）/`changeInvisibleLocked`（:735）→ 同上。
- 同步路径：`Cluster.Apply` → `Manager.ProposeReserved` → 入队 + Wait。调用方：deliver.go:787/:889/:917、txn.go:393/467/479/503/513/533、delay.go:233/281/380、retention.go:203、rpc/handle_secret.go:78、`ApplyOrForward`（meta、adminops）、cmd/sq/main.go:696（ForwardApply 控制通道落地）。
- ForwardAppend 控制通道落地 `pr.Append`（main.go:259-276）→ 天然走异步入口。

### 架构方向

保序是 **raft 组的职责**，放在 `internal/cluster`（组生命周期、leader/任期号状态都在这里）；`replication` 维持纯粘合层，只换调用的 Manager 方法。依赖方向不变（cluster → replication → core）。core 层零改动——core 现有代码按「ApplyAsync 返回即定序」写成，本卡让集群档兑现这句契约，而不是让 core 去迁就。

### 契约

无新增冻结对象。复用：`replication.Replicator`（Apply/ApplyAsync 签名不变，语义补齐为「返回即在同组内定序」）、`replication.Pending`、提案头 `ProposalHeaderLen`=16（不变）、`cluster.ErrNotLeader`。Manager 新增一个导出方法属本工作单元内部可逆接口，仅 replication 消费。

### 队列语义（执行者照此实现；数据结构自定）

入队（调用方线程，非阻塞，O(1)）：

| 入队时状态 | 处理 |
|---|---|
| 组已停止 | 失败：`ErrNotLeader` 包装「组 g 已停止」（经 Wait 报，D4） |
| 已知 leader 是他人 | 失败：`gr.notLeaderErr()`（与现 group.go:1597-1601 快速失败同语义） |
| 本节点是 leader | 入队，绑定当前任期号（`leaderTerm()`） |
| lead==0 | 异步入口：失败 `ErrNotLeader`；同步入口：入队、不绑任期（D1） |

proposer（每组一个 goroutine，随 `run` 启停，严格 FIFO 逐条处理）：

1. 条目 ctx 已取消 → 以 `ctx.Err()` 失败，不提案。
2. 绑任期条目：若「本节点非 leader」或「`leaderTerm()` ≠ 入队任期」或「该任期已被标记作废」→ `ErrNotLeader` 失败，不提案。任期一变，同组所有旧任期在途条目都在这一步依次失败（即「连带后续同组在途提案」）。
3. 回填提案头、注册 waiter（与现 propose 的 :1606-1617 相同）、`rn.Propose(提案ctx)`。提案 ctx = 条目 ctx ∧ 组生命周期 ctx（∧ 绑任期条目的本任期 ctx）。
4. `rn.Propose` 返回错误 → 移除 waiter；`ErrProposalDropped` 或因本任期 ctx 取消而返回 → 归 `ErrNotLeader`，并把该任期标记作废（后续同任期条目走第 2 步失败）；其余错误原样。
5. 成功 → 只把「已进日志」交给等待方，**proposer 不等 apply**（等 apply 在调用方的 Wait 里，否则 proposer 串行等 quorum 会把吞吐打成 1/RTT）。

等待（`Pending.Wait`，调用方线程，恰好一次）：先等 proposer 处理完本条（失败即返回该错误）；再按现 propose :1629-1647 的逻辑等 waiter / 条目 ctx（超时分类 ErrNotLeader 或 ctx.Err() 与现状一致）；另加组退出分支：组已退出且 waiter 未就绪 → 返回停止错误（优先检查 waiter，已 apply 的不得因停机报失败）。

任期读取入口与切换（`dispatchReady`，Ready 循环线程）：本卡在 group 内新增只读方法 `leaderTerm()`（名字可调），返回「Ready 循环已观察到的当前任期」。**不得复用 `gr.lastTerm`/`currentTerm()`**——它在 append 阶段 persistPhase 才更新，滞后于 SoftState（group.go:1781-1790 注释：当选那一轮 term 可能仍是旧值），用它绑任期会把新任期条目绑到旧任期上。实现要求：在 `dispatchReady` 内、处理 SoftState 之前，从非空 `rd.HardState` 读出 term 更新任期缓存（只读，不消费——async 下持久化仍走本地消息）；SoftState.Lead 变化时，在既有「先钩子、后 `lead.Store`」顺序里，**任期缓存更新与本任期 ctx 的取消/新建必须发生在 `lead.Store` 之前**；入队读「是否 leader + 任期」必须得到一致快照（建议把 term 与 isSelf 打包进一个 atomic，执行者自定）。此处依赖承重假设 H2（见工作顺序第 1 步）。

## 3. 工作顺序

每步「做 → 检查」，检查不过不进下一步。

1. **钉承重假设 H1**（rn.Propose 同步返回 = 已按调用顺序进入 leader 未稳定日志）：读 `go.etcd.io/raft/v3@v3.7.0/node.go` 的 `stepWithWaitOption`（:514 起，wait=true 等 `pm.result`）与 `run` 中 propc 处理（:374/:377/:426 propc 置 nil 的三处）、`raft.go` 的 `stepLeader` MsgProp 分支 `appendEntry`。结论写进 proposer.go 文件头注释（带出处行号）。若发现 Propose 返回不代表已 append（例如异步投递），**停下回报主会话**——整个方案依赖它。
   同步钉 **H2**（`rd.HardState` 在 AsyncStorageWrites 下照常填充，且与 `rd.SoftState` 取自同一 raft 状态快照，因此 Ready 循环在看到 Lead=self 时，本轮或更早的 Ready 已带出该任期的 HardState）：出稿时已读 `rawnode.go:148-153`（SoftState/HardState 在 `readyWithoutAccept` 内对同一 `r` 计算，HardState 填充不受 `asyncStorageWrites` 分支影响），执行者复核 `becomeCandidate` 先于 `becomeLeader` 抬任期，并在单测里断言「当选后 `leaderTerm()` == `rn.Status().Term`」。H2 不成立则退路是在 SoftState 变为 self 时于 Ready 循环内同步取 `rn.Status().Term`（须先确认不与 node.run 死锁），走退路前回报主会话。
2. **新增 proposer.go + 改 group.propose**：先只实现 FIFO + 停机收尾，不含任期号。检查：`go build ./...`；`go test ./internal/cluster/ -count=1` 全绿（现有 propose/ApplyAsync/超时/follower 快速失败用例不回归）。
3. **接 Manager 异步入口 + 改 replication.Cluster.ApplyAsync**。检查：两条 TestSQ2 各跑 1 次转绿（这是首条故事级检查；绿后立即跑真机首跑项，见 §4.4）。
4. **加任期号绑定（D2）与 lead==0 分形态处理（D1）**，接 `dispatchReady` 两处、提供 `leaderTerm()`。检查：`go test ./internal/cluster/ ./internal/replication/ -count=1` 全绿，含新增 proposer_test。
5. **写不变式单测**（§4.2）并做变异验证（见 §4.2 末）。
6. **注释收口**：replication.go 文件头与 ApplyAsync 注释、group.propose 注释、`Replicator.ApplyAsync` 接口注释改为真实机制；produce.go:34 与 :202 注释说的「定序」此后对两档都成立，不改 produce.go。
7. **全量与稳定性**：§4.3 全部命令；SQ2 两条连跑 10 次；`-race` 跑 cluster 包与 SQ2 两条各一次。
8. **真机清单**（§4.4）。

## 4. 验收方式

### 4.1 故事级检查（真实组件、真实入口、可观察结果）

| 故事 | 检查 | 判据 |
|---|---|---|
| 同队列写入 apply 顺序 = offset 顺序，alloc 不倒退 | `go test ./internal/core/produce/ -run TestSQ2ClusterSameQueueApplyOrder -count=1 -v` | 50 轮（或 90s 预算内全部轮次）第一层逆序 0 处、第二层 alloc==已应答最大 offset+1；日志行「第一层红 0 轮…第二层红 0 轮」 |
| 并发写与取件下不漏投已确认消息 | `go test ./internal/core/deliver/ -run TestSQ2ClusterReceiveSkipsLateAppliedOffset -count=1 -v` | 输出「未复现」行，无「漏投红」 |
| 稳定性 | `go test ./internal/core/produce/ ./internal/core/deliver/ -run 'TestSQ2' -count=10 -timeout 60m` | 10 次全 PASS（单次最长约 2×90s，总时长 ≤ 30 min） |

两条测试判据不得修改（文件不在白名单）。

### 4.2 不变式单测（只为承重规则写；`internal/cluster/proposer_test.go`）

1. **同组调用顺序 = apply 顺序**：单节点组（复用 group_test.go `startSingleNodeGroup`），64 goroutine 在一把外部互斥锁下取递增序号、构造携序号的批次、调异步入口、解锁后锁外 Wait；用 OnApplied 或读回键序记录 apply 序列，断言严格递增。反例：把异步入口换回「goroutine 内同步 propose」必须变红。
2. **（硬约束，SQ1 F5）入队后任期变化 → 出队即失败、不进 raft**：入队若干绑任期条目并让 proposer 尚未处理（执行者自定手段，例如先让 proposer 卡在一条被阻塞的条目上，或包内直接驱动任期切换 helper），使本组任期变化（真实换届优先：三节点 `newTestCluster` 上 `TransferLeader` 走一圈再回来，使本节点在新任期重新当选——这正是「T 构造、T' 落地」的危险形态；构造不出再退到包内驱动）后放行；断言这些条目 Wait 均为 `ErrNotLeader`，OnApplied / 各节点 store 从未见到它们的载荷，且新任期入队的条目照常成功。另加一条：当选后 `leaderTerm()` == `rn.Status().Term`（H2）。
3. **丢弃后同任期作废**：构造一条提案 `ErrProposalDropped`（例如三节点 `newTestCluster` 中对刚失去领导权的旧 leader 连续入队），断言其后同任期条目全部 `ErrNotLeader`、未进日志。若构造成本过高，可降级为包内对 proposer 单步函数的表驱动测试（注入假 Propose 函数），须在交回物里说明。
4. **条目 ctx 先于处理取消**：不提案、Wait 返回 `context.Canceled`，后续条目照常成功（证明单条非领导权失败不株连）。
5. **停机收尾**：入队后取消组 ctx，所有 Wait 在 5s 内返回非 nil 错误；停机后入队立即（经 Wait）返回停止错误；`<-gr.done()` 后无 proposer goroutine 残留（`-race` + 现有 cleanup 的 5s 等待覆盖）。
6. **lead==0 分形态**：`startLoneGroupOfThree`（永无 leader）上，异步入口 Wait 立即得 `ErrNotLeader`；同步入口在 ctx 超时前阻塞、超时后返回 ctx 错误（现状语义）。

变异验证（执行者亲手做，结果写进交回物）：去掉任期核对 → 第 2 条红；把 `leaderTerm()` 换成 `currentTerm()`（lastTerm）→ 至少 H2 断言红；去掉作废标记 → 第 3 条红；proposer 改为每条 `go` 出去 → 第 1 条与 TestSQ2 均红。

失败语义的安全论证（第 2 条交付目标的依据，测试对着它写）：
- 同任期内「N 失败、N+1 成功」只可能是 N 因自身 ctx 或绑任期核对失败而**从未进日志**。N 未应答；N+1 在其后进日志、apply 后 alloc ≥ N+2；offset N 成为永久空洞（无消息键），取件越过它不丢任何已应答消息；produce 已提前推进的 `qs.next` 与盘上 alloc 一致或更大，单调不回退，不会重复分配——**无需额外失效计数器缓存**。
- N 已进日志但 Wait 失败（ctx 超时/leader 变更）的语义与现状一致：条目可能仍 commit；若被截断，raft 日志匹配性质保证截断是后缀（N 被截则 N+1 必被截），不会出现「N+1 留着、N 后来补上」。
- 领导权丢失：绑任期条目一律不提案；本节点重新当选时 `onLeaderChange(isSelf)` → `InvalidateCounters`（main.go:255）照旧触发，计数器从盘上重读。旧任期条目不会出现在新任期日志里（R1 残余除外），因此新任期重读的 alloc 不会漏算一条「迟到的旧任期条目」。

### 4.3 全量编译与触及包

```
go build ./... && go vet ./internal/cluster/ ./internal/replication/
go test ./internal/cluster/ ./internal/replication/ ./internal/core/... ./internal/rpc/ ./cmd/... -count=1
go test -race ./internal/cluster/ -count=1
go test -race ./internal/core/produce/ ./internal/core/deliver/ -run TestSQ2 -count=1
```
全仓 `go test ./... -count=1` 跑一次作为收尾证据。

### 4.4 真机清单（本机三进程 e2e，`test/e2e` 独立 module，TestMain 自动编 broker）

| # | 命令 | 判据 |
|---|---|---|
| **E1（首跑项）** | `cd test/e2e && go test -tags e2e -count=1 -run 'TestThreeNodeClusterE2E' -v -timeout 20m` | PASS：三节点发收 + kill 数据组 leader 后继续发收「允许重复不允许丢」+ 重启自愈对账无丢失。第 3 步 TestSQ2 首次转绿后立即跑 |
| E2 | `cd test/e2e && go test -tags e2e -count=1 -run 'TestScenario(KillLeaderHard|RollingRestart|ProducerExitsRightAfterCommit|MinorityCannotWrite)|TestClusterDLQ' -v -timeout 40m` | 全 PASS（切主、滚动重启、少数派不可写都穿过新的任期号/停机收尾路径） |
| E3（性能对照，可选但建议） | 在 main@21ee680 与本分支各跑：`cd test/e2e && SQ_BENCH=1 SQ_BENCH_CONC=16,64,256 go test -tags e2e -count=1 -run TestClusterWriteThroughput -v -timeout 40m` | 本分支 quorum-mem/quorum-fsync 各档 msg/s 不低于 main 的 90%；低于则带两组数字回报主会话，不自行优化 |
| E4（SQ3 影响观测，不改判据） | 把 `/Users/df-zidong/workspace/sq/.claude/worktrees/agent-af9ad1270a441e9d9/internal/core/deliver/sq3_cluster_race_test.go` **临时**拷入本 worktree 跑 `go test ./internal/core/deliver/ -run SQ3 -count=3 -v`，记录结果后删除，不提交 | 预期仍红（见 §6）；只记录，不作为本卡通过条件 |

须存证据：每条命令的末尾 PASS/FAIL 行与关键日志行，贴进交回物。

## 5. 决策权限

执行者可自定（交回物逐条列出）：队列数据结构（切片+互斥+信号 chan、链表等）；proposer 内部是否「一次取走全部就绪条目再逐条提案」（建议这样写，为后续合并卡留口子）；合并 ctx 的实现手段（`context.AfterFunc` 等）；任期号与 isSelf 的打包方式；Manager 新方法命名；`chanPending` 去留；Warn 阈值与日志文案/级别（遵守 replication.go 文件头的日志豁免约定，proposer 热路径零日志）；测试辅助手段；第 4.2 第 3 条的降级（须说明）。

必须升级（停下回报主会话）：
- 第 1 步 H1 不成立；
- 需要修改白名单外文件，尤其 produce.go、deliver.go、`isLeader`/`lead.Store` 顺序、`applyEntries`；
- 改提案头或任何持久格式；
- 任期绑定（SQ1 F5 硬约束）无法按 §2 实现、H2 不成立需走退路、或想用 `currentTerm()`/本地计数替代任期号；
- `Cluster.ApplyAsync` 改为同步返回错误（D4 推翻）、队列加上限（D3 推翻）、入队状态表（§2）任何一行的语义改变；
- 任何既有测试需要改断言才能过，或 TestSQ2 两条需要改判据；
- E3 性能低于 90% 基线；
- 发现除 ApplyAsync goroutine 外的第二条乱序来源（例如 apply 侧）。

## 6. 与 SQ1 / SQ3 的接触面

**SQ3（deliver 锁外读旧态 → 重复投递）：本卡不修，影响为「部分变好，主症状不变」。** 修复后同队列的 receive/ack/changeInvisible 批次按持锁顺序进日志，消除了「后取件的 cursor 先 apply、先取件的较低 cursor 后 apply 覆盖」这类由乱序导致的位点回退型重复；但下一个持锁者在前一批 apply 之前读 pebble 拿到旧 cursor/看不到在途 inflight 的陈旧读仍在，SQ3 两条复现用例（用例 A 重复投递、用例 B 顺序锁失效）预期仍红，其收尾丢失检查（预写数据）不受影响。本卡提供的 FIFO 是 SQ3 任何「锁内只等定序、读己之写走内存覆盖层」类方案的前提，不阻碍 SQ3。E4 只观测不判定。

**SQ1（新 leader 未追平即读旧 alloc 分配 offset）：本卡在其后实现的卡之前合入。** 接触面只有两处：
- **硬约束（SQ1 计划 F5，已拍板）**：SQ1 引入「可写 epoch」= 当前任期号，applied ≥ 本任期首条 index 后才开门，produce offset 缓存带 epoch 标签。本卡的队列按**任期号**绑定（不是本地计数），出队前核对「任期未变且仍是 leader」，否则 `ErrNotLeader` 且不进 raft——否则 T 任期构造的 offset 可能落进 T' 日志，绕过 SQ1 的门再次覆盖已确认消息。
- **任期读取入口归本卡提供**：group 内只读方法 `leaderTerm()`（Ready 循环观察到的当前任期，见 §2 队列语义），SQ1 rebase 后直接复用作为「可写 epoch」的任期来源，不得另建第二个任期缓存；本卡不依赖 SQ1 任何代码。
- `group.go dispatchReady`：本卡加两处——处理 SoftState 前从 `rd.HardState` 更新任期缓存，SoftState 变化时在钩子之后、`lead.Store` 之前切换本任期 ctx。SQ1 若改 leader 对外可写的时机（追平本任期首条后才开门），须保持「任期缓存更新先于 leader 身份对外可见；失去 leader 时任期 ctx 取消先于撤销可见」。入队判定用的是「raw leader + 任期」：SQ1 开门前入队的条目绑定的仍是本任期，出队核对任期相同即可提案——SQ1 的门挡的是 produce 分配 offset，不是提案本身，两者不冲突；若 SQ1 要求队列也按「可写」判定，属 SQ1 改动，由 SQ1 计划决定。
- 本卡不碰 `produce.nextOffsetLocked`、`isLeader`、`InvalidateCounters`。

## 7. 风险

- **R1（最大，残余，已知不闭合）**：任期核对与 `rn.Propose` 之间存在检查—执行间隙；`leaderTerm()`/`gr.lead` 由 Ready 循环更新，滞后于 raft node 内部状态。若 node 内部已失去并**重新赢得**领导权（任期已变）而 Ready 循环尚未观察到，绑任期条目可能进入新任期日志。重新当选至少需要一个选举超时（ElectionTick=10 × 100ms ≈ 1s）且需 group.run 主循环 Step 对端投票（同一循环也在消费 Ready），实际不可达，但形式上未闭合；彻底闭合需 apply 时按条目 term 校验（D5，改日志格式）。注意 SoftState 若被合并（lead 自身→0→自身在同一 Ready 中不显示变化），现有 `InvalidateCounters` 同样不触发，这是既有缺口，记给 SQ1。
- **R2**：proposer 单 goroutine 成为每组提案的串行点。`node.run` 本就单 goroutine 串行消费 propc，原并发 goroutine 也只是在 propc 上排队，额外成本是每条一次 goroutine 交接（微秒级），远低于现吞吐（三节点 3–15k msg/s）对应的预算；以 E3 验证。
- **R3**：队头阻塞——同步入口的未绑任期条目在选举窗口内阻塞 `rn.Propose`，其后条目一并等待。现状下选举窗口内所有提案同样阻塞，不是新退化；绑任期条目在本任期 ctx 取消时立即解阻塞。
- **R4**：`rn.Stop` 从未在 Manager 停机路径调用（manager.go 无调用），proposer 退出必须依赖组生命周期 ctx 取消 `rn.Propose`，不能依赖 `ErrStopped`。第 4.2 第 5 条覆盖。

## 8. 读数与证据

- `cd .../worktrees/sq2 && git log --oneline -3` → `5383c28 test(cluster): SQ2 复现…（红）` / `21ee680` / `9e22965`
- `go test ./internal/core/produce/ ./internal/core/deliver/ -run 'TestSQ2' -count=1 -v`（修复前）→ produce：`跑了 5 轮，累计写入 19840 条，用时 373.400791ms；第一层红 5 轮（逆序共 5062 处），第二层红 1 轮`；`首个 第 1 轮…alloc 64 之后 apply 了 alloc 1，即 offset 63 先于 offset 0 进入 raft 日志`；`第 5 轮…盘上 alloc/=19838，已应答最大 offset=19839，期望 alloc=19840（倒退 2）`；`--- FAIL (2.01s)`
- 同上 deliver：`第 2 轮变红，累计写入 39936 条，用时 2.536046958s`；`28 条应答成功的消息从未被投递（本轮 Receive 631 次，最终 cursor=19968 已越过它们）`；`--- FAIL (4.45s)`
- `rg -n "ApplyAsync\(" --type go -g '!*_test.go'` → 异步调用方 6 处：produce.go:197/301/439，deliver.go:554/669/735
- `rg -n "rep\.Apply\(|cl\.Apply\(" --type go -g '!*_test.go'` → 同步调用方：deliver.go:787/889/917，txn.go:393/467/479/503/513/533，delay.go:233/281/380，retention.go:203，rpc/handle_secret.go:78，replication.go:226（ApplyOrForward），cmd/sq/main.go:696（ForwardApply 落地）
- `rg -n "ProposeReserved\(|\.Propose\(" --type go -g '!*_test.go'` → 生产路径唯一 rn.Propose 点 group.go:1619；Manager.Propose/ProposeReserved manager.go:1600/1613；replication.go:334/358
- `grep -n "propc = nil\|propc = n.propc" ~/go/pkg/mod/go.etcd.io/raft/v3@v3.7.0/node.go` → :374 `propc = n.propc`（有 leader）、:377 / :426 `propc = nil`（无 leader / 不在成员表）
- `sed -n 514,540p .../raft/v3@v3.7.0/node.go` → `stepWithWaitOption`：MsgProp 经 propc 投递，wait=true 时等 `pm.result`
- `grep -n "func (rn \*RawNode) readyWithoutAccept" -A40 .../raft/v3@v3.7.0/rawnode.go | grep "HardState\|SoftState\|asyncStorageWrites"` → :148-150 SoftState 变化即填、:152-153 `if hardSt := r.hardState(); !isHardStateEqual(...)` 即填 HardState、:163 `if rn.asyncStorageWrites` 分支在其后（H2 依据）
- `grep -n "rn.Stop\|\.Stop()" internal/cluster/*.go | grep -v _test` → 仅 ticker.Stop，生产代码无 `rn.Stop`
- `rg -n "ErrNotLeader" internal/rpc/*.go | grep -v _test` → server.go:180 统一映射、receive.go:223、txn.go:50：ErrNotLeader 已有协议面可重试翻译（D1 快速失败不需新翻译）
- `sed -n 659,683p internal/cluster/cluster_test.go` → `TestClusterApplyAsyncOnFollowerReturnsErrNotLeader` 断言 ApplyAsync 不报错、Wait 报 ErrNotLeader（D4 依据）
- `git -C /Users/df-zidong/workspace/sq log --all --oneline | grep -i sq1` → `c0483eb test(cluster): SQ1 复现切主后旧 alloc 覆盖已确认消息（红）`；SQ3 复现用例在 `agent-af9ad1270a441e9d9` worktree 未跟踪文件 `internal/core/deliver/sq3_cluster_race_test.go`
