# SQ4.3 计划：集群档合并提案（一条 MsgProp 带多条 entry，L2）

> 状态：**已拍板（2026-11-11，主会话）**——A1–A3 按推荐（≥2 条走 Step + Status 围栏、单条照走 Propose；围栏不确定整批 ErrNotLeader 并作废该任期；加按节点覆盖二进制的 e2e 夹具，门禁旧二进制取开工 HEAD）。低于基线 95% 即停下回报
> 需求：`docs/superpowers/specs/2026-11-11-sq4-perf-batch-design.md`「SQ4.3」节 + 批次共同红线（不改 raft 日志条目格式、不改线上格式、滚动升级新旧节点互通）
> 硬依赖：SQ2（每组保序提案队列，计划 `/Users/df-zidong/workspace/sq/.claude/worktrees/sq2/docs/superpowers/plans/sq2-plan.md`，已拍板）。本计划按分支 `cards/sq2` 截至 bb0c866 的 `internal/cluster/proposer.go` 出稿；SQ2 合入 `cards/sq4` 前**不得开工**（见工作顺序第 0 步）
> 级别：L2，不出骨架；无新增冻结对象（复用契约见「改动边界」）
> 执行形态：本会话 subagent 实现，独立 subagent 审，主会话验收

---

## 待拍板岔口

| # | 岔口 | 推荐 | 被否方案与理由 |
|---|---|---|---|
| A1 | 多条 entry 怎么一次交给 raft、又怎么知道被丢弃 | **批 ≥2 条走 `rn.Step(ctx, MsgProp{Entries: 批})` + 紧随一次 `rn.Status()` 作「围栏」判定是否进了日志；批只有 1 条时照旧走 SQ2 的 `rn.Propose`（行为与 SQ2 逐字相同）**。依据（读数 R1–R6）：`Node.Propose` 只构造单 entry（node.go:471-473）；公开的多 entry 入口只有 `Node.Step`，它对 MsgProp 走 `stepWithWaitOption(wait=false)`（node.go:475-487、504-506），投递成功即返回 nil、**丢弃错误被吞**；但 node.run 在 `case pm := <-propc` 里同步执行完 `r.Step(m)` 才进入下一轮 select（node.go:386-393），而 `Status()` 经 `n.status` 通道由同一 goroutine 在后续轮次应答（node.go:575-583），故 Status 观察到的一定是「本批已被 Step 处理之后」的状态。leader 侧丢弃整条 MsgProp 只有三种原因：本节点不在成员表、`leadTransferee != None`（raft.go:1304）、超未提交上限（本仓未设 `MaxUncommittedEntriesSize`，raft.go:313-314 置 noLimit，不可达）；非 leader 一律丢弃（stepFollower/stepCandidate，raft.go:1673/1718 起）。且丢弃是**整条消息原子**的（`appendEntry` 全批一次 `increaseUncommittedSize`，raft.go:812 起）。所以围栏判据：Status 显示「RaftState=Leader ∧ Term=本批绑定任期 ∧ LeadTransferee=0 ∧ Progress 含本节点」→ 视为已进日志；否则 → 本批失败（见 A2） | ① **只用 `Step` 不加围栏**：丢弃（典型：摊布转移窗口 `leadTransferee` 非空）时 waiter 只能等到调用方 ctx 超时，SQ2 定下的「丢弃即 ErrNotLeader + 同任期在途作废」失效——违反 spec「失败语义与 SQ2 一致」。② **前 k-1 条走 `Step`、最后 1 条走 `Propose(wait)`**：每批 2 条 MsgProp、2 次 `bcastAppend`，合并收益减半，且「末条成功 ≠ 前批成功」的歧义与围栏相同，没换来任何确定性。③ **RawNode 自驱**（`RawNode.Step` 同步返回错误，rawnode.go）：spec 已列本期不做。④ **改条目格式（一个 entry 装多个提案）**：红线禁止 |
| A2 | 围栏判为「不确定」（Status 不再是同任期 leader，或 LeadTransferee 非 0）时怎么回 | **整批立即以 `ErrNotLeader` 失败、移除各自 waiter，并把该任期作废（复用 SQ2 `voidMark`）**。这与 SQ2 对「raft 丢弃」的处理同语义；代价是「其实已进日志、后来被新 leader 提交」的条目也报 `ErrNotLeader`——与现行 `classifyTimeout`「等待期间 leader 换人即报 ErrNotLeader，条目仍可能提交」是同一类既有歧义（SDK 重试，至少一次语义），不破坏 SQ2 的前缀安全论证（失败条目若实际进了日志，不产生空洞；旧任期条目进新任期的路径仍由出队任期核对挡住）。执行者须在第 1 步复核 core 调用方无一依赖「ErrNotLeader ⇒ 一定未进日志」（读数 R9 已初查 produce.go:209-213 只依赖「失败不推进成功」） | 不确定时保留 waiter、照常等 apply：丢弃场景退化为「等满调用方 ctx 才失败」，切主/转移期间写延迟从毫秒级变为 ctx 预算（秒级），属可感知回退 |
| A3 | S2「新旧节点混跑的现有 e2e 用例」用什么跑——仓内**没有**混跑夹具（读数 R10：`brokerBinary` 全进程唯一，`SQ_E2E_BROKER` 只能整体替换） | **给 `test/e2e/cluster_proc_test.go` 的 `procCluster.spawn` 加最小的逐节点二进制覆盖**：环境变量 `SQ_E2E_BROKER_ALT=<旧二进制路径>` + `SQ_E2E_ALT_NODES=<逗号分隔的 1 起节点号>`，命中的节点用旧二进制起进程（重启沿用）；不设则行为不变。用它跑现有进程级场景用例（见验收 §4.4）。旧二进制两份：**门禁** = 本卡开工时的 `cards/sq4` HEAD（SQ2 已合入、无 SQ4.3，隔离本卡变量）；**参考** = `v0.1.0`（真实上一发布，混入了其它 SQ4 卡的变化，红了只回报不归因） | 只给「构造上不改格式」的论证不跑：spec 明文要求「现有 e2e 用例全绿」。论证仍写进交回物作为第二证据：`DisableProposalForwarding: true`（group.go:378）使 MsgProp 永不出本节点；线上只有 MsgApp，而「一条 MsgApp 带多条 entry」今天就是常态（raft 按 `MaxSizePerMsg`=1MiB 打包未发送条目，group.go:374）；盘上 seglog 按 `rd.Entries` 逐条写帧，与提案怎么进来无关 |

拍板后逐条回写本节，状态行改「已拍板（日期）」。

---

## 1. 交付目标

对应 spec SQ4.3 的 S1、S2：

1. SQ2 的 proposer 每次被唤醒时，把**已经排队**的、可合并的条目（见 §2 合批规则）用一条 MsgProp 交给 raft；不为攒批而等待——队列里只有 1 条时立即按 SQ2 原路径提案，低负载延迟不变。
2. raft 日志条目格式、提案头（`ProposalHeaderLen`=16，`[8B 提案者][8B waiter id]`）、apply 侧、seglog 帧格式**全部不变**；每条 entry 仍各有一个 waiter。
3. 失败语义与 SQ2 一致：同组 FIFO；出队任期核对；丢弃（含围栏判不确定）→ `ErrNotLeader` 并作废同任期在途条目；条目 ctx 先于处理取消 → 只失败它自己。
4. S1：`TestClusterWriteThroughput` quorum-mem 并发 64/256 的吞吐与基线（本卡开工时 HEAD）对比，数字进卡；不设硬阈值，无提升则如实记录原因。
5. S2：SQ2 保序回归测试全绿；混跑用例全绿。

**不做**：改条目格式、RawNode 自驱（spec Out of Scope）；SQ1 的可写门；apply 侧合并簿记。

## 2. 改动边界

### 文件集（白名单，超出即升级）

| 文件 | 改什么 |
|---|---|
| `internal/cluster/proposer.go` | `runProposer` 取走一批后按合批规则切段；新增「批提案」处理（核对 → 逐条回填头与注册 waiter → `rn.Step` → 围栏 → 逐条交结果）；单条段仍调现有 `proposeOne`；新增两个未导出原子计数（批次数、批内条目数）供同包测试观测；文件头注释补「合并提案」一节与承重假设 H3–H6 出处 |
| `internal/cluster/proposer_test.go` | 新增 §4.2 不变式单测；不改 SQ2 已有用例断言 |
| `test/e2e/cluster_proc_test.go` | 仅 A3 的逐节点二进制覆盖（`spawn` 选二进制处 + 文件头注释一句）|

**明令不碰**：`group.go`（SQ1 在改 `dispatchReady`/`applyOnce`）、`manager.go`、`replication.go`、提案头布局、`applyEntries`、SQ2 的入队状态表与 `leaderTerm()`/`switchEpoch`、两份 `sq2_*_test.go`、`internal/core/*`。

### 合批规则（执行者照此实现，数据结构自定）

proposer 一次取走的 `batch` 按 FIFO 顺序切成连续段，**段内顺序即日志顺序**：
- 只有「绑任期、同一任期、条目 ctx 未取消、通过出队任期核对」的条目可进同一合并段；遇到未绑任期条目（同步入口在 lead==0 时入队的）或任期不同的条目即断段，后者单独走 `proposeOne`。
- 段内条目按序先做 SQ2 第 1、2 步核对；核对失败的条目就地失败（与 SQ2 同），**并把段在此处断开**（失败条目之后的条目另起一段），保证「交给 raft 的每条 MsgProp 内条目在队列里连续」。
- 段长 1 → `proposeOne`（SQ2 原路径，含 `rn.Propose` wait=true）。段长 ≥2 → 批提案。
- 单条 MsgProp 的载荷总字节上限 = `MaxSizePerMsg`（1MiB，group.go:374）；超过即断段，单条超限的条目自成一段（走单条路径）。

批提案的提案 ctx = 本任期 ctx（`epochState.ctx`，已是生命周期 ctx 的子 ctx）。各条目自身的 ctx 不并入 `Step`：`Step` 在有 leader 时只阻塞到 node.run 接收，ctx 只在 propc 为 nil（raft 内部已无 leader）时起作用，此时本任期 ctx 会被 Ready 循环取消。

结果归类（对整段生效）：

| 情形 | 处理 |
|---|---|
| `Step` 返回错误且生命周期 ctx 已结束 | 全段停止错误（`stoppedErr`），移除 waiter |
| `Step` 返回错误且本任期 ctx 已取消 | 全段 `ErrNotLeader`，`voidMark.mark(该任期)`，移除 waiter |
| `Step` 返回 nil，围栏判「已进日志」（A1 判据） | 全段 resolve nil（交「已进日志」，不等 apply），放弃等待者的 waiter 照 SQ2 第 5 步移除 |
| `Step` 返回 nil，围栏判不确定 | A2 |

### 真实入口

写热路径 `produce.Append/AppendBatch/AppendDelay` 与消费路径 `deliver.receiveOnce/AckBatch/changeInvisibleLocked` → `replication.Cluster.ApplyAsync` → SQ2 异步入口 → 组队列 → **proposer（本卡）** → raft。同步路径 `Cluster.Apply` → `ProposeReserved` → 同一队列。

### 架构方向与契约

合批是 proposer 的内部策略，留在 `internal/cluster`；依赖方向不变。无新增冻结对象。复用：`replication.Replicator`（Apply/ApplyAsync 语义不变）、`cluster.ErrNotLeader`、提案头 `ProposalHeaderLen`、SQ2 的 `ProposalPending.Wait` 契约、go.etcd.io/raft/v3 v3.7.0 的 `Node.Step`/`Node.Status` 既成行为（读数 R1–R6）。

## 3. 工作顺序

每步「做 → 检查」，不过不进下一步。

0. **前置核对**：`git merge-base --is-ancestor <SQ2 合入 cards/sq4 的合并提交> HEAD`，且 `internal/cluster/proposer.go` 存在、含 `voidMark`/`proposeOne`/`epochState`。不满足 → 停，回报主会话。记下 `BASE=$(git rev-parse HEAD)`（基线与门禁旧二进制都取它）。
1. **钉承重假设**（读 `$(go env GOMODCACHE)/go.etcd.io/raft/v3@v3.7.0/`，结论连行号写进 proposer.go 文件头）：
   - H3：`Node.Step` 对 MsgProp 是 wait=false，返回 nil 时 node.run 已取走该消息且其 `r.Step` 在本轮 select 内同步完成（node.go:475-487、504-540、386-393）。
   - H4：之后发起的 `Status()` 由 node.run 在后续轮次应答，观察到的状态晚于本批处理（node.go:575-583）。
   - H5：一条 MsgProp 在 leader 侧要么整批 append、要么整批丢弃（raft.go:1294-1352 stepLeader、:812 appendEntry）；本仓 `MaxUncommittedEntriesSize` 未设（group.go raftConfig）。
   - H6：MsgProp 永不出本节点（`DisableProposalForwarding: true`，stepFollower 丢弃而非转发）。
   - 同时复核 A2 的前提：`rg -n "ErrNotLeader" internal/core` 每处调用方不依赖「ErrNotLeader ⇒ 一定未进日志」。
   H3 或 H4 不成立 → 停，回报（A1 整个依赖它）；A2 前提不成立 → 停，回报。
2. **测基线**（实现前，避免后补）：按 §4.3 E3 在 `BASE` 二进制上跑 3 遍，记中位数。
3. **实现合批与批提案**（§2）。检查：`go build ./... && go vet ./internal/cluster/`；`go test ./internal/cluster/ -count=1` 全绿（SQ2 既有 proposer_test 不回归）。
4. **首条故事级检查**：§4.1 第 1 行 TestSQ2 两条各 1 次绿 → **立即跑真机首跑项 E1**（需先做第 5 步的夹具，夹具很小，可与本步并行完成）。
5. **加混跑夹具**（A3）。检查：不设两个环境变量时 `cd test/e2e && go vet -tags e2e ./...` 通过且 E2 中任一用例照常绿。
6. **不变式单测 + 变异验证**（§4.2）。
7. **全量与稳定性**（§4.1 剩余行、§4.3）。
8. **真机清单剩余项**（§4.4）与 S1 数字。

## 4. 验收方式

### 4.1 故事级检查

| 故事 | 检查 | 判据 |
|---|---|---|
| S2 保序不回退 | `go test ./internal/core/produce/ ./internal/core/deliver/ -run 'TestSQ2' -count=1 -v` | 两条均 PASS（produce「第一层红 0 轮」、deliver「未复现」） |
| S2 保序稳定 | `go test ./internal/core/produce/ ./internal/core/deliver/ -run 'TestSQ2' -count=10 -timeout 60m` | 10 次全 PASS |
| S2 混跑 | §4.4 E1、E2 | 全 PASS |
| S1 吞吐 | §4.4 E3 | 前后各 3 遍中位数进卡；同时进卡：E3 期间 leader 侧「平均批长」（第 6 步计数器之比，执行者在 E3 结束时经一次性 Info 日志或测试读取取得，不得为此新增生产对外接口） |

### 4.2 不变式单测（`internal/cluster/proposer_test.go`）

1. **合批真的发生且保序**：单节点组（`startSingleNodeGroup`），64 goroutine 在外部互斥锁下取递增序号、构造携序号载荷、调异步入口、锁外 Wait；断言 apply 序列严格递增、全部 Wait 为 nil，且批次计数 >0、批内条目数 / 批次数 >1。反例：把批提案改为「段内逐条 `proposeOne`」→ 批次计数断言红；把 proposer 改为每段 `go` 出去 → 保序断言红。
2. **批丢弃快速失败、不进日志**（A1/A2 承重）：三节点 `newTestCluster`（cluster_test.go:852），先 `tc.kill` 一个非 leader 节点（:1120），再对组 0 的 leader 调 `Manager.TransferLeader`（manager.go:1791 附近）转给这个已死节点，使 `leadTransferee` 挂起（raft 约一个选举超时后才中止，见 manager.go 摊布注释「转移挂起期内丢弃全部提案」）；窗口内一次性入队 ≥8 条绑任期条目；断言：全部 Wait 在 2s 内返回 `ErrNotLeader`（远小于条目 ctx 的 30s），各节点 store 从未出现这些载荷，批次计数 ≥1（证明走的是批路径而非单条路径）。若构造不稳定，降级为包内对「批提案单步函数」注入假 `step`/`status` 的表驱动测试（须在交回物说明降级理由）。反例：去掉围栏、`Step` 成功即视为已进日志 → 本条红（Wait 拖到 ctx 超时）。
3. **任期变化后批内条目不提案**：沿用 SQ2 第 2 条用例的构造（入队后换届再放行），条目数 ≥8 以确保落进合并段；断言同 SQ2（全部 `ErrNotLeader`、无载荷落盘、新任期条目照常成功）。
4. **段内单条 ctx 先取消只失败它自己并断段**：入队 a、b（ctx 已取消）、c、d，放行；断言 b 得 `context.Canceled`，a、c、d 成功且 apply 顺序 a<c<d。
5. **字节上限断段**：入队若干载荷合计 >1MiB 的条目，断言全部成功、顺序不变、批次计数 ≥2。

变异验证（执行者亲手做，结果写交回物）：去掉围栏 → 第 2 条红；合批时不检查任期一致 → 第 3 条红；段内失败不断段而跳过 → 第 4 条顺序断言仍绿但需另加断言「同一 MsgProp 内条目在队列中连续」（执行者用计数器或测试钩子实现，不得导出）；每段 `go` 出去 → 第 1 条与 TestSQ2 红。

### 4.3 全量编译与触及包

```
go build ./... && go vet ./internal/cluster/
go test ./internal/cluster/ ./internal/replication/ ./internal/core/... ./internal/rpc/ ./cmd/... -count=1
go test -race ./internal/cluster/ -count=1
go test -race ./internal/core/produce/ ./internal/core/deliver/ -run TestSQ2 -count=1
```
收尾跑一次 `go test ./... -count=1` 作证据。

### 4.4 真机清单（本机三进程 e2e；`S` = 执行者会话 scratchpad 下的 `sq43/` 目录）

旧二进制构建（第 0 步之后、改代码之前做一次）：
```
mkdir -p $S/base $S/v010
git archive $BASE | tar -x -C $S/base && (cd $S/base && go build -o $S/sq.base ./cmd/sq)
git archive v0.1.0 | tar -x -C $S/v010 && (cd $S/v010 && go build -o $S/sq.v010 ./cmd/sq)
```

| # | 命令（均在 `test/e2e` 下） | 判据 |
|---|---|---|
| **E1（首跑项）** | `SQ_E2E_BROKER_ALT=$S/sq.base SQ_E2E_ALT_NODES=1 go test -tags e2e -count=1 -run 'TestScenarioRollingRestart' -v -timeout 20m` | PASS；并从三个节点 `broker.log` 的 leader 变更日志行取证：至少一个组由旧二进制节点领导、至少一个组由新二进制节点领导（摊布使两方向复制都被覆盖）；取证不满足则换 `SQ_E2E_ALT_NODES=3` 重跑一次 |
| E2 | `SQ_E2E_BROKER_ALT=$S/sq.base SQ_E2E_ALT_NODES=2,3 go test -tags e2e -count=1 -run 'TestScenario(KillLeaderHard\|RollingRestart\|ProducerExitsRightAfterCommit\|MinorityCannotWrite)' -v -timeout 40m`，再以 `SQ_E2E_ALT_NODES=1` 跑同一组 | 全 PASS（旧多数派 / 新多数派两种形态） |
| E2' | 同 E2，`SQ_E2E_BROKER_ALT=$S/sq.v010` | 参考项：记录结果；红了查日志，能归因到本卡才阻塞，否则回报主会话 |
| E3 | 基线：`SQ_E2E_BROKER=$S/sq.base SQ_BENCH=1 SQ_BENCH_CONC=64,256 SQ_BENCH_MSGS=100000 go test -tags e2e -count=1 -run 'TestClusterWriteThroughput/cluster-quorum-mem' -v -timeout 40m`；本卡：去掉 `SQ_E2E_BROKER` 同命令；各 3 遍 | 汇总行 `cluster/quorum-mem/conc=64`、`conc=256` 的 msg/s、p50、p99 前后中位数进卡；本卡低于基线 95% → 停，带数字回报主会话，不自行调优 |
| E4 | `go test -tags e2e -count=1 -run 'TestThreeNodeClusterE2E' -v -timeout 20m` | PASS（kill leader 后「允许重复不允许丢」对账） |

须存证据：每条命令末尾 PASS/FAIL 行、E1 的 leader 分布取证行、E3 汇总行，贴交回物。

## 5. 决策权限

执行者可自定（交回物逐条列出）：段切分的数据结构；批内 waiter 注册与回填的循环写法；计数器命名；混跑夹具的变量解析细节（变量名按 A3 不变）；§4.2 第 2 条的降级；E3 的 `SQ_BENCH_MSGS` 若 100000 单遍超过 10 分钟可降到 50000（前后一致）。

必须升级（停下回报主会话）：
- 第 0 步前置不满足；第 1 步 H3/H4 或 A2 前提不成立；
- 需要改白名单外文件（尤其 group.go、manager.go、提案头、apply 侧）；
- 想让未绑任期条目、或不同任期条目进同一合并段；想在批路径上去掉围栏或改 A2 的回法；
- 任何既有测试要改断言才能过；
- E3 低于基线 95%；E1/E2 红且能归因到本卡；
- 发现 raft 侧对多 entry MsgProp 有本计划未列的丢弃/截断行为。

## 6. 与 SQ1 / SQ2 / 其它卡的接触面

- **SQ2**：本卡只改 proposer 的「取走一批之后」那段；入队状态表、任期绑定与 `leaderTerm()`、`switchEpoch`、`Wait` 两段式全部原样复用。SQ2 的残余 R1（任期核对与提案之间的间隙）在批路径上同样存在、不放大：围栏只会把「不确定」判为失败。
- **SQ1**：SQ1 改 `group.go` 的 `dispatchReady`/`applyOnce`/`installSnapshot` 与 `manager.go` 新增 `WaitWritable`，与本卡文件集不重叠；SQ1 的门挡 produce 分配 offset，不挡提案本身，与合批无交互。
- **SQ4.10**：改 `manager.go`/`group.go`/seglog，与本卡文件集不重叠。

## 7. 风险

- **R1（最大）合并收益可能被围栏成本吃掉或根本不在瓶颈上**：每个 ≥2 的批多一次 `Status()`（分配 Progress 副本与 Config 克隆，status.go:68-75）；且 apply 侧每 entry 的簿记与 waiter 不变（spec 要求如此）。若 E3 无提升，交回物须给出 CPU profile（`go test` 起的 broker 进程可开 pprof 的话采之；否则记录批长分布）说明瓶颈在哪，按 spec「如实记录、不硬凑」。
- **R2 围栏误判「已进日志」**：只在「本批处理时 `leadTransferee` 非空、Status 前恰好一次 tick 中止转移」这一窗口发生；后果是这几条等到 ctx 超时后按 `classifyTimeout` 归类，不影响安全。
- **R3 围栏误判「不确定」而条目实际提交**：客户端收到 `ErrNotLeader` 重试导致重复，属既有至少一次语义（A2）。

## 8. 读数与证据

- R1 `sed -n 471,487p .../raft/v3@v3.7.0/node.go` → `func (n *node) Propose(...) { return n.stepWait(ctx, &pb.Message{Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Data: data}}}) }`；`func (n *node) Step(...)`：非本地消息 → `n.step(ctx, m)`
- R2 `sed -n 504,540p node.go` → `step` = `stepWithWaitOption(ctx, m, false)`；wait=false 时 `case ch <- pm: if !wait { return nil }`，`pm.result` 为 nil，丢弃错误无从回传
- R3 `grep -n "case pm := <-propc" node.go` → `386`；其下 `m.From = new(r.id); err := r.Step(m); if pm.result != nil {...}`（同步执行后才回到 select）
- R4 `grep -n "func (n \*node) Status" -A8 node.go` → `575: c := make(chan Status); select { case n.status <- c: return <-c ...}`
- R5 `raft.go` stepLeader MsgProp：`:1304 if r.leadTransferee != None { ... return ErrProposalDropped }`；`Progress[r.id]==nil → ErrProposalDropped`；`if !r.appendEntry(m.GetEntries()...) { return ErrProposalDropped }`；`appendEntry`（:812）对全批一次 `increaseUncommittedSize`；`:313-314 if c.MaxUncommittedEntriesSize == 0 { ... = noLimit }`
- R6 stepFollower（:1718）MsgProp：`r.lead == None → ErrProposalDropped`；`r.disableProposalForwarding → ErrProposalDropped`；stepCandidate（:1673）MsgProp 直接 `ErrProposalDropped`
- R7 `grep -n "MaxSizePerMsg\|MaxInflightMsgs\|DisableProposalForwarding:\|AsyncStorageWrites:\|MaxUncommittedEntriesSize" internal/cluster/group.go`（cards/sq4）→ `374 MaxSizePerMsg: 1 << 20`、`375 MaxInflightMsgs: 256`、`378 DisableProposalForwarding: true`、`395 AsyncStorageWrites: true`；无 `MaxUncommittedEntriesSize`
- R8 `git show cards/sq2:internal/cluster/proposer.go`（bb0c866）→ `runProposer` 已「一次取走全部就绪条目再逐条提案」（注释「为后续合并卡留口子」）；`proposeOne` 五步；`voidMark{term, upTo}`；`mergeCtx`；生产唯一 `rn.Propose` 点在 proposeOne
- R9 `rg -n "ErrNotLeader" internal/core | grep -v _test` → produce.go:142/155/209-213/386/399、txn.go:144/172/176/228、delay.go:313/341、deliver.go:845、retention.go:173；produce.go:209-213 注释只依赖「非 leader 期间提案必被拒、不存在缓存已烧却返回成功」
- R10 `grep -n "brokerBinary\|SQ_E2E_BROKER" test/e2e/*.go` → sdk_test.go:90 `var brokerBinary string`、:116 `SQ_E2E_BROKER` 整体替换；cluster_proc_test.go:103 `exec.Command(brokerBinary, ...)`；无逐节点二进制机制
- R11 `grep -n "func startBenchCluster" -A8 test/e2e/sdk_cluster_bench_test.go` → 走 `startProcCluster`（吃 A3 夹具）；`TestClusterWriteThroughput` 子测试名 `cluster-quorum-mem`/`cluster-quorum-fsync`/`standalone`
- R12 `git tag` → 唯一发布 `v0.1.0`（f31582d）
- R13 spec 基线：`cluster/quorum-mem/conc=64  14696 msg/s  p50=2.592ms  p99=46.846ms`（main@21ee680，非本卡基线，仅作量级参照）
