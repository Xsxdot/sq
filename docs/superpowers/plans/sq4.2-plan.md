# SQ4.2 实现计划：按队列唤醒长轮询，确认 / 改不可见 / 转死信 / 重置位点后也唤醒

- 卡：SQ4.2（父卡 SQ4）｜分支 `cards/sq4`
- 需求：`docs/superpowers/specs/2026-11-11-sq4-perf-batch-design.md`「SQ4.2」节（已批准 2026-11-11）+ 批次红线（不改盘上与线上格式）
- 前置（施工排序，同文件）：SQ4.1 落地后实现，rebase 到其上。SQ4.1 计划 `docs/superpowers/plans/sq4.1-plan.md`（已拍板）把 deliver 改成每队列内存 `queueState`，队列锁仍是每 (group, topic, queue) 一把；本卡**不重复设计内存态**，只在它之上加唤醒。SQ4.1 又排在 SQ1、SQ2 之后，所以本卡实现时 `Router.WaitWritable` 已存在，`produce.InvalidateCounters` 已删。
- 级别：L2。无骨架、无冻结对象。复用的契约：`replication.Replicator`/`Router`（SQ1、SQ2 落地后的版本，不改）、`produce.Producer` 公开写入口 `Append`/`AppendBatch`/`AppendDelay` 的签名（不改）、gRPC 协议（不改）。`produce.Producer.Subscribe` 会被删掉，生产代码里唯一的调用方是 `deliver.go:212`，见 P1。
- 状态：**已拍板（2026-11-11，主会话）**——P1–P3 按推荐（唤醒表归 deliver 新文件，produce 只出「消息可见」监听口；兜底 tick 保留 100ms；ack 类只唤醒同 group×topic×queue）

---

## 待拍板岔口

| # | 岔口 | 推荐 | 被否方案与理由 |
|---|---|---|---|
| P1 | 唤醒原语归谁、依赖方向怎么走 | **deliver 持有唤醒表，produce 只负责报出「(topic, queue) 有新消息可见」**。produce 新增一个可见性监听注册口（`OnVisible` 一类：注册 `func(topic string, q uint32)`，可以注册多个，nil 安全）。`Append`/`AppendBatch` 在 `pending.Wait()` 成功之后、**不持有 produce 任何锁**时逐个调用监听者。`deliver.New` 在构造时把自己的「唤醒该队列全部消费组」注册上去。删掉 produce 的 `wakers`、`Subscribe`、`wakeLocked`。依赖方向保持 deliver → produce，main 不需要改。唤醒表是 deliver 内一个独立结构（`wake.go`），有自己的叶子锁，索引为 (topic, queue) → group → chan，**不放进 SQ4.1 的 `queueState`** | ① 唤醒表仍放在 produce，改成按 (topic, queue) 索引，再导出 `Wake` 给 deliver 在 ack 后调：这样 produce 要么认识消费组（越过它「不做消费可见性判断」的边界，`produce.go:10`），要么 ack 时把同队列所有组都唤醒（一个组 ack，其他组白跑一轮）。② 新建共享包 `core/wake`，由 main 构造后注入两边：`produce.New`/`deliver.New` 的签名都要改，波及 10 处测试装配（读数见末节），收益只是形式上对称。③ 把等待通道挂在 SQ4.1 的 `queueState` 上（SQ4.1 计划第 2 节「对外只读面」提过这个设想）：从 produce 回调里唤醒就得取队列锁，而 `moveToDLQ` 持着源队列锁调 `pr.Append` 写死信 topic，回调再去取死信队列的锁，就成了同时持两把队列锁，违反 SQ4.1 定的锁序「不同时持两把」 |
| P2 | 兜底 tick 要不要改成「按到期堆顶时间定时」 | **不改，保留固定 100ms**。spec 允许保留；本卡的瓶颈（ack 后等 tick）由事件唤醒解决，跟 tick 无关 | 改成 `min(剩余等待, 堆顶到期 − now)`，再给一个更长的上限（比如 1s）：tick 兜的不只是到期这一类事件，还有本卡覆盖不到的可见性来源。新 leader 提交上一任期的条目时，本节点没调过 `Append`，不会发唤醒。拉长上限之前，得先证明这些来源都被覆盖了，超出本卡范围。精度上也没收益：不可见时长是秒级，100ms 的粒度已经够用 |
| P3 | Ack 一类操作唤醒谁 | **只唤醒同一个 (group, topic, queue)**。新消息写入则唤醒该 (topic, queue) 下所有消费组 | ack 时唤醒同队列所有组：别的组的 inflight 和 cursor 不受这次 ack 影响，被唤醒也只是空跑一轮 |

---

## 1. 交付目标

对应 spec SQ4.2 的必交故事 S1–S3，要做到下面四点：

1. 长轮询等待者按 (group, topic, queue) 登记。新消息在某个 (topic, queue) 上可见时，只唤醒这个队列上的等待者（所有组）。
2. 本组在该队列上发生下列状态变化并成功之后，唤醒同一个 (group, topic, queue)：Ack、AckBatch（至少删掉一条才唤醒）、ChangeInvisible、ForwardToDLQ、receiveOnce 内部的超限转死信、ResetCursor，以及 SQ4.1 新增的 `ApplyAdminBatch`（删组后让等待者尽快重装、走到新状态）。
3. 「先订阅、再取件」的防丢唤醒顺序不变。
4. 100ms 兜底轮询不变（P2）。

不做：消费者跨队列合并等待（spec 写明永不做）；tick 改为按到期定时（P2）；rpc 层与 SDK 侧的任何改动。

## 2. 改动边界

### 文件集（超出须升级，见第 5 节）

| 文件 | 改什么 |
|---|---|
| `internal/core/produce/produce.go` | 删 `wakers` 字段、`Subscribe`、`wakeLocked`。加可见性监听：注册方法，加一份监听者列表（写时拷贝 + 原子读，热路径不取 `p.mu`）。`Append` 段 3、`AppendBatch` 末尾把「取 `p.mu` 唤醒」换成锁外逐个调用监听者。包头注释第 6 行「按 topic 的 close-broadcast」、`mu` 注释（:50）、`New` 中的 map 初始化同步改。 |
| `internal/core/produce/produce_test.go` | `TestSubscribeWakesOnAppend`（:110）测的是被删掉的机制，改写为断言 `Append` 与 `AppendBatch` 各触发一次监听，且参数是实际落入的 (topic, queue)。**这是对既有测试的改写，交回物须单列。** |
| `internal/core/deliver/wake.go`（新） | 唤醒表：`subscribe(group, topic, q) <-chan struct{}`、`wakeQueue(topic, q)`（关闭该队列下所有组的通道并移除）、`wakeGroupQueue(group, topic, q)`。自己一把互斥锁，是锁序里的叶子：持它时不取任何别的锁，也不做 I/O。文件头写清职责、边界和锁序。 |
| `internal/core/deliver/deliver.go` | `New` 向 `pr` 注册 `wakeQueue`（`pr == nil` 时跳过）。`Receive` 循环把 `d.pr.Subscribe(topic)` 换成 `d.wk.subscribe(group, topic, queueID)`，位置不变，仍在 `receiveOnce` 之前。把 `tick` 提成包级 `var`（与 `retryBackoffBase` 同一个先例：测试注入，运行期只读）。加一个测试专用的 nil 钩子，记录每一轮取件的起因（首轮 / 被唤醒 / tick），生产恒为 nil，先例是 `afterAppendHook`（:109-112）。在第 1 节第 2 条列出的方法成功返回之前调 `wakeGroupQueue`。包头注释第 9 行与 `Receive` 注释同步改。 |
| `internal/core/deliver/checkpoint.go`（SQ4.1 新增） | `ApplyAdminBatch` 成功后，对它触及的每个 (group, topic, queue) 调 `wakeGroupQueue`。 |
| 测试（新） | `internal/core/deliver/wake_test.go`：不变式单测 W1–W3 与 S2。`internal/core/deliver/sq42_story_test.go`：S1，集群子测试复用 SQ4.1 并入的 `sq3_cluster_race_test.go` 集群装配。 |

明令不碰：`replication` 包、`cluster` 包、`rpc` 包、`cmd/sq/main.go`（P1 下 main 无需改动）、`delay`/`txn`/`admin` 包（它们让消息可见都经 `pr.Append`，自动覆盖，见下表）、盘上键与 gRPC 协议。

### 「让消息可见」的全部写入点（rg 结论，读数见末节）

`msg/` 键只有两处写入：`produce.go:187`（`Append`）和 `:428`（`AppendBatch`）。全部可见路径都收敛到这两个函数：

| 路径 | 落点 | 唤醒由谁发 |
|---|---|---|
| gRPC SendMessage 单条 / 批量 | `rpc/send.go:156` `Append`、`:102` `AppendBatch` | produce 监听 |
| 延时消息已到期直通 | `produce.go:276` `AppendDelay` → `Append` | 同上 |
| 延时搬运 | `delay.go:253` 本地 `Append`；否则 `:258` `ForwardAppend` → leader 侧 `main.go:678` `handleForwardAppend` → `Append` | 落地节点（leader）上的 produce 监听 |
| 事务提交 | `txn.go:234` 本地 `Append`；否则 `:236` `ForwardAppend` → 同上 | 同上 |
| 转死信 | `deliver.go:851` 本地 `Append`；否则 `:865` `ForwardAppend` | 死信 topic 所在 leader 上的监听；死信 topic 的等待者被唤醒 |
| 控制台补发 / 重发 | `admin/messages.go:156,163,247` | 同上 |
| 延时暂存（未到期） | `produce.go:297` 只写 `delay/` | 不可见，不唤醒 |

不经 `Append` 就变得可见的只有一种情况：新 leader 提交上一任期遗留的条目，或者 follower apply 后升主。本节点上没有任何写入调用，所以不会发唤醒，靠 100ms tick 兜底，与现状一致（第 6 节残余）。

### 集群档的唤醒时机

- 新消息：`Append` 只在该队列所属组的 leader 上执行（选队只选本节点 lead 的组，见 `produce.go:145-156`；转发落在 leader 侧）。`pending.Wait()` 返回，表示 quorum 已确认且**本节点 apply 完成**（`replication.go:176-183` 的 `Replicator` 契约），这时被唤醒的等待者读 `msg/` 一定能读到。取件入口 `rpc/receive.go:116` 只在 leader 上放行，等待者和写入者在同一节点。
- 状态变化：Ack 一类调用在 SQ4.1 下只在可写 leader 上成功（`WaitWritable`）。唤醒发在「方法即将成功返回」那一刻：逐次复制模式下是 `Wait` 成功之后，检查点模式下是改完内存、放锁之后。两种模式下这一刻对后来取锁的人来说状态都已可见，所以统一按这一刻发。
- 唤醒一律在**放掉队列锁之后**发：被唤醒的人第一件事就是取同一把队列锁，持锁发唤醒只会制造无谓的锁竞争。

### 锁序（在 SQ4.1 锁序上追加）

`d.mu`（只护 map）→ 围栏 / 毒化等待 → 队列锁 → produce 内部锁 → **唤醒表锁（叶子）**。produce 监听回调在 produce 全部锁之外执行，只取唤醒表锁。`moveToDLQ` 持源队列锁调 `pr.Append`，回调只碰叶子锁，不取死信队列的锁。

## 3. 工作顺序

每一步都是「做 → 检查」，检查不过不进下一步。

0. **基线**（rebase 到 SQ4.1 之前就能做）：在当前分支起点跑 S1 测试的「旧实现等价版」，即 S1 的测量方法原样套在现有 Receive 上（测试文件先写好，断言暂时只打印不判定），记下顺序单队列吞吐，预期 ≤ 20 条/秒，存进末节。
1. **rebase 到 SQ4.1 之上**，核对两条承重假设，任一不成立就停下回报主会话：
   - **H1**：SQ4.1 落地后 `Receive` 仍是「订阅 → receiveOnce → select 等唤醒 / tick / ctx」的循环形态，`receiveOnce` 仍在入口取队列锁、返回前放锁。如果 SQ4.1 把等待挪进了锁内或改了循环结构，本计划第 2 节的唤醒位置要重定。
   - **H2**：rg 确认 `msg/` 的写入仍只在 `produce.go` 的 `Append`/`AppendBatch`（SQ4.1 不应新增写入点），并确认 SQ4.1 的 `ApplyAdminBatch`、`ResetCursor`、`moveToDLQ` 的形态与第 1 节第 2 条一致。
2. **`wake.go` + produce 监听 + `Receive` 改订阅**（Ack 一类先不加唤醒）。检查：W1、W2 绿；`go test ./internal/core/produce/ ./internal/core/deliver/ -count=1` 全绿；S2 绿。
3. **状态变化唤醒**：第 1 节第 2 条逐个方法加上。检查：W3 绿；**S1 单机子测试绿，这是首条故事级检查，绿后立即跑真机首跑项**（第 4 节）。
4. S1 集群子测试（检查点 / 逐次两种模式）。
5. 全量回归（第 4 节「编译与包测试」）、真机清单其余项、注释收口。

## 4. 验收方式

### 故事级检查

| 故事 | 命令 | 判据 |
|---|---|---|
| **S1** | `go test ./internal/core/deliver/ -run 'TestSQ42OrderedAckThroughput' -count=3 -v 2>&1 \| tee /tmp/sq4.2-s1.log` | 四个子测试：{standalone, cluster} × {checkpoint, perop}（模式只改装配）。数据：预写 N=500 条同 MessageGroup 的顺序消息（同一队列）。消费者分两个 goroutine，模拟 SDK「处理当前消息时已经挂着下一个长轮询」：接收者循环 `Receive(maxMsgs=32, invisible=30s, wait=5s)`，把结果交给处理者；处理者收到就 `Ack`。**接收者不等 ack 就发下一次 Receive**，这正是现状要等 tick 的场景。判据：① 500 条全部 ack，无重复、按 offset 有序；② **检查点模式（main 缺省）下吞吐 ≥ 200 条/秒**，单机与集群都要达到；③ 逐次模式只报吞吐数字，另加机制断言：由轮次钩子统计，「被唤醒」起因的取件轮占投出轮数 ≥ 90%（排除 raft 往返本身的耗时，只验唤醒生效）。变异：删掉 Ack 后的 `wakeGroupQueue`，检查点单机子测试必须红（预期回到 ≤ 20 条/秒）。 |
| **S2** | `go test ./internal/core/deliver/ -run 'TestSQ42WakeOnlyTargetQueue' -count=3 -v` | 一个 topic 16 队列。组 g 在队列 1–15 各挂一个长轮询（wait=3s），队列 0 挂一个组 g 与一个组 g2 的长轮询。用 MessageGroup 往队列 0 写 100 条（测试里算出 fnv32a % 16 == 0 的 MessageGroup 串）。轮次钩子按 (group, queue, 起因) 计数。判据：队列 1–15 的「被唤醒」起因轮数 == 0；队列 0 两个组都收到消息（正向对照，防空过）。再追加一段：g 在队列 0 Ack，g2 在队列 0 的「被唤醒」轮数不增加（P3）。变异：把唤醒表索引退回只用 topic，必须红。观测量只统计「被唤醒」起因，tick 起因的轮次不计。 |
| **S3** | 见下方「编译与包测试」与真机清单 | 现有 deliver / rpc / e2e 长轮询用例全绿，**判据与断言一字不改**（`TestSubscribeWakesOnAppend` 的机制改写除外，见第 2 节） |

### 不变式单测（只为承重规则）

- **W1（先订阅再取件，不丢唤醒）**：测试里把 `tick` 设成 1h，关掉兜底，否则丢了唤醒也会被 tick 掩盖。循环 500 次：起一个 `Receive(wait=2s)`，并发地在随机 0–2ms 后 `Append` 一条。断言每次 Receive 都在 500ms 内拿到消息。同一个循环再跑一遍「顺序队列上 Ack 释放顺序锁」的版本。变异：把订阅挪到 `receiveOnce` 之后，必须红（出现等满 2s 的轮）。`-race` 跑一次。
- **W2（键映射）**：produce 监听收到的 (topic, queue) 等于消息实际落入的队列，覆盖 Append、AppendBatch 和 MessageGroup 选队。另一条：死信写入只唤醒死信 topic 的队列 0，不唤醒源队列。
- **W3（状态变化唤醒的作用域）**：第 1 节第 2 条每个方法成功后，同 (group, topic, queue) 的等待者被唤醒，别的组、别的队列不被唤醒。失败路径（attempt 不符、未命中）不要求唤醒，也不能 panic。

### 编译与包测试

- `go build ./... && go vet ./...`
- `set -o pipefail; go test -timeout 15m -count=1 ./internal/... ./cmd/... 2>&1 | tee /tmp/sq4.2-pkgs.log`
- `go test -race -count=1 ./internal/core/deliver/ ./internal/core/produce/`

### 真机清单（本机进程级，`test/e2e`）

1. **首跑项**（步骤 3 S1 单机转绿后立即跑）：`cd test/e2e && go test -tags e2e -count=1 -run 'TestOfficialGoSDKFIFOBlockedUntilAck|TestOfficialGoSDKFIFOOrderedDelivery|TestOfficialGoSDKPushLongPollingWakeup|TestOfficialGoSDKPushFIFOOrderLock|TestOfficialGoSDKDelayDelivery|TestOfficialGoSDKTransactionCommitAndRollback|TestThreeNodeClusterE2E' -v -timeout 40m 2>&1 | tee /tmp/sq4.2-e2e-first.log`。判据：全部 PASS。这几条覆盖顺序消息、push 长轮询唤醒、延时搬运唤醒、事务提交唤醒、集群档。
2. 全量 `make e2e`，全部 PASS。
3. SQ4.1 加的 `TestClusterConsumeThroughput`（`SQ_BENCH=1`）跑一次，与 SQ4.1 存的数字并列报告（不设阈值，只看是否回退）。

须存证据：每条命令末尾的 PASS/FAIL 行；S1 四个子测试的吞吐数字与「被唤醒占比」；步骤 0 的基线数字；S2 各队列的轮次计数表。

## 5. 决策权限

执行者可以自己定（交回物「偏离与自主决定」逐条对照本节）：

- 唤醒表的内部结构、函数命名（含 `OnVisible`、`wake.go` 内各函数）、监听者列表的并发实现。
- 轮次钩子的形态（函数字段还是计数器），前提是生产恒为 nil 或零开销、且不导出。
- `ChangeInvisible` 是每次成功都唤醒，还是只在新到期时刻 ≤ now 时唤醒。
- S1/S2 的具体数据规模与等待时长，在不放宽判据的前提下。
- `TestSubscribeWakesOnAppend` 的改写方式（须在交回物列出）。

必须停下升级：

- 第 3 节步骤 1 的 H1 或 H2 不成立。
- 需要改 `produce.Append`/`AppendBatch`/`AppendDelay`、`deliver.New` 或 `Deliverer` 任何公开方法的签名；需要改 main、rpc、replication、cluster。
- 需要把唤醒挂进 `queueState`、需要在持队列锁时发唤醒，或者出现两把队列锁嵌套。
- S1 检查点模式达不到 200 条/秒（包括只有集群档达不到）；逐次模式「被唤醒占比」< 90%。
- 任何既有长轮询用例需要改断言（`TestSubscribeWakesOnAppend` 除外）。
- 发现除 `Append`/`AppendBatch` 外还有生产代码写 `msg/` 键。

## 6. 风险与残余

- **最大风险：S1 集群档的吞吐被 raft 往返而不是唤醒卡住**。逐次模式下每条消息至少两次提案（取件、确认），本机进程内三节点单次往返如果到了 2–3ms，吞吐就会贴近 200 条/秒。所以硬阈值只放在检查点模式（main 缺省，ack 不提案），逐次模式用「被唤醒占比」验机制。如果检查点模式集群档仍不达标，先看轮次钩子：被唤醒占比高、但单轮耗时长，说明问题在 SQ4.1 取件路径，回报主会话，不在本卡调。
- **前任期条目不发唤醒**：新 leader 提交上一任期遗留的消息时，本节点上的等待者要等 ≤100ms tick。现状如此，本卡不变好也不变坏。
- **唤醒表条目泄漏**：订阅了但一直没被唤醒的条目（等待者超时退出）留在表里，直到下次唤醒才清。规模上限 = 曾被长轮询过的 (group, topic, queue) 数，与现在的 `qmu` map 同量级，可以接受。
- **监听回调在 produce 热路径上**：每条写入多一次原子读加一次叶子锁。现状是每条写入取一次 `p.mu`，开销持平。执行者跑一次 `go test ./internal/core/produce/ -bench AppendParallel -benchtime 2s -count 3`，与改前对比，回退 > 5% 就记进交回物。

## 7. 工作量估计

生产代码约 150–200 行，测试约 400 行，1 个实现会话。

---

## 读数与证据

- `git -C .../worktrees/sq4 log --oneline -1` → `0aa0871 docs(plan): SQ4.1 实现计划（已拍板）`
- `internal/core/produce/produce.go:477-497` `Subscribe`（按 topic 取 / 建 chan）与 `wakeLocked`（close 并删除）；`:56` `wakers map[string]chan struct{}`；`:235-237` `Append` 在 `pending.Wait()` 之后取 `p.mu` 调 `wakeLocked(m.Topic)`；`:454-456` `AppendBatch` 同款；`:10` 包头边界「不做消费可见性判断」
- `internal/core/deliver/deliver.go:207-234` `Receive` 循环：`:212` `d.pr.Subscribe(topic)` 在 `:213` `receiveOnce` 之前；`:224` `tick := 100 * time.Millisecond`；`:228-233` select ctx / wakeCh / `time.After(tick)`
- `internal/core/deliver/deliver.go:109-112` `afterAppendHook` 测试钩子先例；`:56-59` `retryBackoffBase` 包级 var 供测试注入的先例
- `rg -n "Subscribe\(" --type go internal cmd`（排除 pb）→ 生产只有 `deliver.go:212`；测试只有 `produce_test.go:113`（`TestSubscribeWakesOnAppend`，:110）
- `rg -n "MsgKey\(" internal cmd -g '!*_test.go'` → 写入只有 `produce.go:187`、`:428`；其余是读（`deliver.go:378,464,778`、`query.go`、`admin/messages.go:215`）、`retention.go:202` DeleteRange、`snapstream.go:180` 解析
- `rg -n "\.Append\(|\.AppendBatch\(|ForwardAppend" internal cmd -g '!*_test.go'` → `rpc/send.go:102,156`、`delay.go:253,258`、`txn.go:234,236`、`deliver.go:851,865`、`admin/messages.go:156,163,247`、`main.go:678`（`handleForwardAppend` → `pr.Append`）
- `internal/replication/replication.go:176-183` Replicator 契约：集群后端 Apply / Wait 返回时「已 quorum 确认且在本节点 apply 完成」；`:352-360` `Cluster.ApplyAsync` 的 Wait 阻塞到本节点 apply
- `internal/rpc/receive.go:116` `SelfIsLeader` 入口快速失败（取件只在 leader 上长轮询）
- `rg -n "deliver\.New\(|New\(rep, rt, st, mt, pr" --type go` → `main.go:400` 加 9 处测试装配（retention / txn / delay / soak / adminops / rpc / admin×2 / metrics），P1 推荐方案下这些一处都不用改
- SQ4.1 计划 第 2 节「锁序」：`d.mu` → 围栏 / 毒化等待 → 队列锁 → produce 内部锁（DLQ Append），「不同时持两把」；「对外只读面」：给 SQ4.2 不导出任何东西，到期索引在 `queueState` 里；第 3 节步骤 0、11：`TestClusterConsumeThroughput`
- e2e 用例名：`test/e2e/sdk_fifo_test.go:71,115`、`sdk_push_test.go:288,470`、`sdk_delay_test.go:47`、`sdk_txn_test.go:117`；`Makefile:44` `e2e`

### 实现期证据（cards/sq4.2，2026-11-12，本机 macOS；produce 基准在 linux-01）

步骤 0 基线（旧实现 8957215，S1 同法，断言只打印）：

```
[SQ42-S1] TestSQ42OrderedAckThroughput/standalone/checkpoint: 500 条用时 50.607s，吞吐 9.9 条/秒
[SQ42-S1] TestSQ42OrderedAckThroughput/standalone/perop: 500 条用时 50.68s，吞吐 9.9 条/秒
[SQ42-S1] TestSQ42OrderedAckThroughput/cluster/checkpoint: 500 条用时 50.488s，吞吐 9.9 条/秒
[SQ42-S1] TestSQ42OrderedAckThroughput/cluster/perop: 500 条用时 50.861s，吞吐 9.8 条/秒
```

S1（判据已按主会话 2026-11-12 裁定改为「tick 起因占投出轮 ≤ 1%」+ 检查点吞吐 ≥ 200）。非 race，`-count=3`：

```
[SQ42-S1] standalone/checkpoint: 500 条用时 6ms，吞吐 82315.3 条/秒；投出轮 500：被唤醒 498、首轮 2、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/perop: 500 条用时 29ms，吞吐 17147.8 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/checkpoint: 500 条用时 4ms，吞吐 114518.0 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/perop: 500 条用时 78ms，吞吐 6437.9 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/checkpoint: 500 条用时 4ms，吞吐 121089.8 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/perop: 500 条用时 19ms，吞吐 26530.1 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/checkpoint: 500 条用时 4ms，吞吐 121110.6 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/perop: 500 条用时 37ms，吞吐 13693.8 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/checkpoint: 500 条用时 5ms，吞吐 103712.0 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/perop: 500 条用时 16ms，吞吐 31633.8 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/checkpoint: 500 条用时 7ms，吞吐 70110.7 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/perop: 500 条用时 40ms，吞吐 12611.2 条/秒；投出轮 500：被唤醒 498、首轮 2、tick 0（tick 占 0.0%）
ok  	github.com/Xsxdot/sq/internal/core/deliver	12.249s
```

S1，`-race -count=3`：

```
[SQ42-S1] standalone/checkpoint: 500 条用时 54ms，吞吐 9224.6 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/perop: 500 条用时 92ms，吞吐 5409.9 条/秒；投出轮 500：被唤醒 431、首轮 69、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/checkpoint: 500 条用时 52ms，吞吐 9571.2 条/秒；投出轮 500：被唤醒 497、首轮 3、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/perop: 500 条用时 195ms，吞吐 2559.3 条/秒；投出轮 500：被唤醒 429、首轮 71、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/checkpoint: 500 条用时 50ms，吞吐 10026.1 条/秒；投出轮 500：被唤醒 498、首轮 2、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/perop: 500 条用时 88ms，吞吐 5712.2 条/秒；投出轮 500：被唤醒 444、首轮 56、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/checkpoint: 500 条用时 52ms，吞吐 9562.1 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/perop: 500 条用时 192ms，吞吐 2604.8 条/秒；投出轮 500：被唤醒 423、首轮 77、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/checkpoint: 500 条用时 51ms，吞吐 9888.5 条/秒；投出轮 500：被唤醒 499、首轮 1、tick 0（tick 占 0.0%）
[SQ42-S1] standalone/perop: 500 条用时 88ms，吞吐 5681.9 条/秒；投出轮 500：被唤醒 440、首轮 60、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/checkpoint: 500 条用时 53ms，吞吐 9474.1 条/秒；投出轮 500：被唤醒 497、首轮 3、tick 0（tick 占 0.0%）
[SQ42-S1] cluster/perop: 500 条用时 197ms，吞吐 2535.6 条/秒；投出轮 500：被唤醒 417、首轮 83、tick 0（tick 占 0.0%）
ok  	github.com/Xsxdot/sq/internal/core/deliver	14.273s
```

S2 轮次计数（`-count=3`，三次相同）：

```
[SQ42-S2] 队列 1–15 被唤醒轮数全为 0；队列 0：g=1 g2=1
[SQ42-S2] ack 段：g 被唤醒 1→2，g2 被唤醒 1→1
ok  	github.com/Xsxdot/sq/internal/core/deliver	12.434s
```

真机首跑项（02:01:31 → 02:07:34）：

```
--- PASS: TestThreeNodeClusterE2E (125.61s)
--- PASS: TestOfficialGoSDKDelayDelivery (14.10s)
--- PASS: TestOfficialGoSDKFIFOOrderedDelivery (124.19s)
--- PASS: TestOfficialGoSDKFIFOBlockedUntilAck (29.11s)
--- PASS: TestOfficialGoSDKPushFIFOOrderLock (29.24s)
--- PASS: TestOfficialGoSDKPushLongPollingWakeup (27.21s)
--- PASS: TestOfficialGoSDKTransactionCommitAndRollback (9.13s)
ok  	github.com/Xsxdot/sq/test/e2e	361.428s
```

`make e2e`（02:11:46 → 02:46:45）：

```
ok  	github.com/Xsxdot/sq/test/e2e	2097.414s
exit=0
```

`SQ_BENCH=1 TestClusterConsumeThroughput`（同机，base = 8957215）：

```
# base
cluster/quorum-mem/checkpoint-1s            26190 msg/s
standalone/checkpoint-1s                    41325 msg/s
cluster/quorum-mem/perop                    14970 msg/s
standalone/perop                             1158 msg/s
--- PASS: TestClusterConsumeThroughput (192.58s)
# 本卡，第 1 次
cluster/quorum-mem/checkpoint-1s            27065 msg/s
standalone/checkpoint-1s                    35231 msg/s
cluster/quorum-mem/perop                    13615 msg/s
standalone/perop                             1232 msg/s
--- PASS: TestClusterConsumeThroughput (186.63s)
# 本卡，第 2 次
cluster/quorum-mem/checkpoint-1s            27006 msg/s
standalone/checkpoint-1s                    39667 msg/s
cluster/quorum-mem/perop                    14305 msg/s
standalone/perop                             1396 msg/s
ok  	github.com/Xsxdot/sq/test/e2e	201.527s
```

rpc 协议面回归（检查点模式改不可见 / 转死信，SQ4.1 遗留缺陷），8957215 上：

```
8957215 Merge cards/sq4.1: 消费状态内存权威 + 检查点模式（SQ4.1）
    checkpoint_mode_test.go:49: 检查点模式 ChangeInvisibleDuration 应 Code_OK，得 code:INVALID_RECEIPT_HANDLE  message:"receipt 已失效"（err=<nil>）
    checkpoint_mode_test.go:59: 检查点模式 ForwardMessageToDeadLetterQueue 应 Code_OK，得 code:INVALID_RECEIPT_HANDLE  message:"句柄已失效（已确认或已重投）"（err=<nil>）
--- FAIL: TestCheckpointModeChangeInvisibleAndForwardReturnOK (0.07s)
FAIL	github.com/Xsxdot/sq/internal/rpc	0.583s
```
