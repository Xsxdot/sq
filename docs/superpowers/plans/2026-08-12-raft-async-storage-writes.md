# raft 异步存储写入（AsyncStorageWrites）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 打开 `go.etcd.io/raft/v3` 的 `AsyncStorageWrites`，把 `group.handleReady` 的「持久化 → 发送 → apply → Advance」串行四步拆成「主循环分发 + append/apply 两条本地存储协程」，让 leader 的 `MsgApp` 不再被自己的 fsync 挡住、同一组多轮 Ready 可在途，从而攻掉 quorum-fsync 档 +69% 的 raft 机制税。

**Architecture:** raft 库在 async 模式下把日志写入与状态机应用表达成 `MsgStorageAppend` / `MsgStorageApply` 两条**本地消息**（`m.To` 分别是 `raft.LocalAppendThread` / `raft.LocalApplyThread`），随 `rd.Messages` 一起交下来；每条本地消息携带一组 `Responses`，由存储侧在写入完成后投递回 raft。主循环只做分发：网络消息立即外发，两条本地消息可靠入队（满则阻塞，绝不丢），随后立刻回去取下一轮 Ready——`Advance` 从此不存在（在 async 模式下调用会永久阻塞）。

**Tech Stack:** Go 1.26.1，`go.etcd.io/raft/v3 v3.7.0`（原生支持 async，**不 fork**），`internal/cluster/seglog`（日志物理层，本次不碰），Pebble（FSM）。

## Global Constraints

以下每一条都来自设计文档 `docs/superpowers/specs/2026-08-12-raft-async-storage-writes-design.md`，逐 task 隐含生效：

- **raft 语义与确认档语义零变化。** `AckQuorumFsync` / `AckQuorumMem` 的持久性等位不变；`MustSync` 判定语义不变，只是载体从 `Ready.HardState/Entries` 变成 `MsgStorageAppend`（spec §4）。
- **彻底切换，不留配置开关。** 只保留 async 一条路径；两套路径长期共存必然腐化（spec §4）。
- **本地消息绝不能走 `gr.send`。** `Manager.send`（`internal/cluster/manager.go:773-786`）对自指消息走 `gr.step` → `inbox`，而 `inbox` 在安装期是**显式丢弃**、组退出时也丢弃。丢一条 `MsgStorageAppendResp` = raft 永远等不到的那一条 = **组静默卡死且无任何报错**（spec §5.1）。
- **append 阶段的严格顺序**：`rs.Persist` → `mem.SetHardState`/`mem.Append` → 投递 `m.Responses`。任何一步提前，raft 都会读到还不存在的日志（spec §5.2）。
- **durable 要求 append 严、apply 松**：`MsgStorageAppend` 的写入必须先于 Responses 投递而 durable；`MsgStorageApply` 不要求。**不得因为「保险起见」把 apply 也做成同步**（spec §5.3）。
- **不碰 `seglog`、不碰帧格式、不改截断/成员变更/读屏障的语义**（spec §3 非目标）。
- **不做共置优化**——共置税是 CPU 争抢，写部署文档，不写代码（spec §3）。
- **日志用注入的 `gr.lg`（`*slog.Logger`），禁止 `fmt.Printf`。** 稳态热路径（每轮 Ready 分发、每次 append/apply 成功）零日志，只在阈值越界与阶段起停时打点。
- 中文注释；新文件必须有文件头注释（职责 + 边界），导出与非平凡的非导出函数必须有 doc comment。
- **验收锚**：`test/e2e/sdk_cluster_test.go` 全量零修改通过；`internal/cluster` 现有单测除**时序假设确实变更**的以外零修改通过，每一处修改在本计划里都有单独的理由说明（本计划共预告 **2 处**，见 Task 2 Step 8）。

---

## 背景速查（实现者不必回头翻 spec 就能干活）

**为什么现在深度恒为 1**：`handleReady` 严格串行（`group.go:396` 持久化 → `:417` 发送 → `:446` apply → `:532` Advance），而 `node.run` 在非 async 模式下投出 Ready 后必须等 `advancec` 才产下一轮（`raft/v3@v3.7.0/node.go:435-441`）。于是 leader 做 fsync 的那 1.8ms 里 `MsgApp` 一个字节都发不出去，确认链是 `leader fsync → 网络 → follower fsync → 网络 → commit`，两次 fsync 串行相加。

**raft 契约允许 leader 在自己 fsync 完成前就发 `MsgApp`**——leader 只需在 **commit** 前 durable，不需在 **replicate** 前 durable。`AsyncStorageWrites` 就是为此设计的（`raft/v3@v3.7.0/raft.go:151-184`）。

**收益的两层**：① leader 与 follower 的 fsync 并行而非串行相加；② 同一组多轮 Ready 在途后 raft 自然攒出更大的 `Entries` 批——**组内合批是副产品，不需要手写**。

**async 模式下 Ready 各字段的归属（务必记住）**：

| Ready 字段 | async 下怎么处理 |
|---|---|
| `Entries` / `HardState` / `Snapshot` | **不要直接用**——它们已被复制进 `MsgStorageAppend`；直接用会双写 |
| `CommittedEntries` | **不要直接用**——已被复制进 `MsgStorageApply`；直接用会双 apply |
| `Messages` | 唯一入口：按 `m.GetTo()` 三分派 |
| `MustSync` | **仍然要用**：它是本轮 `MsgStorageAppend` 是否需要 fsync 的判据，必须与那条消息配对传给 append 阶段 |
| `ReadStates` / `SoftState` | 照旧在主循环处理（不涉及存储写入） |

（依据：`raft/v3@v3.7.0/rawnode.go:163-176` 的 `readyWithoutAccept`，async 分支把 `newStorageAppendMsg` / `newStorageApplyMsg` 追加进 `rd.Messages`。）

**`Advance` 必须删除**：`node.Advance()` 往 `n.advancec` 发送（`node.go:555-560`），而 async 模式下 `node.run` 的 `advancec` 永远为 nil、那个 case 永不触发（`node.go:435-441`）——调用即永久阻塞到节点 Stop。

---

## File Structure

| 文件 | 动作 | 职责 |
|---|---|---|
| `internal/cluster/group.go` | 修改 | 主循环分发 + 两条本地存储阶段；`handleReady` 消失，`dispatchReady`/`runAppend`/`runApply`/`appendOnce`/`applyOnce`/`deliverResponses`/`enqueueLocal` 取而代之 |
| `internal/cluster/group.go`（`raftConfig`） | 修改 | 打开 `AsyncStorageWrites: true`（单点开关，所有装配路径与测试自动跟随） |
| `internal/cluster/group_async_test.go` | 新建 | async 专属用例：分发路由、响应可靠投递、MustSync 载体、流水线深度、install/apply 互斥 |
| `internal/cluster/group_test.go` | 修改（2 处） | 两个快照用例的入口从 `handleReady` 改为 `appendOnce`（理由见 Task 2 Step 8） |
| `docs/deploy/*`（若存在容量/部署章节） | 修改 | 无——本改造不改部署形态 |

不新建 package：所有改动落在 `internal/cluster` 内，`raftStore` / `seglog` / `manager` 的接口一个不动。

---

## Task 1: 抽出 persist 阶段与 apply 阶段（行为零变化的纯重构）

把 `handleReady` 里两段将来要搬到独立协程的逻辑先原样抽成方法。这一步**不改任何行为**，目的是让 Task 2 的 diff 只剩「谁来调用」而不夹杂「调用什么」。

**Files:**
- Modify: `internal/cluster/group.go:393-411`（持久化 + 双记账）、`internal/cluster/group.go:433-530`（apply 循环）
- Test: `internal/cluster/group_test.go`（现有用例即回归网，不新增）

**Interfaces:**
- Produces:
  - `func (gr *group) persistPhase(hs *raftpb.HardState, ents []*raftpb.Entry, sync bool)` —— 失败时 panic（fail-stop，语义不变）
  - `func (gr *group) applyPhase(ents []*raftpb.Entry) []ccApplied` —— 返回本批登记的成员变更 waiter（通知由调用方在正确时机做）
- Consumes: 无（首个 task）

- [ ] **Step 1: 先跑一遍基线，确认起点是绿的**

```bash
go test ./internal/cluster/ -count=1
```

期望：PASS。若此处已红，**停下来提问**——`internal/cluster` 有一条已知偶发失败（backlog B12「满负载偶发失败一次，用例未定位」），先重跑两次确认是否可复现，别把 B12 记到本次改动头上。

- [ ] **Step 2: 抽出 `persistPhase`**

在 `group.go` 里 `syncPersist` 方法**之前**插入：

```go
// persistPhase 执行「日志持久化 + MemoryStorage 双记账」，是 raft 存储
// 契约里 append 侧的全部写入动作。
//
// 参数：
//   - hs: 本轮的 HardState，nil 或空表示无状态变更
//   - ents: 本轮要追加/覆盖的日志条目，可为空
//   - sync: 本次写入是否带 fsync（quorum-fsync 档的 MustSync 轮）
//
// 顺序即不变量（spec §5.2）：先 rs.Persist 落盘，再推 mem（raft 库读
// 日志的易失视图）。调用方必须在本方法返回**之后**才投递响应给 raft
// ——响应一旦投出，raft 就认为这些条目已 stable 并会立刻去 mem 读它们。
//
// 失败一律 panic（fail-stop）：持久化失败 = 内存日志与磁盘分叉，崩溃后
// 本节点已确认的条目会消失。记 Error 后静默返回只会让组永久卡死，比
// 进程死亡更糟——进程死亡由上层重启接管（走不干净关机判定）。
func (gr *group) persistPhase(hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) {
	if err := gr.rs.Persist(gr.g, hs, ents, sync); err != nil {
		gr.lg.Error("日志持久化失败，组停摆", "entries", len(ents), "sync", sync, "err", err)
		panic(err)
	}
	if !raft.IsEmptyHardState(hs) {
		_ = gr.mem.SetHardState(hs)
		// 缓存本轮的 term（currentTerm 的数据源）：空 HardState 的轮次
		// 不进这里，lastTerm 因此永远停留在「最近一次真实任期」
		gr.lastTerm.Store(hs.GetTerm())
	}
	_ = gr.mem.Append(ents)
}
```

- [ ] **Step 3: 抽出 `applyPhase`**

在 `persistPhase` 之后插入（函数体从 `handleReady` 的 `appliedCC := ...` 到 `flushSeg()` 原样搬运，一行不改）：

```go
// applyPhase 把一批已提交条目应用到 FSM，返回本批登记的成员变更 waiter。
//
// 参数：
//   - ents: 已提交待应用的条目（async 下来自 MsgStorageApply.Entries）
//
// 返回：
//   - 本批 apply 掉的 ConfChangeV2 登记（id + 是否本节点发起）。**通知
//     由调用方做**，不在本方法内——通知时机与 raft 内部 applied 位点的
//     推进强相关（见 applyOnce 的注释），是调用方的责任。
//
// 两条顺序不变量（原 handleReady 的注释原样保留）：
//   - 普通条目攒段合批，遇成员变更必须先冲刷已积累的段——SaveConfState
//     用独立批次写成员表 + applied 位点，段若晚于它提交会把 applied 位点
//     倒退回段内更小的 index，位点单调性破坏；
//   - 跳过 index ≤ applied 的条目：raft 可能重发已 apply 过的条目
//     （conflict 回退重写后），FSM 已是该 index 的状态。
func (gr *group) applyPhase(ents []*raftpb.Entry) []ccApplied {
	appliedCC := make([]ccApplied, 0, 2)
	var seg []*raftpb.Entry
	flushSeg := func() {
		gr.applyEntries(seg)
		seg = seg[:0]
	}
	for _, ent := range ents {
		// ...（原 handleReady 循环体，逐字搬运，不做任何修改）
	}
	flushSeg()
	return appliedCC
}
```

**搬运纪律**：`for` 循环体从 `group.go:447`（`// 重启重放的幂等保证` 那条注释）到 `:528`（`case raftpb.EntryNormal` 分支结束）**逐字复制**，包括全部中文注释。不要顺手"优化"任何一行——这段代码里的每条注释都对应一个曾经踩过的坑。

- [ ] **Step 4: `handleReady` 改为调用这两个方法**

`group.go` 里原第 1 步（`:393-411`）整段替换为：

```go
	// 1. 持久化 + 双记账（见 persistPhase）
	gr.persistPhase(rd.HardState, rd.Entries, gr.syncPersist(rd))
```

原第 3 步（`:433-530`）整段替换为：

```go
	// 3. CommittedEntries apply（见 applyPhase）；本轮登记的成员变更
	//    waiter 在 Advance 之后通知（见下）
	appliedCC := gr.applyPhase(rd.CommittedEntries)
```

`gr.rn.Advance()` 及其后的通知代码保持原样。

- [ ] **Step 5: 编译 + 全量回归，确认行为零变化**

```bash
go build ./... && go vet ./internal/cluster/ && go test ./internal/cluster/ -race -count=1
```

期望：全部 PASS，且**没有任何测试文件被修改**——这是"纯重构"的可执行证明。

- [ ] **Step 6: 补充意图注释**

确认三处注释已到位（Step 2/3 的代码块里已含，此步是自检）：
- `persistPhase` 的 doc comment 说清"顺序即不变量"与"为什么 panic"
- `applyPhase` 的 doc comment 说清"通知为什么不在这里做"
- `handleReady` 里两处调用点保留原有的步骤编号注释，方便 Task 2 对照

本 task 不新增日志：`persistPhase` 的错误日志是从原处搬来的，`applyPhase` 内部日志随代码一起搬运，稳态路径本就零日志（热路径规则）。

- [ ] **Step 7: 提交**

```bash
git add internal/cluster/group.go
git commit -m "refactor(cluster): 抽出 persistPhase/applyPhase——为异步存储写入让路，行为零变化"
```

---

## Task 2: 打开 AsyncStorageWrites，主循环改为分发

本 task 是整个改造的主体：一次性完成「开关 + 主循环分发 + 两条本地存储协程 + 删 Advance + waiter 通知时机重定位」。**这些改动无法拆开落地**——`AsyncStorageWrites` 一旦为 true，`rd.Entries` 就不该再被直接消费、`Advance` 就会永久阻塞，中间态不存在。

**Files:**
- Modify: `internal/cluster/group.go`（`group` 结构体、`newGroup`、`raftConfig`、`run`、`handleReady` → `dispatchReady`）
- Create: `internal/cluster/group_async_test.go`
- Modify: `internal/cluster/group_test.go`（2 处，见 Step 8）

**Interfaces:**
- Consumes: Task 1 的 `persistPhase(hs, ents, sync)`、`applyPhase(ents) []ccApplied`
- Produces:
  - `type localMsg struct { m *raftpb.Message; mustSync bool }`
  - `func (gr *group) dispatchReady(ctx context.Context, rd raft.Ready)`
  - `func (gr *group) runAppend(ctx context.Context)` / `func (gr *group) runApply(ctx context.Context)`
  - `func (gr *group) appendOnce(ctx context.Context, lm localMsg)` / `func (gr *group) applyOnce(ctx context.Context, m *raftpb.Message)`
  - `func (gr *group) deliverResponses(ctx context.Context, resps []*raftpb.Message, stage string)`
  - `func (gr *group) enqueueLocal(ctx context.Context, ch chan<- localMsg, lm localMsg, stage string) bool`
  - `func hardStateOf(m *raftpb.Message) *raftpb.HardState`
  - 常量 `localQueueDepth` / `localQueueBlockWarn`

- [ ] **Step 1: 写失败的测试——分发必须按 `m.To` 三分，本地消息绝不走 send**

新建 `internal/cluster/group_async_test.go`：

```go
// group_async_test.go 覆盖 raft 异步存储写入（AsyncStorageWrites）改造
// 引入的三条新契约。
//
// 职责：
//   - 分发路由：MsgStorageAppend/MsgStorageApply 进本地通道，其余走 send
//   - 响应投递：自指响应必须经 rn.Step 可靠投递，绝不经 gr.send（会丢）
//   - MustSync 载体：fsync 档的同步判定随消息配对传递，语义与旧路径一致
//
// 边界：
//   - 不覆盖多节点选举与消息语义（cluster_test.go 的范围）
//   - 不覆盖快照安装与 apply 互斥（Task 3 的 group_snapinstall 用例）
//   - 复用 group_test.go 的 openClusterTestStore/testSlog/newApplyTestGroup
package cluster

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// msgTo 构造一条只关心 To 字段的消息（分发路由测试用）。
func msgTo(to uint64, typ raftpb.MessageType) *raftpb.Message {
	t := typ
	dst := to
	return &raftpb.Message{Type: &t, To: &dst}
}

// TestDispatchReadyRoutesByTarget 分发的核心契约：本地存储消息按 m.To
// 进两条本地通道，其余消息才交给 send 外发。
//
// 为什么这条必须有用例：走错一路的后果是**组静默卡死而非报错**——
// gr.send 对自指消息走 gr.step（inbox 满则丢、安装期显式丢弃），丢一条
// MsgStorageAppend 就是 raft 永远等不到的那条 MsgStorageAppendResp。
func TestDispatchReadyRoutesByTarget(t *testing.T) {
	gr, _, _ := newApplyTestGroup(t, nil)
	var sent []*raftpb.Message
	gr.send = func(g uint32, msgs []*raftpb.Message) { sent = append(sent, msgs...) }

	rd := raft.Ready{Messages: []*raftpb.Message{
		msgTo(2, raftpb.MsgApp),
		msgTo(raft.LocalAppendThread, raftpb.MsgStorageAppend),
		msgTo(raft.LocalApplyThread, raftpb.MsgStorageApply),
		msgTo(3, raftpb.MsgHeartbeat),
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gr.dispatchReady(ctx, rd)

	if len(sent) != 2 {
		t.Fatalf("只有 2 条网络消息该外发，实际 %d 条——本地存储消息漏进了 send（丢一条即静默卡死）", len(sent))
	}
	for _, m := range sent {
		if raft.IsLocalMsgTarget(m.GetTo()) {
			t.Fatalf("本地存储消息 %s 被交给了 send，必须走本地通道", m.GetType().String())
		}
	}
	select {
	case lm := <-gr.appendCh:
		if lm.m.GetType() != raftpb.MsgStorageAppend {
			t.Fatalf("append 通道收到的应是 MsgStorageAppend，得到 %s", lm.m.GetType().String())
		}
	default:
		t.Fatal("MsgStorageAppend 没有进 append 通道")
	}
	select {
	case lm := <-gr.applyCh:
		if lm.m.GetType() != raftpb.MsgStorageApply {
			t.Fatalf("apply 通道收到的应是 MsgStorageApply，得到 %s", lm.m.GetType().String())
		}
	default:
		t.Fatal("MsgStorageApply 没有进 apply 通道")
	}
}

// TestDispatchReadyCarriesMustSync fsync 档的同步判定必须随 append
// 消息配对传递——async 后 MustSync 不再能在写入点现场读到 Ready。
//
// 语义锚：判定输入换了载体，判定本身（mode==Fsync && MustSync）不变。
// 若实现改成「有 Responses 就 sync」，本用例会失败——那会把 commit-only
// 轮也 fsync，退回 2026-08-08 优化之前的每提案两次盘。
func TestDispatchReadyCarriesMustSync(t *testing.T) {
	gr, _, _ := newApplyTestGroup(t, nil)
	gr.send = func(uint32, []*raftpb.Message) {}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, mustSync := range []bool{true, false} {
		gr.dispatchReady(ctx, raft.Ready{
			MustSync: mustSync,
			Messages: []*raftpb.Message{msgTo(raft.LocalAppendThread, raftpb.MsgStorageAppend)},
		})
		select {
		case lm := <-gr.appendCh:
			if lm.mustSync != mustSync {
				t.Fatalf("MustSync=%v 必须随消息传到 append 阶段，得到 %v", mustSync, lm.mustSync)
			}
		case <-time.After(time.Second):
			t.Fatal("append 通道没收到消息")
		}
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
go test ./internal/cluster/ -run 'TestDispatchReady' -count=1
```

期望：编译失败，`gr.dispatchReady undefined` / `gr.appendCh undefined` / `gr.applyCh undefined`。

- [ ] **Step 3: 打开开关 + 加字段**

`raftConfig`（`group.go:289`）的返回值里加一行，并在函数 doc comment 末尾追加说明段：

```go
		// 异步存储写入（AsyncStorageWrites）：日志写入与状态机应用改由
		// MsgStorageAppend/MsgStorageApply 两条本地消息表达，写入与
		// Ready 迭代解耦。打开它是本仓库攻 quorum-fsync 档 raft 机制税
		// 的手段——非 async 模式下 node.run 投出 Ready 后必须等 advancec
		// 才产下一轮，于是 leader 做 fsync 的那段时间里 MsgApp 一个字节
		// 都发不出去，确认链是「leader fsync → 网络 → follower fsync」
		// 两次串行相加。打开后 leader 可在自己 fsync 完成前就 replicate
		// （raft 只要求 commit 前 durable，不要求 replicate 前 durable）。
		//
		// 单点开关：所有装配路径（StartNode/RestartNode）与全部单元测试
		// 都经本函数取配置，此处置位即全局生效。**不提供配置项**——两套
		// Ready 处理路径长期共存必然腐化，且会制造「两条路径只测了一条」
		// 的虚假安全感（设计文档 §4）。
		AsyncStorageWrites: true,
```

`group` 结构体（`group.go:90-188`）在 `inbox` 字段**之后**插入：

```go
	// 本地存储阶段的两条通道（AsyncStorageWrites）：主循环按 m.To 分发，
	// append/apply 两条协程各自消费。
	//
	// **满则阻塞，绝不丢**——这与 inbox 的「满则丢」契约正好相反，是
	// raft 的硬要求：同一 target 的本地消息必须可靠、有序处理
	// （raft/v3@v3.7.0/raft.go:163-165）。丢一条 MsgStorageAppend 等于
	// 静默丢日志，且 raft 会一直等那条永远不来的 MsgStorageAppendResp
	// ——组静默卡死，没有任何报错。阻塞会传导到主循环并停掉本组 tick，
	// 因此 enqueueLocal 对阻塞留痕（见其注释）。
	appendCh chan localMsg
	applyCh  chan localMsg
	// 本地阶段可观测性（Task 4 补全语义，字段在此一次性声明避免二次改
	// 结构体）：累计入队阻塞时长、累计处理条数、单次处理耗时峰值。
	appendBlockNanos atomic.Uint64
	applyBlockNanos  atomic.Uint64
	appendCount      atomic.Uint64
	applyCount       atomic.Uint64
	respDropped      atomic.Uint64
```

在 `group` 结构体**之前**插入类型与常量：

```go
// localMsg 是投进本地存储阶段的一条消息及其配套判定。
//
// mustSync 只对 MsgStorageAppend 有意义：async 之后写入点已经拿不到
// Ready，而 fsync 档的同步判定（mode==AckQuorumFsync && rd.MustSync）
// 必须逐轮成立，因此判定在主循环现场算好、随消息配对传下去。载体变了，
// 判定本身一个字没变（设计文档 §4）。
//
// 为什么不改成「带 Responses 就 sync」（raft 契约的字面要求）：那比
// MustSync 严格——commit-only 的轮次也会被 fsync，等于退回 2026-08-08
// 「每提案少一次 fsync」优化之前的形态。MustSync 为假意味着无新条目且
// term/vote 未变，此时 Responses 里的 MsgStorageAppendResp 确认的是**更早
// 轮次**已经 fsync 过的条目，commit 位点丢了由重放重新推导——与
// syncPersist 的既有论证完全同构。
type localMsg struct {
	m        *raftpb.Message
	mustSync bool
}

const (
	// localQueueDepth 两条本地存储通道的容量。取 64：单组在途 Ready
	// 受 MaxInflightMsgs(256) 与 MaxCommittedSizePerReady(=MaxSizePerMsg,
	// 1MiB) 双重约束，64 轮在途已远超稳态需要；再大只是把「存储侧跟不上」
	// 从阻塞变成静默堆积内存，反而更难发现。
	localQueueDepth = 64
	// localQueueBlockWarn 入队阻塞多久算异常。50ms ≈ 半个 tick（100ms）
	// ——超过它意味着本组的选举计时器已经开始受影响，必须留痕。
	localQueueBlockWarn = 50 * time.Millisecond
)
```

`newGroup`（`group.go:213-229`）的结构体字面量里，`inbox` 之后加两行：

```go
		appendCh:       make(chan localMsg, localQueueDepth),
		applyCh:        make(chan localMsg, localQueueDepth),
```

- [ ] **Step 4: 实现 `dispatchReady` / `enqueueLocal`**

把 `handleReady` **整个删除**（含其上方的 doc comment），替换为：

```go
// dispatchReady 处理一轮 Ready：网络消息立即外发，两条本地存储消息可靠
// 入队，读状态与 leader 变更就地处理。**没有 Advance**——async 模式下
// node.run 的 advancec 恒为 nil，调用 Advance 会永久阻塞（raft/v3@v3.7.0/
// node.go:435-441、:555-560）；raft 认为「本轮处理完毕」的信号改由本地
// 阶段投递 m.Responses 承担。
//
// 顺序即收益（本改造的全部意义所在）：
//  1. **先外发网络消息**——leader 的 MsgApp 从此不再排在自己的 fsync
//     后面，follower 可以与 leader 并行落盘。这一步不需要任何前置条件：
//     async 下 rd.Messages 里的网络消息「can be sent immediately」，因为
//     一切以持久化为前提的消息都被移进了本地消息的 Responses 里
//     （raft/v3@v3.7.0/node.go:98-110）。
//  2. 入队 append（携带本轮 MustSync）、入队 apply。
//  3. 读状态回流与 leader 变更。
//
// 为什么 leader 变更放在入队之后：入队本身就是排序动作——写入已经进了
// FIFO，不可能被后续轮次的写入越过。这比旧路径（持久化**完成**后才公布
// 新 leader）弱一档，是 async 的固有代价：raft 自身的安全性由 MsgVoteResp
// 随 Responses 在 fsync 之后投递来保证（vote 落盘先于响应投票请求），
// 本节点公布 leader 身份只影响本进程内的路由与钩子。
//
// rd.Entries/HardState/Snapshot/CommittedEntries 在 async 下**一律不直接
// 消费**——它们已被复制进两条本地消息，直接用会双写/双 apply。
func (gr *group) dispatchReady(ctx context.Context, rd raft.Ready) {
	var outbound []*raftpb.Message
	var locals []localMsg
	for _, m := range rd.Messages {
		switch m.GetTo() {
		case raft.LocalAppendThread:
			locals = append(locals, localMsg{m: m, mustSync: rd.MustSync})
		case raft.LocalApplyThread:
			locals = append(locals, localMsg{m: m})
		default:
			outbound = append(outbound, m)
		}
	}
	// 1. 网络消息立即外发（transport 发送永不阻塞——满则丢，raft 心跳
	//    重试兜底，Task 3 契约）。外发前先登记本轮的 MsgSnap 定向台账
	//    ——这是 leader 侧唯一能知道「哪份快照发给了哪个 peer」的时刻。
	if len(outbound) > 0 {
		gr.noteSnapSends(outbound)
		gr.send(gr.g, outbound)
	}
	// 2. 本地存储消息按原序可靠入队。入队失败只可能是组正在退出
	//    （enqueueLocal 已留痕），此时直接收工——后续消息也没有归宿。
	for _, lm := range locals {
		ch := gr.applyCh
		stage := "apply"
		if lm.m.GetTo() == raft.LocalAppendThread {
			ch, stage = gr.appendCh, "append"
		}
		if !gr.enqueueLocal(ctx, ch, lm, stage) {
			return
		}
	}
	// 3. 读状态回流：raft 已确认本节点在当前任期仍是 leader，给出的
	//    readIndex 是本轮读屏障的下界。真正放行由 index<=applied 决定，
	//    apply 阶段每批之后还会再扫一次（见 applyOnce）。
	gr.stepReadStates(rd.ReadStates)
	// 4. leader 变更观测：SoftState 变化是切换的第一信号。
	//    顺序即屏障（batch③ 评审 m1）：先跑钩子（同步失效计数器缓存），
	//    再让 lead.Store 把 leader 身份对外可见。反过来会留下
	//    「IsLeader 已放行、缓存尚未失效」的窗口，并发 Append 拿到
	//    陈旧 offset 覆写已 quorum 提交的消息。
	if rd.SoftState != nil {
		gr.notifyLeaderChange(rd.SoftState.Lead)
		gr.lead.Store(rd.SoftState.Lead)
		gr.lg.Info("组 leader 变更", "lead", rd.SoftState.Lead, "term", gr.currentTerm())
	}
}

// enqueueLocal 把一条本地存储消息可靠投递进指定阶段通道，返回是否成功。
//
// 参数：
//   - ch: 目标通道（gr.appendCh 或 gr.applyCh）
//   - lm: 待投递消息
//   - stage: 阶段名（"append"/"apply"），只用于日志
//
// 返回：true = 已入队；false = 组正在退出（ctx 已取消），调用方应收工。
//
// 与 gr.step（inbox）的契约正好相反：**满则阻塞，绝不丢**。raft 要求同一
// target 的本地消息可靠有序处理，丢一条即静默卡死（设计文档 §5.1）。
// 代价是阻塞会传导到主循环、停掉本组 tick，因此阻塞必须留痕——没有这条
// 日志，「存储侧跟不上」在现场只表现为莫名其妙的换主。
func (gr *group) enqueueLocal(ctx context.Context, ch chan<- localMsg, lm localMsg, stage string) bool {
	// 快路径：稳态下通道永远不满，一次非阻塞发送即完成，零日志零计时
	select {
	case ch <- lm:
		return true
	default:
	}
	start := time.Now()
	select {
	case ch <- lm:
		d := time.Since(start)
		gr.blockNanosOf(stage).Add(uint64(d))
		if d >= localQueueBlockWarn {
			gr.lg.Warn("本地存储通道阻塞（队列满，主循环被拖住，本组 tick 已受影响)",
				"stage", stage, "blocked", d.Round(time.Millisecond).String(),
				"cap", cap(ch), "type", lm.m.GetType().String())
		}
		return true
	case <-ctx.Done():
		gr.respDropped.Add(1)
		gr.lg.Warn("本地存储消息随停机丢弃（组正在退出）",
			"stage", stage, "type", lm.m.GetType().String(),
			"entries", len(lm.m.GetEntries()))
		return false
	}
}

// blockNanosOf 返回阶段对应的阻塞时长累计器（可观测性打点用）。
func (gr *group) blockNanosOf(stage string) *atomic.Uint64 {
	if stage == "append" {
		return &gr.appendBlockNanos
	}
	return &gr.applyBlockNanos
}
```

- [ ] **Step 5: 实现两条本地阶段协程**

紧接 `dispatchReady` 之后插入：

```go
// hardStateOf 从 MsgStorageAppend 还原 HardState，无状态变更时返回 nil。
//
// raft 的构造契约（raft/v3@v3.7.0/rawnode.go:230-241）：HardState 有变化
// 时 Term/Vote/Commit **三者同时赋值**，无变化时三者同时不赋值。因此看
// 任一字段是否为 nil 即可判定，不必逐个比较。
//
// 三个值按值拷贝而不是共享 m 的指针：mem.SetHardState 会长期持有这份
// 结构，而消息的生命周期由 raft 决定——共享指针是"能跑但说不清"的那类
// 依赖，一次拷贝三个 uint64 的代价可以忽略。
func hardStateOf(m *raftpb.Message) *raftpb.HardState {
	if m.Term == nil && m.Vote == nil && m.Commit == nil {
		return nil
	}
	term, vote, commit := m.GetTerm(), m.GetVote(), m.GetCommit()
	return &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
}

// runAppend 是 append 阶段的协程主体：串行消费 appendCh。
//
// 串行是契约要求（同一 target 的本地消息不得重排），也正是攒批的来源
// ——主循环不再等它，raft 于是能连着产出多轮 Ready，本协程一轮一轮
// 消费时每轮的 Entries 自然更大。
func (gr *group) runAppend(ctx context.Context) {
	gr.lg.Info("append 阶段启动", "queue_cap", cap(gr.appendCh))
	defer func() {
		gr.lg.Info("append 阶段退出", "handled", gr.appendCount.Load(),
			"blocked_total", time.Duration(gr.appendBlockNanos.Load()).Round(time.Millisecond).String())
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case lm := <-gr.appendCh:
			gr.appendOnce(ctx, lm)
		}
	}
}

// appendOnce 处理一条 MsgStorageAppend：快照安装（若有）→ 持久化 →
// 双记账 → 投递响应。
//
// 顺序即不变量（设计文档 §5.2）：raft 判定「日志已 stable」的那一刻就是
// 响应投回的那一刻，此后它会立刻去 MemoryStorage 读这些条目。任何一步
// 提前投递响应，raft 都会读到还不存在的日志。
func (gr *group) appendOnce(ctx context.Context, lm localMsg) {
	m := lm.m
	gr.appendCount.Add(1)
	// 0. 快照：async 下快照随 MsgStorageAppend 到达（raft/v3@v3.7.0/
	//    raft.go:167-170「MsgStorageAppend carries ... snapshots to apply」）。
	//    安装期间保持 tick——见 installSnapshotWithRetry。
	if snap := m.GetSnapshot(); !raft.IsEmptySnap(snap) {
		err := gr.installSnapshotWithRetry(ctx, snap)
		if err != nil && ctx.Err() != nil {
			// 停机途中的安装失败不是故障：安装中标记已在盘上，重启时
			// buildGroup 清空重来。此处 panic 只会把一次正常停机变成一次
			// 崩溃退出。直接返回——**且不投递响应**：响应一旦投出，raft
			// 就认为快照已持久化已应用。
			gr.lg.Warn("快照安装随停机中止（安装中标记已在盘上，重启清空重来）",
				"index", snap.Metadata.GetIndex(), "err", err)
			return
		}
		if err != nil {
			// 安装失败是不可恢复状态：绝不能投递响应后静默续跑——
			// MsgStorageAppendResp（携带快照）一旦步进给 raft 内核，内核的
			// appliedSnap（stableSnapTo + appliedTo）即刻把快照标记为已持久
			// 化已应用，raft 从此不再重发 MsgSnap；而磁盘上仍是安装中标记 +
			// 半截数据、内存侧 MemoryStorage 从未更新——三方分叉、永不收敛。
			// 按 Persist/applyEntries 同策略 fail-stop panic。
			//
			// （本段与旧路径唯一的差别是「Advance 步进响应」变成「投递
			// Responses 步进响应」——触发分叉的机制换了名字，后果一字不变。
			// 重启后的恢复路径、leader 侧 reportStalledSnapshots 的兜底
			// 责任，全部与旧注释所述一致，见 installSnapshotWithRetry。）
			gr.lg.Error("快照安装失败，组停摆（等待重启；leader 侧由失败感知重驱动）",
				"index", snap.Metadata.GetIndex(), "err", err)
			panic(err)
		}
	}
	// 1. 持久化 + 双记账。sync 判定：档位 × 本轮 MustSync，语义与旧路径
	//    的 syncPersist 完全一致，只是 MustSync 换了载体（见 localMsg）。
	gr.persistPhase(hardStateOf(m), m.GetEntries(), gr.mode == AckQuorumFsync && lm.mustSync)
	// 2. 投递响应——必须严格晚于第 1 步（本函数 doc comment）
	gr.deliverResponses(ctx, m.GetResponses(), "append")
}

// runApply 是 apply 阶段的协程主体：串行消费 applyCh。
//
// 与 append 阶段并行运行是刻意的（设计文档 §5.3）：MsgStorageApply 的
// 写入**不要求** durable 即可投递响应，apply 因此可以比 append 跑得松。
// 把两者合成一条协程会让 FSM 写入重新挡住日志 fsync，收益折半。
func (gr *group) runApply(ctx context.Context) {
	gr.lg.Info("apply 阶段启动", "queue_cap", cap(gr.applyCh))
	defer func() {
		gr.lg.Info("apply 阶段退出", "handled", gr.applyCount.Load(),
			"blocked_total", time.Duration(gr.applyBlockNanos.Load()).Round(time.Millisecond).String())
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case lm := <-gr.applyCh:
			gr.applyOnce(ctx, lm.m)
		}
	}
}

// applyOnce 处理一条 MsgStorageApply：apply 条目 → 投递响应 → 唤醒
// 成员变更与读屏障等待者。
//
// **通知必须晚于响应投递**，这是 Advance 消失后 ccWaiter 时序的新落点：
// 旧路径里 raft 内部 applied 位点在 Advance 时才推进（Advance 负责把
// MsgStorageApplyResp 步进内核），若在此之前通知，编排层
// （Remove→AddLearner 背靠背提案）紧接着提出的下一条 ConfChange 会落在
// 「pendingConfIndex > applied」校验窗口内，被 raft 静默替换成空普通条目
// ——替换不可观察、ccWaiter 永不通知，调用方只能等超时（Task 7 集成
// 测试抓到的缺口）。async 下承担这件事的是 deliverResponses，因此通知
// 挪到它之后，职责一一对应。
//
// 同旧路径：晚于响应投递只是把窗口收窄，并非闭合——raft 内部 applied
// 的推进要等节点 goroutine 消费 recvc 后才发生，µs 级残余窗口内背靠背的
// 两条 ConfChange 仍可能被静默替换；proposeConfChange 对空条目替换的检测
// 与重试是 batch③ 的兜底缓解。
func (gr *group) applyOnce(ctx context.Context, m *raftpb.Message) {
	gr.applyCount.Add(1)
	appliedCC := gr.applyPhase(m.GetEntries())
	gr.deliverResponses(ctx, m.GetResponses(), "apply")
	for _, cc := range appliedCC {
		if cc.notify {
			gr.notifyWaiter(gr.ccWaiters, cc.id)
		}
	}
	// 读屏障放行必须晚于 apply：applied 是本批 apply 推进的，早于它扫描
	// 只会白扫一遍，屏障要多等一整批才放行。
	gr.notifyReadWaiters(gr.applied.Load())
}

// deliverResponses 投递一条本地存储消息的响应集合。
//
// 参数：
//   - resps: m.Responses，可能为空
//   - stage: 阶段名（"append"/"apply"），只用于日志
//
// 路由是本改造最容易踩死的一处（设计文档 §5.1）：
//
//	| 响应目标        | 去向        | 可靠性                     |
//	|-----------------|-------------|----------------------------|
//	| 本节点（selfID）| gr.rn.Step  | **可靠有序**，满则阻塞不丢 |
//	| 其他 peer       | gr.send     | 可丢，raft 心跳重试兜底    |
//
// **自指响应绝不能走 gr.send**：Manager.send 对自指消息走 gr.step →
// inbox，而 inbox 在快照安装期是显式丢弃、组退出时也丢弃。丢一条
// MsgStorageAppendResp 就是 raft 永远等不到的那一条——组静默卡死，没有
// 任何报错。gr.rn.Step 走 node.recvc，满则阻塞、不丢；且不会与主循环
// 死锁——node.run 的 select 始终可以消费 recvc，即便主循环正阻塞在
// 本地通道入队上。
//
// 对端响应先发、自指响应后步进：follower 的 MsgAppResp 在关键路径上，
// 早一个调度周期就早一点确认；自指响应之间的相对顺序原样保留（raft 要求
// MsgStorageAppendResp 排在 msgsAfterAppend 里的自指 MsgAppResp 之后，
// 见 rawnode.go:245-253 的性能说明）。
func (gr *group) deliverResponses(ctx context.Context, resps []*raftpb.Message, stage string) {
	if len(resps) == 0 {
		return
	}
	var peer []*raftpb.Message
	for _, m := range resps {
		if m.GetTo() != gr.selfID {
			peer = append(peer, m)
		}
	}
	if len(peer) > 0 {
		gr.send(gr.g, peer)
	}
	for _, m := range resps {
		if m.GetTo() != gr.selfID {
			continue
		}
		if err := gr.rn.Step(ctx, m); err != nil {
			// 只可能是 ctx 取消或节点已 Stop（组正在退出）。稳态下不可能
			// 走到这里——真走到了说明有响应没被 raft 收到，必须留痕：
			// 它的症状是组静默卡死，届时这条日志是唯一现场。
			gr.respDropped.Add(1)
			gr.lg.Warn("本地响应步进失败（组正在退出？未收到该响应的组会静默卡死）",
				"stage", stage, "type", m.GetType().String(), "err", err)
			return
		}
	}
}
```

- [ ] **Step 6: 改 `run`——启两条协程，改调 `dispatchReady`**

`run`（`group.go:311-331`）整体替换为：

```go
func (gr *group) run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond) // ElectionTick=10 → 选举超时约 1s
	defer ticker.Stop()
	// 两条本地存储阶段协程与主循环同生命周期：doneCh 必须在三者**全部**
	// 退出之后才关闭，否则测试里 <-gr.done() 返回时仍有协程在碰 store/rs，
	// -race 下是稳定的 use-after-close。
	var stages sync.WaitGroup
	stages.Add(2)
	go func() { defer stages.Done(); gr.runAppend(ctx) }()
	go func() { defer stages.Done(); gr.runApply(ctx) }()
	defer func() {
		stages.Wait()
		close(gr.doneCh)
	}()
	// 存生命周期 ctx：读屏障的合流驱动 goroutine 以它为父 ctx，组退出时
	// 在途的 read-index 轮次随之取消，不会挂着等到 barrierTimeout。
	gr.lifeCtx = ctx
	for {
		select {
		case <-ctx.Done():
			gr.lg.Info("组退出")
			return
		case <-ticker.C:
			gr.rn.Tick()
		case m := <-gr.inbox:
			_ = gr.rn.Step(ctx, m)
		case rd := <-gr.rn.Ready():
			gr.dispatchReady(ctx, rd)
		}
	}
}
```

同时更新 `run` 的 doc comment：把「Ready 处理」改为「Ready 分发」，并加一句「本循环内不做任何存储写入——写入全部经 appendCh/applyCh 交给两条阶段协程，这正是流水线深度的来源」。

`group.go` 的文件头注释（`:1-16`）里，把「Ready 四步契约」改为「Ready 分发 + 本地 append/apply 两阶段」，并在「边界」里补一条：「不在主循环内做存储写入——日志落盘与 FSM apply 分属两条本地阶段协程，见 dispatchReady」。

- [ ] **Step 7: 跑 Step 1 的测试确认通过**

```bash
go test ./internal/cluster/ -run 'TestDispatchReady' -race -count=1
```

期望：两个用例 PASS。

- [ ] **Step 8: 修改两个快照用例的入口（本计划预告的全部 2 处测试修改）**

`handleReady` 已不存在，两个直接调用它的用例必须改入口。**这是时序假设确实变更的情形**，理由如下：

| 用例 | 位置 | 改法 | 理由 |
|---|---|---|---|
| `TestInstallSnapshotFailurePanicsAndKeepsMarker` | `group_test.go:348` | `gr.handleReady(ctx, raft.Ready{Snapshot: snap})` → `gr.appendOnce(ctx, storageAppendWithSnap(snap))` | 快照的到达路径从 `Ready.Snapshot` 变成 `MsgStorageAppend.Snapshot`（raft async 契约），`appendOnce` 是新的 panic 抛出处。断言一字不改 |
| `TestInstallSnapshotAbortsOnShutdown` | `group_test.go:721` | 同上 | 同上 |

在 `group_async_test.go` 里补上构造函数：

```go
// storageAppendWithSnap 构造一条携带快照的 MsgStorageAppend。
//
// async 契约下快照随本地 append 消息到达（raft/v3@v3.7.0/rawnode.go:242-244
// 的 newStorageAppendMsg），不再出现在 Ready.Snapshot 的消费路径上。
func storageAppendWithSnap(snap *raftpb.Snapshot) localMsg {
	typ := raftpb.MsgStorageAppend
	to := raft.LocalAppendThread
	return localMsg{m: &raftpb.Message{Type: &typ, To: &to, Snapshot: snap}}
}
```

两个用例里同时把注释「为什么直接调 handleReady 而非走 run 循环」改成「为什么直接调 appendOnce 而非走 run 循环：panic 发生在阶段协程内时测试进程无法 recover（goroutine 的 panic 不可跨协程捕获）；appendOnce 是 panic 的抛出处，直接在测试协程调用即可捕获断言，生产行为不变」。

- [ ] **Step 9: 全量回归**

```bash
go build ./... && go vet ./internal/cluster/ && go test ./internal/cluster/ -race -count=1
```

期望：全部 PASS。**除上表 2 处外任何测试文件被修改，都说明语义变了——停下来提问，不要改测试让它变绿。**

```bash
go test ./test/e2e/ -race -count=1 -timeout 30m
```

期望：全部 PASS，零修改。

- [ ] **Step 10: 补充关键节点日志**

自检清单（Step 3-6 的代码块里已含，此步逐项确认）：

- [x] append/apply 阶段启动与退出各一条 Info，退出时带累计处理条数与累计阻塞时长（`runAppend`/`runApply`）
- [x] 本地通道阻塞超阈值 Warn，带 stage/blocked/cap/type（`enqueueLocal`）
- [x] 停机丢弃本地消息 Warn，带 stage/type/entries（`enqueueLocal`）
- [x] 本地响应步进失败 Warn，明写「未收到该响应的组会静默卡死」（`deliverResponses`）
- [x] 快照安装失败 Error（fail-stop）与停机中止 Warn，各带 index + err（`appendOnce`）
- [x] 持久化失败 Error 带 entries/sync/err（Task 1 的 `persistPhase`）
- [x] leader 变更 Info（`dispatchReady`，从旧路径原样保留）
- [x] 稳态热路径（每轮分发、每次 append/apply 成功）零日志

- [ ] **Step 11: 补充意图注释**

自检清单：

- [x] `raftConfig` 的 `AsyncStorageWrites` 有段落级注释说明「为什么打开」「为什么不留开关」
- [x] `localMsg.mustSync` 有注释说明「为什么不用『有 Responses 就 sync』」
- [x] `appendCh`/`applyCh` 字段注释写明「满则阻塞绝不丢」及其与 `inbox` 契约的对立
- [x] `deliverResponses` 有三行路由表 + 「为什么不能走 gr.send」
- [x] `applyOnce` 写明 ccWaiter 通知时机从 Advance 迁到 deliverResponses 之后的完整理由
- [x] `dispatchReady` 写明「先外发再入队」的收益理由，以及 leader 变更位置的取舍
- [x] `run` 写明 WaitGroup 存在的理由（doneCh 语义）
- [x] `group.go` 文件头的职责/边界已更新
- [ ] **陈旧引用清扫**：`handleReady` 已不存在，全仓库搜一遍残留引用并改写为新落点

```bash
grep -rn "handleReady" --include='*.go' --include='*.md' . | grep -v docs/superpowers
```

已知需改的两处非代码引用：`internal/cluster/readbarrier.go:122-123`（`notifyReadWaiters` 的 doc comment「在 handleReady 的 apply 循环之后调用」→「在 applyOnce 的 applyPhase 之后调用」）、`internal/cluster/group.go:1206` 附近 `step` 注释里的「Ready 循环在快照安装期间整段不消费 inbox」（async 后主循环仍在跑，改为「安装期主循环可能阻塞在本地通道入队上而不再消费 inbox」）。搜出的其他引用逐条判断，**不要留一条指向已删函数的注释**——那是下一个人排查时的假路标。

- [ ] **Step 12: 提交**

```bash
git add internal/cluster/group.go internal/cluster/group_async_test.go internal/cluster/group_test.go
git commit -m "feat(cluster): 打开 raft AsyncStorageWrites——主循环只分发，append/apply 各自成阶段

Ready 处理从串行四步改为「网络消息立即外发 + 两条本地存储消息可靠入队」，
Advance 随之删除（async 下调用会永久阻塞）。leader 的 MsgApp 不再排在自己
的 fsync 之后，同一组多轮 Ready 可在途，组内攒批是副产品。

MustSync 判定语义不变，载体从 Ready 换成配对传递的 localMsg。
自指响应经 rn.Step 可靠投递，绝不走 gr.send（inbox 会丢，丢一条即静默卡死）。"
```

---

## Task 3: 快照安装与 apply 阶段互斥

Task 2 之后 append 与 apply 是两条并行协程，于是出现了旧路径里不存在的窗口：**快照安装（append 阶段）与条目 apply（apply 阶段）可能同时进行**。raft 明确允许这样——「Messages to different targets can be processed in any order」（`raft/v3@v3.7.0/raft.go:165-166`）。

后果：快照安装会整体重写本组 FSM（`installSnapshot` 第 3 步 `wipeGroupKeys` + 第 4 步拉块 + 第 5 步收口），一批安装之前就已入队的 `MsgStorageApply` 若在安装**之后**落地，就会把陈旧数据写回刚刚被快照覆盖的键上——静默的状态机分叉。

**Files:**
- Modify: `internal/cluster/group.go`（`group` 结构体加 `installMu`；`appendOnce` 与 `applyOnce` 各加一段临界区）
- Test: `internal/cluster/group_async_test.go`（新增 1 个用例）

**Interfaces:**
- Consumes: Task 2 的 `appendOnce` / `applyOnce` / `storageAppendWithSnap`
- Produces: `group.installMu sync.Mutex`

- [ ] **Step 1: 写失败的测试**

追加到 `group_async_test.go`：

```go
// TestSnapshotInstallExcludesApply 快照安装期间 apply 阶段必须让路。
//
// 为什么这条是 async 独有的：旧路径里安装与 apply 都在 Ready 循环内串行
// 执行，物理上不可能重叠；async 之后它们是两条协程，raft 也明确允许不同
// target 的本地消息乱序处理。若不互斥，一批安装前入队的 MsgStorageApply
// 会在 wipeGroupKeys 之后落地，把陈旧数据写回刚被快照覆盖的键——静默的
// 状态机分叉，没有任何报错。
//
// 断言两件事：
//  1. 安装未完成时 applyOnce 不得推进（互斥真的生效）；
//  2. 安装完成后那批陈旧条目被整批跳过（index ≤ applied 的既有守卫兜住），
//     FSM 里不留任何陈旧键。
func TestSnapshotInstallExcludesApply(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	gr := newGroup(0, 1, raft.NewMemoryStorage(), nil, rs, st,
		func(uint32, []*raftpb.Message) {}, AckQuorumMem, nil, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	defer rn.Stop()

	// 拉块回调阻塞在 gate 上：安装卡在第 4 步，安装临界区一直持有
	gate := make(chan struct{})
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		<-gate
		// 一块即完成：done=true，游标无意义，块内无键
		// （snapFetchResp 是 group_test.go 的既有拼包助手）
		return snapFetchResp(true, nil, nil), nil
	}

	idx, tm := uint64(100), uint64(2)
	snap := &raftpb.Snapshot{
		Data: encodeSnapDescriptor(snapDescriptor{ID: 7, Leader: 1, Index: idx}),
		Metadata: &raftpb.SnapshotMetadata{
			Index: &idx, Term: &tm, ConfState: &raftpb.ConfState{},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	installDone := make(chan struct{})
	go func() {
		defer close(installDone)
		gr.appendOnce(ctx, storageAppendWithSnap(snap))
	}()

	// 陈旧条目：index 远低于快照位点，安装完成后必须被整批跳过
	stale := []*raftpb.Entry{normalEntry(t, st, 5, 1, 101, "meta/topic/stale", "old")}
	applyTyp, applyTo := raftpb.MsgStorageApply, raft.LocalApplyThread
	applyMsg := &raftpb.Message{Type: &applyTyp, To: &applyTo, Entries: stale}

	applyDone := make(chan struct{})
	go func() {
		defer close(applyDone)
		gr.applyOnce(ctx, applyMsg)
	}()

	// 安装被 gate 卡住期间，apply 必须进不去
	select {
	case <-applyDone:
		t.Fatal("安装未完成时 apply 就跑完了——两条阶段没有互斥，陈旧条目会覆盖快照数据")
	case <-time.After(200 * time.Millisecond):
	}

	close(gate) // 放行安装
	select {
	case <-installDone:
	case <-time.After(10 * time.Second):
		t.Fatal("快照安装未在 10s 内完成")
	}
	select {
	case <-applyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("安装完成后 apply 仍未放行——互斥没有释放")
	}

	if got := gr.applied.Load(); got != idx {
		t.Fatalf("applied 应停在快照位点 %d，得到 %d——陈旧条目推进了位点", idx, got)
	}
	v, ok, err := st.Get([]byte("meta/topic/stale"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("陈旧条目不该落库（快照已覆盖该组状态），读到 %q", v)
	}
}
```

> 脚手架依赖（都已存在，不要另造）：`snapFetchResp`（`group_test.go:428`，`[1B done][4B BE 游标长][游标][块字节]`）、`normalEntry`（`group_test.go:737`）、`encodeSnapDescriptor` / `snapDescriptor`（`snapinstall.go`）、`store.Store.Get(key) ([]byte, bool, error)`（`internal/store/store.go:70`）。**若某个签名与此处不符，照实际改测试脚手架即可，断言语义不许动。**

- [ ] **Step 2: 跑测试确认失败**

```bash
go test ./internal/cluster/ -run TestSnapshotInstallExcludesApply -race -count=1
```

期望：FAIL，报「安装未完成时 apply 就跑完了」。

- [ ] **Step 3: 加互斥**

`group` 结构体在 `applyMu` 字段之后插入：

```go
	// installMu 串行化「快照安装（append 阶段）」与「条目 apply
	// （apply 阶段）」。
	//
	// 为什么 async 之后才需要：旧路径里两者都在 Ready 循环内顺序执行，
	// 物理上不可能重叠；async 把它们拆成两条协程，而 raft 明确允许不同
	// target 的本地消息乱序处理（raft.go:165-166）。安装会整体重写本组
	// FSM（wipeGroupKeys → 拉块 → 收口批次），一批安装前入队的
	// MsgStorageApply 若在安装之后落地，会把陈旧数据写回刚被覆盖的键。
	//
	// 与 applyMu 的分工（两把锁不嵌套、粒度不同，别合并）：
	//   - applyMu 是**短**临界区，只覆盖「批提交 + 推 applied」，与快照
	//     生成（groupStorage.Snapshot）互斥，保证视图与位点配对；
	//   - installMu 是**长**临界区，覆盖整个安装（分钟级）与整批 apply，
	//     只解决"安装 vs apply"这一件事。
	//
	// 陈旧批次不需要额外处理：安装收口时 applied 已跳到快照位点，
	// applyPhase 里 index ≤ applied 的既有守卫会把整批跳掉。
	installMu sync.Mutex
```

`appendOnce` 的快照分支改为：

```go
	if snap := m.GetSnapshot(); !raft.IsEmptySnap(snap) {
		gr.installMu.Lock()
		err := gr.installSnapshotWithRetry(ctx, snap)
		gr.installMu.Unlock()
		// ...（下面的 ctx 分支与 panic 分支不变）
	}
```

`applyOnce` 的 apply 改为：

```go
	gr.installMu.Lock()
	appliedCC := gr.applyPhase(m.GetEntries())
	gr.installMu.Unlock()
```

- [ ] **Step 4: 跑测试确认通过**

```bash
go test ./internal/cluster/ -run TestSnapshotInstallExcludesApply -race -count=1
```

期望：PASS。

- [ ] **Step 5: 快照全量回归**

```bash
go test ./internal/cluster/ -run 'Snapshot|Install|Snap' -race -count=1 && go test ./internal/cluster/ -race -count=1
```

期望：全部 PASS，无新增测试修改。

- [ ] **Step 6: 补充关键节点日志**

安装本身的进出日志已由 `installSnapshot` / `installSnapshotWithRetry` 覆盖（安装开始/完成/重试/预算耗尽），无需重复。**只补一条**：安装持锁导致 apply 长时间等待时留痕。在 `applyOnce` 的加锁处改为：

```go
	// 安装期 apply 会在这里排队：分钟级安装下这是正常现象，但必须可见
	// ——否则现场只表现为「读屏障迟迟不放行」而查不到原因。
	if !gr.installMu.TryLock() {
		start := time.Now()
		gr.installMu.Lock()
		if d := time.Since(start); d >= localQueueBlockWarn {
			gr.lg.Warn("apply 阶段等待快照安装让路", "waited", d.Round(time.Millisecond).String(),
				"entries", len(m.GetEntries()))
		}
	}
	appliedCC := gr.applyPhase(m.GetEntries())
	gr.installMu.Unlock()
```

- [ ] **Step 7: 补充意图注释**

自检：`installMu` 字段注释已写清「为什么 async 之后才需要」「与 applyMu 的分工」「陈旧批次为什么不用额外处理」（Step 3 的代码块里已含）；`appendOnce`/`applyOnce` 的加锁点各有一行说明。

- [ ] **Step 8: 提交**

```bash
git add internal/cluster/group.go internal/cluster/group_async_test.go
git commit -m "fix(cluster): 快照安装与 apply 阶段互斥——async 拆协程后新暴露的分叉窗口"
```

---

## Task 4: 流水线深度的可执行证据 + 阻塞可观测性收口

设计文档 §7 明确要求：§5.1 的坑一旦踩中，表现是**组静默卡死而非报错**，因此分发路径必须可观测。Task 2/3 已埋了阈值日志，本 task 补两件事：① 一条**直接证明流水线深度打开了**的用例（不依赖三机实测就能证伪）；② 组退出时的阻塞汇总打点。

**Files:**
- Modify: `internal/cluster/group.go`（`run` 的退出打点）
- Test: `internal/cluster/group_async_test.go`（新增 1 个用例）

**Interfaces:**
- Consumes: Task 2 的 `dispatchReady` / `appendCh`；Task 1 的 `persistPhase`
- Produces: 无新导出符号

- [ ] **Step 1: 写失败的测试——持久化阻塞时网络消息必须已经发出去**

追加到 `group_async_test.go`：

```go
// TestOutboundNotBlockedByPersist 本改造的收益命题，落成一条可证伪的
// 用例：一轮 Ready 里的网络消息必须在本地 append 消息**处理完成之前**
// 就已外发。
//
// 旧路径（同步 Ready）里这是不可能的——持久化在 group.go:396、外发在
// :417，leader 做 fsync 的那 1.8ms 里 MsgApp 一个字节都发不出去，确认链
// 于是变成「leader fsync → 网络 → follower fsync」两次串行相加，这正是
// quorum-fsync 档 +69% raft 机制税的来源。
//
// 本用例不测吞吐（那是三机实测的事），只测**顺序**：顺序对了，流水线
// 深度才有可能 > 1。若有人把 dispatchReady 改回「先入队再外发并等它完成」，
// 本用例立刻红。
func TestOutboundNotBlockedByPersist(t *testing.T) {
	gr, _, _ := newApplyTestGroup(t, nil)
	sent := make(chan struct{}, 1)
	gr.send = func(uint32, []*raftpb.Message) { sent <- struct{}{} }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 只起 append 阶段，且让它卡住：模拟一次慢 fsync
	gate := make(chan struct{})
	go func() {
		<-gr.appendCh
		<-gate // append 阶段"正在落盘"，一直不返回
	}()

	appendTyp, appendTo := raftpb.MsgStorageAppend, raft.LocalAppendThread
	gr.dispatchReady(ctx, raft.Ready{MustSync: true, Messages: []*raftpb.Message{
		msgTo(2, raftpb.MsgApp),
		{Type: &appendTyp, To: &appendTo},
	}})

	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("持久化尚未完成时 MsgApp 就该已经发出——外发被排在了本地写入之后，流水线深度仍是 1")
	}
	close(gate)
}
```

- [ ] **Step 2: 跑测试**

```bash
go test ./internal/cluster/ -run TestOutboundNotBlockedByPersist -race -count=1
```

期望：**PASS**（Task 2 的 `dispatchReady` 已经先外发后入队）。这是一条**回归护栏**而非驱动实现的失败测试——若它此时是红的，说明 Task 2 的顺序写反了，回去修 `dispatchReady`。

- [ ] **Step 3: 组退出时汇总打点**

`run` 的 defer 里，`stages.Wait()` 之后、`close(gr.doneCh)` 之前插入：

```go
		// 组退出汇总：本地阶段的阻塞总量是「存储侧跟不上」的唯一量化
		// 信号——它不为零就意味着主循环被拖住过、本组 tick 受过影响。
		// 只在退出时打一次（热路径零日志），长期非零由运维按 search_logs
		// 检索这条文案定位。
		ab, pb := gr.appendBlockNanos.Load(), gr.applyBlockNanos.Load()
		if ab > 0 || pb > 0 || gr.respDropped.Load() > 0 {
			gr.lg.Warn("本地存储阶段存在阻塞或丢弃（存储侧跟不上主循环）",
				"append_blocked", time.Duration(ab).Round(time.Millisecond).String(),
				"apply_blocked", time.Duration(pb).Round(time.Millisecond).String(),
				"append_handled", gr.appendCount.Load(),
				"apply_handled", gr.applyCount.Load(),
				"resp_dropped", gr.respDropped.Load())
		}
```

- [ ] **Step 4: 验证日志可被检索**

```bash
go test ./internal/cluster/ -run 'TestGroupSingleNodeProposeApply' -v -count=1 2>&1 | grep -E 'append 阶段(启动|退出)|apply 阶段(启动|退出)'
```

期望：能看到四条阶段起停日志（`testSlog` 把日志写进测试输出）。若一条都没有，说明阶段协程没被 `run` 起来——回 Task 2 Step 6。

- [ ] **Step 5: 补充意图注释**

自检：Step 3 的代码块已含「为什么只在退出时打一次」「这个数字意味着什么」。此外确认 `TestOutboundNotBlockedByPersist` 的 doc comment 明写它守的是**顺序**而非吞吐——否则将来有人会拿它当性能测试而误判。

- [ ] **Step 6: 全量回归 + 提交**

```bash
go test ./internal/cluster/ -race -count=1
```

```bash
git add internal/cluster/group.go internal/cluster/group_async_test.go
git commit -m "test(cluster): 流水线顺序护栏 + 本地阶段阻塞汇总打点"
```

---

## Task 5: 全量回归与跨平台验证

**Files:** 无代码改动（除非回归暴露问题）

**Interfaces:**
- Consumes: Task 1-4 的全部改动
- Produces: 一份可粘贴进 PR 的回归结论

- [ ] **Step 1: macOS `-race` 全量**

```bash
go test ./... -race -count=1 -timeout 40m
```

期望：全部 PASS。

**遇到失败先分诊**：`internal/cluster` 有一条已知偶发失败（backlog B12「满负载偶发失败一次，用例未定位」）。撞到时**重跑三次**确认可复现性；可稳定复现的一律按本次改动引入处理，**不许**记到 B12 头上；三次只中一次且与本改动无逻辑关联的，在回归结论里注明「疑似 B12，已记录未定位」。

- [ ] **Step 2: 恢复路径专项**

async 改的是 Ready 驱动方式，恢复判定表未动，但 B10/B11 系列是本仓库修过最深的坑，必须单独跑一遍留痕：

```bash
go test ./internal/cluster/ -run 'Recovery|Unclean|Restart|Clean' -race -count=3 -v
```

期望：三轮全 PASS。

- [ ] **Step 3: 交叉编译 Linux（历史教训：两处用例缺陷只在 Linux 上现形）**

```bash
GOOS=linux GOARCH=amd64 go build ./... && GOOS=linux GOARCH=amd64 go test -c -o /tmp/cluster.linux ./internal/cluster/ && GOOS=linux GOARCH=amd64 go test -c -o /tmp/e2e.linux ./test/e2e/
```

期望：三条命令都成功，产出两个二进制。

> 纪律（来自记忆 `prefer-cross-compile-over-remote-toolchain`）：**不要在远端装 Go 工具链**，本地 `go test -c` 之后 scp 过去跑。

- [ ] **Step 4: 32 位构建守卫**

```bash
GOOS=linux GOARCH=386 go build ./... && GOOS=linux GOARCH=arm go build ./...
```

期望：成功。（`maxSnapshotBytes` 的 int64 教训见 `group.go:80-82`；新加的 `atomic.Uint64` 字段在 32 位上有对齐要求，这一步是它的守卫。）

- [ ] **Step 5: 写回归结论 + 提交**

把 Step 1-4 的实际输出摘要写进提交信息：

```bash
git commit --allow-empty -m "chore(cluster): async 存储写入全量回归留痕

macOS -race 全量：<结论>
恢复路径 ×3 轮：<结论>
交叉编译 linux/amd64 + 386 + arm：<结论>
已知偶发（B12）命中情况：<结论>"
```

---

## Task 6: 三机实测验收

对应设计文档 §8 的验收标准 3/4/5。**本 task 需要三台测试机；没有机器时做到 Step 2 停下，交回审核。**

**Files:** 无代码改动

**Interfaces:**
- Consumes: Task 5 交叉编译产出的 `e2e.linux`
- Produces: 一份三档对照结论（fsync 摊销比 / fsync 吞吐 / mem 不劣化）

- [ ] **Step 1: 确认基线口径**

对照基线必须是**同一批机器上重跑的 seglog（main）**，不是历史数字。理由（记忆 `sharedlog-groupcommit-failed-2026-08-11` 的关键教训）：跨批次测试机同标称不同档，绝对吞吐不可比——2026-08-11 上午那批 −47% 的回退在当晚新机器上只剩 −9%。**每批必须重跑 main 做同环境对照，这条已经救过一次命。**

- [ ] **Step 2: 交叉编译并分发**

```bash
GOOS=linux GOARCH=amd64 go test -c -o /tmp/e2e.linux ./test/e2e/
```

e2e 走 `SQ_E2E_BROKER` 注入交叉编译的 broker。**没有测试机的执行者做到这里停下。**

- [ ] **Step 3: fsync 档主实验（≥3 轮交错，轮内配对）**

quorum-fsync，conc=16 与 conc=256 各跑 ≥3 轮，**交错执行**（main → async → main → async …），报中位数 + 离散度。

口径纪律（记忆 `cluster-perf-attribution-2026-08-12` 的两条留痕）：
- 绝对吞吐跨轮离散度可达 15-50%，**一律不可单独引用**，只有轮内配对比值可信；
- `runSendLoad` 只连 `endpoints[0]` 且客户端与 node1 同机（`test/e2e/sdk_cluster_bench_test.go:369`、`:134`），**C3-cross 是下界**——这个缺陷对两侧同等生效，配对比值不受影响。

期望：raft 机制税（S 单机 → C1 单节点集群）从 +69% 显著下降。

- [ ] **Step 4: fsync 摊销比（最能证伪的一条）**

跑 C1（单节点集群）/fsync/conc=16，同时采集盘的 sync 速率，算：

```
摊销比 = msg/s ÷ (同并发档的盘 sync/s)
```

基线是 **~1 条/fsync**（`C1/fsync/16 = 1263 msg/s`，3 个数据组，盘 conc=3 约 1120 sync/s）。对照锚：同档单机 standalone 是 ~8.4 条/fsync（pebble 把 16 个并发写者合成了一次）。

期望：**明显 > 1**。这是「流水线深度真的打开了」的直接证据——比吞吐数字更能证伪，因为吞吐可能被机器噪声掩盖，而摊销比是结构性的。

- [ ] **Step 5: mem 档不得劣化（硬底线）**

quorum-mem，conc=16/256 各 ≥3 轮交错。设计文档 §7 预期 mem 档收益小得多（机制税只有 +21%），**但劣化即验收失败**。

- [ ] **Step 6: 结论归档**

原始输出留在会话 scratchpad；结论写进 `docs/superpowers/specs/2026-08-12-raft-async-storage-writes-design.md` 顶部的状态行（「已评审」→「已实测：<结论摘要>」），并按记忆规范新增/更新一条 memory。

---

## Self-Review

**1. spec 覆盖**

| spec 章节 | 覆盖它的 task |
|---|---|
| §4 彻底切换、不留开关 | Task 2 Step 3（`raftConfig` 注释明写「不提供配置项」） |
| §5.1 local message 绝不走 `gr.send` | Task 2 Step 1（用例）+ Step 5（`deliverResponses` 路由表注释） |
| §5.2 `mem.Append` 先于 Responses | Task 1 Step 2（`persistPhase` 顺序）+ Task 2 Step 5（`appendOnce` 顺序 + doc comment） |
| §5.3 append 严 / apply 松 | Task 2 Step 5（`runApply` doc comment 明写「不得合成一条协程」）；`applyOnce` 不做任何 fsync |
| §6-① apply 合批的 `applyMu` 竞争面 | Task 3（`installMu` 字段注释里的「与 applyMu 的分工」）；`applyEntries` 本身未动，合批边界随 `MsgStorageApply` 批边界走 |
| §6-② `ccWaiters` 唤醒时机 | Task 2 Step 5（`applyOnce` 的完整论证：Advance 的职责由 `deliverResponses` 承接） |
| §6-③ `SaveConfState` 与段冲刷顺序 | Task 1 Step 3（`applyPhase` 逐字搬运，`flushSeg` 顺序不变；apply 阶段串行消费，批内顺序天然保持） |
| §6-④ 快照安装分支 | Task 2 Step 5（迁到 `appendOnce`）+ Task 3（与 apply 互斥） |
| §7 可观测性（通道深度、阻塞时长、配对） | Task 2 Step 10 + Task 4 Step 3 |
| §8-1 语义锚（e2e 零修改、单测修改需说明） | Task 2 Step 8（预告全部 2 处）+ Step 9 |
| §8-2 macOS `-race` 全量 | Task 5 Step 1 |
| §8-3 三机实测 | Task 6 Step 3 |
| §8-4 mem 档不劣化 | Task 6 Step 5 |
| §8-5 fsync 摊销比 | Task 6 Step 4 |
| §8-6 可用 `search_logs` 定位 | Task 4 Step 4（日志可检索性验证） |

**2. 占位符扫描**：无 TBD / "适当处理" / "类似 Task N"。Task 1 Step 3 的 `for` 循环体标注为"逐字搬运"并给出确切行号区间（`group.go:447-528`），是搬运指令而非占位符。Task 3 Step 1 对 `encodeSnapFetchResp` / `st.Get` 的实参形态给了明确的兜底指令（以实际签名为准，断言语义不动）。

**3. 类型一致性**：`localMsg{m, mustSync}` 在 Task 2 定义，Task 3/4 的用例按同一形态构造；`persistPhase(hs *raftpb.HardState, ents []*raftpb.Entry, sync bool)` 与 `raftStore.Persist`（`raftstore.go:233`）签名一致；`applyPhase(ents []*raftpb.Entry) []ccApplied` 与 `applyEntries`（`group.go:967`）、`ccApplied`（既有类型）一致；`raft.LocalAppendThread` / `raft.LocalApplyThread` 是 `uint64`（`raft/v3@v3.7.0/raft.go:42,46`），与 `m.GetTo()` 同型；`raftpb.MsgStorageAppend` 等别名见 `raftpb/alias.go:41-44`。

**4. 一处必须承认的取舍**：`dispatchReady` 把 leader 变更的公布挪到了「本地写入已入队」而非「已完成」之后，比旧路径弱一档。已在 `dispatchReady` 的 doc comment 里逐条写明理由与安全边界（raft 自身的选举安全由 MsgVoteResp 随 Responses 在 fsync 之后投递保证）。**审核时请把这一条单独看一眼**——它是本计划里唯一一处「不是等价平移」的语义改动。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-12-raft-async-storage-writes.md`. Two execution options:

**1. Subagent-Driven (recommended)** - 每个 task 派一个全新 subagent，task 之间两阶段评审，快速迭代

**2. Inline Execution** - 在当前会话内按 `executing-plans` 批量执行，带检查点

Which approach?
