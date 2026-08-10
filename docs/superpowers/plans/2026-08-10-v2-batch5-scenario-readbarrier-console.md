# V2 batch⑤：集群场景测试 + 读屏障 + 控制台集群视图 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **同时必须遵守 `instrumenting-code`**：每个实现类 task 都带有「加关键节点日志」与「加注释」两个 step，它们与「写测试」同级，不是可选项。

**Goal:** 交付 batch④ 显式外推的三件事——B8.3 四类集群场景测试（kill -9 / 少数派不可写 / 滚动重启 / 断电模拟，外加毒消息→DLQ 与 producer 提交后立退）、完整的 read-index / apply barrier 线性一致读屏障、admin 控制台集群视图与 follower 写转发 UX。

**Architecture:** 三个互相独立、可分别合入的部分。

- **Part A（Task 1-3）读屏障**：在 `group` 上新增 raft `ReadIndex` 原语，读状态经 `Ready.ReadStates` 回流，等本节点 `applied` 追过该 index 即放行；`Manager.ReadBarrier(ctx, g)` 做「排队成批」的合流（后到的等待者进下一批，绝不复用早于自己到达时刻的 read index）；接入 `deliver.Receive` 读路径，配置开关控制。
- **Part B（Task 4-8）场景测试**：在既有 `test/e2e` 三节点进程级 harness 上抽出可复用的 `procCluster`（kill / graceful stop / restart / SIGSTOP 暂停），再写四类场景。**网络分区用「杀掉 3 之 2」模拟少数派**——不依赖 iptables/root，可在 macOS 与两台 Linux 上同样跑。不变量对账用「确认集对账器」：producer 每次 Send 成功即登记 msgID，收尾时断言全部 msgID 都被消费到（允许重复、不允许丢失）。
- **Part C（Task 9-12）控制台**：先出可点击原型并过用户确认门（CLAUDE.md 硬约束），再做后端 `GET /admin/cluster`、前端 `Cluster.tsx`，最后补 follower 写转发的 UX。

**Tech Stack:** Go 1.23 / etcd-raft v3（vendored）/ Pebble；`test/e2e` 独立 module（`//go:build e2e`，`SQ_E2E_BROKER` 注入预编译 broker）；前端 React 19 + TypeScript + Vite + vitest；原型站 `prototypes/base/`（静态 HTML + `shared/app.css`）。

## Global Constraints

- **语言**：所有注释、日志、文档、提交信息一律中文。
- **提交信息结尾**：每条 commit message 结尾必须是 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`（与本仓既有 git log 一致）。
- **禁止触碰**：`/Users/xushixin/workspace/sq-m5`、`/Users/xushixin/workspace/sq-m5b` 两个目录一律不得修改。
- **文档提交去向**：`docs/` 下的 spec / plan / backlog 直接提交到 `main`；代码提交到当前分支。
- **日志**：一律 `slog`（`gr.lg` / `d.logger` / `s.logger`），**禁止** `fmt.Printf`；热循环（tick、每条消息）降级到 Debug 或不打。
- **注释**：新建文件必须有文件头注释（职责 + 边界）；导出方法必须有 doc 注释（参数 / 返回 / 注意）；非显然分支必须有「为什么」的中文行内注释。
- **Linux 是一等验证平台**：每个 part 合入前，除本机 macOS 外，还要在 `100.90.99.61`（联想，4 核）与 `47.80.240.57`（临时云主机，2 核 1.6G，可能被回收）上各跑一次。**远端一律本地交叉编译后 scp，绝不在远端装 Go 工具链**：
  ```bash
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o /tmp/cluster.test ./internal/cluster
  ```
  e2e 走 `SQ_E2E_BROKER` 注入同样交叉编译出来的 broker 二进制。
- **性能类验证必记内存峰值**（`memPeakSampler` 已有，Linux 读 `/proc/<pid>/status` 的 VmHWM）。
- **证据入提交说明**：每个 task 的 commit body 写清跑了哪些测试、在哪个平台、关键数字。
- **不得改动的既有约束**：`DisableProposalForwarding: true`（follower 提案必须报错，不静默转发）；`PreVote: true`；持久化 / apply 失败一律 `panic` fail-stop。
- **untracked 文件不要动**：`.superdev/`、`docs/.DS_Store`、`docs/brand/`、`docs/superpowers/.DS_Store`、`docs/superpowers/plans/2026-08-07-sysinfo-console-and-sig-case-fix.md`。
- **`GOPROXY=https://goproxy.cn,direct`**。

---

# Part A：read-index / apply barrier 完整读屏障

## 背景（实现者必读）

`propose` 已经做到「等 apply 而非等 commit」，因此**写己之读**是成立的。缺的是另一半：**别人写、我读**。一个已经被网络隔离、但还没意识到自己失去领导权的旧 leader，本地 `applied` 停在老位点，此时它服务的读会返回过期数据。raft 给的解法是 read-index：

1. 读之前向 raft 要一个 `readIndex`（raft 内核以 `ReadOnlySafe` 档确保：leader 已在当前任期提交过条目，且刚刚收到过多数派心跳确认自己仍是 leader）；
2. 等本节点 `applied >= readIndex`；
3. 此时读到的状态一定包含了「读请求发起之前已经确认的全部写」。

vendored raft 的接口：`rn.ReadIndex(ctx, rctx []byte) error`，结果经 `Ready().ReadStates []raft.ReadState` 回流，`ReadState{Index uint64, RequestCtx []byte}`。本仓已有的 16 字节提案头约定 `[8B 提案者 nodeID][8B waiter id]` 原样复用做 `RequestCtx`——跨节点 waiter id 会撞车，必须校验提案者是本节点。

**本批不做**：follower 上的读屏障（raft 的 `stepFollower` 会把 `MsgReadIndex` 转发给 leader，但 sq 的读路径目前全是 leader-only，follower 读是 V3 的独立议题）。非 leader 调用直接快速失败为 `ErrNotLeader`。

---

## Task 1: group 层 ReadIndex 原语

**Files:**
- Create: `internal/cluster/readbarrier.go`
- Create: `internal/cluster/readbarrier_test.go`
- Modify: `internal/cluster/group.go`（`group` 结构体加字段、`newGroup` 初始化、`run` 存 lifeCtx、`handleReady` 处理 `rd.ReadStates` 与 apply 后唤醒）

**Interfaces:**
- Produces：`func (gr *group) readIndexOnce(ctx context.Context) error`、`func (gr *group) stepReadStates(rss []raft.ReadState)`、`func (gr *group) notifyReadWaiters(applied uint64)`、`func readWaiterInfo(rctx []byte, self uint64) (id uint64, ok bool)`
- Consumes：既有 `gr.nextID`（提案 id 计数器，与 propWaiters/ccWaiters 共用）、`gr.mu`、`gr.applied`（`atomic.Uint64`）、`gr.leader()`、`gr.notLeaderErr()`

- [ ] **Step 1: 写失败的测试（read waiter 的三条不变量）**

新建 `internal/cluster/readbarrier_test.go`：

```go
package cluster

import (
	"encoding/binary"
	"testing"

	"go.etcd.io/raft/v3"
)

// TestReadWaiterInfoRejectsForeignProposer 读状态的 RequestCtx 与提案头
// 同布局：[8B 提案者][8B waiter id]。waiter id 是每节点独立计数器，跨
// 节点必然撞车——提案者不是本节点时必须拒绝，否则别人的读状态会唤醒
// 我方等待者，屏障形同虚设。
func TestReadWaiterInfoRejectsForeignProposer(t *testing.T) {
	rctx := make([]byte, 16)
	binary.BigEndian.PutUint64(rctx[:8], 7)
	binary.BigEndian.PutUint64(rctx[8:], 42)

	if id, ok := readWaiterInfo(rctx, 7); !ok || id != 42 {
		t.Fatalf("本节点提案的读状态应被识别，得到 id=%d ok=%v", id, ok)
	}
	if _, ok := readWaiterInfo(rctx, 8); ok {
		t.Fatal("提案者非本节点的读状态不得识别（跨节点 id 撞车会假放行）")
	}
	if _, ok := readWaiterInfo(rctx[:15], 7); ok {
		t.Fatal("长度不足 16B 的 RequestCtx 不得识别")
	}
}

// TestStepReadStatesFillsIndexAndNotifiesWhenCaughtUp 读状态回流时：
// applied 已追过 → 立即放行；未追上 → 记下 index 挂起，等 apply 推进
// 后由 notifyReadWaiters 放行。
func TestStepReadStatesFillsIndexAndNotifiesWhenCaughtUp(t *testing.T) {
	gr := &group{selfID: 1, readWaiters: map[uint64]*readWait{}, lg: testLogger()}

	// ① applied 已追过：回流即放行
	gr.applied.Store(100)
	done := make(chan struct{})
	gr.readWaiters[1] = &readWait{ch: done}
	gr.stepReadStates([]raft.ReadState{{Index: 90, RequestCtx: readCtxBytes(1, 1)}})
	select {
	case <-done:
	default:
		t.Fatal("applied(100) 已追过 readIndex(90)，应立即放行")
	}
	if len(gr.readWaiters) != 0 {
		t.Fatalf("放行后 waiter 应被摘除，残留 %d 个", len(gr.readWaiters))
	}

	// ② applied 未追上：挂起，等 apply 推进后放行
	pending := make(chan struct{})
	gr.readWaiters[2] = &readWait{ch: pending}
	gr.stepReadStates([]raft.ReadState{{Index: 150, RequestCtx: readCtxBytes(1, 2)}})
	select {
	case <-pending:
		t.Fatal("applied(100) 未追到 readIndex(150)，不应放行")
	default:
	}
	gr.applied.Store(150)
	gr.notifyReadWaiters(150)
	select {
	case <-pending:
	default:
		t.Fatal("applied 追平后应放行")
	}
	if len(gr.readWaiters) != 0 {
		t.Fatalf("放行后 waiter 应被摘除，残留 %d 个", len(gr.readWaiters))
	}
}

// readCtxBytes 构造与提案头同布局的 RequestCtx（测试辅助）。
func readCtxBytes(self, id uint64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], self)
	binary.BigEndian.PutUint64(b[8:], id)
	return b
}
```

若 `testLogger()` 在本包不存在，用 `slog.New(slog.NewTextHandler(io.Discard, nil))` 就地替代，并把它提到包内共享辅助函数。

- [ ] **Step 2: 跑测试确认失败**

```bash
go test ./internal/cluster -run 'TestReadWaiterInfo|TestStepReadStates' -v
```

预期：编译失败，`undefined: readWaiterInfo` / `undefined: readWait` / `gr.readWaiters undefined`。

- [ ] **Step 3: 新建 `internal/cluster/readbarrier.go`**

```go
// Package cluster 的读屏障实现：raft read-index + apply barrier。
//
// 职责：
//   - 向 raft 索取一个 readIndex（ReadOnlySafe 档：leader 已在当前任期
//     提交过条目 + 刚被多数派心跳确认仍是 leader）
//   - 等本节点 applied 追过该 index 后放行，此时的本地读一定包含了读
//     请求发起之前已确认的全部写（线性一致读）
//   - 把并发读者按「排队成批」合流，一轮 read-index 服务一批等待者
//
// 边界：
//   - 只服务 leader：非 leader 直接返回 ErrNotLeader，不做 follower 读
//     转发（raft 内核会把 MsgReadIndex 转给 leader，但 sq 的读路径目前
//     全是 leader-only，follower 读属 V3 议题）
//   - 不碰 FSM：屏障只负责「什么时候可以读」，读什么由调用方决定
//   - 不做超时策略：超时由调用方的 ctx 承担（配置项在 Manager 层）
package cluster

import (
	"context"
	"encoding/binary"
	"time"

	"go.etcd.io/raft/v3"
)

// readWait 一个在等的读屏障等待者。
//
// index 在读状态回流之前是 0：0 表示「readIndex 还没回来」，notifyRead
// Waiters 必须跳过它——否则 applied 恰好非零就会把还没拿到 readIndex 的
// 等待者提前放行，屏障退化成一次空等待。
type readWait struct {
	index uint64
	ch    chan struct{}
}

// readIndexOnce 走一轮完整的 read-index：索取 readIndex → 等 applied 追平。
//
// 参数：
//   - ctx: 控制整轮等待；超时/取消时摘除 waiter 防泄漏
//
// 返回：
//   - nil：本节点 applied 已追过 readIndex，可以安全读
//   - ErrNotLeader：本节点已知不是 leader（读屏障只在 leader 成立）
//   - ctx.Err()：超时或取消
//
// 注意：
//   - 与 propose 同款的快速失败：已探明 leader 是他人就不进 raft，省掉
//     一次必然超时的等待
//   - RequestCtx 复用提案头布局 [8B 提案者][8B waiter id]，跨节点 id
//     撞车由提案者校验挡住（见 readWaiterInfo）
func (gr *group) readIndexOnce(ctx context.Context) error {
	if lead := gr.leader(); lead != gr.selfID {
		gr.lg.Debug("读屏障被拒：本节点非 leader", "lead", lead)
		return gr.notLeaderErr()
	}
	id := gr.nextID.Add(1)
	w := &readWait{ch: make(chan struct{})}
	gr.mu.Lock()
	gr.readWaiters[id] = w
	gr.mu.Unlock()

	rctx := make([]byte, 16)
	binary.BigEndian.PutUint64(rctx[:8], gr.selfID)
	binary.BigEndian.PutUint64(rctx[8:], id)

	start := time.Now()
	if err := gr.rn.ReadIndex(ctx, rctx); err != nil {
		gr.removeReadWaiter(id)
		gr.lg.Warn("read-index 提交失败", "id", id, "err", err)
		return err
	}
	select {
	case <-w.ch:
		gr.lg.Debug("读屏障放行", "id", id, "read_index", w.index,
			"applied", gr.applied.Load(), "cost", time.Since(start))
		return nil
	case <-ctx.Done():
		gr.removeReadWaiter(id)
		// 等待期间领导权已经易主：读屏障在本节点上再也不会满足，归入
		// ErrNotLeader 让调用方换节点重试，而不是当成不可重试的超时
		if cur := gr.leader(); cur != gr.selfID {
			gr.lg.Debug("读屏障等待期间失去领导权", "id", id, "lead", cur)
			return gr.notLeaderErr()
		}
		gr.lg.Warn("读屏障等待超时", "id", id, "read_index", w.index,
			"applied", gr.applied.Load(), "cost", time.Since(start), "err", ctx.Err())
		return ctx.Err()
	}
}

// stepReadStates 消费本轮 Ready 回流的读状态：填 index，若 applied 已经
// 追过就地放行。在 handleReady 的 apply 之前调用——本轮 apply 之后还会
// 再调一次 notifyReadWaiters，两处配合覆盖「回流早于 apply」与「回流晚于
// apply」两种顺序。
func (gr *group) stepReadStates(rss []raft.ReadState) {
	if len(rss) == 0 {
		return
	}
	applied := gr.applied.Load()
	gr.mu.Lock()
	defer gr.mu.Unlock()
	for _, rs := range rss {
		id, ok := readWaiterInfo(rs.RequestCtx, gr.selfID)
		if !ok {
			// 别的节点发起的读状态（raft 会把 follower 的 MsgReadIndex
			// 转给 leader，leader 侧因此会看到非本节点的 RequestCtx）：
			// 本节点没有对应等待者，跳过即可，不是异常
			continue
		}
		w, ok := gr.readWaiters[id]
		if !ok {
			// 等待者已超时摘除：读状态迟到，丢弃
			continue
		}
		w.index = rs.Index
		if applied >= w.index {
			close(w.ch)
			delete(gr.readWaiters, id)
		}
	}
}

// notifyReadWaiters 放行所有 readIndex 已被 applied 追过的等待者。
// 在 handleReady 的 apply 循环之后调用（applied 刚被推进）。
func (gr *group) notifyReadWaiters(applied uint64) {
	gr.mu.Lock()
	defer gr.mu.Unlock()
	for id, w := range gr.readWaiters {
		// index==0 表示 readIndex 还没回流：此时放行等于没有屏障
		if w.index == 0 || applied < w.index {
			continue
		}
		close(w.ch)
		delete(gr.readWaiters, id)
	}
}

// removeReadWaiter 摘除等待者但不关闭 channel（调用方已放弃等待）。
func (gr *group) removeReadWaiter(id uint64) {
	gr.mu.Lock()
	delete(gr.readWaiters, id)
	gr.mu.Unlock()
}

// readWaiterInfo 从 ReadState.RequestCtx 解出 waiter id。
//
// 返回：
//   - id: waiter id
//   - ok: 长度足够且提案者是本节点时为 true
//
// 注意：ok=false 时绝不可唤醒本地等待者——waiter id 是每节点独立计数器，
// 跨节点必然撞车，裸 id 放行会把别节点的读状态当成自己的屏障满足。
func readWaiterInfo(rctx []byte, self uint64) (id uint64, ok bool) {
	if len(rctx) < 16 {
		return 0, false
	}
	if binary.BigEndian.Uint64(rctx[:8]) != self {
		return 0, false
	}
	return binary.BigEndian.Uint64(rctx[8:16]), true
}
```

- [ ] **Step 4: 在 `group` 上挂字段并接进 `handleReady`**

`internal/cluster/group.go`，在 waiter 双命名空间那段字段后追加（约 `internal/cluster/group.go:163` 之后）：

```go
	// readWaiters 读屏障等待者（第三个命名空间）：与 propWaiters/ccWaiters
	// 共用 gr.mu 与 gr.nextID 计数器，但读状态经 Ready.ReadStates 回流、
	// 不走日志条目，因此不会与前两者交叉误唤。
	readWaiters map[uint64]*readWait
```

`newGroup` 的结构体字面量里补一行（`ccWaiters:` 那行之后）：

```go
		readWaiters:    make(map[uint64]*readWait),
```

`handleReady` 里，把 `rd.ReadStates` 的处理放在第 2 步（发送 Messages）之后、第 3 步 apply 之前：

```go
	// 2.5 读状态回流：raft 已确认本节点在当前任期仍是 leader，给出的
	//     readIndex 是本轮读屏障的下界。放在 apply 之前处理只是为了拿到
	//     index；真正放行由 index<=applied 决定，apply 之后还会再扫一次。
	gr.stepReadStates(rd.ReadStates)
```

apply 循环结束、`gr.rn.Advance()` 之后（与 ccWaiters 通知同段），追加：

```go
	// 读屏障放行必须晚于 apply：applied 是本轮 apply 推进的，早于它扫描
	// 只会白扫一遍，屏障要多等一整轮 Ready 才放行。
	gr.notifyReadWaiters(gr.applied.Load())
```

- [ ] **Step 5: 加关键节点日志**

按 `instrumenting-code` 逐条落实（多数已写在 Step 3 的代码里，此处是自检清单，缺哪条补哪条）：

- `readIndexOnce` 入口的非 leader 快速失败：`Debug`（高频，客户端错发是常态）
- `rn.ReadIndex` 失败：`Warn`，带 `id` 与 `err`
- 放行成功：`Debug`，带 `id` / `read_index` / `applied` / `cost`——**成功路径不静默**
- 等待超时：`Warn`，带 `id` / `read_index` / `applied` / `cost` / `err`
- 等待期间失去领导权：`Debug`，带 `lead`
- `stepReadStates` / `notifyReadWaiters` 属每轮 Ready 的热路径，**不打日志**（热循环规则），可观测性由 `readIndexOnce` 的出入口承担

- [ ] **Step 6: 加注释**

- `readbarrier.go` 文件头：职责 + 边界（已在 Step 3 给出，照抄）
- `readIndexOnce` / `stepReadStates` / `notifyReadWaiters` / `readWaiterInfo` 的 doc 注释（同上）
- `readWait.index == 0` 的含义、`stepReadStates` 里「非本节点 RequestCtx 属正常」的原因、`notifyReadWaiters` 必须晚于 apply 的原因——这三处「为什么」的行内注释缺一不可

- [ ] **Step 7: 跑测试确认通过**

```bash
go test ./internal/cluster -run 'TestReadWaiterInfo|TestStepReadStates' -v && go test ./internal/cluster
```

预期：新测试 PASS，既有 cluster 包全绿。

- [ ] **Step 8: 提交**

```bash
git add internal/cluster/readbarrier.go internal/cluster/readbarrier_test.go internal/cluster/group.go
git commit -m "$(cat <<'EOF'
feat(cluster): group 层 read-index 原语——读状态经 ReadStates 回流，applied 追平后放行

线性一致读的第一半：向 raft 索取 readIndex（ReadOnlySafe 档确保 leader
在当前任期提交过条目且刚被多数派心跳确认），等本节点 applied 追过该
index 再放行。RequestCtx 复用提案头布局 [8B 提案者][8B waiter id]，跨
节点 id 撞车由提案者校验挡住——裸 id 放行会把别节点的读状态当成自己的
屏障满足。

readWait.index==0 表示 readIndex 尚未回流，notifyReadWaiters 必须跳过：
否则 applied 非零就会把还没拿到 index 的等待者提前放行，屏障退化成一次
空等待。放行扫描晚于 apply（applied 由本轮 apply 推进）。

验证：go test ./internal/cluster 全绿（macOS）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Manager.ReadBarrier —— 排队成批合流 + 配置开关

**Files:**
- Modify: `internal/cluster/readbarrier.go`（加批次合流）
- Modify: `internal/cluster/group.go`（`run` 存 lifeCtx、结构体加批次字段、`newGroup` 初始化）
- Modify: `internal/cluster/manager.go`（`Manager.ReadBarrier`）
- Modify: `internal/config/config.go`（`ClusterConfig` 两个新字段 + 默认值）
- Modify: `internal/cluster/readbarrier_test.go`（加合流测试）

**Interfaces:**
- Produces：`func (m *Manager) ReadBarrier(ctx context.Context, g uint32) error`、`func (gr *group) readBarrier(ctx context.Context) error`、`ClusterConfig.ReadBarrier bool`、`ClusterConfig.ReadBarrierTimeout time.Duration`、`Options.ReadBarrier bool`、`Options.ReadBarrierTimeout time.Duration`
- Consumes：Task 1 的 `readIndexOnce`

**为什么不是简单的 singleflight（实现者必读）：** 朴素合流会让「后到的读者复用正在进行中那一轮的 readIndex」。那一轮的 index 是在后到者**发起之前**取得的，中间被确认的写它看不到——线性一致直接破功。正确形态是**排队成批**：后到者一律排进「下一批」，一批的 read-index 调用发生在这批所有成员都已入队之后。等待者数没有上限，但在途轮次恒为 1，读 QPS 再高也只有一条 read-index 流。

- [ ] **Step 1: 写失败的测试（合流不得复用早于自己的 readIndex）**

追加到 `internal/cluster/readbarrier_test.go`：

```go
// TestReadBarrierBatchesQueueNextRound 合流的正确性红线：一轮在途时，
// 后到的等待者必须排进下一批，绝不复用当前这一轮的结果——当前轮的
// readIndex 取自后到者发起之前，中间被确认的写它看不到。
func TestReadBarrierBatchesQueueNextRound(t *testing.T) {
	gr := &group{selfID: 1, readWaiters: map[uint64]*readWait{}, lg: testLogger()}
	gr.lifeCtx = context.Background()
	gr.barrierTimeout = time.Second

	rounds := make(chan chan error, 4) // 每轮把自己的「放行开关」交出来
	gr.readIndexFn = func(ctx context.Context) error {
		release := make(chan error, 1)
		rounds <- release
		select {
		case err := <-release:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	first := make(chan error, 1)
	go func() { first <- gr.readBarrier(context.Background()) }()
	r1 := <-rounds // 第一轮已在途

	// 第一轮在途期间到达的等待者：必须触发第二轮，而不是搭第一轮的车
	second := make(chan error, 1)
	go func() { second <- gr.readBarrier(context.Background()) }()
	select {
	case <-rounds:
		t.Fatal("第一轮尚未结束就发起了第二轮（应排队等待）")
	case <-time.After(100 * time.Millisecond):
	}

	r1 <- nil
	if err := <-first; err != nil {
		t.Fatalf("第一轮应成功，得到 %v", err)
	}
	// 第一轮结束后，排队的等待者必须触发一轮**新的** read-index
	select {
	case r2 := <-rounds:
		r2 <- nil
	case <-time.After(2 * time.Second):
		t.Fatal("第一轮结束后未为排队等待者发起第二轮 read-index")
	}
	if err := <-second; err != nil {
		t.Fatalf("第二轮应成功，得到 %v", err)
	}
}

// TestReadBarrierBatchSharesOneRound 同一批（一轮在途之前同时到达）的
// 等待者共享一次 read-index：读 QPS 再高，在途轮次恒为 1。
func TestReadBarrierBatchSharesOneRound(t *testing.T) {
	gr := &group{selfID: 1, readWaiters: map[uint64]*readWait{}, lg: testLogger()}
	gr.lifeCtx = context.Background()
	gr.barrierTimeout = time.Second

	var calls atomic.Int64
	gate := make(chan struct{})
	gr.readIndexFn = func(ctx context.Context) error {
		calls.Add(1)
		<-gate
		return nil
	}

	const n = 8
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() { errs <- gr.readBarrier(context.Background()) }()
	}
	// 等这批全部入队（批次计数到 n）后再放行
	deadline := time.Now().Add(2 * time.Second)
	for gr.batchSize() < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := gr.batchSize(); got != n {
		t.Fatalf("应有 %d 个等待者排在同一批，实得 %d", n, got)
	}
	close(gate)
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("等待者 %d 应成功，得到 %v", i, err)
		}
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("同一批应只走一次 read-index，实走 %d 次", c)
	}
}
```

测试文件顶部 import 补 `"context"`、`"sync/atomic"`、`"time"`。

- [ ] **Step 2: 跑测试确认失败**

```bash
go test ./internal/cluster -run 'TestReadBarrierBatch' -v
```

预期：编译失败，`gr.lifeCtx` / `gr.barrierTimeout` / `gr.readIndexFn` / `gr.readBarrier` / `gr.batchSize` 全部 undefined。

- [ ] **Step 3: 在 `group` 上加批次字段并存 lifeCtx**

`internal/cluster/group.go`，`readWaiters` 字段之后追加：

```go
	// 读屏障的「排队成批」合流状态（brMu 独立于 mu：合流只调度轮次，
	// 不碰 waiter 表，两把锁不嵌套）。
	// running=true 表示有一轮 read-index 在途；此时到达的等待者一律排进
	// next 批，绝不搭当前这一轮的车——当前轮的 readIndex 取自它们发起
	// 之前，中间被确认的写它们看不到，复用即破坏线性一致。
	brMu      sync.Mutex
	brNext    *barrierBatch
	brRunning bool
	// barrierTimeout 单轮 read-index 的时间预算（每轮独立计时，不随某个
	// 等待者的 ctx 走——一个等待者放弃不该让整批失败）。
	barrierTimeout time.Duration
	// lifeCtx 是 run 循环的生命周期 ctx：合流的驱动 goroutine 用它做
	// 父 ctx，组退出时在途轮次随之取消。
	lifeCtx context.Context
	// readIndexFn 单轮 read-index 的执行体，默认 readIndexOnce；测试注入
	// 假实现以观察轮次调度（合流逻辑与 raft 交互解耦）。
	readIndexFn func(ctx context.Context) error
```

`newGroup` 字面量后补齐（`readWaiters:` 之后），并在 `gr.nextID.Store(...)` 附近加：

```go
	gr.barrierTimeout = 3 * time.Second // 默认值；装配方经 Manager 覆盖
	gr.readIndexFn = gr.readIndexOnce
```

`run(ctx)` 开头（`ticker := ...` 之前）加：

```go
	// 存生命周期 ctx：读屏障的合流驱动 goroutine 以它为父 ctx，组退出时
	// 在途的 read-index 轮次随之取消，不会挂着等到 barrierTimeout。
	gr.lifeCtx = ctx
```

- [ ] **Step 4: 实现合流（追加到 `internal/cluster/readbarrier.go`）**

```go
// barrierBatch 一批共享同一轮 read-index 的等待者。
//
// n 只用于测试观察批次规模；生产路径不读它。
type barrierBatch struct {
	done chan struct{}
	err  error
	n    int
}

// readBarrier 等一次读屏障，并发调用按「排队成批」合流。
//
// 参数：
//   - ctx: 本调用方的等待预算；超时只影响本调用方，不影响这一批的其他人
//
// 返回：
//   - nil：可以安全读
//   - ErrNotLeader / ctx.Err() / raft 错误：见 readIndexOnce
//
// 注意：
//   - 一轮在途时到达的等待者排进下一批（见 brRunning 字段注释的红线说明）
//   - 每轮用独立的 barrierTimeout 计时，父 ctx 是组的 lifeCtx——某个调用方
//     放弃不该让整批失败
func (gr *group) readBarrier(ctx context.Context) error {
	gr.brMu.Lock()
	if gr.brNext == nil {
		gr.brNext = &barrierBatch{done: make(chan struct{})}
	}
	b := gr.brNext
	b.n++
	if !gr.brRunning {
		gr.brRunning = true
		go gr.runBarrierRounds()
	}
	gr.brMu.Unlock()

	select {
	case <-b.done:
		return b.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runBarrierRounds 串行驱动读屏障轮次：取走当前排队批 → 跑一轮
// read-index → 唤醒该批 → 若期间又排了新批就继续，否则收摊。
//
// 串行是设计而非限制：在途轮次恒为 1，读 QPS 再高也只有一条 read-index
// 流；等待者数无上限，代价只是多等一轮。
func (gr *group) runBarrierRounds() {
	for {
		gr.brMu.Lock()
		b := gr.brNext
		if b == nil {
			gr.brRunning = false
			gr.brMu.Unlock()
			return
		}
		gr.brNext = nil
		gr.brMu.Unlock()

		parent := gr.lifeCtx
		if parent == nil {
			parent = context.Background()
		}
		ctx, cancel := context.WithTimeout(parent, gr.barrierTimeout)
		b.err = gr.readIndexFn(ctx)
		cancel()
		if b.err != nil {
			gr.lg.Warn("读屏障一轮失败", "waiters", b.n, "err", b.err)
		} else {
			gr.lg.Debug("读屏障一轮放行", "waiters", b.n)
		}
		close(b.done)
	}
}

// batchSize 返回当前排队批的等待者数（测试观察用；无排队批时返回 0）。
func (gr *group) batchSize() int {
	gr.brMu.Lock()
	defer gr.brMu.Unlock()
	if gr.brNext == nil {
		return 0
	}
	return gr.brNext.n
}
```

- [ ] **Step 5: `Manager.ReadBarrier` + Options 透传**

`internal/cluster/manager.go`，紧挨 `Manager.IsLeader` 之后加：

```go
// ReadBarrier 等 g 组的线性一致读屏障：返回 nil 后，本节点的本地读一定
// 包含了本次调用发起之前已被确认的全部写。
//
// 参数：
//   - ctx: 调用方的等待预算（并发调用合流，某个调用方放弃不影响其他人）
//   - g: 数据组号
//
// 返回：
//   - nil：可以安全读
//   - ErrNotLeader：本节点不是该组 leader（读屏障只在 leader 成立）
//   - ctx.Err() 或 raft 错误
//
// 注意：
//   - 读屏障关闭（Options.ReadBarrier=false）时恒返回 nil，零开销
//   - 组不存在按编程错误处理，返回 ErrNotLeader 让调用方换节点重试
func (m *Manager) ReadBarrier(ctx context.Context, g uint32) error {
	if !m.readBarrier {
		return nil
	}
	gr, ok := m.groups[g]
	if !ok {
		m.lg.Warn("读屏障请求了不存在的组", "g", g)
		return fmt.Errorf("%w: 组 %d 不存在于本节点", ErrNotLeader, g)
	}
	return gr.readBarrier(ctx)
}
```

`Options` 加两个字段（挨着既有的 `SnapshotViewTTL` 一类可选项）：

```go
	// ReadBarrier 打开线性一致读屏障（默认 false，零开销直读）。打开后
	// 每次读路径入口走一轮 read-index：一次多数派心跳往返的延迟换掉
	// 「旧 leader 尚未察觉失去领导权时返回过期数据」的窗口。
	ReadBarrier bool
	// ReadBarrierTimeout 单轮 read-index 的时间预算（0 = 默认 3s）。
	ReadBarrierTimeout time.Duration
```

`New`（Manager 构造）里存下 `m.readBarrier = o.ReadBarrier`，并在 `buildGroup` 造出 `gr` 之后覆盖每组的预算：

```go
	if o.ReadBarrierTimeout > 0 {
		gr.barrierTimeout = o.ReadBarrierTimeout
	}
```

Manager 结构体加字段 `readBarrier bool`。

- [ ] **Step 6: 配置项**

`internal/config/config.go` 的 `ClusterConfig` 追加：

```go
	// ReadBarrier 打开线性一致读屏障（read_barrier，默认 false）：打开后
	// 消费读路径每次入口走一轮 raft read-index，用一次多数派心跳往返的
	// 延迟换掉「旧 leader 尚未察觉失去领导权时投递过期数据」的窗口。
	// 关闭时读己之写仍然成立（propose 等 apply），只是别人的写可能读不到。
	ReadBarrier bool `yaml:"read_barrier"`
	// ReadBarrierTimeout 单轮 read-index 的时间预算（read_barrier_timeout，
	// 默认 3s，Go duration 格式）。0 = 未填，按默认。
	ReadBarrierTimeout time.Duration `yaml:"read_barrier_timeout"`
```

`cmd/sq/main.go` 装配 `cluster.Options` 处透传这两项。

- [ ] **Step 7: 加关键节点日志**

- `runBarrierRounds` 每轮结束：成功 `Debug`（带 `waiters` 批次规模）、失败 `Warn`（带 `waiters` + `err`）——**成功路径不静默**
- `Manager.ReadBarrier` 请求了不存在的组：`Warn`（这是装配/路由错误，必须能被 grep 到）
- 屏障开关状态在 Manager 启动日志里带上：`m.lg.Info("集群管理器初始化", ..., "readBarrier", o.ReadBarrier)`——运维排查「为什么读到旧数据」的第一现场

- [ ] **Step 8: 加注释**

- `barrierBatch` / `readBarrier` / `runBarrierRounds` / `batchSize` / `Manager.ReadBarrier` 的 doc 注释（同上）
- `brRunning` 字段上的「为什么不能复用在途轮次」红线注释——这是整个 Part A 最容易被后人"优化"掉的地方，注释必须写清代价
- 两个配置项的 yaml 注释写明默认值与取舍

- [ ] **Step 9: 跑测试**

```bash
go test ./internal/cluster -run 'TestReadBarrier' -v && go test ./internal/cluster ./internal/config && go test -race ./internal/cluster -run TestReadBarrier
```

预期：全 PASS，`-race` 无告警。

- [ ] **Step 10: 提交**

```bash
git add internal/cluster/readbarrier.go internal/cluster/readbarrier_test.go internal/cluster/group.go internal/cluster/manager.go internal/config/config.go cmd/sq/main.go
git commit -m "$(cat <<'EOF'
feat(cluster,config): Manager.ReadBarrier 排队成批合流 + read_barrier 配置开关

合流不能用朴素 singleflight：后到者复用在途那一轮的 readIndex，而那个
index 取自它发起之前，中间被确认的写它看不到——线性一致直接破功。改成
排队成批：一轮在途时到达的等待者一律排进下一批，一批的 read-index 调用
发生在这批所有成员入队之后。在途轮次恒为 1，读 QPS 再高也只有一条流。

每轮以 lifeCtx 为父 ctx 独立计时（barrierTimeout，默认 3s）：某个调用方
放弃不该让整批失败；组退出时在途轮次随之取消。

read_barrier 默认关闭——打开是拿一次多数派心跳往返换「旧 leader 未察觉
失去领导权时返回过期数据」的窗口，取舍交给部署方。

验证：go test ./internal/cluster ./internal/config 全绿 + -race 无告警（macOS）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: 接入消费读路径 + 三节点集成断言

**Files:**
- Modify: `internal/replication/replication.go`（`Router` 接口加 `ReadBarrier`；`StandaloneRouter` 与 `*Cluster` 实现）
- Modify: `internal/core/deliver/deliver.go`（`Receive` 入口挂屏障）
- Modify: `internal/core/deliver/deliver_test.go`（若存在假 Router 需补方法）
- Modify: `internal/cluster/cluster_test.go`（三节点集成断言）

**Interfaces:**
- Consumes：Task 2 的 `Manager.ReadBarrier(ctx, g) error`
- Produces：`replication.Router.ReadBarrier(ctx context.Context, g uint32) error`

- [ ] **Step 1: 写失败的集成测试**

追加到 `internal/cluster/cluster_test.go`：

```go
// TestReadBarrierOnLeaderPassesAndFailsOnFollower 三节点集成：leader 上的
// 读屏障能走通（真的拿到 readIndex 并被 applied 追平），follower 上必须
// 快速失败为 ErrNotLeader——读屏障只在 leader 成立，follower 直读没有
// 线性一致保证，静默放行比报错危险得多。
func TestReadBarrierOnLeaderPassesAndFailsOnFollower(t *testing.T) {
	nodes := startCluster(t, 3, func(o *Options) { o.ReadBarrier = true })
	defer stopCluster(t, nodes)

	const g = uint32(0)
	leader := waitLeader(t, nodes, g)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := leader.ReadBarrier(ctx, g); err != nil {
		t.Fatalf("leader 上的读屏障应放行，得到 %v", err)
	}

	for _, n := range nodes {
		if n == leader {
			continue
		}
		err := n.ReadBarrier(ctx, g)
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("follower 上的读屏障应报 ErrNotLeader，得到 %v", err)
		}
	}
}

// TestReadBarrierSeesRemoteWrite 读屏障的真正价值：别人写、我读。
// 在 leader 上写一条 → 在 leader 上过屏障 → 本地必须读得到。
// （单节点视角下这条恒成立；本用例的意义是把「屏障返回 nil 之后本地读
// 必然可见」钉成回归断言，防止后人把屏障改成空实现。）
func TestReadBarrierSeesRemoteWrite(t *testing.T) {
	nodes := startCluster(t, 3, func(o *Options) { o.ReadBarrier = true })
	defer stopCluster(t, nodes)

	const g = uint32(0)
	leader := waitLeader(t, nodes, g)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := leader.Store().NewBatch()
	key := []byte("read-barrier-probe")
	if err := b.Set(key, []byte("v1")); err != nil {
		t.Fatalf("构造批次失败: %v", err)
	}
	repr := append([]byte(nil), b.Repr()...)
	b.Close()
	if err := leader.Propose(ctx, g, repr); err != nil {
		t.Fatalf("提案失败: %v", err)
	}
	if err := leader.ReadBarrier(ctx, g); err != nil {
		t.Fatalf("读屏障应放行，得到 %v", err)
	}
	v, closer, err := leader.Store().Get(key)
	if err != nil {
		t.Fatalf("屏障放行后本地必须读到刚写入的键，得到 %v", err)
	}
	defer closer.Close()
	if string(v) != "v1" {
		t.Fatalf("读到的值应为 v1，得到 %q", v)
	}
}
```

`startCluster` / `waitLeader` / `stopCluster` 用 `cluster_test.go` 里既有的同名辅助函数；若既有 `startCluster` 不接受 `func(*Options)` 变参，先给它加上变参（改动限于测试文件，其他调用点不受影响）。`leader.Store().Get` 的确切签名以 `internal/store` 为准，若 Get 的返回形态不同就按实际形态调整断言。

- [ ] **Step 2: 跑测试确认失败**

```bash
go test ./internal/cluster -run 'TestReadBarrierOnLeader|TestReadBarrierSeesRemoteWrite' -v
```

预期：编译失败（`o.ReadBarrier` 若 Task 2 已加则通过编译，转为运行失败——`startCluster` 变参未支持）。

- [ ] **Step 3: `Router` 接口加 `ReadBarrier` 并实现两端**

`internal/replication/replication.go`：

```go
// Router 组路由视图。单机实现恒返回 0；集群实现转发 Manager。
type Router interface {
	GroupForQueue(topic string, queueID uint32) uint32
	MetaGroup() uint32
	IsLeader(g uint32) bool
	// ReadBarrier 等 g 组的线性一致读屏障：返回 nil 后本地读一定包含了
	// 本次调用发起之前已被确认的全部写。单机后端恒 nil（无复制、无屏障
	// 可言）；集群后端在读屏障关闭时同样恒 nil（零开销）。
	ReadBarrier(ctx context.Context, g uint32) error
}
```

`StandaloneRouter`：

```go
// ReadBarrier 单机档没有复制，本地读天然线性一致，恒放行。
func (StandaloneRouter) ReadBarrier(context.Context, uint32) error { return nil }
```

`*Cluster`：

```go
// ReadBarrier 转发 Manager.ReadBarrier；读屏障关闭时 Manager 内部即恒 nil。
func (r *Cluster) ReadBarrier(ctx context.Context, g uint32) error {
	return r.m.ReadBarrier(ctx, g)
}
```

- [ ] **Step 4: 接进 `deliver.Receive`**

`internal/core/deliver/deliver.go` 的 `Receive`，在 `EnsureGroup` 之后、长轮询循环之前插入：

```go
	// 读屏障挂在 Receive 入口而不是每次 receiveOnce：一次 Receive 是一次
	// 长轮询批次，屏障成本（一次多数派心跳往返）摊到整批上可以忽略；挂在
	// 内层循环里会让每 100ms 的兜底轮询都付一次往返。
	// 屏障关闭 / 单机档时这里恒 nil，零开销。
	if err := d.rt.ReadBarrier(ctx, d.rt.GroupForQueue(topic, queueID)); err != nil {
		d.logger.Warn("消费读屏障未通过，拒绝投递（避免投出过期数据）",
			"group", group, "topic", topic, "queue", queueID, "err", err)
		return nil, err
	}
```

若 `Deliverer` 的日志字段不叫 `d.logger`，按本文件既有命名改。

- [ ] **Step 5: 补齐测试替身**

搜索实现了 `replication.Router` 的测试替身并补上新方法：

```bash
grep -rn "GroupForQueue" --include='*_test.go' internal/
```

每个替身加：

```go
func (f *fakeRouter) ReadBarrier(context.Context, uint32) error { return nil }
```

- [ ] **Step 6: 加关键节点日志**

- `deliver.Receive` 屏障失败：`Warn`，带 `group`/`topic`/`queue`/`err`（已在 Step 4）
- 屏障成功**不打日志**：Receive 是热路径，成功路径的可观测性由 Task 2 的 `runBarrierRounds` Debug 承担（每轮一条，而不是每次 Receive 一条）——这是热循环规则下「成功路径不静默」的正确落点

- [ ] **Step 7: 加注释**

- `Router.ReadBarrier` 的接口注释写明两个后端的语义
- `deliver.Receive` 里「为什么挂在入口而不是内层循环」的行内注释（已在 Step 4）
- `StandaloneRouter.ReadBarrier` 恒 nil 的原因

- [ ] **Step 8: 跑全量测试**

```bash
go build ./... && go test ./internal/... && go test -race ./internal/cluster ./internal/core/deliver
```

预期：全绿。

- [ ] **Step 9: Linux 双机验证**

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o /tmp/cluster.test ./internal/cluster
for h in 100.90.99.61 47.80.240.57; do scp /tmp/cluster.test root@$h:/tmp/ && ssh root@$h "cd /tmp && ./cluster.test -test.run 'TestReadBarrier' -test.v 2>&1 | tail -5"; done
```

预期：两台均 PASS。

- [ ] **Step 10: 提交**

```bash
git add internal/replication/replication.go internal/core/deliver internal/cluster/cluster_test.go
git commit -m "$(cat <<'EOF'
feat(replication,deliver): 读屏障接入消费读路径——Receive 入口一次，长轮询内层不重复付

Router 接口加 ReadBarrier：单机档恒 nil（无复制无屏障可言），集群档转发
Manager.ReadBarrier（开关关闭时同样恒 nil）。挂在 Receive 入口而不是
receiveOnce：一次 Receive 是一次长轮询批次，屏障成本摊到整批可忽略；挂
内层会让每 100ms 的兜底轮询都付一次多数派往返。

集成断言两条：leader 上屏障放行且放行后本地必然读到刚提案的键；follower
上必须快速失败为 ErrNotLeader——follower 直读没有线性一致保证，静默放行
比报错危险得多。

验证：go test ./internal/... 全绿 + -race（macOS）；TestReadBarrier* 在
联想 4 核与云主机 2 核两台 Linux 上均 PASS。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

# Part B：B8.3 集群场景测试

## 背景（实现者必读）

spec §8.2 说「补上场景测试框架预留的集群节点实现」，但 **`test/scenario/` 目录并不存在**——那份框架计划（`docs/superpowers/plans/2026-08-07-scenario-test.md`）写了没执行。**本批不去补那个框架**：`test/e2e` 已经有可用的三节点进程级 harness（真 broker 进程、真 SDK、真 gRPC），场景测试直接长在它上面，这是最短且证据最强的路径。backlog 里 B8.3 的措辞在 Task 8 收尾时一并订正。

**网络分区怎么做**：不用 iptables/pfctl（要 root、macOS 与 Linux 两套写法、CI 不可复现）。**杀掉 3 之 2 的节点**在 raft 视角下与「剩下那个被隔离到少数派」完全等价：剩余节点失去 quorum，写必须失败。愈合就是把两个节点重新拉起。SIGSTOP/SIGCONT 作为补充手段（进程还在、只是不响应），用于「假死」形态。

---

## Task 4: `procCluster` 进程级场景 harness

**Files:**
- Create: `test/e2e/cluster_proc_test.go`
- Modify: `test/e2e/sdk_cluster_bench_test.go`（`startBenchCluster` 改为委托新 harness）

**Interfaces:**
- Produces：`type procCluster`、`func startProcCluster(t *testing.T, n int, mutate ...func(*config.Config)) *procCluster`，方法 `multi() string`、`endpointOf(i int) string`、`kill(t, i)`、`stopGraceful(t, i)`、`restart(t, i)`、`pause(t, i)`、`resume(t, i)`、`aliveCount() int`、`stopAll(t)`
- Consumes：既有 `pickPorts`、`clusterNodeConfig`、`writeClusterPeers`、`writeNodeConfig`、`brokerHandle`、`waitBrokerReady`、`dumpBrokerLog`、`memPeakSampler`、`brokerBinary`

- [ ] **Step 1: 新建 `test/e2e/cluster_proc_test.go`**

```go
//go:build e2e

// cluster_proc_test.go：进程级三节点集群的场景 harness。
//
// 职责：
//   - 起 N 个真 broker 进程（真配置、真端口、真 raft 传输），统一收尾
//   - 提供故障注入原语：SIGKILL（断电语义）、优雅停（写干净关机标记）、
//     原地重启（复用同一份配置与数据目录）、SIGSTOP/SIGCONT（假死）
//   - 记录每进程内存峰值（全局约束：性能类验证必记）
//
// 边界：
//   - 不做网络层分区（iptables/pfctl 要 root、两平台两套写法、CI 不可
//     复现）：少数派场景一律用「杀掉 3 之 2」等价模拟，raft 视角下与
//     隔离剩余节点完全一致
//   - 不断言业务语义：对账器与场景断言在各自的用例里
//   - 不复用 startBroker：三节点必须全部先起进程再逐个等就绪（raft 选举
//     要 quorum，等第一个就绪会永远卡在「等 meta 组出 leader」）
package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/config"
)

// procCluster 一组进程级 broker 及其重启所需的全部素材。
type procCluster struct {
	handles []*brokerHandle
	dirs    []string
	cfgs    []*config.Config
	// cfgPaths 与 handles 同序：重启时原样复用（同一份配置 + 同一个数据
	// 目录，节点身份不变，这正是「重启自愈」要验证的路径）
	cfgPaths []string
	peaks    *memPeakSampler
}

// startProcCluster 起 n 个 broker 进程并等全部就绪。
//
// 参数：
//   - n: 节点数（场景测试固定用 3）
//   - mutate: 在配置写盘之前逐个应用，供用例覆盖特定配置项
//
// 返回：
//   - 就绪的 *procCluster（收尾已注册到 t.Cleanup）
//
// 注意：必须全部先起进程、再逐个等就绪——raft 选举要 quorum。
func startProcCluster(t *testing.T, n int, mutate ...func(*config.Config)) *procCluster {
	t.Helper()
	grpcPorts := pickPorts(t, n)
	raftPorts := pickPorts(t, n)

	pc := &procCluster{
		handles:  make([]*brokerHandle, n),
		dirs:     make([]string, n),
		cfgs:     make([]*config.Config, n),
		cfgPaths: make([]string, n),
	}
	for i := 0; i < n; i++ {
		pc.dirs[i] = t.TempDir()
		pc.cfgs[i] = clusterNodeConfig(t, pc.dirs[i], uint64(i+1), grpcPorts[i], raftPorts[i], 3)
		for _, fn := range mutate {
			fn(pc.cfgs[i])
		}
	}
	writeClusterPeers(pc.cfgs, grpcPorts, raftPorts)
	for i := 0; i < n; i++ {
		pc.cfgPaths[i] = writeNodeConfig(t, pc.cfgs[i])
		pc.handles[i] = pc.spawn(t, i, fmt.Sprintf("127.0.0.1:%d", grpcPorts[i]))
	}
	for i := 0; i < n; i++ {
		waitBrokerReady(t, pc.handles[i].endpoint, pc.handles[i].waitDone, pc.handles[i].logPath)
		t.Logf("节点 %d 就绪 endpoint=%s pid=%d", i+1, pc.handles[i].endpoint, pc.handles[i].cmd.Process.Pid)
	}
	pc.peaks = newMemPeakSampler(pc.handles)
	t.Cleanup(func() { pc.stopAll(t) })
	return pc
}

// spawn 起单个 broker 进程（不等就绪）。日志按「节点目录/broker.log」
// 追加：重启后同一节点的多段日志留在同一个文件里，排查时不必拼接。
func (pc *procCluster) spawn(t *testing.T, i int, endpoint string) *brokerHandle {
	t.Helper()
	logPath := filepath.Join(pc.dirs[i], "broker.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("打开节点 %d 日志文件失败: %v", i+1, err)
	}
	cmd := exec.Command(brokerBinary, "-config", pc.cfgPaths[i])
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("启动节点 %d 失败: %v", i+1, err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	return &brokerHandle{
		endpoint: endpoint, cfgPath: pc.cfgPaths[i], logPath: logPath,
		logFile: logFile, cmd: cmd, waitDone: waitDone,
	}
}

// multi 返回 SDK 用的「;」分隔多地址（SDK resolver 按 ; 拆分）。
// 已被 kill 的节点仍留在串里：SDK 换节点重试正是要被验证的行为。
func (pc *procCluster) multi() string {
	eps := make([]string, 0, len(pc.handles))
	for _, h := range pc.handles {
		eps = append(eps, h.endpoint)
	}
	return strings.Join(eps, ";")
}

// endpointOf 返回第 i 个节点（0-based）的 gRPC 地址。
func (pc *procCluster) endpointOf(i int) string { return pc.handles[i].endpoint }

// endpoints 返回全部节点地址（0-based 同序）。
func (pc *procCluster) endpoints() []string {
	eps := make([]string, 0, len(pc.handles))
	for _, h := range pc.handles {
		eps = append(eps, h.endpoint)
	}
	return eps
}

// indexOfEndpoint 按地址反查下标；找不到返回 -1。
func (pc *procCluster) indexOfEndpoint(ep string) int {
	for i, h := range pc.handles {
		if h.endpoint == ep {
			return i
		}
	}
	return -1
}

// kill SIGKILL 第 i 个节点（断电语义：不写干净关机标记，重启必须走
// ErrUncleanShutdown → Rejoin 自愈路径）。
func (pc *procCluster) kill(t *testing.T, i int) {
	t.Helper()
	h := pc.handles[i]
	if h.cmd.Process == nil {
		return
	}
	pid := h.cmd.Process.Pid
	if err := h.cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL 节点 %d: %v", i+1, err)
	}
	pc.awaitExit(t, i, "SIGKILL")
	t.Logf("节点 %d 已 SIGKILL（pid=%d）", i+1, pid)
}

// stopGraceful SIGTERM 第 i 个节点并等它自行收尾（写干净关机标记，
// 重启走干净恢复路径而不是 Rejoin）。
func (pc *procCluster) stopGraceful(t *testing.T, i int) {
	t.Helper()
	h := pc.handles[i]
	if h.cmd.Process == nil {
		return
	}
	pid := h.cmd.Process.Pid
	if err := h.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM 节点 %d: %v", i+1, err)
	}
	pc.awaitExit(t, i, "SIGTERM")
	t.Logf("节点 %d 已优雅停机（pid=%d）", i+1, pid)
}

// awaitExit 等进程退出并回收日志文件句柄；超时即失败（进程没死透会让
// 后续 restart 撞端口，报错必须落在这里而不是下游）。
func (pc *procCluster) awaitExit(t *testing.T, i int, how string) {
	t.Helper()
	h := pc.handles[i]
	select {
	case err := <-h.waitDone:
		t.Logf("节点 %d 因 %s 退出：%v", i+1, how, err)
	case <-time.After(20 * time.Second):
		t.Fatalf("节点 %d 在 %s 后 20s 内未退出", i+1, how)
	}
	h.logFile.Close()
	h.cmd.Process = nil
}

// restart 原地重启第 i 个节点（同配置、同数据目录、同节点 id）并等就绪。
//
// 注意：ready 判定沿用 waitBrokerReady（gRPC 可用即算就绪）；集群档的
// 「已追上多数派」由用例自己的对账断言承担，harness 不越俎代庖。
func (pc *procCluster) restart(t *testing.T, i int) {
	t.Helper()
	if pc.handles[i].cmd.Process != nil {
		t.Fatalf("节点 %d 仍在运行，restart 前必须先 kill/stopGraceful", i+1)
	}
	ep := pc.handles[i].endpoint
	pc.handles[i] = pc.spawn(t, i, ep)
	waitBrokerReady(t, ep, pc.handles[i].waitDone, pc.handles[i].logPath)
	t.Logf("节点 %d 已重启就绪 pid=%d", i+1, pc.handles[i].cmd.Process.Pid)
}

// pause SIGSTOP 第 i 个节点（假死：进程还在、端口还占着，但不再响应
// 任何 raft 消息与客户端请求）。用于「不是崩溃而是卡住」的形态。
func (pc *procCluster) pause(t *testing.T, i int) {
	t.Helper()
	if err := pc.handles[i].cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP 节点 %d: %v", i+1, err)
	}
	t.Logf("节点 %d 已 SIGSTOP（假死）", i+1)
}

// resume SIGCONT 第 i 个节点。
func (pc *procCluster) resume(t *testing.T, i int) {
	t.Helper()
	if err := pc.handles[i].cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT 节点 %d: %v", i+1, err)
	}
	t.Logf("节点 %d 已 SIGCONT（恢复）", i+1)
}

// aliveCount 返回仍在运行的节点数。
func (pc *procCluster) aliveCount() int {
	n := 0
	for _, h := range pc.handles {
		if h.cmd.Process != nil {
			n++
		}
	}
	return n
}

// stopAll 采样内存峰值 → 停掉全部存活节点 → 用例失败时打印各节点日志。
func (pc *procCluster) stopAll(t *testing.T) {
	if pc.peaks != nil {
		pc.peaks.stopAndReport(t)
		pc.peaks = nil
	}
	for i, h := range pc.handles {
		if h.cmd.Process != nil {
			h.stop(t)
			h.cmd.Process = nil
		}
		if t.Failed() {
			dumpBrokerLog(t, filepath.Join(pc.dirs[i], "broker.log"))
		}
	}
}
```

- [ ] **Step 2: `startBenchCluster` 改为委托**

`test/e2e/sdk_cluster_bench_test.go` 里把 `startBenchCluster` 的函数体整体替换为：

```go
func startBenchCluster(t *testing.T, ack string, queues int) (string, func()) {
	t.Helper()
	// 日志档压到 info：debug 下三个节点写日志本身就是可观开销，会污染吞吐
	pc := startProcCluster(t, 3, func(c *config.Config) {
		c.Cluster.Ack = ack
		c.DefaultQueueNums = uint32(queues)
		c.LogLevel = "info"
	})
	return pc.endpointOf(0), func() { pc.stopAll(t) }
}
```

删掉该文件里因此不再使用的 import（`os`/`exec`/`filepath` 等，按编译器提示清理）。注意 `startProcCluster` 已把 `stopAll` 注册到 `t.Cleanup`，返回的 stop 再调一次是幂等的（`peaks==nil` 与 `Process==nil` 双重短路）。

- [ ] **Step 3: 加关键节点日志**

harness 用 `t.Logf`（测试内的正确日志载体，会随 `-v` 与失败输出一起呈现）：进程起（pid + endpoint）、就绪、kill / 优雅停 / 重启 / 暂停 / 恢复各一条，退出时带退出错误。**失败路径**（起不来、20s 未退出、重启前仍在运行）一律 `t.Fatalf` 带节点号与原因。

- [ ] **Step 4: 加注释**

文件头注释（职责 + 边界，含「不做网络层分区」的理由，已在 Step 1）；每个导出/共享方法的 doc 注释；`multi()` 里「已 kill 节点仍留在串里」的原因、`spawn` 里日志用 `O_APPEND` 的原因、`restart` 里 ready 判定边界——三处「为什么」注释缺一不可。

- [ ] **Step 5: 编译并跑既有 bench 用例确认没打破**

```bash
cd test/e2e && go vet -tags e2e ./... && SQ_BENCH=1 SQ_BENCH_CONC=16 SQ_BENCH_MSGS=2000 go test -tags e2e -run TestClusterWriteThroughput -timeout 20m -v ./...
```

预期：编译通过，吞吐用例仍 PASS。

- [ ] **Step 6: 提交**

```bash
git add test/e2e/cluster_proc_test.go test/e2e/sdk_cluster_bench_test.go
git commit -m "$(cat <<'EOF'
test(e2e): procCluster 进程级场景 harness——kill/优雅停/原地重启/假死四种故障注入

场景测试长在既有 e2e 三节点进程级 harness 上，而不是去补从未执行的
test/scenario 框架计划：真 broker 进程 + 真 SDK + 真 gRPC，证据最强、
路径最短。

网络分区一律用「杀掉 3 之 2」等价模拟：iptables/pfctl 要 root、两平台
两套写法、CI 不可复现，而 raft 视角下「剩余节点失去 quorum」与「剩余
节点被隔离到少数派」完全一致。SIGSTOP/SIGCONT 作为「假死而非崩溃」的
补充形态。

日志按节点目录 O_APPEND：重启后同一节点的多段日志留在同一文件，排查
不必拼接。startBenchCluster 改为委托本 harness，去掉一份重复的启动序。

验证：SQ_BENCH=1 的吞吐用例仍 PASS（macOS）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: 场景一 —— kill -9 leader + 确认集对账器

**Files:**
- Create: `test/e2e/cluster_scenario_test.go`

**Interfaces:**
- Produces：`type confirmedSet`、`func newConfirmedSet() *confirmedSet`、`(*confirmedSet).confirm(id string)`、`(*confirmedSet).size() int`、`(*confirmedSet).assertAllConsumed(t *testing.T, got map[string]bool)`、`func produceUntil(t *testing.T, p rmq.Producer, topic string, cs *confirmedSet, stop <-chan struct{}) (sent, failed int)`、`func TestScenarioKillLeaderHard(t *testing.T)`
- Consumes：Task 4 的 `procCluster`；既有 `ensureTopic`、`newClusterProducer`、`newClusterConsumer`、`recvAllAck`、`queryRoute`、`routeEndpointCounts`、`leaderOfMostQueues`、`waitRouteSpread`、`clusterConsumerAwaitShort`

- [ ] **Step 1: 写场景一用例（含对账器）**

新建 `test/e2e/cluster_scenario_test.go`：

```go
//go:build e2e

// cluster_scenario_test.go：B8.3 四类集群场景测试。
//
// 职责：
//   - kill -9 leader、少数派不可写与愈合、滚动重启、断电模拟四类场景
//   - 统一的不变量对账：确认集（Send 成功即登记）必须被全量消费到
//
// 边界：
//   - 不测吞吐（那是 sdk_cluster_bench_test.go）
//   - 不测协议细节（那是 sdk_cluster_test.go）
//   - 允许重复投递、不允许丢失：at-least-once 是本系统的投递语义，
//     对账器只断言「确认集 ⊆ 消费集」，从不断言两者相等
package e2e

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
)

// confirmedSet 确认集对账器：producer 每次 Send 成功即登记 msgID，
// 收尾时断言全部 msgID 都被消费到。
//
// 为什么只断言「确认集 ⊆ 消费集」：投递语义是 at-least-once，故障切换
// 期间的重复是设计允许的；而**已确认的消息丢失**是不可接受的红线。
// Send 失败的消息不进确认集——它们的去留本就未定，纳入对账等于把
// 「不确定」当成「必须存在」。
type confirmedSet struct {
	mu  sync.Mutex
	ids map[string]bool
}

func newConfirmedSet() *confirmedSet { return &confirmedSet{ids: map[string]bool{}} }

// confirm 登记一条已被 broker 确认的消息 id。
func (c *confirmedSet) confirm(id string) {
	c.mu.Lock()
	c.ids[id] = true
	c.mu.Unlock()
}

// size 返回确认集大小。
func (c *confirmedSet) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ids)
}

// assertAllConsumed 断言确认集全部出现在消费集里；缺失即失败，并打印
// 前若干条缺失 id 供排查。
func (c *confirmedSet) assertAllConsumed(t *testing.T, got map[string]bool) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	missing := make([]string, 0, 8)
	for id := range c.ids {
		if !got[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		show := missing
		if len(show) > 8 {
			show = show[:8]
		}
		t.Fatalf("确认集有 %d/%d 条未被消费到（已确认消息丢失是红线）：%v",
			len(missing), len(c.ids), show)
	}
	t.Logf("对账通过：确认 %d 条，全部被消费到（消费集 %d 条，重复允许）",
		len(c.ids), len(got))
}

// produceUntil 持续发送直到 stop 关闭；每条成功即登记进确认集。
//
// 返回：
//   - sent: 成功条数（= 进入确认集的条数）
//   - failed: 失败条数（故障窗口内的失败是预期的，不进确认集）
//
// 注意：单条 Send 的超时压到 3s——故障窗口内 SDK 会换节点重试，默认
// 超时会让整个场景用例卡在一条消息上。
func produceUntil(t *testing.T, p rmq.Producer, topic string, cs *confirmedSet, stop <-chan struct{}) (sent, failed int) {
	t.Helper()
	for i := 0; ; i++ {
		select {
		case <-stop:
			return sent, failed
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		recs, err := p.Send(ctx, &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("scenario #%d", i))})
		cancel()
		if err != nil || len(recs) == 0 {
			failed++
			continue
		}
		cs.confirm(recs[0].MessageID)
		sent++
	}
}

// TestScenarioKillLeaderHard 场景一：持续发送期间 kill -9 掉承载队列
// 最多的那个节点（数据组 leader），故障期间允许 Send 失败，恢复后重启
// 该节点，最终对账——确认集必须被全量消费到。
func TestScenarioKillLeaderHard(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-kill-leader", "scn-kill-leader-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()

	// 阶段①：健康期发一段，确保故障前已有确认集
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var sent, failed int
	wg.Add(1)
	go func() { defer wg.Done(); sent, failed = produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(3 * time.Second)
	if cs.size() == 0 {
		close(stop)
		wg.Wait()
		t.Fatal("健康期 3s 内一条都没发成功，集群未就绪")
	}
	before := cs.size()

	// 阶段②：kill -9 承载队列最多的节点（数据组 leader）
	victimEP := leaderOfMostQueues(routeEndpointCounts(queryRoute(t, pc.endpointOf(0), topic)))
	victim := pc.indexOfEndpoint(victimEP)
	if victim < 0 {
		close(stop)
		wg.Wait()
		t.Fatalf("路由里的 leader 地址 %s 不在集群节点列表中", victimEP)
	}
	t.Logf("kill -9 节点 %d（%s），故障前确认集 %d 条", victim+1, victimEP, before)
	pc.kill(t, victim)

	// 阶段③：故障期继续发 15s——重新选举 + SDK 路由刷新（30s 缓存）需要
	// 时间，这段窗口内失败是预期的，只要求「恢复后能继续发成功」
	time.Sleep(15 * time.Second)
	if cs.size() <= before {
		close(stop)
		wg.Wait()
		t.Fatalf("kill leader 后 15s 内没有任何新消息发送成功（确认集停在 %d 条），"+
			"failover 未收敛", before)
	}
	t.Logf("故障期后确认集 %d 条（较故障前 +%d）", cs.size(), cs.size()-before)

	// 阶段④：重启被 kill 的节点（走 ErrUncleanShutdown → Rejoin 自愈）
	pc.restart(t, victim)
	time.Sleep(10 * time.Second)
	close(stop)
	wg.Wait()
	t.Logf("发送收尾：成功 %d 失败 %d（故障窗口内失败是预期的）", sent, failed)

	// 阶段⑤：对账——确认集必须被全量消费到
	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "kill-leader 对账")
	cs.assertAllConsumed(t, got)
}
```

若 `recvAllAck` 在收不满 `want` 条时会自行 `t.Fatalf`，需要把它的失败语义改成「返回已收集合、由调用方断言」——**改法**：给它加一个 `strict bool` 形参（既有调用点传 `true` 保持原行为，场景测试传 `false`），失败信息交给 `assertAllConsumed` 输出更有针对性的缺失清单。

- [ ] **Step 2: 跑用例确认失败/通过**

```bash
cd test/e2e && go test -tags e2e -run TestScenarioKillLeaderHard -timeout 20m -v ./...
```

先跑一次看它是否真的能跑起来（这是集成用例，第一次运行同时也是"确认它测的是真东西"的时刻）。若 15s 窗口内 failover 未收敛，先查日志确认是 SDK 路由缓存（30s）还是选举本身，**不要直接放大超时**——按 `systematic-debugging` 找根因。

- [ ] **Step 3: 加关键节点日志**

用例内 `t.Logf` 覆盖：健康期确认集规模、被 kill 的节点号与地址、故障期确认集增量、重启完成、发送成功/失败总数、对账结果（确认数 / 消费数）。失败分支的 `t.Fatalf` 必须带**数字**（"确认集停在 %d 条"），而不是"failover 失败"这种无法定位的措辞。

- [ ] **Step 4: 加注释**

文件头（职责 + 边界，含「允许重复不允许丢失」，已在 Step 1）；`confirmedSet` 上「为什么只断言子集关系」与「为什么 Send 失败的不进确认集」；`produceUntil` 上「为什么单条超时压到 3s」；每个阶段的行内注释说明它在验证什么。

- [ ] **Step 5: 提交**

```bash
git add test/e2e/cluster_scenario_test.go
git commit -m "$(cat <<'EOF'
test(e2e): 场景一 kill -9 leader + 确认集对账器

对账口径钉死为「确认集 ⊆ 消费集」：投递语义是 at-least-once，故障切换
期间的重复是设计允许的，而已确认消息丢失是红线。Send 失败的不进确认集
——它们的去留本就未定，纳入对账等于把「不确定」当成「必须存在」。

五阶段：健康期建确认集 → kill -9 承载队列最多的节点 → 故障期断言确认集
仍在增长（failover 真的收敛了，而不是全程失败）→ 原地重启走 Rejoin
自愈 → 全量消费对账。

失败信息一律带数字（"确认集停在 %d 条"），不写"failover 失败"这种无法
定位的措辞。

验证：TestScenarioKillLeaderHard PASS（macOS）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: 场景二 —— 少数派不可写 + 愈合追齐

**Files:**
- Modify: `test/e2e/cluster_scenario_test.go`

**Interfaces:**
- Produces：`func TestScenarioMinorityCannotWrite(t *testing.T)`
- Consumes：Task 4 的 `procCluster`、Task 5 的 `confirmedSet` / `produceUntil`

- [ ] **Step 1: 写用例**

追加到 `test/e2e/cluster_scenario_test.go`：

```go
// TestScenarioMinorityCannotWrite 场景二：少数派不可写 + 愈合后追齐。
//
// 分区用「杀掉 3 之 2」等价模拟：剩余节点失去 quorum，raft 视角下与
// 「剩余节点被隔离到少数派」完全一致（见 cluster_proc_test.go 文件头）。
//
// 红线：少数派期间**绝不允许出现 Send 成功**——多数派确认是写入语义的
// 根，少数派上的假成功意味着数据可能在愈合后被截断，比写失败危险得多。
func TestScenarioMinorityCannotWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-minority", "scn-minority-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	// 阶段①：健康期建确认集（只连节点 0——它是本用例的「幸存者」，
	// 后续要单独观察它在少数派下的行为）
	survivor := 0
	producer := newClusterProducer(t, pc.endpointOf(survivor), topic)
	cs := newConfirmedSet()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()
	healthy := cs.size()
	if healthy == 0 {
		t.Fatal("健康期一条都没发成功，集群未就绪")
	}
	t.Logf("健康期确认 %d 条", healthy)

	// 阶段②：杀掉另外两个节点 → 幸存者进入少数派
	for i := range pc.handles {
		if i != survivor {
			pc.kill(t, i)
		}
	}
	if pc.aliveCount() != 1 {
		t.Fatalf("应只剩 1 个存活节点，实为 %d", pc.aliveCount())
	}

	// 阶段③：少数派期间发送——一条都不许成功
	minorityCS := newConfirmedSet()
	mstop := make(chan struct{})
	wg.Add(1)
	var msent, mfailed int
	go func() { defer wg.Done(); msent, mfailed = produceUntil(t, producer, topic, minorityCS, mstop) }()
	time.Sleep(20 * time.Second)
	close(mstop)
	wg.Wait()
	if msent != 0 {
		t.Fatalf("少数派期间有 %d 条 Send 成功（红线：多数派确认是写入语义的根，"+
			"少数派假成功意味着数据可能在愈合后被截断）", msent)
	}
	t.Logf("少数派期间 %d 次发送全部失败（符合预期）", mfailed)
	if mfailed == 0 {
		t.Fatal("少数派期间一次发送都没尝试，用例没测到东西")
	}

	// 阶段④：愈合——把两个节点拉起来
	for i := range pc.handles {
		if i != survivor {
			pc.restart(t, i)
		}
	}

	// 阶段⑤：愈合后写必须恢复
	hstop := make(chan struct{})
	healCS := newConfirmedSet()
	wg.Add(1)
	go func() { defer wg.Done(); produceUntil(t, producer, topic, healCS, hstop) }()
	time.Sleep(20 * time.Second)
	close(hstop)
	wg.Wait()
	if healCS.size() == 0 {
		t.Fatal("愈合后 20s 内仍无一条发送成功，quorum 未恢复")
	}
	t.Logf("愈合后确认 %d 条", healCS.size())

	// 阶段⑥：对账——健康期 + 愈合期的确认集必须全部消费到
	for id := range healCS.ids {
		cs.confirm(id)
	}
	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "minority 对账")
	cs.assertAllConsumed(t, got)
}
```

`healCS.ids` 是同包内的未导出字段，直接读没有并发问题（此处所有 producer goroutine 已 `wg.Wait()` 收敛）；若嫌破封装，给 `confirmedSet` 加一个 `mergeInto(dst *confirmedSet)` 方法并在这里调用。

- [ ] **Step 2: 跑用例**

```bash
cd test/e2e && go test -tags e2e -run TestScenarioMinorityCannotWrite -timeout 20m -v ./...
```

预期：PASS。若阶段③出现 `msent != 0`，**这是真 bug 不是测试问题**——停下来按 `systematic-debugging` 走：先看 broker 日志确认那条 Send 走的是哪个组、该组当时的 leader 与 quorum 状态。

- [ ] **Step 3: 加关键节点日志**

`t.Logf`：健康期确认数、进入少数派后的存活数、少数派期间失败次数、愈合后确认数、对账结果。三处失败分支（少数派有成功 / 少数派一次都没尝试 / 愈合后写不恢复）的 `t.Fatalf` 各自带上判定依据的数字。

- [ ] **Step 4: 加注释**

用例 doc 注释写明「杀 2 之等价于分区」的理由与「少数派假成功比写失败危险」的红线（已在 Step 1）；`mfailed == 0` 那条断言的存在理由（防止用例因 producer goroutine 没跑起来而"空过"）必须有行内注释。

- [ ] **Step 5: 提交**

```bash
git add test/e2e/cluster_scenario_test.go
git commit -m "$(cat <<'EOF'
test(e2e): 场景二 少数派不可写 + 愈合追齐

红线断言：少数派期间绝不允许出现 Send 成功。多数派确认是写入语义的根，
少数派上的假成功意味着数据可能在愈合后被截断——那比写失败危险得多。

另加一条防空过断言：少数派期间必须真的尝试过发送（mfailed>0），否则
producer goroutine 没跑起来的用例会"全绿"却什么都没测。

分区用杀 3 之 2 等价模拟，不依赖 iptables/root，macOS 与两台 Linux 同款。

验证：TestScenarioMinorityCannotWrite PASS（macOS）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: 场景三 —— 滚动重启

**Files:**
- Modify: `test/e2e/cluster_scenario_test.go`

**Interfaces:**
- Produces：`func TestScenarioRollingRestart(t *testing.T)`
- Consumes：Task 4 的 `procCluster`（`stopGraceful` / `restart`）、Task 5 的对账器

- [ ] **Step 1: 写用例**

追加到 `test/e2e/cluster_scenario_test.go`：

```go
// TestScenarioRollingRestart 场景三：持续发送期间逐个优雅重启全部节点，
// 任意时刻只停一个（quorum 始终保持）。
//
// 与场景一的区别：优雅停机会写干净关机标记，重启走的是「干净恢复」
// 路径而不是 Rejoin 自愈——这是运维日常（升级、改配置）真正走的那条
// 路径，必须单独有证据，不能靠 kill -9 的用例代表它。
//
// 红线：全程 quorum 未失，因此**确认集必须持续增长**——每一轮重启后
// 都要比上一轮多。任何一轮不增长都说明滚动重启期间存在写停摆窗口。
func TestScenarioRollingRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-rolling", "scn-rolling-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var sent, failed int
	wg.Add(1)
	go func() { defer wg.Done(); sent, failed = produceUntil(t, producer, topic, cs, stop) }()

	time.Sleep(5 * time.Second)
	prev := cs.size()
	if prev == 0 {
		close(stop)
		wg.Wait()
		t.Fatal("健康期一条都没发成功，集群未就绪")
	}

	for i := range pc.handles {
		t.Logf("滚动重启第 %d 轮：优雅停节点 %d（确认集 %d 条）", i+1, i+1, prev)
		pc.stopGraceful(t, i)
		// 停机窗口留 8s：这段时间 quorum 仍在（3 之 2），写必须继续成功
		time.Sleep(8 * time.Second)
		pc.restart(t, i)
		// 重启后留 12s 让它追齐并让 SDK 路由刷新
		time.Sleep(12 * time.Second)
		now := cs.size()
		if now <= prev {
			close(stop)
			wg.Wait()
			t.Fatalf("滚动重启第 %d 轮期间确认集未增长（%d → %d）："+
				"quorum 全程未失，写不该有停摆窗口", i+1, prev, now)
		}
		t.Logf("第 %d 轮完成，确认集 %d → %d", i+1, prev, now)
		prev = now
	}

	close(stop)
	wg.Wait()
	t.Logf("滚动重启收尾：成功 %d 失败 %d", sent, failed)

	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "rolling-restart 对账")
	cs.assertAllConsumed(t, got)
}
```

- [ ] **Step 2: 跑用例**

```bash
cd test/e2e && go test -tags e2e -run TestScenarioRollingRestart -timeout 25m -v ./...
```

预期：PASS。若某一轮"确认集未增长"，先看该轮日志确认是 leader 恰在被停节点上导致的 failover 窗口（合理，但说明 8s+12s 的窗口不足以覆盖 SDK 30s 路由缓存）还是真的写停摆——**这两者的处置完全不同**，前者是调整窗口，后者是产品缺陷。

- [ ] **Step 3: 加关键节点日志**

每轮 `t.Logf` 带轮次号、节点号、确认集前后值；失败分支带 `%d → %d` 的前后对比，这是判断"停摆多久"的唯一线索。

- [ ] **Step 4: 加注释**

用例 doc 注释写清「与场景一的区别（干净关机标记 vs Rejoin 自愈）」与红线「quorum 全程未失 ⇒ 确认集必须持续增长」（已在 Step 1）；8s / 12s 两个窗口各自在等什么，必须有行内注释。

- [ ] **Step 5: 提交**

```bash
git add test/e2e/cluster_scenario_test.go
git commit -m "$(cat <<'EOF'
test(e2e): 场景三 滚动重启——逐个优雅停重启，确认集必须每轮增长

与场景一的区别不是"温和一点的 kill"：优雅停机写干净关机标记，重启走
干净恢复路径而不是 Rejoin 自愈。那是运维日常（升级、改配置）真正走的
路径，必须单独有证据。

红线断言：任意时刻只停一个，quorum 全程未失，因此确认集必须每轮都比
上一轮多。任何一轮不增长都说明滚动重启期间存在写停摆窗口——失败信息
带 %d → %d 前后对比，这是判断停摆多久的唯一线索。

验证：TestScenarioRollingRestart PASS（macOS）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: 场景四 —— 断电模拟 + 毒消息→DLQ + producer 提交后立退

**Files:**
- Modify: `test/e2e/cluster_scenario_test.go`
- Modify: `docs/superpowers/backlog.md`（B8.3 行订正与收尾，提交到 `main`）

**Interfaces:**
- Produces：`func TestScenarioPowerLoss(t *testing.T)`、`func TestScenarioPoisonMessageToDLQ(t *testing.T)`、`func TestScenarioProducerExitsRightAfterCommit(t *testing.T)`
- Consumes：Task 4 的 `procCluster`、Task 5 的对账器；既有 DLQ 用例的形态参考 `TestClusterDLQ`

- [ ] **Step 1: 写断电模拟用例**

```go
// TestScenarioPowerLoss 场景四：断电模拟——三节点同时 SIGKILL，全部重启，
// 断言确认集一条不少。
//
// 为什么是「同时 kill 全部」而不是逐个：逐个 kill 剩余节点还能靠多数派
// 把日志补回来，考不到「所有节点的未 flush 尾巴同时消失」这个最坏情况。
// quorum-mem 档下未确认的尾巴允许丢，**已确认的必须在**——这正是异步刷盘
// 档位的取舍边界（spec §2.2），也是本用例唯一要钉死的东西。
func TestScenarioPowerLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-power-loss", "scn-power-loss-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(8 * time.Second)

	// 断电：写入正进行中的一刻同时 SIGKILL 全部节点。先 kill 再 stop
	// producer——顺序反了就变成「停机后断电」，考不到写入中途断电
	for i := range pc.handles {
		pc.kill(t, i)
	}
	close(stop)
	wg.Wait()
	confirmed := cs.size()
	if confirmed == 0 {
		t.Fatal("断电前一条都没确认，用例没测到东西")
	}
	t.Logf("断电时确认集 %d 条", confirmed)

	// 上电：全部重启（不干净关机 → ErrUncleanShutdown → Rejoin 自愈）
	for i := range pc.handles {
		pc.restart(t, i)
	}
	waitRouteSpread(t, pc.endpoints(), topic, 120*time.Second)

	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, confirmed, 240*time.Second, "power-loss 对账")
	cs.assertAllConsumed(t, got)
}
```

- [ ] **Step 2: 写毒消息 → DLQ 用例**

```go
// TestScenarioPoisonMessageToDLQ 毒消息：消费者一律不 ack，达到 max_attempts
// 后消息必须落进 DLQ，且**不得阻塞同队列后续消息**——毒消息把队列钉死是
// 消息系统最典型的级联故障。
//
// 与既有 TestClusterDLQ 的区别：本用例在毒消息重投期间**并发发正常消息**，
// 断言正常消息照常被消费到。TestClusterDLQ 只验证毒消息本身进 DLQ。
func TestScenarioPoisonMessageToDLQ(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	// max_attempts 压到 2：默认 16 次重投会让用例跑到分钟级
	pc := startProcCluster(t, 3, func(c *config.Config) { c.DefaultMaxAttempts = 2 })
	const topic, group = "scn-poison", "scn-poison-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	poison, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte("poison")})
	if err != nil || len(poison) == 0 {
		t.Fatalf("发毒消息失败: %v", err)
	}
	poisonID := poison[0].MessageID
	t.Logf("毒消息 id=%s", poisonID)

	// 正常消息：在毒消息重投窗口内发出
	normal := newConfirmedSet()
	for i := 0; i < 20; i++ {
		recs, err := producer.Send(context.Background(), &rmq.Message{
			Topic: topic, Body: []byte(fmt.Sprintf("normal #%d", i))})
		if err != nil || len(recs) == 0 {
			t.Fatalf("发正常消息 #%d 失败: %v", i, err)
		}
		normal.confirm(recs[0].MessageID)
	}

	// 消费者一律不 ack：毒消息与正常消息都会被重投，但正常消息必须**收到**
	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	seen := map[string]bool{}
	deadline := time.Now().Add(120 * time.Second)
	poisonDeliveries := 0
	for time.Now().Before(deadline) && len(seen) < normal.size()+1 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msgs, err := consumer.Receive(ctx, 16, 20*time.Second)
		cancel()
		if err != nil {
			continue
		}
		for _, m := range msgs {
			seen[m.GetMessageId()] = true
			if m.GetMessageId() == poisonID {
				poisonDeliveries++
			}
			// 一律不 ack：让 invisible 到期后重投
		}
	}
	if poisonDeliveries < 1 {
		t.Fatal("毒消息一次都没投递过，用例没测到东西")
	}
	// 红线：毒消息不得把同队列后续消息钉死
	normal.assertAllConsumed(t, seen)
	t.Logf("毒消息投递 %d 次，%d 条正常消息全部收到（毒消息未钉死队列）",
		poisonDeliveries, normal.size())
}
```

`consumer.Receive` 与 `m.GetMessageId()` 的确切签名以既有 `recvAllAck` 里的用法为准，照抄那里的形态。

- [ ] **Step 3: 写「producer 提交后立退」用例**

```go
// TestScenarioProducerExitsRightAfterCommit producer 在 Send 返回成功后
// 立刻 GracefulStop 并退出——断言该消息仍能被消费到。
//
// 为什么单独测：Send 返回成功意味着 broker 侧已 quorum 确认并 apply，
// 消息的存活不该依赖 producer 连接还在。如果实现里有任何「连接关闭时
// 回滚未完成写入」的逻辑（事务半消息就有这种形态），这条会立刻抓到。
func TestScenarioProducerExitsRightAfterCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-producer-exit", "scn-producer-exit-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	cs := newConfirmedSet()
	const n = 30
	for i := 0; i < n; i++ {
		// 每条消息用一个全新的 producer，发完立刻关：把「提交后立退」
		// 的窗口压到最短，而不是发完一批再统一关
		p := newClusterProducer(t, pc.multi(), topic)
		recs, err := p.Send(context.Background(), &rmq.Message{
			Topic: topic, Body: []byte(fmt.Sprintf("exit-now #%d", i))})
		if err != nil || len(recs) == 0 {
			p.GracefulStop()
			t.Fatalf("第 %d 条发送失败: %v", i, err)
		}
		cs.confirm(recs[0].MessageID)
		if err := p.GracefulStop(); err != nil {
			t.Logf("第 %d 个 producer 收尾报错（不影响断言）: %v", i, err)
		}
	}
	t.Logf("已发送并立即退出 %d 次，确认集 %d 条", n, cs.size())

	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "producer-exit 对账")
	cs.assertAllConsumed(t, got)
}
```

`newClusterProducer` 若已把 `GracefulStop` 注册到 `t.Cleanup`，这里的显式调用会重复——检查其实现，重复时改用一个不注册 cleanup 的变体 `newClusterProducerNoCleanup`。

- [ ] **Step 4: 跑三个用例**

```bash
cd test/e2e && go test -tags e2e -run 'TestScenarioPowerLoss|TestScenarioPoisonMessageToDLQ|TestScenarioProducerExitsRightAfterCommit' -timeout 40m -v ./...
```

预期：全 PASS。

- [ ] **Step 5: 全量场景测试 + Linux 双机验证**

```bash
# 本机 macOS 全量
cd test/e2e && go test -tags e2e -run TestScenario -timeout 60m -v ./...

# 交叉编译 broker 与 e2e 测试二进制，推两台 Linux
cd /Users/xushixin/workspace/sq
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/sq-linux ./cmd/sq
cd test/e2e && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -tags e2e -c -o /tmp/e2e.test .
for h in 100.90.99.61 47.80.240.57; do
  scp /tmp/sq-linux /tmp/e2e.test root@$h:/tmp/
  ssh root@$h "cd /tmp && SQ_E2E_BROKER=/tmp/sq-linux ./e2e.test -test.run TestScenario -test.timeout 60m -test.v 2>&1 | tail -30"
done
```

预期：两台 Linux 均全 PASS。云主机只有 2 核 1.6G，若因资源不足超时，把该机的结论限定为「已跑通的子集」并在提交说明里写明**哪些没跑**——不得含糊带过。

- [ ] **Step 6: 加关键节点日志**

三个用例各自补齐 `t.Logf`：断电时确认数 / 上电后对账结果；毒消息 id、投递次数、正常消息全收；producer-exit 的发送次数与确认数。**"用例没测到东西"的守卫断言**（`confirmed == 0`、`poisonDeliveries < 1`）每个用例都要有——没有它们，一个空转的场景用例会永远绿。

- [ ] **Step 7: 加注释**

三个用例的 doc 注释各自写清「为什么这样测」而不是「测了什么」：断电为什么同时 kill 全部、毒消息用例与既有 `TestClusterDLQ` 的区别、producer 立退为什么值得单独一条。

- [ ] **Step 8: 订正并收尾 backlog 的 B8.3**

`docs/superpowers/backlog.md` 的 B8.3 行：

1. 状态 `📋 specced` → `✅ done`；
2. 备注里「补场景测试框架预留的集群节点实现」订正为：**`test/scenario/` 框架计划（2026-08-07）从未执行，本批不去补它；场景测试长在既有 `test/e2e` 三节点进程级 harness 上（真 broker 进程 + 真 SDK + 真 gRPC）**；
3. 证据列写上四类场景 + 毒消息 + producer 立退共 6 条用例，以及三平台（macOS / 联想 4 核 Linux / 云主机 2 核 Linux）的运行结论与内存峰值。

- [ ] **Step 9: 提交（代码到分支，backlog 到 main）**

```bash
git add test/e2e/cluster_scenario_test.go
git commit -m "$(cat <<'EOF'
test(e2e): 场景四 断电模拟 + 毒消息→DLQ + producer 提交后立退

断电用「三节点同时 SIGKILL」而不是逐个：逐个 kill 时剩余节点还能靠多数派
把日志补回来，考不到「所有节点的未 flush 尾巴同时消失」这个最坏情况。
quorum-mem 档下未确认的尾巴允许丢、已确认的必须在——这是异步刷盘档位的
取舍边界，也是本用例唯一要钉死的东西。

毒消息用例与既有 TestClusterDLQ 的区别：本用例在重投窗口内并发发正常
消息并断言它们照常被消费到——毒消息把队列钉死是消息系统最典型的级联
故障，只验证"毒消息进了 DLQ"考不到它。

producer 立退单独一条：Send 返回成功意味着 broker 侧已 quorum 确认并
apply，消息存活不该依赖 producer 连接还在。每条消息用全新 producer 发完
立刻关，把窗口压到最短。

三个用例各带一条「用例没测到东西」的守卫断言——没有它们，空转的场景
用例会永远绿。

验证：TestScenario* 六条全 PASS（macOS + 联想 4 核 Linux + 云主机 2 核 Linux）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"

git stash push docs/superpowers/backlog.md 2>/dev/null; git checkout main && git stash pop
git add docs/superpowers/backlog.md
git commit -m "$(cat <<'EOF'
docs(backlog): B8.3 集群场景测试收尾，订正「补 test/scenario 框架」的措辞

原措辞说「补场景测试框架预留的集群节点实现」，但 test/scenario 目录并不
存在——那份框架计划（2026-08-07）写了没执行。本批不去补它：test/e2e 已有
三节点进程级 harness（真 broker 进程 + 真 SDK + 真 gRPC），场景测试长在
它上面，证据更强、路径更短。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
git checkout -
```

---

# Part C：admin 控制台集群视图 + follower 写转发 UX

## Task 9: 集群页可点击原型（用户确认门）

**Files:**
- Create: `prototypes/v2-batch5-cluster/`（从 `prototypes/base/` fork，gitignore 的临时工作区，不入库）
- Modify: `prototypes/base/README.md`（新增「集群」页行，确认状态列置为「确认中」）

**这是一个带用户门的 task：没有用户确认，Task 11 不得开工。**

- [ ] **Step 1: 调用 prototyping-in-brainstorm skill**

```
Skill: prototyping-in-brainstorm
```

按它的流程走：确认 `prototypes/base/` 存在（已存在），fork 到 `prototypes/v2-batch5-cluster/`。

- [ ] **Step 2: 读基准**

```bash
cat prototypes/base/README.md && ls prototypes/base/
```

关键约束（来自 base/README.md）：`shared/app.css` 是 `web/src/styles/app.css` 的逐字副本，**不得就地改样式**；侧栏结构与 `shared/shell.html` 逐段一致；「offset 带」是本站的签名元素。

- [ ] **Step 3: fork 并写集群页原型**

```bash
cp -R prototypes/base prototypes/v2-batch5-cluster
```

在副本里新增 `cluster.html`，并在副本的侧栏 HTML 里把「集群」加进「排查」分组之后的新分组「运维」。页面内容（形态提案，实现时以用户确认的版本为准）：

- **顶部一行节点卡**：每个节点一张，显示 `node_id` / raft 地址 / 状态（本节点高亮）。
- **组表**：一行一个 raft 组（组 0 = meta，其余是数据组），列：组号、leader 节点、本节点角色（leader/follower/learner）、applied index、日志落后量（`leader applied − 本节点 applied`）。
- **复制进度**（仅在本节点是该组 leader 时有数据）：每个 peer 的 `Match` / `Next` / `RecentActive` / 是否 learner / `PendingSnapshot`。`PendingSnapshot != 0` 时用醒目标记——那正是 batch④ 定向台账要解决的「快照卡住」现场。
- **全站提示**：本节点不是任何组 leader 时，页面顶部挂一条说明条（承接 Task 12 的写转发 UX）。

「落后量」这一列用 base 的签名元素——**offset 带**来表现（它本来就是"位点差"的可视化语汇），不要新造一种图形。

- [ ] **Step 4: 起原型站并让用户看**

```bash
cd prototypes/v2-batch5-cluster && python3 -m http.server 8899
```

用浏览器工具打开 `http://127.0.0.1:8899/cluster.html`，截图/描述形态，**明确请用户确认**：

> 集群页原型在 `prototypes/v2-batch5-cluster/cluster.html`（本地 8899 端口可点）。节点卡 + 组表 + 复制进度三段，落后量沿用 base 的 offset 带。形态确认后我再写 `web/src/pages/Cluster.tsx`。有要改的吗？

- [ ] **Step 5: 用户确认后登记到 base/README.md**

在 `prototypes/base/README.md` 的页面表里新增一行：

| 集群 | `cluster.html` | `web/src/pages/Cluster.tsx` | 确认中 |

（列顺序以该文件既有表头为准。）状态填**确认中**——要等 Task 11 的真实页面开发完成并对照原型验收通过，才由后续流程推进为「已确认」。

- [ ] **Step 6: 提交（base/README.md 到 main；fork 副本不入库）**

```bash
git checkout main
git add prototypes/base/README.md
git commit -m "$(cat <<'EOF'
docs(prototypes): base 页面表登记「集群」页，状态确认中

形态已在 prototypes/v2-batch5-cluster/ 的 fork 副本里与用户确认：节点卡
+ 组表 + 复制进度三段，落后量沿用 base 的签名元素 offset 带（它本来就是
位点差的可视化语汇，不新造图形）。

状态填「确认中」而非「已确认」：要等 web/src/pages/Cluster.tsx 开发完成
并对照本次确认的原型验收通过，才由后续流程推进。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
git checkout -
```

---

## Task 10: 后端 `GET /admin/cluster`

**Files:**
- Create: `internal/admin/cluster.go`
- Create: `internal/admin/cluster_test.go`
- Modify: `internal/cluster/manager.go`（`SelfID` / `PeerAddrs` 导出）
- Modify: `internal/replication/replication.go`（`ClusterView` DTO + `Topologer` 接口 + 两端实现）
- Modify: `internal/admin/server.go`（路由 + 依赖注入）
- Modify: `cmd/sq/main.go`（装配）

**Interfaces:**
- Produces：
  ```go
  // internal/replication
  type ClusterView struct {
      Enabled bool
      SelfID  uint64
      Nodes   []NodeView
      Groups  []GroupView
  }
  type NodeView struct{ ID uint64; RaftAddr string; Self bool }
  type GroupView struct {
      ID       uint32
      Leader   uint64
      IsLeader bool
      Role     string // leader | follower | learner | candidate | unknown
      Applied  uint64
      Term     uint64
      Peers    []PeerProgressView
  }
  type PeerProgressView struct {
      ID              uint64
      Match           uint64
      Next            uint64
      State           string
      RecentActive    bool
      IsLearner       bool
      PendingSnapshot uint64
  }
  type Topologer interface{ Topology() ClusterView }
  ```
  `func (m *Manager) SelfID() uint64`、`func (m *Manager) PeerAddrs() map[uint64]string`、`func (r *Cluster) Topology() ClusterView`、`func (StandaloneRouter) Topology() ClusterView`
- Consumes：既有 `Manager.Status(g) (raft.Status, bool)`、`Manager.Leader(g)`、`Manager.AppliedIndex(g)`、`Manager.Groups()`

- [ ] **Step 1: 写失败的 handler 测试**

新建 `internal/admin/cluster_test.go`：

```go
package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xushixin/sq/internal/replication"
)

// fakeTopology 受控的集群拓扑来源。
type fakeTopology struct{ view replication.ClusterView }

func (f fakeTopology) Topology() replication.ClusterView { return f.view }

// TestHandleClusterReturnsTopology 集群端点把拓扑原样吐成 JSON。
func TestHandleClusterReturnsTopology(t *testing.T) {
	s := &Server{
		logger: testLogger(),
		topo: fakeTopology{view: replication.ClusterView{
			Enabled: true, SelfID: 2,
			Nodes: []replication.NodeView{
				{ID: 1, RaftAddr: "127.0.0.1:9081"},
				{ID: 2, RaftAddr: "127.0.0.1:9082", Self: true},
			},
			Groups: []replication.GroupView{{
				ID: 0, Leader: 1, IsLeader: false, Role: "follower", Applied: 42, Term: 3,
			}},
		}},
	}
	w := httptest.NewRecorder()
	s.handleCluster(w, httptest.NewRequest(http.MethodGet, "/admin/cluster", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("应 200，得到 %d：%s", w.Code, w.Body.String())
	}
	var got replication.ClusterView
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}
	if !got.Enabled || got.SelfID != 2 || len(got.Nodes) != 2 || len(got.Groups) != 1 {
		t.Fatalf("拓扑未原样返回: %+v", got)
	}
	if got.Groups[0].Role != "follower" || got.Groups[0].Applied != 42 {
		t.Fatalf("组视图字段不对: %+v", got.Groups[0])
	}
}

// TestHandleClusterStandaloneReportsDisabled 单机档必须明确回
// enabled=false，而不是 503——前端据此渲染「当前为单机模式」而不是报错。
func TestHandleClusterStandaloneReportsDisabled(t *testing.T) {
	s := &Server{logger: testLogger(), topo: fakeTopology{view: replication.ClusterView{Enabled: false}}}
	w := httptest.NewRecorder()
	s.handleCluster(w, httptest.NewRequest(http.MethodGet, "/admin/cluster", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("单机档应 200 且 enabled=false，得到 %d", w.Code)
	}
	var got replication.ClusterView
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got.Enabled {
		t.Fatal("单机档 enabled 应为 false")
	}
}
```

`testLogger()` 用 `internal/admin` 既有的测试日志辅助函数；没有就地加一个。JSON 字段名以 `ClusterView` 上的 `json:` tag 为准（Step 3 里定义为 snake_case）。

- [ ] **Step 2: 跑测试确认失败**

```bash
go test ./internal/admin -run TestHandleCluster -v
```

预期：编译失败，`s.topo` / `s.handleCluster` / `replication.ClusterView` undefined。

- [ ] **Step 3: `replication` 的 DTO 与两端实现**

`internal/replication/replication.go` 追加：

```go
// ClusterView 控制台用的集群拓扑只读快照。
//
// 为什么 DTO 定在 replication 而不是 cluster：依赖方向是
// cluster → replication → 上层，admin 只依赖 replication；把 raft 的
// tracker.Progress 直接漏给 admin 会让协议面耦合 raft 细节。
type ClusterView struct {
	// Enabled=false 表示单机档：前端据此渲染「当前为单机模式」，而不是报错
	Enabled bool        `json:"enabled"`
	SelfID  uint64      `json:"self_id"`
	Nodes   []NodeView  `json:"nodes"`
	Groups  []GroupView `json:"groups"`
}

// NodeView 成员表里的一个节点。
type NodeView struct {
	ID       uint64 `json:"id"`
	RaftAddr string `json:"raft_addr"`
	Self     bool   `json:"self"`
}

// GroupView 一个 raft 组在**本节点视角**下的状态。
//
// 注意：Peers 只有在本节点是该组 leader 时才有内容——raft 的
// tracker.Progress 只在 leader 上维护，follower 上是空表。前端必须按
// IsLeader 决定要不要渲染这一段，不能把空表当成"没有 peer"。
type GroupView struct {
	ID       uint32             `json:"id"`
	Leader   uint64             `json:"leader"`
	IsLeader bool               `json:"is_leader"`
	Role     string             `json:"role"`
	Applied  uint64             `json:"applied"`
	Term     uint64             `json:"term"`
	Peers    []PeerProgressView `json:"peers"`
}

// PeerProgressView leader 视角下某个 peer 的复制进度。
//
// PendingSnapshot 非零 = 该 peer 正在被发快照。长期非零就是 batch④ 定向
// 台账要解决的「快照卡住」现场，前端应当醒目标记。
type PeerProgressView struct {
	ID              uint64 `json:"id"`
	Match           uint64 `json:"match"`
	Next            uint64 `json:"next"`
	State           string `json:"state"`
	RecentActive    bool   `json:"recent_active"`
	IsLearner       bool   `json:"is_learner"`
	PendingSnapshot uint64 `json:"pending_snapshot"`
}

// Topologer 集群拓扑只读视图来源。单机后端回 Enabled=false 的空视图。
type Topologer interface {
	Topology() ClusterView
}

// Topology 单机档没有集群拓扑：回 Enabled=false，让前端渲染「单机模式」
// 而不是把空数组当成"集群里没有节点"。
func (StandaloneRouter) Topology() ClusterView { return ClusterView{Enabled: false} }

// Topology 汇总本节点视角下的全部组状态。
//
// 注意：Status(g) 的 Progress 只在本节点是该组 leader 时非空（raft 的
// tracker 只在 leader 上维护），follower 上 Peers 恒为空切片。
func (r *Cluster) Topology() ClusterView {
	self := r.m.SelfID()
	v := ClusterView{Enabled: true, SelfID: self}
	for id, addr := range r.m.PeerAddrs() {
		v.Nodes = append(v.Nodes, NodeView{ID: id, RaftAddr: addr, Self: id == self})
	}
	sort.Slice(v.Nodes, func(i, j int) bool { return v.Nodes[i].ID < v.Nodes[j].ID })

	for g := uint32(0); g < r.m.Groups(); g++ {
		st, ok := r.m.Status(g)
		if !ok {
			continue
		}
		leader, _ := r.m.Leader(g)
		gv := GroupView{
			ID:       g,
			Leader:   leader,
			IsLeader: leader == self,
			Role:     st.RaftState.String(),
			Applied:  r.m.AppliedIndex(g),
			Term:     st.Term,
			Peers:    make([]PeerProgressView, 0, len(st.Progress)),
		}
		for id, pr := range st.Progress {
			gv.Peers = append(gv.Peers, PeerProgressView{
				ID: id, Match: pr.Match, Next: pr.Next,
				State: pr.State.String(), RecentActive: pr.RecentActive,
				IsLearner: pr.IsLearner, PendingSnapshot: pr.PendingSnapshot,
			})
		}
		sort.Slice(gv.Peers, func(i, j int) bool { return gv.Peers[i].ID < gv.Peers[j].ID })
		v.Groups = append(v.Groups, gv)
	}
	return v
}
```

import 补 `"sort"`。`st.RaftState` / `st.Term` 的确切取法以 vendored raft 的 `raft.Status`（内嵌 `BasicStatus` → `SoftState` / `HardState`）为准；若字段路径不同（如 `st.SoftState.RaftState`、`st.HardState.Term`）按实际改，**不要为了编译通过而丢字段**。

- [ ] **Step 4: `Manager` 导出两个访问器**

`internal/cluster/manager.go`：

```go
// SelfID 返回本节点在成员表中的 id。
func (m *Manager) SelfID() uint64 { return m.nodeID }

// PeerAddrs 返回完整成员表（**含本节点**）的 id → raft 地址副本。
//
// 与内部的 peerAddrs 的区别：那个刻意剔除本节点（传输层不给自己发消息），
// 这个是给控制台看拓扑用的，缺了本节点就成了残缺的成员表。
func (m *Manager) PeerAddrs() map[uint64]string {
	out := make(map[uint64]string, len(m.peers))
	for id, addr := range m.peers {
		out[id] = addr
	}
	return out
}
```

- [ ] **Step 5: 新建 `internal/admin/cluster.go`**

```go
// cluster.go: GET /admin/cluster —— 集群拓扑与复制进度只读视图。
//
// 职责：
//   - 把 replication.Topologer 的快照原样吐成 JSON，供控制台集群页消费
//
// 边界：
//   - 不做阈值判断：哪个落后量算"危险"、什么时候标红是前端的事
//   - 单机档不报错：回 enabled=false 让前端渲染「当前为单机模式」，
//     503 会让控制台把一个正常形态显示成故障
//   - 只读：领导权转移、成员变更等写操作不在本端点（管理动作要独立
//     的确认流程，不能挂在一个被前端每 5 秒轮询的 GET 上）
package admin

import "net/http"

// handleCluster GET /admin/cluster：成员表 + 每组的 leader/角色/applied，
// 本节点是 leader 的组另附各 peer 的复制进度。
//
// 注意：
//   - topo 未装配时视为单机档（回 enabled=false），不返回 503——控制台
//     在单机部署下也会打开这个页面，那不是故障
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.topo == nil {
		s.logger.Debug("集群端点被调用但拓扑来源未装配，按单机档返回")
		s.writeJSON(w, http.StatusOK, replication.ClusterView{Enabled: false})
		return
	}
	v := s.topo.Topology()
	// 刻意不在这里按 leader 状态打 Warn：本端点被集群页每几秒轮询一次，
	// 「本节点不是任何组 leader」可能持续数小时，逐次记录会把真正的
	// 状态翻转淹掉。翻转的记录归 cluster 层的 leader 变更日志。
	s.logger.Debug("返回集群拓扑", "enabled", v.Enabled, "nodes", len(v.Nodes), "groups", len(v.Groups))
	s.writeJSON(w, http.StatusOK, v)
}
```

import 补 `"github.com/xushixin/sq/internal/replication"`。

- [ ] **Step 6: 路由与装配**

`internal/admin/server.go`：`Server` 结构体加字段

```go
	// topo 集群拓扑来源；单机档传 StandaloneRouter{}，nil 时按单机档处理
	topo replication.Topologer
```

`New` 的参数列表在 `fwd` 之后加 `topo replication.Topologer`，字面量里赋值。`routes` 里加：

```go
	s.mux.HandleFunc("GET /admin/cluster", s.protected(s.handleCluster))
```

`cmd/sq/main.go` 的 `admin.New(...)` 调用点：集群档传 `fwd` 对应的 `*replication.Cluster`（它同时实现 `Topologer`），单机档传 `replication.StandaloneRouter{}`。若 `rt` 变量本身就是这两者之一，直接把 `rt` 断言过去：

```go
	topo, _ := rt.(replication.Topologer)
	adm := admin.New(rep, rt, fwd, topo, st, mt, pr, dl, cfg.AdminUsername, cfg.AdminPassword, sys, sp, reg, srv, logger)
```

同步更新 `internal/admin` 下所有构造 `Server` 的测试。

- [ ] **Step 7: 加关键节点日志**

- `handleCluster` 未装配拓扑来源：`Debug`（正常形态，不是错误）
- 返回拓扑：`Debug`，带 `enabled` / `nodes` / `groups` 数量——成功路径不静默，但因为是轮询端点所以压到 Debug
- **刻意不打的**：「本节点不是任何组 leader」不在这里记 Warn（理由已写进代码注释：轮询端点 + 长期状态 = 会淹掉真正的翻转记录）

- [ ] **Step 8: 加注释**

`cluster.go` 文件头（职责 + 边界，含「只读，管理动作不挂在轮询 GET 上」）；`ClusterView` / `GroupView` / `PeerProgressView` / `Topologer` 的 doc 注释，其中 **`GroupView.Peers` 只在 leader 上非空**、**`PendingSnapshot` 非零的含义**、**`PeerAddrs` 与内部 `peerAddrs` 的区别** 三处是必须写清的「为什么」。

- [ ] **Step 9: 跑测试**

```bash
go build ./... && go test ./internal/admin ./internal/replication ./internal/cluster
```

预期：全绿。

- [ ] **Step 10: 提交**

```bash
git add internal/admin/cluster.go internal/admin/cluster_test.go internal/admin/server.go internal/replication/replication.go internal/cluster/manager.go cmd/sq/main.go
git commit -m "$(cat <<'EOF'
feat(admin,replication): GET /admin/cluster——成员表 + 每组角色/applied + leader 侧复制进度

DTO 定在 replication 而不是 cluster：依赖方向是 cluster → replication →
上层，把 raft 的 tracker.Progress 直接漏给 admin 会让协议面耦合 raft 细节。

单机档回 enabled=false 而不是 503：控制台在单机部署下也会打开这个页面，
503 会把一个正常形态显示成故障。

GroupView.Peers 只在本节点是该组 leader 时非空（raft 的 tracker 只在
leader 上维护）——doc 注释写明，前端必须按 is_leader 决定渲不渲染，不能
把空表当成"没有 peer"。PendingSnapshot 非零即 batch④ 定向台账要解决的
「快照卡住」现场，留给前端醒目标记。

Manager.PeerAddrs 含本节点，与内部 peerAddrs（刻意剔除自己，传输层不给
自己发消息）刻意不同——控制台缺了本节点就是残缺的成员表。

验证：go test ./internal/admin ./internal/replication ./internal/cluster 全绿（macOS）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: 控制台集群页

**Files:**
- Create: `web/src/pages/Cluster.tsx`
- Create: `web/src/pages/Cluster.test.tsx`
- Modify: `web/src/api/types.ts`（`ClusterView` 等类型）
- Modify: `web/src/api/client.ts`（具名端点 `cluster()`）
- Modify: `web/src/main.tsx`（路由 `/cluster`）
- Modify: `web/src/components/Shell.tsx`（侧栏「运维」分组 + 「集群」入口）

**Interfaces:**
- Consumes：Task 10 的 `GET /admin/cluster` 与其 JSON 形状；Task 9 用户确认过的原型形态
- Produces：`export function Cluster()`、`api.cluster(): Promise<ClusterView>`

**前置：Task 9 的用户确认门必须已通过。**

- [ ] **Step 1: 加类型**

`web/src/api/types.ts` 追加（字段名与后端 `json:` tag 逐一对应）：

```typescript
/** 成员表里的一个节点 */
export interface ClusterNode {
  id: number
  raft_addr: string
  self: boolean
}

/** leader 视角下某个 peer 的复制进度。pending_snapshot 非零 = 正在被发快照 */
export interface PeerProgress {
  id: number
  match: number
  next: number
  state: string
  recent_active: boolean
  is_learner: boolean
  pending_snapshot: number
}

/**
 * 一个 raft 组在本节点视角下的状态。
 *
 * peers 只在 is_leader 为 true 时有内容——raft 的复制进度只在 leader 上
 * 维护。渲染时必须按 is_leader 判断，空数组不等于"没有 peer"。
 */
export interface ClusterGroup {
  id: number
  leader: number
  is_leader: boolean
  role: string
  applied: number
  term: number
  peers: PeerProgress[]
}

/** 集群拓扑。enabled=false 表示当前是单机模式，不是故障 */
export interface ClusterView {
  enabled: boolean
  self_id: number
  nodes: ClusterNode[]
  groups: ClusterGroup[]
}
```

`web/src/api/client.ts` 的具名端点区追加：

```typescript
  /** 集群拓扑与复制进度（单机档回 enabled=false）。 */
  cluster: () => request<ClusterView>('/admin/cluster'),
```

并在该文件顶部的 import 里补 `ClusterView`。

- [ ] **Step 2: 写失败的组件测试**

新建 `web/src/pages/Cluster.test.tsx`（形态照抄同目录既有的 `Transactions.test.tsx`，包括它 mock `api` 的方式）：

```tsx
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it, vi } from 'vitest'
import { Cluster } from './Cluster'
import { api } from '../api/client'
import type { ClusterView } from '../api/types'

function view(over: Partial<ClusterView> = {}): ClusterView {
  return {
    enabled: true,
    self_id: 2,
    nodes: [
      { id: 1, raft_addr: '127.0.0.1:9081', self: false },
      { id: 2, raft_addr: '127.0.0.1:9082', self: true },
    ],
    groups: [
      { id: 0, leader: 1, is_leader: false, role: 'StateFollower', applied: 100, term: 3, peers: [] },
    ],
    ...over,
  }
}

describe('Cluster', () => {
  it('单机档渲染「单机模式」而不是错误', async () => {
    vi.spyOn(api, 'cluster').mockResolvedValue(view({ enabled: false, nodes: [], groups: [] }))
    render(<MemoryRouter><Cluster /></MemoryRouter>)
    expect(await screen.findByText(/单机模式/)).toBeInTheDocument()
  })

  it('渲染节点卡并标出本节点', async () => {
    vi.spyOn(api, 'cluster').mockResolvedValue(view())
    render(<MemoryRouter><Cluster /></MemoryRouter>)
    expect(await screen.findByText('127.0.0.1:9082')).toBeInTheDocument()
    expect(screen.getByText(/本节点/)).toBeInTheDocument()
  })

  it('follower 组不渲染复制进度段（peers 空不等于没有 peer）', async () => {
    vi.spyOn(api, 'cluster').mockResolvedValue(view())
    render(<MemoryRouter><Cluster /></MemoryRouter>)
    await screen.findByText('127.0.0.1:9082')
    expect(screen.queryByText(/复制进度/)).not.toBeInTheDocument()
    expect(screen.getByText(/复制进度仅在本节点为该组 leader 时可见/)).toBeInTheDocument()
  })

  it('leader 组渲染 peer 进度，快照挂起被标记', async () => {
    vi.spyOn(api, 'cluster').mockResolvedValue(view({
      groups: [{
        id: 1, leader: 2, is_leader: true, role: 'StateLeader', applied: 500, term: 3,
        peers: [
          { id: 1, match: 500, next: 501, state: 'StateReplicate', recent_active: true, is_learner: false, pending_snapshot: 0 },
          { id: 3, match: 0, next: 1, state: 'StateSnapshot', recent_active: false, is_learner: false, pending_snapshot: 480 },
        ],
      }],
    }))
    render(<MemoryRouter><Cluster /></MemoryRouter>)
    expect(await screen.findByText(/复制进度/)).toBeInTheDocument()
    expect(screen.getByText(/快照挂起/)).toBeInTheDocument()
  })
})
```

- [ ] **Step 3: 跑测试确认失败**

```bash
cd web && npx vitest run src/pages/Cluster.test.tsx
```

预期：`Cannot find module './Cluster'`。

- [ ] **Step 4: 写 `web/src/pages/Cluster.tsx`**

严格照 Task 9 用户确认过的原型形态实现，class 名沿用 `prototypes/base/shared/app.css` 里已有的（**不新增 CSS**——base/README.md 规定 app.css 是逐字副本，新样式必须先进 `web/src/styles/app.css` 再回流 base，本 task 不做那件事）。文件头注释：

```tsx
/**
 * 集群页：成员表、每组角色与 applied、leader 侧复制进度
 *
 * 职责：
 *   - 展示 GET /admin/cluster 的拓扑快照，5 秒轮询
 *   - 落后量用 offset 带表现（沿用全站既有的位点差语汇）
 *   - 快照挂起（pending_snapshot 非零）醒目标记
 *
 * 边界：
 *   - 单机档（enabled=false）渲染「当前为单机模式」，不当成错误
 *   - 只读：领导权转移、成员变更不在本页（管理动作要独立确认流程）
 *   - 复制进度只在本节点是该组 leader 时有数据——peers 为空不等于
 *     「没有 peer」，必须按 is_leader 判断，否则会把 follower 视角
 *     误显示成「集群里只有我一个」
 */
```

用 `usePoll(() => api.cluster(), 5000)`（与全站其他页同款）。

- [ ] **Step 5: 路由与侧栏**

`web/src/main.tsx` 加：

```tsx
        <Route path="/cluster" element={<Cluster />} />
```

并 import。`web/src/components/Shell.tsx` 的 `NAV` 追加：

```tsx
  { group: '运维' },
  { to: '/cluster', label: '集群' },
```

侧栏改动必须同步回 `prototypes/v2-batch5-cluster/shared/shell.html`（Task 9 的 fork 副本），保证「原型与真实前端同款」这条 base 约束在验收时成立。

- [ ] **Step 6: 加关键节点日志**

前端的"日志"载体是**用户可见的状态**，不是 `console.log`（全局约束明确禁止把 console.log 当日志机制）：

- 取数失败：页面内挂 `<Notice>` 错误条（复用 `web/src/components/Notice.tsx`），带后端返回的错误文本——**不静默降级**（Shell 的系统读数可以静默，那是辅助读数；集群页取不到数就是这个页面的全部内容取不到）
- 加载中：显式的加载态，不要用空表冒充"集群里没有节点"
- 单机档：显式文案「当前为单机模式，未启用集群复制」

- [ ] **Step 7: 加注释**

文件头（已在 Step 4）；`peers` 空表与 `is_leader` 的关系、`pending_snapshot` 标记的含义、5 秒轮询周期的选择理由——三处行内注释。

- [ ] **Step 8: 跑前端测试与构建**

```bash
cd web && npx vitest run && npx tsc --noEmit && npm run build
```

预期：全绿。

- [ ] **Step 9: 对照原型验收 + 把 base/README.md 推进为「已确认」**

起 dev server，打开 `/cluster`，与 `prototypes/v2-batch5-cluster/cluster.html` 逐段对照（节点卡、组表、复制进度、offset 带）。一致后把 `prototypes/base/README.md` 里「集群」行的状态由**确认中**推进为**已确认**，并把副本里的 `cluster.html` 与改过的 `shared/shell.html` 回流到 `prototypes/base/`（这正是 `finishing-a-development-branch` 会提示的那件事，此处提前做掉）。

- [ ] **Step 10: 提交**

```bash
git add web/src/pages/Cluster.tsx web/src/pages/Cluster.test.tsx web/src/api/types.ts web/src/api/client.ts web/src/main.tsx web/src/components/Shell.tsx
git commit -m "$(cat <<'EOF'
feat(web): 控制台集群页——成员表 + 组角色 + leader 侧复制进度

形态经 prototypes/v2-batch5-cluster/ 的可点击原型与用户确认，落后量沿用
全站既有的 offset 带语汇，不新增 CSS（app.css 是 base 的逐字副本，新样式
要先进 web 再回流）。

三条测试钉死容易搞错的地方：单机档（enabled=false）渲染「单机模式」而不是
错误；follower 组不渲染复制进度段并显式说明"仅在本节点为该组 leader 时
可见"——peers 空数组不等于「集群里只有我一个」；pending_snapshot 非零
渲染「快照挂起」标记。

取数失败挂 Notice 错误条而不是静默降级：Shell 的系统读数可以静默（辅助
读数），集群页取不到数就是整页内容都没了。

验证：vitest 全绿 + tsc --noEmit + npm run build（macOS）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"

git checkout main
git add prototypes/base
git commit -m "$(cat <<'EOF'
docs(prototypes): 集群页回流 base，状态推进为已确认

真实页面 web/src/pages/Cluster.tsx 已对照本次确认的原型逐段验收通过：
节点卡、组表、复制进度、offset 带一致。cluster.html 与改过的
shared/shell.html（新增「运维」分组）回流 base，保持基准与真实前端同步。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
git checkout -
```

---

## Task 12: follower 写转发 UX

**Files:**
- Modify: `internal/admin/message.go`（或 `handleMessageSend` 所在文件）
- Modify: `web/src/pages/Send.tsx`
- Modify: `web/src/pages/Cluster.tsx`
- Create/Modify: `web/src/pages/Send.test.tsx`
- Modify: `test/e2e/cluster_scenario_test.go`（一条转发 e2e）

**Interfaces:**
- Consumes：Task 10 的 `ClusterView`、既有 `replication.ErrNotLeader`、`ApplyOrForward` 与 `Forwarder.ForwardAppend`

**问题陈述：** 用户在控制台的「发送测试消息」页对着一个 follower 节点点发送时，当前会拿到一条底层的 `ErrNotLeader` 错误文本。这既不解释发生了什么，也不告诉用户该怎么办。集群档下写请求本来就有转发能力（`Forwarder.ForwardAppend`），UX 缺口是：**要么静默转发成功（并告诉用户转发了），要么给出可操作的提示**。

- [ ] **Step 1: 写失败的后端测试**

在 `internal/admin` 对应的测试文件追加：

```go
// TestMessageSendOnFollowerForwardsToLeader 控制台的发送测试消息在
// follower 上必须经转发落到 leader，而不是把 ErrNotLeader 甩给用户。
//
// 为什么不是"提示用户换节点"：控制台的地址通常是运维随手挑的一个节点，
// 要求用户自己找出 leader 再重开一个页面，是把系统内部状态外包给人。
func TestMessageSendOnFollowerForwardsToLeader(t *testing.T) {
	// 构造：rt.IsLeader 恒 false + fwd 记录被调用
	fwd := &recordingForwarder{queueID: 7, offset: 42}
	s := newTestServer(t, withRouter(followerRouter{}), withForwarder(fwd))

	w := httptest.NewRecorder()
	body := `{"topic":"t","body":"aGVsbG8="}`
	s.handleMessageSend(w, httptest.NewRequest(http.MethodPost, "/admin/messages/send", strings.NewReader(body)))
	if w.Code != http.StatusOK {
		t.Fatalf("follower 上的发送应经转发成功，得到 %d：%s", w.Code, w.Body.String())
	}
	if !fwd.called {
		t.Fatal("未经 ForwardAppend 转发（把 ErrNotLeader 甩给用户是 UX 缺陷）")
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["forwarded"] != true {
		t.Fatal("响应必须带 forwarded=true——用户有权知道这条走了转发")
	}
}
```

`newTestServer` / `withRouter` / `withForwarder` / `recordingForwarder` / `followerRouter` 按 `internal/admin` 既有测试的构造习惯就地实现（若既有测试是直接填 `&Server{...}` 字面量，就照那个写法，不要引入新的 option 体系）。

- [ ] **Step 2: 跑测试确认失败**

```bash
go test ./internal/admin -run TestMessageSendOnFollower -v
```

- [ ] **Step 3: 后端实现转发 + 标记**

`handleMessageSend`：本节点不是目标组 leader 时经 `s.fwd.ForwardAppend` 转发，响应体加 `"forwarded": true`；`fwd` 为 nil（单机档，`IsLeader` 恒真，理论不可达）时保持原有错误路径。

`SendResult` 的 JSON 加一个字段：

```go
	// Forwarded 表示这条消息是经本节点转发给组 leader 写入的。
	// 暴露给前端不是为了炫技：用户在 follower 上点发送、消息却写进了
	// 别的节点，这个事实必须可见，否则排查"我发的消息去哪了"时会先
	// 怀疑丢消息
	Forwarded bool `json:"forwarded,omitempty"`
```

- [ ] **Step 4: 前端 UX**

`web/src/pages/Send.tsx`：

- 发送成功且 `forwarded === true` 时，结果区加一条说明：「本节点不是该 topic 所属组的 leader，消息已自动转发给 leader 节点写入」——中性陈述，不是警告（这是正常行为）。
- 发送失败且错误里含 `ErrNotLeader` 语义时，错误条追加可操作指引：「本节点当前不是该组 leader 且转发未成功，请到集群页查看当前 leader」，并附一个跳 `/cluster` 的链接。

`web/src/pages/Cluster.tsx`：本节点不是任何组 leader 时，页面顶部挂一条中性说明条：「本节点当前不是任何 raft 组的 leader，写请求会被转发到对应组的 leader 节点」。

`web/src/pages/Send.test.tsx` 加两条断言：`forwarded: true` 的响应渲染转发说明；`ErrNotLeader` 错误渲染带集群页链接的指引。

- [ ] **Step 5: 加一条转发 e2e**

追加到 `test/e2e/cluster_scenario_test.go`：

```go
// TestScenarioAdminSendOnFollowerForwards 控制台发送测试消息的转发链路
// 端到端：对着**每一个**节点各发一条，全部必须成功且被消费到——总有
// 节点不是目标组的 leader，那条正是走转发的。
func TestScenarioAdminSendOnFollowerForwards(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-admin-forward", "scn-admin-forward-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	cs := newConfirmedSet()
	forwarded := 0
	for i := range pc.handles {
		// admin 端口 = 该节点配置里的 AdminListen
		base := "http://" + strings.TrimPrefix(pc.cfgs[i].AdminListen, ":")
		if strings.HasPrefix(pc.cfgs[i].AdminListen, ":") {
			base = "http://127.0.0.1" + pc.cfgs[i].AdminListen
		}
		id, fwd := adminSend(t, base, topic, fmt.Sprintf("forward probe #%d", i))
		cs.confirm(id)
		if fwd {
			forwarded++
		}
	}
	t.Logf("三节点各发一条，其中 %d 条走了转发", forwarded)
	if forwarded == 0 {
		t.Fatal("三个节点都恰好是目标组 leader（不可能，除非转发标记没生效）")
	}

	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 120*time.Second, "admin-forward 对账")
	cs.assertAllConsumed(t, got)
}
```

`adminSend` 是新增的辅助函数：POST `/admin/messages/send`（body 用 base64 编码），解析响应拿 `msg_id` 与 `forwarded`。若节点配置里 admin 端口没有按节点区分，先在 `clusterNodeConfig` 里给每个节点分配独立的 `AdminListen` 端口（用 `pickPorts` 再要 3 个）。

- [ ] **Step 6: 加关键节点日志**

- 后端转发分支：`Info`，带 `topic` / `group`（组号）/ `leader` 节点 id——转发是跨节点动作，必须留痕
- 转发失败：`Error`，带同样上下文 + `err`
- 前端：转发说明与错误指引都是用户可见文案（前端的"日志"），不用 console

- [ ] **Step 7: 加注释**

`SendResult.Forwarded` 上「为什么要暴露给前端」（已在 Step 3）；`handleMessageSend` 转发分支的「为什么不是提示用户换节点」；`Cluster.tsx` 顶部说明条为什么是中性陈述而不是警告——三处「为什么」注释。

- [ ] **Step 8: 全量验证**

```bash
go build ./... && go test ./internal/... && cd web && npx vitest run && npx tsc --noEmit && npm run build
cd ../test/e2e && go test -tags e2e -run TestScenarioAdminSendOnFollowerForwards -timeout 20m -v ./...
```

预期：全绿。

- [ ] **Step 9: 提交**

```bash
git add internal/admin web/src/pages/Send.tsx web/src/pages/Send.test.tsx web/src/pages/Cluster.tsx test/e2e/cluster_scenario_test.go
git commit -m "$(cat <<'EOF'
feat(admin,web): follower 写转发 UX——自动转发并如实告知，不把 ErrNotLeader 甩给用户

控制台的地址通常是运维随手挑的一个节点，要求用户自己找出 leader 再重开
页面，是把系统内部状态外包给人。改成经 ForwardAppend 自动转发。

但转发必须**可见**：响应带 forwarded=true，前端渲染一条中性说明。用户在
follower 上点发送、消息却写进了别的节点，这个事实藏起来会让排查"我发的
消息去哪了"时先怀疑丢消息。

转发确实失败时，错误条给可操作指引 + 集群页链接，而不是一句 ErrNotLeader。

e2e 对着三个节点各发一条并断言至少一条走了转发——全都不走转发只可能是
标记没生效，那条断言防的就是"功能没接上但测试全绿"。

验证：go test ./internal/... + vitest + tsc + build 全绿；
TestScenarioAdminSendOnFollowerForwards PASS（macOS）。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

# 收尾

- [ ] **全量回归（三平台）**

```bash
# macOS
go build ./... && go test ./... && go test -race ./internal/cluster
cd web && npx vitest run && npx tsc --noEmit && npm run build
cd ../test/e2e && go test -tags e2e -timeout 90m ./...

# 两台 Linux（交叉编译推送，不装工具链）
cd /Users/xushixin/workspace/sq
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/sq-linux ./cmd/sq
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o /tmp/cluster.test ./internal/cluster
cd test/e2e && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -tags e2e -c -o /tmp/e2e.test .
for h in 100.90.99.61 47.80.240.57; do
  scp /tmp/sq-linux /tmp/cluster.test /tmp/e2e.test root@$h:/tmp/
  ssh root@$h "cd /tmp && ./cluster.test -test.timeout 30m 2>&1 | tail -5 && SQ_E2E_BROKER=/tmp/sq-linux ./e2e.test -test.timeout 90m 2>&1 | tail -20"
done
```

- [ ] **`instrumenting-code` 完工自检**（逐条打勾，任一未过即回去补）

  - [ ] 每个错误分支都带上下文与 cause 打了日志
  - [ ] 每个外部调用（raft ReadIndex、ForwardAppend、admin HTTP）前后有日志
  - [ ] 成功路径不静默（读屏障放行、拓扑返回、转发成功各有一条）
  - [ ] 无 `fmt.Printf` / `console.log` 充当日志机制
  - [ ] 新文件都有文件头注释（职责 + 边界）：`readbarrier.go`、`cluster_proc_test.go`、`cluster_scenario_test.go`、`internal/admin/cluster.go`、`Cluster.tsx`
  - [ ] 导出方法都有 doc 注释；非显然分支都有「为什么」注释

- [ ] **CLAUDE.md §5 最终审阅清单**（逐条确认）：完成目标 / 架构一致 / 文件头注释 / 方法注释 / 中文注释 / 合理日志 / 无跨层调用 / 跨模块走 Facade / 优先复用 / 无硬编码 / 事务透传。

- [ ] **backlog 收尾**（提交到 `main`）：B8.2 行补上 batch⑤ 的证据（读屏障 + 控制台集群视图 + 写转发 UX），B8.3 行在 Task 8 已收尾。若 B8 epic 至此全部完成，把 epic 行也推进为 done。

- [ ] **交棒**：调用 `finishing-a-development-branch`。它会提示「本分支原型改动是否回流 `prototypes/base/`」——Task 11 Step 9 已经做过，据实回答即可。**分支合并由用户自己做。**

---

# 自审记录（写完计划后的自检）

**1. spec 覆盖**

| spec / backlog 要求 | 落点 |
|---|---|
| §8.2 kill leader（含 kill -9） | Task 5 |
| §8.2 网络分区（少数派不可写、愈合后追齐） | Task 6（杀 3 之 2 等价模拟，理由已写进 harness 文件头与用例注释） |
| §8.2 滚动重启 | Task 7 |
| §8.2 断电模拟（已确认消息不丢） | Task 8 |
| batch④ 外推：毒消息 → DLQ | Task 8 |
| batch④ 外推：producer 提交后立退 | Task 8 |
| batch④ 外推：read-index / apply barrier 完整读屏障 | Task 1-3 |
| batch④ 外推：admin 控制台集群视图 | Task 9-11 |
| batch④ 外推：follower 写转发 UX | Task 12 |
| §8.2「补场景测试框架预留的集群节点实现」 | **刻意不做**，Task 8 Step 8 订正 backlog 措辞并说明理由 |
| §8.3「不变量对账器原样复用」 | 复用不成立（`test/scenario` 不存在），改为 Task 5 的 `confirmedSet`，口径写进注释与提交说明 |

**2. 占位符扫描**：无 TBD / TODO / "类似 Task N" / "加上适当的错误处理"。每个代码 step 都给出可直接落盘的代码；三处「以实际签名为准」的说明（`raft.Status` 字段路径、`recvAllAck` 的 strict 语义、`newClusterProducer` 是否注册 cleanup）都写明了**具体怎么判断和怎么改**，不是含糊带过。

**3. 类型一致性**

- `readWait` / `barrierBatch` / `readWaiterInfo` / `readIndexOnce` / `readBarrier` / `batchSize`：Task 1 定义，Task 2 使用，命名一致。
- `gr.readIndexFn` 在 Task 2 Step 3 声明、Step 4 使用、测试注入，三处一致。
- `Router.ReadBarrier(ctx, g)` 签名在 Task 3 的接口、两个实现、`deliver` 调用点、测试替身四处一致。
- `ClusterView` / `NodeView` / `GroupView` / `PeerProgressView` 的 Go 字段与 TS interface 字段经 `json:` tag 逐一对应（Task 10 Step 3 ↔ Task 11 Step 1）。
- `procCluster` 的方法名在 Task 4 定义，Task 5-8、12 使用，一致（`endpoints()` / `endpointOf` / `indexOfEndpoint` / `kill` / `stopGraceful` / `restart` / `pause` / `resume` / `aliveCount` / `stopAll` / `multi`）。
- `confirmedSet` 的 `confirm` / `size` / `assertAllConsumed` 在 Task 5 定义，Task 6-8、12 使用，一致。
- Task 6 直接读了 `healCS.ids`（同包未导出字段），已在该 step 注明可改用 `mergeInto` —— 若实现者选了 `mergeInto`，需同时在 Task 5 的 `confirmedSet` 上补该方法。
