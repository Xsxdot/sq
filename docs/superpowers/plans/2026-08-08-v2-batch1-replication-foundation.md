# V2 batch①：复制地基——类型化 Batch + etcd/raft 薄壳 spike 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 V2 raft 拦截点的编译期地基（B2：类型化 Batch），并用最小三节点 etcd/raft 原型实测标定 V2 关键参数（B8.1：选举耗时、两档刷盘吞吐、learner 追齐），数据回填 spec。

**Architecture:** 见 [V2 复制层设计](../specs/2026-08-08-sq-v2-replication-design.md)。本批只做 §4 拦截点的第一半（类型化 Batch）+ §8.1 spike；复制接口的最终形状由 spike 实证后在 batch② 定。spike 放独立 Go module（`spike/raftshell/`），`go.etcd.io/raft/v3` 不进主模块依赖图（B4 的教训）。

**Tech Stack:** Go 1.26、cockroachdb/pebble（版本与主模块对齐）、go.etcd.io/raft/v3。

## Global Constraints

- 分支：`v2-batch1-replication-foundation`，从 main 拉出；docs 回填提交随分支走（合并时一起进 main）。
- spike 是独立 module（`spike/raftshell/go.mod`），主模块 `go.mod` **不得**出现 `go.etcd.io/raft`。
- 注释中文，解释「为什么」；新文件必须有文件头职责/边界注释；导出方法必须有 doc comment（全局 CLAUDE.md §2）。
- 日志用 `log/slog`，禁止 `fmt.Printf` 作为日志机制；热循环内降级 Debug。
- 每个 commit message 结尾：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。
- 不得改动 `/Users/xushixin/workspace/sq-m5`、`/Users/xushixin/workspace/sq-m5b`。
- Go 代理：`GOPROXY=https://goproxy.cn,direct`（spike module 首次拉依赖需要）。
- 远程 Linux 上跑测试/基准：本地交叉编译 `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c` 后 scp，不在远端装工具链；测试二进制 flag 带 `-test.` 前缀。

---

### Task 1: `store.Batch` 类型化——「唯一写入口」编译期强制（B2）

**Files:**
- Modify: `internal/store/store.go`（`NewBatch`/`Apply`/`ApplyAsync`/`Pending` 签名改造）
- Modify: `internal/core/{adminops,retention,txn,deliver,meta,delay,produce}/*.go`、`internal/metrics/series.go`、`internal/rpc/handle_secret.go`（34 处调用点迁移）及各包 `_test.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Consumes: 现有 `Store.NewBatch() *pebble.Batch`、`Apply(*pebble.Batch)`、`ApplyAsync(*pebble.Batch) (Pending, error)`。
- Produces（后续 batch② 的复制接口在此之上拦截）:
  - `type Batch struct{ b *pebble.Batch }`（字段不导出——调用方无法触及裸 batch）
  - `func (s *Store) NewBatch() *Batch`
  - `func (b *Batch) Set(key, value []byte) error`
  - `func (b *Batch) Delete(key []byte) error`
  - `func (b *Batch) DeleteRange(start, end []byte) error`
  - `func (b *Batch) Close() error`
  - `func (s *Store) Apply(b *Batch) error`、`func (s *Store) ApplyAsync(b *Batch) (Pending, error)`（内部用 `b.b`，其余逻辑不变）

- [ ] **Step 1: 写失败测试**

`internal/store/store_test.go` 追加：

```go
// TestTypedBatchRoundTrip 锁定类型化 Batch 的最小 API 面：
// Set/Delete/DeleteRange 经 Apply 提交后语义与裸 pebble 批次一致。
func TestTypedBatchRoundTrip(t *testing.T) {
	st := openTestStore(t, true) // 复用本文件既有的测试构造函数；若名字不同，以现存 helper 为准
	b := st.NewBatch()
	if err := b.Set([]byte("tb/k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("tb/k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("tb/k2")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := st.Get([]byte("tb/k1")); !ok || string(v) != "v1" {
		t.Fatalf("k1 = %q, ok=%v, want v1", v, ok)
	}
	if _, ok, _ := st.Get([]byte("tb/k2")); ok {
		t.Fatal("k2 应已被批内 Delete 删除")
	}

	// DeleteRange 走 ApplyAsync+Wait 路径，顺带锁定拆分提交同样接受类型化批次
	b2 := st.NewBatch()
	if err := b2.DeleteRange([]byte("tb/"), []byte("tb0")); err != nil {
		t.Fatal(err)
	}
	pending, err := st.ApplyAsync(b2)
	if err != nil {
		t.Fatal(err)
	}
	if err := pending.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.Get([]byte("tb/k1")); ok {
		t.Fatal("k1 应已被 DeleteRange 删除")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/store/ -run TestTypedBatchRoundTrip -v`
Expected: 编译错误（`b.Set` 参数不匹配——现在还是裸 `*pebble.Batch`，`Set` 要三个参数）。

- [ ] **Step 3: 实现 `store.Batch`**

`internal/store/store.go`，替换现有 `NewBatch`：

```go
// Batch 类型化写批次——「唯一写入口」的编译期强制（B2 / V2 spec §4）。
//
// 底层 *pebble.Batch 不导出：调用方只能通过本类型组装写入并交给
// Apply/ApplyAsync 提交，无法绕过写入口直接 Commit。V2 集群模式将在
// Apply/ApplyAsync 处拦截整个批次做 raft 复制，本类型是该拦截点成立的前提。
//
// 生命周期约定与旧 NewBatch 注释一致：组装后要么交给 Apply/ApplyAsync
//（此后批次归提交方处置），要么自行 Close 回收，两条路径二选一。
type Batch struct{ b *pebble.Batch }

// NewBatch 创建类型化写批次（生命周期约定见 Batch 类型注释）。
func (s *Store) NewBatch() *Batch { return &Batch{b: s.db.NewBatch()} }

// Set 在批内写入一个键值对。
func (b *Batch) Set(key, value []byte) error { return b.b.Set(key, value, nil) }

// Delete 在批内删除一个键。
func (b *Batch) Delete(key []byte) error { return b.b.Delete(key, nil) }

// DeleteRange 在批内删除 [start, end) 区间的所有键。
func (b *Batch) DeleteRange(start, end []byte) error { return b.b.DeleteRange(start, end, nil) }

// Close 回收未提交的批次（决定不提交时的唯一合法出口）。
func (b *Batch) Close() error { return b.b.Close() }
```

`Apply`/`ApplyAsync` 签名改为接收 `*Batch`，函数体内 `b` 全部换成 `b.b`（提交、Close、丢 GC 的注释与行为一字不动）；`Pending` 内部字段仍持 `*pebble.Batch`，不受影响。

- [ ] **Step 4: 迁移全部调用点**

机械迁移，9 个非测试文件 + 相关测试文件：`b.Set(k, v, nil)` → `b.Set(k, v)`；`b.Delete(k, nil)` → `b.Delete(k)`；`b.DeleteRange(a, z, nil)` → `b.DeleteRange(a, z)`；删除因此不再使用的 `pebble` import。用编译器当清单：

Run: `go build ./... ; echo "EXIT=$?"`（**不要**接管道，避免假 BUILD_OK）
反复修到 `EXIT=0`。

- [ ] **Step 5: 验证不变量——主模块除 store 外无人触及 pebble.Batch**

Run: `grep -rn "pebble.Batch" internal/ cmd/ --include="*.go" | grep -v "internal/store/"`
Expected: 零输出。有输出即说明拦截点有洞，回 Step 4。

- [ ] **Step 6: 全量回归**

Run: `go test -race ./... ; echo "EXIT=$?"`
Expected: EXIT=0，15 包全绿。

- [ ] **Step 7: 注释自检（instrumenting-code）**

本任务是纯签名重构、无新分支无 I/O 变化，**日志豁免**（明确记录于此，不是遗漏）；注释检查：`Batch` 类型注释必须写明「为什么不导出字段」与 V2 拦截点关系（Step 3 已含），旧 `NewBatch` 注释中仍成立的生命周期约定不得丢失。

- [ ] **Step 8: Commit**

```bash
git add -A internal/
git commit -m "feat(store): 类型化 Batch——唯一写入口编译期强制（B2）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: spike module 脚手架 + 单节点 raft 环

**Files:**
- Create: `spike/raftshell/go.mod`
- Create: `spike/raftshell/node.go`
- Create: `spike/raftshell/wal.go`
- Create: `spike/raftshell/transport.go`
- Test: `spike/raftshell/node_test.go`

**Interfaces:**
- Produces（Task 3/4/5 依赖）:
  - `type AckMode int`；`const (AckQuorumFsync AckMode = iota; AckQuorumMem)`
  - `func NewNode(id uint64, peers []raft.Peer, dir string, mode AckMode, tr Transport, lg *slog.Logger) (*Node, error)`
  - `func (n *Node) Start(ctx context.Context)`（启动 tick/Ready 循环）
  - `func (n *Node) Propose(ctx context.Context, payload []byte) error`（阻塞至该条目 commit 并 apply 到本节点 FSM）
  - `func (n *Node) Step(m raftpb.Message)`（transport 投递入口）
  - `func (n *Node) IsLeader() bool`、`func (n *Node) LeaderID() uint64`
  - `func (n *Node) AppliedCount() uint64`（FSM 计数，测试对账用）
  - `type Transport interface{ Send(from uint64, ms []raftpb.Message) }`

- [ ] **Step 1: 建 module**

```bash
mkdir -p spike/raftshell && cd spike/raftshell && go mod init github.com/xushixin/sq/spike/raftshell
PEBBLE_VER=$(cd ../.. && go list -m -f '{{.Version}}' github.com/cockroachdb/pebble)
GOPROXY=https://goproxy.cn,direct go get github.com/cockroachdb/pebble@$PEBBLE_VER go.etcd.io/raft/v3@latest
```

- [ ] **Step 2: 写失败测试（单节点自选举 + 提案落地）**

`node_test.go`：

```go
// 单节点集群自选举后，100 条提案全部 commit 并 apply。
// 这是薄壳最小闭环：tick 驱动、Ready 契约（先持久化后发送）、FSM apply。
func TestSingleNodeProposeApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tr := newChanTransport()
	n, err := NewNode(1, []raft.Peer{{ID: 1}}, t.TempDir(), AckQuorumFsync, tr, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	tr.register(1, n)
	n.Start(ctx)
	for i := 0; i < 100; i++ {
		if err := n.Propose(ctx, []byte(fmt.Sprintf("msg-%03d", i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	if got := n.AppliedCount(); got != 100 {
		t.Fatalf("applied = %d, want 100", got)
	}
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cd spike/raftshell && go test -run TestSingleNodeProposeApply -v`
Expected: 编译失败（类型未定义）。

- [ ] **Step 4: 实现**

`node.go` 核心（完整骨架，实现时按此写）：

```go
// Package raftshell 是 V2 复制层的 spike：用 go.etcd.io/raft/v3 搭最小
// 三节点原型，实测选举耗时、两档刷盘吞吐、learner 追齐速度，数据回填
// V2 spec §2.3/§6。
//
// 职责：raft 薄壳的可行性验证与参数标定。
// 边界：不是生产代码——不做快照压缩、不做成员持久化、不接入 sq 主模块；
//       独立 go module，依赖不进主模块图。
package raftshell

// Node 是一个 raft 节点薄壳：tick 驱动选举/心跳，Ready 循环执行
// 「持久化 → 发送 → apply → Advance」契约，FSM 为计数器 + 提案回执表。
type Node struct {
	id      uint64
	rn      raft.Node
	storage *raft.MemoryStorage // raft 库要求的易失存储视图
	wal     *WAL                // 我们自己的持久化（pebble），两档刷盘在此分流
	tr      Transport
	mode    AckMode
	lg      *slog.Logger

	inbox   chan raftpb.Message
	applied atomic.Uint64
	leader  atomic.Uint64

	mu      sync.Mutex
	waiters map[uint64]chan struct{} // 提案 id → commit 通知；id 编码在 payload 前 8 字节
	nextID  atomic.Uint64
}

func (n *Node) Start(ctx context.Context) {
	go n.run(ctx)
}

func (n *Node) run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond) // ElectionTick=10 → 选举超时约 1s
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			n.lg.Info("节点退出", "id", n.id)
			return
		case <-ticker.C:
			n.rn.Tick()
		case m := <-n.inbox:
			_ = n.rn.Step(ctx, m)
		case rd := <-n.rn.Ready():
			n.handleReady(ctx, rd)
		}
	}
}

// handleReady 执行 etcd/raft 的 Ready 契约。关键顺序（正确性所在）：
//  1. Entries/HardState 先持久化（AckQuorumFsync 档带 fsync）再发送 Messages——
//     否则本节点确认过的日志可能在崩溃后消失，违反 raft 假设；
//  2. AckQuorumMem 档刻意放松第 1 条（NoSync 落盘 + 后台周期 fsync），
//     这正是 spec §2.2 要实测的取舍，配套规则见 Task 5；
//  3. CommittedEntries apply 后才 Advance。
func (n *Node) handleReady(ctx context.Context, rd raft.Ready) {
	if err := n.wal.Persist(rd.HardState, rd.Entries, n.mode == AckQuorumFsync); err != nil {
		n.lg.Error("持久化失败，节点停摆", "id", n.id, "err", err)
		return
	}
	// MemoryStorage 是 raft 库读取日志的视图，必须与 WAL 同步推进
	if !raft.IsEmptyHardState(rd.HardState) {
		_ = n.storage.SetHardState(rd.HardState)
	}
	_ = n.storage.Append(rd.Entries)
	n.tr.Send(n.id, rd.Messages)
	if rd.SoftState != nil {
		n.leader.Store(rd.SoftState.Lead)
	}
	for _, ent := range rd.CommittedEntries {
		switch ent.Type {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			_ = cc.Unmarshal(ent.Data)
			n.rn.ApplyConfChange(cc)
			n.lg.Info("成员变更已 apply", "id", n.id, "type", cc.Type.String(), "node", cc.NodeID)
		case raftpb.EntryNormal:
			if len(ent.Data) >= 8 {
				n.applied.Add(1)
				n.notify(binary.BigEndian.Uint64(ent.Data[:8]))
			}
		}
	}
	n.rn.Advance()
}
```

`Propose`：分配 id → 注册 waiter → payload 前置 8 字节 id → `rn.Propose` → 等 waiter 或 ctx 超时（超时删 waiter 防泄漏）。`raft.Config`：`{ID, ElectionTick: 10, HeartbeatTick: 1, Storage: n.storage, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, Logger: 静默 logger}`（raft 库自身日志噪音大，用 `raft.DefaultLogger` 包 `log.New(io.Discard, "", 0)` 压掉，我们自己的 slog 打关键节点）。

`wal.go`：每节点独立 pebble 目录。`Persist(hs, ents, sync)`：一个批次写 hardstate（固定 key）+ 逐条 entry（key=`ent/<index 大端>`，覆盖语义天然处理日志截断回退），`batch.Commit(sync ? pebble.Sync : pebble.NoSync)`。`AckQuorumMem` 档另起后台 goroutine 每 200ms 提交一个空批次带 `pebble.Sync`（借 WAL 顺序性把此前所有 NoSync 写强制刷盘——spec §2.2「后台批量 fsync」的最简实现）。

`transport.go`：

```go
// chanTransport 进程内通道传输：每节点一个 inbox，支持注入单向延迟与
// 分区/摘除（Task 3 kill-leader、后续分区实验用）。
type chanTransport struct {
	mu    sync.RWMutex
	nodes map[uint64]*Node
	down  map[uint64]bool // 摘除的节点：投给它的消息直接丢弃
	delay time.Duration   // 模拟内网 RTT/2，默认 100µs
}
```

`Send` 对每条消息：目标 down 则丢弃并 Debug 日志计数；否则（延迟后）`nodes[m.To].Step(m)`。**热路径日志必须 Debug 级**。

- [ ] **Step 5: 跑测试确认通过**

Run: `cd spike/raftshell && go test -race -run TestSingleNodeProposeApply -v`
Expected: PASS。

- [ ] **Step 6: 日志与注释自检（instrumenting-code）**

关键节点日志核对：节点启动/退出（Info）、成为 leader / leader 变更（Info，在 SoftState 变化处）、持久化失败（Error 带节点 id 与 err）、成员变更（Info）、消息丢弃（Debug）。文件头注释三个新文件齐全；`handleReady` 的顺序契约注释（为什么先持久化后发送）必须在。

- [ ] **Step 7: Commit**

```bash
git add spike/
git commit -m "feat(spike): raftshell 单节点薄壳——tick/Ready 契约与两档刷盘 WAL（B8.1）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: 三节点集群——选举、复制、kill-leader 切换耗时

**Files:**
- Create: `spike/raftshell/cluster.go`（测试/基准共用的三节点装配）
- Test: `spike/raftshell/cluster_test.go`

**Interfaces:**
- Produces:
  - `type Cluster struct{ Nodes map[uint64]*Node; Tr *chanTransport; cancels map[uint64]context.CancelFunc }`
  - `func NewCluster(dir string, mode AckMode, lg *slog.Logger) (*Cluster, error)`（固定 3 节点 id 1/2/3）
  - `func (c *Cluster) WaitLeader(timeout time.Duration) (uint64, error)`
  - `func (c *Cluster) Kill(id uint64)`（cancel 该节点 ctx + transport 摘除——模拟宕机）
  - `func (c *Cluster) Leader() *Node`

- [ ] **Step 1: 写失败测试**

```go
// 三节点选出唯一 leader；1000 条提案后三节点 FSM 收敛一致。
func TestThreeNodeReplicate(t *testing.T) { /* NewCluster → WaitLeader →
	Leader().Propose ×1000 → 轮询等三节点 AppliedCount 均 ≥1000（含 ConfChange 前置量则按
	EntryNormal 计数，见 Task 2 FSM 只数 EntryNormal）*/ }

// kill leader 后剩余两节点在超时内选出新 leader，且此前已 commit 的条目不丢；
// 记录并打印重选耗时——这是 spec §6 切换窗口的第一个实测分量。
func TestKillLeaderFailover(t *testing.T) {
	// NewCluster → WaitLeader → Propose ×100 → c.Kill(leader) → start := time.Now()
	// → WaitLeader(5s)（新 leader ≠ 旧 leader）→ t.Logf("重选耗时 %v", time.Since(start))
	// → 新 leader Propose ×100 → 存活两节点 AppliedCount ≥ 200
}
```

- [ ] **Step 2: 确认失败** — Run: `go test -run TestThreeNode -v`，Expected: 编译失败。

- [ ] **Step 3: 实现 `cluster.go`** — 三个 `NewNode(id, peers{1,2,3}, dir/<id>, mode, tr, lg)`，各自 `Start`；`WaitLeader` 轮询三节点 `LeaderID()` 直到非零且一致且该节点存活。`Kill`：cancel + `tr.markDown(id)`，Info 日志「节点 %d 已摘除」。

- [ ] **Step 4: 跑测试通过** — Run: `cd spike/raftshell && go test -race ./... ; echo "EXIT=$?"`，Expected: EXIT=0，`TestKillLeaderFailover` 的日志里有重选耗时数字（预期 1~2s 量级：选举超时 1s + 随机化）。

- [ ] **Step 5: 日志与注释自检** — `Kill`/`WaitLeader` doc comment；重选耗时用 `t.Logf` 输出（测试即测量工具）；cluster.go 文件头注释。

- [ ] **Step 6: Commit**

```bash
git add spike/
git commit -m "feat(spike): 三节点集群与 kill-leader 切换实测（B8.1）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: 两档确认模式吞吐基准 + 内存峰值

**Files:**
- Test: `spike/raftshell/bench_test.go`

**Interfaces:**
- Consumes: `NewCluster`、`Node.Propose`。

- [ ] **Step 1: 写基准**

```go
// 两档刷盘的 propose→commit 吞吐。payload 100B 对齐 sq 基准的消息体量级。
// -cpu 决定并发提案数——raft 单组日志把并发提案合并进批次追加，
// 这里量的就是 spec §2.3 预估的合并效应。
func benchPropose(b *testing.B, mode AckMode) {
	c, err := NewCluster(b.TempDir(), mode, slog.New(slog.DiscardHandler))
	if err != nil { b.Fatal(err) }
	if _, err := c.WaitLeader(10 * time.Second); err != nil { b.Fatal(err) }
	payload := bytes.Repeat([]byte("x"), 100)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.Leader().Propose(ctx, payload); err != nil { b.Fatal(err) }
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkProposeQuorumFsync(b *testing.B) { benchPropose(b, AckQuorumFsync) }
func BenchmarkProposeQuorumMem(b *testing.B)   { benchPropose(b, AckQuorumMem) }
```

- [ ] **Step 2: 本机跑通拿首批数字**

Run: `cd spike/raftshell && go test -run '^$' -bench 'BenchmarkPropose' -cpu 1,16,64,256 -benchtime 3s 2>bench_stderr.log`
Expected: 两档 × 4 并发的 msg/s 矩阵。合理性检查：Fsync 档 cpu=1 应接近本机单流 fsync 速率（M1 Pro 约 239/s）；cpu=256 时合并效应应使其达数千以上；Mem 档整体高一个量级。异常则先查 `/tmp` 是否 tmpfs、pebble stderr 是否混入输出（历史坑）。

- [ ] **Step 3: Linux 实测 + 内存峰值**

本地交叉编译后传到可用的 Linux 机器（云服务器若已回收，用 100.90.99.61 或询问用户）：

```bash
cd spike/raftshell && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o raftshell.test .
scp raftshell.test root@<host>:/root/ && ssh root@<host> \
  '/usr/bin/time -v /root/raftshell.test -test.run "^$" -test.bench BenchmarkPropose \
   -test.cpu 64,256 -test.benchtime 3s 2>&1 | grep -E "msg/s|ns/op|Maximum resident"'
```

记录：两档吞吐 + Maximum resident set size（用户明确要求内存峰值必记）。

- [ ] **Step 4: 注释自检** — 基准函数注释说明「量的是什么、对齐 spec 哪一节」（Step 1 已含）。日志豁免（基准代码，静默 logger 是刻意的——避免污染测量）。

- [ ] **Step 5: Commit**

```bash
git add spike/
git commit -m "test(spike): 两档确认模式吞吐基准——quorum fsync vs quorum 内存（B8.1）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: 不干净重启 → learner 追齐原型（spec §2.2 安全配套的可行性验证）

**Files:**
- Modify: `spike/raftshell/node.go`、`spike/raftshell/wal.go`、`spike/raftshell/cluster.go`
- Test: `spike/raftshell/restart_test.go`

**Interfaces:**
- Produces:
  - `func (n *Node) StopClean(ctx)`（写干净关机标记后退出）
  - `func (c *Cluster) Restart(id uint64) error`：读标记——有标记→原身份 `RestartNode` 回归；无标记→**清空该节点状态目录，以 learner 身份重入**（leader 侧 `ProposeConfChange` Remove 旧 voter → AddLearner → 追平后 Promote 为 voter）
  - `func (c *Cluster) WaitConverged(timeout) error`（全体存活节点 AppliedCount 一致）

- [ ] **Step 1: 写失败测试**

```go
// AckQuorumMem 档下模拟断电（不写干净关机标记直接 kill）：重启节点必须
// 走 learner 追齐路径，最终 FSM 收敛，且已 commit 条目一条不丢。
// 这是 spec §2.2「不可裸关 fsync」配套规则的可行性验证 + 追齐耗时测量。
func TestUncleanRestartRejoinsAsLearner(t *testing.T) {
	// NewCluster(AckQuorumMem) → Propose ×1000 → Kill(follower)（不写标记，模拟断电）
	// → 继续 Propose ×1000（集群 2/3 照常工作）
	// → start := time.Now(); c.Restart(follower) → WaitConverged(30s)
	// → t.Logf("2000 条追齐耗时 %v", time.Since(start))
	// → 断言重启节点 AppliedCount == 2000（无丢失）
}

// 对照组：干净关机（写标记）重启走 RestartNode 原身份回归，不清目录不降级。
func TestCleanRestartResumes(t *testing.T) { /* StopClean → Restart → 断言未走 learner 路径
	（依据：重启后 storage 从 WAL 恢复出非零 lastIndex，且无 ConfChange 提案产生）*/ }
```

- [ ] **Step 2: 确认失败** — 编译失败（StopClean/Restart 未定义）。

- [ ] **Step 3: 实现**

`wal.go`：标记 key `meta/clean_shutdown`；启动时读后即删（下次崩溃自然无标记）；`StopClean` 写标记 + Sync 后 cancel。`Restart` 干净路径：`raft.RestartNode(cfg)` + 从 WAL 重放 entries/hardstate 进 MemoryStorage。learner 路径：`os.RemoveAll` 状态目录 → leader `ProposeConfChange(ConfChangeRemoveNode)` → `ProposeConfChange(ConfChangeAddLearnerNode)` → 新 Node 以空 storage 启动（`RestartNode`，peers 为空——身份由 leader 的 ConfChange 日志赋予）→ 追平（`AppliedCount` 达到 leader 值）后 `ProposeConfChange(ConfChangeAddNode)` 升回 voter。全程 Info 日志：「检测到不干净关机，清空状态以 learner 重入」「追齐完成，升级 voter」——这两条将来是生产排障的关键线索，措辞进代码。

注意：spike 不做日志压缩（无 Compact），追齐走全量 entry 重放；快照流式追齐是 B8.2 的事，此处注释写明边界。

- [ ] **Step 4: 跑测试通过** — Run: `go test -race -run 'Restart' -v`，Expected: PASS，日志含追齐耗时。

- [ ] **Step 5: 全量回归** — Run: `cd spike/raftshell && go test -race ./... ; echo "EXIT=$?"` → EXIT=0；主模块 `go test -race ./... ; echo "EXIT=$?"` → EXIT=0（确认 spike 未影响主模块）。

- [ ] **Step 6: 日志与注释自检** — learner 重入全流程每步有 Info 日志；`Restart` doc comment 写清两条路径的判定依据；错误分支（RemoveAll 失败、ConfChange 超时）带上下文 Error。

- [ ] **Step 7: Commit**

```bash
git add spike/
git commit -m "feat(spike): 不干净重启 learner 追齐——异步刷盘安全配套验证（B8.1）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: 数据回填 spec + 收尾

**Files:**
- Modify: `docs/superpowers/specs/2026-08-08-sq-v2-replication-design.md`（§2.3 追加「spike 实测」小节；§6 切换窗口填入重选耗时实测；§8.1 标注完成）
- Modify: `docs/superpowers/backlog.md`（B8.1 验收证据；B2 验收证据）

- [ ] **Step 1: 回填 spec** — §2.3 加表：平台、两档 × 并发的 msg/s、内存峰值、单流 fsync 对照；§6 填「重选实测 X s（ElectionTick=10/100ms tick 配置下）」；§8.1 spike 各判定项打勾并附测试名。数字必须全部来自 Task 3/4/5 的实际输出，禁止估算值冒充实测。

- [ ] **Step 2: 更新 backlog 验收证据** — B2：`go test -race ./...` 全绿 + `grep pebble.Batch` 零泄漏；B8.1：spike 套件 `-race` 全绿 + 基准/切换/追齐三组数字。状态流转（doing→done）留给 finishing-a-development-branch 的评审后执行，本步只填证据不改状态。

- [ ] **Step 3: Commit**

```bash
git add docs/
git commit -m "docs(spec): 回填 V2 spike 实测——选举耗时、两档刷盘吞吐、learner 追齐

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 4: 收尾自检（verification-before-completion）** — 两个 module 各自 `go test -race ./...` 全绿；`go vet ./...` 干净；主模块 `go.mod` 无 `go.etcd.io/raft`（`grep raft go.mod` 零输出）；instrumenting-code 清单过一遍（错误分支带上下文、成功路径不静默、文件头/导出注释齐全）。之后走 superpowers:finishing-a-development-branch。

---

## Self-Review 记录

- **Spec 覆盖**：本批对应 spec §4 前半（类型化 Batch）+ §8.1 全部（spike 三组测量：切换 Task 3、吞吐 Task 4、追齐 Task 5）。§3/§5/§6/§7 属 B8.2，刻意不在本批。
- **占位符扫描**：Task 3/5 的测试体用注释伪码描述断言流程（实现者需展开），其余任务均为可直接落盘的代码；关键 API 签名在各 Interfaces 块给全。
- **类型一致性**：`Transport.Send(from uint64, ms []raftpb.Message)` 与 chanTransport 实现一致；`NewNode` 六参签名在 Task 2/3 一致；`AckMode` 两常量全文统一。
