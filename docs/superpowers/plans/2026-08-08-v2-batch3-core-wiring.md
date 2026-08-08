# V2 batch③：核心接线与协议面 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 core 全部 33 个写点接上 `replication.Replicator`，装配三节点集群模式（config 集群段、QueryRoute 指向组 leader、可重试错误码、leader-only 定时器、leader 摊布、learner 重入自动编排、单机→单节点集群升级），以官方 SDK 三节点 e2e 收口。

**Architecture:** 写点按「归属纪律」划组——队列级键族（`msg/ alloc/ keyidx/ cursor/ inflight/`）归 `GroupForQueue(topic,q)`，全局键族（`meta/ delay/ delayalloc half/ halfidx/ handle_secret`）归 `MetaGroup=0`，`metric/` 本地不复制。跨组操作两条路：语义改造为「先写目标、后删来源」的两段式（幂等重放=at-least-once 允许的重复），以及两个转发原语（逻辑 ForwardAppend 保 leader-only 构造、构造无关批次的 ApplyForwarded）。单机模式零改动零开销：Standalone 后端直通，门控函数为 nil 即永真。

**Tech Stack:** 现有 internal/cluster（batch② 内核，已合并评审通过）+ internal/replication + etcd raft v3.7.0 + Pebble v2.1.6 + 官方 rocketmq-clients Go SDK v5.1.4（e2e）。

## Global Constraints

- **单机默认不回归**：不写 `cluster:` 配置段 = 今天的路径。Standalone 后端零额外开销；接线只许换类型不许换行为（`go test -race ./...` 全绿是底线，`test/e2e` 单机用例不许动断言）。
- **跨组无原子性承诺**（spec §4 原文：「预检通过后逐条按组提交，语义与单机一致——单条 at-least-once，无跨条原子性承诺」）：跨组两段式一律**先写目标、后删来源**，中间崩溃 = 重放 = 重复投递，方向永远不许反（反向 = 丢消息）。
- **本批不做日志截断/快照**（batch④）：raft 日志全量保留，learner 追齐 = 全量日志回放；单机→多节点集群扩容依赖快照，**本批只交付单机→单节点集群**，多节点扩容显式外推 batch④。
- **凭据不入复制**：AK/SK 是静态配置（三节点同配置文件，运维职责），无动态凭据 API 可复制；spec §3 的「凭据」在本批仅落 handle secret。计划偏离已在此记录。
- Linux 是一等验证平台：远程验证一律本地交叉编译（`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c`）+ scp 到 100.90.99.61（或 47.80.240.57 若还在），**不装远端工具链**；结果记入 commit message。`GOPROXY=https://goproxy.cn,direct`。
- 全部 commit message 以 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` 结尾。
- 每个实现类 task 内含「加关键节点日志」与「加注释」step（instrumenting-code）；`logger`/`slog`，禁 `fmt.Printf`。

## 写点归属总表（Task 6/7 的接线依据，来源：08-08 全量清点）

| 键族 | 归属组 | 写点（file:line） |
|---|---|---|
| `meta/topic` `meta/group` | MetaGroup | meta.go:186,214,268,287,304 |
| `meta/handle_secret` | MetaGroup | handle_secret.go:36（装配期，Task 6 特殊处理） |
| `msg/ alloc/ keyidx/` | GroupForQueue(topic,q) | produce.go:142,348；retention.go:159,193（purgeKeyIdx 按 key 内 queueID 分桶）；adminops.go:38（按队列拆） |
| `cursor/ inflight/` | GroupForQueue(**被消费的** topic,q) | deliver.go:377,489,555,607,677；adminops.go:55（按 key 解析分桶） |
| `delay/ delayalloc` | MetaGroup | produce.go:223；delay.go:105,114 |
| `half/ halfidx/` | MetaGroup | txn.go:113,153,162,274,348,360,384,394,414,175 |
| `metric/` | **本地直连 store，不复制** | series.go:362,398（Task 7 只加豁免注释） |

---

### Task 1: config 集群段

**Files:**
- Modify: `internal/config/config.go`
- Test: `internal/config/config_test.go`（已有文件，追加用例）

**Interfaces:**
- Produces: `Config.Cluster *ClusterConfig`（nil = 单机）；`type ClusterConfig struct { NodeID uint64; RaftListen string; DataGroups uint32; Ack string; Peers []ClusterPeer }`；`type ClusterPeer struct { ID uint64; RaftAddr string; AdvertiseHost string; AdvertisePort int }`；`func (c *Config) ClusterEnabled() bool`

- [ ] **Step 1: 写失败测试**

```go
func TestClusterConfigParsing(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "sq.yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := `
cluster:
  node_id: 2
  raft_listen: ":9081"
  peers:
    - id: 1
      raft_addr: "10.0.0.1:9081"
      advertise_host: "10.0.0.1"
      advertise_port: 8081
    - id: 2
      raft_addr: "10.0.0.2:9081"
      advertise_host: "10.0.0.2"
      advertise_port: 8081
    - id: 3
      raft_addr: "10.0.0.3:9081"
      advertise_host: "10.0.0.3"
      advertise_port: 8081
`
	cfg, err := config.Load(write(base))
	if err != nil {
		t.Fatalf("合法集群配置被拒: %v", err)
	}
	if !cfg.ClusterEnabled() || cfg.Cluster.NodeID != 2 {
		t.Fatalf("集群段解析错误: %+v", cfg.Cluster)
	}
	if cfg.Cluster.DataGroups != 3 || cfg.Cluster.Ack != "quorum-mem" {
		t.Fatalf("默认值错误: groups=%d ack=%q", cfg.Cluster.DataGroups, cfg.Cluster.Ack)
	}
	// 拒绝路径：node_id 不在 peers 里 / peers id 重复 / ack 非法 / 单机配置 Cluster==nil
	for name, body := range map[string]string{
		"node_id 不在 peers":  strings.Replace(base, "node_id: 2", "node_id: 9", 1),
		"peers id 重复":       strings.Replace(base, "- id: 3", "- id: 1", 1),
		"ack 非法":            base + "  ack: fsync-everything\n",
		"peers 少于 1":        "cluster:\n  node_id: 1\n  raft_listen: \":9081\"\n  peers: []\n",
	} {
		if _, err := config.Load(write(body)); err == nil {
			t.Errorf("%s: 应拒绝，实际通过", name)
		}
	}
	cfg2, err := config.Load("")
	if err != nil || cfg2.ClusterEnabled() {
		t.Fatalf("空配置应为单机: err=%v cluster=%v", err, cfg2.Cluster)
	}
}
```

- [ ] **Step 2: 跑测确认失败** — `go test ./internal/config/ -run TestClusterConfigParsing -v` → FAIL（字段不存在，编译失败）。

- [ ] **Step 3: 实现** — Config 增加 `Cluster *ClusterConfig \`yaml:"cluster"\``。校验追加在 Load 现有校验链尾部（保持「启动时挡笔误」的既有风格，每条报错带字段名与得到的值）：
  - `NodeID` 必须出现在 `Peers` 中；`Peers` 各 `ID` 非零且互不重复；`RaftAddr`/`AdvertiseHost` 非空、`AdvertisePort` ∈ (0,65535]；
  - `RaftListen` 非空；`DataGroups` 默认 3、范围 [1,64]（组数首启入盘不可变，上限防笔误）；
  - `Ack` 默认 `"quorum-mem"`，只接受 `quorum-mem|quorum-fsync`（spec §2.2 默认异步档）；
  - **peers 数允许 1**（单机→单节点集群升级形态）；偶数节点数打 Warn 日志不拒绝（raft 容忍，但 2 节点无容错价值——留给运维判断）。
  - `ClusterEnabled() bool { return c.Cluster != nil }`。
  - 增加辅助方法（Task 9/11 用）：`func (cc *ClusterConfig) PeerRaftAddrs() map[uint64]string`、`func (cc *ClusterConfig) AdvertiseOf(id uint64) (host string, port int, ok bool)`。

- [ ] **Step 4: 注释** — ClusterConfig 各字段行尾注释（对齐现有 Config 风格）；`Ack` 注释写明两档语义与默认值出处（spec §2.2）；`DataGroups` 注释写「首启持久化后不可变，改配置不改盘上事实（cluster.EnsureGroups 拒启）」。

- [ ] **Step 5: 跑测通过 + 全量回归** — `go test ./internal/config/ -v` → PASS；`go vet ./...` → 0。

- [ ] **Step 6: Commit** — `git add internal/config/ && git commit -m "feat(config): 集群配置段——node_id/raft_listen/peers/ack 档位（B8.2 batch③）"`（附 Co-Authored-By 行）。

---

### Task 2: cluster 基元——ErrNotLeader、OnLeaderChange/OnApplied 钩子

**Files:**
- Modify: `internal/cluster/manager.go`、`internal/cluster/group.go`
- Test: `internal/cluster/manager_test.go`、`internal/cluster/cluster_test.go`（追加）

**Interfaces:**
- Produces: `var ErrNotLeader = errors.New(...)`（cluster 包导出）；`Options.OnLeaderChange func(g uint32, leader uint64, isSelf bool)`；`Options.OnApplied func(g uint32, repr []byte)`；`Manager.Status(g uint32) (raft.Status, bool)`
- Consumes: batch② 的 `group.handleReady`（SoftState 分支、applyEntry 调用点）

- [ ] **Step 1: 写失败测试**

```go
// TestProposeOnFollowerReturnsErrNotLeader follower 上 Propose 必须报
// ErrNotLeader（可被 errors.Is 识别）——协议面据此翻译可重试码。
func TestProposeOnFollowerReturnsErrNotLeader(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lead := tc.leaderOf(t, 1)
	var follower uint64
	for id := range tc.mgrs {
		if id != lead {
			follower = id
			break
		}
	}
	err := tc.mgrs[follower].Propose(ctx, 1, []byte("x"))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower Propose 应返回 ErrNotLeader，得到: %v", err)
	}
}

// TestOnAppliedHookFires 每个节点 apply 提案后钩子必须携带组号与原始 repr 触发。
func TestOnAppliedHookFires(t *testing.T) { /* 三节点起时注入 OnApplied 收集器，
	leader 写一条 meta 组提案，断言 3 个节点都收到 (g=0, repr 相同) */ }
```

（OnApplied 收集器用 `chan struct{ g uint32; repr []byte }`，harness `newTestCluster` 增加可选 hook 注入参数——保持现有签名，加 `newTestClusterOpts` 变体。OnLeaderChange 测试同法：kill leader 后断言存活节点收到 isSelf=true 的回调。）

- [ ] **Step 2: 确认失败** — `go test -race ./internal/cluster/ -run 'ErrNotLeader|OnApplied' -v` → FAIL。

- [ ] **Step 3: 实现**
  - `ErrNotLeader`：`group.propose` / `proposeConfChange` 里，`rn.Propose` 返回 `raft.ErrProposalDropped`（DisableProposalForwarding 下 follower 提案的表现）时包装 `fmt.Errorf("%w: 组 %d 当前 leader=%d", ErrNotLeader, gr.g, lead)`；另在 propose 入口先查 `!gr.isLeader()` 快速失败（不等 raft 静默丢弃+超时——超时是客户端不可接受的失败模式）。等待 waiter 超时（ctx 超期）且期间 leader 已变更的，同样归入 ErrNotLeader（提案可能已丢）。
  - `OnLeaderChange`：`handleReady` 现有 SoftState 分支（group.go:206 附近日志处）追加回调；**注释写明钩子契约：不得阻塞（在 Ready 循环内），重活自行 dispatch**。
  - `OnApplied`：`applyEntry` 成功后回调 `(gr.g, data)`；同一契约注释。两钩子 nil 安全。
  - `Manager.Status(g)`：透传 `rn.Status()`（Task 10 学习者进度监控用）。

- [ ] **Step 4: 日志** — ErrNotLeader 快速失败路径 Debug 级（高频场景，客户端错发是常态不是异常）；钩子 panic 恢复不做——钩子由本仓库装配代码注入，panic 即 bug，按 apply-panic 同策略传播。

- [ ] **Step 5: 跑测 + 回归** — `go test -race ./internal/cluster/ -v -timeout 600s` → PASS。

- [ ] **Step 6: Commit** — `feat(cluster): ErrNotLeader 语义化 + OnLeaderChange/OnApplied 装配钩子（B8.2 batch③）`。

---

### Task 3: 控制通道——保留组号帧 + Manager.Control

**Files:**
- Modify: `internal/cluster/transport.go`、`internal/cluster/manager.go`
- Test: `internal/cluster/transport_test.go`、`internal/cluster/cluster_test.go`（追加）

**Interfaces:**
- Produces: `const ControlGroup uint32 = 0xFFFFFFFF`；`Options.ControlHandler func(op byte, payload []byte) ([]byte, error)`；`Manager.Control(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error)`；控制帧布局 `[4B BE 帧长][4B BE ControlGroup][1B op][payload]`，响应帧同构（op 最高位置 1 表响应，payload 首字节 0=成功 1=失败、失败时余下为 UTF-8 错误文本）
- Consumes: batch② transport 的信封帧与 16MiB 帧上限（复用，不另立编解码）

- [ ] **Step 1: 写失败测试**

```go
// TestControlRoundTrip 节点 A Control 调 B，B 的 handler 收到 op/payload
// 并返回应答；handler 报错时 A 收到带错误文本的 error。
func TestControlRoundTrip(t *testing.T) {
	// 双节点 harness：B 注入 ControlHandler(op, p) → 断言 op==7、回显 p；
	// A 调 m.Control(ctx, B, 7, []byte("ping")) → 得 "ping"。
	// 错误路径：handler 返回 error → A 侧 err 含该文本。
	// 超大 payload（>16MiB）→ 发送侧直接拒绝。
}
```

- [ ] **Step 2: 确认失败** — 编译失败（API 不存在）。

- [ ] **Step 3: 实现**
  - **请求/响应走独立短连接**，不复用 raft 消息流：raft 流是单向流水（batch② 设计），控制通道是低频 RPC（join、转发），一次 dial→写请求帧→读响应帧→close 的生命周期最简单，也不会与 raft 消息在同一 conn 上交织出队头阻塞。dial 超时/读写 deadline 均取 ctx。
  - 接收侧：`acceptLoop`/`readLoop` 里帧的 group == ControlGroup 时不走 deliver，改调 `t.control(op, payload)`，把返回写回**同一条连接**后关闭；handler 为 nil 时回错误帧「控制通道未装配」。
  - `Manager.Control`：查 `m.peers[nodeID]` 得地址、发起短连接；nodeID 不存在报错。
  - ControlGroup 与业务组号的碰撞不可能：组号上限 64（Task 1），保留值取 uint32 最大。

- [ ] **Step 4: 日志与注释** — 控制请求收发各一条 Debug（op、对端、payload 长度）；handler 错误 Warn 带 op 与错误。transport.go 文件头「职责」段补一行控制通道；帧布局注释写在 ControlGroup 常量处（对齐 batch② 信封帧注释风格）。

- [ ] **Step 5: 跑测 + 回归** — `go test -race ./internal/cluster/ -v -timeout 600s` → PASS。

- [ ] **Step 6: Commit** — `feat(cluster): 控制通道——保留组号帧与节点间短连接 RPC（B8.2 batch③）`。

---

### Task 4: Replicator 扩展——ApplyAsync/Pending 与两个转发原语

**Files:**
- Modify: `internal/replication/replication.go`
- Test: `internal/replication/replication_test.go`（新建）

**Interfaces:**
- Produces:

```go
// Pending 一次已定序、待确认的复制提交；Wait 语义与 store.Pending 一致。
type Pending interface{ Wait() error }

type Replicator interface {
	Apply(ctx context.Context, group uint32, b *store.Batch) error
	// ApplyAsync 定序返回、Wait 等确认——produce/deliver 热路径的锁外等待形态。
	// Standalone: store.ApplyAsync 直通（group commit 合并 fsync 的既有机制）。
	// Cluster: 提案发出即返回，Wait 阻塞到本节点 apply（quorum+本地）完成。
	ApplyAsync(ctx context.Context, group uint32, b *store.Batch) (Pending, error)
}

// Router 组路由视图。单机实现恒返回 0；集群实现转发 Manager。
type Router interface {
	GroupForQueue(topic string, queueID uint32) uint32
	MetaGroup() uint32
	IsLeader(g uint32) bool
}

// Forwarder 跨节点转发原语（仅 Cluster 后端实现；Standalone 上调用属编程错误，panic）。
type Forwarder interface {
	// ForwardAppend 把一条逻辑消息交给 g 组 leader 的 produce 栈追加
	//（offset 分配在 leader 侧发生——leader-only 构造不变量的跨节点延伸）。
	// 返回 leader 侧分配的 queueID/offset。自己就是 leader 时属编程错误（调用方先查 IsLeader）。
	ForwardAppend(ctx context.Context, g uint32, msgRaw []byte) (queueID uint32, offset uint64, err error)
	// ForwardApply 把构造无关批次（纯 Delete/DeleteRange/绝对值 Set，不含任何
	// 计数器分配）的 repr 转发给 g 组 leader 提案。带构造状态的批次禁用本方法。
	ForwardApply(ctx context.Context, g uint32, repr []byte) error
}
```

  - 控制通道 op 分配：`opForwardAppend=1`（payload=`[4B BE g][core.EncodeMessage 字节]`，响应 payload=`[4B BE queueID][8B BE offset]`）、`opForwardApply=2`（payload=`[4B BE g][repr]`）、`opPrepareJoin=3`（Task 10）。常量定义在 replication 包，`NewCluster` 注册 handler 的接线在 Task 11（main 装配时把 produce 栈闭包喂给 ControlHandler）。
- Consumes: Task 2 的 ErrNotLeader、Task 3 的 Manager.Control、store.ApplyAsync/Pending（store.go:180,205）、Manager.Propose/GroupForQueue/IsLeader/Leader

- [ ] **Step 1: 写失败测试** — Standalone 侧可直接单测：

```go
func TestStandaloneApplyAsync(t *testing.T) {
	st := openTestStore(t) // t.TempDir + store.Open(dir, false, logger)
	r := replication.NewStandalone(st)
	b := st.NewBatch()
	_ = b.Set([]byte("meta/topic/x"), []byte("v"))
	p, err := r.ApplyAsync(context.Background(), 0, b)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.Get([]byte("meta/topic/x")); !ok {
		t.Fatal("写入不可见")
	}
}
```

Cluster 侧 ApplyAsync/ForwardApply 的行为测试放 `internal/cluster/cluster_test.go` 追加（三节点 harness 在那边）：follower 上 `NewCluster(m).Apply` 返回 ErrNotLeader；`ForwardApply` 从 follower 发构造无关删除批次、经 leader 提案后三节点收敛；`ForwardAppend` 用假 handler 先测线路（真 produce 栈接线在 Task 11 e2e 覆盖）。

- [ ] **Step 2: 确认失败** — 编译失败。

- [ ] **Step 3: 实现**
  - `Standalone.ApplyAsync` → `store.ApplyAsync` 包一层（`store.Pending` 是值类型，包成接口即可）；`Standalone` 增加 `Router` 恒零实现 `type StandaloneRouter struct{}`（GroupForQueue/MetaGroup 恒 0，IsLeader 恒 true）。
  - `Cluster.ApplyAsync`：拷贝 repr、Close 批次，`ch := make(chan error, 1); go func() { ch <- c.m.Propose(ctx, group, repr) }(); return chanPending(ch), nil`。注释写明：集群档的「定序」由 raft 内部批量完成，这里的拆分只为把等待挪出调用方队列锁，与单机同形不同机制。
  - `Cluster` 实现 Router（转 Manager）与 Forwarder（`m.Leader(g)` 找 leader → `m.Control(ctx, leadID, op, payload)`；leader 未知时按 ErrNotLeader 报错让上层重试）。
  - `ClusterRouter`/错误透传边界更新文件头注释（现有「不做错误翻译」边界保留）。

- [ ] **Step 4: 日志** — ForwardAppend/ForwardApply 成功 Debug（g、目标节点、字节数）、失败 Warn 带上下文；Standalone 路径维持零日志（文件头已有豁免说明，续写一句涵盖 ApplyAsync）。

- [ ] **Step 5: 跑测 + 回归** — `go test -race ./internal/replication/ ./internal/cluster/ -timeout 600s` → PASS。

- [ ] **Step 6: Commit** — `feat(replication): ApplyAsync/Pending 与 ForwardAppend/ForwardApply 转发原语（B8.2 batch③）`。

---

### Task 5: 跨组两段式改造——AppendWith 废除 extra

**Files:**
- Modify: `internal/core/produce/produce.go`（AppendWith 签名）、`internal/core/delay/delay.go`（Pass）、`internal/core/deliver/deliver.go`（moveToDLQ、ForwardToDLQ 孤儿分支不动）、`internal/core/txn/txn.go`（End）
- Test: 各包既有测试改造 + 新增幂等重放用例

**Interfaces:**
- Produces: `func (p *Producer) Append(m *core.Message) (*core.Message, error)`（AppendWith 更名收编，**去掉 extra 参数**——本批之后不存在跨语义域单批原子）
- Consumes: 无新依赖——本 task 是纯单机语义改造，store 直连不变，为 Task 6/7 接线扫清结构障碍

**背景（为什么必须改）**：`extra` 的三个使用者（delay.go:114、deliver.go:653、txn.go:175）写的键全部落在与主批次不同的 raft 组，单批原子在集群下不可表达。改为两段式：**第一段写目标（消息追加），第二段删来源（delay 条目 / 源 inflight / half 两键）**。次序保证崩溃只产生重复（重放幂等），不产生丢失。

- [ ] **Step 1: 写失败测试**（每个调用方一条「崩溃窗口重放」用例，以注入故障模拟两段间中断）：

```go
// TestDelayMoveRedeliversOnCrashBetweenPhases 两段之间崩溃后重放：
// 目标消息已在、delay 条目也在 → 下一趟重搬 → 队列出现两条（at-least-once
// 允许的重复），但绝不出现「条目没了消息也没了」。
func TestDelayMoveRedeliversOnCrashBetweenPhases(t *testing.T) {
	// 构造一条到期 delay 条目 → 手工执行第一段（pr.Append）成功后直接返回
	//（不执行第二段）→ 再跑完整 s.Pass() → 断言 msg/ 有两条、delay/ 空。
	// 实现侧给 Scheduler 加测试钩子 afterAppendHook func()（仅测试注入，
	// 生产 nil），钩子内 panic/return 模拟中断。
}
// TestTxnCommitRedelivers / TestDLQMoveRedelivers 同构（txn End 与 moveToDLQ）。
```

- [ ] **Step 2: 确认失败** — 钩子与两段式尚不存在，编译失败。

- [ ] **Step 3: 实现**
  - `AppendWith(m, extra)` → `Append(m)`：删掉 extra 参数与 produce.go:139-141 的注入点；原有导出名 `Append` 若已是 AppendWith 的别名则合并（以仓库实际为准，保持唯一导出追加入口）；AppendDelay 直通分支（produce.go:201）随签名同步。
  - `delay.Pass`（delay.go:110-118）：`s.pr.Append(m)` 成功后，**第二段独立批次**删 `d.key`；第二段失败只记 Error 不回滚第一段（消息已入队是既成事实，条目残留 = 下趟重搬 = 重复，可接受；日志必须把两段坐标都带上）。
  - `deliver.moveToDLQ`（deliver.go:653）：先 `d.pr.Append(dlq)`，后独立批次删 `infKey`；注释写明重放窗口产生重复死信条目的语义与为何可接受。
  - `txn.End` commit 分支（txn.go:175-178）：先 `t.pr.Append(m)`，后独立批次删 `halfKey`+`idxKey`；重放 = 重复提交 = 重复消息，at-least-once 允许；**End 必须先读后删的既有幂等判定（halfKey 不存在即已决断）天然挡住二次 EndTransaction**，注释点明这层已有保护。
  - 三处原「同一 Batch 原子提交：不存在丢失或重复」的注释**必须改写**——那句话在两段式下已不成立，新注释写「先写后删；崩溃窗口重放=重复，at-least-once 语义内；次序不得反转（反转=丢失）」。

- [ ] **Step 4: 日志** — 三处第二段失败的 Error 日志带全部坐标（msg_id、来源键、目标 topic/queue/offset）；第二段成功不新增日志（第一段已有 Info/Debug）。

- [ ] **Step 5: 跑测 + 回归** — `go test -race ./internal/... -timeout 600s` → PASS（既有 delay/txn/DLQ 用例断言的最终状态不变——两段式在无故障路径上与单批等价）。

- [ ] **Step 6: Commit** — `refactor(core): 跨语义域写拆两段式——先写目标后删来源，重放幂等（B8.2 batch③）`。

---

### Task 6: 写点接线 A——meta、produce、handle secret

**Files:**
- Modify: `internal/core/meta/meta.go`、`internal/core/produce/produce.go`、`internal/rpc/handle_secret.go`、全部受构造函数签名影响的调用点（main.go、admin、rpc、各测试）
- Test: 各包既有测试（构造函数改注入）+ meta 缓存重载新用例

**Interfaces:**
- Produces:

```go
// meta
func New(rep replication.Replicator, rt replication.Router, st *store.Store,
	autoCreate bool, defaultQueues uint32, defaultMaxAttempts int32, logger *slog.Logger) (*Meta, error)
func (m *Meta) Reload() error // 丢弃内存缓存、从 store 全量重读（OnApplied 钩子触发）

// produce
func New(rep replication.Replicator, rt replication.Router, st *store.Store,
	mt *meta.Meta, logger *slog.Logger) *Producer
func (p *Producer) InvalidateCounters() // 全部 qstates.loaded=false、delayLoaded=false（leader 变更触发）

// rpc（集群档装配路径）
func LoadOrCreateHandleSecretReplicated(ctx context.Context, st *store.Store,
	rep replication.Replicator, rt replication.Router, logger *slog.Logger) ([]byte, error)

// store（meta 缓存钩子的判定件）
func BatchTouchesPrefix(repr []byte, prefix []byte) (bool, error) // pebble.BatchReader 遍历 repr 的键
```

- Consumes: Task 4 的 Replicator/Router、Task 2 的 OnApplied/OnLeaderChange（接线在 Task 11 main 装配，此处只提供方法）

- [ ] **Step 1: 写失败测试**

```go
// store 侧
func TestBatchTouchesPrefix(t *testing.T) {
	st := openTestStore(t)
	b := st.NewBatch()
	_ = b.Set([]byte("msg/t/0/1"), []byte("v"))
	_ = b.Set([]byte("meta/topic/t"), []byte("v"))
	repr := append([]byte(nil), b.Repr()...)
	_ = b.Close()
	if ok, _ := store.BatchTouchesPrefix(repr, []byte("meta/")); !ok {
		t.Fatal("应命中 meta/ 前缀")
	}
	if ok, _ := store.BatchTouchesPrefix(repr, []byte("half/")); ok {
		t.Fatal("不应命中 half/")
	}
}

// meta 侧：绕过缓存直写 store 模拟 follower 盲 apply，Reload 后读到新 topic
func TestMetaReloadPicksUpBlindApply(t *testing.T) {
	st := openTestStore(t)
	mt, _ := meta.New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, false, 4, 16, testLogger(t))
	// 模拟另一节点的盲 apply：直接 store 写一条 topic 配置（编码用 meta 包的导出编码或 JSON，以实际为准）
	writeTopicRaw(t, st, "ghost", 4)
	if _, err := mt.Topic("ghost"); err == nil {
		t.Fatal("Reload 前不应可见（缓存未失效）")
	}
	if err := mt.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, err := mt.Topic("ghost"); err != nil {
		t.Fatalf("Reload 后应可见: %v", err)
	}
}
```

- [ ] **Step 2: 确认失败** — 编译失败。

- [ ] **Step 3: 实现**
  - `store.BatchTouchesPrefix`：`r, _ := pebble.ReadBatch(repr)`（以 pebble v2.1.6 实际 API 为准——spike 已确认 BatchReader 存在；逐条 `r.Next()` 取 key 判前缀）。放 `internal/store/store.go`，键遍历不解码 value。
  - meta 五个写方法（meta.go:186,214,268,287,304）：`m.st.Apply(b)` → `m.rep.Apply(ctx, m.rt.MetaGroup(), b)`；方法签名補 `ctx context.Context` 参数（一路上抛到 rpc/admin handler——它们本就有 ctx）；缓存更新逻辑不动（leader 路径立即可见；follower 靠 Reload）。`Reload()`：持写锁重扫 `meta/topic/`、`meta/group/` 前缀重建两个 map（New 里已有加载逻辑，抽成共用私有方法）。
  - produce 三个写点（produce.go:142,223,348）：`p.st.ApplyAsync(b)` → `p.rep.ApplyAsync(ctx, g, b)`，其中 Append/AppendBatch 的 `g = p.rt.GroupForQueue(m.Topic, m.QueueID)`（**段 2 锁内、offset 分配后**计算——QueueID 此时已定）、AppendDelay 的 `g = p.rt.MetaGroup()`。`InvalidateCounters()`：`p.mu` + 逐 `qs.mu` 置 `loaded=false`，`delayMu` 内置 `delayLoaded=false`。**注释必须写透为什么**：offset 计数器只在组 leader 上权威，失而复得的 leader 若沿用旧内存值会重复分配/跳号——重新当选后第一笔写强制从 `alloc/` 重读（复制保证它是多数派事实）。
  - `LoadOrCreateHandleSecretReplicated`：读 `meta/handle_secret`（已复制则三节点同值）；缺失时——`rt.IsLeader(MetaGroup)` 则生成并 `rep.Apply` 写入；非 leader 则 200ms 间隔轮询重读直到出现或 ctx 超时（leader 会写，复制会送达）。原 `LoadOrCreateHandleSecret` 保持原样（单机路径）。
  - 所有调用点机械随动（main.go:73,77、admin/rpc 测试构造）：单机装配传 `replication.NewStandalone(st)` + `replication.StandaloneRouter{}`。

- [ ] **Step 4: 日志** — Reload 完成 Info（topic/group 计数）；InvalidateCounters 调用 Info（「leader 变更，offset 缓存失效」）；secret 轮询等待每 5s Info 一次（等 leader 写入是正常慢路径，静默会像卡死）。

- [ ] **Step 5: 注释** — meta.go 文件头「边界」补：缓存一致性契约（leader 写穿透即时可见；follower 靠 OnApplied→Reload，装配见 main）；produce.go 段 2 注释补组号计算时机说明。

- [ ] **Step 6: 跑测 + 回归** — `go test -race ./internal/... -timeout 600s` → PASS。

- [ ] **Step 7: Commit** — `feat(core): meta/produce 接 Replicator——组路由、缓存重载、计数器失效、secret 复制装载（B8.2 batch③）`。

---

### Task 7: 写点接线 B——deliver、txn、delay、retention、adminops、sampler 豁免

**Files:**
- Modify: `internal/core/deliver/deliver.go`、`internal/core/txn/txn.go`、`internal/core/delay/delay.go`、`internal/core/retention/retention.go`、`internal/core/adminops/adminops.go`、`internal/metrics/series.go`（仅注释）、调用点随动
- Test: 各包既有测试改造

**Interfaces:**
- Produces（构造函数统一前置注入，与 Task 6 同形）:

```go
func deliver.New(rep replication.Replicator, rt replication.Router, st *store.Store, mt *meta.Meta, pr *produce.Producer, logger *slog.Logger) *Deliverer
func txn.New(rep replication.Replicator, rt replication.Router, st *store.Store, pr *produce.Producer, mt *meta.Meta, checkInterval time.Duration, maxChecks int, logger *slog.Logger) *Manager
func delay.New(rep replication.Replicator, rt replication.Router, st *store.Store, pr *produce.Producer, logger *slog.Logger) *Scheduler
func retention.New(rep replication.Replicator, rt replication.Router, st *store.Store, mt *meta.Meta, interval time.Duration, dataDir string, watermarkPct int, writeBlocked *atomic.Bool, logger *slog.Logger) *Manager
func adminops.PurgeTopicData(ctx context.Context, rep replication.Replicator, rt replication.Router, fwd replication.Forwarder, st *store.Store, tc meta.TopicConfig, logger *slog.Logger) error
func adminops.PurgeGroupData(ctx context.Context, rep replication.Replicator, rt replication.Router, fwd replication.Forwarder, st *store.Store, group string, logger *slog.Logger) error
```

（fwd 在单机装配传 nil——单机 IsLeader 恒 true，永远走本地 Apply，不会解引用。）
- Consumes: Task 4 全部接口、Task 5 的两段式结构

- [ ] **Step 1: 接线清单逐点改造**（每点：换 Apply/ApplyAsync 调用 + 算组号；无行为变化的既有测试保持绿即是回归判据）：

| 写点 | 组号 | 改法 |
|---|---|---|
| deliver.go:377 receiveOnceLocked | `rt.GroupForQueue(topic, queueID)` | `st.ApplyAsync` → `rep.ApplyAsync`（staged 判据不动——它是「避免空提案洪水」的唯一屏障，注释点名） |
| deliver.go:489 AckBatch | 同上 | 同上 |
| deliver.go:555 changeInvisibleLocked | 同上 | 同上 |
| deliver.go:607 孤儿 inflight 清理 | 同上 | `st.Apply` → `rep.Apply` |
| deliver.go:677 ResetCursor | 同上 | 同上 |
| txn.go 全部 9 点 | `rt.MetaGroup()` | `st.Apply` → `rep.Apply`；**End 的 half 删除段**：非 meta leader 时走 `fwd.ForwardApply`（构造无关：两个绝对键 Delete）——EndTransaction 可能落在任意节点，消息追加段同样：目标组非本节点 leader 时 `fwd.ForwardAppend` |
| delay.go:105 坏条目删 | `rt.MetaGroup()` | `rep.Apply` |
| delay.go 第二段删条目（Task 5 产物） | `rt.MetaGroup()` | `rep.Apply`（调度器只在 meta leader 跑——Task 8，本节点必是 leader） |
| delay 第一段消息追加 | 目标组 | `rt.IsLeader(g)` 则本地 `pr.Append`；否则 `fwd.ForwardAppend`（Append 返回坐标用于日志） |
| retention.go:159 purgeQueue | `rt.GroupForQueue(topic, q)` | `rep.Apply`；**调用前查 `rt.IsLeader(g)`，非 leader 跳过该队列**（各组 leader 各扫各的，Task 8 统一说明） |
| retention.go:193 purgeKeyIdx | 按 key 解析 queueID 分桶 | 每桶一个批次 `rep.Apply(ctx, GroupForQueue(topic, qid), b)`，只处理本节点 lead 的桶；`store` 补导出 `ParseKeyIdxQueueID(k []byte) (uint32, error)`（keys.go 已有布局知识，解析放它家） |
| adminops.go:38 PurgeTopicData | 按队列拆 | 逐队列一个批次（该队列 msg/ DeleteRange + alloc/ Delete）→ lead 则 `rep.Apply`、否则 `fwd.ForwardApply`；keyidx 段按 Task 7 同款分桶 |
| adminops.go:55 PurgeGroupData | 按 key 解析分桶 | 先 Scan `cursor/{group}/` 与 `inflight/{group}/` 收集 (topic,qid) 集合，逐 (topic,qid) 一个批次（两个前缀的 DeleteRange）→ lead 本地 / 非 lead ForwardApply；`store` 补 `ParseCursorTopicQueue(k)` 同款导出 |
| series.go:362,398 | 不复制 | **只加注释**：metric/ 是本节点可观测数据，刻意绕过 Replicator 直连 store——三节点各采各的，复制会同键互覆；快照追齐（batch④）可能混入他节点历史点，可观测数据可接受 |

- [ ] **Step 2: 新增测试** — `TestPurgeGroupDataBucketsByQueue`（多 topic 多队列造 cursor/inflight，purge 后全空——单机 Router 恒零下即验证分桶枚举完整性）；`TestParseKeyIdxQueueID` / `TestParseCursorTopicQueue` 往返用例（构造 key → 解析 → 相等）。

- [ ] **Step 3: 跑测 + 回归** — `go test -race ./internal/... -timeout 600s` → PASS；`grep -rn "st.Apply\|st.ApplyAsync" internal/core/ internal/metrics/ --include="*.go" | grep -v _test` 只剩 series.go 两处（豁免留痕，作为「写点全接线」的机械验证）。

- [ ] **Step 4: 日志** — ForwardAppend/ForwardApply 分支各带 Info（跨节点转发是罕见慢路径，必须可见：g、目标 leader、坐标）；分桶 purge 完成 Info 带桶数与条数。

- [ ] **Step 5: 注释** — adminops.go 文件头重写职责/边界（跨组拆批语义：批间无原子性，崩溃残留由重复执行清净——purge 幂等）；deliver.go/txn.go 文件头补组归属一句。

- [ ] **Step 6: Commit** — `feat(core): 全部写点接 Replicator——按组分桶、跨节点转发、metric 本地豁免（B8.2 batch③）`。

---

### Task 8: leader-only 定时器门 + leader 摊布

**Files:**
- Modify: `internal/core/retention/retention.go`、`internal/core/delay/delay.go`、`internal/core/txn/txn.go`（Run 循环门控）、`internal/cluster/manager.go`（摊布循环）
- Test: `internal/cluster/cluster_test.go`（摊布收敛）、core 各包门控单测

**Interfaces:**
- Produces: `Manager.StartLeaderBalancer(interval time.Duration)`（Start 内按 Options 起，测试可注小间隔）；core 侧无新导出——门控用已注入的 `rt.IsLeader`
- Consumes: Task 2 的 Manager.Status、TransferLeader（batch② 已有）、sortedPeerIDs

- [ ] **Step 1: 写失败测试**

```go
// TestLeaderSpreadConverges 三节点四组，任由初始选举集中，摊布循环应在
// 数个周期内把 leader 分布收敛到 preferred（组 g → sortedPeers[g % n]）。
func TestLeaderSpreadConverges(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem) // harness 注入 balancer 间隔 200ms
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if tc.leadersMatchPreferred(t) { return }
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("leader 摊布未收敛到 preferred 分布")
}
```

core 侧门控单测：给 delay.Scheduler 注入恒 false 的 IsLeader → `Pass` 有到期条目也不搬（返回 0）；恒 true → 照搬。retention 同款（非 lead 队列跳过）。

- [ ] **Step 2: 确认失败**。

- [ ] **Step 3: 实现**
  - **摊布策略**：preferred leader of group g = `sortedPeerIDs(peers)[g % len(peers)]`（确定性、无协调收敛）。Manager 内循环：每 interval 遍历本节点 lead 的组，若 preferred ≠ self 且 preferred 在当前 ConfState voters 中且其 Progress.RecentActive（Status(g) 取）→ `TransferLeader(g, preferred)`。**只有当前 leader 有权转移**，故无并发冲突；preferred 挂掉时留在现任（RecentActive 判据挡住向死节点转移）；preferred 恢复后自动回迁。
  - **门控**：delay.Run/txn.RunChecker 每趟开头 `if !s.rt.IsLeader(s.rt.MetaGroup()) { 等 tick 继续 }`（不退出循环——leader 可能随时轮到自己）；retention.Run 的 checkDisk 保持无条件执行（本地磁盘水位是本节点事实），purge 循环内逐队列判 lead（Task 7 已实现，这里只补 Run 层「本趟 0 队列可处理」的 Debug 日志）。单机注入 StandaloneRouter → IsLeader 恒 true，行为不变。
  - InvalidateCounters 接线预留：OnLeaderChange(g, leader, isSelf) 里 isSelf==true 时调 produce.InvalidateCounters——**装配在 Task 11**，本 task 在 cluster_test 里用钩子收集器断言时机正确（获得任意组 leadership 时触发一次）。

- [ ] **Step 4: 日志** — TransferLeader 发起 Info（g、from、to、原因=「摊布」）；门控跳过为 Debug（每趟都发生，Info 会刷屏）；获得/失去 meta leadership 时 delay/txn 调度器各打一条 Info（「本节点开始/停止承担 delay 调度」）——运维定位「延时消息为什么不动了」的第一线索。

- [ ] **Step 5: 跑测 + 回归** — `go test -race ./internal/cluster/ ./internal/core/... -timeout 600s` → PASS。

- [ ] **Step 6: Commit** — `feat(cluster): leader-only 定时器门与确定性 leader 摊布（B8.2 batch③）`。

---

### Task 9: 协议面——QueryRoute 指向组 leader、HA_NOT_AVAILABLE、Receive 快速失败

**Files:**
- Modify: `internal/rpc/server.go`（endpoints/messageQueues/brokerName）、`internal/rpc/send.go`、`internal/rpc/receive.go`、`internal/rpc/forward.go`、`internal/rpc/txn.go`（错误映射）、`internal/rpc/settings.go`（集群档退避）
- Test: `internal/rpc/*_test.go` 追加

**Interfaces:**
- Produces: `type RouteView interface { QueueEndpoint(topic string, queueID uint32) (host string, port int32, brokerName string, ok bool); SelfIsLeader(topic string, queueID uint32) bool; MetaIsLeader() bool }`；`rpc.New` 增参 `rv RouteView`（单机实现 `staticRouteView{cfg}` 恒返回 advertise 地址 + `sq0` + true）
- Consumes: Task 4 Router、cluster.ErrNotLeader、Manager.Leader、config.AdvertiseOf

- [ ] **Step 1: 写失败测试**（裸 protobuf stub 风格，与既有 rpc 测试同构）：

```go
// TestQueryRoutePointsQueuesAtGroupLeaders 注入假 RouteView：队列 0→节点A、
// 队列 1→节点B → 断言响应里两条 MessageQueue 的 broker.endpoints 各指其主，
// broker.Name 分别为 sq1/sq2。
// TestSendOnNonLeaderReturnsHANotAvailable 假 RouteView SelfIsLeader=false +
// deliver 注入 ErrNotLeader → SendMessage 状态码 == Code_HA_NOT_AVAILABLE。
// TestReceiveOnNonLeaderFailsFastWithoutLongPoll follower 上 ReceiveMessage
// 必须立即返回 HA_NOT_AVAILABLE，耗时 << 长轮询时长（断言 <1s）。
```

- [ ] **Step 2: 确认失败**。

- [ ] **Step 3: 实现**
  - `messageQueues`（server.go:156）：每条队列 `rv.QueueEndpoint(topic, i)` 得 (host, port, brokerName)；leader 未知（选举窗口）→ 整个 QueryRoute 返回 `errStatus(pb.Code_HA_NOT_AVAILABLE, ...)`——SDK 重试会换节点问。`brokerName` 常量（server.go:31）改经 RouteView 提供：集群档 `"sq"+strconv.FormatUint(nodeID,10)`，单机保持 `sq0`。QueryAssignment（receive.go:451）复用后自动获得同行为。
  - **错误映射**：`topicErrStatus`（server.go:129，唯一分类映射器）追加分支 `errors.Is(err, cluster.ErrNotLeader) → pb.Code_HA_NOT_AVAILABLE`——但 rpc 不 import cluster（依赖方向）：replication 包导出 `var ErrNotLeader = cluster.ErrNotLeader` 转发（replication 已依赖 cluster），rpc 只认 replication。Ack/ChangeInvisible/Forward/EndTransaction handler 的内联 `INTERNAL_SERVER_ERROR` 分支前同样插入该判定（receive.go:378,440、forward.go:29、txn.go:48）。
  - **Receive 入口快速失败**：receive.go:54 入口处 `if !s.rv.SelfIsLeader(topic, qid) { return HA_NOT_AVAILABLE }`——否则 follower 会安静长轮询 20s 返回 MESSAGE_NOT_FOUND，消费者停在死路由上毫无线索。SendMessage 同款前置（快速失败优于靠 propose 报错，省一次批次构造）。
  - **HA_NOT_AVAILABLE 选码依据**（注释写进 errStatus 调用处）：50002 语义即「高可用复制层不可用」；实测 SDK 对任意非 OK 码立即重试 + 隔离端点 + 轮换队列（producer.go:214 起），3 次尝试跨 3 个候选队列足以撞上健康 leader；MESSAGE_NOT_FOUND/TOO_MANY_REQUESTS 有特殊分支不可占用。
  - settings.go：集群档 `backoffMax` 提到 3s、`backoffMaxAttempts` 提到 5（选举窗口实测 1.5s 量级，默认 1s×3 次刚好可能全落在窗口内）；单机保持原值。`rpc.New` 已拿 cfg，按 `cfg.ClusterEnabled()` 分支。

- [ ] **Step 4: 日志** — QueryRoute 因 leader 未知拒答 Warn（选举窗口的正常现象但要可查）；ErrNotLeader 映射 Debug（高频）；入口快速失败 Debug。

- [ ] **Step 5: 注释** — server.go:121-125 现有 INTERNAL_SERVER_ERROR 语义论证旁补 HA_NOT_AVAILABLE 的分工（「服务端坏了」vs「问错节点了」）。

- [ ] **Step 6: 跑测 + 回归** — `go test -race ./internal/rpc/ -v` → PASS；全量回归。

- [ ] **Step 7: Commit** — `feat(rpc): 路由指向组 leader + HA_NOT_AVAILABLE 可重试面 + 集群档退避（B8.2 batch③)`。

---

### Task 10: learner 重入自动编排

**Files:**
- Modify: `internal/cluster/manager.go`（PrepareJoin handler、自动升 voter 循环、Rejoin 入口）
- Test: `internal/cluster/cluster_test.go`（自动编排版重入测试）

**Interfaces:**
- Produces: `func Rejoin(ctx context.Context, o Options, dataDir string) (*Manager, error)`（包级：关店清目录→PrepareJoin 全组→fresh 启动）；`opPrepareJoin=3` 控制协议（payload=`[8B BE nodeID]`，响应=该节点完成 Remove→AddLearner 的组号列表 `[4B BE n][n × 4B BE g]`）；`Options.AutoPromoteLearners bool`
- Consumes: batch② WipeForRejoin、ProposeConfChange、cluster_test.go:389 `rejoinAsLearner` 的六步行为规范（**编排语义不得偏离**——测试注释原文）、Task 3 Control

- [ ] **Step 1: 写失败测试** — 把 `TestClusterUncleanNodeRejoinsAsLearner` 复制为自动版：

```go
// TestClusterAutoRejoin 断电节点调用 cluster.Rejoin 一个入口完成全部编排：
// 存活 leader 收 PrepareJoin 自动 Remove→AddLearner；节点 fresh 启动追平后
// 由 AutoPromoteLearners 循环自动升 voter；最终三节点数据收敛且 victim 是
// 全组 voter。原手工编排测试保留（行为规范不动）。
func TestClusterAutoRejoin(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem)
	// 写 100 → kill victim（不写标记）→ 写 100 → m := cluster.Rejoin(ctx, opts, dir)
	// → waitConverged 全键 → 轮询 Status 断言 victim 在每组 ConfState.Voters
}
```

- [ ] **Step 2: 确认失败**。

- [ ] **Step 3: 实现**
  - **PrepareJoin handler**（ControlHandler 内、Manager 自装）：收到 `[nodeID]`，对**本节点当前 lead 的每个组**顺序执行 `ProposeConfChange(Remove, id)`（id 不在成员表时 raft 幂等报错，忽略继续）→ `ProposeConfChange(AddLearner, id)`；返回完成的组号列表。非 leader 的组不动——由请求方轮询全部 peers 收齐并集。
  - **Rejoin 编排**（对应 harness 六步）：① store 已关（调用方职责，签名注释写明）② `WipeForRejoin(dataDir)` ③ 轮询全部 peers 发 PrepareJoin，收齐 0..DataGroups 全组完成（30s 超时，期间 leader 可能换手——重发即可，Remove/AddLearner 幂等）④ 重建 Listener（harness 的 EADDRINUSE 抢占逻辑照搬——注释同款）⑤ `store.Open` + `NewManager`（fresh 路径断言）+ `Start` ⑥ 追平与升 voter 交给 leader 侧自动循环。
  - **自动升 voter**（AutoPromoteLearners=true 时 Start 起循环，与摊布循环共节拍）：本节点 lead 的组里，Status(g).Progress 中 IsLearner 且 `Match == 本组 lastIndex` 的节点 → `ProposeConfChange(AddNode, id)`。「无新提案时位点即锚点」（harness 步骤 5 语义）：Match 追上当下 lastIndex 即达标，其后新提案 learner 照常收——不追求绝对静止点。
  - **启动自愈入口**：`NewManager` 返回 `ErrUncleanShutdown` 时的 wipe+Rejoin 决策**留给 main（Task 11）**——cluster 包不擅自清目录（清空是破坏性动作，调用方要先打日志留痕）。

- [ ] **Step 4: 日志** — Rejoin 六步每步 Info（对齐 harness 的 t.Logf 粒度：目录、组号、对端、耗时）；PrepareJoin handler 收到请求 Info（谁要重入）；升 voter Info（g、node、match）。这是 failover 事故复盘的主证据链，宁多勿少。

- [ ] **Step 5: 注释** — Rejoin 函数注释完整复述六步与幂等性论证（任一步失败可整体重跑：Wipe 幂等、PrepareJoin 幂等、fresh 启动可重试）；AutoPromoteLearners 注释写明与手工 ProposeConfChange 并存不冲突（raft 成员变更天然串行）。

- [ ] **Step 6: 跑测** — `go test -race ./internal/cluster/ -run 'AutoRejoin|Rejoin' -v -timeout 600s` → PASS；`-count=3` 稳定性。

- [ ] **Step 7: Commit** — `feat(cluster): learner 重入自动编排——PrepareJoin 协议与自动升 voter（B8.2 batch③）`。

---

### Task 11: main 装配、单机→单节点集群升级、三节点 e2e、收尾

**Files:**
- Modify: `cmd/sq/main.go`、`internal/rpc/server.go`（RouteView 集群实现放 main 侧新文件 `cmd/sq/clusterview.go` 或 internal/rpc 内——以依赖方向顺者为准）、`test/e2e/`（新增 `sdk_cluster_test.go`）、`docs/superpowers/backlog.md`、`sq.example.yaml`
- Test: e2e + 升级用例

**Interfaces:**
- Consumes: 前十个 task 的全部产出。装配序（关键，注释进 main）：

```
config → store.Open → [集群档] cluster.NewManager（含日志回放；ErrUncleanShutdown
→ 打 Error 日志 → st.Close → cluster.Rejoin 自愈）→ Manager.Start → 等 meta 组
出 leader（有超时）→ rep/rt/fwd 构造 → meta.New（此时 FSM 完整）→ produce/deliver/
txn/delay/retention 注入 → secret（集群档走 Replicated 变体）→ rpc.New（RouteView）
→ OnApplied/OnLeaderChange 钩子闭包接线（meta.Reload / produce.InvalidateCounters
——闭包内 go 出去，钩子契约不阻塞）→ ControlHandler 注册（ForwardAppend 落
pr.Append、ForwardApply 落 rep.Apply）→ 其余照旧。停机：gRPC 排空 → 定时器退出
→ Manager.StopClean → st.Close（defer LIFO 序对齐现有注释风格）
```

- [ ] **Step 1: 单机回归防线先行** — 全量 `go test -race ./...` + `go test -tags e2e ./test/e2e/ -run TestOfficialGoSDK -v` 单机 e2e 全绿后再动 main（接线改动全部就位、装配是最后一块）。

- [ ] **Step 2: main 装配实现** — 按上述装配序；`cfg.ClusterEnabled()==false` 时一行不多跑（Standalone + StandaloneRouter + staticRouteView，现路径）。ControlHandler 的 ForwardAppend 分支：解码消息 → `pr.Append`（本节点必为目标组 leader——发起方按 Leader(g) 寻址；错发时 Append 内的 propose 自然报 ErrNotLeader 回传）。「等 meta 组出 leader」超时 60s，超时报错退出（起不来比半残强）。

- [ ] **Step 3: 升级用例** — `test/e2e/sdk_cluster_test.go`：

```go
// TestStandaloneToSingleNodeClusterUpgrade 单机跑一段（写 topic + 消息）→
// 停 → 同一数据目录加 cluster 段（peers 只有自己）重启 → SDK 能查路由、
// 收到升级前的消息、继续收发。断言 raft/ 前缀已出现（升级确实入了 raft）。
// TestThreeNodeClusterE2E 三节点全新起（三个 data dir、三份配置、同一
// credentials）→ SDK endpoints 填三地址 → 建 topic（default_queue_nums=6，
// 保证三组都有队列）→ 发 200 条 → SimpleConsumer 全收全 ack → kill 当前
// 某数据组 leader 进程 → 继续发收（SDK 重试 + 路由刷新自愈，允许重复不允许
// 丢）→ 重入：以 -rejoin 方式重启被 kill 节点（见 Step 4）→ 三节点对账。
// 进程管理复用 sdk_test.go 的现编二进制 harness（:157,267），端口按节点错开。
```

- [ ] **Step 4: 不干净重启自愈入口** — main 里 `cluster.NewManager` 返回 `ErrUncleanShutdown` → Error 日志（打出「即将清空数据目录以 learner 重入」+ 目录路径）→ `st.Close()` → `cluster.Rejoin(...)`。**无人值守自愈是集群模式默认行为**——拒启等人工介入违背高可用初衷；日志留痕是破坏性动作的补偿。sq.example.yaml 补集群段样例与该行为的说明注释。

- [ ] **Step 5: Linux 验证** — `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -tags e2e -o /tmp/e2e.test ./test/e2e/` 需要 broker 二进制：交叉编译 `cmd/sq` 一并 scp（harness 二进制路径支持环境变量覆盖，缺则补一个 `SQ_E2E_BROKER` 读取）；100.90.99.61 跑 cluster 用例 + `internal/cluster` 全量；结果记 commit message。内存峰值：三节点 e2e 期间用 harness 记录各 broker 进程 RSS 峰值（读 /proc/<pid>/status VmHWM），写进测试日志与 commit message（全局约束：性能类验证必记内存峰值）。

- [ ] **Step 6: 收尾自检**（verification-before-completion + instrumenting-code 清单） — `go vet ./...`→0；`go test -race ./...`→0；e2e 单机+集群全绿；grep 写点豁免仅 series.go 两处；新文件头注释、导出注释、错误分支上下文、成功路径不静默逐项过。

- [ ] **Step 7: backlog 与交棒** — backlog B8.2 变更痕迹补 batch③ 完成行；**顺手清掉已被 R4 超越的「持久化失败停摆可观测化」入口项**（终审复审确认过时）；batch④ 入口项落明：快照 + 日志截断、单机→多节点扩容（快照依赖）、B8.3 集群场景测试（kill -9 / 分区 / 滚动重启 / 断电）、admin 控制台集群视图与 follower 写转发 UX、ConfChange applied 键缺口、首轮 leader 日志 term=0、rd.MustSync、clean 标记+空日志边界、confStateFromEntries 多变更守卫、TestProposalWaiterScopedToProposer goroutine 泄漏。

- [ ] **Step 8: Commit** — `feat(cmd): 集群装配、单机升级路径与三节点 e2e（B8.2 batch③）` + `docs(backlog): B8.2 batch③ 完成痕迹与 batch④ 入口项`。

---

## Self-Review 记录

- **Spec 覆盖**：§3 组拓扑（摊布=Task 8、QueryRoute 任意节点可答=Task 9）；§4 唯一拦截点（33 写点全接=Task 6/7）、leader-only 定时器（Task 8）、跨组预检-逐组提交语义（Task 5 两段式，方向收紧为先写后删）；§6 客户端兼容（Task 9：路由指向 leader、可重试码、退避调参）；§7 升级路径（Task 11 单节点形态；多节点扩容显式外推 batch④——spec 原文「靠快照追齐」本就依赖 batch④ 快照）；§8 场景测试属 batch④（B8.3）。凭据复制的偏离已记 Global Constraints。
- **占位符扫描**：Task 6/7 的机械随动点以「清单表 + 机械验证 grep」表达而非逐点代码——每行给出 file:line、组号、改法三要素，无悬空引用；两处 store key 解析器给出名称与归属文件。
- **类型一致性**：`Replicator/Router/Forwarder/Pending` 在 Task 4 定义、Task 5-11 全部按此签名消费；`ErrNotLeader` 由 cluster 定义（Task 2）、replication 转发（Task 9 依赖方向说明）；`RouteView` 仅 rpc 消费（Task 9 定义、Task 11 集群实现）；构造函数「rep, rt 前置」次序 Task 6/7 一致。
- **风险自记**：① EndTransaction 落在非 leader 节点的转发链（ForwardAppend+ForwardApply 两跳）是本批最长的分布式路径，e2e 必须含事务用例——若官方 SDK 的 endTransaction 寻址行为使该路径不可达，删代码比留死代码好，e2e 见真章；② Receive 每次都过 raft 提案（staged 判据是唯一空提案屏障），三节点消费吞吐必须在 e2e 里留一个粗略数字（非 benchmark，防止量级劣化无感）；③ Cluster.ApplyAsync 的 goroutine-per-propose 在高并发下的分配压力留待实测——raft 侧本就有在途上限（batch② MaxInflightMsgs），先简后优。
