# V2 batch②：复制内核 in-tree（B8.2 前半）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 spike 验证过的 etcd/raft 薄壳生产化进主仓：多组 raft 内核（1 meta + N 数据组）、raft 日志与 FSM 共用一个 Pebble 实例、节点间真实 TCP 传输、`replication.Replicator` 复制接口（单机/集群两后端）——为 batch③ 的 core 接线与协议面路由打好地基。

**Architecture:** spec §3–§5 的内核部分。每节点一个 `cluster.Manager`，装配 G=1+N 个 raft 组（组 0 = meta，组 1..N = 数据组，队列按 `hash(topic,queueID)` 归组）；所有组的 raft 日志（HardState/Entries/applied_index）与 FSM 数据写进**同一个 `store.Store`**、不同 key 前缀、共享一条 WAL（spec §5）；复制内容是**物理 Pebble batch 字节**（`Batch.Repr()`），leader 独占构造、全员盲 apply；两档确认（quorum-fsync / quorum-mem + 后台批量刷盘）沿用 spike 实测过的形态。**本批不接 core 写路径、不改协议面**——`Replicator` 接口就位并经三节点集成测试验证，接线是 batch③。

**Tech Stack:** Go 1.26、`go.etcd.io/raft/v3 v3.7.0`（**本批起进主 go.mod**，spike 阶段的隔离约束到此为止）、`google.golang.org/protobuf v1.36.11`（raftpb 序列化）、pebble v2.1.6（已有）、净 TCP + 长度前缀帧做节点间传输（不引入新的 gRPC 服务面，运维上是一个新监听端口）。

## Global Constraints

- **开工前提**：`v2-batch1-replication-foundation` 已合并 main（B2 类型化 Batch 是本批 `Repr()` 的载体）。开工时从最新 main 拉分支 `v2-batch2-replication-kernel`。
- spike 模块 `spike/raftshell/` **原样保留不动**（历史证据 + 数据来源），本批是移植改造不是搬移删除。
- raftpb 两条移植期陷阱（spike R1 教训，违反即数据竞态/编译错）：① `raftpb.Message` 内嵌互斥锁——**全程指针传递，绝不值拷贝**；② protobuf-go v2 开放结构下 `ConfChange` 标量字段是指针——构造用 `ccType.Enum()` / 取址，`ProposeConfChange` 传 `&cc`。
- Ready 处理契约（etcd/raft 硬约束）：**先持久化 HardState+Entries（按档决定 sync），再发送 Messages，apply CommittedEntries，最后 Advance**。AckQuorumMem 档有意放松第一步的 sync，安全性由干净关机标记 + learner 重入规则兜底（spec §2.2）。
- 两个终审观察项在本批落实为需求：① ConfChange 与普通提案的 waiter **分离命名空间**（两张 map）；② 重启合成 ConfState 时**按 HardState.Commit 裁剪**后再重放 ConfChange。
- 本批显式不做（batch③/④ 范围，代码注释写明边界）：raft 日志截断与快照（日志无界增长，learner 追齐走全量重放）、learner 重入的跨节点自动编排（本批只给原语，测试 harness 编排）、core/协议面接线、leader 摊布自动策略（只给 `TransferLeader` 原语）、单机→集群升级。
- 每个实现 task 必含「加关键节点日志」与「加注释」step（instrumenting-code）；日志走 `slog`，禁止 `fmt.Printf`。
- 构建/测试命令不接管道判断成败：`go build ./... ; echo "EXIT=$?"` 式写法。
- 提交信息尾部：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。

## File Structure

| 文件 | 职责 |
|---|---|
| `internal/store/store.go`（改） | 复制配套 API：`Batch.Repr()` / `NewBatchFromRepr` / `ApplyWith`（显式 sync 控制） |
| `internal/cluster/raftstore.go`（新） | raft 日志持久层：共库键布局、Persist/Load、干净关机标记、dataGroups 持久化校验 |
| `internal/cluster/transport.go`（新） | TCP 传输：分组信封帧、每 peer 发送队列、断线重连、满则丢 |
| `internal/cluster/group.go`（新） | 单组运行体：tick/Ready 循环、propose waiters（双命名空间）、committed apply |
| `internal/cluster/manager.go`（新） | 多组装配：组路由 hash、Propose/Leader/TransferLeader、StopClean、重启两路径 |
| `internal/replication/replication.go`（新） | `Replicator` 接口 + Standalone/Cluster 两后端 |
| 各自 `*_test.go` | 每层独立测试 + 三节点集成测试（`internal/cluster/cluster_test.go`） |

键前缀新增 `raft/`（与现有 `meta/ msg/ alloc/ cursor/ inflight/ keyidx/ delay/ metric/ half/ halfidx/` 无冲突）：

```
raft/groups                      → uint32 BE 数据组数（首启写入，此后启动校验，防换组数错组）
raft/clean_shutdown              → 干净关机标记（StopClean 写，启动读后即删）
raft/<g>/hs                      → HardState（protobuf）
raft/<g>/ent/<index 8B BE>       → Entry（protobuf）
raft/<g>/applied                 → uint64 BE 已 apply 索引（与 FSM 写同批原子，spec §5）
```

---

### Task 1: store 复制配套 API（Repr / NewBatchFromRepr / ApplyWith）

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces:
  - `func (b *Batch) Repr() []byte` — 批次的 Pebble 物理字节（leader 提案的复制载荷）
  - `func (s *Store) NewBatchFromRepr(data []byte) (*Batch, error)` — 从复制字节重建批次（follower 盲 apply 入口）
  - `func (s *Store) ApplyWith(b *Batch, sync bool) error` — 与 `Apply` 同语义但显式控制本次刷盘（raft 日志两档持久化需要 per-write sync，全局 `s.sync` 不够用）

- [ ] **Step 1: 写失败测试**

```go
// TestBatchReprRoundTrip 验证复制载荷的往返一致性：leader 侧组装的批次
// 导出字节，follower 侧重建并 apply 后，数据逐键一致——这是「复制物理
// batch 字节而非逻辑命令」（V2 spec §4）的最小正确性锚点。
func TestBatchReprRoundTrip(t *testing.T) {
	src := openTestStore(t) // 复用本文件已有的测试构造 helper（无则新建 t.TempDir + Open）
	dst := openTestStore(t)
	b := src.NewBatch()
	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("k-absent")); err != nil {
		t.Fatal(err)
	}
	repr := append([]byte(nil), b.Repr()...) // Repr 底层内存归批次所有，拷贝后再提交
	if err := src.Apply(b); err != nil {
		t.Fatal(err)
	}
	rb, err := dst.NewBatchFromRepr(repr)
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.Apply(rb); err != nil {
		t.Fatal(err)
	}
	v, ok, err := dst.Get([]byte("k1"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("follower 侧 apply 后 Get(k1) = %q,%v,%v; want v1,true,nil", v, ok, err)
	}
}

// TestNewBatchFromReprRejectsGarbage 坏字节必须在重建时报错，而不是
// apply 时 panic——复制链路上的损坏要在最早的边界被拦下。
func TestNewBatchFromReprRejectsGarbage(t *testing.T) {
	st := openTestStore(t)
	if _, err := st.NewBatchFromRepr([]byte("not-a-batch")); err == nil {
		t.Fatal("坏批次字节应报错，得到 nil")
	}
}

// TestApplyWithOverridesSync ApplyWith 的 sync 参数独立于 Store 全局档位。
// 行为断言只能到「提交成功且可读」——fsync 是否真实发生无法在单测观测，
// 由集成层（cluster）的两档吞吐差异间接验证。
func TestApplyWithOverridesSync(t *testing.T) {
	st := openTestStore(t) // openTestStore 默认 syncWrites=false
	b := st.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := st.ApplyWith(b, true); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.Get([]byte("k")); !ok {
		t.Fatal("ApplyWith 提交后应可读")
	}
}
```

- [ ] **Step 2: 确认失败** — Run: `go test ./internal/store/ -run 'Repr|ApplyWith' -v ; echo "EXIT=$?"`，Expected: 编译失败（Repr/NewBatchFromRepr/ApplyWith 未定义）。

- [ ] **Step 3: 实现**

```go
// Repr 返回批次的 Pebble 物理字节表示——集群模式的复制载荷（V2 spec §4：
// 复制物理 batch 字节而非逻辑命令）。
//
// 注意：返回的切片底层内存归批次所有，提案方需在批次 Commit/Close 前
// 拷贝（raft 库会长期持有日志条目）。
func (b *Batch) Repr() []byte { return b.b.Repr() }

// NewBatchFromRepr 从复制来的批次字节重建类型化批次——follower 盲 apply
// 的唯一入口。坏字节在此报错，不进 Apply。
func (s *Store) NewBatchFromRepr(data []byte) (*Batch, error) {
	nb := s.db.NewBatch()
	if err := nb.SetRepr(data); err != nil {
		// 批次已废弃，按 Apply 失败同款约定丢给 GC（见 Apply 注释）
		return nil, fmt.Errorf("store NewBatchFromRepr: %w", err)
	}
	return &Batch{b: nb}, nil
}

// ApplyWith 与 Apply 同语义，但本次刷盘由 sync 参数显式决定，不看全局
// 档位。供集群层使用：raft 日志持久化的 sync 由确认档（quorum-fsync/
// quorum-mem）逐次决定，FSM apply 则总是 NoSync（持久性由 raft 日志与
// 后台批量刷盘承担，spec §5）。
//
// 失败/成功的批次归属与 Apply 完全一致（见 Apply 注释）。
func (s *Store) ApplyWith(b *Batch, sync bool) error {
	opt := pebble.NoSync
	if sync {
		opt = pebble.Sync
	}
	start := time.Now()
	if err := b.b.Commit(opt); err != nil {
		return fmt.Errorf("store ApplyWith: %w", err)
	}
	if OnApplyObserve != nil {
		OnApplyObserve(time.Since(start))
	}
	return b.b.Close()
}
```

同步把 `Apply` 重构为 `return s.ApplyWith(b, s.sync)`（DRY，行为不变，直方图口径不变）。

- [ ] **Step 4: 跑测试通过** — Run: `go test -race ./internal/store/ ; echo "EXIT=$?"` → EXIT=0。

- [ ] **Step 5: 加注释自检** — 三个新方法 doc comment 齐（上面代码已含）；`Repr` 的内存归属陷阱必须写明。本 task 无新日志点（store 热路径不打日志是既有边界，见文件头注释）——豁免理由记录于此。

- [ ] **Step 6: 全量回归** — Run: `go test -race ./... ; echo "EXIT=$?"` → EXIT=0（34 个既有调用点不受影响）。

- [ ] **Step 7: Commit**

```bash
git add internal/store/
git commit -m "feat(store): 复制配套 API——Batch.Repr/NewBatchFromRepr/ApplyWith（B8.2）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: raft 日志持久层（共库键布局）

**Files:**
- Create: `internal/cluster/raftstore.go`
- Test: `internal/cluster/raftstore_test.go`

**Interfaces:**
- Consumes: Task 1 的 `store.ApplyWith`；`store.Scan/Get`；`store.PrefixUpperBound`
- Produces:
  - `func newRaftStore(st *store.Store, lg *slog.Logger) *raftStore`
  - `func (r *raftStore) Persist(g uint32, hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error` — 单批原子写；写入新条目后**删除更高 index 的旧条目**（raft 回退覆盖语义）
  - `func (r *raftStore) Load(g uint32) (*raftpb.HardState, []*raftpb.Entry, error)` — 全量读回（本批无截断，条目齐全）
  - `func (r *raftStore) Applied(g uint32) (uint64, error)` / `appliedKey(g uint32) []byte` — applied 读取；写入由 apply 路径并进 FSM 批（Task 4）
  - `func (r *raftStore) EnsureGroups(n uint32) error` — 首启写 `raft/groups`，此后不匹配即报错拒启
  - `func (r *raftStore) MarkCleanShutdown() error` / `ConsumeCleanShutdown() (bool, error)`
  - `func confStateFromEntries(ents []*raftpb.Entry, commit uint64) *raftpb.ConfState` — **按 commit 裁剪**后重放合成（终审观察项②落实处）

- [ ] **Step 1: 写失败测试**

```go
// TestPersistLoadRoundTrip 两组各写 HardState+条目，Load 逐组读回一致，
// 组间互不串扰（键前缀隔离的承重断言）。
func TestPersistLoadRoundTrip(t *testing.T) {
	st := openClusterTestStore(t) // helper：store.Open(t.TempDir(), false, testSlog(t))
	rs := newRaftStore(st, testSlog(t))
	e := func(g uint32, idx, term uint64) *raftpb.Entry { // 测试内构造器
		typ := raftpb.EntryNormal
		return &raftpb.Entry{Index: &idx, Term: &term, Type: &typ, Data: []byte{byte(g)}}
	}
	one, two := uint64(1), uint64(2)
	hs1 := &raftpb.HardState{Term: &two, Commit: &one}
	if err := rs.Persist(0, hs1, []*raftpb.Entry{e(0, 1, 1), e(0, 2, 2)}, true); err != nil {
		t.Fatal(err)
	}
	if err := rs.Persist(1, nil, []*raftpb.Entry{e(1, 1, 1)}, false); err != nil {
		t.Fatal(err)
	}
	hs, ents, err := rs.Load(0)
	if err != nil || len(ents) != 2 || hs.GetCommit() != 1 {
		t.Fatalf("组0 Load = hs.commit %d, %d 条, %v; want 1, 2, nil", hs.GetCommit(), len(ents), err)
	}
	if _, ents1, _ := rs.Load(1); len(ents1) != 1 || ents1[0].Data[0] != 1 {
		t.Fatalf("组1 被组0 污染或缺失: %v", ents1)
	}
}

// TestPersistTruncatesConflictTail raft 回退覆盖：重写 index=2 后，
// 旧的 index=3 必须消失（否则 Load 读回幽灵条目，选举后状态机分叉）。
func TestPersistTruncatesConflictTail(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	e := func(idx, term uint64) *raftpb.Entry {
		typ := raftpb.EntryNormal
		return &raftpb.Entry{Index: &idx, Term: &term, Type: &typ}
	}
	if err := rs.Persist(0, nil, []*raftpb.Entry{e(1, 1), e(2, 1), e(3, 1)}, true); err != nil {
		t.Fatal(err)
	}
	if err := rs.Persist(0, nil, []*raftpb.Entry{e(2, 2)}, true); err != nil { // 新任期覆盖 2，3 成幽灵
		t.Fatal(err)
	}
	_, ents, err := rs.Load(0)
	if err != nil || len(ents) != 2 || ents[1].GetTerm() != 2 {
		t.Fatalf("覆盖后 Load = %d 条 (末条 term %d), %v; want 2 条、term 2", len(ents), ents[len(ents)-1].GetTerm(), err)
	}
}

// TestEnsureGroupsRejectsMismatch 组数是队列→组映射的分母，换组数=数据
// 错组，必须在启动时挡死。
func TestEnsureGroupsRejectsMismatch(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	if err := rs.EnsureGroups(3); err != nil {
		t.Fatal(err)
	}
	if err := rs.EnsureGroups(3); err != nil { // 幂等
		t.Fatal(err)
	}
	if err := rs.EnsureGroups(4); err == nil {
		t.Fatal("组数 3→4 应拒绝启动，得到 nil")
	}
}

// TestConfStateClampsToCommit 终审观察项②：commit 之外的 ConfChange
// 尾巴不得进成员表。
func TestConfStateClampsToCommit(t *testing.T) {
	cc := func(idx uint64, typ raftpb.ConfChangeType, node uint64) *raftpb.Entry {
		etyp := raftpb.EntryConfChange
		c := raftpb.ConfChange{Type: typ.Enum(), NodeId: &node}
		data, _ := proto.Marshal(&c)
		return &raftpb.Entry{Index: &idx, Type: &etyp, Data: data}
	}
	ents := []*raftpb.Entry{
		cc(1, raftpb.ConfChangeAddNode, 1),
		cc(2, raftpb.ConfChangeAddNode, 2),
		cc(3, raftpb.ConfChangeAddNode, 3), // commit=2：这条是未提交尾巴
	}
	cs := confStateFromEntries(ents, 2)
	if len(cs.Voters) != 2 {
		t.Fatalf("voters = %v; want [1 2]（index 3 未提交，不得纳入）", cs.Voters)
	}
}
```

另需 helper：`openClusterTestStore(t)`（`store.Open(t.TempDir(), false, ...)` + `t.Cleanup(st.Close)`）与 `testSlog(t)`（`slog.New(slog.NewTextHandler(testWriter{t}, ...))`，testWriter 参照 spike node_test.go 的实现原样移植）。

- [ ] **Step 2: 确认失败** — 编译失败（包不存在）。

- [ ] **Step 3: 实现**

移植底本：`spike/raftshell/wal.go`（Persist/Load/marker/confStateFromEntries 逻辑已在 spike 验证）。与底本的全部差异：

1. **不再自持 pebble**：构造改为收 `*store.Store`，所有写经 `st.NewBatch()` + Set/DeleteRange + `st.ApplyWith(b, sync)`，读经 `st.Get`/`st.Scan`——B2 唯一写入口在集群层同样成立，无裸 pebble；
2. **键带组号**：`entKey(g, index)` = `fmt.Sprintf("raft/%d/ent/", g)` + 8B BE index；`hsKey(g)`、`appliedKey(g)`；Load 用 `st.Scan(entPrefix(g), store.PrefixUpperBound(entPrefix(g)), 0, ...)`，回调内 `proto.Unmarshal` 后 append（Scan 已保证升序，spike 的 sort 兜底可去，注释说明）；
3. **Persist 尾部截断**：写完 ents 后追加 `b.DeleteRange(entKey(g, last+1), store.PrefixUpperBound(entPrefix(g)))`——spike 靠 MemoryStorage 语义掩盖了幽灵条目问题，共库持久化必须显式删；
4. **confStateFromEntries 加 commit 参数**：`if ent.GetIndex() > commit { break }`（条目升序，直接 break）；
5. **marker 键改 `raft/clean_shutdown`**；后台 flusher 不在本层（Manager 持有，Task 5）；
6. `EnsureGroups`：Get `raft/groups`——不存在则写入（4B BE，Sync）；存在且不等则 `fmt.Errorf("集群数据组数不可变更：磁盘 %d, 配置 %d——组数是队列归组映射的分母，变更会让存量数据错组", ...)`。

- [ ] **Step 4: 加关键节点日志**
  - `EnsureGroups` 首启写入：Info「数据组数已持久化」（groups）
  - `ConsumeCleanShutdown`：Info 标记存在/不存在（决定重启路径的关键事实）
  - `Persist` 失败路径由调用方（group 运行体）带组号记 Error；本层错误全部 `fmt.Errorf` 带 g 与操作名上下文
  - Load 完成：Debug（g、条目数、hs.commit）——重启排障的第一行证据

- [ ] **Step 5: 加注释** — 文件头（职责：raft 日志共库持久化；边界：不做截断/快照——batch④，不持有 pebble——一切经 store 唯一写入口）；键布局表进文件头；Persist 的「先写后删尾」次序为什么不可反（同批原子，无次序问题，但要写明依赖单批原子性）。

- [ ] **Step 6: 跑测试** — Run: `go test -race ./internal/cluster/ ; echo "EXIT=$?"` → EXIT=0。注意：本 task 起主 `go.mod` 引入 `go.etcd.io/raft/v3 v3.7.0` 与 `google.golang.org/protobuf`（`GOPROXY=https://goproxy.cn,direct go mod tidy`）。

- [ ] **Step 7: Commit**

```bash
git add internal/cluster/ go.mod go.sum
git commit -m "feat(cluster): raft 日志共库持久层——组前缀键布局与回退截断（B8.2）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: TCP 传输层（分组信封帧）

**Files:**
- Create: `internal/cluster/transport.go`
- Test: `internal/cluster/transport_test.go`

**Interfaces:**
- Produces:
  - `func newTransport(self uint64, ln net.Listener, peers map[uint64]string, deliver func(g uint32, m *raftpb.Message), lg *slog.Logger) *transport` — peers 是节点 id→地址表（**不含**本节点；本节点消息不经传输，Manager 内部短路）
  - `func (t *transport) Start(ctx context.Context)` — 起 accept 循环与各 peer 的拨号/发送循环；ctx 取消即全部退出
  - `func (t *transport) Send(g uint32, msgs []*raftpb.Message)` — 按 `m.GetTo()` 入各 peer 队列；对端不可达/队列满即丢（raft 心跳重试兜底）
  - 帧格式：`[4B BE 帧长][4B BE 组号][raftpb.Message protobuf]`，帧长 = 4+len(payload)，单帧上限 16MiB（超限断连，防坏帧撑爆内存）

- [ ] **Step 1: 写失败测试**

```go
// TestTransportDeliverAcrossNodes 两个传输体经真实 TCP 互投消息，
// 组号与消息体原样到达（信封帧的往返承重测试）。
func TestTransportDeliverAcrossNodes(t *testing.T) {
	ln1, _ := net.Listen("tcp", "127.0.0.1:0")
	ln2, _ := net.Listen("tcp", "127.0.0.1:0")
	got := make(chan string, 4)
	mk := func(self uint64, ln net.Listener, peerID uint64, peerAddr string) *transport {
		return newTransport(self, ln, map[uint64]string{peerID: peerAddr},
			func(g uint32, m *raftpb.Message) {
				got <- fmt.Sprintf("g%d:from%d:to%d", g, m.GetFrom(), m.GetTo())
			}, testSlog(t))
	}
	t1 := mk(1, ln1, 2, ln2.Addr().String())
	t2 := mk(2, ln2, 1, ln1.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t1.Start(ctx)
	t2.Start(ctx)
	from, to := uint64(1), uint64(2)
	typ := raftpb.MsgHeartbeat
	t1.Send(3, []*raftpb.Message{{Type: &typ, From: &from, To: &to}})
	select {
	case s := <-got:
		if s != "g3:from1:to2" {
			t.Fatalf("到达 %q; want g3:from1:to2", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("5s 未收到消息（含首次拨号重试窗口）")
	}
}

// TestTransportDropsWhenPeerDown 对端未监听时 Send 不阻塞不报错——
// 丢消息是 raft 传输层的合法行为，阻塞才是灾难（会卡死 Ready 循环）。
func TestTransportDropsWhenPeerDown(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	tr := newTransport(1, ln, map[uint64]string{2: "127.0.0.1:1"}, // 1 端口必拒
		func(uint32, *raftpb.Message) {}, testSlog(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	from, to := uint64(1), uint64(2)
	typ := raftpb.MsgHeartbeat
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ { // 远超队列容量，验证满则丢不阻塞
			tr.Send(0, []*raftpb.Message{{Type: &typ, From: &from, To: &to}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("对端不可达时 Send 阻塞——违反不阻塞契约")
	}
}
```

- [ ] **Step 2: 确认失败** — 编译失败（transport 未定义）。

- [ ] **Step 3: 实现**

结构：每 peer 一个 `chan envelope`（cap 4096）+ 一个发送 goroutine（拨号 → 循环写帧 → 断线关连接、500ms 退避重拨）；`Send` 对满队列 `select default` 丢弃；accept 循环每连接一个读 goroutine（`io.ReadFull` 读帧长→读帧体→拆组号→`proto.Unmarshal` 到**新分配的** `&raftpb.Message{}`→`deliver`）。写侧序列化在发送 goroutine 内做（`proto.Marshal(m)`），信封头用 `binary.BigEndian`。坏帧（长度超 16MiB、Unmarshal 失败）记 Warn 后断开该连接，等对端重拨——不能让一个坏字节流永久毒化读循环。

```go
type envelope struct {
	group uint32
	msg   *raftpb.Message // 指针契约：raftpb.Message 内嵌锁，禁止值拷贝
}
```

- [ ] **Step 4: 加关键节点日志**
  - 拨号成功/断线：Info「peer 已连接」/「peer 断开，退避重连」（self、peer、addr、err）
  - 队列满丢弃：**Debug 且带计数**（每连接周期累计，避免风暴刷屏——高频点降级，instrumenting-code 热循环规则）
  - 坏帧断连：Warn（remote、帧长、err）
  - accept 循环退出、发送循环退出：Debug

- [ ] **Step 5: 加注释** — 文件头（职责：raft 消息的节点间投递；边界：不保证送达——丢弃合法、由 raft 重试兜底；不做鉴权/TLS——集群内网假设，batch③ 记录到部署文档）；帧格式注释画出字节布局；「Send 永不阻塞」契约写在方法注释（Ready 循环在上游，阻塞即全组停摆）。

- [ ] **Step 6: 跑测试** — Run: `go test -race ./internal/cluster/ -run Transport -v ; echo "EXIT=$?"` → EXIT=0。

- [ ] **Step 7: Commit**

```bash
git add internal/cluster/transport.go internal/cluster/transport_test.go
git commit -m "feat(cluster): TCP 传输层——分组信封帧、满则丢、断线重连（B8.2）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: 单组运行体（tick/Ready/apply/waiters）

**Files:**
- Create: `internal/cluster/group.go`
- Test: `internal/cluster/group_test.go`

**Interfaces:**
- Consumes: Task 2 `raftStore`（Persist/appliedKey）、Task 1 `store.NewBatchFromRepr`/`ApplyWith`、Task 3 `transport.Send`
- Produces:
  - `func newGroup(g uint32, rn raft.Node, storage *raft.MemoryStorage, rs *raftStore, st *store.Store, send func(g uint32, msgs []*raftpb.Message), mode AckMode, lg *slog.Logger) *group`
  - `func (gr *group) run(ctx context.Context)` — tick 100ms / ElectionTick 10 / HeartbeatTick 1（与 spike 及实测 1.47~1.53s 切换窗口对齐）
  - `func (gr *group) propose(ctx context.Context, batchRepr []byte) error` — 阻塞至条目**在本节点 apply 完成**（读己之写：produce 依赖 Apply 返回后立即可读，等 commit 不等 apply 会破坏它）
  - `func (gr *group) proposeConfChange(ctx context.Context, typ raftpb.ConfChangeType, nodeID uint64) error` — waiter 走独立命名空间（终审观察项①）
  - `func (gr *group) step(m *raftpb.Message)` / `leader() uint64` / `isLeader() bool` / `appliedIndex() uint64` / `done() <-chan struct{}`
  - `type AckMode int` + `AckQuorumFsync` / `AckQuorumMem`（String() 同 spike）
  - 提案编码：`[8B BE waiter id][batch repr]`；apply 时 `data[8:]` 即批次字节
  - raftConfig 同 spike：MaxSizePerMsg 1<<20、MaxInflightMsgs 256、丢弃 raft 自带 logger

- [ ] **Step 1: 写失败测试**

```go
// TestGroupSingleNodeProposeApply 单节点单组最小闭环：propose 的批次
// 字节 apply 进共享 store，且 applied_index 与 FSM 写同批落盘（spec §5
// 原子性承诺的直接断言）。
func TestGroupSingleNodeProposeApply(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	gr := startSingleNodeGroup(t, 0, rs, st, AckQuorumFsync) // helper 见下
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b := st.NewBatch()
	if err := b.Set([]byte("meta/topic/demo"), []byte("payload")); err != nil {
		t.Fatal(err)
	}
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil { // 提案路径只取字节，原批次弃用
		t.Fatal(err)
	}
	if err := gr.propose(ctx, repr); err != nil {
		t.Fatal(err)
	}
	// propose 返回即本节点已 apply：立即可读，且 applied_index 已持久化
	if _, ok, _ := st.Get([]byte("meta/topic/demo")); !ok {
		t.Fatal("propose 返回后 FSM 键不可读——读己之写被破坏")
	}
	if idx, err := rs.Applied(0); err != nil || idx == 0 {
		t.Fatalf("applied_index = %d, %v; want >0", idx, err)
	}
	if idx, gidx := mustApplied(t, rs, 0), gr.appliedIndex(); idx != gidx {
		t.Fatalf("盘上 applied %d != 内存 applied %d（必须同批原子）", idx, gidx)
	}
}

// TestGroupProposeCtxTimeout 无 quorum（三成员只起一个）时 propose 按
// ctx 超时返回，waiter 被清理（泄漏检测靠后续 -race 与 map 断言）。
func TestGroupProposeCtxTimeout(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	gr := startLoneGroupOfThree(t, 0, rs, st) // 成员表 {1,2,3} 只起节点 1：永无 quorum
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := gr.propose(ctx, []byte("x")); err == nil {
		t.Fatal("无 quorum 的 propose 应超时报错，得到 nil")
	}
	gr.mu.Lock()
	n := len(gr.propWaiters)
	gr.mu.Unlock()
	if n != 0 {
		t.Fatalf("超时后 propWaiters 残留 %d 个 waiter", n)
	}
}
```

helper：`startSingleNodeGroup` = `raft.StartNode(raftConfig(1, storage), []raft.Peer{{ID: 1}})` + `newGroup(...)` + `go gr.run(ctx)` + `t.Cleanup` 等 done；send 回调传空函数（单节点无外发需求）。`startLoneGroupOfThree` 同构，peers 为 {1,2,3}。`mustApplied` 是 `rs.Applied` 的 fatal 包装。

- [ ] **Step 2: 确认失败** — 编译失败。

- [ ] **Step 3: 实现**

移植底本：`spike/raftshell/node.go` 的 run/handleReady/Propose/ProposeConfChange/notify 结构。与底本的全部差异：

1. **持久化换层**：`handleReady` 第一步改调 `rs.Persist(g, hs, ents, gr.syncPersist())`——`syncPersist()` = `mode == AckQuorumFsync && (有 entries 或 hardstate 非空)`；MemoryStorage 双记账保留（raft 读取视图）；
2. **apply 换成真 FSM**：`EntryNormal` 且 `len(Data) > 8` 时：`nb, err := st.NewBatchFromRepr(data[8:])` → `nb.Set(rs.appliedKey(g), 8B BE index)` → `st.ApplyWith(nb, false)`——applied 与 FSM 同批原子（spec §5）。**apply 失败是不可恢复错误**：记 Error 后 panic（状态机与日志分叉比进程死更糟；注释写明这是刻意选择）。空条目/短条目只写 applied（单键批次）；
3. **跳过已 apply**：`if ent.GetIndex() <= gr.applied { continue }`——重启重放的幂等保证；apply 后更新内存 `gr.applied`（atomic.Uint64，`appliedIndex()` 读它）；
4. **waiter 双命名空间**：`propWaiters` / `ccWaiters` 两张 map，共用一个 `nextID` 计数器；`EntryNormal` 通知 `propWaiters[id]`，`EntryConfChange` 通知 `ccWaiters[cc.GetId()]`——不同类条目 id 相同也不会误唤（终审观察项①落实处）；
5. **外发经参数注入的 send 回调**（Manager 里接 transport 并短路本节点，见 Task 5）；
6. AckQuorumMem 的后台 flusher 不在 group 层（全组共享一条 WAL，一个 flusher 即可，Manager 持有）。

- [ ] **Step 4: 加关键节点日志**
  - leader 变更（SoftState 变化）：Info「组 leader 变更」（g、lead、term）——切换观测的第一信号
  - apply 失败 panic 前：Error（g、index、err）
  - propose 超时：由调用方带上下文处理，本层 Debug
  - run 退出：Info（g）；tick/心跳等高频路径零日志（热循环规则）

- [ ] **Step 5: 加注释** — 文件头（职责：单组 raft 生命周期与 FSM apply；边界：不管组间路由与成员编排——Manager 的事；不做快照/截断——batch④）；Ready 四步契约注释原样从 spike 移植并标注 mem 档放松点；「propose 等 apply 而非等 commit」的读己之写理由写进 propose doc comment；apply-panic 的取舍理由写进代码。

- [ ] **Step 6: 跑测试** — Run: `go test -race ./internal/cluster/ -run Group -v ; echo "EXIT=$?"` → EXIT=0。

- [ ] **Step 7: Commit**

```bash
git add internal/cluster/group.go internal/cluster/group_test.go
git commit -m "feat(cluster): 单组运行体——Ready 契约、原子 applied、waiter 双命名空间（B8.2）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Manager 多组装配与重启两路径

**Files:**
- Create: `internal/cluster/manager.go`
- Test: `internal/cluster/manager_test.go`

**Interfaces:**
- Consumes: Task 2/3/4 全部
- Produces（batch③ 接线与 batch④ 场景测试将直接消费，签名此处定稿）:

```go
const MetaGroup uint32 = 0

type Options struct {
	NodeID     uint64            // 本节点 id（1..3）
	Peers      map[uint64]string // 全体节点 id → raft 监听地址（含本节点）
	Listener   net.Listener      // 可选：测试注入已建监听（nil 则按 Peers[NodeID] 监听）
	DataGroups uint32            // 数据组数，默认 3；首启持久化，此后不可变
	Mode       AckMode
	Store      *store.Store
	Logger     *slog.Logger
}

func NewManager(o Options) (*Manager, error)   // 检测干净关机标记：不干净时返回 ErrUncleanShutdown
func (m *Manager) Start(ctx context.Context)   // 起全部组 run 循环 + transport + mem 档 flusher
func (m *Manager) StopClean(ctx context.Context) error // 写标记（Sync）后停全部组
func (m *Manager) Done() <-chan struct{}       // 全部组退出后关闭

func (m *Manager) GroupForQueue(topic string, queueID uint32) uint32 // 1 + fnv1a(topic,queueID)%DataGroups
func (m *Manager) Propose(ctx context.Context, g uint32, batchRepr []byte) error
func (m *Manager) IsLeader(g uint32) bool
func (m *Manager) Leader(g uint32) (nodeID uint64, ok bool)
func (m *Manager) TransferLeader(g uint32, to uint64) // 摊布原语，自动策略 batch③
func (m *Manager) Groups() uint32                     // 1 + DataGroups

var ErrUncleanShutdown = errors.New("cluster: 上次为不干净关机，须以 learner 重入（清空状态后经存活 leader 重新加入）")
func WipeForRejoin(dataDir string) error              // 清空整个数据目录（learner 重入前置；调用方负责先关 store）
func (m *Manager) ProposeConfChange(ctx context.Context, g uint32, typ raftpb.ConfChangeType, nodeID uint64) error // 重入编排原语（本批由测试 harness 驱动；生产编排 batch③）
```

- [ ] **Step 1: 写失败测试**

```go
// TestGroupForQueueStable 队列→组映射是入盘契约，黄金值锁死：任何改动
// fnv 输入编码的重构都会在这里炸出来，而不是让存量数据错组。
func TestGroupForQueueStable(t *testing.T) {
	m := &Manager{dataGroups: 3}
	golden := []struct {
		topic string
		q     uint32
		want  uint32
	}{
		// 实现完成后用 t.Log 打出实际值回填此表并 review（首次生成即冻结），
		// 断言三条覆盖：不同 topic 不同组、同 topic 不同 queue 可不同组、组号 ∈ [1,3]
		{"orders", 0, 0}, {"orders", 1, 0}, {"payments", 0, 0},
	}
	for _, g := range golden {
		got := m.GroupForQueue(g.topic, g.q)
		if got < 1 || got > 3 {
			t.Fatalf("GroupForQueue(%s,%d) = %d 越界 [1,3]", g.topic, g.q, got)
		}
		t.Logf("golden 回填: {%q, %d, %d}", g.topic, g.q, got)
	}
}

// TestManagerCleanRestartResumes 单节点集群（成员表只有自己）干净关机后
// 重启：applied 恢复、数据可读、无 ErrUncleanShutdown。
func TestManagerCleanRestartResumes(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem) // helper：单成员 Options
	b := st1.NewBatch()
	_ = b.Set([]byte("meta/topic/t1"), []byte("v"))
	repr := append([]byte(nil), b.Repr()...)
	_ = b.Close()
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := m1.Propose(pctx, MetaGroup, repr); err != nil {
		t.Fatal(err)
	}
	if err := m1.StopClean(ctx); err != nil {
		t.Fatal(err)
	}
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}
	st2, m2 := startSoloManager(t, dir, AckQuorumMem) // 同目录重启
	if _, ok, _ := st2.Get([]byte("meta/topic/t1")); !ok {
		t.Fatal("干净重启后数据丢失")
	}
	_ = m2 // 起来即成功；NewManager 未返回 ErrUncleanShutdown 已在 helper 断言
}

// TestManagerUncleanRestartRefusesResume 不写标记直接停（模拟断电）后，
// NewManager 必须返回 ErrUncleanShutdown——异步刷盘不可裸恢复（spec §2.2）。
func TestManagerUncleanRestartRefusesResume(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem)
	m1.kill() // 测试后门：cancel 运行 ctx，不写标记
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}
	st2 := mustOpenStore(t, dir)
	_, err := NewManager(soloOptions(t, st2, dir, AckQuorumMem))
	if !errors.Is(err, ErrUncleanShutdown) {
		t.Fatalf("不干净重启 NewManager = %v; want ErrUncleanShutdown", err)
	}
}
```

（`startSoloManager`/`soloOptions`/`mustOpenStore` 为本文件 helper：单成员 `Peers: {1: "127.0.0.1:0"}` + `Listener` 注入；`kill()` 是 Manager 上的小写测试方法，等价 spike 的 Kill。）

- [ ] **Step 2: 确认失败** — 编译失败。

- [ ] **Step 3: 实现**

装配序（NewManager）：`EnsureGroups(DataGroups)` → `ConsumeCleanShutdown()`：标记存在→各组 `rs.Load(g)` 重建 MemoryStorage（ApplySnapshot 合成元数据 + Append + SetHardState，snapTerm 近似及其安全论证注释**原样从 spike cluster.go 移植**）+ `raft.RestartNode`，且 `raftConfig.Applied = rs.Applied(g)`（raft 从 applied+1 重投递，配合 group 的跳过逻辑双保险）；标记不存在且盘上有 raft 键（`rs.Applied(0)` 或任一组 hs 存在）→ 返回 `ErrUncleanShutdown`；全新目录→`raft.StartNode(cfg, peers)` 引导。ConfState 合成调 `confStateFromEntries(ents, hs.GetCommit())`。

Start：起 transport（deliver 回调按 g 路由到 `groups[g].step(m)`）、各组 `go run(ctx)`、mem 档起 200ms flusher goroutine（`st.ApplyWith(st.NewBatch(), true)`，借 WAL 顺序性一次 fsync 覆盖全组——spike wal.go 的注释与实现移植）。**本节点消息短路**：group 的 send 回调里 `m.GetTo() == o.NodeID` 的消息直接 `groups[g].step(m)`，不进 transport（省一次序列化往返；单成员测试也靠它跑通）。

`GroupForQueue`：`fnv.New32a` 写入 `topic` 字节 + 4B BE queueID，`1 + h.Sum32()%dataGroups`。映射算法注释标注「入盘契约，永不可变——变更即存量数据错组，黄金值测试锁死」。

`StopClean`：`rs.MarkCleanShutdown()`（失败记 Error 按不干净处理，不阻塞退出——spike 语义）→ cancel → 等全组 done → 停 flusher。

- [ ] **Step 4: 加关键节点日志**
  - NewManager 完成：Info「集群管理器初始化」（node、groups、mode、恢复路径 fresh/clean-resume）
  - `ErrUncleanShutdown` 返回前：Error「检测到不干净关机，拒绝直接恢复——须清空状态以 learner 重入」（沿用 spike 排障措辞语义）
  - StopClean：Info「干净关机标记已写入」
  - flusher 提交失败：Error（WAL sync 失败即 pebble 不可恢复态，下一拍所有写都会炸，这行日志是尸检第一现场）

- [ ] **Step 5: 加注释** — 文件头（职责：多组装配、组路由、生命周期与恢复判定；边界：不自动编排 learner 重入——原语给全、编排属 batch③；不接 core 写路径）；`WipeForRejoin` 注明「必须先 Close store 再调用，且调用后需以全新 store+Manager 走 fresh 路径由存活 leader 的 ConfChange 赋予身份」。

- [ ] **Step 6: 跑测试** — Run: `go test -race ./internal/cluster/ ; echo "EXIT=$?"` → EXIT=0（golden 表按首次输出回填后再跑一遍）。

- [ ] **Step 7: Commit**

```bash
git add internal/cluster/
git commit -m "feat(cluster): Manager 多组装配——组路由、干净/不干净重启判定（B8.2）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Replicator 接口与两后端

**Files:**
- Create: `internal/replication/replication.go`
- Test: `internal/replication/replication_test.go`

**Interfaces:**
- Produces（batch③ 把 core 的 34 个写点改到这上面，形状此处定稿）:

```go
// Replicator 是写路径的复制抽象：单机后端零开销直通，集群后端把批次
// 字节提进所属 raft 组。group 用 cluster.MetaGroup 或
// Manager.GroupForQueue 的返回值。
type Replicator interface {
	// Apply 原子提交批次。返回时保证：单机后端已按 Store 档位落盘；
	// 集群后端已 quorum 确认且在本节点 apply 完成（读己之写成立）。
	// 成功/失败后的批次归属与 store.Apply 一致：调用方不再持有。
	Apply(ctx context.Context, group uint32, b *store.Batch) error
}

func NewStandalone(st *store.Store) *Standalone // Apply 忽略 group，直通 st.Apply
func NewCluster(m *cluster.Manager) *Cluster    // Apply = m.Propose(ctx, group, b.Repr()) + b.Close()
```

集群后端注意：`b.Repr()` 取字节后必须拷贝再 `b.Close()`（Repr 内存归批次所有，Task 1 陷阱）；非 leader 时 `m.Propose` 返回的错误原样上抛——「翻译成客户端可重试错误码」是 batch③ 协议面的事，本层不做错误包装以免吞掉 raft 语义。`ApplyAsync`/`Pending` 拆分形态（produce 热路径的 group-commit 优化）**刻意不进本批接口**：等 batch③ 接线 produce 时按实测需要再加，YAGNI——决策记录在接口注释。

- [ ] **Step 1: 写失败测试**

```go
// TestStandaloneApplyPassthrough 单机后端 = 今天的路径：apply 即可读，
// 忽略 group 参数。
func TestStandaloneApplyPassthrough(t *testing.T) {
	st := openReplTestStore(t)
	r := NewStandalone(st)
	b := st.NewBatch()
	_ = b.Set([]byte("k"), []byte("v"))
	if err := r.Apply(context.Background(), 42, b); err != nil { // group 随便传
		t.Fatal(err)
	}
	if _, ok, _ := st.Get([]byte("k")); !ok {
		t.Fatal("单机后端 apply 后不可读")
	}
}
```

（集群后端的行为测试在 Task 7 三节点集成里做——单测起整套集群不划算，此处只测单机后端与编译期接口满足：`var _ Replicator = (*Standalone)(nil)` / `(*Cluster)(nil)`。）

- [ ] **Step 2: 确认失败** — 编译失败。

- [ ] **Step 3: 实现** — 按 Interfaces 块签名实现；`Cluster.Apply` 内：`repr := append([]byte(nil), b.Repr()...)` → `b.Close()`（关闭失败仅记 Warn，不挡提案——字节已取出）→ `m.Propose(ctx, group, repr)`。

- [ ] **Step 4: 加关键节点日志** — 本层是纯粘合，仅集群后端 `b.Close()` 失败记 Warn（带 group）；其余零日志（成功路径的可观测性由 group 层 leader 变更日志与 store 直方图承担）——豁免决策记录在文件头。

- [ ] **Step 5: 加注释** — 文件头（职责：写路径复制抽象；边界：不做错误翻译、不做重试、不做组路由——路由是调用方拿 Manager 算好传入）；接口注释含 ApplyAsync 缓议的 YAGNI 决策。

- [ ] **Step 6: 跑测试** — Run: `go test -race ./internal/replication/ ; echo "EXIT=$?"` → EXIT=0。

- [ ] **Step 7: Commit**

```bash
git add internal/replication/
git commit -m "feat(replication): Replicator 接口与单机/集群两后端（B8.2）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: 三节点集成测试（真 TCP 全链路）

**Files:**
- Create: `internal/cluster/cluster_test.go`（三节点 harness + 场景）

**Interfaces:**
- Consumes: Task 5 Manager 全部原语、Task 6 `replication.Cluster`
- Produces: `testCluster` harness（batch④ 场景测试的雏形）：`newTestCluster(t, mode)`（三节点、各自 `store.Store` + Manager、127.0.0.1 随机端口先建 Listener 再拼 Peers 表——解拨号先有鸡还是先有蛋）、`leaderOf(g)`、`kill(id)`、`waitConverged(keys)`

- [ ] **Step 1: 写失败测试（四个场景一次写全）**

```go
// TestClusterReplicateAllGroups 三节点起全 4 组；经 Replicator 往 meta 组
// 与全部 3 个数据组各写一键，三个节点的 store 全部可读同值——复制内核
// 端到端（TCP 传输、Ready 契约、盲 apply、applied 原子）一次性验证。
func TestClusterReplicateAllGroups(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for g := uint32(0); g < 4; g++ {
		lead := tc.leaderOf(t, g) // 内部 WaitLeader 语义，超时 Fatal
		r := replication.NewCluster(tc.mgrs[lead])
		b := tc.stores[lead].NewBatch()
		key := fmt.Sprintf("msg/it/g%d", g)
		if err := b.Set([]byte(key), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := r.Apply(ctx, g, b); err != nil {
			t.Fatalf("组 %d apply: %v", g, err)
		}
	}
	tc.waitConverged(t, []string{"msg/it/g0", "msg/it/g1", "msg/it/g2", "msg/it/g3"}, 30*time.Second)
}

// TestClusterKillLeaderWriteContinues 摘除组 1 的 leader 后，幸存两节点
// 重选出新 leader，写入继续成功且在两个存活节点可读——切换语义内核部分。
func TestClusterKillLeaderWriteContinues(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	old := tc.leaderOf(t, 1)
	tc.kill(t, old)
	newLead := tc.leaderOfExcluding(t, 1, old) // 等到 leader ≠ old 且 ≠ 0
	b := tc.stores[newLead].NewBatch()
	_ = b.Set([]byte("msg/it/after-failover"), []byte("v"))
	if err := replication.NewCluster(tc.mgrs[newLead]).Apply(ctx, 1, b); err != nil {
		t.Fatalf("切换后写入: %v", err)
	}
	tc.waitConvergedOn(t, tc.aliveIDs(), []string{"msg/it/after-failover"}, 30*time.Second)
}

// TestClusterProposeOnFollowerFails follower 上直接 Propose 必须报错——
// batch③ 把这个错误翻译成客户端可重试码，内核先保证它不静默转发不假成功。
func TestClusterProposeOnFollowerFails(t *testing.T) {
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
	if err := tc.mgrs[follower].Propose(ctx, 1, []byte("x")); err == nil {
		t.Fatal("follower Propose 应报错，得到 nil")
	}
}

// TestClusterUncleanNodeRejoinsAsLearner 断电节点经 WipeForRejoin +
// harness 编排（存活 leader Remove→AddLearner→追平→AddNode）回归，
// 已 commit 数据一条不丢——spike Task 5 流程在真实存储/传输上的复现。
// 编排自动化是 batch③；本测试同时是那套编排逻辑的行为规范。
func TestClusterUncleanNodeRejoinsAsLearner(t *testing.T) {
	tc := newTestCluster(t, AckQuorumMem)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	lead := tc.leaderOf(t, MetaGroup)
	writeN := func(from, n int) { // 经 meta 组写 n 键
		for i := from; i < from+n; i++ {
			b := tc.stores[tc.leaderOf(t, MetaGroup)].NewBatch()
			_ = b.Set([]byte(fmt.Sprintf("meta/topic/t%04d", i)), []byte("v"))
			if err := replication.NewCluster(tc.mgrs[tc.leaderOf(t, MetaGroup)]).Apply(ctx, MetaGroup, b); err != nil {
				t.Fatalf("写 %d: %v", i, err)
			}
		}
	}
	writeN(0, 100)
	var victim uint64
	for id := range tc.mgrs {
		if id != lead {
			victim = id
			break
		}
	}
	tc.kill(t, victim) // 不写标记 = 断电
	writeN(100, 100)   // 2/3 照常
	tc.rejoinAsLearner(t, ctx, victim) // harness：关旧 store→WipeForRejoin→全组 Remove/AddLearner→新 store+Manager fresh 启动→追平→AddNode
	tc.waitConverged(t, []string{"meta/topic/t0000", "meta/topic/t0099", "meta/topic/t0100", "meta/topic/t0199"}, 60*time.Second)
}
```

- [ ] **Step 2: 确认失败** — harness 未定义，编译失败。

- [ ] **Step 3: 实现 harness** — `newTestCluster`：先 `net.Listen("tcp", "127.0.0.1:0")` ×3 收集地址拼 Peers 表，再逐节点 `store.Open(t.TempDir()/id, false, ...)` + `NewManager`（Listener 注入）+ `Start`；`t.Cleanup` 逆序收尾（先 StopClean 存活节点等 Done，再关 store）。`kill` = 该节点测试后门 cancel（Manager.kill()）+ 记入 killed 集。`waitConverged` = 轮询每个存活 store `Get` 全部 keys 相同非空，超时 Fatal 附各节点缺失键。`rejoinAsLearner` 按测试注释里的步骤序实现，逐步 `t.Logf`。

- [ ] **Step 4: 跑测试** — Run: `go test -race ./internal/cluster/ -v -timeout 600s ; echo "EXIT=$?"` → EXIT=0。四个场景日志肉眼过一遍：leader 变更、断线重连、learner 追齐路径的日志线索齐不齐（这是 batch④ 场景测试要依赖的观测面）。

- [ ] **Step 5: 全量回归 + Linux 验证** — macOS：`go test -race ./... ; echo "EXIT=$?"` → EXIT=0。Linux（一等验证平台）：`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -race -o /tmp/cluster.test ./internal/cluster/` 后 scp 到 100.90.99.61（或 47.80.240.57 若还在）跑 `./cluster.test -test.v -test.timeout 600s`——**本地交叉编译，不装远端工具链**；结果记录在 commit message。

- [ ] **Step 6: Commit**

```bash
git add internal/cluster/cluster_test.go
git commit -m "test(cluster): 三节点集成——复制收敛、kill-leader、learner 重入（B8.2）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: 收尾自检与交棒

- [ ] **Step 1: 收尾自检（verification-before-completion + instrumenting-code 清单）** — `go vet ./... ; echo "EXIT=$?"` → 0；`go test -race ./... ; echo "EXIT=$?"` → 0；`grep -rn "pebble.Batch" internal/ cmd/ --include="*.go" | grep -v "internal/store/"` 仍为空（cluster 层没引入裸批次）；新文件头/导出注释齐；错误分支带上下文、成功路径不静默、热循环降 Debug。

- [ ] **Step 2: backlog 证据更新** — B8.2 行「变更痕迹」补一句：「08-08 batch②（复制内核）完成：internal/cluster + internal/replication 入库，三节点集成 -race 全绿（macOS+Linux）；协议面接线/升级路径为 batch③」。状态保持 🔨 doing（B8.2 整体未完）。

```bash
git add docs/superpowers/backlog.md
git commit -m "docs(backlog): B8.2 batch② 复制内核完成痕迹

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 3: 交棒** — 走 superpowers:finishing-a-development-branch（合并决策）。batch③ 范围备忘（写 plan 时展开）：core 34 写点接 Replicator + 组路由传参、读路径 leader 门控、delay/retention/redelivery/txn checker 的 leader-only 定时器、QueryRoute 指向组 leader + 可重试错误码、config 集群段（含 raft_listen）、learner 重入自动编排（跨节点 join 协议）、leader 摊布策略、单机→集群升级、e2e 官方 SDK 验证；batch④：快照与日志截断、集群场景测试（B8.3）。

---

## Self-Review 记录

- **Spec 覆盖**：本批对应 spec §4 拦截点升级（Replicator 两后端）、§5 存储布局全部（共库共 WAL、applied 原子、异步档刷盘；截断显式外推 batch④ 并在 Global Constraints 记录）、§2.2 两档确认与不干净重启拒绝恢复（重入编排原语 + harness 验证，自动编排外推 batch③）。§3 组拓扑就位（4 组默认、hash 归组、TransferLeader 原语）；§6/§7 协议面与升级完全不碰——batch③。
- **占位符扫描**：Task 5 golden 表初值是「首次实现输出回填」流程而非占位（回填动作是该 step 的显式要求）；Task 4/7 的 helper 以文字给出完整构造序；移植类 step 全部指向仓内已提交的 spike 文件并列明逐条差异，无「similar to」悬空引用。
- **类型一致性**：`AckMode`/`AckQuorumFsync`/`AckQuorumMem` 全文统一；`Propose(ctx, g uint32, batchRepr []byte)` 在 Task 4（group.propose）/5（Manager.Propose）/6（Cluster.Apply 调用）三处签名对齐；`store.ApplyWith(b, sync)` 的调用方（raftstore.Persist、group apply、flusher）参数序一致；`confStateFromEntries(ents, commit)` 定义（Task 2）与使用（Task 5）一致。
- **风险自记**：① raftpb v3.7.0 在共库场景与 pebble 无交集，spike 已证依赖可共存，但主模块引入后 `go mod tidy` 的间接依赖膨胀需在 Task 2 提交时人查一眼 diff；② apply-panic 策略（分叉即死）是刻意从简，batch③ 接真实流量前要不要降级为「组停摆 + 健康上报」在 batch③ plan 里重议。
