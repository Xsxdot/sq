# V2 batch④：快照、日志截断与扩容 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让三节点集群能长期运行且可扩容——raft 日志按 applied 位点截断（不再无界增长）、落后过多或全新的节点靠**真实状态快照**追齐、单机→多节点扩容打通，并关掉 batch②/③ 遗留的 leadership 迁移窗口与四项清洁债。

**Architecture:** FSM 是磁盘型（共享 Pebble），所以快照**不做全量序列化进 raft 消息**：`Storage.Snapshot()` 现场生成一份「描述符」（`[snapID][leader nodeID][index]`，几十字节）随 MsgSnap 走 raft 通道，真实状态由接收方经控制通道 `OpFetchSnapshot` **分块拉取**——发送侧用 Pebble 一致性读视图（`ReadView`）钉住 index 时刻的状态，接收侧带「安装中」标记落盘、崩溃可重来。组的键族归属沿用 batch③ 的归属纪律：组 0 是连续前缀（`meta/ delay/ half/ halfidx/ delayalloc handle_secret`），数据组是 `GroupForQueue` 哈希散布的键（`msg/ alloc/ keyidx/ cursor/ inflight/`），枚举器按组过滤。截断（`Compact` + `raft/<g>/ent/` range delete）与快照元数据（`raft/<g>/snap`）是同一个动作的两面：截断前必须先落一份 `{Index,Term,ConfState}`，否则重启后 `FirstIndex-1` 的 term 不可查、raft 拒启。**ConfState 改为随 `ApplyConfChange` 返回值持久化**（`raft/<g>/conf`），从根上替掉 batch② 那套「重启时重放 ConfChange 条目合成成员表」的临时办法——截断之后前缀条目已不存在，重放路径本就不再成立。

**Tech Stack:** 现有 internal/cluster（batch②③，已评审通过）+ internal/replication + etcd raft v3.7.0（`raft.Storage` 接口 / `MemoryStorage.CreateSnapshot`/`Compact`）+ Pebble v2.1.6（`DB.NewSnapshot` 一致性读视图）+ 官方 rocketmq-clients Go SDK v5.1.4（e2e）。

## Global Constraints

- **单机默认不回归**：不写 `cluster:` 配置段 = 今天的路径。本批新增的截断/快照全部挂在集群后端下，单机零新开销；`go test -race ./...` 全绿是底线，`test/e2e` 单机用例断言不许动。
- **截断安全下界不可放宽**：任何节点截断到 `applied - RetainEntries`；**leader 额外受 `min(Progress.Match)` 约束**。截断把日志删到落后 follower 需要的位点之下是允许的（有快照兜底），但那会把一次心跳级追齐放大成整组状态传输——安全下界是性能纪律，不是正确性开关，不许为「跑得快」调掉。
- **快照一致性靠 ReadView，不靠加锁停写**：`Storage.Snapshot()` 现场取 `applied` 与同一时刻的 `ReadView`，二者必须在同一临界区内取（先取 ReadView 再读 applied 会拿到更新的位点 → 快照声称的 index 高于实际内容 = 静默丢数据）。
- **接收侧安装非原子**：状态跨多个批次写入，必须先落「安装中」标记再写数据，标记消费在 applied 位点写入之后。崩溃在中间 = 重启时该组状态判定为不完整 → 清空该组键族重来，**绝不允许**把半截状态当完整状态启动。
- **跨组无原子性承诺**（沿用 batch③）：跨组两段式一律先写目标、后删来源，方向永远不许反。
- Linux 是一等验证平台：远程验证一律本地交叉编译（`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c`）+ scp 到 100.90.99.61（或 47.80.240.57 若还在），**不装远端工具链**；本批起远端固定加 `-race` 且跑全量 `./internal/...`（batch③ 评审 m3）。结果记入 commit message。`GOPROXY=https://goproxy.cn,direct`。
- 全部 commit message 以 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` 结尾。
- 每个实现类 task 内含「加关键节点日志」与「加注释」step（instrumenting-code）；`logger`/`slog`，禁 `fmt.Printf`。

## 本批不做（显式外推 batch⑤）

- **B8.3 集群场景测试**（kill -9 / 网络分区 / 滚动重启 / 断电模拟 + 毒消息→DLQ + producer 提交后立退）：依赖本批的截断能力才能跑长时压测，本批只交付「截断生效 + 扩容成功」两条 e2e 证据。
- **admin 控制台集群视图与 follower 写转发 UX**：前端形态未过 prototype，属独立子系统。
- **read-index / apply barrier 完整读屏障**：本批只关 leadership 迁移的写侧窗口（Task 9），线性一致读是 V3 议题。

## 键布局增量（在 batch② 的 `raft/` 布局上追加）

| 键 | 内容 | 写入者 |
|---|---|---|
| `raft/<g>/snap` | `raftpb.SnapshotMetadata` protobuf（`{Index,Term,ConfState}`） | 截断前（Task 3/8）、快照安装完成时（Task 7） |
| `raft/<g>/conf` | `raftpb.ConfState` protobuf | 每次 `ApplyConfChange` 后（Task 2） |
| `raft/<g>/snapinstall` | 安装中标记：目标 `SnapshotMetadata` protobuf | 安装开始（Task 7），安装完成时同批删除 |

---

### Task 1: store 一致性读视图 ReadView

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go`（已有文件，追加用例）

**Interfaces:**
- Produces: `func (s *Store) NewReadView() *ReadView`；`type ReadView struct{ ... }`；`func (v *ReadView) Get(key []byte) ([]byte, bool, error)`；`func (v *ReadView) Scan(lower, upper []byte, limit int, fn func(k, v []byte) (bool, error)) error`；`func (v *ReadView) Close() error`

- [ ] **Step 1: 写失败测试**

```go
func TestReadViewIsolatesLaterWrites(t *testing.T) {
	st, err := store.Open(t.TempDir(), false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	b := st.NewBatch()
	if err := b.Set([]byte("k/1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}

	view := st.NewReadView()
	defer view.Close()

	// 视图建立之后的写入不得被视图看见
	b2 := st.NewBatch()
	if err := b2.Set([]byte("k/2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := b2.Set([]byte("k/1"), []byte("v1-new")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b2); err != nil {
		t.Fatal(err)
	}

	got, ok, err := view.Get([]byte("k/1"))
	if err != nil || !ok || string(got) != "v1" {
		t.Fatalf("视图内 k/1 = %q ok=%v err=%v; want \"v1\"", got, ok, err)
	}
	if _, ok, _ := view.Get([]byte("k/2")); ok {
		t.Fatal("视图不得看见建立之后写入的 k/2")
	}
	var keys []string
	if err := view.Scan([]byte("k/"), store.PrefixUpperBound([]byte("k/")), 0,
		func(k, _ []byte) (bool, error) { keys = append(keys, string(k)); return true, nil }); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "k/1" {
		t.Fatalf("视图 Scan = %v; want [k/1]", keys)
	}
	// 主库读到的是最新值（视图不影响主库）
	got, _, _ = st.Get([]byte("k/1"))
	if string(got) != "v1-new" {
		t.Fatalf("主库 k/1 = %q; want v1-new", got)
	}
}

func TestReadViewCloseIsIdempotent(t *testing.T) {
	st, err := store.Open(t.TempDir(), false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	view := st.NewReadView()
	if err := view.Close(); err != nil {
		t.Fatalf("首次 Close: %v", err)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("重复 Close 必须无害（安装失败路径会 defer Close + 显式 Close）: %v", err)
	}
	if _, _, err := view.Get([]byte("k")); err == nil {
		t.Fatal("Close 后读取必须报错，不得静默返回空")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/store/ -run TestReadView -v`
Expected: FAIL，`st.NewReadView undefined`

- [ ] **Step 3: 实现 ReadView**

```go
// ReadView 是 store 的一致性只读视图：建立瞬间之后的写入一律不可见。
//
// 用途：raft 快照发送侧要把「index 时刻的 FSM 状态」流式发给对端，
// 而 FSM 仍在前进——视图把那一瞬间钉住，无需停写。
//
// 注意：
//   - 持有视图会阻止 Pebble 回收被覆盖的旧版本，长期不关会撑大磁盘。
//     调用方必须限时持有（快照注册表按 TTL 回收，见 cluster/snapshot.go）。
//   - Close 幂等；Close 之后一切读取返回错误，不静默返回空。
type ReadView struct {
	snap   *pebble.Snapshot
	closed atomic.Bool
}

// NewReadView 建立一致性只读视图。调用方负责 Close。
func (s *Store) NewReadView() *ReadView {
	return &ReadView{snap: s.db.NewSnapshot()}
}

// Get 从视图读一个键；语义与 Store.Get 相同（不存在时 ok=false）。
func (v *ReadView) Get(key []byte) ([]byte, bool, error) {
	if v.closed.Load() {
		return nil, false, errors.New("store: 读视图已关闭")
	}
	val, closer, err := v.snap.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store 读视图 Get %q: %w", key, err)
	}
	out := append([]byte(nil), val...)
	if err := closer.Close(); err != nil {
		return nil, false, fmt.Errorf("store 读视图 Get 释放 %q: %w", key, err)
	}
	return out, true, nil
}

// Scan 在视图内按键升序遍历 [lower, upper)；语义与 Store.Scan 一致
// （fn 返回 false 提前停止，limit>0 时限量）。
func (v *ReadView) Scan(lower, upper []byte, limit int, fn func(k, val []byte) (bool, error)) error {
	if v.closed.Load() {
		return errors.New("store: 读视图已关闭")
	}
	it, err := v.snap.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return fmt.Errorf("store 读视图 Scan 建迭代器: %w", err)
	}
	defer it.Close()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		cont, ferr := fn(it.Key(), it.Value())
		if ferr != nil {
			return ferr
		}
		n++
		if !cont || (limit > 0 && n >= limit) {
			break
		}
	}
	return it.Error()
}

// Close 释放视图（幂等）。
func (v *ReadView) Close() error {
	if v.closed.Swap(true) {
		return nil
	}
	return v.snap.Close()
}
```

- [ ] **Step 4: 加关键节点日志与注释**

- `NewReadView` 不打日志（高频、无分支）；**持有与回收的日志在快照注册表层打**（Task 4），那里才有 snapID/组号/存活时长这些能定位问题的字段。
- 文件头注释追加一行：`store.go` 职责段补「一致性只读视图（ReadView）供快照发送侧钉住某一时刻状态」。
- `ReadView` 的类型注释必须写明「长期持有会撑大磁盘」这条边界——这是它最容易被误用的地方。

- [ ] **Step 5: 运行测试并提交**

Run: `go test -race ./internal/store/ -v`
Expected: PASS

```bash
git add internal/store/store.go internal/store/store_test.go
git commit -m "feat(store): 一致性只读视图 ReadView——快照发送侧钉住 index 时刻状态

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: ConfState 持久化——替掉重放合成，补上 ConfChange 的 applied 缺口

**Files:**
- Modify: `internal/cluster/raftstore.go`（新增 SaveConfState/LoadConfState；`confStateFromEntries` 降级为**仅**旧数据目录迁移路径）
- Modify: `internal/cluster/group.go`（`ApplyConfChange` 返回值持久化 + applied 位点同批写）
- Modify: `internal/cluster/manager.go:288 buildGroup`（优先读 `raft/<g>/conf`）
- Test: `internal/cluster/raftstore_test.go`、`internal/cluster/cluster_test.go`

**Interfaces:**
- Consumes: Task 1 无依赖（本任务与 Task 1 可并行）
- Produces: `func (r *raftStore) SaveConfState(g uint32, cs *raftpb.ConfState, applied uint64) error`；`func (r *raftStore) LoadConfState(g uint32) (*raftpb.ConfState, bool, error)`

- [ ] **Step 1: 写失败测试**

```go
// TestConfStateSurvivesRestartWithoutEntries batch④ 前置：截断之后
// ConfChange 条目已不在日志里，重启不可能再靠重放合成成员表——
// 成员表必须自己持久化。
func TestConfStateSurvivesRestartWithoutEntries(t *testing.T) {
	st := openTestStore(t)
	rs := newRaftStore(st, testSlog(t))

	cs := &raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	if err := rs.SaveConfState(7, cs, 42); err != nil {
		t.Fatalf("SaveConfState: %v", err)
	}
	got, ok, err := rs.LoadConfState(7)
	if err != nil || !ok {
		t.Fatalf("LoadConfState ok=%v err=%v", ok, err)
	}
	if len(got.Voters) != 2 || got.Voters[0] != 1 || got.Voters[1] != 2 || len(got.Learners) != 1 || got.Learners[0] != 3 {
		t.Fatalf("成员表 = %+v; want voters[1 2] learners[3]", got)
	}
	// applied 与成员表同批写入：ConfChange 条目 apply 后 applied 位点
	// 必须一起落盘，否则重启会重放该条 ConfChange（batch③ 遗留缺口）
	ap, err := rs.Applied(7)
	if err != nil || ap != 42 {
		t.Fatalf("applied = %d err=%v; want 42", ap, err)
	}
	if _, ok, _ := rs.LoadConfState(8); ok {
		t.Fatal("未写过的组不得返回成员表")
	}
}

// TestConfChangeAdvancesPersistedApplied 端到端证明缺口已补：
// 单节点组提一条 AddLearner，重启后 applied 位点不回退。
func TestConfChangeAdvancesPersistedApplied(t *testing.T) {
	h := startSingleNodeManager(t) // 既有测试辅助：单节点 Manager + 1 组
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.m.ProposeConfChange(ctx, 0, raftpb.ConfChangeAddLearnerNode, 9); err != nil {
		t.Fatalf("提 AddLearner: %v", err)
	}
	before, err := h.m.rs.Applied(0)
	if err != nil {
		t.Fatal(err)
	}
	if before == 0 {
		t.Fatal("ConfChange apply 后磁盘 applied 位点仍为 0——缺口未补")
	}
	cs, ok, err := h.m.rs.LoadConfState(0)
	if err != nil || !ok {
		t.Fatalf("成员表未持久化 ok=%v err=%v", ok, err)
	}
	if len(cs.Learners) != 1 || cs.Learners[0] != 9 {
		t.Fatalf("成员表 learners = %v; want [9]", cs.Learners)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/cluster/ -run 'TestConfState|TestConfChangeAdvances' -v`
Expected: FAIL，`rs.SaveConfState undefined`

- [ ] **Step 3: 实现**

`raftstore.go` 追加（键 `raft/<g>/conf`，常量 `groupConfFmt = "raft/%d/conf"`）：

```go
// SaveConfState 持久化一组的成员表，并与 applied 位点**同批**写入。
//
// 为什么同批：ConfChange 条目 apply 后若只更新内存 applied、不落盘，
// 重启会从旧位点重放该条 ConfChange——raft 的 ApplyConfChange 本身幂等，
// 但成员表来源一旦改成持久化值，重放就会用「旧成员表 + 重放变更」
// 二次叠加，与 leader 的实际成员表分叉（batch③ 遗留缺口）。
//
// 参数：
//   - g: 数据组号
//   - cs: rn.ApplyConfChange 的返回值（raft 库算出的权威成员表）
//   - applied: 本条 ConfChange 条目的 index
func (r *raftStore) SaveConfState(g uint32, cs *raftpb.ConfState, applied uint64) error {
	data, err := proto.Marshal(cs)
	if err != nil {
		return fmt.Errorf("raftstore SaveConfState 组 %d 编码: %w", g, err)
	}
	b := r.st.NewBatch()
	if err := b.Set(confKey(g), data); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveConfState 组 %d 写成员表: %w", g, err)
	}
	if err := b.Set(appliedKey(g), store.PutU64(applied)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveConfState 组 %d 写 applied: %w", g, err)
	}
	// 成员表是选举安全的根：Sync 落盘，不进 quorum-mem 的异步刷盘队列
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveConfState 组 %d: %w", g, err)
	}
	r.lg.Info("成员表已持久化", "g", g, "voters", cs.GetVoters(), "learners", cs.GetLearners(), "applied", applied)
	return nil
}

// LoadConfState 读回一组的成员表；从未写入过时 ok=false（调用方回退到
// confStateFromEntries 的日志重放路径，见 buildGroup）。
func (r *raftStore) LoadConfState(g uint32) (*raftpb.ConfState, bool, error) {
	data, ok, err := r.st.Get(confKey(g))
	if err != nil {
		return nil, false, fmt.Errorf("raftstore LoadConfState 组 %d: %w", g, err)
	}
	if !ok {
		return nil, false, nil
	}
	cs := &raftpb.ConfState{}
	if err := proto.Unmarshal(data, cs); err != nil {
		return nil, false, fmt.Errorf("raftstore LoadConfState 组 %d 解码: %w", g, err)
	}
	return cs, true, nil
}
```

`group.go` 两个 ConfChange 分支：`ApplyConfChange` 的返回值改为接收并持久化。V2 分支：

```go
		case raftpb.EntryConfChangeV2:
			var v2 raftpb.ConfChangeV2
			v2.Reset()
			if err := proto.Unmarshal(ent.Data, &v2); err != nil {
				gr.lg.Error("ConfChangeV2 解码失败，组停摆", "index", ent.GetIndex(), "err", err)
				panic(err)
			}
			cs := gr.rn.ApplyConfChange(&v2)
			// 成员表 + applied 同批落盘：截断之后日志前缀不复存在，
			// 重启只能靠这份持久化成员表恢复（Task 3 的截断前提）
			if err := gr.rs.SaveConfState(gr.g, cs, ent.GetIndex()); err != nil {
				gr.lg.Error("成员表持久化失败，组停摆", "index", ent.GetIndex(), "err", err)
				panic(err)
			}
			gr.confState.Store(cs) // Storage.Snapshot() 现场取用（Task 4）
			ccid, ours := ccWaiterInfo(&v2, gr.selfID)
			appliedCC = append(appliedCC, ccApplied{id: ccid, notify: ours})
			gr.applied.Store(ent.GetIndex())
			...
```

V1 分支同样接收返回值并 `SaveConfState`（旧日志遗留格式，路径罕见但不能只更内存）。`group` 结构体加 `confState atomic.Pointer[raftpb.ConfState]`，`newGroup` 用 `rs.LoadConfState` 初始化（`rs` 为 nil 的单元测试路径跳过）。

`manager.go buildGroup` 的 clean 分支：

```go
		// 成员表优先取持久化值（Task 2）：截断之后日志前缀已被删，
		// 重放合成不再可能。仅当从未持久化过（batch③ 及更早的数据
		// 目录首次升级到本版本）才回退到日志重放合成。
		cs, ok, err := m.rs.LoadConfState(g)
		if err != nil {
			return nil, fmt.Errorf("cluster: 组 %d 读成员表: %w", g, err)
		}
		if !ok {
			cs, err = confStateFromEntries(ents, hs.GetCommit())
			if err != nil {
				return nil, fmt.Errorf("cluster: 组 %d 成员表合成: %w", g, err)
			}
			m.lg.Info("成员表由日志重放合成（旧数据目录首次升级）", "g", g,
				"voters", cs.GetVoters(), "learners", cs.GetLearners())
		}
```

`confStateFromEntries` 的注释改写：说明它已降级为「旧数据目录一次性迁移路径」，并补上 batch② 遗留的多变更守卫——`len(ch) > 1` 时不再只取 `ch[0]`，而是遍历全部变更（本进程从不产生联合共识条目，但旧日志与将来的 joint 提案都可能带多条，只取首条会漏掉后续变更）：

```go
			ch := v2.GetChanges()
			if len(ch) == 0 {
				continue // leave-joint 类空条目：不改变单代成员表
			}
			for _, c := range ch {
				applyOne(cs, c.GetType(), c.GetNodeId())
			}
			continue
```

- [ ] **Step 4: 加关键节点日志与注释**

- `SaveConfState` 成功打 Info（组号 + voters + learners + applied）——成员表变更是排查「谁在组里」的第一现场，必须能 grep。
- `buildGroup` 回退到日志重放时打 Info 并注明「旧数据目录首次升级」，正常路径不打（每次启动都打会淹没日志）。
- 持久化失败一律 panic 并先打 Error（与既有 `Persist` 失败同类：状态与多数派分叉，静默停摆比 panic 更糟）。
- `confStateFromEntries` 的文档注释改写为「降级为迁移路径」，并写明多变更遍历的理由。

- [ ] **Step 5: 运行测试并提交**

Run: `go test -race ./internal/cluster/ -run 'ConfState|ConfChange' -v && go test -race ./internal/cluster/`
Expected: PASS

```bash
git add internal/cluster/
git commit -m "feat(cluster): ConfState 持久化替代重放合成，补 ConfChange 的 applied 落盘缺口

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: raftstore 快照元数据与日志截断

**Files:**
- Modify: `internal/cluster/raftstore.go`
- Test: `internal/cluster/raftstore_test.go`

**Interfaces:**
- Consumes: Task 2 的 `confKey`/常量风格
- Produces: `func (r *raftStore) SaveSnapMeta(g uint32, meta *raftpb.SnapshotMetadata) error`；`func (r *raftStore) LoadSnapMeta(g uint32) (*raftpb.SnapshotMetadata, bool, error)`；`func (r *raftStore) TruncateLog(g uint32, upto uint64) error`；`Load` 语义变更：返回 `(hs, ents, snapMeta, err)`

- [ ] **Step 1: 写失败测试**

```go
// TestTruncateLogKeepsSuffixAndSnapMeta 截断的两条不变量：
// ① 截断点之后的条目一条不少；② 截断点的 {Index,Term} 必须留在
// snap 元数据里——raft 重启要查 FirstIndex-1 的 term，查不到就拒启。
func TestTruncateLogKeepsSuffixAndSnapMeta(t *testing.T) {
	st := openTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	var ents []*raftpb.Entry
	for i := uint64(1); i <= 10; i++ {
		idx, term := i, uint64(3)
		ents = append(ents, &raftpb.Entry{Index: &idx, Term: &term, Data: []byte{byte(i)}})
	}
	commit := uint64(10)
	if err := rs.Persist(1, &raftpb.HardState{Commit: &commit}, ents, true); err != nil {
		t.Fatal(err)
	}

	idx, term := uint64(6), uint64(3)
	meta := &raftpb.SnapshotMetadata{Index: &idx, Term: &term,
		ConfState: &raftpb.ConfState{Voters: []uint64{1}}}
	if err := rs.SaveSnapMeta(1, meta); err != nil {
		t.Fatal(err)
	}
	if err := rs.TruncateLog(1, 6); err != nil {
		t.Fatal(err)
	}

	_, got, gotMeta, err := rs.Load(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0].GetIndex() != 7 || got[3].GetIndex() != 10 {
		t.Fatalf("截断后条目 = %d 条，首 %d 末 %d; want 4 条 [7,10]",
			len(got), got[0].GetIndex(), got[len(got)-1].GetIndex())
	}
	if gotMeta == nil || gotMeta.GetIndex() != 6 || gotMeta.GetTerm() != 3 {
		t.Fatalf("snap 元数据 = %+v; want index=6 term=3", gotMeta)
	}
	if len(gotMeta.GetConfState().GetVoters()) != 1 {
		t.Fatalf("snap 元数据必须带成员表（重启 InitialState 用）: %+v", gotMeta.GetConfState())
	}
}

// TestTruncateLogRejectsAboveSnapMeta 截断点不得越过已落盘的 snap 元数据：
// 越过意味着 FirstIndex-1 的 term 无处可查，重启必然拒启。
func TestTruncateLogRejectsAboveSnapMeta(t *testing.T) {
	st := openTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	idx, term := uint64(5), uint64(2)
	if err := rs.SaveSnapMeta(1, &raftpb.SnapshotMetadata{Index: &idx, Term: &term}); err != nil {
		t.Fatal(err)
	}
	if err := rs.TruncateLog(1, 9); err == nil {
		t.Fatal("截断点 9 > snap 元数据 index 5，必须被拒绝")
	}
}

// TestTruncateLogIsIdempotent 重复截断到同一位点无害（周期截断会撞上）。
func TestTruncateLogIsIdempotent(t *testing.T) {
	st := openTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	idx, term := uint64(3), uint64(1)
	if err := rs.SaveSnapMeta(1, &raftpb.SnapshotMetadata{Index: &idx, Term: &term}); err != nil {
		t.Fatal(err)
	}
	if err := rs.TruncateLog(1, 3); err != nil {
		t.Fatal(err)
	}
	if err := rs.TruncateLog(1, 3); err != nil {
		t.Fatalf("重复截断必须无害: %v", err)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/cluster/ -run TestTruncateLog -v`
Expected: FAIL，`rs.SaveSnapMeta undefined`

- [ ] **Step 3: 实现**

```go
// SaveSnapMeta 持久化一组的快照元数据（截断锚点）。
//
// 元数据是截断的前提而非结果：raft 重启时用 FirstIndex-1 的 term 做
// 任期比较，条目一旦删掉，那个 term 只能从这里查。因此顺序恒为
// 「先 SaveSnapMeta（Sync）、后 TruncateLog」——反过来会在两次写之间
// 留下「条目已删、锚点未落」的崩溃窗口，重启直接拒启。
func (r *raftStore) SaveSnapMeta(g uint32, meta *raftpb.SnapshotMetadata) error {
	data, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("raftstore SaveSnapMeta 组 %d 编码: %w", g, err)
	}
	b := r.st.NewBatch()
	if err := b.Set(snapKey(g), data); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveSnapMeta 组 %d: %w", g, err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveSnapMeta 组 %d: %w", g, err)
	}
	r.lg.Info("快照锚点已落盘", "g", g, "index", meta.GetIndex(), "term", meta.GetTerm())
	return nil
}

// LoadSnapMeta 读回快照元数据；从未截断过时 ok=false。
func (r *raftStore) LoadSnapMeta(g uint32) (*raftpb.SnapshotMetadata, bool, error) { ... }

// TruncateLog 删除 index ≤ upto 的日志条目（Pebble range delete）。
//
// 前置：upto ≤ 已落盘 snap 元数据的 Index（锚点必须先落盘，见
// SaveSnapMeta）。违反即报错拒绝执行——这是「先锚点后截断」顺序的
// 编译期之外的运行期守卫。
//
// 幂等：重复截断到同一位点是 range delete 的无操作，周期截断会撞上。
func (r *raftStore) TruncateLog(g uint32, upto uint64) error {
	meta, ok, err := r.LoadSnapMeta(g)
	if err != nil {
		return err
	}
	if !ok || meta.GetIndex() < upto {
		return fmt.Errorf("raftstore TruncateLog 组 %d: 截断点 %d 越过快照锚点（锚点存在=%v, index=%d）——必须先 SaveSnapMeta",
			g, upto, ok, meta.GetIndex())
	}
	b := r.st.NewBatch()
	if err := b.DeleteRange(entKey(g, 0), entKey(g, upto+1)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore TruncateLog 组 %d 删条目(≤%d): %w", g, upto, err)
	}
	if err := r.st.ApplyWith(b, false); err != nil { // 截断丢了只是白留日志，无需 fsync
		return fmt.Errorf("raftstore TruncateLog 组 %d: %w", g, err)
	}
	r.lg.Info("raft 日志已截断", "g", g, "upto", upto)
	return nil
}
```

`Load` 签名改为 `(hs *raftpb.HardState, ents []*raftpb.Entry, snapMeta *raftpb.SnapshotMetadata, err error)`，内部多读一次 `LoadSnapMeta`（不存在时返回 nil）；调用点 `buildGroup` 同步改。

- [ ] **Step 4: 加关键节点日志与注释**

- `SaveSnapMeta` / `TruncateLog` 成功各打一条 Info（组号 + 位点）：截断是「日志为什么变小了」的唯一解释，没有这两条就只能靠猜。
- `TruncateLog` 的越界拒绝打 Error 并带上锚点与请求位点。
- 文件头注释的边界段删掉「不做日志截断与快照（batch④）」，改写为本批的实际能力与顺序契约。

- [ ] **Step 5: 运行测试并提交**

Run: `go test -race ./internal/cluster/ -run 'Truncate|SnapMeta' -v && go test -race ./internal/cluster/`
Expected: PASS

```bash
git add internal/cluster/
git commit -m "feat(cluster): raft 日志截断与快照锚点持久化（先锚点后截断的顺序守卫）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: groupStorage 与快照注册表——按需生成描述符

**Files:**
- Create: `internal/cluster/snapshot.go`
- Modify: `internal/cluster/group.go`（`raftConfig` 参数类型改 `raft.Storage`；`storage` 字段拆成 `mem *raft.MemoryStorage` + `stg raft.Storage`）
- Test: `internal/cluster/snapshot_test.go`

**Interfaces:**
- Consumes: Task 1 `store.ReadView`；Task 2 `gr.confState`
- Produces: `type snapRegistry struct{...}`；`func newSnapRegistry(st *store.Store, ttl time.Duration, lg *slog.Logger) *snapRegistry`；`func (r *snapRegistry) Create(g uint32, index uint64) (id uint64, view *store.ReadView)`；`func (r *snapRegistry) Get(id uint64) (*store.ReadView, bool)`；`func (r *snapRegistry) Release(id uint64)`；`func (r *snapRegistry) GCOnce(now time.Time) int`；`type snapDescriptor struct{ ID uint64; Leader uint64; Index uint64 }`；`func encodeSnapDescriptor(d snapDescriptor) []byte`；`func decodeSnapDescriptor(b []byte) (snapDescriptor, error)`；`type groupStorage struct{...}`（实现 `raft.Storage`）

- [ ] **Step 1: 写失败测试**

```go
// TestGroupStorageSnapshotIsConsistentWithIndex 快照的核心不变量：
// 描述符声称的 index 与 ReadView 看到的状态必须同一时刻——
// 先取视图后读 applied 会让快照声称的 index 高于内容（静默丢数据）。
func TestGroupStorageSnapshotIsConsistentWithIndex(t *testing.T) {
	st := openTestStore(t)
	mem := raft.NewMemoryStorage()
	idx, term := uint64(5), uint64(2)
	if err := mem.Append([]*raftpb.Entry{{Index: &idx, Term: &term}}); err != nil {
		t.Fatal(err)
	}
	var applied atomic.Uint64
	applied.Store(5)
	reg := newSnapRegistry(st, time.Minute, testSlog(t))
	var cs atomic.Pointer[raftpb.ConfState]
	cs.Store(&raftpb.ConfState{Voters: []uint64{1, 2, 3}})

	// applied 与 FSM 的对应关系：写入 k=5 后 applied=5
	b := st.NewBatch()
	if err := b.Set([]byte("msg/t/0/5"), []byte("v5")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}

	gs := newGroupStorage(1, mem, reg, &applied, &cs, 7 /*selfID*/, testSlog(t))
	snap, err := gs.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Metadata.GetIndex() != 5 || snap.Metadata.GetTerm() != 2 {
		t.Fatalf("快照元数据 = index %d term %d; want 5/2", snap.Metadata.GetIndex(), snap.Metadata.GetTerm())
	}
	if len(snap.Metadata.GetConfState().GetVoters()) != 3 {
		t.Fatalf("快照必须带当前成员表: %+v", snap.Metadata.GetConfState())
	}
	d, err := decodeSnapDescriptor(snap.Data)
	if err != nil {
		t.Fatalf("描述符解码: %v", err)
	}
	if d.Index != 5 || d.Leader != 7 {
		t.Fatalf("描述符 = %+v; want index 5 leader 7", d)
	}

	// 生成之后的写入不得进入这份快照
	b2 := st.NewBatch()
	if err := b2.Set([]byte("msg/t/0/6"), []byte("v6")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b2); err != nil {
		t.Fatal(err)
	}
	view, ok := reg.Get(d.ID)
	if !ok {
		t.Fatal("注册表里应有该视图")
	}
	if _, found, _ := view.Get([]byte("msg/t/0/6")); found {
		t.Fatal("快照视图看见了生成之后的写入——一致性被破坏")
	}
	reg.Release(d.ID)
	if _, ok := reg.Get(d.ID); ok {
		t.Fatal("Release 之后不得再取到")
	}
}

// TestSnapRegistryGCReclaimsStaleViews 视图长期不关会撑大磁盘：
// 超时未被拉完的必须被 GC 回收。
func TestSnapRegistryGCReclaimsStaleViews(t *testing.T) {
	st := openTestStore(t)
	reg := newSnapRegistry(st, 50*time.Millisecond, testSlog(t))
	id, _ := reg.Create(1, 10)
	base := time.Now()
	if n := reg.GCOnce(base); n != 0 {
		t.Fatalf("未超时不得回收，回收了 %d", n)
	}
	if n := reg.GCOnce(base.Add(time.Second)); n != 1 {
		t.Fatalf("超时应回收 1 个，回收了 %d", n)
	}
	if _, ok := reg.Get(id); ok {
		t.Fatal("回收后不得再取到")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/cluster/ -run 'GroupStorageSnapshot|SnapRegistry' -v`
Expected: FAIL，`newSnapRegistry undefined`

- [ ] **Step 3: 实现 snapshot.go**

文件头注释（职责/边界）+ 三块：

```go
// snapshot.go 提供 raft 快照的发送侧机件：按需生成的 raft.Storage 包装、
// 快照描述符编解码、以及钉住状态的读视图注册表。
//
// 职责：
//   - groupStorage：包装 MemoryStorage，把 Snapshot() 从「返回预置快照」
//     改为「现场生成」——取当前 applied 与同一时刻的 ReadView，
//     元数据带真实 {Index, Term, ConfState}，Data 只放几十字节描述符
//   - snapRegistry：按 snapID 持有 ReadView，供控制通道分块拉取；
//     TTL 到期强制回收（视图长期不关会阻止 Pebble 回收旧版本）
//   - 描述符编解码：[8B snapID][8B leader nodeID][8B index]
//
// 边界：
//   - 不搬运真实状态字节——那是 snapshotstream.go（Task 5）与控制通道
//     OpFetchSnapshot（Task 6）的事
//   - 不决定何时截断——那是 Manager 的截断循环（Task 8）
```

`groupStorage.Snapshot()` 的关键实现（顺序即不变量）：

```go
// Snapshot 现场生成一份快照描述符。raft 在需要给落后 follower 发
// MsgSnap 时调用本方法。
//
// 顺序不变量：**先取 applied、再建 ReadView** 是错的，反过来也是错的——
// 二者必须在同一临界区内取。实现上先加锁，读 applied，再建视图，
// 期间 apply 路径拿不到锁 → 视图内容恰好对应该 applied 位点。
// （若先建视图后读 applied，读到的位点可能已被后续 apply 推高，
// 快照就会声称包含它其实没有的数据 = 静默丢消息。）
func (s *groupStorage) Snapshot() (raftpb.Snapshot, error) {
	s.applyMu.Lock()
	index := s.applied.Load()
	cs := s.confState.Load()
	id, _ := s.reg.Create(s.g, index)
	s.applyMu.Unlock()

	term, err := s.mem.Term(index)
	if err != nil {
		// index 已被截断到不可查 term：本轮放弃，raft 下轮重试
		s.reg.Release(id)
		s.lg.Warn("快照生成放弃：applied 位点的 term 不可查", "g", s.g, "index", index, "err", err)
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	idx, tm := index, term
	snap := raftpb.Snapshot{
		Data: encodeSnapDescriptor(snapDescriptor{ID: id, Leader: s.selfID, Index: index}),
		Metadata: &raftpb.SnapshotMetadata{Index: &idx, Term: &tm, ConfState: cs},
	}
	s.lg.Info("快照描述符已生成", "g", s.g, "snap_id", id, "index", index, "term", term)
	return snap, nil
}
```

`applyMu` 由 group 持有并传入——apply 路径（`applyEntry` 写 FSM + 推 applied）在同一把锁内，二者互斥。**注意 apply 路径持锁时间必须短**：只覆盖「写批次 + Store(applied)」，不覆盖等待。

`groupStorage` 其余方法（`InitialState`/`Entries`/`Term`/`LastIndex`/`FirstIndex`）直通 `mem`。

- [ ] **Step 4: 加关键节点日志与注释**

- 描述符生成成功 Info（g/snap_id/index/term）；放弃走 Warn 带原因——「follower 追不上」排查时这两条是入口。
- 注册表 Create/Release/GC 各打 Debug/Info：GC 回收打 Info 并带存活时长（视图泄漏是磁盘涨的元凶，必须可观测）。
- `Snapshot()` 的顺序不变量注释按上文原样保留——它是本文件最容易被「优化」掉的地方。

- [ ] **Step 5: 运行测试并提交**

Run: `go test -race ./internal/cluster/ -run 'GroupStorage|SnapRegistry' -v && go test -race ./internal/cluster/`
Expected: PASS

```bash
git add internal/cluster/
git commit -m "feat(cluster): groupStorage 按需生成快照描述符 + 读视图注册表（TTL 回收）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: 组键族枚举与分块编解码

**Files:**
- Create: `internal/cluster/snapstream.go`
- Test: `internal/cluster/snapstream_test.go`

**Interfaces:**
- Consumes: Task 1 `store.ReadView`；`Manager.GroupForQueue` 的同款哈希（提取为包内 `groupForKey`）
- Produces: `func groupKeyRanges(g uint32) []keyRange`（组 0 的连续前缀集）；`func scanGroupKeys(view *store.ReadView, g uint32, groups uint32, from []byte, budget int, emit func(k, v []byte) error) (next []byte, done bool, err error)`；`func encodeChunk(pairs []kv) []byte`；`func decodeChunk(b []byte) ([]kv, error)`

- [ ] **Step 1: 写失败测试**

```go
// TestScanGroupKeysFiltersByGroup 数据组的键是哈希散布的：
// 枚举必须逐键判归属，绝不能按前缀整段搬（会把别组的数据搬过去）。
func TestScanGroupKeysFiltersByGroup(t *testing.T) {
	st := openTestStore(t)
	const groups = uint32(3)
	// 造 30 个队列的消息键，记录每个键的真实归属组
	want := map[uint32][]string{}
	b := st.NewBatch()
	for q := uint32(0); q < 30; q++ {
		k := store.MsgKey("T", q, 1)
		g := groupForQueue("T", q, groups)
		want[g] = append(want[g], string(k))
		if err := b.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	view := st.NewReadView()
	defer view.Close()

	for g := uint32(1); g < groups; g++ {
		var got []string
		from := []byte(nil)
		for {
			next, done, err := scanGroupKeys(view, g, groups, from, 4, func(k, _ []byte) error {
				got = append(got, string(k))
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if done {
				break
			}
			from = next
		}
		if len(got) != len(want[g]) {
			t.Fatalf("组 %d 枚举 %d 个键; want %d", g, len(got), len(want[g]))
		}
		for _, k := range got {
			if groupForQueueOfKey(t, k, groups) != g {
				t.Fatalf("组 %d 的枚举结果混入了别组的键 %q", g, k)
			}
		}
	}
}

// TestScanGroupKeysGroup0CoversGlobalPrefixes 组 0 的键族是全局连续前缀，
// 一个都不能漏——漏掉 half/ 就是事务状态丢失。
func TestScanGroupKeysGroup0CoversGlobalPrefixes(t *testing.T) {
	st := openTestStore(t)
	keys := [][]byte{
		store.TopicMetaKey("T"), store.GroupMetaKey("G"), store.HandleSecretKey(),
		store.DelayKey(1000, 1), store.DelayAllocKey(),
		store.HalfKey(1000, "tx1"), store.HalfIdxKey("tx1"),
	}
	b := st.NewBatch()
	for _, k := range keys {
		if err := b.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// metric/ 是本地不复制键族（batch③ 归属纪律），必须**不**出现在快照里
	if err := b.Set(store.MetricKey(1000), []byte("m")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	view := st.NewReadView()
	defer view.Close()

	got := map[string]bool{}
	from := []byte(nil)
	for {
		next, done, err := scanGroupKeys(view, 0, 3, from, 2, func(k, _ []byte) error {
			got[string(k)] = true
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		from = next
	}
	for _, k := range keys {
		if !got[string(k)] {
			t.Fatalf("组 0 快照漏掉全局键 %q", k)
		}
	}
	if got[string(store.MetricKey(1000))] {
		t.Fatal("metric/ 是本地不复制键族，不得进快照")
	}
}

func TestChunkRoundTrip(t *testing.T) {
	in := []kv{{k: []byte("a"), v: []byte("1")}, {k: []byte(""), v: nil}, {k: []byte("bb"), v: []byte("222")}}
	out, err := decodeChunk(encodeChunk(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("往返 %d 对; want %d", len(out), len(in))
	}
	for i := range in {
		if string(out[i].k) != string(in[i].k) || string(out[i].v) != string(in[i].v) {
			t.Fatalf("第 %d 对 = (%q,%q); want (%q,%q)", i, out[i].k, out[i].v, in[i].k, in[i].v)
		}
	}
	if _, err := decodeChunk([]byte{0, 0, 0, 9, 'x'}); err == nil {
		t.Fatal("坏块（声称长度超出剩余字节）必须报错")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/cluster/ -run 'ScanGroupKeys|ChunkRoundTrip' -v`
Expected: FAIL，`scanGroupKeys undefined`

- [ ] **Step 3: 实现**

- `groupKeyRanges(0)` 返回 `meta/`、`delay/`、`half/`、`halfidx/`、`delayalloc`（单键）五段；**不含** `metric/`（本地键族）与 `raft/`（日志区，不是 FSM）。
- `groupKeyRanges(g>0)` 返回 `msg/`、`alloc/`、`keyidx/`、`cursor/`、`inflight/` 五段，逐键用各自的 Parse 函数取出 `(topic, queueID)` 再 `groupForQueue` 判归属；解析失败的键**报错中止**（不是跳过）：快照漏键 = 静默丢数据，比拒绝生成快照严重得多。
- `from` 游标是「上次枚举到的最后一个键」，跨块续扫；`budget` 是每块的键数上限，`done` 表示全部范围扫完。
- 块编码：`[4B 键长][键][4B 值长][值]` 重复；解码逐段校验剩余长度。

- [ ] **Step 4: 加关键节点日志与注释**

- 枚举本身不打日志（每块几千键，打了就是刷屏）；**每块的汇总**由调用方（Task 6）打。
- 解析失败中止打 Error（组号 + 键的十六进制前缀 + 错误）——这是「快照为什么生成不出来」的唯一线索。
- 文件头注释写清「为什么数据组不能按前缀整段搬」：归属是哈希散布的，前缀搬运会跨组污染。

- [ ] **Step 5: 运行测试并提交**

Run: `go test -race ./internal/cluster/ -run 'ScanGroup|Chunk' -v`
Expected: PASS

```bash
git add internal/cluster/
git commit -m "feat(cluster): 组键族枚举器（哈希归属逐键过滤）与快照分块编解码

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: 控制通道 OpFetchSnapshot——发送侧

**Files:**
- Modify: `internal/cluster/transport.go`（新增 `OpFetchSnapshot byte = 4` 与协议注释）
- Modify: `internal/cluster/manager.go`（`handleFetchSnapshot` + 注册进 control 处理器）
- Test: `internal/cluster/cluster_test.go`

**Interfaces:**
- Consumes: Task 4 `snapRegistry`、Task 5 `scanGroupKeys`/`encodeChunk`
- Produces: `OpFetchSnapshot byte = 4`；请求 `[4B BE 组][8B BE snapID][4B BE 游标键长][游标键]`；响应 `[1B 是否结束][4B BE 下一游标键长][下一游标键][块字节]`；`func (m *Manager) handleFetchSnapshot(payload []byte) ([]byte, error)`

- [ ] **Step 1: 写失败测试**

```go
// TestFetchSnapshotStreamsGroupState 发送侧端到端：注册一份视图，
// 经控制通道分块拉完，内容与视图一致且不含别组键。
func TestFetchSnapshotStreamsGroupState(t *testing.T) {
	h := startThreeNodeCluster(t) // 既有辅助
	defer h.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 在组 0 写入若干全局键（经 leader 提案，三节点收敛）
	leader := h.leaderOf(t, 0)
	for i := 0; i < 50; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("T%02d", i)), []byte("meta")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}

	id, _ := leader.snaps.Create(0, leader.AppliedIndex(0))
	defer leader.snaps.Release(id)

	got := map[string]string{}
	cursor := []byte(nil)
	for i := 0; ; i++ {
		if i > 200 {
			t.Fatal("拉取不收敛（超过 200 块）")
		}
		resp, err := leader.handleFetchSnapshot(encodeFetchReq(0, id, cursor))
		if err != nil {
			t.Fatalf("第 %d 块: %v", i, err)
		}
		done, next, chunk := decodeFetchResp(t, resp)
		pairs, err := decodeChunk(chunk)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range pairs {
			got[string(p.k)] = string(p.v)
		}
		if done {
			break
		}
		cursor = next
	}
	if len(got) < 50 {
		t.Fatalf("拉到 %d 个键; want ≥50（50 个 topic 元数据）", len(got))
	}
	for k := range got {
		if !strings.HasPrefix(k, "meta/") && !strings.HasPrefix(k, "delay") &&
			!strings.HasPrefix(k, "half") {
			t.Fatalf("组 0 快照混入了非全局键 %q", k)
		}
	}
}

// TestFetchSnapshotRejectsUnknownID 过期/未知 snapID 必须显式报错，
// 不能返回空块让接收方以为「拉完了」（那是静默的空状态安装）。
func TestFetchSnapshotRejectsUnknownID(t *testing.T) {
	h := startThreeNodeCluster(t)
	defer h.stopAll()
	if _, err := h.nodes[0].handleFetchSnapshot(encodeFetchReq(0, 99999, nil)); err == nil {
		t.Fatal("未知 snapID 必须报错")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/cluster/ -run TestFetchSnapshot -v`
Expected: FAIL，`handleFetchSnapshot undefined`

- [ ] **Step 3: 实现**

- `transport.go` 的 op 常量注释块追加 `OpFetchSnapshot=4` 的载荷布局（与既有三个 op 同样格式）。
- `handleFetchSnapshot`：解析请求 → `snaps.Get(id)`（不存在报错并带 id）→ 续期 → `scanGroupKeys(view, g, groups, cursor, chunkKeys, emit)` → `encodeChunk` → 组装响应。**每块字节数受 `SnapshotChunkBytes`（默认 4MiB）与帧上限 16MiB 双重约束**：emit 累计到阈值即提前收口，游标落在最后一个已发出的键上。
- `Manager` 的 control 处理器加分支；`main.go` 的 `controlHandler` 无需改（该 op 由 Manager 内部处理，与 ForwardAppend/ForwardApply 不同——后者需要 core 组件）。**这一点要在 `controlHandler` 注释里写明**，否则下一个人会以为漏接了一个 op。

- [ ] **Step 4: 加关键节点日志与注释**

- 每块打 Debug（g/snap_id/键数/字节数/是否结束）；**首块与末块打 Info**——一次快照传输的起止是排查追齐耗时的基本单位。
- 未知 snapID 打 Warn 带 id 与请求方（过期 TTL 是常见原因，日志要能区分「没生成过」与「已回收」）。
- 单块超帧上限的收口逻辑加注释说明双阈值关系。

- [ ] **Step 5: 运行测试并提交**

Run: `go test -race ./internal/cluster/ -run FetchSnapshot -v`
Expected: PASS

```bash
git add internal/cluster/
git commit -m "feat(cluster): 控制通道 OpFetchSnapshot 分块流式发送组状态

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: 接收侧安装——安装中标记与崩溃可重来

**Files:**
- Modify: `internal/cluster/group.go`（`handleReady` 处理 `rd.Snapshot`）
- Create: `internal/cluster/snapinstall.go`
- Modify: `internal/cluster/raftstore.go`（`MarkInstalling`/`ClearInstalling`/`LoadInstalling`）
- Modify: `internal/cluster/manager.go:288 buildGroup`（启动时发现安装中标记 → 清空该组键族重来）
- Test: `internal/cluster/snapinstall_test.go`、`internal/cluster/cluster_test.go`

**Interfaces:**
- Consumes: Task 5 `decodeChunk`/`groupKeyRanges`、Task 6 的请求/响应布局、Task 3 `SaveSnapMeta`
- Produces: `func (gr *group) installSnapshot(ctx context.Context, snap raftpb.Snapshot) error`；`func (r *raftStore) MarkInstalling(g uint32, meta *raftpb.SnapshotMetadata) error`；`func (r *raftStore) LoadInstalling(g uint32) (*raftpb.SnapshotMetadata, bool, error)`；`func wipeGroupKeys(st *store.Store, g, groups uint32) error`

- [ ] **Step 1: 写失败测试**

```go
// TestInstallingMarkerSurvivesCrash 安装中崩溃 = 半截状态。
// 重启必须判定该组不完整并清空重来，绝不能把半截状态当完整状态启动。
func TestInstallingMarkerSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir, false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	rs := newRaftStore(st, testSlog(t))
	idx, term := uint64(88), uint64(4)
	if err := rs.MarkInstalling(1, &raftpb.SnapshotMetadata{Index: &idx, Term: &term}); err != nil {
		t.Fatal(err)
	}
	// 写入「半截」状态
	b := st.NewBatch()
	if err := b.Set(store.MsgKey("T", 0, 1), []byte("half")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := store.Open(dir, false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	rs2 := newRaftStore(st2, testSlog(t))
	meta, ok, err := rs2.LoadInstalling(1)
	if err != nil || !ok {
		t.Fatalf("安装中标记必须在重启后可见 ok=%v err=%v", ok, err)
	}
	if meta.GetIndex() != 88 {
		t.Fatalf("标记 index = %d; want 88", meta.GetIndex())
	}
}

// TestWipeGroupKeysOnlyTouchesOwnGroup 清空重来只能清自己组的键——
// 共享 store 里误清别组 = 把无辜的组也打成需要快照。
func TestWipeGroupKeysOnlyTouchesOwnGroup(t *testing.T) {
	st := openTestStore(t)
	const groups = uint32(3)
	var mine, others [][]byte
	b := st.NewBatch()
	for q := uint32(0); q < 30; q++ {
		k := store.MsgKey("T", q, 1)
		if groupForQueue("T", q, groups) == 1 {
			mine = append(mine, k)
		} else {
			others = append(others, k)
		}
		if err := b.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Set(store.TopicMetaKey("T"), []byte("m")); err != nil { // 组 0 的键
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	if len(mine) == 0 || len(others) == 0 {
		t.Fatal("测试前提不成立：组 1 与其它组都要有键")
	}
	if err := wipeGroupKeys(st, 1, groups); err != nil {
		t.Fatal(err)
	}
	for _, k := range mine {
		if _, ok, _ := st.Get(k); ok {
			t.Fatalf("组 1 的键 %q 未被清除", k)
		}
	}
	for _, k := range others {
		if _, ok, _ := st.Get(k); !ok {
			t.Fatalf("别组的键 %q 被误清", k)
		}
	}
	if _, ok, _ := st.Get(store.TopicMetaKey("T")); !ok {
		t.Fatal("组 0 的全局键被误清")
	}
}

// TestLaggingFollowerCatchesUpBySnapshot 端到端：让一个 follower 停摆、
// leader 写入并截断到它需要的位点之上、follower 回来 —— 必须靠快照追平。
func TestLaggingFollowerCatchesUpBySnapshot(t *testing.T) {
	h := startThreeNodeCluster(t, withRetainEntries(8)) // 极小保留量逼出快照路径
	defer h.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	victim := h.nonLeaderOf(t, 0)
	h.partitionOff(victim) // 既有辅助：断开该节点的传输层

	leader := h.leaderOf(t, 0)
	for i := 0; i < 200; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("S%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	h.truncateNow(t, 0) // 触发一次截断循环

	h.healPartition(victim)
	// 追平判据：victim 上能读到最后一个键
	waitFor(t, 60*time.Second, func() bool {
		_, ok, _ := victim.Store().Get(store.TopicMetaKey("S199"))
		return ok
	}, "落后节点未在 60s 内经快照追平")

	if n := h.countLog(victim, "快照安装完成"); n < 1 {
		t.Fatal("追平未走快照路径（日志无「快照安装完成」）——测试前提失效")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/cluster/ -run 'Installing|WipeGroupKeys|LaggingFollower' -v`
Expected: FAIL，`rs.MarkInstalling undefined`

- [ ] **Step 3: 实现**

`handleReady` 在第 1 步之前插入快照分支（raft 契约：快照必须先于本轮条目应用）：

```go
	// 0. 快照：raft 判定本节点落后过多，leader 发来了 MsgSnap。
	//    安装期间本组暂停处理普通条目（raft 契约），但必须保持 tick——
	//    否则选举计时器停摆，安装完成后本节点会被判定失联。
	//    rn.Tick() 可跨 goroutine 调用（内部走 channel，满则丢）。
	if !raft.IsEmptySnap(rd.Snapshot) {
		stop := gr.keepTicking()
		err := gr.installSnapshot(ctx, rd.Snapshot)
		stop()
		if err != nil {
			// 安装失败不 panic：标记仍在盘上，本轮放弃后 raft 会重发
			// MsgSnap 重来；重启则由 buildGroup 的标记检查清空重来。
			gr.lg.Error("快照安装失败，本轮放弃（raft 将重发）", "g", gr.g,
				"index", rd.Snapshot.Metadata.GetIndex(), "err", err)
			gr.rn.Advance()
			return
		}
	}
```

`installSnapshot` 六步（顺序即不变量，逐条写进注释）：

1. 解析描述符 → 得到 `(snapID, leader, index)`；
2. `rs.MarkInstalling(g, meta)`（Sync）——**必须早于任何数据写入**；
3. `wipeGroupKeys(st, g, groups)`——清掉本组旧状态（组 0 用 DeleteRange 连续前缀；数据组逐键判归属后批量 Delete）；
4. 循环 `Control(ctx, leader, OpFetchSnapshot, req)` 拉块 → `decodeChunk` → 批量写入（每块一个 batch，NoSync）；
5. 收口批次：写 `applied = meta.Index`、`SaveConfState(meta.ConfState)`、`SaveSnapMeta(meta)`、**删安装中标记**——四者同批 Sync，这一批的原子性就是「安装完成」的定义；
6. 内存侧：`mem.ApplySnapshot(snap)`（元数据版，Data 置空即可）、`gr.applied.Store(meta.Index)`、`gr.confState.Store(meta.ConfState)`。

`buildGroup` 启动路径在 `Load` 之后追加：

```go
		// 安装中标记存在 = 上次快照安装未完成，本组状态是半截的。
		// 清空该组键族让 raft 重新发快照——半截状态当完整状态启动
		// 会让本节点向客户端返回缺失的消息（静默丢数据）。
		if meta, installing, err := m.rs.LoadInstalling(g); err != nil {
			return nil, err
		} else if installing {
			m.lg.Warn("发现未完成的快照安装，清空该组状态重来", "g", g, "index", meta.GetIndex())
			if err := wipeGroupKeys(m.st, g, m.groups); err != nil {
				return nil, fmt.Errorf("cluster: 组 %d 清空重来: %w", g, err)
			}
			if err := m.rs.ResetGroupProgress(g); err != nil { // applied=0 + 删 snap 锚点 + 删标记
				return nil, err
			}
			applied = 0
		}
```

- [ ] **Step 4: 加关键节点日志与注释**

- 安装开始 Info（g/snap_id/leader/index）、每 N 块 Debug 进度（已写键数/字节数）、**安装完成 Info「快照安装完成」带耗时**（e2e 用它当路径证据，文案不许改）。
- 失败 Error 带步骤名（拉块失败 / 写入失败 / 收口失败 各自可区分）。
- `installSnapshot` 的六步顺序在函数注释里逐条写明「为什么是这个顺序」，尤其第 2 步早于第 3 步（先标记后清空——反过来崩溃就是「已清空、无标记」= 静默空状态）。

- [ ] **Step 5: 运行测试并提交**

Run: `go test -race ./internal/cluster/ -run 'Installing|Wipe|Lagging' -v && go test -race ./internal/cluster/`
Expected: PASS

```bash
git add internal/cluster/
git commit -m "feat(cluster): 快照接收侧安装——安装中标记 + 清空重来 + 收口批次原子完成

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: 截断循环与配置项

**Files:**
- Modify: `internal/config/config.go`（`log_retain_entries`、`truncate_interval`、`snapshot_chunk_bytes`）
- Modify: `internal/cluster/manager.go`（`truncateLoop` + Options 字段）
- Modify: `cmd/sq/main.go`（配置透传）
- Test: `internal/config/config_test.go`、`internal/cluster/cluster_test.go`

**Interfaces:**
- Consumes: Task 3 `SaveSnapMeta`/`TruncateLog`、Task 4 `confState`
- Produces: `Options.RetainEntries uint64`（默认 10000）、`Options.TruncateInterval time.Duration`（默认 30s）、`Options.SnapshotChunkBytes int`（默认 4<<20）；`func (m *Manager) truncateOnce(g uint32) (upto uint64, done bool)`

- [ ] **Step 1: 写失败测试**

```go
// TestTruncateOnceRespectsLeaderMinMatch leader 截断的安全下界：
// 不得截到最慢 follower 的 Match 之下——那会把一次心跳级追齐
// 放大成整组状态传输。
func TestTruncateOnceRespectsLeaderMinMatch(t *testing.T) {
	h := startThreeNodeCluster(t, withRetainEntries(4))
	defer h.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	leader := h.leaderOf(t, 0)

	victim := h.nonLeaderOf(t, 0)
	h.partitionOff(victim)
	for i := 0; i < 100; i++ {
		b := leader.Store().NewBatch()
		if err := b.Set(store.TopicMetaKey(fmt.Sprintf("K%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(ctx, 0, b.Repr()); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	upto, done := leader.truncateOnce(0)
	if !done {
		t.Fatal("应执行一次截断")
	}
	st, _ := leader.Status(0)
	minMatch := uint64(math.MaxUint64)
	for id, pr := range st.Progress {
		if id != leader.nodeID && pr.Match < minMatch {
			minMatch = pr.Match
		}
	}
	if upto > minMatch {
		t.Fatalf("截断到 %d 越过了最慢 follower 的 Match %d——安全下界失效", upto, minMatch)
	}
	h.healPartition(victim)
}

// TestTruncateOnceNoopWhenNothingToDrop 保留量之内不该动手
// （周期循环每 30s 触发一次，空转不许写盘、不许刷日志）。
func TestTruncateOnceNoopWhenNothingToDrop(t *testing.T) {
	h := startSingleNodeManager(t, withRetainEntries(10000))
	if _, done := h.m.truncateOnce(0); done {
		t.Fatal("条目数远小于保留量时不应截断")
	}
}

func TestClusterTruncationConfigDefaults(t *testing.T) {
	cfg := loadClusterConfig(t, `
cluster:
  node_id: 1
  raft_listen: ":9081"
  peers:
    - {id: 1, raft_addr: "127.0.0.1:9081", advertise_host: "127.0.0.1", advertise_port: 8081}
`)
	if cfg.Cluster.LogRetainEntries != 10000 {
		t.Fatalf("log_retain_entries 默认 = %d; want 10000", cfg.Cluster.LogRetainEntries)
	}
	if cfg.Cluster.TruncateInterval != 30*time.Second {
		t.Fatalf("truncate_interval 默认 = %v; want 30s", cfg.Cluster.TruncateInterval)
	}
	if cfg.Cluster.SnapshotChunkBytes != 4<<20 {
		t.Fatalf("snapshot_chunk_bytes 默认 = %d; want 4MiB", cfg.Cluster.SnapshotChunkBytes)
	}
	// 上界守卫：分块必须小于传输层帧上限，否则整份快照永远发不出去
	if _, err := loadClusterConfigErr(t, "  snapshot_chunk_bytes: 33554432\n"); err == nil {
		t.Fatal("分块大小超过 16MiB 帧上限必须被拒绝")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/cluster/ ./internal/config/ -run 'Truncate' -v`
Expected: FAIL，`m.truncateOnce undefined`

- [ ] **Step 3: 实现**

```go
// truncateOnce 对一组执行一次截断评估与执行。
//
// 截断点计算（三重下界取最小，再减保留量）：
//   - 本节点 applied：不能截掉还没 apply 的条目；
//   - leader 额外取 min(Progress.Match)：不能截掉最慢 follower 还要的条目。
//     这不是正确性开关（有快照兜底），而是性能纪律——截过头会把一次
//     心跳级追齐放大成整组状态传输（Global Constraints）；
//   - 减 RetainEntries：留一段余量给刚落后一点点的 follower。
//
// 返回：实际截断到的位点与是否真的执行了截断（无事可做时 done=false）。
func (m *Manager) truncateOnce(g uint32) (uint64, bool) {
	gr, ok := m.group(g)
	if !ok {
		return 0, false
	}
	upto := gr.applied.Load()
	if st, ok := m.Status(g); ok && st.RaftState == raft.StateLeader {
		for id, pr := range st.Progress {
			if id != m.nodeID && pr.Match < upto {
				upto = pr.Match
			}
		}
	}
	if upto <= m.retainEntries {
		return 0, false
	}
	upto -= m.retainEntries
	first, err := gr.stg.FirstIndex()
	if err != nil || upto < first {
		return 0, false // 已经截到这儿了，空转
	}
	term, err := gr.stg.Term(upto)
	if err != nil {
		m.lg.Warn("截断放弃：位点 term 不可查", "g", g, "upto", upto, "err", err)
		return 0, false
	}
	idx, tm := upto, term
	meta := &raftpb.SnapshotMetadata{Index: &idx, Term: &tm, ConfState: gr.confState.Load()}
	// 顺序：先落锚点（Sync）→ 再删盘上条目 → 最后压内存视图。
	// 任一步崩溃都停在「锚点已落、条目还在」的安全态（多留日志无害）。
	if err := m.rs.SaveSnapMeta(g, meta); err != nil {
		m.lg.Error("截断放弃：锚点落盘失败", "g", g, "upto", upto, "err", err)
		return 0, false
	}
	if err := m.rs.TruncateLog(g, upto); err != nil {
		m.lg.Error("截断放弃：删条目失败", "g", g, "upto", upto, "err", err)
		return 0, false
	}
	if err := gr.mem.Compact(upto); err != nil && !errors.Is(err, raft.ErrCompacted) {
		m.lg.Warn("内存日志视图压缩失败（盘上已截断，下次重启自愈）", "g", g, "upto", upto, "err", err)
	}
	return upto, true
}
```

`truncateLoop` 按 `TruncateInterval` 遍历全部组调用 `truncateOnce`，随 `Start` 拉起、随 ctx 退出。config 侧三个字段带默认值与上界校验（`snapshot_chunk_bytes` 必须 < 16MiB）。

- [ ] **Step 4: 加关键节点日志与注释**

- `truncateOnce` 真正执行时打 Info（g/upto/删除条目数估算/leader 与否）；空转**不打**（30s 一轮 × 4 组，打了就是噪声）。
- 三处放弃路径分别打 Warn/Error 带原因——「日志为什么一直不截断」必须能从日志直接答出来。
- 截断点计算的三重下界在函数注释里逐条写明理由（尤其 minMatch 是性能纪律不是正确性开关，防止被当无用检查删掉）。

- [ ] **Step 5: 运行测试并提交**

Run: `go test -race ./internal/cluster/ ./internal/config/ -run Truncate -v && go test -race ./internal/cluster/`
Expected: PASS

```bash
git add internal/cluster/ internal/config/ cmd/sq/main.go
git commit -m "feat(cluster,config): 周期截断循环与保留量/分块配置（leader 受 minMatch 下界约束）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: leadership 迁移写屏障与四项清洁债

**Files:**
- Modify: `internal/cluster/group.go`（`notifyLeaderChange` 早于 `lead.Store`；term 日志取值；`rd.MustSync`）
- Modify: `internal/cluster/group_test.go`（`rn.Stop()` 补漏）
- Modify: `internal/core/produce/produce.go`（注释修正）
- Modify: `cmd/sq/main.go`（`handleForwardApply` 形参改具体类型）
- Test: `internal/cluster/group_test.go`、`internal/core/produce/produce_test.go`

**Interfaces:**
- Consumes: 无（独立于快照链路，可与 Task 3–8 并行）
- Produces: 行为变更——`IsLeader(g)` 返回 true 之前，`OnLeaderChange` 钩子已执行完毕

- [ ] **Step 1: 写失败测试**

```go
// TestLeaderVisibleOnlyAfterHookCompletes batch③ 评审 m1：
// gr.lead.Store 早于 notifyLeaderChange，中间还隔着一条日志——
// 这段窗口里 IsLeader 已放行而计数器缓存尚未失效，并发 Append
// 会用陈旧 offset 覆写已 quorum 提交的消息。
// 屏障要求：钩子跑完之前 IsLeader 必须仍为 false。
func TestLeaderVisibleOnlyAfterHookCompletes(t *testing.T) {
	var sawLeaderInsideHook atomic.Bool
	var gr *group
	hook := func(g uint32, leader uint64, isSelf bool) {
		if !isSelf {
			return
		}
		// 钩子执行期间，对外可见的 leader 身份必须还没生效
		if gr.isLeader() {
			sawLeaderInsideHook.Store(true)
		}
	}
	gr = newTestGroupWithHook(t, hook) // 辅助：单节点组 + 注入钩子
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go gr.run(ctx)
	waitFor(t, 5*time.Second, gr.isLeader, "单节点组未当选")
	if sawLeaderInsideHook.Load() {
		t.Fatal("钩子执行期间 IsLeader 已返回 true——写屏障失效，"+
			"并发 Append 会在计数器失效前拿到陈旧 offset")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/cluster/ -run TestLeaderVisibleOnlyAfterHook -v`
Expected: FAIL，钩子内 `isLeader()` 已为 true

- [ ] **Step 3: 实现五处修改**

1. `group.go` SoftState 分支改顺序并补注释：

```go
	if rd.SoftState != nil {
		// 顺序即屏障（batch③ 评审 m1）：先跑钩子（同步失效计数器缓存），
		// 再让 lead.Store 把 leader 身份对外可见。反过来会留下
		// 「IsLeader 已放行、缓存尚未失效」的窗口，并发 Append 拿到
		// 陈旧 offset 覆写已 quorum 提交的消息。日志同样挪到 Store 之后，
		// 别让一次 stdout 写入撑大窗口。
		gr.notifyLeaderChange(rd.SoftState.Lead)
		gr.lead.Store(rd.SoftState.Lead)
		gr.lg.Info("组 leader 变更", "lead", rd.SoftState.Lead, "term", gr.currentTerm())
	}
```

`currentTerm()` 取自最近一次非空 HardState 的缓存值——原来的 `rd.HardState.GetTerm()` 在 HardState 为空的那一轮恒为 0（backlog「首轮 leader 日志 term=0」）。

2. `rd.MustSync`：`syncPersist` 改为 `gr.mode == AckQuorumFsync && rd.MustSync`。raft 已经算准了「本轮是否必须落盘才能对外确认」，比现有的「有条目或有 HardState 就刷」更准；quorum-mem 档不变（永不 Sync）。**必须带基准证据**：`BenchmarkProposeQuorumFsync` 前后对比记入 commit message，没有提升就回退这一条（不为改而改）。

3. `group_test.go:152` 的 `raft.StartNode` 补 `defer rn.Stop()`——`StartNode` 起了 raft 节点 goroutine，测试从不停止它，`-race` 下是稳定的 goroutine 泄漏。

4. `produce.go:201-204` 注释第一条按评审 m1 改写：失效发生在**重新当选**时（`isSelf==true`），非 leader 期间提案必然被 `ErrNotLeader` 拒，因此不存在返回烧掉 offset 的路径；并注明「重获 leadership 的窗口已由 group.go 的顺序屏障关闭」。

5. `cmd/sq/main.go:601` `handleForwardApply` 形参 `rep replication.Replicator` 改为 `cl *replication.Cluster`（与 `handleForwardAppend` 形状对齐）——判空即真实生效，不再是永远为假的误导性防御（评审 m2）。

- [ ] **Step 4: 加关键节点日志与注释**

- leader 变更日志的 term 取值改动加注释说明「空 HardState 轮次恒为 0」的成因。
- 顺序屏障的注释按上文原样写入——这是全批最容易被「顺手优化」掉的两行。
- `syncPersist` 的 `MustSync` 改动注释里附上基准数字（改前/改后 ops）。

- [ ] **Step 5: 运行测试并提交**

Run: `go test -race ./internal/cluster/ ./internal/core/produce/ -v && go vet ./...`
Expected: PASS

```bash
git add internal/cluster/ internal/core/produce/produce.go cmd/sq/main.go
git commit -m "fix(cluster,produce,main): leadership 迁移写屏障 + 四项清洁债（term=0 日志、MustSync、goroutine 泄漏、形参形状）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: 单机→多节点扩容、e2e 证据与收尾

**Files:**
- Modify: `internal/cluster/manager.go`（`Join` 入口：新节点空目录加入既有集群）
- Modify: `test/e2e/sdk_cluster_test.go`
- Create: `docs/superpowers/plans/`（无新文件，收尾更新 backlog）
- Test: `test/e2e/sdk_cluster_test.go`、`internal/cluster/cluster_test.go`

**Interfaces:**
- Consumes: Task 6/7 的快照链路、Task 8 的截断循环、batch③ 的 `PrepareJoin`/`AutoPromoteLearners`
- Produces: `func Join(ctx context.Context, o Options, seedPeers map[uint64]string) (*Manager, error)`

- [ ] **Step 1: 写失败测试**

```go
// TestExpandStandaloneToThreeNodes 单机→多节点扩容（spec §7）：
// 单机数据目录直接以单节点集群启动，另两个空目录节点靠快照追齐入组。
func TestExpandStandaloneToThreeNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("扩容用例耗时长")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// ① 单机档写入 200 条，正常关停
	seedDir := t.TempDir()
	writeStandaloneMessages(t, seedDir, "EXPAND", 200)

	// ② 同一目录以单节点集群启动（peers 只有自己）
	seed := startClusterNode(t, seedDir, 1, map[uint64]string{1: addr(1)})
	waitFor(t, 30*time.Second, func() bool { return seed.IsLeader(0) }, "种子节点未当选")

	// ③ 两个空目录节点 Join
	for _, id := range []uint64{2, 3} {
		n, err := cluster.Join(ctx, nodeOptions(t, t.TempDir(), id,
			map[uint64]string{1: addr(1), 2: addr(2), 3: addr(3)}), map[uint64]string{1: addr(1)})
		if err != nil {
			t.Fatalf("节点 %d 加入失败: %v", id, err)
		}
		t.Cleanup(func() { _ = n.StopClean(context.Background()) })
	}

	// ④ 全部数据在新节点上可见（走的是快照追齐，不是日志重放——
	//    种子的日志只有单机导入那一段，新节点从零开始必然需要快照）
	waitFor(t, 120*time.Second, func() bool {
		for _, n := range allNodes(t) {
			if _, ok, _ := n.Store().Get(store.TopicMetaKey("EXPAND")); !ok {
				return false
			}
		}
		return true
	}, "新节点未在 120s 内追平")

	// ⑤ 新节点最终升为 voter（AutoPromoteLearners）
	waitFor(t, 60*time.Second, func() bool {
		st, ok := seed.Status(0)
		return ok && len(st.Config.Voters[0]) == 3
	}, "learner 未自动升为 voter")

	// ⑥ 消息可读：官方 SDK 从任一节点消费，200 条零丢失
	assertConsumeAll(t, 200)
}

// TestClusterTruncationKeepsLogBounded e2e 截断证据：
// 持续写入下 raft 日志键数必须被压住，不许无界增长。
func TestClusterTruncationKeepsLogBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("长跑用例")
	}
	h := startThreeNodeE2E(t, withRetainEntries(500), withTruncateInterval(2*time.Second))
	defer h.stopAll()
	sendMessages(t, h, 5000)
	waitFor(t, 60*time.Second, func() bool {
		return h.countRaftEntries(t, 0) < 3000 // 5000 条写入后仍远小于总量
	}, "raft 日志未被截断，键数仍在增长")
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/cluster/ ./test/e2e/ -run 'Expand|TruncationKeepsLog' -v`
Expected: FAIL，`cluster.Join undefined`

- [ ] **Step 3: 实现 Join**

`Join` 复用 batch③ 的 `Rejoin` 编排骨架，差别只在前置条件（不 Wipe，因为目录本就是空的；不需要消费 clean 标记）：

1. 校验数据目录为空（非空报错，指向 `Rejoin`——「重入」与「新加入」是两个语义，混用会静默清数据）；
2. 轮询 `seedPeers` 发 `PrepareJoin`，收齐全部组的 AddLearner 完成；
3. `store.Open` + `NewManager` fresh 路径 + `Start`；
4. 返回；追平与升 voter 由 leader 侧 `AutoPromoteLearners` 完成——**这一步现在真的能收敛了**，因为落后到日志之外时有 Task 7 的快照兜底（batch③ 时这条路走不通，正是本批要解的问题）。

- [ ] **Step 4: 加关键节点日志与注释**

- `Join` 每一步打 Info（目录校验 / PrepareJoin 完成组号 / 启动完成），失败带步骤名。
- 文档注释写明 `Join` 与 `Rejoin` 的语义分界（新加入 vs 断电重入），以及为什么 `Join` 拒绝非空目录。
- e2e 用例的断言注释写明「为什么这条能证明走了快照路径」。

- [ ] **Step 5: 全量验证、Linux 证据与收尾提交**

Run（本地）：
```bash
go vet ./... && go test -race ./internal/... && go test ./test/e2e/ -run 'Expand|Truncation|ThreeNode|ClusterDLQ' -v
```

Run（Linux，本批起固定 `-race` 全量）：
```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o /tmp/cluster-linux.test ./internal/cluster/
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o /tmp/e2e-cluster-linux.test ./test/e2e/
```
（`-race` 需 CGO：交叉编译拿不到 race 检测器，Linux 侧的 `-race` 全量按既有做法 rsync 源码后在远端 `go test -race ./internal/...`，与 b207a01 同一路径。）

收尾：更新 `docs/superpowers/backlog.md` 的 B8.2 完成痕迹与 B8.3 入口项（把本批外推的三项写进去）。

```bash
git add internal/cluster/ test/e2e/ docs/superpowers/backlog.md
git commit -m "feat(cluster,e2e): 单机→多节点扩容 + 截断/快照 e2e 证据，batch④ 收口

Evidence（Linux，<host>）：<粘贴远端 -race 全量与两个 e2e 用例的实际输出>
本地：go vet ./... 0 告警；go test -race ./internal/... 全绿。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Self-Review 记录

**1. spec 覆盖**
- spec §5「applied_index 随 FSM 写原子持久化；raft 日志保留至 FSM flush 过该 index 后方可截断（Pebble range delete）」→ Task 2（ConfChange 路径的 applied 缺口）+ Task 3（range delete）+ Task 8（截断点 ≤ applied）。
- spec §7「单机 → 集群平滑升级：现有数据目录作为第一个节点，另两节点靠快照追齐入组」→ Task 10 `TestExpandStandaloneToThreeNodes` 逐条对应。
- spec §8.3「薄壳的日志截断、快照追赶、干净关机标记均需独立测试」→ Task 3（截断）、Task 7（追赶）；干净关机标记已在 batch② 覆盖，本批只补「空日志边界」（Task 7 的 `buildGroup` 路径顺带覆盖：安装中标记与 clean 标记的判定互不干扰）。
- **未覆盖且已显式外推**：spec §8.2 的四类场景测试（batch⑤ / B8.3）。

**2. 占位符扫描**：无 TBD/TODO；每个实现步骤都有代码或精确到 file:line 的改动说明；测试步骤均为可运行的完整用例。`Task 10` 的 commit evidence 段留了 `<host>`/`<粘贴…>` 占位——这是**执行时才产生的实测数据**，不是设计缺口。

**3. 类型一致性**
- `raftStore.Load` 签名在 Task 3 变为四返回值，Task 2 的 `buildGroup` 改动与 Task 7 的启动路径都按新签名书写；Task 2 先落地时 `Load` 仍是三返回值，Task 3 落地时需同步改 Task 2 写下的调用点——**已在 Task 3 的实现步骤里显式点明**（`Load` 签名改动那一行）。
- `group.storage` 字段在 Task 4 拆为 `mem *raft.MemoryStorage` + `stg raft.Storage`，Task 8 的 `truncateOnce` 用 `gr.stg.FirstIndex()`/`gr.stg.Term()` 与 `gr.mem.Compact()`，与拆分后的命名一致。
- `groupForQueue(topic string, queueID uint32, groups uint32) uint32` 是 Task 5 从 `Manager.GroupForQueue` 提取的包内函数，Task 7 `wipeGroupKeys` 复用同一个——两处不得各写一份哈希。

**4. 风险自记**
- ① **Task 4 的 `applyMu`**：给 apply 路径加锁是本批唯一进入热路径的改动。锁只覆盖「写批次 + Store(applied)」，但若实现时不小心把 `WaitSync` 圈进去，写吞吐会塌。执行时必须跑 `BenchmarkProposeQuorumFsync`/`Mem` 前后对比，退化超过 5% 即停下重新设计（例如改用 seqlock 式的双读校验）。
- ② **Task 7 的安装期 tick**：`keepTicking` 依赖 `rn.Tick()` 可跨 goroutine 调用。若实测发现安装期间该节点仍被判失联（raft 内部 tick 通道满则丢），退路是把安装拆成「分块拉取在 Ready 循环外的 goroutine 做、只有收口批次回到循环内」——代价是复杂度，收益是彻底不阻塞。
- ③ **数据组快照的扫描成本**：数据组的键哈希散布，一次快照要全表扫 `msg/ alloc/ keyidx/ cursor/ inflight/` 五段并逐键判归属，大库上是分钟级。本批接受这个成本（快照是罕见路径），但要在 Task 6 的首块/末块日志里带上耗时——如果实测发现常态化触发，说明 `RetainEntries` 配小了，属配置问题而非设计问题。
