# Produce 写吞吐优化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 解锁队列内 group commit、默认队列数提到 16、SendBatch 整批一次 fsync、soak 基准入库——把单队列写吞吐从 fsync 封顶（435 msg/s）解放出来。

**Architecture:** store 层新增拆分式提交（`ApplyAsync` 定序 + `Pending.Wait` 等 fsync，基于 Pebble v2 的 `ApplyNoSyncWait`/`SyncWait`）；produce 层把 fsync 等待挪出每队列锁 `qs.mu`，使同队列的并发写入被 Pebble commit pipeline 合并为一次 fsync；新增 `AppendBatch` 整批同队列原子落盘；rpc 层为纯普通消息的多条请求路由到批量快路径。

**Tech Stack:** Go、Pebble v2.1.6（`DB.ApplyNoSyncWait` + `Batch.SyncWait`）、`log/slog`、`testing`（含 benchmark）。

**Spec:** `docs/superpowers/specs/2026-08-08-produce-throughput-optimization-design.md`

## Global Constraints

- **语义红线 1**：每条消息 fsync 完成后才 ACK（`AppendWith`/`AppendBatch` 返回前必须 `Wait()` 成功）。
- **语义红线 2**：同队列 offset 顺序 == 落盘顺序（offset 分配与批次定序必须在同一 `qs.mu` 临界区内完成）。
- **语义红线 3**：消息体与 alloc 计数器同一 Pebble Batch 原子提交。
- 日志一律 `slog`（沿包内 `p.logger`/`s.logger`），禁止 `fmt.Printf`；注释中文、解释"为什么"。
- 失败的 Batch 不 Close、丢给 GC（store.go Apply 注释的既有约定）；未提交而放弃的 Batch 必须 Close（NewBatch 契约路径 2）。
- 每个 task 结束跑 `go test ./internal/...` 全绿 + `gofmt -l internal/` 无输出后才 commit。
- 验收基准硬件：云服务器 root@47.80.240.57（2 vCPU、fsync 456 次/s、免密登录）。改动前基线：单队列 435 msg/s、默认 4 队列 ~900 msg/s、queues=256/并发 256 → 56,990 msg/s。

---

### Task 1: store 拆分式提交（ApplyAsync + Pending.Wait）

**Files:**
- Modify: `internal/store/store.go`（在 `Apply` 之后新增）
- Test: `internal/store/store_test.go`（追加）

**Interfaces:**
- Consumes: 既有 `Store.NewBatch/Apply`、包级 `OnApplyObserve`。
- Produces（后续 Task 2/3 依赖，签名精确如下）:
  - `type Pending struct { ... }`（值类型）
  - `func (s *Store) ApplyAsync(b *pebble.Batch) (Pending, error)`
  - `func (p Pending) Wait() error`

- [ ] **Step 1: 写失败测试**

在 `internal/store/store_test.go` 追加（包名与文件内既有测试一致，为 `store` 包内测试；import 需含 `log/slog`、`sync`、`fmt`）：

```go
// TestApplyAsyncThenWaitPersists 验证拆分式提交（sync 模式）：ApplyAsync 定序、
// Wait 等待持久化，之后数据可读。
func TestApplyAsyncThenWaitPersists(t *testing.T) {
	s, err := Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	b := s.NewBatch()
	b.Set([]byte("k1"), []byte("v1"), nil)
	pending, err := s.ApplyAsync(b)
	if err != nil {
		t.Fatalf("ApplyAsync: %v", err)
	}
	if err := pending.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	v, ok, err := s.Get([]byte("k1"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get k1 = %q ok=%v err=%v, want v1", v, ok, err)
	}
}

// TestApplyAsyncNoSyncFallback 验证 syncWrites=false 的退化路径：ApplyAsync 内
// 一次性完成提交，Wait 只做批次回收，行为与 sync 模式外观一致。
func TestApplyAsyncNoSyncFallback(t *testing.T) {
	s, err := Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	b := s.NewBatch()
	b.Set([]byte("k2"), []byte("v2"), nil)
	pending, err := s.ApplyAsync(b)
	if err != nil {
		t.Fatalf("ApplyAsync: %v", err)
	}
	if err := pending.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	v, ok, err := s.Get([]byte("k2"))
	if err != nil || !ok || string(v) != "v2" {
		t.Fatalf("Get k2 = %q ok=%v err=%v, want v2", v, ok, err)
	}
}

// TestApplyAsyncConcurrentAllDurable 验证并发拆分式提交全部持久化——这是
// group commit 合并 fsync 的使用形态，不能有条目丢失或错序覆盖。
func TestApplyAsyncConcurrentAllDurable(t *testing.T) {
	s, err := Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	const goroutines, perG = 16, 20
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				b := s.NewBatch()
				key := fmt.Sprintf("cc/%d/%d", g, i)
				b.Set([]byte(key), []byte("x"), nil)
				pending, err := s.ApplyAsync(b)
				if err != nil {
					t.Errorf("ApplyAsync %s: %v", key, err)
					return
				}
				if err := pending.Wait(); err != nil {
					t.Errorf("Wait %s: %v", key, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perG; i++ {
			key := fmt.Sprintf("cc/%d/%d", g, i)
			if _, ok, err := s.Get([]byte(key)); err != nil || !ok {
				t.Fatalf("key %s 丢失: ok=%v err=%v", key, ok, err)
			}
		}
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/store/ -run 'TestApplyAsync' -v`
Expected: 编译失败 `undefined: (*Store).ApplyAsync` / `undefined: Pending`

- [ ] **Step 3: 实现 ApplyAsync 与 Pending.Wait**

在 `internal/store/store.go` 的 `Apply` 函数之后追加：

```go
// Pending 一次「已定序、待确认持久化」的提交。值类型，热路径零额外分配。
// 由 ApplyAsync 返回；调用方必须恰好调用一次 Wait，且在 Wait 成功前不得
// 把这次写入当作已持久化（不得 ACK、不得唤醒读者）。
type Pending struct {
	b        *pebble.Batch
	start    time.Time
	needSync bool // true=还需 SyncWait（sync 模式）；false=提交已完成，Wait 只回收批次
}

// ApplyAsync 提交批次——写 WAL、发布可见、完成定序——但不等待 fsync。
//
// 与 Apply 的关系：Apply == ApplyAsync + Wait。写热路径（produce）用拆分形式
// 把 fsync 等待挪出队列锁，让 Pebble commit pipeline 把同队列多条在途提交
// 合并为一次 fsync（group commit）；其余调用点无此需求，继续用 Apply。
//
// syncWrites=false 的部署没有「等待 fsync」阶段：本方法内一次性完成提交，
// 返回的 Pending.Wait 退化为纯批次回收。
//
// 失败时批次按 Apply 同款约定丢给 GC，调用方不得再碰（理由见 Apply 注释）。
func (s *Store) ApplyAsync(b *pebble.Batch) (Pending, error) {
	start := time.Now()
	if !s.sync {
		if err := b.Commit(pebble.NoSync); err != nil {
			return Pending{}, fmt.Errorf("store ApplyAsync: %w", err)
		}
		if OnApplyObserve != nil {
			OnApplyObserve(time.Since(start))
		}
		return Pending{b: b}, nil
	}
	// ApplyNoSyncWait 要求 opts.Sync=true（Pebble 契约）：批次进入 commit
	// pipeline 排队等待 WAL fsync，但本调用不阻塞等它完成。
	if err := s.db.ApplyNoSyncWait(b, pebble.Sync); err != nil {
		return Pending{}, fmt.Errorf("store ApplyAsync: %w", err)
	}
	return Pending{b: b, start: start, needSync: true}, nil
}

// Wait 等待批次持久化完成并回收批次。
//
// 观测口径与 Apply 一致：OnApplyObserve 覆盖「提交开始 → 持久化完成」全程，
// 直方图语义不因拆分而改变。SyncWait 失败时批次丢给 GC（同 Apply 约定）；
// WAL sync 失败意味着 Pebble 已进入不可恢复错误态，后续写入都会失败，
// 调用方把错误上抛即可，无需（也无法）在此挽救。
func (p Pending) Wait() error {
	if p.needSync {
		if err := p.b.SyncWait(); err != nil {
			return fmt.Errorf("store WaitSync: %w", err)
		}
		if OnApplyObserve != nil {
			OnApplyObserve(time.Since(p.start))
		}
	}
	return p.b.Close()
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/store/ -v`
Expected: 全部 PASS（含既有测试）

- [ ] **Step 5: 自检日志与注释**

本层遵循 store.go 文件头声明的边界「热路径不打日志，语义级日志由调用方负责」——ApplyAsync/Wait 不加日志是**遵守**既有边界，不是遗漏。确认：`Pending` 类型注释、两个导出方法的 doc 注释（含契约与失败约定）、`needSync` 字段的 why 注释都已就位（Step 3 代码已含）。

- [ ] **Step 6: gofmt + 提交**

```bash
gofmt -l internal/store/ && go test ./internal/store/ && git add internal/store/ && git commit -m "feat(store): 拆分式提交 ApplyAsync/Pending.Wait——为队列内 group commit 铺路

Apply == ApplyAsync + Wait。基于 Pebble ApplyNoSyncWait/SyncWait（CockroachDB
写 raft log 的生产验证模式）。NoSync 配置退化为一次性提交。观测口径不变。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: AppendWith 解锁——fsync 等待挪出 qs.mu

**Files:**
- Modify: `internal/core/produce/produce.go:143-157`（段 2 尾部与段 3）
- Test: `internal/core/produce/produce_test.go`（追加）

**Interfaces:**
- Consumes: Task 1 的 `Store.ApplyAsync(b) (store.Pending, error)`、`Pending.Wait() error`。
- Produces: `AppendWith`/`Append` 签名不变，行为语义不变（fsync 后才返回）；仅内部锁范围收窄。所有经 `AppendWith` 的调用方（DLQ、延时转正、事务提交）自动受益。

- [ ] **Step 1: 写失败前提确认 + FIFO 回归测试**

在 `internal/core/produce/produce_test.go` 追加（import 增加 `hash/fnv`）：

```go
// TestAppendConcurrentSameGroupOffsetsContiguous 是 group commit 解锁后的 FIFO
// 回归测试：并发写同一 MessageGroup（固定落同一队列），验证 offset 恰为
// 0..N-1 连续无洞无重、alloc 计数器与消息严格一致。解锁改动若破坏
// 「offset 顺序 == 落盘顺序」的临界区，本测试在 -race 下必然暴露。
func TestAppendConcurrentSameGroupOffsetsContiguous(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	const goroutines, perG = 8, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := p.Append(&core.Message{Topic: "fifo-cc", MessageGroup: "g1", Body: []byte("x")}); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	// 用与实现相同的 fnv 哈希算出该 group 固定落的队列号（fixture 为 4 队列）
	h := fnv.New32a()
	h.Write([]byte("g1"))
	qid := h.Sum32() % 4
	total := uint64(goroutines * perG)
	v, ok, err := st.Get(store.AllocKey("fifo-cc", qid))
	if err != nil || !ok {
		t.Fatalf("alloc 计数器缺失: ok=%v err=%v", ok, err)
	}
	if got := store.GetU64(v); got != total {
		t.Fatalf("alloc 计数器 = %d, want %d", got, total)
	}
	for off := uint64(0); off < total; off++ {
		if _, ok, err := st.Get(store.MsgKey("fifo-cc", qid, off)); err != nil || !ok {
			t.Fatalf("offset %d 消息缺失: ok=%v err=%v", off, ok, err)
		}
	}
}
```

- [ ] **Step 2: 先在旧实现上跑，确认测试本身是绿的（回归测试基线）**

Run: `go test ./internal/core/produce/ -run TestAppendConcurrentSameGroup -race -v`
Expected: PASS（此测试锁定现状语义；解锁改动后必须依旧 PASS）

- [ ] **Step 3: 记录改动前本机基准（对照用）**

Run: `go test ./internal/core/produce/ -bench BenchmarkAppendParallel -benchtime 3s -run '^$' | tee /tmp/bench-before.txt`
Expected: 输出 ns/op 基线（本机数量级 ~1e6 ns/op）

- [ ] **Step 4: 实现解锁**

将 `internal/core/produce/produce.go` 中 `AppendWith` 的这段（原 143-157 行）：

```go
	if err := p.st.Apply(b); err != nil {
		qs.mu.Unlock()
		return nil, fmt.Errorf("写入消息 %s (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}
	// Apply 成功才推进（失败的写不能烧 offset，原注释原样保留）
	qs.next = off + 1
	qs.loaded = true
	qs.mu.Unlock()

	// 段 3（p.mu）：唤醒长轮询。必须在落盘成功之后——被唤醒的订阅者读 store
	// 必能看到这条消息。
	p.mu.Lock()
	p.wakeLocked(m.Topic)
	p.mu.Unlock()
```

替换为：

```go
	pending, err := p.st.ApplyAsync(b)
	if err != nil {
		qs.mu.Unlock()
		return nil, fmt.Errorf("写入消息 %s (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}
	// ApplyAsync 成功 == 本条已在 WAL/memtable 中定序。此刻立即推进 offset 缓存
	// 并解锁：同队列后继消息随即进锁定序，与本条一起挂在 commit pipeline 里共享
	// 同一次 fsync——这就是 group commit 在队列内生效的机制（吞吐从 1/fsync延迟
	// 变为 合并深度/fsync延迟）。
	//
	// 为什么敢在 Wait 之前推进 qs.next：若后续 WaitSync 失败，说明 WAL sync 失败、
	// Pebble 已进入不可恢复错误态，之后所有写入都会失败，进程只能重启；重启后
	// 计数器与实际落盘由「同批原子提交」保证严格一致，内存里烧掉的 offset 无害。
	qs.next = off + 1
	qs.loaded = true
	qs.mu.Unlock()

	// 锁外等待持久化：fsync 完成之前绝不返回、绝不唤醒（语义红线 1）。
	// 可见性窗口说明（防止后人误改）：Pebble 的 Commit(Sync) 本就是「先发布可见、
	// 后等 fsync」，拉取型读者在旧实现里同样可能于 fsync 完成前读到本条——本改动
	// 没有扩大该窗口，只是把等待从锁内挪到锁外。
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待消息 %s 持久化 (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}

	// 段 3（p.mu）：唤醒长轮询。必须在持久化成功之后——被唤醒的订阅者读 store
	// 必能看到这条消息。
	p.mu.Lock()
	p.wakeLocked(m.Topic)
	p.mu.Unlock()
```

同时更新 `queueState` 类型注释（produce.go:31-33）第二句为：

```go
// queueState 单个 (topic, queue) 的写入状态。qs.mu 只串行化 offset 分配与
// 批次定序（ApplyAsync 为止）；fsync 等待在锁外，同队列多条在途提交由 Pebble
// commit pipeline 合并为一次 fsync——队列内 group commit 的关键。
```

- [ ] **Step 5: 全量测试 + race**

Run: `go test ./internal/... -race`
Expected: 全部 PASS（重点：TestAppendConcurrentSameGroupOffsetsContiguous、produce 全部既有测试、txn/delay/deliver 相关测试）

- [ ] **Step 6: 跑改动后基准对照**

Run: `go test ./internal/core/produce/ -bench BenchmarkAppendParallel -benchtime 3s -run '^$' | tee /tmp/bench-after.txt && paste /tmp/bench-before.txt /tmp/bench-after.txt`
Expected: ns/op 显著下降（本机 16 队列 fixture 下预期 2 倍以上；最终验收在 Task 7 云服务器上做）

- [ ] **Step 7: 自检日志与注释**

- 成功路径日志：既有 `p.logger.Debug("消息已写入", ...)` 保留在 Wait 成功之后（位置不变即满足）。
- 错误分支：ApplyAsync 失败与 Wait 失败各自带 msg_id/topic/queue/offset 上下文包装（Step 4 代码已含）。
- 注释：解锁机制 why、qs.next 提前推进 why、可见性窗口说明（Step 4 代码已含）。

- [ ] **Step 8: gofmt + 提交**

```bash
gofmt -l internal/core/produce/ && git add internal/core/produce/ && git commit -m "perf(produce): fsync 等待挪出队列锁——group commit 在队列内生效

qs.mu 只护定序（ApplyAsync 为止），Wait 在锁外。同队列多条在途提交共享一次
fsync，单队列吞吐从 1/fsync延迟 解放为 合并深度/fsync延迟。语义不变：fsync
后才返回、offset 顺序==落盘顺序、同批原子性未动。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Producer.AppendBatch——整批同队列原子落盘

**Files:**
- Modify: `internal/core/produce/produce.go`（`Append` 之后新增方法）
- Test: `internal/core/produce/produce_test.go`（追加）

**Interfaces:**
- Consumes: Task 1 的 `ApplyAsync`/`Wait`；既有 `qkey`、`queueState.nextOffsetLocked`、`store.MsgKey/AllocKey/KeyIdxKey/PutU64`、`p.wakeLocked`。
- Produces（Task 4 依赖）: `func (p *Producer) AppendBatch(msgs []*core.Message) ([]*core.Message, error)`——入参全部为普通消息（无事务/延时/MessageGroup）、同 topic；返回与入参同序、`QueueID/Offset/ID` 已回填。

- [ ] **Step 1: 写失败测试**

在 `internal/core/produce/produce_test.go` 追加：

```go
// TestAppendBatchContiguousSameQueue 验证批量写入核心不变式：整批同队列、
// offset 连续段 [off, off+N)、alloc 计数器一次推进到 off+N、keys 索引齐全。
func TestAppendBatchContiguousSameQueue(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	msgs := make([]*core.Message, 5)
	for i := range msgs {
		msgs[i] = &core.Message{Topic: "tb", Body: []byte("x"), Keys: []string{"k-idx"}}
	}
	stored, err := p.AppendBatch(msgs)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if len(stored) != 5 {
		t.Fatalf("返回 %d 条, want 5", len(stored))
	}
	qid := stored[0].QueueID
	for i, m := range stored {
		if m.QueueID != qid {
			t.Fatalf("第 %d 条队列 %d != 首条队列 %d（整批必须同队列）", i, m.QueueID, qid)
		}
		if m.Offset != stored[0].Offset+uint64(i) {
			t.Fatalf("第 %d 条 offset %d 不连续（首条 %d）", i, m.Offset, stored[0].Offset)
		}
		if m.ID == "" {
			t.Fatalf("第 %d 条未回填 ID", i)
		}
		if _, ok, err := st.Get(store.MsgKey("tb", qid, m.Offset)); err != nil || !ok {
			t.Fatalf("offset %d 消息未落盘: ok=%v err=%v", m.Offset, ok, err)
		}
	}
	v, ok, err := st.Get(store.AllocKey("tb", qid))
	if err != nil || !ok || store.GetU64(v) != stored[4].Offset+1 {
		t.Fatalf("alloc 计数器 = %v ok=%v err=%v, want %d", v, ok, err, stored[4].Offset+1)
	}
}

// TestAppendBatchRejectsSpecialMessages 验证防御校验：事务/延时/FIFO 消息
// 不允许进批量路径（路由约束在 rpc 层，此处防御手写调用方）。
func TestAppendBatchRejectsSpecialMessages(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	cases := []*core.Message{
		{Topic: "tb2", Body: []byte("x"), MessageGroup: "g"},
		{Topic: "tb2", Body: []byte("x"), DeliverAtMs: time.Now().UnixMilli() + 60_000},
		{Topic: "tb2", Body: []byte("x"), Transactional: true},
	}
	for i, special := range cases {
		if _, err := p.AppendBatch([]*core.Message{{Topic: "tb2", Body: []byte("x")}, special}); err == nil {
			t.Fatalf("case %d: 含特殊消息的批应报错", i)
		}
	}
	if _, err := p.AppendBatch(nil); err == nil {
		t.Fatal("空批应报错")
	}
}

// TestAppendBatchAtomicOnInvalidBody 验证整批原子性：批内任一条 body 非法时
// 整批拒绝、零落盘（alloc 计数器不存在、无任何消息 key）。
func TestAppendBatchAtomicOnInvalidBody(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	msgs := []*core.Message{
		{Topic: "tb3", Body: []byte("ok")},
		{Topic: "tb3", Body: nil}, // 非法：空 body
	}
	if _, err := p.AppendBatch(msgs); err == nil {
		t.Fatal("含空 body 的批应报错")
	}
	// 4 个队列全部确认零落盘
	for q := uint32(0); q < 4; q++ {
		if _, ok, _ := st.Get(store.AllocKey("tb3", q)); ok {
			t.Fatalf("队列 %d 的 alloc 计数器不应存在（整批应零落盘）", q)
		}
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/produce/ -run TestAppendBatch -v`
Expected: 编译失败 `p.AppendBatch undefined`

- [ ] **Step 3: 实现 AppendBatch**

在 `internal/core/produce/produce.go` 的 `Append`（236 行）之后追加：

```go
// AppendBatch 将同一 topic 的一批普通消息整批落入同一队列：连续 offset 段
// [off, off+N)、单个 Pebble Batch、一次 fsync，整批原子——要么全部落盘要么
// 全部不落，比逐条 Append 的「第 N 条失败前 N-1 条无法撤回」语义更强。
//
// 参数：msgs 非空；全部同 topic、且均为普通消息（无事务、无延时、无
// MessageGroup）。路由约束由 rpc 层保证，此处再做防御校验：FIFO 消息的
// 队列由 MessageGroup 哈希决定，与整批轮询选队冲突；事务/延时各有独立
// 暂存路径——三者一律报错，调用方应回退逐条处理。
//
// 返回：与入参同序的消息切片，QueueID/Offset/ID/时间戳已回填。
//
// 注意：整批绑定单一队列与 RocketMQ batch 绑定单 MessageQueue 的语义一致；
// 批与批之间仍按轮询换队列，长期负载均衡不受影响。
func (p *Producer) AppendBatch(msgs []*core.Message) ([]*core.Message, error) {
	if len(msgs) == 0 {
		return nil, fmt.Errorf("AppendBatch 要求至少一条消息")
	}
	topic := msgs[0].Topic
	for _, m := range msgs {
		if m.Topic != topic {
			return nil, fmt.Errorf("AppendBatch 批内 topic 不一致: %q vs %q", topic, m.Topic)
		}
		if m.Transactional || m.DeliverAtMs > 0 || m.MessageGroup != "" {
			return nil, fmt.Errorf("AppendBatch 仅接受普通消息（批内含事务/延时/FIFO 消息）")
		}
		if len(m.Body) == 0 || len(m.Body) > MaxBodySize {
			return nil, fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), MaxBodySize)
		}
	}
	tc, err := p.mt.EnsureTopic(topic)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	for _, m := range msgs {
		if m.ID == "" {
			m.ID = core.NewMessageID()
		}
		if m.BornAtMs == 0 {
			m.BornAtMs = now
		}
		m.StoreAtMs = now
	}

	// 段 1（p.mu）：整批一次队列选择——批内同队列是一次 fsync 与整批原子的前提。
	p.mu.Lock()
	qid := p.rr[topic] % tc.Queues
	p.rr[topic]++
	k := qkey(topic, qid)
	qs, ok := p.qstates[k]
	if !ok {
		qs = &queueState{}
		p.qstates[k] = qs
	}
	p.mu.Unlock()

	// 段 2（qs.mu）：连续 offset 段分配 + 编码 + 单批定序。
	qs.mu.Lock()
	off, err := qs.nextOffsetLocked(p.st, topic, qid)
	if err != nil {
		qs.mu.Unlock()
		return nil, err
	}
	b := p.st.NewBatch()
	for i, m := range msgs {
		m.QueueID = qid
		m.Offset = off + uint64(i)
		raw, err := core.EncodeMessage(m)
		if err != nil {
			qs.mu.Unlock()
			// 未提交而放弃的批次必须自行 Close 回收（store.NewBatch 契约路径 2）
			b.Close()
			return nil, fmt.Errorf("编码消息 %s (topic=%s): %w", m.ID, topic, err)
		}
		b.Set(store.MsgKey(topic, qid, m.Offset), raw, nil)
		for _, key := range m.Keys {
			if key == "" {
				continue // 空 key 无检索意义（与 AppendWith 同款防御）
			}
			b.Set(store.KeyIdxKey(topic, key, m.StoreAtMs, qid, m.Offset), nil, nil)
		}
	}
	// alloc 计数器一次写到 off+N，与全部消息同批原子（语义红线 3 的批量形态）
	b.Set(store.AllocKey(topic, qid), store.PutU64(off+uint64(len(msgs))), nil)
	pending, err := p.st.ApplyAsync(b)
	if err != nil {
		qs.mu.Unlock()
		return nil, fmt.Errorf("批量写入 %d 条 (topic=%s q=%d off=%d): %w", len(msgs), topic, qid, off, err)
	}
	// 提前推进的理由与 AppendWith 完全相同（见其注释）
	qs.next = off + uint64(len(msgs))
	qs.loaded = true
	qs.mu.Unlock()

	// 锁外等待持久化：fsync 完成之前绝不返回、绝不唤醒（语义红线 1）
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待批量写入持久化 (topic=%s q=%d off=%d n=%d): %w", topic, qid, off, len(msgs), err)
	}

	p.mu.Lock()
	p.wakeLocked(topic)
	p.mu.Unlock()
	p.logger.Debug("批量消息已写入", "topic", topic, "queue", qid, "first_offset", off, "count", len(msgs))
	return msgs, nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/produce/ -race -v`
Expected: 全部 PASS

- [ ] **Step 5: 自检日志与注释**

- 成功路径：`批量消息已写入` Debug 日志带 topic/queue/first_offset/count（已含）。
- 错误分支：编码、ApplyAsync、Wait 三处均带 topic/queue/offset/count 上下文（已含）。
- doc 注释含参数约束、返回语义、RocketMQ 语义对齐说明（已含）。

- [ ] **Step 6: gofmt + 提交**

```bash
gofmt -l internal/core/produce/ && go test ./internal/... && git add internal/core/produce/ && git commit -m "feat(produce): AppendBatch 整批同队列原子落盘——一次 fsync 写 N 条

连续 offset 段 + 单 Pebble Batch + ApplyAsync/Wait。整批原子性比逐条更强。
特殊消息（事务/延时/FIFO）防御性拒绝，路由约束在 rpc 层另行保证。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: rpc 层批量路由——纯普通消息多条请求走 AppendBatch

**Files:**
- Modify: `internal/rpc/send.go:77-122`（第二遍写入循环之前插入快路径）
- Test: `internal/rpc/send_test.go`（追加）

**Interfaces:**
- Consumes: Task 3 的 `s.pr.AppendBatch(msgs) ([]*core.Message, error)`；既有 `okStatus/topicErrStatus`、`newTestEnv/newTestClient` fixture（server_test.go）。
- Produces: 无新导出接口；`SendMessage` 对官方 SDK 批量请求的响应格式不变（Entries 与请求同序）。

- [ ] **Step 1: 写失败测试**

在 `internal/rpc/send_test.go` 追加（fixture 用法照 `TestSendMessageNormal`，`newTestEnv` 的 meta 为 4 队列）：

```go
// TestSendMessageBatchFastPath 验证纯普通消息的多条请求走整批落盘：
// 响应 Entries 与请求同序、offset 连续、全部同队列（整批一个 Pebble Batch
// 一次 fsync 的外部可见特征）。
func TestSendMessageBatchFastPath(t *testing.T) {
	c := newTestClient(t)
	req := &pb.SendMessageRequest{}
	for i := 0; i < 3; i++ {
		req.Messages = append(req.Messages, &pb.Message{
			Topic: &pb.Resource{Name: "batch-t"},
			SystemProperties: &pb.SystemProperties{
				MessageId:   fmt.Sprintf("%032X", i+1),
				MessageType: pb.MessageType_NORMAL,
			},
			Body: []byte("hello"),
		})
	}
	resp, err := c.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status: %v", resp.GetStatus())
	}
	entries := resp.GetEntries()
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(entries))
	}
	for i, e := range entries {
		if e.GetStatus().GetCode() != pb.Code_OK || e.GetMessageId() == "" {
			t.Fatalf("entry %d: %v", i, e)
		}
		if i > 0 && e.GetOffset() != entries[0].GetOffset()+int64(i) {
			t.Fatalf("entry %d offset %d 不连续（首条 %d）——整批应落同一队列连续 offset 段",
				i, e.GetOffset(), entries[0].GetOffset())
		}
	}
}

// TestSendMessageBatchWithFifoFallsBack 验证含 FIFO 消息的多条请求回退逐条
// 路径：行为与历史版本一致（各条独立成功），不因批量快路径引入而改变。
func TestSendMessageBatchWithFifoFallsBack(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{
			{
				Topic: &pb.Resource{Name: "mix-t"},
				SystemProperties: &pb.SystemProperties{
					MessageId:   "000000000000000000000000000000A1",
					MessageType: pb.MessageType_NORMAL,
				},
				Body: []byte("plain"),
			},
			{
				Topic: &pb.Resource{Name: "mix-t"},
				SystemProperties: &pb.SystemProperties{
					MessageId:    "000000000000000000000000000000A2",
					MessageType:  pb.MessageType_FIFO,
					MessageGroup: strPtr("g1"),
				},
				Body: []byte("fifo"),
			},
		},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status: %v", resp.GetStatus())
	}
	if len(resp.GetEntries()) != 2 {
		t.Fatalf("entries = %d, want 2", len(resp.GetEntries()))
	}
}
```

注意：若 `SystemProperties.MessageGroup` 的 setter 形态与上不符（proto3 optional 指针），以 `internal/rpc/pb/apache/rocketmq/v2` 生成代码为准调整字段赋值方式；`strPtr` 为 send_test.go 既有辅助函数。

- [ ] **Step 2: 跑测试确认现状**

Run: `go test ./internal/rpc/ -run 'TestSendMessageBatch' -v`
Expected: `TestSendMessageBatchWithFifoFallsBack` PASS（现状即逐条）；`TestSendMessageBatchFastPath` **FAIL 于 offset 连续断言**——现状逐条 round-robin 把 3 条散到 3 个队列，offset 全为 0。这个失败证明快路径尚未存在。

- [ ] **Step 3: 实现批量路由**

在 `internal/rpc/send.go` 第二遍循环（`entries := make(...)` 之前）插入：

```go
	// 快路径：多条且全部为普通消息 → AppendBatch 整批同队列、单 Pebble Batch、
	// 一次 fsync、整批原子。官方 SDK 的 batch send 只会产生这种批（客户端侧
	// 已禁止批内混入事务/延时/FIFO）；含特殊消息的多条请求走下方逐条回退
	// 路径，行为与历史版本完全一致。
	if batchable(msgs) {
		stored, err := s.pr.AppendBatch(msgs)
		if err != nil {
			s.logger.Warn("SendMessage 批量写入失败", "topic", msgs[0].Topic, "count", len(msgs), "err", err)
			return &pb.SendMessageResponse{
				Status: s.topicErrStatus("SendMessage", msgs[0].Topic, err, "batch_count", len(msgs)),
			}, nil
		}
		entries := make([]*pb.SendResultEntry, 0, len(stored))
		for _, m := range stored {
			entries = append(entries, &pb.SendResultEntry{
				Status:    okStatus(),
				MessageId: m.ID,
				Offset:    int64(m.Offset),
			})
		}
		s.logger.Debug("SendMessage 批量写入完成", "topic", msgs[0].Topic, "count", len(stored),
			"queue", stored[0].QueueID, "first_offset", stored[0].Offset)
		return &pb.SendMessageResponse{Status: okStatus(), Entries: entries}, nil
	}
```

并在文件末尾追加辅助函数：

```go
// batchable 判断一批消息可否走 AppendBatch 快路径：多条、且全部为普通消息。
// 事务/延时消息各有独立暂存路径；FIFO 消息的队列由 MessageGroup 哈希决定，
// 与整批轮询选队冲突——三者任一出现即回退逐条处理，保证历史行为不变。
func batchable(msgs []*core.Message) bool {
	if len(msgs) < 2 {
		return false
	}
	for _, m := range msgs {
		if m.Transactional || m.DeliverAtMs > 0 || m.MessageGroup != "" {
			return false
		}
	}
	return true
}
```

同时更新 send.go 第二遍前的注释块（原 77-84 行）：在「注意：这里仍可能在写到第 N 条时 Append 失败」段落之前补一句「以下 at-least-once 论证仅适用于逐条回退路径；批量快路径整批原子，不存在部分落盘」。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/rpc/ -race -v`
Expected: 全部 PASS（含既有 send/receive/txn 等全部测试——批量原子性测试 `TestSendMessageRejectsBatchAtomically` 等必须依旧绿）

- [ ] **Step 5: 自检日志与注释**

- 批量成功路径 Debug 日志（topic/count/queue/first_offset）、失败 Warn 日志带上下文（Step 3 代码已含）。
- `batchable` doc 注释解释三类排除的 why（已含）。

- [ ] **Step 6: gofmt + 提交**

```bash
gofmt -l internal/rpc/ && go test ./internal/... && git add internal/rpc/ && git commit -m "feat(rpc): SendMessage 批量快路径——纯普通消息多条请求整批一次 fsync

官方 SDK batch send 即刻受益；含事务/延时/FIFO 的批回退逐条，历史行为不变。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: 默认队列数 4 → 16 + README 调优指引

**Files:**
- Modify: `internal/config/config.go:79`
- Modify: `internal/config/config_test.go:30`
- Modify: `README.md`（`default_queue_nums: 4` 所在配置示例 + 新增性能小节）

**Interfaces:**
- Consumes: 无。
- Produces: 新部署自动建 topic 默认 16 队列。存量 topic（队列数已持久化于 meta）不受影响。

- [ ] **Step 1: 改默认值与测试断言**

`internal/config/config.go:79`：`DefaultQueueNums: 4` → `DefaultQueueNums: 16`，并在行尾注释：`// 16：写吞吐 ≈ min(队列数,并发)×fsync速率×合并系数，4 会把云盘部署封在 ~1k msg/s`

`internal/config/config_test.go:30`：断言 `cfg.DefaultQueueNums != 4` → `cfg.DefaultQueueNums != 16`

- [ ] **Step 2: 跑配置测试**

Run: `go test ./internal/config/ -v`
Expected: 全部 PASS

- [ ] **Step 3: README 同步**

`README.md` 配置示例中 `default_queue_nums: 4` → `default_queue_nums: 16`，并在其附近（或性能章节）加入：

```markdown
### 写吞吐与队列数

写吞吐的第一决定因素是 topic 的队列数，不是磁盘速度：
`吞吐 ≈ min(队列数, 客户端并发) × fsync速率 × group commit 合并系数`。

- 默认 `default_queue_nums: 16` 适合大多数场景；高吞吐 topic 建议显式建
  topic 并给更多队列（Admin API `queues` 参数，上限 1024）。
- 单条 fsync 延迟决定单队列的串行下限；批量发送（SDK batch send）与更高
  客户端并发都能进一步摊薄 fsync。
- 参考实测（2 vCPU 云主机、fsync 456 次/s）：256 队列 + 256 并发可达
  5 万+ msg/s，内存峰值 ~72MB（由 Pebble 配置决定，不随负载增长）。
```

- [ ] **Step 4: 提交**

```bash
go test ./internal/... && git add internal/config/ README.md && git commit -m "feat(config): 默认队列数 4→16——解除自动建 topic 的写吞吐硬顶

只影响新自动建的 topic；存量 topic 队列数持久化于 meta 不受影响。
README 补写吞吐经验公式与调优指引。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: 基准入库——队列扫描、批量基准、soak 长跑

**Files:**
- Modify: `internal/core/produce/bench_test.go`（追加两个基准）
- Create: `internal/core/produce/soak_test.go`
- Modify: `Makefile`（追加 `soak` 目标，`.PHONY` 行同步）

**Interfaces:**
- Consumes: Task 3 的 `AppendBatch`；既有 `newBenchProducer` fixture。
- Produces: `BenchmarkAppendQueueSweep`、`BenchmarkAppendBatch32`、`TestSoak`（环境变量门控）、`make soak`。

- [ ] **Step 1: 追加基准**

在 `internal/core/produce/bench_test.go` 追加（import 增加 `fmt`）：

```go
// BenchmarkAppendQueueSweep 固定并发（由 -cpu/GOMAXPROCS 决定）只变队列数，
// 复现「写吞吐随队列数近似线性放大」的结论（spec §1）。改锁前后各跑一轮
// 即可量化 group commit 解锁对低队列数配置的收益。
func BenchmarkAppendQueueSweep(b *testing.B) {
	body := []byte("benchmark-payload-62B.........................................")
	for _, queues := range []uint32{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("q%d", queues), func(b *testing.B) {
			st, err := store.Open(b.TempDir(), true, slog.Default())
			if err != nil {
				b.Fatalf("store: %v", err)
			}
			b.Cleanup(func() { st.Close() })
			mt, err := meta.New(st, true, queues, 16, slog.Default())
			if err != nil {
				b.Fatalf("meta: %v", err)
			}
			p := New(st, mt, slog.Default())
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := p.Append(&core.Message{Topic: "t-sweep", Body: body}); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkAppendBatch32 批量写入基准：每次迭代 = 一批 32 条一次 fsync。
// 换算 msg/s 时乘 32（ns/op 是「每批」耗时，不是每条）。
func BenchmarkAppendBatch32(b *testing.B) {
	p, _ := newBenchProducer(b, b.TempDir())
	body := []byte("benchmark-payload-62B.........................................")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msgs := make([]*core.Message, 32)
			for i := range msgs {
				msgs[i] = &core.Message{Topic: "t-batch", Body: body}
			}
			if _, err := p.AppendBatch(msgs); err != nil {
				b.Fatal(err)
			}
		}
	})
}
```

- [ ] **Step 2: 本机烟测基准可运行**

Run: `go test ./internal/core/produce/ -bench 'BenchmarkAppendQueueSweep/q4|BenchmarkAppendBatch32' -benchtime 1s -run '^$'`
Expected: 两项输出 ns/op，无错误

- [ ] **Step 3: 创建 soak_test.go**

创建 `internal/core/produce/soak_test.go`：

```go
// Package produce 实现消息写入路径：队列选择、offset 分配、落盘、长轮询唤醒。
//
// 职责（soak 文件）：
//   - TestSoak：长时间持续写入基准，观察 Pebble compaction 稳态下吞吐是否
//     崩塌（短跑基准触发不了 compaction，数字会虚高——本文件补这个盲区）
//
// 边界：
//   - 默认跳过（SQ_SOAK=1 启用），绝不进普通 CI 路径
//   - 不做自动阈值断言：不同硬件基线不同，自动断言会把慢盘机器变成假失败；
//     验收由人工读打点日志判定（后半程均值 ≥ 前半程 70%，spec §6）
package produce

import (
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// TestSoak 持续高写入长跑：16 队列 / 64 并发 / 真实 fsync，每 10s 打点吞吐。
//
// 环境变量：
//   - SQ_SOAK=1 启用（否则 Skip）
//   - SQ_SOAK_DURATION 时长（默认 10m）
//   - SQ_SOAK_DIR 数据目录（默认 t.TempDir()——注意某些机器 /tmp 在 tmpfs 上，
//     量真实磁盘时必须显式指定）
func TestSoak(t *testing.T) {
	if os.Getenv("SQ_SOAK") == "" {
		t.Skip("soak 长跑默认跳过；SQ_SOAK=1 启用（make soak）")
	}
	dur := 10 * time.Minute
	if v := os.Getenv("SQ_SOAK_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("SQ_SOAK_DURATION 非法: %v", err)
		}
		dur = d
	}
	dir := t.TempDir()
	if v := os.Getenv("SQ_SOAK_DIR"); v != "" {
		dir = v
	}
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()
	mt, err := meta.New(st, true, 16, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	p := New(st, mt, slog.Default())
	logger := slog.Default().With("mod", "soak")
	logger.Info("soak 开始", "duration", dur.String(), "dir", dir, "queues", 16, "workers", 64)

	var total atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	body := make([]byte, 62)
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := p.Append(&core.Message{Topic: "t-soak", Body: body}); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				total.Add(1)
			}
		}()
	}
	start := time.Now()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	var last uint64
	for time.Since(start) < dur {
		<-tick.C
		cur := total.Load()
		// 每 10s 一条打点：验收就是人工读这串 rate_per_s 的走势
		logger.Info("soak 打点", "elapsed_s", int(time.Since(start).Seconds()),
			"total", cur, "rate_per_s", (cur-last)/10)
		last = cur
	}
	close(stop)
	wg.Wait()
	logger.Info("soak 结束", "total", total.Load(),
		"avg_per_s", total.Load()/uint64(dur.Seconds()))
}
```

- [ ] **Step 4: 烟测 soak（30 秒短跑）**

Run: `SQ_SOAK=1 SQ_SOAK_DURATION=30s go test ./internal/core/produce/ -run TestSoak -v -timeout 5m`
Expected: PASS，可见 3 条「soak 打点」日志与 1 条「soak 结束」

再确认默认跳过：`go test ./internal/core/produce/ -run TestSoak -v` → SKIP

- [ ] **Step 5: Makefile 目标**

`.PHONY` 行追加 ` soak`，文件末尾追加：

```makefile
# 写入 soak 长跑（默认 10 分钟，16 队列/64 并发/真实 fsync）。
# SQ_SOAK_DURATION=2m 缩短；SQ_SOAK_DIR=/path 指定真实磁盘目录
# （默认 TempDir 在部分机器上落 tmpfs，量不到真实 fsync）。
soak:
	SQ_SOAK=1 go test ./internal/core/produce/ -run TestSoak -v -timeout 30m
```

Run: `SQ_SOAK_DURATION=30s make soak`（环境变量前置——make 命令行变量不会自动进入 recipe 环境）
Expected: 与 Step 4 相同输出

- [ ] **Step 6: 自检日志与注释**

- soak 打点/开始/结束均为 slog 结构化日志（非 fmt.Printf），带 mod=soak（已含）。
- 新文件头注释含职责与边界（默认跳过、不自动断言的 why）（已含）。

- [ ] **Step 7: gofmt + 提交**

```bash
gofmt -l internal/core/produce/ && go test ./internal/... && git add internal/core/produce/ Makefile && git commit -m "test(produce): 基准入库——队列扫描、批量写入、soak 长跑

BenchmarkAppendQueueSweep 复现吞吐随队列数放大；BenchmarkAppendBatch32 量批量
收益；TestSoak（SQ_SOAK 门控 + make soak）补短跑基准量不到 compaction 稳态的
盲区。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: 云服务器全表验收（手动，交叉编译）

**Files:**
- Modify: `docs/superpowers/specs/2026-08-08-produce-throughput-optimization-design.md`（追加「附录：验收数据」）

**Interfaces:**
- Consumes: Task 1-6 全部完成后的代码；云服务器 root@47.80.240.57（免密）。
- Produces: spec 附录中的验收数据表。

- [ ] **Step 0: 本地全量回归（spec §7.5）**

```bash
make test && make e2e
```

Expected: 主模块全部单测 + e2e（含 FIFO、recovery、txn、delay）全绿。任一失败先修再进验收。

- [ ] **Step 1: 交叉编译测试二进制**（不在远端装工具链——本地 `go test -c` 后 scp）

```bash
GOOS=linux GOARCH=amd64 go test -c -o /tmp/produce.test ./internal/core/produce/
GOOS=linux GOARCH=amd64 go test -c -o /tmp/store.test ./internal/store/
scp /tmp/produce.test /tmp/store.test root@47.80.240.57:/root/
```

- [ ] **Step 2: 远端跑正确性测试（-race 不支持交叉编译产物，跑普通模式）**

```bash
ssh root@47.80.240.57 '/root/store.test -test.v -test.count=1 && /root/produce.test -test.run "Test" -test.v -test.count=1'
```

Expected: 全部 PASS

- [ ] **Step 3: 远端跑验收基准（64 并发形态用 -test.cpu 64）**

```bash
ssh root@47.80.240.57 'mkdir -p /root/sqbench && cd /root/sqbench && /root/produce.test -test.bench "BenchmarkAppendQueueSweep|BenchmarkAppendParallel|BenchmarkAppendBatch32" -test.benchtime 3s -test.run "^$" -test.cpu 64'
```

对照验收表（msg/s = 1e9/ns_op；AppendBatch32 乘 32）：

| 指标 | 基线 | 目标 | 对应基准 |
|---|---|---|---|
| 单队列 64 并发 | 435 | ≥ 2,000 | `QueueSweep/q1` |
| 16 队列 64 并发 | —（4 队列 ~900） | ≥ 3,000 | `QueueSweep/q16` |
| 批量 32 条/批 | ~435 | ≥ 5,000 | `AppendBatch32` × 32 |

- [ ] **Step 4: 远端跑 soak 10 分钟**

```bash
ssh root@47.80.240.57 'SQ_SOAK=1 SQ_SOAK_DURATION=10m SQ_SOAK_DIR=/root/sqbench/soak-data /root/produce.test -test.run TestSoak -test.v -test.timeout 30m'
```

Expected: 打点 rate_per_s 后半程均值 ≥ 前半程 70%，无 >5s 的 0 打点（写停顿）

- [ ] **Step 5: 数据回填 spec 附录并提交**

在 spec 文档末尾追加「## 附录：验收数据（2026-08-08，root@47.80.240.57）」，填入 Step 3/4 实测数字与达标结论；未达标项记录原因与后续任务。

```bash
git add docs/superpowers/specs/2026-08-08-produce-throughput-optimization-design.md && git commit -m "docs(spec): 回填写吞吐优化验收数据

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 6: 清理远端**

```bash
ssh root@47.80.240.57 'rm -rf /root/produce.test /root/store.test /root/sqbench'
```
