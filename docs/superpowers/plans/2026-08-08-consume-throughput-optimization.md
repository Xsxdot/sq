# 消费吞吐优化实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 produce 已验证的拆分提交模式搬到消费路径（Ack/receiveOnce/ChangeInvisible），加 AckBatch 批量确认与 rpc 分组，收口 AppendDelay/txn 的锁跨 fsync，让消费吞吐追上生产吞吐（云服务器同队列 ack 456/s → ≥2,000/s，批量 ≥5,000/s）。

**Architecture:** deliver 的队列锁只保护 inflight/cursor 的读-改-写与批次定序（`ApplyAsync` 为止），fsync 等待（`Pending.Wait`）挪到锁外，由 Pebble commit pipeline 合并同队列在途提交；AckMessage RPC 按 (group,topic,queue) 分组走新的 `AckBatch`（单 Batch 一次 fsync）；txn 全局锁按 txID 分片（读-改-写不变式是 per-txID 的）。

**Tech Stack:** Go + Pebble v2.1.6（`ApplyNoSyncWait`/`SyncWait` 已由 store.ApplyAsync/Pending.Wait 封装）；spec：`docs/superpowers/specs/2026-08-08-consume-throughput-optimization-design.md`。

## Global Constraints

- **工作环境**：一切操作在 worktree `/Users/xushixin/workspace/sq/.claude/worktrees/consume-throughput`（分支 `perf/consume-throughput-optimization`，基于 B2 提交 bb1a2c8）。**绝对禁止**在主 checkout `/Users/xushixin/workspace/sq` 执行任何 git 写操作或文件修改——它被另一个会话占用。
- **语义红线（spec §3.1，每个改动必须同时满足）**：① fsync 完成前绝不响应（取件不交件、ack 不返回、延时/事务不确认）；② inflight/cursor 的读-改-写仍在 (group,topic,queue) 队列锁内，`ApplyAsync` 返回（已发布 memtable）后才可解锁；③ 原子批不变（取件的重投+新 inflight+cursor 同批、批量 ack 的全部 Delete 同批、延时条目+seq 同批）；④ per-entry 语义不变（陈旧/缺失句柄逐条落空 `(false,nil)` 形态，仅存储故障整组报错）。
- **store.Batch 生命周期（B2 后 API）**：`st.NewBatch() *store.Batch`；`b.Set(k,v)`/`b.Delete(k)`（无第三参数）；提交成功归 Apply/ApplyAsync 处置；提交失败弃给 GC 不再碰；未提交而放弃必须 `b.Close()`。
- **日志**：只用注入的 `slog`（`d.logger`/`p.logger`/`t.logger`），禁止 `fmt.Printf`。错误分支带上下文（group/topic/queue/offset），成功路径不静默（Debug 级）。
- 每个 task 结束：`gofmt -l internal/` 无输出 + `go test ./internal/... -count=1` 全绿后才 commit。
- **验收硬件**：云服务器 root@47.80.240.57（2 vCPU Xeon、裸 fsync 456 次/s、免密登录）；远端跑测试一律本地交叉编译 `GOOS=linux GOARCH=amd64 go test -c` 后 scp，不装工具链。
- 基线（produce 侧已实测）：produce soak 稳态 ~22,000 msg/s；消费侧 ack 基线由 Task 1 实测回填。

---

### Task 1: BenchmarkAckParallel 基准入库与基线实测

**Files:**
- Create: `internal/core/deliver/bench_test.go`

**Interfaces:**
- Consumes: `store.Open/NewBatch/Apply`、`store.InflightKey`、`core.EncodeInflight`、`deliver.New`、`Deliverer.Ack`（现有签名 `Ack(group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error)`）。
- Produces: `BenchmarkAckParallel`（Task 2 复测用，名字不得改）。

- [ ] **Step 1: 写基准文件**

创建 `internal/core/deliver/bench_test.go`：

```go
// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责（基准文件）：
//   - BenchmarkAckParallel 量化"队列锁跨 fsync 导致同队列 ack 串行"的确认吞吐基线，
//     以及拆分提交后的收益（改锁前后各跑一轮对比）
//   - BenchmarkAckBatch32 量化 AckBatch 批量一次 fsync 的收益（Task 2 引入）
//
// 边界：
//   - 只跑基准，不包含断言测试
//   - 直接预写 inflight 记录构造可 ack 状态，不走 Receive（剥离取件成本，
//     量到的是纯确认路径）
package deliver

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// newBenchDeliverer 构造真实 fsync 的单队列 Deliverer，并预写 n 条
// attempt=1、过期时间远在将来的 inflight 记录（offset 0..n-1）。
// Ack 只读 inflight 不读消息本体，因此无需预写 msg/ 键。
func newBenchDeliverer(b *testing.B, n int) *Deliverer {
	b.Helper()
	st, err := store.Open(b.TempDir(), true, slog.Default())
	if err != nil {
		b.Fatalf("store: %v", err)
	}
	b.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		b.Fatalf("meta: %v", err)
	}
	pr := produce.New(st, mt, slog.Default())
	d := New(st, mt, pr, slog.Default())
	exp := time.Now().Add(time.Hour).UnixMilli()
	const chunk = 4096 // 分块提交，避免单个超大 Batch 撑爆内存
	for lo := 0; lo < n; lo += chunk {
		hi := lo + chunk
		if hi > n {
			hi = n
		}
		wb := st.NewBatch()
		for i := lo; i < hi; i++ {
			wb.Set(store.InflightKey("g-bench", "t-ack", 0, uint64(i)),
				core.EncodeInflight(&core.InflightState{ExpireAtMs: exp, Attempts: 1}))
		}
		if err := st.Apply(wb); err != nil {
			b.Fatalf("预写 inflight: %v", err)
		}
	}
	return d
}

// BenchmarkAckParallel 同队列并发逐条 ack。并发度由 -cpu/GOMAXPROCS 决定
// （云服务器验收用 -test.cpu 64）。改锁前（队列锁跨 fsync）此数≈单流 fsync
// 速率；拆分提交后应放大约「合并深度」倍。
func BenchmarkAckParallel(b *testing.B) {
	d := newBenchDeliverer(b, b.N)
	var next atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			off := next.Add(1) - 1
			ok, err := d.Ack("g-bench", "t-ack", 0, off, 1)
			if err != nil || !ok {
				b.Fatalf("ack off=%d: ok=%v err=%v", off, ok, err)
			}
		}
	})
}
```

- [ ] **Step 2: 本地跑通（验证接线，不作数）**

```bash
cd /Users/xushixin/workspace/sq/.claude/worktrees/consume-throughput
go test ./internal/core/deliver/ -run '^$' -bench BenchmarkAckParallel -benchtime 2s
```

Expected: 正常输出 ns/op（Mac 上参考值即可，验收基线以云服务器为准）。

- [ ] **Step 3: 云服务器实测基线**

```bash
cd /Users/xushixin/workspace/sq/.claude/worktrees/consume-throughput
GOOS=linux GOARCH=amd64 go test -c -o /tmp/deliver-baseline.test ./internal/core/deliver/
ssh root@47.80.240.57 'mkdir -p /root/sqbench'
scp /tmp/deliver-baseline.test root@47.80.240.57:/root/sqbench/
ssh root@47.80.240.57 'cd /root/sqbench && ./deliver-baseline.test -test.run "^$" -test.bench BenchmarkAckParallel -test.benchtime 3s -test.cpu 64'
```

Expected: ns/op ≈ 2,200,000（即 ~456 ack/s，等于单流 fsync 速率——锁跨 fsync 的串行证据）。把实测数字记进 Step 5 的 commit message，Task 7 回填 spec 附录基线列。

- [ ] **Step 4: gofmt + 全量单测**

```bash
gofmt -l internal/ && go test ./internal/... -count=1
```

Expected: gofmt 无输出，全绿。

- [ ] **Step 5: Commit**

```bash
git add internal/core/deliver/bench_test.go
git commit -m "test(deliver): ack 并发基准入库——云服务器基线实测 <实测值> ns/op（~456 ack/s，锁跨 fsync 串行证据）"
```

---

### Task 2: AckBatch（拆分提交形态）+ Ack 薄封装

**Files:**
- Modify: `internal/core/deliver/deliver.go`（`Ack` 函数整体替换，其后新增 `AckEntry`/`AckResult`/`AckBatch`）
- Modify: `internal/core/deliver/bench_test.go`（追加 `BenchmarkAckBatch32`）
- Test: `internal/core/deliver/deliver_test.go`（追加）

**Interfaces:**
- Consumes: `store.ApplyAsync(b *store.Batch) (store.Pending, error)`、`store.Pending.Wait() error`（B2 后签名）；现有 `lockQueue`、`store.InflightKey`、`core.DecodeInflight`。
- Produces（Task 4/6 依赖，签名精确如下）:
  - `type AckEntry struct { Offset uint64; Attempt int32 }`
  - `type AckResult struct { Offset uint64; OK bool }`
  - `func (d *Deliverer) AckBatch(group, topic string, queueID uint32, entries []AckEntry) ([]AckResult, error)`
  - `Ack` 签名不变（变成 AckBatch 单条封装），全部现有调用方零改动。

- [ ] **Step 1: 写失败测试**

在 `internal/core/deliver/deliver_test.go` 末尾追加（文件已 import `context`/`time`；本步需确认 import 含 `sync`、`sync/atomic`，Task 3 的并发测试也要用）：

```go
// TestAckBatchMixedEntries 验证批量确认的 per-entry 语义（语义红线 4）：
// 有效、陈旧 attempt、不存在的 offset 混在一批——逐条独立判定，
// 只有有效条目被删除，落空条目不影响其它条目也不报错。
func TestAckBatchMixedEntries(t *testing.T) {
	f := newFixture(t)
	f.send(t, "ab-t", "m0")
	f.send(t, "ab-t", "m1")
	msgs, err := f.dl.Receive(context.Background(), "g", "ab-t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("receive: %v msgs=%d", err, len(msgs))
	}
	results, err := f.dl.AckBatch("g", "ab-t", 0, []AckEntry{
		{Offset: msgs[0].Offset, Attempt: msgs[0].DeliveryAttempt}, // 有效
		{Offset: msgs[1].Offset, Attempt: 99},                      // 陈旧 attempt
		{Offset: 9999, Attempt: 1},                                 // 不存在
	})
	if err != nil {
		t.Fatalf("AckBatch: %v", err)
	}
	if len(results) != 3 || !results[0].OK || results[1].OK || results[2].OK {
		t.Fatalf("per-entry 结果错误: %+v", results)
	}
	// 有效条目的 inflight 已删；陈旧条目的 inflight 必须原样保留
	if _, ok, _ := f.st.Get(store.InflightKey("g", "ab-t", 0, msgs[0].Offset)); ok {
		t.Fatal("已确认消息的 inflight 未删除")
	}
	if _, ok, _ := f.st.Get(store.InflightKey("g", "ab-t", 0, msgs[1].Offset)); !ok {
		t.Fatal("陈旧句柄不应误删他人 inflight")
	}
}

// TestAckBatchAllInvalidNoWrite 验证全部落空时不产生任何写入（批次走
// 自行 Close 回收路径），且不报错。
func TestAckBatchAllInvalidNoWrite(t *testing.T) {
	f := newFixture(t)
	f.send(t, "ab2-t", "m")
	msgs, err := f.dl.Receive(context.Background(), "g", "ab2-t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v", err)
	}
	results, err := f.dl.AckBatch("g", "ab2-t", 0, []AckEntry{
		{Offset: msgs[0].Offset, Attempt: 42}, // 陈旧
		{Offset: 8888, Attempt: 1},            // 不存在
	})
	if err != nil || results[0].OK || results[1].OK {
		t.Fatalf("全落空批应无错且全 false: %v %+v", err, results)
	}
	if _, ok, _ := f.st.Get(store.InflightKey("g", "ab2-t", 0, msgs[0].Offset)); !ok {
		t.Fatal("inflight 不应被触碰")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
go test ./internal/core/deliver/ -run 'TestAckBatch' -v
```

Expected: 编译失败——`undefined: AckEntry`。

- [ ] **Step 3: 实现 AckBatch，Ack 改薄封装**

在 `internal/core/deliver/deliver.go` 中，将现有 `Ack` 方法（含其注释块）整体替换为：

```go
// Ack 确认消息。attempt 必须与该 offset 当前持久化的 InflightState.Attempts 一致——
// 消费者持有的 attempt 来自它收到的那次 Receive（core.Message.DeliveryAttempt）。
//
// 为什么要校验 attempt，而不是只按 (group,topic,queue,offset) 删记录：
// 若消费者 A 收到 X（attempt=1）后处理超时，X 会被过期重投给消费者 B（attempt=2，
// 全新的过期时间）。此时 A 迟到的 Ack(X) 若不带 attempt 校验，会直接删掉 B 那条
// 记录——X 从此既无 inflight 兜底、cursor 也已跳过它，一旦 B 处理失败或崩溃，
// X 就永久丢失，直接违反本包头注释声明的"已 ack 前消息不丢"。attempt 不匹配
// 说明这是一个陈旧句柄，语义上等价于"记录已不存在"：幂等返回 (false, nil)，不报错。
//
// inflight 不存在或 attempt 不匹配都返回 (false, nil)，幂等，不算错误。
// 实现即 AckBatch 单条形态，校验/锁/持久化语义完全一致。
func (d *Deliverer) Ack(group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error) {
	results, err := d.AckBatch(group, topic, queueID, []AckEntry{{Offset: offset, Attempt: attempt}})
	if err != nil {
		return false, err
	}
	return results[0].OK, nil
}

// AckEntry 批量确认的单条输入：offset + 消费者持有的 attempt（校验规则见 Ack 注释）。
type AckEntry struct {
	Offset  uint64
	Attempt int32
}

// AckResult 批量确认的单条结果。OK=false 即落空（已 ack / 已重投 / 陈旧
// attempt——三种情况统一归约，与 Ack 返回 (false,nil) 的幂等语义一致）。
type AckResult struct {
	Offset uint64
	OK     bool
}

// AckBatch 单一 (group,topic,queue) 的批量确认：一把队列锁、逐条校验、
// 有效条目的 Delete 合成单个 Batch、一次 fsync。
//
// 参数：entries 至少一条（rpc 层保证；空批直接返回 nil,nil 防御）。
// 返回：与 entries 同序的结果；error 仅存储故障（此时整组失败，任何条目
// 都未确认——单 Batch 原子性保证不存在部分生效）。
//
// 锁与持久化（与 produce/receiveOnce 的拆分提交同款论证）：
//   - 队列锁内完成全部 Get→校验→Delete 暂存与 ApplyAsync（定序 + memtable
//     发布），解锁后同队列的下一个拿锁者读到的 inflight 状态与提交顺序一致；
//   - fsync 等待（Wait）在锁外，同队列多个在途确认由 Pebble commit pipeline
//     合并为一次 fsync——ack 吞吐从 1/fsync延迟 解放为 合并深度/fsync延迟；
//   - Wait 成功前绝不向调用方报告确认成功（语义红线 1）。
func (d *Deliverer) AckBatch(group, topic string, queueID uint32, entries []AckEntry) ([]AckResult, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	results := make([]AckResult, len(entries))
	b := d.st.NewBatch()
	staged := 0
	for i, e := range entries {
		results[i] = AckResult{Offset: e.Offset}
		k := store.InflightKey(group, topic, queueID, e.Offset)
		v, ok, err := d.st.Get(k)
		if err != nil {
			qlock.Unlock()
			// 未提交而放弃的批次必须自行 Close 回收（store.NewBatch 契约路径 2）
			b.Close()
			return nil, fmt.Errorf("批量 ack 查询 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, e.Offset, err)
		}
		if !ok {
			// 已 ack 或已重投：幂等落空，不影响其它条目（语义红线 4）
			d.logger.Debug("批量 ack 目标不存在（重复 ack 或已重投）",
				"group", group, "topic", topic, "queue", queueID, "offset", e.Offset)
			continue
		}
		ist, err := core.DecodeInflight(v)
		if err != nil {
			qlock.Unlock()
			b.Close()
			return nil, fmt.Errorf("批量 ack 解码 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, e.Offset, err)
		}
		if ist.Attempts != e.Attempt {
			d.logger.Debug("批量 ack attempt 不匹配（陈旧句柄，已被重投覆盖）",
				"group", group, "topic", topic, "queue", queueID, "offset", e.Offset,
				"want_attempt", ist.Attempts, "got_attempt", e.Attempt)
			continue
		}
		b.Delete(k)
		results[i].OK = true
		staged++
	}
	if staged == 0 {
		qlock.Unlock()
		b.Close() // 全部落空：零写入，批次走自行回收路径
		return results, nil
	}
	pending, err := d.st.ApplyAsync(b)
	if err != nil {
		qlock.Unlock()
		// ApplyAsync 失败的批次按 store 约定弃给 GC，不再 Close
		return nil, fmt.Errorf("批量 ack 提交 (group=%s topic=%s q=%d n=%d): %w", group, topic, queueID, staged, err)
	}
	qlock.Unlock()
	// 锁外等待持久化：fsync 完成前绝不报告确认成功（语义红线 1）
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待批量 ack 持久化 (group=%s topic=%s q=%d n=%d): %w", group, topic, queueID, staged, err)
	}
	d.logger.Debug("消息已确认", "group", group, "topic", topic, "queue", queueID,
		"requested", len(entries), "acked", staged)
	return results, nil
}
```

同时更新 `Deliverer` 类型注释：把「Receive 经 receiveOnce、Ack、ChangeInvisible 三者」那句中的 `Ack` 说明为 `Ack/AckBatch`（新增任何直接读写 inflight 的方法必须持队列锁的告诫保持原样）。

- [ ] **Step 4: 追加 BenchmarkAckBatch32**

在 `internal/core/deliver/bench_test.go` 末尾追加：

```go
// BenchmarkAckBatch32 批量确认基准：每次迭代 = 一批 32 条一次 fsync。
// 换算 msg/s 时乘 32（ns/op 是「每批」耗时，不是每条）。
func BenchmarkAckBatch32(b *testing.B) {
	d := newBenchDeliverer(b, b.N*32)
	var next atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			base := (next.Add(1) - 1) * 32
			entries := make([]AckEntry, 32)
			for i := range entries {
				entries[i] = AckEntry{Offset: base + uint64(i), Attempt: 1}
			}
			results, err := d.AckBatch("g-bench", "t-ack", 0, entries)
			if err != nil {
				b.Fatalf("AckBatch: %v", err)
			}
			for _, r := range results {
				if !r.OK {
					b.Fatalf("entry off=%d 落空", r.Offset)
				}
			}
		}
	})
}
```

- [ ] **Step 5: 跑新测试与回归**

```bash
go test ./internal/core/deliver/ -run 'TestAckBatch|TestAck' -v -count=1
go test ./internal/core/deliver/ -count=1 -race
```

Expected: 全 PASS（现有 TestAckIdempotent/TestAckStaleAttemptRejected/TestAckCorrectAttemptSucceeds 是 Ack 薄封装化的回归网，必须全绿）。

- [ ] **Step 6: 基准复测（本地即可，验收在 Task 7 云服务器）**

```bash
go test ./internal/core/deliver/ -run '^$' -bench 'BenchmarkAck' -benchtime 2s
```

Expected: BenchmarkAckParallel 相比 Task 1 Step 2 的本地值显著放大（GOMAXPROCS 倍附近）；AckBatch32 的 ns/op÷32 远小于单条。

- [ ] **Step 7: 日志与注释自检（instrumenting-code 清单）**

- 错误分支全部带 group/topic/queue/offset 上下文 ✓（Step 3 代码已含）
- 成功路径不静默：`消息已确认` Debug 含 requested/acked ✓
- 落空分支有 Debug（不存在/attempt 不匹配）✓
- 新导出类型/方法有文档注释（AckEntry/AckResult/AckBatch）✓
- 无 fmt.Printf ✓

- [ ] **Step 8: gofmt + 全量单测 + Commit**

```bash
gofmt -l internal/ && go test ./internal/... -count=1
git add internal/core/deliver/deliver.go internal/core/deliver/deliver_test.go internal/core/deliver/bench_test.go
git commit -m "feat(deliver): AckBatch 批量确认（拆分提交）——同队列 N 条 ack 合成一次 fsync，Ack 变单条薄封装"
```

---

### Task 3: receiveOnce / ChangeInvisible 拆分提交

**Files:**
- Modify: `internal/core/deliver/deliver.go`（`receiveOnce` 拆成 wrapper + `receiveOnceLocked`；`ChangeInvisible` 拆成 wrapper + `changeInvisibleLocked`）
- Test: `internal/core/deliver/deliver_test.go`（追加并发回归测试）

**Interfaces:**
- Consumes: Task 2 已确立的拆分提交惯用法；`store.Pending`（零值可用，`needSync=false` 时 Wait 仅回收）。
- Produces: `receiveOnce` 对外行为与签名不变（`Receive` 零改动）；`ChangeInvisible` 签名不变。内部新增 `receiveOnceLocked(...) ([]*core.Message, store.Pending, bool, error)` 与 `changeInvisibleLocked(...) (store.Pending, bool, error)`。

- [ ] **Step 1: 写并发回归测试（先跑通旧实现，改后必须依旧绿）**

在 `internal/core/deliver/deliver_test.go` 追加（import 需含 `sync`、`sync/atomic`）：

```go
// TestConcurrentReceiveAckNoRace 是拆分提交的核心回归：8 个 worker 并发
// 取件+确认同一队列，验证 (1) 每条消息恰好被投递并确认一次（invisible 足够长，
// 无重投）(2) 结束后 inflight 清零 (3) -race 干净。若拆锁破坏了
// inflight/cursor 读-改-写的互斥（语义红线 2），本测试在 -race 下必然暴露。
func TestConcurrentReceiveAckNoRace(t *testing.T) {
	f := newFixture(t)
	const total = 300
	for i := 0; i < total; i++ {
		f.send(t, "cc-t", "m")
	}
	var acked atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for acked.Load() < total {
				msgs, err := f.dl.Receive(context.Background(), "g", "cc-t", 0, 32, time.Minute, 0, nil)
				if err != nil {
					t.Errorf("Receive: %v", err)
					return
				}
				for _, m := range msgs {
					ok, err := f.dl.Ack("g", "cc-t", 0, m.Offset, m.DeliveryAttempt)
					if err != nil {
						t.Errorf("Ack off=%d: %v", m.Offset, err)
						return
					}
					if ok {
						acked.Add(1)
					}
				}
				if len(msgs) == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
	if got := acked.Load(); got != total {
		t.Fatalf("确认总数 = %d, want %d（invisible 1 分钟内不应有重投）", got, total)
	}
	pfx := store.InflightPrefix("g", "cc-t", 0)
	n := 0
	if err := f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("全部确认后残留 %d 条 inflight", n)
	}
}
```

- [ ] **Step 2: 在旧实现上跑通（锁定行为基线）**

```bash
go test ./internal/core/deliver/ -run TestConcurrentReceiveAckNoRace -v -race -count=1
```

Expected: PASS（旧实现本来就该满足；不过它是串行 fsync，会比较慢）。

- [ ] **Step 3: 拆分 receiveOnce**

对 `internal/core/deliver/deliver.go` 做三处修改：

**(a)** 现有 `receiveOnce` 函数改名为 `receiveOnceLocked`，签名与开头改为（删除取锁三行，函数改为要求调用方持锁）：

```go
// receiveOnceLocked 单次取件的锁内部分：过期 inflight 重投 + 新消息扫描
// （可带 Tag 过滤），合计不超过 maxMsgs；批次组装完成后 ApplyAsync 定序。
// 调用方必须持有该队列的 qlock；fsync 等待由调用方在锁外完成。
//
// 返回：(消息, pending, applied, error)。applied=false 表示本轮无暂存写入
// （批次已 Close 回收），pending 为零值，调用方无需 Wait。
func (d *Deliverer) receiveOnceLocked(group, topic string, queueID uint32, maxMsgs int, invisible time.Duration, maxAttempts int32, filter *TagFilter) ([]*core.Message, store.Pending, bool, error) {
	now := time.Now().UnixMilli()
	...（原函数体，qlock := d.lockQueue(...) / qlock.Lock() / defer qlock.Unlock() 三行删除）
```

函数体内所有 `return nil, fmt.Errorf(...)` 形式的错误返回改为 `return nil, store.Pending{}, false, fmt.Errorf(...)`（共 6 处：扫 inflight 失败、读重投消息失败、解码重投消息失败、moveToDLQ 失败、读位点失败、扫新消息失败——每处前面的 `b.Close()` 原样保留）。

**(b)** 函数收尾（从 `if !staged {` 到函数结束）整体替换为：

```go
	if !staged {
		b.Close()
		return nil, store.Pending{}, false, nil
	}
	if newCursor > cursor {
		b.Set(store.CursorKey(group, topic, queueID), store.PutU64(newCursor))
	}
	pending, err := d.st.ApplyAsync(b)
	if err != nil {
		// ApplyAsync 失败的批次按 store 约定弃给 GC
		return nil, store.Pending{}, false, fmt.Errorf("提交取件批次 (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	if len(out) == 0 {
		// 本轮做了"无投递但有写入"的工作：清理孤儿 inflight（上面那条 Warn
		// 已说明是哪几条），或全部新消息被 Tag 过滤跳过、只推进了本组位点
		// （上方 Debug 日志说明跳过了几条）。单独一句话，不与投递共用消息：
		// 这恰恰是运维最需要读懂的一轮——打成"投递消息 count=0"会让人以为
		// 队列空转，从而忽略掉刚刚发生的数据修复或过滤推进。
		d.logger.Debug("本轮无可投递消息，仅清理了孤儿 inflight 或推进了过滤位点",
			"group", group, "topic", topic, "queue", queueID, "cursor", newCursor)
	} else {
		d.logger.Debug("投递消息已定序", "group", group, "topic", topic, "queue", queueID,
			"count", len(out), "redelivered", len(reds), "cursor", newCursor)
	}
	return out, pending, true, nil
```

**(c)** 新增 wrapper（放在 `receiveOnceLocked` 之前，保持原 `receiveOnce` 名字，`Receive` 零改动）：

```go
// receiveOnce 单次取件：锁内定序（receiveOnceLocked），锁外等待持久化。
// inflight 与 cursor 落盘之前绝不交件（语义红线 1/3——消费者拿到消息时，
// 它的 inflight 兜底记录必然已持久化，崩溃后该消息仍会被重投而不是丢失）。
// 解锁后同队列的下一次取件/确认立即可进锁定序，与本批共享同一次 fsync——
// 队列内 group commit 在消费路径生效的机制，与 produce 侧完全同款。
func (d *Deliverer) receiveOnce(group, topic string, queueID uint32, maxMsgs int, invisible time.Duration, maxAttempts int32, filter *TagFilter) ([]*core.Message, error) {
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	out, pending, applied, err := d.receiveOnceLocked(group, topic, queueID, maxMsgs, invisible, maxAttempts, filter)
	qlock.Unlock()
	if err != nil || !applied {
		return out, err
	}
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待取件批次持久化 (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	return out, nil
}
```

- [ ] **Step 4: 拆分 ChangeInvisible**

现有 `ChangeInvisible` 整体替换为（原方法注释保留在 wrapper 上）：

```go
// ChangeInvisible 重设不可见截止时间（消费端主动延长/缩短处理时间）。
// attempt 校验规则与 Ack 相同：必须与当前持久化的 Attempts 一致，否则视为
// 陈旧句柄，返回 (false, nil)——理由同 Ack 的注释：本方法也是一次 Get→改→Set
// 的读-改-写，若操作的是已被重投覆盖的旧记录，写回去的 ExpireAtMs 会作用在
// "别人正在处理的新一轮投递"上，等于让消费者延长了一个不属于自己的窗口。
//
// inflight 不存在或 attempt 不匹配都返回 (false, nil)，幂等，不算错误。
// 拆分提交：锁内定序（changeInvisibleLocked），锁外等 fsync——成功返回前
// 新的过期时间必然已持久化（语义红线 1）。
func (d *Deliverer) ChangeInvisible(group, topic string, queueID uint32, offset uint64, attempt int32, invisible time.Duration) (bool, error) {
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	pending, ok, err := d.changeInvisibleLocked(group, topic, queueID, offset, attempt, invisible)
	qlock.Unlock()
	if err != nil || !ok {
		return false, err
	}
	if err := pending.Wait(); err != nil {
		return false, fmt.Errorf("等待改不可见时间持久化 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Debug("已更新不可见时间", "group", group, "topic", topic, "queue", queueID,
		"offset", offset, "attempt", attempt, "invisible_ms", invisible.Milliseconds())
	return true, nil
}

// changeInvisibleLocked 锁内部分：Get→校验→Set→ApplyAsync。调用方必须持队列锁。
// 与 receiveOnce 的过期重投互斥的理由见 wrapper 注释（读-改-写不能与重投交错）。
func (d *Deliverer) changeInvisibleLocked(group, topic string, queueID uint32, offset uint64, attempt int32, invisible time.Duration) (store.Pending, bool, error) {
	k := store.InflightKey(group, topic, queueID, offset)
	v, ok, err := d.st.Get(k)
	if err != nil {
		return store.Pending{}, false, fmt.Errorf("改不可见时间查询 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if !ok {
		d.logger.Debug("改不可见时间目标不存在（已 ack 或已重投）", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return store.Pending{}, false, nil
	}
	ist, err := core.DecodeInflight(v)
	if err != nil {
		return store.Pending{}, false, fmt.Errorf("解码 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if ist.Attempts != attempt {
		d.logger.Debug("改不可见时间 attempt 不匹配（陈旧句柄，已被重投覆盖）",
			"group", group, "topic", topic, "queue", queueID, "offset", offset,
			"want_attempt", ist.Attempts, "got_attempt", attempt)
		return store.Pending{}, false, nil
	}
	ist.ExpireAtMs = time.Now().Add(invisible).UnixMilli()
	b := d.st.NewBatch()
	b.Set(k, core.EncodeInflight(ist))
	pending, err := d.st.ApplyAsync(b)
	if err != nil {
		return store.Pending{}, false, fmt.Errorf("改不可见时间 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	return pending, true, nil
}
```

- [ ] **Step 5: 全套回归 + race**

```bash
go test ./internal/core/deliver/ -count=1 -race
go test ./internal/rpc/ -count=1
```

Expected: 全 PASS——deliver 现有全套（顺序锁不变式、重投、退避、DLQ、Tag 过滤、孤儿清理）+ Step 1 的并发测试 + rpc 层（走 Receive/Ack 的集成路径）。

- [ ] **Step 6: 日志与注释自检**

- receiveOnceLocked 头注释说明「调用方必须持锁 + applied 语义」✓
- wrapper 注释说明可见性/交件顺序（防后人误改回锁内 Wait）✓
- 「投递消息已定序」措辞如实反映此刻状态（定序完成、fsync 未必完成）✓
- 错误分支上下文齐全 ✓

- [ ] **Step 7: gofmt + 全量单测 + Commit**

```bash
gofmt -l internal/ && go test ./internal/... -count=1
git add internal/core/deliver/deliver.go internal/core/deliver/deliver_test.go
git commit -m "perf(deliver): 取件与改不可见时间拆分提交——fsync 等待挪出队列锁，消费路径 group commit 生效"
```

---

### Task 4: rpc AckMessage 按队列分组走 AckBatch

**Files:**
- Modify: `internal/rpc/receive.go`（`AckMessage` 函数整体替换；`ackAggregateStatus` 不动）
- Test: `internal/rpc/receive_test.go`（追加）

**Interfaces:**
- Consumes: Task 2 的 `deliver.AckEntry`/`deliver.AckResult`/`AckBatch`；现有 `receiptDecode(secret, s) (group, topic, q, off, attempt, err)`、`truncateForLog`、`okStatus`/`errStatus`、`ackAggregateStatus`。
- Produces: `AckMessage` 对外协议行为不变（per-entry 状态、聚合规则、响应顺序均与现状一致）。

- [ ] **Step 1: 写失败测试**

在 `internal/rpc/receive_test.go` 追加（复用现有 `newTestClient`/`sendOne`/`receiveOne` 辅助）：

```go
// TestAckMessageBatchSameQueueSingleCommit 验证同队列多 entry 走批量路径：
// 全部成功、响应与请求同序；再次整批 ack 全部落空（INVALID_RECEIPT_HANDLE），
// 证明第一批真正生效且幂等语义不变。
func TestAckMessageBatchSameQueueSingleCommit(t *testing.T) {
	c := newTestClient(t)
	const topic = "ack-batch-q"
	const group = "g-ack-batch"
	// 同一队列凑 3 条：轮询 4 个队列多次发送，从队列 0 收满 3 条为止
	for i := 0; i < 12; i++ {
		sendOne(t, c, topic, "m")
	}
	var handles, ids []string
	for len(handles) < 3 {
		m := receiveOne(t, c, group, topic, 0, time.Minute)
		if m == nil {
			t.Fatal("队列 0 未收到足够消息")
		}
		handles = append(handles, m.GetSystemProperties().GetReceiptHandle())
		ids = append(ids, m.GetSystemProperties().GetMessageId())
	}
	req := &pb.AckMessageRequest{Group: &pb.Resource{Name: group}, Topic: &pb.Resource{Name: topic}}
	for i := range handles {
		req.Entries = append(req.Entries, &pb.AckMessageEntry{ReceiptHandle: handles[i], MessageId: ids[i]})
	}
	resp, err := c.AckMessage(context.Background(), req)
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("批量 ack: %v %v", resp.GetStatus(), err)
	}
	if len(resp.GetEntries()) != 3 {
		t.Fatalf("entries = %d, want 3", len(resp.GetEntries()))
	}
	for i, e := range resp.GetEntries() {
		if e.GetStatus().GetCode() != pb.Code_OK || e.GetMessageId() != ids[i] {
			t.Fatalf("entry %d 顺序或状态错误: %v", i, e)
		}
	}
	// 重放同一批：全部应落空（幂等语义与逐条路径一致）
	again, err := c.AckMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("重放: %v", err)
	}
	for i, e := range again.GetEntries() {
		if e.GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE {
			t.Fatalf("重放 entry %d 应 INVALID_RECEIPT_HANDLE: %v", i, e)
		}
	}
}

// TestAckMessageMixedQueuesGrouped 验证跨队列 entries 正确分组：不同队列的
// 消息在同一请求中确认，全部成功且响应保持请求顺序。
func TestAckMessageMixedQueuesGrouped(t *testing.T) {
	c := newTestClient(t)
	const topic = "ack-mix-q"
	const group = "g-ack-mix-q"
	sendOne(t, c, topic, "a")
	sendOne(t, c, topic, "b")
	// 两条消息按轮询落在不同队列，逐队列收齐
	var msgs []*pb.Message
	for q := int32(0); q < 4 && len(msgs) < 2; q++ {
		if m := receiveOne(t, c, group, topic, q, time.Minute); m != nil {
			msgs = append(msgs, m)
		}
	}
	if len(msgs) != 2 {
		t.Fatalf("期望 2 条，实际 %d", len(msgs))
	}
	req := &pb.AckMessageRequest{Group: &pb.Resource{Name: group}, Topic: &pb.Resource{Name: topic}}
	for _, m := range msgs {
		req.Entries = append(req.Entries, &pb.AckMessageEntry{
			ReceiptHandle: m.GetSystemProperties().GetReceiptHandle(),
			MessageId:     m.GetSystemProperties().GetMessageId(),
		})
	}
	resp, err := c.AckMessage(context.Background(), req)
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("跨队列批量 ack: %v %v", resp.GetStatus(), err)
	}
	for i, e := range resp.GetEntries() {
		if e.GetStatus().GetCode() != pb.Code_OK ||
			e.GetMessageId() != msgs[i].GetSystemProperties().GetMessageId() {
			t.Fatalf("entry %d 顺序或状态错误: %v", i, e)
		}
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
go test ./internal/rpc/ -run 'TestAckMessageBatchSameQueue|TestAckMessageMixedQueues' -v
```

Expected: 目前逐条路径下这两个测试其实会 PASS（行为外观相同）——这是行为保持型改造，测试的价值是改后回归。确认 PASS 后继续（这一步锁定基线，与 Task 3 Step 2 同理）。

- [ ] **Step 3: 替换 AckMessage 实现**

`internal/rpc/receive.go` 中 `AckMessage` 整体替换（函数头注释一并更新）：

```go
// AckMessage 批量确认。两遍处理：第一遍逐条解码 handle（失败的当场生成
// per-entry 失败结果，不影响其它条目）；第二遍把解码成功的按
// (group,topic,queue) 分组，每组一次 deliver.AckBatch——同队列多条合成
// 单个 Pebble Batch 一次 fsync（官方 SDK 批量 ack 即刻受益）。
//
// per-entry 语义与逐条时代完全一致：落空（已 ack/已重投/陈旧 attempt）
// 翻译成 INVALID_RECEIPT_HANDLE（SDK 收到即静默丢弃、不重试，正是想要的）；
// 存储故障时该组全部 INTERNAL_SERVER_ERROR。响应 entries 严格保持请求顺序。
// 顶层 Status 仍由 ackAggregateStatus 聚合（规则与理由见该函数注释）。
func (s *Server) AckMessage(ctx context.Context, req *pb.AckMessageRequest) (*pb.AckMessageResponse, error) {
	type ackSlot struct {
		idx     int
		offset  uint64
		attempt int32
		e       *pb.AckMessageEntry
	}
	type ackKey struct {
		group string
		topic string
		q     uint32
	}
	entries := make([]*pb.AckMessageResultEntry, len(req.GetEntries()))
	groups := make(map[ackKey][]ackSlot)
	var order []ackKey // 按首次出现序处理各组，行为确定可测
	for i, e := range req.GetEntries() {
		g, topic, q, off, attempt, err := receiptDecode(s.handleSecret, e.GetReceiptHandle())
		if err != nil {
			// 非法 handle 是客户端问题（篡改/损坏/过期协议版本），Warn 即可，
			// 不是服务端故障。handle 是客户端可控的任意长度字符串，日志里只留
			// 截断预览（truncateForLog），不把不受信任的输入原样灌进日志。
			s.logger.Warn("ack handle 非法", "handle", truncateForLog(e.GetReceiptHandle()), "err", err)
			entries[i] = &pb.AckMessageResultEntry{
				// ReceiptHandle 必须回填：MessageId 在客户端请求里可能为空
				// （proto 字段非必填），此时 ReceiptHandle 是客户端把这条失败
				// 结果关联回自己请求里对应 entry 的唯一线索。
				Status:        errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error()),
				MessageId:     e.GetMessageId(),
				ReceiptHandle: e.GetReceiptHandle(),
			}
			continue
		}
		k := ackKey{group: g, topic: topic, q: q}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], ackSlot{idx: i, offset: off, attempt: attempt, e: e})
	}
	for _, k := range order {
		slots := groups[k]
		acks := make([]deliver.AckEntry, len(slots))
		for j, sl := range slots {
			acks[j] = deliver.AckEntry{Offset: sl.offset, Attempt: sl.attempt}
		}
		results, err := s.dl.AckBatch(k.group, k.topic, k.q, acks)
		if err != nil {
			// 存储故障：该组整体失败（AckBatch 单 Batch 原子，不存在部分生效），
			// 客户端对这些 entry 重试即可
			s.logger.Error("批量 ack 失败", "group", k.group, "topic", k.topic,
				"queue", k.q, "count", len(slots), "err", err)
			for _, sl := range slots {
				entries[sl.idx] = &pb.AckMessageResultEntry{
					Status:        errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error()),
					MessageId:     sl.e.GetMessageId(),
					ReceiptHandle: sl.e.GetReceiptHandle(),
				}
			}
			continue
		}
		for j, sl := range slots {
			st := okStatus()
			if !results[j].OK {
				// 落空的三种情况已在 deliver 层归约成 OK=false，翻译规则与
				// 逐条时代一致（理由见旧实现注释：SDK 收到该码即静默丢弃）
				st = errStatus(pb.Code_INVALID_RECEIPT_HANDLE, "receipt 已失效")
			}
			entries[sl.idx] = &pb.AckMessageResultEntry{
				Status: st, MessageId: sl.e.GetMessageId(), ReceiptHandle: sl.e.GetReceiptHandle(),
			}
		}
	}
	s.logger.Debug("AckMessage 处理完成", "entries", len(entries), "groups", len(order))
	return &pb.AckMessageResponse{Status: ackAggregateStatus(entries), Entries: entries}, nil
}
```

确认 `internal/rpc/receive.go` 的 import 含 `github.com/xushixin/sq/internal/core/deliver`（Server 已持有 `*deliver.Deliverer`，通常已在）。

- [ ] **Step 4: 回归**

```bash
go test ./internal/rpc/ -count=1 -race
```

Expected: 全 PASS——重点回归 `TestAckMessageAggregatesMixedResultsAsMultipleResults`（聚合规则）、`TestAckMessageMalformedHandleEntryIncludesReceiptHandle`（非法 handle 回填）、`TestAckWithStaleAttemptTokenFailsAndMessageStaysRedeliverable`（陈旧句柄）与 Step 1 两个新测试。

- [ ] **Step 5: 日志与注释自检**

- handle 非法 Warn（截断）、存储故障 Error（含组坐标与条数）、成功 Debug（entries/groups）✓
- 函数头注释解释两遍处理与 per-entry 翻译规则 ✓

- [ ] **Step 6: gofmt + 全量单测 + Commit**

```bash
gofmt -l internal/ && go test ./internal/... -count=1
git add internal/rpc/receive.go internal/rpc/receive_test.go
git commit -m "feat(rpc): AckMessage 按队列分组走 AckBatch——同队列多 entry 一次 fsync，协议行为不变"
```

---

### Task 5: AppendDelay 拆分提交 + txn 锁按 txID 分片

**Files:**
- Modify: `internal/core/produce/produce.go`（`AppendDelay` 的落盘段）
- Modify: `internal/core/txn/txn.go`（`Manager.mu` → 分片；`End`/`checkOne`/`dropLocked` 三处取锁）
- Test: `internal/core/produce/produce_test.go`、`internal/core/txn/txn_test.go`（各追加）

**Interfaces:**
- Consumes: `store.ApplyAsync`/`Pending.Wait`；`hash/fnv`。
- Produces: `AppendDelay`/`End`/`checkOne`/`dropLocked` 签名与外部行为均不变。

- [ ] **Step 1: 写失败（基线）测试**

`internal/core/produce/produce_test.go` 追加：

```go
// TestAppendDelayConcurrentSeqUnique 并发写延时消息，验证 seq 不重不漏：
// delay 条目总数与 delayalloc 计数器严格一致。拆分提交若破坏 seq 分配的
// 临界区（提前推进逻辑写错），本测试必挂。
func TestAppendDelayConcurrentSeqUnique(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	due := time.Now().Add(time.Hour).UnixMilli()
	const goroutines, perG = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := p.AppendDelay(&core.Message{Topic: "dly-cc", Body: []byte("x"), DeliverAtMs: due}); err != nil {
					t.Errorf("AppendDelay: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	total := uint64(goroutines * perG)
	n := 0
	pfx := []byte(store.DelayPrefix)
	if err := st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if uint64(n) != total {
		t.Fatalf("delay 条目 = %d, want %d（seq 撞号会覆盖变少）", n, total)
	}
	v, ok, err := st.Get(store.DelayAllocKey())
	if err != nil || !ok || store.GetU64(v) != total {
		t.Fatalf("delayalloc = %v ok=%v err=%v, want %d", v, ok, err, total)
	}
}
```

`internal/core/txn/txn_test.go` 追加（import 需含 `sync`）：

```go
// TestEndConcurrentDistinctTx 并发决断互不相同的 txID：分片锁下不同事务
// 并行提交，全部 found=true 且 half 暂存区清零。若分片实现让同 txID 的
// 互斥失效，现有 TestEndTwiceSecondIsNoop 等幂等测试会先暴露。
func TestEndConcurrentDistinctTx(t *testing.T) {
	f := newFixture(t, time.Minute, 15)
	const n = 16
	txIDs := make([]string, n)
	for i := 0; i < n; i++ {
		_, txID, err := f.mgr.Stage(&core.Message{Topic: "tx-cc", Body: []byte("x")})
		if err != nil {
			t.Fatalf("Stage: %v", err)
		}
		txIDs[i] = txID
	}
	var wg sync.WaitGroup
	for _, id := range txIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			found, err := f.mgr.End(id, true)
			if err != nil || !found {
				t.Errorf("End(%s): found=%v err=%v", id, found, err)
			}
		}(id)
	}
	wg.Wait()
	if got := f.halfCount(t); got != 0 {
		t.Fatalf("并发提交后 half 暂存区残留 %d 条", got)
	}
}
```

- [ ] **Step 2: 在旧实现上跑通（锁定基线）+ race**

```bash
go test ./internal/core/produce/ -run TestAppendDelayConcurrentSeqUnique -v -race -count=1
go test ./internal/core/txn/ -run TestEndConcurrentDistinctTx -v -race -count=1
```

Expected: 均 PASS（旧实现语义正确，只是串行慢）。

- [ ] **Step 3: AppendDelay 拆分提交**

`internal/core/produce/produce.go` 中 `AppendDelay` 的落盘段（从 `p.delayMu.Lock()` 到 `return m, nil`）替换为：

```go
	p.delayMu.Lock()
	seq, err := p.nextDelaySeqLocked()
	if err != nil {
		p.delayMu.Unlock()
		return nil, err
	}
	b := p.st.NewBatch()
	b.Set(store.DelayKey(m.DeliverAtMs, seq), raw)
	b.Set(store.DelayAllocKey(), store.PutU64(seq+1))
	pending, err := p.st.ApplyAsync(b)
	if err != nil {
		p.delayMu.Unlock()
		return nil, fmt.Errorf("写入延时消息 %s (topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
	}
	// 定序成功即推进 seq 缓存并解锁：并发的延时写入随即进锁定序，与本条共享
	// 同一次 fsync（拆分提交，与 AppendWith 的 qs.next 同款）。提前推进的安全性
	// 论证也相同：WaitSync 失败 == WAL sync 失败 == Pebble 不可恢复错误态，
	// 重启后 seq 计数器与条目由同批原子提交保证一致，内存里烧掉的 seq 无害。
	p.delayNext = seq + 1
	p.delayLoaded = true
	p.delayMu.Unlock()

	// 锁外等待持久化：fsync 完成之前绝不确认（语义红线 1）
	if err := pending.Wait(); err != nil {
		return nil, fmt.Errorf("等待延时消息 %s 持久化 (topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
	}
	p.logger.Debug("延时消息已暂存", "topic", m.Topic, "msg_id", m.ID,
		"due_ms", m.DeliverAtMs, "seq", seq)
	return m, nil
```

注意：原实现是 `p.delayMu.Lock()` + `defer p.delayMu.Unlock()`——替换后不再用 defer，每个分支显式解锁（与 AppendWith 风格一致）。

- [ ] **Step 4: txn 锁分片**

`internal/core/txn/txn.go` 四处修改：

**(a)** import 增加 `"hash/fnv"`。

**(b)** `Manager` 结构体的 `mu sync.Mutex` 字段与类型注释替换为：

```go
// Manager 事务管理器。并发安全：锁按 txID 分片（txnLockShards 片）——
// 「同一 txID 的状态迁移」必须串行（End 与回查改期都会搬移 half 键，交错
// 会把已决断的事务复活成僵尸），但不同 txID 之间毫无共享状态，全局锁徒然
// 让所有事务的 fsync 串行化。同 txID 恒定落在同一分片，互斥保证不变。
type Manager struct {
	st            *store.Store
	pr            *produce.Producer
	mt            *meta.Meta
	checkInterval time.Duration
	maxChecks     int
	logger        *slog.Logger

	mus     [txnLockShards]sync.Mutex
	checks  atomic.Uint64 // 累计回查排期次数（/metrics 的 sq_txn_checks_total）
	dropped atomic.Uint64 // 累计超限丢弃条数（/metrics 的 sq_txn_dropped_total）
}

// txnLockShards 事务锁分片数。32 片对「低频但可能并发」的事务决断绰绰有余；
// 片内条目哈希碰撞只影响并行度，不影响正确性。
const txnLockShards = 32

// lockFor 返回 txID 对应的分片锁。fnv 与 produce 的 FIFO 选队保持同一哈希家族。
func (t *Manager) lockFor(txID string) *sync.Mutex {
	h := fnv.New32a()
	h.Write([]byte(txID))
	return &t.mus[h.Sum32()%txnLockShards]
}
```

**(c)** `End` 与 `checkOne` 开头的：

```go
	t.mu.Lock()
	defer t.mu.Unlock()
```

各替换为：

```go
	mu := t.lockFor(txID)
	mu.Lock()
	defer mu.Unlock()
```

**(d)** `dropLocked` 同样替换（它的参数就有 txID）。`New` 构造函数无需改动（零值分片数组即可用）。

- [ ] **Step 5: 回归 + race**

```bash
go test ./internal/core/produce/ ./internal/core/txn/ ./internal/core/delay/ -count=1 -race
```

Expected: 全 PASS——txn 现有全套（End 幂等、回查改期、超限丢弃、坏条目清理）是分片正确性的回归网。

- [ ] **Step 6: 日志与注释自检**

- AppendDelay 提前推进 seq 的 why 注释 ✓；成功 Debug 保留 ✓
- Manager 分片理由写进类型注释（为什么全局锁过宽、为什么同 txID 仍互斥）✓
- 无新增静默路径 ✓

- [ ] **Step 7: gofmt + 全量单测 + Commit**

```bash
gofmt -l internal/ && go test ./internal/... -count=1
git add internal/core/produce/produce.go internal/core/produce/produce_test.go internal/core/txn/txn.go internal/core/txn/txn_test.go
git commit -m "perf(produce,txn): 延时写入拆分提交 + 事务锁按 txID 分片——收口低流量路径的锁跨 fsync"
```

---

### Task 6: 端到端 soak（TestSoakE2E）+ Makefile

**Files:**
- Create: `internal/core/deliver/soak_test.go`
- Modify: `Makefile`（`.PHONY` 行与新 target）

**Interfaces:**
- Consumes: Task 2 的 `AckBatch`；`produce.Append`、`Receive`。
- Produces: `TestSoakE2E`（SQ_SOAK=1 门控）、`make soak-e2e`。

- [ ] **Step 1: 写 soak 文件**

创建 `internal/core/deliver/soak_test.go`：

```go
// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责（soak 文件）：
//   - TestSoakE2E：边写边消费的端到端长跑，观察消费速率能否持续跟上生产、
//     堆积（produced−acked）是否单调增长（短跑基准量不到 compaction 与
//     inflight 累积的稳态——本文件补这个盲区）
//
// 边界：
//   - 默认跳过（SQ_SOAK=1 启用），绝不进普通 CI 路径
//   - 不做自动阈值断言（不同硬件基线不同）；验收由人工读打点判定：
//     ack_rate 均值 ≥ produce_rate 均值的 80%，backlog 曲线不单调增长（spec §3.7）
package deliver

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// TestSoakE2E 端到端长跑：16 队列 / 64 个 producer / 16 个 consumer（每队列
// 一个，Receive 32 条批量 + AckBatch 整批确认）/ 真实 fsync，每 10s 打点。
//
// 环境变量：
//   - SQ_SOAK=1 启用（否则 Skip）
//   - SQ_SOAK_DURATION 时长（默认 10m）
//   - SQ_SOAK_DIR 数据目录（默认 t.TempDir()——某些机器 /tmp 在 tmpfs 上，
//     量真实磁盘时必须显式指定）
func TestSoakE2E(t *testing.T) {
	if os.Getenv("SQ_SOAK") == "" {
		t.Skip("soak 长跑默认跳过；SQ_SOAK=1 启用（make soak-e2e）")
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
	pr := produce.New(st, mt, slog.Default())
	dl := New(st, mt, pr, slog.Default())
	logger := slog.Default().With("mod", "soak-e2e")
	logger.Info("soak-e2e 开始", "duration", dur.String(), "dir", dir,
		"queues", 16, "producers", 64, "consumers", 16)

	var produced, acked atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	body := make([]byte, 62)

	// 64 个 producer：持续写入
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
				if _, err := pr.Append(&core.Message{Topic: "t-soak-e2e", Body: body}); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				produced.Add(1)
			}
		}()
	}
	// 16 个 consumer：每队列一个，批量取件 + 整批确认（同时压测 AckBatch 路径）
	for q := uint32(0); q < 16; q++ {
		wg.Add(1)
		go func(q uint32) {
			defer wg.Done()
			ctx := context.Background()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// invisible 5 分钟：soak 全程不触发重投，acked 数即唯一确认数
				msgs, err := dl.Receive(ctx, "g-soak", "t-soak-e2e", q, 32, 5*time.Minute, 200*time.Millisecond, nil)
				if err != nil {
					t.Errorf("Receive q=%d: %v", q, err)
					return
				}
				if len(msgs) == 0 {
					continue
				}
				entries := make([]AckEntry, len(msgs))
				for i, m := range msgs {
					entries[i] = AckEntry{Offset: m.Offset, Attempt: m.DeliveryAttempt}
				}
				results, err := dl.AckBatch("g-soak", "t-soak-e2e", q, entries)
				if err != nil {
					t.Errorf("AckBatch q=%d: %v", q, err)
					return
				}
				for _, r := range results {
					if r.OK {
						acked.Add(1)
					}
				}
			}
		}(q)
	}

	start := time.Now()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	var lastP, lastA uint64
	for time.Since(start) < dur {
		<-tick.C
		p, a := produced.Load(), acked.Load()
		// 每 10s 一条打点：验收就是人工读 produce_rate/ack_rate/backlog 的走势
		logger.Info("soak-e2e 打点", "elapsed_s", int(time.Since(start).Seconds()),
			"produce_rate", (p-lastP)/10, "ack_rate", (a-lastA)/10, "backlog", p-a)
		lastP, lastA = p, a
	}
	close(stop)
	wg.Wait()
	p, a := produced.Load(), acked.Load()
	logger.Info("soak-e2e 结束", "produced", p, "acked", a, "backlog", p-a,
		"avg_produce_per_s", p/uint64(dur.Seconds()), "avg_ack_per_s", a/uint64(dur.Seconds()))
}
```

- [ ] **Step 2: Makefile**

`.PHONY` 行追加 `soak-e2e`；`soak:` target 之后追加：

```makefile
# 端到端 soak（默认 10 分钟，16 队列 / 64 producer / 16 consumer / 真实 fsync）。
# 判定：ack_rate 均值 ≥ produce_rate 均值的 80%，backlog 不单调增长。
# SQ_SOAK_DURATION=2m 缩短；SQ_SOAK_DIR=/path 指定真实磁盘目录。
soak-e2e:
	SQ_SOAK=1 go test ./internal/core/deliver/ -run TestSoakE2E -v -timeout 30m
```

- [ ] **Step 3: 短跑验证接线**

```bash
SQ_SOAK=1 SQ_SOAK_DURATION=30s go test ./internal/core/deliver/ -run TestSoakE2E -v -timeout 5m
```

Expected: 跑 30 秒，打点里 produce_rate/ack_rate/backlog 三列都有合理数字（本地 Mac 上 ack_rate 应与 produce_rate 同量级），正常结束无 goroutine 泄漏报错。

- [ ] **Step 4: gofmt + 全量单测 + Commit**

```bash
gofmt -l internal/ && go test ./internal/... -count=1
git add internal/core/deliver/soak_test.go Makefile
git commit -m "test(deliver): 端到端 soak 入库——边写边消费打点 produce/ack/backlog，SQ_SOAK 门控"
```

---

### Task 7: 云服务器验收与数据回填

**Files:**
- Modify: `docs/superpowers/specs/2026-08-08-consume-throughput-optimization-design.md`（追加附录）

**Interfaces:**
- Consumes: Task 1-6 全部产物；云服务器 root@47.80.240.57。
- Produces: spec 附录验收数据；远端清理干净。

- [ ] **Step 0: 本地全量回归**

```bash
cd /Users/xushixin/workspace/sq/.claude/worktrees/consume-throughput
make test && make e2e
```

Expected: 全绿（e2e 覆盖官方 SDK 收发/ack 的真实链路）。

- [ ] **Step 1: 交叉编译 + 上传**

```bash
GOOS=linux GOARCH=amd64 go test -c -o /tmp/deliver.test ./internal/core/deliver/
GOOS=linux GOARCH=amd64 go test -c -o /tmp/produce.test ./internal/core/produce/
GOOS=linux GOARCH=amd64 go test -c -o /tmp/txn.test ./internal/core/txn/
GOOS=linux GOARCH=amd64 go test -c -o /tmp/rpc.test ./internal/rpc/
ssh root@47.80.240.57 'mkdir -p /root/sqbench'
scp /tmp/deliver.test /tmp/produce.test /tmp/txn.test /tmp/rpc.test root@47.80.240.57:/root/sqbench/
```

- [ ] **Step 2: 远端正确性**

```bash
ssh root@47.80.240.57 'cd /root/sqbench && ./deliver.test -test.run Test -test.count=1 && ./produce.test -test.run Test -test.count=1 && ./txn.test -test.run Test -test.count=1 && ./rpc.test -test.run Test -test.count=1'
```

Expected: 全部 PASS（soak 类测试因未设 SQ_SOAK 自动 Skip；交叉编译产物不支持 -race，普通模式）。

- [ ] **Step 3: 验收基准**

```bash
ssh root@47.80.240.57 'cd /root/sqbench && ./deliver.test -test.run "^$" -test.bench "BenchmarkAck" -test.benchtime 3s -test.cpu 64'
```

Expected（spec §2）：AckParallel ≥2,000 ack/s（即 ns/op ≤ 500,000）；AckBatch32 换算（1e9/ns_op×32）≥5,000 msg/s。对照 Task 1 记录的基线（~456/s）计算放大倍数。

- [ ] **Step 4: 远端端到端 soak 10 分钟**

```bash
ssh root@47.80.240.57 'cd /root/sqbench && mkdir -p soak-e2e-data && SQ_SOAK=1 SQ_SOAK_DURATION=10m SQ_SOAK_DIR=/root/sqbench/soak-e2e-data ./deliver.test -test.run TestSoakE2E -test.v -test.timeout 30m'
```

Expected: 打点全程 ack_rate 均值 ≥ produce_rate 均值的 80%；backlog 有波动但不单调增长；无 >5s 的 0 打点。

- [ ] **Step 5: 数据回填 spec 附录并提交**

在 `docs/superpowers/specs/2026-08-08-consume-throughput-optimization-design.md` 末尾追加「## 附录：验收数据（日期，root@47.80.240.57）」：环境说明、Step 0 回归结果、Step 2 正确性、Step 3 基准原始输出与换算表（含 Task 1 基线列与放大倍数）、Step 4 打点序列与判定表、结论与未达标项（如有）。

```bash
git add docs/superpowers/specs/2026-08-08-consume-throughput-optimization-design.md
git commit -m "docs(spec): 回填消费吞吐优化验收数据"
```

- [ ] **Step 6: 清理远端**

```bash
ssh root@47.80.240.57 'rm -rf /root/sqbench'
```

---

## 完成后

分支 `perf/consume-throughput-optimization` 含 spec + 7 个实现 commit。合并回 main 前注意：本分支基于 B2（bb1a2c8，来自 v2-batch1-replication-foundation）——若 V2 分支先合并，本分支直接接上；若本分支先合并，main 会连带引入 B2（内容自洽、测试全绿，无碍）。合并时机与方式由用户决定。
