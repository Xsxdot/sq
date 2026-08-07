# M6 事务消息实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 RocketMQ 5.x 语义的事务消息：半消息暂存（消费不可见）、`EndTransaction` 提交/回滚、超时未决经 Telemetry 双向流下发 `RecoverOrphanedTransaction` 回查，官方 Go SDK 全链路（commit / rollback / 回查）通过。

**Architecture:** 沿用 delay 的「暂存区 + 后台调度器」模式：`SendMessage`(TRANSACTION) 在 produce.Append 之前分流，写 `half/{下次回查ms:8B}{txId}`（值=完整编码消息）+ `halfidx/{txId}`（值=回查状态），同批原子提交；`EndTransaction`(commit) 经 `produce.AppendWith` 原子移入 `msg/` 并删除两个 half 键，(rollback) 原子删除；回查调度器周期扫 `half/` 到期项，经新增的 Telemetry 会话注册表挑一个发布该 topic 的 producer 流下发回查命令，改期重扫，超限（默认 15 次）丢弃并记日志。会话注册表同时补上 M5 递延的「连接数」指标与控制台展示。

**Tech Stack:** Go（Pebble、grpc-go、prometheus/client_golang）+ React 18/TS/Vite 控制台 + 官方 rocketmq-clients Go SDK 做 e2e。

## Global Constraints

- 工作区：**从 `main`（`c8b8ae1` 或更新）新建 worktree + 分支 `m6-transaction-messages`**（用 superpowers:using-git-worktrees）。绝不触碰 `/Users/xushixin/workspace/sq`（m5c 会话在用）、`sq-m5`、`sq-m5b` 三个既有 worktree。本计划文档作为分支首个提交入库。
- core 不 import 任何 proto/pb 包（spec §3 协议适配层约束）——回查下发经 `txn.Notifier` 接口反转依赖。
- store 写入一律经 `store.Apply`（唯一写入口，v2 Raft 拦截点）；DAO 级键编码只在 `internal/store/keys.go` 定义。
- 日志用 `log/slog`（`logger.With("mod", ...)` 惯例），禁止 `fmt.Printf`；错误日志必带 topic/msgId/txId 上下文（spec §8）。
- 注释规范：新文件顶部职责/边界头注释；导出方法 doc 注释；复杂逻辑中文「为什么」注释（全局 CLAUDE.md §2）。
- 每个实现 task 含「加关键节点日志」与「加注释」步骤（instrumenting-code 纪律）；成功路径不得静默。
- 提交信息结尾：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。
- 时钟策略沿用 spec §7：回查依赖墙钟，回拨仅导致回查推迟，不丢失不提前。
- 磁盘水位拒写只拦 `SendMessage`（含事务半消息，既有检查已覆盖）；`EndTransaction` 不拦——提交是把已落盘的数据移位（净增量≈0），拦了只会让半消息滞留、回查空转，与 delay 到期搬运不受水位限制的既有取舍一致。

## 文件结构

| 文件 | 动作 | 职责 |
|---|---|---|
| `internal/store/keys.go` | 修改 | `half/`、`halfidx/` 键编码（schema 唯一事实源） |
| `internal/config/config.go` | 修改 | `txn_check_interval`（默认 30s）、`txn_max_checks`（默认 15） |
| `internal/core/types.go` | 修改 | `Message.Transactional bool json:"-"`（路由标记，不落盘） |
| `internal/core/txn/txn.go` | 新建 | 事务管理器：Stage/End/回查调度（spec §3 的 txn 引擎） |
| `internal/rpc/sessions.go` | 新建 | Telemetry 会话注册表（回查路由 + 连接数） |
| `internal/rpc/server.go` | 修改 | Telemetry 注册会话、`AcceptMessageTypes` 加 TRANSACTION、`New` 签名 |
| `internal/rpc/send.go` | 修改 | TRANSACTION 校验分支 + Stage 路由 + `SendResultEntry.TransactionId` |
| `internal/rpc/txn.go` | 新建 | `EndTransaction` handler + `RecoverOrphan`（core.Message→pb 回查命令） |
| `internal/metrics/stats.go` / `collector.go` | 修改 | `HalfDepth` gauge、回查计数 counter、连接数 gauge |
| `internal/admin/server.go` / `messages.go` | 修改 | `GET /admin/transactions`、overview 加 `half_depth`/`connections` |
| `cmd/sq/main.go` | 修改 | 装配 txn.Manager + 回查 goroutine（停机顺序同 delay） |
| `web/src/...` | 修改/新建 | 事务页（镜像 Delay 页）、Overview 连接数 |
| `test/e2e/sdk_txn_test.go` | 新建 | 官方 SDK commit/rollback/回查全链路 |
| `README.md` | 修改 | 消息类型、配置项、metrics、控制台页面表同步 |

---

### Task 1: store 层 half/halfidx 键编码

**Files:**
- Modify: `internal/store/keys.go`
- Test: `internal/store/keys_test.go`（追加用例）

**Interfaces:**
- Produces:
  - `HalfPrefix` 常量（`"half/"`，导出供 txn/metrics/admin 扫描）
  - `HalfKey(nextCheckMs int64, txID string) []byte`
  - `HalfScanUpperBound(nowMs int64) []byte`
  - `ParseHalfKey(k []byte) (nextCheckMs int64, txID string, err error)`
  - `HalfIdxKey(txID string) []byte`

- [ ] **Step 1: 写失败测试**

在 `keys_test.go` 追加（对齐既有 Delay 键测试风格）：

```go
func TestHalfKeyRoundTrip(t *testing.T) {
	k := HalfKey(1723000000000, "ABCD1234")
	ms, txID, err := ParseHalfKey(k)
	if err != nil {
		t.Fatalf("ParseHalfKey: %v", err)
	}
	if ms != 1723000000000 || txID != "ABCD1234" {
		t.Fatalf("回读不一致: ms=%d txID=%q", ms, txID)
	}
}

func TestHalfKeyOrdering(t *testing.T) {
	// 字节序=数值序：早到期的 key 必须小于晚到期的
	a := HalfKey(1000, "ZZZZ")
	b := HalfKey(2000, "AAAA")
	if bytes.Compare(a, b) >= 0 {
		t.Fatalf("到期时间序被破坏: %q >= %q", a, b)
	}
}

func TestHalfScanUpperBound(t *testing.T) {
	// 上界必须恰好包含 nowMs 内全部 txID，且不含 nowMs+1 的任何条目
	now := int64(5000)
	in := HalfKey(5000, "\xff\xff")   // nowMs 内字典序最大的 txID 形态
	out := HalfKey(5001, "")
	ub := HalfScanUpperBound(now)
	if bytes.Compare(in, ub) >= 0 {
		t.Fatalf("nowMs 内条目落在扫描区间外")
	}
	if bytes.Compare(out, ub) < 0 {
		t.Fatalf("nowMs+1 条目落进扫描区间")
	}
}

func TestParseHalfKeyRejectsBadKey(t *testing.T) {
	for _, bad := range [][]byte{
		[]byte("half/short"),          // 不足 8B ms
		HalfKey(1000, ""),             // txID 为空
		[]byte("delay/xxxxxxxxxxxx"),  // 前缀不对
	} {
		if _, _, err := ParseHalfKey(bad); err == nil {
			t.Fatalf("坏 key %q 未被拒绝", bad)
		}
	}
}

func TestHalfIdxKey(t *testing.T) {
	if got := string(HalfIdxKey("TX1")); got != "halfidx/TX1" {
		t.Fatalf("HalfIdxKey = %q", got)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/store/ -run 'TestHalf' -v`
Expected: FAIL（undefined: HalfKey 等）

- [ ] **Step 3: 实现键编码**

在 `keys.go` 前缀常量块追加 `halfPrefix = "half/"`、`halfIdxPrefix = "halfidx/"`、`HalfPrefix = halfPrefix`，并在 DelayAllocKey 附近追加：

```go
// HalfKey 事务半消息暂存条目：half/{nextCheckMs:8B}{txID}，值为完整编码消息（spec §4）。
// nextCheckMs 是下次回查时间（墙钟 UnixMilli 恒为正），大端编码保证扫描按到期升序；
// txID 由服务端生成（core.NewMessageID，定长 32 位十六进制），天然唯一故无需 seq。
func HalfKey(nextCheckMs int64, txID string) []byte {
	k := make([]byte, 0, len(halfPrefix)+8+len(txID))
	k = append(k, halfPrefix...)
	k = append(k, PutU64(uint64(nextCheckMs))...)
	return append(k, txID...)
}

// HalfScanUpperBound 到期回查扫描 [HalfPrefix, 本上界) 的开区间上界：
// nextCheckMs+1 的空 txID key，恰好纳入 nowMs 内全部条目（任意 txID 均排在
// 空串之后同一毫秒段内），不含 nowMs+1 的任何条目——与 DelayScanUpperBound 同构。
func HalfScanUpperBound(nowMs int64) []byte { return HalfKey(nowMs+1, "") }

// ParseHalfKey 解析半消息条目 key：前缀后先 8B 定长 ms，剩余全部为 txID（非空）。
func ParseHalfKey(k []byte) (int64, string, error) {
	rest, ok := bytes.CutPrefix(k, []byte(halfPrefix))
	if !ok || len(rest) <= 8 {
		return 0, "", fmt.Errorf("非法 half key: %q", k)
	}
	return int64(binary.BigEndian.Uint64(rest[:8])), string(rest[8:]), nil
}

// HalfIdxKey 半消息反查索引：halfidx/{txID}，值为 JSON 编码的回查状态
// （见 txn.HalfRef）。EndTransaction 只拿到 txID，靠它反查 half/ 条目当前位置。
func HalfIdxKey(txID string) []byte { return []byte(halfIdxPrefix + txID) }
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/store/ -run 'TestHalf' -v`
Expected: PASS

- [ ] **Step 5: 注释自检**

上面代码已含 doc 注释；核对：每个导出函数说明了参数含义与「为什么」（txID 无需 seq 的理由、上界构造的边界推理）。纯键编码无 I/O，无日志要求。

- [ ] **Step 6: 提交**

```bash
git add internal/store/keys.go internal/store/keys_test.go
git commit -m "feat(store): 事务半消息 half/halfidx 键编码"
```

---

### Task 2: config 事务回查配置项

**Files:**
- Modify: `internal/config/config.go`
- Test: `internal/config/config_test.go`（追加用例）

**Interfaces:**
- Produces: `Config.TxnCheckInterval string`（yaml `txn_check_interval`，默认 `"30s"`）、`Config.TxnMaxChecks int`（yaml `txn_max_checks`，默认 15）、`(*Config).TxnInterval() time.Duration`

- [ ] **Step 1: 写失败测试**

```go
func TestTxnConfigDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TxnCheckInterval != "30s" || cfg.TxnInterval() != 30*time.Second {
		t.Fatalf("txn_check_interval 默认值错误: %q", cfg.TxnCheckInterval)
	}
	if cfg.TxnMaxChecks != 15 {
		t.Fatalf("txn_max_checks 默认值错误: %d", cfg.TxnMaxChecks)
	}
}

func TestTxnConfigValidation(t *testing.T) {
	// 写盘一个坏配置再 Load，风格对齐既有校验用例
	for name, body := range map[string]string{
		"负间隔":  "txn_check_interval: \"-1s\"",
		"零间隔":  "txn_check_interval: \"0s\"",
		"非法串":  "txn_check_interval: \"abc\"",
		"零次数":  "txn_max_checks: 0",
		"负次数":  "txn_max_checks: -3",
	} {
		p := filepath.Join(t.TempDir(), "sq.yaml")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(p); err == nil {
			t.Fatalf("%s 应在启动时报错", name)
		}
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/config/ -run 'TestTxn' -v`
Expected: FAIL

- [ ] **Step 3: 实现**

Config 结构体（M5 区块之后）追加：

```go
	// —— M6 事务消息 ——
	// TxnCheckInterval 半消息回查间隔（Go duration 格式）。半消息落盘后第一次
	// 回查发生在写入后一个间隔，此后每次回查后再排一个间隔，直到决断或超限。
	TxnCheckInterval string `yaml:"txn_check_interval"`
	// TxnMaxChecks 单条半消息最大回查次数，超限即丢弃并记日志（spec §5 流程 5）。
	TxnMaxChecks int `yaml:"txn_max_checks"`
```

默认值：`TxnCheckInterval: "30s", TxnMaxChecks: 15`。校验（对齐 retention_check_interval 的写法与理由——空串/拼错不能让回查调度器空转或整趟跳过）：

```go
	if d, err := time.ParseDuration(cfg.TxnCheckInterval); err != nil || d <= 0 {
		return nil, fmt.Errorf("配置 txn_check_interval 须为正 duration（如 30s），得到 %q", cfg.TxnCheckInterval)
	}
	// 上限 1000：回查是丢弃前的最后防线，配大到近乎无限等于永不丢弃，
	// half/ 区会被永远无人决断的僵尸条目占满
	if cfg.TxnMaxChecks < 1 || cfg.TxnMaxChecks > 1000 {
		return nil, fmt.Errorf("配置 txn_max_checks 须在 [1,1000]，得到 %d", cfg.TxnMaxChecks)
	}
```

`TxnInterval()` 与 `RetentionInterval()` 同款（Load 已校验过，忽略二次解析错误）。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/config/ -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(config): txn_check_interval 与 txn_max_checks 配置项"
```

---

### Task 3: core/txn 事务管理器（Stage / End 状态机）

**Files:**
- Modify: `internal/core/types.go`（Message 加 `Transactional` 字段）
- Create: `internal/core/txn/txn.go`
- Test: `internal/core/txn/txn_test.go`

**Interfaces:**
- Consumes: `store.HalfKey/HalfIdxKey/ParseHalfKey/HalfScanUpperBound/HalfPrefix`（Task 1）、`produce.(*Producer).AppendWith(m, extra)`、`meta.(*Meta).EnsureTopic`、`core.NewMessageID/EncodeMessage/DecodeMessage`
- Produces:
  - `txn.New(st *store.Store, pr *produce.Producer, mt *meta.Meta, checkInterval time.Duration, maxChecks int, logger *slog.Logger) *Manager`
  - `(*Manager).Stage(m *core.Message) (*core.Message, string, error)` — 返回（写入后消息, txID）
  - `(*Manager).End(txID string, commit bool) (found bool, err error)` — found=false 表示 txID 不存在（幂等场景）
  - `txn.HalfRef{NextCheckMs int64; Checks int}`（halfidx 值，JSON）
  - `(*Manager).ChecksTotal() uint64`、`(*Manager).DroppedTotal() uint64`（Task 4 递增，Task 9 消费）
- `core.Message` 追加字段：`Transactional bool \`json:"-"\``（仅路由用，不落盘——半消息的身份由所在 `half/` 前缀表达，提交后即普通消息）

- [ ] **Step 1: 写失败测试**

`txn_test.go`（fixture 对齐 delay_test.go 的 newFixture 模式）：

```go
// txn 状态机测试（spec §10 核心单测第 2 项）。
package txn

import (
	"log/slog"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

type fixture struct {
	st  *store.Store
	pr  *produce.Producer
	dl  *deliver.Deliverer
	mgr *Manager
}

func newFixture(t *testing.T, interval time.Duration, maxChecks int) *fixture {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(st, mt, slog.Default())
	dl := deliver.New(st, mt, pr, slog.Default())
	return &fixture{st: st, pr: pr, dl: dl, mgr: New(st, pr, mt, interval, maxChecks, slog.Default())}
}

func (f *fixture) halfCount(t *testing.T) int {
	t.Helper()
	n := 0
	pfx := []byte(store.HalfPrefix)
	if err := f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func msg(topic string) *core.Message {
	return &core.Message{Topic: topic, Body: []byte("hello"), Transactional: true}
}

func TestStageWritesHalfAndIdxAtomically(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	m, txID, err := f.mgr.Stage(msg("t-txn"))
	if err != nil {
		t.Fatal(err)
	}
	if txID == "" || m.ID == "" {
		t.Fatalf("Stage 未生成 txID/msgID: %q %q", txID, m.ID)
	}
	if f.halfCount(t) != 1 {
		t.Fatalf("half 条目数 = %d", f.halfCount(t))
	}
	if _, ok, _ := f.st.Get(store.HalfIdxKey(txID)); !ok {
		t.Fatal("halfidx 未写入")
	}
	// 半消息对消费不可见：msg/ 区必须没有任何条目
	pfx := []byte("msg/")
	n := 0
	f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil })
	if n != 0 {
		t.Fatalf("半消息漏进了 msg/：%d 条", n)
	}
}

func TestCommitMovesToMsgAndCleansHalf(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	_, txID, _ := f.mgr.Stage(msg("t-txn"))
	found, err := f.mgr.End(txID, true)
	if err != nil || !found {
		t.Fatalf("End(commit): found=%v err=%v", found, err)
	}
	if f.halfCount(t) != 0 {
		t.Fatal("commit 后 half 条目残留")
	}
	if _, ok, _ := f.st.Get(store.HalfIdxKey(txID)); ok {
		t.Fatal("commit 后 halfidx 残留")
	}
	// 提交后可正常消费到
	got, err := f.dl.Receive("g", "t-txn", 0, 1, 10*time.Second, "", "*")
	if err != nil || len(got) != 1 {
		t.Fatalf("提交后的消息不可消费: %v %d", err, len(got))
	}
}

func TestRollbackDeletesEverything(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	_, txID, _ := f.mgr.Stage(msg("t-txn"))
	found, err := f.mgr.End(txID, false)
	if err != nil || !found {
		t.Fatalf("End(rollback): found=%v err=%v", found, err)
	}
	if f.halfCount(t) != 0 {
		t.Fatal("rollback 后 half 条目残留")
	}
	got, _ := f.dl.Receive("g", "t-txn", 0, 1, 10*time.Second, "", "*")
	if len(got) != 0 {
		t.Fatal("rollback 的消息被消费到了")
	}
}

func TestEndUnknownTxIDIsIdempotent(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	found, err := f.mgr.End("NO-SUCH-TX", true)
	if err != nil {
		t.Fatalf("未知 txID 不该报错（幂等）: %v", err)
	}
	if found {
		t.Fatal("未知 txID 不该 found")
	}
}

func TestEndTwiceSecondIsNoop(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	_, txID, _ := f.mgr.Stage(msg("t-txn"))
	f.mgr.End(txID, true)
	found, err := f.mgr.End(txID, true) // SDK 网络重试会走到这里
	if err != nil || found {
		t.Fatalf("重复 commit 应为幂等 no-op: found=%v err=%v", found, err)
	}
	// 消息只有一条，没有被重复投入
	got, _ := f.dl.Receive("g", "t-txn", 0, 10, 10*time.Second, "", "*")
	if len(got) != 1 {
		t.Fatalf("重复 End 导致消息条数 = %d", len(got))
	}
}
```

> 注意：`deliver.Receive` 的真实签名以 `internal/core/deliver/deliver.go` 为准（M2 起带 tag 过滤参数）；写测试时按现有 delay/deliver 测试中的调用样式对齐，上面的调用形参仅为示意结构，**落码前先看一眼既有测试怎么调 Receive**。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/txn/ -v`
Expected: FAIL（包不存在）

- [ ] **Step 3: 实现 Manager**

`internal/core/txn/txn.go`：

```go
// Package txn 实现事务消息管理器：半消息暂存、提交/回滚、回查调度
// （spec §3 的 txn 引擎，§5 流程 5）。
//
// 职责：
//   - Stage：TRANSACTION 消息在 produce.Append 之前分流至 half/ 暂存区，
//     half 条目与 halfidx 反查索引同批原子提交
//   - End：commit 经 produce.AppendWith 原子移入 msg/ 并删除两键；
//     rollback 原子删除两键；txID 不存在时幂等返回 found=false
//   - RunChecker：周期扫描到期半消息，经 Notifier 下发回查、改期重扫，
//     超限丢弃（Task 4 实现）
//
// 边界：
//   - 不 import 任何 proto/pb（回查命令的协议编码在 rpc 层，经 Notifier 反转）
//   - 不管提交后的消费语义（deliver 的事）；提交后的消息就是普通消息
//   - 崩溃恢复零代码：half/halfidx 全在 Pebble，重启后扫描即恢复；
//     每次状态迁移都是单批原子操作，不存在两键不一致的中间态
package txn

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// HalfRef halfidx/{txID} 的值：半消息当前所在 half/ 键的定位信息与回查计数。
// NextCheckMs 是 half key 里的 8B 时间戳——End 只拿到 txID，必须靠它重建
// half key 才能找到消息本体；Checks 是已排期的回查轮数，超过上限即丢弃。
type HalfRef struct {
	NextCheckMs int64 `json:"next_check_ms"`
	Checks      int   `json:"checks"`
}

// Manager 事务管理器。并发安全：mu 串行化「同一 txID 的状态迁移」——
// End（客户端决断）与回查调度器的改期都会搬移 half 键，两者交错时后者
// 必须看到前者的结果，否则已提交的事务会被改期逻辑复活成僵尸半消息。
type Manager struct {
	st            *store.Store
	pr            *produce.Producer
	mt            *meta.Meta
	checkInterval time.Duration
	maxChecks     int
	logger        *slog.Logger

	mu      sync.Mutex
	checks  atomic.Uint64 // 累计回查排期次数（/metrics 的 sq_txn_checks_total）
	dropped atomic.Uint64 // 累计超限丢弃条数（/metrics 的 sq_txn_dropped_total）
}

// New 构造事务管理器。checkInterval/maxChecks 来自 config（已在 Load 校验为正）。
func New(st *store.Store, pr *produce.Producer, mt *meta.Meta,
	checkInterval time.Duration, maxChecks int, logger *slog.Logger) *Manager {
	return &Manager{st: st, pr: pr, mt: mt,
		checkInterval: checkInterval, maxChecks: maxChecks,
		logger: logger.With("mod", "txn")}
}

// Stage 暂存一条事务半消息，返回（写入后消息, 服务端生成的 txID）。
//
// 半消息不分配队列与 offset（提交移入 msg/ 时才由正常写入路径分配），
// 因此对消费者天然不可见——deliver 只扫 msg/。
// 首次回查排在 now+checkInterval：客户端正常几毫秒内就会 EndTransaction，
// 只有本地事务悬而未决（进程崩溃、网络分区）的孤儿才会活到第一次回查。
func (t *Manager) Stage(m *core.Message) (*core.Message, string, error) {
	if len(m.Body) == 0 || len(m.Body) > produce.MaxBodySize {
		return nil, "", fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), produce.MaxBodySize)
	}
	// 与 AppendDelay 同理：错误要在发送端立刻暴露，不能等到提交时才发现
	// topic 不存在、消息无处可去
	if _, err := t.mt.EnsureTopic(m.Topic); err != nil {
		return nil, "", err
	}
	if m.ID == "" {
		m.ID = core.NewMessageID()
	}
	if m.BornAtMs == 0 {
		m.BornAtMs = time.Now().UnixMilli()
	}
	m.StoreAtMs = time.Now().UnixMilli()
	raw, err := core.EncodeMessage(m)
	if err != nil {
		return nil, "", err
	}
	txID := core.NewMessageID()
	next := time.Now().Add(t.checkInterval).UnixMilli()
	ref, _ := json.Marshal(&HalfRef{NextCheckMs: next, Checks: 0}) // 结构固定无失败路径
	// half 条目与反查索引同批原子提交：崩溃后两键要么都在要么都不在，
	// End 靠 halfidx 定位、调度器靠 half 扫描，任何一键单独存在都是孤儿
	b := t.st.NewBatch()
	b.Set(store.HalfKey(next, txID), raw, nil)
	b.Set(store.HalfIdxKey(txID), ref, nil)
	if err := t.st.Apply(b); err != nil {
		return nil, "", fmt.Errorf("写入半消息 %s (topic=%s tx=%s): %w", m.ID, m.Topic, txID, err)
	}
	t.logger.Info("事务半消息已暂存", "topic", m.Topic, "msg_id", m.ID,
		"tx_id", txID, "next_check_ms", next)
	return m, txID, nil
}

// End 决断一条事务：commit=true 原子移入 msg/，false 原子删除。
//
// 返回 found=false 表示 txID 当前不存在半消息——三种正常来源：客户端网络
// 重试（第一次已生效）、回查决断与客户端决断赛跑输的一方、超限已丢弃。
// 三者都不是错误，调用方按幂等成功处理（记 Warn 即可）。
func (t *Manager) End(txID string, commit bool) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	refRaw, ok, err := t.st.Get(store.HalfIdxKey(txID))
	if err != nil {
		return false, fmt.Errorf("读取 halfidx (tx=%s): %w", txID, err)
	}
	if !ok {
		return false, nil
	}
	ref := &HalfRef{}
	if err := json.Unmarshal(refRaw, ref); err != nil {
		return false, fmt.Errorf("解码 halfidx (tx=%s): %w", txID, err)
	}
	halfKey := store.HalfKey(ref.NextCheckMs, txID)
	raw, ok, err := t.st.Get(halfKey)
	if err != nil {
		return false, fmt.Errorf("读取半消息 (tx=%s): %w", txID, err)
	}
	if !ok {
		// 两键同批写入/删除，正常不可能只剩 idx。真走到这里说明数据被外部
		// 改写，删掉孤儿 idx 止损并 Error 留痕（与 delay 清坏条目同理）
		t.logger.Error("halfidx 存在但 half 条目缺失，删除孤儿索引",
			"tx_id", txID, "next_check_ms", ref.NextCheckMs)
		b := t.st.NewBatch()
		b.Delete(store.HalfIdxKey(txID), nil)
		if aerr := t.st.Apply(b); aerr != nil {
			return false, fmt.Errorf("删除孤儿 halfidx (tx=%s): %w", txID, aerr)
		}
		return false, nil
	}
	if !commit {
		b := t.st.NewBatch()
		b.Delete(halfKey, nil)
		b.Delete(store.HalfIdxKey(txID), nil)
		if err := t.st.Apply(b); err != nil {
			return false, fmt.Errorf("回滚删除半消息 (tx=%s): %w", txID, err)
		}
		t.logger.Info("事务已回滚", "tx_id", txID, "checks", ref.Checks)
		return true, nil
	}
	m, err := core.DecodeMessage(raw)
	if err != nil {
		return false, fmt.Errorf("解码半消息 (tx=%s): %w", txID, err)
	}
	// 写 msg/（正常分配队列与 offset、写 keyidx、唤醒长轮询）+ 删两个 half 键，
	// 同一 Batch 原子提交：与 delay 到期搬运同构，不存在丢失或重复
	idxKey := store.HalfIdxKey(txID)
	stored, err := t.pr.AppendWith(m, func(b *pebble.Batch) {
		b.Delete(halfKey, nil)
		b.Delete(idxKey, nil)
	})
	if err != nil {
		return false, fmt.Errorf("提交半消息 (tx=%s msg=%s topic=%s): %w", txID, m.ID, m.Topic, err)
	}
	t.logger.Info("事务已提交", "tx_id", txID, "msg_id", stored.ID,
		"topic", stored.Topic, "queue", stored.QueueID, "offset", stored.Offset, "checks", ref.Checks)
	return true, nil
}

// ChecksTotal 返回累计回查排期次数（含下发失败的轮次；见 Pass 的注释）。
func (t *Manager) ChecksTotal() uint64 { return t.checks.Load() }

// DroppedTotal 返回累计超限丢弃条数。
func (t *Manager) DroppedTotal() uint64 { return t.dropped.Load() }
```

同时在 `internal/core/types.go` 的 Message 结构体 `DeliveryAttempt` 之前追加：

```go
	// Transactional 事务消息路由标记（M6）：true 时 SendMessage 分流至
	// txn.Stage 而非 produce.Append。不落盘——半消息的身份由所在 half/
	// 前缀表达，提交移入 msg/ 后它就是普通消息。
	Transactional bool `json:"-"`
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/txn/ -v && go test ./internal/core/... -count=1`
Expected: PASS（且既有 core 测试无回归）

- [ ] **Step 5: 日志自检（instrumenting-code）**

核对：Stage 成功路径 Info（topic/msg_id/tx_id/next_check_ms）✓；End 提交/回滚成功各一条 Info（tx_id + 上下文）✓；每个错误分支 `fmt.Errorf` 带 txID 上下文向上传播（rpc 层落日志）✓；孤儿 idx 异常 Error ✓。无静默成功路径。

- [ ] **Step 6: 注释自检**

包头职责/边界 ✓；HalfRef/Manager/每个导出方法 doc 注释 ✓；「为什么首查排在 now+interval」「为什么 found=false 是幂等而非错误」「为什么两键必须同批」均有中文说明 ✓。

- [ ] **Step 7: 提交**

```bash
git add internal/core/types.go internal/core/txn/
git commit -m "feat(txn): 事务管理器——半消息暂存与提交/回滚状态机"
```

---

### Task 4: core/txn 回查调度器

**Files:**
- Modify: `internal/core/txn/txn.go`
- Test: `internal/core/txn/checker_test.go`

**Interfaces:**
- Produces:
  - `txn.Notifier` 接口：`RecoverOrphan(m *core.Message, txID string) bool`（true=已成功下发到某个 producer 流；rpc.Server 在 Task 7 实现）
  - `(*Manager).RunChecker(ctx context.Context, n Notifier)`（阻塞循环，main 放独立 goroutine）
  - `(*Manager).Pass(n Notifier) (int, error)`（单趟，测试直调）
  - 包级 `var scanInterval = time.Second`、`var maxCheckPerPass = 256`（测试可注入，对齐 delay 包做法）

- [ ] **Step 1: 写失败测试**

`checker_test.go`：

```go
package txn

import (
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/store"
)

// fakeNotifier 记录收到的回查请求，可编程返回值。
type fakeNotifier struct {
	got  []string // 收到的 txID 序列
	send bool     // RecoverOrphan 返回值
}

func (f *fakeNotifier) RecoverOrphan(m *core.Message, txID string) bool {
	f.got = append(f.got, txID)
	return f.send
}

// stageOverdue 暂存一条半消息并把它的下次回查时间改到过去（造到期条目——
// 正常 Stage 首查在 30s 后，测试等不起，手法同 delay_test 直写暂存区）。
func stageOverdue(t *testing.T, f *fixture, topic string) string {
	t.Helper()
	m, txID, err := f.mgr.Stage(msg(topic))
	if err != nil {
		t.Fatal(err)
	}
	_ = m
	rewriteNextCheck(t, f, txID, time.Now().Add(-time.Second).UnixMilli())
	return txID
}

// rewriteNextCheck 把 txID 的 half 条目搬到指定回查时间（保持两键一致）。
func rewriteNextCheck(t *testing.T, f *fixture, txID string, ms int64) {
	t.Helper()
	refRaw, ok, err := f.st.Get(store.HalfIdxKey(txID))
	if err != nil || !ok {
		t.Fatalf("halfidx 缺失: %v", err)
	}
	ref := &HalfRef{}
	mustUnmarshal(t, refRaw, ref)
	raw, ok, err := f.st.Get(store.HalfKey(ref.NextCheckMs, txID))
	if err != nil || !ok {
		t.Fatalf("half 条目缺失: %v", err)
	}
	old := store.HalfKey(ref.NextCheckMs, txID)
	ref.NextCheckMs = ms
	b := f.st.NewBatch()
	b.Delete(old, nil)
	b.Set(store.HalfKey(ms, txID), raw, nil)
	b.Set(store.HalfIdxKey(txID), mustMarshal(t, ref), nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

func TestPassSendsRecoverAndReschedules(t *testing.T) {
	f := newFixture(t, 30*time.Second, 15)
	txID := stageOverdue(t, f, "t-check")
	n := &fakeNotifier{send: true}
	sent, err := f.mgr.Pass(n)
	if err != nil || sent != 1 {
		t.Fatalf("Pass: sent=%d err=%v", sent, err)
	}
	if len(n.got) != 1 || n.got[0] != txID {
		t.Fatalf("回查未下发: %v", n.got)
	}
	// 改期后 checks+1、NextCheckMs 在未来，且 half 键已搬到新位置
	refRaw, _, _ := f.st.Get(store.HalfIdxKey(txID))
	ref := &HalfRef{}
	mustUnmarshal(t, refRaw, ref)
	if ref.Checks != 1 || ref.NextCheckMs <= time.Now().UnixMilli() {
		t.Fatalf("改期状态错误: %+v", ref)
	}
	if _, ok, _ := f.st.Get(store.HalfKey(ref.NextCheckMs, txID)); !ok {
		t.Fatal("half 条目未搬到新回查时间")
	}
	if f.mgr.ChecksTotal() != 1 {
		t.Fatalf("ChecksTotal = %d", f.mgr.ChecksTotal())
	}
}

func TestPassReschedulesEvenWhenNoProducerOnline(t *testing.T) {
	// 无在线 producer（RecoverOrphan=false）也必须改期计数：否则 producer
	// 永不回来时半消息永远停在同一到期位、每秒被重扫，maxChecks 形同虚设
	f := newFixture(t, 30*time.Second, 15)
	txID := stageOverdue(t, f, "t-check")
	n := &fakeNotifier{send: false}
	if _, err := f.mgr.Pass(n); err != nil {
		t.Fatal(err)
	}
	refRaw, _, _ := f.st.Get(store.HalfIdxKey(txID))
	ref := &HalfRef{}
	mustUnmarshal(t, refRaw, ref)
	if ref.Checks != 1 {
		t.Fatalf("未下发也要计轮次: %+v", ref)
	}
}

func TestPassDropsAfterMaxChecks(t *testing.T) {
	f := newFixture(t, 30*time.Second, 2) // 上限 2 次
	txID := stageOverdue(t, f, "t-drop")
	n := &fakeNotifier{send: true}
	for i := 0; i < 3; i++ {
		if _, err := f.mgr.Pass(n); err != nil {
			t.Fatal(err)
		}
		rewriteNextCheck(t, f, txID, time.Now().Add(-time.Second).UnixMilli())
		if f.halfCount(t) == 0 {
			break
		}
	}
	if f.halfCount(t) != 0 {
		t.Fatal("超限半消息未被丢弃")
	}
	if _, ok, _ := f.st.Get(store.HalfIdxKey(txID)); ok {
		t.Fatal("超限丢弃后 halfidx 残留")
	}
	if f.mgr.DroppedTotal() != 1 {
		t.Fatalf("DroppedTotal = %d", f.mgr.DroppedTotal())
	}
}

func TestPassSkipsEntryEndedInBetween(t *testing.T) {
	// 扫描收集与逐条处理之间客户端完成了 End——处理阶段必须重验 halfidx，
	// 不能凭已收集的旧键复活已决断的事务
	f := newFixture(t, 30*time.Second, 15)
	txID := stageOverdue(t, f, "t-race")
	if found, _ := f.mgr.End(txID, true); !found {
		t.Fatal("End 失败")
	}
	n := &fakeNotifier{send: true}
	sent, err := f.mgr.Pass(n)
	if err != nil || sent != 0 || len(n.got) != 0 {
		t.Fatalf("已决断事务被回查: sent=%d got=%v err=%v", sent, n.got, err)
	}
}
```

（`mustMarshal/mustUnmarshal` 为 5 行 test helper，json 包装 + t.Fatal。）

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/txn/ -run 'TestPass' -v`
Expected: FAIL（undefined: Pass/Notifier）

- [ ] **Step 3: 实现调度器**

在 `txn.go` 追加：

```go
// scanInterval 回查扫描间隔。1s 对「几十秒级的回查间隔」精度绰绰有余。
// var 而非 const：测试需注入小值。
var scanInterval = time.Second

// maxCheckPerPass 单趟最多处理条数（预算上界，理由同 delay.maxMovePerPass）。
var maxCheckPerPass = 256

// Notifier 回查命令下发通道。由协议适配层实现（经 Telemetry 流发
// RecoverOrphanedTransactionCommand）；core 经本接口反向解耦，不感知 proto。
// 返回 true 表示命令已写入某个 producer 流（不保证客户端处理成功——
// 客户端的决断最终仍以 EndTransaction 到达）。
type Notifier interface {
	RecoverOrphan(m *core.Message, txID string) bool
}

// RunChecker 阻塞运行回查调度循环，结构与 delay.Scheduler.Run 同构：
// 启动即跑一趟，此后每 scanInterval 一趟，单趟满额立即续趟。ctx 取消即返回。
func (t *Manager) RunChecker(ctx context.Context, n Notifier) {
	t.logger.Info("txn 回查调度器启动",
		"scan_interval", scanInterval.String(),
		"check_interval", t.checkInterval.String(), "max_checks", t.maxChecks)
	tk := time.NewTicker(scanInterval)
	defer tk.Stop()
	for {
		handled, err := t.Pass(n)
		if err != nil {
			// 单趟失败只记日志不退出：store 瞬时故障恢复后下一趟自然重试
			t.logger.Error("txn 回查趟失败", "err", err)
		} else if handled > 0 {
			t.logger.Info("txn 回查趟完成", "handled", handled)
		}
		if err == nil && handled == maxCheckPerPass {
			continue // 满额=可能还有到期积压，立即续趟
		}
		select {
		case <-ctx.Done():
			t.logger.Info("txn 回查调度器退出")
			return
		case <-tk.C:
		}
	}
}

// Pass 执行一趟到期回查，返回处理条数（下发+改期、或超限丢弃，均计入）。
func (t *Manager) Pass(n Notifier) (int, error) {
	now := time.Now().UnixMilli()
	// 先收集后处理：Scan 回调里不能开写事务（迭代器与写入交错），拷贝出来
	type dueEntry struct {
		txID string
		raw  []byte
	}
	var dues []dueEntry
	err := t.st.Scan([]byte(store.HalfPrefix), store.HalfScanUpperBound(now), maxCheckPerPass,
		func(k, v []byte) (bool, error) {
			_, txID, perr := store.ParseHalfKey(k)
			if perr != nil {
				return false, perr
			}
			dues = append(dues, dueEntry{txID: txID, raw: append([]byte(nil), v...)})
			return true, nil
		})
	if err != nil {
		return 0, fmt.Errorf("扫描 half 暂存区: %w", err)
	}
	handled := 0
	for _, d := range dues {
		m, err := core.DecodeMessage(d.raw)
		if err != nil {
			// 坏条目永远无法决断，删除止损并 Error 留痕（同 delay 清坏条目）。
			// 注意坏条目只能按 idx 定位删除——half key 需要 NextCheckMs，
			// 而它在 idx 里，所以两键都从 idx 侧重建
			t.logger.Error("half 条目解码失败，丢弃坏条目", "tx_id", d.txID, "err", err)
			if err := t.dropLocked(d.txID); err != nil {
				return handled, err
			}
			continue
		}
		send, err := t.checkOne(d.txID, m)
		if err != nil {
			// 失败即中断本趟：条目未动，下一趟重扫自然重试
			return handled, err
		}
		handled++
		if send {
			// 下发放在改期之后、锁之外（见 checkOne 注释），这里只负责调用
			if !n.RecoverOrphan(m, d.txID) {
				t.logger.Warn("回查命令无处下发：没有发布该 topic 的在线 producer",
					"tx_id", d.txID, "topic", m.Topic, "msg_id", m.ID)
			}
		}
	}
	return handled, nil
}

// checkOne 处理一条到期半消息：重验存在性→超限丢弃或改期。返回 send=true
// 表示调用方应继续下发回查命令。
//
// 为什么改期在下发之前、且全程持 mu：客户端的 EndTransaction 随时可能并发
// 到达。改期（搬 half 键）与 End（删 half 键）都以 halfidx 为准且都持 mu，
// 因此任一方总能看到另一方的完整结果；若先下发后改期，客户端可能在两者的
// 间隙完成 End，改期一方再按旧键重写就把已决断的事务复活成僵尸。
// 下发本身是网络操作，放在锁外，避免一个慢客户端拖住全部事务状态迁移。
func (t *Manager) checkOne(txID string, m *core.Message) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	refRaw, ok, err := t.st.Get(store.HalfIdxKey(txID))
	if err != nil {
		return false, fmt.Errorf("读取 halfidx (tx=%s): %w", txID, err)
	}
	if !ok {
		// 扫描后、加锁前已被 End 决断——正常赛跑结果，静默跳过即可
		return false, nil
	}
	ref := &HalfRef{}
	if err := json.Unmarshal(refRaw, ref); err != nil {
		return false, fmt.Errorf("解码 halfidx (tx=%s): %w", txID, err)
	}
	oldKey := store.HalfKey(ref.NextCheckMs, txID)
	if ref.Checks >= t.maxChecks {
		// spec §5：回查上限默认 15 次，超限丢弃并记日志。Error 级：这代表
		// 一条业务消息被放弃，运维必须能从日志里找到它
		b := t.st.NewBatch()
		b.Delete(oldKey, nil)
		b.Delete(store.HalfIdxKey(txID), nil)
		if err := t.st.Apply(b); err != nil {
			return false, fmt.Errorf("丢弃超限半消息 (tx=%s): %w", txID, err)
		}
		t.dropped.Add(1)
		t.logger.Error("半消息回查超限，丢弃", "tx_id", txID, "msg_id", m.ID,
			"topic", m.Topic, "checks", ref.Checks, "max_checks", t.maxChecks)
		return false, nil
	}
	// 改期：half 键搬到新回查时间、checks+1，同批原子。无论下发是否成功都
	// 计轮次——producer 永不回来时，半消息也要在 maxChecks 轮后被丢弃，
	// 而不是永远滞留（每轮间隔 checkInterval，丢弃前给了它完整的重连窗口）
	next := time.Now().Add(t.checkInterval).UnixMilli()
	raw, ok, err := t.st.Get(oldKey)
	if err != nil || !ok {
		return false, fmt.Errorf("重读 half 条目失败 (tx=%s ok=%v): %w", txID, ok, err)
	}
	newRef, _ := json.Marshal(&HalfRef{NextCheckMs: next, Checks: ref.Checks + 1})
	b := t.st.NewBatch()
	b.Delete(oldKey, nil)
	b.Set(store.HalfKey(next, txID), raw, nil)
	b.Set(store.HalfIdxKey(txID), newRef, nil)
	if err := t.st.Apply(b); err != nil {
		return false, fmt.Errorf("半消息改期 (tx=%s): %w", txID, err)
	}
	t.checks.Add(1)
	t.logger.Debug("半消息回查已排期", "tx_id", txID, "msg_id", m.ID,
		"topic", m.Topic, "check_round", ref.Checks+1, "next_check_ms", next)
	return true, nil
}

// dropLocked 按 txID 删除 half 两键（坏条目清理用）。自行加锁。
func (t *Manager) dropLocked(txID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	refRaw, ok, err := t.st.Get(store.HalfIdxKey(txID))
	if err != nil {
		return fmt.Errorf("读取 halfidx (tx=%s): %w", txID, err)
	}
	b := t.st.NewBatch()
	if ok {
		ref := &HalfRef{}
		if err := json.Unmarshal(refRaw, ref); err == nil {
			b.Delete(store.HalfKey(ref.NextCheckMs, txID), nil)
		}
	}
	b.Delete(store.HalfIdxKey(txID), nil)
	return t.st.Apply(b)
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/txn/ -count=1 -race -v`
Expected: PASS（含 -race：End 与 Pass 的锁纪律必须过竞态检测）

- [ ] **Step 5: 日志自检**

启动/退出 Info ✓；每趟 handled>0 Info ✓；超限丢弃 Error（tx_id/msg_id/topic/checks）✓；无处下发 Warn ✓；改期 Debug（避免高频刷屏）✓；错误分支带 txID 上下文 ✓。

- [ ] **Step 6: 注释自检**

「为什么改期在下发之前」「为什么未下发也计轮次」「为什么下发在锁外」三处竞态/语义推理均已成注释 ✓。

- [ ] **Step 7: 提交**

```bash
git add internal/core/txn/
git commit -m "feat(txn): 回查调度器——到期扫描、Notifier 下发、改期与超限丢弃"
```

---

### Task 5: rpc Telemetry 会话注册表

**Files:**
- Create: `internal/rpc/sessions.go`
- Modify: `internal/rpc/server.go`（Telemetry handler 注册会话；Settings 回包改走会话发送）
- Test: `internal/rpc/sessions_test.go`

**Interfaces:**
- Produces:
  - `newSessions() *sessions`（包内）；`(*sessions).add/remove/count`、`(*sessions).pickProducer(topic string) *session`
  - `(*session).send(cmd *pb.TelemetryCommand) error`（持内部 sendMu，串行化同流并发写）
  - `(*Server).ConnectionCount() int`（导出，供 metrics/admin 消费；M5 递延的连接数在此兑现）
- 语义：会话 = 完成 Settings 协商的 Telemetry 流；连接数按此口径统计。

- [ ] **Step 1: 写失败测试**

`sessions_test.go`（纯注册表逻辑，不起 gRPC）：

```go
package rpc

import (
	"testing"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

func producerSession(topics ...string) *session {
	ts := map[string]bool{}
	for _, t := range topics {
		ts[t] = true
	}
	return &session{clientType: pb.ClientType_PRODUCER, topics: ts}
}

func TestSessionsCountAndRemove(t *testing.T) {
	ss := newSessions()
	a, b := producerSession("t1"), &session{clientType: pb.ClientType_SIMPLE_CONSUMER}
	ss.add(a)
	ss.add(b)
	if ss.count() != 2 {
		t.Fatalf("count = %d", ss.count())
	}
	ss.remove(a)
	if ss.count() != 1 {
		t.Fatalf("remove 后 count = %d", ss.count())
	}
}

func TestPickProducerMatchesTopicStrictly(t *testing.T) {
	ss := newSessions()
	ss.add(producerSession("t1"))
	ss.add(&session{clientType: pb.ClientType_SIMPLE_CONSUMER, topics: map[string]bool{"t2": true}})
	if got := ss.pickProducer("t1"); got == nil {
		t.Fatal("发布 t1 的 producer 应被选中")
	}
	// 只有 consumer 声明过 t2：不能退而求其次拿 consumer 或不相关 producer——
	// 回查命令发给没发布过该 topic 的客户端，其 checker 会对陌生事务做出决断
	if got := ss.pickProducer("t2"); got != nil {
		t.Fatal("无匹配 producer 时必须返回 nil")
	}
}

func TestPickProducerRotates(t *testing.T) {
	ss := newSessions()
	a, b := producerSession("t1"), producerSession("t1")
	ss.add(a)
	ss.add(b)
	first := ss.pickProducer("t1")
	second := ss.pickProducer("t1")
	if first == second {
		t.Fatal("多 producer 时应轮转，不能永远打同一个")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestSessions|TestPickProducer' -v`
Expected: FAIL

- [ ] **Step 3: 实现注册表**

`internal/rpc/sessions.go`：

```go
// Telemetry 会话注册表：记录完成 Settings 协商的双向流，为事务回查提供
// 下发通道，为 metrics/控制台提供连接数（M5 递延项）。
//
// 职责：
//   - 会话生命周期：Settings 协商成功即注册，流结束即注销
//   - pickProducer：按 topic 严格匹配发布方会话（轮转均衡）
//   - session.send：同一条流上的并发写序列化（grpc-go 禁止并发 SendMsg）
//
// 边界：
//   - 不理解回查语义（txn 的事），只是「找到一条能写的流并写进去」
//   - 不做会话保活/超时（流断开由 gRPC 通知，Telemetry handler 退出时注销）
package rpc

import (
	"sync"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// session 一条已完成 Settings 协商的 Telemetry 流。
type session struct {
	stream pb.MessagingService_TelemetryServer
	// sendMu 串行化本流上的所有服务端写：Settings 回包（handler goroutine）
	// 与回查命令（checker goroutine）可能并发，grpc-go 明确禁止同一流并发
	// SendMsg，漏了这把锁就是数据竞争加流损坏
	sendMu     sync.Mutex
	clientType pb.ClientType
	topics     map[string]bool // producer 声明发布的 topics（来自 Settings.Publishing.Topics）
}

// send 向该会话写一条命令（并发安全）。
func (se *session) send(cmd *pb.TelemetryCommand) error {
	se.sendMu.Lock()
	defer se.sendMu.Unlock()
	return se.stream.Send(cmd)
}

// sessions 会话注册表。并发安全。
type sessions struct {
	mu   sync.Mutex
	all  map[*session]struct{}
	next int // pickProducer 轮转游标
}

func newSessions() *sessions { return &sessions{all: map[*session]struct{}{}} }

func (ss *sessions) add(se *session) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.all[se] = struct{}{}
}

func (ss *sessions) remove(se *session) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.all, se)
}

func (ss *sessions) count() int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return len(ss.all)
}

// pickProducer 挑一条发布过 topic 的 producer 会话（多条时轮转）。
//
// 严格按 topic 匹配、绝不降级到任意 producer：回查是把消息交给客户端的
// TransactionChecker 做决断，发给没发布过该 topic 的进程，它的 checker 面对
// 陌生事务多半回 ROLLBACK/UNKNOWN——等于让无关方替业务做了错误决定。
// 找不到匹配会话时返回 nil，由调度器改期后下轮再试（producer 重连即恢复）。
func (ss *sessions) pickProducer(topic string) *session {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	var matches []*session
	for se := range ss.all {
		if se.clientType == pb.ClientType_PRODUCER && se.topics[topic] {
			matches = append(matches, se)
		}
	}
	if len(matches) == 0 {
		return nil
	}
	se := matches[ss.next%len(matches)]
	ss.next++
	return se
}
```

`server.go` 修改：

1. `Server` 结构体加字段 `sessions *sessions`（`New` 里初始化 `sessions: newSessions()`；注意 Task 6 会同步给 `New` 加 txn 参数，两处签名改动合并在 Task 6 落地，本 task 先只加 sessions 字段并保持 `New` 现有形参）。
2. `Telemetry` handler：进入循环前 `var sess *session`，`defer func() { if sess != nil { s.sessions.remove(sess); s.logger.Debug("telemetry 会话注销", "connections", s.sessions.count()) } }()`；`Settings` 分支改为：

```go
			case *pb.TelemetryCommand_Settings:
				settings := s.negotiateSettings(c.Settings)
				// 先注册会话再回包：回包经 sess.send 走同一把 sendMu，
				// 从此本流上服务端的每一次写都被串行化（回查命令并发写安全）
				if sess == nil {
					sess = &session{stream: stream, clientType: c.Settings.GetClientType(), topics: map[string]bool{}}
					s.sessions.add(sess)
					s.logger.Debug("telemetry 会话注册",
						"client_type", sess.clientType, "connections", s.sessions.count())
				}
				// SDK 周期性重发 Settings（topic 列表会增长），每次都全量刷新
				if pubs := c.Settings.GetPublishing(); pubs != nil {
					fresh := map[string]bool{}
					for _, tp := range pubs.GetTopics() {
						fresh[tp.GetName()] = true
					}
					sess.topics = fresh
				}
				if err := sess.send(&pb.TelemetryCommand{
					Status:  okStatus(),
					Command: &pb.TelemetryCommand_Settings{Settings: settings},
				}); err != nil {
					return fmt.Errorf("telemetry 下发 settings: %w", err)
				}
```

（原 `stream.Send` 与其后的 Debug 日志保留语义，只是搬进 `sess.send`。）

3. 追加导出方法：

```go
// ConnectionCount 返回当前已完成 Settings 协商的客户端连接数
// （/metrics 的 sq_connections 与控制台总览共用此口径，spec §9 递延自 M5）。
func (s *Server) ConnectionCount() int { return s.sessions.count() }
```

> 竞态说明（写进 Telemetry 的注释）：`sess.topics` 的整体替换发生在 handler goroutine，`pickProducer` 读它发生在 checker goroutine——map 需在 sessions.mu 内读、写时替换整个 map 引用仍不够（Go map 非原子）。**实现时把 topics 的刷新也搬进 sessions 的锁**：给 sessions 加方法 `updateTopics(se *session, fresh map[string]bool)`，pickProducer 与 updateTopics 同锁互斥。测试 `-race` 必须覆盖（Step 4）。

- [ ] **Step 4: 跑测试确认通过（含竞态）**

Run: `go test ./internal/rpc/ -count=1 -race`
Expected: PASS（既有 telemetry/settings 测试无回归）

- [ ] **Step 5: 日志自检**

会话注册/注销 Debug（带 connections 计数）✓；send 失败由调用方（Task 7 的 RecoverOrphan）落 Warn ✓。

- [ ] **Step 6: 注释自检**

文件头职责/边界 ✓；sendMu 存在理由（grpc-go 并发约束）✓；pickProducer 不降级的语义推理 ✓；topics 刷新的锁纪律 ✓。

- [ ] **Step 7: 提交**

```bash
git add internal/rpc/sessions.go internal/rpc/server.go internal/rpc/sessions_test.go
git commit -m "feat(rpc): Telemetry 会话注册表——回查下发通道与连接数"
```

---

### Task 6: rpc 发送路径打开 TRANSACTION

**Files:**
- Modify: `internal/rpc/send.go`（toCoreMessage TRANSACTION 分支 + SendMessage 路由）
- Modify: `internal/rpc/server.go`（`New` 加 txn 参数；`messageQueues` 的 AcceptMessageTypes 加 TRANSACTION）
- Modify: `internal/rpc/settings.go`（注释更新：客户端本地不再拒事务）
- Modify: `internal/rpc/server_test.go`（测试 helper 的 `New(...)` 调用加 txn.Manager）
- Test: `internal/rpc/send_test.go`（追加用例）

**Interfaces:**
- Consumes: `txn.(*Manager).Stage`（Task 3）
- Produces: `rpc.New(cfg, mt, pr, dl, tx *txn.Manager, writeBlocked, logger)`（**签名变更**，main 与测试 helper 同步改）；`SendResultEntry.TransactionId` 回填

- [ ] **Step 1: 写失败测试**

`send_test.go` 追加（沿用文件内既有 stub 客户端手法）：

```go
func TestSendTransactionStagesHalfMessage(t *testing.T) {
	h := newTestHarness(t, true) // 以文件内既有 helper 名为准
	resp, err := h.client.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "t-txn"},
			SystemProperties: &pb.SystemProperties{
				MessageId:   "M1",
				MessageType: pb.MessageType_TRANSACTION,
			},
			Body: []byte("half"),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status = %v", resp.GetStatus())
	}
	entry := resp.GetEntries()[0]
	if entry.GetTransactionId() == "" {
		t.Fatal("SendResultEntry 未回填 transaction_id——SDK 的 Commit/Rollback 全靠它")
	}
	// 未提交前不可消费（半消息不可见）
	// ……用文件内既有的 receive 断言手法确认 msg/ 为空或收不到消息
}

func TestSendTransactionRejectsConflicts(t *testing.T) {
	// TRANSACTION + delivery_timestamp / + message_group 均为属性冲突
	// （RocketMQ 语义：事务不可与延时/顺序组合），断言
	// MESSAGE_PROPERTY_CONFLICT_WITH_TYPE，手法对齐既有 NORMAL 冲突用例
}

func TestRouteAdvertisesTransaction(t *testing.T) {
	// QueryRoute 的 AcceptMessageTypes 必须含 TRANSACTION——SDK 开着
	// ValidateMessageType，缺了它事务消息在客户端本地就被拒（与 DELAY/FIFO 同教训）
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestSendTransaction|TestRouteAdvertises' -v`
Expected: FAIL

- [ ] **Step 3: 实现**

`server.go`：Server 加字段 `tx *txn.Manager`；`New` 签名改为 `New(cfg *config.Config, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer, tx *txn.Manager, writeBlocked *atomic.Bool, logger *slog.Logger)`（doc 注释补一句 tx 的职责）；`messageQueues` 的 AcceptMessageTypes 追加：

```go
				// M6 起接受事务消息，理由同上（SDK ValidateMessageType 在客户端
				// 本地校验，缺了 TRANSACTION 事务消息根本发不出来）
				pb.MessageType_TRANSACTION,
```

`send.go` 文件头边界行改为 `// 边界：NORMAL、DELAY（M3）、FIFO（M4）与 TRANSACTION（M6）。`；`toCoreMessage` 的 TRANSACTION 分支替换为：

```go
	case pb.MessageType_TRANSACTION:
		// 事务不可与延时/顺序组合（RocketMQ 语义）：半消息的可见时机由
		// EndTransaction 决定，delivery_timestamp 无处安放；提交时经正常
		// 写入路径重新入队，无法承诺组内相对顺序
		if sp.GetDeliveryTimestamp() != nil {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"TRANSACTION 消息不应携带 delivery_timestamp")
		}
		if sp.GetMessageGroup() != "" {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"TRANSACTION 消息不应携带 message_group")
		}
		transactional = true
```

（函数开头声明 `var transactional bool`，构造 `core.Message` 时带上 `Transactional: transactional`。）

`SendMessage` 第二遍路由改为三分支：

```go
		var stored *core.Message
		var txID string
		var err error
		switch {
		case m.Transactional:
			// 半消息进暂存区：无队列无 offset（entry.Offset 回 0），
			// TransactionId 必须回填——SDK 的 transactionImpl 靠它发起
			// Commit/RollBack，漏了它整个事务 API 在客户端侧无法收尾
			stored, txID, err = s.tx.Stage(m)
		case m.DeliverAtMs > 0:
			stored, err = s.pr.AppendDelay(m)
		default:
			stored, err = s.pr.Append(m)
		}
```

entry 构造处补 `TransactionId: txID`（非事务消息为空串，proto3 省略）。

`settings.go` 第 91-94 行注释更新：`AcceptMessageTypes=[NORMAL, DELAY, FIFO, TRANSACTION]（M6 起全类型开放），ValidateMessageType 仍开启以拦截未知类型`。

`server_test.go` 的 helper：构造 `tx := txn.New(st, pr, mt, 30*time.Second, 15, slog.Default())` 并传入 `New(...)`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/rpc/ -count=1 && go build ./...`
Expected: rpc 包 PASS；`go build ./...` 此刻会因 main.go 的 `rpc.New` 旧签名失败——**允许**，Task 8 修 main；先 `go vet ./internal/...` 确认库代码干净。

- [ ] **Step 5: 日志自检**

Stage 失败沿用 `topicErrStatus`（带 msg_id）✓；Stage 成功的 Info 在 txn 层已打，rpc 层不重复 ✓。

- [ ] **Step 6: 提交**

```bash
git add internal/rpc/
git commit -m "feat(rpc): SendMessage 打开 TRANSACTION——半消息分流与 transaction_id 回执"
```

---

### Task 7: rpc EndTransaction 与回查下发

**Files:**
- Create: `internal/rpc/txn.go`
- Test: `internal/rpc/txn_test.go`

**Interfaces:**
- Consumes: `txn.(*Manager).End`（Task 3）、`sessions.pickProducer/session.send`（Task 5）、包内 `bodyEncodingToPB/digestToPB/crc32Checksum`（receive.go 既有 helper）
- Produces:
  - `(*Server).EndTransaction(ctx, *pb.EndTransactionRequest) (*pb.EndTransactionResponse, error)`（覆盖 Unimplemented 桩）
  - `(*Server).RecoverOrphan(m *core.Message, txID string) bool` —— **实现 `txn.Notifier` 接口**

- [ ] **Step 1: 写失败测试**

`txn_test.go`：

```go
func TestEndTransactionCommitAndRollback(t *testing.T) {
	// stub 客户端：Send(TRANSACTION) 拿 txID → EndTransaction(COMMIT) → 消息可收
	// 第二条：EndTransaction(ROLLBACK) → 消息永不可收
	// 断言两次响应 Status 均 OK
}

func TestEndTransactionUnknownTxIDIsOK(t *testing.T) {
	// 未知 txID 回 OK（幂等）：SDK 网络重试/回查赛跑都会走到，回错误码
	// 会让一次已成功的提交在客户端侧表现为失败
}

func TestEndTransactionRejectsUnspecifiedResolution(t *testing.T) {
	// resolution 未指定 → Code_BAD_REQUEST（决断不能靠猜）
}

func TestRecoverOrphanSendsCommandToProducerStream(t *testing.T) {
	// 用真实 bufconn Telemetry 流：客户端上报 producer Settings（topics 含
	// "t-orphan"）完成注册；服务端调 srv.RecoverOrphan(msg, "TX9")；
	// 客户端流上应收到 RecoverOrphanedTransactionCommand，断言
	// TransactionId=="TX9"、Message.SystemProperties.MessageType==TRANSACTION、
	// MessageId/Body 与原消息一致
}

func TestRecoverOrphanNoProducerReturnsFalse(t *testing.T) {
	// 无注册会话时 RecoverOrphan 返回 false（调度器据此打 Warn）
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestEndTransaction|TestRecoverOrphan' -v`
Expected: FAIL（EndTransaction 走 Unimplemented 桩）

- [ ] **Step 3: 实现**

`internal/rpc/txn.go`：

```go
// EndTransaction 与事务回查下发：proto 语义 ↔ txn.Manager 的翻译层。
//
// 职责：
//   - EndTransaction RPC：resolution 校验、txn.End 调用、幂等语义翻译
//   - RecoverOrphan：实现 txn.Notifier——把 core.Message 编码为
//     RecoverOrphanedTransactionCommand 并写入一条 producer Telemetry 流
//
// 边界：
//   - 不做事务状态判断（txn.Manager 的事）；不管会话怎么挑（sessions 的事）
package rpc

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xushixin/sq/internal/core"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// EndTransaction 事务提交/回滚（spec §6）。
//
// 幂等：txID 不存在回 OK——三种正常来源（客户端网络重试、与回查决断赛跑、
// 超限已丢弃）都不该让客户端把一次实际已生效的决断当成失败去重试。
// 不拦磁盘水位：提交是已落盘数据的移位（删 half 写 msg，净增量≈0），
// 拦了只会让半消息滞留、回查空转（与 delay 到期搬运同一取舍）。
func (s *Server) EndTransaction(ctx context.Context, req *pb.EndTransactionRequest) (*pb.EndTransactionResponse, error) {
	txID := req.GetTransactionId()
	var commit bool
	switch req.GetResolution() {
	case pb.TransactionResolution_COMMIT:
		commit = true
	case pb.TransactionResolution_ROLLBACK:
		commit = false
	default:
		// 决断不能靠猜：UNSPECIFIED 提交会放出未确认的业务消息，回滚会
		// 丢掉已确认的——两个方向都错，只能拒绝
		s.logger.Warn("EndTransaction 决断未指定", "tx_id", txID, "msg_id", req.GetMessageId())
		return &pb.EndTransactionResponse{Status: errStatus(pb.Code_BAD_REQUEST,
			"resolution 必须为 COMMIT 或 ROLLBACK")}, nil
	}
	found, err := s.tx.End(txID, commit)
	if err != nil {
		s.logger.Error("EndTransaction 失败", "tx_id", txID,
			"msg_id", req.GetMessageId(), "topic", req.GetTopic().GetName(),
			"resolution", req.GetResolution(), "err", err)
		return &pb.EndTransactionResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
	}
	if !found {
		s.logger.Warn("EndTransaction 目标事务不存在（幂等按成功处理）",
			"tx_id", txID, "msg_id", req.GetMessageId(), "resolution", req.GetResolution(),
			"source", req.GetSource())
		return &pb.EndTransactionResponse{Status: okStatus()}, nil
	}
	s.logger.Info("EndTransaction 完成", "tx_id", txID, "msg_id", req.GetMessageId(),
		"topic", req.GetTopic().GetName(), "resolution", req.GetResolution(), "source", req.GetSource())
	return &pb.EndTransactionResponse{Status: okStatus()}, nil
}

// RecoverOrphan 实现 txn.Notifier：把到期未决半消息经 Telemetry 流下发给
// 一个发布该 topic 的 producer，由其 TransactionChecker 决断后回 EndTransaction。
// 返回 false 表示没有可用会话或写流失败（调度器改期后下轮再试）。
func (s *Server) RecoverOrphan(m *core.Message, txID string) bool {
	sess := s.sessions.pickProducer(m.Topic)
	if sess == nil {
		return false
	}
	cmd := &pb.TelemetryCommand{
		Status: okStatus(),
		Command: &pb.TelemetryCommand_RecoverOrphanedTransactionCommand{
			RecoverOrphanedTransactionCommand: &pb.RecoverOrphanedTransactionCommand{
				Message:       halfToProtoMessage(m),
				TransactionId: txID,
			},
		},
	}
	if err := sess.send(cmd); err != nil {
		// 流写失败通常意味着客户端已断开而注销尚未发生：Warn 留痕即可，
		// 会话随 handler 退出注销，调度器下轮会挑别的会话
		s.logger.Warn("回查命令写流失败", "tx_id", txID, "topic", m.Topic,
			"msg_id", m.ID, "err", err)
		return false
	}
	s.logger.Info("回查命令已下发", "tx_id", txID, "topic", m.Topic, "msg_id", m.ID)
	return true
}

// halfToProtoMessage 半消息 → 协议消息（回查命令载荷）。
//
// 与 receive.go 的投递构造刻意分开：那边带 ReceiptHandle/InvisibleDuration/
// DeliveryAttempt 等 POP 消费语义字段，回查载荷没有这些概念。类型固定回填
// TRANSACTION——SDK 的 MessageView 靠它识别这是半消息回查。
// digest 兜底补算 CRC32 的理由同 receive.go（SDK 对 UNSPECIFIED 刷 WARN）。
func halfToProtoMessage(m *core.Message) *pb.Message {
	enc, _ := bodyEncodingToPB(m.BodyEncoding)
	if m.BodyEncoding == core.BodyEncodingUnspecified {
		enc = pb.Encoding_IDENTITY
	}
	digest := digestToPB(m.BodyDigest)
	if digest == nil {
		digest = &pb.Digest{Type: pb.DigestType_CRC32, Checksum: crc32Checksum(m.Body)}
	}
	sp := &pb.SystemProperties{
		MessageId:      m.ID,
		MessageType:    pb.MessageType_TRANSACTION,
		Keys:           m.Keys,
		BodyEncoding:   enc,
		BodyDigest:     digest,
		BornTimestamp:  timestamppb.New(time.UnixMilli(m.BornAtMs)),
		BornHost:       m.BornHost,
		StoreTimestamp: timestamppb.New(time.UnixMilli(m.StoreAtMs)),
	}
	if m.Tag != "" {
		tag := m.Tag
		sp.Tag = &tag
	}
	if m.TraceContext != "" {
		tc := m.TraceContext
		sp.TraceContext = &tc
	}
	return &pb.Message{
		Topic:            &pb.Resource{Name: m.Topic},
		SystemProperties: sp,
		UserProperties:   m.Properties,
		Body:             m.Body,
	}
}
```

（`bodyEncodingToPB/digestToPB/crc32Checksum` 的真实名字以 receive.go/sysprops.go 现有实现为准，落码时核对。）

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/rpc/ -count=1 -race`
Expected: PASS

- [ ] **Step 5: 日志自检**

EndTransaction 三个出口（成功 Info / 幂等 Warn / 失败 Error）全部带 tx_id+msg_id+resolution ✓；回查下发成功 Info、写流失败 Warn ✓；无静默路径 ✓。

- [ ] **Step 6: 注释自检**

文件头 ✓；幂等语义、水位不拦、halfToProtoMessage 与投递构造分开的理由 ✓。

- [ ] **Step 7: 提交**

```bash
git add internal/rpc/txn.go internal/rpc/txn_test.go
git commit -m "feat(rpc): EndTransaction 与 Telemetry 回查命令下发"
```

---

### Task 8: main 装配

**Files:**
- Modify: `cmd/sq/main.go`

**Interfaces:**
- Consumes: `txn.New/RunChecker`、`rpc.New` 新签名、`(*rpc.Server).RecoverOrphan`

- [ ] **Step 1: 实现装配**

`run()` 中，`dl := deliver.New(...)` 之后、metrics 块之前插入：

```go
	// 事务管理器。构造顺序有讲究：rpc.Server 要拿它处理 Send/EndTransaction，
	// 回查调度器又要拿 rpc.Server 当 Notifier（下发回查命令）——先建 Manager、
	// 再建 Server、最后起调度 goroutine，依赖环在构造期就被拆开。
	tx := txn.New(st, pr, mt, cfg.TxnInterval(), cfg.TxnMaxChecks, logger)
```

把 `srv := rpc.New(cfg, mt, pr, dl, writeBlocked, logger)` **上移**到 metrics 块之前（改为 `rpc.New(cfg, mt, pr, dl, tx, writeBlocked, logger)`）——metrics（Task 9）与 admin 需要 `srv.ConnectionCount`。`rpc.New` 无副作用，上移不改变任何行为；原位置只留 `srv.Register(gs)`。

delay 调度器 defer 块之后追加：

```go
	// txn 回查调度器：到期未决半消息经 Telemetry 下发回查。停机顺序同
	// retention/delay——defer LIFO 保证先取消并等待调度 goroutine 退出，
	// 再轮到 st.Close。停机窗口内的回查下发会因流已收尾而写失败，
	// RecoverOrphan 按 Warn 处理、条目已改期，重启后自然继续。
	txnCtx, txnCancel := context.WithCancel(context.Background())
	var txnWG sync.WaitGroup
	txnWG.Add(1)
	go func() { defer txnWG.Done(); tx.RunChecker(txnCtx, srv) }()
	defer func() { txnCancel(); txnWG.Wait() }()
```

启动日志 `"sq 已启动"` 追加 `"txn_check_interval", cfg.TxnCheckInterval, "txn_max_checks", cfg.TxnMaxChecks`。

- [ ] **Step 2: 构建与全量单测**

Run: `go build ./... && go vet ./... && go test ./internal/... -count=1`
Expected: 全部 PASS

- [ ] **Step 3: 注释自检**

装配顺序（依赖环拆解）与停机顺序注释 ✓（上面代码已含）。

- [ ] **Step 4: 提交**

```bash
git add cmd/sq/main.go
git commit -m "feat(main): 装配事务管理器与回查调度器"
```

---

### Task 9: metrics——half 深度、回查计数、连接数

**Files:**
- Modify: `internal/metrics/stats.go`（Stats 加 `HalfDepth int`，Collect 扫 `half/` 前缀计数，手法同 DelayDepth）
- Modify: `internal/metrics/collector.go`（新指标）
- Modify: `cmd/sq/main.go`（`NewRegistry` 调用处传新参）
- Test: `internal/metrics/metrics_test.go`（追加用例）

**Interfaces:**
- Produces:
  - `metrics.TxnStats` 接口：`ChecksTotal() uint64; DroppedTotal() uint64`（`*txn.Manager` 天然实现）
  - `metrics.ConnCounter` 接口：`ConnectionCount() int`（`*rpc.Server` 天然实现）
  - `metrics.NewRegistry(st, mt, sys, tx TxnStats, conns ConnCounter, logger)`（**签名变更**；tx/conns 允许 nil——测试与降级场景跳过对应指标）
  - 新指标：`sq_half_messages`（gauge，半消息暂存深度）、`sq_txn_checks_total`（counter，累计回查排期次数）、`sq_txn_dropped_total`（counter，超限丢弃条数）、`sq_connections`（gauge，已协商客户端连接数）
- 明确不做：half 深度**不进**时序落库（`series.go` 的分钟点 schema 不动）——事务量级下瞬时 gauge 足够，改 schema 牵连存量数据兼容，留给真实需求出现时。

- [ ] **Step 1: 写失败测试**

```go
type fakeTxnStats struct{ checks, dropped uint64 }

func (f *fakeTxnStats) ChecksTotal() uint64  { return f.checks }
func (f *fakeTxnStats) DroppedTotal() uint64 { return f.dropped }

type fakeConns struct{ n int }

func (f *fakeConns) ConnectionCount() int { return f.n }

func TestRegistryExportsTxnAndConnMetrics(t *testing.T) {
	// fixture 手法对齐文件内既有 Registry 测试：造 store/meta/sysinfo，
	// 直写两条 half 条目（store.HalfKey/HalfIdxKey），然后
	// reg := NewRegistry(st, mt, sys, &fakeTxnStats{checks: 7, dropped: 1}, &fakeConns{n: 3}, slog.Default())
	// Gather 后断言：
	//   sq_half_messages == 2
	//   sq_txn_checks_total == 7
	//   sq_txn_dropped_total == 1
	//   sq_connections == 3
}

func TestRegistryTolerantToNilTxnAndConns(t *testing.T) {
	// NewRegistry(st, mt, sys, nil, nil, logger) 不 panic，
	// Gather 输出不含 sq_txn_* 与 sq_connections
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/metrics/ -run 'TestRegistry' -v`
Expected: FAIL

- [ ] **Step 3: 实现**

stats.go：`Stats` 加 `HalfDepth int`（注释：半消息暂存条数，扫 `half/` 前缀）；`Collect` 里按 DelayDepth 的同款扫描追加 half 计数。collector.go：新增 desc 与 Collect 分支（gauge 从 Stats 取；counter 用 `prometheus.CounterValue` + `tx.ChecksTotal()/DroppedTotal()`；连接数 gauge 用 `conns.ConnectionCount()`；`tx==nil`/`conns==nil` 跳过）。main.go：`metrics.NewRegistry(st, mt, sys, tx, srv, logger)`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/metrics/ -count=1 && go build ./...`
Expected: PASS

- [ ] **Step 5: 注释与日志自检**

新指标 desc 帮助文案说清口径（连接数=完成 Settings 协商的流）✓；collector 无新日志需求（纯读）✓。

- [ ] **Step 6: 提交**

```bash
git add internal/metrics/ cmd/sq/main.go
git commit -m "feat(metrics): 半消息深度、回查计数与连接数指标"
```

---

### Task 10: admin——事务视图与总览扩展

**Files:**
- Modify: `internal/admin/server.go`（路由 + `New` 签名加 `conns ConnCounter`）
- Modify: `internal/admin/messages.go`（`handleTransactionsList`、`handleOverview` 扩展）
- Modify: `cmd/sq/main.go`（admin.New 调用处传 srv）
- Test: `internal/admin/messages_test.go`（追加用例；admin 测试 helper 的 `New` 调用补 nil/fake）

**Interfaces:**
- Produces:
  - `admin.ConnCounter` 接口：`ConnectionCount() int`（nil 容忍：overview 回 0）
  - `GET /admin/transactions?limit=64` → `[{"tx_id","msg_id","topic","next_check_ms","checks","born_ms"}]`（按下次回查时间升序）
  - `GET /admin/overview` 响应追加 `"half_depth"`、`"connections"` 两键

- [ ] **Step 1: 写失败测试**

```go
func TestAdminTransactionsList(t *testing.T) {
	// 直写一条 half+halfidx（store.HalfKey/HalfIdxKey + txn.HalfRef JSON），
	// GET /admin/transactions?limit=10 断言返回 1 条且字段齐全，
	// 手法完全对齐 messages_test.go 里 /admin/delay 的既有用例
}

func TestAdminOverviewHasHalfDepthAndConnections(t *testing.T) {
	// overview 响应含 half_depth 与 connections 键；helper 传入的 fake
	// ConnCounter 返回 2，断言 connections == 2
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/admin/ -run 'TestAdminTransactions|TestAdminOverviewHas' -v
Expected: FAIL

- [ ] **Step 3: 实现**

server.go 路由块追加 `s.mux.HandleFunc("GET /admin/transactions", s.protected(s.handleTransactionsList))`；`New` 签名加 `conns ConnCounter`（doc 注释说明 nil 容忍）。messages.go 追加（结构对齐 `handleDelayList`，含坏条目 Warn 跳过）：

```go
// handleTransactionsList GET /admin/transactions：待决半消息（按下次回查时间升序）。
// halfidx 里的 Checks 靠二次 Get 取——列表是排查用的低频操作，N+1 读可接受。
func (s *Server) handleTransactionsList(w http.ResponseWriter, r *http.Request) {
	limit64, err := queryUint(r, "limit", 64)
	if err != nil {
		s.httpError(w, http.StatusBadRequest, "limit 非法: %v", err)
		return
	}
	type txnEntry struct {
		TxID        string `json:"tx_id"`
		MsgID       string `json:"msg_id"`
		Topic       string `json:"topic"`
		NextCheckMs int64  `json:"next_check_ms"`
		Checks      int    `json:"checks"`
		BornMs      int64  `json:"born_ms"`
	}
	out := []txnEntry{}
	hp := []byte(store.HalfPrefix)
	err = s.st.Scan(hp, store.PrefixUpperBound(hp), int(limit64), func(k, v []byte) (bool, error) {
		next, txID, perr := store.ParseHalfKey(k)
		if perr != nil {
			return false, perr
		}
		m, derr := core.DecodeMessage(v)
		if derr != nil {
			// 坏条目由回查调度器负责清理（那里删除并 Error 留痕），管理面只读跳过
			s.logger.Warn("admin 事务视图跳过坏条目", "key", string(k), "err", derr)
			return true, nil
		}
		checks := 0
		if refRaw, ok, _ := s.st.Get(store.HalfIdxKey(txID)); ok {
			ref := &txn.HalfRef{}
			if json.Unmarshal(refRaw, ref) == nil {
				checks = ref.Checks
			}
		}
		out = append(out, txnEntry{TxID: txID, MsgID: m.ID, Topic: m.Topic,
			NextCheckMs: next, Checks: checks, BornMs: m.BornAtMs})
		return true, nil
	})
	if err != nil {
		s.logger.Error("admin 事务视图扫描失败", "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.writeJSON(w, http.StatusOK, out)
}
```

`handleOverview` 的响应 map 追加 `"half_depth": st2.HalfDepth`（来自 Task 9 的 Stats）与：

```go
	conns := 0
	if s.conns != nil {
		conns = s.conns.ConnectionCount()
	}
```

main.go：`admin.New(st, mt, pr, dl, cfg.AdminUsername, cfg.AdminPassword, sys, sp, reg, srv, logger)`（参数位置按现有形参表插入，落码时对齐）。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/admin/ -count=1 && go build ./...`
Expected: PASS

- [ ] **Step 5: 日志与注释自检**

扫描失败 Error、坏条目 Warn ✓；N+1 读的取舍注释 ✓。

- [ ] **Step 6: 提交**

```bash
git add internal/admin/ cmd/sq/main.go
git commit -m "feat(admin): /admin/transactions 事务视图与总览连接数/半消息深度"
```

---

### Task 11: 控制台——事务页与总览连接数

**Files:**
- Modify: `web/src/api/`（types + client：`TxnEntry`、`transactions(limit)`；overview 类型加 `half_depth`/`connections`）
- Create: `web/src/pages/Transactions.tsx`（结构、样式、轮询 hook 用法**逐项镜像 `Delay.tsx`**）
- Modify: 路由注册与侧边导航（对齐 Delay 页的接入点：router 配置与 nav 项各一处）
- Modify: `web/src/pages/Overview.tsx`（统计卡加「连接数」，值取 overview.connections；「半消息」计数可并入既有堆积/深度卡区，样式对齐延时深度的展示）
- Test: `web/src/pages/Transactions.test.tsx`（vitest，手法对齐 `Messages.test.tsx`：mock api、断言表格渲染与空态文案）

**Interfaces:**
- Consumes: Task 10 的 `/admin/transactions` 与 overview 新键
- 列定义：事务ID（截断展示）/ 消息ID / Topic（链接到 topic 详情）/ 已回查次数 / 下次回查时间（相对时间，格式化函数复用 Delay 页的到期时间展示）/ 暂存时刻

- [ ] **Step 1: 写失败测试（vitest）**

`Transactions.test.tsx`：mock `transactions()` 返回一条 `{tx_id:"TX1", msg_id:"M1", topic:"t", next_check_ms: Date.now()+30000, checks: 2, born_ms: Date.now()-1000}`，断言渲染出 TX1/M1/回查次数 2；mock 空数组断言空态文案（「暂无待决事务」，风格对齐 Delay 页空态）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cd web && npx vitest run src/pages/Transactions.test.tsx`
Expected: FAIL

- [ ] **Step 3: 实现页面**

api types/client 追加；`Transactions.tsx` 从 `Delay.tsx` 复制骨架改列（usePoll 用法、刷新按钮、limit 交互全部保持一致；**注意 M5b 评审教训**：路由参数变化即刷新的 `.refresh()` 模式、Notice 用 JSX 不拼 HTML 字符串）；路由与导航各加一项（名称「事务」，位置放在「延时」之后）。Overview 加连接数统计卡。文件头注释（职责/边界）按项目前端惯例写。

- [ ] **Step 4: 跑测试与类型检查确认通过**

Run: `cd web && npx tsc --noEmit && npx vitest run`
Expected: 全部 PASS

- [ ] **Step 5: 构建嵌入验证**

Run: `make web && go build ./...`（以 Makefile 现有 target 名为准）
Expected: 构建通过，`web/dist` 更新

- [ ] **Step 6: 提交**

```bash
git add web/
git commit -m "feat(console): 事务待决视图与总览连接数"
```

---

### Task 12: e2e——官方 SDK 事务全链路

**Files:**
- Create: `test/e2e/sdk_txn_test.go`

**Interfaces:**
- Consumes: `startBroker(t, mutate ...func(*config.Config))`（既有 harness，支持按用例改配置）；官方 SDK `rmq.WithTransactionChecker`、`producer.BeginTransaction/SendWithTransaction`、`transaction.Commit/RollBack`

- [ ] **Step 1: 写测试（红→绿由 broker 功能是否完整决定，此处直接写全量用例）**

```go
//go:build e2e

// 官方 Go SDK 事务消息端到端：M6 出口标准（spec §11——半消息 + Telemetry
// 回查全链路）。三条链路：显式提交、显式回滚、孤儿回查（不调 Commit，
// 等 broker 经 Telemetry 下发回查、SDK checker 决断提交）。
//
// 边界：不测回查超限丢弃（需等 maxChecks×interval，纯服务端逻辑已有单测覆盖）。
package e2e

import (
	"context"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
)

// txnCheckIntervalE2E 回查间隔压到 2s：孤儿用例要在测试超时内等到第一次回查。
const txnCheckIntervalE2E = "2s"

func TestOfficialGoSDKTransactionCommitAndRollback(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) { c.TxnCheckInterval = txnCheckIntervalE2E })
	// producer：WithTopics(topic) + WithTransactionChecker（本用例 checker 不应
	// 被调用——收到回查即 t.Error，因为提交/回滚都在首个间隔内完成）
	// 1) BeginTransaction → SendWithTransaction → 在提交前先断言消费不到
	//    （SimpleConsumer Receive 短等待应得 MESSAGE_NOT_FOUND 语义的空结果）
	// 2) transaction.Commit() → 消费到该 msgId 并 ack
	// 3) 第二个事务 SendWithTransaction → transaction.RollBack() →
	//    再等一个消费窗口，断言永远收不到第二个 msgId
}

func TestOfficialGoSDKTransactionOrphanRecovery(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) { c.TxnCheckInterval = txnCheckIntervalE2E })
	// checker 收到回查即记录 msgId 并返回 rmq.COMMIT
	// SendWithTransaction 后【故意不 Commit/RollBack】
	// 断言：≤3×间隔内 checker 被调用（收到的 TransactionId/MessageId 与发送
	// 回执一致），随后消息可被 SimpleConsumer 消费到——证明
	// half→Telemetry 回查→SDK checker→EndTransaction(COMMIT)→msg/ 全链路闭环
}
```

（producer/consumer 的构造、topic/group 命名、超时与轮询断言的具体写法**逐项对齐 `sdk_delay_test.go` 的既有模式**；checker 回调注意用 channel 传递结果，不在回调里直接 t.Fatal——回调跑在 SDK 的 goroutine 里。）

- [ ] **Step 2: 跑 e2e**

Run: `go test -tags e2e ./test/e2e/ -run 'TestOfficialGoSDKTransaction' -v -timeout 300s`
Expected: PASS（失败时 harness 自动倾倒 broker 日志——Task 3/4/7 的 tx_id 日志链应能直接定位断点）

- [ ] **Step 3: 全量回归**

Run: `go test ./internal/... -count=1 && go test -tags e2e ./test/e2e/ -timeout 600s`
Expected: 全部 PASS（既有普通/延时/顺序/认证 e2e 无回归）

- [ ] **Step 4: 提交**

```bash
git add test/e2e/sdk_txn_test.go
git commit -m "test(e2e): 官方 SDK 事务提交/回滚/孤儿回查全链路"
```

---

### Task 13: 文档同步

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 逐处更新 README**

- 消息类型支持表/文案：事务消息从「未实现（M6）」改为已支持，写明半消息、EndTransaction、回查（间隔与上限的配置项名）。
- 配置项表追加 `txn_check_interval`（默认 30s）与 `txn_max_checks`（默认 15），一句话解释语义与超限丢弃行为。
- metrics 清单追加 `sq_half_messages`/`sq_txn_checks_total`/`sq_txn_dropped_total`/`sq_connections`（写明连接数口径）。
- Admin API 清单追加 `GET /admin/transactions`；overview 响应新键说明。
- 控制台页面表追加「事务」页；总览描述补连接数。
- 检查「限制/未实现」小节：移除事务，留下的条目核对仍然属实。

- [ ] **Step 2: 交叉核对**

Run: `grep -n "M6\|事务" README.md`
Expected: 无「事务将在 M6 支持」类残留表述。

- [ ] **Step 3: 提交**

```bash
git add README.md
git commit -m "docs: README 同步事务消息能力、配置、指标与控制台页面"
```

---

## 收尾（执行完全部 task 后）

1. 按 instrumenting-code 清单整支自检：关键节点日志、错误分支上下文、成功路径不静默、新文件头注释、导出方法注释。
2. 按全局 CLAUDE.md §5 最终审阅清单逐项过（分层、Facade、复用、无硬编码——注意 `maxCheckPerPass`、回查间隔等均已进常量/配置）。
3. `go build ./... && go vet ./... && go test ./internal/... -count=1 -race && go test -tags e2e ./test/e2e/ -timeout 600s && cd web && npx tsc --noEmit && npx vitest run`。
4. 走 superpowers:finishing-a-development-branch（合并前提示：本分支控制台改动——事务页与总览连接数——需回流 `prototypes/base/`）。

## Self-Review 记录

- Spec 覆盖：§4 `half/`+`halfidx/`（Task 1）、§5 流程 5 全句（Task 3/4/6/7：暂存不可见/commit 原子移入/rollback 删除/回查经 Telemetry/上限 15 丢弃记日志）、§6 EndTransaction+Telemetry 回查通道（Task 7）与协议验收=官方 SDK（Task 12）、§8 回查次数指标（Task 9）、§9 总览连接数（M5 递延，Task 5/9/10/11）、§10 事务状态机单测（Task 3/4）。
- 类型一致性：`Stage(m) (*core.Message, string, error)`、`End(txID, commit) (bool, error)`、`Notifier.RecoverOrphan(m, txID) bool`、`HalfRef{NextCheckMs, Checks}`、`ConnectionCount() int` 在 Task 3/4/5/7/9/10 间交叉引用一致。
- 已知留白（刻意，非遗漏）：控制台发送页不加事务类型（半消息发出后无人决断只会变成回查噪音）；half 深度不进时序落库；不支持事务+延时/顺序组合（协议层显式拒绝）；`deliver.Receive`/若干测试 helper 的形参以现库为准，任务内已标注「落码前核对」。
