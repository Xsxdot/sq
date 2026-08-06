# M4 顺序消息（FIFO）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 RocketMQ 5.x FIFO 消息语义：同 MessageGroup 严格按序投递、失败卡队头不跳过、超限入 DLQ 后推进，官方 Go SDK FIFO 用例可过（spec §5 流程 4、§11 M4 出口标准）。

**Architecture:** 发送端 MessageGroup hash 定队列已在 M1 实现（produce.AppendWith），M4 只做消费端：`InflightState` 增 `Ordered` 标记，`receiveOnce` 加队列级顺序锁——存在未终结的顺序 inflight 时不投后续顺序消息、位点停在被拦消息之前（不推进即天然崩溃安全）；顺序消息重投不设指数退避下限（spec §5 流程 6 退避仅限非顺序），超限走既有 moveToDLQ 解锁队列。协议层放开 FIFO 类型并新增 `ForwardMessageToDeadLetterQueue` RPC（显式入 DLQ，handle 自带定位坐标）。

**Tech Stack:** Go + Pebble v2（既有 store）；e2e 用官方 rocketmq-clients Go SDK v5.1.2（模块缓存已有）。

## Global Constraints

- 全局 CLAUDE.md §2：新文件必须有中文文件头（职责+边界）；导出方法必须有 doc 注释（参数/返回/注意）；复杂逻辑中文注释写"为什么"；日志用 `slog`（本仓库注入的 `*slog.Logger`），禁止 `fmt.Printf`。
- instrumenting-code：每个实现类 task 含「加关键节点日志」「加注释」step；错误分支日志带上下文；成功路径不静默；循环内降 Debug。
- store 批次契约：`NewBatch` 后 `Apply`/`Close` 恰好其一；`Scan` 回调的 k/v 仅回调期间有效，跨回调使用必须拷贝。
- 落盘 JSON 兼容约定（core/types.go）：新字段一律 `omitempty`，旧数据解码得零值、零值编码结果与升级前逐字节相同。
- deliver 并发约定（deliver.go 类型注释）：任何直接读写 inflight 的新方法必须持同一把队列锁（qmu），锁序只允许 deliver → produce 单向。
- 提交信息结尾：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。
- **M4 不新增任何 config 字段**（规避 M2 的 e2e yaml 零值序列化陷阱，同 M3）。

## 基线

- 分支 `m3-delay-messages`（HEAD `772a979`）或其合并后的 main，两者等价。
- M1-M3 已就绪且 M4 直接复用的设施：MessageGroup hash 定队列（produce.AppendWith 内 fnv 哈希）、惰性重投 + attempt 校验（receiveOnce/Ack）、moveToDLQ 原子转移、receipt handle 编解码（rpc/receipt.go：`receiptEncode`/`receiptDecode`，编码 group/topic/queue/offset/attempt 五元组）、e2e 每用例独立 broker（`startBroker(t)`）。

## 设计决策（执行前必读）

**1. 顺序语义按消息判定（`MessageGroup != ""` 即顺序消息），不引入 topic 类型属性。**
RocketMQ 官方以 topic 属性声明 FIFO，但 sq 在 M5 之前没有 Admin API，autoCreate 时也无从得知类型——引入 topic 属性就必须同时引入设置它的途径，超出 M4 范围。按消息判定与协议不冲突：SDK 只要 `SetMessageGroup` 就自动标 `MessageType_FIFO`（publishing_message.go:65-66，先于 delay 判定），服务端对带组消息统一施加顺序锁即得到正确语义。代价如实记录：普通消息与顺序消息混发同一 topic 时，被顺序锁拦住的消息会队头阻塞其后的普通消息（见决策 3），推荐专用 FIFO topic——与 RocketMQ 官方实践一致，写入 README。

**2. 顺序锁的不变式：每 (group, topic, queue) 至多 1 条 `Ordered` inflight。**
锁的载体就是 inflight 记录本身（`InflightState.Ordered` 标记），无新增键空间、崩溃恢复零代码：重启后 inflight 还在，锁就还在。receiveOnce 阶段 1 扫 inflight 时顺带得出「本队列是否存在未终结顺序 inflight」（`orderedBusy`），阶段 2 据此拦截。不变式由投递路径自身维护：投出一条顺序消息即置 busy，本轮及后续轮次都不再投第二条，直到它被 ack（记录消失）或转 DLQ（同批删除）。

**3. 被拦消息不推进位点——这是「卡住而不跳过」的实现根基。**
现有 cursor 语义是「越过已检视消息」；顺序锁拦下的消息**未被检视投递**，扫描在它面前停止且不推进 newCursor。由此：崩溃重启后从原位点重扫，被拦消息仍是下一条候选；无需任何补偿逻辑。副作用即决策 1 的队头阻塞：拦住一条顺序消息，其后的普通消息同样取不到（位点在它前面）。Tag 过滤的既有语义不变：不匹配的消息（含顺序消息）仍永久跳过并推进位点——它对本消费组永不投递，跳过不破坏"已投递消息间"的顺序。**检查顺序必须是 Tag 过滤在前、顺序锁在后**，否则被锁拦住的不匹配消息会永远堵住位点。

**4. 顺序消息重投不设退避下限（spec §5 流程 6：“非顺序消息重试指数退避”）。**
顺序消息失败要的是快速原地重投（卡队头），指数退避会把整条队列拖住数分钟。重投时不可见时长直接用客户端值；协议层 `InvisibleDuration` 回填（receive.go 的 RetryBackoff 兜底）同步跳过顺序消息，两侧判据一致（`MessageGroup == ""` 才用退避）。超限仍走既有 moveToDLQ（原子删 inflight = 解锁），队列推进。

**5. `ForwardMessageToDeadLetterQueue` RPC 在 M4 实现（spec §6 RPC 表：顺序消息重试超限显式入 DLQ）。**
请求自带 `ReceiptHandle`，sq 的 handle 编码了 (group, topic, queue, offset, attempt) 五元组，可直接定位 inflight——无需 message_id 索引。Go SDK SimpleConsumer 不调用它（Java PushConsumer FIFO 才用），因此 e2e 不覆盖，单测用原生 client 直接打 RPC 验证。

**6. SDK 客户端侧校验暗礁（与 M3 DELAY 完全同型）：路由必须通告 FIFO。**
sq 下发 `ValidateMessageType: true`，SDK 发送前用路由的 `AcceptMessageTypes` 本地校验（producer.go:191）——不加 FIFO，顺序消息在客户端就发不出去，且报错发生在客户端极难排查。Task 3 修改并配守护测试。

**7. ack 解锁后的投递延迟 ≤100ms，无需新增唤醒机制。**
长轮询的唤醒信号只覆盖"新消息写入"；ack 解锁属时间外事件，由 Receive 既有的 100ms 兜底轮询发现（deliver.go Receive 内注释已说明该兜底同时覆盖"过期重投"这类无事件可订阅的场景，ack 解锁同属此类）。对"秒级顺序消费"足够，不为此加 ack→wake 链路。

## File Structure

| 文件 | 动作 | 职责 |
|---|---|---|
| `internal/core/types.go` | 修改 | InflightState 增 `Ordered` 标记（omitempty 兼容） |
| `internal/core/types_test.go` | 修改 | Ordered 往返 + 旧数据兼容测试 |
| `internal/core/deliver/deliver.go` | 修改 | receiveOnce 顺序锁；重投保留 Ordered、顺序不退避；新增 ForwardToDLQ |
| `internal/core/deliver/deliver_test.go` | 修改 | 顺序锁全套单测 |
| `internal/rpc/send.go` | 修改 | toCoreMessage 放开 FIFO、组合冲突校验 |
| `internal/rpc/send_test.go` | 修改 | FIFO 写路径 + 路由通告守护测试 |
| `internal/rpc/receive.go` | 修改 | toPBMessage FIFO 回填；InvisibleDuration 回填跳过顺序 |
| `internal/rpc/receive_test.go` | 修改 | FIFO 回填与全链路测试 |
| `internal/rpc/forward.go` | 新建 | ForwardMessageToDeadLetterQueue RPC |
| `internal/rpc/forward_test.go` | 新建 | Forward RPC 测试 |
| `internal/rpc/server.go` | 修改 | messageQueues 通告 FIFO |
| `internal/rpc/settings.go` | 修改 | 注释同步（通告列表变更） |
| `test/e2e/sdk_fifo_test.go` | 新建 | 官方 SDK 顺序投递 + 卡住语义两条用例 |
| `README.md` | 修改 | 功能清单与限制更新 |

`cmd/sq/main.go` 无改动（无新后台任务）；**无 config 改动**。

---

### Task 1: InflightState 增加 Ordered 标记

**Files:**
- Modify: `internal/core/types.go`（InflightState 定义，约 118 行）
- Test: `internal/core/types_test.go`

**Interfaces:**
- Produces: `core.InflightState.Ordered bool`（json `ordered,omitempty`）——Task 2 的顺序锁判据；Encode/DecodeInflight 签名不变。

- [ ] **Step 1: 写失败测试**

在 `internal/core/types_test.go` 追加：

```go
func TestInflightOrderedRoundTripAndCompat(t *testing.T) {
	// 新字段往返
	raw := EncodeInflight(&InflightState{ExpireAtMs: 100, Attempts: 2, Ordered: true})
	got, err := DecodeInflight(raw)
	if err != nil || !got.Ordered || got.ExpireAtMs != 100 || got.Attempts != 2 {
		t.Fatalf("Ordered 往返失败: %+v %v", got, err)
	}
	// 旧数据兼容：M3 及以前落盘的 inflight JSON 没有 ordered 键，解码得 false
	old, err := DecodeInflight([]byte(`{"expire_at_ms":100,"attempts":1}`))
	if err != nil || old.Ordered {
		t.Fatalf("旧数据兼容失败: %+v %v", old, err)
	}
	// 零值不产生新键：非顺序 inflight 编码结果与升级前逐字节一致
	if bytes.Contains(EncodeInflight(&InflightState{ExpireAtMs: 1, Attempts: 1}), []byte("ordered")) {
		t.Fatal("零值 Ordered 不应出现在 JSON 中")
	}
}
```

（该文件已 import `bytes`——M3 加过；若编译报未使用/缺失自查 import 块。）

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/ -run TestInflightOrderedRoundTripAndCompat -v`
Expected: FAIL（`unknown field Ordered`，编译错误即失败形态）

- [ ] **Step 3: 最小实现**

`internal/core/types.go` 的 InflightState 改为：

```go
type InflightState struct {
	ExpireAtMs int64 `json:"expire_at_ms"` // 不可见截止时间；早于 now 即可重投
	Attempts   int32 `json:"attempts"`     // 已投递次数（首投=1）
	// Ordered 顺序消息标记（M4）：true 表示这条 inflight 对应 MessageGroup 非空
	// 的顺序消息，它的存在即该队列顺序锁被占用——deliver 不变式：每
	// (group,topic,queue) 至多 1 条 Ordered inflight。omitempty：M3 及以前
	// 落盘的旧记录无此键，解码得 false（非顺序），无需迁移。
	Ordered bool `json:"ordered,omitempty"`
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/ -v`
Expected: 全部 PASS（含既有用例——零值编码不变）

- [ ] **Step 5: Commit**

```bash
git add internal/core/types.go internal/core/types_test.go
git commit -m "feat(core): InflightState 增 Ordered 顺序标记（旧数据零值兼容）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: deliver 顺序锁——一次一条、卡住不跳过、超限 DLQ 解锁

**Files:**
- Modify: `internal/core/deliver/deliver.go`（receiveOnce 阶段 1/阶段 2、包头注释）
- Test: `internal/core/deliver/deliver_test.go`

**Interfaces:**
- Consumes: Task 1 的 `core.InflightState.Ordered`。
- Produces: receiveOnce 行为变更（签名不变）——顺序消息一次一条、未 ack 阻塞、重投无退避下限、位点停在被拦消息前。Task 3/4 的 rpc 测试依赖此行为。

- [ ] **Step 1: 写失败测试**

在 `internal/core/deliver/deliver_test.go` 追加测试辅助与用例（fixture 默认 1 队列，顺序消息全落 queue 0）：

```go
// sendGrouped 发送一条顺序消息（MessageGroup 非空，M4 顺序锁用例专用辅助）
func (f *fixture) sendGrouped(t *testing.T, topic, body, group string) {
	t.Helper()
	if _, err := f.pr.Append(&core.Message{Topic: topic, Body: []byte(body), MessageGroup: group}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// 顺序消息一次只投一条：未 ack 时后续顺序消息全部被拦，ack 后放行下一条
func TestOrderedDeliversOneAtATime(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	f.sendGrouped(t, "t", "b", "g1")
	f.sendGrouped(t, "t", "c", "g1")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 || string(msgs[0].Body) != "a" {
		t.Fatalf("maxMsgs=10 也只能投第 1 条顺序消息: %d %v", len(msgs), err)
	}
	// 未 ack 期间再取：空（顺序锁占用）
	again, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(again) != 0 {
		t.Fatalf("未 ack 不应投出后续顺序消息: %d %v", len(again), err)
	}
	// ack 后放行下一条
	if _, err := f.dl.Ack("g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(next) != 1 || string(next[0].Body) != "b" {
		t.Fatalf("ack 后应投第 2 条: %d %v", len(next), err)
	}
}

// 卡住语义：过期重投的还是队头那条（attempt 递增），绝不先投下一条；
// 且重投后顺序锁仍占用（Ordered 标记在重投时被保留）
func TestOrderedStuckOnExpiredRedelivery(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	f.sendGrouped(t, "t", "b", "g1")
	first, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 30*time.Millisecond, 0, nil)
	if err != nil || len(first) != 1 || string(first[0].Body) != "a" {
		t.Fatalf("首投: %d %v", len(first), err)
	}
	time.Sleep(50 * time.Millisecond)
	red, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(red) != 1 || string(red[0].Body) != "a" || red[0].DeliveryAttempt != 2 {
		t.Fatalf("过期后应重投队头 a（attempt=2）而非跳到 b: %+v %v", red, err)
	}
	// 重投后 b 仍被拦（Ordered 标记未在重投中丢失）
	blocked, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("重投后 b 仍应被顺序锁拦住: %d %v", len(blocked), err)
	}
	// ack 重投那次的句柄（attempt=2）后放行 b
	if _, err := f.dl.Ack("g", "t", 0, red[0].Offset, red[0].DeliveryAttempt); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	next, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if len(next) != 1 || string(next[0].Body) != "b" {
		t.Fatalf("ack 后应投 b: %d", len(next))
	}
}

// 顺序消息重投不设指数退避下限（spec §5 流程 6 退避仅限非顺序消息）：
// 用远小于 retryBackoffBase(10s) 的不可见时长连续重投，attempt 快速递增。
// 若误用退避下限，attempt=3 那次要等 10s，本用例会超时失败。
func TestOrderedRedeliveryNoBackoffFloor(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	m, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, 30*time.Millisecond, 0, nil)
	if err != nil || len(m) != 1 {
		t.Fatalf("首投: %d %v", len(m), err)
	}
	for want := int32(2); want <= 3; want++ {
		time.Sleep(50 * time.Millisecond)
		m, err = f.dl.Receive(context.Background(), "g", "t", 0, 1, 30*time.Millisecond, 0, nil)
		if err != nil || len(m) != 1 || m[0].DeliveryAttempt != want {
			t.Fatalf("顺序重投应立即可得（无退避下限），attempt 期望 %d: %+v %v", want, m, err)
		}
	}
}

// 超限转 DLQ 后队列解锁推进：卡住的消息进 %DLQ%{group}，下一条顺序消息可投
func TestOrderedExhaustedToDLQUnblocksQueue(t *testing.T) {
	f := newFixtureMaxAttempts(t, 2)
	f.sendGrouped(t, "t", "a", "g1")
	f.sendGrouped(t, "t", "b", "g1")
	// attempt 1、2 各一轮，均不 ack、放到过期
	for i := 0; i < 2; i++ {
		m, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, 30*time.Millisecond, 0, nil)
		if err != nil || len(m) != 1 || string(m[0].Body) != "a" {
			t.Fatalf("第 %d 轮应投 a: %+v %v", i+1, m, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// attempts 已达上限：本轮把 a 转 DLQ 并解锁，b 在同轮或下一轮可投
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, nil)
	if err != nil || len(next) != 1 || string(next[0].Body) != "b" {
		t.Fatalf("a 入 DLQ 后应投 b: %+v %v", next, err)
	}
	// DLQ 里恰有 a，且 MessageGroup 已清空（死信不再参与顺序，moveToDLQ 既有行为）
	dlq, err := f.dl.Receive(context.Background(), "g", meta.DLQTopicName("g"), 0, 10, time.Minute, 0, nil)
	if err != nil || len(dlq) != 1 || string(dlq[0].Body) != "a" || dlq[0].MessageGroup != "" {
		t.Fatalf("DLQ 内容不符: %+v %v", dlq, err)
	}
}

// 混发队列的队头阻塞语义（设计决策 3）：顺序消息被投出后，其后的普通消息
// 照常投（顺序锁只拦顺序消息）；被锁拦下的顺序消息则连同其后的一切消息
// 都取不到（位点停在它前面），ack 解锁后继续。
func TestMixedQueueHeadOfLineBlocking(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1") // offset 0
	f.send(t, "t", "n")              // offset 1，普通消息
	f.sendGrouped(t, "t", "c", "g1") // offset 2
	f.send(t, "t", "d")              // offset 3，普通消息，被 c 队头阻塞
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 2 || string(msgs[0].Body) != "a" || string(msgs[1].Body) != "n" {
		t.Fatalf("应投出 a 与 n，c/d 被拦: %+v %v", msgs, err)
	}
	// 位点停在 c（offset 2）之前——崩溃重启后 c 仍是下一条候选
	v, ok, err := f.st.Get(store.CursorKey("g", "t", 0))
	if err != nil || !ok || store.GetU64(v) != 2 {
		t.Fatalf("位点应停在被拦消息处（2）: %v %v", v, err)
	}
	if _, err := f.dl.Ack("g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt); err != nil {
		t.Fatalf("Ack a: %v", err)
	}
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(next) != 2 || string(next[0].Body) != "c" || string(next[1].Body) != "d" {
		t.Fatalf("解锁后应投 c 与 d: %+v %v", next, err)
	}
}

// Tag 过滤先于顺序锁（设计决策 3）：不匹配的顺序消息永久跳过、推进位点，
// 不会被锁拦成永久堵塞
func TestOrderedTagFilteredStillSkipped(t *testing.T) {
	f := newFixture(t)
	if _, err := f.pr.Append(&core.Message{Topic: "t", Body: []byte("a"), MessageGroup: "g1", Tag: "keep"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pr.Append(&core.Message{Topic: "t", Body: []byte("b"), MessageGroup: "g1", Tag: "drop"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pr.Append(&core.Message{Topic: "t", Body: []byte("c"), MessageGroup: "g1", Tag: "keep"}); err != nil {
		t.Fatal(err)
	}
	filter, err := ParseTagFilter("keep")
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, filter)
	if err != nil || len(msgs) != 1 || string(msgs[0].Body) != "a" {
		t.Fatalf("首投 a: %+v %v", msgs, err)
	}
	if _, err := f.dl.Ack("g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt); err != nil {
		t.Fatal(err)
	}
	// b 不匹配被永久跳过，直接投 c
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, filter)
	if err != nil || len(next) != 1 || string(next[0].Body) != "c" {
		t.Fatalf("b 应被过滤跳过、投出 c: %+v %v", next, err)
	}
}
```

（`ParseTagFilter(expr string) (*TagFilter, error)` 是 deliver 包既有构造函数，filter.go:24，签名已核对。）

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/deliver/ -run 'TestOrdered|TestMixedQueue' -v`
Expected: FAIL——现有实现无顺序锁，`TestOrderedDeliversOneAtATime` 首次 Receive 即返回 3 条。

- [ ] **Step 3: 实现顺序锁（receiveOnce 两处修改）**

`internal/core/deliver/deliver.go` receiveOnce **阶段 1** 整段替换为（变化：redeliver 增 ordered 字段；扫描不再提前停；重投保留 Ordered、顺序不退避；DLQ/孤儿清理解锁）：

```go
	// 阶段 1：重投过期 inflight。k/v 回调内有效，需解析后立即使用
	type redeliver struct {
		offset   uint64
		attempts int32
		ordered  bool
	}
	var reds []redeliver
	// orderedBusy：本队列是否存在未终结的顺序 inflight（顺序锁的内存判据，
	// spec §5 流程 4）。不变式「每队列至多 1 条 Ordered inflight」由阶段 2 的
	// 投递门维护，因此单个 bool 足够，无需计数。
	orderedBusy := false
	pfx := store.InflightPrefix(group, topic, queueID)
	err := d.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		_, _, _, off, err := store.ParseInflightKey(k)
		if err != nil {
			return false, err
		}
		ist, err := core.DecodeInflight(v)
		if err != nil {
			return false, err
		}
		if ist.Ordered {
			orderedBusy = true
		}
		if ist.ExpireAtMs <= now && len(reds) < maxMsgs {
			reds = append(reds, redeliver{offset: off, attempts: ist.Attempts, ordered: ist.Ordered})
		}
		// M1-M3 在收满 maxMsgs 个重投候选后提前停扫；M4 起必须看完整个队列的
		// inflight——顺序锁判据 orderedBusy 需要完整视野，提前停会漏看排在
		// 后面的 Ordered 记录，导致顺序锁形同虚设。代价可控：单队列 inflight
		// 条数以未 ack 消息数为上界，远小于消息总量。
		return true, nil
	})
	if err != nil {
		b.Close() // 未提交，按批次生命周期契约自行回收
		return nil, fmt.Errorf("扫描 inflight (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	for _, r := range reds {
		raw, ok, err := d.st.Get(store.MsgKey(topic, queueID, r.offset))
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("读取重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, r.offset, err)
		}
		if !ok {
			// 消息已被 retention 清理但 inflight 残留：清掉记录并跳过（M2 起 retention 会同步清理）。
			// 这条 Delete 必须真正提交——见本函数收尾处关于 staged 的说明，否则孤儿
			// 记录永不消失，长轮询每 100ms 轮询一次就会反复扫描、反复打这条 Warn，
			// 形成永久日志洪水。
			d.logger.Warn("inflight 指向的消息不存在，清理孤儿记录", "group", group, "topic", topic, "queue", queueID, "offset", r.offset)
			b.Delete(store.InflightKey(group, topic, queueID, r.offset), nil)
			staged = true
			if r.ordered {
				// 被清理的正是持有顺序锁的记录：锁随记录消失（不变式保证至多一条）
				orderedBusy = false
			}
			continue
		}
		m, err := core.DecodeMessage(raw)
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("解码重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, r.offset, err)
		}
		// 投递次数耗尽：转入死信 topic，不再投递。
		// DLQ 写入与 inflight 删除经 AppendWith 同批原子提交，与本函数的取件批次 b
		// 相互独立（此消息不进 b，不置 staged）。崩溃窗口内至多重复转入
		//（at-least-once），死信消费端按消息 ID 幂等即可。
		// 锁语义没有破坏：本方法已持队列锁，AppendWith 拿的是 produce 自己的锁，
		// 两把锁全程单向（deliver → produce），无环即无死锁。
		if r.attempts >= maxAttempts {
			if err := d.moveToDLQ(group, topic, queueID, r.offset, m); err != nil {
				b.Close()
				return nil, err
			}
			if r.ordered {
				// 卡住队头的顺序消息已随 inflight 一并移除：顺序锁释放，
				// 本轮阶段 2 即可投出下一条顺序消息（卡住→超限→推进，spec 流程 4/6）
				orderedBusy = false
				d.logger.Info("顺序消息超限入死信，队列解锁", "group", group, "topic", topic,
					"queue", queueID, "offset", r.offset, "msg_id", m.ID)
			}
			continue
		}
		m.DeliveryAttempt = r.attempts + 1
		// 指数退避只作用于非顺序消息（spec §5 流程 6）：顺序消息要的是原地
		// 快速重投（卡队头），退避会把整条队列拖住数分钟——不可见时长直接用
		// 客户端值。协议层 InvisibleDuration 回填用同一判据（receive.go）。
		exp := expireAt
		if !r.ordered {
			if bo := retryBackoff(m.DeliveryAttempt); bo > invisible {
				exp = now + bo.Milliseconds()
			}
		}
		// 重投必须原样保留 Ordered 标记：丢了它，重投后的记录不再被视为
		// 顺序锁占用，下一条顺序消息会与卡在队头的这条并发投递，顺序即破。
		b.Set(store.InflightKey(group, topic, queueID, r.offset),
			core.EncodeInflight(&core.InflightState{ExpireAtMs: exp, Attempts: m.DeliveryAttempt, Ordered: r.ordered}), nil)
		out = append(out, m)
		staged = true
	}
```

**阶段 2** 的 Scan 回调整段替换为（变化：位点推进移到 Tag 过滤之后、顺序锁判定与投递门）：

```go
		err = d.st.Scan(lower, upper, scanBudget, func(k, v []byte) (bool, error) {
			m, err := core.DecodeMessage(v)
			if err != nil {
				return false, err
			}
			// Tag 过滤在顺序锁之前：不匹配的消息（含顺序消息）对本消费组永久
			// 跳过、推进位点（spec §5 流程 2 语义不变）。顺序消息被过滤跳过不
			// 破坏顺序——它对本组从未投递，"已投递消息间"的相对顺序完好。
			// 若把顺序锁放在前面，被锁拦住的不匹配消息会永远堵住位点。
			if !filter.Match(m.Tag) {
				newCursor = m.Offset + 1
				skipped++
				return true, nil
			}
			// 顺序锁（spec §5 流程 4）：队列存在未终结的顺序 inflight 时，
			// 后续顺序消息不投。停止扫描且【不推进 newCursor】——这条消息
			// 未被投递，位点停在它前面，崩溃重启后它仍是下一条候选（卡住
			// 而不跳过的实现根基）。副作用：其后的普通消息一并等待（队头
			// 阻塞，设计决策 3，README 建议顺序消息用专用 topic）。
			if m.MessageGroup != "" && orderedBusy {
				d.logger.Debug("顺序锁阻塞取件", "group", group, "topic", topic,
					"queue", queueID, "blocked_offset", m.Offset)
				return false, nil
			}
			newCursor = m.Offset + 1
			m.DeliveryAttempt = 1
			ordered := m.MessageGroup != ""
			if ordered {
				// 投出即占锁：本轮扫描内的下一条顺序消息就会被上面的判定拦下，
				// 由此维持「每队列至多 1 条 Ordered inflight」不变式
				orderedBusy = true
			}
			b.Set(store.InflightKey(group, topic, queueID, m.Offset),
				core.EncodeInflight(&core.InflightState{ExpireAtMs: expireAt, Attempts: 1, Ordered: ordered}), nil)
			out = append(out, m)
			return len(out) < maxMsgs, nil
		})
```

- [ ] **Step 4: 加关键节点日志**（上面代码已内嵌，核对清单）
  - 顺序锁阻塞取件：Debug（每次 Receive 至多一条，长轮询下频繁，不能 Info）
  - 顺序消息超限入死信解锁：Info（状态变更，含 group/topic/queue/offset/msg_id）
  - 重投/投递的既有 Debug 日志保持不变；孤儿清理 Warn 保持不变
  - 全部经 `d.logger`（slog），无 `fmt.Printf`

- [ ] **Step 5: 加注释**（上面代码已内嵌，核对清单）
  - 阶段 1 不再提前停扫的为什么（顺序锁需要完整视野）
  - 重投保留 Ordered 的为什么（丢标记 = 锁失效）
  - 顺序不退避的为什么（spec 流程 6 + 卡队头语义）
  - 阶段 2 不推进位点的为什么（卡住语义 + 崩溃安全）与 Tag 过滤先行的为什么
  - 包头注释更新：边界一节 "Tag 过滤已落地……" 之后追加一行
    `//   - 顺序消息（M4）：队列级顺序锁，MessageGroup 非空即顺序；重投无退避、超限入 DLQ 解锁`

- [ ] **Step 6: 跑测试确认通过**

Run: `go test ./internal/core/deliver/ -v`
Expected: 新用例与全部既有用例（重投、退避、Tag、DLQ、长轮询）PASS——既有非顺序行为不受影响。

- [ ] **Step 7: Commit**

```bash
git add internal/core/deliver/deliver.go internal/core/deliver/deliver_test.go
git commit -m "feat(deliver): 队列级顺序锁——一次一条、卡住不跳过、超限入 DLQ 解锁

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: rpc 写方向——放开 FIFO、组合冲突校验、路由通告

**Files:**
- Modify: `internal/rpc/send.go`（toCoreMessage 的 switch、文件头注释）
- Modify: `internal/rpc/server.go`（messageQueues 的 AcceptMessageTypes，约 149 行）
- Modify: `internal/rpc/settings.go`（约 91 行的通告列表注释）
- Test: `internal/rpc/send_test.go`

**Interfaces:**
- Consumes: Task 2 的顺序投递行为（全链路断言用）。
- Produces: SendMessage 接受 `MessageType_FIFO`（MessageGroup 必填、不得带 delivery_timestamp）；NORMAL/DELAY 携带 message_group 被拒。路由通告含 FIFO。

- [ ] **Step 1: 写失败测试**

在 `internal/rpc/send_test.go` 追加：

```go
// FIFO 消息经全链路投递且遵守顺序锁：发 2 条同组，deliver 一次只吐 1 条
func TestSendFifoMessageOrderedThroughStack(t *testing.T) {
	env := newTestEnv(t, true)
	for _, body := range []string{"f1", "f2"} {
		resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
			Messages: []*pb.Message{{
				Topic: &pb.Resource{Name: "fifo"},
				SystemProperties: &pb.SystemProperties{
					MessageType:  pb.MessageType_FIFO,
					MessageGroup: strPtr("grp-1"),
				},
				Body: []byte(body),
			}},
		})
		if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
			t.Fatalf("FIFO 发送应成功: %v %v", resp.GetStatus(), err)
		}
	}
	// 同组消息落同一队列；顺序锁下首轮只投 f1。队列号由 hash 决定，逐队列探测。
	got := 0
	for q := uint32(0); q < 4; q++ {
		msgs, err := env.dl.Receive(context.Background(), "g", "fifo", q, 10, time.Minute, 0, nil)
		if err != nil {
			t.Fatalf("Receive q%d: %v", q, err)
		}
		got += len(msgs)
		for _, m := range msgs {
			if string(m.Body) != "f1" || m.MessageGroup != "grp-1" {
				t.Fatalf("首轮只应投 f1 且组名保留: %+v", m)
			}
		}
	}
	if got != 1 {
		t.Fatalf("顺序锁下首轮应恰投 1 条，实际 %d", got)
	}
}

func TestSendFifoMissingGroupRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "fifo"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_FIFO},
			Body:             []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_ILLEGAL_MESSAGE_GROUP {
		t.Fatalf("期望 ILLEGAL_MESSAGE_GROUP，得到 %v %v", resp.GetStatus(), err)
	}
}

func TestSendFifoWithDeliveryTimestampRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fifo"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_FIFO,
				MessageGroup:      strPtr("grp"),
				DeliveryTimestamp: timestamppb.New(time.Now().Add(time.Hour)),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("期望 MESSAGE_PROPERTY_CONFLICT_WITH_TYPE，得到 %v %v", resp.GetStatus(), err)
	}
}

// NORMAL/DELAY 携带 message_group 被拒：SDK 只要设了组就自动标 FIFO，
// 标其他类型却带组的只可能是行为异常的客户端；静默收下会让消息悄悄获得/
// 失去顺序语义（与 M3 的 NORMAL+delivery_timestamp 拒绝完全同型）
func TestSendNormalWithMessageGroupRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fifo"},
			SystemProperties: &pb.SystemProperties{
				MessageType:  pb.MessageType_NORMAL,
				MessageGroup: strPtr("grp"),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("期望 MESSAGE_PROPERTY_CONFLICT_WITH_TYPE，得到 %v %v", resp.GetStatus(), err)
	}
}

func TestSendDelayWithMessageGroupRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fifo"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_DELAY,
				MessageGroup:      strPtr("grp"),
				DeliveryTimestamp: timestamppb.New(time.Now().Add(time.Hour)),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("期望 MESSAGE_PROPERTY_CONFLICT_WITH_TYPE，得到 %v %v", resp.GetStatus(), err)
	}
}

// 守护 SDK 客户端侧校验（与 M3 的 DELAY 守护测试同型）：ValidateMessageType=true
// 时 SDK 发送前检查路由的 AcceptMessageTypes，缺 FIFO 则顺序消息在客户端本地
// 就被拒（producer.go:191）
func TestQueryRouteAdvertisesFifoType(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{Topic: &pb.Resource{Name: "fifo"}})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("QueryRoute: %v %v", resp.GetStatus(), err)
	}
	for _, mq := range resp.GetMessageQueues() {
		hasFifo := false
		for _, mt := range mq.GetAcceptMessageTypes() {
			if mt == pb.MessageType_FIFO {
				hasFifo = true
			}
		}
		if !hasFifo {
			t.Fatalf("队列 %d 未通告 FIFO 类型", mq.GetId())
		}
	}
}
```

（`strPtr` 是 send_test.go 既有辅助；`timestamppb`/`time` 已在 M3 引入该文件。）

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestSendFifo|TestSendNormalWithMessageGroup|TestSendDelayWithMessageGroup|TestQueryRouteAdvertisesFifo' -v`
Expected: FAIL——当前 FIFO 分支直接拒绝（"顺序消息将在 M4 支持"）。

- [ ] **Step 3: 实现**

`internal/rpc/send.go` toCoreMessage 的 switch 改为：

```go
	switch sp.GetMessageType() {
	case pb.MessageType_NORMAL:
		// SDK 只要带 deliveryTimestamp 就自动标 DELAY 类型，标 NORMAL 却带
		// 到期时间的只可能是行为异常的客户端。静默忽略该时间戳等于把"延时"
		// 悄悄变成"立即投递"，必须显式拒绝。
		if sp.GetDeliveryTimestamp() != nil {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"NORMAL 消息不应携带 delivery_timestamp")
		}
		// 同理（M4）：SDK 设了 messageGroup 就自动标 FIFO，标 NORMAL 却带组
		// 意味着这条消息会在 deliver 侧获得顺序锁语义而发送端不自知——拒绝。
		if sp.GetMessageGroup() != "" {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"NORMAL 消息不应携带 message_group")
		}
	case pb.MessageType_FIFO:
		// 顺序消息（M4）：MessageGroup 是顺序语义的全部依据（hash 定队列 +
		// 消费端顺序锁），缺了它"顺序"无从谈起，用协议专用码报错。
		if sp.GetMessageGroup() == "" {
			return nil, errStatus(pb.Code_ILLEGAL_MESSAGE_GROUP, "FIFO 消息缺少 message_group")
		}
		if sp.GetDeliveryTimestamp() != nil {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"FIFO 消息不应携带 delivery_timestamp")
		}
	case pb.MessageType_DELAY:
		if sp.GetDeliveryTimestamp() == nil {
			return nil, errStatus(pb.Code_ILLEGAL_DELIVERY_TIME, "DELAY 消息缺少 delivery_timestamp")
		}
		// 延时与顺序不可组合（M4）：SDK 两者都设时按组判定标 FIFO（上面的
		// 分支拒绝），裸客户端标 DELAY 带组同样拒绝——到期搬运经 AppendWith
		// 重新入队，无法承诺组内相对顺序。
		if sp.GetMessageGroup() != "" {
			return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
				"DELAY 消息不应携带 message_group")
		}
		delayAt = sp.GetDeliveryTimestamp().AsTime().UnixMilli()
		// 时间戳存在但落在非正区间（1970 epoch 或零值 time.Time）时不能静默
		// 降级成 NORMAL：DeliverAtMs 停在 0 会让 SendMessage 的路由门
		// m.DeliverAtMs>0 走不到 AppendDelay，消息被当普通消息落盘，类型与
		// 时间戳回读双双丢失。钳到 1ms 保证路由进门，已过期由 AppendDelay
		// 的直通逻辑立即投递，DELAY 语义原样保留。
		if delayAt <= 0 {
			delayAt = 1
		}
	case pb.MessageType_TRANSACTION:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "事务消息将在 M6 支持")
	default:
		...（保持现状）
	}
```

（DELAY 分支仅新增 message_group 拒绝，其余为 M3 现状原样保留——对照现文件逐行核对，勿覆盖 M3 的钳 1ms 逻辑。FIFO 消息的落盘路径无需改动：`DeliverAtMs==0` 走 `pr.Append`，MessageGroup hash 定队列在 produce 内部。）

文件头注释第一行改为：
`// SendMessage 相关：proto Message ↔ core.Message 的写方向翻译。`
`// 边界：NORMAL、DELAY（M3）与 FIFO（M4）；M6 事务在其里程碑打开。`

`internal/rpc/server.go` messageQueues 的 AcceptMessageTypes 改为：

```go
			AcceptMessageTypes: []pb.MessageType{
				pb.MessageType_NORMAL,
				// M3 起接受延时消息。不能漏：SDK 开着 ValidateMessageType，
				// 发送前用本列表在客户端本地校验，缺了 DELAY 则延时消息
				// 根本发不出来
				pb.MessageType_DELAY,
				// M4 起接受顺序消息，理由同上（缺了 FIFO 顺序消息在客户端
				// 本地就被拒；M6 时继续追加 TRANSACTION）
				pb.MessageType_FIFO,
			},
```

`internal/rpc/settings.go` 约 91 行注释里的 `AcceptMessageTypes=[NORMAL, DELAY]（M3 起）` 改为 `AcceptMessageTypes=[NORMAL, DELAY, FIFO]（M4 起）`，后半句 "拒掉顺序/事务消息" 改为 "拒掉事务消息"。

- [ ] **Step 4: 加关键节点日志**：写方向的拒绝路径经既有 errStatus → SendMessage 的统一失败日志链路，无新增日志点；确认无 `fmt.Printf`。

- [ ] **Step 5: 加注释**：上面代码已内嵌（NORMAL 带组为什么拒、FIFO 缺组用什么码、DELAY 与组为什么互斥、路由通告为什么不能漏）；核对文件头与 settings.go 注释已同步。

- [ ] **Step 6: 跑测试确认通过**

Run: `go test ./internal/rpc/ -v`
Expected: 新用例 PASS，且既有全部用例 PASS（M1-M3 无 NORMAL+组合法用例，无需改动既有测试；若有编译/断言失败，先查是不是误改了 DELAY 分支）。

- [ ] **Step 7: Commit**

```bash
git add internal/rpc/send.go internal/rpc/server.go internal/rpc/settings.go internal/rpc/send_test.go
git commit -m "feat(rpc): 接受 FIFO 消息与组合冲突校验，路由通告 FIFO 类型

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: rpc 读方向——FIFO 类型回填、InvisibleDuration 回填跳过顺序

**Files:**
- Modify: `internal/rpc/receive.go`（toPBMessage 的 mtype 判定与退避回填，约 210/247 行）
- Test: `internal/rpc/receive_test.go`

**Interfaces:**
- Consumes: Task 2 的顺序重投无退避行为（回填判据必须与 deliver 侧一致）。
- Produces: 投递方向 `MessageGroup != ""` 的消息回填 `MessageType_FIFO`；顺序消息的 InvisibleDuration 回填不再套退避下限。

- [ ] **Step 1: 写失败测试**

在 `internal/rpc/receive_test.go` 追加：

```go
// 投递方向 FIFO 回填：盘上带 MessageGroup 的消息类型必须回填 FIFO；
// DeliverAtMs 优先（写方向已拒绝两者组合，读方向仍需确定性优先级）
func TestToPBMessageEchoesFifoType(t *testing.T) {
	env := newTestEnv(t, true)
	m := &core.Message{ID: "f1", Topic: "t", Body: []byte("x"), MessageGroup: "grp", DeliveryAttempt: 1}
	pm := env.srv.toPBMessage(m, "g", time.Minute)
	sp := pm.GetSystemProperties()
	if sp.GetMessageType() != pb.MessageType_FIFO || sp.GetMessageGroup() != "grp" {
		t.Fatalf("FIFO 回填不符: type=%v group=%q", sp.GetMessageType(), sp.GetMessageGroup())
	}
	// 组合数据（理论上写方向已拒绝）按 DELAY 优先，保证确定性
	both := &core.Message{ID: "f2", Topic: "t", Body: []byte("x"), MessageGroup: "grp",
		DeliverAtMs: time.Now().UnixMilli(), DeliveryAttempt: 1}
	if env.srv.toPBMessage(both, "g", time.Minute).GetSystemProperties().GetMessageType() != pb.MessageType_DELAY {
		t.Fatal("DeliverAtMs 与 MessageGroup 并存时应按 DELAY 回填")
	}
}

// 顺序消息重投的 InvisibleDuration 回填不套退避下限：deliver 侧对顺序消息
// 不退避（Task 2），协议层若仍按退避公式回填，SDK 换算出的可见时间点会
// 晚于服务端实际，消费端白等
func TestToPBMessageOrderedRedeliveryNoBackoffEcho(t *testing.T) {
	env := newTestEnv(t, true)
	ord := &core.Message{ID: "o1", Topic: "t", Body: []byte("x"), MessageGroup: "grp", DeliveryAttempt: 3}
	pm := env.srv.toPBMessage(ord, "g", time.Second)
	if d := pm.GetSystemProperties().GetInvisibleDuration().AsDuration(); d != time.Second {
		t.Fatalf("顺序重投回填应用客户端值 1s，得到 %v", d)
	}
	// 对照：非顺序重投仍套退避下限（既有行为不回归）
	norm := &core.Message{ID: "n1", Topic: "t", Body: []byte("x"), DeliveryAttempt: 3}
	pm2 := env.srv.toPBMessage(norm, "g", time.Second)
	if d := pm2.GetSystemProperties().GetInvisibleDuration().AsDuration(); d != deliver.RetryBackoff(3) {
		t.Fatalf("非顺序重投应回填退避下限 %v，得到 %v", deliver.RetryBackoff(3), d)
	}
}

// 全链路：FIFO 发送 → 投递 → 消费端读回类型与组名
func TestSendFifoDeliveredWithTypeEcho(t *testing.T) {
	env := newTestEnv(t, true)
	resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fifo2"},
			SystemProperties: &pb.SystemProperties{
				MessageType:  pb.MessageType_FIFO,
				MessageGroup: fifoGroupPtr("grp-2"),
			},
			Body: []byte("hello"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("发送: %v %v", resp.GetStatus(), err)
	}
	pm := receiveOne(t, env.client, "g", "fifo2", 0, time.Minute)
	sp := pm.GetSystemProperties()
	if sp.GetMessageType() != pb.MessageType_FIFO || sp.GetMessageGroup() != "grp-2" {
		t.Fatalf("投递回读不符: type=%v group=%q", sp.GetMessageType(), sp.GetMessageGroup())
	}
}

func fifoGroupPtr(s string) *string { return &s }
```

注意两处按现文件实际情况适配（先看后写，不要照抄）：
1. `receiveOne` 的确切签名以 receive_test.go 现有用法为准（M3 的 `TestSendPastDueDelayDeliveredImmediatelyWithTimestampEcho` 用过，直接对照）。它按 queueID 收取——FIFO 消息落在 hash 决定的队列，未必是 0：全链路用例若收不到，改为逐队列（0..3）尝试 `receiveOne`，或将 topic 换名试出落在 q0 的组名并在注释里说明。**更稳的写法**：跳过 receiveOne，直接 `env.dl` 逐队列 Receive 后用 `env.srv.toPBMessage` 回填断言（与 TestToPBMessageEchoesFifoType 同型），执行者二选一，以能稳定过为准。
2. `fifoGroupPtr` 若与 send_test.go 的 `strPtr` 同包冲突则删掉本函数直接用 `strPtr`（两个 _test.go 同属 package rpc）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestToPBMessageEchoesFifo|TestToPBMessageOrderedRedelivery|TestSendFifoDelivered' -v`
Expected: FAIL——当前 mtype 判定只认 DeliverAtMs，退避回填不区分顺序。

- [ ] **Step 3: 实现**

`internal/rpc/receive.go` toPBMessage 中 mtype 判定改为：

```go
	// 投递时如实回填消息类型：延时看 DeliverAtMs、顺序看 MessageGroup。
	// 写方向已拒绝两者组合，这里的优先级只为对脏数据保持确定性；
	// DLQ 消息的 MessageGroup 在 moveToDLQ 时已清空，回 NORMAL，符合
	// "死信不再参与顺序"的语义。
	mtype := pb.MessageType_NORMAL
	switch {
	case m.DeliverAtMs > 0:
		mtype = pb.MessageType_DELAY
	case m.MessageGroup != "":
		mtype = pb.MessageType_FIFO
	}
```

退避回填处（约 211 行）改为：

```go
	eff := invisible
	// 顺序消息除外（M4）：deliver 对顺序重投不设退避下限（卡队头要的是快速
	// 原地重投，spec §5 流程 6 的退避仅限非顺序），这里的回填判据必须与
	// deliver 侧（receiveOnce 的 !r.ordered）保持一致，否则 SDK 依
	// InvisibleDuration 换算的可见时间点晚于服务端实际。
	if m.DeliveryAttempt >= 2 && m.MessageGroup == "" {
		if bo := deliver.RetryBackoff(m.DeliveryAttempt); bo > eff {
			eff = bo
		}
	}
```

同时把 toPBMessage 的方法注释里 "DeliverAtMs 也在此回填（类型 + DeliveryTimestamp 两个字段）" 一句后追加 "；MessageGroup 非空时类型回填 FIFO（M4）"。

- [ ] **Step 4: 加关键节点日志**：本 task 为纯映射逻辑，无新增 I/O 或状态变更节点，沿用既有日志；确认无 `fmt.Printf`。

- [ ] **Step 5: 加注释**：上面代码已内嵌（类型优先级为什么、退避跳过为什么、判据与 deliver 一致性）；核对方法注释已更新。

- [ ] **Step 6: 跑测试确认通过**

Run: `go test ./internal/rpc/ -v`
Expected: 全部 PASS（既有的非顺序退避回填用例 `TestToPBMessageBackfillsRetryBackoffFloorOnRedelivery` 必须仍然 PASS——它就是本次改动的回归哨兵）。

- [ ] **Step 7: Commit**

```bash
git add internal/rpc/receive.go internal/rpc/receive_test.go
git commit -m "feat(rpc): 投递回填 FIFO 类型；顺序重投 InvisibleDuration 不套退避下限

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: ForwardMessageToDeadLetterQueue RPC（显式入 DLQ）

**Files:**
- Modify: `internal/core/deliver/deliver.go`（新增 ForwardToDLQ 方法）
- Create: `internal/rpc/forward.go`
- Test: `internal/core/deliver/deliver_test.go`、Create: `internal/rpc/forward_test.go`

**Interfaces:**
- Consumes: 既有 `receiptDecode(s) (group, topic string, queueID uint32, offset uint64, attempt int32, err error)`（rpc/receipt.go:46）、既有 `moveToDLQ`、`okStatus()`/`errStatus(code, msg)`。
- Produces: `func (d *Deliverer) ForwardToDLQ(group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error)`；RPC `ForwardMessageToDeadLetterQueue`（成功 OK；句柄无效/陈旧 INVALID_RECEIPT_HANDLE）。

- [ ] **Step 1: 写失败测试（deliver 层）**

在 `internal/core/deliver/deliver_test.go` 追加：

```go
// 显式转入 DLQ：inflight 删除、消息入 %DLQ%{group}、顺序锁释放
func TestForwardToDLQHappyPathUnblocksOrdered(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	f.sendGrouped(t, "t", "b", "g1")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("首投: %d %v", len(msgs), err)
	}
	ok, err := f.dl.ForwardToDLQ("g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt)
	if err != nil || !ok {
		t.Fatalf("ForwardToDLQ: %v %v", ok, err)
	}
	// 顺序锁已释放：b 可投
	next, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, nil)
	if err != nil || len(next) != 1 || string(next[0].Body) != "b" {
		t.Fatalf("forward 后应投 b: %+v %v", next, err)
	}
	// DLQ 里有 a
	dlq, err := f.dl.Receive(context.Background(), "g", meta.DLQTopicName("g"), 0, 10, time.Minute, 0, nil)
	if err != nil || len(dlq) != 1 || string(dlq[0].Body) != "a" {
		t.Fatalf("DLQ 内容不符: %+v %v", dlq, err)
	}
}

// 陈旧句柄幂等拒绝（语义与 Ack 的 attempt 校验一致）：不存在或 attempt
// 不匹配都返回 (false, nil)，不误伤重投后的新记录
func TestForwardToDLQStaleHandle(t *testing.T) {
	f := newFixture(t)
	f.sendGrouped(t, "t", "a", "g1")
	msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, nil)
	if ok, err := f.dl.ForwardToDLQ("g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt+1); ok || err != nil {
		t.Fatalf("attempt 不匹配应幂等拒绝: %v %v", ok, err)
	}
	if ok, err := f.dl.ForwardToDLQ("g", "t", 0, 999, 1); ok || err != nil {
		t.Fatalf("不存在的 offset 应幂等拒绝: %v %v", ok, err)
	}
	// 原 inflight 未被误删：a 仍可 ack
	if ok, err := f.dl.Ack("g", "t", 0, msgs[0].Offset, msgs[0].DeliveryAttempt); !ok || err != nil {
		t.Fatalf("原记录应完好: %v %v", ok, err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/deliver/ -run TestForwardToDLQ -v`
Expected: FAIL（ForwardToDLQ 未定义，编译错误）

- [ ] **Step 3: 实现 deliver.ForwardToDLQ**

在 `internal/core/deliver/deliver.go` 的 ChangeInvisible 之后追加：

```go
// ForwardToDLQ 将一条 inflight 消息显式转入死信 %DLQ%{group}（协议
// ForwardMessageToDeadLetterQueue，spec §6：顺序消息重试超限时客户端显式
// 入 DLQ；对非顺序消息同样可用）。
//
// 参数与校验规则与 Ack 完全一致：attempt 必须与当前持久化的 Attempts 匹配，
// 陈旧句柄（已被重投覆盖）幂等返回 (false, nil)，绝不误伤新一轮投递的记录。
//
// 返回：(true, nil) 已转入（或目标消息已被 retention 清理、孤儿 inflight 已
// 清除——调用方要的"这条消息别再投了"两种情况下都已成立）；(false, nil)
// 目标不存在或句柄陈旧；错误仅在存储故障时返回。
func (d *Deliverer) ForwardToDLQ(group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error) {
	// 与 Ack/ChangeInvisible 同理：直接读写 inflight 必须持队列锁（类型注释
	// 声明的并发安全前提），否则与过期重投的读-改-写交错
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	defer qlock.Unlock()

	k := store.InflightKey(group, topic, queueID, offset)
	v, ok, err := d.st.Get(k)
	if err != nil {
		return false, fmt.Errorf("forward 查询 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if !ok {
		d.logger.Debug("forward 目标不存在（已 ack 或已重投）", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return false, nil
	}
	ist, err := core.DecodeInflight(v)
	if err != nil {
		return false, fmt.Errorf("解码 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	if ist.Attempts != attempt {
		d.logger.Debug("forward attempt 不匹配（陈旧句柄，已被重投覆盖）",
			"group", group, "topic", topic, "queue", queueID, "offset", offset,
			"want_attempt", ist.Attempts, "got_attempt", attempt)
		return false, nil
	}
	raw, ok, err := d.st.Get(store.MsgKey(topic, queueID, offset))
	if err != nil {
		return false, fmt.Errorf("读取 forward 消息 (topic=%s q=%d off=%d): %w", topic, queueID, offset, err)
	}
	if !ok {
		// 消息已被 retention 清理但 inflight 残留：与 receiveOnce 的孤儿清理
		// 同理删除止损。对调用方视为成功——"这条消息别再投了"已经成立。
		b := d.st.NewBatch()
		b.Delete(k, nil)
		if err := d.st.Apply(b); err != nil {
			return false, fmt.Errorf("清理孤儿 inflight (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
		}
		d.logger.Warn("forward 目标消息已不存在，清理孤儿 inflight", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return true, nil
	}
	m, err := core.DecodeMessage(raw)
	if err != nil {
		return false, fmt.Errorf("解码 forward 消息 (topic=%s q=%d off=%d): %w", topic, queueID, offset, err)
	}
	// moveToDLQ 内部：DLQ 写入与 inflight 删除同批原子提交，并打 Info 日志
	if err := d.moveToDLQ(group, topic, queueID, offset, m); err != nil {
		return false, err
	}
	return true, nil
}
```

- [ ] **Step 4: 跑 deliver 测试确认通过**

Run: `go test ./internal/core/deliver/ -v`
Expected: 全部 PASS

- [ ] **Step 5: 写失败测试（rpc 层）**

新建 `internal/rpc/forward_test.go`：

```go
// ForwardMessageToDeadLetterQueue RPC 测试。
//
// 职责（测试文件）：
//   - 验证按 receipt handle 定位并转入 DLQ，成功后同 handle 二次调用失效
//   - 验证非法/陈旧 handle 返回 INVALID_RECEIPT_HANDLE
//
// 边界：
//   - DLQ 转移的原子性与内容由 deliver 单测覆盖，这里只测协议映射
package rpc

import (
	"context"
	"testing"
	"time"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

func TestForwardMessageToDeadLetterQueue(t *testing.T) {
	env := newTestEnv(t, true)
	// 发一条 FIFO 消息并收取，拿到 receipt handle
	resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "fwd"},
			SystemProperties: &pb.SystemProperties{
				MessageType:  pb.MessageType_FIFO,
				MessageGroup: strPtr("grp"),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("发送: %v %v", resp.GetStatus(), err)
	}
	pm := receiveOneAnyQueue(t, env, "g", "fwd", time.Minute)
	handle := pm.GetSystemProperties().GetReceiptHandle()
	fresp, err := env.client.ForwardMessageToDeadLetterQueue(context.Background(),
		&pb.ForwardMessageToDeadLetterQueueRequest{
			Group:         &pb.Resource{Name: "g"},
			Topic:         &pb.Resource{Name: "fwd"},
			ReceiptHandle: handle,
			MessageId:     pm.GetSystemProperties().GetMessageId(),
		})
	if err != nil || fresp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("forward 应成功: %v %v", fresp.GetStatus(), err)
	}
	// 同一 handle 再来一次：目标已消失
	fresp2, err := env.client.ForwardMessageToDeadLetterQueue(context.Background(),
		&pb.ForwardMessageToDeadLetterQueueRequest{ReceiptHandle: handle})
	if err != nil || fresp2.GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE {
		t.Fatalf("重复 forward 应 INVALID_RECEIPT_HANDLE: %v %v", fresp2.GetStatus(), err)
	}
}

func TestForwardMessageMalformedHandle(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.ForwardMessageToDeadLetterQueue(context.Background(),
		&pb.ForwardMessageToDeadLetterQueueRequest{ReceiptHandle: "not-a-handle"})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_INVALID_RECEIPT_HANDLE {
		t.Fatalf("非法 handle 应 INVALID_RECEIPT_HANDLE: %v %v", resp.GetStatus(), err)
	}
}
```

`receiveOneAnyQueue` 辅助（FIFO 消息队列由 hash 决定，逐队列找）——若 receive_test.go 已有等价辅助则复用，否则加在 forward_test.go：

```go
// receiveOneAnyQueue 逐队列（0..3）尝试收取一条消息（FIFO 消息落在 hash
// 决定的队列，测试无法预知队列号）
func receiveOneAnyQueue(t *testing.T, env testEnv, group, topic string, invisible time.Duration) *pb.Message {
	t.Helper()
	for q := uint32(0); q < 4; q++ {
		msgs, err := env.dl.Receive(context.Background(), group, topic, q, 1, invisible, 0, nil)
		if err != nil {
			t.Fatalf("Receive q%d: %v", q, err)
		}
		if len(msgs) == 1 {
			return env.srv.toPBMessage(msgs[0], group, invisible)
		}
	}
	t.Fatal("全部队列均无消息")
	return nil
}
```

- [ ] **Step 6: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run TestForwardMessage -v`
Expected: FAIL——RPC 未实现时 grpc 返回 Unimplemented（error 非 nil）。

- [ ] **Step 7: 实现 rpc 层**

新建 `internal/rpc/forward.go`：

```go
// ForwardMessageToDeadLetterQueue：显式转入死信（spec §6 RPC 表——顺序消息
// 重试超限时由客户端显式入 DLQ；Java PushConsumer 的 FIFO 消费依赖它，
// Go SDK SimpleConsumer 不调用）。
//
// 边界：
//   - 仅按 receipt handle 定位（sq 无 message_id 索引，请求里的 MessageId/
//     DeliveryAttempt 只用于日志），handle 编码见 receipt.go
//   - 转移的原子性（DLQ 写入 + inflight 删除同批）由 deliver.ForwardToDLQ 保证
package rpc

import (
	"context"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// ForwardMessageToDeadLetterQueue 按 receipt handle 将消息转入 %DLQ%{group}。
// 成功返回 OK；handle 无法解析、目标不存在或已被重投覆盖（陈旧句柄）返回
// INVALID_RECEIPT_HANDLE；存储故障返回 INTERNAL_SERVER_ERROR。
func (s *Server) ForwardMessageToDeadLetterQueue(ctx context.Context, req *pb.ForwardMessageToDeadLetterQueueRequest) (*pb.ForwardMessageToDeadLetterQueueResponse, error) {
	g, topic, q, off, attempt, err := receiptDecode(req.GetReceiptHandle())
	if err != nil {
		s.logger.Warn("forward 句柄无法解析", "handle", req.GetReceiptHandle(), "msg_id", req.GetMessageId(), "err", err)
		return &pb.ForwardMessageToDeadLetterQueueResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error())}, nil
	}
	ok, err := s.dl.ForwardToDLQ(g, topic, q, off, attempt)
	if err != nil {
		s.logger.Error("forward 转入死信失败", "group", g, "topic", topic, "queue", q, "offset", off, "err", err)
		return &pb.ForwardMessageToDeadLetterQueueResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
	}
	if !ok {
		// 幂等语义与 Ack 一致：目标不存在/句柄陈旧不算服务端错误，
		// 用协议码告知客户端句柄已失效即可
		return &pb.ForwardMessageToDeadLetterQueueResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, "句柄已失效（已确认或已重投）")}, nil
	}
	s.logger.Info("消息经 forward 显式转入死信", "group", g, "topic", topic, "queue", q, "offset", off, "msg_id", req.GetMessageId())
	return &pb.ForwardMessageToDeadLetterQueueResponse{Status: okStatus()}, nil
}
```

- [ ] **Step 8: 加关键节点日志**（已内嵌，核对清单）：句柄解析失败 Warn（带 handle 与 msg_id）、存储故障 Error（带完整坐标）、成功 Info（成功路径不静默；deliver 侧 moveToDLQ 另有 Info，两条粒度不同不算重复——一条协议视角一条存储视角）。

- [ ] **Step 9: 加注释**（已内嵌，核对清单）：文件头职责+边界、导出方法 doc 注释、幂等语义的为什么。

- [ ] **Step 10: 跑测试确认通过**

Run: `go test ./internal/rpc/ ./internal/core/deliver/ -v`
Expected: 全部 PASS

- [ ] **Step 11: Commit**

```bash
git add internal/core/deliver/deliver.go internal/core/deliver/deliver_test.go internal/rpc/forward.go internal/rpc/forward_test.go
git commit -m "feat(rpc): ForwardMessageToDeadLetterQueue 显式入 DLQ

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: 官方 SDK e2e——顺序投递与卡住语义；README 更新

**Files:**
- Create: `test/e2e/sdk_fifo_test.go`
- Modify: `README.md`

**Interfaces:**
- Consumes: e2e 既有设施 `startBroker(t)`；SDK `msg.SetMessageGroup(string)`、`mv.GetMessageGroup() *string`。

- [ ] **Step 1: 写 e2e 用例**

新建 `test/e2e/sdk_fifo_test.go`：

```go
//go:build e2e

// 官方 Go SDK 顺序消息 e2e：验证 M4 出口标准「顺序锁 + 卡住语义」。
//
// 职责：
//   - 顺序投递：同 MessageGroup 8 条消息严格按发送序到达
//   - 卡住语义：队头消息未 ack 期间只会反复收到它（不可见超时重投），
//     绝不先收到后一条；ack 后放行
//
// 边界：
//   - 不验证多 group 并行吞吐（顺序天然按 group 串行，性能属 spec §10）
//   - ForwardMessageToDeadLetterQueue 由单测覆盖（Go SDK SimpleConsumer 不调用）
package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// newFifoConsumer 构造订阅单 topic 的 SimpleConsumer（本文件专用辅助）
func newFifoConsumer(t *testing.T, endpoint, group, topic string) rmq.SimpleConsumer {
	t.Helper()
	c, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	t.Cleanup(func() { c.GracefulStop() })
	return c
}

// newFifoProducer 构造 producer 并发送 n 条同组顺序消息（body: {prefix}-{i}）
func sendFifoBatch(t *testing.T, endpoint, topic, group, prefix string, n int) {
	t.Helper()
	producer, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := producer.Start(); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	defer producer.GracefulStop()
	for i := 0; i < n; i++ {
		msg := &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("%s-%d", prefix, i))}
		msg.SetMessageGroup(group)
		if _, err := producer.Send(context.Background(), msg); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
}

// TestOfficialGoSDKFIFOOrderedDelivery 同组 8 条消息严格按发送序到达，
// MessageGroup 回读一致，逐条 ack
func TestOfficialGoSDKFIFOOrderedDelivery(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-fifo"
		group = "e2e-fifo-g"
		mg    = "order-key"
		total = 8
	)
	sendFifoBatch(t, endpoint, topic, mg, "fifo", total)
	consumer := newFifoConsumer(t, endpoint, group, topic)
	next := 0
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) && next < total {
		mvs, err := consumer.Receive(context.Background(), 16, 20*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			want := fmt.Sprintf("fifo-%d", next)
			if string(mv.GetBody()) != want {
				t.Fatalf("乱序：期望 %s 收到 %s", want, mv.GetBody())
			}
			if g := mv.GetMessageGroup(); g == nil || *g != mg {
				t.Fatalf("MessageGroup 回读不符: %v", g)
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack %s: %v", want, err)
			}
			next++
		}
	}
	if next != total {
		t.Fatalf("只按序收到 %d/%d 条", next, total)
	}
}

// TestOfficialGoSDKFIFOBlockedUntilAck 卡住语义：first 未 ack 的 20s 窗口内
// 只会收到 first（不可见 5s，期间重投数次、attempt 递增），绝不能见到
// second；ack 最新一次收到的 first 后 second 到达
func TestOfficialGoSDKFIFOBlockedUntilAck(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-fifo-block"
		group = "e2e-fifo-block-g"
		mg    = "block-key"
	)
	sendFifoBatch(t, endpoint, topic, mg, "m", 2) // m-0, m-1
	consumer := newFifoConsumer(t, endpoint, group, topic)

	// 阶段 1：不 ack，观察 20s——收到的每一条都必须是 m-0
	var last *rmq.MessageView
	phase1End := time.Now().Add(20 * time.Second)
	for time.Now().Before(phase1End) {
		mvs, err := consumer.Receive(context.Background(), 16, 5*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != "m-0" {
				t.Fatalf("m-0 未 ack 前收到 %q——顺序锁失效", mv.GetBody())
			}
			last = mv // 只保留最新句柄：旧句柄已被重投覆盖，ack 会被幂等拒绝
		}
	}
	if last == nil {
		t.Fatal("20s 内未收到 m-0")
	}
	// 阶段 2：ack 最新一次的 m-0，m-1 放行
	if err := consumer.Ack(context.Background(), last); err != nil {
		t.Fatalf("Ack m-0: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 20*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != "m-1" {
				t.Fatalf("ack 后期望 m-1，收到 %q", mv.GetBody())
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack m-1: %v", err)
			}
			return
		}
	}
	t.Fatal("ack 后 60s 内未收到 m-1")
}
```

阶段 1 有一个已知的时序窗口需要执行者注意：`last` 的句柄在 ack 时可能刚好又被服务端重投覆盖（5s 不可见 + 循环边界），此时 SDK Ack 返回的错误码是 INVALID_RECEIPT_HANDLE 类。若实际跑出现此抖动，把阶段 1 的最后一轮改为"收到 m-0 后立即 break 出观察循环再 ack"（保证句柄新鲜），并在注释里说明；不要用忽略 Ack 错误的方式糊过去。

- [ ] **Step 2: 单元全量 + 构建先行**

Run: `gofmt -l . && go vet ./... && go test ./...`
Expected: 无输出/全 PASS（e2e 不在默认 tag 内）

- [ ] **Step 3: 跑 e2e**

Run: `go test -tags e2e ./test/e2e/ -run TestOfficialGoSDKFIFO -v -timeout 10m`
Expected: 两条用例 PASS（卡住用例本身要观察 20s+，耐心等）

Run: `go test -tags e2e ./test/e2e/ -v -timeout 20m`
Expected: 全部既有 e2e（普通/重试/DLQ/Tag/Keys/retention/延时/重启恢复）不回归

- [ ] **Step 4: README 更新**

`README.md`：
- 功能清单追加一行：`- 顺序消息：同 MessageGroup 严格按序（FIFO），失败卡队头重投、超限入 DLQ 后推进；建议顺序消息使用专用 topic（顺序锁按队列生效，与普通消息混发会队头阻塞）`
- 「当前状态」行改为：`当前状态：M4（顺序消息）。里程碑与设计见 docs/superpowers/specs/。`
- 「限制」行 `未实现：顺序/事务消息、控制台、多 broker 集群。` 改为 `未实现：事务消息、控制台、多 broker 集群。`

- [ ] **Step 5: Commit**

```bash
git add test/e2e/sdk_fifo_test.go README.md
git commit -m "test(e2e): 官方 SDK 顺序投递与卡住语义；docs: README M4 功能

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec 覆盖对照：**
- §5 流程 4 发送端 hash 定队列 → M1 已有（produce fnv hash），Task 3 全链路测试覆盖
- §5 流程 4 消费端顺序锁（未 ack 不投后续）→ Task 2（TestOrderedDeliversOneAtATime / TestMixedQueueHeadOfLineBlocking）
- §5 流程 4 失败卡队头阻塞重投（卡住不跳过）→ Task 2（TestOrderedStuckOnExpiredRedelivery）+ Task 6 e2e 卡住用例
- §5 流程 6 非顺序才指数退避 → Task 2（TestOrderedRedeliveryNoBackoffFloor）+ Task 4 协议回填一致性
- §5 流程 6 超限入 DLQ → Task 2（TestOrderedExhaustedToDLQUnblocksQueue，复用既有 moveToDLQ）
- §6 RPC 表 ForwardMessageToDeadLetterQueue → Task 5
- §11 M4 出口「顺序锁 + 卡住语义，官方 SDK FIFO 用例过」→ Task 6 两条 e2e
- 核心单测清单「顺序锁」（spec §10 测试策略）→ Task 2 七个用例

**已知取舍（有意为之，不是遗漏）：**
- 顺序语义按消息判定而非 topic 属性：M5 Admin API 之前无处设置 topic 属性（决策 1，README 已提示专用 topic 实践）
- 混发队列队头阻塞：决策 3 的直接推论，测试钉住语义（TestMixedQueueHeadOfLineBlocking）
- ack 解锁靠 100ms 兜底轮询发现，无专用唤醒：决策 7
- 每 receiveOnce 全扫队列 inflight（不再提前停）：顺序锁需完整视野，上界为未 ack 数，注释已说明
- QueryAssignment/顺序负载均衡不改：sq 单 broker，SimpleConsumer 逐队列收取即可

**类型一致性检查：** `InflightState.Ordered`（Task 1 定义，Task 2 读写）；`ForwardToDLQ(group, topic string, queueID uint32, offset uint64, attempt int32) (bool, error)`（Task 5 定义与 rpc 调用一致）；`receiptDecode` 返回五元组与既有 receipt.go:46 签名一致；测试用 fixture（`newFixture`/`newFixtureMaxAttempts`/`f.send`/`f.st`）与现文件一致；`orderedBusy` 仅存在于 receiveOnce 栈上（无跨方法状态）。

**占位符扫描：** 无 TBD/TODO/“类似 Task N”；Task 4 的 receiveOne 一处"按现文件适配"是"先看后写"的核对指令而非占位——指令给出了对照对象（M3 既有用例）与更稳的替代落法，执行者二选一。

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-06-m4-fifo-messages.md`. Two execution options:

**1. Subagent-Driven (recommended)** — 每 task 派新 subagent，task 间评审

**2. Inline Execution** — executing-plans 批量执行带检查点
