# M3 延时消息 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
> **同时必须遵守 instrumenting-code skill**：每个实现类 task 的「加关键节点日志」「加注释」步骤是一等公民，缺失即为 task 未完成。

**Goal:** 官方 RocketMQ 5.x SDK 可发送任意秒级延时消息，到期后经正常投递链路消费，broker 重启不丢（spec §11 M3 出口标准）。

**Architecture:** 带 `deliveryTimestamp` 的消息写入 `delay/{到期ms:8B}{seq:8B}` 暂存区（不进 `msg/`、不占 offset）；独立调度器每 100ms 扫描头部，到期条目经 `produce.AppendWith` 移入目标 topic（此时才分配 queue/offset）并在**同一 WriteBatch** 原子删除 delay 条目。崩溃恢复零代码：暂存区在 Pebble 里，重启后扫描自然继续（spec §5 流程 3、§7 崩溃恢复）。

**Tech Stack:** Go + Pebble v2（现有 store 封装）、`apache.rocketmq.v2` proto、官方 rocketmq-clients Go SDK（e2e 验收）。

## Global Constraints

- 执行基线：分支 `m2-retry-dlq-tag-keys-retention` @ `6aef9a8`（M2 完成态；若先合并回 main 则基线为合并后的 main，内容等价）
- core 不 import 任何 proto/pb 包（spec §3 协议适配层约束）
- 日志一律 `slog`（构造时 `logger.With("mod", "...")`），禁止 `fmt.Printf`；错误日志必带 topic/msg_id 等上下文
- 新文件必须有中文文件头注释（职责 + 边界）；导出函数必须有 doc comment；非显然逻辑写「为什么」注释
- store 批次生命周期契约：`NewBatch` 后要么 `Apply`（提交）要么 `Close`（放弃），两条路径必居其一
- `store.Scan` 回调的 k/v 底层内存仅回调期间有效，跨回调持有必须拷贝
- 提交信息结尾：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- 时钟策略（spec §7）：延时依赖墙钟，回拨仅导致延迟投递（扫描以 `到期 <= now` 为准），不丢失不提前——注释里要写明这一点
- M3 **不新增任何 config 字段**，因此 e2e 的 `writeBrokerConfig` 配置结构体无需改动（M2 踩过的 yaml 零值序列化陷阱本里程碑不存在）

## 基线关键形态（实现者必读）

1. **SDK 客户端侧类型校验（本里程碑最大的协议暗礁）**：官方 Go SDK 生产者只要消息带 `deliveryTimestamp` 就自动标 `MessageType_DELAY`（SDK `publishing_message.go:71`）；且服务端 settings 下发了 `ValidateMessageType: true`，SDK 发送前会检查目标队列路由的 `AcceptMessageTypes` 是否包含 DELAY（SDK `producer.go:191`），**不包含则请求根本不会发出**，客户端本地报 "current message type not match with topic accept message types"。所以 Task 5 必须同步修改 `messageQueues` 的通告，e2e 才可能通过。
2. `produce.AppendWith(m, extra func(b *pebble.Batch))` 已存在（M2 为 DLQ 建的原子扩展写入口），延时到期移入复用它：写消息 + 删 delay 条目同批原子。它内部会 `EnsureTopic`、分配 queue/offset、写 keyidx、唤醒长轮询——延时消息到期后的一切「普通消息待遇」都免费获得。
3. `core.Message` 编码为 JSON，新增字段一律 `omitempty`：旧数据解码得零值、零值编码不产生新键，无需迁移（types.go 已有整段注释说明此约定）。
4. e2e 基建：`startBroker(t, mutate ...func(*config.Config))` 每用例独立 broker；重启类用例用 `writeBrokerConfig` + `launchBroker` + `h.stop(t)` 组合（见 `sdk_recovery_test.go` 的 TestOfficialGoSDKRestartRecovery 范式）。
5. rpc 包测试 fixture：`newTestEnv(t, autoCreate, opts...)` 返回 `testEnv{srv, client, dl, blocked}`；Task 5 为它增加 `st *store.Store` 字段（延时用例要直接查盘上 delay 前缀）。
6. `retention.Manager` 的 Run/Pass/停机模式（`cmd/sq/main.go` 的 ctx+WaitGroup+defer LIFO 注释）是 Task 4/7 调度器的直接范本。retention 不清理 `delay/`（那里的消息还未投递，不受 topic 保留时长约束）——这是边界，不是遗漏。

## 接口总表（跨 task 契约，实现前先读）

```go
// store（Task 1 产出）
const DelayPrefix = "delay/"
func DelayKey(dueMs int64, seq uint64) []byte            // delay/{dueMs:8B}{seq:8B}
func DelayScanUpperBound(nowMs int64) []byte             // 开区间上界，恰好含 dueMs<=nowMs 全部条目
func ParseDelayKey(k []byte) (dueMs int64, seq uint64, err error)
func DelayAllocKey() []byte                              // 全局 seq 计数器（8B，下一可用 seq）

// core（Task 2 产出）
type Message struct { ...; DeliverAtMs int64 `json:"deliver_at_ms,omitempty"` }

// produce（Task 3 产出）
func (p *Producer) AppendDelay(m *core.Message) (*core.Message, error)
// m.DeliverAtMs 必须 >0；到期时间已过则内部直通 AppendWith 立即投递

// delay（Task 4 产出，新包 internal/core/delay）
var scanInterval = 100 * time.Millisecond   // var：测试注入
var maxMovePerPass = 512                    // var：测试注入
func New(st *store.Store, pr *produce.Producer, logger *slog.Logger) *Scheduler
func (s *Scheduler) Run(ctx context.Context)   // 阻塞循环，ctx 取消即返回
func (s *Scheduler) Pass() (int, error)        // 单趟移动，返回移动条数

// rpc（Task 5 产出）
type testEnv struct { srv *Server; client pb.MessagingServiceClient; dl *deliver.Deliverer; blocked *atomic.Bool; st *store.Store }
```

---

### Task 1: store 层 delay key 编码

**Files:**
- Modify: `internal/store/keys.go`
- Test: `internal/store/keys_test.go`

**Interfaces:**
- Consumes: 既有 `PutU64`/`GetU64`、`PrefixUpperBound` 模式
- Produces: `DelayPrefix`、`DelayKey`、`DelayScanUpperBound`、`ParseDelayKey`、`DelayAllocKey`（签名见接口总表）

- [ ] **Step 1: 写失败测试**

```go
func TestDelayKeyRoundTrip(t *testing.T) {
	k := DelayKey(1700000000123, 42)
	due, seq, err := ParseDelayKey(k)
	if err != nil || due != 1700000000123 || seq != 42 {
		t.Fatalf("round trip: %d %d %v", due, seq, err)
	}
}

func TestDelayKeyOrdering(t *testing.T) {
	// 字节序 = (dueMs, seq) 字典序：先按到期时间，同一毫秒按 seq
	a := DelayKey(1000, 999)
	b := DelayKey(1001, 0)
	c := DelayKey(1001, 1)
	if !(bytes.Compare(a, b) < 0 && bytes.Compare(b, c) < 0) {
		t.Fatal("delay key 排序错误")
	}
}

func TestDelayScanUpperBoundIsInclusiveOfNow(t *testing.T) {
	// 上界必须恰好包含 dueMs==now 的全部 seq，且不包含 now+1 的任何条目
	up := DelayScanUpperBound(1000)
	atNowMaxSeq := DelayKey(1000, math.MaxUint64)
	atNextMs := DelayKey(1001, 0)
	if !(bytes.Compare(atNowMaxSeq, up) < 0) {
		t.Fatal("dueMs==now 的条目被上界排除")
	}
	if bytes.Compare(atNextMs, up) < 0 {
		t.Fatal("dueMs==now+1 的条目被上界纳入")
	}
}

func TestParseDelayKeyRejectsGarbage(t *testing.T) {
	for _, k := range [][]byte{[]byte("delay/short"), []byte("msg/x"), DelayKey(1, 1)[:10]} {
		if _, _, err := ParseDelayKey(k); err == nil {
			t.Fatalf("应拒绝非法 key: %q", k)
		}
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/store/ -run TestDelay -v`
Expected: FAIL（undefined: DelayKey 等）

- [ ] **Step 3: 实现**

在 keys.go 前缀常量区加 `delayPrefix = "delay/"` 与导出 `DelayPrefix = delayPrefix`（供 delay 包做扫描下界），常量 `delayAllocKey = "delayalloc"`，然后：

```go
// DelayKey 延时暂存条目：delay/{dueMs:8B}{seq:8B}，值为完整编码消息（spec §4）。
// dueMs 是墙钟 UnixMilli 恒为正，按 uint64 大端编码后字节序即数值序；
// seq 是全局分配的写入序号，仅用于同一毫秒内多条消息的 key 去重与稳定排序。
func DelayKey(dueMs int64, seq uint64) []byte {
	k := make([]byte, 0, len(delayPrefix)+16)
	k = append(k, delayPrefix...)
	k = append(k, PutU64(uint64(dueMs))...)
	k = append(k, PutU64(seq)...)
	return k
}

// DelayScanUpperBound 到期扫描 [DelayPrefix, 本上界) 的开区间上界：
// 用 dueMs+1 的最小 key，恰好把 dueMs<=nowMs 的全部条目（含 nowMs 毫秒内
// 任意 seq）纳入区间，且不含 nowMs+1 的任何条目。
func DelayScanUpperBound(nowMs int64) []byte {
	return DelayKey(nowMs+1, 0)
}

// ParseDelayKey 解析延时条目 key。前缀后必须恰好 16 字节定长二进制。
func ParseDelayKey(k []byte) (int64, uint64, error) {
	rest, ok := bytes.CutPrefix(k, []byte(delayPrefix))
	if !ok || len(rest) != 16 {
		return 0, 0, fmt.Errorf("非法 delay key: %q", k)
	}
	return int64(binary.BigEndian.Uint64(rest[:8])), binary.BigEndian.Uint64(rest[8:]), nil
}

// DelayAllocKey 延时 seq 全局计数器（值为下一可用 seq 的 8B 大端编码）。
func DelayAllocKey() []byte { return []byte(delayAllocKey) }
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/store/ -v`
Expected: 全部 PASS（含既有用例）

- [ ] **Step 5: 注释自检**

上面代码块中的 doc comment 必须完整落入；`delayAllocKey` 常量旁注释「全局单 key，与 alloc/{topic} 的按队列计数器不同：延时条目移入前不属于任何队列」。

- [ ] **Step 6: Commit**

```bash
git add internal/store/keys.go internal/store/keys_test.go
git commit -m "feat(store): delay 暂存区 key 编码与全局 seq 计数器 key"
```

---

### Task 2: core.Message 增加 DeliverAtMs

**Files:**
- Modify: `internal/core/types.go`
- Test: `internal/core/types_test.go`

**Interfaces:**
- Produces: `Message.DeliverAtMs int64`（json tag `deliver_at_ms,omitempty`），后续 task 全部依赖此字段名

- [ ] **Step 1: 写失败测试**

```go
func TestMessageDeliverAtMsRoundTripAndCompat(t *testing.T) {
	// 新字段往返
	m := &Message{ID: "x", Topic: "t", Body: []byte("b"), DeliverAtMs: 12345}
	raw, err := EncodeMessage(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMessage(raw)
	if err != nil || got.DeliverAtMs != 12345 {
		t.Fatalf("DeliverAtMs 往返失败: %+v %v", got, err)
	}
	// 旧数据兼容：M2 及以前落盘的 JSON 没有 deliver_at_ms 键，解码得零值
	old, err := DecodeMessage([]byte(`{"id":"y","topic":"t","body":"Yg=="}`))
	if err != nil || old.DeliverAtMs != 0 {
		t.Fatalf("旧数据兼容失败: %+v %v", old, err)
	}
	// 零值不产生新键：普通消息编码结果与升级前逐字节一致
	m2 := &Message{ID: "z", Topic: "t", Body: []byte("b")}
	raw2, _ := EncodeMessage(m2)
	if bytes.Contains(raw2, []byte("deliver_at_ms")) {
		t.Fatal("零值 DeliverAtMs 不应出现在 JSON 中")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/ -run TestMessageDeliverAtMs -v`
Expected: FAIL（字段不存在，编译错误）

- [ ] **Step 3: 实现**

在 `Message` 结构体 `StoreAtMs` 之后加：

```go
	// DeliverAtMs 延时消息的到期投递时间（UnixMilli）；0 = 普通消息。
	// 移入 msg/ 后仍保留：投递时协议层据此回填 MessageType_DELAY 与
	// DeliveryTimestamp，SDK 消费端才能读到自己当初设置的延时时间。
	DeliverAtMs int64 `json:"deliver_at_ms,omitempty"`
```

并把结构体上方那段 omitempty 兼容性注释的适用范围句子扩为「BodyEncoding/BodyDigest/BornHost/TraceContext/DeliverAtMs」。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/ -v`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add internal/core/types.go internal/core/types_test.go
git commit -m "feat(core): Message 增 DeliverAtMs（延时到期时间，旧数据零值兼容）"
```

---

### Task 3: produce.AppendDelay（延时暂存写入 + 持久化 seq 计数器）

**Files:**
- Modify: `internal/core/produce/produce.go`
- Test: `internal/core/produce/produce_test.go`

**Interfaces:**
- Consumes: Task 1 的 `DelayKey`/`DelayAllocKey`、Task 2 的 `DeliverAtMs`
- Produces: `func (p *Producer) AppendDelay(m *core.Message) (*core.Message, error)`；Producer 内部新增字段 `delayNext uint64`、`delayLoaded bool`（p.mu 保护）

- [ ] **Step 1: 写失败测试**

```go
// sendDelay 构造一条延时消息并 AppendDelay（测试辅助）
func sendDelay(t *testing.T, p *Producer, topic string, body string, dueMs int64) *core.Message {
	t.Helper()
	m, err := p.AppendDelay(&core.Message{Topic: topic, Body: []byte(body), DeliverAtMs: dueMs})
	if err != nil {
		t.Fatalf("AppendDelay: %v", err)
	}
	return m
}

func countPrefix(t *testing.T, st *store.Store, lower, upper []byte) int {
	t.Helper()
	n := 0
	if err := st.Scan(lower, upper, 0, func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return n
}

func TestAppendDelayWritesDelayEntryNotMsg(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	due := time.Now().Add(time.Hour).UnixMilli()
	m := sendDelay(t, p, "t", "later", due)
	if m.ID == "" {
		t.Fatal("应分配消息 ID")
	}
	dpfx := []byte(store.DelayPrefix)
	if n := countPrefix(t, st, dpfx, store.PrefixUpperBound(dpfx)); n != 1 {
		t.Fatalf("delay 条目数 = %d，期望 1", n)
	}
	mpfx := []byte("msg/")
	if n := countPrefix(t, st, mpfx, store.PrefixUpperBound(mpfx)); n != 0 {
		t.Fatalf("msg/ 应为空，实际 %d 条", n)
	}
}

func TestAppendDelayPastDueFallsThroughToImmediate(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	m := sendDelay(t, p, "t", "now", time.Now().Add(-time.Second).UnixMilli())
	// 已过期：直接普通写入，分配了队列与 offset，delay 区为空
	raw, ok, err := st.Get(store.MsgKey("t", m.QueueID, m.Offset))
	if err != nil || !ok {
		t.Fatalf("过期延时消息应立即入 msg/: %v", err)
	}
	got, _ := core.DecodeMessage(raw)
	if got.DeliverAtMs == 0 {
		t.Fatal("直通写入也要保留 DeliverAtMs（投递时回填协议字段用）")
	}
	dpfx := []byte(store.DelayPrefix)
	if n := countPrefix(t, st, dpfx, store.PrefixUpperBound(dpfx)); n != 0 {
		t.Fatal("过期延时消息不应写 delay 条目")
	}
}

func TestAppendDelayRejectsInvalid(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	if _, err := p.AppendDelay(&core.Message{Topic: "t", Body: nil, DeliverAtMs: time.Now().Add(time.Hour).UnixMilli()}); err == nil {
		t.Fatal("空 body 应拒绝")
	}
	if _, err := p.AppendDelay(&core.Message{Topic: "t", Body: []byte("x"), DeliverAtMs: 0}); err == nil {
		t.Fatal("DeliverAtMs<=0 是编程错误，应拒绝")
	}
}

func TestAppendDelaySeqPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	p, st := newTestProducer(t, dir)
	due := time.Now().Add(time.Hour).UnixMilli()
	sendDelay(t, p, "t", "a", due)
	sendDelay(t, p, "t", "b", due)
	st.Close()
	// 重开：seq 计数器从盘上恢复，不与已有条目撞 key
	p2, st2 := newTestProducer(t, dir)
	defer st2.Close()
	sendDelay(t, p2, "t", "c", due)
	dpfx := []byte(store.DelayPrefix)
	if n := countPrefix(t, st2, dpfx, store.PrefixUpperBound(dpfx)); n != 3 {
		t.Fatalf("重启后 delay 条目数 = %d，期望 3（seq 撞 key 会覆盖变 2）", n)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/produce/ -run TestAppendDelay -v`
Expected: FAIL（AppendDelay 未定义）

- [ ] **Step 3: 实现**

Producer 结构体 `next` 字段旁新增：

```go
	delayNext   uint64 // 下一延时 seq（内存缓存，与 delayalloc key 同步；delayLoaded 后有效）
	delayLoaded bool
```

方法（放在 AppendWith 之后）：

```go
// AppendDelay 将延时消息写入 delay/ 暂存区（spec §5 流程 3 前半）。
//
// 参数：m.DeliverAtMs 必须 >0（协议层已保证 DELAY 消息带 delivery_timestamp，
// <=0 属编程错误直接报错）。到期时间已过（<=now）时直通 AppendWith 立即投递：
// 语义上"到期的延时消息"就是普通消息，绕道暂存区再被调度器搬回来只是
// 多一次读写放大，结果完全相同。
//
// 返回：写入后的消息。注意暂存态消息没有队列与 offset（m.QueueID/Offset
// 保持零值）——它们在到期移入 msg/ 时才由正常写入路径分配。
//
// 原子性：delay 条目与 seq 计数器同一 Batch 提交，理由与 Append 的
// offset 计数器完全相同（崩溃后计数器与已写条目严格一致，seq 绝不复用）。
func (p *Producer) AppendDelay(m *core.Message) (*core.Message, error) {
	if m.DeliverAtMs <= 0 {
		return nil, fmt.Errorf("AppendDelay 要求 DeliverAtMs>0，得到 %d", m.DeliverAtMs)
	}
	if len(m.Body) == 0 || len(m.Body) > MaxBodySize {
		return nil, fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), MaxBodySize)
	}
	// 写入时就确认 topic 存在（autoCreate 时创建）：错误要在发送端立刻暴露，
	// 不能等到几小时后到期移入时才发现 topic 不存在、消息无处可去。
	if _, err := p.mt.EnsureTopic(m.Topic); err != nil {
		return nil, err
	}
	if m.DeliverAtMs <= time.Now().UnixMilli() {
		return p.AppendWith(m, nil)
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
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	seq, err := p.nextDelaySeqLocked()
	if err != nil {
		return nil, err
	}
	b := p.st.NewBatch()
	b.Set(store.DelayKey(m.DeliverAtMs, seq), raw, nil)
	b.Set(store.DelayAllocKey(), store.PutU64(seq+1), nil)
	if err := p.st.Apply(b); err != nil {
		return nil, fmt.Errorf("写入延时消息 %s (topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
	}
	// 与 Append 同理：Apply 成功后才推进内存缓存，失败的写不能烧掉 seq
	p.delayNext = seq + 1
	p.delayLoaded = true
	p.logger.Debug("延时消息已暂存", "topic", m.Topic, "msg_id", m.ID,
		"due_ms", m.DeliverAtMs, "seq", seq)
	return m, nil
}

// nextDelaySeqLocked 取下一延时 seq。缓存未命中时读盘上 delayalloc 计数器，
// 崩溃/重启后 O(1) 恢复。调用方必须持有 p.mu。
func (p *Producer) nextDelaySeqLocked() (uint64, error) {
	if p.delayLoaded {
		return p.delayNext, nil
	}
	v, ok, err := p.st.Get(store.DelayAllocKey())
	if err != nil {
		return 0, fmt.Errorf("读取延时 seq 计数器: %w", err)
	}
	if !ok {
		return 0, nil
	}
	return store.GetU64(v), nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/produce/ -v`
Expected: 全部 PASS

- [ ] **Step 5: 日志与注释自检（instrumenting-code）**

- 成功路径 Debug「延时消息已暂存」带 topic/msg_id/due_ms/seq ✅（上面代码已含，确认落入）
- 全部错误分支 `fmt.Errorf` 带 msg_id/topic/due 上下文 ✅
- `AppendDelay` doc comment 说明参数约束、直通语义、原子性理由 ✅
- 包头注释「职责」列表补一行：「AppendDelay：延时消息写 delay/ 暂存区，seq 计数器同批原子提交」；「边界」的「不判定延时/事务」句子改为「延时判定入口在此（AppendDelay），到期搬运是 delay 包的事；事务（M6）仍在 Append 之前分流」

- [ ] **Step 6: Commit**

```bash
git add internal/core/produce/produce.go internal/core/produce/produce_test.go
git commit -m "feat(produce): AppendDelay 延时暂存写入，seq 计数器同批原子提交"
```

---

### Task 4: delay 调度器（到期扫描 + 原子移入）

**Files:**
- Create: `internal/core/delay/delay.go`
- Test: `internal/core/delay/delay_test.go`

**Interfaces:**
- Consumes: Task 1 的 `DelayPrefix`/`DelayScanUpperBound`/`ParseDelayKey`、`produce.AppendWith`
- Produces: `delay.New(st, pr, logger) *Scheduler`、`(s *Scheduler) Run(ctx)`、`(s *Scheduler) Pass() (int, error)`、测试注入变量 `scanInterval`/`maxMovePerPass`

- [ ] **Step 1: 写失败测试**

```go
// delay 调度器测试。到期条目无法经 AppendDelay 制造（它对已过期时间直通立即
// 投递），因此直接向 store 写 delay 条目造数据——与 retention_test 注入旧
// StoreAtMs 的手法同理。
package delay

import (
	"bytes"
	"context"
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
	st *store.Store
	pr *produce.Producer
	dl *deliver.Deliverer
	sc *Scheduler
}

func newFixture(t *testing.T) *fixture {
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
	return &fixture{st: st, pr: pr, dl: dl, sc: New(st, pr, slog.Default())}
}

// putDelay 直接向暂存区写一条到期条目（绕过 AppendDelay 的直通逻辑）
func (f *fixture) putDelay(t *testing.T, seq uint64, dueMs int64, m *core.Message) {
	t.Helper()
	m.DeliverAtMs = dueMs
	raw, err := core.EncodeMessage(m)
	if err != nil {
		t.Fatal(err)
	}
	b := f.st.NewBatch()
	b.Set(store.DelayKey(dueMs, seq), raw, nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) delayCount(t *testing.T) int {
	t.Helper()
	n := 0
	pfx := []byte(store.DelayPrefix)
	if err := f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPassMovesDueAndPreservesMessage(t *testing.T) {
	f := newFixture(t)
	past := time.Now().Add(-time.Second).UnixMilli()
	f.putDelay(t, 0, past, &core.Message{ID: "m1", Topic: "t", Body: []byte("hello"),
		Keys: []string{"k1"}, Tag: "tg", BornAtMs: 123})
	moved, err := f.sc.Pass()
	if err != nil || moved != 1 {
		t.Fatalf("Pass: moved=%d err=%v", moved, err)
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatalf("delay 条目未删除: %d", n)
	}
	// 经正常投递链路可消费，DeliverAtMs/Tag/Keys 完整保留
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("到期消息应可投递: %d %v", len(msgs), err)
	}
	got := msgs[0]
	if got.ID != "m1" || string(got.Body) != "hello" || got.DeliverAtMs != past ||
		got.Tag != "tg" || got.BornAtMs != 123 {
		t.Fatalf("消息字段丢失: %+v", got)
	}
	// Keys 索引在移入时由 AppendWith 顺带写入
	kpfx := store.KeyIdxKeyPrefix("t", "k1")
	found := 0
	f.st.Scan(kpfx, store.PrefixUpperBound(kpfx), 0, func(k, v []byte) (bool, error) { found++; return true, nil })
	if found != 1 {
		t.Fatalf("keyidx 未写入: %d", found)
	}
}

func TestPassLeavesNotDueEntries(t *testing.T) {
	f := newFixture(t)
	f.putDelay(t, 0, time.Now().Add(time.Hour).UnixMilli(), &core.Message{ID: "m1", Topic: "t", Body: []byte("x")})
	moved, err := f.sc.Pass()
	if err != nil || moved != 0 {
		t.Fatalf("未到期不应移动: %d %v", moved, err)
	}
	if n := f.delayCount(t); n != 1 {
		t.Fatalf("未到期条目不应消失: %d", n)
	}
}

func TestPassDeletesCorruptEntryInsteadOfWedging(t *testing.T) {
	f := newFixture(t)
	b := f.st.NewBatch()
	b.Set(store.DelayKey(time.Now().Add(-time.Second).UnixMilli(), 0), []byte("not-json"), nil)
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}
	moved, err := f.sc.Pass()
	if err != nil || moved != 0 {
		t.Fatalf("坏条目不算移动也不算错: %d %v", moved, err)
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatal("坏条目应被删除（否则每 100ms 重扫一次，永久日志洪水）")
	}
}

func TestPassRespectsBudgetAndDrains(t *testing.T) {
	oldBudget := maxMovePerPass
	maxMovePerPass = 2
	defer func() { maxMovePerPass = oldBudget }()
	f := newFixture(t)
	past := time.Now().Add(-time.Second).UnixMilli()
	for i := uint64(0); i < 5; i++ {
		f.putDelay(t, i, past, &core.Message{ID: string(rune('a'+i)), Topic: "t", Body: []byte("x")})
	}
	// 单趟受预算限制
	moved, err := f.sc.Pass()
	if err != nil || moved != 2 {
		t.Fatalf("首趟应恰好移动预算数: %d %v", moved, err)
	}
	// 连续 Pass 可排空
	total := moved
	for total < 5 {
		n, err := f.sc.Pass()
		if err != nil || n == 0 {
			t.Fatalf("排空中断: n=%d total=%d err=%v", n, total, err)
		}
		total += n
	}
	if n := f.delayCount(t); n != 0 {
		t.Fatalf("排空后仍剩 %d 条", n)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.sc.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未在 ctx 取消后退出")
	}
}

var _ = bytes.Compare // 若最终未用 bytes 则删除此行与 import
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/delay/ -v`
Expected: FAIL（包不存在/编译错误）

- [ ] **Step 3: 实现 delay.go**

```go
// Package delay 实现延时消息调度：扫描 delay/ 暂存区头部，到期条目移入
// 目标 topic 的正常队列（spec §5 流程 3 后半）。
//
// 职责：
//   - 周期（scanInterval）扫描 [DelayPrefix, DelayScanUpperBound(now))，
//     到期条目经 produce.AppendWith 写入 msg/ 并同批原子删除 delay 条目
//   - 单趟预算 maxMovePerPass，满额立即续趟排空积压（不等下个 tick）
//
// 边界：
//   - 不感知协议；不管投递/重试/DLQ——移入 msg/ 后就是普通消息，一切
//     消费语义由 deliver 负责
//   - 崩溃恢复零代码：暂存区在 Pebble，重启后从头扫描即恢复；移入是
//     单批原子操作，不存在"已入 msg/ 但 delay 条目残留"的中间态
//   - 时钟回拨（NTP 校时）只会让扫描上界暂时变小、到期条目晚一点被搬运
//     ——仅延迟投递，不丢失不提前（spec §7 时钟策略）
package delay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// scanInterval 扫描间隔。100ms 即延时精度的上界（spec §5：调度器每 100ms
// 扫头部），对"秒级延时"的承诺足够。var 而非 const：测试需注入小值。
var scanInterval = 100 * time.Millisecond

// maxMovePerPass 单趟最多搬运条数：单趟工作量必须有上界，否则大量同时
// 到期的消息会让一趟扫描长时间占用，期间新写入的 fsync 全部排在后面。
// var 而非 const：测试注入小值验证预算与排空行为。
var maxMovePerPass = 512

// Scheduler 延时消息调度器。单 goroutine 运行（Run），Pass 可单独调用（测试用）。
type Scheduler struct {
	st     *store.Store
	pr     *produce.Producer
	logger *slog.Logger
}

// New 构造调度器。
func New(st *store.Store, pr *produce.Producer, logger *slog.Logger) *Scheduler {
	return &Scheduler{st: st, pr: pr, logger: logger.With("mod", "delay")}
}

// Run 阻塞运行调度循环：启动即跑一趟，此后每 scanInterval 一趟；单趟满额
// （moved==maxMovePerPass）说明还有积压，立即续跑不等 tick。ctx 取消即返回。
// 调用方（main）负责放入独立 goroutine 并在停机时先取消再关 store。
func (s *Scheduler) Run(ctx context.Context) {
	s.logger.Info("delay 调度器启动", "interval", scanInterval.String())
	t := time.NewTicker(scanInterval)
	defer t.Stop()
	for {
		moved, err := s.Pass()
		if err != nil {
			// 单趟失败只记日志不退出：store 瞬时故障恢复后下一趟自然重试，
			// 头部条目还在原地（移入失败不会删除条目）
			s.logger.Error("delay 调度趟失败", "err", err)
		} else if moved > 0 {
			s.logger.Info("延时消息到期移入", "moved", moved)
		}
		if err == nil && moved == maxMovePerPass {
			continue // 满额=可能还有积压，立即续趟
		}
		select {
		case <-ctx.Done():
			s.logger.Info("delay 调度器退出")
			return
		case <-t.C:
		}
	}
}

// Pass 执行一趟到期搬运，返回移入 msg/ 的条数（被清理的坏条目不计入）。
func (s *Scheduler) Pass() (int, error) {
	now := time.Now().UnixMilli()
	// 先收集后搬运：Scan 回调的 k/v 仅回调期间有效，且回调里不能再开写
	// 事务（迭代器与写入交错），必须拷贝出来
	type due struct {
		key []byte
		raw []byte
	}
	var dues []due
	lower := []byte(store.DelayPrefix)
	err := s.st.Scan(lower, store.DelayScanUpperBound(now), maxMovePerPass, func(k, v []byte) (bool, error) {
		dues = append(dues, due{key: append([]byte(nil), k...), raw: append([]byte(nil), v...)})
		return true, nil
	})
	if err != nil {
		return 0, fmt.Errorf("扫描 delay 暂存区: %w", err)
	}
	moved := 0
	for _, d := range dues {
		m, err := core.DecodeMessage(d.raw)
		if err != nil {
			// 坏条目永远无法投递，留着只会每 scanInterval 重扫一次、重报一次
			// ——与 deliver 清理孤儿 inflight 同理，删除止损并 Error 留痕
			s.logger.Error("delay 条目解码失败，删除坏条目", "key", fmt.Sprintf("%q", d.key), "err", err)
			b := s.st.NewBatch()
			b.Delete(d.key, nil)
			if err := s.st.Apply(b); err != nil {
				return moved, fmt.Errorf("删除坏 delay 条目: %w", err)
			}
			continue
		}
		key := d.key
		// 写 msg/（正常分配队列与 offset、写 keyidx、唤醒长轮询）+ 删 delay
		// 条目，同一 Batch 原子提交：崩溃窗口内要么都发生要么都不发生，
		// 不存在丢失或重复投递
		if _, err := s.pr.AppendWith(m, func(b *pebble.Batch) { b.Delete(key, nil) }); err != nil {
			// 失败即中断本趟：条目未删除，下一趟从头重扫自然重试
			return moved, fmt.Errorf("延时消息移入 (msg_id=%s topic=%s due=%d): %w", m.ID, m.Topic, m.DeliverAtMs, err)
		}
		moved++
		s.logger.Debug("延时消息已移入队列", "msg_id", m.ID, "topic", m.Topic,
			"queue", m.QueueID, "offset", m.Offset, "due_ms", m.DeliverAtMs)
	}
	return moved, nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/delay/ -v`
Expected: 全部 PASS。测试文件里如未用到 `bytes` 删掉占位行与 import。

- [ ] **Step 5: 日志与注释自检（instrumenting-code）**

- 启动/退出 Info、每趟移入聚合 Info、单条 Debug、坏条目与趟失败 Error 均带上下文 ✅
- 文件头职责/边界注释含时钟回拨说明 ✅
- 「先收集后搬运」「坏条目删除止损」「满额续趟」三处 why 注释 ✅

- [ ] **Step 6: Commit**

```bash
git add internal/core/delay/
git commit -m "feat(delay): 到期扫描调度器，AppendWith 同批原子移入 msg/"
```

---

### Task 5: rpc 写方向——接受 DELAY 消息并通告类型

**Files:**
- Modify: `internal/rpc/send.go`（toCoreMessage、SendMessage 路由）
- Modify: `internal/rpc/server.go`（messageQueues 的 AcceptMessageTypes）
- Modify: `internal/rpc/settings.go`（仅注释更新）
- Modify: `internal/rpc/server_test.go`（testEnv 增 st 字段）
- Test: `internal/rpc/send_test.go`

**Interfaces:**
- Consumes: Task 3 的 `AppendDelay`、Task 2 的 `DeliverAtMs`
- Produces: `testEnv.st *store.Store`（Task 6 的读方向测试也用它）

- [ ] **Step 1: testEnv 增 st 字段**

`server_test.go` 的 testEnv 结构体加 `st *store.Store`（字段注释：「需要绕开协议层直接查盘上状态时用——延时用例查 delay/ 前缀」），`newTestEnv` 返回处改为 `return testEnv{srv: srv, client: pb.NewMessagingServiceClient(conn), dl: dl, blocked: blocked, st: st}`。

- [ ] **Step 2: 写失败测试**

```go
func TestSendDelayMessageGoesToDelayAreaNotDeliverable(t *testing.T) {
	env := newTestEnv(t, true)
	due := time.Now().Add(time.Hour)
	resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "dly"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_DELAY,
				DeliveryTimestamp: timestamppb.New(due),
			},
			Body: []byte("later"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("延时发送应成功: %v %v", resp.GetStatus(), err)
	}
	// 未到期：正常消费链路取不到
	msgs, err := env.dl.Receive(context.Background(), "g", "dly", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("未到期不应可消费: %d %v", len(msgs), err)
	}
	// 盘上 delay/ 恰有一条
	pfx := []byte(store.DelayPrefix)
	n := 0
	env.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) { n++; return true, nil })
	if n != 1 {
		t.Fatalf("delay 条目数 = %d，期望 1", n)
	}
}

func TestSendDelayMissingTimestampRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "dly"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_DELAY},
			Body:             []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_ILLEGAL_DELIVERY_TIME {
		t.Fatalf("期望 ILLEGAL_DELIVERY_TIME，得到 %v %v", resp.GetStatus(), err)
	}
}

func TestSendNormalWithDeliveryTimestampRejected(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "dly"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_NORMAL,
				DeliveryTimestamp: timestamppb.New(time.Now().Add(time.Hour)),
			},
			Body: []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE {
		t.Fatalf("期望 MESSAGE_PROPERTY_CONFLICT_WITH_TYPE，得到 %v %v", resp.GetStatus(), err)
	}
}

func TestQueryRouteAdvertisesDelayType(t *testing.T) {
	// 守护 SDK 客户端侧校验：ValidateMessageType=true 时 SDK 发送前检查路由的
	// AcceptMessageTypes，缺 DELAY 则延时消息在客户端本地就被拒（producer.go:191）
	c := newTestClient(t)
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{Topic: &pb.Resource{Name: "dly"}})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("QueryRoute: %v %v", resp.GetStatus(), err)
	}
	for _, mq := range resp.GetMessageQueues() {
		hasDelay := false
		for _, mt := range mq.GetAcceptMessageTypes() {
			if mt == pb.MessageType_DELAY {
				hasDelay = true
			}
		}
		if !hasDelay {
			t.Fatalf("队列 %d 未通告 DELAY 类型", mq.GetId())
		}
	}
}
```

（send_test.go 需补 import：`timestamppb`、`store`。）

- [ ] **Step 3: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestSendDelay|TestSendNormalWithDelivery|TestQueryRouteAdvertises' -v`
Expected: FAIL（DELAY 被拒 / AcceptMessageTypes 无 DELAY / testEnv 无 st 编译错）

- [ ] **Step 4: 实现**

`send.go` toCoreMessage 的 switch 改为：

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
	case pb.MessageType_DELAY:
		if sp.GetDeliveryTimestamp() == nil {
			return nil, errStatus(pb.Code_ILLEGAL_DELIVERY_TIME, "DELAY 消息缺少 delivery_timestamp")
		}
	case pb.MessageType_FIFO:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "顺序消息将在 M4 支持")
	case pb.MessageType_TRANSACTION:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "事务消息将在 M6 支持")
	default:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
			fmt.Sprintf("未知消息类型 %v", sp.GetMessageType()))
	}
```

返回的 `core.Message` 字面量加一个字段（放 BornAtMs 附近）：

```go
		// DELAY 消息才有值；任意未来时间都接受（spec：任意秒级延时，不设上限），
		// 已过期的时间戳由 AppendDelay 直通立即投递
		DeliverAtMs: delayAt,
```

其中 switch 前算好 `var delayAt int64`，DELAY 分支里 `delayAt = sp.GetDeliveryTimestamp().AsTime().UnixMilli()`。

`SendMessage` 第二遍循环里的写入改为按类型路由：

```go
		var stored *core.Message
		var err error
		if m.DeliverAtMs > 0 {
			// 延时消息进暂存区（未分配 offset，entry 里 Offset 回 0——SDK 的
			// SendReceipt 只消费 MessageId，offset 字段对延时场景无意义）
			stored, err = s.pr.AppendDelay(m)
		} else {
			stored, err = s.pr.Append(m)
		}
```

`server.go` messageQueues：

```go
			AcceptMessageTypes: []pb.MessageType{
				pb.MessageType_NORMAL,
				// M3 起接受延时消息。不能漏：SDK 开着 ValidateMessageType，
				// 发送前用本列表在客户端本地校验，缺了 DELAY 则延时消息
				// 根本发不出来（M4/M6 时继续追加 FIFO/TRANSACTION）
				pb.MessageType_DELAY,
			},
```

`settings.go` 92-94 行注释更新为「sq 的 QueryRoute 通告 AcceptMessageTypes=[NORMAL, DELAY]（M3 起），让客户端在本地就拒掉顺序/事务消息…」。`send.go` 文件头「边界」行改为「NORMAL 与 DELAY（M3）；M4 FIFO / M6 事务在各自里程碑打开」。

- [ ] **Step 5: 跑测试确认通过**

Run: `go test ./internal/rpc/ -v`
Expected: 全部 PASS（含既有用例——特别注意 `TestSendReceivePreservesPassthroughSystemProperties` 若其构造的 NORMAL 消息未带 DeliveryTimestamp 则不受影响）

- [ ] **Step 6: 日志与注释自检（instrumenting-code）**

- 拒绝分支复用 SendMessage 既有的 `s.logger.Warn("SendMessage 拒绝", ...)` 汇聚点（toCoreMessage 返回 status 后统一打）✅ 确认新分支无静默路径
- 上述代码块中的 why 注释（NORMAL+timestamp 拒绝理由、AcceptMessageTypes 不能漏的理由、offset=0 说明）全部落入

- [ ] **Step 7: Commit**

```bash
git add internal/rpc/send.go internal/rpc/server.go internal/rpc/settings.go internal/rpc/server_test.go internal/rpc/send_test.go
git commit -m "feat(rpc): 接受 DELAY 消息入延时暂存区，路由通告 DELAY 类型"
```

---

### Task 6: rpc 读方向——投递时回填 DELAY 类型与 DeliveryTimestamp

**Files:**
- Modify: `internal/rpc/receive.go`（toPBMessage）
- Test: `internal/rpc/receive_test.go`

**Interfaces:**
- Consumes: Task 2 的 `DeliverAtMs`、Task 5 的 testEnv.st
- Produces: 无新接口（协议行为变更）

- [ ] **Step 1: 写失败测试**

```go
func TestToPBMessageEchoesDelayTypeAndTimestamp(t *testing.T) {
	env := newTestEnv(t, true)
	due := time.Now().Add(-time.Second).UnixMilli() // 已过期：发送即直通立即投递
	m := &core.Message{ID: "d1", Topic: "t", Body: []byte("x"), DeliverAtMs: due, DeliveryAttempt: 1}
	pm := env.srv.toPBMessage(m, "g", time.Minute)
	if pm.GetSystemProperties().GetMessageType() != pb.MessageType_DELAY {
		t.Fatalf("延时消息投递类型应为 DELAY，得到 %v", pm.GetSystemProperties().GetMessageType())
	}
	got := pm.GetSystemProperties().GetDeliveryTimestamp()
	if got == nil || got.AsTime().UnixMilli() != due {
		t.Fatalf("DeliveryTimestamp 未回填: %v", got)
	}
	// 普通消息不受影响
	n := &core.Message{ID: "n1", Topic: "t", Body: []byte("y"), DeliveryAttempt: 1}
	pn := env.srv.toPBMessage(n, "g", time.Minute)
	if pn.GetSystemProperties().GetMessageType() != pb.MessageType_NORMAL ||
		pn.GetSystemProperties().GetDeliveryTimestamp() != nil {
		t.Fatal("普通消息不应带 DELAY 类型或 DeliveryTimestamp")
	}
}

// 全链路：过期时间戳的 DELAY 消息发送后直通立即投递，消费端读回自己设置的时间
func TestSendPastDueDelayDeliveredImmediatelyWithTimestampEcho(t *testing.T) {
	env := newTestEnv(t, true)
	due := time.Now().Add(-time.Second)
	resp, err := env.client.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "dly2"},
			SystemProperties: &pb.SystemProperties{
				MessageType:       pb.MessageType_DELAY,
				DeliveryTimestamp: timestamppb.New(due),
			},
			Body: []byte("imm"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("发送: %v %v", resp.GetStatus(), err)
	}
	pm := receiveOne(t, env.client, "g", "dly2")
	if pm.GetSystemProperties().GetMessageType() != pb.MessageType_DELAY {
		t.Fatal("投递类型应为 DELAY")
	}
	if ts := pm.GetSystemProperties().GetDeliveryTimestamp(); ts == nil || ts.AsTime().UnixMilli() != due.UnixMilli() {
		t.Fatalf("DeliveryTimestamp 回读不符: %v", ts)
	}
}
```

（`receiveOne` 是 receive_test.go 既有辅助；若其签名不同，按现有形态改造调用。）

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestToPBMessageEchoesDelay|TestSendPastDueDelay' -v`
Expected: FAIL（MessageType 恒为 NORMAL）

- [ ] **Step 3: 实现**

toPBMessage 中，构造 `sp` 前加：

```go
	// 延时消息投递时如实回填类型与到期时间：SDK 的 MessageView 会把
	// DeliveryTimestamp 暴露给应用（message.go fromProtobuf），少了它，
	// 消费端就读不回自己发送时设置的延时时间
	mtype := pb.MessageType_NORMAL
	if m.DeliverAtMs > 0 {
		mtype = pb.MessageType_DELAY
	}
```

`sp` 字面量里 `MessageType: pb.MessageType_NORMAL` 改为 `MessageType: mtype`；字面量之后（Tag/MessageGroup 的可选字段回填区）加：

```go
	if m.DeliverAtMs > 0 {
		sp.DeliveryTimestamp = timestamppb.New(time.UnixMilli(m.DeliverAtMs))
	}
```

toPBMessage 的 doc comment「本函数必须回填 toCoreMessage 收下的每一个透传字段」一段后追加一句：「DeliverAtMs 也在此回填（类型 + DeliveryTimestamp 两个字段）」。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/rpc/ -v`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add internal/rpc/receive.go internal/rpc/receive_test.go
git commit -m "feat(rpc): 延时消息投递回填 DELAY 类型与 DeliveryTimestamp"
```

---

### Task 7: main 装配 delay 调度器

**Files:**
- Modify: `cmd/sq/main.go`

**Interfaces:**
- Consumes: Task 4 的 `delay.New`/`Run`
- Produces: 进程内后台调度 goroutine（与 retention 同款生命周期）

- [ ] **Step 1: 实现**

import 增 `"github.com/xushixin/sq/internal/core/delay"`。在 retention 装配块之后、`net.Listen` 之前加：

```go
	// delay 调度器：到期延时消息移入正常队列。停机顺序与 retention 同理——
	// defer LIFO 保证先取消并等待调度 goroutine 退出，再轮到 st.Close 的 defer，
	// 不会在 store 关闭后提交搬运批次。
	dlyCtx, dlyCancel := context.WithCancel(context.Background())
	var dlyWG sync.WaitGroup
	ds := delay.New(st, pr, logger)
	dlyWG.Add(1)
	go func() { defer dlyWG.Done(); ds.Run(dlyCtx) }()
	defer func() { dlyCancel(); dlyWG.Wait() }()
```

- [ ] **Step 2: 构建与全量单测**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: 全绿

- [ ] **Step 3: 注释自检**

上面代码块的停机顺序注释必须落入（这是 main.go 里第二处同样的模式，注释指明「与 retention 同理」即可，不整段重复）。

- [ ] **Step 4: Commit**

```bash
git add cmd/sq/main.go
git commit -m "feat: main 装配 delay 调度器，停机先停调度再关 store"
```

---

### Task 8: e2e（官方 SDK 延时投递 + 重启恢复）与 README

**Files:**
- Create: `test/e2e/sdk_delay_test.go`
- Modify: `README.md`（功能列表加延时消息）

**Interfaces:**
- Consumes: e2e 既有 `startBroker(t)`、`writeBrokerConfig`/`launchBroker`/`brokerHandle.stop`（重启范式见 sdk_recovery_test.go）；SDK `msg.SetDelayTimestamp(time.Time)`、`mv.GetDeliveryTimestamp()`
- Produces: M3 出口标准的两条验收用例

- [ ] **Step 1: 写 e2e 用例**

```go
//go:build e2e

// 官方 Go SDK 延时消息 e2e：验证 M3 出口标准「任意秒级延时，重启不丢」。
//
// 职责：
//   - 延时投递：到期前收不到、到期后收到、DeliveryTimestamp 回读一致
//   - 重启恢复：延时消息暂存期间重启 broker，到期后仍被投递
//
// 边界：
//   - 不验证海量到期吞吐（性能基线属 spec §10 长稳测试）
//   - 延时精度按调度间隔 100ms + 长轮询节奏放宽断言，不做毫秒级卡点
package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// newDelayConsumer 构造订阅单 topic 的 SimpleConsumer（本文件专用辅助）。
func newDelayConsumer(t *testing.T, endpoint, group, topic string) rmq.SimpleConsumer {
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

// TestOfficialGoSDKDelayDelivery 延时 6s 的消息：前 3s 收不到，到期后收到，
// 且 MessageView 能读回发送时设置的 DeliveryTimestamp。
func TestOfficialGoSDKDelayDelivery(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-delay"
		group = "e2e-delay-g"
	)
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

	due := time.Now().Add(6 * time.Second)
	msg := &rmq.Message{Topic: topic, Body: []byte("delayed")}
	msg.SetDelayTimestamp(due)
	if _, err := producer.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	consumer := newDelayConsumer(t, endpoint, group, topic)

	// 到期前 3s 窗口内不应收到（留 3s 余量吸收轮询节奏，不贴着 due 卡点）
	notBefore := due.Add(-3 * time.Second)
	for time.Now().Before(notBefore) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			t.Fatalf("到期前收到消息: body=%s（距 due 还有 %v）", mv.GetBody(), time.Until(due))
		}
	}
	// 到期后 60s 内必须收到
	deadline := due.Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != "delayed" {
				t.Fatalf("消息体不符: %s", mv.GetBody())
			}
			if time.Now().Before(due) {
				t.Fatalf("提前投递: 距 due 还有 %v", time.Until(due))
			}
			ts := mv.GetDeliveryTimestamp()
			if ts == nil || ts.UnixMilli() != due.UnixMilli() {
				t.Fatalf("DeliveryTimestamp 回读不符: %v（期望 %v）", ts, due)
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			return
		}
	}
	t.Fatal("到期后 60s 内未收到延时消息")
}

// TestOfficialGoSDKDelayRestartRecovery 延时消息暂存期间重启 broker（M3 出口
// 标准「重启不丢」）：第一代进程收下延时 8s 的消息后立即停机，同一数据目录
// 拉起第二代，到期后消息仍被投递且恰好一次到达（首投 attempt 语义由
// broker 侧单测覆盖，这里只断言到达与内容）。
func TestOfficialGoSDKDelayRestartRecovery(t *testing.T) {
	cfgPath, endpoint := writeBrokerConfig(t)
	logPath := brokerLogPath(cfgPath)
	const (
		topic = "e2e-delay-restart"
		group = "e2e-delay-restart-g"
		body  = "survive-restart"
	)

	// 第一代：发送延时消息后停机
	h1 := launchBroker(t, cfgPath, endpoint, logPath)
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
	due := time.Now().Add(8 * time.Second)
	msg := &rmq.Message{Topic: topic, Body: []byte(body)}
	msg.SetDelayTimestamp(due)
	if _, err := producer.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}
	producer.GracefulStop()
	h1.stop(t)

	// 第二代：同一数据目录重启，到期后消费到
	h2 := launchBroker(t, cfgPath, endpoint, logPath)
	t.Cleanup(func() { h2.stop(t) })
	consumer := newDelayConsumer(t, endpoint, group, topic)
	deadline := due.Add(90 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != body {
				t.Fatalf("消息体不符: %s", mv.GetBody())
			}
			if time.Now().Before(due) {
				t.Fatalf("提前投递: 距 due 还有 %v", time.Until(due))
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			return
		}
	}
	t.Fatal("重启后到期消息未到达——延时消息在重启中丢失")
}

var _ = fmt.Sprintf // 若最终未用 fmt 则删除此行与 import
```

注意两点适配：① `launchBroker`/`brokerHandle` 的确切签名以 sdk_recovery_test.go 现状为准（若日志路径参数形态不同，照现有重启用例抄）；② 若现仓库没有 `brokerLogPath` 辅助，按 `filepath.Join(filepath.Dir(cfgPath), "broker.log")` 内联，与 `startBroker` 内部一致。

- [ ] **Step 2: 跑 e2e**

Run: `go test -tags e2e -count=1 -run 'TestOfficialGoSDKDelay' ./test/e2e/ -v`
Expected: 两条用例 PASS（合计约 1~2 分钟）

- [ ] **Step 3: 全量回归**

Run: `go build ./... && go vet ./... && gofmt -l . && go test ./... && go test -tags e2e -count=1 ./test/e2e/...`
Expected: 全绿、gofmt 无输出

- [ ] **Step 4: README 更新**

「功能」清单加一行：`- 延时消息：任意秒级延时（deliveryTimestamp），重启不丢，精度 ~100ms 调度间隔`；里程碑表（若有）把 M3 标记为已完成。

- [ ] **Step 5: Commit**

```bash
git add test/e2e/sdk_delay_test.go README.md
git commit -m "test(e2e): 官方 SDK 延时投递与重启恢复；docs: README M3 功能"
```

---

## Self-Review

**Spec 覆盖（§5 流程 3 + §11 M3 出口标准）：**
- 「带 deliveryTimestamp → 写 delay/ 而非 msg/」→ Task 3/5
- 「调度器每 100ms 扫 delay/ 头部」→ Task 4（scanInterval=100ms）
- 「到期消息移入目标 topic msg/（正常分配 offset）并删 delay 条目，同一 WriteBatch 原子完成」→ Task 4（AppendWith + extra Delete）
- 「任意秒级延时」→ 不设延时上限（Task 5 注释明示）；过期时间戳直通立即投递（Task 3）
- 「重启不丢」→ Task 8 第二条 e2e；崩溃恢复零代码依据（spec §7）写入 delay 包头注释
- 时钟回拨仅延迟不提前不丢（spec §7）→ Task 4 包头注释
- SDK 全链路验收（spec §6 协议验收标准）→ Task 8

**已知取舍（不是遗漏）：**
- retention 不清理 delay/：暂存消息未投递，不受 topic 保留时长约束（基线关键形态 6）
- moveToDLQ 复制死信时不带 DeliverAtMs：死信不再延时，属预期
- 延时消息的磁盘水位拒写：SendMessage 入口统一拦截（M2 已有），delay 路径无需单独处理

**占位符扫描：** 全文无 TBD/TODO/「适当处理」；每个代码步骤均给出完整代码；后续 task 引用的 `DelayKey`/`AppendDelay`/`testEnv.st` 等均在前置 task 定义。

**类型一致性：** `DeliverAtMs int64`（UnixMilli）贯穿 core/produce/delay/rpc；`DelayKey(dueMs int64, seq uint64)` 与 `ParseDelayKey` 返回类型对称；`Scheduler.Pass() (int, error)` 与 Run 的调用一致。
