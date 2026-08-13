# AutoRenew 自动续租不可见期 实现计划（B13.5）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消费者声明 `auto_renew=true` 时，broker 在其存活期间自动延长该消息的不可见期，使慢 handler 不再被重复投递。

**Architecture:** 在既有的「过期 inflight 重投」判定点分叉——满足四条判据（本次取件启用续租、续租预算未耗尽、归属已知、持有者 Telemetry 会话仍活）就只改写 `ExpireAtMs` 续租，否则走原重投路径。续租发生在本来就要写盘的时刻，零额外写放大；判定点已持队列锁，零新增并发推理。归属与预算持久化在 `InflightState` 上（`omitempty`，旧记录零迁移）；存活判定由 rpc 层以闭包随取件调用传入，deliver 不反向依赖 rpc。

**Tech Stack:** Go 1.2x、Pebble（`internal/store`）、gRPC/protobuf（`apache/rocketmq/v2`）、官方 Go SDK v5.1.4（e2e）。

**设计依据：** [spec](../specs/2026-08-14-auto-renew-design.md)。本计划的每条判据、每个默认值都出自该 spec，实现时遇到与 spec 冲突的地方以 spec 为准并提工单。

## Global Constraints

- **基线分支：`feat/push-consumer-e2e`（b364deb）**。本项要同步改造该分支上的 e2e 用例 6，不能基于 main。
- **不合并进 main，不 push 到 origin/main。** 合并决策留给用户。
- 日志一律用 `d.logger` / `s.logger`（`*slog.Logger`）。**禁止 `fmt.Printf` 作为日志机制。**
- 新建文件必须有文件头注释（职责 + 边界）；导出方法必须有注释（参数、返回、注意事项）；复杂逻辑与边界条件用中文注释解释**为什么**。
- **不得改动 `Attempts` / DLQ / 退避的任何既有语义。** 续租只影响「何时重投」，不影响「重投时怎么算」。
- **不得改 `deliver.Receive` 的既有形参。** 新参数一律走尾置变参 `ReceiveOption`——该方法有约 90 处测试调用点，改签名的清扫成本见 backlog B13.1「接口化地雷」。
- 每个 task 完成即 commit。
- 全部改动完成后，主套件必须 `go test -race -count=1 ./...` 全绿。

---

## File Structure

| 文件 | 动作 | 职责 |
|---|---|---|
| `internal/config/config.go` | 修改 | 新增 `AutoRenewEnabled` / `AutoRenewMaxDuration` 字段、默认值、`AutoRenewMax()` 访问器 |
| `internal/config/config_test.go` | 修改 | 访问器三态（缺省/非法/合法）与默认值 |
| `sq.example.yaml` | 修改 | 两个新配置项的示例与注释 |
| `internal/core/types.go` | 修改 | `InflightState` 增 `Owner` / `RenewUntilMs` 两个 `omitempty` 字段 |
| `internal/core/types_test.go` | 修改 | 旧格式解码兼容 + 新字段往返 |
| `internal/rpc/clientid.go` | **新建** | 从 gRPC metadata 取 `x-mq-client-id` |
| `internal/rpc/clientid_test.go` | **新建** | 取头的有无/空值三态 |
| `internal/rpc/sessions.go` | 修改 | `session.clientID`；`sessions.byClient` 引用计数索引；`aliveClient` |
| `internal/rpc/sessions_test.go` | 修改 | 同 clientID 多流并存与逐个注销 |
| `internal/rpc/server.go` | 修改 | Telemetry 注册会话时填 `clientID` |
| `internal/core/deliver/lease.go` | **新建** | `Lease` / `ReceiveOption` / `WithLease` / `renewable` |
| `internal/core/deliver/lease_test.go` | **新建** | `Lease.Enabled()` 与 `renewable` 判据表逐条 |
| `internal/core/deliver/deliver.go` | 修改 | `Receive` 收变参；`receiveOnce`/`receiveOnceLocked` 透传 `Lease`；扫描点分叉续租；两处 inflight 写入带上归属与预算 |
| `internal/core/deliver/deliver_test.go` | 修改 | 续租行为、不变式、硬上限、旧格式 |
| `internal/rpc/receive.go` | 修改 | 组装 `Lease` 并传入 `Receive` |
| `internal/rpc/receive_test.go` | 修改 | 有/无 client-id 头、配置关闭三态的组装结果 |
| `test/e2e/sdk_push_test.go` | 修改 | 用例 6 显式关闭续租；新增用例 7 |
| `README.md` | 修改 | 配置表补两项 + 行为说明 |

---

### Task 1: 配置项与访问器

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `sq.example.yaml`

**Interfaces:**
- Consumes: 无
- Produces: `Config.AutoRenewEnabled bool`、`Config.AutoRenewMaxDuration string`、`func (c *Config) AutoRenewMax() time.Duration`

- [ ] **Step 1: 写失败测试**

在 `internal/config/config_test.go` 追加：

```go
func TestAutoRenewMax(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"合法值按原样解析", "30s", 30 * time.Second},
		{"空串回落默认", "", 10 * time.Minute},
		{"非法值回落默认", "十分钟", 10 * time.Minute},
		{"零值回落默认", "0s", 10 * time.Minute},
		{"负值回落默认", "-1m", 10 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{AutoRenewMaxDuration: tc.raw}
			if got := c.AutoRenewMax(); got != tc.want {
				t.Fatalf("AutoRenewMax(%q) = %v，期望 %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestAutoRenewDefaults(t *testing.T) {
	// path=="" 走纯默认值分支，不读文件
	c, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\"): %v", err)
	}
	// 默认开启：这是协议正确的行为，SDK 的 PushConsumer 每次请求都在要它
	if !c.AutoRenewEnabled {
		t.Fatal("AutoRenewEnabled 默认应为 true")
	}
	if got := c.AutoRenewMax(); got != 10*time.Minute {
		t.Fatalf("AutoRenewMax 默认 = %v，期望 10m", got)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/config -run 'TestAutoRenew' -v`
Expected: 编译失败，`c.AutoRenewMaxDuration undefined` / `c.AutoRenewMax undefined`

- [ ] **Step 3: 实现**

在 `internal/config/config.go` 的 `DefaultInvisibleDuration` 字段声明之后插入：

```go
	// AutoRenewEnabled 消费者在 ReceiveMessage 里声明 auto_renew 时，是否在其
	// 存活期间自动续租不可见期。默认 true——这是协议正确的行为，官方 SDK 的
	// PushConsumer 每次请求都下发 auto_renew=true 且从不下发 invisible_duration，
	// 关掉它意味着 listener 慢过不可见期就会被重复投递。
	//
	// 注意与 AutoRenewMaxDuration 的不对称：本项用 &Config{...} 字面量构造时
	// 得 false（续租关闭，退化为固定不可见期），失败方向是安全的，所以不需要
	// 像 AutoRenewMax() 那样的兜底。
	AutoRenewEnabled bool `yaml:"auto_renew_enabled"`
	// AutoRenewMaxDuration 单次投递允许续租的总时长上限（Go duration 格式）。
	// 超过即按正常过期重投（Attempts++、可能进 DLQ）。它是防「消费者活着但
	// 卡死」的唯一兜底：没有上限，一个死锁的 handler 能把消息永久扣住，
	// 死信队列永远等不到它。
	AutoRenewMaxDuration string `yaml:"auto_renew_max_duration"`
```

在 `Load` 的默认值字面量里，`DefaultInvisibleDuration: "1m",` 之后加一行：

```go
		AutoRenewEnabled:         true,
		AutoRenewMaxDuration:     "10m",
```

在 `DefaultInvisible()` 之后追加访问器：

```go
// AutoRenewMax 解析后的单次投递续租总时长上限。解析失败或非正数兜底返回 10m。
//
// 不返回 0 的理由与 DefaultInvisible 同源：0 会让续租判据恒假，把一个配置
// 笔误变成静默的功能关闭——运维配了 auto_renew_max_duration 却发现续租没生效，
// 而日志里没有任何线索。Load 的校验只覆盖走 Load 的路径，用
// &config.Config{...} 字面量构造时拿到空串就会踩到。
func (c *Config) AutoRenewMax() time.Duration {
	d, err := time.ParseDuration(c.AutoRenewMaxDuration)
	if err != nil || d <= 0 {
		return 10 * time.Minute
	}
	return d
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/config -run 'TestAutoRenew' -v`
Expected: PASS（两个测试，共 6 个子用例）

- [ ] **Step 5: 补 sq.example.yaml**

在 `default_invisible_duration` 条目之后加入：

```yaml
# 消费者在 ReceiveMessage 里声明 auto_renew 时，是否在其存活期间自动续租
# 不可见期。官方 SDK 的 PushConsumer 每次请求都声明它，用来表达「我还在处理」。
# 关闭后 push 消费退回固定不可见期：listener 慢过 default_invisible_duration
# 就会被重复投递。
auto_renew_enabled: true
# 单次投递允许续租的总时长上限，超过即按正常过期重投（投递次数 +1，可能进死信）。
# 它兜的是「消费者进程活着但 handler 卡死」——没有上限这条消息会被永久扣住。
# 顺序消息尤其要注意：续租期间该队列的顺序锁一直被占，后续消息全部等待。
auto_renew_max_duration: 10m
```

- [ ] **Step 6: 加注释自检**

本 task 的注释已内联在 Step 3/5 的代码里，逐条确认：两个新字段各有「为什么」注释；`AutoRenewMax()` 有导出方法注释并写明兜底理由与踩坑场景；yaml 两项各有运维视角的说明。**无需额外日志**——配置解析在 `Load` 内，非法值的兜底由访问器承担，此处打日志会在每次调用时刷屏（访问器是热路径，每次 `ReceiveMessage` 都会调）。

- [ ] **Step 7: 提交**

```bash
git add internal/config/config.go internal/config/config_test.go sq.example.yaml
git commit -m "feat(config): auto_renew_enabled 与 auto_renew_max_duration 配置项"
```

---

### Task 2: `InflightState` 承载归属与续租预算

**Files:**
- Modify: `internal/core/types.go`
- Modify: `internal/core/types_test.go`

**Interfaces:**
- Consumes: 无
- Produces: `core.InflightState.Owner string`、`core.InflightState.RenewUntilMs int64`

- [ ] **Step 1: 写失败测试**

在 `internal/core/types_test.go` 追加：

```go
func TestInflightStateOldRecordDecodesAsNonRenewable(t *testing.T) {
	// M1–M4 落盘的旧记录：没有 owner / renew_until_ms 两个键
	old := []byte(`{"expire_at_ms":1700000000000,"attempts":3}`)
	st, err := DecodeInflight(old)
	if err != nil {
		t.Fatalf("解码旧记录: %v", err)
	}
	if st.ExpireAtMs != 1700000000000 || st.Attempts != 3 {
		t.Fatalf("旧字段解码错误: %+v", st)
	}
	// 零值即「不参与续租」，这是零迁移的全部依据
	if st.Owner != "" || st.RenewUntilMs != 0 {
		t.Fatalf("旧记录不应带续租信息: owner=%q renew_until=%d", st.Owner, st.RenewUntilMs)
	}
}

func TestInflightStateOmitsRenewFieldsWhenUnset(t *testing.T) {
	// 不启用续租时编码结果必须与改造前逐字节相同，否则存量部署重启后
	// 每条 inflight 都会因为多出两个键而变长，且旧版本 sq 读不回去
	b := EncodeInflight(&InflightState{ExpireAtMs: 1, Attempts: 1})
	if got := string(b); got != `{"expire_at_ms":1,"attempts":1}` {
		t.Fatalf("未启用续租时的编码结果 = %s，期望不含 owner/renew_until_ms", got)
	}
}

func TestInflightStateRenewFieldsRoundTrip(t *testing.T) {
	want := &InflightState{ExpireAtMs: 10, Attempts: 2, Ordered: true, Owner: "cli-1", RenewUntilMs: 99}
	got, err := DecodeInflight(EncodeInflight(want))
	if err != nil {
		t.Fatalf("往返解码: %v", err)
	}
	if *got != *want {
		t.Fatalf("往返后 = %+v，期望 %+v", got, want)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core -run 'TestInflightState' -v`
Expected: 编译失败，`unknown field Owner in struct literal`

- [ ] **Step 3: 实现**

`internal/core/types.go` 的 `InflightState`，在 `Ordered` 字段之后追加：

```go
	// Owner 本次投递的持有者客户端标识（gRPC metadata 的 x-mq-client-id）。
	// 仅在该次投递启用了自动续租时写入；空串表示不参与续租判定。
	// omitempty：M1–M4 落盘的旧记录无此键，解码得空串，行为与改造前相同，
	// 无需迁移——与 Ordered 当初的处理方式一致。
	Owner string `json:"owner,omitempty"`
	// RenewUntilMs 本次投递允许续租到的绝对时刻（毫秒）。0 表示不续租。
	//
	// 它是**硬上限**而不是目标：续租每次把 ExpireAtMs 推到
	// min(now+不可见期, RenewUntilMs)，越过它就必须走重投。没有这条线，
	// 一个「进程活着但 handler 卡死」的消费者能永久扣住消息，投递次数永不
	// 增长，死信队列永远等不到它——而这类故障恰恰最需要 DLQ 兜底。
	RenewUntilMs int64 `json:"renew_until_ms,omitempty"`
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core -run 'TestInflightState' -v`
Expected: PASS（3 个测试）

- [ ] **Step 5: 加注释自检**

两个新字段的「为什么」注释已在 Step 3 内联：`Owner` 说明零迁移依据，`RenewUntilMs` 说明它是硬上限而非目标、以及没有它会怎样。**本 task 无日志点**——纯数据结构，无分支无 I/O。

- [ ] **Step 6: 提交**

```bash
git add internal/core/types.go internal/core/types_test.go
git commit -m "feat(core): InflightState 承载续租归属与预算（omitempty，零迁移）"
```

---

### Task 3: 客户端标识提取与会话存活索引

**Files:**
- Create: `internal/rpc/clientid.go`
- Create: `internal/rpc/clientid_test.go`
- Modify: `internal/rpc/sessions.go`
- Modify: `internal/rpc/sessions_test.go`
- Modify: `internal/rpc/server.go`

**Interfaces:**
- Consumes: 无
- Produces: `func clientIDFrom(ctx context.Context) string`、`func (ss *sessions) aliveClient(id string) bool`

- [ ] **Step 1: 写失败测试**

新建 `internal/rpc/clientid_test.go`：

```go
package rpc

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestClientIDFrom(t *testing.T) {
	cases := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{"无 metadata", context.Background(), ""},
		{"无该头", metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("x-mq-language", "golang")), ""},
		{"头值为空", metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(clientIDHeaderKey, "")), ""},
		{"正常取值", metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(clientIDHeaderKey, "cli-abc")), "cli-abc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clientIDFrom(tc.ctx); got != tc.want {
				t.Fatalf("clientIDFrom = %q，期望 %q", got, tc.want)
			}
		})
	}
}
```

在 `internal/rpc/sessions_test.go` 追加：

```go
func TestAliveClientRefCount(t *testing.T) {
	ss := newSessions()
	if ss.aliveClient("c1") {
		t.Fatal("未注册的客户端不应判为存活")
	}
	a := &session{clientID: "c1", topics: map[string]bool{}}
	b := &session{clientID: "c1", topics: map[string]bool{}}
	ss.add(a)
	ss.add(b)
	if !ss.aliveClient("c1") {
		t.Fatal("已注册应判为存活")
	}
	// 重连窗口内新旧两条流短暂共存：注销其中一条后客户端仍然活着。
	// 用指针 map 会让后注册的覆盖先注册的，先注册的注销即整条删除，
	// 这里就会误判为已死——引用计数正是为了挡住这个。
	ss.remove(a)
	if !ss.aliveClient("c1") {
		t.Fatal("同一 clientID 尚有一条流存活，不应判为已死")
	}
	ss.remove(b)
	if ss.aliveClient("c1") {
		t.Fatal("全部注销后应判为已死")
	}
}

func TestAliveClientIgnoresEmptyID(t *testing.T) {
	ss := newSessions()
	ss.add(&session{clientID: "", topics: map[string]bool{}})
	// 空 id 不入索引，否则所有未带头的客户端会挤在同一个桶里互相"续命"
	if ss.aliveClient("") {
		t.Fatal("空 clientID 不应被判为存活")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc -run 'TestClientIDFrom|TestAliveClient' -v`
Expected: 编译失败，`undefined: clientIDFrom` / `undefined: clientIDHeaderKey` / `se.clientID undefined`

- [ ] **Step 3: 实现取头函数**

新建 `internal/rpc/clientid.go`：

```go
// 客户端标识提取：从 gRPC metadata 取 x-mq-client-id。
//
// 职责：
//   - 把「这个请求来自哪个客户端实例」这一事实从传输层取出来，供自动续租
//     判定持有者存活使用
//
// 边界：
//   - 不校验、不拒绝：取不到就是取不到，由调用方决定退化行为。手写客户端
//     不带这个头是合法的，它只是享受不到自动续租，不该被拒绝服务
//   - 不做缓存：metadata 取值是 map 查找，成本远低于一次缓存失效判断
package rpc

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// clientIDHeaderKey 客户端实例标识头。这是协议内置约定而非自造：官方 SDK 在
// 每个出站请求（含 Telemetry 流、含未开鉴权时）都附带它，protobuf 里还有
// 专门的错误码注释「Request is rejected due to missing of x-mq-client-id header」。
const clientIDHeaderKey = "x-mq-client-id"

// clientIDFrom 从入站 ctx 的 metadata 取客户端标识。
//
// 取不到（无 metadata / 无该头 / 头值为空）一律返回空串，调用方据此退化为
// 「不启用续租」。返回空串不是错误，不打日志——ReceiveMessage 是热路径，
// 手写客户端每次轮询都会命中这条分支，打日志即刷屏。
func clientIDFrom(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vs := md.Get(clientIDHeaderKey)
	if len(vs) == 0 {
		return ""
	}
	return vs[0]
}
```

- [ ] **Step 4: 实现会话索引**

`internal/rpc/sessions.go`：

在 `session` 结构体的 `clientType` 字段之后加：

```go
	// clientID 客户端实例标识（x-mq-client-id）。自动续租靠它判定
	// 「持有这条 inflight 的客户端是否还活着」。可能为空（手写客户端未带头），
	// 空值不入 byClient 索引。
	clientID string
```

在 `sessions` 结构体的 `nextID` 字段之后加：

```go
	// byClient 客户端标识 → 该标识当前活跃的会话数。
	//
	// 用计数而不是 map[string]*session：同一个 client id 理论上可以并存多条
	// Telemetry 流（客户端重连时新旧流在一小段窗口内共存）。用指针会让后注册
	// 的覆盖先注册的，随后先注册的那条注销时把整个条目删掉——客户端明明活着
	// 却被判为已死，它手上正在处理的消息会被立刻重投。
	byClient map[string]int
```

`newSessions` 改为：

```go
func newSessions() *sessions {
	return &sessions{all: map[*session]struct{}{}, byClient: map[string]int{}}
}
```

`add` 在 `ss.all[se] = struct{}{}` 之后加：

```go
	if se.clientID != "" {
		// 空 id 不入索引：所有未带头的客户端会挤在同一个桶里，
		// 任一条流活着就让全体被判为存活，等于让续租判据失效
		ss.byClient[se.clientID]++
	}
```

`remove` 在 `delete(ss.all, se)` 之后加：

```go
	if se.clientID != "" {
		if n := ss.byClient[se.clientID] - 1; n > 0 {
			ss.byClient[se.clientID] = n
		} else {
			delete(ss.byClient, se.clientID)
		}
	}
```

在 `count()` 之后追加：

```go
// aliveClient 判定某客户端标识当前是否仍有活跃的 Telemetry 会话。
//
// 供自动续租的判据 4 使用（见 auto-renew spec §6）。空 id 恒为 false。
//
// 固有窗口：Telemetry 流断开到重连之间会判为 false，于是消息被重投——
// 这恰好是改造前的无条件行为，不是回归，只是那一刻少续了一次租。反向误判
// （进程已死但 gRPC 尚未感知流断）由 InflightState.RenewUntilMs 硬上限兜底。
func (ss *sessions) aliveClient(id string) bool {
	if id == "" {
		return false
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.byClient[id] > 0
}
```

- [ ] **Step 5: 接线 Telemetry 注册点**

`internal/rpc/server.go` 的 Telemetry 会话注册处，把

```go
					sess = &session{stream: stream, clientType: c.Settings.GetClientType(), topics: map[string]bool{}}
					s.sessions.add(sess)
					s.logger.Debug("telemetry 会话注册",
						"client_type", sess.clientType, "connections", s.sessions.count())
```

替换为

```go
					sess = &session{stream: stream, clientType: c.Settings.GetClientType(),
						topics: map[string]bool{}, clientID: clientIDFrom(stream.Context())}
					s.sessions.add(sess)
					s.logger.Debug("telemetry 会话注册",
						"client_type", sess.clientType, "client_id", sess.clientID,
						"connections", s.sessions.count())
```

- [ ] **Step 6: 跑测试确认通过**

Run: `go test -race ./internal/rpc -run 'TestClientIDFrom|TestAliveClient' -v`
Expected: PASS（2 个测试，共 6 个子用例）

- [ ] **Step 7: 加关键节点日志**

- 会话注册的 Debug 日志已在 Step 5 补上 `client_id`——这是「某客户端何时被认为上线」的唯一线索，缺了就无法解释「为什么这条消息没续租」。
- 会话注销：`internal/rpc/server.go` 里既有的注销 defer

```go
			s.logger.Debug("telemetry 会话注销", "connections", s.sessions.count())
```

  改为带上标识：

```go
			s.logger.Debug("telemetry 会话注销", "client_id", sess.clientID,
				"connections", s.sessions.count())
```

  这条与「持有者已断线导致重投」的日志（Task 4）配对，排障时能拼出完整时序。
- `clientIDFrom` 取不到时**刻意不打日志**，理由已写在函数注释里（热路径刷屏）。

- [ ] **Step 8: 加注释自检**

确认：`clientid.go` 有文件头注释（职责 + 边界，含「不校验不拒绝」这条边界）；`clientIDHeaderKey` 注释说明它是协议内置而非自造；`clientIDFrom` 有导出级注释说明返回空串的语义与不打日志的理由；`byClient` 注释说明为什么是计数而非指针；`aliveClient` 有注释说明固有窗口与兜底。

- [ ] **Step 9: 提交**

```bash
git add internal/rpc/clientid.go internal/rpc/clientid_test.go internal/rpc/sessions.go internal/rpc/sessions_test.go internal/rpc/server.go
git commit -m "feat(rpc): 客户端标识提取与会话存活索引（引用计数，防重连窗口误判）"
```

---

### Task 4: 续租租约与扫描点分叉（核心）

**Files:**
- Create: `internal/core/deliver/lease.go`
- Create: `internal/core/deliver/lease_test.go`
- Modify: `internal/core/deliver/deliver.go`
- Modify: `internal/core/deliver/deliver_test.go`

**Interfaces:**
- Consumes: `core.InflightState.Owner` / `.RenewUntilMs`（Task 2）
- Produces:
  - `type Lease struct { Owner string; MaxRenew time.Duration; Alive func(string) bool }`
  - `func (l Lease) Enabled() bool`
  - `type ReceiveOption func(*receiveOpts)`
  - `func WithLease(l Lease) ReceiveOption`
  - `func (d *Deliverer) Receive(ctx, group, topic string, queueID uint32, maxMsgs int, invisible, wait time.Duration, filter Filter, opts ...ReceiveOption) ([]*core.Message, error)`

- [ ] **Step 1: 写失败测试（判据表）**

新建 `internal/core/deliver/lease_test.go`：

```go
package deliver

import (
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
)

func TestLeaseEnabled(t *testing.T) {
	alive := func(string) bool { return true }
	cases := []struct {
		name string
		l    Lease
		want bool
	}{
		{"三项齐全", Lease{Owner: "c", MaxRenew: time.Minute, Alive: alive}, true},
		{"缺 Owner", Lease{MaxRenew: time.Minute, Alive: alive}, false},
		{"MaxRenew 为零", Lease{Owner: "c", Alive: alive}, false},
		{"MaxRenew 为负", Lease{Owner: "c", MaxRenew: -time.Second, Alive: alive}, false},
		{"缺 Alive", Lease{Owner: "c", MaxRenew: time.Minute}, false},
		{"全空", Lease{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.l.Enabled(); got != tc.want {
				t.Fatalf("Enabled() = %v，期望 %v", got, tc.want)
			}
		})
	}
}

func TestRenewableCriteria(t *testing.T) {
	const now = int64(1_000_000)
	okLease := Lease{Owner: "poller", MaxRenew: time.Minute, Alive: func(string) bool { return true }}
	deadLease := Lease{Owner: "poller", MaxRenew: time.Minute, Alive: func(string) bool { return false }}
	fresh := func() *core.InflightState {
		return &core.InflightState{ExpireAtMs: now - 1, Attempts: 1, Owner: "holder", RenewUntilMs: now + 1000}
	}

	if !renewable(okLease, fresh(), now) {
		t.Fatal("四条判据全真时应当续租")
	}
	// 判据 1：本次取件未启用续租（如 SimpleConsumer 接管了这个队列）
	if renewable(Lease{}, fresh(), now) {
		t.Fatal("判据 1：未启用续租的取件不应续租")
	}
	// 判据 2：旧格式记录（RenewUntilMs 为 0）
	st := fresh()
	st.RenewUntilMs = 0
	if renewable(okLease, st, now) {
		t.Fatal("判据 2：旧格式记录不应续租")
	}
	// 判据 2：续租预算已耗尽（边界值——等于 now 即算耗尽）
	st = fresh()
	st.RenewUntilMs = now
	if renewable(okLease, st, now) {
		t.Fatal("判据 2：预算恰好耗尽时不应续租")
	}
	// 判据 3：归属未知
	st = fresh()
	st.Owner = ""
	if renewable(okLease, st, now) {
		t.Fatal("判据 3：归属未知时不应续租")
	}
	// 判据 4：持有者已断线
	if renewable(deadLease, fresh(), now) {
		t.Fatal("判据 4：持有者已断线时不应续租")
	}
}

func TestRenewableChecksRecordOwnerNotPoller(t *testing.T) {
	// 判据 4 判的是记录里的持有者，不是本次轮询方——重平衡后两者会不同
	const now = int64(1_000_000)
	var asked string
	l := Lease{Owner: "poller", MaxRenew: time.Minute, Alive: func(id string) bool {
		asked = id
		return true
	}}
	st := &core.InflightState{ExpireAtMs: now - 1, Owner: "holder", RenewUntilMs: now + 1000}
	renewable(l, st, now)
	if asked != "holder" {
		t.Fatalf("存活判定问的是 %q，应当问记录里的持有者 \"holder\"", asked)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/deliver -run 'TestLease|TestRenewable' -v`
Expected: 编译失败，`undefined: Lease` / `undefined: renewable`

- [ ] **Step 3: 实现 lease.go**

新建 `internal/core/deliver/lease.go`：

```go
// 取件租约：自动续租不可见期的参数与判据。
//
// 职责：
//   - 描述一次取件的续租参数（持有者是谁、预算多长、如何判定存活）
//   - 给出「一条已到期的 inflight 该续租还是该重投」的唯一判据
//
// 边界：
//   - 不做存活判定本身——判定逻辑属于 rpc 层的会话注册表，这里只收一个闭包。
//     这样 deliver 不反向依赖 rpc，也避免了「构造后 setter 忘了调就静默不续租」
//     的哑火面
//   - 不碰 Attempts / DLQ / 退避：续租只影响「何时重投」，不影响「重投时怎么算」
package deliver

import (
	"time"

	"github.com/xushixin/sq/internal/core"
)

// Lease 本次取件的租约参数。
//
// 三个字段任一为零值即视为不启用续租（见 Enabled）——调用方漏配任何一项都
// 整体退化成固定不可见期的既有行为，不会出现「半开」的中间态。
type Lease struct {
	// Owner 本次取件方的客户端标识（x-mq-client-id）。本轮投出去的消息
	// 会把它记进 InflightState.Owner 作为归属。
	Owner string
	// MaxRenew 单次投递允许续租的总时长上限。投递时换算成绝对时刻记进
	// InflightState.RenewUntilMs。
	MaxRenew time.Duration
	// Alive 判定某个持有者是否仍有活跃的 Telemetry 会话。
	// 注意入参是**记录里的持有者**，不一定等于本 Lease 的 Owner——重平衡后
	// 轮询方与持有者会是两个不同的客户端。
	Alive func(string) bool
}

// Enabled 本次取件是否启用自动续租。
func (l Lease) Enabled() bool {
	return l.Owner != "" && l.MaxRenew > 0 && l.Alive != nil
}

// receiveOpts 取件可选参数的聚合。
type receiveOpts struct {
	lease Lease
}

// ReceiveOption 取件可选参数。
//
// 之所以用变参而不是给 Receive 加形参：Receive 只有 1 处生产调用点，却有
// 约 90 处测试调用点，改签名的清扫成本极高（见 backlog B13.1「接口化地雷」：
// 上一次改它造成了编译期无感、运行期 panic 的大面积清扫）。变参尾置让既有
// 调用点一字不改照常编译。
type ReceiveOption func(*receiveOpts)

// WithLease 为本次取件启用自动续租。
//
// 传入的 Lease 不满足 Enabled() 时等价于不传——调用方不需要自己做条件判断。
func WithLease(l Lease) ReceiveOption {
	return func(o *receiveOpts) { o.lease = l }
}

// renewable 判定一条**已到期**的 inflight 应当续租而非重投。
//
// 调用前提：调用方已确认 st.ExpireAtMs <= nowMs。四条判据全真才续租，任一
// 不成立都回落到原重投路径——失败方向永远是「重投」，因为重投是 at-least-once
// 契约内的既有行为，而错误地续租会让消息被一个可能已经死掉的持有者扣住，
// 直到硬上限才释放。
func renewable(l Lease, st *core.InflightState, nowMs int64) bool {
	// 判据 1 必须先行：Enabled() 保证了 l.Alive 非 nil，判据 4 才能直接调用
	if !l.Enabled() {
		return false
	}
	// 判据 2：0 表示旧格式记录或本就不续租；<= nowMs 表示续租预算已耗尽
	if st.RenewUntilMs <= nowMs {
		return false
	}
	// 判据 3：归属未知就无从判定存活，保守重投
	if st.Owner == "" {
		return false
	}
	// 判据 4：问的是记录里的持有者，不是本次轮询方
	return l.Alive(st.Owner)
}
```

- [ ] **Step 4: 跑判据测试确认通过**

Run: `go test ./internal/core/deliver -run 'TestLease|TestRenewable' -v`
Expected: PASS（3 个测试，共 8 个子用例）

- [ ] **Step 5: 写 deliver 行为的失败测试**

在 `internal/core/deliver/deliver_test.go` 追加。**注意**：请先阅读该文件既有测试的建造方式（如何构造 `*Deliverer`、如何写入消息、辅助函数叫什么），下列用例的骨架必须与既有风格一致，不要另起一套 harness。

```go
// aliveAlways / aliveNever 构造测试用的存活判定闭包
func aliveAlways() func(string) bool { return func(string) bool { return true } }
func aliveNever() func(string) bool  { return func(string) bool { return false } }

// TestReceiveRenewsInsteadOfRedelivering 持有者活着且预算充足时，到期的
// inflight 被续租而不是重投：不产出消息、Attempts 不变、ExpireAtMs 被推后。
func TestReceiveRenewsInsteadOfRedelivering(t *testing.T) {
	// 建 Deliverer、建 topic/group、写 1 条消息（沿用本文件既有辅助）
	// 1) 首次取件：invisible=50ms，带 WithLease{Owner:"c1", MaxRenew:time.Minute, Alive:aliveAlways()}
	//    断言取到 1 条，attempt==1
	// 2) 等待 80ms 让不可见期到期
	// 3) 再次取件（同 lease）：断言取到 0 条——被续租了，没有重投
	// 4) 直接读 inflight 记录断言：Attempts 仍为 1、Owner=="c1"、
	//    RenewUntilMs>0、ExpireAtMs 已被推到未来
}

// TestReceiveRedeliversWhenOwnerGone 持有者断线时立刻走重投（故障转移不退化）
func TestReceiveRedeliversWhenOwnerGone(t *testing.T) {
	// 同上构造，第 3 步改用 Alive:aliveNever() 的 lease 取件
	// 断言取到 1 条且 DeliveryAttempt==2
}

// TestRenewDoesNotExceedBudget 续租不越过硬上限，越过后正常重投且 Attempts++
func TestRenewDoesNotExceedBudget(t *testing.T) {
	// MaxRenew 设为 60ms、invisible 设为 20ms：
	// 头几轮取件被续租（取到 0 条），等预算耗尽后再取件必须取到 1 条且 attempt==2
	// 同时断言续租期间 ExpireAtMs 从未超过 RenewUntilMs
}

// TestRenewPreservesOrderedLock 顺序消息续租后仍占着队列顺序锁
func TestRenewDoesNotBreakOrderedLock(t *testing.T) {
	// 写 2 条同 MessageGroup 的顺序消息，取件取到第 1 条（占锁）
	// 等其不可见期到期后带 lease 再取件：断言取到 0 条
	//（既没重投第 1 条，也没因为锁被误释放而投出第 2 条）
}

// TestOldInflightRecordStillRedelivers 旧格式 inflight（无 Owner/RenewUntilMs）
// 即便本次取件启用了续租也照常重投——零迁移的行为保证
func TestOldInflightRecordStillRedelivers(t *testing.T) {
	// 取件后手工把 inflight 记录改写为不含 owner/renew_until_ms 的旧格式 JSON，
	// 等到期后带 lease 取件：断言取到 1 条且 attempt==2
}

// TestReceiveWithoutLeaseUnchanged 不传 WithLease 时行为与改造前逐字节相同
func TestReceiveWithoutLeaseUnchanged(t *testing.T) {
	// 取件（不带任何 option）后读 inflight：断言 Owner=="" 且 RenewUntilMs==0
	// 等到期后再取件：断言正常重投，attempt==2
}
```

上述 6 个用例的注释块描述的是**必须实现的断言**，不是可选说明——实现者要把它们写成真正的测试代码。

- [ ] **Step 6: 跑测试确认失败**

Run: `go test ./internal/core/deliver -run 'TestReceiveRenews|TestReceiveRedelivers|TestRenewDoes|TestOldInflight|TestReceiveWithoutLease' -v`
Expected: FAIL——`Receive` 尚不接受 `ReceiveOption`，编译失败

- [ ] **Step 7: 改造 deliver.go**

**7.1 `Receive` 收变参并透传：**

```go
func (d *Deliverer) Receive(ctx context.Context, group, topic string, queueID uint32, maxMsgs int, invisible, wait time.Duration, filter Filter, opts ...ReceiveOption) ([]*core.Message, error) {
	var ro receiveOpts
	for _, opt := range opts {
		opt(&ro)
	}
	gc, err := d.mt.EnsureGroup(ctx, group)
```

并把循环体内的调用改为：

```go
		msgs, err := d.receiveOnce(ctx, group, topic, queueID, maxMsgs, invisible, gc.EffectiveMaxAttempts(), filter, ro.lease)
```

**7.2 `receiveOnce` / `receiveOnceLocked` 各加尾置形参 `lease Lease`**，并在 `receiveOnce` 内原样透传给 `receiveOnceLocked`。

**7.3 扫描点分叉。** 在 `receiveOnceLocked` 内，`var reds []redeliver` 之后加：

```go
	// renewal 一条判定为「应续租而非重投」的 inflight。带值拷贝而非指针：
	// Scan 回调的 k/v 出了回调即失效，虽然 DecodeInflight 解出的是独立对象，
	// 这里仍按回调契约取值拷贝，免得日后有人把解码改成零拷贝时踩坑。
	type renewal struct {
		offset uint64
		st     core.InflightState
	}
	var renews []renewal
```

把

```go
		if ist.ExpireAtMs <= now && len(reds) < maxMsgs {
			reds = append(reds, redeliver{offset: off, attempts: ist.Attempts, ordered: ist.Ordered})
		}
```

替换为

```go
		if ist.ExpireAtMs <= now {
			if renewable(lease, ist, now) {
				// 续租不占 maxMsgs 预算：它不产出可投递的消息，只是把这条
				// inflight 的不可见期往后推。占了预算会让「一批慢消息正在
				// 续租」白白挤掉本轮本可以投出的新消息。
				renews = append(renews, renewal{offset: off, st: *ist})
			} else if len(reds) < maxMsgs {
				reds = append(reds, redeliver{offset: off, attempts: ist.Attempts, ordered: ist.Ordered})
			}
		}
```

**7.4 续租写入。** 在扫描的 `if err != nil { ... }` 错误处理之后、`for _, r := range reds {` 之前插入：

```go
	// 阶段 1.5：续租。放在重投循环之前，让日志里「续租」与「重投」的时序与
	// 扫描时的判定顺序一致，排障时不用反着推。两者写的是同一片 inflight 键
	// 空间的不同键，先后不影响正确性。
	for _, rn := range renews {
		newExp := now + invisible.Milliseconds()
		if newExp > rn.st.RenewUntilMs {
			// 不越过硬上限：到点这一轮把 ExpireAtMs 正好压在上限上，
			// 下一轮扫描时判据 2 即为假，自然转入重投
			newExp = rn.st.RenewUntilMs
		}
		rn.st.ExpireAtMs = newExp
		// Attempts 与 Ordered 原样保留，这两条都是承重的：
		//   - 收据句柄由 (group,topic,queue,offset,attempts) 编码，Ack 与
		//     ChangeInvisible 都拿 attempts 校验陈旧句柄。续租若动了它，
		//     等于把持有者手里那个还在用的句柄当场作废——它处理完回来 ack
		//     会被拒，消息必然重投，续租的目的完全落空。
		//   - Ordered 丢了，顺序锁判据 orderedBusy 会漏看这条记录，
		//     下一条顺序消息会与它并发投递，顺序即破。
		b.Set(store.InflightKey(group, topic, queueID, rn.offset), core.EncodeInflight(&rn.st))
		staged = true
		d.logger.Debug("inflight 自动续租", "group", group, "topic", topic,
			"queue", queueID, "offset", rn.offset, "owner", rn.st.Owner,
			"attempts", rn.st.Attempts, "new_expire_at_ms", newExp,
			"budget_remain_ms", rn.st.RenewUntilMs-now)
	}
```

**7.5 重投写入带上新归属。** 把重投循环里的

```go
		b.Set(store.InflightKey(group, topic, queueID, r.offset),
			core.EncodeInflight(&core.InflightState{ExpireAtMs: exp, Attempts: m.DeliveryAttempt, Ordered: r.ordered}))
```

替换为

```go
		nst := &core.InflightState{ExpireAtMs: exp, Attempts: m.DeliveryAttempt, Ordered: r.ordered}
		if lease.Enabled() {
			// 重投等于一次全新的投递：归属换成本次轮询方，续租预算重新计时。
			// 不继承旧记录的 RenewUntilMs——那是上一任持有者的预算，
			// 继承会让接手者的可用续租时间莫名其妙地短一截。
			nst.Owner = lease.Owner
			nst.RenewUntilMs = now + lease.MaxRenew.Milliseconds()
		}
		b.Set(store.InflightKey(group, topic, queueID, r.offset), core.EncodeInflight(nst))
```

**7.6 新投递写入带上归属。** 把阶段 2 的

```go
			b.Set(store.InflightKey(group, topic, queueID, m.Offset),
				core.EncodeInflight(&core.InflightState{ExpireAtMs: expireAt, Attempts: 1, Ordered: ordered}))
```

替换为

```go
			nst := &core.InflightState{ExpireAtMs: expireAt, Attempts: 1, Ordered: ordered}
			if lease.Enabled() {
				// 只有启用续租的取件才在记录上留归属与预算：SimpleConsumer 的
				// 投递不写这两个字段，其 inflight 与改造前逐字节相同
				nst.Owner = lease.Owner
				nst.RenewUntilMs = now + lease.MaxRenew.Milliseconds()
			}
			b.Set(store.InflightKey(group, topic, queueID, m.Offset), core.EncodeInflight(nst))
```

- [ ] **Step 8: 跑测试确认通过**

Run: `go test -race -count=1 ./internal/core/deliver`
Expected: PASS（含既有全部用例——它们不传 option，行为必须完全不变）

- [ ] **Step 9: 加关键节点日志**

除 Step 7.4 已内联的续租 Debug 日志外，补两条 Info——它们是 spec §14 点名要求的排障入口：

在 Step 7.3 的 `else if len(reds) < maxMsgs` 分支内，重投候选入列之前加分诊日志：

```go
			} else if len(reds) < maxMsgs {
				// 只在「本可以续租却没续成」时记一笔，区分两种原因——这是慢消费者
				// 排障的入口：看到「预算耗尽」说明 handler 卡死该查业务，
				// 看到「持有者已断线」说明是故障转移在正常工作。
				// 判据 1（本次取件没启用续租）不打日志：SimpleConsumer 的每一次
				// 正常重投都会命中它，打了就是刷屏。
				if lease.Enabled() && ist.Owner != "" {
					if ist.RenewUntilMs <= now {
						d.logger.Info("续租预算已耗尽，转正常重投", "group", group,
							"topic", topic, "queue", queueID, "offset", off,
							"owner", ist.Owner, "attempts", ist.Attempts,
							"renew_until_ms", ist.RenewUntilMs)
					} else {
						d.logger.Info("持有者会话已断，立即重投", "group", group,
							"topic", topic, "queue", queueID, "offset", off,
							"owner", ist.Owner, "attempts", ist.Attempts)
					}
				}
				reds = append(reds, redeliver{offset: off, attempts: ist.Attempts, ordered: ist.Ordered})
			}
```

自检清单：续租成功路径有 Debug（成功路径不静默）；两条失败/转移路径各有 Info 且带 owner 与足够上下文；高频且无信息量的分支（判据 1）刻意不打并写明理由；全程用 `d.logger`，无 `fmt.Printf`。

- [ ] **Step 10: 加注释自检**

确认：`lease.go` 有文件头注释（职责 + 两条边界）；`Lease` 及其三个字段、`Enabled`、`ReceiveOption`、`WithLease`、`renewable` 各有注释，其中 `ReceiveOption` 写明「为什么用变参」、`renewable` 写明「失败方向永远是重投」及其理由、`Lease.Alive` 写明入参是记录里的持有者而非 Owner；`deliver.go` 的三处改动各有「为什么」注释（续租不占 maxMsgs 预算、Attempts/Ordered 承重、重投不继承旧预算）。

- [ ] **Step 11: 提交**

```bash
git add internal/core/deliver/lease.go internal/core/deliver/lease_test.go internal/core/deliver/deliver.go internal/core/deliver/deliver_test.go
git commit -m "feat(deliver): 扫描点自动续租——四条判据、硬上限、句柄不失效"
```

---

### Task 5: rpc 层接线

**Files:**
- Modify: `internal/rpc/receive.go`
- Modify: `internal/rpc/receive_test.go`

**Interfaces:**
- Consumes: `deliver.Lease` / `deliver.WithLease`（Task 4）、`clientIDFrom` / `sessions.aliveClient`（Task 3）、`Config.AutoRenewEnabled` / `AutoRenewMax()`（Task 1）
- Produces: 无（终点接线）

- [ ] **Step 1: 写失败测试**

在 `internal/rpc/receive_test.go` 追加。**注意**：`leaseFor` 是本 task 要新增的小函数，把「要不要启用续租、启用成什么样」这个判断从 `ReceiveMessage` 的长函数里摘出来，使其可单测。

```go
func TestLeaseForAutoRenew(t *testing.T) {
	ss := newSessions()
	base := &Config{AutoRenewEnabled: true, AutoRenewMaxDuration: "30s"}
	withID := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(clientIDHeaderKey, "cli-1"))

	t.Run("正常启用", func(t *testing.T) {
		l := leaseFor(withID, base, ss, true)
		if !l.Enabled() {
			t.Fatal("应当启用续租")
		}
		if l.Owner != "cli-1" || l.MaxRenew != 30*time.Second {
			t.Fatalf("租约参数错误: %+v", l)
		}
	})
	t.Run("客户端未请求续租", func(t *testing.T) {
		if leaseFor(withID, base, ss, false).Enabled() {
			t.Fatal("客户端未设 auto_renew 时不应启用")
		}
	})
	t.Run("服务端配置关闭", func(t *testing.T) {
		off := &Config{AutoRenewEnabled: false, AutoRenewMaxDuration: "30s"}
		if leaseFor(withID, off, ss, true).Enabled() {
			t.Fatal("配置关闭时不应启用")
		}
	})
	t.Run("缺 client-id 头时退化而不报错", func(t *testing.T) {
		if leaseFor(context.Background(), base, ss, true).Enabled() {
			t.Fatal("无 client-id 头时不应启用")
		}
	})
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc -run TestLeaseForAutoRenew -v`
Expected: 编译失败，`undefined: leaseFor`

- [ ] **Step 3: 实现**

在 `internal/rpc/receive.go` 里新增：

```go
// leaseFor 组装本次取件的续租租约。
//
// 三个开关全开才启用：客户端在请求里声明了 auto_renew、服务端配置未关闭、
// 且能从 metadata 取到客户端标识。任一不满足返回零值 Lease（deliver 侧视为
// 不启用，退化为固定不可见期）——**不返回错误、不拒绝请求**：手写客户端不带
// x-mq-client-id 是合法的，它只是享受不到自动续租。
func leaseFor(ctx context.Context, cfg *Config, ss *sessions, wantAutoRenew bool) deliver.Lease {
	if !wantAutoRenew || !cfg.AutoRenewEnabled {
		return deliver.Lease{}
	}
	cid := clientIDFrom(ctx)
	if cid == "" {
		return deliver.Lease{}
	}
	return deliver.Lease{Owner: cid, MaxRenew: cfg.AutoRenewMax(), Alive: ss.aliveClient}
}
```

> `cfg` 的具体类型以 `Server.cfg` 的实际类型为准（`*config.Config`），import 相应调整；上面的 `*Config` 只是占位写法，实现时用真实类型。

在 `ReceiveMessage` 里，把

```go
	msgs, err := s.dl.Receive(stream.Context(), group, topic, queueID, batch, invisible, wait, filter)
```

替换为

```go
	var opts []deliver.ReceiveOption
	if lease := leaseFor(stream.Context(), s.cfg, s.sessions, req.GetAutoRenew()); lease.Enabled() {
		opts = append(opts, deliver.WithLease(lease))
		s.logger.Debug("ReceiveMessage 启用自动续租", "group", group, "topic", topic,
			"queue", queueID, "owner", lease.Owner, "max_renew", lease.MaxRenew)
	} else if req.GetAutoRenew() {
		// 客户端要了续租却没启用成——这是「为什么我的慢 handler 还是被重投了」
		// 的第一个排查点。Debug 而非 Warn：手写客户端不带 client-id 头是合法的，
		// 而 push 消费每次轮询都会走到这里，Warn 会刷屏。
		s.logger.Debug("ReceiveMessage 请求了自动续租但未启用（配置关闭或缺 x-mq-client-id 头）",
			"group", group, "topic", topic, "queue", queueID,
			"cfg_enabled", s.cfg.AutoRenewEnabled, "has_client_id", clientIDFrom(stream.Context()) != "")
	}
	msgs, err := s.dl.Receive(stream.Context(), group, topic, queueID, batch, invisible, wait, filter, opts...)
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -race -count=1 ./internal/rpc`
Expected: PASS（含既有全部用例）

- [ ] **Step 5: 加关键节点日志**

日志已内联在 Step 3：启用成功走 Debug 并带 owner 与预算（成功路径不静默）；「要了却没启用」走 Debug 并带两个判据的实际取值，直接指出是配置关闭还是缺头。自检：无 `fmt.Printf`，两条分支都带 group/topic/queue 上下文。

- [ ] **Step 6: 加注释自检**

确认 `leaseFor` 有导出级质量的注释，写明三个开关、零值语义、以及「不返回错误不拒绝请求」这条边界及其理由。

- [ ] **Step 7: 提交**

```bash
git add internal/rpc/receive.go internal/rpc/receive_test.go
git commit -m "feat(rpc): ReceiveMessage 接线自动续租租约"
```

---

### Task 6: e2e——用例 6 改造与用例 7 新增

**Files:**
- Modify: `test/e2e/sdk_push_test.go`

**Interfaces:**
- Consumes: 全部前序 task
- Produces: 无

- [ ] **Step 1: 改造用例 6，让它显式关闭续租**

`TestOfficialGoSDKPushRedeliverAfterInvisibleExpiry` 的 `startBroker` 调用，把

```go
	endpoint := startBroker(t, func(c *config.Config) {
		c.DefaultInvisibleDuration = "5s"
	})
```

替换为

```go
	endpoint := startBroker(t, func(c *config.Config) {
		c.DefaultInvisibleDuration = "5s"
		// 必须显式关闭自动续租：PushConsumer 每次 ReceiveMessage 都下发
		// auto_renew=true，续租开启时这条消息会在持有者存活期间被一路续下去，
		// 永远走不到重投——本用例就再也观测不到它要测的东西了。
		// 这不是给用例打补丁：用例 6 测的是「不可见期到期兜底」这条路径本身，
		// 它必须在续租关闭的前提下才可见。续租开启时的正确行为由用例 7 覆盖，
		// 两者是一对互补的断言。
		c.AutoRenewEnabled = false
	})
```

- [ ] **Step 2: 跑用例 6 确认仍绿**

Run: `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPushRedeliverAfterInvisibleExpiry -v`
Expected: PASS

- [ ] **Step 3: 新增用例 7**

在用例 6 之后追加：

```go
// TestOfficialGoSDKPushAutoRenewPreventsRedelivery 用例 7：慢 handler 在持有
// 消息期间被自动续租，因而不会被重复投递。
//
// 与用例 6 是一对互补断言：同样的构造（不可见期 5s、单消费线程、首投占住
// 8s），用例 6 关掉续租测「到期兜底还活着」，本用例开着续租测「续租真的挡住
// 了那次重投」。缺了本用例，把续租代码整段删掉用例 6 反而更绿——那样的测试
// 套件对本特性是零覆盖。
func TestOfficialGoSDKPushAutoRenewPreventsRedelivery(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) {
		// 5s 不可见期：不续租的话 t=5s 必然重投（用例 6 已证明这一点），
		// 所以「没有第二次投递」这个观测结果只能由续租解释。
		c.DefaultInvisibleDuration = "5s"
		// 显式写出默认值，让用例不依赖默认值将来是否变动
		c.AutoRenewEnabled = true
		c.AutoRenewMaxDuration = "10m"
	})
	const (
		topic = "e2e-push-autorenew"
		group = "e2e-push-autorenew-g"
		body  = "renew-me"
	)
	sendPlain(t, endpoint, topic, body)

	c := newCollector(t)
	c.decide = func(mv *rmq.MessageView) rmq.ConsumerResult {
		// 首投占住唯一消费线程 8s，跨过 5s 不可见期。与用例 6 的区别只有
		// broker 侧开着续租——同样按投递次数分支，万一真的发生重投，
		// 重投件立即返回，用例不会滚成无限重投而是直接在断言处红。
		if mv.GetDeliveryAttempt() == 1 {
			time.Sleep(8 * time.Second)
		}
		return rmq.SUCCESS
	}
	startPushConsumer(t, endpoint, group, topic, c,
		rmq.WithPushConsumptionThreadCount(1))

	// 时间线（确定的）：
	//   t=0    首投进 listener，attempt=1，句柄 H1，唯一 worker 被占住；
	//   t≈5s   broker 侧不可见期到期，长轮询触发扫描——四条判据全真
	//          （本次取件带租约、预算 10m 未耗尽、归属已记、持有者
	//          Telemetry 会话仍活）→ 续租到 t≈10s，不重投；
	//   t=8s   handler 返回，用**仍然有效**的 H1 ack 成功（续租没动 attempts，
	//          所以句柄没失效）；
	//   t=8s~20s 静默段，确认不再有任何投递。
	waitCount(t, c, 1, 20*time.Second)

	// 7b【承重】：静默段必须长于一个完整的不可见期（12s > 5s）。窗口短于
	// 不可见期时，「ack 成功」与「ack 失败、正在等下一次重投」两个世界观测
	// 一致，7a 就成了假绿——这是 B13.2 终审抓出用例 1b 假绿的同款陷阱。
	time.Sleep(12 * time.Second)

	got := c.snapshot()
	// 7a【承重】：全窗口只投递过一次。这里必须用 == 而不是 >=：本用例证明的
	// 就是「不多投」，>= 会让断言恒真。
	if len(got) != 1 {
		t.Fatalf("期望恰好 1 次投递（续租应挡住重投），实际 %d 次: %+v", len(got), got)
	}
	// 7c：续租没有污染投递次数。它同时反证了句柄未失效——若续租改了
	// attempts，ack 会被 broker 拒（陈旧句柄），这条消息必然在 t≈13s 重投，
	// 7a 会先红。
	if got[0].attempt != 1 {
		t.Fatalf("attempt = %d，期望 1（续租不应改变投递次数）", got[0].attempt)
	}
	t.Logf("用例 7 通过：慢 handler 持有 8s 跨过 5s 不可见期，续租挡住了重投")
}
```

- [ ] **Step 4: 跑用例 7 确认通过**

Run: `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPushAutoRenewPreventsRedelivery -v`
Expected: PASS，耗时约 25–35s

**若它红且 `len(got) == 2`**：说明续租没生效。按此顺序排查，不要改测试去迁就：
1. broker 日志里有没有 `inflight 自动续租` 的 Debug 行；
2. 没有的话，看有没有 `ReceiveMessage 请求了自动续租但未启用`——那是配置或 client-id 头的问题；
3. 都没有，看有没有 `持有者会话已断，立即重投`——那是会话索引没接对（Task 3）。

- [ ] **Step 5: 跑完整 push 套件**

Run: `cd test/e2e && go test -tags e2e -count=1 -run 'TestOfficialGoSDKPush' -v`
Expected: 7/7 PASS

- [ ] **Step 6: 提交**

```bash
git add test/e2e/sdk_push_test.go
git commit -m "test(e2e): 用例 6 显式关闭续租，新增用例 7 验证续租挡住重投"
```

---

### Task 7: 文档与全量验收

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: 全部前序 task
- Produces: 无

- [ ] **Step 1: 补 README**

在 README 的配置项表格里，`default_invisible_duration` 之后补两行（表格列以 README 现有格式为准）：

| 配置项 | 默认 | 说明 |
|---|---|---|
| `auto_renew_enabled` | `true` | 消费者声明 `auto_renew` 时，在其存活期间自动续租不可见期。官方 SDK 的 PushConsumer 总是声明它；关闭后 push 消费退回固定不可见期，慢 handler 会被重复投递 |
| `auto_renew_max_duration` | `10m` | 单次投递允许续租的总时长上限，超过即按正常过期重投（投递次数 +1，可能进死信）。兜的是「消费者活着但 handler 卡死」 |

并在 README 讲消费语义的段落补一段（措辞可按 README 既有风格调整，内容不可少）：

> **自动续租**：消费者可以在 `ReceiveMessage` 里声明 `auto_renew`，表示「我还在处理这条消息」。broker 会在该客户端的 Telemetry 会话存活期间自动延长这条消息的不可见期，使处理时间超过 `default_invisible_duration` 的 handler 不被重复投递。续租有 `auto_renew_max_duration` 的硬上限，超过即按正常过期重投——这是「消费者进程活着但处理逻辑卡死」时死信队列仍能兜底的保证。客户端一旦断线，续租立即停止，消息在当前不可见期到期后正常重投，故障转移速度不受影响。
>
> 顺序消息需额外注意：续租期间该队列的顺序锁一直被持有，后续消息全部等待，最长可达 `auto_renew_max_duration`。

- [ ] **Step 2: 主套件全量验收**

Run: `go test -race -count=1 ./...`
Expected: 全部包 PASS

- [ ] **Step 3: e2e 全量验收**

Run: `cd test/e2e && go test -tags e2e -count=1 -timeout 60m`
Expected: 全部 PASS

> `web/dist/` 只含 `.gitkeep` 时 `TestConsoleServedFromBinary` 会失败——前端构建产物不入库。先跑 `make web` 再重跑，这是环境前置条件，不是回归。

- [ ] **Step 4: 完工自检（instrumenting-code 清单）**

逐条确认并在提交信息里报告结果：

- [ ] 每个错误分支都带上下文与原因打了日志
- [ ] 续租的**成功**路径有日志（不静默）
- [ ] 无 `fmt.Printf` / `print` 作为日志机制
- [ ] 新建文件（`clientid.go`、`lease.go`）都有文件头注释（职责 + 边界）
- [ ] 导出方法（`AutoRenewMax`、`Lease`、`Enabled`、`WithLease`、`ReceiveOption`）都有注释
- [ ] 非显然分支有「为什么」注释：续租不占 `maxMsgs` 预算、`Attempts`/`Ordered` 承重、重投不继承旧预算、`byClient` 用计数而非指针、判据 1 刻意不打日志

- [ ] **Step 5: 提交**

```bash
git add README.md
git commit -m "docs(readme): 补自动续租配置与语义说明"
```

---

## 附：自审记录

本计划写完后按 writing-plans 自审三项，结果如下：

**1. spec 覆盖**：spec §5（状态格式）→ Task 2；§6（判据）→ Task 4 Step 1/3；§7（存活判定）→ Task 3；§8（变参不改签名）→ Task 4 Step 3/7.1；§9（配置）→ Task 1；§10（用例 6 改造 + 用例 7）→ Task 6；§11（测试计划）九行逐条落在 Task 1/2/3/4/6 的测试步骤里；§13.1（顺序消息）→ Task 4 Step 5 的 `TestRenewDoesNotBreakOrderedLock` 与 Task 7 的 README 段落；§14（日志与注释）→ 每个实现 task 的「加日志」「加注释」两步。**无遗漏。**

**2. 占位符扫描**：Task 4 Step 5 的六个测试用例以注释块描述断言而非给出完整代码，这是**刻意**的——该文件的 `*Deliverer` 建造方式与辅助函数只有读了现场才知道，硬写一套会与既有风格打架。已在该步显式标注「描述的是必须实现的断言，不是可选说明」。其余步骤均给出可直接落地的代码。

**3. 类型一致性**：`Lease` 三字段名（`Owner` / `MaxRenew` / `Alive`）在 Task 4 定义、Task 5 使用，一致；`renewable(l Lease, st *core.InflightState, nowMs int64)` 的调用点（Task 4 Step 7.3）传 `ist` 为 `*core.InflightState`（`DecodeInflight` 的返回类型），一致；`clientIDFrom` / `clientIDHeaderKey` / `aliveClient` 在 Task 3 定义、Task 5 使用，一致；配置字段 `AutoRenewEnabled` / `AutoRenewMaxDuration` / `AutoRenewMax()` 在 Task 1 定义，Task 5、Task 6 使用，一致。
