# PushConsumer 真实 e2e 验证 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用官方 Go SDK 的 `PushConsumer` 建立 6 条真实 e2e 覆盖，实证 `settings.go` 里 `fifo=false` 是正确终态而非待翻转的临时值，并把结论写回注释。

**Architecture:** 新增单文件 `test/e2e/sdk_push_test.go`，内含一个采集器（把 callback 驱动的消费收敛成可断言结构）+ 三个 helper + 6 个用例。broker 启停完全复用现有 `startBroker(t, mutate...)`。为让「停机不丢」用例能在秒级跑完，把 `receive.go` 里硬编码的 1 分钟默认不可见时长提为配置项 `default_invisible_duration`。

**Tech Stack:** Go 1.x；`github.com/apache/rocketmq-clients/golang/v5 v5.1.4`；e2e 是独立 module（`test/e2e/go.mod`），编译标签 `e2e`。

**设计依据：** `docs/superpowers/specs/2026-08-13-push-consumer-e2e-design.md`。计划里引用 `§x.y` 一律指该 spec。

## Global Constraints

- **所有 e2e 文件首行必须是 `//go:build e2e`**，跑测试必须带 `-tags e2e`。漏了标签会得到 "no test files"，看起来像跑过了——这是本仓库踩过的坑。
- **测试函数命名统一 `TestOfficialGoSDKPush*` 前缀**（§10），与既有 `TestOfficialGoSDK*` 同族且可用 `-run TestOfficialGoSDKPush` 一次筛出全部 push 用例。
- **listener 内绝对不得调用 `t.Fatalf` / `t.Fatal`**（§5.3 第 1 条）。listener 跑在 SDK 后台 goroutine 上，`Fatalf` 只在测试 goroutine 内有定义行为。采集器只采集，断言一律回主 goroutine。
- **测试代码的「日志」= `t.Logf`**，不是 `fmt.Printf`。broker 侧日志由 `startBroker` 的 `dumpBrokerLog` 在失败时自动展开，不需要额外接线。
- **不得为让测试跑通而放宽断言**：§6.1 明确写了「真跑出间歇性红时，正确修法是调高不可见时长，不是把断言放宽成忽略重复」。其余用例同理。
- **本条目不翻转 `fifo` 标志**（§2）。Task 6 只改注释文本，`fifo := false` 这行代码不动。
- 中文注释；新建文件写「职责 + 边界」头注释；导出/包级 helper 写用途注释。

---

## File Structure

| 文件 | 动作 | 职责 |
|---|---|---|
| `internal/config/config.go` | 修改 | 新增 `DefaultInvisibleDuration` 字段 + 默认值 + 校验 + `DefaultInvisible()` 访问器 |
| `internal/config/config_test.go` | 修改 | 新增 `TestDefaultInvisibleDuration` |
| `internal/rpc/receive.go` | 修改 | 硬编码 `time.Minute` 换成 `s.cfg.DefaultInvisible()`（按文本定位，不按行号） |
| `sq.example.yaml` | 修改 | 新增配置项示例 |
| `README.md` | 修改 | 配置块新增一行 |
| `test/e2e/sdk_test.go` | 修改 | `writeBrokerConfig` 补 `DefaultInvisibleDuration` 字段（不补 broker 起不来） |
| `test/e2e/sdk_cluster_test.go` | 修改 | `clusterNodeConfig` 同款陷阱，同样补 `DefaultInvisibleDuration` |
| `test/e2e/sdk_push_test.go` | **新建** | 采集器 + 3 helper + 6 个 push 用例 |
| `internal/rpc/settings.go` | 修改 | `fifo` 注释订正为 §7.1 的终态文本 |

---

### Task 1: `default_invisible_duration` 配置项

**Files:**
- Modify: `internal/config/config.go`（结构体字段、`Load` 默认值、`Load` 校验、访问器）
- Modify: `internal/config/config_test.go`
- Modify: `internal/rpc/receive.go`（`invisible := req.GetInvisibleDuration()...` 那个 `if` 块；**按文本定位不要按行号**——本分支基线含 B13.1 的过滤改动，该块的行号约在 133 而非 110）
- Modify: `sq.example.yaml`
- Modify: `README.md`
- Modify: `test/e2e/sdk_test.go`（`writeBrokerConfig` 的 `config.Config` 字面量）

**Interfaces:**
- Produces: `func (c *Config) DefaultInvisible() time.Duration` —— 后续 task 不直接调它，但 Task 5 的用例 6 靠 `startBroker(t, func(c *config.Config){ c.DefaultInvisibleDuration = "5s" })` 生效。

**动机（写进注释，不要只写在计划里）：** 官方 Go SDK 的 `PushConsumer` 在 `WrapReceiveMessageRequest`（`push_consumer.go:734`）里只设 `LongPollingTimeout` / `BatchSize` / `AutoRenew`，**从不下发 `invisible_duration`**（§3.5）。所以整条 push 消费路径的不可见时长 100% 由服务端这个默认值决定，它不是边角配置。

- [ ] **Step 1: 写失败的配置测试**

在 `internal/config/config_test.go` 的 `TestDefaultMaxAttempts` 之后追加（`time` / `os` / `filepath` 均已在 import 块内，无需改 import）：

```go
// TestDefaultInvisibleDuration 默认不可见时长为 1m（与改造前 receive.go 的硬编码
// 一致），且非正 duration 必须在 Load 阶段被拒——配成 0 会让消息投出去立刻可再投，
// 表现为无限重复消费，等到运行时才发现的代价远高于启动即挡。
func TestDefaultInvisibleDuration(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\"): %v", err)
	}
	if got := cfg.DefaultInvisible(); got != time.Minute {
		t.Fatalf("默认不可见时长 = %v，期望 1m", got)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	os.WriteFile(p, []byte("default_invisible_duration: 0s\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("应拒绝 default_invisible_duration=0s")
	}
	os.WriteFile(p, []byte("default_invisible_duration: \"\"\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("应拒绝空 default_invisible_duration")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/config -run TestDefaultInvisibleDuration -v`
Expected: 编译失败，`cfg.DefaultInvisible undefined`

- [ ] **Step 3: 加结构体字段**

`internal/config/config.go`，在 `RetentionCheckInterval` 那行**之前**插入（紧跟 `DefaultMaxAttempts`，同属订阅/投递语义）：

```go
	// DefaultInvisibleDuration 客户端未在 ReceiveMessage 里指定 invisible_duration
	// 时采用的不可见时长（Go duration 格式）。
	//
	// 这不是边角配置：官方 Go SDK 的 PushConsumer 从不下发该字段（其
	// WrapReceiveMessageRequest 只设 LongPollingTimeout/BatchSize/AutoRenew），
	// 所以整条 push 消费路径的不可见时长全部由它决定。默认 1m。
	DefaultInvisibleDuration string `yaml:"default_invisible_duration"`
```

- [ ] **Step 4: 加默认值**

同文件 `Load` 里的 cfg 字面量（`DefaultMaxAttempts: 16, LogLevel: "info",` 的下一行区域），在 `RetentionCheckInterval: "5m",` **之前**插入：

```go
		DefaultInvisibleDuration: "1m",
```

- [ ] **Step 5: 加校验**

同文件，在校验 `RetentionCheckInterval` 的 `if` **之前**插入（与它同款写法）：

```go
	if d, err := time.ParseDuration(cfg.DefaultInvisibleDuration); err != nil || d <= 0 {
		return nil, fmt.Errorf("配置 default_invisible_duration 须为正 duration（如 1m），得到 %q", cfg.DefaultInvisibleDuration)
	}
```

- [ ] **Step 6: 加访问器**

同文件，在 `RetentionInterval()` **之前**插入：

```go
// DefaultInvisible 解析后的默认不可见时长（Load 已校验合法，此处不会失败）。
func (c *Config) DefaultInvisible() time.Duration {
	d, _ := time.ParseDuration(c.DefaultInvisibleDuration)
	return d
}
```

- [ ] **Step 7: 跑测试确认通过**

Run: `go test ./internal/config -run TestDefaultInvisibleDuration -v`
Expected: PASS

- [ ] **Step 8: 接到 receive.go（含关键节点日志）**

`internal/rpc/receive.go`，把这段：

```go
	invisible := req.GetInvisibleDuration().AsDuration()
	if invisible <= 0 {
		invisible = time.Minute
	}
```

替换为：

```go
	invisible := req.GetInvisibleDuration().AsDuration()
	if invisible <= 0 {
		// 客户端没给不可见时长，落到服务端默认值。这条分支是 push 消费的常态而
		// 非兜底：官方 Go SDK 的 PushConsumer 从不下发 invisible_duration，
		// 整条 push 路径的不可见窗口都由 default_invisible_duration 决定。
		invisible = s.cfg.DefaultInvisible()
		s.logger.Debug("ReceiveMessage 采用服务端默认不可见时长",
			"group", group, "topic", topic, "queue", queueID, "invisible", invisible)
	}
```

日志取 Debug 级：ReceiveMessage 是长轮询高频路径，Info 会把日志淹掉（全局规范「循环内高频日志降级到 Debug」）。e2e 的 broker 本来就跑 `log_level: debug`，排查时看得到。

> `s.cfg` 一定非 nil：`rpc.New` 的六个依赖里 cfg 是第一个，`cmd/sq/main.go:383` 与 `internal/rpc/server_test.go:102`（用 `config.Load("")`）都传了真值。**不要**在这里加 `if s.cfg == nil` 兜底——那是给不存在的调用方写的补丁。

- [ ] **Step 9: 检查 `time` 是否还被 receive.go 使用**

Run: `go build ./internal/rpc`
若报 `"time" imported and not used`，说明 `time.Minute` 是该文件对 `time` 的唯一引用，删掉 import；否则不动。**不要预先猜**，以编译器结论为准。

- [ ] **Step 10: 补 e2e 的 broker 配置字面量（漏了就全线红）**

`test/e2e/sdk_test.go` 的 `writeBrokerConfig`，在 `RetentionCheckInterval: "5m",` 那段**之后**插入：

```go
		// 与 RetentionCheckInterval / TxnCheckInterval 同款陷阱：本结构体不走
		// config.Load 的默认值，零值会序列化成 default_invisible_duration: ""
		// 并被 Load 的校验拒绝，broker 直接起不来。取值同 Load 的缺省。
		DefaultInvisibleDuration: "1m",
```

**同款陷阱还有第二处**：`test/e2e/sdk_cluster_test.go` 的 `clusterNodeConfig`（约 117 行）同样会被 `yaml.Marshal` 后交给 `launchBroker` → `config.Load`，也必须补 `DefaultInvisibleDuration: "1m"`，注释风格与该字面量里既有的 `MessageEncoding` / `DiskWatermarkPercent` 两条对齐。

判据统一为一句话：**这个 `config.Config` 字面量最终会不会被序列化成 yaml 交给 `config.Load`**。会，就必须显式给值。

**`test/e2e/sdk_test.go` 的 `rewriteBrokerConfig`（约 283 行）不要动**：它是先 `yaml.Unmarshal` 既有配置文件到空字面量、mutate 后再 `Marshal` 回写，字段会被原样带过，不存在空串问题。在那里塞硬编码值反而会覆盖调用方经 `writeBrokerConfig` mutate 设进去的值——那正是跨档位互读用例要避免的。

**这一步漏掉会让全部现存 e2e 红**（broker 起不来），而不是只影响新用例。

> 本步的第二处（`clusterNodeConfig`）是**执行期由 executor 发工单撞出来的 plan 补漏**，写计划时只核了 `sdk_test.go` 一个字面量。已按上述判据统一收口。

- [ ] **Step 11: 更新文档**

`sq.example.yaml`，在 `default_max_attempts: 16` 之后插入：

```yaml
# 客户端未指定 invisible_duration 时的不可见时长（Go duration，如 1m、30s）。
# 官方 Go SDK 的 PushConsumer 不下发该字段，push 消费全靠这个默认值。
default_invisible_duration: 1m
```

`README.md` 配置块，在 `default_max_attempts: 16       # ...` 之后插入：

```
default_invisible_duration: 1m # 客户端未指定时的消息不可见时长；push 消费全靠它
```

- [ ] **Step 12: 全量回归 + 提交**

```bash
go test -race -count=1 ./internal/config ./internal/rpc
```
Expected: 两包全 PASS

```bash
cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKSendAndReceive
```
Expected: PASS（证明 Step 10 的单机字面量生效，broker 起得来）

> 初稿这里写的是 `-run TestOfficialGoSDKBasic`，**那个测试不存在**——执行期由 executor 撞出并替换。选任何一条起单机 broker 的现存用例都可以，`TestOfficialGoSDKSendAndReceive` 是最短的那条。

再挑**最短的一条集群用例**跑一遍，证明 `clusterNodeConfig` 那处也生效（全量集群套件留给 Task 6 Step 4，这里只要一条起得来即可）。

```bash
git add internal/config/config.go internal/config/config_test.go internal/rpc/receive.go sq.example.yaml README.md test/e2e/sdk_test.go
git commit -m "feat(config): default_invisible_duration 配置项，取代 receive.go 硬编码的 1 分钟

push 消费路径的不可见时长全部由服务端默认值决定（官方 Go SDK 的 PushConsumer
从不下发 invisible_duration），配置化后 e2e 才能把它调到秒级。"
```

---

### Task 2: push harness + 用例 1（基础闭环）

**Files:**
- Create: `test/e2e/sdk_push_test.go`

**Interfaces:**
- Produces（后续 4 个 task 全部依赖，签名以此为准）：
  - `type recv struct { id, body string; attempt int32; receipt, group string }`
  - `type pushCollector struct { ... }`，字段 `decide func(*rmq.MessageView) rmq.ConsumerResult`、`hold time.Duration`
  - `func newCollector(t *testing.T) *pushCollector`
  - `func (c *pushCollector) listener() *rmq.FuncMessageListener`
  - `func (c *pushCollector) snapshot() []recv`
  - `func (c *pushCollector) count() int`
  - `func startPushConsumer(t *testing.T, endpoint, group, topic string, c *pushCollector, opts ...rmq.PushConsumerOption) (rmq.PushConsumer, func())`
  - `func waitCount(t *testing.T, c *pushCollector, n int, within time.Duration)`
  - `func sendPlain(t *testing.T, endpoint, topic string, bodies ...string)`
- Consumes: `startBroker`（`sdk_test.go`）、`sendFifoBatch`（`sdk_fifo_test.go`，Task 3 用）——同 package `e2e`，同 `//go:build e2e`，直接可用。

> **与 spec §5.2 的一处有意偏离**：spec 写 `startPushConsumer` 只返回 `rmq.PushConsumer`。这里改为额外返回一个 `stop func()`。理由是用例 6 必须在测试中途显式停 A，而 `t.Cleanup` 里还会再停一次；`stop` 用 `sync.Once` 包住，重复调用是 no-op。只返回 consumer 的话，用例 6 要么自己重复 `GracefulStop`（`cli.GracefulStop()` 可能返错），要么绕开 helper 自己建消费者（丢掉 cleanup 保障）。

- [ ] **Step 1: 写文件头 + 采集器（含头注释与边界说明）**

新建 `test/e2e/sdk_push_test.go`：

```go
//go:build e2e

// 官方 Go SDK PushConsumer e2e：B13.2 的验证载体。
//
// 职责：
//   - 覆盖 push 消费路径（callback 驱动）的六条真实链路：基础闭环、长轮询唤醒、
//     消费失败重投、超限转 DLQ、FIFO 顺序不破、停机后 inflight 不丢
//   - 实证 settings.go 下发的 fifo=false 是正确终态：重试计数与死信判定都归 broker
//
// 边界：
//   - 不覆盖 LitePushConsumer（依赖未实现的 SyncLiteSubscription）
//   - 不覆盖集群档（本文件全部单机 broker）
//   - 不验证 AutoRenew 续租（sq 侧未实现，已另立 backlog B13.5）
//   - 不做批量 ReceiveBatchSize 的断言（客户端侧观察不到有意义的差别）
package e2e

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// recv 是一次 listener 调用入口处的值快照。
//
// 边界：刻意不持有 *rmq.MessageView 指针——SDK 在回调返回后可能复用/回收该对象，
// 跨 goroutine 持有它读出来的值不可信。
type recv struct {
	id      string
	body    string
	attempt int32
	receipt string // 回执句柄：用例 3 用它判别「broker 重投」还是「客户端本地重投」
	group   string // MessageGroup
}

// pushCollector 把 callback 驱动的消费收敛成测试可断言的结构。
//
// 边界：只做采集与返回值决策，不做任何断言。listener 跑在 SDK 的后台 goroutine
// 上，而 t.Fatalf 只有在测试 goroutine 内调用才有定义的行为——在别处调不会正确
// 终止测试，只会让失败以诡异方式表现。所有断言一律回主 goroutine 做。
type pushCollector struct {
	t *testing.T

	mu  sync.Mutex
	got []recv // 按 listener【入口】到达序追加

	inFlight    atomic.Int32 // 当前并发进行中的 listener 调用数
	maxInFlight atomic.Int32 // 历史峰值，用例 5 的核心断言

	// decide 决定每条消息的消费结果；nil 视为 rmq.SUCCESS。
	decide func(*rmq.MessageView) rmq.ConsumerResult
	// hold 是 listener 内部的停留时长，用于撑开并发重叠的观测窗口。
	hold time.Duration
}

// newCollector 构造一个默认全部 ACK、不停留的采集器。
func newCollector(t *testing.T) *pushCollector {
	return &pushCollector{t: t}
}

// listener 返回可交给 WithPushMessageListener 的监听器。
//
// MessageListener 接口的 consume 方法未导出（§3.6），包外无法自行实现，
// 只能走 SDK 提供的 FuncMessageListener 适配器。
func (c *pushCollector) listener() *rmq.FuncMessageListener {
	return &rmq.FuncMessageListener{
		Consume: func(mv *rmq.MessageView) rmq.ConsumerResult {
			// 并发峰值必须在【入口】抬起、在【出口】落下。只在出口统计的话，
			// 无论真实并发多少，读到的永远是 1。
			n := c.inFlight.Add(1)
			for {
				old := c.maxInFlight.Load()
				if n <= old || c.maxInFlight.CompareAndSwap(old, n) {
					break
				}
			}
			defer c.inFlight.Add(-1)

			var mg string
			if g := mv.GetMessageGroup(); g != nil {
				mg = *g
			}
			r := recv{
				id:      mv.GetMessageId(),
				body:    string(mv.GetBody()),
				attempt: mv.GetDeliveryAttempt(),
				receipt: mv.GetReceiptHandle(),
				group:   mg,
			}
			// 采集点必须在 hold 之前：用例 5 要证的是「listener 被调用的顺序」。
			// 先 hold 再 append 采到的是【完成序】——完成序乱不代表调用序乱，
			// 那是在测另一件事，且是会假红的那种错。
			c.mu.Lock()
			c.got = append(c.got, r)
			c.mu.Unlock()
			c.t.Logf("listener 入口: body=%s attempt=%d inflight=%d", r.body, r.attempt, n)

			if c.hold > 0 {
				time.Sleep(c.hold)
			}
			res := rmq.SUCCESS
			if c.decide != nil {
				res = c.decide(mv)
			}
			c.t.Logf("listener 出口: body=%s 结果=%+v", r.body, res.Type)
			return res
		},
	}
}

// snapshot 加锁拷贝已采集的记录。
func (c *pushCollector) snapshot() []recv {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]recv, len(c.got))
	copy(out, c.got)
	return out
}

// count 已采集条数。
func (c *pushCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got)
}
```

> **import 只写本 task 用得到的**。Go 对未使用的 import 是编译错误，不是警告。上面的 import 块是 Task 2 结束时的完整状态；`fmt` 由 Task 3 引入、`github.com/xushixin/sq/internal/config` 由 Task 4 引入，各自的 step 里有明确指示。

- [ ] **Step 2: 写三个 helper**

追加到同文件：

```go
// startPushConsumer 建 PushConsumer、Start、注册 cleanup，返回消费者与一个
// 幂等的 stop 函数。
//
// 返回 stop 而不是只返回消费者：用例 6 必须在测试中途显式停 A，而 cleanup 里
// 还会再停一次；sync.Once 包住后重复调用是 no-op。
func startPushConsumer(t *testing.T, endpoint, group, topic string, c *pushCollector, opts ...rmq.PushConsumerOption) (rmq.PushConsumer, func()) {
	t.Helper()
	base := []rmq.PushConsumerOption{
		rmq.WithPushSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
		rmq.WithPushMessageListener(c.listener()),
	}
	pc, err := rmq.NewPushConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	}, append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewPushConsumer(%s): %v", group, err)
	}
	if err := pc.Start(); err != nil {
		t.Fatalf("pushConsumer.Start(%s): %v", group, err)
	}
	t.Logf("PushConsumer 已启动: group=%s topic=%s", group, topic)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			if err := pc.GracefulStop(); err != nil {
				t.Logf("GracefulStop(%s) 返回错误（不判失败）: %v", group, err)
			}
			t.Logf("PushConsumer 已停止: group=%s", group)
		})
	}
	t.Cleanup(stop)
	return pc, stop
}

// waitCount 轮询等到累计采集条数达 n，超时即 Fatalf。
//
// 用轮询而非 channel 唤醒：channel 通知要处理「唤醒丢失」和「多次到达只醒一次」，
// 写错就是间歇性挂；e2e 本身是秒级尺度，50ms 轮询没有隐蔽失败模式。
func waitCount(t *testing.T, c *pushCollector, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if c.count() >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("在 %v 内只采集到 %d/%d 条，已收到: %+v", within, c.count(), n, c.snapshot())
}

// sendPlain 发送若干条普通消息（无 MessageGroup、无 tag）。
func sendPlain(t *testing.T, endpoint, topic string, bodies ...string) {
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
	for _, b := range bodies {
		if _, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(b)}); err != nil {
			t.Fatalf("Send %q: %v", b, err)
		}
	}
	t.Logf("已发送 %d 条到 topic=%s", len(bodies), topic)
}

// bodySet 把采集记录折成 body 集合，便于做集合断言。
func bodySet(rs []recv) map[string]int {
	m := make(map[string]int, len(rs))
	for _, r := range rs {
		m[r.body]++
	}
	return m
}
```

- [ ] **Step 3: 写用例 1（基础闭环）**

追加到同文件：

```go
// TestOfficialGoSDKPushBasicLoop 用例 1：QueryAssignment → 长轮询 Receive →
// listener → Ack 整条链路通，且 Ack 真落地（位点推进）。
func TestOfficialGoSDKPushBasicLoop(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-push-basic"
		group = "e2e-push-basic-g"
	)
	bodies := []string{"p-0", "p-1", "p-2", "p-3", "p-4"}
	sendPlain(t, endpoint, topic, bodies...)

	c := newCollector(t)
	_, stopA := startPushConsumer(t, endpoint, group, topic, c)
	// 1a：5 条全部到达，body 集合与发送集合相等
	waitCount(t, c, len(bodies), 30*time.Second)
	got := bodySet(c.snapshot())
	for _, b := range bodies {
		if got[b] != 1 {
			t.Fatalf("body %q 收到 %d 次，期望恰好 1 次；全量: %v", b, got[b], got)
		}
	}
	if len(got) != len(bodies) {
		t.Fatalf("收到 %d 种 body，期望 %d 种: %v", len(got), len(bodies), got)
	}

	// 1b：停掉 A 后同组新起 B，15s 窗口内零调用 —— 证明 Ack 真落地，位点推过了。
	//
	// 这里不需要 SQL92 用例那种「N 轮连续空轮询」的手法：那条纪律的成因是
	// SimpleConsumer 每次 Receive 只轮询一个队列，单次为空不代表所有队列都空。
	// PushConsumer 对每个已分配队列各维持一条长轮询，一个持续窗口内的零调用
	// 即覆盖全部队列。
	stopA()
	cB := newCollector(t)
	// B 必须与 A 【同组】：换组会从头重投，那测的是另一件事。
	startPushConsumer(t, endpoint, group, topic, cB)
	time.Sleep(15 * time.Second)
	if n := cB.count(); n != 0 {
		t.Fatalf("Ack 未落地：新消费者在 15s 窗口内收到 %d 条: %+v", n, cB.snapshot())
	}
	t.Logf("用例 1 通过：5 条闭环 + Ack 位点已推进")
}
```

> **B 的消费组不得改成 `group+"-b"`**：换组会从头重投，1b 就永远红，然后很容易被「修」成删掉这条断言。同组是这条断言成立的前提。

- [ ] **Step 4: 跑用例 1**

Run: `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPushBasicLoop -v`
Expected: PASS。失败时 broker 日志会自动展开（`startBroker` 的 cleanup）。

- [ ] **Step 5: 提交**

```bash
git add test/e2e/sdk_push_test.go
git commit -m "test(e2e): PushConsumer 采集器 harness 与基础闭环用例

采集器把 callback 驱动的消费收敛成可断言结构：入口采集、并发峰值统计、
值快照不留 MessageView 指针；listener 内不做任何断言。"
```

---

### Task 3: 用例 5（FIFO 顺序不破 + 并发峰值）

**Files:**
- Modify: `test/e2e/sdk_push_test.go`

**Interfaces:**
- Consumes: Task 2 的全部 harness；`sendFifoBatch(t, endpoint, topic, group, prefix string, n int)`（`sdk_fifo_test.go:47`，**第 4 个参数是 MessageGroup**，不是消费组）。

**这是本条目承重最大的一条**：`settings.go` 里 `fifo=false` 正确与否，全靠它。

- [ ] **Step 1: 写用例 5（先补 import）**

先在 `test/e2e/sdk_push_test.go` 的 import 块加 `"fmt"`（本 task 的 `fmt.Sprintf` 是它的首个使用者），再追加：

```go
// TestOfficialGoSDKPushFIFOOrderLock 用例 5：同 MessageGroup 的 20 条消息在
// 4 线程 push 消费下顺序不破。
//
// 这是 settings.go 下发 fifo=false 的承重断言：顺序安全不依赖协商标志，
// 而由 broker 侧的队列级顺序锁保证（每队列至多一条未终结的顺序 inflight）。
func TestOfficialGoSDKPushFIFOOrderLock(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-push-fifo"
		group = "e2e-push-fifo-g"
		mg    = "order-key"
		total = 20
	)
	sendFifoBatch(t, endpoint, topic, mg, "fifo", total)

	c := newCollector(t)
	// hold 必须远小于不可见时长（默认 1m，本用例不改）：若 hold 逼近不可见窗口，
	// broker 会认为该条超时未终结而重投，重投件与仍在跑的那件重叠 →
	// maxInFlight 变 2 → 假红。50ms 相对 1m 有三个数量级余量。
	c.hold = 50 * time.Millisecond
	startPushConsumer(t, endpoint, group, topic, c,
		rmq.WithPushConsumptionThreadCount(4))
	waitCount(t, c, total, 120*time.Second)

	// 断言 A：body 序严格全等于发送序。
	//
	// 用严格全等而不是「递增」：重投会让 body 重复出现，全等能把它照出来。
	// 真跑出间歇性红时正确的修法是调高不可见时长，不是把断言放宽成忽略重复
	// ——那等于把要测的东西测没了。
	snap := c.snapshot()
	if len(snap) != total {
		t.Fatalf("采集到 %d 条，期望恰好 %d 条（多出来的是重投件）: %+v", len(snap), total, snap)
	}
	for i, r := range snap {
		want := fmt.Sprintf("fifo-%d", i)
		if r.body != want {
			t.Fatalf("第 %d 条乱序：期望 %s 收到 %s；全量: %+v", i, want, r.body, snap)
		}
		if r.group != mg {
			t.Fatalf("第 %d 条 MessageGroup 回读不符: %q", i, r.group)
		}
	}

	// 断言 B：并发峰值恒为 1 —— 顺序锁的直接证据，也是本用例真正的证明。
	//
	// 只写断言 A 是不够的：顺序锁正确时客户端手上永远只有一条，4 个线程没有
	// 乱序机会，A 会恒真；锁坏掉时 A 只是【概率性】变红。B 是确定性的——
	// 线程池有 4 个线程、listener 撑住 50ms，只要 broker 任何一次放出两条
	// 同队列顺序消息，重叠必被观测到，maxInFlight 立刻变 2。
	if peak := c.maxInFlight.Load(); peak != 1 {
		t.Fatalf("顺序锁失效：listener 并发峰值 = %d，期望 1", peak)
	}
	t.Logf("用例 5 通过：20 条严格按序，并发峰值 1")
}
```

- [ ] **Step 2: 跑用例 5**

Run: `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPushFIFOOrderLock -v`
Expected: PASS。

预期耗时**秒级**：现有 SimpleConsumer 版 FIFO 用例要跑约 110s（每次 `Receive` 只轮询一个队列，每条都要等一轮四队列循环）；PushConsumer 对每个已分配队列各维持一条长轮询，broker 在 ack 后 100ms 内即可放下一条。**若实测远超 30s，先在 broker 日志里确认是不是长轮询没挂上，不要直接调大 deadline。**

- [ ] **Step 3: 提交**

```bash
git add test/e2e/sdk_push_test.go
git commit -m "test(e2e): push 顺序锁用例——20 条同组严格按序且并发峰值恒为 1

并发峰值断言是顺序锁的确定性证据：4 线程 + 50ms 停留，只要 broker 放出
两条同队列顺序消息，重叠必被观测到。"
```

---

### Task 4: 用例 3 + 用例 4（fifo 欠账核心：重试与死信都归 broker）

**Files:**
- Modify: `test/e2e/sdk_push_test.go`

**Interfaces:**
- Consumes: Task 2 的 harness；`startBroker(t, mutate...)` 的 mutate 形态见 `TestOfficialGoSDKDLQ`。

**这两条合成一个 task**：它们证的是同一件事的两半（重试计数归 broker / 死信判定归 broker），共享 `decide` 恒返 `FAILURE` 的构造，且都要按 §3.4 的退避参数算窗口。

**退避事实（§3.4，写死在计划里，实现者不要重算）：** `retryBackoffBase = 10s`，`retryBackoffMax = 5min`，均为包私有变量；e2e 把 broker 当独立进程跑，**注不进小值**。所以第 2 次投递至少在第 1 次的 10 秒之后。

- [ ] **Step 1: 写用例 3**

追加到 `test/e2e/sdk_push_test.go`：

```go
// TestOfficialGoSDKPushRetryOwnedByBroker 用例 3：消费失败后由 broker 重投，
// 不是客户端本地循环重投 listener。
//
// 这是 fifo=false 的另一半证据：翻成 true 会让客户端改建 FiFoConsumeService，
// 消费失败转为客户端本地重投，重试计数就不再归 broker 了。
func TestOfficialGoSDKPushRetryOwnedByBroker(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-push-retry"
		group = "e2e-push-retry-g"
		body  = "retry-me"
	)
	sendPlain(t, endpoint, topic, body)

	c := newCollector(t)
	c.decide = func(*rmq.MessageView) rmq.ConsumerResult { return rmq.FAILURE }
	startPushConsumer(t, endpoint, group, topic, c)
	// 第 2 次投递的不可见窗口 = max(默认不可见 1m, 服务端退避下限 10s) = 1m，
	// 窗口取 120s 留足余量。
	waitCount(t, c, 2, 120*time.Second)

	snap := c.snapshot()
	first, second := snap[0], snap[1]
	// 3a：同一条消息被投了两次
	if first.id != second.id {
		t.Fatalf("两次不是同一条消息: %s vs %s", first.id, second.id)
	}
	if first.body != body || second.body != body {
		t.Fatalf("body 不符: %q / %q", first.body, second.body)
	}
	// 3b：投递次数递增
	if first.attempt != 1 || second.attempt != 2 {
		t.Fatalf("投递次数不符: %d → %d，期望 1 → 2", first.attempt, second.attempt)
	}
	// 3c：判别器 —— 回执句柄必须变。
	//
	// 3b 单独不足以判别：客户端本地重试同样会自增 deliveryAttempt
	// （eraseFifoMessage 里就是 mv.deliveryAttempt += 1），两条路的 attempt 都会涨。
	// 只有句柄能把它们分开——本地重试复用同一个 MessageView 反复喂 listener，
	// 句柄不变；只有真的回了 broker、broker 重新发件，才会拿到编着新 attempt
	// 的新句柄。
	if first.receipt == second.receipt {
		t.Fatalf("回执句柄未变（%q）：说明是客户端本地重投，不是 broker 重投", first.receipt)
	}
	t.Logf("用例 3 通过：attempt 1→2 且句柄已换，重试归 broker")
}
```

- [ ] **Step 2: 跑用例 3**

Run: `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPushRetryOwnedByBroker -v`
Expected: PASS

- [ ] **Step 3: 写用例 4（先补 import）**

先在 import 块加 `"github.com/xushixin/sq/internal/config"`（本用例的 `startBroker` mutate 是它的首个使用者），再追加到同文件：

```go
// TestOfficialGoSDKPushDLQOwnedByBroker 用例 4：投递超限后由 broker 转入
// %DLQ%{group}，listener 不再被调用。
//
// 限界（如实记录，不包装）：DLQ 条目本身带不出「谁转的」签名——
// ForwardMessageToDeadLetterQueue（RPC 路径，deliver.go:611）与 broker 自动超限
// 路径最终汇进同一个 moveToDLQ（deliver.go:671），产出完全一致。所以「DLQ 判定
// 归 broker」不是单条断言能证的，它由三件事合起来支撑：4a（listener 停止被调用）
// + 4b（消息确实在 DLQ）+ SDK 侧静态事实（标准消费服务的 eraseMessage 只有
// ack/nack 两条分支，根本没有 forward 调用）。第三条是读代码得来的，不是跑出来的。
func TestOfficialGoSDKPushDLQOwnedByBroker(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) {
		c.DefaultMaxAttempts = 2 // 2 次投递即超限，控制用例时长
	})
	const (
		topic = "e2e-push-dlq"
		group = "e2e-push-dlq-g"
		body  = "push-poison"
	)
	sendPlain(t, endpoint, topic, body)

	c := newCollector(t)
	c.decide = func(*rmq.MessageView) rmq.ConsumerResult { return rmq.FAILURE }
	startPushConsumer(t, endpoint, group, topic, c)
	waitCount(t, c, 2, 120*time.Second)

	// 4a：恰 2 次后不再被调用。
	//
	// 必须用【连续静默窗口】判定，不是「等一会儿看一眼」：看一眼恰好落在两次
	// 投递之间就会误判为已停止。连续 15 轮 × 1s 计数不变才算数。
	stable := 0
	for i := 0; i < 15; i++ {
		time.Sleep(time.Second)
		if c.count() == 2 {
			stable++
			continue
		}
		t.Fatalf("超限后 listener 仍被调用：第 %d 秒计数变为 %d；全量: %+v", i+1, c.count(), c.snapshot())
	}
	if stable != 15 {
		t.Fatalf("静默窗口未坐满: %d/15", stable)
	}

	// 4b：死信作为普通 topic 从 %DLQ%{group} 被读到。
	//
	// 转入是惰性的（原队列下一次 Receive 触发）——这里不需要像 SimpleConsumer 版
	// 那样手动「戳原 topic」：push 消费者一直挂着长轮询，戳的动作天然在发生。
	dlqTopic := "%DLQ%" + group
	dlqConsumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group + "-reader",
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{dlqTopic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer(DLQ): %v", err)
	}
	if err := dlqConsumer.Start(); err != nil {
		t.Fatalf("dlqConsumer.Start: %v", err)
	}
	defer dlqConsumer.GracefulStop()

	var gotBody string
	deadline := time.Now().Add(120 * time.Second)
	for gotBody == "" && time.Now().Before(deadline) {
		mvs, err := dlqConsumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 返回，属正常
		}
		for _, mv := range mvs {
			gotBody = string(mv.GetBody())
			if props := mv.GetProperties(); props["sq-origin-topic"] != topic {
				t.Fatalf("死信缺少来源属性 sq-origin-topic: %v", props)
			}
			if err := dlqConsumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack 死信: %v", err)
			}
		}
	}
	if gotBody != body {
		t.Fatalf("死信未到达或内容不符: %q", gotBody)
	}
	t.Logf("用例 4 通过：listener 恰 2 次后静默，消息在 %s 中", dlqTopic)
}
```

- [ ] **Step 4: 跑用例 4**

Run: `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPushDLQOwnedByBroker -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add test/e2e/sdk_push_test.go
git commit -m "test(e2e): push 重试与死信归属用例——回执句柄判别 + DLQ 静默窗口

用例 3 用回执句柄变化判别 broker 重投 vs 客户端本地重投（attempt 递增两条路
都会发生，单看它判不出）；用例 4 用连续 15s 静默窗口判定 listener 已停。"
```

---

### Task 5: 用例 2（长轮询唤醒）+ 用例 6（停机后 inflight 不丢）

**Files:**
- Modify: `test/e2e/sdk_push_test.go`

**Interfaces:**
- Consumes: Task 2 的 harness（含 `startPushConsumer` 返回的 `stop` 函数）；Task 1 的 `DefaultInvisibleDuration` 配置项。

**用例 6 的一条硬约束（务必先读，否则会写出死锁的测试）：** `GracefulStop()` **会无上限等待在途 listener 完成**。调用链是 `GracefulStop` → `consumerService.Shutdown()` → `simpleThreadPool.Shutdown()` → `tp.waitGroup.Wait()`（SDK `simple_thread_pool.go:86`）。`waitingReceiveRequestFinished` 那层确实有超时，线程池这层没有。**所以「让 listener 永久阻塞以保证不 ack」这个构造不可用**——它会让测试永久死锁。改用下面的「缓存未消费」构造。

- [ ] **Step 1: 写用例 2**

追加到 `test/e2e/sdk_push_test.go`：

```go
// TestOfficialGoSDKPushLongPollingWakeup 用例 2：消息到达即唤醒长轮询，
// 而不是慢周期轮询。
func TestOfficialGoSDKPushLongPollingWakeup(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-push-wakeup"
		group = "e2e-push-wakeup-g"
	)
	c := newCollector(t)
	startPushConsumer(t, endpoint, group, topic, c)

	// 探针先行：消费者 Start() 之后要走 Telemetry 协商 + QueryAssignment 才会挂上
	// 长轮询，这段耗时不确定。不用探针而直接「起消费者 → 睡 2s → 发消息 → 计时」，
	// 测的是「协商耗时 + 唤醒耗时」的和，协商慢一点就假红。探针到达后，
	// t0 之后长轮询确定已在挂着，量到的才是纯唤醒延迟。
	sendPlain(t, endpoint, topic, "probe")
	waitCount(t, c, 1, 30*time.Second)

	t0 := time.Now()
	sendPlain(t, endpoint, topic, "wakeup")
	waitCount(t, c, 2, 30*time.Second)
	elapsed := time.Since(t0)

	// 阈值 3s：若客户端退化成慢周期轮询，周期上限是 defaultLongPolling = 20s，
	// 唤醒延迟会落在秒级到 20s；3s 能把两者分开，同时对本机 e2e 抖动留足余量。
	//
	// 注意 elapsed 含一次 producer 建连 + Send 的往返，本机量级在几十毫秒。
	if elapsed >= 3*time.Second {
		t.Fatalf("唤醒延迟 %v ≥ 3s：疑似退化成周期轮询而非长轮询", elapsed)
	}
	if snap := c.snapshot(); snap[1].body != "wakeup" {
		t.Fatalf("第 2 条 body = %q，期望 wakeup", snap[1].body)
	}
	t.Logf("用例 2 通过：唤醒延迟 %v", elapsed)
}
```

- [ ] **Step 2: 跑用例 2**

Run: `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPushLongPollingWakeup -v`
Expected: PASS

- [ ] **Step 3: 写用例 6**

追加到同文件：

```go
// TestOfficialGoSDKPushInflightSurvivesStop 用例 6：消费者停机时，已投给客户端
// 但尚未进入 listener 的缓存消息不丢，会被重投给同组的另一个消费者。
//
// 构造说明：不能用「让 listener 永久阻塞以保证不 ack」——GracefulStop 会无上限
// 等待在途 listener 完成（simpleThreadPool.Shutdown → waitGroup.Wait），
// 那样写测试会永久死锁。这里改测更真实的停机丢失风险点：消息已被 broker 投给
// 客户端、缓存在 process queue 里，但还没轮到进 listener 就停机了。
func TestOfficialGoSDKPushInflightSurvivesStop(t *testing.T) {
	// 5s 不可见时长：不改的话走默认 1m，本用例要干等 60s+。
	endpoint := startBroker(t, func(c *config.Config) {
		c.DefaultInvisibleDuration = "5s"
	})
	const (
		topic = "e2e-push-stop"
		group = "e2e-push-stop-g"
		total = 10
	)
	bodies := make([]string, 0, total)
	for i := 0; i < total; i++ {
		bodies = append(bodies, fmt.Sprintf("stop-%d", i))
	}
	sendPlain(t, endpoint, topic, bodies...)

	// A：单线程 + 每条停留 2s，使它在短时间内只能消费掉少数几条，其余留在缓存里。
	cA := newCollector(t)
	cA.hold = 2 * time.Second
	_, stopA := startPushConsumer(t, endpoint, group, topic, cA,
		rmq.WithPushConsumptionThreadCount(1))
	waitCount(t, cA, 1, 30*time.Second)
	// GracefulStop 会等当前那条 listener 跑完（≤2s，有界）并 ack 它，然后关闭
	// 线程池——缓存里剩下的那些从未进入 listener，也从未被 ack。
	stopA()

	consumedByA := bodySet(cA.snapshot())
	// 6a：A 确实没消费完就停了。否则本用例什么都没测到，必须显式挡住。
	if len(consumedByA) >= total {
		t.Fatalf("A 在停机前消费了全部 %d 条，本用例未构造出未消费缓存", total)
	}
	t.Logf("A 停机前消费了 %d/%d 条", len(consumedByA), total)

	// B：同组同 topic，收剩下的。
	cB := newCollector(t)
	startPushConsumer(t, endpoint, group, topic, cB)
	remaining := make([]string, 0, total)
	for _, b := range bodies {
		if consumedByA[b] == 0 {
			remaining = append(remaining, b)
		}
	}
	waitCount(t, cB, len(remaining), 60*time.Second)

	// 6b：B 收到 A 未见过的全部剩余 body。
	//
	// 断言【包含】而非【相等】：A 停机时最后那条的 ack 若失败，B 会额外收到它，
	// 那不是缺陷。要挡的是「有消息丢了」，不是「有消息多投了一次」。
	gotB := bodySet(cB.snapshot())
	for _, b := range remaining {
		if gotB[b] == 0 {
			t.Fatalf("消息 %q 在 A 停机后丢失：B 未收到；B 收到 %v", b, gotB)
		}
	}

	// 6c：B 收到的每条都是重投件而不是新消息。
	//
	// 这条同时覆盖两条重投路径：SDK 在关闭时可能主动 nack 缓存中未消费的消息，
	// 也可能什么都不做、由 broker 在 5s 不可见期后重投。两条路都会让 attempt
	// 递增，断言对二者都成立——本用例不区分它们，也不应区分。
	for _, r := range cB.snapshot() {
		if r.attempt < 2 {
			t.Fatalf("B 收到的 %q attempt = %d，期望 ≥2（应为重投件）", r.body, r.attempt)
		}
	}
	t.Logf("用例 6 通过：A 停机后 %d 条未消费缓存全部由 B 收到", len(remaining))
}
```

- [ ] **Step 4: 跑用例 6**

Run: `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPushInflightSurvivesStop -v`
Expected: PASS

若 6a 挡住（A 消费完了全部 10 条），说明 `hold=2s` 不足以拖住——**正确修法是加大消息条数或加大 hold，不是删掉 6a**。6a 是防「用例空转」的护栏。

- [ ] **Step 5: 提交**

```bash
git add test/e2e/sdk_push_test.go
git commit -m "test(e2e): push 长轮询唤醒与停机不丢用例

用例 2 探针先行，把协商耗时从唤醒延迟里摘出去；用例 6 改用「缓存未消费」构造
——GracefulStop 会无上限等待在途 listener，永久阻塞式构造会死锁。"
```

---

### Task 6: `settings.go` fifo 注释订正 + 全量验收

**Files:**
- Modify: `internal/rpc/settings.go:117-121`

**Interfaces:**
- Consumes: Task 3/4 的用例结论（顺序锁 + 归属权），是本 task 注释文本的实证依据。

**这一步不改代码行为**：`fifo := false` 这行不动，只把注释从「待翻转的临时值」订正为「已验的终态」。

- [ ] **Step 1: 替换注释**

`internal/rpc/settings.go`，把这三行：

```go
			// M4 起顺序由 broker 端强制（队列级顺序锁），消费端无需协商关闭；
			// fifo 协商标志待 push 消费流程验证后（M5+）再翻转，
			// 当前保持显式下发 false（不能留空让客户端去猜）。
```

替换为：

```go
			// 显式下发 false，且这是终态，不是待翻转的临时值（B13.2 已验）。
			//
			// 两条理由，缺一不可：
			//  1. 顺序安全不依赖此标志：M4 起队列级顺序锁保证每队列至多一条
			//     未终结的顺序 inflight（deliver.go 顺序锁）。e2e
			//     TestOfficialGoSDKPushFIFOOrderLock 用 4 线程 push 消费 20 条
			//     同组消息，实测 listener 并发峰值恒为 1。
			//  2. 翻成 true 会夺走归属权：客户端会改建 FiFoConsumeService，消费
			//     失败转为【客户端本地循环重投 listener】，不回 broker——重试
			//     计数与死信判定都会从 broker 挪到客户端，与 sq 的设计相反。
			//     e2e TestOfficialGoSDKPushRetryOwnedByBroker /
			//     TestOfficialGoSDKPushDLQOwnedByBroker 证的就是当前归属在 broker。
			//
			// 仍必须显式下发而不是留空：留空让客户端去猜，协商结果不确定。
```

- [ ] **Step 2: 编译 + 单测**

Run: `go build ./... && go test -race -count=1 ./internal/rpc ./internal/config`
Expected: 全 PASS

- [ ] **Step 3: 全量 push e2e**

Run: `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPush -v`
Expected: 6/6 PASS。记录总耗时。

- [ ] **Step 4: 全量 e2e 回归（挡 Task 1 Step 10 的连带影响）**

Run: `cd test/e2e && go test -tags e2e -count=1 -timeout 60m`
Expected: 全 PASS。**这一步不可省**：Task 1 动了 `writeBrokerConfig`，它是所有 e2e 用例共用的 broker 配置构造器。

- [ ] **Step 5: 提交**

```bash
git add internal/rpc/settings.go
git commit -m "docs(rpc): fifo=false 注释订正为已验终态，附两条实证依据

原注释写「待 push 消费流程验证后再翻转」，方向是错的：翻成 true 会让客户端改建
FiFoConsumeService，把重试计数与死信判定从 broker 挪到客户端。B13.2 的 push e2e
证明顺序安全由队列级顺序锁保证，与该标志无关。"
```

---

## 验收标准

全部 task 完成后，以下三条必须同时成立（逐条附真实命令输出，不接受自述）：

| # | 命令 | 期望 |
|---|---|---|
| 1 | `go test -race -count=1 ./internal/config ./internal/rpc` | 两包全 PASS |
| 2 | `cd test/e2e && go test -tags e2e -count=1 -run TestOfficialGoSDKPush -v` | 6/6 PASS |
| 3 | `cd test/e2e && go test -tags e2e -count=1 -timeout 60m` | 全量 e2e 全 PASS |

---

## Self-Review

**1. Spec 覆盖核对**

| spec 章节 | 落到哪个 task |
|---|---|
| §4 用例 1 | Task 2 Step 3 |
| §4 用例 2 | Task 5 Step 1 |
| §4 用例 3 | Task 4 Step 1 |
| §4 用例 4 | Task 4 Step 3 |
| §4 用例 5 | Task 3 Step 1 |
| §4 用例 6 | Task 5 Step 3 |
| §5.1 采集器 | Task 2 Step 1 |
| §5.2 三 helper | Task 2 Step 2（多了 `count`/`bodySet` 两个小工具，属实现细节） |
| §5.3 四处承重设计 | 逐条落在 Task 2 的注释里（不 Fatalf / 入口采集 / 退避窗口 / 轮询等待） |
| §7.1 注释订正 | Task 6 Step 1 |
| §7.2 配置项 | Task 1 全部 |
| §8 另立条目 | 已记为 backlog B13.5 / B13.6，本计划不实现 |

无遗漏。

**2. 占位符扫描**：无 TBD / TODO / "similar to Task N" / 无代码的实现步骤。所有测试函数体完整。

**3. 类型一致性核对**

- `pushCollector` 字段名在 Task 2 定义为 `decide` / `hold`，Task 3/4/5 全部按此使用（spec §5.1 写的是 `handle`，因与 `recv.handle` 撞名，计划里统一改为 `decide`，`recv` 的对应字段改为 `receipt`）。
- `startPushConsumer` 在 Task 2 定义为返回 `(rmq.PushConsumer, func())`，Task 3 用 `startPushConsumer(...)` 单值上下文——**这在 Go 里非法**。已核对：Task 3 Step 1 写的是 `startPushConsumer(t, ...)` 作为**语句**（丢弃两个返回值），合法；Task 2 Step 3 的 1b 与 Task 5 用 `_, stopA :=` 接收。Task 4 两处均为语句形式。一致。
- `sendFifoBatch(t, endpoint, topic, mg, "fifo", total)` —— 第 4 参是 MessageGroup，与 `sdk_fifo_test.go:47` 的签名及既有调用一致。
- `config.Config` 字段名 `DefaultInvisibleDuration`（string）与访问器 `DefaultInvisible()`（time.Duration）在 Task 1 与 Task 5 的 mutate 中一致。
- **import 分期**：Task 2 的 import 块刻意不含 `fmt` 与 `internal/config`——那时还没有使用者，Go 会以编译错误拒绝。`fmt` 在 Task 3 Step 1 引入，`internal/config` 在 Task 4 Step 3 引入，两处都在 step 标题里写明。初稿曾把两者一次性写进 Task 2，会导致 Task 2 编译不过。

**4. 已知限界（不是缺陷，写下来避免被当成遗漏）**

- 用例 4 的「DLQ 判定归 broker」有一半靠 SDK 侧静态代码事实支撑，e2e 证不到，注释里已如实写明。
- 本计划不覆盖集群档、不覆盖 Java SDK、不实现 AutoRenew——均属 spec §2 明确的非目标。
