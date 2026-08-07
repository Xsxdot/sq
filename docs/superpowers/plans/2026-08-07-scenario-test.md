# 真实场景测试（scenario test）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建成第三层测试 `test/scenario/`：真实 broker 子进程 + 官方 RocketMQ Go SDK 多角色并发混跑普通/延时/FIFO 消息，运行期注入 kill -9 重启、优雅停机重启、并发 Admin 操作，收敛后按 10 条不变量全局对账。

**Architecture:** 场景 = 流量配方（演员）+ 故障时间线（条件触发调度器）+ 不变量集合（对账器），三者只通过「账本（Ledger）」与「Cluster/Node 接口」耦合。broker 拉起逻辑抽到 `test/internal/broker` 与 e2e 共享。收敛用排水模式（消费侧全员转「收到即 ack」持续拉取），不是停消费等归零——sq 重投是惰性的。

**Tech Stack:** Go 1.26、`github.com/apache/rocketmq-clients/golang/v5`（官方 SDK）、Admin HTTP API、`go test -race -tags scenario`。

**Spec:** `docs/superpowers/specs/2026-08-07-scenario-test-design.md`（本计划的唯一需求源，冲突时以 spec 为准）

## Global Constraints

- 所有 `test/scenario/` 文件带 `//go:build scenario` 标签，包名 `scenario`；`go test ./...` 与 `make e2e` 不得包含它。
- broker 二进制固定带 `-race` 编译（spec §2）；scenario 测试进程自身也用 `go test -race` 跑。
- 位点重置**只向后**（重置到当前 cursor 之前），绝不向前（spec §2/§6）。
- FIFO producer 纪律：某 seq 未 confirmed 就不推进（spec §5.1）。
- 收敛 = 排水模式（spec §7.1），不是停消费等归零。
- 随机性只来自单一种子 `SQ_SCENARIO_SEED`（缺省随机并在日志第一行打印）；演员/调度器各自从主种子派生独立 `*rand.Rand`，不共享（避免锁与串扰）。
- 测试框架内日志一律 `t.Logf` + 账本事件流，**禁止** `fmt.Printf`/`print`；broker 侧日志由被测进程自己写（LogLevel=debug）。
- 注释规范遵循全局 CLAUDE.md §2：新文件顶部职责+边界，导出函数 doc comment，非显然逻辑写「为什么」。测试文件同样适用（参照 test/e2e/sdk_test.go 的注释密度）。
- SDK 全局前置（TestMain，抄 e2e）：`rocketmq.client.logRoot` 指临时目录 + `rmq.ResetLogger()`、`rmq.EnableSsl = false`。
- Admin API 的请求/响应形态以 `internal/admin/*.go` 的 struct 为唯一事实源；落码前打开对应 handler 核对字段名。

---

### Task 1: 抽取共享 broker helper `test/internal/broker`

把 e2e 里「编译/写配置/拉起/就绪等待/停机」五件套抽成无 build tag 的共享包，e2e 改为薄封装调用。scenario 的 Harness（Task 3）建立在它上面。

**Files:**
- Create: `test/internal/broker/broker.go`
- Modify: `test/e2e/sdk_test.go:88-396`（helper 区改为委托共享包；常量与断言语义留在 e2e）
- Test: 现有 e2e 套件就是本任务的回归测试（行为不变的纯搬移）

**Interfaces:**
- Consumes: `internal/config.Config`（yaml 序列化配置）
- Produces（Task 3 依赖，签名固定）:
  - `broker.RepoRoot() (string, error)`
  - `broker.Build(out string, race bool) ([]byte, error)` — race=true 加 `-race` 编译参数
  - `broker.PickPort(preferred int) (int, error)`
  - `broker.WriteConfig(dir string, preferred int, mutate ...func(*config.Config)) (cfgPath, endpoint string, err error)`
  - `broker.Launch(bin, cfgPath, endpoint, logPath string, startTimeout time.Duration) (*broker.Handle, error)`
  - `(*Handle).Alive() bool` / `(*Handle).Kill() error`（SIGKILL+等待）/ `(*Handle).Term(hardTimeout time.Duration) (elapsed time.Duration, waitErr error, timedOut bool)`（SIGTERM+等待，不做断言）/ `(*Handle).ExitedEarly() (error, bool)`（非阻塞探测）/ `(*Handle).Close()`（关日志文件）
  - `Handle` 导出字段：`Endpoint, CfgPath, LogPath string`

- [ ] **Step 1: 建共享包，搬移并去 testing 化**

新建 `test/internal/broker/broker.go`。把 `test/e2e/sdk_test.go` 中 `repoRoot`、`buildBroker`、`pickPort`、`writeBrokerConfig`、`launchBroker`、`waitBrokerReady`、`stopBroker` 的**逻辑**搬入，做三处改造（其余逐行保留，包括全部中文注释）：

1. 去掉 `*testing.T`：所有函数返回 `error`，`t.Fatalf` 改为 `fmt.Errorf`（保留原错误文案）；
2. `Build` 增加 `race bool` 参数：`args := []string{"build"}; if race { args = append(args, "-race") }; args = append(args, "-o", out, "./cmd/sq")`；
3. `stopBroker` 拆成无断言的 `(*Handle).Term(hardTimeout)`：发 SIGTERM → select 等 `waitDone` 或 `hardTimeout` 超时（超时则 `Kill()` 且 `timedOut=true`）→ 返回 `(耗时, waitErr, timedOut)`。**退出码/预算断言不进共享包**——e2e 与 scenario 的断言口径不同（e2e 当场 Errorf，scenario 记账本由对账器裁决）。

`Handle` 结构（导出字段 + 内部件）：

```go
// Handle 一个已启动的 broker 进程及其现场。
// CfgPath 保留是刻意的：同一份配置可喂给 Launch 多次——重启恢复类场景
// 靠「停掉上一代、用同一配置拉起下一代」验证跨进程持久化。
type Handle struct {
	Endpoint string
	CfgPath  string
	LogPath  string

	cmd      *exec.Cmd
	logFile  *os.File
	waitDone chan error
}

// Alive 非阻塞探测进程是否仍在运行。
func (h *Handle) Alive() bool {
	select {
	case err := <-h.waitDone:
		// 已退出：把结果放回 channel，让后续 Term/Kill 仍能读到
		h.waitDone <- err
		return false
	default:
		return true
	}
}

// Kill 发 SIGKILL 并等待进程回收。kill -9 场景专用：不走任何优雅路径。
func (h *Handle) Kill() error {
	if err := h.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("SIGKILL 失败: %w", err)
	}
	<-h.waitDone // SIGKILL 后必然退出，等待即可
	return nil
}
```

注意 `waitDone` 用容量 1 的 channel（原 e2e 即如此），`Alive` 读出后要放回。

- [ ] **Step 2: e2e 改为薄封装**

`test/e2e/sdk_test.go` 中：

- `TestMain` 里 `buildBroker(brokerBinary)` 改为 `broker.Build(brokerBinary, false)`（e2e 不带 race，行为不变）；
- `writeBrokerConfig(t, mutate...)` 保留原签名，内部调 `broker.WriteConfig(t.TempDir(), preferredPort, mutate...)` 并把 error 转 `t.Fatalf`；
- `launchBroker(t, ...)` 保留原签名，内部调 `broker.Launch(brokerBinary, cfgPath, endpoint, logPath, brokerStartTimeout)`；`brokerHandle` 改为包一层 `*broker.Handle`；
- `stopBroker` 保留原有全部断言文案（退出码 0、`brokerStopBudget` 预算、45s 强杀），内部调 `h.Term(brokerStopTimeout)` 取回 `(elapsed, waitErr, timedOut)` 再断言；
- 其余 e2e 文件（sdk_delay/fifo/recovery/auth/admin/console）不动——它们只用 `startBroker`/`writeBrokerConfig`/`launchBroker` 这几个 e2e 内封装。

- [ ] **Step 3: 编译验证**

Run: `go vet ./... && go build ./...`
Expected: 通过（共享包无 build tag，必须在无 tag 下也编译干净）

- [ ] **Step 4: e2e 回归**

Run: `make e2e`
Expected: 全部 PASS（纯搬移不改行为；任何失败都说明搬移走样，回头对照原代码修）

- [ ] **Step 5: 注释自检**

共享包文件头写清职责与边界（「只管进程生命周期与配置写盘；不做任何断言——断言口径归各测试层自己」）；`Build/WriteConfig/Launch/Term/Kill/Alive` 全部有 doc comment（参数、返回、陷阱——如 waitDone 放回语义）。

- [ ] **Step 6: Commit**

```bash
git add test/internal/broker/broker.go test/e2e/sdk_test.go
git commit -m "refactor(test): 抽取共享 broker 进程 helper，e2e 改为薄封装"
```

---

### Task 2: scenario 骨架——Profile、约束校验、TestMain、Makefile

**Files:**
- Create: `test/scenario/profile.go`
- Create: `test/scenario/profile_test.go`
- Create: `test/scenario/main_test.go`
- Modify: `Makefile`（新增 scenario target）

**Interfaces:**
- Produces（后续所有任务依赖）:
  - `type Profile struct`（字段见下）
  - `ShortProfile() Profile` / `LoadProfile() (Profile, error)`（读 `SQ_SCENARIO_DURATION`/`SQ_SCENARIO_SEED`）
  - `(Profile) Validate() error`（spec §3.1 约束表）
  - `backoffSum(maxAttempts int32) time.Duration` / `maxBackoff(maxAttempts int32) time.Duration`
  - 包级 `var brokerBin string`（TestMain 编译产物路径）

- [ ] **Step 1: 写失败测试（约束表）**

`test/scenario/profile_test.go`：

```go
//go:build scenario

package scenario

import (
	"testing"
	"time"
)

// TestShortProfileValid 短档默认值必须自洽——这是 §3.1 约束表的第一道防线：
// 改任何默认值时，这里立刻告诉你改坏了哪条不等式。
func TestShortProfileValid(t *testing.T) {
	if err := ShortProfile().Validate(); err != nil {
		t.Fatalf("短档默认 Profile 未通过约束校验: %v", err)
	}
}

// TestValidateCatchesBadProfiles 每条约束都要能独立拦截违规值。
func TestValidateCatchesBadProfiles(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Profile)
	}{
		{"max_attempts 过大", func(p *Profile) { p.MaxAttempts = 16 }},
		{"awaitDuration 超 3s", func(p *Profile) { p.AwaitDuration = 10 * time.Second }},
		{"延时窗口超过流量期", func(p *Profile) { p.DelayWindowMax = p.TrafficDuration + time.Second }},
		{"DLQ deadline 塞不下退避", func(p *Profile) { p.EventDeadline = 5 * time.Second }},
		{"收敛超时不够排水", func(p *Profile) { p.DrainTimeout = time.Second }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := ShortProfile()
			tc.mutate(&p)
			if err := p.Validate(); err == nil {
				t.Fatalf("期望校验失败，却通过了")
			}
		})
	}
}

// TestBackoffSum 与 internal/core/deliver 的硬编码退避（10s 起 ×2 封顶 5min）对表。
func TestBackoffSum(t *testing.T) {
	if got := backoffSum(2); got != 10*time.Second {
		t.Fatalf("backoffSum(2) = %v, 期望 10s", got)
	}
	if got := backoffSum(3); got != 30*time.Second {
		t.Fatalf("backoffSum(3) = %v, 期望 30s (10+20)", got)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -race -tags scenario -run 'TestShortProfile|TestValidate|TestBackoff' ./test/scenario/`
Expected: FAIL（undefined: ShortProfile 等）

- [ ] **Step 3: 实现 profile.go**

```go
//go:build scenario

// Package scenario 真实场景测试：多角色并发混跑 + 故障注入 + 全局对账。
//
// 职责：
//   - 混跑普通/延时/FIFO 流量，注入 kill -9/优雅停机/Admin 操作
//   - 排水收敛后按 spec §7.2 的 10 条不变量对账
//
// 边界：
//   - 只面向 Cluster/Node 接口，不假设单机拓扑（M7 docker、v2 集群走新实现）
//   - 不测事务消息（M6 未实现）、不触发磁盘水位、不动 retention
package scenario

// Profile 一次场景运行的全部规模参数。短档/长档只是两组值，场景代码唯一。
type Profile struct {
	Seed            int64
	TrafficDuration time.Duration // 流量期
	DrainTimeout    time.Duration // 排水收敛上限
	EventDeadline   time.Duration // 条件事件（DLQ 重发等）的前置满足 deadline，相对流量期起点

	NormalProducers int
	DelayProducers  int
	FIFOProducers   int
	FIFOGroups      int           // 每个 FIFO producer 的 MessageGroup 数
	SendInterval    time.Duration // 每 producer 相邻两次发送的基础间隔（叠加抖动）

	DelayWindowMin time.Duration
	DelayWindowMax time.Duration

	MaxAttempts    int32         // broker default_max_attempts 覆写（必须 2–3，见 Validate）
	ChaosInvisible time.Duration // 捣乱组 Receive invisibleDuration
	AwaitDuration  time.Duration // 所有消费者长轮询上限
	StopBudget     time.Duration // 优雅停机耗时上界（不变量 10）

	KillMin, GracefulMin, ResetMin, ResendMin, NewTopicMin int // 每类事件最少次数（结束期校验）
	EventMinGap time.Duration // 相邻两个注入事件的最小间隔
}

// ShortProfile 短档默认值：~2 分钟流量期，日常回归可跑。
// 每个数值都被 Validate 的不等式钉住，改动前先看约束表（spec §3.1）。
func ShortProfile() Profile {
	return Profile{
		TrafficDuration: 2 * time.Minute,
		DrainTimeout:    2 * time.Minute,
		EventDeadline:   90 * time.Second,
		NormalProducers: 2, DelayProducers: 1, FIFOProducers: 1, FIFOGroups: 3,
		SendInterval:   200 * time.Millisecond, // -race 下 broker 慢 2–10 倍，速率保守
		DelayWindowMin: 2 * time.Second, DelayWindowMax: 30 * time.Second,
		MaxAttempts: 2, ChaosInvisible: 3 * time.Second,
		AwaitDuration: 3 * time.Second, StopBudget: 5 * time.Second,
		KillMin: 1, GracefulMin: 1, ResetMin: 1, ResendMin: 1, NewTopicMin: 1,
		EventMinGap: 8 * time.Second,
	}
}

// LoadProfile 从环境变量装配 Profile：
//   - SQ_SCENARIO_DURATION：流量期时长（Go duration），缺省用短档
//   - SQ_SCENARIO_SEED：随机种子，缺省用纳秒时钟
// 长档只放大时长与事件次数下限，速率类参数不放大——浸泡的价值在时间跨度，
// 不在把 broker 打满。
func LoadProfile() (Profile, error)

// Validate spec §3.1 参数自洽约束表。任一不等式不成立即返回错误，
// 错误文案必须点名是哪条约束、当前值多少、界在哪里——这是「参数不自洽」
// 与「环境抖动」两类失败的分界线。
func (p Profile) Validate() error
```

`Validate` 实现的约束（逐条 if，错误文案含实际值）：

```go
func (p Profile) Validate() error {
	if p.MaxAttempts < 2 || p.MaxAttempts > 3 {
		return fmt.Errorf("MaxAttempts=%d 越界：必须 2–3（退避硬编码 10s×2，16 次需 ~55 分钟才进 DLQ）", p.MaxAttempts)
	}
	if p.AwaitDuration > 3*time.Second {
		return fmt.Errorf("AwaitDuration=%v 超 3s：GracefulStop 等在途长轮询，StopBudget=%v 断言会 flake", p.AwaitDuration, p.StopBudget)
	}
	if p.DelayWindowMax >= p.TrafficDuration {
		return fmt.Errorf("DelayWindowMax=%v ≥ 流量期 %v：clamp 后延时消息全部挤在尾部", p.DelayWindowMax, p.TrafficDuration)
	}
	// DLQ 事件前置：捣乱组一条消息走完全部投递需 invisible + Σ退避，再留 10s 余量
	need := p.ChaosInvisible + backoffSum(p.MaxAttempts) + 10*time.Second
	if p.EventDeadline < need || p.EventDeadline > p.TrafficDuration {
		return fmt.Errorf("EventDeadline=%v 不满足 %v(=invisible+Σ退避+余量) ≤ deadline ≤ 流量期 %v", p.EventDeadline, need, p.TrafficDuration)
	}
	// 排水收敛：最晚 deliveryAt(=流量期尾) 之后还可能压着一轮最大退避
	drainNeed := p.DelayWindowMax + maxBackoff(p.MaxAttempts) + 30*time.Second
	if p.DrainTimeout < drainNeed {
		return fmt.Errorf("DrainTimeout=%v < %v(=DelayWindowMax+最大退避档+余量)", p.DrainTimeout, drainNeed)
	}
	if p.SendInterval < 100*time.Millisecond {
		return fmt.Errorf("SendInterval=%v < 100ms：broker 带 -race，速率过高变成纯背压测试", p.SendInterval)
	}
	return nil
}

// backoffSum 一条消息从首投到第 maxAttempts 次投递之间全部重投退避之和。
// 与 internal/core/deliver 硬编码一致：10s 起、每次 ×2、封顶 5min。
func backoffSum(maxAttempts int32) time.Duration {
	total, d := time.Duration(0), 10*time.Second
	for i := int32(1); i < maxAttempts; i++ {
		total += d
		if d *= 2; d > 5*time.Minute {
			d = 5 * time.Minute
		}
	}
	return total
}

// maxBackoff 第 maxAttempts 次投递前的单次最大退避档。
func maxBackoff(maxAttempts int32) time.Duration {
	d := 10 * time.Second
	for i := int32(2); i < maxAttempts; i++ {
		if d *= 2; d > 5*time.Minute {
			d = 5 * time.Minute
		}
	}
	return d
}
```

`main_test.go`（TestMain，抄 e2e 的 SDK 前置 + race 编译）：

```go
//go:build scenario

package scenario

// brokerBin TestMain 编译出的 -race broker 二进制，全部场景共用。
var brokerBin string

// TestMain 前置：SDK 日志改道临时目录 + 关 TLS（同 e2e，见 test/e2e/sdk_test.go
// TestMain 的注释——两项都必须在任何 SDK 客户端构造前完成）；broker 带 -race
// 编译（不变量 10「无 DATA RACE」的前提，spec §2）。
func TestMain(m *testing.M) {
	logRoot, err := os.MkdirTemp("", "sq-scenario-rmq-log-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 SDK 日志临时目录失败: %v\n", err)
		os.Exit(1)
	}
	os.Setenv("rocketmq.client.logRoot", logRoot)
	rmq.ResetLogger()
	rmq.EnableSsl = false

	buildDir, err := os.MkdirTemp("", "sq-scenario-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建构建临时目录失败: %v\n", err)
		os.Exit(1)
	}
	brokerBin = filepath.Join(buildDir, "sq")
	if out, err := broker.Build(brokerBin, true); err != nil {
		fmt.Fprintf(os.Stderr, "编译 -race broker 失败: %v\n%s\n", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(buildDir)
	os.RemoveAll(logRoot)
	os.Exit(code)
}
```

Makefile 新增（`.PHONY` 行同步补 `scenario`）：

```make
# 场景混跑测试：短档 ~3 分钟；SQ_SCENARIO_DURATION=30m 升长档浸泡
scenario:
	go test -race -tags scenario -count=1 -timeout 60m -v ./test/scenario/...
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -race -tags scenario -run 'TestShortProfile|TestValidate|TestBackoff' ./test/scenario/`
Expected: PASS

- [ ] **Step 5: 日志与注释自检**

`LoadProfile` 里种子与最终 Profile 用 `t.Logf` 打不出来（不在测试上下文）——改为由调用方（Task 10 主场景）打印；本任务确认 profile.go 文件头、每个导出符号 doc comment、Validate 每条约束的中文「为什么」注释齐全。

- [ ] **Step 6: Commit**

```bash
git add test/scenario/profile.go test/scenario/profile_test.go test/scenario/main_test.go Makefile
git commit -m "test(scenario): Profile 与 §3.1 约束表校验、TestMain 骨架、make scenario"
```

---

### Task 3: Harness——Cluster/Node 接口与 ProcessCluster

**Files:**
- Create: `test/scenario/cluster.go`
- Test: `test/scenario/cluster_test.go`

**Interfaces:**
- Consumes: Task 1 的 `broker.*`、Task 2 的 `Profile`/`brokerBin`
- Produces（Task 5/8/10 依赖）:
  - `type Cluster interface { Start(ctx context.Context) error; Nodes() []Node; Endpoint() string; AdminEndpoint() string; Close() }`
  - `type Node interface { Kill() error; Stop() (time.Duration, error); Restart() error; Alive() bool; LogPaths() []string }`
  - `NewProcessCluster(t *testing.T, prof Profile) *ProcessCluster`（实现 Cluster；`Nodes()[0]` 实现 Node）

- [ ] **Step 1: 写失败测试**

`cluster_test.go`：

```go
//go:build scenario

package scenario

// TestProcessClusterLifecycle 单节点进程集群的完整生命周期：
// 起 → kill -9 → 重启（同数据目录）→ 优雅停 → 重启 → Close。
// 这是 Task 8 故障调度器全部进程类事件的地基。
func TestProcessClusterLifecycle(t *testing.T) {
	c := NewProcessCluster(t, ShortProfile())
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Close()
	n := c.Nodes()[0]
	if !n.Alive() {
		t.Fatal("启动后节点应存活")
	}
	if err := n.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if n.Alive() {
		t.Fatal("SIGKILL 后节点不应存活")
	}
	if err := n.Restart(); err != nil {
		t.Fatalf("kill 后 Restart: %v", err)
	}
	elapsed, err := n.Stop()
	if err != nil {
		t.Fatalf("优雅停机: %v", err)
	}
	t.Logf("优雅停机耗时 %v", elapsed)
	if err := n.Restart(); err != nil {
		t.Fatalf("优雅停机后 Restart: %v", err)
	}
	// 三代进程 = 三个独立日志文件（不变量 10 逐文件扫 panic/DATA RACE）
	if got := len(n.LogPaths()); got != 3 {
		t.Fatalf("期望 3 个日志文件（三代进程），得到 %d", got)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -race -tags scenario -run TestProcessClusterLifecycle ./test/scenario/`
Expected: FAIL（undefined: NewProcessCluster）

- [ ] **Step 3: 实现 cluster.go**

```go
//go:build scenario

package scenario

// Cluster 场景代码看到的「集群」。v1 只有单机进程实现；M7 docker、v2 三节点
// Raft 走新实现，场景与对账代码不动（spec §4）。
type Cluster interface {
	Start(ctx context.Context) error
	Nodes() []Node
	Endpoint() string      // SDK 接入点
	AdminEndpoint() string // Admin HTTP 接入点
	Close()
}

// Node 可被故障注入的单个节点。
type Node interface {
	Kill() error                  // SIGKILL，不走优雅路径
	Stop() (time.Duration, error) // SIGTERM 优雅停机，返回耗时；断言归对账器
	Restart() error               // 用同一配置/数据目录拉起下一代进程
	Alive() bool
	LogPaths() []string // 每代进程一个日志文件，按代序排列
}

// ProcessCluster 单机子进程实现。
type ProcessCluster struct {
	t       *testing.T
	prof    Profile
	dir     string // 配置/数据/日志的根（t.TempDir）
	cfgPath string
	grpcEP  string
	adminEP string

	mu       sync.Mutex
	handle   *broker.Handle
	gen      int      // 进程代数，从 1 起——日志文件名用它区分
	logPaths []string
}

const clusterStartTimeout = 30 * time.Second

// NewProcessCluster 选两个端口（gRPC + admin）、写好配置。此时进程未启动。
func NewProcessCluster(t *testing.T, prof Profile) *ProcessCluster {
	t.Helper()
	dir := t.TempDir()
	adminPort, err := broker.PickPort(18082)
	if err != nil {
		t.Fatalf("探测 admin 端口失败: %v", err)
	}
	cfgPath, grpcEP, err := broker.WriteConfig(dir, 18081, func(c *config.Config) {
		c.DefaultMaxAttempts = prof.MaxAttempts // §3.1：DLQ 链路在测试时长内可达的关键旋钮
		c.AdminListen = fmt.Sprintf("127.0.0.1:%d", adminPort)
	})
	if err != nil {
		t.Fatalf("写 broker 配置失败: %v", err)
	}
	return &ProcessCluster{t: t, prof: prof, dir: dir, cfgPath: cfgPath,
		grpcEP: grpcEP, adminEP: fmt.Sprintf("127.0.0.1:%d", adminPort)}
}

func (c *ProcessCluster) Start(ctx context.Context) error { return c.launch() }
func (c *ProcessCluster) Endpoint() string                { return c.grpcEP }
func (c *ProcessCluster) AdminEndpoint() string           { return c.adminEP }
func (c *ProcessCluster) Nodes() []Node                   { return []Node{(*processNode)(c)} }

// launch 拉起下一代进程。gen 自增、日志文件独立——失败时能分清哪一代干的。
func (c *ProcessCluster) launch() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++
	logPath := filepath.Join(c.dir, fmt.Sprintf("broker-gen%d.log", c.gen))
	h, err := broker.Launch(brokerBin, c.cfgPath, c.grpcEP, logPath, clusterStartTimeout)
	if err != nil {
		return fmt.Errorf("拉起第 %d 代 broker 失败: %w", c.gen, err)
	}
	c.handle = h
	c.logPaths = append(c.logPaths, logPath)
	c.t.Logf("broker 第 %d 代已就绪 endpoint=%s admin=%s", c.gen, c.grpcEP, c.adminEP)
	return nil
}

func (c *ProcessCluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle != nil && c.handle.Alive() {
		// 收尾用 Term 而不是 Kill：让最后一代也走一次优雅路径，
		// 但此处不断言耗时——对账器只裁决调度器主动注入的那几次
		if _, _, timedOut := c.handle.Term(45 * time.Second); timedOut {
			c.t.Errorf("Close 时 broker 45s 未响应 SIGTERM，已强杀")
		}
	}
	if c.handle != nil {
		c.handle.Close()
	}
}

// processNode Node 视图：与 ProcessCluster 同体（v1 单节点）。
type processNode ProcessCluster

func (n *processNode) Alive() bool { ... 加锁读 handle.Alive() ... }
func (n *processNode) Kill() error { ... 加锁，handle.Kill()，t.Logf("已 SIGKILL 第 %d 代", gen) ... }
func (n *processNode) Stop() (time.Duration, error) {
	// Term 返回三元组；timedOut 或 waitErr 非空都折成 error 上报，
	// 耗时照常返回——对账器要用它比对 StopBudget
}
func (n *processNode) Restart() error { return (*ProcessCluster)(n).launch() }
func (n *processNode) LogPaths() []string { ... 加锁返回副本 ... }
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -race -tags scenario -run TestProcessClusterLifecycle ./test/scenario/`
Expected: PASS（约 15–30s：三次 -race broker 启动）

- [ ] **Step 5: 日志与注释自检**

关键节点日志已含：每代启动（gen+端点）、Kill、Stop 耗时（在 Node 实现里 `t.Logf`）。确认文件头、接口与每个导出方法 doc comment、`processNode` 同体转换的「为什么」注释齐全。

- [ ] **Step 6: Commit**

```bash
git add test/scenario/cluster.go test/scenario/cluster_test.go
git commit -m "test(scenario): Cluster/Node 抽象与单机进程实现"
```

---

### Task 4: 载荷编解码、三态分类与账本

**Files:**
- Create: `test/scenario/payload.go`
- Create: `test/scenario/ledger.go`
- Test: `test/scenario/ledger_test.go`

**Interfaces:**
- Produces（Task 6/7/8/9 依赖）:
  - `type Payload struct { ActorID string; Seq uint64; Group string; SentAtMs int64; DeliverAtMs int64 }`，`(Payload) Encode() []byte`、`DecodePayload([]byte) (Payload, error)`、`(Payload) Key() string`（`actorID/seq`，账本匹配副通道）
  - `type SendState int`：`SendConfirmed`/`SendIndeterminate`/`SendFailed`；`ClassifySendErr(error) SendState`
  - `TagMatches(expr, tag string) bool`
  - `type SendRecord / DeliveryRecord / Event / Subscription struct`（字段见下）
  - `NewLedger() *Ledger`；方法 `RecordSend/RecordDelivery/RecordEvent/RegisterSub/Snapshot`
  - `type LedgerSnapshot struct { Sends []SendRecord; Deliveries []DeliveryRecord; Events []Event; Subs map[string]Subscription }`

- [ ] **Step 1: 写失败测试**

`ledger_test.go` 核心用例（全部纯内存，无 broker）：

```go
//go:build scenario

package scenario

// TestPayloadRoundtrip 载荷是 crash 后对账的唯一凭据，编解码必须无损。
func TestPayloadRoundtrip(t *testing.T) {
	p := Payload{ActorID: "fifo-0", Seq: 42, Group: "g1", SentAtMs: 1700000000000, DeliverAtMs: 1700000030000}
	got, err := DecodePayload(p.Encode())
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if got != p {
		t.Fatalf("roundtrip 不一致: %+v != %+v", got, p)
	}
}

// TestClassifySendErr 三态分类表（spec §5.2）：
// 服务端业务码 → failed；其余一律 indeterminate——宁可漏一个 failed 检查，
// 不能把传输错误误判成 failed 在对账里制造假阳性。
func TestClassifySendErr(t *testing.T) {
	if got := ClassifySendErr(nil); got != SendConfirmed {
		t.Fatalf("nil → %v, 期望 confirmed", got)
	}
	// 官方 SDK 服务端业务码的载体（落码前 go doc 核对确切类型名与字段）
	if got := ClassifySendErr(&rmq.ErrRpcStatus{Code: 40402, Message: "TOPIC_NOT_FOUND"}); got != SendFailed {
		t.Fatalf("业务码 → %v, 期望 failed", got)
	}
	if got := ClassifySendErr(status.Error(codes.Unavailable, "connection refused")); got != SendIndeterminate {
		t.Fatalf("Unavailable → %v, 期望 indeterminate", got)
	}
	if got := ClassifySendErr(context.DeadlineExceeded); got != SendIndeterminate {
		t.Fatalf("DeadlineExceeded → %v, 期望 indeterminate", got)
	}
}

// TestTagMatches sq 只支持 "*"/单 tag/"a || b" 三种订阅形态（README）。
func TestTagMatches(t *testing.T) {
	cases := []struct {
		expr, tag string
		want      bool
	}{
		{"*", "x", true}, {"", "x", true},
		{"a", "a", true}, {"a", "b", false},
		{"a || b", "b", true}, {"a || b", "c", false},
	}
	for _, c := range cases {
		if got := TagMatches(c.expr, c.tag); got != c.want {
			t.Errorf("TagMatches(%q,%q)=%v 期望 %v", c.expr, c.tag, got, c.want)
		}
	}
}

// TestLedgerConcurrentRecord 账本是全部演员共享的热点，-race 下并发写必须干净。
func TestLedgerConcurrentRecord(t *testing.T) {
	l := NewLedger()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				l.RecordSend(SendRecord{Payload: Payload{ActorID: fmt.Sprintf("a%d", n), Seq: uint64(j)}, State: SendConfirmed})
				l.RecordDelivery(DeliveryRecord{ConsumerGroup: "g", Payload: Payload{ActorID: fmt.Sprintf("a%d", n), Seq: uint64(j)}})
			}
		}(i)
	}
	wg.Wait()
	s := l.Snapshot()
	if len(s.Sends) != 800 || len(s.Deliveries) != 800 {
		t.Fatalf("并发记录丢条目: sends=%d deliveries=%d", len(s.Sends), len(s.Deliveries))
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -race -tags scenario -run 'TestPayload|TestClassify|TestTag|TestLedger' ./test/scenario/`
Expected: FAIL（undefined）

- [ ] **Step 3: 实现 payload.go 与 ledger.go**

payload.go：`Encode` 用 `json.Marshal`（错不可能发生，忽略）；`DecodePayload` 严格校验 `ActorID != ""`（收到非本场景消息说明 topic 被污染，对账器按不变量 2 处理）。

ledger.go 结构与方法：

```go
// SendRecord 发送侧一次尝试的结果。FIFO 演员同 seq 重试时会产生多条记录
// （若干 indeterminate + 最后一条 confirmed），对账器按 Payload.Key 归并。
type SendRecord struct {
	Payload Payload
	Topic   string
	Tag     string
	Keys    []string
	State   SendState
	MsgID   string // 仅 confirmed 有
	Err     string
	At      time.Time
}

// DeliveryRecord 消费侧一次投递（含 ack 结果；未尝试 ack 时 AckAttempted=false）。
type DeliveryRecord struct {
	ConsumerGroup string
	Topic         string
	MsgID         string
	Payload       Payload
	Attempt       int32
	At            time.Time
	AckAttempted  bool
	AckOK         bool
	AckErr        string
}

// Event 故障调度器的一次注入（kill/restart/graceful/reset-cursor/dlq-resend/new-topic）
// 与运行阶段切换（drain-start 等）。Detail 里放事件专属坐标：
// reset-cursor 存 group/topic/queue/old_cursor/new_offset（不变量 7 的判定依据），
// dlq-resend 存 group/msg_id（不变量 3/6 的豁免名单），graceful 存 elapsed_ms/exit_err。
type Event struct {
	At     time.Time
	Kind   string
	Detail map[string]string
}

type Subscription struct {
	Topic string
	Expr  string
}

// Ledger 全场唯一事实源。互斥锁足够：写入频率（百/秒级）远低于锁竞争阈值，
// 不值得为它上分片或 lock-free。
type Ledger struct {
	mu         sync.Mutex
	sends      []SendRecord
	deliveries []DeliveryRecord
	events     []Event
	subs       map[string]Subscription
}
```

`Snapshot()` 深拷贝切片与 map（对账期无人再写，但拷贝换来「快照后随便排序分组」的自由）。

- [ ] **Step 4: 核对 SDK 错误类型后跑测试**

先 `go doc github.com/apache/rocketmq-clients/golang/v5.ErrRpcStatus` 核对类型名与字段；对不上就以 go doc 结果修正 ClassifySendErr 与测试里的构造。
Run: `go test -race -tags scenario -run 'TestPayload|TestClassify|TestTag|TestLedger' ./test/scenario/`
Expected: PASS

- [ ] **Step 5: 注释自检**

两个新文件头职责+边界；`ClassifySendErr` 的「宁可 indeterminate 不可误判 failed」、`Ledger` 锁选型、`Event.Detail` 各事件坐标约定——这三处「为什么」注释必须在。

- [ ] **Step 6: Commit**

```bash
git add test/scenario/payload.go test/scenario/ledger.go test/scenario/ledger_test.go
git commit -m "test(scenario): 载荷自描述、发送三态分类与并发安全账本"
```

---

### Task 5: Admin 客户端

**Files:**
- Create: `test/scenario/adminclient.go`
- Test: `test/scenario/adminclient_test.go`

**Interfaces:**
- Consumes: Task 3 `ProcessCluster`
- Produces（Task 7/8/9 依赖）:
  - `NewAdminClient(adminEndpoint string) *AdminClient`
  - `(c) CreateTopic(name string, queues uint32) error` — POST /admin/topics
  - `(c) GroupDetail(name string) (GroupDetail, error)` — GET /admin/groups/{name}；`GroupDetail{Name string; MaxAttempts int32; Topics []TopicProgress}`，`TopicProgress{Topic string; Queues []QueueProgress}`，`QueueProgress{QueueID uint32; Cursor, NextOffset, Pending uint64; Inflight int}`（字段名对齐 internal/admin/groups.go 的 json tag）
  - `(c) ResetCursor(group, topic string, queueID uint32, offset uint64) error` — POST /admin/groups/{group}/reset-cursor，body `{"topic":..,"queue_id":..,"offset":..}`
  - `(c) DLQResend(group string, queueID uint32, offset uint64) (msgID string, err error)` — POST /admin/dlq/{group}/resend（**单条**，body `{"queue_id":..,"offset":..}`；internal/admin/messages.go handleDLQResend）
  - `(c) QueryByKey(topic, key string, limit int) ([]AdminMsg, error)`、`(c) Browse(topic string, queueID uint32, from uint64, limit int) ([]AdminMsg, error)` — GET /admin/messages（key 走 keyidx；否则 queue_id+from_offset 顺序浏览；两者都必须带 topic）；`AdminMsg` 对齐 msgJSON 全部字段
  - `(c) Overview() (Overview, error)` — GET /admin/overview（字段名以 handleOverview 尾部 writeJSON 为准，落码前核对）
  - `(c) DelayList(limit int) ([]DelayEntry, error)` — GET /admin/delay，`DelayEntry{DueMs int64; MsgID, Topic string}`
  - `DLQTopic(group string) string` — 返回 `"%DLQ%" + group`（对齐 internal/core/meta.DLQTopicName）

- [ ] **Step 1: 写失败测试**

```go
//go:build scenario

package scenario

// TestAdminClientBasics 对真实 broker 打一轮最小闭环：
// 建 topic → 免登录 Overview → 组不存在 404 → key 检索空结果。
// 目的不是测 Admin API 本身（admin 包单测已覆盖），是钉住客户端的
// 路径拼写、参数名与反序列化字段——这些错在混跑里只会表现为对账悬案。
func TestAdminClientBasics(t *testing.T) {
	c := NewProcessCluster(t, ShortProfile())
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Close()
	ac := NewAdminClient(c.AdminEndpoint())

	if err := ac.CreateTopic("scn-admin-basic", 4); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	ov, err := ac.Overview()
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if ov.Topics < 1 {
		t.Fatalf("建 topic 后 Overview.Topics=%d", ov.Topics)
	}
	if _, err := ac.GroupDetail("no-such-group"); err == nil {
		t.Fatal("不存在的组应返回错误")
	}
	msgs, err := ac.QueryByKey("scn-admin-basic", "nope", 10)
	if err != nil {
		t.Fatalf("QueryByKey: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("空 topic 按 key 检索应为空，得到 %d 条", len(msgs))
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -race -tags scenario -run TestAdminClientBasics ./test/scenario/`
Expected: FAIL（undefined: NewAdminClient）

- [ ] **Step 3: 实现 adminclient.go**

要点（其余是常规 net/http + json）：

- base = `"http://" + adminEndpoint`；`http.Client{Timeout: 10s}`；
- 免登录形态（场景配置不设 admin 用户名密码），不带 Cookie；
- 非 2xx 统一 `fmt.Errorf("%s %s: HTTP %d: %s", method, path, code, body)`——对账悬案时这行错误就是第一现场；
- DLQ topic 含 `%`，query 参数一律 `url.Values.Encode()`，路径段用 `url.PathEscape`；
- 每个方法入口 `//` 注释标注对应 handler 文件:行，字段名改动时能一步找到事实源。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -race -tags scenario -run TestAdminClientBasics ./test/scenario/`
Expected: PASS

- [ ] **Step 5: 注释自检**

文件头（职责：typed HTTP 封装；边界：不做重试、不做断言）；每个方法 doc comment 带对应服务端 handler 位置。

- [ ] **Step 6: Commit**

```bash
git add test/scenario/adminclient.go test/scenario/adminclient_test.go
git commit -m "test(scenario): Admin API typed 客户端"
```

---

### Task 6: 生产者演员（普通/延时/FIFO）

**Files:**
- Create: `test/scenario/producers.go`
- Test: 本任务无独立测试文件——演员必须连 broker 才有意义，验收测试在 Task 7 Step 4 的无故障冒烟场景（两任务连做）；本任务出口是编译干净 + vet 通过

**Interfaces:**
- Consumes: Task 4 `Ledger/Payload/ClassifySendErr`、Task 2 `Profile`
- Produces（Task 7/10 依赖）:
  - `type producerSet struct`；`startProducers(t *testing.T, endpoint string, prof Profile, seed int64, lg *Ledger) (*producerSet, error)`
  - `(ps *producerSet) run(ctx context.Context)`（阻塞至 ctx 取消，内部 WaitGroup 全收）
  - `(ps *producerSet) close()`（GracefulStop 所有 SDK producer）
  - topic 常量：`topicNormal = "scn-normal"`、`topicDelay = "scn-delay"`、`topicFIFO = "scn-fifo"`、`topicChaos = "scn-chaos"`（捣乱组专用，普通 producer 中固定一个演员往它发）、`var tagPool = []string{"", "tagA", "tagB", "tagC"}`

- [ ] **Step 1: 实现 producers.go**

一个演员一个 goroutine；`*rand.Rand` 由 `rand.New(rand.NewSource(seed + actorIndex))` 派生，演员间不共享。SDK 用法对照 e2e：普通/收发 `test/e2e/sdk_test.go`、延时 `sdk_delay_test.go`、FIFO `sdk_fifo_test.go`（producer 构造、`SetDelayTimestamp`、`SetMessageGroup` 的现成写法，落码前打开抄）。

普通演员主循环（骨架，含账本三态与日志——延时/FIFO 演员按下述差异改造同一骨架）：

```go
// runNormal 普通消息演员：随机 tag/keys 混发，结果按三态入账后继续。
// broker 被 kill 期间发送必然报错——这不是演员的异常路径，是账本的
// indeterminate 数据来源（spec §5.1：演员不判断 broker 存活）。
func (a *producerActor) runNormal(ctx context.Context, t *testing.T) {
	for seq := uint64(1); ; seq++ {
		select {
		case <-ctx.Done():
			t.Logf("演员 %s 收工：共发 %d 条", a.id, seq-1)
			return
		case <-time.After(a.prof.SendInterval + time.Duration(a.rnd.Int63n(int64(a.prof.SendInterval)))):
		}
		p := Payload{ActorID: a.id, Seq: seq, SentAtMs: time.Now().UnixMilli()}
		msg := &rmq.Message{Topic: a.topic, Body: p.Encode()}
		tag := tagPool[a.rnd.Intn(len(tagPool))]
		if tag != "" {
			msg.SetTag(tag)
		}
		var keys []string
		if a.rnd.Intn(2) == 0 { // 一半消息带 key，供不变量 8 抽样检索
			keys = []string{fmt.Sprintf("k-%s-%d", a.id, seq)}
			msg.SetKeys(keys...)
		}
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		receipts, err := a.p.Send(sctx, msg)
		cancel()
		rec := SendRecord{Payload: p, Topic: a.topic, Tag: tag, Keys: keys,
			State: ClassifySendErr(err), At: time.Now()}
		if err != nil {
			rec.Err = err.Error()
		} else {
			rec.MsgID = receipts[0].MessageID
		}
		a.ledger.RecordSend(rec)
	}
}
```

延时演员差异：`deliverAt := now + DelayWindowMin + rnd(DelayWindowMax-DelayWindowMin)`；**clamp 规则（§3.1）**：`trafficEnd` 由 startProducers 传入，`deliverAt > trafficEnd` 时截到 `trafficEnd`；若 `trafficEnd - now < DelayWindowMin` 则本演员直接收工（不再发）。`msg.SetDelayTimestamp(deliverAt)`，`p.DeliverAtMs = deliverAt.UnixMilli()`。

FIFO 演员差异（**纪律，Global Constraints）**：每组独立 `nextSeq`；发送失败（indeterminate/failed）时**同 seq 重试**（每次间隔 500ms + 抖动，重试也逐条入账），直到 confirmed 才 `nextSeq++`；ctx 取消时放弃当前 seq 收工。`msg.SetMessageGroup(group)`，topic 固定 `topicFIFO`。

`startProducers`：按 Profile 数量构造全部演员与 SDK producer（`rmq.WithTopics(全部四个 topic)`）；普通演员中 index 0 固定发 `topicChaos`（喂捣乱组），其余发 `topicNormal`。

- [ ] **Step 2: 编译与 vet**

Run: `go vet -tags scenario ./test/scenario/ && go build -tags scenario ./test/scenario/`
Expected: 通过（无独立单测，联测在 Task 7）

- [ ] **Step 3: 日志与注释自检**

每个演员：入口一条（id/kind/topic）、收工一条（总量）、FIFO 重试分支一条 Debug 级 `t.Logf`（含 group/seq/错误）。文件头 + `startProducers`/`run`/`close` doc comment + clamp 与 FIFO 纪律的「为什么」注释。

- [ ] **Step 4: Commit**

```bash
git add test/scenario/producers.go
git commit -m "test(scenario): 普通/延时/FIFO 生产者演员（FIFO 同 seq 重试纪律）"
```

---

### Task 7: 消费者演员与排水模式 + 无故障冒烟场景

**Files:**
- Create: `test/scenario/consumers.go`
- Test: `test/scenario/smoke_test.go`（`TestScenarioSmokeNoFaults`——同时是 Task 6 的验收）

**Interfaces:**
- Consumes: Task 4/5/6 全部
- Produces（Task 9/10 依赖）:
  - `type consumerSet struct`；`startConsumers(t *testing.T, endpoint string, prof Profile, seed int64, lg *Ledger) (*consumerSet, error)`
  - `(cs *consumerSet) run(ctx context.Context)`
  - `(cs *consumerSet) enterDrain()`（全员切「收到即 ack」，spec §7.1）
  - `(cs *consumerSet) close()`
  - `waitDrained(t *testing.T, ac *AdminClient, timeout time.Duration) error`（堆积+inflight 双归零且延时暂存区为空，连续 3 次采样成立才算收敛——单次归零可能是重投退避间隙的假象）
  - 消费组常量：`groupA = "scn-ga"`（SUB_ALL @ topicNormal）、`groupB = "scn-gb"`（SUB_ALL @ topicNormal，与 A 同 topic 验证位点独立）、`groupTag = "scn-gtag"`（`"tagA || tagB"` @ topicNormal）、`groupFIFO = "scn-gfifo"`（SUB_ALL @ topicFIFO）、`groupDelay = "scn-gdelay"`（SUB_ALL @ topicDelay）、`groupChaos = "scn-gchaos"`（SUB_ALL @ topicChaos，捣乱行为）

- [ ] **Step 1: 写失败冒烟测试**

`smoke_test.go`：

```go
//go:build scenario

package scenario

// TestScenarioSmokeNoFaults 无故障版最小混跑：全部演员跑 30s → 排水 →
// 「不丢」与「不冒」两条最基本不变量必须成立（完整对账器在 Task 9，
// 这里先用账本裸算）。它是演员与账本联调的验收门，也是以后改演员
// 逻辑时最快的回归入口。
func TestScenarioSmokeNoFaults(t *testing.T) {
	prof := ShortProfile()
	prof.TrafficDuration = 30 * time.Second
	seed := time.Now().UnixNano()
	t.Logf("scenario seed=%d profile=%+v", seed, prof)

	c := NewProcessCluster(t, prof)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Close()
	lg := NewLedger()
	ac := NewAdminClient(c.AdminEndpoint())

	ps, err := startProducers(t, c.Endpoint(), prof, seed, lg)
	if err != nil {
		t.Fatalf("startProducers: %v", err)
	}
	cs, err := startConsumers(t, c.Endpoint(), prof, seed+1000, lg)
	if err != nil {
		t.Fatalf("startConsumers: %v", err)
	}

	trafficCtx, cancelTraffic := context.WithTimeout(context.Background(), prof.TrafficDuration)
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	go cs.run(consumeCtx)
	ps.run(trafficCtx) // 阻塞到流量期结束
	cancelTraffic()
	ps.close()

	lg.RecordEvent("drain-start", nil)
	cs.enterDrain()
	if err := waitDrained(t, ac, prof.DrainTimeout); err != nil {
		t.Fatalf("排水未收敛: %v", err)
	}
	cancelConsume()
	cs.close()

	// 裸算不丢/不冒：confirmed 全部要被订阅匹配的组收到；收到的全部要能对上账
	s := lg.Snapshot()
	delivered := map[string]bool{} // group+"/"+Payload.Key
	for _, d := range s.Deliveries {
		if _, err := DecodePayload(d.Payload.Encode()); err != nil {
			t.Errorf("不冒违例：组 %s 收到账外消息 msgId=%s", d.ConsumerGroup, d.MsgID)
		}
		delivered[d.ConsumerGroup+"/"+d.Payload.Key()] = true
	}
	for _, sd := range s.Sends {
		if sd.State != SendConfirmed {
			continue
		}
		for g, sub := range s.Subs {
			if sub.Topic == sd.Topic && TagMatches(sub.Expr, sd.Tag) && !delivered[g+"/"+sd.Payload.Key()] {
				t.Errorf("不丢违例：confirmed 消息 %s (topic=%s tag=%q) 未被订阅组 %s 收到", sd.Payload.Key(), sd.Topic, sd.Tag, g)
			}
		}
	}
	t.Logf("冒烟对账通过：sends=%d deliveries=%d", len(s.Sends), len(s.Deliveries))
}
```

（无故障时捣乱组也会走到 DLQ——冒烟里捣乱组固定用排水前很短的流量,为控制变量，`prof.TrafficDuration=30s` 且 MaxAttempts=2 时死信会在排水期出现在 `%DLQ%{scn-gchaos}`；因此冒烟对 `groupChaos` 的「不丢」判定放宽为：未 delivered 的 confirmed 消息必须能用 `ac.Browse(DLQTopic(groupChaos), ...)` 在 DLQ 找到——这段查询逻辑写进冒烟测试，Task 9 会把它抽进正式对账器。）

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -race -tags scenario -run TestScenarioSmokeNoFaults ./test/scenario/`
Expected: FAIL（undefined: startConsumers 等）

- [ ] **Step 3: 实现 consumers.go**

要点：

- SimpleConsumer 构造对照 e2e（`rmq.NewSimpleConsumer` + `rmq.WithAwaitDuration(prof.AwaitDuration)` + `rmq.WithSubscriptionExpressions`），tag 过滤组用 `rmq.NewFilterExpression("tagA || tagB")`；订阅注册后立刻 `lg.RegisterSub(group, Subscription{Topic, Expr})`；
- 主循环：`Receive(ctx, 16, invisible)`；`MESSAGE_NOT_FOUND` 静默 continue（长轮询空结果的正常形态，e2e 已锁定其表述）；其他错误 `t.Logf` 后 sleep 500ms continue（broker 重启窗口的正常噪音）；
- 每条消息：`DecodePayload(mv.GetBody())` → `DeliveryRecord{ConsumerGroup, Topic, MsgID: mv.GetMessageId(), Payload, Attempt: mv.GetDeliveryAttempt(), At: time.Now()}`（GetDeliveryAttempt 的确切方法名落码前对照 e2e sdk_fifo/dlq 用例核实）；
- 行为分派：`mode` 是 `atomic.Int32`（0=normal 1=chaos 2=drain）。normal/drain：ack 并把结果写进记录；chaos：`r := rnd.Float64()` → r<0.4 不 ack（记录 AckAttempted=false）；r<0.6 慢 ack（sleep invisible×1.5 后再 ack，多半已过不可见窗，ack 错误照记）；否则正常 ack。**chaos 组的 rnd 访问只在本组 goroutine 内**，无锁；
- `enterDrain()`：全员 `mode.Store(2)` 并 `t.Logf("进入排水模式")`；
- `waitDrained`：每 2s 轮询 `ac.Overview()` + `ac.DelayList(1)`；`TotalPending==0 && TotalInflight==0 && len(delay)==0` 连续 3 次才返回 nil；超时返回带最后一次读数的错误（区分「没收敛」和「admin 打不通」）。

- [ ] **Step 4: 跑冒烟确认通过**

Run: `go test -race -tags scenario -run TestScenarioSmokeNoFaults ./test/scenario/`
Expected: PASS（约 1.5–2 分钟：30s 流量 + 排水）
失败时优先怀疑：订阅注册时序（consumer 必须在 producer 首发前 Start，否则首批消息落在订阅建立前——sq 的组游标从订阅时刻起算）。冒烟里 startConsumers 先于 startProducers 调用即可规避；把这条写进 smoke_test 注释。

- [ ] **Step 5: 日志与注释自检**

关键日志：每组启动（组名/订阅）、进入排水、waitDrained 每次采样读数（Debug 频率，2s 一条可接受）、收敛成功一条。注释：文件头、chaos 三分支概率的「为什么」、waitDrained 连续 3 次采样的「为什么」（重投退避间隙的假归零）。

- [ ] **Step 6: Commit**

```bash
git add test/scenario/consumers.go test/scenario/smoke_test.go
git commit -m "test(scenario): 六消费组演员、排水收敛与无故障冒烟场景"
```

---

### Task 8: 故障调度器（条件触发 + deadline）

**Files:**
- Create: `test/scenario/scheduler.go`
- Test: `test/scenario/scheduler_test.go`

**Interfaces:**
- Consumes: Task 3 `Node`、Task 4 `Ledger`、Task 5 `AdminClient`
- Produces（Task 10 依赖）:
  - `newScheduler(t *testing.T, node Node, ac *AdminClient, lg *Ledger, prof Profile, seed int64) *scheduler`
  - `(s *scheduler) run(ctx context.Context)`（阻塞至 ctx 取消）
  - `(s *scheduler) verifyMinimums() error`（结束期校验每类事件次数达标，spec §6）
  - 事件 Kind 常量：`evKill/evGraceful/evReset/evResend/evNewTopic`（写入 Event.Kind）

- [ ] **Step 1: 写失败测试**

调度器的进程类事件依赖真 broker，但**选择与前置判定逻辑**可以纯内存测。把「挑下一个可执行事件」抽成纯函数测它：

```go
//go:build scenario

package scenario

// TestSchedulerPickEligible 事件选择必须只在「前置谓词满足」的候选里随机，
// 且已达 deadline 仍不满足前置的事件要被报告为参数不自洽（spec §6）。
func TestSchedulerPickEligible(t *testing.T) {
	specs := []eventSpec{
		{kind: "a", pre: func() (bool, string) { return true, "" }},
		{kind: "b", pre: func() (bool, string) { return false, "还没内容" }},
	}
	rnd := rand.New(rand.NewSource(1))
	for i := 0; i < 20; i++ {
		got := pickEligible(specs, rnd)
		if got == nil || got.kind != "a" {
			t.Fatalf("第 %d 次选择返回 %v，只有 a 前置满足", i, got)
		}
	}
}

// TestSchedulerVerifyMinimums 结束期最少次数校验。
func TestSchedulerVerifyMinimums(t *testing.T) {
	s := &scheduler{counts: map[string]int{evKill: 1, evGraceful: 1, evReset: 1, evResend: 0, evNewTopic: 1},
		prof: ShortProfile()}
	if err := s.verifyMinimums(); err == nil {
		t.Fatal("resend=0 应校验失败")
	}
	s.counts[evResend] = 1
	if err := s.verifyMinimums(); err != nil {
		t.Fatalf("全部达标仍报错: %v", err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -race -tags scenario -run TestScheduler ./test/scenario/`
Expected: FAIL

- [ ] **Step 3: 实现 scheduler.go**

```go
// eventSpec 一类可注入事件：前置谓词 + 执行体。pre 返回 (满足?, 不满足原因)，
// 原因进 deadline 超时的报错文案——「参数不自洽」要能说清卡在哪个前置上。
type eventSpec struct {
	kind string
	min  int
	pre  func() (bool, string)
	fire func() error
}
```

主循环：`sleep(EventMinGap + rnd 抖动)` → `pickEligible` → `fire`（错误 `t.Errorf`，事件失败即测试失败——注入器自己不能带病继续）→ `lg.RecordEvent(kind, detail)` → `counts[kind]++`。每轮顺带检查：任何 `counts[k] < spec.min` 且 `now > 流量期起点+EventDeadline` 且前置仍不满足的事件 → `t.Errorf("事件 %s 到 deadline 前置仍不满足（%s）——参数不自洽，检查 §3.1 约束表", ...)`。

五类事件的 fire 实现要点：

- `evKill`：`node.Kill()` → sleep 1–3s（rnd）→ `node.Restart()`；detail 记 gen 前后；
- `evGraceful`：`elapsed, err := node.Stop()` → detail 记 `elapsed_ms`、`exit_err`（对账器据此裁决 StopBudget，调度器本身只在 err != nil 时 Errorf）→ `node.Restart()`；
- 进程类事件公共前置：`node.Alive()` **且**账本里最近一次 confirmed 发送晚于最近一次 restart 事件（证明演员已重连——防止连环 kill 把流量饿死；从 `lg` 加一个只读方法 `lastConfirmedAt()/lastEventAt(kind)` 支持该谓词）；
- `evReset`：前置 = `ac.GroupDetail(groupA)` 有某队列 `Cursor > 4`；fire = 挑 cursor 最大的队列，`old := q.Cursor`，`target := old / 2`（**只向后**），先 `lg.RecordEvent(evReset, {group, topic, queue, old_cursor, new_offset})` 再 `ac.ResetCursor(...)`——先记后做，kill 竞态下宁可账本里有一条没执行成的重置（fire 报错测试即失败），不可有一次没入账的重置；
- `evResend`：前置 = `ac.Browse(DLQTopic(groupChaos), 0..queues-1, 0, 1)` 任一队列非空；fire = 取第一条，`msgID, err := ac.DLQResend(groupChaos, q, off)`，detail 记 `{group, msg_id, queue, offset}`——不变量 3/6 的豁免名单就是全部 evResend 事件的 msg_id 集合；
- `evNewTopic`：fire = `ac.CreateTopic(fmt.Sprintf("scn-dyn-%d", n), 4)` 后向该 topic 用普通 producer 发 3 条并入账（auto_create 竞态的变体：admin 建 topic 与 SDK 首次路由并发）。该 producer 从 producerSet 借（`startProducers` 多暴露一个 `(ps).sendTo(topic string, n int)` 方法，内部演员 id 用 `dyn-{n}`，正常入账）。groupA/groupB 不订阅动态 topic，因此这些消息的「不丢」由**专门订阅 `scn-dyn-*` 的处置**覆盖——为控制复杂度：`evNewTopic` 的 fire 在发完后自行用 `ac.Browse(newTopic, ...)` 断言 3 条都在（写入即算达标），账本里把它们标记 `Topic: newTopic`，对账器对无订阅组的 topic 只做不变量 2（不冒）不做不变量 1（不丢）——`Subs` 里没有该 topic 的组，不变量 1 的循环天然跳过，无需特判。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -race -tags scenario -run TestScheduler ./test/scenario/`
Expected: PASS

- [ ] **Step 5: 日志与注释自检**

每次事件注入前后各一条 `t.Logf`（kind + detail + 第几次）；deadline 检查命中时的 Errorf 文案含前置不满足原因。注释：文件头、「先记后做」的为什么、进程类前置「重连证明」的为什么。

- [ ] **Step 6: Commit**

```bash
git add test/scenario/scheduler.go test/scenario/scheduler_test.go
git commit -m "test(scenario): 条件触发+deadline 故障调度器（五类事件）"
```

---

### Task 9: 对账器与诊断打包

**Files:**
- Create: `test/scenario/checker.go`
- Create: `test/scenario/diag.go`
- Test: `test/scenario/checker_test.go`

**Interfaces:**
- Consumes: Task 4 `LedgerSnapshot`、Task 5 `AdminClient`
- Produces（Task 10 依赖）:
  - `type CheckResult struct { Name string; Violations []string; Stats map[string]int }`
  - `runAllChecks(t *testing.T, s *LedgerSnapshot, ac *AdminClient, prof Profile, logPaths []string) []CheckResult`（内部依次调下列检查，全部跑完不短路——一次运行报出全部违例）
  - 纯函数检查（可单测）：`checkNoLoss(s, dlqByGroup map[string][]AdminMsg) CheckResult`、`checkNoPhantom(s) CheckResult`、`checkAckFinality(s) CheckResult`、`checkFIFO(s, dlqFIFOSeqs map[uint64]bool) CheckResult`、`checkDelay(s) CheckResult`、`checkResetRedelivery(s) CheckResult`
  - 带外部依赖的检查（在 runAllChecks 内联）：keys 抽样（不变量 8）、终局 Overview 对账（9）、日志扫描与停机预算（10）、DLQ 溯源属性（5，用 dlqByGroup 的 Properties）
  - `writeDiagBundle(t *testing.T, dir string, prof Profile, seed int64, s *LedgerSnapshot, results []CheckResult, logPaths []string)`（spec §8）

- [ ] **Step 1: 写失败测试（合成账本，纯内存）**

`checker_test.go` 至少覆盖以下用例（每条不变量一正一反）：

```go
//go:build scenario

package scenario

// mkSnap 合成账本快照的测试构造器。
func mkSnap(sends []SendRecord, dels []DeliveryRecord, evs []Event) *LedgerSnapshot {
	return &LedgerSnapshot{Sends: sends, Deliveries: dels, Events: evs,
		Subs: map[string]Subscription{
			groupA: {Topic: topicNormal, Expr: "*"},
			groupTag: {Topic: topicNormal, Expr: "tagA || tagB"},
		}}
}

// TestCheckNoLossTagAware 不丢必须按订阅匹配判定：tagC 消息 groupTag 收不到不是违例。
func TestCheckNoLossTagAware(t *testing.T) {
	p := Payload{ActorID: "n0", Seq: 1}
	s := mkSnap(
		[]SendRecord{{Payload: p, Topic: topicNormal, Tag: "tagC", State: SendConfirmed, MsgID: "m1"}},
		[]DeliveryRecord{{ConsumerGroup: groupA, Payload: p, MsgID: "m1"}},
		nil)
	r := checkNoLoss(s, nil)
	if len(r.Violations) != 0 {
		t.Fatalf("tagC 未到 groupTag 不应违例: %v", r.Violations)
	}
	// 去掉 groupA 的投递 → groupA 违例、groupTag 仍不违例
	s.Deliveries = nil
	r = checkNoLoss(s, nil)
	if len(r.Violations) != 1 {
		t.Fatalf("期望恰好 1 条违例(groupA)，得到 %v", r.Violations)
	}
}

// TestCheckFIFOHoleNeedsDLQ 空洞只有在 DLQ 能找到该 seq 时才合法（spec §7.2-4）。
func TestCheckFIFOHoleNeedsDLQ(t *testing.T) {
	mk := func(seq uint64) DeliveryRecord {
		return DeliveryRecord{ConsumerGroup: groupFIFO,
			Payload: Payload{ActorID: "f0", Seq: seq, Group: "g1"}}
	}
	s := mkSnap(nil, []DeliveryRecord{mk(1), mk(2), mk(4)}, nil)
	if r := checkFIFO(s, map[uint64]bool{}); len(r.Violations) == 0 {
		t.Fatal("seq=3 空洞且不在 DLQ，应违例")
	}
	if r := checkFIFO(s, map[uint64]bool{3: true}); len(r.Violations) != 0 {
		t.Fatalf("seq=3 在 DLQ，空洞合法: %v", r.Violations)
	}
	// 乱序（非前缀重放形态）必须违例：4 之后又出现 2 且 2 并非重投重复
	s = mkSnap(nil, []DeliveryRecord{mk(1), mk(2), mk(3), mk(4), mk(2), mk(5)}, nil)
	if r := checkFIFO(s, nil); len(r.Violations) == 0 {
		t.Fatal("回跳 seq=2 后又推进到 5，重复合法但序列须仍是前缀重放；此例合法")
	}
}

// TestCheckAckFinalityExemptions ack 后重投默认违例；kill 窗口 / 重置窗口 /
// 重发名单三类豁免只计数不违例（spec §7.2-3）。
func TestCheckAckFinalityExemptions(t *testing.T) {
	p := Payload{ActorID: "n0", Seq: 7}
	ackAt := time.Unix(100, 0)
	again := time.Unix(200, 0)
	dels := []DeliveryRecord{
		{ConsumerGroup: groupA, MsgID: "m7", Payload: p, At: ackAt, AckAttempted: true, AckOK: true},
		{ConsumerGroup: groupA, MsgID: "m7", Payload: p, At: again},
	}
	// 无任何事件 → 违例
	if r := checkAckFinality(mkSnap(nil, dels, nil)); len(r.Violations) == 0 {
		t.Fatal("ack 后重投且无豁免事件，应违例")
	}
	// 两次投递之间有 kill → 豁免
	evs := []Event{{At: time.Unix(150, 0), Kind: evKill}}
	if r := checkAckFinality(mkSnap(nil, dels, evs)); len(r.Violations) != 0 {
		t.Fatalf("kill 窗口应豁免: %v", r.Violations)
	}
}

// TestCheckDelayEarly 早投violation：首投时间早于 deliveryAt-1s。
func TestCheckDelayEarly(t *testing.T) {
	p := Payload{ActorID: "d0", Seq: 1, DeliverAtMs: 60_000}
	early := []DeliveryRecord{{ConsumerGroup: groupDelay, Payload: p, At: time.UnixMilli(58_000)}}
	if r := checkDelay(mkSnap(nil, early, nil)); len(r.Violations) == 0 {
		t.Fatal("早投 2s 应违例")
	}
	ok := []DeliveryRecord{{ConsumerGroup: groupDelay, Payload: p, At: time.UnixMilli(59_500)}}
	if r := checkDelay(mkSnap(nil, ok, nil)); len(r.Violations) != 0 {
		t.Fatalf("容差内不应违例: %v", r.Violations)
	}
}
```

（`checkNoPhantom`、`checkResetRedelivery` 各配一正一反用例，同构不赘述——phantom 用一条 Deliveries 里 Payload.ActorID 在 Sends 中不存在的记录；reset 用 evReset 事件 detail 的 old_cursor/new_offset 与其后是否出现重投递记录。）

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -race -tags scenario -run TestCheck ./test/scenario/`
Expected: FAIL

- [ ] **Step 3: 实现 checker.go**

各检查实现要点（对应 spec §7.2 十条）：

1. `checkNoLoss`：对每条 confirmed send × 每个订阅匹配组：`delivered[group/payloadKey]` 或 `dlqByGroup[group]` 里按 Payload 匹配；两者皆无 → 违例。DLQ 命中时同步校验 `sq-origin-topic/queue/offset` 三属性齐全（缺失也是违例，这就是不变量 5 的溯源半边）；
2. `checkNoPhantom`：每条 delivery 的 Payload.Key 必须存在于 Sends（任意 state）；`DecodePayload` 失败的已在演员侧记为特殊 ActorID=""，此处直接违例；
3. `checkAckFinality`：按 (group, msgID) 分组投递记录排时序；存在 AckOK 记录之后又有投递 → 查豁免：两次之间存在 evKill/evReset 事件，或 msgID 在 evResend 名单 → 计 `Stats["exempted_redelivery"]++`；否则违例；
4. `checkFIFO`：按 MessageGroup 分组、按 At 排序取 seq 序列；合法性判定 = 序列可由「前缀重放」生成（维护 `maxSeen`：`seq <= maxSeen+1` 合法，`seq > maxSeen+1` 时对 `(maxSeen, seq)` 区间内每个缺失 seq 查 `dlqFIFOSeqs`，都在才合法）；
5. DLQ 语义在 runAllChecks 内联：拉全 `%DLQ%{scn-gchaos}` 与 `%DLQ%{scn-gfifo}` 的消息（Browse 逐队列翻页）构造 `dlqByGroup`/`dlqFIFOSeqs` 传给 1/4；「原队列不再投」= 对每个 DLQ msgID，检查其入 DLQ 时间（DLQ 消息 StoreAtMs）之后原 topic 无该 msgID 的投递记录，evResend 名单内的除外；
6. `checkDelay`：对每条 DeliverAtMs>0 的消息取**首次**投递，`At >= DeliverAt - 1s`；
7. `checkResetRedelivery`：对每个 evReset 事件：`[new_offset, old_cursor)` 区间应有消息在事件后被重新投递——放宽为「事件后该组该 topic 出现过 At > 事件时刻 且 Payload 曾在事件前投递过的记录」至少 1 条（精确按 offset 对账需要账本记 offset，投递侧 SDK 拿不到 offset，用重复投递作为重置生效的证据足够）；
8. keys 抽样：从 confirmed 且带 keys 的 sends 里 rnd 抽 ≤20 条，`ac.QueryByKey(topic, key, 8)` 必须命中该 msgID；
9. 终局 Overview：`ov.TotalPending==0 && ov.TotalInflight==0`；`ov.TotalWritten ≥ confirmed 总数`（≥ 而非 ==：indeterminate 落盘与 DLQ 重发副本都会推高写入计数，== 是对不上的）；
10. 进程健康：逐个 logPath 扫 `panic` 与 `DATA RACE` 子串；遍历 evGraceful 事件断言 `elapsed_ms ≤ StopBudget` 且 `exit_err` 为空。

`diag.go`：`writeDiagBundle` 在任一 CheckResult 有违例时被 Task 10 调用——目录结构 `{dir}/profile.json`、`events.jsonl`、`violations.txt`（按检查分节）、`sends.jsonl`/`deliveries.jsonl`（只写违例涉及的 Payload.Key 相关记录，全量太大）、`overview.json`、`broker-gen*.log`（拷贝）；写完 `t.Logf("诊断包: %s", dir)`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test -race -tags scenario -run TestCheck ./test/scenario/`
Expected: PASS

- [ ] **Step 5: 日志与注释自检**

runAllChecks 每个检查完成打一条（名称/违例数/Stats）；writeDiagBundle 成功与每个文件写失败都有日志。注释：文件头、checkFIFO 前缀重放判定算法的「为什么」、Overview 用 ≥ 的「为什么」、checkResetRedelivery 放宽判定的「为什么」。

- [ ] **Step 6: Commit**

```bash
git add test/scenario/checker.go test/scenario/diag.go test/scenario/checker_test.go
git commit -m "test(scenario): 十条不变量对账器与失败诊断打包"
```

---

### Task 10: 主场景组装与收尾

**Files:**
- Create: `test/scenario/scenario_test.go`（`TestScenarioMixed`）
- Modify: `README.md`（「测试」小节补 scenario 层说明与跑法）

**Interfaces:**
- Consumes: 前面全部任务

- [ ] **Step 1: 实现 TestScenarioMixed**

```go
//go:build scenario

package scenario

// TestScenarioMixed 完整混跑场景（spec 全文的落地入口）：
// 六消费组 + 四类 producer 混跑 → 调度器注入五类事件 → 排水 → 十条不变量对账。
// 短档 ~3 分钟；SQ_SCENARIO_DURATION=30m 升长档。失败时看诊断包路径。
func TestScenarioMixed(t *testing.T) {
	prof, err := LoadProfile()
	if err != nil {
		t.Fatalf("LoadProfile: %v", err)
	}
	if err := prof.Validate(); err != nil {
		t.Fatalf("Profile 未通过 §3.1 约束校验: %v", err)
	}
	t.Logf("scenario seed=%d profile=%+v", prof.Seed, prof) // 日志第一行：复现入口

	c := NewProcessCluster(t, prof)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("集群启动: %v", err)
	}
	defer c.Close()
	lg := NewLedger()
	ac := NewAdminClient(c.AdminEndpoint())

	cs, err := startConsumers(t, c.Endpoint(), prof, prof.Seed+1000, lg)
	if err != nil {
		t.Fatalf("startConsumers: %v", err)
	}
	ps, err := startProducers(t, c.Endpoint(), prof, prof.Seed, lg)
	if err != nil {
		t.Fatalf("startProducers: %v", err)
	}
	sched := newScheduler(t, c.Nodes()[0], ac, lg, prof, prof.Seed+2000)

	trafficCtx, cancelTraffic := context.WithTimeout(context.Background(), prof.TrafficDuration)
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	schedCtx, cancelSched := context.WithCancel(context.Background())
	go cs.run(consumeCtx)
	go sched.run(schedCtx)
	ps.run(trafficCtx) // 流量期
	cancelTraffic()

	cancelSched() // 先停注入再排水：排水期还在 kill 会让收敛判定失去意义
	ps.close()
	if err := sched.verifyMinimums(); err != nil {
		t.Errorf("事件次数未达标: %v", err)
	}

	lg.RecordEvent("drain-start", nil)
	cs.enterDrain()
	if err := waitDrained(t, ac, prof.DrainTimeout); err != nil {
		t.Errorf("排水未收敛: %v", err)
	}
	cancelConsume()
	cs.close()

	results := runAllChecks(t, lg.Snapshot(), ac, prof, c.Nodes()[0].LogPaths())
	bad := false
	for _, r := range results {
		t.Logf("[%s] 违例=%d stats=%v", r.Name, len(r.Violations), r.Stats)
		for _, v := range r.Violations {
			bad = true
			t.Errorf("[%s] %s", r.Name, v)
		}
	}
	if bad || t.Failed() {
		dir := filepath.Join(os.TempDir(), fmt.Sprintf("sq-scenario-diag-%d", prof.Seed))
		writeDiagBundle(t, dir, prof, prof.Seed, lg.Snapshot(), results, c.Nodes()[0].LogPaths())
	}
}
```

注意排水前调度器必须已停但 broker 必须存活：若最后一个事件是 kill 且 Restart 失败，`waitDrained` 会把 admin 打不通如实报出来。

- [ ] **Step 2: 短档全量跑**

Run: `make scenario`
Expected: 全部 PASS（TestScenarioMixed ~3–5 分钟；连同冒烟与单测总计 <10 分钟）。间歇失败先用日志里的 seed 复跑事件时间线，再看诊断包。

- [ ] **Step 3: README 补「测试」说明**

在 README 合适位置（现有测试相关叙述附近）补一小节：三层测试各自定位（单测/e2e/scenario）、`make scenario` 跑法、`SQ_SCENARIO_DURATION`/`SQ_SCENARIO_SEED` 两个环境变量、失败时诊断包在哪。措辞对齐 spec §1。

- [ ] **Step 4: instrumenting-code 完工清单自检**

逐项过：每个错误分支带上下文日志 ✓；外部调用（SDK send/receive、admin HTTP、进程信号）前后有日志 ✓；成功路径不静默（演员收工统计、事件注入、检查结果、收敛成功各有一条）✓；无 fmt.Printf ✓；新文件头注释 ✓；导出符号 doc comment ✓。缺哪项补哪项。

- [ ] **Step 5: 全量回归**

Run: `go vet ./... && make test && make e2e && make scenario`
Expected: 全部通过（确认共享包抽取没伤到任何一层）

- [ ] **Step 6: Commit**

```bash
git add test/scenario/scenario_test.go README.md
git commit -m "test(scenario): 混跑主场景组装与 README 测试说明"
```

---

## Self-Review 记录

- **Spec 覆盖**：§3 目录/档位/种子/Makefile→Task 2；§4 Harness→Task 3；§5 演员/账本/三态表→Task 4/6/7；§6 五类事件（spec 修订后为三类 Admin + 两类进程）条件触发→Task 8；§7.1 排水→Task 7；§7.2 十条不变量→Task 9（1–7 纯函数、8–10 内联）；§8 诊断→Task 9 diag + Task 10 接线；§3.1 约束表→Task 2 Validate。共享 helper（§3 末条）→Task 1。
- **发现并修正**：DLQ 重发是按 queue_id+offset 单条重发（handleDLQResend 源码），spec §6 的「重发事件」在 Task 8 落为「浏览 DLQ 取首条重发」；动态 topic 消息无订阅组，不变量 1 靠 Subs 循环天然跳过 + fire 内 Browse 断言写入，无特判。
- **类型一致性**：`Payload.Key()`/`SendState`/`Event.Detail` 坐标约定/`GroupDetail` 层级在 Task 4/5/8/9 间已对拍；`waitDrained` 签名 Task 7 定义、Task 10 使用一致。
- **占位符扫描**：SDK 确切符号（`ErrRpcStatus` 字段、`GetDeliveryAttempt`、`SetDelayTimestamp`）标注了「落码前 go doc / 对照 e2e 核对」并给出预期形态与验证手段，不属于留白；无 TBD/TODO。
