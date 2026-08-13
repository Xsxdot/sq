# AutoRenew：消费期间自动续租不可见期（B13.5）设计

> 状态：待用户审阅。本 spec 在用户外出期间由协调者独立产出，§12 列出了所有
> 需要用户复核的假设——尤其是 §12.1 的默认开关取值。

## 1. 背景与缺口

官方 Go SDK 的 PushConsumer 每次 `ReceiveMessage` 都下发 `auto_renew=true`
且**从不下发** `invisible_duration`（`push_consumer.go:734` 的
`WrapReceiveMessageRequest` 只设 `LongPollingTimeout` / `BatchSize` /
`AutoRenew`）。sq 对该字段零业务引用——全仓只有 protobuf 生成的 accessor，
`internal/rpc/receive.go:133-138` 直接回落 `default_invisible_duration`。

**后果**：listener 处理时间超过不可见期时，broker 会在客户端仍在处理的情况下
重投这条消息。sq 是 at-least-once，重复投递在契约内，所以这**不是正确性缺陷，
是与参考 broker 的行为背离**——`auto_renew` 存在的全部意义就是防这一幕。慢
消费者（大文件处理、外部 API 调用、批量入库）必然撞到，且撞到时的表现是「消息
被重复消费，日志里看不出为什么」。

极端情形还会误判 DLQ：一条一直在被正确处理、只是慢的消息，会因反复重投把
`Attempts` 顶到 `DefaultMaxAttempts`（默认 16）而进死信，而它其实从未失败过。

## 2. 协议语义：`auto_renew` 到底承诺什么

`proto/apache/rocketmq/v2/service.proto:91-101`：

```proto
message ReceiveMessageRequest {
  // ...
  // Required if client type is simple consumer.
  optional google.protobuf.Duration invisible_duration = 5;
  // For message auto renew and clean
  bool auto_renew = 6;
  // ...
}
```

协议注释很省，但三种消费者的取值构成完整对照，语义可以从对照里读出来：

| 消费者 | `auto_renew` | `invisible_duration` | 出处 |
|---|---|---|---|
| SimpleConsumer | false | **必填**（注释明说） | `simple_consumer.go:187-188` |
| LitePushConsumer | false | 显式下发 | `lite_push_consumer.go:210-212` |
| **PushConsumer** | **true** | **不下发** | `push_consumer.go:758` |

读法：`auto_renew=true` 等于**「我不告诉你要多久，我还在处理就帮我续着」**。
它把租期管理的责任从客户端转移到服务端——这也是为什么它和「不下发
`invisible_duration`」总是成对出现。

对照实现（Apache RocketMQ proxy 的 `ReceiptHandleProcessor`）：为 `auto_renew`
的收据句柄登记续租，周期性调 `changeInvisibleDuration` 延长，直到累计续租超过
上限、或该客户端的连接不再活跃为止。**服务端续租 + 绑定客户端存活 + 硬上限**
这三件套就是本 spec 要复刻的东西。

## 3. 方案选型

### 方案 A：维持现状，只在文档里承认

零成本，但 B13 这个 epic 的主题就是把「声称兼容」变成「真的兼容」，把一个
SDK 每次请求都在发的字段永久忽略，与 epic 目标直接冲突。**不取。**

### 方案 B：把 `auto_renew` 当作「换一个更长的常量」

`auto_renew=true` 时改用 `auto_renew_invisible_duration`（比如 10m）。一行改动。

**不取**，两条硬伤：

1. 它没解决问题，只是把任意常量换成另一个任意常量——handler 慢过 10m 照样穿帮。
2. 它让**所有** push 消费者的崩溃故障转移从 1m 恶化到 10m。消费者进程挂掉后，
   它手里的消息要等满 10m 才会被别人接走。用一个明确的可用性退化换一个不彻底
   的兼容性改善，不划算。

### 方案 C：扫描点续租（**采用**）

在既有的「过期 inflight 重投」判定点上分叉：这条 inflight 是带续租资格投出去
的、持有者的 Telemetry 会话还活着、且未超硬上限 → **续租**（只改
`ExpireAtMs`）而不是重投。

为什么这是对的形态，三条：

- **续租发生在本来就要写盘的时刻**。重投本身就要改写 inflight 记录，续租只是把
  同一次写的内容换掉，**零额外写放大**。不需要后台续租协程、不需要定时器。
- **判定点已经持有队列锁**，与 Ack / ChangeInvisible 的互斥关系自动继承，不用
  新增任何并发推理（`deliver.go:81-88` 那段「三者统一持队列锁」的不变式因此
  仍然成立——本方案不新增直接读写 inflight 的方法）。
- **故障转移不退化**。持有者一死，续租判定当场为假，走原来的重投路径；最坏
  延迟是一个不可见期，与今天完全相同。

代价是需要三样新东西：客户端标识、inflight 上的归属与上限、存活判定通道。下面
逐一说明。

### 明确否决的第四条路：在 `ReceiveMessage` 到达时续租

「客户端还在长轮询就说明它活着，顺手把它队列上的 inflight 都续一遍」——听起来
更省事，但**在最需要它的场景下失效**：PushConsumer 的本地缓存
（`maxCacheMessageCount`）满了会停止发起新的 `ReceiveMessage`，而缓存满正是
「一堆消息处理不完」的慢消费者场景。靠收件请求驱动续租，等于在需要续租时恰好
没有触发源。不取。

## 4. 架构

```
ReceiveMessage (rpc)
  │  从 gRPC metadata 取 x-mq-client-id
  │  req.GetAutoRenew() && 配置开启 → 组装 Lease{Owner, MaxRenew, Alive}
  ▼
deliver.Receive(..., WithLease(lease))
  ▼
receiveOnceLocked  ── 持队列锁
  │
  ├─ 阶段 1 扫描 inflight
  │    ist.ExpireAtMs <= now ?
  │        ├─ 可续租（§6 判据） → renews：只改 ExpireAtMs，Attempts 原样
  │        └─ 否则               → reds：原重投路径（Attempts++、退避、DLQ 判定）
  │
  └─ 阶段 2 投递新消息
       lease 启用 → 写入的 InflightState 带上 Owner / RenewUntilMs
```

存活判定由 rpc 层以闭包形式随 `Lease` 传入，**deliver 不持有对 rpc 的引用**。

## 5. 状态格式变更

`internal/core/types.go` 的 `InflightState` 增两个字段：

```go
type InflightState struct {
	ExpireAtMs int64 `json:"expire_at_ms"`
	Attempts   int32 `json:"attempts"`
	Ordered    bool  `json:"ordered,omitempty"`
	// Owner 本次投递的持有者客户端标识（gRPC metadata 的 x-mq-client-id）。
	// 仅在该次投递启用了自动续租时写入；空串表示不参与续租判定。
	Owner string `json:"owner,omitempty"`
	// RenewUntilMs 本次投递允许续租到的绝对时刻（毫秒）。0 表示不续租。
	// 它是硬上限而不是目标：续租每次把 ExpireAtMs 推到
	// min(now+不可见期, RenewUntilMs)，越过它就必须走重投，
	// 否则一个「活着但卡死」的消费者能永久扣住消息，DLQ 永远等不到。
	RenewUntilMs int64 `json:"renew_until_ms,omitempty"`
}
```

**兼容性：零迁移。** 两个字段都是 `omitempty`，M1–M4 落盘的旧记录解码得
`Owner=""`、`RenewUntilMs=0`，判据（§6）第一条即为假，行为与今天逐字节相同。
这与 `Ordered` 当初的处理方式一致。

## 6. 续租判据

在 `receiveOnceLocked` 阶段 1 的 `ist.ExpireAtMs <= now` 分支内，四条**全部**
成立才续租，任一不成立走原重投路径：

| # | 判据 | 不成立时的含义 |
|---|---|---|
| 1 | `lease.Enabled()` | 本次轮询没启用续租（配置关闭、无 client-id 头、或 SimpleConsumer 接管了这个队列）→ 按其语义重投 |
| 2 | `ist.RenewUntilMs > now` | 旧格式记录，或续租预算已耗尽 → 必须重投 |
| 3 | `ist.Owner != ""` | 归属未知，无法判定存活 → 保守重投 |
| 4 | `lease.Alive(ist.Owner)` | 持有者的 Telemetry 会话已断 → 立刻重投（故障转移） |

判据顺序即求值顺序：1 保证 `lease.Alive` 非 nil（`Enabled()` 已含该检查），
判据 4 才可以直接调用。注意 2/3/4 判的是**记录里的持有者**（`ist.Owner`），
而 1 判的是**本次轮询方**（`lease`）——两者可以是不同的客户端，§13.2 讨论了
这种情形。

新的 `ExpireAtMs = min(now + invisible, ist.RenewUntilMs)`。
**`Attempts` 与 `Ordered` 原样保留**——这是承重的：

- 收据句柄由 `(group, topic, queue, offset, attempts)` 编码，`ChangeInvisible`
  和 `Ack` 都拿 `attempts` 做陈旧句柄校验（`deliver.go` 的
  `changeInvisibleLocked`）。续租若动了 `attempts`，等于把持有者手里那个还在
  用的句柄当场作废——它处理完回来 ack 会被拒，消息必然重投，续租的目的
  完全落空。
- `Ordered` 丢了，顺序锁判据 `orderedBusy` 会漏看这条记录，顺序即破（原重投
  路径的注释已经写明这一点，续租路径同理）。

## 7. 客户端存活判定

### 7.1 客户端标识

`x-mq-client-id` 是协议内置头，官方 Go SDK 在 `client.go:668-700` 的
`sign(ctx)` 里对**每一个**出站请求（含 Telemetry 流、含未开鉴权时）都附带；
`definition.pb.go` 里还有专门的错误码注释「Request is rejected due to missing
of x-mq-client-id header」。所以这不是自造约定。

`internal/rpc/auth.go:122` 已经在读 metadata，取这个头是同一套机制，新增一个
小工具函数即可。

**取不到时**：视为不启用续租（保守退化），打 Debug 日志。不拒绝请求——手写
客户端不带这个头是合法的，它只是享受不到续租。

### 7.2 会话索引

`internal/rpc/sessions.go` 的 `session` 增 `clientID string` 字段，`sessions`
注册表增按 clientID 的索引：

```go
// byClient 客户端标识 → 该标识当前活跃的会话数。
// 用计数而非 map[string]*session：同一个 client id 理论上可以并存多条
// Telemetry 流（重连窗口内新旧流短暂共存），用指针会让后注册的覆盖先注册的，
// 先注册的注销时把整个条目删掉，导致「客户端其实活着但被判为死」。
byClient map[string]int
```

新增 `func (s *sessions) aliveClient(id string) bool`。注册/注销挂在既有的
Settings 协商成功点与 `defer` 注销点上，不新增生命周期。

### 7.3 判定的固有窗口

Telemetry 流断开到重连之间，判定会说「死了」，于是消息被重投——**这恰好是
今天的行为**（今天无条件重投），所以不是回归，只是那一刻少续了一次租。反向的
误判（客户端进程已死但 gRPC 还没感知到流断）由 §5 的 `RenewUntilMs` 硬上限
兜底。

## 8. 取件参数：为什么用变参而不是改签名

`deliver.Receive` 只有 **1 处生产调用点**（`internal/rpc/receive.go:148`），
但有约 **90 处测试调用点**。B13.1 把 `*TagFilter` 改成 `Filter` 时，正是这批
调用点造成了「编译期无感、运行期 panic」的清扫成本（见 backlog B13.1 的
「接口化地雷」）。本次不重蹈：

```go
// Lease 本次取件的租约参数。三个字段任一为零值即视为不启用续租——
// 调用方漏配任何一项都退化成 M1 起的固定不可见期行为，不会半开。
type Lease struct {
	Owner    string            // 持有者客户端标识（x-mq-client-id）
	MaxRenew time.Duration     // 单次投递允许续租的总时长上限
	Alive    func(string) bool // 判定某持有者是否仍有活跃 Telemetry 会话
}

func (l Lease) Enabled() bool {
	return l.Owner != "" && l.MaxRenew > 0 && l.Alive != nil
}

// ReceiveOption 取件可选参数。
type ReceiveOption func(*receiveOpts)

// WithLease 为本次取件启用自动续租。
func WithLease(l Lease) ReceiveOption

func (d *Deliverer) Receive(ctx context.Context, group, topic string,
	queueID uint32, maxMsgs int, invisible, wait time.Duration,
	filter Filter, opts ...ReceiveOption) ([]*core.Message, error)
```

变参尾置，**90 处既有调用点一字不改照常编译**，唯一的生产调用点加一个
`WithLease(...)`。

存活判定以闭包**随调用传入**而不是注入 `Deliverer`，是刻意的：会话注册表由
`rpc.Server` 持有，而 `deliver.New` 在它之前构造，注入需要一个构造后 setter，
那会留下「忘了调 setter 就静默不续租」的哑火面。随调用传入让依赖在唯一的
生产调用点显式可见，漏了当场就能看出来。

## 9. 配置

沿用扁平风格（与 `default_invisible_duration` 一致）：

```yaml
# 消费者声明 auto_renew 时，是否在其存活期间自动续租不可见期。
auto_renew_enabled: true
# 单次投递允许续租的总时长上限。超过即按正常过期重投（Attempts++、可能进 DLQ）。
auto_renew_max_duration: 10m
```

| 项 | 默认 | 理由 |
|---|---|---|
| `auto_renew_enabled` | `true` | 见 §12.1——这是需要用户复核的假设 |
| `auto_renew_max_duration` | `10m` | 兼顾「覆盖绝大多数慢 handler」与「卡死的消费者最多扣住消息 10m」。Apache proxy 的对应上限是 3h，对 sq 的定位（中小规模、单机/小集群）过于宽松；10m 已是 `default_invisible_duration` 默认值的 10 倍 |

Go 侧字段：

```go
AutoRenewEnabled     bool   `yaml:"auto_renew_enabled"`
AutoRenewMaxDuration string `yaml:"auto_renew_max_duration"`
```

默认值机制沿用既有形态——`Load` 在 unmarshal **之前**把默认值铺在结构体上
（`config.go:156-171`），所以 `bool` 默认 `true` 无需 `*bool`：YAML 省略该键即
保持 `true`，显式写 `false` 则被覆盖。

访问器 `AutoRenewMax()` 解析失败或 `<= 0` → 回落 `10m` 并打 Warn。
**不返回 0**——理由与 B13.2 给 `DefaultInvisible()` 补兜底时相同：0 会让续租
判据恒假，把一个配置笔误变成静默的功能关闭。

`AutoRenewEnabled` 没有这个隐患：用 `&config.Config{...}` 字面量构造（测试常见）
时它得 `false`，即续租关闭、退化为当前行为——**失败方向是安全的**，因此不需要
额外兜底。

## 10. 对 B13.2 用例 6 的同步改造（承重）

`TestOfficialGoSDKPushRedeliverAfterInvisibleExpiry` 的构造是：单 PushConsumer、
消费线程 1、首投占住线程 8s、broker 不可见期 5s，断言消息被重投且句柄变化。
PushConsumer 下发 `auto_renew=true`，所以本特性一上线，这条消息会被续租而
**永不重投，用例必红**。

**这是预期的行为变更，不是回归。** 处置：

1. 该用例的 broker mutate 增加 `c.AutoRenewEnabled = false`，并在注释里写明
   为什么——它测的是「不可见期到期兜底」这条路径本身，必须在续租关掉的前提下
   才观测得到。
2. **新增用例 7：续租使慢 handler 不被重投**。与用例 6 同构造（不可见期 5s、
   单消费线程、首投 hold 8s），但续租开启（默认即开）。时间线：t=0 首投；
   t≈5s broker 侧不可见期到期，轮询触发扫描，判据四条全真 → 续租到 t≈10s
   而非重投；t=8s handler 返回，用**仍然有效**的原句柄 ack 成功。

   断言（观测窗口 = hold 8s + 静默 12s，共 20s）：
   - 7a：`len(got) == 1`——全窗口只投递过一次，没有重投。这里必须用 `==` 而
     非 `>=`：本用例证明的就是「不多投」，`>=` 会让它恒真。
   - 7b：静默段必须**长于一个完整的不可见期**（12s > 5s）。窗口短于不可见期时
     「ack 成功」与「ack 失败后等待重投」观测一致，7a 就成了假绿——这条纪律是
     B13.2 终审抓 1b 假绿的教训，不能再犯。
   - 7c：`got[0].attempt == 1`——续租没有污染投递次数（§6 的承重不变式）。
     它同时反证了句柄未失效：若续租改了 `Attempts`，ack 会被拒，t≈13s 必然
     出现第二次投递，7a 先红。

用例 7 是本 spec 唯一的**行为正确性**验收，不能省。

## 11. 测试计划

| 层 | 用例 | 验证点 |
|---|---|---|
| `internal/config` | `auto_renew_max_duration` 缺省 / 非法 / 合法三态 | §9 的兜底不返回 0 |
| `internal/core/deliver` | 续租判据四条各自不成立时都走重投 | §6 判据表逐条 |
| `internal/core/deliver` | 续租后 `Attempts` / `Ordered` 不变 | §6 承重不变式 |
| `internal/core/deliver` | 续租不越过 `RenewUntilMs`，越过后正常重投并 `Attempts++` | §5 硬上限 |
| `internal/core/deliver` | 旧格式 inflight（无 Owner/RenewUntilMs）行为不变 | §5 零迁移 |
| `internal/core/deliver` | 顺序消息续租期间 `orderedBusy` 仍为真、顺序锁不失效 | §6 + §13.1 |
| `internal/rpc` | `aliveClient` 在同 clientID 多流并存/逐个注销下的正确性 | §7.2 计数而非指针 |
| `internal/rpc` | 无 `x-mq-client-id` 头时退化为不续租，请求不被拒 | §7.1 |
| `test/e2e` | 用例 6 改造 + 新增用例 7 | §10 |

## 12. 需要用户复核的假设

### 12.1 `auto_renew_enabled` 默认 `true`（最需要复核的一条）

**取 `true` 的理由**：这是协议正确的行为，也是 SDK 的期待；当前行为从客户端
视角看就是个 bug；有 §5 的硬上限兜底；关掉只需一行配置。

**反面**：它改变了每一个现存 sq 部署的核心投递行为，而这个判断是在用户不在场
时做的。

**若用户不同意**：改默认为 `false` 的成本是一行——`internal/config` 的默认值
与 `sq.example.yaml` 的注释，用例 7 的 broker mutate 相应改为显式开启。设计的
其余部分不受影响。

### 12.2 `auto_renew_max_duration` 取 10m 而非 Apache 的 3h

见 §9 表格。10m 是保守取值：宁可让极慢的 handler 撞一次重投，也不要让一个卡死
的消费者扣住消息 3 小时。

### 12.3 分支基线

本项必须改 B13.2 的用例 6，因此基线取 `feat/push-consumer-e2e`（b364deb），
形成 `main → feat/sql92-filter → feat/push-consumer-e2e → 本项` 的链。
**main 不动**，三条分支的合并决策全部留给用户。

## 13. 边界与风险

### 13.1 顺序消息

续租一条 `Ordered` inflight 会让队列顺序锁被持有更久。这在语义上是**正确的**
——持有者确实还在处理这条队头消息，后续消息本就该等。但它把「卡死的消费者
阻塞整个队列」的时长从一个不可见期放大到 `auto_renew_max_duration`。硬上限是
唯一的兜底，这也是 §12.2 不取 3h 的直接原因。

### 13.2 重平衡后队列换主

队列被重新分配给同组的另一个消费者后，新主的轮询会触发扫描，此时判据 3
（原持有者存活）通常仍为真（老消费者进程还活着，只是不再负责这个队列），于是
消息继续被续租，直到硬上限。表现是：重平衡后这条消息的接管被推迟，最坏
`auto_renew_max_duration`。

**接受这个代价**，不做补救。补救需要 broker 侧知道「谁现在拥有哪个队列」，
而 sq 的 `QueryAssignment` 是无状态计算的（不落盘归属），要为此引入一份归属
状态，成本远超收益——重平衡本就是低频事件，且推迟接管不丢消息。

### 13.3 进程重启 / 集群 leader 切换

`Owner` 和 `RenewUntilMs` 都是持久化的，但**会话注册表是内存态**。重启或
leader 切换后，原持有者在新节点上没有会话记录 → 判据 3 为假 → 消息立刻按
正常过期重投。这是安全的退化方向（宁可重投也不要错误地扣住），无需额外处理。

### 13.4 明确不做

- **不做「持有者一死立刻释放 inflight」的主动故障转移**。它很诱人（能把故障
  转移从「一个不可见期」压到「秒级」），但那是一条独立的可用性优化，与
  `auto_renew` 的协议语义无关，混进来会让本项的验收范围失焦。若认为有价值，
  另立 backlog 条目。
- **不给 SimpleConsumer 加续租**。它按协议下发 `auto_renew=false` 且必填
  `invisible_duration`，租期本就归它自己管，它还有 `ChangeInvisibleDuration`
  可以主动延长。
- **不改动 `Attempts` / DLQ / 退避的任何语义**。续租只影响「何时重投」，不影响
  「重投时怎么算」。

## 14. 日志与注释要求

按 `instrumenting-code`，实现必须覆盖：

- **续租发生时** Debug 一条：group / topic / queue / offset / owner / 新的
  `ExpireAtMs` / 距硬上限剩余。这条是慢消费者排障的唯一线索，缺了就只能靠猜
  「为什么这条消息没重投」。
- **续租预算耗尽转重投时** Info 一条，明确写出「续租上限已到，转正常重投」，
  并带 owner 与累计续租时长。这是 §13.1 队列阻塞的诊断入口。
- **判据 3 为假（持有者已断线）导致重投时** Info 一条，带 owner。这是故障转移
  实际发生的证据。
- 新增文件/字段/导出方法的注释按项目规范：`Lease` / `WithLease` /
  `aliveClient` / 两个新 `InflightState` 字段各自说明「为什么」，不复述「是什么」。
