# 真实场景测试（scenario test）设计

日期：2026-08-07
状态：已与用户对齐，待实施
关联：`docs/superpowers/specs/2026-08-06-sq-mq-design.md`（里程碑 M1–M5b 已完成，M6 事务消息未做，M7 发布打磨、v2 Raft 集群在后）

## 1. 目标与定位

现有测试分两层：`internal/` 各包的 stub 单测、`test/e2e/` 的「真实二进制 + 官方 SDK」按功能分文件的端到端测试。两层都缺「多种流量并发混跑 × 故障同时发生」的交叉竞态覆盖。

本设计新增第三层：**场景混跑 + 故障注入**测试——

- 真实 broker 子进程 + 官方 RocketMQ Go SDK，多角色并发混跑普通/延时/FIFO 消息；
- 运行期注入 kill -9 重启、优雅停机重启、并发 Admin 操作；
- 结束后全局对账，校验一组明确的不变量（不丢、ack 语义、FIFO 有序、延时不早投、DLQ 溯源等）。

非目标（本期不做，明确排除）：

- 磁盘水位拒写的场景化触发（实现成本高，缓做）；
- 事务消息（M6 未实现）；
- docker/systemd 形态与多节点集群（M7/v2，只预留抽象）；
- Jepsen 式通用线性化检验（投入产出不成比例）。

## 2. 形态决策记录

| 决策点 | 结论 | 理由 |
|--------|------|------|
| 测试形态 | 场景混跑 + 故障注入（非分立小用例、非纯 -race 压测） | 竞态产生在「流量 × 故障」交叉处，单一混跑场景逼出能力最强 |
| 时长档位 | 可调：短档 ~2–3 分钟进日常回归，环境变量升长档浸泡 | 同一套场景代码，只换规模参数 |
| 故障范围 | kill -9 重启、优雅停机重启、运行中 Admin 操作 | 用户选定；磁盘水位缓做 |
| broker 接入 | **纯自管进程**：harness 始终现场编译并拉起子进程，不支持外部地址 | 杀进程/重启类故障要求进程控制权；与 e2e 做法一致；规避官方 SDK 与 sq 自建 protobuf 绑定同进程 init panic |
| M7/v2 预留 | 只预留「集群/节点」接口抽象，不写任何 docker/集群代码 | 将来新增实现即可，场景与对账代码不动 |

## 3. 目录与运行方式

- 新目录 `test/scenario/`，独立 build tag `scenario`；`go test ./...` 与 `make e2e` 均不包含它。
- Makefile 新增 `scenario` target：

```make
scenario:
	go test -tags scenario -count=1 -timeout 60m ./test/scenario/...
```

- 档位与参数全部收敛到一个 **Profile** 结构（时长、各类 producer/consumer 数量、发送速率、故障频率、收敛超时）。短档/长档是两组默认值：
  - 短档（默认）：~2–3 分钟，日常回归可跑；
  - 长档：`SQ_SCENARIO_DURATION=30m`（或更长）升档，其余规模参数随时长按 Profile 内规则放大。
- 随机性统一走单一种子：`SQ_SCENARIO_SEED` 指定；未指定则随机生成并在日志第一行打印。同种子 + 同 Profile 可复现同一事件时间线。
- broker 现场编译与拉起逻辑从 `test/e2e/` 抽成共享 helper，放 `test/internal/`（无 build tag），e2e 与 scenario 共用，不复制粘贴。

## 4. Harness：集群/节点抽象

场景与对账代码只面向以下接口编程：

```go
type Cluster interface {
    Start(ctx context.Context) error
    Nodes() []Node
    Endpoint() string      // SDK 接入点（v1 即唯一节点地址）
    AdminEndpoint() string // Admin HTTP 接入点
}

type Node interface {
    Kill() error                        // SIGKILL
    Stop() (time.Duration, error)       // SIGTERM 优雅停机，返回停机耗时
    Restart() error
    Alive() bool
}
```

v1 唯一实现 `ProcessCluster`：单节点，子进程运行现场编译的 `cmd/sq` 二进制，数据目录在测试临时目录内，重启复用同一数据目录（考察恢复路径）。每次进程生命周期的 stdout/stderr 写独立日志文件。

扩展路径（本期不实现）：M7 新增 docker 节点实现跑发布镜像；v2 新增三节点 Raft 集群实现（kill leader、少数派存活等场景）。届时只加实现与新场景，不改现有场景与对账器。

## 5. 流量演员与账本

### 5.1 演员

每个演员一个 goroutine，数量由 Profile 定：

- **普通 producer**：随机 topic/tag/keys 混发；
- **延时 producer**：随机 deliveryTimestamp（几秒到几十秒窗口，须落在测试时长内）；
- **FIFO producer**：若干 MessageGroup，组内载荷带严格递增 seq；FIFO 使用专用 topic（与 README 建议一致，避免与普通消息混发的队头阻塞干扰对账）；
- **正常 consumer 组**：SimpleConsumer，收到即 ack；
- **捣乱 consumer 组**：随机不 ack、慢 ack、收到即断连——专门逼重投与 DLQ 链路。捣乱组订阅专用 topic，避免把正常组的对账搅浑。

broker 被 kill/重启期间，演员对错误的预期行为：发送侧把错误按 5.2 的三态入账后继续；消费侧容忍拉取失败并重试。演员自身不做「broker 是否存活」的判断。

### 5.2 账本

并发安全、append-only 的事件账本，测试全程唯一事实来源：

- **消息载荷自描述**：载荷内编码 `{actorID, seq, messageGroup, sentAt, deliveryAt}`，crash 后不依赖 broker 状态也能对账；
- **发送侧三态**：
  - `confirmed`——收到成功响应，msgId 入账；**必须**最终被消费到；
  - `indeterminate`——发送窗口撞上 kill，响应丢失；消息可出现可不出现，出现则内容必须正确；
  - `failed`——收到明确业务错误；不应出现在消费侧；
- **消费侧**：记录每条消息的每次投递（时间、attempt、队列、消费组）与每次 ack 的结果；
- **事件流**：故障调度器的每个事件（kill、restart、admin 操作）带时间戳入账，供对账异常时对照「当时正发生什么」。

匹配通道：msgId 为主、载荷自描述为辅（indeterminate 消息没有 msgId，只能靠载荷）。

## 6. 故障时间线

独立的调度器 goroutine，在流量运行期内按 Profile 频率、带种子随机抖动排布事件：

**进程类**：

- kill -9 → 等 1–3 秒 → 重启；
- 优雅停机（SIGTERM）→ 断言退出码 0、**停机耗时 ≤ 5s**（README 承诺 ~0.04s 量级，宽松上界防 Telemetry 收尾逻辑回归）→ 重启。

**Admin 类**（与流量并发，专打管理面/数据面竞态）：

- 消费进行中对某组做位点重置；
- DLQ 有内容后触发 DLQ 重发；
- 动态建新 topic 并让 producer 立刻使用（auto_create 竞态）;
- 修改订阅组 max_attempts。

**调度约束**：事件间留最小间隔；kill 后必须确认 broker 恢复、演员重连成功后才允许注入下一个事件——防止测试把自己饿死。短档至少保证每类事件发生一次（时间线生成后校验，不满足则重排）。

## 7. 对账与不变量

对账前先**收敛**：停演员 → 停故障注入 → 等所有队列排空（admin 堆积与 inflight 读数归零，带超时；延时消息按最晚 deliveryAt 等待）→ 校验：

1. **不丢**：每条 `confirmed` 消息都被消费到，或在 `%DLQ%{group}` 中且带完整 `sq-origin-topic/queue/offset` 溯源；
2. **ack 语义**：ack 成功后该消息不再重投；kill 窗口内 ack 响应丢失导致的重复不算失败，账本单独计数并在报告中上报;
3. **FIFO**：每个 MessageGroup 的消费序列去重后 seq 严格递增无空洞；重复只允许呈重投产生的前缀重放形态；
4. **延时**：任何消息的首次投递时间 ≥ deliveryTimestamp − 1s（容差覆盖 ~100ms 调度精度）；
5. **DLQ**：捣乱组超过 max_attempts 的消息必到对应 DLQ，且原队列不再投递；
6. **位点重置**：重置事件后，重置点之后的消息被重新投递（按事件时间窗判定，允许重复）；
7. **终局对账**：最后一次重启后，admin 总览读数（堆积、inflight、topic 列表）与账本推算一致；
8. **进程健康**：全程 broker 日志无 panic、无 `DATA RACE`；每次优雅停机耗时达标、退出码 0。

## 8. 失败诊断输出

任一不变量失败时，一次性打包写入测试临时目录并打印路径：

- 种子与完整 Profile（复现入口）；
- 事件时间线全文；
- 违反不变量的具体消息清单：msgId、载荷、发送/每次投递/每次 ack 的完整轨迹；
- broker 全程日志（每次进程生命周期一个文件）；
- 收敛后的 admin 总览快照。

目标：**不重跑即可定位**；需要重跑时同种子复现。

## 9. 后续扩展挂点

- **M6 事务消息**：新增事务 producer 演员 + 半消息回查相关不变量；
- **M7 发布打磨**：新增 docker 节点实现，同一场景跑发布镜像做验收；
- **v2 Raft 集群**：新增三节点集群实现与 kill-leader、滚动重启场景；不变量 1–8 原样复用。
