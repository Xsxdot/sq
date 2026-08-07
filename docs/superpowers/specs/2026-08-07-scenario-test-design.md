# 真实场景测试（scenario test）设计

日期：2026-08-07（评审修订版）
状态：已与用户对齐，经子 agent 评审修订，待实施
关联：`docs/superpowers/specs/2026-08-06-sq-mq-design.md`（里程碑 M1–M5b 已完成，M6 事务消息未做，M7 发布打磨、v2 Raft 集群在后）

## 1. 目标与定位

现有测试分两层：`internal/` 各包的 stub 单测、`test/e2e/` 的「真实二进制 + 官方 SDK」按功能分文件的端到端测试。两层都缺「多种流量并发混跑 × 故障同时发生」的交叉竞态覆盖。

本设计新增第三层：**场景混跑 + 故障注入**测试——

- 真实 broker 子进程 + 官方 RocketMQ Go SDK，多角色并发混跑普通/延时/FIFO 消息；
- 运行期注入 kill -9 重启、优雅停机重启、并发 Admin 操作；
- 结束后全局对账，校验一组明确的不变量（不丢、不冒、ack 语义、FIFO 有序、延时不早投、DLQ 溯源等）。

非目标（本期不做，明确排除）：

- 磁盘水位拒写的场景化触发（实现成本高，缓做）；
- 事务消息（M6 未实现）；
- docker/systemd 形态与多节点集群（M7/v2，只预留抽象）；
- retention 后台清理与消费并发的竞态场景（记扩展挂点；本期只要求场景 topic 的 retention ≫ 测试时长，harness 启动时校验，防止清理干扰对账）；
- 认证开启形态与 fsync async 档（记扩展挂点，Profile 布尔位预留）；
- Jepsen 式通用线性化检验（投入产出不成比例）。

## 2. 形态决策记录

| 决策点 | 结论 | 理由 |
|--------|------|------|
| 测试形态 | 场景混跑 + 故障注入（非分立小用例、非纯 -race 压测） | 竞态产生在「流量 × 故障」交叉处，单一混跑场景逼出能力最强 |
| 时长档位 | 可调：短档 ~2–3 分钟进日常回归，环境变量升长档浸泡 | 同一套场景代码，只换规模参数 |
| 故障范围 | kill -9 重启、优雅停机重启、运行中 Admin 操作 | 用户选定；磁盘水位缓做 |
| broker 接入 | **纯自管进程**：harness 始终现场编译并拉起子进程，不支持外部地址 | 杀进程/重启类故障要求进程控制权；与 e2e 做法一致；规避官方 SDK 与 sq 自建 protobuf 绑定同进程 init panic |
| M7/v2 预留 | 只预留「集群/节点」接口抽象，不写任何 docker/集群代码 | 将来新增实现即可，场景与对账代码不动 |
| broker 编译选项 | scenario 的 broker 固定带 `-race` 编译 | 不变量「无 DATA RACE」否则形同虚设；2–10 倍性能退化已计入参数约束表（§3.1） |
| 「修改订阅组 max_attempts」事件 | **删除**（评审 R1）：Admin API 没有该端点（消费组仅 GET/DELETE/reset-cursor） | max_attempts 改用配置旋钮 `default_max_attempts` 在 harness 启动时设定；将来若补 `PATCH /admin/groups/{name}` 端点可作为新事件加入 |
| 位点重置方向 | **只做向后重置**（评审 L3） | 向前重置=永久跳过消息，会击穿「不丢」；向后重置的重投用事件窗豁免处理 |
| 收敛方式 | **排水模式**（评审 L1）：只停发送侧，消费侧全员转「收到即 ack」持续拉取 | sq 重投是惰性的（Receive 时检查，无后台扫描），停消费者则过期 inflight 永不归零 |

## 3. 目录与运行方式

- 新目录 `test/scenario/`，独立 build tag `scenario`；`go test ./...` 与 `make e2e` 均不包含它。
- Makefile 新增 `scenario` target：

```make
scenario:
	go test -tags scenario -count=1 -timeout 60m ./test/scenario/...
```

- 档位与参数全部收敛到一个 **Profile** 结构（时长、各类 producer/consumer 数量、发送速率、故障频率、收敛超时、broker 配置覆写）。短档/长档是两组默认值：
  - 短档（默认）：~2–3 分钟流量期 + 收敛期，日常回归可跑；
  - 长档：`SQ_SCENARIO_DURATION=30m`（或更长）升档，其余规模参数随时长按 Profile 内规则放大。
- 随机性统一走单一种子：`SQ_SCENARIO_SEED` 指定；未指定则随机生成并在日志第一行打印。同种子 + 同 Profile 复现**同一事件时间线**（真实进程与 SDK 的并发交错不可复现，种子的价值是复现事件序列与流量配方，不是复现 bug 本身）。
- broker 现场编译与拉起逻辑从 `test/e2e/` 抽成共享 helper，放 `test/internal/`（无 build tag），e2e 与 scenario 共用，不复制粘贴。

### 3.1 参数自洽约束表（评审 R2/R3/R7）

短档默认值必须满足以下不等式，Profile 构造时断言校验，违反即测试启动失败：

| 约束 | 理由 |
|------|------|
| `default_max_attempts` 设为 2–3（经 broker 配置覆写） | 重投退避硬编码 10s 起 ×2 封顶 5min；默认 16 次需 ~55 分钟才进 DLQ，任何档位都等不起 |
| 捣乱组 invisible（ReceiveMessage invisibleDuration）≈ 3s | 与上一条配合，首条死信约在流量开始后 30–60s 出现，短档内 DLQ 事件可达 |
| `invisible + Σ退避(max_attempts) + 余量 < DLQ 事件 deadline < 流量期时长` | DLQ 重发是条件触发事件（§6），前置条件必须在 deadline 前成立 |
| 所有消费者 `awaitDuration ≤ 3s` | GracefulStop 会等在途长轮询；README 的 30s 强杀兜底就是按 20s 长轮询取的。优雅停机 ≤5s 断言只有在短轮询下才守得住 |
| 延时窗口 clamp：`deliveryAt ≤ 流量期截止时间`；发送时刻剩余时长不足窗口下限时该 producer 停发延时消息 | 收敛超时的推导需要「最晚 deliveryAt」这个确定上界 |
| `收敛超时 ≥ 最晚 deliveryAt + 排水时间 + 最大退避档（10×2^(max_attempts-1) s）+ 余量` | 排水期内最后一轮重投可能刚好落在最大退避档上 |
| 场景 topic 的 retention ≫ 测试总时长（用默认 3 天即可），harness 启动时校验配置 | 防 retention 清理删掉未对账消息击穿「不丢」 |
| 发送速率按 -race 性能退化（2–10 倍）预留余量 | broker 固定 -race 编译，速率过高会把长档变成纯背压测试 |

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

v1 唯一实现 `ProcessCluster`：单节点，子进程运行现场编译（带 `-race`）的 `cmd/sq` 二进制，数据目录在测试临时目录内，重启复用同一数据目录（考察恢复路径）。每次进程生命周期的 stdout/stderr 写独立日志文件。

扩展路径（本期不实现）：M7 新增 docker 节点实现跑发布镜像；v2 新增三节点 Raft 集群实现（kill leader、少数派存活等场景）。届时只加实现与新场景，不改现有场景与对账器。

## 5. 流量演员与账本

### 5.1 演员

每个演员一个 goroutine，数量由 Profile 定：

- **普通 producer**：随机 topic/tag/keys 混发；
- **延时 producer**：随机 deliveryTimestamp（几秒到几十秒窗口，按 §3.1 clamp 规则截断）；
- **FIFO producer**：若干 MessageGroup，组内载荷带严格递增 seq；FIFO 使用专用 topic（README 建议，避免与普通消息混发的队头阻塞干扰对账）。**演员纪律（评审 L4）**：某 seq 发送得到 indeterminate 或 failed 时，必须**同 seq 重发直到 confirmed 才推进**——否则真丢的 indeterminate 会造成合法空洞，FIFO 不变量无法校验（若首发实际已落盘，重发产生 broker 侧重复，由去重语义吸收）；
- **正常 consumer 组 ×2，订阅同一普通 topic**（评审 C3）：SUB_ALL，收到即 ack，各自独立对账——覆盖多消费组位点独立性 × 故障；
- **tag 过滤 consumer 组**（评审 C1）：订阅表达式为单 tag 或 `a || b`，收到即 ack。账本记录每个组的订阅表达式，「不丢」按订阅匹配后的子集判定——同时覆盖「被过滤消息永久越过位点 × kill」的持久化竞态；
- **捣乱 consumer 组**：随机不 ack、慢 ack、收到即断连——专门逼重投与 DLQ 链路。捣乱组订阅专用 topic，避免把正常组的对账搅浑。

broker 被 kill/重启期间，演员对错误的预期行为：发送侧把错误按 5.2 分类入账后继续（FIFO 演员按上述纪律重发）；消费侧容忍拉取失败并重试。演员自身不做「broker 是否存活」的判断。

### 5.2 账本

并发安全、append-only 的事件账本，测试全程唯一事实来源：

- **消息载荷自描述**：载荷内编码 `{actorID, seq, messageGroup, sentAt, deliveryAt}`，crash 后不依赖 broker 状态也能对账；
- **发送侧三态**，错误分类表（评审 R5）：

| 三态 | 判定 | 对账语义 |
|------|------|----------|
| `confirmed` | 收到 SendReceipt，msgId 入账 | 必须最终被消费到（按组订阅匹配） |
| `indeterminate` | 传输类错误：`Unavailable`、`DeadlineExceeded`、连接 reset/EOF 及 SDK 内部重试后的最终传输错误 | 可出现可不出现；出现则内容必须正确 |
| `failed` | 明确业务错误码：TOPIC_NOT_FOUND、FORBIDDEN、MESSAGE_BODY_TOO_LARGE、ILLEGAL_* 等 | 不应出现在消费侧 |

  注意官方 SDK 发送内部有自动重试，同一载荷可能产生「异 msgId 的合法副本」，对账按载荷通道归类（见不变量 2）；
- **消费侧**：记录每条消息的每次投递（时间、attempt、队列、消费组）与每次 ack 的结果。**不假设单条消息 attempt 单调递增**（位点重置后 attempt 从 1 重计，评审 L3）；
- **事件流**：故障调度器的每个事件（kill、restart、admin 操作）带时间戳入账。位点重置事件在执行前先 `GET /admin/groups/{name}` 快照旧位点，连同目标位点一并入账（评审 R6）——「重置点之后」的判定依据。

匹配通道：msgId 为主、载荷自描述为辅（indeterminate 消息没有 msgId，只能靠载荷）。

## 6. 故障时间线

独立的调度器 goroutine，采用**条件触发 + 截止时间**模型（评审 L7）：每类事件挂一个前置谓词与 deadline——

- 前置谓词满足且到达随机触发时刻（带种子抖动）→ 执行事件；
- 超过 deadline 前置仍不满足 → 测试直接失败并报「参数不自洽」（这是 §3.1 约束表算错的信号，不是环境抖动）；
- 「每类事件至少发生一次」在运行结束期校验，不做时间线预生成。

**进程类**：

- kill -9（前置：broker 存活）→ 等 1–3 秒 → 重启；
- 优雅停机 SIGTERM（前置：broker 存活）→ 断言退出码 0、**停机耗时 ≤ 5s**（前提是 §3.1 的 awaitDuration ≤ 3s 约束；宽松上界防 Telemetry 收尾逻辑回归）→ 重启。

**Admin 类**（与流量并发，专打管理面/数据面竞态）：

- **向后位点重置**（前置：目标组已有消费进度）：先快照旧位点，再重置到旧位点之前的随机位置；只向后，不向前（向前=永久跳过，击穿「不丢」）；
- **DLQ 重发**（前置：`%DLQ%{捣乱组}` 非空）：触发后进入「重发事件窗」，窗内语义见不变量 6；
- **动态建 topic**（前置：无）：建新 topic 并让某 producer 立刻使用（auto_create 竞态）。

**调度约束**：事件间留最小间隔；kill 后必须确认 broker 恢复、演员重连成功后才允许注入下一个事件——防止测试把自己饿死。

## 7. 收敛与对账不变量

### 7.1 收敛：排水模式（评审 L1/L2）

sq 的重投是惰性的（Receive 时检查，无后台扫描 goroutine），admin 的 inflight 读数对过期条目不区分——**停消费者等归零是等不到的**。因此收敛流程为：

1. 停发送侧演员、停故障注入；
2. 消费侧**全员切换为排水模式**：包括捣乱组在内的所有组都转为「收到即 ack」持续 Receive（捣乱组由此把自己欠下的重投与 DLQ 转移排干）；
3. 等 admin 堆积与 inflight 读数双归零（超时按 §3.1 约束表推导；延时消息按最晚 deliveryAt 等待）；
4. 停消费侧，进入对账。

### 7.2 不变量清单

1. **不丢**：每条 `confirmed` 消息，对每个**订阅匹配**它的消费组——被该组消费到，或在该组 `%DLQ%{group}` 中且带完整 `sq-origin-topic/queue/offset` 溯源。tag 不匹配的组不参与判定；
2. **不冒**（评审 C6）：消费到的每条消息必须匹配账本中的某次发送（confirmed 或 indeterminate）；SDK 重试产生的「同载荷、异 msgId」副本按载荷通道归类，不算幽灵；账本外的消息 = 失败；
3. **ack 语义**：ack 成功后该消息不再重投。豁免（账本单独计数并在报告中上报，不算失败）：kill 窗口内 ack 响应丢失导致的重复；**向后位点重置事件窗内的重投**（重置清 inflight、attempt 重计是设计语义）；DLQ 重发副本；
4. **FIFO**：每个 MessageGroup 的消费序列去重后 seq 递增；空洞仅在**该 seq 能在 DLQ 中找到**时合法（FIFO 超限入 DLQ 后推进是 sq 特色语义，评审 L5——此豁免同时把该语义纳入校验）；重复只允许呈重投/重置产生的前缀重放形态；
5. **延时**：任何消息的首次投递时间 ≥ deliveryTimestamp − 1s（容差覆盖 ~100ms 调度精度）；
6. **DLQ**：捣乱组超过 max_attempts 的消息必到对应 DLQ；「原队列不再投递」限定在 **DLQ 重发事件窗之外**（评审 L6：重发保留原 msgId、经 produce.Append 以新 offset 写回原 topic，窗内同 msgId 再现是设计语义）。重发副本按「同 msgId + 新 offset」识别、单独归类；FIFO 消息被重发后失去 MessageGroup（回归普通消息），其乱序出现由去重吸收；
7. **位点重置**：重置事件后，从快照旧位点回退到目标位点之间的消息被重新投递（依据事件账本中的位点快照判定，允许重复）；
8. **Keys 检索**（评审 C2）：终局抽样若干 confirmed 且带 keys 的消息，走 Admin API 按 key 检索必须命中；
9. **终局对账**：最后一次重启后，admin 总览读数（堆积、inflight、topic 列表）与账本推算一致；
10. **进程健康**：全程 broker 日志无 panic、无 `DATA RACE`（broker 带 -race 编译）；每次优雅停机耗时达标、退出码 0。

## 8. 失败诊断输出

任一不变量失败时，一次性打包写入测试临时目录并打印路径：

- 种子与完整 Profile（复现事件时间线的入口；并发交错不可复现，见 §3）；
- 事件时间线全文（含位点快照）；
- 违反不变量的具体消息清单：msgId、载荷、发送/每次投递/每次 ack 的完整轨迹；
- broker 全程日志（每次进程生命周期一个文件）；
- 收敛后的 admin 总览快照。

目标：**不重跑即可定位**。

## 9. 后续扩展挂点

- **M6 事务消息**：新增事务 producer 演员 + 半消息回查相关不变量；
- **M7 发布打磨**：新增 docker 节点实现，同一场景跑发布镜像做验收；
- **v2 Raft 集群**：新增三节点集群实现与 kill-leader、滚动重启场景；不变量原样复用；
- **retention × 消费竞态**：把 `retention_check_interval` 调小、保留时长压进测试窗口，专测清理与消费/对账并发（含孤儿 inflight 兜底路径）；
- **认证形态**：Profile 布尔位开 AK/SK，覆盖认证态下 kill 后 SDK 重连重签名；
- **fsync async 档**：kill -9 下 OS 页缓存仍在，语义与 sync 相同可直接跑；作为 Profile 维度轮换；
- **修改订阅组 max_attempts 事件**：待 Admin API 补 `PATCH /admin/groups/{name}` 后加入。
