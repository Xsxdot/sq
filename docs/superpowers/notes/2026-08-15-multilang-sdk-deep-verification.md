# 多语言 SDK 深水区验证（Java / Python / C#）

> 2026-08-15。目标：补 `2026-08-14-four-gaps-verification.md`「仍未覆盖」里的
> 「多语言 SDK 深水区」——08-14 只做了最小往返冒烟，事务、FIFO、push 消费、
> SQL92 过滤都没验。本轮用官方 Python 与 C# SDK 逐特性验证。

## 结论

**Java（参考实现）10/10 全过**，且**挖到一条真缺陷**：sq 不发 `ReceiveMessageResponse`
信封层的 `delivery_timestamp`，导致官方 **Python SDK 的 PushConsumer 完全不可用**。
已记为 [B13.8](../backlog.md)，**08-16 已修复并验收**（见下方「已修复」一节）。

| 特性 | Java | Python | C# | 判据 |
|---|---|---|---|---|
| SQL92 复合表达式 | ✅ | ✅ | ✅ | 命中集合精确相等，反例逐条排除 |
| SQL92 三值逻辑 | ✅ | ✅ | — | 属性缺失时 `NOT (age > 18)` 不误投 |
| SQL92 `IS NULL` | ✅ | ✅ | — | 精确选出属性缺失那条 |
| SQL92 `BETWEEN` | — | — | ✅ | 区间 + 等值复合 |
| FIFO 同组顺序 | ✅（1 队列） | ✅（**4 队列**，10 条按序） | ✅（1 队列） | 见下方「FIFO 证明力说明」 |
| FIFO 同组同队列 | — | ✅ | — | 10 条全部落在同一队列 |
| FIFO 路由非退化 | — | ✅ | — | 5 个 group 各自收敛到单一队列，合计用到 4 个不同队列 |
| 事务 显式提交/回滚 | — | ✅ | — | committed 可见、rolledback 35s 内不可见 |
| 事务 已裁决不再回查 | — | ✅ | — | checker 未被触发 |
| **事务 孤儿回查** | ✅ | — | ✅ | broker 主动回查 2 次，COMMIT/ROLLBACK 裁决分别生效 |
| **PushConsumer + AutoRenew** | ✅ | ✅（08-16 修复后） | ✅ | 处理 45s > 30s 不可见期，零重投 |
| 定时消息 | ✅ | ✅ | — | 15.1s 后到达；早于 12s 判为未生效 |
| Recall 撤回（含对照组） | ✅ | ✅ | 服务端已证 | 对照组如期到达 + 被撤回那条始终不出现 |

**AutoRenew（B13.5）由此第一次被非 Go 的真实客户端验证**：此前只有 Go SDK 的 e2e，
现在 Java 与 C# 各独立验过一遍。

### FIFO 证明力说明

Java 与 C# 的 FIFO topic 是自动创建的，`default_queue_nums: 1`——**单队列天然按
offset 有序，那两条 PASS 的证明力很弱**，只能说明「没有乱序返回」。真正的判据在
Python 侧：topic 显式建 4 个队列，验的是

1. 同一 `message_group` 的 10 条**全部落在同一队列**（顺序是路由的结果，
   路由错了顺序无从谈起）；
2. **路由非退化**——再发 4 个 message group 各 2 条，要求每组各自收敛到单一队列、
   且全体用到 ≥2 个不同队列。实测用到 4 个。只看「主组 10 条全在队列 0」无法区分
   「按组哈希正确」与「哈希坏了、所有消息都进队列 0」。

## 发现（真缺陷）：`ReceiveMessageResponse` 缺信封层 `delivery_timestamp`

`proto/apache/rocketmq/v2/service.proto:105` 定义：

```protobuf
message ReceiveMessageResponse {
  oneof content {
    Status status = 1;
    Message message = 2;
    // The timestamp that brokers start to deliver status line or message.
    google.protobuf.Timestamp delivery_timestamp = 3;
  }
}
```

注释写明这一帧**由 broker 发**。sq 从不发：`ReceiveMessageResponse_DeliveryTimestamp`
在仓库里只出现于生成的 pb 代码，`internal/rpc/receive.go` 零引用。

（注意别和 `SystemProperties.DeliveryTimestamp` 混淆——那是**每条消息**的定时投递
到期时间，sq 填得好好的，是另一回事。）

### 失败链

1. `consumer.py:225` —— `if len(messages) > 0 and transport_delivery_timestamp:`
   字段缺失 → 不给 `msg.transport_delivery_timestamp` 赋值，保持 `None`
2. `client_metrics.py:151` —— `latency = current - transport_delivery_timestamp`
   → `TypeError: unsupported operand type(s) for -: 'int' and 'NoneType'`
3. `push_consumer.py:266` —— 该异常被 `__handle_received_message` 的 `except` 吞掉
4. 同一 `try` 块里**排在它后面**的 `cache_messages` 与 `execute_consume` 因此都不执行
   —— **listener 永远不触发**
5. 转入 `__execute_receive_later`，无限重试；消息卡在 in-flight

服务端视角完全正常：`/admin/groups/<组>` 显示 `cursor` 已推进、`inflight: 1`。
即消息**已经投出去了**，只是客户端在处理阶段崩了。

### 为什么至今不可见

Go SDK（`simple_consumer.go:257`）、C# SDK 与 **Java 参考实现（5.2.1）** 都只是把
该字段存进变量、容忍缺失。所以 Go e2e 全绿，C# 与 Java 的 PushConsumer 实测完全正常。
**只有 Python 严格依赖它**，而 Python 此前只做过最小往返冒烟（SimpleConsumer 路径
不走 `receive_after`，不受影响）。

这正是单语言验证照不出、必须多语言交叉才能抓到的那类缺口。

**定级**：补测 Java 后确认参考实现不受影响，故 B13.8 优先级由「高」下调为「中」
——它不是「协议实现整体错」，而是「四门 SDK 里 Python 这一门用不了」。

### 影响与修法

- **影响**：Python 用户的 push 消费完全不可用；Python 的 SimpleConsumer 不受影响。
  Java / Go / C# 全部不受影响，AutoRenew 在 Java 与 C# 上均已验证正常。
- **独立复现**：在两台不同机器、两个不同 broker 实例上各复现一次，客户端日志的
  异常文本逐字一致（`unsupported operand type(s) for -: 'int' and 'NoneType'`），
  排除环境偶然。
- **修法预计极小**：在 `receive.go` 投递消息时补发一帧
  `&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_DeliveryTimestamp{...}}`。

### 已修复（2026-08-16）

设计见 [spec](../specs/2026-08-16-receive-delivery-timestamp-design.md)，
实现见 [plan](../plans/2026-08-16-b13.8-receive-delivery-timestamp.md)。
改动只有一处：`ReceiveMessage` 在确认本批有消息后、进入发送循环前补发一帧
信封层 `delivery_timestamp`，取值为该时刻的 `time.Now()`。

两处**刻意偏离参考实现**，判据写在 spec 的 D1/D2：

1. **只在本批确有消息时发**（上游 `ReceiveMessageResponseStreamWriter.onComplete()`
   在 `finally` 里无条件发）。空长轮询是常态热路径，而没有任何已知 SDK 在空响应
   下读它——Python 的赋值条件带 `len(messages) > 0`，Go 只把它当
   `fromProtobuf_MessageView2` 的入参。
2. **放批首而非批尾**（上游放最后）。sq 的 status 在末尾、被当作「本批已全部
   发完」的终止信号，其后再挂 metadata 帧自相矛盾；批首也是 proto 注释
   "start to deliver" 的字面语义。帧序对客户端无影响：Python 与 Go 都是收完
   整条流再按 oneof 分类。

**验收**（联想真机 `100.90.99.61`，broker 与客户端同机，配置同上文「环境」）：

| 判据 | 结果 |
|---|---|
| **承重：Python PushConsumer** | **PASS**——listener 收到 `slow-one`（第 1 次），45s 处理跨过 30s 不可见期零重投 |
| 客户端日志 | 修复后该轮 `unsupported operand type` **0 次**；修复前同一脚本同一机器在 `23:32:23` 有 1 次 |
| Python 全套 | **12 项 0 失败**（08-15 是 12 项 2 失败：push=本条、FIFO=取样预算） |
| Java 深水区 | **10 项 0 失败**——多一帧没有打坏参考实现 |
| Go e2e 全套 | **48 PASS / 0 FAIL / 2 SKIP**（`cd test/e2e && go test -tags e2e -count=1 -timeout 40m ./...`，1612s）。跳过的两条是吞吐基准（`TestClusterWriteThroughput` / `TestExternalClusterWriteThroughput`，需显式开关），与 08-15 Linux 基线的通过/跳过集合逐条一致 |
| 单测 | `internal/rpc` 新增 2 条：首帧位置 + 取值落在调用时间窗内；空长轮询仍只有一帧 status。两条各做过变异验证（删发送点 / 取值改定值，都转红） |

**修复前后是真对照**，不是「修完跑一遍」：同一台机器、同一份 `py_deep.py`、
同一份 broker 配置，只换二进制。修复前 push 用例失败且客户端日志有那条 TypeError，
修复后用例通过且该条日志不再出现。

**未覆盖**：C# 未回归——它的运行环境在已失联的阿里云机上，联想机没有 .NET 工具链。
风险判断为可忽略但不为零：C# 与 Java 在这一点上结构相同（都只是把该值存进变量、
容忍缺失），Java 的 10/10 已经证明「多收一帧不会打坏一个本就容忍缺失的客户端」。
要彻底消掉这一格，需要在有 .NET 的机器上重跑 `CsDeep.cs`。

## 环境

两批。broker 配置相同：单机档、集成基线二进制、
`default_invisible_duration: 30s`、`auto_renew_enabled: true`、`txn_check_interval: 5s`。

**第一批（阿里云三临时机，验证中途失联）**

- Broker `172.19.25.180:28081` / admin `:28082`
- Python 客户端 `172.19.25.178`；C# 客户端 `172.19.25.179`（`RocketMQ.Client` 5.2.1 / .NET 8.0.129）

**第二批（联想真机 `100.90.99.61`，4 核 11.7 GB，全程稳定）**

- broker 与客户端同机（`127.0.0.1:28081`）。同机只影响吞吐数字，本轮不测吞吐，
  对特性正确性无影响。
- Java 客户端：`rocketmq-client-java` 5.2.1 / OpenJDK 17.0.19 / Maven 3.8.7
- Python 客户端：同一份 `py_deep.py`，只改 `ENDPOINT` 常量

## 两条 SDK 侧的坑（不是 sq 的问题，但会误导排查）

1. **Python 的 `Message.delivery_timestamp` 单位是秒，不是毫秒**，且 setter/getter
   不对称：写侧 `producer.py:518` 是
   `system_properties.delivery_timestamp.seconds = message.delivery_timestamp`
   直接赋给 protobuf 的 `seconds` 字段；读侧 `fromProtobuf` 却用 `Misc.to_mills`
   返回毫秒。按毫秒传**不会报错**，只是把消息排到 1000 倍远的未来——表现为
   「定时消息永远不到」，极易误判成 broker 的定时投递坏了。我第一轮就是这么栽的。
2. **Python 的 `Producer` 即使只做显式 commit/rollback 也强制要求 `checker` 非空**，
   否则构造时抛 `IllegalArgumentException: Transaction checker should not be null.`
3. **Java 与 C# 的客户端默认走 TLS**，而 sq 监听明文 gRPC，必须
   `enableSsl(false)`（Java）/ `EnableSsl(false)`（C#）。不关掉就死在启动期的
   QueryRoute 上：Java 报 `NotSslRecordException: not an SSL/TLS record`，
   C# 报 `AuthenticationException: Cannot determine the frame size`。
   Java 侧还会把它包装成 `IllegalStateException: Expected the service
   ProducerImpl-0 [FAILED] to be RUNNING`——这层包装完全盖掉了根因，
   必须去 `~/logs/rocketmq/rocketmq-client.log` 看原始异常。
   Python 与 Go 默认明文，不需要这一步。

## 我自己在用例设计上栽的四跤（都已修，值得记）

判据设计原则是「每项必须有反例」，但我仍然踩了四次，形态各不相同：

1. **假阳性**：Recall 用例最初「通过」了，其实是因为坑 1 让延时消息本来就不会到——
   撤不撤回都收不到。**修法**：加对照组，同样延时但不撤回，必须先证明「不撤回时
   消息确实会到」，撤回用例才有判别力。
2. **把无关缺陷记到别的用例头上**：FIFO 因取样不足失败后，我改用 PushConsumer——
   而 PushConsumer 正被 B13.8 打死，于是 FIFO 变成 0/10。**修法**：改回
   SimpleConsumer 并压短 `await_duration`、给足预算。
3. **断言选错**：Recall 断言写成「一条都没收到」，但对照组消息在同一 topic 里，
   而撤回用例换了个新消费组、会从头再读一遍，`count == 0` 永远不成立。
   **修法**：只断言「被撤回的那条消息体不出现」。
4. **跨轮次污染**：topic 会累积上轮消息，把「同组 10 条是否都在一个队列」的
   计数断言打成 30。**修法**：topic 名带运行号，每轮全新。

另外补了一条 FIFO 的**非退化路由**判据：只看「主组 10 条全在队列 0」无法区分
「按组哈希正确」与「哈希坏了、所有消息都进队列 0」，所以再发 4 个 message group
各 2 条，要求每组各自收敛到单一队列、且全体至少用到 2 个不同队列。

## 未完成

- **C++ 深水区**：未开始。08-14 只做过最小往返冒烟。
- **Python 侧的 C# 对偶项**：Python 没验事务孤儿回查（C#/Java 已验），
  C# 没验三值逻辑与 `IS NULL`（Python/Java 已验）。三门加起来覆盖完整，
  单看任一门都有空缺。
- **多消费者再均衡、鉴权组合**：均不在本轮范围。

阿里云那三台临时机在第一批验证途中失联后**再未恢复**（ssh 持续超时）。
第二批改在联想真机上做，Java 与 Python 的完整结果都出自那台。

## 复跑

三个脚本已随本记录入库：

- [`2026-08-15-sdk-deep/JavaDeep.java`](2026-08-15-sdk-deep/JavaDeep.java)
- [`2026-08-15-sdk-deep/py_deep.py`](2026-08-15-sdk-deep/py_deep.py)
- [`2026-08-15-sdk-deep/CsDeep.cs`](2026-08-15-sdk-deep/CsDeep.cs)

broker 地址都写死在文件头部的常量里（`ENDPOINT` / `Endpoint`），换机器只改这一处。
（08-16 同步：`py_deep.py` 的 `ENDPOINT` 默认值改为 `127.0.0.1:28081`——原值指向
已失联的阿里云机；FIFO 取样预算 150s → 400s，把 08-15 重跑时的临时改动落进库里，
此前只有 note 记了结论、脚本没跟上，照库里的脚本复跑会重现那个假红。）
三者都支持只跑单项：传用例名的子串作为第一个参数，例如
`python3 py_deep.py FIFO`、`mvn exec:java -Dexec.mainClass=JavaDeep -Dexec.args=PushConsumer`。

Java 侧需要 `rocketmq-client-java` 5.2.1 + JDK 17；Maven `pom.xml` 见本记录正文的
环境一节（依赖只有这一个 artifact）。
