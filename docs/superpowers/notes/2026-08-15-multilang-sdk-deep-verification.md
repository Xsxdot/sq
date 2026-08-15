# 多语言 SDK 深水区验证（Python / C#）

> 2026-08-15。目标：补 `2026-08-14-four-gaps-verification.md`「仍未覆盖」里的
> 「多语言 SDK 深水区」——08-14 只做了最小往返冒烟，事务、FIFO、push 消费、
> SQL92 过滤都没验。本轮用官方 Python 与 C# SDK 逐特性验证。

## 结论

**挖到一条真缺陷**：sq 不发 `ReceiveMessageResponse` 信封层的 `delivery_timestamp`，
导致官方 **Python SDK 的 PushConsumer 完全不可用**。已记为 [B13.8](../backlog.md)。

其余特性在真实客户端下均成立：

| 特性 | Python | C# | 判据 |
|---|---|---|---|
| SQL92 复合表达式 | ✅ | ✅ | 命中集合精确相等，反例逐条排除 |
| SQL92 三值逻辑 | ✅ | — | 属性缺失时 `NOT (age > 18)` 不误投 |
| SQL92 `IS NULL` | ✅ | — | 精确选出属性缺失那条 |
| SQL92 `BETWEEN` | — | ✅ | 区间 + 等值复合 |
| FIFO 同组顺序 | 路由已证 | ✅ | C#：10 条按发送序到达 |
| 事务 显式提交/回滚 | ✅ | — | committed 可见、rolledback 35s 内不可见 |
| 事务 已裁决不再回查 | ✅ | — | checker 未被触发 |
| **事务 孤儿回查** | — | ✅ | broker 主动回查 2 次，COMMIT/ROLLBACK 裁决分别生效 |
| **PushConsumer + AutoRenew** | ❌ B13.8 | ✅ | C#：处理 45s > 30s 不可见期，零重投 |
| Recall 撤回 | 见下 | 服务端已证 | 撤回后 topic 内只剩对照组消息 |

**AutoRenew（B13.5）由此第一次被非 Go 的真实客户端验证**：此前只有 Go SDK 的 e2e。

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

Go SDK（`simple_consumer.go:257`）与 C# SDK 都只是把该字段存进变量、容忍缺失。
所以 Go e2e 全绿、C# PushConsumer 实测完全正常。**只有 Python 严格依赖它**，
而 Python 此前只做过最小往返冒烟（SimpleConsumer 路径不走 `receive_after`，不受影响）。

这正是单语言验证照不出、必须多语言交叉才能抓到的那类缺口。

### 影响与修法

- **影响**：Python 用户的 push 消费完全不可用；SimpleConsumer 不受影响。
  连带影响 B13.5——AutoRenew 就是为 PushConsumer 设计的。
- **修法预计极小**：在 `receive.go` 投递消息时补发一帧
  `&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_DeliveryTimestamp{...}}`。

## 环境

- Broker：`172.19.25.180:28081`（gRPC）/ `:28082`（admin），单机档，集成基线二进制，
  `default_invisible_duration: 30s`、`auto_renew_enabled: true`、`txn_check_interval: 5s`
- Python 客户端：`172.19.25.178`，`rocketmq-python-client`（venv）
- C# 客户端：`172.19.25.179`，`RocketMQ.Client` 5.2.1 / .NET SDK 8.0.129

## 两条 SDK 侧的坑（不是 sq 的问题，但会误导排查）

1. **Python 的 `Message.delivery_timestamp` 单位是秒，不是毫秒**，且 setter/getter
   不对称：写侧 `producer.py:518` 是
   `system_properties.delivery_timestamp.seconds = message.delivery_timestamp`
   直接赋给 protobuf 的 `seconds` 字段；读侧 `fromProtobuf` 却用 `Misc.to_mills`
   返回毫秒。按毫秒传**不会报错**，只是把消息排到 1000 倍远的未来——表现为
   「定时消息永远不到」，极易误判成 broker 的定时投递坏了。我第一轮就是这么栽的。
2. **Python 的 `Producer` 即使只做显式 commit/rollback 也强制要求 `checker` 非空**，
   否则构造时抛 `IllegalArgumentException: Transaction checker should not be null.`

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

三台临时机在终版脚本跑到一半时**再次全部失联**（ssh 全超时，与 08-14 同样的表现）。
因此以下没有拿到结果：

- **Python 侧修正后的终版**：FIFO 三条判据（同队列 / 非退化路由 / 顺序）、
  定时消息（改用正确的秒单位）、Recall（改用「消息体不出现」断言）。
  前几轮已分别确证过 SQL92 三条与事务两条。
- **C# 侧修正后的终版**：仅 Recall 断言有改动；其 6 项在失联前的完整一轮里已全部
  PASS，Recall 的那次 FAIL 已用服务端取证（broker 日志 `RecallMessage 撤回成功`
  + `/admin/messages` 显示 topic 内只剩对照组消息）证明是我的断言错、撤回本身正确。
- **C++ 深水区**：未开始。
- **Java SDK**：B13.3 范围本就未含。

脚本已随本记录入库，机器重开后可直接重跑：

- [`2026-08-15-sdk-deep/py_deep.py`](2026-08-15-sdk-deep/py_deep.py)
- [`2026-08-15-sdk-deep/CsDeep.cs`](2026-08-15-sdk-deep/CsDeep.cs)

两者都把 broker 地址写死在文件头部的常量里（`ENDPOINT` / `Endpoint`），
换机器只改这一处。
