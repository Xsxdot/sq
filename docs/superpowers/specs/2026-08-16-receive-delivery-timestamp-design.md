# `ReceiveMessageResponse` 信封层 `delivery_timestamp` 设计（B13.8）

> 2026-08-16。修复「sq 从不发 `ReceiveMessageResponse` 的信封层
> `delivery_timestamp` 帧，导致官方 Python SDK 的 PushConsumer 完全不可用」。
> 缺陷取证见 [多语言 SDK 深水区验证](../notes/2026-08-15-multilang-sdk-deep-verification.md)。

## 问题

`proto/apache/rocketmq/v2/service.proto:105`：

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

第三个分支**由 broker 发**。sq 从不发它——`ReceiveMessageResponse_DeliveryTimestamp`
在仓库里只出现于生成的 pb 代码，`internal/rpc/receive.go` 零引用。

（不要与 `SystemProperties.DeliveryTimestamp` 混淆：那是**每条消息**的定时投递
到期时间，sq 填得好好的，两回事。）

后果是链式的，只在 Python 上致命：

1. `consumer.py:225` 因字段缺失不给 `msg.transport_delivery_timestamp` 赋值，保持 `None`
2. `client_metrics.py:151` `latency = current - transport_delivery_timestamp`
   抛 `TypeError: unsupported operand type(s) for -: 'int' and 'NoneType'`
3. 异常被 `push_consumer.py:266` 的 `__handle_received_message` 吞掉
4. 同一 `try` 块里排在其后的 `cache_messages` 与 `execute_consume` 都不执行
   —— **listener 永不触发**，消息卡在 in-flight 并无限重试

Go（`simple_consumer.go:242`）、C#、Java（参考实现 5.2.1）只是把该值存进变量、
容忍缺失，所以这条缺口在 Go e2e 里全绿了很久。

## 参考实现做法（已取原文核对，非推测）

`apache/rocketmq` 的 `ReceiveMessageResponseStreamWriter`：

```java
protected void onComplete() {
    writeResponseWithErrorIgnore(ReceiveMessageResponse.newBuilder()
        .setDeliveryTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
        .build());
    streamObserver.onCompleted();
}
```

三条事实：

1. **帧序**：status 在前 → 各 message → delivery_timestamp **收尾**。
2. **取值**：`System.currentTimeMillis()`，即服务端写这一帧时的当前时间。
3. **无条件发**：`onComplete()` 在 `finally` 里，`MESSAGE_NOT_FOUND`、
   `POLLING_FULL`、异常路径统统会发。

## 设计

### D1：只在「本批确有消息」时发一帧

sq 与参考实现在这一点上**刻意不同**：空长轮询与各错误分支不发。

判据不是省事，是「没有任何已知客户端在空响应下读它」——这是可证伪的：

| SDK | 空响应下是否用 `delivery_timestamp` |
|---|---|
| Python | 否。`consumer.py:225` 的赋值条件是 `len(messages) > 0 and transport_delivery_timestamp`；`client_metrics.receive_after` 首行就是 `if not consumer_group or not messages or len(messages) == 0: return` |
| Go | 否。只作为 `fromProtobuf_MessageView2(message, mq, deliveryTimestamp)` 的入参，无消息即无调用 |
| Java / C# | 否。08-15 深水区实测：两者的 PushConsumer 在大量空轮询下全程正常，本就容忍该帧缺失 |

代价对比：空长轮询是 sq 的常态热路径（每消费者每队列每 20s 一次），无条件发
等于给这条路径**恒定加一帧**，换不到任何客户端行为上的改善。错误分支（非
leader、非法过滤表达式、topic 不存在）更是纯噪声。

**若将来出现一门在空响应下依赖该帧的 SDK，修法是把发送点扩到空/错误分支**
——这条留在这里，是为了让后来者知道当初排除了什么、以及推翻它需要什么证据。

### D2：帧放在**批首**，不放批尾

sq 的帧序本来就与参考实现不同，且这个不同是刻意的、写在 `ReceiveMessage`
的函数注释里：sq 把 status 放**末尾**，作为「本批已全部发完」的天然信号。

在这个前提下，metadata 帧只能放在 status **之前**，否则就自相矛盾——
终止信号后面还挂着数据。放批首同时也是 proto 注释字面语义
（"the timestamp that brokers **start to** deliver"）最直接的读法。

最终帧序：`delivery_timestamp` → message × N → status。

兼容性不受影响，且这一点有实证而非推断：Python（`__handle_receive_message_response`）
与 Go（`simple_consumer.go:236` 起的 `for _, resp := range resps`）都是**把整条
流收完再按 oneof 分类**，帧序对二者完全无关。sq 现有的 status-last 与上游的
status-first 两种顺序都已用真实 SDK 实跑验证过（见 `receive.go` 函数注释）。

### D3：取值 = 开始发送本批的那一刻

`time.Now()`，取在消息发送循环之前。

Python 用它算 `latency = 客户端收到的时刻 - 该值`，即传输延迟。取「开始发」
比参考实现的「发完了」更贴合这个指标的本意，两者在正常批量下相差不足毫秒。

### D4：可观测性

- 现有的 `ReceiveMessage 完成` Debug 日志补 `delivery_ts` 字段，让「客户端说
  算出来的延迟不对」这类问题能在服务端侧直接对账。
- 发帧失败与发消息失败同等对待：Warn + 原样返回 err 结束流。它是批首帧，
  失败意味着流已经断了，后面的消息发了也没有意义；不能静默吞掉——吞掉就会
  变成「客户端什么都没收到，服务端日志一片安静」。

## 影响面

- **修好**：Python SDK 的 PushConsumer。
- **不变**：Python SimpleConsumer（不走 `receive_after`）、Go / Java / C# 全部
  路径（它们本就容忍缺失，多收一帧走的是既有的 oneof 分支）。
- **协议**：只在既有 oneof 里补发一个 broker 侧本就该发的分支，无 proto 改动、
  无新字段、无版本协商。

## 验证

| 层 | 判据 |
|---|---|
| 单测 | 有消息时首帧必须是 `delivery_timestamp`，取值落在 `[发送前, 收完后]` 区间内；空长轮询仍然**只有**一帧 status，不含 `delivery_timestamp` |
| 变异 | 把发送点删掉，上面第一条必须转红（否则断言没有判别力） |
| 真实客户端（承重） | 官方 Python SDK 的 PushConsumer 在真 broker 上收到并触发 listener——这是本条唯一能证明「缺陷真的修好了」的判据，单测只能证明帧发出去了 |
| 回归 | Go e2e 全套（`-tags e2e`）；Java 深水区 10 项；C# 深水区 7 项。三门此前全绿，多一帧后必须仍全绿 |

## 不做

- 不改 proto。
- 不改 `SystemProperties.DeliveryTimestamp` 的任何行为。
- 不改上游 Python SDK（它对可选字段做无保护减法本身也是它的缺陷，但那不归
  sq 修；sq 侧补发该帧后这条路径不再被触发）。
