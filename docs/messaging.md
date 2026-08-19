# 消息语义

本文覆盖投递、过滤、延时、顺序和事务消息的运行语义；不覆盖 YAML 配置全表和 HTTP 管理路由。返回 [README](../README.md) 查看能力概览。

## 消费失败与死信

消息投递后未确认（Receive 超时或 ack 失败）会在不可见窗口到期后重投。非顺序消息的重投退避从 10 秒开始，每次乘 2，封顶 5 分钟。超过消费组 `default_max_attempts`（默认 16）后，消息原子转入 `%DLQ%{group}`，并写入 `sq-origin-topic`、`sq-origin-queue`、`sq-origin-offset` 溯源属性。

顺序消息失败时会卡住队头，直到成功、超过次数进入 DLQ 后推进。顺序锁按队列生效，建议顺序消息使用专用 topic，避免与普通消息混发造成队头阻塞。

## 自动续租

消费者在 `ReceiveMessage` 中声明 `auto_renew` 后，broker 会在 Telemetry 会话存活期间延长消息不可见期。`auto_renew_max_duration`（默认 10 分钟）是单次投递的硬上限；进程断线或超过上限后消息照常重投。顺序消息续租期间会一直持有队列锁，慢 handler 会阻塞同队列后续消息。

## 订阅过滤

未命中或无法判定的消息会跳过并永久推进位点，不进入 inflight；跳过原因通过 `/metrics` 的 `sq_filter_skipped_total{topic,group,reason}` 暴露。

支持：

- TAG：`*`、单 tag、`a || b`。
- SQL92 子集：比较、`AND`/`OR`/`NOT`、括号、`BETWEEN`、`IN`、`IS [NOT] NULL` 以及布尔常量。

不支持 `LIKE`、算术、函数、子查询、字符串大小比较、属性间比较和 `!=`（用 `<>`）。表达式上限为 1024 字节、嵌套 16 层、IN 列表 64 项。`TAGS` 是保留属性名；`k = NULL` 应改为 `k IS NULL`。

## 延时与撤回

延时消息按 `deliveryTimestamp` 调度，扫描间隔约 100ms，重启后不丢。到期前可调用 `RecallMessage` 撤回；消息一旦到期进入投递队列，撤回会失败，不提供补偿成功。

集群模式下，延时暂存区属于元数据组，只有 meta leader 执行调度和撤回。SDK 按 topic 路由通常不知道 meta leader，因此集群撤回可能需要客户端重试；单机模式没有这个限制。

## 事务与普通消息

事务消息先以半消息暂存，生产者通过 `EndTransaction` 提交或回滚后才对消费者可见。未决半消息按 `txn_check_interval`（默认 30 秒）回查，超过 `txn_max_checks`（默认 15 次）仍无决断就丢弃并记日志。事务不能与延时或顺序组合。

普通消息体上限为 4MiB。sq 实现的是 RocketMQ 5.x gRPC 服务面；4.x remoting 协议不在范围内。PullConsumer 族的 `PullMessage`、`UpdateOffset`、`GetOffset`、`QueryOffset` 四个 RPC 不实现，使用已支持的 Receive/Ack 消费路径。
