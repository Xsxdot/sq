# sq — simple queue

RocketMQ 协议兼容、单二进制、无 JVM 的轻量消息队列。面向中小团队：
功能全（普通/延时/顺序/事务消息，逐里程碑交付）、部署轻（一个二进制 + 一个数据目录）。

## 快速开始

```bash
go build -o sq ./cmd/sq
./sq                # gRPC 监听 :8081，数据写 ./data
```

用官方 RocketMQ 5.x SDK（Java/Go/Python/C#/C++）直接连接 `127.0.0.1:8081` 收发。
当前状态：M3（延时消息，任意秒级延时、重启不丢）。里程碑与设计见 docs/superpowers/specs/。

停机用 `SIGINT`/`SIGTERM` 即可：收到信号后先让协议层结束没有自然终点的长流
（`Telemetry`），再等在途 RPC 处理完（gRPC `GracefulStop`），最后关闭底层存储，
干净退出（退出码 0）；进程启动失败时退出码非 0，日志会带上失败原因。

**关于停机耗时**：`Telemetry` 是一条双向长流，官方 SDK 停机时并不会主动关闭它，
因此服务端必须自己收尾——否则 `GracefulStop` 会一直等下去（实测：接一个 producer
时停机约 9.5 秒，再接一个 SimpleConsumer 后就再也停不下来）。sq 会在
`GracefulStop` 之前主动结束这些长流，同样场景下停机耗时约 0.04 秒。

即便如此，仍有 **30 秒的强制中断兜底**（`gracefulStopTimeout`）：超时后立即中断
所有在途 RPC。它按在途 RPC 里最长的 `ReceiveMessage` 长轮询（服务端上限 20 秒，
`defaultLongPolling`）加余量取值。编排系统的强杀超时（如 Kubernetes 的
`terminationGracePeriodSeconds`）应设得比 30 秒宽裕，让这层兜底有机会跑完；
被中断的 `ReceiveMessage` 不会丢消息——inflight 记录已落盘，消费者没收到就不会
确认，不可见窗口一过即重投。

## 功能列表

- 普通消息收发：官方 SDK 直连，gRPC + Pebble 存储，消息体上限 4MB
- Tag 过滤：服务端按订阅表达式过滤（`"*"`/单 tag/`a || b`），被过滤消息跳过并
  永久越过位点，不占消费 inflight
- 重试与死信：投递失败按指数退避重投（10s 起、每次 ×2、封顶 5min），超过订阅组
  `default_max_attempts` 后原子转入 `%DLQ%{group}` 死信队列并带 `sq-origin-*`
  溯源属性（见「消费失败链路」）
- Keys 业务索引：发送时可带 keys，按 key 检索消息
- 消息 retention：按 topic 保留时长后台清理（默认 3 天），消息与 key 索引一并删除
- 磁盘水位保护：磁盘使用率超过阈值（默认 85%）时拒写保读，低于阈值自动恢复
- 延时消息：任意秒级延时（deliveryTimestamp），重启不丢，精度 ~100ms 调度间隔

## 消费失败链路

消息投递后若消费者未确认（Receive 超时、ack 失败），会在不可见窗口过后重投；
非顺序消息的两次投递之间按指数退避：10s 起、每次 ×2、封顶 5 分钟。投递次数超过
订阅组的 `default_max_attempts`（默认 16）后，消息被原子转入死信队列
`%DLQ%{group}`（group 为原消费组名）并从原队列移除，不再重投。转入时写入
`sq-origin-topic`/`sq-origin-queue`/`sq-origin-offset` 溯源属性，可用任意
RocketMQ SDK 把 `%DLQ%{group}` 当普通 topic 订阅，做人工处理或重放。

## 配置

```yaml
# sq.yaml（全部可省略）
grpc_listen: ":8081"
advertise_host: "127.0.0.1"   # 路由响应中的对外地址，容器/远程访问时必改
advertise_port: 8081
data_dir: "./data"
fsync: sync                   # sync|async
auto_create_topic: true
default_queue_nums: 4
default_max_attempts: 16       # 新订阅组默认最大投递次数，超过转入 %DLQ%{group}
retention_check_interval: 5m   # 过期清理扫描间隔（Go duration 格式）
disk_watermark_percent: 85     # 磁盘使用率超过即拒写保读；0=关闭
log_level: info
```

通过 `-config` 指定配置文件路径，省略则使用以上默认值：

```bash
./sq -config sq.yaml
```

## 限制

- 消息体上限 4MB（`produce.MaxBodySize`）；默认同步刷盘（`fsync: sync`）。
- 未实现：顺序/事务消息、控制台、多 broker 集群。
