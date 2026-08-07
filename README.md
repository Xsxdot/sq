# sq — simple queue

RocketMQ 协议兼容、单二进制、无 JVM 的轻量消息队列。面向中小团队：
功能全（普通/延时/顺序/事务消息，逐里程碑交付）、部署轻（一个二进制 + 一个数据目录）。

## 快速开始

```bash
go build -o sq ./cmd/sq
./sq                # gRPC 监听 :8081，数据写 ./data
```

用官方 RocketMQ 5.x SDK（Java/Go/Python/C#/C++）直接连接 `127.0.0.1:8081` 收发。
当前状态：M5b（内嵌 Web 控制台）。里程碑与设计见 docs/superpowers/specs/。

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
- 认证（M5a）：gRPC 静态 AK/SK 签名校验（默认关闭，`access_key`/`secret_key` 成对
  配置启用）；Admin API 独立用户名密码登录（成对配置，均空 = 免登录）
- Admin API（M5a）：topic/消费组管理、消息 Keys 查询与队列浏览、发送测试消息、
  DLQ 重发、延时视图、总览（见「Admin API」）
- Prometheus /metrics（M5a）：topic 写入计数、消费组堆积/inflight、延时深度、
  store 提交耗时直方图（见「Admin API」）
- Web 控制台（M5b）：`go:embed` 进单二进制的静态站，10 个页面覆盖总览/时序/总账、
  topic 与消费组管理、消息查询与发送、死信重发、延时视图（见「Web 控制台」）

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
# —— M5a 认证与管理面 ——
admin_listen: ":8082"          # Admin HTTP 监听地址；"" = 关闭管理面（含 /metrics）
admin_username: ""             # 与 admin_password 成对设置；均空 = 免登录，只填一半启动报错
admin_password: ""
access_key: ""                 # 与 secret_key 成对设置；均空 = 不做 gRPC 鉴权（默认关闭）
secret_key: ""
# —— M5b 时序与 Web 控制台 ——
metrics_retention_hours: 168   # 时序落库保留小时数；0 = 只留内存环的最近 1 小时
```

通过 `-config` 指定配置文件路径，省略则使用以上默认值：

```bash
./sq -config sq.yaml
```

## Admin API

管理面与 `/metrics` 由 `admin_listen` 上的独立 HTTP 服务提供（默认 `:8082`，
`admin_listen: ""` 可整个关闭）。配置了 `admin_username`/`admin_password` 后，
除 `/admin/login` 与 `/metrics` 外的端点都要求 `Authorization: Bearer <token>`。

登录拿 token：

```bash
TOKEN=$(curl -s -X POST localhost:8082/admin/login \
  -d '{"username":"root","password":"pw"}' | jq -r .token)
curl -H "Authorization: Bearer $TOKEN" localhost:8082/admin/topics
```

端点一览：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/admin/login` | 用户名密码登录，返回 token |
| GET | `/admin/overview` | 总览计数（topic/组数、写入/堆积/inflight、延时深度） |
| GET | `/admin/topics` | topic 列表 |
| POST | `/admin/topics` | 建 topic（`{"name","queues","retention_ms"}`） |
| GET | `/admin/topics/{name}` | topic 详情与每队列写入位置 |
| PATCH | `/admin/topics/{name}` | 改 retention |
| DELETE | `/admin/topics/{name}` | 删除 topic（先停流量，见下） |
| GET | `/admin/groups` | 消费组列表 |
| GET | `/admin/groups/{name}` | 组详情：逐 topic 堆积与 inflight |
| POST | `/admin/groups/{name}/reset-cursor` | 位点重置（`{"topic","queue_id","offset"}`） |
| DELETE | `/admin/groups/{name}` | 删除消费组（先停流量，见下） |
| GET | `/admin/messages` | 消息查询：`key` 非空走 Keys 检索，否则按 `topic+queue_id` 顺序浏览 |
| POST | `/admin/messages/send` | 发送测试消息（`{"topic","body","tag","keys"}`） |
| POST | `/admin/dlq/{group}/resend` | 按 `queue_id+offset` 把死信重发回原 topic |
| GET | `/admin/delay` | 延时暂存区视图（按到期时间升序） |
| GET | `/metrics` | Prometheus 指标（免鉴权） |

`/metrics` 暴露的业务指标（另有 `go_*`/`process_*` 标准采集器）：

- `sq_topics`、`sq_groups`、`sq_delay_depth`
- `sq_topic_messages_written_total{topic}`（收发 QPS 由 `rate()` 推导）
- `sq_group_pending_messages{group,topic}`（堆积）
- `sq_group_inflight_messages{group,topic}`
- `sq_store_apply_duration_seconds`（store 批次提交含 fsync 耗时直方图）

安全边界（刻意为之，部署前请知悉）：

- AK/SK 签名**不含重放窗口**：服务端不做 nonce/时间戳校验，截获的签名可重放，
  定位是可信内网，不要把 gRPC 端口对公网开放。
- `/metrics` **不设防**：免鉴权暴露 topic/组名与流量计数，`admin_listen` 同样只应
  绑定内网地址。
- 删除类操作（topic/消费组）是即时物理删除，**先停对应流量再删**。
- Admin token 只存于进程内存，**重启即全部失效**，需重新登录。

## Web 控制台

启动后访问 `http://127.0.0.1:8082/`（端口即 `admin_listen`）。控制台是
`go:embed` 进单二进制的静态站：不需要 Node、不需要单独部署前端，二进制在哪它就在哪。

登录：`admin_username` / `admin_password` 两项都留空时免登录；配置了就要登录，
token 有效期 24 小时、存在浏览器本地（localStorage），过期重新登录即可。

十个页面，每个解决一个具体问题：

| 页面 | 路径 | 解决什么 |
| --- | --- | --- |
| 总览 | `/` | 六项读数 + 1h/24h/7d 趋势图 + 消费关系总账，一眼看出哪个组落在后面 |
| Topic | `/topics` | 建 topic、看队列数与写入位置、删 topic |
| Topic 详情 | `/topics/:name` | 逐队列写入头、改 retention |
| 消费组 | `/groups` | 组列表、删消费组 |
| 组详情 | `/groups/:name` | 逐 topic 堆积与在途、重置位点 |
| 消息查询 | `/messages` | 按 Keys 检索或按队列顺序浏览消息 |
| 死信 | `/dlq` | 按组看死信、单条重发回原 topic |
| 延时 | `/delay` | 延时暂存区视图（按到期时间升序） |
| 发送 | `/send` | 发测试消息（普通/延时） |
| 登录 | `/login` | 用户名密码登录拿 token |

时序数据：内存环保留最近 1 小时（5 秒粒度）；`metrics_retention_hours` 控制
落库保留时长（默认 168 小时 = 7 天，设 0 只保留内存环）。落库是 1 分钟粒度
且**取该分钟的峰值**——分钟平均会把尖峰抹平，而排查时找的就是尖峰。

构建：`make build` 会先 `make web`（需要 Node）再编 Go；只改后端时用
`make build-go` 跳过前端构建。未构建控制台时二进制照样能起，访问 `/` 会
提示先执行 `make web`。

## 限制

- 消息体上限 4MB（`produce.MaxBodySize`）；默认同步刷盘（`fsync: sync`）。
- 未实现：顺序/事务消息、多 broker 集群。
