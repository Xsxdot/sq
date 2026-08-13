# sq — simple queue

RocketMQ 协议兼容、单二进制、无 JVM 的轻量消息队列。面向中小团队：
功能全（普通/延时/顺序/事务消息，逐里程碑交付）、部署轻（一个二进制 + 一个数据目录）。

## 快速开始

```bash
go build -o sq ./cmd/sq
./sq                # gRPC 监听 :8081，数据写 ./data
```

用官方 RocketMQ 5.x SDK（Java/Go/Python/C#/C++）直接连接 `127.0.0.1:8081` 收发。
开启鉴权（配置 `credentials` 列表，见「配置」）后同样兼容这五种 SDK：官方实现之间的签名
头差异（Credential 段带不带 `/{region}/{service}`、签名十六进制大小写）服务端
都已容忍；其中 Go SDK 有真实 e2e 用例覆盖，其余按官方源码的头格式对齐。
当前状态：M6（事务消息）。里程碑与设计见 docs/superpowers/specs/。

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
- SQL92 属性过滤：按消息属性求值订阅表达式（`age > 10`、`region IN ('cn','us')`），
  未命中与无法判定的消息跳过并永久越过位点；支持子集与限制见「订阅过滤」
- 重试与死信：投递失败按指数退避重投（10s 起、每次 ×2、封顶 5min），超过订阅组
  `default_max_attempts` 后原子转入 `%DLQ%{group}` 死信队列并带 `sq-origin-*`
  溯源属性（见「消费失败链路」）
- Keys 业务索引：发送时可带 keys，按 key 检索消息
- 消息 retention：按 topic 保留时长后台清理（默认 3 天），消息与 key 索引一并删除
- 磁盘水位保护：磁盘使用率超过阈值（默认 85%）时拒写保读，低于阈值自动恢复
- 延时消息：任意秒级延时（deliveryTimestamp），重启不丢，精度 ~100ms 调度间隔
- 顺序消息：同 MessageGroup 严格按序（FIFO），失败卡队头重投、超限入 DLQ 后推进；建议顺序消息使用专用 topic（顺序锁按队列生效，与普通消息混发会队头阻塞）
- 事务消息：SendMessage 带 TRANSACTION 类型时先以半消息暂存，由
  `EndTransaction` 提交/回滚后才可见；未决半消息服务端按 `txn_check_interval`
  （默认 30s）定期回查，超 `txn_max_checks`（默认 15）次仍无决断即丢弃并记日志；
  事务不可与延时/顺序组合（协议层显式拒绝），控制台发送页也不提供事务类型
- 认证（M5a）：gRPC 静态 AK/SK 签名校验（默认关闭，配置 `credentials` 列表后启用——
  每个接入方一对、可单独吊销）；Admin API 独立用户名密码登录（成对配置，均空 = 免登录）
- Admin API（M5a）：topic/消费组管理、消息 Keys 查询与队列浏览、发送测试消息、
  DLQ 重发、延时与事务视图、总览（见「Admin API」）
- Prometheus /metrics（M5a）：topic 写入计数、消费组堆积/inflight、延时深度、
  半消息深度与回查/丢弃计数、订阅过滤跳过计数（按原因分桶）、连接数、
  store 提交耗时直方图（见「Admin API」）
- Web 控制台（M5b）：`go:embed` 进单二进制的静态站，11 个页面覆盖总览/时序/总账、
  topic 与消费组管理、消息查询与发送、死信重发、延时与事务视图（见「Web 控制台」）

## 消费失败链路

消息投递后若消费者未确认（Receive 超时、ack 失败），会在不可见窗口过后重投；
非顺序消息的两次投递之间按指数退避：10s 起、每次 ×2、封顶 5 分钟。投递次数超过
订阅组的 `default_max_attempts`（默认 16）后，消息被原子转入死信队列
`%DLQ%{group}`（group 为原消费组名）并从原队列移除，不再重投。转入时写入
`sq-origin-topic`/`sq-origin-queue`/`sq-origin-offset` 溯源属性，可用任意
RocketMQ SDK 把 `%DLQ%{group}` 当普通 topic 订阅，做人工处理或重放。

## 订阅过滤

服务端按订阅表达式过滤，未命中（或无法判定）的消息跳过并永久越过位点：不投递、
不占消费 inflight，换更宽的过滤器也收不回。跳过计数按原因分桶暴露在 `/metrics`
的 `sq_filter_skipped_total{topic,group,reason}`（`tag_miss`/`sql_false`/`sql_unknown`）。

支持两种过滤器：TAG（`"*"`/单 tag/`a || b`）与 SQL92 属性过滤。SQL92 支持以下
语法子集（RocketMQ 文档化子集）：

- 比较：`=`、`<>`、`>`、`>=`、`<`、`<=`，属性与常量比较，如 `age > 10`
- 逻辑组合：`AND` / `OR` / `NOT`，可用括号改变优先级，如 `(a = 1 OR b = 2) AND c = 3`
- `BETWEEN lo AND hi`：`age BETWEEN 18 AND 65`
- `IN (...)`：`region IN ('cn', 'us')`
- `IS [NOT] NULL`：`nickname IS NOT NULL`
- 布尔常量：`TRUE` / `FALSE`

注意与 RocketMQ 对齐的几条约定：

- **`TAGS` 是保留属性名**：映射到消息的 tag（RocketMQ 把 tag 作为可过滤属性暴露）。
  若消息同时带有同名用户属性 `TAGS`，系统映射遮蔽该用户属性——同名用户属性不会生效。
- **`k = NULL` 不被接受**，请用 `k IS NULL`。这是相对 SQL 标准的有意偏离：
  `NULL` 不参与相等比较，`k = NULL` 恒为 UNKNOWN，对过滤毫无意义，构建期直接拒绝
  并给出可照做的替代写法。

不支持（明确拒绝）：`LIKE`、算术运算、函数调用、子查询、字符串大小比较
（`>`/`>=`/`<`/`<=` 只支持数值）、属性间比较（两侧必须是属性与常量）、`!=`
（请用 `<>`）。

表达式上限：长度 1024 字节、嵌套 16 层、IN 列表 64 个元素，超限拒绝。

**排查提示**：若 `sql_unknown` 持续增长而 `sql_false` 恒为 0，通常不是表达式
本身不匹配，而是属性名拼错或属性值的类型与表达式常量对不上（如数字属性写成了
字符串）——`unknown` 桶就是为这类「表达式没写错但数据对不上」的情形准备的。

## 配置

```yaml
# sq.yaml（全部可省略）
grpc_listen: ":8081"
advertise_host: "127.0.0.1"   # 路由响应中的对外地址，容器/远程访问时必改
advertise_port: 8081
data_dir: "./data"
fsync: sync                   # sync|async
message_encoding: json         # json|binary。binary 让 Body 以原始字节落盘，
                               # 省掉 base64 的 +33% 体积与编解码 CPU；
                               # 开启前请先读「升级注意」的两步纪律
auto_create_topic: true
default_queue_nums: 16
default_max_attempts: 16       # 新订阅组默认最大投递次数，超过转入 %DLQ%{group}
default_invisible_duration: 1m # 客户端未指定时的消息不可见时长；push 消费全靠它
retention_check_interval: 5m   # 过期清理扫描间隔（Go duration 格式）
disk_watermark_percent: 85     # 磁盘使用率超过即拒写保读；0=关闭。状态可在控制台
                               # 总览页与 /metrics 的 sq_write_blocked 观察
                               # 集群模式下 raft 段文件预分配占用见 sq.example.yaml
log_level: info
# —— M5a 认证与管理面 ——
admin_listen: ":8082"          # Admin HTTP 监听地址；"" = 关闭管理面（含 /metrics）
admin_username: ""             # 与 admin_password 成对设置；均空 = 免登录，只填一半启动报错
admin_password: ""
credentials: []                # gRPC 静态鉴权凭据列表；空/缺省 = 不做鉴权（默认关闭）。
                               # 每个接入方一对、可单独吊销（从列表移除即失效）：
                               #   - name: 订单服务    # 可选，仅日志追溯用
                               #     access_key: AK1
                               #     secret_key: SK1
# —— M5b 时序与 Web 控制台 ——
metrics_retention_hours: 168   # 时序落库保留小时数；0 = 只留内存环的最近 1 小时
# —— M6 事务消息 ——
txn_check_interval: 30s        # 半消息回查间隔；未决半消息定期回查，等生产者决断
txn_max_checks: 15             # 单条半消息最大回查次数，超限丢弃并记日志
```

通过 `-config` 指定配置文件路径，省略则使用以上默认值：

```bash
./sq -config sq.yaml
```

### 写吞吐与队列数

fsync=sync 时，并发写入由 Pebble commit pipeline 合并 fsync（group commit，
同队列的在途提交也参与合并）：`写吞吐 ≈ fsync速率 × 平均合并深度`，合并深度
约等于在途并发数——**写吞吐由客户端并发和批量发送决定，与队列数基本无关**
（实测 2 vCPU 云主机 64 并发下 1/4/16/64 队列吞吐收敛在同一量级）。

- 队列数决定的是**消费并行度**：一个消费组内每个队列同一时刻只被一个消费者
  持有位点，16 队列即最多 16 路并行消费；FIFO 场景下队列越多，不同
  MessageGroup 之间越少互相排队。默认 `default_queue_nums: 16` 适合大多数
  场景；消费侧吞吐要求高的 topic 建议显式建 topic 并给更多队列（Admin API
  `queues` 参数，上限 1024）。
- 提升写吞吐的手段依次是：批量发送（SDK batch send，整批一次 fsync）、
  提高客户端并发；两者都在摊薄单次 fsync 的成本。
- 参考实测（2 vCPU 云主机、裸 fsync 456 次/s）：64 并发稳态 ~22,000 msg/s
  （10 分钟 soak 无衰减）；批量 32 条/批 ~147,000 msg/s；内存峰值 ~72MB
  （由 Pebble 配置决定，不随负载增长）。

## 升级注意

- **receipt handle 本版本起带签名**：handle 由服务端 HMAC-SHA256 签名（密钥首次启动
  生成并持久化，重启不换钥），伪造 handle 直接拒绝。升级重启后，升级前在途的旧 handle
  （无签名）全部失效，对应消息会按不可见窗口到期重投——这是升级重启这一次的一次性
  影响，此后新领取的 handle 均带签名。
- **配置迁移：`access_key`/`secret_key` 标量改为 `credentials` 列表（破坏性变更）**：
  旧标量写法不再生效：启动时若检测到 `access_key`/`secret_key`，进程会**拒绝启动**
  并提示迁移到 `credentials` 列表——避免升级后鉴权在无人知晓时无声关闭，请迁移为列表写法：

  ```yaml
  # 旧（已失效）
  access_key: AK1
  secret_key: SK1
  # 新
  credentials:
    - name: 订单服务    # 可选，仅日志追溯用
      access_key: AK1
      secret_key: SK1
  ```
- **加入既有集群（单机→多节点扩容）需种子日志已压缩**：新节点的追齐只有走
  快照路径才能带上单机档的 FSM 存量数据；种子日志未压缩（写入量不足约
  2×`log_retain_entries`，默认 10000）时新节点走日志重放，存量数据静默缺失。
  扩容前请先让种子节点写入越过该量，运维指引见 B8.3。
- **开启 `message_encoding: binary` 必须分两步（顺序不可颠倒）**：本版本起
  broker **读**方向同时认识两种消息格式（历史 JSON 与 v1 二进制），**写**方向
  由 `message_encoding` 决定，缺省仍是 `json`。

  1. **先把全集群升到本版本，`message_encoding` 保持缺省 `json`。** 此阶段
     所有节点写的都是历史格式，任何新旧混版组合都安全。
  2. **确认全部节点都已升级后**，再逐节点把 `message_encoding` 改为 `binary`
     并重启。翻档期间部分节点写二进制、部分写 JSON——所有节点都能解双格式，
     安全。

  **为什么顺序不能颠倒**：跨组写入经 `OpForwardAppend` 把编码字节直接转发给
  目标组 leader。混版集群里提前开启 binary，二进制字节会转发给尚未升级的
  旧版 leader，它解不开，写入直接失败。

  **回滚边界**：第 1 步可自由回滚。第 2 步之后盘上已有二进制数据，
  **降级到不认识该格式的旧版本不受支持**；把 `message_encoding` 改回 `json`
  只停止新增二进制数据，不消除存量。

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
| GET | `/admin/overview` | 总览计数（topic/组数、写入/堆积/inflight、延时深度、半消息深度、连接数） |
| GET | `/admin/system` | 运行态读数（磁盘用量与水位、数据目录占用、Go 运行时内存、协程数、运行时长、拒写状态） |
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
| POST | `/admin/messages/send` | 发送测试消息（`{"topic","body","tag","keys","delay_ms","message_group"}`；延时与顺序互斥） |
| POST | `/admin/dlq/{group}/resend` | 按 `queue_id+offset` 把死信重发回原 topic |
| GET | `/admin/delay` | 延时暂存区视图（按到期时间升序） |
| GET | `/admin/transactions` | 待决事务视图（半消息按下次回查时间升序：tx_id/msg_id/topic/next_check_ms/checks/born_ms） |
| GET | `/metrics` | Prometheus 指标（免鉴权） |

`/metrics` 暴露的业务指标（另有 `go_*`/`process_*` 标准采集器）：

- `sq_topics`、`sq_groups`、`sq_delay_depth`
- `sq_topic_messages_written_total{topic}`（收发 QPS 由 `rate()` 推导）
- `sq_group_pending_messages{group,topic}`（堆积）
- `sq_group_inflight_messages{group,topic}`
- `sq_store_apply_duration_seconds`（store 批次提交含 fsync 耗时直方图）
- `sq_disk_used_percent`、`sq_disk_free_bytes`（数据目录所在文件系统，与 `df` 同口径）
- `sq_data_dir_bytes`（数据目录占用，60s TTL 缓存）
- `sq_half_messages`（半消息暂存区待回查条数，gauge）
- `sq_txn_checks_total`（事务回查累计排期次数，含下发失败的轮次，counter）
- `sq_txn_dropped_total`（事务回查超限累计丢弃条数，counter）
- `sq_connections`（已完成 Settings 协商的客户端连接数，gauge；同一连接复用长连接，一条连接可带多条流）
- `sq_write_blocked`（磁盘水位拒写开关，1=拒写保读中）——**这一项适合直接配告警**：
  它为 1 时生产端写入全部失败

内存指标沿用 `go_memstats_*`（Go 采集器自带），不另开一套 `sq_` 前缀的口径。

安全边界（刻意为之，部署前请知悉）：

- AK/SK 签名**不含重放窗口**：服务端不做 nonce/时间戳校验，截获的签名可重放，
  定位是可信内网，不要把 gRPC 端口对公网开放。
- `/metrics` **不设防**：免鉴权暴露 topic/组名与流量计数，`admin_listen` 同样只应
  绑定内网地址。
- 删除类操作（topic/消费组）是即时物理删除，**先停对应流量再删**。
- Admin token 只存于进程内存，**重启即全部失效**，需重新登录。
- gRPC 签名**不绑请求内容**：官方 gRPC 协议本身只对 `x-mq-date-time` 做 HMAC
  （remoting 协议的 ACL 1.0 才对请求字段签名），因此签名可跨请求复用。这是协议
  性质而非本实现的取舍，抗重放只能靠 TLS 与网络隔离。
- 支持**多组 AK/SK，但只认证不授权**：签名通过即全权限，没有 topic/消费组粒度、
  没有 Pub/Sub 区分、没有 IP 白名单（RocketMQ ACL 2.0 有这些）。

## Web 控制台

启动后访问 `http://127.0.0.1:8082/`（端口即 `admin_listen`）。控制台是
`go:embed` 进单二进制的静态站：不需要 Node、不需要单独部署前端，二进制在哪它就在哪。

登录：`admin_username` / `admin_password` 两项都留空时免登录；配置了就要登录，
token 有效期 24 小时、存在浏览器本地（localStorage），过期重新登录即可。

十一个页面，每个解决一个具体问题：

| 页面 | 路径 | 解决什么 |
| --- | --- | --- |
| 总览 | `/` | 七项读数（写入/总落后/在途/延时待投/死信/连接数/TOPIC·消费组，半消息深度并入延时卡） + 1h/24h/7d 趋势图 + 消费关系总账，一眼看出哪个组落在后面 |
| Topic | `/topics` | 建 topic、看队列数与写入位置、删 topic |
| Topic 详情 | `/topics/:name` | 逐队列写入头、改 retention |
| 消费组 | `/groups` | 组列表、删消费组 |
| 组详情 | `/groups/:name` | 逐 topic 堆积与在途、重置位点 |
| 消息查询 | `/messages` | 按 Keys 检索或按队列顺序浏览消息 |
| 死信 | `/dlq` | 按组看死信、单条重发回原 topic |
| 延时 | `/delay` | 延时暂存区视图（按到期时间升序） |
| 事务 | `/transactions` | 待决事务视图：未决半消息的事务ID/消息ID/topic/回查次数/下次回查/暂存时刻，排查「事务卡住 / 回查异常」 |
| 发送 | `/send` | 发测试消息（普通/延时/顺序） |
| 登录 | `/login` | 用户名密码登录拿 token |

总览页第二条读数带给系统运行态：磁盘使用率与水位线、数据目录占用与可用空间、
Go 运行时内存、运行时长与协程数。**磁盘水位触发拒写时，全站每个页面顶部都会
挂一条红色横幅**——生产端「发不进去」的原因不该只存在于服务端日志里。

内存那一格是 **Go 运行时口径**（堆占用 / 向 OS 申请量），不是进程 RSS：Go 归还
内存有延迟，拿 RSS 读容易把正常的高位驻留误判成内存泄漏。Linux 上要看真实 RSS，
用 `/metrics` 的 `process_resident_memory_bytes`。

数据目录占用带 60 秒缓存（它只能靠递归遍历得到，每 5 秒算一遍是把诊断读数变成
I/O 负担），因此这个数最多滞后一分钟。

时序数据：内存环保留最近 1 小时（5 秒粒度）；`metrics_retention_hours` 控制
落库保留时长（默认 168 小时 = 7 天，设 0 只保留内存环）。落库是 1 分钟粒度
且**取该分钟的峰值**——分钟平均会把尖峰抹平，而排查时找的就是尖峰。

构建：`make build` 会先 `make web`（需要 Node）再编 Go；只改后端时用
`make build-go` 跳过前端构建。未构建控制台时二进制照样能起，访问 `/` 会
提示先执行 `make web`。

## 开发与测试

- 单元测试：`make test`（主模块 `go test ./...`，不含 e2e）
- 端到端测试：`test/e2e` 是独立 Go module（自带 go.mod，把官方 SDK 及其约 20 个
  间接依赖隔离出主模块依赖图）；全量跑 `make e2e`，或
  `cd test/e2e && go test -tags e2e -count=1 ./...`

## 限制

- 消息体上限 4MB（`produce.MaxBodySize`）；默认同步刷盘（`fsync: sync`）。
- 未实现：多 broker 集群。
