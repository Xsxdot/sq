# Admin API 与 Web 控制台

本文覆盖 Admin HTTP 路由、Prometheus 指标和控制台页面；不覆盖 gRPC 消息协议。返回 [README](../README.md) 查看如何启动管理面。

## HTTP 管理面

管理面监听 `admin_listen`（默认 `:8082`），设为空即可关闭。配置 `admin_username` 与 `admin_password` 后，除 `/admin/login` 和 `/metrics` 外的路由都需要 `Authorization: Bearer <token>`。

```bash
TOKEN=$(curl -s -X POST localhost:8082/admin/login \
  -d '{"username":"root","password":"pw"}' | jq -r .token)
curl -H "Authorization: Bearer $TOKEN" localhost:8082/admin/topics
```

## 路由

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/admin/login` | 用户名密码登录，返回 token |
| GET | `/admin/overview` | topic、消费组、写入、堆积、inflight、延时、半消息和连接总览 |
| GET | `/admin/system` | 磁盘、数据目录、Go 运行时、协程、运行时长和拒写状态 |
| GET/POST | `/admin/topics` | 列出或创建 topic；创建体为 `name`、`queues`、`retention_ms` |
| GET/PATCH/DELETE | `/admin/topics/{name}` | 查看、修改 retention 或删除 topic |
| GET | `/admin/groups` | 消费组列表 |
| GET | `/admin/groups/{name}` | 查看组详情 |
| POST | `/admin/groups/{name}/reset-cursor` | 重置指定 topic/queue 的消费位点 |
| DELETE | `/admin/groups/{name}` | 删除消费组 |
| GET | `/admin/messages` | 按 key 检索，或按 topic/queue 顺序浏览 |
| POST | `/admin/messages/send` | 发送测试消息，支持普通、延时和顺序字段 |
| POST | `/admin/dlq/{group}/resend` | 按队列和 offset 重发死信 |
| GET | `/admin/delay` | 延时暂存区，按到期时间排序 |
| GET | `/admin/transactions` | 待决事务半消息和回查状态 |
| GET | `/admin/timeseries` | 时序采样数据 |
| GET | `/admin/ledger` | 消费关系总账 |
| GET | `/admin/cluster` | 集群节点与分组状态；单机模式不提供集群信息 |
| GET | `/metrics` | Prometheus 指标，免 Admin 登录 |

删除 topic 或消费组是即时物理删除，操作前先停止对应流量。Admin token 只保存在进程内存，重启后需要重新登录。`/metrics` 不设防，只应绑定在可信内网地址。

## 诊断端点

以下 pprof 路由也挂在 Admin 面，并遵循同一鉴权中间件：

- `/debug/pprof/`：剖面索引；具名剖面如 heap、goroutine、block 和 mutex 由索引访问。
- `/debug/pprof/cmdline`、`/debug/pprof/profile`、`/debug/pprof/symbol`、`/debug/pprof/trace`：命令行、CPU、符号和 trace 诊断。

## Prometheus 指标

除 `go_*` 与 `process_*` 标准采集器外，业务指标包括 `sq_topics`、`sq_groups`、`sq_delay_depth`、`sq_topic_messages_written_total`、`sq_group_pending_messages`、`sq_group_inflight_messages`、`sq_filter_skipped_total`、`sq_store_apply_duration_seconds`、`sq_disk_used_percent`、`sq_disk_free_bytes`、`sq_data_dir_bytes`、`sq_half_messages`、`sq_txn_checks_total`、`sq_txn_dropped_total`、`sq_connections` 和 `sq_write_blocked`。其中 `sq_write_blocked=1` 表示磁盘水位保护正在拒绝写入。

## Web 控制台

访问 `http://127.0.0.1:8082/`。控制台静态资源通过 `go:embed` 编进二进制，页面数量以实际路由为准，共 12 个：

| 页面 | 路径 |
|---|---|
| 总览 | `/` |
| Topic / Topic 详情 | `/topics`、`/topics/:name` |
| 消费组 / 组详情 | `/groups`、`/groups/:name` |
| 消息查询、死信、延时 | `/messages`、`/dlq`、`/delay` |
| 事务、发送、集群 | `/transactions`、`/send`、`/cluster` |
| 登录 | `/login` |

配置 Admin 用户名和密码后，登录页取得的 token 有效期为 24 小时并保存在浏览器本地。构建控制台的 Node 要求和空白控制台陷阱见 [部署说明](deployment.md#从源码构建)。
