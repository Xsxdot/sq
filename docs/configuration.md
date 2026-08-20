# 配置

本文覆盖 sq 的运行配置、默认值与单机/集群模式；不覆盖 Admin API 和消息语义。返回 [README](../README.md) 查看发布包与快速开始。

## 配置文件

用 `-config` 指定 YAML 文件：

```bash
./sq -config sq.yaml
```

不传 `-config` 时使用进程内默认值。sq 不会自动探测 `/etc/sq/sq.yaml` 或其他路径；systemd 安装单元会显式传入该路径。配置采用严格字段解析，拼写未知字段会在启动时失败。

## 单机配置

下表是根级字段和默认值。持续时间使用 Go 格式，例如 `30s`、`5m`。

| 字段 | 默认值 | 作用 |
|---|---:|---|
| `grpc_listen` | `:8081` | RocketMQ 5.x gRPC 监听地址 |
| `advertise_host` / `advertise_port` | `127.0.0.1` / `8081` | 路由响应中的对外地址 |
| `data_dir` | `./data` | Pebble 数据目录 |
| `fsync` | `sync` | `sync` 或 `async` 写盘档位 |
| `message_encoding` | `json` | `json` 或 `binary` 落盘编码 |
| `auto_create_topic` | `true` | 未知 topic 是否自动创建 |
| `default_queue_nums` | `16` | 自动创建 topic 的队列数 |
| `default_max_attempts` | `16` | 新消费组的最大投递次数 |
| `default_invisible_duration` | `1m` | 客户端未指定时的不可见时长 |
| `auto_renew_enabled` | `true` | 是否允许消费者自动续租 |
| `auto_renew_max_duration` | `10m` | 单次投递可续租的总时长上限 |
| `retention_check_interval` | `5m` | retention 清理扫描间隔 |
| `disk_watermark_percent` | `85` | 超过该磁盘使用率时拒写保读；`0` 关闭 |
| `log_level` | `info` | `debug`、`info`、`warn` 或 `error` |
| `admin_listen` | `:8082` | Admin API、指标与控制台监听地址；空值关闭 |
| `admin_username` / `admin_password` | 空 | 两者都为空时 Admin API 免登录 |
| `credentials` | `[]` | gRPC AK/SK 列表；为空时不做鉴权 |
| `metrics_retention_hours` | `168` | 时序落库小时数；`0` 只保留内存环 |
| `txn_check_interval` | `30s` | 半消息回查间隔 |
| `txn_max_checks` | `15` | 单条半消息最大回查次数 |

`credentials` 中每项包含可选的 `name`、成对的 `access_key` 和 `secret_key`。AK 全局唯一；Admin 用户名和密码必须成对设置。`message_encoding: binary` 的集群升级顺序见 [部署说明](deployment.md#内部部署历史)。

## 集群配置

不写 `cluster` 段就是单机模式；一旦出现该段就是集群模式。每个节点都需要自己的 `node_id`、`raft_listen` 和成员表；`raft_listen` 必须与 gRPC 端口不同。

```yaml
cluster:
  node_id: 1
  raft_listen: ":9081"
  data_groups: 3
  ack: quorum-mem
  peers:
    - id: 1
      raft_addr: "10.0.0.1:9081"
      advertise_host: "10.0.0.1"
      advertise_port: 8081
      admin_port: 8082
    - id: 2
      raft_addr: "10.0.0.2:9081"
      advertise_host: "10.0.0.2"
      advertise_port: 8081
      admin_port: 8082
    - id: 3
      raft_addr: "10.0.0.3:9081"
      advertise_host: "10.0.0.3"
      advertise_port: 8081
      admin_port: 8082
```

| 字段 | 默认值 | 作用 |
|---|---:|---|
| `node_id` | 无 | 成员表中的唯一节点 ID，必须出现在 `peers` |
| `raft_listen` | 无 | Raft 组间复制监听地址 |
| `data_groups` | `3` | 数据组数量；首次持久化后不可变 |
| `ack` | `quorum-mem` | `quorum-mem` 或 `quorum-fsync` |
| `peers` | 无 | 成员列表，必须包含本节点 |
| `log_retain_entries` | `10000` | 周期截断保留的日志条数 |
| `truncate_interval` | `30s` | 日志截断评估间隔 |
| `snapshot_chunk_bytes` | `4MiB` | 快照单块大小，必须小于 16MiB |
| `snapshot_view_ttl` | `5m` | 快照视图闲置回收时间 |
| `read_barrier` | `false` | 是否为消费读路径启用线性一致读屏障 |
| `read_barrier_timeout` | `3s` | 单轮 read-index 时间预算 |

`peers` 的 `id`、`raft_addr`、`advertise_host`、`advertise_port` 必须为同一成员的对应信息。

- `admin_port`：该节点的管理面端口，可选。留空时 `sq status` 回落取本机 `admin_listen` 的端口（隐含各节点端口一致的假设）。集群运行时不读这个字段，它只服务于 `sq status` 的跨节点查询。`quickstart.sh` 生成的配置会显式写上。

`data_groups` 会在首启写入磁盘，不能靠修改配置改变既有数据布局。集群的磁盘预分配和异常恢复注意事项见 [部署说明](deployment.md#集群部署)。
