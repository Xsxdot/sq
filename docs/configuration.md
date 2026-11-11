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
| `fsync` | `sync` | 单机写盘档位：`sync` 每次写入落盘后才应答，掉电不丢已确认消息；`async` 写入后台每 200ms 刷一次盘，掉电最多丢 200ms 内已确认的消息 |
| `message_encoding` | `binary` | `binary` 或 `json` 落盘编码；读方向始终两种都认 |
| `auto_create_topic` | `true` | 未知 topic 是否自动创建 |
| `default_queue_nums` | `16` | 自动创建 topic 的队列数 |
| `default_max_attempts` | `16` | 新消费组的最大投递次数 |
| `default_invisible_duration` | `1m` | 客户端未指定时的不可见时长 |
| `auto_renew_enabled` | `true` | 是否允许消费者自动续租 |
| `auto_renew_max_duration` | `10m` | 单次投递可续租的总时长上限 |
| `consume_checkpoint_interval` | `1s` | 消费进度检查点间隔；`0` = 逐次复制。取舍见下文「消费进度持久化」 |
| `retention_check_interval` | `5m` | retention 清理扫描间隔 |
| `disk_watermark_percent` | `85` | 超过该磁盘使用率时拒写保读；`0` 关闭 |
| `log_level` | `info` | `debug`、`info`、`warn` 或 `error` |
| `admin_listen` | `:8082` | Admin API、指标与控制台监听地址；空值关闭 |
| `admin_username` / `admin_password` | 空 | 两者都为空时 Admin API 免登录 |
| `credentials` | `[]` | gRPC AK/SK 列表；为空时不做鉴权 |
| `metrics_retention_hours` | `168` | 时序落库小时数；`0` 只保留内存环 |
| `txn_check_interval` | `30s` | 半消息回查间隔 |
| `txn_max_checks` | `15` | 单条半消息最大回查次数 |

`credentials` 中每项包含可选的 `name`、成对的 `access_key` 和 `secret_key`。AK 全局唯一；Admin 用户名和密码必须成对设置。`message_encoding` 缺省为 `binary`，与旧版本混跑的边界见 [部署说明](deployment.md#内部部署历史)。

## 消费进度持久化（consume_checkpoint_interval）

消费进度（每个消费组 × topic × 队列的取件位点，与该队列已投未确认的记录）由该队列所属组的 leader 在内存持有，取件、确认、改不可见时间都在内存里完成。它如何落盘由本项决定，是「重复投递」换「吞吐」的取舍：

| 取值 | 行为 | 故障后果 |
|---|---|---|
| `1s`（缺省，>0 即检查点模式） | 取件/确认改完内存即返回；每个间隔把有变化的队列整体落盘一次 | leader 进程崩溃（kill -9、断电）后，约一个间隔内的进度丢失：已确认的消息可能再投一次、已投出的消息可能以首投再投一次。**不丢消息** |
| `0`（逐次复制） | 每次取件/确认都等复制落盘后才返回 | 零重复；每个队列的取件/确认受一次复制往返约束，吞吐低于检查点模式 |

两种模式下，进程正常停机（SIGTERM）与 leader 摊布的计划内转移之前都会先做一次全量检查点，计划内切换不产生重复。负值或非法时长启动即拒。指标与控制台读的是落盘值，检查点模式下最多滞后一个间隔。

## 存储引擎（storage）

`storage` 段调 Pebble 的内存与压实参数。整段可省略，省略或留空的键使用下表缺省值。

```yaml
storage:
  cache_size: 256MiB        # block cache
  memtable_size: 32MiB      # 单个 memtable
  compaction_concurrency: 0 # 压实并发上限，0 = min(4, CPU 核数)
```

| 字段 | 默认值 | 范围 | 作用 |
|---|---:|---|---|
| `cache_size` | `256MiB` | `8MiB`–`64GiB` | 块缓存容量；memtable 的内存从这里预留 |
| `memtable_size` | `32MiB` | `1MiB`–`1GiB` | 单个 memtable 的稳态大小 |
| `compaction_concurrency` | `0` | `0`–`64` | 压实并发上限；常态 1 路，L0 积压时才升到上限。`0` = min(4, CPU 核数) |

大小写成「非负整数 + 可选单位」，单位只认 `B`、`KiB`、`MiB`、`GiB`（区分大小写），不带单位按字节。`KB`/`MB`/`GB` 有 1000/1024 歧义，会在启动时报错并提示改写；小数、负数、越界值同样启动即拒。生效值记录在启动日志 `store 已打开` 的 `cache_size`、`memtable_size`、`compaction_concurrency` 字段。

布隆过滤器（每键 10 位）始终开启，不可配置。它让点查跳过不含目标键的 L0–L5 文件；最底层（L6）的点查不走过滤器，靠块缓存容量。

### 内存占用估算

- **Pebble 常驻上界** ≈ `max(cache_size, 3 × memtable_size)` + 文件句柄缓存等小项。memtable 的内存从块缓存里预留，最多约 3 个（活动 + 待刷 + 1 个回收复用），不是在缓存之外另加。memtable 从 256KiB 起倍增到设定值，空载实例占用远小于上界。
- **Go 堆放大**：发布构建（`CGO_ENABLED=0`）下块缓存与 memtable 都分配在 Go 堆上。默认 `GOGC=100` 下 GC 回收前的垃圾也计入占用，缓存灌满时进程峰值明显高于 `cache_size`：只在 128MiB 与 256MiB 两点实测过，分别约为 2.6 倍与 2.5 倍（见下方实测原始数）。更大的缓存未实测，不要按这个倍数外推。
- **实测**（2026-11-11，Ryzen 9 7950X / Linux，发布构建 `CGO_ENABLED=0`，进程 `VmHWM`）：

  | 负载 | `cache_size: 256MiB`（缺省） | `cache_size: 128MiB` |
  |---|---:|---:|
  | broker 单机 `fsync: sync`，5 分钟 SDK 持续写入（同机另跑边写边消费负载） | 198MiB | 199MiB |
  | 块缓存灌满：约 700MiB 数据上 16 路随机点查 90 秒（只含存储引擎，不含 broker 其余部分） | 646MiB（661176kB） | 332MiB（339720kB） |
  | 同上，加 `GOMEMLIMIT=400MiB` | 375MiB（384288kB，点查吞吐降约 8%） | — |
  | 同上，加 `GOMEMLIMIT=768MiB` | 641MiB（656252kB，与不加无差别） | — |

  写入为主、消费跟得上的负载读的是刚写入的数据，缓存远未填满，峰值与 `cache_size` 无关；读冷数据（消费积压、按 key 查历史消息）才会把缓存灌满。

**2GB 内存机器**：缺省值可以运行：缓存灌满时存储引擎实测 646MiB，加上 broker 其余部分估计在 0.7–0.8GiB。机器上还有其他服务、需要收紧时：

- 首选把 `cache_size` 降到 `128MiB`：缓存灌满时实测峰值约 332MiB，代价是冷读命中率下降。
- 也可以设环境变量 `GOMEMLIMIT`，但它只是 Go 运行时的软上限，不是硬限制：设得过低时 GC 会频繁运行、吞吐下降。实测有效的值只有 `400MiB`（缺省缓存下峰值 375MiB，点查吞吐降约 8%）；`768MiB` 实测高于自然峰值，没有效果。

**回滚注意**：配置了 `storage` 段之后，回滚到不认识该段的旧版本之前，必须先从配置里删掉整个 `storage` 段——旧版本严格解析配置，遇到未知键会拒绝启动。数据目录本身新旧版本互通，无需处理。

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
| `log_retain_entries` | `10000` | 日志保留的按条数下界；同时是每组内存热区的条数（热区另有 64MiB 内部上限） |
| `log_retain_bytes` | `1GiB` | 日志保留的按字节下界，**按节点总量**计（下限 64MiB，写法同 storage 段字节键） |
| `log_peer_grace` | `10m` | 死 peer 宽限期：探活失败的节点在此期间仍作截断下界（须 > 0） |
| `truncate_interval` | `30s` | 日志截断评估间隔 |
| `snapshot_chunk_bytes` | `4MiB` | 快照单块大小，必须小于 16MiB |
| `snapshot_view_ttl` | `5m` | 快照视图闲置回收时间 |
| `read_barrier` | `false` | 是否为消费读路径启用线性一致读屏障 |
| `read_barrier_timeout` | `3s` | 单轮 read-index 时间预算 |

### 日志保留与落后节点追平

每轮截断（`truncate_interval`）在每个组上取以下下界中**最小**的那个作截断点，即保留量取最大：

- **按条数**：`min(本节点 applied, peer 下界) − log_retain_entries`。
- **按字节**：截断后剩余的日志段总字节 ≥ `log_retain_bytes / (data_groups + 1)`（`+1` 是 meta 组）。按段（64MiB）回收，只会多留，不会少留。默认 1GiB、3 个数据组时，每组保留 256MiB。
- **peer 下界**（只在 leader 上生效）：learner 不参与。活着的 peer 取它的复制位点。探活失败的 peer 从首次判死起 `log_peer_grace` 内仍取它的位点，超期后放弃。

磁盘用量达到 `disk_watermark_percent` 时，截断会放弃全部 peer 下界（活着的 peer 也一样），只保留按条数和按字节两条下界。这里用的阈值和判据与拒写相同；`disk_watermark_percent: 0` 时不会判定为磁盘告急。

一个节点停机后在宽限期内回来，leader 会从磁盘日志读出它缺的条目，经日志追平，不触发全量快照。只有超过宽限期、磁盘告急，或者它落后于截断点时，才会改走快照。

截断点之前的日志只留在磁盘上，内存里每组只保留最近 `min(log_retain_entries 条, 64MiB)` 的热区，所以内存占用不随保留量增长。

**磁盘占用估算**：每组约为「按字节下界 + 宽限期内的写入量」。宽限期内，停机节点会钉住它所在各组的截断点。新当选的 leader 对停机节点的复制位点从 0 记起，这时该组在宽限期内完全不截断。估算时请按「宽限期 × 峰值写入速率」预留磁盘，也可以靠磁盘水位兜底。

`peers` 的 `id`、`raft_addr`、`advertise_host`、`advertise_port` 必须为同一成员的对应信息。

- `admin_port`：该节点的管理面端口，可选。留空时 `sq status` 回落取本机 `admin_listen` 的端口（隐含各节点端口一致的假设）。集群运行时不读这个字段，它只服务于 `sq status` 的跨节点查询。`quickstart.sh` 生成的配置会显式写上。

`data_groups` 会在首启写入磁盘，不能靠修改配置改变既有数据布局。集群的磁盘预分配和异常恢复注意事项见 [部署说明](deployment.md#集群部署)。
