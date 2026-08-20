# 部署与运维

本文覆盖 Release 安装、systemd、源码构建、集群部署和升级注意；不覆盖每个配置字段及 Admin API 详情。返回 [README](../README.md) 查看产品定位。

## Release 与 systemd

以 Linux amd64 为例，下载并解包 0.1.0 发布包：

```bash
curl -LO https://github.com/Xsxdot/sq/releases/download/v0.1.0/sq_0.1.0_linux_amd64.tar.gz
tar -xzf sq_0.1.0_linux_amd64.tar.gz
sudo ./install.sh
```

`install.sh` 会安装二进制到 `/usr/local/bin/sq`、配置到 `/etc/sq/sq.yaml`、数据目录 `/var/lib/sq`，创建专用系统用户 `sq`，并安装 `sq.service`。已有 `/etc/sq/sq.yaml` 不会覆盖。`install.sh` 安装完成后需要编辑配置，**明确把 `data_dir` 设为 `/var/lib/sq`**，然后启动。（走 `quickstart.sh` 不需要这一步——它生成的配置里已经写好了绝对路径。）

```bash
sudo systemctl enable --now sq
systemctl status sq
sq --version
```

脚本不会自动 enable 或 start 服务。systemd 单元的停止超时为 45 秒，给 sq 自身 30 秒的优雅停止兜底留出余量。

## 一键安装（quickstart.sh）

`install.sh` 只负责装盘，配置要自己写。`quickstart.sh` 在它之上多做三件事：取包、生成配置、收紧配置权限。两者都不会自动启动服务。

单机：

```bash
sudo ./quickstart.sh
```

生成的 `/etc/sq/sq.yaml` 只包含与本机部署形态相关的字段（`data_dir`、`advertise_host`、管理面凭据），其余走进程内默认值；完整字段说明会被铺到 `/etc/sq/sq.example.yaml`。

三节点集群在三台机器上各跑一次，`--peers` 的顺序即 node id，三次执行只有 `--node-id` 不同：

```bash
# 10.0.0.1 上
sudo ./quickstart.sh --cluster --node-id 1 --peers 10.0.0.1,10.0.0.2,10.0.0.3
# 10.0.0.2 上（凭据取第一台生成的值）
sudo ./quickstart.sh --cluster --node-id 2 --peers 10.0.0.1,10.0.0.2,10.0.0.3 \
  --admin-user admin --admin-password '<第一台生成的口令>'
# 10.0.0.3 同上，--node-id 3
```

端口固定为 gRPC 8081、控制台 8082、raft 9081。需要改端口请装完后编辑 `/etc/sq/sq.yaml` 再重启。

脚本不 SSH、不分发，也不做多机编排。三台各跑一次是刻意的形态：SSH 推送要引入免密、跨机提权和半途失败回滚，这些前提失败时的现场比手工装三次更难排查。

### 管理面凭据

控制台口令默认自动生成（24 位），用户名默认 `admin`，两者都写进配置并在脚本结尾打印一次。要关掉鉴权必须显式传 `--no-admin-auth`。

集群的三个节点**必须使用同一组凭据**——`sq status` 在 follower 上会跳到 leader 的管理面取完整视图，凭据不一致会 401 并降级。第一台生成后，脚本结尾会打印带上该口令的完整命令供另两台复制。

配置文件含明文口令，权限为 `0640 root:sq`：sq 进程以 `sq` 用户运行需要读，其他普通用户读不到。

### 重复执行

检测到已有配置或非空数据目录时，脚本报告现状后退出，不做任何改动。确认要覆盖配置加 `--force`，旧配置会被改名为 `sq.yaml.bak.<时间戳>`。

`--force` **不会**删除数据目录。集群的 `data_groups` 首次启动即写盘、此后不可变，数据目录里是盘上事实，要换布局请自己确认后手工清理。

`--force` 且未显式传 `--admin-password` 时，会从旧配置里复用原口令，避免一次无关的重跑（比如只改 `--advertise-host`）静默换掉口令、让这台机器与其余节点失配。

### 离线与网络受限

脚本会按平台自动下载对应的发布包并核对 `SHA256SUMS`。两个逃生口：`--version 0.1.0` 跳过查询最新版本，`--tarball <路径或 URL>` 直接指定包（本地路径和旁路 URL 都跳过校验和）。发布包内自带 `quickstart.sh`，此时它直接用同目录的二进制，不联网。

## 查看运行状态（sq status）

```bash
sq status -config /etc/sq/sq.yaml
```

单机模式打印 topic、消费组、连接数、写入量与磁盘水位。集群模式打印成员表与各组的 leader、term、applied 和待 apply 量。

数据来自管理面 HTTP，因此 `admin_listen` 设为空（关闭管理面）时本命令不可用，会直接报错说明原因。配置里设了 `admin_username` 时命令会自动登录。

在 follower 节点上执行时，命令会跳到元数据组的 leader 再取一次完整视图——各 peer 的复制进度只在 leader 侧维护，不跳转就看不到其余节点的死活。跳不过去时降级为本机视角并明确标注，不会让整条命令失败。

退出码可以直接写进监控脚本：

| 码 | 含义 |
|---|---|
| 0 | 健康：全部组有 leader，全部 peer 活跃 |
| 1 | 够不着本机管理面：服务没起、管理面关闭、凭据错 |
| 2 | 集群降级：有组无 leader，或有 peer 失联 |
| 3 | 判定不完整：跳转 leader 失败，peer 活跃度不可见 |

三节点集群第一台装完启动后没有 leader，`sq status` 会报退出码 2，这是预期行为——raft 按全量成员表引导，凑不齐多数派就没有 leader，进程本身正常运行。

## 从源码构建

源码构建需要 Go 1.26.1+ **和 Node**。控制台通过 `go:embed` 编进二进制，`web/dist` 的构建产物不入库；用下面的目标构建完整程序：

```bash
make build
```

只用 `go build`、`go install` 或 `make build-go` 得到的二进制，后端功能仍可用，但控制台会是空白的，因为没有先生成前端资源。Node 版本按发布流水线使用 22.x。

## 单机与集群

单机不写 `cluster` 段，默认 gRPC 监听 `:8081`、Admin/控制台监听 `:8082`、数据写入 `./data`。远程部署时要设置 `advertise_host`/`advertise_port`，并把数据目录放在稳定磁盘。

集群由 `cluster` 段启用。每个节点需要唯一 `node_id`、独立 `raft_listen` 和包含全体成员的 `peers`；默认 3 个数据组、`quorum-mem` 确认、10000 条日志保留、30 秒截断、4MiB 快照分块和 5 分钟视图 TTL。`data_groups` 首次写盘后不可修改；预分配和数据组增大都会提高磁盘下界，需给 `disk_watermark_percent` 留余量。完整字段见 [配置](configuration.md#集群配置)。

集群模式支持多 broker，但客户端按 topic 路由并不识别元数据组 leader。RecallMessage 属于元数据组，只有 meta leader 可执行；因此集群撤回可能要重试，单机不受此限制。

## 停止与容量

收到 SIGINT/SIGTERM 后，sq 先结束 Telemetry 长流，再等待在途 RPC 并关闭存储；最长 30 秒后强制中断。编排系统的强杀超时应大于 30 秒，systemd 单元已设置 45 秒。

`fsync: sync` 时 Pebble 会合并提交和 fsync；写吞吐主要由客户端批量与并发决定，队列数主要决定消费并行度。默认 16 队列适合多数场景，需要更多消费并行度时再为 topic 显式设置队列数。

## 内部部署历史

以下内容主要服务于已经运行过内部版本的部署，首次公开安装无需迁移。

- receipt handle 现在带 HMAC-SHA256 签名；升级重启时，升级前仍在途的旧 handle 会失效，对应消息在不可见窗口后重投。
- 旧的 `access_key`/`secret_key` 标量写法会被拒绝，迁移为 `credentials` 列表：

  ```yaml
  credentials:
    - name: 订单服务
      access_key: AK1
      secret_key: SK1
  ```

- 从单机扩容到集群前，先确认种子节点日志已压缩到可由快照追齐的状态；新节点需要完整的 FSM 存量数据。
- `message_encoding: binary` 必须先让全集群升级并继续用 `json`，确认全部节点升级后再逐节点改为 `binary`。回退到不认识 binary 的旧版本不受支持；改回 `json` 只会停止新增 binary 数据，不会转换已有数据。
