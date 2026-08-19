# 部署与运维

本文覆盖 Release 安装、systemd、源码构建、集群部署和升级注意；不覆盖每个配置字段及 Admin API 详情。返回 [README](../README.md) 查看产品定位。

## Release 与 systemd

以 Linux amd64 为例，下载并解包 0.1.0 发布包：

```bash
curl -LO https://github.com/Xsxdot/sq/releases/download/v0.1.0/sq_0.1.0_linux_amd64.tar.gz
tar -xzf sq_0.1.0_linux_amd64.tar.gz
sudo ./install.sh
```

`install.sh` 会安装二进制到 `/usr/local/bin/sq`、配置到 `/etc/sq/sq.yaml`、数据目录 `/var/lib/sq`，创建专用系统用户 `sq`，并安装 `sq.service`。已有 `/etc/sq/sq.yaml` 不会覆盖。安装完成后编辑配置，**明确把 `data_dir` 设为 `/var/lib/sq`**，然后启动：

```bash
sudo systemctl enable --now sq
systemctl status sq
sq --version
```

脚本不会自动 enable 或 start 服务。systemd 单元的停止超时为 45 秒，给 sq 自身 30 秒的优雅停止兜底留出余量。

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
