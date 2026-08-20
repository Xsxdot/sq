# sq

RocketMQ 5.x 协议兼容、单二进制、无 JVM 的轻量消息队列，适合希望少运维组件的中小团队。

## 为什么不是 RocketMQ

sq 用 Go 和 Pebble 提供一个进程、一个数据目录，不需要 JVM 或单独部署 NameServer。它保留 RocketMQ 5.x gRPC 客户端熟悉的消息模型，支持普通、延时、顺序、事务消息以及常用管理面。RocketMQ 的生态、协议覆盖和成熟度更广；如果需要 4.x remoting、PullConsumer 全套 RPC 或更完整的官方运维体系，应直接使用 RocketMQ。

## 快速开始

下载 Release 二进制（Linux amd64 示例）：

```bash
curl -LO https://github.com/Xsxdot/sq/releases/download/v0.1.0/sq_0.1.0_linux_amd64.tar.gz
tar -xzf sq_0.1.0_linux_amd64.tar.gz
./sq                # gRPC :8081，控制台 :8082，数据写 ./data
```

用官方 RocketMQ 5.x SDK 连接 `127.0.0.1:8081` 即可收发；浏览器打开 `http://127.0.0.1:8082/` 查看控制台。运行 `./sq --version` 可查看版本、commit 和构建时间。

从源码构建需要 Go 1.26.1+ **和 Node**——控制台是 `go:embed` 进二进制的，`web/dist` 的构建产物不入库。用 `make build`（含前端）；只用 `go build` 或 `go install` 得到的二进制**控制台是空白的**，其余功能不受影响。详见 [部署说明](docs/deployment.md#从源码构建)。

装完需要自己编辑配置再启动。如果想让脚本把配置也生成好，用同一个包里的 `quickstart.sh`：

```bash
sudo ./quickstart.sh
```

它会生成 `/etc/sq/sq.yaml`（数据目录已指向 `/var/lib/sq`）、自动生成控制台口令、装好 systemd 单元，然后打印启动命令。它同样不会自动启动服务。

三节点集群在三台机器上各跑一次，只差 `--node-id`：

```bash
sudo ./quickstart.sh --cluster --node-id 1 --peers 10.0.0.1,10.0.0.2,10.0.0.3
```

第一台会自动生成控制台口令，另外两台需要用 `--admin-user`/`--admin-password` 带上同一组凭据，脚本结尾会打印可直接复制的命令。装完用 `sq status` 查看集群状态。

不传 `-config` 时使用进程内默认值，不会自动搜索 `/etc/sq/sq.yaml`。需要自定义配置时显式传入：

```bash
./sq -config sq.yaml
```

收到 SIGINT 或 SIGTERM 后，sq 会先结束长流、等待在途 RPC，再关闭存储；最长 30 秒后才强制中断。使用 systemd 或其他编排器时，请给停止操作留出比 30 秒更宽裕的时间。

## 功能一览

| 能力 | 说明 |
|---|---|
| 普通消息 | gRPC + Pebble，消息体上限 4MiB |
| Tag / SQL92 过滤 | 支持 TAG 表达式和 SQL92 子集，未命中消息推进位点 |
| 重试与死信 | 10s 起、指数退避、封顶 5min，默认 16 次后转 DLQ |
| Keys 索引 | 按业务 key 检索消息 |
| retention 与磁盘保护 | 默认保留 3 天；磁盘使用率默认超过 85% 拒写保读 |
| 延时与撤回 | 秒级延时，约 100ms 扫描间隔，到期前可 Recall |
| 顺序消息 | 同 MessageGroup 按队列保持 FIFO，失败会卡队头 |
| 事务消息 | 半消息、提交/回滚、默认 30s 回查和 15 次上限 |
| 自动续租 | 默认开启，单次投递最多续租 10m |
| 鉴权 | gRPC 多组 AK/SK；Admin 用户名密码可独立开启 |
| Admin API 与指标 | topic、消费组、消息、DLQ、诊断、Prometheus |
| Web 控制台 | 12 个页面，静态资源随完整构建的二进制发布 |
| 安装与自检 | `quickstart.sh` 一条命令完成装盘与配置；`sq status` 查看单机/集群状态 |
| 集群模式 | 多 broker 复制、节点状态和恢复路径 |

细节见 [消息语义](docs/messaging.md)、[配置](docs/configuration.md) 和 [Admin API](docs/admin-api.md)。

## 部署

Release 包包含二进制、配置样例、许可证、README 和 systemd 安装件，平台为 Linux amd64、Linux arm64、macOS arm64；不发布 Windows 或 386 包。下载后可用 `SHA256SUMS` 校验完整性。Linux 上可执行 `sudo ./install.sh` 安装到 FHS 路径；安装后把 `data_dir` 设为 `/var/lib/sq`，再执行 `systemctl enable --now sq`。也可以直接运行二进制，默认数据目录是当前工作目录下的 `./data`。

详见 [部署与运维](docs/deployment.md)。

集群部署需要为每个节点准备独立的 Raft 监听地址和成员表，首次启动前应确认磁盘容量。单机数据目录不能通过随意改写 `data_groups` 变成另一种布局，具体迁移和恢复边界见部署文档。

直接运行二进制时，`admin_listen` 默认开启，因此第一次启动即可看到控制台；关闭它只会关闭 Admin HTTP、指标和控制台，不会关闭 gRPC 消息服务。需要远程客户端时，还要把 `advertise_host` 改成客户端可达的地址。

正式安装前建议先核对校验和，再查看 `sq --version`。版本输出包含发布版本、短 commit 和 UTC 构建时间，启动日志也会记录版本与 commit，便于把问题与具体二进制对应起来。

## 文档索引

- [配置](docs/configuration.md)：单机、鉴权、事务和集群 YAML。
- [消息语义](docs/messaging.md)：失败重投、续租、过滤、延时、顺序和事务。
- [Admin API 与 Web 控制台](docs/admin-api.md)：HTTP 路由、指标和页面。
- [部署与运维](docs/deployment.md)：Release、systemd、源码构建、集群和升级。

配置样例 `sq.example.yaml` 随发布包提供；发布包中的 `LICENSE` 是 Apache-2.0。源码构建、发布包内容和平台差异以部署文档为准。

## 现状与限制

0.1.0 是首个公开版本，接口和配置可能继续变化。本文和链接文档只对已经列出的能力作说明，未列出的行为不作承诺。

SDK 验证程度不同：Go 官方 SDK 全量 e2e 为 48 PASS / 0 FAIL / 2 SKIP；Python 深水区实测 12/0；Java 深水区实测 10/0。C# 做过最小往返冒烟，但未针对最近一次协议修复回归；C++ 只做过最小往返冒烟。C# 与 C++ 的未覆盖项不是已知故障，但不应写成已全部验透。

协议边界明确如下：不实现 RocketMQ 4.x remoting 协议，定位是 5.x gRPC；不实现 PullConsumer 的 `PullMessage`、`UpdateOffset`、`GetOffset`、`QueryOffset` 四个 RPC。

消息体上限为 4MiB，默认刷盘档位为 `fsync: sync`。集群模式已经支持多 broker；但 RecallMessage 只在 meta leader 上成立，SDK 按 topic 路由不知道 meta leader，集群撤回可能需要客户端重试，单机不受影响。

Admin 的 `/metrics` 不要求登录，会暴露 topic、消费组和流量计数；gRPC 签名也不提供重放窗口。生产部署应将管理面和 gRPC 端口限制在可信网络内，并按需配置 TLS 或外部网络隔离。

这些限制是当前公开版本的边界，不代表未列出的 RocketMQ 行为都已经兼容。遇到问题时请先提供 `sq --version` 的三行输出、配置中相关字段和启动日志中的版本/commit。
