# sq — simple queue

RocketMQ 协议兼容、单二进制、无 JVM 的轻量消息队列。面向中小团队：
功能全（普通/延时/顺序/事务消息，逐里程碑交付）、部署轻（一个二进制 + 一个数据目录）。

## 快速开始

```bash
go build -o sq ./cmd/sq
./sq                # gRPC 监听 :8081，数据写 ./data
```

用官方 RocketMQ 5.x SDK（Java/Go/Python/C#/C++）直接连接 `127.0.0.1:8081` 收发。
当前状态：M1（普通消息）。里程碑与设计见 docs/superpowers/specs/。

停机用 `SIGINT`/`SIGTERM` 即可：收到信号后先等在途 RPC 处理完（gRPC
`GracefulStop`），再关闭底层存储，干净退出（退出码 0）；进程启动失败时退出码
非 0，日志会带上失败原因。

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
log_level: info
```

通过 `-config` 指定配置文件路径，省略则使用以上默认值：

```bash
./sq -config sq.yaml
```

## 限制（M1）

- 消息体上限 4MB（`produce.MaxBodySize`）；默认同步刷盘（`fsync: sync`）。
- 未实现：延时/顺序/事务消息、Tag 过滤、Keys 索引、retention、DLQ、控制台。
