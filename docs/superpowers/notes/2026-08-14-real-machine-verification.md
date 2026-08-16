# 2026-08-14 最终真机验证记录

## 结论

六条未合分支合成的集成基线，在 Linux x86_64 真机上**全量单测 18/18 包通过、e2e 48 通过 / 2 跳过 / 0 失败**，
另加一轮跨主机部署冒烟（启动、Admin API、延时调度、优雅停机、重启恢复）全部符合预期。

**这不是「可以合 main」的结论**——合并次序与取舍仍待人工决定。本次只回答一个问题：
这六条分支合到一起，在真实 Linux 机器上是否成立。

> **08-16 补记**：合并次序已定并执行——把这条集成基线的后继
> `verify/final-integration-3` 整棵快进到 main（`0a23542`），而不是逐条重放。
> 理由与核验见 [分支合并收口](2026-08-16-branch-merge-closure.md)。
> 本节上面那句「不是可以合 main 的结论」在写下时是对的：当时验的是
> `verify/final-integration`（`629151c`），其后还叠了 B14/B15 与 B13.8 两轮，
> 各自另有验收。

## 被验对象

集成分支 `verify/final-integration`，tip `629151c`，由 `main`（`bed9b20`）加三次合并构成。

六条分支的实际拓扑是**一条链加一个分叉**，不是六条平行分支：

```
main → sql92 → (b13.2 base) → push-consumer-e2e → (b13.5 base) → auto-renew ─┬→ recall-message
                                                                              └→ retry-policy-per-client
```

因此只需三次合并：`feat/recall-message`（自带整条链）、`feat/retry-policy-per-client`、
`fix/b12-diagnosability-2`。

- 代码零冲突。仅 `docs/superpowers/backlog.md` 冲突两次，取 main 侧（main 的 backlog 已含
  B13.4/B13.6/B12 的 done 行，是较新的一份）。
- 「代码零冲突」本身是有效信号：这六条并行开发的分支没有互相踩。
- `feat/seglog-prealloc-fdatasync` 不在合并列表——它已经在 main 里（`git merge-base --is-ancestor` 确认）。
- 相对 main：62 文件 / +5859 / −312。

## 验证机器

`xsx-Lenovo-XiaoXin-700-15ISK` = `192.168.0.3`（LAN）= `100.90.99.61`（tailscale），root 免密。

- Ubuntu 24.04.4 LTS，kernel 6.17.0-29-generic，x86_64
- 4 核 / 11.7 GB / nvme + ext4，根分区 41G 可用

**该机装的是 go1.22.2，而仓库要求 go 1.26.1**，所以按既定纪律走 macOS 交叉编译后 scp，
远端零工具链安装：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o sq.linux ./cmd/sq
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o <pkg>.test <pkg>          # 18 个包
cd test/e2e && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -tags e2e -c -o e2e.linux.test ./
```

产物 19 个二进制 + broker 共 457MB，tar czf 经 ssh 落到 `/root/sq-verify/`（传输 15s）。
`-race` 需要 CGO，交叉编译拿不到——race 证据仍以 macOS 本地为准，本次 Linux 侧跑非 race 全量。

## 一、全量单测：18/18 包 PASS

逐包串行执行（4 核机上并行会污染时序敏感的集群用例），`-test.timeout=15m`。

| 包 | 结果 | 耗时 |
|---|---|---|
| internal/cluster | PASS | 87s |
| internal/rpc | PASS | 38s |
| internal/core/deliver | PASS | 11s |
| internal/core/produce | PASS | 5s |
| internal/admin | PASS | 3s |
| 其余 13 包 | PASS | ≤2s |

**`internal/config` 首跑 FAIL 是跑批脚本的缺陷，不是代码问题。**
`TestLoadStrictAcceptsExampleConfig` 读 `../../sq.example.yaml`，而测试二进制被放在扁平目录里跑，
相对路径落空。在真机补出 `repo/internal/config/` 目录结构、把 `sq.example.yaml` 放到对应位置后
重跑 `exit=0` / PASS。`logs/SUMMARY.txt` 里那行 FAIL 是首跑的陈旧记录，未回写覆盖——
两份原始日志都在 `/root/sq-verify/logs/`。

## 二、e2e：48 PASS / 2 SKIP / 0 FAIL

`SQ_E2E_BROKER=/root/sq-verify/sq.linux ./e2e.linux.test -test.timeout=45m -test.v`，
每个用例起真实 broker 进程。

2 条 SKIP 是设计如此：`TestClusterWriteThroughput` 与 `TestExternalClusterWriteThroughput`
是吞吐基准，需 `SQ_BENCH=1` 显式开启，不随套件跑。

本轮六条分支的新特性在真机上都有对应用例落地：

| 条目 | 真机用例 |
|---|---|
| B13.1 SQL92 过滤 | SQL92Numeric / SQL92StringIn / SQL92Combined / SQL92MissingProperty |
| B13.2 PushConsumer | PushBasicLoop / PushFIFOOrderLock / PushRetryOwnedByBroker / PushDLQOwnedByBroker / PushLongPollingWakeup / PushRedeliverAfterInvisibleExpiry |
| B13.4 RecallMessage | RecallPreventsDelivery / RecallAfterDeliveryFails |
| B13.5 AutoRenew | PushAutoRenewPreventsRedelivery |
| B13.6 退避策略分岔 | 无专用 e2e；证据是 settings 单测 T1–T7（含配置漂移判别器 T7） |
| B12 可诊断性 | internal/store PASS（含 pebble logger 接线用例） |

耗时最长的五条：ClusterDLQ 161.7s、FIFOOrderedDelivery 122.5s、ThreeNodeClusterE2E 112.9s、
ScenarioRebootedMemNeedsPermit 109.1s、OfficialGoSDKDLQ 103.3s。

## 三、跨主机部署冒烟

单机档 broker 跑在真机（`grpc :28081` / `admin :28082`，`fsync: sync`），
Admin API 从 macOS 经 LAN 调用——真实跨主机网络路径，不是 loopback。

1. **端口占用的失败路径**（意外收获）：首次用 18081/18082 撞上该机已有的 node / python3 服务，
   broker 打出 `ERROR sq 启动失败 err="admin HTTP 监听 :18082: listen tcp :18082: bind: address already in use"`，
   且各组件（txn / delay / retention / 时序采样 / store）按序打出退出日志后才终止——
   正是 B12 想要的那种「失败自带可读线索」。改用 28081/28082 后正常启动。
2. **建 topic / 发消息 / Keys 索引查询**：`POST /admin/topics`（`queues:4`）→
   `POST /admin/messages/send`（`keys:["k-001"]`）→ `GET /admin/messages?key=k-001` 取回原消息。
3. **延时调度**：`delay_ms:20000` 发出后 `/admin/delay` 列出该条、`delay_depth:1`；
   到期后 `delay_depth` 归 0、`total_written` 3→4，延时条目从列表消失。
4. **/metrics 与控制台**：`sq_*` 指标正常导出（`sq_disk_used_percent` 65.2 等）；
   `GET /` 返回 200，控制台静态资源从二进制内嵌服务。
5. **优雅停机**：SIGTERM 后按序打出「协议适配层进入停机 → admin HTTP 停机 → txn/delay/retention
   退出 → store 关闭」，无残留。
6. **重启恢复**：同 data_dir 重启，`meta 加载完成 topics=2`，Keys 索引查询仍取回重启前那条消息——
   真实 nvme/ext4 上的持久化成立。

冒烟用的两个 topic 与消息留在 `/root/sq-verify/deploy/data`，broker 已停、端口已释放，
该机原有的 node/python3 服务未受影响。

## 取证位置

- 真机：`/root/sq-verify/logs/{SUMMARY.txt,e2e.log,*.log}`、`/root/sq-verify/deploy/broker*.log`
- 二进制与 457MB 产物仍留在 `/root/sq-verify/`，需要回收时 `rm -rf /root/sq-verify`

## 未覆盖

- **Linux 侧 race 检测**：交叉编译拿不到 CGO，`-race` 只能在 macOS 本地跑。
- **多语言 SDK（B13.3）**：Python/C#/C++ 冒烟需在机器上装对应工具链，本次未做。
- **吞吐基准**：两条 bench 用例按设计跳过；性能结论仍以三机 bench 的既有记录为准，
  本机（4 核笔记本、且有其他服务在跑）不具备可比性。
- **集群档的跨物理机部署**：e2e 的集群用例都是同机多进程；真正的跨机组网未在本轮验证。
