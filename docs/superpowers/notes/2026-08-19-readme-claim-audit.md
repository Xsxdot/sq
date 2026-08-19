# README 能力声明逐条核对（2026-08-19，B7.2 Task 6）

> 目的：让 README 重写建立在核过的事实上，而不是把旧文字重新排版。
> 判据：每条声明必须能指向代码位置或测试；指不出的，要么改写要么删。

| # | 声明原文（截断） | README 位置 | 代码/测试佐证 | 结论 | 处置 |
|---|---|---|---|---|---|
| F0 | 「功能列表」共 16 条 | README.md:35–65 | 实际 `-` 项为 15 条；`grep -c '^- ' README.md` 得 15 | ⚠️ 不精确 | 重写为按能力归类的表，不写未经核实的条数 |
| F1 | 消息体上限 4MB | README.md:37 | `internal/core/produce/produce.go:30-31`：`MaxBodySize = 4 * 1024 * 1024` | ✅ 属实 | 照写 |
| F2 | Tag 过滤支持 `*`、单 tag、`a \|\| b` | README.md:38 | `internal/core/filter` 的 TAG 解析与 `internal/rpc/receive.go` 投递过滤；过滤测试覆盖命中、跳过和位点推进 | ✅ 属实 | 照写，细节移 `docs/messaging.md` |
| F3 | SQL92 属性过滤及示例 | README.md:39–41 | `internal/core/filter` 的比较、逻辑、`BETWEEN`、`IN`、`IS NULL` 解析/求值测试 | ✅ 属实 | 照写支持子集与限制 |
| F4 | 失败重试 10s 起、×2、封顶 5min，转 DLQ | README.md:42–44 | `internal/core/deliver/deliver.go:56-58`；`deliver_test.go:524-530` 断言 10s/20s/40s/5m；DLQ 处理在 `internal/core/deliver` | ✅ 属实 | 照写，细节移 `docs/messaging.md` |
| F5 | Keys 业务索引 | README.md:45 | `internal/core/query/query.go`；`internal/core/produce/produce_test.go:124`；`internal/admin/messages.go` | ✅ 属实 | 照写 |
| F6 | retention 默认 3 天 | README.md:46 | `internal/core/meta/meta.go:38`：`DefaultRetentionMs = 3 * 24 * 60 * 60 * 1000`；meta 默认值测试 | ✅ 属实 | 照写 |
| F7 | 磁盘水位默认 85% | README.md:47 | `internal/config/config.go:181-182`：`DiskWatermarkPercent: 85`；配置校验允许 0–99 | ✅ 属实 | 照写 |
| F8 | 延时消息任意秒级、精度约 100ms | README.md:48 | `internal/core/delay/delay.go:42-44`：`scanInterval = 100*time.Millisecond`；调度测试与重启测试 | ✅ 属实 | 照写“扫描间隔约 100ms”，避免承诺固定投递延迟 |
| F9 | 延时消息可撤回 | README.md:49–52 | `internal/rpc/recall.go`；`internal/core/delay/recall_test.go`；`internal/rpc/recall_test.go` | ✅ 属实 | 照写，并补集群 meta leader 限制 |
| F10 | 顺序消息按 MessageGroup FIFO | README.md:53–54 | `internal/rpc/send.go`、`internal/core/deliver` FIFO 路径及 `internal/rpc/send_test.go` | ✅ 属实 | 照写，补专用 topic/队头阻塞提示 |
| F11 | 事务回查默认 30s、最多 15 次 | README.md:55–57 | `internal/config/config.go:184-186`：`TxnCheckInterval: "30s"`、`TxnMaxChecks: 15`；`internal/rpc/txn.go` 流程与测试 | ✅ 属实 | 照写 |
| F12 | 认证支持 AK/SK 与 Admin 登录 | README.md:58–60 | `internal/config/config.go` 的 `credentials`/Admin 字段；`internal/rpc/auth_test.go`；`internal/admin/server.go` 登录路由 | ✅ 属实 | 去掉内部里程碑编号，照写能力与默认关闭状态 |
| F13 | Admin API 提供管理、查询、重发、延时和事务视图 | README.md:61 | `internal/admin/server.go:92-126` 注册 topics/groups/messages/dlq/delay/transactions/overview/system 等路由 | ✅ 属实 | 照写，完整端点移 `docs/admin-api.md` |
| F14 | Prometheus `/metrics` 指标 | README.md:62 | `internal/admin/server.go:93`；`internal/metrics/collector.go:57-84` | ✅ 属实 | 去掉内部里程碑编号，照写 |
| F15 | Web 控制台有 11 个页面 | README.md:63–65、README.md:307–321 | `web/src/pages/` 非测试 `.tsx` 实际 12 个；`web/src/main.tsx:44-56` 注册 `/cluster`，旧表列 Login 却漏 Cluster | ⚠️ 不精确 | 明确“12 个前端页面（含登录与集群）”，或另列 11 个业务页；旧页面表同步修正 |
| S1 | 「当前状态：M6（事务消息）」 | README.md:17 | 事务实现与测试已存在；内部里程碑编号不属于对外事实 | ❌ 陈旧 | 删除状态行和所有内部里程碑编号 |
| S2 | 只有 Go SDK 有真实 e2e，其余按官方源码对齐 | README.md:13–16 | `docs/superpowers/notes/2026-08-15-multilang-sdk-deep-verification.md`：Go 48 PASS/0 FAIL/2 SKIP，Python 12/0，Java 10/0；C# 未针对 B13.8 回归；C++ 仅最小往返冒烟 | ❌ 陈旧 | 如实写各 SDK 验证程度及 C#/C++ 缺口，不写成全都验透 |
| S3 | 「未实现：多 broker 集群」 | README.md:限制章节（末尾） | `internal/cluster` 实现与集群测试；B8 三节点真机验收记录；Admin `/admin/cluster` 已注册 | ❌ 陈旧 | 删除该句，改写集群已支持的形态与已知限制 |
| C1 | 配置块列出全部配置及默认值 | README.md:122–170 | README 根级列出 22 个键，`internal/config/config.go` 根级有 23 个 yaml 字段；默认值逐项核对：监听 `:8081`/`:8082`、数据 `./data`、`sync`、JSON、自动建 topic、16 队列、16 次、1m、续租 true/10m、retention 5m、85%、info、metrics 168h、事务 30s/15 均一致；README 漏 `cluster` | ⚠️ 不精确 | `docs/configuration.md` 补全 `cluster` 及嵌套字段；README 只保留入口与关键默认值 |
| C2 | `-config` 指定配置，省略使用默认值 | README.md:165–170 | `internal/config/config.go:173` `Load(path)`；空路径返回默认配置 | ✅ 属实 | 照写，并明确不会自动探测 `/etc/sq/sq.yaml` |
| C3 | 升级注意：旧 `access_key`/`secret_key` 与 `message_encoding` 迁移 | README.md:188–228 | `internal/config/config.go` 旧键硬拒绝与编码解析；但 0.1.0 是首个公开版本 | ⚠️ 不精确 | 移到 `docs/deployment.md`“内部部署历史”，不放对外 README |
| A1 | Admin API 端点表完整 | README.md:230–298 | 路由注册还包括 `/admin/timeseries`、`/admin/ledger`、`/admin/cluster` 及 `/debug/pprof/` 五类端点；旧表未列这些 | ⚠️ 不精确 | `docs/admin-api.md` 按 `server.go` 完整列出核心、诊断和集群端点 |
| A2 | Web 控制台页面表完整 | README.md:299–341 | `web/src/main.tsx` 有 Cluster 路由；页面目录共 12 个，旧表将 Login 算入却漏 Cluster | ⚠️ 不精确 | 以路由和页面目录为准重写页面表 |
| D1 | 4.x remoting 协议/协议边界 | README.md:1–4（仅写 5.x 兼容） | `proto/apache/rocketmq/v2/service.proto` 为 5.x gRPC 服务定义；仓库无 4.x remoting 服务实现 | ⚠️ 不精确 | 在 README 现状与限制中明确“不做 4.x remoting” |
| D2 | PullConsumer 能力边界 | README.md:快速开始/功能列表未说明 | `service.proto` 声明 PullMessage、UpdateOffset、GetOffset、QueryOffset；`internal/rpc` 未实现这四个 RPC | ⚠️ 不精确 | 在 README 现状与限制中明确 PullConsumer 四个 RPC 不做 |
| D3 | 集群档 RecallMessage 对所有节点均等可用 | README.md:49–52 未说明 | `internal/core/delay/delay.go:110,156` 仅 meta group leader 调度；`internal/cluster` 的 meta group 为 0；`internal/rpc/routeview.go` 暴露 `MetaIsLeader` | ⚠️ 不精确 | 补充：集群需 meta leader，客户端按 topic 路由不知道它，可能需重试；单机不受影响 |

## 本次核对发现、待另立 backlog 条目

本次没有发现需要在 B7.2 内修复的代码缺陷；以下是已经确认、但应由文档重写处理或另立条目的事实缺口：

1. README 的功能项实际为 15 条而非计划所称的 16 条；重写不再依赖这个数字。
2. README 的配置表漏掉整个 `cluster` 配置段；字段与默认值以 `internal/config/config.go` 和 `sq.example.yaml` 为准。
3. Admin API 旧表漏列 timeseries、ledger、cluster 与 pprof 路由；控制台旧表漏列 Cluster 页面。
4. 多语言 SDK 验证缺口应保留为公开限制：C# 未针对 B13.8 回归，C++ 只做过最小往返冒烟；这不是已知代码故障。
5. 集群 Recall 的 meta leader 路由限制是现有行为约束，不在本计划中修复。

以上发现仅记录，不改变业务代码；Task 7 的每条对外能力描述必须回指本表的结论与处置。
