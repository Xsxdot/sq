# sq 需求总账

上游需求中枢：先沉淀、想透、再领取。状态流转 `💡 idea → 📋 specced → 🔨 doing → ✅ done`，
`🗄️ shelved` 为显式搁置，`📦 epic` 为大需求的伞行（不可直接领取）。

- `💡 idea` 尚未聊透，需要先走 brainstorming 产出 spec 才可领取。
- `📋 specced` 才是可领取池，领取后交棒 writing-plans。

## Backlog

| ID | 标题 | 状态 | 优先级 | Spec | 原型/流程图 | 验收 | 变更痕迹 | 备注 |
|----|------|------|--------|------|------------|------|---------|------|
| B1 | `produce.Append` 全局锁跨越 fsync，pebble group commit 失效 | 🔨 doing | 高 | [spec](specs/2026-08-07-throughput-and-hygiene-design.md) | — | — | — | 源自 M1 最终整支审查。设计 spec §7 指望 group commit 合并 fsync 使同步写可负担，但 `Append` 全程持 `p.mu` 跨越 `store.Apply`，并发 Apply 永不重叠，实际是单线程逐条 fsync。代码已注释说明，做任何吞吐声明前必须先解决。候选方案：改按队列加锁，或在 Apply 前释放锁。需 benchmark，整块时间。08-07 纳入 M7(B7)：性能基线声明（5k msg/s）的前置。领于 08-07，plan: [m7-batch1](plans/2026-08-07-m7-batch1-hardening-throughput-hygiene.md) |
| B2 | `store.NewBatch` 返回裸 `*pebble.Batch`，「唯一写入口」无编译期强制 | 💡 idea | 中 | — | — | — | — | 源自 M1 最终整支审查。当前 6 处写入全部走 `store.Apply`（已核实零处直接 `b.Commit`），但调用方拿到裸 batch 后可绕过。这正是 v2 Raft 拦截点所依赖的不变量，上 Raft 前应包一层 batch 类型使其编译期成立 |
| B3 | receipt handle 加签，并补 `ReceiveMessage`/`AckMessage` 的 topic 校验 | 🔨 doing | 中 | [spec](specs/2026-08-07-auth-and-protocol-hardening-design.md) | — | — | — | 源自 M1 最终整支审查。handle 是无签名 base64(JSON)，任何客户端可自造一个写着别的 group 的 handle 去 ack 别人的 inflight；`ReceiveMessage`/`AckMessage` 也只校验 group、不校验 topic 合法性与存在性，queueID 亦不受 `tc.Queues` 约束。M1 完全无鉴权所以不算回归——两条是同一个洞，应与鉴权里程碑一并关闭。依赖：鉴权里程碑——08-07 纳入 M7(B7)，与 B7.1 多组 AK/SK 同批（spec 定为独立持久化密钥而非 secret 派生——鉴权关闭时防伪造也生效）。领于 08-07，plan: [m7-batch1](plans/2026-08-07-m7-batch1-hardening-throughput-hygiene.md) |
| B4 | `test/e2e` 拆为独立 Go module | 🔨 doing | 中 | [spec](specs/2026-08-07-throughput-and-hygiene-design.md) | — | — | — | 源自 M1 最终整支审查。官方 SDK 是 direct require（计划如此规定），虽仅在 `e2e` build tag 下 import，却把约 20 个间接模块（google.golang.org/api、opencensus/ocagent、zap、validator）拉进模块图——任何人 `go install` sq 都要下载，且都进不了最终二进制。v1.0 前处理。碎片时间。08-07 纳入 M7(B7)。领于 08-07，plan: [m7-batch1](plans/2026-08-07-m7-batch1-hardening-throughput-hygiene.md) |
| B5 | 补两条承重测试：`Scan(limit<=0)` 与并发 `Append` 的 `-race` | 🔨 doing | 中 | [spec](specs/2026-08-07-throughput-and-hygiene-design.md) | — | — | — | 源自 M1 最终整支审查。两条已从「覆盖率缺口」变成承重：`store.Scan` 的 `limit<=0 == 不限量` 语义现在被 `deliver` 阶段 2 的跳过逻辑直接依赖，却无测试锁定；`produce.Producer` 的类型注释声称「并发安全」，但 `-race` 只跑过顺序用例。碎片时间。08-07 纳入 M7(B7)。领于 08-07，plan: [m7-batch1](plans/2026-08-07-m7-batch1-hardening-throughput-hygiene.md) |
| B6 | 混 topic 批次可能在部分落盘后返回不可重试错误 | 🔨 doing | 低 | [spec](specs/2026-08-07-auth-and-protocol-hardening-design.md) | — | — | — | 源自 M1 最终修复波的自查 + 再审查确认。`SendMessage` 第一趟校验不解析 topic，第二趟逐条 `Append`→`EnsureTopic`；`["orders","bad/name"]` 这类批次会在消息 1 已落盘后返回不可重试的 `ILLEGAL_TOPIC`/`TOPIC_NOT_FOUND`，客户端不会重试，消息 1 成为幽灵。官方 Go/Java SDK 客户端侧即拒绝混 topic 批次（`producer.go:276`），仅手写 gRPC 客户端可触发。正解是第一趟加**只读**预检（`ValidateName` + `autoCreate=false` 时 `GetTopic` 探测）；不可把会创建 topic 的 `EnsureTopic` 提上去，那会破坏第一趟无副作用的保证。碎片时间。08-07 纳入 M7(B7)，与 B3 同批（协议面收尾）。领于 08-07，plan: [m7-batch1](plans/2026-08-07-m7-batch1-hardening-throughput-hygiene.md) |
| B7 | M7：v1.0 发布打磨 | 📦 epic | 高 | — | — | — | — | 设计 spec §11 原定内容：文档、docker 镜像、systemd 单元、快速开始。08-07 立项定范围：B7.1 多组 AK/SK + B7.2 发布打磨 + 纳入 B1/B3/B4/B5/B6（历史 ID 保留不改号）。场景测试计划（plans/2026-08-07-scenario-test.md）独立于 M7 另行执行 |
| B7.2 | 发布打磨：docker 镜像、systemd 单元、快速开始、文档总校 | 💡 idea | 高 | — | — | — | — | 属 B7。spec §11 M7 原定内容。放最后做：配置格式（B7.1 改 list）与全部能力定稿后只付一次文档成本 |
| B7.1 | 多组 AK/SK | 🔨 doing | 高 | [spec](specs/2026-08-07-auth-and-protocol-hardening-design.md) | — | — | — | 属 B7。把 `access_key`/`secret_key` 两个标量改为 list，**破坏性配置变更**——v1.0 前改配置格式不算破坏兼容，v1.0 后就贵了，M7 是最晚窗口。与 docker 镜像/systemd/快速开始文档同批改，只付一次文档成本。纯增量安全增强，单账号在「可信内网」定位下够用，无人阻塞。领于 08-07，plan: [m7-batch1](plans/2026-08-07-m7-batch1-hardening-throughput-hygiene.md) |
