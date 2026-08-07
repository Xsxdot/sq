# 鉴权与协议面安全收尾设计（B7.1 多组 AK/SK + B3 handle 加签与 Receive 校验 + B6 发送预检）

> M7（B7 epic）第一批。三件事共享同一主题——把协议面上「可信内网里也该有的墙」补齐，
> 并赶在 v1.0 前完成唯一一次破坏性配置变更。
>
> 需求源：backlog B7.1 / B3 / B6；主设计 spec §6（认证默认关闭）、§11（M7）。

## 1. 背景与范围

- **B7.1 多组 AK/SK**：现为单对标量 `access_key`/`secret_key`。改为 list 是破坏性配置
  变更，v1.0 前是最晚窗口（之后改格式就要付兼容成本）。纯增量安全增强，无人阻塞。
- **B3**：receipt handle 是无签名 base64(JSON)，任何能连上 broker 的客户端可自造 handle
  去 ack 别人的 inflight；`ReceiveMessage` 也不校验 topic 存在性与 queueID 边界。
- **B6**：混 topic 批次可在部分落盘后返回不可重试错误，已落盘条目成为幽灵消息。

范围外（YAGNI，见 §9）：凭据权限粒度、重放窗口、控制台连接列表页、旧 handle 兼容。

## 2. 配置：credentials list（破坏性变更）

删除 `access_key` / `secret_key` 两个标量字段，新增：

```yaml
credentials:
  - name: 订单服务        # 可选，仅用于日志追溯
    access_key: AK1
    secret_key: SK1
  - access_key: AK2       # name 可缺省
    secret_key: SK2
```

校验规则（`config.Load`）：

| 规则 | 违反时 |
|---|---|
| 每条 `access_key`、`secret_key` 均非空 | 拒绝启动，报「第 N 条凭据 access_key/secret_key 必须成对非空」——沿用现在「只填一半必是笔误」的原则 |
| `access_key` 全局唯一 | 拒绝启动，报重复的 AK |
| list 为空或缺省 | 鉴权关闭（语义等同现在的「双空」） |
| `name` | 任意字符串，可缺省，不参与校验 |

旧字段出现在配置里时 yaml 解码会静默忽略——为避免用户升级后误以为鉴权还开着，
`Load` 对未知字段**保持现状处理**（不额外做严格模式），但 README/CHANGELOG 必须写明
迁移方法（B7.2 收口文档时统一落）。`sq.example.yaml` 本批同步改。

## 3. auth.go 改造：多凭据校验

拦截器构造时把 credentials 建成 `map[access_key] → {secret, name}`（只读，无锁）。

`verifyAuth` 流程改为：

1. 解析 authorization 头取 (ak, sig)——不变。
2. 查表。**命中与否都走完全相同的后续流程**：未命中时用一个进程启动时生成的随机
   dummy secret 计算 HMAC 并做常数时间比较（结果必然失败）。为什么：现实现刻意
   同时判定 AK 与签名再短路、不泄露「AK 对不对」这一位信息；查表 miss 直接返回
   会把这个性质丢掉——AK 枚举从时序上变得可探测。
3. 命中且签名过 → 放行；其余统一 `Unauthenticated`，错误信息不区分原因（现状）。

日志：失败 Warn 带 `access_key`（现状）＋命中条目时的 `name`；成功路径 Debug 带
`name`（高频，不上 Info）。大小写折叠、SDK 五语言形状兼容等既有行为全部保持，
auth_test.go 的 SDK 形状表不动、只扩多凭据用例。

`NewAuthInterceptors` 签名改为接收 `[]config.Credential`；main 装配处仅在 list 非空时
安装拦截器（对应现在的「accessKey 非空时安装」）。

## 4. receipt handle 加签（B3 主体）

**密钥**：首次启动生成 32 字节随机密钥（crypto/rand），持久化在 `meta/handle_secret`
（沿用 meta 区键式，store/keys.go 加 `HandleSecretKey()`）。启动时读不到就生成并写入，
读到就复用——重启后旧 handle 仍有效，不制造无谓重投。与 AK/SK 配置无关：鉴权关闭时
handle 防伪造同样生效。

**格式**：`base64(JSON payload) + "." + base64(HMAC-SHA256(key, JSON payload))`。

- 选 SHA-256 而非鉴权头的 SHA-1：鉴权头算法是官方 SDK 协议定死的，handle 是纯服务端
  内部格式不受约束，选无争议的。
- `receiptEncode` 增加签名段；`receiptDecode` 先验签（常数时间比较）再解 JSON，
  验签失败与现有「非法 base64/JSON」同路径：调用方统一映射 `INVALID_RECEIPT_HANDLE`，
  Warn 日志只留截断预览（truncateForLog，现状纪律）。
- receipt.go 的编解码需要密钥 → 从纯函数变为携带密钥的小结构（如 `receiptCodec`），
  rpc.Server 持有并在装配时注入；receipt.go 仍不做 I/O，密钥加载在 main/装配层。

**覆盖面**：handle 的全部三个消费入口——`AckMessage`、`ChangeInvisibleDuration`、
`ForwardMessageToDeadLetterQueue`。它们都经 `receiptDecode`，改造点集中在解码函数本身。

**不做旧格式兼容**：升级重启瞬间消费者手头未 ack 的旧 handle 失效 → 按
`INVALID_RECEIPT_HANDLE` 拒绝 → 不可见窗口到期后重投。一次性代价，v1.0 前可接受，
README 注明。

## 5. ReceiveMessage 入口校验（B3 剩余）

handle 加签后，Ack/ChangeInvisible/Forward 拿到的 group/topic/queue/offset/attempt 是
服务端自己签发的可信数据——**Ack 侧不再需要 topic 校验**，「自造 handle ack 别人的
inflight」这条路被签名封死。剩余的洞收窄到 `ReceiveMessage` 入口：

1. **topic 存在性**：只读 `GetTopic` 探测（不走 `EnsureTopic`——消费动作不应创建
   topic），不存在回 `TOPIC_NOT_FOUND`。
2. **queueID 边界**：`queueID >= tc.Queues` 时拒绝，错误码 `BAD_REQUEST`（40000，
   proto 无更细分的 queue 非法码）；跟随现有 receive.go 对客户端输入错误的分类原则，
   Warn 级日志。

两条校验放在现有 TAG 过滤解析之后、`dl.Receive` 之前，错误经 stream.Send 回状态帧
（与现有 `ILLEGAL_FILTER_EXPRESSION` 同形）。

## 6. SendMessage 只读预检（B6）

第一趟校验（现在只做消息级校验、不解析 topic）追加两条**只读**检查，全部通过才进入
第二趟逐条 Append：

1. **批内 topic 一致**：所有 entry 的 topic 必须相同，不同则整批拒
   （官方 Go/Java SDK 客户端侧本就拒绝混 topic 批次，服务端补墙）。
2. **topic 预检**：`ValidateName` 失败 → `ILLEGAL_TOPIC`；`auto_create_topic=false` 时
   `GetTopic` 探测不存在 → `TOPIC_NOT_FOUND`。整批拒绝、零落盘。

`EnsureTopic` 保持在第二趟不动——预检必须无副作用（不能把会创建 topic 的调用提前）。
`auto_create_topic=true` 时跳过存在性探测（第二趟 EnsureTopic 会创建），只做名字校验。

## 7. 错误处理与日志

- 认证失败：统一 `Unauthenticated`＋模糊错误信息（现状不变），Warn 带 method/ak/name。
- handle 验签失败：`INVALID_RECEIPT_HANDLE`，Warn 带截断预览——与非法 base64 同纪律。
- 预检拒绝：Warn 带 topic 与原因；成功路径不加新日志（发送热路径）。
- `meta/handle_secret` 生成/加载：启动时 Info 一条（「已生成」或「已加载」，不打密钥）。
- 全部走 slog logger，禁止 fmt.Printf（instrumenting-code 纪律）。

## 8. 测试策略

单测：

- config：credentials 校验表驱动（缺半、重复 AK、空 list=关闭、name 缺省）。
- auth：多凭据命中第 1/第 N 条、AK 未命中（dummy 路径返回同样的 Unauthenticated）、
  命中但签名错；既有 SDK 形状表全部保留并在多凭据下重跑。
- receipt：签名往返；payload 或签名段篡改一字节即拒；缺签名段（旧格式）即拒；
  密钥持久化——重启（重开 store）后旧 handle 仍验签通过。
- send 预检：混 topic 整批拒且零落盘（扫 store 断言）；autoCreate=false 下不存在
  topic 整批拒；autoCreate=true 下放行。
- receive：不存在 topic 回 TOPIC_NOT_FOUND；queueID 越界拒绝。

e2e（官方 Go SDK）：多凭据配置下用第二条凭据完成收发 ack 全链路；错误凭据被拒
（RPC 报 Unauthenticated）。

## 9. 明确不做

- 凭据级权限粒度（topic/操作 ACL）——超出 v1.0「可信内网」定位。
- 重放窗口 / x-mq-date-time 时效校验——auth.go 头注释里的既有决策，不因多凭据改变。
- 控制台连接列表页、name 穿透进 session——v1 的 name 只到日志层，真需要时是小改动。
- 旧 handle 格式兼容、旧标量配置字段兼容——v1.0 前的破坏窗口就是为此存在的。

## 10. 出口标准

`go test -race ./internal/...` 全绿；e2e 鉴权用例过；`sq.example.yaml` 与 config 校验
一致；README 的鉴权段落改为多凭据写法（完整文档总校留给 B7.2）。
