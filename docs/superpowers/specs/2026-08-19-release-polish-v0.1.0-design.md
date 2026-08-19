# B7.2 发布打磨：v0.1.0 首个公开发布 设计

> 状态：待用户复核
> 日期：2026-08-19
> backlog：B7.2（属 B7 epic，B7 进度将由 6/7 转 7/7）

## 1. 背景与目标

sq 至今没有对外发布过任何版本。仓库虽然公开（`github.com/Xsxdot/sq`），但：

- **没有 LICENSE**——法律上「保留所有权利」，任何人都不能合法使用；
- **没有可下载的二进制**——README 的快速开始要求读者本机装 Go **和** Node；
- **没有版本号**——全仓零 `ldflags`、零 `Version` 变量，`sq --version` 不存在，
  唯一的 git tag 是 `m1`；
- **模块路径指向不存在的仓库**（见 §3.1），`go get` 必然失败；
- **README 有确凿的陈旧记述**——第 17 行「当前状态：M6」、`## 限制` 的
  「未实现：多 broker 集群」，都是「写时是对的，代码走了文字没跟上」。

目标：让一个零上下文的陌生人能在 5 分钟内拿到 sq 并跑起来，且拿到的是
**合法可用、版本可追溯、控制台真的能打开**的二进制。

**不是目标**：把 sq 推广出去。本项只负责「发布通道可用」，不负责宣传。

## 2. 范围

### 做

| # | 交付件 | 性质 |
|---|---|---|
| ① | LICENSE（Apache-2.0） | 新增文件 |
| ② | 模块路径订正 | 机械改动 563 处 |
| ③ | 版本注入 + `sq --version` | 唯一的服务端代码改动 |
| ④ | GitHub Actions 出包 | 新增 CI |
| ⑤ | systemd 单元 + 安装脚本 | 新增部署件 |
| ⑥ | README 重写 + 拆 `docs/` | 文档重构 |
| ⑦ | 能力声明逐条核对 | 工作量最大，价值最高 |

### 不做（含理由，不要再议）

- **Docker 镜像**——用户 08-19 明确砍掉。B7.2 原标题含 docker，本轮范围据此收窄。
- **`windows` / `386` 平台**——sq 是服务端。每多一个平台，那个平台就成了
  「发出去但从没跑过」的承诺。
- **配置文件路径自动发现**（`--config` 为空时找 `/etc/sq/sq.yaml`）——它会把
  「不给配置就用内置默认」这个确定行为，变成「取决于机器上有没有那个文件」，
  悄悄改变所有现存部署。发布打磨不该改运行时语义，属另立条目。
- **把 `web/dist` 提交进仓库**——Release 走 Actions 编。`go install` 拿到的
  二进制控制台空白这件事，在 README 里如实写明，不靠提交构建产物绕过。
- **改 `test/e2e` 的测试内容**——只跟着改模块路径。

## 3. 逐件设计

### 3.1 模块路径订正（范围增项，需用户确认）

**事实**：`go.mod` 声明 `module github.com/xushixin/sq`，而仓库在
`github.com/Xsxdot/sq`。这不是大小写差异，是两个不同的名字。

**后果**：Go 的模块路径**就是**下载地址。`go install github.com/xushixin/sq/cmd/sq@latest`
会去 `github.com/xushixin/sq` 拉取——那不是这个仓库。公开发布后任何人
`go get` 都会失败，而重写后的 README 快速开始正要写这条命令。

**为什么至今无感**：本地开发全靠相对路径与 `replace`，从来没有人从仓库外
`go get` 过。这是典型的「只有公开发布才照得出来」的缺陷。

**改法**：全仓 `github.com/xushixin/sq` → `github.com/Xsxdot/sq`，
共 563 处 `.go` 引用 + `go.mod` + `test/e2e/go.mod`（含它的 `require` 与 `replace`）。
纯机械替换，`go build ./... && go test ./...` 即可验证完整性。

**风险**：低。没有外部导入者（路径从来就是错的，不可能有人导入成功）。

**待用户确认**：仓库归属是否就是 `Xsxdot`？若你打算把仓库转到别的账号或
改名，路径应一次改到位——改两次意味着 0.1.0 和 0.2.0 是两个不同的模块。

### 3.2 LICENSE：Apache-2.0

用户 08-19 选定。根目录放标准全文 `LICENSE`，版权行填写年份与所有者。
选它而非 MIT 的实际理由：带显式专利授权，企业法务最容易过审，且与
RocketMQ 本身同许可证——sq 是协议兼容实现，许可证一致能省掉一类疑问。

**不做**：不逐文件加 license header（收益低、噪声大，Apache-2.0 不要求）。

### 3.3 版本注入

`cmd/sq` 新增三个包级变量，默认值让本地裸 `go build` 也不报错：

```go
var (
    version   = "dev"
    commit    = "none"
    buildDate = "unknown"
)
```

`-ldflags "-X main.version=... -X main.commit=... -X main.buildDate=..."` 注入。

`sq --version` 打印三行后 **退出码 0**。实现放在 `flag.Parse()` 之后、任何
存储/网络装配之前——`--version` 必须在配置错误、端口占用、数据目录不可写的
机器上照样能跑通，那正是用户被要求「报一下你的版本」的场景。

**不碰启动语义**：`recover` 子命令的分支在 `os.Args[1]` 上，早于 `flag.Parse()`，
两者互不影响。

### 3.4 GitHub Actions 出包

`.github/workflows/release.yml`，触发 `push` 且 `tags: v*`。

单 job，步骤：

1. `actions/checkout`（`fetch-depth: 0`，`git describe` 需要历史）
2. `actions/setup-node` —— **钉住主版本**。`web/package.json` 无 `engines` 字段，
   Node 版本此前完全不受约束，CI 是第一次给它定锚。
3. `actions/setup-go` —— 钉 `1.26.1`（`go.mod` 的要求）
4. `cd web && npm ci && npm run build` —— **只编一次**。前端是纯静态资源，
   与 `GOARCH` 无关，三个平台共用同一份 `web/dist`。
5. 按矩阵交叉编译 `linux/amd64`、`linux/arm64`、`darwin/arm64`。
   纯 Go 无 CGO，交叉编译不受限（已实测：B8.2 期间 386/arm 交叉编译通过）。
   版本号从 tag 取（`v0.1.0` → `0.1.0`，去掉 `v` 前缀）。
6. 每平台打 `sq_<version>_<os>_<arch>.tar.gz`，内含：
   `sq`、`sq.example.yaml`、`deploy/sq.service`、`deploy/install.sh`、
   `LICENSE`、`README.md`
7. 生成 `SHA256SUMS`
8. `gh release create` 上传全部产物

**承重约束（写进 workflow 注释，别改回去）**：第 4 步不可省。
`web/dist` 只有 `.gitkeep` 入库，用裸 `go build` 出的二进制**控制台是空的**——
而 README 承诺「单二进制，启动即见一切」。这是 0.1.0 最可能的首个 issue。

**已知代价与调试路径**：本仓库零 CI 基础，这是第一个 workflow。
用户 08-19 在知悉「先本地脚本跑通再固化成 CI」这个更便宜的替代方案后，
仍选择直接上 Actions，因此调试成本要在 workflow 设计里自己消化：

**同时挂 `workflow_dispatch` 触发器**，让构建与打包能手工触发、反复调试，
不必靠推 tag。只有确认产物正确后，才推真 tag 触发完整发布。
`workflow_dispatch` 路径**跳过 `gh release create`**（无 tag 可挂），
只上传 artifact 供下载核对——这样调试轮次不会污染 Release 页。

若 tag 触发后仍失败，退路是删 tag 重推（`git tag -d` + `git push --delete`），
**不要为了绕过失败而手工传产物**——那会让 Release 产物与 workflow 的产出脱钩，
下一次发版就没人知道产物到底是怎么来的。

### 3.5 systemd 单元

`deploy/sq.service` + `deploy/install.sh`。布局按 FHS：

| 用途 | 路径 |
|---|---|
| 二进制 | `/usr/local/bin/sq` |
| 配置 | `/etc/sq/sq.yaml` |
| 数据 | `/var/lib/sq` |
| 运行用户 | 专用系统用户 `sq` |

单元要点：

- `ExecStart=/usr/local/bin/sq --config /etc/sq/sq.yaml` —— 显式指定。
  **刻意不给 sq 加配置发现**（见 §2「不做」）。
- `WorkingDirectory=/var/lib/sq` —— sq 的 `data_dir` 默认是相对路径 `./data`，
  配置里应显式写绝对路径，但工作目录兜住配置漏写的情况。
- `TimeoutStopSec=45` —— **承重取值，不是拍脑袋**。sq 有 30 秒强制中断兜底
  （`gracefulStopTimeout`，按 `ReceiveMessage` 长轮询服务端上限 20 秒加余量取的）。
  systemd 的停机超时必须比它宽裕，否则兜底还没跑完就被 SIGKILL，
  在途 RPC 被硬切。45 = 30 + 15 余量。
- `Restart=on-failure` —— 进程启动失败时 sq 退出码非 0；正常停机退出码 0，
  不触发重启。
- 加固：`NoNewPrivileges=true`、`ProtectSystem=strict`、`ProtectHome=true`、
  `ReadWritePaths=/var/lib/sq`。

`install.sh` 负责：建 `sq` 系统用户 → 建目录并 chown → 拷二进制 →
`sq.example.yaml` 拷成 `/etc/sq/sq.yaml`（**已存在则不覆盖**，升级不能吃掉用户配置）
→ 装单元 → `systemctl daemon-reload`。**不自动 enable/start**——
装完打印下一步命令让用户自己执行，安装脚本擅自起服务是不礼貌的。

### 3.6 README 重写 + 拆 `docs/`

现有 README 352 行，同时是门面、配置手册（45 行 yaml 全表）、
Admin API 参考（70 行）、控制台说明（43 行）。对外发布时这几部分的读者不是同一批人，
**更新节奏也不同**——配置项每加一个改一次，而门面几乎不动。混在一起的结果就是
今天这样：门面部分的「当前状态：M6」在四个里程碑之后还留在第 17 行没人注意到。

**新 `README.md`（目标 ~120 行）**：

1. 一句话定位
2. 为什么不是 RocketMQ——无 JVM、单二进制、一个数据目录
3. 快速开始——**下载 Release 二进制**，不再是 `go build`
4. 功能一览（压成表，每行一句）
5. 部署指路（→ `docs/deployment.md`）
6. 文档索引
7. 0.1.0 现状与限制

**拆出四份**：

| 文件 | 收纳 |
|---|---|
| `docs/configuration.md` | 配置全表 |
| `docs/admin-api.md` | Admin API 端点 + Web 控制台 |
| `docs/messaging.md` | 消费失败链路 / 自动续租 / 订阅过滤 / 顺序 / 事务 的行为细则 |
| `docs/deployment.md` | systemd / 集群 / 升级注意 / 写吞吐与队列数 |

**版本叙事统一**：里程碑编号（M1–M7、v1.0、v1.1）对外一律去掉——那是内部排期语言，
对读者没有意义。`## 升级注意` 现有内容全部在讲跨版本迁移
（`access_key` 标量改 `credentials` 列表、`message_encoding` 两步开启等），
而 **0.1.0 是首个公开版本，没有「上一版」的用户要迁移**。这些内容移入
`docs/deployment.md` 的「内部部署历史」小节保留（对已有内部部署仍有效），
不出现在对外的 README。

**0.1.0 意味着什么**（README 里明写）：接口可能变；已实测的能力见文档，
未列出的不作承诺。

### 3.7 能力声明逐条核对（工作量最大）

**这是 B7.2 唯一真正值钱的部分。** 现有 README 已确认错了三处，而三处都是
「写时是对的，后来代码走了、文字没跟上」。**重写如果只是重新排版，等于把剩下
那些还没被发现的同类错误原样搬进新文档，还盖了一个「刚校对过」的章。**

核对范围与判据——每条声明必须能指向代码位置或测试佐证：

| 对象 | 数量 | 核对到什么 |
|---|---|---|
| 功能列表 | 16 条 | 对应实现文件；带数字的（4MB 上限、3 天 retention、85% 水位、10s 起退避封顶 5min、30s 回查、15 次上限）逐个回到常量定义 |
| 配置全表 | ~45 行 yaml | 对 `internal/config/config.go` 的字段与默认值 |
| Admin API | 端点清单 | 对路由注册处 |
| 控制台 | 「11 个页面」 | 对 `web/src/pages/` 实际页面数 |

**已发现一处待判别的漂移**（08-19 自审时实测）：`web/src/pages/` 下非测试页面
实为 **12 个**（Cluster / Delay / Dlq / GroupDetail / Groups / Login / Messages /
Overview / Send / TopicDetail / Topics / Transactions），README 写 11。
差额很可能是 `Login` 不算「控制台页面」——**但这是推测，核对时必须确认，
不许照推测写数字**。它恰好是本节要抓的那类漂移的活样本：一个当时正确、
后来多了一个页面就不再正确的具体数字。

**已知必改三处**：

1. `## 限制` 的「未实现：多 broker 集群」——B8 早已交付，三机真机验过
2. 第 17 行「当前状态：M6（事务消息）」——已到 v1.1 SDK 兼容收口
3. 第 13–16 行 SDK 验证程度——现在 Python/Java/C#/C++ 都真跑过（B13.3），
   Python 深水区 12/0、Java 10/0。**但必须如实写出缺口**：C# 未针对 B13.8 回归、
   C++ 只做过最小往返冒烟（见 B17）。不能因为「多语言都验过了」就写成全都验透了。

**核对中发现的新缺陷不在本项修**——记进 backlog 另立条目，README 按代码现状如实写。
本项是校对，不是修 bug。

## 4. 验收判据

推 `v0.1.0` tag 后：

1. Release 页出现 3 个 `tar.gz` + `SHA256SUMS`
2. 下载 `linux/amd64` 那个到联想真机（100.90.99.61），校验 sha256 一致
3. 跑 `install.sh` → `systemctl start sq` 起得来，`systemctl status` 为 active
4. `sq --version` 打出 `0.1.0` 与真实 commit
5. **浏览器打开 `:8082` 控制台页面能渲染**——这是「前端真编进二进制」的唯一硬证据，
   不能用「文件大小看起来对」代替
6. `systemctl stop sq` 在 `TimeoutStopSec` 内干净退出，journal 里无 SIGKILL 记录
7. 全新目录 `git clone git@github.com:Xsxdot/sq.git && go build ./...` 通过
   （证明模块路径订正完整）
8. `go test ./...` 18 包全绿、`cd test/e2e && go test -tags e2e ./...` 与
   08-16 基线一致（证明路径订正没碰坏别的）
9. README 每条能力声明有对应代码位置或测试佐证

## 5. 待复核假设

| # | 假设 | 若不成立 |
|---|---|---|
| A1 | 仓库归属就是 `Xsxdot`，不打算改名或转账号 | 模块路径要改两次，0.1.0 与 0.2.0 会是两个不同的模块 |
| A2 | Apache-2.0 的版权所有者写用户本人 | 若归属公司，版权行要改 |
| A3 | GitHub Actions 的 `GITHUB_TOKEN` 默认权限足以建 Release | 需在仓库设置里放开 workflow 写权限 |
| A4 | 联想真机（100.90.99.61）仍可达，可用于验收 | 换一台 linux/amd64 机器；验收判据 2–6 不可省 |
| A5 | 0.1.0 只发布，不承诺兼容性 | 若要承诺，`## 限制` 的措辞要更保守 |

## 6. 风险

- **Actions 首次配置的调试循环慢**（只能推 tag 触发）。缓解：先用
  `workflow_dispatch` 手工触发跑通构建与打包，确认无误后才推真 tag 触发完整发布。
- **逐条核对可能挖出真缺陷**，导致范围膨胀。约束：发现即记 backlog，本项不修。
- **模块路径订正碰 563 处**。缓解：纯机械替换 + 全量测试兜底，改动面可 grep 归零验证。
