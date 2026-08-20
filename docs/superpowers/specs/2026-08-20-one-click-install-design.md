# 一键安装脚本与 `sq status` 设计

> 状态：待用户复核
> 日期：2026-08-20
> backlog：未立项（本 spec 先于 backlog 条目产生）

## 1. 背景与目标

v0.1.0 已经有发布包和 `deploy/install.sh`，但从「下载」到「服务在跑」之间，
用户仍要自己完成三件容易出错的事：

1. **认平台、下包、核校验和**——README 里是一串手抄的 `curl` + `tar`；
2. **写配置**——`install.sh` 拷的是 `sq.example.yaml`（一份 100 行的教学文档，
   集群段整体注释掉）。用户必须自己把 `data_dir` 改成 `/var/lib/sq`
   （README 用粗体提醒过，说明这是个已知会被忘的坑），集群部署还要手写
   `node_id` / `raft_listen` / 三条 `peers`；
3. **装完不知道成没成**——尤其集群档，三台各自装完之后没有任何单一命令
   能回答「集群成型了吗、谁是 leader、谁落后」。

目标：

- 单机与三节点都能用**一条命令**完成「取包 → 装盘 → 生成正确配置」，
  且三节点的三次执行只差一个 `--node-id`；
- 装完之后有**一个命令**能自证集群状态，退出码可直接写进监控。

**不是目标**：编排。本脚本不 SSH、不分发、不代替 ansible。三台机器各跑一次
是刻意的形态选择（见 §3.1）。

## 2. 范围

### 做

| # | 交付件 | 性质 |
|---|---|---|
| ① | `deploy/quickstart.sh` | 新增脚本 |
| ② | `ClusterPeer.AdminPort` 配置字段 | 配置面新增一个可选字段 |
| ③ | `sq status` 子命令 | 新增 Go 代码 |
| ④ | release 工作流把 quickstart.sh 打进包 | CI 一行 |
| ⑤ | 文档更新（README / docs/deployment.md） | 文档 |

### 不做（含理由，不要再议）

- **改 `deploy/install.sh`**——它的边界（「不 enable、不 start」）继续成立，
  一个字不改。衔接方式见 §3.2。
- **自动启动服务**——quickstart 装完打印启动命令，由用户自己执行。理由与
  install.sh 同源：安装器擅自起服务在编排系统里会与其调度打架。
- **交互式向导**——纯 flag。`curl ... | bash` 时 stdin 被管道占着，交互分支
  必须 `</dev/tty` 绕，且无法写进自动化脚本。
- **端口可调**——gRPC `8081` / Admin `8082` / Raft `9081` 写死。三台机器端口
  不一致会静默凑出一个连不上的集群，而脚本看不到另两台、无从校验。端口被占
  的用户自己改 `/etc/sq/sq.yaml` 重启。
- **SSH 分发 / 多机编排**——见 §1「不是目标」。
- **本机 IP 自动探测**——多网卡 / 容器 / NAT 下探出来的地址经常是错的，
  静默配错比让用户显式写一次更糟。
- **`--dry-run`**——落盘前把生成的配置打到 stdout 即可达到同样效果，
  少一个 flag 少一条测试路径。
- **非 systemd / 非 Linux**——脚本前置校验直接拒绝。macOS 发布包存在，
  但它没有 systemd，装了也托管不起来。

## 3. 架构

### 3.1 三节点的形态：三台各跑一次

```
机器 A:  quickstart.sh --cluster --node-id 1 --peers 10.0.0.1,10.0.0.2,10.0.0.3
机器 B:  quickstart.sh --cluster --node-id 2 --peers 10.0.0.1,10.0.0.2,10.0.0.3
机器 C:  quickstart.sh --cluster --node-id 3 --peers 10.0.0.1,10.0.0.2,10.0.0.3
```

`--peers` 的顺序即 node id。三次执行**只有 `--node-id` 不同**，这是刻意的：
让「三台参数必须一致」这条约束在命令行上肉眼可查。

选它而不选 SSH 推送，是因为 SSH 形态要引入免密、跨机 sudo 提权、半途失败
回滚这一整套前提，而这些前提失败时的现场比手工装三次更难排查。

### 3.2 分层：quickstart.sh 与 install.sh

| | `quickstart.sh`（新增） | `install.sh`（现有，不改） |
|---|---|---|
| 定位 | 用户显式选择的「全都帮我办了」 | 发布包内的装盘器 |
| 管 | 取包、生成配置、调 install.sh、打印下一步 | 建用户、建目录、装二进制/单元、daemon-reload |
| 不管 | 装盘细节（全部委托） | 配置内容、启动 |

**关键衔接点**：`install.sh` 对已存在的 `/etc/sq/sq.yaml` 一律不覆盖
（`deploy/install.sh` 里的 `if [[ -f "${CFG_DST}" ]]` 分支）。因此
quickstart **先写配置、再调 install.sh**——install.sh 走到那个分支自然让路，
打一行「已存在，保留不动」就过去了。

两者零冲突，不需要给 install.sh 加任何开关。这个顺序是承重的，**不要
调换**：反过来会让 install.sh 先写一份 `sq.example.yaml` 的副本，quickstart
再去覆盖它，「不覆盖」这条保护就形同虚设。

否掉的两个替代：**quickstart 自己实现装盘**（路径约定、建用户、单元内容会变成
两份，改一处漏一处）；**把配置生成塞进 install.sh**（发布包用户拿到的
install.sh 行为要跟着变，且「装」和「配」揉进同一个文件）。

### 3.3 执行链路

```
quickstart.sh
  ├─ 1. 前置校验：root / systemd / Linux / 参数自洽
  ├─ 2. 取包（唯一的分叉，两条路径汇合到「一个含 sq+install.sh+sq.service 的目录」）
  │      ├─ 同目录已有 sq 二进制 → 用所在目录（发布包内场景）
  │      └─ 否则 → 按 uname 识别平台 → 拉 tarball + SHA256SUMS 校验 → 解到临时目录
  ├─ 3. 生成 /etc/sq/sq.yaml，权限 0600 root:root（先打到 stdout，再落盘）
  ├─ 4. 调包内 install.sh（装盘、建 sq 用户；它会跳过已存在的配置）
  ├─ 5. 回头收权限：chown root:sq + chmod 0640（依赖第 4 步建出的 sq 用户）
  ├─ 6. 拷 sq.example.yaml → /etc/sq/sq.example.yaml（字段参考）
  └─ 7. 打印下一步（配置在哪、凭据是什么、怎么启动、怎么开机自启、怎么看状态）
```

第 2 步之后的全部步骤两条路径共用。

第 3 步与第 5 步**必须分开**：配置里有明文密码，写出来的那一刻就不能是
世界可读，所以先 `0600 root:root`；而目标属组 `sq` 要等 install.sh 建完用户
才存在，所以 `chown` 只能放在其后。理由详见 §4.5。

## 4. `quickstart.sh` 详细设计

### 4.1 参数

```
--cluster                 集群档（不给 = 单机档）
--node-id N               本机是 peers 里的第几个，集群档必填
--peers ip1,ip2,ip3       全体成员地址，顺序即 node id，集群档必填
--advertise-host HOST     对外地址；单机档默认 127.0.0.1
--admin-user U            admin 控制台用户名，默认 admin
--admin-password P        不给则自动生成（见 §4.5）
--no-admin-auth           显式关闭管理面鉴权
--version X.Y.Z           指定下载版本，默认拉最新 release
--tarball PATH|URL        直接指定包，绕开 GitHub
--force                   已有配置时备份后覆盖
-h / --help
```

参数校验（全部在动任何盘之前完成）：

- `--cluster` 时 `--node-id` 与 `--peers` 必填；
- `--peers` 至少一个元素，`--node-id` 须落在 `1..len(peers)`；
- `--peers` 元素去重校验（重复地址必然是复制粘贴错误）；
- 偶数个 peers 打警告不拒绝（与 `config.go` 现有行为一致）；
- `--no-admin-auth` 与 `--admin-user` / `--admin-password` 互斥；
- 非 `--cluster` 时给了 `--node-id` / `--peers` → 报错（避免用户以为装了集群）。

### 4.2 取包

- 平台识别：`uname -s` 须为 `Linux`；`uname -m` 的 `x86_64`→`amd64`、
  `aarch64`/`arm64`→`arm64`，其余拒绝；
- 版本：`--version` 未给时向 `https://api.github.com/repos/Xsxdot/sq/releases/latest`
  取 `tag_name`。**这一步失败不是致命错**——报错信息里必须给出
  `--version` 与 `--tarball` 两个逃生口（未认证的 GitHub API 有每 IP 60 次/小时
  限流，国内网络还可能直接不通）；
- 下载 `sq_<ver>_linux_<arch>.tar.gz` 与同目录 `SHA256SUMS`，**校验和不匹配即中止**；
- `--tarball` 给本地路径时跳过下载与校验（用户自己拿的包，自己负责）；
  给 URL 时下载但跳过校验（旁路仓库没有 SHA256SUMS）；
- 临时目录用 `mktemp -d`，`trap ... EXIT` 清理。

### 4.3 重跑语义

检测到 `/etc/sq/sq.yaml` 已存在或 `/var/lib/sq` 非空时：

- **默认**：打印现状（已有配置的形态摘要、数据目录大小）后**退出非零，不动任何东西**；
- **`--force`**：把旧配置改名为 `/etc/sq/sq.yaml.bak.<时间戳>` 后写新的；
- **`--force` 也从不碰数据目录**。集群档的 `data_groups` 首启即持久化、此后不可变，
  数据目录里是盘上事实，脚本没有资格删。要换布局请用户自己确认后手工清理。

这条是为了堵住一个具体陷阱：用户第一次把 `--peers` 写错了，改参数重跑，
若沿用 install.sh 的「不覆盖」语义，跑的仍是旧配置而用户以为已生效。

### 4.4 生成的配置

进程内默认值（`internal/config/config.go` 的 `Load`）与 `sq.example.yaml`
**逐字段完全一致**，所以生成文件只写与本机部署形态相关的字段，其余省略，
行为完全可预测。

单机档：

```yaml
# 本文件由 sq quickstart.sh 生成，形态：单机
# 未列出的字段走进程内默认值，字段说明见 /etc/sq/sq.example.yaml
data_dir: "/var/lib/sq"
advertise_host: "127.0.0.1"
```

集群档（node 2 为例）：

```yaml
# 本文件由 sq quickstart.sh 生成，形态：集群，本机 node_id=2
# 未列出的字段走进程内默认值，字段说明见 /etc/sq/sq.example.yaml
data_dir: "/var/lib/sq"
advertise_host: "10.0.0.2"
cluster:
  node_id: 2
  raft_listen: ":9081"
  data_groups: 3
  ack: quorum-mem
  peers:
    - { id: 1, raft_addr: "10.0.0.1:9081", advertise_host: "10.0.0.1", advertise_port: 8081, admin_port: 8082 }
    - { id: 2, raft_addr: "10.0.0.2:9081", advertise_host: "10.0.0.2", advertise_port: 8081, admin_port: 8082 }
    - { id: 3, raft_addr: "10.0.0.3:9081", advertise_host: "10.0.0.3", advertise_port: 8081, admin_port: 8082 }
```

两档都追加 `admin_username` / `admin_password` 两行（默认自动生成，见 §4.5）；
只有显式 `--no-admin-auth` 时这两行才不出现。

为什么是最小配置而不是那份注释版：抄注释版会让「脚本决定的」和「默认值」
混成一片，出问题时看不出这台机器的身份是什么。最小配置一眼看完，且
`diff` 三台的配置只会显出该不同的那几行。字段说明另拷一份
`sq.example.yaml` 到 `/etc/sq/` 供查。

顺带修掉的现存脚：`data_dir` 直接写绝对路径，README 里那句「**明确把
`data_dir` 设为 `/var/lib/sq`**」的粗体提醒对走 quickstart 的用户不再需要。

`advertise_host`：集群档自动取 `peers[node-id-1]`，用户不必额外给；单机档
默认 `127.0.0.1`，此时脚本**显式警告**「远程客户端连不上，要连远程请加
`--advertise-host`」。

### 4.5 admin 凭据与配置文件权限

**默认生成，不默认敞开。** 用户名默认 `admin`，密码在未给 `--admin-password`
时自动生成。想要旧的免鉴权行为必须显式 `--no-admin-auth`——一键脚本默默敞开
一个无鉴权管理面是不可接受的默认值，而"默认安全、要不安全得自己说出口"是
唯一站得住的取舍。

**生成方式**：`LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 24`。
不用 `openssl rand`——openssl 不是所有精简发行版都装。不用 `$RANDOM`
（16 位 LCG，不是密码学随机源）。

#### 集群档的一致性问题（承重）

`sq status` 在 follower 上要跳到 leader 的管理面（§6.2 第 6 步），用的是**本机
配置里的凭据**。三台各自生成会得到三个不同的密码，跳转必然 401，
`sq status` 从此永远降级到码 3。

处置：**第一台生成，另两台显式携带**。集群档下未给 `--admin-password` 时，
脚本生成后在结尾提示里打印一条可直接复制的命令，把生成值明码带进另两台的
调用：

```
[quickstart] ⚠ 另外两台必须使用同一组凭据，否则 sq status 无法跨节点查看。
[quickstart]   在机器 10.0.0.2 上执行：
[quickstart]     ./quickstart.sh --cluster --node-id 2 --peers 10.0.0.1,10.0.0.2,10.0.0.3 \
[quickstart]       --admin-user admin --admin-password 'K3f9…（完整值见上）'
```

否掉的两个替代：**集群档拒绝自动生成、强制显式给**（安全等价，但把
「一键」变成「先自己想个密码」，与本项目标冲突）；**由 `--peers` 派生确定性
密码**（`--peers` 是公开信息，等于没有密码）。

脚本**无法检测**用户是否真的在另两台带了同一个值——它看不到另两台。所以这条
只能靠提示的醒目程度兜住，失败模式是 `sq status` 降级到码 3（不是数据损坏，
可事后改配置重启修复）。这个残余风险是已知且接受的。

#### `--force` 重跑不重新生成密码

`--force` 且未显式给 `--admin-password` 时，从**被备份的旧配置**里提取
`admin_password` 复用，提取不到才新生成。否则一次无关的重跑（比如改
`--advertise-host`）会静默换掉密码，让这台机器与另两台失配。

#### 配置文件权限

配置里落明文密码，权限必须收紧。现有 `install.sh` 装配置用的是
`install -m 0644`（世界可读），但因 quickstart 先写配置、install.sh 走
「已存在不覆盖」分支，**那行 `0644` 根本不会作用到 quickstart 生成的文件上**
——install.sh 仍然一字不改。

目标权限：`0640`，属主 `root`，属组 `sq`。systemd 单元以 `User=sq` 运行，
进程必须读得到；root 之外的普通用户读不到。

**这带来一处执行顺序约束**：`sq` 用户是 install.sh 建的，而 quickstart 先写
配置——写的时候该用户还不存在，`chown` 会失败。故 §3.3 的链路调整为：
先以 `0600 root:root` 写配置 → 调 install.sh（建用户、跳过配置） → **回头
`chown root:sq` + `chmod 0640`**。顺序不能省，也不能把 chown 提前。

### 4.6 结尾提示

```
[quickstart] 安装完成。形态：集群 node_id=2 / 3 节点

[quickstart] 配置文件：/etc/sq/sq.yaml   （0640 root:sq，字段说明见 /etc/sq/sq.example.yaml）
[quickstart] 数据目录：/var/lib/sq
[quickstart] 二进制  ：/usr/local/bin/sq

[quickstart] 控制台   ：http://10.0.0.2:8082/
[quickstart] 用户名   ：admin
[quickstart] 密码     ：K3f9xQ7mB2vLpN4sT8wZ （已自动生成，也在配置文件里）

[quickstart] 立即启动    ：systemctl start sq
[quickstart] 开机自启    ：systemctl enable sq
[quickstart] 一步到位    ：systemctl enable --now sq
[quickstart] 进程状态    ：systemctl status sq
[quickstart] 集群状态    ：sq status -config /etc/sq/sq.yaml

[quickstart] ⚠ 三台都装完并启动后，集群才会选出 leader。
[quickstart] ⚠ 另外两台必须使用同一组凭据，否则 sq status 无法跨节点查看：
[quickstart]     ./quickstart.sh --cluster --node-id 3 --peers 10.0.0.1,10.0.0.2,10.0.0.3 \
[quickstart]       --admin-user admin --admin-password 'K3f9xQ7mB2vLpN4sT8wZ'
```

凭据三行只在启用了鉴权时打印（即未给 `--no-admin-auth`）；密码自动生成时才
带「已自动生成」后缀，显式传入时不回显。集群一致性那条警告只在集群档且
密码为自动生成时打印——用户自己传了密码就说明他知道要传给另两台。

`--no-admin-auth` 时改打一条警告：`⚠ 管理面 :8082 无鉴权，请用防火墙限制来源。`

## 5. `ClusterPeer.AdminPort` 配置字段

`internal/config/config.go` 的 `ClusterPeer` 现有四个字段（`id` / `raft_addr` /
`advertise_host` / `advertise_port`），**没有管理面地址**；`admin_listen` 是顶层
字段，只描述本节点自己。因此从配置读不到「别的节点的 admin 端口」。

新增：

```go
AdminPort int `yaml:"admin_port"` // 该节点管理面端口；0 = 未填，回落取本机 admin_listen 的端口
```

- **非破坏性**：存量配置不含该字段时值为 0，回落到本机端口，行为与今天一致；
- 校验：非 0 时须落在 `1..65535`，否则拒启（与其余端口校验同规格）；
- 只被 `sq status` 消费。集群运行时不读它——**这一点要在字段注释里写死**，
  否则后来者会以为它参与了拓扑。

替代方案与否决理由：

- **写死 8082**——信息不在配置里就凭空假设，异构部署直接错；
- **用本机 admin 端口 + peer 的 advertise_host 拼**——零成本，但仍是「三台同构」
  的假设。quickstart 装出来的集群确实同构，可 `sq status` 不只服务于 quickstart
  装出来的集群；
- **让本节点直接报 peer 死活**（raft 传输层本就与每个 peer 保持连接，把连接
  存活状态加进 `/admin/cluster`）——最干净、且完全不需要 admin 端口跨机可达，
  但要动 `replication.ClusterView` 与传输层，是三者里改动最大的。**这个方案
  没有被证伪，只是没被选**；将来若 admin 端口跨机不可达成为真实痛点，它是
  首选的下一步。

## 6. `sq status` 详细设计

### 6.1 入口与数据来源

挂在 `cmd/sq/main.go:78` 现有子命令分派旁边，与 `sq recover` 同构：

```go
if len(os.Args) > 1 && os.Args[1] == "status" { ... }
```

`-config` 可选（缺省走进程内默认值，正好对上默认部署的 `:8082`）。

**只能走 admin HTTP**：sq 进程持有 Pebble 独占锁，服务运行时离线直读数据目录
必然失败。副作用是 `admin_listen: ""` 时 `sq status` 无数据来源，此时必须
**立刻明确报错**（「管理面已关闭（admin_listen 为空），sq status 无数据来源」）
而不是去连一个空地址再超时。

### 6.2 流程

1. `config.Load(-config)`；
2. `admin_listen` 为空 → 报错退出（码 1）；
3. `admin_username` 非空 → `POST /admin/login` 换 Bearer token（`internal/admin/auth.go`，
   token 内存表 TTL 24h）；否则直连；
4. `GET /admin/cluster`；
5. `enabled == false` → 单机档：再取 `/admin/overview`，渲染后退出 0；
6. `enabled == true`：取元数据组（`cluster.MetaGroup`，固定组号 `0`）的 `leader`；
   - 本机即 leader → 直接渲染；
   - 否则 → 用 `peers[leader].advertise_host` + 该 peer 的 `admin_port` 重拉一次
     `/admin/cluster`（同一套凭据）；
   - **重拉失败不算失败**：降级为本机视角并在输出里显式标注
     「视角：本机（follower）— 未能连上 leader，peer 复制进度不可见」。
     看得到多少报多少，比整条命令失败有用。
7. 渲染 + 按 §6.4 定退出码。

第 6 步「跳到 leader」的必要性：`GroupView.Peers`（各 peer 的复制进度）
只在本节点是该组 leader 时才有内容，follower 上是空表——这是 raft 的
`tracker.Progress` 只在 leader 侧维护决定的，不是 bug。

### 6.3 输出

集群档：

```
sq 0.1.0   本机 node 2   集群 3 节点   数据组 3
视角：node 1 (10.0.0.1:8082) — leader

成员                                组
 id  地址                状态         组  leader  term  applied  待apply
  1  10.0.0.1            ● 视角源      0       1     5    10231        0
  2  10.0.0.2  (本机)    ● 活跃        1       1     5    88213        0
  3  10.0.0.3            ○ 失联        2       3     7    41002        0
                                       3       1     5    77120        4
```

「待apply」= `Commit − Applied`，每个节点自己就算得出（不依赖 leader 数据），
applier 卡住会立刻显形。

单机档：

```
sq 0.1.0   单机模式
topic 12   消费组 5   消息 1,203,441   磁盘 34% / 水位 85%
```

### 6.4 退出码

| 码 | 含义 |
|---|---|
| 0 | 健康：全部组有 leader，且全部 peer `recent_active` |
| 1 | 够不着本机管理面：服务没起 / `admin_listen` 为空 / 凭据错 / HTTP 失败 |
| 2 | 集群降级：有组 `leader == 0`，或有 peer 失联 |
| 3 | 判定不完整：视角降级（§6.2 第 6 步跳转失败），组 leader 判据通过但 peer 活跃度不可见 |

分级的目的是能直接写进监控脚本与 `ExecStartPost` 之类的钩子。三个非零码分别对应
三种完全不同的处置：1 是「我什么都不知道，先去看这台机器」，2 是「集群确实有问题」，
3 是「我只看到了一半，去 leader 上再看一眼」。

码 3 是必须单列的，不能并进 0 或 2。视角降级时 `GroupView.Peers` 是空表——**空表
不等于没有 peer，只等于本节点不是该组 leader**。此时报 0 是拿一份看不见 peer 的
视图谎称健康，正是监控脚本最不能容忍的；报 2 又是在没有任何证据的情况下声称故障。
判定优先级：组 leader 判据不通过一律先判 2（这条在 follower 视角下同样成立，
因为 follower 也知道每组的 leader 是谁）；leader 判据通过、且视角降级时才判 3。

## 7. 测试策略

**`sq status`**

- 渲染与判级抽成纯函数（`replication.ClusterView` + overview → 文本 + 退出码），
  单测覆盖六种及其期望退出码：单机档（0）、集群全健康（0）、有组无 leader（2）、
  有 peer 失联（2）、视角降级且 leader 判据通过（3）、视角降级且有组无 leader（2
  ——验证「leader 判据优先于降级」这条优先级）；
- HTTP 交互层用 `httptest` 打桩，测三条：登录换 token、follower 跳 leader、
  跳转失败降级为本机视角；
- `admin_listen` 为空时的即时报错单测。

**`quickstart.sh`**

- `shellcheck` 进 CI；
- 参数校验用例：缺 `--node-id`、`--node-id` 越界、`--peers` 有重复、
  非 `--cluster` 却给了集群参数、`--no-admin-auth` 与 `--admin-password` 同给、非 root；
- 配置生成用例：单机档 / 集群档三个 node-id 各生成一次，与期望文本逐字比对。
  密码是随机的，**比对前把 `admin_password:` 那行掩掉**（或用环境变量注入
  固定的生成器桩），否则用例恒挂；
- 凭据用例：默认生成时长度为 24 且字符集合法、`--admin-password` 显式传入时
  原样落盘、`--no-admin-auth` 时 `admin_username`/`admin_password` 两行不出现；
- 权限用例：生成后立刻是 `0600 root:root`，走完 install.sh 后变为 `0640 root:sq`
  ——**中间态也要断言**，这是「明文密码不得有世界可读的时间窗」的判据；
- `--force` 密码复用用例：已有配置含密码 P，`--force` 且不给 `--admin-password`
  重跑后新配置里仍是 P（而不是新生成值）；旧配置无密码时才新生成；
- 取包分支用 `file://` 指向本地打好的 tarball 打桩，覆盖校验和不匹配即中止；
- 重跑语义用例：已有配置时默认退出非零且文件未被修改、`--force` 时备份文件存在。

**真机**

1. 三台 Linux 各跑一次 quickstart：第一台不给密码（走自动生成），后两台按结尾
   提示带上 `--admin-user/--admin-password`。确认生成的三份配置只有身份字段不同，
   凭据完全一致，且三份都是 `0640 root:sq`；
2. 只启动第一台，跑 `sq status` —— 期望退出码 2（无 leader），验证 §9 的
   「首台无 leader 是预期行为」；
3. 三台全部启动，在三台上分别跑 `sq status` —— 期望全部 0，且 follower 上
   输出的「视角」行显示已跳到 leader；
4. 停掉一台，在 leader 上跑 —— 期望 2 且该节点标为失联；
5. 制造视角降级（在某台 follower 上用防火墙挡掉到 leader 的 8082），
   跑 `sq status` —— 期望退出码 3 且输出标注 peer 进度不可见。

**这一步必须真跑**——脚本类交付件「单测全绿」和「真能装」是两回事。第 5 步
尤其不能只靠单测：降级路径是 §6.2 里唯一一条依赖跨机网络的分支。

## 8. 文档改动

- `README.md`：快速开始加 quickstart 一行命令（单机）；「集群模式」提一句三节点用法；
- `docs/deployment.md`：新增 quickstart 小节（单机 + 三节点完整示例），
  并写明 admin 密码默认自动生成、集群三台必须共用同一组凭据、配置文件为
  `0640 root:sq`；
  「明确把 `data_dir` 设为 `/var/lib/sq`」的提醒改为「手工装才需要，走 quickstart 已写好」；
- `docs/configuration.md`：集群配置补 `admin_port` 字段说明；
- `.github/workflows/release.yml`：`cp deploy/sq.service deploy/install.sh` 那行加上
  `deploy/quickstart.sh`，并 `chmod +x`。

## 9. 已知边界

- **首台机器在另外两台起来之前根本起不来**。08-20 三机实测订正：装配序会在
  `cmd/sq/main.go:336` 阻塞等待元数据组选出 leader，`metaLeaderWaitTimeout`
  为 60s，超时即 `StopClean` 后退出（注释写明理由：半残启动会让全部 meta 写
  路径以 HA_NOT_AVAILABLE 毒化客户端路由缓存，比拒绝启动更糟）。配合
  `Restart=on-failure` + `RestartSec=5`，该节点会重启循环直到多数派出现。
  此期间管理面未监听，`sq status` 报 **1（够不着管理面）而不是 2**。

  **本条原先写的是「进程正常运行不退出、`sq status` 报 2」，是错的**——写 spec
  时只读到装配序注释里「等 meta 组」几个字，没有跟进它是阻塞且带超时的。
  容器与单机都验不出来（不涉及多数派），只有三机真跑才会暴露。
- **`sq status` 依赖 admin 端口跨机可达**（仅集群档且本机非 leader 时）。不可达
  时降级为本机视角，功能不中断但看不到 peer 进度。彻底解法见 §5 的第三个替代方案。
- **各节点 admin 凭据须一致**（仅集群档且本机非 leader 时）。密码默认自动生成，
  而**自动生成在三台机器上并不天然一致**——这正是 §4.5 要靠结尾提示把第一台的
  生成值传给另两台的原因。用户没照做时跳转会 401 并降级为本机视角
  （退出码 3）。脚本看不到另两台，无法检测，只能靠提示兜住。
  事后修复方式：把三台的 `admin_password` 改成同一个值并重启。
