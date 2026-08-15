# 2026-08-14 补齐四块未覆盖面（临时三机）

承接 [最终真机验证](2026-08-14-real-machine-verification.md) 末尾列出的四块未覆盖面。
被验对象仍是集成分支 `verify/final-integration`（tip `629151c`）。

## 结论速览

| 块 | 结果 |
|---|---|
| ① Linux 侧 `-race` | 单测 18/18 包全过、零 race；**但 e2e（broker 带 `-race`）挖出一个真 race** ——见下方「发现 1」 |
| ② 多语言 SDK 冒烟（B13.3） | Python / C# / C++ 三门全部收发通过，含开鉴权与错误凭据必拒 |
| ③ 吞吐基准 | 跨机 quorum-mem 中位 10.4k–16.3k msg/s；**复制税≈0，共置税 1.6–2.2×**——与既有归因一致 |
| ④ 跨物理机组网 | 三机真实 raft 组网通过：跨机选主、写转发、杀 leader 切换、重启追平 |

## 机器

阿里云三台临时机，同规格：Ubuntu 24.04.4 / 2 核 / 3.5GB / vda 40G / root 免密。

| 公网 | 内网 | 本轮角色 |
|---|---|---|
| 47.80.240.57 | 172.19.25.181 | 集群 node1；bench 客户端；C++ 构建与冒烟 |
| 47.80.243.155 | 172.19.25.182 | 集群 node2；Go 工具链 + `-race` |
| 47.80.243.95 | 172.19.25.183 | 集群 node3；SDK 冒烟 broker（免鉴权 :8081 / 开鉴权 :8091） |

三台的 ssh host key 与 known_hosts 里的旧记录不符（临时机重装），已 `ssh-keygen -R` 后重新接受。
内网段 `172.19.25.18x` 三台互通；**公网 8081/8082 被安全组挡住**，所有 Admin API 调用都在机内发起。

> 内网 IP 与既有记录不同：[[sq-bench-three-machines]] 记的是 172.19.25.184/185/186，
> 本批是 181/182/183。机器重开过，跨批次硬件不可比这条依然成立。

---

## 发现 1（真 bug）：`store.OnApplyObserve` 在集群档下的启动期数据竞态

**这是本轮唯一的真实缺陷，由「broker 带 `-race` 跑 e2e」挖出——单测 `-race` 全绿没能发现它。**

竞态双方（20 次报告全部同一个地址、同一对调用栈）：

```
Write at 0x24ea568 by main goroutine:
  metrics.NewRegistry()        internal/metrics/collector.go:190
  main.run()                   cmd/sq/main.go:389

Previous read at 0x24ea568 by goroutine 48:
  store.(*Store).ApplyWith()   internal/store/store.go:205
  cluster.(*group).applyEntries()  internal/cluster/group.go:1506
  cluster.(*group).runApply()      internal/cluster/group.go:838
```

竞争对象是包级钩子 `store.OnApplyObserve`（`internal/store/store.go:31`）。

**根因是启动顺序违反了代码里自己写下的契约。** `cmd/sq/main.go:385-388` 明确写着：

> metrics registry 必须先于任何后台 goroutine 装配：NewRegistry 会写包级钩子
> store.OnApplyObserve，其契约是「装配阶段设置一次、之后只读」——retention/delay
> 的 goroutine 启动即可能走 store.Apply 读这个钩子，放到它们之后装配就是契约禁止的
> 无同步并发读写。

这段推理只点了 retention/delay，**漏了集群档的 raft apply goroutine**：`m.Start(runCtx)`
在 `main.go:299` 就把它们拉起来了，比 `NewRegistry`（`main.go:389`）早 90 行。单机档没有
这条路径，所以问题只在集群档暴露。

**为什么单测没抓到**：这条竞态是 `cmd/sq/main.go` 的装配顺序问题，任何包内单测都不
经过 main 的启动编排。必须让「带 `-race` 的真 broker 进程」跑集群档才会现形。

**修复不是一行挪动**：`NewRegistry(st, mt, sys, tx, srv, logger)` 依赖的 `mt/tx/srv`
都在 `cluster.NewManager` 之后才构造，没法简单前移到 `m.Start` 之前。建议把
`OnApplyObserve` 改成 `atomic.Pointer[func(time.Duration)]`（读路径一次原子 load，
热路径代价可忽略），而不是继续用「靠顺序保证」的裸函数指针——顺序契约已经被打破过
一次，说明它不是一个守得住的约束。**这需要一份正经的 plan，我没有就地改。**

已记入 backlog（见 B14）。

## 发现 2（可诊断性）：正常恢复路径打 ERROR 日志

跨机集群里 `kill -9` 掉 node1 再拉起，每次都会打 4 条（每组一条）：

```
level=ERROR msg="不干净关机后本地恢复：任期已抬、投票已清" mod=raftstore g=0 term=3
```

出处 `internal/cluster/raftstore.go:1191`，走的是 `pathLocalResume` + quorum-mem，
是 `manager.go:466` 注释写明的**预期路径**（mem 档投票走 NoSync，可能没落盘，所以抬任期）。
后果只是多一轮选举。把一条documented 的正常恢复打成 ERROR，会训练运维忽略 ERROR——
这正是 B12 关心的那类可诊断性问题。建议降为 WARN。已记入 backlog（见 B15）。

---

## ① Linux 侧 `-race`

远端装 Go 1.26.1（用户已授权「随意折腾」，且仓库要求 1.26.1、机器自带 go1.22.2 不够用），
`git archive verify/final-integration` 送源码，`GOPROXY=https://goproxy.cn,direct`。

**单测全量：18/18 包 PASS，`DATA RACE` 计数 0，退出码 0。** 最慢 `internal/cluster` 100.8s、
`internal/rpc` 34.0s。

**e2e（broker 用 `go build -race` 构建）：这才是真正有价值的一档**，结果见发现 1。
最终计数 **37 PASS / 11 FAIL / 2 SKIP**（2 SKIP 仍是需 `SQ_BENCH=1` 的两条基准），
20 条 `WARNING: DATA RACE` 全部指向同一个地址、同一对调用栈，即**同一个 bug 被 20 个
broker 进程各报一次**。失败面必须说清楚归因：

- 10 条集群/场景用例 FAIL，**全部是同一个原因的下游**：race detector 让 broker 进程以
  `exit status 66` 退出，而 harness 断言 SIGTERM 后必须干净退出（`cluster_proc_test.go:288`
  「broker 未能干净退出：SIGTERM 后 cmd.Wait 返回 exit status 66（期望退出码 0）」）。
  不是 10 个独立故障。
- `TestConsoleServedFromBinary` FAIL 是**我的取证方式的产物，不是缺陷**：`web/dist` 按
  设计不入库（`web/embed.go:8`「dist/ 由 make web 生成且不入库」），源码 tarball 里没有
  构建好的控制台，broker 日志明写「控制台未构建，/ 将返回构建提示」。Lenovo 那轮用的是
  本地工作树编译的二进制、带控制台，所以那边这条是 PASS。

## ② 多语言 SDK 冒烟（B13.3）

每门语言一条最小冒烟：免鉴权往返 + 开鉴权往返 + **错误凭据必须被拒**。
第三项刻意用「已存在且刚成功收发过」的 topic，否则失败原因会和「topic 不存在」混淆。

| 语言 | 客户端 | 免鉴权 | 开鉴权 | 错误凭据 |
|---|---|---|---|---|
| Python 3.12 | `rocketmq-python-client`（PyPI） | PASS | PASS | PASS（被拒） |
| C# / .NET 8 | `RocketMQ.Client` 5.2.1（NuGet） | PASS | PASS | PASS（`Unauthenticated / 认证失败`） |
| C++ | `apache/rocketmq-clients` cpp，CMake 构建 | PASS | PASS | PASS（0 条发出） |

**鉴权这一栏是 B13.3 的核心**：README 此前只承认「按官方源码格式对齐、没跑过」。
现在三门语言的签名头都被 sq 实际验过，broker 侧留有对称证据——错误凭据在
`rpc.auth` 打出 `认证失败：AK 或签名不匹配 method=/apache.rocketmq.v2.MessagingService/Telemetry`。

C++ 那条还顺带覆盖了 `ChangeInvisibleDuration`：`example_simple_consumer` 收 4 条时
对其中一条连续改了 3 次不可见期，sq 全部正常应答。

三处与 sq 无关、但会卡住复现的环境事实，记下来省下次的时间：

1. **C# 客户端默认走 TLS**，sq 是明文 gRPC，不调 `.EnableSsl(false)` 会死在 QueryRoute
   （`AuthenticationException: Cannot determine the frame size`）。等价于 Go SDK 的包级 `EnableSsl`。
2. **Python 客户端 `keys` 的 setter 签名是 `(self, *keys)`**，property 赋值只能传单值；
   传 list 会让它对 list 本身调 `.strip()` 而抛异常。
3. **C++ 用 CMake 而非 Bazel 可以构建**（Ubuntu 24.04 的 libgrpc++-dev 1.51 + libprotobuf-dev
   3.21 够用），但有两个坑：`protos/` 是 git submodule，`--depth 1` 克隆后 `cpp/proto/**` 全是
   断链符号链接，要 `git submodule update --init --recursive`；且它用
   `find_package(protobuf CONFIG)`，而 Ubuntu 的 libprotobuf-dev 不装 `protobuf-config.cmake`，
   需要一个转接到模块模式的垫片。

另：`SimpleConsumer` 每次 receive 只轮询一个队列（SDK 内部按队列轮转），冒烟 topic 一律
预建成 **1 个队列**，否则收不到不是因为消息没到，而是轮不到那个队列。

## ③ 吞吐基准

参数：200000 条 / 128B / 队列 24 / 并发档 16·64·256，跨机档重复 3 轮取中位数
（topic 名带轮次标签，避免沿用同名 topic 继承上一轮的队列数与既有数据）。

**跨机（每机一节点，客户端与 node1 共机）**，msg/s：

| 档位 | conc=16 | conc=64 | conc=256 |
|---|---|---|---|
| quorum-mem | 10395 | 13665 | 16307 |
| quorum-fsync | 3146 | 6051 | 8595 |

三轮离散度：mem 档 5%/15%/18%，fsync 档 8%/11%/27%。**conc=256 的 fsync 档离散 27%，
单点数字不可引用**，与 [[fsync-large-body-high-conc-unmeasurable]] 记的现象同类。

**共置（三节点 + 客户端全挤一台 2 核机，单轮）**，msg/s：

| 档位 | conc=16 | conc=64 | conc=256 |
|---|---|---|---|
| cluster/quorum-mem | 6398 | 7019 | 7465 |
| cluster/quorum-fsync | 1122 | 2143 | 4976 |
| standalone（单机基线） | 7434 | 13658 | 17650 |

两个可引用的结论：

- **复制税≈0**：跨机 quorum-mem 对同硬件单机基线，conc=64 持平（13665 vs 13658）、
  conc=256 −8%（16307 vs 17650）、conc=16 反而更高。三节点复制本身几乎不要钱。
- **共置税 1.6–2.2×**：同为 quorum-mem，跨机比共置快 1.62×(conc16) / 1.95×(conc64) /
  2.18×(conc256)。三份 CPU 抢 2 核、三条 WAL 落同一块盘才是真正的代价。

两条都与 [[cluster-perf-attribution-2026-08-12]] 的既有归因一致，本轮是在真实每机一节点
部署下的独立复现。fsync 档相对 mem 档的代价随并发下降（3.3× → 2.3× → 1.9×），
是 group commit 在起作用。

**不可引用的部分**：客户端与 node1 共机，跨机档的数字含客户端 CPU 争抢；
2 核机与三机 bench 的历史数字不可跨批比较。

## ④ 跨物理机组网

三节点 raft，`data_groups: 3`（meta 组 + 3 数据组），peers 用内网 IP。

1. **跨机选主**：4 个组的 leader 自然分散到 3 台机（g0→1、g1→2、g2→3、g3→1），
   所有 peer `StateReplicate` / `recent_active: true`。
2. **meta 跨机复制**：在 node2 建 topic，三台 `/admin/topics` 立即一致。
3. **写转发走真实网络**：从每台各发一条，非组 leader 的两台返回 `"forwarded": true`。
4. **杀 leader**：`kill -9` node1 后，g0 leader 1→2（term 3）、g3 leader 1→2（term 4），
   剩余两节点继续可写、可建新 topic。
5. **重启追平**：node1 拉起后三台 topic 列表一致，且重新赢回 g0/g3 的 leadership；
   `applied == commit`。这一步顺带暴露了发现 2。

---

## 取证位置

- `47.80.243.155:/root/race.log`（单测 -race）、`/root/e2e-race.log`（e2e + race broker）
- `47.80.240.57:/root/sqbench/bench-{quorummem,quorumfsync}-x3.log`、`bench-colocated.log`
- `47.80.243.95:/root/sdk/{plain,auth}/broker.log`、`/root/sdk/py_smoke.py`、`/root/sdk/cs/Program.cs`
- `47.80.240.57:/root/rmq-cpp/`（C++ 客户端源码与构建产物）、`/root/pbshim/`（protobuf CMake 垫片）

## 仍未覆盖

- **e2e 全量在 race broker 下的干净通过**：要先修掉发现 1，否则 exit 66 会继续把
  10 条场景用例判失败。修完值得再跑一轮，那才是「集群档零 race」的完整证据。
- **多语言 SDK 的深水区**：本轮只做最小往返 + 鉴权。事务、FIFO、push 消费、
  SQL92 过滤在 Python/C#/C++ 上都没验。
- **Java SDK**：B13.3 的范围就没含它，本轮同样没做。
