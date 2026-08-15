# 集成基线 Linux 真机验证（含 B14/B15 修复）

> 2026-08-15。目标：补上 `2026-08-14-four-gaps-verification.md` 里列在「仍未覆盖」
> 首位的那一项——**e2e 全量在 race broker 下的干净通过**——并在真实跨物理机
> 部署里核对 B15 分级日志。三台阿里云临时机重开后进行。

## 结论

| 项 | 结果 |
|---|---|
| 集成合并 | 12 条特性分支 + B14/B15 修复，**仅 1 处机械冲突**，无语义冲突 |
| Linux `-race` 全量单测 | **18/18 包 ok，零 DATA RACE**，rc=0 |
| Linux race broker 跑 e2e | **48 PASS / 0 FAIL / 2 SKIP**，零 DATA RACE、零 `exit status 66` |
| 跨物理机三节点集群 | 选主 / meta 复制 / 写转发 / 切主 / 重启追平 全部通过，三机零 panic 零 race |
| B15 分级日志（真实部署） | 同一场景由 4 条 **ERROR → 4 条 WARN**，node1 全程零 ERROR |

## 机器

| 公网 | 内网 | 角色 |
|---|---|---|
| 47.80.243.95 | 172.19.25.180 | 构建机；`-race` 单测与 e2e；集群 node1；亦作另两台的 ssh 跳板 |
| 47.80.243.155 | 172.19.25.178 | 集群 node2 |
| （公网不可达） | 172.19.25.179 | 集群 node3 |

均 Ubuntu 24.04.4 / 2 核 / 1.6 GB + 4 GB swap / Go 1.26.1（官方 tarball，apt 的 1.22 低于
`go.mod` 要求的 1.26.1）。

**第三台的公网 IP 变了**：上一轮记的 `47.80.240.57` 已不通，内网扫描确认同批次主机
（主机名 `iZmj7d0gx1qc1mopo88qh{q,r,s}Z`）的第三台是 `172.19.25.179`，经 `.95` 跳板可达。
同网段的 `172.19.25.177`（409 MB）不属于这批，未触碰。

三台的 broker 二进制各自本机编译，md5 **完全一致** `f7f3865dff32…`——顺带确认了可重现构建。

## 一、集成基线

`verify/final-integration-2` = `verify/final-integration` + main（7 个 docs 提交）+
`fix/b14-b15-observer-and-loglevel`。相对 main 领先 57 个提交 / 67 文件
（+6317 / −345）。其余 11 条特性分支此前已在 `verify/final-integration` 内。

**唯一冲突**在 `internal/metrics/collector.go`：SQL92 分支给 `NewRegistry` 加了
`fs FilterStats` 参数，B14 修复重写了同一段注释与签名——纯文本层面撞在一起，
取「修复的注释 + 集成的签名」。

**没撞的地方更要紧**：`internal/cluster/group_test.go` 里原本直接读写裸全局
`store.OnApplyObserve` 的那段，被修复迁到了 `SwapApplyObserver`，与集成分支上
seglog、apply 合批那批改动能共存，`go vet`（含测试文件编译）全过。工作树里
`OnApplyObserve` 只剩 4 处注释、**零代码引用**——没有留兼容别名。

## 二、Linux `-race` 全量单测

```
go test -race -timeout 40m -p 1 ./...
→ 18/18 包 ok，WARNING: DATA RACE 计数 0，FAIL 行 0，rc=0
```

`-p 1` 限包级并行是必要的：2 核 / 1.6 GB 上 race 插桩并行编译加跑测极易 OOM，
而 OOM 杀出来的失败会伪装成测试失败——比慢更糟。

## 三、Linux race broker 跑 e2e（本轮的承重项）

```
go build -race -o /root/sq.race ./cmd/sq
cd test/e2e && SQ_E2E_BROKER=/root/sq.race go test -tags e2e -timeout 60m -count=1 -v ./...
→ 50 RUN：48 PASS / 0 FAIL / 2 SKIP，耗时 1770s，rc=0
→ WARNING: DATA RACE 计数 0；exit status 66 计数 0
→ 11 条 TestScenario* 全绿
```

2 条 SKIP 是 `TestClusterWriteThroughput` / `TestExternalClusterWriteThroughput`
（需 `SQ_BENCH=1`），不是失败。

**这一项此前是空的**。08-14 那轮因 B14 竞态导致 race broker 以 exit 66 退出，
连带判失败 10 条集群/场景用例，所以「集群档零 race」始终没有完整证据。现在有了。

`TestOfficialGoSDKDelayRestartRecovery` 在 Linux 上直接绿（14.21s）——支持它在
macOS 那次 6.03s 超停机预算是负载离群值，不是缺陷。

### 一个自己踩的坑（记下来免得重犯）

首次跑 e2e 秒退、`rc=1`、零用例，日志只有 `matched no packages`。原因是 e2e 全套由
`//go:build e2e` 门控，**必须带 `-tags e2e`**。这个失败长得很像「跑了但全挂」，
实则根本没跑。

## 四、跨物理机三节点集群

`data_groups: 3`（meta 组 + 3 数据组），`ack` 缺省 `quorum-mem`，peers 用内网 IP。

1. **跨机选主**：4 个组的 leader 自然分散到 3 台（g0→node1、g1→node2、g2→node3、
   g3→node1），所有 peer `StateReplicate` / `recent_active: true`。
2. **meta 跨机复制**：在 node2 建 `xmachine`，三台 `/admin/topics` 立即一致。
3. **写转发走真实网络**：从每台各发一条；node1/node2 返回 `"forwarded": true`，
   node3 恰为该队列所属组的 leader，直写不转发——符合预期。
4. **杀 leader**：`kill -9` node1 后，g0 leader 1→3（term 4）、g3 leader 1→2（term 4），
   node1 在存活节点视角转 `StateProbe` / `recent_active: false`；剩余两节点仍可写、
   可建新 topic。
5. **重启追平**：node1 拉起后三台 topic 列表一致，重新赢回 g0/g3 的 leadership，
   各组 `applied == commit`。
6. 三台全程 `panic` 0 次、`WARNING: DATA RACE` 0 次。

### B15 在真实部署路径下的前后对比

这是本轮唯一能拿真实部署做「修复前 vs 修复后」直接对比的一条。场景完全相同
（跨机 `kill -9` node1 再拉起，每组一条共 4 条）：

**修复前**（08-14 记录）：

```
level=ERROR msg="不干净关机后本地恢复：任期已抬、投票已清" mod=raftstore g=0 term=3
```

**修复后**（本轮）：

```
level=WARN  msg="mem 档本地恢复：任期已抬、投票已清（投票记录走 NoSync 可能未落盘，
                 抬任期是预期动作，代价是多一轮选举）" mod=raftstore g=0 term=4 legacy=false
```

4 条全部 WARN，**node1 整个进程生命周期内 ERROR 计数为 0**（修复前此处必有 4 条）。

且它不是碰巧走到 WARN：恢复路径判定日志把全部判定输入打了出来——

```
recovery=local-resume  mode=ack-quorum-mem  clean=false  hasRaft=true
bootgenNow=416e4782… bootgenStored=416e4782…（相等）  permit=false
```

——正是 `needsTermBump(pathLocalResume, AckQuorumMem)` 为真、设计上该走 WARN 的那条路。
签字放行的 `ForceLocalRecover`（ERROR 档）本轮未被触发，符合预期。

## 五、两处属于我自己的操作缺陷（非产品问题）

1. **集群配置漏了 `advertise_host` / `advertise_port`**，三节点启动即失败。
   `sq.example.yaml` 的示例其实是完整的，是我按关键字 grep 时漏看了那两行。
   顺带一提，报错 `配置 cluster.peers[0] 的 advertise_host 不能为空` 精确到下标与
   字段名——这是 B12 可诊断性改造的实际收益。
2. **启动脚本把 `&` 作用到了 `cd X && nohup …` 整个复合命令上**，子 shell 在前台
   等 broker 并一直攥着 ssh 的 stdout，导致每台 ssh 阻塞到该节点 60s 启动超时才返回。
   三台因此相隔 63s 依次启动、从未同时在线，凑不出多数派。**症状极具误导性**：
   日志里只有「meta 组在 1m0s 内未选出 leader」，看着像跨机网络不通，实测 TCP 29081
   却是通的。修法见脚本注释：用 `sh -c 'echo $$ > pidfile; exec broker'` 拿准确 pid，
   三台并行启动。

## 取证位置

- `47.80.243.95:/root/verify/{unit-race.log,e2e-race.log,summary.txt,summary2.txt}`
- 三节点日志已取回本地 scratchpad `cluster-out/node-172.19.25.{180,178,179}.log`
- 驱动脚本：scratchpad `remote-race.sh` / `e2e-race.sh` / `cluster-verify.sh` / `mkcluster.sh`

集群三节点在验证结束时仍在运行（`http://<内网 IP>:28082/admin/cluster`），未清理。

## 仍未覆盖

- **多语言 SDK 的深水区**：事务、FIFO、push 消费、SQL92 过滤在 Python/C#/C++ 上都没验；
  本轮未重跑 08-14 已做过的最小往返冒烟。
- **Java SDK**：B13.3 范围本就未含。
- **吞吐基准**：本批机器规格与 08-14 那批不同（2C/1.6G），跨批次数字不可比，未跑。
