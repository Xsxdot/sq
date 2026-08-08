# 消费吞吐优化设计：deliver 拆分提交 + 批量 ack + delay/txn 收口

日期：2026-08-08
前置：`2026-08-08-produce-throughput-optimization-design.md`（已完成验收）

## 1. 背景与问题

produce 写吞吐优化落地后（group commit 解锁，云服务器 soak 稳态 ~22,000 msg/s），
消费路径成为全系统最短的板——它带着与 produce 改锁前完全相同的病：

- **锁跨 fsync**。deliver 的每 (group,topic,queue) 队列锁横跨 `store.Apply`（含
  fsync）：`receiveOnce`（取件整批一次 Apply，部分摊薄）、`Ack`（**每条消息一次
  fsync，最热**）、`ChangeInvisible`（偶发）。每队列 ack 上限 ≈ 单流 fsync 速率
  （云盘 456/s），16 队列全开约 7,300 msg/s 消费天花板——**生产是消费的 3 倍，
  持续写入下堆积是数学必然**。
- **协议批量入口被逐条浪费**。`AckMessage` 请求是 `repeated entries`，但 rpc 层
  循环逐条调 `dl.Ack`——N 条 N 次抢锁 N 次 fsync。与 SendMessage 批量入口
  当初的处境相同。
- **低流量角落同病**。`AppendDelay` 全局 `delayMu` 跨 fsync（所有延时消息全局
  串行 456/s）；txn `Manager` 的 `End`/`checkOne`/`dropLocked` 持全局 `t.mu`
  跨 fsync（提交路径还在锁内调 `AppendWith` 等 fsync），全部事务决断串行。
- **消费侧零基准**。deliver 只有正确性测试，没有任何吞吐基准——改前必须先立基线。

修复模式已在 produce 侧生产验证：锁内 `ApplyAsync`（定序 + memtable 发布）、
锁外 `Pending.Wait`（等 fsync），同锁多条在途提交由 Pebble commit pipeline
合并为一次 fsync。store 层零改动。

## 2. 目标与验收

验收环境与 produce spec 相同：云服务器 root@47.80.240.57（2 vCPU Xeon、
裸 fsync 456 次/s、免密登录），本地交叉编译 `GOOS=linux GOARCH=amd64
go test -c` 后 scp（不在远端装工具链）。

| 指标 | 基线（预期） | 目标 | 对应基准 |
|---|---|---|---|
| 同队列 64 并发逐条 ack | ~456/s（锁内串行 fsync） | ≥ 2,000/s | `BenchmarkAckParallel` |
| 批量 ack 32 条/批 | ~456/s（逐条等价） | ≥ 5,000/s | `BenchmarkAckBatch32` × 32 |
| 端到端 soak 10 分钟（64 写并发 + 消费循环） | 未验证 | 消费速率 ≥ 生产速率 80%，堆积（写入总数 − ack 总数）不单调增长 | `TestSoakE2E` |

基线数字在 Task 1 动手前先在云服务器实测回填（消费侧此前无基准，"~456"
是由锁结构推出的预期值，验收表以实测为准）。

### 非目标

- **ack 异步化**（响应不等 fsync、后台批量刷盘，RocketMQ ASYNC 形态）——语义
  变化（崩溃后已 ack 消息可能重投），属 durability 分档独立 spec。
- `ForwardToDLQ`/`ResetCursor`/`moveToDLQ`（含其持队列锁调 `AppendWith` 等
  fsync 的行为）——罕见路径，收益测不出。
- delay `Scheduler.Pass` 到期搬运的批量化——单后台 goroutine 逐条 `AppendWith`，
  456/s 的搬运速率对延时消息流量足够。
- 存储格式 JSON→二进制——独立 spec。
- 消费侧协议行为变化——per-entry 状态语义、`ackAggregateStatus` 聚合规则、
  attempt 校验规则全部保持不变。

## 3. 设计

### 3.1 语义红线（全部改动必须同时满足）

1. **fsync 完成前绝不响应**：取件不交件、ack 不返回、延时/事务操作不确认。
2. **读-改-写互斥不变**：inflight/cursor 的 Get→校验→写仍在 (group,topic,queue)
   队列锁内完成；`ApplyAsync` 返回即已发布到 memtable，解锁后的下一个拿锁者
   读到的状态与提交顺序一致。
3. **原子批不变**：一次取件的「重投改写 + 新 inflight + cursor 推进」、一次
   批量 ack 的全部 Delete、延时暂存的「条目 + seq 计数器」各自仍是单一
   Pebble Batch。
4. **per-entry 语义不变**：批量 ack 中陈旧句柄/缺失记录仍逐条独立落空
   （(false,nil) 形态），只有存储故障才整组报错——与 SendBatch 先例一致。

### 3.2 deliver 拆分提交（receiveOnce / Ack / ChangeInvisible）

三处的 `st.Apply(b)` 改为：

```
锁内: ... 组 Batch → pending := st.ApplyAsync(b) → 更新内存态（无） → 解锁
锁外: pending.Wait() → 成功后才 交件/返回 ok
```

- `receiveOnce`：`Wait` 成功后才把 `out` 返回给消费者（inflight 与 cursor 先
  持久化再交件，语义与现状一致）。`Wait` 失败按现有错误路径上抛，消费者拿不到
  消息，重投兜底照常。
- `Ack`/`ChangeInvisible`：`Wait` 成功后才返回 `(true, nil)`。
- 锁内不再有 fsync 等待后，同队列并发 ack/取件的在途提交在 Pebble commit
  pipeline 合并——机制与收益与 produce 的 3f4ac12 完全同款。

为什么解锁安全（与 produce 同一论证）：`ApplyAsync` 返回即本批已在 WAL/
memtable 定序，队列锁保护的 RMW 一致性由「下一个拿锁者读 memtable」保证；
若 `Wait` 失败，WAL sync 已坏、Pebble 进入不可恢复错误态，后续所有写入都会
失败，进程只能重启，重启后状态由「同批原子提交」保证一致。

### 3.3 AckBatch：同队列多条 ack 合成一次 fsync

deliver 新增：

```go
type AckEntry struct { Offset uint64; Attempt int32 }
type AckResult struct { Offset uint64; OK bool }   // OK=false 即落空（幂等，非错误）

// AckBatch 单一 (group,topic,queue) 的批量确认：一把队列锁、逐条校验、
// 有效条目的 Delete 合成单 Batch、一次 fsync。error 仅存储故障（整组失败）。
func (d *Deliverer) AckBatch(group, topic string, queueID uint32, entries []AckEntry) ([]AckResult, error)
```

- 锁内逐条 `Get`→解码→attempt 校验：落空条目标记 `OK=false`，不进 Batch；
  有效条目 `b.Delete(inflightKey)`。全部落空时无写入，直接返回（不 Apply，
  Batch Close 回收——NewBatch 契约路径 2）。
- 有效条目 ≥1 时 `ApplyAsync` + 锁外 `Wait`，成功后有效条目全部 `OK=true`。
- 现有 `Ack` 改为 `AckBatch` 单条的薄封装（签名与语义不变，调用方零改动）。

### 3.4 rpc AckMessage 按队列分组

`AckMessage` 处理流程改为两遍：

1. 第一遍：逐条 `receiptDecode`。解码失败的 entry 当场生成失败结果（现行为）；
   成功的按 (group,topic,queueID) 分组，记录原始下标以保持响应顺序。
2. 第二遍：每组调 `dl.AckBatch`；组内 per-entry 结果映射回
   `AckMessageResultEntry`（`OK=false` → `INVALID_RECEIPT_HANDLE "receipt 已失效"`，
   存储故障 → 该组全部 `INTERNAL_SERVER_ERROR`）。响应 entries 按请求原序排列，
   `ackAggregateStatus` 聚合规则不变。

SDK 逐条 ack（单 entry 请求）退化为单条 AckBatch，行为与现状一致；SDK 批量
ack 的同队列 entries 立即享受单次 fsync。

### 3.5 AppendDelay 拆分提交

与 produce offset 计数器完全同构：`delayMu` 内 `ApplyAsync` → 推进
`delayNext`/`delayLoaded` → 解锁；锁外 `Wait`，成功后才返回。提前推进 seq 的
安全论证与 produce 的 qs.next 相同（WAL sync 失败即 Pebble 死透，重启后
计数器与条目由同批原子保证一致）。

### 3.6 txn 锁按 txID 分片

`t.mu` 保护的读-改-写不变式（half/halfidx 两键的一致操作）是 **per-txID** 的，
全局锁过宽。改为 32 片分片锁：`t.mus[fnv32(txID)%32]`。

- `End`/`checkOne`/`dropLocked` 改持对应分片锁；同一 txID 的并发决断仍严格
  串行（同片），不同 txID 并行——各自锁内的 fsync（含提交路径 `AppendWith`
  内部的等待）在 Pebble 管道合并。
- 不动 `AppendWith` API，不引入拆分提交——这镜像的是 produce 第一阶段
  「锁按队列拆分」（722305c），对事务流量足够。
- `Stage` 本就不持锁（txID 唯一、无共享计数器），不动。

### 3.7 基准与端到端 soak

`internal/core/deliver/bench_test.go`（新建）：

- `BenchmarkAckParallel`：fixture 预写 N 条消息 + inflight 记录（直接组 Batch
  写 `MsgKey`/`InflightKey`，attempt=1），`RunParallel` 逐条 `Ack`。
  `-test.cpu 64` 即 64 并发同队列。
- `BenchmarkAckBatch32`：同 fixture，每次迭代 `AckBatch` 32 条（ns/op 为每批，
  换算 msg/s 乘 32）。

`internal/core/deliver/soak_test.go`（新建）`TestSoakE2E`：

- `SQ_SOAK=1` 门控、`SQ_SOAK_DURATION`（默认 10m）、`SQ_SOAK_DIR`，与
  produce soak 同款约定；Makefile 增 `soak-e2e` target。
- 拓扑：16 队列 topic；64 个 producer worker 持续 `Append`；16 个 consumer
  worker（每队列一个）循环 `Receive(32 条, invisible 5m)` → 逐条 `Ack`。
- 每 10s 打点：`produce_rate`、`ack_rate`、`backlog`（produced − acked）。
- 判定（人工读打点，理由同 produce soak 不做自动断言）：ack_rate 均值 ≥
  produce_rate 均值的 80%；backlog 曲线不单调增长（允许波动）。

## 4. 测试策略

- **现有 deliver 全套测试是拆锁的回归网**：顺序消息不变式（每队列至多 1 条
  Ordered inflight）、过期重投、attempt 陈旧句柄幂等、孤儿清理——改后必须
  全绿且 `-race` 干净。
- 新增：
  - AckBatch 混合批正确性：有效 + 陈旧 attempt + 不存在的 offset 混在一批，
    验证 per-entry 结果、只有有效条目被删、一次提交。
  - 并发竞态：同队列并发 `Receive`+`Ack`（-race），验证 cursor/inflight 无
    交错损坏。
  - `AppendDelay` 并发写 seq 唯一性（重启后计数器一致，镜像现有
    `TestAppendDelaySeqPersistsAcrossReopen` 的并发版）。
  - txn 分片后回归：现有 txn 全套（同 txID 决断互斥由同片锁保证）。
  - rpc：`AckMessage` 多队列混合批的分组、响应顺序、聚合状态；单 entry 与
    现行为逐字节一致。
- 每 task 结束：`go test ./internal/...` 全绿 + `gofmt -l internal/` 无输出。

## 5. 实施顺序

1. deliver 拆分提交（receiveOnce/Ack/ChangeInvisible）——先立 ack 基线基准，
   改后同基准复测。
2. AckBatch + `Ack` 薄封装化 + rpc 分组。
3. AppendDelay 拆分提交 + txn 锁分片。
4. 基准入库 + TestSoakE2E + Makefile。
5. 本地全量回归（`make test && make e2e`）→ 云服务器验收（交叉编译 scp，
   跑 §2 全表 + soak）→ 数据回填本 spec 附录 → 清理远端。

每步独立成 commit、独立可验证。

## 附录：验收数据（2026-08-08，root@47.80.240.57）

### 环境说明

- 云服务器：root@47.80.240.57，2 vCPU Xeon（Intel(R) Xeon(R) Platinum），裸 fsync 456 次/s（Task 1 实测）。
- 测试方式：本机（macOS）`GOOS=linux GOARCH=amd64 go test -c` 交叉编译 4 个测试二进制，`scp -C` 压缩上传至 `/root/sqbench`，SSH 远端执行。普通模式（交叉编译产物不支持 -race）。
- 测试二进制：`deliver.test` / `produce.test` / `txn.test` / `rpc.test`，来自 HEAD a9a25fb（Task 1-6 完成后）。

### Step 0 本地全量回归

- `make test`：全包 PASS（admin/config/core/core/deliver/delay/meta/produce/query/retention/rpc/store/sysinfo/txn 等全部 `ok`）。
- `make e2e`：`ok github.com/xushixin/sq/test/e2e`（官方 SDK 收发/ack 真实链路）。注：首次运行因 worktree 内 `web/dist` 无构建产物（gitignore，需 `make web`），`TestConsoleServedFromBinary` 返回 503；复制主 checkout 已构建产物后全绿，与代码无关。

### Step 2 远端正确性

```bash
cd /root/sqbench && ./deliver.test -test.run Test -test.count=1 && ./produce.test -test.run Test -test.count=1 && ./txn.test -test.run Test -test.count=1 && ./rpc.test -test.run Test -test.count=1
```

结果：4 个测试二进制全部 `PASS`（exit 0，0 FAIL）。soak 类测试因未设 `SQ_SOAK` 自动 Skip。

### Step 3 验收基准（原始输出）

```bash
cd /root/sqbench && ./deliver.test -test.run "^$" -test.bench "BenchmarkAck" -test.benchtime 3s -test.cpu 64
```

```
goos: linux
goarch: amd64
pkg: github.com/xushixin/sq/internal/core/deliver
cpu: Intel(R) Xeon(R) Platinum
BenchmarkAckParallel-64    47611    79013 ns/op
BenchmarkAckBatch32-64     10000    911733 ns/op
PASS
```

换算表（与 Task 1 基线对比）：

| 基准 | Task 1 基线（优化前） | 本次（优化后） | 吞吐换算 | 放大倍数 |
|------|----------------------|----------------|----------|----------|
| BenchmarkAckParallel | 2,346,455 ns/op（~426 ack/s） | 79,013 ns/op | 1e9/79013 ≈ 12,656 ack/s | ~29.7x |
| BenchmarkAckBatch32 | — | 911,733 ns/op | 1e9/911733×32 ≈ 35,098 msg/s | —（新建） |

- AckParallel 验收标准：≥2,000 ack/s（ns/op ≤ 500,000）→ **达标**（12,656 ack/s）。
- AckBatch32 验收标准：换算 ≥5,000 msg/s → **达标**（35,098 msg/s）。
- 12,656 ack/s 远超裸 fsync 456 次/s，证明 group commit（多 ack 合成一次 fsync）在并发下实际生效。

### Step 4 远端端到端 soak 10 分钟（打点序列）

```bash
cd /root/sqbench && mkdir -p soak-e2e-data && SQ_SOAK=1 SQ_SOAK_DURATION=10m SQ_SOAK_DIR=/root/sqbench/soak-e2e-data ./deliver.test -test.run TestSoakE2E -test.v -test.timeout 30m
```

结果：`--- PASS: TestSoakE2E (600.03s)`；produced=5,804,835 acked=5,804,770 backlog=65；avg_produce_per_s=9674 avg_ack_per_s=9674。

完整打点（elapsed_s produce_rate ack_rate backlog）：

```
10   16961 16949 117 | 20 16113 16112 130 | 30 13919 13920 120 | 40 13533 13531 140 | 50 12287 12291 96  | 60 11154 11153 109
70   10579 10578 118 | 80 10161 10159 140 | 90 11626 11627 133 | 100 11795 11794 139 | 110 11736 11735 151 | 120 11270 11273 127
130  11492 11492 126 | 140 10975 10973 144 | 150 10260 10264 95 | 160 10518 10515 125 | 170 10058 10059 111 | 180 9559 9555 146
190  9090  9091  130 | 200 9417 9416 142 | 210 9887 9888 130 | 220 8668 8669 116 | 230 8655 8654 125 | 240 8490 8490 128
250  9029  9027  150 | 260 8835 8837 136 | 270 8610 8612 120 | 280 7887 7885 142 | 290 8609 8610 126 | 300 8627 8626 131
310  8445  8444  143 | 320 8542 8545 116 | 330 9369 9369 115 | 340 8888 8887 125 | 350 8862 8861 136 | 360 9008 9008 130
370  8862  8866  94  | 380 8765 8761 141 | 390 8769 8769 135 | 400 9664 9665 124 | 410 9467 9465 149 | 420 8302 8307 100
430  7690  7688  126 | 440 9445 9445 127 | 450 8700 8699 140 | 460 8091 8093 117 | 470 8730 8726 154 | 480 8314 8317 125
490  8420  8421  109 | 500 8690 8689 115 | 510 8444 8441 148 | 520 8491 8494 118 | 530 8990 8990 124 | 540 8277 8277 126
550  8581  8581  122 | 560 7895 7893 139 | 570 9187 9189 124 | 580 8639 8640 121 | 590 8737 8738 116 | 600 8385 8384 121
```

判定表：

| 验收标准 | 实测 | 结论 |
|----------|------|------|
| ack_rate 均值 ≥ produce_rate 均值 80% | 9674.0 / 9674.1 = 100.0%（60 个打点 ack 全程不落后于 produce，含瞬时 100%+） | 达标 |
| backlog 有波动但不单调增长 | 范围 94~154，前/中/后 10 点均值 124/129/126，围绕 ~125 波动 | 达标 |
| 无 >5s 的 0 打点 | 打点间隔恒定 10s，全部 60 点 rate 非零（最低 ack 7688/s） | 达标 |

### 结论

全部验收标准达标，无未达标项：

- AckParallel ~12,656 ack/s（基线 ~426 ack/s，放大 ~29.7x），远超 ≥2,000 ack/s 目标。
- AckBatch32 ~35,098 msg/s，远超 ≥5,000 msg/s 目标。
- 10 分钟 e2e soak 全程 ack 与 produce 同步（100.0%），backlog 稳定波动，无停滞。
