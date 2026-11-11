# SQ4.5 计划：pebble 参数可配置并调优默认值（L2）

> 状态：**已拍板（2026-11-11，主会话）**——岔口 1–6 按推荐；岔口 4 授权执行者按基准在 memtable 32MiB 与 64MiB 间取缺省值（用户可见，交回物须给对比数据与理由，主会话复核后告知用户）
> 需求：`docs/superpowers/specs/2026-11-11-sq4-perf-batch-design.md`「SQ4.5」节 + 批次共同红线（不改盘上与线上格式、不降低持久化安全承诺）
> 基线：spec 读数为 main@21ee680；本计划读数复核于集成分支 `cards/sq4`@3374f4a（已含 SQ4.6/4.7/4.11：`config.go`、`sq.example.yaml`、`docs/configuration.md`、`cmd/sq/main.go`、`produce/bench_test.go` 有改动，本计划引用的行号已在 3374f4a 上复核）；pebble v2.1.6，源码读自模块缓存 `$(go env GOMODCACHE)/github.com/cockroachdb/pebble/v2@v2.1.6`
> 执行形态：本会话 subagent 实现，独立 subagent 审，主会话验收

## 待拍板岔口

1. **store 打开接口形态**
   - 推荐：保留 `store.Open(dir, syncWrites, logger)` 不变（内部等价于零值选项），新增 `store.OpenWithOptions(dir, syncWrites, logger, store.Options)`；`store.Options` 只含三项：`CacheSize int64`、`MemTableSize uint64`、`CompactionConcurrency int`，**零值 = 本包缺省值**。生产调用方 3 处改用新函数（见 §2），55 处测试调用（含 `test/e2e` 独立 module）不动。
   - 被否：改 `Open` 签名——55 处测试调用点跨两个 module 机械改动，零收益；可变参数 `...Option`——只有一个选项结构体，函数式选项是过度设计。
2. **集群重入（Rejoin）如何拿到调优参数**
   - 推荐：`cluster.Options` 新增字段 `StoreOptions store.Options`，`Rejoin` 第 5 步（`manager.go:2274`）改用 `store.OpenWithOptions(dataDir, false, lg, o.StoreOptions)`；main 组装 `opts` 时填入。改动在 `manager.go` 只有「Options 结构体加一个字段 + 一行调用」，与 SQ2 不同行。
   - 被否 a：从旧 store 实例读出参数——main 在调 Rejoin 前已 `opts.Store = nil`（`cmd/sq/main.go:319`），取不到。
   - 被否 b：注入打开函数 `OpenStore func(dir string) (*store.Store, error)`——比一个数据字段更抽象，没有第二个实现。
   - 不改则后果：节点重入后 store 回到 pebble 出厂参数（8MB 缓存），性能静默退回，且无日志可见。
3. **缺省值归属**
   - 推荐：缺省值常量定义在 `store` 包（导出常量，`store.Options` 零值字段解析为它们）；config 对未填写的键保持空串（不写默认值），只解析和校验用户显式填写的值；文档与示例配置写明同一组数字。理由：所有走 `store.Open` 的测试与基准自动用上调优值（基准量到的就是生产默认），且全仓只有一处真值。
   - 被否：缺省值放 config——config 不能 import store（会把 pebble 拉进 config 包），store 零值只能退回 pebble 出厂值，形成「生产一套、测试一套」两份真相。
4. **缺省值（用户可见）**：spec 示例给 `cache_size: 256MiB`、`memtable_size: 64MiB`。读源码发现 **memtable 的内存从 block cache 里预留**（`db.go:2557` `d.opts.Cache.Reserve(int(size))`），活动 + 待刷 + 回收复用最多约 3 个 memtable：64MiB × 3 = 192MiB 会把 256MiB 缓存里留给数据块的份额压到约 64MiB——与「提高缓存命中」的目标直接冲突。
   - 推荐：基准矩阵同时测 `memtable_size` 64MiB 与 32MiB（缓存都是 256MiB），若 32MiB 的 ack 路径 pread 占比更低且 produce 吞吐不差于 64MiB 的 95%，则缺省改为 32MiB 并**回主会话确认**（改用户可见缺省 = 偏离 spec 示例）；否则按 spec 取 64MiB。
   - 被否：不测直接照抄 spec 示例。
5. **`compaction_concurrency` 映射到 pebble 的方式**：pebble v2 是区间 `CompactionConcurrencyRange func() (lower, upper int)`，lower = 常态并发，upper = L0 读放大 / 压实欠账过高时可升到的上限（`options.go:956` 注释）。
   - 推荐：映射为 `(1, n)`，n = 配置值（0 → `min(4, runtime.NumCPU())`）；文档称其为「压实并发上限」。常态单路压实不抢 2 核小机的 CPU，积压时自动升到 n。
   - 被否：`(n, n)`——常态就 n 路并发，小机上与写入路径抢 CPU；只有基准证明 `(n,n)` 显著更好时才回主会话改。
6. **大小写法与校验范围（用户可见）**
   - 推荐：值为「非负整数 + 可选单位」，单位只认 IEC 二进制：`B`、`KiB`、`MiB`、`GiB`（区分大小写），不带单位 = 字节；拒绝 `KB/MB/GB`（报错提示改用 KiB/MiB/GiB，避免 1000/1024 歧义）、拒绝小数、负数、空白夹杂。范围：`cache_size` ∈ [8MiB, 64GiB]；`memtable_size` ∈ [1MiB, 1GiB]（pebble 硬上限 < 4GiB，`options.go:2386`）；`compaction_concurrency` ∈ [0, 64]。解析函数放 config 包内私有（SQ4.10 的 `log_retain_bytes: 1GiB` 复用同一个，谁先落地谁建）。
   - 被否：同时接受 SI 与 IEC（docker 风格）——`256MB` 到底是 256×10⁶ 还是 256×2²⁰ 用户和代码都会猜错；仓库现有大小配置是裸整数字节（`snapshot_chunk_bytes int`，`config.go:141`），但 spec 已定用带单位写法，故新增解析，不改旧键。

## 1. 交付目标

| 故事 | 做到哪里 |
|---|---|
| S1 deliver 与 produce 包基准（fsync=async 变体）对比基线：ack 路径 pread 占比显著下降 | 新增 deliver 的 async 基准变体（produce 的 `BenchmarkAppendParallelAsync` 已由 SQ4.6 入库，复用）；改前/改后 CPU 剖析中 `syscall.pread` 累计占比与 ns/op 进卡 |
| S2 配置缺省、覆盖、非法值启动即拒 | `storage` 段解析与校验 + config 包表驱动测试 |
| S3 三节点 e2e 写 + 消费不回退 | e2e 功能用例全绿 + 写吞吐基准改前/改后数字进卡 |

附带交付：`docs/configuration.md` 新增 `storage` 节，含内存占用估算（公式 + linux-01 实测峰值）；`sq.example.yaml` 加注释示例。

不交付：按机器内存自动推算缓存（spec Out of Scope，落 roadmap）；GOMEMLIMIT 自动设置；任何 `FormatMajorVersion` 变化。

## 2. 改动边界

| 文件 | 改什么 |
|---|---|
| `internal/store/store.go` | 新增 `Options` 类型、缺省值常量、`OpenWithOptions`；`Open` 改为调用它（零值选项）；组装 `pebble.Options`：`CacheSize`、`MemTableSize`、`CompactionConcurrencyRange`（岔口 5）、`Levels[0].FilterPolicy = bloom.FilterPolicy(10)`（经 `EnsureL1PlusDefaults` 继承到全部层）、L0 阈值（若基准选定，见 §3）；「store 已打开」日志补上生效的 cache/memtable/压实并发/L0 阈值字段 |
| `internal/store/store_test.go`（或同包新文件） | 零值选项解析为缺省常量、显式值透传、日志字段 |
| `internal/config/config.go` | 新增顶层 `Storage StorageConfig yaml:"storage"`（三个键：`cache_size`、`memtable_size` 为字符串，`compaction_concurrency` 为整数）、私有字节解析函数、`Load` 内校验、取值方法（返回字节数，空 = 0 = 用 store 缺省） |
| `internal/config/config_test.go` | S2 表驱动用例 |
| `cmd/sq/main.go` | 装配点：由 `cfg.Storage` 取值方法构造 `store.Options`，`store.Open` → `store.OpenWithOptions`；集群分支 `cluster.Options` 填 `StoreOptions`（岔口 2） |
| `cmd/sq/recover.go` | 同上改用 `OpenWithOptions`（离线恢复也按用户参数打开，避免与运行期参数不一致） |
| `internal/cluster/manager.go` | `Options` 加 `StoreOptions store.Options` 字段 + `Rejoin` 第 5 步一行调用（岔口 2），别处不动 |
| `internal/core/deliver/bench_test.go` | 新增 `BenchmarkAckParallelAsync`：`newBenchDeliverer` 加 `syncWrites bool` 参数（照 SQ4.6 对 `newBenchProducer` 的同款改法），现有基准传 `true`，行为与数字口径不变。`internal/core/produce/bench_test.go` 不改（`BenchmarkAppendParallelAsync` 已在 `bench_test.go:52`） |
| `sq.example.yaml`、`docs/configuration.md` | 新 `storage` 段（注释形式示例，不改变示例文件的生效值） |

**`store.Open` 全部调用方**（签名不变，故无需改；仅 3 处生产调用改用新函数）：生产 `cmd/sq/main.go:149`、`cmd/sq/recover.go:57`、`internal/cluster/manager.go:2274`；测试 55 处（`grep -rn "store\.Open(" --include='*_test.go' . | wc -l`），分布见「读数与证据」。

**真实入口**：`config.Load` → `cmd/sq/main.go` 装配 → `store.OpenWithOptions` → `pebble.Open`；集群重入 `cluster.Rejoin` → `store.OpenWithOptions`。

**架构方向**：config（纯解析，不依赖 store）← cmd/sq（装配点，做 config→store 映射）→ store（拥有 pebble 与缺省值）。cluster 依赖 store 已存在，只多一个字段。无新增跨仓/跨进程契约；无新增冻结对象。

**格式红线核对（已读源码）**：
- 布隆过滤器只是 SST 内多一个 `fullfilter.<name>` 元块；读侧按已注册的过滤器名查找（`sstable/reader.go:516-521`），未注册的名字直接忽略——旧版本二进制读新 SST、新版本读旧 SST 都正常，回滚安全。
- 不设置 `FormatMajorVersion`（保持 pebble 默认，不做格式棘轮）。
- 缓存、memtable、L0 阈值、压实并发都是运行期参数，不落盘语义（pebble 会把选项写进 OPTIONS 文件，但打开时只核对 comparer/merger 名）。
- raft 日志条目、pebble 键值、gRPC 协议均不变。

**布隆过滤器的实际作用范围**（写进代码注释与卡，避免对收益的误判）：`db.Get` 走 `getIter`，其 `IterOptions` 不设 `UseL6Filters`（`db.go:588-592`），`file_cache.go:659-661` 对 L6 一律不读过滤块——过滤器省的是 L0–L5 的无效探查，不省 L6 的索引/数据块读取。后者靠缓存容量解决。

## 3. 工作顺序

1. **先落基准变体并取基线（阻塞后续）**：只改 deliver 的 `bench_test.go`，store.go 不动。跑 S1 基线命令（§4），存档 `.txt` 与 `.pprof`。若本机剖析里 `syscall.pread` 累计占比与 spec 读数（deliver_async 65.55%）不在同一量级（<30%），停下回主会话——承重假设「ack 路径慢在缓存未命中」不成立，调参方向要重议。
2. **用 overlay 做参数探索（不提交）**：在 scratchpad 写若干份 `store.go` 变体，经 `go test -overlay <scratchpad>/sq45-overlay/<变体>.json` 跑同一基准，矩阵：
   - A：pebble 出厂（= 基线）
   - B：cache 256MiB + memtable 64MiB + 布隆 + 压实 (1, min(4,NumCPU))
   - C：B 但 memtable 32MiB（岔口 4）
   - D：在 B/C 中较优者之上，L0 阈值 (L0CompactionThreshold, L0StopWritesThreshold) = (8, 24)
   - E：同上，(6, 18)
   
   L0 取舍规则：D/E 中 produce async 吞吐较「B/C 较优者」提升 ≥ 5% 且 deliver ack 不差于其 95% 的，取提升最大者；都达不到就**不设** L0 阈值（保持 pebble 默认 4/12），卡上记数字说明。每个变体 `-count 5` 取中位数。
3. 按探索结论写正式 `store.go`（`Options`、缺省常量、`OpenWithOptions`）+ store 包不变式测试。
4. config `storage` 段 + S2 测试。
5. 装配：main / recover / cluster.Options + Rejoin。
6. 文档与示例配置；内存估算先写公式，数字待真机 R2 填入（R2 不跑完不交付文档）。
7. 改后跑 S1、S3。

## 4. 验收方式

### 故事级检查

**S1**（本机，同一会话先基线后改后；`<S>` = scratchpad 目录）：

```
go test ./internal/core/deliver/ -run '^$' -bench '^BenchmarkAckParallelAsync$' -benchtime 1000000x -count 5 -cpuprofile <S>/sq45/deliver_before.pprof -o <S>/sq45/deliver_before.test 2>&1 | tee <S>/sq45/deliver_before.txt
go tool pprof -top -cum <S>/sq45/deliver_before.test <S>/sq45/deliver_before.pprof | grep -E 'syscall.pread|\(\*Store\)\.Get'
go test ./internal/core/produce/ -run '^$' -bench '^BenchmarkAppendParallelAsync$' -benchtime 2000000x -count 5 -cpuprofile <S>/sq45/produce_before.pprof -o <S>/sq45/produce_before.test 2>&1 | tee <S>/sq45/produce_before.txt
```

改后同命令把 `before` 换成 `after`。判据：deliver 的 `syscall.pread` cum% 显著下降（数字进卡；低于基线一半视为达成，未达成须在卡上解释原因，不硬凑）；produce ns/op 不差于基线的 95%（调参不得以写路径为代价）。`-benchtime` 用固定次数，保证改前/改后预写的 inflight 数据量一致（数据量决定是否超出缓存）。

**S2**（config 包表驱动 `TestStorageConfig`）：

```
go test ./internal/config/ -run TestStorageConfig -count=1 -v
```

用例：无 `storage` 段 → 三个取值方法返回 0（= 用 store 缺省）；`cache_size: 512MiB` → 536870912；`memtable_size: 1048576`（裸字节）→ 1048576；`compaction_concurrency: 2` → 2；非法值逐条 `Load` 返回错误且错误信息含键名：`-1MiB`、`256MB`（提示 KiB/MiB/GiB）、`1.5GiB`、`abc`、`0`、`7MiB`（低于下限）、`2GiB` 的 memtable（超上限）、`compaction_concurrency: -1`、`65`。

**S3**（三节点 e2e，TestMain 自动编译 broker）：

```
cd test/e2e && go test -tags e2e -count=1 -run 'TestThreeNodeClusterE2E|TestClusterDLQ' -v -timeout 20m ./ 2>&1 | tee <S>/sq45/e2e_func.log
cd test/e2e && SQ_BENCH=1 SQ_BENCH_CONC=64 go test -tags e2e -count=1 -run 'TestClusterWriteThroughput' -v -timeout 40m ./ 2>&1 | tee <S>/sq45/e2e_bench_after.log
```

判据：功能用例全绿；写吞吐与改前（本卡分支的起点提交，用 `git worktree add <S>/sq45-base <起点提交>` 建临时 worktree 跑同命令——不用 21ee680，因 SQ4.6/4.7 已改变写路径性能）比，quorum-mem / quorum-fsync / standalone 三档 msg/s 均不低于基线 95%。

### 不变式单测（只为承重规则）

| 编号 | 规则 | 要点 |
|---|---|---|
| I1 | 零值 `store.Options` 解析为 store 缺省常量；显式值原样透传 | 同包测试调用解析函数（或捕获「store 已打开」日志的字段，参照 seglog `TestZeroTailLogsDebugNotWarn` 的日志捕获写法）——这条断言穿过 config 之外的真实组装链 |
| I2 | 布隆过滤器真的写进 SST 并在点查中生效 | store 同包测试可直接访问未导出字段 `s.db`（不新增导出）：写入一批键 → `s.db.Flush()` 落成 L0 SST → 对一批不存在的键 `Get` → 断言 `s.db.Metrics().Filter.Hits > 0`（`Metrics.Filter` 见 pebble `metrics.go:283`、`sstable/filter.go:10-18`）。反例：去掉 FilterPolicy 设置，Hits 恒为 0，本用例必须失败（review 变异项） |
| I3 | 配置值穿过装配点到达 store | main 包测试：给定 `config.Config`（含 storage 覆盖值）→ 装配函数产出的 `store.Options` 字段相等；空值 → 零值（区分「未配置」与「配置为某值」） |

### 编译与触及包测试

```
go build ./... && go vet ./...
go test ./internal/store/ ./internal/config/ ./cmd/sq/ ./internal/core/... -count=1
make test-cluster
```

### 真机清单（linux-01，登录见 `~/.claude/infra/machines.md`）

- **R1（首跑项）**：发布形态构建 `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o <S>/sq45/sq.linux ./cmd/sq`，在 linux-01 用含 `storage:` 覆盖值（如 `cache_size: 128MiB`）的配置单机启动，`grep "store 已打开"` 日志字段与配置一致；再写一个非法值（`cache_size: 256MB`）启动，进程必须拒启且错误含键名。证据：两段日志存档。
- **R2**：内存峰值实测（填进文档的数字来源）。缺省配置单机启动，跑 `make soak-e2e` 同款负载 5 分钟（`SQ_SOAK=1 SQ_SOAK_DURATION=5m SQ_SOAK_DIR=<NVMe 目录>`，或 e2e 写吞吐基准 `SQ_E2E_BROKER=<sq.linux>`，其自带 VmHWM 采样），记录 broker 进程 `VmHWM`；同负载对照 `cache_size: 128MiB` 一次。文档写「公式 + 实测」，并给 2GB 机器建议值（若实测缺省峰值 > 1.2GiB，回主会话讨论缺省值）。
- R3：linux-01 上复跑 S1 deliver 基准（cgo 关闭构建：`CGO_ENABLED=0 go test -c`），因发布版是 CGO_ENABLED=0、缓存走 Go 堆，与本机 cgo 构建的分配路径不同，数字作为补充进卡。

### 文档里的内存估算（公式由本计划定，数字由 R2 填）

- pebble 常驻上界 ≈ `max(cache_size, 3 × memtable_size)` + 文件句柄缓存等小项：memtable 内存从缓存预留（`db.go:2557`），最多约 3 个（活动 + 待刷，受 `MemTableStopWritesThreshold=2` 约束 + 1 个回收复用，`db.go:2540-2560`）；memtable 从 256KB 起倍增到设定值（`options.go:890` 注释），空载实例占用远小于上界。
- 发布构建 `CGO_ENABLED=0`（`.github/workflows/release.yml:75`）下 pebble 的缓存与 memtable 走 Go 堆（`internal/manual/manual_nocgo.go:20` `make([]byte, n)`），默认 `GOGC=100` 时堆峰值可达常驻量约 2 倍；文档建议 2GB 机器设 `GOMEMLIMIT`（具体值按 R2 实测给）。

## 5. 决策权限

执行者可自行决定：私有函数与常量命名；`store.Options` 零值解析的写法；基准变体的 fixture 重构方式（不改变现有基准口径即可）；L0 阈值按 §3 第 2 步的取舍规则落定（数字进卡）；探索矩阵在 A–E 之外多加变体。

须升级主会话：
- 任何用户可见面变化：键名、缺省值偏离 spec 示例（含岔口 4 的 memtable 32MiB）、单位写法、校验范围。
- 改 `store.Open` 签名；新增导出符号超出 `Options`、缺省常量、`OpenWithOptions` 三者。
- 设置 `FormatMajorVersion`、开启 `UseL6Filters` 一类改变读路径语义的选项、或调 §2 列表之外的 pebble 参数（如 `MaxOpenFiles`、压缩算法）。
- S1 第 1 步承重假设不成立；R2 实测峰值超出 2GB 机器安全线。

## 6. 最大风险

2GB 机器上的内存：发布版缓存与 memtable 都在 Go 堆里，GC 余量可能让 256MiB 缓存在峰值时体现为 500MiB 以上；同时 memtable 预留会吃掉缓存给数据块的份额——缺省值组合选错会两头落空（内存高、命中率没上去）。R2 实测是止损点。

## 读数与证据

```
$ git rev-parse --short HEAD
3374f4a
$ git log --oneline a8639eb..HEAD
3374f4a merge: SQ4.11 合入 SQ4 集成分支
daf12a5 merge: SQ4.7 合入 SQ4 集成分支
5b48eca merge: SQ4.6 合入 SQ4 集成分支
be749cb fix(core): 事务 txID 单独用 crypto/rand 生成
18e5ac8 feat(store): 单机 fsync=async 档补 200ms 周期 WAL 刷盘
d4e5511 feat(config): message_encoding 缺省改为 binary
6be92d6 perf(core): 消息 ID 改用运行时 ChaCha8 随机源并一次分配编码
$ grep -n "^func Benchmark" internal/core/produce/bench_test.go internal/core/deliver/bench_test.go
internal/core/deliver/bench_test.go:68:func BenchmarkAckParallel(b *testing.B) {
internal/core/deliver/bench_test.go:85:func BenchmarkAckBatch32(b *testing.B) {
internal/core/produce/bench_test.go:31:func BenchmarkAppendParallel(b *testing.B) {
internal/core/produce/bench_test.go:52:func BenchmarkAppendParallelAsync(b *testing.B) {
internal/core/produce/bench_test.go:85:func BenchmarkAppendQueueSweep(b *testing.B) {
internal/core/produce/bench_test.go:113:func BenchmarkAppendBatch32(b *testing.B) {
$ grep -n "pebble" go.mod
6:	github.com/cockroachdb/pebble/v2 v2.1.6
$ grep -n "pebble.Open(dir" internal/store/store.go
93:	db, err := pebble.Open(dir, &pebble.Options{Logger: newPebbleLogger(l)})
$ grep -rn "store\.Open(" --include='*.go' cmd internal | grep -v _test
cmd/sq/recover.go:57:	st, err := store.Open(cfg.DataDir, cfg.Fsync == "sync", logger)
cmd/sq/main.go:149:	st, err := store.Open(cfg.DataDir, cfg.Fsync == "sync", logger)
internal/cluster/manager.go:2274:	st, err := store.Open(dataDir, false, lg)
$ grep -rn "store\.Open(" --include='*_test.go' . | wc -l
      55
$ grep -rn "store\.Open(" --include='*.go' . | grep -v "^./spike" | awk -F: '{print $1}' | sort | uniq -c
   1 cmd/sq/main.go
   1 cmd/sq/recover.go
   2 internal/admin/server_test.go
   9 internal/cluster/cluster_test.go
   1 internal/cluster/manager.go
   2 internal/cluster/manager_test.go
   1 internal/cluster/raftstore_test.go
   2 internal/cluster/snapinstall_test.go
   1 internal/core/adminops/adminops_test.go
   1 internal/core/delay/delay_test.go
   1 internal/core/deliver/bench_test.go
   1 internal/core/deliver/deliver_test.go
   1 internal/core/deliver/soak_test.go
   7 internal/core/meta/meta_test.go
   3 internal/core/produce/bench_test.go
   1 internal/core/produce/produce_test.go
   1 internal/core/produce/soak_test.go
   1 internal/core/query/query_test.go
   1 internal/core/retention/retention_test.go
   1 internal/core/txn/txn_test.go
   2 internal/metrics/metrics_test.go
   1 internal/metrics/series_test.go
   3 internal/replication/replication_test.go
   4 internal/rpc/handle_secret_test.go
   1 internal/rpc/server_test.go
   1 internal/rpc/settings_test.go
   2 internal/store/store_readview_test.go
   4 test/e2e/cluster_expand_test.go
   1 test/e2e/sdk_cluster_test.go
$ grep -n "opts.Store = nil" cmd/sq/main.go
319:			opts.Store = nil // Rejoin 忽略 Store/Listener，内部按 dataDir 重开
$ P=$(go env GOMODCACHE)/github.com/cockroachdb/pebble/v2@v2.1.6
$ grep -n "cacheDefaultSize       =" $P/options.go
40:	cacheDefaultSize       = 8 << 20 // 8 MB
$ grep -n "o.L0CompactionThreshold = 4\|o.L0StopWritesThreshold = 12\|o.MemTableSize = 4 << 20\|o.MemTableStopWritesThreshold = 2\|return 1, 1 }" $P/options.go
1417:		o.L0CompactionThreshold = 4
1446:		o.L0StopWritesThreshold = 12
1477:		o.MemTableSize = 4 << 20 // 4 MB
1480:		o.MemTableStopWritesThreshold = 2
1486:		o.CompactionConcurrencyRange = func() (int, int) { return 1, 1 }
$ grep -n "o.Levels\[0\].EnsureL0Defaults()\|EnsureL1PlusDefaults(&o.Levels\[i-1\])" $P/options.go
1459:	o.Levels[0].EnsureL0Defaults()
1461:		o.Levels[i].EnsureL1PlusDefaults(&o.Levels[i-1])
$ grep -n "o.FilterPolicy = previousLevel.FilterPolicy\|o.FilterPolicy = NoFilterPolicy" $P/options.go
455:		o.FilterPolicy = NoFilterPolicy
480:		o.FilterPolicy = previousLevel.FilterPolicy
$ grep -n "^func FilterPolicy\|^type FilterPolicy" $P/bloom/bloom.go
218:type FilterPolicy int
$ grep -n "filterBlockSizeLimit = sstable.NeverUseFilterBlock" -B3 $P/file_cache.go
658-		// flushable ingests, as these blocks can be very big.
659-		if !opts.UseL6Filters {
660-			if opts.layer == manifest.Level(6) {
661:				filterBlockSizeLimit = sstable.NeverUseFilterBlock
$ sed -n 588,592p $P/db.go
		iterOpts: IterOptions{
			// TODO(sumeer): replace with a parameter provided by the caller.
			Category:                      categoryGet,
			logger:                        d.opts.Logger,
			snapshotForHideObsoletePoints: seqNum,
$ grep -n 'meta\["fullfilter."+name\]' $P/sstable/reader.go
517:		if bh, ok := meta["fullfilter."+name]; ok {
$ grep -n "actual MemTable size starts at" $P/options.go
890:	// The size of a MemTable in steady state. The actual MemTable size starts at
$ grep -n "memtblOpts.releaseAccountingReservation = d.opts.Cache.Reserve" $P/db.go
2557:		memtblOpts.releaseAccountingReservation = d.opts.Cache.Reserve(int(size))
$ grep -n "Filter " $P/metrics.go | head -1
283:	Filter FilterMetrics
$ grep -n "Hits int64\|Misses int64" $P/sstable/filter.go
14:	Hits int64
18:	Misses int64
$ grep -n "uint64(o.MemTableSize) >= maxMemTableSize" $P/options.go
2386:	if uint64(o.MemTableSize) >= maxMemTableSize {
$ grep -n "slice := make(\[\]byte, n)" $P/internal/manual/manual_nocgo.go
20:	slice := make([]byte, n)
$ grep -n "CGO_ENABLED=0" .github/workflows/release.yml
75:            GOOS="${GOOS}" GOARCH="${GOARCH}" CGO_ENABLED=0 \
$ grep -n "SnapshotChunkBytes int" internal/config/config.go
141:	SnapshotChunkBytes int `yaml:"snapshot_chunk_bytes"`
$ grep -n "time.ParseDuration(cfg.RetentionCheckInterval)" internal/config/config.go
261:	if d, err := time.ParseDuration(cfg.RetentionCheckInterval); err != nil || d <= 0 {
$ grep -n "type Options struct\|Store \*store.Store" internal/cluster/manager.go | head -3
70:type Options struct {
77:	Store      *store.Store
$ head -20 <scratchpad>/prof/deliver_async.out | grep ns/op
  935673	     10610 ns/op	     502 B/op	      13 allocs/op
$ grep -n "syscall.pread\|Store).Get" <scratchpad>/prof/deliver_async.cpu.cum.txt | head -3
20:         0     0% 69.93%     11.40s 65.55%  syscall.pread
24:     0.01s 0.058% 69.98%     10.48s 60.26%  github.com/Xsxdot/sq/internal/store.(*Store).Get
```

（`<scratchpad>` = `/private/tmp/claude-501/-Users-df-zidong--claude-spares-spare-DqhYVb/98ca6811-45c1-4c05-ab6b-dd70b9886a38/scratchpad`）

spec 剖析读数（main@21ee680，本机 Apple M4，cgo 构建）：deliver_async `syscall.pread` cum 65.55%，`store.(*Store).Get` 60.26%。基线时的 async 基准是会话内临时文件 `BenchmarkAsyncAckParallel`，未入库——故本卡第 1 步新增入库的 async 变体并重取基线。
