# SQ4.4 计划：seglog 后台刷盘移出锁外、各组并行刷（L2）

> 状态：**已拍板（2026-11-11，主会话）**——岔口 1–3 按推荐；岔口 1 的未导出 syncHook（生产恒 nil）依仓库先例 deliver.afterAppendHook 放行，不得导出、不得有生产分支逻辑；真机首跑项在 linux-01 跑
> 需求：`docs/superpowers/specs/2026-11-11-sq4-perf-batch-design.md`「SQ4.4」节 + 批次共同红线（不改盘上与线上格式、不降低持久化安全承诺）
> 基线：spec 读数为 main@21ee680；本计划读数复核于集成分支 `cards/sq4`（a8639eb 起读，出稿时已前进到 3374f4a，合入的 SQ4.6/4.7/4.11 未触及 `internal/cluster/`，引用行号不变）
> 执行形态：本会话 subagent 实现，独立 subagent 审，主会话验收

## 待拍板岔口

1. **S1「刷盘期间追加不被阻塞」的证明手段**
   - 推荐：`seglog.Log` 加一个**未导出**的测试注入字段 `syncHook func()`（生产恒为 nil），在 `Sync` 释放锁之后、发起落盘系统调用之前调用一次。同包测试设置它阻塞住刷盘，然后在另一 goroutine 里 `Append`，断言 `Append` 在钩子释放前就返回。先例：`internal/core/deliver/deliver.go:109-112` 的 `afterAppendHook`（「测试专用注入钩子（生产恒为 nil）」）。包外不可达，不算生产入口。
   - 被否 a：纯计时统计（刷盘循环里测 Append 延迟分布）——fsync 耗时随机器/文件系统漂移，判据不确定、易抖。
   - 被否 b：包级函数变量替换 `datasync`——全局可变状态，与改 `SegMaxBytes` 的现有用例一样无法并行，且影响同进程其它 Log。
2. **锁外刷盘被并发事件「抢先」时的返回语义**
   - 推荐：刷盘拿到的文件句柄若在系统调用前已被关闭，回锁复核——若 `activeSeq` 已前进（被**轮转**关闭）返回 nil（轮转屏障已对旧段完整 fsync，覆盖本次调用前的全部写入）；若仍是同一段而 `active == nil`（被 **Close** 关闭）返回「已关闭」错误（Close 本身不刷盘，不能冒称已覆盖）。真 I/O 错误（EIO 等）一律原样上抛，绝不吞。
   - 被否 a：一律返回 nil——Close 抢先的情形会把「未刷」报告成「已刷」，违反覆盖语义。
   - 被否 b：一律报错——每次段轮转与 flusher 撞车都会让 flusher 跳过本拍 `SyncWAL`（连坐），轮转越频繁 FSM 刷盘越被拖延，属无谓降级。
3. **SyncLogs 并行形态**
   - 推荐：每个已打开的组日志一个 goroutine，`sync.WaitGroup` 等全部完成后用 `errors.Join` 汇总返回（组数上限 64 数据组 + 1 meta 组，不设并发上限）。
   - 被否 a：固定大小 worker 池——组数小，池只增加代码不增加收益。
   - 被否 b：`golang.org/x/sync/errgroup`——go.mod 无此依赖，为一处 Wait 新增依赖不值；且 errgroup 的「首错取消」语义在这里不需要（每组都应尽力刷）。

## 1. 交付目标

| 故事 | 做到哪里 |
|---|---|
| S1 刷盘进行中同一日志的追加不被阻塞 | `Log.Sync` 只在锁内取「活动段句柄 + 段号 + prealloc 标志」，fdatasync/fsync 在锁外执行；seglog 包测试以钩子证明 |
| S2 轮转与刷盘并发压测无错误、无数据丢失 | 并发压测用例：小段长强制高频轮转，刷盘循环与追加并行，`Sync` 全程无错误；关闭后重开逐条校验 |
| S3 三节点 e2e quorum-mem p99 对比基线 | 同机同会话跑改前/改后 `TestClusterWriteThroughput/cluster-quorum-mem`，p50/p99/吞吐数字进卡（不设硬阈值） |

不交付：改 200ms 周期或做成可配置（spec Out of Scope）；改 `group.go`；改 flusher 的连坐策略。

## 2. 改动边界

**文件集**（只改这些）：

| 文件 | 改什么 |
|---|---|
| `internal/cluster/seglog/seglog.go` | `Sync` 改为锁内取句柄、锁外落盘、被抢先时回锁复核（岔口 2）；把 `syncActive` 的「按 prealloc 选 datasync / fsync」抽成对**给定句柄**操作的私有函数，供 `syncActive`（锁内调用点不变：Append 的 sync=true、轮转后 HS 补写）与锁外 `Sync` 共用；`Log` 类型并发注释改写（Sync 不再全程持锁）；新增 `syncHook` 字段（岔口 1） |
| `internal/cluster/seglog/falloc_linux.go` | `datasync` 改走 `f.SyscallConn()` + `RawConn.Control` 执行 `unix.Fdatasync`，并让「文件已关闭」可被调用方识别（见下「承重事实」） |
| `internal/cluster/seglog/falloc_other.go` | 仅在需要统一「已关闭」识别方式时调整（`f.Sync()` 已返回可 `errors.Is(err, os.ErrClosed)` 的错误） |
| `internal/cluster/raftstore.go` | `SyncLogs` 并行化（岔口 3）；错误信息带组号（现状只带底层错误，无法定位哪个组） |
| `internal/cluster/manager.go` | **只改 flusher 方法注释**（`manager.go:1424-1429`「SyncLogs 内部按组循环」改为并行描述），逻辑不动——与 SQ2 的提案队列改动无行重叠 |
| `internal/cluster/seglog/seglog_test.go`、`internal/cluster/raftstore_test.go` | 新增用例 |

**不碰**：`internal/cluster/group.go`（SQ2/SQ1 在改）；`maybeRotate`、`TruncateTo`、`Close` 的逻辑（只核对其与锁外刷盘的并发正确性，见下）；段文件与帧格式。

**真实入口**：`Manager.flusher`（`manager.go:1439`）→ `raftStore.SyncLogs`（`raftstore.go:868`）→ `seglog.(*Log).Sync`（`seglog.go:634`）。

**架构方向**：依赖方向不变（cluster → seglog）；无新增导出 API，`Log.Sync`、`raftStore.SyncLogs` 签名不变。无新增冻结对象；复用契约：flusher「先日志后 WAL」顺序契约（`raftstore.go:865-867`、`manager.go:1448-1450`）。

### 承重事实（已读源码钉死）

1. **现状 `datasync` 在锁外不安全**：`falloc_linux.go:57` 用 `unix.Fdatasync(int(f.Fd()))`——裸 fd 不持 Go 运行时的引用计数。移到锁外后，并发轮转 `Close` 可能先执行 `close(2)`，fdatasync 撞 EBADF，甚至 fd 号被复用到别的文件。**必须改**。
2. **`RawConn.Control` 持引用**：Go 1.26.1 `internal/poll/fd_posix.go:56-63` `RawControl` 先 `incref`、回调结束 `decref`；`fd_unix.go:91-115` `Close` 只标记关闭，真正的 `close(2)` 由最后一次 `decref` 执行。故 Control 回调内的 fdatasync 必然作用在有效 fd 上，并发 Close 被推迟到回调返回后。Linux 普通文件注册 epoll 失败 → `isBlocking=1`（`fd_unix.go:62-70`），`Close` 不等待在途调用，**轮转不会被锁外刷盘反向阻塞**。
3. **「已关闭」的识别**：`os.File.Sync` 经 `wrapErr` 把 `poll.ErrFileClosing` 换成 `os.ErrClosed`（`os/file.go:466-476`）；`rawConn.Control` 返回的是未换皮的 `poll.ErrFileClosing`（`os/rawconn.go:18-25`），`errors.Is(_, os.ErrClosed)` 不成立。但 `Control` 的返回值**只**反映 fd 状态（关闭），系统调用错误经闭包变量带出——据此可干净区分「已关闭」与「真 I/O 错误」，无需比对字符串。
4. **轮转屏障**：`maybeRotate`（`seglog.go:494-512`）在 `Close` 旧段前已 `Truncate` + 完整 `Sync`；`activeSeq` 只在新段创建成功后才前进（`seglog.go:537-539`）。故「`activeSeq` 已前进」⇒ 旧段已完整落盘，岔口 2 的「轮转覆盖 → nil」成立。
5. **TruncateTo 不与锁外刷盘冲突**：它只删 `segMax` 里的已关闭段（`seglog.go:683-696`），活动段永不在列；若删到某个锁外刷盘仍持有的旧段，unlink 后的 inode 仍可 fdatasync，结果无害。
6. **关闭路径不与 flusher 重叠**：`StopClean`/`kill` 都先等 flusher 退出再 `CloseLogs`（`manager.go:1497-1519`、`1552-1567`）；`wipeLog` 只经 `migrateLog`（`manager.go:492`）与 `ResetGroupProgress`（`manager.go:696`，未完成快照安装的启动期重置）调用，两处都在 `NewManager` 期，flusher 在 `Start`（`manager.go:906`）才起。岔口 2 的 Close 分支是防御，不是常态。

## 3. 工作顺序

1. **先钉承重假设（阻塞后续全部）**：写不变式用例 I1/I2（见 §4），用 `syncHook` 让 `Sync` 停在「已取句柄、未落盘」，再让测试触发轮转 / Close，放行后核返回值。先在 macOS 跑（走 `f.Sync()` 路径）。失败 → 停，回报主会话（说明 Go 运行时的引用计数行为与上读数不符）。
2. 实现 `datasync` 的 Control 版与 `Sync` 改造；I1/I2/I3 + S1 用例转绿；`go test -race ./internal/cluster/seglog/ -count=3` 绿。
3. **真机首跑项**（见 §4 真机清单 R1）：在 linux-01 上跑 seglog 包测试，确认预分配生效、datasync 走 Control 路径时 I1/I2/S1/S2 全绿。失败 → 不进第 4 步，回主会话。
4. S2 并发压测用例。
5. `SyncLogs` 并行化 + raftstore 用例 I4；flusher 注释更新。
6. `make test-cluster`（`internal/cluster/...` 全量，-count=1）。
7. S3：同机同会话先跑改前（`git stash` 或在 `21ee680` 的临时 worktree 构建）再跑改后，记数字。

## 4. 验收方式

### 故事级检查（每条必交故事一条）

| 故事 | 检查 | 命令（worktree 根目录） | 判据 |
|---|---|---|---|
| S1 | `TestSyncDoesNotBlockAppend`：设 `syncHook` 阻塞；goroutine 调 `Sync`；等钩子被进入后，主 goroutine 连续 `Append` 若干条（含 HS） | `go test -race ./internal/cluster/seglog/ -run TestSyncDoesNotBlockAppend -count=3` | 钩子释放**之前** Append 全部返回且无错误（2s 超时即失败）；释放后 `Sync` 返回 nil。反例：把 `Sync` 改回全程持锁，本用例必须超时失败（review 变异项） |
| S2 | `TestRotateConcurrentWithSyncNoLoss`：`SegMaxBytes` 调到约 256 字节（defer 还原），追加 goroutine 写 5000 条 entry（每 100 条带一次 HS，sync=false），刷盘 goroutine 循环 `Sync` 直到追加结束；然后 `Close`、`Open` 重开 | `go test -race ./internal/cluster/seglog/ -run TestRotateConcurrentWithSyncNoLoss -count=5` | 所有 `Sync` 返回 nil；轮转次数 > 50（读段文件数，证明确实高频撞车）；重开得到 index 1..5000 连续、Data 逐条相等、HS 为最后一次写入值 |
| S3 | 三节点 e2e 写吞吐 | 见下「S3 命令」 | 改前/改后 quorum-mem conc=64、256 的 msg/s、p50、p99 两组数字进卡；不设阈值，但若 p99 变差须在卡上解释 |

S3 命令（e2e 是独立 module，TestMain 自动编译 broker）：

```
cd test/e2e && SQ_BENCH=1 SQ_BENCH_CONC=64,256 go test -tags e2e -count=1 -run 'TestClusterWriteThroughput/cluster-quorum-mem' -v -timeout 40m ./ 2>&1 | tee <scratchpad>/sq4.4-e2e-after.log
```

改前同命令在本卡分支起点提交的临时 worktree 里跑（`git worktree add <scratchpad>/sq44-base <起点提交>`；不用 21ee680——集成分支上 SQ4.6/4.7 已改变写路径性能，spec 记录的 14696 msg/s 只作参考），输出到 `sq4.4-e2e-before.log`。两次同机、同会话、机器空闲时连续跑。

### 不变式单测（只为承重规则）

| 编号 | 规则 | 用例要点 |
|---|---|---|
| I1 | 被轮转抢先关闭 → 返回 nil | 钩子阻塞 `Sync` → 测试 Append 足量数据触发轮转（`SegMaxBytes` 调小）→ 放行 → `Sync` 返回 nil；且返回值不得是「file already closed / use of closed file」类错误 |
| I2 | 被 Close 抢先 → 返回「已关闭」错误 | 钩子阻塞 `Sync` → `Close()` → 放行 → `Sync` 返回非 nil 且错误信息含「已关闭」 |
| I3 | Close 后再 Sync 仍报错（现有行为不回退） | `Close` 后 `Sync` 返回非 nil |
| I4 | `SyncLogs` 等全部组完成才返回、任一组失败即返回错误且错误带组号 | raftstore 测试：打开 3 个组日志，直接关闭其中一个的 `*seglog.Log`，`SyncLogs` 返回错误且含该组号；另两组照常刷（不 panic、不提前返回） |

### 编译与触及包测试

```
go build ./... && go vet ./...
go test -race ./internal/cluster/seglog/ -count=1
make test-cluster
```

### 真机清单（linux-01，登录方式见 `~/.claude/infra/machines.md`；NVMe ext4，预分配生效）

- **R1（首跑项）**：seglog 包测试在 Linux 预分配路径上全绿。步骤：在 linux-01 上若有 Go 1.26 工具链，同步 worktree 后直接 `go test -race ./internal/cluster/seglog/ -count=3 -v`；没有则本机 `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -c -o <scratchpad>/seglog.linux.test ./internal/cluster/seglog/`，scp 后在 NVMe 目录下 `TMPDIR=/root/sqtmp ./seglog.linux.test -test.v -test.count=3`（无 -race）。证据：完整输出存档；输出中不得出现「预分配未生效」日志（证明走的是 datasync/Control 路径）。
- R2：linux-01 上跑 S3 同一命令（改前/改后），数字作为 Linux 侧补充进卡。

## 5. 决策权限

执行者可自行决定：私有函数命名与拆分；「已关闭」识别的具体实现（哨兵错误或包装 `os.ErrClosed`，只要不比对字符串）；测试中的段长、条数、超时取值；SyncLogs 收集错误的具体写法。

须升级主会话：
- 刷盘覆盖语义的任何变化（「一次 Sync 覆盖调用前全部写入」之外的表述）；岔口 2 的返回语义偏离拍板结果。
- 需要改 `group.go`、flusher 逻辑（周期、连坐策略、Sync 与 SyncWAL 的顺序）、`maybeRotate`/`TruncateTo`/`Close` 的行为。
- 新增任何导出符号或配置项；段文件/帧格式的任何变化。
- 第 1 步承重假设不成立，或 R1 在 Linux 上失败。

## 6. 最大风险

- **Linux datasync 路径只能真机验**：本机 macOS 不预分配、走 `f.Sync()`，Control 版 fdatasync 的引用计数行为只在 linux-01 上被执行到——R1 是首跑项的原因。
- **既有、非本卡引入**：Linux fsync 错误只报告给一个调用者（errseq 按 open file description 记账）。锁外刷盘与轮转屏障的 fsync 作用在同一 fd 上时，EIO 只会被其中一个看见；改前两者串行同样存在「flusher 先看到 EIO、轮转 fsync 随后返回成功」的序列，本卡不使之更坏，也不在本卡修。卡上留一行观察记录。

## 读数与证据

```
$ git rev-parse --short HEAD
3374f4a
$ git diff --stat a8639eb..HEAD -- internal/cluster
（无输出：集成分支新合入的提交未触及 internal/cluster）
$ grep -n "func (l \*Log) Sync" internal/cluster/seglog/seglog.go
634:func (l *Log) Sync() error {
$ grep -n "unix.Fdatasync(int(f.Fd()))" internal/cluster/seglog/falloc_linux.go
57:	if err := unix.Fdatasync(int(f.Fd())); err != nil {
$ grep -n "m.rs.SyncLogs\|m.st.SyncWAL" internal/cluster/manager.go
1451:			if err := m.rs.SyncLogs(); err != nil {
1467:			if err := m.st.SyncWAL(); err != nil {
$ go version
go version go1.26.1 darwin/arm64
$ sed -n 56,63p $(go env GOROOT)/src/internal/poll/fd_posix.go
func (fd *FD) RawControl(f func(uintptr)) error {
	if err := fd.incref(); err != nil {
		return err
	}
	defer fd.decref()
	f(uintptr(fd.Sysfd))
	return nil
}
$ grep -n "func (fd \*FD) Close\|increfAndClose\|fd.pd.evict()\|err := fd.decref()\|if fd.isBlocking == 0\|runtime_Semacquire(&fd.csema)" $(go env GOROOT)/src/internal/poll/fd_unix.go
91:func (fd *FD) Close() error {
92:	if !fd.fdmu.increfAndClose() {
101:	fd.pd.evict()
105:	err := fd.decref()
111:	// No need for an atomic read of isBlocking, increfAndClose means
113:	if fd.isBlocking == 0 {
114:		runtime_Semacquire(&fd.csema)
$ sed -n 62,70p $(go env GOROOT)/src/internal/poll/fd_unix.go
	if !pollable {
		fd.isBlocking = 1
		return nil
	}
	err := fd.pd.init(fd)
	if err != nil {
		// If we could not initialize the runtime poller,
		// assume we are using blocking mode.
		fd.isBlocking = 1
$ sed -n 18,25p $(go env GOROOT)/src/os/rawconn.go
func (c *rawConn) Control(f func(uintptr)) error {
	if err := c.file.checkValid("SyscallConn.Control"); err != nil {
		return err
	}
	err := c.file.pfd.RawControl(f)
	runtime.KeepAlive(c.file)
	return err
}
$ grep -n "if err == poll.ErrFileClosing" -A1 $(go env GOROOT)/src/os/file.go
470:	if err == poll.ErrFileClosing {
471-		err = ErrClosed
$ grep -n "wipeLog\|ResetGroupProgress\|migrateLog(" internal/cluster/raftstore.go internal/cluster/manager.go | grep -v "//"   # 节选调用行
internal/cluster/manager.go:492:		if err := m.rs.migrateLog(g); err != nil {
internal/cluster/manager.go:696:			if err := m.rs.ResetGroupProgress(g); err != nil {
internal/cluster/raftstore.go:194:func (r *raftStore) wipeLog(g uint32) error {
internal/cluster/raftstore.go:487:	if err := r.wipeLog(g); err != nil {
internal/cluster/raftstore.go:786:	if err := r.wipeLog(g); err != nil {
$ grep -n "go m.flusher()" internal/cluster/manager.go
906:		go m.flusher()
$ grep -n "cc.DataGroups < 1 || cc.DataGroups > 64" internal/config/config.go
364:		if cc.DataGroups < 1 || cc.DataGroups > 64 {
$ grep -n "golang.org/x/sync" go.mod
（无输出）
$ grep -n "afterAppendHook" internal/core/deliver/deliver.go | head -2
109:	// afterAppendHook 测试专用注入钩子（生产恒为 nil）：在 moveToDLQ 第一段
112:	afterAppendHook func()
$ grep -n "gr.appendOnce(ctx, lm)" internal/cluster/group.go
532:				gr.appendOnce(ctx, lm) // 内联快路径：无 fsync，内存级写入
728:			gr.appendOnce(ctx, lm) // 快照独立处理，自带计数与响应投递
```

spec 记录的 e2e 基线（main@21ee680，本机）：`cluster/quorum-mem/conc=64  14696 msg/s  p50=2.592ms  p99=46.846ms`。
