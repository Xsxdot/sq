# B11-OPEN-1：mem 档本地恢复丢失已确认消息（healthy 阶段尾部）

> 交接物：写给一个没有本文上下文的读者。本文记录 2026-08-11 在 Task 7 e2e 场景用例
> 实跑中暴露的一条**已确认消息丢失**缺陷，与「静置纪律」的预期不符。
>
> **状态：已定位、已修复、已验证（2026-08-11）。根因见第 0 节；第 1–7 节保留调查
> 当时的原始现场，其中第 6 节有一条排除结论事后被证明是错的，已就地标注。**

## 0. 根因与修复（结论）

**根因：mem 档的「200ms 周期刷盘」是一个彻底的空操作，mem 档实际上从不 fsync。**

`Manager.flusher()` 每 200ms 执行 `m.st.ApplyWith(m.st.NewBatch(), true)`——提交一个
**空批次**并带 `pebble.Sync`。而 Pebble 的提交管线开头第一句就是短路：

```go
// pebble/v2@v2.1.6 commit.go:298
func (p *commitPipeline) Commit(b *Batch, syncWAL bool, noSyncWait bool) error {
	if b.Empty() {
		return nil
	}
```

空批次连 WAL 都不写，更不会触发 fsync。叠加 `syncPersist` 在 mem 档恒为 false
（raft 日志与 HardState 也是 NoSync），**quorum-mem 档在整个运行期一次盘都不刷**，
落盘完全依赖 Pebble WAL 缓冲写满自发外刷。断电时丢的就是最后那个没写满的
WAL block——正好是「健康期尾部十几条已确认消息」这个形态。

三个节点丢失条数与内容完全一致，是因为恢复后 raft 会向日志最长的那个节点收敛，
最终存量 = 各节点持久化前缀的最大值。

**引入时间**：`0c7aa8c`（B8.2，2026-08-08），比 B11 早两天。**B11 是暴露者不是引入者**
——B11 的场景用例是第一次用「全集群 SIGKILL 后必须零丢失」去压这条路径。

**判据（vfs 层直接数 fsync 调用次数，不再用 WAL 文件大小这种会骗人的代理指标）**：

| 动作 | fsync 增量 |
|---|---|
| NoSync 写 1000 条 | 基线 |
| 空批次 + `pebble.Sync` × 5 拍（旧 flusher 的精确动作） | **0** |
| `LogData` + `pebble.Sync` × 5 拍（新实现） | **5** |
| 非空批次 + `pebble.Sync` × 1（对照） | 1 |

**修复**：新增 `store.SyncWAL()`，用 `Batch.LogData` 往批次里塞一条只进 WAL、不进
memtable/sstable、不被索引的屏障记录，再 `pebble.Sync`。批次因此非空，提交管线不再
短路，fsync 真正发生；同时不污染键空间、不产生 compaction 压力、不影响任何扫描路径。
`Manager.flusher()` 改调 `store.SyncWAL()`。

**回归**：`TestSyncWALPersistsNoSyncWrites`（`internal/store`）用 `vfs.NewCrashableMem`
的 `CrashClone`（只保留已 fsync 的字节）做真掉电语义断言，并带反向对照（不加屏障
必须真的丢，否则用例失去判别力）。e2e 侧 `TestScenarioFullProcessCrashRecoversLocally`
新增**存量断言**：断电前后各读一次 `GET /admin/topics/{name}` 的 `queues_detail[].next_offset`
求和，恢复后不许比断电前少。

**修复前后实测（同一条 e2e 用例）**：

| | kill 前存量 | 恢复后存量 | 对账 |
|---|---|---|---|
| 修复前 | 175（三节点一致） | **158（三节点一致，丢 17）** | FAIL，确认集 17 条未消费 |
| 修复后 | 164（三节点一致） | **164（三节点一致，零丢失）** | PASS，确认 328 条全量消费 |

---

## 1. 现象

三条新加的 e2e 场景用例，两条在实跑中稳定丢失已确认消息：

| 用例 | 恢复路径 | 丢消息数 | 结果 |
|---|---|---|---|
| `TestScenarioFullProcessCrashRecoversLocally` | **local-resume**（mem 档，抬 term） | 17 条 | **FAIL**（两次实跑均红） |
| `TestScenarioRebootedMemRecoversAfterGrant` | **local-forced**（mem 档，抬 term + 消费许可） | 19–20 条 | **FAIL**（两次实跑均红） |
| `TestScenarioRebootedFsyncResumesLocally` | local-resume（**fsync 档**，不抬 term） | 0 条 | PASS |
| `TestScenarioRebootedMemNeedsPermit` | 拒启（无恢复） | — | PASS |

丢失的消息全部是**健康期（kill 之前）发送阶段的尾部**：msg_id 计数器 0x9B–0xAC
（约 155–172，健康期共确认 172–173 条）——也就是紧贴 kill 之前最后写入的那一批。

## 2. 关键点：这不是「静置纪律无效」，是 mem 档本地恢复特有

- 三个 mem 档 / fsync 档用例都做了「**静置 ≥500ms 再 kill**」（> `flusher()` 的 200ms
  周期 + 余量），且都等 `produceUntil` 完全停止后才静置。
- **local-resume（mem 档）也丢**：这与最初「只有 grant 路径丢」的猜测不符。实测证据：
  `TestScenarioFullProcessCrashRecoversLocally` 单独实跑两次，每次丢 17 条。
- **fsync 档不丢**：`TestScenarioRebootedFsyncResumesLocally` 实跑确认集 218 条全量
  消费，零丢失。
- 因此分界线不是「签字 vs 不签字」，而是 **mem 档的本地恢复（local-resume 与
  local-forced 两条路径都抬 term）**。fsync 档不抬 term、不丢。

> 注：Task 6b 起，mem 档 local-resume 也会抬 term（`needsTermBump`）。fsync 档
> local-resume 不抬 term。即「丢」的两条路径都经历了「本地恢复 + 抬 term」，
> 不丢的 fsync 路径两者都不经历。这个相关性留作线索，不等同于根因。

## 3. 硬数字

### 3.1 healthy 阶段每组确认数与恢复后 raft 日志读回数对不上

grant 用例（`scn-reboot-grant`，queue→group：q2,q4→g1；q1→g2；q0,q3,q5→g3）：

| 组 | healthy 阶段确认条数 | 恢复读回 raft entries | 差值 |
|---|---|---|---|
| g1 | 58 | 57（commit 56） | 少 1–2 |
| g2 | 28 | 29（commit 29） | 0 |
| g3 | 86 | 81（commit 81） | **少 5** |

`sq recover` 报告（未 grant 前的只读快照）同样显示 g1 日志尾 57 / g3 日志尾 81。

crash 用例（`scn-full-crash`，queue→group：q0,q3,q5→g1；q1→g2；q2,q4→g3）：

| 组 | healthy 阶段确认条数 | 恢复读回 raft entries | 差值 |
|---|---|---|---|
| g1 | 86 | 82（commit 81） | **少 4–5** |
| g2 | 28 | 30（commit 30） | 0 |
| g3 | 58 | 57（commit 57） | 少 1 |

即：**恢复时盘上的 raft 日志尾部，比 healthy 阶段已确认/已 apply 的条目少**
——缺失的正是第 1 节点名的健康期尾部那批消息。

### 3.2 丢失的消息「已经写过 broker」

丢失的每条 msg_id 都能在 healthy 阶段 broker 日志里找到对应的 `消息已写入`
（queue/offset 具体位置见第 4 节）。即这些消息**在 kill 前已经 Send 成功并被
broker 确认**（produce 语义：quorum 确认 + 本地 apply），却在恢复后消费不到。

## 4. 关键日志原文

### 4.1 丢失消息确实写过（grant 用例，第二次实跑）

```
{"time":"2026-08-11T01:38:20.107+08","level":"DEBUG","msg":"消息已写入","mod":"produce","topic":"scn-reboot-grant","queue":5,"offset":25,"msg_id":"013EA206EED901F6850A8BA68C0000009A"}
{"time":"2026-08-11T01:38:20.160+08","level":"DEBUG","msg":"消息已写入","mod":"produce","topic":"scn-reboot-grant","queue":0,"offset":13,"msg_id":"013EA206EED901F6850A8BA68C0000009D"}
{"time":"2026-08-11T01:38:20.178+08","level":"DEBUG","msg":"消息已写入","mod":"produce","topic":"scn-reboot-grant","queue":2,"offset":35,"msg_id":"013EA206EED901F6850A8BA68C0000009E"}
{"time":"2026-08-11T01:38:20.196+08","level":"DEBUG","msg":"消息已写入","mod":"produce","topic":"scn-reboot-grant","queue":3,"offset":39,"msg_id":"013EA206EED901F6850A8BA68C0000009F"}
{"time":"2026-08-11T01:38:20.231+08","level":"DEBUG","msg":"消息已写入","mod":"produce","topic":"scn-reboot-grant","queue":1,"offset":26,"msg_id":"013EA206EED901F6850A8BA68C000000A1"}
{"time":"2026-08-11T01:38:20.265+08","level":"DEBUG","msg":"消息已写入","mod":"produce","topic":"scn-reboot-grant","queue":3,"offset":41,"msg_id":"013EA206EED901F6850A8BA68C000000A3"}
{"time":"2026-08-11T01:38:20.299+08","level":"DEBUG","msg":"消息已写入","mod":"produce","topic":"scn-reboot-grant","queue":5,"offset":26,"msg_id":"013EA206EED901F6850A8BA68C000000A5"}
{"time":"2026-08-11T01:38:20.351+08","level":"DEBUG","msg":"消息已写入","mod":"produce","topic":"scn-reboot-grant","queue":2,"offset":38,"msg_id":"013EA206EED901F6850A8BA68C000000A8"}
```

### 4.2 恢复路径判定（grant 用例，三节点均 local-forced）

```
{"level":"INFO","msg":"恢复路径判定","recovery":"local-forced","reason":"不干净关机且机器已重启，quorum-mem 档下日志尾可能残缺；已存在与本次机器世代匹配的运维许可，按签字放行本地恢复","clean":false,"hasRaft":true,"mode":"ack-quorum-mem","bootgenNow":"gen-after-reboot","bootgenNowOK":true,"bootgenStored":"boottime-…","bootgenStoredOK":true,"permit":true}
```

### 4.3 `sq recover` 报告（grant 前只读快照，三节点一致）

```
恢复判定：rejoin
  理由：不干净关机且无法证明本地日志可信（机器世代不可比或已变、确认档为
        quorum-mem、无运维许可），走重入编排
  结论：**需要运维签字**。quorum-mem 档的后台刷盘周期为 200ms，
        带着本地日志恢复最多可能丢失最后 200ms 内已确认的写入。

各组现场：
  组    applied    日志尾        尾 term    快照锚点       任期     commit
  0    6          6          2         0          2      6
  1    56         57         3         0          3      56
  2    29         29         2         0          2      29
  3    81         81         3         0          3      81
```

### 4.4 对账失败（两条用例各自）

```
grant:  reboot-grant 对账：3455 轮收齐并 ack 326 条（want 345）
        确认集有 19/345 条未被消费到（已确认消息丢失是红线）：[…0000009D …0000009F
        …000000A8 …000000A3 …0000009E …000000A1 …0000009A …000000A5 …]

crash:  full-crash 对账：3455 轮收齐并 ack 326 条（want 343）
        确认集有 17/343 条未被消费到（已确认消息丢失是红线）：[…000000A2 …0000009B
        …000000AB …000000A5 …000000AA …0000009F …000000A7 …000000A6 …]
```

## 5. 复现命令

```bash
# 在 test/e2e 目录（独立 module）执行；-v 完整输出落盘，勿用 | tail（会吃掉退出码）
go test -tags e2e ./ -run 'TestScenarioFullProcessCrashRecoversLocally' -v -timeout 30m
go test -tags e2e ./ -run 'TestScenarioRebootedMemRecoversAfterGrant' -v -timeout 30m

# 对照：不丢的两条
go test -tags e2e ./ -run 'TestScenarioRebootedFsyncResumesLocally' -v -timeout 30m
go test -tags e2e ./ -run 'TestScenarioRebootedMemNeedsPermit' -v -timeout 30m
```

实跑记录：
- `TestScenarioFullProcessCrashRecoversLocally`：两次 FAIL（193.41s / 208.33s，各丢 17）
- `TestScenarioRebootedMemRecoversAfterGrant`：两次 FAIL（206.54s / 207.89s，丢 20 / 19）
- `TestScenarioRebootedFsyncResumesLocally`：PASS（27.75s / 29.21s）
- `TestScenarioRebootedMemNeedsPermit`：PASS（109.60s）

## 6. 已排除的假设

1. ~~**「静置纪律无效 / flusher 没跑」**：fsync 档同条件不丢、mem 档丢——若静置普遍
   无效，fsync 档也会丢。且同用例内非尾部消息全部存活，只有健康期尾部（kill 前
   最后 ~300ms 写入）丢失，与「整段 WAL 没刷」的形态不符。~~
   **这条排除是错的，真正的根因就在这里（见第 0 节）。** 两条推理都不成立：
   (a) fsync 档不丢是因为它靠 `raft.MustSync` 逐次 fsync，**根本不需要 flusher**，
   所以它不丢证明不了 flusher 在 mem 档下有效；(b)「只有尾部丢」恰恰**就是**
   「从不 fsync、只靠 WAL 缓冲写满自发外刷」的形态——丢的是最后一个没写满的
   block，不是「整段 WAL 没刷」。
   *教训：拿一条不依赖该机制的路径去为该机制作证，是无效对照。*
2. **「grant 签字路径特有」**：local-resume（mem 档）单测同样丢 17 条，已排除。
3. **「测试写得不对 / 断言时机错」**：静置 500ms 在 produceUntil 完全停止后才开始
   （`close(stop)` → `wg.Wait()` → `GracefulStop()` → sleep），不是边发边 kill；
   与 fsync 用例的形态完全一致，fsync 用例过了。
4. **「丢失的是紧贴 kill 未落盘那批（符合签字的 ≤200ms 损失面）」**：丢失的是
   **已写进 broker 日志（已 apply）**的消息，且静置了 500ms（> 200ms 周期 + 余量），
   按 spec §8 的「静置后零丢失可断言」应当不丢。若这是 mem 档固有损失面，那
   spec §8/§11 的断言前提就要改写——但本地-resume 也丢这点说明不是签字引入的。

## 7. 未能排除的假设（审核者接手点）

1. **恢复重放/抬 term 路径丢了日志尾**：恢复读回的 raft entries 比 healthy 阶段
   已确认数少（§3.1）。可能是 `buildGroup` 重放起点、`LoadEntries` 截断、或
   抬 term 与 HardState 持久化的先后导致尾部条目在恢复时不可见。
2. **mem 档 HardState/日志尾的持久化路径与 fsync 档不同**：`syncPersist` 只跟随
   `raft.MustSync`，mem 档下 NoSync 写入 + 周期 flusher 的组合，可能在「进程被杀
   瞬间→重启」的窗口内丢了 raft 日志尾部，而 flusher 的 200ms 空批次 Sync 在
   **多组共享一条 WAL** 的前提下未必覆盖到每个组的最新尾部。
3. **apply 与 Persist 的时序竞态**：`消息已写入` 已打印（= apply 完成），但对应
   raft 条目的持久化可能尚未完成就被 kill——即「应用先于持久化」，恢复时
   raft 日志里没有该条目，消息就丢了。

以上三点需要读 `internal/cluster/group.go` 的 Ready 循环、`raftstore.Persist` 的
调用时机与 `Manager.flusher()` 的 WAL 语义才能定位，超出本 task 范围。

> **事后结论**：三条全不成立，根因是第 3 条猜的方向（持久化路径）里更朴素的一环
> ——`Manager.flusher()` 的 WAL 语义。定位它的决定性一步不是继续读代码，而是先做
> 一次**判别实验**：`GET /admin/topics/{name}` 在断电前后各读一次每队列 `next_offset`，
> 把假设空间一刀切成两半——存量变少 = 存储真丢（持久化/恢复路径），存量不变 =
> 存着但投递不到（消费/位点）。实测 175 → 158，直接砍掉一半假设。

## 8. 处置（调查当时；已被第 0 节的修复取代）

- 两条受影响用例（`TestScenarioFullProcessCrashRecoversLocally`、
  `TestScenarioRebootedMemRecoversAfterGrant`）断言**不做任何弱化**，函数体第一行
  `t.Skip("B11-OPEN-1：mem 档本地恢复丢失已确认消息，根因未定位，证据见本文")`。
- 用例本身编码的是正确语义：它现在测的是一个真 bug，不是测错了。
- backlog 新增 `B11-OPEN-1`（见 backlog 变更痕迹），优先级按仓库既有约定给「高」：
  已确认消息在正常无故障恢复路径上丢失 = 数据正确性问题，与 B10 同级。
