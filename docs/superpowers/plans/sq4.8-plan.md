# SQ4.8 计划：保留期清理不再全扫 key 索引（L2）

> 状态：**已拍板（2026-11-11，主会话）**——岔口 1–3 按推荐（补扫完成态不持久化；孤儿判定=过期且 msg 键不存在；512KiB 索引删除预算、只在消息边界截停）
> 需求：`docs/superpowers/specs/2026-11-11-sq4-perf-batch-design.md`「SQ4.8」节 + 批次共同红线（不改盘上与线上格式、不降低持久化安全承诺）
> 基线：spec 读数为 main@21ee680；本计划读数复核于集成分支 `cards/sq4`@3374f4a（新合入的 SQ4.6/4.7/4.11 未触及 `internal/core/retention/`、`internal/core/query/`；`internal/store/keys.go` 只改了一行注释，与 keyidx 无关）
> 执行形态：本会话 subagent 实现，独立 subagent 审，主会话验收

## 待拍板岔口

1. **「补扫已完成」是否持久化**
   - 推荐：**不持久化**。retention 进程内维护「已补扫完成的组」集合；每个进程生命周期内，对本节点 lead 的每个组至多做一次限速全扫。升级后不会再产生新孤儿（见 §2「孤儿来源」），重启后重扫只是一次限速的空跑，代价有界。
   - 被否：新增持久化标记键。任何新键都是盘上格式变化：数据组快照枚举 `keyGroupOf` 对不认识的键族直接报错中止（`internal/cluster/snapstream.go:189-190`「不属于任何数据键族」），滚动升级窗口里旧版本节点给持有新键的组生成快照会失败；放进 meta 组也要逐一核对新旧两侧的快照与重放路径。按批次红线需两步升级，收益只是省掉每次重启的一次空扫，不值。
2. **孤儿判定条件**
   - 推荐：索引条目同时满足「嵌入的 storeMs < 本 topic 当前 cutoff」**且**「`msg/{topic}/{queueID}/{offset}` 不存在」才删除。前一条是廉价预筛（只有过期区间才需要点查），后一条是孤儿的定义。
   - 被否 a：只看 storeMs < cutoff（旧逻辑）——时钟回跳会让 `purgeQueue` 的边界停在仍存活的过期消息之前（`retention.go:162-164` 注释已承认），只按时间删会让这类仍在盘上的消息查不到 key。
   - 被否 b：只看消息是否存在——需要对 topic 下每条索引点查一次，全扫退化成全扫 + 全量点查。
3. **清理批次大小**
   - 推荐：`purgeQueue` 保留「单趟最多 10000 条消息」，另加**索引删除字节预算 512KiB**：扫描回调里把将删的索引键字节累加，超预算即在**消息边界**停止（边界之前的消息连同其全部索引都在本批，边界之后的留待下趟）；首条消息自身超预算也至少纳入（保证有进展）。理由：一批 = 一条 raft 提案，`MaxSizePerMsg` 为 1MiB（`group.go:374`），传输层单帧硬上限 16MiB（`transport.go:52`）——超帧的提案永远发不出去，组会卡死；用户 key 长度与个数只受 gRPC 消息大小约束，10000 条 × 多个长 key 可以轻易越过 16MiB。
   - 被否：索引删除单独成批——失去「消息与其索引同批原子删除」，两批之间崩溃就是新孤儿，正是本卡要消灭的东西。

## 1. 交付目标

| 故事 | 做到哪里 |
|---|---|
| S1 消息过期删除后，其 key 索引同批删除；按 key 查询不再返回已删消息 | `purgeQueue` 在逐条解码被删消息时拼出全部索引键，与 `DeleteRange` 同一批次提交 |
| S2 稳态清理不再扫描未过期的 key 索引 | 删除 `purgeKeyIdx` 及其在 `Pass` 中的调用；以「本趟 keyidx 扫描键数」为观测量，稳态恒为 0 |
| S3 存量孤儿索引最终被清掉 | 后台补扫：限速、可中断、只在 leader 上、按组记录完成状态（岔口 1、2） |

不交付：改 keyidx 键布局（spec Out of Scope，永不在本批做）；`query.ByKey` 的行为（已对缺失消息跳过，`query.go:39-41`，不动）；adminops 删 topic 时的索引清理（`adminops.go:55`，不动）。

## 2. 改动边界

| 文件 | 改什么 |
|---|---|
| `internal/core/retention/retention.go` | `purgeQueue`：回调内对每条将删消息按下面的规则拼索引键并 `b.Delete`，加字节预算截停（岔口 3）；删除 `purgeKeyIdx` 与 `Pass` 中的调用；新增补扫（独立 goroutine，由 `Run` 启动并在 `Run` 返回前等它退出）；包头注释「keyidx 只能全扫」一段改写；`Manager` 新增未导出统计字段（本趟稳态 keyidx 扫描键数、补扫扫描键数/删除数），并写进现有 Info 日志 |
| `internal/core/retention/retention_test.go` | 新增用例（§4） |
| `internal/store/keys.go` | **预期不改**：`KeyIdxKey`（`keys.go:242`）已是唯一的拼键函数，写侧与删侧共用即保证一致。若执行中发现需要新增解析/拼键辅助，先升级 |

**真实入口**：`retention.(*Manager).Run`（`cmd/sq/main.go:471` 起 goroutine，停机时 `retCtx` 取消并 `retWG.Wait()` 后才关 store）→ `Pass` → `purgeQueue`；补扫 goroutine 由 `Run` 内部拉起，受同一 ctx 与 WaitGroup 约束，main 不改。

**架构方向**：retention 依赖 store / meta / replication 的方向不变；不新增导出 API；`Pass` 签名不变（`processable` 语义不变）。无新增冻结对象；复用契约：`replication.Replicator.Apply`（按组提交批次、非 leader 返回 `ErrNotLeader`）、`replication.Router.IsLeader/GroupForQueue`、`store.KeyIdxKey` 键布局。

### 拼键规则（已对写侧源码核实）

- 写侧只有 `produce` 两处，均与消息同批：`produce.go:194` `KeyIdxKey(m.Topic, key, m.StoreAtMs, m.QueueID, m.Offset)`（Append）、`produce.go:433` `KeyIdxKey(topic, key, m.StoreAtMs, qid, m.Offset)`（AppendBatch）；空 key 跳过。全仓写 `msg/` 的也只有这两处（`produce.go:187`、`428`），被删消息的值就是同一个 `m` 的编码。
- 删侧：`purgeQueue(topic, q)` 回调里解码出的 `msg`，对 `msg.Keys` 中每个非空 key 删除 `store.KeyIdxKey(topic, key, msg.StoreAtMs, q, msg.Offset)`。**时间字段取 `msg.StoreAtMs`**（与写侧同源；不是 `BornAtMs`）；topic 取扫描前缀里的 `topic` 参数（与写侧 `m.Topic` 相同，因为 msg 键也是用它拼的）；队列号取 `q`；同一消息 Keys 里的重复 key 拼出同一键，重复删除无害。
- 复制路径：点删与 `DeleteRange` 同在一个 `store.Batch`，经 `m.rep.Apply(ctx, g, b)` 作为一条提案复制到该队列组，follower 原样重放——pebble batch 编码格式不变，旧版本 follower 可正常应用。

### 孤儿来源（决定「补扫一次即可」是否成立）

- 升级前旧逻辑每趟只清 1 万条过期索引，积压的部分 = 存量孤儿。
- 滚动升级窗口内，旧版本 leader 的清理是「先删消息、另行全扫删索引」，可能留下少量新孤儿；这些组的新 leader（新版本）进程生命周期内补扫一次即可清掉，或下次重启再扫。
- 全部升级后：写侧消息与索引同批，删侧也同批，无新孤儿来源。

### 补扫设计（执行者在此边界内实现）

- 只扫本节点当前 lead、且尚未完成的组相关的条目：按 topic 遍历 `KeyIdxTopicPrefix(topic)`，每块最多 1000 个键（`Scan` 的 limit），块与块之间休眠 50ms（上限约 2 万键/秒，常量，不做成配置），下一块从上一块最后一个键之后继续。
- 每块内：解析 `ParseKeyIdxKey` 拿 storeMs、queueID、offset → 预筛 storeMs < cutoff 且 `GroupForQueue(topic, qid)` 属于待补扫组 → 块扫描结束后再逐条 `st.Get(MsgKey(...))`（不在迭代回调里读写交错）→ 不存在者按组攒批，提交前复核 `IsLeader(g)`，非 leader 丢弃该组本块结果并把该组移出本轮。
- 一轮从全部 topic 扫完后，把「本轮开始时就在列、全程未丢 leader、提交无错误」的组记入已完成集合；之后每个 `interval` 检查一次是否有新 lead 但未完成的组，有则再起一轮，无则空闲（不扫）。
- `ctx` 取消即在块边界返回；非 leader（本节点不 lead 任何未完成组）不发起任何扫描。
- 与 `purgeQueue` 并发：两者删同一索引键都是幂等删除；补扫看到「索引在、消息不在」时，消息与索引若是被 `purgeQueue` 同批删的，补扫再删一次无害。

## 3. 工作顺序

1. **先验证承重假设（阻塞后续）**：写 S1 故事级用例（走真实 `produce.Append` 写入，见 §4）并先在**未改代码**上跑——预期绿（旧 `purgeKeyIdx` 也能清掉）；再临时注释掉 `Pass` 里的 `purgeKeyIdx` 调用（不提交）跑一次——预期红。两次结果符合预期，才证明这条用例能区分「索引被清」与「没被清」。不符合 → 停，回主会话。
2. 改 `purgeQueue`（拼键 + 同批删除 + 字节预算），删除 `purgeKeyIdx`；S1、I1、I2 绿；现有 `TestPassPurgesExpired` 等全绿。
3. 统计字段 + S2 用例。
4. 补扫 + S3 用例 + I3/I4。
5. 触及包测试与全量编译；真机清单。

## 4. 验收方式

### 故事级检查

| 故事 | 检查 | 判据 |
|---|---|---|
| S1 | `TestPurgeDeletesKeyIndexSameBatch`：standalone 夹具，经真实 `produce.New(...).Append` 写 topic `t` 若干条带 Keys（含一条 Keys 重复、一条含 `/` 的 key）的消息；`mt.UpdateTopicRetention(ctx, "t", 1)` 后等 10ms；另一 topic `keep` 用默认保留期写同 key 作对照；`Pass` | `query.ByKey(st, "t", key, 0)` 返回 0 条；`KeyIdxTopicPrefix("t")` 下扫描为空；`keep` 的索引与消息原样在；`Pass` 返回 purged = 写入条数 |
| S2 | `TestSteadyPassScansNoKeyIndex`：写 2 万条未过期带 key 的消息（直写夹具 `writeMsgAt`，带 key）+ 1 条过期带 key 的消息；`Pass` | 统计字段「本趟 keyidx 扫描键数」== 0；过期那条的索引已删、未过期索引数仍为 2 万。反例：恢复旧全扫逻辑，该统计必 > 0（review 变异项） |
| S3 | `TestBackfillRemovesOrphans`：直写三类索引——(a) 孤儿：storeMs 过期、无对应消息；(b) 过期但消息仍在（模拟时钟回跳）；(c) 未过期、消息在；调用补扫的单轮入口（同包未导出函数） | (a) 全部删除；(b)(c) 全部保留；补扫完成后该组进入已完成集合，再调一次不再扫描（扫描计数不增长） |

命令：

```
go test ./internal/core/retention/ -run 'TestPurgeDeletesKeyIndexSameBatch|TestSteadyPassScansNoKeyIndex|TestBackfillRemovesOrphans' -count=1 -v
```

### 不变式单测（只为承重规则）

| 编号 | 规则 | 要点 |
|---|---|---|
| I1 | 消息与其索引不被拆到不同批次（字节预算在消息边界截停） | 写 N 条每条带若干长 key（单条索引字节约为预算的 1/3）的过期消息，跑一次 `Pass`：被删消息的索引全部不在、未删消息的索引全部在，不存在「消息在而部分索引已删」或反之 |
| I2 | 单条消息超预算也有进展 | 一条消息的索引字节 > 预算，`Pass` 仍删掉它及其全部索引 |
| I3 | 非 leader 不扫 | `fixedLeaderRouter{leader:false}` 下跑 `Run` 一小段（短 interval，随即取消）：补扫扫描计数 == 0，孤儿仍在 |
| I4 | 可中断 | 造 5 万条孤儿，补扫开始后取消 ctx：`Run` 在 2s 内返回；该组未被记为已完成 |

### 编译与触及包测试

```
go build ./... && go vet ./...
go test -race ./internal/core/retention/ ./internal/core/query/ ./internal/core/adminops/ -count=1
go test ./internal/core/... -count=1
```

### 真机清单

- **R1（首跑项）**：本机单机 broker 端到端。配置 `retention_check_interval: 5s`、开 admin（`admin_listen`）；用 SDK 或 `POST /admin/messages/send` 向 topic 写 20 条带 key 的消息；`PATCH /admin/topics/{name}`（请求体 `{"retention_ms":1000}`）把保留期改为 1000ms；等 10s；`GET /admin/messages?topic=…&key=…` 返回空；broker 日志出现带「索引删除数」字段的 retention 清理 Info 行，且无「retention 清理索引」（旧全扫日志）出现。证据：请求/响应与日志片段存档。
- **R2**：三节点集群（`test/e2e` 现成编排，`go test -tags e2e -run TestThreeNodeClusterE2E -count=1 ./` 全绿作为回归）；另在 linux-01（登录见 `~/.claude/infra/machines.md`）用旧版本二进制写入一批带 key 的消息并制造存量孤儿（旧版本跑出积压：写入量 > 1 万条且保留期调短），换新版本启动，观察补扫完成日志与 `GET /admin/messages` 不再命中旧 key 的索引（以补扫删除计数日志为准）。

## 5. 决策权限

执行者可自行决定：补扫的块大小、休眠时长（在「≤ 2 万键/秒」量级内）；统计字段与日志字段命名；补扫 goroutine 的内部结构；字节预算在 256KiB–768KiB 之间的取值。

须升级主会话：
- 新增任何持久化键或改变 keyidx/msg 键布局（岔口 1 的推荐被推翻也属此类）。
- 改 `Pass` 签名或 `processable` 语义；改 `query`、`adminops`、`produce` 的行为。
- 发现 §2「拼键规则」之外还有写 keyidx 或 msg 的路径。
- 需要改 `internal/store/keys.go`。
- 第 1 步承重验证不符合预期。

## 6. 最大风险

旧 `purgeKeyIdx` 删除后，任何未被识别的索引写入路径（或未来新增的写路径）产生的孤儿只能靠「每进程一次」的补扫兜底；若写侧将来出现「索引与消息不同批」的写法，孤儿会悄悄积累。缓解：在 `KeyIdxKey` 注释与 retention 包头写明「索引必须与消息同批写、同批删」的约束。

## 读数与证据

```
$ git rev-parse --short HEAD
3374f4a
$ git diff --stat a8639eb..HEAD -- internal/core/retention internal/core/query internal/store/keys.go
 internal/store/keys.go | 2 +-
 1 file changed, 1 insertion(+), 1 deletion(-)
$ grep -n "const maxPurgePerQueue\|func (m \*Manager) purgeQueue\|func (m \*Manager) purgeKeyIdx\|err := m.st.Scan(pfx, store.PrefixUpperBound(pfx), 0\|return n < maxPurgePerQueue, nil\|m.purgeKeyIdx(ctx, tc.Name, cutoff)\|core.DecodeMessage(v)" internal/core/retention/retention.go
37:const maxPurgePerQueue = 10000
155:		if err := m.purgeKeyIdx(ctx, tc.Name, cutoff); err != nil {
174:func (m *Manager) purgeQueue(ctx context.Context, topic string, q uint32, cutoff int64) (int, bool, error) {
183:		msg, err := core.DecodeMessage(v)
218:func (m *Manager) purgeKeyIdx(ctx context.Context, topic string, cutoff int64) error {
227:	err := m.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
242:		return n < maxPurgePerQueue, nil
$ grep -n "func KeyIdxKey" internal/store/keys.go
242:func KeyIdxKey(topic, key string, storeMs int64, queueID uint32, offset uint64) []byte {
$ grep -rn "KeyIdxKey(" --include='*.go' internal | grep -v _test | grep -v keys.go
internal/core/produce/produce.go:194:		b.Set(store.KeyIdxKey(m.Topic, key, m.StoreAtMs, m.QueueID, m.Offset), nil)
internal/core/produce/produce.go:433:			b.Set(store.KeyIdxKey(topic, key, m.StoreAtMs, qid, m.Offset), nil)
$ grep -rn "b.Set(store.MsgKey" --include='*.go' internal | grep -v _test
internal/core/produce/produce.go:187:	b.Set(store.MsgKey(m.Topic, m.QueueID, m.Offset), raw)
internal/core/produce/produce.go:428:		b.Set(store.MsgKey(topic, qid, m.Offset), raw)
$ grep -n "MaxSizePerMsg:\|maxFrameLen = 16 << 20" internal/cluster/group.go internal/cluster/transport.go
internal/cluster/group.go:374:		MaxSizePerMsg:   1 << 20,
internal/cluster/transport.go:52:	maxFrameLen = 16 << 20
$ sed -n 186,191p internal/cluster/snapstream.go
		topic, qid, err = store.ParseCursorTopicQueue(k)
	case bytes.HasPrefix(k, []byte(inflightPrefix)):
		_, topic, qid, _, err = store.ParseInflightKey(k)
	default:
		return 0, fmt.Errorf("键 %q 不属于任何数据键族", k)
	}
$ grep -n "rm.Run(retCtx)" cmd/sq/main.go
471:	go func() { defer retWG.Done(); rm.Run(retCtx) }()
$ grep -n "retWG.Wait\|retCancel()" cmd/sq/main.go
472:	defer func() { retCancel(); retWG.Wait() }()
$ grep -rn "retention_ms" internal/admin/*.go | grep -v _test | head -3
internal/admin/topics.go:22:	RetentionMs int64  `json:"retention_ms"`
internal/admin/topics.go:44:		RetentionMs int64  `json:"retention_ms"`
internal/admin/topics.go:50:		s.httpError(w, http.StatusBadRequest, "retention_ms 不能为负，得到 %d（0 表示用默认）", req.RetentionMs)
$ grep -n "func (m \*Meta) UpdateTopicRetention" internal/core/meta/meta.go
306:func (m *Meta) UpdateTopicRetention(ctx context.Context, name string, retentionMs int64) (TopicConfig, error) {
$ grep -n "msgs, err = query.ByKey" internal/admin/messages.go
75:		msgs, err = query.ByKey(s.st, topic, key, int(limit64))
$ grep -n "PATCH /admin/topics" internal/admin/server.go
97:	s.mux.HandleFunc("PATCH /admin/topics/{name}", s.protected(s.handleTopicPatch))
```
