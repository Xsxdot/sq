# B13.4：RecallMessage——撤回未到期的定时消息

> 关联 backlog：B13.4（属 B13 epic）
> 日期：2026-08-14

## 1. 背景与缺口

RocketMQ 5.3+ 新增 `RecallMessage`：撤回一条已发出、但尚未到投递时间的定时
消息。Java SDK 已有 `producer.recall()`。sq 未实现，`internal/rpc/server.go:38`
内嵌的 `UnimplementedMessagingServiceServer` 使客户端调用得到 `UNIMPLEMENTED`。

协议面（`proto/apache/rocketmq/v2/service.proto:316-324`）：

```protobuf
message RecallMessageRequest {
  Resource topic = 1;
  // Refer to SendResultEntry.
  string recall_handle = 2;
}
message RecallMessageResponse {
  Status status = 1;
  string message_id = 2;
}
```

`recall_handle` 来自 `SendResultEntry.recall_handle`（字段 5，注释
"Unique handle to identify message to recall, support delay message for now"）
——**服务端必须先在 SendMessage 的响应里签发这个句柄**，客户端才有东西可撤。
sq 目前 `internal/rpc/send.go:159` 构造 `SendResultEntry` 时不填该字段。

所以本项是两件事：**签发句柄**（发送侧）+ **兑现句柄**（撤回侧）。

## 2. 现场事实（不要重新盘）

- 延时消息暂存键：`store.DelayKey(dueMs, seq)` = `delay/{dueMs:8B}{seq:8B}`，
  值为完整编码消息（`internal/store/keys.go:179`）。撤回 == 删掉这个键。
- `produce.AppendDelay`（`internal/core/produce/produce.go:254`）分配 `seq`
  并写入，但**返回值不含 seq**——返回的 `*core.Message` 在暂存态没有
  QueueID/Offset，也没有 seq。签发句柄需要它。
- 调度器 `internal/core/delay/delay.go`：
  - **只在 meta leader 上跑**（`非 meta leader，delay 趟跳过`，第 140 行附近）。
  - 每趟扫 `[DelayPrefix, DelayScanUpperBound(now))`，即 `due <= now` 的条目。
  - **两段式移入**：第一段写 `msg/`（分配队列与 offset、唤醒长轮询），
    第二段**独立批次**删 delay 条目。先写后删，次序不得反转。
- 句柄签发惯例：`internal/rpc/receipt.go:50` 的 `receiptEncode` ——
  `base64(json(payload)) + sep + base64(hmac_sha256(secret, json))`，密钥来自
  `LoadOrCreateHandleSecret`（单机）/ `...Replicated`（集群）。
- 错误映射：`internal/rpc/server.go:166` 的 `topicErrStatus` 已覆盖
  `ErrNotLeader → HA_NOT_AVAILABLE`（语义「这节点没坏，你该去问 leader」）。
  `RecallMessageRequest` 带 `topic`，可直接复用，无需新增映射。

## 3. 承重问题：撤回与调度器的竞态

这是本设计的核心，也是「工作量小」这个原始判断不成立的原因。

调度器是**两段式**的：第一段把消息写进 `msg/`（此刻它已可被消费），第二段才
删除 delay 条目。**在两段之间，delay 条目仍然存在，但消息已经投出去了。**

若撤回实现成朴素的「键还在就删掉、返回成功」，落在这个窗口里就会给客户端一个
**假成功**：客户端以为撤回了，消费者却已经收到。撤回语义下这是致命的——
定时消息的典型用途是「超时未支付则关单」，假成功意味着订单被误关。

### 3.1 关闭窗口的三道独立闸门

> **本节已订正：由两道改为三道。** 初稿只写了闸门一（互斥）与闸门二（时间），
> 并断言「`due > now_recall` 的条目要被某趟收集必须 `now_scan >= due > now_recall`，
> 即那趟扫描发生在撤回判定**之后**；有闸门一定序，二者不会交错」。
> **这个论证是错的**，因为 `Pass` 的 `Scan` 发生在互斥量**之外**——两次
> `time.Now()` 读取之间没有任何同步原语，闸门一从未与扫描定序。
> 可复现的假成功交错：
>
> 1. `Recall` 持锁，读 `now_recall`，`dueMs > now_recall` → 过闸门二
>    （此后它还要持锁跨一次 `rep.Apply`，即一次 fsync/quorum，是毫秒级的持锁）；
> 2. 调度器**无锁**执行 `Scan`，读到 `now_scan >= dueMs`（只需比 `now_recall`
>    晚一点点），条目此刻仍在盘上 → 连同 `raw` 快照进 `dues`；
> 3. `Recall` 的删除提交成功、返回 nil → 客户端收到**撤回成功**；
> 4. `moveOne` 拿到锁，照着步骤 2 的**陈旧 raw** 走 `pr.Append` → 消息入队被投递。
>
> 补第三道闸门（锁内重读）才真正关上。

**闸门一（互斥）**：把调度器**每个条目**的两段移入用一把互斥量包住
（不是整趟——一趟最多 `maxMovePerPass` 条且含跨节点转发，整趟持锁会长时间
阻塞撤回），撤回取同一把锁。撤回因此只能在条目与条目**之间**插入，永远不会
落在某个条目的两段之中。

**闸门二（锁内重读）**：`moveOne` 拿到锁后必须先 `st.Get(key)` 确认条目**仍在**，
不在则跳过本条。这是把「`Scan` 在锁外」这个事实兜住的那一道：拷贝出 `key/raw`
之后、`moveOne` 拿到锁之前，撤回可能已经删掉条目并向客户端返回了成功。条目还
在不在，只有在锁内重读才算数。经互斥量建立的 happens-before 保证重读一定能看见
撤回已提交的删除。（`raw` 仍可直接用于解码：delay 条目一经写入不再改写，键在则
内容不变。）

**闸门三（时间）**：撤回额外要求 `dueMs > now`。它挡的是另一类残留——第二段
删除失败时消息已入队而条目仍在，此时条目的 `due` 必已落在过去，撤回据此拒绝，
不会去撤一条已经投出去的消息。

三道缺一不可：没有闸门一，删除与搬运可以逐字节交错；没有闸门二，上面那条
「扫描在锁外」的路径原样成立；没有闸门三，第二段失败留下的残条目会被误判为
「可撤回」。

### 3.2 撤回必须在 meta leader 上执行

闸门一是**进程内**互斥量，只有当撤回与调度器在同一进程才有效。调度器只在
meta leader 上跑，所以撤回也必须在 meta leader 上执行。

值得注意的是：删除提案本身**不需要**在 leader 上发起（raft 的 Propose 在
follower 上会转发给 leader），所以「能不能写」不是约束，「能不能与调度器互斥」
才是。这一点容易看反。

见 §7 A2：集群档下由此产生一处已知限制。

## 4. 方案

### 4.1 句柄：`internal/rpc/recall.go`

新增 `recallEncode` / `recallDecode`，形态与 `receipt.go` 逐字对齐
（`base64(json) + sep + base64(hmac)`，共用 `s.handleSecret`），负载为
`{t: topic, d: dueMs, s: seq}`。

**域分隔（必须做）**：recall 句柄的 HMAC 计算在负载前拼一个固定域前缀
（如 `"sq-recall\x00"`），receipt 句柄没有前缀。理由：两类句柄共用同一把密钥，
若不做域分隔，一个合法的 receipt 句柄（负载 `{"g":..,"t":..,"q":..,"o":..,"a":..}`）
拿去做 recall 解码会**验签通过**，然后 JSON 宽松解码把未知字段丢掉、缺失字段
补零，得到 `t=该 receipt 的 topic, d=0, s=0`——一个签名有效、语义完全错位的
伪句柄。

> **本段理由已订正。** 初稿写「一个指向 `delay/{0}{0}` 的伪句柄，空库里第一条
> 延时消息的键恰好就是它」。**这是错的**：`DelayKey` 的第一分量是 epoch 毫秒，
> 空库第一条延时消息的键是 `delay/{真实 due}{0}`，`{0}{0}` 要求
> `DeliverAtMs == 0`，而 `AppendDelay` 开头就拒绝 `DeliverAtMs <= 0`。伪句柄的
> `d=0` 实际会先撞闸门三（`dueMs <= now`）→ `ErrRecallTooLate`，删不到东西。
>
> 域分隔**仍然该做**，但真实理由是：不做域分隔时，句柄的完整性就依赖一个
> **下游**判据（闸门三）没被人改动。域分隔让这种混淆在验签这一层——最早的
> 一层——就失败。理由要说真话，否则后人一验证就会发现注释是错的，进而怀疑
> 整个约束。

反向不需要额外处理：recall 句柄的签名是对「前缀+负载」算的，拿去 `receiptDecode`
（对裸负载算）必然验签失败。

### 4.2 发送侧：`AppendDelay` 暴露 seq

签名改为：

```go
func (p *Producer) AppendDelay(ctx context.Context, m *core.Message) (stored *core.Message, seq uint64, staged bool, err error)
```

> **本节已订正。** 初稿写的是三返回值 `(*core.Message, uint64, error)`，**不可用**。
> `AppendDelay` 开头有一条直通分支：`m.DeliverAtMs <= now` 时消息**不进暂存区**，
> 直接走 `p.Append`——此时没有 seq、也无条目可撤。三返回值形态下调用方只能拿
> `seq == 0` 当「没暂存」的哨兵，而 `nextDelaySeqLocked` 在计数器不存在时返回 **0**，
> 所以空库里第一条延时消息的 seq 就是 0，是个**合法值**。用 0 当哨兵会让它永远
> 签不出句柄。故必须多一个 `staged bool`。

生产调用点仅两处（`internal/rpc/send.go:143`、`internal/admin/messages.go:146`），
加测试共约 8 处。

**为什么改签名而不是把 seq 挂到 `core.Message` 上**：`core.Message` 是会被
`EncodeMessage` 持久化的结构，刚做完消息 v1 二进制编码，加字段等于动存储格式。
而改签名是**编译期可见**的——这正是 B13.1「接口化地雷」（`*TagFilter`→`Filter`
编译期无感、运行期 panic）的反面教材所要求的方向。

`send.go` 拿到 seq 后填 `SendResultEntry.RecallHandle`。
**只有延时分支填**：事务半消息与普通消息不可撤回，字段留空（proto3 省略）。

### 4.3 撤回侧：`delay.Scheduler.Recall`

撤回逻辑放在 `internal/core/delay`——它必须与调度器共用互斥量，而互斥量属于
调度器。放 produce 或新包都会把这把锁暴露出去。

```go
// Recall 撤回一条尚未到期的延时消息。返回被撤回消息的 ID。
func (s *Scheduler) Recall(ctx context.Context, topic string, dueMs int64, seq uint64) (string, error)
```

判据与返回，按序。**判据 1 在锁外，判据 2–4 与随后的删除全部在同一次持锁期间
完成**：

| # | 判据 | 位置 | 不成立时 |
|---|---|---|---|
| 1 | 本节点是 meta leader | 锁外 | `replication.ErrNotLeader` |
| 2 | `dueMs > time.Now().UnixMilli()` | 持锁 | `ErrRecallTooLate` |
| 3 | `DelayKey(dueMs, seq)` 存在 | 持锁 | `ErrRecallNotFound` |
| 4 | 解码出的消息 `m.Topic == topic` | 持锁 | `ErrRecallTopicMismatch` |

四条全过 → **仍在锁内**构造单批次 `b.Delete(DelayKey(dueMs, seq))`，经
`rep.Apply(ctx, rt.MetaGroup(), b)` 提交（与 `AppendDelay` 写入、调度器删除
走的是同一条路径），确认成功后再释放锁，返回 `m.ID`。

**删除必须在持锁期间完成，不能「判定完释放锁再删」**——那样会把 §3 的窗口原封
不动地重新打开：判定说「还没到期、条目还在」，释放锁后调度器插进来把它搬走，
撤回的删除落到一个已经被搬走的键上（删除空键不报错），于是返回成功而消息已经
投出去了。这正是本设计要消灭的那个假成功。

代价是持锁跨越一次复制提交（集群档含 quorum 往返）。这与调度器持锁跨越一次
跨节点转发追加是同一量级，可接受；两者都不在消息收发热路径上。

判据 4 是纵深防御：句柄已验签，topic 理应一致；但请求体的 topic 与句柄内的
topic 不一致说明客户端拿错了句柄，宁可拒绝也不要跨 topic 删除。这条与
`receipt.go` 文件头「Ack 只信 handle，不信请求体」的思路一致——差别在于
recall **两者都要求**，因为 `RecallMessageRequest.topic` 是协议必填字段，
不校验等于留一个无声不一致。

### 4.4 协议层：`internal/rpc/recall.go` 的 handler

`func (s *Server) RecallMessage(ctx, req) (*pb.RecallMessageResponse, error)`：

| 情况 | Code | 理由 |
|---|---|---|
| 句柄缺失 / base64 / 验签 / JSON 任一层非法 | `BAD_REQUEST` | 不用 `INVALID_RECEIPT_HANDLE`：那是消费侧收据句柄的码，用在这里会把排查引向错误的方向 |
| topic 不匹配（判据 4） | `BAD_REQUEST` | 同上，是客户端输入不一致 |
| 条目不存在（判据 3） | `MESSAGE_NOT_FOUND` | 已投递、或已被撤回过 |
| 已过投递时间（判据 2） | `MESSAGE_NOT_FOUND` | **与判据 3 合并成同一个码**：对客户端而言「没赶上」与「已经不在了」行为完全相同，区分只会让客户端多写一条永远走同一分支的代码。服务端日志里两者用不同措辞区分，便于排查 |
| 非 meta leader（判据 1） | `HA_NOT_AVAILABLE` | 复用 `topicErrStatus`，语义「这节点没坏，你该问 leader」 |
| 成功 | `OK` + `MessageId` | |

## 5. 测试计划

**单元（`internal/rpc/recall_test.go`）**
- U1 编解码往返：`recallDecode(recallEncode(...))` 还原 topic/due/seq。
- U2 **域分隔（承重）**：用 `receiptEncode` 造一个合法 receipt 句柄，交给
  `recallDecode` 必须**验签失败**。这条用例删掉之后 §4.1 的整个论证就失效了，
  不是可选断言。
- U3 篡改：改一个 base64 字节，验签失败。

**单元（`internal/core/delay/recall_test.go`）**
- U4 正常撤回：`AppendDelay` 一条 due=now+1h 的消息 → `Recall` 成功、返回正确
  msg ID → 键已不存在 → 再 `Recall` 一次得 `ErrRecallNotFound`（幂等拒绝）。
- U5 已过期：due=now-1s（直接构造 delay 键，绕过 `AppendDelay` 的
  「已到期直通 Append」分支）→ `ErrRecallTooLate`。
- U6 topic 不匹配 → `ErrRecallTopicMismatch`。
- U7 **竞态（承重）**：起一个持续跑 `runOnce` 的调度器，并发对一批
  due=now+50ms 的条目调 `Recall`，断言**不变式**：每条消息要么被撤回成功且
  最终不出现在队列里，要么撤回失败且出现在队列里——**绝不允许「撤回返回成功
  但消息仍被投递」**。这是 §3 的可执行形式。

**e2e（`test/e2e`，仅单机档）**
- E1 发一条 due=now+10s 的定时消息 → 断言 `SendReceipt` 带回非空
  recall handle → 立即 recall → 等 15s，消费端**收不到**该消息。
- E2 发一条 due=now+2s → 等 4s（已投递）→ recall → 断言返回
  `MESSAGE_NOT_FOUND`，且消费端仍能正常收到该消息（撤回失败不得损坏消息）。

E2 的静默段必须**长于一个完整的投递周期**，否则「撤回失败」与「撤回成功但
消费还没轮到」在观测上无法区分——这是 B13.2 终审抓到的那类假绿陷阱。

## 6. 边界与非目标

- 只支持延时消息，与协议注释 "support delay message for now" 一致。事务半
  消息、普通消息不签发 recall handle。
- 不支持批量撤回（协议本身就是单条）。
- 不做撤回审计/留痕（撤回即物理删除条目）。
- 不改动调度器的两段式移入顺序。**先写后删是正确的**（反转 = 崩溃丢消息），
  本设计是绕开它的窗口，不是消除它。

## 7. 需要用户复核的假设

- **A1（范围修正，最重要）**：backlog 原判「工作量小」**不成立**。原判把撤回
  看成「删一个键」，漏掉了两段式移入造成的假成功窗口（§3）。实际范围包含：
  改 `AppendDelay` 签名、新增句柄族与域分隔、给调度器加逐条互斥量、新增
  RPC handler、以及一条并发不变式用例。仍是可控的中等改动，但不是「顺手做掉」。
- **A2（已知限制）**：集群档下，撤回只在 **meta leader** 节点成立，其他节点回
  `HA_NOT_AVAILABLE`。而 SDK 是按 topic 路由 `RecallMessage` 的，并不知道谁是
  meta leader——所以集群部署里撤回会**经常打到错误的节点**。本设计**不新增
  跨节点转发原语**（那是一处真正的架构新增），选择如实暴露限制。
  **单机档不受影响，功能完整。** 若要在集群档可用，需另立一条「撤回转发到
  meta leader」的条目。这条必须让用户看到再决定，不应由我在其外出期间扩权。
- **A3**：「已过期」与「不存在」合并成同一个 `MESSAGE_NOT_FOUND`（§4.4）。
  代价是客户端无法区分二者；收益是不引入一个客户端永远不会分别处理的码。

## 8. 日志与注释要求

- `internal/rpc/recall.go`、`internal/core/delay` 的 recall 部分需文件头/段落
  注释：职责 + 边界。
- **域分隔的理由（§4.1）必须写成代码注释**，否则后人「简化」掉那个前缀时不会
  意识到自己打开了一条伪造路径。
- **逐条互斥量的理由（§3.1）必须写在调度器加锁处**，说明为什么是逐条而不是
  整趟、以及为什么撤回必须取同一把锁。这把锁看起来像是可以「优化」掉的。
- 撤回成功 → Info（含 topic / msg_id / due / seq）：这是有外部影响的状态变更，
  不能静默。
- 四条判据的每个拒绝分支各打一条 Debug 或 Warn，措辞要能区分「没赶上」与
  「不存在」（§4.4 里二者返回同一个 Code，日志是唯一的区分手段）。
- 非 meta leader 分支用 Debug：集群档下这是高频路径（见 `server.go:159` 对
  同类分支的既有理由）。
