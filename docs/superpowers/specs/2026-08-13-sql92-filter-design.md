# SQL92 属性过滤设计

- 日期：2026-08-13
- 状态：设计已确认，待 writing-plans
- Backlog：B13.1（属 B13「v1.1：官方 SDK 全兼容收口」epic）
- 前置：M2 的 Tag 过滤已落地（`internal/core/deliver/filter.go`），过滤位点语义（跳过即永久越过位点）已由 deliver 主流程实现并有用例；本 spec 复用该位点，不改变它

## 1. 背景与目标

### 1.1 现状

sq 的服务端订阅过滤只支持 TAG 表达式（`"*"` / 单 tag / `a || b`）。`internal/rpc/receive.go` 对任何非 `FilterType_TAG` 的过滤请求直接返回 `ILLEGAL_FILTER_EXPRESSION`：

```go
if fe.GetType() != pb.FilterType_TAG {
    return ... errStatus(pb.Code_ILLEGAL_FILTER_EXPRESSION, "仅支持 TAG 过滤（SQL92 计划 v1.1）")
}
```

后果是官方 SDK 的 SQL92 订阅（Java 的 `new FilterExpression(sql, SQL92)`、Go 的 `NewFilterExpressionWithType(expr, SQL92)`）在 sq 上必挂。这是 B13 epic 盘点出的**唯一一条用户确定会撞到的功能缺口**——其余缺口要么是官方 SDK 尚无实现（PullConsumer 一族），要么是替代关系（AdminService vs sq 自建 Admin API）。

sq 主 spec（`2026-08-06-sq-mq-design.md` §6）原就把 SQL92 排在 v1.1。

### 1.2 目标

让官方 5.x SDK 的 SQL92 订阅在 sq 上可用，语义覆盖 RocketMQ 官方文档明列的语法，且「表达式写错导致收不到消息」这一场景在服务端可诊断。

### 1.3 非目标

- 不做 RocketMQ 的 `ConsumerFilterManager` + BloomFilter 索引结构（理由见 §2.3）
- 不做 SQL92 之外的过滤类型
- 不改变现有 TAG 过滤的任何行为

## 2. 三条定档决策

### 2.1 兼容基准：文档化子集

**取**：覆盖 RocketMQ 官方文档明列的语法，语义以文档为准；文档未写清的边界（类型强转细节、NULL 传播细节）由本 spec 自行定义并写死。

**舍**：bug-for-bug 复刻 Apache RocketMQ 的 Java 实现。它的怪癖没有文档，只能读源码反推，且要逐条建对照用例，成本显著更高；收益（存量表达式行为分毫不差）对 sq 的目标用户（中小团队新建部署）价值有限。

**风险留痕**：与 Java broker 的极端边界行为可能有细微差异。若未来出现真实迁移用户报差异，按个案对齐，不预先投入。

### 2.2 类型与 NULL：常量驱动类型 + SQL 三值逻辑

消息属性在协议里全是字符串（`core.Message.Properties map[string]string`），没有类型信息。**由表达式里的常量决定怎么解释属性**；解释不出来（属性缺失或格式不符）则该次比较为 UNKNOWN，按 SQL 三值逻辑传播，最终非 TRUE 即不投递。

**舍二值逻辑**（不可解析即 false）的理由：`NOT (age > 10)` 在 `age` 缺失时会变成 true，把用户明确不想要的消息投出去，与 SQL 直觉相反，且这类 bug 在生产中极难归因。

**舍「类型不匹配即报错给客户端」**的理由：表达式本身合法，问题在数据；一条脏消息不该让整个消费组停摆。且 `ILLEGAL_FILTER_EXPRESSION` 用来报数据问题在语义上不诚实。可诊断性改由 §7 的分桶计数解决。

### 2.3 无配置开关，直接启用

RocketMQ broker 的 `enablePropertyFilter` 默认为 false，**sq 不照抄这个默认值**。

理由是两边的成本结构不同：RocketMQ 为 SQL92 在 ConsumeQueue 侧维护 `ConsumerFilterManager` + BloomFilter 索引，好处是不读消息体即可过滤，代价是一整套索引结构要建要维护——它默认关掉的是这个。sq 没有那层结构：`deliver.receiveOnceLocked` 的扫描回调**本来就要 `core.DecodeMessage` 解出整条消息**（TAG 过滤也在解完之后执行），SQL92 的增量成本只是一次 map 查找加几次比较。

因此：不引入配置项，不引入关闭分支。防病态输入靠 §8 的表达式上限，不靠开关。这与 sq「配置全可省略、开箱即用」的定位一致，也避免了「目标是官方 SDK 全兼容，却给自己设一道默认关闭的门」这一自相矛盾。

## 3. 语法子集

### 3.1 文法

优先级由低到高分层，递归下降逐层对应：

```ebnf
expr      := orExpr
orExpr    := andExpr { OR andExpr }
andExpr   := notExpr { AND notExpr }
notExpr   := [ NOT ] primary
primary   := '(' expr ')'
           | identifier IS [ NOT ] NULL
           | identifier [ NOT ] BETWEEN constant AND constant
           | identifier [ NOT ] IN '(' constant { ',' constant } ')'
           | operand compareOp operand
compareOp := '=' | '<>' | '>' | '>=' | '<' | '<='
operand   := identifier | constant
constant  := number | string | TRUE | FALSE | NULL
```

`IS NULL` / `BETWEEN` / `IN` 三种形态的左侧**只接受属性名**，不接受常量——`1 IS NULL`、`'a' IN ('a')` 是恒定值的退化写法，没有使用价值，在文法层直接排除比留到构建期再拒更干净。只有 `operand compareOp operand` 需要 `operand` 放宽到常量，理由见 §3.3。

**不接受 `!=`**：RocketMQ 文档用 `<>`，本 spec 的基准是文档化子集（§2.1），不擅自扩展同义写法。`!=` 会被词法器报为未知 token。

### 3.2 词法规则

| 规则 | 定义 |
|---|---|
| 关键字大小写 | 不敏感（`and` / `AND` / `And` 等价）。关键字集：`AND OR NOT BETWEEN IN IS NULL TRUE FALSE` |
| 属性名大小写 | **敏感**（`age` 与 `Age` 是两个属性） |
| 属性名字符集 | 首字符为字母或 `_`，后续为字母、数字或 `_` |
| 字符串常量 | 单引号包围；内部 `''` 表示一个单引号字符 |
| 数值常量 | 十进制整数或小数，可带前导 `-`。不支持科学计数法、十六进制 |
| 空白 | 空格、制表符、换行等价，可出现在任意 token 之间 |

### 3.3 两条结构约束

**`BETWEEN x AND y` 的 `AND` 必须由 BETWEEN 分支自己消费**，不能被 `andExpr` 层抢走。递归下降天然满足（BETWEEN 分支解析完下界后直接消费 `AND` token 再解析上界），但这是本解析器最经典的错处，必须有专门用例（§9.1）。

**`operand compareOp operand` 允许两种顺序**：`age > 10` 与 `10 < age` 都合法（SQL 书写习惯，递归下降代价为零）。但构建期拒绝两种退化形态：

- 两侧都是常量（`1 = 1`）：无意义
- 两侧都是属性名（`a = b`）：属性间比较的类型规则无法定义，RocketMQ 也不保证此语义

## 4. 语义

### 4.1 三值逻辑真值表

| AND | T | F | U |
|---|---|---|---|
| **T** | T | F | U |
| **F** | F | F | F |
| **U** | U | F | U |

| OR | T | F | U |
|---|---|---|---|
| **T** | T | T | T |
| **F** | T | F | U |
| **U** | T | U | U |

| NOT | 结果 |
|---|---|
| T | F |
| F | T |
| **U** | **U** |

**只有整个表达式求值为 TRUE 才投递。** FALSE 与 UNKNOWN 都不投递，但二者在计数上必须分开（§7.1）。

`NOT UNKNOWN = UNKNOWN` 是这套逻辑相对二值逻辑的核心差异点，也是实现最容易退化回二值的地方，需独立用例守住。

### 4.2 类型规则（常量驱动）

属性值一律是字符串，由参与比较的常量决定解释方式：

| 常量形态 | 属性解释方式 | 允许的运算 | 解释失败时 |
|---|---|---|---|
| 数值（`10`、`3.14`） | 数值 | `= <> > >= < <=`、`BETWEEN`、`IN` | UNKNOWN |
| 字符串（`'abc'`） | 原样字符串 | `= <> IN` | 不适用（字符串总能解释） |
| `TRUE` / `FALSE` | 布尔：属性值为 `"true"` / `"false"`，**大小写不敏感** | `= <>` | UNKNOWN |

**字符串不支持大小比较**（与 RocketMQ 文档一致）。因常量侧类型在构建期已知，`k > 'abc'` 在**构建期**即可判定并拒绝，不必等到求值。

**数值比较分两档，不得一律用 float64**：

1. 若属性值与常量**都能解析为 `int64`** → 走精确整数比较
2. 否则 → 双方转 `float64` 比较

理由：消息属性中放订单号、雪花 ID 极常见，而 float64 在 2^53 以上丢精度——一律转 double 会让两个不同的雪花 ID 判定为相等。这是静默的错误匹配，比不匹配更坏。

`BETWEEN` 与 `IN` 的类型规则等同于其展开形式：`a BETWEEN x AND y` 等价 `a >= x AND a <= y`，`a IN (x, y)` 等价 `a = x OR a = y`（含三值逻辑传播）。

**`BETWEEN` 的上下界与 `IN` 的列表元素，其常量类型必须一致**，混用（`k IN (1, 'a')`、`k BETWEEN 1 AND 'z'`）在构建期报错。理由：常量驱动类型的前提是「常量侧类型唯一」，混用会让同一个属性在同一个表达式里被要求同时按两种方式解释，无法给出可解释的语义。

### 4.3 NULL 与 `IS NULL`

- 属性不存在 → 任何比较（除 `IS NULL` / `IS NOT NULL`）结果为 UNKNOWN
- `k IS NULL` → 属性不存在时 TRUE，存在时 FALSE。**永不返回 UNKNOWN**
- `k IS NOT NULL` → 与上相反
- **`k = NULL` 在构建期报错**，错误信息给出 `请改用 k IS NULL`

最后一条是相对 SQL 标准的**有意偏离**（标准中它合法且恒为 UNKNOWN）。理由：合法但恒不成立的表达式对用户是纯陷阱，而这里服务端有能力在入口就说清楚。此偏离需写入 README。

### 4.4 可过滤的属性来源

| 来源 | 说明 |
|---|---|
| `m.Properties` 全部键 | 生产者设置的用户属性 |
| `TAGS` → `m.Tag` | RocketMQ 把 tag 作为可过滤属性暴露，而 sq 的 tag 是独立字段不在 Properties 里，需显式接上 |

`TAGS` 因此成为**保留属性名**：同名用户属性被系统映射遮蔽。此行为需写入 README。

**不映射 `KEYS`**：sq 的 `Keys` 是 `[]string` 多值，塞进单值比较语义含糊（`KEYS = 'x'` 是「其中之一等于」还是「整体等于」无法自然确定）。按 key 检索有 Admin API 的 keys 索引，不需要走过滤器。

### 4.5 不变的两条投递语义

**位点语义与 TAG 过滤完全一致**：不匹配的消息跳过并永久推进本组 fetch 位点，不投递、不占 inflight。SQL92 求值插在与 TAG 过滤相同的位置——**顺序锁判定之前**。理由与 TAG 相同：放在顺序锁之后，被锁拦住的不匹配消息会永远堵住位点。

**重投不重新过滤**：`receiveOnceLocked` 阶段 1 的过期 inflight 重投不经过滤器（`Deliverer.Receive` 的文档注释已明写此现状）。已投递过即说明当时命中，中途更改订阅表达式不追溯。

## 5. 组件与接线

### 5.1 文件划分

| 文件 | 状态 | 职责 |
|---|---|---|
| `internal/core/deliver/filter.go` | 改 | `Filter` 接口、`AllPass` 单例、`ParseFilter` 入口；`TagFilter` 保留并对齐新签名 |
| `internal/core/deliver/sql92_parse.go` | 新 | 词法 + 递归下降语法 → AST；构建期校验与上限检查 |
| `internal/core/deliver/sql92_eval.go` | 新 | AST 三值求值、类型强转规则、属性来源映射 |

解析与求值拆两个文件：二者是独立关注点，各自可单独测（解析测「文本 → AST / 错误」，求值测「AST + 属性 → 真值」），合计体量也已到该拆的程度。

### 5.2 接口

```go
// Result 过滤判定的三值结果。只有 ResultTrue 才投递。
type Result uint8

const (
    ResultTrue    Result = iota // 命中
    ResultFalse                 // 明确不命中
    ResultUnknown               // 无法判定（属性缺失或类型解释失败）
)

// Filter 服务端订阅过滤器。实现必须不可变（同一实例跨消息复用）。
type Filter interface {
    Match(m *core.Message) Result
}
```

**`Match` 返回三值而非 `bool`，是 §7.1 的诊断能力所必需的**：跳过计数要把 FALSE 与 UNKNOWN 分桶，而 `bool` 把这两者压成同一个值，调用方再也无从区分。语义上二者也确实不同——「明确不匹配」和「没法判断」是两件事，投递决策上一致（都不投）不代表可观测性上可以合并。

`TagFilter.Match(tag string)` 改为 `Match(m *core.Message) Result`，只返回 `ResultTrue` / `ResultFalse`（tag 匹配不存在无法判定的情形）。`deliver` 扫描循环中是一行 `if r := filter.Match(m); r != ResultTrue`，随后按 `r` 与过滤器种类累加对应的 reason 计数，无类型判断。

### 5.3 用 `AllPass` 单例取代 nil 过滤器

现状 `ParseTagFilter` 对 `"*"` 返回 `(nil, nil)`，靠 `*TagFilter` 的 nil receiver 方法支撑「nil 匹配一切」。

**换成接口后这个模式必须废弃**：`var f *TagFilter = nil` 赋给 `Filter` 会产生 typed-nil——`i != nil` 为真，方法调用仍需 nil receiver 兜底。这种代码能跑，但在加入第二个实现时极易踩中。

因此：`"*"`、空表达式、请求未带 `FilterExpression` 三种情况一律返回 `AllPass{}` 单例，**`deliver` 永远拿不到 nil `Filter`**，nil receiver 依赖整个删除。

### 5.4 解析入口下沉到 deliver

```go
func ParseFilter(kind FilterKind, expr string) (Filter, error)
```

`FilterKind` 是 deliver 自己的枚举（`FilterTag` / `FilterSQL92`），**不是 `pb.FilterType`**——`internal/core` 不 import 任何 pb 包是既有架构约束（`internal/core/types.go` 包头注释明写）。

协议层 `internal/rpc/receive.go` 负责 `pb.FilterType → deliver.FilterKind` 映射：`FilterType_TAG → FilterTag`，`FilterType_SQL → FilterSQL92`，其余（含未指定）仍回 `ILLEGAL_FILTER_EXPRESSION`。原先硬编码的 TAG 分支收缩为一次映射加一次 `ParseFilter`，两种过滤走同一条路径。

`deliver.Receive` 及其内层 `receiveOnce` / `receiveOnceLocked` 的 `filter *TagFilter` 参数改为 `filter Filter`。

## 6. 错误处理与核心不变式

### 6.1 不变式：构建期报错，求值期永不报错

`Filter.Match(m *core.Message) Result` **没有 error 返回值**，这是硬约束而非风格偏好。

求值发生在 `receiveOnceLocked` 的扫描回调内，该回调**持有队列锁**，且返回 error 会中断整趟扫描并让 `Receive` 整体失败。若求值可报错，一条属性格式异常的脏消息就能让该消费组在该队列上停摆——而它本应只是「这条不匹配，跳过」。

因此类型解析失败一律吸收为 UNKNOWN（§4.2），**所有可提前判定的错误必须在构建期抓住**。构建期拒绝清单：

| 拒绝项 | 示例 |
|---|---|
| 语法错误 | `a = ` |
| 字符串大小比较 | `k > 'abc'` |
| `= NULL` | `k = NULL` |
| 常量对常量 | `1 = 1` |
| 属性对属性 | `a = b` |
| `IN` / `BETWEEN` 常量类型混用 | `k IN (1, 'a')` |
| 超出上限（§8） | 表达式过长 / 嵌套过深 / IN 列表过大 |

### 6.2 错误信息形态

构建期错误带列号，原样进入 `ILLEGAL_FILTER_EXPRESSION` 的 message 回给客户端：

```
第 14 列：BETWEEN 需要 AND 连接上下界，实际读到 ')'
```

列号以 UTF-8 字节偏移加一计（表达式为 ASCII 时即直观列号）。

## 7. 可观测性

### 7.1 分桶跳过计数——本项的核心诊断能力

「表达式语法合法但一条消息都收不到」有两类原因，且**排查方向完全相反**：

| 现象 | 含义 | 该查哪里 |
|---|---|---|
| 全 FALSE | 表达式正确，只是没有匹配的消息 | 生产者发了什么 |
| 全 UNKNOWN | 属性名拼错，或类型对不上（`age="abc"` 撞 `age > 10`） | 表达式本身 |

服务端能分辨，客户端不能。因此跳过计数按原因分桶：

```
sq_filter_skipped_total{topic, group, reason}
reason ∈ { tag_miss | sql_false | sql_unknown }
```

`sql_unknown` 持续增长而 `sql_false` 为 0，基本实锤属性名拼错或类型不匹配。这是把「静默零命中」从玄学变为一眼可判的关键，也是三值逻辑除语义正确外的第二个红利——二值逻辑下这两类会混成同一个数，永远分不开。

指标命名与既有 `sq_*_total` 计数器一致（参照 `sq_topic_messages_written_total`、`sq_txn_checks_total`）。

### 7.2 日志

| 位点 | 级别 | 内容 |
|---|---|---|
| `ParseFilter` 成功 | Debug | group / topic / kind / expr |
| `ParseFilter` 失败 | Warn | group / topic / expr / 具体错因（复用现有 TAG 分支形态） |
| 扫描跳过汇总 | Debug | group / topic / queue / 三个 reason 的计数 |

跳过日志维持现状的**每趟扫描汇总一行**，不逐条打——扫描是投递热路径。现有的「Tag 过滤跳过消息」Debug 行扩成带 reason 分项。

### 7.3 不做控制台页面

metrics 足以定位问题，控制台加一页要付前端加原型对照的成本，而该排查场景的受众是运维与开发，看 Prometheus 更自然。

## 8. 表达式上限

替代配置开关的兜底（§2.3）：

| 限制 | 阈值 |
|---|---|
| 表达式长度 | 1024 字节 |
| AST 深度 | 16 层 |
| 单个 IN 列表元素数 | 64 |

超限返回 `ILLEGAL_FILTER_EXPRESSION`。三个阈值均远高于任何正常表达式，卡的是失手或恶意写出的病态输入。阈值为编译期常量，不做配置项。

## 9. 测试策略

### 9.1 解析器单测（`sql92_parse_test.go`，表驱动）

合法输入断言 AST 形状；非法输入断言**具体错误**而非「有错即可」。必须覆盖：

- **`BETWEEN` 的 `AND` 归属**：`a BETWEEN 1 AND 2 AND b = 3` 必须解析为 `(a BETWEEN 1 AND 2) AND (b = 3)`（§3.3 点名的陷阱）
- 优先级 `NOT > AND > OR`：`a = 1 OR b = 2 AND c = 3` 的结合形状
- 括号改变优先级
- 字符串 `''` 转义；字符串内含关键字（`k = 'A AND B'` 不得被切开）
- 关键字大小写不敏感、属性名大小写敏感
- §6.1 构建期拒绝清单逐项（含 `IN` / `BETWEEN` 常量类型混用）
- 文法层排除项：`1 IS NULL` 这类常量左侧、`!=` 未知 token

### 9.2 求值单测（`sql92_eval_test.go`）

- **三值真值表逐格**：AND 9 格、OR 9 格、NOT 3 格全部显式覆盖。`NOT UNKNOWN = UNKNOWN` 单列一条
- 类型三档各自的命中 / 不命中 / 解释失败
- **int64 精度边界**：两个相差 1 且均大于 2^53 的雪花 ID，`=` 必须判不等。此条是 §4.2 分档决策的看门用例，退回纯 float64 必红
- `TAGS` 映射到 `m.Tag`；同名用户属性被遮蔽
- 属性缺失 → UNKNOWN；`IS NULL` / `IS NOT NULL` 正确
- `BETWEEN` / `IN` 与其展开形式等价

### 9.3 deliver 集成（扩 `deliver_test.go`）

- SQL92 不匹配 → 跳过、推进位点、不占 inflight（与 TAG 同款断言）
- 求值在顺序锁判定之前
- 重投不重新过滤
- **三个 reason 计数正确**——§7.1 的诊断能力靠它，不测等于没有

### 9.4 协议层（扩 `internal/rpc/receive_test.go`）

`pb.FilterType_SQL` 正确映射为 `FilterSQL92`；非法表达式回 `ILLEGAL_FILTER_EXPRESSION` 且 message 带列号；未知 FilterType 仍拒。

### 9.5 e2e（`test/e2e/sdk_sql92_test.go`，新）

用官方 Go SDK 的 `NewFilterExpressionWithType(expr, golang.SQL92)` 起 SimpleConsumer（v5.1.4 已支持，映射到 `v2.FilterType_SQL`），单 topic 混发带不同属性的消息，断言只收到命中的那批：

- 数值比较 + `BETWEEN`
- 字符串 `IN`
- `AND` / `OR` / `NOT` 组合
- 属性缺失的消息不被投递（三值逻辑的端到端证据）

**一个必须避开的 SDK 坑**：Go SDK 的 `FilterExpressionType` 中 `SQL92 = iota = 0`，是该类型的零值；`NewFilterExpression()` 才显式设 `TAG`。写用例时若忘记传类型参数，拿到的是 SQL92 而非 TAG——该默认值反直觉，会造成「以为在测 TAG，实际在测 SQL」。

## 10. 明确不做

- `LIKE` 及任何模式匹配
- 算术运算 `+ - * /`
- 函数调用、子查询
- 字符串大小比较（`>` `<` `>=` `<=` 作用于字符串常量）
- 属性之间的比较
- 裸标识符作布尔条件（须写 `k = TRUE`）
- 跨请求的编译结果缓存：每次 `ReceiveMessage` 入口解析一次，该 RPC 内所有消息共用同一棵 AST。一次长轮询最多返回 `maxMsgs` 条且可能阻塞至 20 秒，相比之下解析一个几十字节表达式的开销可忽略；加 LRU 需处理容量、失效、并发三件事，收益测不出来
- SQL92 的 BloomFilter / ConsumeQueue 索引优化（§2.3）

## 11. 验收标准

1. `go test -race ./...` 全绿（含新增的 `sql92_parse_test.go`、`sql92_eval_test.go` 与扩充的 deliver / rpc 用例）
2. `test/e2e` 全量绿，含新增 `sdk_sql92_test.go` 的四组断言
3. 官方 Go SDK 用 `SQL92` 类型订阅可正常收到命中消息，未命中消息不投递且位点已推进（e2e 证据）
4. TAG 过滤的既有行为与用例零回归
5. README 补三处：SQL92 支持的语法子集、`TAGS` 保留属性名、`= NULL` 相对 SQL 标准的有意偏离
6. `/metrics` 暴露 `sq_filter_skipped_total` 且三个 reason 分桶可分别观测到非零值（由 deliver 集成用例覆盖）
