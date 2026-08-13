# 消息二进制编码（去 base64）设计

- 日期：2026-08-12
- 状态：已实现、三机配对 bench 验收通过（结果见 §7）
- 前置：`internal/core/types.go` 的 `EncodeMessage/DecodeMessage` 是消息编解码唯一出入口（包头注释明言"若未来需要可替换为二进制编码，Encode/Decode 是唯一出入口"——本 spec 即兑现该预留）

## 1. 背景与目标

### 1.1 现状

消息落盘格式是 JSON，`Body []byte` 经 encoding/json 自动 base64。这带来两笔税：

| 税 | 量化 | 说明 |
|---|---|---|
| 体积税 | Body 部分 ×4/3（+33%） | base64 编码固有膨胀 |
| CPU 税 | 每条消息每节点一次 base64 编/解码 + JSON 全量字符串转义扫描 | Body 越大越贵 |

关键在于这笔体积税**不是付一次，是乘遍整条写路径**。一条消息从 produce 到三副本落盘，编码后的字节要经过：

1. produce 侧 Pebble batch 构造（value = 编码字节）
2. propose 的 batch repr 拷贝（含 16B 提案头重组）
3. raft entry（进 raft 内存 + unstable log）
4. seglog append（写入 + **fsync 字节数**）
5. 网络发送 ×2 follower
6. 三个节点各自的 Pebble WAL + memtable + compaction

Body 主导的消息（业务典型形态），value 体积 −25% 意味着上述每一环的字节量都省 25%。

### 1.2 为什么现在做

2026-08-12 三机归因实测（见 memory `cluster-perf-attribution-2026-08-12`）：

- **mem 档是 CPU-bound**（机器 A CPU 99% 饱和）——省下的 base64/JSON CPU 直接兑换为吞吐；
- **fsync 档是延迟串行化-bound**——每批 fsync 字节数下降，缩短单次 fsync 的写放大，同时网络/复制字节量下降；
- 认证拦截器 6.8% CPU 都排进了本轮优化（子任务 A），编解码税与它同量级且同样集中在热路径。

### 1.3 目标与非目标

**目标**：

- 消除 Body 的 base64 膨胀与编解码 CPU；
- 新旧格式按 value 首字节自动区分，**旧数据零迁移**；
- 滚动升级安全：混版集群不产生任何一个节点解不了的字节。

**非目标**：

- 不改 `InflightState` 等小型 JSON 结构（体积个位数字节级，不值得）；
- 不做全二进制/protobuf 化（见 §2.3 备选方案）；
- 不改协议面（gRPC proto 不动，本 spec 只管 sq 自己的存储与内部转发编码）。

## 2. 格式设计

### 2.1 v1 二进制混合格式

```
偏移        内容
[0]         0x01                  版本字节
[1,5)       u32 BE metaLen        元数据 JSON 长度
[5,5+m)     元数据 JSON            Message 除 Body 外的全部字段，键与现有 json tag 一致
[5+m,末尾)   Body 原始字节          长度 = 总长 − 5 − metaLen，不需要长度字段
```

要点：

- **格式判别**：合法 JSON 文档首字节恒为 `'{'`（0x7B），版本字节选 0x01（非可打印字符，不可能是 JSON 开头）。`DecodeMessage` 按首字节分流：`0x7B` → 旧 JSON 路径；`0x01` → v1 二进制路径；其他 → 报错（错误信息带首字节值，供排查）。
- **元数据仍是 JSON**：保留现有 schema 演进能力（omitempty 缺键零值语义，`types.go` Message 结构注释里已论证过的向后兼容性质原样继承），人眼可读性只损失 Body 段。元数据典型 ~200B，JSON 税可忽略。
- **Body 原始字节直落**：无 base64、无转义扫描、无长度字段（尾部隐含）。
- **版本字节即演进空间**：未来需要全二进制元数据时启用 0x02，判别逻辑不变。

### 2.2 编解码规则

**Encode**（`message_encoding: binary` 时）：

```go
// 用嵌入遮蔽把 Body 排除出元数据 JSON——不 mutate 调用方的 Message
type msgMeta struct {
    *Message
    Body []byte `json:"-"` // 遮蔽外层：Body 不进元数据段
}
meta, _ := json.Marshal(&msgMeta{Message: m})
out := make([]byte, 5+len(meta)+len(m.Body))
out[0] = 0x01
binary.BigEndian.PutUint32(out[1:5], uint32(len(meta)))
copy(out[5:], meta)
copy(out[5+len(meta):], m.Body)
```

**Decode**（v1 路径）：

```go
metaLen := binary.BigEndian.Uint32(b[1:5])   // 越界前先校验 5+metaLen <= len(b)
json.Unmarshal(b[5:5+metaLen], m)
m.Body = append([]byte(nil), b[5+metaLen:]...) // 必须拷贝，见下
```

⚠️ **Body 必须拷贝，不能子切片引用**。今天 JSON 解码天然产生拷贝，调用方（deliver 的 Pebble `Scan` 回调等）默认解码结果的生命周期独立于底层字节；Pebble 迭代器的 value 切片仅在回调内有效，子切片引用会引入 use-after-free 级别的数据竞争。这是二进制化最容易踩的坑，必须钉进单测。

**编码方向由配置决定，解码方向永远双格式**——这是滚动升级安全的根基。

### 2.3 备选方案与否决理由

| 方案 | 否决理由 |
|---|---|
| 全量 protobuf | `core` 包边界明文约束"不 import 任何 proto/pb 包"（协议适配层约束）；需要 pb 类型 ↔ `core.Message` 双向转换或推翻全部字段直访调用点，侵入性远超收益 |
| 自定义全二进制 TLV | 代码量最大；schema 演进要自建 tag 纪律，JSON omitempty 白得的兼容性质要手工重造；元数据只有 ~200B，全二进制的额外收益是零头 |
| 保留 JSON、只去 base64 | 不可能——JSON 字符串不能承载任意原始字节 |

混合格式用最小代码拿走 95% 的收益（Body 才是大头），且保留全部演进空间。

## 3. 改动点

1. **`internal/core/types.go`**：
   - `DecodeMessage` 按首字节分流双格式（永久保留，无淘汰计划）；
   - `EncodeMessage` 增加编码档位（包级 `SetEncoding` 一次性装配，或函数变体，实现时定）；默认 `json`；
   - 包头注释更新职责说明；v1 布局注释写在编码函数处。
2. **`internal/config/config.go`**：新增 `message_encoding: json|binary`（缺省 `json`），与 `fsync`/`ack` 同级；main 装配时注入 core，启动日志打印当前档位。
3. **调用点零改动**：全部 15 处 `EncodeMessage/DecodeMessage` 调用点（produce.go ×3、deliver.go ×4、query.go ×2、retention.go、txn.go ×3、delay.go、admin/messages.go ×4、cmd/sq/main.go ForwardAppend handler）不动——编码集中在唯一出入口正是为了此刻。
4. **测试**：`types_test.go` 双格式 round-trip、旧格式字节不变性、Body 拷贝语义、边界（空 Body、大 metaLen 越界、非法首字节）。

## 4. 兼容与滚动升级

### 4.1 兼容面清单

| 面 | 载体 | 兼容策略 |
|---|---|---|
| Pebble 落盘 value（msg/、half/、delay/） | 磁盘 | 按 value 首字节逐条判别，新旧混存永久合法，零迁移 |
| **OpForwardAppend 控制 RPC** | **跨节点网络** | payload=[4B 组号][EncodeMessage 字节]（transport.go:82，leader 侧 main.go:601 解码）——**这是编码字节唯一的跨节点直传面**，混版约束见 §4.2 |
| raft entry / seglog | 磁盘+网络 | 批 repr 盲复制盲应用，不解码 value，无兼容问题；但旧版本节点回放含二进制 value 的日志后，其本地盘上就有了二进制数据（见 §4.3 回滚） |
| admin 查询 / retention / txn 回查 / `sq recover` | 本地读 | 全走 `DecodeMessage`，双格式解码自动覆盖 |

### 4.2 滚动升级流程（两步纪律）

1. **第一步：全集群升级到带双解码的版本**，`message_encoding` 保持缺省 `json`。此阶段所有节点写的都是旧格式，任何新旧混版组合都安全。
2. **第二步：确认全部节点已升级后**，逐节点把 `message_encoding` 翻为 `binary`（重启生效）。翻档期间部分节点写二进制、部分写 JSON——所有节点都能解双格式，安全。

**为什么必须两步**：若在混版集群提前开 binary，新节点经 ForwardAppend 把二进制字节转发给旧版 leader，旧版 `DecodeMessage` 直接报错，写入失败；同理旧版节点当上 leader 后读到二进制 value 也解不了。运维纪律：**binary 开关是升级完成的确认动作，不是升级的一部分**。

### 4.3 回滚语义

- 第一步可自由回滚（没写过新格式）。
- 第二步之后，盘上（含 raft 日志回放产物）已有二进制 value，**降级到不认识 0x01 的旧版本不受支持**——旧版本读到它会解码报错。把 `message_encoding` 翻回 `json` 只停止新增二进制数据，不消除存量。这与 seglog prealloc spec 的回滚语义同构：格式向前走之后，代码版本不能倒着走。

## 5. 代价与风险

| 风险 | 缓解 |
|---|---|
| Body 子切片 aliasing（§2.2）| 解码强制拷贝 + 专项单测钉住 |
| metaLen 越界 / 损坏字节 | 解码先校验 `len(b) >= 5 && 5+metaLen <= len(b)`，报错带上下文；CRC 完整性由下层（seglog frame CRC、Pebble block checksum）承担，本层不重复 |
| 运维忘记两步纪律，混版开 binary | 启动日志打印档位；README/升级文档写明纪律；错误信息带首字节值使误配可在日志里一眼定位 |
| 元数据 JSON 键与旧格式 json tag 漂移 | 同一个结构体同一套 tag，物理上无法漂移 |
| 可读性损失（strings 大法失效） | admin 查询接口照常工作（走 DecodeMessage）；接受 |

预期收益（待实测校准）：Body 主导消息 value 体积 −25%，乘遍 §1.1 六环；mem 档（CPU-bound）吞吐正收益最直接，fsync 档每批字节量下降。

## 6. 验收标准

1. **单测**：
   - `encoding=json` 时 `EncodeMessage` 输出与改动前**逐字节相同**（golden 用例）；
   - v1 round-trip：全字段消息 encode→decode 深度相等；空 Body、nil Properties、含非 UTF-8 Body 各一例；
   - **Body 拷贝语义**：decode 后篡改输入切片，消息 Body 不变；
   - 损坏输入：非法首字节、metaLen 越界、截断——全部报错不 panic；
   - 双格式混读：一批 JSON value + 一批 binary value 逐条 decode 全部成功。
2. **e2e**：单集群先以 json 档写入一批，切 binary 重启，再写一批，消费端两批全部收到且 Body 逐字节正确；事务/延时消息各覆盖一例。
3. **三机配对 bench**（按既有方法论：轮内配对，跨轮绝对值不可引用；C1/C3cross × mem/fsync × 并发 16/256）：
   - mem 档吞吐相对 json 档为正收益（这是主要验收指标，CPU-bound 档位省 CPU 必须能兑现）；
   - 任一格子劣化不超过 5%（沿用 async-storage-writes 复测确立的红线）；
   - Body 取业务典型尺寸（≥1KB），并附一组小 Body（~100B）对照说明收益随 Body 占比缩放。
4. **纪律项**（instrumenting-code 清单）：启动日志打印 `message_encoding` 档位；解码失败错误含首字节与长度上下文；新增代码中文注释解释"为什么"（拷贝语义、判别字节选择、两步纪律各一处）。

## 7. 实测结果

2026-08-13 三机配对 bench（三台 2 核 / 1612MB / ext4 机器，data_groups=3，客户端与 node1 共置，
20000 条/格，3 轮交错 × 2 ack × 2 body × 2 并发 = 36 格，36/36 成功）。

三臂：`main` = 主干；`opt` = 本分支 `message_encoding: json`；`bin` = **同一个 opt 二进制**
切 `message_encoding: binary`。`bin/opt` 是仅有配置差异的最干净配对，即本 spec 的净收益。

方法论沿用既有纪律：**只算轮内配对比值，跨轮绝对吞吐不可引用**（历史实测跨轮离散度
15–50%）；轮内三臂背靠背，臂序逐轮轮换以抵消块内漂移。

### 7.1 bin/opt —— 二进制编码净收益

| ack | body | conc | 逐轮比值 | 中位 | 同向 |
|---|---|---|---|---|---|
| quorum-mem | 128B | 16 | 0.986 / 0.988 / 0.999 | **−1.2%** | 是 |
| quorum-mem | 128B | 256 | 1.029 / 1.021 / 0.945 | +2.1% | 否 |
| quorum-mem | 1024B | 16 | 1.015 / 1.029 / 1.038 | **+2.9%** | 是 |
| quorum-mem | 1024B | 256 | 1.089 / 1.092 / 1.039 | **+8.9%** | 是 |
| quorum-fsync | 128B | 16 | 1.026 / 0.896 / 1.044 | +2.6% | 否 |
| quorum-fsync | 128B | 256 | 1.034 / 1.133 / 1.244 | +13.3% | 是 |
| quorum-fsync | 1024B | 16 | 1.019 / 1.049 / 0.991 | +1.9% | 否 |
| quorum-fsync | 1024B | 256 | 0.858 / 0.923 / 1.127 | −7.7% | 否（见 7.3） |

**收益随 Body 占比缩放，与 §1.1 的预测一致**：mem 档四格里，1024B 两格都是同向正收益
（+2.9% / +8.9%），128B/conc=16 则是同向 **−1.2%**。这个小负值是真的，不是噪声（三轮同向、
逐轮离散度 ≤3.3%），成因也清楚：128B Body 省下的 base64 膨胀绝对量很小，而 v1 多付了
5 字节定长头 + 元数据单独 marshal 一次的固定开销，两者在小 Body 处接近抵消并略微翻负。
1.2% 远在 5% 红线内，且 §6.3 明确把小 Body 组定位为「说明收益随 Body 占比缩放」的对照组，
主验收尺寸是 ≥1KB 的业务典型形态——该尺寸下两个并发档都是同向正收益。

### 7.2 bin/main —— 本分支合计（7 项低成本优化 + 二进制编码）

| ack | body | conc | 逐轮比值 | 中位 | 同向 |
|---|---|---|---|---|---|
| quorum-mem | 128B | 16 | 1.022 / 1.053 / 1.066 | +5.3% | 是 |
| quorum-mem | 128B | 256 | 1.068 / 1.094 / 1.038 | +6.8% | 是 |
| quorum-mem | 1024B | 16 | 1.083 / 1.080 / 1.051 | +8.0% | 是 |
| quorum-mem | 1024B | 256 | 1.117 / 1.135 / 1.087 | **+11.7%** | 是 |

mem 档四格全部同向正收益，最大格 +11.7%。fsync 档合计各格中位 +4.1% ~ +16.2% 但均不同向，
按纪律不作为结论引用。

作为分解，`opt/main`（7 项低成本优化，不含本 spec）在 mem 档同样四格同向为正：
+6.7% / +7.1% / +4.9% / +3.9%（逐轮离散度 ≤6.5%）。

### 7.3 唯一触碰红线的格子：fsync / 1024B / conc=256

主批该格 `bin/opt` 中位 −7.7%，破 5% 红线。但它**不同向**（0.858 / 0.923 / 1.127），
且它是全批离散度最大的格：三臂各自的逐轮绝对吞吐离散度为 main 17.0% / opt 44.5% /
bin 33.0%——分母本身就在剧烈抖动。同一格的 `opt/main` 读数是 +21.5%（同样不同向），
是同一份噪声的另一面，同样不可引用。

**未按「大概是噪声」放过，补跑了 5 轮该格**（3 臂 × 5 轮）：

| 臂 | 逐轮 msg/s | 离散度 |
|---|---|---|
| main | 4851 5426 4190 4231 4948 | 25.5% |
| opt | 5861 4475 4963 5043 4962 | 27.9% |
| bin | 5074 4748 3856 5425 5551 | 33.4% |

| 配对 | 逐轮比值 | 中位 | 同向 |
|---|---|---|---|
| bin/opt | 0.866 1.061 0.777 1.076 1.119 | **+6.1%** | 否 |
| opt/main | 1.208 0.825 1.184 1.192 1.003 | +18.4% | 否 |
| bin/main | 1.046 0.875 0.920 1.282 1.122 | +4.6% | 否 |

补跑把 `bin/opt` 的中位从 −7.7% 翻到 +6.1%，符号随样本翻转、五轮仍不同向、三臂离散度
25–33%。**结论：该格在本机器上不可测量，−7.7% 不构成劣化证据**（+6.1% 同样不构成收益证据）。
八轮合并看，bin 的中位绝对吞吐（5250）反而高于 opt（5003）与 main（4900），也不支持劣化。

这个 regime（fsync + 大 Body + 高并发）此前在 [[seglog-bench-2026-08-11]] / async-storage-writes
复测里就以高离散著称——三副本 fsync 的跨批合并时机对调度极度敏感。要在这里得到可判读的
数字，需要更多轮次或更稳的机器，不是本次验收该承担的成本。

### 7.4 验收判定（对照 §6.3）

| 条件 | 判定 |
|---|---|
| mem 档相对 json 档为正收益（主指标） | ✅ 业务典型尺寸 1024B 两格同向正收益 +2.9% / +8.9%；128B 对照组 −1.2%（同向、真实、远在红线内，成因见 7.1） |
| 任一格子劣化不超过 5% | ✅ 唯一破线格补跑 5 轮后符号翻转、离散度 25–33%，判定为不可测量而非劣化 |
| Body 尺寸对照说明收益缩放 | ✅ 128B → 1024B、conc 16 → 256，收益单调放大（−1.2% → +2.1% → +2.9% → +8.9%） |

**通过。** 建议默认档位保持 `json`，按 §4.2 两步纪律在全集群升级完成后再切 `binary`。
