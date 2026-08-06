# M2 消费失败链路与存储卫生 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **REQUIRED SUB-SKILL: instrumenting-code** —— 每个实现类 task 内含「加关键节点日志」与「加注释」step；实现完成、声明完工前，必须按 instrumenting-code 清单自检（错误分支带上下文、成功路径不静默、新文件头注释、导出方法注释、禁 fmt.Printf）。

**Goal:** 补齐消费失败链路（重试指数退避 + 投递超限转 DLQ）、服务端 Tag 过滤、Keys 业务索引与按 key 查询、消息 retention 后台清理、磁盘水位拒写保读——即 spec 里程碑 M2「消费失败链路完整」。

**Architecture:** 全部在 M1 已落地的四层架构内做增量：store 层新增 `keyidx/` 编码；core 层扩展 meta 配置（maxAttempts/retention）、produce 增加 `AppendWith` 原子扩展写（DLQ 转入与 M3 延时转正共用的关键机制）、deliver 增加过滤与退避/DLQ 分支、新增 retention 后台任务（M1 之后第一个后台 goroutine）；rpc 层接入 TAG 表达式与 FORBIDDEN 拒写。所有多键状态变更仍收敛在单一 Pebble WriteBatch（`store.Apply` 唯一写入口）内原子提交。

**Tech Stack:** Go 1.26、cockroachdb/pebble v2、google.golang.org/grpc、log/slog、官方 rocketmq-clients Go SDK v5.1.4（e2e）。

**执行基线：** 分支 `claude/m1-normal-message-plan-4f53dd`（工作树 `.claude/worktrees/m1-normal-message-plan-4f53dd`）的 M1 实现，含合并前修复：投递时 BodyDigest 补算/UNSPECIFIED encoding 归一化、AckTimeout 重投与 broker 重启恢复两条 e2e。本计划在该分支（或其合入 main 后的主干）上执行；开工前先 `make test && make e2e` 确认基线全绿。

**基线关键形态（与其他 M1 实现不同处，执行者须知）：**
- deliver 的 `Ack`/`ChangeInvisible` 带 attempt 校验且与 `receiveOnce` 共持队列锁（`lockQueue`）——新增任何直接读写 inflight 的路径必须持同一把锁。
- `receiveOnce` 用 `staged` 标志决定批次 Close/Apply（不是 `dirty`）；阶段 2 的进入守卫是 `if len(out) < maxMsgs`。
- Settings 协商在 `internal/rpc/settings.go` 的 `negotiateSettings`（BackoffPolicy/ValidateMessageType/长轮询上限均已下发，M2 无需再补）。
- gRPC 传输上限在 `internal/rpc/limits.go`（`MaxGRPCMessageSize`）。
- e2e 在 `test/e2e/sdk_test.go`：每用例独立 broker 进程 + 独立数据目录 + 端口回退（`startBroker(t)`），broker 配置由 `config.Config` 结构体序列化生成——**config 每加一个带校验的新字段，`startBroker` 的配置构造必须同 task 更新**，否则 yaml.Marshal 会把零值写进配置文件、broker 启动即被校验拒绝（`make test` 发现不了，只有 `make e2e` 会炸）。

## Global Constraints

- 模块名 `github.com/xushixin/sq`；Go 1.26。
- 日志一律 `log/slog`（JSON），**禁止** `fmt.Printf`/`log.Printf` 作为日志手段；错误日志必带 topic/group/queue/offset/msgId 上下文（spec §8）。
- 注释规范：新文件顶部中文「职责/边界」头注释；导出方法写参数/返回/注意；复杂逻辑注释「为什么」（用户全局 CLAUDE.md §2）。
- store key 整数字段大端定长（字节序=数值序）；topic/group 名合法字符 `^[A-Za-z0-9_%\-]{1,127}$`（`%` 合法，DLQ topic 依赖）；用户消息 key 为任意字符串（**可含 `/`**）。
- 所有状态变更经 `store.Apply(batch)` 单一写入口；一次语义操作 = 一个 WriteBatch。
- 每个 task 结束时 `make test` 必须全绿（含 `go vet ./...`）；跨 task 不留编译断点——改动导出签名的 task 负责同 task 内更新全部调用点。
- TDD：先写失败测试再实现；每 task 至少一次提交。
- 协议错误严格用 proto `Status` code（`ILLEGAL_FILTER_EXPRESSION`/`FORBIDDEN` 等），业务错误走响应内 Status 字段而非 gRPC error（M1 既有约定）。
- 与生成 pb 代码或 SDK API 的字段名出入时，以生成代码/SDK 实际为准调整（预期内修正，不算设计变更；M1 已验证此规则）。

## File Structure

```
internal/
  config/config.go            修改：新增 default_max_attempts / retention_check_interval /
                                    disk_watermark_percent + log_level 校验补全
  store/keys.go               修改：keyidx/ 编码（KeyIdxKey/Parse/两种前缀）
  core/
    meta/meta.go              修改：GroupConfig.MaxAttempts、TopicConfig.RetentionMs、
                                    DLQTopicName、Topics() 枚举、New 增参
    produce/produce.go        修改：AppendWith（原子扩展写）+ Keys 索引同批写入
    deliver/
      deliver.go              修改：Receive 增 filter 参数；阶段2 Tag 过滤+扫描预算；
                                    阶段1 重试退避 + 超限转 DLQ
      filter.go               新建：TagFilter 解析与匹配
    query/query.go            新建：ByKey 按业务 key 检索（M5 Admin API 复用）
    retention/
      retention.go            新建：后台清理任务（msg DeleteRange + keyidx 扫删 + 水位检查）
      disk_unix.go            新建：unix statfs 磁盘用量
      disk_other.go           新建：非 unix 平台降级
  rpc/
    server.go                 修改：New 增 writeBlocked 参数
    receive.go                修改：TAG 表达式解析接入（替换 "*"-only 守卫）；
                                    toPBMessage 补 InvisibleDuration
    send.go                   修改：磁盘水位拒写（FORBIDDEN）
cmd/sq/main.go                修改：retention 任务装配与停机顺序、水位 guard 装配
test/e2e/sdk_test.go          修改：startBroker 支持配置覆盖；Tag 过滤 e2e、DLQ e2e
README.md                     修改：M2 功能与新配置项
```

## 任务间接口总表（跨 task 契约，执行者以此为准）

```go
// meta（Task 3）
func New(st *store.Store, autoCreate bool, defaultQueues uint32, defaultMaxAttempts int32, logger *slog.Logger) (*Meta, error)
const DefaultMaxAttempts = 16
const DefaultRetentionMs int64 = 3 * 24 * 60 * 60 * 1000
func DLQTopicName(group string) string                    // "%DLQ%" + group
func (g GroupConfig) EffectiveMaxAttempts() int32         // 0 → DefaultMaxAttempts
func (t TopicConfig) EffectiveRetention() time.Duration   // 0 → DefaultRetentionMs
func (m *Meta) Topics() []TopicConfig

// store（Task 2）
func KeyIdxKey(topic, key string, storeMs int64, queueID uint32, offset uint64) []byte
func KeyIdxKeyPrefix(topic, key string) []byte
func KeyIdxTopicPrefix(topic string) []byte
func ParseKeyIdxKey(k []byte) (topic, key string, storeMs int64, queueID uint32, offset uint64, err error)

// produce（Task 4）
func (p *Producer) AppendWith(m *core.Message, extra func(b *pebble.Batch)) (*core.Message, error)
func (p *Producer) Append(m *core.Message) (*core.Message, error)   // 签名不变 = AppendWith(m, nil)

// query（Task 4）
func ByKey(st *store.Store, topic, key string, limit int) ([]*core.Message, error)

// deliver（Task 5/6）
func ParseTagFilter(expr string) (*TagFilter, error)      // "*"/"" → (nil, nil)
func (f *TagFilter) Match(tag string) bool                // nil 接收者匹配一切
func (d *Deliverer) Receive(ctx context.Context, group, topic string, queueID uint32,
	maxMsgs int, invisible, wait time.Duration, filter *TagFilter) ([]*core.Message, error)

// retention（Task 8 初版 / Task 9 终版）
func New(st *store.Store, mt *meta.Meta, interval time.Duration, logger *slog.Logger) *Manager            // Task 8
func New(st *store.Store, mt *meta.Meta, interval time.Duration, dataDir string, watermarkPct int,
	writeBlocked *atomic.Bool, logger *slog.Logger) *Manager                                              // Task 9 终版
func (m *Manager) Run(ctx context.Context)   // 阻塞循环，ctx 取消返回
func (m *Manager) Pass() (int, error)        // 单趟清理，导出供测试

// rpc（Task 9）
func New(cfg *config.Config, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer,
	writeBlocked *atomic.Bool, logger *slog.Logger) *Server

// e2e（Task 3/8/9/10 递增修改）
func startBroker(t *testing.T, mutate ...func(*config.Config)) string  // 既有 startBroker 增可选配置覆盖

// config（Task 3/8/9 累计新增字段；Task 1 只补 log_level 校验，不加字段）
DefaultMaxAttempts     int32  `yaml:"default_max_attempts"`      // 默认 16，>0
RetentionCheckInterval string `yaml:"retention_check_interval"`  // 默认 "5m"，ParseDuration>0
DiskWatermarkPercent   int    `yaml:"disk_watermark_percent"`    // 默认 85，0..99（0=关闭）
func (c *Config) RetentionInterval() time.Duration
```

---

### Task 1: M1 审阅收尾（log_level 校验、InvisibleDuration 回填）

M1 审阅遗留的两个小项，先清干净再开新功能。（基线的 Settings 协商已下发 BackoffPolicy/ValidateMessageType，`default_queue_nums` 校验也已存在——这两项无需再做。）

**Files:**
- Modify: `internal/config/config.go`（`Load` 增 log_level 校验）
- Modify: `internal/rpc/receive.go`（`toPBMessage` 增 invisible 参数并回填 InvisibleDuration）
- Test: `internal/config/config_test.go`、`internal/rpc/receive_test.go`

**Interfaces:**
- Consumes: M1 的 `config.Load`、`toPBMessage(m *core.Message, group string)`
- Produces: `toPBMessage(m *core.Message, group string, invisible time.Duration)`——**本文件内部方法签名变更，同 task 更新 `ReceiveMessage` 调用处与 receive_test.go 全部 `toPBMessage` 调用（含基线的 BodyDigest 回归测试）**

- [ ] **Step 1: 写失败测试**

`internal/config/config_test.go` 追加（import 若缺则补 `os`、`path/filepath`）：

```go
// TestLoadRejectsBadLogLevel 非法 log_level 必须在启动时报错：现状 SetupSlog
// 静默降级为 info，一个拼写错误（如 verbose）会让 debug 日志无声消失，
// 与同文件 fsync/default_queue_nums 的严格校验风格也不一致。
func TestLoadRejectsBadLogLevel(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sq.yaml")
	if err := os.WriteFile(p, []byte("log_level: verbose\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err == nil {
		t.Fatal("期望拒绝非法 log_level")
	}
}
```

`internal/rpc/receive_test.go` 追加：

```go
// TestToPBMessageSetsInvisibleDuration 下发消息须回填本次的不可见时长，
// SDK 依此换算消息可见时间点展示/重试。
func TestToPBMessageSetsInvisibleDuration(t *testing.T) {
	s := &Server{}
	msg := s.toPBMessage(&core.Message{ID: "A", Topic: "t", Body: []byte("x"), DeliveryAttempt: 1}, "g", 45*time.Second)
	if got := msg.GetSystemProperties().GetInvisibleDuration().AsDuration(); got != 45*time.Second {
		t.Fatalf("InvisibleDuration: 期望 45s，得到 %v", got)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/config ./internal/rpc -run 'TestLoadRejectsBadLogLevel|TestToPBMessageSetsInvisibleDuration' -v`
Expected: config 用例 FAIL（校验缺失）；rpc 编译失败（`toPBMessage` 现为 2 参）——TDD 下编译失败等同测试失败

- [ ] **Step 3: 实现**

`internal/config/config.go` 的 `Load`，在 default_queue_nums 校验后追加：

```go
	// log_level 与 SetupSlog 的 switch 分支必须同步：这里不挡住，SetupSlog 的
	// default 分支会把拼错的级别静默降级成 info，错误从此不可见。
	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return nil, fmt.Errorf("配置 log_level 只接受 debug|info|warn|error，得到 %q", cfg.LogLevel)
	}
```

`internal/rpc/receive.go`：`toPBMessage` 签名改为 `toPBMessage(m *core.Message, group string, invisible time.Duration)`（import 补 `google.golang.org/protobuf/types/known/durationpb`），`SystemProperties` 字面量中追加一行：

```go
		InvisibleDuration: durationpb.New(invisible),
```

`ReceiveMessage` 内的调用处改为 `s.toPBMessage(m, group, invisible)`（`invisible` 变量已在作用域内）；`receive_test.go` 既有 `toPBMessage` 调用（BodyDigest 回归测试等）补第 3 参（如 `time.Minute`）。

- [ ] **Step 4: 跑测试确认通过**

Run: `make test`
Expected: 全部 PASS（含 M1 既有用例）

- [ ] **Step 5: 核对日志与注释（instrumenting-code）**

本 task 无新执行路径，核对两点即可：log_level 校验注释说明了「为什么必须启动时挡住」（上码已含）；config 校验错误信息含非法值本身（上码已含）。

- [ ] **Step 6: Commit**

```bash
git add internal/config internal/rpc
git commit -m "fix: M1 审阅收尾（log_level 校验、InvisibleDuration 回填）"
```

---

### Task 2: store keyidx 业务索引编码

**Files:**
- Modify: `internal/store/keys.go`
- Test: `internal/store/keys_test.go`

**Interfaces:**
- Consumes: 既有 `PutU64`/`putU32`/`PrefixUpperBound`
- Produces: `KeyIdxKey(topic, key string, storeMs int64, queueID uint32, offset uint64) []byte`、`KeyIdxKeyPrefix(topic, key string) []byte`、`KeyIdxTopicPrefix(topic string) []byte`、`ParseKeyIdxKey(k []byte) (topic, key string, storeMs int64, queueID uint32, offset uint64, err error)`

**设计要点（写进代码注释）：** 用户消息 key 是任意字符串、**可含 `/`**，与 topic/group 名不同。因此布局 `keyidx/{topic}/{key}/{storeMs:8B}{queueID:4B}{offset:8B}` 的解析必须**从尾部定长回推**（末 20 字节二进制 + 其前必为 `/`），不能按 `/` 分割。前缀查询靠「剥前缀后剩余恰好 20 字节」区分真命中与「本 key 是另一 key 的路径前缀」的伪命中。

- [ ] **Step 1: 写失败测试**

`internal/store/keys_test.go` 追加：

```go
func TestKeyIdxKeyRoundTrip(t *testing.T) {
	k := KeyIdxKey("orders", "oid-1", 1700000000000, 3, 42)
	topic, key, ms, q, off, err := ParseKeyIdxKey(k)
	if err != nil || topic != "orders" || key != "oid-1" || ms != 1700000000000 || q != 3 || off != 42 {
		t.Fatalf("round trip: %v %v %v %v %v %v", topic, key, ms, q, off, err)
	}
}

// TestKeyIdxKeyWithSlashInKey 用户 key 可含 '/'：必须尾部定长解析，不能 Split。
func TestKeyIdxKeyWithSlashInKey(t *testing.T) {
	k := KeyIdxKey("t", "a/b/c", 1, 0, 7)
	_, key, _, _, off, err := ParseKeyIdxKey(k)
	if err != nil || key != "a/b/c" || off != 7 {
		t.Fatalf("含 '/' 的 key: %v %v %v", key, off, err)
	}
}

// TestKeyIdxPrefixNoFalseMatch key "oid" 的查询前缀不得命中 "oid2"；
// 命中 "oid/x"（路径前缀伪命中）时能靠剩余长度 != 20 区分。
func TestKeyIdxPrefixNoFalseMatch(t *testing.T) {
	p := KeyIdxKeyPrefix("t", "oid")
	if bytes.HasPrefix(KeyIdxKey("t", "oid2", 1, 0, 1), p) {
		t.Fatal("前缀误匹配 oid2")
	}
	sub := KeyIdxKey("t", "oid/x", 1, 0, 1)
	if !bytes.HasPrefix(sub, p) {
		t.Fatal("测试前提不成立：oid/x 应落在 oid/ 前缀区间")
	}
	if len(sub)-len(p) == 20 {
		t.Fatal("应能靠剩余长度区分子路径 key")
	}
}

func TestParseKeyIdxKeyRejectsGarbage(t *testing.T) {
	for _, bad := range [][]byte{[]byte("keyidx/"), []byte("keyidx/t"), []byte("keyidx/t/short"), []byte("msg/t/x")} {
		if _, _, _, _, _, err := ParseKeyIdxKey(bad); err == nil {
			t.Fatalf("应拒绝非法 key %q", bad)
		}
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/store -run TestKeyIdx -v`
Expected: FAIL，`undefined: KeyIdxKey`

- [ ] **Step 3: 实现**

`internal/store/keys.go` 前缀常量块追加 `keyIdxPrefix = "keyidx/"`，文件尾追加：

```go
// KeyIdxKey Keys 业务索引：keyidx/{topic}/{key}/{storeMs:8B}{queueID:4B}{offset:8B}，值为空。
//
// 注意：key 是用户任意字符串（可含 '/'，与 topic/group 名不同），
// 解析必须从尾部定长回推（见 ParseKeyIdxKey），不能按 '/' 分割。
// storeMs 用消息 StoreAtMs：同一 key 多条消息按写入时间排序，retention 清理同用此值。
func KeyIdxKey(topic, key string, storeMs int64, queueID uint32, offset uint64) []byte {
	k := make([]byte, 0, len(keyIdxPrefix)+len(topic)+1+len(key)+1+20)
	k = append(k, keyIdxPrefix...)
	k = append(k, topic...)
	k = append(k, '/')
	k = append(k, key...)
	k = append(k, '/')
	k = append(k, PutU64(uint64(storeMs))...)
	k = append(k, putU32(queueID)...)
	k = append(k, PutU64(offset)...)
	return k
}

// KeyIdxKeyPrefix 按 (topic, key) 精确查询的扫描下界（含末尾 '/'）。
// 区间内可能混入「以本 key 为路径前缀」的其他 key（如查 "a" 命中 "a/b"），
// 调用方须用 ParseKeyIdxKey 成功 + key 等值过滤（见 query.ByKey）。
func KeyIdxKeyPrefix(topic, key string) []byte {
	k := KeyIdxKey(topic, key, 0, 0, 0)
	return k[:len(k)-20]
}

// KeyIdxTopicPrefix 某 topic 全部索引的扫描下界（retention 清理用）。
func KeyIdxTopicPrefix(topic string) []byte {
	return []byte(keyIdxPrefix + topic + "/")
}

// ParseKeyIdxKey 解析索引 key。key 段可能含 '/'，因此从尾部回推：
// 末 20 字节为定长二进制（8B storeMs + 4B queueID + 8B offset），其前必须是 '/'；
// topic 后第一个 '/' 与该分隔符之间的全部内容为 key。
func ParseKeyIdxKey(k []byte) (topic, key string, storeMs int64, queueID uint32, offset uint64, err error) {
	rest, ok := bytes.CutPrefix(k, []byte(keyIdxPrefix))
	if !ok {
		return "", "", 0, 0, 0, fmt.Errorf("非法 keyidx key: %q", k)
	}
	i := bytes.IndexByte(rest, '/')
	if i < 0 {
		return "", "", 0, 0, 0, fmt.Errorf("keyidx key 结构错误: %q", k)
	}
	topic = string(rest[:i])
	rest = rest[i+1:]
	if len(rest) < 1+20 || rest[len(rest)-21] != '/' {
		return "", "", 0, 0, 0, fmt.Errorf("keyidx key 结构错误: %q", k)
	}
	key = string(rest[:len(rest)-21])
	bin := rest[len(rest)-20:]
	return topic, key, int64(binary.BigEndian.Uint64(bin[:8])),
		binary.BigEndian.Uint32(bin[8:12]), binary.BigEndian.Uint64(bin[12:]), nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/store -v`
Expected: 全部 PASS

- [ ] **Step 5: 核对注释（instrumenting-code）**

store 层纯编码无日志（M1 既定边界）；核对新导出函数 4 个均有 doc comment、「为什么尾部回推」写在注释里（上码已含）。

- [ ] **Step 6: Commit**

```bash
git add internal/store
git commit -m "feat(store): keyidx 业务索引 key 编码（尾部定长解析，key 可含 '/'）"
```

---

### Task 3: meta 配置扩展（MaxAttempts / Retention / DLQ 命名 / Topics 枚举）

**Files:**
- Modify: `internal/core/meta/meta.go`
- Modify: `internal/config/config.go`（新增 `default_max_attempts`）
- Modify: `cmd/sq/main.go`、`internal/core/produce/produce_test.go`、`internal/core/deliver/deliver_test.go`、`internal/rpc/server_test.go`、`internal/core/meta/meta_test.go`、`test/e2e/sdk_test.go`（`meta.New` 增参与 e2e 配置补字段，本 task 内全部更新）
- Test: `internal/core/meta/meta_test.go`、`internal/config/config_test.go`

**Interfaces:**
- Produces: 见「任务间接口总表」meta 段；`config.Config.DefaultMaxAttempts int32`

- [ ] **Step 1: 写失败测试**

`internal/core/meta/meta_test.go` 追加（fixture 若为局部构造，按现有写法内联 `store.Open(t.TempDir(),...)`）：

```go
// TestGroupMaxAttempts 新组按 broker 默认注册；旧数据（0 值）回退包默认。
func TestGroupMaxAttempts(t *testing.T) {
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	m, err := New(st, true, 4, 2, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	gc, err := m.EnsureGroup("g1")
	if err != nil || gc.EffectiveMaxAttempts() != 2 {
		t.Fatalf("新组 maxAttempts: %d %v", gc.EffectiveMaxAttempts(), err)
	}
	// M1 落盘的旧组无 max_attempts 字段（解码为 0）→ 回退 DefaultMaxAttempts
	if got := (GroupConfig{}).EffectiveMaxAttempts(); got != DefaultMaxAttempts {
		t.Fatalf("零值回退: %d", got)
	}
}

func TestTopicRetentionDefault(t *testing.T) {
	st, _ := store.Open(t.TempDir(), true, slog.Default())
	t.Cleanup(func() { st.Close() })
	m, _ := New(st, true, 4, 16, slog.Default())
	tc, err := m.CreateTopic("t", 4)
	if err != nil || tc.EffectiveRetention() != 72*time.Hour {
		t.Fatalf("默认 retention: %v %v", tc.EffectiveRetention(), err)
	}
	if got := (TopicConfig{}).EffectiveRetention(); got != 72*time.Hour {
		t.Fatalf("零值回退: %v", got)
	}
}

func TestDLQTopicName(t *testing.T) {
	name := DLQTopicName("orders-g")
	if name != "%DLQ%orders-g" {
		t.Fatalf("DLQ 名: %s", name)
	}
	if err := ValidateName(name); err != nil {
		t.Fatalf("DLQ 名必须通过名字校验（'%%' 在合法字符集内）: %v", err)
	}
}

func TestTopicsSnapshot(t *testing.T) {
	st, _ := store.Open(t.TempDir(), true, slog.Default())
	t.Cleanup(func() { st.Close() })
	m, _ := New(st, true, 4, 16, slog.Default())
	m.CreateTopic("a", 1)
	m.CreateTopic("b", 2)
	if got := len(m.Topics()); got != 2 {
		t.Fatalf("Topics 快照: %d", got)
	}
}
```

`internal/config/config_test.go` 追加：

```go
func TestDefaultMaxAttempts(t *testing.T) {
	cfg, err := Load("")
	if err != nil || cfg.DefaultMaxAttempts != 16 {
		t.Fatalf("默认 max attempts: %d %v", cfg.DefaultMaxAttempts, err)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	os.WriteFile(p, []byte("default_max_attempts: 0\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("应拒绝 default_max_attempts=0")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/meta ./internal/config -run 'TestGroupMaxAttempts|TestTopicRetention|TestDLQTopicName|TestTopicsSnapshot|TestDefaultMaxAttempts' -v`
Expected: 编译失败（`New` 参数不符 / 未定义符号）——TDD 下编译失败等同测试失败

- [ ] **Step 3: 实现 meta**

`internal/core/meta/meta.go`：

```go
// DefaultMaxAttempts 订阅组默认最大投递次数（超过即转 DLQ），与 RocketMQ 默认一致。
const DefaultMaxAttempts = 16

// DefaultRetentionMs 默认消息保留时长：3 天（spec §5 流程 7）。
const DefaultRetentionMs int64 = 3 * 24 * 60 * 60 * 1000

const dlqPrefix = "%DLQ%"

// DLQTopicName 消费组对应的死信 topic 名。'%' 在名字合法字符集内，
// 死信 topic 是普通 topic（可用 SDK 直接消费、控制台可查可重发）。
func DLQTopicName(group string) string { return dlqPrefix + group }
```

`TopicConfig` 增字段 `RetentionMs int64 \`json:"retention_ms,omitempty"\``；`GroupConfig` 增 `MaxAttempts int32 \`json:"max_attempts,omitempty"\``。访问器：

```go
// EffectiveMaxAttempts 生效的最大投递次数。0 表示 M1 时期落盘的旧配置，回退包默认。
func (g GroupConfig) EffectiveMaxAttempts() int32 {
	if g.MaxAttempts <= 0 {
		return DefaultMaxAttempts
	}
	return g.MaxAttempts
}

// EffectiveRetention 生效的消息保留时长。0 表示 M1 旧配置，回退包默认。
func (t TopicConfig) EffectiveRetention() time.Duration {
	if t.RetentionMs <= 0 {
		return time.Duration(DefaultRetentionMs) * time.Millisecond
	}
	return time.Duration(t.RetentionMs) * time.Millisecond
}
```

`Meta` 结构体增字段 `defaultMaxAttempts int32`；`New` 签名改为：

```go
// New 构造并从 store 加载全部已有配置。
// defaultMaxAttempts<=0 时使用 DefaultMaxAttempts（防御配置层漏校验）。
func New(st *store.Store, autoCreate bool, defaultQueues uint32, defaultMaxAttempts int32, logger *slog.Logger) (*Meta, error) {
	if defaultMaxAttempts <= 0 {
		defaultMaxAttempts = DefaultMaxAttempts
	}
	m := &Meta{
		st: st, autoCreate: autoCreate, defaultQueues: defaultQueues, defaultMaxAttempts: defaultMaxAttempts,
		// ……其余与 M1 相同
```

`CreateTopic` 构造 `tc` 时增 `RetentionMs: DefaultRetentionMs`；`EnsureGroup` 构造 `gc` 时增 `MaxAttempts: m.defaultMaxAttempts`。新增：

```go
// Topics 返回全部 topic 配置快照（retention 后台扫描用；乱序）。
func (m *Meta) Topics() []TopicConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]TopicConfig, 0, len(m.topics))
	for _, tc := range m.topics {
		out = append(out, tc)
	}
	return out
}
```

- [ ] **Step 4: 实现 config 与调用点更新**

`internal/config/config.go`：`Config` 增 `DefaultMaxAttempts int32 \`yaml:"default_max_attempts"\` // 新订阅组默认最大投递次数`；`Load` 默认值 `DefaultMaxAttempts: 16`，校验块追加：

```go
	if cfg.DefaultMaxAttempts <= 0 {
		return nil, fmt.Errorf("配置 default_max_attempts 必须 >0，得到 %d", cfg.DefaultMaxAttempts)
	}
```

全部 `meta.New`/`New` 调用点更新：
- `cmd/sq/main.go`（`run()` 内）→ `meta.New(st, cfg.AutoCreateTopic, cfg.DefaultQueueNums, cfg.DefaultMaxAttempts, logger)`
- `internal/core/produce/produce_test.go`（`newTestProducer` fixture）→ `meta.New(st, true, 4, 16, slog.Default())`
- `internal/core/deliver/deliver_test.go`（`newFixture` fixture）→ `meta.New(st, true, 1, 16, slog.Default())`
- `internal/rpc/server_test.go`（`newTestEnv` fixture，注意第 2 参是变量 `autoCreate` 不是字面量）→ `meta.New(st, autoCreate, 4, 16, slog.Default())`
- `internal/core/meta/meta_test.go` 内全部包内 `New(` 调用补第 4 参 `16`
- `test/e2e/sdk_test.go` 的 `startBroker`：`config.Config` 结构体构造补 `DefaultMaxAttempts: 16`——不补的话 yaml.Marshal 写出 `default_max_attempts: 0`，broker 被本 task 的新校验拒启（`make test` 盖不住，`make e2e` 才会炸；见执行基线说明）

- [ ] **Step 5: 跑测试确认通过**

Run: `make test`
Expected: 全部 PASS

- [ ] **Step 6: 核对日志与注释（instrumenting-code）**

`EnsureGroup`/`CreateTopic` 的 Info 日志已在 M1 存在；本 task 在「topic 已创建」日志中追加 `"retention_ms", tc.RetentionMs`、「消费组已注册」日志中追加 `"max_attempts", gc.MaxAttempts`，让新配置项可观测。核对新导出符号（2 常量、3 方法、1 函数）均有 doc comment。

- [ ] **Step 7: Commit**

```bash
git add internal/core/meta internal/config cmd/sq internal/core/produce internal/core/deliver internal/rpc
git commit -m "feat(meta): 订阅组 maxAttempts 与 topic retention 配置、DLQ 命名、Topics 枚举"
```

---

### Task 4: produce AppendWith 原子扩展写 + Keys 索引 + query.ByKey

`AppendWith` 是本里程碑最重要的机制性改动：让「写一条消息」能与「另一处状态变更」共用同一 WriteBatch 原子提交。Task 6 的 DLQ 转入（写死信 + 删 inflight）依赖它，M3 的延时转正（写消息 + 删 delay 条目）将复用它。

**Files:**
- Modify: `internal/core/produce/produce.go`
- Create: `internal/core/query/query.go`
- Test: `internal/core/produce/produce_test.go`、`internal/core/query/query_test.go`

**Interfaces:**
- Consumes: Task 2 的 `KeyIdxKey`/`KeyIdxKeyPrefix`/`ParseKeyIdxKey`
- Produces: `(p *Producer) AppendWith(m *core.Message, extra func(b *pebble.Batch)) (*core.Message, error)`；`Append` 签名不变；`query.ByKey(st *store.Store, topic, key string, limit int) ([]*core.Message, error)`

- [ ] **Step 1: 写失败测试**

`internal/core/produce/produce_test.go` 追加（import 补 `bytes`、`github.com/cockroachdb/pebble/v2`）：

```go
// TestAppendWritesKeyIndex Keys 索引必须与消息同批落盘。
func TestAppendWritesKeyIndex(t *testing.T) {
	pr, st := newTestProducer(t, t.TempDir()) // 本文件既有 fixture（Task 3 已改 4 参 meta.New）
	defer st.Close()
	m, err := pr.Append(&core.Message{Topic: "t", Body: []byte("x"), Keys: []string{"k1", "k2"}})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	for _, key := range []string{"k1", "k2"} {
		pfx := store.KeyIdxKeyPrefix("t", key)
		found := 0
		err := st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
			_, pk, _, q, off, err := store.ParseKeyIdxKey(k)
			if err != nil || pk != key || q != m.QueueID || off != m.Offset {
				t.Fatalf("索引内容不符: %v %v %v %v", pk, q, off, err)
			}
			found++
			return true, nil
		})
		if err != nil || found != 1 {
			t.Fatalf("key %s 索引条数: %d %v", key, found, err)
		}
	}
}

// TestAppendWithExtraAtomic extra 写操作与消息同批提交。
func TestAppendWithExtraAtomic(t *testing.T) {
	pr, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	marker := []byte("test/marker")
	_, err := pr.AppendWith(&core.Message{Topic: "t", Body: []byte("x")},
		func(b *pebble.Batch) { b.Set(marker, []byte("1"), nil) })
	if err != nil {
		t.Fatalf("AppendWith: %v", err)
	}
	if _, ok, _ := st.Get(marker); !ok {
		t.Fatal("extra 写操作未随消息落盘")
	}
}
```

（fixture 说明：`produce_test.go` 既有 helper 是 `newTestProducer(t *testing.T, dir string) (*Producer, *store.Store)`，调用方自行 `defer st.Close()`；直接复用，**不得改动既有用例**。）

`internal/core/query/query_test.go` 新建：

```go
// query 包测试：按业务 key 检索。
package query

import (
	"log/slog"
	"testing"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

func newFixture(t *testing.T) (*store.Store, *produce.Producer) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	return st, produce.New(st, mt, slog.Default())
}

func TestByKeyFindsMessages(t *testing.T) {
	st, pr := newFixture(t)
	for _, body := range []string{"a", "b", "c"} {
		if _, err := pr.Append(&core.Message{Topic: "t", Body: []byte(body), Keys: []string{"oid"}}); err != nil {
			t.Fatal(err)
		}
	}
	pr.Append(&core.Message{Topic: "t", Body: []byte("other"), Keys: []string{"other-key"}})

	msgs, err := ByKey(st, "t", "oid", 0)
	if err != nil || len(msgs) != 3 {
		t.Fatalf("ByKey: %d %v", len(msgs), err)
	}
	for i, want := range []string{"a", "b", "c"} { // storeMs 同毫秒时按 queue/offset 续排，单队列即写入序
		if string(msgs[i].Body) != want {
			t.Fatalf("第 %d 条 body: %s", i, msgs[i].Body)
		}
	}
}

// TestByKeyNoPrefixCollision 查 "oid" 不得混入 "oid2" 与 "oid/x" 的消息。
func TestByKeyNoPrefixCollision(t *testing.T) {
	st, pr := newFixture(t)
	pr.Append(&core.Message{Topic: "t", Body: []byte("hit"), Keys: []string{"oid"}})
	pr.Append(&core.Message{Topic: "t", Body: []byte("miss1"), Keys: []string{"oid2"}})
	pr.Append(&core.Message{Topic: "t", Body: []byte("miss2"), Keys: []string{"oid/x"}})
	msgs, err := ByKey(st, "t", "oid", 0)
	if err != nil || len(msgs) != 1 || string(msgs[0].Body) != "hit" {
		t.Fatalf("ByKey 精确性: %d %v", len(msgs), err)
	}
}

// TestByKeySkipsPurgedMessage retention 清走消息但索引未清（清理竞态）时跳过不报错。
func TestByKeySkipsPurgedMessage(t *testing.T) {
	st, pr := newFixture(t)
	m, _ := pr.Append(&core.Message{Topic: "t", Body: []byte("x"), Keys: []string{"k"}})
	b := st.NewBatch()
	b.Delete(store.MsgKey("t", m.QueueID, m.Offset), nil)
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	msgs, err := ByKey(st, "t", "k", 0)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("应跳过已清理消息: %d %v", len(msgs), err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/produce ./internal/core/query -v`
Expected: 编译失败（`AppendWith`/`ByKey` 未定义）

- [ ] **Step 3: 实现 produce.AppendWith**

`internal/core/produce/produce.go`（import 补 `github.com/cockroachdb/pebble/v2`）。将现有 `Append` 主体改名为 `AppendWith` 并增强，`Append` 变为薄封装：

```go
// AppendWith 在 Append 基础上，把 extra 组装的额外写操作并入同一原子批次。
//
// 用途：DLQ 转入（写死信消息 + 删源 inflight）、M3 延时转正（写消息 + 删 delay
// 条目）等「消息写入必须与另一处状态变更同生共死」的场景。extra 可为 nil。
//
// 注意：extra 只应操作与本消息无键冲突的 key；extra 内不得再调用本 Producer
// 的任何方法（p.mu 不可重入）。
func (p *Producer) AppendWith(m *core.Message, extra func(b *pebble.Batch)) (*core.Message, error) {
	// ……体检/EnsureTopic/ID/时间戳填充与 M1 的 Append 完全一致……
	// ……p.mu.Lock、队列选择、nextOffsetLocked、EncodeMessage 与 M1 一致……
	b := p.st.NewBatch()
	b.Set(store.MsgKey(m.Topic, m.QueueID, m.Offset), raw, nil)
	b.Set(store.AllocKey(m.Topic, m.QueueID), store.PutU64(off+1), nil)
	// Keys 业务索引与消息同批写入（spec §5 流程 1）：崩溃时索引与消息要么都在要么都不在
	for _, key := range m.Keys {
		if key == "" {
			continue // 空 key 无检索意义（SDK 不会生成，防御脏输入）
		}
		b.Set(store.KeyIdxKey(m.Topic, key, m.StoreAtMs, m.QueueID, m.Offset), nil, nil)
	}
	if extra != nil {
		extra(b)
	}
	if err := p.st.Apply(b); err != nil {
		return nil, fmt.Errorf("写入消息 %s (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}
	// ……p.next 缓存更新、wakeLocked、Debug 日志与 M1 一致……
	return m, nil
}

// Append 写入一条普通消息（M1 签名保持不变）。
func (p *Producer) Append(m *core.Message) (*core.Message, error) { return p.AppendWith(m, nil) }
```

（「与 M1 一致」的段落指原 `Append` 对应代码原样保留在 `AppendWith` 内，此处省略是为了突出差异；执行者做的是**方法改名 + 插入上述三段新代码**，不是重写。）

- [ ] **Step 4: 实现 query.ByKey**

`internal/core/query/query.go` 新建：

```go
// Package query 提供消息检索只读路径（Keys 业务索引查询）。
//
// 职责：
//   - 按 (topic, key) 从 keyidx/ 找回消息，供 M5 Admin API / 控制台复用
//
// 边界：
//   - 只读，不修改任何状态
//   - 索引与消息可能因 retention 清理竞态短暂不一致：消息缺失即跳过，不算错
package query

import (
	"fmt"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/store"
)

// defaultLimit 未指定 limit 时的返回上限（控制台单页量级）。
const defaultLimit = 64

// ByKey 按业务 key 查询 topic 下的消息，按写入时间升序，最多 limit 条（<=0 用 defaultLimit）。
func ByKey(st *store.Store, topic, key string, limit int) ([]*core.Message, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	pfx := store.KeyIdxKeyPrefix(topic, key)
	var out []*core.Message
	err := st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		_, pk, _, q, off, perr := store.ParseKeyIdxKey(k)
		if perr != nil || pk != key {
			// 结构不符或 key 不等值 = 命中了「以本 key 为路径前缀」的其他 key
			//（如查 "a" 扫到 "a/b" 的条目），跳过即可，不是数据损坏
			return true, nil
		}
		raw, ok, gerr := st.Get(store.MsgKey(topic, q, off))
		if gerr != nil {
			return false, gerr
		}
		if !ok {
			return true, nil // 消息已被 retention 清走而索引未清（清理竞态），跳过
		}
		m, derr := core.DecodeMessage(raw)
		if derr != nil {
			return false, derr
		}
		out = append(out, m)
		return len(out) < limit, nil
	})
	if err != nil {
		return nil, fmt.Errorf("按 key 查询 (topic=%s key=%q): %w", topic, key, err)
	}
	return out, nil
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `make test`
Expected: 全部 PASS（produce 既有用例不受影响——`Append` 行为仅多写 keyidx）

- [ ] **Step 6: 核对日志与注释（instrumenting-code）**

- `AppendWith` 的 Debug 日志沿用 M1「消息已写入」，追加 `"keys", len(m.Keys)` 字段（有索引写入时可观测）
- `query.ByKey` 为只读查询路径，错误已带 topic/key 上下文；无状态变更故无 Info 日志（边界已写入文件头）
- 核对：query.go 文件头职责/边界、`ByKey` doc comment、`AppendWith` 的「不可重入」注意事项

- [ ] **Step 7: Commit**

```bash
git add internal/core/produce internal/core/query
git commit -m "feat(produce): AppendWith 原子扩展写与 Keys 索引；query.ByKey 按键检索"
```

---

### Task 5: deliver 服务端 Tag 过滤

**Files:**
- Create: `internal/core/deliver/filter.go`
- Modify: `internal/core/deliver/deliver.go`（`Receive`/`receiveOnce` 签名与阶段 2）
- Modify: `internal/rpc/receive.go`（仅补第 8 个实参 `nil` 保编译，真实接入在 Task 7）
- Test: `internal/core/deliver/filter_test.go`（新建）、`internal/core/deliver/deliver_test.go`

**Interfaces:**
- Produces: `ParseTagFilter(expr string) (*TagFilter, error)`、`(f *TagFilter) Match(tag string) bool`、`Receive(ctx, group, topic string, queueID uint32, maxMsgs int, invisible, wait time.Duration, filter *TagFilter)`
- 语义（spec §5 流程 2）：Tag 不匹配的消息**跳过并推进本消费组位点，不投递、不占 inflight**——对该组永久跳过。阶段 1 重投不重新过滤（inflight 是已投递事实；组内混用不同过滤器属未定义行为，与 RocketMQ 一致）。

- [ ] **Step 1: 写失败测试**

`internal/core/deliver/filter_test.go` 新建：

```go
// TagFilter 解析与匹配单测。
package deliver

import "testing"

func TestParseTagFilter(t *testing.T) {
	if f, err := ParseTagFilter("*"); f != nil || err != nil {
		t.Fatalf("* 应解析为 nil 过滤器: %v %v", f, err)
	}
	if f, err := ParseTagFilter(""); f != nil || err != nil {
		t.Fatalf("空串应解析为 nil 过滤器: %v %v", f, err)
	}
	f, err := ParseTagFilter("a || b")
	if err != nil {
		t.Fatalf("多 tag 解析: %v", err)
	}
	for tag, want := range map[string]bool{"a": true, "b": true, "c": false, "": false} {
		if f.Match(tag) != want {
			t.Fatalf("Match(%q) 应为 %v", tag, want)
		}
	}
	if !(*TagFilter)(nil).Match("anything") {
		t.Fatal("nil 过滤器应匹配一切")
	}
	for _, bad := range []string{"a ||", "||", "a || || b"} {
		if _, err := ParseTagFilter(bad); err == nil {
			t.Fatalf("应拒绝非法表达式 %q", bad)
		}
	}
}
```

`internal/core/deliver/deliver_test.go`：fixture 增 tag 发送 helper，追加过滤用例：

```go
func (f *fixture) sendTagged(t *testing.T, topic, body, tag string) {
	t.Helper()
	if _, err := f.pr.Append(&core.Message{Topic: topic, Body: []byte(body), Tag: tag}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// TestTagFilterDelivery 只投匹配 tag；不匹配的跳过、推进位点、不占 inflight。
func TestTagFilterDelivery(t *testing.T) {
	f := newFixture(t)
	f.sendTagged(t, "t", "a", "tagA") // offset 0
	f.sendTagged(t, "t", "b", "tagB") // offset 1
	f.sendTagged(t, "t", "c", "tagA") // offset 2

	flt, err := ParseTagFilter("tagA")
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, flt)
	if err != nil || len(msgs) != 2 || string(msgs[0].Body) != "a" || string(msgs[1].Body) != "c" {
		t.Fatalf("过滤投递: %d %v", len(msgs), err)
	}
	// b 已被位点跳过：即便换成全量过滤器也收不到（本组永久跳过）
	msgs, err = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("被过滤消息不应再投: %d %v", len(msgs), err)
	}
	// b 不占 inflight
	if _, ok, _ := f.st.Get(store.InflightKey("g", "t", 0, 1)); ok {
		t.Fatal("被过滤消息不应写 inflight")
	}
	// 另一消费组不受影响，能收到全部 3 条
	msgs, err = f.dl.Receive(context.Background(), "g2", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 3 {
		t.Fatalf("其他组应不受过滤影响: %d %v", len(msgs), err)
	}
}

// TestTagFilterAllFilteredAdvancesCursor 全部不匹配时位点仍须持久化推进，
// 否则每次 Receive 重复扫描同一批不匹配消息（性能退化 + 永不前进）。
func TestTagFilterAllFilteredAdvancesCursor(t *testing.T) {
	f := newFixture(t)
	f.sendTagged(t, "t", "x", "tagB")
	flt, _ := ParseTagFilter("tagA")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, flt)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("Receive: %d %v", len(msgs), err)
	}
	v, ok, err := f.st.Get(store.CursorKey("g", "t", 0))
	if err != nil || !ok || store.GetU64(v) != 1 {
		t.Fatalf("位点应推进到 1: ok=%v %v", ok, err)
	}
}
```

同时把本文件**全部既有 `dl.Receive(...)` 调用**补上第 8 个实参 `nil`（共 23 处，散布在 `TestReceiveAckFlow`/`TestUnackedRedeliveryAfterExpire`/`TestAckIdempotent`/`TestLongPollingWakesOnNewMessage`/`TestTwoGroupsIndependentCursors`/`TestRedeliveryFillDoesNotUnboundNewMessageScan`/`TestOrphanInflightCleanupPersistsAndDoesNotReportDelivery`/`TestAckStaleAttemptRejected`/`TestAckCorrectAttemptSucceeds`/`TestChangeInvisible*` 三个用例等 12 个测试函数中；以 `grep -n 'dl.Receive(' deliver_test.go` 实际结果为准）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/deliver -v`
Expected: 编译失败（`ParseTagFilter` 未定义 / `Receive` 参数不符）

- [ ] **Step 3: 实现 filter.go**

```go
// TagFilter：服务端 Tag 过滤（spec §5 流程 2；RocketMQ TAG 表达式子集）。
//
// 职责：
//   - 解析 "*" / "tagA" / "tagA || tagB" 三种 TAG 表达式
//   - O(1) 判定消息 tag 是否命中
//
// 边界：
//   - SQL92 属性过滤按 spec 排到 v1.1，不在此处
//   - 过滤的位点语义（跳过即永久越过）由 deliver 主流程负责，本文件只管匹配
package deliver

import (
	"fmt"
	"strings"
)

// TagFilter 不可变 tag 集合过滤器。nil 值匹配一切（等价 "*"）。
type TagFilter struct {
	tags map[string]struct{}
}

// ParseTagFilter 解析 TAG 过滤表达式。"*" 与空串返回 (nil, nil) 表示不过滤；
// 其余按 "||" 分隔为 tag 集合；出现空 token（如 "a ||"）视为非法。
func ParseTagFilter(expr string) (*TagFilter, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" || expr == "*" {
		return nil, nil
	}
	set := map[string]struct{}{}
	for _, tok := range strings.Split(expr, "||") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			return nil, fmt.Errorf("TAG 表达式含空 tag: %q", expr)
		}
		set[tok] = struct{}{}
	}
	return &TagFilter{tags: set}, nil
}

// Match 判定 tag 是否命中。区分大小写（与 RocketMQ 一致）。
func (f *TagFilter) Match(tag string) bool {
	if f == nil {
		return true
	}
	_, ok := f.tags[tag]
	return ok
}
```

- [ ] **Step 4: 实现 deliver.go 阶段 2 改造**

`Receive` 签名末尾增 `filter *TagFilter`，透传给 `receiveOnce`（其签名同步增参）。文件顶新增常量：

```go
// scanBudget 单次取件最多检视的新消息条数。Tag 过滤可能连续跳过大量不匹配
// 消息，必须限制单趟工作量；位点照常推进，下一趟从新位点继续，保证前进性。
// M1 时 Scan 的 limit 用 maxMsgs-len(out)（检视数=投递数）；过滤下两者分离：
// 检视上限用本常量，投递上限由回调内 len(out) < maxMsgs 控制。
const scanBudget = 1024
```

`receiveOnce` 阶段 2 改造。既有骨架**保留不动**：cursor 读取、外层 `if len(out) < maxMsgs` 守卫（防 `maxMsgs-len(out)==0` 被 Scan 当"不限"的注释与逻辑）、扫描后的 `if newCursor > cursor { staged = true }`、函数尾部集中写位点与 `staged` 判定 Close/Apply。只替换 Scan 调用本身：limit 由 `maxMsgs-len(out)` 改为 `scanBudget`，回调加过滤分支：

```go
		lower := store.MsgKey(topic, queueID, cursor)
		upper := store.PrefixUpperBound(store.MsgQueuePrefix(topic, queueID))
		skipped := 0
		err = d.st.Scan(lower, upper, scanBudget, func(k, v []byte) (bool, error) {
			m, err := core.DecodeMessage(v)
			if err != nil {
				return false, err
			}
			// 位点始终越过已检视消息：Tag 不匹配即对本消费组永久跳过
			//（spec §5 流程 2：不投递、不占 inflight、推进本组视角位点）。
			// 全部被过滤（out 为空）时位点也必须持久化——newCursor 前进即触发
			// 下方既有的 staged 判定进 Apply——否则下趟重复扫描同一批不匹配消息。
			newCursor = m.Offset + 1
			if !filter.Match(m.Tag) {
				skipped++
				return true, nil
			}
			m.DeliveryAttempt = 1
			b.Set(store.InflightKey(group, topic, queueID, m.Offset),
				core.EncodeInflight(&core.InflightState{ExpireAtMs: expireAt, Attempts: 1}), nil)
			out = append(out, m)
			return len(out) < maxMsgs, nil
		})
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("扫描新消息 (topic=%s q=%d cursor=%d): %w", topic, queueID, cursor, err)
		}
		if newCursor > cursor {
			staged = true
		}
		if skipped > 0 {
			d.logger.Debug("Tag 过滤跳过消息", "group", group, "topic", topic, "queue", queueID, "skipped", skipped)
		}
```

（`staged`/`b.Close()`/`Apply`/末尾 Debug 日志等骨架沿用基线现状，不重写；包头注释中「M1 只接受 "*"」的边界说明同步改为「Tag 过滤已落地，SQL92 属 v1.1」。）

- [ ] **Step 5: rpc 调用点保编译**

`internal/rpc/receive.go` 的 `s.dl.Receive(...)` 调用补第 8 个实参 `nil`；既有 `"*"`-only 过滤守卫**保持不变**（真实表达式接入是 Task 7 的事，本步只保证编译与既有行为不变）。

- [ ] **Step 6: 跑测试确认通过**

Run: `make test`
Expected: 全部 PASS（含 deliver 既有 7 个用例）

- [ ] **Step 7: 核对日志与注释（instrumenting-code）**

- 过滤跳过有 Debug 日志（带 group/topic/queue/skipped，高频路径降级 Debug——上码已含）
- `scanBudget` 常量注释解释了「为什么要预算」；位点推进语义注释在跳过分支处（上码已含）
- filter.go 文件头 + 2 导出符号 doc comment（上码已含）
- 基线 `receiveOnce` 收尾处 `len(out)==0` 的那条「本轮无可投递消息，仅清理了孤儿 inflight 记录」Debug 日志：措辞与注释改为同时覆盖「本轮全部被过滤、只推进了位点」的新情况，避免运维误读

- [ ] **Step 8: Commit**

```bash
git add internal/core/deliver internal/rpc
git commit -m "feat(deliver): 服务端 Tag 过滤（位点跳过式、单趟扫描预算）"
```

---

### Task 6: deliver 重试指数退避 + 投递超限转 DLQ

**Files:**
- Modify: `internal/core/deliver/deliver.go`（阶段 1 改造 + `moveToDLQ`）
- Test: `internal/core/deliver/deliver_test.go`

**Interfaces:**
- Consumes: Task 3 `meta.DLQTopicName`/`EffectiveMaxAttempts`、Task 4 `produce.AppendWith`
- Produces: 行为——过期重投第 n 次（n≥2）不可见时长下限 `retryBackoffBase×2^(n-2)` 封顶 `retryBackoffMax`；attempts 达组上限后转 `%DLQ%{group}`（1 队列）并原子删 inflight
- 内部：`retryBackoff(attempts int32) time.Duration`、`moveToDLQ(group, topic string, queueID uint32, offset uint64, m *core.Message) error`

- [ ] **Step 1: 写失败测试**

`internal/core/deliver/deliver_test.go` 追加。fixture 需要可指定 maxAttempts 的变体：

```go
// newFixtureMaxAttempts 指定组默认 maxAttempts 的 fixture（DLQ 用例用小值控制时长）。
func newFixtureMaxAttempts(t *testing.T, maxAttempts int32) *fixture {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, maxAttempts, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(st, mt, slog.Default())
	return &fixture{st: st, pr: pr, dl: New(st, mt, pr, slog.Default())}
}
```

（既有 `newFixture` 改为 `return newFixtureMaxAttempts(t, 16)`，消重。）

```go
func TestRetryBackoffTable(t *testing.T) {
	cases := map[int32]time.Duration{2: 10 * time.Second, 3: 20 * time.Second, 4: 40 * time.Second, 30: 5 * time.Minute}
	for attempts, want := range cases {
		if got := retryBackoff(attempts); got != want {
			t.Fatalf("retryBackoff(%d) = %v，期望 %v", attempts, got, want)
		}
	}
}

// TestRedeliveryUsesBackoffFloor 重投时不可见时长取 max(客户端要求, 退避下限)。
func TestRedeliveryUsesBackoffFloor(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	// 首投 20ms 不可见，过期
	if msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, 20*time.Millisecond, 0, nil); len(msgs) != 1 {
		t.Fatal("首投失败")
	}
	time.Sleep(40 * time.Millisecond)
	// 第 2 次投递：客户端只要 20ms，但退避下限 10s 生效
	before := time.Now().UnixMilli()
	if msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, 20*time.Millisecond, 0, nil); len(msgs) != 1 {
		t.Fatal("重投失败")
	}
	v, ok, err := f.st.Get(store.InflightKey("g", "t", 0, 0))
	if err != nil || !ok {
		t.Fatalf("inflight 缺失: %v", err)
	}
	st2, err := core.DecodeInflight(v)
	if err != nil {
		t.Fatal(err)
	}
	if st2.ExpireAtMs-before < (10 * time.Second).Milliseconds() {
		t.Fatalf("退避下限未生效: expire 距 now 仅 %dms", st2.ExpireAtMs-before)
	}
}

// TestExhaustedAttemptsGoToDLQ 投递次数耗尽后转入 %DLQ%{group}，原队列不再投递。
func TestExhaustedAttemptsGoToDLQ(t *testing.T) {
	// 缩小退避基数，让第 2 次投递快速过期（var 供测试注入，见实现注释）
	oldBase := retryBackoffBase
	retryBackoffBase = 10 * time.Millisecond
	defer func() { retryBackoffBase = oldBase }()

	f := newFixtureMaxAttempts(t, 2)
	f.send(t, "t", "poison")
	// 第 1、2 次投递均不 ack、任其过期
	for i := 0; i < 2; i++ {
		msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, 20*time.Millisecond, 0, nil)
		if err != nil || len(msgs) != 1 {
			t.Fatalf("第 %d 次投递: %d %v", i+1, len(msgs), err)
		}
		time.Sleep(60 * time.Millisecond) // > invisible 与退避的较大者
	}
	// 第 3 次 Receive 触发 DLQ 转入，原队列返回空
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("超限后原队列不应再投: %d %v", len(msgs), err)
	}
	// inflight 已删
	if _, ok, _ := f.st.Get(store.InflightKey("g", "t", 0, 0)); ok {
		t.Fatal("DLQ 转入后 inflight 未删除")
	}
	// 死信可从 %DLQ%g 消费，带来源属性
	dlq, err := f.dl.Receive(context.Background(), "dlq-reader", meta.DLQTopicName("g"), 0, 10, time.Minute, 0, nil)
	if err != nil || len(dlq) != 1 {
		t.Fatalf("DLQ 消费: %d %v", len(dlq), err)
	}
	if string(dlq[0].Body) != "poison" || dlq[0].Properties["sq-origin-topic"] != "t" {
		t.Fatalf("死信内容/来源属性不符: %s %v", dlq[0].Body, dlq[0].Properties)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/deliver -run 'TestRetryBackoff|TestRedeliveryUsesBackoffFloor|TestExhaustedAttemptsGoToDLQ' -v`
Expected: 编译失败（`retryBackoff` 未定义）

- [ ] **Step 3: 实现**

`internal/core/deliver/deliver.go`（import 补 `strconv`、`github.com/cockroachdb/pebble/v2`）：

```go
// 重试退避参数（spec §5 流程 6：非顺序消息重试指数退避，靠重投时的不可见时长实现）。
// 第 n 次投递（n≥2）的不可见时长下限 = base × 2^(n-2)，封顶 max：10s, 20s, 40s … 5min。
// 客户端要求的 invisible 更长时取客户端值。
// var 而非 const：测试需注入小值控制用例时长；运行期只读。
var (
	retryBackoffBase = 10 * time.Second
	retryBackoffMax  = 5 * time.Minute
)

// retryBackoff 第 attempts 次投递的退避下限（attempts >= 2；乘法防溢出走上限）。
func retryBackoff(attempts int32) time.Duration {
	d := retryBackoffBase
	for i := int32(2); i < attempts; i++ {
		d *= 2
		if d >= retryBackoffMax {
			return retryBackoffMax
		}
	}
	if d >= retryBackoffMax {
		return retryBackoffMax
	}
	return d
}
```

`Receive`：`EnsureGroup` 的返回值不再丢弃，透传组上限：

```go
	gc, err := d.mt.EnsureGroup(group)
	if err != nil {
		return nil, err
	}
	// …循环内：
	msgs, err := d.receiveOnce(group, topic, queueID, maxMsgs, invisible, gc.EffectiveMaxAttempts(), filter)
```

`receiveOnce` 签名：`receiveOnce(group, topic string, queueID uint32, maxMsgs int, invisible time.Duration, maxAttempts int32, filter *TagFilter)`。阶段 1 的 `for _, r := range reds` 重投循环整段替换为（Get 失败/解码失败/孤儿清理三个分支与基线逐字相同——**孤儿清理处那段「必须真正提交否则日志洪水」的既有注释原样保留**——只插入 DLQ 分支与退避计算两段新代码）：

```go
	for _, r := range reds {
		raw, ok, err := d.st.Get(store.MsgKey(topic, queueID, r.offset))
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("读取重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, r.offset, err)
		}
		if !ok {
			// （基线既有孤儿清理分支与注释，原样保留）
			d.logger.Warn("inflight 指向的消息不存在，清理孤儿记录", "group", group, "topic", topic, "queue", queueID, "offset", r.offset)
			b.Delete(store.InflightKey(group, topic, queueID, r.offset), nil)
			staged = true
			continue
		}
		m, err := core.DecodeMessage(raw)
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("解码重投消息 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, r.offset, err)
		}
		// 投递次数耗尽：转入死信 topic，不再投递。
		// DLQ 写入与 inflight 删除经 AppendWith 同批原子提交，与本函数的取件批次 b
		// 相互独立（此消息不进 b，不置 staged）。崩溃窗口内至多重复转入
		//（at-least-once），死信消费端按消息 ID 幂等即可。
		// 锁语义没有破坏：本方法已持队列锁，AppendWith 拿的是 produce 自己的锁，
		// 两把锁全程单向（deliver → produce），无环即无死锁。
		if r.attempts >= maxAttempts {
			if err := d.moveToDLQ(group, topic, queueID, r.offset, m); err != nil {
				b.Close()
				return nil, err
			}
			continue
		}
		m.DeliveryAttempt = r.attempts + 1
		// 指数退避：不可见时长取客户端要求与退避下限的较大者
		exp := expireAt
		if bo := retryBackoff(m.DeliveryAttempt); bo > invisible {
			exp = now + bo.Milliseconds()
		}
		b.Set(store.InflightKey(group, topic, queueID, r.offset),
			core.EncodeInflight(&core.InflightState{ExpireAtMs: exp, Attempts: m.DeliveryAttempt}), nil)
		out = append(out, m)
		staged = true
	}
```

新增方法：

```go
// moveToDLQ 将投递次数耗尽的消息复制入 %DLQ%{group} 并原子删除源 inflight。
//
// 死信保留原消息 ID/Body/Tag/Keys（ID 不变便于全链路追踪），来源坐标写入
// Properties（sq-origin-topic/queue/offset，控制台溯源与重发用）；
// MessageGroup 置空——死信不再参与顺序语义。
func (d *Deliverer) moveToDLQ(group, topic string, queueID uint32, offset uint64, m *core.Message) error {
	dlqTopic := meta.DLQTopicName(group)
	// 死信 topic 固定 1 队列：量小、顺序无关、控制台浏览简单。CreateTopic 幂等。
	if _, err := d.mt.CreateTopic(dlqTopic, 1); err != nil {
		return fmt.Errorf("创建 DLQ topic %s: %w", dlqTopic, err)
	}
	props := make(map[string]string, len(m.Properties)+3)
	for k, v := range m.Properties {
		props[k] = v
	}
	props["sq-origin-topic"] = topic
	props["sq-origin-queue"] = strconv.FormatUint(uint64(queueID), 10)
	props["sq-origin-offset"] = strconv.FormatUint(offset, 10)
	dlq := &core.Message{
		ID: m.ID, Topic: dlqTopic, Tag: m.Tag, Keys: m.Keys,
		Properties: props, Body: m.Body, BornAtMs: m.BornAtMs,
	}
	infKey := store.InflightKey(group, topic, queueID, offset)
	if _, err := d.pr.AppendWith(dlq, func(b *pebble.Batch) { b.Delete(infKey, nil) }); err != nil {
		return fmt.Errorf("消息转入 DLQ (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Info("消息投递超限转入死信", "group", group, "topic", topic, "queue", queueID,
		"offset", offset, "msg_id", m.ID, "dlq_topic", dlqTopic)
	return nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `make test`
Expected: 全部 PASS，但有一个**已知必改的基线用例**：`TestAckStaleAttemptRejected` 的结尾依赖「attempt=2 的 inflight 在 300ms 内再过期、重投出 attempt=3」来证明陈旧 ack 没删掉记录——退避下限（attempt=2 → 10s）会让这一步永远等不到。本 task 内在该用例开头按 `TestExhaustedAttemptsGoToDLQ` 同款方式注入 `retryBackoffBase = 10 * time.Millisecond`（defer 恢复原值），用例逻辑与断言不动。其余既有用例不受影响：`TestUnackedRedeliveryAfterExpire`/`TestAckCorrectAttemptSucceeds` 只依赖首轮过期（attempt 1 的过期时间不走退避，退避从重投即 attempt≥2 起算），`TestChangeInvisiblePreservesAttempts` 经 ChangeInvisible 显式改写过期时间（消费端主动指定，语义上就该绕过服务端退避下限）。

- [ ] **Step 5: 核对日志与注释（instrumenting-code）**

- DLQ 转入是关键状态变更 → Info 日志带全坐标（上码已含）
- `moveToDLQ` 错误分支全部带 group/topic/queue/offset（上码已含）
- 核对 `retryBackoff` 的「为什么用 var」注释、DLQ 原子性注释

- [ ] **Step 6: Commit**

```bash
git add internal/core/deliver
git commit -m "feat(deliver): 重试指数退避 + 投递超限转 DLQ（AppendWith 原子转入）"
```

---

### Task 7: rpc 接入真实 TAG 过滤表达式

**Files:**
- Modify: `internal/rpc/receive.go`
- Test: `internal/rpc/receive_test.go`

**Interfaces:**
- Consumes: Task 5 `deliver.ParseTagFilter`、`Receive(..., filter)`
- Produces: `ReceiveMessage` 支持 `"*"`/单 tag/`"a || b"`；SQL92 与非法表达式返回流内 `ILLEGAL_FILTER_EXPRESSION`

- [ ] **Step 1: 写失败测试**

`internal/rpc/receive_test.go` 追加：

```go
// sendTagged 发送带 tag 的消息（Tag 是 *string，需取址）。
func sendTagged(t *testing.T, c pb.MessagingServiceClient, topic, body, tag string) {
	t.Helper()
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: topic},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL, Tag: &tag},
			Body:             []byte(body),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("send: %v %v", resp.GetStatus(), err)
	}
}

// receiveQueue 从指定队列收一次，返回消息（helper：给过滤用例复用）。
// 3s deadline 让空队列长轮询快速返回（服务端 wait=deadline-1s≈2s），
// 否则无 deadline 时默认长轮询 20s，空队列用例会拖慢整个测试。
func receiveQueue(t *testing.T, c pb.MessagingServiceClient, group, topic string, q int32, expr string) []*pb.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: group},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: topic}, Id: q},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: expr},
		BatchSize:         16,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	msgs, _ := recvAll(t, stream)
	return msgs
}

// TestReceiveTagFilter 8 条消息 tagA/tagB 交替，按 tagA 过滤只收 4 条 A；
// 被过滤的 B 已被位点跳过，事后用 "*" 也收不到。
func TestReceiveTagFilter(t *testing.T) {
	c := newTestClient(t)
	for i := 0; i < 8; i++ {
		tag, body := "tagA", "a"
		if i%2 == 1 {
			tag, body = "tagB", "b"
		}
		sendTagged(t, c, "tf", body, tag)
	}
	var got []string
	for q := int32(0); q < 4; q++ {
		for _, m := range receiveQueue(t, c, "g-tf", "tf", q, "tagA") {
			got = append(got, string(m.GetBody()))
		}
	}
	if len(got) != 4 {
		t.Fatalf("tagA 消息数: %d (%v)", len(got), got)
	}
	for _, b := range got {
		if b != "a" {
			t.Fatalf("混入非 tagA 消息: %v", got)
		}
	}
	for q := int32(0); q < 4; q++ {
		if rest := receiveQueue(t, c, "g-tf", "tf", q, "*"); len(rest) != 0 {
			t.Fatalf("被过滤消息不应可再收: %d", len(rest))
		}
	}
}

// TestReceiveRejectsUnsupportedFilter SQL92 与非法 TAG 表达式返回 ILLEGAL_FILTER_EXPRESSION。
func TestReceiveRejectsUnsupportedFilter(t *testing.T) {
	c := newTestClient(t)
	sendOne(t, c, "tf-bad", "x")
	cases := []*pb.FilterExpression{
		{Type: pb.FilterType_SQL, Expression: "a > 1"},
		{Type: pb.FilterType_TAG, Expression: "a ||"},
	}
	for _, fe := range cases {
		stream, err := c.ReceiveMessage(context.Background(), &pb.ReceiveMessageRequest{
			Group:             &pb.Resource{Name: "g-bad"},
			MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: "tf-bad"}, Id: 0},
			FilterExpression:  fe,
			BatchSize:         1,
			InvisibleDuration: durationpb.New(time.Minute),
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		msgs, st := recvAll(t, stream)
		if len(msgs) != 0 || st.GetCode() != pb.Code_ILLEGAL_FILTER_EXPRESSION {
			t.Fatalf("期望 ILLEGAL_FILTER_EXPRESSION，得到 %v (msgs=%d)", st, len(msgs))
		}
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc -run 'TestReceiveTagFilter|TestReceiveRejectsUnsupportedFilter' -v`
Expected: `TestReceiveTagFilter` FAIL——现有实现对 `"tagA"` 表达式直接整流拒绝（M1 只放行 `"*"`）

- [ ] **Step 3: 实现**

`internal/rpc/receive.go` 的 `ReceiveMessage`：删除 M1 的 `"*"`-only 守卫，替换为：

```go
	// TAG 表达式解析（M2）：支持 "*" / 单 tag / "a || b"。SQL92 → v1.1。
	var filter *deliver.TagFilter
	if fe := req.GetFilterExpression(); fe != nil {
		if fe.GetType() != pb.FilterType_TAG {
			s.logger.Warn("不支持的过滤类型", "group", group, "topic", topic, "type", fe.GetType())
			return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
				Status: errStatus(pb.Code_ILLEGAL_FILTER_EXPRESSION, "仅支持 TAG 过滤（SQL92 计划 v1.1）"),
			}})
		}
		f, err := deliver.ParseTagFilter(fe.GetExpression())
		if err != nil {
			s.logger.Warn("TAG 表达式非法", "group", group, "topic", topic, "expr", fe.GetExpression(), "err", err)
			return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
				Status: errStatus(pb.Code_ILLEGAL_FILTER_EXPRESSION, err.Error()),
			}})
		}
		filter = f
	}
```

`Receive` 调用的第 8 个实参由 `nil` 改为 `filter`（import `deliver` M1 已有）。

- [ ] **Step 4: 跑测试确认通过**

Run: `make test`
Expected: 全部 PASS（`TestReceiveAckRoundTrip` 用 `"*"` → 解析为 nil 过滤器，行为不变）

- [ ] **Step 5: 核对日志与注释（instrumenting-code）**

拒绝分支均有 Warn 日志带 group/topic/expr（上码已含）；无新导出符号。

- [ ] **Step 6: Commit**

```bash
git add internal/rpc
git commit -m "feat(rpc): ReceiveMessage 接入 TAG 过滤表达式（SQL92 拒绝）"
```

---

### Task 8: retention 后台清理任务

M1 之后第一个后台 goroutine。设计取舍（写进文件头）：msg 用 DeleteRange 按 offset 边界整段清理（队列内 StoreAtMs 随 offset 单调，扫到首条未过期即边界）；keyidx 按 key 排序无法按时间 DeleteRange，只能全扫按嵌入的 storeMs 逐条删；两者都有单趟上限，超出留待下趟（不静默截断）。

**Files:**
- Create: `internal/core/retention/retention.go`
- Modify: `internal/config/config.go`（`retention_check_interval`）
- Modify: `cmd/sq/main.go`（装配 + 停机顺序）
- Test: `internal/core/retention/retention_test.go`、`internal/config/config_test.go`

**Interfaces:**
- Consumes: Task 3 `meta.Topics()`/`EffectiveRetention()`、Task 2 `KeyIdxTopicPrefix`/`ParseKeyIdxKey`
- Produces: `retention.New(st, mt, interval, logger) *Manager`（Task 9 会扩签名）、`(m *Manager) Run(ctx)`、`(m *Manager) Pass() (int, error)`；`config.Config.RetentionCheckInterval string` + `(c *Config) RetentionInterval() time.Duration`

- [ ] **Step 1: 写失败测试**

`internal/core/retention/retention_test.go` 新建：

```go
// retention 清理任务测试：直接向 store 注入带旧 StoreAtMs 的消息制造过期数据
//（produce.Append 总用当前时间，无法造旧数据）。
package retention

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

func newFixture(t *testing.T) (*store.Store, *meta.Meta) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	return st, mt
}

// writeMsgAt 以指定 StoreAtMs 直写一条消息（含 alloc 计数器与 keyidx，模拟真实写入）。
func writeMsgAt(t *testing.T, st *store.Store, topic string, offset uint64, storeAt int64, keys ...string) {
	t.Helper()
	m := &core.Message{ID: core.NewMessageID(), Topic: topic, QueueID: 0, Offset: offset,
		Keys: keys, Body: []byte("x"), BornAtMs: storeAt, StoreAtMs: storeAt}
	raw, err := core.EncodeMessage(m)
	if err != nil {
		t.Fatal(err)
	}
	b := st.NewBatch()
	b.Set(store.MsgKey(topic, 0, offset), raw, nil)
	b.Set(store.AllocKey(topic, 0), store.PutU64(offset+1), nil)
	for _, k := range keys {
		b.Set(store.KeyIdxKey(topic, k, storeAt, 0, offset), nil, nil)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

// TestPassPurgesExpired 过期消息与索引被清，未过期保留。
func TestPassPurgesExpired(t *testing.T) {
	st, mt := newFixture(t)
	if _, err := mt.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli() // 超过默认 3 天
	fresh := time.Now().UnixMilli()
	writeMsgAt(t, st, "t", 0, old, "k-old")
	writeMsgAt(t, st, "t", 1, fresh, "k-new")

	m := New(st, mt, time.Minute, slog.Default())
	n, err := m.Pass()
	if err != nil || n != 1 {
		t.Fatalf("Pass: %d %v", n, err)
	}
	if _, ok, _ := st.Get(store.MsgKey("t", 0, 0)); ok {
		t.Fatal("过期消息未清理")
	}
	if _, ok, _ := st.Get(store.MsgKey("t", 0, 1)); !ok {
		t.Fatal("未过期消息被误删")
	}
	// 索引：旧删新留
	oldIdx := store.KeyIdxKeyPrefix("t", "k-old")
	gone := true
	st.Scan(oldIdx, store.PrefixUpperBound(oldIdx), 0, func(k, v []byte) (bool, error) { gone = false; return false, nil })
	if !gone {
		t.Fatal("过期索引未清理")
	}
	newIdx := store.KeyIdxKeyPrefix("t", "k-new")
	kept := false
	st.Scan(newIdx, store.PrefixUpperBound(newIdx), 0, func(k, v []byte) (bool, error) { kept = true; return false, nil })
	if !kept {
		t.Fatal("未过期索引被误删")
	}
}

// TestPassIdempotentAndNoExpired 无过期数据时 Pass 是无害空转。
func TestPassIdempotentAndNoExpired(t *testing.T) {
	st, mt := newFixture(t)
	mt.CreateTopic("t", 1)
	writeMsgAt(t, st, "t", 0, time.Now().UnixMilli())
	m := New(st, mt, time.Minute, slog.Default())
	for i := 0; i < 2; i++ {
		if n, err := m.Pass(); err != nil || n != 0 {
			t.Fatalf("第 %d 次 Pass: %d %v", i+1, n, err)
		}
	}
}

// TestConsumeAfterPurge 清理后消费从位点扫描自然越过已删区间（cursor 无需修正）。
func TestConsumeAfterPurge(t *testing.T) {
	st, mt := newFixture(t)
	mt.CreateTopic("t", 1)
	old := time.Now().Add(-4 * 24 * time.Hour).UnixMilli()
	writeMsgAt(t, st, "t", 0, old)
	writeMsgAt(t, st, "t", 1, time.Now().UnixMilli())
	if _, err := New(st, mt, time.Minute, slog.Default()).Pass(); err != nil {
		t.Fatal(err)
	}
	pr := produce.New(st, mt, slog.Default())
	dl := deliver.New(st, mt, pr, slog.Default())
	msgs, err := dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0, nil)
	if err != nil || len(msgs) != 1 || msgs[0].Offset != 1 {
		t.Fatalf("清理后消费: %d %v", len(msgs), err)
	}
}

// TestRunStopsOnCancel Run 循环响应 ctx 取消退出（停机路径）。
func TestRunStopsOnCancel(t *testing.T) {
	st, mt := newFixture(t)
	m := New(st, mt, time.Hour, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未响应取消")
	}
}
```

`internal/config/config_test.go` 追加：

```go
func TestRetentionInterval(t *testing.T) {
	cfg, err := Load("")
	if err != nil || cfg.RetentionInterval() != 5*time.Minute {
		t.Fatalf("默认 retention 间隔: %v %v", cfg.RetentionCheckInterval, err)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	os.WriteFile(p, []byte("retention_check_interval: nonsense\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("应拒绝非法 interval")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/retention ./internal/config -v`
Expected: 编译失败（包不存在 / 字段未定义）

- [ ] **Step 3: 实现 config**

`Config` 增 `RetentionCheckInterval string \`yaml:"retention_check_interval"\` // 过期清理扫描间隔（Go duration 格式）`；默认 `"5m"`；`Load` 校验块追加（import 补 `time`）：

```go
	if d, err := time.ParseDuration(cfg.RetentionCheckInterval); err != nil || d <= 0 {
		return nil, fmt.Errorf("配置 retention_check_interval 须为正 duration（如 5m），得到 %q", cfg.RetentionCheckInterval)
	}
```

```go
// RetentionInterval 解析后的清理扫描间隔（Load 已校验合法，此处不会失败）。
func (c *Config) RetentionInterval() time.Duration {
	d, _ := time.ParseDuration(c.RetentionCheckInterval)
	return d
}
```

同 task 更新 `test/e2e/sdk_test.go` 的 broker 配置构造：补 `RetentionCheckInterval: "5m"`（否则 yaml 序列化出空串被新校验拒绝，理由见执行基线说明）。

- [ ] **Step 4: 实现 retention.go**

```go
// Package retention 实现消息过期清理后台任务（spec §5 流程 7）。
//
// 职责：
//   - 周期扫描全部 topic：按各自 retention 时长清理过期 msg/（DeleteRange）
//     与对应 keyidx/ 条目
//   - 单趟工作量有上限（maxPurgePerQueue），超出留待下趟并记日志（不静默截断）
//
// 边界：
//   - 不清理 cursor/inflight：消费位点扫描天然越过已删区间；指向已删消息的
//     inflight 由 deliver 的孤儿清理兜底（M1 已实现并有用例钉住）
//   - msg 能按 offset 边界 DeleteRange（队列内 StoreAtMs 随 offset 单调）；
//     keyidx 按 key 排序，只能全扫按嵌入 storeMs 逐条删——中小规模可接受，
//     量级上来后的优化（时间副索引）留给真实瓶颈出现时
package retention

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// maxPurgePerQueue 单队列/单索引扫描单趟最多清理条数，防止单趟长时间占用。
const maxPurgePerQueue = 10000

// Manager 过期清理任务。单 goroutine 运行（Run），Pass 可单独调用（测试/未来 Admin 触发）。
type Manager struct {
	st       *store.Store
	mt       *meta.Meta
	interval time.Duration
	logger   *slog.Logger
}

// New 构造清理任务。interval 为扫描间隔（config.RetentionInterval()）。
func New(st *store.Store, mt *meta.Meta, interval time.Duration, logger *slog.Logger) *Manager {
	return &Manager{st: st, mt: mt, interval: interval, logger: logger.With("mod", "retention")}
}

// Run 阻塞运行清理循环：启动即跑一趟，此后每 interval 一趟；ctx 取消即返回。
// 调用方（main）负责放入独立 goroutine 并在停机时先取消再关 store。
func (m *Manager) Run(ctx context.Context) {
	m.logger.Info("retention 任务启动", "interval", m.interval.String())
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		if n, err := m.Pass(); err != nil {
			m.logger.Error("retention 清理失败", "err", err)
		} else if n > 0 {
			m.logger.Info("retention 清理完成", "purged_msgs", n)
		}
		select {
		case <-ctx.Done():
			m.logger.Info("retention 任务退出")
			return
		case <-t.C:
		}
	}
}

// Pass 执行一趟全量清理，返回清掉的消息条数（keyidx 条目不计入）。
func (m *Manager) Pass() (int, error) {
	now := time.Now().UnixMilli()
	total := 0
	for _, tc := range m.mt.Topics() {
		cutoff := now - tc.EffectiveRetention().Milliseconds()
		for q := uint32(0); q < tc.Queues; q++ {
			n, err := m.purgeQueue(tc.Name, q, cutoff)
			if err != nil {
				return total, fmt.Errorf("清理 %s/q%d: %w", tc.Name, q, err)
			}
			total += n
		}
		if err := m.purgeKeyIdx(tc.Name, cutoff); err != nil {
			return total, fmt.Errorf("清理 keyidx %s: %w", tc.Name, err)
		}
	}
	return total, nil
}

// purgeQueue 找到 [队首, 首条未过期) 边界并 DeleteRange 整段删除。
// 队列内消息按 offset 追加写入、StoreAtMs 单调不减，扫到首条未过期即可停。
func (m *Manager) purgeQueue(topic string, q uint32, cutoff int64) (int, error) {
	pfx := store.MsgQueuePrefix(topic, q)
	var boundary uint64
	found := 0
	err := m.st.Scan(pfx, store.PrefixUpperBound(pfx), maxPurgePerQueue, func(k, v []byte) (bool, error) {
		msg, err := core.DecodeMessage(v)
		if err != nil {
			return false, fmt.Errorf("解码 %q: %w", k, err)
		}
		if msg.StoreAtMs >= cutoff {
			return false, nil
		}
		boundary = msg.Offset + 1
		found++
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	if found == 0 {
		return 0, nil
	}
	b := m.st.NewBatch()
	// DeleteRange 从 offset 0 起：此前趟次已删的区间为空集，重复覆盖无害
	b.DeleteRange(store.MsgKey(topic, q, 0), store.MsgKey(topic, q, boundary), nil)
	if err := m.st.Apply(b); err != nil {
		return 0, fmt.Errorf("DeleteRange 提交: %w", err)
	}
	if found == maxPurgePerQueue {
		m.logger.Info("retention 达单趟上限，剩余留待下趟", "topic", topic, "queue", q)
	}
	m.logger.Debug("retention 清理队列", "topic", topic, "queue", q, "purged", found, "boundary", boundary)
	return found, nil
}

// purgeKeyIdx 清理 topic 下 storeMs < cutoff 的索引条目（全扫逐删，见文件头边界说明）。
func (m *Manager) purgeKeyIdx(topic string, cutoff int64) error {
	pfx := store.KeyIdxTopicPrefix(topic)
	b := m.st.NewBatch()
	n := 0
	err := m.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		_, _, ms, _, _, perr := store.ParseKeyIdxKey(k)
		if perr != nil {
			return false, perr
		}
		if ms < cutoff {
			b.Delete(k, nil) // Batch 编码时即拷贝 key，回调切片可直接用
			n++
		}
		return n < maxPurgePerQueue, nil
	})
	if err != nil {
		b.Close()
		return err
	}
	if n == 0 {
		b.Close()
		return nil
	}
	if err := m.st.Apply(b); err != nil {
		return fmt.Errorf("索引删除提交: %w", err)
	}
	m.logger.Debug("retention 清理索引", "topic", topic, "purged_idx", n)
	return nil
}
```

- [ ] **Step 5: main 装配**

`cmd/sq/main.go` 的 `run()`：组件构造后（`dl := deliver.New(...)` 之后）、`net.Listen` 前插入（import 补 `context`、`sync`、`github.com/xushixin/sq/internal/core/retention`——基线 main.go 尚未 import context）。`defer st.Close()` 在基线 `run()` 开头已挂好，本 defer 注册在它之后、按 LIFO 先执行，满足「先停清理再关 store」：

```go
	// retention 后台清理。停机顺序关键：先取消并等待清理 goroutine 退出，
	// 再让 defer 关闭 store——否则可能在 store 关闭后提交清理批次（panic）。
	// defer 为 LIFO：本 defer 注册在 st.Close 的 defer 之后，故先执行。
	retCtx, retCancel := context.WithCancel(context.Background())
	var retWG sync.WaitGroup
	rm := retention.New(st, mt, cfg.RetentionInterval(), logger)
	retWG.Add(1)
	go func() { defer retWG.Done(); rm.Run(retCtx) }()
	defer func() { retCancel(); retWG.Wait() }()
```

- [ ] **Step 6: 跑测试确认通过**

Run: `make test && go build ./...`
Expected: 全部 PASS

- [ ] **Step 7: 核对日志与注释（instrumenting-code）**

- 任务启动/退出 Info、每趟有清理时 Info（purged_msgs）、失败 Error、单队列明细 Debug、达上限明示 Info——成功路径与截断都不静默（上码已含）
- 文件头两条设计取舍（DeleteRange 边界依据、keyidx 全扫原因）已写明
- main 的停机顺序注释解释了 defer LIFO 依赖（上码已含）

- [ ] **Step 8: Commit**

```bash
git add internal/core/retention internal/config cmd/sq
git commit -m "feat(retention): 按 topic 保留时长后台清理 msg 与 keyidx"
```

---

### Task 9: 磁盘水位保护（拒写保读，spec §7）

spec §7 的可靠性条目，与 retention 同属存储卫生、共用后台循环，提前到 M2 落地。分工：retention 循环每趟探测磁盘用量并更新共享 `atomic.Bool`；rpc 层 `SendMessage` 检查该开关，超水位返回 `FORBIDDEN`（拒写保读——Receive/Ack 不受影响）。内部写入（DLQ 转入、位点推进）不拦，量级小且拦了反而破坏消费链路。

**Files:**
- Create: `internal/core/retention/disk_unix.go`、`internal/core/retention/disk_other.go`
- Modify: `internal/core/retention/retention.go`（`New` 终版签名 + 每趟水位检查）
- Modify: `internal/rpc/server.go`（`New` 增参）、`internal/rpc/send.go`（拒写检查）
- Modify: `internal/config/config.go`（`disk_watermark_percent`）
- Modify: `cmd/sq/main.go`、`internal/rpc/server_test.go`（调用点）
- Test: `internal/core/retention/disk_test.go`、`internal/rpc/send_test.go`、`internal/config/config_test.go`

**Interfaces:**
- Produces（终版）: `retention.New(st, mt, interval, dataDir string, watermarkPct int, writeBlocked *atomic.Bool, logger) *Manager`；`rpc.New(cfg, mt, pr, dl, writeBlocked *atomic.Bool, logger) *Server`；`config.Config.DiskWatermarkPercent int`
- 语义: `writeBlocked` 由 retention 每趟更新；`watermarkPct<=0` 或 `writeBlocked==nil` 时禁用；非 unix 平台探测报错→告警一次并视为不阻塞

- [ ] **Step 1: 写失败测试**

`internal/core/retention/disk_test.go` 新建：

```go
// 磁盘用量探测测试（unix 实测 statfs；数值合理性断言）。
package retention

import "testing"

func TestDiskUsedPercentSane(t *testing.T) {
	v, err := diskUsedPercent(t.TempDir())
	if err != nil {
		t.Skipf("平台不支持磁盘探测: %v", err)
	}
	if v < 0 || v > 100 {
		t.Fatalf("磁盘用量超出 [0,100]: %v", v)
	}
}
```

`internal/rpc/send_test.go` 追加：

```go
// TestSendMessageRejectedWhenDiskBlocked 超水位拒写返回 FORBIDDEN（保读不保写）。
func TestSendMessageRejectedWhenDiskBlocked(t *testing.T) {
	env := newTestEnv(t, true) // server_test.go 既有 fixture，本 task 为其增 blocked 字段
	c, blocked := env.client, env.blocked
	blocked.Store(true)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "dw"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			Body:             []byte("x"),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_FORBIDDEN {
		t.Fatalf("期望 FORBIDDEN，得到 %v %v", resp.GetStatus(), err)
	}
	blocked.Store(false)
	sendOne(t, c, "dw", "x") // 恢复后可写
}
```

`internal/rpc/server_test.go`：既有 `testEnv` 结构体增字段 `blocked *atomic.Bool`（import 补 `sync/atomic`），`newTestEnv` 内：

```go
	blocked := &atomic.Bool{}
	srv := New(cfg, mt, pr, dl, blocked, slog.Default())
	// ……bufconn/dial 等其余不变，return 改为：
	return testEnv{srv: srv, client: pb.NewMessagingServiceClient(conn), dl: dl, blocked: blocked}
```

（`newTestClient` 简写与全部既有用例不动。）

`internal/config/config_test.go` 追加：

```go
func TestDiskWatermark(t *testing.T) {
	cfg, err := Load("")
	if err != nil || cfg.DiskWatermarkPercent != 85 {
		t.Fatalf("默认水位: %d %v", cfg.DiskWatermarkPercent, err)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	os.WriteFile(p, []byte("disk_watermark_percent: 120\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("应拒绝 >99 的水位")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/retention ./internal/rpc ./internal/config -v`
Expected: 编译失败（`diskUsedPercent`/新签名未定义）

- [ ] **Step 3: 实现磁盘探测**

`internal/core/retention/disk_unix.go`：

```go
//go:build unix

// 磁盘用量探测（unix：syscall.Statfs，darwin/linux 字段一致）。
package retention

import "syscall"

// diskUsedPercent 返回 dir 所在文件系统的已用空间百分比 [0,100]。
func diskUsedPercent(dir string) (float64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return 0, err
	}
	if fs.Blocks == 0 {
		return 0, nil
	}
	// 用 Bavail（非特权可用块）计算，与 df 口径一致
	free := float64(fs.Bavail) * 100 / float64(fs.Blocks)
	return 100 - free, nil
}
```

`internal/core/retention/disk_other.go`：

```go
//go:build !unix

// 非 unix 平台的磁盘探测降级（M2 边界：水位保护仅支持 unix，其余平台禁用并告警）。
package retention

import "errors"

func diskUsedPercent(dir string) (float64, error) {
	return 0, errors.New("当前平台不支持磁盘水位检查")
}
```

- [ ] **Step 4: retention.Manager 扩展**

`Manager` 增字段 `dataDir string`、`watermarkPct int`、`writeBlocked *atomic.Bool`（import `sync/atomic`）；`New` 终版：

```go
// New 构造清理任务。
//
// 参数：
//   - interval: 扫描间隔（config.RetentionInterval()）
//   - dataDir/watermarkPct/writeBlocked: 磁盘水位保护三件套；
//     watermarkPct<=0 或 writeBlocked 为 nil 时水位检查禁用
func New(st *store.Store, mt *meta.Meta, interval time.Duration, dataDir string,
	watermarkPct int, writeBlocked *atomic.Bool, logger *slog.Logger) *Manager {
	return &Manager{st: st, mt: mt, interval: interval, dataDir: dataDir,
		watermarkPct: watermarkPct, writeBlocked: writeBlocked, logger: logger.With("mod", "retention")}
}
```

`Run` 循环内、`Pass()` 调用前插入 `m.checkDisk()`；新增：

```go
// checkDisk 探测磁盘用量并更新拒写开关。只在状态翻转时打日志（避免每趟刷屏）。
func (m *Manager) checkDisk() {
	if m.watermarkPct <= 0 || m.writeBlocked == nil {
		return
	}
	used, err := diskUsedPercent(m.dataDir)
	if err != nil {
		m.logger.Warn("磁盘水位检查失败，本趟跳过", "dir", m.dataDir, "err", err)
		return
	}
	blocked := used >= float64(m.watermarkPct)
	if blocked != m.writeBlocked.Load() {
		if blocked {
			m.logger.Error("磁盘使用率超过水位线，进入拒写保读", "used_pct", used, "watermark", m.watermarkPct)
		} else {
			m.logger.Info("磁盘使用率回落，恢复写入", "used_pct", used, "watermark", m.watermarkPct)
		}
		m.writeBlocked.Store(blocked)
	}
}
```

Task 8 的 retention_test 调用点改为终版签名：`New(st, mt, time.Minute, t.TempDir(), 0, nil, slog.Default())`（水位禁用，不影响既有用例）。

- [ ] **Step 5: rpc 与 config 与 main**

`internal/rpc/server.go`：`Server` 增字段 `writeBlocked *atomic.Bool`；`New` 增第 5 参 `writeBlocked *atomic.Bool`（import `sync/atomic`）。

`internal/rpc/send.go` 的 `SendMessage` 函数体开头：

```go
	// 磁盘水位拒写保读（spec §7）：只拦生产者写入，消费链路（Receive/Ack）不受影响
	if s.writeBlocked != nil && s.writeBlocked.Load() {
		s.logger.Warn("磁盘水位超限，拒绝写入", "messages", len(req.GetMessages()))
		return &pb.SendMessageResponse{Status: errStatus(pb.Code_FORBIDDEN,
			"磁盘使用率超过水位线，暂时拒写（保读）")}, nil
	}
```

`config.Config` 增 `DiskWatermarkPercent int \`yaml:"disk_watermark_percent"\` // 超过即拒写，0=关闭`；默认 `85`；校验：

```go
	if cfg.DiskWatermarkPercent < 0 || cfg.DiskWatermarkPercent > 99 {
		return nil, fmt.Errorf("配置 disk_watermark_percent 须在 [0,99]（0=关闭），得到 %d", cfg.DiskWatermarkPercent)
	}
```

`cmd/sq/main.go`（import 补 `sync/atomic`）：

```go
	writeBlocked := &atomic.Bool{}
	rm := retention.New(st, mt, cfg.RetentionInterval(), cfg.DataDir, cfg.DiskWatermarkPercent, writeBlocked, logger)
	// ……rpc 装配处（srv 变量必须保留：停机路径的 srv.Shutdown() 依赖它）：
	srv := rpc.New(cfg, mt, pr, dl, writeBlocked, logger)
	srv.Register(gs)
```

同 task 更新 `test/e2e/sdk_test.go` 的 broker 配置构造：补 `DiskWatermarkPercent: 0`（e2e 机器磁盘状况不可控，显式关闭水位以免误拒写；0 在校验范围 [0,99] 内表示关闭）。

- [ ] **Step 6: 跑测试确认通过**

Run: `make test && go build ./...`
Expected: 全部 PASS

- [ ] **Step 7: 核对日志与注释（instrumenting-code）**

- 水位翻转 Error/Info、探测失败 Warn、拒写 Warn——状态变更可观测且不刷屏（上码已含）
- 平台边界（非 unix 禁用）写在 disk_other.go 头注释；「为什么内部写入不拦」写在 Task 说明与 send.go 注释

- [ ] **Step 8: Commit**

```bash
git add internal/core/retention internal/rpc internal/config cmd/sq
git commit -m "feat: 磁盘水位保护——超阈值 SendMessage 拒写保读（spec §7）"
```

---

### Task 10: e2e 扩展（Tag 过滤 / DLQ 全链路）+ README + 终检

M2 出口标准：官方 SDK 完成「按 tag 订阅只收匹配消息」与「不 ack 直到超限 → 从 %DLQ% topic 消费到死信」两条链路。

**Files:**
- Modify: `test/e2e/sdk_test.go`（broker 配置覆盖钩子 + 两个新用例）
- Modify: `README.md`
- Test: `make e2e`

**Interfaces:**
- Consumes: 全部前置 task；官方 SDK `rmq.NewFilterExpression(tag)`（TAG 类型）
- 前置知识: e2e 每用例独立 broker 子进程（`startBroker`），配置由 `config.Config` 结构体 yaml 序列化生成；`%DLQ%` 在 SDK topic 名合法字符集内；基线已把 broker 基建拆为 `writeBrokerConfig`/`launchBroker`/`brokerHandle.stop`，`startBroker` 是对外入口

- [ ] **Step 1: startBroker 增配置覆盖钩子**

`startBroker` 增可选变参 `mutate ...func(*config.Config)`，在配置写盘（yaml.Marshal）之前逐个应用（若配置构造在 `writeBrokerConfig` 内，则把变参透传给它）。既有调用零改动：

```go
func startBroker(t *testing.T, mutate ...func(*config.Config)) string {
	// ……既有 cfg := &config.Config{...} 构造之后、yaml.Marshal 之前：
	for _, f := range mutate {
		f(cfg)
	}
	// ……其余不变
}
```

- [ ] **Step 2: 写 Tag 过滤 e2e**

`test/e2e/sdk_test.go` 追加（沿用本文件既有用例的内联构造风格）：

```go
// TestOfficialGoSDKTagFilter 官方 SDK 按 tag 订阅：只收到匹配消息，不匹配的被
// 服务端跳过且对本消费组永久越过（M2 出口标准之一）。
//
// 断言分两层：订 tagA 的消费者恰好收齐全部 4 条 A 且无一条 B；随后同组换
// SUB_ALL 再收，必须颗粒无收——证明 B 是被位点永久跳过，不是暂时不可见。
func TestOfficialGoSDKTagFilter(t *testing.T) {
	endpoint := startBroker(t)
	const (
		topic = "e2e-tag"
		group = "e2e-tag-g"
	)
	producer, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := producer.Start(); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	defer producer.GracefulStop()

	for i := 0; i < 4; i++ {
		for _, tag := range []string{"tagA", "tagB"} {
			msg := &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("%s-%d", tag, i))}
			msg.SetTag(tag)
			if _, err := producer.Send(context.Background(), msg); err != nil {
				t.Fatalf("Send %s#%d: %v", tag, i, err)
			}
		}
	}

	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			topic: rmq.NewFilterExpression("tagA"),
		}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	defer consumer.GracefulStop()

	got := 0
	deadline := time.Now().Add(60 * time.Second)
	for got < 4 && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			if tag := mv.GetTag(); tag == nil || *tag != "tagA" {
				t.Fatalf("收到非 tagA 消息: tag=%v body=%s", mv.GetTag(), mv.GetBody())
			}
			got++
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
	}
	if got != 4 {
		t.Fatalf("tagA 消息应恰好 4 条，实际 %d", got)
	}

	// 同组换 SUB_ALL 再收：tagB 已被位点永久跳过，必须一无所获
	allConsumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(2*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			topic: rmq.SUB_ALL,
		}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer(SUB_ALL): %v", err)
	}
	if err := allConsumer.Start(); err != nil {
		t.Fatalf("allConsumer.Start: %v", err)
	}
	defer allConsumer.GracefulStop()
	for i := 0; i < 4; i++ {
		mvs, err := allConsumer.Receive(context.Background(), 16, 30*time.Second)
		if err == nil && len(mvs) > 0 {
			t.Fatalf("被过滤消息泄漏: %d 条（首条 body=%s）", len(mvs), mvs[0].GetBody())
		}
	}
}
```

（import 若缺则补 `fmt`；`NewFilterExpression`/`SetTag`/`GetTag` 与 SDK v5.1.4 实际 API 不符时以 `$GOMODCACHE/github.com/apache/rocketmq-clients/golang/v5@v5.1.4/` 源码为准——预期内修正。）

- [ ] **Step 3: 写 DLQ e2e**

```go
// TestOfficialGoSDKDLQ 不 ack 直到投递超限（本用例 broker 配 default_max_attempts=2），
// 死信作为普通 topic 从 %DLQ%{group} 被 SDK 消费到，且带 sq-origin-* 溯源属性
//（M2 出口标准之一）。
//
// 时序说明：第 2 次投递的不可见窗口 = max(客户端 3s, 服务端退避下限 10s) = 10s；
// 转入是惰性的（原队列下一次 Receive 触发），所以窗口过期后要继续戳原 topic。
// DLQ topic 可能因 dlqConsumer 先 QueryRoute 而按默认 4 队列自动建出，
// moveToDLQ 的 CreateTopic(1) 幂等返回既有配置——属预期，不影响断言。
func TestOfficialGoSDKDLQ(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) {
		c.DefaultMaxAttempts = 2 // 2 次投递即超限，控制用例时长
	})
	const (
		topic = "e2e-dlq"
		group = "e2e-dlq-g"
		body  = "dlq-poison"
	)
	producer, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := producer.Start(); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	defer producer.GracefulStop()
	if _, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(body)}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			topic: rmq.SUB_ALL,
		}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	defer consumer.GracefulStop()

	// 第 1、2 次投递均收到但不 ack（invisible 3s，任其过期）
	seen := 0
	deadline := time.Now().Add(90 * time.Second)
	for seen < 2 && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 3*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) == body {
				seen++
				t.Logf("第 %d 次收到毒消息", seen)
			}
		}
	}
	if seen < 2 {
		t.Fatalf("未完成 2 次投递: %d", seen)
	}

	// DLQ 消费者：%DLQ%{group} 是普通 topic，SDK 直接订阅
	dlqTopic := "%DLQ%" + group
	dlqConsumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group + "-reader",
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			dlqTopic: rmq.SUB_ALL,
		}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer(DLQ): %v", err)
	}
	if err := dlqConsumer.Start(); err != nil {
		t.Fatalf("dlqConsumer.Start: %v", err)
	}
	defer dlqConsumer.GracefulStop()

	// 循环：戳原 topic（等待退避窗口过期 + 触发惰性转入）→ 查 DLQ
	var gotBody string
	deadline = time.Now().Add(120 * time.Second)
	for gotBody == "" && time.Now().Before(deadline) {
		_, _ = consumer.Receive(context.Background(), 16, 3*time.Second) // 空轮询错误可忽略
		mvs, err := dlqConsumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			gotBody = string(mv.GetBody())
			props := mv.GetProperties()
			if props["sq-origin-topic"] != topic {
				t.Fatalf("死信缺少来源属性 sq-origin-topic: %v", props)
			}
			if err := dlqConsumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack 死信: %v", err)
			}
		}
	}
	if gotBody != body {
		t.Fatalf("死信未到达或内容不符: %q", gotBody)
	}

	// 原 topic 不应再投出毒消息（inflight 已随转入原子删除）
	for i := 0; i < 2; i++ {
		mvs, err := consumer.Receive(context.Background(), 16, 3*time.Second)
		if err != nil {
			continue
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) == body {
				t.Fatal("超限消息不应再从原 topic 投出")
			}
		}
	}
}
```

（`GetProperties`/`GetBody` 与 SDK MessageView 实际方法名不符时以 SDK 源码为准——预期内修正。）

- [ ] **Step 4: 跑 e2e**

Run: `make e2e`
Expected: 既有 4 用例（SendAndReceive / EmptyPoll / AckTimeoutRedelivery / RestartRecovery）+ 新 2 用例全部 PASS。DLQ 用例受服务端 10s 退避下限影响，预计 30–50s，慢于其他用例属预期。排查顺序（沿用 M1 经验）：先看 broker 日志（测试失败时自动打印），再看 SDK console 日志。

- [ ] **Step 5: README 更新**

`README.md` 追加/更新以下内容（保持既有结构，勿重写全文）：
- 功能列表：服务端 Tag 过滤（`"*"`/单 tag/`a || b`）、重试指数退避（10s 起 ×2 封顶 5min）与超限转 `%DLQ%{group}`、Keys 业务索引、消息 retention（默认 3 天）、磁盘水位拒写保读（默认 85%）
- 配置表新增四行：

```markdown
| default_max_attempts | 16 | 新订阅组默认最大投递次数，超过转入 %DLQ%{group} |
| retention_check_interval | 5m | 过期清理扫描间隔（Go duration 格式） |
| disk_watermark_percent | 85 | 磁盘使用率超过即拒写保读；0=关闭 |
```

（`default_queue_nums`/`log_level` 行若已存在则不动；表格列对齐随现有 README。）
- 「消费失败链路」小节：一段话说明 重试→退避→DLQ 的行为与 `sq-origin-*` 溯源属性

- [ ] **Step 6: M2 终检（instrumenting-code + CLAUDE.md 清单）**

逐项核对，任一不过先修复：
- [ ] `grep -rn "fmt.Print\|log.Print" internal/ cmd/ --include='*.go' | grep -v _test` 无输出
- [ ] 新文件（filter.go、query.go、retention.go、disk_unix.go、disk_other.go）均有职责/边界头注释
- [ ] 新导出符号均有 doc comment（`AppendWith`/`ByKey`/`ParseTagFilter`/`Match`/`Topics`/`DLQTopicName`/`EffectiveMaxAttempts`/`EffectiveRetention`/`retention.New`/`Run`/`Pass`/`RetentionInterval` 等）
- [ ] 错误分支全部带 group/topic/queue/offset/msgId 上下文；DLQ 转入、水位翻转、retention 完成等状态变更有 Info/Error 日志；高频路径（过滤跳过、队列明细）降级 Debug
- [ ] 无跨层调用（rpc 不碰 store key；retention 不碰 deliver）；跨包只经导出接口
- [ ] `make test`、`go vet ./...`、`make e2e` 三绿；`go test -race ./internal/...` 通过
- [ ] spec §5 流程 2（Tag）/流程 6（重试 DLQ）/流程 7（retention）、§7 水位、§4 keyidx 编码逐条能指到实现

- [ ] **Step 7: Commit**

```bash
git add test/e2e README.md
git commit -m "test(e2e): Tag 过滤与 DLQ 全链路；docs: README M2 功能与配置"
```

---

## Self-Review（计划自检记录）

**1. Spec 覆盖核对（M2 = 重试/DLQ、Tag 过滤、Keys 索引、retention，出口「消费失败链路完整」）：**
- §5 流程 6 重试/DLQ：Task 6（退避 + `%DLQ%{group}` 1 队列 + 原子转入）+ Task 3（maxAttempts）✓；「非顺序消息指数退避」✓；顺序消息的 DLQ（ForwardMessageToDeadLetterQueue RPC）按 spec 属 M4，不在本计划 ✓
- §5 流程 2 Tag 过滤：Task 5（跳过并推进位点、不占 inflight）+ Task 7（协议接入）✓；SQL92 明确拒绝（v1.1）✓
- §4/§5 流程 1 Keys 索引：Task 2（编码）+ Task 4（同批写入 + ByKey 查询）✓；QueryMessage 协议 RPC 不在 v1 11-RPC 列表，Admin API 属 M5，M2 只交付 core 查询能力 ✓
- §5 流程 7 retention：Task 8（默认 3 天、DeleteRange、keyidx 同清）✓；「可选按最大字节数丢弃」spec 标注可选，YAGNI 留待需要 ✓
- §7 磁盘水位：Task 9（85% 拒写保读 FORBIDDEN）✓——spec 未指定里程碑，与 retention 同池落地并在 task 头说明理由
- §8 可观测性：各 task 的 instrumenting 步骤覆盖「DLQ 转入、调度扫描」等关键节点日志 ✓（Prometheus /metrics 属 M5，不在本计划）

**2. 占位符扫描：** 全文无 TBD/TODO/「适当处理」；Task 4 的「与 M1 一致」段落均明确标注是**改名保留原代码**而非待补内容，且指明了插入的新代码段。SDK API 名（`NewFilterExpression`/`SetTag`）与 pb 字段名按既定对齐规则处理，已在 task 内写明排查路径。

**3. 类型/签名一致性：**
- `meta.New(st, autoCreate, defaultQueues uint32, defaultMaxAttempts int32, logger)`：Task 3 定义，Task 3 内更新全部 6 处调用点（main、produce/deliver/rpc 三个测试 fixture、meta_test 包内调用、e2e broker 配置补字段），Task 4/6/8 的新 fixture 均按此签名 ✓
- `deliver.Receive(..., filter *TagFilter)` 8 参：Task 5 定义并列出全部调用点补参；Task 6/8 的用例、Task 7 的 rpc 接入均为 8 参 ✓
- `receiveOnce(group, topic, queueID, maxMsgs, invisible, maxAttempts, filter)`：Task 5 先加 filter、Task 6 再加 maxAttempts，Task 6 展示的是含两者的终版阶段 1 ✓
- `produce.AppendWith(m, extra func(b *pebble.Batch))`：Task 4 定义，Task 6 `moveToDLQ` 使用处签名一致 ✓
- `retention.New` 两版签名：Task 8 初版 4 参、Task 9 终版 7 参，Task 9 明确列出回改 Task 8 测试调用点 ✓
- `rpc.New` 6 参：Task 9 定义并同 task 更新 main 与 server_test fixture ✓
- config 新增 3 字段与「任务间接口总表」一致（Task 3 `default_max_attempts`、Task 8 `retention_check_interval`、Task 9 `disk_watermark_percent`；Task 1 只补 log_level 校验）；三个加字段的 task 均同步更新 e2e `startBroker` 的配置构造（否则 yaml 零值被校验拒绝、`make e2e` 才暴露）✓

**已知风险与对齐规则（预期内修正，不算设计变更）：**
- pb `RetryPolicy`/`ExponentialBackoff`/`FilterType` 等字段名以生成代码为准
- SDK `NewFilterExpression`/`SetTag` 构造名以 v5.1.4 源码为准
- `syscall.Statfs_t` 字段（`Bavail`/`Blocks`）darwin/linux 均可用；其他 unix 变体若类型不符（如 32 位平台），以编译结果为准做显式转换
- e2e DLQ 用例时长受服务端退避下限（10s）影响约 30s，慢于其他用例属预期；超 120s deadline 才算失败



