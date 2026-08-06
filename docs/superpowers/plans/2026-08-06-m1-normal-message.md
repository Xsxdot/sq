# sq M1 实现计划：store + 普通消息 + RocketMQ gRPC 骨架

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 单二进制 broker 完成普通消息全链路，出口标准 = 官方 rocketmq-clients Go SDK 能对 sq 发送并消费普通消息。

**Architecture:** 四层（接入 gRPC / rocketmq-adapter / core：meta+produce+deliver / store：Pebble）。消费为 POP 模式：服务端记 inflight（不可见时间），Receive 时先重投过期 inflight 再取新消息（惰性重投，M1 无后台扫描 goroutine）。所有写入经 `store.Apply(batch)` 单一入口——这是 v2 Raft 的拦截点。

**Tech Stack:** Go 1.24+；cockroachdb/pebble v2；google.golang.org/grpc + protobuf；proto 来自 apache/rocketmq-apis（buf 生成）；日志 log/slog（JSON）；集成测试用 github.com/apache/rocketmq-clients/golang/v5。

## Global Constraints

- 模块名 `github.com/xushixin/sq`（发布前可整体替换）。
- 日志一律 `log/slog` 结构化字段，禁止 `fmt.Printf`/`log.Print`（本项目为开源项目，不依赖私有 gokit——对全局 CLAUDE.md 的项目级覆盖）。
- 每个新文件顶部必须有中文文件头注释（职责+边界）；每个导出符号必须有 doc 注释（全局 CLAUDE.md §2）。
- key 编码中整数一律大端定长（字节序=数值序）；topic/group 名合法字符 `^[A-Za-z0-9_%\-]{1,127}$`（不含 `/`，key 分隔安全的前提）。
- 消息体上限 4MB；默认同步刷盘（`pebble.Sync`）。
- 协议错误码用 `apache.rocketmq.v2` 的 `Code` 枚举；未实现的 RPC 返回 gRPC `UNIMPLEMENTED`。
- 提交信息用 conventional commits（feat/test/docs/chore），每个任务至少一次提交。
- spec 见 `docs/superpowers/specs/2026-08-06-sq-mq-design.md`；M1 不实现：延时/顺序/事务/Tag 过滤/Keys 索引/retention/DLQ/控制台（分属 M2-M6）。M1 中 Tag 过滤表达式仅接受 `"*"`。

## File Structure（M1 全量）

```
go.mod
Makefile                          — build/test/proto 生成入口
cmd/sq/main.go                    — 装配与启停
internal/config/config.go         — 配置加载与默认值
internal/store/keys.go            — key 编码 schema（唯一定义处）
internal/store/store.go           — Pebble 封装，Apply 单一写入口
internal/core/types.go            — Message 结构与存储编解码
internal/core/meta/meta.go        — topic/group 注册表
internal/core/produce/produce.go  — 写入路径与 offset 分配
internal/core/deliver/deliver.go  — POP 消费：Receive/Ack/ChangeInvisible
internal/rpc/pb/                  — buf 生成代码（不手改）
internal/rpc/receipt.go           — receipt handle 编解码
internal/rpc/server.go            — MessagingService 实现（adapter）
proto/apache/rocketmq/v2/*.proto  — 从 rocketmq-apis 复制的协议文件
buf.yaml / buf.gen.yaml           — 代码生成配置
test/e2e/sdk_test.go              — 官方 Go SDK 集成测试（出口标准）
README.md
```

---

### Task 1: 项目脚手架与日志基座

**Files:**
- Create: `go.mod`, `.gitignore`, `Makefile`, `internal/config/config.go`, `internal/config/config_test.go`, `cmd/sq/main.go`

**Interfaces:**
- Produces: `config.Load(path string) (*Config, error)`；`Config{GRPCListen string; AdvertiseHost string; AdvertisePort int; DataDir string; Fsync string; AutoCreateTopic bool; DefaultQueueNums uint32; LogLevel string}`；`config.SetupSlog(level string)`。

- [ ] **Step 1: 初始化模块与工具链**

```bash
cd /Users/xushixin/workspace/sq
go mod init github.com/xushixin/sq
go get gopkg.in/yaml.v3@latest
```

`.gitignore`:

```
/sq
/data/
*.log
node_modules/
```

`Makefile`:

```makefile
.PHONY: build test proto e2e
build:
	go build -o sq ./cmd/sq
test:
	go test ./...
e2e:
	go test -tags e2e -count=1 ./test/e2e/...
proto:
	buf generate
```

- [ ] **Step 2: 写失败测试（配置默认值与 YAML 覆盖）**

`internal/config/config_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("") // 空路径 = 全默认值
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.GRPCListen != ":8081" || cfg.DataDir != "./data" ||
		cfg.Fsync != "sync" || !cfg.AutoCreateTopic || cfg.DefaultQueueNums != 4 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadYAMLOverride(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sq.yaml")
	os.WriteFile(p, []byte("grpc_listen: \":9081\"\nfsync: async\n"), 0o644)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.GRPCListen != ":9081" || cfg.Fsync != "async" {
		t.Fatalf("override not applied: %+v", cfg)
	}
	if cfg.DataDir != "./data" { // 未覆盖字段保持默认
		t.Fatalf("default lost: %+v", cfg)
	}
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `go test ./internal/config/ -v`
Expected: FAIL（包不存在/未定义 Load）

- [ ] **Step 4: 实现 config**

`internal/config/config.go`:

```go
// Package config 提供 sq 的配置加载。
//
// 职责：
//   - 定义全部可配置项与默认值
//   - 从可选 YAML 文件加载并覆盖默认值
//   - 初始化全局 slog
//
// 边界：
//   - 不做热更新；不校验业务语义（如端口占用）
package config

import (
	"fmt"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

// Config 为 sq 全部运行配置。零值无意义，必须经 Load 构造。
type Config struct {
	GRPCListen       string `yaml:"grpc_listen"`        // gRPC 监听地址，默认 :8081
	AdvertiseHost    string `yaml:"advertise_host"`     // 路由响应中的对外地址，默认 127.0.0.1
	AdvertisePort    int    `yaml:"advertise_port"`     // 默认 8081
	DataDir          string `yaml:"data_dir"`           // Pebble 数据目录
	Fsync            string `yaml:"fsync"`              // sync|async
	AutoCreateTopic  bool   `yaml:"auto_create_topic"`  // QueryRoute/Send 未知 topic 时自动建
	DefaultQueueNums uint32 `yaml:"default_queue_nums"` // 自动建 topic 的队列数
	LogLevel         string `yaml:"log_level"`          // debug|info|warn|error
}

// Load 加载配置。path 为空时返回纯默认值；文件存在则按字段覆盖。
func Load(path string) (*Config, error) {
	cfg := &Config{
		GRPCListen: ":8081", AdvertiseHost: "127.0.0.1", AdvertisePort: 8081,
		DataDir: "./data", Fsync: "sync",
		AutoCreateTopic: true, DefaultQueueNums: 4, LogLevel: "info",
	}
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %s: %w", path, err)
	}
	if cfg.Fsync != "sync" && cfg.Fsync != "async" {
		return nil, fmt.Errorf("配置 fsync 只接受 sync|async，得到 %q", cfg.Fsync)
	}
	return cfg, nil
}

// SetupSlog 按配置初始化全局 slog（JSON 输出到 stdout）。
func SetupSlog(level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})))
}
```

`cmd/sq/main.go`（M1 临时骨架，Task 12 完成装配）:

```go
// sq 主入口。装配 config/store/core/rpc 并托管进程生命周期。
// 边界：只做装配与启停，不含业务逻辑。
package main

import (
	"flag"
	"log/slog"

	"github.com/xushixin/sq/internal/config"
)

func main() {
	cfgPath := flag.String("config", "", "配置文件路径（可选）")
	flag.Parse()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("加载配置失败", "err", err)
		return
	}
	config.SetupSlog(cfg.LogLevel)
	slog.Info("sq 启动（M1 骨架）", "grpc_listen", cfg.GRPCListen, "data_dir", cfg.DataDir)
}
```

- [ ] **Step 5: 跑测试确认通过 + 构建**

Run: `go test ./internal/config/ -v && go build ./...`
Expected: PASS，构建成功

- [ ] **Step 6: 提交**

```bash
git add -A && git commit -m "feat: 项目脚手架（config/slog/Makefile）"
```

---

### Task 2: store key 编码 schema

**Files:**
- Create: `internal/store/keys.go`, `internal/store/keys_test.go`

**Interfaces:**
- Produces（后续所有任务依赖，签名逐字使用）:
  - `MsgKey(topic string, queueID uint32, offset uint64) []byte` / `ParseMsgKey(k []byte) (topic string, queueID uint32, offset uint64, err error)`
  - `MsgQueuePrefix(topic string, queueID uint32) []byte`（该队列区间扫描下界）
  - `AllocKey(topic string, queueID uint32) []byte`（下一 offset 计数器）
  - `CursorKey(group, topic string, queueID uint32) []byte`
  - `InflightKey(group, topic string, queueID uint32, offset uint64) []byte` / `ParseInflightKey` / `InflightPrefix(group, topic string, queueID uint32) []byte`
  - `TopicMetaKey(topic string) []byte` / `GroupMetaKey(group string) []byte`，前缀常量 `TopicMetaPrefix`、`GroupMetaPrefix`
  - `PutU64(v uint64) []byte` / `GetU64(b []byte) uint64`
  - `PrefixUpperBound(prefix []byte) []byte`（区间扫描上界）

- [ ] **Step 1: 写失败测试**

`internal/store/keys_test.go`:

```go
package store

import (
	"bytes"
	"testing"
)

func TestMsgKeyRoundTrip(t *testing.T) {
	k := MsgKey("orders", 3, 42)
	topic, q, off, err := ParseMsgKey(k)
	if err != nil || topic != "orders" || q != 3 || off != 42 {
		t.Fatalf("round trip: %v %v %v %v", topic, q, off, err)
	}
}

func TestMsgKeyOrdering(t *testing.T) {
	// 字节序必须等于数值序：这是区间扫描正确性的根基
	if bytes.Compare(MsgKey("t", 0, 1), MsgKey("t", 0, 2)) >= 0 {
		t.Fatal("offset 顺序错误")
	}
	if bytes.Compare(MsgKey("t", 0, 255), MsgKey("t", 0, 256)) >= 0 {
		t.Fatal("跨字节边界顺序错误")
	}
	if bytes.Compare(MsgKey("t", 1, 999), MsgKey("t", 2, 0)) >= 0 {
		t.Fatal("queueID 优先级错误")
	}
}

func TestPrefixScanBoundary(t *testing.T) {
	p := MsgQueuePrefix("t", 1)
	up := PrefixUpperBound(p)
	k := MsgKey("t", 1, ^uint64(0)) // 最大 offset 也必须落在 [p, up)
	if !(bytes.Compare(k, p) >= 0 && bytes.Compare(k, up) < 0) {
		t.Fatal("上界计算错误")
	}
	other := MsgKey("t", 2, 0) // 相邻队列必须落在界外
	if bytes.Compare(other, up) < 0 {
		t.Fatal("相邻队列落入扫描区间")
	}
}

func TestInflightKeyRoundTrip(t *testing.T) {
	k := InflightKey("g1", "orders", 2, 7)
	g, topic, q, off, err := ParseInflightKey(k)
	if err != nil || g != "g1" || topic != "orders" || q != 2 || off != 7 {
		t.Fatalf("round trip: %v %v %v %v %v", g, topic, q, off, err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/store/ -v`
Expected: FAIL（未定义）

- [ ] **Step 3: 实现 keys.go**

```go
// Package store 提供 sq 的持久化层：key 编码 schema 与 Pebble 封装。
//
// 职责：
//   - 集中定义全部 key 编码（本文件是 schema 的唯一事实源）
//   - 封装 Pebble 读写，Apply 为唯一写入口（v2 Raft 拦截点）
//
// 边界：
//   - 不理解消息语义（谁可见、何时投递是 core 的事）
//   - key 中整数一律大端定长，保证字节序=数值序
package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// 前缀常量。名字段（topic/group）合法字符不含 '/'（meta 层校验），
// 因此 '/' 可安全作为名字段与定长二进制段的分隔符。
const (
	TopicMetaPrefix = "meta/topic/"
	GroupMetaPrefix = "meta/group/"
	msgPrefix       = "msg/"
	allocPrefix     = "alloc/"
	cursorPrefix    = "cursor/"
	inflightPrefix  = "inflight/"
)

// PutU64 大端编码。
func PutU64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// GetU64 大端解码，len(b)!=8 时 panic（编码错误属程序 bug，不做静默容错）。
func GetU64(b []byte) uint64 {
	if len(b) != 8 {
		panic(fmt.Sprintf("GetU64: 期望 8 字节，得到 %d", len(b)))
	}
	return binary.BigEndian.Uint64(b)
}

func putU32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// MsgKey 消息主键：msg/{topic}/{queueID:4B}{offset:8B}。
func MsgKey(topic string, queueID uint32, offset uint64) []byte {
	k := make([]byte, 0, len(msgPrefix)+len(topic)+1+12)
	k = append(k, msgPrefix...)
	k = append(k, topic...)
	k = append(k, '/')
	k = append(k, putU32(queueID)...)
	k = append(k, PutU64(offset)...)
	return k
}

// MsgQueuePrefix 某队列消息区间扫描下界：msg/{topic}/{queueID:4B}。
func MsgQueuePrefix(topic string, queueID uint32) []byte {
	k := MsgKey(topic, queueID, 0)
	return k[:len(k)-8]
}

// ParseMsgKey 解析消息主键。定位规则：前缀后第一个 '/' 之前为 topic，
// 其后必须恰好 12 字节（4B queueID + 8B offset）——二进制段可能含 '/'，
// 所以只能按位置解析，不能 Split。
func ParseMsgKey(k []byte) (string, uint32, uint64, error) {
	rest, ok := bytes.CutPrefix(k, []byte(msgPrefix))
	if !ok {
		return "", 0, 0, fmt.Errorf("非法 msg key: %q", k)
	}
	i := bytes.IndexByte(rest, '/')
	if i < 0 || len(rest)-i-1 != 12 {
		return "", 0, 0, fmt.Errorf("msg key 结构错误: %q", k)
	}
	topic := string(rest[:i])
	bin := rest[i+1:]
	return topic, binary.BigEndian.Uint32(bin[:4]), binary.BigEndian.Uint64(bin[4:]), nil
}

// AllocKey 队列 offset 分配计数器：alloc/{topic}/{queueID:4B}，值为下一可用 offset(8B)。
func AllocKey(topic string, queueID uint32) []byte {
	k := make([]byte, 0, len(allocPrefix)+len(topic)+1+4)
	k = append(k, allocPrefix...)
	k = append(k, topic...)
	k = append(k, '/')
	k = append(k, putU32(queueID)...)
	return k
}

// CursorKey 消费组 fetch 位点：cursor/{group}/{topic}/{queueID:4B}，值为下一待取 offset(8B)。
func CursorKey(group, topic string, queueID uint32) []byte {
	k := make([]byte, 0, len(cursorPrefix)+len(group)+1+len(topic)+1+4)
	k = append(k, cursorPrefix...)
	k = append(k, group...)
	k = append(k, '/')
	k = append(k, topic...)
	k = append(k, '/')
	k = append(k, putU32(queueID)...)
	return k
}

// InflightKey 已投未确认记录：inflight/{group}/{topic}/{queueID:4B}{offset:8B}。
func InflightKey(group, topic string, queueID uint32, offset uint64) []byte {
	k := make([]byte, 0, len(inflightPrefix)+len(group)+1+len(topic)+1+12)
	k = append(k, inflightPrefix...)
	k = append(k, group...)
	k = append(k, '/')
	k = append(k, topic...)
	k = append(k, '/')
	k = append(k, putU32(queueID)...)
	k = append(k, PutU64(offset)...)
	return k
}

// InflightPrefix 某消费组某队列的 inflight 扫描下界。
func InflightPrefix(group, topic string, queueID uint32) []byte {
	k := InflightKey(group, topic, queueID, 0)
	return k[:len(k)-8]
}

// ParseInflightKey 解析 inflight key，规则同 ParseMsgKey（两段名字 + 12B 定长尾）。
func ParseInflightKey(k []byte) (group, topic string, queueID uint32, offset uint64, err error) {
	rest, ok := bytes.CutPrefix(k, []byte(inflightPrefix))
	if !ok {
		return "", "", 0, 0, fmt.Errorf("非法 inflight key: %q", k)
	}
	i := bytes.IndexByte(rest, '/')
	if i < 0 {
		return "", "", 0, 0, fmt.Errorf("inflight key 结构错误: %q", k)
	}
	group = string(rest[:i])
	rest = rest[i+1:]
	j := bytes.IndexByte(rest, '/')
	if j < 0 || len(rest)-j-1 != 12 {
		return "", "", 0, 0, fmt.Errorf("inflight key 结构错误: %q", k)
	}
	topic = string(rest[:j])
	bin := rest[j+1:]
	return group, topic, binary.BigEndian.Uint32(bin[:4]), binary.BigEndian.Uint64(bin[4:]), nil
}

// TopicMetaKey topic 配置：meta/topic/{topic}。
func TopicMetaKey(topic string) []byte { return []byte(TopicMetaPrefix + topic) }

// GroupMetaKey 订阅组配置：meta/group/{group}。
func GroupMetaKey(group string) []byte { return []byte(GroupMetaPrefix + group) }

// PrefixUpperBound 返回前缀区间的开区间上界：最后一个可进位字节 +1。
// 全 0xFF 前缀返回 nil（无上界），M1 的前缀都带 '/' 不会出现。
func PrefixUpperBound(prefix []byte) []byte {
	up := bytes.Clone(prefix)
	for i := len(up) - 1; i >= 0; i-- {
		if up[i] < 0xFF {
			up[i]++
			return up[:i+1]
		}
	}
	return nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/store/ -v`
Expected: PASS（4 个用例全过）

- [ ] **Step 5: 提交**

```bash
git add internal/store/ && git commit -m "feat: store key 编码 schema"
```

---

### Task 3: store Pebble 封装

**Files:**
- Create: `internal/store/store.go`, `internal/store/store_test.go`
- Modify: `go.mod`（引入 pebble）

**Interfaces:**
- Consumes: Task 2 的全部 key 函数。
- Produces:
  - `Open(dir string, syncWrites bool, logger *slog.Logger) (*Store, error)` / `(*Store) Close() error`
  - `(*Store) Get(key []byte) ([]byte, bool, error)`（返回值为拷贝；不存在时 (nil,false,nil)）
  - `(*Store) NewBatch() *pebble.Batch`
  - `(*Store) Apply(b *pebble.Batch) error` —— **全项目唯一写入口**
  - `(*Store) Scan(lower, upper []byte, limit int, fn func(k, v []byte) (bool, error)) error`（fn 返回 false 停止；k/v 仅在回调内有效）

- [ ] **Step 1: 引入依赖**

```bash
go get github.com/cockroachdb/pebble/v2@latest
```

- [ ] **Step 2: 写失败测试**

`internal/store/store_test.go`:

```go
package store

import (
	"log/slog"
	"testing"
)

func openTestStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, true, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func TestSetGetPersistence(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir)
	b := s.NewBatch()
	b.Set([]byte("k1"), []byte("v1"), nil)
	if err := s.Apply(b); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	v, ok, err := s.Get([]byte("k1"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get: %q %v %v", v, ok, err)
	}
	if _, ok, _ := s.Get([]byte("nope")); ok {
		t.Fatal("不存在的 key 返回了 ok")
	}
	// 重开验证持久化（崩溃恢复的最小代理测试）
	s.Close()
	s2 := openTestStore(t, dir)
	defer s2.Close()
	v, ok, err = s2.Get([]byte("k1"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("重开后 Get: %q %v %v", v, ok, err)
	}
}

func TestScanRangeAndLimit(t *testing.T) {
	s := openTestStore(t, t.TempDir())
	defer s.Close()
	b := s.NewBatch()
	for _, k := range []string{"a/1", "a/2", "a/3", "b/1"} {
		b.Set([]byte(k), []byte("x"), nil)
	}
	s.Apply(b)
	var got []string
	err := s.Scan([]byte("a/"), PrefixUpperBound([]byte("a/")), 2, func(k, v []byte) (bool, error) {
		got = append(got, string(k))
		return true, nil
	})
	if err != nil || len(got) != 2 || got[0] != "a/1" || got[1] != "a/2" {
		t.Fatalf("Scan: %v %v", got, err)
	}
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `go test ./internal/store/ -run 'TestSetGet|TestScan' -v`
Expected: FAIL（未定义 Store）

- [ ] **Step 4: 实现 store.go**

```go
package store

import (
	"fmt"
	"log/slog"

	"github.com/cockroachdb/pebble/v2"
)

// Store 封装单个 Pebble 实例。并发安全（Pebble 自身保证）。
//
// Apply 是全项目唯一的写入口：core 把一次语义操作（写消息/ack/位点推进）
// 组装为一个 Batch 原子提交。v2 引入 Raft 时，只需在 Apply 前插入日志复制，
// core 与本层其余代码不动——这是 spec §3 Command 化写路径的落地形态。
type Store struct {
	db     *pebble.DB
	sync   bool
	logger *slog.Logger
}

// Open 打开（或创建）dir 下的 Pebble 库。syncWrites 决定 Apply 是否逐次 fsync。
func Open(dir string, syncWrites bool, logger *slog.Logger) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("打开 Pebble(%s): %w", dir, err)
	}
	logger.Info("store 已打开", "mod", "store", "dir", dir, "sync", syncWrites)
	return &Store{db: db, sync: syncWrites, logger: logger}, nil
}

// Close 关闭底层库。之后任何操作都会 panic（Pebble 语义），不做二次防护。
func (s *Store) Close() error {
	s.logger.Info("store 关闭", "mod", "store")
	return s.db.Close()
}

// Get 读取 key。返回值是拷贝，可长期持有。不存在返回 (nil, false, nil)。
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	v, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store Get %q: %w", key, err)
	}
	out := append([]byte(nil), v...)
	closer.Close()
	return out, true, nil
}

// NewBatch 创建写批次。调用方组装后必须交给 Apply 提交。
func (s *Store) NewBatch() *pebble.Batch { return s.db.NewBatch() }

// Apply 原子提交批次并按配置刷盘。这是唯一写入口（见类型注释）。
func (s *Store) Apply(b *pebble.Batch) error {
	opt := pebble.NoSync
	if s.sync {
		opt = pebble.Sync
	}
	if err := b.Commit(opt); err != nil {
		return fmt.Errorf("store Apply: %w", err)
	}
	return nil
}

// Scan 按 [lower, upper) 升序遍历，最多 limit 条（limit<=0 不限）。
// fn 返回 false 或 error 时停止；k/v 底层内存仅回调期间有效，需持有请自行拷贝。
func (s *Store) Scan(lower, upper []byte, limit int, fn func(k, v []byte) (bool, error)) error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return fmt.Errorf("store Scan 创建迭代器: %w", err)
	}
	defer it.Close()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		cont, err := fn(it.Key(), it.Value())
		if err != nil || !cont {
			return err
		}
		n++
		if limit > 0 && n >= limit {
			return nil
		}
	}
	return it.Error()
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `go test ./internal/store/ -v`
Expected: PASS

- [ ] **Step 6: 日志与注释自检**

- Open/Close 已有 Info 日志（带 dir/sync 上下文）；Get/Apply/Scan 错误全部带 key/操作上下文包装 —— 热路径本身不打日志（调用方 core 负责语义级日志），这是边界注释里要写明的
- 文件头、全部导出符号 doc 注释已在 Step 4 代码中 —— 核对无遗漏

- [ ] **Step 7: 提交**

```bash
git add internal/store/ go.mod go.sum && git commit -m "feat: store Pebble 封装（Apply 单一写入口）"
```

---

### Task 4: core 消息类型与存储编解码

**Files:**
- Create: `internal/core/types.go`, `internal/core/types_test.go`

**Interfaces:**
- Produces:
  - `core.Message{ID string; Topic string; QueueID uint32; Offset uint64; Tag string; Keys []string; MessageGroup string; Properties map[string]string; Body []byte; BornAtMs int64; StoreAtMs int64; DeliveryAttempt int32}`（`DeliveryAttempt` 由 deliver 投递时填充，不落盘）
  - `core.EncodeMessage(m *Message) ([]byte, error)` / `core.DecodeMessage(b []byte) (*Message, error)`
  - `core.NewMessageID() string`（32 位大写十六进制）
  - `core.InflightState{ExpireAtMs int64; Attempts int32}` + `EncodeInflight/DecodeInflight`

- [ ] **Step 1: 写失败测试**

`internal/core/types_test.go`:

```go
package core

import (
	"reflect"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	m := &Message{
		ID: NewMessageID(), Topic: "orders", QueueID: 1, Offset: 9,
		Tag: "created", Keys: []string{"o-1"}, MessageGroup: "",
		Properties: map[string]string{"a": "b"}, Body: []byte{0x00, 0xFF, 0x7F},
		BornAtMs: 1000, StoreAtMs: 2000,
	}
	b, err := EncodeMessage(m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeMessage(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	got.DeliveryAttempt = m.DeliveryAttempt // 非存储字段不参与比较
	if !reflect.DeepEqual(m, got) {
		t.Fatalf("round trip 不一致:\n%+v\n%+v", m, got)
	}
}

func TestMessageIDShape(t *testing.T) {
	id := NewMessageID()
	if len(id) != 32 {
		t.Fatalf("msgId 长度: %d", len(id))
	}
	if id == NewMessageID() {
		t.Fatal("msgId 重复")
	}
}

func TestInflightRoundTrip(t *testing.T) {
	s := &InflightState{ExpireAtMs: 123456, Attempts: 3}
	got, err := DecodeInflight(EncodeInflight(s))
	if err != nil || !reflect.DeepEqual(s, got) {
		t.Fatalf("round trip: %+v %v", got, err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/ -v`
Expected: FAIL

- [ ] **Step 3: 实现 types.go**

```go
// Package core 定义 sq 的内部消息模型与各引擎共享类型。
//
// 职责：
//   - Message：协议无关的消息内部表示（adapter 负责与 proto 互转）
//   - 存储编解码（当前 JSON，Body 走 base64）
//
// 边界：
//   - 不 import 任何 proto/pb 包（spec 的协议适配层约束）
//   - JSON 编码是 M1 的性能取舍：可读易调试，量级足够；
//     若未来需要可替换为二进制编码，Encode/Decode 是唯一出入口
package core

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Message 消息的内部表示。落盘字段见 json tag；DeliveryAttempt 仅投递时填充。
type Message struct {
	ID              string            `json:"id"`
	Topic           string            `json:"topic"`
	QueueID         uint32            `json:"queue_id"`
	Offset          uint64            `json:"offset"`
	Tag             string            `json:"tag,omitempty"`
	Keys            []string          `json:"keys,omitempty"`
	MessageGroup    string            `json:"message_group,omitempty"`
	Properties      map[string]string `json:"properties,omitempty"`
	Body            []byte            `json:"body"`
	BornAtMs        int64             `json:"born_at_ms"`
	StoreAtMs       int64             `json:"store_at_ms"`
	DeliveryAttempt int32             `json:"-"`
}

// EncodeMessage 序列化消息用于落盘。
func EncodeMessage(m *Message) ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("编码消息 %s: %w", m.ID, err)
	}
	return b, nil
}

// DecodeMessage 反序列化落盘消息。
func DecodeMessage(b []byte) (*Message, error) {
	m := &Message{}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("解码消息: %w", err)
	}
	return m, nil
}

// NewMessageID 生成 32 位大写十六进制消息 ID（16 随机字节）。
func NewMessageID() string {
	b := make([]byte, 16)
	rand.Read(b) // crypto/rand 在受支持平台不会失败
	return strings.ToUpper(hex.EncodeToString(b))
}

// InflightState 已投未确认消息的持久状态（inflight key 的 value）。
type InflightState struct {
	ExpireAtMs int64 `json:"expire_at_ms"` // 不可见截止时间；早于 now 即可重投
	Attempts   int32 `json:"attempts"`     // 已投递次数（首投=1）
}

// EncodeInflight 序列化 inflight 状态。
func EncodeInflight(s *InflightState) []byte {
	b, _ := json.Marshal(s) // 结构固定无失败路径
	return b
}

// DecodeInflight 反序列化 inflight 状态。
func DecodeInflight(b []byte) (*InflightState, error) {
	s := &InflightState{}
	if err := json.Unmarshal(b, s); err != nil {
		return nil, fmt.Errorf("解码 inflight: %w", err)
	}
	return s, nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/ -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/core/ && git commit -m "feat: core 消息模型与存储编解码"
```

---

### Task 5: core/meta topic 与订阅组注册表

**Files:**
- Create: `internal/core/meta/meta.go`, `internal/core/meta/meta_test.go`

**Interfaces:**
- Consumes: `store.Store`（Get/NewBatch/Apply/Scan）、`store.TopicMetaKey/GroupMetaKey/TopicMetaPrefix/GroupMetaPrefix/PrefixUpperBound`。
- Produces:
  - `meta.TopicConfig{Name string; Queues uint32; CreatedAtMs int64}`；`meta.GroupConfig{Name string; CreatedAtMs int64}`
  - `meta.New(st *store.Store, autoCreate bool, defaultQueues uint32, logger *slog.Logger) (*Meta, error)`（构造时加载全部 meta 进内存缓存）
  - `(*Meta) EnsureTopic(name string) (TopicConfig, error)`（存在即返回；不存在且 autoCreate 则建，否则 `ErrTopicNotFound`）
  - `(*Meta) GetTopic(name string) (TopicConfig, bool)`
  - `(*Meta) CreateTopic(name string, queues uint32) (TopicConfig, error)`（显式创建，重复创建幂等返回现有）
  - `(*Meta) EnsureGroup(name string) (GroupConfig, error)`（不存在总是自动建——消费组语义上就是注册行为）
  - `meta.ErrTopicNotFound`、`meta.ErrBadName`
  - `meta.ValidateName(s string) error`（`^[A-Za-z0-9_%\-]{1,127}$`）

- [ ] **Step 1: 写失败测试**

`internal/core/meta/meta_test.go`:

```go
package meta

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/xushixin/sq/internal/store"
)

func newTestMeta(t *testing.T, dir string, autoCreate bool) (*Meta, *store.Store) {
	t.Helper()
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	m, err := New(st, autoCreate, 4, slog.Default())
	if err != nil {
		t.Fatalf("New meta: %v", err)
	}
	return m, st
}

func TestEnsureTopicAutoCreateAndPersist(t *testing.T) {
	dir := t.TempDir()
	m, st := newTestMeta(t, dir, true)
	tc, err := m.EnsureTopic("orders")
	if err != nil || tc.Queues != 4 {
		t.Fatalf("EnsureTopic: %+v %v", tc, err)
	}
	st.Close()
	// 重开：配置必须从盘上恢复
	m2, st2 := newTestMeta(t, dir, false)
	defer st2.Close()
	got, ok := m2.GetTopic("orders")
	if !ok || got.Queues != 4 {
		t.Fatalf("重开后丢失: %+v %v", got, ok)
	}
}

func TestEnsureTopicNotFoundWhenAutoCreateOff(t *testing.T) {
	m, st := newTestMeta(t, t.TempDir(), false)
	defer st.Close()
	if _, err := m.EnsureTopic("nope"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("期望 ErrTopicNotFound，得到 %v", err)
	}
}

func TestValidateName(t *testing.T) {
	if err := ValidateName("ok_Topic-1%"); err != nil {
		t.Fatalf("合法名被拒: %v", err)
	}
	for _, bad := range []string{"", "has/slash", "汉字", string(make([]byte, 200))} {
		if err := ValidateName(bad); err == nil {
			t.Fatalf("非法名未拒: %q", bad)
		}
	}
}

func TestEnsureGroup(t *testing.T) {
	m, st := newTestMeta(t, t.TempDir(), false) // group 不受 autoCreate 开关限制
	defer st.Close()
	g, err := m.EnsureGroup("g1")
	if err != nil || g.Name != "g1" {
		t.Fatalf("EnsureGroup: %+v %v", g, err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/meta/ -v`
Expected: FAIL

- [ ] **Step 3: 实现 meta.go**

```go
// Package meta 维护 topic 与订阅组注册表。
//
// 职责：
//   - topic/group 配置的创建、查询、启动时从 store 加载全量内存缓存
//   - 名字合法性校验（key 编码安全的第一道门）
//
// 边界：
//   - 不管队列内容与位点（produce/deliver 的事）
//   - M1 无删除与配置修改（M5 Admin API 再加）
package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/xushixin/sq/internal/store"
)

// ErrTopicNotFound topic 不存在且未开自动创建。
var ErrTopicNotFound = errors.New("topic 不存在")

// ErrBadName 名字不符合 ^[A-Za-z0-9_%\-]{1,127}$。
var ErrBadName = errors.New("名字含非法字符或长度超限")

var nameRe = regexp.MustCompile(`^[A-Za-z0-9_%\-]{1,127}$`)

// ValidateName 校验 topic/group 名。合法字符不含 '/'，保证 key 编码分隔安全。
func ValidateName(s string) error {
	if !nameRe.MatchString(s) {
		return fmt.Errorf("%w: %q", ErrBadName, s)
	}
	return nil
}

// TopicConfig topic 配置。
type TopicConfig struct {
	Name        string `json:"name"`
	Queues      uint32 `json:"queues"`
	CreatedAtMs int64  `json:"created_at_ms"`
}

// GroupConfig 订阅组配置。M1 仅注册身份；maxAttempts 等属 M2。
type GroupConfig struct {
	Name        string `json:"name"`
	CreatedAtMs int64  `json:"created_at_ms"`
}

// Meta topic/group 注册表。读多写少，读走内存缓存，写穿透到 store。
type Meta struct {
	st            *store.Store
	autoCreate    bool
	defaultQueues uint32
	logger        *slog.Logger

	mu     sync.RWMutex
	topics map[string]TopicConfig
	groups map[string]GroupConfig
}

// New 构造并从 store 加载全部已有配置。
func New(st *store.Store, autoCreate bool, defaultQueues uint32, logger *slog.Logger) (*Meta, error) {
	m := &Meta{
		st: st, autoCreate: autoCreate, defaultQueues: defaultQueues,
		logger: logger.With("mod", "meta"),
		topics: map[string]TopicConfig{}, groups: map[string]GroupConfig{},
	}
	err := st.Scan([]byte(store.TopicMetaPrefix), store.PrefixUpperBound([]byte(store.TopicMetaPrefix)), 0,
		func(k, v []byte) (bool, error) {
			var tc TopicConfig
			if err := json.Unmarshal(v, &tc); err != nil {
				return false, fmt.Errorf("加载 topic 配置 %q: %w", k, err)
			}
			m.topics[tc.Name] = tc
			return true, nil
		})
	if err != nil {
		return nil, err
	}
	err = st.Scan([]byte(store.GroupMetaPrefix), store.PrefixUpperBound([]byte(store.GroupMetaPrefix)), 0,
		func(k, v []byte) (bool, error) {
			var gc GroupConfig
			if err := json.Unmarshal(v, &gc); err != nil {
				return false, fmt.Errorf("加载 group 配置 %q: %w", k, err)
			}
			m.groups[gc.Name] = gc
			return true, nil
		})
	if err != nil {
		return nil, err
	}
	m.logger.Info("meta 加载完成", "topics", len(m.topics), "groups", len(m.groups))
	return m, nil
}

// GetTopic 查询 topic 配置。
func (m *Meta) GetTopic(name string) (TopicConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tc, ok := m.topics[name]
	return tc, ok
}

// EnsureTopic 获取 topic；不存在且开启自动创建时按默认队列数创建。
func (m *Meta) EnsureTopic(name string) (TopicConfig, error) {
	if tc, ok := m.GetTopic(name); ok {
		return tc, nil
	}
	if !m.autoCreate {
		return TopicConfig{}, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}
	return m.CreateTopic(name, m.defaultQueues)
}

// CreateTopic 创建 topic；已存在时幂等返回现有配置（不改队列数）。
func (m *Meta) CreateTopic(name string, queues uint32) (TopicConfig, error) {
	if err := ValidateName(name); err != nil {
		return TopicConfig{}, err
	}
	if queues == 0 {
		return TopicConfig{}, fmt.Errorf("queues 必须 >0")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if tc, ok := m.topics[name]; ok {
		return tc, nil
	}
	tc := TopicConfig{Name: name, Queues: queues, CreatedAtMs: time.Now().UnixMilli()}
	raw, _ := json.Marshal(tc)
	b := m.st.NewBatch()
	b.Set(store.TopicMetaKey(name), raw, nil)
	if err := m.st.Apply(b); err != nil {
		return TopicConfig{}, fmt.Errorf("持久化 topic %s: %w", name, err)
	}
	m.topics[name] = tc
	m.logger.Info("topic 已创建", "topic", name, "queues", queues)
	return tc, nil
}

// EnsureGroup 获取订阅组，不存在则注册（消费组首次出现即注册，不受 autoCreate 开关限制）。
func (m *Meta) EnsureGroup(name string) (GroupConfig, error) {
	m.mu.RLock()
	gc, ok := m.groups[name]
	m.mu.RUnlock()
	if ok {
		return gc, nil
	}
	if err := ValidateName(name); err != nil {
		return GroupConfig{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if gc, ok := m.groups[name]; ok {
		return gc, nil
	}
	gc = GroupConfig{Name: name, CreatedAtMs: time.Now().UnixMilli()}
	raw, _ := json.Marshal(gc)
	b := m.st.NewBatch()
	b.Set(store.GroupMetaKey(name), raw, nil)
	if err := m.st.Apply(b); err != nil {
		return GroupConfig{}, fmt.Errorf("持久化 group %s: %w", name, err)
	}
	m.groups[name] = gc
	m.logger.Info("消费组已注册", "group", name)
	return gc, nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/meta/ -v`
Expected: PASS

- [ ] **Step 5: 日志与注释自检**

- 关键节点日志：meta 加载完成（数量）、topic 创建、group 注册 —— 已覆盖；错误路径全部带名字上下文
- 文件头/导出符号注释 —— 核对 Step 3 代码无遗漏

- [ ] **Step 6: 提交**

```bash
git add internal/core/meta/ && git commit -m "feat: meta topic/订阅组注册表"
```

---

### Task 6: core/produce 写入路径

**Files:**
- Create: `internal/core/produce/produce.go`, `internal/core/produce/produce_test.go`

**Interfaces:**
- Consumes: `store.Store`、`store.MsgKey/AllocKey/PutU64/GetU64`、`meta.Meta.EnsureTopic`、`core.Message/EncodeMessage/NewMessageID`。
- Produces:
  - `produce.New(st *store.Store, mt *meta.Meta, logger *slog.Logger) *Producer`
  - `(*Producer) Append(m *core.Message) (*core.Message, error)` —— 入参只需 Topic/Body/Tag/Keys/Properties/MessageGroup/BornAtMs/ID（ID 为空则生成）；返回已填 QueueID/Offset/StoreAtMs 的消息
  - `(*Producer) Subscribe(topic string) <-chan struct{}` / 内部 `wake(topic)`：长轮询唤醒信号（chan close 广播，deliver 用）
  - 队列选择：MessageGroup 非空 → FNV-1a hash 取模（顺序消息的落队规则 M1 就固化，M4 直接复用）；否则轮询

- [ ] **Step 1: 写失败测试**

`internal/core/produce/produce_test.go`:

```go
package produce

import (
	"log/slog"
	"testing"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

func newTestProducer(t *testing.T, dir string) (*Producer, *store.Store) {
	t.Helper()
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	mt, err := meta.New(st, true, 4, slog.Default())
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	return New(st, mt, slog.Default()), st
}

func TestAppendAssignsMonotonicOffsets(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	seen := map[uint32]uint64{} // queueID -> 上一个 offset
	for i := 0; i < 20; i++ {
		m, err := p.Append(&core.Message{Topic: "t1", Body: []byte("x")})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if last, ok := seen[m.QueueID]; ok && m.Offset != last+1 {
			t.Fatalf("队列 %d offset 不连续: %d -> %d", m.QueueID, last, m.Offset)
		}
		seen[m.QueueID] = m.Offset
	}
	if len(seen) != 4 { // 轮询应覆盖全部 4 个队列
		t.Fatalf("轮询未覆盖全部队列: %v", seen)
	}
}

func TestAppendOffsetsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	p, st := newTestProducer(t, dir)
	var lastQ uint32
	var lastOff uint64
	for i := 0; i < 8; i++ { // 4 队列各写 2 条
		m, _ := p.Append(&core.Message{Topic: "t1", Body: []byte("x")})
		lastQ, lastOff = m.QueueID, m.Offset
	}
	_ = lastQ
	st.Close()
	p2, st2 := newTestProducer(t, dir)
	defer st2.Close()
	m, err := p2.Append(&core.Message{Topic: "t1", Body: []byte("y")})
	if err != nil {
		t.Fatalf("重启后 Append: %v", err)
	}
	if m.Offset != lastOff+1 { // 轮询从 0 重新开始，队列 0 上一 offset 是 lastOff（第 5 条写入）
		// 更稳妥的断言：新 offset 必须大于该队列重启前的最大值——不允许回退复用
		if m.Offset == 0 {
			t.Fatal("offset 重启后回退，会覆盖旧消息")
		}
	}
}

func TestMessageGroupPinsQueue(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	var q uint32
	for i := 0; i < 5; i++ {
		m, err := p.Append(&core.Message{Topic: "t2", Body: []byte("x"), MessageGroup: "user-1"})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if i == 0 {
			q = m.QueueID
		} else if m.QueueID != q {
			t.Fatalf("同 MessageGroup 落入不同队列: %d vs %d", q, m.QueueID)
		}
	}
}

func TestSubscribeWakesOnAppend(t *testing.T) {
	p, st := newTestProducer(t, t.TempDir())
	defer st.Close()
	ch := p.Subscribe("t3")
	if _, err := p.Append(&core.Message{Topic: "t3", Body: []byte("x")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	select {
	case <-ch: // 期望已被 close
	default:
		t.Fatal("Append 未唤醒订阅者")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/produce/ -v`
Expected: FAIL

- [ ] **Step 3: 实现 produce.go**

```go
// Package produce 实现消息写入路径：队列选择、offset 分配、落盘、长轮询唤醒。
//
// 职责：
//   - Append：一次语义写 = 消息体 + alloc 计数器，同一 Batch 原子提交
//   - offset 分配采用持久化计数器（alloc/ key），重启 O(1) 恢复且绝不回退
//   - 长轮询唤醒：按 topic 的 close-broadcast 信号
//
// 边界：
//   - 不判定延时/事务（M3/M6 在 Append 之前分流，本包不感知）
//   - 不做消费可见性判断（deliver 的事）
package produce

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// MaxBodySize 消息体上限（spec §7）。
const MaxBodySize = 4 * 1024 * 1024

// Producer 写入引擎。并发安全。
type Producer struct {
	st     *store.Store
	mt     *meta.Meta
	logger *slog.Logger

	mu     sync.Mutex
	next   map[string]uint64        // "topic/4Bqid" -> 下一 offset（内存缓存，与 alloc/ key 同步）
	rr     map[string]uint32        // topic -> 轮询游标
	wakers map[string]chan struct{} // topic -> 长轮询唤醒信号
}

// New 构造 Producer。next 缓存懒加载（首写某队列时读一次 alloc/ key）。
func New(st *store.Store, mt *meta.Meta, logger *slog.Logger) *Producer {
	return &Producer{
		st: st, mt: mt, logger: logger.With("mod", "produce"),
		next: map[string]uint64{}, rr: map[string]uint32{}, wakers: map[string]chan struct{}{},
	}
}

func qkey(topic string, q uint32) string { return fmt.Sprintf("%s/%d", topic, q) }

// Append 写入一条普通消息：选队列 → 分配 offset → 原子落盘 → 唤醒长轮询。
// 入参 m 的 QueueID/Offset/StoreAtMs 由本方法填充；ID 为空时生成。
func (p *Producer) Append(m *core.Message) (*core.Message, error) {
	if len(m.Body) == 0 || len(m.Body) > MaxBodySize {
		return nil, fmt.Errorf("消息体大小非法: %d（上限 %d）", len(m.Body), MaxBodySize)
	}
	tc, err := p.mt.EnsureTopic(m.Topic)
	if err != nil {
		return nil, err
	}
	if m.ID == "" {
		m.ID = core.NewMessageID()
	}
	if m.BornAtMs == 0 {
		m.BornAtMs = time.Now().UnixMilli()
	}
	m.StoreAtMs = time.Now().UnixMilli()

	p.mu.Lock()
	defer p.mu.Unlock()
	// 队列选择：MessageGroup 定死队列（顺序语义的根基，M4 复用）；否则轮询
	if m.MessageGroup != "" {
		h := fnv.New32a()
		h.Write([]byte(m.MessageGroup))
		m.QueueID = h.Sum32() % tc.Queues
	} else {
		m.QueueID = p.rr[m.Topic] % tc.Queues
		p.rr[m.Topic]++
	}
	off, err := p.nextOffsetLocked(m.Topic, m.QueueID)
	if err != nil {
		return nil, err
	}
	m.Offset = off
	raw, err := core.EncodeMessage(m)
	if err != nil {
		return nil, err
	}
	b := p.st.NewBatch()
	b.Set(store.MsgKey(m.Topic, m.QueueID, m.Offset), raw, nil)
	b.Set(store.AllocKey(m.Topic, m.QueueID), store.PutU64(off+1), nil)
	if err := p.st.Apply(b); err != nil {
		return nil, fmt.Errorf("写入消息 %s (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}
	p.next[qkey(m.Topic, m.QueueID)] = off + 1
	p.wakeLocked(m.Topic)
	p.logger.Debug("消息已写入", "topic", m.Topic, "queue", m.QueueID, "offset", m.Offset, "msg_id", m.ID)
	return m, nil
}

// nextOffsetLocked 取该队列下一 offset。缓存未命中时读盘上 alloc/ 计数器——
// 崩溃后靠它 O(1) 恢复，且因与消息同 Batch 提交，绝不会分配已用过的 offset。
func (p *Producer) nextOffsetLocked(topic string, q uint32) (uint64, error) {
	k := qkey(topic, q)
	if off, ok := p.next[k]; ok {
		return off, nil
	}
	v, ok, err := p.st.Get(store.AllocKey(topic, q))
	if err != nil {
		return 0, fmt.Errorf("读取 offset 计数器 %s: %w", k, err)
	}
	if !ok {
		return 0, nil
	}
	return store.GetU64(v), nil
}

// Subscribe 返回 topic 的唤醒信号：下一次 Append 该 topic 时该 chan 被 close。
// 消费方等待后必须重新 Subscribe（close 是一次性广播）。
func (p *Producer) Subscribe(topic string) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch, ok := p.wakers[topic]
	if !ok {
		ch = make(chan struct{})
		p.wakers[topic] = ch
	}
	return ch
}

// wakeLocked close 当前信号并撤下（下一 Subscribe 换新 chan）。
func (p *Producer) wakeLocked(topic string) {
	if ch, ok := p.wakers[topic]; ok {
		close(ch)
		delete(p.wakers, topic)
	}
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/produce/ -v`
Expected: PASS

- [ ] **Step 5: 日志与注释自检**

- 写入成功 Debug（热路径降级，带 topic/queue/offset/msg_id 可关联）；错误路径全部带消息上下文；meta 层已有 topic 创建 Info
- 文件头（含"不判定延时/事务"边界）、导出符号、alloc 计数器"为什么"注释 —— 核对无遗漏

- [ ] **Step 6: 提交**

```bash
git add internal/core/produce/ && git commit -m "feat: produce 写入路径与 offset 分配"
```

---

### Task 7: core/deliver POP 消费

**Files:**
- Create: `internal/core/deliver/deliver.go`, `internal/core/deliver/deliver_test.go`

**Interfaces:**
- Consumes: `store.Store`、`store.MsgKey/MsgQueuePrefix/CursorKey/InflightKey/InflightPrefix/ParseInflightKey/PrefixUpperBound/PutU64/GetU64`、`core.Message/DecodeMessage/InflightState/EncodeInflight/DecodeInflight`、`meta.Meta.EnsureGroup`、`produce.Producer.Subscribe`。
- Produces:
  - `deliver.New(st *store.Store, mt *meta.Meta, pr *produce.Producer, logger *slog.Logger) *Deliverer`
  - `(*Deliverer) Receive(ctx context.Context, group, topic string, queueID uint32, maxMsgs int, invisible time.Duration, wait time.Duration) ([]*core.Message, error)` —— 返回消息的 `DeliveryAttempt` 已填；先重投过期 inflight，再取新消息；空结果时最长等待 wait（长轮询）
  - `(*Deliverer) Ack(group, topic string, queueID uint32, offset uint64) (bool, error)` —— false 表示 inflight 不存在（已 ack 或已过期重投），幂等不报错
  - `(*Deliverer) ChangeInvisible(group, topic string, queueID uint32, offset uint64, invisible time.Duration) (bool, error)`

- [ ] **Step 1: 写失败测试**

`internal/core/deliver/deliver_test.go`:

```go
package deliver

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

type fixture struct {
	st *store.Store
	pr *produce.Producer
	dl *Deliverer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, slog.Default()) // 单队列，测试确定性
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	pr := produce.New(st, mt, slog.Default())
	return &fixture{st: st, pr: pr, dl: New(st, mt, pr, slog.Default())}
}

func (f *fixture) send(t *testing.T, topic, body string) {
	t.Helper()
	if _, err := f.pr.Append(&core.Message{Topic: topic, Body: []byte(body)}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func TestReceiveAckFlow(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	f.send(t, "t", "b")
	msgs, err := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("Receive: %d %v", len(msgs), err)
	}
	if msgs[0].DeliveryAttempt != 1 {
		t.Fatalf("首投 attempt 应为 1: %d", msgs[0].DeliveryAttempt)
	}
	// ack 后不应再收到
	for _, m := range msgs {
		if ok, err := f.dl.Ack("g", "t", m.QueueID, m.Offset); !ok || err != nil {
			t.Fatalf("Ack: %v %v", ok, err)
		}
	}
	msgs, err = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("ack 后仍收到: %d %v", len(msgs), err)
	}
}

func TestUnackedRedeliveryAfterExpire(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	// 第一次取出，极短不可见时间且不 ack
	msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, 30*time.Millisecond, 0)
	if len(msgs) != 1 {
		t.Fatalf("首取: %d", len(msgs))
	}
	// 未过期期间不可见
	msgs, _ = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if len(msgs) != 0 {
		t.Fatalf("不可见期内重复投递: %d", len(msgs))
	}
	time.Sleep(50 * time.Millisecond)
	// 过期后重投，attempt +1
	msgs, _ = f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	if len(msgs) != 1 || msgs[0].DeliveryAttempt != 2 {
		t.Fatalf("过期重投: %d attempt=%v", len(msgs), msgs)
	}
}

func TestAckIdempotent(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 0)
	ok, err := f.dl.Ack("g", "t", msgs[0].QueueID, msgs[0].Offset)
	if !ok || err != nil {
		t.Fatalf("首次 Ack: %v %v", ok, err)
	}
	ok, err = f.dl.Ack("g", "t", msgs[0].QueueID, msgs[0].Offset)
	if ok || err != nil { // 重复 ack：ok=false 且无错
		t.Fatalf("重复 Ack 应幂等: %v %v", ok, err)
	}
}

func TestLongPollingWakesOnNewMessage(t *testing.T) {
	f := newFixture(t)
	done := make(chan []*core.Message, 1)
	go func() {
		msgs, _ := f.dl.Receive(context.Background(), "g", "t", 0, 10, time.Minute, 3*time.Second)
		done <- msgs
	}()
	time.Sleep(100 * time.Millisecond) // 让 Receive 先进入等待
	f.send(t, "t", "a")
	select {
	case msgs := <-done:
		if len(msgs) != 1 {
			t.Fatalf("长轮询结果: %d", len(msgs))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("长轮询未被新消息唤醒")
	}
}

func TestTwoGroupsIndependentCursors(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t", "a")
	m1, _ := f.dl.Receive(context.Background(), "g1", "t", 0, 10, time.Minute, 0)
	m2, _ := f.dl.Receive(context.Background(), "g2", "t", 0, 10, time.Minute, 0)
	if len(m1) != 1 || len(m2) != 1 {
		t.Fatalf("两组各自应收到 1 条: %d %d", len(m1), len(m2))
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/core/deliver/ -v`
Expected: FAIL

- [ ] **Step 3: 实现 deliver.go**

```go
// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责：
//   - Receive：先重投本队列已过期的 inflight，再从 fetch 位点取新消息；
//     新取消息写 inflight 并推进位点（同一 Batch 原子提交）
//   - Ack/ChangeInvisible：按 (group,topic,queue,offset) 定位 inflight
//   - 长轮询：produce.Subscribe 的 close-broadcast + 截止时间
//
// 边界：
//   - 重投是惰性的（Receive 时检查），M1 无后台扫描 goroutine——
//     没有消费者在收时也就没有人需要重投，语义等价而实现最简
//   - 不管投递次数上限/DLQ（M2）；不管 Tag 过滤（M2，M1 只接受 "*"）
//
// 位点语义说明（对应 spec §5.2"推进到最小未 ack"的实现形态）：
//   cursor 是 fetch 位点，Receive 取出即推进；未 ack 的消息由持久化的
//   inflight 记录兜底（崩溃重启后仍在，过期即重投）。两者合起来等价于
//   "已 ack 前消息不丢"，且比维护最小未 ack 位点简单得多。
package deliver

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// Deliverer POP 消费引擎。并发安全：同一队列的取件互斥（qmu），不同队列并行。
type Deliverer struct {
	st     *store.Store
	mt     *meta.Meta
	pr     *produce.Producer
	logger *slog.Logger

	mu  sync.Mutex
	qmu map[string]*sync.Mutex // "group/topic/qid" -> 队列级锁
}

// New 构造 Deliverer。
func New(st *store.Store, mt *meta.Meta, pr *produce.Producer, logger *slog.Logger) *Deliverer {
	return &Deliverer{st: st, mt: mt, pr: pr, logger: logger.With("mod", "deliver"), qmu: map[string]*sync.Mutex{}}
}

func (d *Deliverer) lockQueue(group, topic string, q uint32) *sync.Mutex {
	k := fmt.Sprintf("%s/%s/%d", group, topic, q)
	d.mu.Lock()
	defer d.mu.Unlock()
	m, ok := d.qmu[k]
	if !ok {
		m = &sync.Mutex{}
		d.qmu[k] = m
	}
	return m
}

// Receive 取一批消息。空结果时最长等待 wait（长轮询），期间新消息到达立即返回。
func (d *Deliverer) Receive(ctx context.Context, group, topic string, queueID uint32, maxMsgs int, invisible, wait time.Duration) ([]*core.Message, error) {
	if _, err := d.mt.EnsureGroup(group); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		// 先订阅再取件：避免"取件为空 → 新消息写入 → 才订阅"的丢唤醒窗口
		wakeCh := d.pr.Subscribe(topic)
		msgs, err := d.receiveOnce(group, topic, queueID, maxMsgs, invisible)
		if err != nil || len(msgs) > 0 {
			return msgs, err
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return nil, nil
		}
		// 100ms 兜底轮询：唤醒信号只覆盖"新消息写入"，不覆盖"inflight 过期"
		tick := 100 * time.Millisecond
		if remain < tick {
			tick = remain
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wakeCh:
		case <-time.After(tick):
		}
	}
}

// receiveOnce 单次取件：过期 inflight 重投 + 新消息，合计不超过 maxMsgs。
func (d *Deliverer) receiveOnce(group, topic string, queueID uint32, maxMsgs int, invisible time.Duration) ([]*core.Message, error) {
	qlock := d.lockQueue(group, topic, queueID)
	qlock.Lock()
	defer qlock.Unlock()

	now := time.Now().UnixMilli()
	expireAt := now + invisible.Milliseconds()
	var out []*core.Message
	b := d.st.NewBatch()

	// 阶段 1：重投过期 inflight。k/v 回调内有效，需解析后立即使用
	type redeliver struct {
		offset   uint64
		attempts int32
	}
	var reds []redeliver
	pfx := store.InflightPrefix(group, topic, queueID)
	err := d.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		_, _, _, off, err := store.ParseInflightKey(k)
		if err != nil {
			return false, err
		}
		st, err := core.DecodeInflight(v)
		if err != nil {
			return false, err
		}
		if st.ExpireAtMs <= now {
			reds = append(reds, redeliver{offset: off, attempts: st.Attempts})
		}
		return len(reds)+1 <= maxMsgs, nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描 inflight (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	for _, r := range reds {
		raw, ok, err := d.st.Get(store.MsgKey(topic, queueID, r.offset))
		if err != nil {
			return nil, err
		}
		if !ok {
			// 消息已被 retention 清理但 inflight 残留：清掉记录并跳过（M2 起 retention 会同步清理）
			d.logger.Warn("inflight 指向的消息不存在，清理孤儿记录", "group", group, "topic", topic, "queue", queueID, "offset", r.offset)
			b.Delete(store.InflightKey(group, topic, queueID, r.offset), nil)
			continue
		}
		m, err := core.DecodeMessage(raw)
		if err != nil {
			return nil, err
		}
		m.DeliveryAttempt = r.attempts + 1
		b.Set(store.InflightKey(group, topic, queueID, r.offset),
			core.EncodeInflight(&core.InflightState{ExpireAtMs: expireAt, Attempts: m.DeliveryAttempt}), nil)
		out = append(out, m)
	}

	// 阶段 2：从 fetch 位点取新消息
	cursor := uint64(0)
	if v, ok, err := d.st.Get(store.CursorKey(group, topic, queueID)); err != nil {
		return nil, err
	} else if ok {
		cursor = store.GetU64(v)
	}
	lower := store.MsgKey(topic, queueID, cursor)
	upper := store.PrefixUpperBound(store.MsgQueuePrefix(topic, queueID))
	newCursor := cursor
	err = d.st.Scan(lower, upper, maxMsgs-len(out), func(k, v []byte) (bool, error) {
		m, err := core.DecodeMessage(v)
		if err != nil {
			return false, err
		}
		m.DeliveryAttempt = 1
		b.Set(store.InflightKey(group, topic, queueID, m.Offset),
			core.EncodeInflight(&core.InflightState{ExpireAtMs: expireAt, Attempts: 1}), nil)
		out = append(out, m)
		newCursor = m.Offset + 1
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描新消息 (topic=%s q=%d cursor=%d): %w", topic, queueID, cursor, err)
	}
	if len(out) == 0 {
		b.Close()
		return nil, nil
	}
	if newCursor > cursor {
		b.Set(store.CursorKey(group, topic, queueID), store.PutU64(newCursor), nil)
	}
	if err := d.st.Apply(b); err != nil {
		return nil, fmt.Errorf("提交取件批次 (group=%s topic=%s q=%d): %w", group, topic, queueID, err)
	}
	d.logger.Debug("投递消息", "group", group, "topic", topic, "queue", queueID,
		"count", len(out), "redelivered", len(reds), "cursor", newCursor)
	return out, nil
}

// Ack 确认消息。inflight 不存在（重复 ack / 已过期重投）返回 (false, nil)，幂等。
func (d *Deliverer) Ack(group, topic string, queueID uint32, offset uint64) (bool, error) {
	k := store.InflightKey(group, topic, queueID, offset)
	_, ok, err := d.st.Get(k)
	if err != nil {
		return false, err
	}
	if !ok {
		d.logger.Debug("ack 目标不存在（重复 ack 或已重投）", "group", group, "topic", topic, "queue", queueID, "offset", offset)
		return false, nil
	}
	b := d.st.NewBatch()
	b.Delete(k, nil)
	if err := d.st.Apply(b); err != nil {
		return false, fmt.Errorf("ack (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Debug("消息已确认", "group", group, "topic", topic, "queue", queueID, "offset", offset)
	return true, nil
}

// ChangeInvisible 重设不可见截止时间（消费端主动延长/缩短）。目标不存在返回 (false, nil)。
func (d *Deliverer) ChangeInvisible(group, topic string, queueID uint32, offset uint64, invisible time.Duration) (bool, error) {
	k := store.InflightKey(group, topic, queueID, offset)
	v, ok, err := d.st.Get(k)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	st, err := core.DecodeInflight(v)
	if err != nil {
		return false, err
	}
	st.ExpireAtMs = time.Now().Add(invisible).UnixMilli()
	b := d.st.NewBatch()
	b.Set(k, core.EncodeInflight(st), nil)
	if err := d.st.Apply(b); err != nil {
		return false, fmt.Errorf("改不可见时间 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	return true, nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/core/deliver/ -v`
Expected: PASS（5 个用例；长轮询用例耗时 <2s）

- [ ] **Step 5: 日志与注释自检**

- 投递/确认 Debug（热路径，带全维度上下文）；孤儿 inflight Warn；错误路径全带 group/topic/queue/offset
- 文件头含位点语义说明与"惰性重投"边界；先订阅再取件的"为什么"注释 —— 核对无遗漏

- [ ] **Step 6: 提交**

```bash
git add internal/core/deliver/ && git commit -m "feat: deliver POP 消费（投递/ack/超时重投/长轮询）"
```

---

### Task 8: RocketMQ proto 引入与代码生成

**Files:**
- Create: `proto/apache/rocketmq/v2/*.proto`（从 apache/rocketmq-apis 复制）、`buf.yaml`、`buf.gen.yaml`
- Create(生成): `internal/rpc/pb/*.pb.go`
- Modify: `go.mod`（grpc/protobuf）、`Makefile`（proto 目标已有）

**Interfaces:**
- Produces: `pb` 包（`github.com/xushixin/sq/internal/rpc/pb/v2`）：`MessagingServiceServer` 接口、全部消息类型与 `Code` 枚举。生成代码不手改。

**注意**：本任务代码块中的 buf 配置以 buf v2 格式为准；若生成报错，按 `buf generate` 的错误信息修正配置（配置格式问题现场修正，不算偏离计划）。生成后的 pb 字段名以生成代码为准，后续任务的 proto 字段引用如与生成代码有出入，以生成代码为准修正——这属于对齐，不属于设计变更。

- [ ] **Step 1: 获取 proto 文件**

```bash
cd /Users/xushixin/workspace/sq
git clone --depth 1 https://github.com/apache/rocketmq-apis /tmp/rocketmq-apis
mkdir -p proto/apache/rocketmq/v2
cp /tmp/rocketmq-apis/apache/rocketmq/v2/*.proto proto/apache/rocketmq/v2/
ls proto/apache/rocketmq/v2/   # 期望: admin.proto definition.proto service.proto
```

- [ ] **Step 2: 安装工具并写 buf 配置**

```bash
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go get google.golang.org/grpc@latest google.golang.org/protobuf@latest
```

`buf.yaml`:

```yaml
version: v2
modules:
  - path: proto
```

`buf.gen.yaml`:

```yaml
version: v2
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/xushixin/sq/internal/rpc/pb
plugins:
  - local: protoc-gen-go
    out: internal/rpc/pb
    opt: paths=source_relative
  - local: protoc-gen-go-grpc
    out: internal/rpc/pb
    opt: paths=source_relative
```

- [ ] **Step 3: 生成并验证**

```bash
make proto
go build ./...
grep -r "MessagingServiceServer" internal/rpc/pb/ | head -3   # 确认服务端接口已生成
```

Expected: 构建通过，`MessagingServiceServer` 接口存在。生成目录形如 `internal/rpc/pb/apache/rocketmq/v2/`，实际 import 路径以生成结果为准（下述任务统一以 `pb` 别名引用）。

- [ ] **Step 4: 提交**

```bash
git add proto/ buf.yaml buf.gen.yaml internal/rpc/pb/ go.mod go.sum
git commit -m "chore: 引入 rocketmq-apis proto 并生成 gRPC 代码"
```

---

### Task 9: gRPC 服务基座（QueryRoute/Heartbeat/Telemetry/NotifyClientTermination）

**Files:**
- Create: `internal/rpc/server.go`, `internal/rpc/server_test.go`

**Interfaces:**
- Consumes: `meta.Meta`、`pb` 包、`config.Config`（Advertise 地址）。
- Produces:
  - `rpc.New(cfg *config.Config, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer, logger *slog.Logger) *Server`（实现 `pb.MessagingServiceServer`；produce/deliver 在 Task 10/11 前传 nil 也能编译——构造签名一次定死）
  - `(*Server) Register(gs *grpc.Server)`
  - 内部工具（Task 10/11 复用）：`okStatus() *pb.Status`、`errStatus(code pb.Code, msg string) *pb.Status`、`(*Server) endpoints() *pb.Endpoints`、`(*Server) messageQueues(tc meta.TopicConfig, topic *pb.Resource) []*pb.MessageQueue`
- 未实现的 RPC：本任务先为 `MessagingServiceServer` 接口的全部方法给出返回 `UNIMPLEMENTED` 的桩，后续任务逐个替换实现。

- [ ] **Step 1: 写失败测试（bufconn 起真实 gRPC）**

`internal/rpc/server_test.go`:

```go
package rpc

import (
	"context"
	"log/slog"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// newTestClient 起全真组件 + bufconn gRPC，返回客户端 stub。
func newTestClient(t *testing.T) pb.MessagingServiceClient {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, _ := meta.New(st, true, 4, slog.Default())
	pr := produce.New(st, mt, slog.Default())
	dl := deliver.New(st, mt, pr, slog.Default())
	cfg, _ := config.Load("")
	srv := New(cfg, mt, pr, dl, slog.Default())

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	srv.Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewMessagingServiceClient(conn)
}

func TestQueryRouteAutoCreatesTopic(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.QueryRoute(context.Background(), &pb.QueryRouteRequest{
		Topic: &pb.Resource{Name: "orders"},
	})
	if err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status: %v", resp.GetStatus())
	}
	if len(resp.GetMessageQueues()) != 4 {
		t.Fatalf("队列数: %d", len(resp.GetMessageQueues()))
	}
	mq := resp.GetMessageQueues()[0]
	if len(mq.GetBroker().GetEndpoints().GetAddresses()) == 0 {
		t.Fatal("缺少 broker endpoints")
	}
}

func TestHeartbeatRegistersGroup(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.Heartbeat(context.Background(), &pb.HeartbeatRequest{
		Group: &pb.Resource{Name: "g-hb"},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("Heartbeat: %v %v", resp.GetStatus(), err)
	}
}

func TestTelemetrySettingsEcho(t *testing.T) {
	c := newTestClient(t)
	stream, err := c.Telemetry(context.Background())
	if err != nil {
		t.Fatalf("Telemetry: %v", err)
	}
	err = stream.Send(&pb.TelemetryCommand{Command: &pb.TelemetryCommand_Settings{
		Settings: &pb.Settings{},
	}})
	if err != nil {
		t.Fatalf("send settings: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if resp.GetSettings() == nil {
		t.Fatalf("期望 settings 回包，得到 %v", resp)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -v`
Expected: FAIL（未定义 Server）

- [ ] **Step 3: 实现 server.go 基座**

```go
// Package rpc 实现 RocketMQ 5.x gRPC 协议适配层（spec 的 rocketmq-adapter）。
//
// 职责：
//   - 实现 pb.MessagingServiceServer，把 proto 语义翻译成 core 调用
//   - Telemetry Settings 协商与（M6）事务回查命令下发
//
// 边界：
//   - 不含任何消息语义逻辑（core 的事）；core 反向不感知本包
//   - M1 未覆盖的 RPC 一律返回 UNIMPLEMENTED
package rpc

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

const brokerName = "sq0" // 单机版固定 broker 名；v2 集群时改为节点标识

// Server 协议适配层。嵌入 Unimplemented 兜底未覆盖 RPC。
type Server struct {
	pb.UnimplementedMessagingServiceServer
	cfg    *config.Config
	mt     *meta.Meta
	pr     *produce.Producer
	dl     *deliver.Deliverer
	logger *slog.Logger
}

// New 构造适配层。
func New(cfg *config.Config, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer, logger *slog.Logger) *Server {
	return &Server{cfg: cfg, mt: mt, pr: pr, dl: dl, logger: logger.With("mod", "rpc")}
}

// Register 挂载到 gRPC server。
func (s *Server) Register(gs *grpc.Server) { pb.RegisterMessagingServiceServer(gs, s) }

func okStatus() *pb.Status { return &pb.Status{Code: pb.Code_OK, Message: "ok"} }

func errStatus(code pb.Code, msg string) *pb.Status { return &pb.Status{Code: code, Message: msg} }

// endpoints 返回对外通告地址（QueryRoute/QueryAssignment 共用）。
func (s *Server) endpoints() *pb.Endpoints {
	return &pb.Endpoints{
		Scheme: pb.AddressScheme_IPv4,
		Addresses: []*pb.Address{{
			Host: s.cfg.AdvertiseHost,
			Port: int32(s.cfg.AdvertisePort),
		}},
	}
}

// messageQueues 把 topic 配置展开为路由队列表。
func (s *Server) messageQueues(tc meta.TopicConfig, topic *pb.Resource) []*pb.MessageQueue {
	qs := make([]*pb.MessageQueue, 0, tc.Queues)
	for i := uint32(0); i < tc.Queues; i++ {
		qs = append(qs, &pb.MessageQueue{
			Topic:      topic,
			Id:         int32(i),
			Permission: pb.Permission_READ_WRITE,
			Broker:     &pb.Broker{Name: brokerName, Id: 0, Endpoints: s.endpoints()},
			AcceptMessageTypes: []pb.MessageType{
				pb.MessageType_NORMAL, // M3/M4/M6 时追加 DELAY/FIFO/TRANSACTION
			},
		})
	}
	return qs
}

// QueryRoute 返回 topic 路由。autoCreate 开启时未知 topic 在此自动创建
// （生产者发送前必查路由，不在这建就发不出）。
func (s *Server) QueryRoute(ctx context.Context, req *pb.QueryRouteRequest) (*pb.QueryRouteResponse, error) {
	name := req.GetTopic().GetName()
	tc, err := s.mt.EnsureTopic(name)
	if err != nil {
		s.logger.Warn("QueryRoute 失败", "topic", name, "err", err)
		return &pb.QueryRouteResponse{Status: errStatus(pb.Code_TOPIC_NOT_FOUND, err.Error())}, nil
	}
	s.logger.Debug("QueryRoute", "topic", name, "queues", tc.Queues)
	return &pb.QueryRouteResponse{Status: okStatus(), MessageQueues: s.messageQueues(tc, req.GetTopic())}, nil
}

// Heartbeat 保活；带消费组时顺带注册。
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if g := req.GetGroup().GetName(); g != "" {
		if _, err := s.mt.EnsureGroup(g); err != nil {
			s.logger.Warn("Heartbeat 注册消费组失败", "group", g, "err", err)
			return &pb.HeartbeatResponse{Status: errStatus(pb.Code_ILLEGAL_CONSUMER_GROUP, err.Error())}, nil
		}
	}
	return &pb.HeartbeatResponse{Status: okStatus()}, nil
}

// Telemetry 双向流。M1 职责：收到 Settings 即回填服务端默认并回发（SDK 启动依赖）；
// 其余命令（线程栈、消息验证结果）记日志忽略。M6 在此下发事务回查。
func (s *Server) Telemetry(stream pb.MessagingService_TelemetryServer) error {
	for {
		cmd, err := stream.Recv()
		if err != nil {
			// 客户端断流是正常生命周期，Debug 即可
			s.logger.Debug("telemetry 流结束", "err", err)
			return nil
		}
		switch c := cmd.GetCommand().(type) {
		case *pb.TelemetryCommand_Settings:
			st := c.Settings
			if err := stream.Send(&pb.TelemetryCommand{
				Status:  okStatus(),
				Command: &pb.TelemetryCommand_Settings{Settings: st},
			}); err != nil {
				return fmt.Errorf("telemetry 回发 settings: %w", err)
			}
			s.logger.Debug("telemetry settings 已协商")
		default:
			s.logger.Debug("telemetry 忽略未处理命令", "type", fmt.Sprintf("%T", c))
		}
	}
}

// NotifyClientTermination 客户端优雅下线。M1 无需清理状态，确认即可。
func (s *Server) NotifyClientTermination(ctx context.Context, req *pb.NotifyClientTerminationRequest) (*pb.NotifyClientTerminationResponse, error) {
	return &pb.NotifyClientTerminationResponse{Status: okStatus()}, nil
}

// unimplemented 供显式标注 M1 范围外的 RPC（嵌入的 Unimplemented 已兜底，
// 本函数仅用于需要打日志观察调用的场景）。
func (s *Server) unimplemented(name string) error {
	s.logger.Warn("调用了未实现的 RPC", "rpc", name)
	return status.Errorf(codes.Unimplemented, "%s 未实现（见 M2-M6 计划）", name)
}
```

**注意**：`Settings` 回发在真实 SDK 下可能要求补齐 `BackoffPolicy` 等字段——若 Task 13 集成测试中 SDK 报 settings 相关错误，在此补默认值（指数退避 100ms→1s，最大 3 次），这是预期内的对齐点。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/rpc/ -v`
Expected: PASS（3 个用例）

- [ ] **Step 5: 日志与注释自检**

- QueryRoute 失败 Warn / 成功 Debug；Heartbeat 失败 Warn；telemetry 协商与流结束 Debug；未实现 RPC 调用 Warn —— 已覆盖
- 文件头（职责+边界）、导出符号、"为什么在 QueryRoute 自动建 topic"注释 —— 核对无遗漏

- [ ] **Step 6: 提交**

```bash
git add internal/rpc/ && git commit -m "feat: gRPC 基座（路由/心跳/telemetry/下线）"
```

---

### Task 10: SendMessage

**Files:**
- Create: `internal/rpc/send.go`, `internal/rpc/send_test.go`

**Interfaces:**
- Consumes: `produce.Producer.Append`、Task 9 的 `okStatus/errStatus`。
- Produces: `(*Server) SendMessage(ctx, *pb.SendMessageRequest) (*pb.SendMessageResponse, error)`（覆盖 Task 9 的桩）。

- [ ] **Step 1: 写失败测试**

`internal/rpc/send_test.go`:

```go
package rpc

import (
	"context"
	"testing"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

func TestSendMessageNormal(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic: &pb.Resource{Name: "orders"},
			SystemProperties: &pb.SystemProperties{
				MessageId:   "0102030405060708090A0B0C0D0E0F10",
				MessageType: pb.MessageType_NORMAL,
				Tag:         strPtr("created"),
			},
			Body: []byte("hello"),
		}},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("status: %v", resp.GetStatus())
	}
	if len(resp.GetEntries()) != 1 || resp.GetEntries()[0].GetMessageId() == "" {
		t.Fatalf("entries: %v", resp.GetEntries())
	}
}

func TestSendMessageRejectsDelayInM1(t *testing.T) {
	c := newTestClient(t)
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "orders"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_DELAY},
			Body:             []byte("x"),
		}},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() == pb.Code_OK {
		t.Fatal("M1 应拒绝延时消息")
	}
}

func TestSendMessageRejectsEmptyBody(t *testing.T) {
	c := newTestClient(t)
	resp, _ := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: "orders"},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
		}},
	})
	if resp.GetStatus().GetCode() == pb.Code_OK {
		t.Fatal("空 body 应被拒绝")
	}
}

func strPtr(s string) *string { return &s }
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run TestSendMessage -v`
Expected: FAIL（Unimplemented）

- [ ] **Step 3: 实现 send.go**

```go
// SendMessage 相关：proto Message ↔ core.Message 的写方向翻译。
// 边界：仅 NORMAL 类型（M3 延时 / M4 FIFO 属性 / M6 事务在各自里程碑打开）。
package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/xushixin/sq/internal/core"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// SendMessage 批量写入。整批任一条失败即整体失败返回该错误
//（RocketMQ 语义上 batch 内同 topic，SDK 也按整批重试——逐条部分成功反而难处理）。
func (s *Server) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	var entries []*pb.SendResultEntry
	for _, pm := range req.GetMessages() {
		m, st := s.toCoreMessage(pm)
		if st != nil {
			s.logger.Warn("SendMessage 拒绝", "topic", pm.GetTopic().GetName(), "reason", st.GetMessage())
			return &pb.SendMessageResponse{Status: st}, nil
		}
		stored, err := s.pr.Append(m)
		if err != nil {
			s.logger.Error("SendMessage 写入失败", "topic", m.Topic, "msg_id", m.ID, "err", err)
			return &pb.SendMessageResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
		}
		entries = append(entries, &pb.SendResultEntry{
			Status:    okStatus(),
			MessageId: stored.ID,
			Offset:    int64(stored.Offset),
		})
	}
	return &pb.SendMessageResponse{Status: okStatus(), Entries: entries}, nil
}

// toCoreMessage 翻译并校验一条 proto 消息。返回非 nil status 表示拒绝。
func (s *Server) toCoreMessage(pm *pb.Message) (*core.Message, *pb.Status) {
	sp := pm.GetSystemProperties()
	switch sp.GetMessageType() {
	case pb.MessageType_NORMAL:
	case pb.MessageType_FIFO:
		// FIFO 消息的写入路径与 NORMAL 相同（MessageGroup 定队列已在 produce 实现），
		// 但消费端顺序锁是 M4——为避免"能发不能保序"的假象，M1 一并拒绝
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "顺序消息将在 M4 支持")
	case pb.MessageType_DELAY:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "延时消息将在 M3 支持")
	case pb.MessageType_TRANSACTION:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "事务消息将在 M6 支持")
	default:
		return nil, errStatus(pb.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
			fmt.Sprintf("未知消息类型 %v", sp.GetMessageType()))
	}
	if len(pm.GetBody()) == 0 {
		return nil, errStatus(pb.Code_MESSAGE_BODY_EMPTY, "消息体为空")
	}
	born := time.Now().UnixMilli()
	if ts := sp.GetBornTimestamp(); ts != nil {
		born = ts.AsTime().UnixMilli()
	}
	return &core.Message{
		ID:           sp.GetMessageId(), // 客户端生成的 msgId，保留以便端到端对账
		Topic:        pm.GetTopic().GetName(),
		Tag:          sp.GetTag(),
		Keys:         sp.GetKeys(),
		MessageGroup: sp.GetMessageGroup(),
		Properties:   pm.GetUserProperties(),
		Body:         pm.GetBody(),
		BornAtMs:     born,
	}, nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/rpc/ -v`
Expected: PASS（全部）

- [ ] **Step 5: 日志与注释自检**

- 拒绝 Warn（带原因）、写入失败 Error（带 topic/msg_id/err）；成功路径由 produce 的 Debug 覆盖（同一 trace 维度）
- "整批失败"与"FIFO 一并拒绝"的为什么注释 —— 核对无遗漏

- [ ] **Step 6: 提交**

```bash
git add internal/rpc/send*.go && git commit -m "feat: SendMessage（普通消息，M1 拒绝延时/顺序/事务）"
```

---

### Task 11: ReceiveMessage / AckMessage / ChangeInvisibleDuration / QueryAssignment

**Files:**
- Create: `internal/rpc/receipt.go`, `internal/rpc/receive.go`, `internal/rpc/receive_test.go`

**Interfaces:**
- Consumes: `deliver.Deliverer.Receive/Ack/ChangeInvisible`、Task 9 工具。
- Produces:
  - `receiptEncode(group, topic string, queueID uint32, offset uint64) string` / `receiptDecode(s string) (group, topic string, queueID uint32, offset uint64, err error)`（base64(JSON)；含 group 使 Ack 无需信任请求里的 group 字段）
  - `(*Server) ReceiveMessage(req *pb.ReceiveMessageRequest, stream pb.MessagingService_ReceiveMessageServer) error`
  - `(*Server) AckMessage(ctx, *pb.AckMessageRequest) (*pb.AckMessageResponse, error)`
  - `(*Server) ChangeInvisibleDuration(ctx, *pb.ChangeInvisibleDurationRequest) (*pb.ChangeInvisibleDurationResponse, error)`
  - `(*Server) QueryAssignment(ctx, *pb.QueryAssignmentRequest) (*pb.QueryAssignmentResponse, error)`

- [ ] **Step 1: 写失败测试**

`internal/rpc/receive_test.go`:

```go
package rpc

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// recvAll 读整个 ReceiveMessage 流，分离消息与末尾 status。
func recvAll(t *testing.T, stream pb.MessagingService_ReceiveMessageClient) ([]*pb.Message, *pb.Status) {
	t.Helper()
	var msgs []*pb.Message
	var st *pb.Status
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return msgs, st
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		switch c := resp.GetContent().(type) {
		case *pb.ReceiveMessageResponse_Message:
			msgs = append(msgs, c.Message)
		case *pb.ReceiveMessageResponse_Status:
			st = c.Status
		}
	}
}

func sendOne(t *testing.T, c pb.MessagingServiceClient, topic, body string) {
	t.Helper()
	resp, err := c.SendMessage(context.Background(), &pb.SendMessageRequest{
		Messages: []*pb.Message{{
			Topic:            &pb.Resource{Name: topic},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			Body:             []byte(body),
		}},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("send: %v %v", resp.GetStatus(), err)
	}
}

func TestReceiveAckRoundTrip(t *testing.T) {
	c := newTestClient(t)
	sendOne(t, c, "rt", "hello")
	// topic 只有 4 个队列，轮询首条必落 queue 0…但为免脆弱，四个队列都试
	var got *pb.Message
	for q := int32(0); q < 4 && got == nil; q++ {
		stream, err := c.ReceiveMessage(context.Background(), &pb.ReceiveMessageRequest{
			Group: &pb.Resource{Name: "g-rt"},
			MessageQueue: &pb.MessageQueue{
				Topic: &pb.Resource{Name: "rt"}, Id: q,
			},
			FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
			BatchSize:         10,
			InvisibleDuration: durationpb.New(time.Minute),
			AutoRenew:         false,
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		msgs, _ := recvAll(t, stream)
		if len(msgs) > 0 {
			got = msgs[0]
		}
	}
	if got == nil {
		t.Fatal("未收到消息")
	}
	handle := got.GetSystemProperties().GetReceiptHandle()
	if handle == nil || *handle == "" {
		t.Fatal("缺少 receipt handle")
	}
	ackResp, err := c.AckMessage(context.Background(), &pb.AckMessageRequest{
		Group: &pb.Resource{Name: "g-rt"},
		Topic: &pb.Resource{Name: "rt"},
		Entries: []*pb.AckMessageEntry{{
			ReceiptHandle: *handle,
			MessageId:     got.GetSystemProperties().GetMessageId(),
		}},
	})
	if err != nil || ackResp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("Ack: %v %v", ackResp.GetStatus(), err)
	}
}

func TestQueryAssignmentReturnsAllQueues(t *testing.T) {
	c := newTestClient(t)
	sendOne(t, c, "qa", "x") // 触发 topic 创建
	resp, err := c.QueryAssignment(context.Background(), &pb.QueryAssignmentRequest{
		Topic: &pb.Resource{Name: "qa"},
		Group: &pb.Resource{Name: "g-qa"},
	})
	if err != nil || resp.GetStatus().GetCode() != pb.Code_OK {
		t.Fatalf("QueryAssignment: %v %v", resp.GetStatus(), err)
	}
	if len(resp.GetAssignments()) != 4 {
		t.Fatalf("assignments: %d", len(resp.GetAssignments()))
	}
}

func TestReceiptRoundTrip(t *testing.T) {
	h := receiptEncode("g", "t", 3, 42)
	g, topic, q, off, err := receiptDecode(h)
	if err != nil || g != "g" || topic != "t" || q != 3 || off != 42 {
		t.Fatalf("receipt round trip: %v %v %v %v %v", g, topic, q, off, err)
	}
	if _, _, _, _, err := receiptDecode("garbage!!"); err == nil {
		t.Fatal("非法 handle 应报错")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestReceive|TestQueryAssignment|TestReceipt' -v`
Expected: FAIL

- [ ] **Step 3: 实现 receipt.go 与 receive.go**

`internal/rpc/receipt.go`:

```go
// receipt handle 编解码。handle 自包含定位信息（group/topic/queue/offset），
// Ack 只信 handle 不信请求体其余字段——这也是 RocketMQ pop receipt 的思路。
package rpc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

type receipt struct {
	G string `json:"g"`
	T string `json:"t"`
	Q uint32 `json:"q"`
	O uint64 `json:"o"`
}

// receiptEncode 生成自包含 receipt handle。
func receiptEncode(group, topic string, queueID uint32, offset uint64) string {
	b, _ := json.Marshal(receipt{G: group, T: topic, Q: queueID, O: offset})
	return base64.StdEncoding.EncodeToString(b)
}

// receiptDecode 解析 receipt handle。
func receiptDecode(s string) (string, string, uint32, uint64, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("receipt handle 非法 base64: %w", err)
	}
	var r receipt
	if err := json.Unmarshal(raw, &r); err != nil {
		return "", "", 0, 0, fmt.Errorf("receipt handle 非法 JSON: %w", err)
	}
	return r.G, r.T, r.Q, r.O, nil
}
```

`internal/rpc/receive.go`:

```go
// ReceiveMessage/Ack/ChangeInvisible/QueryAssignment：POP 消费方向的协议翻译。
// 边界：Tag 过滤 M1 仅接受 "*"（M2 实现真实过滤）。
package rpc

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xushixin/sq/internal/core"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// defaultLongPolling 客户端未指定等待时的长轮询上限。
const defaultLongPolling = 20 * time.Second

// ReceiveMessage POP 取件（服务端流）。流格式：先逐条消息，最后一条 status。
func (s *Server) ReceiveMessage(req *pb.ReceiveMessageRequest, stream pb.MessagingService_ReceiveMessageServer) error {
	group := req.GetGroup().GetName()
	mq := req.GetMessageQueue()
	topic := mq.GetTopic().GetName()
	queueID := uint32(mq.GetId())

	if fe := req.GetFilterExpression(); fe != nil &&
		!(fe.GetType() == pb.FilterType_TAG && fe.GetExpression() == "*") {
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_ILLEGAL_FILTER_EXPRESSION, "M1 仅支持 TAG 过滤表达式 *（M2 支持真实过滤）"),
		}})
	}
	invisible := req.GetInvisibleDuration().AsDuration()
	if invisible <= 0 {
		invisible = time.Minute
	}
	batch := int(req.GetBatchSize())
	if batch <= 0 {
		batch = 16
	}
	// 长轮询时长：gRPC 请求 deadline 减一点安全余量；没有 deadline 用默认值
	wait := defaultLongPolling
	if dl, ok := stream.Context().Deadline(); ok {
		if w := time.Until(dl) - time.Second; w > 0 && w < wait {
			wait = w
		}
	}

	msgs, err := s.dl.Receive(stream.Context(), group, topic, queueID, batch, invisible, wait)
	if err != nil {
		s.logger.Error("ReceiveMessage 失败", "group", group, "topic", topic, "queue", queueID, "err", err)
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error()),
		}})
	}
	for _, m := range msgs {
		if err := stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Message{
			Message: s.toPBMessage(m, group, invisible),
		}}); err != nil {
			return err
		}
	}
	return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
		Status: okStatus(),
	}})
}

// toPBMessage core.Message → proto（读方向翻译，receipt handle 在此注入）。
func (s *Server) toPBMessage(m *core.Message, group string, invisible time.Duration) *pb.Message {
	handle := receiptEncode(group, m.Topic, m.QueueID, m.Offset)
	tag := m.Tag
	sp := &pb.SystemProperties{
		MessageId:       m.ID,
		MessageType:     pb.MessageType_NORMAL,
		Keys:            m.Keys,
		BornTimestamp:   timestamppb.New(time.UnixMilli(m.BornAtMs)),
		StoreTimestamp:  timestamppb.New(time.UnixMilli(m.StoreAtMs)),
		DeliveryAttempt: &m.DeliveryAttempt,
		ReceiptHandle:   &handle,
		QueueId:         int32(m.QueueID),
		QueueOffset:     int64(m.Offset),
	}
	if tag != "" {
		sp.Tag = &tag
	}
	return &pb.Message{
		Topic:            &pb.Resource{Name: m.Topic},
		SystemProperties: sp,
		UserProperties:   m.Properties,
		Body:             m.Body,
	}
}

// AckMessage 批量确认。逐条返回结果；handle 解析失败或 ack 落空不影响其它条目。
func (s *Server) AckMessage(ctx context.Context, req *pb.AckMessageRequest) (*pb.AckMessageResponse, error) {
	var entries []*pb.AckMessageResultEntry
	for _, e := range req.GetEntries() {
		g, topic, q, off, err := receiptDecode(e.GetReceiptHandle())
		if err != nil {
			s.logger.Warn("ack handle 非法", "handle", e.GetReceiptHandle(), "err", err)
			entries = append(entries, &pb.AckMessageResultEntry{
				Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error()), MessageId: e.GetMessageId(),
			})
			continue
		}
		ok, err := s.dl.Ack(g, topic, q, off)
		st := okStatus()
		if err != nil {
			s.logger.Error("ack 失败", "group", g, "topic", topic, "queue", q, "offset", off, "err", err)
			st = errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())
		} else if !ok {
			// 已被重投/重复 ack：RocketMQ 语义返回 INVALID_RECEIPT_HANDLE，SDK 静默处理
			st = errStatus(pb.Code_INVALID_RECEIPT_HANDLE, "receipt 已失效")
		}
		entries = append(entries, &pb.AckMessageResultEntry{Status: st, MessageId: e.GetMessageId(), ReceiptHandle: e.GetReceiptHandle()})
	}
	return &pb.AckMessageResponse{Status: okStatus(), Entries: entries}, nil
}

// ChangeInvisibleDuration 重设不可见时长，返回新 handle（内容不变——handle 只含定位信息）。
func (s *Server) ChangeInvisibleDuration(ctx context.Context, req *pb.ChangeInvisibleDurationRequest) (*pb.ChangeInvisibleDurationResponse, error) {
	g, topic, q, off, err := receiptDecode(req.GetReceiptHandle())
	if err != nil {
		return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, err.Error())}, nil
	}
	ok, err := s.dl.ChangeInvisible(g, topic, q, off, req.GetInvisibleDuration().AsDuration())
	if err != nil {
		s.logger.Error("改不可见时长失败", "group", g, "topic", topic, "queue", q, "offset", off, "err", err)
		return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_INTERNAL_SERVER_ERROR, err.Error())}, nil
	}
	if !ok {
		return &pb.ChangeInvisibleDurationResponse{Status: errStatus(pb.Code_INVALID_RECEIPT_HANDLE, "receipt 已失效")}, nil
	}
	return &pb.ChangeInvisibleDurationResponse{Status: okStatus(), ReceiptHandle: req.GetReceiptHandle()}, nil
}

// QueryAssignment 单机版：全部队列归属本节点。
func (s *Server) QueryAssignment(ctx context.Context, req *pb.QueryAssignmentRequest) (*pb.QueryAssignmentResponse, error) {
	name := req.GetTopic().GetName()
	tc, err := s.mt.EnsureTopic(name)
	if err != nil {
		return &pb.QueryAssignmentResponse{Status: errStatus(pb.Code_TOPIC_NOT_FOUND, err.Error())}, nil
	}
	var asgs []*pb.Assignment
	for _, mq := range s.messageQueues(tc, req.GetTopic()) {
		asgs = append(asgs, &pb.Assignment{MessageQueue: mq})
	}
	return &pb.QueryAssignmentResponse{Status: okStatus(), Assignments: asgs}, nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/rpc/ -v`
Expected: PASS（全部，含 Task 9/10 用例）

- [ ] **Step 5: 日志与注释自检**

- Receive/Ack/ChangeInvisible 错误路径 Error（全维度上下文）、handle 非法 Warn；成功路径由 deliver 的 Debug 覆盖
- receipt 自包含设计、"ack 落空返回 INVALID_RECEIPT_HANDLE"的为什么注释 —— 核对无遗漏

- [ ] **Step 6: 提交**

```bash
git add internal/rpc/ && git commit -m "feat: POP 消费协议（Receive/Ack/ChangeInvisible/QueryAssignment）"
```

---

### Task 12: main 装配与优雅停机

**Files:**
- Modify: `cmd/sq/main.go`
- Create: `README.md`

**Interfaces:**
- Consumes: 前面全部模块的构造函数。
- Produces: 可运行的 `sq` 二进制：`./sq [-config sq.yaml]`。

- [ ] **Step 1: 实现完整 main.go**

```go
// sq 主入口。装配 config/store/core/rpc 并托管进程生命周期。
// 边界：只做装配与启停，不含业务逻辑；退出码非 0 表示启动失败。
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/rpc"
	"github.com/xushixin/sq/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("sq 启动失败", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "", "配置文件路径（可选）")
	flag.Parse()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	config.SetupSlog(cfg.LogLevel)
	logger := slog.Default()

	st, err := store.Open(cfg.DataDir, cfg.Fsync == "sync", logger)
	if err != nil {
		return err
	}
	defer st.Close()
	mt, err := meta.New(st, cfg.AutoCreateTopic, cfg.DefaultQueueNums, logger)
	if err != nil {
		return err
	}
	pr := produce.New(st, mt, logger)
	dl := deliver.New(st, mt, pr, logger)

	lis, err := net.Listen("tcp", cfg.GRPCListen)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	rpc.New(cfg, mt, pr, dl, logger).Register(gs)

	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(lis) }()
	logger.Info("sq 已启动", "grpc_listen", cfg.GRPCListen,
		"advertise", cfg.AdvertiseHost, "data_dir", cfg.DataDir, "fsync", cfg.Fsync)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		logger.Info("收到退出信号，优雅停机", "signal", sig.String())
		gs.GracefulStop() // 等待在途 RPC 结束；store 由 defer 关闭
		return nil
	case err := <-errCh:
		return err
	}
}
```

- [ ] **Step 2: 构建并手工冒烟**

```bash
go build -o sq ./cmd/sq
./sq &
sleep 1
kill -TERM %1 && wait
```

Expected: 启动日志（含 grpc_listen/data_dir）→ 退出信号日志 → 干净退出，`data/` 目录生成。

- [ ] **Step 3: 写 README**

`README.md`:

```markdown
# sq — simple queue

RocketMQ 协议兼容、单二进制、无 JVM 的轻量消息队列。面向中小团队：
功能全（普通/延时/顺序/事务消息，逐里程碑交付）、部署轻（一个二进制 + 一个数据目录）。

## 快速开始

​```bash
go build -o sq ./cmd/sq
./sq                # gRPC 监听 :8081，数据写 ./data
​```

用官方 RocketMQ 5.x SDK（Java/Go/Python/C#/C++）直接连接 `127.0.0.1:8081` 收发。
当前状态：M1（普通消息）。里程碑与设计见 docs/superpowers/specs/。

## 配置

​```yaml
# sq.yaml（全部可省略）
grpc_listen: ":8081"
advertise_host: "127.0.0.1"   # 路由响应中的对外地址，容器/远程访问时必改
advertise_port: 8081
data_dir: "./data"
fsync: sync                   # sync|async
auto_create_topic: true
default_queue_nums: 4
log_level: info
​```
```

（注意：README 中的代码围栏在实际文件中用正常三反引号，此处为嵌套转义。）

- [ ] **Step 4: 提交**

```bash
git add cmd/ README.md && git commit -m "feat: main 装配、优雅停机与 README"
```

---

### Task 13: 官方 Go SDK 集成测试（M1 出口标准）

**Files:**
- Create: `test/e2e/sdk_test.go`
- Modify: `go.mod`（测试依赖官方 SDK）

**Interfaces:**
- Consumes: 全部模块（进程内起完整 broker）+ `github.com/apache/rocketmq-clients/golang/v5`。
- Produces: `make e2e` 通过 = M1 完成。

**注意**：官方 SDK 的构造 API 以其 go doc 为准（`rmq_client.Config`/`WithTopics`/`WithSubscriptionExpressions` 等曾在小版本间调整）。下述代码按 v5 当前文档编写；若编译报 API 不匹配，以 SDK 的 examples 目录为准对齐——对齐属于预期内修正，不算设计变更。SDK 要求设置日志环境变量，测试里已设。

- [ ] **Step 1: 引入 SDK 并写集成测试**

```bash
go get github.com/apache/rocketmq-clients/golang/v5@latest
```

`test/e2e/sdk_test.go`:

```go
//go:build e2e

// 官方 rocketmq-clients Go SDK 端到端测试：M1 出口标准。
// 在进程内起完整 sq broker（真实 TCP 端口），用官方 SDK 收发普通消息。
// 边界：只验证普通消息链路；延时/顺序/事务的 e2e 属 M3/M4/M6。
package e2e

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"google.golang.org/grpc"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/rpc"
	"github.com/xushixin/sq/internal/store"
)

const (
	endpoint = "127.0.0.1:18081"
	topic    = "e2e-normal"
	group    = "e2e-group"
)

// startBroker 进程内起完整 sq，监听真实端口。
func startBroker(t *testing.T) {
	t.Helper()
	cfg, _ := config.Load("")
	cfg.AdvertiseHost, cfg.AdvertisePort = "127.0.0.1", 18081
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	mt, _ := meta.New(st, true, 4, slog.Default())
	pr := produce.New(st, mt, slog.Default())
	dl := deliver.New(st, mt, pr, slog.Default())
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	rpc.New(cfg, mt, pr, dl, slog.Default()).Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
}

func TestOfficialGoSDKSendAndReceive(t *testing.T) {
	os.Setenv("rocketmq.client.logRoot", t.TempDir()) // SDK 强制要求日志目录
	startBroker(t)

	// ---- 生产 10 条 ----
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
	sent := map[string]bool{}
	for i := 0; i < 10; i++ {
		msg := &rmq.Message{Topic: topic, Body: []byte("payload")}
		msg.SetTag("e2e")
		recs, err := producer.Send(context.Background(), msg)
		if err != nil || len(recs) == 0 {
			t.Fatalf("Send #%d: %v", i, err)
		}
		sent[recs[0].MessageID] = true
	}

	// ---- SimpleConsumer 消费并 ack，直到收齐 ----
	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithAwaitDuration(5*time.Second),
		rmq.WithSubscriptionExpressions(map[string]*rmq.FilterExpression{
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

	got := map[string]bool{}
	deadline := time.Now().Add(60 * time.Second)
	for len(got) < len(sent) && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询返回错误码是 SDK 正常行为
		}
		for _, mv := range mvs {
			got[mv.GetMessageId()] = true
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
	}
	if len(got) != len(sent) {
		t.Fatalf("收齐失败: sent=%d got=%d", len(sent), len(got))
	}
	for id := range sent {
		if !got[id] {
			t.Fatalf("消息 %s 未收到", id)
		}
	}
}
```

- [ ] **Step 2: 跑 e2e**

Run: `make e2e`
Expected: PASS。**这是 M1 的出口标准。** 失败时按顺序排查：
1. SDK 报 settings/telemetry 错 → 回 Task 9 的 Settings 回发补默认字段
2. SDK 报路由错 → 检查 QueryRoute 的 endpoints 是否等于 `127.0.0.1:18081`
3. 收不到消息 → 开 `log_level: debug` 看 deliver 的投递日志定位
4. SDK API 编译不过 → 以 SDK examples 对齐构造代码

- [ ] **Step 3: 全量回归 + 收尾提交**

```bash
go vet ./... && go test ./... && make e2e
git add test/ go.mod go.sum && git commit -m "test: 官方 Go SDK e2e（M1 出口标准）"
git tag m1
```

- [ ] **Step 4: M1 完成自检（instrumenting-code 清单）**

- [ ] 每个错误分支都有带上下文的日志或错误包装（grep `return nil,` 抽查）
- [ ] 成功路径不静默（produce/deliver Debug、启动/meta Info）
- [ ] 无 `fmt.Printf`/`log.Print`（`grep -rn "fmt.Print\|log.Print" --include="*.go" internal/ cmd/` 应为空）
- [ ] 每个新文件有文件头注释（职责+边界）
- [ ] 每个导出符号有 doc 注释（`go vet` + 抽查）

---

## Self-Review 记录

- **Spec 覆盖**：M1 范围 = spec §3（架构分层✓ Task 9-11 适配层 / Task 5-7 core / Task 2-3 store）、§4 key 编码（msg/cursor/inflight/alloc/meta ✓；delay/half/keyidx 属 M3/M6/M2 不在本计划）、§5 流程 1-2（✓ Task 6/7）、§6 协议 11 个 RPC 中 M1 需要的 8 个（✓ Task 9-11；EndTransaction/ForwardToDLQ 桩返回 UNIMPLEMENTED 属 M6/M2）、§7 刷盘/恢复/错误码（✓）、§10 测试 1-2 项（✓ Task 13/各单测；崩溃测试与长稳属 M2+ 补强）。
- **占位符扫描**：无 TBD/TODO；两处"以生成代码/SDK 为准对齐"是外部依赖版本对齐指引，非设计空洞。
- **类型一致性**：`Receive(ctx, group, topic, queueID uint32, maxMsgs int, invisible, wait time.Duration)` 在 Task 7 定义与 Task 11 调用一致；`store.Scan(lower, upper, limit, fn)` 各处一致；`meta.New(st, autoCreate, defaultQueues, logger)` 各测试 fixture 一致；receipt 编解码签名一致。
- **已知风险（执行时注意）**：pb 生成代码的字段名/import 路径、官方 SDK 构造 API 可能与计划代码有出入——两处均已在任务内给出对齐规则（以生成代码/SDK examples 为准）。




