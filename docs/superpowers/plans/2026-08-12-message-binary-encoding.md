# 消息二进制编码（去 base64）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让消息落盘/转发编码从「整条 JSON、Body 走 base64（+33% 膨胀）」换成 v1 二进制混合格式（Body 原始字节直落），消掉乘遍写路径六环的体积税与每消息每节点一次的 base64/JSON CPU 税。

**Architecture:** 在 `internal/core` 新增 `encoding.go` 承载消息编解码：**写方向**按装配期档位二选一（默认 `json`，与今天逐字节相同；`binary` 走 v1 格式），**读方向永远双格式**——按 value 首字节判别（`'{'`=历史 JSON，`0x01`=v1），旧数据零迁移。v1 布局为 `[1B 版本][4B BE 元数据长度][元数据 JSON][Body 原始字节]`，元数据仍是 JSON 以继承 `Message` 现有 tag 的 omitempty 兼容性质。15 处 `EncodeMessage`/`DecodeMessage` 调用点一行不动。

**Tech Stack:** Go 1.26.1，标准库 `encoding/json` + `encoding/binary` + `sync/atomic`。不引入任何新依赖。

**Spec:** `docs/superpowers/specs/2026-08-12-message-binary-encoding-design.md`

## Global Constraints

- **解码方向永远双格式，永久保留，无淘汰计划。** 盘上新旧混存是常态（升级不迁移），且 `OpForwardAppend` 把编码字节直接跨节点转发，混版集群里两种格式会同时出现在网络上。任何「只认新格式」的简化都会让滚动升级窗口内的写入失败。
- **`json` 档编码输出必须逐字节不变。** 锚点：`TestEncodeJSONFormatUnchanged` 的 golden 字节（Task 1 Step 1 给出实测值）。
- **调用点零改动。** `internal/core/produce/produce.go`、`deliver/deliver.go`、`query/query.go`、`retention/retention.go`、`txn/txn.go`、`delay/delay.go`、`internal/admin/messages.go`、`cmd/sq/main.go`（ForwardAppend handler）共 15 处调用一行不动——编码集中在唯一出入口正是为了此刻。任一调用点需要修改，说明抽象破了，停下来说明原因。
- **v1 解码的 Body 必须拷贝，不能子切片引用输入。** 历史 JSON 路径经 base64 解码天然产生新内存，`deliver`/`query` 的 Pebble 迭代回调依赖这个性质；Pebble 的 value 只在回调内有效，子切片引用是 use-after-free 级别的数据竞争。
- **元数据段的 Body 遮蔽字段 tag 必须是 `json:"body,omitempty"`，绝不能是 `json:"-"`。** 实测：`json:"-"` 版本输出 `{"id":...,"body":"AP8=",...}`——`encoding/json` 在扫描阶段对 `-` 字段直接 `continue`，它不进入同名字段的深度竞争，内嵌 `*Message` 里 promoted 的 `Body` 反而成了唯一 body 字段被编进去，整个去 base64 的目的**静默落空**。回归锚：`TestV1MetaSegmentHasNoBodyKey`。
- **`core` 包不得 import 任何 proto/pb 包**（既有边界，见 `types.go` 包头）。这也是不选「全量 protobuf」方案的硬约束之一。
- **默认档位 `json`。** `binary` 必须显式开启，且开启是「升级已完成」的确认动作，不是升级的一部分（见 README 两步纪律）。
- **观测性适配（instrumenting-code）：`core` 是无 logger 的叶子包，且编解码是每消息热路径——此处禁止逐条日志（会淹没日志系统）。** 本计划对该 skill 的落实方式固定为三条，缺一即为实现缺陷：
  1. **每个错误分支的 error 必须自带定位上下文**（首字节值、长度、越界值），由上层调用点负责打印——错误信息就是这一层的「日志」；
  2. **装配期 Info 日志**（`main` 打印生效档位）+ 两条「sq 已启动」日志加 `message_encoding` 字段：这是判断「盘上为什么出现二进制数据」的唯一线索；
  3. **禁止 `fmt.Printf`/`print`** 作为日志机制（全局规范）。
- **注释纪律：** 新文件写职责+边界文件头；导出函数写参数/返回/注意；`json:"body,omitempty"` 遮蔽、Body 拷贝、首字节判别、两步升级纪律这四处「为什么」必须有中文注释——它们全是「代码本身看不出、改错了还静默」的地方。

---

## File Structure

| 文件 | 动作 | 职责 |
|---|---|---|
| `internal/core/encoding.go` | 新建 | 消息存储编码：档位开关、v1 编解码、双格式判别 |
| `internal/core/encoding_test.go` | 新建 | 格式回归：遮蔽、round-trip、Body 拷贝、损坏输入、双格式混读、json golden |
| `internal/core/types.go` | 修改 | 移出消息编解码（`EncodeMessage`/`DecodeMessage`），更新包头职责；`Message` 模型与 `InflightState` 编解码留在原处 |
| `internal/config/config.go` | 修改 | 新增 `message_encoding` 配置项、默认值与白名单校验 |
| `internal/config/config_test.go` | 修改 | 默认值与非法档位的启动期拒绝 |
| `cmd/sq/main.go` | 修改 | 装配期调用 `core.SetEncoding`、档位 Info 日志、启动日志加字段 |
| `README.md` / `sq.example.yaml` | 修改 | 配置项说明 + 「升级注意」两步纪律与回滚边界 |
| `test/e2e/sdk_test.go` | 修改 | 配置字面量补 `MessageEncoding`（否则新校验打断全部 e2e）；新增 `rewriteBrokerConfig` helper（跨代改配置） |
| `test/e2e/sdk_cluster_test.go` | 修改 | 同上，`clusterNodeConfig` 字面量补 `MessageEncoding` |
| `test/e2e/sdk_encoding_test.go` | 新建 | 跨档位互读 e2e：json 档写入 → 切 binary 重启 → 再写 → 全部消费 |

**为什么把编解码从 `types.go` 拆出来**：`types.go` 是模型文件，而编解码即将从 16 行长到约 130 行（两套格式 + 版本判别 + 遮蔽类型）。留在原处会让模型文件的多数内容变成编解码。`InflightState` 的编解码不动——它体积个位数字节级、不在本次改动范围，跟着自己的类型走。

**既有测试的归属**：`types_test.go` 里的 `TestMessageRoundTrip` 与 `TestDecodeLegacyMessageWithoutPassthroughFields` **不迁移**。它们测的是包的公开 API（仍在 `package core` 内），迁移只是无收益的 churn 和丢测试的风险。新格式的用例进 `encoding_test.go`。

---

### Task 1: v1 格式原语、双格式解码与档位开关（core 包）

本任务完成后进程行为**零变化**（默认 `json` 档），但已具备解开 v1 数据的能力——这正是 spec §4.2 滚动升级第一步要发布的全部内容。

**Files:**
- Create: `internal/core/encoding.go`
- Create: `internal/core/encoding_test.go`
- Modify: `internal/core/types.go`（删除 `EncodeMessage`/`DecodeMessage`，更新包头注释）

**Interfaces:**
- Consumes: `core.Message`（既有类型，`internal/core/types.go:69`）
- Produces（Task 2 依赖）：
  - `const EncodingJSON = "json"` / `const EncodingBinary = "binary"`
  - `func SetEncoding(enc string) error` — 装配期一次性设置写方向档位；未知 token 返回 error
  - `func EncodeMessage(m *Message) ([]byte, error)` — 签名不变
  - `func DecodeMessage(b []byte) (*Message, error)` — 签名不变

- [ ] **Step 1: 写失败的测试**

创建 `internal/core/encoding_test.go`：

```go
// encoding_test.go 验证消息存储编码的两套格式与它们的互操作。
//
// 职责：v1 布局正确性、元数据段不含 Body、Body 拷贝语义、损坏输入的
//       拒绝、双格式混读、json 档字节不变性。
// 边界：不测配置加载与装配（config/main 的事）；不测调用点行为。
package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// goldenMsg 一条字段齐全、取值稳定的消息，供 golden 与 round-trip 共用。
// 不用 NewMessageID()：golden 用例要求输出可复现，随机 ID 会让它每次都变。
func goldenMsg() *Message {
	return &Message{
		ID: "0123456789ABCDEF0123456789ABCDEF", Topic: "orders", QueueID: 3, Offset: 42,
		Tag: "created", Keys: []string{"o-1"},
		Properties: map[string]string{"k": "v"},
		Body:       []byte{0x00, 0xFF, 0x7F},
		BornAtMs:   1000, StoreAtMs: 2000,
	}
}

// TestEncodeJSONFormatUnchanged 锁死「json 档输出逐字节不变」。
//
// 这条是整个改动的兼容性地基：默认档位必须与升级前产出完全相同的字节，
// 否则「升级后不开 binary 就零风险」这个前提不成立。golden 串取自改动
// 前的实测输出，不是照着结构体默写的——默写会跟着实现一起错。
func TestEncodeJSONFormatUnchanged(t *testing.T) {
	const golden = `{"id":"0123456789ABCDEF0123456789ABCDEF","topic":"orders",` +
		`"queue_id":3,"offset":42,"tag":"created","keys":["o-1"],` +
		`"properties":{"k":"v"},"body":"AP9/","born_at_ms":1000,"store_at_ms":2000}`
	b, err := EncodeMessage(goldenMsg())
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	if string(b) != golden {
		t.Fatalf("json 档输出已变化（破坏升级兼容性）:\n实得 %s\n应为 %s", b, golden)
	}
}

// TestV1MetaSegmentHasNoBodyKey 是本改动最重要的一条回归，锁死遮蔽字段的 tag。
//
// 背景：元数据段用内嵌 *Message + 同名字段遮蔽的方式排除 Body。直觉写法
// `json:"-"` 是**错的且静默**——encoding/json 扫描字段时对 tag 为 "-" 的
// 字段直接 continue，它不参与同名字段的深度竞争，于是 promoted 的
// Message.Body（深度 1）成了唯一的 body 字段，照样以 base64 编进元数据。
// 实测 `json:"-"` 版本输出 {"id":...,"body":"AP8=",...}：整个去 base64 的
// 目的落空，而所有 round-trip 用例照样全绿。只有直接断言「元数据段里没有
// body 键」才拦得住。
func TestV1MetaSegmentHasNoBodyKey(t *testing.T) {
	m := goldenMsg()
	b, err := encodeMessageV1(m)
	if err != nil {
		t.Fatalf("encodeMessageV1: %v", err)
	}
	metaLen := binary.BigEndian.Uint32(b[1:msgV1HeaderLen])
	meta := b[msgV1HeaderLen : msgV1HeaderLen+int(metaLen)]

	var kv map[string]json.RawMessage
	if err := json.Unmarshal(meta, &kv); err != nil {
		t.Fatalf("元数据段不是合法 JSON: %v (%s)", err, meta)
	}
	if _, ok := kv["body"]; ok {
		t.Fatalf("元数据段仍含 body 键——遮蔽字段 tag 写错（不可用 json:\"-\"）: %s", meta)
	}
	// 双保险：即便将来键名改了，base64 串本身也不该出现在元数据里
	if bytes.Contains(meta, []byte("AP9/")) {
		t.Fatalf("元数据段含 Body 的 base64——去 base64 未生效: %s", meta)
	}
	// Body 必须原样落在末尾
	if got := b[msgV1HeaderLen+int(metaLen):]; !bytes.Equal(got, m.Body) {
		t.Fatalf("Body 段 = %x, 应为 %x", got, m.Body)
	}
}

// TestV1Layout 锁死定长头的三个字段：版本字节、大端元数据长度、总长关系。
func TestV1Layout(t *testing.T) {
	m := goldenMsg()
	b, err := encodeMessageV1(m)
	if err != nil {
		t.Fatalf("encodeMessageV1: %v", err)
	}
	if b[0] != msgVersionV1 {
		t.Fatalf("版本字节 = 0x%02X, 应为 0x%02X", b[0], msgVersionV1)
	}
	metaLen := int(binary.BigEndian.Uint32(b[1:msgV1HeaderLen]))
	if want := msgV1HeaderLen + metaLen + len(m.Body); len(b) != want {
		t.Fatalf("总长 = %d, 应为 %d（头 %d + 元数据 %d + Body %d）",
			len(b), want, msgV1HeaderLen, metaLen, len(m.Body))
	}
	// 首字节必须与历史 JSON 的 '{' 不同，否则判别失效
	if b[0] == jsonObjectStart {
		t.Fatal("v1 版本字节与 JSON 起始字节相同，格式判别失效")
	}
}

// TestV1RoundTrip v1 编解码往返：全字段消息深度相等。
func TestV1RoundTrip(t *testing.T) {
	m := goldenMsg()
	m.MessageGroup = "g1"
	m.BodyEncoding = BodyEncodingGzip
	m.BodyDigest = &BodyDigest{Type: DigestTypeCRC32, Checksum: "3610a686"}
	m.BornHost = "10.0.0.7:54321"
	m.DeliverAtMs = 1700000000000
	m.TraceContext = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	b, err := encodeMessageV1(m)
	if err != nil {
		t.Fatalf("encodeMessageV1: %v", err)
	}
	got, err := DecodeMessage(b)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if !reflect.DeepEqual(m, got) {
		t.Fatalf("round trip 不一致:\n%+v\n%+v", m, got)
	}
}

// TestV1EmptyBody 空 Body 的边界：元数据段之后没有任何字节，解码得零长 Body。
//
// 断言用 len()==0 而不是 DeepEqual：v1 路径的空 Body 解出 nil，历史 JSON
// 路径（"body":""）解出非 nil 的零长切片。两者语义等价（协议面都编码成
// 空字节），这处差异是已知且无害的，不值得为对齐它增加代码。
func TestV1EmptyBody(t *testing.T) {
	m := goldenMsg()
	m.Body = nil
	b, err := encodeMessageV1(m)
	if err != nil {
		t.Fatalf("encodeMessageV1: %v", err)
	}
	got, err := DecodeMessage(b)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if len(got.Body) != 0 {
		t.Fatalf("空 Body 解出 %d 字节", len(got.Body))
	}
	if got.ID != m.ID || got.Topic != m.Topic {
		t.Fatalf("元数据丢失: %+v", got)
	}
}

// TestV1BodyIsCopied 锁死「解码结果的生命周期独立于输入字节」。
//
// 为什么必须钉：调用方（deliver/query 的 Pebble 迭代回调）默认解码结果
// 与底层字节无关——历史 JSON 路径经 base64 解码天然产生新内存，这个性质
// 是被依赖的。v1 的 Body 段就在输入切片里，若图省事直接子切片引用，
// Pebble 迭代器移动后那段内存会被复用，表现为「消息内容随机变成别人的」，
// 是最难排查的一类损坏。
func TestV1BodyIsCopied(t *testing.T) {
	m := goldenMsg()
	m.Body = []byte("original-payload")
	b, err := encodeMessageV1(m)
	if err != nil {
		t.Fatalf("encodeMessageV1: %v", err)
	}
	got, err := DecodeMessage(b)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	// 把输入整段涂掉，模拟 Pebble 迭代器复用底层内存
	for i := range b {
		b[i] = 0xAA
	}
	if string(got.Body) != "original-payload" {
		t.Fatalf("Body 引用了输入字节（涂改后变成 %q）——必须拷贝", got.Body)
	}
}

// TestDecodeBothFormats 双格式混读：同一批 value 里新旧格式逐条都能解开。
// 这是升级窗口与「盘上永久混存」的核心保证。
func TestDecodeBothFormats(t *testing.T) {
	m := goldenMsg()
	legacy, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	binv1, err := encodeMessageV1(m)
	if err != nil {
		t.Fatalf("encodeMessageV1: %v", err)
	}
	for i, raw := range [][]byte{legacy, binv1, legacy, binv1} {
		got, err := DecodeMessage(raw)
		if err != nil {
			t.Fatalf("第 %d 条解码失败: %v", i, err)
		}
		if got.ID != m.ID || !bytes.Equal(got.Body, m.Body) {
			t.Fatalf("第 %d 条内容不符: %+v", i, got)
		}
	}
}

// TestDecodeRejectsCorrupt 损坏输入必须报错且不 panic，错误信息带定位上下文
// （首字节/长度/越界值）——core 无 logger，错误信息就是这一层唯一的线索。
func TestDecodeRejectsCorrupt(t *testing.T) {
	v1 := func(metaLen uint32, tail ...byte) []byte {
		b := make([]byte, msgV1HeaderLen)
		b[0] = msgVersionV1
		binary.BigEndian.PutUint32(b[1:], metaLen)
		return append(b, tail...)
	}
	cases := []struct {
		name string
		in   []byte
		want string // 错误信息必须包含的片段
	}{
		{"空输入", []byte{}, "空字节"},
		{"未知首字节", []byte{0x7E, 0x00}, "0x7E"},
		{"v1 头截断", []byte{msgVersionV1, 0x00}, "不足定长头"},
		{"元数据长度越界", v1(9999, '{', '}'), "越界"},
		{"元数据非法 JSON", v1(3, 'n', 'o', 'p'), "元数据"},
		{"JSON 体损坏", []byte(`{"id":`), "解码消息"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeMessage(tc.in)
			if err == nil {
				t.Fatalf("应报错却成功: %+v", got)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("错误信息缺少定位上下文 %q: %v", tc.want, err)
			}
		})
	}
}

// TestSetEncoding 档位开关：切换只影响写方向，未知 token 报错且不改状态。
func TestSetEncoding(t *testing.T) {
	t.Cleanup(func() { _ = SetEncoding(EncodingJSON) }) // 包级状态，用完还原

	if err := SetEncoding(EncodingBinary); err != nil {
		t.Fatalf("SetEncoding(binary): %v", err)
	}
	b, err := EncodeMessage(goldenMsg())
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	if b[0] != msgVersionV1 {
		t.Fatalf("binary 档产出首字节 0x%02X, 应为 0x%02X", b[0], msgVersionV1)
	}

	if err := SetEncoding("protobuf"); err == nil {
		t.Fatal("未知档位应报错")
	}
	// 报错后档位不得被改坏
	b2, err := EncodeMessage(goldenMsg())
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	if !bytes.Equal(b, b2) {
		t.Fatal("非法档位调用改变了生效档位")
	}

	if err := SetEncoding(EncodingJSON); err != nil {
		t.Fatalf("SetEncoding(json): %v", err)
	}
	b3, err := EncodeMessage(goldenMsg())
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	if b3[0] != jsonObjectStart {
		t.Fatalf("json 档产出首字节 0x%02X, 应为 '{'", b3[0])
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/core/ -run 'TestV1|TestSetEncoding|TestDecodeBoth|TestDecodeRejects|TestEncodeJSONFormat' -v`
Expected: 编译失败 —— `undefined: encodeMessageV1`、`undefined: msgV1HeaderLen`、`undefined: msgVersionV1`、`undefined: jsonObjectStart`、`undefined: SetEncoding`、`undefined: EncodingBinary`

- [ ] **Step 3: 从 types.go 移出旧编解码**

编辑 `internal/core/types.go`，删除这两个函数（整体移到 `encoding.go`，Task 1 Step 4 会以新形态重建）：

```go
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
```

同时把包头注释的这两行：

```go
//   - Message：协议无关的消息内部表示（adapter 负责与 proto 互转）
//   - 存储编解码（当前 JSON，Body 走 base64）
```

改成：

```go
//   - Message：协议无关的消息内部表示（adapter 负责与 proto 互转）
//   - InflightState 等辅助结构及其编解码
```

并把包头「边界」里的这两行：

```go
//   - JSON 编码是 M1 的性能取舍：可读易调试，量级足够；
//     若未来需要可替换为二进制编码，Encode/Decode 是唯一出入口
```

改成：

```go
//   - 不含消息的存储编解码——已独立到 encoding.go（两套格式 + 档位开关），
//     本文件只留 Message 模型本身与 InflightState 的编解码
```

`import` 块保持不变：`encoding/json` 与 `fmt` 仍被 `InflightState` 的编解码使用。

- [ ] **Step 4: 写最小实现**

创建 `internal/core/encoding.go`：

```go
// encoding.go: 消息的存储编码——历史 JSON 格式与 v1 二进制混合格式。
//
// 职责：
//   - EncodeMessage/DecodeMessage：Message ↔ 落盘/跨节点转发字节的唯一出入口
//   - 写方向档位开关（SetEncoding）：装配期一次性选定 json 或 binary
//   - 读方向双格式判别：按首字节区分历史 JSON 与 v1，旧数据零迁移
//
// 边界：
//   - 不管 InflightState 等其它结构的编解码（体积个位数字节级，留在 types.go）
//   - 不做完整性校验：CRC 由下层承担（seglog 帧 CRC32-Castagnoli、Pebble
//     block checksum），本层重复校验只是重复开销
//   - 不解释 Body 内容：压缩与校验和是端到端的事，sq 只原样收下、原样交还
//     （见 types.go 的 BodyEncoding 注释）
//   - 本文件不打日志：core 是无 logger 的叶子包，且编解码是每消息热路径，
//     逐条日志会淹没日志系统。可观测性由「错误自带定位上下文（首字节/长度/
//     越界值）+ 装配期档位日志（cmd/sq/main.go）」承担
package core

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync/atomic"
)

// 写方向编码档位的 token，与配置项 message_encoding 的取值一一对应。
const (
	// EncodingJSON 历史格式：整条消息 JSON，Body 经 base64（+33% 膨胀）。
	EncodingJSON = "json"
	// EncodingBinary v1 二进制混合格式：Body 原始字节直落（见 encodeMessageV1）。
	EncodingBinary = "binary"
)

const (
	// msgVersionV1 v1 格式的版本字节。取 0x01 不是随手选的：合法 JSON 文档
	// 的首字节恒为 '{'，0x01 不可打印、与之永不碰撞，解码据此零歧义判别
	// 新旧格式（见 DecodeMessage）。将来若需要新布局，用 0x02 即可，判别
	// 逻辑不变。
	msgVersionV1 byte = 0x01
	// msgV1HeaderLen v1 定长头：1B 版本 + 4B 大端元数据长度。
	msgV1HeaderLen = 5
	// jsonObjectStart 历史格式的判别字节：json.Marshal 结构体的输出恒以
	// '{' 开头且无前导空白。
	jsonObjectStart byte = '{'
)

// binaryEncoding 写方向是否使用 v1 二进制格式。
//
// 装配期一次性写入（SetEncoding），此后只读。用 atomic 而不是裸 bool：
// 写在装配 goroutine、读在各请求 goroutine，裸 bool 在 -race 下是未定义
// 的数据竞争（即便实际取值不会变）。
//
// 只影响写方向——解码永远双格式，这是滚动升级安全的根基（见 DecodeMessage）。
var binaryEncoding atomic.Bool

// SetEncoding 设置写方向的编码档位，由 main 在开始服务前调用一次。
//
// 参数：
//   - enc: EncodingJSON 或 EncodingBinary（即配置项 message_encoding 的值）
//
// 返回：
//   - error: enc 不是已知档位。调用方应 fail-stop——静默回落默认档会让
//     运维以为配置生效了，而盘上格式与预期不符是事后极难察觉的偏差。
//
// 注意：
//   - 非并发安全的语义边界是「装配期调用一次」；运行中改档位不受支持
//     （已写出的数据不会重编码，也没有这个需求）。
func SetEncoding(enc string) error {
	switch enc {
	case EncodingJSON:
		binaryEncoding.Store(false)
	case EncodingBinary:
		binaryEncoding.Store(true)
	default:
		return fmt.Errorf("core: 未知消息编码档位 %q（只接受 %s|%s）",
			enc, EncodingJSON, EncodingBinary)
	}
	return nil
}

// EncodeMessage 序列化消息，用于落盘与跨节点转发（OpForwardAppend 载荷）。
//
// 参数：
//   - m: 待编码消息；Body 可为空
//
// 返回：
//   - 编码字节；格式由装配期档位决定（SetEncoding），默认 json 与历史
//     版本逐字节相同
//   - error: 序列化失败（对正常消息不可达），信息带消息 ID
//
// 注意：
//   - 无论产出哪一档，本版本的 DecodeMessage 都能解开；反之，旧版本
//     进程解不开 binary 档的产物——这正是 README「升级注意」里两步纪律
//     的由来。
func EncodeMessage(m *Message) ([]byte, error) {
	if binaryEncoding.Load() {
		return encodeMessageV1(m)
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("编码消息 %s: %w", m.ID, err)
	}
	return b, nil
}

// msgMeta 编码 v1 元数据段时用来遮蔽 Body 的包装类型：Body 单独作为原始
// 字节段落在帧末尾，绝不能再进元数据 JSON——否则 base64 膨胀原样保留，
// 整个改动一无所获。
//
// 为什么遮蔽字段的 tag 是 `json:"body,omitempty"` 而不是直觉上的 `json:"-"`：
// encoding/json 扫描结构体字段时，对 tag 为 "-" 的字段直接跳过，它**根本
// 不进入同名字段的深度竞争**；于是内嵌 *Message 里 promoted 的 Body
// （深度 1）成了唯一的 body 字段，照样被编码进去。实测 `json:"-"` 版本
// 输出 {"id":...,"body":"AP8=",...}——什么也没省下，而且所有 round-trip
// 用例照样全绿，静默得毫无察觉。带真实 tag 的遮蔽字段位于深度 0，按 JSON
// 名竞争胜出，配合 omitempty + 零值即完全不产生 body 键。
//
// 改这里的 tag 前先看 TestV1MetaSegmentHasNoBodyKey。
type msgMeta struct {
	*Message
	Body []byte `json:"body,omitempty"`
}

// encodeMessageV1 按 v1 二进制混合格式编码：
//
//	[0]           0x01             版本字节
//	[1,5)         u32 BE metaLen   元数据 JSON 字节数
//	[5,5+m)       元数据 JSON       Message 除 Body 外的全部字段（tag 与历史格式一致）
//	[5+m,末尾)     Body 原始字节     长度由总长隐含，不另存长度字段
//
// 元数据为什么仍用 JSON：它原样继承了 Message 现有 tag 的演进性质
// （omitempty + 缺键即零值，旧数据无需迁移），而典型元数据只有百字节
// 量级，这点 JSON 开销可忽略——真正的大头是 Body 的 base64 膨胀，
// 这里把它整段拿掉即可。全二进制元数据的收益是零头，代价是自建一套
// tag 演进纪律。
func encodeMessageV1(m *Message) ([]byte, error) {
	meta, err := json.Marshal(&msgMeta{Message: m})
	if err != nil {
		return nil, fmt.Errorf("编码消息 %s 元数据: %w", m.ID, err)
	}
	out := make([]byte, msgV1HeaderLen+len(meta)+len(m.Body))
	out[0] = msgVersionV1
	binary.BigEndian.PutUint32(out[1:msgV1HeaderLen], uint32(len(meta)))
	copy(out[msgV1HeaderLen:], meta)
	copy(out[msgV1HeaderLen+len(meta):], m.Body)
	return out, nil
}

// DecodeMessage 反序列化落盘/转发消息，按首字节自动判别格式：
//
//	'{'  → 历史 JSON 格式
//	0x01 → v1 二进制混合格式
//
// 参数：
//   - b: 编码字节；调用方可在本函数返回后复用/覆写它（Body 已拷贝）
//
// 返回：
//   - 解码后的消息（Body 与 b 无内存共享）
//   - error: 空输入、未知格式、v1 头损坏、元数据非法。错误信息带首字节
//     与长度——core 无 logger，这是排查的唯一线索
//
// 注意：
//   - 双格式解码永久保留，无淘汰计划。盘上新旧混存是常态（升级不迁移），
//     且 OpForwardAppend 把编码字节直接跨节点转发，混版集群里两种格式会
//     同时出现在网络上。任何"只认新格式"的简化都会让滚动升级窗口内的
//     写入失败。
func DecodeMessage(b []byte) (*Message, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("解码消息: 空字节")
	}
	switch b[0] {
	case msgVersionV1:
		return decodeMessageV1(b)
	case jsonObjectStart:
		m := &Message{}
		if err := json.Unmarshal(b, m); err != nil {
			return nil, fmt.Errorf("解码消息: %w", err)
		}
		return m, nil
	default:
		// 首字节既不是 '{' 也不是已知版本号：数据损坏，或来自更新版本
		// 写入的格式（降级运行）。首字节与长度是运维区分这两类的仅有信息。
		return nil, fmt.Errorf("解码消息: 无法识别的格式，首字节 0x%02X（长度 %d B）",
			b[0], len(b))
	}
}

// decodeMessageV1 解 v1 格式（布局见 encodeMessageV1）。
func decodeMessageV1(b []byte) (*Message, error) {
	if len(b) < msgV1HeaderLen {
		return nil, fmt.Errorf("解码消息: v1 帧长 %d B 不足定长头 %d B",
			len(b), msgV1HeaderLen)
	}
	metaLen := binary.BigEndian.Uint32(b[1:msgV1HeaderLen])
	// 先校验再切片：metaLen 来自盘上字节，损坏时可能是任意值，直接切会
	// panic。提升到 uint64 相加，避免 32 位平台上 5+metaLen 溢出回绕后
	// 通过校验（回绕成小数值时切片仍然越界）。
	if uint64(msgV1HeaderLen)+uint64(metaLen) > uint64(len(b)) {
		return nil, fmt.Errorf("解码消息: v1 元数据长度 %d B 越界（总长 %d B）",
			metaLen, len(b))
	}
	end := msgV1HeaderLen + int(metaLen)
	m := &Message{}
	if err := json.Unmarshal(b[msgV1HeaderLen:end], m); err != nil {
		return nil, fmt.Errorf("解码消息: v1 元数据: %w", err)
	}
	// Body 必须拷贝，绝不能子切片引用 b：调用方（deliver/query 的 Pebble
	// 迭代回调）默认解码结果的生命周期独立于底层字节——历史 JSON 路径经
	// base64 解码天然产生新内存，这个性质是被依赖的。Pebble 的 value 只在
	// 回调内有效，迭代器移动后内存会被复用，子切片引用的表现是「消息内容
	// 随机变成别人的」。零长时 append 返回 nil，与元数据里可能残留的
	// body 值一并清掉，语义干净。
	m.Body = append([]byte(nil), b[end:]...)
	return m, nil
}
```

- [ ] **Step 5: 运行测试确认通过**

Run: `go test ./internal/core/ -v`
Expected: PASS —— 新增 8 个用例全绿，且 `types_test.go` 既有的 `TestMessageRoundTrip`、`TestDecodeLegacyMessageWithoutPassthroughFields` 等**零修改**通过（它们是 json 档不变性的第二重锚）

- [ ] **Step 6: 全仓构建与下游回归**

Run:
```bash
go build ./... && go vet ./... && go test ./internal/core/... ./internal/store/... ./internal/admin/... -count=1
```
Expected: 全部 PASS。下游包一行未改却必须全绿——这是「调用点零改动」约束的直接验证。

- [ ] **Step 7: 加关键节点日志**

本任务**不加任何日志**，这是刻意的，理由写在 `encoding.go` 文件头「边界」第四条：`core` 是无 logger 的叶子包，编解码是每消息热路径，逐条日志会淹没日志系统。本任务对可观测性的落实由两件已完成的事承担，逐项确认：

- 每个错误分支的 error 都带定位上下文：`SetEncoding` 带非法 token 与合法取值；`DecodeMessage` 带首字节与长度；`decodeMessageV1` 带帧长/头长、越界的 metaLen 与总长；`encodeMessageV1` 带消息 ID。
- 装配期档位日志在 Task 2 Step 4 补齐（`core` 侧无 logger 可用，只能在 `main` 打）。

确认无 `fmt.Printf`/`print`：

Run: `grep -n 'fmt\.Print\|println(' internal/core/encoding.go`
Expected: 无输出

- [ ] **Step 8: 加注释**

逐项确认（缺任一项即返工）：
- `encoding.go` 文件头：职责 3 条 + 边界 4 条（含「不打日志」及其理由）
- 导出符号 doc 注释：`EncodingJSON`/`EncodingBinary`/`SetEncoding`/`EncodeMessage`/`DecodeMessage`，均含参数/返回/注意
- 「为什么」注释四处：`msgMeta` 的 tag 陷阱（最长的一处，含实测输出）、`decodeMessageV1` 的 Body 拷贝、`msgVersionV1` 的取值理由、`DecodeMessage` 的双格式永久保留
- 边界条件注释：`uint64` 提升防溢出回绕、零长 Body 的 append 语义
- `types.go` 包头已按 Step 3 更新，不再宣称自己负责存储编解码

- [ ] **Step 9: 提交**

```bash
git add internal/core/encoding.go internal/core/encoding_test.go internal/core/types.go
git commit -m "feat(core): 消息 v1 二进制编码与双格式解码——写方向默认仍为 json"
```

---

### Task 2: 编码档位配置、装配与升级纪律文档

**Files:**
- Modify: `internal/config/config.go`（字段 + 默认值 + 白名单校验）
- Modify: `internal/config/config_test.go`（默认值 + 非法档位拒绝）
- Modify: `cmd/sq/main.go`（装配 + 日志）
- Modify: `README.md`（配置表 + 升级注意）
- Modify: `sq.example.yaml`（配置项说明）

**Interfaces:**
- Consumes: `core.SetEncoding`、`core.EncodingJSON`、`core.EncodingBinary`（Task 1）
- Produces: `config.Config.MessageEncoding string`（yaml 键 `message_encoding`，缺省 `"json"`）

- [ ] **Step 1: 写失败的测试**

在 `internal/config/config_test.go` 末尾追加：

```go
// TestMessageEncodingDefault 缺省必须是 json：binary 只能显式开启。
//
// 为什么默认值本身值得钉一条用例：这个默认值是「升级不改配置就零风险」
// 的全部依据。若某次重构把默认翻成 binary，升级后混版集群里旧节点立刻
// 解不开新节点转发的消息，而单元测试全绿——只有直接断言默认值拦得住。
func TestMessageEncodingDefault(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MessageEncoding != "json" {
		t.Fatalf("message_encoding 默认值 = %q, 应为 json", cfg.MessageEncoding)
	}
}

// TestMessageEncodingOverrideAndValidation 覆盖生效 + 非法值启动即拒。
//
// 非法值必须挡在启动期：静默回落默认档会让运维以为 binary 已生效，
// 而盘上格式与预期不符是事后极难察觉的偏差（数据没坏，只是没省下）。
func TestMessageEncodingOverrideAndValidation(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "sq.yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		return p
	}

	cfg, err := Load(write(t, "message_encoding: binary\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MessageEncoding != "binary" {
		t.Fatalf("覆盖未生效: %q", cfg.MessageEncoding)
	}

	if _, err := Load(write(t, "message_encoding: protobuf\n")); err == nil {
		t.Fatal("非法 message_encoding 应拒绝启动")
	} else if !strings.Contains(err.Error(), "message_encoding") {
		t.Fatalf("错误信息未指明配置项: %v", err)
	}
}
```

若 `config_test.go` 尚未 import `strings`，在 import 块补上。

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/config/ -run TestMessageEncoding -v`
Expected: 编译失败 —— `cfg.MessageEncoding undefined`

- [ ] **Step 3: 写最小实现（config）**

在 `internal/config/config.go` 的 `Config` 结构体里，紧随 `Fsync` 字段之后插入：

```go
	// MessageEncoding 消息落盘/跨节点转发的编码档位：json|binary，缺省 json。
	//
	// json = 历史格式（整条 JSON，Body 走 base64，+33% 体积）；
	// binary = v1 混合格式（Body 原始字节直落）。取值与 core.EncodingJSON/
	// core.EncodingBinary 一一对应（此处用字面量与 fsync/ack 同风格，
	// 保持 config 包不依赖 core）。
	//
	// ⚠️ 开启 binary 必须在**全集群升级完成之后**：解码永远双格式，但旧版本
	// 进程不认识 v1，混版期提前开启会让旧节点解不开转发来的消息。两步纪律
	// 见 README「升级注意」。
	MessageEncoding string `yaml:"message_encoding"`
```

在 `Load` 的默认值字面量里，`Fsync: "sync",` 同一行之后加上：

```go
		MessageEncoding: "json",
```

在 `Fsync` 的校验之后插入白名单校验（与 `fsync` 同风格）：

```go
	// 非法档位必须挡在启动期：静默回落默认档会让运维以为 binary 已生效，
	// 而"盘上格式与预期不符"不会报错、不会丢数据，只是什么也没省下——
	// 这类偏差事后几乎不可能被发现。
	if cfg.MessageEncoding != "json" && cfg.MessageEncoding != "binary" {
		return nil, fmt.Errorf("配置 message_encoding 只接受 json|binary，得到 %q", cfg.MessageEncoding)
	}
```

- [ ] **Step 4: 写最小实现（装配与日志）**

编辑 `cmd/sq/main.go`。在 `config.SetupSlog(cfg.LogLevel)` / `logger := slog.Default()` 之后、`store.Open` 之前插入：

```go
	// 编码档位必须在任何编解码发生之前装配：它是进程级一次性开关。
	// 非法值 fail-stop——config.Load 已挡过一道，这里是 core 侧边界的
	// 二次确认，两处都便宜且都在启动期。
	if err := core.SetEncoding(cfg.MessageEncoding); err != nil {
		return err
	}
	// 档位是"盘上为什么出现二进制数据"的唯一线索，无条件打一条：
	// 「已启动」日志在 recover 子命令与启动期失败时不会出现。
	logger.Info("消息编码档位已装配", "message_encoding", cfg.MessageEncoding)
```

`core` 已在 import 块中（`cmd/sq/main.go:40`），无需新增 import。

在两条「sq 已启动」日志里各补一个字段。集群模式那条：

```go
		logger.Info("sq 已启动（集群模式）", "grpc_listen", cfg.GRPCListen,
			"advertise", cfg.AdvertiseHost, "data_dir", cfg.DataDir, "fsync", cfg.Fsync,
			"message_encoding", cfg.MessageEncoding,
			"node_id", cfg.Cluster.NodeID, "data_groups", cfg.Cluster.DataGroups,
			"ack", cfg.Cluster.Ack, "peers", len(cfg.Cluster.Peers))
```

单机模式那条：

```go
		logger.Info("sq 已启动（单机模式）", "grpc_listen", cfg.GRPCListen,
			"advertise", cfg.AdvertiseHost, "data_dir", cfg.DataDir, "fsync", cfg.Fsync,
			"message_encoding", cfg.MessageEncoding,
			"txn_check_interval", cfg.TxnCheckInterval, "txn_max_checks", cfg.TxnMaxChecks)
```

- [ ] **Step 5: 修复 e2e 配置字面量（本步不可跳过——新校验必然打断全部 e2e）**

`test/e2e` 的两处配置是用 `config.Config` **结构体字面量** 构造后 `yaml.Marshal` 出去的，不走 `Load` 的默认值。新增字段的零值会被照实序列化成 `message_encoding: ""`（yaml.v3 对无 `omitempty` 的空串照写，已实测），broker 读到后被 Step 3 的白名单挡下，**全部 e2e 用例启动失败**。这与既有的 `DefaultMaxAttempts`/`RetentionCheckInterval` 是同一个陷阱，那两处的注释已写明。

`test/e2e/sdk_test.go` 的 `writeBrokerConfig`（约 226 行的字面量），在 `Fsync: "sync",` 之后加：

```go
		// 与 DefaultMaxAttempts/RetentionCheckInterval 同款陷阱：本结构体
		// 不走 config.Load 的默认值，零值会序列化成 message_encoding: ""
		// 并被 Load 的白名单拒绝，broker 直接起不来。取值同 Load 的缺省。
		MessageEncoding: "json",
```

`test/e2e/sdk_cluster_test.go` 的 `clusterNodeConfig`（约 117 行的字面量），同样在 `Fsync: "sync",` 之后加：

```go
		MessageEncoding: "json", // 同 writeBrokerConfig：字面量不走 Load 默认值，空串会被拒
```

- [ ] **Step 6: 运行测试确认通过**

Run: `go test ./internal/config/ -v && go build ./... && go vet ./...`
Expected: PASS + 构建通过

再确认 e2e 能起来（只跑一条最快的用例做冒烟，全量回归留给 Task 3）：

Run: `cd test/e2e && go test -tags e2e -run TestOfficialGoSDKDelayDelivery -v -timeout 5m 2>&1 | tail -5`
Expected: PASS（若报 `message_encoding 只接受 json|binary`，说明 Step 5 漏改了某处字面量）

- [ ] **Step 7: 手工验证档位日志真的打出来**

用临时数据目录，别让默认的 `./data` 落进仓库工作区：

```bash
D=$(mktemp -d) && printf 'data_dir: "%s/data"\nadmin_listen: ""\n' "$D" > "$D/ok.yaml" \
  && (go run ./cmd/sq -config "$D/ok.yaml" 2>&1 & sleep 6; kill %1) | grep -m2 'message_encoding'
```
Expected: 能看到 `消息编码档位已装配 ... message_encoding=json`，以及「sq 已启动（单机模式）」那条里带 `message_encoding=json`。

再验非法值被挡住：

```bash
D=$(mktemp -d) && printf 'data_dir: "%s/data"\nmessage_encoding: protobuf\n' "$D" > "$D/bad.yaml" \
  && go run ./cmd/sq -config "$D/bad.yaml"; echo "退出码=$?"
```
Expected: `sq 启动失败`，错误信息含 `message_encoding 只接受 json|binary`，`退出码=1`。

- [ ] **Step 8: 更新配置文档**

`sq.example.yaml`，在 `fsync: sync` 之后插入：

```yaml
# 消息编码：json | binary
#   json   = 历史格式（整条 JSON，Body 走 base64，体积 +33%）
#   binary = v1 混合格式（Body 原始字节直落，省掉 base64 膨胀与编解码 CPU）
# ⚠️ 开启 binary 必须在全集群升级完成之后（两步纪律见 README「升级注意」）：
#    解码永远双格式，但旧版本进程不认识 binary 产物。
message_encoding: json
```

`README.md` 的「配置」YAML 块，在 `fsync: sync                   # sync|async` 之后插入：

```yaml
message_encoding: json         # json|binary。binary 让 Body 以原始字节落盘，
                               # 省掉 base64 的 +33% 体积与编解码 CPU；
                               # 开启前请先读「升级注意」的两步纪律
```

`README.md` 的「升级注意」小节末尾追加一条：

```markdown
- **开启 `message_encoding: binary` 必须分两步（顺序不可颠倒）**：本版本起
  broker **读**方向同时认识两种消息格式（历史 JSON 与 v1 二进制），**写**方向
  由 `message_encoding` 决定，缺省仍是 `json`。

  1. **先把全集群升到本版本，`message_encoding` 保持缺省 `json`。** 此阶段
     所有节点写的都是历史格式，任何新旧混版组合都安全。
  2. **确认全部节点都已升级后**，再逐节点把 `message_encoding` 改为 `binary`
     并重启。翻档期间部分节点写二进制、部分写 JSON——所有节点都能解双格式，
     安全。

  **为什么顺序不能颠倒**：跨组写入经 `OpForwardAppend` 把编码字节直接转发给
  目标组 leader。混版集群里提前开启 binary，二进制字节会转发给尚未升级的
  旧版 leader，它解不开，写入直接失败。

  **回滚边界**：第 1 步可自由回滚。第 2 步之后盘上已有二进制数据，
  **降级到不认识该格式的旧版本不受支持**；把 `message_encoding` 改回 `json`
  只停止新增二进制数据，不消除存量。
```

- [ ] **Step 9: 加关键节点日志**

已在 Step 4 完成，逐项确认：
- 装配成功打 Info（`消息编码档位已装配`，带档位）——成功路径不静默
- 装配失败经 `return err` 汇入 `main` 的 `slog.Error("sq 启动失败", ...)`——错误分支带上下文（`core.SetEncoding` 的错误信息含非法 token 与合法取值）
- 两条「已启动」日志带 `message_encoding` 字段
- 无 `fmt.Printf`：`grep -n 'fmt\.Print' cmd/sq/main.go` 应无新增

- [ ] **Step 10: 加注释**

- `Config.MessageEncoding` 字段注释：取值含义 + 两步纪律警告 + 为何用字面量而非 import core
- `Load` 里白名单校验的「为什么必须启动期挡住」注释
- `main.go` 装配处两条注释：为何必须在编解码之前、为何无条件打日志
- e2e 两处字面量的注释：指明「不走 Load 默认值」这个同款陷阱
- 两条新增测试的用例注释说明「为什么这条值得钉」

- [ ] **Step 11: 提交**

```bash
git add internal/config/config.go internal/config/config_test.go cmd/sq/main.go \
        README.md sq.example.yaml test/e2e/sdk_test.go test/e2e/sdk_cluster_test.go
git commit -m "feat(config,cmd): message_encoding 档位配置与装配，README 补两步升级纪律"
```

---

### Task 3: 跨档位互读 e2e 回归

单测证明格式对，e2e 证明**真实进程跨重启换档后数据依然连续**——这是两步升级纪律唯一的端到端护栏。

**Files:**
- Modify: `test/e2e/sdk_test.go`（新增 `rewriteBrokerConfig` helper）
- Create: `test/e2e/sdk_encoding_test.go`

**Interfaces:**
- Consumes: `writeBrokerConfig`/`launchBroker`/`brokerHandle.stop`/`dumpBrokerLog`/`sendMessages`/`newSimpleConsumer`（`test/e2e` 既有 helper）；`config.Config.MessageEncoding`（Task 2）
- Produces: `func rewriteBrokerConfig(t *testing.T, cfgPath string, mutate ...func(*config.Config))`

- [ ] **Step 1: 写失败的测试**

在 `test/e2e/sdk_test.go` 的 `writeBrokerConfig` 之后插入 helper：

```go
// rewriteBrokerConfig 就地改写已有的 broker 配置文件（保留 data_dir、端口等
// 全部既有取值），供「同一份数据目录、跨代换配置重启」的用例使用。
//
// 为什么不能再调一次 writeBrokerConfig：它内部 t.TempDir() + pickPort()
// 会给出全新的数据目录与端口，第二代进程就读不到第一代写下的数据了——
// 而跨档位互读要验证的恰恰是"同一份盘上数据被另一档位的进程读出来"。
func rewriteBrokerConfig(t *testing.T, cfgPath string, mutate ...func(*config.Config)) {
	t.Helper()
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("读 broker 配置失败: %v", err)
	}
	cfg := &config.Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		t.Fatalf("解析 broker 配置失败: %v", err)
	}
	for _, f := range mutate {
		f(cfg)
	}
	out, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("序列化 broker 配置失败: %v", err)
	}
	if err := os.WriteFile(cfgPath, out, 0o600); err != nil {
		t.Fatalf("写 broker 配置失败: %v", err)
	}
}
```

创建 `test/e2e/sdk_encoding_test.go`：

> ⚠️ **首行必须是 `//go:build e2e`**（`test/e2e/` 全部文件都带此标签，漏写会让用例根本不参与编译，表现为「跑了但一条没执行」）。

```go
//go:build e2e

// sdk_encoding_test.go 验证消息编码档位切换的端到端连续性。
//
// 职责：同一份数据目录跨代换档重启后，两代写入的消息都能被消费到，
//       且 Body 逐字节正确（含延时消息这类走独立键前缀的路径）。
// 边界：不测格式内部布局（internal/core 的单测覆盖）；不测集群混版
//       （需要多机，属 spec 的三机验收范围）。
package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"

	"github.com/xushixin/sq/internal/config"
)

// TestMessageEncodingCrossGeneration 锁死两步升级纪律的端到端前提：
// 换档重启后，**旧档写的存量**与**新档写的增量**必须都能正常消费。
//
// 这是单测覆盖不到的一环——单测里两种格式的字节是同一个进程造出来的，
// 而这里第一代进程只会写 JSON、第二代进程只会写二进制，盘上真真正正
// 是混存的，解码分流走的是真实的 Pebble 读路径。
func TestMessageEncodingCrossGeneration(t *testing.T) {
	const topic = "e2e-encoding"
	const group = "e2e-encoding-g"
	const perGen = 3

	cfgPath, endpoint := writeBrokerConfig(t, func(c *config.Config) {
		c.MessageEncoding = "json" // 第一代：历史格式
	})
	dir := filepath.Dir(cfgPath)
	run1Log := filepath.Join(dir, "broker-json.log")
	run2Log := filepath.Join(dir, "broker-binary.log")

	var cur *brokerHandle
	t.Cleanup(func() {
		if cur != nil {
			cur.stop(t)
		}
		if t.Failed() { // 换档用例最难排查的是"哪一代出的问题"，两代日志都展开
			dumpBrokerLog(t, run1Log)
			dumpBrokerLog(t, run2Log)
		}
	})

	// ---- 第一代（json 档）：写 perGen 条 ----
	cur = launchBroker(t, cfgPath, endpoint, run1Log)
	genA := sendMessages(t, endpoint, topic, perGen)
	t.Logf("第一代（json）已写入 %d 条", len(genA))
	cur.stop(t)
	cur = nil

	// ---- 换档为 binary，同一份 data_dir 重启 ----
	rewriteBrokerConfig(t, cfgPath, func(c *config.Config) {
		c.MessageEncoding = "binary"
	})
	cur = launchBroker(t, cfgPath, endpoint, run2Log)
	genB := sendMessages(t, endpoint, topic, perGen)
	t.Logf("第二代（binary）已写入 %d 条", len(genB))

	// ---- 消费：两代写入的消息必须全部收到 ----
	want := make(map[string]bool, perGen*2)
	for _, id := range append(append([]string{}, genA...), genB...) {
		want[id] = false
	}
	consumer := newSimpleConsumer(t, endpoint, group, topic)
	deadline := time.Now().Add(60 * time.Second)
	got := 0
	for got < len(want) && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, recvInvisible)
		if err != nil {
			continue // 空轮询
		}
		for _, mv := range mvs {
			id := mv.GetMessageId()
			if seen, ok := want[id]; ok && !seen {
				want[id] = true
				got++
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack msgId=%s 失败: %v", id, err)
			}
		}
	}
	for id, seen := range want {
		if !seen {
			t.Errorf("消息 %s 未被消费到（换档后存量/增量丢失）", id)
		}
	}
	if t.Failed() {
		t.Fatalf("换档消费不完整：%d/%d", got, len(want))
	}
}

// TestMessageEncodingDelayMessage 延时消息在 binary 档下的往返。
//
// 单独一条：延时消息落在 delay/ 前缀、到期后被改写移入 msg/，是编码
// 出入口之外还会"再读一次再写一次"的路径，容易在格式切换时被漏掉。
func TestMessageEncodingDelayMessage(t *testing.T) {
	const topic = "e2e-encoding-delay"
	const group = "e2e-encoding-delay-g"

	endpoint := startBroker(t, func(c *config.Config) {
		c.MessageEncoding = "binary"
	})

	// 含非 UTF-8 字节：base64 能安全承载它，原始字节段同样必须能
	body := []byte{0x00, 0xFF, 0x7F, 'd', 'e', 'l', 'a', 'y'}

	// producer 内联构造：test/e2e 里没有公共的 newProducer helper，
	// 每个用例各自 rmq.NewProducer（与 sdk_delay_test.go 同形）。
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

	due := time.Now().Add(6 * time.Second) // 与既有延时用例同节奏，不贴着到期卡点
	msg := &rmq.Message{Topic: topic, Body: body}
	msg.SetDelayTimestamp(due)
	res, err := producer.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("发送延时消息失败: %v", err)
	}
	if len(res) == 0 {
		t.Fatal("发送延时消息返回空结果")
	}
	t.Logf("延时消息已发送 msgId=%s due=%s", res[0].MessageID, due.Format(time.RFC3339))

	// 用 newDelayConsumer（sdk_delay_test.go 既有）：它的 AwaitDuration 就是
	// 按延时用例的轮询节奏调好的，复用而不是再造一个。
	consumer := newDelayConsumer(t, endpoint, group, topic)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, recvInvisible)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 返回，属正常
		}
		for _, mv := range mvs {
			if got := mv.GetBody(); string(got) != string(body) {
				t.Fatalf("Body 不一致: %x, 应为 %x", got, body)
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			return // 收到且内容正确，用例达成
		}
	}
	t.Fatal("延时消息到期后 60s 内未被投递")
}
```

> **实现者注意**：`recvInvisible`（`sdk_recovery_test.go:36`，值 3s）与 `newDelayConsumer`（`sdk_delay_test.go:25`）都是 `package e2e` 内既有符号，直接复用即可，**不要重复定义**（同包重名会编译失败）。

- [ ] **Step 2: 运行测试确认失败**

Run:
```bash
cd test/e2e && go test -tags e2e -run 'TestMessageEncoding' -v 2>&1 | tail -20
```
Expected: 编译失败 —— `undefined: rewriteBrokerConfig`（若 helper 尚未加）或 `c.MessageEncoding undefined`（若 Task 2 未完成）

> `-tags e2e` 不可省：`test/e2e/` 全部文件带 `//go:build e2e`，漏了标签会「零用例执行」却显示 ok，看起来像通过。

- [ ] **Step 3: 补齐实现并运行**

Task 1、2 完成后本任务无额外产品代码，只需按 Step 1 落地 helper 与用例，并按「实现者注意」复用既有符号。

Run:
```bash
cd test/e2e && go test -tags e2e -run 'TestMessageEncoding' -v -timeout 10m 2>&1 | tail -30
```
Expected: 两条用例 PASS（日志里应能看到 `-- PASS: TestMessageEncodingCrossGeneration` 与 `-- PASS: TestMessageEncodingDelayMessage` 两行；只显示 `ok` 而无 PASS 行 = 标签漏了）

- [ ] **Step 4: e2e 全量回归**

Run:
```bash
cd test/e2e && go test -tags e2e -count=1 -timeout 30m ./... 2>&1 | tail -10
```
Expected: 全部 PASS —— 既有用例走 `json` 档（Task 2 Step 5 已在两处配置字面量显式写入 `MessageEncoding: "json"`），行为应与改动前完全一致

> 若出现 `message_encoding 只接受 json|binary` 的启动失败，说明还有第三处 `config.Config` 结构体字面量没改到。定位：`grep -rn 'config.Config{' test/e2e/`。

- [ ] **Step 5: 加关键节点日志**

e2e 侧不新增产品代码，日志覆盖由 Task 1/2 承担。本步确认 e2e 的**可诊断性**：
- 两代 broker 日志分文件（`broker-json.log` / `broker-binary.log`），失败时都展开——换档用例最贵的排查成本就是分不清哪一代出的问题
- 每代写入后 `t.Logf` 记条数，消费失败时 `t.Errorf` 逐条列出丢失的 msgId（不是只报总数）

- [ ] **Step 6: 加注释**

- `sdk_encoding_test.go` 文件头：职责 + 边界
- `rewriteBrokerConfig` doc 注释：含「为什么不能再调 writeBrokerConfig」
- 两条用例的注释说明「为什么单测覆盖不了这一环」

- [ ] **Step 7: 提交**

```bash
git add test/e2e/sdk_test.go test/e2e/sdk_encoding_test.go
git commit -m "test(e2e): 编码档位跨代互读回归——json 存量与 binary 增量都可消费"
```

---

### Task 4: 三机配对 bench 与 spec 回填

**Files:**
- Modify: `docs/superpowers/specs/2026-08-12-message-binary-encoding-design.md`（§7 实测结果回填）

**Interfaces:**
- Consumes: `TestExternalClusterWriteThroughput`（`test/e2e/sdk_cluster_bench_test.go` 既有）
- Produces: 合入决策（收益达标 → 合 main；mem 档劣化 > 5% → 不合入，按既有前例留痕）

- [ ] **Step 1: macOS 全量回归 + -race**

Run:
```bash
go build ./... && go vet ./... && go test ./internal/... -count=1
go test ./internal/core/... ./internal/store/... -race -count=1
```
Expected: 全绿

- [ ] **Step 2: 交叉编译并部署三机**

⚠️ **派发前先确认执行机平台**（既有教训：devbox 是 macOS，不是 Linux）。本地交叉编译后 scp，不在远端装工具链。

```bash
GOOS=linux GOARCH=amd64 go build -o /tmp/sq.linux ./cmd/sq
cd test/e2e && GOOS=linux GOARCH=amd64 go test -tags e2e -c -o /tmp/e2e.linux.test .
```

把 `/tmp/sq.linux` 分发到三台机器并按既有集群配置部署（`message_encoding` 先保持 `json`），`/tmp/e2e.linux.test` 放压测机。

- [ ] **Step 3: 跑配对轮次**

**方法论铁律（既有前例，违反则数据作废）**：只引用**轮内配对**比值，跨轮绝对吞吐离散度 15–50%，绝对值不可引用；每批必须重跑对照档，不可跨批次比较。

每一轮内依次跑 4 个格子（`json` 与 `binary` 各跑 `quorum-mem` / `quorum-fsync`），至少 3 轮：

```bash
SQ_BENCH=1 SQ_E2E_BROKER=/root/sqbench/sq \
SQ_BENCH_ENDPOINTS=<n1>:8081,<n2>:8081,<n3>:8081 \
SQ_BENCH_LABEL=enc-json-mem SQ_BENCH_BODY=1024 SQ_BENCH_CONC=16,256 \
./e2e.linux.test -test.run TestExternalClusterWriteThroughput -test.v -test.timeout=40m
```

改 `message_encoding: binary` 重启三节点后，同参数换 `SQ_BENCH_LABEL=enc-binary-mem` 再跑；`ack` 换 `quorum-fsync` 重复上述两次。

补一组小 Body 对照（说明收益随 Body 占比缩放）：`SQ_BENCH_BODY=128`，标签加 `-small`。

- [ ] **Step 4: 判定与回填**

验收线（spec §6.3）：
- **mem 档吞吐相对 json 档为正收益**（主要指标：mem 档是 CPU-bound，省下的 base64/JSON CPU 必须能兑现）
- **任一格子劣化不超过 5%**（沿用 async-storage-writes 复测确立的红线）

把每轮每格的配对比值、`SQ_BENCH_BODY` 两档的对照、以及判定结论写进 spec §7「实测结果」，**结论为不通过时同样如实回填并说明不合入**（既有前例：`befcfc0` 的 async 存储写入首轮验收未过即留痕不合入）。

- [ ] **Step 5: 提交**

```bash
git add docs/superpowers/specs/2026-08-12-message-binary-encoding-design.md
git commit -m "docs(spec): 消息二进制编码三机实测结果回填"
```

---

## Self-Review

**1. Spec coverage**

| Spec 章节 | 覆盖任务 |
|---|---|
| §2.1 v1 格式布局（版本字节/metaLen/元数据/Body） | Task 1 Step 4 `encodeMessageV1`；测试 Task 1 Step 1 `TestV1Layout` |
| §2.1 首字节格式判别（'{' vs 0x01） | Task 1 Step 4 `DecodeMessage`；测试 `TestV1Layout`、`TestDecodeBothFormats` |
| §2.2 元数据遮蔽 Body | Task 1 Step 4 `msgMeta`；测试 `TestV1MetaSegmentHasNoBodyKey` |
| §2.2 Body 必须拷贝 | Task 1 Step 4 `decodeMessageV1`；测试 `TestV1BodyIsCopied` |
| §2.2 写方向按档位、读方向永远双格式 | Task 1 Step 4 `EncodeMessage`/`DecodeMessage`；测试 `TestSetEncoding` |
| §2.3 不选 protobuf/全二进制 | Global Constraints + `encodeMessageV1` 注释记录理由 |
| §3.1 types.go 改动 | Task 1 Step 3、Step 4 |
| §3.2 config 新增 message_encoding | Task 2 Step 3 |
| §3.3 调用点零改动 | Global Constraints；验证 Task 1 Step 6（下游包不改而全绿） |
| §3.4 测试（round-trip/不变性/拷贝/边界） | Task 1 Step 1 全部 8 条用例 |
| §4.1 兼容面清单（盘上/ForwardAppend/raft/admin） | Task 1 双格式解码 + Task 3 e2e 跨代；ForwardAppend 语义记在 `DecodeMessage` 注释与 README |
| §4.2 两步滚动升级纪律 | Task 2 Step 7（README + sq.example.yaml + 字段注释） |
| §4.3 回滚语义 | Task 2 Step 7 README「回滚边界」 |
| §5 风险缓解（aliasing/越界/误配/可读性） | Task 1 Step 4 各处校验与注释；Task 2 Step 3 白名单校验 |
| §6.1 单测五类 | Task 1 Step 1 |
| §6.2 e2e | Task 3 |
| §6.3 三机配对 bench | Task 4 |
| §6.4 纪律项（启动日志/错误上下文/中文注释） | Task 1 Step 7–8、Task 2 Step 8–9 |

无遗漏。

**2. Placeholder scan**

无 TBD/TODO；每个代码步骤都是可直接粘贴的完整代码；无「参照 Task N」式省略。Task 3 Step 1 的「实现者注意」不是占位符，而是**明写的复用要求**（`recvInvisible`、`newDelayConsumer` 是同包既有符号，重复定义会编译失败）。

写完后逐条核过的落地事实（都已在环境里实测，不是推断）：
- `json:"-"` 遮蔽失效 → 实测输出 `{"id":"X","body":"AP8=","tag":"t"}`，`json:"body,omitempty"` 输出 `{"id":"X","tag":"t"}`
- golden JSON 串取自改动前 `EncodeMessage` 的真实输出（188 B；同消息 v1 为 182 B）
- `test/e2e` 全部文件带 `//go:build e2e`，命令一律加 `-tags e2e`
- `test/e2e` 没有公共 `newProducer` helper（各用例内联 `rmq.NewProducer`），Task 3 已按此形态写
- yaml.v3 对无 `omitempty` 的空串照实序列化 → 新增配置字段必然打断 e2e 的两处结构体字面量，已升格为 Task 2 Step 5 的必做步骤

**3. Type consistency**

- `EncodingJSON` / `EncodingBinary` — Task 1 定义，Task 1 测试与 Task 2 `main.go` 注释引用；config 侧刻意用字面量 `"json"`/`"binary"`（保持 config 不依赖 core），取值一致，理由已写进字段注释
- `SetEncoding(enc string) error` — Task 1 定义，Task 1 `TestSetEncoding` 与 Task 2 Step 4 `main.go` 调用，签名一致
- `EncodeMessage(*Message) ([]byte, error)` / `DecodeMessage([]byte) (*Message, error)` — 签名与改动前完全一致（调用点零改动的前提）
- `encodeMessageV1` / `decodeMessageV1` — Task 1 定义并在同任务测试中直调（包内私有）
- `msgVersionV1` / `msgV1HeaderLen` / `jsonObjectStart` — Task 1 定义，Task 1 测试引用，名称一致
- `Config.MessageEncoding` — Task 2 Step 3 定义，Task 2 测试、Task 3 e2e mutate 引用，名称一致
- `rewriteBrokerConfig(t, cfgPath, mutate...)` — Task 3 Step 1 定义并在同文件用例中调用

**4. 已知取舍（明写，不藏）**

- **空 Body 的 nil vs 零长切片**：v1 路径解出 `nil`，历史 JSON 路径（`"body":""`）解出非 nil 零长切片。两者在协议面等价（都编码成空字节），差异无害；`TestV1EmptyBody` 因此断言 `len()==0` 而非 `DeepEqual`。为对齐它增加代码不划算。
- **元数据仍是 JSON**：`strings`/`grep` 直接查盘上文件的可读性对 Body 失效（元数据仍可读）。admin 查询接口照常工作（走 `DecodeMessage`），接受。
- **`config` 包用字面量而非 import `core`**：与既有 `fsync`/`ack` 校验同风格，保持 config 包零业务依赖；代价是取值在两处出现，由字段注释交叉引用兜住。
