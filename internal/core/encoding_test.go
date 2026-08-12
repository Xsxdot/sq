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
