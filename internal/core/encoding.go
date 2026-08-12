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
