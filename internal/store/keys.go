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
	keyIdxPrefix    = "keyidx/"
	delayPrefix     = "delay/"
	// DelayPrefix 延时暂存区扫描下界（导出供 delay 包使用）。
	DelayPrefix = delayPrefix
	// delayAllocKey 全局单 key，与 alloc/{topic} 的按队列计数器不同：
	// 延时条目移入前不属于任何队列，无法按队列维护计数器。
	delayAllocKey = "delayalloc"
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

// DelayKey 延时暂存条目：delay/{dueMs:8B}{seq:8B}，值为完整编码消息（spec §4）。
// dueMs 是墙钟 UnixMilli 恒为正，按 uint64 大端编码后字节序即数值序；
// seq 是全局分配的写入序号，仅用于同一毫秒内多条消息的 key 去重与稳定排序。
func DelayKey(dueMs int64, seq uint64) []byte {
	k := make([]byte, 0, len(delayPrefix)+16)
	k = append(k, delayPrefix...)
	k = append(k, PutU64(uint64(dueMs))...)
	k = append(k, PutU64(seq)...)
	return k
}

// DelayScanUpperBound 到期扫描 [DelayPrefix, 本上界) 的开区间上界：
// 用 dueMs+1 的最小 key，恰好把 dueMs<=nowMs 的全部条目（含 nowMs 毫秒内
// 任意 seq）纳入区间，且不含 nowMs+1 的任何条目。
func DelayScanUpperBound(nowMs int64) []byte {
	return DelayKey(nowMs+1, 0)
}

// ParseDelayKey 解析延时条目 key。前缀后必须恰好 16 字节定长二进制。
func ParseDelayKey(k []byte) (int64, uint64, error) {
	rest, ok := bytes.CutPrefix(k, []byte(delayPrefix))
	if !ok || len(rest) != 16 {
		return 0, 0, fmt.Errorf("非法 delay key: %q", k)
	}
	return int64(binary.BigEndian.Uint64(rest[:8])), binary.BigEndian.Uint64(rest[8:]), nil
}

// DelayAllocKey 延时 seq 全局计数器（值为下一可用 seq 的 8B 大端编码）。
func DelayAllocKey() []byte { return []byte(delayAllocKey) }

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

// CursorPrefix 全部消费位点的扫描下界（metrics/管理面全量遍历用）。
func CursorPrefix() []byte { return []byte(cursorPrefix) }

// CursorGroupPrefix 某消费组全部位点的扫描下界（含结尾 '/'，防 "g1" 误扫 "g10"）。
func CursorGroupPrefix(group string) []byte { return []byte(cursorPrefix + group + "/") }

// InflightAllPrefix 全部 inflight 记录的扫描下界（metrics 统计用）。
func InflightAllPrefix() []byte { return []byte(inflightPrefix) }

// InflightGroupPrefix 某消费组全部 inflight 的扫描下界（含结尾 '/'）。
func InflightGroupPrefix(group string) []byte { return []byte(inflightPrefix + group + "/") }

// ParseCursorKey 解析 cursor key：cursor/{group}/{topic}/{queueID:4B}。
// 两段名字按 '/' 定位，尾部必须恰好 4 字节定长二进制（与 ParseInflightKey 同理，
// 二进制段可能含 '/'，只能按位置解析）。
func ParseCursorKey(k []byte) (group, topic string, queueID uint32, err error) {
	rest, ok := bytes.CutPrefix(k, []byte(cursorPrefix))
	if !ok {
		return "", "", 0, fmt.Errorf("非法 cursor key: %q", k)
	}
	i := bytes.IndexByte(rest, '/')
	if i < 0 {
		return "", "", 0, fmt.Errorf("cursor key 结构错误: %q", k)
	}
	group = string(rest[:i])
	rest = rest[i+1:]
	j := bytes.IndexByte(rest, '/')
	if j < 0 || len(rest)-j-1 != 4 {
		return "", "", 0, fmt.Errorf("cursor key 结构错误: %q", k)
	}
	topic = string(rest[:j])
	return group, topic, binary.BigEndian.Uint32(rest[j+1:]), nil
}
