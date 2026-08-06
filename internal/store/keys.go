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
