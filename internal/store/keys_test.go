// Package store 提供 sq 的持久化层：key 编码 schema 与 Pebble 封装。
//
// 职责（测试）：
//   - 验证 key 编码的正确性（往返序列化）
//   - 验证 key 排序顺序符合业务逻辑（字节序 = 数值序）
//   - 验证区间扫描边界计算正确
//
// 边界：
//   - 仅测试 keys.go 导出的函数
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
