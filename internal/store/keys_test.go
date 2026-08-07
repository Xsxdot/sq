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
	"math"
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

func TestDelayKeyRoundTrip(t *testing.T) {
	k := DelayKey(1700000000123, 42)
	due, seq, err := ParseDelayKey(k)
	if err != nil || due != 1700000000123 || seq != 42 {
		t.Fatalf("round trip: %d %d %v", due, seq, err)
	}
}

func TestDelayKeyOrdering(t *testing.T) {
	// 字节序 = (dueMs, seq) 字典序：先按到期时间，同一毫秒按 seq
	a := DelayKey(1000, 999)
	b := DelayKey(1001, 0)
	c := DelayKey(1001, 1)
	if !(bytes.Compare(a, b) < 0 && bytes.Compare(b, c) < 0) {
		t.Fatal("delay key 排序错误")
	}
}

func TestDelayScanUpperBoundIsInclusiveOfNow(t *testing.T) {
	// 上界必须恰好包含 dueMs==now 的全部 seq，且不包含 now+1 的任何条目
	up := DelayScanUpperBound(1000)
	atNowMaxSeq := DelayKey(1000, math.MaxUint64)
	atNextMs := DelayKey(1001, 0)
	if !(bytes.Compare(atNowMaxSeq, up) < 0) {
		t.Fatal("dueMs==now 的条目被上界排除")
	}
	if bytes.Compare(atNextMs, up) < 0 {
		t.Fatal("dueMs==now+1 的条目被上界纳入")
	}
}

func TestParseDelayKeyRejectsGarbage(t *testing.T) {
	for _, k := range [][]byte{[]byte("delay/short"), []byte("msg/x"), DelayKey(1, 1)[:10]} {
		if _, _, err := ParseDelayKey(k); err == nil {
			t.Fatalf("应拒绝非法 key: %q", k)
		}
	}
}
