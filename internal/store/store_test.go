// 存储库的写入路径与读取操作测试。
//
// 职责：
//   - 验证 Store 的批量写入（Apply）能正确持久化数据
//   - 验证 Get 操作的存在性语义与不存在返回值
//   - 验证 Scan 的范围扫描与limit限制
//   - 验证持久化一致性：关闭后重开能恢复数据（崩溃恢复代理）
//
// 边界：
//   - 不测试并发写入冲突（由 Pebble 保证线程安全）
//   - 不测试性能特性（如刷盘延迟）
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
