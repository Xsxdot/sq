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
	"fmt"
	"log/slog"
	"sync"
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

// TestApplyAsyncThenWaitPersists 验证拆分式提交（sync 模式）：ApplyAsync 定序、
// Wait 等待持久化，之后数据可读。
func TestApplyAsyncThenWaitPersists(t *testing.T) {
	s, err := Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	b := s.NewBatch()
	b.Set([]byte("k1"), []byte("v1"), nil)
	pending, err := s.ApplyAsync(b)
	if err != nil {
		t.Fatalf("ApplyAsync: %v", err)
	}
	if err := pending.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	v, ok, err := s.Get([]byte("k1"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get k1 = %q ok=%v err=%v, want v1", v, ok, err)
	}
}

// TestApplyAsyncNoSyncFallback 验证 syncWrites=false 的退化路径：ApplyAsync 内
// 一次性完成提交，Wait 只做批次回收，行为与 sync 模式外观一致。
func TestApplyAsyncNoSyncFallback(t *testing.T) {
	s, err := Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	b := s.NewBatch()
	b.Set([]byte("k2"), []byte("v2"), nil)
	pending, err := s.ApplyAsync(b)
	if err != nil {
		t.Fatalf("ApplyAsync: %v", err)
	}
	if err := pending.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	v, ok, err := s.Get([]byte("k2"))
	if err != nil || !ok || string(v) != "v2" {
		t.Fatalf("Get k2 = %q ok=%v err=%v, want v2", v, ok, err)
	}
}

// TestApplyAsyncConcurrentAllDurable 验证并发拆分式提交全部持久化——这是
// group commit 合并 fsync 的使用形态，不能有条目丢失或错序覆盖。
func TestApplyAsyncConcurrentAllDurable(t *testing.T) {
	s, err := Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	const goroutines, perG = 16, 20
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				b := s.NewBatch()
				key := fmt.Sprintf("cc/%d/%d", g, i)
				b.Set([]byte(key), []byte("x"), nil)
				pending, err := s.ApplyAsync(b)
				if err != nil {
					t.Errorf("ApplyAsync %s: %v", key, err)
					return
				}
				if err := pending.Wait(); err != nil {
					t.Errorf("Wait %s: %v", key, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perG; i++ {
			key := fmt.Sprintf("cc/%d/%d", g, i)
			if _, ok, err := s.Get([]byte(key)); err != nil || !ok {
				t.Fatalf("key %s 丢失: ok=%v err=%v", key, ok, err)
			}
		}
	}
}

func TestScanNonPositiveLimitMeansUnlimited(t *testing.T) {
	// limit<=0 == 不限量：该语义被 deliver 阶段 2 的跳过逻辑直接依赖（B5）
	st := openTestStore(t, t.TempDir()) // 文件内既有 helper（store_test.go:19）
	defer st.Close()
	const n = 10
	for i := 0; i < n; i++ {
		b := st.NewBatch()
		b.Set([]byte(fmt.Sprintf("scan-t/%03d", i)), []byte("v"), nil)
		if err := st.Apply(b); err != nil {
			t.Fatal(err)
		}
	}
	for _, limit := range []int{0, -1} {
		got := 0
		err := st.Scan([]byte("scan-t/"), []byte("scan-t0"), limit, func(k, v []byte) (bool, error) {
			got++
			return true, nil
		})
		if err != nil || got != n {
			t.Fatalf("limit=%d: got=%d err=%v（期望全量 %d）", limit, got, err, n)
		}
	}
}
