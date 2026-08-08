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
	b.Set([]byte("k1"), []byte("v1"))
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
		b.Set([]byte(k), []byte("x"))
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
	b.Set([]byte("k1"), []byte("v1"))
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
	b.Set([]byte("k2"), []byte("v2"))
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
				b.Set([]byte(key), []byte("x"))
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

// TestTypedBatchRoundTrip 锁定类型化 Batch 的最小 API 面：
// Set/Delete/DeleteRange 经 Apply 提交后语义与裸 pebble 批次一致。
func TestTypedBatchRoundTrip(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	defer st.Close()
	b := st.NewBatch()
	if err := b.Set([]byte("tb/k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("tb/k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("tb/k2")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := st.Get([]byte("tb/k1")); !ok || string(v) != "v1" {
		t.Fatalf("k1 = %q, ok=%v, want v1", v, ok)
	}
	if _, ok, _ := st.Get([]byte("tb/k2")); ok {
		t.Fatal("k2 应已被批内 Delete 删除")
	}

	// DeleteRange 走 ApplyAsync+Wait 路径，顺带锁定拆分提交同样接受类型化批次
	b2 := st.NewBatch()
	if err := b2.DeleteRange([]byte("tb/"), []byte("tb0")); err != nil {
		t.Fatal(err)
	}
	pending, err := st.ApplyAsync(b2)
	if err != nil {
		t.Fatal(err)
	}
	if err := pending.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.Get([]byte("tb/k1")); ok {
		t.Fatal("k1 应已被 DeleteRange 删除")
	}
}

func TestScanNonPositiveLimitMeansUnlimited(t *testing.T) {
	// limit<=0 == 不限量：该语义被 deliver 阶段 2 的跳过逻辑直接依赖（B5）
	st := openTestStore(t, t.TempDir()) // 文件内既有 helper（store_test.go:19）
	defer st.Close()
	const n = 10
	for i := 0; i < n; i++ {
		b := st.NewBatch()
		b.Set([]byte(fmt.Sprintf("scan-t/%03d", i)), []byte("v"))
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

// TestBatchReprRoundTrip 验证复制载荷的往返一致性：leader 侧组装的批次
// 导出字节，follower 侧重建并 apply 后，数据逐键一致——这是「复制物理
// batch 字节而非逻辑命令」（V2 spec §4）的最小正确性锚点。
func TestBatchReprRoundTrip(t *testing.T) {
	src := openTestStore(t, t.TempDir())
	dst := openTestStore(t, t.TempDir())
	b := src.NewBatch()
	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("k-absent")); err != nil {
		t.Fatal(err)
	}
	repr := append([]byte(nil), b.Repr()...) // Repr 底层内存归批次所有，拷贝后再提交
	if err := src.Apply(b); err != nil {
		t.Fatal(err)
	}
	rb, err := dst.NewBatchFromRepr(repr)
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.Apply(rb); err != nil {
		t.Fatal(err)
	}
	v, ok, err := dst.Get([]byte("k1"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("follower 侧 apply 后 Get(k1) = %q,%v,%v; want v1,true,nil", v, ok, err)
	}
}

// TestNewBatchFromReprRejectsGarbage 坏字节必须在重建时报错，而不是
// apply 时 panic——复制链路上的损坏要在最早的边界被拦下。
func TestNewBatchFromReprRejectsGarbage(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	if _, err := st.NewBatchFromRepr([]byte("not-a-batch")); err == nil {
		t.Fatal("坏批次字节应报错，得到 nil")
	}
}

// TestApplyWithOverridesSync ApplyWith 的 sync 参数独立于 Store 全局档位。
// 行为断言只能到「提交成功且可读」——fsync 是否真实发生无法在单测观测，
// 由集成层（cluster）的两档吞吐差异间接验证。
func TestApplyWithOverridesSync(t *testing.T) {
	// 全局档位显式开成 false：若用 openTestStore（sync=true）则
	// ApplyWith(b, true) ≡ Apply，测不出「参数覆盖全局档位」这一维度。
	st, err := Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	b := st.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := st.ApplyWith(b, true); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.Get([]byte("k")); !ok {
		t.Fatal("ApplyWith 提交后应可读")
	}
}
