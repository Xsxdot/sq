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

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
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

// TestBatchTouchesPrefix 验证复制批次字节的键前缀判定：follower 侧盲 apply
// 前用它判断批次是否触及某键族（如 meta/），是 meta 缓存重载钩子的判定件。
func TestBatchTouchesPrefix(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	defer st.Close()
	b := st.NewBatch()
	_ = b.Set([]byte("msg/t/0/1"), []byte("v"))
	_ = b.Set([]byte("meta/topic/t"), []byte("v"))
	repr := append([]byte(nil), b.Repr()...)
	_ = b.Close()
	if ok, _ := BatchTouchesPrefix(repr, []byte("meta/")); !ok {
		t.Fatal("应命中 meta/ 前缀")
	}
	if ok, _ := BatchTouchesPrefix(repr, []byte("half/")); ok {
		t.Fatal("不应命中 half/")
	}
}

// TestNewBatchFromReprOwnsCopy 回归验证 NewBatchFromRepr 必须拷贝输入字节：
// pebble 的 SetRepr 是零拷贝接管（b.data 直接指向传入切片），而批次在
// ApplyWith 提交后会 Close 回收到 batch sync.Pool，池中批次连同 b.data
// 一起被下一次 NewBatch 复用。若直接传调用方的切片，重建批次与调用方
// 共享同一块内存——集群 apply 路径传的是 raft 日志条目自身的 Data 缓冲，
// 之后任何 raftstore.Persist 复用池中批次就会原地覆盖日志条目，把日志
// 写花（三节点 e2e 复现的损坏，见 store.go NewBatchFromRepr 注释）。
//
// 本测试：重建批次后再修改源字节，验证批次内容不受影响（拷贝成立）。
func TestNewBatchFromReprOwnsCopy(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	defer st.Close()
	b := st.NewBatch()
	_ = b.Set([]byte("k1"), []byte("v1"))
	repr := append([]byte(nil), b.Repr()...)
	_ = b.Close()

	rb, err := st.NewBatchFromRepr(repr)
	if err != nil {
		t.Fatalf("NewBatchFromRepr: %v", err)
	}
	// 篡改源字节：若批次与源共享缓冲，apply 出的会是篡改后的内容
	for i := range repr {
		repr[i] = 0xAA
	}
	if err := st.Apply(rb); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	v, ok, err := st.Get([]byte("k1"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get 应读到原始值 v1，got %q ok=%v err=%v", v, ok, err)
	}
}

// TestSyncWALPersistsNoSyncWrites 断电语义测试：NoSync 写完之后调一次
// SyncWAL，模拟掉电（丢弃一切未 fsync 的字节）后数据必须一条不少。
//
// 这条用例是 mem 档「周期刷盘」这一整套持久性承诺的地基。此前 flusher 用
// 「空批次 + pebble.Sync」实现，而 Pebble 的 commitPipeline.Commit 开头就是
// `if b.Empty() { return nil }`——空批次根本不进 WAL、更不 fsync，周期刷盘
// 是彻底的空操作，mem 档实际上从不 fsync。用 vfs.NewCrashableMem 的
// CrashClone（零值配置 = 只保留已 fsync 的字节）能把这个洞钉死在单测里。
func TestSyncWALPersistsNoSyncWrites(t *testing.T) {
	const n = 200

	// 写入 n 条 NoSync，然后按 barrier 参数决定是否做一次 WAL 屏障，
	// 最后取崩溃克隆重开，返回幸存条数。
	survivorsAfterCrash := func(t *testing.T, barrier bool) int {
		t.Helper()
		fs := vfs.NewCrashableMem()
		db, err := pebble.Open("/db", &pebble.Options{FS: fs})
		if err != nil {
			t.Fatalf("打开 pebble 失败: %v", err)
		}
		s := &Store{db: db, sync: false, logger: slog.Default()}
		for i := 0; i < n; i++ {
			b := s.NewBatch()
			if err := b.Set([]byte(fmt.Sprintf("k%06d", i)), []byte("v")); err != nil {
				t.Fatalf("组装批次失败: %v", err)
			}
			if err := s.ApplyWith(b, false); err != nil {
				t.Fatalf("NoSync 提交失败: %v", err)
			}
		}
		if barrier {
			if err := s.SyncWAL(); err != nil {
				t.Fatalf("SyncWAL 失败: %v", err)
			}
		}
		// 掉电：克隆只保留已 fsync 的字节；原库不再使用，直接丢弃
		crashed := fs.CrashClone(vfs.CrashCloneCfg{})
		_ = db.Close()

		db2, err := pebble.Open("/db", &pebble.Options{FS: crashed})
		if err != nil {
			t.Fatalf("崩溃后重开 pebble 失败: %v", err)
		}
		defer db2.Close()
		s2 := &Store{db: db2, sync: false, logger: slog.Default()}
		got := 0
		for i := 0; i < n; i++ {
			if _, ok, err := s2.Get([]byte(fmt.Sprintf("k%06d", i))); err != nil {
				t.Fatalf("崩溃后读取失败: %v", err)
			} else if ok {
				got++
			}
		}
		return got
	}

	if got := survivorsAfterCrash(t, true); got != n {
		t.Fatalf("SyncWAL 之后掉电，幸存 %d 条，要求 %d 条一条不少", got, n)
	}
	// 反向对照：不加屏障必须真的会丢，否则上面那条断言是被 Pebble 的
	// 自发刷盘蒙对的，证明不了 SyncWAL 起了作用。
	if got := survivorsAfterCrash(t, false); got == n {
		t.Fatalf("无屏障时也一条不丢（%d 条），本用例失去判别力——请检查写入量是否够撑过一个 WAL block", got)
	}
}

// TestBatchMergeRepr 验证把复制批次字节合并进已有批次：合并后一次提交，
// 两侧写入都落库。这是集群 apply 合批（一轮 Ready 的多条 CommittedEntries
// 合成单次引擎提交）的存储层地基。
//
// 同时断言合并是值拷贝：合并后篡改源字节不得影响批次内容——集群路径
// 传入的是 raft 日志条目自身的 Data 缓冲，条目在 MemoryStorage 里长期
// 存活，任何共享都会重演 NewBatchFromRepr 注释里的日志写花事故。
func TestBatchMergeRepr(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	defer st.Close()
	src := st.NewBatch()
	_ = src.Set([]byte("k-merged"), []byte("v-merged"))
	repr := append([]byte(nil), src.Repr()...)
	_ = src.Close()

	dst := st.NewBatch()
	_ = dst.Set([]byte("k-own"), []byte("v-own"))
	if err := dst.MergeRepr(repr); err != nil {
		t.Fatalf("MergeRepr: %v", err)
	}
	// 篡改源字节：若合并与源共享缓冲，提交出的会是篡改后的内容
	for i := range repr {
		repr[i] = 0xEE
	}
	if err := st.Apply(dst); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if v, ok, err := st.Get([]byte("k-merged")); err != nil || !ok || string(v) != "v-merged" {
		t.Fatalf("Get(k-merged) = %q,%v,%v; want v-merged,true,nil", v, ok, err)
	}
	if v, ok, err := st.Get([]byte("k-own")); err != nil || !ok || string(v) != "v-own" {
		t.Fatalf("Get(k-own) = %q,%v,%v; want v-own,true,nil", v, ok, err)
	}
}

// TestBatchMergeReprRejectsGarbage 坏字节必须在合并时报错——与
// NewBatchFromRepr 同边界：复制链路上的损坏在最早的边界被拦下，
// 不进提交路径。
func TestBatchMergeReprRejectsGarbage(t *testing.T) {
	st := openTestStore(t, t.TempDir())
	defer st.Close()
	dst := st.NewBatch()
	defer dst.Close()
	if err := dst.MergeRepr([]byte("not-a-batch")); err == nil {
		t.Fatal("坏批次字节应报错，得到 nil")
	}
}
