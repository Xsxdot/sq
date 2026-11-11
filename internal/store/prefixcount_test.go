// prefixcount 不变式测试（SQ4.9 计划 C1–C4）：登记前缀的内存计数在任何
// 提交之后都等于对本库前缀全扫的结果。
//
// 边界：只测计数本身（store 包内），不测 metrics 如何使用它。
package store

import (
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
)

var testCountPrefixes = []string{"delay/", "half/"}

func openCounted(t *testing.T, dir string, sync bool) *Store {
	t.Helper()
	s, err := OpenWithOptions(dir, sync, slog.Default(), Options{CountPrefixes: testCountPrefixes})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	return s
}

// scanCount 前缀全扫计数（对照基准）。
func scanCount(t *testing.T, s *Store, prefix string) int64 {
	t.Helper()
	var n int64
	p := []byte(prefix)
	if err := s.Scan(p, PrefixUpperBound(p), 0, func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatalf("scan %s: %v", prefix, err)
	}
	return n
}

func assertCountsMatch(t *testing.T, s *Store, ctx string) {
	t.Helper()
	for _, p := range testCountPrefixes {
		got, ok := s.PrefixCount(p)
		if !ok {
			t.Fatalf("%s: 前缀 %s 未登记", ctx, p)
		}
		if want := scanCount(t, s, p); got != want {
			t.Fatalf("%s: 前缀 %s 计数 %d ≠ 全扫 %d", ctx, p, got, want)
		}
	}
}

// keyUniverse 小键空间（高碰撞）：两个计数族 + 紧邻的 halfidx/（不得计入
// half/）+ 无关键族。
func keyUniverse() [][]byte {
	var ks [][]byte
	for _, fam := range []string{"delay/", "half/", "halfidx/", "other/"} {
		for i := 0; i < 8; i++ {
			ks = append(ks, []byte(fmt.Sprintf("%s%02d", fam, i)))
		}
	}
	return ks
}

// randomBatch 组装一个随机批次：新/旧键 Set、Delete（含不存在的键）、同批
// 先删后写同一键、与计数族相交/不相交的 DeleteRange。
func randomBatch(t *testing.T, s *Store, r *rand.Rand, ks [][]byte) *Batch {
	t.Helper()
	b := s.NewBatch()
	n := 1 + r.Intn(6)
	for i := 0; i < n; i++ {
		k := ks[r.Intn(len(ks))]
		var err error
		switch x := r.Intn(20); {
		case x < 8:
			err = b.Set(k, []byte("v"))
		case x < 15:
			err = b.Delete(k)
		case x < 17: // txn 重排期：同批先删后写
			if err = b.Delete(k); err == nil {
				err = b.Set(k, []byte("v2"))
			}
		case x < 19: // 随机区间（可能跨族、可能落在计数族之外）
			a, c := ks[r.Intn(len(ks))], ks[r.Intn(len(ks))]
			if string(a) > string(c) {
				a, c = c, a
			}
			err = b.DeleteRange(a, append(append([]byte(nil), c...), 0))
		default: // 整族 DeleteRange（快照安装形状）
			p := []byte(testCountPrefixes[r.Intn(2)])
			err = b.DeleteRange(p, PrefixUpperBound(p))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return b
}

// commitRotating 按 i 在全部提交方法之间轮换。
func commitRotating(t *testing.T, s *Store, b *Batch, i int) {
	t.Helper()
	var err error
	switch i % 4 {
	case 0:
		err = s.Apply(b)
	case 1:
		err = s.ApplyWith(b, true)
	case 2:
		err = s.ApplyWith(b, false)
	case 3:
		var p Pending
		if p, err = s.ApplyAsync(b); err == nil {
			err = p.Wait()
		}
	}
	if err != nil {
		t.Fatalf("提交（方法 %d）: %v", i%4, err)
	}
}

// C1：随机操作序列，每次提交后计数 = 全扫；sync 档与 nosync 档的 store 各跑
// （覆盖 ApplyAsync 的两个分支）。
func TestPrefixCountMatchesScanRandom(t *testing.T) {
	for _, syncMode := range []bool{true, false} {
		t.Run(fmt.Sprintf("sync=%v", syncMode), func(t *testing.T) {
			s := openCounted(t, t.TempDir(), syncMode)
			defer s.Close()
			ks := keyUniverse()
			for seed := int64(1); seed <= 5; seed++ {
				r := rand.New(rand.NewSource(seed))
				for i := 0; i < 400; i++ {
					commitRotating(t, s, randomBatch(t, s, r, ks), i)
					assertCountsMatch(t, s, fmt.Sprintf("seed=%d i=%d", seed, i))
				}
			}
		})
	}
}

// C2：8 个 goroutine 并发对重叠键集合提交，其中两个专门重复删同一批键
// （模拟 End 与回查并发双删）；-race 下跑，结束后计数 = 全扫。
func TestPrefixCountConcurrent(t *testing.T) {
	s := openCounted(t, t.TempDir(), false)
	defer s.Close()
	ks := keyUniverse()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(100 + w)))
			for i := 0; i < 300; i++ {
				var b *Batch
				if w < 2 {
					// 双删者：先确保存在（另一 goroutine 也在写），再删同一批键
					b = s.NewBatch()
					for j := 0; j < 4; j++ {
						if err := b.Delete([]byte(fmt.Sprintf("half/%02d", j))); err != nil {
							t.Error(err)
							return
						}
					}
				} else {
					b = randomBatch(t, s, r, ks)
				}
				var err error
				if i%2 == 0 {
					err = s.Apply(b)
				} else {
					var p Pending
					if p, err = s.ApplyAsync(b); err == nil {
						err = p.Wait()
					}
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	assertCountsMatch(t, s, "并发结束")
}

// C3：重开后基数 = 全扫；不触及计数族的批次不取计数锁；未登记前缀 ok=false。
func TestPrefixCountReopenAndNoLockForUntouched(t *testing.T) {
	dir := t.TempDir()
	s := openCounted(t, dir, true)
	b := s.NewBatch()
	for i := 0; i < 50; i++ {
		b.Set([]byte(fmt.Sprintf("delay/%03d", i)), []byte("v"))
		b.Set([]byte(fmt.Sprintf("halfidx/%03d", i)), []byte("v"))
	}
	for i := 0; i < 7; i++ {
		b.Set([]byte(fmt.Sprintf("half/%03d", i)), []byte("v"))
	}
	if err := s.Apply(b); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openCounted(t, dir, true)
	defer s.Close()
	assertCountsMatch(t, s, "重开")
	if n, _ := s.PrefixCount("half/"); n != 7 {
		t.Fatalf("half/ 应为 7（halfidx/ 不计入），得 %d", n)
	}
	if _, ok := s.PrefixCount("msg/"); ok {
		t.Fatal("未登记前缀应返回 ok=false")
	}

	before := s.pc.lockTaken.Load()
	b = s.NewBatch()
	b.Set([]byte("msg/x"), []byte("v"))
	b.Set([]byte("halfidx/zz"), []byte("v"))
	b.DeleteRange([]byte("cursor/"), []byte("cursor0"))
	if err := s.Apply(b); err != nil {
		t.Fatal(err)
	}
	if got := s.pc.lockTaken.Load(); got != before {
		t.Fatalf("不触及计数族的批次取了计数锁（%d→%d）", before, got)
	}
	b = s.NewBatch()
	b.Set([]byte("delay/new"), []byte("v"))
	if err := s.Apply(b); err != nil {
		t.Fatal(err)
	}
	if got := s.pc.lockTaken.Load(); got != before+1 {
		t.Fatalf("触及计数族的批次应取一次计数锁（%d→%d）", before, got)
	}
	assertCountsMatch(t, s, "收尾")

	// 未登记任何前缀的库：读数恒 ok=false，提交路径不受影响
	plain := openTestStore(t, t.TempDir())
	defer plain.Close()
	if _, ok := plain.PrefixCount("delay/"); ok {
		t.Fatal("未登记前缀的库应返回 ok=false")
	}
}

// C4：快照安装形状——同批整族 DeleteRange 后再 Set 新内容。
func TestPrefixCountSnapshotShapedBatch(t *testing.T) {
	s := openCounted(t, t.TempDir(), false)
	defer s.Close()
	b := s.NewBatch()
	for i := 0; i < 30; i++ {
		b.Set([]byte(fmt.Sprintf("delay/%03d", i)), []byte("v"))
		b.Set([]byte(fmt.Sprintf("half/%03d", i)), []byte("v"))
	}
	if err := s.Apply(b); err != nil {
		t.Fatal(err)
	}
	b = s.NewBatch()
	for _, p := range []string{"delay/", "half/", "halfidx/"} {
		b.DeleteRange([]byte(p), PrefixUpperBound([]byte(p)))
	}
	for i := 0; i < 5; i++ {
		b.Set([]byte(fmt.Sprintf("delay/%03d", 100+i)), []byte("v"))
	}
	b.Set([]byte("half/x"), []byte("v"))
	if err := s.ApplyWith(b, false); err != nil {
		t.Fatal(err)
	}
	assertCountsMatch(t, s, "快照式批次")
	if n, _ := s.PrefixCount("delay/"); n != 5 {
		t.Fatalf("delay/ 应为 5，得 %d", n)
	}
	// 之后的分块写入（逐块 Set）继续按存在性计数
	b = s.NewBatch()
	b.Set([]byte("delay/104"), []byte("dup")) // 已存在：不加
	b.Set([]byte("delay/200"), []byte("v"))
	if err := s.ApplyWith(b, false); err != nil {
		t.Fatal(err)
	}
	assertCountsMatch(t, s, "快照后分块")
}
