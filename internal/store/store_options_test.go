// store_options_test.go: Pebble 调优参数（SQ4.5）的不变式测试。
//
// 职责：
//   - I1：零值 Options 解析为本包缺省常量、显式值原样透传——经「store 已打开」
//     日志字段断言，穿过 OpenWithOptions → pebble.Open 的真实组装链
//   - I2：布隆过滤器真的写进 SST 并在点查中生效（Filter.Hits > 0）
//
// 边界：
//   - 不测缓存命中率与性能（由 deliver/produce 基准承担）
//   - 不测 config 解析（config 包 TestStorageConfig）
package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
	"testing"
)

// openedRecord 打开一个库并返回「store 已打开」日志记录的字段。
func openedRecord(t *testing.T, opts Options) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	lg := slog.New(slog.NewJSONHandler(&buf, nil))
	s, err := OpenWithOptions(t.TempDir(), false, lg, opts)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer s.Close()
	for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
		var rec map[string]any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber() // 字节数按整数原样比较，避免 float64 的科学计数格式
		if dec.Decode(&rec) == nil && rec["msg"] == "store 已打开" {
			return rec
		}
	}
	t.Fatalf("未找到「store 已打开」日志，全部输出:\n%s", buf.String())
	return nil
}

// TestOptionsResolveDefaultsAndPassThrough I1：零值 = 缺省常量，显式值透传。
// 「未配置」与「配置为某值」必须可区分，否则用户调参静默不生效。
func TestOptionsResolveDefaultsAndPassThrough(t *testing.T) {
	num := func(rec map[string]any, k string) string { return fmt.Sprint(rec[k]) }

	rec := openedRecord(t, Options{})
	wantCC := min(DefaultMaxCompactionConcurrency, runtime.NumCPU())
	if num(rec, "cache_size") != fmt.Sprint(DefaultCacheSize) ||
		num(rec, "memtable_size") != fmt.Sprint(DefaultMemTableSize) ||
		num(rec, "compaction_concurrency") != fmt.Sprint(wantCC) {
		t.Fatalf("零值 Options 应解析为缺省 cache=%d memtable=%d cc=%d，日志为 %v",
			DefaultCacheSize, DefaultMemTableSize, wantCC, rec)
	}
	// 缺省缓存必须远大于 pebble 出厂 8MiB——退回出厂值正是本卡要修的问题
	if DefaultCacheSize <= 8<<20 {
		t.Fatalf("DefaultCacheSize=%d 不应退回 pebble 出厂 8MiB", DefaultCacheSize)
	}

	rec = openedRecord(t, Options{CacheSize: 16 << 20, MemTableSize: 2 << 20, CompactionConcurrency: 3})
	if num(rec, "cache_size") != fmt.Sprint(16<<20) ||
		num(rec, "memtable_size") != fmt.Sprint(2<<20) ||
		num(rec, "compaction_concurrency") != "3" {
		t.Fatalf("显式值应原样透传，日志为 %v", rec)
	}
}

// TestBloomFilterEffectiveOnGet I2：布隆过滤器写进 SST 且在点查中生效。
//
// 写一批键 → Flush 落成 L0 SST → 点查一批不存在（但落在 SST 键区间内）的
// 键 → Filter.Hits 必须 > 0。去掉 FilterPolicy 设置时 SST 里没有过滤块，
// Hits 恒为 0，本用例失败。键区间要覆盖查询键，否则 Pebble 靠文件边界就
// 跳过了该 SST，根本走不到过滤器。
func TestBloomFilterEffectiveOnGet(t *testing.T) {
	s, err := OpenWithOptions(t.TempDir(), false, slog.Default(), Options{})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer s.Close()
	b := s.NewBatch()
	for i := 0; i < 1000; i += 2 { // 只写偶数键，奇数键在区间内但不存在
		if err := b.Set([]byte(fmt.Sprintf("k%06d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Apply(b); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := s.db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := 1; i < 1000; i += 2 {
		if _, ok, err := s.Get([]byte(fmt.Sprintf("k%06d", i))); err != nil || ok {
			t.Fatalf("Get 奇数键 %d: ok=%v err=%v", i, ok, err)
		}
	}
	m := s.db.Metrics()
	if m.Filter.Hits == 0 {
		t.Fatalf("布隆过滤器未生效：Filter.Hits=0 Misses=%d", m.Filter.Misses)
	}
	t.Logf("Filter.Hits=%d Misses=%d", m.Filter.Hits, m.Filter.Misses)
}
