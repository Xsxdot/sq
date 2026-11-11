// storeopts_test.go: 装配点 config → store.Options 映射的不变式测试（SQ4.5 I3）。
//
// 职责：配置里的 storage 覆盖值穿过装配点原样到达 store.Options；未配置时
// 为零值（= store 缺省），「未配置」与「配置为某值」可区分。
//
// 边界：不测 config 的解析与校验（config 包），不测 store 的缺省解析（store 包）。
package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Xsxdot/sq/internal/config"
	"github.com/Xsxdot/sq/internal/metrics"
	"github.com/Xsxdot/sq/internal/store"
)

func TestStoreOptionsFromConfig(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sq.yaml")
	body := "storage:\n  cache_size: 128MiB\n  memtable_size: 16MiB\n  compaction_concurrency: 2\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// CountPrefixes 不来自配置：装配点恒登记指标用的计数前缀（SQ4.9）
	want := store.Options{CacheSize: 128 << 20, MemTableSize: 16 << 20, CompactionConcurrency: 2,
		CountPrefixes: metrics.CountedPrefixes()}
	if got := storeOptions(cfg); !reflect.DeepEqual(got, want) {
		t.Fatalf("覆盖值未穿过装配点: got %+v want %+v", got, want)
	}

	def, err := config.Load("")
	if err != nil {
		t.Fatalf("Load(\"\"): %v", err)
	}
	if got := storeOptions(def); !reflect.DeepEqual(got, store.Options{CountPrefixes: metrics.CountedPrefixes()}) {
		t.Fatalf("未配置时调优字段应为零值（= store 缺省），得到 %+v", got)
	}
}
