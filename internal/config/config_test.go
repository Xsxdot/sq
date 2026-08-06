package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("") // 空路径 = 全默认值
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.GRPCListen != ":8081" || cfg.DataDir != "./data" ||
		cfg.Fsync != "sync" || !cfg.AutoCreateTopic || cfg.DefaultQueueNums != 4 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadYAMLOverride(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sq.yaml")
	os.WriteFile(p, []byte("grpc_listen: \":9081\"\nfsync: async\n"), 0o644)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.GRPCListen != ":9081" || cfg.Fsync != "async" {
		t.Fatalf("override not applied: %+v", cfg)
	}
	if cfg.DataDir != "./data" { // 未覆盖字段保持默认
		t.Fatalf("default lost: %+v", cfg)
	}
}
