// Package config 提供 sq 的配置加载测试。
//
// 职责：
//   - 验证 Config 默认值正确性
//   - 验证 YAML 文件覆盖默认值的行为
//   - 确保配置加载不产生预期外的错误
//   - 验证 default_queue_nums 的上下界校验：配置笔误必须在启动时挡住，
//     不能等到运行时伪装成 broker 故障或负数队列 id
//
// 边界：
//   - 不测试业务语义校验（如端口合法性）
//   - 不测试文件系统异常情况
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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
	if err := os.WriteFile(p, []byte("grpc_listen: \":9081\"\nfsync: async\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
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

// TestLoadRejectsOutOfRangeQueueNums 锁定 default_queue_nums 的两端边界。
//
// 下界（0）：漏填或写成 0 时，每一次 EnsureTopic 都会失败，而这个失败会一路
// 翻译成客户端看到的 INTERNAL_SERVER_ERROR——一个 yaml 笔误从此伪装成
// "broker 坏了"。上界：队列 id 在路由响应里是 int32(i)，超过 int32 范围后
// 客户端会收到一批负数 id 的队列。两种都必须在进程起来之前就被挡住。
func TestLoadRejectsOutOfRangeQueueNums(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"零值", "default_queue_nums: 0\n"},
		{"超出上限", "default_queue_nums: 3000000000\n"},
		{"恰好越过上限", "default_queue_nums: 1025\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "sq.yaml")
			if err := os.WriteFile(p, []byte(tc.yaml), 0o644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			if _, err := Load(p); err == nil {
				t.Fatalf("非法 default_queue_nums 应在 Load 阶段报错，配置：%s", tc.yaml)
			}
		})
	}
}

// TestLoadRejectsBadLogLevel 非法 log_level 必须在启动时报错：现状 SetupSlog
// 静默降级为 info，一个拼写错误（如 verbose）会让 debug 日志无声消失，
// 与同文件 fsync/default_queue_nums 的严格校验风格也不一致。
func TestLoadRejectsBadLogLevel(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sq.yaml")
	if err := os.WriteFile(p, []byte("log_level: verbose\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err == nil {
		t.Fatal("期望拒绝非法 log_level")
	}
}

// TestLoadAcceptsBoundaryQueueNums 与上一条互补：边界值本身必须放行，
// 免得校验写成了排他区间。
func TestLoadAcceptsBoundaryQueueNums(t *testing.T) {
	for _, n := range []uint32{1, MaxDefaultQueueNums} {
		p := filepath.Join(t.TempDir(), "sq.yaml")
		if err := os.WriteFile(p, fmt.Appendf(nil, "default_queue_nums: %d\n", n), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		cfg, err := Load(p)
		if err != nil {
			t.Fatalf("default_queue_nums=%d 应被接受: %v", n, err)
		}
		if cfg.DefaultQueueNums != n {
			t.Fatalf("default_queue_nums=%d 未生效: %+v", n, cfg)
		}
	}
}

func TestDefaultMaxAttempts(t *testing.T) {
	cfg, err := Load("")
	if err != nil || cfg.DefaultMaxAttempts != 16 {
		t.Fatalf("默认 max attempts: %d %v", cfg.DefaultMaxAttempts, err)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	os.WriteFile(p, []byte("default_max_attempts: 0\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("应拒绝 default_max_attempts=0")
	}
}

func TestDiskWatermark(t *testing.T) {
	cfg, err := Load("")
	if err != nil || cfg.DiskWatermarkPercent != 85 {
		t.Fatalf("默认水位: %d %v", cfg.DiskWatermarkPercent, err)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	os.WriteFile(p, []byte("disk_watermark_percent: 120\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("应拒绝 >99 的水位")
	}
}

func TestRetentionInterval(t *testing.T) {
	cfg, err := Load("")
	if err != nil || cfg.RetentionInterval() != 5*time.Minute {
		t.Fatalf("默认 retention 间隔: %v %v", cfg.RetentionCheckInterval, err)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	os.WriteFile(p, []byte("retention_check_interval: nonsense\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("应拒绝非法 interval")
	}
}

// TestLoadAuthPairValidation 认证配置必须成对：只填一半是笔误，启动即报错，
// 不能静默变成"看起来配了认证实际没生效"。
func TestLoadAuthPairValidation(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"只有access_key", "access_key: ak\n"},
		{"只有secret_key", "secret_key: sk\n"},
		{"只有admin_username", "admin_username: root\n"},
		{"只有admin_password", "admin_password: pw\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "sq.yaml")
			if err := os.WriteFile(p, []byte(c.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(p); err == nil {
				t.Fatalf("半配置 %q 应报错", c.yaml)
			}
		})
	}
}

// TestLoadAuthDefaults 默认值：认证全关、admin 监听 :8082；成对配置能被加载。
func TestLoadAuthDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccessKey != "" || cfg.SecretKey != "" || cfg.AdminUsername != "" || cfg.AdminPassword != "" {
		t.Fatalf("认证默认应全空: %+v", cfg)
	}
	if cfg.AdminListen != ":8082" {
		t.Fatalf("admin_listen 默认应为 :8082，得到 %q", cfg.AdminListen)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	y := "access_key: ak\nsecret_key: sk\nadmin_username: root\nadmin_password: pw\nadmin_listen: \"\"\n"
	if err := os.WriteFile(p, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccessKey != "ak" || cfg.AdminUsername != "root" || cfg.AdminListen != "" {
		t.Fatalf("成对配置加载不符: %+v", cfg)
	}
}

func TestMetricsRetentionHours(t *testing.T) {
	// 默认值：7 天。改这个默认值等于改控制台历史曲线能回看多久，测试钉住它。
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load 默认配置: %v", err)
	}
	if cfg.MetricsRetentionHours != 168 {
		t.Fatalf("默认 metrics_retention_hours 应为 168，得到 %d", cfg.MetricsRetentionHours)
	}

	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "c.yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("写配置文件: %v", err)
		}
		return p
	}

	// 0 = 只保留内存环、不落库，是合法取值而不是笔误
	cfg, err = Load(write("metrics_retention_hours: 0\n"))
	if err != nil {
		t.Fatalf("0 应被接受: %v", err)
	}
	if cfg.MetricsRetentionHours != 0 {
		t.Fatalf("应为 0，得到 %d", cfg.MetricsRetentionHours)
	}

	for _, bad := range []string{"-1", "8761"} {
		if _, err := Load(write("metrics_retention_hours: " + bad + "\n")); err == nil {
			t.Fatalf("metrics_retention_hours=%s 应被拒绝", bad)
		}
	}
}

func TestTxnConfigDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TxnCheckInterval != "30s" || cfg.TxnInterval() != 30*time.Second {
		t.Fatalf("txn_check_interval 默认值错误: %q", cfg.TxnCheckInterval)
	}
	if cfg.TxnMaxChecks != 15 {
		t.Fatalf("txn_max_checks 默认值错误: %d", cfg.TxnMaxChecks)
	}
}

func TestTxnConfigValidation(t *testing.T) {
	// 写盘一个坏配置再 Load，风格对齐既有校验用例
	for name, body := range map[string]string{
		"负间隔": "txn_check_interval: \"-1s\"",
		"零间隔": "txn_check_interval: \"0s\"",
		"非法串": "txn_check_interval: \"abc\"",
		"零次数": "txn_max_checks: 0",
		"负次数": "txn_max_checks: -3",
	} {
		p := filepath.Join(t.TempDir(), "sq.yaml")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(p); err == nil {
			t.Fatalf("%s 应在启动时报错", name)
		}
	}
}
