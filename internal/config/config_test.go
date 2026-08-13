// Package config 提供 sq 的配置加载测试。
//
// 职责：
//   - 验证 Config 默认值正确性
//   - 验证 YAML 文件覆盖默认值的行为
//   - 确保配置加载不产生预期外的错误
//   - 验证 default_queue_nums 的上下界校验：配置笔误必须在启动时挡住，
//     不能等到运行时伪装成 broker 故障或负数队列 id
//   - 验证 cluster 段的解析与校验：node_id 归属、peer 唯一性、ack 档位、
//     单机回退（Cluster==nil）
//
// 边界：
//   - 不测试业务语义校验（如端口合法性）
//   - 不测试文件系统异常情况
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("") // 空路径 = 全默认值
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.GRPCListen != ":8081" || cfg.DataDir != "./data" ||
		cfg.Fsync != "sync" || !cfg.AutoCreateTopic || cfg.DefaultQueueNums != 16 {
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

// TestLoadAuthPairValidation 管理面认证配置必须成对：只填一半是笔误，启动即报错，
// 不能静默变成"看起来配了认证实际没生效"。gRPC 凭据的成对校验已由
// TestLoadCredentials 覆盖，此处只保留 admin 一项。
func TestLoadAuthPairValidation(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
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

// TestLoadAuthDefaults 默认值：gRPC 凭据列表为空（不鉴权）、admin 监听 :8082；
// 成对配置能被加载。
func TestLoadAuthDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Credentials) != 0 || cfg.AdminUsername != "" || cfg.AdminPassword != "" {
		t.Fatalf("认证默认应全空: %+v", cfg)
	}
	if cfg.AdminListen != ":8082" {
		t.Fatalf("admin_listen 默认应为 :8082，得到 %q", cfg.AdminListen)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	y := "credentials:\n  - access_key: ak\n    secret_key: sk\nadmin_username: root\nadmin_password: pw\nadmin_listen: \"\"\n"
	if err := os.WriteFile(p, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Credentials) != 1 || cfg.Credentials[0].AccessKey != "ak" || cfg.AdminUsername != "root" || cfg.AdminListen != "" {
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

// TestLoadCredentials 多凭据列表的成对非空与 AK 唯一性校验：
// 空列表 = 关闭鉴权；缺一半、AK 重复都是启动期笔误，必须挡住。
func TestLoadCredentials(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr bool
		wantN   int
	}{
		{"空列表=关闭", "", false, 0},
		{"两条合法", "credentials:\n  - name: 订单服务\n    access_key: AK1\n    secret_key: SK1\n  - access_key: AK2\n    secret_key: SK2\n", false, 2},
		{"缺 secret_key", "credentials:\n  - access_key: AK1\n", true, 0},
		{"缺 access_key", "credentials:\n  - secret_key: SK1\n", true, 0},
		{"AK 重复", "credentials:\n  - access_key: AK1\n    secret_key: a\n  - access_key: AK1\n    secret_key: b\n", true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "sq.yaml")
			if err := os.WriteFile(p, []byte(c.yaml), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(p)
			if c.wantErr {
				if err == nil {
					t.Fatal("期望校验失败，却通过了")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(cfg.Credentials) != c.wantN {
				t.Fatalf("凭据数 = %d, 期望 %d", len(cfg.Credentials), c.wantN)
			}
		})
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

// TestLoadRejectsLegacyScalarKeys 旧 access_key/secret_key 标量必须硬报错并带迁移
// 提示——yaml 静默忽略会让升级的运维无声回到不鉴权（安全静默降级，v1.0 前必修）。
func TestLoadRejectsLegacyScalarKeys(t *testing.T) {
	cases := map[string]string{
		"只有 access_key": "access_key: AK1\n",
		"只有 secret_key": "secret_key: SK1\n",
		"两个都有":          "access_key: AK1\nsecret_key: SK1\n",
	}
	for name, y := range cases {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "sq.yaml")
			if err := os.WriteFile(p, []byte(y), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := Load(p)
			if err == nil {
				t.Fatalf("%s: 应拒绝启动", name)
			}
			if !strings.Contains(err.Error(), "credentials") {
				t.Fatalf("%s: 错误应带 credentials 迁移提示，得到 %v", name, err)
			}
		})
	}
}

// TestLoadStrictRejectsTypoField 未知键名（笔误）必须被严格解析挡住。
func TestLoadStrictRejectsTypoField(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sq.yaml")
	if err := os.WriteFile(p, []byte("auto_crete_topic: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err == nil {
		t.Fatal("键名笔误应被严格解析拒绝")
	}
}

// TestLoadStrictAcceptsExampleConfig 仓库自带的 sq.example.yaml 必须与严格 schema
// 一致（它是用户照抄的模板，任何漂移都等于把错误教给用户）。
func TestLoadStrictAcceptsExampleConfig(t *testing.T) {
	cfg, err := Load("../../sq.example.yaml")
	if err != nil {
		t.Fatalf("sq.example.yaml 应通过严格解析: %v", err)
	}
	if len(cfg.Credentials) != 0 {
		t.Fatalf("示例配置默认应空凭据（不鉴权）: %+v", cfg.Credentials)
	}
}

// TestLoadEmptyOrCommentOnlyFile 空文件/纯注释文件仍按无配置处理（默认值），
// yaml.Decoder 对空输入返回 io.EOF，必须显式放过，不能把合法的空配置当错误。
func TestLoadEmptyOrCommentOnlyFile(t *testing.T) {
	for _, body := range []string{"", "# 只有注释\n"} {
		p := filepath.Join(t.TempDir(), "sq.yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(p)
		if err != nil {
			t.Fatalf("内容 %q 应视为空配置: %v", body, err)
		}
		if cfg.DataDir != "./data" {
			t.Fatalf("空配置应保留默认值，得到 %q", cfg.DataDir)
		}
	}
}

// TestClusterConfigParsing 集群段的全量解析与校验：
// 合法配置解析出 node_id/peers/默认档；四类笔误（node_id 不在成员表、
// peer id 重复、ack 非法、空 peers）启动即拒；不带 cluster 段 = 单机模式。
func TestClusterConfigParsing(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "sq.yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := `
cluster:
  node_id: 2
  raft_listen: ":9081"
  peers:
    - id: 1
      raft_addr: "10.0.0.1:9081"
      advertise_host: "10.0.0.1"
      advertise_port: 8081
    - id: 2
      raft_addr: "10.0.0.2:9081"
      advertise_host: "10.0.0.2"
      advertise_port: 8081
    - id: 3
      raft_addr: "10.0.0.3:9081"
      advertise_host: "10.0.0.3"
      advertise_port: 8081
`
	cfg, err := Load(write(base))
	if err != nil {
		t.Fatalf("合法集群配置被拒: %v", err)
	}
	if !cfg.ClusterEnabled() || cfg.Cluster.NodeID != 2 {
		t.Fatalf("集群段解析错误: %+v", cfg.Cluster)
	}
	if cfg.Cluster.DataGroups != 3 || cfg.Cluster.Ack != "quorum-mem" {
		t.Fatalf("默认值错误: groups=%d ack=%q", cfg.Cluster.DataGroups, cfg.Cluster.Ack)
	}
	// 拒绝路径：node_id 不在 peers 里 / peers id 重复 / ack 非法 / 单机配置 Cluster==nil
	for name, body := range map[string]string{
		"node_id 不在 peers": strings.Replace(base, "node_id: 2", "node_id: 9", 1),
		"peers id 重复":      strings.Replace(base, "- id: 3", "- id: 1", 1),
		"ack 非法":           base + "  ack: fsync-everything\n",
		"peers 少于 1":       "cluster:\n  node_id: 1\n  raft_listen: \":9081\"\n  peers: []\n",
	} {
		if _, err := Load(write(body)); err == nil {
			t.Errorf("%s: 应拒绝，实际通过", name)
		}
	}
	cfg2, err := Load("")
	if err != nil || cfg2.ClusterEnabled() {
		t.Fatalf("空配置应为单机: err=%v cluster=%v", err, cfg2.Cluster)
	}
}

// TestClusterHelpers 成员表辅助方法：PeerRaftAddrs 展开 id→raft 地址映射，
// AdvertiseOf 查成员对外广告地址（id 不在表内 = ok=false）。
// Task 9/11 装配 raft 组配置与路由广播依赖这两个方法的语义。
func TestClusterHelpers(t *testing.T) {
	y := `
cluster:
  node_id: 1
  raft_listen: ":9081"
  data_groups: 8
  peers:
    - id: 1
      raft_addr: "10.0.0.1:9081"
      advertise_host: "10.0.0.1"
      advertise_port: 8081
    - id: 2
      raft_addr: "10.0.0.2:9081"
      advertise_host: "10.0.0.2"
      advertise_port: 8081
`
	p := filepath.Join(t.TempDir(), "sq.yaml")
	if err := os.WriteFile(p, []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	addrs := cfg.Cluster.PeerRaftAddrs()
	if len(addrs) != 2 || addrs[1] != "10.0.0.1:9081" || addrs[2] != "10.0.0.2:9081" {
		t.Fatalf("PeerRaftAddrs 展开错误: %v", addrs)
	}
	if host, port, ok := cfg.Cluster.AdvertiseOf(2); !ok || host != "10.0.0.2" || port != 8081 {
		t.Fatalf("AdvertiseOf(2) 错误: %q:%d ok=%v", host, port, ok)
	}
	if _, _, ok := cfg.Cluster.AdvertiseOf(99); ok {
		t.Fatal("AdvertiseOf(99) 应返回 ok=false")
	}
}

// loadClusterConfig 从 YAML 文档加载配置并断言成功：文档原样写入
// （cluster 段自带完整结构），Load 后返回配置。
func loadClusterConfig(t *testing.T, yamlBody string) *Config {
	t.Helper()
	cfg, err := loadYAML(t, yamlBody)
	if err != nil {
		t.Fatalf("Load 集群配置: %v", err)
	}
	return cfg
}

// loadClusterConfigErr 把 2 空格缩进的 cluster 段字段片段嵌入一个最小
// 合法集群配置后 Load，返回 (cfg, err)——上界校验类用例只关心 err。
func loadClusterConfigErr(t *testing.T, fragment string) (*Config, error) {
	t.Helper()
	body := "cluster:\n  node_id: 1\n  raft_listen: \":9081\"\n  peers:\n    - {id: 1, raft_addr: \"127.0.0.1:9081\", advertise_host: \"127.0.0.1\", advertise_port: 8081}\n" + fragment
	return loadYAML(t, body)
}

// loadYAML 把配置体写入临时文件后 Load。
func loadYAML(t *testing.T, body string) (*Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "sq.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

// TestMessageEncodingDefault 缺省必须是 json：binary 只能显式开启。
//
// 为什么默认值本身值得钉一条用例：这个默认值是「升级不改配置就零风险」
// 的全部依据。若某次重构把默认翻成 binary，升级后混版集群里旧节点立刻
// 解不开新节点转发的消息，而单元测试全绿——只有直接断言默认值拦得住。
func TestMessageEncodingDefault(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MessageEncoding != "json" {
		t.Fatalf("message_encoding 默认值 = %q, 应为 json", cfg.MessageEncoding)
	}
}

// TestMessageEncodingOverrideAndValidation 覆盖生效 + 非法值启动即拒。
//
// 非法值必须挡在启动期：静默回落默认档会让运维以为 binary 已生效，
// 而盘上格式与预期不符是事后极难察觉的偏差（数据没坏，只是没省下）。
func TestMessageEncodingOverrideAndValidation(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "sq.yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		return p
	}

	cfg, err := Load(write(t, "message_encoding: binary\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MessageEncoding != "binary" {
		t.Fatalf("覆盖未生效: %q", cfg.MessageEncoding)
	}

	if _, err := Load(write(t, "message_encoding: protobuf\n")); err == nil {
		t.Fatal("非法 message_encoding 应拒绝启动")
	} else if !strings.Contains(err.Error(), "message_encoding") {
		t.Fatalf("错误信息未指明配置项: %v", err)
	}
}

// TestClusterTruncationConfigDefaults 截断循环与快照分块三项配置的默认
// 值与上界校验：缺省即 10000/30s/4MiB；分块必须 < 16MiB 传输层帧上限，
// 否则整份快照永远发不出去。
func TestClusterTruncationConfigDefaults(t *testing.T) {
	cfg := loadClusterConfig(t, `
cluster:
  node_id: 1
  raft_listen: ":9081"
  peers:
    - {id: 1, raft_addr: "127.0.0.1:9081", advertise_host: "127.0.0.1", advertise_port: 8081}
`)
	if cfg.Cluster.LogRetainEntries != 10000 {
		t.Fatalf("log_retain_entries 默认 = %d; want 10000", cfg.Cluster.LogRetainEntries)
	}
	if cfg.Cluster.TruncateInterval != 30*time.Second {
		t.Fatalf("truncate_interval 默认 = %v; want 30s", cfg.Cluster.TruncateInterval)
	}
	if cfg.Cluster.SnapshotChunkBytes != 4<<20 {
		t.Fatalf("snapshot_chunk_bytes 默认 = %d; want 4MiB", cfg.Cluster.SnapshotChunkBytes)
	}
	if cfg.Cluster.SnapshotViewTTL != 5*time.Minute {
		t.Fatalf("snapshot_view_ttl 默认 = %v; want 5m", cfg.Cluster.SnapshotViewTTL)
	}
	// 上界守卫：分块必须小于传输层帧上限，否则整份快照永远发不出去
	if _, err := loadClusterConfigErr(t, "  snapshot_chunk_bytes: 33554432\n"); err == nil {
		t.Fatal("分块大小超过 16MiB 帧上限必须被拒绝")
	}
	// 非正 TTL：视图刚建完就过期，快照传输永远拉不完——启动即挡
	if _, err := loadClusterConfigErr(t, "  snapshot_view_ttl: -1s\n"); err == nil {
		t.Fatal("负 snapshot_view_ttl 必须被拒绝")
	}
	// 显式填值透传（不被默认值盖掉）
	cfg2 := loadClusterConfig(t, `
cluster:
  node_id: 1
  raft_listen: ":9081"
  snapshot_view_ttl: 20m
  peers:
    - {id: 1, raft_addr: "127.0.0.1:9081", advertise_host: "127.0.0.1", advertise_port: 8081}
`)
	if cfg2.Cluster.SnapshotViewTTL != 20*time.Minute {
		t.Fatalf("snapshot_view_ttl 显式值 = %v; want 20m", cfg2.Cluster.SnapshotViewTTL)
	}
}
