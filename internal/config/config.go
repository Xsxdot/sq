// Package config 提供 sq 的配置加载。
//
// 职责：
//   - 定义全部可配置项与默认值
//   - 从可选 YAML 文件加载并覆盖默认值
//   - 初始化全局 slog
//
// 边界：
//   - 不做热更新；不校验业务语义（如端口占用）
package config

import (
	"fmt"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

// Config 为 sq 全部运行配置。零值无意义，必须经 Load 构造。
type Config struct {
	GRPCListen       string `yaml:"grpc_listen"`        // gRPC 监听地址，默认 :8081
	AdvertiseHost    string `yaml:"advertise_host"`     // 路由响应中的对外地址，默认 127.0.0.1
	AdvertisePort    int    `yaml:"advertise_port"`     // 默认 8081
	DataDir          string `yaml:"data_dir"`           // Pebble 数据目录
	Fsync            string `yaml:"fsync"`              // sync|async
	AutoCreateTopic  bool   `yaml:"auto_create_topic"`  // QueryRoute/Send 未知 topic 时自动建
	DefaultQueueNums uint32 `yaml:"default_queue_nums"` // 自动建 topic 的队列数
	LogLevel         string `yaml:"log_level"`          // debug|info|warn|error
}

// Load 加载配置。path 为空时返回纯默认值；文件存在则按字段覆盖。
func Load(path string) (*Config, error) {
	cfg := &Config{
		GRPCListen: ":8081", AdvertiseHost: "127.0.0.1", AdvertisePort: 8081,
		DataDir: "./data", Fsync: "sync",
		AutoCreateTopic: true, DefaultQueueNums: 4, LogLevel: "info",
	}
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %s: %w", path, err)
	}
	if cfg.Fsync != "sync" && cfg.Fsync != "async" {
		return nil, fmt.Errorf("配置 fsync 只接受 sync|async，得到 %q", cfg.Fsync)
	}
	return cfg, nil
}

// SetupSlog 按配置初始化全局 slog（JSON 输出到 stdout）。
func SetupSlog(level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})))
}
