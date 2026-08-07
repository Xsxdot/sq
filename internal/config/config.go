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
	"time"

	"gopkg.in/yaml.v3"
)

// MaxDefaultQueueNums 是 default_queue_nums 允许的上限。
//
// 取 1024 的依据：RocketMQ 单个 topic 的队列数惯例在个位到两位数（默认 4~16），
// 队列数的意义是消费并行度上限，远超消费者实例数之后只会让每个队列更空、
// 长轮询更频繁。1024 已给出两个数量级的余量，同时守住两条硬边界——它远小于
// int32 上限（路由响应里的队列 id 是 int32(i)，越界会变成负数），
// 且每次 QueryRoute/QueryAssignment 都要展开这么多个 MessageQueue 条目，
// 响应体大小必须有个头。
const MaxDefaultQueueNums = 1024

// Config 为 sq 全部运行配置。零值无意义，必须经 Load 构造。
type Config struct {
	GRPCListen             string `yaml:"grpc_listen"`              // gRPC 监听地址，默认 :8081
	AdvertiseHost          string `yaml:"advertise_host"`           // 路由响应中的对外地址，默认 127.0.0.1
	AdvertisePort          int    `yaml:"advertise_port"`           // 默认 8081
	DataDir                string `yaml:"data_dir"`                 // Pebble 数据目录
	Fsync                  string `yaml:"fsync"`                    // sync|async
	AutoCreateTopic        bool   `yaml:"auto_create_topic"`        // QueryRoute/Send 未知 topic 时自动建
	DefaultQueueNums       uint32 `yaml:"default_queue_nums"`       // 自动建 topic 的队列数
	DefaultMaxAttempts     int32  `yaml:"default_max_attempts"`     // 新订阅组默认最大投递次数
	RetentionCheckInterval string `yaml:"retention_check_interval"` // 过期清理扫描间隔（Go duration 格式）
	DiskWatermarkPercent   int    `yaml:"disk_watermark_percent"`   // 超过即拒写，0=关闭
	LogLevel               string `yaml:"log_level"`                // debug|info|warn|error
	// —— M5 认证与管理面 ——
	AdminListen   string `yaml:"admin_listen"`   // Admin HTTP 监听地址，"" = 关闭；默认 :8082
	AdminUsername string `yaml:"admin_username"` // Admin API 登录用户名（与密码成对，均空 = 免登录）
	AdminPassword string `yaml:"admin_password"` // Admin API 登录密码
	AccessKey     string `yaml:"access_key"`     // gRPC 静态 AK（与 secret_key 成对，均空 = 不鉴权，spec §6 默认关闭）
	SecretKey     string `yaml:"secret_key"`     // gRPC 静态 SK

	// MetricsRetentionHours 时序采样点的落库保留时长（小时）。0 = 不落库，
	// 只保留内存环的最近 1 小时（进程重启即丢）。默认 168（7 天）。
	MetricsRetentionHours int `yaml:"metrics_retention_hours"`
}

// Load 加载配置。path 为空时返回纯默认值；文件存在则按字段覆盖。
func Load(path string) (*Config, error) {
	cfg := &Config{
		GRPCListen: ":8081", AdvertiseHost: "127.0.0.1", AdvertisePort: 8081,
		DataDir: "./data", Fsync: "sync",
		AutoCreateTopic: true, DefaultQueueNums: 4, DefaultMaxAttempts: 16, LogLevel: "info",
		RetentionCheckInterval: "5m",
		DiskWatermarkPercent:   85,
		AdminListen:            ":8082",
		MetricsRetentionHours:  168,
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
	// 队列数必须在启动时挡住，不能等到运行时才发作：
	//   - 写 0（或漏填成 0）时，每一次 EnsureTopic 都会失败，而这个失败会一路
	//     翻译成客户端看到的 INTERNAL_SERVER_ERROR——一个配置笔误从此伪装成
	//     "broker 坏了"，运维要顺着服务端日志找很久才知道问题在自己的 yaml 里；
	//   - 写一个超大值（如 3000000000）时，路由响应里的队列 id 由 int32(i) 得出，
	//     越过 2^31 之后直接变成负数，客户端拿到一批负 id 的队列。
	// 两种情况都不该由客户端来承担，进程干脆不要起来。
	if cfg.DefaultQueueNums < 1 || cfg.DefaultQueueNums > MaxDefaultQueueNums {
		return nil, fmt.Errorf("配置 default_queue_nums 必须在 1..%d 之间，得到 %d",
			MaxDefaultQueueNums, cfg.DefaultQueueNums)
	}
	// max_attempts=0 会让新订阅组全部回退包默认，使配置项的语义与
	// meta.New 的防御性回退重叠，配置层面的笔误不该静默吞掉，启动即报错。
	if cfg.DefaultMaxAttempts <= 0 {
		return nil, fmt.Errorf("配置 default_max_attempts 必须 >0，得到 %d", cfg.DefaultMaxAttempts)
	}
	// retention_check_interval 必须是正 duration：空串（yaml 漏填）或拼写错误
	// 都不能让 retention 任务以 0 间隔空转或整趟跳过，启动时挡住。
	if d, err := time.ParseDuration(cfg.RetentionCheckInterval); err != nil || d <= 0 {
		return nil, fmt.Errorf("配置 retention_check_interval 须为正 duration（如 5m），得到 %q", cfg.RetentionCheckInterval)
	}
	// disk_watermark_percent 必须在 [0,99]：100 意味着"永不触发拒写"却保留着
	// 拒写逻辑的错觉，99 才是"留 1% 余量"的真实语义；负数/超限是配置笔误，
	// 启动时挡住比运行期静默不拒写更容易被发现。
	if cfg.DiskWatermarkPercent < 0 || cfg.DiskWatermarkPercent > 99 {
		return nil, fmt.Errorf("配置 disk_watermark_percent 须在 [0,99]（0=关闭），得到 %d", cfg.DiskWatermarkPercent)
	}
	// log_level 与 SetupSlog 的 switch 分支必须同步：这里不挡住，SetupSlog 的
	// default 分支会把拼错的级别静默降级成 info，错误从此不可见。
	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return nil, fmt.Errorf("配置 log_level 只接受 debug|info|warn|error，得到 %q", cfg.LogLevel)
	}
	// 认证配置必须成对出现：只填一半几乎必然是笔误——比如配了 access_key 忘了
	// secret_key，此时"启用但秘钥为空"和"未启用"两种解释都会让用户在真出事时
	// 误判认证状态，启动即报错是唯一不含糊的处理。
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return nil, fmt.Errorf("配置 access_key/secret_key 必须成对设置（或都留空以关闭 gRPC 认证）")
	}
	if (cfg.AdminUsername == "") != (cfg.AdminPassword == "") {
		return nil, fmt.Errorf("配置 admin_username/admin_password 必须成对设置（或都留空以免登录）")
	}
	// 上界 8760（一年）：时序落库是每分钟一条定长记录，一年约 52 万条、
	// 几十 MB，再往上就该接外部 TSDB 而不是塞进消息队列自己的 Pebble 了。
	// 负数是笔误，启动时挡住比运行期静默按 0 处理更容易被发现。
	if cfg.MetricsRetentionHours < 0 || cfg.MetricsRetentionHours > 8760 {
		return nil, fmt.Errorf("配置 metrics_retention_hours 须在 [0,8760]（0=不落库），得到 %d", cfg.MetricsRetentionHours)
	}
	return cfg, nil
}

// RetentionInterval 解析后的清理扫描间隔（Load 已校验合法，此处不会失败）。
func (c *Config) RetentionInterval() time.Duration {
	d, _ := time.ParseDuration(c.RetentionCheckInterval)
	return d
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
