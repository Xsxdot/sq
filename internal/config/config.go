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
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
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

// Credential 一条 gRPC 静态鉴权凭据。多条凭据 = 每个接入方一对、可单独吊销。
type Credential struct {
	Name      string `yaml:"name"` // 可选，仅用于日志追溯（如"订单服务"），不参与校验
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

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
	// Credentials gRPC 静态鉴权凭据列表；空/缺省 = 不鉴权（spec §6 默认关闭）。
	// v1.0 前的破坏性变更：取代旧的 access_key/secret_key 标量对。
	Credentials []Credential `yaml:"credentials"`

	// MetricsRetentionHours 时序采样点的落库保留时长（小时）。0 = 不落库，
	// 只保留内存环的最近 1 小时（进程重启即丢）。默认 168（7 天）。
	MetricsRetentionHours int `yaml:"metrics_retention_hours"`
	// —— M6 事务消息 ——
	// TxnCheckInterval 半消息回查间隔（Go duration 格式）。半消息落盘后第一次
	// 回查发生在写入后一个间隔，此后每次回查后再排一个间隔，直到决断或超限。
	TxnCheckInterval string `yaml:"txn_check_interval"`
	// TxnMaxChecks 单条半消息最大回查次数，超限即丢弃并记日志（spec §5 流程 5）。
	TxnMaxChecks int `yaml:"txn_max_checks"`
	// Cluster 集群模式配置段。nil = 单机模式；段一旦出现即集群模式。
	Cluster *ClusterConfig `yaml:"cluster"`
}

// MaxClusterSnapshotChunkBytes 是快照分块的上界（16MiB，传输层帧上限）：
// 分块 ≥ 帧上限时整份快照永远发不出去（超帧响应被双端拒收），启动即挡。
const MaxClusterSnapshotChunkBytes = 16 << 20

// 集群档截断循环与快照分块的默认值（batch④）：与 cluster.Options 的
// 默认常量保持一致（defaultRetainEntries/defaultTruncateInterval/
// defaultSnapshotChunkBytes）。
const (
	defaultClusterLogRetainEntries   = 10000
	defaultClusterTruncateInterval   = 30 * time.Second
	defaultClusterSnapshotChunkBytes = 4 << 20
	defaultClusterSnapshotViewTTL    = 5 * time.Minute
)

// ClusterConfig 集群模式配置段；nil = 单机模式。段一旦出现即集群模式，
// 全部字段按集群语义严格校验（见 Load 末尾校验链）。
type ClusterConfig struct {
	NodeID     uint64        `yaml:"node_id"`     // 本节点在成员表中的唯一 id，必须出现在 peers 中
	RaftListen string        `yaml:"raft_listen"` // 本节点 raft 组间复制流量的监听地址（如 ":9081"）
	DataGroups uint32        `yaml:"data_groups"` // 数据组数；首启持久化后不可变，改配置不改盘上事实（cluster.EnsureGroups 拒启）
	Ack        string        `yaml:"ack"`         // 确认档位：quorum-mem|quorum-fsync；缺省 quorum-mem（spec §2.2 复制确认+异步刷盘）
	Peers      []ClusterPeer `yaml:"peers"`       // 成员表（含本节点）；1 个 = 单机→单节点集群升级形态
	// LogRetainEntries 周期截断的日志保留量（log_retain_entries，默认
	// 10000）：落后 follower 位点落在截断点之下就只能靠快照追平。
	// 0 = 未填，按默认。
	LogRetainEntries uint64 `yaml:"log_retain_entries"`
	// TruncateInterval 周期截断循环的执行间隔（truncate_interval，默认
	// 30s，Go duration 格式）：每周期对全部组评估一次日志截断。
	// 0 = 未填，按默认。
	TruncateInterval time.Duration `yaml:"truncate_interval"`
	// SnapshotChunkBytes 快照拉取的单个分块字节预算（snapshot_chunk_bytes，
	// 默认 4MiB）：必须 < 16MiB 传输层帧上限，否则整份快照永远发不出去。
	// 0 = 未填，按默认。
	SnapshotChunkBytes int `yaml:"snapshot_chunk_bytes"`
	// SnapshotViewTTL 快照视图的存活时长（snapshot_view_ttl，默认 5m）：
	// 视图被借出（对端每拉一块）即续期，超过 TTL 无人问津即回收；从建档
	// 起活过 TTL×10 则命中不可续期的硬上限被强制作废。
	//
	// 调大的代价是磁盘：持有视图会阻止 Pebble 回收被覆盖的旧版本，视图
	// 活多久、旧版本就压多久。大库 + 慢网需要调大（否则传输还没完就被
	// 回收，重来一遍），但请按「最慢一个 follower 拉完全量的时间」估，
	// 不要无脑放大。0 = 未填，按默认。
	SnapshotViewTTL time.Duration `yaml:"snapshot_view_ttl"`
	// ReadBarrier 打开线性一致读屏障（read_barrier，默认 false）：打开后
	// 消费读路径每次入口走一轮 raft read-index，用一次多数派心跳往返的
	// 延迟换掉「旧 leader 尚未察觉失去领导权时投递过期数据」的窗口。
	// 关闭时读己之写仍然成立（propose 等 apply），只是别人的写可能读不到。
	ReadBarrier bool `yaml:"read_barrier"`
	// ReadBarrierTimeout 单轮 read-index 的时间预算（read_barrier_timeout，
	// 默认 3s，Go duration 格式）。0 = 未填，按默认。
	ReadBarrierTimeout time.Duration `yaml:"read_barrier_timeout"`
}

// ClusterPeer 成员表里一个节点的描述。
type ClusterPeer struct {
	ID            uint64 `yaml:"id"`             // 成员 id，全表唯一且非零
	RaftAddr      string `yaml:"raft_addr"`      // 该节点的 raft 监听地址（组间复制流量）
	AdvertiseHost string `yaml:"advertise_host"` // 对外广告地址（路由/协议面广播给客户端）
	AdvertisePort int    `yaml:"advertise_port"` // 对外广告端口
}

// Load 加载配置。path 为空时返回纯默认值；文件存在则按字段覆盖。
func Load(path string) (*Config, error) {
	cfg := &Config{
		GRPCListen: ":8081", AdvertiseHost: "127.0.0.1", AdvertisePort: 8081,
		DataDir: "./data", Fsync: "sync",
		AutoCreateTopic: true, DefaultQueueNums: 16, // 16：队列数决定消费并行度（每队列一路消费）；写吞吐自队列内 group commit 后与队列数基本无关

		DefaultMaxAttempts: 16, LogLevel: "info",
		RetentionCheckInterval: "5m",
		DiskWatermarkPercent:   85,
		AdminListen:            ":8082",
		MetricsRetentionHours:  168,
		TxnCheckInterval:       "30s",
		TxnMaxChecks:           15,
	}
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %s: %w", path, err)
	}
	// 先扫一遍旧标量键：严格解析本身也会挡下它们（field not found），但那个报错
	// 不含迁移指引，运维会以为是普通拼写错误。单独扫一遍是为了给出「已废弃、改
	// credentials 列表」的明确提示——旧配置被静默忽略会让鉴权无声回到关闭，这是
	// 安全静默降级，必须启动时硬报错（README「升级注意」有迁移示例）。
	var probe map[string]any
	if err := yaml.Unmarshal(raw, &probe); err == nil {
		var legacy []string
		if _, ok := probe["access_key"]; ok {
			legacy = append(legacy, "access_key")
		}
		if _, ok := probe["secret_key"]; ok {
			legacy = append(legacy, "secret_key")
		}
		if len(legacy) > 0 {
			return nil, fmt.Errorf("配置使用了已废弃的 %s——自本版本起改为 credentials 列表（迁移示例见 README「升级注意」）；为防鉴权静默关闭，本配置拒绝启动",
				strings.Join(legacy, "/"))
		}
	}
	// 严格解析（KnownFields）：任何未知键名都拒绝——配置笔误要在启动时暴露，
	// 而不是被 yaml 静默吞掉，运维带着"配置了却无效"的困惑上线。
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
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
	// 每条凭据必须成对非空（同旧标量时代"只填一半必是笔误"的原则），AK 全局唯一
	// ——重复 AK 会让 map 构建时后者静默覆盖前者，吊销/排查全部错乱，启动即挡。
	seen := map[string]int{}
	for i, c := range cfg.Credentials {
		if c.AccessKey == "" || c.SecretKey == "" {
			return nil, fmt.Errorf("配置 credentials[%d] 的 access_key/secret_key 必须成对非空", i)
		}
		if j, dup := seen[c.AccessKey]; dup {
			return nil, fmt.Errorf("配置 credentials[%d] 与 credentials[%d] 的 access_key 重复: %q", i, j, c.AccessKey)
		}
		seen[c.AccessKey] = i
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
	// txn_check_interval 必须是正 duration：空串（yaml 漏填）或拼写错误
	// 都不能让回查调度器以 0 间隔空转或整趟跳过，启动时挡住。
	if d, err := time.ParseDuration(cfg.TxnCheckInterval); err != nil || d <= 0 {
		return nil, fmt.Errorf("配置 txn_check_interval 须为正 duration（如 30s），得到 %q", cfg.TxnCheckInterval)
	}
	// 上限 1000：回查是丢弃前的最后防线，配大到近乎无限等于永不丢弃，
	// half/ 区会被永远无人决断的僵尸条目占满
	if cfg.TxnMaxChecks < 1 || cfg.TxnMaxChecks > 1000 {
		return nil, fmt.Errorf("配置 txn_max_checks 须在 [1,1000]，得到 %d", cfg.TxnMaxChecks)
	}
	// —— 集群段（cluster）校验 ——
	// 段缺省 = 单机模式（Cluster 保持 nil，ClusterEnabled()==false）；段一旦出现，
	// 每个字段都按集群语义严格校验——半配的集群段比没有更危险，启动时挡住。
	if cfg.Cluster != nil {
		cc := cfg.Cluster
		// DataGroups/Ack 的缺省档只在段存在时生效：yaml 标量无法区分
		// 「显式 0/空串」与「未填」，一律按未填给默认值，再做范围校验。
		if cc.DataGroups == 0 {
			cc.DataGroups = 3
		}
		if cc.Ack == "" {
			cc.Ack = "quorum-mem"
		}
		// 截断/分块档默认值：yaml 标量无法区分「显式 0/空串」与「未填」，
		// 一律按未填给默认值（与 DataGroups/Ack 同款语义），再做范围校验。
		if cc.LogRetainEntries == 0 {
			cc.LogRetainEntries = defaultClusterLogRetainEntries
		}
		if cc.TruncateInterval == 0 {
			cc.TruncateInterval = defaultClusterTruncateInterval
		}
		// 负间隔是笔误（time.NewTicker 对非正周期直接 panic，跑起来才炸
		// 比启动时挡住更难排查），启动即挡。
		if cc.TruncateInterval < 0 {
			return nil, fmt.Errorf("配置 cluster.truncate_interval 须为正 duration（如 30s），得到 %s", cc.TruncateInterval)
		}
		if cc.SnapshotViewTTL == 0 {
			cc.SnapshotViewTTL = defaultClusterSnapshotViewTTL
		}
		// 非正 TTL 会让每个视图刚建完就被 GC 判定过期回收，快照传输永远
		// 拉不完（每块都得重开视图，游标锚在已回收的 snapID 上直接失败）
		// ——启动即挡，不留给运行时去「发现」。
		if cc.SnapshotViewTTL < 0 {
			return nil, fmt.Errorf("配置 cluster.snapshot_view_ttl 须为正 duration（如 5m），得到 %s", cc.SnapshotViewTTL)
		}
		// 非正 read_barrier_timeout 会让每轮 read-index 立即超时，屏障恒
		// 失败、消费读路径被直接拒投——配置笔误启动即挡，不留到运行时。
		if cc.ReadBarrierTimeout < 0 {
			return nil, fmt.Errorf("配置 cluster.read_barrier_timeout 须为正 duration（如 3s），得到 %s", cc.ReadBarrierTimeout)
		}
		if cc.SnapshotChunkBytes == 0 {
			cc.SnapshotChunkBytes = defaultClusterSnapshotChunkBytes
		}
		// 上界 16MiB（传输层帧上限，见 cluster/transport.go 的 maxFrameLen）：
		// 分块 ≥ 帧上限时整份快照永远发不出去——超帧的响应被传输层双端
		// 拒收（坏帧断连），拉取永远卡在同一个块上。配置笔误启动即挡，
		// 不能等运行时靠断连重试「发现」。
		if cc.SnapshotChunkBytes < 1 || cc.SnapshotChunkBytes >= MaxClusterSnapshotChunkBytes {
			return nil, fmt.Errorf("配置 cluster.snapshot_chunk_bytes 须在 [1,%d)（必须 < 16MiB 传输层帧上限），得到 %d",
				MaxClusterSnapshotChunkBytes, cc.SnapshotChunkBytes)
		}
		if cc.RaftListen == "" {
			return nil, fmt.Errorf("配置 cluster.raft_listen 不能为空（集群模式必须声明 raft 监听地址）")
		}
		// 组数范围 [1,64]：组数在首启时持久化进盘、之后不可变（cluster.EnsureGroups
		// 拒启），这里只防笔误——64 组 × 每组长轮询心跳已是极宽余量。
		if cc.DataGroups < 1 || cc.DataGroups > 64 {
			return nil, fmt.Errorf("配置 cluster.data_groups 须在 [1,64]，得到 %d", cc.DataGroups)
		}
		// 档位白名单：其他值（如 "async"）会在复制层静默按默认档执行，
		// 运维以为开了严格档实际没有——启动时挡下。
		switch cc.Ack {
		case "quorum-mem", "quorum-fsync":
		default:
			return nil, fmt.Errorf("配置 cluster.ack 只接受 quorum-mem|quorum-fsync，得到 %q", cc.Ack)
		}
		// peers 至少 1：0 个 peer 的集群段是半配（node_id 必然找不到归属）；
		// 1 个 peer 合法——单机→单节点集群的升级形态。
		if len(cc.Peers) < 1 {
			return nil, fmt.Errorf("配置 cluster.peers 至少 1 个，得到 %d 个", len(cc.Peers))
		}
		seen := make(map[uint64]int, len(cc.Peers))
		seenNode := false
		for i, p := range cc.Peers {
			if p.ID == 0 {
				return nil, fmt.Errorf("配置 cluster.peers[%d] 的 id 必须 >0，得到 %d", i, p.ID)
			}
			if j, dup := seen[p.ID]; dup {
				return nil, fmt.Errorf("配置 cluster.peers[%d] 与 peers[%d] 的 id 重复: %d", i, j, p.ID)
			}
			seen[p.ID] = i
			if p.ID == cc.NodeID {
				seenNode = true
			}
			if p.RaftAddr == "" {
				return nil, fmt.Errorf("配置 cluster.peers[%d] 的 raft_addr 不能为空", i)
			}
			if p.AdvertiseHost == "" {
				return nil, fmt.Errorf("配置 cluster.peers[%d] 的 advertise_host 不能为空", i)
			}
			if p.AdvertisePort < 1 || p.AdvertisePort > 65535 {
				return nil, fmt.Errorf("配置 cluster.peers[%d] 的 advertise_port 须在 [1,65535]，得到 %d", i, p.AdvertisePort)
			}
		}
		// node_id 必须落在成员表里：不在 = 笔误，启动即挡（raft 需要成员表自洽，
		// 缺本的节点无从开始）。
		if !seenNode {
			return nil, fmt.Errorf("配置 cluster.node_id %d 必须出现在 peers 的 id 中", cc.NodeID)
		}
		// 校验全部通过后才提示：偶数节点数无容错价值（2 节点任一挂即失 quorum），
		// raft 容忍但不拒——留给运维判断。
		if len(cc.Peers) > 1 && len(cc.Peers)%2 == 0 {
			slog.Default().Warn("集群节点数为偶数，无容错价值，建议奇数", "nodes", len(cc.Peers))
		}
	}
	return cfg, nil
}

// RetentionInterval 解析后的清理扫描间隔（Load 已校验合法，此处不会失败）。
func (c *Config) RetentionInterval() time.Duration {
	d, _ := time.ParseDuration(c.RetentionCheckInterval)
	return d
}

// TxnInterval 解析后的半消息回查间隔（Load 已校验合法，此处不会失败）。
func (c *Config) TxnInterval() time.Duration {
	d, _ := time.ParseDuration(c.TxnCheckInterval)
	return d
}

// ClusterEnabled 是否集群模式：Cluster 段存在即集群，nil 即单机。
func (c *Config) ClusterEnabled() bool {
	return c.Cluster != nil
}

// PeerRaftAddrs 返回成员表 id → raft 监听地址 的映射。
//
// Task 9/11 装配 raft 组配置时用：组启动把本节点与各 peer 的地址
// 展开进 raft 节点列表，避免调用方各自重复遍历。
func (cc *ClusterConfig) PeerRaftAddrs() map[uint64]string {
	m := make(map[uint64]string, len(cc.Peers))
	for _, p := range cc.Peers {
		m[p.ID] = p.RaftAddr
	}
	return m
}

// AdvertiseOf 返回指定成员 id 的对外广告地址（host, port）。
//
// 协议/路由面需要把集群成员的对地址广播给客户端时用；
// id 不在成员表返回 ok=false，调用方按缺失处理。
func (cc *ClusterConfig) AdvertiseOf(id uint64) (host string, port int, ok bool) {
	for _, p := range cc.Peers {
		if p.ID == id {
			return p.AdvertiseHost, p.AdvertisePort, true
		}
	}
	return "", 0, false
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
