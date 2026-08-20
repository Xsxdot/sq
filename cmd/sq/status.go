// status.go 实现 `sq status` 子命令：一条命令回答「本节点/本集群现在什么状态」。
//
// 职责：
//   - 读配置、定位本机管理面、按需登录
//   - 单机档取 overview + system；集群档取 cluster，必要时跳到 leader 再取一次
//   - 把结果交给 renderStatus 渲染、statusVerdict 定退出码
//
// 边界：
//   - 不启动 broker、不碰数据目录、不做任何写操作——纯只读诊断
//   - 报告走 stdout，事件与失败走 slog（与 cmd/sq/recover.go 同款区分：
//     报告是给人看的命令输出，不是事件；两边都记会产生重复）
//   - 跳不到 leader 不算失败，降级为本机视角并如实标注（退出码 3）。
//     看得到多少报多少，比整条命令失败有用
//
// 为什么需要跳到 leader：GroupView.Peers（各 peer 的复制进度）只在本节点是
// 该组 leader 时才有内容——raft 的 tracker.Progress 只在 leader 侧维护，
// follower 上是空表。不跳转就永远看不到「另外两台活着吗」。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/config"
	"github.com/Xsxdot/sq/internal/replication"
)

// statusHTTPTimeout 单次管理面请求的超时。
//
// 3s：本机管理面正常在毫秒级返回；跳 leader 是跨机一跳。够不着时要快速
// 降级而不是让命令挂着——运维敲 status 就是因为已经怀疑出事了。
const statusHTTPTimeout = 3 * time.Second

// runStatus 执行 `sq status` 子命令。
//
// 参数：args 为 `status` 之后的全部参数
// 返回：进程退出码（statusOK / statusUnreachable / statusDegraded / statusIncomplete）
//
// 注意：本函数自己把错误打进 slog 并返回退出码，不返回 error——退出码是
// 本命令的主要产物（会被写进监控脚本），交给调用方从 error 反推容易丢信息。
func runStatus(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "配置文件路径（缺省则用进程内默认值）")
	if err := fs.Parse(args); err != nil {
		return statusUnreachable
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("加载配置失败", "path", *cfgPath, "err", err)
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return statusUnreachable
	}
	config.SetupSlog(cfg.LogLevel)
	slog.Debug("sq status 开始", "config", *cfgPath, "cluster", cfg.ClusterEnabled())

	port, err := cfg.LocalAdminPort()
	if err != nil {
		// 管理面关掉时必须立刻说清楚，而不是去连一个空地址再超时——
		// 「连不上」会把运维引向网络排查，而真实原因是配置里关掉了管理面
		slog.Error("无法定位本机管理面", "err", err)
		fmt.Fprintf(os.Stderr, "%v\n本命令依赖管理面 HTTP 取数；请在配置里设置 admin_listen 后重启 sq。\n", err)
		return statusUnreachable
	}

	v, code := collectStatus(cfg, fmt.Sprintf("http://127.0.0.1:%d", port))
	if code == statusUnreachable {
		return code
	}
	renderStatus(os.Stdout, v)
	final := statusVerdict(v)
	slog.Debug("sq status 完成", "exit", final, "degraded", v.Degraded)
	return final
}

// collectStatus 取数并填出视图。
//
// 参数：
//   - cfg: 已加载的配置
//   - localBase: 本机管理面 base URL，如 "http://127.0.0.1:8082"
//
// 返回：填好的视图；第二个返回值只在够不着本机管理面时为 statusUnreachable，
// 其余情况一律为 statusOK（真正的判级由 statusVerdict 做，不在这里）。
func collectStatus(cfg *config.Config, localBase string) (statusView, int) {
	v := statusView{Version: version, PeerHost: map[uint64]string{}}
	if cfg.ClusterEnabled() {
		for _, p := range cfg.Cluster.Peers {
			v.PeerHost[p.ID] = p.AdvertiseHost
		}
	}

	local := newAdminClient(localBase, statusHTTPTimeout)
	if err := authenticate(local, cfg, localBase); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return v, statusUnreachable
	}

	var cv replication.ClusterView
	if err := local.getJSON("/admin/cluster", &cv); err != nil {
		slog.Error("取本机集群视图失败", "base", localBase, "err", err)
		fmt.Fprintf(os.Stderr, "够不着本机管理面 %s: %v\n请确认 sq 已启动（systemctl status sq）。\n", localBase, err)
		return v, statusUnreachable
	}
	v.Cluster = cv
	// 本机身份在这里定死：下面若跳转 leader 成功，v.Cluster 会被换成
	// leader 的视图，那份视图的 SelfID/Self 描述的是 leader 不是本机。
	v.LocalID = cv.SelfID

	if !cv.Enabled {
		// 单机档：再取两个只在单机档渲染的端点。这两个失败不致命——
		// 拓扑已经拿到了，少两行数字比整条命令失败有用
		var ov adminOverview
		if err := local.getJSON("/admin/overview", &ov); err != nil {
			slog.Warn("取总览失败，跳过总览段", "err", err)
		} else {
			v.Overview = &ov
		}
		var sys adminSystem
		if err := local.getJSON("/admin/system", &sys); err != nil {
			slog.Warn("取系统信息失败，跳过磁盘段", "err", err)
		} else {
			v.System = &sys
		}
		slog.Info("sq status 取数完成（单机档）")
		return v, statusOK
	}

	v.ViewSource = fmt.Sprintf("node %d (%s) — 本机", cv.SelfID, localBase)
	metaLeader := metaGroupLeader(cv)
	if metaLeader == 0 || metaLeader == cv.SelfID {
		// 本机就是 leader（或当前无 leader，无处可跳）——本机视角已是最全的
		if metaLeader == cv.SelfID {
			v.ViewSource = fmt.Sprintf("node %d (%s) — leader", cv.SelfID, localBase)
		}
		slog.Info("sq status 取数完成（集群档，本机视角）", "self", cv.SelfID, "meta_leader", metaLeader)
		return v, statusOK
	}

	// 本机是 follower：跳到 leader 才看得到 peer 复制进度
	leaderView, source, err := fetchFromLeader(cfg, metaLeader)
	if err != nil {
		slog.Warn("跳转到 leader 失败，降级为本机视角", "leader", metaLeader, "err", err)
		v.Degraded = true
		v.DegradeReason = err.Error()
		return v, statusOK
	}
	v.Cluster = leaderView
	v.ViewSource = source
	slog.Info("sq status 取数完成（集群档，leader 视角）", "self", cv.SelfID, "leader", metaLeader)
	return v, statusOK
}

// authenticate 在服务端配了鉴权时登录换 token；未配则直接返回 nil。
//
// 参数：c 客户端；cfg 配置；base 仅用于组织报错文案
func authenticate(c *adminClient, cfg *config.Config, base string) error {
	if cfg.AdminUsername == "" {
		// 配置未设用户名 = 服务端免鉴权，调 /admin/login 会回 400，不能调
		slog.Debug("配置未设 admin_username，按免鉴权直连", "base", base)
		return nil
	}
	if err := c.login(cfg.AdminUsername, cfg.AdminPassword); err != nil {
		slog.Error("管理面登录失败", "base", base, "err", err)
		return fmt.Errorf("登录管理面 %s 失败: %w", base, err)
	}
	return nil
}

// metaGroupLeader 返回元数据组（cluster.MetaGroup，固定组号 0）的 leader id。
//
// 返回：0 表示当前无 leader 或视图里没有该组。
//
// 注意：用元数据组而不是任意数据组来决定跳哪儿——各数据组的 leader 可能
// 分散在不同节点，随便挑一个会让「视角来源」这一行在两次执行间跳来跳去。
// 元数据组是全局唯一的锚点。
func metaGroupLeader(cv replication.ClusterView) uint64 {
	for _, g := range cv.Groups {
		if g.ID == cluster.MetaGroup {
			return g.Leader
		}
	}
	return 0
}

// fetchFromLeader 到 leader 节点的管理面取一份完整集群视图。
//
// 参数：cfg 配置（用于查成员地址与凭据）；leaderID 元数据组 leader 的 id
// 返回：集群视图、视角来源描述、错误
//
// 注意：任何失败都以 error 返回，由调用方降级——不要在这里 os.Exit，
// 也不要把失败升级成整条命令失败。
func fetchFromLeader(cfg *config.Config, leaderID uint64) (replication.ClusterView, string, error) {
	var zero replication.ClusterView
	p, ok := cfg.PeerByID(leaderID)
	if !ok {
		return zero, "", fmt.Errorf("leader id %d 不在本机成员表里（配置可能与集群不一致）", leaderID)
	}
	addr, err := cfg.PeerAdminAddr(p)
	if err != nil {
		return zero, "", fmt.Errorf("推导 leader %d 的管理面地址失败: %w", leaderID, err)
	}
	base := "http://" + addr
	slog.Debug("跳转到 leader 取完整视图", "leader", leaderID, "base", base)
	c := newAdminClient(base, statusHTTPTimeout)
	if err := authenticate(c, cfg, base); err != nil {
		// 只有 401 才提凭据。把「连接被拒」也说成凭据问题会把运维引向
		// 错误方向——08-20 三机实测里挡掉 8082 后就撞到过这个误导。
		if strings.Contains(err.Error(), "401") {
			return zero, "", fmt.Errorf("连不上 leader %s（凭据不匹配，各节点 admin 凭据必须一致）: %w", base, err)
		}
		return zero, "", fmt.Errorf("连不上 leader %s: %w", base, err)
	}
	var cv replication.ClusterView
	if err := c.getJSON("/admin/cluster", &cv); err != nil {
		return zero, "", fmt.Errorf("连不上 leader %s: %w", base, err)
	}
	return cv, fmt.Sprintf("node %d (%s) — leader", leaderID, addr), nil
}
