// statusview.go 提供 `sq status` 的两件纯计算：把一份状态视图判成退出码，
// 以及把它渲染成给人看的文本。
//
// 职责：
//   - 定义 admin HTTP 三个端点的 JSON 形状（overview / system / cluster 的载体）
//   - statusVerdict：视图 → 退出码
//   - renderStatus：视图 → 文本
//
// 边界：
//   - 不发起任何网络请求、不读配置、不碰 os.Exit——取数与退出由 status.go 负责。
//     这样切开是为了让判级逻辑可被穷举单测：它的输出会被写进运维的监控脚本，
//     是本命令唯一不能出错的部分
//   - 不打日志：纯函数没有「关键节点」可言，且它每次调用都会把全部输入
//     渲染到 stdout，再打一份 slog 等于同一件事记两遍
package main

import (
	"fmt"
	"io"
	"sort"

	"github.com/Xsxdot/sq/internal/replication"
)

// sq status 的退出码。分级是为了能直接写进监控脚本：三个非零码对应三种
// 完全不同的处置，不能合并。
const (
	// statusOK 健康：全部组有 leader，且全部 peer 活跃
	statusOK = 0
	// statusUnreachable 够不着本机管理面：服务没起 / 管理面关闭 / 凭据错 / HTTP 失败
	statusUnreachable = 1
	// statusDegraded 集群降级：有组无 leader，或有 peer 失联
	statusDegraded = 2
	// statusIncomplete 判定不完整：视角降级，组 leader 判据通过但 peer 活跃度不可见
	statusIncomplete = 3
)

// adminDisk 对应 /admin/system 里的 disk 段。
type adminDisk struct {
	UsedPercent float64 `json:"used_percent"`
}

// adminSystem 对应 GET /admin/system 的响应（只取本命令用得上的字段）。
type adminSystem struct {
	Disk             *adminDisk `json:"disk"`
	WatermarkPercent int        `json:"watermark_percent"`
	UptimeSeconds    int64      `json:"uptime_seconds"`
}

// adminOverview 对应 GET /admin/overview 的响应（只取本命令用得上的字段）。
type adminOverview struct {
	Topics       int    `json:"topics"`
	Groups       int    `json:"groups"`
	Connections  int    `json:"connections"`
	TotalWritten uint64 `json:"total_written"`
	TotalPending uint64 `json:"total_pending"`
	TotalDLQ     uint64 `json:"total_dlq"`
}

// statusView 渲染与判级所需的全部输入。由 status.go 的取数流程填充。
type statusView struct {
	// Version 本二进制的版本号（main.version）
	Version string
	// Cluster 集群拓扑。Enabled=false 即单机档。
	//
	// 注意：本字段在跳转 leader 成功后装的是 **leader 的视图**，其 SelfID
	// 与 Nodes[].Self 描述的是 leader 而不是本机。判断「哪一行是本机」一律
	// 用 LocalID，不要读 Cluster.SelfID——08-20 三机实测踩过这个坑：
	// follower 上会把 leader 那行标成「(本机)」、抬头也显示成 leader 的 id。
	Cluster replication.ClusterView
	// LocalID 执行本命令的这台机器的节点 id，取自**跳转之前**的本机视图。
	// 它不随视角变化，是成员表里「(本机)」标记的唯一依据。
	LocalID uint64
	// Overview / System 只在单机档非 nil
	Overview *adminOverview
	System   *adminSystem
	// ViewSource 这份数据来自哪个节点，如 "node 1 (10.0.0.1:8082) — leader"
	ViewSource string
	// Degraded true = 本机是 follower 且跳 leader 失败，退回了本机视角。
	// 此时 Cluster.Groups[].Peers 必为空表，peer 活跃度不可知
	Degraded bool
	// DegradeReason Degraded 时的原因，原样展示给用户
	DegradeReason string
	// PeerHost id → advertise_host，用于成员表渲染出人能认的地址
	PeerHost map[uint64]string
}

// statusVerdict 由视图定退出码。
//
// 参数：v 已填充完毕的视图
// 返回：statusOK / statusDegraded / statusIncomplete 之一
// （statusUnreachable 由取数层直接返回，到不了这里）
//
// 判级优先级是承重的，不可调换：
//
//  1. 有组 leader==0 → statusDegraded。这条在降级视角下同样成立——follower
//     也知道每组 leader 是谁，所以它比「视角是否完整」更硬。
//  2. 视角降级 → statusIncomplete。GroupView.Peers 在非 leader 上是空表，
//     而**空表不等于没有 peer**；此时报 OK 等于拿看不见 peer 的视图谎称健康，
//     这是监控脚本最不能容忍的一种谎。
//  3. 有 peer !RecentActive → statusDegraded。
//  4. 其余 → statusOK。
func statusVerdict(v statusView) int {
	if !v.Cluster.Enabled {
		// 单机档没有 leader 与 peer 的概念：能取到数据本身就是健康的证明
		return statusOK
	}
	for _, g := range v.Cluster.Groups {
		if g.Leader == 0 {
			return statusDegraded
		}
	}
	if v.Degraded {
		return statusIncomplete
	}
	for _, g := range v.Cluster.Groups {
		for _, p := range g.Peers {
			if !p.RecentActive {
				return statusDegraded
			}
		}
	}
	return statusOK
}

// renderStatus 把视图渲染成给人看的文本。
//
// 参数：
//   - w: 输出目标（正常是 os.Stdout；单测传 strings.Builder）
//   - v: 已填充完毕的视图
//
// 注意：本函数不返回错误——渲染失败无从处置，且 w 是 stdout 时写失败
// 本身就意味着输出管道已断。
func renderStatus(w io.Writer, v statusView) {
	if !v.Cluster.Enabled {
		renderStandalone(w, v)
		return
	}
	renderCluster(w, v)
}

// renderStandalone 单机档：两行讲完，没有 leader/成员/组的概念。
func renderStandalone(w io.Writer, v statusView) {
	fmt.Fprintf(w, "sq %s   单机模式\n", v.Version)
	if v.Overview != nil {
		fmt.Fprintf(w, "topic %d   消费组 %d   连接 %d   已写入 %d   积压 %d   死信 %d\n",
			v.Overview.Topics, v.Overview.Groups, v.Overview.Connections,
			v.Overview.TotalWritten, v.Overview.TotalPending, v.Overview.TotalDLQ)
	}
	if v.System != nil && v.System.Disk != nil {
		fmt.Fprintf(w, "磁盘 %.1f%% / 拒写水位 %d%%\n", v.System.Disk.UsedPercent, v.System.WatermarkPercent)
	}
}

// renderCluster 集群档：先说视角来源（这份数据是谁给的），再成员表，再各组。
//
// 视角来源放最前面是刻意的——同一条命令在 leader 与 follower 上能看到的
// 东西不一样，读者必须先知道自己在看谁的视角，后面的数字才有意义。
func renderCluster(w io.Writer, v statusView) {
	fmt.Fprintf(w, "sq %s   本机 node %d   集群 %d 节点   数据组 %d\n",
		v.Version, v.LocalID, len(v.Cluster.Nodes), len(v.Cluster.Groups)-1)
	if v.Degraded {
		fmt.Fprintf(w, "视角：本机（%s）——%s\n", "未能跳转到 leader", v.DegradeReason)
		fmt.Fprintf(w, "      各 peer 的复制进度在本视角下**不可见**（非 leader 节点不维护 peer 进度）\n")
	} else {
		fmt.Fprintf(w, "视角：%s\n", v.ViewSource)
	}

	fmt.Fprintf(w, "\n成员\n")
	fmt.Fprintf(w, " %-4s %-22s %-22s %s\n", "id", "地址", "raft", "状态")
	for _, n := range v.Cluster.Nodes {
		host := v.PeerHost[n.ID]
		// 用 LocalID 而不是 n.Self：跳转 leader 后这份视图来自 leader，
		// 它的 Self 标志指的是 leader 自己。
		if n.ID == v.LocalID {
			host += "  (本机)"
		}
		fmt.Fprintf(w, " %-4d %-22s %-22s %s\n", n.ID, host, n.RaftAddr, peerState(v, n.ID))
	}

	fmt.Fprintf(w, "\n组\n")
	fmt.Fprintf(w, " %-4s %-8s %-6s %-10s %s\n", "组", "leader", "term", "applied", "待apply")
	groups := append([]replication.GroupView(nil), v.Cluster.Groups...)
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })
	for _, g := range groups {
		leader := fmt.Sprintf("%d", g.Leader)
		if g.Leader == 0 {
			leader = "无"
		}
		// 待 apply = commit − applied，每个节点自己就算得出（不依赖 leader 数据）。
		// applier 卡住会在这一列立刻显形。
		fmt.Fprintf(w, " %-4d %-8s %-6d %-10d %d\n", g.ID, leader, g.Term, g.Applied, g.Commit-g.Applied)
	}
}

// peerState 给成员表的「状态」列取一句话。
//
// 注意：视角降级时一律回「不可见」而不是「活跃」——空的 Peers 表只说明
// 本节点不是 leader，不说明对端活着。这个区分是退出码 3 存在的同一个理由。
func peerState(v statusView, id uint64) string {
	if id == v.LocalID {
		return "● 本节点"
	}
	if v.Degraded {
		return "? 不可见"
	}
	for _, g := range v.Cluster.Groups {
		for _, p := range g.Peers {
			if p.ID == id && !p.RecentActive {
				return "○ 失联"
			}
		}
	}
	return "● 活跃"
}
