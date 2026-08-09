// routeview.go 定义队列归属视图 RouteView 与单机实现 staticRouteView。
//
// 职责：
//   - RouteView：协议面（QueryRoute/QueryAssignment/SendMessage/ReceiveMessage）
//     获取「队列归谁管」的唯一入口——集群档指向各 raft 组当前 leader，
//     单机档恒指向本节点自身
//   - staticRouteView：单机实现，恒返回 advertise 地址 + 固定 broker 名 sq0
//
// 边界：
//   - 不解析组号、不感知 raft：队列→leader 的映射由实现方（main 装配的
//     集群视图）完成，本包只消费接口
//   - rpc 包不 import cluster 包（依赖方向 cluster → replication → rpc），
//     集群语义的错误统一经 replication 转发的 ErrNotLeader 识别
package rpc

import "github.com/xushixin/sq/internal/config"

// RouteView 队列归属视图：协议面据此把每条队列指向其所属组的当前 leader，
// 并在 follower 上快速失败（避免长轮询空等 / 白做批次构造）。
type RouteView interface {
	// QueueEndpoint 返回 (topic, queueID) 队列当前 leader 节点的对外通告地址
	// 与 broker 名。leader 未知（选举窗口）时 ok=false——调用方按
	// HA_NOT_AVAILABLE 处理，SDK 会换节点重问。
	QueueEndpoint(topic string, queueID uint32) (host string, port int32, brokerName string, ok bool)
	// SelfIsLeader 本节点是否为 (topic, queueID) 队列所属组的 leader。
	SelfIsLeader(topic string, queueID uint32) bool
	// MetaIsLeader 本节点是否为元数据组（事务暂存等 meta 键族）的 leader。
	MetaIsLeader() bool
}

// StaticRouteView 构造单机路由视图（main 单机装配与测试 fixture 共用）。
// 集群档的 RouteView 由 main 按 cfg.Cluster 装配（见 cmd/sq）。
func StaticRouteView(cfg *config.Config) RouteView { return staticRouteView{cfg: cfg} }

// staticRouteView 单机实现：全部队列归本节点，恒返回 advertise 地址 +
// 固定 broker 名 sq0（与 v1 时代路由响应逐字节一致，客户端侧不感知变更）。
type staticRouteView struct {
	cfg *config.Config
}

// QueueEndpoint 恒返回本节点 advertise 地址与 broker 名 sq0，ok 恒真。
func (s staticRouteView) QueueEndpoint(string, uint32) (string, int32, string, bool) {
	return s.cfg.AdvertiseHost, int32(s.cfg.AdvertisePort), "sq0", true
}

// SelfIsLeader 恒真：单机不存在非 leader 节点。
func (staticRouteView) SelfIsLeader(string, uint32) bool { return true }

// MetaIsLeader 恒真：单机全部键族归本节点。
func (staticRouteView) MetaIsLeader() bool { return true }
