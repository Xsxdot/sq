// clusterview.go 提供集群形态的队列归属视图（rpc.RouteView 实现）。
//
// 职责：
//   - 把「队列 → 组 → 当前 leader 节点」的映射翻译成协议面需要的
//     「(topic, queueID) → 通告地址 + broker 名」
//   - 依赖方向：rpc 包不 import cluster（cluster → replication → rpc
//     的单向依赖约束），本实现只能放在 main 侧——rpc.RouteView 接口
//     在 rpc 包定义、这里实现，main 装配时注入
//
// 边界：
//   - 不做任何缓存：leader 未知（选举窗口）时返回 ok=false，协议面
//     按 HA_NOT_AVAILABLE 处理，由 SDK 换节点重问
//   - broker 名取「sq{nodeID}」：路由响应里每条队列的 broker 名必须
//     能区分节点，客户端按 broker 名归组做负载均衡与位点管理
package main

import (
	"fmt"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/config"
)

// clusterRouteView 集群路由视图：QueueEndpoint 经 Manager 查队列所属
// 组的当前 leader，再按配置成员表的通告地址对外广播。
type clusterRouteView struct {
	m  *cluster.Manager
	cc *config.ClusterConfig
}

// QueueEndpoint 返回 (topic, queueID) 所属组当前 leader 的通告地址与
// broker 名。leader 未知（选举窗口）或 leader 不在成员表（配置与运行
// 态不一致，防御）时 ok=false。
func (v clusterRouteView) QueueEndpoint(topic string, queueID uint32) (string, int32, string, bool) {
	g := v.m.GroupForQueue(topic, queueID)
	lead, ok := v.m.Leader(g)
	if !ok {
		return "", 0, "", false
	}
	host, port, ok := v.cc.AdvertiseOf(lead)
	if !ok {
		return "", 0, "", false
	}
	return host, int32(port), fmt.Sprintf("sq%d", lead), true
}

// SelfIsLeader 本节点是否为 (topic, queueID) 所属组的 leader。
func (v clusterRouteView) SelfIsLeader(topic string, queueID uint32) bool {
	return v.m.IsLeader(v.m.GroupForQueue(topic, queueID))
}

// MetaIsLeader 本节点是否为元数据组的 leader。
func (v clusterRouteView) MetaIsLeader() bool {
	return v.m.IsLeader(cluster.MetaGroup)
}
