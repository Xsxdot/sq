// cluster.go: GET /admin/cluster —— 集群拓扑与复制进度只读视图。
//
// 职责：
//   - 把 replication.Topologer 的快照原样吐成 JSON，供控制台集群页消费
//
// 边界：
//   - 不做阈值判断：哪个落后量算"危险"、什么时候标红是前端的事
//   - 单机档不报错：回 enabled=false 让前端渲染「当前为单机模式」，
//     503 会让控制台把一个正常形态显示成故障
//   - 只读：领导权转移、成员变更等写操作不在本端点（管理动作要独立
//     的确认流程，不能挂在一个被前端每 5 秒轮询的 GET 上）
package admin

import (
	"net/http"

	"github.com/Xsxdot/sq/internal/replication"
)

// handleCluster GET /admin/cluster：成员表 + 每组的 leader/角色/applied，
// 本节点是 leader 的组另附各 peer 的复制进度。
//
// 注意：
//   - topo 未装配时视为单机档（回 enabled=false），不返回 503——控制台
//     在单机部署下也会打开这个页面，那不是故障
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.topo == nil {
		s.logger.Debug("集群端点被调用但拓扑来源未装配，按单机档返回")
		s.writeJSON(w, http.StatusOK, replication.ClusterView{Enabled: false})
		return
	}
	v := s.topo.Topology()
	// 刻意不在这里按 leader 状态打 Warn：本端点被集群页每几秒轮询一次，
	// 「本节点不是任何组 leader」可能持续数小时，逐次记录会把真正的
	// 状态翻转淹掉。翻转的记录归 cluster 层的 leader 变更日志。
	s.logger.Debug("返回集群拓扑", "enabled", v.Enabled, "nodes", len(v.Nodes), "groups", len(v.Groups))
	s.writeJSON(w, http.StatusOK, v)
}
