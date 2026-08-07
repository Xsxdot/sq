// system.go: GET /admin/system —— 进程与宿主机运行态读数。
//
// 职责：
//   - 把 sysinfo.Reporter 的快照原样吐成 JSON，供控制台的系统读数条与
//     全站拒写横幅消费
//   - 提供 blocked()：admin 内部判定「当前是否拒写」的唯一入口
//
// 边界：
//   - 不做任何加工与阈值判断：显示成什么颜色、什么时候报警是前端的事
//   - 不返回数据目录的绝对路径：端点虽然受登录保护，但路径对排查没有增量
//     价值，少暴露一项部署信息
package admin

import "net/http"

// handleSystem GET /admin/system：磁盘用量、数据目录占用、Go 运行时内存、
// 协程数、运行时长与拒写状态。
//
// 注意：
//   - sys 未装配时返回 503 而不是一组 0 —— 与 /admin/timeseries 在采样器
//     缺席时的处理一致：不返回误导性的空数据
func (s *Server) handleSystem(w http.ResponseWriter, r *http.Request) {
	if s.sys == nil {
		s.httpError(w, http.StatusServiceUnavailable, "系统读数采集器未启用")
		return
	}
	snap := s.sys.Snapshot()
	// 拒写是会让生产端全挂的状态，每次被查询都记一笔，便于事后对齐
	// 「控制台什么时候看到的」与「retention 什么时候翻转的」
	if snap.WriteBlocked {
		s.logger.Warn("系统读数查询：当前处于拒写保读状态",
			"watermark_pct", snap.WatermarkPercent, "remote", r.RemoteAddr)
	}
	s.writeJSON(w, http.StatusOK, snap)
}

// blocked 返回当前是否处于拒写保读状态。sys 未装配时视为未拒写。
func (s *Server) blocked() bool { return s.sys != nil && s.sys.WriteBlocked() }
