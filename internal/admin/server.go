// Package admin 提供 HTTP Admin API（spec §9 的后端；React 控制台 M5b 只消费
// 本 API，无私有通道）。
//
// 职责：
//   - 路由装配、登录门禁、/metrics 暴露
//   - 资源 handler：topic/消费组管理、消息查询、DLQ 重发、延时视图、测试消息
//
// 边界：
//   - 只组合 core 导出 API 与 store 只读扫描；消息语义（投递/重试/顺序）不在此层
//   - /metrics 不设防（抓取器无登录流程），端口暴露范围由部署侧控制
//   - 删除类操作不与在线流量互斥（adminops 的运维契约：先停流量再删）
package admin

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// Server Admin HTTP 服务。
type Server struct {
	st           *store.Store
	mt           *meta.Meta
	pr           *produce.Producer
	dl           *deliver.Deliverer
	username     string
	password     string
	writeBlocked *atomic.Bool
	logger       *slog.Logger

	tokens sync.Map // token(string) → 过期时间(time.Time)
	mux    *http.ServeMux
	hs     *http.Server
}

// New 构造 Admin 服务并装配全部路由。username/password 均空 = 免登录。
func New(st *store.Store, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer,
	username, password string, writeBlocked *atomic.Bool,
	reg *prometheus.Registry, logger *slog.Logger) *Server {
	s := &Server{
		st: st, mt: mt, pr: pr, dl: dl,
		username: username, password: password, writeBlocked: writeBlocked,
		logger: logger.With("mod", "admin"),
		mux:    http.NewServeMux(),
	}
	s.routes(reg)
	return s
}

// routes 注册全部路由。后续资源 handler（groups/messages/dlq/delay/overview）
// 在各自实现的 task 里往这里追加。
func (s *Server) routes(reg *prometheus.Registry) {
	s.mux.HandleFunc("POST /admin/login", s.handleLogin)
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	s.mux.HandleFunc("GET /admin/topics", s.protected(s.handleTopicsList))
	s.mux.HandleFunc("POST /admin/topics", s.protected(s.handleTopicCreate))
	s.mux.HandleFunc("GET /admin/topics/{name}", s.protected(s.handleTopicGet))
	s.mux.HandleFunc("PATCH /admin/topics/{name}", s.protected(s.handleTopicPatch))
	s.mux.HandleFunc("DELETE /admin/topics/{name}", s.protected(s.handleTopicDelete))
	s.mux.HandleFunc("GET /admin/groups", s.protected(s.handleGroupsList))
	s.mux.HandleFunc("GET /admin/groups/{name}", s.protected(s.handleGroupGet))
	s.mux.HandleFunc("POST /admin/groups/{name}/reset-cursor", s.protected(s.handleGroupResetCursor))
	s.mux.HandleFunc("DELETE /admin/groups/{name}", s.protected(s.handleGroupDelete))
}

// Handler 返回根 handler（测试注入用）。
func (s *Server) Handler() http.Handler { return s.mux }

// Serve 在 ln 上阻塞服务（调用方放入 goroutine）。Shutdown 后返回 http.ErrServerClosed。
func (s *Server) Serve(ln net.Listener) error {
	s.hs = &http.Server{Handler: s.mux}
	s.logger.Info("admin HTTP 服务中", "addr", ln.Addr().String())
	return s.hs.Serve(ln)
}

// Shutdown 优雅停止（等在途请求，受 ctx 限时）。Serve 未调用过时为空操作。
func (s *Server) Shutdown(ctx context.Context) error {
	if s.hs == nil {
		return nil
	}
	s.logger.Info("admin HTTP 停机")
	return s.hs.Shutdown(ctx)
}
