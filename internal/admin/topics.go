// topics.go: topic 管理端点（列表/创建/详情/改 retention/删除）。
//
// 职责：REST 参数校验 + 翻译为 meta/adminops 调用；HTTP 状态码语义见各 handler
// 边界：删除操作的「先停流量」运维契约见 adminops 包注释
package admin

import (
	"errors"
	"net/http"
	"sort"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/adminops"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// topicJSON topic 的 API 序列化形状（列表与详情共用）。
type topicJSON struct {
	Name        string `json:"name"`
	Queues      uint32 `json:"queues"`
	RetentionMs int64  `json:"retention_ms"`
	CreatedAtMs int64  `json:"created_at_ms"`
}

// handleTopicsList GET /admin/topics：全部 topic 按名字序。
func (s *Server) handleTopicsList(w http.ResponseWriter, r *http.Request) {
	tcs := s.mt.Topics()
	sort.Slice(tcs, func(i, j int) bool { return tcs[i].Name < tcs[j].Name })
	out := make([]topicJSON, 0, len(tcs))
	for _, tc := range tcs {
		out = append(out, topicJSON{Name: tc.Name, Queues: tc.Queues,
			RetentionMs: tc.RetentionMs, CreatedAtMs: tc.CreatedAtMs})
	}
	s.writeJSON(w, http.StatusOK, out)
}

// handleTopicCreate POST /admin/topics。409 语义：CreateTopic 本身幂等返回旧配置，
// 管理面必须显式区分"新建成功"与"早已存在"，否则控制台上建错名字无感知。
func (s *Server) handleTopicCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string `json:"name"`
		Queues      uint32 `json:"queues"`
		RetentionMs int64  `json:"retention_ms"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if req.RetentionMs < 0 {
		s.httpError(w, http.StatusBadRequest, "retention_ms 不能为负，得到 %d（0 表示用默认）", req.RetentionMs)
		return
	}
	if req.Queues < 1 || req.Queues > config.MaxDefaultQueueNums {
		s.httpError(w, http.StatusBadRequest, "queues 必须在 1..%d，得到 %d", config.MaxDefaultQueueNums, req.Queues)
		return
	}
	if _, ok := s.mt.GetTopic(req.Name); ok {
		s.httpError(w, http.StatusConflict, "topic %s 已存在", req.Name)
		return
	}
	tc, err := s.mt.CreateTopic(req.Name, req.Queues)
	if err != nil {
		if errors.Is(err, meta.ErrBadName) {
			s.httpError(w, http.StatusBadRequest, "%v", err)
			return
		}
		s.logger.Error("admin 创建 topic 失败", "topic", req.Name, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	if req.RetentionMs > 0 {
		if tc, err = s.mt.UpdateTopicRetention(req.Name, req.RetentionMs); err != nil {
			s.logger.Error("admin 设置 retention 失败", "topic", req.Name, "err", err)
			s.httpError(w, http.StatusInternalServerError, "%v", err)
			return
		}
	}
	s.logger.Info("admin 创建 topic", "topic", tc.Name, "queues", tc.Queues, "retention_ms", tc.RetentionMs)
	s.writeJSON(w, http.StatusCreated, topicJSON{Name: tc.Name, Queues: tc.Queues,
		RetentionMs: tc.RetentionMs, CreatedAtMs: tc.CreatedAtMs})
}

// handleTopicGet GET /admin/topics/{name}：配置 + 每队列写入位置。
func (s *Server) handleTopicGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	tc, ok := s.mt.GetTopic(name)
	if !ok {
		s.httpError(w, http.StatusNotFound, "topic %s 不存在", name)
		return
	}
	type queueDetail struct {
		QueueID    uint32 `json:"queue_id"`
		NextOffset uint64 `json:"next_offset"`
	}
	qs := make([]queueDetail, 0, tc.Queues)
	for q := uint32(0); q < tc.Queues; q++ {
		var next uint64
		raw, ok, err := s.st.Get(store.AllocKey(name, q))
		if err != nil {
			s.logger.Error("admin 读 alloc 失败", "topic", name, "queue", q, "err", err)
			s.httpError(w, http.StatusInternalServerError, "%v", err)
			return
		}
		if ok {
			next = store.GetU64(raw)
		}
		qs = append(qs, queueDetail{QueueID: q, NextOffset: next})
	}
	s.writeJSON(w, http.StatusOK, struct {
		topicJSON
		QueuesDetail []queueDetail `json:"queues_detail"`
	}{topicJSON{Name: tc.Name, Queues: tc.Queues, RetentionMs: tc.RetentionMs, CreatedAtMs: tc.CreatedAtMs}, qs})
}

// handleTopicPatch PATCH /admin/topics/{name}：目前仅支持改 retention_ms。
func (s *Server) handleTopicPatch(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		RetentionMs int64 `json:"retention_ms"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if req.RetentionMs <= 0 {
		s.httpError(w, http.StatusBadRequest, "retention_ms 必须 >0")
		return
	}
	tc, err := s.mt.UpdateTopicRetention(name, req.RetentionMs)
	if err != nil {
		if errors.Is(err, meta.ErrTopicNotFound) {
			s.httpError(w, http.StatusNotFound, "%v", err)
			return
		}
		s.logger.Error("admin 更新 retention 失败", "topic", name, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.writeJSON(w, http.StatusOK, topicJSON{Name: tc.Name, Queues: tc.Queues,
		RetentionMs: tc.RetentionMs, CreatedAtMs: tc.CreatedAtMs})
}

// handleTopicDelete DELETE /admin/topics/{name}。先清数据后删注册表
// （顺序理由见 adminops.PurgeTopicData 注释）。
func (s *Server) handleTopicDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	tc, ok := s.mt.GetTopic(name)
	if !ok {
		s.httpError(w, http.StatusNotFound, "topic %s 不存在", name)
		return
	}
	if err := adminops.PurgeTopicData(s.st, tc, s.logger); err != nil {
		s.logger.Error("admin 清理 topic 数据失败", "topic", name, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	if err := s.mt.DeleteTopic(name); err != nil {
		s.logger.Error("admin 删除 topic 注册表失败", "topic", name, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 删除 topic", "topic", name)
	w.WriteHeader(http.StatusNoContent)
}
