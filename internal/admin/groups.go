// groups.go: 消费组端点（列表/进度详情/位点重置/删除）。
//
// 职责：组×topic×queue 的消费进度推导（cursor 扫描 + alloc 差值 + inflight 计数）
// 边界：进度是抓取瞬间的快照，与在线消费存在毫秒级竞态，管理面展示可接受
package admin

import (
	"errors"
	"net/http"
	"sort"

	"github.com/xushixin/sq/internal/core/adminops"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// handleGroupsList GET /admin/groups。
func (s *Server) handleGroupsList(w http.ResponseWriter, r *http.Request) {
	gcs := s.mt.Groups()
	sort.Slice(gcs, func(i, j int) bool { return gcs[i].Name < gcs[j].Name })
	type groupJSON struct {
		Name        string `json:"name"`
		MaxAttempts int32  `json:"max_attempts"`
		CreatedAtMs int64  `json:"created_at_ms"`
	}
	out := make([]groupJSON, 0, len(gcs))
	for _, gc := range gcs {
		out = append(out, groupJSON{Name: gc.Name, MaxAttempts: gc.EffectiveMaxAttempts(), CreatedAtMs: gc.CreatedAtMs})
	}
	s.writeJSON(w, http.StatusOK, out)
}

// queueProgress 单队列消费进度。
type queueProgress struct {
	QueueID    uint32 `json:"queue_id"`
	Cursor     uint64 `json:"cursor"`
	NextOffset uint64 `json:"next_offset"`
	Pending    uint64 `json:"pending"`
	Inflight   int    `json:"inflight"`
}

// handleGroupGet GET /admin/groups/{name}：按 cursor 记录发现该组消费过的
// (topic, queue)，推导各自进度。没有 cursor 的 topic 不出现（组从未拉取过）。
func (s *Server) handleGroupGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	gc, ok := s.mt.GetGroup(name)
	if !ok {
		s.httpError(w, http.StatusNotFound, "消费组 %s 不存在", name)
		return
	}
	byTopic := map[string][]queueProgress{}
	cp := store.CursorGroupPrefix(name)
	err := s.st.Scan(cp, store.PrefixUpperBound(cp), 0, func(k, v []byte) (bool, error) {
		_, topic, q, perr := store.ParseCursorKey(k)
		if perr != nil {
			return false, perr
		}
		cur := store.GetU64(v)
		var next uint64
		if raw, ok, gerr := s.st.Get(store.AllocKey(topic, q)); gerr != nil {
			return false, gerr
		} else if ok {
			next = store.GetU64(raw)
		}
		p := queueProgress{QueueID: q, Cursor: cur, NextOffset: next}
		if next > cur {
			p.Pending = next - cur
		}
		ip := store.InflightPrefix(name, topic, q)
		if serr := s.st.Scan(ip, store.PrefixUpperBound(ip), 0, func(k2, v2 []byte) (bool, error) {
			p.Inflight++
			return true, nil
		}); serr != nil {
			return false, serr
		}
		byTopic[topic] = append(byTopic[topic], p)
		return true, nil
	})
	if err != nil {
		s.logger.Error("admin 组进度推导失败", "group", name, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	type topicProgress struct {
		Topic  string          `json:"topic"`
		Queues []queueProgress `json:"queues"`
	}
	topics := make([]topicProgress, 0, len(byTopic))
	for tp, qs := range byTopic {
		sort.Slice(qs, func(i, j int) bool { return qs[i].QueueID < qs[j].QueueID })
		topics = append(topics, topicProgress{Topic: tp, Queues: qs})
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Topic < topics[j].Topic })
	s.writeJSON(w, http.StatusOK, struct {
		Name        string          `json:"name"`
		MaxAttempts int32           `json:"max_attempts"`
		Topics      []topicProgress `json:"topics"`
	}{gc.Name, gc.EffectiveMaxAttempts(), topics})
}

// handleGroupResetCursor POST /admin/groups/{name}/reset-cursor。
// 经 deliver.ResetCursor（持队列锁），不直接写 store——理由见该方法注释。
func (s *Server) handleGroupResetCursor(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.mt.GetGroup(name); !ok {
		s.httpError(w, http.StatusNotFound, "消费组 %s 不存在", name)
		return
	}
	var req struct {
		Topic   string `json:"topic"`
		QueueID uint32 `json:"queue_id"`
		Offset  uint64 `json:"offset"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	tc, ok := s.mt.GetTopic(req.Topic)
	if !ok {
		s.httpError(w, http.StatusNotFound, "topic %s 不存在", req.Topic)
		return
	}
	// queue_id 越界会写入永远无人消费的孤儿 cursor 键，在组进度里显示为幽灵队列
	if req.QueueID >= tc.Queues {
		s.httpError(w, http.StatusBadRequest, "queue_id 越界: topic %s 共 %d 个队列，得到 %d", req.Topic, tc.Queues, req.QueueID)
		return
	}
	if err := s.dl.ResetCursor(r.Context(), name, req.Topic, req.QueueID, req.Offset); err != nil {
		s.logger.Error("admin 位点重置失败", "group", name, "topic", req.Topic,
			"queue", req.QueueID, "offset", req.Offset, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 位点重置", "group", name, "topic", req.Topic,
		"queue", req.QueueID, "offset", req.Offset)
	w.WriteHeader(http.StatusNoContent)
}

// handleGroupDelete DELETE /admin/groups/{name}。先清数据后删注册表（同 topic 删除）。
func (s *Server) handleGroupDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.mt.GetGroup(name); !ok {
		s.httpError(w, http.StatusNotFound, "消费组 %s 不存在", name)
		return
	}
	if err := adminops.PurgeGroupData(r.Context(), s.rep, s.rt, s.fwd, s.st, name, s.logger); err != nil {
		s.logger.Error("admin 清理组数据失败", "group", name, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	if err := s.mt.DeleteGroup(r.Context(), name); err != nil && !errors.Is(err, meta.ErrGroupNotFound) {
		s.logger.Error("admin 删除组注册表失败", "group", name, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 删除消费组", "group", name)
	w.WriteHeader(http.StatusNoContent)
}
