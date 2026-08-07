// messages.go: 消息面端点——查询/浏览、测试发送、DLQ 重发、延时视图、总览。
//
// 职责：REST 参数解析 + 组合 query/produce/metrics 既有能力
// 边界：
//   - msgId 精确查询不做（需要独立 msgidx，v1 用 Keys/浏览覆盖排查主路径，
//     决策记录见计划 Self-Review）
//   - 消息体一律 base64 返回（body 可能是任意二进制）
package admin

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/query"
	"github.com/xushixin/sq/internal/core/txn"
	"github.com/xushixin/sq/internal/metrics"
	"github.com/xushixin/sq/internal/store"
)

// msgJSON 消息的 API 序列化形状。
type msgJSON struct {
	ID           string            `json:"id"`
	Topic        string            `json:"topic"`
	QueueID      uint32            `json:"queue_id"`
	Offset       uint64            `json:"offset"`
	Tag          string            `json:"tag,omitempty"`
	Keys         []string          `json:"keys,omitempty"`
	MessageGroup string            `json:"message_group,omitempty"`
	Properties   map[string]string `json:"properties,omitempty"`
	BodyBase64   string            `json:"body_base64"`
	BornAtMs     int64             `json:"born_at_ms"`
	StoreAtMs    int64             `json:"store_at_ms"`
	DeliverAtMs  int64             `json:"deliver_at_ms,omitempty"`
}

func toMsgJSON(m *core.Message) msgJSON {
	return msgJSON{
		ID: m.ID, Topic: m.Topic, QueueID: m.QueueID, Offset: m.Offset,
		Tag: m.Tag, Keys: m.Keys, MessageGroup: m.MessageGroup, Properties: m.Properties,
		BodyBase64: base64.StdEncoding.EncodeToString(m.Body),
		BornAtMs:   m.BornAtMs, StoreAtMs: m.StoreAtMs, DeliverAtMs: m.DeliverAtMs,
	}
}

// queryUint 解析 uint 型查询参数；缺省返回 def。非法值由调用方拿 err 转 400。
func queryUint(r *http.Request, name string, def uint64) (uint64, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, nil
	}
	return strconv.ParseUint(v, 10, 64)
}

// handleMessagesQuery GET /admin/messages：key 非空走 keyidx（Keys 查询），
// 否则 queue_id 必填走顺序浏览。两条路径都要求 topic。
func (s *Server) handleMessagesQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	if topic == "" {
		s.httpError(w, http.StatusBadRequest, "缺少 topic 参数")
		return
	}
	limit64, err := queryUint(r, "limit", 0)
	if err != nil {
		s.httpError(w, http.StatusBadRequest, "limit 非法: %v", err)
		return
	}
	var msgs []*core.Message
	if key := q.Get("key"); key != "" {
		msgs, err = query.ByKey(s.st, topic, key, int(limit64))
	} else if q.Get("queue_id") != "" {
		var qid, from uint64
		if qid, err = queryUint(r, "queue_id", 0); err != nil {
			s.httpError(w, http.StatusBadRequest, "queue_id 非法: %v", err)
			return
		}
		if from, err = queryUint(r, "from_offset", 0); err != nil {
			s.httpError(w, http.StatusBadRequest, "from_offset 非法: %v", err)
			return
		}
		msgs, err = query.Browse(s.st, topic, uint32(qid), from, int(limit64))
	} else {
		s.httpError(w, http.StatusBadRequest, "必须提供 key（Keys 查询）或 queue_id（顺序浏览）之一")
		return
	}
	if err != nil {
		s.logger.Error("admin 消息查询失败", "topic", topic, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	out := make([]msgJSON, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, toMsgJSON(m))
	}
	s.writeJSON(w, http.StatusOK, out)
}

// handleMessageSend POST /admin/messages/send：控制台"发送测试消息"。
// 与 gRPC 写路径同受磁盘水位拒写约束——管理面不该有绕过保护的后门。
func (s *Server) handleMessageSend(w http.ResponseWriter, r *http.Request) {
	if s.blocked() {
		s.httpError(w, http.StatusServiceUnavailable, "磁盘水位超限，写入已暂停")
		return
	}
	var req struct {
		Topic        string   `json:"topic"`
		Body         string   `json:"body"`
		Tag          string   `json:"tag"`
		Keys         []string `json:"keys"`
		DelayMs      int64    `json:"delay_ms"`      // >0 = 延时消息，相对当前时刻
		MessageGroup string   `json:"message_group"` // 非空 = 顺序消息
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if req.Topic == "" {
		s.httpError(w, http.StatusBadRequest, "缺少 topic")
		return
	}
	// 延时与顺序不能同时给：延时消息到期后才进队列，此时它相对同组其他消息的
	// 先后已经由到期时间决定，MessageGroup 承诺的「按发送顺序投递」无从保证。
	// 这与 rpc 层对 SDK 的组合校验是同一条规则，管理面不该有更宽的口子。
	if req.DelayMs > 0 && req.MessageGroup != "" {
		s.httpError(w, http.StatusBadRequest, "delay_ms 与 message_group 不能同时指定")
		return
	}
	if _, err := s.mt.EnsureTopic(req.Topic); err != nil {
		s.httpError(w, http.StatusBadRequest, "topic 不可用: %v", err)
		return
	}
	now := time.Now().UnixMilli()
	m := &core.Message{
		ID: core.NewMessageID(), Topic: req.Topic, Tag: req.Tag, Keys: req.Keys,
		MessageGroup: req.MessageGroup,
		Body:         []byte(req.Body), BornAtMs: now, BornHost: "admin",
	}
	var err error
	if req.DelayMs > 0 {
		m.DeliverAtMs = now + req.DelayMs
		m, err = s.pr.AppendDelay(m)
	} else {
		m, err = s.pr.Append(m)
	}
	if err != nil {
		s.logger.Error("admin 测试消息发送失败", "topic", req.Topic,
			"delay_ms", req.DelayMs, "message_group", req.MessageGroup, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 测试消息已发送", "topic", req.Topic, "msg_id", m.ID,
		"queue", m.QueueID, "offset", m.Offset, "deliver_at_ms", m.DeliverAtMs)
	s.writeJSON(w, http.StatusCreated, map[string]any{
		"msg_id": m.ID, "queue_id": m.QueueID, "offset": m.Offset,
		"deliver_at_ms": m.DeliverAtMs,
	})
}

// handleDLQResend POST /admin/dlq/{group}/resend：把死信按 sq-origin-topic
// 属性重新投回原 topic。死信条目保留（审计与再次重发），与 RocketMQ 控制台
// 行为一致。
func (s *Server) handleDLQResend(w http.ResponseWriter, r *http.Request) {
	if s.blocked() {
		s.httpError(w, http.StatusServiceUnavailable, "磁盘水位超限，写入已暂停")
		return
	}
	group := r.PathValue("group")
	var req struct {
		QueueID uint32 `json:"queue_id"`
		Offset  uint64 `json:"offset"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	dlqTopic := meta.DLQTopicName(group)
	raw, ok, err := s.st.Get(store.MsgKey(dlqTopic, req.QueueID, req.Offset))
	if err != nil {
		s.logger.Error("admin 读死信失败", "group", group, "offset", req.Offset, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	if !ok {
		s.httpError(w, http.StatusNotFound, "死信不存在 (dlq=%s q=%d off=%d)", dlqTopic, req.QueueID, req.Offset)
		return
	}
	m, err := core.DecodeMessage(raw)
	if err != nil {
		s.logger.Error("admin 死信解码失败", "group", group, "offset", req.Offset, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	origin := m.Properties["sq-origin-topic"]
	if origin == "" {
		// 老死信或手工放入的消息没有溯源属性——没有目的地，重发无从谈起
		s.httpError(w, http.StatusUnprocessableEntity, "死信缺少 sq-origin-topic 溯源属性，无法定位原 topic")
		return
	}
	if _, err := s.mt.EnsureTopic(origin); err != nil {
		s.httpError(w, http.StatusBadRequest, "原 topic %s 不可用: %v", origin, err)
		return
	}
	// ID 保留：全链路追踪同一条消息；Properties 保留溯源坐标（再次超限入 DLQ
	// 时会被 moveToDLQ 覆盖为新坐标）；MessageGroup 不恢复——死信重发回普通消息
	resend := &core.Message{
		ID: m.ID, Topic: origin, Tag: m.Tag, Keys: m.Keys,
		Properties: m.Properties, Body: m.Body, BornAtMs: m.BornAtMs, BornHost: m.BornHost,
	}
	resend, err = s.pr.Append(resend)
	if err != nil {
		s.logger.Error("admin 死信重发失败", "group", group, "origin", origin, "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 死信已重发", "group", group, "msg_id", resend.ID,
		"origin_topic", origin, "queue", resend.QueueID, "offset", resend.Offset)
	s.writeJSON(w, http.StatusCreated, map[string]any{
		"msg_id": resend.ID, "topic": origin, "queue_id": resend.QueueID, "offset": resend.Offset,
	})
}

// handleDelayList GET /admin/delay：延时暂存区头部条目（按到期时间升序）。
func (s *Server) handleDelayList(w http.ResponseWriter, r *http.Request) {
	limit64, err := queryUint(r, "limit", 64)
	if err != nil {
		s.httpError(w, http.StatusBadRequest, "limit 非法: %v", err)
		return
	}
	type delayEntry struct {
		DueMs int64  `json:"due_ms"`
		MsgID string `json:"msg_id"`
		Topic string `json:"topic"`
	}
	out := []delayEntry{}
	dp := []byte(store.DelayPrefix)
	err = s.st.Scan(dp, store.PrefixUpperBound(dp), int(limit64), func(k, v []byte) (bool, error) {
		due, _, perr := store.ParseDelayKey(k)
		if perr != nil {
			return false, perr
		}
		m, derr := core.DecodeMessage(v)
		if derr != nil {
			// 坏条目由 delay 调度器负责清理（那里会删除并 Error 留痕），
			// 管理面只读，跳过即可
			s.logger.Warn("admin 延时视图跳过坏条目", "key", string(k), "err", derr)
			return true, nil
		}
		out = append(out, delayEntry{DueMs: due, MsgID: m.ID, Topic: m.Topic})
		return true, nil
	})
	if err != nil {
		s.logger.Error("admin 延时视图扫描失败", "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.writeJSON(w, http.StatusOK, out)
}

// handleTransactionsList GET /admin/transactions：待决半消息（按下次回查时间升序）。
// halfidx 里的 Checks 靠二次 Get 取——列表是排查用的低频操作，N+1 读可接受。
func (s *Server) handleTransactionsList(w http.ResponseWriter, r *http.Request) {
	limit64, err := queryUint(r, "limit", 64)
	if err != nil {
		s.httpError(w, http.StatusBadRequest, "limit 非法: %v", err)
		return
	}
	type txnEntry struct {
		TxID        string `json:"tx_id"`
		MsgID       string `json:"msg_id"`
		Topic       string `json:"topic"`
		NextCheckMs int64  `json:"next_check_ms"`
		Checks      int    `json:"checks"`
		BornMs      int64  `json:"born_ms"`
	}
	out := []txnEntry{}
	hp := []byte(store.HalfPrefix)
	err = s.st.Scan(hp, store.PrefixUpperBound(hp), int(limit64), func(k, v []byte) (bool, error) {
		next, txID, perr := store.ParseHalfKey(k)
		if perr != nil {
			// 坏 key 由回查调度器 ~1s 内删除并 Error 留痕（那里是唯一写入口），
			// 管理面只读跳过即可——与下方解码失败分支的处置一致，不能中断整个
			// 扫描把后面的健康条目一起 500 掉
			s.logger.Warn("admin 事务视图跳过坏 key", "key", string(k), "err", perr)
			return true, nil
		}
		m, derr := core.DecodeMessage(v)
		if derr != nil {
			// 坏条目由回查调度器负责清理（那里删除并 Error 留痕），管理面只读跳过
			s.logger.Warn("admin 事务视图跳过坏条目", "key", string(k), "err", derr)
			return true, nil
		}
		checks := 0
		if refRaw, ok, _ := s.st.Get(store.HalfIdxKey(txID)); ok {
			ref := &txn.HalfRef{}
			if json.Unmarshal(refRaw, ref) == nil {
				checks = ref.Checks
			}
		}
		out = append(out, txnEntry{TxID: txID, MsgID: m.ID, Topic: m.Topic,
			NextCheckMs: next, Checks: checks, BornMs: m.BornAtMs})
		return true, nil
	})
	if err != nil {
		s.logger.Error("admin 事务视图扫描失败", "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.writeJSON(w, http.StatusOK, out)
}

// handleOverview GET /admin/overview：总览计数（复用 metrics.Collect，
// 控制台图表与 Prometheus 看到同一份事实源）。
//
// 死信从 total_written 里剔除并单列 total_dlq：把死信算进「写入量」会让
// 一次故障看起来像一次流量高峰，这是总览页最不该出现的误导。
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	st, err := metrics.Collect(s.st, s.mt)
	if err != nil {
		s.logger.Error("admin 总览采集失败", "err", err)
		s.httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	var written, pending, dlq uint64
	var inflight int
	for topic, n := range st.Written {
		if meta.IsDLQTopic(topic) {
			dlq += n
			continue
		}
		written += n
	}
	for _, n := range st.Pending {
		pending += n
	}
	for _, n := range st.Inflight {
		inflight += n
	}
	// qps 只有采样器启用时才有：它需要两个时刻的差分，单次快照给不出速率。
	// 未启用时给 null 而不是 0——0 表示「确实没有流量」，null 表示「不知道」。
	var qps *float64
	if s.sp != nil {
		if p, ok := s.sp.Latest(); ok {
			v := p.QPS
			qps = &v
		}
	}
	// topics 计数同样剔除死信 topic：死信队列是系统自建的，不是用户建的 topic
	topics := 0
	for _, tc := range s.mt.Topics() {
		if !meta.IsDLQTopic(tc.Name) {
			topics++
		}
	}
	// connections 来自 rpc.Server 的在线会话数；conns 为 nil（测试构造）时回 0
	conns := 0
	if s.conns != nil {
		conns = s.conns.ConnectionCount()
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"topics": topics, "groups": st.Groups, "delay_depth": st.DelayDepth,
		"half_depth": st.HalfDepth, "connections": conns,
		"total_written": written, "total_pending": pending, "total_inflight": inflight,
		"total_dlq": dlq, "qps": qps,
	})
}
