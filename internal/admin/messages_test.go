// 消息查询/测试发送/DLQ 重发/延时视图/总览端点测试。
package admin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/txn"
	"github.com/xushixin/sq/internal/store"
)

func TestMessagesSendBrowseAndKeyQuery(t *testing.T) {
	s, _, _, pr, _, _ := newTestServer(t, "", "")
	h := s.Handler()
	// 测试发送
	w := doJSON(t, h, "POST", "/admin/messages/send", "",
		map[string]any{"topic": "t1", "body": "hello", "tag": "tg", "keys": []string{"k1"}})
	if w.Code != http.StatusCreated {
		t.Fatalf("发送应 201，得到 %d body=%s", w.Code, w.Body)
	}
	var sent struct {
		MsgID   string `json:"msg_id"`
		QueueID uint32 `json:"queue_id"`
		Offset  uint64 `json:"offset"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &sent); err != nil || sent.MsgID == "" {
		t.Fatalf("应返回 msg_id: %s %v", w.Body, err)
	}
	// 浏览：body 以 base64 返回
	w = doJSON(t, h, "GET", "/admin/messages?topic=t1&queue_id=0&from_offset=0&limit=10", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("浏览应 200，得到 %d body=%s", w.Code, w.Body)
	}
	var msgs []struct {
		ID         string `json:"id"`
		Tag        string `json:"tag"`
		BodyBase64 string `json:"body_base64"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &msgs); err != nil || len(msgs) != 1 {
		t.Fatalf("应 1 条: %s %v", w.Body, err)
	}
	if b, _ := base64.StdEncoding.DecodeString(msgs[0].BodyBase64); string(b) != "hello" || msgs[0].Tag != "tg" {
		t.Fatalf("内容不符: %+v", msgs[0])
	}
	// Keys 查询走 keyidx
	w = doJSON(t, h, "GET", "/admin/messages?topic=t1&key=k1", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("key 查询应 200，得到 %d", w.Code)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &msgs); err != nil || len(msgs) != 1 {
		t.Fatalf("key 查询应 1 条: %s", w.Body)
	}
	// 参数校验：既无 key 也无 queue_id → 400
	if w := doJSON(t, h, "GET", "/admin/messages?topic=t1", "", nil); w.Code != http.StatusBadRequest {
		t.Fatalf("缺参数应 400，得到 %d", w.Code)
	}
	_ = pr
}

func TestDLQResend(t *testing.T) {
	s, _, _, pr, dl, _ := newTestServer(t, "", "")
	h := s.Handler()
	// 构造一条死信：与 moveToDLQ 的写入形状一致（origin 坐标在 Properties）。
	// 不驱动真实重试超限（那是 deliver 测试的职责），这里只验证重发路径。
	dlqTopic := meta.DLQTopicName("g1")
	if _, err := pr.Append(context.Background(), &core.Message{
		ID: "dead-1", Topic: dlqTopic, Body: []byte("dead"),
		Properties: map[string]string{
			"sq-origin-topic": "t-orig", "sq-origin-queue": "0", "sq-origin-offset": "5",
		},
	}); err != nil {
		t.Fatal(err)
	}
	w := doJSON(t, h, "POST", "/admin/dlq/g1/resend", "", map[string]any{"queue_id": 0, "offset": 0})
	if w.Code != http.StatusCreated {
		t.Fatalf("重发应 201，得到 %d body=%s", w.Code, w.Body)
	}
	// 原 topic 里能消费到重发的消息
	got, err := dl.Receive(context.Background(), "g-verify", "t-orig", 0, 1, time.Minute, 0, nil)
	if err != nil || len(got) != 1 || string(got[0].Body) != "dead" || got[0].ID != "dead-1" {
		t.Fatalf("原 topic 应收到重发消息: %+v %v", got, err)
	}
	// 死信条目保留（审计需要），重复重发仍可用
	if w := doJSON(t, h, "POST", "/admin/dlq/g1/resend", "", map[string]any{"queue_id": 0, "offset": 0}); w.Code != http.StatusCreated {
		t.Fatalf("重复重发应可行，得到 %d", w.Code)
	}
	// 不存在的死信 → 404
	if w := doJSON(t, h, "POST", "/admin/dlq/g1/resend", "", map[string]any{"queue_id": 0, "offset": 99}); w.Code != http.StatusNotFound {
		t.Fatalf("不存在的死信应 404，得到 %d", w.Code)
	}
}

func TestDelayViewAndOverview(t *testing.T) {
	s, _, _, pr, _, _ := newTestServer(t, "", "")
	h := s.Handler()
	due := time.Now().Add(time.Hour).UnixMilli()
	if _, err := pr.AppendDelay(context.Background(), &core.Message{Topic: "t1", Body: []byte("later"), DeliverAtMs: due}); err != nil {
		t.Fatal(err)
	}
	if _, err := pr.Append(context.Background(), &core.Message{Topic: "t1", Body: []byte("now")}); err != nil {
		t.Fatal(err)
	}
	w := doJSON(t, h, "GET", "/admin/delay?limit=10", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("延时视图应 200，得到 %d", w.Code)
	}
	var entries []struct {
		DueMs int64  `json:"due_ms"`
		MsgID string `json:"msg_id"`
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &entries); err != nil || len(entries) != 1 {
		t.Fatalf("应 1 条延时: %s", w.Body)
	}
	if entries[0].DueMs != due || entries[0].Topic != "t1" {
		t.Fatalf("延时条目不符: %+v（期望 due=%s）", entries[0], strconv.FormatInt(due, 10))
	}
	// 总览
	w = doJSON(t, h, "GET", "/admin/overview", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("总览应 200，得到 %d", w.Code)
	}
	var ov struct {
		Topics       int    `json:"topics"`
		Groups       int    `json:"groups"`
		DelayDepth   int    `json:"delay_depth"`
		TotalWritten uint64 `json:"total_written"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &ov); err != nil {
		t.Fatal(err)
	}
	if ov.Topics != 1 || ov.DelayDepth != 1 || ov.TotalWritten != 1 {
		t.Fatalf("总览不符: %+v", ov)
	}
}

// fakeConns 测试用 ConnCounter：返回固定连接数（总览 connections 断言用）。
type fakeConns struct{ n int }

func (f fakeConns) ConnectionCount() int { return f.n }

// TestAdminTransactionsList 事务视图：直写一条 half+halfidx 条目（形状与
// txn.Stage 一致），GET /admin/transactions?limit=10 应返回该条且字段齐全。
// 另放一条坏 key（前缀后仅 1B，ParseHalfKey 必拒，且字典序排在所有合法
// half 键之前、正好在扫描头部）：旧实现直接中断整趟返回 500，修复后应与
// 解码失败分支一致——只读跳过，健康条目照常返回。
func TestAdminTransactionsList(t *testing.T) {
	s, st, _, _, _, _ := newTestServer(t, "", "")
	h := s.Handler()
	next := time.Now().Add(time.Hour).UnixMilli()
	msg := &core.Message{ID: "txn-msg-1", Topic: "t1", Body: []byte("txn"), BornAtMs: 1723000000000}
	raw, err := core.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := json.Marshal(&txn.HalfRef{NextCheckMs: next, Checks: 3}) // 结构固定无失败路径
	b := st.NewBatch()
	b.Set(append([]byte(store.HalfPrefix), 0x00), []byte("whatever")) // 坏 key 排在扫描头部
	b.Set(store.HalfKey(next, "TX0001"), raw)
	b.Set(store.HalfIdxKey("TX0001"), ref)
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	w := doJSON(t, h, "GET", "/admin/transactions?limit=10", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("事务视图应 200，得到 %d body=%s", w.Code, w.Body)
	}
	var entries []struct {
		TxID        string `json:"tx_id"`
		MsgID       string `json:"msg_id"`
		Topic       string `json:"topic"`
		NextCheckMs int64  `json:"next_check_ms"`
		Checks      int    `json:"checks"`
		BornMs      int64  `json:"born_ms"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &entries); err != nil || len(entries) != 1 {
		t.Fatalf("应 1 条半消息: %s %v", w.Body, err)
	}
	if e := entries[0]; e.TxID != "TX0001" || e.MsgID != "txn-msg-1" || e.Topic != "t1" ||
		e.NextCheckMs != next || e.Checks != 3 || e.BornMs != 1723000000000 {
		t.Fatalf("事务条目不符: %+v", entries[0])
	}
}

// TestAdminOverviewHasHalfDepthAndConnections 总览必须带 half_depth 与
// connections 两键：fake ConnCounter 返回 2 时 connections == 2；直写一条
// 半消息后 half_depth == 1。nil conns（helper 默认形态）时 connections 回 0。
func TestAdminOverviewHasHalfDepthAndConnections(t *testing.T) {
	s, st, _, _, _, _ := newTestServerWithConns(t, "", "", fakeConns{2})
	h := s.Handler()
	next := time.Now().Add(time.Hour).UnixMilli()
	msg := &core.Message{ID: "txn-msg-2", Topic: "t1", Body: []byte("txn")}
	raw, err := core.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := json.Marshal(&txn.HalfRef{NextCheckMs: next, Checks: 0})
	b := st.NewBatch()
	b.Set(store.HalfKey(next, "TX0002"), raw)
	b.Set(store.HalfIdxKey("TX0002"), ref)
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	w := doJSON(t, h, "GET", "/admin/overview", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("总览应 200，得到 %d", w.Code)
	}
	var ov struct {
		HalfDepth   int `json:"half_depth"`
		Connections int `json:"connections"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &ov); err != nil {
		t.Fatal(err)
	}
	if ov.HalfDepth != 1 {
		t.Fatalf("half_depth 应为 1，得到 %d", ov.HalfDepth)
	}
	if ov.Connections != 2 {
		t.Fatalf("connections 应为 2（fake 返回 2），得到 %d", ov.Connections)
	}
	// nil conns 容忍：helper 默认传 nil，connections 回 0 而非 500
	s2, _, _, _, _, _ := newTestServer(t, "", "")
	w2 := doJSON(t, s2.Handler(), "GET", "/admin/overview", "", nil)
	var ov2 struct {
		Connections int `json:"connections"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &ov2); err != nil || ov2.Connections != 0 {
		t.Fatalf("nil conns 时 connections 应为 0: %+v %v", ov2, err)
	}
}

// TestOverviewCarriesQPSAndDLQ 总览必须把死信从业务写入量中剔除并单列 total_dlq，
// qps 在采样器无样本时为 null（语义：不知道），有样本时给出数值。
func TestOverviewCarriesQPSAndDLQ(t *testing.T) {
	srv, _, _, pr, _, sp := newTestServer(t, "", "")
	h := srv.Handler()

	// 空环语义：采样器还没采过样时 qps 应为 null 而非 0——0 意味着「确实没有流量」，
	// null 意味着「不知道」。newTestServer 不调 Sampler.Run，环恒为空。
	w := doJSON(t, h, "GET", "/admin/overview", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("overview 应 200，得到 %d", w.Code)
	}
	var got struct {
		Topics       int      `json:"topics"`
		TotalWritten uint64   `json:"total_written"`
		TotalDLQ     uint64   `json:"total_dlq"`
		QPS          *float64 `json:"qps"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("解析 overview: %v", err)
	}
	if got.QPS != nil {
		t.Fatalf("空环时 qps 应为 null，得到 %v", *got.QPS)
	}

	// 业务 topic 写入 2 条；死信 topic 写入 3 条（pr.Append 内部 EnsureTopic 自动建 topic）
	for i := 0; i < 2; i++ {
		if _, err := pr.Append(context.Background(), &core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := pr.Append(context.Background(), &core.Message{Topic: meta.DLQTopicName("notify-svc"), Body: []byte("dead")}); err != nil {
			t.Fatal(err)
		}
	}

	// 启动采样器并等首个样本入环：qps 需要采样器提供，Run 首 tick 立即采一次
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sp.Run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := sp.Latest(); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("采样器未在 2s 内产生首个样本")
		}
		time.Sleep(10 * time.Millisecond)
	}

	w = doJSON(t, h, "GET", "/admin/overview", "", nil)
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("解析 overview: %v", err)
	}
	if got.TotalDLQ != 3 {
		t.Fatalf("total_dlq 应为 3，得到 %d", got.TotalDLQ)
	}
	// 死信不该混进业务写入量：一次故障不应看起来像一次流量高峰
	if got.TotalWritten != 2 {
		t.Fatalf("total_written 不应包含死信，期望 2 得到 %d", got.TotalWritten)
	}
	// 死信 topic 是系统自建的，不该计入用户建的 topic 数
	if got.Topics != 1 {
		t.Fatalf("topics 应剔除死信 topic，期望 1 得到 %d", got.Topics)
	}
	if got.QPS == nil {
		t.Fatal("采样器已产生样本时 qps 不应为 null")
	}
}

// TestSendDelayAndFIFO 测试发送支持延时（进 delay 暂存区）与顺序（同组同队列）。
func TestSendDelayAndFIFO(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t, "", "")
	h := srv.Handler()

	// 延时消息：进 delay 暂存区，不进正常队列——响应必须带 deliver_at_ms
	w := doJSON(t, h, "POST", "/admin/messages/send", "", map[string]any{"topic": "tdelay", "body": "hi", "delay_ms": 60000})
	if w.Code != http.StatusCreated {
		t.Fatalf("延时发送应 201，得到 %d：%s", w.Code, w.Body)
	}
	var got struct {
		MsgID       string `json:"msg_id"`
		DeliverAtMs int64  `json:"deliver_at_ms"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("解析响应: %v", err)
	}
	if got.DeliverAtMs == 0 {
		t.Fatal("延时消息响应应带 deliver_at_ms")
	}

	// 顺序消息：同一 MessageGroup 必须落到同一队列，否则顺序语义不成立
	var qids []uint32
	for i := 0; i < 3; i++ {
		w := doJSON(t, h, "POST", "/admin/messages/send", "", map[string]any{"topic": "tfifo", "body": "x", "message_group": "ORD-1"})
		if w.Code != http.StatusCreated {
			t.Fatalf("顺序发送应 201，得到 %d：%s", w.Code, w.Body)
		}
		var r struct {
			QueueID uint32 `json:"queue_id"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
			t.Fatalf("解析响应: %v", err)
		}
		qids = append(qids, r.QueueID)
	}
	if qids[0] != qids[1] || qids[1] != qids[2] {
		t.Fatalf("同一 MessageGroup 应落同一队列，得到 %v", qids)
	}

	// delay_ms 与 message_group 同时给出是语义冲突，必须 400
	if w := doJSON(t, h, "POST", "/admin/messages/send", "", map[string]any{"topic": "tx", "body": "x", "delay_ms": 1000, "message_group": "G"}); w.Code != http.StatusBadRequest {
		t.Fatalf("延时+顺序组合应 400，得到 %d", w.Code)
	}
}

// followerRouter 恒非 leader 的路由替身：本节点不 lead 任何组。
type followerRouter struct{}

func (followerRouter) GroupForQueue(string, uint32) uint32       { return 0 }
func (followerRouter) MetaGroup() uint32                         { return 0 }
func (followerRouter) IsLeader(uint32) bool                      { return false }
func (followerRouter) ReadBarrier(context.Context, uint32) error { return nil }

// recordingForwarder 记录 ForwardAppend 被调用的假转发器。
type recordingForwarder struct {
	called  bool
	queueID uint32
	offset  uint64
}

func (f *recordingForwarder) ForwardAppend(ctx context.Context, g uint32, msgRaw []byte) (uint32, uint64, error) {
	f.called = true
	return f.queueID, f.offset, nil
}

func (f *recordingForwarder) ForwardApply(ctx context.Context, g uint32, repr []byte) error {
	return nil
}

// TestMessageSendOnFollowerForwardsToLeader 控制台的发送测试消息在
// follower 上必须经转发落到 leader，而不是把 ErrNotLeader 甩给用户。
//
// 为什么不是"提示用户换节点"：控制台的地址通常是运维随手挑的一个节点，
// 要求用户自己找出 leader 再重开一个页面，是把系统内部状态外包给人。
func TestMessageSendOnFollowerForwardsToLeader(t *testing.T) {
	s, _, _, _, _, _ := newTestServer(t, "", "")
	fwd := &recordingForwarder{queueID: 7, offset: 42}
	s.rt = followerRouter{}
	s.fwd = fwd
	h := s.Handler()

	w := doJSON(t, h, "POST", "/admin/messages/send", "",
		map[string]any{"topic": "t", "body": "hello"})
	if w.Code != http.StatusCreated {
		t.Fatalf("follower 上的发送应经转发成功，得到 %d：%s", w.Code, w.Body)
	}
	if !fwd.called {
		t.Fatal("未经 ForwardAppend 转发（把 ErrNotLeader 甩给用户是 UX 缺陷）")
	}
	var got struct {
		Forwarded bool `json:"forwarded"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if !got.Forwarded {
		t.Fatal("响应必须带 forwarded=true——用户有权知道这条走了转发")
	}
}
