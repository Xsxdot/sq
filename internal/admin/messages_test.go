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
)

func TestMessagesSendBrowseAndKeyQuery(t *testing.T) {
	s, _, _, pr, _ := newTestServer(t, "", "")
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
	s, _, _, pr, dl := newTestServer(t, "", "")
	h := s.Handler()
	// 构造一条死信：与 moveToDLQ 的写入形状一致（origin 坐标在 Properties）。
	// 不驱动真实重试超限（那是 deliver 测试的职责），这里只验证重发路径。
	dlqTopic := meta.DLQTopicName("g1")
	if _, err := pr.Append(&core.Message{
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
	s, _, _, pr, _ := newTestServer(t, "", "")
	h := s.Handler()
	due := time.Now().Add(time.Hour).UnixMilli()
	if _, err := pr.AppendDelay(&core.Message{Topic: "t1", Body: []byte("later"), DeliverAtMs: due}); err != nil {
		t.Fatal(err)
	}
	if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("now")}); err != nil {
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
