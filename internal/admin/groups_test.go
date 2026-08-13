// 消费组端点测试。
package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
)

func TestGroupProgressAndResetCursor(t *testing.T) {
	s, _, mt, pr, dl, _ := newTestServer(t, "", "")
	h := s.Handler()
	for i := 0; i < 3; i++ {
		if _, err := pr.Append(context.Background(), &core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dl.Receive(context.Background(), "g1", "t1", 0, 1, time.Minute, 0, deliver.AllPass); err != nil {
		t.Fatal(err)
	}
	// 列表
	w := doJSON(t, h, "GET", "/admin/groups", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("组列表应 200，得到 %d", w.Code)
	}
	// 详情：cursor=1 next=3 pending=2 inflight=1
	w = doJSON(t, h, "GET", "/admin/groups/g1", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("组详情应 200，得到 %d body=%s", w.Code, w.Body)
	}
	var detail struct {
		Name   string `json:"name"`
		Topics []struct {
			Topic  string `json:"topic"`
			Queues []struct {
				QueueID    uint32 `json:"queue_id"`
				Cursor     uint64 `json:"cursor"`
				NextOffset uint64 `json:"next_offset"`
				Pending    uint64 `json:"pending"`
				Inflight   int    `json:"inflight"`
			} `json:"queues"`
		} `json:"topics"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if len(detail.Topics) != 1 || len(detail.Topics[0].Queues) != 1 {
		t.Fatalf("详情结构不符: %s", w.Body)
	}
	q := detail.Topics[0].Queues[0]
	if q.Cursor != 1 || q.NextOffset != 3 || q.Pending != 2 || q.Inflight != 1 {
		t.Fatalf("进度不符: %+v", q)
	}
	// 位点重置到 0
	if w := doJSON(t, h, "POST", "/admin/groups/g1/reset-cursor", "",
		map[string]any{"topic": "t1", "queue_id": 0, "offset": 0}); w.Code != http.StatusNoContent {
		t.Fatalf("重置应 204，得到 %d body=%s", w.Code, w.Body)
	}
	got, err := dl.Receive(context.Background(), "g1", "t1", 0, 3, time.Minute, 0, deliver.AllPass)
	if err != nil || len(got) != 3 {
		t.Fatalf("重置后应从头收 3 条: %d %v", len(got), err)
	}
	// 未知组 → 404
	if w := doJSON(t, h, "GET", "/admin/groups/nope", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("未知组应 404，得到 %d", w.Code)
	}
	// 删除组
	if w := doJSON(t, h, "DELETE", "/admin/groups/g1", "", nil); w.Code != http.StatusNoContent {
		t.Fatalf("删组应 204，得到 %d", w.Code)
	}
	if _, ok := mt.GetGroup("g1"); ok {
		t.Fatal("组注册表应已删除")
	}
}

// queue_id 越界的位点重置应 400，而不是写入孤儿 cursor 键（会在组进度里显示为幽灵队列）。
func TestGroupResetCursorQueueOutOfRange(t *testing.T) {
	s, _, mt, _, _, _ := newTestServer(t, "", "")
	h := s.Handler()
	if _, err := mt.CreateTopic(context.Background(), "t1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := mt.EnsureGroup(context.Background(), "g1"); err != nil {
		t.Fatal(err)
	}
	// 1 队列 topic 上重置 queue_id=99 → 400
	if w := doJSON(t, h, "POST", "/admin/groups/g1/reset-cursor", "",
		map[string]any{"topic": "t1", "queue_id": 99, "offset": 0}); w.Code != http.StatusBadRequest {
		t.Fatalf("queue_id 越界应 400，得到 %d body=%s", w.Code, w.Body)
	}
	// 合法边界值 queue_id=0 不受影响 → 204
	if w := doJSON(t, h, "POST", "/admin/groups/g1/reset-cursor", "",
		map[string]any{"topic": "t1", "queue_id": 0, "offset": 0}); w.Code != http.StatusNoContent {
		t.Fatalf("合法 queue_id 应 204，得到 %d body=%s", w.Code, w.Body)
	}
}
