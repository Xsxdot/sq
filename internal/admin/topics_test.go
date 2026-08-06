// topic 管理端点测试（fixture 与 doJSON 复用 server_test.go，同包可见）。
package admin

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/store"
)

func TestTopicCRUD(t *testing.T) {
	s, st, mt, pr, _ := newTestServer(t, "", "")
	h := s.Handler()
	// 创建
	w := doJSON(t, h, "POST", "/admin/topics", "", map[string]any{"name": "t1", "queues": 2, "retention_ms": 60000})
	if w.Code != http.StatusCreated {
		t.Fatalf("创建应 201，得到 %d body=%s", w.Code, w.Body)
	}
	// 重复创建 → 409
	if w := doJSON(t, h, "POST", "/admin/topics", "", map[string]any{"name": "t1", "queues": 2}); w.Code != http.StatusConflict {
		t.Fatalf("重复创建应 409，得到 %d", w.Code)
	}
	// 非法参数
	if w := doJSON(t, h, "POST", "/admin/topics", "", map[string]any{"name": "坏名字", "queues": 2}); w.Code != http.StatusBadRequest {
		t.Fatalf("非法名字应 400，得到 %d", w.Code)
	}
	if w := doJSON(t, h, "POST", "/admin/topics", "", map[string]any{"name": "t2", "queues": 0}); w.Code != http.StatusBadRequest {
		t.Fatalf("queues=0 应 400，得到 %d", w.Code)
	}
	// 详情含每队列 next_offset
	if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	w = doJSON(t, h, "GET", "/admin/topics/t1", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("详情应 200，得到 %d", w.Code)
	}
	var detail struct {
		Name         string `json:"name"`
		RetentionMs  int64  `json:"retention_ms"`
		QueuesDetail []struct {
			QueueID    uint32 `json:"queue_id"`
			NextOffset uint64 `json:"next_offset"`
		} `json:"queues_detail"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.RetentionMs != 60000 || len(detail.QueuesDetail) != 2 {
		t.Fatalf("详情不符: %+v", detail)
	}
	if detail.QueuesDetail[0].NextOffset+detail.QueuesDetail[1].NextOffset != 1 {
		t.Fatalf("写入 1 条后 next_offset 之和应为 1: %+v", detail.QueuesDetail)
	}
	// PATCH retention
	if w := doJSON(t, h, "PATCH", "/admin/topics/t1", "", map[string]any{"retention_ms": 120000}); w.Code != http.StatusOK {
		t.Fatalf("PATCH 应 200，得到 %d body=%s", w.Code, w.Body)
	}
	if tc, _ := mt.GetTopic("t1"); tc.RetentionMs != 120000 {
		t.Fatalf("retention 未更新: %+v", tc)
	}
	// 删除：注册表与数据都要消失
	if w := doJSON(t, h, "DELETE", "/admin/topics/t1", "", nil); w.Code != http.StatusNoContent {
		t.Fatalf("删除应 204，得到 %d", w.Code)
	}
	if _, ok := mt.GetTopic("t1"); ok {
		t.Fatal("注册表应已删除")
	}
	n := 0
	if err := st.Scan(store.MsgQueuePrefix("t1", 0), store.PrefixUpperBound(store.MsgQueuePrefix("t1", 0)), 0,
		func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("消息数据应已清理，剩 %d", n)
	}
	// 删不存在的 → 404
	if w := doJSON(t, h, "DELETE", "/admin/topics/t1", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("重复删除应 404，得到 %d", w.Code)
	}
}

// 创建时负 retention_ms 应 400（与 PATCH 校验对齐），0 保持原语义（用默认值）。
func TestTopicCreateNegativeRetention(t *testing.T) {
	s, _, _, _, _ := newTestServer(t, "", "")
	h := s.Handler()
	if w := doJSON(t, h, "POST", "/admin/topics", "",
		map[string]any{"name": "t1", "queues": 1, "retention_ms": -1}); w.Code != http.StatusBadRequest {
		t.Fatalf("负 retention_ms 应 400，得到 %d body=%s", w.Code, w.Body)
	}
	// retention_ms=0 创建成功（走默认 retention）
	if w := doJSON(t, h, "POST", "/admin/topics", "",
		map[string]any{"name": "t1", "queues": 1, "retention_ms": 0}); w.Code != http.StatusCreated {
		t.Fatalf("retention_ms=0 应 201，得到 %d body=%s", w.Code, w.Body)
	}
}
