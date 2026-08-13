// 时序曲线与消费关系总账端点的测试。
// fixture 复用 server_test.go 的 newTestServer/newTestServerNoSampler；
// seedConsumption 参照 groups_test.go 的消费姿势制造 cursor 与 inflight。
package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/produce"
)

// seedConsumption 制造一条消费关系：向 t1 写入 3 条、拉取 1 条不 ack，
// 得到 cursor=1 / next=3 / pending=2 / inflight=1（TestLedgerShape 的断言基准）。
func seedConsumption(t *testing.T, pr *produce.Producer, dl *deliver.Deliverer) {
	t.Helper()
	for i := 0; i < 3; i++ {
		if _, err := pr.Append(context.Background(), &core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dl.Receive(context.Background(), "g1", "t1", 0, 1, time.Minute, 0, deliver.AllPass); err != nil {
		t.Fatal(err)
	}
}

func TestTimeseriesRangeValidation(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t, "", "")

	// 非法 range 必须 400 而不是静默按默认值处理：拼错参数却拿到一条
	// 看起来正常的曲线，是最难发现的一类问题
	req := httptest.NewRequest(http.MethodGet, "/admin/timeseries?range=30m", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("非法 range 应 400，得到 %d", w.Code)
	}

	// 缺省 range = 1h
	req = httptest.NewRequest(http.MethodGet, "/admin/timeseries", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("缺省 range 应 200，得到 %d：%s", w.Code, w.Body.String())
	}
	var got struct {
		Range         string `json:"range"`
		GranularityMs int64  `json:"granularity_ms"`
		Source        string `json:"source"`
		Points        []any  `json:"points"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("解析响应: %v", err)
	}
	if got.Range != "1h" || got.Source != "ring" || got.GranularityMs != 5000 {
		t.Fatalf("1h 应走内存环、5 秒粒度，得到 %+v", got)
	}
}

func TestTimeseriesWithoutSampler(t *testing.T) {
	srv, _, _, _, _, _ := newTestServerNoSampler(t, "", "")
	req := httptest.NewRequest(http.MethodGet, "/admin/timeseries", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	// 采样器没启用时给 503 + 明确原因，而不是空数组——空数组会被
	// 前端画成一条平的零线，看起来像「系统完全没流量」
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("未启用采样器应 503，得到 %d", w.Code)
	}
}

func TestLedgerShape(t *testing.T) {
	srv, _, _, pr, dl, _ := newTestServer(t, "", "")
	seedConsumption(t, pr, dl) // 造一个组在一个 topic 上有 cursor 与 inflight

	req := httptest.NewRequest(http.MethodGet, "/admin/ledger", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ledger 应 200，得到 %d：%s", w.Code, w.Body.String())
	}
	var rows []struct {
		Group      string `json:"group"`
		Topic      string `json:"topic"`
		Cursor     uint64 `json:"cursor"`
		NextOffset uint64 `json:"next_offset"`
		Pending    uint64 `json:"pending"`
		Inflight   int    `json:"inflight"`
		Queues     []struct {
			QueueID    uint32 `json:"queue_id"`
			Cursor     uint64 `json:"cursor"`
			NextOffset uint64 `json:"next_offset"`
		} `json:"queues"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil {
		t.Fatalf("解析 ledger: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("应至少有一行消费关系")
	}
	r := rows[0]
	if len(r.Queues) == 0 {
		t.Fatal("每行应带队列级明细（总览表展开要用）")
	}
	// 行级读数必须等于队列级之和，否则展开后对不上账
	var cur, next uint64
	for _, q := range r.Queues {
		cur += q.Cursor
		next += q.NextOffset
	}
	if r.Cursor != cur || r.NextOffset != next {
		t.Fatalf("行级读数应等于队列级之和：行 cursor=%d/next=%d，队列和 %d/%d",
			r.Cursor, r.NextOffset, cur, next)
	}
	if r.Pending != next-cur {
		t.Fatalf("pending 应等于 next-cursor，得到 %d", r.Pending)
	}
}
