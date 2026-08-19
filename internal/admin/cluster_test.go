// cluster_test.go: GET /admin/cluster 的 handler 测试——拓扑原样吐成 JSON、
// 单机档明确回 enabled=false。
package admin

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Xsxdot/sq/internal/replication"
)

// testLogger 构造丢弃输出但保留级别过滤的测试日志器（本包测试共享）。
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeTopology 受控的集群拓扑来源。
type fakeTopology struct{ view replication.ClusterView }

func (f fakeTopology) Topology() replication.ClusterView { return f.view }

// TestHandleClusterReturnsTopology 集群端点把拓扑原样吐成 JSON。
func TestHandleClusterReturnsTopology(t *testing.T) {
	s := &Server{
		logger: testLogger(),
		topo: fakeTopology{view: replication.ClusterView{
			Enabled: true, SelfID: 2,
			Nodes: []replication.NodeView{
				{ID: 1, RaftAddr: "127.0.0.1:9081"},
				{ID: 2, RaftAddr: "127.0.0.1:9082", Self: true},
			},
			Groups: []replication.GroupView{{
				ID: 0, Leader: 1, IsLeader: false, Role: "follower", Applied: 42, Term: 3,
			}},
		}},
	}
	w := httptest.NewRecorder()
	s.handleCluster(w, httptest.NewRequest(http.MethodGet, "/admin/cluster", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("应 200，得到 %d：%s", w.Code, w.Body.String())
	}
	var got replication.ClusterView
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}
	if !got.Enabled || got.SelfID != 2 || len(got.Nodes) != 2 || len(got.Groups) != 1 {
		t.Fatalf("拓扑未原样返回: %+v", got)
	}
	if got.Groups[0].Role != "follower" || got.Groups[0].Applied != 42 {
		t.Fatalf("组视图字段不对: %+v", got.Groups[0])
	}
}

// TestHandleClusterStandaloneReportsDisabled 单机档必须明确回
// enabled=false，而不是 503——前端据此渲染「当前为单机模式」而不是报错。
func TestHandleClusterStandaloneReportsDisabled(t *testing.T) {
	s := &Server{logger: testLogger(), topo: fakeTopology{view: replication.ClusterView{Enabled: false}}}
	w := httptest.NewRecorder()
	s.handleCluster(w, httptest.NewRequest(http.MethodGet, "/admin/cluster", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("单机档应 200 且 enabled=false，得到 %d", w.Code)
	}
	var got replication.ClusterView
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got.Enabled {
		t.Fatal("单机档 enabled 应为 false")
	}
}
