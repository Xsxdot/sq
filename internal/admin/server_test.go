// Admin HTTP 骨架测试：登录流程、token 门禁、免登录直通。
// 用 httptest + 真实 store/meta/produce/deliver fixture（不 mock core）。
package admin

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core/deliver"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/metrics"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
	"github.com/Xsxdot/sq/internal/sysinfo"
)

// newTestServer 构造 admin Server 与其依赖。user/pass 均空 = 免登录。
// 返回尾部多一个 *metrics.Sampler：时序/总账端点测试需要它（空环查询安全）。
// conns 传 nil：总览 connections 回 0（nil 容忍的常规形态）。
func newTestServer(t *testing.T, user, pass string) (*Server, *store.Store, *meta.Meta, *produce.Producer, *deliver.Deliverer, *metrics.Sampler) {
	return newTestServerWithConns(t, user, pass, nil)
}

// newTestServerWithConns 同 newTestServer，但额外注入 conns（总览连接数断言用）。
func newTestServerWithConns(t *testing.T, user, pass string, conns ConnCounter) (*Server, *store.Store, *meta.Meta, *produce.Producer, *deliver.Deliverer, *metrics.Sampler) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	mt, err := meta.New(rep, rt, st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	dl := deliver.New(rep, rt, st, mt, pr, slog.Default())
	sp := metrics.NewSampler(st, mt, time.Hour, slog.Default())
	// sys 与 /metrics 的系统 Collector、/admin/system 共用同一个
	// sysinfo.Reporter（数据目录用独立临时目录，不影响 store 所在目录）
	sys := sysinfo.New(t.TempDir(), 0, &atomic.Bool{}, slog.Default())
	s := New(rep, rt, nil, replication.StandaloneRouter{}, st, mt, pr, dl, user, pass, sys, sp, metrics.NewRegistry(st, mt, sys, nil, nil, nil, slog.Default()), conns, slog.Default())
	return s, st, mt, pr, dl, sp
}

// newTestServerNoSampler 同 newTestServer，但向 Server 传 sp=nil（模拟
// admin_listen 关闭时分支不装配采样器的生产形态）。查询安全、时序端点 503。
func newTestServerNoSampler(t *testing.T, user, pass string) (*Server, *store.Store, *meta.Meta, *produce.Producer, *deliver.Deliverer, *metrics.Sampler) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	mt, err := meta.New(rep, rt, st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	dl := deliver.New(rep, rt, st, mt, pr, slog.Default())
	sys := sysinfo.New(t.TempDir(), 0, &atomic.Bool{}, slog.Default())
	s := New(rep, rt, nil, replication.StandaloneRouter{}, st, mt, pr, dl, user, pass, sys, nil, metrics.NewRegistry(st, mt, sys, nil, nil, nil, slog.Default()), nil, slog.Default())
	return s, st, mt, pr, dl, nil
}

// doJSON 发 JSON 请求，返回响应记录器。token 非空时带 Bearer 头。
func doJSON(t *testing.T, h http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestLoginAndTokenGate(t *testing.T) {
	s, _, _, _, _, _ := newTestServer(t, "root", "pw123")
	h := s.Handler()
	// 未带 token 访问受保护路由 → 401
	if w := doJSON(t, h, "GET", "/admin/topics", "", nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("无 token 应 401，得到 %d", w.Code)
	}
	// 错密码 → 401
	if w := doJSON(t, h, "POST", "/admin/login", "", map[string]string{"username": "root", "password": "bad"}); w.Code != http.StatusUnauthorized {
		t.Fatalf("错密码应 401，得到 %d", w.Code)
	}
	// 正确登录 → token
	w := doJSON(t, h, "POST", "/admin/login", "", map[string]string{"username": "root", "password": "pw123"})
	if w.Code != http.StatusOK {
		t.Fatalf("登录应 200，得到 %d body=%s", w.Code, w.Body)
	}
	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Token == "" {
		t.Fatalf("应返回 token: %s %v", w.Body, err)
	}
	// 带 token → 放行
	if w := doJSON(t, h, "GET", "/admin/topics", resp.Token, nil); w.Code != http.StatusOK {
		t.Fatalf("带 token 应 200，得到 %d body=%s", w.Code, w.Body)
	}
	// 伪 token → 401
	if w := doJSON(t, h, "GET", "/admin/topics", "deadbeef", nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("伪 token 应 401，得到 %d", w.Code)
	}
}

func TestNoAuthConfiguredPassthrough(t *testing.T) {
	s, _, _, _, _, _ := newTestServer(t, "", "")
	h := s.Handler()
	if w := doJSON(t, h, "GET", "/admin/topics", "", nil); w.Code != http.StatusOK {
		t.Fatalf("免登录模式应直通，得到 %d", w.Code)
	}
	// 免登录时 login 端点明确告知无需登录
	if w := doJSON(t, h, "POST", "/admin/login", "", map[string]string{"username": "x", "password": "y"}); w.Code != http.StatusBadRequest {
		t.Fatalf("免登录模式 login 应 400，得到 %d", w.Code)
	}
}

func TestMetricsEndpointOpen(t *testing.T) {
	s, _, _, _, _, _ := newTestServer(t, "root", "pw123")
	// /metrics 不设防（Prometheus 抓取器无登录流程）
	if w := doJSON(t, s.Handler(), "GET", "/metrics", "", nil); w.Code != http.StatusOK {
		t.Fatalf("/metrics 应 200，得到 %d", w.Code)
	}
}
