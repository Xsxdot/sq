// 内嵌控制台静态站测试：SPA fallback 与 /admin/*、/metrics 不被 "/" 吞掉。
// 用真实 fixture（newTestServer），不 mock core。
package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConsoleSPAFallback(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t, "", "")

	// 未知的前端路由必须回 index.html（客户端路由接管），而不是 404
	req := httptest.NewRequest(http.MethodGet, "/groups/order-svc", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	// dist 里只有占位（未构建）时给 503 并说明怎么构建；构建过则 200
	if w.Code != http.StatusOK && w.Code != http.StatusServiceUnavailable {
		t.Fatalf("前端路由应回 index.html 或未构建提示，得到 %d", w.Code)
	}
	if w.Code == http.StatusServiceUnavailable && !strings.Contains(w.Body.String(), "make web") {
		t.Fatalf("未构建时应提示 make web，得到：%s", w.Body.String())
	}
}

func TestAdminRoutesNotShadowedByConsole(t *testing.T) {
	srv, _, _, _, _, _ := newTestServer(t, "", "")
	// "/" 是 catch-all，绝不能把 /admin/* 与 /metrics 吃掉
	for _, p := range []string{"/admin/topics", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		if w.Code == http.StatusServiceUnavailable || w.Code == http.StatusNotFound {
			t.Fatalf("%s 不应被静态站接管，得到 %d", p, w.Code)
		}
	}
}
