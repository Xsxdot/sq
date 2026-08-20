// statusfetch_test.go: admin HTTP 取数层单测。用 httptest 打桩，
// 覆盖登录换 token、无鉴权直连、401、以及 JSON 解码。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// adminStub 起一个最小的 admin 服务桩。token 非空时要求 Bearer 匹配。
func adminStub(t *testing.T, user, pass, token string, cluster any) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/login", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Username != user || req.Password != pass {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "用户名或密码错误"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"token": token})
	})
	mux.HandleFunc("GET /admin/cluster", func(w http.ResponseWriter, r *http.Request) {
		if token != "" && r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(cluster)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestAdminClientLoginAndGet(t *testing.T) {
	srv := adminStub(t, "admin", "pw", "tok123", map[string]any{"enabled": true, "self_id": 2})
	c := newAdminClient(srv.URL, 3*time.Second)
	if err := c.login("admin", "pw"); err != nil {
		t.Fatalf("登录应成功: %v", err)
	}
	var out struct {
		Enabled bool   `json:"enabled"`
		SelfID  uint64 `json:"self_id"`
	}
	if err := c.getJSON("/admin/cluster", &out); err != nil {
		t.Fatalf("取集群视图应成功: %v", err)
	}
	if !out.Enabled || out.SelfID != 2 {
		t.Fatalf("解码结果不对: %+v", out)
	}
}

func TestAdminClientLoginRejectsBadCredentials(t *testing.T) {
	srv := adminStub(t, "admin", "pw", "tok123", nil)
	c := newAdminClient(srv.URL, 3*time.Second)
	err := c.login("admin", "wrong")
	if err == nil {
		t.Fatal("错误凭据必须报错")
	}
	// 报错要能让运维一眼看出是凭据问题，而不是网络问题
	if !strings.Contains(err.Error(), "401") && !strings.Contains(err.Error(), "凭据") {
		t.Fatalf("报错必须点明凭据/401，得到: %v", err)
	}
}

func TestAdminClientWorksWithoutAuth(t *testing.T) {
	// token 为空 = 服务端未开鉴权，不调 login 直接取
	srv := adminStub(t, "", "", "", map[string]any{"enabled": false})
	c := newAdminClient(srv.URL, 3*time.Second)
	var out struct {
		Enabled bool `json:"enabled"`
	}
	if err := c.getJSON("/admin/cluster", &out); err != nil {
		t.Fatalf("免鉴权直连应成功: %v", err)
	}
	if out.Enabled {
		t.Fatal("期望 enabled=false")
	}
}

func TestAdminClientSurfacesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"内部错误"}`))
	}))
	t.Cleanup(srv.Close)
	c := newAdminClient(srv.URL, 3*time.Second)
	var out map[string]any
	err := c.getJSON("/admin/cluster", &out)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("5xx 必须带状态码报错，得到: %v", err)
	}
}
