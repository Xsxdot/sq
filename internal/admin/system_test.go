// system_test.go: /admin/system 端点测试。
package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSystemEndpointReportsSnapshot(t *testing.T) {
	// newTestServer 返回 6 个值，仅取 Server（辅助函数内部装配非 nil 的
	// *sysinfo.Reporter，/admin/system 才能吐出真实快照）
	s, _, _, _, _, _ := newTestServer(t, "", "") // 免登录，与该文件既有辅助一致
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/system", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("应返回 200，得到 %d：%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Disk *struct {
			TotalBytes  uint64  `json:"total_bytes"`
			UsedPercent float64 `json:"used_percent"`
		} `json:"disk"`
		WatermarkPercent int    `json:"watermark_percent"`
		WriteBlocked     bool   `json:"write_blocked"`
		DataDirBytes     *int64 `json:"data_dir_bytes"`
		GoHeapInuseBytes uint64 `json:"go_heap_inuse_bytes"`
		Goroutines       int    `json:"goroutines"`
		UptimeSeconds    int64  `json:"uptime_seconds"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}
	if got.GoHeapInuseBytes == 0 {
		t.Fatal("堆内存不该为 0")
	}
	if got.Goroutines < 1 {
		t.Fatalf("协程数至少为 1，得到 %d", got.Goroutines)
	}
	if got.WriteBlocked {
		t.Fatal("测试环境不该处于拒写状态")
	}
	if got.Disk == nil {
		t.Fatal("磁盘读数不该为 nil")
	}
	if got.DataDirBytes == nil {
		t.Fatal("数据目录占用不该为 nil")
	}
}

func TestSystemEndpointRequiresLogin(t *testing.T) {
	s, _, _, _, _, _ := newTestServer(t, "root", "pw")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/system", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("未带 token 应返回 401，得到 %d", rec.Code)
	}
}
