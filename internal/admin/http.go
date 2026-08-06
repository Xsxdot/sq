// http.go: Admin API 的 JSON 读写工具与统一错误形状。
//
// 职责：
//   - writeJSON/httpError/decodeJSON 三个包内工具，全部 handler 共用
//
// 边界：
//   - 错误响应统一 {"error": "..."}；不做 i18n
package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// writeJSON 写 JSON 响应。编码失败已无法挽回响应（头已发），只记日志。
func (s *Server) writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Error("admin 响应编码失败", "err", err)
	}
}

// httpError 写统一错误形状 {"error": "..."}。
func (s *Server) httpError(w http.ResponseWriter, code int, format string, args ...any) {
	s.writeJSON(w, code, map[string]string{"error": fmt.Sprintf(format, args...)})
}

// decodeJSON 解析请求体到 v，失败时已写 400 响应并返回 false。
// 1MB 上限：管理面请求（登录、建 topic、测试消息）都远小于此，挡住误传大文件。
func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		s.httpError(w, http.StatusBadRequest, "请求体解析失败: %v", err)
		return false
	}
	return true
}
