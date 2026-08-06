// auth.go: Admin API 登录与 token 门禁（用户名密码来自配置文件，spec §6
// 「控制台独立简单密码登录」的服务端半边）。
//
// 职责：
//   - POST /admin/login 校验用户名密码、签发随机 token（内存表，TTL 24h）
//   - protected 中间件校验 Bearer token；未配置用户名密码时直通
//
// 边界：
//   - token 表在内存：进程重启全部失效，重新登录即可（单机管理面的刻意取舍）
//   - 不做多用户/权限分级；不做登录限速（部署侧用防火墙圈住 admin 端口）
package admin

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

// tokenTTL 登录 token 有效期。24h：覆盖一个工作日，过期重登成本可忽略。
const tokenTTL = 24 * time.Hour

// handleLogin POST /admin/login。两个比较都走常数时间且不短路，
// 不泄露"用户名对不对"这一位信息。
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.username == "" {
		httpError(w, http.StatusBadRequest, "服务端未配置登录，无需认证")
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	userOK := subtle.ConstantTimeCompare([]byte(req.Username), []byte(s.username)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(req.Password), []byte(s.password)) == 1
	if !userOK || !passOK {
		s.logger.Warn("admin 登录失败", "username", req.Username, "remote", r.RemoteAddr)
		httpError(w, http.StatusUnauthorized, "用户名或密码错误")
		return
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		s.logger.Error("生成登录 token 失败", "err", err)
		httpError(w, http.StatusInternalServerError, "生成 token 失败")
		return
	}
	token := hex.EncodeToString(buf)
	s.tokens.Store(token, time.Now().Add(tokenTTL))
	s.logger.Info("admin 登录成功", "username", req.Username, "remote", r.RemoteAddr)
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

// protected 包装需要登录的 handler。未配置用户名密码 = 免登录直通（默认关闭语义）。
func (s *Server) protected(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.username == "" {
			h(w, r)
			return
		}
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			httpError(w, http.StatusUnauthorized, "缺少 Bearer token，请先 POST /admin/login")
			return
		}
		exp, found := s.tokens.Load(token)
		if !found {
			httpError(w, http.StatusUnauthorized, "token 无效")
			return
		}
		if time.Now().After(exp.(time.Time)) {
			// 惰性清理：过期 token 在下次被使用时删除，无后台清扫协程——
			// token 量级 = 登录次数，单机管理面不会累积成内存问题
			s.tokens.Delete(token)
			httpError(w, http.StatusUnauthorized, "token 已过期，请重新登录")
			return
		}
		h(w, r)
	}
}
