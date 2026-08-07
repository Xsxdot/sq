// web.go: 内嵌控制台的静态资源服务。
//
// 职责：
//   - 在 "/" 上提供 web 包内嵌的 Vite 产物
//   - 未命中实体文件时回退到 index.html，交给前端路由（SPA fallback）
//
// 边界：
//   - "/" 是 ServeMux 的兜底模式，/admin/* 与 /metrics 有更具体的模式，
//     Go 1.22 起按最具体者优先匹配，不会被本 handler 吃掉
//   - 控制台未构建（dist 里只有占位文件）时返回 503 并给出构建命令，
//     不返回 404——404 会被误读成「路由写错了」
package admin

import (
	"errors"
	"io/fs"
	"net/http"
	"path"
	"strings"

	"github.com/xushixin/sq/web"
)

// consoleHandler 构造控制台静态站 handler。
func (s *Server) consoleHandler() http.HandlerFunc {
	sub, err := fs.Sub(web.Dist, "dist")
	if err != nil {
		// 目录名写死在 embed 指令里，取不到子树属于程序 bug
		s.logger.Error("控制台产物子树不可用", "err", err)
		return func(w http.ResponseWriter, r *http.Request) {
			s.httpError(w, http.StatusInternalServerError, "控制台产物不可用: %v", err)
		}
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		s.logger.Warn("控制台未构建，/ 将返回构建提示（执行 make web 后重新编译）")
	}
	return func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if p == "" || p == "." {
			p = "index.html"
		}
		data, err := fs.ReadFile(sub, p)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				s.logger.Error("读控制台资源失败", "path", p, "err", err)
				s.httpError(w, http.StatusInternalServerError, "%v", err)
				return
			}
			// 不存在的路径一律回 index.html：前端是 BrowserRouter，
			// /groups/order-svc 这类地址在磁盘上没有对应文件
			p = "index.html"
			if data, err = fs.ReadFile(sub, p); err != nil {
				s.httpError(w, http.StatusServiceUnavailable,
					"控制台尚未构建，请先执行 make web 后重新编译二进制")
				return
			}
		}
		w.Header().Set("Content-Type", contentType(p))
		if p == "index.html" {
			// index.html 里写着当次构建的哈希资源名，缓存住它等于把用户
			// 钉在旧版本上；资源文件名自带哈希，可以长缓存
			w.Header().Set("Cache-Control", "no-cache")
		} else {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		if _, err := w.Write(data); err != nil {
			s.logger.Debug("控制台资源写出中断", "path", p, "err", err)
		}
	}
}

// contentType 按扩展名给 MIME。只覆盖 Vite 产物会出现的类型；
// 未知扩展名交给 http.DetectContentType 的默认行为（八位字节流）。
func contentType(p string) string {
	switch path.Ext(p) {
	case ".html":
		return "text/html; charset=utf-8"
	case ".js":
		return "text/javascript; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".json":
		return "application/json; charset=utf-8"
	case ".svg":
		return "image/svg+xml"
	case ".woff2":
		return "font/woff2"
	default:
		return "application/octet-stream"
	}
}
