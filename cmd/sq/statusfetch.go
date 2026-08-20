// statusfetch.go 提供 `sq status` 的 admin HTTP 取数层。
//
// 职责：
//   - 按需登录换 Bearer token（服务端配了用户名密码时）
//   - GET admin 端点并解码 JSON
//
// 边界：
//   - 不决定「取哪个节点」——那是 status.go 的编排职责
//   - 不渲染、不判级、不退出
//   - 不复用 broker 侧的任何 HTTP 客户端：本命令是独立进程，
//     只用标准库，不引入依赖
//
// 为什么必须走 HTTP 而不是直读数据目录：sq 进程持有 Pebble 独占锁，
// 服务运行时 store.Open 必然失败（cmd/sq/recover.go 正是靠这条互斥
// 防止运行中误签）。status 要在服务**运行时**回答问题，只能走管理面。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// adminClient 一个 admin HTTP 端点的最小客户端。
//
// 注意：非并发安全——token 字段在 login 后被写入。本命令是单线程顺序取数，
// 不需要加锁；若将来要并发 ping 多个节点，请每个节点各建一个实例。
type adminClient struct {
	baseURL string
	token   string
	hc      *http.Client
}

// newAdminClient 建一个指向 baseURL 的客户端。
//
// 参数：
//   - baseURL: 形如 "http://10.0.0.1:8082"，末尾不带斜杠
//   - timeout: 单次请求超时。取值应当明显小于人的耐心——够不着的节点
//     要快速失败并降级，而不是让命令挂在那里
func newAdminClient(baseURL string, timeout time.Duration) *adminClient {
	return &adminClient{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		hc:      &http.Client{Timeout: timeout},
	}
}

// login 用用户名密码换 Bearer token 并记在客户端上。
//
// 参数：user/pass 来自配置的 admin_username / admin_password
// 返回：错误。401 会被翻译成点明「凭据」的错误——运维最先怀疑的是网络，
// 而这里恰恰不是网络问题，必须说清楚。
//
// 注意：服务端未配置鉴权时不要调用本方法（/admin/login 会回 400
// 「服务端未配置登录，无需认证」）。由调用方按配置判断。
func (c *adminClient) login(user, pass string) error {
	body, err := json.Marshal(map[string]string{"username": user, "password": pass})
	if err != nil {
		return fmt.Errorf("构造登录请求失败: %w", err)
	}
	url := c.baseURL + "/admin/login"
	slog.Debug("向管理面登录", "url", url, "user", user)
	resp, err := c.hc.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("请求 %s 失败: %w", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		slog.Error("管理面登录被拒", "url", url, "user", user, "status", resp.StatusCode)
		return fmt.Errorf("登录 %s 失败（401，凭据不匹配）：请核对配置里的 admin_username / admin_password", url)
	}
	if resp.StatusCode != http.StatusOK {
		slog.Error("管理面登录返回异常状态", "url", url, "status", resp.StatusCode, "body", string(raw))
		return fmt.Errorf("登录 %s 失败（HTTP %d）: %s", url, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.Token == "" {
		return fmt.Errorf("登录 %s 的响应无法解析出 token: %s", url, strings.TrimSpace(string(raw)))
	}
	c.token = out.Token
	slog.Debug("管理面登录成功", "url", url)
	return nil
}

// getJSON GET 一个 admin 端点并把响应解码进 out。
//
// 参数：
//   - path: 以斜杠开头的路径，如 "/admin/cluster"
//   - out: 解码目标指针
//
// 返回：错误。非 2xx 一律带上状态码与响应体——admin 的错误形状是
// {"error": "..."}，原样带出比翻译更有用。
func (c *adminClient) getJSON(path string, out any) error {
	url := c.baseURL + path
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("构造请求 %s 失败: %w", url, err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	slog.Debug("请求管理面", "url", url)
	resp, err := c.hc.Do(req)
	if err != nil {
		slog.Error("请求管理面失败", "url", url, "err", err)
		return fmt.Errorf("请求 %s 失败: %w", url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取 %s 响应失败: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		slog.Error("管理面返回异常状态", "url", url, "status", resp.StatusCode, "body", string(raw))
		return fmt.Errorf("请求 %s 返回 HTTP %d: %s", url, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("解析 %s 响应失败: %w", url, err)
	}
	slog.Debug("管理面请求完成", "url", url, "bytes", len(raw))
	return nil
}
