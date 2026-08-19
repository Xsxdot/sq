//go:build e2e

// Admin API e2e 冒烟：真实进程上 登录 → 建 topic → 发测试消息 → 浏览 → 总览。
//
// 职责：验证 admin HTTP 在完整进程装配下真实可用（端口、登录、与 store 的接线）
// 边界：端点行为细节由 internal/admin 单测覆盖，这里只走一条主链路
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/Xsxdot/sq/internal/config"
)

// adminJSON 带 token 调 admin API 并解析 JSON 响应；expectCode 不符即 Fatal。
func adminJSON(t *testing.T, method, url, token string, body any, expectCode int, out any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != expectCode {
		t.Fatalf("%s %s 应 %d，得到 %d body=%s", method, url, expectCode, resp.StatusCode, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("解析响应失败: %v body=%s", err, raw)
		}
	}
}

func TestAdminAPISmoke(t *testing.T) {
	adminPort := pickPort(t)
	// pickPort 首选固定端口 preferredPort：探测完即释放，紧接着 startBroker
	// 内部为 gRPC 再选一次还会拿到同一个端口——实测 admin HTTP 先绑定成功、
	// gRPC 随后 bind 失败导致 broker 退出。先占住 adminPort 再选出 gRPC 端口，
	// 迫使 pickPort 走内核分配分支，保证两个端口不同。
	hold, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", adminPort))
	if err != nil {
		t.Fatalf("占住 admin 端口失败: %v", err)
	}
	grpcPort := pickPort(t)
	hold.Close()
	startBroker(t, func(c *config.Config) {
		c.GRPCListen = fmt.Sprintf("127.0.0.1:%d", grpcPort)
		c.AdvertisePort = grpcPort
		c.AdminListen = fmt.Sprintf("127.0.0.1:%d", adminPort)
		c.AdminUsername = "root"
		c.AdminPassword = "e2e-pw"
	})
	base := fmt.Sprintf("http://127.0.0.1:%d", adminPort)

	// 未登录 → 401
	resp, err := http.Get(base + "/admin/topics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("未登录应 401，得到 %d", resp.StatusCode)
	}
	// 登录
	var login struct {
		Token string `json:"token"`
	}
	adminJSON(t, "POST", base+"/admin/login", "",
		map[string]string{"username": "root", "password": "e2e-pw"}, http.StatusOK, &login)
	if login.Token == "" {
		t.Fatal("登录应返回 token")
	}
	// 建 topic → 发测试消息 → 浏览
	adminJSON(t, "POST", base+"/admin/topics", login.Token,
		map[string]any{"name": "e2e-admin", "queues": 1}, http.StatusCreated, nil)
	var sent struct {
		MsgID string `json:"msg_id"`
	}
	adminJSON(t, "POST", base+"/admin/messages/send", login.Token,
		map[string]any{"topic": "e2e-admin", "body": "from-admin"}, http.StatusCreated, &sent)
	var msgs []struct {
		ID string `json:"id"`
	}
	adminJSON(t, "GET", base+"/admin/messages?topic=e2e-admin&queue_id=0", login.Token,
		nil, http.StatusOK, &msgs)
	if len(msgs) != 1 || msgs[0].ID != sent.MsgID {
		t.Fatalf("浏览应见刚发的消息: %+v (want %s)", msgs, sent.MsgID)
	}
	// 总览与 /metrics
	var ov struct {
		Topics       int    `json:"topics"`
		TotalWritten uint64 `json:"total_written"`
	}
	adminJSON(t, "GET", base+"/admin/overview", login.Token, nil, http.StatusOK, &ov)
	if ov.Topics < 1 || ov.TotalWritten < 1 {
		t.Fatalf("总览计数不符: %+v", ov)
	}
	mresp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	mraw, _ := io.ReadAll(mresp.Body)
	mresp.Body.Close()
	if mresp.StatusCode != http.StatusOK || !bytes.Contains(mraw, []byte("sq_topic_messages_written_total")) {
		t.Fatalf("/metrics 应含 sq_topic_messages_written_total: code=%d", mresp.StatusCode)
	}
}
