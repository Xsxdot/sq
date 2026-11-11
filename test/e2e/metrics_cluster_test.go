//go:build e2e

// metrics_cluster_test.go：三节点集群下 delay/half 深度指标在每个节点上都
// 正确（SQ4.9 真机首跑项）。
//
// 职责：
//   - delay/half 深度改由 store 内存计数后，leader 与 follower（apply 路径）
//     的 /metrics 都等于期望值：延时暂存的写入与到期搬运（删除）、经 follower
//     转发的事务半消息写入
//   - kill -9 meta 组 leader 再重启：重启节点（Open 重建基数 / Rejoin 重开
//     store / 快照安装重数）与切主后的其余节点读数仍正确
//
// 边界：只断言 sq_delay_depth、sq_half_messages 两个值；不测其余指标（包级
// S3 覆盖）。
package e2e

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"

	"github.com/Xsxdot/sq/internal/config"
)

// scrapeGauge 抓 base/metrics 中无标签 gauge name 的值；抓取失败返回 ok=false。
func scrapeGauge(base, name string) (float64, bool) {
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(base + "/metrics")
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if rest, ok := strings.CutPrefix(line, name+" "); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			return v, err == nil
		}
	}
	return 0, false
}

// metaLeaderIndex 经 /admin/cluster 查 meta 组（组 0）leader 的节点下标（0-based）。
func metaLeaderIndex(t *testing.T, pc *procCluster) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for i, c := range pc.cfgs {
			if pc.handles[i].cmd.Process == nil {
				continue
			}
			var v struct {
				Groups []struct {
					ID     uint32 `json:"id"`
					Leader uint64 `json:"leader"`
				} `json:"groups"`
			}
			adminJSON(t, "GET", "http://"+c.AdminListen+"/admin/cluster", "", nil, http.StatusOK, &v)
			for _, g := range v.Groups {
				if g.ID == 0 && g.Leader > 0 {
					return int(g.Leader) - 1
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("30s 内查不到 meta 组 leader")
	return -1
}

// waitDepthsOnAll 轮询全部节点直到 delay/half 深度都等于期望值。
func waitDepthsOnAll(t *testing.T, pc *procCluster, wantDelay, wantHalf float64, within time.Duration, phase string) {
	t.Helper()
	deadline := time.Now().Add(within)
	start := time.Now()
	var last string
	for time.Now().Before(deadline) {
		ok := true
		var parts []string
		for i, c := range pc.cfgs {
			base := "http://" + c.AdminListen
			d, ok1 := scrapeGauge(base, "sq_delay_depth")
			h, ok2 := scrapeGauge(base, "sq_half_messages")
			parts = append(parts, fmt.Sprintf("节点%d delay=%v(%v) half=%v(%v)", i+1, d, ok1, h, ok2))
			if !ok1 || !ok2 || d != wantDelay || h != wantHalf {
				ok = false
			}
		}
		last = strings.Join(parts, "；")
		if ok {
			t.Logf("[%s] %v 后三节点读数达标：%s", phase, time.Since(start).Round(100*time.Millisecond), last)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("[%s] %v 内未达标（期望 delay=%v half=%v）：%s", phase, within, wantDelay, wantHalf, last)
}

// adminSendDelay 经 meta leader 的控制台发送端点写一条延时消息（延时暂存
// 只在 meta leader 受理）。
//
// 为什么不用 SDK：集群档下 SDK 的 Send/Recall 按 topic 路由挑端点，只有
// meta leader 恰好也领导该 topic 某个队列组、且 SDK 轮到它时才会成功
// （rpc.SendMessage 的 leadsAnyQueueGroup 探针 + delay 写入要求 meta
// leader），实测 5 次里 3 次发不出——那是既有的集群路由限制，与本用例要
// 验的计数无关，故走确定性的控制台入口。
func adminSendDelay(t *testing.T, adminBase, topic, body string, delay time.Duration) {
	t.Helper()
	adminJSON(t, "POST", adminBase+"/admin/messages/send", "",
		map[string]any{"topic": topic, "body": body, "delay_ms": delay.Milliseconds()}, http.StatusCreated, nil)
}

func TestClusterMetricsDepthOnAllNodes(t *testing.T) {
	// 回查间隔拉长：孤儿半消息在用例期间不被回查（不受 checker 影响）；
	// 关客户端鉴权，沿用单机用例的 producer 辅助（空凭据）
	pc := startProcCluster(t, 3, func(c *config.Config) {
		c.TxnCheckInterval = "10m"
		c.Credentials = nil
	})
	const (
		topicDelay = "e2e-m49-delay"
		topicTxn   = "e2e-m49-txn"
		K, R, T    = 7, 3, 4
	)
	ensureTopic(t, pc.endpoints(), topicDelay)
	ensureTopic(t, pc.endpoints(), topicTxn)

	ml := metaLeaderIndex(t, pc)
	t.Logf("meta 组 leader = 节点 %d", ml+1)
	// K 条延时：K−R 条 10 分钟（留在暂存区），R 条 2 秒（到期被调度器搬走，
	// 即暂存键的删除经复制到达各节点）
	mlAdmin := "http://" + pc.cfgs[ml].AdminListen
	for i := 0; i < K; i++ {
		d := 10 * time.Minute
		if i < R {
			d = 2 * time.Second
		}
		adminSendDelay(t, mlAdmin, topicDelay, fmt.Sprintf("d-%d", i), d)
	}
	// 事务半消息：经 follower 发（half 写入走 ApplyOrForward 转发到 leader）
	fol := (ml + 1) % 3
	tp := newTxnProducer(t, pc.endpointOf(fol), topicTxn, &rmq.TransactionChecker{
		Check: func(*rmq.MessageView) rmq.TransactionResolution { return rmq.UNKNOWN },
	})
	for i := 0; i < T; i++ {
		if _, err := tp.SendWithTransaction(context.Background(),
			&rmq.Message{Topic: topicTxn, Body: []byte(fmt.Sprintf("h-%d", i))}, tp.BeginTransaction()); err != nil {
			t.Fatalf("SendWithTransaction: %v", err)
		}
	}
	t.Logf("已发 K=%d 条延时（其中 R=%d 条 2s 后到期搬走）、T=%d 条孤儿事务", K, R, T)
	waitDepthsOnAll(t, pc, K-R, T, 15*time.Second, "写入后")

	pc.kill(t, ml)
	pc.restart(t, ml)
	waitDepthsOnAll(t, pc, K-R, T, 30*time.Second, "kill -9 meta leader 并重启后")
	// 重启节点的计数来自重开 store 时的基数重建（同一日志文件跨重启追加）
	raw, err := os.ReadFile(pc.handles[ml].logPath)
	if err != nil {
		t.Fatal(err)
	}
	var rebuilt []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, "前缀计数基数已建立") || strings.Contains(line, "区间删除触及计数前缀") ||
			strings.Contains(line, "重入编排: 第 5 步") {
			rebuilt = append(rebuilt, line)
		}
	}
	for _, l := range rebuilt {
		t.Logf("节点 %d 日志: %s", ml+1, l)
	}
	if n := strings.Count(string(raw), "前缀计数基数已建立"); n < 2 {
		t.Fatalf("重启节点应至少两次建立计数基数（首启 + 重启），得 %d", n)
	}
	// 切主后新 leader 继续受理延时写入：含重启节点在内三节点同步增长
	nl := metaLeaderIndex(t, pc)
	t.Logf("新 meta 组 leader = 节点 %d", nl+1)
	for i := 0; i < 2; i++ {
		adminSendDelay(t, "http://"+pc.cfgs[nl].AdminListen, topicDelay, fmt.Sprintf("d2-%d", i), 10*time.Minute)
	}
	waitDepthsOnAll(t, pc, K-R+2, T, 15*time.Second, "切主后新 leader 再写 2 条")
}
