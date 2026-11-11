//go:build e2e

// metrics_cluster_test.go：三节点集群下 delay/half 深度指标在每个节点上都
// 正确（SQ4.9 真机首跑项）。
//
// 职责：
//   - delay/half 深度改由 store 内存计数后，leader 与 follower（apply 路径）
//     的 /metrics 都等于期望值
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

func TestClusterMetricsDepthOnAllNodes(t *testing.T) {
	// 回查间隔拉长：孤儿半消息在用例期间不被回查（不受 checker 影响）
	pc := startProcCluster(t, 3, func(c *config.Config) { c.TxnCheckInterval = "10m" })
	const (
		topicDelay = "e2e-m49-delay"
		topicTxn   = "e2e-m49-txn"
		K, R, T    = 7, 3, 4
	)
	ensureTopic(t, pc.endpoints(), topicDelay)
	ensureTopic(t, pc.endpoints(), topicTxn)

	// 撤回必须落在 meta leader 上（delay.Scheduler.Recall 的判据 1）
	ml := metaLeaderIndex(t, pc)
	t.Logf("meta 组 leader = 节点 %d", ml+1)
	p := newRecallProducer(t, pc.endpointOf(ml), topicDelay)
	var handles []string
	for i := 0; i < K; i++ {
		h, _ := sendDelayed(t, p, topicDelay, fmt.Sprintf("d-%d", i), 10*time.Minute)
		if h == "" {
			t.Fatal("延时消息未带回 recall 句柄")
		}
		handles = append(handles, h)
	}
	for _, h := range handles[:R] {
		if _, err := p.Recall(context.Background(), topicDelay, h); err != nil {
			t.Fatalf("Recall: %v", err)
		}
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
	t.Logf("已发 K=%d 条延时（撤回 R=%d）、T=%d 条孤儿事务", K, R, T)
	waitDepthsOnAll(t, pc, K-R, T, 15*time.Second, "写入后")

	pc.kill(t, ml)
	pc.restart(t, ml)
	waitDepthsOnAll(t, pc, K-R, T, 30*time.Second, "kill -9 meta leader 并重启后")
	t.Logf("新 meta 组 leader = 节点 %d", metaLeaderIndex(t, pc)+1)
}
