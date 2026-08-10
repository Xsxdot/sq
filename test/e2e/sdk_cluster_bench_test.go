//go:build e2e

// sdk_cluster_bench_test.go 度量**端到端**写吞吐：官方 Go SDK → gRPC →
// broker → raft 复制 → quorum 确认，单位 msg/s。
//
// 职责：
//   - 三节点集群的并发写吞吐（quorum-mem / quorum-fsync 两档）
//   - 同机单机档基线（同一台机器、同一份 broker 二进制），给出复制税
//   - 内存峰值（复用 memPeakSampler，Linux VmHWM）
//   - 对**外部已部署**集群跑同一套负载（TestExternalClusterWriteThroughput），
//     用于每机一节点的跨机测量——与共置档同一个 runSendLoad，差值即共置税
//
// 边界：
//   - 不测消费侧吞吐（投递路径另有长轮询节拍变量，见 newClusterConsumer）
//   - 不做正确性断言（那是 TestThreeNodeClusterE2E 的职责）；本文件只在
//     发送失败率超阈值时失败——失败率不明的吞吐数字没有意义
//   - 默认不随套件运行：必须显式 SQ_BENCH=1 开启。它要跑满机器数十秒，
//     混进功能套件只会让 e2e 变慢且数字失真
//
// 跑法（远端零 Go 工具链，交叉编译产物直接跑）：
//
//	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/sq.linux ./cmd/sq
//	cd test/e2e && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -tags e2e -c -o /tmp/e2e.linux.test ./
//	# scp 两个产物后：
//	SQ_BENCH=1 SQ_E2E_BROKER=/tmp/sq.linux ./e2e.linux.test \
//	  -test.run TestClusterWriteThroughput -test.v -test.timeout=40m
//
// 可调环境变量：
//
//	SQ_BENCH_CONC    并发发送协程数列表，逗号分隔（默认 "16,64,256"）
//	SQ_BENCH_MSGS    每个档位发送的消息总数（默认 20000）
//	SQ_BENCH_BODY    消息体字节数（默认 128）
//	SQ_BENCH_QUEUES  topic 队列数（默认 24，需 ≥ 数据组数才能摊到各组）
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"

	"github.com/xushixin/sq/internal/config"
)

// benchMaxProducers 是 producer 池上限（见 runSendLoad 注释）。
const benchMaxProducers = 32

// newBenchProducer 构造一个带集群凭据的 Producer，启动失败时重试。
//
// 为什么要重试：SDK 的 startUp 会做一次 QueryRoute + telemetry 握手，
// 在负载机上偶发 protobuf size mismatch（golang/protobuf#1609——客户端
// 侧并发缺陷，与 broker 无关）。基准跑在压满的机器上，一次客户端抖动
// 不该让整轮测量作废；重试仍失败才判失败，那才是真的连不上。
func newBenchProducer(t *testing.T, endpoint, topic string) rmq.Producer {
	t.Helper()
	var last error
	for attempt := 1; attempt <= 3; attempt++ {
		p, err := rmq.NewProducer(&rmq.Config{
			Endpoint:    endpoint,
			Credentials: &credentials.SessionCredentials{AccessKey: clusterAK, AccessSecret: clusterSK},
		}, rmq.WithTopics(topic))
		if err == nil {
			if err = p.Start(); err == nil {
				return p
			}
			_ = p.GracefulStop()
		}
		last = err
		t.Logf("producer 启动第 %d 次失败（将重试）: %v", attempt, err)
		time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
	}
	t.Fatalf("producer 连续 3 次启动失败: %v", last)
	return nil
}

// benchResult 一个档位的测量结果。
type benchResult struct {
	label   string        // 档位名（如 "cluster/quorum-mem/conc=64"）
	sent    int           // 成功发送条数
	failed  int           // 失败条数
	elapsed time.Duration // 墙钟耗时
	p50     time.Duration // 单条 Send 延迟中位数
	p99     time.Duration
}

// msgPerSec 成功发送速率。
func (r benchResult) msgPerSec() float64 {
	if r.elapsed <= 0 {
		return 0
	}
	return float64(r.sent) / r.elapsed.Seconds()
}

// benchEnvInt 读整型正数环境变量，缺省/非法时回落 def。
func benchEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// benchConcurrencies 解析并发档位列表（SQ_BENCH_CONC，逗号分隔），
// 缺省 16/64/256——三档跨两个数量级，足以看出合并效应的斜率。
func benchConcurrencies() []int {
	raw := os.Getenv("SQ_BENCH_CONC")
	if raw == "" {
		return []int{16, 64, 256}
	}
	var out []int
	for _, f := range strings.Split(raw, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(f)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return []int{16, 64, 256}
	}
	return out
}

// runSendLoad 用 conc 个协程向 endpoint 并发同步发送约 total 条消息，
// 返回吞吐与延迟分位。
//
// 参数：
//   - endpoint: 接入点（集群档给任一节点，写会自动转发到组 leader）
//   - topic: 已建好的 topic（调用方负责 ensureTopic）
//   - conc: 并发发送协程数（共享 min(conc, benchMaxProducers) 个 Producer）
//   - total: 目标总条数，按协程均分（每协程 total/conc 条）
//   - bodyBytes: 消息体字节数
//
// 为什么是 producer 池而不是「每协程一个」：共用单实例测出来的是客户端
// 侧的序列化瓶颈，但一协程一实例同样不对——256 个 Producer 的 Start/Stop
// 各带一条 telemetry 长连接与后台协程，构造析构本身就压过了测量窗口
// （2 核机上实测：standalone 三档净发送 15s，子测总耗时 357s，差额全是
// 建/拆 producer），且 SDK 在这个量级会偶发 protobuf size mismatch
// （golang/protobuf#1609，客户端侧并发缺陷，与 broker 无关）。池上限
// benchMaxProducers 取 32：足够让服务端看到真并发，又不让客户端自己成为
// 被测对象。
//
// 为什么用同步 Send 而非 SendAsync：同步 Send 的每条耗时就是「客户端看到
// 的确认时延」，与 msg/s 一起报出来才解释得了吞吐从哪来（≈并发/延迟）；
// 异步发送会把客户端队列深度混进数字里。
//
// 注意：失败率 >1% 直接判失败——高失败率下的高 msg/s 是假象。
func runSendLoad(t *testing.T, endpoint, topic string, conc, total, bodyBytes int, label string) benchResult {
	t.Helper()
	body := make([]byte, bodyBytes)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	nProd := conc
	if nProd > benchMaxProducers {
		nProd = benchMaxProducers
	}
	producers := make([]rmq.Producer, nProd)
	for i := 0; i < nProd; i++ {
		producers[i] = newBenchProducer(t, endpoint, topic)
	}
	defer func() {
		for _, p := range producers {
			_ = p.GracefulStop()
		}
	}()

	perG := total / conc
	if perG == 0 {
		perG = 1
	}
	var sent, failed atomic.Int64
	var firstErr atomic.Value // 首个发送错误，失败时打进日志
	lat := make([][]time.Duration, conc)
	var wg sync.WaitGroup
	// 起跑线对齐：全部协程就位后同时开闸，避免先到的协程在别人还在建连接
	// 时独占服务端，把爬坡段算进稳态吞吐
	start := make(chan struct{})
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := producers[idx%nProd]
			mine := make([]time.Duration, 0, perG)
			<-start
			for j := 0; j < perG; j++ {
				msg := &rmq.Message{Topic: topic, Body: body}
				t0 := time.Now()
				_, err := p.Send(context.Background(), msg)
				d := time.Since(t0)
				if err != nil {
					failed.Add(1)
					firstErr.CompareAndSwap(nil, err)
					continue
				}
				sent.Add(1)
				mine = append(mine, d)
			}
			lat[idx] = mine
		}(i)
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t0)

	var all []time.Duration
	for _, s := range lat {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pick := func(q float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(float64(len(all)-1)*q)]
	}
	r := benchResult{
		label: label,
		sent:  int(sent.Load()), failed: int(failed.Load()),
		elapsed: elapsed, p50: pick(0.5), p99: pick(0.99),
	}
	if r.sent == 0 {
		t.Fatalf("%s: 一条都没发成功（失败 %d 条，首个错误: %v）", label, r.failed, firstErr.Load())
	}
	if rate := float64(r.failed) / float64(r.sent+r.failed); rate > 0.01 {
		t.Fatalf("%s: 失败率 %.2f%%（成功 %d / 失败 %d，首个错误: %v）超 1%%，吞吐数字不可信",
			label, 100*rate, r.sent, r.failed, firstErr.Load())
	}
	t.Logf("%s: %.0f msg/s（成功 %d 条 / %v，失败 %d，p50=%v p99=%v）",
		label, r.msgPerSec(), r.sent, elapsed.Round(time.Millisecond), r.failed,
		r.p50.Round(time.Microsecond), r.p99.Round(time.Microsecond))
	return r
}

// TestClusterWriteThroughput 端到端写吞吐：三节点两档确认 + 同机单机基线。
//
// 默认跳过（见文件头）：SQ_BENCH=1 才跑。单机基线与集群跑在同一台机器、
// 同一份二进制、同样的并发档上——只有这样，「复制税」才是可比的差值，
// 而不是跨机器数字相减得出的臆测。
func TestClusterWriteThroughput(t *testing.T) {
	if os.Getenv("SQ_BENCH") == "" {
		t.Skip("吞吐基准默认不随套件运行：设 SQ_BENCH=1 开启")
	}
	total := benchEnvInt("SQ_BENCH_MSGS", 20000)
	bodyBytes := benchEnvInt("SQ_BENCH_BODY", 128)
	queues := benchEnvInt("SQ_BENCH_QUEUES", 24)
	concs := benchConcurrencies()
	t.Logf("参数：总条数=%d 体积=%dB 队列数=%d 并发档=%v", total, bodyBytes, queues, concs)

	var results []benchResult
	for _, ack := range []string{"quorum-mem", "quorum-fsync"} {
		ack := ack
		t.Run("cluster-"+ack, func(t *testing.T) {
			endpoint, stop := startBenchCluster(t, ack, queues)
			defer stop()
			for _, c := range concs {
				topic := fmt.Sprintf("bench-%s-%d", strings.ReplaceAll(ack, "-", ""), c)
				ensureTopic(t, []string{endpoint}, topic)
				results = append(results, runSendLoad(t, endpoint, topic, c, total, bodyBytes,
					fmt.Sprintf("cluster/%s/conc=%d", ack, c)))
			}
		})
	}
	t.Run("standalone", func(t *testing.T) {
		// 单机档也带同一套凭据：集群配置带 credentials，而 runSendLoad 的
		// newBenchProducer 恒带凭据——两侧凭据一致才是同一条被测路径
		endpoint := startBroker(t, func(c *config.Config) {
			c.DefaultQueueNums = uint32(queues)
			c.DiskWatermarkPercent = 0
			c.LogLevel = "info"
			c.Credentials = []config.Credential{{Name: "bench", AccessKey: clusterAK, SecretKey: clusterSK}}
		})
		for _, c := range concs {
			topic := fmt.Sprintf("bench-solo-%d", c)
			ensureTopic(t, []string{endpoint}, topic)
			results = append(results, runSendLoad(t, endpoint, topic, c, total, bodyBytes,
				fmt.Sprintf("standalone/conc=%d", c)))
		}
	})

	t.Log("=== 汇总（msg/s，端到端 SDK→gRPC→raft→quorum 确认）===")
	for _, r := range results {
		t.Logf("%-32s %8.0f msg/s  p50=%-12v p99=%v",
			r.label, r.msgPerSec(), r.p50.Round(time.Microsecond), r.p99.Round(time.Microsecond))
	}
}

// benchEndpoints 解析外部集群接入点列表（SQ_BENCH_ENDPOINTS，逗号分隔的
// host:port）。未设或全为空返回 nil——调用方据此跳过跨机档。
func benchEndpoints() []string {
	var out []string
	for _, f := range strings.Split(os.Getenv("SQ_BENCH_ENDPOINTS"), ",") {
		if ep := strings.TrimSpace(f); ep != "" {
			out = append(out, ep)
		}
	}
	return out
}

// benchTopicTag 把标签压成可安全用作 topic 名的一段（只留字母数字，
// 其余一律丢弃）。topic 名进路由键与 Pebble 键，不接受任意字符。
func benchTopicTag(label string) string {
	var b strings.Builder
	for _, r := range label {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "ext"
	}
	return b.String()
}

// TestExternalClusterWriteThroughput 对**已在别处部署好**的集群跑同一套写
// 负载，用于跨机（每机一节点）测量。
//
// 与 TestClusterWriteThroughput 的分工：那个用例自己拉起三个共置进程，测出
// 的数字里含共置税——三份 CPU 抢同一台机器、三条 WAL 落同一块盘；本用例不
// 管拓扑，只做客户端，集群按真实部署摆在别处。两者用的是同一个 runSendLoad，
// 所以数字可以直接对比，差值就是共置税。
//
// 拓扑事实（几节点、什么确认档）本进程无从得知，由 SQ_BENCH_LABEL 显式带进
// 标签；不给就记 external——跑完一批分不清档位的数字比没有数字更糟。
//
// topic 名带标签指纹（benchTopicTag）：同一个集群上换档重跑时，若沿用同名
// topic，队列数与既有数据都会跟着上一档走，测的就不是本档了。
//
// 跑法（客户端所在机零 Go 工具链，交叉编译产物直接跑；SQ_E2E_BROKER 只为
// 满足 TestMain 的二进制自检，本用例不拉起任何进程）：
//
//	SQ_BENCH=1 SQ_E2E_BROKER=/root/sqbench/sq \
//	SQ_BENCH_ENDPOINTS=172.19.25.178:8081,172.19.25.179:8081,172.19.25.180:8081 \
//	SQ_BENCH_LABEL=3host-quorum-fsync ./e2e.linux.test \
//	  -test.run TestExternalClusterWriteThroughput -test.v -test.timeout=40m
func TestExternalClusterWriteThroughput(t *testing.T) {
	if os.Getenv("SQ_BENCH") == "" {
		t.Skip("吞吐基准默认不随套件运行：设 SQ_BENCH=1 开启")
	}
	endpoints := benchEndpoints()
	if len(endpoints) == 0 {
		t.Skip("未设 SQ_BENCH_ENDPOINTS：跨机档需要外部已部署好的集群接入点")
	}
	label := os.Getenv("SQ_BENCH_LABEL")
	if label == "" {
		label = "external"
	}
	total := benchEnvInt("SQ_BENCH_MSGS", 20000)
	bodyBytes := benchEnvInt("SQ_BENCH_BODY", 128)
	concs := benchConcurrencies()
	t.Logf("参数：接入点=%v 标签=%s 总条数=%d 体积=%dB 并发档=%v",
		endpoints, label, total, bodyBytes, concs)

	tag := benchTopicTag(label)
	var results []benchResult
	for _, c := range concs {
		topic := fmt.Sprintf("bench-%s-%d", tag, c)
		ensureTopic(t, endpoints, topic)
		results = append(results, runSendLoad(t, endpoints[0], topic, c, total, bodyBytes,
			fmt.Sprintf("%s/conc=%d", label, c)))
	}

	t.Log("=== 汇总（msg/s，端到端 SDK→gRPC→raft→quorum 确认）===")
	for _, r := range results {
		t.Logf("%-32s %8.0f msg/s  p50=%-12v p99=%v",
			r.label, r.msgPerSec(), r.p50.Round(time.Microsecond), r.p99.Round(time.Microsecond))
	}
}

// startBenchCluster 起一个三节点集群并等全部就绪，返回首节点 endpoint
// 与收尾函数。ack 为确认档（quorum-mem|quorum-fsync），queues 为队列数。
//
// 为什么不用 launchBroker：它内部就等就绪，而三节点必须**全部先起进程、
// 再逐个等就绪**——raft 选举要 quorum，等第一个节点就绪会永远卡在「等
// meta 组出 leader」（同 TestThreeNodeClusterE2E 的启动序注释）。
//
// 日志档压到 info：debug 下三个节点写日志本身就是可观开销，会污染吞吐。
func startBenchCluster(t *testing.T, ack string, queues int) (string, func()) {
	t.Helper()
	grpcPorts := pickPorts(t, 3)
	raftPorts := pickPorts(t, 3)
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cfgs := make([]*config.Config, 3)
	for i := 0; i < 3; i++ {
		cfgs[i] = clusterNodeConfig(t, dirs[i], uint64(i+1), grpcPorts[i], raftPorts[i], 3)
		cfgs[i].Cluster.Ack = ack
		cfgs[i].DefaultQueueNums = uint32(queues)
		cfgs[i].LogLevel = "info"
	}
	writeClusterPeers(cfgs, grpcPorts, raftPorts)

	handles := make([]*brokerHandle, 3)
	for i := 0; i < 3; i++ {
		cfgPath := writeNodeConfig(t, cfgs[i])
		endpoint := fmt.Sprintf("127.0.0.1:%d", grpcPorts[i])
		logPath := filepath.Join(dirs[i], "broker.log")
		logFile, err := os.Create(logPath)
		if err != nil {
			t.Fatalf("创建 broker 日志文件失败: %v", err)
		}
		cmd := exec.Command(brokerBinary, "-config", cfgPath)
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err := cmd.Start(); err != nil {
			logFile.Close()
			t.Fatalf("启动 broker 进程失败: %v", err)
		}
		waitDone := make(chan error, 1)
		go func() { waitDone <- cmd.Wait() }()
		handles[i] = &brokerHandle{
			endpoint: endpoint, cfgPath: cfgPath, logPath: logPath,
			logFile: logFile, cmd: cmd, waitDone: waitDone,
		}
	}
	for i := 0; i < 3; i++ {
		waitBrokerReady(t, handles[i].endpoint, handles[i].waitDone, handles[i].logPath)
		t.Logf("bench broker %d 就绪 endpoint=%s pid=%d", i+1, handles[i].endpoint, handles[i].cmd.Process.Pid)
	}
	peaks := newMemPeakSampler(handles)
	stop := func() {
		peaks.stopAndReport(t)
		for _, h := range handles {
			if h.cmd.Process != nil {
				h.stop(t)
			}
		}
	}
	return handles[0].endpoint, stop
}
