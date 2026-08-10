//go:build e2e

// 集群模式端到端：单机→单节点集群升级路径与三节点全流程（B8.2 batch③
// 收口用例，plan Task 11 Step 3）。
//
// 职责：
//   - TestStandaloneToSingleNodeClusterUpgrade：单机跑一段（写 topic +
//     消息）→ 停 → 同一数据目录加 cluster 段（peers 只有自己）重启 →
//     SDK 能查路由、收到升级前的消息、继续收发。断言 raft/ 前缀已出现
//     （升级确实入了 raft，而不是退回单机路径）。
//   - TestThreeNodeClusterE2E：三节点全新起（三个 data dir、三份配置、
//     同一 credentials）→ SDK endpoints 填三地址 → 建 topic
//     （default_queue_nums=6，保证三组都有队列）→ 发 200 条 →
//     SimpleConsumer 全收全 ack → kill 当前某数据组 leader 进程 →
//     继续发收（SDK 重试 + 路由刷新自愈，允许重复不允许丢）→ 重入：
//     以原配置重启被 kill 节点（main 的 ErrUncleanShutdown 自愈入口
//     自动走 cluster.Rejoin）→ 三节点对账（路由恢复 + 无丢失）。
//
// 边界：
//   - 事务用例放在三节点健康期，以 broker 日志证据断言两跳转发链真实可达
//     （plan 风险自记 ① 的 keep-vs-delete 判定）：SDK 的发送/提交落在
//     非 meta leader 节点时，Stage 与 End 第二段经 ApplyOrForward 触发
//     ForwardApply（转发节点日志「跨节点转发批次」）；End 第一段落在非
//     队列 0 所属组 leader 节点时经 ForwardAppend 转发（日志「事务提交
//     消息跨节点转发」）。用例连发 6 条事务（SDK 发布负载均衡按队列轮询，
//     6 队列覆盖 3 组 3 节点），逐节点日志 grep 后断言两类转发各 ≥1 次
//     ——两跳均有实测证据，转发代码是承重路径而非防御死代码
//   - 消费吞吐只记粗略数字（plan 风险自记 ②：Receive 每次都过 raft
//     提案，防止量级劣化无感），非 benchmark；健康期消费者用 100ms 短
//     轮询档测量（评审 Important 3：3s 长档下数字被 SDK 轮询节拍主导，
//     不能代表 broker 吞吐）
//   - 内存峰值：轮询 /proc/<pid>/status VmHWM（Linux；非 Linux 跳过）
package e2e

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	sdkpb "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v3"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/store"
)

// clusterAK/clusterSK 三节点共用的静态凭据：客户端在节点间轮询时任一
// 节点都要能验签（AK/SK 复制语义见 rpc/handle_secret.go 的
// LoadOrCreateHandleSecretReplicated）。
const (
	clusterAK = "cluster-ak"
	clusterSK = "cluster-sk"

	// clusterProduceCount 三节点健康期发送的消息条数（非 200 的整数倍
	// 会破坏「全收全 ack」断言的可读性，固定 200 与 plan 一致）。
	clusterProduceCount = 200

	// txnForwardProbeCount 事务转发链探针的事务条数：SDK 发布负载均衡
	// 按队列轮询（topic 6 队列 = 3 组 × 2 队列，覆盖 3 个节点），连发
	// 6 条保证两类转发（End 第一段 ForwardAppend / Stage·End 第二段
	// ForwardApply）都至少命中一次——测试据此对 broker 日志做转发
	// 证据断言（plan 风险自记① 的 keep-vs-delete 裁决）。
	txnForwardProbeCount = 6

	// clusterConsumerAwaitShort / clusterConsumerAwaitDefault 是集群用例
	// 消费者的 SDK 轮询档位（见 newClusterConsumer 注释）。短档用于
	// 吞吐测量路径（健康期消费）：100ms 让空轮询快速返回，测量值反映
	// broker 投递能力而非轮询节拍。默认档（3s）用于其余消费者——长轮询
	// 减少空轮询请求数，语义不变。
	//
	// clusterConsumerReqTimeout 是短档消费者配套的请求超时：SDK 的
	// Receive 超时 = await + 请求超时（默认 3s），服务端长轮询按请求
	// deadline 推导——只调 await 时空轮询仍等 ≈2.1s，测量仍被轮询节拍
	// 主导（实测 13 msg/s）。压到 200ms 后空轮询 ≈ await 即返。
	clusterConsumerAwaitShort   = 100 * time.Millisecond
	clusterConsumerAwaitDefault = 3 * time.Second
	clusterConsumerReqTimeout   = 200 * time.Millisecond
)

// pickPorts 选 n 个互不相同的空闲端口（与 pickPort 同款 bind-probe 语义，
// 集群用例需要一组错开的端口）。
func pickPorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, 0, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("探测空闲端口失败: %v", err)
		}
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
		l.Close()
	}
	return ports
}

// clusterNodeConfig 构造三节点之一的基础配置（grpc/raft 端口错开、
// 同一 credentials）。dataDir 由调用方指定（升级用例要复用目录）。
func clusterNodeConfig(t *testing.T, dir string, nodeID uint64, grpcPort, raftPort int, dataGroups uint32) *config.Config {
	t.Helper()
	cfg := &config.Config{
		GRPCListen:             fmt.Sprintf("127.0.0.1:%d", grpcPort),
		AdvertiseHost:          "127.0.0.1",
		AdvertisePort:          grpcPort,
		DataDir:                filepath.Join(dir, "data"),
		Fsync:                  "sync",
		AutoCreateTopic:        true,
		DefaultQueueNums:       6, // plan：保证三组都有队列
		DefaultMaxAttempts:     16,
		RetentionCheckInterval: "5m",
		TxnCheckInterval:       "30s",
		TxnMaxChecks:           15,
		DiskWatermarkPercent:   0, // 同 writeBrokerConfig：e2e 机器磁盘不可控，关闭水位
		LogLevel:               "debug",
		Credentials: []config.Credential{
			{Name: "e2e-cluster", AccessKey: clusterAK, SecretKey: clusterSK},
		},
	}
	// 集群段由装配层显式给出（升级用例需要两代配置不同 peers）。
	cfg.Cluster = &config.ClusterConfig{
		NodeID:     nodeID,
		RaftListen: fmt.Sprintf("127.0.0.1:%d", raftPort),
		DataGroups: dataGroups,
		Ack:        "quorum-mem",
	}
	return cfg
}

// writeClusterPeers 把三节点的成员表写进 cfg.Cluster.Peers（每份配置
// 同一张表——raft 成员表必须全节点一致）。
func writeClusterPeers(cfgs []*config.Config, grpcPorts, raftPorts []int) {
	peers := make([]config.ClusterPeer, 0, len(cfgs))
	for i, c := range cfgs {
		peers = append(peers, config.ClusterPeer{
			ID:            c.Cluster.NodeID,
			RaftAddr:      fmt.Sprintf("127.0.0.1:%d", raftPorts[i]),
			AdvertiseHost: "127.0.0.1",
			AdvertisePort: grpcPorts[i],
		})
	}
	for _, c := range cfgs {
		c.Cluster.Peers = peers
	}
}

// writeNodeConfig 把节点配置序列化到 dir/sq.yaml。
func writeNodeConfig(t *testing.T, cfg *config.Config) string {
	t.Helper()
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("序列化节点配置失败: %v", err)
	}
	cfgPath := filepath.Join(filepath.Dir(cfg.DataDir), "sq.yaml")
	if err := os.WriteFile(cfgPath, raw, 0o600); err != nil {
		t.Fatalf("写节点配置失败: %v", err)
	}
	return cfgPath
}

// queryRoute 直接以 sdkpb 客户端向任意节点发 QueryRoute，返回 topic 的
// 队列路由。为什么要绕开 SDK 高层 API：测试需要「当前 leader 是谁」的
// 事实（kill 目标选择、重入后路由恢复断言），SDK 的 producer/consumer
// 不暴露路由细节；路由指向组 leader 正是被测契约本身（Task 9）。
//
// 节点配置了 AK/SK（三份配置同一 credentials），原始 gRPC 调用必须像
// SDK 那样带 MQv2-HMAC-SHA1 签名头（格式见 internal/rpc/auth.go），
// 否则会被认证拦截器拒掉。
func queryRoute(t *testing.T, endpoint, topic string) []*sdkpb.MessageQueue {
	t.Helper()
	qs, err := tryQueryRoute(endpoint, topic)
	if err != nil {
		t.Fatalf("QueryRoute(%s) 失败: %v", endpoint, err)
	}
	return qs
}

// tryQueryRoute 是 queryRoute 的非致命版本：返回错误而不是 Fatal——
// 供「轮询直到某节点可答」的创建路径使用（见 ensureTopic）。
func tryQueryRoute(endpoint, topic string) ([]*sdkpb.MessageQueue, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc 连接 %s: %w", endpoint, err)
	}
	defer conn.Close()
	cli := sdkpb.NewMessagingServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().Format("20060102T150405Z")
	h := hmac.New(sha1.New, []byte(clusterSK))
	h.Write([]byte(now))
	auth := fmt.Sprintf("MQv2-HMAC-SHA1 Credential=%s//Rocketmq, SignedHeaders=x-mq-date-time, Signature=%s",
		clusterAK, hex.EncodeToString(h.Sum(nil)))
	ctx = metadata.AppendToOutgoingContext(ctx, "x-mq-date-time", now, "authorization", auth)
	resp, err := cli.QueryRoute(ctx, &sdkpb.QueryRouteRequest{Topic: &sdkpb.Resource{Name: topic}})
	if err != nil {
		return nil, err
	}
	if st := resp.GetStatus(); st.GetCode() != sdkpb.Code_OK {
		return nil, fmt.Errorf("code=%d msg=%s", st.GetCode(), st.GetMessage())
	}
	return resp.GetMessageQueues(), nil
}

// ensureTopic 在集群上创建 topic 并等全部节点可答路由。
//
// 为什么必须显式建 topic 而不是依赖 SDK 的 autoCreate：topic 注册是
// meta 组写，只有 meta leader 能创建（follower 上 EnsureTopic →
// rep.Apply(MetaGroup) 报 ErrNotLeader → HA_NOT_AVAILABLE）。官方 SDK
// 的 producer.Start 对初始 topic 只做**一次** QueryRoute（client.go
// startUp → getMessageQueues），不跨节点重试——若首跳落在 follower，
// 启动直接失败。集群语义下「建 topic」必须是可重试操作：轮询全部节点
// 直到某节点（meta leader）创建成功；随后等**全部**节点路由可答——
// follower 的 meta 缓存经 OnApplied→Reload 追平（Task 6 装配的钩子），
// 追平前它的 QueryRoute 仍会尝试重建 topic 而报错。
func ensureTopic(t *testing.T, endpoints []string, topic string) {
	t.Helper()
	// 阶段 1：轮询各节点直到创建成功（只有 meta leader 能写）
	created := false
	deadline := time.Now().Add(60 * time.Second)
	for !created && time.Now().Before(deadline) {
		for _, ep := range endpoints {
			if _, err := tryQueryRoute(ep, topic); err == nil {
				created = true
				break
			}
		}
		if !created {
			time.Sleep(2 * time.Second)
		}
	}
	if !created {
		t.Fatal("60s 内未能创建 topic（meta 组 leader 一直不可写）")
	}
	t.Logf("topic %s 已创建（meta leader 写入）", topic)
	// 阶段 2：等全部节点路由可答（follower 缓存追平），否则 producer.Start
	// 可能撞上缓存未加载的 follower 被 HA_NOT_AVAILABLE 拒死
	deadline = time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		allOK := true
		for _, ep := range endpoints {
			if _, err := tryQueryRoute(ep, topic); err != nil {
				allOK = false
				break
			}
		}
		if allOK {
			t.Logf("全部节点已可答 topic %s 路由", topic)
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("60s 内未等齐全部节点路由可答（OnApplied→Reload 追平失败）")
}

// routeEndpointCounts 统计每条队列的 leader 端点分布（host:port → 队列数）。
func routeEndpointCounts(queues []*sdkpb.MessageQueue) map[string]int {
	counts := map[string]int{}
	for _, q := range queues {
		for _, a := range q.GetBroker().GetEndpoints().GetAddresses() {
			counts[fmt.Sprintf("%s:%d", a.GetHost(), a.GetPort())]++
		}
	}
	return counts
}

// leaderOfMostQueues 返回路由中持有最多队列的端点——该节点是某数据组
// 的 leader（本用例的 kill 目标）。
func leaderOfMostQueues(counts map[string]int) string {
	best, bestN := "", -1
	for ep, n := range counts {
		if n > bestN {
			best, bestN = ep, n
		}
	}
	return best
}

// kill 直接 SIGKILL 进程（断电语义：不写干净关机标记，重启必须走
// ErrUncleanShutdown → Rejoin 自愈路径）。
func (h *brokerHandle) kill(t *testing.T) {
	t.Helper()
	if err := h.cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL broker: %v", err)
	}
	select {
	case err := <-h.waitDone:
		t.Logf("broker 已 SIGKILL（pid=%d 退出 %v）", h.cmd.Process.Pid, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("SIGKILL 后 10s 内进程未退出")
	}
	h.logFile.Close()
}

// memPeakSampler 轮询各 broker 进程的 VmHWM（RSS 峰值）并记录每进程
// 峰值；Linux 才有 /proc/<pid>/status，非 Linux 跳过（记一次日志）。
// 全局约束：性能类验证必记内存峰值。
type memPeakSampler struct {
	mu    sync.Mutex
	peaks map[int]uint64 // pid → VmHWM kB
	stop  chan struct{}
	done  chan struct{}
}

func newMemPeakSampler(handles []*brokerHandle) *memPeakSampler {
	s := &memPeakSampler{peaks: map[int]uint64{}, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-tk.C:
				for _, h := range handles {
					if h.cmd.Process == nil {
						continue
					}
					if v := readVmHWM(h.cmd.Process.Pid); v > 0 {
						s.mu.Lock()
						if v > s.peaks[h.cmd.Process.Pid] {
							s.peaks[h.cmd.Process.Pid] = v
						}
						s.mu.Unlock()
					}
				}
			}
		}
	}()
	return s
}

func (s *memPeakSampler) stopAndReport(t *testing.T) {
	close(s.stop)
	<-s.done
	if len(s.peaks) == 0 {
		t.Logf("内存峰值采样：无数据（非 Linux 环境，无 /proc/<pid>/status，跳过）")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for pid, kb := range s.peaks {
		t.Logf("内存峰值：broker pid=%d VmHWM=%.1f MiB", pid, float64(kb)/1024)
	}
}

// readVmHWM 读 /proc/<pid>/status 的 VmHWM 字段（kB）；非 Linux 或无
// 权限返回 0。
func readVmHWM(pid int) uint64 {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if rest, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			var kb uint64
			if _, err := fmt.Sscanf(strings.TrimSpace(rest), "%d", &kb); err == nil {
				return kb
			}
			return 0
		}
	}
	return 0
}

// TestStandaloneToSingleNodeClusterUpgrade 单机→单节点集群升级路径：
// 单机写 topic + 消息 → 干净停机 → 同一数据目录加 cluster 段（peers
// 只有自己）→ 重启 → SDK 路由可见、收到升级前消息、继续收发 → 断言
// raft/ 前缀已出现在数据目录（升级确实入了 raft）。
//
// 为什么升级必须能「看到旧数据」：FSM 与 raft 日志共库（spec §5），
// 单机写入的 msg//meta/ 键直接就是集群模式的 FSM 数据；首启（fresh
// 路径）只引导 raft 组、不清库，meta.New 从 store 读到既有 topic，
// deliver 从 msg/ 读到既有消息——升级对数据是「原样接管」。
func TestStandaloneToSingleNodeClusterUpgrade(t *testing.T) {
	grpcPorts := pickPorts(t, 1)
	raftPorts := pickPorts(t, 1)
	dir := t.TempDir()

	// ---- 第一代：纯单机（无 cluster 段）----
	standalone := clusterNodeConfig(t, dir, 1, grpcPorts[0], raftPorts[0], 3)
	standalone.Cluster = nil // 去掉 cluster 段 = 单机模式
	cfgPath := writeNodeConfig(t, standalone)
	ep := fmt.Sprintf("127.0.0.1:%d", grpcPorts[0])
	h1 := launchBroker(t, cfgPath, ep, filepath.Join(dir, "broker-gen1.log"))
	producer := newClusterProducer(t, ep, "e2e-upgrade")
	// 升级前写一批消息（topic 自动建，default_queue_nums=6）
	sent := map[string]bool{}
	for i := 0; i < 5; i++ {
		recs, err := producer.Send(context.Background(), &rmq.Message{Topic: "e2e-upgrade", Body: []byte(fmt.Sprintf("pre-upgrade #%d", i))})
		if err != nil {
			t.Fatalf("升级前 Send #%d: %v", i, err)
		}
		sent[recs[0].MessageID] = true
	}
	producer.GracefulStop()
	h1.stop(t) // 干净停机（SIGTERM）

	// ---- 第二代：同一数据目录 + cluster 段（peers 只有自己）----
	// 独立构造一份集群配置（第一代是 Cluster=nil 的单机档，cluster 段
	// 字段不可用）；grpc/raft 端口与数据目录沿用第一代。
	gen2 := clusterNodeConfig(t, dir, 1, grpcPorts[0], raftPorts[0], 3)
	writeClusterPeers([]*config.Config{gen2}, grpcPorts, raftPorts)
	cfgPath = writeNodeConfig(t, gen2)
	h2 := launchBroker(t, cfgPath, ep, filepath.Join(dir, "broker-gen2.log"))
	h2Stopped := false
	t.Cleanup(func() {
		if !h2Stopped {
			h2.stop(t)
		}
		if t.Failed() {
			dumpBrokerLog(t, filepath.Join(dir, "broker-gen2.log"))
		}
	})

	// SDK 能查路由（topic 还在）。注意：QueryRoute 需要全部数据组都有
	// leader 才可答，而单节点集群各组选举完成时间在 1-2s 随机窗口内
	// （raft 选举超时抖动）——首跳固定落在 meta leader 就绪的瞬间
	// 可能撞上数据组选举未完成，这里轮询等路由可答（与 ensureTopic 的
	// 等待口径一致）。
	deadline := time.Now().Add(30 * time.Second)
	var queues []*sdkpb.MessageQueue
	for time.Now().Before(deadline) {
		if qs, err := tryQueryRoute(ep, "e2e-upgrade"); err == nil {
			queues = qs
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if len(queues) == 0 {
		t.Fatal("升级后 QueryRoute 30s 内未返回路由（数据组选举未完成）")
	}

	// 收到升级前的全部消息
	consumer := newClusterConsumer(t, ep, "e2e-upgrade-g", "e2e-upgrade", clusterConsumerAwaitDefault)
	got := recvAll(t, consumer, len(sent), 60*time.Second, "升级前消息")
	for id := range sent {
		if !got[id] {
			t.Errorf("升级前消息 %s 未收到", id)
		}
	}
	consumer.GracefulStop()

	// 升级后继续收发（写路径已入 raft）
	producer2 := newClusterProducer(t, ep, "e2e-upgrade")
	for i := 0; i < 5; i++ {
		recs, err := producer2.Send(context.Background(), &rmq.Message{Topic: "e2e-upgrade", Body: []byte(fmt.Sprintf("post-upgrade #%d", i))})
		if err != nil {
			t.Fatalf("升级后 Send #%d: %v", i, err)
		}
		sent[recs[0].MessageID] = true
	}
	producer2.GracefulStop()
	consumer2 := newClusterConsumer(t, ep, "e2e-upgrade-g2", "e2e-upgrade", clusterConsumerAwaitDefault)
	got2 := recvAll(t, consumer2, len(sent), 60*time.Second, "升级后全量")
	for id := range sent {
		if !got2[id] {
			t.Errorf("升级后消息 %s 未收到", id)
		}
	}
	consumer2.GracefulStop()
	h2.stop(t)
	h2Stopped = true

	// ---- 断言 raft/ 前缀已出现：升级确实入了 raft ----
	// 直接打开数据目录扫 raft/ 前缀（broker 已停，pebble 锁已释放）。
	st, err := store.Open(filepath.Join(dir, "data"), false, testSlog())
	if err != nil {
		t.Fatalf("打开升级后数据目录: %v", err)
	}
	defer st.Close()
	raftKeys := 0
	err = st.Scan([]byte("raft/"), store.PrefixUpperBound([]byte("raft/")), 0,
		func(k, v []byte) (bool, error) {
			raftKeys++
			return true, nil
		})
	if err != nil {
		t.Fatalf("扫描 raft/ 前缀: %v", err)
	}
	if raftKeys == 0 {
		t.Fatal("升级后数据目录未出现 raft/ 前缀——集群段未生效或回退到了单机路径")
	}
	t.Logf("升级验证完成：raft/ 前缀 %d 个键，升级前 %d 条 + 升级后 %d 条消息全部收发", raftKeys, 5, 5)
}

// TestClusterDLQ 三节点下投递次数耗尽的毒消息跨组转入死信（评审 C1）：
// 死信 topic（%DLQ%{group}）是独立 topic，组号 GroupForQueue 与被消费
// 队列无关——本节点不 lead 死信组时，moveToDLQ 第一段必须经
// ForwardAppend 转发给死信组 leader（修复前直接 pr.Append 报
// ErrNotLeader，attempts 耗尽恰是 DLQ 设计场景，该队列每次 Receive 都
// 撞死、消费永久停摆）。
//
// 用例：发毒消息 → 消费 2 次不 ack（maxAttempts=2）→ 断言死信 topic
// 收到（新消费组拉取，含来源属性）→ 断言原队列仍可正常收发（不停摆）。
// 修复前约 2/3 概率失败（毒消息队列组 leader 恰不 lead 死信组时）。
func TestClusterDLQ(t *testing.T) {
	grpcPorts := pickPorts(t, 3)
	raftPorts := pickPorts(t, 3)
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cfgs := make([]*config.Config, 3)
	for i := 0; i < 3; i++ {
		cfgs[i] = clusterNodeConfig(t, dirs[i], uint64(i+1), grpcPorts[i], raftPorts[i], 3)
		cfgs[i].DefaultMaxAttempts = 2 // 2 次投递即超限，控制用例时长
	}
	writeClusterPeers(cfgs, grpcPorts, raftPorts)
	endpoints := make([]string, 3)
	cfgPaths := make([]string, 3)
	logPaths := make([]string, 3)
	for i := 0; i < 3; i++ {
		cfgPaths[i] = writeNodeConfig(t, cfgs[i])
		endpoints[i] = fmt.Sprintf("127.0.0.1:%d", grpcPorts[i])
		logPaths[i] = filepath.Join(dirs[i], "broker.log")
	}
	started := make([]*brokerHandle, 3)
	for i := 0; i < 3; i++ {
		logFile, err := os.Create(logPaths[i])
		if err != nil {
			t.Fatalf("创建 broker 日志文件失败: %v", err)
		}
		cmd := exec.Command(brokerBinary, "-config", cfgPaths[i])
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err := cmd.Start(); err != nil {
			logFile.Close()
			t.Fatalf("启动 broker 进程失败: %v", err)
		}
		waitDone := make(chan error, 1)
		go func() { waitDone <- cmd.Wait() }()
		started[i] = &brokerHandle{
			endpoint: endpoints[i], cfgPath: cfgPaths[i], logPath: logPaths[i],
			logFile: logFile, cmd: cmd, waitDone: waitDone,
		}
	}
	for i := 0; i < 3; i++ {
		waitBrokerReady(t, started[i].endpoint, started[i].waitDone, started[i].logPath)
	}
	t.Cleanup(func() {
		for i, h := range started {
			if h.cmd.Process != nil {
				h.stop(t)
			}
			if t.Failed() {
				dumpBrokerLog(t, logPaths[i])
			}
		}
	})
	multi := strings.Join(endpoints, ";")
	const topic, group = "e2e-dlq", "e2e-dlq-g"
	ensureTopic(t, endpoints, topic)
	waitRouteSpread(t, endpoints, topic, 60*time.Second)

	// 发毒消息（不发消息则重投扫描无事可做，转入是惰性的）。连发 3 条：
	// SDK 发布负载均衡按队列轮询（首 3 次落到 q2/q3/q4），DLQ topic
	// 归组 g2——q3 的源队列组是 g3，与死信组必然不同节点（摊布后每节点
	// 恰领一组），第 1 段跨组转发被确定性触发；其余走本地路径，两类都
	// 覆盖到。
	producer := newClusterProducer(t, multi, topic)
	poisonBodies := []string{"dlq-poison-a", "dlq-poison-b", "dlq-poison-c"}
	for i, body := range poisonBodies {
		if _, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(body)}); err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
	}
	producer.GracefulStop()

	// 消费 2 次不 ack（invisible 短窗，任其过期重投）——第 2 次投递后
	// attempts 达上限，下一次 Receive 触发转入死信。逐条计数，3 条都
	// 要见过 2 次投递。
	consumer := newClusterConsumer(t, multi, group, topic, clusterConsumerAwaitDefault)
	seen := map[string]int{"dlq-poison-a": 0, "dlq-poison-b": 0, "dlq-poison-c": 0}
	deadline := time.Now().Add(120 * time.Second)
	for (seen["dlq-poison-a"] < 2 || seen["dlq-poison-b"] < 2 || seen["dlq-poison-c"] < 2) && time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 3*time.Second)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, mv := range mvs {
			if n, ok := seen[string(mv.GetBody())]; ok {
				seen[string(mv.GetBody())] = n + 1
				t.Logf("毒消息 %s 第 %d 次收到（不 ack）", mv.GetBody(), n+1)
			}
		}
	}
	for _, body := range poisonBodies {
		if seen[body] < 2 {
			t.Fatalf("毒消息 %s 未完成 2 次投递: %d", body, seen[body])
		}
	}

	// 死信消费者：%DLQ%{group} 是普通 topic，SDK 直接订阅（新消费组）
	dlqTopic := "%DLQ%" + group
	dlqConsumer := newClusterConsumer(t, multi, group+"-reader", dlqTopic, clusterConsumerAwaitDefault)
	gotDLQ := map[string]bool{}
	deadline = time.Now().Add(120 * time.Second)
	for len(gotDLQ) < len(poisonBodies) && time.Now().Before(deadline) {
		_, _ = consumer.Receive(context.Background(), 16, 3*time.Second) // 戳原 topic：惰性转入 + 退避窗口过期
		mvs, err := dlqConsumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, mv := range mvs {
			gotDLQ[string(mv.GetBody())] = true
			props := mv.GetProperties()
			if props["sq-origin-topic"] != topic {
				t.Fatalf("死信缺少来源属性 sq-origin-topic: %v", props)
			}
			if err := dlqConsumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack 死信: %v", err)
			}
		}
	}
	for _, body := range poisonBodies {
		if !gotDLQ[body] {
			t.Fatalf("死信未收到 %s（修复前：不 lead 死信组的节点转入失败、队列停摆）", body)
		}
	}
	// 转发证据：moveToDLQ 第一段在非死信组 leader 节点经 ForwardAppend
	// 转发（日志「死信消息跨节点转发」）。q3 毒消息的源组与死信组必然
	// 不同节点，断言 ≥1 次——跨组转发路径被确定性覆盖（评审 C1）。
	fwdDLQ := countLogLines(t, logPaths, "死信消息跨节点转发")
	if fwdDLQ == 0 {
		t.Fatalf("死信跨组转发零次：q3 源组(g3)与死信组(g2)应不同节点，转发路径未触发")
	}
	t.Logf("毒消息已跨组转入死信：%s 收齐 %d 条，来源属性完整；死信转发计数=%d",
		dlqTopic, len(gotDLQ), fwdDLQ)

	// 原队列不停摆：继续发一条并收 ack（修复前该队列的 Receive 会因
	// moveToDLQ 的 ErrNotLeader 持续失败）
	producer2 := newClusterProducer(t, multi, topic)
	if _, err := producer2.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte("after-dlq")}); err != nil {
		t.Fatalf("Send after-dlq: %v", err)
	}
	producer2.GracefulStop()
	gotAfter := recvAllAck(t, consumer, 1, 60*time.Second, "DLQ 后原队列消费", true)
	for id := range gotAfter {
		t.Logf("DLQ 后原队列正常消费 msgId=%s（不停摆）", id)
	}
	consumer.GracefulStop()
	dlqConsumer.GracefulStop()
}

// TestThreeNodeClusterE2E 三节点全流程：全新起（三份配置、同一
// credentials）→ SDK 三地址接入 → 发 200 → 全收全 ack（记粗略消费
// 吞吐）→ kill 某数据组 leader → 继续发收（允许重复不允许丢）→ 重启
// 被 kill 节点（自动 Rejoin 自愈）→ 三节点对账（路由恢复 + 无丢失）。
func TestThreeNodeClusterE2E(t *testing.T) {
	grpcPorts := pickPorts(t, 3)
	raftPorts := pickPorts(t, 3)
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}

	cfgs := make([]*config.Config, 3)
	for i := 0; i < 3; i++ {
		cfgs[i] = clusterNodeConfig(t, dirs[i], uint64(i+1), grpcPorts[i], raftPorts[i], 3)
	}
	writeClusterPeers(cfgs, grpcPorts, raftPorts)
	endpoints := make([]string, 3)
	cfgPaths := make([]string, 3)
	handles := make([]*brokerHandle, 3)
	logPaths := make([]string, 3)
	for i := 0; i < 3; i++ {
		cfgPaths[i] = writeNodeConfig(t, cfgs[i])
		endpoints[i] = fmt.Sprintf("127.0.0.1:%d", grpcPorts[i])
		logPaths[i] = filepath.Join(dirs[i], "broker.log")
	}
	// 三节点必须**全部先起进程、再逐个等就绪**：raft 选举要 quorum，
	// 单节点先等就绪会永远卡在「等 meta 组出 leader」（main 的 60s
	// 超时兜底），等完再起第二个节点 = 第一个节点已经退出了。
	started := make([]*brokerHandle, 3)
	for i := 0; i < 3; i++ {
		logFile, err := os.Create(logPaths[i])
		if err != nil {
			t.Fatalf("创建 broker 日志文件失败: %v", err)
		}
		cmd := exec.Command(brokerBinary, "-config", cfgPaths[i])
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err := cmd.Start(); err != nil {
			logFile.Close()
			t.Fatalf("启动 broker 进程失败: %v", err)
		}
		waitDone := make(chan error, 1)
		go func() { waitDone <- cmd.Wait() }()
		started[i] = &brokerHandle{
			endpoint: endpoints[i], cfgPath: cfgPaths[i], logPath: logPaths[i],
			logFile: logFile, cmd: cmd, waitDone: waitDone,
		}
	}
	for i := 0; i < 3; i++ {
		waitBrokerReady(t, started[i].endpoint, started[i].waitDone, started[i].logPath)
		t.Logf("broker %d 已就绪，endpoint=%s pid=%d", i+1, started[i].endpoint, started[i].cmd.Process.Pid)
	}
	handles = started
	// 内存峰值采样（Linux）；全部 broker 收尾时统一 stop（先采样后停，
	// 顺序无关紧要——峰值是采样窗口内的上限）。
	peaks := newMemPeakSampler(handles)
	t.Cleanup(func() {
		peaks.stopAndReport(t)
		for i, h := range handles {
			if h.cmd.Process != nil {
				h.stop(t)
			}
			if t.Failed() {
				dumpBrokerLog(t, filepath.Join(dirs[i], "broker.log"))
			}
		}
	})

	// SDK 用「;」分隔的三地址接入（SDK resolver 按 ; 拆分）
	multi := strings.Join(endpoints, ";")
	const topic, group = "e2e-cluster", "e2e-cluster-g"
	// 显式建 topic：SDK 的 producer.Start 对初始 topic 只做一次
	// QueryRoute 不跨节点重试，而 topic 注册是 meta 组写（只有 meta
	// leader 能建）——先轮询创建、再等三节点路由齐备，见 ensureTopic。
	ensureTopic(t, endpoints, topic)
	ensureTopic(t, endpoints, "e2e-cluster-txn")
	// 诊断：打印每个节点的路由视角（leader 摊布是否生效、各队列指向谁）
	for i, ep := range endpoints {
		qs := queryRoute(t, ep, topic)
		detail := make([]string, 0, len(qs))
		for _, q := range qs {
			for _, a := range q.GetBroker().GetEndpoints().GetAddresses() {
				detail = append(detail, fmt.Sprintf("q%d@%s:%d(%s)", q.GetId(), a.GetHost(), a.GetPort(), q.GetBroker().GetName()))
			}
		}
		t.Logf("节点 %d 的路由视角：%s", i+1, strings.Join(detail, " "))
	}
	// 等 leader 摊布收敛（60s）：三节点健康期是吞吐测量 + 事务转发证据
	// 的窗口，摊布循环（5s 周期 + 3 周期稳定观察 ≈ 15s+）会把各组转移到
	// preferred 节点——转移窗口内 SDK 路由缓存（30s 刷新）指向旧 leader，
	// Receive 立即回 HA_NOT_AVAILABLE，消费侧对陈旧路由空转（实测 4661
	// 次/s 热循环），吞吐数字被路由陈旧时间主导、事务转发证据偶发为 0
	// （全组恰在同一节点）。等摊布落定后再进入测量窗口，数字才反映
	// broker 真实投递能力。
	waitRouteSpread(t, endpoints, topic, 60*time.Second)
	producer := newClusterProducer(t, multi, topic)
	consumer := newClusterConsumer(t, multi, group, topic, clusterConsumerAwaitShort) // 吞吐测量路径：短轮询档（见函数注释）

	// ---- 健康期：发 200 → 全收全 ack（含粗略消费吞吐）----
	sent := map[string]bool{}
	for i := 0; i < clusterProduceCount; i++ {
		recs, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("cluster #%d", i))})
		if err != nil {
			t.Fatalf("健康期 Send #%d: %v", i, err)
		}
		sent[recs[0].MessageID] = true
	}
	t.Logf("健康期已发送 %d 条", len(sent))

	// 注意：新消费组 fetch 位点从 0 起步（重放该 topic 全量历史，
	// 见 deliver 阶段 2 注释），因此每个阶段都用**新消费组**做全量
	// 对账——天然把「kill/重入后一条不丢」的断言强化为全历史无丢失。
	recvStart := time.Now()
	got := recvAllAck(t, consumer, len(sent), 180*time.Second, "健康期消费", true)
	elapsed := time.Since(recvStart)
	rate := float64(len(got)) / elapsed.Seconds()
	// plan 风险自记 ②：Receive 每次都过 raft 提案，消费吞吐必须有粗略
	// 数字防量级劣化无感（非 benchmark，不做抖动分析）。消费者用短轮询
	// 档（clusterConsumerAwaitShort=100ms）——长档 3s 下每次空轮询等满
	// 3s，测出的是 SDK 轮询节拍不是 broker 吞吐（评审 Important 3）；
	// 短档下空轮询 100ms 即返，数字反映 broker 实际投递能力。
	t.Logf("三节点消费吞吐（短轮询档 100ms，粗略非 benchmark）：%.0f msg/s（%d 条 / %.1fs）",
		rate, len(got), elapsed.Seconds())
	for id := range sent {
		if !got[id] {
			t.Errorf("健康期消息 %s 未收到", id)
		}
	}
	// 注意：consumer 不在这里 GracefulStop——kill 后阶段要复用它
	// （同组游标继续，收 kill 后新增；见「继续发收」段注释）。

	// ---- 事务用例（plan 风险自记 ①）：两跳转发链的可达性证据 ----
	// 事务消息独立 topic，避免与 200 条普通消息的消费位点交织。
	// 转发链：Stage/End 第二段的 half 键写经 ApplyOrForward（接收节点
	// 非 meta leader → ForwardApply），End 第一段的队列写入在接收节点
	// 非队列 0 所属组 leader 时经 ForwardAppend 转发。SDK 的发布负载
	// 均衡按队列轮询（6 队列 = 3 组 × 2 队列，覆盖 3 个节点），连发
	// txnForwardProbeCount 条保证两类转发都至少命中一次；提交完成后
	// 逐节点日志 grep 断言（转发节点 Info 日志，见下述两个计数）。
	txnProducer := newClusterProducer(t, multi, "e2e-cluster-txn")
	txnConsumer := newClusterConsumer(t, multi, "e2e-cluster-txn-g", "e2e-cluster-txn", clusterConsumerAwaitDefault)
	txnSent := map[string]bool{}
	for i := 0; i < txnForwardProbeCount; i++ {
		tx := txnProducer.BeginTransaction()
		recs, err := txnProducer.SendWithTransaction(context.Background(),
			&rmq.Message{Topic: "e2e-cluster-txn", Body: []byte(fmt.Sprintf("cluster-txn-commit #%d", i))}, tx)
		if err != nil {
			t.Fatalf("SendWithTransaction #%d: %v", i, err)
		}
		if len(recs) == 0 || recs[0].MessageID == "" {
			t.Fatalf("SendWithTransaction #%d 返回空回执: %v", i, recs)
		}
		txnSent[recs[0].MessageID] = true
		if err := tx.Commit(); err != nil {
			t.Fatalf("事务提交 #%d 失败: %v", i, err)
		}
	}
	t.Logf("事务已提交 %d 条 msgId=%v（两跳转发链探针）", len(txnSent), txnSent)
	txnGot := recvAllAck(t, txnConsumer, len(txnSent), 60*time.Second, "事务提交后消费", true)
	for id := range txnSent {
		if !txnGot[id] {
			t.Fatalf("事务消息 %s 未收到（事务链路断裂）", id)
		}
	}
	// 转发证据断言：逐节点日志 grep。SDK 轮询 6 队列必然落到不同节点，
	// 两类转发日志合计都应为正——这是风险自记① 的 keep-vs-delete 裁决
	// （转发代码是承重路径，删了就断链，不是防御死代码）。
	fwdAppend := countLogLines(t, logPaths, "事务提交消息跨节点转发") // End 第一段 ForwardAppend（txn.go）
	fwdApply := countLogLines(t, logPaths, "跨节点转发批次")      // Stage/End 第二段 ForwardApply（replication.go）
	t.Logf("事务转发证据：ForwardAppend 第一段=%d 次，ForwardApply 第二段=%d 次（全节点日志合计）",
		fwdAppend, fwdApply)
	if fwdAppend == 0 {
		t.Fatalf("事务 End 第一段 ForwardAppend 零次：SDK 轮询应覆盖非队列 0 组 leader 节点（转发链证据缺失）")
	}
	if fwdApply == 0 {
		t.Fatalf("事务 Stage/End 第二段 ForwardApply 零次：SDK 轮询应覆盖非 meta leader 节点（转发链证据缺失）")
	}
	txnProducer.GracefulStop()
	txnConsumer.GracefulStop()

	// ---- kill 当前某数据组 leader 进程 ----
	queues := queryRoute(t, endpoints[0], topic)
	t.Logf("kill 前路由：%d 条队列，端点分布 %v", len(queues), routeEndpointCounts(queues))
	victimEP := leaderOfMostQueues(routeEndpointCounts(queues))
	victimIdx := -1
	for i, ep := range endpoints {
		if ep == victimEP {
			victimIdx = i
			break
		}
	}
	if victimIdx < 0 {
		t.Fatalf("路由端点 %s 不在三节点里: %v", victimEP, endpoints)
	}
	t.Logf("kill 数据组 leader：%s（路由持有 %d/%d 队列）", victimEP, routeEndpointCounts(queues)[victimEP], len(queues))
	handles[victimIdx].kill(t)

	// ---- 继续发收：SDK 重试 + 路由刷新自愈，允许重复不允许丢 ----
	// 复用 kill 前的 producer/consumer（生产环境「节点挂掉客户端不重启」
	// 的真实形态）。SDK 的路由缓存有 30s 刷新节拍：kill 后缓存仍指向
	// 死节点，头几条 Send 会烧满 SDK 内部 5 次尝试 × 5s 拨号超时后失败
	//（diag 实测），刷新节拍一到即恢复——这里先等刷新窗口再发，避免把
	// 预知的慢路径反复踩满（消息不允许丢，允许测试侧重发）。
	time.Sleep(35 * time.Second) // SDK 路由刷新节拍（30s）+ 新 leader 选举余量
	postKill := map[string]bool{}
	for i := 0; i < 50; i++ {
		recs, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("post-kill #%d", i))})
		if err != nil {
			t.Logf("post-kill Send #%d 首次失败（SDK 路由刷新/选举窗口）: %v", i, err)
			for retry := 0; retry < 12; retry++ {
				time.Sleep(5 * time.Second)
				recs, err = producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("post-kill #%d", i))})
				if err == nil {
					break
				}
			}
			if err != nil {
				t.Fatalf("post-kill Send #%d 重试仍失败: %v", i, err)
			}
		}
		postKill[recs[0].MessageID] = true
	}
	t.Logf("kill 后已发送 %d 条", len(postKill))

	// 复用原 consumer（同组游标已越过健康期消息，只收 kill 后新增）
	got3 := recvAllAck(t, consumer, len(postKill), 240*time.Second, "kill 后消费", true)
	for id := range postKill {
		if !got3[id] {
			t.Errorf("kill 后消息 %s 丢失", id)
		}
	}
	consumer.GracefulStop()

	// ---- 重入：以原配置重启被 kill 节点 ----
	// main 检测到 ErrUncleanShutdown（SIGKILL 没写干净标记）→ 打 Error
	// 日志 → st.Close → cluster.Rejoin：清空数据目录、PrepareJoin 全组
	// 编排、fresh 启动追平、AutoPromoteLearners 升回 voter——无人值守
	// 自愈，无需人工介入（plan Task 11 Step 4）。
	handles[victimIdx] = launchBroker(t, cfgPaths[victimIdx], endpoints[victimIdx],
		filepath.Join(dirs[victimIdx], "broker-rejoin.log"))
	t.Logf("被 kill 节点已重启（自动 Rejoin 自愈）: %s", endpoints[victimIdx])

	// ---- 三节点对账：路由恢复（重入节点重新 lead 组）+ 无丢失 ----
	// 重入节点先以 learner 追平，AutoPromote 升 voter 后摊布循环把
	// preferred 组转回给它——轮询路由直到它的端点重新出现。
	deadline := time.Now().Add(180 * time.Second)
	rejoined := false
	for time.Now().Before(deadline) {
		qs := queryRoute(t, endpoints[(victimIdx+1)%3], topic)
		if routeEndpointCounts(qs)[endpoints[victimIdx]] > 0 {
			rejoined = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !rejoined {
		t.Fatal("重入节点 180s 内未重新出现在路由中（learner 追平/升 voter 链路断裂）")
	}
	t.Logf("重入节点已恢复为组 leader：%s 重新出现在路由", endpoints[victimIdx])

	producer4 := newClusterProducer(t, multi, topic)
	for i := 0; i < 20; i++ {
		recs, err := producer4.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("final #%d", i))})
		if err != nil {
			t.Fatalf("final Send #%d: %v", i, err)
		}
		postKill[recs[0].MessageID] = true
	}
	producer4.GracefulStop()
	// 对账：新消费组从位点 0 重放全量历史——健康期 200 + kill 后 50 +
	// 重入后 20 全部收齐，证明三阶段无一条丢失（新组重放 = 全历史断言）。
	consumer4 := newClusterConsumer(t, multi, "e2e-cluster-g3", topic, clusterConsumerAwaitDefault)
	got4 := recvAllAck(t, consumer4, len(sent)+len(postKill), 240*time.Second, "三节点对账消费", true)
	for id := range sent {
		if !got4[id] {
			t.Errorf("对账发现健康期消息丢失: %s", id)
		}
	}
	for id := range postKill {
		if !got4[id] {
			t.Errorf("对账发现 kill 后消息丢失: %s", id)
		}
	}
	consumer4.GracefulStop()
	t.Logf("三节点对账完成：健康期 %d + kill 后 %d + 重入后 %d = %d 条消息无丢失",
		len(sent), 50, 20, len(sent)+len(postKill))
}

// newClusterProducer 构造带集群凭据的 Producer（各用例独立 topic）。
func newClusterProducer(t *testing.T, endpoint, topic string) rmq.Producer {
	t.Helper()
	p, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{AccessKey: clusterAK, AccessSecret: clusterSK},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	return p
}

// newClusterConsumer 构造带集群凭据的 SimpleConsumer（SUB_ALL）。
//
// await 是 SDK 的简单消费者轮询档位：Receive 对单个队列的长轮询上限
// （官方 SDK 会把 awaitDuration 放进请求的 long-polling timeout）。
// 吞吐测量路径必须用短档（clusterConsumerAwaitShort）——长档（3s）下
// 每次空轮询都要等满 3s，测量结果是「轮询节拍」而不是 broker 吞吐
// （8 msg/s ≈ 16 条/3s 周期的惨案，评审 Important 3）。
//
// 光调 await 还不够：SDK 的 Receive 超时 = awaitDuration + 客户端请求
// 超时（默认 3s），服务端长轮询时长按请求 deadline 推导——只调 await
// 时空轮询仍要等 ≈3s+100ms-1s 余量 ≈2.1s（实测 13 msg/s 仍被空轮询
// 主导）。因此测量路径同时把请求超时压到 clusterConsumerReqTimeout，
// 空轮询 ≈ await 即返，数字才反映 broker 实际投递能力。
func newClusterConsumer(t *testing.T, endpoint, group, topic string, await time.Duration) rmq.SimpleConsumer {
	t.Helper()
	c, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{AccessKey: clusterAK, AccessSecret: clusterSK},
	},
		rmq.WithSimpleAwaitDuration(await),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if await == clusterConsumerAwaitShort {
		c.SetRequestTimeout(clusterConsumerReqTimeout) // 吞吐测量路径：压短请求超时（见函数注释）
	}
	if err := c.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	return c
}

// countLogLines 统计全部 broker 日志文件中包含 needle 的行数。
//
// 用途：跨节点转发链的证据断言（事务用例）——转发动作在**转发节点**
// 打 Info 日志（ForwardAppend 见 txn.go「事务提交消息跨节点转发」、
// ForwardApply 见 replication.go「跨节点转发批次」），测试在事务完成后
// 逐节点日志 grep 计数，据此判定哪些跳真实执行过（plan 风险自记①）。
func countLogLines(t *testing.T, logPaths []string, needle string) int {
	t.Helper()
	total := 0
	for _, p := range logPaths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Logf("读取 broker 日志 %s 失败: %v", p, err)
			continue
		}
		total += strings.Count(string(raw), needle)
	}
	return total
}

// waitRouteSpread 轮询路由直到 topic 的队列摊布到全部节点（每节点至少
// 领 1 条队列），超时 Fatal。
//
// 为什么需要等摊布收敛：leader 摊布循环把各组转移到 preferred 节点需要
// 若干周期（5s 周期 × 3 稳定观察 ≈ 15s+），收敛前可能全部数据组集中
// 在同一节点。健康期吞吐测量与事务转发证据断言都要求路由稳定且分散——
// 转移窗口内 SDK 路由缓存指向旧 leader，Receive 立即回 HA_NOT_AVAILABLE，
// 消费侧对陈旧路由空转（实测 4661 次/s 热循环），吞吐数字被路由陈旧
// 时间主导而非 broker 能力；事务转发证据在「全组同节点」时天然为 0。
func waitRouteSpread(t *testing.T, endpoints []string, topic string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		qs := queryRoute(t, endpoints[0], topic)
		if len(routeEndpointCounts(qs)) == len(endpoints) {
			t.Logf("leader 摊布已收敛：%d 条队列分布到 %d 个节点 %v",
				len(qs), len(endpoints), routeEndpointCounts(qs))
			return
		}
		time.Sleep(1 * time.Second)
	}
	qs := queryRoute(t, endpoints[0], topic)
	t.Fatalf("leader 摊布 %v 内未收敛到 %d 个节点，当前分布 %v",
		timeout, len(endpoints), routeEndpointCounts(qs))
}

// recvAll 轮询消费直到收齐 want 个不同 msgId 或超时，返回收到的 msgId
// 集合。空轮询（MESSAGE_NOT_FOUND）与临时错误按正常路径继续。
func recvAll(t *testing.T, consumer rmq.SimpleConsumer, want int, window time.Duration, phase string) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	deadline := time.Now().Add(window)
	rounds := 0
	for len(got) < want && time.Now().Before(deadline) {
		rounds++
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			// 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常；HA_NOT_AVAILABLE
			// 出现在 SDK 路由缓存过期（leader 摊布转移后 30s 刷新窗口内）——
			// 必须退避再试，否则对陈旧路由的 Receive 会以 RPC 速率空转
			// （三节点 e2e 实测 4661 次/s 的 CPU 热循环）
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, mv := range mvs {
			got[mv.GetMessageId()] = true
		}
	}
	if len(got) < want {
		t.Fatalf("%s：%v 内只收到 %d/%d 条", phase, window, len(got), want)
	}
	t.Logf("%s：%d 轮收齐 %d 条", phase, rounds, len(got))
	return got
}

// recvAllAck 与 recvAll 同款轮询，但逐条 ack（消费位点推进，避免同组
// 重复投递污染后续阶段断言）。
//
// strict=true 时收不满 want 条直接 Fatal（既有调用点语义）；strict=false
// 时返回「已收集到的集合」交由调用方断言——场景测试的确认集对账器要
// 输出更有针对性的缺失清单（见 cluster_scenario_test.go 的
// assertAllConsumed），不能在这里用一句「只收到 X/Y 条」把缺失详情吞掉。
func recvAllAck(t *testing.T, consumer rmq.SimpleConsumer, want int, window time.Duration, phase string, strict bool) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	deadline := time.Now().Add(window)
	rounds := 0
	for len(got) < want && time.Now().Before(deadline) {
		rounds++
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			// 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常；HA_NOT_AVAILABLE
			// 出现在 SDK 路由缓存过期（leader 摊布转移后 30s 刷新窗口内）——
			// 必须退避再试，否则对陈旧路由的 Receive 会以 RPC 速率空转
			// （三节点 e2e 实测 4661 次/s 的 CPU 热循环）
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, mv := range mvs {
			got[mv.GetMessageId()] = true
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("%s：Ack %s 失败: %v", phase, mv.GetMessageId(), err)
			}
		}
	}
	if strict && len(got) < want {
		t.Fatalf("%s：%v 内只收到 %d/%d 条", phase, window, len(got), want)
	}
	t.Logf("%s：%d 轮收齐并 ack %d 条（want %d）", phase, rounds, len(got), want)
	return got
}

// testSlog 供打开 store 时构造 logger（避免向全局默认 logger 写测试噪音）。
func testSlog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
