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
//   - 事务用例放在三节点健康期：EndTransaction 落 meta leader 后，若
//     目标队列组 leader 是别节点，txn.End 第一段走 ForwardAppend——这是
//     官方 SDK 可达的最长分布式路径（plan 风险自记 ①，两跳转发链中
//     ForwardApply 一跳因 SDK 寻址行为不可达，属防御代码不删）
//   - 消费吞吐只记粗略数字（plan 风险自记 ②：Receive 每次都过 raft
//     提案，防止量级劣化无感），非 benchmark
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
		GRPCListen:         fmt.Sprintf("127.0.0.1:%d", grpcPort),
		AdvertiseHost:      "127.0.0.1",
		AdvertisePort:      grpcPort,
		DataDir:            filepath.Join(dir, "data"),
		Fsync:              "sync",
		AutoCreateTopic:    true,
		DefaultQueueNums:   6, // plan：保证三组都有队列
		DefaultMaxAttempts: 16,
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
	consumer := newClusterConsumer(t, ep, "e2e-upgrade-g", "e2e-upgrade")
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
	consumer2 := newClusterConsumer(t, ep, "e2e-upgrade-g2", "e2e-upgrade")
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
	producer := newClusterProducer(t, multi, topic)
	consumer := newClusterConsumer(t, multi, group, topic)

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
	got := recvAllAck(t, consumer, len(sent), 180*time.Second, "健康期消费")
	elapsed := time.Since(recvStart)
	rate := float64(len(got)) / elapsed.Seconds()
	// plan 风险自记 ②：Receive 每次都过 raft 提案，消费吞吐必须有粗略
	// 数字防量级劣化无感（非 benchmark，不做抖动分析）
	t.Logf("三节点消费吞吐（粗略，非 benchmark）：%.0f msg/s（%d 条 / %.1fs）", rate, len(got), elapsed.Seconds())
	for id := range sent {
		if !got[id] {
			t.Errorf("健康期消息 %s 未收到", id)
		}
	}
	// 注意：consumer 不在这里 GracefulStop——kill 后阶段要复用它
	// （同组游标继续，收 kill 后新增；见「继续发收」段注释）。

	// ---- 事务用例（plan 风险自记 ①）：最长分布式路径 ----
	// EndTransaction 落 meta leader；若目标队列组 leader 是别节点，
	// txn.End 第一段走 ForwardAppend（控制通道转发 + leader 侧 offset
	// 分配）——官方 SDK 可达的最长路径。事务消息独立 topic，避免与
	// 200 条普通消息的消费位点交织。
	txnProducer := newClusterProducer(t, multi, "e2e-cluster-txn")
	txnConsumer := newClusterConsumer(t, multi, "e2e-cluster-txn-g", "e2e-cluster-txn")
	tx := txnProducer.BeginTransaction()
	recs, err := txnProducer.SendWithTransaction(context.Background(),
		&rmq.Message{Topic: "e2e-cluster-txn", Body: []byte("cluster-txn-commit")}, tx)
	if err != nil {
		t.Fatalf("SendWithTransaction: %v", err)
	}
	if len(recs) == 0 || recs[0].MessageID == "" {
		t.Fatalf("SendWithTransaction 返回空回执: %v", recs)
	}
	txnMsgID := recs[0].MessageID
	if err := tx.Commit(); err != nil {
		t.Fatalf("事务提交失败: %v", err)
	}
	t.Logf("事务已提交 msgId=%s（跨节点转发链）", txnMsgID)
	txnGot := recvAllAck(t, txnConsumer, 1, 60*time.Second, "事务提交后消费")
	if !txnGot[txnMsgID] {
		t.Fatalf("事务消息 %s 未收到（ForwardAppend 转发链断裂）", txnMsgID)
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
	got3 := recvAllAck(t, consumer, len(postKill), 240*time.Second, "kill 后消费")
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
	consumer4 := newClusterConsumer(t, multi, "e2e-cluster-g3", topic)
	got4 := recvAllAck(t, consumer4, len(sent)+len(postKill), 240*time.Second, "三节点对账消费")
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
func newClusterConsumer(t *testing.T, endpoint, group, topic string) rmq.SimpleConsumer {
	t.Helper()
	c, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
		Credentials:   &credentials.SessionCredentials{AccessKey: clusterAK, AccessSecret: clusterSK},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	return c
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
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
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
func recvAllAck(t *testing.T, consumer rmq.SimpleConsumer, want int, window time.Duration, phase string) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	deadline := time.Now().Add(window)
	rounds := 0
	for len(got) < want && time.Now().Before(deadline) {
		rounds++
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询以 MESSAGE_NOT_FOUND 错误返回，属正常
		}
		for _, mv := range mvs {
			got[mv.GetMessageId()] = true
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("%s：Ack %s 失败: %v", phase, mv.GetMessageId(), err)
			}
		}
	}
	if len(got) < want {
		t.Fatalf("%s：%v 内只收到 %d/%d 条", phase, window, len(got), want)
	}
	t.Logf("%s：%d 轮收齐并 ack %d 条", phase, rounds, len(got))
	return got
}

// testSlog 供打开 store 时构造 logger（避免向全局默认 logger 写测试噪音）。
func testSlog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
