//go:build e2e

// consume_failover_test.go：消费状态内存权威态的故障注入对账（卡 SQ4.1 S2/S3）。
//
// 职责：
//   - 持续生产 + 多消费者收+ack 的同时注入三类故障：(a) kill -9 承载队列
//     最多的节点；(c) 同类节点 SIGTERM 优雅停机后重启；(b) 重启后摊布把
//     preferred 组转回该节点（计划内转移领导权）
//   - 两种持久化模式（检查点 / 逐次复制）各跑一遍；检查点间隔 kill9 场景
//     1s，graceful+transfer 场景抬到 1h（判别力前提，见 cfGracefulTransfer）
//   - 判据（计划第 4 节「重复」口径）：丢失 = 0（任何模式、任何场景）；
//     (b)(c) 检查点丢失型重复 = 0；(a) 逐次模式 = 0、检查点模式
//     ≤ 2 × R × (I + 1s)（P5，R = kill 前 5s 全体消费者实测投递速率）
//
// 「检查点丢失型重复」= (i) 某投递在该消息一次 Ack 成功之后又被投出；
// (ii) 同一条存储消息（msgID@offset）以 DeliveryAttempt=1 被投出超过一次。
// 处理超时、ack 失败后的正常重投（attempt 递增、此前无成功 ack）不计入。
//
// ack 走原始 gRPC：官方 Go SDK 的 SimpleConsumer.Ack 不检查响应状态码
// （simple_consumer.go:442，HA_NOT_AVAILABLE / INVALID_RECEIPT_HANDLE 也
// 返回 nil），用它判「Ack 成功」会把未生效的 ack 当成功、把随后的正常重投
// 误计为重复。原始 ack 逐节点尝试：OK = 成功；INVALID_RECEIPT_HANDLE =
// 终局未确认；HA_NOT_AVAILABLE/传输错误 = 换节点重试，至多 30s。
//
// 边界：不测吞吐（sdk_cluster_bench_test.go 的 TestClusterConsumeThroughput）。
package e2e

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	sdkpb "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/Xsxdot/sq/internal/config"
)

func init() {
	// 消费吞吐基准的模式维度（sdk_cluster_bench_test.go 的扩展点）：本卡起
	// 可配置检查点间隔。
	consumeBenchModes = []consumeBenchMode{
		{"checkpoint-1s", func(c *config.Config) { c.ConsumeCheckpointInterval = "1s" }},
		{"perop", func(c *config.Config) { c.ConsumeCheckpointInterval = "0" }},
	}
}

const (
	cfInvisible  = 60 * time.Second // 排除超时重投干扰（计划第 4 节）
	cfConsumers  = 4
	cfAckWindow  = 30 * time.Second
	cfRateWindow = 5 * time.Second
	// cfTakeover 场景 (c) 接管期消费者起来后、重启停机节点前的消费时长（见
	// cfGracefulTransfer 判别力前提 1）
	cfTakeover = 10 * time.Second
	// cfTransferCheckpoint 场景 (b) 检查点模式的间隔：远长于用例时长，周期
	// 检查点不会抢在转移前落盘，见 cfGracefulTransfer 判别力前提 2
	cfTransferCheckpoint = time.Hour
)

// cfDelivery 一次投递记录。
type cfDelivery struct {
	key     string // msgID@offset：同一条存储消息（Send 重试可能落成两条同 ID 消息）
	id      string
	attempt int32
	at      time.Time
}

// cfRecorder 全体消费者的投递与成功 ack 记录。
type cfRecorder struct {
	mu       sync.Mutex
	dels     []cfDelivery
	ackedAt  map[string]time.Time // key → 首次 ack 成功时刻
	consumed map[string]bool      // msgID 集合（对账用）
}

func newCFRecorder() *cfRecorder {
	return &cfRecorder{ackedAt: map[string]time.Time{}, consumed: map[string]bool{}}
}

func (r *cfRecorder) deliver(d cfDelivery) {
	r.mu.Lock()
	r.dels = append(r.dels, d)
	r.consumed[d.id] = true
	r.mu.Unlock()
}

func (r *cfRecorder) acked(key string, at time.Time) {
	r.mu.Lock()
	if _, ok := r.ackedAt[key]; !ok {
		r.ackedAt[key] = at
	}
	r.mu.Unlock()
}

// snapshotConsumed 消费集拷贝。
func (r *cfRecorder) snapshotConsumed() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]bool, len(r.consumed))
	for k := range r.consumed {
		out[k] = true
	}
	return out
}

// rate [from,to) 内的投递速率（条/秒）。
func (r *cfRecorder) rate(from, to time.Time) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, d := range r.dels {
		if !d.at.Before(from) && d.at.Before(to) {
			n++
		}
	}
	return float64(n) / to.Sub(from).Seconds()
}

// dups 两类检查点丢失型重复的条数与样例。
func (r *cfRecorder) dups() (afterAck, attempt1 int, samples []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	first1 := map[string]bool{}
	for _, d := range r.dels {
		if at, ok := r.ackedAt[d.key]; ok && d.at.After(at) {
			afterAck++
			if len(samples) < 6 {
				samples = append(samples, fmt.Sprintf("ack 后重投 %s attempt=%d 距 ack=%v", d.key, d.attempt, d.at.Sub(at).Round(time.Millisecond)))
			}
		}
		if d.attempt == 1 {
			if first1[d.key] {
				attempt1++
				if len(samples) < 6 {
					samples = append(samples, fmt.Sprintf("首投重复 %s", d.key))
				}
			}
			first1[d.key] = true
		}
	}
	return afterAck, attempt1, samples
}

// rawAcker 原始 gRPC ack（逐节点尝试，见文件头）。
type rawAcker struct {
	conns        []*grpc.ClientConn
	group, topic string
}

func newRawAcker(t *testing.T, endpoints []string, group, topic string) *rawAcker {
	t.Helper()
	a := &rawAcker{group: group, topic: topic}
	for _, ep := range endpoints {
		c, err := grpc.NewClient(ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("grpc 连接 %s: %v", ep, err)
		}
		a.conns = append(a.conns, c)
	}
	t.Cleanup(func() {
		for _, c := range a.conns {
			c.Close()
		}
	})
	return a
}

// signedCtx 带 MQv2-HMAC-SHA1 签名头（同 tryQueryRoute）。
func signedCtx(ctx context.Context) context.Context {
	now := time.Now().Format("20060102T150405Z")
	h := hmac.New(sha1.New, []byte(clusterSK))
	h.Write([]byte(now))
	auth := fmt.Sprintf("MQv2-HMAC-SHA1 Credential=%s//Rocketmq, SignedHeaders=x-mq-date-time, Signature=%s",
		clusterAK, hex.EncodeToString(h.Sum(nil)))
	return metadata.AppendToOutgoingContext(ctx, "x-mq-date-time", now, "authorization", auth)
}

// ack 返回 true = 某节点回 OK（确认生效）；false = 终局未确认或 30s 内未成功。
func (a *rawAcker) ack(mv *rmq.MessageView, start int) bool {
	deadline := time.Now().Add(cfAckWindow)
	req := &sdkpb.AckMessageRequest{
		Group: &sdkpb.Resource{Name: a.group}, Topic: &sdkpb.Resource{Name: a.topic},
		Entries: []*sdkpb.AckMessageEntry{{MessageId: mv.GetMessageId(), ReceiptHandle: mv.GetReceiptHandle()}},
	}
	for i := start; time.Now().Before(deadline); i++ {
		c := sdkpb.NewMessagingServiceClient(a.conns[i%len(a.conns)])
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := c.AckMessage(signedCtx(ctx), req)
		cancel()
		if err == nil && len(resp.GetEntries()) == 1 {
			switch resp.GetEntries()[0].GetStatus().GetCode() {
			case sdkpb.Code_OK:
				return true
			case sdkpb.Code_INVALID_RECEIPT_HANDLE:
				return false
			}
		}
		if i%len(a.conns) == len(a.conns)-1 {
			time.Sleep(200 * time.Millisecond) // 一圈都不行：选举/摊布窗口，退避
		}
	}
	return false
}

// runCFConsumers 起 cfConsumers 个 SimpleConsumer 持续收 + 原始 ack，直到 stop
// 关闭；协程登记到 wg。endpoint 是 SDK 的接入地址串（「;」分隔），SDK 启动时
// 从它取一次路由、此后每 30s 刷新；ack 一律经全部节点的原始 gRPC。
func runCFConsumers(t *testing.T, pc *procCluster, endpoint, group, topic string, rec *cfRecorder, stop <-chan struct{}, wg *sync.WaitGroup) {
	t.Helper()
	acker := newRawAcker(t, pc.endpoints(), group, topic)
	for w := 0; w < cfConsumers; w++ {
		c := newClusterConsumer(t, endpoint, group, topic, clusterConsumerAwaitDefault)
		wg.Add(1)
		go func(w int, c rmq.SimpleConsumer) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				mvs, err := c.Receive(context.Background(), 16, cfInvisible)
				if err != nil {
					time.Sleep(50 * time.Millisecond) // 空轮询 / 路由陈旧：退避（同 recvAllAck）
					continue
				}
				now := time.Now()
				for _, mv := range mvs {
					rec.deliver(cfDelivery{key: fmt.Sprintf("%s@%d", mv.GetMessageId(), mv.GetOffset()),
						id: mv.GetMessageId(), attempt: mv.GetDeliveryAttempt(), at: now})
				}
				for _, mv := range mvs {
					if acker.ack(mv, w) {
						rec.acked(fmt.Sprintf("%s@%d", mv.GetMessageId(), mv.GetOffset()), time.Now())
					}
				}
			}
		}(w, c)
	}
}

// drainUntil 等消费集覆盖确认集且时刻不早于 minEnd（或 180s 超时），返回消费集。
func drainUntil(t *testing.T, cs *confirmedSet, rec *cfRecorder, minEnd time.Time) map[string]bool {
	t.Helper()
	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		got := rec.snapshotConsumed()
		missing := 0
		cs.mu.Lock()
		for id := range cs.ids {
			if !got[id] {
				missing++
			}
		}
		cs.mu.Unlock()
		if missing == 0 && !time.Now().Before(minEnd) {
			return got
		}
		time.Sleep(500 * time.Millisecond)
	}
	return rec.snapshotConsumed()
}

// cfMode 一种持久化模式。
type cfMode struct {
	name     string
	interval time.Duration // 0 = 逐次复制
}

var cfModes = []cfMode{{"checkpoint", time.Second}, {"perop", 0}}

func (m cfMode) mutate(c *config.Config) {
	c.ConsumeCheckpointInterval = m.interval.String()
	if m.interval == 0 {
		c.ConsumeCheckpointInterval = "0"
	}
	c.LogLevel = "info"
}

// cfSetup 起集群、建 topic、等摊布收敛，启动持续生产与消费。
type cfRun struct {
	pc       *procCluster
	cs       *confirmedSet
	rec      *cfRecorder
	stopProd chan struct{}
	stopCons chan struct{}
	prodWG   sync.WaitGroup
	consWG   sync.WaitGroup
}

func cfSetup(t *testing.T, m cfMode, topic, group string) *cfRun {
	t.Helper()
	r := &cfRun{cs: newConfirmedSet(), rec: newCFRecorder(),
		stopProd: make(chan struct{}), stopCons: make(chan struct{})}
	r.pc = startProcCluster(t, 3, m.mutate)
	ensureTopic(t, r.pc.endpoints(), topic)
	waitRouteSpread(t, r.pc.endpoints(), topic, 60*time.Second)
	producer := newClusterProducer(t, r.pc.multi(), topic)
	r.prodWG.Add(1)
	go func() { defer r.prodWG.Done(); produceUntil(t, producer, topic, r.cs, r.stopProd) }()
	runCFConsumers(t, r.pc, r.pc.multi(), group, topic, r.rec, r.stopCons, &r.consWG)
	t.Cleanup(func() { r.stop() })
	return r
}

func (r *cfRun) stop() {
	select {
	case <-r.stopProd:
	default:
		close(r.stopProd)
	}
	r.prodWG.Wait()
	select {
	case <-r.stopCons:
	default:
		close(r.stopCons)
	}
	r.consWG.Wait()
}

// waitRouteAvoids 轮询 endpoint 上的 topic 路由，直到没有任何队列指向 avoid
// （停机节点领导的组已在存活节点选出新 leader），超时 Fatal。
func waitRouteAvoids(t *testing.T, endpoint, topic, avoid string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		qs, err := tryQueryRoute(endpoint, topic)
		if err == nil && len(qs) > 0 && routeEndpointCounts(qs)[avoid] == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%v 内 %s 上 topic %s 的路由仍指向已停节点 %s（err=%v）", timeout, endpoint, topic, avoid, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (r *cfRun) victim(t *testing.T, topic string) int {
	t.Helper()
	ep := leaderOfMostQueues(routeEndpointCounts(queryRoute(t, r.pc.endpointOf(0), topic)))
	v := r.pc.indexOfEndpoint(ep)
	if v < 0 {
		t.Fatalf("路由里的 leader 地址 %s 不在集群节点列表中", ep)
	}
	return v
}

func (r *cfRun) logs(skip int) []string {
	var out []string
	for i, d := range r.pc.dirs {
		if i != skip {
			out = append(out, filepath.Join(d, "broker.log"))
		}
	}
	return out
}

// TestConsumeFailover S2/S3：{检查点, 逐次} × {kill9, graceful+transfer}。
func TestConsumeFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("故障注入场景耗时，-short 跳过")
	}
	for _, m := range cfModes {
		m := m
		t.Run(m.name+"/kill9", func(t *testing.T) { cfKill9(t, m) })
		t.Run(m.name+"/graceful+transfer", func(t *testing.T) { cfGracefulTransfer(t, m) })
	}
}

// cfKill9 场景 (a)：kill -9 承载队列最多的节点 → 35s → 重启 → 排空。
func cfKill9(t *testing.T, m cfMode) {
	topic, group := "cf-kill-"+m.name, "cf-kill-g-"+m.name
	r := cfSetup(t, m, topic, group)
	time.Sleep(8 * time.Second) // 健康期：消费进入稳态
	v := r.victim(t, topic)
	killAt := time.Now()
	rate := r.rec.rate(killAt.Add(-cfRateWindow), killAt)
	t.Logf("kill -9 节点 %d，kill 前 %v 投递速率 R=%.1f 条/s，确认集 %d", v+1, cfRateWindow, rate, r.cs.size())
	r.pc.kill(t, v)
	time.Sleep(35 * time.Second)
	r.pc.restart(t, v)
	time.Sleep(10 * time.Second)
	close(r.stopProd)
	r.prodWG.Wait()
	// 排空：至少到 kill 后 cfInvisible+15s，让「ack 已成功但检查点丢失」的
	// inflight 过期重投显形（否则 (i) 类被低估）
	got := drainUntil(t, r.cs, r.rec, killAt.Add(cfInvisible+15*time.Second))
	r.stop()
	after, att1, samples := r.rec.dups()
	total := after + att1
	bound := 2 * rate * (m.interval + time.Second).Seconds()
	t.Logf("kill9/%s：确认集 %d 消费集 %d 投递 %d；检查点丢失型重复 ack后重投=%d 首投重复=%d 合计=%d；R=%.1f I=%v 上界=%.0f；样例 %v",
		m.name, r.cs.size(), len(got), len(r.rec.dels), after, att1, total, rate, m.interval, bound, samples)
	r.cs.assertAllConsumed(t, got)
	if m.interval == 0 {
		if total != 0 {
			t.Fatalf("逐次复制模式 kill -9 后检查点丢失型重复应为 0，得到 %d", total)
		}
	} else if float64(total) > bound {
		t.Fatalf("检查点模式 kill -9 后重复 %d 超过上界 2×R×(I+1s)=%.0f", total, bound)
	}
}

// cfGracefulTransfer 场景 (c)+(b)：SIGTERM 承载队列最多的节点 → 等其组在
// 存活节点选出新 leader → 接管期消费 → 重启；之后等摊布把 preferred 组转回
// （计划内转移）→ 排空。两段合计重复必须为 0，且转移前回调必须确实落过盘
// （防空过）。
//
// 判别力的两个前提（审查 M-2：曾把 main.go 的转移前回调改成 return nil，
// 旧版本 3/3 照样零重复）：
//
//  1. 被转移组在存活节点上必须真有消费流量。官方 Go SDK 只在启动时取一次
//     路由、此后每 30s 刷新（client.go startUp 的 ticker.Tick(f, 30s)；仅把
//     停机拉长到 40s 等刷新也不可靠：实测一轮变异下只出 2 条重复，推断刷新
//     失败时 SDK 保留旧路由，未钉死）。旧版本停机只等
//     10s：整个停机窗口里 SDK 路由仍把该组队列指向已停节点，存活 leader 从
//     未装载该组消费状态，转移时没有可丢的内存态——有没有回调结果都一样。
//     现做法：路由显示该组已由存活节点领导后，另起一批只接入存活节点的
//     「接管期消费者」（启动即取新路由），保证接管期内该组在存活 leader 上
//     有取件与确认。
//  2. 转移那一刻该组必须有未落盘的变更。检查点模式在本场景把间隔抬到
//     cfTransferCheckpoint（远长于用例时长）：周期检查点不会抢先落盘，存活
//     leader 任期内的全部取件/确认只能靠转移前回调落盘——缺了它，新 leader
//     从盘装载到停机时的位点，已确认的消息以 attempt=1 再投、ack 后重投
//     同时显形。抬间隔不放宽语义：「计划内切换零重复」对任何间隔都应成立。
func cfGracefulTransfer(t *testing.T, m cfMode) {
	topic, group := "cf-gt-"+m.name, "cf-gt-g-"+m.name
	if m.interval > 0 {
		m.interval = cfTransferCheckpoint
	}
	r := cfSetup(t, m, topic, group)
	time.Sleep(8 * time.Second)
	v := r.victim(t, topic)
	victimEP := r.pc.endpointOf(v)
	t.Logf("SIGTERM 节点 %d，确认集 %d（检查点间隔 %v）", v+1, r.cs.size(), m.interval)
	r.pc.stopGraceful(t, v)
	stoppedAt := time.Now()
	var live []string
	for i, ep := range r.pc.endpoints() {
		if i != v {
			live = append(live, ep)
		}
	}
	waitRouteAvoids(t, live[0], topic, victimEP, 30*time.Second)
	runCFConsumers(t, r.pc, strings.Join(live, ";"), group, topic, r.rec, r.stopCons, &r.consWG)
	t.Logf("停机后 %v 路由已避开节点 %d，接管期消费者已起（接入 %v）",
		time.Since(stoppedAt).Round(time.Second), v+1, live)
	time.Sleep(cfTakeover)
	survivors := r.logs(v)
	before := countLogLines(t, survivors, "摊布：组领导权转移")
	r.pc.restart(t, v)
	restartAt := time.Now()
	// (b) 转移已发生的证据：存活节点日志在重启之后新增「摊布：组领导权转移」
	deadline := restartAt.Add(60 * time.Second)
	moved := 0
	for time.Now().Before(deadline) {
		if moved = countLogLines(t, survivors, "摊布：组领导权转移") - before; moved > 0 {
			break
		}
		time.Sleep(time.Second)
	}
	if moved == 0 {
		t.Fatalf("重启后 60s 内存活节点未发起任何摊布转移（(b) 未被覆盖，防空过）")
	}
	movedAt := time.Now()
	t.Logf("重启后 %v 摊布转移 %d 次", movedAt.Sub(restartAt).Round(time.Second), moved)
	time.Sleep(15 * time.Second) // 转移后继续生产消费一段
	close(r.stopProd)
	r.prodWG.Wait()
	// 排空到转移后 cfInvisible+15s：转移丢掉的 ack 在原取件的不可见期满后才
	// 重投（≤ 转移时刻 + cfInvisible），早停会漏计
	got := drainUntil(t, r.cs, r.rec, movedAt.Add(cfInvisible+15*time.Second))
	r.stop()
	after, att1, samples := r.rec.dups()
	prepared, preparedQueues := countPrepared(t, survivors, restartAt)
	t.Logf("graceful+transfer/%s：确认集 %d 消费集 %d 投递 %d；检查点丢失型重复 ack后重投=%d 首投重复=%d；"+
		"重启后转移前检查点完成 %d 次、落盘队列 %d；样例 %v",
		m.name, r.cs.size(), len(got), len(r.rec.dels), after, att1, prepared, preparedQueues, samples)
	r.cs.assertAllConsumed(t, got)
	// 转移前回调确实跑过（a）；检查点模式下还须确实落过该组的未落盘变更（b 的
	// 前提成立的证据，否则零重复可能只是「无可丢」）。用 Errorf 不中断，让下面
	// 的零重复断言照样给出读数。
	if prepared == 0 {
		t.Errorf("重启后存活节点没有任何「转移前检查点完成」日志：转移前回调未执行")
	}
	if m.interval > 0 && preparedQueues == 0 {
		t.Errorf("转移前检查点没有落盘任何队列：被转移组在存活节点上无未落盘变更，本用例无判别力")
	}
	if after+att1 != 0 {
		t.Fatalf("计划内切换（优雅停机 + 摊布转移）检查点丢失型重复应为 0，得到 %d（ack后重投=%d 首投重复=%d）",
			after+att1, after, att1)
	}
}

// countPrepared 统计 since 之后各日志里「转移前检查点完成」的条数与其 queues
// 字段之和（JSON 行日志；解析失败的行只计条数并记一笔）。
func countPrepared(t *testing.T, logPaths []string, since time.Time) (lines, queues int) {
	t.Helper()
	for _, p := range logPaths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Logf("读取 broker 日志 %s 失败: %v", p, err)
			continue
		}
		for _, ln := range strings.Split(string(raw), "\n") {
			if !strings.Contains(ln, "转移前检查点完成") {
				continue
			}
			var rec struct {
				Time   time.Time `json:"time"`
				Queues int       `json:"queues"`
			}
			if err := json.Unmarshal([]byte(ln), &rec); err != nil {
				t.Logf("解析转移前检查点日志行失败（%v）：%s", err, ln)
				lines++
				continue
			}
			if rec.Time.Before(since) {
				continue
			}
			lines++
			queues += rec.Queues
		}
	}
	return lines, queues
}
