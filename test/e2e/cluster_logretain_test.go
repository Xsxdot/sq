//go:build e2e

// cluster_logretain_test.go：卡 SQ4.10 S1 的进程级故事检查——持续写入下
// 停一个节点数分钟再启动，它经**日志**追平，不触发快照。
//
// 职责：
//   - 三进程集群（真二进制、真配置），把截断保留量调到远小于停机期间的
//     落后量，迫使 leader 的保留下界只能靠「死 peer 宽限期」维持
//   - 以 broker 日志判定快照是否发生（仓内无快照指标，见 plan R13）、以
//     /admin/cluster 的复制进度判定追平、以 SDK 对账判定不丢消息
//
// 边界：
//   - 不测宽限期超期 / 磁盘告急（包级测试 TestTruncate(PeerGrace|DiskPressure)）
//   - 不做吞吐判定：写入延迟 p99 只记录（plan §5 升级条件的取证），不断言
package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"

	"github.com/Xsxdot/sq/internal/config"
	"github.com/Xsxdot/sq/internal/replication"
)

// logRetainEntries 本用例的按条数保留量：远小于停机期间每组的落后量，
// 使「按条数」下界在停机后几秒内就失效——只剩宽限期下界钉住截断。
const logRetainEntries = 200

// logRetainProducers 并发发送协程数（每协程按 produceThrottle 节流）。
const logRetainProducers = 4

// logRetainBody 消息体字节数：让每组停机期间的日志量（数千条 × 8KiB ≈
// 数十 MiB）超过按字节下界（64MiB / 4 组 = 16MiB）——否则字节下界一条
// 就把整段日志全留住，「死 peer 宽限期」是否生效用例分辨不出来
// （宽限判定改成「判死即放弃」的变异在默认 1GiB 下实测仍绿）。
const logRetainBody = 8 << 10

// TestScenarioFollowerDownCatchesUpByLog S1：停一个「至少领导一个数据组、
// 且在至少一个数据组是 follower」的节点，持续写入 SQ_E2E_DOWN（缺省 2m）
// 后重启，判据：
//
//	① 重启节点在重启之后无「快照安装开始」
//	② 三节点在停机至判定结束期间无「快照描述符已生成」
//	③ 60s 内各数据组上该节点追平停写时的 commit
//	④ 至少一个节点日志出现冷读 Info 行（窗口外条目确经磁盘日志发出）
//	⑤ 停写后 SDK 对账：确认集全部可消费
func TestScenarioFollowerDownCatchesUpByLog(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	down := 2 * time.Minute
	if v := os.Getenv("SQ_E2E_DOWN"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			t.Fatalf("SQ_E2E_DOWN=%q 非法（须为正 Go duration）", v)
		}
		down = d
	}
	pc := startProcCluster(t, 3, func(c *config.Config) {
		c.Cluster.LogRetainEntries = logRetainEntries
		c.Cluster.TruncateInterval = 5 * time.Second
		// 字节下界取允许的最小值（见 logRetainBody）。基线二进制不认识这个
		// 键（严格解析拒启），跑基线对照（plan E0）时设
		// SQ_E2E_NO_RETAIN_BYTES=1 不写它——基线本无按字节保留，行为不变
		if os.Getenv("SQ_E2E_NO_RETAIN_BYTES") == "" {
			c.Cluster.LogRetainBytes = "64MiB"
		}
	})
	const topic, group = "scn-logretain", "scn-logretain-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	victim := pickMixedRoleNode(t, pc)
	t.Logf("选中节点 %d（至少领导一个数据组、且至少在一个数据组是 follower）", victim+1)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()
	lat := &latencyRec{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for p := 0; p < logRetainProducers; p++ {
		wg.Add(1)
		go func() { defer wg.Done(); produceTimed(producer, topic, cs, lat, stop) }()
	}
	stopWriting := func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		wg.Wait()
	}
	defer stopWriting()

	time.Sleep(10 * time.Second) // 健康期：取停机前的写延迟基线
	if cs.size() == 0 {
		t.Fatal("健康期一条都没发成功，集群未就绪")
	}
	preP99 := lat.p99AndReset()

	before := dataGroupCommits(t, pc, -1)
	stopOffsets := logSizes(t, pc)
	pc.stopGraceful(t, victim)
	t.Logf("节点 %d 已停机，停机时长 %s，持续写入中", victim+1, down)
	time.Sleep(down)
	after := dataGroupCommits(t, pc, victim)
	for g, c := range after {
		if adv := c - before[g]; c < before[g] || adv <= logRetainEntries*3 {
			t.Fatalf("组 %d 停机期间 commit 只前进 %d（%d→%d），≤ 3×保留量 %d：写入量不足，用例无判别力",
				g, int64(c)-int64(before[g]), before[g], c, logRetainEntries*3)
		}
		t.Logf("组 %d 停机期间 commit %d → %d（前进 %d，保留量 %d）", g, before[g], c, c-before[g], logRetainEntries)
	}

	restartOffset := logSizes(t, pc)[victim]
	lat.p99AndReset()
	pc.restart(t, victim)
	time.Sleep(30 * time.Second)
	catchP99 := lat.p99AndReset()
	stopWriting()
	t.Logf("写入延迟 p99：停机前 %s，重启追平期 %s（plan §5：恶化 >5 倍即升级）", preP99, catchP99)

	// ③ 追平：停写时刻的各组 commit（存活多数派视角）是追平目标
	target := dataGroupCommits(t, pc, victim)
	victimID := uint64(victim + 1)
	deadline := time.Now().Add(60 * time.Second)
	var lastSeen map[uint32]uint64
	for {
		lastSeen = victimProgress(t, pc, victimID)
		ok := true
		for g, c := range target {
			if lastSeen[g] < c {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("③ 60s 内节点 %d 未追平：进度 %v，目标 commit %v", victimID, lastSeen, target)
		}
		time.Sleep(time.Second)
	}
	t.Logf("③ 追平：节点 %d 进度 %v ≥ 停写 commit %v", victimID, lastSeen, target)

	// ① 重启节点：重启之后无快照安装
	installs := countSince(t, pc.handles[victim].logPath, restartOffset, "快照安装开始")
	// ② 三节点：停机至今无快照描述符生成
	snaps := 0
	for i := range pc.handles {
		snaps += countSince(t, pc.handles[i].logPath, stopOffsets[i], "快照描述符已生成")
	}
	// ④ 冷读 Info 行（任一节点）
	cold := 0
	var coldSample string
	for i := range pc.handles {
		n, s := grepColdReads(t, pc.handles[i].logPath, stopOffsets[i])
		cold += n
		if coldSample == "" {
			coldSample = s
		}
	}
	t.Logf("① 重启后快照安装开始=%d ② 快照描述符已生成=%d ④ 冷读行=%d 样例=%s", installs, snaps, cold, coldSample)
	if installs != 0 {
		t.Errorf("① 重启节点在重启后发生 %d 次快照安装（应经日志追平）", installs)
	}
	if snaps != 0 {
		t.Errorf("② 停机至判定结束期间生成了 %d 份快照描述符（应为 0）", snaps)
	}
	if cold == 0 {
		t.Errorf("④ 无任何冷读 Info 行：窗口外条目未经磁盘日志发出")
	}
	if t.Failed() {
		return
	}

	// ⑤ 对账：确认集全部可消费（多消费者并行排水，单消费者 ~60 msg/s 排不完）
	got := drainParallel(t, pc, group, topic, cs, 8, 10*time.Minute)
	cs.assertAllConsumed(t, got)
}

// pickMixedRoleNode 找一个至少领导一个数据组、且至少在一个数据组是
// follower 的节点（0-based）；leader 摊布未收敛时轮询等待。
func pickMixedRoleNode(t *testing.T, pc *procCluster) int {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for i := range pc.handles {
			v := clusterView(t, pc, i)
			lead, follow := 0, 0
			for _, g := range v.Groups {
				if g.ID == 0 {
					continue // meta 组不计
				}
				if g.IsLeader {
					lead++
				} else {
					follow++
				}
			}
			if lead > 0 && follow > 0 {
				return i
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("60s 内找不到「既领导又跟随」的节点（leader 摊布未收敛）")
	return -1
}

// clusterView 取第 i 个节点的 /admin/cluster（e2e 配置免登录）。
func clusterView(t *testing.T, pc *procCluster, i int) replication.ClusterView {
	t.Helper()
	var v replication.ClusterView
	adminJSON(t, http.MethodGet, "http://"+pc.cfgs[i].AdminListen+"/admin/cluster", "", nil, http.StatusOK, &v)
	return v
}

// dataGroupCommits 返回各数据组在存活节点视角下的最大 commit（跳过节点 skip）。
func dataGroupCommits(t *testing.T, pc *procCluster, skip int) map[uint32]uint64 {
	t.Helper()
	out := map[uint32]uint64{}
	for i := range pc.handles {
		if i == skip || pc.handles[i].cmd.Process == nil {
			continue
		}
		for _, g := range clusterView(t, pc, i).Groups {
			if g.ID == 0 {
				continue
			}
			if g.Commit > out[g.ID] {
				out[g.ID] = g.Commit
			}
		}
	}
	return out
}

// victimProgress 返回节点 id 在各数据组的追平位点：组 leader 是别人时取
// leader 复制进度里它的 Match；它自己是 leader 时取它自己的 applied。
func victimProgress(t *testing.T, pc *procCluster, id uint64) map[uint32]uint64 {
	t.Helper()
	out := map[uint32]uint64{}
	for i := range pc.handles {
		for _, g := range clusterView(t, pc, i).Groups {
			if g.ID == 0 || !g.IsLeader {
				continue
			}
			if uint64(i+1) == id {
				out[g.ID] = g.Applied
				continue
			}
			for _, p := range g.Peers {
				if p.ID == id {
					out[g.ID] = p.Match
				}
			}
		}
	}
	return out
}

// logSizes 返回各节点 broker.log 当前字节数（之后的内容即「此刻之后」）。
func logSizes(t *testing.T, pc *procCluster) []int64 {
	t.Helper()
	out := make([]int64, len(pc.handles))
	for i, h := range pc.handles {
		fi, err := os.Stat(h.logPath)
		if err != nil {
			t.Fatalf("stat 节点 %d 日志: %v", i+1, err)
		}
		out[i] = fi.Size()
	}
	return out
}

// countSince 统计 path 中 off 字节之后包含 needle 的行数。
func countSince(t *testing.T, path string, off int64, needle string) int {
	n, _ := grepSince(t, path, off, needle)
	return n
}

// grepSince 返回 path 中 off 字节之后包含 needle 的行数与首个命中行。
func grepSince(t *testing.T, path string, off int64, needle string) (int, string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读日志 %s: %v", path, err)
	}
	if off > int64(len(raw)) {
		off = int64(len(raw))
	}
	n, first := 0, ""
	for _, line := range strings.Split(string(raw[off:]), "\n") {
		if strings.Contains(line, needle) {
			n++
			if first == "" {
				first = line
			}
		}
	}
	return n, first
}

// grepColdReads 统计 off 之后「真有冷读」的冷读 Info 行（reads>0；只有
// errors 的行是冷读失败，不算条目经磁盘日志发出）。
func grepColdReads(t *testing.T, path string, off int64) (int, string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读日志 %s: %v", path, err)
	}
	if off > int64(len(raw)) {
		off = int64(len(raw))
	}
	n, first := 0, ""
	for _, line := range strings.Split(string(raw[off:]), "\n") {
		if strings.Contains(line, "冷读：") && !strings.Contains(line, `"reads":0,`) {
			n++
			if first == "" {
				first = line
			}
		}
	}
	return n, first
}

// latencyRec 记录 Send 成功的耗时，按窗口取 p99。
type latencyRec struct {
	mu sync.Mutex
	ds []time.Duration
}

func (l *latencyRec) add(d time.Duration) { l.mu.Lock(); l.ds = append(l.ds, d); l.mu.Unlock() }

// p99AndReset 返回当前窗口的 p99 并清空窗口。
func (l *latencyRec) p99AndReset() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	ds := l.ds
	l.ds = nil
	if len(ds) == 0 {
		return 0
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds[len(ds)*99/100]
}

// produceTimed 与 produceUntil 同语义（成功登记确认集、按 produceThrottle
// 节流），另记每条成功 Send 的耗时。
func produceTimed(p rmq.Producer, topic string, cs *confirmedSet, lat *latencyRec, stop <-chan struct{}) {
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		start := time.Now()
		body := make([]byte, logRetainBody)
		copy(body, fmt.Sprintf("logretain #%d", i))
		recs, err := p.Send(ctx, &rmq.Message{Topic: topic, Body: body})
		cancel()
		if err == nil && len(recs) > 0 {
			lat.add(time.Since(start))
			cs.confirm(recs[0].MessageID)
		}
		select {
		case <-stop:
			return
		case <-time.After(produceThrottle):
		}
	}
}

// drainParallel 用 n 个同组 SimpleConsumer 并行排水，直到确认集全部出现
// 在消费集里或超时；返回消费集（重复允许）。
func drainParallel(t *testing.T, pc *procCluster, group, topic string, cs *confirmedSet, n int, window time.Duration) map[string]bool {
	t.Helper()
	cs.mu.Lock()
	pending := make(map[string]bool, len(cs.ids))
	for id := range cs.ids {
		pending[id] = true
	}
	cs.mu.Unlock()
	want := len(pending)
	var mu sync.Mutex
	got := map[string]bool{}
	done := make(chan struct{})
	var once sync.Once
	deadline := time.Now().Add(window)
	var wg sync.WaitGroup
	for k := 0; k < n; k++ {
		c := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				select {
				case <-done:
					return
				default:
				}
				mvs, err := c.Receive(context.Background(), 16, 30*time.Second)
				if err != nil {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				for _, mv := range mvs {
					_ = c.Ack(context.Background(), mv)
					mu.Lock()
					got[mv.GetMessageId()] = true
					delete(pending, mv.GetMessageId())
					if len(pending) == 0 {
						once.Do(func() { close(done) })
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("并行排水：%d 个消费者收齐 %d 条（want %d）", n, len(got), want)
	return got
}
