//go:build e2e

// cluster_scenario_test.go：B8.3 四类集群场景测试。
//
// 职责：
//   - kill -9 leader、少数派不可写与愈合、滚动重启、断电模拟四类场景
//   - 统一的不变量对账：确认集（Send 成功即登记）必须被全量消费到
//
// 边界：
//   - 不测吞吐（那是 sdk_cluster_bench_test.go）
//   - 不测协议细节（那是 sdk_cluster_test.go）
//   - 允许重复投递、不允许丢失：at-least-once 是本系统的投递语义，
//     对账器只断言「确认集 ⊆ 消费集」，从不断言两者相等
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"

	"github.com/xushixin/sq/internal/config"
)

// confirmedSet 确认集对账器：producer 每次 Send 成功即登记 msgID，
// 收尾时断言全部 msgID 都被消费到。
//
// 为什么只断言「确认集 ⊆ 消费集」：投递语义是 at-least-once，故障切换
// 期间的重复是设计允许的；而**已确认的消息丢失**是不可接受的红线。
// Send 失败的消息不进确认集——它们的去留本就未定，纳入对账等于把
// 「不确定」当成「必须存在」。
type confirmedSet struct {
	mu  sync.Mutex
	ids map[string]bool
}

func newConfirmedSet() *confirmedSet { return &confirmedSet{ids: map[string]bool{}} }

// confirm 登记一条已被 broker 确认的消息 id。
func (c *confirmedSet) confirm(id string) {
	c.mu.Lock()
	c.ids[id] = true
	c.mu.Unlock()
}

// size 返回确认集大小。
func (c *confirmedSet) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ids)
}

// assertAllConsumed 断言确认集全部出现在消费集里；缺失即失败，并打印
// 前若干条缺失 id 供排查。
func (c *confirmedSet) assertAllConsumed(t *testing.T, got map[string]bool) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	missing := make([]string, 0, 8)
	for id := range c.ids {
		if !got[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		show := missing
		if len(show) > 8 {
			show = show[:8]
		}
		t.Fatalf("确认集有 %d/%d 条未被消费到（已确认消息丢失是红线）：%v",
			len(missing), len(c.ids), show)
	}
	t.Logf("对账通过：确认 %d 条，全部被消费到（消费集 %d 条，重复允许）",
		len(c.ids), len(got))
}

// produceThrottle 是场景用例的发送节流：每条 Send 之间等这么久。
//
// 为什么要节流：SDK 单消费者对积压的排水速率实测仅约 60 msg/s（一次
// Receive 只轮询一个队列、每轮至多 16 条），而全速发送在 quorum-mem 档
// 下可达 2000+ msg/s——健康期+故障期几十秒的确认集能冲到数万条，对账
// 阶段根本排不完（180s 只够排 1 万条量级）。节流到 ~66 msg/s 后，确认
// 集总量落在对账窗口可排的范围内，同时「持续发送期间杀节点」的故障
// 注入语义不受影响（写是连续的，只是不快）。
var produceThrottle = 15 * time.Millisecond

// produceUntil 持续发送直到 stop 关闭；每条成功即登记进确认集。
//
// 返回：
//   - sent: 成功条数（= 进入确认集的条数）
//   - failed: 失败条数（故障窗口内的失败是预期的，不进确认集）
//
// 注意：单条 Send 的超时压到 3s——故障窗口内 SDK 会换节点重试，默认
// 超时会让整个场景用例卡在一条消息上。发送按 produceThrottle 节流
// （见该变量注释）。
func produceUntil(t *testing.T, p rmq.Producer, topic string, cs *confirmedSet, stop <-chan struct{}) (sent, failed int) {
	t.Helper()
	for i := 0; ; i++ {
		select {
		case <-stop:
			return sent, failed
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		recs, err := p.Send(ctx, &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("scenario #%d", i))})
		cancel()
		if err != nil || len(recs) == 0 {
			failed++
		} else {
			cs.confirm(recs[0].MessageID)
			sent++
		}
		select {
		case <-stop:
			return sent, failed
		case <-time.After(produceThrottle):
		}
	}
}

// TestScenarioKillLeaderHard 场景一：持续发送期间 kill -9 掉承载队列
// 最多的那个节点（数据组 leader），故障期间允许 Send 失败，恢复后重启
// 该节点，最终对账——确认集必须被全量消费到。
func TestScenarioKillLeaderHard(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-kill-leader", "scn-kill-leader-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()

	// 阶段①：健康期发一段，确保故障前已有确认集
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var sent, failed int
	wg.Add(1)
	go func() { defer wg.Done(); sent, failed = produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(3 * time.Second)
	if cs.size() == 0 {
		close(stop)
		wg.Wait()
		t.Fatal("健康期 3s 内一条都没发成功，集群未就绪")
	}
	before := cs.size()

	// 阶段②：kill -9 承载队列最多的节点（数据组 leader）
	victimEP := leaderOfMostQueues(routeEndpointCounts(queryRoute(t, pc.endpointOf(0), topic)))
	victim := pc.indexOfEndpoint(victimEP)
	if victim < 0 {
		close(stop)
		wg.Wait()
		t.Fatalf("路由里的 leader 地址 %s 不在集群节点列表中", victimEP)
	}
	t.Logf("kill -9 节点 %d（%s），故障前确认集 %d 条", victim+1, victimEP, before)
	pc.kill(t, victim)

	// 阶段③：故障期继续发 35s（覆盖 SDK 路由缓存刷新节拍 30s + 选举余量）。
	// 这段窗口内**旧 producer** 的恢复点不定（实测 kill 后 13s~90s+ 都有，
	// 那是 SDK 客户端路由缓存的恢复行为，不是 broker 故障）——「集群恢复」
	// 不在旧 producer 身上断言，交给阶段④的全新 producer 验证（fresh
	// QueryRoute，把客户端缓存与集群事实解耦）。
	time.Sleep(35 * time.Second)

	// 阶段④：验证集群恢复可写——全新 producer 连一个存活节点（fresh
	// QueryRoute 到当前 leader），发送带重试（选举/摊布收敛前可能短暂
	// 失败）。这一步才是「failover 真的收敛」的确定性观测。
	fresh := newClusterProducer(t, pc.endpointOf((victim+1)%3), topic)
	for i := 0; i < 50; i++ {
		recs, err := fresh.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("post-failover #%d", i))})
		if err != nil {
			for retry := 0; retry < 12 && err != nil; retry++ {
				time.Sleep(5 * time.Second)
				recs, err = fresh.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte(fmt.Sprintf("post-failover #%d", i))})
			}
		}
		if err != nil || len(recs) == 0 {
			fresh.GracefulStop()
			close(stop)
			wg.Wait()
			t.Fatalf("failover 后 60s 内集群仍不可写（第 %d 条）: %v", i, err)
		}
		cs.confirm(recs[0].MessageID)
	}
	fresh.GracefulStop()
	t.Logf("failover 后全新 producer 写入 50 条（集群恢复可写，确认集 %d 条）", cs.size())

	// 阶段⑤：重启被 kill 的节点（走 ErrUncleanShutdown → Rejoin 自愈）
	pc.restart(t, victim)
	time.Sleep(10 * time.Second)
	close(stop)
	wg.Wait()
	t.Logf("发送收尾：成功 %d 失败 %d（故障窗口内失败是预期的）", sent, failed)

	// 阶段⑥：对账——确认集必须被全量消费到
	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "kill-leader 对账", false)
	cs.assertAllConsumed(t, got)
}

// TestScenarioMinorityCannotWrite 场景二：少数派不可写。
//
// 分区用「杀掉 3 之 2」等价模拟：剩余节点失去 quorum，raft 视角下与
// 「剩余节点被隔离到少数派」完全一致（见 cluster_proc_test.go 文件头）。
//
// 红线：少数派期间**绝不允许出现 Send 成功**——多数派确认是写入语义的
// 根，少数派上的假成功意味着数据可能在愈合后被截断，比写失败危险得多。
func TestScenarioMinorityCannotWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-minority", "scn-minority-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	// 阶段①：健康期建确认集（只连节点 0——它是本用例的「幸存者」，
	// 后续要单独观察它在少数派下的行为）
	survivor := 0
	producer := newClusterProducer(t, pc.endpointOf(survivor), topic)
	cs := newConfirmedSet()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()
	healthy := cs.size()
	if healthy == 0 {
		t.Fatal("健康期一条都没发成功，集群未就绪")
	}
	t.Logf("健康期确认 %d 条", healthy)

	// 阶段②：杀掉另外两个节点 → 幸存者进入少数派
	for i := range pc.handles {
		if i != survivor {
			pc.kill(t, i)
		}
	}
	if pc.aliveCount() != 1 {
		t.Fatalf("应只剩 1 个存活节点，实为 %d", pc.aliveCount())
	}

	// 阶段③：少数派期间发送——一条都不许成功
	minorityCS := newConfirmedSet()
	mstop := make(chan struct{})
	wg.Add(1)
	var msent, mfailed int
	go func() { defer wg.Done(); msent, mfailed = produceUntil(t, producer, topic, minorityCS, mstop) }()
	time.Sleep(20 * time.Second)
	close(mstop)
	wg.Wait()
	if msent != 0 {
		t.Fatalf("少数派期间有 %d 条 Send 成功（红线：多数派确认是写入语义的根，"+
			"少数派假成功意味着数据可能在愈合后被截断）", msent)
	}
	t.Logf("少数派期间 %d 次发送全部失败（符合预期）", mfailed)
	// 防空过断言：producer goroutine 若没跑起来（比如健康期就已挂），
	// 少数派期间的「全部失败」会变成空转的全绿——必须真的尝试过发送
	if mfailed == 0 {
		t.Fatal("少数派期间一次发送都没尝试，用例没测到东西")
	}

	// —— 愈合段已删除 ——
	// 删除的真实原因（实测发现，P0）：2-of-3 硬宕后，被 kill 的节点经
	// Rejoin（第 2 步 WipeForRejoin 清空数据目录 → 第 3 步 PrepareJoin 找
	// leader）重入，需要幸存 leader 提交 Remove→AddLearner 成员变更——而
	// raft 成员变更要 quorum，幸存单节点（3 之 1）给不出，重入永远等不到
	// leader 放行，集群无法自愈。这是当前架构的已知 P0 洞（不干净关机自愈
	// 会在确认可恢复前销毁数据，见 backlog），batch⑤ 不改生产代码；本用例
	// 因此只验证「少数派不可写」红线。将来 wipe 顺序修复（先确认 leader
	// 可达并愿意接纳、再 wipe）后，愈合可在此补回绿路径。
}

// TestScenarioRollingRestart 场景三：持续发送期间逐个优雅重启全部节点，
// 任意时刻只停一个（quorum 始终保持）。
//
// 与场景一的区别：优雅停机会写干净关机标记，重启走的是「干净恢复」
// 路径而不是 Rejoin 自愈——这是运维日常（升级、改配置）真正走的那条
// 路径，必须单独有证据，不能靠 kill -9 的用例代表它。
//
// 红线：全程 quorum 未失，因此**确认集必须持续增长**——每一轮重启后
// 都要比上一轮多。任何一轮不增长都说明滚动重启期间存在写停摆窗口。
func TestScenarioRollingRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-rolling", "scn-rolling-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var sent, failed int
	wg.Add(1)
	go func() { defer wg.Done(); sent, failed = produceUntil(t, producer, topic, cs, stop) }()

	time.Sleep(5 * time.Second)
	prev := cs.size()
	if prev == 0 {
		close(stop)
		wg.Wait()
		t.Fatal("健康期一条都没发成功，集群未就绪")
	}

	for i := range pc.handles {
		t.Logf("滚动重启第 %d 轮：优雅停节点 %d（确认集 %d 条）", i+1, i+1, prev)
		pc.stopGraceful(t, i)
		// 停机窗口留 10s：这段时间 quorum 仍在（3 之 2），写必须继续成功
		//（SDK 路由缓存会短暂指向被停节点导致局部失败，恢复点在路由刷新后）
		time.Sleep(10 * time.Second)
		pc.restart(t, i)
		// 重启后等确认集增长：干净恢复 + SDK 路由刷新耗时不定，轮询等
		//（最多 90s）——增长即证明该轮没有写停摆窗口
		roundDeadline := time.Now().Add(90 * time.Second)
		for cs.size() <= prev && time.Now().Before(roundDeadline) {
			time.Sleep(2 * time.Second)
		}
		now := cs.size()
		if now <= prev {
			close(stop)
			wg.Wait()
			t.Fatalf("滚动重启第 %d 轮期间确认集未增长（%d → %d）："+
				"quorum 全程未失，写不该有停摆窗口", i+1, prev, now)
		}
		t.Logf("第 %d 轮完成，确认集 %d → %d", i+1, prev, now)
		prev = now
	}

	close(stop)
	wg.Wait()
	t.Logf("滚动重启收尾：成功 %d 失败 %d", sent, failed)

	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "rolling-restart 对账", false)
	cs.assertAllConsumed(t, got)
}

// TestScenarioFullProcessCrashRecoversLocally 三节点同时 SIGKILL 后重启，
// 断言集群**自愈**：三节点各自以原身份从本地日志恢复、重新选出 leader、
// 恢复可写，且崩溃前的确认集一条不丢。
//
// 这条用例取代了旧的 TestScenarioFullPowerLossCannotRecover。旧用例把
// 「三节点全崩 = 集群永久需要人工介入」固化成了期望行为，而那个行为的
// 前提是错的：SIGKILL 杀的是进程，机器没重启，页缓存还在，三份日志基本
// 完好——它们只是被一条过度保守的规则拦在门外（B11）。
//
// 真掉电（页缓存丢失）的形态由 TestScenarioRebootedMemNeedsPermit 与
// TestScenarioRebootedMemRecoversAfterGrant 覆盖，B10 的顺序红线断言在
// 前者中继续生效。
//
// **kill 前必须静置**：Pebble 的 NoSync 提交返回时，数据可能还在进程内的
// WAL 缓冲里（flusher goroutine 异步写出），所以紧贴 SIGKILL 发出的那一批
// 已确认消息本来就会丢——断言它们不丢等于断言系统从来不具备的性质，用例
// 会真红且红得没有意义。静置 500ms（> flusher() 的 200ms 周期 + 余量）之后
// 全部已确认写入都已随周期 fsync 落盘，零丢失才是可断言的。
func TestScenarioFullProcessCrashRecoversLocally(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-full-crash", "scn-full-crash-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()
	// 阶段①：健康期发一批确认集（发送节流与对账语义同其余场景用例）
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var sent, failed int
	wg.Add(1)
	go func() { defer wg.Done(); sent, failed = produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(3 * time.Second)
	if cs.size() == 0 {
		close(stop)
		wg.Wait()
		t.Fatal("健康期 3s 内一条都没发成功，集群未就绪")
	}
	close(stop)
	wg.Wait()
	producer.GracefulStop()
	t.Logf("阶段①健康期：成功 %d 失败 %d，确认集 %d 条", sent, failed, cs.size())

	// 让 200ms 周期 fsync 至少跑满一轮，把上面这批确认集变成可断言的
	time.Sleep(500 * time.Millisecond)

	beforeKill := dumpQueueOffsets(t, pc, topic, "kill 前")

	// 断电：同时 SIGKILL 全部节点
	for i := range pc.handles {
		pc.kill(t, i)
	}
	// 上电：整批原地重启（先全起进程、再逐个等就绪——逐台 restart 会在
	// 第一个节点上死锁，见 restartAll 注释）
	pc.restartAll(t)

	// 存量断言：静置 ≥500ms 后断电，恢复后每个节点的落库条数必须一条不少。
	// 这条比下面的消息级对账更早失败、也更能指认病灶——它把「存储真丢了」
	// 与「存着但投递不到」分开。B11-OPEN-1（mem 档周期刷盘是空操作）就是
	// 这条数字对比钉死的。
	afterRecover := dumpQueueOffsets(t, pc, topic, "恢复后")
	for i := range afterRecover {
		if afterRecover[i] < beforeKill[i] {
			t.Errorf("节点 %d 断电恢复后落库条数从 %d 掉到 %d，丢了 %d 条已落库消息",
				i+1, beforeKill[i], afterRecover[i], beforeKill[i]-afterRecover[i])
		}
	}

	// 等路由全部可答再发：本地恢复后各组需重新选主，produceUntil 容忍
	// 选举窗口内的发送失败（失败不登记），但恢复可写的证据要等路由就绪
	ensureTopic(t, pc.endpoints(), topic)

	// 阶段②：恢复后继续发一批——集群恢复可写 = 选主成功 = 三份本地日志
	// 都被接纳。用全新 producer（fresh QueryRoute，把客户端路由缓存与集群
	// 事实解耦，同场景一的阶段④）。
	producer2 := newClusterProducer(t, pc.multi(), topic)
	stop2 := make(chan struct{})
	var wg2 sync.WaitGroup
	var sent2, failed2 int
	wg2.Add(1)
	go func() { defer wg2.Done(); sent2, failed2 = produceUntil(t, producer2, topic, cs, stop2) }()
	time.Sleep(3 * time.Second)
	close(stop2)
	wg2.Wait()
	producer2.GracefulStop()
	t.Logf("阶段②恢复后：成功 %d 失败 %d，确认集累计 %d 条", sent2, failed2, cs.size())

	// 对账：崩溃前 + 恢复后的确认集必须全部被消费到（零丢失）
	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "full-crash 对账", false)
	cs.assertAllConsumed(t, got)

	for i := range pc.handles {
		logPath := []string{pc.handles[i].logPath}
		// 世代未变 ⇒ 本地日志完整 ⇒ 必须走 local-resume 而非清空重入
		if n := countLogLines(t, logPath, "local-resume"); n == 0 {
			t.Fatalf("节点 %d 未走本地恢复路径——世代未变时不该再清空数据目录重入", i+1)
		}
		// B10 的顺序红线（搬到 rebooted-mem 用例）在此同样成立：
		// local-resume 路径不该出现「状态目录已清空」
		if n := countLogLines(t, logPath, "状态目录已清空"); n != 0 {
			t.Fatalf("节点 %d 清空了数据目录（%d 次）——进程崩溃而机器未重启时，本地日志是完整的，清空是纯粹的浪费与风险", i+1, n)
		}
	}
}

// TestScenarioRebootedMemNeedsPermit 模拟真掉电：三节点 SIGKILL 后把机器
// 世代改成新值再重启，断言三节点**拒启且数据目录保留**。
//
// 这里保管着 B10 的顺序红线断言（原在 TestScenarioFullPowerLossCannotRecover）：
//   - 日志出现「数据目录保持原样未清空」——拒启原因必须是 PrepareJoin 求
//     不到接纳，而不是别的启动失败
//   - 日志**不**出现「状态目录已清空」——清空绝不能发生在取得接纳之前
//
// 若哪天有人把 Rejoin 的第 2/3 步调回去，本用例会红在这里。
func TestScenarioRebootedMemNeedsPermit(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic = "scn-reboot-mem"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	// 建立一点确认集：健康期发一批（发送节流与对账语义同其余场景用例）
	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()
	producer.GracefulStop()
	t.Logf("健康期确认集 %d 条", cs.size())

	for i := range pc.handles {
		pc.kill(t, i)
	}
	// 世代换新 = 机器重启过 = 页缓存已丢，mem 档下日志尾不可信
	for i := range pc.handles {
		pc.setEnv(i, "SQ_BOOTGEN_OVERRIDE=gen-after-reboot")
	}

	for i := range pc.handles {
		pc.restartUnready(t, i)
		logPath := []string{pc.handles[i].logPath}
		// 拒启走的必须是「求不到接纳」这条路，而不是别的启动失败
		if n := countLogLines(t, logPath, "数据目录保持原样未清空"); n == 0 {
			t.Fatalf("节点 %d 日志里没有「数据目录保持原样未清空」——拒启原因不是 PrepareJoin 求不到接纳，机制对不上", i+1)
		}
		// 顺序红线：清空绝不能发生在取得接纳之前
		if n := countLogLines(t, logPath, "状态目录已清空"); n != 0 {
			t.Fatalf("节点 %d 在没有 leader 可接纳的情况下清空了数据目录（%d 次）——Rejoin 的第 2/3 步顺序被调回去了，这是数据永久全损的口子", i+1, n)
		}
		// 数据实体仍在：WipeForRejoin 是 RemoveAll+重建空目录，没被清就非空
		entries, err := os.ReadDir(pc.cfgs[i].DataDir)
		if err != nil {
			t.Fatalf("节点 %d 数据目录读不出来: %v", i+1, err)
		}
		if len(entries) == 0 {
			t.Fatalf("节点 %d 数据目录被清空了（0 个条目）——掉电前那份数据是集群仅剩的兜底，拒启的前提是它还在", i+1)
		}
		t.Logf("节点 %d：拒启且数据目录保留（%d 个条目）", i+1, len(entries))
	}
}

// TestScenarioRebootedMemRecoversAfterGrant 承接上一条：三节点拒启之后，
// 逐台执行 `sq recover --grant` 签字，断言集群起得来、确认集零丢失。
//
// 逐台签字是刻意的（spec §3.5）：每个节点丢的尾巴不一样长，运维应逐台
// 看到各自的代价；真到全集群硬宕，人本来就要逐台去开机。
func TestScenarioRebootedMemRecoversAfterGrant(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-reboot-grant", "scn-reboot-grant-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()
	producer.GracefulStop()
	t.Logf("健康期确认集 %d 条", cs.size())

	time.Sleep(500 * time.Millisecond) // 理由同上一条用例：等周期 fsync 跑满一轮

	for i := range pc.handles {
		pc.kill(t, i)
	}
	for i := range pc.handles {
		pc.setEnv(i, "SQ_BOOTGEN_OVERRIDE=gen-after-reboot")
	}
	// 逐台签字：环境变量要一并带上，否则命令读到的是真实世代、
	// 写出来的许可绑错了世代，启动时不会被消费
	for i := range pc.handles {
		cmd := exec.Command(brokerBinary, "recover", "-config", pc.cfgPaths[i], "--grant")
		cmd.Env = append(os.Environ(), "SQ_BOOTGEN_OVERRIDE=gen-after-reboot")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("节点 %d 签字失败: %v\n%s", i+1, err, out)
		}
		t.Logf("节点 %d 签字输出：\n%s", i+1, out)
	}
	pc.restartAll(t)
	// 等路由全部可答再发（同 FullProcessCrash 用例的注释）
	ensureTopic(t, pc.endpoints(), topic)

	producer2 := newClusterProducer(t, pc.multi(), topic)
	stop2 := make(chan struct{})
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() { defer wg2.Done(); produceUntil(t, producer2, topic, cs, stop2) }()
	time.Sleep(3 * time.Second)
	close(stop2)
	wg2.Wait()
	producer2.GracefulStop()
	t.Logf("恢复后确认集累计 %d 条", cs.size())

	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "reboot-grant 对账", false)
	cs.assertAllConsumed(t, got)

	for i := range pc.handles {
		if n := countLogLines(t, []string{pc.handles[i].logPath}, "local-forced"); n == 0 {
			t.Fatalf("节点 %d 未走签字放行路径——许可没被消费", i+1)
		}
	}
}

// TestScenarioRebootedFsyncResumesLocally fsync 档即使机器重启过也不需要
// 签字：条目与 term/vote 每次变更都随 raft.MustSync 落了盘，掉电只可能丢
// commit 位点的推进，而那可由 leader 重新告知。
func TestScenarioRebootedFsyncResumesLocally(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3, func(c *config.Config) { c.Cluster.Ack = "quorum-fsync" })
	const topic, group = "scn-reboot-fsync", "scn-reboot-fsync-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()
	producer.GracefulStop()
	t.Logf("健康期确认集 %d 条", cs.size())

	// fsync 档下条目每次 MustSync 都已落盘，理论上无需静置；仍留一小段，
	// 让本用例与前两条保持同一形态，避免日后有人以为这里可以省
	time.Sleep(500 * time.Millisecond)

	for i := range pc.handles {
		pc.kill(t, i)
	}
	for i := range pc.handles {
		pc.setEnv(i, "SQ_BOOTGEN_OVERRIDE=gen-after-reboot")
	}
	pc.restartAll(t)
	ensureTopic(t, pc.endpoints(), topic)

	producer2 := newClusterProducer(t, pc.multi(), topic)
	stop2 := make(chan struct{})
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() { defer wg2.Done(); produceUntil(t, producer2, topic, cs, stop2) }()
	time.Sleep(3 * time.Second)
	close(stop2)
	wg2.Wait()
	producer2.GracefulStop()
	t.Logf("恢复后确认集累计 %d 条", cs.size())

	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "reboot-fsync 对账", false)
	cs.assertAllConsumed(t, got)
}

// restartUnready 启动第 i 个节点但不等待就绪，断言它**无法**提供服务：
// 重启后 35s 内 gRPC 监听不出现、或进程在重入编排失败后自行退出。
//
// 与 restart 的 ready 判定刻意相反：本方法用于断言「安全门拒绝放行」的
// 行为（见 TestScenarioRebootedMemNeedsPermit）。结束时保证进程已死
// （自行退出或强制清理），收尾不留僵尸。
func (pc *procCluster) restartUnready(t *testing.T, i int) {
	t.Helper()
	if pc.handles[i].cmd.Process != nil {
		t.Fatalf("节点 %d 仍在运行，restartUnready 前必须先 kill", i+1)
	}
	ep := pc.handles[i].endpoint
	pc.handles[i] = pc.spawn(t, i, ep)
	h := pc.handles[i]

	deadline := time.Now().Add(35 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		select {
		case err := <-h.waitDone:
			// 进程退出 = 重入编排失败后 main 返回错误，断言成立
			t.Logf("节点 %d 重入编排失败后进程退出：%v", i+1, err)
			h.logFile.Close()
			h.cmd.Process = nil
			return
		default:
		}
		conn, err := net.DialTimeout("tcp", ep, time.Second)
		if err == nil {
			conn.Close()
			ready = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if ready {
		// 安全门该拒启却起来了：说明签字门失效或拒绝路径被改掉。
		// 这是预期信号，但要让用例失败并明确指向原因，而不是静默绿
		t.Fatalf("节点 %d 在需要许可的拒启场景下竟然能提供服务——"+
			"安全门（ErrUncleanShutdown）可能已失效，本用例的断言需要按新行为改写", i+1)
	}
	t.Logf("节点 %d 重启后 35s 内无法提供服务（安全门拒启，数据保留）", i+1)
	// 收尾：进程还活着（挂死在重入轮询），强制杀掉不留僵尸
	h.cmd.Process.Kill()
	<-h.waitDone
	h.logFile.Close()
	h.cmd.Process = nil
}

// TestScenarioPowerLossQuorumLostThenRestored 断电后 quorum 丢失再恢复：
// 断电时一个节点优雅停（SIGTERM 写干净关机标记）、一个节点 kill -9（不
// 干净）、一个节点保持存活——此刻 1 of 3 存活、无 quorum、写不进去
// （「少数派不可写」红线已由场景二守着，不重复断言）。
//
// 断电前确认集里的消息，在两个掉队节点追齐之后必须全部可消费。
//
// 为什么是这两种停机方式而不是全杀：干净关机（优雅停）的节点重启时从
// 本地日志**原身份回归**（RestartNode，不 wipe）——这是全套件里唯一单独
// 覆盖「集群档下干净关机 → 本地日志原身份恢复」的用例；kill -9 的节点走
// 的是不干净的 Rejoin 追齐。同一场景里干净与不干净两条恢复路混合出现，
// 正是「断电」区别于「kill leader」的地方。
//
// **重启顺序是硬性的：必须先重启优雅停的节点，再重启被 kill 的节点。**
// 这不是测试技巧，它就是这类故障的运维恢复步骤：先把能干净恢复的节点
// 拉起来重建多数派，再让不干净的节点重入。顺序反过来的话——被 kill 的
// 节点先起时集群还没有 leader（存活单节点给不出成员变更 quorum），重入
// 编排拿不到接纳：在本分支的代码上它先清空数据目录再失败（数据没了），
// wipe 顺序修复后会保留数据拒启——两种都让用例不成立。
func TestScenarioPowerLossQuorumLostThenRestored(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-quorum-lost", "scn-quorum-lost-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	cs := newConfirmedSet()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); produceUntil(t, producer, topic, cs, stop) }()
	time.Sleep(8 * time.Second)
	close(stop)
	wg.Wait()
	producer.GracefulStop()
	confirmed := cs.size()
	if confirmed == 0 {
		t.Fatal("断电前一条都没确认，用例没测到东西")
	}
	t.Logf("断电时确认集 %d 条", confirmed)

	// 断电：X 优雅停（干净关机标记）、Y kill -9（不干净）、Z 保持存活。
	// 此刻 1 of 3 存活、无 quorum（写不进去，红线由场景二守着）。
	x, y, z := 0, 1, 2
	t.Logf("断电：节点 %d 优雅停、节点 %d kill -9、节点 %d 存活", x+1, y+1, z+1)
	pc.stopGraceful(t, x)
	pc.kill(t, y)
	if pc.aliveCount() != 1 {
		t.Fatalf("断电后应只剩 1 个存活节点，实为 %d", pc.aliveCount())
	}

	// 上电①：先重启 X（优雅停 → 干净恢复，从本地日志原身份回归）。
	pc.restart(t, x)
	// 等 quorum 恢复：X + Z = 2 of 3，全部组选出 leader、路由可答。用
	// QueryRoute（无副作用，不污染 topic 影响对账）轮询等就绪。
	readyDeadline := time.Now().Add(60 * time.Second)
	ready := false
	for !ready && time.Now().Before(readyDeadline) {
		if qs, err := tryQueryRoute(pc.endpointOf(z), topic); err == nil && len(qs) > 0 {
			ready = true
		} else {
			time.Sleep(2 * time.Second)
		}
	}
	if !ready {
		t.Fatal("X 干净恢复后 60s 内 quorum 未恢复（2 of 3 无法选主）")
	}
	t.Logf("X 干净恢复后 quorum 已恢复（2 of 3 选出 leader）")

	// 上电②：再重启 Y（不干净 → Rejoin；此刻有 leader 可批准成员变更）。
	pc.restart(t, y)
	waitRouteSpread(t, pc.endpoints(), topic, 180*time.Second)

	// 对账：断电前确认集必须全部可消费（X 干净回归 + Z 未离场保留数据，
	// Y 追齐后三节点一致）
	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, confirmed, 240*time.Second, "quorum-lost-restore 对账", false)
	cs.assertAllConsumed(t, got)
}

// TestScenarioPoisonMessageToDLQ 毒消息：消费者一律不 ack，达到 max_attempts
// 后消息必须落进 DLQ，且**不得阻塞同队列后续消息**——毒消息把队列钉死是
// 消息系统最典型的级联故障。
//
// 与既有 TestClusterDLQ 的区别：本用例在毒消息重投期间**并发发正常消息**，
// 断言正常消息照常被消费到。TestClusterDLQ 只验证毒消息本身进 DLQ。
func TestScenarioPoisonMessageToDLQ(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	// max_attempts 压到 2：默认 16 次重投会让用例跑到分钟级
	pc := startProcCluster(t, 3, func(c *config.Config) { c.DefaultMaxAttempts = 2 })
	const topic, group = "scn-poison", "scn-poison-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	producer := newClusterProducer(t, pc.multi(), topic)
	poison, err := producer.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte("poison")})
	if err != nil || len(poison) == 0 {
		t.Fatalf("发毒消息失败: %v", err)
	}
	poisonID := poison[0].MessageID
	t.Logf("毒消息 id=%s", poisonID)

	// 正常消息：在毒消息重投窗口内发出
	normal := newConfirmedSet()
	for i := 0; i < 20; i++ {
		recs, err := producer.Send(context.Background(), &rmq.Message{
			Topic: topic, Body: []byte(fmt.Sprintf("normal #%d", i))})
		if err != nil || len(recs) == 0 {
			t.Fatalf("发正常消息 #%d 失败: %v", i, err)
		}
		normal.confirm(recs[0].MessageID)
	}
	producer.GracefulStop()

	// 消费者一律不 ack：毒消息与正常消息都会被重投，但正常消息必须**收到**
	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	seen := map[string]bool{}
	deadline := time.Now().Add(120 * time.Second)
	poisonDeliveries := 0
	for time.Now().Before(deadline) && len(seen) < normal.size()+1 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msgs, err := consumer.Receive(ctx, 16, 20*time.Second)
		cancel()
		if err != nil {
			continue
		}
		for _, m := range msgs {
			seen[m.GetMessageId()] = true
			if m.GetMessageId() == poisonID {
				poisonDeliveries++
			}
			// 一律不 ack：让 invisible 到期后重投
		}
	}
	if poisonDeliveries < 1 {
		t.Fatal("毒消息一次都没投递过，用例没测到东西")
	}
	// 红线：毒消息不得把同队列后续消息钉死
	normal.assertAllConsumed(t, seen)
	t.Logf("毒消息投递 %d 次，%d 条正常消息全部收到（毒消息未钉死队列）",
		poisonDeliveries, normal.size())
}

// TestScenarioProducerExitsRightAfterCommit producer 在 Send 返回成功后
// 立刻 GracefulStop 并退出——断言该消息仍能被消费到。
//
// 为什么单独测：Send 返回成功意味着 broker 侧已 quorum 确认并 apply，
// 消息的存活不该依赖 producer 连接还在。如果实现里有任何「连接关闭时
// 回滚未完成写入」的逻辑（事务半消息就有这种形态），这条会立刻抓到。
func TestScenarioProducerExitsRightAfterCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-producer-exit", "scn-producer-exit-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	cs := newConfirmedSet()
	const n = 30
	for i := 0; i < n; i++ {
		// 每条消息用一个全新的 producer，发完立刻关：把「提交后立退」
		// 的窗口压到最短，而不是发完一批再统一关
		p := newClusterProducer(t, pc.multi(), topic)
		recs, err := p.Send(context.Background(), &rmq.Message{
			Topic: topic, Body: []byte(fmt.Sprintf("exit-now #%d", i))})
		if err != nil || len(recs) == 0 {
			p.GracefulStop()
			t.Fatalf("第 %d 条发送失败: %v", i, err)
		}
		cs.confirm(recs[0].MessageID)
		if err := p.GracefulStop(); err != nil {
			t.Logf("第 %d 个 producer 收尾报错（不影响断言）: %v", i, err)
		}
	}
	t.Logf("已发送并立即退出 %d 次，确认集 %d 条", n, cs.size())

	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 180*time.Second, "producer-exit 对账", false)
	cs.assertAllConsumed(t, got)
}

// queueNextOffsets 读某节点上该 topic 每个队列的 next_offset（= 该队列已存
// 消息条数）。诊断用：sum(next_offset) 就是这个节点存里实际有多少条消息，
// 与「确认集大小」直接可比。
//
// 这是「丢没丢」与「投递得到吗」的判别器：崩溃前后各测一次，
//   - 恢复后总数 < 崩溃前总数 → 消息真从存储里没了（持久化/恢复路径问题）
//   - 恢复后总数 = 崩溃前总数 → 消息还在，是投递侧看不到（消费/位点问题）
//
// B11-OPEN-1 就是靠这两组数字定位的：175 → 158，全部三个节点同样少 17 条，
// 证明是存储侧真丢而非投递侧看不到，根因是 mem 档周期刷盘（空批次 + Sync）
// 被 Pebble 短路成空操作，mem 档实际从不 fsync。
func queueNextOffsets(t *testing.T, adminBase, topic string) (map[uint32]uint64, uint64) {
	t.Helper()
	resp, err := http.Get(adminBase + "/admin/topics/" + topic)
	if err != nil {
		t.Fatalf("读 %s 的 topic 详情失败: %v", adminBase, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("读 %s 的 topic 详情 HTTP %d", adminBase, resp.StatusCode)
	}
	var out struct {
		QueuesDetail []struct {
			QueueID    uint32 `json:"queue_id"`
			NextOffset uint64 `json:"next_offset"`
		} `json:"queues_detail"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("解析 topic 详情失败: %v", err)
	}
	m := make(map[uint32]uint64, len(out.QueuesDetail))
	var total uint64
	for _, q := range out.QueuesDetail {
		m[q.QueueID] = q.NextOffset
		total += q.NextOffset
	}
	return m, total
}

// dumpQueueOffsets 把三个节点的队列位点全打出来，返回各节点的合计条数。
func dumpQueueOffsets(t *testing.T, pc *procCluster, topic, phase string) []uint64 {
	t.Helper()
	totals := make([]uint64, 0, len(pc.handles))
	for i := range pc.handles {
		m, total := queueNextOffsets(t, "http://"+pc.cfgs[i].AdminListen, topic)
		t.Logf("[存量/%s] 节点 %d 队列位点 %v 合计 %d 条", phase, i+1, m, total)
		totals = append(totals, total)
	}
	return totals
}

// adminSend 走 admin HTTP 向指定节点发一条测试消息，返回 msg_id 与是否
// 走了转发（响应 forwarded=true）。admin 端点免登录（节点配置未设
// admin_username/admin_password）。
func adminSend(t *testing.T, adminBase, topic, body string) (msgID string, forwarded bool) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"topic": topic, "body": body})
	if err != nil {
		t.Fatalf("构造 admin 发送请求失败: %v", err)
	}
	resp, err := http.Post(adminBase+"/admin/messages/send", "application/json", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatalf("admin 发送请求失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("admin 发送应 201，得到 %d：%s", resp.StatusCode, raw)
	}
	var got struct {
		MsgID     string `json:"msg_id"`
		Forwarded bool   `json:"forwarded"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("解析 admin 发送响应失败: %v", err)
	}
	return got.MsgID, got.Forwarded
}

// TestScenarioAdminSendOnFollowerForwards 控制台发送测试消息的转发链路
// 端到端：对着**每一个**节点各发一条，全部必须成功且被消费到——总有
// 节点不是目标组的 leader，那条正是走转发的。
func TestScenarioAdminSendOnFollowerForwards(t *testing.T) {
	if testing.Short() {
		t.Skip("场景测试耗时，-short 跳过")
	}
	pc := startProcCluster(t, 3)
	const topic, group = "scn-admin-forward", "scn-admin-forward-g"
	ensureTopic(t, pc.endpoints(), topic)
	waitRouteSpread(t, pc.endpoints(), topic, 60*time.Second)

	cs := newConfirmedSet()
	forwarded := 0
	for i := range pc.handles {
		// admin 监听地址是 "127.0.0.1:<port>"，直接拼成 HTTP 基址
		adminBase := "http://" + pc.cfgs[i].AdminListen
		id, fwd := adminSend(t, adminBase, topic, fmt.Sprintf("forward probe #%d", i))
		cs.confirm(id)
		if fwd {
			forwarded++
		}
	}
	t.Logf("三节点各发一条，其中 %d 条走了转发", forwarded)
	if forwarded == 0 {
		t.Fatal("三个节点都恰好是目标组 leader（不可能，除非转发标记没生效）")
	}

	consumer := newClusterConsumer(t, pc.multi(), group, topic, clusterConsumerAwaitShort)
	got := recvAllAck(t, consumer, cs.size(), 120*time.Second, "admin-forward 对账", false)
	cs.assertAllConsumed(t, got)
}
