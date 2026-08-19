//go:build e2e

// cluster_proc_test.go：进程级三节点集群的场景 harness。
//
// 职责：
//   - 起 N 个真 broker 进程（真配置、真端口、真 raft 传输），统一收尾
//   - 提供故障注入原语：SIGKILL（断电语义）、优雅停（写干净关机标记）、
//     原地重启（复用同一份配置与数据目录）、SIGSTOP/SIGCONT（假死）
//   - 记录每进程内存峰值（全局约束：性能类验证必记）
//
// 边界：
//   - 不做网络层分区（iptables/pfctl 要 root、两平台两套写法、CI 不可
//     复现）：少数派场景一律用「杀掉 3 之 2」等价模拟，raft 视角下与
//     隔离剩余节点完全一致
//   - 不断言业务语义：对账器与场景断言在各自的用例里
//   - 不复用 startBroker：三节点必须全部先起进程再逐个等就绪（raft 选举
//     要 quorum，等第一个就绪会永远卡在「等 meta 组出 leader」）
package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/config"
)

// procCluster 一组进程级 broker 及其重启所需的全部素材。
type procCluster struct {
	handles []*brokerHandle
	dirs    []string
	cfgs    []*config.Config
	// cfgPaths 与 handles 同序：重启时原样复用（同一份配置 + 同一个数据
	// 目录，节点身份不变，这正是「重启自愈」要验证的路径）
	cfgPaths []string
	// env 是逐节点附加的环境变量（形如 "K=V"），随进程重启保持。
	// 场景用例用它注入 SQ_BOOTGEN_OVERRIDE 来模拟「机器重启过」——
	// 进程级 e2e 起的是真 broker 进程，注不进 Go 函数，只能走环境变量。
	env [][]string
	peaks    *memPeakSampler
}

// startProcCluster 起 n 个 broker 进程并等全部就绪。
//
// 参数：
//   - n: 节点数（场景测试固定用 3）
//   - mutate: 在配置写盘之前逐个应用，供用例覆盖特定配置项
//
// 返回：
//   - 就绪的 *procCluster（收尾已注册到 t.Cleanup）
//
// 注意：必须全部先起进程、再逐个等就绪——raft 选举要 quorum。
func startProcCluster(t *testing.T, n int, mutate ...func(*config.Config)) *procCluster {
	t.Helper()
	grpcPorts := pickPorts(t, n)
	raftPorts := pickPorts(t, n)
	// admin HTTP 端口逐节点错开：默认 :8082 在同机多进程下会互相抢占
	adminPorts := pickPorts(t, n)

	pc := &procCluster{
		handles:  make([]*brokerHandle, n),
		dirs:     make([]string, n),
		cfgs:     make([]*config.Config, n),
		cfgPaths: make([]string, n),
		env:      make([][]string, n),
	}
	for i := 0; i < n; i++ {
		pc.dirs[i] = t.TempDir()
		pc.cfgs[i] = clusterNodeConfig(t, pc.dirs[i], uint64(i+1), grpcPorts[i], raftPorts[i], 3)
		pc.cfgs[i].AdminListen = fmt.Sprintf("127.0.0.1:%d", adminPorts[i])
		for _, fn := range mutate {
			fn(pc.cfgs[i])
		}
	}
	writeClusterPeers(pc.cfgs, grpcPorts, raftPorts)
	for i := 0; i < n; i++ {
		pc.cfgPaths[i] = writeNodeConfig(t, pc.cfgs[i])
		pc.handles[i] = pc.spawn(t, i, fmt.Sprintf("127.0.0.1:%d", grpcPorts[i]))
	}
	for i := 0; i < n; i++ {
		waitBrokerReady(t, pc.handles[i].endpoint, pc.handles[i].waitDone, pc.handles[i].logPath)
		t.Logf("节点 %d 就绪 endpoint=%s pid=%d", i+1, pc.handles[i].endpoint, pc.handles[i].cmd.Process.Pid)
	}
	pc.peaks = newMemPeakSampler(pc.handles)
	t.Cleanup(func() { pc.stopAll(t) })
	return pc
}

// spawn 起单个 broker 进程（不等就绪）。日志按「节点目录/broker.log」
// 追加：重启后同一节点的多段日志留在同一个文件里，排查时不必拼接。
func (pc *procCluster) spawn(t *testing.T, i int, endpoint string) *brokerHandle {
	t.Helper()
	logPath := filepath.Join(pc.dirs[i], "broker.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("打开节点 %d 日志文件失败: %v", i+1, err)
	}
	cmd := exec.Command(brokerBinary, "-config", pc.cfgPaths[i])
	if len(pc.env[i]) > 0 {
		cmd.Env = append(os.Environ(), pc.env[i]...)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("启动节点 %d 失败: %v", i+1, err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	return &brokerHandle{
		endpoint: endpoint, cfgPath: pc.cfgPaths[i], logPath: logPath,
		logFile: logFile, cmd: cmd, waitDone: waitDone,
	}
}

// multi 返回 SDK 用的「;」分隔多地址（SDK resolver 按 ; 拆分）。
// 已被 kill 的节点仍留在串里：SDK 换节点重试正是要被验证的行为。
func (pc *procCluster) multi() string {
	eps := make([]string, 0, len(pc.handles))
	for _, h := range pc.handles {
		eps = append(eps, h.endpoint)
	}
	return strings.Join(eps, ";")
}

// endpointOf 返回第 i 个节点（0-based）的 gRPC 地址。
func (pc *procCluster) endpointOf(i int) string { return pc.handles[i].endpoint }

// setEnv 给第 i 个节点追加环境变量，下次 spawn 生效（重启后保持）。
func (pc *procCluster) setEnv(i int, kv ...string) {
	pc.env[i] = append(pc.env[i], kv...)
}

// endpoints 返回全部节点地址（0-based 同序）。
func (pc *procCluster) endpoints() []string {
	eps := make([]string, 0, len(pc.handles))
	for _, h := range pc.handles {
		eps = append(eps, h.endpoint)
	}
	return eps
}

// indexOfEndpoint 按地址反查下标；找不到返回 -1。
func (pc *procCluster) indexOfEndpoint(ep string) int {
	for i, h := range pc.handles {
		if h.endpoint == ep {
			return i
		}
	}
	return -1
}

// kill SIGKILL 第 i 个节点（断电语义：不写干净关机标记，重启必须走
// ErrUncleanShutdown → Rejoin 自愈路径）。
func (pc *procCluster) kill(t *testing.T, i int) {
	t.Helper()
	h := pc.handles[i]
	if h.cmd.Process == nil {
		return
	}
	pid := h.cmd.Process.Pid
	if err := h.cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL 节点 %d: %v", i+1, err)
	}
	pc.awaitExit(t, i, "SIGKILL")
	t.Logf("节点 %d 已 SIGKILL（pid=%d）", i+1, pid)
}

// stopGraceful SIGTERM 第 i 个节点并等它自行收尾（写干净关机标记，
// 重启走干净恢复路径而不是 Rejoin）。
func (pc *procCluster) stopGraceful(t *testing.T, i int) {
	t.Helper()
	h := pc.handles[i]
	if h.cmd.Process == nil {
		return
	}
	pid := h.cmd.Process.Pid
	if err := h.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM 节点 %d: %v", i+1, err)
	}
	pc.awaitExit(t, i, "SIGTERM")
	t.Logf("节点 %d 已优雅停机（pid=%d）", i+1, pid)
}

// awaitExit 等进程退出并回收日志文件句柄；超时即失败（进程没死透会让
// 后续 restart 撞端口，报错必须落在这里而不是下游）。
func (pc *procCluster) awaitExit(t *testing.T, i int, how string) {
	t.Helper()
	h := pc.handles[i]
	select {
	case err := <-h.waitDone:
		t.Logf("节点 %d 因 %s 退出：%v", i+1, how, err)
	case <-time.After(20 * time.Second):
		t.Fatalf("节点 %d 在 %s 后 20s 内未退出", i+1, how)
	}
	h.logFile.Close()
	h.cmd.Process = nil
}

// restart 原地重启第 i 个节点（同配置、同数据目录、同节点 id）并等就绪。
//
// 注意：ready 判定沿用 waitBrokerReady（gRPC 可用即算就绪）；集群档的
// 「已追上多数派」由用例自己的对账断言承担，harness 不越俎代庖。
//
// 只适用于「多数派仍存活」的单点重启：ready 判定依赖 waitMetaLeader 先过，
// 而选主要 quorum——全集群停机后的整批重启请用 restartAll，逐台 restart 会
// 在第一个节点上死锁（详见 restartAll 注释）。
func (pc *procCluster) restart(t *testing.T, i int) {
	t.Helper()
	if pc.handles[i].cmd.Process != nil {
		t.Fatalf("节点 %d 仍在运行，restart 前必须先 kill/stopGraceful", i+1)
	}
	ep := pc.handles[i].endpoint
	pc.handles[i] = pc.spawn(t, i, ep)
	waitBrokerReady(t, ep, pc.handles[i].waitDone, pc.handles[i].logPath)
	t.Logf("节点 %d 已重启就绪 pid=%d", i+1, pc.handles[i].cmd.Process.Pid)
}

// restartAll 整批原地重启全部节点：先全部起进程、再逐个等就绪。
//
// 与 restart 的区别：全集群停机（进程全崩／真掉电后上电）后**必须先把进程
// 全起来、再逐个等就绪**——ready 判定依赖 waitMetaLeader，而 raft 选举要
// quorum；首个节点起来时是 1 of 3，选不出 leader，gRPC 监听不会出现。逐台
// restart 会在第一个节点上等满 brokerStartTimeout 然后 Fatal（用例红在
// harness 上，和被测行为无关）。restartAll 让三个进程几乎同时起，第二、三
// 个节点一就绪多数派立刻成型、选举随即收敛。与 startProcCluster 的装配序
// 是同一个道理（见该函数注释）。
func (pc *procCluster) restartAll(t *testing.T) {
	t.Helper()
	for i := range pc.handles {
		if pc.handles[i].cmd.Process != nil {
			t.Fatalf("节点 %d 仍在运行，restartAll 前必须先 kill/stopGraceful", i+1)
		}
	}
	for i := range pc.handles {
		ep := pc.handles[i].endpoint
		pc.handles[i] = pc.spawn(t, i, ep)
	}
	for i := range pc.handles {
		waitBrokerReady(t, pc.handles[i].endpoint, pc.handles[i].waitDone, pc.handles[i].logPath)
		t.Logf("节点 %d 已重启就绪 pid=%d", i+1, pc.handles[i].cmd.Process.Pid)
	}
}

// pause SIGSTOP 第 i 个节点（假死：进程还在、端口还占着，但不再响应
// 任何 raft 消息与客户端请求）。用于「不是崩溃而是卡住」的形态。
func (pc *procCluster) pause(t *testing.T, i int) {
	t.Helper()
	if err := pc.handles[i].cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP 节点 %d: %v", i+1, err)
	}
	t.Logf("节点 %d 已 SIGSTOP（假死）", i+1)
}

// resume SIGCONT 第 i 个节点。
func (pc *procCluster) resume(t *testing.T, i int) {
	t.Helper()
	if err := pc.handles[i].cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT 节点 %d: %v", i+1, err)
	}
	t.Logf("节点 %d 已 SIGCONT（恢复）", i+1)
}

// aliveCount 返回仍在运行的节点数。
func (pc *procCluster) aliveCount() int {
	n := 0
	for _, h := range pc.handles {
		if h.cmd.Process != nil {
			n++
		}
	}
	return n
}

// stopAll 采样内存峰值 → 停掉全部存活节点 → 用例失败时打印各节点日志。
func (pc *procCluster) stopAll(t *testing.T) {
	if pc.peaks != nil {
		pc.peaks.stopAndReport(t)
		pc.peaks = nil
	}
	for i, h := range pc.handles {
		if h.cmd.Process != nil {
			h.stop(t)
			h.cmd.Process = nil
		}
		if t.Failed() {
			dumpBrokerLog(t, filepath.Join(pc.dirs[i], "broker.log"))
		}
	}
}
