package raftshell

import (
	"log/slog"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// Transport 是节点间的消息投递契约（Task 3/4/5 复用）。
// Send 把本节点产出的若干消息投递给各自的收件节点；实现必须非阻塞。
type Transport interface {
	Send(from uint64, ms []raftpb.Message)
}

// chanTransport 进程内通道传输：每节点一个 inbox，支持注入单向延迟与
// 分区/摘除（Task 3 kill-leader、后续分区实验用）。
//
// 职责：模拟内网 RTT（默认 100µs）与节点摘除（down）语义。
// 边界：不保证投递顺序之外的消息语义——丢包、乱序等真实网络故障
//       不属于本 spike 范围（raft 库本身容忍这些）。
type chanTransport struct {
	mu    sync.RWMutex
	nodes map[uint64]*Node
	down  map[uint64]bool // 摘除的节点：投给它的消息直接丢弃
	delay time.Duration   // 模拟内网 RTT/2，默认 100µs
	lg    *slog.Logger
}

// newChanTransport 构造默认传输：延迟 100µs，无摘除节点。
func newChanTransport() *chanTransport {
	return &chanTransport{
		nodes: make(map[uint64]*Node),
		down:  make(map[uint64]bool),
		delay: 100 * time.Microsecond,
		lg:    slog.Default(),
	}
}

// register 注册节点：Send 投给它的消息将经由 Step 入队。
// 重复注册以最后一次为准。
func (t *chanTransport) register(id uint64, n *Node) {
	t.mu.Lock()
	t.nodes[id] = n
	t.mu.Unlock()
}

// kill 摘除节点：此后投给它的消息被直接丢弃（模拟进程宕机）。
func (t *chanTransport) kill(id uint64) {
	t.mu.Lock()
	t.down[id] = true
	t.mu.Unlock()
}

// restore 恢复被摘除的节点。
func (t *chanTransport) restore(id uint64) {
	t.mu.Lock()
	delete(t.down, id)
	t.mu.Unlock()
}

// Send 对每条消息：目标 down 则丢弃并打 Debug 日志；
// 否则等待 delay 后投递（异步，模拟网络单向延迟，不阻塞 Ready 循环）。
func (t *chanTransport) Send(from uint64, ms []raftpb.Message) {
	for _, m := range ms {
		to := m.GetTo()
		t.mu.RLock()
		down := t.down[to]
		target, ok := t.nodes[to]
		t.mu.RUnlock()
		if !ok || down {
			t.lg.Debug("消息丢弃", "from", from, "to", to, "type", m.Type.String())
			continue
		}
		msg := m
		go func() {
			time.Sleep(t.delay)
			target.Step(msg)
		}()
	}
}
