// transport_test.go 验证 TCP 传输层的两条核心契约。
//
// 职责：真实 TCP 上信封帧的往返承重（组号+消息体原样到达）、
// 对端不可达时 Send 永不阻塞（满则丢）。
// 边界：不覆盖消息乱序/重复（raft 库容忍，属于上层语义）；
// testSlog 等 helper 复用 raftstore_test.go。
package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// TestTransportDeliverAcrossNodes 两个传输体经真实 TCP 互投消息，
// 组号与消息体原样到达（信封帧的往返承重测试）。
func TestTransportDeliverAcrossNodes(t *testing.T) {
	ln1, _ := net.Listen("tcp", "127.0.0.1:0")
	ln2, _ := net.Listen("tcp", "127.0.0.1:0")
	got := make(chan string, 4)
	mk := func(self uint64, ln net.Listener, peerID uint64, peerAddr string) *transport {
		return newTransport(self, ln, map[uint64]string{peerID: peerAddr},
			func(g uint32, m *raftpb.Message) {
				got <- fmt.Sprintf("g%d:from%d:to%d", g, m.GetFrom(), m.GetTo())
			}, testSlog(t))
	}
	t1 := mk(1, ln1, 2, ln2.Addr().String())
	t2 := mk(2, ln2, 1, ln1.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t1.Start(ctx)
	t2.Start(ctx)
	from, to := uint64(1), uint64(2)
	typ := raftpb.MsgHeartbeat
	t1.Send(3, []*raftpb.Message{{Type: &typ, From: &from, To: &to}})
	select {
	case s := <-got:
		if s != "g3:from1:to2" {
			t.Fatalf("到达 %q; want g3:from1:to2", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("5s 未收到消息（含首次拨号重试窗口）")
	}
}

// TestTransportDropsWhenPeerDown 对端未监听时 Send 不阻塞不报错——
// 丢消息是 raft 传输层的合法行为，阻塞才是灾难（会卡死 Ready 循环）。
func TestTransportDropsWhenPeerDown(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	tr := newTransport(1, ln, map[uint64]string{2: "127.0.0.1:1"}, // 1 端口必拒
		func(uint32, *raftpb.Message) {}, testSlog(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	from, to := uint64(1), uint64(2)
	typ := raftpb.MsgHeartbeat
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ { // 远超队列容量，验证满则丢不阻塞
			tr.Send(0, []*raftpb.Message{{Type: &typ, From: &from, To: &to}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("对端不可达时 Send 阻塞——违反不阻塞契约")
	}
}

// TestTransportDropPeerDrainsQueue 成员变更移除节点后，传输队列里
// 变更前的心跳积压必须被 DropPeer 排空——不排空的话，对端以空/短日志
// 重入时收到携带旧 commit 索引的陈旧心跳会触发 raft 库 tocommit 越界
// panic（Task 7 集成测试抓到的缺口，见 Manager.ProposeConfChange）。
func TestTransportDropPeerDrainsQueue(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	tr := newTransport(1, ln, map[uint64]string{2: "127.0.0.1:1"}, // 1 端口必拒：无连接，消息全积压
		func(uint32, *raftpb.Message) {}, testSlog(t))
	from, to := uint64(1), uint64(2)
	typ := raftpb.MsgHeartbeat
	// 远超队列容量（4096）：既攒出积压又真正触发在途丢弃——drops 计数
	// 与 DropPeer 的重置都被实际锻炼到（只发 100 条的话 drops 恒为 0，
	// 断言形同虚设）。
	const sends = 10000
	for i := 0; i < sends; i++ {
		tr.Send(0, []*raftpb.Message{{Type: &typ, From: &from, To: &to}})
	}
	if n := len(tr.queues[2]); n == 0 {
		t.Fatal("积压前置不成立：队列应为空？——测试自身有问题")
	}
	if n := tr.drops[2].Load(); n == 0 {
		t.Fatal("丢弃计数应为非零——Send 的满则丢路径未被触发")
	}
	tr.DropPeer(2)
	if n := len(tr.queues[2]); n != 0 {
		t.Fatalf("DropPeer 后队列残留 %d 条", n)
	}
	tr.DropPeer(2) // 幂等：重复调用不炸
	if n := tr.drops[2].Load(); n != 0 {
		t.Fatalf("DropPeer 后丢弃计数 = %d; want 0", n)
	}
}
