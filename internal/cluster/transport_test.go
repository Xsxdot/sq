// transport_test.go 验证 TCP 传输层的两条核心契约。
//
// 职责：真实 TCP 上信封帧的往返承重（组号+消息体原样到达）、
// 对端不可达时 Send 永不阻塞（满则丢）。
// 边界：不覆盖消息乱序/重复（raft 库容忍，属于上层语义）；
// testSlog 等 helper 复用 raftstore_test.go。
package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
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
			}, nil, testSlog(t))
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
		func(uint32, *raftpb.Message) {}, nil, testSlog(t))
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
		func(uint32, *raftpb.Message) {}, nil, testSlog(t))
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

// TestTransportUnregistersClosedConns 终审 R4：入站连接在 readLoop 退出
// （对端断开等）时必须从看门狗集合摘除——不摘除的话，抖动对端反复
// 连接又断开会让 conns 无界增长直到 ctx 取消。
func TestTransportUnregistersClosedConns(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tr := newTransport(1, ln, nil, func(uint32, *raftpb.Message) {}, nil, testSlog(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	// 直连监听器：不经拨号队列，入站连接走 accept→readLoop 注册
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// 等到入站连接注册进看门狗集合
	deadline := time.Now().Add(5 * time.Second)
	for {
		tr.connsMu.Lock()
		n := len(tr.conns)
		tr.connsMu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("入站连接未在 5s 内注册（conns=%d）", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	conn.Close()
	// readLoop 读到 EOF 退出，必须把连接从集合摘除
	for {
		tr.connsMu.Lock()
		n := len(tr.conns)
		tr.connsMu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("连接关闭后未在 5s 内摘除（conns=%d）", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestTransportControlFrames 传输层控制通道的承重测试：ControlGroup
// 帧不进 deliver（raft 消息流）、handler 应答写回同一条连接；nil
// handler 时对端收到「控制通道未装配」错误帧。
func TestTransportControlFrames(t *testing.T) {
	// 场景 1：handler 回显——请求 op=7 payload="ping"，响应帧 op
	// 最高位置 1（0x87）、payload 首字节 0（成功）、余下为应答
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan struct{})
	tr := newTransport(1, ln, nil,
		func(uint32, *raftpb.Message) { close(delivered) },
		func(op byte, payload []byte) ([]byte, error) {
			return append([]byte("echo:"), payload...), nil
		}, testSlog(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := append([]byte{7}, []byte("ping")...)
	if _, err := conn.Write(encodeFrame(nil, ControlGroup, req)); err != nil {
		t.Fatal(err)
	}
	respOp, status, resp, err := readControlResponse(t, conn)
	if err != nil {
		t.Fatal(err)
	}
	if respOp != 7|0x80 {
		t.Fatalf("响应 op=0x%x; want 0x%x（最高位应置 1 表响应）", respOp, 7|0x80)
	}
	if status != 0 {
		t.Fatalf("响应状态字节 = %d; want 0（成功）", status)
	}
	if string(resp) != "echo:ping" {
		t.Fatalf("应答 %q; want %q", resp, "echo:ping")
	}
	select {
	case <-delivered:
		t.Fatal("控制帧被投递给了 deliver——应只走 handler，不进 raft 消息流")
	case <-time.After(500 * time.Millisecond):
	}

	// 场景 2：nil handler 回「控制通道未装配」错误帧（状态字节 1）
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tr2 := newTransport(1, ln2, nil,
		func(uint32, *raftpb.Message) {}, nil, testSlog(t))
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	tr2.Start(ctx2)
	conn2, err := net.Dial("tcp", ln2.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	if _, err := conn2.Write(encodeFrame(nil, ControlGroup, []byte{3, 'x'})); err != nil {
		t.Fatal(err)
	}
	respOp2, status2, resp2, err := readControlResponse(t, conn2)
	if err != nil {
		t.Fatal(err)
	}
	if respOp2 != 3|0x80 {
		t.Fatalf("错误帧 op=0x%x; want 0x%x", respOp2, 3|0x80)
	}
	if status2 != 1 {
		t.Fatalf("错误帧状态字节 = %d; want 1（失败）", status2)
	}
	if !strings.Contains(string(resp2), "控制通道未装配") {
		t.Fatalf("错误帧文本 %q; want 含「控制通道未装配」", resp2)
	}
}

// readControlResponse 在同一连接上读一帧控制响应：返回 op（含响应
// 位）、状态字节与应答 payload。帧布局同请求：[4B 帧长][4B 组号]
// [1B op][payload]，组号已在调用侧语境（控制通道）中确认。
func readControlResponse(t *testing.T, conn net.Conn) (op, status byte, payload []byte, err error) {
	t.Helper()
	header := make([]byte, 4)
	if _, err = io.ReadFull(conn, header); err != nil {
		return
	}
	frameLen := binary.BigEndian.Uint32(header)
	if frameLen < 5 {
		return 0, 0, nil, fmt.Errorf("帧长 %d 过短（控制帧至少 5 字节）", frameLen)
	}
	body := make([]byte, int(frameLen))
	if _, err = io.ReadFull(conn, body); err != nil {
		return
	}
	return body[4], body[5], body[6:], nil
}
