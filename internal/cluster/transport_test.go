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
	"sync/atomic"
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

// TestTransportBatchedFramesDecodeIndividually 发送侧合批的承重测试：
// 先把一批消息全部入队再 Start——writeLoop 建连后首轮即面对满队列，
// 非阻塞抽干后编成连续多帧、一次（或少数几次）写出。接收端 readLoop
// 未改，必须能从连续字节流中逐帧解出每条消息，组号与消息体一一对应
// 且保序（帧格式逐字节不变是合批的前提契约）。
func TestTransportBatchedFramesDecodeIndividually(t *testing.T) {
	ln1, _ := net.Listen("tcp", "127.0.0.1:0")
	ln2, _ := net.Listen("tcp", "127.0.0.1:0")
	type rec struct {
		g   uint32
		idx uint64
	}
	const n = 200
	got := make(chan rec, n)
	t2 := newTransport(2, ln2, nil,
		func(g uint32, m *raftpb.Message) { got <- rec{g, m.GetIndex()} }, nil, testSlog(t))
	t1 := newTransport(1, ln1, map[uint64]string{2: ln2.Addr().String()},
		func(uint32, *raftpb.Message) {}, nil, testSlog(t))
	from, to := uint64(1), uint64(2)
	typ := raftpb.MsgHeartbeat
	// Start 前全部入队：确保 writeLoop 拿到第一条时队列里已有大量积压，
	// 抽干路径真正被锻炼到（而非退化成一条一写）。
	for i := uint64(0); i < n; i++ {
		idx := i
		t1.Send(uint32(i%5), []*raftpb.Message{{Type: &typ, From: &from, To: &to, Index: &idx}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t1.Start(ctx)
	t2.Start(ctx)
	for i := uint64(0); i < n; i++ {
		select {
		case r := <-got:
			if r.g != uint32(i%5) || r.idx != i {
				t.Fatalf("第 %d 条解出 (g=%d idx=%d); want (g=%d idx=%d)——合批破坏了帧边界或保序", i, r.g, r.idx, uint32(i%5), i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("5s 内仅收到 %d/%d 条（含首次拨号重试窗口）", i, n)
		}
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

// TestTransportDiscardsBacklogWhilePeerUnreachable 对端不可达期间不积压：
// 拨号失败即清空该 peer 队列，对端恢复后只收到恢复之后发出的消息。
//
// 承重原因（SQ4.10 排查）：停机期间 leader 每 tick 发的心跳与每次提交
// 广播的空 MsgApp 在队列里攒成数百条；对端重启后逐条回 MsgHeartbeatResp，
// 而 raft 在 StateProbe 下每收一条心跳响应就解除暂停、从同一 Next 重发一条
// 满 MaxSizePerMsg 的 MsgApp——重发条数与停机时长成正比，落后条目经
// 冷读层每次新分配，leader 内存随之放大。积压的陈旧消息对 raft 毫无用处
// （raft 自身按 tick 重发心跳、按 Progress 重发日志），丢弃是合法行为。
func TestTransportDiscardsBacklogWhilePeerUnreachable(t *testing.T) {
	// 先占端口再关：拿到一个「当前无人监听、稍后可重新监听」的地址。
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	ln1, _ := net.Listen("tcp", "127.0.0.1:0")
	tr := newTransport(1, ln1, map[uint64]string{2: addr},
		func(uint32, *raftpb.Message) {}, nil, testSlog(t))
	from, to := uint64(1), uint64(2)
	typ := raftpb.MsgHeartbeat
	const stale = 1000
	for i := uint64(0); i < stale; i++ {
		idx := i
		tr.Send(0, []*raftpb.Message{{Type: &typ, From: &from, To: &to, Index: &idx}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	// 至少跨过一轮「拨号失败 → 退避」，积压应在此期间被清空。
	time.Sleep(2 * redialBackoff)

	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("端口 %s 被占用，无法重建对端：%v", addr, err)
	}
	got := make(chan uint64, stale+1)
	peer := newTransport(2, ln2, nil,
		func(_ uint32, m *raftpb.Message) { got <- m.GetIndex() }, nil, testSlog(t))
	peer.Start(ctx)
	const marker = uint64(1 << 40)
	mk := marker
	tr.Send(0, []*raftpb.Message{{Type: &typ, From: &from, To: &to, Index: &mk}})

	staleSeen := 0
	for {
		select {
		case idx := <-got:
			if idx == marker {
				if staleSeen != 0 {
					t.Fatalf("对端恢复后先收到 %d 条不可达期间的积压消息; want 0（积压应在拨号失败时清空）", staleSeen)
				}
				return
			}
			staleSeen++
		case <-time.After(5 * time.Second):
			t.Fatalf("5s 未收到恢复后发出的消息（已收积压 %d 条）", staleSeen)
		}
	}
}

// awaitOnlyFresh 每 100ms 发一条新鲜 marker（Index ≥ markerBase），直到
// 首条 marker 到达；期间统计先于它到达的非 marker 消息（均为故障期间
// 入队的陈旧消息），有任何一条即失败。marker 反复发：恢复后的首次重连
// 时刻不可预知，单发的 marker 自己也可能在队列里老化被丢。
func awaitOnlyFresh(t *testing.T, tr *transport, got <-chan uint64, wait time.Duration) {
	t.Helper()
	const markerBase = uint64(1 << 40)
	from, to := uint64(1), uint64(2)
	hb := raftpb.MsgHeartbeat
	stale := 0
	deadline := time.After(wait)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for i := markerBase; ; {
		select {
		case <-tick.C:
			idx := i
			i++
			tr.Send(0, []*raftpb.Message{{Type: &hb, From: &from, To: &to, Index: &idx}})
		case idx := <-got:
			if idx >= markerBase {
				if stale != 0 {
					t.Fatalf("对端恢复后先收到 %d 条故障期间入队的陈旧消息; want 0（超过选举超时的积压应在出队时丢弃）", stale)
				}
				return
			}
			stale++
		case <-deadline:
			t.Fatalf("%s 内未收到恢复后发出的消息（已收陈旧消息 %d 条）", wait, stale)
		}
	}
}

// TestTransportDropsStaleBacklogAfterDialHang 故障形态一：对端主机掉电/
// 黑洞，拨号既不成功也不立即失败（无超时时挂约 127s）。拨号挂起期间入队
// 的消息到恢复时已超过选举超时，必须在出队时丢弃，而不是恢复后整批送达
// （否则重启 follower 逐条回心跳响应，leader 在探测态逐条整段重发）。
func TestTransportDropsStaleBacklogAfterDialHang(t *testing.T) {
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan uint64, 2048)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer := newTransport(2, ln2, nil,
		func(_ uint32, m *raftpb.Message) { got <- m.GetIndex() }, nil, testSlog(t))
	peer.Start(ctx)

	ln1, _ := net.Listen("tcp", "127.0.0.1:0")
	tr := newTransport(1, ln1, map[uint64]string{2: ln2.Addr().String()},
		func(uint32, *raftpb.Message) {}, nil, testSlog(t))
	release := make(chan struct{})
	var hung atomic.Bool
	hung.Store(true)
	tr.dial = func(dctx context.Context, addr string) (net.Conn, error) {
		if hung.Load() {
			select { // 模拟黑洞：挂到「恢复」或拨号超时
			case <-release:
			case <-dctx.Done():
				return nil, dctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(dctx, "tcp", addr)
	}
	from, to := uint64(1), uint64(2)
	typ := raftpb.MsgHeartbeat
	for i := uint64(0); i < 1000; i++ {
		idx := i
		tr.Send(0, []*raftpb.Message{{Type: &typ, From: &from, To: &to, Index: &idx}})
	}
	tr.Start(ctx)
	time.Sleep(staleMsgAge + 500*time.Millisecond) // 积压老化越过选举超时
	hung.Store(false)
	close(release)
	awaitOnlyFresh(t, tr, got, 10*time.Second)
}

// TestTransportDropsStaleBacklogAfterHalfOpenConn 故障形态二：旧连接半开
// （对端进程冻结/掉电但未发 RST），对端不读、写阻塞（无写期限时挂到
// tcp_retries2，约 15 分钟）。写期限到期须关连接重拨；阻塞期间入队的
// 消息到恢复时已超过选举超时，必须在出队时丢弃。
func TestTransportDropsStaleBacklogAfterHalfOpenConn(t *testing.T) {
	frozenLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := frozenLn.Addr().String()
	frozen := make(chan net.Conn, 4)
	go func() { // 冻结的对端：接受连接但从不读
		for {
			c, err := frozenLn.Accept()
			if err != nil {
				return
			}
			frozen <- c
		}
	}()
	defer func() {
		for {
			select {
			case c := <-frozen:
				c.Close()
			default:
				return
			}
		}
	}()

	ln1, _ := net.Listen("tcp", "127.0.0.1:0")
	tr := newTransport(1, ln1, map[uint64]string{2: addr},
		func(uint32, *raftpb.Message) {}, nil, testSlog(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	from, to := uint64(1), uint64(2)
	app := raftpb.MsgApp
	// 64 条 × 1MiB：远超两端套接字缓冲，写必然阻塞在冻结连接上
	big := make([]byte, 1<<20)
	for i := uint64(0); i < 64; i++ {
		idx := i
		tr.Send(0, []*raftpb.Message{{Type: &app, From: &from, To: &to, Index: &idx,
			Entries: []*raftpb.Entry{{Data: big}}}})
	}
	select {
	case <-frozen: // 连接已建立，冻结连接保持打开（不发 RST，即半开）
	case <-time.After(5 * time.Second):
		t.Fatal("5s 未建立到冻结对端的连接")
	}
	// 冻结期间继续入队一批小消息，随后对端「恢复」：同地址换成正常读的
	// 传输体（旧冻结连接仍不关，模拟半开）。
	hb := raftpb.MsgHeartbeat
	for i := uint64(100); i < 1100; i++ {
		idx := i
		tr.Send(0, []*raftpb.Message{{Type: &hb, From: &from, To: &to, Index: &idx}})
	}
	time.Sleep(staleMsgAge + 500*time.Millisecond)
	frozenLn.Close()
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("端口 %s 被占用，无法重建对端：%v", addr, err)
	}
	got := make(chan uint64, 4096)
	peer := newTransport(2, ln2, nil,
		func(_ uint32, m *raftpb.Message) { got <- m.GetIndex() }, nil, testSlog(t))
	peer.Start(ctx)
	awaitOnlyFresh(t, tr, got, writeTimeout+dialTimeout+redialBackoff+5*time.Second)
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
