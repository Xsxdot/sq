// transport.go 提供 raft 消息的节点间 TCP 投递：每 peer 一个发送队列
// + 发送 goroutine，accept 循环为每连接开读 goroutine，帧带组号信封。
//
// 职责：
//   - Send 按目标节点入队，发送 goroutine 内做序列化与写帧（信封帧）
//   - 断线 500ms 退避重拨，坏帧（超长/不可反序列化）断开等对端重拨
//   - 控制通道：ControlGroup 保留组号帧不进 raft 消息流，同一连接上
//     应答后关闭（短连接 RPC，见 ControlGroup 注释）
//
// 边界：
//   - 不保证送达：队列满即丢（raft 心跳重试是丢消息的兜底），
//     阻塞会让上游 Ready 循环停摆，所以宁可丢也不能等
//   - 不做鉴权/TLS：集群内网信任假设，安全加固留待 batch③
//     （届时在部署文档中记录本层无加密这一事实）
//   - 不保证顺序之外的语义：乱序/重复由 raft 库自身容忍
//
// 帧格式（信封帧，大端字节布局）：
//
//	┌───────────────┬───────────────┬──────────────────────┐
//	│ 4B 帧长(frame) │ 4B 组号(group) │ payload (protobuf)   │
//	└───────────────┴───────────────┴──────────────────────┘
//
//	帧长 = 4 + len(payload)，即帧长字段之后的全部剩余字节数。
//	payload 是 raftpb.Message 的 protobuf 序列化；组号随帧投递，
//	接收端原样交给 deliver 回调（调用方据此归组）。
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	// peerQueueCap 每 peer 发送队列容量。队列满即丢——容量只是积压
	// 缓冲，不是送达承诺（raft 心跳重试兜底）。
	peerQueueCap = 4096

	// maxFrameLen 单帧上限 16MiB：坏帧可声明任意帧长，不设上限会把
	// 读缓冲撑爆；超限即断连，等对端重拨。
	maxFrameLen = 16 << 20

	// redialBackoff 断线后重拨间隔。
	redialBackoff = 500 * time.Millisecond
)

// ControlGroup 是控制通道的保留组号：业务组号上限 64（Task 1 数据组
// 数契约），取 uint32 最大值与业务组永不碰撞。控制帧与信封帧同构，
// 但 payload 首字节为 op、且**不进 raft 消息流**——请求/响应各走
// 独立短连接（Manager.Control 一次拨号→写请求帧→读响应帧→关闭），
// 避免低频 RPC 与 raft 流水消息在同一连接上交织出队头阻塞。
//
// 帧布局（大端，对齐信封帧注释风格）：
//
//	┌───────────────┬────────────────┬──────┬───────────────────┐
//	│ 4B 帧长(frame) │ 4B ControlGroup │ 1B op │ payload            │
//	└───────────────┴────────────────┴──────┴───────────────────┘
//
//	帧长 = 4 + 1 + len(payload)（帧长字段之后的全部字节数）。
//	响应帧同构：op 最高位（0x80）置 1 表响应；payload 首字节
//	0=成功（余下为应答数据）、1=失败（余下为 UTF-8 错误文本）。
const ControlGroup uint32 = 0xFFFFFFFF

// 控制通道操作码注册表：op 是控制帧 payload 的首字节（Manager.Control
// 与传输层 controlFrame 之间的协议面），取值即跨节点线协议——改动即
// 不兼容（TestControlOpRegistry 锁死黄金值）。
//
//   - OpForwardAppend=1: payload=[4B BE 目标组][core.EncodeMessage 字节]，
//     响应 payload=[4B BE queueID][8B BE offset]——跨节点消息追加，
//     offset 分配发生在 leader 侧（replication.ForwardAppend 消费）
//   - OpForwardApply=2: payload=[4B BE 目标组][store.Batch.Repr 字节]，
//     响应空——构造无关批次的跨节点提案（replication.ForwardApply 消费）
//   - OpPrepareJoin=3: payload=[8B BE nodeID]，响应=该节点完成
//     Remove→AddLearner 的组号列表——learner 重入编排（Task 10
//     PrepareJoin handler 消费）
//   - OpFetchSnapshot=4: payload=[4B BE 组][8B BE snapID][4B BE 游标
//     键长][游标键]，响应=[1B 是否结束][4B BE 下一游标键长][下一游标
//     键][块字节]——按 snapID 分块拉取钉住的快照视图（Manager 内部
//     handler 消费，见 handleFetchSnapshot）
//
// 定义在本包而非 replication：Task 10 的 PrepareJoin handler 在 cluster
// 侧装配，而依赖方向是 replication→cluster——常量放集群侧才能被双方
// 引用（plan 原稿放 replication 的决定经终审裁定迁移，见 progress 预检
// 注记）。Task 11 的 ControlHandler 接线在 main 装配时引用本注册表。
const (
	OpForwardAppend byte = 1
	OpForwardApply  byte = 2
	OpPrepareJoin   byte = 3
	OpFetchSnapshot byte = 4
)

// envelope 发送队列中的一条待发消息：组号 + 消息指针。
//
// msg 必须是指针——v3 的 raftpb.Message 内嵌互斥锁（protoimpl.MessageState），
// 值拷贝会触发 vet copylocks 且破坏发送侧并发安全。
type envelope struct {
	group uint32
	msg   *raftpb.Message
}

// transport 是 raft 消息的 TCP 传输层。
//
// 结构：每 peer 一个 chan envelope（cap peerQueueCap）+ 一个发送
// goroutine（拨号→循环写帧→断线关连接、redialBackoff 重拨）；
// accept 循环为每连接开一个读 goroutine。ctx 取消即全部退出。
type transport struct {
	self    uint64
	ln      net.Listener
	peers   map[uint64]string // peer 节点 id → 地址表（不含本节点）
	deliver func(g uint32, m *raftpb.Message)
	lg      *slog.Logger

	// control 是控制通道处理器（可为 nil）：收到 ControlGroup 帧时在
	// 读循环内同步调用，应答写回同一条连接后关闭；nil 时回
	// 「控制通道未装配」错误帧（见 controlFrame）。
	control func(op byte, payload []byte) ([]byte, error)

	queues map[uint64]chan envelope
	drops  map[uint64]*atomic.Uint64 // 队列满丢弃计数（周期上报，防刷屏）

	connsMu sync.Mutex
	conns   map[net.Conn]struct{} // 活动读连接，ctx 取消时统一关闭

	wg sync.WaitGroup
}

// newTransport 构造传输层。
//
// 参数：
//   - self: 本节点 id（仅用于日志上下文）
//   - ln: 监听器，Start 起 accept 循环；ctx 取消时会被本层关闭，
//     调用方不得复用
//   - peers: 节点 id → 地址表，**不含本节点**——本节点消息由 Manager
//     内部短路，不经传输（配置了也不会被 Send 命中）
//   - deliver: 接收回调（g 为信封组号）；可能被多个读 goroutine 并发调用，
//     调用方需自行保证线程安全
//   - control: 控制通道处理器（可为 nil）；nil 时对端收到
//     「控制通道未装配」错误帧（见 controlFrame）
//   - lg: 日志器，本层以 mod=transport 绑定
func newTransport(self uint64, ln net.Listener, peers map[uint64]string,
	deliver func(g uint32, m *raftpb.Message),
	control func(op byte, payload []byte) ([]byte, error), lg *slog.Logger) *transport {
	t := &transport{
		self:    self,
		ln:      ln,
		peers:   peers,
		deliver: deliver,
		control: control,
		lg:      lg.With("mod", "transport"),
		queues:  make(map[uint64]chan envelope, len(peers)),
		drops:   make(map[uint64]*atomic.Uint64, len(peers)),
		conns:   make(map[net.Conn]struct{}),
	}
	for peerID := range peers {
		t.queues[peerID] = make(chan envelope, peerQueueCap)
		t.drops[peerID] = &atomic.Uint64{}
	}
	return t
}

// Start 启动传输层：accept 循环 + 每 peer 一个拨号/发送循环。
//
// 注意：
//   - ctx 取消即全部退出：监听器与活动连接被本层关闭，调用方不得复用
//   - 本方法立即返回，所有循环在后台 goroutine 运行
func (t *transport) Start(ctx context.Context) {
	// 看门狗：ctx 取消时关闭监听器与所有活动连接，解除阻塞中的
	// Accept/ReadFull/Write，各循环随即自行退出。
	go func() {
		<-ctx.Done()
		t.ln.Close()
		t.connsMu.Lock()
		for c := range t.conns {
			c.Close()
		}
		t.connsMu.Unlock()
	}()

	t.wg.Add(1)
	go t.acceptLoop(ctx)

	for peerID, addr := range t.peers {
		t.wg.Add(1)
		go t.sendLoop(ctx, peerID, addr)
	}
}

// Send 把一组消息按目标节点（m.GetTo()）分投各 peer 队列。
//
// 契约：永不阻塞——队列满时静默丢弃并计数（raft 心跳重试是丢消息的
// 兜底）。Ready 循环在上游，阻塞即全组停摆，所以这里宁可丢也不能等；
// 消息的序列化在发送 goroutine 内完成，本方法只做入队，开销与
// 是否在线无关（对端不可达同样不阻塞）。
//
// 参数：
//   - g: 数据组号，作为信封组号随帧传递，接收端原样交给 deliver
//   - msgs: 待发送消息（指针切片，禁止值拷贝消息体）
func (t *transport) Send(g uint32, msgs []*raftpb.Message) {
	for _, m := range msgs {
		to := m.GetTo()
		q, ok := t.queues[to]
		if !ok {
			// 未配置对端（含本节点自消息，Manager 正常应短路）：
			// 静默丢，不阻塞 Ready 循环。
			continue
		}
		select {
		case q <- envelope{group: g, msg: m}:
		default:
			t.drops[to].Add(1) // 队列满：计数丢弃，等 raft 重试
		}
	}
}

// DropPeer 排空指定 peer 的发送队列并清零丢弃计数。
//
// 成员变更移除节点后调用：raft 侧该节点的 Progress 已删除、不再产生
// 新消息，但队列里还压着变更前的心跳等积压——它们携带变更前的 commit
// 索引（min(旧 Match, committed)），对端若以空/短日志重入，收到即
// 触发 raft 库的 tocommit 越界 panic（排障见 Manager.ProposeConfChange
// 注释）。排空的确定性依赖「对端离线」假设：离线时发送 goroutine
// 停在拨号失败循环、没有在途写，队列排空后重连只会收到新成员状态
// 消息。对端在线的 Remove（活节点成员变更）不在此列——已出队的帧在
// 写中途仍可能送达，属 batch③ 范围。
//
// 幂等：重复调用（多组各自 Remove 同一节点）安全。
func (t *transport) DropPeer(id uint64) {
	q, ok := t.queues[id]
	if !ok {
		return
	}
	for {
		select {
		case <-q:
		default:
			goto drained
		}
	}
drained:
	if d, ok := t.drops[id]; ok {
		d.Store(0)
	}
}

// acceptLoop 接受入站连接并为每连接开一个读 goroutine。
//
// 注意：退出路径不打日志——本循环唯一的退出方式就是 ctx 取消后的
// 监听器关闭（调用方发起的确定性关机），而关机后打日志会与调用方
// 的生命周期结束竞争（测试日志器下会 panic「Log after test done」）。
// 关机的观测由调用方负责（它持有 cancel 的时机）。
func (t *transport) acceptLoop(ctx context.Context) {
	defer t.wg.Done()
	t.lg.Debug("accept 循环启动", "self", t.self, "addr", t.ln.Addr().String())
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return // 看门狗关监听器：正常关机路径，静默退出
			}
			t.lg.Warn("accept 失败，重试", "self", t.self, "err", err)
			// 防瞬态错误（如 fd 耗尽）忙转。
			time.Sleep(100 * time.Millisecond)
			continue
		}
		t.connsMu.Lock()
		if ctx.Err() != nil { // 看门狗已关闭连接：新连接即刻拒收
			t.connsMu.Unlock()
			conn.Close()
			continue
		}
		t.conns[conn] = struct{}{}
		t.connsMu.Unlock()
		t.wg.Add(1)
		go t.readLoop(conn)
	}
}

// sendLoop 对一个 peer 的拨号/发送主循环：拨号成功进入 writeLoop，
// 断线后关连接、退避重拨，直到 ctx 取消。
//
// 注意：退出路径不打日志（同 acceptLoop——唯一退出方式即调用方
// 发起的 ctx 取消，静默退出，关机观测归调用方）。
func (t *transport) sendLoop(ctx context.Context, peerID uint64, addr string) {
	defer t.wg.Done()
	q := t.queues[peerID]
	drops := t.drops[peerID]
	lastDropLog := time.Time{}
	for {
		if ctx.Err() != nil {
			return
		}
		// 对端未起时队列积满会被 Send 丢弃，退避期间周期上报一次。
		t.logDrops(peerID, drops, &lastDropLog)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(redialBackoff):
			}
			continue
		}
		// 出站连接注册进与入站连接同一个 conns 看门狗集合：ctx 取消时
		// 看门狗统一关闭全部连接，解除 writeLoop 中阻塞的 conn.Write——
		// 否则被对端拖住不读的写会无视 ctx 取消一直挂到对端关 socket。
		t.connsMu.Lock()
		if ctx.Err() != nil { // 拨号成功但恰逢取消：不入集合，直接丢弃
			t.connsMu.Unlock()
			conn.Close()
			return
		}
		t.conns[conn] = struct{}{}
		t.connsMu.Unlock()
		if ctx.Err() == nil { // 恰逢取消时跳过上报（关机后不打日志）
			t.lg.Info("peer 已连接", "self", t.self, "peer", peerID, "addr", addr)
		}
		writeErr := t.writeLoop(ctx, conn, q, peerID, drops, &lastDropLog)
		// 写循环已返回（断线/取消）：连接退出看门狗集合再关闭。
		// 与看门狗的双重 Close 安全（net.Conn Close 幂等）。
		t.unregisterConn(conn)
		conn.Close()
		if ctx.Err() != nil { // 正常关机路径的断开不打 Info（会误导成故障）
			return
		}
		t.lg.Info("peer 断开，退避重连", "self", t.self, "peer", peerID, "addr", addr, "err", writeErr)
		select {
		case <-ctx.Done():
			return
		case <-time.After(redialBackoff):
		}
	}
}

// writeLoop 已连接状态下的写帧循环：从队列取信封 → proto.Marshal →
// 写帧头+帧体。任一写失败即返回，由 sendLoop 负责关连接与重拨。
func (t *transport) writeLoop(ctx context.Context, conn net.Conn, q chan envelope,
	peerID uint64, drops *atomic.Uint64, lastDropLog *time.Time) error {
	buf := make([]byte, 0, 4096) // 帧缓冲复用，避免每帧一次分配
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case env := <-q:
			payload, err := proto.Marshal(env.msg)
			if err != nil {
				// 序列化失败（对正常消息不可达）只影响本条，继续投递后续。
				t.lg.Error("发送消息序列化失败，丢弃", "self", t.self, "peer", peerID, "err", err)
				continue
			}
			buf = encodeFrame(buf, env.group, payload)
			if _, err := conn.Write(buf); err != nil {
				return err
			}
			if ctx.Err() == nil { // 恰逢取消时跳过上报（关机后不打日志）
				t.logDrops(peerID, drops, lastDropLog)
			}
		}
	}
}

// readLoop 单连接的读帧循环：io.ReadFull 读帧长→读帧体→拆组号→
// proto.Unmarshal 到新分配的 &raftpb.Message{} → deliver。
//
// 坏帧（帧长越界、反序列化失败）记 Warn 后退出循环（defer 关连接），
// 等对端重拨——不能让一个坏字节流永久毒化读循环。
//
// 退出时把连接从看门狗集合摘除（终审 R4）：不摘除的话，抖动对端
// （反复连接又断开）会让 conns 无界增长直到 ctx 取消。
func (t *transport) readLoop(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()
	defer t.unregisterConn(conn) // delete-then-close，与 sendLoop 同风格
	remote := conn.RemoteAddr().String()
	header := make([]byte, 4)
	for {
		if _, err := io.ReadFull(conn, header); err != nil {
			return // 对端断开或看门狗关连接：正常退出
		}
		frameLen := binary.BigEndian.Uint32(header)
		if frameLen < 4 || frameLen > maxFrameLen {
			t.lg.Warn("坏帧：帧长越界，断开连接", "remote", remote, "frameLen", frameLen)
			return
		}
		body := make([]byte, int(frameLen)) // 帧体 = 组号 + payload
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		group := binary.BigEndian.Uint32(body[:4])
		if group == ControlGroup {
			// 控制通道帧：不进 raft 消息流。请求/响应是短连接 RPC 的
			// 一次往返——应答写回同一条连接后本循环退出（defer 关连接），
			// 与 raft 流水消息互不交织（见 ControlGroup 注释）。
			t.controlFrame(conn, remote, body)
			return
		}
		msg := &raftpb.Message{}
		if err := proto.Unmarshal(body[4:], msg); err != nil {
			t.lg.Warn("坏帧：反序列化失败，断开连接", "remote", remote, "frameLen", frameLen, "err", err)
			return
		}
		// deliver 可能阻塞（上层归组入队）；这正是 TCP 背压的体现——
		// 读端慢，写端自然阻塞，而非本地丢弃。
		t.deliver(group, msg)
	}
}

// unregisterConn 把连接从看门狗集合中摘除（读/写循环退出时调用）。
// 摘除与关闭的次序按调用方风格（readLoop/sendLoop 都是 delete-then-close）；
// 与看门狗的双重 Close 安全（net.Conn Close 幂等）。
func (t *transport) unregisterConn(conn net.Conn) {
	t.connsMu.Lock()
	delete(t.conns, conn)
	t.connsMu.Unlock()
}

// controlFrame 处理一帧控制通道请求：body = [4B 组号][1B op][payload]。
//
// 生命周期：调 handler → 应答帧（或错误帧）写回**同一条连接** →
// 由 readLoop 的 defer 关闭连接。handler 为 nil 时回「控制通道未装配」
// 错误帧——对端节点未配置 ControlHandler 属装配错配，Warn 供排查。
// handler 错误同样回错误帧：应答 payload 首字节 1 + UTF-8 错误文本
// （布局见 ControlGroup 注释），对端 Manager.Control 会把它还原成
// 错误返回。
func (t *transport) controlFrame(conn net.Conn, remote string, body []byte) {
	if len(body) < 5 { // 组号 4B + op 1B，无 op 字节即坏帧
		t.lg.Warn("坏帧：控制帧缺 op 字节，断开连接", "remote", remote, "frameLen", len(body)+4)
		return
	}
	op := body[4]
	payload := body[5:]
	t.lg.Debug("控制请求到达", "remote", remote, "op", op, "len", len(payload))
	handler := t.control
	if handler == nil {
		handler = func(byte, []byte) ([]byte, error) {
			return nil, errControlUnassembled
		}
	}
	resp, err := handler(op, payload)
	if err != nil {
		t.lg.Warn("控制请求处理失败", "remote", remote, "op", op, "err", err)
	}
	// 响应帧：op 最高位置 1 表响应；payload 首字节 0=成功 / 1=失败。
	// 失败时余下 payload 为错误文本，对端 Manager.Control 原样还原。
	respOp := op | 0x80
	statusPayload := make([]byte, 0, 1+len(resp))
	if err != nil {
		statusPayload = append(append(statusPayload, 1), []byte(err.Error())...)
	} else {
		statusPayload = append(statusPayload, 0)
		statusPayload = append(statusPayload, resp...)
	}
	ctrl := append([]byte{respOp}, statusPayload...)
	if _, werr := conn.Write(encodeFrame(nil, ControlGroup, ctrl)); werr != nil {
		// 写失败不重试：对端已断开/超时，重写无意义，短连接生命周期结束
		t.lg.Warn("控制应答写出失败", "remote", remote, "op", op, "err", werr)
	}
}

// errControlUnassembled 是 ControlHandler 未装配时的应答错误文本：
// 对端 Manager.Control 会把「控制通道未装配」原样带回调用方，双方
// 日志可相互印证。
var errControlUnassembled = errors.New("控制通道未装配")

// encodeFrame 组装一帧：[4B 帧长][4B 组号][payload]，帧长 = 4+len(payload)。
// buf 为可复用缓冲，返回组装后的帧切片。
func encodeFrame(buf []byte, group uint32, payload []byte) []byte {
	buf = buf[:0]
	buf = binary.BigEndian.AppendUint32(buf, uint32(4+len(payload)))
	buf = binary.BigEndian.AppendUint32(buf, group)
	buf = append(buf, payload...)
	return buf
}

// logDrops 周期上报队列满丢弃计数：丢弃点是高频热路径，逐条 Debug 会
// 刷屏，必须带计数累积、最多每秒一条（instrumenting-code 热循环规则）。
// 仅在发送 goroutine 内调用（lastDropLog 无锁）。
func (t *transport) logDrops(peerID uint64, drops *atomic.Uint64, lastDropLog *time.Time) {
	c := drops.Load()
	if c == 0 || time.Since(*lastDropLog) < time.Second {
		return
	}
	t.lg.Debug("队列满丢弃消息", "self", t.self, "peer", peerID, "dropped", c)
	*lastDropLog = time.Now()
}
