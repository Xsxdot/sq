// Package raftshell 是 V2 复制层的 spike：用 go.etcd.io/raft/v3 搭最小
// 三节点原型，实测选举耗时、两档刷盘吞吐、learner 追齐速度，数据回填
// V2 spec §2.3/§6。
//
// 职责：raft 薄壳的可行性验证与参数标定。
// 边界：不是生产代码——不做快照压缩、不做成员持久化、不接入 sq 主模块；
// 独立 go module，依赖不进主模块图。
package raftshell

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// AckMode 决定提案确认的持久化语义。
//
// AckQuorumFsync：每条日志在 commit 确认前必须带 fsync 落盘（最保守）；
// AckQuorumMem：日志 NoSync 写入，quorum 确认后由后台批量 fsync 兜底
// （spec §2.2 要实测的取舍档）。
type AckMode int

const (
	AckQuorumFsync AckMode = iota
	AckQuorumMem
)

// Node 是一个 raft 节点薄壳：tick 驱动选举/心跳，Ready 循环执行
// 「持久化 → 发送 → apply → Advance」契约，FSM 为计数器 + 提案回执表。
type Node struct {
	id      uint64
	rn      raft.Node
	storage *raft.MemoryStorage // raft 库要求的易失存储视图
	wal     *WAL                // 我们自己的持久化（pebble），两档刷盘在此分流
	tr      Transport
	mode    AckMode
	lg      *slog.Logger

	// v3 的 raftpb.Message 是 protobuf-go v2 生成，内部嵌入互斥锁
	// （protoimpl.MessageState）。消息全链路用指针传递，避免按值拷贝
	// 触发 vet copylocks 检查，也省一次拷贝开销。
	inbox   chan *raftpb.Message
	applied atomic.Uint64
	leader  atomic.Uint64

	done chan struct{} // run 循环完全退出（含 WAL 关闭）后关闭，测试/调用方同步用
	// cancel 是 Start 派生出的运行上下文取消函数，StopClean（干净关机）
	// 通过它退出 run 循环；为 nil 表示 Start 尚未被调用。
	cancel context.CancelFunc

	mu      sync.Mutex
	waiters map[uint64]chan struct{} // 提案 id → commit 通知；id 编码在 payload 前 8 字节
	nextID  atomic.Uint64
}

// NewNode 构造并启动底层 raft 节点（内部 raft 循环由 Start 驱动）。
//
// 参数：
//   - id: 本节点 ID，必须与 peers 中某项一致
//   - peers: 初始成员表（单节点测试传自身即可）
//   - dir: 本节点 pebble WAL 目录
//   - mode: 刷盘档位，见 AckMode
//   - tr: 消息传输层（chanTransport 或外部实现）
//   - lg: 结构化日志
//
// 返回：
//   - 就绪的 *Node（未启动）
//   - 错误信息（WAL 打开或 raft 初始化失败）
func NewNode(id uint64, peers []raft.Peer, dir string, mode AckMode, tr Transport, lg *slog.Logger) (*Node, error) {
	if lg == nil {
		lg = slog.Default()
	}
	wal, err := openWAL(dir, mode, lg)
	if err != nil {
		return nil, err
	}
	storage := raft.NewMemoryStorage()
	n := newNode(id, raft.StartNode(raftConfig(id, storage), peers), storage, wal, tr, mode, lg)
	lg.Info("节点初始化完成", "id", id, "mode", mode.String(), "peers", len(peers))
	return n, nil
}

// raftConfig 构造共享的 raft 配置：tick 参数、单消息/在途日志上限、
// 丢弃日志器（raft 库自身日志噪音大，关键节点由我们的 slog 承担）。
func raftConfig(id uint64, storage *raft.MemoryStorage) *raft.Config {
	return &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		Logger:          &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	}
}

// newNode 组装 Node 结构（不启动 run 循环）。
// NewNode（StartNode 初始引导）与 Cluster.Restart 的两种恢复路径
// （RestartNode 原身份回归 / 空存储 learner 重入）共用。
func newNode(id uint64, rn raft.Node, storage *raft.MemoryStorage, wal *WAL, tr Transport, mode AckMode, lg *slog.Logger) *Node {
	return &Node{
		id:      id,
		rn:      rn,
		storage: storage,
		wal:     wal,
		tr:      tr,
		mode:    mode,
		lg:      lg,
		inbox:   make(chan *raftpb.Message, 1024),
		waiters: make(map[uint64]chan struct{}),
		done:    make(chan struct{}),
	}
}

// Start 启动节点的后台循环（tick、消息接收、Ready 处理），不阻塞。
// ctx 取消时节点退出并关闭 WAL。
func (n *Node) Start(ctx context.Context) {
	// 从调用方 ctx 派生：CancelFunc 保存在节点上，StopClean 干净关机可用
	ctx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	n.lg.Info("节点启动", "id", n.id)
	go n.run(ctx)
}

// StopClean 干净关机：先写持久化的干净关机标记（Sync 落盘），再取消
// 节点运行。重启时 Restart 读到标记即走原身份 RestartNode 回归。
//
// 参数：
//   - ctx: 保留以与 Propose 等接口签名一致；标记写入与退出不依赖它
//
// 注意：
//   - 必须在 Start 之后调用（需要 cancel）
//   - 标记写入同步完成且先于 cancel：run 循环退出时 WAL Close 不会早于标记落盘
//   - 标记写失败按「不干净关机」处理（等同无标记），不阻塞退出
func (n *Node) StopClean(ctx context.Context) {
	if err := n.wal.MarkCleanShutdown(); err != nil {
		// 标记未落盘：下次重启将走 learner 重入路径，等同于断电，
		// 属于安全退化而非数据丢失
		n.lg.Error("写干净关机标记失败，按不干净关机处理", "id", n.id, "err", err)
	}
	n.lg.Info("干净关机标记已写入，退出", "id", n.id)
	n.cancel()
}

func (n *Node) run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond) // ElectionTick=10 → 选举超时约 1s
	defer ticker.Stop()
	// 先注册 close(done) 再注册 wal.Close：LIFO 保证 done 在 WAL 关闭之后才关闭，
	// 即 Done() 可观察到的「完全退出」包含持久化层清理。
	defer close(n.done)
	defer n.wal.Close()
	for {
		select {
		case <-ctx.Done():
			n.lg.Info("节点退出", "id", n.id)
			return
		case <-ticker.C:
			n.rn.Tick()
		case m := <-n.inbox:
			_ = n.rn.Step(ctx, m)
		case rd := <-n.rn.Ready():
			n.handleReady(ctx, rd)
		}
	}
}

// handleReady 执行 etcd/raft 的 Ready 契约。关键顺序（正确性所在）：
//  1. Entries/HardState 先持久化（AckQuorumFsync 档带 fsync）再发送 Messages——
//     否则本节点确认过的日志可能在崩溃后消失，违反 raft 假设；
//  2. AckQuorumMem 档刻意放松第 1 条（NoSync 落盘 + 后台周期 fsync），
//     这正是 spec §2.2 要实测的取舍，配套规则见 Task 5；
//  3. CommittedEntries apply 后才 Advance。
func (n *Node) handleReady(ctx context.Context, rd raft.Ready) {
	if err := n.wal.Persist(rd.HardState, rd.Entries, n.mode == AckQuorumFsync); err != nil {
		n.lg.Error("持久化失败，节点停摆", "id", n.id, "err", err)
		return
	}
	// MemoryStorage 是 raft 库读取日志的视图，必须与 WAL 同步推进
	if !raft.IsEmptyHardState(rd.HardState) {
		_ = n.storage.SetHardState(rd.HardState)
	}
	_ = n.storage.Append(rd.Entries)
	// raft v3 的 Ready.Messages 本就是指针切片，Transport 契约同为指针，
	// 直接透传，无拷贝
	n.tr.Send(n.id, rd.Messages)
	if rd.SoftState != nil {
		n.leader.Store(rd.SoftState.Lead)
		n.lg.Info("leader 变更", "id", n.id, "lead", rd.SoftState.Lead)
	}
	for _, ent := range rd.CommittedEntries {
		// v3 的 proposal 条目 Type 可为 nil，GetType() 会把 nil 归零为 EntryNormal
		switch ent.GetType() {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Reset()
			// v3 的 raftpb 是 protobuf-go v2 生成：需要显式 Unmarshal
			_ = proto.Unmarshal(ent.Data, &cc)
			n.rn.ApplyConfChange(&cc)
			// 按 ConfChange.Id 通知 ProposeConfChange 的等待方（见该函数）
			n.notify(cc.GetId())
			n.lg.Info("成员变更已 apply", "id", n.id, "type", cc.GetType().String(), "node", cc.GetNodeId())
		case raftpb.EntryNormal:
			if len(ent.Data) >= 8 {
				n.applied.Add(1)
				n.notify(binary.BigEndian.Uint64(ent.Data[:8]))
			}
		}
	}
	n.rn.Advance()
}

// Propose 提交一条提案并阻塞直到它被 commit 且 apply 到本节点 FSM。
//
// 实现：分配自增提案 id → 注册 waiter → 把 id（大端 8 字节）前置到
// payload 前 8 字节 → rn.Propose → 等 waiter 通知或 ctx 超时。
//
// 返回：
//   - nil：条目已 apply
//   - error：Propose 失败或 ctx 超时/取消（超时后 waiter 会被删除，防止泄漏）
func (n *Node) Propose(ctx context.Context, payload []byte) error {
	id := n.nextID.Add(1)
	ch := make(chan struct{})
	n.mu.Lock()
	n.waiters[id] = ch
	n.mu.Unlock()

	// 提案 id 编码在 payload 前 8 字节，apply 时据此回调对应 waiter
	data := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint64(data, id)
	copy(data[8:], payload)

	if err := n.rn.Propose(ctx, data); err != nil {
		n.removeWaiter(id)
		return err
	}
	select {
	case <-ch:
		n.lg.Debug("提案已 apply", "id", n.id, "proposal", id)
		return nil
	case <-ctx.Done():
		// 超时：条目可能仍会被提交，但调用方已放弃等待，
		// 删除 waiter 防泄漏；后续 apply 时 notify 找不到它即可。
		n.removeWaiter(id)
		return ctx.Err()
	}
}

// ProposeConfChange 提出一条成员变更并阻塞直到它被 commit 且 apply
// 到本节点 FSM。
//
// 实现：复用提案 waiter 机制——ConfChange.Id 充当提案 id（raft 库原样
// 把它编进 EntryConfChange 的 Data），apply 时按 Id 通知等待方，
// 见 handleReady 的 EntryConfChange 分支。
//
// 参数：
//   - ctx: 控制等待；超时/取消后 waiter 被移除（条目可能仍会被提交）
//   - ccType: 变更类型（ConfChangeAddNode/ConfChangeRemoveNode/
//     ConfChangeAddLearnerNode）
//   - nodeID: 变更目标节点 ID
//
// 返回：
//   - nil：成员变更已 apply
//   - error：ProposeConfChange 失败或 ctx 超时/取消
func (n *Node) ProposeConfChange(ctx context.Context, ccType raftpb.ConfChangeType, nodeID uint64) error {
	id := n.nextID.Add(1)
	ch := make(chan struct{})
	n.mu.Lock()
	n.waiters[id] = ch
	n.mu.Unlock()

	// ConfChange 的标量字段在 protobuf-go v2 开放结构下是指针，取址传参；
	// ProposeConfChange 要求 ConfChangeI 接口（AsV1 为指针接收者），传指针
	cc := raftpb.ConfChange{Type: ccType.Enum(), NodeId: &nodeID, Id: &id}
	if err := n.rn.ProposeConfChange(ctx, &cc); err != nil {
		n.removeWaiter(id)
		return err
	}
	select {
	case <-ch:
		n.lg.Debug("成员变更已 apply", "id", n.id, "cc", ccType.String(), "node", nodeID)
		return nil
	case <-ctx.Done():
		n.removeWaiter(id)
		return ctx.Err()
	}
}

// Step 是 transport 的消息投递入口，投递给底层 raft 节点（异步入队）。
//
// 注意：m 必须为指针——v3 的 raftpb.Message 内嵌互斥锁
// （protoimpl.MessageState），按值传递会触发 vet copylocks，
// 且与 raft 库 rn.Step 的指针签名天然一致。
func (n *Node) Step(m *raftpb.Message) {
	n.inbox <- m
}

// IsLeader 返回本节点当前是否为 leader。
func (n *Node) IsLeader() bool {
	return n.leader.Load() == n.id
}

// LeaderID 返回当前 leader 的节点 ID（尚未选举完成时为 0）。
func (n *Node) LeaderID() uint64 {
	return n.leader.Load()
}

// AppliedCount 返回已 apply 到 FSM 的普通条目数（测试对账用）。
func (n *Node) AppliedCount() uint64 {
	return n.applied.Load()
}

// Done 返回一个在节点完全退出（run 循环结束、WAL 已关闭）后关闭的 channel。
// 调用方可用它同步等待 ctx 取消后的清理完成。
func (n *Node) Done() <-chan struct{} {
	return n.done
}

// notify 通知并移除对应提案的 waiter；不存在时静默（超时已移除）。
func (n *Node) notify(id uint64) {
	n.mu.Lock()
	ch, ok := n.waiters[id]
	if ok {
		delete(n.waiters, id)
	}
	n.mu.Unlock()
	if ok {
		close(ch)
	}
}

// removeWaiter 删除 waiter 但不去关闭 channel（调用方已放弃等待）。
func (n *Node) removeWaiter(id uint64) {
	n.mu.Lock()
	delete(n.waiters, id)
	n.mu.Unlock()
}

// String 返回 AckMode 的可读名称。
func (m AckMode) String() string {
	if m == AckQuorumMem {
		return "ack-quorum-mem"
	}
	return "ack-quorum-fsync"
}
