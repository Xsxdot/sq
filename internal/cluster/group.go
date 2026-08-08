// group.go 提供单组运行体：tick 驱动、Ready 四步契约、真实 FSM apply
// 与 waiter 双命名空间。
//
// 职责：
//   - 驱动单个 raft 组的生命周期：tick、消息步进、Ready 循环
//   - propose/proposeConfChange 阻塞至条目在本节点 apply 完成（读己之写）
//   - applied 位点与 FSM 数据同批原子写入共享 store（spec §5）
//   - 按确认档位决定日志持久化是否带 fsync
//
// 边界：
//   - 不管组间路由与成员编排——Manager 的事（Task 5 组装）
//   - 不做快照与日志截断——batch④，日志无界增长、追齐走全量重放
//   - AckQuorumMem 的后台批量 fsync 不在本层——全组共享一条 WAL，
//     一个 flusher 即可，由 Manager 持有
//   - 传输层生命周期（拨号/断线/关闭日志）归属 Manager 层（Task 3 约定）
package cluster

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

	"github.com/xushixin/sq/internal/store"
)

// AckMode 决定提案确认的持久化语义。
//
// AckQuorumFsync：每条日志在 commit 确认前必须带 fsync 落盘（最保守）；
// AckQuorumMem：日志 NoSync 写入，quorum 确认后由 Manager 层后台
// 批量 fsync 兜底（spec §2.2 要实测的取舍档）。
type AckMode int

const (
	AckQuorumFsync AckMode = iota
	AckQuorumMem
)

// String 返回 AckMode 的可读名称。
func (m AckMode) String() string {
	if m == AckQuorumMem {
		return "ack-quorum-mem"
	}
	return "ack-quorum-fsync"
}

// group 是一个 raft 组运行体：tick 驱动选举/心跳，Ready 循环执行
// 「持久化 → 发送 → apply → Advance」契约，FSM 为共享 store。
type group struct {
	g       uint32
	rn      raft.Node
	storage *raft.MemoryStorage // raft 库读取日志的易失视图
	rs      *raftStore          // 日志的共库持久层
	st      *store.Store        // 共享 store：FSM apply 的唯一落点
	send    func(g uint32, msgs []*raftpb.Message)
	mode    AckMode
	lg      *slog.Logger
	selfID  uint64 // 本节点 ID：构造时从 rn.Status() 取一次，isLeader 比较用

	// v3 的 raftpb.Message 是 protobuf-go v2 生成，内部嵌入互斥锁
	// （protoimpl.MessageState）。消息全链路用指针传递，避免按值拷贝
	// 触发 vet copylocks 检查，也省一次拷贝开销。
	inbox   chan *raftpb.Message
	applied atomic.Uint64 // 已 apply 的最高条目 index（重启重放幂等的基础）
	lead    atomic.Uint64 // 当前 leader 节点 ID，SoftState 变化时更新

	doneCh chan struct{} // run 循环完全退出后关闭，测试/调用方同步用

	// waiter 双命名空间（终审观察项①）：普通提案与成员变更的 id 共用
	// 同一个 nextID 计数器，但 apply 时 EntryNormal 只通知 propWaiters、
	// EntryConfChange 只通知 ccWaiters——id 相同也不会交叉误唤。
	mu          sync.Mutex
	propWaiters map[uint64]chan struct{}
	ccWaiters   map[uint64]chan struct{}
	nextID      atomic.Uint64
}

// newGroup 构造单组运行体（不启动 run 循环）。
//
// 参数：
//   - g: 数据组号
//   - rn: 已用 StartNode 启动的 raft 节点
//   - storage: raft 库要求的易失存储视图（重启恢复时由 Manager 回放日志）
//   - rs: 日志持久层（同一 store 库）
//   - st: 共享 store，FSM apply 的唯一落点
//   - send: 消息外发回调（Manager 里接 transport 并短路本节点，Task 5）
//   - mode: 确认档位，见 AckMode
//   - lg: 结构化日志（nil 时退化为 slog.Default）
//
// 返回：
//   - 就绪的 *group（未启动）
func newGroup(g uint32, rn raft.Node, storage *raft.MemoryStorage, rs *raftStore, st *store.Store, send func(g uint32, msgs []*raftpb.Message), mode AckMode, lg *slog.Logger) *group {
	if lg == nil {
		lg = slog.Default()
	}
	return &group{
		g:           g,
		rn:          rn,
		storage:     storage,
		rs:          rs,
		st:          st,
		send:        send,
		mode:        mode,
		lg:          lg.With("mod", "group", "g", g),
		selfID:      rn.Status().ID,
		inbox:       make(chan *raftpb.Message, 1024),
		propWaiters: make(map[uint64]chan struct{}),
		ccWaiters:   make(map[uint64]chan struct{}),
		doneCh:      make(chan struct{}),
	}
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

// run 驱动组循环：tick 驱动选举/心跳、消息步进、Ready 处理，
// 直至 ctx 取消退出。
//
// 注意：tick 与心跳是高频路径，本循环内零日志（热循环规则）；
// 关键节点日志全部落在 handleReady/propose 等低频路径上。
func (gr *group) run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond) // ElectionTick=10 → 选举超时约 1s
	defer ticker.Stop()
	defer close(gr.doneCh)
	for {
		select {
		case <-ctx.Done():
			gr.lg.Info("组退出")
			return
		case <-ticker.C:
			gr.rn.Tick()
		case m := <-gr.inbox:
			_ = gr.rn.Step(ctx, m)
		case rd := <-gr.rn.Ready():
			gr.handleReady(ctx, rd)
		}
	}
}

// handleReady 执行 etcd/raft 的 Ready 四步契约。关键顺序（正确性所在）：
//  1. Entries/HardState 先持久化（quorum-fsync 档带 fsync）再发送 Messages——
//     否则本节点确认过的日志可能在崩溃后消失，违反 raft 假设；
//  2. 发送 Messages；
//  3. CommittedEntries apply（FSM 数据与 applied 位点同批原子）后才 Advance；
//  4. Advance——只有在此之后 raft 库才认为本轮处理完毕，继续产下一轮 Ready。
//
// AckQuorumMem 档刻意放松第 1 步的 fsync：NoSync 落盘 + Manager 层后台
// 周期批量 fsync 兜底，这正是 spec §2.2 要实测的取舍，配套规则见 Task 5。
func (gr *group) handleReady(ctx context.Context, rd raft.Ready) {
	// 1. 持久化：HardState + Entries 单批原子（rs.Persist），sync 与否
	//    由确认档位逐轮判定；MemoryStorage 是 raft 库读取日志的视图，
	//    必须与持久层同步推进（双记账）。
	if err := gr.rs.Persist(gr.g, rd.HardState, rd.Entries, gr.syncPersist(rd.HardState, rd.Entries)); err != nil {
		// 持久化失败：内存日志与磁盘分叉，崩溃后已确认的条目会消失，
		// 继续跑没有任何可恢复路径，只能停摆等上层接管
		gr.lg.Error("日志持久化失败，组停摆", "err", err)
		return
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		_ = gr.storage.SetHardState(rd.HardState)
	}
	_ = gr.storage.Append(rd.Entries)
	// 2. 发送 Messages：经注入的 send 回调外发（transport 发送永不
	//    阻塞——满则丢，raft 心跳重试兜底，Task 3 契约）
	gr.send(gr.g, rd.Messages)
	// leader 变更观测：SoftState 变化是切换的第一信号（当选与失联都在此）
	if rd.SoftState != nil {
		gr.lead.Store(rd.SoftState.Lead)
		gr.lg.Info("组 leader 变更", "lead", rd.SoftState.Lead, "term", rd.HardState.GetTerm())
	}
	// 3. CommittedEntries apply
	for _, ent := range rd.CommittedEntries {
		// 重启重放的幂等保证：raft 可能重发已 apply 过的条目
		// （conflict 回退重写后），跳过即可——FSM 已是该 index 的状态
		if ent.GetIndex() <= gr.applied.Load() {
			continue
		}
		switch ent.GetType() {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Reset()
			// v3 的 raftpb 是 protobuf-go v2 生成：需要显式 Unmarshal
			if err := proto.Unmarshal(ent.Data, &cc); err != nil {
				gr.lg.Error("ConfChange 解码失败，组停摆", "index", ent.GetIndex(), "err", err)
				panic(err)
			}
			gr.rn.ApplyConfChange(&cc)
			// 按 ConfChange.Id 通知 ccWaiters——独立命名空间，
			// 同 id 的普通提案 waiter 不会被误唤
			gr.notifyWaiter(gr.ccWaiters, cc.GetId())
			gr.applied.Store(ent.GetIndex())
			gr.lg.Debug("成员变更已 apply", "type", cc.GetType().String(), "node", cc.GetNodeId())
		case raftpb.EntryNormal:
			// 条目数据布局：[8B waiter id][batch repr]——apply 时
			// 跳过前 8 字节取批次载荷
			var data []byte
			if len(ent.Data) > 8 {
				data = ent.Data[8:]
			}
			gr.applyEntry(ent.GetIndex(), data)
			if len(ent.Data) >= 8 {
				gr.notifyWaiter(gr.propWaiters, binary.BigEndian.Uint64(ent.Data[:8]))
			}
		}
	}
	// 4. Advance——raft 库据此确认本轮 Ready 已处理，继续产下一轮
	gr.rn.Advance()
}

// syncPersist 判定本轮 Ready 持久化是否带 fsync。
//
// quorum-fsync 档且本轮确有落盘内容（有条目或 HardState 非空）时才
// Sync——无事可写时白刷一次盘没有意义；quorum-mem 档永不 Sync
// （NoSync 落盘 + Manager 层后台批量 fsync 兜底）。
func (gr *group) syncPersist(hs *raftpb.HardState, ents []*raftpb.Entry) bool {
	return gr.mode == AckQuorumFsync && (len(ents) > 0 || !raft.IsEmptyHardState(hs))
}

// applyEntry 把一条普通条目应用到 FSM：applied 位点与 FSM 数据在
// 同一批次内原子写入共享 store（spec §5）。data 是条目载荷
// （waiter id 之后的部分，即原始批次字节）。
//
// 空/短条目（选举产生的空条目等）没有 FSM 数据，只写 applied 位点
// （单键批次）。
//
// apply 失败是不可恢复错误：状态机与日志分叉比进程死亡更糟——分叉后
// 本节点 FSM 与多数派永远不一致且无法被检测修复，重启重放只会回到
// 与日志一致的状态；直接 panic 让进程死亡、由上层重启接管才是安全
// 的选择。这是刻意取舍，不是疏漏。
func (gr *group) applyEntry(index uint64, data []byte) {
	var b *store.Batch
	var err error
	if len(data) > 0 {
		// 批次字节重建：坏字节在此报错——apply 路径不允许任何静默降级
		b, err = gr.st.NewBatchFromRepr(data)
	} else {
		b = gr.st.NewBatch()
	}
	if err != nil {
		gr.lg.Error("FSM 批次重建失败，组停摆", "index", index, "err", err)
		panic(err)
	}
	if err := b.Set(appliedKey(gr.g), store.PutU64(index)); err != nil {
		b.Close()
		gr.lg.Error("写 applied 位点失败，组停摆", "index", index, "err", err)
		panic(err)
	}
	// FSM 数据与 applied 位点同批提交。apply 总是 NoSync：持久性由
	// raft 日志与后台批量刷盘承担（spec §5），不另起 fsync。
	if err := gr.st.ApplyWith(b, false); err != nil {
		gr.lg.Error("FSM apply 失败，组停摆", "index", index, "err", err)
		panic(err)
	}
	gr.applied.Store(index)
}

// propose 提交一条提案并阻塞直到它在本节点 apply 完成。
//
// 等 apply 而非等 commit（读己之写）：propose 返回后调用方立即可读
// 自己写入的数据；commit 只代表多数派确认，本节点 FSM 可能尚未追上，
// 只等 commit 会让「写入后立即可读」落空。
//
// 实现：分配自增提案 id → 注册 waiter（propWaiters）→ id（大端 8B）
// 前置到批次字节前 → rn.Propose → 等 waiter 通知或 ctx 超时。
//
// 参数：
//   - ctx: 控制等待；超时/取消后 waiter 被移除（条目可能仍会被提交，
//     调用方已放弃等待，apply 时 notify 找不到它即可）
//   - batchRepr: store.Batch.Repr() 的物理字节（提案载荷）
//
// 返回：
//   - nil：条目已 apply
//   - error：Propose 失败或 ctx 超时/取消（调用方带上下文处理）
func (gr *group) propose(ctx context.Context, batchRepr []byte) error {
	id := gr.nextID.Add(1)
	ch := make(chan struct{})
	gr.mu.Lock()
	gr.propWaiters[id] = ch
	gr.mu.Unlock()

	// 提案 id 编码在载荷前 8 字节，apply 时据此回调对应 waiter
	data := make([]byte, 8+len(batchRepr))
	binary.BigEndian.PutUint64(data, id)
	copy(data[8:], batchRepr)

	if err := gr.rn.Propose(ctx, data); err != nil {
		gr.removeWaiter(gr.propWaiters, id)
		return err
	}
	select {
	case <-ch:
		gr.lg.Debug("提案已 apply", "id", id)
		return nil
	case <-ctx.Done():
		// 超时：条目可能仍会被提交，但调用方已放弃等待；删除
		// waiter 防泄漏（测试断言超时后 map 为空），后续 apply 时
		// notify 找不到它即可
		gr.removeWaiter(gr.propWaiters, id)
		gr.lg.Debug("propose 等待超时", "id", id, "err", ctx.Err())
		return ctx.Err()
	}
}

// proposeConfChange 提出一条成员变更并阻塞直到它被 apply 到本节点。
//
// waiter 走独立命名空间（ccWaiters）：普通提案与成员变更的 id 共用
// 同一个 nextID 计数器，但 apply 时 EntryNormal 只通知 propWaiters、
// EntryConfChange 只通知 ccWaiters——id 相同也不会交叉误唤
// （终审观察项①）。
//
// 参数：
//   - ctx: 控制等待；超时/取消后 waiter 被移除（条目可能仍会被提交）
//   - typ: 变更类型（ConfChangeAddNode/ConfChangeRemoveNode/
//     ConfChangeAddLearnerNode）
//   - nodeID: 变更目标节点 ID
//
// 返回：
//   - nil：成员变更已 apply
//   - error：ProposeConfChange 失败或 ctx 超时/取消
func (gr *group) proposeConfChange(ctx context.Context, typ raftpb.ConfChangeType, nodeID uint64) error {
	// 提案 id 与 ConfChange.Id 共用同一个 nextID 计数器：单节点内
	// 原子自增不可能碰撞；双命名空间下同一 id 跨条目种类也不会误唤
	id := gr.nextID.Add(1)
	ch := make(chan struct{})
	gr.mu.Lock()
	gr.ccWaiters[id] = ch
	gr.mu.Unlock()

	// ConfChange 的标量字段在 protobuf-go v2 开放结构下是指针，取址传参；
	// ProposeConfChange 要求 ConfChangeI 接口（AsV1 为指针接收者），传指针
	cc := raftpb.ConfChange{Type: typ.Enum(), NodeId: &nodeID, Id: &id}
	if err := gr.rn.ProposeConfChange(ctx, &cc); err != nil {
		gr.removeWaiter(gr.ccWaiters, id)
		return err
	}
	select {
	case <-ch:
		gr.lg.Debug("成员变更已 apply", "cc", typ.String(), "node", nodeID)
		return nil
	case <-ctx.Done():
		gr.removeWaiter(gr.ccWaiters, id)
		gr.lg.Debug("成员变更等待超时", "id", id, "err", ctx.Err())
		return ctx.Err()
	}
}

// step 是消息投递入口（transport 读帧 goroutine 调用），把 raft 消息
// 交给本组处理。异步入队由 run 循环单 goroutine 消费，tick 与 Step
// 因此不会竞争。
//
// 注意：m 必须为指针——v3 的 raftpb.Message 内嵌互斥锁
// （protoimpl.MessageState），按值传递会触发 vet copylocks，
// 且与 rn.Step 的指针签名天然一致。
func (gr *group) step(m *raftpb.Message) {
	gr.inbox <- m
}

// leader 返回当前 leader 的节点 ID（尚未选举完成时为 0）。
func (gr *group) leader() uint64 {
	return gr.lead.Load()
}

// isLeader 返回本节点当前是否为 leader。
func (gr *group) isLeader() bool {
	return gr.lead.Load() == gr.selfID
}

// appliedIndex 返回已 apply 到 FSM 的最高条目 index。
// 重启后由 Manager 从磁盘 applied 位点初始化内存值。
func (gr *group) appliedIndex() uint64 {
	return gr.applied.Load()
}

// done 返回一个在组完全退出（run 循环结束）后关闭的 channel。
// 调用方可用它同步等待 ctx 取消后的清理完成。
func (gr *group) done() <-chan struct{} {
	return gr.doneCh
}

// notifyWaiter 通知并移除指定命名空间中的 waiter；不存在时静默
// （超时已移除）。
func (gr *group) notifyWaiter(m map[uint64]chan struct{}, id uint64) {
	gr.mu.Lock()
	ch, ok := m[id]
	if ok {
		delete(m, id)
	}
	gr.mu.Unlock()
	if ok {
		close(ch)
	}
}

// removeWaiter 删除 waiter 但不去关闭 channel（调用方已放弃等待）。
func (gr *group) removeWaiter(m map[uint64]chan struct{}, id uint64) {
	gr.mu.Lock()
	delete(m, id)
	gr.mu.Unlock()
}
