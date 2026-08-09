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
	"errors"
	"fmt"
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

// ErrNotLeader 表示本节点不是指定组的当前 leader，提案被拒绝。
//
// 协议面据此翻译成客户端可重试码（Task 9 的 Code_HA_NOT_AVAILABLE）：
// 收到本错误后应先经 Manager.Leader(g) 找到当前 leader 再重试，而不是
// 原地死等。可被 errors.Is 识别。
var ErrNotLeader = errors.New("cluster: 本节点不是该组 leader，提案被拒绝")

// group 是一个 raft 组运行体：tick 驱动选举/心跳，Ready 循环执行
// 「持久化 → 发送 → apply → Advance」契约，FSM 为共享 store。
type group struct {
	g  uint32
	rn raft.Node // 由装配方在 newGroup 之后创建并赋值（Config.Storage 要包了快照生成器的 stg）
	// 日志存储双记账（Task 4）：mem 是 raft 库读写日志的易失视图
	// （Append/SetHardState/Compact），stg 是包在 mem 外的 raft.Storage
	// 包装——raft 经它读日志，Snapshot() 由 groupStorage 现场生成
	mem    *raft.MemoryStorage
	stg    raft.Storage
	rs     *raftStore   // 日志的共库持久层
	st     *store.Store // 共享 store：FSM apply 的唯一落点
	send   func(g uint32, msgs []*raftpb.Message)
	mode   AckMode
	lg     *slog.Logger
	selfID uint64 // 本节点 ID：newGroup 装配参数，isLeader 比较与快照描述符 leader 字段用

	// 装配钩子（nil 安全，见 notifyLeaderChange/notifyApplied 的契约注释）：
	// 在 Ready 循环内同步触发，不得阻塞；重活由装配代码自行 dispatch。
	onLeaderChange func(g uint32, leader uint64, isSelf bool)
	onApplied      func(g uint32, repr []byte)

	// v3 的 raftpb.Message 是 protobuf-go v2 生成，内部嵌入互斥锁
	// （protoimpl.MessageState）。消息全链路用指针传递，避免按值拷贝
	// 触发 vet copylocks 检查，也省一次拷贝开销。
	inbox   chan *raftpb.Message
	applied atomic.Uint64 // 已 apply 的最高条目 index（重启重放幂等的基础）
	lead    atomic.Uint64 // 当前 leader 节点 ID，SoftState 变化时更新

	// confState 是当前成员表（rn.ApplyConfChange 的返回值，ConfChange
	// apply 时同步更新）。重启时由 newGroup 从持久化值初始化（见
	// newGroup）；Task 4 的 Storage.Snapshot() 现场取用它生成快照。
	confState atomic.Pointer[raftpb.ConfState]

	// applyMu 串行化「写 FSM 批次 + 推 applied」临界区与快照生成
	// （Task 4）：groupStorage.Snapshot() 在同一把锁内读 applied 与建
	// ReadView，视图内容恰好对应该位点。持锁时间必须短——只覆盖批
	// 提交 + Store(applied)，apply 路径绝不可在锁内阻塞等待。
	applyMu sync.Mutex

	doneCh chan struct{} // run 循环完全退出后关闭，测试/调用方同步用

	// waiter 双命名空间（终审观察项①）：普通提案与成员变更的 id 共用
	// 同一个 nextID 计数器，但 apply 时 EntryNormal 只通知 propWaiters、
	// EntryConfChange 只通知 ccWaiters——id 相同也不会交叉误唤。
	// 终审 R4：通知还带提案者作用域——条目头/Context 携带提案者身份，
	// 只有本节点发起的条目才唤醒 waiter（跨节点 id 碰撞不得假成功）。
	mu          sync.Mutex
	propWaiters map[uint64]chan struct{}
	ccWaiters   map[uint64]chan struct{}
	nextID      atomic.Uint64
}

// newGroup 构造单组运行体（不启动 run 循环）。
//
// 参数：
//   - g: 数据组号
//   - selfID: 本节点 ID（装配方传入；也写进快照描述符的 leader 字段）
//   - storage: raft 库要求的易失存储视图（重启恢复时由 Manager 回放日志）
//   - snaps: 快照注册表（Manager 每个实例一份，全组共享；nil 时按默认
//     TTL 自建，单组单元测试路径用）
//   - rs: 日志持久层（同一 store 库）
//   - st: 共享 store，FSM apply 的唯一落点
//   - send: 消息外发回调（Manager 里接 transport 并短路本节点，Task 5）
//   - mode: 确认档位，见 AckMode
//   - onLeaderChange: leader 变更钩子（可选，nil 安全）
//   - onApplied: 条目 apply 钩子（可选，nil 安全）
//   - lg: 结构化日志（nil 时退化为 slog.Default）
//
// 返回：
//   - 就绪的 *group（rn 尚未赋值——装配方须以 gr.stg 建 raft.Config、
//     创建 raft.Node 后回填 gr.rn）
func newGroup(g uint32, selfID uint64, storage *raft.MemoryStorage, snaps *snapRegistry, rs *raftStore, st *store.Store, send func(g uint32, msgs []*raftpb.Message), mode AckMode, onLeaderChange func(g uint32, leader uint64, isSelf bool), onApplied func(g uint32, repr []byte), lg *slog.Logger) *group {
	if lg == nil {
		lg = slog.Default()
	}
	gr := &group{
		g:              g,
		selfID:         selfID,
		mem:            storage,
		rs:             rs,
		st:             st,
		send:           send,
		mode:           mode,
		onLeaderChange: onLeaderChange,
		onApplied:      onApplied,
		lg:             lg.With("mod", "group", "g", g),
		inbox:          make(chan *raftpb.Message, 1024),
		propWaiters:    make(map[uint64]chan struct{}),
		ccWaiters:      make(map[uint64]chan struct{}),
		doneCh:         make(chan struct{}),
	}
	// 提案 id 用时间戳做种子（终审 R4）：干净重启后计数器从远离旧值
	// 的位置继续，配合条目头的提案者校验双保险——旧进程的等待者已随
	// 进程消亡，新进程的 id 空间不得与旧进程重叠（否则重启回零是
	// 跨节点 id 碰撞的第二条路径）。
	gr.nextID.Store(uint64(time.Now().UnixNano()))
	// 成员表从持久化值初始化（Task 2）：重启后 Storage.Snapshot()
	// 现场取用 gr.confState，无需再等第一条 ConfChange apply。
	// rs 为 nil 的单元测试路径跳过；读回失败按不可恢复处理（与
	// 「持久化失败一律 panic」同策略——成员表是选举安全的根）。
	if rs != nil {
		if cs, ok, err := rs.LoadConfState(g); err != nil {
			gr.lg.Error("读回成员表失败", "err", err)
			panic(err)
		} else if ok {
			gr.confState.Store(cs)
		}
	}
	// 快照包装（Task 4）：raft 的 Config.Storage 用包在 mem 外的 stg——
	// raft 给落后 follower 发 MsgSnap 时调用的是 groupStorage.Snapshot()
	// 的现场生成逻辑，而不是 MemoryStorage 的预置快照。stg 引用组内
	// atomics（applied/confState）与 applyMu，只能在本函数末尾、组骨架
	// 就绪后创建；raft.Node 的创建因此推迟到 newGroup 之后（装配方做）。
	reg := snaps
	if reg == nil {
		// 未注入注册表的路径（单组单元测试）：按默认 TTL 自建一个，
		// 保证 Snapshot() 的 reg 永不为空
		reg = newSnapRegistry(gr.st, snapRegistryDefaultTTL, gr.lg)
	}
	gr.stg = newGroupStorage(g, storage, reg, &gr.applied, &gr.confState, gr.selfID, &gr.applyMu, gr.lg)
	return gr
}

// raftConfig 构造共享的 raft 配置：tick 参数、单消息/在途日志上限、
// 丢弃日志器（raft 库自身日志噪音大，关键节点由我们的 slog 承担）。
//
// storage 为 raft.Storage 接口：装配方传入包了快照生成器的 stg——
// raft 经它读日志，需要给落后 follower 发 MsgSnap 时调用它的
// Snapshot() 现场生成（Task 4）。
//
// DisableProposalForwarding 是内核契约：follower 上的 Propose 必须
// 报错（batch③ 据此翻译成客户端可重试码），不静默转发——默认行为会
// 把提案转发给 leader 并假成功，且任意字节载荷经转发进入日志后在
// apply 时炸 FSM（batch④ 集成测试抓到的缺口，见 TestClusterProposeOnFollowerFails）。
func raftConfig(id uint64, storage raft.Storage) *raft.Config {
	return &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		Logger:          &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
		// 内核契约：follower 收到提案直接丢弃并返回错误，绝不转发给 leader
		DisableProposalForwarding: true,
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
		// 持久化失败 = 内存日志与磁盘分叉：崩溃后本节点已确认的条目
		// 会消失，与 applyEntry 的失败同属「日志/状态与多数派分叉」的
		// 不可恢复类。统一走 panic——进程死亡由上层重启接管（走不干净
		// 判定）；若记 Error 后停摆返回，run 循环只是安静退出，Manager
		// 无从感知、组永久静默卡死，比 panic 更糟。
		gr.lg.Error("日志持久化失败，组停摆", "err", err)
		panic(err)
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		_ = gr.mem.SetHardState(rd.HardState)
	}
	_ = gr.mem.Append(rd.Entries)
	// 2. 发送 Messages：经注入的 send 回调外发（transport 发送永不
	//    阻塞——满则丢，raft 心跳重试兜底，Task 3 契约）
	gr.send(gr.g, rd.Messages)
	// leader 变更观测：SoftState 变化是切换的第一信号（当选与失联都在此）
	if rd.SoftState != nil {
		gr.lead.Store(rd.SoftState.Lead)
		gr.lg.Info("组 leader 变更", "lead", rd.SoftState.Lead, "term", rd.HardState.GetTerm())
		gr.notifyLeaderChange(rd.SoftState.Lead)
	}
	// 3. CommittedEntries apply
	// 本轮回合的成员变更登记：Advance 后再通知（见循环外注释），
	// notify=false 表示该变更非本节点发起，不通知（超时兜底）
	appliedCC := make([]ccApplied, 0, 2)
	for _, ent := range rd.CommittedEntries {
		// 重启重放的幂等保证：raft 可能重发已 apply 过的条目
		// （conflict 回退重写后），跳过即可——FSM 已是该 index 的状态
		if ent.GetIndex() <= gr.applied.Load() {
			continue
		}
		switch ent.GetType() {
		case raftpb.EntryConfChange:
			// 旧格式 V1 ConfChange（旧日志可能遗留）：照常 apply，但
			// 永不通知 waiter——V1 条目没有提案者身份，通知有跨节点
			// id 碰撞的假成功风险；且本进程只提议 V2，V1 条目在本
			// 进程内不可能有对应 waiter（重启后 ccWaiters 为空）。
			// 不通知 = 保守超时 = 安全方向。
			var cc raftpb.ConfChange
			cc.Reset()
			// v3 的 raftpb 是 protobuf-go v2 生成：需要显式 Unmarshal
			if err := proto.Unmarshal(ent.Data, &cc); err != nil {
				gr.lg.Error("ConfChange 解码失败，组停摆", "index", ent.GetIndex(), "err", err)
				panic(err)
			}
			cs := gr.rn.ApplyConfChange(&cc)
			// 成员表 + applied 同批落盘（同 V2 分支）：V1 路径罕见
			// 但不能只更内存——重启后旧日志仍会被重放，内存成员表
			// 不落盘就与持久化值分叉
			if gr.rs != nil {
				if err := gr.rs.SaveConfState(gr.g, cs, ent.GetIndex()); err != nil {
					gr.lg.Error("成员表持久化失败，组停摆", "index", ent.GetIndex(), "err", err)
					panic(err)
				}
				gr.confState.Store(cs) // Storage.Snapshot() 现场取用（Task 4）
			}
			gr.applied.Store(ent.GetIndex())
			gr.lg.Debug("成员变更已 apply", "type", cc.GetType().String(), "node", cc.GetNodeId())
		case raftpb.EntryConfChangeV2:
			var v2 raftpb.ConfChangeV2
			v2.Reset()
			if err := proto.Unmarshal(ent.Data, &v2); err != nil {
				gr.lg.Error("ConfChangeV2 解码失败，组停摆", "index", ent.GetIndex(), "err", err)
				panic(err)
			}
			cs := gr.rn.ApplyConfChange(&v2)
			// 成员表 + applied 同批落盘：截断之后日志前缀不复存在，
			// 重启只能靠这份持久化成员表恢复（Task 3 的截断前提）
			if gr.rs != nil {
				if err := gr.rs.SaveConfState(gr.g, cs, ent.GetIndex()); err != nil {
					gr.lg.Error("成员表持久化失败，组停摆", "index", ent.GetIndex(), "err", err)
					panic(err)
				}
				gr.confState.Store(cs) // Storage.Snapshot() 现场取用（Task 4）
			}
			// 成员变更的 waiter 通知放到 Advance 之后（见循环外注释）：
			// 这里只登记（id, 是否本节点发起），不直接通知
			ccid, ours := ccWaiterInfo(&v2, gr.selfID)
			appliedCC = append(appliedCC, ccApplied{id: ccid, notify: ours})
			gr.applied.Store(ent.GetIndex())
			if ch := v2.GetChanges(); len(ch) > 0 {
				gr.lg.Debug("成员变更已 apply", "type", ch[0].GetType().String(), "node", ch[0].GetNodeId())
			}
		case raftpb.EntryNormal:
			// 条目数据布局：[8B 提案者][8B waiter id][batch repr]——
			// apply 时跳过 16 字节头取批次载荷；waiter 通知限定
			// 本节点提案（proposalWaiter 的提案者校验，跨节点条目
			// id 碰撞不得假成功）
			var data []byte
			if len(ent.Data) > 16 {
				data = ent.Data[16:]
			}
			if len(ent.Data) > 0 && len(ent.Data) <= 16 {
				gr.lg.Warn("疑似损坏条目：普通条目数据 ≤16B 无批次载荷", "index", ent.GetIndex(),
					"len", len(ent.Data), "head", fmt.Sprintf("%x", ent.Data))
			}
			gr.applyEntry(ent.GetIndex(), data)
			if id, ok := proposalWaiter(ent.Data, gr.selfID); ok {
				gr.notifyWaiter(gr.propWaiters, id)
			}
		}
	}
	// 4. Advance——raft 库据此确认本轮 Ready 已处理，继续产下一轮
	gr.rn.Advance()
	// 成员变更 waiter 通知必须晚于 Advance：raft 库在 Advance 时才更新
	// 内部 applied 位点，FSM 层 apply（ApplyConfChange）发生时它仍停在
	// 上一条。若此时就通知，编排层（Remove→AddLearner 背靠背提案）紧
	// 接着提出的下一条 ConfChange 会落在「pendingConfIndex > applied」
	// 校验窗口内，被 raft 静默替换成空普通条目——替换不可观察、ccWaiter
	// 永不通知，调用方只能等超时（Task 7 集成测试抓到的缺口）。
	//
	// 注意：晚于 Advance 只是把窗口收窄，并非闭合——raft 内部 applied
	// 的推进要等节点 goroutine 消费 advancec 后才发生，µs 级残余窗口内
	// 背靠背的两条 ConfChange 仍可能被静默替换；proposeConfChange 对
	// 空条目替换的检测与重试是 batch③ 的兜底缓解。
	for _, cc := range appliedCC {
		if cc.notify {
			gr.notifyWaiter(gr.ccWaiters, cc.id)
		}
	}
}

// ccApplied 记录本轮已 apply 的成员变更条目：id 用于 Advance 后唤醒
// ccWaiters；notify 为 false（变更不是本节点发起）时不通知——跨节点
// 条目 id 碰撞时通知会造成假成功（apply 的是别节点发起的变更）。
type ccApplied struct {
	id     uint64
	notify bool
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
		// 坏批次字节是严重的数据完整性问题，panic 前把载荷头与长度留痕
		// （完整 dump 离线解码在排查时按需补充；日志头 64B 已足够定位
		// 损坏形态，如混入的 raftstore 键族）
		gr.lg.Error("FSM 批次重建失败，组停摆", "index", index, "len", len(data),
			"first64", fmt.Sprintf("%x", data[:min(len(data), 64)]), "err", err)
		panic(err)
	}
	if err := b.Set(appliedKey(gr.g), store.PutU64(index)); err != nil {
		b.Close()
		gr.lg.Error("写 applied 位点失败，组停摆", "index", index, "err", err)
		panic(err)
	}
	// FSM 数据与 applied 位点同批提交。apply 总是 NoSync：持久性由
	// raft 日志与后台批量刷盘承担（spec §5），不另起 fsync。
	// 「批提交 + 推 applied」在 applyMu 临界区内（Task 4）：快照生成在
	// 同一把锁内读 applied 并建 ReadView，二者互斥——视图内容恰好对
	// 应该位点，不会出现「快照声称包含它其实没有的数据」。持锁时间
	// 只有一次批提交，无任何等待。
	gr.applyMu.Lock()
	if err := gr.st.ApplyWith(b, false); err != nil {
		gr.applyMu.Unlock()
		gr.lg.Error("FSM apply 失败，组停摆", "index", index, "err", err)
		panic(err)
	}
	gr.applied.Store(index)
	gr.applyMu.Unlock()
	gr.notifyApplied(data)
}

// notifyLeaderChange 触发 OnLeaderChange 装配钩子（nil 安全）。
//
// 钩子契约：本方法在 Ready 循环内同步执行——钩子不得阻塞（阻塞即卡死
// 该组全部 tick/消息/apply 处理），重活由装配代码自行 dispatch 到独立
// goroutine；钩子 panic 不恢复，按 apply-panic 同策略传播（钩子由本仓库
// 装配代码注入，panic 即 bug）。
func (gr *group) notifyLeaderChange(leader uint64) {
	if gr.onLeaderChange != nil {
		gr.onLeaderChange(gr.g, leader, leader == gr.selfID)
	}
}

// notifyApplied 触发 OnApplied 装配钩子（nil 安全）。
//
// 钩子契约同 notifyLeaderChange：不得阻塞 Ready 循环，重活自行 dispatch；
// 且不得保留 data 引用——Ready 循环结束后条目标缓冲可能被 raft 复用，
// 需保留时必须自行拷贝。
func (gr *group) notifyApplied(data []byte) {
	if gr.onApplied != nil {
		gr.onApplied(gr.g, data)
	}
}

// propose 提交一条提案并阻塞直到它在本节点 apply 完成。
//
// 等 apply 而非等 commit（读己之写）：propose 返回后调用方立即可读
// 自己写入的数据；commit 只代表多数派确认，本节点 FSM 可能尚未追上，
// 只等 commit 会让「写入后立即可读」落空。
//
// 实现：分配自增提案 id → 注册 waiter（propWaiters）→ 提案者与 id
// 各 8B 大端前置到批次字节前 → rn.Propose → 等 waiter 通知或 ctx 超时。
//
// 非 leader 的提案路径（ErrNotLeader，见 raftConfig 的
// DisableProposalForwarding 契约）：
//   - 已知 leader 是他人：入口快速失败，不等 raft 静默丢弃
//   - leader 未知（选举进行中，lead=0）：不在此拒绝——本节点可能即将
//     当选，raft 的 Propose 会阻塞至 leader 产生再处理（单节点/启动
//     窗口期靠它保住读己之写，见 node.go run 循环的 propc 门闩）
//   - raft 层丢弃（ErrProposalDropped）：包装 ErrNotLeader 返回
//   - 等待超时且期间 leader 已变更：同样归入 ErrNotLeader——未提交的
//     低任期条目会被新 leader 追齐时截断，提案已丢
//
// 参数：
//   - ctx: 控制等待；超时/取消后 waiter 被移除（条目可能仍会被提交，
//     调用方已放弃等待，apply 时 notify 找不到它即可）
//   - batchRepr: store.Batch.Repr() 的物理字节（提案载荷）
//
// 返回：
//   - nil：条目已 apply
//   - error：Propose 失败、ctx 超时/取消，或 ErrNotLeader（调用方带
//     上下文处理，协议面按可重试码翻译）
func (gr *group) propose(ctx context.Context, batchRepr []byte) error {
	// 快速失败：已探明 leader 是他人（follower 提案在 DisableProposal
	// Forwarding 下必被丢弃），立即返回 ErrNotLeader 让客户端经
	// Leader(g) 重试——等待 raft 处理周期或等到超时都是客户端不可
	// 接受的失败模式。高频场景（客户端错发是常态不是异常）记 Debug。
	if lead := gr.leader(); lead != 0 && lead != gr.selfID {
		gr.lg.Debug("提案被拒：本节点非 leader", "lead", lead)
		return gr.notLeaderErr()
	}
	// 超时判定基线：等待期间 leader 变更 = 提案可能已被新任期截断丢弃
	leadAtEntry := gr.leader()
	id := gr.nextID.Add(1)
	ch := make(chan struct{})
	gr.mu.Lock()
	gr.propWaiters[id] = ch
	gr.mu.Unlock()

	// 提案头布局（终审 R4）：[8B 提案者 nodeID][8B waiter id]——apply
	// 时据此回调对应 waiter，且只有提案者为本节点的条目才被唤醒
	// （waiter id 是每节点独立计数器，跨节点可能撞车，裸 id 通知会
	// 把别节点丢失的提案误判成自己的成功）
	data := make([]byte, 16+len(batchRepr))
	binary.BigEndian.PutUint64(data[:8], gr.selfID)
	binary.BigEndian.PutUint64(data[8:16], id)
	copy(data[16:], batchRepr)

	if err := gr.rn.Propose(ctx, data); err != nil {
		gr.removeWaiter(gr.propWaiters, id)
		// follower 提案被 raft 丢弃（DisableProposalForwarding）——
		// 归入 ErrNotLeader，客户端据此重试；其余错误（ctx 超期等）
		// 原样返回
		if errors.Is(err, raft.ErrProposalDropped) {
			return gr.notLeaderErr()
		}
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
		// 等待期间 leader 已变更：未提交条目在新 leader 追齐时被截断，
		// 提案已丢——归入 ErrNotLeader 让客户端重试，而非当作不可重试
		// 的 deadline；leader 未变（如仍是我方但无 quorum）时条目仍
		// 可能提交，保持 ctx.Err()
		if cur := gr.leader(); cur != leadAtEntry && cur != gr.selfID {
			return gr.notLeaderErr()
		}
		gr.lg.Debug("propose 等待超时", "id", id, "err", ctx.Err())
		return ctx.Err()
	}
}

// notLeaderErr 构造带组号与当前 leader 上下文的 ErrNotLeader 包装错误，
// 调用方日志/协议面可据此定位重试目标。
func (gr *group) notLeaderErr() error {
	return fmt.Errorf("%w: 组 %d 当前 leader=%d", ErrNotLeader, gr.g, gr.leader())
}

// proposeConfChange 提出一条成员变更并阻塞直到它被 apply 到本节点。
//
// waiter 走独立命名空间（ccWaiters）：普通提案与成员变更的 id 共用
// 同一个 nextID 计数器，但 apply 时 EntryNormal 只通知 propWaiters、
// EntryConfChange 只通知 ccWaiters——id 相同也不会交叉误唤
// （终审观察项①）。成员变更以 ConfChangeV2 提交，提案者与 waiter id
// 放进 V2 的 Context 透传字段，apply 时只有本节点发起的变更才通知
// （终审 R4：跨节点 id 碰撞不得假成功）。
//
// 非 leader 路径与 propose 同契约（ErrNotLeader）：raft 的
// ProposeConfChange 不等待 raft 结果（node.go stepWithWaitOption
// wait=false），follower 上的变更被静默丢弃、waiter 只能靠超时兜底
// ——入口快速失败与超时归类因此比 propose 更关键。
//
// 参数：
//   - ctx: 控制等待；超时/取消后 waiter 被移除（条目可能仍会被提交）
//   - typ: 变更类型（ConfChangeAddNode/ConfChangeRemoveNode/
//     ConfChangeAddLearnerNode）
//   - nodeID: 变更目标节点 ID
//
// 返回：
//   - nil：成员变更已 apply
//   - error：ProposeConfChange 失败、ctx 超时/取消，或 ErrNotLeader
func (gr *group) proposeConfChange(ctx context.Context, typ raftpb.ConfChangeType, nodeID uint64) error {
	// 快速失败：同 propose——follower 的成员变更会被 raft 静默丢弃，
	// 等待只能以超时收场，那是客户端不可接受的失败模式
	if lead := gr.leader(); lead != 0 && lead != gr.selfID {
		gr.lg.Debug("成员变更被拒：本节点非 leader", "lead", lead)
		return gr.notLeaderErr()
	}
	leadAtEntry := gr.leader()
	// 提案 id 与 ConfChange.Id 共用同一个 nextID 计数器：单节点内
	// 原子自增不可能碰撞；双命名空间下同一 id 跨条目种类也不会误唤
	id := gr.nextID.Add(1)
	ch := make(chan struct{})
	gr.mu.Lock()
	gr.ccWaiters[id] = ch
	gr.mu.Unlock()

	// 成员变更以 ConfChangeV2 提交（终审 R4）：v3.7 的 V2 格式没有
	// Id 字段（Id 仅存于旧 V1 格式），waiter id 与提案者身份一并放进
	// Context——raft 核心只校验 Changes（pendingConfIndex/联合共识），
	// Context 是原样透传的私有字段（MarshalConfChange 把 V2 编码为
	// EntryConfChangeV2 条目类型，raft.go:1305 的 V2 分支不触碰
	// Context）。ConfChange 的标量字段在 protobuf-go v2 开放结构下是
	// 指针，取址传参。
	cc := raftpb.ConfChange{Type: typ.Enum(), NodeId: &nodeID, Id: &id}
	v2 := cc.AsV2()
	v2.Context = make([]byte, 16)
	binary.BigEndian.PutUint64(v2.Context[:8], gr.selfID)
	binary.BigEndian.PutUint64(v2.Context[8:16], id)
	if err := gr.rn.ProposeConfChange(ctx, v2); err != nil {
		gr.removeWaiter(gr.ccWaiters, id)
		if errors.Is(err, raft.ErrProposalDropped) {
			return gr.notLeaderErr()
		}
		return err
	}
	select {
	case <-ch:
		gr.lg.Debug("成员变更已 apply", "cc", typ.String(), "node", nodeID)
		return nil
	case <-ctx.Done():
		gr.removeWaiter(gr.ccWaiters, id)
		// 同 propose 的超时归类：等待期间 leader 已变更（含 raft 静默
		// 丢弃的 follower 变更——对端当选后我方才知道 leader 换了人，
		// 此时变更必然没进日志），归入 ErrNotLeader
		if cur := gr.leader(); cur != leadAtEntry && cur != gr.selfID {
			return gr.notLeaderErr()
		}
		gr.lg.Debug("成员变更等待超时", "id", id, "err", ctx.Err())
		return ctx.Err()
	}
}

// step 是消息投递入口（transport 读帧 goroutine 调用），把 raft 消息
// 交给本组处理。异步入队由 run 循环单 goroutine 消费，tick 与 Step
// 因此不会竞争。
//
// 全量入队不会撑爆 inbox 的不变量：单组在途消息 ≤ 2×MaxInflightMsgs
// （256）=512，小于 inbox 容量 1024——三节点拓扑下每条消息至多往返
// 一次（出站 + 对端回包），raft 的 inflight 上限即入队总量上界；
// 单节点组稳态不产自消息（自我投递仅出现在选举期）。
//
// 组已退出后（doneCh 关闭）丢弃消息而不阻塞（终审 R4）：传输读
// goroutine 由整条连接共享，阻塞在某一组的 step 会拖死同连接上其余
// 所有组的消息投递——raft 重试与上层编排是丢弃的兜底。
//
// 注意：m 必须为指针——v3 的 raftpb.Message 内嵌互斥锁
// （protoimpl.MessageState），按值传递会触发 vet copylocks，
// 且与 rn.Step 的指针签名天然一致。
func (gr *group) step(m *raftpb.Message) {
	select {
	case gr.inbox <- m:
	case <-gr.doneCh:
		// 组已退出：丢弃（raft 重试/上层编排兜底），不阻塞传输读循环
	}
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

// proposalWaiter 解析普通提案条目的 16B 头部，返回 waiter id 与
// 「是否本节点提案」。
//
// 布局：[8B 提案者 nodeID][8B waiter id]。ok=false 的两种情况：
//   - 头部不足 16B（旧格式或选举空条目）：无提案者身份，保守不通知，
//     提案方只能超时（安全方向，杜绝假成功）；
//   - 提案者不是本节点：该 id 在本节点可能恰好与自己的计数器撞车
//     （waiter id 是每节点独立自增），通知会把别节点丢失的提案误判
//     成自己的成功——apply 的是别节点的消息，必须静默。
func proposalWaiter(data []byte, selfID uint64) (id uint64, ok bool) {
	if len(data) < 16 {
		return 0, false
	}
	if binary.BigEndian.Uint64(data[:8]) != selfID {
		return 0, false
	}
	return binary.BigEndian.Uint64(data[8:16]), true
}

// ccWaiterInfo 解析 ConfChangeV2 的 Context 头，返回成员变更的 waiter
// id 与「是否本节点发起」。
//
// Context 是我们私有的 16B 头（[8B 提案者][8B waiter id]，见
// proposeConfChange）：raft 核心只校验 Changes，不触碰 Context。
// ok=false（长度不足或提案者不是本节点）时不得唤醒 ccWaiters——
// 跨节点/无头条目的通知缺失只造成超时（安全方向），假成功才是灾难。
func ccWaiterInfo(v2 *raftpb.ConfChangeV2, selfID uint64) (id uint64, ok bool) {
	ctx := v2.GetContext()
	if len(ctx) < 16 {
		return 0, false
	}
	if binary.BigEndian.Uint64(ctx[:8]) != selfID {
		return 0, false
	}
	return binary.BigEndian.Uint64(ctx[8:16]), true
}
