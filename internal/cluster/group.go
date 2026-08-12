// group.go 提供单组运行体：tick 驱动、Ready 分发 + 本地 append/apply
// 两阶段、真实 FSM apply 与 waiter 双命名空间。
//
// 职责：
//   - 驱动单个 raft 组的生命周期：tick、消息步进、Ready 分发
//   - propose/proposeConfChange 阻塞至条目在本节点 apply 完成（读己之写）
//   - applied 位点与 FSM 数据同批原子写入共享 store（spec §5）
//   - 按确认档位决定日志持久化是否带 fsync
//
// 边界：
//   - 不管组间路由与成员编排——Manager 的事（Task 5 组装）
//   - 不做快照与日志截断——batch④，日志无界增长、追齐走全量重放
//   - 不在主循环内做存储写入——日志落盘与 FSM apply 分属两条本地阶段
//     协程，见 dispatchReady
//   - AckQuorumMem 的后台批量 fsync 不在本层——全组共享一条 WAL，
//     一个 flusher 即可，由 Manager 持有
//   - 传输层生命周期（拨号/断线/关闭日志）归属 Manager 层（Task 3 约定）
package cluster

import (
	"bytes"
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

// 快照拉取的两条绝对上限（I4）：正常安装远达不到，命中即说明发送侧
// 枚举异常或对端不可信。取值理由——
//   - maxSnapshotChunks=65536：按默认 4 MiB 分块预算，相当于 256 GiB
//     单组状态；单组真到这个量级，快照追齐本身已不是可行方案；
//   - maxSnapshotBytes=256 GiB：与上一条同量级，兜住「块数不多但每块
//     巨大」的另一半（块大小由发送侧决定，接收侧不能假设它守规矩）。
//
// 为什么必须有：没有上限时，拉块循环的唯一退出条件是对端说 done——
// 把「对端诚实」当成了本节点不崩的前提。坏对端（或发送侧枚举 bug）
// 可以无界地往本节点磁盘写。
//
// 为什么是 var 而不是 const：两条上限都取在「正常永不触及」的量级上，
// 真按生产值跑一遍要拉 256 GiB——那条守卫就永远没有测试覆盖，写反了
// 比较方向也无人知晓。测试把它们临时调小、defer 还原，是让守卫本身
// 可验证的唯一低成本办法；生产路径不改这两个值。
//
// maxSnapshotBytes 必须是 int64 而不是 int：256<<30 超出 32 位 int 的
// 表示范围，写成 int 时 GOARCH=386/arm 直接编译失败（终审 R3-4）。
var (
	maxSnapshotChunks       = 1 << 16
	maxSnapshotBytes  int64 = 256 << 30
)

// localMsg 是投进本地存储阶段的一条消息及其配套判定。
//
// mustSync 只对 MsgStorageAppend 有意义：async 之后写入点已经拿不到
// Ready，而 fsync 档的同步判定（mode==AckQuorumFsync && rd.MustSync）
// 必须逐轮成立，因此判定在主循环现场算好、随消息配对传下去。载体变了，
// 判定本身一个字没变（设计文档 §4）。
//
// 为什么不改成「带 Responses 就 sync」（raft 契约的字面要求）：那比
// MustSync 严格——commit-only 的轮次也会被 fsync，等于退回 2026-08-08
// 「每提案少一次 fsync」优化之前的形态。MustSync 为假意味着无新条目且
// term/vote 未变，此时 Responses 里的 MsgStorageAppendResp 确认的是**更早
// 轮次**已经 fsync 过的条目，commit 位点丢了由重放重新推导——与旧路径
// syncPersist 的既有论证完全同构。
type localMsg struct {
	m        *raftpb.Message
	mustSync bool
}

const (
	// localQueueDepth 两条本地存储通道的容量。取 64：单组在途 Ready
	// 受 MaxInflightMsgs(256) 与 MaxCommittedSizePerReady(=MaxSizePerMsg,
	// 1MiB) 双重约束，64 轮在途已远超稳态需要；再大只是把「存储侧跟不上」
	// 从阻塞变成静默堆积内存，反而更难发现。
	localQueueDepth = 64
	// localQueueBlockWarn 入队阻塞多久算异常。50ms ≈ 半个 tick（100ms）
	// ——超过它意味着本组的选举计时器已经开始受影响，必须留痕。
	localQueueBlockWarn = 50 * time.Millisecond
)

// group 是一个 raft 组运行体：tick 驱动选举/心跳，Ready 循环按 m.To
// 分发（网络消息立即外发，本地存储消息经 appendCh/applyCh 交给两条
// 阶段协程），FSM 为共享 store。
type group struct {
	g  uint32
	rn raft.Node // 由装配方在 newGroup 之后创建并赋值（Config.Storage 要包了快照生成器的 stg）
	// 日志存储双记账（Task 4）：mem 是 raft 库读写日志的易失视图
	// （Append/SetHardState/Compact），stg 是包在 mem 外的 raft.Storage
	// 包装——raft 经它读日志，Snapshot() 由 groupStorage 现场生成
	mem *raft.MemoryStorage
	stg raft.Storage
	// snaps 是快照视图注册表（Manager 全组共享；单组单测路径由 newGroup
	// 自建）。本组用它做两件事：stg 的现场快照生成建视图，以及 MsgSnap
	// 外发时登记定向台账（noteSnapSends → leader 侧失败感知的判活依据）。
	snaps  *snapRegistry
	rs     *raftStore   // 日志的共库持久层
	st     *store.Store // 共享 store：FSM apply 的唯一落点
	send   func(g uint32, msgs []*raftpb.Message)
	mode   AckMode
	lg     *slog.Logger
	selfID uint64 // 本节点 ID：newGroup 装配参数，isLeader 比较与快照描述符 leader 字段用

	// control 是控制通道 RPC 回调（Manager 在 buildGroup 装配，同 rn
	// 的「newGroup 后赋值」模式）：installSnapshot 经它向快照生成节点
	// 分块拉取组状态（OpFetchSnapshot，见 snapinstall.go）。nil 时
	// installSnapshot 按装配错配报错——单元测试路径不装它。
	control func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error)

	// groups 是数据组总数（Manager 在 buildGroup 装配）：wipeGroupKeys
	// 哈希归属的分母（清空重来只清本组键，见 snapinstall.go）。
	groups uint32

	// 装配钩子（nil 安全，见 notifyLeaderChange/notifyApplied 的契约注释）：
	// 在 Ready 循环内同步触发，不得阻塞；重活由装配代码自行 dispatch。
	onLeaderChange func(g uint32, leader uint64, isSelf bool)
	onApplied      func(g uint32, repr []byte)

	// v3 的 raftpb.Message 是 protobuf-go v2 生成，内部嵌入互斥锁
	// （protoimpl.MessageState）。消息全链路用指针传递，避免按值拷贝
	// 触发 vet copylocks 检查，也省一次拷贝开销。
	inbox   chan *raftpb.Message
	// 本地存储阶段的两条通道（AsyncStorageWrites）：主循环按 m.To 分发，
	// append/apply 两条协程各自消费。
	//
	// **满则阻塞，绝不丢**——这与 inbox 的「满则丢」契约正好相反，是
	// raft 的硬要求：同一 target 的本地消息必须可靠、有序处理
	// （raft/v3@v3.7.0/raft.go:163-165）。丢一条 MsgStorageAppend 等于
	// 静默丢日志，且 raft 会一直等那条永远不来的 MsgStorageAppendResp
	// ——组静默卡死，没有任何报错。阻塞会传导到主循环并停掉本组 tick，
	// 因此 enqueueLocal 对阻塞留痕（见其注释）。
	appendCh chan localMsg
	applyCh  chan localMsg
	// 本地阶段可观测性（Task 4 补全语义，字段在此一次性声明避免二次改
	// 结构体）：累计入队阻塞时长、累计处理条数、单次处理耗时峰值。
	appendBlockNanos atomic.Uint64
	applyBlockNanos  atomic.Uint64
	appendCount      atomic.Uint64
	applyCount       atomic.Uint64
	respDropped      atomic.Uint64
	applied atomic.Uint64 // 已 apply 的最高条目 index（重启重放幂等的基础）
	lead    atomic.Uint64 // 当前 leader 节点 ID，SoftState 变化时更新
	// lastTerm 是最近一次非空 HardState 的 term 缓存（currentTerm 的
	// 数据源）：HardState 只在有变化的那一轮非空，直接读
	// rd.HardState.GetTerm() 在空轮次恒为 0——详见 currentTerm 注释
	lastTerm atomic.Uint64

	// confState 是当前成员表（rn.ApplyConfChange 的返回值，ConfChange
	// apply 时同步更新）。重启时由 newGroup 从持久化值初始化（见
	// newGroup）；Task 4 的 Storage.Snapshot() 现场取用它生成快照。
	confState atomic.Pointer[raftpb.ConfState]

	// applyMu 串行化「写 FSM 批次 + 推 applied」临界区与快照生成
	// （Task 4）：groupStorage.Snapshot() 在同一把锁内读 applied 与建
	// ReadView，视图内容恰好对应该位点。持锁时间必须短——只覆盖批
	// 提交 + Store(applied)，apply 路径绝不可在锁内阻塞等待。
	applyMu sync.Mutex

	// installMu 串行化「快照安装（append 阶段）」与「条目 apply
	// （apply 阶段）」。
	//
	// 为什么 async 之后才需要：旧路径里两者都在 Ready 循环内顺序执行，
	// 物理上不可能重叠；async 把它们拆成两条协程，而 raft 明确允许不同
	// target 的本地消息乱序处理（raft.go:165-166）。安装会整体重写本组
	// FSM（wipeGroupKeys → 拉块 → 收口批次），一批安装前入队的
	// MsgStorageApply 若在安装之后落地，会把陈旧数据写回刚被覆盖的键。
	//
	// 与 applyMu 的分工（两把锁不嵌套、粒度不同，别合并）：
	//   - applyMu 是**短**临界区，只覆盖「批提交 + 推 applied」，与快照
	//     生成（groupStorage.Snapshot）互斥，保证视图与位点配对；
	//   - installMu 是**长**临界区，覆盖整个安装（分钟级）与整批 apply，
	//     只解决"安装 vs apply"这一件事。
	//
	// 陈旧批次不需要额外处理：安装收口时 applied 已跳到快照位点，
	// applyPhase 里 index ≤ applied 的既有守卫会把整批跳掉。
	installMu sync.Mutex

	doneCh chan struct{} // run 循环完全退出后关闭，测试/调用方同步用

	// installing 标记本组正在安装快照（appendOnce 的快照分支进出时
	// 置位/清位）。安装期主循环可能阻塞在本地通道入队上而不再消费
	// inbox，step 必须改为「满则丢弃」——见 step 注释的 I5 说明。
	installing atomic.Bool
	// installDrops 累计安装期因 inbox 满而丢弃的消息数（可观测性：
	// 安装结束时打点，长期非零说明安装耗时已长到影响心跳投递）。
	installDrops atomic.Uint64

	// waiter 双命名空间（终审观察项①）：普通提案与成员变更的 id 共用
	// 同一个 nextID 计数器，但 apply 时 EntryNormal 只通知 propWaiters、
	// EntryConfChange 只通知 ccWaiters——id 相同也不会交叉误唤。
	// 终审 R4：通知还带提案者作用域——条目头/Context 携带提案者身份，
	// 只有本节点发起的条目才唤醒 waiter（跨节点 id 碰撞不得假成功）。
	mu          sync.Mutex
	propWaiters map[uint64]chan struct{}
	ccWaiters   map[uint64]chan struct{}
	nextID      atomic.Uint64

	// readWaiters 读屏障等待者（第三个命名空间）：与 propWaiters/ccWaiters
	// 共用 gr.mu 与 gr.nextID 计数器，但读状态经 Ready.ReadStates 回流、
	// 不走日志条目，因此不会与前两者交叉误唤。
	readWaiters map[uint64]*readWait

	// 读屏障的「排队成批」合流状态（brMu 独立于 mu：合流只调度轮次，
	// 不碰 waiter 表，两把锁不嵌套）。
	// running=true 表示有一轮 read-index 在途；此时到达的等待者一律排进
	// next 批，绝不搭当前这一轮的车——当前轮的 readIndex 取自它们发起
	// 之前，中间被确认的写它们看不到，复用即破坏线性一致。
	brMu      sync.Mutex
	brNext    *barrierBatch
	brRunning bool
	// barrierTimeout 单轮 read-index 的时间预算（每轮独立计时，不随某个
	// 等待者的 ctx 走——一个等待者放弃不该让整批失败）。
	barrierTimeout time.Duration
	// lifeCtx 是 run 循环的生命周期 ctx：合流的驱动 goroutine 用它做
	// 父 ctx，组退出时在途轮次随之取消。
	lifeCtx context.Context
	// readIndexFn 单轮 read-index 的执行体，默认 readIndexOnce；测试注入
	// 假实现以观察轮次调度（合流逻辑与 raft 交互解耦）。
	readIndexFn func(ctx context.Context) error
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
		appendCh:       make(chan localMsg, localQueueDepth),
		applyCh:        make(chan localMsg, localQueueDepth),
		propWaiters:    make(map[uint64]chan struct{}),
		ccWaiters:      make(map[uint64]chan struct{}),
		readWaiters:    make(map[uint64]*readWait),
		doneCh:         make(chan struct{}),
	}
	// 提案 id 用时间戳做种子（终审 R4）：干净重启后计数器从远离旧值
	// 的位置继续，配合条目头的提案者校验双保险——旧进程的等待者已随
	// 进程消亡，新进程的 id 空间不得与旧进程重叠（否则重启回零是
	// 跨节点 id 碰撞的第二条路径）。
	gr.nextID.Store(uint64(time.Now().UnixNano()))
	// 读屏障装配默认（Task 2 的 Manager 层可覆盖）：单轮 read-index 的
	// 时间预算与执行体
	gr.barrierTimeout = 3 * time.Second // 默认值；装配方经 Manager 覆盖
	gr.readIndexFn = gr.readIndexOnce
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
	gr.snaps = reg
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
//
// PreVote（raft thesis §9.6）消除长安装后的立即竞选中击：快照安装期间
// run 循环阻塞、心跳无法被 Step，electionElapsed 攒满整个安装周期，而
// promotable() 因 in-progress snapshot 为 false 不会中途竞选；安装一结束
// 第一个 tick 即触发竞选，旧任期下直接 term bump、白白换一次
// 主（生产级快照 = 分钟级 chunk RTT，每次安装都换主）。PreVote 先跑一轮
// 预选（以 r.Term+1 发 MsgPreVote 但不递增任期，raft 源码「Never change
// our term in response to a PreVote」）：掉队节点的预选被拒，term 不动。
// 安全面：预选败北不发真实竞选，重分区收敛多一个 RTT 往返（1s 选举
// 超时下可忽略）；既有 failover 时序测试（TestClusterKillLeaderWrite
// Continues 等）与 quorum-mem/fsync 两档模式均不受影响。
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
		// 见 raftConfig 注释：预选阶段挡掉掉队节点的 term bump（安装攒满
		// electionElapsed → 安装后立即竞选换主）
		PreVote: true,
		// 异步存储写入（AsyncStorageWrites）：日志写入与状态机应用改由
		// MsgStorageAppend/MsgStorageApply 两条本地消息表达，写入与
		// Ready 迭代解耦。打开它是本仓库攻 quorum-fsync 档 raft 机制税
		// 的手段——非 async 模式下 node.run 投出 Ready 后必须等 advancec
		// 才产下一轮，于是 leader 做 fsync 的那段时间里 MsgApp 一个字节
		// 都发不出去，确认链是「leader fsync → 网络 → follower fsync」
		// 两次串行相加。打开后 leader 可在自己 fsync 完成前就 replicate
		// （raft 只要求 commit 前 durable，不要求 replicate 前 durable）。
		//
		// 单点开关：所有装配路径（StartNode/RestartNode）与全部单元测试
		// 都经本函数取配置，此处置位即全局生效。**不提供配置项**——两套
		// Ready 处理路径长期共存必然腐化，且会制造「两条路径只测了一条」
		// 的虚假安全感（设计文档 §4）。
		AsyncStorageWrites: true,
	}
}

// run 驱动组循环：tick 驱动选举/心跳、消息步进、Ready 分发，
// 直至 ctx 取消退出。
//
// 本循环内不做任何存储写入——写入全部经 appendCh/applyCh 交给两条
// 阶段协程，这正是流水线深度的来源。
//
// 注意：tick 与心跳是高频路径，本循环内零日志（热循环规则）；
// 关键节点日志全部落在 dispatchReady/阶段协程/propose 等低频路径上。
func (gr *group) run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond) // ElectionTick=10 → 选举超时约 1s
	defer ticker.Stop()
	// 两条本地存储阶段协程与主循环同生命周期：doneCh 必须在三者**全部**
	// 退出之后才关闭，否则测试里 <-gr.done() 返回时仍有协程在碰 store/rs，
	// -race 下是稳定的 use-after-close。
	var stages sync.WaitGroup
	stages.Add(2)
	go func() { defer stages.Done(); gr.runAppend(ctx) }()
	go func() { defer stages.Done(); gr.runApply(ctx) }()
	defer func() {
		stages.Wait()
		// 组退出汇总：本地阶段的阻塞总量是「存储侧跟不上」的唯一量化
		// 信号——它不为零就意味着主循环被拖住过、本组 tick 受过影响。
		// 只在退出时打一次（热路径零日志），长期非零由运维按 search_logs
		// 检索这条文案定位。
		ab, pb := gr.appendBlockNanos.Load(), gr.applyBlockNanos.Load()
		if ab > 0 || pb > 0 || gr.respDropped.Load() > 0 {
			gr.lg.Warn("本地存储阶段存在阻塞或丢弃（存储侧跟不上主循环）",
				"append_blocked", time.Duration(ab).Round(time.Millisecond).String(),
				"apply_blocked", time.Duration(pb).Round(time.Millisecond).String(),
				"append_handled", gr.appendCount.Load(),
				"apply_handled", gr.applyCount.Load(),
				"resp_dropped", gr.respDropped.Load())
		}
		close(gr.doneCh)
	}()
	// 存生命周期 ctx：读屏障的合流驱动 goroutine 以它为父 ctx，组退出时
	// 在途的 read-index 轮次随之取消，不会挂着等到 barrierTimeout。
	gr.lifeCtx = ctx
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
			gr.dispatchReady(ctx, rd)
		}
	}
}

// dispatchReady 处理一轮 Ready：网络消息立即外发，两条本地存储消息可靠
// 入队，读状态与 leader 变更就地处理。**没有 Advance**——async 模式下
// node.run 的 advancec 恒为 nil，调用 Advance 会永久阻塞（raft/v3@v3.7.0/
// node.go:435-441、:555-560）；raft 认为「本轮处理完毕」的信号改由本地
// 阶段投递 m.Responses 承担。
//
// 顺序即收益（本改造的全部意义所在）：
//  1. **先外发网络消息**——leader 的 MsgApp 从此不再排在自己的 fsync
//     后面，follower 可以与 leader 并行落盘。这一步不需要任何前置条件：
//     async 下 rd.Messages 里的网络消息「can be sent immediately」，因为
//     一切以持久化为前提的消息都被移进了本地消息的 Responses 里
//     （raft/v3@v3.7.0/node.go:98-110）。
//  2. 入队 append（携带本轮 MustSync）、入队 apply。
//  3. 读状态回流与 leader 变更。
//
// 为什么 leader 变更放在入队之后：入队本身就是排序动作——写入已经进了
// FIFO，不可能被后续轮次的写入越过。这比旧路径（持久化**完成**后才公布
// 新 leader）弱一档，是 async 的固有代价：raft 自身的安全性由 MsgVoteResp
// 随 Responses 在 fsync 之后投递来保证（vote 落盘先于响应投票请求），
// 本节点公布 leader 身份只影响本进程内的路由与钩子。
//
// rd.Entries/HardState/Snapshot/CommittedEntries 在 async 下**一律不直接
// 消费**——它们已被复制进两条本地消息，直接用会双写/双 apply。
func (gr *group) dispatchReady(ctx context.Context, rd raft.Ready) {
	var outbound []*raftpb.Message
	var locals []localMsg
	for _, m := range rd.Messages {
		switch m.GetTo() {
		case raft.LocalAppendThread:
			locals = append(locals, localMsg{m: m, mustSync: rd.MustSync})
		case raft.LocalApplyThread:
			locals = append(locals, localMsg{m: m})
		default:
			outbound = append(outbound, m)
		}
	}
	// 1. 网络消息立即外发（transport 发送永不阻塞——满则丢，raft 心跳
	//    重试兜底，Task 3 契约）。外发前先登记本轮的 MsgSnap 定向台账
	//    ——这是 leader 侧唯一能知道「哪份快照发给了哪个 peer」的时刻。
	if len(outbound) > 0 {
		gr.noteSnapSends(outbound)
		gr.send(gr.g, outbound)
	}
	// 2. 本地存储消息按原序可靠入队。入队失败只可能是组正在退出
	//    （enqueueLocal 已留痕），此时直接收工——后续消息也没有归宿。
	for _, lm := range locals {
		ch := gr.applyCh
		stage := "apply"
		if lm.m.GetTo() == raft.LocalAppendThread {
			ch, stage = gr.appendCh, "append"
		}
		if !gr.enqueueLocal(ctx, ch, lm, stage) {
			return
		}
	}
	// 3. 读状态回流：raft 已确认本节点在当前任期仍是 leader，给出的
	//    readIndex 是本轮读屏障的下界。真正放行由 index<=applied 决定，
	//    apply 阶段每批之后还会再扫一次（见 applyOnce）。
	gr.stepReadStates(rd.ReadStates)
	// 4. leader 变更观测：SoftState 变化是切换的第一信号。
	//    顺序即屏障（batch③ 评审 m1）：先跑钩子（同步失效计数器缓存），
	//    再让 lead.Store 把 leader 身份对外可见。反过来会留下
	//    「IsLeader 已放行、缓存尚未失效」的窗口，并发 Append 拿到
	//    陈旧 offset 覆写已 quorum 提交的消息。
	if rd.SoftState != nil {
		gr.notifyLeaderChange(rd.SoftState.Lead)
		gr.lead.Store(rd.SoftState.Lead)
		gr.lg.Info("组 leader 变更", "lead", rd.SoftState.Lead, "term", gr.currentTerm())
	}
}

// enqueueLocal 把一条本地存储消息可靠投递进指定阶段通道，返回是否成功。
//
// 参数：
//   - ch: 目标通道（gr.appendCh 或 gr.applyCh）
//   - lm: 待投递消息
//   - stage: 阶段名（"append"/"apply"），只用于日志
//
// 返回：true = 已入队；false = 组正在退出（ctx 已取消），调用方应收工。
//
// 与 gr.step（inbox）的契约正好相反：**满则阻塞，绝不丢**。raft 要求同一
// target 的本地消息可靠有序处理，丢一条即静默卡死（设计文档 §5.1）。
// 代价是阻塞会传导到主循环、停掉本组 tick，因此阻塞必须留痕——没有这条
// 日志，「存储侧跟不上」在现场只表现为莫名其妙的换主。
func (gr *group) enqueueLocal(ctx context.Context, ch chan<- localMsg, lm localMsg, stage string) bool {
	// 快路径：稳态下通道永远不满，一次非阻塞发送即完成，零日志零计时
	select {
	case ch <- lm:
		return true
	default:
	}
	start := time.Now()
	select {
	case ch <- lm:
		d := time.Since(start)
		gr.blockNanosOf(stage).Add(uint64(d))
		if d >= localQueueBlockWarn {
			gr.lg.Warn("本地存储通道阻塞（队列满，主循环被拖住，本组 tick 已受影响)",
				"stage", stage, "blocked", d.Round(time.Millisecond).String(),
				"cap", cap(ch), "type", lm.m.GetType().String())
		}
		return true
	case <-ctx.Done():
		gr.respDropped.Add(1)
		gr.lg.Warn("本地存储消息随停机丢弃（组正在退出）",
			"stage", stage, "type", lm.m.GetType().String(),
			"entries", len(lm.m.GetEntries()))
		return false
	}
}

// blockNanosOf 返回阶段对应的阻塞时长累计器（可观测性打点用）。
func (gr *group) blockNanosOf(stage string) *atomic.Uint64 {
	if stage == "append" {
		return &gr.appendBlockNanos
	}
	return &gr.applyBlockNanos
}

// hardStateOf 从 MsgStorageAppend 还原 HardState，无状态变更时返回 nil。
//
// raft 的构造契约（raft/v3@v3.7.0/rawnode.go:230-241）：HardState 有变化
// 时 Term/Vote/Commit **三者同时赋值**，无变化时三者同时不赋值。因此看
// 任一字段是否为 nil 即可判定，不必逐个比较。
//
// 三个值按值拷贝而不是共享 m 的指针：mem.SetHardState 会长期持有这份
// 结构，而消息的生命周期由 raft 决定——共享指针是"能跑但说不清"的那类
// 依赖，一次拷贝三个 uint64 的代价可以忽略。
func hardStateOf(m *raftpb.Message) *raftpb.HardState {
	if m.Term == nil && m.Vote == nil && m.Commit == nil {
		return nil
	}
	term, vote, commit := m.GetTerm(), m.GetVote(), m.GetCommit()
	return &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
}

// runAppend 是 append 阶段的协程主体：串行消费 appendCh。
//
// 串行是契约要求（同一 target 的本地消息不得重排），也正是攒批的来源
// ——主循环不再等它，raft 于是能连着产出多轮 Ready，本协程一轮一轮
// 消费时每轮的 Entries 自然更大。
func (gr *group) runAppend(ctx context.Context) {
	gr.lg.Info("append 阶段启动", "queue_cap", cap(gr.appendCh))
	defer func() {
		gr.lg.Info("append 阶段退出", "handled", gr.appendCount.Load(),
			"blocked_total", time.Duration(gr.appendBlockNanos.Load()).Round(time.Millisecond).String())
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case lm := <-gr.appendCh:
			gr.appendOnce(ctx, lm)
		}
	}
}

// appendOnce 处理一条 MsgStorageAppend：快照安装（若有）→ 持久化 →
// 双记账 → 投递响应。
//
// 顺序即不变量（设计文档 §5.2）：raft 判定「日志已 stable」的那一刻就是
// 响应投回的那一刻，此后它会立刻去 MemoryStorage 读这些条目。任何一步
// 提前投递响应，raft 都会读到还不存在的日志。
func (gr *group) appendOnce(ctx context.Context, lm localMsg) {
	m := lm.m
	gr.appendCount.Add(1)
	// 0. 快照：async 下快照随 MsgStorageAppend 到达（raft/v3@v3.7.0/
	//    raft.go:167-170「MsgStorageAppend carries ... snapshots to apply」）。
	//    安装期间保持 tick——见 installSnapshotWithRetry。
	if snap := m.GetSnapshot(); !raft.IsEmptySnap(snap) {
		// 安装持 installMu：整个安装期间 apply 阶段不得落地（见字段注释）
		gr.installMu.Lock()
		err := gr.installSnapshotWithRetry(ctx, snap)
		gr.installMu.Unlock()
		if err != nil && ctx.Err() != nil {
			// 停机途中的安装失败不是故障：安装中标记已在盘上，重启时
			// buildGroup 清空重来。此处 panic 只会把一次正常停机变成一次
			// 崩溃退出。直接返回——**且不投递响应**：响应一旦投出，raft
			// 就认为快照已持久化已应用。
			gr.lg.Warn("快照安装随停机中止（安装中标记已在盘上，重启清空重来）",
				"index", snap.Metadata.GetIndex(), "err", err)
			return
		}
		if err != nil {
			// 安装失败是不可恢复状态：绝不能投递响应后静默续跑——
			// MsgStorageAppendResp（携带快照）一旦步进给 raft 内核，内核的
			// appliedSnap（stableSnapTo + appliedTo）即刻把快照标记为已持久
			// 化已应用，raft 从此不再重发 MsgSnap；而磁盘上仍是安装中标记 +
			// 半截数据、内存侧 MemoryStorage 从未更新——三方分叉、永不收敛。
			// 按 Persist/applyEntries 同策略 fail-stop panic。
			//
			// （本段与旧路径唯一的差别是「Advance 步进响应」变成「投递
			// Responses 步进响应」——触发分叉的机制换了名字，后果一字不变。
			// 重启后的恢复路径、leader 侧 reportStalledSnapshots 的兜底
			// 责任，全部与旧注释所述一致，见 installSnapshotWithRetry。）
			gr.lg.Error("快照安装失败，组停摆（等待重启；leader 侧由失败感知重驱动）",
				"index", snap.Metadata.GetIndex(), "err", err)
			panic(err)
		}
	}
	// 1. 持久化 + 双记账。sync 判定：档位 × 本轮 MustSync，语义与旧路径
	//    的 syncPersist 完全一致，只是 MustSync 换了载体（见 localMsg）。
	gr.persistPhase(hardStateOf(m), m.GetEntries(), gr.mode == AckQuorumFsync && lm.mustSync)
	// 2. 投递响应——必须严格晚于第 1 步（本函数 doc comment）
	gr.deliverResponses(ctx, m.GetResponses(), "append")
}

// runApply 是 apply 阶段的协程主体：串行消费 applyCh。
//
// 与 append 阶段并行运行是刻意的（设计文档 §5.3）：MsgStorageApply 的
// 写入**不要求** durable 即可投递响应，apply 因此可以比 append 跑得松。
// 把两者合成一条协程会让 FSM 写入重新挡住日志 fsync，收益折半。
func (gr *group) runApply(ctx context.Context) {
	gr.lg.Info("apply 阶段启动", "queue_cap", cap(gr.applyCh))
	defer func() {
		gr.lg.Info("apply 阶段退出", "handled", gr.applyCount.Load(),
			"blocked_total", time.Duration(gr.applyBlockNanos.Load()).Round(time.Millisecond).String())
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case lm := <-gr.applyCh:
			gr.applyOnce(ctx, lm.m)
		}
	}
}

// applyOnce 处理一条 MsgStorageApply：apply 条目 → 投递响应 → 唤醒
// 成员变更与读屏障等待者。
//
// **通知必须晚于响应投递**，这是 Advance 消失后 ccWaiter 时序的新落点：
// 旧路径里 raft 内部 applied 位点在 Advance 时才推进（Advance 负责把
// MsgStorageApplyResp 步进内核），若在此之前通知，编排层
// （Remove→AddLearner 背靠背提案）紧接着提出的下一条 ConfChange 会落在
// 「pendingConfIndex > applied」校验窗口内，被 raft 静默替换成空普通条目
// ——替换不可观察、ccWaiter 永不通知，调用方只能等超时（Task 7 集成
// 测试抓到的缺口）。async 下承担这件事的是 deliverResponses，因此通知
// 挪到它之后，职责一一对应。
//
// 同旧路径：晚于响应投递只是把窗口收窄，并非闭合——raft 内部 applied
// 的推进要等节点 goroutine 消费 recvc 后才发生，µs 级残余窗口内背靠背的
// 两条 ConfChange 仍可能被静默替换；proposeConfChange 对空条目替换的检测
// 与重试是 batch③ 的兜底缓解。
func (gr *group) applyOnce(ctx context.Context, m *raftpb.Message) {
	gr.applyCount.Add(1)
	// 与快照安装互斥（installMu）：安装重写整个 FSM，期间落地的一批
	// 陈旧条目会把旧数据写回刚被覆盖的键——静默状态机分叉。
	// 安装期 apply 会在这里排队：分钟级安装下这是正常现象，但必须可见
	// ——否则现场只表现为「读屏障迟迟不放行」而查不到原因。
	if !gr.installMu.TryLock() {
		start := time.Now()
		gr.installMu.Lock()
		if d := time.Since(start); d >= localQueueBlockWarn {
			gr.lg.Warn("apply 阶段等待快照安装让路", "waited", d.Round(time.Millisecond).String(),
				"entries", len(m.GetEntries()))
		}
	}
	appliedCC := gr.applyPhase(m.GetEntries())
	gr.installMu.Unlock()
	gr.deliverResponses(ctx, m.GetResponses(), "apply")
	for _, cc := range appliedCC {
		if cc.notify {
			gr.notifyWaiter(gr.ccWaiters, cc.id)
		}
	}
	// 读屏障放行必须晚于 apply：applied 是本批 apply 推进的，早于它扫描
	// 只会白扫一遍，屏障要多等一整批才放行。
	gr.notifyReadWaiters(gr.applied.Load())
}

// deliverResponses 投递一条本地存储消息的响应集合。
//
// 参数：
//   - resps: m.Responses，可能为空
//   - stage: 阶段名（"append"/"apply"），只用于日志
//
// 路由是本改造最容易踩死的一处（设计文档 §5.1）：
//
//	| 响应目标        | 去向        | 可靠性                     |
//	|-----------------|-------------|----------------------------|
//	| 本节点（selfID）| gr.rn.Step  | **可靠有序**，满则阻塞不丢 |
//	| 其他 peer       | gr.send     | 可丢，raft 心跳重试兜底    |
//
// **自指响应绝不能走 gr.send**：Manager.send 对自指消息走 gr.step →
// inbox，而 inbox 在快照安装期是显式丢弃、组退出时也丢弃。丢一条
// MsgStorageAppendResp 就是 raft 永远等不到的那一条——组静默卡死，没有
// 任何报错。gr.rn.Step 走 node.recvc，满则阻塞、不丢；且不会与主循环
// 死锁——node.run 的 select 始终可以消费 recvc，即便主循环正阻塞在
// 本地通道入队上。
//
// 对端响应先发、自指响应后步进：follower 的 MsgAppResp 在关键路径上，
// 早一个调度周期就早一点确认；自指响应之间的相对顺序原样保留（raft 要求
// MsgStorageAppendResp 排在 msgsAfterAppend 里的自指 MsgAppResp 之后，
// 见 rawnode.go:245-253 的性能说明）。
func (gr *group) deliverResponses(ctx context.Context, resps []*raftpb.Message, stage string) {
	if len(resps) == 0 {
		return
	}
	var peer []*raftpb.Message
	for _, m := range resps {
		if m.GetTo() != gr.selfID {
			peer = append(peer, m)
		}
	}
	if len(peer) > 0 {
		gr.send(gr.g, peer)
	}
	for _, m := range resps {
		if m.GetTo() != gr.selfID {
			continue
		}
		if err := gr.rn.Step(ctx, m); err != nil {
			// 只可能是 ctx 取消或节点已 Stop（组正在退出）。稳态下不可能
			// 走到这里——真走到了说明有响应没被 raft 收到，必须留痕：
			// 它的症状是组静默卡死，届时这条日志是唯一现场。
			gr.respDropped.Add(1)
			gr.lg.Warn("本地响应步进失败（组正在退出？未收到该响应的组会静默卡死）",
				"stage", stage, "type", m.GetType().String(), "err", err)
			return
		}
	}
}

// keepTicking 在快照安装期间保持本组的选举计时器走动，返回停止函数。
//
// 为什么需要：installSnapshot 同步阻塞 run 循环（本循环是唯一收件点），
// 常规 tick 路径（run 的 ticker 分支）在安装期间停摆。若选举计时器
// 冻结，安装耗时超过 leader 的活跃判定窗口后，本节点会被判定失联。
// keepTicking 用独立 goroutine 以两倍于常规节奏调用 rn.Tick()——
// rn.Tick() 可跨 goroutine 调用（内部走 channel，满则丢，见 vendored
// node.go 的 Tick 实现），安装结束由调用方 stop() 收敛。
func (gr *group) keepTicking() (stop func()) {
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				gr.rn.Tick()
			}
		}
	}()
	return func() { close(stopCh) }
}

// installSnapshot 安装 leader 发来的快照（接收侧六步，顺序即不变量，
// 逐条写明「为什么是这个顺序」）：
//
//  1. 解析描述符 → (snapID, leader, index)：一切后续动作的数据源——
//     snapID 是源节点注册表里的视图 id，leader 是拉块目标，index 是
//     本组状态将落到的位点；
//  2. rs.MarkInstalling(g, meta)（Sync）——必须早于任何数据写入：
//     先标记后清空的崩溃窗口只留下「有标记」的半截状态，重启经
//     buildGroup 的标记检查清空重来；反过来（先清空后标记）的窗口里
//     磁盘是「已清空、无标记」= 静默空状态，重启会把它当完整状态
//     启动，客户端读到的消息永久缺失；
//  3. wipeGroupKeys(st, g, groups)——清掉本组旧状态：组 0 按连续前缀
//     DeleteRange 整段删；数据组逐键判哈希归属后批量 Delete。在标记
//     之后、写入之前，因此「有标记」恒等于「正在安装」；
//  4. 循环 Control(ctx, leader, OpFetchSnapshot, req) 拉块 →
//     decodeChunk → 批量写入（每块一个 batch，NoSync）：数据从源节点
//     分块流式取回。NoSync 是刻意的——崩溃时多余的半截键由重启的
//     清空重来兜底，收口批次（第 5 步）一次性 Sync 才是持久性承诺；
//  5. 收口批次：写 applied = meta.Index、SaveConfState(meta.ConfState)、
//     SaveSnapMeta(meta)、删安装中标记——四者同批 Sync。这一批的原子
//     性就是「安装完成」的定义：崩溃在此之前的任何一刻都等价于
//     「没装过」（标记在、数据半截，重启清空重来），只有这一批整体
//     落盘后标记才消失——不存在「数据已装完、标记残留」的重装；
//  6. 内存侧：mem.ApplySnapshot(snap)（元数据版，Data 置空即可）、
//     gr.applied.Store(meta.Index)、gr.confState.Store(meta.ConfState)。
//     先盘后内存：ApplySnapshot 之后 raft 的 FirstIndex 才越过截断点，
//     与磁盘上的 applied/锚点配对；应用侧 applied 不推进，raft 会重放
//     快照覆盖的条目（FSM 重放幂等，靠 applied 跳过）。
//
// snap 必须为指针：v3.7 的 raftpb.Snapshot 内嵌互斥锁
// （protoimpl.MessageState），按值传递触发 vet copylocks。
//
// 失败语义：任一步失败返回错误，appendOnce 按 fail-stop 策略 panic
// （进程死亡由上层重启接管）。为什么不能「放弃本轮等 raft 重发」：
// 投递 Responses 会把携带快照的 MsgStorageAppendResp 步进给 raft 内核，
// 内核的 appliedSnap（stableSnapTo + appliedTo）把快照标记为已持久化已
// 应用，raft 不会重发 MsgSnap——静默续跑是内存/磁盘/raft 三方分叉的
// 永久卡死（vendored raft.go MsgStorageAppendResp 分支）。
//
// panic 之后的恢复路径：panic 不写干净关机标记，重启时 Start 判定不干净
// 关机并返回 ErrUncleanShutdown，恢复手段是整目录 WipeForRejoin + 以
// learner 重新加入。buildGroup 里那条「见安装中标记即按组清空重来」的
// 分支走的是另一种情形——干净关机却留下了标记（停机途中止的安装，见
// appendOnce 的 ctx 分支）；标记（第 2 步，Sync）先于任何数据写入，
// 失败必然发生在第 2 步之后、收口批次删标记之前，标记恒在盘上，那条
// 路径上的清空重来永远成立。

// noteSnapSends 把本轮外发的 MsgSnap 登记进快照注册表的定向台账
// （(组, 目标节点) → snapID）。
//
// 为什么放在这里：leader 侧唯一知道「这份快照发给了谁」的时刻就是
// MsgSnap 外发的这一刻——raft 的 Progress 事后只保留 PendingSnapshot
// 位点，不保留 snapID。没有这份台账，失败感知只能按位点聚合判活，
// 一个 peer 的正常拉取会掩盖另一个 peer 的停摆（终审 R3-1）。
//
// 参数：msgs 为本轮 Ready 的待发消息（非 MsgSnap 一律跳过）。
//
// 注意：描述符不可解析时只记 Warn 不中断——那是发送侧自身的编码
// 问题，接收方拉取时同样会报错并走既有的失败路径，这里没有更好的
// 处置手段，绝不能因为台账登记失败而阻断消息外发。
func (gr *group) noteSnapSends(msgs []*raftpb.Message) {
	if gr.snaps == nil {
		return
	}
	for _, m := range msgs {
		if m == nil || m.GetType() != raftpb.MsgSnap {
			continue
		}
		desc, err := decodeSnapDescriptor(m.GetSnapshot().GetData())
		if err != nil {
			gr.lg.Warn("MsgSnap 描述符不可解析，跳过定向台账登记（判活将退化为按停滞上报）",
				"g", gr.g, "to", m.GetTo(), "err", err)
			continue
		}
		gr.snaps.NoteSent(gr.g, m.GetTo(), desc.ID)
		gr.lg.Info("向落后节点发出快照", "g", gr.g, "to", m.GetTo(),
			"snap_id", desc.ID, "index", desc.Index)
	}
}

// snapInstallRetryWindow / snapInstallRetryBase / snapInstallRetryMax 是
// 安装的重试预算：在 snapInstallRetryWindow 之内按 1s 起、翻倍、封顶
// snapInstallRetryMax 的退避反复重试。
//
// 为什么要重试：安装第 4 步是分钟级的网络操作（分块拉取），一次瞬时
// 错误（对端重启、连接 reset、控制帧超时）就直接 fail-stop panic，等于
// 把「网络抖了一下」升级成「进程自杀 + 该组重装一遍」。整个安装流程
// 幂等——标记可重写、wipe 可重跑、块可重拉，重试的代价只是重来一遍。
//
// 为什么预算按时间窗而不是按次数（终审 R3-2）：旧实现是「3 次，退避
// 1s+2s」，总窗口约 3 秒——而它自称要吸收的「对端重启」在真实部署里
// 是 10~60 秒级。3 秒的预算必然在对端重启完成之前耗尽，升级成 panic；
// 而 panic 是不干净关机，重启后 Start 直接返回 ErrUncleanShutdown，
// 恢复代价是整目录 WipeForRejoin + 全量重新同步。用「够长的时间窗 +
// 有上限的退避」才真正覆盖它声称覆盖的故障，同时 ctx 取消随时可退。
//
// 为什么窗口不是无限：真正不可恢复的错误（描述符不可解析、对端视图已
// 回收、磁盘写失败）重试多久都一样；超出窗口就该交给 fail-stop +
// leader 侧重驱动这条更彻底的恢复路径。
//
// 为什么是 var 而不是 const（同 maxSnapshotChunks 的理由）：生产窗口是
// 分钟级，而「注入永久失败 → 断言 fail-stop」的用例按生产窗口跑要空转
// 两分钟。测试临时改小、defer 还原，是让 fail-stop 语义本身保持可验证
// 的唯一低成本办法；生产路径不改这三个值。
var (
	snapInstallRetryWindow = 2 * time.Minute
	snapInstallRetryBase   = time.Second
	snapInstallRetryMax    = 15 * time.Second
)

// installSnapshotWithRetry 是 installSnapshot 的有限重试包装，并负责
// 安装期的两项全局状态：keepTicking（选举计时器不停摆）与 installing
// 标记（step 改为满则丢，见 step 注释的 I5 说明）。
//
// 参数：
//   - ctx: 组运行上下文；取消即立即放弃重试并返回最后一次错误
//   - snap: raft 交下来的快照（元数据 + 描述符 Data）
//
// 返回：全部尝试都失败时返回最后一次错误；调用方（appendOnce）据
// ctx 是否已取消区分「停机中止」与「真故障 fail-stop」。
//
// 注意：本方法返回后 installing 必然已清位、keepTicking 必然已收敛
// （defer 保证，含 panic 路径）。
func (gr *group) installSnapshotWithRetry(ctx context.Context, snap *raftpb.Snapshot) error {
	gr.installing.Store(true)
	stop := gr.keepTicking()
	defer func() {
		stop()
		gr.installing.Store(false)
		// 安装期丢弃计数打点：长期非零说明安装耗时已长到影响心跳投递，
		// 是「该把安装挪出 Ready 循环」的量化信号
		if n := gr.installDrops.Swap(0); n > 0 {
			gr.lg.Warn("安装期入站消息丢弃（inbox 满，raft 重发兜底）", "g", gr.g, "dropped", n)
		}
	}()
	var err error
	deadline := time.Now().Add(snapInstallRetryWindow)
	backoff := snapInstallRetryBase
	for attempt := 1; ; attempt++ {
		if err = gr.installSnapshot(ctx, snap); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err // 停机中：不再重试，交调用方按停机路径处理
		}
		// 退避到期时间已越过窗口即收工——判据用「睡完之后」而不是
		// 「此刻」，避免最后一次退避睡过头、把 2 分钟的窗口拖成 2 分 15 秒
		if !time.Now().Add(backoff).Before(deadline) {
			break
		}
		gr.lg.Warn("快照安装失败，退避重试（安装流程幂等，重来一遍）", "g", gr.g,
			"index", snap.Metadata.GetIndex(), "attempt", attempt,
			"backoff", backoff.String(), "window", snapInstallRetryWindow.String(), "err", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return err
		}
		if backoff *= 2; backoff > snapInstallRetryMax {
			backoff = snapInstallRetryMax
		}
	}
	gr.lg.Error("快照安装重试预算耗尽（转入 fail-stop）", "g", gr.g,
		"index", snap.Metadata.GetIndex(), "window", snapInstallRetryWindow.String(), "err", err)
	return err
}

func (gr *group) installSnapshot(ctx context.Context, snap *raftpb.Snapshot) error {
	// 第 1 步：解析描述符 → (snapID, leader, index)
	meta := snap.GetMetadata()
	desc, err := decodeSnapDescriptor(snap.GetData())
	if err != nil {
		return fmt.Errorf("快照安装第 1 步 解析描述符失败: %w", err)
	}
	if gr.control == nil {
		return errors.New("快照安装: 控制回调未装配（装配错配）")
	}
	if gr.rs == nil {
		return errors.New("快照安装: 日志持久层未装配（装配错配）")
	}
	gr.lg.Info("快照安装开始", "g", gr.g, "snap_id", desc.ID, "leader", desc.Leader, "index", desc.Index)
	start := time.Now()
	// 第 2 步：标记先行（Sync）——先标记后清空，顺序见方法注释
	if err := gr.rs.MarkInstalling(gr.g, meta); err != nil {
		return fmt.Errorf("快照安装第 2 步 写安装标记失败: %w", err)
	}
	// 第 3 步：清空本组旧状态（清空重来只碰本组键）
	if err := wipeGroupKeys(gr.st, gr.g, gr.groups); err != nil {
		return fmt.Errorf("快照安装第 3 步 清空旧状态失败: %w", err)
	}
	// 第 4 步：从生成节点分块拉取组状态并落盘（每块一个 NoSync 批次）
	if err := gr.pullSnapshotChunks(ctx, desc); err != nil {
		return err
	}
	// 第 5 步：收口批次（Sync）——applied、成员表、锚点、删标记四者
	// 同批原子落盘，这一批的原子性就是「安装完成」的定义（见方法注释）。
	// applied 与成员表由 writeConfState 一并写入（conf 键 + applied 键），
	// 与 SaveConfState 同一份核心，绝无分叉。
	b := gr.st.NewBatch()
	if err := writeConfState(b, gr.g, meta.GetConfState(), meta.GetIndex()); err != nil {
		b.Close()
		return fmt.Errorf("快照安装第 5 步 收口失败（写 applied/成员表）: %w", err)
	}
	if err := writeSnapMeta(b, gr.g, meta); err != nil {
		b.Close()
		return fmt.Errorf("快照安装第 5 步 收口失败（写锚点）: %w", err)
	}
	if err := deleteInstallingKey(b, gr.g); err != nil {
		b.Close()
		return fmt.Errorf("快照安装第 5 步 收口失败（删标记）: %w", err)
	}
	// 收口必须 Sync：这一批是「安装完成」的持久性承诺，NoSync 会在
	// 崩溃后留下「标记已删、数据未落」的窗口——重启当作完整状态启动
	if err := gr.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("快照安装第 5 步 收口失败: %w", err)
	}
	// 第 6 步：内存侧——mem.ApplySnapshot（元数据版，Data 置空即可，
	// 描述符只对「发送快照的节点」有意义）、applied/confState 推进。
	// applyMu 临界区与 applyEntries 同契约：Snapshot() 在同一把锁内读
	// applied + confState，位点与成员表必须配对提交。
	idx, tm := meta.GetIndex(), meta.GetTerm()
	ms := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: &idx, Term: &tm, ConfState: meta.GetConfState()}}
	gr.applyMu.Lock()
	if err := gr.mem.ApplySnapshot(ms); err != nil {
		gr.applyMu.Unlock()
		return fmt.Errorf("快照安装第 6 步 内存侧失败: %w", err)
	}
	gr.applied.Store(idx)
	if cs := meta.GetConfState(); cs != nil {
		gr.confState.Store(cs)
	}
	gr.applyMu.Unlock()
	// e2e 用这条日志当快照路径证据（文案不许改，见
	// TestLaggingFollowerCatchesUpBySnapshot 的 countLog 断言）
	gr.lg.Info("快照安装完成", "g", gr.g, "snap_id", desc.ID, "index", idx,
		"duration", time.Since(start).Round(time.Millisecond).String())
	return nil
}

// pullSnapshotChunks 从快照生成节点（描述符里的 leader）分块拉取组
// 状态并逐块落盘（installSnapshot 第 4 步）。
//
// 请求/响应线格式见 encodeSnapFetchReq/decodeSnapFetchResp；块内容用
// snapstream 的 decodeChunk 还原（与发送侧 encodeChunk 唯一配对）。
// 每块一个 NoSync 批次——持久性由收口批次（第 5 步）一次性承担，
// 中途崩溃由安装标记的「清空重来」兜底。
//
// 三条硬护栏（I4）——本循环的退出条件不能只靠对端诚实：
//   - 游标必须严格前进：next ≤ 上一游标即报错中止。发送侧的枚举单调性
//     一旦被破坏（键族表写错、对端版本不一致、恶意对端），症状是同一批
//     键反复重发、循环永不 done；校验游标把「静默死循环」变成一条明确的
//     协议错误。这也是 batch④ 首轮评审 C1 之所以表现为死循环而非报错的
//     直接原因；
//   - 块数上限 maxSnapshotChunks：兜住「每块都推进一点点但永远拉不完」；
//   - 累计字节上限 maxSnapshotBytes：兜住「块数不多但每块巨大」，也挡住
//     坏对端把本节点磁盘写爆。
//
// 两条上限都取得极宽（正常快照远达不到），命中即说明对端行为异常——
// 报错中止后由 fail-stop 与 leader 侧的失败感知（reportStalledSnapshots）
// 共同收敛，不会卡死。
func (gr *group) pullSnapshotChunks(ctx context.Context, desc snapDescriptor) error {
	var cursor []byte
	var nbytes int64 // 与 maxSnapshotBytes 同宽：32 位平台上 int 装不下上限
	keys, chunks := 0, 0
	for {
		chunks++
		if chunks > maxSnapshotChunks {
			return fmt.Errorf("快照安装第 4 步 块数超上限 %d（已拉 %d 键 %d B，对端枚举异常）",
				maxSnapshotChunks, keys, nbytes)
		}
		req := encodeSnapFetchReq(gr.g, desc.ID, cursor)
		resp, err := gr.control(ctx, desc.Leader, OpFetchSnapshot, req)
		if err != nil {
			return fmt.Errorf("快照安装第 4 步 拉块失败（第 %d 块）: %w", chunks, err)
		}
		done, next, chunk, err := decodeSnapFetchResp(resp)
		if err != nil {
			return fmt.Errorf("快照安装第 4 步 响应解码失败（第 %d 块）: %w", chunks, err)
		}
		// 游标单调性校验：done=true 的最后一块不再续扫，next 无意义，跳过
		if !done && cursor != nil && bytes.Compare(next, cursor) <= 0 {
			return fmt.Errorf("快照安装第 4 步 游标未前进（第 %d 块）：next=0x%s ≤ 上一游标 0x%s，对端枚举不单调",
				chunks, keyHexPrefix(next), keyHexPrefix(cursor))
		}
		pairs, err := decodeChunk(chunk)
		if err != nil {
			return fmt.Errorf("快照安装第 4 步 块内容解码失败（第 %d 块）: %w", chunks, err)
		}
		if len(pairs) > 0 {
			b := gr.st.NewBatch()
			for _, p := range pairs {
				if err := b.Set(p.k, p.v); err != nil {
					b.Close()
					return fmt.Errorf("快照安装第 4 步 写入失败（第 %d 块）: %w", chunks, err)
				}
			}
			// NoSync：崩溃只留下多余的半截键，重启见标记即重新清空
			if err := gr.st.ApplyWith(b, false); err != nil {
				return fmt.Errorf("快照安装第 4 步 提交失败（第 %d 块）: %w", chunks, err)
			}
			keys += len(pairs)
			for _, p := range pairs {
				nbytes += int64(8 + len(p.k) + len(p.v)) // 8B = 块格式双长度头
			}
			if nbytes > maxSnapshotBytes {
				return fmt.Errorf("快照安装第 4 步 累计字节 %d 超上限 %d（第 %d 块，已拉 %d 键，对端状态异常）",
					nbytes, maxSnapshotBytes, chunks, keys)
			}
		}
		if chunks%16 == 0 {
			gr.lg.Debug("快照拉取进度", "g", gr.g, "snap_id", desc.ID,
				"chunk", chunks, "keys", keys, "bytes", nbytes)
		}
		if done {
			break
		}
		cursor = next
	}
	gr.lg.Debug("快照拉取结束", "g", gr.g, "snap_id", desc.ID, "chunks", chunks, "keys", keys, "bytes", nbytes)
	return nil
}

// ccApplied 记录本轮已 apply 的成员变更条目：id 用于响应投递后唤醒
// ccWaiters（applyOnce：deliverResponses 之后才通知）；notify 为 false
// （变更不是本节点发起）时不通知——跨节点
// 条目 id 碰撞时通知会造成假成功（apply 的是别节点发起的变更）。
type ccApplied struct {
	id     uint64
	notify bool
}

// persistPhase 执行「日志持久化 + MemoryStorage 双记账」，是 raft 存储
// 契约里 append 侧的全部写入动作。
//
// 参数：
//   - hs: 本轮的 HardState，nil 或空表示无状态变更
//   - ents: 本轮要追加/覆盖的日志条目，可为空
//   - sync: 本次写入是否带 fsync（quorum-fsync 档的 MustSync 轮）
//
// 顺序即不变量（spec §5.2）：先 rs.Persist 落盘，再推 mem（raft 库读
// 日志的易失视图）。调用方必须在本方法返回**之后**才投递响应给 raft
// ——响应一旦投出，raft 就认为这些条目已 stable 并会立刻去 mem 读它们。
//
// 失败一律 panic（fail-stop）：持久化失败 = 内存日志与磁盘分叉，崩溃后
// 本节点已确认的条目会消失。记 Error 后静默返回只会让组永久卡死，比
// 进程死亡更糟——进程死亡由上层重启接管（走不干净关机判定）。
func (gr *group) persistPhase(hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) {
	if err := gr.rs.Persist(gr.g, hs, ents, sync); err != nil {
		gr.lg.Error("日志持久化失败，组停摆", "entries", len(ents), "sync", sync, "err", err)
		panic(err)
	}
	if !raft.IsEmptyHardState(hs) {
		_ = gr.mem.SetHardState(hs)
		// 缓存本轮的 term（currentTerm 的数据源）：空 HardState 的轮次
		// 不进这里，lastTerm 因此永远停留在「最近一次真实任期」
		gr.lastTerm.Store(hs.GetTerm())
	}
	_ = gr.mem.Append(ents)
}

// applyPhase 把一批已提交条目应用到 FSM，返回本批登记的成员变更 waiter。
//
// 参数：
//   - ents: 已提交待应用的条目（async 下来自 MsgStorageApply.Entries）
//
// 返回：
//   - 本批 apply 掉的 ConfChangeV2 登记（id + 是否本节点发起）。**通知
//     由调用方做**，不在本方法内——通知时机与 raft 内部 applied 位点的
//     推进强相关（见调用方注释），是调用方的责任。
//
// 两条顺序不变量（原 handleReady 的注释原样保留）：
//   - 普通条目攒段合批，遇成员变更必须先冲刷已积累的段——SaveConfState
//     用独立批次写成员表 + applied 位点，段若晚于它提交会把 applied 位点
//     倒退回段内更小的 index，位点单调性破坏；
//   - 跳过 index ≤ applied 的条目：raft 可能重发已 apply 过的条目
//     （conflict 回退重写后），FSM 已是该 index 的状态。
func (gr *group) applyPhase(ents []*raftpb.Entry) []ccApplied {
	// 本轮回合的成员变更登记：由调用方在正确时机通知（见调用点注释），
	// notify=false 表示该变更非本节点发起，不通知（超时兜底）
	appliedCC := make([]ccApplied, 0, 2)
	// 普通条目段积累（apply 合批）：连续的 EntryNormal 攒进 seg，由
	// applyEntries 合成单次引擎提交。遇到成员变更必须先冲刷已积累的段
	// ——SaveConfState 用独立批次写成员表 + applied 位点，段若晚于它
	// 提交会把 applied 位点倒退回段内更小的 index，位点单调性破坏。
	var seg []*raftpb.Entry
	flushSeg := func() {
		gr.applyEntries(seg)
		seg = seg[:0]
	}
	for _, ent := range ents {
		// 重启重放的幂等保证：raft 可能重发已 apply 过的条目
		// （conflict 回退重写后），跳过即可——FSM 已是该 index 的状态。
		// 注意 applied 只在段冲刷/成员变更时推进，段内递增的 index 不会
		// 被本判定误跳。
		if ent.GetIndex() <= gr.applied.Load() {
			continue
		}
		switch ent.GetType() {
		case raftpb.EntryConfChange:
			flushSeg()
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
			// applyMu 临界区（同 applyEntries）：Snapshot 在同一把锁内读
			// applied + confState，写批次与成员表必须与 applied 位点配对
			// 提交——否则可能产出「元数据 index=N 却携带更新的成员表」
			// 的快照，配对不变量被破坏
			gr.applyMu.Lock()
			if gr.rs != nil {
				if err := gr.rs.SaveConfState(gr.g, cs, ent.GetIndex()); err != nil {
					gr.applyMu.Unlock()
					gr.lg.Error("成员表持久化失败，组停摆", "index", ent.GetIndex(), "err", err)
					panic(err)
				}
				gr.confState.Store(cs) // Storage.Snapshot() 现场取用（Task 4）
			}
			gr.applied.Store(ent.GetIndex())
			gr.applyMu.Unlock()
			gr.lg.Debug("成员变更已 apply", "type", cc.GetType().String(), "node", cc.GetNodeId())
		case raftpb.EntryConfChangeV2:
			flushSeg()
			var v2 raftpb.ConfChangeV2
			v2.Reset()
			if err := proto.Unmarshal(ent.Data, &v2); err != nil {
				gr.lg.Error("ConfChangeV2 解码失败，组停摆", "index", ent.GetIndex(), "err", err)
				panic(err)
			}
			cs := gr.rn.ApplyConfChange(&v2)
			// 成员表 + applied 同批落盘：截断之后日志前缀不复存在，
			// 重启只能靠这份持久化成员表恢复（Task 3 的截断前提）
			// applyMu 临界区（同 applyEntries）：Snapshot 在同一把锁内读
			// applied + confState，写批次与成员表必须与 applied 位点配对
			// 提交——否则可能产出「元数据 index=N 却携带更新的成员表」
			// 的快照，配对不变量被破坏
			gr.applyMu.Lock()
			if gr.rs != nil {
				if err := gr.rs.SaveConfState(gr.g, cs, ent.GetIndex()); err != nil {
					gr.applyMu.Unlock()
					gr.lg.Error("成员表持久化失败，组停摆", "index", ent.GetIndex(), "err", err)
					panic(err)
				}
				gr.confState.Store(cs) // Storage.Snapshot() 现场取用（Task 4）
			}
			gr.applied.Store(ent.GetIndex())
			gr.applyMu.Unlock()
			// 成员变更的 waiter 通知放到本方法之外（见调用点注释）：
			// 这里只登记（id, 是否本节点发起），不直接通知；登记与
			// 通知都不是状态写入，留在锁外
			ccid, ours := ccWaiterInfo(&v2, gr.selfID)
			appliedCC = append(appliedCC, ccApplied{id: ccid, notify: ours})
			if ch := v2.GetChanges(); len(ch) > 0 {
				gr.lg.Debug("成员变更已 apply", "type", ch[0].GetType().String(), "node", ch[0].GetNodeId())
			}
		case raftpb.EntryNormal:
			// 条目数据布局：[8B 提案者][8B waiter id][batch repr]——
			// 载荷提取、waiter 通知（限定本节点提案）都在 applyEntries
			// 内按条目粒度处理，这里只积累
			seg = append(seg, ent)
		}
	}
	flushSeg()
	return appliedCC
}

// entryPayload 取普通条目的批次载荷：跳过 16B 头（[8B 提案者][8B waiter
// id]），空/短条目（选举 no-op 等）返回 nil。1B..16B 的非空短条目按疑似
// 损坏留痕——正常写路径不可能产出这种长度。
func (gr *group) entryPayload(ent *raftpb.Entry) []byte {
	if len(ent.Data) > 16 {
		return ent.Data[16:]
	}
	if len(ent.Data) > 0 {
		gr.lg.Warn("疑似损坏条目：普通条目数据 ≤16B 无批次载荷", "index", ent.GetIndex(),
			"len", len(ent.Data), "head", fmt.Sprintf("%x", ent.Data))
	}
	return nil
}

// applyEntries 把一段连续的普通条目合并成**单次**引擎提交应用到 FSM
// （apply 合批）：各条目的批次字节 MergeRepr 进同一批次，applied 位点只写
// 最后一条的 index，批提交 + 推 applied 在 applyMu 临界区内一次完成。
//
// 为什么合批：逐条提交时每条目走一遍 Pebble 提交流水线（WAL 记录、
// seqnum 发布、memtable 写入调度），高并发下一轮 Ready 常携带几十条
// CommittedEntries，逐条提交把 apply 变成每条一次的固定开销；合批后
// 该开销按轮摊销。三机压测剖面里 handleReady 的提交路径是前台热点之一。
//
// 语义与逐条路径逐项对齐：
//   - applied 位点与全部 FSM 数据同批原子（spec §5）——中间条目不再有
//     独立位点，但重启重放从上一位点起幂等重 apply，与逐条无差别；
//   - OnApplied 钩子与 waiter 通知仍按条目粒度、按条目序触发，只是统一
//     挪到合批提交之后——通知时数据必然已提交，读己之写不破坏；
//   - apply 失败照旧 fail-stop panic：状态机与日志分叉比进程死亡更糟——
//     分叉后本节点 FSM 与多数派永远不一致且无法被检测修复，重启重放只会
//     回到与日志一致的状态；panic 让进程死亡、由上层重启接管才是安全的
//     选择。这是刻意取舍，不是疏漏。
//
// 调用方（applyPhase）保证段内不含 ConfChange：成员变更走独立的
// SaveConfState 批次，遇到即先冲刷已积累的段（顺序不变量）。
func (gr *group) applyEntries(ents []*raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	b := gr.st.NewBatch()
	for _, ent := range ents {
		data := gr.entryPayload(ent)
		if len(data) == 0 {
			continue // 空条目没有 FSM 数据，只随批推进 applied 位点
		}
		if err := b.MergeRepr(data); err != nil {
			// 坏批次字节是严重的数据完整性问题，panic 前把载荷头留痕
			// （与原逐条路径的 NewBatchFromRepr 失败同边界同策略）
			_ = b.Close()
			gr.lg.Error("FSM 批次合并失败，组停摆", "index", ent.GetIndex(), "len", len(data),
				"first64", fmt.Sprintf("%x", data[:min(len(data), 64)]), "err", err)
			panic(err)
		}
	}
	last := ents[len(ents)-1].GetIndex()
	if err := b.Set(appliedKey(gr.g), store.PutU64(last)); err != nil {
		_ = b.Close()
		gr.lg.Error("写 applied 位点失败，组停摆", "index", last, "err", err)
		panic(err)
	}
	// 「批提交 + 推 applied」在 applyMu 临界区内（Task 4）：快照生成在
	// 同一把锁内读 applied 并建 ReadView，二者互斥——视图内容恰好对应
	// 该位点。apply 总是 NoSync：持久性由 raft 日志与后台批量刷盘承担
	// （spec §5），不另起 fsync。
	gr.applyMu.Lock()
	if err := gr.st.ApplyWith(b, false); err != nil {
		gr.applyMu.Unlock()
		gr.lg.Error("FSM apply 失败，组停摆", "last", last, "entries", len(ents), "err", err)
		panic(err)
	}
	gr.applied.Store(last)
	gr.applyMu.Unlock()
	// 钩子与 waiter 通知在提交之后、按条目序逐条触发（粒度不变，见上）
	for _, ent := range ents {
		gr.notifyApplied(gr.entryPayload(ent))
		if id, ok := proposalWaiter(ent.Data, gr.selfID); ok {
			gr.notifyWaiter(gr.propWaiters, id)
		}
	}
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
// 稳态下全量入队不会撑爆 inbox：单组在途消息 ≤ 2×MaxInflightMsgs
// （256）=512，小于 inbox 容量 1024——三节点拓扑下每条消息至多往返
// 一次（出站 + 对端回包），raft 的 inflight 上限即入队总量上界；
// 单节点组稳态不产自消息（自我投递仅出现在选举期）。
//
// 组已退出后（doneCh 关闭）丢弃消息而不阻塞（终审 R4）：传输读
// goroutine 由整条连接共享，阻塞在某一组的 step 会拖死同连接上其余
// 所有组的消息投递——raft 重试与上层编排是丢弃的兜底。
//
// 安装期改为「满则丢弃」（I5）：上面那条 inflight 不变量只覆盖稳态——
// 安装期主循环可能阻塞在本地通道入队上而不再消费 inbox，而 leader 的心跳按
// HeartbeatTick 持续到达（100ms tick ≈ 10 条/秒），与 inflight 无关地
// 单向累积；生产级快照是分钟级操作，约 100 秒即填满 1024 的队列，此后
// step 阻塞的是**整条连接**的读循环，同连接上其余所有组的消息投递一起
// 停摆。丢心跳无害（raft 按 tick 重发，选举计时由 keepTicking 维持），
// 拖死其它组则是真故障，故安装期宁可丢。丢弃计数在安装结束时打点。
//
// 注意：m 必须为指针——v3 的 raftpb.Message 内嵌互斥锁
// （protoimpl.MessageState），按值传递会触发 vet copylocks，
// 且与 rn.Step 的指针签名天然一致。
func (gr *group) step(m *raftpb.Message) {
	if gr.installing.Load() {
		select {
		case gr.inbox <- m:
		case <-gr.doneCh:
		default:
			gr.installDrops.Add(1) // 安装期队列满：丢弃，raft 重发兜底
		}
		return
	}
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

// currentTerm 返回最近一次非空 HardState 缓存的 term（leader 变更
// 日志取它，而非 rd.HardState.GetTerm()）。
//
// 为什么不能直接读 rd.HardState.GetTerm()：HardState 只在 term/commit/
// vote 任一有变化的那一轮非空，其余轮次是空结构体、GetTerm() 恒为 0
// ——而 leader 变更（SoftState 变化）恰恰常常与 HardState 变化不在
// 同一轮（当选那一轮 SoftState 先变、HardState 下一轮才落盘），直接
// 取值会在日志里打出 term=0，误导排查（backlog「首轮 leader 日志
// term=0」）。lastTerm 在 HardState 非空的那一轮更新（见 persistPhase），
// 是「最近真实任期」的准确缓存。
func (gr *group) currentTerm() uint64 {
	return gr.lastTerm.Load()
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
