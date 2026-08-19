// snapshot.go 提供 raft 快照的发送侧机件：按需生成的 raft.Storage 包装、
// 快照描述符编解码、以及钉住状态的读视图注册表。
//
// 职责：
//   - groupStorage：包装 MemoryStorage，把 Snapshot() 从「返回预置快照」
//     改为「现场生成」——取当前 applied 与同一时刻的 ReadView，
//     元数据带真实 {Index, Term, ConfState}，Data 只放几十字节描述符
//   - snapRegistry：按 snapID 持有 ReadView，供控制通道分块拉取；
//     借用计数（Get 借出/Put 归还）+ 借出续期（每次借出刷新 TTL 基线）；
//     TTL 到期强制回收，另有不可续期的硬上限兜底（视图长期不关会阻止
//     Pebble 回收旧版本）；NoteSent/PeerBorrow 供 leader 侧按「发给哪个
//     peer 的哪份快照」精确判定对端是否真在拉
//   - 描述符编解码：[8B snapID][8B leader nodeID][8B index]
//
// 边界：
//   - 不搬运真实状态字节——那是 snapshotstream.go（Task 5）与控制通道
//     OpFetchSnapshot（Task 6）的事
//   - 不决定何时截断——那是 Manager 的截断循环（Task 8）
package cluster

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/Xsxdot/sq/internal/store"
)

// snapRegistryDefaultTTL 是快照视图的默认存活时长：超时未被拉完的
// 视图会被 GC 强制回收。持有视图会阻止 Pebble 回收被覆盖的旧版本，
// 泄漏即磁盘涨——TTL 是视图生命周期的兜底下限。生产值由配置面注入
// （cluster.snapshot_view_ttl → Options.SnapshotViewTTL），本常量只在
// 未填时兜底。
const snapRegistryDefaultTTL = 5 * time.Minute

// snapViewHardTTLFactor 是视图硬上限相对 TTL 的倍数：视图从建档起活过
// ttl×该倍数即被强制作废，**不受借出续期影响**。
//
// 为什么续期之外还要硬上限：Get 每次借出都把 created 推到当下，只要对端
// 持续发 OpFetchSnapshot（哪怕游标原地不动、哪怕是有 bug 或恶意的对端），
// 软 TTL 就永不到期——续期把「大库传输被误杀」的活锁修好的同时，也把
// TTL 这道「视图必然被回收」的闸整个让掉了，退化成无限期钉住 Pebble 旧
// 版本的磁盘泄漏。硬上限是不可续期的兜底：命中即作废（拒绝新借出），
// 待在途借用归还后立即 Close。10 倍留给正常大库传输足够余量（默认
// 5min×10=50min），又保证泄漏有确定上界。
const snapViewHardTTLFactor = 10

// snapDescriptor 是快照 Data 载荷的编码内容：用几十字节代替真实状态
// 字节，接收方据此从源节点分块拉取（Task 6 的 OpFetchSnapshot）。
//
// 布局：[8B snapID][8B leader nodeID][8B index]。
type snapDescriptor struct {
	ID     uint64 // 源节点 snapRegistry 里的登记 id
	Leader uint64 // 生成快照的节点 id（接收方拉取时连它）
	Index  uint64 // 快照覆盖到的 applied 位点
}

// encodeSnapDescriptor 编码描述符：[8B snapID][8B leader][8B index]，
// 全部大端。
func encodeSnapDescriptor(d snapDescriptor) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b[:8], d.ID)
	binary.BigEndian.PutUint64(b[8:16], d.Leader)
	binary.BigEndian.PutUint64(b[16:24], d.Index)
	return b
}

// decodeSnapDescriptor 解码描述符；长度非 24B 按协议错报错（对端或
// 盘上数据损坏，不可静默降级）。
func decodeSnapDescriptor(b []byte) (snapDescriptor, error) {
	if len(b) != 24 {
		return snapDescriptor{}, fmt.Errorf("cluster: 快照描述符长度 %d 非法（want 24）", len(b))
	}
	return snapDescriptor{
		ID:     binary.BigEndian.Uint64(b[:8]),
		Leader: binary.BigEndian.Uint64(b[8:16]),
		Index:  binary.BigEndian.Uint64(b[16:24]),
	}, nil
}

// snapEntry 是注册表里一条视图登记。
type snapEntry struct {
	view    *store.ReadView
	created time.Time // 建立时刻：GCOnce 按 ttl 判活的基线；Get 借出时刷新（续期）
	bornAt  time.Time // 建档时刻：**永不刷新**，硬上限（ttl×hardTTLFactor）的基线
	revoked bool      // 已作废：拒绝新借出，refs 归零即 Close（硬上限命中时置位）
	g       uint32    // 所属组：日志上下文（排查视图泄漏定位用）
	index   uint64    // 快照位点：日志上下文
	refs    uint64    // 未归还的借用数：>0 时 GCOnce/Release 不得 Close 视图
}

// snapRegistry 按 snapID 持有 ReadView：raft 的 Snapshot() 现场生成
// 描述符时把视图钉住登记，控制通道按 snapID 分块拉取（Task 6）；
// TTL 到期由 GCOnce 强制回收——视图长期不关会阻止 Pebble 回收旧版本。
//
// 并发模型（为什么是引用计数而非 RWMutex）：视图的 Close 只允许在
// refs==0 时发生，而 refs 的全部变更（Get 借出/Put 归还/GCOnce 判活/
// Release 注销）都在 r.mu 内完成——「借出 → 扫描 → 归还」期间 refs>0，
// Close 无从插入，扫描天然安全。RWMutex 的方案是「扫描持读锁、GC 持
// 写锁 Close」，但扫描期间持读锁会把全部读与 GC 串行化（慢扫描阻塞
// 整表），且 RUnlock 后视图仍可能被 Close 出竞态窗口；引用计数把正确
// 性建立在不变式上而非锁的持有范围，扫描不需要持任何锁，GC 也不会被
// 长扫描阻塞。ReadView.closed.Load() 检查保留为纵深防御。
type snapRegistry struct {
	st  *store.Store  // 建 ReadView 的源（与 FSM 同库）
	ttl time.Duration // 视图存活时长，超时强制回收
	lg  *slog.Logger

	// now 是时间源：生产用 time.Now，测试注入假时钟使 TTL/续期断言
	// 确定性。所有「当前时刻」判断（Create 建档、Get 续期）经它取。
	now func() time.Time

	mu     sync.Mutex           // 保护以下字段
	nextID uint64               // 单调自增 snapID（从 1 起，永不复用）
	views  map[uint64]snapEntry // snapID → 视图登记

	// sent 是「本节点作为 leader 发给哪个 peer 哪份快照」的定向台账
	// （见 NoteSent/PeerBorrow）。规模上界是 组数 × 成员数，条目被同
	// (g, to) 的后一次发送覆盖，不会无界增长。
	sent map[snapSendKey]uint64 // (组, 目标节点) → snapID
}

// snapSendKey 是 sent 台账的键：一个 peer 在一个组里同时只可能处于
// 一份快照的接收中（raft 的 Progress.PendingSnapshot 是单值）。
type snapSendKey struct {
	g  uint32
	to uint64
}

// newSnapRegistry 构造快照注册表。st 是建 ReadView 的源，ttl 是视图
// 存活时长，lg 为结构化日志。
func newSnapRegistry(st *store.Store, ttl time.Duration, lg *slog.Logger) *snapRegistry {
	return &snapRegistry{
		st:    st,
		ttl:   ttl,
		lg:    lg,
		now:   time.Now,
		views: make(map[uint64]snapEntry),
		sent:  make(map[snapSendKey]uint64),
	}
}

// Create 建立一份钉住当前时刻的 ReadView 并登记，返回自增 snapID。
//
// 视图内容与调用时刻的 FSM 一致——「该时刻 = 声明的 applied 位点」的
// 对应关系由调用方（groupStorage.Snapshot）在 applyMu 临界区内保证，
// 本方法自身不做位点判断。
//
// 注意：Create 不返回视图——原始指针直接外露会绕过借用计数，制造
// 「无借用扫描」的竞态窗口。拉取方一律经 Get 借出视图。
//
// 返回：
//   - id: 自增 snapID（>0），拉取方经 Get(id) 借出视图，用完 Put(id)
func (r *snapRegistry) Create(g uint32, index uint64) (id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	id = r.nextID
	now := r.now()
	r.views[id] = snapEntry{view: r.st.NewReadView(), created: now, bornAt: now, g: g, index: index}
	r.lg.Debug("快照视图已登记", "snap_id", id, "g", g, "index", index)
	return id
}

// Get 借出指定 snapID 的视图并续期：refs+1、created 刷新为当前时刻。
//
// 借出语义：
//   - 借出期间（直到 Put/Release 归还）GCOnce 与 Release 都不得 Close
//     该视图——借出者扫描视图的安全性由 refs>0 保证
//   - 续期让「活跃拉取中的视图」TTL 永不自然到期：每块拉取都刷新
//     created，传输窗口超过固定 TTL 也不中断（旧的「Get 不续期」语义
//     在大库传输上活锁：回收 → 对端重试 → 再回收）
//   - 续期不能无限：已被硬上限作废（revoked）的视图拒绝借出，见
//     snapViewHardTTLFactor——否则「一直请求但游标不推进」的对端可以
//     把视图永久钉住
//
// 调用方必须配对调用 Put(id) 归还；已释放/已回收/已作废时 ok=false
// （无借用，调用方不得再 Put）。
func (r *snapRegistry) Get(id uint64) (*store.ReadView, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.views[id]
	if !ok || e.revoked {
		return nil, false
	}
	e.refs++
	e.created = r.now()
	r.views[id] = e // 写回：refs/created 的变更必须落回登记表
	return e.view, true
}

// NoteSent 登记「本节点作为 leader 把 snapID 这份快照发给了 peer to」。
//
// 为什么需要定向台账（终审 R3-1）：按 (g, index) 反查会把同一位点上的
// 全部视图聚合成一个「有没有人在拉」的判断——peer A 正在正常拉块，就
// 把 peer B 的彻底停摆一并掩盖掉，B 的失败上报要等到 A 传完 + 软 TTL
// 自然到期才可能触发。快照是发给谁的只有发送侧知道（raft 的
// Progress 只留 PendingSnapshot 位点，不留 snapID），因此在 MsgSnap
// 外发时把 (组, 目标) → snapID 记下来，判活时按这份精确对应查。
//
// 幂等：同 (g, to) 重复登记直接覆盖——raft 给同一 peer 换发新快照时
// 旧那份已无意义。
func (r *snapRegistry) NoteSent(g uint32, to uint64, id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent[snapSendKey{g: g, to: to}] = id
	r.lg.Debug("快照已发出，登记定向台账", "g", g, "to", to, "snap_id", id)
}

// SentTo 返回定向台账里登记的「最近发给 peer to 的快照 id」，不校验视图
// 是否仍在册——判活请用 PeerBorrow。用于验证台账确实被真实发送路径填上
// （视图在对端追平后就被回收了，那之后 PeerBorrow 只会返回 ok=false，
// 无法回答「到底登记过没有」）。
func (r *snapRegistry) SentTo(g uint32, to uint64) (id uint64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok = r.sent[snapSendKey{g: g, to: to}]
	return id, ok
}

// PeerBorrow 返回「leader 最近发给 peer to 的那份快照视图」的借用实况，
// 供 leader 侧失败感知（N1 / reportStalledSnapshots）判定对端是否真在拉。
//
// 参数 index 是调用方从 raft Progress.PendingSnapshot 读到的快照位点，
// 用于校验台账没有过期：台账里的 snapID 必须仍在册且位点相符，否则
// 视为「这份快照对端已不可能拉完」。
//
// 返回：
//   - last: 该视图最近一次借出时刻（从未借出过的返回建档时刻，等于给
//     对端一个完整的启动宽限期）
//   - borrowing: 此刻是否有在途借用（refs>0）。单块传输耗时超过判定
//     窗口时 last 会显得陈旧，但借用仍在——它同样是「对端活着」的证据
//     （终审 R3-3）
//   - ok: 台账与视图是否都还有效；false 表示视图已回收/作废/位点不符/
//     从未给该 peer 发过快照
func (r *snapRegistry) PeerBorrow(g uint32, to uint64, index uint64) (last time.Time, borrowing bool, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.sent[snapSendKey{g: g, to: to}]
	if !ok {
		return time.Time{}, false, false
	}
	e, ok := r.views[id]
	if !ok || e.revoked || e.g != g || e.index != index {
		return time.Time{}, false, false
	}
	return e.created, e.refs > 0, true
}

// Put 归还一次 Get 借出的视图：refs-1。条目保留在登记表里供后续分块
// 续拉（多块传输之间 refs=0 的窗口内不注销），TTL 到期由 GCOnce 回收。
//
// 注意：
//   - 必须在最后一次使用视图之后调用（调用方契约，与任何 use-after-
//     close 约定相同）；对从未借出的 id 调用是编程错误，打 Warn 防御
//   - refs 不为负数：重复归还只告警、不再扣减，避免回绕把条目变成
//     「永不回收」的泄漏
//   - 已被硬上限作废（revoked）的条目在 refs 归零的这一刻立即注销并
//     Close——作废的语义是「等在途借用还完就走」，等下一轮 GC 只会白白
//     多钉住一个 GC 周期的 Pebble 旧版本
func (r *snapRegistry) Put(id uint64) {
	r.mu.Lock()
	e, ok := r.views[id]
	if ok && e.refs == 0 {
		r.lg.Warn("快照视图归还不匹配：无未归还借用（重复 Put 或未 Get 即 Put）",
			"snap_id", id, "g", e.g, "index", e.index)
	}
	closeNow := false
	if ok && e.refs > 0 {
		e.refs--
		if e.refs == 0 && e.revoked {
			delete(r.views, id) // 作废条目还清借用：就地注销
			closeNow = true
		} else {
			r.views[id] = e // 写回：refs 的变更必须落回登记表
		}
	}
	r.mu.Unlock()
	// Close 放在锁外：与 GCOnce 同一纪律，不让 Close 阻塞其它 Get/Create
	if closeNow {
		_ = e.view.Close()
		r.lg.Info("快照视图作废后归还即回收", "snap_id", id, "g", e.g, "index", e.index)
	}
}

// Release 注销指定快照视图：从登记表移除并 Close，立即释放 Pebble
// 旧版本。仅在无未归还借用（refs==0）时生效——还有借用者时跳过并告警，
// 条目留待借用者归还后由 TTL GC 兜底回收。
//
// 适用场景：快照生成失败路径（Create 后未发出描述符，无人能借出）、
// 测试收尾。持有借用的调用方应走 Put。
func (r *snapRegistry) Release(id uint64) {
	r.mu.Lock()
	e, ok := r.views[id]
	if ok && e.refs == 0 {
		delete(r.views, id)
	}
	r.mu.Unlock()
	if !ok {
		return
	}
	if e.refs == 0 {
		_ = e.view.Close()
		r.lg.Debug("快照视图已注销", "snap_id", id, "g", e.g, "index", e.index)
	} else {
		r.lg.Warn("快照视图注销跳过：尚有未归还借用，待归还后由 GC 兜底",
			"snap_id", id, "g", e.g, "index", e.index, "refs", e.refs)
	}
}

// WasCreated 判定 snapID 是否曾在本注册表分配过（id ≤ nextID）。
//
// snapID 单调自增、永不复用：Get(id) 未命中时据此区分「从未生成过」
// （id 超出已分配空间，请求方持有陈旧描述符）与「已释放/已超时回收」
// （TTL 过期是常见原因）——handleFetchSnapshot 的 Warn 日志用它。
// nextID 只增不减，判定结果不随并发分配漂移。
func (r *snapRegistry) WasCreated(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return id <= r.nextID
}

// GCOnce 回收全部已过期（created+ttl ≤ now）且无借用（refs==0）的
// 视图，返回回收数量。
//
// 视图泄漏是磁盘涨的元凶：持有视图期间 Pebble 不回收被覆盖的旧版本，
// 超时未拉完的必须强制回收（拉取方对端拿到错误后重试新快照）。
// 借出中的视图（refs>0）即使超 TTL 也跳过：Close 会炸正在扫描的借用
// 者（pebble 对已关快照建迭代器直接 panic，无 recover 则进程死亡）——
// 借出是一次分块扫描，短命，顺延到下一轮 GC 即可。
//
// 硬上限（bornAt + ttl×snapViewHardTTLFactor，不受借出续期影响）：
//   - refs==0：与软 TTL 同路径，直接回收；
//   - refs>0：置 revoked（拒绝新借出），待在途借用经 Put 归还时立即
//     Close。作废不 Close 是同一条不变式——refs>0 时 Close 必炸借用者。
//
// 回收打 Info 并带存活时长，保证泄漏可观测。
func (r *snapRegistry) GCOnce(now time.Time) int {
	// 先快照出过期集合再统一释放：Close 不持 r.mu（只注销时持），
	// 避免长时间持有视图的 Close 阻塞其它 Get/Create
	hard := r.ttl * snapViewHardTTLFactor
	ids := make([]uint64, 0)
	stale := make([]snapEntry, 0)
	reasons := make([]string, 0)
	revoked := make([]snapEntry, 0)
	revokedIDs := make([]uint64, 0)
	r.mu.Lock()
	for id, e := range r.views {
		expired := now.Sub(e.created) >= r.ttl
		overHard := now.Sub(e.bornAt) >= hard
		if e.refs == 0 && (expired || overHard || e.revoked) {
			ids = append(ids, id)
			stale = append(stale, e)
			reason := "ttl"
			if overHard {
				reason = "hard-ttl"
			} else if e.revoked {
				reason = "revoked"
			}
			reasons = append(reasons, reason)
			delete(r.views, id)
			continue
		}
		// refs>0 且撞硬上限：作废而非 Close——Close 会炸在途借用者
		if overHard && !e.revoked {
			e.revoked = true
			r.views[id] = e
			revokedIDs = append(revokedIDs, id)
			revoked = append(revoked, e)
		}
	}
	r.mu.Unlock()
	for i, id := range revokedIDs {
		e := revoked[i]
		r.lg.Warn("快照视图撞硬上限，已作废（拒绝新借出，待在途借用归还即回收）",
			"snap_id", id, "g", e.g, "index", e.index, "refs", e.refs,
			"age", now.Sub(e.bornAt).Round(time.Second).String(), "hard_ttl", hard.String())
	}
	for i, id := range ids {
		e := stale[i]
		_ = e.view.Close()
		r.lg.Info("快照视图回收", "snap_id", id, "g", e.g, "index", e.index,
			"reason", reasons[i], "age", now.Sub(e.bornAt).Round(time.Second).String())
	}
	return len(ids)
}

// groupStorage 是包在 MemoryStorage 外的 raft.Storage 包装：除 Snapshot
// 外的全部方法直通 mem，Snapshot 现场生成描述符。
//
// raft 库经它读日志（Term/Entries/LastIndex/FirstIndex 与 InitialState），
// 需要给落后 follower 发 MsgSnap 时调用 Snapshot()——「快照是现场拍的」，
// 而不是启动时预置的。
type groupStorage struct {
	g         uint32
	mem       *raft.MemoryStorage
	reg       *snapRegistry
	applied   *atomic.Uint64                    // 指向组内 applied（组持有）
	confState *atomic.Pointer[raftpb.ConfState] // 指向组内 confState（组持有）
	applyMu   *sync.Mutex                       // 与组 apply 路径共享的锁（组持有并传入）
	selfID    uint64                            // 本节点 id：写进描述符的 leader 字段
	lg        *slog.Logger
}

// newGroupStorage 构造快照包装。applied/confState/applyMu 均指向组内
// 实例：Snapshot 与组的 apply 路径在 applyMu 上互斥，读 applied 与建
// ReadView 才能原子配对（见 Snapshot 的顺序不变量注释）。
func newGroupStorage(g uint32, mem *raft.MemoryStorage, reg *snapRegistry, applied *atomic.Uint64, cs *atomic.Pointer[raftpb.ConfState], selfID uint64, applyMu *sync.Mutex, lg *slog.Logger) *groupStorage {
	return &groupStorage{g: g, mem: mem, reg: reg, applied: applied, confState: cs, applyMu: applyMu, selfID: selfID, lg: lg}
}

// InitialState 直通 mem：重启时 raft 取成员表与 HardState 用。
func (s *groupStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	return s.mem.InitialState()
}

// Entries 直通 mem：raft 追齐时读日志段。
func (s *groupStorage) Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error) {
	return s.mem.Entries(lo, hi, maxSize)
}

// Term 直通 mem：任期比较用。
func (s *groupStorage) Term(i uint64) (uint64, error) {
	return s.mem.Term(i)
}

// LastIndex 直通 mem：日志末位。
func (s *groupStorage) LastIndex() (uint64, error) {
	return s.mem.LastIndex()
}

// FirstIndex 直通 mem：日志起点（截断后 = 截断点+1）。
func (s *groupStorage) FirstIndex() (uint64, error) {
	return s.mem.FirstIndex()
}

// Snapshot 现场生成一份快照描述符。raft 在需要给落后 follower 发
// MsgSnap 时调用本方法。
//
// 顺序不变量：**先取 applied、再建 ReadView** 是错的，反过来也是错的——
// 二者必须在同一临界区内取。实现上先加锁，读 applied，再建视图，
// 期间 apply 路径拿不到锁 → 视图内容恰好对应该 applied 位点。
// （若先建视图后读 applied，读到的位点可能已被后续 apply 推高，
// 快照就会声称包含它其实没有的数据 = 静默丢消息。）
func (s *groupStorage) Snapshot() (*raftpb.Snapshot, error) {
	s.applyMu.Lock()
	index := s.applied.Load()
	cs := s.confState.Load()
	id := s.reg.Create(s.g, index)
	s.applyMu.Unlock()

	term, err := s.mem.Term(index)
	if err != nil {
		// index 已被截断到不可查 term：本轮放弃，raft 下轮重试
		s.reg.Release(id)
		s.lg.Warn("快照生成放弃：applied 位点的 term 不可查", "g", s.g, "index", index, "err", err)
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	idx, tm := index, term
	snap := raftpb.Snapshot{
		Data:     encodeSnapDescriptor(snapDescriptor{ID: id, Leader: s.selfID, Index: index}),
		Metadata: &raftpb.SnapshotMetadata{Index: &idx, Term: &tm, ConfState: cs},
	}
	s.lg.Info("快照描述符已生成", "g", s.g, "snap_id", id, "index", index, "term", term)
	return &snap, nil
}
