// snapshot.go 提供 raft 快照的发送侧机件：按需生成的 raft.Storage 包装、
// 快照描述符编解码、以及钉住状态的读视图注册表。
//
// 职责：
//   - groupStorage：包装 MemoryStorage，把 Snapshot() 从「返回预置快照」
//     改为「现场生成」——取当前 applied 与同一时刻的 ReadView，
//     元数据带真实 {Index, Term, ConfState}，Data 只放几十字节描述符
//   - snapRegistry：按 snapID 持有 ReadView，供控制通道分块拉取；
//     TTL 到期强制回收（视图长期不关会阻止 Pebble 回收旧版本）
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

	"github.com/xushixin/sq/internal/store"
)

// snapRegistryDefaultTTL 是快照视图的默认存活时长：超时未被拉完的
// 视图会被 GC 强制回收。持有视图会阻止 Pebble 回收被覆盖的旧版本，
// 泄漏即磁盘涨——TTL 是视图生命周期的兜底下限。生产值由 Task 8 的
// 配置面接管，本常量只做装配默认。
const snapRegistryDefaultTTL = 5 * time.Minute

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
	created time.Time // 建立时刻：GCOnce 按 ttl 判活的基线
	g       uint32    // 所属组：日志上下文（排查视图泄漏定位用）
	index   uint64    // 快照位点：日志上下文
}

// snapRegistry 按 snapID 持有 ReadView：raft 的 Snapshot() 现场生成
// 描述符时把视图钉住登记，控制通道按 snapID 分块拉取（Task 6）；
// TTL 到期由 GCOnce 强制回收——视图长期不关会阻止 Pebble 回收旧版本。
type snapRegistry struct {
	st  *store.Store  // 建 ReadView 的源（与 FSM 同库）
	ttl time.Duration // 视图存活时长，超时强制回收
	lg  *slog.Logger

	mu     sync.Mutex           // 保护以下字段
	nextID uint64               // 单调自增 snapID（从 1 起，永不复用）
	views  map[uint64]snapEntry // snapID → 视图登记
}

// newSnapRegistry 构造快照注册表。st 是建 ReadView 的源，ttl 是视图
// 存活时长，lg 为结构化日志。
func newSnapRegistry(st *store.Store, ttl time.Duration, lg *slog.Logger) *snapRegistry {
	return &snapRegistry{st: st, ttl: ttl, lg: lg, views: make(map[uint64]snapEntry)}
}

// Create 建立一份钉住当前时刻的 ReadView 并登记，返回自增 snapID。
//
// 视图内容与调用时刻的 FSM 一致——「该时刻 = 声明的 applied 位点」的
// 对应关系由调用方（groupStorage.Snapshot）在 applyMu 临界区内保证，
// 本方法自身不做位点判断。
//
// 返回：
//   - id: 自增 snapID（>0），拉取方经 Get(id) 取视图
//   - view: 一致性只读视图，拉取方用完须 Release（或等 GC）
func (r *snapRegistry) Create(g uint32, index uint64) (id uint64, view *store.ReadView) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	id = r.nextID
	view = r.st.NewReadView()
	r.views[id] = snapEntry{view: view, created: time.Now(), g: g, index: index}
	r.lg.Debug("快照视图已登记", "snap_id", id, "g", g, "index", index)
	return id, view
}

// Get 取回指定 snapID 的视图；已释放/已回收时 ok=false。
func (r *snapRegistry) Get(id uint64) (*store.ReadView, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.views[id]
	if !ok {
		return nil, false
	}
	return e.view, true
}

// Release 释放并注销指定视图（幂等：重复释放/已回收时静默）。
func (r *snapRegistry) Release(id uint64) {
	r.mu.Lock()
	e, ok := r.views[id]
	if ok {
		delete(r.views, id)
	}
	r.mu.Unlock()
	if ok {
		_ = e.view.Close()
		r.lg.Debug("快照视图已释放", "snap_id", id, "g", e.g, "index", e.index)
	}
}

// GCOnce 回收全部已过期（created+ttl ≤ now）的视图，返回回收数量。
//
// 视图泄漏是磁盘涨的元凶：持有视图期间 Pebble 不回收被覆盖的旧版本，
// 超时未拉完的必须强制回收（拉取方对端拿到错误后重试新快照）。
// 回收打 Info 并带存活时长，保证泄漏可观测。
func (r *snapRegistry) GCOnce(now time.Time) int {
	// 先快照出过期集合再统一释放：Close 不持 r.mu（只注销时持），
	// 避免长时间持有视图的 Close 阻塞其它 Get/Create
	ids := make([]uint64, 0)
	stale := make([]snapEntry, 0)
	r.mu.Lock()
	for id, e := range r.views {
		if now.Sub(e.created) >= r.ttl {
			ids = append(ids, id)
			stale = append(stale, e)
			delete(r.views, id)
		}
	}
	r.mu.Unlock()
	for i, id := range ids {
		e := stale[i]
		_ = e.view.Close()
		r.lg.Info("快照视图超时回收", "snap_id", id, "g", e.g, "index", e.index, "lifetime", r.ttl.String())
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
	id, _ := s.reg.Create(s.g, index)
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
