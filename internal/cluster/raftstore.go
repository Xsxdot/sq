// raftstore.go 提供 raft 日志的共库持久化层。日志条目与 HardState 的
// 物理归宿是 seglog 分段追加日志（<data_dir>/raftlog/<g>/，Task 5 迁移），
// 其余元数据（成员表/快照锚点/组数/干净关机标记/机器世代/本地恢复许可）
// 仍在共库 store.Store 里，键全部落在 raft/ 前缀下，与 FSM 数据同库隔离。
//
// 职责：
//   - 每轮 Ready 的 HardState + Entries 单批持久化（Persist，落盘目标是
//     该组的 seglog 段日志）
//   - 回退覆盖语义：seglog 是纯追加日志，物理上不做删除；新条目写入后，
//     旧的更高 index 条目不是被物理删掉，而是在重放/读取时按「后写的赢」
//     规则从可见结果里裁掉——覆盖语义由「同批物理删除」变成了「读时重放
//     裁剪」，对调用方完全透明（见 Persist/Load 的注释）
//   - 成员表的持久化（SaveConfState/LoadConfState）与重启恢复
//     （Load + 干净关机判定）：成员表优先读持久化值，confStateFromEntries
//     的日志重放合成仅作为旧数据目录的迁移路径
//   - 快照锚点与日志截断（SaveSnapMeta/LoadSnapMeta/TruncateLog）：
//     顺序契约恒为「先锚点后截断」——锚点 Sync 落盘是截断的前提，
//     截断是范围删除；重启时锚点的 {Index, Term, ConfState} 就是
//     MemoryStorage 快照的位点与成员表来源
//   - 数据组数契约（EnsureGroups）与干净关机标记（Mark/ConsumeCleanShutdown）
//
// 边界：
//   - 不持有 pebble：元数据的一切写经 store 唯一写入口（NewBatch +
//     ApplyWith），本层没有裸 db 句柄——B2 唯一写入口在集群层同样成立；
//     日志条目/HardState 的写入口则是 seglog.Log.Append（同一约束的
//     seglog 侧对应版本，见 seglog 包）
//   - applied 的写入两处：普通条目经 FSM apply 批次并进，ConfChange
//     条目经 SaveConfState 与成员表同批
//
// 键布局（共库下以 raft/ 前缀隔离元数据区与 FSM 区；键内定长二进制大端，
// 保证字节序=数值序，区间扫描天然升序）：
//
//	raft/groups                  → uint32 BE 数据组数（首启写入，此后校验）
//	raft/clean_shutdown          → 干净关机标记（StopClean 写，启动读后删）
//	raft/preraft                 → 前 raft 期存量数据标记（单机档直写 FSM
//	                               的数据不在任何 raft 日志里，MarkPreRaft/
//	                               HasPreRaft，Join 的种子档位判据）
//	raft/bootgen                 → 机器世代（写入时机见 SaveBootGen 注释：
//	                               只在能启动成功的路径上写，拒启分支绝不写）
//	raft/local_recover_permit    → 一次性本地恢复许可（两行文本：授予时间 /
//	                               授予时机器世代；由 ForceLocalRecover 消费）
//	raft/<g>/hs                  → 【仅 legacy 回退用】HardState protobuf——
//	                               迁移前旧数据目录的读回退键；legacyPending
//	                               命中该键存在时 loadLegacy 才会读它，迁移
//	                               后的组 HardState 物理归宿是 seglog，
//	                               Persist 不再写这个键（bumpTermsInto 对
//	                               未迁移组仍会写它，见其注释）
//	raft/<g>/conf                → ConfState protobuf（成员表，ConfChange
//	                               apply 时整表覆盖写，SaveConfState）
//	raft/<g>/snap                → SnapshotMetadata protobuf（快照锚点，
//	                               截断的前提，SaveSnapMeta Sync 落盘）
//	raft/<g>/snapinstall         → SnapshotMetadata protobuf（快照安装中
//	                               标记，先于任何数据写入落盘；存在即上次
//	                               安装未收口，MarkInstalling/ResetGroupProgress）
//	raft/<g>/ent/<index 8B BE>   → 【仅 legacy 回退用】Entry protobuf——
//	                               迁移前旧数据目录的读回退键，同上；迁移
//	                               后的组 Entries 物理归宿是 seglog
//	raft/<g>/applied             → uint64 BE applied index（普通条目经
//	                               FSM 批次并进，ConfChange 与成员表同批）
package cluster

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/xushixin/sq/internal/cluster/seglog"
	"github.com/xushixin/sq/internal/store"
)

// raft 前缀常量。全部 raft 日志键在同一个 store 库内，与 FSM 数据
// （msg/、cursor/、meta/ 等）同库隔离；组号用十进制字符串编码，
// 后跟 '/' 定界——任意两位组号的键序严格分离（"1/" < "10/…"），
// 前缀扫描不会跨组串扰。
const (
	// raftPrefix / raftPrefixEnd 圈出全部 raft 元数据键的半开区间
	// ['raft/', 'raft0')——'/'=0x2f 的后继是 '0'=0x30，故 "raft0" 是
	// "raft/" 前缀的右开边界。用于把 raft 元数据与 FSM 数据分开扫描
	// （见 storeHasFSMKeys）。
	raftPrefix    = "raft/"
	raftPrefixEnd = "raft0"

	groupsKey           = "raft/groups"
	cleanShutdownKey    = "raft/clean_shutdown"
	preRaftKey          = "raft/preraft"
	bootGenKey          = "raft/bootgen"
	recoverPermitKey    = "raft/local_recover_permit"
	groupEntPrefixFmt   = "raft/%d/ent/"
	groupHsKeyFmt       = "raft/%d/hs"
	groupConfFmt        = "raft/%d/conf"
	groupSnapFmt        = "raft/%d/snap"
	groupSnapInstallFmt = "raft/%d/snapinstall"
	groupAppliedFmt     = "raft/%d/applied"
)

// raftStore 是 raft 日志的共库持久层。
//
// raft 日志（HardState + Entries）本身已迁出 Pebble，改由 seglog 分段
// 追加日志承载（本次迁移，Task 5）；成员表/快照锚点/组数/干净关机标记/
// 机器世代/本地恢复许可等元数据仍在 Pebble（st）里，与旧实现一致。
type raftStore struct {
	st *store.Store
	lg *slog.Logger

	// seglog 组日志：惰性打开（首个 Persist/Load 触发）。logsMu 只保护
	// 两张 map 本身的读写，Log 与 recovered 的元素一旦写入，各自的并发
	// 安全性由 seglog.Log（内部自带锁）与 Persist 的更新规则保证。
	logsMu    sync.Mutex
	logs      map[uint32]*seglog.Log
	recovered map[uint32]*logRecovered
}

// logRecovered 是某一组当前应被 Load 读到的 (HardState, Entries) 状态。
//
// 初值来自 seglog.Open 扫描恢复出的快照（open 时刻），此后每次同一组的
// Persist 都会原地更新它——不这样做的话，Load 在同一进程内会一直读到
// open 时刻的陈旧状态，看不见期间发生的 Persist（尤其是换届覆盖，见
// Persist 的注释）。
//
// 别名约定（finding 5）：hs/ents 保留的是调用方（raft 库 rd.HardState/
// rd.Entries）交下来的指针，Load 对外返回的也是这份共享指针（未做深拷贝）；
// 调用方必须把它们当只读数据，不得原地修改——MemoryStorage 只拷贝指针，
// 不拷贝值。
//
// 切片本身（不是元素）则是写时复制的：Persist 做换届回退裁剪时，若真的
// 裁掉了尾巴，会另分配一条新切片再拼接，绝不在原底层数组上覆盖写。这样
// 一来，Load 之前返回给调用方的那个切片头即便还被持有，它指向的元素也不
// 会被后来的 Persist 改掉——见 Persist 里的裁剪代码。
type logRecovered struct {
	hs   *raftpb.HardState
	ents []*raftpb.Entry
}

// newRaftStore 构造 raft 日志持久层。lg 绑定 mod=raftstore 键。
func newRaftStore(st *store.Store, lg *slog.Logger) *raftStore {
	return &raftStore{
		st:        st,
		lg:        lg.With("mod", "raftstore"),
		logs:      make(map[uint32]*seglog.Log),
		recovered: make(map[uint32]*logRecovered),
	}
}

// groupLogDir 返回一组 seglog 段目录的绝对路径：<storeDir>/raftlog/<g>。
//
// 唯一的路径推导点：getLog（打开）、wipeLog（删除）与 manager.go 的
// groupHasSegFiles（探测）三处必须指向同一个目录，各写一遍
// filepath.Join 迟早会有一处漏改，而这类不一致的表现是「探测说没有、
// 打开却读到了」——最难查的一类不一致。
func groupLogDir(storeDir string, g uint32) string {
	return filepath.Join(storeDir, "raftlog", strconv.FormatUint(uint64(g), 10))
}

// getLog 惰性打开一组的段日志：首次访问时 seglog.Open 扫描恢复，句柄与
// 恢复态入缓存；此后直接复用缓存句柄（Open 的扫描成本只承担一次）。
//
// 注意：seglog.Open 会 MkdirAll + O_CREATE，因此本方法**有副作用**——
// 对一个从未写过日志的组调用它，会在盘上留下空目录与 0 字节段文件。
// 只读路径（InspectRecovery）不得无条件调用，见 inspectGroupState。
func (r *raftStore) getLog(g uint32) (*seglog.Log, error) {
	r.logsMu.Lock()
	defer r.logsMu.Unlock()
	if l, ok := r.logs[g]; ok {
		return l, nil
	}
	dir := groupLogDir(r.st.Dir(), g)
	l, hs, ents, err := seglog.Open(dir, r.lg)
	if err != nil {
		return nil, fmt.Errorf("raftstore 组 %d 打开段日志 %s: %w", g, dir, err)
	}
	r.logs[g] = l
	r.recovered[g] = &logRecovered{hs: hs, ents: ents}
	return l, nil
}

// wipeLog 物理清空一组的 seglog 目录：关闭已打开的句柄（若有）、整个
// 目录连同全部段文件一起删除、清掉内存缓存。之后任何 getLog(g) 都会
// 当作全新空日志重新 Open（MkdirAll 重建空目录）。
//
// 调用方有两处：ResetGroupProgress（组进度整体清零，见其注释）与
// migrateLog（一次性迁移的幂等锚点：先清空再重写，任何后续步骤崩溃
// 重启都从这里重新开始，见 migrateLog 注释）。os.RemoveAll 对不存在的
// 目录返回 nil，因此本方法对「从未在本进程打开过、磁盘上也从未写过」
// 的组同样安全（no-op）。
func (r *raftStore) wipeLog(g uint32) error {
	r.logsMu.Lock()
	l, hadLog := r.logs[g]
	delete(r.logs, g)
	delete(r.recovered, g)
	r.logsMu.Unlock()

	if hadLog {
		if err := l.Close(); err != nil {
			return fmt.Errorf("raftstore wipeLog 组 %d 关闭段日志: %w", g, err)
		}
	}
	dir := groupLogDir(r.st.Dir(), g)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("raftstore wipeLog 组 %d 删除目录 %s: %w", g, dir, err)
	}
	return nil
}

// Persist 持久化一轮 Ready 的 HardState 与 Entries，落盘目标是该组的
// seglog 段日志（不再是 Pebble）。
//
// 参数：
//   - g: 数据组号
//   - hs: 非空的 HardState（含 Term/Vote/Commit），可为 nil（本轮无状态变更）
//   - ents: 本轮要追加/覆盖的日志条目；非空时其末条 index 即新日志尾
//   - sync: true 时本次写入带 fsync（quorum-fsync 档 MustSync 轮）
//
// 注意（删尾语义已由「重放」替代，为什么）：
//   - 旧实现（Pebble）里回退覆盖需要「写新条目 + 删更高 index 旧条目」
//     同批原子完成，否则崩溃窗口会留下幽灵条目。seglog 是纯追加日志，
//     物理上不做删除：新条目原样追加在旧条目之后，「谁是真正的日志尾」
//     由重放规则决定——重启扫描（seglog.Open）与本进程内的 Load 都遵循
//     「后写的赢」：新写入的条目 index 一旦 <= 已知的日志尾，就把
//     [该 index, 旧尾] 区间的旧条目从可见结果里截掉。也就是说，删尾这件
//     事没有消失，只是从「物理删除」变成了「读时重放裁剪」，语义对调用方
//     完全透明（Load 的返回值与旧实现等价）。
//   - Append 本身失败即返回错误（fail-stop，见 seglog.Log.Append 注释），
//     本层不重试。
// Sync 把组 g 的活动段刷到盘。
//
// 参数：
//   - g: 组号
//
// 返回：段日志打开失败或 fsync 失败时的错误
//
// 用途是 append 阶段的合批落盘：批内每条 MsgStorageAppend 各自
// `Persist(sync=false)` 只写不刷，最后由本方法统一刷一次。这与
// 「最后一条 Persist(sync=true)」在盘上完全等价——`seglog.Log.Append`
// 的 sync 分支做的就是 `Sync` 做的这件事（`syncActive`），而中途若发生
// 段轮转，`maybeRotate` 自己会在关旧段前把旧段 fsync 掉，不依赖这里。
func (r *raftStore) Sync(g uint32) error {
	l, err := r.getLog(g)
	if err != nil {
		return fmt.Errorf("raftstore Sync 组 %d 打开段日志: %w", g, err)
	}
	if err := l.Sync(); err != nil {
		return fmt.Errorf("raftstore Sync 组 %d: %w", g, err)
	}
	return nil
}

func (r *raftStore) Persist(g uint32, hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error {
	l, err := r.getLog(g)
	if err != nil {
		return fmt.Errorf("raftstore Persist 组 %d 打开段日志: %w", g, err)
	}
	if err := l.Append(hs, ents, sync); err != nil {
		return fmt.Errorf("raftstore Persist 组 %d: %w", g, err)
	}

	// 同步刷新 recovered 缓存：它是 Load 的数据源，初值只是 seglog.Open
	// 那一刻的快照，若不在这里跟着更新，同一进程内「Persist 后紧接着
	// Load」（如换届覆盖场景）会读到过期状态——这正是 TestPersistTruncates
	// ConflictTail 验证的行为，必须在内存缓存里也成立，不能只指望重启
	// 后重新 Open 才生效。
	r.logsMu.Lock()
	rec := r.recovered[g]
	if rec == nil {
		rec = &logRecovered{}
		r.recovered[g] = rec
	}
	if hs != nil {
		rec.hs = hs
	}
	if len(ents) > 0 {
		// 与 seglog.Open 重放同一条「后写的赢」规则：本批次首条 index
		// 就是新日志尾要覆盖的起点，缓存里 >= 该 index 的旧条目全部作废。
		// 批内 ents 本身升序、彼此不冲突（raft Ready 契约），因此只需按
		// 首条 index 裁一次，不必逐条比对。
		first := ents[0].GetIndex()
		cut := len(rec.ents)
		for cut > 0 && rec.ents[cut-1].GetIndex() >= first {
			cut--
		}
		if cut < len(rec.ents) {
			// 真发生了回退裁剪：必须换一条新底层数组，不能 append 回原地。
			// 原因是 Load 返回的只是切片头的浅拷贝，元素与这里共享——若在
			// 原数组上从 cut 位置覆盖写，一个已经拿到 [1..10] 的调用方会在
			// 毫无察觉的情况下看到自己手里的第 8..10 项变成新领导者的条目。
			// 代价只是换届时的一次分配（低频事件），换掉的是一个只在时序
			// 巧合下才会显形的隐患。无裁剪的常规追加走下面的 append 原路，
			// 热路径零额外开销。
			kept := make([]*raftpb.Entry, cut, cut+len(ents))
			copy(kept, rec.ents[:cut])
			rec.ents = kept
		}
		rec.ents = append(rec.ents, ents...)
	}
	r.logsMu.Unlock()
	return nil
}

// Load 读回一组的 HardState、现存日志条目与快照元数据（截断锚点）。
//
// 返回：
//   - hs: 从未持久化过时为空 HardState（raft.IsEmptyHardState 为真）
//   - ents: 按 index 升序的现存条目，可能为空。**不保证**截断过的组
//     一定不含 ≤ 锚点 index 的条目：本进程内 TruncateLog 之后的 Load
//     确实读不到它们（内存视图被主动裁到锚点之后），但重启后重新 Open
//     扫描是按段粒度回收的物理结果，同段内 index ≤ 锚点的条目会照样读
//     回来。多读回的旧条目是安全方向——MemoryStorage 按快照锚点自行
//     丢弃，详见 TruncateLog 的注释
//   - snapMeta: 组的快照元数据（截断锚点）；从未截断过时为 nil
//   - err: 读取或反序列化失败
//
// legacy 回退（为什么只读、绝不顺手迁移）：
//
//	迁移前的旧数据目录里，日志仍在 Pebble 的 hsKey/entPrefix 键族下；
//	legacyPending 命中即说明这组还没迁移，直接走 loadLegacy 原样读旧键。
//	读路径本身绝不做迁移写入——恢复判定命令（sq recover）与迁移步骤自身
//	都要求「读」是无副作用的：前者可能在决定是否允许启动之前就调用 Load，
//	若这里顺手写盘，一次只读的诊断操作就会悄悄改变磁盘状态；后者
//	（Manager 启动序里的 migrateRaftLogs）需要自己独占控制迁移的时机与
//	原子性，读路径抢先做了等于把迁移逻辑拆成两处，且时序不可控。
func (r *raftStore) Load(g uint32) (*raftpb.HardState, []*raftpb.Entry, *raftpb.SnapshotMetadata, error) {
	pending, err := r.legacyPending(g)
	if err != nil {
		return nil, nil, nil, err
	}
	if pending {
		return r.loadLegacy(g)
	}

	if _, err := r.getLog(g); err != nil { // 惰性 Open；恢复态随之入缓存
		return nil, nil, nil, fmt.Errorf("raftstore Load 组 %d 打开段日志: %w", g, err)
	}

	// hs 与 ents 必须在同一次持锁区间内一起读出：Persist/TruncateLog 都是
	// 在持锁期间原地更新 rec.hs/rec.ents 的（见二者的注释），若解锁后才
	// 分别取用两个字段，会撞上「先取到旧 hs、解锁后被并发 Persist 换成新
	// ents」这类撕裂读。rec 本身也可能是 nil——getLog 与这里重新加锁取值
	// 之间，CloseLogs 可能把 recovered 整体替换成一张新的空 map，此时按
	// 旧组号查到的就是 nil，必须当空日志防御性处理，不能直接解引用。
	// ents 只做切片头（指针/长度/容量）的浅拷贝：按 logRecovered 的别名
	// 约定，底层 Entry 元素仍与内部缓存共享指针；浅拷贝拿到的是这份切片
	// 头在锁内那一刻的快照，之后即便同一组发生新的 Persist/TruncateLog
	// 重新指向别处，也不会影响调用方已经拿到手的这个切片头。
	r.logsMu.Lock()
	rec := r.recovered[g]
	var hs *raftpb.HardState
	var ents []*raftpb.Entry
	if rec != nil {
		hs = rec.hs
		ents = rec.ents
	}
	r.logsMu.Unlock()
	if hs == nil {
		hs = &raftpb.HardState{} // 调用方契约：从未持久化过时返回空 HardState 而非 nil
	}

	// 截断锚点与条目一并读回：buildGroup 用它在日志被全量截断时
	// 恢复 MemoryStorage 的位点与成员表（见 buildGroup）。
	snapMeta, _, err := r.LoadSnapMeta(g)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raftstore Load 组 %d 读快照元数据: %w", g, err)
	}
	// 重启排障的第一行证据：组号、条目数、commit 位、锚点位。
	r.lg.Debug("raft 日志已读回（seglog）", "g", g, "entries", len(ents),
		"commit", hs.GetCommit(), "snap", snapMeta.GetIndex())
	return hs, ents, snapMeta, nil
}

// legacyPending 判定一组是否仍停留在迁移前的 Pebble 键族形态：
// hsKey(g) 存在，或 entPrefix(g) 扫描能找到至少一条条目键，任一为真即
// 未迁移完成。两个键族独立判定是因为二者可能不同时存在（如只写过投票
// 轮 HardState、从未追加过条目的组）。
func (r *raftStore) legacyPending(g uint32) (bool, error) {
	_, ok, err := r.st.Get(hsKey(g))
	if err != nil {
		return false, fmt.Errorf("raftstore legacyPending 组 %d 读 HardState: %w", g, err)
	}
	if ok {
		return true, nil
	}
	found := false
	err = r.st.Scan(entPrefix(g), store.PrefixUpperBound(entPrefix(g)), 1,
		func(_, _ []byte) (bool, error) {
			found = true
			return false, nil // 命中一条即可判定，不必扫完整个前缀
		})
	if err != nil {
		return false, fmt.Errorf("raftstore legacyPending 组 %d 扫描条目: %w", g, err)
	}
	return found, nil
}

// loadLegacy 是迁移前的 Load 原实现（改名保留，行为不变）：直接读
// Pebble 的 hsKey/entPrefix 键族。只读，不做任何迁移写入（见 Load 的
// legacy 回退注释）。
func (r *raftStore) loadLegacy(g uint32) (*raftpb.HardState, []*raftpb.Entry, *raftpb.SnapshotMetadata, error) {
	hs := &raftpb.HardState{}
	hsData, ok, err := r.st.Get(hsKey(g))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raftstore loadLegacy 组 %d 读 HardState: %w", g, err)
	}
	if ok {
		hs.Reset()
		if err := proto.Unmarshal(hsData, hs); err != nil {
			return nil, nil, nil, fmt.Errorf("raftstore loadLegacy 组 %d 解码 HardState: %w", g, err)
		}
	}

	var ents []*raftpb.Entry
	err = r.st.Scan(entPrefix(g), store.PrefixUpperBound(entPrefix(g)), 0,
		func(_, v []byte) (bool, error) {
			ent := &raftpb.Entry{}
			ent.Reset()
			// proto.Unmarshal 拷贝值内容，Scan 回调的 v 复用缓冲区，
			// 条目对象可安全长期持有。
			if err := proto.Unmarshal(v, ent); err != nil {
				return false, fmt.Errorf("raftstore loadLegacy 组 %d 解码条目: %w", g, err)
			}
			ents = append(ents, ent)
			return true, nil
		})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raftstore loadLegacy 组 %d 扫描条目: %w", g, err)
	}

	snapMeta, _, err := r.LoadSnapMeta(g)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raftstore loadLegacy 组 %d 读快照元数据: %w", g, err)
	}
	r.lg.Debug("raft 日志已读回（legacy）", "g", g, "entries", len(ents), "commit", hs.GetCommit(),
		"snap", snapMeta.GetIndex())
	return hs, ents, snapMeta, nil
}

// migrateLog 把一组的 raft 日志（HardState + Entries）从迁移前的 Pebble
// 键族形态一次性搬进 seglog（Task 7 接线，spec §6）。调用点见 NewManager：
// 必须在恢复判定（decideRecovery，只读 legacy 键）与可能抬 term 的分支
// （ForceLocalRecover/BumpTermsForLocalResume，见 bumpTermsInto 对未迁移
// 组写回 legacy hsKey 的注释）之后、buildGroup 装配循环之前逐组调用——
// 判定先于迁移，抬 term 也先于迁移，这样迁移搬走的才是「含 bump」的
// 最终状态，不会被后续的 legacy 写坏迁移判定，也不会被判定读到迁移后的
// 假状态。
//
// 语义（①是幂等锚点，任何一步之后崩溃，重启重迁都安全）：
//  1. legacyPending(g) 为假：组早已迁移过（或天生就在 seglog，从未见过
//     legacy 键），no-op，只打 Debug。
//  2. 为真：os.RemoveAll 整个 seglog 目录、清掉内存缓存（复用 wipeLog，
//     "清半截"）——保证接下来写入的是一份干净的新 seglog，不会跟上次
//     半途而废的迁移残留混在一起。
//  3. loadLegacy(g) 只读旧值（不写，遵守 Load 的 legacy 回退只读契约），
//     经 r.Persist(g, hs, ents, true) 写进刚清空的新 seglog——Persist 内部
//     调用 Append，HS 先于条目、fsync（seglog.Log.Append 保证的帧序），
//     并且会同步更新 r.recovered 缓存，migrateLog 返回后同进程内的
//     Load(g) 立刻能读到迁移后的内容，不会因为缓存陈旧看到假的空日志。
//  4. Pebble 单批 Sync 删 hsKey(g) + DeleteRange(entPrefix(g))——这一步
//     落盘之后 legacyPending(g) 才会翻假，是「迁移生效」的唯一判据。
//
// 崩溃安全：③④之间任一步崩溃，Pebble 旧键仍在（④还没提交），下次启动
// legacyPending 仍判真，从头①重来一遍——②的 RemoveAll 会把上次写了一半
// 的 seglog 目录整个清掉，不会出现「迁移了一半的 seglog + 还没删的旧键」
// 这种需要合并的复杂半态。
// migrateChunkEntries 迁移分块大小（条目数）。只约束迁移路径的内存峰值，
// 与段轮转（SegMaxBytes，按字节）无关；1024 条按最大消息体 4MiB 估算
// 上限约 4GiB，实际消息体远小于上限，典型峰值在几十 MiB 量级。
const migrateChunkEntries = 1024

func (r *raftStore) migrateLog(g uint32) error {
	pending, err := r.legacyPending(g)
	if err != nil {
		return fmt.Errorf("raftstore migrateLog 组 %d 判定迁移状态: %w", g, err)
	}
	if !pending {
		r.lg.Debug("组无需迁移（已在 seglog，或从未见过 legacy 键）", "g", g)
		return nil
	}
	start := time.Now()

	// ① 清半截：整个目录 + 内存缓存一起清空（幂等锚点，见函数注释）。
	if err := r.wipeLog(g); err != nil {
		return fmt.Errorf("raftstore migrateLog 组 %d 清理旧 seglog: %w", g, err)
	}

	// ② 只读旧值。
	hs, ents, _, err := r.loadLegacy(g)
	if err != nil {
		return fmt.Errorf("raftstore migrateLog 组 %d 读旧日志: %w", g, err)
	}

	// ③ 分块写入刚清空的新 seglog（Persist 顺带保证 recovered 缓存是最新
	// 的，见函数注释）。为什么分块：seglog.Append 会把一次调用的全部条目
	// 组装进单个内存缓冲，迁移是唯一可能一次性拿到全量日志的调用方——
	// 整块写入会让该缓冲膨胀到整份日志的大小（最坏 = 截断保留窗口内的
	// 全部消息体）。按块追加把峰值压到块大小；HS 只随首块写一次（帧序
	// 保证 HS 先于全部条目），只在最后一块 fsync——中途崩溃无妨，④未
	// 提交则 legacyPending 仍真，重启从①重迁。
	for i := 0; ; i += migrateChunkEntries {
		end := min(i+migrateChunkEntries, len(ents))
		var chunkHS *raftpb.HardState
		if i == 0 {
			chunkHS = hs
		}
		lastChunk := end == len(ents)
		if err := r.Persist(g, chunkHS, ents[i:end], lastChunk); err != nil {
			return fmt.Errorf("raftstore migrateLog 组 %d 写入 seglog（条目 %d..%d）: %w", g, i, end, err)
		}
		if lastChunk {
			break
		}
	}

	// ④ 删 legacy 键族，单批 Sync——落盘后 legacyPending 才翻假。
	b := r.st.NewBatch()
	if err := b.Delete(hsKey(g)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore migrateLog 组 %d 删 legacy HardState 键: %w", g, err)
	}
	if err := b.DeleteRange(entKey(g, 0), store.PrefixUpperBound(entPrefix(g))); err != nil {
		b.Close()
		return fmt.Errorf("raftstore migrateLog 组 %d 删 legacy 条目键: %w", g, err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore migrateLog 组 %d 提交删除批次: %w", g, err)
	}

	r.lg.Info("legacy raft 日志已迁移到 seglog", "g", g, "entries", len(ents), "elapsed", time.Since(start))
	return nil
}

// Applied 读取一组的已应用位点；从未写入过时返回 0。
func (r *raftStore) Applied(g uint32) (uint64, error) {
	data, ok, err := r.st.Get(appliedKey(g))
	if err != nil {
		return 0, fmt.Errorf("raftstore Applied 组 %d: %w", g, err)
	}
	if !ok {
		return 0, nil
	}
	return store.GetU64(data), nil
}

// SaveConfState 持久化一组的成员表，并与 applied 位点**同批**写入。
//
// 为什么同批：ConfChange 条目 apply 后若只更新内存 applied、不落盘，
// 重启会从旧位点重放该条 ConfChange——raft 的 ApplyConfChange 本身幂等，
// 但成员表来源一旦改成持久化值，重放就会用「旧成员表 + 重放变更」
// 二次叠加，与 leader 的实际成员表分叉（batch③ 遗留缺口）。
//
// 参数：
//   - g: 数据组号
//   - cs: rn.ApplyConfChange 的返回值（raft 库算出的权威成员表）
//   - applied: 本条 ConfChange 条目的 index
func (r *raftStore) SaveConfState(g uint32, cs *raftpb.ConfState, applied uint64) error {
	b := r.st.NewBatch()
	if err := writeConfState(b, g, cs, applied); err != nil {
		b.Close()
		return err
	}
	// 成员表是选举安全的根：Sync 落盘，不进 quorum-mem 的异步刷盘队列
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveConfState 组 %d: %w", g, err)
	}
	r.lg.Info("成员表已持久化", "g", g, "voters", cs.GetVoters(), "learners", cs.GetLearners(), "applied", applied)
	return nil
}

// writeConfState 把成员表与 applied 位点写入调用方给定的批次。
//
// 两个调用方共用同一份核心：
//   - SaveConfState：自建批次 + Sync 提交（成员变更 apply 路径）
//   - installSnapshot 第 5 步收口批次：applied、成员表、锚点、删安装
//     标记四者必须同批原子落盘——「安装完成」的定义就是这一批的原子
//     性，拆开提交的崩溃窗口里会出现「数据已装完、标记残留」的重启
//     重装（见 snapinstall 的收口注释）
//
// 批内写成员表与 applied 两个键，失败时批次由调用方处理（Close）。
func writeConfState(b *store.Batch, g uint32, cs *raftpb.ConfState, applied uint64) error {
	data, err := proto.Marshal(cs)
	if err != nil {
		return fmt.Errorf("raftstore 组 %d 编码成员表: %w", g, err)
	}
	if err := b.Set(confKey(g), data); err != nil {
		return fmt.Errorf("raftstore 组 %d 写成员表: %w", g, err)
	}
	if err := b.Set(appliedKey(g), store.PutU64(applied)); err != nil {
		return fmt.Errorf("raftstore 组 %d 写 applied: %w", g, err)
	}
	return nil
}

// LoadConfState 读回一组的成员表；从未写入过时 ok=false（调用方回退到
// confStateFromEntries 的日志重放路径，见 buildGroup）。
func (r *raftStore) LoadConfState(g uint32) (*raftpb.ConfState, bool, error) {
	data, ok, err := r.st.Get(confKey(g))
	if err != nil {
		return nil, false, fmt.Errorf("raftstore LoadConfState 组 %d: %w", g, err)
	}
	if !ok {
		return nil, false, nil
	}
	cs := &raftpb.ConfState{}
	if err := proto.Unmarshal(data, cs); err != nil {
		return nil, false, fmt.Errorf("raftstore LoadConfState 组 %d 解码: %w", g, err)
	}
	return cs, true, nil
}

// SaveSnapMeta 持久化一组的快照元数据（截断锚点）。
//
// 元数据是截断的前提而非结果：raft 重启时用 FirstIndex-1 的 term 做
// 任期比较，条目一旦删掉，那个 term 只能从这里查。因此顺序恒为
// 「先 SaveSnapMeta（Sync）、后 TruncateLog」——反过来会在两次写之间
// 留下「条目已删、锚点未落」的崩溃窗口，重启直接拒启。
func (r *raftStore) SaveSnapMeta(g uint32, meta *raftpb.SnapshotMetadata) error {
	b := r.st.NewBatch()
	if err := writeSnapMeta(b, g, meta); err != nil {
		b.Close()
		return err
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveSnapMeta 组 %d: %w", g, err)
	}
	r.lg.Info("快照锚点已落盘", "g", g, "index", meta.GetIndex(), "term", meta.GetTerm())
	return nil
}

// writeSnapMeta 把快照锚点写入调用方给定的批次。
//
// 两个调用方共用同一份核心：
//   - SaveSnapMeta：自建批次 + Sync 提交（截断前置，Task 3）
//   - installSnapshot 第 5 步收口批次：接收方安装的快照锚点必须与
//     applied、成员表、删安装标记同批原子落盘——锚点单独提交会让
//     「已装完、标记残留」的崩溃窗口被重启误判为完整状态
//
// 失败时批次由调用方处理（Close）。
func writeSnapMeta(b *store.Batch, g uint32, meta *raftpb.SnapshotMetadata) error {
	data, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("raftstore 组 %d 编码快照元数据: %w", g, err)
	}
	if err := b.Set(snapKey(g), data); err != nil {
		return fmt.Errorf("raftstore 组 %d 写快照元数据: %w", g, err)
	}
	return nil
}

// LoadSnapMeta 读回快照元数据；从未截断过时 ok=false。
func (r *raftStore) LoadSnapMeta(g uint32) (*raftpb.SnapshotMetadata, bool, error) {
	data, ok, err := r.st.Get(snapKey(g))
	if err != nil {
		return nil, false, fmt.Errorf("raftstore LoadSnapMeta 组 %d: %w", g, err)
	}
	if !ok {
		return nil, false, nil
	}
	meta := &raftpb.SnapshotMetadata{}
	if err := proto.Unmarshal(data, meta); err != nil {
		return nil, false, fmt.Errorf("raftstore LoadSnapMeta 组 %d 解码: %w", g, err)
	}
	return meta, true, nil
}

// MarkInstalling 写入一组的快照安装中标记并 Sync 落盘。
//
// 顺序不变量：标记必须先于任何快照数据写入（installSnapshot 第 2 步
// 早于第 3 步清空）——反过来（先清空后标记）的崩溃窗口里，磁盘上是
// 「已清空、无标记」= 静默空状态，重启会把它当完整状态启动，客户端
// 读到的消息永久缺失。先标记后清空则崩溃只留下「有标记」的半截状态，
// 重启经 buildGroup 的标记检查清空重来。
//
// 标记值是本次要安装的快照元数据（{Index, Term, ConfState}）——
// 重启的 Warn 日志用 Index 说明「装到一半的位点」。
func (r *raftStore) MarkInstalling(g uint32, meta *raftpb.SnapshotMetadata) error {
	data, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("raftstore MarkInstalling 组 %d 编码: %w", g, err)
	}
	b := r.st.NewBatch()
	if err := b.Set(snapInstallKey(g), data); err != nil {
		b.Close()
		return fmt.Errorf("raftstore MarkInstalling 组 %d 写标记: %w", g, err)
	}
	// 标记是崩溃判定的唯一依据：Sync 落盘，任何崩溃窗口都见得到它
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore MarkInstalling 组 %d: %w", g, err)
	}
	r.lg.Info("快照安装标记已落盘", "g", g, "index", meta.GetIndex(), "term", meta.GetTerm())
	return nil
}

// LoadInstalling 读回一组的快照安装中标记；无标记时 ok=false。
//
// 标记损坏必须报错而不是返回「无标记」——静默当作未安装 = 把半截状态
// 当完整状态启动（见 MarkInstalling 的顺序不变量）。
func (r *raftStore) LoadInstalling(g uint32) (*raftpb.SnapshotMetadata, bool, error) {
	data, ok, err := r.st.Get(snapInstallKey(g))
	if err != nil {
		return nil, false, fmt.Errorf("raftstore LoadInstalling 组 %d: %w", g, err)
	}
	if !ok {
		return nil, false, nil
	}
	meta := &raftpb.SnapshotMetadata{}
	if err := proto.Unmarshal(data, meta); err != nil {
		return nil, false, fmt.Errorf("raftstore LoadInstalling 组 %d 解码: %w", g, err)
	}
	return meta, true, nil
}

// ResetGroupProgress 把一组的进度整体重置：applied=0 + 删快照锚点 +
// 删安装中标记（Pebble 单批 Sync 落盘）+ 物理清空该组 seglog 目录
// （日志与 HardState 一并归零）。
//
// 契约（C1 修复后）：本组状态半截，全部重来——日志与 HardState 清空后，
// 重启的节点以空日志启动（firstIndex=1），leader 的探测只能全量重放
// （完整）或发快照（完整），两条路径都完整。只清 applied 不动日志会把
// 半截日志（锚点 A 之后的部分）当完整日志启动：raft 从 A+1 重放进被
// 清空的 FSM，状态 1..A 永久丢失且 raft 不知情，leader 换人后探测锚
// 在 A+1..P 上撞车、永不发快照——静默丢段。
//
// 何时用：buildGroup 启动时发现安装中标记 → wipeGroupKeys 清掉该组
// FSM 键后调用——半截状态必须整体归零（applied=0 让 raft 从 1 重投递、
// 锚点删除让 TruncateLog 的守卫重新放行、标记删除让「安装已完成」的
// 判定不再成立、日志与 HardState 清空让日志起点回到 1），残留任何一个
// 都会让重启路径把半截状态当完整状态。
//
// 为什么日志清空不再是同一个 Pebble 批次的一部分（seglog 迁移后的
// 顺序取舍，务必知情）：
//
//	HardState/Entries 的物理归宿已经是 seglog（每组一份独立目录），
//	与 applied/锚点/安装标记分属两套存储引擎，物理上做不到跨引擎单
//	事务。本方法退化为两步：① Pebble 侧（applied/锚点/标记，含遗留的
//	legacy hsKey/entKey 键族一并清掉，对非 legacy 组是无操作）先 Sync
//	落盘；② 成功后再调用 wipeLog(g) 物理删除该组的 seglog 目录。
//	顺序刻意如此（先 Pebble 后 seglog，不能反过来）：若步骤①先成功、
//	②因崩溃未执行，重启看到的是「进度已清零（applied=0、无锚点、无
//	标记）+ seglog 里还残留旧日志」——这组旧日志会被当作合法历史重放，
//	是「多余但无害」的方向（该组本来就是要整组重建，多重放几条旧条目
//	不会引入新的不一致，后续快照安装会覆盖）。反过来若①未落、②先执行
//	（seglog 已清空但 Pebble 进度仍是旧值，如 applied=42、锚点仍在），
//	重启会读到「空日志 + 旧 applied/锚点」，applied 越过 committed=0
//	的组合正是本次迁移过程中在别处实测触发过的 raft 库拒启 Panic
//	（appliedTo 校验），后果严重得多。因此本方法固定「先 Pebble 后
//	seglog」的顺序，把可能的崩溃窗口限定在更安全的一侧。
//
// 注意：成员表键（raft/<g>/conf）刻意不删——重启后经全量重放或快照
// 自然重建（ConfChange 条目 apply 时整表覆盖写），删除反而让重启节点
// 在收到第一个条目前没有成员表可用。
func (r *raftStore) ResetGroupProgress(g uint32) error {
	b := r.st.NewBatch()
	if err := b.Set(appliedKey(g), store.PutU64(0)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore ResetGroupProgress 组 %d 写 applied=0: %w", g, err)
	}
	if err := b.Delete(snapKey(g)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore ResetGroupProgress 组 %d 删快照锚点: %w", g, err)
	}
	if err := b.Delete(snapInstallKey(g)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore ResetGroupProgress 组 %d 删安装标记: %w", g, err)
	}
	// 遗留 legacy 键族一并清掉：对已迁移到 seglog 的组这是无操作（键本
	// 就不存在），对尚未迁移的组保证重置后 legacyPending 不再误判为
	// 「未迁移」（残留的旧键会让下次 Load 继续走 legacy 回退，读到本该
	// 已清空的旧状态）。
	if err := b.DeleteRange(entKey(g, 0), store.PrefixUpperBound(entPrefix(g))); err != nil {
		b.Close()
		return fmt.Errorf("raftstore ResetGroupProgress 组 %d 删遗留日志条目: %w", g, err)
	}
	if err := b.Delete(hsKey(g)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore ResetGroupProgress 组 %d 删遗留 HardState: %w", g, err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore ResetGroupProgress 组 %d: %w", g, err)
	}
	// Pebble 侧已经落盘成功之后才清 seglog——顺序理由见方法注释。
	if err := r.wipeLog(g); err != nil {
		return fmt.Errorf("raftstore ResetGroupProgress 组 %d 清空段日志: %w", g, err)
	}
	r.lg.Warn("组进度已整体重置（applied=0、锚点/标记/日志/HardState 已清空，重启后 firstIndex=1）", "g", g)
	return nil
}

// deleteInstallingKey 在调用方给定的批次内删除一组的安装中标记。
//
// 为什么做成 batch 方法而不是导出裸键：安装完成的定义是「applied、
// 成员表、锚点、删标记四者同批原子落盘」（installSnapshot 收口批次），
// 标记删除必须与其它三项同一批次——收口批次一旦分开提交，崩溃窗口里
// 就会出现「数据已装完、标记残留」的重启重装。方法签名收一个批次，
// 调用方只能把删除并入某个批次，拆不出单独提交。
func deleteInstallingKey(b *store.Batch, g uint32) error {
	if err := b.Delete(snapInstallKey(g)); err != nil {
		return fmt.Errorf("raftstore 删组 %d 安装标记: %w", g, err)
	}
	return nil
}

// TruncateLog 回收 index ≤ upto 的日志占用空间（seglog 按段整段删除，
// 见 seglog.Log.TruncateTo）。
//
// 前置：upto ≤ 已落盘 snap 元数据的 Index（锚点必须先落盘，见
// SaveSnapMeta）。违反即报错拒绝执行——这是「先锚点后截断」顺序的
// 编译期之外的运行期守卫，锚点守卫本身与迁移无关，原样保留。
//
// 幂等：重复截断到同一位点，TruncateTo 找不到可回收的段即为无操作。
//
// 注意（recovered 缓存必须跟着裁剪，为什么）：
//
//	seglog.TruncateTo 只按整段删除物理文件，不知道、也不该知道
//	raftStore 内存里还缓存着一份 rec.ents——若不在这里同步裁掉
//	index ≤ upto 的部分，同一进程内「TruncateLog 后紧接着 Load」
//	（如本方法的调用方 flusher/快照压缩后立即读一次）会继续读到已经
//	逻辑上不该再可见的旧条目，与「本进程已经声明截断到 upto」的意图
//	相悖。注意方向不要理解反了：这里是缓存视图主动收窄到锚点之后，
//	不代表盘面也精确同步到同一粒度——seglog 按整段回收，物理删除的
//	粒度比这里粗得多，重启后重新 Open 完全可能读回比这里裁剪后更多
//	的条目（该段里 index ≤ upto 的条目所在段还没被回收）。这是安全
//	方向：多出来的旧条目会被重放进 MemoryStorage，MemoryStorage 自己
//	会按快照锚点把 index ≤ upto 的部分丢弃，不会造成状态错误。
func (r *raftStore) TruncateLog(g uint32, upto uint64) error {
	meta, ok, err := r.LoadSnapMeta(g)
	if err != nil {
		return err
	}
	if !ok || meta.GetIndex() < upto {
		// 截断是「日志为什么变小了」的唯一解释，拒绝必须带锚点与请求位点
		r.lg.Error("截断点越过快照锚点，拒绝执行", "g", g, "upto", upto,
			"anchor_ok", ok, "anchor_index", meta.GetIndex())
		return fmt.Errorf("raftstore TruncateLog 组 %d: 截断点 %d 越过快照锚点（锚点存在=%v, index=%d）——必须先 SaveSnapMeta",
			g, upto, ok, meta.GetIndex())
	}
	l, err := r.getLog(g)
	if err != nil {
		return fmt.Errorf("raftstore TruncateLog 组 %d: %w", g, err)
	}
	if err := l.TruncateTo(upto); err != nil {
		return fmt.Errorf("raftstore TruncateLog 组 %d: %w", g, err)
	}

	r.logsMu.Lock()
	if rec := r.recovered[g]; rec != nil {
		cut := 0
		for cut < len(rec.ents) && rec.ents[cut].GetIndex() <= upto {
			cut++
		}
		rec.ents = rec.ents[cut:]
	}
	r.logsMu.Unlock()

	r.lg.Info("raft 日志已截断（按段回收）", "g", g, "upto", upto)
	return nil
}

// SyncLogs 逐个已打开组日志刷盘——mem 档 200ms flusher 的日志侧入口。
//
// 顺序契约（spec §5）：调用方必须先 SyncLogs 再 store.SyncWAL，保证
// 崩溃窗口只出现「日志超前 FSM」（重放即补齐，安全）的方向，反过来
// 「FSM 超前日志」会让已应用的状态在重启后凭空消失，不可接受。
func (r *raftStore) SyncLogs() error {
	r.logsMu.Lock()
	logs := make([]*seglog.Log, 0, len(r.logs))
	for _, l := range r.logs {
		logs = append(logs, l)
	}
	r.logsMu.Unlock()
	for _, l := range logs {
		if err := l.Sync(); err != nil {
			return fmt.Errorf("raftstore SyncLogs: %w", err)
		}
	}
	return nil
}

// CloseLogs 关闭全部已打开的组日志句柄，并清空缓存（Manager 停机/重入
// 前调用；幂等——清空后的 map 上再调用等价于没打开过任何组）。
//
// 单组关闭失败只记 Error、不中断也不返回错误：停机路径不能因为某一个
// 组的文件句柄关闭失败就卡住整体退出；清空 map 让后续若有代码再次
// getLog 同一组，会走一次全新的 Open 重新扫描，不会复用已失效的句柄。
func (r *raftStore) CloseLogs() {
	r.logsMu.Lock()
	logs := r.logs
	r.logs = make(map[uint32]*seglog.Log)
	r.recovered = make(map[uint32]*logRecovered)
	r.logsMu.Unlock()
	failed := 0
	for g, l := range logs {
		if err := l.Close(); err != nil {
			failed++
			r.lg.Error("raftstore CloseLogs 组关闭失败", "g", g, "err", err)
		}
	}
	// 停机路径的退出证据：一次性调用（非热路径），打 Info 不会刷屏；
	// 数量对上即可判断这次停机是否干净——failed>0 时上面已有逐组 Error，
	// 这里只汇总计数，不重复错误详情。
	r.lg.Info("raftstore CloseLogs 完成", "opened", len(logs), "closeFailed", failed)
}

// EnsureGroups 校验数据组数与磁盘记录一致。
//
// 首启（raft/groups 不存在）时写入 4B BE 组数并 Sync 落盘——这是一次性
// 契约写入，后续启动不再允许变更：组数是队列归组映射的分母，
// 换组数会让存量数据错组，必须在此挡死。
func (r *raftStore) EnsureGroups(n uint32) error {
	data, ok, err := r.st.Get([]byte(groupsKey))
	if err != nil {
		return fmt.Errorf("raftstore EnsureGroups: %w", err)
	}
	if !ok {
		b := r.st.NewBatch()
		if err := b.Set([]byte(groupsKey), putU32BE(n)); err != nil {
			b.Close()
			return fmt.Errorf("raftstore EnsureGroups 写组数: %w", err)
		}
		if err := r.st.ApplyWith(b, true); err != nil {
			return fmt.Errorf("raftstore EnsureGroups: %w", err)
		}
		r.lg.Info("数据组数已持久化", "groups", n)
		return nil
	}
	disk := binary.BigEndian.Uint32(data)
	if disk != n {
		return fmt.Errorf("集群数据组数不可变更：磁盘 %d, 配置 %d——组数是队列归组映射的分母，变更会让存量数据错组", disk, n)
	}
	return nil
}

// MarkCleanShutdown 写入干净关机标记并 Sync 落盘。
// 调用方保证它是节点退出前的最后一次同步写（StopClean 停完全部机件
// 后再调用）——标记在全部写入之后落盘，「有标记即数据齐全」才成立。
func (r *raftStore) MarkCleanShutdown() error {
	b := r.st.NewBatch()
	if err := b.Set([]byte(cleanShutdownKey), []byte{1}); err != nil {
		b.Close()
		return fmt.Errorf("raftstore MarkCleanShutdown: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore MarkCleanShutdown: %w", err)
	}
	return nil
}

// ConsumeCleanShutdown 读取并删除干净关机标记，返回标记是否存在。
//
// 删除带 Sync：本节点此后若再次崩溃（未走 StopClean），标记不会残留，
// 重启时能正确判定为不干净关机。标记存在与否就是重启两条路径的
// 唯一判定依据，调用方据此决定走原身份回归还是重入编排。
func (r *raftStore) ConsumeCleanShutdown() (bool, error) {
	_, ok, err := r.st.Get([]byte(cleanShutdownKey))
	if err != nil {
		return false, fmt.Errorf("raftstore ConsumeCleanShutdown: %w", err)
	}
	if !ok {
		r.lg.Info("干净关机标记不存在", "重启路径", "重入编排")
		return false, nil
	}
	b := r.st.NewBatch()
	if err := b.Delete([]byte(cleanShutdownKey)); err != nil {
		b.Close()
		return true, fmt.Errorf("raftstore ConsumeCleanShutdown 删标记: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return true, fmt.Errorf("raftstore ConsumeCleanShutdown: %w", err)
	}
	r.lg.Info("干净关机标记已消费", "重启路径", "原身份回归")
	return true, nil
}

// MarkPreRaft 写入「前 raft 期存量数据」标记并 Sync 落盘。
//
// 语义：本节点在**首次以集群模式启动前**，数据目录里就已有 FSM 数据
// （单机档直写 store 写进去的）。这批数据不在任何 raft 日志里——新节点
// 靠日志重放追齐时带不过去，只有快照能带走。标记是 Join 判定「种子档位
// 是否危险」的必要条件（见 probeSeedState）。
//
// 幂等：重复调用只是重写同一字节；调用点在 NewManager 的 fresh 路径，
// 每个数据目录一生只经过一次。
func (r *raftStore) MarkPreRaft() error {
	b := r.st.NewBatch()
	if err := b.Set([]byte(preRaftKey), []byte{1}); err != nil {
		b.Close()
		return fmt.Errorf("raftstore MarkPreRaft: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore MarkPreRaft: %w", err)
	}
	r.lg.Warn("检测到前 raft 期存量数据，已打标记——该批数据不在 raft 日志里，" +
		"本节点作为 Join 种子时须先等日志压缩（否则新节点走重放会丢这批数据）")
	return nil
}

// HasPreRaft 返回本节点是否带「前 raft 期存量数据」标记。
//
// 标记只增不删（存量数据永远不会回到日志里），因此它是**档位属性**而非
// 瞬时状态：Join 探测把它与「日志未压缩」合取，才构成拒绝条件。
func (r *raftStore) HasPreRaft() (bool, error) {
	_, ok, err := r.st.Get([]byte(preRaftKey))
	if err != nil {
		return false, fmt.Errorf("raftstore HasPreRaft: %w", err)
	}
	return ok, nil
}

// LoadBootGen 读取盘上记录的机器世代。
//
// 返回：
//   - 世代字符串、是否存在、错误
//   - 从未写入过时返回 ("", false, nil)——首次以本版本启动的旧数据目录
//     即此形态，恢复判定必须把它当作「世代不可比 = 保守处理」
func (r *raftStore) LoadBootGen() (string, bool, error) {
	data, ok, err := r.st.Get([]byte(bootGenKey))
	if err != nil {
		return "", false, fmt.Errorf("raftstore LoadBootGen: %w", err)
	}
	if !ok {
		return "", false, nil
	}
	return string(data), true, nil
}

// SaveBootGen 写入本次启动的机器世代并 Sync 落盘。
//
// 写入时机是安全关键，调用方必须遵守：**只在决定走一条能启动成功的
// 路径之后才调用**（clean-resume / fresh / local-resume / local-forced），
// ErrUncleanShutdown 分支绝不调用。
//
// 理由：本键的语义是「本数据目录最后一次被运行中的节点写入，发生在哪个
// 机器世代」。若拒启分支也写，序列就变成——机器重启 → 判定需要人工签字
// → 顺手写了新世代 → 重入编排失败拒启 → 运维直接重启进程 → 此时盘上
// 世代已等于当前世代 → 自动判定「机器没重启过、本地日志可信」→ 签字门
// 被静默绕过。整扇安全门会因这一处顺序错误而形同虚设。
func (r *raftStore) SaveBootGen(gen string) error {
	b := r.st.NewBatch()
	if err := b.Set([]byte(bootGenKey), []byte(gen)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveBootGen: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveBootGen: %w", err)
	}
	r.lg.Info("机器世代已记录", "gen", gen)
	return nil
}

// recoverPermit 是一次性本地恢复许可的内容。
//
// Gen 绑定授予时的机器世代，这是许可的作废机制：机器每重启一次世代就变，
// 旧许可自动失效。用世代而非 TTL，是为了不依赖时钟——签字只对运维当时
// 看到的那一次事故有效。
type recoverPermit struct {
	GrantedAt string // 授予时间（RFC3339，只给人看）
	Gen       string // 授予时的机器世代（作废判据）
}

// SaveRecoverPermit 写入一次性本地恢复许可并 Sync 落盘。
//
// 值编码为两行 UTF-8 文本（第一行授予时间、第二行授予时世代）而非
// protobuf：运维可能要用普通工具直接查看它，可读性远比编码效率重要，
// 而这个值一生只被写一次、读一次。
func (r *raftStore) SaveRecoverPermit(p recoverPermit) error {
	b := r.st.NewBatch()
	val := p.GrantedAt + "\n" + p.Gen
	if err := b.Set([]byte(recoverPermitKey), []byte(val)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveRecoverPermit: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveRecoverPermit: %w", err)
	}
	r.lg.Error("已写入一次性本地恢复许可——下次启动将允许带着可能残缺的本地日志恢复，可能丢失已确认的消息",
		"grantedAt", p.GrantedAt, "gen", p.Gen)
	return nil
}

// LoadRecoverPermit 读取一次性本地恢复许可。
//
// 返回：许可内容、是否存在、错误。格式不合法时按「不存在」处理并记
// Error——一个读不懂的许可绝不能被当成有效签字。
func (r *raftStore) LoadRecoverPermit() (recoverPermit, bool, error) {
	data, ok, err := r.st.Get([]byte(recoverPermitKey))
	if err != nil {
		return recoverPermit{}, false, fmt.Errorf("raftstore LoadRecoverPermit: %w", err)
	}
	if !ok {
		return recoverPermit{}, false, nil
	}
	parts := strings.SplitN(string(data), "\n", 2)
	if len(parts) != 2 || parts[1] == "" {
		r.lg.Error("本地恢复许可格式不合法，按无许可处理", "raw", string(data))
		return recoverPermit{}, false, nil
	}
	return recoverPermit{GrantedAt: parts[0], Gen: parts[1]}, true, nil
}

// termBumpReason 区分抬任期的两种来路。它们的严重性差一个数量级，
// 日志级别必须跟着分开：常规重启是预期动作（代价只是多一轮选举），
// 签字放行则意味着运维已接受可能丢已确认消息。
type termBumpReason uint8

const (
	bumpLocalResume   termBumpReason = iota // mem 档常规重启（pathLocalResume）
	bumpForcedRecover                       // sq recover --grant 签字放行
)

// bumpTermsInto 把组 0..dataGroups 每组 HardState 的 Term 加 1、Vote 清空，
// 逐组经 Persist 落盘（sync=true）。
//
// commit 位点刻意不动：它由日志重放与 leader 重新告知恢复，抬它没有意义
// 且会与真实日志脱节。
//
// 为什么改走 Persist/Load 而不再是调用方给的单个 Pebble 批次（与迁移前
// 的关键差异，务必读完）：
//
//	HardState 的物理归宿已经从 Pebble 的 hsKey 迁到了 seglog（本次改造，
//	Task 5）。若这里继续裸读写 hsKey，会踩两个坑：①读到的根本不是当前
//	真实状态——Persist 早就不写 hsKey 了，裸读永远看到「未持久化」的空
//	HardState，term 从 0 起跳，凭空抹掉已经跑到很高的真实任期；②裸写
//	hsKey 会把这个组的 legacyPending 判定从 false 翻成 true（hsKey 存在
//	=旧键族未迁移的信号），后续 Load 因此整组滑进 legacy 只读回退，读到
//	commit=0、entries=空——这正是本次改造过程中在 TestUncleanSameBootResumes
//	Locally 上实际炸出来的 panic（raft 库校验 applied 越过 committed 直接
//	Panicf）。改走 r.Load(g) 读当前真实 HardState、r.Persist(g, ..., true)
//	写回，两条路径与业务读写用的是同一套 legacy/seglog 判定逻辑，不会
//	再产生这种「旁路写坏迁移判定位」的情况。
//
// 代价（原子性收窄，调用方必须知情）：原实现把「N 组抬任期」与「消费
// 许可/写入调用」放进同一个 Pebble 批次，跨组原子。现在 HardState 分散
// 在 N 份独立的 seglog 文件里，不存在跨文件的单事务，只能退化为「逐组
// 各自 sync 落盘」。这个退化在 raft 语义下依然安全：抬任期本身是幂等
// 安全操作（多抬一次只是多一轮选举，见方法原注释），因此哪怕在抬到一半
// 时崩溃，重启后只要许可（或世代判定）仍然成立、调用方重试一次
// bumpTermsInto，把已经抬过的组再抬一次也不会破坏正确性——真正不可
// 重复的操作（消费许可）被调用方安排在全部组都抬完之后才做，见
// ForceLocalRecover。
//
// 未迁移组必须写回 legacy hsKey，不能走 r.Persist（review finding 1，
// 务必读完）：上面那段「改走 Persist/Load」只解决了已迁移组（seglog 是
// 权威）的问题，但对 legacyPending(g)==true 的组（这组的盘还没跑过
// Task 7 的迁移步骤，HardState 权威仍在 Pebble 的 hsKey）会引入新坑——
// r.Load(g) 走 legacyPending 判定确实能读到正确的旧值，可如果紧接着用
// r.Persist(g, bumped, nil, true) 写回，Persist 无条件写 seglog，完全
// 不看 legacyPending，于是：①legacyPending 的判定依据（hsKey 是否存在）
// 完全没被这次写触碰，仍然是 true，②但这组真正的最新 HardState 已经
// 只存在于 seglog 里、Pebble 的 hsKey 还是抬之前的旧值。后续任何一次
// Load 都会因为 legacyPending==true 继续走 loadLegacy 读 Pebble，读到
// 的还是没被抬过的旧 term——这次 bump 白做了，term 不单调，且许可已经
// 被消费掉，「同任期不二次投票」这条不变量在这组上直接失效。更严重的
// 是：Task 7 的迁移步骤对未迁移组的处理方式是先 os.RemoveAll 掉 seglog
// 目录、再以 Pebble 的旧键族为准重新写一份，届时刚才悄悄写进 seglog 的
// 这份 bump 会被连根拔起。根子在于：迁移前的盘必须整体停留在迁移前
// 形态，半新半旧（HardState 在 seglog、legacyPending 却仍判定为
// legacy）不是一个迁移步骤会承认的中间态。因此这里按 legacyPending(g)
// 显式分流：为 true 时走 pre-Task-5 的写法（marshal + Set(hsKey(g)) +
// Sync 批次提交，与 loadLegacy 的读路径配对，让整组继续待在「未迁移」
// 这一种形态里）；为 false 时保持现状，走 r.Persist（seglog 是权威）。
func (r *raftStore) bumpTermsInto(dataGroups uint32, reason termBumpReason) error {
	for g := uint32(0); g <= dataGroups; g++ {
		hs, _, _, err := r.Load(g)
		if err != nil {
			return fmt.Errorf("组 %d 读 HardState: %w", g, err)
		}
		newTerm := hs.GetTerm() + 1
		var noVote uint64
		commit := hs.GetCommit()
		bumped := &raftpb.HardState{Term: &newTerm, Vote: &noVote, Commit: &commit}

		pending, err := r.legacyPending(g)
		if err != nil {
			return fmt.Errorf("组 %d 判定迁移状态: %w", g, err)
		}
		if pending {
			// 未迁移组：写回 legacy hsKey，让这组整体停留在迁移前形态
			// （见上方 doc comment）。与 loadLegacy 的读路径配对——不走
			// r.Persist，避免污染到 seglog 却又读不到。
			data, err := proto.Marshal(bumped)
			if err != nil {
				return fmt.Errorf("组 %d 编码 HardState: %w", g, err)
			}
			b := r.st.NewBatch()
			if err := b.Set(hsKey(g), data); err != nil {
				b.Close()
				return fmt.Errorf("组 %d 写 legacy HardState: %w", g, err)
			}
			if err := r.st.ApplyWith(b, true); err != nil {
				return fmt.Errorf("组 %d 写 legacy HardState: %w", g, err)
			}
		} else {
			if err := r.Persist(g, bumped, nil, true); err != nil {
				return fmt.Errorf("组 %d 写 HardState: %w", g, err)
			}
		}
		// 同一个动作、两种严重性：级别与文案都按来路分开，让读日志的人
		// 一眼看出这是常规重启还是签字放行。
		if reason == bumpForcedRecover {
			r.lg.Error("签字放行的本地恢复：任期已抬、投票已清",
				"g", g, "term", newTerm, "legacy", pending)
		} else {
			r.lg.Warn("mem 档本地恢复：任期已抬、投票已清（投票记录走 NoSync 可能未落盘，"+
				"抬任期是预期动作，代价是多一轮选举）",
				"g", g, "term", newTerm, "legacy", pending)
		}
	}
	return nil
}

// ForceLocalRecover 执行签字放行的本地恢复前置：抬全部组的任期、清空
// 投票，并消费掉一次性许可。
//
// 参数：
//   - dataGroups: 数据组数；本方法处理组 0（meta 组）到 dataGroups
//
// 为什么必须抬 term：quorum-mem 档掉电可能丢掉投票记录——本节点在任期 T
// 投给过 A，重启后忘了，又在 T 投给 B，于是同一任期出现两个 leader、
// 日志分叉。这比丢数据严重，是损坏。抬任期在 raft 中永远安全（代价只是
// 强制一次重新选举），抬完之后本节点不可能再在 T 投第二次。
//
// 顺序契约（HardState 迁到 seglog 后已不能再与许可删除同批原子，见
// bumpTermsInto 的「代价」注释，此处是那份代价换来的安全前提）：必须
// 先把全部组的任期抬完，最后才删许可——若先删许可、抬任期一半时崩溃，
// 运维签的字就白费了（下次不干净关机会被判定为无许可、无法恢复）；
// 反过来，任期抬完才删，哪怕中间真的崩溃，许可仍在、下次可以带着同一
// 份许可重试整个流程（重复抬任期本身无害，见 bumpTermsInto）。
//
// 注意：commit 位点不动——它由日志重放与 leader 重新告知恢复，抬它没有
// 意义且会与真实日志脱节。抬 term 的适用范围见 needsTermBump。
func (r *raftStore) ForceLocalRecover(dataGroups uint32) error {
	if err := r.bumpTermsInto(dataGroups, bumpForcedRecover); err != nil {
		return fmt.Errorf("raftstore ForceLocalRecover: %w", err)
	}
	b := r.st.NewBatch()
	if err := b.Delete([]byte(recoverPermitKey)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore ForceLocalRecover 删许可: %w", err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore ForceLocalRecover: %w", err)
	}
	r.lg.Error("一次性本地恢复许可已消费（本次生效，不再对后续任何一次不干净关机放行）", "groups", dataGroups+1)
	return nil
}

// BumpTermsForLocalResume 为 local-resume 抬任期、清投票并逐组落盘。
//
// 与 ForceLocalRecover 的区别只有一个：本方法**不碰许可键**。local-resume
// 本来就不需要运维签字（它要么世代未变、要么是 fsync 档），只是 mem 档下
// 投票记录可能没落盘，所以同样要抬任期。见 needsTermBump 的注释；跨组
// 原子性的取舍见 bumpTermsInto。
func (r *raftStore) BumpTermsForLocalResume(dataGroups uint32) error {
	if err := r.bumpTermsInto(dataGroups, bumpLocalResume); err != nil {
		return fmt.Errorf("raftstore BumpTermsForLocalResume: %w", err)
	}
	return nil
}

// confStateFromEntries 从日志条目重放成员变更，按 commit 裁剪后合成
// 重启所需的 ConfState。
//
// **降级声明（batch④）**：本函数已从主路径降级为「旧数据目录一次性
// 迁移路径」——新版本启动时成员表一律优先读持久化值（LoadConfState，
// 见 buildGroup），只有 batch③ 及更早的数据目录从未写入过 raft/<g>/conf、
// 首次升级到本版本时才走到这里。升级后的首次 ConfChange apply 会立即
// 把成员表持久化，此路径此后不再被命中。
//
// raft 库 newRaft 只信任 Storage.InitialState() 返回的 ConfState，不会
// 自己回放 ConfChange 条目重建成员表；而本批不做快照，因此必须由调用方
// 重放。重放只到 commit 为止——commit 之外的未提交 ConfChange 尾巴
// 不得进成员表（终审观察项②），否则重启的节点会把从未被多数派接受的
// 成员变更当成既定事实。
//
// 条目按 index 升序传入（Load 的返回契约），commit 裁剪直接 break。
//
// 损坏条目直接报错拒启（终审 R4 修正）：旧实现跳过损坏条目，但一条
// 损坏的 RemoveNode 被跳过会让被移除的 voter 残留成员表——与注释宣称
// 的「安全」恰好相反，且静默无日志无测试。拒启让损坏显式化：operator
// 清空状态（WipeForRejoin）经存活 leader 以 learner 身份重入即可恢复，
// 比静默残留一个已移除的 voter 安全得多。
//
// 同时支持 V1 与 V2 两种 ConfChange 条目格式（V2 是 R4 起的提案格式，
// 旧日志可能遗留 V1）；任一格式的解码失败都拒启。V2 条目可能携带多条
// Changes（batch② 遗留的多变更守卫）：本进程从不产生联合共识条目，但
// 旧日志与将来的 joint 提案都可能带多条——只取首条会漏掉后续变更，
// 因此逐条遍历应用（applyOne）。
func confStateFromEntries(ents []*raftpb.Entry, commit uint64) (*raftpb.ConfState, error) {
	cs := &raftpb.ConfState{}
	for _, ent := range ents {
		if ent.GetIndex() > commit {
			break // 条目升序，越过 commit 即全部未提交
		}
		switch ent.GetType() {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Reset()
			if err := proto.Unmarshal(ent.Data, &cc); err != nil {
				return nil, fmt.Errorf("解码 ConfChange 条目 %d: %w", ent.GetIndex(), err)
			}
			applyOne(cs, cc.GetType(), cc.GetNodeId())
		case raftpb.EntryConfChangeV2:
			var v2 raftpb.ConfChangeV2
			v2.Reset()
			if err := proto.Unmarshal(ent.Data, &v2); err != nil {
				return nil, fmt.Errorf("解码 ConfChangeV2 条目 %d: %w", ent.GetIndex(), err)
			}
			ch := v2.GetChanges()
			if len(ch) == 0 {
				continue // leave-joint 类空条目：不改变单代成员表
			}
			for _, c := range ch {
				applyOne(cs, c.GetType(), c.GetNodeId())
			}
			continue
		default:
			continue // 普通条目与选举空条目不参与成员表
		}
	}
	return cs, nil
}

// applyOne 把单条成员变更应用到合成中的成员表（confStateFromEntries
// 的公共应用步骤，V1 单条与 V2 多条 Changes 共用）。
func applyOne(cs *raftpb.ConfState, typ raftpb.ConfChangeType, nodeID uint64) {
	switch typ {
	case raftpb.ConfChangeAddNode:
		removeUint64(&cs.Voters, nodeID)
		removeUint64(&cs.Learners, nodeID)
		cs.Voters = append(cs.Voters, nodeID)
	case raftpb.ConfChangeAddLearnerNode:
		removeUint64(&cs.Voters, nodeID)
		removeUint64(&cs.Learners, nodeID)
		cs.Learners = append(cs.Learners, nodeID)
	case raftpb.ConfChangeRemoveNode:
		removeUint64(&cs.Voters, nodeID)
		removeUint64(&cs.Learners, nodeID)
	}
}

// removeUint64 从切片中移除第一个等于 v 的元素（不存在时静默）。
func removeUint64(s *[]uint64, v uint64) {
	for i, x := range *s {
		if x == v {
			*s = append((*s)[:i], (*s)[i+1:]...)
			return
		}
	}
}

// entPrefix 返回一组条目键的扫描下界：raft/<g>/ent/。
func entPrefix(g uint32) []byte {
	return []byte(fmt.Sprintf(groupEntPrefixFmt, g))
}

// entKey 返回条目键：raft/<g>/ent/<index 8B BE>。
// key 带 index 后，回退覆盖时新条目直接覆盖同 index 旧条目。
func entKey(g uint32, index uint64) []byte {
	k := make([]byte, 0, len(groupEntPrefixFmt)+8)
	k = append(k, entPrefix(g)...)
	return append(k, store.PutU64(index)...)
}

// hsKey 返回一组 HardState 的固定键。单个固定键天然覆盖语义：
// 每次 Ready 的 HardState 覆盖旧值即可。
func hsKey(g uint32) []byte {
	return []byte(fmt.Sprintf(groupHsKeyFmt, g))
}

// appliedKey 返回一组已应用位点键。写入两处：普通条目段经 applyEntries 的
// FSM 批次并进（spec §5），ConfChange 条目经 SaveConfState 与成员表
// 同批（见 SaveConfState 注释）。
func appliedKey(g uint32) []byte {
	return []byte(fmt.Sprintf(groupAppliedFmt, g))
}

// confKey 返回一组成员表的固定键。ConfChange apply 时整表覆盖写
// （SaveConfState），单个固定键天然覆盖语义。
func confKey(g uint32) []byte {
	return []byte(fmt.Sprintf(groupConfFmt, g))
}

// snapKey 返回一组的快照锚点固定键。截断前整表覆盖写（SaveSnapMeta），
// 单个固定键天然覆盖语义。
func snapKey(g uint32) []byte {
	return []byte(fmt.Sprintf(groupSnapFmt, g))
}

// snapInstallKey 返回一组的快照安装中标记固定键。存在即上次安装未
// 收口（MarkInstalling 写、ResetGroupProgress/收口批次删），覆盖语义
// 与 snapKey 相同——重装时直接覆盖旧标记。
func snapInstallKey(g uint32) []byte {
	return []byte(fmt.Sprintf(groupSnapInstallFmt, g))
}

// putU32BE 大端编码 4 字节（组数/节点 ID 等 uint32 字段用；8B 用 store.PutU64）。
func putU32BE(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}
