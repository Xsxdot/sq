// raftstore.go 提供 raft 日志的共库持久化层：raft 日志与 FSM 数据共用
// 同一个 store.Store，日志键全部落在 raft/ 前缀下。
//
// 职责：
//   - 每轮 Ready 的 HardState + Entries 单批原子持久化（Persist）
//   - 回退覆盖语义：写入新条目后删除更高 index 的旧条目（尾截断），
//     全部在同一个批内完成——「先写后删尾」之所以成立，正依赖单批原子性：
//     要么新旧一致整体落盘，要么整体不落，不存在「新条目在、幽灵条目也在」
//     的半截状态。批内次序本身无所谓（同批内无中间可见性），
//     删尾必须与写条目同批才是语义关键。
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
//   - 不持有 pebble：一切写经 store 唯一写入口（NewBatch + ApplyWith），
//     本层没有裸 db 句柄——B2 唯一写入口在集群层同样成立
//   - applied 的写入两处：普通条目经 FSM apply 批次并进，ConfChange
//     条目经 SaveConfState 与成员表同批
//
// 键布局（共库下以 raft/ 前缀隔离日志区与 FSM 区；键内定长二进制大端，
// 保证字节序=数值序，区间扫描天然升序）：
//
//	raft/groups                  → uint32 BE 数据组数（首启写入，此后校验）
//	raft/clean_shutdown          → 干净关机标记（StopClean 写，启动读后删）
//	raft/<g>/hs                  → HardState protobuf
//	raft/<g>/conf                → ConfState protobuf（成员表，ConfChange
//	                               apply 时整表覆盖写，SaveConfState）
//	raft/<g>/snap                → SnapshotMetadata protobuf（快照锚点，
//	                               截断的前提，SaveSnapMeta Sync 落盘）
//	raft/<g>/ent/<index 8B BE>   → Entry protobuf
//	raft/<g>/applied             → uint64 BE applied index（普通条目经
//	                               FSM 批次并进，ConfChange 与成员表同批）
package cluster

import (
	"encoding/binary"
	"fmt"
	"log/slog"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/xushixin/sq/internal/store"
)

// raft 前缀常量。全部 raft 日志键在同一个 store 库内，与 FSM 数据
// （msg/、cursor/、meta/ 等）同库隔离；组号用十进制字符串编码，
// 后跟 '/' 定界——任意两位组号的键序严格分离（"1/" < "10/…"），
// 前缀扫描不会跨组串扰。
const (
	groupsKey         = "raft/groups"
	cleanShutdownKey  = "raft/clean_shutdown"
	groupEntPrefixFmt = "raft/%d/ent/"
	groupHsKeyFmt     = "raft/%d/hs"
	groupConfFmt      = "raft/%d/conf"
	groupSnapFmt      = "raft/%d/snap"
	groupAppliedFmt   = "raft/%d/applied"
)

// raftStore 是 raft 日志的共库持久层。
type raftStore struct {
	st *store.Store
	lg *slog.Logger
}

// newRaftStore 构造 raft 日志持久层。lg 绑定 mod=raftstore 键。
func newRaftStore(st *store.Store, lg *slog.Logger) *raftStore {
	return &raftStore{st: st, lg: lg.With("mod", "raftstore")}
}

// Persist 单批原子持久化一轮 Ready 的 HardState 与 Entries。
//
// 参数：
//   - g: 数据组号
//   - hs: 非空的 HardState（含 Term/Vote/Commit），可为 nil（本轮无状态变更）
//   - ents: 本轮要追加/覆盖的日志条目；非空时其末条 index 即新日志尾，
//     批内同时删除更高 index 的旧条目（raft 回退覆盖语义）
//   - sync: true 时本次提交带 fsync（quorum-fsync 档）
//
// 注意：
//   - 写 HardState、写条目、删尾三部曲在同一个批次内完成（store 单批原子），
//     不存在半写状态；「删尾」与「写新条目」同批是语义关键，
//     分开提交就会在崩溃窗口留下幽灵条目，选举后状态机分叉。
//   - ents 为空（仅 HardState 变更，如投票轮）时不删尾——此时日志未被
//     改动，若仍从 index 1 起删会把整段日志误清。
func (r *raftStore) Persist(g uint32, hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error {
	b := r.st.NewBatch()
	if hs != nil {
		data, err := proto.Marshal(hs)
		if err != nil {
			b.Close()
			return fmt.Errorf("raftstore Persist 组 %d 编码 HardState: %w", g, err)
		}
		if err := b.Set(hsKey(g), data); err != nil {
			b.Close()
			return fmt.Errorf("raftstore Persist 组 %d 写 HardState: %w", g, err)
		}
	}
	for _, ent := range ents {
		data, err := proto.Marshal(ent)
		if err != nil {
			b.Close()
			return fmt.Errorf("raftstore Persist 组 %d 编码条目 %d: %w", g, ent.GetIndex(), err)
		}
		if err := b.Set(entKey(g, ent.GetIndex()), data); err != nil {
			b.Close()
			return fmt.Errorf("raftstore Persist 组 %d 写条目 %d: %w", g, ent.GetIndex(), err)
		}
	}
	if len(ents) > 0 {
		// 新日志尾 = ents 末条 index；其上的旧条目是回退冲突残留，
		// 与写入同批删光（见方法注释）。
		last := ents[len(ents)-1].GetIndex()
		if err := b.DeleteRange(entKey(g, last+1), store.PrefixUpperBound(entPrefix(g))); err != nil {
			b.Close()
			return fmt.Errorf("raftstore Persist 组 %d 删尾(>%d): %w", g, last, err)
		}
	}
	// 失败时批次按 store 契约丢给 GC，调用方不得再碰（store.Apply 注释）。
	if err := r.st.ApplyWith(b, sync); err != nil {
		return fmt.Errorf("raftstore Persist 组 %d: %w", g, err)
	}
	return nil
}

// Load 读回一组的 HardState、现存日志条目与快照元数据（截断锚点）。
//
// 返回：
//   - hs: 从未持久化过时为空 HardState（raft.IsEmptyHardState 为真）
//   - ents: 按 index 升序的现存条目；截断过的组不含 ≤ 锚点 index 的
//     条目（已被 TruncateLog 范围删除），可能为空（日志被全量截断）
//   - snapMeta: 组的快照元数据（截断锚点）；从未截断过时为 nil
//   - err: 读取或反序列化失败
//
// 注意：store.Scan 按 key 升序遍历，key 内 8B 大端 index 保证
// 字节序=数值序，读回天然升序——spike 里兜底的显式 sort 在此可去。
// 条目连续性由 raft 写入契约保证，本层不校验。
func (r *raftStore) Load(g uint32) (*raftpb.HardState, []*raftpb.Entry, *raftpb.SnapshotMetadata, error) {
	hs := &raftpb.HardState{}
	hsData, ok, err := r.st.Get(hsKey(g))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raftstore Load 组 %d 读 HardState: %w", g, err)
	}
	if ok {
		hs.Reset()
		if err := proto.Unmarshal(hsData, hs); err != nil {
			return nil, nil, nil, fmt.Errorf("raftstore Load 组 %d 解码 HardState: %w", g, err)
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
				return false, fmt.Errorf("raftstore Load 组 %d 解码条目: %w", g, err)
			}
			ents = append(ents, ent)
			return true, nil
		})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raftstore Load 组 %d 扫描条目: %w", g, err)
	}

	// 截断锚点与条目一并读回：buildGroup 用它在日志被全量截断时
	// 恢复 MemoryStorage 的位点与成员表（见 buildGroup）。
	snapMeta, _, err := r.LoadSnapMeta(g)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raftstore Load 组 %d 读快照元数据: %w", g, err)
	}
	// 重启排障的第一行证据：组号、条目数、commit 位、锚点位。
	r.lg.Debug("raft 日志已读回", "g", g, "entries", len(ents), "commit", hs.GetCommit(),
		"snap", snapMeta.GetIndex())
	return hs, ents, snapMeta, nil
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
	data, err := proto.Marshal(cs)
	if err != nil {
		return fmt.Errorf("raftstore SaveConfState 组 %d 编码: %w", g, err)
	}
	b := r.st.NewBatch()
	if err := b.Set(confKey(g), data); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveConfState 组 %d 写成员表: %w", g, err)
	}
	if err := b.Set(appliedKey(g), store.PutU64(applied)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveConfState 组 %d 写 applied: %w", g, err)
	}
	// 成员表是选举安全的根：Sync 落盘，不进 quorum-mem 的异步刷盘队列
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveConfState 组 %d: %w", g, err)
	}
	r.lg.Info("成员表已持久化", "g", g, "voters", cs.GetVoters(), "learners", cs.GetLearners(), "applied", applied)
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
	data, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("raftstore SaveSnapMeta 组 %d 编码: %w", g, err)
	}
	b := r.st.NewBatch()
	if err := b.Set(snapKey(g), data); err != nil {
		b.Close()
		return fmt.Errorf("raftstore SaveSnapMeta 组 %d: %w", g, err)
	}
	if err := r.st.ApplyWith(b, true); err != nil {
		return fmt.Errorf("raftstore SaveSnapMeta 组 %d: %w", g, err)
	}
	r.lg.Info("快照锚点已落盘", "g", g, "index", meta.GetIndex(), "term", meta.GetTerm())
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

// TruncateLog 删除 index ≤ upto 的日志条目（Pebble range delete）。
//
// 前置：upto ≤ 已落盘 snap 元数据的 Index（锚点必须先落盘，见
// SaveSnapMeta）。违反即报错拒绝执行——这是「先锚点后截断」顺序的
// 编译期之外的运行期守卫。
//
// 幂等：重复截断到同一位点是 range delete 的无操作，周期截断会撞上。
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
	b := r.st.NewBatch()
	if err := b.DeleteRange(entKey(g, 0), entKey(g, upto+1)); err != nil {
		b.Close()
		return fmt.Errorf("raftstore TruncateLog 组 %d 删条目(≤%d): %w", g, upto, err)
	}
	if err := r.st.ApplyWith(b, false); err != nil { // 截断丢了只是白留日志，无需 fsync
		return fmt.Errorf("raftstore TruncateLog 组 %d: %w", g, err)
	}
	r.lg.Info("raft 日志已截断", "g", g, "upto", upto)
	return nil
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

// appliedKey 返回一组已应用位点键。写入两处：普通条目经 applyEntry 的
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

// putU32BE 大端编码 4 字节（组数/节点 ID 等 uint32 字段用；8B 用 store.PutU64）。
func putU32BE(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}
