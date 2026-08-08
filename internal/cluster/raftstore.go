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
//   - 重启全量重读（Load）与按 commit 裁剪的成员表合成（confStateFromEntries）
//   - 数据组数契约（EnsureGroups）与干净关机标记（Mark/ConsumeCleanShutdown）
//
// 边界：
//   - 不做日志截断与快照（batch④）：日志无界增长，learner 追齐走全量重放
//   - 不持有 pebble：一切写经 store 唯一写入口（NewBatch + ApplyWith），
//     本层没有裸 db 句柄——B2 唯一写入口在集群层同样成立
//   - applied 的写入由 FSM apply 路径并进批次（Task 4），本层只读
//
// 键布局（共库下以 raft/ 前缀隔离日志区与 FSM 区；键内定长二进制大端，
// 保证字节序=数值序，区间扫描天然升序）：
//
//	raft/groups                  → uint32 BE 数据组数（首启写入，此后校验）
//	raft/clean_shutdown          → 干净关机标记（StopClean 写，启动读后删）
//	raft/<g>/hs                  → HardState protobuf
//	raft/<g>/ent/<index 8B BE>   → Entry protobuf
//	raft/<g>/applied             → uint64 BE applied index（只读，写入在 Task 4）
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

// Load 全量读回一组的 HardState 与全部日志条目（本批无截断，一条不落）。
//
// 返回：
//   - hs: 从未持久化过时为空 HardState（raft.IsEmptyHardState 为真）
//   - ents: 按 index 升序的全部条目，可能为空
//   - err: 读取或反序列化失败
//
// 注意：store.Scan 按 key 升序遍历，key 内 8B 大端 index 保证
// 字节序=数值序，读回天然升序——spike 里兜底的显式 sort 在此可去。
// 条目连续性由 raft 写入契约保证，本层不校验。
func (r *raftStore) Load(g uint32) (*raftpb.HardState, []*raftpb.Entry, error) {
	hs := &raftpb.HardState{}
	hsData, ok, err := r.st.Get(hsKey(g))
	if err != nil {
		return nil, nil, fmt.Errorf("raftstore Load 组 %d 读 HardState: %w", g, err)
	}
	if ok {
		hs.Reset()
		if err := proto.Unmarshal(hsData, hs); err != nil {
			return nil, nil, fmt.Errorf("raftstore Load 组 %d 解码 HardState: %w", g, err)
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
		return nil, nil, fmt.Errorf("raftstore Load 组 %d 扫描条目: %w", g, err)
	}
	// 重启排障的第一行证据：组号、条目数、commit 位。
	r.lg.Debug("raft 日志已读回", "g", g, "entries", len(ents), "commit", hs.GetCommit())
	return hs, ents, nil
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
// 必须在节点退出之前调用，标记的持久化先于任何关闭操作。
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
// raft 库 newRaft 只信任 Storage.InitialState() 返回的 ConfState，不会
// 自己回放 ConfChange 条目重建成员表；而本批不做快照，因此必须由调用方
// 重放。重放只到 commit 为止——commit 之外的未提交 ConfChange 尾巴
// 不得进成员表（终审观察项②），否则重启的节点会把从未被多数派接受的
// 成员变更当成既定事实。
//
// 条目按 index 升序传入（Load 的返回契约），commit 裁剪直接 break。
func confStateFromEntries(ents []*raftpb.Entry, commit uint64) *raftpb.ConfState {
	cs := &raftpb.ConfState{}
	for _, ent := range ents {
		if ent.GetIndex() > commit {
			break // 条目升序，越过 commit 即全部未提交
		}
		if ent.GetType() != raftpb.EntryConfChange {
			continue
		}
		var cc raftpb.ConfChange
		cc.Reset()
		_ = proto.Unmarshal(ent.Data, &cc)
		id := cc.GetNodeId()
		switch cc.GetType() {
		case raftpb.ConfChangeAddNode:
			removeUint64(&cs.Voters, id)
			removeUint64(&cs.Learners, id)
			cs.Voters = append(cs.Voters, id)
		case raftpb.ConfChangeAddLearnerNode:
			removeUint64(&cs.Voters, id)
			removeUint64(&cs.Learners, id)
			cs.Learners = append(cs.Learners, id)
		case raftpb.ConfChangeRemoveNode:
			removeUint64(&cs.Voters, id)
			removeUint64(&cs.Learners, id)
		}
	}
	return cs
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

// appliedKey 返回一组已应用位点键。写入由 FSM apply 路径并进批次（Task 4）。
func appliedKey(g uint32) []byte {
	return []byte(fmt.Sprintf(groupAppliedFmt, g))
}

// putU32BE 大端编码 4 字节（组数/节点 ID 等 uint32 字段用；8B 用 store.PutU64）。
func putU32BE(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}
