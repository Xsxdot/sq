// wal.go 提供每节点独立的 pebble 持久化层，两档刷盘在此分流。
//
// 职责：raft 日志（HardState + Entries）的单批次持久化与截断回退覆盖；
// AckQuorumMem 档的后台周期 fsync；干净关机标记（meta/clean_shutdown）
// 的写入与消费；重启时的全量重放读取（Load）与成员表 ConfState 合成。
// 边界：不做快照、不做成员表持久化——重启时 raft 库只信任
// InitialState() 的 ConfState，而 MemoryStorage 的 ConfState 来自快照
// 元数据，因此由调用方从日志重放 ConfChange 合成（confStateFromEntries）。
package raftshell

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// WAL 是每个节点独立的 pebble 持久化层，两档刷盘在此分流：
// AckQuorumFsync 每批带 pebble.Sync；AckQuorumMem 用 NoSync 提交，
// 由后台 goroutine 每 200ms 借一个空批次的 pebble.Sync 强制刷盘
// （WAL 顺序性保证：一次 fsync 让此前所有 NoSync 写都落盘）。
//
// 职责：raft 日志（HardState + Entries）的持久化与截断回退；
// AckQuorumMem 档的后台周期 fsync；干净关机标记与全量重放读取。
// 边界：不做快照、不做成员表持久化（重启时由调用方从日志重放合成）。
type WAL struct {
	db   *pebble.DB
	mode AckMode
	lg   *slog.Logger

	closeOnce sync.Once
	stopCh    chan struct{} // 仅 AckQuorumMem 档使用：通知后台刷盘 goroutine 退出
	doneCh    chan struct{}
}

const (
	// hardstateKey 是 HardState 的固定 key。
	// 单个固定 key 天然覆盖语义：每次 Ready 的 HardState 覆盖旧值即可。
	hardstateKey = "hs"
	// entryKeyPrefix 是日志条目 key 前缀，后续跟 8 字节大端 index。
	// key 带 index 后，日志截断回退时新条目直接覆盖同 index 旧条目，
	// 不需要显式删除。
	entryKeyPrefix = "ent/"
	// entryKeyLen = len(entryKeyPrefix) + 8
	entryKeyLen = 4 + 8
	// cleanShutdownKey 是干净关机标记：StopClean 写（Sync 落盘），
	// 重启时读到即走原身份回归；读后即删，下次崩溃自然无标记。
	// 标记存在与否就是 Restart 两条路径的唯一判定依据。
	cleanShutdownKey = "meta/clean_shutdown"
)

// openWAL 打开（或创建）节点目录下的 pebble 数据库。
// mode 为 AckQuorumMem 时同时启动后台周期 fsync goroutine。
func openWAL(dir string, mode AckMode, lg *slog.Logger) (*WAL, error) {
	// pebble 默认 logger 会把「Found N WALs」等打开期信息打到 stderr，
	// spike 的日志统一走 slog，这里显式压掉
	db, err := pebble.Open(dir, &pebble.Options{Logger: discardLogger{}})
	if err != nil {
		return nil, err
	}
	w := &WAL{db: db, mode: mode, lg: lg, stopCh: make(chan struct{}), doneCh: make(chan struct{})}
	lg.Info("WAL 已打开", "dir", dir, "mode", mode.String())
	if mode == AckQuorumMem {
		go w.memFsyncLoop()
	}
	return w, nil
}

// Persist 把一轮 Ready 的 HardState 与 Entries 写入 WAL。
//
// 参数：
//   - hs: 非空的 HardState（含 Term/Vote/Commit），可为 nil
//   - ents: 本轮新增的日志条目（含截断回退场景下的覆盖条目）
//   - sync: true 时本次提交带 fsync（AckQuorumFsync 档）
//
// 注意：所有写入在单个批次内完成——HardState 与对应 Entries
// 要么全部落盘、要么全部丢失，避免半写状态。
func (w *WAL) Persist(hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error {
	b := w.db.NewBatch()
	defer b.Close()

	if !raft.IsEmptyHardState(hs) {
		data, err := proto.Marshal(hs)
		if err != nil {
			return err
		}
		if err := b.Set([]byte(hardstateKey), data, nil); err != nil {
			return err
		}
	}
	for _, ent := range ents {
		key := make([]byte, entryKeyLen)
		copy(key, entryKeyPrefix)
		binary.BigEndian.PutUint64(key[len(entryKeyPrefix):], ent.GetIndex())
		data, err := proto.Marshal(ent)
		if err != nil {
			return err
		}
		if err := b.Set(key, data, nil); err != nil {
			return err
		}
	}
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	return b.Commit(opts)
}

// MarkCleanShutdown 写入干净关机标记并 Sync 落盘。
// 必须在节点退出（WAL 关闭）之前调用，标记的持久化先于任何关闭操作。
func (w *WAL) MarkCleanShutdown() error {
	return w.db.Set([]byte(cleanShutdownKey), []byte{1}, pebble.Sync)
}

// ConsumeCleanShutdown 读取并删除干净关机标记，返回标记是否存在。
//
// 删除带 Sync：本节点此后若再次崩溃（未走 StopClean），标记不会残留，
// 重启时 Restart 能正确判定为不干净关机。
func (w *WAL) ConsumeCleanShutdown() (bool, error) {
	_, closer, err := w.db.Get([]byte(cleanShutdownKey))
	switch {
	case err == pebble.ErrNotFound:
		return false, nil
	case err != nil:
		return false, err
	}
	closer.Close()
	if err := w.db.Delete([]byte(cleanShutdownKey), pebble.Sync); err != nil {
		return true, err
	}
	return true, nil
}

// Load 全量读取 WAL 中的 HardState 与全部日志条目（spike 不做压缩，
// 一条不落全量读回）。
//
// 返回：
//   - hs: 从未持久化过时为空 HardState（raft.IsEmptyHardState 为真）
//   - ents: 按 index 升序的全部条目，可能为空
//   - err: 读取或反序列化失败
//
// 注意：条目连续性由 raft 写入契约保证；这里显式按 index 排序，
// 不依赖 pebble 的迭代顺序。
func (w *WAL) Load() (*raftpb.HardState, []*raftpb.Entry, error) {
	hs := &raftpb.HardState{}
	hsData, closer, err := w.db.Get([]byte(hardstateKey))
	switch {
	case err == pebble.ErrNotFound:
	case err != nil:
		return nil, nil, err
	default:
		closer.Close()
		hs.Reset()
		if err := proto.Unmarshal(hsData, hs); err != nil {
			return nil, nil, err
		}
	}

	prefix := []byte(entryKeyPrefix)
	var ents []*raftpb.Entry
	it, err := w.db.NewIter(nil)
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if !bytes.HasPrefix(it.Key(), prefix) {
			continue
		}
		ent := &raftpb.Entry{}
		ent.Reset()
		if err := proto.Unmarshal(it.Value(), ent); err != nil {
			return nil, nil, err
		}
		ents = append(ents, ent)
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].GetIndex() < ents[j].GetIndex() })
	return hs, ents, nil
}

// confStateFromEntries 从日志条目重放成员变更，合成重启所需的 ConfState。
//
// raft 库 newRaft 只信任 Storage.InitialState() 返回的 ConfState（来自
// MemoryStorage 的快照元数据），不会自己回放 ConfChange 条目重建成员表；
// 而 spike 不做快照，因此必须由调用方重放。条目中的 ConfChange 序列
// 与节点运行期 apply 的顺序一致，重放结果即关机时刻的成员表。
//
// 注意（B8.2 观察项）：这里重放了全部 ConfChange 条目，包括 commit
// 之外可能存在的未提交尾部；正确做法应只回放到 HardState.Commit
// （调用方手里有 HardState 却未传入）。当前 spike 不可能出现未提交的
// ConfChange 尾部：ConfChange 只由 restartAsLearner 中存活的 leader
// 提出，且 ProposeConfChange 等待 apply 返回后才继续，重启节点自身
// 从不当 leader，因此重放序列必然全部已提交。若未来允许 leader 带着
// 未提交 ConfChange 崩溃、或引入日志截断，此处必须先按 commit 裁剪
// 再合成成员表，纳入 B8.2 快照/压缩观察项。
func confStateFromEntries(ents []*raftpb.Entry) *raftpb.ConfState {
	cs := &raftpb.ConfState{}
	for _, ent := range ents {
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

// memFsyncLoop 是 AckQuorumMem 档的后台刷盘 goroutine：
// 每 200ms 提交一个空批次并带 pebble.Sync。空批次不携带数据，
// 但会触发一次 WAL fsync，借 WAL 顺序性把此前所有 NoSync 写入
// 一并刷盘（spec §2.2「后台批量 fsync」的最简实现）。
func (w *WAL) memFsyncLoop() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	defer close(w.doneCh)
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			b := w.db.NewBatch()
			err := b.Commit(pebble.Sync)
			b.Close()
			if err != nil {
				w.lg.Error("后台刷盘失败", "err", err)
			}
		}
	}
}

// discardLogger 是 pebble 的静默日志器：满足 pebble.Logger 接口但不输出。
type discardLogger struct{}

func (discardLogger) Infof(string, ...interface{})  {}
func (discardLogger) Errorf(string, ...interface{}) {}
func (discardLogger) Fatalf(string, ...interface{}) {}

// Close 关闭 WAL：先停后台刷盘 goroutine，再关闭 pebble。
// 幂等，可安全多次调用。
func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		if w.mode == AckQuorumMem {
			close(w.stopCh)
			<-w.doneCh
		}
		err = w.db.Close()
		w.lg.Info("WAL 已关闭")
	})
	return err
}
