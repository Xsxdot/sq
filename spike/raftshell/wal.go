// wal.go 提供每节点独立的 pebble 持久化层，两档刷盘在此分流。
//
// 职责：raft 日志（HardState + Entries）的单批次持久化与截断回退覆盖；
// AckQuorumMem 档的后台周期 fsync。
// 边界：不做快照、不做成员表持久化、不做日志读取——spike 只写不读，
// raft 库的读取视图由 Node.storage（MemoryStorage）承担。
package raftshell

import (
	"encoding/binary"
	"log/slog"
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
// 职责：raft 日志（HardState + Entries）的持久化与截断回退。
// 边界：不做快照、不做成员表持久化、不做日志读取（spike 只写不读，
//
//	raft 库的读取视图由 Node.storage 承担）。
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
