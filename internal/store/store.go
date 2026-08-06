// store.go: Pebble 数据库的封装与写入路径设计。
//
// 职责：
//   - 使用 Pebble LSM 树提供持久化 KV 存储，支持批量操作与范围扫描
//   - 通过单一 Apply 入口保证所有写操作原子性，为未来 Raft 复制预留设计空间
//   - 提供 Get 和 Scan 查询接口，负责错误包装与内存管理
//
// 边界：
//   - Get/Apply/Scan 热路径不打日志：语义级日志由调用方（core）负责，
//     此层仅在 Open/Close 生命周期事件和错误处理时记录上下文日志
//   - 并发安全由 Pebble 自身保证，不做额外加锁
//   - 不处理数据模式变迁，key/value 解释权属于各使用方（keys.go 定义约定）
package store

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/cockroachdb/pebble/v2"
)

// Store 封装单个 Pebble 实例。并发安全（Pebble 自身保证）。
//
// Apply 是全项目唯一的写入口：core 把一次语义操作（写消息/ack/位点推进）
// 组装为一个 Batch 原子提交。v2 引入 Raft 时，只需在 Apply 前插入日志复制，
// core 与本层其余代码不动——这是 spec §3 Command 化写路径的落地形态。
type Store struct {
	db     *pebble.DB
	sync   bool
	logger *slog.Logger
}

// Open 打开（或创建）dir 下的 Pebble 库。syncWrites 决定 Apply 是否逐次 fsync。
func Open(dir string, syncWrites bool, logger *slog.Logger) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("打开 Pebble(%s): %w", dir, err)
	}
	logger.Info("store 已打开", "mod", "store", "dir", dir, "sync", syncWrites)
	return &Store{db: db, sync: syncWrites, logger: logger}, nil
}

// Close 关闭底层库。之后任何操作都会 panic（Pebble 语义），不做二次防护。
func (s *Store) Close() error {
	s.logger.Info("store 关闭", "mod", "store")
	return s.db.Close()
}

// Get 读取 key。返回值是拷贝，可长期持有。不存在返回 (nil, false, nil)。
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store Get %q: %w", key, err)
	}
	out := append([]byte(nil), v...)
	closer.Close()
	return out, true, nil
}

// NewBatch 创建写批次。调用方组装后必须交给 Apply 提交。
func (s *Store) NewBatch() *pebble.Batch { return s.db.NewBatch() }

// Apply 原子提交批次并按配置刷盘。这是唯一写入口（见类型注释）。
// 成功后批次被关闭并回收到 Pebble 的内存池；失败时批次由调用方处理。
// 调用方不应在 Apply 后重用或关闭批次（除非 Apply 失败）。
func (s *Store) Apply(b *pebble.Batch) error {
	opt := pebble.NoSync
	if s.sync {
		opt = pebble.Sync
	}
	if err := b.Commit(opt); err != nil {
		return fmt.Errorf("store Apply: %w", err)
	}
	// 提交成功，关闭批次以回收到 Pebble 的 sync.Pool（见 Pebble DB.Set 源码）。
	// 这确保热路径不会持续分配新的批次结构。
	return b.Close()
}

// Scan 按 [lower, upper) 升序遍历，最多 limit 条（limit<=0 不限）。
// fn 返回 false 或 error 时停止；k/v 底层内存仅回调期间有效，需持有请自行拷贝。
func (s *Store) Scan(lower, upper []byte, limit int, fn func(k, v []byte) (bool, error)) error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return fmt.Errorf("store Scan 创建迭代器: %w", err)
	}
	defer it.Close()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		cont, err := fn(it.Key(), it.Value())
		if err != nil || !cont {
			return err
		}
		n++
		if limit > 0 && n >= limit {
			return nil
		}
	}
	return it.Error()
}
