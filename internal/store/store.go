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
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// OnApplyObserve 若非 nil，每次 Apply 成功提交后以提交耗时（含 fsync）回调，
// 供 metrics 装配 fsync 延迟直方图（spec §8）。契约：进程装配阶段设置一次，
// 服务启动后只读——据此不加锁；运行期改写属数据竞态，禁止。
var OnApplyObserve func(d time.Duration)

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
	// mod 一次性绑在 logger 上（与 meta/produce/deliver/rpc 各包的做法一致），
	// 之后各调用点不再逐条重复这对键值。
	l := logger.With("mod", "store")
	l.Info("store 已打开", "dir", dir, "sync", syncWrites)
	return &Store{db: db, sync: syncWrites, logger: l}, nil
}

// Close 关闭底层库。之后任何操作都会 panic（Pebble 语义），不做二次防护。
func (s *Store) Close() error {
	s.logger.Info("store 关闭")
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

// NewBatch 创建写批次。批次有两条合法的终止路径，且仅能选一条：
//  1. 组装后交给 Apply 提交——此后批次归 Apply 处置，调用方不再碰它
//     （成功即回收，失败则弃给 GC，理由见 Apply 注释）
//  2. 决定不提交，则必须自行调用 Close() 以回收内存
//
// 不可同时执行两条路径，也不可多次 Close。
func (s *Store) NewBatch() *pebble.Batch { return s.db.NewBatch() }

// Apply 原子提交批次并按配置刷盘。这是唯一写入口（见类型注释）。
//
// 成功时批次由 Apply 关闭并回收到 Pebble 内存池；调用方不再拥有此批次。
//
// 失败时批次被有意丢给 GC，调用方不需要（也不应该）再调 Close()。这与 Pebble
// 自己的做法一致：DB.Set/DB.Delete 内部同样是 `if err := d.Apply(b, opts);
// err != nil { return err }`，并在注释里写明 "Only release the batch on
// success"。理由有三条，缺一不可：
//   - Batch.release() 明确拒绝回收出过错的批次，Close() 换不回内存池复用，
//     只是把一次本可以省掉的调用加到每个错误分支上；
//   - DB.Close() 会追踪泄漏的迭代器、快照与 memtable 预留，唯独不追踪批次，
//     所以一个没关的失败批次就是一块普通垃圾，不会让 Close 报泄漏；
//   - 提交失败的批次内部状态已不可信，再去碰它没有任何收益。
//
// 写下这段是为了防止有人"顺手修好"：本项目 6 个调用点全都在 Apply 出错后直接
// 返回，这是刻意的，不是遗漏。
func (s *Store) Apply(b *pebble.Batch) error {
	opt := pebble.NoSync
	if s.sync {
		opt = pebble.Sync
	}
	start := time.Now()
	if err := b.Commit(opt); err != nil {
		return fmt.Errorf("store Apply: %w", err)
	}
	if OnApplyObserve != nil {
		// 只观测成功提交：失败路径由调用方带上下文记日志，混进直方图反而
		// 会把错误重试的耗时污染进正常刷盘分布。
		OnApplyObserve(time.Since(start))
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
