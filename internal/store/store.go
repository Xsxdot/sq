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
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/batchrepr"
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

// Batch 类型化写批次——「唯一写入口」的编译期强制（B2 / V2 spec §4）。
//
// 底层 *pebble.Batch 不导出：调用方只能通过本类型组装写入并交给
// Apply/ApplyAsync 提交，无法绕过写入口直接 Commit。V2 集群模式将在
// Apply/ApplyAsync 处拦截整个批次做 raft 复制，本类型是该拦截点成立的前提。
//
// 生命周期约定与旧 NewBatch 注释一致：组装后要么交给 Apply/ApplyAsync
// （此后批次归提交方处置），要么自行 Close 回收，两条路径二选一。
type Batch struct{ b *pebble.Batch }

// NewBatch 创建类型化写批次（生命周期约定见 Batch 类型注释）。
func (s *Store) NewBatch() *Batch { return &Batch{b: s.db.NewBatch()} }

// Set 在批内写入一个键值对。
func (b *Batch) Set(key, value []byte) error { return b.b.Set(key, value, nil) }

// Delete 在批内删除一个键。
func (b *Batch) Delete(key []byte) error { return b.b.Delete(key, nil) }

// DeleteRange 在批内删除 [start, end) 区间的所有键。
func (b *Batch) DeleteRange(start, end []byte) error { return b.b.DeleteRange(start, end, nil) }

// Close 回收未提交的批次（决定不提交时的唯一合法出口）。
func (b *Batch) Close() error { return b.b.Close() }

// Repr 返回批次的 Pebble 物理字节表示——集群模式的复制载荷（V2 spec §4：
// 复制物理 batch 字节而非逻辑命令）。
//
// 注意：返回的切片底层内存归批次所有，提案方需在批次 Commit/Close 前
// 拷贝（raft 库会长期持有日志条目）。
func (b *Batch) Repr() []byte { return b.b.Repr() }

// NewBatchFromRepr 从复制来的批次字节重建类型化批次——follower 盲 apply
// 的唯一入口。坏字节在此报错，不进 Apply。
//
// 注意：内部必须先拷贝 data 再交给 pebble 的 SetRepr——SetRepr 是
// 零拷贝接管（b.data 直接指向传入切片），而本方法返回的批次在
// ApplyWith 提交后会 Close 回收到 Pebble 的 batch sync.Pool，池中批次
// 连同 b.data 一起被下一次 NewBatch 复用。若直接传调用方的切片：
//   - applyEntry 传的是 raft 日志条目自身的 Data 缓冲（protobuf 反序列
//     化后 cap > len，Set 追加是原地写），条目字节与池中批次共享同一
//     块内存；
//   - 之后任何一组 raftstore.Persist 复用该池中批次写 raft/<g>/hs 等
//     键，就会原地覆盖 raft 日志条目在 MemoryStorage 里的内容，把日志
//     写花（三节点 e2e 复现的「raft/1/hs 混入 FSM 批次」损坏，apply 时
//     panic）。
//
// 拷贝成本是每 apply 一次 memcpy，相对批次解析与提交可忽略。
func (s *Store) NewBatchFromRepr(data []byte) (*Batch, error) {
	nb := s.db.NewBatch()
	if err := nb.SetRepr(append([]byte(nil), data...)); err != nil {
		// 批次已废弃，按 Apply 失败同款约定丢给 GC（见 Apply 注释）
		return nil, fmt.Errorf("store NewBatchFromRepr: %w", err)
	}
	return &Batch{b: nb}, nil
}

// BatchTouchesPrefix 判定复制批次字节中是否存在以 prefix 开头的键——只遍历
// 键、不解码值。是 meta 缓存重载钩子的判定件：follower 盲 apply 前用它判断
// 批次是否触及 meta/ 键族，命中才值得触发整表 Reload。
//
// 实现：batchrepr.Reader 逐条解出 (kind, ukey, value)，对 Set/Delete 等带键
// 条目取 ukey 判前缀即可；坏字节在此报错（与 NewBatchFromRepr 同边界）。
func BatchTouchesPrefix(repr []byte, prefix []byte) (bool, error) {
	r := batchrepr.Read(repr)
	if r == nil {
		return false, nil
	}
	for {
		_, ukey, _, ok, err := r.Next()
		if err != nil {
			return false, fmt.Errorf("store BatchTouchesPrefix 解析批次: %w", err)
		}
		if !ok {
			return false, nil
		}
		if bytes.HasPrefix(ukey, prefix) {
			return true, nil
		}
	}
}

// ApplyWith 与 Apply 同语义，但本次刷盘由 sync 参数显式决定，不看全局
// 档位。供集群层使用：raft 日志持久化的 sync 由确认档（quorum-fsync/
// quorum-mem）逐次决定，FSM apply 则总是 NoSync（持久性由 raft 日志与
// 后台批量刷盘承担，spec §5）。
//
// 失败/成功的批次归属与 Apply 完全一致（见 Apply 注释）。
func (s *Store) ApplyWith(b *Batch, sync bool) error {
	opt := pebble.NoSync
	if sync {
		opt = pebble.Sync
	}
	start := time.Now()
	if err := b.b.Commit(opt); err != nil {
		return fmt.Errorf("store ApplyWith: %w", err)
	}
	// OnApplyObserve 只观测成功提交：失败路径由调用方打日志，
	// 混进观测会污染 fsync 延迟直方图分布。
	if OnApplyObserve != nil {
		OnApplyObserve(time.Since(start))
	}
	// 成功后必须 Close 回收批次：Pebble 把关闭的批次还回内部
	// sync.Pool 复用，热路径不能持续分配新的 batch 结构。
	return b.b.Close()
}

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
func (s *Store) Apply(b *Batch) error {
	return s.ApplyWith(b, s.sync)
}

// Pending 一次「已定序、待确认持久化」的提交。值类型，热路径零额外分配。
// 由 ApplyAsync 返回；调用方必须恰好调用一次 Wait，且在 Wait 成功前不得
// 把这次写入当作已持久化（不得 ACK、不得唤醒读者）。
type Pending struct {
	b        *pebble.Batch
	start    time.Time
	needSync bool // true=还需 SyncWait（sync 模式）；false=提交已完成，Wait 只回收批次
}

// ApplyAsync 提交批次——写 WAL、发布可见、完成定序——但不等待 fsync。
//
// 与 Apply 的关系：Apply == ApplyAsync + Wait。写热路径（produce）用拆分形式
// 把 fsync 等待挪出队列锁，让 Pebble commit pipeline 把同队列多条在途提交
// 合并为一次 fsync（group commit）；其余调用点无此需求，继续用 Apply。
//
// syncWrites=false 的部署没有「等待 fsync」阶段：本方法内一次性完成提交，
// 返回的 Pending.Wait 退化为纯批次回收。
//
// 失败时批次按 Apply 同款约定丢给 GC，调用方不得再碰（理由见 Apply 注释）。
func (s *Store) ApplyAsync(b *Batch) (Pending, error) {
	start := time.Now()
	if !s.sync {
		if err := b.b.Commit(pebble.NoSync); err != nil {
			return Pending{}, fmt.Errorf("store ApplyAsync: %w", err)
		}
		if OnApplyObserve != nil {
			OnApplyObserve(time.Since(start))
		}
		return Pending{b: b.b}, nil
	}
	// ApplyNoSyncWait 要求 opts.Sync=true（Pebble 契约）：批次进入 commit
	// pipeline 排队等待 WAL fsync，但本调用不阻塞等它完成。
	if err := s.db.ApplyNoSyncWait(b.b, pebble.Sync); err != nil {
		return Pending{}, fmt.Errorf("store ApplyAsync: %w", err)
	}
	return Pending{b: b.b, start: start, needSync: true}, nil
}

// Wait 等待批次持久化完成并回收批次。
//
// 观测口径与 Apply 一致：OnApplyObserve 覆盖「提交开始 → 持久化完成」全程，
// 直方图语义不因拆分而改变。SyncWait 失败时批次丢给 GC（同 Apply 约定）；
// WAL sync 失败意味着 Pebble 已进入不可恢复错误态，后续写入都会失败，
// 调用方把错误上抛即可，无需（也无法）在此挽救。
func (p Pending) Wait() error {
	if p.needSync {
		if err := p.b.SyncWait(); err != nil {
			return fmt.Errorf("store WaitSync: %w", err)
		}
		if OnApplyObserve != nil {
			OnApplyObserve(time.Since(p.start))
		}
	}
	return p.b.Close()
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
