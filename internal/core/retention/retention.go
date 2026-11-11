// Package retention 实现消息过期清理后台任务（spec §5 流程 7）。
//
// 职责：
//   - 周期扫描全部 topic：按各自 retention 时长清理过期 msg/（DeleteRange），
//     并在同一批次点删这些消息的 keyidx/ 条目（索引键由被删消息的 Keys、
//     StoreAtMs、queueID、offset 拼出，见 purgeQueue）
//   - 单趟工作量有上限（maxPurgePerQueue 条消息 + maxIdxDeleteBytes 索引字节），
//     超出留待下趟并记日志（不静默截断）
//   - 后台补扫（backfill）：清掉升级前遗留的孤儿索引（消息已删、索引残留），
//     限速、可中断、只扫本节点 lead 的组，每进程每组至多完成一次
//
// 约束（承重）：key 索引必须与其消息同批写、同批删。写侧是 produce 的
// Append/AppendBatch（消息与索引同一批次），删侧是 purgeQueue（同一批次）。
// 稳态清理因此不扫描 keyidx——keyidx 按业务 key 排序而非时间，全扫的代价随
// 保留期内消息数线性增长。未来若出现「索引与消息不同批」的写法，产生的孤儿
// 只能靠每进程一次的补扫兜底，会悄悄积累——新增写路径必须遵守本约束。
//
// 边界：
//   - 不清理 cursor/inflight：消费位点扫描天然越过已删区间；指向已删消息的
//     inflight 由 deliver 的孤儿清理兜底（M1 已实现并有用例钉住）
//   - 补扫完成状态只在进程内存（不新增持久化键：任何新键都是盘上格式变化，
//     数据组快照枚举会拒绝未知键族）；重启后重扫一次是有界的空跑
//
// 组归属（batch③）：msg/ 与 keyidx/ 键族归所属队列组——purgeQueue 先查
// rt.IsLeader(g) 再动手，非 leader 队列跳过（各组 leader 各扫各的，摊布语义
// 见 Task 8 说明）；补扫按 key 内 queueID 归组、按组攒批，提交前复核 leader。
// Run 层另有「本趟 0 队列可处理」的 Debug 日志——本节点未 lead 任何组时的
// 空转观测面（摊布稳态下是常态，启动期可定位问题）。
package retention

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
	"github.com/Xsxdot/sq/internal/sysinfo"
)

// maxPurgePerQueue 单队列/单索引扫描单趟最多清理条数，防止单趟长时间占用。
const maxPurgePerQueue = 10000

// maxIdxDeleteBytes 单批索引删除的键字节预算。一批 = 一条 raft 提案：raft
// MaxSizePerMsg 为 1MiB、传输层单帧硬上限 16MiB，超帧的提案永远发不出去、
// 组会卡死；用户 key 长度与个数只受 gRPC 消息大小约束，不设预算时 1 万条
// 消息 × 多个长 key 能轻易越过上限。预算只在消息边界截停（见 purgeQueue）。
const maxIdxDeleteBytes = 512 << 10

// Manager 过期清理任务。单 goroutine 运行（Run），Pass 可单独调用（测试/未来 Admin 触发）。
type Manager struct {
	rep      replication.Replicator
	rt       replication.Router
	st       *store.Store
	mt       *meta.Meta
	interval time.Duration
	logger   *slog.Logger

	// 磁盘水位保护三件套（spec §7 拒写保读）：
	// dataDir 为探测目标（磁盘使用率按它所在的文件系统计算）；
	// watermarkPct<=0 或 writeBlocked 为 nil 时水位检查整体禁用。
	dataDir      string
	watermarkPct int
	writeBlocked *atomic.Bool

	// 观测统计（日志与测试读取；Pass 与补扫分属两个 goroutine，故用原子量）。
	// 本包内一切 keyidx 迭代都经 scanKeyIdx 并计入对应计数器——
	// passIdxScanned 是「稳态清理扫描 keyidx 键数」，按设计恒为 0。
	passIdxScanned     atomic.Int64 // 最近一趟 Pass 扫描的 keyidx 键数
	passIdxDeleted     atomic.Int64 // 最近一趟 Pass 同批删除的索引条数
	backfillScanned    atomic.Int64 // 补扫累计扫描的 keyidx 键数
	backfillDeleted    atomic.Int64 // 补扫累计删除的孤儿索引条数
	backfillDoneGroups atomic.Int64 // 已完成补扫的组数

	// backfillDone 已完成补扫的组集合。只由补扫 goroutine（或同步调用
	// backfillRound 的测试）访问，不加锁。
	backfillDone map[uint32]bool
}

// New 构造清理任务。
//
// 参数：
//   - rep/rt: 复制抽象与组路由视图（单机档传 replication.NewStandalone(st)
//     与 StandaloneRouter{}，集群档由 main 装配）——按队列组提交清理批次、
//     只清本节点 leader 的组
//   - interval: 扫描间隔（config.RetentionInterval()）
//   - dataDir/watermarkPct/writeBlocked: 磁盘水位保护三件套；
//     watermarkPct<=0 或 writeBlocked 为 nil 时水位检查禁用
func New(rep replication.Replicator, rt replication.Router, st *store.Store, mt *meta.Meta,
	interval time.Duration, dataDir string, watermarkPct int, writeBlocked *atomic.Bool,
	logger *slog.Logger) *Manager {
	return &Manager{rep: rep, rt: rt, st: st, mt: mt, interval: interval, dataDir: dataDir,
		watermarkPct: watermarkPct, writeBlocked: writeBlocked, logger: logger.With("mod", "retention"),
		backfillDone: map[uint32]bool{}}
}

// Run 阻塞运行清理循环：启动即跑一趟，此后每 interval 一趟；ctx 取消即返回。
// 调用方（main）负责放入独立 goroutine 并在停机时先取消再关 store。
//
// 门控说明：retention 没有「meta leader 才跑」的整趟门——数据组分散在
// 各节点，每个节点都清自己 lead 的队列（purgeQueue 逐队列判 lead）；
// checkDisk 也无条件执行（本地磁盘水位是本节点的事实，与组 leadership
// 无关，拒写保护不能等 leader 产生才生效）。Run 层只补「本趟 0 队列
// 可处理」的 Debug 观测（见下方注释）。
//
// 补扫 goroutine 由 Run 拉起、受同一 ctx 约束，Run 返回前等它退出——调用方
// 「取消 → 等 Run 返回 → 关 store」的停机顺序因此同样覆盖补扫。
func (m *Manager) Run(ctx context.Context) {
	m.logger.Info("retention 任务启动", "interval", m.interval.String())
	var bfWG sync.WaitGroup
	bfWG.Add(1)
	go func() { defer bfWG.Done(); m.runBackfill(ctx) }()
	defer bfWG.Wait()
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		m.checkDisk()
		if n, proc, err := m.Pass(ctx); err != nil {
			m.logger.Error("retention 清理失败", "err", err)
		} else if n > 0 {
			m.logger.Info("retention 清理完成", "purged_msgs", n,
				"purged_idx", m.passIdxDeleted.Load(), "idx_scanned", m.passIdxScanned.Load())
		} else if proc == 0 {
			// 本趟 0 队列可处理：本节点未 lead 任何队列组，清理由各组
			// leader 承担——摊布稳态下每节点只清自己的组，这是常态；
			// 但启动窗口/leader 变更期看到它，可定位「为什么没清」。
			m.logger.Debug("本趟 0 队列可处理——本节点未 lead 任何队列组")
		}
		select {
		case <-ctx.Done():
			m.logger.Info("retention 任务退出")
			return
		case <-t.C:
		}
	}
}

// checkDisk 探测磁盘用量并更新拒写开关。只在状态翻转时打日志（避免每趟刷屏）。
//
// 探测实现在 internal/sysinfo：控制台的 /admin/system 与 /metrics 的
// sq_disk_used_percent 走的是同一个函数，三方看到的百分比必须是同一个数 ——
// 「日志说拒写了但控制台显示 60%」是最坏的排查体验。
func (m *Manager) checkDisk() {
	if m.watermarkPct <= 0 || m.writeBlocked == nil {
		return
	}
	d, err := sysinfo.DiskUsage(m.dataDir)
	if err != nil {
		m.logger.Warn("磁盘水位检查失败，本趟跳过", "dir", m.dataDir, "err", err)
		return
	}
	blocked := d.UsedPercent >= float64(m.watermarkPct)
	if blocked != m.writeBlocked.Load() {
		if blocked {
			m.logger.Error("磁盘使用率超过水位线，进入拒写保读",
				"used_pct", d.UsedPercent, "watermark", m.watermarkPct,
				"free_bytes", d.FreeBytes, "total_bytes", d.TotalBytes)
		} else {
			m.logger.Info("磁盘使用率回落，恢复写入",
				"used_pct", d.UsedPercent, "watermark", m.watermarkPct,
				"free_bytes", d.FreeBytes)
		}
		m.writeBlocked.Store(blocked)
	}
}

// Pass 执行一趟全量清理。
//
// 返回：
//   - purged: 清掉的消息条数（keyidx 条目不计入，见 passIdxDeleted）
//   - processable: 本趟可处理的队列数（其组由本节点 lead）——Run 层
//     据此判定「本趟 0 队列可处理」的空转形态
//   - err: 错误信息
func (m *Manager) Pass(ctx context.Context) (purged int, processable int, err error) {
	idxDeleted := 0
	m.passIdxScanned.Store(0) // 稳态路径不扫 keyidx；任何扫描都会经 scanKeyIdx 计入
	defer func() { m.passIdxDeleted.Store(int64(idxDeleted)) }()
	now := time.Now().UnixMilli()
	for _, tc := range m.mt.Topics() {
		cutoff := now - tc.EffectiveRetention().Milliseconds()
		for q := uint32(0); q < tc.Queues; q++ {
			n, ni, led, perr := m.purgeQueue(ctx, tc.Name, q, cutoff)
			if perr != nil {
				return purged, processable, fmt.Errorf("清理 %s/q%d: %w", tc.Name, q, perr)
			}
			purged += n
			idxDeleted += ni
			if led {
				processable++
			}
		}
	}
	return purged, processable, nil
}

// purgeQueue 找到 [队首, 首条未过期) 边界，DeleteRange 整段删除，并在同一批次
// 点删这些消息的全部 key 索引。
// 队列内消息按 offset 追加写入、StoreAtMs 单调不减，扫到首条未过期即可停。
// 注：单调性假设可能被时钟回跳（NTP 校时）短暂打破——被回跳越过停止边界的
// 过期消息会在后续趟次被清掉，只有延迟、不丢消息。
//
// 索引键由被删消息自身拼出：KeyIdxKey(topic, key, msg.StoreAtMs, q, msg.Offset)，
// 与 produce 写侧（消息与索引同批写入）同源同参，因此删侧不需要扫描 keyidx。
// 单批规模双上限：最多 maxPurgePerQueue 条消息；索引键字节累计超
// maxIdxDeleteBytes 即在消息边界截停（首条消息自身超预算也纳入，保证进展）——
// 消息与其索引永不拆到两个批次，否则两批之间崩溃就是孤儿索引。
//
// 返回（清理消息条数, 删除索引条数, 是否本节点可处理（该队列组由本节点 lead）, 错误）：
// 可处理标志供 Pass 统计 processable——非 leader 队列跳过是摊布语义
// 的常态（各组 leader 各扫各的），不算错误。
//
// 集群档只清本节点 leader 的队列：各组 leader 各扫各的（摊布语义见 Task 8
// 说明），非 leader 队列跳过——清理批次归该队列组，非 leader 提交会报
// ErrNotLeader，跳过优于报错重试。
func (m *Manager) purgeQueue(ctx context.Context, topic string, q uint32, cutoff int64) (int, int, bool, error) {
	g := m.rt.GroupForQueue(topic, q)
	if !m.rt.IsLeader(g) {
		return 0, 0, false, nil
	}
	pfx := store.MsgQueuePrefix(topic, q)
	// 索引点删在扫描回调内写进批次：批次未提交前不影响迭代器（与读写交错无关）
	b := m.st.NewBatch()
	var boundary uint64
	found, idxN, idxBytes := 0, 0, 0
	budgetHit := false
	err := m.st.Scan(pfx, store.PrefixUpperBound(pfx), maxPurgePerQueue, func(k, v []byte) (bool, error) {
		msg, err := core.DecodeMessage(v)
		if err != nil {
			return false, fmt.Errorf("解码 %q: %w", k, err)
		}
		if msg.StoreAtMs >= cutoff {
			return false, nil
		}
		var idxKeys [][]byte
		sz := 0
		for _, key := range msg.Keys {
			if key == "" {
				continue // 写侧同样跳过空 key（produce.go）
			}
			ik := store.KeyIdxKey(topic, key, msg.StoreAtMs, q, msg.Offset)
			idxKeys = append(idxKeys, ik)
			sz += len(ik)
		}
		if found > 0 && idxBytes+sz > maxIdxDeleteBytes {
			budgetHit = true
			return false, nil // 消息边界截停：本条连同其索引留待下趟
		}
		for _, ik := range idxKeys {
			if err := b.Delete(ik); err != nil {
				return false, fmt.Errorf("索引删除入批 %q: %w", ik, err)
			}
		}
		idxN += len(idxKeys)
		idxBytes += sz
		boundary = msg.Offset + 1
		found++
		return true, nil
	})
	if err != nil {
		b.Close()
		return 0, 0, true, err
	}
	if found == 0 {
		b.Close()
		return 0, 0, true, nil
	}
	// DeleteRange 从 offset 0 起：此前趟次已删的区间为空集，重复覆盖无害
	if err := b.DeleteRange(store.MsgKey(topic, q, 0), store.MsgKey(topic, q, boundary)); err != nil {
		b.Close()
		return 0, 0, true, fmt.Errorf("DeleteRange 入批: %w", err)
	}
	if err := m.rep.Apply(ctx, g, b); err != nil {
		// 失败批次按 store 契约丢给 GC，不 Close（见 store.Apply 注释）
		return 0, 0, true, fmt.Errorf("消息与索引删除提交: %w", err)
	}
	if found == maxPurgePerQueue || budgetHit {
		m.logger.Info("retention 达单趟上限，剩余留待下趟", "topic", topic, "queue", q,
			"purged", found, "purged_idx", idxN, "idx_bytes", idxBytes, "idx_budget_hit", budgetHit)
	}
	m.logger.Debug("retention 清理队列", "topic", topic, "queue", q, "purged", found,
		"purged_idx", idxN, "boundary", boundary)
	return found, idxN, true, nil
}
