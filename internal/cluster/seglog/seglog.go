// seglog.go 提供单个 raft 组的分段追加日志实现：段扫描恢复、追加、
// fsync、关闭与按 index 的随机读（卡 SQ4.10 冷区）。
//
// 职责：
//   - Open 时按序扫描全部段文件，按「后写的赢」重建 HardState 与读索引
//     （连续段 run + 检查点 + 任期变化点），不在内存里累积条目本体
//   - Append 把一轮 Ready 的 HardState/Entries 编码为帧写入活动段，并以
//     与 Open 扫描同一个函数（noteEntry）维护读索引
//   - ReadEntries/Term：按 index 区间读回条目、按 index 查任期（raft 冷区
//     读路径，调用方是 cluster.groupStorage）
//   - 处理掉电导致的 torn tail（仅末段合法）与非末段真损坏（拒绝启动）
//
// 边界：
//   - 盘上格式（帧、段命名）不因读路径改变：读索引全部是内存派生结构，
//     重启由 Open 扫描重建
//   - 读路径只保证「已写入文件的条目」可读；条目是否已提交、是否会被换届
//     改写由调用方保证（cluster 侧不变式 I-cold：冷区只读 index ≤ applied
//     的条目，已提交条目永不被改写）
//   - 锁纪律（与 SQ4.4 锁外刷盘一致）：读路径锁内只取索引快照与段的已写
//     长度，打开文件与 ReadAt 全在锁外，用独立只读句柄——不与 Append 争锁
//     做 I/O，轮转关闭写句柄也不影响读句柄
package seglog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"
)

// Log 一组的分段追加日志。并发安全：Append（组 run goroutine）、
// Sync（flusher goroutine）、TruncateTo（截断 goroutine，Task 4）来自不同
// goroutine，内部单互斥保护全部字段。Append/TruncateTo/Close 全程持锁；
// Sync 只在锁内取活动段句柄，落盘系统调用在锁外执行——否则每 200ms 一次
// 的刷盘会让组 raft 主循环（内联 Append）停顿一次刷盘时长。锁外落盘与
// 轮转/Close 的并发正确性见 Sync 注释。
type Log struct {
	mu        sync.Mutex
	dir       string
	lg        *slog.Logger
	active    *os.File // 当前活动段（始终打开，Append 的写入目标）
	activeSeq uint64   // 活动段序号
	// activeSize 活动段已写入的有效字节数。两个用途：
	//   1. 轮转判定（>= SegMaxBytes 触发）
	//   2. 下一次写入的偏移（WriteAt 的第二参数）
	// 预分配后文件的物理大小恒为 SegMaxBytes，与本字段不再相等——凡是
	// 需要「写了多少」的地方一律用本字段，绝不用 Stat().Size()。
	activeSize int64
	lastIndex  uint64            // 日志尾 index；0 = 空日志
	lastHS     *raftpb.HardState // 最新已写 HardState（轮转时补写进新段首条）
	// segMax 已关闭段号 → 该段的段内最大 entry index（回收判定：
	// TruncateTo(upto) 删除 max<=upto 的段；无 entry 只有 HS 的段值为 0）。
	// 只有在一个段被判定为「完全可回收资格」的那一刻才会写入这个 map，写
	// 入之后不再修改——若关闭后发生跨段冲突重写（新领导者的写入起点落在
	// 这个已关闭段覆盖的 index 范围内，但物理条目已经写到更晚的段），这里
	// 记录的值会偏高（stale-high），但绝不会偏低。TruncateTo 用 max<=upto
	// 判定可删，偏高只会让该段回收得更晚，不会在段内仍有活条目时误删——
	// 见 TruncateTo 函数注释。
	segMax map[uint64]uint64
	buf    []byte // Append 的帧组装缓冲（复用减分配）
	// prealloc 当前活动段是否已预分配到 SegMaxBytes。
	//
	// 它是 datasync 的使用前提，不是性能开关：文件大小仍在增长时
	// fdatasync 不保证「文件长了」这件事落盘，掉电后已写入的尾部字节
	// 读不回来。非 Linux 平台、以及 fallocate 返回 ENOTSUP 的文件系统上
	// 恒为 false，落盘全程走完整 fsync，行为与本特性落地之前一致。
	prealloc bool
	// 读索引（卡 SQ4.10）：runs/terms/segBytes 由 noteEntry（Open 扫描与
	// Append 共用）、maybeRotate、TruncateTo 维护，全部受 mu 保护。
	//
	// runs 按 first 升序、区间互不重叠：一个 run = 同一段文件内一串 index
	// 连续的有效条目。换届改写（新条目 index ≤ 当前尾）把 ≥ 该 index 的旧
	// run 丢弃或裁短——与 Open 重放的「后写的赢」同一规则、同一函数。
	runs []run
	// terms 任期变化点（index 升序）：某 index 的任期 = 最后一个起点 ≤ 它
	// 的变化点的任期。只对 runs 覆盖的 index 有意义（查询先核覆盖）。
	terms []termPoint
	// segBytes 已关闭段的逻辑字节数（读上界与按字节保留的计算输入）；
	// 活动段的读上界是 activeSize。
	segBytes map[uint64]int64
	offs     []int64 // Append 的逐条帧偏移暂存（复用减分配）
	// syncHook 测试专用注入钩子（生产恒为 nil）：Sync 释放 l.mu 之后、
	// 发起落盘系统调用之前调用一次，让同包测试把 Sync 停在「已取句柄、
	// 未落盘」这一点，确定性地制造与 Append/轮转/Close 的并发交错。
	// 不导出、无生产分支逻辑；先例见 deliver.afterAppendHook。
	syncHook func()
}

// SegMaxBytes 单段字节数上限，Append 后据此判定是否轮转到下一段。
//
// 声明为包内变量而非常量：测试需要把它调小（如 64 字节），让小数据量的
// 用例也能稳定触发跨段场景（轮转、HS 补写、TruncateTo）；生产环境不修改，
// 保持默认的 64MiB。
var SegMaxBytes int64 = 64 << 20

// segName 段文件名：8 位十进制序号，字典序 = 数值序。
func segName(seq uint64) string { return fmt.Sprintf("%08d.seg", seq) }

// segSuffix 段文件名后缀：完整格式为 8 位数字 + segSuffix。
const segSuffix = ".seg"

// syncDir fsync 一个目录，把「目录项本身」刷到盘上。
//
// 为什么需要它（POSIX 语义，不是保险起见）：fsync(file) 只保证文件的
// **内容**落盘，不保证「这个文件名出现在它的父目录里」这件事落盘。
// 掉电后完全可能出现「段文件的数据块都在，但目录里查不到这个名字」——
// 对我们来说等价于整个日志凭空消失。因此每创建一个新的段文件（或新的
// 段目录），都必须补一次父目录的 fsync。Pebble 建 WAL/SST、etcd 建 WAL
// 段都是同款做法。
//
// 删除方向不需要这道保险：目录项没落盘只意味着文件"复活"，多出来的旧
// 段会被下次 Open 正常扫描或再次回收，不丢数据（见 TruncateTo）。
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("seglog: 打开目录 %s 以 fsync 失败: %w", path, err)
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return fmt.Errorf("seglog: fsync 目录 %s 失败: %w", path, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("seglog: 关闭目录句柄 %s 失败: %w", path, err)
	}
	return nil
}

// mkdirAllSync 等价于 os.MkdirAll，但每新建一级目录都会 fsync 它的父
// 目录，让新目录的目录项本身也具备掉电存活能力。
//
// 逐级递归而不是"建完再 sync 一次"：MkdirAll 可能一口气创建 raftlog/
// 与 raftlog/<g> 两级，只 sync 最后一级的话，raftlog 这个名字在数据
// 目录里仍可能是未落盘的——掉电后连同它下面所有段文件一起消失。
func mkdirAllSync(dir string) error {
	if fi, err := os.Stat(dir); err == nil {
		if !fi.IsDir() {
			return fmt.Errorf("seglog: %s 已存在且不是目录", dir)
		}
		return nil // 已存在：目录项此前必已由创建者 sync 过，无需重复
	}
	parent := filepath.Dir(dir)
	if parent != dir { // 递归终止于根目录（Dir("/") == "/"）
		if err := mkdirAllSync(parent); err != nil {
			return err
		}
	}
	if err := os.Mkdir(dir, 0o755); err != nil && !os.IsExist(err) {
		return fmt.Errorf("seglog: 创建目录 %s 失败: %w", dir, err)
	}
	return syncDir(parent)
}

// Open 打开（或创建）dir 下的组日志：按序扫描全部段、恢复日志状态、
// 打开末段续写。
//
// 返回：
//   - Log: 就绪的日志（尾部定位在最后一条好帧之后）；「后写的赢」重放
//     后的条目经 ReadEntries/Term 按需读，FirstIndex/LastIndex 给出范围
//   - hs: 恢复出的最新 HardState；从未写过时为 nil
//   - err: 非末段坏帧（真损坏，非 torn write）或 I/O 错误
//
// 内存：扫描逐帧解码后只留读索引（每段 / 每次改写 / 每约 1MiB 一项），
// 不留条目本体——内存不随日志保留量线性增长（卡 SQ4.10 S3）。
//
// 注意：末段的 torn tail（掉电正常形态）在此被物理截断到好帧边界后
// 继续——绝不静默保留坏字节，否则续写会接在坏帧后面永远读不回。
func Open(dir string, lg *slog.Logger) (*Log, *raftpb.HardState, error) {
	if err := mkdirAllSync(dir); err != nil {
		return nil, nil, err
	}

	seqs, err := scanSegSeqs(dir)
	if err != nil {
		return nil, nil, err
	}

	// idx 承载扫描期的读索引：与运行期同一个 noteEntry 维护（Open 重建的
	// 索引与运行期逐字一致，见 noteEntry 注释）
	idx := &Log{segBytes: make(map[uint64]int64)}
	var (
		hs        *raftpb.HardState
		scanned   int // 扫描解码的 entry 帧数（含被改写的旧版本），仅用于日志
		lastIndex uint64
		segMax    = make(map[uint64]uint64)
		tornInfo  string // 非空表示发生了 torn 截断，写入 Info 日志用

		// activeLogicalEnd 活动段（= 最后一段）扫描结束时的偏移，即已写入的
		// 有效字节数。它是 activeSize 的唯一来源。
		//
		// 为什么不用 f.Stat().Size()：现状下两者恰好相等（扫描遇到坏帧会
		// 物理截断到好帧边界），但那让「轮转判定的输入」依赖「扫描一定会
		// 物理截断零尾」这个不写在任何地方的副作用。预分配落地后物理大小
		// 恒为 SegMaxBytes，这条隐式依赖一旦被后人改动截断策略就会静默
		// 踩塌轮转。显式取扫描结果，依赖关系从隐含变成明写。
		activeLogicalEnd int64
	)

	for i, seq := range seqs {
		isLast := i == len(seqs)-1
		name := segName(seq)
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, fmt.Errorf("seglog: 读段 %s 失败: %w", name, err)
		}

		if !isLast {
			// 非末段=重启后一定是「已关闭」段，先占好 segMax 的位，哪怕它
			// 一条 entry 都没有（纯 HS 段）也要有个 0 值。不这样做的话，
			// 只有下面 recEntry 分支才会写 segMax，纯 HS 段就永远不会出现
			// 在这个 map 里——TruncateTo 遍历 segMax 时根本看不到它，变成
			// 重启后再也回收不掉的段。default 值 0 与 TruncateTo(upto>=0)
			// 天然可回收的约定一致；下面扫到 entry 帧时会覆盖成真实的
			// 段内最大 index。
			segMax[seq] = 0
		}

		off := 0
	scan: // 供内层 switch-case 里的 break 精确跳出本段扫描（plain break 在
		// switch 内只会跳出 switch，需要带标签才能跳出这层 for）
		for off < len(data) {
			typ, payload, n, ferr := readFrame(data[off:])
			if ferr != nil {
				if !isLast {
					// 非末段出现坏帧：真损坏（写入方 bug 或位腐），不是掉电的
					// 正常形态——掉电只可能截断当时的活动段（即最后一段）。
					// 拒绝启动比静默丢数据安全。
					return nil, nil, fmt.Errorf(
						"seglog: 段 %s 偏移 %d 帧损坏且非末段——真损坏，拒绝启动", name, off)
				}
				// 末段坏帧：两种成因，必须分开记。
				//   - 全零：预分配段尾的零填充，每次干净重启的正常形态
				//   - 非全零：掉电时最后一次 write() 只落盘一部分，真 torn write
				// 不分开的话，Warn 会被干净重启淹没成噪声，真事故里就没人信它了。
				//
				// 两种成因的**处理动作完全一致**：物理截断到好帧边界。绝不
				// 静默保留坏字节——续写虽然按 activeSize 偏移覆盖写，但残留
				// 在逻辑末尾之后的字节会让下次扫描再次撞上同一分支。
				discarded := len(data) - off
				zeroTail := allZero(data[off:])
				if err := os.Truncate(path, int64(off)); err != nil {
					return nil, nil, fmt.Errorf("seglog: 截断段 %s 到偏移 %d 失败: %w", name, off, err)
				}
				if zeroTail {
					lg.Debug("seglog: 预分配零尾，截断到逻辑末尾",
						"segment", name, "goodOffset", off, "zeroBytes", discarded)
				} else {
					lg.Warn("seglog: 检测到 torn tail，已截断到好帧边界",
						"segment", name, "goodOffset", off, "discardedBytes", discarded)
					tornInfo = name
				}
				break
			}

			switch typ {
			case recEntry:
				var e raftpb.Entry
				if uerr := proto.Unmarshal(payload, &e); uerr != nil {
					if !isLast {
						return nil, nil, fmt.Errorf(
							"seglog: 段 %s 偏移 %d entry 解码失败且非末段——真损坏，拒绝启动: %w", name, off, uerr)
					}
					// CRC 校验通过但内容解码失败：写入方 bug 或位腐，按 torn
					// 处理（末段才容忍），截断丢弃。
					discarded := len(data) - off
					if err := os.Truncate(path, int64(off)); err != nil {
						return nil, nil, fmt.Errorf("seglog: 截断段 %s 到偏移 %d 失败: %w", name, off, err)
					}
					lg.Warn("seglog: 末段 entry 解码失败，按 torn 截断到好帧边界",
						"segment", name, "goodOffset", off, "discardedBytes", discarded)
					tornInfo = name
					break scan // 文件已物理截断到 off，不能再按 len(data) 继续扫描
				}
				ei := e.GetIndex()
				// 后写的赢：新条目 index <= 当前已知尾，说明发生了换届回退
				// 重写，[ei, 旧尾] 区间的旧条目已失效——noteEntry 裁掉它们的
				// 读索引后再登记新条目（与 Append 同一函数）。
				idx.noteEntry(seq, int64(off), ei, e.GetTerm())
				scanned++
				lastIndex = ei
				// 段内最大 entry index：随着本段扫描推进单调覆盖写入，最终
				// 落在「本段物理上最后一条 entry 帧」的 index。若本段内部
				// 也发生过换届冲突重写（同段内先写 7..10 又写 7'），最终值
				// 就是冲突后幸存的那条（7），天然与 noteEntry 的读索引裁剪结果一致，
				// 不会偏高。跨段冲突（重写发生在更晚的段）不会回头修正本段
				// 已经落定的值——那种情况下这里会偏高，见 segMax 字段注释。
				segMax[seq] = ei
			case recHardState:
				var h raftpb.HardState
				if uerr := proto.Unmarshal(payload, &h); uerr != nil {
					if !isLast {
						return nil, nil, fmt.Errorf(
							"seglog: 段 %s 偏移 %d hardstate 解码失败且非末段——真损坏，拒绝启动: %w", name, off, uerr)
					}
					discarded := len(data) - off
					if err := os.Truncate(path, int64(off)); err != nil {
						return nil, nil, fmt.Errorf("seglog: 截断段 %s 到偏移 %d 失败: %w", name, off, err)
					}
					lg.Warn("seglog: 末段 hardstate 解码失败，按 torn 截断到好帧边界",
						"segment", name, "goodOffset", off, "discardedBytes", discarded)
					tornInfo = name
					break scan // 文件已物理截断到 off，不能再按 len(data) 继续扫描
				}
				// 后写的赢：同一轮或跨轮写入的 HardState 以最后一条为准。
				hs = &h
			}
			off += n
		}
		if isLast {
			activeLogicalEnd = int64(off)
		} else {
			idx.segBytes[seq] = int64(off)
		}
	}

	// 确定活动段：无任何段时创建 00000001.seg；否则续用最后一段。
	var activeSeq uint64 = 1
	if len(seqs) > 0 {
		activeSeq = seqs[len(seqs)-1]
	}
	activePath := filepath.Join(dir, segName(activeSeq))
	// len(seqs)==0 ⇒ 这一行 O_CREATE 会**新建**首段文件；否则只是续用已
	// 存在的末段，不产生新目录项。
	createdFirstSeg := len(seqs) == 0
	// 不带 O_APPEND：预分配后 EOF 就是 SegMaxBytes，O_APPEND 会把每次写
	// 都定位到段尾。写入位置改由 activeSize 显式给出（WriteAt），两个
	// 平台走同一条写路径——未预分配时 WriteAt(activeSize) 与顺序追加等价。
	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("seglog: 打开活动段 %s 失败: %w", segName(activeSeq), err)
	}
	if createdFirstSeg {
		// 首段的目录项必须在这里就落盘，不能等到有人 Append+fsync 文件内容。
		// 承重场景是一次性迁移（raftstore.migrateLog）：③ Persist→Append
		// (sync=true) 只 fsync 了段文件本身，④ 随后用 Pebble Sync 批次删掉
		// legacy 键。若首段的目录项还没落盘就在这个窗口掉电，重启后 legacy
		// 键已经没了、段文件也查不到——整组 raft 日志永久丢失。migrateLog
		// 走的正是 getLog → 本函数这条链，所以这道 fsync 必须在 Open 里，
		// 而不是留给调用方补。
		if err := syncDir(dir); err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	// 活动段自身不计入 segMax（segMax 只记「已关闭」段），delete 掉活动段号
	// 若上面扫描时误记（例如活动段就是唯一段）。
	delete(segMax, activeSeq)

	l := &Log{
		dir:        dir,
		lg:         lg,
		active:     f,
		activeSeq:  activeSeq,
		activeSize: activeLogicalEnd,
		lastIndex:  lastIndex,
		lastHS:     hs,
		segMax:     segMax,
		runs:       idx.runs,
		terms:      idx.terms,
		segBytes:   idx.segBytes,
	}

	// 活动段补分配。必须在 l 构造完成、activeSize 已定之后——顺序写反
	// 会让 activeSize 取到预分配后的物理大小（SegMaxBytes），重启即触发
	// 轮转（spec §2.3）。
	//
	// 重启后段文件的物理大小已被扫描截回逻辑末尾（零尾走 torn tail 分支
	// 物理截断），这里重新扩回 SegMaxBytes，让重启后的段重新获得「写入
	// 不改变文件大小」这个前提。
	if err := l.preallocActive(); err != nil {
		f.Close()
		return nil, nil, err
	}

	commit := uint64(0)
	if hs != nil {
		commit = hs.GetCommit()
	}
	lg.Info("seglog: 打开完成",
		"segments", len(seqs), "entries", scanned, "runs", len(l.runs), "firstIndex", l.firstIndexLocked(), "lastIndex", lastIndex,
		"hsCommit", commit, "tornTruncated", tornInfo != "", "tornSegment", tornInfo,
		"prealloc", l.prealloc)

	return l, hs, nil
}

// scanSegSeqs 列出 dir 下全部 *.seg 文件的序号，按数值序升序返回。
func scanSegSeqs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("seglog: 读目录 %s 失败: %w", dir, err)
	}
	var seqs []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, segSuffix) {
			continue
		}
		numPart := strings.TrimSuffix(name, segSuffix)
		seq, perr := strconv.ParseUint(numPart, 10, 64)
		if perr != nil {
			continue // 非本包命名格式的文件，忽略
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

// allZero 判定切片是否全零。
//
// 用途：区分「预分配段尾的零填充」与「掉电导致的真 torn write」。两者都
// 让 readFrame 报坏帧，但前者是每次干净重启的正常形态、后者是需要告警的
// 异常——不分开记，Warn 就会被干净重启淹没成噪声。
//
// 全零并不能百分之百证明是预分配（真 torn write 也可能恰好落在一段零
// 扇区上），但那种情况按「零尾」处理同样安全：截断行为完全一致，只是
// 少一条告警。反方向才危险（把真损坏当零尾静默掉），而非零字节一定
// 走告警分支，那个方向不会误判。
func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// Append 追加一轮 Ready 的 HardState 与条目。
//
// 参数：
//   - hs: 非空时先写一条 hardstate 记录（帧序保证扫描时 HS 不晚于同轮条目）
//   - ents: 逐条写 entry 记录；空切片合法（仅 HS 轮）
//   - sync: true 时写完 fsync（quorum-fsync 档 MustSync 轮）；false 时
//     只 write() 进内核页缓存（mem 档持久性等位，进程 crash 不丢）
//
// 失败即返回错误，调用方（raftStore.Persist → group.appendOnce）按
// fail-stop 处理；本层不重试——写失败后文件偏移状态不可信。
func (l *Log) Append(hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active == nil {
		return fmt.Errorf("seglog: 已关闭，拒绝 Append")
	}

	// 帧组装走 appendFrameMarshal：protobuf 序列化经 MarshalAppend 直接
	// 续写进 l.buf，省掉每条记录「Marshal 到临时切片再整拷进 buf」的一次
	// 分配 + 一次拷贝；盘上帧格式与 appendFrame 逐字节相同（见
	// appendFrameMarshal 注释与对照测试），Open 扫描回读不受影响。
	l.buf = l.buf[:0]
	l.offs = l.offs[:0]
	var err error
	if hs != nil {
		if l.buf, err = appendFrameMarshal(l.buf, recHardState, hs); err != nil {
			return fmt.Errorf("seglog: 编码 hardstate 失败: %w", err)
		}
	}
	for _, e := range ents {
		// 帧在段内的起始偏移：写入成功后登记进读索引（检查点要用）
		l.offs = append(l.offs, l.activeSize+int64(len(l.buf)))
		if l.buf, err = appendFrameMarshal(l.buf, recEntry, e); err != nil {
			return fmt.Errorf("seglog: 编码 entry(index=%d) 失败: %w", e.GetIndex(), err)
		}
	}

	if len(l.buf) > 0 {
		n, err := l.active.WriteAt(l.buf, l.activeSize)
		if err != nil {
			return fmt.Errorf("seglog: 写活动段 %s 偏移 %d 失败: %w", segName(l.activeSeq), l.activeSize, err)
		}
		l.activeSize += int64(n)
	}

	// 只有 Write 成功之后才更新内存态：lastHS/lastIndex 必须与「已经落进
	// 文件」的内容保持一致。若上面提前 return 了错误，这两个字段维持失败
	// 前的旧值——但按 fail-stop 契约，Append 一旦返回错误，调用方就不应
	// 该再信任、继续复用这个 Log 实例（本层不重试，文件写入偏移状态已经
	// 不可信），该终止就终止、要恢复就走重新 Open 扫描，而不是指望这里的
	// 字段还准确。
	if hs != nil {
		l.lastHS = hs
	}
	if len(ents) > 0 {
		l.lastIndex = ents[len(ents)-1].GetIndex()
	}
	// 读索引与文件内容同步推进（同样只在写成功之后）：noteEntry 与 Open
	// 扫描是同一个函数，运行期索引与重启重建的索引一致
	for i, e := range ents {
		l.noteEntry(l.activeSeq, l.offs[i], e.GetIndex(), e.GetTerm())
	}

	if sync {
		if err := l.syncActive(); err != nil {
			return err
		}
	}

	return l.maybeRotate()
}

// maybeRotate 在活动段达到 SegMaxBytes 阈值时轮转到下一段。调用方须持有
// l.mu（本方法只在 Append 尾部被调用一次，不单独加锁）。
//
// 轮转步骤：旧段 fsync → 关闭旧段 → 创建新段 → 若存在最新 HardState，立即
// 补写一份到新段首条 → 全部成功后，最后一步才把旧段的段内最大 entry index
// 登记进 segMax（授予回收资格）。任何一步失败都提前 return、不登记
// segMax——旧段在新段真正就绪之前必须保持「不可回收」，见函数末尾注释。
func (l *Log) maybeRotate() error {
	if l.activeSize < SegMaxBytes {
		return nil
	}

	oldSeq := l.activeSeq
	oldBytes := l.activeSize
	// 旧段关闭前实际写入过的最后一条 entry index，供轮转彻底成功后登记
	// 进 segMax（见函数末尾）。这里只是先取值存起来，不在此处写 map。
	oldMaxIndex := l.lastIndex

	// 关段前把物理大小截回逻辑大小：已关闭段绝不能带预分配零尾——Open
	// 扫描对非末段坏帧的判定是「真损坏，拒绝启动」，零尾会让每一次重启
	// 都撞上它，整组日志永久打不开。
	//
	// 未预分配时物理大小本就等于 oldBytes，Truncate 是空操作，但仍然
	// 无条件执行：让两个平台走同一条代码路径，别留「只在 Linux 上执行
	// 的那几行」这种只在生产环境才跑到的分支。
	if err := l.active.Truncate(oldBytes); err != nil {
		return fmt.Errorf("seglog: 轮转前截断旧段 %s 到 %d 字节失败: %w",
			segName(oldSeq), oldBytes, err)
	}
	// 轮转屏障：旧段必须先 fsync 落盘、再关闭，然后才允许开新段。这样
	// 一来，一旦发生掉电，可能出现 torn tail（部分帧未落盘）的段永远
	// 只会是「当前活动段」，也就是全部段里的最后一段——Open 扫描时「非
	// 末段坏帧 = 真损坏，拒绝启动」的判定前提才成立。如果不设这道屏障，
	// 旧段可能带着未 fsync 的尾部就被认定「已关闭」，重启后一扫描就会
	// 命中「非末段损坏」直接拒绝启动。
	//
	// 这里必须是完整 fsync，不能走 syncActive：上一行的 Truncate 刚改过
	// 文件大小，fdatasync 不保证 inode 元数据落盘。
	if err := l.active.Sync(); err != nil {
		return fmt.Errorf("seglog: 轮转前 fsync 旧段 %s 失败: %w", segName(oldSeq), err)
	}
	if err := l.active.Close(); err != nil {
		return fmt.Errorf("seglog: 轮转关闭旧段 %s 失败: %w", segName(oldSeq), err)
	}
	// 旧段的读上界从此固定为逻辑字节数（读路径用它限定 ReadAt 范围）
	l.segBytes[oldSeq] = oldBytes
	// 旧段已经物理关闭：先置空，避免 Close 成功但下面开新段失败时，
	// l.active 仍指着一个已关闭的文件句柄——那样后续 Append/Sync 走到
	// Write/Sync 会得到一个含糊的「文件已关闭」I/O 错误，不如让「已关闭，
	// 拒绝 Append」这个明确的 fail-stop 信号直接生效。
	l.active = nil

	newSeq := oldSeq + 1
	newPath := filepath.Join(l.dir, segName(newSeq))
	// 不带 O_APPEND：预分配后 EOF 就是 SegMaxBytes，O_APPEND 会把每次写
	// 都定位到段尾。写入位置改由 activeSize 显式给出（WriteAt），两个
	// 平台走同一条写路径——未预分配时 WriteAt(activeSize) 与顺序追加等价。
	f, err := os.OpenFile(newPath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("seglog: 轮转创建新段 %s 失败: %w", segName(newSeq), err)
	}
	// 新段的目录项立刻落盘，且必须早于本函数末尾授予旧段回收资格那一步：
	// 一旦旧段进了 segMax，TruncateTo 随时可以把它删掉，届时新段就是这组
	// 日志的唯一载体。若新段的名字此刻还没进目录，掉电后会出现「旧段被
	// 删、新段查不到」——永久数据丢失。fsync(file) 保证不了目录项，
	// 见 syncDir 的注释。
	if err := syncDir(l.dir); err != nil {
		f.Close()
		return err
	}
	l.active = f
	l.activeSeq = newSeq
	l.activeSize = 0

	// 新段预分配。放在 activeSize 归零之后、HS 补写之前：补写要用
	// WriteAt(l.activeSize)，此刻偏移必须已经是 0。
	if err := l.preallocActive(); err != nil {
		return err
	}

	// HS 补写：把最新 HardState 立即写入新段首条帧。原因是 TruncateTo
	// 按段整段删除，如果最新 HS 只存在于某个旧段里，那个旧段一旦被回收，
	// 重启就再也读不到 HardState 了。让每次轮转都把「当时已知的最新 HS」
	// 带一份到新段，保证「最新 HS 一定在最新（未回收）的段里」这个不变式
	// 始终成立，回收多老的旧段都不影响 HS 的可恢复性。
	if l.lastHS != nil {
		payload, merr := proto.Marshal(l.lastHS)
		if merr != nil {
			return fmt.Errorf("seglog: 轮转补写 hardstate 编码失败: %w", merr)
		}
		frame := appendFrame(nil, recHardState, payload)
		n, werr := l.active.WriteAt(frame, l.activeSize)
		if werr != nil {
			return fmt.Errorf("seglog: 轮转补写 hardstate 写入新段 %s 偏移 %d 失败: %w",
				segName(newSeq), l.activeSize, werr)
		}
		l.activeSize += int64(n)
		// 补写帧必须立即落盘，不能等下一次批量刷盘：本函数末尾一旦把旧段
		// 登记进 segMax，TruncateTo 随时可能删掉旧段——若此刻补写帧还悬在
		// 页缓存里，「删旧段 + 掉电」的窗口会同时失去新旧两份 HS，重启后
		// HardState 归零（term/vote 丢失，违反 raft 持久化契约）。普通条目
		// 帧走 mem 档 NoSync 是因为旧副本还在；HS 补写帧是回收授权的前置，
		// 持久性要求跟着回收动作走，不跟确认档走。
		// 走 syncActive：此刻新段刚预分配完、写入未改变文件大小。
		if serr := l.syncActive(); serr != nil {
			return fmt.Errorf("seglog: 轮转补写 hardstate 落盘新段 %s 失败: %w", segName(newSeq), serr)
		}
	}

	// 回收资格必须在新段完全就绪之后才授予，放在整个轮转流程的最后一步：
	// 旧段已经 fsync+关闭、新段文件已经创建成功、（如果有）HS 补写也已经
	// 写入成功——新段这时才真正具备独立承接后续写入、独立支撑重启恢复的
	// 能力。如果中途任何一步失败（尤其是 os.OpenFile 创建新段失败，比如
	// ENOSPC），必须提前 return 且不写这个 map：旧段此刻仍是唯一持有已
	// fsync 数据的文件，若过早把它标记为「可回收」，独立运行的 TruncateTo
	// goroutine 就可能把它删掉——而这时新段还没建成，进程重启会发现日志
	// 整个丢失，是永久数据丢失而不是可恢复的 fail-stop。
	l.segMax[oldSeq] = oldMaxIndex

	l.lg.Info("seglog: 段轮转完成",
		"oldSegment", oldSeq, "oldSegmentBytes", oldBytes, "newSegment", newSeq,
		"truncatedTo", oldBytes)
	return nil
}

// preallocActive 把当前活动段预分配到 SegMaxBytes，并刷新 prealloc 标志。
//
// 调用方须持有 l.mu。调用时机有两处：Open 打开活动段之后（且必须在
// activeSize 定下来之后——顺序写反会让 activeSize 变成 SegMaxBytes，
// 重启即触发轮转），以及 maybeRotate 创建新段之后。
//
// 失败语义：文件系统不支持时 prealloc=false、返回 nil，落盘退回 fsync，
// 功能不受影响——预分配是纯性能优化，不是正确性前提。只有真 I/O 错误
// （ENOSPC 等）才上抛。
func (l *Log) preallocActive() error {
	ok, err := preallocate(l.active, SegMaxBytes)
	if err != nil {
		return err
	}
	l.prealloc = ok
	if !ok {
		// 降级是低频且影响性能画像的事实，必须留痕：否则线上看到落盘慢
		// 于预期时，无从判断是盘慢还是根本没走上预分配这条路。
		l.lg.Info("seglog: 预分配未生效，落盘退回完整 fsync",
			"dir", l.dir, "segment", segName(l.activeSeq))
	}
	return nil
}

// syncActive 把活动段落盘。调用方须持有 l.mu。
//
// 已预分配时用 datasync（不同步 inode 元数据，实测单次 1.82ms → 0.61ms）；
// 未预分配时文件大小仍在增长，必须用完整 fsync。
//
// 注意：轮转屏障那次落盘**不能**走本方法——它紧跟在 Truncate 之后，
// 那一刻文件大小刚变，必须完整 fsync（见 maybeRotate）。
func (l *Log) syncActive() error {
	return syncSegment(l.active, l.activeSeq, l.prealloc)
}

// syncSegment 把给定段句柄落盘，不碰 Log 的任何字段，可在锁外调用。
//
// 参数：
//   - f: 段文件句柄
//   - seq: 段号，仅用于错误信息
//   - prealloc: 该段是否已预分配；true 走 datasync，false 走完整 fsync
//
// 返回：句柄已被关闭时错误满足 errors.Is(err, os.ErrClosed)（两条路径
// 统一），调用方据此区分「被并发关闭」与真 I/O 错误；其余错误原样包装上抛。
func syncSegment(f *os.File, seq uint64, prealloc bool) error {
	if prealloc {
		return datasync(f)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("seglog: fsync 活动段 %s 失败: %w", segName(seq), err)
	}
	return nil
}

// Sync 刷活动段到盘（mem 档 200ms flusher 的批量刷盘入口）。
//
// 覆盖语义：返回 nil 即本次调用之前的全部写入已落盘。
//
// 锁内只取「活动段句柄 + 段号 + prealloc 标志」，落盘系统调用在锁外执行，
// 期间 Append（组 raft 主循环内联调用）不被阻塞。调用前的写入都落在取到
// 的这一段内（越过段长的写入会在同一次 Append 里轮转），故对该句柄落盘
// 即覆盖它们；锁外期间新到的写入不在本次覆盖范围内。
//
// 句柄在落盘前被并发关闭时回锁复核：
//   - activeSeq 已前进 = 被轮转关闭：轮转屏障已对旧段 Truncate+完整
//     fsync，覆盖本次调用前的全部写入，返回 nil；
//   - 仍是同一段 = 被 Close（或轮转中途失败）关闭：Close 不刷盘，不能
//     冒称已覆盖，返回「已关闭」错误。
//
// 真 I/O 错误（EIO 等）一律原样上抛，绝不吞。
func (l *Log) Sync() error {
	l.mu.Lock()
	if l.active == nil {
		l.mu.Unlock()
		return fmt.Errorf("seglog: 已关闭，拒绝 Sync")
	}
	f, seq, prealloc := l.active, l.activeSeq, l.prealloc
	l.mu.Unlock()

	if l.syncHook != nil {
		l.syncHook()
	}

	err := syncSegment(f, seq, prealloc)
	if err == nil || !errors.Is(err, os.ErrClosed) {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.activeSeq != seq {
		// 低频（只在刷盘与轮转撞车时出现），Debug 留痕便于追查交错
		l.lg.Debug("seglog: Sync 句柄被轮转关闭，由轮转屏障覆盖",
			"dir", l.dir, "segment", segName(seq), "activeSegment", l.activeSeq)
		return nil
	}
	return fmt.Errorf("seglog: 刷盘期间日志已关闭（段 %s），本次 Sync 未落盘: %w", segName(seq), err)
}

// Close 关闭活动段句柄。之后任何 Append/Sync 返回错误。
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active == nil {
		return nil
	}
	err := l.active.Close()
	l.active = nil
	if err != nil {
		return fmt.Errorf("seglog: 关闭活动段 %s 失败: %w", segName(l.activeSeq), err)
	}
	return nil
}

// TruncateTo 按段回收：删除「已关闭且段内最大 entry index <= upto」的段
// 文件，用于状态机快照/anchor 推进后释放旧日志占用的磁盘空间。
//
// 只删整段、不做段内部分截断——粒度足够（每段默认 64MiB，快照周期通常
// 远小于其覆盖的日志量），避免了部分截断需要重写文件的复杂度。
//
// 安全性：
//   - 只从 segMax（已关闭段的登记表）里选段，active 段永不在此列，因此
//     正在被 Append 写入的段不会被删——不存在删除时另一端仍在写的竞态。
//   - segMax 记录的值可能偏高（stale-high，见字段注释），偏高只会让某个
//     段该被回收的时候还没到判定条件，绝不会造成「段里还有活条目却被
//     删掉」——该值的偏差方向天然安全。
//   - HS-only 段（从未写过 entry，segMax 缺省 0）在 upto>=0 时一样可以
//     被回收：轮转补写机制保证了任意时刻更新的段都带着当时最新的 HS 副本，
//     删掉旧的 HS-only 段不会丢失唯一的 HardState 记录。
func (l *Log) TruncateTo(upto uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var (
		deleted    []uint64
		freedBytes int64
	)
	for seq, max := range l.segMax {
		if max > upto {
			continue
		}
		path := filepath.Join(l.dir, segName(seq))
		if fi, statErr := os.Stat(path); statErr == nil {
			freedBytes += fi.Size()
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("seglog: 回收段 %s 失败: %w", segName(seq), err)
		}
		delete(l.segMax, seq)
		delete(l.segBytes, seq)
		deleted = append(deleted, seq)
	}
	l.dropRunsOf(deleted)

	if len(deleted) > 0 {
		l.lg.Info("seglog: 段回收完成",
			"dir", l.dir, "upto", upto, "deletedSegments", deleted, "freedBytes", freedBytes)
	}
	return nil
}

// ---- 读路径（卡 SQ4.10 冷区）----

// ErrCompacted 表示请求的 index 不在磁盘日志里：所在段已被 TruncateTo
// 回收、从未写入（越过日志尾或落在快照安装留下的空洞里）、或段文件在读
// 期间被并发删除。调用方（cluster.groupStorage）把它与其它读错误一并映射
// 为 raft.ErrCompacted（raft 对该 peer 改发快照）。
var ErrCompacted = errors.New("seglog: 请求的条目不在磁盘日志中")

// ckptInterval 检查点间隔：一个 run 内每写过约这么多字节记一个
// (index, 偏移)，冷读从 ≤lo 的最近检查点顺读，单次读的无效解码量 ≤ 1MiB。
// 内存账：每 GiB 保留日志约 1k 个检查点（plan B2）。
const ckptInterval int64 = 1 << 20

// ckpt 检查点：index 为 idx 的条目帧起始于段内偏移 off。
type ckpt struct {
	idx uint64
	off int64
}

// run 同一段内一串 index 连续的有效条目 [first, last]。ckpts[0] 恒为
// first 的帧偏移，其后按 ckptInterval 稀疏登记，idx 升序且都 ≤ last。
type run struct {
	seq   uint64
	first uint64
	last  uint64
	ckpts []ckpt
}

// termPoint 任期变化点：自 idx 起任期为 term（直到下一个变化点）。
type termPoint struct {
	idx  uint64
	term uint64
}

// noteEntry 登记一条刚写入（或刚扫描到）的 entry 帧，维护 runs 与 terms。
// 调用方须持有 l.mu（Open 扫描期的临时实例无并发，不持锁）。
//
// 规则与 Open 重放的「后写的赢」逐字一致，是两处唯一的实现：
//   - idx ≤ 当前尾：换届改写，先丢弃/裁短覆盖 [idx, 尾] 的旧 run 与
//     ≥ idx 的任期变化点；
//   - 同段、紧接尾部且非改写：延长当前 run，按 ckptInterval 补检查点；
//   - 其余（段轮转、改写起点、快照安装后的空洞）：新起一个 run。
func (l *Log) noteEntry(seq uint64, off int64, idx, term uint64) {
	rewrite := false
	if n := len(l.runs); n > 0 && idx <= l.runs[n-1].last {
		l.trimFrom(idx)
		rewrite = true
	}
	n := len(l.runs)
	// 改写后必须新起 run，即使裁短后的旧 run 恰好在同段且「紧接」idx：
	// 物理上旧 run 的末帧之后还躺着被作废的旧版本，run 内「帧物理连续」
	// 的承重前提不再成立
	if !rewrite && n > 0 && l.runs[n-1].seq == seq && l.runs[n-1].last+1 == idx {
		r := &l.runs[n-1]
		r.last = idx
		if off-r.ckpts[len(r.ckpts)-1].off >= ckptInterval {
			r.ckpts = append(r.ckpts, ckpt{idx: idx, off: off})
		}
	} else {
		l.runs = append(l.runs, run{seq: seq, first: idx, last: idx, ckpts: []ckpt{{idx: idx, off: off}}})
	}
	if k := len(l.terms); k == 0 || l.terms[k-1].term != term {
		l.terms = append(l.terms, termPoint{idx: idx, term: term})
	}
}

// trimFrom 让 index ≥ idx 的旧条目失效：整段落在其上的 run 丢弃，跨越
// 它的 run 裁到 idx-1；任期变化点同步丢弃 ≥ idx 的部分。调用方持锁。
func (l *Log) trimFrom(idx uint64) {
	for len(l.runs) > 0 {
		r := &l.runs[len(l.runs)-1]
		if r.first >= idx {
			l.runs = l.runs[:len(l.runs)-1]
			continue
		}
		if r.last >= idx {
			r.last = idx - 1
			k := len(r.ckpts)
			for k > 1 && r.ckpts[k-1].idx > r.last {
				k--
			}
			r.ckpts = r.ckpts[:k]
		}
		break // runs 按 first 升序且互不重叠：更早的 run 都 < idx
	}
	k := len(l.terms)
	for k > 0 && l.terms[k-1].idx >= idx {
		k--
	}
	l.terms = l.terms[:k]
}

// dropRunsOf 删除属于已回收段的 run，并修剪不再可达的任期变化点（保留
// 剩余最小 index 之前的最后一个变化点——它决定该 index 的任期）。调用方持锁。
func (l *Log) dropRunsOf(deleted []uint64) {
	if len(deleted) == 0 {
		return
	}
	gone := make(map[uint64]bool, len(deleted))
	for _, s := range deleted {
		gone[s] = true
	}
	kept := l.runs[:0]
	for _, r := range l.runs {
		if !gone[r.seq] {
			kept = append(kept, r)
		}
	}
	l.runs = kept
	if len(l.runs) == 0 {
		return // 任期表留给后续追加接续（变化点只在任期变化时追加）
	}
	lo := l.runs[0].first
	cut := 0
	for cut+1 < len(l.terms) && l.terms[cut+1].idx <= lo {
		cut++
	}
	l.terms = append(l.terms[:0], l.terms[cut:]...)
}

// firstIndexLocked 读索引覆盖的最小 index；无条目时为 0。调用方持锁。
func (l *Log) firstIndexLocked() uint64 {
	if len(l.runs) == 0 {
		return 0
	}
	return l.runs[0].first
}

// FirstIndex 返回磁盘日志中仍可读的最小 index；无条目时 ok=false。
//
// 注意：按段回收的粒度下，它通常 ≤ 快照锚点（同段内锚点之前的条目仍在
// 盘上）；raft 视角的日志起点由调用方按锚点另行裁定。
func (l *Log) FirstIndex() (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.runs) == 0 {
		return 0, false
	}
	return l.runs[0].first, true
}

// LastIndex 返回磁盘日志中最大的有效 index；无条目时 ok=false。
func (l *Log) LastIndex() (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.runs) == 0 {
		return 0, false
	}
	return l.runs[len(l.runs)-1].last, true
}

// HardState 返回最新已写的 HardState（从未写过为 nil）。返回的是共享
// 指针，调用方只读。
func (l *Log) HardState() *raftpb.HardState {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastHS
}

// findRun 返回覆盖 idx 的 run 下标；不覆盖时 -1。调用方持锁。
func (l *Log) findRun(idx uint64) int {
	i := sort.Search(len(l.runs), func(i int) bool { return l.runs[i].last >= idx })
	if i == len(l.runs) || l.runs[i].first > idx {
		return -1
	}
	return i
}

// Term 返回 index 为 idx 的条目任期。
//
// 返回：idx 不被读索引覆盖时 ErrCompacted（已回收 / 未写入 / 空洞）。
// 纯内存查询（任期变化点表），不碰磁盘。
func (l *Log) Term(idx uint64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.findRun(idx) < 0 {
		return 0, fmt.Errorf("%w: term(%d) 不在读索引覆盖内", ErrCompacted, idx)
	}
	k := sort.Search(len(l.terms), func(i int) bool { return l.terms[i].idx > idx })
	if k == 0 {
		// 有 run 覆盖却没有 ≤idx 的变化点：索引维护缺陷，按读不到处理
		return 0, fmt.Errorf("%w: term(%d) 缺任期变化点（读索引不一致）", ErrCompacted, idx)
	}
	return l.terms[k-1].term, nil
}

// readPiece 一次冷读里落在同一 run 的那一段：从检查点 (ckIdx, ckOff) 顺读，
// 收 [from, to] 的条目；limit 是该段在取快照那一刻的已写字节数。
type readPiece struct {
	seq   uint64
	from  uint64
	to    uint64
	ckIdx uint64
	ckOff int64
	limit int64
}

// ReadEntries 读回 [lo, hi) 区间的条目（升序、index 连续、起于 lo）。
//
// 参数：
//   - lo, hi: 半开区间，lo < hi
//   - maxSize: 返回条目 proto 字节数之和的上限（raft MaxSizePerMsg 语义，
//     与 raft 的 limitSize 同口径）；无论多小都至少返回一条
//
// 返回：
//   - 条目：可能短于 hi-lo——遇字节上限，或遇读索引的覆盖空洞即止
//   - err: lo 不被覆盖时 ErrCompacted；段文件已被并发回收时 ErrCompacted；
//     帧损坏、index 不连续等 I/O / 一致性错误原样带上下文返回
//
// 并发：锁内只做 O(log n) 的索引定位与快照，打开文件与读全在锁外，用
// 独立只读句柄；与 Append/Sync/TruncateTo 并发安全。读到半途段被删除：
// 已打开的句柄在 POSIX 下照常可读（unlink 不影响已打开 fd），未打开的
// 返回 ErrCompacted，不会读到半删文件的内容。
func (l *Log) ReadEntries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error) {
	if lo >= hi {
		return nil, nil
	}
	l.mu.Lock()
	i := l.findRun(lo)
	if i < 0 {
		l.mu.Unlock()
		return nil, fmt.Errorf("%w: [%d,%d) 起点不在读索引覆盖内", ErrCompacted, lo, hi)
	}
	var pieces []readPiece
	for next := lo; i < len(l.runs) && next < hi; i++ {
		r := l.runs[i]
		if r.first > next {
			break // 覆盖空洞：只返回空洞之前的部分
		}
		to := min(r.last, hi-1)
		k := sort.Search(len(r.ckpts), func(j int) bool { return r.ckpts[j].idx > next }) - 1
		limit := l.segBytes[r.seq]
		if r.seq == l.activeSeq {
			limit = l.activeSize
		}
		pieces = append(pieces, readPiece{seq: r.seq, from: next, to: to,
			ckIdx: r.ckpts[k].idx, ckOff: r.ckpts[k].off, limit: limit})
		next = to + 1
	}
	dir := l.dir
	l.mu.Unlock()

	var (
		out   []*raftpb.Entry
		total uint64
	)
	for _, p := range pieces {
		done, err := readPieceInto(dir, p, maxSize, &out, &total)
		if err != nil {
			if len(out) > 0 && errors.Is(err, ErrCompacted) {
				return out, nil // 前缀已读到：先返回前缀，下一轮再碰缺口
			}
			return nil, err
		}
		if done {
			break
		}
	}
	return out, nil
}

// readPieceInto 顺读一个 readPiece，把 [from, to] 的条目追加进 *out。
// 返回 done=true 表示字节上限已到、调用方不应再读后续 piece。
func readPieceInto(dir string, p readPiece, maxSize uint64, out *[]*raftpb.Entry, total *uint64) (bool, error) {
	name := segName(p.seq)
	f, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		if os.IsNotExist(err) {
			return false, fmt.Errorf("%w: 段 %s 已回收", ErrCompacted, name)
		}
		return false, fmt.Errorf("seglog: 冷读打开段 %s 失败: %w", name, err)
	}
	defer f.Close()
	if p.ckOff > p.limit {
		return false, fmt.Errorf("seglog: 冷读段 %s 检查点偏移 %d 越过已写长度 %d", name, p.ckOff, p.limit)
	}
	br := bufio.NewReaderSize(io.NewSectionReader(f, p.ckOff, p.limit-p.ckOff), 256<<10)
	var (
		frame  []byte
		expect = p.ckIdx
		off    = p.ckOff
	)
	for expect <= p.to {
		frame = frame[:0]
		var hdr [frameHeaderLen]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return false, fmt.Errorf("seglog: 冷读段 %s 偏移 %d 读帧头失败（期望 index %d）: %w", name, off, expect, err)
		}
		ln := binary.BigEndian.Uint32(hdr[0:4])
		if ln == 0 || ln > maxFrameLen {
			return false, fmt.Errorf("seglog: 冷读段 %s 偏移 %d 帧长 %d 非法", name, off, ln)
		}
		frame = append(frame, hdr[:]...)
		frame = append(frame, make([]byte, ln)...)
		if _, err := io.ReadFull(br, frame[frameHeaderLen:]); err != nil {
			return false, fmt.Errorf("seglog: 冷读段 %s 偏移 %d 读帧体失败: %w", name, off, err)
		}
		typ, payload, n, ferr := readFrame(frame)
		if ferr != nil {
			return false, fmt.Errorf("seglog: 冷读段 %s 偏移 %d 帧校验失败: %w", name, off, ferr)
		}
		off += int64(n)
		if typ != recEntry {
			continue // HardState 帧：读路径不关心
		}
		e := &raftpb.Entry{}
		if err := proto.Unmarshal(payload, e); err != nil {
			return false, fmt.Errorf("seglog: 冷读段 %s 偏移 %d entry 解码失败: %w", name, off-int64(n), err)
		}
		if e.GetIndex() != expect {
			// run 内条目物理连续是索引的承重前提，不符即索引与文件不一致
			return false, fmt.Errorf("seglog: 冷读段 %s 偏移 %d 得到 index %d，期望 %d（读索引与文件不一致）",
				name, off-int64(n), e.GetIndex(), expect)
		}
		expect++
		if e.GetIndex() < p.from {
			continue // 检查点到 from 之间的条目
		}
		sz := uint64(proto.Size(e))
		if len(*out) > 0 && *total+sz > maxSize {
			return true, nil
		}
		*out = append(*out, e)
		*total += sz
	}
	return false, nil
}

// SegInfo 一个段的只读视图（按字节保留的计算输入）。
type SegInfo struct {
	Seq      uint64
	Bytes    int64  // 逻辑字节数（活动段 = 已写长度）
	MaxIndex uint64 // TruncateTo 的回收判据值（活动段无此值，恒 0）
	Active   bool
}

// Segments 返回全部段的只读视图，按段号升序（活动段在末尾）。
func (l *Log) Segments() []SegInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]SegInfo, 0, len(l.segMax)+1)
	for seq, max := range l.segMax {
		out = append(out, SegInfo{Seq: seq, Bytes: l.segBytes[seq], MaxIndex: max})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return append(out, SegInfo{Seq: l.activeSeq, Bytes: l.activeSize, Active: true})
}

// RetainBytesCap 返回「保证截断后剩余段总字节 ≥ budget」所允许的最大
// 截断点 upto（TruncateTo(upto) 只删 MaxIndex ≤ upto 的已关闭段）。
//
// 算法：从最新段往回累加字节，直到累计 ≥ budget；这些段都必须保留，
// upto 须小于它们中最小的 MaxIndex。段粒度、只多不少（plan B3）：
//   - 活动段一段已够 budget：返回 math.MaxUint64（字节不构成约束）；
//   - 全部段加起来都不够 budget：返回 0（一条都不许截——截断点同时是
//     raft 视角的日志起点，不能只看「删不删段」）；
//   - 需要保留的段里有纯 HardState 段（MaxIndex=0）：返回 0（保守方向）。
func RetainBytesCap(segs []SegInfo, budget int64) uint64 {
	var acc int64
	upto := uint64(math.MaxUint64)
	for i := len(segs) - 1; i >= 0; i-- {
		if acc >= budget {
			return upto
		}
		s := segs[i]
		acc += s.Bytes
		if s.Active {
			continue
		}
		if s.MaxIndex == 0 {
			return 0
		}
		upto = min(upto, s.MaxIndex-1)
	}
	if acc >= budget {
		return upto
	}
	return 0
}
