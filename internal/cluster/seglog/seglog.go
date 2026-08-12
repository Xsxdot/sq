// seglog.go 提供单个 raft 组的分段追加日志实现：段扫描恢复、追加、
// fsync 与关闭。
//
// 职责：
//   - Open 时按序扫描全部段文件，重放出连续的 HardState + Entry 状态
//   - Append 把一轮 Ready 的 HardState/Entries 编码为帧写入活动段
//   - 处理掉电导致的 torn tail（仅末段合法）与非末段真损坏（拒绝启动）
//
// 边界：
//   - 不提供随机读接口：raft 侧读日志走 MemoryStorage 双记账（spec §3）
package seglog

import (
	"fmt"
	"log/slog"
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
// goroutine，内部单互斥串行化——三者频率都低（每轮 Ready / 200ms /
// 30s），锁竞争可忽略。
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
//   - Log: 就绪的日志（尾部定位在最后一条好帧之后）
//   - hs: 恢复出的最新 HardState；从未写过时为 nil
//   - ents: 「后写的赢」重放后的连续条目序列（升序）；可能为空
//   - err: 非末段坏帧（真损坏，非 torn write）或 I/O 错误
//
// 注意：末段的 torn tail（掉电正常形态）在此被物理截断到好帧边界后
// 继续——绝不静默保留坏字节，否则续写会接在坏帧后面永远读不回。
func Open(dir string, lg *slog.Logger) (*Log, *raftpb.HardState, []*raftpb.Entry, error) {
	if err := mkdirAllSync(dir); err != nil {
		return nil, nil, nil, err
	}

	seqs, err := scanSegSeqs(dir)
	if err != nil {
		return nil, nil, nil, err
	}

	var (
		hs        *raftpb.HardState
		ents      []*raftpb.Entry
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
			return nil, nil, nil, fmt.Errorf("seglog: 读段 %s 失败: %w", name, err)
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
					return nil, nil, nil, fmt.Errorf(
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
					return nil, nil, nil, fmt.Errorf("seglog: 截断段 %s 到偏移 %d 失败: %w", name, off, err)
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
						return nil, nil, nil, fmt.Errorf(
							"seglog: 段 %s 偏移 %d entry 解码失败且非末段——真损坏，拒绝启动: %w", name, off, uerr)
					}
					// CRC 校验通过但内容解码失败：写入方 bug 或位腐，按 torn
					// 处理（末段才容忍），截断丢弃。
					discarded := len(data) - off
					if err := os.Truncate(path, int64(off)); err != nil {
						return nil, nil, nil, fmt.Errorf("seglog: 截断段 %s 到偏移 %d 失败: %w", name, off, err)
					}
					lg.Warn("seglog: 末段 entry 解码失败，按 torn 截断到好帧边界",
						"segment", name, "goodOffset", off, "discardedBytes", discarded)
					tornInfo = name
					break scan // 文件已物理截断到 off，不能再按 len(data) 继续扫描
				}
				idx := e.GetIndex()
				// 后写的赢：新条目 index <= 当前已知 lastIndex，说明发生了
				// 换届回退重写，之前收集的 [idx, lastIndex] 区间条目已失效，
				// 截掉后再 append 新条目。条目升序写入、回退距离通常很短，
				// 线性回退足够（无需二分）。
				if idx <= lastIndex {
					cut := len(ents)
					for cut > 0 && ents[cut-1].GetIndex() >= idx {
						cut--
					}
					ents = ents[:cut]
				}
				ents = append(ents, &e)
				lastIndex = idx
				// 段内最大 entry index：随着本段扫描推进单调覆盖写入，最终
				// 落在「本段物理上最后一条 entry 帧」的 index。若本段内部
				// 也发生过换届冲突重写（同段内先写 7..10 又写 7'），最终值
				// 就是冲突后幸存的那条（7），天然与上面的 ents 截断结果一致，
				// 不会偏高。跨段冲突（重写发生在更晚的段）不会回头修正本段
				// 已经落定的值——那种情况下这里会偏高，见 segMax 字段注释。
				segMax[seq] = idx
			case recHardState:
				var h raftpb.HardState
				if uerr := proto.Unmarshal(payload, &h); uerr != nil {
					if !isLast {
						return nil, nil, nil, fmt.Errorf(
							"seglog: 段 %s 偏移 %d hardstate 解码失败且非末段——真损坏，拒绝启动: %w", name, off, uerr)
					}
					discarded := len(data) - off
					if err := os.Truncate(path, int64(off)); err != nil {
						return nil, nil, nil, fmt.Errorf("seglog: 截断段 %s 到偏移 %d 失败: %w", name, off, err)
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
		return nil, nil, nil, fmt.Errorf("seglog: 打开活动段 %s 失败: %w", segName(activeSeq), err)
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
			return nil, nil, nil, err
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
		return nil, nil, nil, err
	}

	commit := uint64(0)
	if hs != nil {
		commit = hs.GetCommit()
	}
	lg.Info("seglog: 打开完成",
		"segments", len(seqs), "entries", len(ents), "lastIndex", lastIndex,
		"hsCommit", commit, "tornTruncated", tornInfo != "", "tornSegment", tornInfo,
		"prealloc", l.prealloc)

	return l, hs, ents, nil
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
// 失败即返回错误，调用方（raftStore.Persist → group.handleReady）按
// fail-stop 处理；本层不重试——写失败后文件偏移状态不可信。
func (l *Log) Append(hs *raftpb.HardState, ents []*raftpb.Entry, sync bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active == nil {
		return fmt.Errorf("seglog: 已关闭，拒绝 Append")
	}

	l.buf = l.buf[:0]
	if hs != nil {
		payload, err := proto.Marshal(hs)
		if err != nil {
			return fmt.Errorf("seglog: 编码 hardstate 失败: %w", err)
		}
		l.buf = appendFrame(l.buf, recHardState, payload)
	}
	for _, e := range ents {
		payload, err := proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("seglog: 编码 entry(index=%d) 失败: %w", e.GetIndex(), err)
		}
		l.buf = appendFrame(l.buf, recEntry, payload)
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
	if l.prealloc {
		return datasync(l.active)
	}
	if err := l.active.Sync(); err != nil {
		return fmt.Errorf("seglog: fsync 活动段 %s 失败: %w", segName(l.activeSeq), err)
	}
	return nil
}

// Sync 刷活动段到盘（mem 档 200ms flusher 的批量刷盘入口）。
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active == nil {
		return fmt.Errorf("seglog: 已关闭，拒绝 Sync")
	}
	return l.syncActive()
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
		deleted = append(deleted, seq)
	}

	if len(deleted) > 0 {
		l.lg.Info("seglog: 段回收完成",
			"dir", l.dir, "upto", upto, "deletedSegments", deleted, "freedBytes", freedBytes)
	}
	return nil
}
