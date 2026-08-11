// seglog.go 提供单个 raft 组的分段追加日志实现：段扫描恢复、追加、
// fsync 与关闭。
//
// 职责：
//   - Open 时按序扫描全部段文件，重放出连续的 HardState + Entry 状态
//   - Append 把一轮 Ready 的 HardState/Entries 编码为帧写入活动段
//   - 处理掉电导致的 torn tail（仅末段合法）与非末段真损坏（拒绝启动）
//
// 边界：
//   - 本 task 只实现单段版本；段轮转与按段回收（TruncateTo）留给 Task 4，
//     本文件只声明并维护轮转所需字段（segMax/lastHS/activeSize），不实现
//     轮转逻辑
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
	mu         sync.Mutex
	dir        string
	lg         *slog.Logger
	active     *os.File          // 当前活动段（始终打开，Append 的写入目标）
	activeSeq  uint64            // 活动段序号
	activeSize int64             // 活动段当前字节数（轮转判定，Task 4 用）
	lastIndex  uint64            // 日志尾 index；0 = 空日志
	lastHS     *raftpb.HardState // 最新已写 HardState（轮转补写用，Task 4）
	segMax     map[uint64]uint64 // 已关闭段号 → 段内最大 entry index（回收判定，Task 4）
	buf        []byte            // Append 的帧组装缓冲（复用减分配）
}

// segName 段文件名：8 位十进制序号，字典序 = 数值序。
func segName(seq uint64) string { return fmt.Sprintf("%08d.seg", seq) }

// segSuffix 段文件名后缀：完整格式为 8 位数字 + segSuffix。
const segSuffix = ".seg"

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
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, nil, fmt.Errorf("seglog: 创建目录 %s 失败: %w", dir, err)
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
	)

	for i, seq := range seqs {
		isLast := i == len(seqs)-1
		name := segName(seq)
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("seglog: 读段 %s 失败: %w", name, err)
		}

		off := 0
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
				// 末段 torn tail：掉电时最后一次 write() 可能只落盘一部分，
				// 是正常形态。物理截断到好帧边界后继续——绝不静默保留坏
				// 字节，否则续写会接在坏帧后面，这段坏字节永远读不回。
				discarded := len(data) - off
				if err := os.Truncate(path, int64(off)); err != nil {
					return nil, nil, nil, fmt.Errorf("seglog: 截断段 %s 到偏移 %d 失败: %w", name, off, err)
				}
				lg.Warn("seglog: 检测到 torn tail，已截断到好帧边界",
					"segment", name, "goodOffset", off, "discardedBytes", discarded)
				tornInfo = name
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
					off = len(data) // 跳出外层循环
					goto doneSeg
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
					off = len(data)
					goto doneSeg
				}
				// 后写的赢：同一轮或跨轮写入的 HardState 以最后一条为准。
				hs = &h
			}
			off += n
		}
	doneSeg:
	}

	// 确定活动段：无任何段时创建 00000001.seg；否则续用最后一段。
	var activeSeq uint64 = 1
	if len(seqs) > 0 {
		activeSeq = seqs[len(seqs)-1]
	}
	activePath := filepath.Join(dir, segName(activeSeq))
	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("seglog: 打开活动段 %s 失败: %w", segName(activeSeq), err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, nil, fmt.Errorf("seglog: stat 活动段 %s 失败: %w", segName(activeSeq), err)
	}
	// 活动段自身不计入 segMax（segMax 只记「已关闭」段），delete 掉活动段号
	// 若上面扫描时误记（例如活动段就是唯一段）。
	delete(segMax, activeSeq)

	l := &Log{
		dir:        dir,
		lg:         lg,
		active:     f,
		activeSeq:  activeSeq,
		activeSize: fi.Size(),
		lastIndex:  lastIndex,
		lastHS:     hs,
		segMax:     segMax,
	}

	commit := uint64(0)
	if hs != nil {
		commit = hs.GetCommit()
	}
	lg.Info("seglog: 打开完成",
		"segments", len(seqs), "entries", len(ents), "lastIndex", lastIndex,
		"hsCommit", commit, "tornTruncated", tornInfo != "", "tornSegment", tornInfo)

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
		l.lastHS = hs
	}
	for _, e := range ents {
		payload, err := proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("seglog: 编码 entry(index=%d) 失败: %w", e.GetIndex(), err)
		}
		l.buf = appendFrame(l.buf, recEntry, payload)
	}

	if len(l.buf) > 0 {
		n, err := l.active.Write(l.buf)
		if err != nil {
			return fmt.Errorf("seglog: 写活动段 %s 失败: %w", segName(l.activeSeq), err)
		}
		l.activeSize += int64(n)
	}

	if len(ents) > 0 {
		l.lastIndex = ents[len(ents)-1].GetIndex()
	}

	if sync {
		if err := l.active.Sync(); err != nil {
			return fmt.Errorf("seglog: fsync 活动段 %s 失败: %w", segName(l.activeSeq), err)
		}
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
	if err := l.active.Sync(); err != nil {
		return fmt.Errorf("seglog: fsync 活动段 %s 失败: %w", segName(l.activeSeq), err)
	}
	return nil
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
