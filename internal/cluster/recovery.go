// recovery.go 提供不干净关机后的恢复路径判定，以及给 sq recover 命令用
// 的只读现场采集与许可授予入口。
//
// 职责：
//   - 把「盘上状态 + 确认档位 + 机器世代 + 运维许可」四组判据映射到
//     唯一一条恢复路径，并给出可直接进日志/进报告的中文理由
//   - InspectRecovery / GrantRecoverPermit：CLI 侧的只读现场与签字入口，
//     与 NewManager 共用同一套判据（见文件头注释）
//
// 边界：
//   - decideRecovery 是纯函数：不碰 raft、不碰磁盘、不打日志、不读环境
//     变量。四组判据由调用方采集后传入
//   - 不执行恢复：本文件只回答「走哪条路」，怎么走在 manager.go
//   - InspectRecovery 只读（连干净关机标记都不消费）；GrantRecoverPermit
//     是唯一的写入口，且只写许可键
//
// 为什么要抽成纯函数：NewManager 与 sq recover 必须给出**完全一致**的
// 判断。一旦两处各写一套，迟早出现「命令说你不用签字、进程说你要签字」，
// 那是最伤运维信任的一类分歧。共用一个函数是唯一可靠的保证方式；顺带
// 让五条分支能脱离 raft 与磁盘直接单测（recovery_test.go 的判定表）。
package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/xushixin/sq/internal/store"
)

// recoveryPath 是不干净关机后的恢复路径。
type recoveryPath int

const (
	// pathCleanResume 上次是优雅停机：回放本地日志，原身份回归
	pathCleanResume recoveryPath = iota
	// pathFresh 全新数据目录：按成员表引导启动
	pathFresh
	// pathLocalResume 不干净关机但本地日志可证完好：等同 clean-resume
	pathLocalResume
	// pathLocalForced 不干净关机且日志可能残缺，但运维已签字放行：
	// 抬 term 后回放本地日志（抬 term 的理由见 manager.go 的调用点）
	pathLocalForced
	// pathRejoin 不干净关机且无法证明日志可信：交给重入编排
	// （求集群接纳→清空→learner 重入；求不到接纳则拒启且数据完好）
	pathRejoin
)

// String 返回路径的短名，用于日志的 recovery 字段。
func (p recoveryPath) String() string {
	switch p {
	case pathCleanResume:
		return "clean-resume"
	case pathFresh:
		return "fresh"
	case pathLocalResume:
		return "local-resume"
	case pathLocalForced:
		return "local-forced"
	case pathRejoin:
		return "rejoin"
	}
	return "unknown"
}

// recoveryInput 是恢复判定的全部输入判据。
//
// 字段两两成对的 OK 位不是冗余：世代「读不到」与世代「是空串」必须能
// 区分开，否则两次都读不到会比较相等，被误判成「机器没重启过」。
type recoveryInput struct {
	Clean   bool    // 干净关机标记是否存在
	HasRaft bool    // 盘上是否已有 raft 状态（任一组 HardState 或 applied 非零）
	Mode    AckMode // 确认档位

	GenNow        string // 本机当前机器世代
	GenNowOK      bool   // 当前世代是否可用（读不到时为 false）
	GenStored     string // 盘上记录的机器世代
	GenStoredOK   bool   // 盘上是否记录过世代（旧数据目录首次升级时为 false）

	PermitGen string // 运维许可绑定的机器世代
	PermitOK  bool   // 是否存在运维许可
}

// sameMachineBoot 判定「本数据目录上次被写入」与「现在」是否属于同一次开机。
//
// 只有两侧世代都可用且相等才为真。任何一侧不可用都返回 false——不可比
// 就是不可信，这是安全方向。
func (in recoveryInput) sameMachineBoot() bool {
	return in.GenNowOK && in.GenStoredOK && in.GenNow == in.GenStored
}

// permitValid 判定运维许可是否对**本次**事故有效。
//
// 许可绑定授予时的机器世代，因此只对运维当时看到的那一次事故有效：
// 机器再重启一次，世代就变了，旧许可自动失效。这条替代了 TTL，
// 因而不依赖时钟。
func (in recoveryInput) permitValid() bool {
	return in.PermitOK && in.GenNowOK && in.PermitGen == in.GenNow
}

// decideRecovery 判定恢复路径。
//
// 参数：
//   - in: 四组判据，采集方式见 recoveryInput 各字段注释
//
// 返回：
//   - 恢复路径
//   - 中文理由串（进 NewManager 的判定日志与 sq recover 的报告；恒非空）
//
// 判定表见 spec §3.2；改这里必须同步改 recovery_test.go 的用例表。
func decideRecovery(in recoveryInput) (recoveryPath, string) {
	switch {
	case in.Clean:
		return pathCleanResume, "上次为干净关机，回放本地日志以原身份回归"
	case !in.HasRaft:
		return pathFresh, "数据目录无 raft 状态，按成员表引导启动"
	case in.sameMachineBoot():
		// 机器没重启过 ⇒ 页缓存从未丢失 ⇒ NoSync 写入也都还在
		return pathLocalResume, "不干净关机，但机器世代未变（进程崩溃、机器未重启），本地日志完整，直接以原身份恢复"
	case in.Mode == AckQuorumFsync:
		// fsync 档跟随 raft.MustSync：条目与 term/vote 每次变更都已落盘，
		// 掉电只可能丢 commit 位点的推进，而那可由 leader 重新告知
		return pathLocalResume, "不干净关机且机器可能已重启，但确认档为 quorum-fsync（条目与投票每次变更均已落盘），本地日志可信，直接以原身份恢复"
	case in.permitValid():
		return pathLocalForced, "不干净关机且机器已重启，quorum-mem 档下日志尾可能残缺；已存在与本次机器世代匹配的运维许可，按签字放行本地恢复"
	case in.PermitOK:
		// 有许可但绑的是别的世代：等于没有
		return pathRejoin, "存在运维许可但绑定的机器世代与本次不符（旧事故的签字不能复用于本次），按不干净关机走重入编排"
	default:
		return pathRejoin, "不干净关机且无法证明本地日志可信（机器世代不可比或已变、确认档为 quorum-mem、无运维许可），走重入编排"
	}
}

// GroupReport 是单个 raft 组在恢复报告中的现场数据。
type GroupReport struct {
	Group     uint32 // 组号（0 为 meta 组）
	Applied   uint64 // 已应用位点
	LastIndex uint64 // 日志尾 index（无条目时为 0）
	LastTerm  uint64 // 日志尾 term（无条目时为 0）
	SnapIndex uint64 // 快照锚点 index（从未截断时为 0）
	Term      uint64 // 当前任期
	Commit    uint64 // 提交位点
}

// RecoveryReport 是 sq recover 打给运维看的完整报告。
type RecoveryReport struct {
	Path        string        // 恢复路径短名
	Reason      string        // 中文理由
	NeedsPermit bool          // 是否需要运维签字才能本地恢复
	GenNow      string        // 当前机器世代（读不到时为空）
	GenNowOK    bool          // 当前世代是否可用
	GenStored   string        // 盘上记录的机器世代
	GenStoredOK bool          // 盘上是否记录过
	Mode        AckMode       // 确认档位
	HasPermit   bool          // 是否已存在许可
	PermitGen   string        // 已存在许可绑定的世代
	Groups      []GroupReport // 各组现场
}

// InspectRecovery 只读地采集恢复判据与各组现场，供 sq recover 渲染报告。
//
// 参数：
//   - st: 已打开的 store（调用方负责开关）
//   - dataGroups: 数据组数（组 0..dataGroups 全部纳入报告）
//   - mode: 确认档位（来自配置）
//   - bootGen: 机器世代读取函数（nil 走平台实现）
//   - lg: 日志器
//
// 返回：报告与错误。
//
// 注意：本函数**只读**——不消费干净关机标记、不写机器世代、不动任何键。
// 判定复用 decideRecovery，与 NewManager 同源，因此命令与进程给出的结论
// 恒等一致（见本文件头注释）。
func InspectRecovery(st *store.Store, dataGroups uint32, mode AckMode, bootGen BootGenFunc, lg *slog.Logger) (RecoveryReport, error) {
	rs := newRaftStore(st, lg)
	// 只探标记在不在，不消费它——消费是 NewManager 的副作用，命令不能替它做
	_, hasClean, err := st.Get([]byte(cleanShutdownKey))
	if err != nil {
		return RecoveryReport{}, fmt.Errorf("cluster: 读干净关机标记: %w", err)
	}
	genNow, genNowOK := resolveBootGen(bootGen, lg)
	genStored, genStoredOK, err := rs.LoadBootGen()
	if err != nil {
		return RecoveryReport{}, err
	}
	permit, permitOK, err := rs.LoadRecoverPermit()
	if err != nil {
		return RecoveryReport{}, err
	}

	hasRaft := false
	groups := make([]GroupReport, 0, dataGroups+1)
	for g := uint32(0); g <= dataGroups; g++ {
		hs, ents, snapMeta, err := rs.Load(g)
		if err != nil {
			return RecoveryReport{}, err
		}
		applied, err := rs.Applied(g)
		if err != nil {
			return RecoveryReport{}, err
		}
		gr := GroupReport{
			Group: g, Applied: applied,
			SnapIndex: snapMeta.GetIndex(),
			Term:      hs.GetTerm(), Commit: hs.GetCommit(),
		}
		if n := len(ents); n > 0 {
			gr.LastIndex = ents[n-1].GetIndex()
			gr.LastTerm = ents[n-1].GetTerm()
		}
		if applied != 0 || !raft.IsEmptyHardState(hs) {
			hasRaft = true
		}
		groups = append(groups, gr)
	}

	path, reason := decideRecovery(recoveryInput{
		Clean: hasClean, HasRaft: hasRaft, Mode: mode,
		GenNow: genNow, GenNowOK: genNowOK,
		GenStored: genStored, GenStoredOK: genStoredOK,
		PermitGen: permit.Gen, PermitOK: permitOK,
	})
	return RecoveryReport{
		Path: path.String(), Reason: reason,
		NeedsPermit: path == pathRejoin,
		GenNow:      genNow, GenNowOK: genNowOK,
		GenStored: genStored, GenStoredOK: genStoredOK,
		Mode:      mode,
		HasPermit: permitOK, PermitGen: permit.Gen,
		Groups: groups,
	}, nil
}

// GrantRecoverPermit 写入一次性本地恢复许可，绑定当前机器世代。
//
// 参数：
//   - st: 已打开的 store
//   - now: 授予时间（调用方传入，便于测试固定时钟）
//   - bootGen: 机器世代读取函数（nil 走平台实现）
//   - lg: 日志器
//
// 世代读不到时拒绝授予：许可靠世代作废，没有世代的许可等于一张永不过期
// 的通行证，比不给还危险。
func GrantRecoverPermit(st *store.Store, now time.Time, bootGen BootGenFunc, lg *slog.Logger) error {
	gen, ok := resolveBootGen(bootGen, lg)
	if !ok {
		return errors.New("cluster: 读不到本机机器世代，拒绝授予本地恢复许可——" +
			"许可靠世代作废，没有世代的许可是一张永不过期的通行证")
	}
	rs := newRaftStore(st, lg)
	return rs.SaveRecoverPermit(recoverPermit{GrantedAt: now.Format(time.RFC3339), Gen: gen})
}

// needsTermBump 判定某条恢复路径是否必须在回放前抬任期、清投票。
//
// 判据是**投票记录是不是同步落盘的**，不是「机器有没有重启」：
//   - fsync 档：syncPersist 跟随 raft.MustSync，term/vote 每次变更都已 fsync，
//     投票不可能丢，抬了纯属白白多付一次选举
//   - mem 档：HardState 走 Pebble 的 NoSync——commit 返回时数据可能还在
//     进程内的 WAL 缓冲里（write(2) 由 flusher goroutine 异步执行），
//     所以任何一次不干净关机后都可能少一张已投出的票
//
// 少一张票的后果是**损坏**而非丢数据：同一任期投第二次 → 两个 leader →
// 日志分叉。抬任期在 raft 中永远安全，代价只是强制一次重新选举。
//
// clean-resume 不需要：MarkCleanShutdown 是 Sync 写，会把此前所有 NoSync
// 写一并刷盘。fresh 无历史。rejoin 会清空状态重入，更无从谈起。
func needsTermBump(p recoveryPath, mode AckMode) bool {
	switch p {
	case pathLocalForced:
		return true
	case pathLocalResume:
		return mode == AckQuorumMem
	default:
		return false
	}
}
