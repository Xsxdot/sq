// recovery.go 提供不干净关机后的恢复路径判定。
//
// 职责：
//   - 把「盘上状态 + 确认档位 + 机器世代 + 运维许可」四组判据映射到
//     唯一一条恢复路径，并给出可直接进日志/进报告的中文理由
//
// 边界：
//   - 纯函数：不碰 raft、不碰磁盘、不打日志、不读环境变量。四组判据由
//     调用方采集后传入
//   - 不执行恢复：本文件只回答「走哪条路」，怎么走在 manager.go
//
// 为什么要抽成纯函数：NewManager 与 sq recover 必须给出**完全一致**的
// 判断。一旦两处各写一套，迟早出现「命令说你不用签字、进程说你要签字」，
// 那是最伤运维信任的一类分歧。共用一个函数是唯一可靠的保证方式；顺带
// 让五条分支能脱离 raft 与磁盘直接单测（recovery_test.go 的判定表）。
package cluster

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
