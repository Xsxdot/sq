package cluster

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

// TestDecideRecovery 覆盖五条恢复分支的全部判据组合。
//
// 这张表就是 spec §3.2 的判定表本身；改动判定逻辑必须先改这张表。
func TestDecideRecovery(t *testing.T) {
	const genA, genB = "gen-a", "gen-b"
	cases := []struct {
		name string
		in   recoveryInput
		want recoveryPath
	}{
		{
			name: "有干净关机标记：原身份回归，其余判据一概不看",
			in:   recoveryInput{Clean: true, HasRaft: true, Mode: AckQuorumMem, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true},
			want: pathCleanResume,
		},
		{
			name: "全新目录：引导启动",
			in:   recoveryInput{Clean: false, HasRaft: false, Mode: AckQuorumMem, GenNow: genA, GenNowOK: true},
			want: pathFresh,
		},
		{
			name: "不干净 + 世代未变：进程崩溃而已，页缓存完好，本地恢复",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genA, GenNowOK: true, GenStored: genA, GenStoredOK: true},
			want: pathLocalResume,
		},
		{
			name: "不干净 + 世代变了 + fsync 档：每轮 MustSync 已落盘，无需签字",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumFsync, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true},
			want: pathLocalResume,
		},
		{
			name: "不干净 + 世代变了 + mem 档 + 有匹配许可：签字放行",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true, PermitGen: genB, PermitOK: true},
			want: pathLocalForced,
		},
		{
			name: "不干净 + 世代变了 + mem 档 + 无许可：重入编排",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true},
			want: pathRejoin,
		},
		{
			name: "许可绑的是别的世代：等于没有许可（堵死旧许可被后一次事故复用）",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genB, GenNowOK: true, GenStored: genA, GenStoredOK: true, PermitGen: genA, PermitOK: true},
			want: pathRejoin,
		},
		{
			name: "当前世代读不到：不可比，保守走重入编排",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNowOK: false, GenStored: genA, GenStoredOK: true},
			want: pathRejoin,
		},
		{
			name: "盘上没记过世代（旧数据目录首次升级）：不可比，保守走重入编排",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNow: genA, GenNowOK: true, GenStoredOK: false},
			want: pathRejoin,
		},
		{
			name: "两侧世代都读不到：绝不能因为都是空串就判成相等",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumMem, GenNowOK: false, GenStoredOK: false},
			want: pathRejoin,
		},
		{
			name: "世代读不到 + fsync 档：档位本身就够，仍可本地恢复",
			in:   recoveryInput{Clean: false, HasRaft: true, Mode: AckQuorumFsync, GenNowOK: false, GenStoredOK: false},
			want: pathLocalResume,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, reason := decideRecovery(c.in)
			if got != c.want {
				t.Fatalf("decideRecovery = %v（理由：%s）; want %v", got, reason, c.want)
			}
			if reason == "" {
				t.Fatal("decideRecovery 的理由串为空——它要进日志和 sq recover 的报告，不能没有")
			}
		})
	}
}

// TestInspectRecoveryReportsPathAndPermitNeed CLI 用的只读入口必须与
// NewManager 得出同一条路径——两处各判一次迟早会出现「命令说你不用签字、
// 进程说你要签字」，那是最伤运维信任的一类分歧。
func TestInspectRecoveryReportsPathAndPermitNeed(t *testing.T) {
	dir := t.TempDir()
	st := mustOpenStore(t, dir)
	rs := newRaftStore(st, testSlog(t))
	if err := rs.EnsureGroups(3); err != nil {
		t.Fatalf("EnsureGroups: %v", err)
	}
	term, vote, commit := uint64(3), uint64(1), uint64(5)
	if err := rs.Persist(0, &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}, nil, true); err != nil {
		t.Fatalf("造 HardState: %v", err)
	}
	if err := rs.SaveBootGen("gen-a"); err != nil {
		t.Fatalf("SaveBootGen: %v", err)
	}

	rep, err := InspectRecovery(st, 3, AckQuorumMem, func() (string, error) { return "gen-b", nil }, testSlog(t))
	if err != nil {
		t.Fatalf("InspectRecovery: %v", err)
	}
	if !rep.NeedsPermit {
		t.Fatal("mem 档 + 世代变了 + 无许可，NeedsPermit 应为 true")
	}
	if rep.Reason == "" {
		t.Fatal("Reason 为空——它是报告要打给运维看的主体")
	}
	if len(rep.Groups) != 4 {
		t.Fatalf("Groups 长度 = %d; want 4（组 0..3）", len(rep.Groups))
	}

	// 世代未变时不需要签字，命令必须明说，而不是闷头写一个永不被消费的许可
	rep2, err := InspectRecovery(st, 3, AckQuorumMem, func() (string, error) { return "gen-a", nil }, testSlog(t))
	if err != nil {
		t.Fatalf("InspectRecovery: %v", err)
	}
	if rep2.NeedsPermit {
		t.Fatal("世代未变时 NeedsPermit 应为 false")
	}
}

// TestNeedsTermBump 抬 term 的适用范围——按 spec §3.3 的表逐格覆盖。
//
// 判据是「投票记录是不是同步落盘的」，不是「机器有没有重启」：
// fsync 档跟随 MustSync，term/vote 每次变更都已 fsync，投票不可能丢；
// mem 档走 NoSync 异步路径，commit 返回时可能还在进程内缓冲。
func TestNeedsTermBump(t *testing.T) {
	cases := []struct {
		path recoveryPath
		mode AckMode
		want bool
	}{
		{pathLocalResume, AckQuorumMem, true},
		{pathLocalResume, AckQuorumFsync, false},
		{pathLocalForced, AckQuorumMem, true},
		{pathCleanResume, AckQuorumMem, false},
		{pathCleanResume, AckQuorumFsync, false},
		{pathFresh, AckQuorumMem, false},
		{pathRejoin, AckQuorumMem, false},
	}
	for _, c := range cases {
		if got := needsTermBump(c.path, c.mode); got != c.want {
			t.Fatalf("needsTermBump(%v, %v) = %v; want %v", c.path, c.mode, got, c.want)
		}
	}
}
