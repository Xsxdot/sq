package cluster

import "testing"

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
