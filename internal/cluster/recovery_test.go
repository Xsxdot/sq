package cluster

import (
	"os"
	"path/filepath"
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

// TestInspectRecoveryLeavesNoSeglogArtifacts 全新数据目录上跑一次
// `sq recover`，磁盘必须原样不动。
//
// 这条断言守的是三个缺陷连成的死循环：InspectRecovery 逐组 Load →
// getLog → seglog.Open 会 MkdirAll + O_CREATE，一次只读诊断就给每个组
// 留下一个 0 字节的 00000001.seg；而 diskHasRaftState 只看 `.seg` 后缀，
// 于是下次启动这台全新节点会被判成「曾参与集群」→ 不干净关机 →
// ErrUncleanShutdown 拒启；再跑 `sq recover` 又报 fresh、`--grant` 拒绝
// 写许可——诊断命令自己造出一个自己解不开的死结。
func TestInspectRecoveryLeavesNoSeglogArtifacts(t *testing.T) {
	dir := t.TempDir()
	st := mustOpenStore(t, dir)
	rs := newRaftStore(st, testSlog(t))
	if err := rs.EnsureGroups(3); err != nil {
		t.Fatalf("EnsureGroups: %v", err)
	}

	rep, err := InspectRecovery(st, 3, AckQuorumMem, func() (string, error) { return "gen-a", nil }, testSlog(t))
	if err != nil {
		t.Fatalf("InspectRecovery: %v", err)
	}
	if rep.Path != pathFresh.String() {
		t.Fatalf("全新数据目录的判定 = %s; want %s", rep.Path, pathFresh.String())
	}

	raftlogDir := filepath.Join(st.Dir(), "raftlog")
	if _, err := os.Stat(raftlogDir); !os.IsNotExist(err) {
		var names []string
		if fis, rerr := os.ReadDir(raftlogDir); rerr == nil {
			for _, fi := range fis {
				names = append(names, fi.Name())
			}
		}
		t.Fatalf("InspectRecovery 在全新数据目录里创建了 %s（子项 %v，stat err=%v）"+
			"——只读契约被破坏：一次诊断就把节点变成「曾参与集群」", raftlogDir, names, err)
	}

	// 只读判定必须与进程侧同源：此刻 diskHasRaftState 也应为假
	m := &Manager{st: st, rs: rs, dataGroups: 3}
	has, err := m.diskHasRaftState()
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("InspectRecovery 之后 diskHasRaftState 变成了 true——命令与进程结论已经分家")
	}
}

// TestDiskHasRaftStateIgnoresEmptySegFile 0 字节段文件不是 raft 状态。
//
// 空文件里没有任何一条 HardState/Entry 帧，seglog.Open 扫它得到的是空
// 日志。把它算作「曾参与集群」，等于让任何一次创建文件失败/被打断的
// 残留（或上一版 InspectRecovery 留下的诊断产物）永久拒绝掉一个本可以
// 引导启动的全新节点。判据必须是「有内容」，不是「有文件名」。
func TestDiskHasRaftStateIgnoresEmptySegFile(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	segDir := filepath.Join(st.Dir(), "raftlog", "1")
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(segDir, "00000001.seg"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	m := &Manager{st: st, rs: rs, dataGroups: 1}
	has, err := m.diskHasRaftState()
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("raftlog/1 下只有一个 0 字节段文件，diskHasRaftState 必须为 false——" +
			"否则全新节点会被空文件锁死在 ErrUncleanShutdown 上")
	}
}

// TestInspectRecoveryDetectsSeglogOnlyState 只在 seglog 里有条目、既无
// legacy 键也没推进 applied 的组，InspectRecovery 必须判定为「盘上已有
// raft 状态」。
//
// 与 TestDiskHasRaftStateDetectsSeglogOnlyState 是同一个缺口的两侧：
// manager.go 的 diskHasRaftState 已经补了 seglog 这一支，recovery.go 的
// hasRaft 还停在 `applied != 0 || !IsEmptyHardState(hs)`。两处判据不同源，
// 就会出现「命令说你不用签字、进程说你要签字」——文件头注释里承诺的
// 恒等一致被破坏。
func TestInspectRecoveryDetectsSeglogOnlyState(t *testing.T) {
	dir := t.TempDir()
	st := mustOpenStore(t, dir)
	rs := newRaftStore(st, testSlog(t))
	if err := rs.EnsureGroups(1); err != nil {
		t.Fatalf("EnsureGroups: %v", err)
	}
	// 只写条目、不写 HardState：raft 在 follower 首次收到日志时就是这个
	// 形态（HardState 可能同轮无变更）。applied 也仍是 0。
	idx, trm, typ := uint64(1), uint64(1), raftpb.EntryNormal
	if err := rs.Persist(1, nil, []*raftpb.Entry{{Index: &idx, Term: &trm, Type: &typ, Data: []byte("x")}}, true); err != nil {
		t.Fatalf("造 seglog 条目: %v", err)
	}
	rs.CloseLogs()

	// 前置核验：legacy 键与 applied 都不该命中，否则测试没打到目标缺口
	if _, ok, err := st.Get(hsKey(1)); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("前置失败：Persist 不该写 legacy hsKey(1)")
	}
	if app, err := rs.Applied(0); err != nil {
		t.Fatal(err)
	} else if app != 0 {
		t.Fatalf("前置失败：applied 应仍为 0，got %d", app)
	}

	rep, err := InspectRecovery(st, 1, AckQuorumMem, func() (string, error) { return "gen-a", nil }, testSlog(t))
	if err != nil {
		t.Fatalf("InspectRecovery: %v", err)
	}
	if rep.Path == pathFresh.String() {
		t.Fatal("组 1 在 seglog 里有真实条目，InspectRecovery 却判成 fresh——" +
			"hasRaft 缺了 seglog 那一支，与 diskHasRaftState 不同源")
	}

	// 进程侧同判据交叉验证（实现独立，结论必须一致）
	m := &Manager{st: st, rs: newRaftStore(st, testSlog(t)), dataGroups: 1}
	has, err := m.diskHasRaftState()
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("前置失败：diskHasRaftState 应为 true")
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
