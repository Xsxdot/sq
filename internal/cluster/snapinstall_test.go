// snapinstall_test.go 验证快照安装中标记与清空重来工具。
//
// 职责：
//   - 安装中标记必须跨重启可见（崩溃 = 半截状态，重启须清空重来，
//     绝不能把半截状态当完整状态启动）
//   - wipeGroupKeys 只能清自己组的键——共享 store 里误清别组 =
//     把无辜的组也打成需要快照
//   - ResetGroupProgress 把组进度整体重置（applied=0、锚点与标记删除）
//
// 边界：不覆盖 installSnapshot 完整流程与端到端追平（Task 7 part B）；
// openClusterTestStore/testSlog 复用 raftstore_test.go。
package cluster

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/xushixin/sq/internal/store"
)

// TestInstallingMarkerSurvivesCrash 安装中崩溃 = 半截状态。
// 重启必须判定该组不完整并清空重来，绝不能把半截状态当完整状态启动。
func TestInstallingMarkerSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir, false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	rs := newRaftStore(st, testSlog(t))
	idx, term := uint64(88), uint64(4)
	if err := rs.MarkInstalling(1, &raftpb.SnapshotMetadata{Index: &idx, Term: &term}); err != nil {
		t.Fatal(err)
	}
	// 写入「半截」状态
	b := st.NewBatch()
	if err := b.Set(store.MsgKey("T", 0, 1), []byte("half")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := store.Open(dir, false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	rs2 := newRaftStore(st2, testSlog(t))
	meta, ok, err := rs2.LoadInstalling(1)
	if err != nil || !ok {
		t.Fatalf("安装中标记必须在重启后可见 ok=%v err=%v", ok, err)
	}
	if meta.GetIndex() != 88 {
		t.Fatalf("标记 index = %d; want 88", meta.GetIndex())
	}
}

// TestWipeGroupKeysOnlyTouchesOwnGroup 清空重来只能清自己组的键——
// 共享 store 里误清别组 = 把无辜的组也打成需要快照。
func TestWipeGroupKeysOnlyTouchesOwnGroup(t *testing.T) {
	st := openClusterTestStore(t)
	const groups = uint32(3)
	var mine, others [][]byte
	b := st.NewBatch()
	for q := uint32(0); q < 30; q++ {
		k := store.MsgKey("T", q, 1)
		if groupForQueue("T", q, groups) == 1 {
			mine = append(mine, k)
		} else {
			others = append(others, k)
		}
		if err := b.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Set(store.TopicMetaKey("T"), []byte("m")); err != nil { // 组 0 的键
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	if len(mine) == 0 || len(others) == 0 {
		t.Fatal("测试前提不成立：组 1 与其它组都要有键")
	}
	if err := wipeGroupKeys(st, 1, groups); err != nil {
		t.Fatal(err)
	}
	for _, k := range mine {
		if _, ok, _ := st.Get(k); ok {
			t.Fatalf("组 1 的键 %q 未被清除", k)
		}
	}
	for _, k := range others {
		if _, ok, _ := st.Get(k); !ok {
			t.Fatalf("别组的键 %q 被误清", k)
		}
	}
	if _, ok, _ := st.Get(store.TopicMetaKey("T")); !ok {
		t.Fatal("组 0 的全局键被误清")
	}
}

// TestWipeGroupKeysGroup0ClearsGlobalKeys 组 0 是全局连续前缀键族，
// DeleteRange 整段删——全局键必须清干净，数据组键必须原样保留。
func TestWipeGroupKeysGroup0ClearsGlobalKeys(t *testing.T) {
	st := openClusterTestStore(t)
	const groups = uint32(3)
	global := [][]byte{
		store.TopicMetaKey("T"), store.DelayKey(1000, 1), store.DelayAllocKey(),
		store.HalfKey(1000, "tx1"), store.HalfIdxKey("tx1"),
	}
	b := st.NewBatch()
	for _, k := range global {
		if err := b.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// 数据组键（msg/）不属于组 0，必须原样保留
	dataKey := store.MsgKey("T", 0, 1)
	if err := b.Set(dataKey, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	if err := wipeGroupKeys(st, 0, groups); err != nil {
		t.Fatal(err)
	}
	for _, k := range global {
		if _, ok, _ := st.Get(k); ok {
			t.Fatalf("组 0 的全局键 %q 未被清除", k)
		}
	}
	if _, ok, _ := st.Get(dataKey); !ok {
		t.Fatal("数据组键被组 0 清空误删")
	}
}

// TestResetGroupProgressClearsAnchorAndMarker 清空重来后组进度必须整体
// 归零：applied=0（raft 从 1 重投递）、快照锚点删除、安装中标记删除——
// 残留任何一个都会让重启路径把半截状态当完整状态。
func TestResetGroupProgressClearsAnchorAndMarker(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	idx, term := uint64(10), uint64(2)
	if err := rs.SaveSnapMeta(1, &raftpb.SnapshotMetadata{Index: &idx, Term: &term}); err != nil {
		t.Fatal(err)
	}
	b := st.NewBatch()
	if err := b.Set(appliedKey(1), store.PutU64(42)); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
	if err := rs.MarkInstalling(1, &raftpb.SnapshotMetadata{Index: &idx, Term: &term}); err != nil {
		t.Fatal(err)
	}
	if err := rs.ResetGroupProgress(1); err != nil {
		t.Fatal(err)
	}
	if ap, err := rs.Applied(1); err != nil || ap != 0 {
		t.Fatalf("applied = %d err=%v; want 0", ap, err)
	}
	if _, ok, _ := rs.LoadSnapMeta(1); ok {
		t.Fatal("快照锚点应被删除")
	}
	if _, ok, _ := rs.LoadInstalling(1); ok {
		t.Fatal("安装中标记应被删除")
	}
}
