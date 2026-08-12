// group_async_test.go 覆盖 raft 异步存储写入（AsyncStorageWrites）改造
// 引入的三条新契约。
//
// 职责：
//   - 分发路由：MsgStorageAppend/MsgStorageApply 进本地通道，其余走 send
//   - 响应投递：自指响应必须经 rn.Step 可靠投递，绝不经 gr.send（会丢）
//   - MustSync 载体：fsync 档的同步判定随消息配对传递，语义与旧路径一致
//
// 边界：
//   - 不覆盖多节点选举与消息语义（cluster_test.go 的范围）
//   - 不覆盖快照安装与 apply 互斥（Task 3 的 group_snapinstall 用例）
//   - 复用 group_test.go 的 openClusterTestStore/testSlog/newApplyTestGroup
package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// msgTo 构造一条只关心 To 字段的消息（分发路由测试用）。
func msgTo(to uint64, typ raftpb.MessageType) *raftpb.Message {
	t := typ
	dst := to
	return &raftpb.Message{Type: &t, To: &dst}
}

// storageAppendWithSnap 构造一条携带快照的 MsgStorageAppend。
//
// async 契约下快照随本地 append 消息到达（raft/v3@v3.7.0/rawnode.go:242-244
// 的 newStorageAppendMsg），不再出现在 Ready.Snapshot 的消费路径上。
func storageAppendWithSnap(snap *raftpb.Snapshot) localMsg {
	typ := raftpb.MsgStorageAppend
	to := raft.LocalAppendThread
	return localMsg{m: &raftpb.Message{Type: &typ, To: &to, Snapshot: snap}}
}

// TestDispatchReadyRoutesByTarget 分发的核心契约：本地存储消息按 m.To
// 进两条本地通道，其余消息才交给 send 外发。
//
// 为什么这条必须有用例：走错一路的后果是**组静默卡死而非报错**——
// gr.send 对自指消息走 gr.step（inbox 满则丢、安装期显式丢弃），丢一条
// MsgStorageAppend 就是 raft 永远等不到的那条 MsgStorageAppendResp。
func TestDispatchReadyRoutesByTarget(t *testing.T) {
	gr, _, _ := newApplyTestGroup(t, nil)
	var sent []*raftpb.Message
	gr.send = func(g uint32, msgs []*raftpb.Message) { sent = append(sent, msgs...) }

	rd := raft.Ready{Messages: []*raftpb.Message{
		msgTo(2, raftpb.MsgApp),
		msgTo(raft.LocalAppendThread, raftpb.MsgStorageAppend),
		msgTo(raft.LocalApplyThread, raftpb.MsgStorageApply),
		msgTo(3, raftpb.MsgHeartbeat),
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gr.dispatchReady(ctx, rd)

	if len(sent) != 2 {
		t.Fatalf("只有 2 条网络消息该外发，实际 %d 条——本地存储消息漏进了 send（丢一条即静默卡死）", len(sent))
	}
	for _, m := range sent {
		if raft.IsLocalMsgTarget(m.GetTo()) {
			t.Fatalf("本地存储消息 %s 被交给了 send，必须走本地通道", m.GetType().String())
		}
	}
	select {
	case lm := <-gr.appendCh:
		if lm.m.GetType() != raftpb.MsgStorageAppend {
			t.Fatalf("append 通道收到的应是 MsgStorageAppend，得到 %s", lm.m.GetType().String())
		}
	default:
		t.Fatal("MsgStorageAppend 没有进 append 通道")
	}
	select {
	case lm := <-gr.applyCh:
		if lm.m.GetType() != raftpb.MsgStorageApply {
			t.Fatalf("apply 通道收到的应是 MsgStorageApply，得到 %s", lm.m.GetType().String())
		}
	default:
		t.Fatal("MsgStorageApply 没有进 apply 通道")
	}
}

// TestDispatchReadyCarriesMustSync fsync 档的同步判定必须随 append
// 消息配对传递——async 后 MustSync 不再能在写入点现场读到 Ready。
//
// 语义锚：判定输入换了载体，判定本身（mode==Fsync && MustSync）不变。
// 若实现改成「有 Responses 就 sync」，本用例会失败——那会把 commit-only
// 轮也 fsync，退回 2026-08-08 优化之前的每提案两次盘。
func TestDispatchReadyCarriesMustSync(t *testing.T) {
	gr, _, _ := newApplyTestGroup(t, nil)
	gr.send = func(uint32, []*raftpb.Message) {}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, mustSync := range []bool{true, false} {
		gr.dispatchReady(ctx, raft.Ready{
			MustSync: mustSync,
			Messages: []*raftpb.Message{msgTo(raft.LocalAppendThread, raftpb.MsgStorageAppend)},
		})
		select {
		case lm := <-gr.appendCh:
			if lm.mustSync != mustSync {
				t.Fatalf("MustSync=%v 必须随消息传到 append 阶段，得到 %v", mustSync, lm.mustSync)
			}
		case <-time.After(time.Second):
			t.Fatal("append 通道没收到消息")
		}
	}
}

// TestSnapshotInstallExcludesApply 快照安装期间 apply 阶段必须让路。
//
// 为什么这条是 async 独有的：旧路径里安装与 apply 都在 Ready 循环内串行
// 执行，物理上不可能重叠；async 之后它们是两条协程，raft 也明确允许不同
// target 的本地消息乱序处理。若不互斥，一批安装前入队的 MsgStorageApply
// 会在 wipeGroupKeys 之后落地，把陈旧数据写回刚被快照覆盖的键——静默的
// 状态机分叉，没有任何报错。
//
// 断言两件事：
//  1. 安装未完成时 applyOnce 不得推进（互斥真的生效）；
//  2. 安装完成后那批陈旧条目被整批跳过（index ≤ applied 的既有守卫兜住），
//     FSM 里不留任何陈旧键。
func TestSnapshotInstallExcludesApply(t *testing.T) {
	st := openClusterTestStore(t)
	rs := newRaftStore(st, testSlog(t))
	gr := newGroup(0, 1, raft.NewMemoryStorage(), nil, rs, st,
		func(uint32, []*raftpb.Message) {}, AckQuorumMem, nil, nil, testSlog(t))
	rn := raft.StartNode(raftConfig(1, gr.stg), []raft.Peer{{ID: 1}})
	gr.rn = rn
	defer rn.Stop()

	// 拉块回调阻塞在 gate 上：安装卡在第 4 步，安装临界区一直持有
	gate := make(chan struct{})
	// 收尾兜底（defer 在安装协程启动后注册）：失败路径 t.Fatal 会跳过
	// 下方的放行，必须先放 gate 并等安装协程收工，否则 t.Cleanup 关闭
	// store 与安装协程竞争（pebble: closed panic 会掩盖真正的断言失败）。
	var gateOnce sync.Once
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		<-gate
		// 一块即完成：done=true，游标无意义，块内无键
		// （snapFetchResp 是 group_test.go 的既有拼包助手）
		return snapFetchResp(true, nil, nil), nil
	}

	idx, tm := uint64(100), uint64(2)
	snap := &raftpb.Snapshot{
		Data: encodeSnapDescriptor(snapDescriptor{ID: 7, Leader: 1, Index: idx}),
		Metadata: &raftpb.SnapshotMetadata{
			Index: &idx, Term: &tm, ConfState: &raftpb.ConfState{},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	installDone := make(chan struct{})
	go func() {
		defer close(installDone)
		gr.appendOnce(ctx, storageAppendWithSnap(snap))
	}()
	defer func() {
		gateOnce.Do(func() { close(gate) })
		<-installDone
	}()

	// 陈旧条目：index 远低于快照位点，安装完成后必须被整批跳过
	stale := []*raftpb.Entry{normalEntry(t, st, 5, 1, 101, "meta/topic/stale", "old")}
	applyTyp, applyTo := raftpb.MsgStorageApply, raft.LocalApplyThread
	applyMsg := &raftpb.Message{Type: &applyTyp, To: &applyTo, Entries: stale}

	applyDone := make(chan struct{})
	go func() {
		defer close(applyDone)
		gr.applyOnce(ctx, applyMsg)
	}()

	// 安装被 gate 卡住期间，apply 必须进不去
	select {
	case <-applyDone:
		t.Fatal("安装未完成时 apply 就跑完了——两条阶段没有互斥，陈旧条目会覆盖快照数据")
	case <-time.After(200 * time.Millisecond):
	}

	gateOnce.Do(func() { close(gate) }) // 放行安装
	select {
	case <-installDone:
	case <-time.After(10 * time.Second):
		t.Fatal("快照安装未在 10s 内完成")
	}
	select {
	case <-applyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("安装完成后 apply 仍未放行——互斥没有释放")
	}

	if got := gr.applied.Load(); got != idx {
		t.Fatalf("applied 应停在快照位点 %d，得到 %d——陈旧条目推进了位点", idx, got)
	}
	v, ok, err := st.Get([]byte("meta/topic/stale"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("陈旧条目不该落库（快照已覆盖该组状态），读到 %q", v)
	}
}
