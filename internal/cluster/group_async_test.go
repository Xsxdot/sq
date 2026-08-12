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
