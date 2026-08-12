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

// storageAppendAt 构造一条携带单条日志的 MsgStorageAppend。
//
// mustSync 对应 raft 本轮的 MustSync 判定（fsync 档下才有意义）；
// Responses 留空——本文件里用它的两个用例只关心落盘与 fsync 次数。
func storageAppendAt(index, term uint64, mustSync bool) localMsg {
	typ := raftpb.MsgStorageAppend
	to := raft.LocalAppendThread
	idx, tm := index, term
	etyp := raftpb.EntryNormal
	return localMsg{
		m: &raftpb.Message{Type: &typ, To: &to, Entries: []*raftpb.Entry{
			{Index: &idx, Term: &tm, Type: &etyp, Data: []byte("x")},
		}},
		mustSync: mustSync,
	}
}

// TestAppendBatchSingleFsync append 合批的核心承诺：一批抽干的
// MsgStorageAppend 合成**恰好一次** fsync。
//
// 为什么这条是整个改造的验收锚：2026-08-12 三机实测证明，异步化本身
// 是负收益——同步路径下「主循环阻塞在 fsync 里」本就是一次隐式 group
// commit，异步化把它拆掉后，同样的提案流被切得更碎、一条一 fsync，
// 摊薄比反而从 4.11 条/次跌到 1.60。本用例守的就是「批内只落一次盘」，
// 改回逐条 sync 会让 fsync 计数变成 8。
func TestAppendBatchSingleFsync(t *testing.T) {
	gr, _, _ := newApplyTestGroup(t, nil)
	gr.mode = AckQuorumFsync

	var batch []localMsg
	for i := uint64(1); i <= 8; i++ {
		batch = append(batch, storageAppendAt(i, 1, true))
	}
	gr.appendBatch(context.Background(), batch)

	if got := gr.syncCount.Load(); got != 1 {
		t.Fatalf("8 条 append 产生 %d 次 fsync，合批要求恰好 1 次", got)
	}
	if got := gr.appendCount.Load(); got != 8 {
		t.Fatalf("appendCount = %d; want 8（合批不能吞掉逐条计数）", got)
	}
	// 落盘内容不能因为合批而少写：8 条都要在 MemoryStorage 里
	last, err := gr.mem.LastIndex()
	if err != nil || last != 8 {
		t.Fatalf("mem.LastIndex = %d, %v; want 8, nil", last, err)
	}
}

// TestAppendBatchNoFsyncWhenNoneRequires 合批不得凭空制造 fsync：
// 批内没有任何一条要求同步时，一次盘都不能落。
//
// 这条守的是 2026-08-08 那次优化的成果——「只有 commit 前进、没有新日志」
// 的轮次本来就不该 fsync。用「批非空」代替「批内有 MustSync」做判据，
// 会把这类轮次重新拖回落盘，本用例即刻变红。
func TestAppendBatchNoFsyncWhenNoneRequires(t *testing.T) {
	gr, _, _ := newApplyTestGroup(t, nil)
	gr.mode = AckQuorumFsync

	var batch []localMsg
	for i := uint64(1); i <= 4; i++ {
		batch = append(batch, storageAppendAt(i, 1, false))
	}
	gr.appendBatch(context.Background(), batch)

	if got := gr.syncCount.Load(); got != 0 {
		t.Fatalf("批内无一条要求同步，却落了 %d 次盘; want 0", got)
	}
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
	// entered 在安装真正进入临界区（已持 installMu）后关闭。
	//
	// 必须有这个握手：control 回调是在 installMu 之内被调用的，而下面的
	// apply 协程只要抢在安装拿到锁之前跑到 TryLock，就会合法地一路跑完，
	// 「安装期间 apply 必须让路」的断言当场误报。这不是实现的问题，是
	// 用例自己的启动顺序竞态——实测约 1/10 概率红。等到 entered 之后再
	// 启动 apply，顺序就确定了，断言本身一字未改、判别力不变。
	entered := make(chan struct{})
	var enteredOnce sync.Once
	gr.control = func(ctx context.Context, nodeID uint64, op byte, payload []byte) ([]byte, error) {
		enteredOnce.Do(func() { close(entered) })
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

	// 等安装真正进了临界区再启动 apply（见 entered 的注释）
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("安装未在 10s 内进入临界区")
	}

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

// TestOutboundNotBlockedByPersist 本改造的收益命题，落成一条可证伪的
// 用例：一轮 Ready 里的网络消息必须在本地 append 消息**入队之前**就已外发。
//
// 旧路径（同步 Ready）里这是不可能的——持久化在 group.go:396、外发在
// :417，leader 做 fsync 的那 1.8ms 里 MsgApp 一个字节都发不出去，确认链
// 于是变成「leader fsync → 网络 → follower fsync」两次串行相加，这正是
// quorum-fsync 档 +69% raft 机制税的来源。
//
// 本用例不测吞吐（那是三机实测的事），只测**顺序**：顺序对了，流水线
// 深度才有可能 > 1。别拿它当性能测试误判。
//
// 三条形态约束，缺一条判别力就没了：
//  1. 为什么先灌满 appendCh：通道容量是 64（localQueueDepth），不满的话
//     即便有人把 dispatchReady 改成「先入队、再外发」，入队进一个有空位
//     的通道也不会阻塞，sent 照样立刻到达，用例抓不住顺序颠倒。灌满后
//     入队必然阻塞，外发若排在入队之后，sent 永远等不到，用例才真的红。
//  2. 为什么不能有消费者：任何从 appendCh 收消息的 goroutine 都会腾出
//     位置，阻塞中的入队随即成功、外发照常执行——判别条件当场失效
//     （上一版「取一条卡 gate 模拟慢 fsync」的写法实测对调顺序后仍然
//     全绿，缺陷就在这里）。整个 dispatchReady 期间通道必须保持满，
//     谁也不许收。后人若想补回一个「更逼真」的消费者，请先读这段。
//  3. 为什么 dispatchReady 要在独立 goroutine 里调、且收工靠 cancel：
//     顺序正确时 send 先执行、紧接着 enqueueLocal 就阻塞在满通道上不
//     返回——主协程直调根本走不到断言行，正确实现反而红。阻塞中的
//     enqueueLocal 只有 ctx 取消这一个出口，因此断言无论成败都要
//     cancel 并等 done，不留悬挂协程。
func TestOutboundNotBlockedByPersist(t *testing.T) {
	gr, _, _ := newApplyTestGroup(t, nil)
	sent := make(chan struct{}, 1) // 有缓冲：send 发生时主协程还没在收
	gr.send = func(uint32, []*raftpb.Message) { sent <- struct{}{} }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 灌满 appendCh，且不起任何消费者（形态约束 1、2）
	for i := 0; i < cap(gr.appendCh); i++ {
		gr.appendCh <- localMsg{m: msgTo(raft.LocalAppendThread, raftpb.MsgStorageAppend)}
	}

	appendTyp, appendTo := raftpb.MsgStorageAppend, raft.LocalAppendThread
	rd := raft.Ready{MustSync: true, Messages: []*raftpb.Message{
		msgTo(2, raftpb.MsgApp),
		{Type: &appendTyp, To: &appendTo},
	}}

	done := make(chan struct{})
	go func() { defer close(done); gr.dispatchReady(ctx, rd) }()
	// 收工保障（形态约束 3）：t.Fatal 走 Goexit，只有 defer 能在失败路径
	// 上也放行阻塞中的 enqueueLocal 并等 dispatchReady 返回，不留悬挂协程
	defer func() { cancel(); <-done }()

	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("持久化入队已阻塞、MsgApp 却还没发出——外发被排在了本地写入之后，流水线深度仍是 1")
	}
}
