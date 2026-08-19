// 复制层测试：单机后端直通语义、集群后端转发线路（solo Manager）与
// 编译期接口满足。
//
// 职责：
//   - 验证 Standalone.Apply/ApplyAsync 与 store 等价（apply 即可读，
//     Wait 后可见，忽略 group）
//   - 用单节点真实 Manager + 假 ControlHandler 验证 Cluster 转发原语的
//     载荷编码与响应解析（线路层；真 produce 栈接线在 Task 11 e2e）
//   - 编译期断言各后端/视图满足全部接口
//
// 边界：
//   - 不测集群转发收敛——三节点集成测试在 cluster_test.go（本包反向
//     import cluster 合法，但三节点 harness 的未导出原语过不来，
//     收敛语义由那边经同语义 shim 覆盖）
package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/store"
)

func openReplTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestStandaloneApplyPassthrough 单机后端 = 今天的路径：apply 即可读，
// 忽略 group 参数。
func TestStandaloneApplyPassthrough(t *testing.T) {
	st := openReplTestStore(t)
	r := NewStandalone(st)
	b := st.NewBatch()
	_ = b.Set([]byte("k"), []byte("v"))
	if err := r.Apply(context.Background(), 42, b); err != nil { // group 随便传
		t.Fatal(err)
	}
	if _, ok, _ := st.Get([]byte("k")); !ok {
		t.Fatal("单机后端 apply 后不可读")
	}
}

// TestStandaloneApplyAsync 单机档 ApplyAsync = store.ApplyAsync 直通
// （group commit 合并 fsync 的既有机制）：Wait 后写入可见。
func TestStandaloneApplyAsync(t *testing.T) {
	st := openReplTestStore(t)
	r := NewStandalone(st)
	b := st.NewBatch()
	_ = b.Set([]byte("meta/topic/x"), []byte("v"))
	p, err := r.ApplyAsync(context.Background(), 0, b)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.Get([]byte("meta/topic/x")); !ok {
		t.Fatal("写入不可见")
	}
}

// TestClusterForwarderWire 用单节点真实 Manager + 假 ControlHandler 验证
// Cluster 转发原语的线路层：ForwardAppend 的载荷编码（[4B BE g][msgRaw]）
// 与响应解析（[4B BE queueID][8B BE offset]）、ForwardApply 的载荷编码
// （[4B BE g][repr]）。真 produce 栈接线在 Task 11 e2e 覆盖；跨节点收敛
// 语义由 cluster_test.go 的 ForwardApply 收敛测试承担。
//
// 单节点 Manager 自选举为全部组 leader，「自己就是 leader 属编程错误」
// 的约束在此刻意绕过——转发目标即本节点，经 Control 自拨号回环到
// 自己的 ControlHandler，线路真实性不受影响（Control 不走 raft 消息流，
// 与对端是谁无关）。
func TestClusterForwarderWire(t *testing.T) {
	st, err := store.Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// 假 produce 栈：收到请求即回坐标/确认。handler 契约要求并发安全
	//（控制通道多连接并发调用）——通道缓冲足够且只被本测试驱动。
	got := make(chan struct {
		op      byte
		payload []byte
	}, 4)
	handler := func(op byte, payload []byte) ([]byte, error) {
		got <- struct {
			op      byte
			payload []byte
		}{op, append([]byte(nil), payload...)}
		switch op {
		case cluster.OpForwardAppend:
			if len(payload) < 4 {
				return nil, fmt.Errorf("ForwardAppend 载荷过短: %d", len(payload))
			}
			resp := make([]byte, 12)
			binary.BigEndian.PutUint32(resp[:4], 7)
			binary.BigEndian.PutUint64(resp[4:], 42)
			return resp, nil
		case cluster.OpForwardApply:
			return nil, nil
		default:
			return nil, fmt.Errorf("unexpected op %d", op)
		}
	}
	m, err := cluster.NewManager(cluster.Options{
		NodeID:         1,
		Peers:          map[uint64]string{1: ln.Addr().String()}, // 真实地址：Control 自拨号按 Peers 寻址
		Listener:       ln,
		Mode:           cluster.AckQuorumMem,
		Store:          st,
		Logger:         slog.Default(),
		ControlHandler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		if err := m.StopClean(context.Background()); err != nil {
			t.Logf("清理: StopClean: %v", err)
		}
		select {
		case <-m.Done():
		case <-time.After(10 * time.Second):
			t.Error("manager 未在 10s 内完全退出")
		}
	})
	// 等自选举：Leader(1) 就绪（单节点约 1s）
	deadline := time.Now().Add(30 * time.Second)
	for {
		if lead, ok := m.Leader(1); ok && lead == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("单节点组未在 30s 内自选举为 leader")
		}
		time.Sleep(50 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r := NewCluster(m)

	// ForwardAppend：载荷 [4B BE g=1][EncodeMessage 字节]，响应坐标解析
	raw, err := core.EncodeMessage(&core.Message{ID: "FWD1", Topic: "orders", Body: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	qid, off, err := r.ForwardAppend(ctx, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if qid != 7 || off != 42 {
		t.Fatalf("坐标 qid=%d off=%d; want 7/42", qid, off)
	}
	evt := <-got
	want := make([]byte, 4)
	binary.BigEndian.PutUint32(want, 1)
	want = append(want, raw...)
	if evt.op != cluster.OpForwardAppend || !bytes.Equal(evt.payload, want) {
		t.Fatalf("handler 收到 op=%d payload=%v; want op=%d payload=%v", evt.op, evt.payload, cluster.OpForwardAppend, want)
	}

	// ForwardApply：载荷 [4B BE g=1][repr]，响应空
	repr := []byte("构造无关批次的 repr")
	if err := r.ForwardApply(ctx, 1, repr); err != nil {
		t.Fatal(err)
	}
	evt = <-got
	want = append(want[:4], repr...)
	if evt.op != cluster.OpForwardApply || !bytes.Equal(evt.payload, want) {
		t.Fatalf("handler 收到 op=%d payload=%v; want op=%d payload=%v", evt.op, evt.payload, cluster.OpForwardApply, want)
	}
}

// TestClusterForwardLeaderUnknownReturnsErrNotLeader 未启动的 Manager 无
// 选举、Leader(g) 无结果：转发原语必须按 ErrNotLeader 报错（上层据此
// 重试），而非返回零值静默假成功。
func TestClusterForwardLeaderUnknownReturnsErrNotLeader(t *testing.T) {
	st, err := store.Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	m, err := cluster.NewManager(cluster.Options{
		NodeID:   1,
		Peers:    map[uint64]string{1: ln.Addr().String()},
		Listener: ln,
		Store:    st,
		Logger:   slog.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// 未 Start：无 run 循环、无选举，Leader(g) 恒无结果
	if _, _, err := (NewCluster(m)).ForwardAppend(context.Background(), 1, nil); !errors.Is(err, cluster.ErrNotLeader) {
		t.Fatalf("leader 未知的 ForwardAppend 应返回 ErrNotLeader，得到: %v", err)
	}
	if err := (NewCluster(m)).ForwardApply(context.Background(), 1, nil); !errors.Is(err, cluster.ErrNotLeader) {
		t.Fatalf("leader 未知的 ForwardApply 应返回 ErrNotLeader，得到: %v", err)
	}
}

// 编译期接口满足：任一实现漏方法都会在此处报错。
var _ Replicator = (*Standalone)(nil)
var _ Replicator = (*Cluster)(nil)
var _ Router = (*StandaloneRouter)(nil)
var _ Router = (*Cluster)(nil)
var _ Forwarder = (*Cluster)(nil)
var _ Pending = pending{}
var _ Pending = (chanPending)(nil)

// fakeLeaderRouter 可编程的 Router：IsLeader 按字段返回（转发分支测试用）。
type fakeLeaderRouter struct{ leader bool }

func (r fakeLeaderRouter) GroupForQueue(string, uint32) uint32       { return 7 }
func (r fakeLeaderRouter) MetaGroup() uint32                         { return 0 }
func (r fakeLeaderRouter) IsLeader(uint32) bool                      { return r.leader }
func (r fakeLeaderRouter) ReadBarrier(context.Context, uint32) error { return nil }

// fakeCaptureForwarder 捕获 ForwardApply 载荷的假转发器。
type fakeCaptureForwarder struct {
	g    uint32
	repr []byte
	err  error
}

func (f *fakeCaptureForwarder) ForwardAppend(context.Context, uint32, []byte) (uint32, uint64, error) {
	return 0, 0, errors.New("未使用")
}

func (f *fakeCaptureForwarder) ForwardApply(_ context.Context, g uint32, repr []byte) error {
	f.g = g
	f.repr = append([]byte(nil), repr...)
	return f.err
}

// TestApplyOrForwardLeaderBranch 本节点是 leader：直通 rep.Apply，批次落地。
func TestApplyOrForwardLeaderBranch(t *testing.T) {
	st := openReplTestStore(t)
	b := st.NewBatch()
	_ = b.Delete([]byte("half/1"))
	if err := ApplyOrForward(context.Background(), NewStandalone(st),
		StandaloneRouter{}, nil, 0, b, slog.Default()); err != nil {
		t.Fatal(err)
	}
	// Standalone.Apply 直通 store：删除应已生效（键不存在）
	if _, ok, _ := st.Get([]byte("half/1")); ok {
		t.Fatal("本地分支删除未生效")
	}
}

// TestApplyOrForwardForwardBranch 非 leader：批次字节（Repr 拷贝）转交给
// fwd.ForwardApply，本节点不落盘——「转发路径内部完成 Repr 拷贝与 Close」的
// 可观察契约：转发器收到完整载荷、本地无写入、失败原样上抛。
func TestApplyOrForwardForwardBranch(t *testing.T) {
	st := openReplTestStore(t)
	fwd := &fakeCaptureForwarder{}
	b := st.NewBatch()
	_ = b.Set([]byte("cursor/g/t/q"), []byte("v"))
	want := append([]byte(nil), b.Repr()...)
	if err := ApplyOrForward(context.Background(), NewStandalone(st),
		fakeLeaderRouter{leader: false}, fwd, 7, b, slog.Default()); err != nil {
		t.Fatal(err)
	}
	if fwd.g != 7 || !bytes.Equal(fwd.repr, want) {
		t.Fatalf("转发器收到 g=%d repr=%v; want g=7 repr=%v", fwd.g, fwd.repr, want)
	}
	// 转发分支本节点不落盘（follower 不 apply）
	if _, ok, _ := st.Get([]byte("cursor/g/t/q")); ok {
		t.Fatal("转发分支不应在本节点落盘")
	}
}

// TestApplyOrForwardForwardBranchError 转发失败必须原样上抛（上层按协议语义
// 重试），不能吞成成功。
func TestApplyOrForwardForwardBranchError(t *testing.T) {
	st := openReplTestStore(t)
	fwd := &fakeCaptureForwarder{err: errors.New("leader 无响应")}
	b := st.NewBatch()
	_ = b.Delete([]byte("half/1"))
	err := ApplyOrForward(context.Background(), NewStandalone(st),
		fakeLeaderRouter{leader: false}, fwd, 7, b, slog.Default())
	if err == nil || !strings.Contains(err.Error(), "leader 无响应") {
		t.Fatalf("转发失败应原样上抛，得到 %v", err)
	}
}
