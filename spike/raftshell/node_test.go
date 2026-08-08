// node_test.go 验证 raftshell 薄壳的最小闭环。
//
// 职责：单节点自选举 + 提案 commit/apply 全链路，覆盖两档刷盘模式。
// 边界：不覆盖多节点选举、分区与消息语义（Task 3 范围）。
package raftshell

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
)

// testLogger 返回写往测试输出的 slog，便于失败时直接看到节点日志。
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// TestSingleNodeProposeApply 单节点集群自选举后，100 条提案全部 commit 并 apply。
// 这是薄壳最小闭环：tick 驱动、Ready 契约（先持久化后发送）、FSM apply。
func TestSingleNodeProposeApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var n *Node
	// 先注册等待、后注册 cancel：LIFO 保证 cancel 先于等待执行。
	// 否则等待会在 ctx 取消前启动，节点永远不会退出。
	defer func() {
		if n == nil {
			return // NewNode 失败提前退出，无需等待
		}
		select {
		case <-n.Done():
		case <-time.After(5 * time.Second):
			t.Error("node did not shut down within 5s")
		}
	}()
	defer cancel()
	tr := newChanTransport()
	var err error
	n, err = NewNode(1, []raft.Peer{{ID: 1}}, t.TempDir(), AckQuorumFsync, tr, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	tr.register(1, n)
	n.Start(ctx)
	for i := 0; i < 100; i++ {
		if err := n.Propose(ctx, []byte(fmt.Sprintf("msg-%03d", i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	if got := n.AppliedCount(); got != 100 {
		t.Fatalf("applied = %d, want 100", got)
	}
}

// TestSingleNodeProposeApplyMem 与 Fsync 档同构，但走 AckQuorumMem：
// NoSync 批量提交 + 后台 200ms 周期 fsync。验证两档刷盘路径都闭合，
// 且后台刷盘 goroutine 在节点退出时正常停止（Close 不挂起）。
func TestSingleNodeProposeApplyMem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var n *Node
	defer func() {
		if n == nil {
			return // NewNode 失败提前退出，无需等待
		}
		select {
		case <-n.Done():
		case <-time.After(5 * time.Second):
			t.Error("node did not shut down within 5s")
		}
	}()
	defer cancel()
	tr := newChanTransport()
	var err error
	n, err = NewNode(1, []raft.Peer{{ID: 1}}, t.TempDir(), AckQuorumMem, tr, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	tr.register(1, n)
	n.Start(ctx)
	for i := 0; i < 50; i++ {
		if err := n.Propose(ctx, []byte(fmt.Sprintf("msg-%03d", i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	if got := n.AppliedCount(); got != 50 {
		t.Fatalf("applied = %d, want 50", got)
	}
}
