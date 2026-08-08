// manager_test.go 验证 Manager 多组装配：组路由黄金值、干净/不干净
// 重启两条路径的判定与恢复。
//
// 职责：队列→组映射的入盘契约（黄金值锁死）、单节点干净关机后原身份
// 回归（数据可读、无 ErrUncleanShutdown）、不写标记直接停后 NewManager
// 必须拒绝裸恢复。
// 边界：不覆盖多节点消息路径（batch④ 三节点集成测试）；
// openClusterTestStore/testSlog/testWriter 复用 raftstore_test.go。
package cluster

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/store"
)

// TestGroupForQueueStable 队列→组映射是入盘契约，黄金值锁死：任何改动
// fnv 输入编码的重构都会在这里炸出来，而不是让存量数据错组。
func TestGroupForQueueStable(t *testing.T) {
	m := &Manager{dataGroups: 3}
	golden := []struct {
		topic string
		q     uint32
		want  uint32
	}{
		// 首次生成即冻结，断言三条覆盖：不同 topic 不同组、同 topic
		// 不同 queue 可不同组、组号 ∈ [1,3]
		{"orders", 0, 1}, {"orders", 1, 3}, {"payments", 0, 3},
	}
	for _, g := range golden {
		got := m.GroupForQueue(g.topic, g.q)
		if got != g.want {
			t.Fatalf("GroupForQueue(%s,%d) = %d; want %d——fnv 输入编码被改动", g.topic, g.q, got, g.want)
		}
		if got < 1 || got > 3 {
			t.Fatalf("GroupForQueue(%s,%d) = %d 越界 [1,3]", g.topic, g.q, got)
		}
	}
}

// TestManagerCleanRestartResumes 单节点集群（成员表只有自己）干净关机后
// 重启：applied 恢复、数据可读、无 ErrUncleanShutdown。
func TestManagerCleanRestartResumes(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem) // helper：单成员 Options
	b := st1.NewBatch()
	_ = b.Set([]byte("meta/topic/t1"), []byte("v"))
	repr := append([]byte(nil), b.Repr()...)
	_ = b.Close()
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := m1.Propose(pctx, MetaGroup, repr); err != nil {
		t.Fatal(err)
	}
	if err := m1.StopClean(ctx); err != nil {
		t.Fatal(err)
	}
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}
	st2, m2 := startSoloManager(t, dir, AckQuorumMem) // 同目录重启
	if _, ok, _ := st2.Get([]byte("meta/topic/t1")); !ok {
		t.Fatal("干净重启后数据丢失")
	}
	_ = m2 // 起来即成功；NewManager 未返回 ErrUncleanShutdown 已在 helper 断言
}

// TestManagerUncleanRestartRefusesResume 不写标记直接停（模拟断电）后，
// NewManager 必须返回 ErrUncleanShutdown——异步刷盘不可裸恢复（spec §2.2）。
func TestManagerUncleanRestartRefusesResume(t *testing.T) {
	dir := t.TempDir()
	st1, m1 := startSoloManager(t, dir, AckQuorumMem)
	// kill 前先落地一条提案，保证至少一轮 Ready 被处理（HardState/
	// 条目已持久化）：若 kill 抢在全部组第一个 Ready 之前，盘上不会
	// 有任何 raft 状态，重启会走 fresh 路径——既往 ~1/16 抖动源。
	b := st1.NewBatch()
	_ = b.Set([]byte("meta/topic/t1"), []byte("v"))
	repr := append([]byte(nil), b.Repr()...)
	_ = b.Close()
	pctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := m1.Propose(pctx, MetaGroup, repr); err != nil {
		t.Fatal(err)
	}
	m1.kill() // 测试后门：cancel 运行 ctx，不写标记
	<-m1.Done()
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}
	st2 := mustOpenStore(t, dir)
	_, err := NewManager(soloOptions(t, st2, dir, AckQuorumMem))
	if !errors.Is(err, ErrUncleanShutdown) {
		t.Fatalf("不干净重启 NewManager = %v; want ErrUncleanShutdown", err)
	}
}

// mustOpenStore 打开指定目录下的 store，随测试结束自动关闭。
// 测试体可能已显式 Close（重启测试必须先关再重开），而 pebble 对
// 已关闭 DB 再 Close 是 panic 而非返回错误——清理期按「关闭已做过」
// 语义吞掉该 panic。
func mustOpenStore(t *testing.T, dir string) *store.Store {
	t.Helper()
	st, err := store.Open(dir, false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer func() { _ = recover() }() // pebble: closed——重复关闭，忽略
		st.Close()
	})
	return st
}

// soloOptions 构造单成员（NodeID=1，成员表只有自己）的 Manager 选项：
// Peers 地址为注入监听器的占位（本节点消息不走传输层，地址无需真实），
// DataGroups 用默认 3。dir 仅保持签名与 startSoloManager 一致。
func soloOptions(t *testing.T, st *store.Store, dir string, mode AckMode) Options {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return Options{
		NodeID:     1,
		Peers:      map[uint64]string{1: "127.0.0.1:0"},
		Listener:   ln,
		DataGroups: 3,
		Mode:       mode,
		Store:      st,
		Logger:     testSlog(t),
	}
}

// startSoloManager 打开（或复用）dir 下的 store 并启动单成员 Manager；
// NewManager 返回错误（含 ErrUncleanShutdown）在此直接失败。测试结束后
// 自动 kill 并等待完全退出，防止 goroutine 泄漏到后续清理（LIFO：kill
// 先于 mustOpenStore 的 store 关闭注册）。
func startSoloManager(t *testing.T, dir string, mode AckMode) (*store.Store, *Manager) {
	t.Helper()
	st := mustOpenStore(t, dir)
	m, err := NewManager(soloOptions(t, st, dir, mode))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		m.kill()
		select {
		case <-m.Done():
		case <-time.After(5 * time.Second):
			t.Error("manager 未在 5s 内完全退出")
		}
	})
	return st, m
}
