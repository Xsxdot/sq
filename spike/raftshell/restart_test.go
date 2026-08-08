// restart_test.go 验证重启恢复的两条路径：不干净关机 → learner 追齐回归；
// 干净关机（写标记）→ 原身份 RestartNode 回归。
//
// 职责：AckQuorumMem 档「不可裸关 fsync」配套规则（spec §2.2）的可行性
// 验证，并实测 learner 追齐耗时回填 spec §6。
// 边界：不测快照流式追齐（B8.2 范围）；不测重启 leader 节点。
package raftshell

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureLogger 返回写往测试输出、同时把所有日志行存入内存缓冲的 slog，
// 供测试断言「走了哪条恢复路径」（以日志线索为证）。
func captureLogger(t *testing.T) (*slog.Logger, *captureBuf) {
	b := &captureBuf{}
	h := slog.NewTextHandler(io.MultiWriter(testWriter{t}, b), &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), b
}

// captureBuf 是并发安全的日志缓冲（节点 run 循环多 goroutine 写日志）。
type captureBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *captureBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *captureBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestUncleanRestartRejoinsAsLearner 验证 AckQuorumMem 档下模拟断电
// （不写干净关机标记直接 kill）重启的节点必须走 learner 追齐路径，
// 最终 FSM 收敛且已 commit 条目一条不丢。
//
// 这是 spec §2.2「不可裸关 fsync」配套规则的可行性验证 + 追齐耗时测量：
// 2000 条已 commit 日志在断电节点的全量重放耗时。
func TestUncleanRestartRejoinsAsLearner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := NewCluster(t.TempDir(), AckQuorumMem, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Shutdown(); err != nil {
			t.Error(err)
		}
	}()
	lead, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	leader := c.Leader()
	if leader == nil {
		t.Fatalf("WaitLeader 返回 %d 但 Leader() 为空", lead)
	}
	// 挑一个非 leader 节点作为断电对象
	var follower uint64
	for _, id := range clusterPeerIDs {
		if id != lead {
			follower = id
			break
		}
	}
	for i := 0; i < 1000; i++ {
		if err := leader.Propose(ctx, []byte(fmt.Sprintf("msg-%04d", i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	waitApplied(t, c, 1000, 15*time.Second)
	// 断电：不写干净关机标记直接 kill（模拟进程被杀、电源中断）
	c.Kill(follower)
	// 集群 2/3 照常工作，再追 1000 条
	cur := c.Leader()
	if cur == nil {
		t.Fatalf("Kill(%d) 后 Leader() 为空", follower)
	}
	for i := 0; i < 1000; i++ {
		if err := cur.Propose(ctx, []byte(fmt.Sprintf("msg-%04d", 1000+i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	waitApplied(t, c, 2000, 15*time.Second)

	// 重启走 learner 追齐路径；整个恢复过程（移除 → 加 learner → 追平 →
	// 升 voter）耗时即本任务要实测的分量
	start := time.Now()
	if err := c.Restart(follower); err != nil {
		t.Fatalf("Restart(%d): %v", follower, err)
	}
	if err := c.WaitConverged(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	t.Logf("2000 条追齐耗时 %v", time.Since(start))
	if got := c.Nodes[follower].AppliedCount(); got != 2000 {
		t.Fatalf("重启节点 applied = %d, want 2000（已 commit 条目不丢）", got)
	}
}

// TestCleanRestartResumes 对照组：干净关机（写标记）后重启走 RestartNode
// 原身份回归，不清状态目录、不降级、不产生任何成员变更。
//
// 判定依据：重启后 storage 从 WAL 重放恢复非零 lastIndex——无需任何新
// 提案，applied 直接恢复为关机前的 100；且日志中不出现 learner 重入的
// 排障线索。
func TestCleanRestartResumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lg, buf := captureLogger(t)
	c, err := NewCluster(t.TempDir(), AckQuorumMem, lg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Shutdown(); err != nil {
			t.Error(err)
		}
	}()
	lead, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	leader := c.Leader()
	if leader == nil {
		t.Fatalf("WaitLeader 返回 %d 但 Leader() 为空", lead)
	}
	var follower uint64
	for _, id := range clusterPeerIDs {
		if id != lead {
			follower = id
			break
		}
	}
	for i := 0; i < 100; i++ {
		if err := leader.Propose(ctx, []byte(fmt.Sprintf("msg-%03d", i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	waitApplied(t, c, 100, 15*time.Second)
	// 干净关机：写标记（Sync 落盘）后退出
	c.Nodes[follower].StopClean(ctx)
	select {
	case <-c.Nodes[follower].Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("节点 %d 未在 StopClean 后 5s 内退出", follower)
	}
	if err := c.Restart(follower); err != nil {
		t.Fatalf("Restart(%d): %v", follower, err)
	}
	// 干净路径断言一：WAL 重放恢复了非零 lastIndex——重启节点会重新投递
	// 全部已 commit 条目（raft applied 指针从零开始），无需任何新提案，
	// applied 恢复到关机前的 100；轮询等待重放完成
	waitApplied(t, c, 100, 15*time.Second)
	if got := c.Nodes[follower].AppliedCount(); got != 100 {
		t.Fatalf("干净重启后 applied = %d, want 100（WAL 重放应恢复已 commit 状态）", got)
	}
	// 干净路径断言二：未走 learner 路径——日志中不应出现不干净关机的排障线索
	if s := buf.String(); strings.Contains(s, uncleanRestartLogMsg) {
		t.Fatalf("干净关机被误判为不干净关机，走了 learner 重入路径\n%s", s)
	}
	// 节点以原身份回归，集群继续可用（新提案全量复制到三个节点）
	cur := c.Leader()
	if cur == nil {
		t.Fatalf("Restart 后 Leader() 为空")
	}
	for i := 0; i < 50; i++ {
		if err := cur.Propose(ctx, []byte(fmt.Sprintf("msg-%03d", 100+i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	waitApplied(t, c, 150, 15*time.Second)
}
