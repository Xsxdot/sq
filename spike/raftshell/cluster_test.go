// cluster_test.go 验证三节点集群装配与 kill-leader 切换窗口。
//
// 职责：三节点选出唯一 leader、1000 条提案后三节点 FSM 收敛一致；
// kill leader 后重选耗时实测——spec §6 切换窗口的第一个实测分量。
// 边界：不测分区（Task 4）、不测吞吐（Task 5）；模式统一取
// AckQuorumMem 保证测试速度，刷盘档位的对比留给 Task 5 基准。
package raftshell

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// waitApplied 轮询所有存活节点直到 AppliedCount 均 ≥ want；超时报出各节点计数。
// 摘除节点（killed）不再参与对账——其 run 循环已退出，计数停在摘除前。
func waitApplied(t *testing.T, c *Cluster, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for id, n := range c.Nodes {
			if c.killed[id] {
				continue
			}
			if n.AppliedCount() < want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			counts := make([]string, 0, len(c.Nodes))
			for id, n := range c.Nodes {
				counts = append(counts, fmt.Sprintf("node%d=%d", id, n.AppliedCount()))
			}
			t.Fatalf("存活节点未收敛到 %d：%s", want, strings.Join(counts, " "))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestThreeNodeReplicate 三节点选出唯一 leader；1000 条提案后三节点 FSM 收敛一致。
// AppliedCount 只数 EntryNormal（初始成员表的 ConfChange 条目不计入，见 Task 2）。
func TestThreeNodeReplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	for i := 0; i < 1000; i++ {
		if err := leader.Propose(ctx, []byte(fmt.Sprintf("msg-%04d", i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	waitApplied(t, c, 1000, 15*time.Second)
}

// TestKillLeaderFailover kill leader 后剩余两节点在超时内选出新 leader，
// 且此前已 commit 的条目不丢；记录并打印重选耗时——spec §6 切换窗口实测。
func TestKillLeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	oldLead, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	old := c.Leader()
	if old == nil {
		t.Fatalf("WaitLeader 返回 %d 但 Leader() 为空", oldLead)
	}
	for i := 0; i < 100; i++ {
		if err := old.Propose(ctx, []byte(fmt.Sprintf("msg-%03d", i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	c.Kill(oldLead)
	start := time.Now()
	newLead, err := c.WaitLeader(5 * time.Second)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if newLead == oldLead {
		t.Fatalf("新 leader 仍是旧节点 %d", newLead)
	}
	t.Logf("重选耗时 %v（旧 leader=%d 新 leader=%d）", elapsed, oldLead, newLead)
	cur := c.Leader()
	if cur == nil {
		t.Fatalf("WaitLeader 返回 %d 但 Leader() 为空", newLead)
	}
	// 此前 100 条已 commit 的条目不丢：新 leader 再追加 100 条后，
	// 存活两节点 AppliedCount 应 ≥ 200
	for i := 0; i < 100; i++ {
		if err := cur.Propose(ctx, []byte(fmt.Sprintf("msg-%03d", 100+i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	waitApplied(t, c, 200, 15*time.Second)
}
