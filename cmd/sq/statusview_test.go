// statusview_test.go: sq status 的判级与渲染单测。
//
// 判级用例是承重的：退出码会被写进监控脚本，改判据必须先改这里的断言。
package main

import (
	"strings"
	"testing"

	"github.com/Xsxdot/sq/internal/replication"
)

// clusterView 构造一份可用的集群视图，便于各用例只改自己关心的那一处。
func clusterView() replication.ClusterView {
	return replication.ClusterView{
		Enabled: true,
		SelfID:  2,
		Nodes: []replication.NodeView{
			{ID: 1, RaftAddr: "10.0.0.1:9081"},
			{ID: 2, RaftAddr: "10.0.0.2:9081", Self: true},
			{ID: 3, RaftAddr: "10.0.0.3:9081"},
		},
		Groups: []replication.GroupView{
			{ID: 0, Leader: 1, IsLeader: false, Role: "follower", Applied: 10, Commit: 10, Term: 5,
				Peers: []replication.PeerProgressView{
					{ID: 1, RecentActive: true}, {ID: 2, RecentActive: true}, {ID: 3, RecentActive: true},
				}},
			{ID: 1, Leader: 1, IsLeader: false, Role: "follower", Applied: 88, Commit: 88, Term: 5,
				Peers: []replication.PeerProgressView{
					{ID: 1, RecentActive: true}, {ID: 2, RecentActive: true}, {ID: 3, RecentActive: true},
				}},
		},
	}
}

func TestVerdictStandaloneIsOK(t *testing.T) {
	v := statusView{Cluster: replication.ClusterView{Enabled: false}, Overview: &adminOverview{}}
	if got := statusVerdict(v); got != statusOK {
		t.Fatalf("单机档期望 %d，得到 %d", statusOK, got)
	}
}

func TestVerdictHealthyClusterIsOK(t *testing.T) {
	if got := statusVerdict(statusView{Cluster: clusterView()}); got != statusOK {
		t.Fatalf("全健康期望 %d，得到 %d", statusOK, got)
	}
}

func TestVerdictNoLeaderIsDegraded(t *testing.T) {
	cv := clusterView()
	cv.Groups[1].Leader = 0
	if got := statusVerdict(statusView{Cluster: cv}); got != statusDegraded {
		t.Fatalf("有组无 leader 期望 %d，得到 %d", statusDegraded, got)
	}
}

func TestVerdictInactivePeerIsDegraded(t *testing.T) {
	cv := clusterView()
	cv.Groups[0].Peers[2].RecentActive = false
	if got := statusVerdict(statusView{Cluster: cv}); got != statusDegraded {
		t.Fatalf("peer 失联期望 %d，得到 %d", statusDegraded, got)
	}
}

func TestVerdictDegradedViewIsIncomplete(t *testing.T) {
	// 视角降级：Peers 是空表。空表不等于没有 peer，只等于本节点不是 leader，
	// 此时报 0 就是拿看不见 peer 的视图谎称健康。
	cv := clusterView()
	for i := range cv.Groups {
		cv.Groups[i].Peers = nil
	}
	v := statusView{Cluster: cv, Degraded: true, DegradeReason: "连不上 leader"}
	if got := statusVerdict(v); got != statusIncomplete {
		t.Fatalf("视角降级期望 %d，得到 %d", statusIncomplete, got)
	}
}

func TestVerdictNoLeaderBeatsDegradedView(t *testing.T) {
	// 优先级承重：leader 判据不通过时，即使视角降级也判 2 而不是 3。
	// follower 同样知道每组的 leader 是谁，这条判据在降级视角下依然成立。
	cv := clusterView()
	cv.Groups[1].Leader = 0
	for i := range cv.Groups {
		cv.Groups[i].Peers = nil
	}
	v := statusView{Cluster: cv, Degraded: true}
	if got := statusVerdict(v); got != statusDegraded {
		t.Fatalf("无 leader 优先于降级，期望 %d，得到 %d", statusDegraded, got)
	}
}

func TestRenderClusterMentionsSelfAndLeader(t *testing.T) {
	var sb strings.Builder
	renderStatus(&sb, statusView{
		Version:    "0.1.0",
		Cluster:    clusterView(),
		ViewSource: "node 1 (10.0.0.1:8082) — leader",
		PeerHost:   map[uint64]string{1: "10.0.0.1", 2: "10.0.0.2", 3: "10.0.0.3"},
	})
	out := sb.String()
	for _, want := range []string{"0.1.0", "node 2", "10.0.0.2", "(本机)", "node 1 (10.0.0.1:8082)"} {
		if !strings.Contains(out, want) {
			t.Fatalf("输出缺少 %q：\n%s", want, out)
		}
	}
}

func TestRenderDegradedShowsReason(t *testing.T) {
	var sb strings.Builder
	cv := clusterView()
	for i := range cv.Groups {
		cv.Groups[i].Peers = nil
	}
	renderStatus(&sb, statusView{
		Version: "0.1.0", Cluster: cv, Degraded: true,
		DegradeReason: "连不上 leader 10.0.0.1:8082",
		PeerHost:      map[uint64]string{1: "10.0.0.1", 2: "10.0.0.2", 3: "10.0.0.3"},
	})
	out := sb.String()
	if !strings.Contains(out, "连不上 leader 10.0.0.1:8082") {
		t.Fatalf("降级原因必须出现在输出里：\n%s", out)
	}
	if !strings.Contains(out, "不可见") {
		t.Fatalf("必须显式标注 peer 进度不可见，避免读者把空表当成没有 peer：\n%s", out)
	}
}

func TestRenderStandalone(t *testing.T) {
	var sb strings.Builder
	renderStatus(&sb, statusView{
		Version:  "0.1.0",
		Cluster:  replication.ClusterView{Enabled: false},
		Overview: &adminOverview{Topics: 12, Groups: 5, TotalWritten: 1203441},
		System:   &adminSystem{Disk: &adminDisk{UsedPercent: 34.2}, WatermarkPercent: 85},
	})
	out := sb.String()
	for _, want := range []string{"单机模式", "12", "5", "1203441", "34.2", "85"} {
		if !strings.Contains(out, want) {
			t.Fatalf("单机输出缺少 %q：\n%s", want, out)
		}
	}
}
