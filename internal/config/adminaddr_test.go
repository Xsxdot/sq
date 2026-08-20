// adminaddr_test.go: 管理面地址推导的单测——覆盖回落、显式端口、
// 管理面关闭三条路径。
package config

import "testing"

func cfgWithPeers(adminListen string, ports ...int) *Config {
	peers := make([]ClusterPeer, 0, len(ports))
	for i, port := range ports {
		peers = append(peers, ClusterPeer{
			ID:            uint64(i + 1),
			RaftAddr:      "10.0.0.1:9081",
			AdvertiseHost: "10.0.0.1",
			AdvertisePort: 8081,
			AdminPort:     port,
		})
	}
	return &Config{AdminListen: adminListen, Cluster: &ClusterConfig{Peers: peers}}
}

func TestLocalAdminPort(t *testing.T) {
	c := &Config{AdminListen: ":8082"}
	got, err := c.LocalAdminPort()
	if err != nil || got != 8082 {
		t.Fatalf("期望 8082/nil，得到 %d/%v", got, err)
	}
}

func TestLocalAdminPortRejectsClosedAdmin(t *testing.T) {
	c := &Config{AdminListen: ""}
	if _, err := c.LocalAdminPort(); err == nil {
		t.Fatal("admin_listen 为空时必须报错，不能回一个可用端口")
	}
}

func TestPeerAdminAddrUsesExplicitPort(t *testing.T) {
	c := cfgWithPeers(":8082", 9999)
	got, err := c.PeerAdminAddr(c.Cluster.Peers[0])
	if err != nil || got != "10.0.0.1:9999" {
		t.Fatalf("期望 10.0.0.1:9999，得到 %q/%v", got, err)
	}
}

func TestPeerAdminAddrFallsBackToLocalPort(t *testing.T) {
	// admin_port 未填（0）时回落本机 admin_listen 的端口——存量配置的兼容路径
	c := cfgWithPeers(":8082", 0)
	got, err := c.PeerAdminAddr(c.Cluster.Peers[0])
	if err != nil || got != "10.0.0.1:8082" {
		t.Fatalf("期望 10.0.0.1:8082，得到 %q/%v", got, err)
	}
}

func TestPeerAdminAddrFallbackFailsWhenAdminClosed(t *testing.T) {
	c := cfgWithPeers("", 0)
	if _, err := c.PeerAdminAddr(c.Cluster.Peers[0]); err == nil {
		t.Fatal("admin_port 未填且管理面关闭时必须报错")
	}
}

func TestPeerByID(t *testing.T) {
	c := cfgWithPeers(":8082", 8082, 8082)
	if p, ok := c.PeerByID(2); !ok || p.ID != 2 {
		t.Fatalf("期望查到 id=2，得到 %+v/%v", p, ok)
	}
	if _, ok := c.PeerByID(99); ok {
		t.Fatal("不存在的 id 必须回 ok=false")
	}
	if _, ok := (&Config{}).PeerByID(1); ok {
		t.Fatal("单机档（Cluster==nil）必须回 ok=false，不能 panic")
	}
}
