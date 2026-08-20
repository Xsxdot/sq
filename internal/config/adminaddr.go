// adminaddr.go: 管理面地址推导——把 admin_listen 与 peers 的 admin_port
// 翻译成「某个节点的管理面 host:port」。
//
// 职责：
//   - 解析本节点 admin_listen 的端口
//   - 按成员推导其管理面地址；admin_port 未填时回落本机端口
//   - 按 id 查成员表
//
// 边界：
//   - 纯字符串推导，不发起任何网络请求、不判断对端是否真的活着
//   - 不判断管理面是否开着——admin_listen 为空时返回错误，由调用方
//     翻译成对用户有意义的话（「管理面已关闭」而不是连接超时）
//   - 只被 `sq status` 消费；集群运行时不经过这里
package config

import (
	"fmt"
	"net"
	"strconv"
)

// LocalAdminPort 返回本节点 admin_listen 的端口。
//
// 返回：端口号与错误。
//
// 注意：admin_listen 为空（管理面整体关闭）时返回错误而不是某个默认值——
// 让调用方能给出「管理面已关闭」这句准确的话，而不是去连一个空地址再超时。
func (c *Config) LocalAdminPort() (int, error) {
	if c.AdminListen == "" {
		return 0, fmt.Errorf("管理面已关闭（admin_listen 为空），没有管理端口可用")
	}
	_, portStr, err := net.SplitHostPort(c.AdminListen)
	if err != nil {
		return 0, fmt.Errorf("解析 admin_listen %q 失败: %w", c.AdminListen, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("admin_listen %q 的端口部分非法", c.AdminListen)
	}
	return port, nil
}

// PeerAdminAddr 返回给定成员的管理面地址，形如 "10.0.0.1:8082"。
//
// 参数：
//   - p: 成员表中的一项
//
// 返回：host:port 与错误
//
// 注意：p.AdminPort 为 0（未填）时回落取本机 admin_listen 的端口。这条回落
// 隐含「各节点管理端口一致」的假设，是为存量配置准备的兼容路径；
// quickstart.sh 生成的配置会显式写上 admin_port，不走回落。
func (c *Config) PeerAdminAddr(p ClusterPeer) (string, error) {
	port := p.AdminPort
	if port == 0 {
		local, err := c.LocalAdminPort()
		if err != nil {
			return "", fmt.Errorf("成员 %d 未配置 admin_port，回落取本机端口失败: %w", p.ID, err)
		}
		port = local
	}
	return net.JoinHostPort(p.AdvertiseHost, strconv.Itoa(port)), nil
}

// PeerByID 按 id 查成员表。
//
// 返回：成员与是否命中。单机档（Cluster 为 nil）一律回 false，不 panic。
func (c *Config) PeerByID(id uint64) (ClusterPeer, bool) {
	if c.Cluster == nil {
		return ClusterPeer{}, false
	}
	for _, p := range c.Cluster.Peers {
		if p.ID == id {
			return p, true
		}
	}
	return ClusterPeer{}, false
}
