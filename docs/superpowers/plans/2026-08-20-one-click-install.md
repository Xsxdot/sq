# 一键安装脚本与 `sq status` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让单机与三节点部署都能用一条命令完成「取包 → 装盘 → 生成正确配置」，并新增 `sq status` 子命令让运维一条命令自证集群状态。

**Architecture:** 新增 `deploy/quickstart.sh` 作为 `deploy/install.sh` 之上的一层（install.sh 一字不改，靠「配置已存在则不覆盖」这条既有分支自然让路）；新增 `ClusterPeer.admin_port` 配置字段让 `sq status` 能跨节点跳到 leader；`sq status` 按 `sq recover` 的既有子命令形态实现，取数走 admin HTTP，渲染与判级抽成纯函数以便单测。

**Tech Stack:** Go 1.26.1+（标准库 `net/http` / `flag` / `log/slog`，不引入新依赖）、Bash 4+、GitHub Actions、shellcheck。

设计依据：[docs/superpowers/specs/2026-08-20-one-click-install-design.md](../specs/2026-08-20-one-click-install-design.md)。**本计划与 spec 冲突时以 spec 为准**，并把冲突报告给协调者，不要自行取舍。

## Global Constraints

- **不引入任何新的 Go 依赖**。`sq status` 只用标准库。
- **`deploy/install.sh` 一字不改**。任何要改它的冲动都是设计理解错了，停下来问。
- **端口写死**：gRPC `8081`、Admin `8082`、Raft `9081`。不加端口 flag。
- **日志用 `log/slog`**（Go 侧，经 `config.SetupSlog(cfg.LogLevel)` 后取 `slog.Default()`），**禁止 `fmt.Printf` 作为日志机制**。给人看的报告走 stdout（与 `cmd/sq/recover.go` 同款区分，见该文件头注释）。
- **Shell 侧的"日志"就是它的 stdout/stderr**，但必须结构化：统一 `[quickstart]` 前缀，分 `log` / `warn` / `fail` 三级，`fail` 写 stderr 并退出非零。每个错误分支都必须有一条带上下文的 `fail`。
- **所有注释、日志、报错文案用中文**（与仓库现状一致）。
- **新文件必须有文件头注释**（职责 + 边界），**导出函数必须有 doc 注释**。
- `gofmt` 必须干净；`go vet ./...` 必须过。
- 每个 task 结束都要 commit，commit message 用中文正文 + `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` 结尾。

## 计划期发现的一处 spec 缺口（已在本计划内处理）

spec §7 写「`shellcheck` 进 CI」，但**本仓库目前没有任何测试用的 CI workflow**，只有 `release.yml`。Task 7 因此新建一个**刻意窄**的 `.github/workflows/ci.yml`：只跑 shellcheck 与 shell 测试脚本，**不跑 `go test ./...`**——这个仓库的 Go 测试从未在 CI 跑过，贸然全量接进来会被无关的既有 flake 挡住本项交付。Go 侧测试仍按现状本地跑。

---

### Task 1: `ClusterPeer.AdminPort` 字段与管理面地址推导

**Files:**
- Modify: `internal/config/config.go`（`ClusterPeer` 结构体 ~162-167；peers 校验循环 ~372-392）
- Create: `internal/config/adminaddr.go`
- Test: `internal/config/adminaddr_test.go`，并在 `internal/config/config_test.go` 追加校验用例

**Interfaces:**
- Consumes: 无（本 task 是最底层）
- Produces:
  - `config.ClusterPeer.AdminPort int`（yaml 键 `admin_port`）
  - `func (c *Config) LocalAdminPort() (int, error)`
  - `func (c *Config) PeerAdminAddr(p ClusterPeer) (string, error)`
  - `func (c *Config) PeerByID(id uint64) (ClusterPeer, bool)`

- [ ] **Step 1: 写失败测试——地址推导**

创建 `internal/config/adminaddr_test.go`：

```go
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
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/config -run 'TestLocalAdminPort|TestPeerAdmin|TestPeerByID' -v`
Expected: 编译失败，`c.LocalAdminPort undefined` / `unknown field AdminPort`

- [ ] **Step 3: 给 `ClusterPeer` 加字段**

在 `internal/config/config.go` 的 `ClusterPeer` 结构体末尾追加（在 `AdvertisePort` 之后）：

```go
	// AdminPort 该节点管理面 HTTP 端口。
	//
	// 0 = 未填：此时 Config.PeerAdminAddr 回落取本机 admin_listen 的端口。
	// 存量配置不含本字段，回落后行为与新增本字段之前完全一致，故非破坏性变更。
	//
	// 注意：**集群运行时不读本字段**——raft 复制与协议面路由都不经过管理面。
	// 它只服务于 `sq status` 的跨节点查询。不要把它当成拓扑的一部分，
	// 也不要在复制层引用它。
	AdminPort int `yaml:"admin_port"`
```

- [ ] **Step 4: 加校验**

在 `internal/config/config.go` 的 peers 校验循环里，紧跟在 `AdvertisePort` 那条校验之后追加：

```go
			// 0 = 未填（回落本机 admin 端口，见 ClusterPeer.AdminPort 注释）；
			// 负数与越界是笔误，与 advertise_port 同规格，启动即挡。
			if p.AdminPort < 0 || p.AdminPort > 65535 {
				return nil, fmt.Errorf("配置 cluster.peers[%d] 的 admin_port 须在 [1,65535] 或留空，得到 %d", i, p.AdminPort)
			}
```

- [ ] **Step 5: 建 `internal/config/adminaddr.go`**

```go
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
```

- [ ] **Step 6: 跑测试确认通过**

Run: `go test ./internal/config -run 'TestLocalAdminPort|TestPeerAdmin|TestPeerByID' -v`
Expected: 全部 PASS

- [ ] **Step 7: 加配置校验的失败用例**

在 `internal/config/config_test.go` 末尾追加（沿用该文件既有的 `t.TempDir()` + 写 yaml 的模式）：

```go
func TestLoadRejectsBadAdminPort(t *testing.T) {
	for _, port := range []int{-1, 65536} {
		p := filepath.Join(t.TempDir(), "sq.yaml")
		yml := fmt.Sprintf(`
cluster:
  node_id: 1
  raft_listen: ":9081"
  peers:
    - id: 1
      raft_addr: "127.0.0.1:9081"
      advertise_host: "127.0.0.1"
      advertise_port: 8081
      admin_port: %d
`, port)
		if err := os.WriteFile(p, []byte(yml), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(p); err == nil {
			t.Fatalf("admin_port=%d 必须被拒绝", port)
		}
	}
}

func TestLoadAcceptsOmittedAdminPort(t *testing.T) {
	// 存量配置不含 admin_port —— 必须照常加载，值为 0（回落语义）
	p := filepath.Join(t.TempDir(), "sq.yaml")
	yml := `
cluster:
  node_id: 1
  raft_listen: ":9081"
  peers:
    - id: 1
      raft_addr: "127.0.0.1:9081"
      advertise_host: "127.0.0.1"
      advertise_port: 8081
`
	if err := os.WriteFile(p, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("存量配置（无 admin_port）必须能加载: %v", err)
	}
	if cfg.Cluster.Peers[0].AdminPort != 0 {
		t.Fatalf("未填时期望 0，得到 %d", cfg.Cluster.Peers[0].AdminPort)
	}
}
```

若该文件尚未 import `fmt`/`os`，补上 import。

- [ ] **Step 8: 跑全包测试**

Run: `go test ./internal/config -count=1`
Expected: PASS（含既有全部用例，确认新字段没打破任何现存校验）

- [ ] **Step 9: 加日志与注释自检**

本 task 是纯配置解析层，**不加运行时日志**——理由必须写进 `adminaddr.go` 的文件头（已写：纯字符串推导、不发网络请求）。配置层的错误一律以 error 返回给调用方，由调用方决定怎么记；`config.go` 现有校验分支同样如此，加 slog 会与既有风格冲突且在 `Load` 阶段 logger 尚未装配。
逐项确认：
- `adminaddr.go` 有文件头注释（职责 + 边界）✓
- 三个导出方法都有 doc 注释，写明参数、返回、注意事项 ✓
- `AdminPort` 字段注释写明「运行时不读它」这条边界 ✓
- 回落分支有「为什么」注释（存量兼容 + 隐含假设）✓

- [ ] **Step 10: gofmt 与 vet**

Run: `gofmt -l internal/config && go vet ./internal/config`
Expected: 无输出

- [ ] **Step 11: Commit**

```bash
git add internal/config/config.go internal/config/adminaddr.go internal/config/adminaddr_test.go internal/config/config_test.go
git commit -m "$(cat <<'EOF'
feat(config): peers 增加可选 admin_port 字段与管理面地址推导

sq status 需要从 follower 跳到 leader 的管理面，但 peers 表里原本没有
admin 地址（admin_listen 是顶层字段，只描述本节点）。新增可选
admin_port，未填时回落取本机端口，存量配置行为不变。

集群运行时不读该字段，只服务于 sq status——已在字段注释里写死这条边界。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `sq status` 的渲染与判级纯函数

**Files:**
- Create: `cmd/sq/statusview.go`
- Test: `cmd/sq/statusview_test.go`

**Interfaces:**
- Consumes: `replication.ClusterView` / `NodeView` / `GroupView` / `PeerProgressView`（`internal/replication/replication.go:60-107`，字段见下方代码）
- Produces:
  - `type adminOverview struct{...}`（`/admin/overview` 的 JSON 形状）
  - `type adminSystem struct{...}`（`/admin/system` 的 JSON 形状）
  - `type statusView struct{...}`
  - `const statusOK/statusUnreachable/statusDegraded/statusIncomplete = 0/1/2/3`
  - `func statusVerdict(v statusView) int`
  - `func renderStatus(w io.Writer, v statusView)`

本 task **不碰网络**，只做「一份视图 → 文本 + 退出码」。Task 3 负责把视图填出来。

- [ ] **Step 1: 写失败测试**

创建 `cmd/sq/statusview_test.go`：

```go
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
		Version: "0.1.0", Cluster: clusterView(),
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
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./cmd/sq -run 'TestVerdict|TestRender' -v`
Expected: 编译失败，`undefined: statusView` 等

- [ ] **Step 3: 实现 `cmd/sq/statusview.go`**

```go
// statusview.go 提供 `sq status` 的两件纯计算：把一份状态视图判成退出码，
// 以及把它渲染成给人看的文本。
//
// 职责：
//   - 定义 admin HTTP 三个端点的 JSON 形状（overview / system / cluster 的载体）
//   - statusVerdict：视图 → 退出码
//   - renderStatus：视图 → 文本
//
// 边界：
//   - 不发起任何网络请求、不读配置、不碰 os.Exit——取数与退出由 status.go 负责。
//     这样切开是为了让判级逻辑可被穷举单测：它的输出会被写进运维的监控脚本，
//     是本命令唯一不能出错的部分
//   - 不打日志：纯函数没有「关键节点」可言，且它每次调用都会把全部输入
//     渲染到 stdout，再打一份 slog 等于同一件事记两遍
package main

import (
	"fmt"
	"io"
	"sort"

	"github.com/Xsxdot/sq/internal/replication"
)

// sq status 的退出码。分级是为了能直接写进监控脚本：三个非零码对应三种
// 完全不同的处置，不能合并。
const (
	// statusOK 健康：全部组有 leader，且全部 peer 活跃
	statusOK = 0
	// statusUnreachable 够不着本机管理面：服务没起 / 管理面关闭 / 凭据错 / HTTP 失败
	statusUnreachable = 1
	// statusDegraded 集群降级：有组无 leader，或有 peer 失联
	statusDegraded = 2
	// statusIncomplete 判定不完整：视角降级，组 leader 判据通过但 peer 活跃度不可见
	statusIncomplete = 3
)

// adminDisk 对应 /admin/system 里的 disk 段。
type adminDisk struct {
	UsedPercent float64 `json:"used_percent"`
}

// adminSystem 对应 GET /admin/system 的响应（只取本命令用得上的字段）。
type adminSystem struct {
	Disk             *adminDisk `json:"disk"`
	WatermarkPercent int        `json:"watermark_percent"`
	UptimeSeconds    int64      `json:"uptime_seconds"`
}

// adminOverview 对应 GET /admin/overview 的响应（只取本命令用得上的字段）。
type adminOverview struct {
	Topics       int    `json:"topics"`
	Groups       int    `json:"groups"`
	Connections  int    `json:"connections"`
	TotalWritten uint64 `json:"total_written"`
	TotalPending uint64 `json:"total_pending"`
	TotalDLQ     uint64 `json:"total_dlq"`
}

// statusView 渲染与判级所需的全部输入。由 status.go 的取数流程填充。
type statusView struct {
	// Version 本二进制的版本号（main.version）
	Version string
	// Cluster 集群拓扑。Enabled=false 即单机档
	Cluster replication.ClusterView
	// Overview / System 只在单机档非 nil
	Overview *adminOverview
	System   *adminSystem
	// ViewSource 这份数据来自哪个节点，如 "node 1 (10.0.0.1:8082) — leader"
	ViewSource string
	// Degraded true = 本机是 follower 且跳 leader 失败，退回了本机视角。
	// 此时 Cluster.Groups[].Peers 必为空表，peer 活跃度不可知
	Degraded bool
	// DegradeReason Degraded 时的原因，原样展示给用户
	DegradeReason string
	// PeerHost id → advertise_host，用于成员表渲染出人能认的地址
	PeerHost map[uint64]string
}

// statusVerdict 由视图定退出码。
//
// 参数：v 已填充完毕的视图
// 返回：statusOK / statusDegraded / statusIncomplete 之一
// （statusUnreachable 由取数层直接返回，到不了这里）
//
// 判级优先级是承重的，不可调换：
//
//  1. 有组 leader==0 → statusDegraded。这条在降级视角下同样成立——follower
//     也知道每组的 leader 是谁，所以它比「视角是否完整」更硬。
//  2. 视角降级 → statusIncomplete。GroupView.Peers 在非 leader 上是空表，
//     而**空表不等于没有 peer**；此时报 OK 等于拿看不见 peer 的视图谎称健康，
//     这是监控脚本最不能容忍的一种谎。
//  3. 有 peer !RecentActive → statusDegraded。
//  4. 其余 → statusOK。
func statusVerdict(v statusView) int {
	if !v.Cluster.Enabled {
		// 单机档没有 leader 与 peer 的概念：能取到数据本身就是健康的证明
		return statusOK
	}
	for _, g := range v.Cluster.Groups {
		if g.Leader == 0 {
			return statusDegraded
		}
	}
	if v.Degraded {
		return statusIncomplete
	}
	for _, g := range v.Cluster.Groups {
		for _, p := range g.Peers {
			if !p.RecentActive {
				return statusDegraded
			}
		}
	}
	return statusOK
}

// renderStatus 把视图渲染成给人看的文本。
//
// 参数：
//   - w: 输出目标（正常是 os.Stdout；单测传 strings.Builder）
//   - v: 已填充完毕的视图
//
// 注意：本函数不返回错误——渲染失败无从处置，且 w 是 stdout 时写失败
// 本身就意味着输出管道已断。
func renderStatus(w io.Writer, v statusView) {
	if !v.Cluster.Enabled {
		renderStandalone(w, v)
		return
	}
	renderCluster(w, v)
}

// renderStandalone 单机档：两行讲完，没有 leader/成员/组的概念。
func renderStandalone(w io.Writer, v statusView) {
	fmt.Fprintf(w, "sq %s   单机模式\n", v.Version)
	if v.Overview != nil {
		fmt.Fprintf(w, "topic %d   消费组 %d   连接 %d   已写入 %d   积压 %d   死信 %d\n",
			v.Overview.Topics, v.Overview.Groups, v.Overview.Connections,
			v.Overview.TotalWritten, v.Overview.TotalPending, v.Overview.TotalDLQ)
	}
	if v.System != nil && v.System.Disk != nil {
		fmt.Fprintf(w, "磁盘 %.1f%% / 拒写水位 %d%%\n", v.System.Disk.UsedPercent, v.System.WatermarkPercent)
	}
}

// renderCluster 集群档：先说视角来源（这份数据是谁给的），再成员表，再各组。
//
// 视角来源放最前面是刻意的——同一条命令在 leader 与 follower 上能看到的
// 东西不一样，读者必须先知道自己在看谁的视角，后面的数字才有意义。
func renderCluster(w io.Writer, v statusView) {
	fmt.Fprintf(w, "sq %s   本机 node %d   集群 %d 节点   数据组 %d\n",
		v.Version, v.Cluster.SelfID, len(v.Cluster.Nodes), len(v.Cluster.Groups)-1)
	if v.Degraded {
		fmt.Fprintf(w, "视角：本机（%s）——%s\n", "未能跳转到 leader", v.DegradeReason)
		fmt.Fprintf(w, "      各 peer 的复制进度在本视角下**不可见**（非 leader 节点不维护 peer 进度）\n")
	} else {
		fmt.Fprintf(w, "视角：%s\n", v.ViewSource)
	}

	fmt.Fprintf(w, "\n成员\n")
	fmt.Fprintf(w, " %-4s %-22s %-22s %s\n", "id", "地址", "raft", "状态")
	for _, n := range v.Cluster.Nodes {
		host := v.PeerHost[n.ID]
		if n.Self {
			host += "  (本机)"
		}
		fmt.Fprintf(w, " %-4d %-22s %-22s %s\n", n.ID, host, n.RaftAddr, peerState(v, n.ID))
	}

	fmt.Fprintf(w, "\n组\n")
	fmt.Fprintf(w, " %-4s %-8s %-6s %-10s %s\n", "组", "leader", "term", "applied", "待apply")
	groups := append([]replication.GroupView(nil), v.Cluster.Groups...)
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })
	for _, g := range groups {
		leader := fmt.Sprintf("%d", g.Leader)
		if g.Leader == 0 {
			leader = "无"
		}
		// 待 apply = commit − applied，每个节点自己就算得出（不依赖 leader 数据）。
		// applier 卡住会在这一列立刻显形。
		fmt.Fprintf(w, " %-4d %-8s %-6d %-10d %d\n", g.ID, leader, g.Term, g.Applied, g.Commit-g.Applied)
	}
}

// peerState 给成员表的「状态」列取一句话。
//
// 注意：视角降级时一律回「不可见」而不是「活跃」——空的 Peers 表只说明
// 本节点不是 leader，不说明对端活着。这个区分是退出码 3 存在的同一个理由。
func peerState(v statusView, id uint64) string {
	if id == v.Cluster.SelfID {
		return "● 本节点"
	}
	if v.Degraded {
		return "? 不可见"
	}
	for _, g := range v.Cluster.Groups {
		for _, p := range g.Peers {
			if p.ID == id && !p.RecentActive {
				return "○ 失联"
			}
		}
	}
	return "● 活跃"
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./cmd/sq -run 'TestVerdict|TestRender' -v`
Expected: 全部 PASS（9 个用例）

- [ ] **Step 5: 加日志与注释自检**

本 task 全是纯函数，**刻意不打日志**——理由已写进文件头（渲染本身就把全部输入吐给了用户，再记一份 slog 是同一件事记两遍）。这是 `instrumenting-code` 允许的例外：无 I/O、无外部调用、无状态变更。**注意 Task 3 不适用本例外**。
逐项确认：
- 文件头注释写了职责与边界（含「不打日志」及其理由）✓
- `statusVerdict` doc 注释写了参数、返回、以及四条判级优先级 ✓
- `renderStatus` doc 注释写了参数与「不返回错误」的理由 ✓
- 三处「为什么」注释到位：优先级承重、视角来源为何放最前、空表不等于没有 peer ✓

- [ ] **Step 6: gofmt 与 vet**

Run: `gofmt -l cmd/sq && go vet ./cmd/sq`
Expected: 无输出

- [ ] **Step 7: Commit**

```bash
git add cmd/sq/statusview.go cmd/sq/statusview_test.go
git commit -m "$(cat <<'EOF'
feat(cli): sq status 的判级与渲染纯函数

退出码 0/1/2/3 会被写进运维监控脚本，判级是本命令唯一不能出错的部分，
因此与取数彻底切开以便穷举单测。

判级优先级承重：leader 判据先于视角完整性（follower 也知道每组 leader）；
视角降级时 GroupView.Peers 是空表，而空表不等于没有 peer——此时必须报 3
而不是 0，否则等于拿看不见 peer 的视图谎称健康。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: `sq status` 取数、子命令入口与 main 分派

**Files:**
- Create: `cmd/sq/statusfetch.go`
- Create: `cmd/sq/status.go`
- Modify: `cmd/sq/main.go:78-85`（子命令分派）
- Test: `cmd/sq/statusfetch_test.go`

**Interfaces:**
- Consumes: Task 1 的 `config.LocalAdminPort` / `PeerAdminAddr` / `PeerByID`；Task 2 的 `statusView` / `adminOverview` / `adminSystem` / `statusVerdict` / `renderStatus` / 四个退出码常量
- Produces:
  - `type adminClient struct{...}` + `newAdminClient(baseURL string, timeout time.Duration) *adminClient`
  - `func (c *adminClient) login(user, pass string) error`
  - `func (c *adminClient) getJSON(path string, out any) error`
  - `func runStatus(args []string) int`

- [ ] **Step 1: 写失败测试**

创建 `cmd/sq/statusfetch_test.go`：

```go
// statusfetch_test.go: admin HTTP 取数层单测。用 httptest 打桩，
// 覆盖登录换 token、无鉴权直连、401、以及 JSON 解码。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// adminStub 起一个最小的 admin 服务桩。token 非空时要求 Bearer 匹配。
func adminStub(t *testing.T, user, pass, token string, cluster any) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/login", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Username, Password string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Username != user || req.Password != pass {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "用户名或密码错误"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"token": token})
	})
	mux.HandleFunc("GET /admin/cluster", func(w http.ResponseWriter, r *http.Request) {
		if token != "" && r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(cluster)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestAdminClientLoginAndGet(t *testing.T) {
	srv := adminStub(t, "admin", "pw", "tok123", map[string]any{"enabled": true, "self_id": 2})
	c := newAdminClient(srv.URL, 3*time.Second)
	if err := c.login("admin", "pw"); err != nil {
		t.Fatalf("登录应成功: %v", err)
	}
	var out struct {
		Enabled bool   `json:"enabled"`
		SelfID  uint64 `json:"self_id"`
	}
	if err := c.getJSON("/admin/cluster", &out); err != nil {
		t.Fatalf("取集群视图应成功: %v", err)
	}
	if !out.Enabled || out.SelfID != 2 {
		t.Fatalf("解码结果不对: %+v", out)
	}
}

func TestAdminClientLoginRejectsBadCredentials(t *testing.T) {
	srv := adminStub(t, "admin", "pw", "tok123", nil)
	c := newAdminClient(srv.URL, 3*time.Second)
	err := c.login("admin", "wrong")
	if err == nil {
		t.Fatal("错误凭据必须报错")
	}
	// 报错要能让运维一眼看出是凭据问题，而不是网络问题
	if !strings.Contains(err.Error(), "401") && !strings.Contains(err.Error(), "凭据") {
		t.Fatalf("报错必须点明凭据/401，得到: %v", err)
	}
}

func TestAdminClientWorksWithoutAuth(t *testing.T) {
	// token 为空 = 服务端未开鉴权，不调 login 直接取
	srv := adminStub(t, "", "", "", map[string]any{"enabled": false})
	c := newAdminClient(srv.URL, 3*time.Second)
	var out struct {
		Enabled bool `json:"enabled"`
	}
	if err := c.getJSON("/admin/cluster", &out); err != nil {
		t.Fatalf("免鉴权直连应成功: %v", err)
	}
	if out.Enabled {
		t.Fatal("期望 enabled=false")
	}
}

func TestAdminClientSurfacesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"内部错误"}`))
	}))
	t.Cleanup(srv.Close)
	c := newAdminClient(srv.URL, 3*time.Second)
	var out map[string]any
	err := c.getJSON("/admin/cluster", &out)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("5xx 必须带状态码报错，得到: %v", err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./cmd/sq -run 'TestAdminClient' -v`
Expected: 编译失败，`undefined: newAdminClient`

- [ ] **Step 3: 实现 `cmd/sq/statusfetch.go`**

```go
// statusfetch.go 提供 `sq status` 的 admin HTTP 取数层。
//
// 职责：
//   - 按需登录换 Bearer token（服务端配了用户名密码时）
//   - GET admin 端点并解码 JSON
//
// 边界：
//   - 不决定「取哪个节点」——那是 status.go 的编排职责
//   - 不渲染、不判级、不退出
//   - 不复用 broker 侧的任何 HTTP 客户端：本命令是独立进程，
//     只用标准库，不引入依赖
//
// 为什么必须走 HTTP 而不是直读数据目录：sq 进程持有 Pebble 独占锁，
// 服务运行时 store.Open 必然失败（cmd/sq/recover.go 正是靠这条互斥
// 防止运行中误签）。status 要在服务**运行时**回答问题，只能走管理面。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// adminClient 一个 admin HTTP 端点的最小客户端。
//
// 注意：非并发安全——token 字段在 login 后被写入。本命令是单线程顺序取数，
// 不需要加锁；若将来要并发 ping 多个节点，请每个节点各建一个实例。
type adminClient struct {
	baseURL string
	token   string
	hc      *http.Client
}

// newAdminClient 建一个指向 baseURL 的客户端。
//
// 参数：
//   - baseURL: 形如 "http://10.0.0.1:8082"，末尾不带斜杠
//   - timeout: 单次请求超时。取值应当明显小于人的耐心——够不着的节点
//     要快速失败并降级，而不是让命令挂在那里
func newAdminClient(baseURL string, timeout time.Duration) *adminClient {
	return &adminClient{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		hc:      &http.Client{Timeout: timeout},
	}
}

// login 用用户名密码换 Bearer token 并记在客户端上。
//
// 参数：user/pass 来自配置的 admin_username / admin_password
// 返回：错误。401 会被翻译成点明「凭据」的错误——运维最先怀疑的是网络，
// 而这里恰恰不是网络问题，必须说清楚。
//
// 注意：服务端未配置鉴权时不要调用本方法（/admin/login 会回 400
// 「服务端未配置登录，无需认证」）。由调用方按配置判断。
func (c *adminClient) login(user, pass string) error {
	body, err := json.Marshal(map[string]string{"username": user, "password": pass})
	if err != nil {
		return fmt.Errorf("构造登录请求失败: %w", err)
	}
	url := c.baseURL + "/admin/login"
	slog.Debug("向管理面登录", "url", url, "user", user)
	resp, err := c.hc.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("请求 %s 失败: %w", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		slog.Error("管理面登录被拒", "url", url, "user", user, "status", resp.StatusCode)
		return fmt.Errorf("登录 %s 失败（401，凭据不匹配）：请核对配置里的 admin_username / admin_password", url)
	}
	if resp.StatusCode != http.StatusOK {
		slog.Error("管理面登录返回异常状态", "url", url, "status", resp.StatusCode, "body", string(raw))
		return fmt.Errorf("登录 %s 失败（HTTP %d）: %s", url, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.Token == "" {
		return fmt.Errorf("登录 %s 的响应无法解析出 token: %s", url, strings.TrimSpace(string(raw)))
	}
	c.token = out.Token
	slog.Debug("管理面登录成功", "url", url)
	return nil
}

// getJSON GET 一个 admin 端点并把响应解码进 out。
//
// 参数：
//   - path: 以斜杠开头的路径，如 "/admin/cluster"
//   - out: 解码目标指针
//
// 返回：错误。非 2xx 一律带上状态码与响应体——admin 的错误形状是
// {"error": "..."}，原样带出比翻译更有用。
func (c *adminClient) getJSON(path string, out any) error {
	url := c.baseURL + path
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("构造请求 %s 失败: %w", url, err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	slog.Debug("请求管理面", "url", url)
	resp, err := c.hc.Do(req)
	if err != nil {
		slog.Error("请求管理面失败", "url", url, "err", err)
		return fmt.Errorf("请求 %s 失败: %w", url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取 %s 响应失败: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		slog.Error("管理面返回异常状态", "url", url, "status", resp.StatusCode, "body", string(raw))
		return fmt.Errorf("请求 %s 返回 HTTP %d: %s", url, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("解析 %s 响应失败: %w", url, err)
	}
	slog.Debug("管理面请求完成", "url", url, "bytes", len(raw))
	return nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./cmd/sq -run 'TestAdminClient' -v`
Expected: 4 个用例全 PASS

- [ ] **Step 5: 实现 `cmd/sq/status.go`（编排层）**

```go
// status.go 实现 `sq status` 子命令：一条命令回答「本节点/本集群现在什么状态」。
//
// 职责：
//   - 读配置、定位本机管理面、按需登录
//   - 单机档取 overview + system；集群档取 cluster，必要时跳到 leader 再取一次
//   - 把结果交给 renderStatus 渲染、statusVerdict 定退出码
//
// 边界：
//   - 不启动 broker、不碰数据目录、不做任何写操作——纯只读诊断
//   - 报告走 stdout，事件与失败走 slog（与 cmd/sq/recover.go 同款区分：
//     报告是给人看的命令输出，不是事件；两边都记会产生重复）
//   - 跳不到 leader 不算失败，降级为本机视角并如实标注（退出码 3）。
//     看得到多少报多少，比整条命令失败有用
//
// 为什么需要跳到 leader：GroupView.Peers（各 peer 的复制进度）只在本节点是
// 该组 leader 时才有内容——raft 的 tracker.Progress 只在 leader 侧维护，
// follower 上是空表。不跳转就永远看不到「另外两台活着吗」。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/config"
	"github.com/Xsxdot/sq/internal/replication"
)

// statusHTTPTimeout 单次管理面请求的超时。
//
// 3s：本机管理面正常在毫秒级返回；跳 leader 是跨机一跳。够不着时要快速
// 降级而不是让命令挂着——运维敲 status 就是因为已经怀疑出事了。
const statusHTTPTimeout = 3 * time.Second

// runStatus 执行 `sq status` 子命令。
//
// 参数：args 为 `status` 之后的全部参数
// 返回：进程退出码（statusOK / statusUnreachable / statusDegraded / statusIncomplete）
//
// 注意：本函数自己把错误打进 slog 并返回退出码，不返回 error——退出码是
// 本命令的主要产物（会被写进监控脚本），交给调用方从 error 反推容易丢信息。
func runStatus(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "配置文件路径（缺省则用进程内默认值）")
	if err := fs.Parse(args); err != nil {
		return statusUnreachable
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("加载配置失败", "path", *cfgPath, "err", err)
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return statusUnreachable
	}
	config.SetupSlog(cfg.LogLevel)
	slog.Debug("sq status 开始", "config", *cfgPath, "cluster", cfg.ClusterEnabled())

	port, err := cfg.LocalAdminPort()
	if err != nil {
		// 管理面关掉时必须立刻说清楚，而不是去连一个空地址再超时——
		// 「连不上」会把运维引向网络排查，而真实原因是配置里关掉了管理面
		slog.Error("无法定位本机管理面", "err", err)
		fmt.Fprintf(os.Stderr, "%v\n本命令依赖管理面 HTTP 取数；请在配置里设置 admin_listen 后重启 sq。\n", err)
		return statusUnreachable
	}

	v, code := collectStatus(cfg, fmt.Sprintf("http://127.0.0.1:%d", port))
	if code == statusUnreachable {
		return code
	}
	renderStatus(os.Stdout, v)
	final := statusVerdict(v)
	slog.Debug("sq status 完成", "exit", final, "degraded", v.Degraded)
	return final
}

// collectStatus 取数并填出视图。
//
// 参数：
//   - cfg: 已加载的配置
//   - localBase: 本机管理面 base URL，如 "http://127.0.0.1:8082"
//
// 返回：填好的视图；第二个返回值只在够不着本机管理面时为 statusUnreachable，
// 其余情况一律为 statusOK（真正的判级由 statusVerdict 做，不在这里）。
func collectStatus(cfg *config.Config, localBase string) (statusView, int) {
	v := statusView{Version: version, PeerHost: map[uint64]string{}}
	if cfg.ClusterEnabled() {
		for _, p := range cfg.Cluster.Peers {
			v.PeerHost[p.ID] = p.AdvertiseHost
		}
	}

	local := newAdminClient(localBase, statusHTTPTimeout)
	if err := authenticate(local, cfg, localBase); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return v, statusUnreachable
	}

	var cv replication.ClusterView
	if err := local.getJSON("/admin/cluster", &cv); err != nil {
		slog.Error("取本机集群视图失败", "base", localBase, "err", err)
		fmt.Fprintf(os.Stderr, "够不着本机管理面 %s: %v\n请确认 sq 已启动（systemctl status sq）。\n", localBase, err)
		return v, statusUnreachable
	}
	v.Cluster = cv

	if !cv.Enabled {
		// 单机档：再取两个只在单机档渲染的端点。这两个失败不致命——
		// 拓扑已经拿到了，少两行数字比整条命令失败有用
		var ov adminOverview
		if err := local.getJSON("/admin/overview", &ov); err != nil {
			slog.Warn("取总览失败，跳过总览段", "err", err)
		} else {
			v.Overview = &ov
		}
		var sys adminSystem
		if err := local.getJSON("/admin/system", &sys); err != nil {
			slog.Warn("取系统信息失败，跳过磁盘段", "err", err)
		} else {
			v.System = &sys
		}
		slog.Info("sq status 取数完成（单机档）")
		return v, statusOK
	}

	v.ViewSource = fmt.Sprintf("node %d (%s) — 本机", cv.SelfID, localBase)
	metaLeader := metaGroupLeader(cv)
	if metaLeader == 0 || metaLeader == cv.SelfID {
		// 本机就是 leader（或当前无 leader，无处可跳）——本机视角已是最全的
		if metaLeader == cv.SelfID {
			v.ViewSource = fmt.Sprintf("node %d (%s) — leader", cv.SelfID, localBase)
		}
		slog.Info("sq status 取数完成（集群档，本机视角）", "self", cv.SelfID, "meta_leader", metaLeader)
		return v, statusOK
	}

	// 本机是 follower：跳到 leader 才看得到 peer 复制进度
	leaderView, source, err := fetchFromLeader(cfg, metaLeader)
	if err != nil {
		slog.Warn("跳转到 leader 失败，降级为本机视角", "leader", metaLeader, "err", err)
		v.Degraded = true
		v.DegradeReason = err.Error()
		return v, statusOK
	}
	v.Cluster = leaderView
	v.ViewSource = source
	slog.Info("sq status 取数完成（集群档，leader 视角）", "self", cv.SelfID, "leader", metaLeader)
	return v, statusOK
}

// authenticate 在服务端配了鉴权时登录换 token；未配则直接返回 nil。
//
// 参数：c 客户端；cfg 配置；base 仅用于组织报错文案
func authenticate(c *adminClient, cfg *config.Config, base string) error {
	if cfg.AdminUsername == "" {
		// 配置未设用户名 = 服务端免鉴权，调 /admin/login 会回 400，不能调
		slog.Debug("配置未设 admin_username，按免鉴权直连", "base", base)
		return nil
	}
	if err := c.login(cfg.AdminUsername, cfg.AdminPassword); err != nil {
		slog.Error("管理面登录失败", "base", base, "err", err)
		return fmt.Errorf("登录管理面 %s 失败: %w", base, err)
	}
	return nil
}

// metaGroupLeader 返回元数据组（cluster.MetaGroup，固定组号 0）的 leader id。
//
// 返回：0 表示当前无 leader 或视图里没有该组。
//
// 注意：用元数据组而不是任意数据组来决定跳哪儿——各数据组的 leader 可能
// 分散在不同节点，随便挑一个会让「视角来源」这一行在两次执行间跳来跳去。
// 元数据组是全局唯一的锚点。
func metaGroupLeader(cv replication.ClusterView) uint64 {
	for _, g := range cv.Groups {
		if g.ID == cluster.MetaGroup {
			return g.Leader
		}
	}
	return 0
}

// fetchFromLeader 到 leader 节点的管理面取一份完整集群视图。
//
// 参数：cfg 配置（用于查成员地址与凭据）；leaderID 元数据组 leader 的 id
// 返回：集群视图、视角来源描述、错误
//
// 注意：任何失败都以 error 返回，由调用方降级——不要在这里 os.Exit，
// 也不要把失败升级成整条命令失败。
func fetchFromLeader(cfg *config.Config, leaderID uint64) (replication.ClusterView, string, error) {
	var zero replication.ClusterView
	p, ok := cfg.PeerByID(leaderID)
	if !ok {
		return zero, "", fmt.Errorf("leader id %d 不在本机成员表里（配置可能与集群不一致）", leaderID)
	}
	addr, err := cfg.PeerAdminAddr(p)
	if err != nil {
		return zero, "", fmt.Errorf("推导 leader %d 的管理面地址失败: %w", leaderID, err)
	}
	base := "http://" + addr
	slog.Debug("跳转到 leader 取完整视图", "leader", leaderID, "base", base)
	c := newAdminClient(base, statusHTTPTimeout)
	if err := authenticate(c, cfg, base); err != nil {
		return zero, "", fmt.Errorf("连不上 leader %s（凭据不匹配？各节点 admin 凭据必须一致）: %w", base, err)
	}
	var cv replication.ClusterView
	if err := c.getJSON("/admin/cluster", &cv); err != nil {
		return zero, "", fmt.Errorf("连不上 leader %s: %w", base, err)
	}
	return cv, fmt.Sprintf("node %d (%s) — leader", leaderID, addr), nil
}
```

- [ ] **Step 6: 接进 main.go 的子命令分派**

在 `cmd/sq/main.go` 里，把 recover 分支之后、`run()` 之前插入 status 分支：

```go
	if len(os.Args) > 1 && os.Args[1] == "status" {
		os.Exit(runStatus(os.Args[2:]))
	}
```

同时把 `main.go:12` 的文件头注释里那句列举子命令的话补上 status（现有是
「`sq recover --grant` 的签字出口」，在其后追加一行「`sq status` 的只读状态报告」）。

- [ ] **Step 7: 跑全包测试与构建**

Run: `go test ./cmd/sq -count=1 && go build ./cmd/sq && ./sq status -config /nonexistent.yaml; echo "退出码=$?"`
Expected: 测试全 PASS；构建成功；最后一条因配置文件不存在而报错并打印 `退出码=1`

（跑完删掉构建产物：`rm -f sq`）

- [ ] **Step 8: 手工端到端自证**

```bash
go build -o /tmp/sq ./cmd/sq
/tmp/sq -config /dev/null &   # 单机默认档，admin 在 :8082
sleep 2
/tmp/sq status; echo "退出码=$?"
kill %1
```
Expected: 打印「sq dev   单机模式」及 topic/消费组/磁盘各行，`退出码=0`

- [ ] **Step 9: 加日志与注释自检（本 task 有 I/O，不适用 Task 2 的例外）**

逐项确认：
- 进入关键操作有日志：`runStatus` 开头 Debug（带 config 路径与是否集群档）✓
- 外部调用前后有日志：`getJSON` / `login` 前 Debug、后 Debug；失败 Error 带 url + status + body ✓
- 每个错误分支都记且带上下文：配置加载失败、管理面定位失败、取视图失败、登录失败、跳转失败 ✓
- **成功路径不静默**：`collectStatus` 三条出口各有一条 Info（单机档 / 本机视角 / leader 视角），`runStatus` 收尾 Debug 记退出码 ✓
- 降级是状态转移，记 Warn 且带 leader id 与原因 ✓
- 无 `fmt.Printf` 充当日志——`fmt.Fprintf(os.Stderr, ...)` 只用于给人看的错误提示，与 slog 并存是刻意的（同 recover.go）✓
- 两个新文件都有文件头注释（职责 + 边界 + 为什么必须走 HTTP / 为什么要跳 leader）✓
- 全部导出与非导出函数有 doc 注释 ✓

- [ ] **Step 10: gofmt 与 vet**

Run: `gofmt -l cmd/sq && go vet ./cmd/sq`
Expected: 无输出

- [ ] **Step 11: Commit**

```bash
git add cmd/sq/statusfetch.go cmd/sq/status.go cmd/sq/statusfetch_test.go cmd/sq/main.go
git commit -m "$(cat <<'EOF'
feat(cli): 新增 sq status 子命令

一条命令回答「本节点/本集群现在什么状态」，退出码 0/1/2/3 可直接写进监控。

取数只能走 admin HTTP：sq 进程持有 Pebble 独占锁，服务运行时直读数据目录
必然失败。follower 上会跳到元数据组 leader 再取一次——peer 复制进度只在
leader 侧维护，不跳就永远看不到同伴死活；跳不过去则降级为本机视角并如实
标注，不让整条命令失败。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `quickstart.sh` 骨架、参数解析与校验

**Files:**
- Create: `deploy/quickstart.sh`
- Create: `deploy/quickstart_test.sh`

**Interfaces:**
- Consumes: 无
- Produces（供 Task 5/6 与测试脚本调用的 shell 函数）：
  - `log/warn/fail` 三级输出
  - `parse_args "$@"` → 设置全局 `OPT_CLUSTER` `OPT_NODE_ID` `OPT_PEERS`（数组）`OPT_ADVERTISE_HOST` `OPT_ADMIN_USER` `OPT_ADMIN_PASS` `OPT_NO_ADMIN_AUTH` `OPT_VERSION` `OPT_TARBALL` `OPT_FORCE`
  - `validate_args` → 校验通过返回 0，否则 `fail`
  - 可覆盖的路径变量：`SQ_CFG_DIR` `SQ_DATA_DIR` `SQ_BIN_DST` `SQ_UNIT_DST`

**可测性设计（承重）**：脚本末尾用 `if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then main "$@"; fi` 包住入口，测试脚本 `source` 它即可只调单个函数而不执行安装。所有写盘路径用 `${VAR:=默认}` 形式，测试指向临时目录。

- [ ] **Step 1: 写失败测试**

创建 `deploy/quickstart_test.sh`（可执行）：

```bash
#!/usr/bin/env bash
#
# quickstart.sh 的测试脚本。
#
# 职责：source 被测脚本、逐个调用其函数、断言行为。
# 边界：不需要 root、不写 /etc 与 /var（全部路径重定向到临时目录），
#       不联网（取包分支用 file:// 打桩）。
#
# 用法：./deploy/quickstart_test.sh

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PASS=0; FAIL=0

ok()   { PASS=$((PASS+1)); printf '  ✓ %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '  ✗ %s\n' "$*" >&2; }
check(){ if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1（期望 %s，得到 %s）"; printf '     期望: %s\n     实际: %s\n' "$3" "$2" >&2; fi; }

# 在子 shell 里跑一次被测函数，回显退出码。source 后再调，避免执行 main。
run_in_sub() {
  ( set +e
    # shellcheck source=/dev/null
    source "${HERE}/quickstart.sh" >/dev/null 2>&1
    "$@" >/dev/null 2>&1
    echo $? )
}

echo "== 参数解析 =="

# 单机档：零参数应当通过校验
check "零参数（单机档）通过校验" "$(run_in_sub bash -c 'parse_args && validate_args')" "0"

echo "== 参数校验的拒绝路径 =="

expect_reject() {
  local desc="$1"; shift
  local code
  code="$( ( set +e
    # shellcheck source=/dev/null
    source "${HERE}/quickstart.sh" >/dev/null 2>&1
    parse_args "$@" >/dev/null 2>&1 && validate_args >/dev/null 2>&1
    echo $? ) )"
  if [[ "${code}" != "0" ]]; then ok "${desc}"; else bad "${desc}：应当被拒绝却通过了"; fi
}

expect_reject "--cluster 缺 --node-id"        --cluster --peers 10.0.0.1,10.0.0.2,10.0.0.3
expect_reject "--cluster 缺 --peers"          --cluster --node-id 1
expect_reject "--node-id 越界（0）"            --cluster --node-id 0 --peers 10.0.0.1,10.0.0.2,10.0.0.3
expect_reject "--node-id 越界（4/3 台）"       --cluster --node-id 4 --peers 10.0.0.1,10.0.0.2,10.0.0.3
expect_reject "--peers 有重复地址"             --cluster --node-id 1 --peers 10.0.0.1,10.0.0.1,10.0.0.3
expect_reject "非集群档却给了 --node-id"       --node-id 1
expect_reject "非集群档却给了 --peers"         --peers 10.0.0.1
expect_reject "--no-admin-auth 与 --admin-password 同给" --no-admin-auth --admin-password pw
expect_reject "未知参数"                       --bogus

echo
printf '通过 %d，失败 %d\n' "${PASS}" "${FAIL}"
[[ "${FAIL}" -eq 0 ]]
```

```bash
chmod +x deploy/quickstart_test.sh
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./deploy/quickstart_test.sh`
Expected: 失败——`deploy/quickstart.sh` 不存在，source 失败，所有用例挂

- [ ] **Step 3: 写 `deploy/quickstart.sh` 的骨架与参数层**

```bash
#!/usr/bin/env bash
#
# sq 一键安装脚本。
#
# 职责：取包（本地或下载）→ 生成 /etc/sq/sq.yaml → 委托 install.sh 装盘
#       → 收紧配置权限 → 打印下一步。单机与三节点集群都走这一个脚本。
# 边界：不启动服务（与 install.sh 同源的理由：安装器擅自起服务在编排系统里
#       会与其调度打架）；不 SSH、不分发、不做多机编排——三节点是三台各跑
#       一次本脚本，只差 --node-id；不实现装盘逻辑，那是 install.sh 的事。
#
# 与 install.sh 的衔接（承重，不要改顺序）：本脚本**先写配置、再调
# install.sh**。install.sh 对已存在的 /etc/sq/sq.yaml 一律不覆盖，走到那个
# 分支自然让路。反过来会让 install.sh 先铺一份 sq.example.yaml 的副本、
# 本脚本再去覆盖它，「不覆盖」那条保护就形同虚设。
#
# 必须以 root 运行，且只支持带 systemd 的 Linux。
#
# 可测性：末尾的 main 入口被 BASH_SOURCE 守卫包住，测试脚本 source 本文件
# 即可单独调用其中的函数而不触发安装。全部写盘路径都是可覆盖变量。

set -euo pipefail

# —— 路径约定（与 deploy/install.sh 必须一致，改一处就要改两处）——
# 用 := 形式让测试把它们重定向到临时目录。
: "${SQ_CFG_DIR:=/etc/sq}"
: "${SQ_DATA_DIR:=/var/lib/sq}"
: "${SQ_BIN_DST:=/usr/local/bin/sq}"
: "${SQ_USER:=sq}"

CFG_DST="${SQ_CFG_DIR}/sq.yaml"
EXAMPLE_DST="${SQ_CFG_DIR}/sq.example.yaml"

# —— 写死的端口（spec §2「不做」：不提供端口 flag）——
# 三台机器端口不一致会静默凑出一个连不上的集群，而脚本看不到另两台、
# 无从校验。端口被占的用户自行改 /etc/sq/sq.yaml 后重启。
GRPC_PORT=8081
ADMIN_PORT=8082
RAFT_PORT=9081

# —— 下载来源 ——
: "${SQ_REPO:=Xsxdot/sq}"
: "${SQ_RELEASE_API:=https://api.github.com/repos/${SQ_REPO}/releases/latest}"
: "${SQ_DOWNLOAD_BASE:=https://github.com/${SQ_REPO}/releases/download}"

# log/warn/fail 是本脚本的日志机制：stdout 就是安装器的界面。
# 统一前缀是为了让用户能把安装输出与系统其他输出区分开；fail 一律写
# stderr 并退出非零，绝不静默失败。
log()  { printf '[quickstart] %s\n' "$*"; }
warn() { printf '[quickstart] ⚠ %s\n' "$*" >&2; }
fail() { printf '[quickstart] 失败：%s\n' "$*" >&2; exit 1; }

# —— 参数（由 parse_args 填充）——
OPT_CLUSTER=0
OPT_NODE_ID=""
OPT_PEERS=()
OPT_ADVERTISE_HOST=""
OPT_ADMIN_USER=""
OPT_ADMIN_PASS=""
OPT_NO_ADMIN_AUTH=0
OPT_VERSION=""
OPT_TARBALL=""
OPT_FORCE=0

usage() {
  cat <<'USAGE'
用法：quickstart.sh [选项]

单机：
  sudo ./quickstart.sh

三节点（三台机器各跑一次，只差 --node-id）：
  sudo ./quickstart.sh --cluster --node-id 1 --peers 10.0.0.1,10.0.0.2,10.0.0.3

选项：
  --cluster                 集群档（不给 = 单机档）
  --node-id N               本机是 peers 里的第几个，集群档必填
  --peers ip1,ip2,ip3       全体成员地址，顺序即 node id，集群档必填
  --advertise-host HOST     对外地址；单机档默认 127.0.0.1，集群档自动取 peers[node-id-1]
  --admin-user U            控制台用户名，默认 admin
  --admin-password P        控制台密码；不给则自动生成
  --no-admin-auth           显式关闭管理面鉴权（不推荐）
  --version X.Y.Z           指定下载版本，默认拉最新 release
  --tarball PATH|URL        直接指定发布包，绕开 GitHub
  --force                   已有配置时备份后覆盖（从不碰数据目录）
  -h, --help                显示本帮助

端口固定为 gRPC 8081 / 控制台 8082 / raft 9081，需要改请装完后编辑
/etc/sq/sq.yaml 再重启。
USAGE
}

# parse_args 解析命令行到 OPT_* 全局变量。
#
# 参数：全部命令行参数
# 注意：只做解析不做校验——校验集中在 validate_args，便于单独测试，
# 也保证「任何盘都没动之前，所有参数问题都已暴露」。
parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --cluster)          OPT_CLUSTER=1; shift ;;
      --node-id)          OPT_NODE_ID="${2:-}"; shift 2 ;;
      --peers)            IFS=',' read -r -a OPT_PEERS <<< "${2:-}"; shift 2 ;;
      --advertise-host)   OPT_ADVERTISE_HOST="${2:-}"; shift 2 ;;
      --admin-user)       OPT_ADMIN_USER="${2:-}"; shift 2 ;;
      --admin-password)   OPT_ADMIN_PASS="${2:-}"; shift 2 ;;
      --no-admin-auth)    OPT_NO_ADMIN_AUTH=1; shift ;;
      --version)          OPT_VERSION="${2:-}"; shift 2 ;;
      --tarball)          OPT_TARBALL="${2:-}"; shift 2 ;;
      --force)            OPT_FORCE=1; shift ;;
      -h|--help)          usage; exit 0 ;;
      *)                  fail "未知参数 $1（用 --help 看用法）" ;;
    esac
  done
}

# validate_args 校验参数自洽性。
#
# 全部校验在动任何盘之前完成：安装到一半才发现参数错，现场比一开始就
# 拒绝难收拾得多。
validate_args() {
  if [[ ${OPT_CLUSTER} -eq 1 ]]; then
    [[ -n "${OPT_NODE_ID}" ]] || fail "--cluster 需要 --node-id"
    [[ ${#OPT_PEERS[@]} -ge 1 ]] || fail "--cluster 需要 --peers（形如 --peers 10.0.0.1,10.0.0.2,10.0.0.3）"
    [[ "${OPT_NODE_ID}" =~ ^[0-9]+$ ]] || fail "--node-id 必须是正整数，得到 ${OPT_NODE_ID}"
    if [[ "${OPT_NODE_ID}" -lt 1 || "${OPT_NODE_ID}" -gt ${#OPT_PEERS[@]} ]]; then
      fail "--node-id 须落在 1..${#OPT_PEERS[@]}（--peers 给了 ${#OPT_PEERS[@]} 个地址），得到 ${OPT_NODE_ID}"
    fi
    # 重复地址必然是复制粘贴错误：raft 成员表要求各成员地址唯一，
    # 重复会让两个 id 指向同一台机器，集群永远选不出稳定的 leader
    local uniq
    uniq="$(printf '%s\n' "${OPT_PEERS[@]}" | sort -u | wc -l | tr -d ' ')"
    [[ "${uniq}" -eq ${#OPT_PEERS[@]} ]] || fail "--peers 里有重复地址：${OPT_PEERS[*]}"
    # 偶数节点无容错价值（2 节点任一挂即失 quorum），与 config.go 同款：
    # 警告但不拒绝，留给运维判断
    if [[ ${#OPT_PEERS[@]} -gt 1 && $(( ${#OPT_PEERS[@]} % 2 )) -eq 0 ]]; then
      warn "集群节点数为偶数（${#OPT_PEERS[@]}），无容错价值，建议奇数"
    fi
  else
    [[ -z "${OPT_NODE_ID}" ]] || fail "--node-id 只在 --cluster 时有意义（是不是漏了 --cluster？）"
    [[ ${#OPT_PEERS[@]} -eq 0 ]] || fail "--peers 只在 --cluster 时有意义（是不是漏了 --cluster？）"
  fi

  if [[ ${OPT_NO_ADMIN_AUTH} -eq 1 ]]; then
    [[ -z "${OPT_ADMIN_USER}" && -z "${OPT_ADMIN_PASS}" ]] \
      || fail "--no-admin-auth 与 --admin-user/--admin-password 互斥：要么关鉴权，要么设凭据"
  fi
}

# main 是完整安装流程。Task 5/6 会逐步填充它。
main() {
  parse_args "$@"
  validate_args
  log "参数校验通过"
}

# 守卫：被 source 时不执行安装，只导出函数供测试调用。
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
```

```bash
chmod +x deploy/quickstart.sh
```

- [ ] **Step 4: 跑测试确认通过**

Run: `./deploy/quickstart_test.sh`
Expected: `通过 10，失败 0`

- [ ] **Step 5: 跑 shellcheck**

Run: `shellcheck deploy/quickstart.sh deploy/quickstart_test.sh`
Expected: 无输出。（本机没有 shellcheck 时：macOS `brew install shellcheck`，Debian/Ubuntu `apt-get install -y shellcheck`）

- [ ] **Step 6: 加日志与注释自检**

- 文件头注释写了职责、边界、与 install.sh 的衔接顺序（承重）、可测性设计 ✓
- 每个函数有注释说明职责；`parse_args` 注释说明「只解析不校验」的理由 ✓
- 每个 `fail` 都带上下文（哪个参数、期望什么、实际得到什么）✓
- 「为什么」注释到位：端口写死的理由、重复地址为何必然是错、偶数节点为何只警告 ✓
- 关键节点有 `log`（校验通过）✓

- [ ] **Step 7: Commit**

```bash
git add deploy/quickstart.sh deploy/quickstart_test.sh
git commit -m "$(cat <<'EOF'
feat(deploy): quickstart.sh 骨架与参数校验

一键安装脚本的第一片：参数解析、自洽校验、可测结构。

可测性是刻意设计：main 入口被 BASH_SOURCE 守卫包住，测试脚本 source 它
就能只调单个函数而不触发安装；写盘路径全部是可覆盖变量，测试重定向到
临时目录，不需要 root、不碰 /etc 与 /var。

全部参数校验在动任何盘之前完成——装到一半才发现参数错，现场比一开始
就拒绝难收拾得多。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: 凭据生成与配置生成

**Files:**
- Modify: `deploy/quickstart.sh`（新增函数 + 扩充 `main`）
- Modify: `deploy/quickstart_test.sh`（新增用例）

**Interfaces:**
- Consumes: Task 4 的 `OPT_*` 变量与 `log/warn/fail`
- Produces:
  - `gen_password` → 24 位随机口令写 stdout
  - `resolve_credentials` → 设置 `CRED_USER` `CRED_PASS` `CRED_GENERATED`（1=自动生成）
  - `resolve_advertise_host` → 设置 `ADVERTISE_HOST`
  - `render_config` → 完整 yaml 写 stdout

- [ ] **Step 1: 追加失败测试**

在 `deploy/quickstart_test.sh` 的 `echo` 汇总行之前插入：

```bash
echo "== 凭据 =="

# 生成的口令：24 位，只含大小写字母与数字
gen_out="$( ( # shellcheck source=/dev/null
  source "${HERE}/quickstart.sh" >/dev/null 2>&1; gen_password ) )"
check "生成口令长度为 24" "${#gen_out}" "24"
if [[ "${gen_out}" =~ ^[A-Za-z0-9]{24}$ ]]; then ok "生成口令字符集合法"; else bad "生成口令含非法字符：${gen_out}"; fi

# 两次生成必须不同（随机源真的在随机）
gen2="$( ( # shellcheck source=/dev/null
  source "${HERE}/quickstart.sh" >/dev/null 2>&1; gen_password ) )"
if [[ "${gen_out}" != "${gen2}" ]]; then ok "两次生成的口令不同"; else bad "两次生成的口令相同，随机源可疑"; fi

echo "== 配置生成 =="

# 用固定的口令生成器桩，让配置文本可逐字比对
render() {
  ( # shellcheck source=/dev/null
    source "${HERE}/quickstart.sh" >/dev/null 2>&1
    gen_password() { printf 'FIXEDPASSWORD0123456789A'; }
    parse_args "$@" && validate_args && resolve_credentials && resolve_advertise_host && render_config )
}

single="$(render)"
for want in 'data_dir: "/var/lib/sq"' 'advertise_host: "127.0.0.1"' 'admin_username: "admin"' 'admin_password: "FIXEDPASSWORD0123456789A"'; do
  if grep -qF "${want}" <<< "${single}"; then ok "单机配置含 ${want}"; else bad "单机配置缺 ${want}：\n${single}"; fi
done
if grep -q '^cluster:' <<< "${single}"; then bad "单机配置不应有 cluster 段"; else ok "单机配置无 cluster 段"; fi

cl="$(render --cluster --node-id 2 --peers 10.0.0.1,10.0.0.2,10.0.0.3)"
for want in 'advertise_host: "10.0.0.2"' 'node_id: 2' 'raft_listen: ":9081"' 'data_groups: 3' 'ack: quorum-mem' \
            '{ id: 1, raft_addr: "10.0.0.1:9081", advertise_host: "10.0.0.1", advertise_port: 8081, admin_port: 8082 }' \
            '{ id: 3, raft_addr: "10.0.0.3:9081", advertise_host: "10.0.0.3", advertise_port: 8081, admin_port: 8082 }'; do
  if grep -qF "${want}" <<< "${cl}"; then ok "集群配置含 ${want}"; else bad "集群配置缺 ${want}：\n${cl}"; fi
done

noauth="$(render --no-admin-auth)"
if grep -q 'admin_password' <<< "${noauth}"; then bad "--no-admin-auth 时不应出现 admin_password"; else ok "--no-admin-auth 时无凭据行"; fi

explicit="$(render --admin-user ops --admin-password 'sekrit')"
for want in 'admin_username: "ops"' 'admin_password: "sekrit"'; do
  if grep -qF "${want}" <<< "${explicit}"; then ok "显式凭据原样落盘：${want}"; else bad "显式凭据未落盘：${want}"; fi
done
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./deploy/quickstart_test.sh`
Expected: 新增用例失败（`gen_password: command not found` 等）

- [ ] **Step 3: 实现凭据与地址解析**

在 `deploy/quickstart.sh` 的 `validate_args` 之后、`main` 之前插入：

```bash
# —— 凭据与地址 ——
CRED_USER=""
CRED_PASS=""
CRED_GENERATED=0
ADVERTISE_HOST=""

# gen_password 生成一个 24 位随机口令（大小写字母 + 数字）写 stdout。
#
# 用 /dev/urandom 而不是 $RANDOM：后者是 16 位 LCG，不是密码学随机源。
# 不用 openssl rand：openssl 不是所有精简发行版都装。
# LC_ALL=C 是必须的——某些 locale 下 tr 的字符类会按多字节解释而吐出乱码。
gen_password() {
  LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 24
}

# resolve_credentials 定下最终的管理面凭据，设置 CRED_USER/CRED_PASS/CRED_GENERATED。
#
# 默认生成、不默认敞开：一键脚本默默开一个无鉴权管理面是不可接受的默认值。
# 要免鉴权必须显式 --no-admin-auth。
resolve_credentials() {
  if [[ ${OPT_NO_ADMIN_AUTH} -eq 1 ]]; then
    log "按 --no-admin-auth 关闭管理面鉴权"
    return 0
  fi
  CRED_USER="${OPT_ADMIN_USER:-admin}"
  if [[ -n "${OPT_ADMIN_PASS}" ]]; then
    CRED_PASS="${OPT_ADMIN_PASS}"
    log "使用显式传入的管理面凭据（用户名 ${CRED_USER}）"
  else
    CRED_PASS="$(gen_password)"
    CRED_GENERATED=1
    log "已自动生成管理面口令（用户名 ${CRED_USER}）"
  fi
  [[ -n "${CRED_PASS}" ]] || fail "生成管理面口令失败（/dev/urandom 不可读？）"
}

# resolve_advertise_host 定下本机对外地址，设置 ADVERTISE_HOST。
#
# 集群档自动取 peers[node-id-1]——成员表里已经写着本机地址，再让用户
# 传一次只会制造不一致。单机档默认 127.0.0.1（与进程内默认一致），
# 此时远程客户端连不上，必须显式警告：这是最常见的「装完连不上」原因。
resolve_advertise_host() {
  if [[ -n "${OPT_ADVERTISE_HOST}" ]]; then
    ADVERTISE_HOST="${OPT_ADVERTISE_HOST}"
    return 0
  fi
  if [[ ${OPT_CLUSTER} -eq 1 ]]; then
    ADVERTISE_HOST="${OPT_PEERS[$(( OPT_NODE_ID - 1 ))]}"
    return 0
  fi
  ADVERTISE_HOST="127.0.0.1"
  warn "advertise_host 取默认值 127.0.0.1：远程客户端将连不上本机。需要远程访问请加 --advertise-host <本机对外地址>"
}

# render_config 把完整的 sq.yaml 写到 stdout。
#
# 只写与本机部署形态相关的字段——进程内默认值（internal/config/config.go
# 的 Load）与 sq.example.yaml 逐字段一致，省略的字段行为完全可预测。
# 抄那份 100 行注释版会让「脚本决定的」和「默认值」混成一片，出问题时
# 看不出这台机器的身份是什么。
render_config() {
  local shape="单机"
  [[ ${OPT_CLUSTER} -eq 1 ]] && shape="集群，本机 node_id=${OPT_NODE_ID}"
  printf '# 本文件由 sq quickstart.sh 生成，形态：%s\n' "${shape}"
  printf '# 未列出的字段走进程内默认值，字段说明见 %s\n' "${EXAMPLE_DST}"
  printf 'data_dir: "%s"\n' "${SQ_DATA_DIR}"
  printf 'advertise_host: "%s"\n' "${ADVERTISE_HOST}"
  if [[ ${OPT_NO_ADMIN_AUTH} -ne 1 ]]; then
    printf 'admin_username: "%s"\n' "${CRED_USER}"
    printf 'admin_password: "%s"\n' "${CRED_PASS}"
  fi
  [[ ${OPT_CLUSTER} -eq 1 ]] || return 0
  printf 'cluster:\n'
  printf '  node_id: %s\n' "${OPT_NODE_ID}"
  printf '  raft_listen: ":%s"\n' "${RAFT_PORT}"
  printf '  data_groups: 3\n'
  printf '  ack: quorum-mem\n'
  printf '  peers:\n'
  local i host
  for i in "${!OPT_PEERS[@]}"; do
    host="${OPT_PEERS[$i]}"
    # admin_port 显式写上而不是留空：留空会走「回落取本机端口」的兼容
    # 路径，那条路径隐含各节点端口一致的假设。生成的配置没有理由依赖假设。
    printf '    - { id: %d, raft_addr: "%s:%s", advertise_host: "%s", advertise_port: %s, admin_port: %s }\n' \
      "$(( i + 1 ))" "${host}" "${RAFT_PORT}" "${host}" "${GRPC_PORT}" "${ADMIN_PORT}"
  done
}
```

同时把 `main` 改成：

```bash
main() {
  parse_args "$@"
  validate_args
  log "参数校验通过"
  resolve_credentials
  resolve_advertise_host
  log "即将写入的配置："
  render_config | sed 's/^/[quickstart]   /'
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `./deploy/quickstart_test.sh`
Expected: 全部通过，失败 0

- [ ] **Step 5: 跑 shellcheck**

Run: `shellcheck deploy/quickstart.sh deploy/quickstart_test.sh`
Expected: 无输出

- [ ] **Step 6: 加日志与注释自检**

- 关键节点有 `log`：校验通过、凭据来源（生成 vs 显式 vs 关闭）、即将写入的配置全文 ✓
- 错误分支带上下文：`gen_password` 失败点明 `/dev/urandom` ✓
- **成功路径不静默**：凭据解析成功会记一行、配置内容会整份回显 ✓
- 「为什么」注释到位：为何不用 `$RANDOM`/`openssl`、为何要 `LC_ALL=C`、为何默认生成而非默认敞开、为何显式写 `admin_port` 而不是留空吃回落、为何是最小配置 ✓

- [ ] **Step 7: Commit**

```bash
git add deploy/quickstart.sh deploy/quickstart_test.sh
git commit -m "$(cat <<'EOF'
feat(deploy): quickstart.sh 的凭据生成与配置生成

管理面口令默认自动生成（24 位 /dev/urandom），要免鉴权必须显式
--no-admin-auth——一键脚本默默开一个无鉴权管理面是不可接受的默认值。

生成的是最小配置而非那份 100 行注释版：进程内默认值与 sq.example.yaml
逐字段一致，省略的字段行为完全可预测；混在一起反而看不出这台机器的身份。

peers 的 admin_port 显式写上而不是留空吃回落——回落隐含各节点端口一致
的假设，生成的配置没有理由依赖假设。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: 取包、重跑语义、装盘衔接、权限收口与结尾提示

**Files:**
- Modify: `deploy/quickstart.sh`
- Modify: `deploy/quickstart_test.sh`

**Interfaces:**
- Consumes: Task 4/5 的全部函数与变量
- Produces:
  - `preflight` / `detect_arch` / `resolve_tarball` / `acquire_package` → 设置 `PKG_DIR`
  - `check_existing` / `backup_config` / `reuse_existing_password`
  - `write_config` / `run_installer` / `tighten_perms` / `print_next_steps`

- [ ] **Step 1: 追加失败测试**

在 `deploy/quickstart_test.sh` 汇总行之前插入：

```bash
echo "== 架构识别 =="
arch_of() { ( # shellcheck source=/dev/null
  source "${HERE}/quickstart.sh" >/dev/null 2>&1
  uname() { if [[ "${1:-}" == "-m" ]]; then printf '%s' "$1_STUB"; fi; }
  detect_arch "$1" ) }
check "x86_64 → amd64"  "$(arch_of x86_64)"  "amd64"
check "aarch64 → arm64" "$(arch_of aarch64)" "arm64"
check "arm64 → arm64"   "$(arch_of arm64)"   "arm64"
bad_arch="$( ( set +e; # shellcheck source=/dev/null
  source "${HERE}/quickstart.sh" >/dev/null 2>&1; detect_arch armv7l >/dev/null 2>&1; echo $? ) )"
if [[ "${bad_arch}" != "0" ]]; then ok "不支持的架构被拒绝"; else bad "armv7l 应当被拒绝"; fi

echo "== 重跑语义 =="

tmp_env() {
  local d="$1"; shift
  ( export SQ_CFG_DIR="${d}/etc" SQ_DATA_DIR="${d}/data"
    mkdir -p "${SQ_CFG_DIR}" "${SQ_DATA_DIR}"
    # shellcheck source=/dev/null
    source "${HERE}/quickstart.sh" >/dev/null 2>&1
    "$@" )
}

d="$(mktemp -d)"; trap 'rm -rf "${d}"' EXIT
printf 'data_dir: "/var/lib/sq"\nadmin_password: "OLDPASSWORD"\n' > "${d}/etc/sq.yaml" 2>/dev/null || { mkdir -p "${d}/etc"; printf 'data_dir: "/var/lib/sq"\nadmin_password: "OLDPASSWORD"\n' > "${d}/etc/sq.yaml"; }

code="$( ( set +e; tmp_env "${d}" bash -c 'parse_args && validate_args && check_existing' >/dev/null 2>&1; echo $? ) )"
if [[ "${code}" != "0" ]]; then ok "已有配置时默认拒绝重跑"; else bad "已有配置时应拒绝"; fi

if [[ -f "${d}/etc/sq.yaml" ]] && grep -q OLDPASSWORD "${d}/etc/sq.yaml"; then
  ok "拒绝重跑时未修改任何文件"
else
  bad "拒绝重跑时不应动文件"
fi

reused="$(tmp_env "${d}" bash -c 'reuse_existing_password')"
check "--force 从旧配置复用口令" "${reused}" "OLDPASSWORD"

echo "== 结尾提示 =="
tips="$( ( # shellcheck source=/dev/null
  source "${HERE}/quickstart.sh" >/dev/null 2>&1
  gen_password() { printf 'FIXEDPASSWORD0123456789A'; }
  parse_args --cluster --node-id 1 --peers 10.0.0.1,10.0.0.2,10.0.0.3 \
    && validate_args && resolve_credentials && resolve_advertise_host && print_next_steps ) )"
for want in 'systemctl enable --now sq' 'sq status' 'FIXEDPASSWORD0123456789A' '--admin-password'; do
  if grep -qF "${want}" <<< "${tips}"; then ok "结尾提示含 ${want}"; else bad "结尾提示缺 ${want}：\n${tips}"; fi
done

tips_single="$( ( # shellcheck source=/dev/null
  source "${HERE}/quickstart.sh" >/dev/null 2>&1
  gen_password() { printf 'FIXEDPASSWORD0123456789A'; }
  parse_args && validate_args && resolve_credentials && resolve_advertise_host && print_next_steps ) )"
if grep -qF -- '--admin-password' <<< "${tips_single}"; then
  bad "单机档不应打印集群凭据传递提示"
else
  ok "单机档不打印集群凭据传递提示"
fi
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./deploy/quickstart_test.sh`
Expected: 新增用例失败（`detect_arch: command not found` 等）

- [ ] **Step 3: 实现前置校验与取包**

在 `deploy/quickstart.sh` 里，`render_config` 之后插入：

```bash
# —— 前置校验与取包 ——
PKG_DIR=""
PKG_TMP=""

# preflight 检查运行环境。全部在动盘之前完成。
preflight() {
  [[ ${EUID} -eq 0 ]] || fail "需要 root 权限（建用户、写 /usr/local/bin 与 /etc）"
  [[ "$(uname -s)" == "Linux" ]] || fail "只支持 Linux（本机 $(uname -s)）；macOS 发布包存在但没有 systemd，装了也托管不起来"
  command -v systemctl >/dev/null 2>&1 || fail "未找到 systemctl，本脚本只支持 systemd 系统"
  log "环境检查通过：Linux + systemd + root"
}

# detect_arch 把 uname -m 翻译成发布包的架构名。
#
# 参数：$1 = uname -m 的输出（显式传入而非直接读，便于单测）
# 输出：amd64 | arm64（写 stdout）
detect_arch() {
  case "$1" in
    x86_64)        printf 'amd64' ;;
    aarch64|arm64) printf 'arm64' ;;
    *)             fail "不支持的架构 $1（发布包只有 linux/amd64 与 linux/arm64）" ;;
  esac
}

# resolve_version 定下要下载的版本号，写 stdout。
#
# --version 未给时问 GitHub 最新 release。这一步失败**不是致命错**：
# 未认证的 GitHub API 有每 IP 60 次/小时限流，国内网络还可能直接不通。
# 报错必须给出两个逃生口，否则用户只能干瞪眼。
resolve_version() {
  if [[ -n "${OPT_VERSION}" ]]; then printf '%s' "${OPT_VERSION}"; return 0; fi
  log "查询最新 release：${SQ_RELEASE_API}" >&2
  local body tag
  if ! body="$(curl -fsSL "${SQ_RELEASE_API}" 2>&1)"; then
    fail "查询最新版本失败（${SQ_RELEASE_API}）：${body}
       逃生口一：显式指定版本  --version 0.1.0
       逃生口二：直接给包路径  --tarball ./sq_0.1.0_linux_amd64.tar.gz"
  fi
  tag="$(printf '%s' "${body}" | grep -o '"tag_name"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')"
  [[ -n "${tag}" ]] || fail "从 release 响应里解析不出 tag_name，请改用 --version 或 --tarball"
  printf '%s' "${tag#v}"
}

# acquire_package 把发布包准备好，设置 PKG_DIR 指向含 sq/install.sh/sq.service 的目录。
#
# 两条路径最终汇合到同一个状态，后续步骤完全共用：
#   1. 同目录已有 sq 二进制 → 直接用脚本所在目录（发布包内场景）
#   2. 否则下载 tarball 解包到临时目录
acquire_package() {
  local here; here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  if [[ -z "${OPT_TARBALL}" && -f "${here}/sq" && -f "${here}/install.sh" ]]; then
    PKG_DIR="${here}"
    log "使用脚本所在目录的发布包：${PKG_DIR}"
    return 0
  fi
  PKG_TMP="$(mktemp -d)"
  # trap 放在这里而不是脚本顶部：只有真的建了临时目录才需要清理
  trap 'rm -rf "${PKG_TMP}"' EXIT
  local tarball="${PKG_TMP}/pkg.tar.gz"

  if [[ -n "${OPT_TARBALL}" && -f "${OPT_TARBALL}" ]]; then
    log "使用本地发布包：${OPT_TARBALL}（用户自带的包，跳过校验和）"
    cp "${OPT_TARBALL}" "${tarball}" || fail "复制 ${OPT_TARBALL} 失败"
  elif [[ -n "${OPT_TARBALL}" ]]; then
    log "从指定 URL 下载：${OPT_TARBALL}（旁路来源无 SHA256SUMS，跳过校验和）"
    curl -fsSL -o "${tarball}" "${OPT_TARBALL}" || fail "下载 ${OPT_TARBALL} 失败"
  else
    local arch ver name
    arch="$(detect_arch "$(uname -m)")"
    ver="$(resolve_version)"
    name="sq_${ver}_linux_${arch}.tar.gz"
    log "下载发布包：${SQ_DOWNLOAD_BASE}/v${ver}/${name}"
    curl -fsSL -o "${tarball}" "${SQ_DOWNLOAD_BASE}/v${ver}/${name}" \
      || fail "下载 ${name} 失败（版本或架构不存在？可用 --tarball 指定本地包）"
    log "校验 SHA256"
    curl -fsSL -o "${PKG_TMP}/SHA256SUMS" "${SQ_DOWNLOAD_BASE}/v${ver}/SHA256SUMS" \
      || fail "下载 SHA256SUMS 失败，无法校验完整性（要跳过校验请改用 --tarball）"
    local want got
    want="$(grep -F "${name}" "${PKG_TMP}/SHA256SUMS" | awk '{print $1}')"
    [[ -n "${want}" ]] || fail "SHA256SUMS 里没有 ${name} 的记录"
    got="$(sha256sum "${tarball}" | awk '{print $1}')"
    # 校验和不匹配即中止：包可能被截断或被替换，装上去比装不上更糟
    [[ "${want}" == "${got}" ]] || fail "SHA256 校验失败：期望 ${want}，实际 ${got}"
    log "SHA256 校验通过"
  fi

  tar -xzf "${tarball}" -C "${PKG_TMP}" || fail "解包失败：${tarball}"
  PKG_DIR="${PKG_TMP}"
  local f
  for f in sq install.sh sq.service sq.example.yaml; do
    [[ -f "${PKG_DIR}/${f}" ]] || fail "发布包缺少 ${f}（期望在 ${PKG_DIR} 下）"
  done
  log "发布包就绪：${PKG_DIR}"
}
```

- [ ] **Step 4: 实现重跑语义、装盘衔接与权限收口**

继续追加：

```bash
# —— 重跑语义 ——

# check_existing 检测已有安装。默认拒绝重跑，--force 才继续。
#
# 为什么默认拒绝：用户第一次把 --peers 写错了，改参数重跑，若沿用
# install.sh 的「已存在不覆盖」语义，跑的仍是旧配置而用户以为已生效。
# 静默保留错配置比明确拒绝危险得多。
check_existing() {
  local existing=0
  [[ -f "${CFG_DST}" ]] && existing=1
  [[ -d "${SQ_DATA_DIR}" ]] && [[ -n "$(ls -A "${SQ_DATA_DIR}" 2>/dev/null)" ]] && existing=1
  [[ ${existing} -eq 1 ]] || return 0

  if [[ ${OPT_FORCE} -ne 1 ]]; then
    warn "检测到已有安装："
    [[ -f "${CFG_DST}" ]] && warn "  配置：${CFG_DST}（形态：$(config_shape "${CFG_DST}")）"
    [[ -d "${SQ_DATA_DIR}" ]] && warn "  数据：${SQ_DATA_DIR}（$(du -sh "${SQ_DATA_DIR}" 2>/dev/null | awk '{print $1}')）"
    fail "已有安装，未做任何改动。确认要覆盖配置请加 --force（数据目录永远不会被本脚本删除）"
  fi
  log "--force 已给：将备份旧配置后覆盖；数据目录不动"
}

# config_shape 从一份已有配置里读出部署形态，用于拒绝重跑时的现状报告。
config_shape() {
  if grep -q '^cluster:' "$1" 2>/dev/null; then
    printf '集群，node_id=%s' "$(grep -E '^[[:space:]]+node_id:' "$1" | head -1 | awk '{print $2}')"
  else
    printf '单机'
  fi
}

# reuse_existing_password 从已有配置里读回 admin_password 写 stdout；读不到则空。
#
# --force 重跑且未显式给口令时复用旧口令：否则一次无关的重跑（比如只改
# --advertise-host）会静默换掉口令，让这台机器与另两台失配，
# sq status 从此永远降级。
reuse_existing_password() {
  [[ -f "${CFG_DST}" ]] || return 0
  grep -E '^admin_password:' "${CFG_DST}" 2>/dev/null | head -1 | sed 's/^admin_password:[[:space:]]*//; s/^"//; s/"$//'
}

# backup_config 把旧配置改名保留，写 stdout 返回备份路径。
backup_config() {
  local bak="${CFG_DST}.bak.$(date +%Y%m%d%H%M%S)"
  mv "${CFG_DST}" "${bak}" || fail "备份旧配置到 ${bak} 失败"
  printf '%s' "${bak}"
}

# —— 落盘 ——

# write_config 生成配置并以 0600 root:root 写入。
#
# 权限承重：配置里有明文口令，写出来的那一刻就不能是世界可读。属组要等
# install.sh 建完 sq 用户才存在，所以这里先 0600 root:root，
# 收紧到 0640 root:sq 由 tighten_perms 在装盘之后做。
write_config() {
  install -d -m 0755 "${SQ_CFG_DIR}" || fail "创建 ${SQ_CFG_DIR} 失败"
  if [[ -f "${CFG_DST}" ]]; then
    local bak; bak="$(backup_config)"
    log "旧配置已备份：${bak}"
  fi
  local tmp="${SQ_CFG_DIR}/.sq.yaml.tmp.$$"
  ( umask 077; render_config > "${tmp}" ) || fail "生成配置失败"
  chmod 0600 "${tmp}" || fail "设置 ${tmp} 权限失败"
  mv "${tmp}" "${CFG_DST}" || fail "写入 ${CFG_DST} 失败"
  log "配置已写入：${CFG_DST}（0600 root:root，稍后收紧为 0640 root:${SQ_USER}）"
}

# run_installer 委托发布包内的 install.sh 装盘。
#
# 它会看到 ${CFG_DST} 已存在而走「保留不动」分支——这正是本脚本先写配置
# 的原因。install.sh 一字不改。
run_installer() {
  log "委托 install.sh 装盘：${PKG_DIR}/install.sh"
  SQ_CFG_DIR="${SQ_CFG_DIR}" bash "${PKG_DIR}/install.sh" || fail "install.sh 执行失败（上面的 [install] 日志是现场）"
  log "装盘完成"
}

# tighten_perms 把配置收紧到 0640 root:sq，并铺一份字段说明。
#
# 必须在 install.sh 之后：sq 系统用户是它建的，此前 chown 会失败。
tighten_perms() {
  id -u "${SQ_USER}" >/dev/null 2>&1 || fail "系统用户 ${SQ_USER} 不存在（install.sh 应当已创建它）"
  chown "root:${SQ_USER}" "${CFG_DST}" || fail "设置 ${CFG_DST} 属主失败"
  chmod 0640 "${CFG_DST}" || fail "设置 ${CFG_DST} 权限失败"
  log "配置权限已收紧：0640 root:${SQ_USER}"
  install -m 0644 "${PKG_DIR}/sq.example.yaml" "${EXAMPLE_DST}" || fail "写入 ${EXAMPLE_DST} 失败"
  log "字段说明已铺好：${EXAMPLE_DST}"
}

# print_next_steps 打印装完之后该做什么。
#
# 本脚本刻意不启动服务（理由见文件头），所以这段提示是用户唯一的指引，
# 必须自足：配置在哪、凭据是什么、怎么启动、怎么开机自启、怎么看状态。
print_next_steps() {
  local shape="单机"
  [[ ${OPT_CLUSTER} -eq 1 ]] && shape="集群 node_id=${OPT_NODE_ID} / ${#OPT_PEERS[@]} 节点"
  printf '\n'
  log "安装完成。形态：${shape}"
  printf '\n'
  log "配置文件：${CFG_DST}   （0640 root:${SQ_USER}，字段说明见 ${EXAMPLE_DST}）"
  log "数据目录：${SQ_DATA_DIR}"
  log "二进制  ：${SQ_BIN_DST}"
  if [[ ${OPT_NO_ADMIN_AUTH} -ne 1 ]]; then
    printf '\n'
    log "控制台   ：http://${ADVERTISE_HOST}:${ADMIN_PORT}/"
    log "用户名   ：${CRED_USER}"
    if [[ ${CRED_GENERATED} -eq 1 ]]; then
      log "密码     ：${CRED_PASS} （已自动生成，也在配置文件里）"
    else
      log "密码     ：（你传入的值，见配置文件）"
    fi
  fi
  printf '\n'
  log "立即启动    ：systemctl start sq"
  log "开机自启    ：systemctl enable sq"
  log "一步到位    ：systemctl enable --now sq"
  log "进程状态    ：systemctl status sq"
  log "集群状态    ：sq status -config ${CFG_DST}"
  printf '\n'
  if [[ ${OPT_NO_ADMIN_AUTH} -eq 1 ]]; then
    warn "管理面 :${ADMIN_PORT} 无鉴权，请用防火墙限制来源。"
  fi
  [[ ${OPT_CLUSTER} -eq 1 ]] || return 0
  warn "三台都装完并启动后，集群才会选出 leader（在此之前 sq status 报退出码 2 是预期行为）。"
  [[ ${CRED_GENERATED} -eq 1 ]] || return 0
  warn "另外两台必须使用同一组凭据，否则 sq status 无法跨节点查看。在其余机器上执行："
  local i
  for i in "${!OPT_PEERS[@]}"; do
    [[ $(( i + 1 )) -eq ${OPT_NODE_ID} ]] && continue
    warn "    ./quickstart.sh --cluster --node-id $(( i + 1 )) --peers $(IFS=,; echo "${OPT_PEERS[*]}") \\"
    warn "      --admin-user ${CRED_USER} --admin-password '${CRED_PASS}'"
  done
}
```

把 `main` 改成完整流程：

```bash
main() {
  parse_args "$@"
  validate_args
  log "参数校验通过"
  preflight
  check_existing
  # --force 重跑且未显式给口令时，先把旧口令捞出来当默认值——必须在
  # write_config 备份/覆盖旧文件之前做，否则就读不到了
  if [[ ${OPT_FORCE} -eq 1 && -z "${OPT_ADMIN_PASS}" && ${OPT_NO_ADMIN_AUTH} -ne 1 ]]; then
    local old; old="$(reuse_existing_password)"
    if [[ -n "${old}" ]]; then
      OPT_ADMIN_PASS="${old}"
      log "--force 重跑：复用旧配置里的管理面口令（避免与其余节点失配）"
    fi
  fi
  resolve_credentials
  resolve_advertise_host
  acquire_package
  log "即将写入的配置："
  render_config | sed 's/^/[quickstart]   /'
  write_config
  run_installer
  tighten_perms
  print_next_steps
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `./deploy/quickstart_test.sh`
Expected: 全部通过，失败 0

- [ ] **Step 6: 跑 shellcheck**

Run: `shellcheck deploy/quickstart.sh deploy/quickstart_test.sh`
Expected: 无输出

- [ ] **Step 7: 本地容器烟测（不需要真机）**

```bash
docker run --rm -v "$PWD:/src" -w /src debian:12 bash -c '
  apt-get update -qq && apt-get install -y -qq curl ca-certificates systemd >/dev/null 2>&1
  ./deploy/quickstart.sh --help
  ./deploy/quickstart.sh --cluster --node-id 2 --peers 10.0.0.1,10.0.0.2,10.0.0.3 --tarball /nonexistent 2>&1 | tail -5
'
```
Expected: `--help` 正常输出；第二条因包不存在而以 `[quickstart] 失败：` 开头的报错退出（证明前置校验与取包的错误路径都带上下文）

- [ ] **Step 8: 加日志与注释自检**

- 关键节点 `log`：环境检查通过、包来源、下载、校验和通过、包就绪、配置写入、装盘完成、权限收紧、说明铺好 ✓
- 外部调用前后有日志：curl 下载前 log、失败 fail 带 URL；tar 解包失败带路径；install.sh 调用前后各一条 ✓
- 每个错误分支带上下文（哪个文件、哪个 URL、期望值 vs 实际值）✓
- **成功路径不静默**：每一步都有成功日志，结尾提示自足 ✓
- 「为什么」注释到位：为何默认拒绝重跑、为何 --force 也不碰数据、为何先 0600 后 0640、为何 chown 必须在 install.sh 之后、为何 trap 放在 mktemp 之后、为何校验和失败要中止 ✓

- [ ] **Step 9: Commit**

```bash
git add deploy/quickstart.sh deploy/quickstart_test.sh
git commit -m "$(cat <<'EOF'
feat(deploy): quickstart.sh 取包、重跑语义、装盘衔接与权限收口

取包两条路径（同目录发布包 / 下载并校验 SHA256）汇合到同一状态，
后续步骤共用。重跑默认拒绝并报告现状，--force 才备份覆盖，
数据目录永不触碰——data_groups 首启即持久化，盘上是事实。

权限分两步且不可合并：配置含明文口令，写出来即 0600 root:root，
不留世界可读的时间窗；收紧到 0640 root:sq 必须等 install.sh 建完
sq 用户之后。install.sh 仍一字不改，靠「配置已存在则不覆盖」让路。

--force 且未显式给口令时从旧配置复用，避免一次无关重跑静默换掉口令
造成集群内凭据失配。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: CI 与发布打包

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `.github/workflows/release.yml:79`（打包时带上 quickstart.sh）

**Interfaces:**
- Consumes: Task 4-6 的 `deploy/quickstart.sh` 与 `deploy/quickstart_test.sh`
- Produces: 无（CI 与打包）

**范围说明**：本仓库目前没有任何测试 CI，只有 `release.yml`。本 task 新建的 `ci.yml` **刻意只跑 shell 相关检查**，不跑 `go test ./...`——Go 测试从未在 CI 跑过，贸然全量接进来会被无关的既有 flake 挡住本项交付。要不要给 Go 测试建 CI 是另一个决定，不在本计划范围。

- [ ] **Step 1: 建 `.github/workflows/ci.yml`**

```yaml
# ci.yml —— shell 交付件的持续检查。
#
# 职责：对 deploy/ 下的脚本跑 shellcheck 与自带的测试脚本。
# 边界：**刻意不跑 go test**。本仓库的 Go 测试从未在 CI 跑过，全量接进来
#       会被无关的既有 flake 挡住；要不要给 Go 建 CI 是另一个决定。
name: ci

on:
  push:
    paths:
      - 'deploy/**'
      - '.github/workflows/ci.yml'
  pull_request:
    paths:
      - 'deploy/**'
      - '.github/workflows/ci.yml'
  workflow_dispatch:

jobs:
  shell:
    name: shell 检查
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5

      - name: 安装 shellcheck
        run: sudo apt-get update -qq && sudo apt-get install -y -qq shellcheck

      - name: shellcheck
        run: shellcheck deploy/*.sh

      - name: quickstart 测试
        run: ./deploy/quickstart_test.sh
```

- [ ] **Step 2: 本地验证 workflow 语法**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"`
Expected: `yaml ok`

- [ ] **Step 3: 本地跑一遍 CI 会跑的两条命令**

Run: `shellcheck deploy/*.sh && ./deploy/quickstart_test.sh`
Expected: shellcheck 无输出；测试脚本 `失败 0`

- [ ] **Step 4: 让发布包带上 quickstart.sh**

修改 `.github/workflows/release.yml` 里那行 `cp deploy/sq.service deploy/install.sh "${stage}/"`：

```bash
            cp deploy/sq.service deploy/install.sh deploy/quickstart.sh "${stage}/"
            chmod +x "${stage}/install.sh" "${stage}/quickstart.sh"
```

（原本只有 `chmod +x "${stage}/install.sh"`，一并改掉。）

- [ ] **Step 5: 本地模拟打包，确认产物齐全**

```bash
stage="$(mktemp -d)"
go build -o "${stage}/sq" ./cmd/sq
cp sq.example.yaml LICENSE README.md "${stage}/"
cp deploy/sq.service deploy/install.sh deploy/quickstart.sh "${stage}/"
chmod +x "${stage}/install.sh" "${stage}/quickstart.sh"
tar -czf /tmp/sq_test.tar.gz -C "${stage}" .
tar -tzf /tmp/sq_test.tar.gz | sort
rm -rf "${stage}" /tmp/sq_test.tar.gz
```
Expected: 列表含 `./quickstart.sh`、`./install.sh`、`./sq`、`./sq.service`、`./sq.example.yaml`、`./LICENSE`、`./README.md`

- [ ] **Step 6: 验证发布包内场景真的走「本地包」分支**

```bash
stage="$(mktemp -d)"
go build -o "${stage}/sq" ./cmd/sq
cp sq.example.yaml "${stage}/"; cp deploy/sq.service deploy/install.sh deploy/quickstart.sh "${stage}/"
chmod +x "${stage}/install.sh" "${stage}/quickstart.sh"
# 非 root 跑，应当停在 preflight 而不是去下载
"${stage}/quickstart.sh" 2>&1 | tail -3
rm -rf "${stage}"
```
Expected: 报「需要 root 权限」——证明它没有先去联网下载（取包在 preflight 之后）

- [ ] **Step 7: Commit**

```bash
git add .github/workflows/ci.yml .github/workflows/release.yml
git commit -m "$(cat <<'EOF'
ci: 新增 shell 检查工作流，发布包带上 quickstart.sh

ci.yml 只跑 shellcheck 与 quickstart 自带的测试脚本，刻意不跑 go test：
本仓库的 Go 测试从未在 CI 跑过，全量接进来会被无关的既有 flake 挡住本项
交付。要不要给 Go 建 CI 是另一个决定。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: 文档

**Files:**
- Modify: `README.md`（快速开始、功能一览）
- Modify: `docs/deployment.md`（新增 quickstart 小节，改写 data_dir 提醒）
- Modify: `docs/configuration.md`（集群配置补 `admin_port`）

**Interfaces:**
- Consumes: Task 1-7 的全部行为
- Produces: 无

**文风纪律**：仓库文档不吹、不承诺未验证的行为、不用感叹号。只写已经实现并测过的东西。

- [ ] **Step 1: 改 README 快速开始**

在 `README.md` 的「快速开始」里，把现有的 `curl` + `tar` + `sudo ./install.sh` 三行之后追加一段：

```markdown
装完需要自己编辑配置再启动。如果想让脚本把配置也生成好，用同一个包里的 `quickstart.sh`：

```bash
sudo ./quickstart.sh
```

它会生成 `/etc/sq/sq.yaml`（数据目录已指向 `/var/lib/sq`）、自动生成控制台口令、装好 systemd 单元，然后打印启动命令。它同样不会自动启动服务。

三节点集群在三台机器上各跑一次，只差 `--node-id`：

```bash
sudo ./quickstart.sh --cluster --node-id 1 --peers 10.0.0.1,10.0.0.2,10.0.0.3
```

第一台会自动生成控制台口令，另外两台需要用 `--admin-user`/`--admin-password` 带上同一组凭据，脚本结尾会打印可直接复制的命令。装完用 `sq status` 查看集群状态。
```

- [ ] **Step 2: 改 README 功能一览**

在「功能一览」表格的「Web 控制台」行之后插入一行：

```markdown
| 安装与自检 | `quickstart.sh` 一条命令完成装盘与配置；`sq status` 查看单机/集群状态 |
```

- [ ] **Step 3: 改 docs/deployment.md**

在「## Release 与 systemd」小节之后、「## 从源码构建」之前，插入新小节：

```markdown
## 一键安装（quickstart.sh）

`install.sh` 只负责装盘，配置要自己写。`quickstart.sh` 在它之上多做三件事：取包、生成配置、收紧配置权限。两者都不会自动启动服务。

单机：

```bash
sudo ./quickstart.sh
```

生成的 `/etc/sq/sq.yaml` 只包含与本机部署形态相关的字段（`data_dir`、`advertise_host`、管理面凭据），其余走进程内默认值；完整字段说明会被铺到 `/etc/sq/sq.example.yaml`。

三节点集群在三台机器上各跑一次，`--peers` 的顺序即 node id，三次执行只有 `--node-id` 不同：

```bash
# 10.0.0.1 上
sudo ./quickstart.sh --cluster --node-id 1 --peers 10.0.0.1,10.0.0.2,10.0.0.3
# 10.0.0.2 上（凭据取第一台生成的值）
sudo ./quickstart.sh --cluster --node-id 2 --peers 10.0.0.1,10.0.0.2,10.0.0.3 \
  --admin-user admin --admin-password '<第一台生成的口令>'
# 10.0.0.3 同上，--node-id 3
```

端口固定为 gRPC 8081、控制台 8082、raft 9081。需要改端口请装完后编辑 `/etc/sq/sq.yaml` 再重启。

脚本不 SSH、不分发，也不做多机编排。三台各跑一次是刻意的形态：SSH 推送要引入免密、跨机提权和半途失败回滚，这些前提失败时的现场比手工装三次更难排查。

### 管理面凭据

控制台口令默认自动生成（24 位），用户名默认 `admin`，两者都写进配置并在脚本结尾打印一次。要关掉鉴权必须显式传 `--no-admin-auth`。

集群的三个节点**必须使用同一组凭据**——`sq status` 在 follower 上会跳到 leader 的管理面取完整视图，凭据不一致会 401 并降级。第一台生成后，脚本结尾会打印带上该口令的完整命令供另两台复制。

配置文件含明文口令，权限为 `0640 root:sq`：sq 进程以 `sq` 用户运行需要读，其他普通用户读不到。

### 重复执行

检测到已有配置或非空数据目录时，脚本报告现状后退出，不做任何改动。确认要覆盖配置加 `--force`，旧配置会被改名为 `sq.yaml.bak.<时间戳>`。

`--force` **不会**删除数据目录。集群的 `data_groups` 首次启动即写盘、此后不可变，数据目录里是盘上事实，要换布局请自己确认后手工清理。

`--force` 且未显式传 `--admin-password` 时，会从旧配置里复用原口令，避免一次无关的重跑（比如只改 `--advertise-host`）静默换掉口令、让这台机器与其余节点失配。

### 离线与网络受限

脚本会按平台自动下载对应的发布包并核对 `SHA256SUMS`。两个逃生口：`--version 0.1.0` 跳过查询最新版本，`--tarball <路径或 URL>` 直接指定包（本地路径和旁路 URL 都跳过校验和）。发布包内自带 `quickstart.sh`，此时它直接用同目录的二进制，不联网。

## 查看运行状态（sq status）

```bash
sq status -config /etc/sq/sq.yaml
```

单机模式打印 topic、消费组、连接数、写入量与磁盘水位。集群模式打印成员表与各组的 leader、term、applied 和待 apply 量。

数据来自管理面 HTTP，因此 `admin_listen` 设为空（关闭管理面）时本命令不可用，会直接报错说明原因。配置里设了 `admin_username` 时命令会自动登录。

在 follower 节点上执行时，命令会跳到元数据组的 leader 再取一次完整视图——各 peer 的复制进度只在 leader 侧维护，不跳转就看不到其余节点的死活。跳不过去时降级为本机视角并明确标注，不会让整条命令失败。

退出码可以直接写进监控脚本：

| 码 | 含义 |
|---|---|
| 0 | 健康：全部组有 leader，全部 peer 活跃 |
| 1 | 够不着本机管理面：服务没起、管理面关闭、凭据错 |
| 2 | 集群降级：有组无 leader，或有 peer 失联 |
| 3 | 判定不完整：跳转 leader 失败，peer 活跃度不可见 |

三节点集群第一台装完启动后没有 leader，`sq status` 会报退出码 2，这是预期行为——raft 按全量成员表引导，凑不齐多数派就没有 leader，进程本身正常运行。
```

- [ ] **Step 4: 改 docs/deployment.md 的 data_dir 提醒**

现有「Release 与 systemd」小节里那句「安装完成后编辑配置，**明确把 `data_dir` 设为 `/var/lib/sq`**」改成：

```markdown
`install.sh` 安装完成后需要编辑配置，**明确把 `data_dir` 设为 `/var/lib/sq`**，然后启动。（走 `quickstart.sh` 不需要这一步——它生成的配置里已经写好了绝对路径。）
```

- [ ] **Step 5: 改 docs/configuration.md**

在集群配置的 peers 字段说明里补一行：

```markdown
- `admin_port`：该节点的管理面端口，可选。留空时 `sq status` 回落取本机 `admin_listen` 的端口（隐含各节点端口一致的假设）。集群运行时不读这个字段，它只服务于 `sq status` 的跨节点查询。`quickstart.sh` 生成的配置会显式写上。
```

（若该文档的集群小节结构与此不同，按其既有格式插入同等内容，不要照抄本行的 markdown 形态。）

- [ ] **Step 6: 通读自检**

Run: `grep -nE '！|极其|完美|轻松搞定|强大' README.md docs/deployment.md docs/configuration.md`
Expected: 无输出（无感叹号、无宣传语——与仓库既有文风一致）

再人工确认三点：
- 文档里承诺的每个行为，Task 1-7 都真的实现了；
- 退出码表与 `cmd/sq/statusview.go` 的四个常量逐一对应；
- 三节点示例里的凭据传递写清楚了，没有暗示"三台各跑一次就自动一致"。

- [ ] **Step 7: Commit**

```bash
git add README.md docs/deployment.md docs/configuration.md
git commit -m "$(cat <<'EOF'
docs: 补 quickstart.sh 与 sq status 的说明

deployment.md 新增一键安装与状态查看两节，含凭据传递、重复执行、
离线逃生口和退出码表。README 快速开始加 quickstart 用法。
configuration.md 补 peers.admin_port 字段。

明确写出「集群三台必须共用同一组凭据」——自动生成在三台上并不天然一致，
这是最容易踩的一处。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## 三机真机验收（由协调者执行，不派发）

> **这一节不是 task，不要交给执行者。** 它需要三台真实 Linux 机器，且第 5 步要动防火墙。执行者跑不了，硬跑会得到编造的结论。

前置：本地交叉编译一份 broker 与发布包（`GOOS=linux GOARCH=amd64 go build ./cmd/sq`），按 `prefer-cross-compile-over-remote-toolchain` 的既有做法 scp 过去，不要在远端装工具链。

1. 三台各跑一次 quickstart：第一台不给密码（走自动生成），后两台按结尾提示带上 `--admin-user/--admin-password`。确认三份配置只有身份字段不同、凭据完全一致、且都是 `0640 root:sq`。
2. 只启动第一台，跑 `sq status` —— 期望退出码 **2**（无 leader），验证「首台无 leader 是预期行为」。
3. 三台全部启动，在三台上分别跑 `sq status` —— 期望全部 **0**，且 follower 上「视角」行显示已跳到 leader。
4. 停掉一台，在 leader 上跑 —— 期望 **2** 且该节点标为失联。
5. 在某台 follower 上用防火墙挡掉到 leader 的 8082，跑 `sq status` —— 期望退出码 **3** 且输出标注 peer 进度不可见。

第 5 步尤其不能只靠单测：降级路径是整个设计里唯一一条依赖跨机网络的分支。

---

## Self-Review 记录

**1. spec 覆盖检查**

| spec 章节 | 落在哪个 task |
|---|---|
| §3.2 与 install.sh 的分层 | Task 6（`run_installer` + 先写配置的顺序） |
| §3.3 执行链路（含 0600→0640 两步） | Task 6（`main` 全流程 + `write_config`/`tighten_perms`） |
| §4.1 参数与校验 | Task 4 |
| §4.2 取包（平台、版本、校验和、逃生口） | Task 6（`detect_arch`/`resolve_version`/`acquire_package`） |
| §4.3 重跑语义 | Task 6（`check_existing`/`backup_config`） |
| §4.4 生成的配置 | Task 5（`render_config`） |
| §4.5 凭据与文件权限 | Task 5（生成、复用）+ Task 6（权限、集群一致性提示） |
| §4.6 结尾提示 | Task 6（`print_next_steps`） |
| §5 `ClusterPeer.AdminPort` | Task 1 |
| §6.1 入口与数据来源 | Task 3（`runStatus`、管理面关闭即时报错） |
| §6.2 流程（含 leader 跳转与降级） | Task 3（`collectStatus`/`fetchFromLeader`） |
| §6.3 输出 | Task 2（`renderStatus`） |
| §6.4 退出码（含码 3 的优先级） | Task 2（`statusVerdict` + 6 个判级用例） |
| §7 测试策略 | 各 task 内嵌 + Task 7 的 CI + 末尾的三机验收 |
| §8 文档改动 | Task 8 + Task 7（release.yml） |
| §9 已知边界 | Task 8（写进 deployment.md 的凭据一致性与退出码 2 说明） |

无遗漏。

**2. 占位符扫描**：全文无 TBD/TODO/「类似 Task N」/「加上适当的错误处理」。每个代码步骤都给了可直接落盘的完整代码。

**3. 类型与命名一致性**（跨 task 核对过）：
- `statusView` / `adminOverview` / `adminSystem` / `adminDisk` 在 Task 2 定义，Task 3 按同名同字段使用 ✓
- `statusOK/statusUnreachable/statusDegraded/statusIncomplete` 四个常量在 Task 2 定义，Task 3 与 Task 8 文档表格逐一对应 ✓
- `LocalAdminPort` / `PeerAdminAddr` / `PeerByID` 在 Task 1 定义，Task 3 按同名签名调用 ✓
- `cluster.MetaGroup` 是既有常量（`internal/cluster/manager.go:54`），Task 3 引用 ✓
- shell 侧：`OPT_*`（Task 4 定义）→ Task 5/6 使用；`CRED_USER/CRED_PASS/CRED_GENERATED`（Task 5）→ Task 6 的 `print_next_steps` 使用；`PKG_DIR`（Task 6 内部）✓
- `SQ_CFG_DIR`/`SQ_DATA_DIR`/`CFG_DST`/`EXAMPLE_DST` 在 Task 4 定义，Task 5/6 沿用 ✓

**4. 计划期发现并已处理的 spec 缺口**：spec §7 假定存在可挂载 shellcheck 的 CI，实际本仓库没有测试 CI。Task 7 新建了刻意窄的 `ci.yml`，理由已写在 task 与 workflow 文件头里。
