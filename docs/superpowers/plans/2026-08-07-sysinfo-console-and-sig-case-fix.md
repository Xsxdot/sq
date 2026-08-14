# 运行时系统读数上控制台 + 官方 SDK 签名大小写兼容 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把「磁盘拒写状态 / 磁盘用量 / 数据目录占用 / Go 运行时内存」这四类目前只存在于日志里的运行态，同时暴露到 `/metrics` 与 Web 控制台；并修掉 C#/C++ 官方 SDK 因签名十六进制大小写不一致导致的 100% 认证失败。

**Architecture:** 新增 `internal/sysinfo` 包，把原本藏在 `internal/core/retention` 里的包私有磁盘探测提出来成为公共能力，并加一个带 TTL 缓存的 `Reporter` 统一产出运行态快照；`retention`（水位判定）、`metrics`（Prometheus gauge）、`admin`（`GET /admin/system`）三方共用同一个 `Reporter`，保证控制台、`/metrics`、拒写开关看到的是同一份事实。前端在总览页加第二条 `.strip` 显示系统读数，并在全站外壳 `Shell` 顶部加拒写横幅。签名修复是独立的一处三行改动，排在最前面单独提交。

**Tech Stack:** Go 1.22+（`syscall.Statfs` / `filepath.WalkDir` / `runtime.ReadMemStats`）、prometheus/client_golang、React 18 + TypeScript + Vite + Vitest。

---

## 背景：为什么做这两件事

**C（签名大小写）**：核对 `apache/rocketmq-clients` 五个语言实现后确认，官方 SDK 自己就没统一签名的十六进制大小写：

| SDK | `Credential=` 值 | Signature 十六进制 | 源码依据 |
|---|---|---|---|
| Go | `{ak}//Rocketmq` | 小写 | `hex.EncodeToString` |
| Java | `{ak}` | 小写 | `Utilities.encodeHexString(..., false)` → `DIGITS_LOWER` |
| Python | `{ak}` | 小写 | `hexlify` |
| C# | `{ak}` | **大写** | `BitConverter.ToString(digest).Replace("-", "")` |
| C++ | `{ak}/{region}/Rocketmq` | **大写** | `MixAll::hex` 的字典是 `'0'..'9','A'..'F'` |

`Credential=` 的两种格式当前代码已经吃住了（`strings.Cut(v, "/")` 取第一段），但 `verifyAuth` 用 `hex.EncodeToString`（小写）算 expect 再做精确比对，**C#/C++ 客户端一旦开启 `access_key`/`secret_key` 必然 100% 认证失败**，而且报的是不区分原因的「认证失败」，排查代价极高。

**A（系统读数）**：`disk_watermark_percent` 触发后生产端全部写失败，但这个状态目前只在 `retention.checkDisk` 的日志里 —— `/admin/overview` 没有、`/metrics` 也没有。M5 的出口标准是「日常排查够用」，唯一会让写入全挂的状态却排查不出来，属于 M5 的欠账。spec §10 长稳测试要求「盯内存/磁盘曲线」，这个能力现在同样不存在。

---

## 文件结构

**新建**

| 文件 | 职责 |
|---|---|
| `internal/sysinfo/sysinfo.go` | 包文档 + `Disk`/`Snapshot` 两个数据类型 + `dirSize` 目录遍历 |
| `internal/sysinfo/disk_unix.go` | unix 平台磁盘探测（`syscall.Statfs`），由 `retention` 迁移而来并扩展为返回字节数 |
| `internal/sysinfo/disk_other.go` | 非 unix 平台降级实现 |
| `internal/sysinfo/reporter.go` | `Reporter`：持有 dataDir/水位/拒写开关/启动时刻，产出 `Snapshot`，含数据目录大小的 TTL 缓存 |
| `internal/sysinfo/sysinfo_test.go` | `DiskUsage` 与 `dirSize` 的行为测试 |
| `internal/sysinfo/reporter_test.go` | `Snapshot` 字段与 TTL 缓存行为测试 |

**删除**

- `internal/core/retention/disk_unix.go`
- `internal/core/retention/disk_other.go`

**修改**

| 文件 | 改什么 |
|---|---|
| `internal/rpc/auth.go` | 签名比较前统一折成小写；补边界注释 |
| `internal/rpc/auth_test.go` | 表驱动覆盖四种官方 SDK 头形状 |
| `internal/core/retention/retention.go` | `checkDisk` 改用 `sysinfo.DiskUsage` |
| `internal/metrics/collector.go` | `Collector`/`NewCollector`/`NewRegistry` 接受 `*sysinfo.Reporter`，新增四个 gauge |
| `internal/metrics/metrics_test.go` | 更新 `NewRegistry` 调用点 |
| `internal/admin/server.go` | `Server.writeBlocked` 字段换成 `sys *sysinfo.Reporter`；注册 `GET /admin/system` |
| `internal/admin/messages.go` | 两处 `s.writeBlocked.Load()` 换成 `s.blocked()` |
| `internal/admin/system.go`（新建） | `handleSystem` |
| `internal/admin/system_test.go`（新建） | `/admin/system` 端点测试 |
| `internal/admin/server_test.go` | 更新两处 `New(...)` 调用点 |
| `cmd/sq/main.go` | `writeBlocked` 提前创建、装配 `sysinfo.Reporter` 并注入 registry 与 admin |
| `prototypes/base/index.html` | 新增第二条系统 `.strip` |
| `prototypes/base/shared/shell.html` | `main.content` 顶部加拒写横幅样板 |
| `web/src/api/types.ts` | 新增 `DiskUsage`/`SystemInfo` |
| `web/src/api/client.ts` | 新增 `api.system()` |
| `web/src/lib/format.ts` | 新增 `bytes()` / `uptime()` |
| `web/src/lib/format.test.ts`（新建） | `bytes()`/`uptime()` 测试 |
| `web/src/pages/Overview.tsx` | 新增系统读数条 |
| `web/src/components/Shell.tsx` | 全站拒写横幅 |
| `web/src/components/Shell.test.tsx`（新建） | 横幅显隐测试 |
| `README.md` | 兼容性限定说明、`/admin/system` 端点、新增 metrics、控制台系统读数 |

---

## Task 1: 修复官方 SDK 签名十六进制大小写不兼容

**Files:**
- Modify: `internal/rpc/auth.go:79-86`
- Test: `internal/rpc/auth_test.go`

- [ ] **Step 1: 写失败测试**

在 `internal/rpc/auth_test.go` 中，把现有的 `signedCtx` 改造成基于一个更通用的构造器，并新增四种官方 SDK 头形状的表驱动用例。

先在 import 块中加入 `"strings"`，然后把原来的 `signedCtx` 函数整体替换为：

```go
// sdkCtx 按官方 SDK 的头形状构造带签名头的 incoming context。
//
// 参数：
//   - cred: authorization 里 Credential= 后面那一整段。Go/C++ SDK 拼的是
//     "{ak}//Rocketmq"（带 region/service），Java/Python/C# 只拼裸 "{ak}"
//   - upper: 签名十六进制是否大写。Go/Java/Python 输出小写，C#/C++ 输出大写
func sdkCtx(cred, secret, datetime string, upper bool) context.Context {
	h := hmac.New(sha1.New, []byte(secret))
	h.Write([]byte(datetime))
	sig := hex.EncodeToString(h.Sum(nil))
	if upper {
		sig = strings.ToUpper(sig)
	}
	auth := fmt.Sprintf("MQv2-HMAC-SHA1 Credential=%s, SignedHeaders=x-mq-date-time, Signature=%s",
		cred, sig)
	md := metadata.Pairs("x-mq-date-time", datetime, "authorization", auth)
	return metadata.NewIncomingContext(context.Background(), md)
}

// signedCtx 构造 Go SDK 形状的签名头（其余用例继续用它，形状变化只在
// TestAuthAcceptsAllOfficialSDKHeaderShapes 里穷举）。
func signedCtx(ak, secret, datetime string) context.Context {
	return sdkCtx(ak+"//Rocketmq", secret, datetime, false)
}
```

再在文件末尾（`fakeServerStream` 定义之前）追加：

```go
// TestAuthAcceptsAllOfficialSDKHeaderShapes 钉住五个官方 SDK 的头形状差异。
//
// 官方实现自己并不统一：Credential 段 Go/C++ 带 "//Rocketmq"、Java/Python/C#
// 是裸 AK；签名十六进制 Go/Java/Python 小写、C#/C++ 大写。任何一种形状被拒，
// 对应语言的客户端在开启鉴权后就是 100% 连不上。
func TestAuthAcceptsAllOfficialSDKHeaderShapes(t *testing.T) {
	u, _ := NewAuthInterceptors("ak1", "sk1", slog.Default())
	cases := []struct {
		name  string
		cred  string
		upper bool
	}{
		{"Go: ak//Rocketmq + 小写", "ak1//Rocketmq", false},
		{"Java/Python: 裸 ak + 小写", "ak1", false},
		{"C#: 裸 ak + 大写", "ak1", true},
		{"C++: ak//Rocketmq + 大写", "ak1//Rocketmq", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := sdkCtx(c.cred, "sk1", "20260807T120000Z", c.upper)
			if err := callUnary(t, u, ctx); err != nil {
				t.Fatalf("该形状应放行: %v", err)
			}
		})
	}
}

// TestAuthStillRejectsWrongSecretInBothCases 防止「统一折小写」被误实现成
// 「跳过签名校验」：密钥错时无论大小写都必须拒。
func TestAuthStillRejectsWrongSecretInBothCases(t *testing.T) {
	u, _ := NewAuthInterceptors("ak1", "sk1", slog.Default())
	for _, upper := range []bool{false, true} {
		ctx := sdkCtx("ak1", "wrong", "20260807T120000Z", upper)
		if status.Code(callUnary(t, u, ctx)) != codes.Unauthenticated {
			t.Fatalf("密钥错误必须拒绝（upper=%v）", upper)
		}
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestAuthAccepts|TestAuthStillRejects' -v`
Expected: `TestAuthAcceptsAllOfficialSDKHeaderShapes/C#...` 与 `/C++...` 两个子用例 FAIL，报「该形状应放行: rpc error: code = Unauthenticated desc = 认证失败」；其余子用例 PASS。

- [ ] **Step 3: 写最小实现**

在 `internal/rpc/auth.go` 的 `verifyAuth` 中，把签名比较那一行替换为：

```go
	akOK := subtle.ConstantTimeCompare([]byte(ak), []byte(wantAK)) == 1
	// 官方 SDK 五个语言实现的十六进制大小写并不统一：Go/Java/Python 输出小写
	// （hex.EncodeToString / encodeHexString(...,false) / hexlify），C#/C++ 输出
	// 大写（BitConverter.ToString / MixAll::hex 的 'A'-'F' 字典）。服务端统一
	// 折成小写再比 —— 否则 C#/C++ 客户端开启鉴权后 100% 认证失败，且错误信息
	// 刻意不区分原因，几乎无法自助排查。
	// 对客户端送来的串做小写化不泄露密钥的任何信息，常数时间比较的性质不变。
	sigOK := subtle.ConstantTimeCompare([]byte(strings.ToLower(sig)), []byte(expect)) == 1
```

（`strings` 已在 import 中，无需新增。）

- [ ] **Step 4: 更新文件头注释（instrumenting-code：注释）**

把 `internal/rpc/auth.go` 头注释「边界」里的最后一条替换为：

```go
//   - 签名算法与头格式以官方 SDK 为准，且必须容忍各语言实现之间的差异：
//     Credential 段可能是 "{ak}//Rocketmq"（Go/C++）也可能是裸 "{ak}"
//     （Java/Python/C#）；签名十六进制可能小写（Go/Java/Python）也可能大写
//     （C#/C++）。两种差异都在 auth_test.go 的 SDK 形状表里钉住
```

- [ ] **Step 5: 运行全部 rpc 测试确认通过**

Run: `go test ./internal/rpc/ -v -run TestAuth`
Expected: 全部 PASS，包含原有的 `TestAuthUnaryRejects` 六个子用例。

- [ ] **Step 6: 提交**

```bash
git add internal/rpc/auth.go internal/rpc/auth_test.go
git commit -m "fix(rpc): 兼容 C#/C++ 官方 SDK 的大写十六进制签名"
```

---

## Task 2: 抽出 internal/sysinfo 磁盘探测，retention 改用它

**Files:**
- Create: `internal/sysinfo/sysinfo.go`
- Create: `internal/sysinfo/disk_unix.go`
- Create: `internal/sysinfo/disk_other.go`
- Create: `internal/sysinfo/sysinfo_test.go`
- Delete: `internal/core/retention/disk_unix.go`
- Delete: `internal/core/retention/disk_other.go`
- Modify: `internal/core/retention/retention.go:80-99`

- [ ] **Step 1: 写失败测试**

创建 `internal/sysinfo/sysinfo_test.go`：

```go
package sysinfo

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDiskUsageReportsPlausibleNumbers(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("磁盘探测仅在 unix 平台可用")
	}
	d, err := DiskUsage(t.TempDir())
	if err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	if d.TotalBytes == 0 {
		t.Fatal("总容量不该为 0")
	}
	if d.FreeBytes > d.TotalBytes {
		t.Fatalf("可用 %d 不该大于总量 %d", d.FreeBytes, d.TotalBytes)
	}
	if d.UsedPercent < 0 || d.UsedPercent > 100 {
		t.Fatalf("已用百分比越界: %v", d.UsedPercent)
	}
}

func TestDiskUsageOnMissingDirFails(t *testing.T) {
	if _, err := DiskUsage(filepath.Join(t.TempDir(), "no-such-dir")); err == nil {
		t.Fatal("不存在的目录应返回错误")
	}
}

func TestDirSizeSumsRegularFiles(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(p string, n int) {
		if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "a.bin"), 1000)
	write(filepath.Join(sub, "b.bin"), 2000)

	got, err := dirSize(root)
	if err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	if got != 3000 {
		t.Fatalf("目录大小应为 3000，得到 %d", got)
	}
}

func TestDirSizeOnMissingDirFails(t *testing.T) {
	if _, err := dirSize(filepath.Join(t.TempDir(), "no-such-dir")); err == nil {
		t.Fatal("不存在的目录应返回错误")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/sysinfo/`
Expected: FAIL，报 `no required module provides package` 或 `undefined: DiskUsage`（包尚未创建）。

- [ ] **Step 3: 创建 internal/sysinfo/sysinfo.go**

```go
// Package sysinfo 提供进程与宿主机的运行态读数（磁盘用量、数据目录占用、
// Go 运行时内存、协程数、运行时长）。
//
// 职责：
//   - 磁盘探测：数据目录所在文件系统的总量/可用/已用百分比（与 df 同口径）
//   - 数据目录占用统计（递归遍历，带 TTL 缓存，见 reporter.go）
//   - 汇总成一个 Snapshot，供 retention（水位判定）、metrics（Prometheus
//     gauge）、admin（GET /admin/system）三方共用同一份事实
//
// 边界：
//   - 只读不改：本包不做任何拒写决策，拒写开关由 retention 写、本包只读出来报
//   - 不提供进程 RSS：Go 标准库没有可移植的 RSS 读法，强行给一个数会诱导
//     「Go 归还内存有延迟 → RSS 长期高位」被误读成内存泄漏。只给 Go 运行时
//     自己的口径（HeapInuse / Sys），Linux 上的真实 RSS 交给 /metrics 的
//     process 采集器
//   - 磁盘探测仅 unix 可用，其余平台返回错误（与 M2 的既有边界一致）
package sysinfo

import (
	"io/fs"
	"path/filepath"
)

// Disk 数据目录所在文件系统的容量读数。
type Disk struct {
	TotalBytes  uint64  `json:"total_bytes"`
	FreeBytes   uint64  `json:"free_bytes"`
	UsedPercent float64 `json:"used_percent"`
}

// Snapshot 一次采集得到的全部运行态读数。
//
// 指针字段表示「不知道」而不是「是 0」：磁盘探测失败与「磁盘是空的」是两件事，
// 前端据此显示占位符而不是一个误导性的 0（与 admin overview 里 qps 的处理一致）。
type Snapshot struct {
	// Disk 为 nil 表示探测失败或平台不支持
	Disk *Disk `json:"disk"`
	// WatermarkPercent 拒写水位线，0 表示水位保护关闭
	WatermarkPercent int `json:"watermark_percent"`
	// WriteBlocked 当前是否处于拒写保读状态
	WriteBlocked bool `json:"write_blocked"`
	// DataDirBytes 为 nil 表示尚未成功采到过
	DataDirBytes *int64 `json:"data_dir_bytes"`
	// GoHeapInuseBytes / GoSysBytes 是 Go 运行时口径，不是进程 RSS
	GoHeapInuseBytes uint64 `json:"go_heap_inuse_bytes"`
	GoSysBytes       uint64 `json:"go_sys_bytes"`
	Goroutines       int    `json:"goroutines"`
	UptimeSeconds    int64  `json:"uptime_seconds"`
}

// dirSize 递归累加 dir 下所有常规文件的大小，返回字节数。
//
// 注意：
//   - 单个条目读失败会被跳过而不是让整趟失败 —— retention 清理与 Pebble 压实
//     随时可能删掉刚枚举到的文件，那是常态不是故障；数据目录大小本就是个近似值
//   - dir 本身不存在时仍然返回错误（那是配置错误，必须暴露）
func dirSize(dir string) (int64, error) {
	if _, err := filepath.Abs(dir); err != nil {
		return 0, err
	}
	var total int64
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// 根目录本身出错要往上抛，子条目出错跳过
			if p == dir {
				return err
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}
```

- [ ] **Step 4: 创建 internal/sysinfo/disk_unix.go**

```go
//go:build unix

// 磁盘用量探测（unix：syscall.Statfs，darwin/linux 字段一致）。
package sysinfo

import "syscall"

// DiskUsage 返回 dir 所在文件系统的容量读数。
//
// 参数：
//   - dir: 探测目标目录（用它所在的文件系统，不是这个目录本身的大小）
//
// 返回：
//   - Disk: 总量/可用/已用百分比
//   - error: dir 不存在或 statfs 失败
//
// 注意：
//   - 可用量用 Bavail（非特权可用块）计算，与 df 口径一致；用 Bfree 会把
//     只有 root 能动的保留块算成可用，导致水位永远差几个百分点触发不了
func DiskUsage(dir string) (Disk, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return Disk{}, err
	}
	if fs.Blocks == 0 {
		return Disk{}, nil
	}
	bsize := uint64(fs.Bsize)
	freePct := float64(fs.Bavail) * 100 / float64(fs.Blocks)
	return Disk{
		TotalBytes:  fs.Blocks * bsize,
		FreeBytes:   uint64(fs.Bavail) * bsize,
		UsedPercent: 100 - freePct,
	}, nil
}
```

- [ ] **Step 5: 创建 internal/sysinfo/disk_other.go**

```go
//go:build !unix

// 非 unix 平台的磁盘探测降级（M2 起的既有边界：水位保护仅支持 unix）。
package sysinfo

import "errors"

// DiskUsage 在非 unix 平台恒返回错误，调用方据此降级为「不知道」。
func DiskUsage(dir string) (Disk, error) {
	return Disk{}, errors.New("当前平台不支持磁盘用量探测")
}
```

- [ ] **Step 6: 运行测试确认通过**

Run: `go test ./internal/sysinfo/ -v`
Expected: 四个用例全部 PASS。

- [ ] **Step 7: 删除 retention 的私有实现并改用 sysinfo**

```bash
git rm internal/core/retention/disk_unix.go internal/core/retention/disk_other.go
```

在 `internal/core/retention/retention.go` 的 import 块中加入 `"github.com/xushixin/sq/internal/sysinfo"`，并把 `checkDisk` 整体替换为：

```go
// checkDisk 探测磁盘用量并更新拒写开关。只在状态翻转时打日志（避免每趟刷屏）。
//
// 探测实现在 internal/sysinfo：控制台的 /admin/system 与 /metrics 的
// sq_disk_used_percent 走的是同一个函数，三方看到的百分比必须是同一个数 ——
// 「日志说拒写了但控制台显示 60%」是最坏的排查体验。
func (m *Manager) checkDisk() {
	if m.watermarkPct <= 0 || m.writeBlocked == nil {
		return
	}
	d, err := sysinfo.DiskUsage(m.dataDir)
	if err != nil {
		m.logger.Warn("磁盘水位检查失败，本趟跳过", "dir", m.dataDir, "err", err)
		return
	}
	blocked := d.UsedPercent >= float64(m.watermarkPct)
	if blocked != m.writeBlocked.Load() {
		if blocked {
			m.logger.Error("磁盘使用率超过水位线，进入拒写保读",
				"used_pct", d.UsedPercent, "watermark", m.watermarkPct,
				"free_bytes", d.FreeBytes, "total_bytes", d.TotalBytes)
		} else {
			m.logger.Info("磁盘使用率回落，恢复写入",
				"used_pct", d.UsedPercent, "watermark", m.watermarkPct,
				"free_bytes", d.FreeBytes)
		}
		m.writeBlocked.Store(blocked)
	}
}
```

- [ ] **Step 8: 运行全量测试确认没打断既有行为**

Run: `go test ./...`
Expected: 全部 PASS（`internal/core/retention` 的既有用例不依赖被删的私有函数）。

- [ ] **Step 9: 提交**

```bash
git add internal/sysinfo cmd internal/core/retention
git commit -m "refactor(sysinfo): 抽出公共磁盘探测包，retention 改用并补日志上下文"
```

---

## Task 3: sysinfo.Reporter —— 带 TTL 缓存的运行态快照

**Files:**
- Create: `internal/sysinfo/reporter.go`
- Test: `internal/sysinfo/reporter_test.go`

- [ ] **Step 1: 写失败测试**

创建 `internal/sysinfo/reporter_test.go`：

```go
package sysinfo

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func newTestReporter(t *testing.T, dir string, watermark int, blocked *atomic.Bool) *Reporter {
	t.Helper()
	return New(dir, watermark, blocked, slog.Default())
}

func TestSnapshotReportsWriteBlockedAndWatermark(t *testing.T) {
	blocked := &atomic.Bool{}
	r := newTestReporter(t, t.TempDir(), 85, blocked)

	if s := r.Snapshot(); s.WriteBlocked {
		t.Fatal("初始不该处于拒写状态")
	}
	blocked.Store(true)
	s := r.Snapshot()
	if !s.WriteBlocked {
		t.Fatal("拒写开关置位后 Snapshot 应反映出来")
	}
	if s.WatermarkPercent != 85 {
		t.Fatalf("水位线应为 85，得到 %d", s.WatermarkPercent)
	}
}

func TestSnapshotReportsRuntimeReadings(t *testing.T) {
	r := newTestReporter(t, t.TempDir(), 0, &atomic.Bool{})
	s := r.Snapshot()
	if s.GoHeapInuseBytes == 0 {
		t.Fatal("堆内存不该为 0")
	}
	if s.GoSysBytes < s.GoHeapInuseBytes {
		t.Fatalf("向 OS 申请量 %d 不该小于堆占用 %d", s.GoSysBytes, s.GoHeapInuseBytes)
	}
	if s.Goroutines < 1 {
		t.Fatalf("协程数至少为 1，得到 %d", s.Goroutines)
	}
	if s.UptimeSeconds < 0 {
		t.Fatalf("运行时长不该为负: %d", s.UptimeSeconds)
	}
}

func TestSnapshotDataDirBytesIsCached(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.bin"), make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	r := newTestReporter(t, dir, 0, &atomic.Bool{})

	first := r.Snapshot().DataDirBytes
	if first == nil || *first != 1000 {
		t.Fatalf("首次统计应为 1000，得到 %v", first)
	}
	// TTL 内新增文件不该被立刻看到 —— 这正是缓存存在的证据
	if err := os.WriteFile(filepath.Join(dir, "b.bin"), make([]byte, 2000), 0o644); err != nil {
		t.Fatal(err)
	}
	second := r.Snapshot().DataDirBytes
	if second == nil || *second != 1000 {
		t.Fatalf("TTL 内应返回缓存值 1000，得到 %v", second)
	}
	// 把缓存时刻推回过去，模拟 TTL 过期
	r.mu.Lock()
	r.dirAt = time.Now().Add(-2 * dirSizeTTL)
	r.mu.Unlock()

	third := r.Snapshot().DataDirBytes
	if third == nil || *third != 3000 {
		t.Fatalf("TTL 过期后应重新统计得到 3000，得到 %v", third)
	}
}

func TestSnapshotOnBadDirDegradesGracefully(t *testing.T) {
	r := newTestReporter(t, filepath.Join(t.TempDir(), "no-such-dir"), 85, &atomic.Bool{})
	s := r.Snapshot()
	if s.Disk != nil {
		t.Fatal("目录不存在时 Disk 应为 nil（不知道），而不是一组 0")
	}
	if s.DataDirBytes != nil {
		t.Fatal("目录不存在时 DataDirBytes 应为 nil")
	}
	// 但运行时读数仍然要有：磁盘探不到不该让整个端点变哑
	if s.GoHeapInuseBytes == 0 {
		t.Fatal("磁盘失败不应影响运行时读数")
	}
}

func TestWriteBlockedTolerantOfNilSwitch(t *testing.T) {
	r := New(t.TempDir(), 0, nil, slog.Default())
	if r.WriteBlocked() {
		t.Fatal("拒写开关为 nil 时应视为未拒写")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/sysinfo/ -run 'TestSnapshot|TestWriteBlocked'`
Expected: FAIL，报 `undefined: Reporter` / `undefined: New`。

- [ ] **Step 3: 写实现**

创建 `internal/sysinfo/reporter.go`：

```go
// reporter.go: 运行态快照的产出者，含数据目录大小的 TTL 缓存。
//
// 职责：
//   - 持有采集所需的全部依赖（数据目录、水位线、拒写开关、进程启动时刻）
//   - Snapshot 一次性产出磁盘 / 数据目录 / Go 运行时 / 协程 / 运行时长
//
// 边界：
//   - 不起后台 goroutine：调用方（metrics 抓取、admin 请求）触发采集，
//     昂贵的目录遍历靠 TTL 缓存兜住，不需要额外的生命周期管理
//   - 不做任何判定：是否拒写由 retention 决定并写进 writeBlocked，这里只读
package sysinfo

import (
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// dirSizeTTL 数据目录大小的缓存有效期。
//
// 为什么要缓存：目录大小只能靠递归遍历得到，而控制台每 5 秒轮询一次、
// Prometheus 也在按自己的间隔抓取；每次都遍历一遍，等于把一个诊断读数
// 变成一份持续的 I/O 负担。60 秒意味着这个数最多滞后一分钟 —— 对
// 「数据目录涨到多大了」这类判断完全够用。
const dirSizeTTL = 60 * time.Second

// Reporter 运行态读数的产出者。并发安全。
type Reporter struct {
	dataDir      string
	watermarkPct int
	writeBlocked *atomic.Bool
	startedAt    time.Time
	logger       *slog.Logger

	mu       sync.Mutex
	dirBytes int64
	dirAt    time.Time
	dirOK    bool
}

// New 构造 Reporter。
//
// 参数：
//   - dataDir: 数据目录，既是磁盘探测目标也是占用统计目标
//   - watermarkPct: 拒写水位线，0 表示水位保护关闭（原样报给调用方）
//   - writeBlocked: retention 维护的拒写开关；为 nil 时一律视为未拒写
//
// 注意：
//   - startedAt 取构造时刻。本对象在 main 装配早期创建，与进程启动只差毫秒级
func New(dataDir string, watermarkPct int, writeBlocked *atomic.Bool, logger *slog.Logger) *Reporter {
	r := &Reporter{
		dataDir:      dataDir,
		watermarkPct: watermarkPct,
		writeBlocked: writeBlocked,
		startedAt:    time.Now(),
		logger:       logger.With("mod", "sysinfo"),
	}
	r.logger.Info("sysinfo 采集器就绪",
		"data_dir", dataDir, "watermark_pct", watermarkPct, "dir_size_ttl", dirSizeTTL.String())
	return r
}

// WriteBlocked 返回当前是否处于拒写保读状态。开关未装配时返回 false。
func (r *Reporter) WriteBlocked() bool {
	return r.writeBlocked != nil && r.writeBlocked.Load()
}

// Snapshot 采集一次运行态读数。
//
// 返回：
//   - Snapshot: 磁盘/目录占用采不到时对应字段为 nil（表示「不知道」），
//     运行时读数恒可用 —— 磁盘探测失败不该让整个端点变哑
//
// 注意：
//   - 会调用 runtime.ReadMemStats，有一次极短的 STW。控制台 5 秒一次、
//     Prometheus 按抓取间隔一次，这个频率下开销可忽略
func (r *Reporter) Snapshot() Snapshot {
	s := Snapshot{
		WatermarkPercent: r.watermarkPct,
		WriteBlocked:     r.WriteBlocked(),
		Goroutines:       runtime.NumGoroutine(),
		UptimeSeconds:    int64(time.Since(r.startedAt).Seconds()),
		DataDirBytes:     r.dataDirBytes(),
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.GoHeapInuseBytes = ms.HeapInuse
	s.GoSysBytes = ms.Sys

	if d, err := DiskUsage(r.dataDir); err != nil {
		// Warn 而非 Error：非 unix 平台本就探不到，这是已声明的边界不是故障
		r.logger.Warn("磁盘用量探测失败，本次快照缺磁盘读数", "dir", r.dataDir, "err", err)
	} else {
		s.Disk = &d
	}
	return s
}

// dataDirBytes 返回数据目录占用，带 TTL 缓存；从未成功采到过时返回 nil。
func (r *Reporter) dataDirBytes() *int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dirOK && time.Since(r.dirAt) < dirSizeTTL {
		v := r.dirBytes
		return &v
	}
	n, err := dirSize(r.dataDir)
	if err != nil {
		r.logger.Warn("数据目录占用统计失败", "dir", r.dataDir, "err", err, "has_stale", r.dirOK)
		if r.dirOK {
			// 保留上一次的值：一个滞后的数远比「不知道」有用，
			// 前端也不会因为一次瞬时失败而闪成占位符
			v := r.dirBytes
			return &v
		}
		return nil
	}
	if !r.dirOK || n != r.dirBytes {
		r.logger.Debug("数据目录占用刷新", "dir", r.dataDir, "bytes", n, "prev_bytes", r.dirBytes)
	}
	r.dirBytes, r.dirAt, r.dirOK = n, time.Now(), true
	return &n
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./internal/sysinfo/ -v`
Expected: 全部 PASS（共 9 个用例）。

- [ ] **Step 5: 提交**

```bash
git add internal/sysinfo
git commit -m "feat(sysinfo): 新增 Reporter 运行态快照与数据目录 TTL 缓存"
```

---

## Task 4: /metrics 暴露磁盘与拒写状态

**Files:**
- Modify: `internal/metrics/collector.go`
- Modify: `internal/metrics/metrics_test.go:77`
- Test: `internal/metrics/metrics_test.go`

- [ ] **Step 1: 写失败测试**

在 `internal/metrics/metrics_test.go` 末尾追加（`newStore`/`newMeta` 之类的既有测试辅助按该文件现状复用；若现有用例是通过 `NewRegistry(st, mt, slog.Default())` 拿 registry，这里照同样方式取 st/mt）：

```go
// TestRegistryExposesDiskAndWriteBlocked 钉住磁盘与拒写状态进入 /metrics。
// 这是 M5 的欠账：拒写会让生产端全挂，却没有任何指标可供告警。
func TestRegistryExposesDiskAndWriteBlocked(t *testing.T) {
	st, mt := newTestStoreMeta(t) // 与该文件既有用例相同的构造方式
	blocked := &atomic.Bool{}
	sys := sysinfo.New(t.TempDir(), 85, blocked, slog.Default())
	reg := NewRegistry(st, mt, sys, slog.Default())

	names := gatherNames(t, reg)
	for _, want := range []string{"sq_write_blocked", "sq_data_dir_bytes"} {
		if !names[want] {
			t.Fatalf("/metrics 应包含 %s，实际有 %v", want, names)
		}
	}
	if v := gatherValue(t, reg, "sq_write_blocked"); v != 0 {
		t.Fatalf("未拒写时 sq_write_blocked 应为 0，得到 %v", v)
	}
	blocked.Store(true)
	if v := gatherValue(t, reg, "sq_write_blocked"); v != 1 {
		t.Fatalf("拒写时 sq_write_blocked 应为 1，得到 %v", v)
	}
}

// gatherNames 收集 registry 当前产出的全部指标名。
func gatherNames(t *testing.T, reg *prometheus.Registry) map[string]bool {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather 失败: %v", err)
	}
	out := map[string]bool{}
	for _, mf := range mfs {
		out[mf.GetName()] = true
	}
	return out
}

// gatherValue 取一个无标签 gauge 的当前值。
func gatherValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather 失败: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("未找到指标 %s", name)
	return 0
}
```

import 块补上 `"sync/atomic"`、`"github.com/prometheus/client_golang/prometheus"`、`"github.com/xushixin/sq/internal/sysinfo"`。

同时把该文件第 77 行的既有调用改为：

```go
	reg := NewRegistry(st, mt, sysinfo.New(t.TempDir(), 0, &atomic.Bool{}, slog.Default()), slog.Default())
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/metrics/`
Expected: FAIL，报 `too many arguments in call to NewRegistry`。

- [ ] **Step 3: 写实现**

在 `internal/metrics/collector.go` 的 `var (...)` 描述块末尾追加：

```go
	descDiskUsed = prometheus.NewDesc("sq_disk_used_percent",
		"数据目录所在文件系统已用百分比（与 df 同口径）", nil, nil)
	descDiskFree = prometheus.NewDesc("sq_disk_free_bytes",
		"数据目录所在文件系统的非特权可用字节数", nil, nil)
	descDataDir = prometheus.NewDesc("sq_data_dir_bytes",
		"数据目录占用字节数（TTL 缓存，最多滞后 60s）", nil, nil)
	descBlocked = prometheus.NewDesc("sq_write_blocked",
		"磁盘水位拒写开关：1=拒写保读中，0=正常写入", nil, nil)
```

把 `Collector` 结构体、构造器、`Describe`、`Collect` 分别改为：

```go
// Collector 抓取期状态采集器。
type Collector struct {
	st     *store.Store
	mt     *meta.Meta
	sys    *sysinfo.Reporter
	logger *slog.Logger
}

// NewCollector 构造状态 Collector。sys 为 nil 时不产出系统类指标
// （测试可只关心业务指标；main 装配时恒非 nil）。
func NewCollector(st *store.Store, mt *meta.Meta, sys *sysinfo.Reporter, logger *slog.Logger) *Collector {
	return &Collector{st: st, mt: mt, sys: sys, logger: logger.With("mod", "metrics")}
}

// Describe 实现 prometheus.Collector。
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descTopics
	ch <- descGroups
	ch <- descDelay
	ch <- descWrite
	ch <- descPending
	ch <- descInflight
	ch <- descDiskUsed
	ch <- descDiskFree
	ch <- descDataDir
	ch <- descBlocked
}
```

在 `Collect` 方法体最后（`for gt, n := range s.Inflight {...}` 之后）追加：

```go
	c.collectSystem(ch)
}

// collectSystem 产出系统类指标。
//
// 注意：
//   - 这里刻意不出内存指标：GoCollector 已经提供 go_memstats_*，再暴露一份
//     只会让告警规则出现两个口径不同的来源。控制台的 /admin/system 之所以
//     仍给内存，是为了「不接 Prometheus 也能看一眼」这条产品承诺
//   - sq_write_blocked 无论探测成功与否都要出：一个会消失的 gauge 对告警
//     而言比恒为 0 的更危险（absent 与 false 混在一起）
func (c *Collector) collectSystem(ch chan<- prometheus.Metric) {
	if c.sys == nil {
		return
	}
	snap := c.sys.Snapshot()
	blocked := 0.0
	if snap.WriteBlocked {
		blocked = 1
	}
	ch <- prometheus.MustNewConstMetric(descBlocked, prometheus.GaugeValue, blocked)
	if snap.Disk != nil {
		ch <- prometheus.MustNewConstMetric(descDiskUsed, prometheus.GaugeValue, snap.Disk.UsedPercent)
		ch <- prometheus.MustNewConstMetric(descDiskFree, prometheus.GaugeValue, float64(snap.Disk.FreeBytes))
	}
	if snap.DataDirBytes != nil {
		ch <- prometheus.MustNewConstMetric(descDataDir, prometheus.GaugeValue, float64(*snap.DataDirBytes))
	}
}
```

把 `NewRegistry` 的签名与内部 `NewCollector` 调用改为：

```go
// NewRegistry 组装进程级指标注册表并挂接 store.Apply 耗时直方图。
func NewRegistry(st *store.Store, mt *meta.Meta, sys *sysinfo.Reporter, logger *slog.Logger) *prometheus.Registry {
```

内部原来的 `reg.MustRegister(NewCollector(st, mt, logger))` 改为 `reg.MustRegister(NewCollector(st, mt, sys, logger))`。

import 块加入 `"github.com/xushixin/sq/internal/sysinfo"`。

- [ ] **Step 4: 更新文件头注释（instrumenting-code：注释）**

把 `internal/metrics/collector.go` 头注释的「职责」补一条、「边界」补一条：

```go
// 职责：
//   - 抓取时调用 Collect，翻译为带标签的 gauge/counter
//   - 产出系统类指标（磁盘用量、数据目录占用、拒写开关），与控制台
//     /admin/system 共用同一个 sysinfo.Reporter
//   - NewRegistry 一站式装配：Go/process 采集器、状态 Collector、
//     store.Apply 耗时直方图（挂接包级钩子）
//
// 边界：
//   - NewRegistry 写 store.OnApplyObserve 包级钩子，进程内只可调用一次
//     （装配期契约，见 store 侧注释）
//   - 不出内存指标：GoCollector 已提供 go_memstats_*，重复暴露会让告警
//     规则出现两个口径不同的来源
```

- [ ] **Step 5: 运行测试确认通过**

Run: `go test ./internal/metrics/ -v`
Expected: 全部 PASS，含新增的 `TestRegistryExposesDiskAndWriteBlocked`。

- [ ] **Step 6: 提交**

```bash
git add internal/metrics
git commit -m "feat(metrics): /metrics 暴露磁盘用量、数据目录占用与拒写开关"
```

---

## Task 5: GET /admin/system 端点与 main 装配

**Files:**
- Create: `internal/admin/system.go`
- Create: `internal/admin/system_test.go`
- Modify: `internal/admin/server.go:33-62`（结构体与构造器）、`internal/admin/server.go:64-88`（路由）
- Modify: `internal/admin/messages.go:104`、`internal/admin/messages.go:165`
- Modify: `internal/admin/server_test.go:38,57`
- Modify: `cmd/sq/main.go:78-126`

- [ ] **Step 1: 写失败测试**

创建 `internal/admin/system_test.go`：

```go
// system_test.go: /admin/system 端点测试。
package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSystemEndpointReportsSnapshot(t *testing.T) {
	s := newTestServer(t, "", "") // 免登录，与该文件既有辅助一致
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/system", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("应返回 200，得到 %d：%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Disk *struct {
			TotalBytes  uint64  `json:"total_bytes"`
			UsedPercent float64 `json:"used_percent"`
		} `json:"disk"`
		WatermarkPercent int    `json:"watermark_percent"`
		WriteBlocked     bool   `json:"write_blocked"`
		DataDirBytes     *int64 `json:"data_dir_bytes"`
		GoHeapInuseBytes uint64 `json:"go_heap_inuse_bytes"`
		Goroutines       int    `json:"goroutines"`
		UptimeSeconds    int64  `json:"uptime_seconds"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}
	if got.GoHeapInuseBytes == 0 {
		t.Fatal("堆内存不该为 0")
	}
	if got.Goroutines < 1 {
		t.Fatalf("协程数至少为 1，得到 %d", got.Goroutines)
	}
	if got.WriteBlocked {
		t.Fatal("测试环境不该处于拒写状态")
	}
}

func TestSystemEndpointRequiresLogin(t *testing.T) {
	s := newTestServer(t, "root", "pw")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/system", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("未带 token 应返回 401，得到 %d", rec.Code)
	}
}
```

若 `internal/admin/server_test.go` 中的辅助函数名不是 `newTestServer(t, user, pass)`，按该文件实际的辅助名调整这两处调用；辅助函数必须传入一个非 nil 的 `*sysinfo.Reporter`。

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/admin/ -run TestSystemEndpoint`
Expected: FAIL，`/admin/system` 命中兜底的 console handler，返回 200 但 body 不是 JSON（`json.Unmarshal` 报错），或返回 404。

- [ ] **Step 3: 改造 admin.Server 持有 sysinfo.Reporter**

在 `internal/admin/server.go`：

import 块中去掉 `"sync/atomic"`，加入 `"github.com/xushixin/sq/internal/sysinfo"`。

结构体字段 `writeBlocked *atomic.Bool` 替换为：

```go
	// sys 运行态读数来源，同时是拒写开关的唯一读取入口。为 nil 时
	// /admin/system 返回 503，拒写判定一律视为未拒写（测试构造用）
	sys *sysinfo.Reporter
```

构造器签名与赋值改为：

```go
// New 构造 Admin 服务并装配全部路由。username/password 均空 = 免登录。
// sp 为 nil 时时序/总账端点返回 503（采样器未启用，不返回误导性的空数据）；
// sys 为 nil 时 /admin/system 同理返回 503。
func New(st *store.Store, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer,
	username, password string, sys *sysinfo.Reporter, sp *metrics.Sampler,
	reg *prometheus.Registry, logger *slog.Logger) *Server {
	s := &Server{
		st: st, mt: mt, pr: pr, dl: dl,
		username: username, password: password, sys: sys, sp: sp,
		logger: logger.With("mod", "admin"),
		mux:    http.NewServeMux(),
	}
	s.routes(reg)
	return s
}
```

在 `routes` 里 `GET /admin/overview` 那一行后面追加：

```go
	s.mux.HandleFunc("GET /admin/system", s.protected(s.handleSystem))
```

- [ ] **Step 4: 创建 internal/admin/system.go**

```go
// system.go: GET /admin/system —— 进程与宿主机运行态读数。
//
// 职责：
//   - 把 sysinfo.Reporter 的快照原样吐成 JSON，供控制台的系统读数条与
//     全站拒写横幅消费
//   - 提供 blocked()：admin 内部判定「当前是否拒写」的唯一入口
//
// 边界：
//   - 不做任何加工与阈值判断：显示成什么颜色、什么时候报警是前端的事
//   - 不返回数据目录的绝对路径：端点虽然受登录保护，但路径对排查没有增量
//     价值，少暴露一项部署信息
package admin

import "net/http"

// handleSystem GET /admin/system：磁盘用量、数据目录占用、Go 运行时内存、
// 协程数、运行时长与拒写状态。
//
// 注意：
//   - sys 未装配时返回 503 而不是一组 0 —— 与 /admin/timeseries 在采样器
//     缺席时的处理一致：不返回误导性的空数据
func (s *Server) handleSystem(w http.ResponseWriter, r *http.Request) {
	if s.sys == nil {
		s.httpError(w, http.StatusServiceUnavailable, "系统读数采集器未启用")
		return
	}
	snap := s.sys.Snapshot()
	// 拒写是会让生产端全挂的状态，每次被查询都记一笔，便于事后对齐
	// 「控制台什么时候看到的」与「retention 什么时候翻转的」
	if snap.WriteBlocked {
		s.logger.Warn("系统读数查询：当前处于拒写保读状态",
			"watermark_pct", snap.WatermarkPercent, "remote", r.RemoteAddr)
	}
	s.writeJSON(w, http.StatusOK, snap)
}

// blocked 返回当前是否处于拒写保读状态。sys 未装配时视为未拒写。
func (s *Server) blocked() bool { return s.sys != nil && s.sys.WriteBlocked() }
```

- [ ] **Step 5: 把 messages.go 的两处拒写判定切到 blocked()**

`internal/admin/messages.go:104` 与 `:165`，两处形如：

```go
	if s.writeBlocked != nil && s.writeBlocked.Load() {
```

改为：

```go
	if s.blocked() {
```

如果 `internal/admin/messages.go` 因此不再使用 `sync/atomic`，从其 import 中删除。

- [ ] **Step 6: 更新既有 admin 测试的构造点**

`internal/admin/server_test.go:38` 与 `:57`，把 `&atomic.Bool{}` 换成一个 `*sysinfo.Reporter`，并补上 `NewRegistry` 的新参数：

```go
	sys := sysinfo.New(t.TempDir(), 0, &atomic.Bool{}, slog.Default())
	s := New(st, mt, pr, dl, user, pass, sys, sp, metrics.NewRegistry(st, mt, sys, slog.Default()), slog.Default())
```

第 57 行同理（该处 `sp` 传 nil，保持不变）。import 加入 `"github.com/xushixin/sq/internal/sysinfo"`，`sync/atomic` 保留。

- [ ] **Step 7: 更新 main 装配顺序**

在 `cmd/sq/main.go`：

把原本位于 retention 段落里的 `writeBlocked := &atomic.Bool{}` **上移**到 metrics registry 段落之前，并在其后创建 `sysinfo.Reporter`。具体地，把 `// metrics registry 必须先于任何后台 goroutine 装配` 这段注释之前插入：

```go
	// writeBlocked 由 retention 每趟探测磁盘后更新，rpc.SendMessage 据此拒写。
	// 必须先于 metrics registry 创建：registry 里的系统 Collector 要拿着
	// sysinfo.Reporter，而 Reporter 持有的正是这个开关的指针。
	writeBlocked := &atomic.Bool{}
	// sysinfo 采集器：retention 的水位判定、/metrics 的 sq_disk_* 与控制台的
	// /admin/system 三方共用它，保证看到的是同一份磁盘事实。
	sys := sysinfo.New(cfg.DataDir, cfg.DiskWatermarkPercent, writeBlocked, logger)
```

把 `reg = metrics.NewRegistry(st, mt, logger)` 改为：

```go
		reg = metrics.NewRegistry(st, mt, sys, logger)
```

删除 retention 段落里原来的 `writeBlocked := &atomic.Bool{}` 那一行以及它上面那句已经上移的注释，保留：

```go
	rm := retention.New(st, mt, cfg.RetentionInterval(), cfg.DataDir, cfg.DiskWatermarkPercent, writeBlocked, logger)
```

把 admin 构造改为：

```go
		adm := admin.New(st, mt, pr, dl, cfg.AdminUsername, cfg.AdminPassword, sys, sp, reg, logger)
```

import 加入 `"github.com/xushixin/sq/internal/sysinfo"`。

- [ ] **Step 8: 运行全量测试确认通过**

Run: `go test ./... && go build ./...`
Expected: 全部 PASS 且编译通过。

- [ ] **Step 9: 提交**

```bash
git add internal/admin cmd/sq/main.go
git commit -m "feat(admin): 新增 GET /admin/system 运行态读数端点"
```

---

## Task 6: 更新原型基准（形态确认 + 维持既有不变量）

**Files:**
- Modify: `prototypes/base/index.html:44-67`
- Modify: `prototypes/base/shared/shell.html:53-55`

> `web/src/pages/Overview.tsx` 与 `web/src/components/Shell.tsx` 的头注释都写着「布局与 class 名与 prototypes/base/… 逐段一致」。原型必须先改，React 才有对照基准 —— 这不是可选项。本 Task 同时兼任「新形态在写码前先看一眼」。

- [ ] **Step 1: 在 index.html 的业务 strip 之后插入系统 strip**

在 `prototypes/base/index.html` 第 67 行 `</div>`（业务 `.strip` 的结束标签）之后、`<section class="panel">` 之前插入：

```html
      <!-- 系统读数条：业务信号之下的第二梯队。四项都是「排查时才看、但看不到就抓瞎」的数 -->
      <div class="strip">
        <div class="stat">
          <div><div class="stat-label">磁盘使用</div><div class="stat-val" id="v-disk"></div></div>
        </div>
        <div class="stat">
          <div><div class="stat-label">数据目录</div><div class="stat-val" id="v-datadir"></div></div>
        </div>
        <div class="stat">
          <div><div class="stat-label">GO 运行时内存</div><div class="stat-val" id="v-mem"></div></div>
        </div>
        <div class="stat">
          <div><div class="stat-label">运行时长 / 协程</div><div class="stat-val" id="v-proc"></div></div>
        </div>
      </div>
```

- [ ] **Step 2: 在 index.html 的 mock 脚本里填上这四个读数**

在 `prototypes/base/index.html` 底部 `<script>` 里，与其它 `v-xxx` 赋值并列处追加（沿用该文件既有的取元素方式）：

```javascript
      // 系统读数的样例值。磁盘故意取一个逼近水位但未触发的数：
      // 原型要能同时看清「正常态长什么样」和「离触发有多近」
      document.getElementById('v-disk').innerHTML = '71.4<small>% / 水位 85%</small>';
      document.getElementById('v-datadir').innerHTML = '2.4 GB<small>可用 128.6 GB</small>';
      document.getElementById('v-mem').innerHTML = '86.2 MB<small>/ 申请 142.0 MB</small>';
      document.getElementById('v-proc').innerHTML = '3d7h<small>/ 42</small>';
```

- [ ] **Step 3: 在 shell.html 里加拒写横幅样板**

在 `prototypes/base/shared/shell.html` 的 `<main class="content">` 内、`<!-- 页面主体内容放这里 -->` 之前插入：

```html
      <!-- 磁盘拒写横幅：仅当服务端 write_blocked=true 时渲染，其余时候整段不存在。
           放在外壳而不是某个页面里，是因为「发不进去」可能在任何页面被察觉 —— 
           尤其是发送测试消息页。复用 .notice.bad，不需要新 CSS -->
      <div class="notice bad">
        <span><b>磁盘水位已触发，服务当前拒写保读。</b>已用 86.3%，水位线 85%。生产端会收到写入失败，消费不受影响；清理磁盘或调高 disk_watermark_percent 后自动恢复。</span>
      </div>
```

- [ ] **Step 4: 肉眼确认原型形态**

Run: `open prototypes/base/index.html`
Expected: 总览页出现第二条读数带，四项对齐、`small` 副文本不换行；`shell.html` 用浏览器打开可见红色横幅样式正常。若两条 `.strip` 之间间距过挤，**不要改 `app.css`** —— 先记下来，等真实页面渲染出来再一并判断（原型与真实页共用同一份 CSS，改一处会同时影响两边）。

- [ ] **Step 5: 提交**

```bash
git add prototypes/base
git commit -m "chore(prototypes): 基准站补系统读数条与拒写横幅"
```

---

## Task 7: 前端类型、客户端与格式化函数

**Files:**
- Modify: `web/src/api/types.ts`
- Modify: `web/src/api/client.ts:126-133`
- Modify: `web/src/lib/format.ts`
- Create: `web/src/lib/format.test.ts`

- [ ] **Step 1: 写失败测试**

创建 `web/src/lib/format.test.ts`：

```typescript
import { describe, it, expect } from 'vitest'
import { bytes, uptime } from './format'

describe('bytes', () => {
  it('小于 1KB 时按字节显示', () => {
    expect(bytes(0)).toBe('0 B')
    expect(bytes(999)).toBe('999 B')
  })
  it('逐级进位到合适的单位', () => {
    expect(bytes(1024)).toBe('1.0 KB')
    expect(bytes(1536)).toBe('1.5 KB')
    expect(bytes(1024 * 1024 * 2.5)).toBe('2.5 MB')
    expect(bytes(1024 ** 3 * 3)).toBe('3.0 GB')
  })
  it('三位数以上不再保留小数：诊断读数看量级不看精度', () => {
    expect(bytes(1024 * 128)).toBe('128 KB')
  })
})

describe('uptime', () => {
  it('分钟以下说秒', () => {
    expect(uptime(42)).toBe('42s')
  })
  it('一小时以下说分', () => {
    expect(uptime(600)).toBe('10m')
  })
  it('一天以下说时分', () => {
    expect(uptime(3600 * 5 + 60 * 7)).toBe('5h7m')
  })
  it('一天以上说天时', () => {
    expect(uptime(86400 * 3 + 3600 * 7)).toBe('3d7h')
  })
})
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd web && npx vitest run src/lib/format.test.ts`
Expected: FAIL，报 `bytes is not exported` / `uptime is not exported`。

- [ ] **Step 3: 在 format.ts 追加两个格式化函数**

在 `web/src/lib/format.ts` 末尾追加：

```typescript
/**
 * 字节数。诊断读数看的是量级，两位有效数字足够，不需要精确到字节。
 * 三位数以上丢掉小数位——「128.4 MB」比「128 MB」多出的那一位不改变任何判断。
 */
export function bytes(n: number): string {
  if (n < 1024) return `${Math.round(n)} B`
  const units = ['KB', 'MB', 'GB', 'TB', 'PB']
  let v = n / 1024
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v >= 100 ? Math.round(v) : v.toFixed(1)} ${units[i]}`
}

/** 运行时长。分钟以下说秒，一天以上说天时——「73 小时」不如「3d1h」好读。 */
export function uptime(sec: number): string {
  if (sec < 60) return `${Math.round(sec)}s`
  if (sec < 3600) return `${Math.floor(sec / 60)}m`
  if (sec < 86400) return `${Math.floor(sec / 3600)}h${Math.floor((sec % 3600) / 60)}m`
  return `${Math.floor(sec / 86400)}d${Math.floor((sec % 86400) / 3600)}h`
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd web && npx vitest run src/lib/format.test.ts`
Expected: 全部 PASS。

- [ ] **Step 5: 新增类型**

在 `web/src/api/types.ts` 的 `Overview` 接口之后追加：

```typescript
/** 数据目录所在文件系统的容量读数。 */
export interface DiskUsage {
  total_bytes: number
  free_bytes: number
  /** 与 df 同口径的已用百分比 */
  used_percent: number
}

/** 运行态系统读数（GET /admin/system）。 */
export interface SystemInfo {
  /** null = 探测失败或非 unix 平台，不是「磁盘为空」 */
  disk: DiskUsage | null
  /** 拒写水位线，0 = 水位保护关闭 */
  watermark_percent: number
  /** true = 当前拒写保读，生产端写入全部失败 */
  write_blocked: boolean
  /** null = 尚未成功统计过 */
  data_dir_bytes: number | null
  /** Go 运行时口径的堆占用，不是进程 RSS */
  go_heap_inuse_bytes: number
  /** Go 运行时向 OS 申请的总量 */
  go_sys_bytes: number
  goroutines: number
  uptime_seconds: number
}
```

- [ ] **Step 6: 新增客户端方法**

在 `web/src/api/client.ts` 的 import type 列表里加入 `SystemInfo`，并在 `overview` 那一行之后追加：

```typescript
  /** 运行态系统读数（磁盘 / 数据目录 / Go 内存 / 拒写状态）。 */
  system: () => request<SystemInfo>('/admin/system'),
```

- [ ] **Step 7: 类型检查与提交**

Run: `cd web && npx tsc -b`
Expected: 无输出（通过）。

```bash
git add web/src/api web/src/lib
git commit -m "feat(console): 新增 SystemInfo 类型、api.system 与字节/时长格式化"
```

---

## Task 8: 总览页系统读数条

**Files:**
- Modify: `web/src/pages/Overview.tsx`

- [ ] **Step 1: 接入取数**

在 `Overview` 函数体内，`const led = usePoll(() => api.ledger())` 之后追加：

```tsx
  // 系统读数变化远慢于业务读数，15 秒一次足够；后端的数据目录统计本身
  // 还有 60 秒 TTL 缓存，拉得更勤只是白跑一趟
  const sys = usePoll(() => api.system(), 15000)
```

- [ ] **Step 2: 在 loading 判定处补一句注释（不把 sys 算进去）**

把 `loading` 的定义前的注释替换为：

```tsx
  // 三个业务数据源首次都为空时整页转「加载中」，之后各自出错各自亮 Notice，不吞错误。
  // sys 刻意不进这个判定：系统读数是第二梯队，让它拖住整页首屏是本末倒置——
  // 它没到就先显示占位符，到了自然补上
```

- [ ] **Step 3: 插入系统读数条**

在业务 `.strip` 的结束 `</div>` 之后、`<section className="panel">` 之前插入：

```tsx
          {/* 系统读数条。布局与 class 名与 prototypes/base/index.html 的第二条 .strip 逐段一致 */}
          <div className="strip">
            <div className="stat">
              <div>
                <div className="stat-label">磁盘使用</div>
                {/* disk 为 null = 探测失败或非 unix 平台，显示「—」而不是 0%；
                    拒写中标红，让这一格自己就能说明「为什么写不进去」 */}
                <div className={`stat-val ${sys.data?.write_blocked ? 'bad' : ''}`}>
                  {sys.data?.disk == null ? '—' : sys.data.disk.used_percent.toFixed(1)}
                  <small>
                    {sys.data?.disk == null
                      ? '不可用'
                      : sys.data.watermark_percent > 0
                        ? `% / 水位 ${sys.data.watermark_percent}%`
                        : '% / 水位未启用'}
                  </small>
                </div>
              </div>
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">数据目录</div>
                <div className="stat-val">
                  {sys.data?.data_dir_bytes == null ? '—' : bytes(sys.data.data_dir_bytes)}
                  <small>{sys.data?.disk ? `可用 ${bytes(sys.data.disk.free_bytes)}` : ''}</small>
                </div>
              </div>
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">GO 运行时内存</div>
                {/* 口径必须写在标签里：这是 Go 运行时的堆占用，不是进程 RSS。
                    Go 归还内存有延迟，把它当 RSS 读会得出「内存泄漏」的错误结论 */}
                <div className="stat-val">
                  {sys.data ? bytes(sys.data.go_heap_inuse_bytes) : '—'}
                  <small>{sys.data ? `/ 申请 ${bytes(sys.data.go_sys_bytes)}` : ''}</small>
                </div>
              </div>
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">运行时长 / 协程</div>
                {/* 运行时长顺带回答「是不是刚重启过」——admin token 重启即失效，
                    用户被踢回登录页时第一个想确认的就是这件事 */}
                <div className="stat-val">
                  {sys.data ? uptime(sys.data.uptime_seconds) : '—'}
                  <small>{sys.data ? `/ ${sys.data.goroutines}` : ''}</small>
                </div>
              </div>
            </div>
          </div>
```

- [ ] **Step 4: 更新 import 与文件头注释**

把 format 的 import 改为：

```tsx
import { fmt, ago, bytes, uptime } from '../lib/format'
```

把文件头注释的「职责」第一条改为：

```tsx
 * 职责：
 *   - 上半屏给整体信号：六项业务读数（写入/总落后/在途/延时待投/死信/TOPIC·消费组）
 *     + 四项系统读数（磁盘/数据目录/Go 内存/运行时长·协程）+ 写入与落后趋势图（1h/24h/7d）
 *   - 下半屏给全部消费关系总账：全表共用一把刻度画 offset 带，可逐行展开到队列级并就地发起操作
```

并在「边界」追加一条：

```tsx
 *   - 系统读数不参与首屏 loading 判定：它是第二梯队，不该拖住业务信号的呈现
```

- [ ] **Step 5: 类型检查与既有测试**

Run: `cd web && npx tsc -b && npx vitest run`
Expected: 全部 PASS。

- [ ] **Step 6: 提交**

```bash
git add web/src/pages/Overview.tsx
git commit -m "feat(console): 总览页新增磁盘/数据目录/内存/运行时长读数条"
```

---

## Task 9: 全站拒写横幅

**Files:**
- Modify: `web/src/components/Shell.tsx`
- Create: `web/src/components/Shell.test.tsx`

- [ ] **Step 1: 写失败测试**

创建 `web/src/components/Shell.test.tsx`：

```typescript
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { Shell } from './Shell'
import { api } from '../api/client'
import type { SystemInfo } from '../api/types'

const base: SystemInfo = {
  disk: { total_bytes: 1000, free_bytes: 100, used_percent: 86.3 },
  watermark_percent: 85,
  write_blocked: false,
  data_dir_bytes: 500,
  go_heap_inuse_bytes: 1024,
  go_sys_bytes: 2048,
  goroutines: 10,
  uptime_seconds: 60,
}

function renderShell() {
  return render(
    <MemoryRouter>
      <Shell title="测试页"><p>内容</p></Shell>
    </MemoryRouter>,
  )
}

beforeEach(() => vi.restoreAllMocks())
afterEach(() => vi.restoreAllMocks())

describe('Shell 拒写横幅', () => {
  it('未拒写时不渲染横幅', async () => {
    vi.spyOn(api, 'system').mockResolvedValue(base)
    renderShell()
    await screen.findByText('内容')
    expect(screen.queryByText(/拒写保读/)).toBeNull()
  })

  it('拒写时渲染横幅并带上百分比与水位线', async () => {
    vi.spyOn(api, 'system').mockResolvedValue({ ...base, write_blocked: true })
    renderShell()
    await waitFor(() => expect(screen.getByText(/拒写保读/)).toBeTruthy())
    expect(screen.getByText(/86\.3%/)).toBeTruthy()
    expect(screen.getByText(/水位线 85%/)).toBeTruthy()
  })

  it('取数失败时静默：外壳不该因为一个辅助读数而报错', async () => {
    vi.spyOn(api, 'system').mockRejectedValue(new Error('boom'))
    renderShell()
    await screen.findByText('内容')
    expect(screen.queryByText(/boom/)).toBeNull()
  })
})
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd web && npx vitest run src/components/Shell.test.tsx`
Expected: 第二个用例 FAIL（找不到「拒写保读」文本）。

- [ ] **Step 3: 写实现**

在 `web/src/components/Shell.tsx`：

import 加入：

```tsx
import { usePoll } from '../hooks/usePoll'
```

在 `Shell` 函数体内 `const nav = useNavigate()` 之后追加：

```tsx
  // 拒写是全站级的运行状态，不是某个页面的业务数据：用户可能在任何页面
  // 察觉「发不进去」——尤其是发送测试消息页。15 秒一次，与总览页同频。
  // 取数失败在这里刻意静默：外壳不该因为一个辅助读数亮红，页面自己的
  // 错误提示已经足够，多一条会淹没真正的问题
  const sys = usePoll(() => api.system(), 15000)
```

把 `<main className="content">{children}</main>` 替换为：

```tsx
        <main className="content">
          {sys.data?.write_blocked && (
            <div className="notice bad">
              <span>
                <b>磁盘水位已触发，服务当前拒写保读。</b>
                {sys.data.disk &&
                  `已用 ${sys.data.disk.used_percent.toFixed(1)}%，水位线 ${sys.data.watermark_percent}%。`}
                生产端会收到写入失败，消费不受影响；清理磁盘或调高 disk_watermark_percent 后自动恢复。
              </span>
            </div>
          )}
          {children}
        </main>
```

- [ ] **Step 4: 更新文件头注释（instrumenting-code：注释）**

把 `web/src/components/Shell.tsx` 的头注释改为：

```tsx
/**
 * 页面外壳：侧栏 + 顶部条
 *
 * 职责：
 *   - 全站导航、当前页高亮、主题切换、退出登录
 *   - 监听 401 事件并跳转登录页
 *   - 拒写保读时在内容区顶部挂全站横幅
 *
 * 边界：
 *   - 不取业务数据：页面自己负责自己的数据。唯一例外是 /admin/system 的
 *     拒写状态——它是全站级运行状态（同「离线提示」一类），放进任何单个
 *     页面都会漏掉用户实际察觉问题的那个页面
 *   - 系统读数取数失败时静默降级，不在外壳亮错误
 *   - 侧栏结构与 prototypes/base/shared/shell.html 逐段一致，class 不改
 *   - 登录页不套本外壳，因此这里的轮询不会在未登录状态下打空转
 */
```

- [ ] **Step 5: 运行全部前端测试**

Run: `cd web && npx tsc -b && npx vitest run`
Expected: 全部 PASS，含新增的三个 Shell 用例。

- [ ] **Step 6: 提交**

```bash
git add web/src/components/Shell.tsx web/src/components/Shell.test.tsx
git commit -m "feat(console): 拒写保读时挂全站横幅"
```

---

## Task 10: 文档同步

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 修正快速开始的兼容性表述**

把「快速开始」里这一句：

```markdown
用官方 RocketMQ 5.x SDK（Java/Go/Python/C#/C++）直接连接 `127.0.0.1:8081` 收发。
```

替换为：

```markdown
用官方 RocketMQ 5.x SDK（Java/Go/Python/C#/C++）直接连接 `127.0.0.1:8081` 收发。
开启鉴权（`access_key`/`secret_key`）后同样兼容这五种 SDK：官方实现之间的签名
头差异（Credential 段带不带 `/{region}/{service}`、签名十六进制大小写）服务端
都已容忍；其中 Go SDK 有真实 e2e 用例覆盖，其余按官方源码的头格式对齐。
```

- [ ] **Step 2: Admin API 端点表补一行**

在 `| GET | /admin/overview | ... |` 那一行之后插入：

```markdown
| GET | `/admin/system` | 运行态读数（磁盘用量与水位、数据目录占用、Go 运行时内存、协程数、运行时长、拒写状态） |
```

- [ ] **Step 3: metrics 清单补四项**

把 `/metrics` 业务指标列表末尾追加：

```markdown
- `sq_disk_used_percent`、`sq_disk_free_bytes`（数据目录所在文件系统，与 `df` 同口径）
- `sq_data_dir_bytes`（数据目录占用，60s TTL 缓存）
- `sq_write_blocked`（磁盘水位拒写开关，1=拒写保读中）——**这一项适合直接配告警**：
  它为 1 时生产端写入全部失败
```

并在该列表下方补一句：

```markdown
内存指标沿用 `go_memstats_*`（Go 采集器自带），不另开一套 `sq_` 前缀的口径。
```

- [ ] **Step 4: Web 控制台段落补说明**

在「十个页面」表格之后、「时序数据」段落之前插入：

```markdown
总览页第二条读数带给系统运行态：磁盘使用率与水位线、数据目录占用与可用空间、
Go 运行时内存、运行时长与协程数。**磁盘水位触发拒写时，全站每个页面顶部都会
挂一条红色横幅**——生产端「发不进去」的原因不该只存在于服务端日志里。

内存那一格是 **Go 运行时口径**（堆占用 / 向 OS 申请量），不是进程 RSS：Go 归还
内存有延迟，拿 RSS 读容易把正常的高位驻留误判成内存泄漏。Linux 上要看真实 RSS，
用 `/metrics` 的 `process_resident_memory_bytes`。

数据目录占用带 60 秒缓存（它只能靠递归遍历得到，每 5 秒算一遍是把诊断读数变成
I/O 负担），因此这个数最多滞后一分钟。
```

- [ ] **Step 5: 安全边界补一条**

在「安全边界（刻意为之，部署前请知悉）」列表末尾追加：

```markdown
- gRPC 签名**不绑请求内容**：官方 gRPC 协议本身只对 `x-mq-date-time` 做 HMAC
  （remoting 协议的 ACL 1.0 才对请求字段签名），因此签名可跨请求复用。这是协议
  性质而非本实现的取舍，抗重放只能靠 TLS 与网络隔离。
- 只有**单组 AK/SK 且只认证不授权**：签名通过即全权限，没有 topic/消费组粒度、
  没有 Pub/Sub 区分、没有 IP 白名单（RocketMQ ACL 2.0 有这些）。多组 AK/SK 排在
  v1.0 之前，见里程碑。
```

- [ ] **Step 6: 配置段补一句**

在 `disk_watermark_percent: 85` 那一行的注释后追加说明（改为）：

```yaml
disk_watermark_percent: 85     # 磁盘使用率超过即拒写保读；0=关闭。状态可在控制台
                               # 总览页与 /metrics 的 sq_write_blocked 观察
```

- [ ] **Step 7: 提交**

```bash
git add README.md
git commit -m "docs: 同步系统读数端点、新增 metrics、签名兼容性与安全边界"
```

---

## Task 11: 端到端验证与收尾

**Files:** 无（验证）

- [ ] **Step 1: 全量测试**

Run: `go test ./... && cd web && npx vitest run && npx tsc -b`
Expected: 全部 PASS。

- [ ] **Step 2: e2e 测试**

Run: `make e2e`
Expected: 全部 PASS（Task 1 改动了认证路径，`test/e2e/sdk_auth_test.go` 必须仍然过）。

- [ ] **Step 3: 完整构建**

Run: `make build`
Expected: 生成 `./sq`，无错误。

- [ ] **Step 4: 起服务人工验证**

按 superdev skill 的纪律，先 `list_services` 确认本项目是否已被 SuperDev 接管：
- **已接管**：用 `restart_service` 起服务，用 `tail_logs` 看启动日志。
- **未接管**：`./sq -config sq.yaml`。

起来后逐项确认：

```bash
curl -s localhost:8082/admin/system | jq
```
Expected: 返回含 `disk`（非 null）、`write_blocked: false`、`data_dir_bytes`（数字）、`go_heap_inuse_bytes`、`goroutines`、`uptime_seconds` 的 JSON。

```bash
curl -s localhost:8082/metrics | grep -E '^sq_(disk|data_dir|write_blocked)'
```
Expected: 四行指标，`sq_write_blocked 0`。

浏览器打开 `http://127.0.0.1:8082/`，确认总览页第二条读数带四项都有值、没有「—」。

- [ ] **Step 5: 验证拒写横幅真的会出现**

把 `sq.yaml` 的 `disk_watermark_percent` 临时改成一个低于当前使用率的值（比如当前 71% 就填 `10`），重启服务，等一个 retention 周期（`retention_check_interval`，默认 5m；想快点可临时改成 `10s`）。

Expected:
- 日志出现 `磁盘使用率超过水位线，进入拒写保读`，带 `used_pct`/`watermark`/`free_bytes`/`total_bytes`。
- 控制台**每个页面**顶部出现红色横幅，磁盘那一格数字标红。
- `curl -s localhost:8082/metrics | grep sq_write_blocked` 返回 `1`。
- 在「发送测试消息」页发一条，收到写入被拒的错误。

验证完把 `disk_watermark_percent` 与 `retention_check_interval` 改回原值并重启，确认日志出现「磁盘使用率回落，恢复写入」、横幅消失。

- [ ] **Step 6: instrumenting-code 完工自检**

逐条确认，任一未过必须先修：

- [ ] 每个错误分支都带上下文记了日志：`sysinfo.dataDirBytes` 的统计失败（带 `dir`/`err`/`has_stale`）、`Snapshot` 的磁盘探测失败（带 `dir`/`err`）、`retention.checkDisk` 的探测失败（带 `dir`/`err`）
- [ ] 状态变更有日志：`retention.checkDisk` 的拒写翻转两向都记，且带 `used_pct`/`watermark`/`free_bytes`
- [ ] 成功路径不静默：`sysinfo.New` 记「采集器就绪」（带 `data_dir`/`watermark_pct`/`dir_size_ttl`），`dataDirBytes` 刷新时记 Debug
- [ ] 关键查询有记录：`handleSystem` 在拒写态下记 Warn，可与 retention 的翻转日志对齐时间线
- [ ] 全程使用 `slog`，没有 `fmt.Printf` / `console.log`
- [ ] 新文件都有头注释（职责 + 边界）：`sysinfo.go`、`disk_unix.go`、`disk_other.go`、`reporter.go`、`system.go`、`Shell.test.tsx` 以外的三个前端新文件
- [ ] 导出函数都有文档注释：`DiskUsage`、`New`、`WriteBlocked`、`Snapshot`、`bytes`、`uptime`
- [ ] 非显然分支有「为什么」注释：TTL 缓存的存在理由、`Bavail` 而非 `Bfree` 的理由、签名折小写的理由、Shell 破例取数的理由、内存不进 `/metrics` 的理由

- [ ] **Step 7: 最终代码审阅（用户 CLAUDE.md §5 清单）**

逐项对照全局 CLAUDE.md 的最终审阅表确认，重点：
- 无跨层调用：`admin` 只通过 `sysinfo.Reporter` 读运行态，没有绕过它直接摸 `atomic.Bool`
- 优先复用：横幅复用了既有的 `.notice.bad`，没有新增 CSS
- 无硬编码：`dirSizeTTL` 是具名常量；水位线来自配置

- [ ] **Step 8: 收尾**

Run: `git log --oneline -11`
Expected: 看到本计划的 10 次提交（Task 1–10），历史干净。

按 `superpowers:finishing-a-development-branch` 决定合并方式。收尾时确认：本次对 `prototypes/base/` 的改动（Task 6）已经在分支内完成，不需要额外回流。

---

## 自查记录

**覆盖检查**：C（签名大小写）→ Task 1；A 的三件事 —— 拒写状态可见 → Task 4（metrics）+ Task 5（端点）+ Task 8/9（前端）；整体磁盘使用率与水位线 → Task 2/3/5/8；数据目录占用 → Task 3（TTL 缓存）/4/5/8；内存 → Task 3/5/8（刻意不进 metrics，理由已写入注释与 README）。原型不变量 → Task 6。文档 → Task 10。验证 → Task 11。

**类型一致性**：`sysinfo.Disk`/`Snapshot` 的 JSON tag 与前端 `DiskUsage`/`SystemInfo` 字段逐一对应（`disk`/`watermark_percent`/`write_blocked`/`data_dir_bytes`/`go_heap_inuse_bytes`/`go_sys_bytes`/`goroutines`/`uptime_seconds`）。`Reporter` 在 Go 侧的三个使用方（retention 用 `DiskUsage` 函数、metrics 用 `Snapshot()`、admin 用 `Snapshot()` 与 `WriteBlocked()`）签名一致。`admin.Server.blocked()` 是 admin 内部判定拒写的唯一入口，两处调用点已列明。

**已知的执行期不确定点**（不是占位符，是需要按现状对齐的地方）：`internal/metrics/metrics_test.go` 与 `internal/admin/server_test.go` 的测试辅助函数名以各文件现状为准，Task 4 Step 1 与 Task 5 Step 1 已注明按实际辅助名调整调用。
