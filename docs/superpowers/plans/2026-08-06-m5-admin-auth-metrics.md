# M5a — 认证 + Admin API + Metrics 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 sq 增加 gRPC 静态 AK/SK 认证、带用户名密码登录的 HTTP Admin API、Prometheus `/metrics`（spec §6 认证、§8 可观测性、§9 Admin API 的后端部分）。

**Architecture:** 三块相互独立的接入面增强：(1) gRPC 拦截器校验官方 SDK 的 `MQv2-HMAC-SHA1` 签名头；(2) 新增 `internal/admin` HTTP 服务（net/http + Go 1.22 ServeMux 路由，无新框架），登录发 token，资源 handler 全部组合既有 core API；(3) `internal/metrics` 把 broker 状态做成**抓取期计算**的 Prometheus Collector——堆积/inflight/写入总量全部从 store 状态推导，**零热路径埋点**（唯一例外是 store.Apply 的 fsync 直方图钩子）。core 层只做纯新增（meta 增删改、deliver.ResetCursor、query.Browse、adminops 清理），不改任何既有函数的行为。

**Tech Stack:** Go 1.26 标准库 net/http、prometheus/client_golang（新依赖）、既有 Pebble/slog/官方 RocketMQ Go SDK（e2e）。

## M5 拆分说明（scope）

spec §11 的 M5 = 控制台 + Admin API + metrics。本计划是 **M5a：后端全部**（认证 + Admin API + metrics）。**M5b（React 控制台）单独成计划**，理由：(1) 控制台只消费 Admin API，是独立可交付子系统（writing-plans 的 Scope Check 要求拆开）；(2) 前端页面形态按全局规范须先走原型/brainstorm 流程确认；(3) M5a 出口标准「日常排查够用」靠 Admin API + metrics 已能达成（curl / Prometheus 即可用）。

**用户明确要求**：登录用户名与密码在配置文件中配置——本计划 Task 1 的 `admin_username`/`admin_password` 即此项，Task 5 实现登录。

## 与 M4 并行执行说明（重要）

- 本计划基线是 `m3-delay-messages` tip（`340e989`），分支 `m5-admin-auth`。M4 正在另一会话/主工作区执行，**执行本计划必须用独立 git worktree checkout `m5-admin-auth`，绝不能在主工作区（M4 占用中）操作**。
- 冲突面已刻意压到最小：M4 改 `deliver.go` 主体/`send.go`/`receive.go`/`server.go` 路由通告/`core/types.go`；本计划对这些文件**只在文件尾追加新函数**（`deliver.ResetCursor`）或完全不碰（send/receive/types）。`server.go` 不改；gRPC 认证是新文件 `rpc/auth.go` + `main.go` 接线（M4 不动 main.go）。deliver 的新测试放**新文件** `reset_test.go`，不碰 M4 正在改的 `deliver_test.go`。
- 合并顺序：M4 → main 之后，本分支 merge/rebase main；预期冲突仅 README 功能表与 go.mod/go.sum（M4 不加依赖，大概率零冲突）。

## Global Constraints

- 日志一律 `slog`（logger.With("mod", ...) 惯例），**禁止 fmt.Printf/print**；关键节点（进入/退出/错误/状态变更）必须打日志，错误必带上下文（instrumenting-code skill 全程生效）。
- 每个新文件顶部写中文职责/边界注释；每个导出方法写文档注释；复杂逻辑注释「为什么」。
- 认证默认关闭（spec §6）：AK/SK 两项均空 = gRPC 不鉴权；admin 用户名密码均空 = Admin API 免登录。半配置（只填一半）= 启动报错。
- e2e 配置零值陷阱（M2 教训）：`writeBrokerConfig` 用 `yaml.Marshal(config.Config)` 序列化零值——新增配置字段的**空串/零值必须是合法语义**（本计划所有新字段零值=功能关闭，天然满足；不允许出现「必须非零」的新字段）。
- 单元测试 `go test ./...`；e2e `go test -tags e2e ./test/e2e/ -run <用例> -v -timeout 300s`（e2e 由 TestMain 现场编译 broker 二进制）。
- 每个 task 结束跑 `gofmt -l .`（应无输出）与 `go vet ./...`。
- 提交信息结尾：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。

## File Structure

- Modify: `internal/config/config.go` — 5 个新字段 + 成对校验（Task 1）
- Create: `internal/rpc/auth.go` + `internal/rpc/auth_test.go` — gRPC 签名校验拦截器（Task 2）
- Modify: `internal/store/keys.go` — CursorPrefix/ParseCursorKey/按组前缀（Task 3）
- Modify: `internal/core/meta/meta.go` — GetGroup/Groups/UpdateTopicRetention/DeleteTopic/DeleteGroup（Task 3）
- Create: `internal/core/adminops/adminops.go` — topic/group 数据成片清理（Task 3）
- Modify: `internal/core/deliver/deliver.go` — 文件尾追加 ResetCursor（Task 3）
- Create: `internal/core/deliver/reset_test.go` — ResetCursor 测试（新文件避开 M4）（Task 3）
- Modify: `internal/store/store.go` — Apply 耗时观测钩子（Task 4）
- Create: `internal/metrics/stats.go` + `collector.go` + `metrics_test.go`（Task 4）
- Create: `internal/admin/server.go` + `auth.go` + `http.go` + 各 handler 文件 + 测试（Task 5-7）
- Modify: `internal/core/query/query.go` — 追加 Browse（Task 7）
- Modify: `cmd/sq/main.go` — gRPC 拦截器接线（Task 2）、admin HTTP 起停（Task 5）
- Create: `test/e2e/sdk_auth_test.go`、`test/e2e/admin_test.go`（Task 8）
- Modify: `README.md`（Task 8）

---

### Task 1: 配置 — 认证与 Admin 监听

**Files:**
- Modify: `internal/config/config.go`
- Test: `internal/config/config_test.go`（已存在，追加用例）

**Interfaces:**
- Consumes: 无
- Produces: `Config` 新字段 `AdminListen string`、`AdminUsername string`、`AdminPassword string`、`AccessKey string`、`SecretKey string`（后续所有 task 从 cfg 读取）

- [ ] **Step 1: 写失败测试**（追加到 `internal/config/config_test.go`）

```go
// TestLoadAuthPairValidation 认证配置必须成对：只填一半是笔误，启动即报错，
// 不能静默变成"看起来配了认证实际没生效"。
func TestLoadAuthPairValidation(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"只有access_key", "access_key: ak\n"},
		{"只有secret_key", "secret_key: sk\n"},
		{"只有admin_username", "admin_username: root\n"},
		{"只有admin_password", "admin_password: pw\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "sq.yaml")
			if err := os.WriteFile(p, []byte(c.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(p); err == nil {
				t.Fatalf("半配置 %q 应报错", c.yaml)
			}
		})
	}
}

// TestLoadAuthDefaults 默认值：认证全关、admin 监听 :8082；成对配置能被加载。
func TestLoadAuthDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccessKey != "" || cfg.SecretKey != "" || cfg.AdminUsername != "" || cfg.AdminPassword != "" {
		t.Fatalf("认证默认应全空: %+v", cfg)
	}
	if cfg.AdminListen != ":8082" {
		t.Fatalf("admin_listen 默认应为 :8082，得到 %q", cfg.AdminListen)
	}
	p := filepath.Join(t.TempDir(), "sq.yaml")
	y := "access_key: ak\nsecret_key: sk\nadmin_username: root\nadmin_password: pw\nadmin_listen: \"\"\n"
	if err := os.WriteFile(p, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccessKey != "ak" || cfg.AdminUsername != "root" || cfg.AdminListen != "" {
		t.Fatalf("成对配置加载不符: %+v", cfg)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/config/ -run 'TestLoadAuth' -v`
Expected: FAIL（字段不存在，编译错误）

- [ ] **Step 3: 实现**

`Config` 结构体追加（放在 `LogLevel` 之后）：

```go
	// —— M5 认证与管理面 ——
	AdminListen   string `yaml:"admin_listen"`   // Admin HTTP 监听地址，"" = 关闭；默认 :8082
	AdminUsername string `yaml:"admin_username"` // Admin API 登录用户名（与密码成对，均空 = 免登录）
	AdminPassword string `yaml:"admin_password"` // Admin API 登录密码
	AccessKey     string `yaml:"access_key"`     // gRPC 静态 AK（与 secret_key 成对，均空 = 不鉴权，spec §6 默认关闭）
	SecretKey     string `yaml:"secret_key"`     // gRPC 静态 SK
```

`Load` 的默认值块加 `AdminListen: ":8082",`。校验（追加在 log_level 校验之后）：

```go
	// 认证配置必须成对出现：只填一半几乎必然是笔误——比如配了 access_key 忘了
	// secret_key，此时"启用但秘钥为空"和"未启用"两种解释都会让用户在真出事时
	// 误判认证状态，启动即报错是唯一不含糊的处理。
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return nil, fmt.Errorf("配置 access_key/secret_key 必须成对设置（或都留空以关闭 gRPC 认证）")
	}
	if (cfg.AdminUsername == "") != (cfg.AdminPassword == "") {
		return nil, fmt.Errorf("配置 admin_username/admin_password 必须成对设置（或都留空以免登录）")
	}
```

注意：**所有新字段零值都是合法语义（=关闭）**，这是 Global Constraints 里 e2e yaml 零值陷阱的要求——`writeBrokerConfig` 序列化出的 `admin_listen: ""` 会让既有 e2e 的 broker 不开 admin 端口（避免并行用例抢端口），行为正确且无需改动既有 e2e。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/config/ -v`
Expected: 全部 PASS（含既有用例）

- [ ] **Step 5: gofmt/vet + 提交**

```bash
gofmt -l . && go vet ./...
git add internal/config/
git commit -m "feat(config): M5 认证与 admin 监听配置（成对校验，零值=关闭）"
```

---

### Task 2: gRPC AK/SK 认证拦截器

**Files:**
- Create: `internal/rpc/auth.go`
- Test: `internal/rpc/auth_test.go`
- Modify: `cmd/sq/main.go`（grpc.NewServer 选项接线）

**Interfaces:**
- Consumes: Task 1 的 `cfg.AccessKey`/`cfg.SecretKey`
- Produces: `func NewAuthInterceptors(accessKey, secretKey string, logger *slog.Logger) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor)`

**协议事实（已对官方 Go SDK v5.1.4 源码核实，client.go:664 Sign 方法）**：SDK 对每个 RPC 附带 metadata：`x-mq-date-time` = `time.Now().Format("20060102T150405Z")`；`authorization` = `MQv2-HMAC-SHA1 Credential={ak}//Rocketmq, SignedHeaders=x-mq-date-time, Signature={hex(hmac_sha1(secret, datetime串))}`（hex 为小写）。Credential 第二段（region）为空串、第三段固定 `Rocketmq`。`Credentials` 为 nil 时完全不带 authorization 头；**既有 e2e 用 `&credentials.SessionCredentials{}`（非 nil、空 AK/SK），会带着空 AK 的签名头**——服务端未启用认证时必须无条件放行，不能因为头存在就校验。

- [ ] **Step 1: 写失败测试** `internal/rpc/auth_test.go`

```go
// 认证拦截器测试：按官方 SDK 的签名算法（hmac-sha1(secret, x-mq-date-time)，
// hex 小写）构造 metadata，验证通过/拒绝路径。不起真实 gRPC 连接——拦截器
// 只依赖 context 里的 metadata，直接构造 incoming context 即可。
package rpc

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// signedCtx 按 SDK 算法构造带签名头的 incoming context。
func signedCtx(ak, secret, datetime string) context.Context {
	h := hmac.New(sha1.New, []byte(secret))
	h.Write([]byte(datetime))
	auth := fmt.Sprintf("MQv2-HMAC-SHA1 Credential=%s//Rocketmq, SignedHeaders=x-mq-date-time, Signature=%s",
		ak, hex.EncodeToString(h.Sum(nil)))
	md := metadata.Pairs("x-mq-date-time", datetime, "authorization", auth)
	return metadata.NewIncomingContext(context.Background(), md)
}

// callUnary 让 ctx 过一遍 unary 拦截器，返回 handler 是否被放行执行。
func callUnary(t *testing.T, u grpc.UnaryServerInterceptor, ctx context.Context) error {
	t.Helper()
	called := false
	_, err := u(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test/M"},
		func(ctx context.Context, req any) (any, error) { called = true; return nil, nil })
	if err == nil && !called {
		t.Fatal("放行时 handler 应被执行")
	}
	if err != nil && called {
		t.Fatal("拒绝时 handler 不应被执行")
	}
	return err
}

func TestAuthUnaryAcceptsValidSignature(t *testing.T) {
	u, _ := NewAuthInterceptors("ak1", "sk1", slog.Default())
	if err := callUnary(t, u, signedCtx("ak1", "sk1", "20260806T120000Z")); err != nil {
		t.Fatalf("合法签名应放行: %v", err)
	}
}

func TestAuthUnaryRejects(t *testing.T) {
	u, _ := NewAuthInterceptors("ak1", "sk1", slog.Default())
	cases := []struct {
		name string
		ctx  context.Context
	}{
		{"AK不匹配", signedCtx("evil", "sk1", "20260806T120000Z")},
		{"秘钥不匹配", signedCtx("ak1", "wrong", "20260806T120000Z")},
		{"无认证头", metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("x-mq-date-time", "20260806T120000Z"))},
		{"无metadata", context.Background()},
		{"头格式损坏", metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("x-mq-date-time", "20260806T120000Z", "authorization", "Basic abc"))},
		{"缺datetime头", metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("authorization", "MQv2-HMAC-SHA1 Credential=ak1//Rocketmq, SignedHeaders=x-mq-date-time, Signature=00"))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := callUnary(t, u, c.ctx)
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("应返回 Unauthenticated，得到 %v", err)
			}
		})
	}
}

func TestAuthStreamRejects(t *testing.T) {
	_, s := NewAuthInterceptors("ak1", "sk1", slog.Default())
	err := s(nil, &fakeServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test/S"},
		func(srv any, stream grpc.ServerStream) error { t.Fatal("拒绝时不应进入 handler"); return nil })
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("应返回 Unauthenticated，得到 %v", err)
	}
	err = s(nil, &fakeServerStream{ctx: signedCtx("ak1", "sk1", "20260806T120000Z")}, &grpc.StreamServerInfo{FullMethod: "/test/S"},
		func(srv any, stream grpc.ServerStream) error { return nil })
	if err != nil {
		t.Fatalf("合法签名应放行: %v", err)
	}
}

// fakeServerStream 只提供 Context()，其余方法不会被拦截器触碰。
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestAuth' -v`
Expected: FAIL（NewAuthInterceptors 未定义）

- [ ] **Step 3: 实现** `internal/rpc/auth.go`

```go
// auth.go: gRPC 静态 AK/SK 认证（spec §6「可选静态 AK/SK（Signature 头校验），默认关闭」）。
//
// 职责：
//   - 校验官方 SDK 的 MQv2-HMAC-SHA1 签名头（unary 与 stream 两个拦截器）
//   - 校验失败返回 gRPC codes.Unauthenticated，SDK 侧表现为 RPC 直接报错
//
// 边界：
//   - 只在 main 装配时按配置决定是否安装；本文件不读配置
//   - 不校验 x-mq-date-time 的时效（不做重放窗口）：目标场景是可信内网里挡住
//     误连与弱隔离，不对抗抓包重放；引入时间窗会让客户端时钟偏移变成一类
//     极难排查的"随机认证失败"，代价大于收益，边界在 README 写明
//   - 签名算法与头格式以官方 Go SDK v5.1.4 client.go Sign 方法为准：
//     authorization = "MQv2-HMAC-SHA1 Credential={ak}//Rocketmq,
//     SignedHeaders=x-mq-date-time, Signature={hex_lower(hmac_sha1(sk, datetime))}"
package rpc

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	authHeaderKey  = "authorization"
	dateTimeKey    = "x-mq-date-time"
	authSchemeName = "MQv2-HMAC-SHA1"
)

// parseAuthorization 解析 SDK 的 authorization 头，取出 AccessKey 与签名。
// 头格式不符返回 ok=false——统一按认证失败处理，不区分"格式坏"与"没带"。
func parseAuthorization(h string) (ak, sig string, ok bool) {
	rest, found := strings.CutPrefix(h, authSchemeName+" ")
	if !found {
		return "", "", false
	}
	for _, part := range strings.Split(rest, ",") {
		part = strings.TrimSpace(part)
		if v, f := strings.CutPrefix(part, "Credential="); f {
			// Credential={ak}/{region}/{service}：SDK 固定拼 "{ak}//Rocketmq"，
			// AK 取第一段。AK 含 '/' 时客户端侧编码本身就有歧义，不予支持。
			ak, _, _ = strings.Cut(v, "/")
		} else if v, f := strings.CutPrefix(part, "Signature="); f {
			sig = v
		}
	}
	return ak, sig, ak != "" && sig != ""
}

// verifyAuth 校验 ctx metadata 中的签名。所有失败路径统一返回 Unauthenticated，
// 错误信息不区分"AK 错"与"签名错"——认证错误细节是给攻击者的探针，不外泄。
func verifyAuth(ctx context.Context, wantAK, secret string, logger *slog.Logger, method string) error {
	md, mok := metadata.FromIncomingContext(ctx)
	if !mok {
		logger.Warn("认证失败：请求无 metadata", "method", method)
		return status.Error(codes.Unauthenticated, "认证失败")
	}
	auths := md.Get(authHeaderKey)
	dates := md.Get(dateTimeKey)
	if len(auths) == 0 || len(dates) == 0 {
		logger.Warn("认证失败：缺少认证头", "method", method,
			"has_authorization", len(auths) > 0, "has_date_time", len(dates) > 0)
		return status.Error(codes.Unauthenticated, "认证失败")
	}
	ak, sig, pok := parseAuthorization(auths[0])
	if !pok {
		logger.Warn("认证失败：authorization 头格式不符", "method", method)
		return status.Error(codes.Unauthenticated, "认证失败")
	}
	h := hmac.New(sha1.New, []byte(secret))
	h.Write([]byte(dates[0]))
	expect := hex.EncodeToString(h.Sum(nil))
	// 两个比较都走常数时间，且必须同时判定后再短路——先比 AK 再比签名的
	// 提前返回会泄露"AK 对不对"这一位信息。
	akOK := subtle.ConstantTimeCompare([]byte(ak), []byte(wantAK)) == 1
	sigOK := subtle.ConstantTimeCompare([]byte(sig), []byte(expect)) == 1
	if !akOK || !sigOK {
		logger.Warn("认证失败：AK 或签名不匹配", "method", method, "access_key", ak)
		return status.Error(codes.Unauthenticated, "认证失败")
	}
	return nil
}

// NewAuthInterceptors 构造 unary 与 stream 两个认证拦截器。调用方（main）仅在
// accessKey 非空时安装；两个拦截器共享同一套校验逻辑，覆盖全部 RPC——包括
// ReceiveMessage（服务端流）与 Telemetry（双向流），SDK 对它们同样签名。
func NewAuthInterceptors(accessKey, secretKey string, logger *slog.Logger) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	l := logger.With("mod", "rpc.auth")
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := verifyAuth(ctx, accessKey, secretKey, l, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := verifyAuth(ss.Context(), accessKey, secretKey, l, info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
	return unary, stream
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/rpc/ -run 'TestAuth' -v`
Expected: PASS

- [ ] **Step 5: main.go 接线**

`cmd/sq/main.go` 中 `gs := grpc.NewServer(...)` 改为：

```go
	gopts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(rpc.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(rpc.MaxGRPCMessageSize),
	}
	// AK/SK 认证按配置装配（spec §6 默认关闭）。拦截器必须 unary+stream 成对装：
	// 只装 unary 会让 ReceiveMessage/Telemetry 两条流绕过认证。
	if cfg.AccessKey != "" {
		au, as := rpc.NewAuthInterceptors(cfg.AccessKey, cfg.SecretKey, logger)
		gopts = append(gopts, grpc.ChainUnaryInterceptor(au), grpc.ChainStreamInterceptor(as))
		logger.Info("gRPC AK/SK 认证已启用", "access_key", cfg.AccessKey)
	}
	gs := grpc.NewServer(gopts...)
```

（原 `grpc.NewServer` 上方关于 MaxRecvMsgSize 的注释保留在 gopts 定义处。）

- [ ] **Step 6: 全量测试 + gofmt/vet**

Run: `go build ./... && go test ./... && gofmt -l . && go vet ./...`
Expected: 全部通过，gofmt 无输出

- [ ] **Step 7: 提交**

```bash
git add internal/rpc/auth.go internal/rpc/auth_test.go cmd/sq/main.go
git commit -m "feat(rpc): gRPC 静态 AK/SK 认证拦截器（MQv2-HMAC-SHA1，默认关闭）"
```

---

### Task 3: core 管理面基础（keys/meta/adminops/ResetCursor）

**Files:**
- Modify: `internal/store/keys.go`（追加 4 个函数）
- Test: `internal/store/keys_test.go`（追加）
- Modify: `internal/core/meta/meta.go`（追加 5 个方法 + 1 个 sentinel error）
- Test: `internal/core/meta/meta_test.go`（追加）
- Create: `internal/core/adminops/adminops.go` + `adminops_test.go`
- Modify: `internal/core/deliver/deliver.go`（**仅文件尾追加** ResetCursor，避开 M4 冲突区）
- Create: `internal/core/deliver/reset_test.go`（新文件，不碰 deliver_test.go）

**Interfaces:**
- Consumes: 既有 `store.Store`（NewBatch/Apply/Scan/Get）、`meta.Meta`、deliver fixture 辅助（`newFixture(t)` 返回 `*fixture{st,pr,dl}`，topic 默认 1 队列；`f.send(t, topic, body)`；`dl.Receive(ctx, group, topic, queueID, maxMsgs, invisible, wait, filter)`）
- Produces（后续 task 依赖的准确签名）:
  - `store.CursorPrefix() []byte`、`store.CursorGroupPrefix(group string) []byte`、`store.InflightAllPrefix() []byte`、`store.InflightGroupPrefix(group string) []byte`、`store.ParseCursorKey(k []byte) (group, topic string, queueID uint32, err error)`
  - `meta.ErrGroupNotFound`、`(*Meta) GetGroup(name string) (GroupConfig, bool)`、`(*Meta) Groups() []GroupConfig`、`(*Meta) UpdateTopicRetention(name string, retentionMs int64) (TopicConfig, error)`、`(*Meta) DeleteTopic(name string) error`、`(*Meta) DeleteGroup(name string) error`
  - `adminops.PurgeTopicData(st *store.Store, tc meta.TopicConfig) error`、`adminops.PurgeGroupData(st *store.Store, group string) error`
  - `(*deliver.Deliverer) ResetCursor(group, topic string, queueID uint32, offset uint64) error`

- [ ] **Step 1: keys.go 失败测试**（追加到 `internal/store/keys_test.go`）

```go
// TestCursorKeyRoundTrip Cursor key 的构造/解析闭环与前缀正确性。
func TestCursorKeyRoundTrip(t *testing.T) {
	k := CursorKey("g1", "t1", 7)
	g, tp, q, err := ParseCursorKey(k)
	if err != nil || g != "g1" || tp != "t1" || q != 7 {
		t.Fatalf("解析不符: %s %s %d %v", g, tp, q, err)
	}
	if !bytes.HasPrefix(k, CursorGroupPrefix("g1")) {
		t.Fatal("CursorGroupPrefix 应是 CursorKey 的前缀")
	}
	if !bytes.HasPrefix(k, CursorPrefix()) {
		t.Fatal("CursorPrefix 应是 CursorKey 的前缀")
	}
	// 组名前缀必须带 '/'：不带的话 "g1" 会误扫 "g10" 的位点
	if bytes.HasPrefix(CursorKey("g10", "t1", 0), CursorGroupPrefix("g1")) {
		t.Fatal("CursorGroupPrefix(\"g1\") 不得匹配 g10 的 key")
	}
	if _, _, _, err := ParseCursorKey([]byte("cursor/损坏")); err == nil {
		t.Fatal("坏 key 应报错")
	}
	if !bytes.HasPrefix(InflightKey("g1", "t1", 0, 0), InflightGroupPrefix("g1")) {
		t.Fatal("InflightGroupPrefix 应是 InflightKey 的前缀")
	}
	if !bytes.HasPrefix(InflightKey("g1", "t1", 0, 0), InflightAllPrefix()) {
		t.Fatal("InflightAllPrefix 应是 InflightKey 的前缀")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/store/ -run TestCursorKeyRoundTrip -v`
Expected: FAIL（函数未定义）

- [ ] **Step 3: keys.go 实现**（追加到文件尾）

```go
// CursorPrefix 全部消费位点的扫描下界（metrics/管理面全量遍历用）。
func CursorPrefix() []byte { return []byte(cursorPrefix) }

// CursorGroupPrefix 某消费组全部位点的扫描下界（含结尾 '/'，防 "g1" 误扫 "g10"）。
func CursorGroupPrefix(group string) []byte { return []byte(cursorPrefix + group + "/") }

// InflightAllPrefix 全部 inflight 记录的扫描下界（metrics 统计用）。
func InflightAllPrefix() []byte { return []byte(inflightPrefix) }

// InflightGroupPrefix 某消费组全部 inflight 的扫描下界（含结尾 '/'）。
func InflightGroupPrefix(group string) []byte { return []byte(inflightPrefix + group + "/") }

// ParseCursorKey 解析 cursor key：cursor/{group}/{topic}/{queueID:4B}。
// 两段名字按 '/' 定位，尾部必须恰好 4 字节定长二进制（与 ParseInflightKey 同理，
// 二进制段可能含 '/'，只能按位置解析）。
func ParseCursorKey(k []byte) (group, topic string, queueID uint32, err error) {
	rest, ok := bytes.CutPrefix(k, []byte(cursorPrefix))
	if !ok {
		return "", "", 0, fmt.Errorf("非法 cursor key: %q", k)
	}
	i := bytes.IndexByte(rest, '/')
	if i < 0 {
		return "", "", 0, fmt.Errorf("cursor key 结构错误: %q", k)
	}
	group = string(rest[:i])
	rest = rest[i+1:]
	j := bytes.IndexByte(rest, '/')
	if j < 0 || len(rest)-j-1 != 4 {
		return "", "", 0, fmt.Errorf("cursor key 结构错误: %q", k)
	}
	topic = string(rest[:j])
	return group, topic, binary.BigEndian.Uint32(rest[j+1:]), nil
}
```

Run: `go test ./internal/store/ -v` → PASS

- [ ] **Step 4: meta 失败测试**（追加到 `internal/core/meta/meta_test.go`；该文件既有 fixture 直接复用——若无独立 fixture，用 `store.Open(t.TempDir(), true, slog.Default())` + `meta.New(st, true, 4, 16, slog.Default())` 就地构造）

```go
// TestTopicUpdateAndDelete 修改 retention、删除 topic 与错误路径。
func TestTopicUpdateAndDelete(t *testing.T) {
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	m, err := New(st, true, 4, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic("t1", 2); err != nil {
		t.Fatal(err)
	}
	tc, err := m.UpdateTopicRetention("t1", 1000)
	if err != nil || tc.RetentionMs != 1000 {
		t.Fatalf("更新 retention 失败: %+v %v", tc, err)
	}
	// 持久化必须生效：重开 meta 后新值仍在
	m2, err := New(st, true, 4, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if tc, _ := m2.GetTopic("t1"); tc.RetentionMs != 1000 {
		t.Fatalf("retention 未持久化: %+v", tc)
	}
	if _, err := m.UpdateTopicRetention("t1", 0); err == nil {
		t.Fatal("retention<=0 应拒绝")
	}
	if _, err := m.UpdateTopicRetention("nope", 1000); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("不存在的 topic 应返回 ErrTopicNotFound: %v", err)
	}
	if err := m.DeleteTopic("t1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.GetTopic("t1"); ok {
		t.Fatal("删除后不应可见")
	}
	if err := m.DeleteTopic("t1"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("重复删除应返回 ErrTopicNotFound: %v", err)
	}
}

// TestGroupAccessorsAndDelete GetGroup/Groups/DeleteGroup。
func TestGroupAccessorsAndDelete(t *testing.T) {
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	m, err := New(st, true, 4, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.EnsureGroup("g1"); err != nil {
		t.Fatal(err)
	}
	if gc, ok := m.GetGroup("g1"); !ok || gc.Name != "g1" {
		t.Fatalf("GetGroup 不符: %+v %v", gc, ok)
	}
	if _, ok := m.GetGroup("nope"); ok {
		t.Fatal("不存在的组不应命中")
	}
	if gs := m.Groups(); len(gs) != 1 || gs[0].Name != "g1" {
		t.Fatalf("Groups 快照不符: %+v", gs)
	}
	if err := m.DeleteGroup("g1"); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteGroup("g1"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("重复删除应返回 ErrGroupNotFound: %v", err)
	}
}
```

Run: `go test ./internal/core/meta/ -run 'TestTopicUpdateAndDelete|TestGroupAccessorsAndDelete' -v` → FAIL

- [ ] **Step 5: meta 实现**（追加到 meta.go；sentinel 放在 ErrBadName 旁）

```go
// ErrGroupNotFound 订阅组不存在（Admin API 删除/查询用）。
var ErrGroupNotFound = errors.New("订阅组不存在")
```

```go
// GetGroup 查询订阅组配置（只读，不注册——与 EnsureGroup 的区别）。
func (m *Meta) GetGroup(name string) (GroupConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	gc, ok := m.groups[name]
	return gc, ok
}

// Groups 返回全部订阅组配置快照（Admin API/metrics 用；乱序）。
func (m *Meta) Groups() []GroupConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]GroupConfig, 0, len(m.groups))
	for _, gc := range m.groups {
		out = append(out, gc)
	}
	return out
}

// UpdateTopicRetention 修改 topic 保留时长并持久化。retentionMs 必须 >0：
// 0 在 TopicConfig 里是"M1 旧数据回退默认"的哨兵值，允许写入会让两种语义混淆。
func (m *Meta) UpdateTopicRetention(name string, retentionMs int64) (TopicConfig, error) {
	if retentionMs <= 0 {
		return TopicConfig{}, fmt.Errorf("retention_ms 必须 >0，得到 %d", retentionMs)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.topics[name]
	if !ok {
		return TopicConfig{}, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}
	tc.RetentionMs = retentionMs
	raw, _ := json.Marshal(tc)
	b := m.st.NewBatch()
	b.Set(store.TopicMetaKey(name), raw, nil)
	if err := m.st.Apply(b); err != nil {
		return TopicConfig{}, fmt.Errorf("持久化 topic %s: %w", name, err)
	}
	m.topics[name] = tc
	m.logger.Info("topic retention 已更新", "topic", name, "retention_ms", retentionMs)
	return tc, nil
}

// DeleteTopic 删除 topic 注册表条目。只删注册表——msg/keyidx/alloc 等数据清理
// 是 adminops.PurgeTopicData 的职责（本包边界：不管队列内容）。不存在返回
// ErrTopicNotFound，让 Admin API 能区分 404 与 500。
func (m *Meta) DeleteTopic(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.topics[name]; !ok {
		return fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}
	b := m.st.NewBatch()
	b.Delete(store.TopicMetaKey(name), nil)
	if err := m.st.Apply(b); err != nil {
		return fmt.Errorf("删除 topic %s: %w", name, err)
	}
	delete(m.topics, name)
	m.logger.Info("topic 已删除", "topic", name)
	return nil
}

// DeleteGroup 删除订阅组注册表条目（cursor/inflight 清理归 adminops.PurgeGroupData）。
func (m *Meta) DeleteGroup(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.groups[name]; !ok {
		return fmt.Errorf("%w: %s", ErrGroupNotFound, name)
	}
	b := m.st.NewBatch()
	b.Delete(store.GroupMetaKey(name), nil)
	if err := m.st.Apply(b); err != nil {
		return fmt.Errorf("删除 group %s: %w", name, err)
	}
	delete(m.groups, name)
	m.logger.Info("消费组已删除", "group", name)
	return nil
}
```

Run: `go test ./internal/core/meta/ -v` → PASS

- [ ] **Step 6: adminops 失败测试** `internal/core/adminops/adminops_test.go`

```go
// adminops 清理测试：写入真实数据后成片删除，验证目标区间清空且相邻数据无损。
package adminops

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

func fixture(t *testing.T) (*store.Store, *meta.Meta, *produce.Producer, *deliver.Deliverer) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(st, mt, slog.Default())
	return st, mt, pr, deliver.New(st, mt, pr, slog.Default())
}

func countPrefix(t *testing.T, st *store.Store, lower []byte) int {
	t.Helper()
	n := 0
	if err := st.Scan(lower, store.PrefixUpperBound(lower), 0, func(k, v []byte) (bool, error) {
		n++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPurgeTopicData(t *testing.T) {
	st, mt, pr, _ := fixture(t)
	for i := 0; i < 3; i++ {
		if _, err := pr.Append(&core.Message{Topic: "del-me", Body: []byte("x"), Keys: []string{"k1"}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pr.Append(&core.Message{Topic: "keep", Body: []byte("y"), Keys: []string{"k1"}}); err != nil {
		t.Fatal(err)
	}
	tc, _ := mt.GetTopic("del-me")
	if err := PurgeTopicData(st, tc); err != nil {
		t.Fatal(err)
	}
	if n := countPrefix(t, st, store.MsgQueuePrefix("del-me", 0)); n != 0 {
		t.Fatalf("msg/ 应清空，剩 %d", n)
	}
	if n := countPrefix(t, st, store.KeyIdxTopicPrefix("del-me")); n != 0 {
		t.Fatalf("keyidx/ 应清空，剩 %d", n)
	}
	if _, ok, _ := st.Get(store.AllocKey("del-me", 0)); ok {
		t.Fatal("alloc 计数器应删除")
	}
	// 相邻 topic 必须无损
	if n := countPrefix(t, st, store.MsgQueuePrefix("keep", 0)); n != 1 {
		t.Fatalf("keep topic 应剩 1 条，得到 %d", n)
	}
}

func TestPurgeGroupData(t *testing.T) {
	st, _, pr, dl := fixture(t)
	if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	// 真实消费一条：产生 cursor 与 inflight
	if _, err := dl.Receive(context.Background(), "g-del", "t1", 0, 1, time.Minute, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := dl.Receive(context.Background(), "g-keep", "t1", 0, 1, time.Minute, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := PurgeGroupData(st, "g-del"); err != nil {
		t.Fatal(err)
	}
	if n := countPrefix(t, st, store.CursorGroupPrefix("g-del")); n != 0 {
		t.Fatalf("g-del cursor 应清空，剩 %d", n)
	}
	if n := countPrefix(t, st, store.InflightGroupPrefix("g-del")); n != 0 {
		t.Fatalf("g-del inflight 应清空，剩 %d", n)
	}
	if n := countPrefix(t, st, store.CursorGroupPrefix("g-keep")); n != 1 {
		t.Fatalf("g-keep cursor 应无损，得到 %d", n)
	}
}
```

（注意 `dl.Receive` 第 7 参是 `wait time.Duration`，传 `0` 表示不等长轮询；第 8 参 filter 传 nil。）

Run: `go test ./internal/core/adminops/ -v` → FAIL（包不存在）

- [ ] **Step 7: adminops 实现** `internal/core/adminops/adminops.go`

```go
// Package adminops 提供管理面的成片数据清理（Admin API 删除类操作的 store 落地）。
//
// 职责：
//   - topic 删除后的 msg/alloc/keyidx 区间清理
//   - 订阅组删除后的 cursor/inflight 区间清理
//
// 边界：
//   - 不在消息热路径；不做并发防护——删除期间仍有生产/消费流量时的行为未定义
//     （alloc 计数器被删后并发 Append 会从 0 重新计数）。「删除前先停对应流量」
//     是运维契约，README 与 Admin API 文档写明，代码不为这种边界加锁
//   - 不动注册表（meta 的事）；调用顺序契约见各函数注释
package adminops

import (
	"fmt"
	"log/slog"

	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// PurgeTopicData 清理 topic 的全部消息数据：各队列 msg/ 区间、alloc 计数器、
// keyidx/ 索引，单批原子提交。
//
// 调用顺序契约：先 Purge 再 meta.DeleteTopic。崩溃在两步之间的中间态是
// 「注册表还在、数据已空」——等价于一个空 topic，无害且可重试；反过来会留下
// 永远没人清理的孤儿数据（注册表没了，retention 不再扫它）。
func PurgeTopicData(st *store.Store, tc meta.TopicConfig) error {
	b := st.NewBatch()
	for q := uint32(0); q < tc.Queues; q++ {
		mp := store.MsgQueuePrefix(tc.Name, q)
		b.DeleteRange(mp, store.PrefixUpperBound(mp), nil)
		b.Delete(store.AllocKey(tc.Name, q), nil)
	}
	kp := store.KeyIdxTopicPrefix(tc.Name)
	b.DeleteRange(kp, store.PrefixUpperBound(kp), nil)
	if err := st.Apply(b); err != nil {
		return fmt.Errorf("清理 topic %s 数据: %w", tc.Name, err)
	}
	slog.Default().Info("topic 数据已清理", "mod", "adminops", "topic", tc.Name, "queues", tc.Queues)
	return nil
}

// PurgeGroupData 清理订阅组的 cursor/ 与 inflight/ 全部记录，单批原子提交。
// 调用顺序契约与 PurgeTopicData 同理：先 Purge 再 meta.DeleteGroup。
func PurgeGroupData(st *store.Store, group string) error {
	b := st.NewBatch()
	cp := store.CursorGroupPrefix(group)
	b.DeleteRange(cp, store.PrefixUpperBound(cp), nil)
	ip := store.InflightGroupPrefix(group)
	b.DeleteRange(ip, store.PrefixUpperBound(ip), nil)
	if err := st.Apply(b); err != nil {
		return fmt.Errorf("清理 group %s 数据: %w", group, err)
	}
	slog.Default().Info("消费组数据已清理", "mod", "adminops", "group", group)
	return nil
}
```

Run: `go test ./internal/core/adminops/ -v` → PASS

- [ ] **Step 8: ResetCursor 失败测试** `internal/core/deliver/reset_test.go`（**新文件**；复用 deliver_test.go 的 `newFixture`/`f.send`，同包可见）

```go
// ResetCursor（Admin API 位点重置）测试。独立成文件：deliver_test.go 是 M4
// 并行改动区，本文件只新增不修改，避免合并冲突。
package deliver

import (
	"context"
	"testing"
	"time"
)

// TestResetCursorRewindsAndClearsInflight 重置到 0 后：inflight 清空、
// 消息从头重新投递、投递次数从 1 重新计（旧 inflight 已删，不是重投）。
func TestResetCursorRewindsAndClearsInflight(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t-reset", "m-0")
	f.send(t, "t-reset", "m-1")
	got, err := f.dl.Receive(context.Background(), "g", "t-reset", 0, 2, time.Minute, 0, nil)
	if err != nil || len(got) != 2 {
		t.Fatalf("首轮应收 2 条: %d %v", len(got), err)
	}
	// 未 ack 期间正常路径收不到任何东西（都在 inflight 里且未过期）
	if got, _ := f.dl.Receive(context.Background(), "g", "t-reset", 0, 2, time.Minute, 0, nil); len(got) != 0 {
		t.Fatalf("未过期不应重投，得到 %d 条", len(got))
	}
	if err := f.dl.ResetCursor("g", "t-reset", 0, 0); err != nil {
		t.Fatal(err)
	}
	got, err = f.dl.Receive(context.Background(), "g", "t-reset", 0, 2, time.Minute, 0, nil)
	if err != nil || len(got) != 2 {
		t.Fatalf("重置后应从头重新收 2 条: %d %v", len(got), err)
	}
	if string(got[0].Body) != "m-0" || got[0].DeliveryAttempt != 1 {
		t.Fatalf("重置后首条应为 m-0 且 attempt=1（inflight 已清空，属首投而非重投）: body=%s attempt=%d",
			got[0].Body, got[0].DeliveryAttempt)
	}
}

// TestResetCursorForwardSkips 向前重置 = 跳过消息（运维快进场景）。
func TestResetCursorForwardSkips(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t-skip", "m-0")
	f.send(t, "t-skip", "m-1")
	if err := f.dl.ResetCursor("g", "t-skip", 0, 1); err != nil {
		t.Fatal(err)
	}
	got, err := f.dl.Receive(context.Background(), "g", "t-skip", 0, 2, time.Minute, 0, nil)
	if err != nil || len(got) != 1 || string(got[0].Body) != "m-1" {
		t.Fatalf("快进后应只收 m-1: %v %v", got, err)
	}
}
```

Run: `go test ./internal/core/deliver/ -run TestResetCursor -v` → FAIL

- [ ] **Step 9: ResetCursor 实现**（**追加到 deliver.go 文件尾**，不改动任何既有函数）

```go
// ResetCursor 重置某队列的消费位点并清空该队列全部 inflight（Admin API 位点
// 重置）。offset 允许任意值：向后 = 重复消费（at-least-once 语义内），向前 =
// 跳过消息。
//
// 必须持队列锁执行：绕开锁直接写 store 会与 receiveOnce/Ack 的读改写竞态，
// 出现"重置后 cursor 又被并发投递推回去"的幽灵回退。清空 inflight 同样必须：
// 残留的 inflight 记录会被阶段 1 当作过期重投候选、又会在 Ack 位点推进时
// 参与空洞计算，与重置后的新 cursor 语义互相打架。
func (d *Deliverer) ResetCursor(group, topic string, queueID uint32, offset uint64) error {
	qmu := d.lockQueue(group, topic, queueID)
	qmu.Lock()
	defer qmu.Unlock()
	b := d.st.NewBatch()
	b.Set(store.CursorKey(group, topic, queueID), store.PutU64(offset), nil)
	ip := store.InflightPrefix(group, topic, queueID)
	b.DeleteRange(ip, store.PrefixUpperBound(ip), nil)
	if err := d.st.Apply(b); err != nil {
		return fmt.Errorf("重置位点 (group=%s topic=%s q=%d off=%d): %w", group, topic, queueID, offset, err)
	}
	d.logger.Info("消费位点已重置", "group", group, "topic", topic, "queue", queueID, "offset", offset)
	return nil
}
```

（`pebble.Batch.DeleteRange` 与 `store.PutU64` 均为既有依赖，deliver.go 已 import。若 M4 合并后 deliver.go 出现冲突，本函数整体是文件尾追加块，保留双方即可。）

Run: `go test ./internal/core/deliver/ -v` → PASS（既有用例 + 新用例）

- [ ] **Step 10: 日志与注释自检 + 提交**

自检（instrumenting-code 清单）：meta 增删改路径有 Info 日志、adminops 两函数成功路径有 Info、ResetCursor 有 Info、全部错误分支带上下文包装；新文件 adminops.go/reset_test.go 有文件头职责边界注释；全部导出方法有文档注释。

```bash
gofmt -l . && go vet ./... && go test ./...
git add internal/store/ internal/core/meta/ internal/core/adminops/ internal/core/deliver/
git commit -m "feat(core): 管理面基础——meta 增删改、位点重置、topic/group 数据清理"
```

---

### Task 4: Prometheus metrics（抓取期 Collector + fsync 直方图）

**Files:**
- Modify: `internal/store/store.go`（Apply 耗时钩子，M4 不碰此文件）
- Create: `internal/metrics/stats.go`、`internal/metrics/collector.go`
- Test: `internal/metrics/metrics_test.go`

**Interfaces:**
- Consumes: Task 3 的 `store.CursorPrefix`/`ParseCursorKey`/`InflightAllPrefix`、`meta.Groups()`；既有 `store.AllocKey`/`GetU64`/`DelayPrefix`/`ParseInflightKey`、`meta.Topics()`
- Produces:
  - `store.OnApplyObserve var func(d time.Duration)`（包级钩子）
  - `metrics.Collect(st *store.Store, mt *meta.Meta) (*Stats, error)`，`type Stats struct { Topics, Groups, DelayDepth int; Written map[string]uint64; Pending map[GroupTopic]uint64; Inflight map[GroupTopic]int }`，`type GroupTopic struct{ Group, Topic string }`
  - `metrics.NewRegistry(st *store.Store, mt *meta.Meta, logger *slog.Logger) *prometheus.Registry`（Task 5 的 admin HTTP 挂 `/metrics` 用；Task 7 overview 复用 `Collect`）

**设计（为什么是抓取期计算）**：QPS/堆积类指标不在热路径埋计数器，而是抓取时从 store 状态推导——`alloc/` 的下一 offset 就是每队列累计写入量（天然单调 counter，Prometheus `rate()` 直接得写入 QPS）；`cursor` 与 alloc 的差是待拉取堆积；`inflight/` 计数即已投未确认。这样 M5 与 M4 的热路径改动**零交集**，且指标天然崩溃一致（状态即指标）。代价：每次抓取全量扫 cursor/inflight/delay 前缀，目标量级（5k msg/s、单机）下毫秒级，可接受；边界写入包注释。

- [ ] **Step 1: 依赖**

```bash
go get github.com/prometheus/client_golang@latest
```

- [ ] **Step 2: 失败测试** `internal/metrics/metrics_test.go`

```go
// metrics 测试：走真实 produce/deliver 制造状态，断言 Collect 推导值与
// Prometheus 文本输出。
package metrics

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

func fixture(t *testing.T) (*store.Store, *meta.Meta, *produce.Producer, *deliver.Deliverer) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(st, mt, slog.Default())
	return st, mt, pr, deliver.New(st, mt, pr, slog.Default())
}

func TestCollectDerivesStats(t *testing.T) {
	st, mt, pr, dl := fixture(t)
	for i := 0; i < 3; i++ {
		if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	// 消费 1 条不 ack：cursor=1、inflight=1、待拉取=2
	if _, err := dl.Receive(context.Background(), "g1", "t1", 0, 1, time.Minute, 0, nil); err != nil {
		t.Fatal(err)
	}
	s, err := Collect(st, mt)
	if err != nil {
		t.Fatal(err)
	}
	if s.Topics != 1 || s.Groups != 1 {
		t.Fatalf("topics/groups 不符: %+v", s)
	}
	if s.Written["t1"] != 3 {
		t.Fatalf("t1 写入总量应为 3: %v", s.Written)
	}
	gt := GroupTopic{Group: "g1", Topic: "t1"}
	if s.Pending[gt] != 2 {
		t.Fatalf("待拉取应为 2: %v", s.Pending)
	}
	if s.Inflight[gt] != 1 {
		t.Fatalf("inflight 应为 1: %v", s.Inflight)
	}
	if s.DelayDepth != 0 {
		t.Fatalf("delay 深度应为 0: %d", s.DelayDepth)
	}
}

func TestRegistryExposesMetrics(t *testing.T) {
	st, mt, pr, _ := fixture(t)
	if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry(st, mt, slog.Default())
	// Append 走过 store.Apply，直方图应已有样本；写入 counter 应为 1
	got, err := testutil.GatherAndCount(reg, "sq_topic_messages_written_total", "sq_store_apply_duration_seconds")
	if err != nil || got == 0 {
		t.Fatalf("指标缺失: n=%d err=%v", got, err)
	}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP sq_topic_messages_written_total topic 累计写入消息条数（各队列 offset 计数器之和）
# TYPE sq_topic_messages_written_total counter
sq_topic_messages_written_total{topic="t1"} 1
`), "sq_topic_messages_written_total"); err != nil {
		t.Fatalf("written_total 输出不符: %v", err)
	}
}
```

注意：`NewRegistry` 会设置包级钩子 `store.OnApplyObserve`，测试并行跑会互相覆盖——`metrics_test.go` 内不要用 `t.Parallel()`，并在测试注释里写明原因。HELP 文案必须与实现里的 Help 字符串逐字一致（GatherAndCompare 全文比对）。

Run: `go test ./internal/metrics/ -v` → FAIL（包不存在）

- [ ] **Step 3: store 钩子实现**（store.go）

`Store` 上方追加：

```go
// OnApplyObserve 若非 nil，每次 Apply 成功提交后以提交耗时（含 fsync）回调，
// 供 metrics 装配 fsync 延迟直方图（spec §8）。契约：进程装配阶段设置一次，
// 服务启动后只读——据此不加锁；运行期改写属数据竞态，禁止。
var OnApplyObserve func(d time.Duration)
```

`Apply` 的 Commit 段改为：

```go
	start := time.Now()
	if err := b.Commit(opt); err != nil {
		return fmt.Errorf("store Apply: %w", err)
	}
	if OnApplyObserve != nil {
		// 只观测成功提交：失败路径由调用方带上下文记日志，混进直方图反而
		// 会把错误重试的耗时污染进正常刷盘分布。
		OnApplyObserve(time.Since(start))
	}
```

（import 增加 `"time"`。）

- [ ] **Step 4: stats.go 实现**

```go
// stats.go: 从 store 状态推导运行指标（Collector 与 Admin overview 共用）。
//
// 职责：
//   - 单趟扫描推导：topic 写入总量（alloc 计数器）、各组待拉取堆积
//     （alloc-cursor 差值）、inflight 计数、延时队列深度
//
// 边界：
//   - 全量扫 cursor/inflight/delay 前缀，代价与这三类记录条数成线性——目标量级
//     （单机、5k msg/s）毫秒级；不适合超大 inflight/延时积压场景的高频抓取
//   - 没有 cursor 记录的 (group, topic) 不出现在 Pending 里：组从未拉取过就
//     没有"它视角的堆积"可言（要看总量有 Written）
package metrics

import (
	"fmt"

	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// GroupTopic 组×topic 二元标签键。
type GroupTopic struct{ Group, Topic string }

// Stats 一次抓取推导出的全部指标值。
type Stats struct {
	Topics     int
	Groups     int
	DelayDepth int
	Written    map[string]uint64     // topic → 累计写入条数
	Pending    map[GroupTopic]uint64 // 已写入未拉取
	Inflight   map[GroupTopic]int    // 已投递未确认
}

// Collect 扫描 store 推导当前指标快照。
func Collect(st *store.Store, mt *meta.Meta) (*Stats, error) {
	s := &Stats{
		Written:  map[string]uint64{},
		Pending:  map[GroupTopic]uint64{},
		Inflight: map[GroupTopic]int{},
	}
	topics := mt.Topics()
	s.Topics = len(topics)
	s.Groups = len(mt.Groups())
	// 每队列下一 offset（= 累计写入量）；cursor 差值计算复用同一份
	next := map[string]map[uint32]uint64{}
	for _, tc := range topics {
		qn := map[uint32]uint64{}
		var sum uint64
		for q := uint32(0); q < tc.Queues; q++ {
			raw, ok, err := st.Get(store.AllocKey(tc.Name, q))
			if err != nil {
				return nil, fmt.Errorf("读 alloc (%s/%d): %w", tc.Name, q, err)
			}
			if ok {
				qn[q] = store.GetU64(raw)
			}
			sum += qn[q]
		}
		next[tc.Name] = qn
		s.Written[tc.Name] = sum
	}
	cp := store.CursorPrefix()
	err := st.Scan(cp, store.PrefixUpperBound(cp), 0, func(k, v []byte) (bool, error) {
		g, tp, q, perr := store.ParseCursorKey(k)
		if perr != nil {
			return false, perr
		}
		cur := store.GetU64(v)
		if n, ok := next[tp][q]; ok && n > cur {
			s.Pending[GroupTopic{g, tp}] += n - cur
		}
		// n <= cur：topic 被删后重建（alloc 归零）或纯粹已消费完，都按 0 堆积处理
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描 cursor: %w", err)
	}
	ip := store.InflightAllPrefix()
	err = st.Scan(ip, store.PrefixUpperBound(ip), 0, func(k, v []byte) (bool, error) {
		g, tp, _, _, perr := store.ParseInflightKey(k)
		if perr != nil {
			return false, perr
		}
		s.Inflight[GroupTopic{g, tp}]++
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描 inflight: %w", err)
	}
	dp := []byte(store.DelayPrefix)
	err = st.Scan(dp, store.PrefixUpperBound(dp), 0, func(k, v []byte) (bool, error) {
		s.DelayDepth++
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描 delay: %w", err)
	}
	return s, nil
}
```

- [ ] **Step 5: collector.go 实现**

```go
// collector.go: 把 Collect 的快照适配成 Prometheus Collector，并组装进程级 Registry。
//
// 职责：
//   - 抓取时调用 Collect，翻译为带标签的 gauge/counter
//   - NewRegistry 一站式装配：Go/process 采集器、状态 Collector、
//     store.Apply 耗时直方图（挂接包级钩子）
//
// 边界：
//   - NewRegistry 写 store.OnApplyObserve 包级钩子，进程内只可调用一次
//     （装配期契约，见 store 侧注释）
package metrics

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

var (
	descTopics = prometheus.NewDesc("sq_topics", "已注册 topic 数", nil, nil)
	descGroups = prometheus.NewDesc("sq_groups", "已注册消费组数", nil, nil)
	descDelay  = prometheus.NewDesc("sq_delay_depth", "延时暂存区未到期条数", nil, nil)
	descWrite  = prometheus.NewDesc("sq_topic_messages_written_total",
		"topic 累计写入消息条数（各队列 offset 计数器之和）", []string{"topic"}, nil)
	descPending = prometheus.NewDesc("sq_group_pending_messages",
		"消费组视角已写入未拉取的消息数", []string{"group", "topic"}, nil)
	descInflight = prometheus.NewDesc("sq_group_inflight_messages",
		"已投递未确认的消息数", []string{"group", "topic"}, nil)
)

// Collector 抓取期状态采集器。
type Collector struct {
	st     *store.Store
	mt     *meta.Meta
	logger *slog.Logger
}

// NewCollector 构造状态 Collector。
func NewCollector(st *store.Store, mt *meta.Meta, logger *slog.Logger) *Collector {
	return &Collector{st: st, mt: mt, logger: logger.With("mod", "metrics")}
}

// Describe 实现 prometheus.Collector。
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descTopics
	ch <- descGroups
	ch <- descDelay
	ch <- descWrite
	ch <- descPending
	ch <- descInflight
}

// Collect 实现 prometheus.Collector：每次抓取现算。失败时上报 invalid metric
// （抓取端能看到错误）并记 Error 日志，不让一次 store 故障 panic 掉抓取协程。
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	s, err := Collect(c.st, c.mt)
	if err != nil {
		c.logger.Error("metrics 采集失败", "err", err)
		ch <- prometheus.NewInvalidMetric(descTopics, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(descTopics, prometheus.GaugeValue, float64(s.Topics))
	ch <- prometheus.MustNewConstMetric(descGroups, prometheus.GaugeValue, float64(s.Groups))
	ch <- prometheus.MustNewConstMetric(descDelay, prometheus.GaugeValue, float64(s.DelayDepth))
	for topic, n := range s.Written {
		ch <- prometheus.MustNewConstMetric(descWrite, prometheus.CounterValue, float64(n), topic)
	}
	for gt, n := range s.Pending {
		ch <- prometheus.MustNewConstMetric(descPending, prometheus.GaugeValue, float64(n), gt.Group, gt.Topic)
	}
	for gt, n := range s.Inflight {
		ch <- prometheus.MustNewConstMetric(descInflight, prometheus.GaugeValue, float64(n), gt.Group, gt.Topic)
	}
}

// NewRegistry 组装进程级指标注册表并挂接 store.Apply 耗时直方图。
// 只能在装配阶段调用一次（会写 store.OnApplyObserve 包级钩子）。
func NewRegistry(st *store.Store, mt *meta.Meta, logger *slog.Logger) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(NewCollector(st, mt, logger))
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "sq_store_apply_duration_seconds",
		Help: "store 批次提交耗时（含 fsync）",
		// 0.1ms 起倍增 14 档（~1.6s 封顶）：同步刷盘常态在 0.5~10ms，
		// 尾部预算给磁盘抖动
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
	})
	reg.MustRegister(h)
	store.OnApplyObserve = func(d time.Duration) { h.Observe(d.Seconds()) }
	logger.Info("metrics registry 已装配", "mod", "metrics")
	return reg
}
```

- [ ] **Step 6: 跑测试确认通过**

Run: `go test ./internal/metrics/ ./internal/store/ -v`
Expected: PASS（含 store 既有用例——钩子为 nil 时行为不变）

- [ ] **Step 7: gofmt/vet + 全量测试 + 提交**

```bash
gofmt -l . && go vet ./... && go test ./...
git add go.mod go.sum internal/store/store.go internal/metrics/
git commit -m "feat(metrics): 抓取期状态 Collector 与 store 提交耗时直方图"
```

---

### Task 5: Admin HTTP 骨架 + 用户名密码登录

**Files:**
- Create: `internal/admin/server.go`、`internal/admin/auth.go`、`internal/admin/http.go`
- Test: `internal/admin/server_test.go`
- Modify: `cmd/sq/main.go`（admin HTTP 起停接线）

**Interfaces:**
- Consumes: Task 4 的 `metrics.NewRegistry`；Task 1 的 `cfg.AdminListen`/`AdminUsername`/`AdminPassword`
- Produces:
  - `admin.New(st *store.Store, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer, username, password string, writeBlocked *atomic.Bool, reg *prometheus.Registry, logger *slog.Logger) *Server`
  - `(*Server) Handler() http.Handler`（测试用）、`(*Server) Serve(ln net.Listener) error`、`(*Server) Shutdown(ctx context.Context) error`
  - 包内工具（Task 6/7 的 handler 用）：`writeJSON(w http.ResponseWriter, code int, v any)`、`httpError(w http.ResponseWriter, code int, format string, args ...any)`、`decodeJSON(w, r, v) bool`、`(*Server) protected(h http.HandlerFunc) http.HandlerFunc`
  - 路由注册集中在 `(*Server) routes(reg *prometheus.Registry)`——Task 6/7 往这里加行

**认证设计**：`POST /admin/login` 提交 `{"username","password"}`，常数时间比对，通过则发 32 字节随机 hex token（内存表，TTL 24h，进程重启失效——单机管理面可接受，边界写明）。其余 `/admin/*` 路由经 `protected` 中间件要求 `Authorization: Bearer <token>`。用户名密码均未配置（默认）= 免登录直通（spec「默认关闭」）。`/metrics` 不设防——Prometheus 抓取器不会走登录流程，端口暴露范围由部署侧控制（README 写明）。

- [ ] **Step 1: 失败测试** `internal/admin/server_test.go`

```go
// Admin HTTP 骨架测试：登录流程、token 门禁、免登录直通。
// 用 httptest + 真实 store/meta/produce/deliver fixture（不 mock core）。
package admin

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/metrics"
	"github.com/xushixin/sq/internal/store"
)

// newTestServer 构造 admin Server 与其依赖。user/pass 均空 = 免登录。
func newTestServer(t *testing.T, user, pass string) (*Server, *store.Store, *meta.Meta, *produce.Producer, *deliver.Deliverer) {
	t.Helper()
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(st, mt, slog.Default())
	dl := deliver.New(st, mt, pr, slog.Default())
	s := New(st, mt, pr, dl, user, pass, &atomic.Bool{}, metrics.NewRegistry(st, mt, slog.Default()), slog.Default())
	return s, st, mt, pr, dl
}

// doJSON 发 JSON 请求，返回响应记录器。token 非空时带 Bearer 头。
func doJSON(t *testing.T, h http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestLoginAndTokenGate(t *testing.T) {
	s, _, _, _, _ := newTestServer(t, "root", "pw123")
	h := s.Handler()
	// 未带 token 访问受保护路由 → 401
	if w := doJSON(t, h, "GET", "/admin/topics", "", nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("无 token 应 401，得到 %d", w.Code)
	}
	// 错密码 → 401
	if w := doJSON(t, h, "POST", "/admin/login", "", map[string]string{"username": "root", "password": "bad"}); w.Code != http.StatusUnauthorized {
		t.Fatalf("错密码应 401，得到 %d", w.Code)
	}
	// 正确登录 → token
	w := doJSON(t, h, "POST", "/admin/login", "", map[string]string{"username": "root", "password": "pw123"})
	if w.Code != http.StatusOK {
		t.Fatalf("登录应 200，得到 %d body=%s", w.Code, w.Body)
	}
	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Token == "" {
		t.Fatalf("应返回 token: %s %v", w.Body, err)
	}
	// 带 token → 放行
	if w := doJSON(t, h, "GET", "/admin/topics", resp.Token, nil); w.Code != http.StatusOK {
		t.Fatalf("带 token 应 200，得到 %d body=%s", w.Code, w.Body)
	}
	// 伪 token → 401
	if w := doJSON(t, h, "GET", "/admin/topics", "deadbeef", nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("伪 token 应 401，得到 %d", w.Code)
	}
}

func TestNoAuthConfiguredPassthrough(t *testing.T) {
	s, _, _, _, _ := newTestServer(t, "", "")
	h := s.Handler()
	if w := doJSON(t, h, "GET", "/admin/topics", "", nil); w.Code != http.StatusOK {
		t.Fatalf("免登录模式应直通，得到 %d", w.Code)
	}
	// 免登录时 login 端点明确告知无需登录
	if w := doJSON(t, h, "POST", "/admin/login", "", map[string]string{"username": "x", "password": "y"}); w.Code != http.StatusBadRequest {
		t.Fatalf("免登录模式 login 应 400，得到 %d", w.Code)
	}
}

func TestMetricsEndpointOpen(t *testing.T) {
	s, _, _, _, _ := newTestServer(t, "root", "pw123")
	// /metrics 不设防（Prometheus 抓取器无登录流程）
	if w := doJSON(t, s.Handler(), "GET", "/metrics", "", nil); w.Code != http.StatusOK {
		t.Fatalf("/metrics 应 200，得到 %d", w.Code)
	}
}
```

（`GET /admin/topics` 在 Task 6 才有实现——本 task 先注册一个返回 `[]` 的占位版本？**不**：占位是计划红旗。做法：Task 5 直接实现 topics 列表只读 handler（它只有 `mt.Topics()` 一行数据逻辑，归属骨架并不牵强），Task 6 实现其余资源路由。）

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/admin/ -v`
Expected: FAIL（包不存在）

- [ ] **Step 3: 实现 http.go（JSON 工具）**

```go
// http.go: Admin API 的 JSON 读写工具与统一错误形状。
//
// 职责：
//   - writeJSON/httpError/decodeJSON 三个包内工具，全部 handler 共用
//
// 边界：
//   - 错误响应统一 {"error": "..."}；不做 i18n
package admin

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
)

// writeJSON 写 JSON 响应。编码失败已无法挽回响应（头已发），只记日志。
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Default().Error("admin 响应编码失败", "mod", "admin", "err", err)
	}
}

// httpError 写统一错误形状 {"error": "..."}。
func httpError(w http.ResponseWriter, code int, format string, args ...any) {
	writeJSON(w, code, map[string]string{"error": fmt.Sprintf(format, args...)})
}

// decodeJSON 解析请求体到 v，失败时已写 400 响应并返回 false。
// 1MB 上限：管理面请求（登录、建 topic、测试消息）都远小于此，挡住误传大文件。
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		httpError(w, http.StatusBadRequest, "请求体解析失败: %v", err)
		return false
	}
	return true
}
```

- [ ] **Step 4: 实现 auth.go（登录与门禁）**

```go
// auth.go: Admin API 登录与 token 门禁（用户名密码来自配置文件，spec §6
// 「控制台独立简单密码登录」的服务端半边）。
//
// 职责：
//   - POST /admin/login 校验用户名密码、签发随机 token（内存表，TTL 24h）
//   - protected 中间件校验 Bearer token；未配置用户名密码时直通
//
// 边界：
//   - token 表在内存：进程重启全部失效，重新登录即可（单机管理面的刻意取舍）
//   - 不做多用户/权限分级；不做登录限速（部署侧用防火墙圈住 admin 端口）
package admin

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

// tokenTTL 登录 token 有效期。24h：覆盖一个工作日，过期重登成本可忽略。
const tokenTTL = 24 * time.Hour

// handleLogin POST /admin/login。两个比较都走常数时间且不短路，
// 不泄露"用户名对不对"这一位信息。
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.username == "" {
		httpError(w, http.StatusBadRequest, "服务端未配置登录，无需认证")
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	userOK := subtle.ConstantTimeCompare([]byte(req.Username), []byte(s.username)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(req.Password), []byte(s.password)) == 1
	if !userOK || !passOK {
		s.logger.Warn("admin 登录失败", "username", req.Username, "remote", r.RemoteAddr)
		httpError(w, http.StatusUnauthorized, "用户名或密码错误")
		return
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		s.logger.Error("生成登录 token 失败", "err", err)
		httpError(w, http.StatusInternalServerError, "生成 token 失败")
		return
	}
	token := hex.EncodeToString(buf)
	s.tokens.Store(token, time.Now().Add(tokenTTL))
	s.logger.Info("admin 登录成功", "username", req.Username, "remote", r.RemoteAddr)
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

// protected 包装需要登录的 handler。未配置用户名密码 = 免登录直通（默认关闭语义）。
func (s *Server) protected(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.username == "" {
			h(w, r)
			return
		}
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			httpError(w, http.StatusUnauthorized, "缺少 Bearer token，请先 POST /admin/login")
			return
		}
		exp, found := s.tokens.Load(token)
		if !found {
			httpError(w, http.StatusUnauthorized, "token 无效")
			return
		}
		if time.Now().After(exp.(time.Time)) {
			// 惰性清理：过期 token 在下次被使用时删除，无后台清扫协程——
			// token 量级 = 登录次数，单机管理面不会累积成内存问题
			s.tokens.Delete(token)
			httpError(w, http.StatusUnauthorized, "token 已过期，请重新登录")
			return
		}
		h(w, r)
	}
}
```

- [ ] **Step 5: 实现 server.go（骨架 + topics 列表只读路由）**

```go
// Package admin 提供 HTTP Admin API（spec §9 的后端；React 控制台 M5b 只消费
// 本 API，无私有通道）。
//
// 职责：
//   - 路由装配、登录门禁、/metrics 暴露
//   - 资源 handler：topic/消费组管理、消息查询、DLQ 重发、延时视图、测试消息
//
// 边界：
//   - 只组合 core 导出 API 与 store 只读扫描；消息语义（投递/重试/顺序）不在此层
//   - /metrics 不设防（抓取器无登录流程），端口暴露范围由部署侧控制
//   - 删除类操作不与在线流量互斥（adminops 的运维契约：先停流量再删）
package admin

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/store"
)

// Server Admin HTTP 服务。
type Server struct {
	st           *store.Store
	mt           *meta.Meta
	pr           *produce.Producer
	dl           *deliver.Deliverer
	username     string
	password     string
	writeBlocked *atomic.Bool
	logger       *slog.Logger

	tokens sync.Map // token(string) → 过期时间(time.Time)
	mux    *http.ServeMux
	hs     *http.Server
}

// New 构造 Admin 服务并装配全部路由。username/password 均空 = 免登录。
func New(st *store.Store, mt *meta.Meta, pr *produce.Producer, dl *deliver.Deliverer,
	username, password string, writeBlocked *atomic.Bool,
	reg *prometheus.Registry, logger *slog.Logger) *Server {
	s := &Server{
		st: st, mt: mt, pr: pr, dl: dl,
		username: username, password: password, writeBlocked: writeBlocked,
		logger: logger.With("mod", "admin"),
		mux:    http.NewServeMux(),
	}
	s.routes(reg)
	return s
}

// routes 注册全部路由。后续资源 handler（groups/messages/dlq/delay/overview）
// 在各自实现的 task 里往这里追加。
func (s *Server) routes(reg *prometheus.Registry) {
	s.mux.HandleFunc("POST /admin/login", s.handleLogin)
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	s.mux.HandleFunc("GET /admin/topics", s.protected(s.handleTopicsList))
}

// Handler 返回根 handler（测试注入用）。
func (s *Server) Handler() http.Handler { return s.mux }

// Serve 在 ln 上阻塞服务（调用方放入 goroutine）。Shutdown 后返回 http.ErrServerClosed。
func (s *Server) Serve(ln net.Listener) error {
	s.hs = &http.Server{Handler: s.mux}
	s.logger.Info("admin HTTP 服务中", "addr", ln.Addr().String())
	return s.hs.Serve(ln)
}

// Shutdown 优雅停止（等在途请求，受 ctx 限时）。Serve 未调用过时为空操作。
func (s *Server) Shutdown(ctx context.Context) error {
	if s.hs == nil {
		return nil
	}
	s.logger.Info("admin HTTP 停机")
	return s.hs.Shutdown(ctx)
}

// topicJSON topic 的 API 序列化形状（列表与详情共用）。
type topicJSON struct {
	Name        string `json:"name"`
	Queues      uint32 `json:"queues"`
	RetentionMs int64  `json:"retention_ms"`
	CreatedAtMs int64  `json:"created_at_ms"`
}

// handleTopicsList GET /admin/topics：全部 topic 按名字序。
func (s *Server) handleTopicsList(w http.ResponseWriter, r *http.Request) {
	tcs := s.mt.Topics()
	sort.Slice(tcs, func(i, j int) bool { return tcs[i].Name < tcs[j].Name })
	out := make([]topicJSON, 0, len(tcs))
	for _, tc := range tcs {
		out = append(out, topicJSON{Name: tc.Name, Queues: tc.Queues,
			RetentionMs: tc.RetentionMs, CreatedAtMs: tc.CreatedAtMs})
	}
	writeJSON(w, http.StatusOK, out)
}
```

- [ ] **Step 6: 跑测试确认通过**

Run: `go test ./internal/admin/ -v`
Expected: PASS

- [ ] **Step 7: main.go 接线**

在 delay 调度器块之后、`net.Listen`（gRPC）之前插入（顺序有意：admin 端口先绑定，e2e 以 gRPC 就绪为信号时 admin 必已可用）：

```go
	// Admin HTTP（含 /metrics）。admin_listen 为空 = 关闭。停机顺序：本 defer
	// 注册在 st.Close 的 defer 之后（LIFO 先执行），保证 handler 不会在 store
	// 关闭后还在读写它。
	if cfg.AdminListen != "" {
		reg := metrics.NewRegistry(st, mt, logger)
		adm := admin.New(st, mt, pr, dl, cfg.AdminUsername, cfg.AdminPassword, writeBlocked, reg, logger)
		aln, err := net.Listen("tcp", cfg.AdminListen)
		if err != nil {
			return fmt.Errorf("admin HTTP 监听 %s: %w", cfg.AdminListen, err)
		}
		go func() {
			// 运行期 Serve 异常只记日志不退进程：admin 是辅助面，它挂掉不该
			// 连累消息主链路；启动期端口占用则已在上面 fail-fast
			if err := adm.Serve(aln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("admin HTTP 异常退出", "err", err)
			}
		}()
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := adm.Shutdown(sctx); err != nil {
				logger.Warn("admin HTTP 停机超时", "err", err)
			}
		}()
		logger.Info("admin HTTP 已启动", "listen", cfg.AdminListen,
			"login_required", cfg.AdminUsername != "")
	}
```

import 追加：`"fmt"`、`"net/http"`、`"github.com/xushixin/sq/internal/admin"`、`"github.com/xushixin/sq/internal/metrics"`（`errors` 若未引入也追加）。

- [ ] **Step 8: 编译 + 全量测试 + gofmt/vet + 提交**

```bash
go build ./... && go test ./... && gofmt -l . && go vet ./...
git add internal/admin/ cmd/sq/main.go
git commit -m "feat(admin): HTTP Admin API 骨架——登录 token 门禁、/metrics、topic 列表"
```

---

### Task 6: Admin API — topic 管理与消费组

**Files:**
- Create: `internal/admin/topics.go`、`internal/admin/groups.go`
- Test: `internal/admin/topics_test.go`、`internal/admin/groups_test.go`
- Modify: `internal/admin/server.go`（routes 追加；handleTopicsList 移入 topics.go 亦可，二选一后保持一致）

**Interfaces:**
- Consumes: Task 3 全部产物（meta 增删改、adminops、ResetCursor、CursorGroupPrefix/ParseCursorKey）；Task 5 的 writeJSON/httpError/decodeJSON/protected；`config.MaxDefaultQueueNums`
- Produces: REST 端点（M5b 控制台的契约）：
  - `POST /admin/topics` `{"name","queues","retention_ms"?}` → 201 topicJSON | 400 | 409
  - `GET /admin/topics/{name}` → topicJSON + `queues_detail: [{queue_id, next_offset}]`
  - `PATCH /admin/topics/{name}` `{"retention_ms"}` → 200
  - `DELETE /admin/topics/{name}` → 204（先 Purge 后删注册表）
  - `GET /admin/groups` → `[{name, max_attempts, created_at_ms}]`
  - `GET /admin/groups/{name}` → `{name, max_attempts, topics: [{topic, queues: [{queue_id, cursor, next_offset, pending, inflight}]}]}`
  - `POST /admin/groups/{name}/reset-cursor` `{"topic","queue_id","offset"}` → 204
  - `DELETE /admin/groups/{name}` → 204

- [ ] **Step 1: routes 追加**（server.go 的 routes 函数）

```go
	s.mux.HandleFunc("POST /admin/topics", s.protected(s.handleTopicCreate))
	s.mux.HandleFunc("GET /admin/topics/{name}", s.protected(s.handleTopicGet))
	s.mux.HandleFunc("PATCH /admin/topics/{name}", s.protected(s.handleTopicPatch))
	s.mux.HandleFunc("DELETE /admin/topics/{name}", s.protected(s.handleTopicDelete))
	s.mux.HandleFunc("GET /admin/groups", s.protected(s.handleGroupsList))
	s.mux.HandleFunc("GET /admin/groups/{name}", s.protected(s.handleGroupGet))
	s.mux.HandleFunc("POST /admin/groups/{name}/reset-cursor", s.protected(s.handleGroupResetCursor))
	s.mux.HandleFunc("DELETE /admin/groups/{name}", s.protected(s.handleGroupDelete))
```

- [ ] **Step 2: 失败测试** `internal/admin/topics_test.go`

```go
// topic 管理端点测试（fixture 与 doJSON 复用 server_test.go，同包可见）。
package admin

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/store"
)

func TestTopicCRUD(t *testing.T) {
	s, st, mt, pr, _ := newTestServer(t, "", "")
	h := s.Handler()
	// 创建
	w := doJSON(t, h, "POST", "/admin/topics", "", map[string]any{"name": "t1", "queues": 2, "retention_ms": 60000})
	if w.Code != http.StatusCreated {
		t.Fatalf("创建应 201，得到 %d body=%s", w.Code, w.Body)
	}
	// 重复创建 → 409
	if w := doJSON(t, h, "POST", "/admin/topics", "", map[string]any{"name": "t1", "queues": 2}); w.Code != http.StatusConflict {
		t.Fatalf("重复创建应 409，得到 %d", w.Code)
	}
	// 非法参数
	if w := doJSON(t, h, "POST", "/admin/topics", "", map[string]any{"name": "坏名字", "queues": 2}); w.Code != http.StatusBadRequest {
		t.Fatalf("非法名字应 400，得到 %d", w.Code)
	}
	if w := doJSON(t, h, "POST", "/admin/topics", "", map[string]any{"name": "t2", "queues": 0}); w.Code != http.StatusBadRequest {
		t.Fatalf("queues=0 应 400，得到 %d", w.Code)
	}
	// 详情含每队列 next_offset
	if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	w = doJSON(t, h, "GET", "/admin/topics/t1", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("详情应 200，得到 %d", w.Code)
	}
	var detail struct {
		Name         string `json:"name"`
		RetentionMs  int64  `json:"retention_ms"`
		QueuesDetail []struct {
			QueueID    uint32 `json:"queue_id"`
			NextOffset uint64 `json:"next_offset"`
		} `json:"queues_detail"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.RetentionMs != 60000 || len(detail.QueuesDetail) != 2 {
		t.Fatalf("详情不符: %+v", detail)
	}
	if detail.QueuesDetail[0].NextOffset+detail.QueuesDetail[1].NextOffset != 1 {
		t.Fatalf("写入 1 条后 next_offset 之和应为 1: %+v", detail.QueuesDetail)
	}
	// PATCH retention
	if w := doJSON(t, h, "PATCH", "/admin/topics/t1", "", map[string]any{"retention_ms": 120000}); w.Code != http.StatusOK {
		t.Fatalf("PATCH 应 200，得到 %d body=%s", w.Code, w.Body)
	}
	if tc, _ := mt.GetTopic("t1"); tc.RetentionMs != 120000 {
		t.Fatalf("retention 未更新: %+v", tc)
	}
	// 删除：注册表与数据都要消失
	if w := doJSON(t, h, "DELETE", "/admin/topics/t1", "", nil); w.Code != http.StatusNoContent {
		t.Fatalf("删除应 204，得到 %d", w.Code)
	}
	if _, ok := mt.GetTopic("t1"); ok {
		t.Fatal("注册表应已删除")
	}
	n := 0
	if err := st.Scan(store.MsgQueuePrefix("t1", 0), store.PrefixUpperBound(store.MsgQueuePrefix("t1", 0)), 0,
		func(k, v []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("消息数据应已清理，剩 %d", n)
	}
	// 删不存在的 → 404
	if w := doJSON(t, h, "DELETE", "/admin/topics/t1", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("重复删除应 404，得到 %d", w.Code)
	}
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `go test ./internal/admin/ -run TestTopicCRUD -v` → FAIL

- [ ] **Step 4: 实现 topics.go**（handleTopicsList 从 server.go 移到这里，topicJSON 随之移动）

```go
// topics.go: topic 管理端点（列表/创建/详情/改 retention/删除）。
//
// 职责：REST 参数校验 + 翻译为 meta/adminops 调用；HTTP 状态码语义见各 handler
// 边界：删除操作的「先停流量」运维契约见 adminops 包注释
package admin

import (
	"errors"
	"net/http"
	"sort"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/adminops"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// handleTopicCreate POST /admin/topics。409 语义：CreateTopic 本身幂等返回旧配置，
// 管理面必须显式区分"新建成功"与"早已存在"，否则控制台上建错名字无感知。
func (s *Server) handleTopicCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string `json:"name"`
		Queues      uint32 `json:"queues"`
		RetentionMs int64  `json:"retention_ms"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Queues < 1 || req.Queues > config.MaxDefaultQueueNums {
		httpError(w, http.StatusBadRequest, "queues 必须在 1..%d，得到 %d", config.MaxDefaultQueueNums, req.Queues)
		return
	}
	if _, ok := s.mt.GetTopic(req.Name); ok {
		httpError(w, http.StatusConflict, "topic %s 已存在", req.Name)
		return
	}
	tc, err := s.mt.CreateTopic(req.Name, req.Queues)
	if err != nil {
		if errors.Is(err, meta.ErrBadName) {
			httpError(w, http.StatusBadRequest, "%v", err)
			return
		}
		s.logger.Error("admin 创建 topic 失败", "topic", req.Name, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	if req.RetentionMs > 0 {
		if tc, err = s.mt.UpdateTopicRetention(req.Name, req.RetentionMs); err != nil {
			s.logger.Error("admin 设置 retention 失败", "topic", req.Name, "err", err)
			httpError(w, http.StatusInternalServerError, "%v", err)
			return
		}
	}
	s.logger.Info("admin 创建 topic", "topic", tc.Name, "queues", tc.Queues, "retention_ms", tc.RetentionMs)
	writeJSON(w, http.StatusCreated, topicJSON{Name: tc.Name, Queues: tc.Queues,
		RetentionMs: tc.RetentionMs, CreatedAtMs: tc.CreatedAtMs})
}

// handleTopicGet GET /admin/topics/{name}：配置 + 每队列写入位置。
func (s *Server) handleTopicGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	tc, ok := s.mt.GetTopic(name)
	if !ok {
		httpError(w, http.StatusNotFound, "topic %s 不存在", name)
		return
	}
	type queueDetail struct {
		QueueID    uint32 `json:"queue_id"`
		NextOffset uint64 `json:"next_offset"`
	}
	qs := make([]queueDetail, 0, tc.Queues)
	for q := uint32(0); q < tc.Queues; q++ {
		var next uint64
		raw, ok, err := s.st.Get(store.AllocKey(name, q))
		if err != nil {
			s.logger.Error("admin 读 alloc 失败", "topic", name, "queue", q, "err", err)
			httpError(w, http.StatusInternalServerError, "%v", err)
			return
		}
		if ok {
			next = store.GetU64(raw)
		}
		qs = append(qs, queueDetail{QueueID: q, NextOffset: next})
	}
	writeJSON(w, http.StatusOK, struct {
		topicJSON
		QueuesDetail []queueDetail `json:"queues_detail"`
	}{topicJSON{Name: tc.Name, Queues: tc.Queues, RetentionMs: tc.RetentionMs, CreatedAtMs: tc.CreatedAtMs}, qs})
}

// handleTopicPatch PATCH /admin/topics/{name}：目前仅支持改 retention_ms。
func (s *Server) handleTopicPatch(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		RetentionMs int64 `json:"retention_ms"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.RetentionMs <= 0 {
		httpError(w, http.StatusBadRequest, "retention_ms 必须 >0")
		return
	}
	tc, err := s.mt.UpdateTopicRetention(name, req.RetentionMs)
	if err != nil {
		if errors.Is(err, meta.ErrTopicNotFound) {
			httpError(w, http.StatusNotFound, "%v", err)
			return
		}
		s.logger.Error("admin 更新 retention 失败", "topic", name, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, topicJSON{Name: tc.Name, Queues: tc.Queues,
		RetentionMs: tc.RetentionMs, CreatedAtMs: tc.CreatedAtMs})
}

// handleTopicDelete DELETE /admin/topics/{name}。先清数据后删注册表
// （顺序理由见 adminops.PurgeTopicData 注释）。
func (s *Server) handleTopicDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	tc, ok := s.mt.GetTopic(name)
	if !ok {
		httpError(w, http.StatusNotFound, "topic %s 不存在", name)
		return
	}
	if err := adminops.PurgeTopicData(s.st, tc); err != nil {
		s.logger.Error("admin 清理 topic 数据失败", "topic", name, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	if err := s.mt.DeleteTopic(name); err != nil {
		s.logger.Error("admin 删除 topic 注册表失败", "topic", name, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 删除 topic", "topic", name)
	w.WriteHeader(http.StatusNoContent)
}
```

（server.go 的 routes 里 `GET /admin/topics` 与 handleTopicsList/topicJSON 一并挪进本文件，保持 server.go 只剩骨架。sort import 随之迁移。）

- [ ] **Step 5: groups 失败测试** `internal/admin/groups_test.go`

```go
// 消费组端点测试。
package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
)

func TestGroupProgressAndResetCursor(t *testing.T) {
	s, _, mt, pr, dl := newTestServer(t, "", "")
	h := s.Handler()
	for i := 0; i < 3; i++ {
		if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dl.Receive(context.Background(), "g1", "t1", 0, 1, time.Minute, 0, nil); err != nil {
		t.Fatal(err)
	}
	// 列表
	w := doJSON(t, h, "GET", "/admin/groups", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("组列表应 200，得到 %d", w.Code)
	}
	// 详情：cursor=1 next=3 pending=2 inflight=1
	w = doJSON(t, h, "GET", "/admin/groups/g1", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("组详情应 200，得到 %d body=%s", w.Code, w.Body)
	}
	var detail struct {
		Name   string `json:"name"`
		Topics []struct {
			Topic  string `json:"topic"`
			Queues []struct {
				QueueID    uint32 `json:"queue_id"`
				Cursor     uint64 `json:"cursor"`
				NextOffset uint64 `json:"next_offset"`
				Pending    uint64 `json:"pending"`
				Inflight   int    `json:"inflight"`
			} `json:"queues"`
		} `json:"topics"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if len(detail.Topics) != 1 || len(detail.Topics[0].Queues) != 1 {
		t.Fatalf("详情结构不符: %s", w.Body)
	}
	q := detail.Topics[0].Queues[0]
	if q.Cursor != 1 || q.NextOffset != 3 || q.Pending != 2 || q.Inflight != 1 {
		t.Fatalf("进度不符: %+v", q)
	}
	// 位点重置到 0
	if w := doJSON(t, h, "POST", "/admin/groups/g1/reset-cursor", "",
		map[string]any{"topic": "t1", "queue_id": 0, "offset": 0}); w.Code != http.StatusNoContent {
		t.Fatalf("重置应 204，得到 %d body=%s", w.Code, w.Body)
	}
	got, err := dl.Receive(context.Background(), "g1", "t1", 0, 3, time.Minute, 0, nil)
	if err != nil || len(got) != 3 {
		t.Fatalf("重置后应从头收 3 条: %d %v", len(got), err)
	}
	// 未知组 → 404
	if w := doJSON(t, h, "GET", "/admin/groups/nope", "", nil); w.Code != http.StatusNotFound {
		t.Fatalf("未知组应 404，得到 %d", w.Code)
	}
	// 删除组
	if w := doJSON(t, h, "DELETE", "/admin/groups/g1", "", nil); w.Code != http.StatusNoContent {
		t.Fatalf("删组应 204，得到 %d", w.Code)
	}
	if _, ok := mt.GetGroup("g1"); ok {
		t.Fatal("组注册表应已删除")
	}
}
```

Run: `go test ./internal/admin/ -run TestGroupProgress -v` → FAIL

- [ ] **Step 6: 实现 groups.go**

```go
// groups.go: 消费组端点（列表/进度详情/位点重置/删除）。
//
// 职责：组×topic×queue 的消费进度推导（cursor 扫描 + alloc 差值 + inflight 计数）
// 边界：进度是抓取瞬间的快照，与在线消费存在毫秒级竞态，管理面展示可接受
package admin

import (
	"errors"
	"net/http"
	"sort"

	"github.com/xushixin/sq/internal/core/adminops"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// handleGroupsList GET /admin/groups。
func (s *Server) handleGroupsList(w http.ResponseWriter, r *http.Request) {
	gcs := s.mt.Groups()
	sort.Slice(gcs, func(i, j int) bool { return gcs[i].Name < gcs[j].Name })
	type groupJSON struct {
		Name        string `json:"name"`
		MaxAttempts int32  `json:"max_attempts"`
		CreatedAtMs int64  `json:"created_at_ms"`
	}
	out := make([]groupJSON, 0, len(gcs))
	for _, gc := range gcs {
		out = append(out, groupJSON{Name: gc.Name, MaxAttempts: gc.EffectiveMaxAttempts(), CreatedAtMs: gc.CreatedAtMs})
	}
	writeJSON(w, http.StatusOK, out)
}

// queueProgress 单队列消费进度。
type queueProgress struct {
	QueueID    uint32 `json:"queue_id"`
	Cursor     uint64 `json:"cursor"`
	NextOffset uint64 `json:"next_offset"`
	Pending    uint64 `json:"pending"`
	Inflight   int    `json:"inflight"`
}

// handleGroupGet GET /admin/groups/{name}：按 cursor 记录发现该组消费过的
// (topic, queue)，推导各自进度。没有 cursor 的 topic 不出现（组从未拉取过）。
func (s *Server) handleGroupGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	gc, ok := s.mt.GetGroup(name)
	if !ok {
		httpError(w, http.StatusNotFound, "消费组 %s 不存在", name)
		return
	}
	byTopic := map[string][]queueProgress{}
	cp := store.CursorGroupPrefix(name)
	err := s.st.Scan(cp, store.PrefixUpperBound(cp), 0, func(k, v []byte) (bool, error) {
		_, topic, q, perr := store.ParseCursorKey(k)
		if perr != nil {
			return false, perr
		}
		cur := store.GetU64(v)
		var next uint64
		if raw, ok, gerr := s.st.Get(store.AllocKey(topic, q)); gerr != nil {
			return false, gerr
		} else if ok {
			next = store.GetU64(raw)
		}
		p := queueProgress{QueueID: q, Cursor: cur, NextOffset: next}
		if next > cur {
			p.Pending = next - cur
		}
		ip := store.InflightPrefix(name, topic, q)
		if serr := s.st.Scan(ip, store.PrefixUpperBound(ip), 0, func(k2, v2 []byte) (bool, error) {
			p.Inflight++
			return true, nil
		}); serr != nil {
			return false, serr
		}
		byTopic[topic] = append(byTopic[topic], p)
		return true, nil
	})
	if err != nil {
		s.logger.Error("admin 组进度推导失败", "group", name, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	type topicProgress struct {
		Topic  string          `json:"topic"`
		Queues []queueProgress `json:"queues"`
	}
	topics := make([]topicProgress, 0, len(byTopic))
	for tp, qs := range byTopic {
		sort.Slice(qs, func(i, j int) bool { return qs[i].QueueID < qs[j].QueueID })
		topics = append(topics, topicProgress{Topic: tp, Queues: qs})
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Topic < topics[j].Topic })
	writeJSON(w, http.StatusOK, struct {
		Name        string          `json:"name"`
		MaxAttempts int32           `json:"max_attempts"`
		Topics      []topicProgress `json:"topics"`
	}{gc.Name, gc.EffectiveMaxAttempts(), topics})
}

// handleGroupResetCursor POST /admin/groups/{name}/reset-cursor。
// 经 deliver.ResetCursor（持队列锁），不直接写 store——理由见该方法注释。
func (s *Server) handleGroupResetCursor(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.mt.GetGroup(name); !ok {
		httpError(w, http.StatusNotFound, "消费组 %s 不存在", name)
		return
	}
	var req struct {
		Topic   string `json:"topic"`
		QueueID uint32 `json:"queue_id"`
		Offset  uint64 `json:"offset"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if _, ok := s.mt.GetTopic(req.Topic); !ok {
		httpError(w, http.StatusNotFound, "topic %s 不存在", req.Topic)
		return
	}
	if err := s.dl.ResetCursor(name, req.Topic, req.QueueID, req.Offset); err != nil {
		s.logger.Error("admin 位点重置失败", "group", name, "topic", req.Topic,
			"queue", req.QueueID, "offset", req.Offset, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 位点重置", "group", name, "topic", req.Topic,
		"queue", req.QueueID, "offset", req.Offset)
	w.WriteHeader(http.StatusNoContent)
}

// handleGroupDelete DELETE /admin/groups/{name}。先清数据后删注册表（同 topic 删除）。
func (s *Server) handleGroupDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.mt.GetGroup(name); !ok {
		httpError(w, http.StatusNotFound, "消费组 %s 不存在", name)
		return
	}
	if err := adminops.PurgeGroupData(s.st, name); err != nil {
		s.logger.Error("admin 清理组数据失败", "group", name, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	if err := s.mt.DeleteGroup(name); err != nil && !errors.Is(err, meta.ErrGroupNotFound) {
		s.logger.Error("admin 删除组注册表失败", "group", name, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 删除消费组", "group", name)
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 7: 跑测试 + gofmt/vet + 提交**

```bash
go test ./internal/admin/ -v && gofmt -l . && go vet ./... && go test ./...
git add internal/admin/
git commit -m "feat(admin): topic 管理与消费组进度/位点重置/删除端点"
```

---

### Task 7: Admin API — 消息查询/测试发送/DLQ 重发/延时视图/总览

**Files:**
- Modify: `internal/core/query/query.go`（追加 Browse）
- Test: `internal/core/query/query_test.go`（追加；若不存在则新建）
- Create: `internal/admin/messages.go`
- Test: `internal/admin/messages_test.go`
- Modify: `internal/admin/server.go`（routes 追加）

**Interfaces:**
- Consumes: 既有 `query.ByKey(st, topic, key, limit)`、`produce.Append`、`core.NewMessageID()`、`meta.DLQTopicName(group)`、`store.MsgKey/MsgQueuePrefix/DelayPrefix/ParseDelayKey`、moveToDLQ 写入的 `sq-origin-topic/queue/offset` 属性（M2 已实现，deliver.go moveToDLQ 已核实）；Task 4 的 `metrics.Collect`
- Produces:
  - `query.Browse(st *store.Store, topic string, queueID uint32, fromOffset uint64, limit int) ([]*core.Message, error)`
  - REST：`GET /admin/messages?topic=&key=&queue_id=&from_offset=&limit=`、`POST /admin/messages/send`、`POST /admin/dlq/{group}/resend`、`GET /admin/delay?limit=`、`GET /admin/overview`

- [ ] **Step 1: query.Browse 失败测试**（`internal/core/query/query_test.go` 追加；该文件已存在则沿用其 fixture 惯例，否则按下面自建）

```go
// TestBrowse 按队列 offset 顺序浏览。
func TestBrowse(t *testing.T) {
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 1, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(st, mt, slog.Default())
	for i := 0; i < 5; i++ {
		if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte{byte('0' + i)}}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := Browse(st, "t1", 0, 2, 2)
	if err != nil || len(got) != 2 {
		t.Fatalf("应返回 2 条: %d %v", len(got), err)
	}
	if got[0].Offset != 2 || got[1].Offset != 3 {
		t.Fatalf("offset 应为 2,3: %d,%d", got[0].Offset, got[1].Offset)
	}
	// 越界起点：空结果不报错（控制台翻页到底的正常形态）
	if got, err := Browse(st, "t1", 0, 99, 10); err != nil || len(got) != 0 {
		t.Fatalf("越界应空: %d %v", len(got), err)
	}
}
```

Run: `go test ./internal/core/query/ -run TestBrowse -v` → FAIL

- [ ] **Step 2: query.Browse 实现**（追加到 query.go）

```go
// Browse 按 (topic, queueID) 从 fromOffset 起顺序读取至多 limit 条消息
// （<=0 用 defaultLimit）。控制台"按 topic 浏览"与 DLQ 查看共用此路径。
// 越界/空队列返回空切片不报错——翻页到底是正常形态，不是错误。
func Browse(st *store.Store, topic string, queueID uint32, fromOffset uint64, limit int) ([]*core.Message, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	lower := store.MsgKey(topic, queueID, fromOffset)
	upper := store.PrefixUpperBound(store.MsgQueuePrefix(topic, queueID))
	var out []*core.Message
	err := st.Scan(lower, upper, limit, func(k, v []byte) (bool, error) {
		m, derr := core.DecodeMessage(v)
		if derr != nil {
			return false, derr
		}
		out = append(out, m)
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("浏览队列 (topic=%s q=%d from=%d): %w", topic, queueID, fromOffset, err)
	}
	return out, nil
}
```

Run: `go test ./internal/core/query/ -v` → PASS

- [ ] **Step 3: admin 消息面失败测试** `internal/admin/messages_test.go`

```go
// 消息查询/测试发送/DLQ 重发/延时视图/总览端点测试。
package admin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
)

func TestMessagesSendBrowseAndKeyQuery(t *testing.T) {
	s, _, _, pr, _ := newTestServer(t, "", "")
	h := s.Handler()
	// 测试发送
	w := doJSON(t, h, "POST", "/admin/messages/send", "",
		map[string]any{"topic": "t1", "body": "hello", "tag": "tg", "keys": []string{"k1"}})
	if w.Code != http.StatusCreated {
		t.Fatalf("发送应 201，得到 %d body=%s", w.Code, w.Body)
	}
	var sent struct {
		MsgID   string `json:"msg_id"`
		QueueID uint32 `json:"queue_id"`
		Offset  uint64 `json:"offset"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &sent); err != nil || sent.MsgID == "" {
		t.Fatalf("应返回 msg_id: %s %v", w.Body, err)
	}
	// 浏览：body 以 base64 返回
	w = doJSON(t, h, "GET", "/admin/messages?topic=t1&queue_id=0&from_offset=0&limit=10", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("浏览应 200，得到 %d body=%s", w.Code, w.Body)
	}
	var msgs []struct {
		ID         string `json:"id"`
		Tag        string `json:"tag"`
		BodyBase64 string `json:"body_base64"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &msgs); err != nil || len(msgs) != 1 {
		t.Fatalf("应 1 条: %s %v", w.Body, err)
	}
	if b, _ := base64.StdEncoding.DecodeString(msgs[0].BodyBase64); string(b) != "hello" || msgs[0].Tag != "tg" {
		t.Fatalf("内容不符: %+v", msgs[0])
	}
	// Keys 查询走 keyidx
	w = doJSON(t, h, "GET", "/admin/messages?topic=t1&key=k1", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("key 查询应 200，得到 %d", w.Code)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &msgs); err != nil || len(msgs) != 1 {
		t.Fatalf("key 查询应 1 条: %s", w.Body)
	}
	// 参数校验：既无 key 也无 queue_id → 400
	if w := doJSON(t, h, "GET", "/admin/messages?topic=t1", "", nil); w.Code != http.StatusBadRequest {
		t.Fatalf("缺参数应 400，得到 %d", w.Code)
	}
	_ = pr
}

func TestDLQResend(t *testing.T) {
	s, _, _, pr, dl := newTestServer(t, "", "")
	h := s.Handler()
	// 构造一条死信：与 moveToDLQ 的写入形状一致（origin 坐标在 Properties）。
	// 不驱动真实重试超限（那是 deliver 测试的职责），这里只验证重发路径。
	dlqTopic := meta.DLQTopicName("g1")
	if _, err := pr.Append(&core.Message{
		ID: "dead-1", Topic: dlqTopic, Body: []byte("dead"),
		Properties: map[string]string{
			"sq-origin-topic": "t-orig", "sq-origin-queue": "0", "sq-origin-offset": "5",
		},
	}); err != nil {
		t.Fatal(err)
	}
	w := doJSON(t, h, "POST", "/admin/dlq/g1/resend", "", map[string]any{"queue_id": 0, "offset": 0})
	if w.Code != http.StatusCreated {
		t.Fatalf("重发应 201，得到 %d body=%s", w.Code, w.Body)
	}
	// 原 topic 里能消费到重发的消息
	got, err := dl.Receive(context.Background(), "g-verify", "t-orig", 0, 1, time.Minute, 0, nil)
	if err != nil || len(got) != 1 || string(got[0].Body) != "dead" || got[0].ID != "dead-1" {
		t.Fatalf("原 topic 应收到重发消息: %+v %v", got, err)
	}
	// 死信条目保留（审计需要），重复重发仍可用
	if w := doJSON(t, h, "POST", "/admin/dlq/g1/resend", "", map[string]any{"queue_id": 0, "offset": 0}); w.Code != http.StatusCreated {
		t.Fatalf("重复重发应可行，得到 %d", w.Code)
	}
	// 不存在的死信 → 404
	if w := doJSON(t, h, "POST", "/admin/dlq/g1/resend", "", map[string]any{"queue_id": 0, "offset": 99}); w.Code != http.StatusNotFound {
		t.Fatalf("不存在的死信应 404，得到 %d", w.Code)
	}
}

func TestDelayViewAndOverview(t *testing.T) {
	s, _, _, pr, _ := newTestServer(t, "", "")
	h := s.Handler()
	due := time.Now().Add(time.Hour).UnixMilli()
	if _, err := pr.AppendDelay(&core.Message{Topic: "t1", Body: []byte("later"), DeliverAtMs: due}); err != nil {
		t.Fatal(err)
	}
	if _, err := pr.Append(&core.Message{Topic: "t1", Body: []byte("now")}); err != nil {
		t.Fatal(err)
	}
	w := doJSON(t, h, "GET", "/admin/delay?limit=10", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("延时视图应 200，得到 %d", w.Code)
	}
	var entries []struct {
		DueMs int64  `json:"due_ms"`
		MsgID string `json:"msg_id"`
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &entries); err != nil || len(entries) != 1 {
		t.Fatalf("应 1 条延时: %s", w.Body)
	}
	if entries[0].DueMs != due || entries[0].Topic != "t1" {
		t.Fatalf("延时条目不符: %+v（期望 due=%s）", entries[0], strconv.FormatInt(due, 10))
	}
	// 总览
	w = doJSON(t, h, "GET", "/admin/overview", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("总览应 200，得到 %d", w.Code)
	}
	var ov struct {
		Topics       int    `json:"topics"`
		Groups       int    `json:"groups"`
		DelayDepth   int    `json:"delay_depth"`
		TotalWritten uint64 `json:"total_written"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &ov); err != nil {
		t.Fatal(err)
	}
	if ov.Topics != 1 || ov.DelayDepth != 1 || ov.TotalWritten != 1 {
		t.Fatalf("总览不符: %+v", ov)
	}
}
```

（`pr.AppendDelay` 是 M3 既有方法（produce.go:152），签名 `AppendDelay(m *core.Message) (*core.Message, error)`，要求 `m.DeliverAtMs > 0`。）

Run: `go test ./internal/admin/ -run 'TestMessages|TestDLQ|TestDelay' -v` → FAIL

- [ ] **Step 4: 实现 messages.go**

```go
// messages.go: 消息面端点——查询/浏览、测试发送、DLQ 重发、延时视图、总览。
//
// 职责：REST 参数解析 + 组合 query/produce/metrics 既有能力
// 边界：
//   - msgId 精确查询不做（需要独立 msgidx，v1 用 Keys/浏览覆盖排查主路径，
//     决策记录见计划 Self-Review）
//   - 消息体一律 base64 返回（body 可能是任意二进制）
package admin

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/query"
	"github.com/xushixin/sq/internal/metrics"
	"github.com/xushixin/sq/internal/store"
)

// msgJSON 消息的 API 序列化形状。
type msgJSON struct {
	ID           string            `json:"id"`
	Topic        string            `json:"topic"`
	QueueID      uint32            `json:"queue_id"`
	Offset       uint64            `json:"offset"`
	Tag          string            `json:"tag,omitempty"`
	Keys         []string          `json:"keys,omitempty"`
	MessageGroup string            `json:"message_group,omitempty"`
	Properties   map[string]string `json:"properties,omitempty"`
	BodyBase64   string            `json:"body_base64"`
	BornAtMs     int64             `json:"born_at_ms"`
	StoreAtMs    int64             `json:"store_at_ms"`
	DeliverAtMs  int64             `json:"deliver_at_ms,omitempty"`
}

func toMsgJSON(m *core.Message) msgJSON {
	return msgJSON{
		ID: m.ID, Topic: m.Topic, QueueID: m.QueueID, Offset: m.Offset,
		Tag: m.Tag, Keys: m.Keys, MessageGroup: m.MessageGroup, Properties: m.Properties,
		BodyBase64: base64.StdEncoding.EncodeToString(m.Body),
		BornAtMs:   m.BornAtMs, StoreAtMs: m.StoreAtMs, DeliverAtMs: m.DeliverAtMs,
	}
}

// queryUint 解析 uint 型查询参数；缺省返回 def。非法值由调用方拿 err 转 400。
func queryUint(r *http.Request, name string, def uint64) (uint64, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, nil
	}
	return strconv.ParseUint(v, 10, 64)
}

// handleMessagesQuery GET /admin/messages：key 非空走 keyidx（Keys 查询），
// 否则 queue_id 必填走顺序浏览。两条路径都要求 topic。
func (s *Server) handleMessagesQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	if topic == "" {
		httpError(w, http.StatusBadRequest, "缺少 topic 参数")
		return
	}
	limit64, err := queryUint(r, "limit", 0)
	if err != nil {
		httpError(w, http.StatusBadRequest, "limit 非法: %v", err)
		return
	}
	var msgs []*core.Message
	if key := q.Get("key"); key != "" {
		msgs, err = query.ByKey(s.st, topic, key, int(limit64))
	} else if q.Get("queue_id") != "" {
		var qid, from uint64
		if qid, err = queryUint(r, "queue_id", 0); err != nil {
			httpError(w, http.StatusBadRequest, "queue_id 非法: %v", err)
			return
		}
		if from, err = queryUint(r, "from_offset", 0); err != nil {
			httpError(w, http.StatusBadRequest, "from_offset 非法: %v", err)
			return
		}
		msgs, err = query.Browse(s.st, topic, uint32(qid), from, int(limit64))
	} else {
		httpError(w, http.StatusBadRequest, "必须提供 key（Keys 查询）或 queue_id（顺序浏览）之一")
		return
	}
	if err != nil {
		s.logger.Error("admin 消息查询失败", "topic", topic, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	out := make([]msgJSON, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, toMsgJSON(m))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleMessageSend POST /admin/messages/send：控制台"发送测试消息"。
// 与 gRPC 写路径同受磁盘水位拒写约束——管理面不该有绕过保护的后门。
func (s *Server) handleMessageSend(w http.ResponseWriter, r *http.Request) {
	if s.writeBlocked != nil && s.writeBlocked.Load() {
		httpError(w, http.StatusServiceUnavailable, "磁盘水位超限，写入已暂停")
		return
	}
	var req struct {
		Topic string   `json:"topic"`
		Body  string   `json:"body"`
		Tag   string   `json:"tag"`
		Keys  []string `json:"keys"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Topic == "" {
		httpError(w, http.StatusBadRequest, "缺少 topic")
		return
	}
	if _, err := s.mt.EnsureTopic(req.Topic); err != nil {
		httpError(w, http.StatusBadRequest, "topic 不可用: %v", err)
		return
	}
	now := time.Now().UnixMilli()
	m := &core.Message{
		ID: core.NewMessageID(), Topic: req.Topic, Tag: req.Tag, Keys: req.Keys,
		Body: []byte(req.Body), BornAtMs: now, BornHost: "admin",
	}
	m, err := s.pr.Append(m)
	if err != nil {
		s.logger.Error("admin 测试消息发送失败", "topic", req.Topic, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 测试消息已发送", "topic", req.Topic, "msg_id", m.ID,
		"queue", m.QueueID, "offset", m.Offset)
	writeJSON(w, http.StatusCreated, map[string]any{
		"msg_id": m.ID, "queue_id": m.QueueID, "offset": m.Offset,
	})
}

// handleDLQResend POST /admin/dlq/{group}/resend：把死信按 sq-origin-topic
// 属性重新投回原 topic。死信条目保留（审计与再次重发），与 RocketMQ 控制台
// 行为一致。
func (s *Server) handleDLQResend(w http.ResponseWriter, r *http.Request) {
	if s.writeBlocked != nil && s.writeBlocked.Load() {
		httpError(w, http.StatusServiceUnavailable, "磁盘水位超限，写入已暂停")
		return
	}
	group := r.PathValue("group")
	var req struct {
		QueueID uint32 `json:"queue_id"`
		Offset  uint64 `json:"offset"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	dlqTopic := meta.DLQTopicName(group)
	raw, ok, err := s.st.Get(store.MsgKey(dlqTopic, req.QueueID, req.Offset))
	if err != nil {
		s.logger.Error("admin 读死信失败", "group", group, "offset", req.Offset, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	if !ok {
		httpError(w, http.StatusNotFound, "死信不存在 (dlq=%s q=%d off=%d)", dlqTopic, req.QueueID, req.Offset)
		return
	}
	m, err := core.DecodeMessage(raw)
	if err != nil {
		s.logger.Error("admin 死信解码失败", "group", group, "offset", req.Offset, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	origin := m.Properties["sq-origin-topic"]
	if origin == "" {
		// 老死信或手工放入的消息没有溯源属性——没有目的地，重发无从谈起
		httpError(w, http.StatusUnprocessableEntity, "死信缺少 sq-origin-topic 溯源属性，无法定位原 topic")
		return
	}
	if _, err := s.mt.EnsureTopic(origin); err != nil {
		httpError(w, http.StatusBadRequest, "原 topic %s 不可用: %v", origin, err)
		return
	}
	// ID 保留：全链路追踪同一条消息；Properties 保留溯源坐标（再次超限入 DLQ
	// 时会被 moveToDLQ 覆盖为新坐标）；MessageGroup 不恢复——死信重发回普通消息
	resend := &core.Message{
		ID: m.ID, Topic: origin, Tag: m.Tag, Keys: m.Keys,
		Properties: m.Properties, Body: m.Body, BornAtMs: m.BornAtMs, BornHost: m.BornHost,
	}
	resend, err = s.pr.Append(resend)
	if err != nil {
		s.logger.Error("admin 死信重发失败", "group", group, "origin", origin, "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.logger.Info("admin 死信已重发", "group", group, "msg_id", resend.ID,
		"origin_topic", origin, "queue", resend.QueueID, "offset", resend.Offset)
	writeJSON(w, http.StatusCreated, map[string]any{
		"msg_id": resend.ID, "topic": origin, "queue_id": resend.QueueID, "offset": resend.Offset,
	})
}

// handleDelayList GET /admin/delay：延时暂存区头部条目（按到期时间升序）。
func (s *Server) handleDelayList(w http.ResponseWriter, r *http.Request) {
	limit64, err := queryUint(r, "limit", 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "limit 非法: %v", err)
		return
	}
	type delayEntry struct {
		DueMs int64  `json:"due_ms"`
		MsgID string `json:"msg_id"`
		Topic string `json:"topic"`
	}
	out := []delayEntry{}
	dp := []byte(store.DelayPrefix)
	err = s.st.Scan(dp, store.PrefixUpperBound(dp), int(limit64), func(k, v []byte) (bool, error) {
		due, _, perr := store.ParseDelayKey(k)
		if perr != nil {
			return false, perr
		}
		m, derr := core.DecodeMessage(v)
		if derr != nil {
			// 坏条目由 delay 调度器负责清理（那里会删除并 Error 留痕），
			// 管理面只读，跳过即可
			s.logger.Warn("admin 延时视图跳过坏条目", "key", string(k), "err", derr)
			return true, nil
		}
		out = append(out, delayEntry{DueMs: due, MsgID: m.ID, Topic: m.Topic})
		return true, nil
	})
	if err != nil {
		s.logger.Error("admin 延时视图扫描失败", "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// handleOverview GET /admin/overview：总览计数（复用 metrics.Collect，
// 控制台图表与 Prometheus 看到同一份事实源）。
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	st, err := metrics.Collect(s.st, s.mt)
	if err != nil {
		s.logger.Error("admin 总览采集失败", "err", err)
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	var written, pending uint64
	var inflight int
	for _, n := range st.Written {
		written += n
	}
	for _, n := range st.Pending {
		pending += n
	}
	for _, n := range st.Inflight {
		inflight += n
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"topics": st.Topics, "groups": st.Groups, "delay_depth": st.DelayDepth,
		"total_written": written, "total_pending": pending, "total_inflight": inflight,
	})
}
```

routes 追加（server.go）：

```go
	s.mux.HandleFunc("GET /admin/messages", s.protected(s.handleMessagesQuery))
	s.mux.HandleFunc("POST /admin/messages/send", s.protected(s.handleMessageSend))
	s.mux.HandleFunc("POST /admin/dlq/{group}/resend", s.protected(s.handleDLQResend))
	s.mux.HandleFunc("GET /admin/delay", s.protected(s.handleDelayList))
	s.mux.HandleFunc("GET /admin/overview", s.protected(s.handleOverview))
```

- [ ] **Step 5: 跑测试 + gofmt/vet + 提交**

```bash
go test ./internal/admin/ ./internal/core/query/ -v && gofmt -l . && go vet ./... && go test ./...
git add internal/admin/ internal/core/query/
git commit -m "feat(admin): 消息查询/测试发送/DLQ 重发/延时视图/总览端点"
```

---

### Task 8: e2e 验收 + README

**Files:**
- Create: `test/e2e/sdk_auth_test.go`、`test/e2e/admin_test.go`
- Modify: `README.md`

**Interfaces:**
- Consumes: e2e 既有辅助 `startBroker(t, mutate ...func(*config.Config)) string`（返回 endpoint，mutate 可改配置）、`pickPort(t)`；官方 SDK `credentials.SessionCredentials{AccessKey, AccessSecret}`
- Produces: M5a 出口验收用例

- [ ] **Step 1: 认证 e2e** `test/e2e/sdk_auth_test.go`

```go
//go:build e2e

// 官方 Go SDK 认证 e2e：AK/SK 配置后，错凭据被拒、对凭据全链路可用（spec §6）。
//
// 职责：
//   - 验证服务端签名校验对真实 SDK 的行为：拒绝路径与放行路径
//
// 边界：
//   - 不测重放窗口（服务端刻意不做，见 internal/rpc/auth.go 边界注释）
package e2e

import (
	"context"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"

	"github.com/xushixin/sq/internal/config"
)

func TestOfficialGoSDKAuth(t *testing.T) {
	endpoint := startBroker(t, func(c *config.Config) {
		c.AccessKey = "e2e-ak"
		c.SecretKey = "e2e-sk"
	})
	const topic = "e2e-auth"

	// 错误凭据：QueryRoute 在客户端启动路径上就会被拒。SDK 对 gRPC 层错误
	// 可能在 Start 或首次 Send 时暴露，两处都接受，但必须在限时内失败。
	bad, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{AccessKey: "e2e-ak", AccessSecret: "wrong"},
	}, rmq.WithTopics(topic))
	if err == nil {
		if serr := bad.Start(); serr == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, senderr := bad.Send(ctx, &rmq.Message{Topic: topic, Body: []byte("x")})
			cancel()
			bad.GracefulStop()
			if senderr == nil {
				t.Fatal("错误凭据的发送应失败")
			}
			t.Logf("错误凭据在 Send 阶段被拒: %v", senderr)
		} else {
			t.Logf("错误凭据在 Start 阶段被拒: %v", serr)
		}
	} else {
		t.Logf("错误凭据在构造阶段被拒: %v", err)
	}

	// 正确凭据：发送 + 消费 + ack 全链路
	good, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{AccessKey: "e2e-ak", AccessSecret: "e2e-sk"},
	}, rmq.WithTopics(topic))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := good.Start(); err != nil {
		t.Fatalf("正确凭据 Start 失败: %v", err)
	}
	defer good.GracefulStop()
	if _, err := good.Send(context.Background(), &rmq.Message{Topic: topic, Body: []byte("authed")}); err != nil {
		t.Fatalf("正确凭据发送失败: %v", err)
	}
	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: "e2e-auth-g",
		Credentials:   &credentials.SessionCredentials{AccessKey: "e2e-ak", AccessSecret: "e2e-sk"},
	},
		rmq.WithSimpleAwaitDuration(3*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{topic: rmq.SUB_ALL}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	defer consumer.GracefulStop()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			continue // 空轮询返回 MESSAGE_NOT_FOUND，正常
		}
		for _, mv := range mvs {
			if string(mv.GetBody()) != "authed" {
				t.Fatalf("消息体不符: %s", mv.GetBody())
			}
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			return
		}
	}
	t.Fatal("正确凭据 60s 内未消费到消息")
}
```

- [ ] **Step 2: Admin API 冒烟 e2e** `test/e2e/admin_test.go`

```go
//go:build e2e

// Admin API e2e 冒烟：真实进程上 登录 → 建 topic → 发测试消息 → 浏览 → 总览。
//
// 职责：验证 admin HTTP 在完整进程装配下真实可用（端口、登录、与 store 的接线）
// 边界：端点行为细节由 internal/admin 单测覆盖，这里只走一条主链路
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/config"
)

// adminJSON 带 token 调 admin API 并解析 JSON 响应；expectCode 不符即 Fatal。
func adminJSON(t *testing.T, method, url, token string, body any, expectCode int, out any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != expectCode {
		t.Fatalf("%s %s 应 %d，得到 %d body=%s", method, url, expectCode, resp.StatusCode, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("解析响应失败: %v body=%s", err, raw)
		}
	}
}

func TestAdminAPISmoke(t *testing.T) {
	adminPort := pickPort(t)
	startBroker(t, func(c *config.Config) {
		c.AdminListen = fmt.Sprintf("127.0.0.1:%d", adminPort)
		c.AdminUsername = "root"
		c.AdminPassword = "e2e-pw"
	})
	base := fmt.Sprintf("http://127.0.0.1:%d", adminPort)

	// 未登录 → 401
	resp, err := http.Get(base + "/admin/topics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("未登录应 401，得到 %d", resp.StatusCode)
	}
	// 登录
	var login struct {
		Token string `json:"token"`
	}
	adminJSON(t, "POST", base+"/admin/login", "",
		map[string]string{"username": "root", "password": "e2e-pw"}, http.StatusOK, &login)
	if login.Token == "" {
		t.Fatal("登录应返回 token")
	}
	// 建 topic → 发测试消息 → 浏览
	adminJSON(t, "POST", base+"/admin/topics", login.Token,
		map[string]any{"name": "e2e-admin", "queues": 1}, http.StatusCreated, nil)
	var sent struct {
		MsgID string `json:"msg_id"`
	}
	adminJSON(t, "POST", base+"/admin/messages/send", login.Token,
		map[string]any{"topic": "e2e-admin", "body": "from-admin"}, http.StatusCreated, &sent)
	var msgs []struct {
		ID string `json:"id"`
	}
	adminJSON(t, "GET", base+"/admin/messages?topic=e2e-admin&queue_id=0", login.Token,
		nil, http.StatusOK, &msgs)
	if len(msgs) != 1 || msgs[0].ID != sent.MsgID {
		t.Fatalf("浏览应见刚发的消息: %+v (want %s)", msgs, sent.MsgID)
	}
	// 总览与 /metrics
	var ov struct {
		Topics       int    `json:"topics"`
		TotalWritten uint64 `json:"total_written"`
	}
	adminJSON(t, "GET", base+"/admin/overview", login.Token, nil, http.StatusOK, &ov)
	if ov.Topics < 1 || ov.TotalWritten < 1 {
		t.Fatalf("总览计数不符: %+v", ov)
	}
	mresp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	mraw, _ := io.ReadAll(mresp.Body)
	mresp.Body.Close()
	if mresp.StatusCode != http.StatusOK || !bytes.Contains(mraw, []byte("sq_topic_messages_written_total")) {
		t.Fatalf("/metrics 应含 sq_topic_messages_written_total: code=%d", mresp.StatusCode)
	}
	_ = time.Second // 保持 import 稳定（若最终未用到 time 则删除本行与 import）
}
```

（收尾时若 `time` 确实未用，删掉占位行与 import——不留死代码。）

- [ ] **Step 3: 跑 e2e**

```bash
go test -tags e2e ./test/e2e/ -run 'TestOfficialGoSDKAuth|TestAdminAPISmoke' -v -timeout 300s
```
Expected: PASS。若 SDK 对 Unauthenticated 的行为与预期不符（如 Start 不返回错误而是内部无限重试），按用例注释的两分支放宽处，只调断言位置，不放宽「限时内必须失败」。

- [ ] **Step 4: README 更新**

- 功能表增 M5a 行：认证（gRPC AK/SK + Admin 登录）、Admin API、/metrics。
- 配置样例追加五个新键与语义（成对、零值=关闭、admin_listen 默认 :8082）。
- 新增「Admin API」小节：端点表（方法/路径/用途一行一个）、登录流程 curl 示例、`/metrics` 指标名列表。
- 安全边界说明：AK/SK 无重放窗口（可信内网定位）、/metrics 不设防、删除类操作先停流量、token 重启失效。

- [ ] **Step 5: 全量回归 + 提交**

```bash
go build ./... && go test ./... && gofmt -l . && go vet ./...
git add test/e2e/ README.md
git commit -m "test(e2e): 官方 SDK 认证与 Admin API 冒烟；docs: README M5a"
```

---

## Self-Review

**1. Spec 覆盖（对 spec §6/§8/§9 与用户要求逐条）**：
- §6「可选静态 AK/SK（Signature 头校验），默认关闭」→ Task 1/2/8 ✓
- §6「控制台独立简单密码登录」+ 用户要求「用户名密码在配置文件配置」→ Task 1/5 ✓
- §8 结构化日志 → 各 task 的日志步骤 + 既有惯例 ✓；Prometheus /metrics（收发 QPS、堆积、inflight、延时深度、fsync 延迟）→ Task 4（QPS 由 written_total/cursor 的 rate 推导；「回查次数」属 M6 事务，届时补）✓
- §9 Admin API：总览/Topic 管理/消费组（堆积、inflight、位点重置）/消息查询（Keys、按 topic 浏览）/DLQ 查看（浏览 %DLQ% topic）与单条重发/延时队列视图/发送测试消息 → Task 5/6/7 ✓
- **刻意不做并记录**：msgId 精确查询（需独立 msgidx 索引，且该索引与 retention 的 DeleteRange 清理模型冲突——keyidx 能按 topic+时间清理，msgid 键无法按 topic 分区；Keys+浏览已覆盖排查主路径，M5b 控制台阶段再评估是否值得加索引）；QPS 折线的服务端聚合（Prometheus rate() 的事）；React 控制台（M5b 单独计划）。
- 连接数指标（spec §9 总览提到）：v1 无会话注册表，不做，M6 Telemetry 会话管理后顺手补——记录为已知缺口。

**2. Placeholder 扫描**：全部步骤含真实代码/命令；无 TBD/「适当处理」。Task 5 曾考虑占位 handler，已改为把 topics 列表实现并入骨架任务消除占位。唯一的执行期自适应点：Task 3/7 「meta_test.go/query_test.go 若已有 fixture 则沿用」与 e2e 认证用例的两分支断言——均给出了完整的两种写法，不是留白。

**3. 类型一致性核查**：
- `New(st, mt, pr, dl, username, password, writeBlocked, reg, logger)`——Task 5 定义，server_test.go/Task 6/7 测试同序使用 ✓
- `metrics.Collect(st, mt) (*Stats, error)`、`Stats.Written/Pending/Inflight` 字段名——Task 4 定义，Task 7 overview 使用 ✓
- `deliver.ResetCursor(group, topic string, queueID uint32, offset uint64) error`——Task 3 定义，Task 6 handler 使用 ✓
- `store.ParseCursorKey` 返回 `(group, topic, queueID, err)` 四值——Task 3 定义，Task 4 stats 与 Task 6 groups 使用 ✓
- `dl.Receive(ctx, group, topic, queueID, maxMsgs, invisible, wait, filter)` 8 参——与 deliver.go:122 既有签名一致，所有测试传参 `(ctx, "g", "t", 0, n, time.Minute, 0, nil)` ✓
- `pr.AppendDelay(m)` 要求 `DeliverAtMs>0`——Task 7 测试用未来时间 ✓

**4. 与 M4 的冲突面复核**：本计划改动的既有文件 = config.go、keys.go、meta.go、store.go、query.go、main.go、deliver.go（仅文件尾追加）、README。M4 改动 = types.go、deliver.go 主体、send.go、receive.go、server.go、deliver_test.go、README。交集 = deliver.go（两侧一改主体一追加尾部，可自动合并）与 README（功能表相邻行，手工秒解）。
