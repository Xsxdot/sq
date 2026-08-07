# M7 第一波：鉴权加固 + 吞吐根因 + 工程卫生 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 一次落地 M7 前三批——①多组 AK/SK、receipt handle 加签、Receive/Send 入口校验（B7.1+B3+B6）；②`produce.Append` 队列粒度锁修复 group commit 失效并出基准数字（B1）；③两条承重测试与 test/e2e 独立模块（B5+B4）。

**Architecture:** ①是协议面三处独立收口，全部集中在 config/rpc 两层，不触存储格式（新增一个 `meta/handle_secret` 键）；②把 `produce.Producer` 的单一全局锁拆为「全局锁只护共享 map + 每 (topic,queue) 一把锁护 offset 分配与 Apply」，同队列语义不变、跨队列 fsync 得以被 Pebble group commit 合并；③是纯测试与模块图手术。

**Tech Stack:** Go 1.26、Pebble v2、grpc-go、官方 RocketMQ Go SDK（仅 e2e）。

**Specs（需求唯一来源，冲突以 spec 为准）:**
- `docs/superpowers/specs/2026-08-07-auth-and-protocol-hardening-design.md`（Task 1–5）
- `docs/superpowers/specs/2026-08-07-throughput-and-hygiene-design.md`（Task 6–9）

## Global Constraints

- 在独立 worktree 分支（建议 `m7-batch1-hardening`）上执行，不动 main。
- 每个 task 结束时整个仓库可编译、`go test ./internal/...` 全绿（e2e 除外）。
- 日志一律 `slog`（Server/组件注入的 logger），**禁止** `fmt.Printf`/`print`。错误分支必须带上下文（topic/group/msg_id 等），成功路径不静默（热路径用 Debug）。
- 注释遵循全局 CLAUDE.md §2：新文件顶部职责+边界；导出函数 doc comment；非显然逻辑写「为什么」。
- 认证错误信息统一「认证失败」，不区分 AK 错/签名错（既有纪律，勿改）。
- handle 相关 Warn 日志对客户端输入只留截断预览（`truncateForLog`，既有纪律）。
- 提交信息结尾：`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。
- 破坏性变更已获批：配置删除 `access_key`/`secret_key` 标量；handle 不兼容旧格式。
- Task 7→8 顺序不可调换：基准必须先在旧代码上记录数字。

---

### Task 1: 配置多凭据化 + auth 多凭据校验 + main 装配

配置层与鉴权层必须同一提交（删标量字段会同时破坏 config/auth/main/e2e 四处，分开提交会留下不可编译的中间态）。

**Files:**
- Modify: `internal/config/config.go`（删 AccessKey/SecretKey 字段与成对校验，加 Credential/Credentials 与校验）
- Modify: `internal/config/config_test.go`（新增表驱动用例）
- Modify: `internal/rpc/auth.go`（多凭据 map + dummy secret）
- Modify: `internal/rpc/auth_test.go`（多凭据用例；SDK 形状表保留）
- Modify: `cmd/sq/main.go:191-195`（装配条件与日志）
- Modify: `sq.example.yaml`（credentials 段；顺手补漏 M6 的 txn 两项）
- Modify: `test/e2e/sdk_auth_test.go:25-26`（`c.AccessKey/SecretKey` 改 `c.Credentials`）

**Interfaces:**
- Produces: `config.Credential{Name, AccessKey, SecretKey string}`、`Config.Credentials []Credential`、`rpc.NewAuthInterceptors(creds []config.Credential, logger *slog.Logger) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor)`——Task 5 的 e2e 依赖 `config.Credential`。

- [ ] **Step 1: 写失败的 config 测试**

`internal/config/config_test.go` 追加（沿用文件内既有的写临时 yaml + Load 的模式）：

```go
func TestLoadCredentials(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr bool
		wantN   int
	}{
		{"空列表=关闭", "", false, 0},
		{"两条合法", "credentials:\n  - name: 订单服务\n    access_key: AK1\n    secret_key: SK1\n  - access_key: AK2\n    secret_key: SK2\n", false, 2},
		{"缺 secret_key", "credentials:\n  - access_key: AK1\n", true, 0},
		{"缺 access_key", "credentials:\n  - secret_key: SK1\n", true, 0},
		{"AK 重复", "credentials:\n  - access_key: AK1\n    secret_key: a\n  - access_key: AK1\n    secret_key: b\n", true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "sq.yaml")
			if err := os.WriteFile(p, []byte(c.yaml), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(p)
			if c.wantErr {
				if err == nil {
					t.Fatal("期望校验失败，却通过了")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(cfg.Credentials) != c.wantN {
				t.Fatalf("凭据数 = %d, 期望 %d", len(cfg.Credentials), c.wantN)
			}
		})
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/config/ -run TestLoadCredentials -v`
Expected: FAIL（`cfg.Credentials` 未定义，编译错）

- [ ] **Step 3: 实现 config 变更**

`internal/config/config.go`——删除第 48-49 行两个标量字段与第 122-127 行成对校验，新增：

```go
// Credential 一条 gRPC 静态鉴权凭据。多条凭据 = 每个接入方一对、可单独吊销。
type Credential struct {
	Name      string `yaml:"name"`       // 可选，仅用于日志追溯（如"订单服务"），不参与校验
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}
```

Config 结构体在原 AccessKey 位置替换为：

```go
	// Credentials gRPC 静态鉴权凭据列表；空/缺省 = 不鉴权（spec §6 默认关闭）。
	// v1.0 前的破坏性变更：取代旧的 access_key/secret_key 标量对。
	Credentials []Credential `yaml:"credentials"`
```

Load 校验（放在原成对校验的位置）：

```go
	// 每条凭据必须成对非空（同旧标量时代"只填一半必是笔误"的原则），AK 全局唯一
	// ——重复 AK 会让 map 构建时后者静默覆盖前者，吊销/排查全部错乱，启动即挡。
	seen := map[string]int{}
	for i, c := range cfg.Credentials {
		if c.AccessKey == "" || c.SecretKey == "" {
			return nil, fmt.Errorf("配置 credentials[%d] 的 access_key/secret_key 必须成对非空", i)
		}
		if j, dup := seen[c.AccessKey]; dup {
			return nil, fmt.Errorf("配置 credentials[%d] 与 credentials[%d] 的 access_key 重复: %q", i, j, c.AccessKey)
		}
		seen[c.AccessKey] = i
	}
```

- [ ] **Step 4: 写失败的 auth 多凭据测试**

`internal/rpc/auth_test.go` 追加。文件内已有现成 helper：`signedCtx(ak, secret, datetime)` 构造带签名头的 incoming context，`callUnary(t, u, ctx)` 让 ctx 过一遍 unary 拦截器并返回 err——直接复用：

```go
func TestAuthMultiCredential(t *testing.T) {
	creds := []config.Credential{
		{Name: "订单服务", AccessKey: "AK1", SecretKey: "SK1"},
		{AccessKey: "AK2", SecretKey: "SK2"},
	}
	u, _ := NewAuthInterceptors(creds, slog.Default())
	// 命中非首条凭据：AK2/SK2 正确签名必须放行
	if err := callUnary(t, u, signedCtx("AK2", "SK2", "20260807T120000Z")); err != nil {
		t.Fatalf("第二条凭据应放行: %v", err)
	}
	// 命中但签名错（拿别人的 secret 签）与 AK 不存在（dummy 路径）：
	// 两者必须同为 Unauthenticated，且错误信息完全一致——不泄露"AK 对不对"
	errHit := callUnary(t, u, signedCtx("AK1", "SK2", "20260807T120000Z"))
	errMiss := callUnary(t, u, signedCtx("AK9", "SK1", "20260807T120000Z"))
	for name, err := range map[string]error{"命中但签名错": errHit, "AK不存在": errMiss} {
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("%s: 应返回 Unauthenticated，得到 %v", name, err)
		}
	}
	if errHit.Error() != errMiss.Error() {
		t.Fatalf("两类失败的错误信息必须一致（防 AK 枚举探针）: %q vs %q", errHit, errMiss)
	}
}
```

既有用例的 `NewAuthInterceptors("ak1", "sk1", slog.Default())` 全部改为
`NewAuthInterceptors([]config.Credential{{AccessKey: "ak1", SecretKey: "sk1"}}, slog.Default())`
（共约 4 处：TestAuthUnaryAcceptsValidSignature / TestAuthUnaryRejects /
TestAuthStreamRejects / TestAuthAcceptsAllOfficialSDKHeaderShapes）。SDK 五语言
形状表（Credential 段、大小写）原样保留。auth_test.go 补 import config。

- [ ] **Step 5: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run TestVerifyAuth -v`
Expected: FAIL（`NewAuthInterceptors` 签名不符，编译错）

- [ ] **Step 6: 实现 auth 多凭据**

`internal/rpc/auth.go`：

```go
// credInfo 一条凭据的校验材料（由 config.Credential 构建，包内私有形态）。
type credInfo struct {
	secret string
	name   string
}

// dummySecret 未命中 AK 时用于计算 HMAC 的占位密钥。它的作用只是让"AK 不存在"
// 与"AK 存在但签名错"走完全相同的计算路径（一次 HMAC + 一次常数时间比较），
// 抹平时序差；它不承担保密职责——未命中时无论签名比对结果如何，found=false
// 都会强制拒绝，攻击者即使预先算出 dummy 的"正确"签名也无法通过。
var dummySecret = []byte("sq-dummy-secret-for-timing-equalization")

func verifyAuth(ctx context.Context, byAK map[string]credInfo, logger *slog.Logger, method string) error {
	// ……metadata 提取、parseAuthorization 与现状完全一致，此处只列变更段……
	info, found := byAK[ak]
	secret := dummySecret
	if found {
		secret = []byte(info.secret)
	}
	h := hmac.New(sha1.New, secret)
	h.Write([]byte(dates[0]))
	expect := hex.EncodeToString(h.Sum(nil))
	// 大小写折叠的理由见原实现注释（C#/C++ SDK 输出大写），原样保留
	sigOK := subtle.ConstantTimeCompare([]byte(strings.ToLower(sig)), []byte(expect)) == 1
	if !found || !sigOK {
		// 失败原因刻意不外泄（错误信息统一），日志侧保留细节供运维排查
		logger.Warn("认证失败：AK 或签名不匹配", "method", method, "access_key", ak, "name", info.name)
		return status.Error(codes.Unauthenticated, "认证失败")
	}
	logger.Debug("认证通过", "method", method, "access_key", ak, "name", info.name)
	return nil
}

// NewAuthInterceptors 构造 unary 与 stream 两个认证拦截器。调用方（main）仅在
// credentials 非空时安装；两个拦截器共享同一张只读凭据表，覆盖全部 RPC——
// 包括 ReceiveMessage（服务端流）与 Telemetry（双向流），SDK 对它们同样签名。
func NewAuthInterceptors(creds []config.Credential, logger *slog.Logger) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	byAK := make(map[string]credInfo, len(creds))
	for _, c := range creds {
		byAK[c.AccessKey] = credInfo{secret: c.SecretKey, name: c.Name}
	}
	l := logger.With("mod", "rpc.auth")
	// unary/stream 闭包体与现状一致，仅把 (accessKey, secretKey) 换成 byAK
	...
}
```

原「先比 AK 再比签名会泄露一位信息」的注释改写为解释 dummy 路径（见 dummySecret 注释）。文件头注释的职责段补一句「多凭据：按 AK 查表，未命中走 dummy 路径抹平时序」。auth.go 需新增 `"github.com/xushixin/sq/internal/config"` import。

- [ ] **Step 7: 改 main 装配与 e2e helper**

`cmd/sq/main.go:191-195`：

```go
	if len(cfg.Credentials) > 0 {
		au, as := rpc.NewAuthInterceptors(cfg.Credentials, logger)
		gopts = append(gopts, grpc.ChainUnaryInterceptor(au), grpc.ChainStreamInterceptor(as))
		logger.Info("gRPC AK/SK 认证已启用", "credentials", len(cfg.Credentials))
	}
```

`test/e2e/sdk_auth_test.go:25-26`：

```go
		c.Credentials = []config.Credential{{Name: "e2e", AccessKey: "e2e-ak", SecretKey: "e2e-sk"}}
```

- [ ] **Step 8: 改 sq.example.yaml**

鉴权段整段替换（并在 metrics_retention_hours 之后补 M6 漏掉的 txn 两项）：

```yaml
# —— gRPC 静态鉴权 ——
# 凭据列表；空/缺省 = 不做 gRPC 鉴权。每条 access_key/secret_key 必须成对，
# access_key 全局唯一；name 可选，仅用于日志追溯。
# credentials:
#   - name: 订单服务
#     access_key: "AK1"
#     secret_key: "SK1"
credentials: []

# —— 事务消息 ——
# 半消息回查间隔（Go duration）；单条最大回查次数，超限丢弃并记日志
txn_check_interval: 30s
txn_max_checks: 15
```

- [ ] **Step 9: 全量跑测试**

Run: `go build ./... && go vet ./... && go test ./internal/config/ ./internal/rpc/ -count=1`
Expected: PASS（e2e 是 tag 隔离的，此处编译不到；`go vet -tags e2e ./test/e2e/` 单独跑一次确认 helper 改对）

- [ ] **Step 10: 自检日志与注释**

- config 校验错误信息带下标定位（credentials[N]）✔（Step 3 已含）
- auth 失败 Warn 带 method/ak/name，成功 Debug ✔（Step 6 已含）
- main 启用日志打凭据条数、不打密钥 ✔（Step 7 已含）
- Credential 结构体、NewAuthInterceptors doc comment、dummySecret「为什么」注释 ✔

- [ ] **Step 11: Commit**

```bash
git add internal/config/ internal/rpc/auth.go internal/rpc/auth_test.go cmd/sq/main.go sq.example.yaml test/e2e/sdk_auth_test.go
git commit -m "feat(auth): 多组 AK/SK——credentials 列表取代标量对（v1.0 前破坏性变更）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: receipt handle 加签（持久化密钥 + HMAC-SHA256）

**Files:**
- Modify: `internal/store/keys.go`（HandleSecretKey）
- Create: `internal/rpc/handle_secret.go`（密钥加载/生成）
- Modify: `internal/rpc/receipt.go`（签名编解码）
- Modify: `internal/rpc/server.go`（Server 持密钥，New 加参）
- Modify: `internal/rpc/receive.go:204,305,372`、`internal/rpc/forward.go:21`（调用点）
- Modify: `cmd/sq/main.go`（加载密钥并传入 rpc.New）
- Create/Modify: `internal/rpc/receipt_test.go`、各 `*_test.go` 中 `rpc.New(` 调用点
- Test: `internal/rpc/handle_secret_test.go`

**Interfaces:**
- Consumes: Task 1 无依赖（可与 Task 1 并行开发，但按序执行）。
- Produces: `store.HandleSecretKey() []byte`、`rpc.LoadOrCreateHandleSecret(st *store.Store, logger *slog.Logger) ([]byte, error)`、`rpc.New(..., writeBlocked *atomic.Bool, handleSecret []byte, logger *slog.Logger)`（在 writeBlocked 与 logger 之间插参）、`receiptEncode(secret []byte, group, topic string, queueID uint32, offset uint64, attempt int32) string`、`receiptDecode(secret []byte, s string) (group, topic string, queueID uint32, offset uint64, attempt int32, err error)`。

- [ ] **Step 1: 写失败的 receipt 签名测试**

`internal/rpc/receipt_test.go`（若已存在则追加）：

```go
func TestReceiptSignRoundtrip(t *testing.T) {
	secret := []byte("test-handle-secret")
	h := receiptEncode(secret, "g1", "t1", 3, 42, 2)
	g, topic, q, off, a, err := receiptDecode(secret, h)
	if err != nil || g != "g1" || topic != "t1" || q != 3 || off != 42 || a != 2 {
		t.Fatalf("往返失败: %v %v %v %v %v %v", g, topic, q, off, a, err)
	}
}

func TestReceiptRejectsTampering(t *testing.T) {
	secret := []byte("test-handle-secret")
	h := receiptEncode(secret, "g1", "t1", 0, 1, 1)
	cases := map[string]string{
		"payload 篡改": "x" + h[1:],
		"签名段篡改":     h[:len(h)-2] + "xx",
		"缺签名段（旧格式）": strings.Split(h, ".")[0],
		"换密钥":       receiptEncode([]byte("other-secret"), "g1", "t1", 0, 1, 1)[:len(h)], // 用错误密钥签的完整 handle
	}
	for name, bad := range cases {
		if _, _, _, _, _, err := receiptDecode(secret, bad); err == nil {
			t.Fatalf("%s: 未被拒绝", name)
		}
	}
	// 伪造攻击本体：自造 payload 不带合法签名，必须被拒
	forged := base64.StdEncoding.EncodeToString([]byte(`{"g":"victim","t":"t1","q":0,"o":1,"a":1}`))
	if _, _, _, _, _, err := receiptDecode(secret, forged); err == nil {
		t.Fatal("无签名伪造 handle 未被拒绝")
	}
}
```

`internal/rpc/handle_secret_test.go`（`store.Open(dir, sync bool, logger)` 的用法照
server_test.go:67）：

```go
func TestHandleSecretPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	st1, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	k1, err := LoadOrCreateHandleSecret(st1, slog.Default())
	if err != nil || len(k1) != 32 {
		t.Fatalf("首次生成: %v len=%d", err, len(k1))
	}
	st1.Close()
	st2, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	k2, err := LoadOrCreateHandleSecret(st2, slog.Default())
	if err != nil || !bytes.Equal(k1, k2) {
		t.Fatalf("重开后密钥变了: %v", err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestReceipt|TestHandleSecret' -v`
Expected: FAIL（签名不符，编译错）

- [ ] **Step 3: 实现**

`internal/store/keys.go`：

```go
// HandleSecretKey receipt handle 签名密钥：meta/handle_secret。
// 首次启动生成、永不轮换——轮换会使全部在途 handle 失效，收益为零。
func HandleSecretKey() []byte { return []byte("meta/handle_secret") }
```

`internal/rpc/handle_secret.go`（新文件，带文件头职责/边界注释）：

```go
// receipt handle 签名密钥的加载与生成。
//
// 职责：
//   - 首次启动生成 32 字节随机密钥并持久化到 meta/handle_secret
//   - 此后每次启动原样加载——重启不换钥，在途 handle 跨重启仍有效
//
// 边界：
//   - 与 AK/SK 鉴权配置无关：鉴权关闭时 handle 防伪造同样生效
//   - 只在 main 装配期调用一次，不做缓存失效或轮换
package rpc

import (
	"crypto/rand"
	"fmt"
	"log/slog"

	"github.com/xushixin/sq/internal/store"
)

// LoadOrCreateHandleSecret 加载或生成 handle 签名密钥。
func LoadOrCreateHandleSecret(st *store.Store, logger *slog.Logger) ([]byte, error) {
	v, ok, err := st.Get(store.HandleSecretKey())
	if err != nil {
		return nil, fmt.Errorf("读取 handle 签名密钥: %w", err)
	}
	if ok {
		logger.Info("receipt handle 签名密钥已加载")
		return v, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("生成 handle 签名密钥: %w", err)
	}
	b := st.NewBatch()
	b.Set(store.HandleSecretKey(), key, nil)
	if err := st.Apply(b); err != nil {
		return nil, fmt.Errorf("持久化 handle 签名密钥: %w", err)
	}
	logger.Info("receipt handle 签名密钥已生成并持久化")
	return key, nil
}
```

`internal/rpc/receipt.go`——编解码加签名段（文件头注释补「为什么加签：无签名的
base64(JSON) 任何客户端可自造，冒充别的 group ack 掉他人 inflight」）：

```go
// receiptSigSep 分隔 payload 与签名段。StdEncoding 字母表不含 '.'，Cut 无歧义。
const receiptSigSep = "."

func receiptEncode(secret []byte, group, topic string, queueID uint32, offset uint64, attempt int32) string {
	b, _ := json.Marshal(receipt{G: group, T: topic, Q: queueID, O: offset, A: attempt}) // 结构固定无失败路径
	mac := hmac.New(sha256.New, secret)
	mac.Write(b)
	return base64.StdEncoding.EncodeToString(b) + receiptSigSep + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func receiptDecode(secret []byte, s string) (group, topic string, queueID uint32, offset uint64, attempt int32, err error) {
	payload, sig, found := strings.Cut(s, receiptSigSep)
	if !found {
		return "", "", 0, 0, 0, fmt.Errorf("receipt handle 缺少签名段")
	}
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("receipt handle 非法 base64: %w", err)
	}
	sigRaw, err := base64.StdEncoding.DecodeString(sig)
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("receipt handle 签名段非法 base64: %w", err)
	}
	// 先验签再解 JSON：未通过验签的字节不值得进一步解析（hmac.Equal 常数时间）
	mac := hmac.New(sha256.New, secret)
	mac.Write(raw)
	if !hmac.Equal(sigRaw, mac.Sum(nil)) {
		return "", "", 0, 0, 0, fmt.Errorf("receipt handle 签名校验失败")
	}
	var r receipt
	if err := json.Unmarshal(raw, &r); err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("receipt handle 非法 JSON: %w", err)
	}
	return r.G, r.T, r.Q, r.O, r.A, nil
}
```

`internal/rpc/server.go`：Server 加字段 `handleSecret []byte`；`New` 在 writeBlocked
与 logger 之间加参 `handleSecret []byte` 并赋值。四个调用点改为
`receiptEncode(s.handleSecret, ...)` / `receiptDecode(s.handleSecret, ...)`。

`cmd/sq/main.go`：在构造 `rpc.New` 之前：

```go
	handleSecret, err := rpc.LoadOrCreateHandleSecret(st, logger)
	if err != nil {
		return err
	}
```

并把 `handleSecret` 传入 `rpc.New`。

- [ ] **Step 4: 修全部 rpc.New 测试调用点**

测试侧构造 Server 的入口集中在 `internal/rpc/server_test.go:85` 的 `newTestEnv`：

```go
	srv := New(cfg, mt, pr, dl, tx, blocked, []byte("test-handle-secret"), slog.Default())
```

再 `grep -rn "= New(" internal/rpc/*_test.go` 确认没有第二处直接调用（有则同样补参）。

- [ ] **Step 5: 跑测试**

Run: `go build ./... && go test ./internal/rpc/ ./internal/store/ -count=1`
Expected: PASS——受影响的既有用例（ack/changeInvisible/forward/receive）因编解码都走
同一密钥而原样通过。

- [ ] **Step 6: 自检日志与注释**

- 密钥生成/加载各一条 Info、不打密钥内容 ✔（Step 3）
- 验签失败错误带原因、调用方沿用既有 Warn+截断预览路径 ✔（无需改调用方）
- 新文件头注释、receiptSigSep/先验签再解析的「为什么」✔

- [ ] **Step 7: Commit**

```bash
git add internal/store/keys.go internal/rpc/ cmd/sq/main.go
git commit -m "feat(rpc): receipt handle HMAC-SHA256 加签——持久化服务端密钥，伪造 handle 直接拒绝

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: ReceiveMessage 入口校验（topic 存在性 + queueID 边界）

**Files:**
- Modify: `internal/rpc/receive.go:53-87`（校验插在 TAG 过滤解析后、dl.Receive 前）
- Test: `internal/rpc/receive_test.go`

**Interfaces:**
- Consumes: `meta.GetTopic(name string) (TopicConfig, bool)`（已有）。

- [ ] **Step 1: 写失败的测试**

`internal/rpc/receive_test.go` 追加（模式照 `TestReceiveMessageEmptyPollReportsMessageNotFound`：`newTestClient` + 流式收 `recvAll`）：

```go
func TestReceiveRejectsUnknownTopic(t *testing.T) {
	// 从未被 QueryRoute/Send 创建过的 topic 直接 Receive → TOPIC_NOT_FOUND。
	// 正常 SDK 到不了这个分支（它先 QueryRoute，autoCreate 时那里已建 topic），
	// 挡的是绕过路由的手写客户端与已删除 topic
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: "g-nt"},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: "never-created"}, Id: 0},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
		BatchSize:         10,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	_, st := recvAll(t, stream)
	if st.GetCode() != pb.Code_TOPIC_NOT_FOUND {
		t.Fatalf("状态码应为 TOPIC_NOT_FOUND，实际 %v", st)
	}
}

func TestReceiveRejectsOutOfRangeQueue(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// 先经 QueryRoute 自动建 topic（默认 4 队列），再请求越界队列 99
	if _, err := c.QueryRoute(ctx, &pb.QueryRouteRequest{Topic: &pb.Resource{Name: "t-oob"}}); err != nil {
		t.Fatalf("QueryRoute: %v", err)
	}
	stream, err := c.ReceiveMessage(ctx, &pb.ReceiveMessageRequest{
		Group:             &pb.Resource{Name: "g-oob"},
		MessageQueue:      &pb.MessageQueue{Topic: &pb.Resource{Name: "t-oob"}, Id: 99},
		FilterExpression:  &pb.FilterExpression{Type: pb.FilterType_TAG, Expression: "*"},
		BatchSize:         10,
		InvisibleDuration: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	_, st := recvAll(t, stream)
	if st.GetCode() != pb.Code_BAD_REQUEST {
		t.Fatalf("状态码应为 BAD_REQUEST，实际 %v", st)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestReceiveRejects' -v`
Expected: FAIL（当前不校验，走到 MESSAGE_NOT_FOUND 或成功空返回）

- [ ] **Step 3: 实现**

`internal/rpc/receive.go`，插在 filter 解析块之后（77 行前）：

```go
	// topic 存在性与队列边界必须在进入 deliver 前挡住（spec 鉴权收尾 §5）。
	// 用只读 GetTopic 而非 EnsureTopic：消费动作不应创建 topic。
	// 为什么这里可以硬拒而不影响正常 SDK：SDK 总是先 QueryRoute（那里在
	// auto_create 开启时会建 topic）再 Receive，走到这里 topic 必已存在；
	// 能命中该分支的是绕过路由的手写客户端或已被删除的 topic。
	tc, ok := s.mt.GetTopic(topic)
	if !ok {
		s.logger.Warn("ReceiveMessage 拒绝：topic 不存在", "group", group, "topic", topic, "queue", queueID)
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_TOPIC_NOT_FOUND, fmt.Sprintf("topic %q 不存在", topic)),
		}})
	}
	if queueID >= tc.Queues {
		s.logger.Warn("ReceiveMessage 拒绝：queueID 越界", "group", group, "topic", topic, "queue", queueID, "queues", tc.Queues)
		return stream.Send(&pb.ReceiveMessageResponse{Content: &pb.ReceiveMessageResponse_Status{
			Status: errStatus(pb.Code_BAD_REQUEST, fmt.Sprintf("queueID %d 越界（topic %q 共 %d 队列）", queueID, topic, tc.Queues)),
		}})
	}
```

（receive.go 若未 import fmt 则补。）

- [ ] **Step 4: 跑测试**

Run: `go test ./internal/rpc/ -count=1`
Expected: PASS（既有用例都先 EnsureTopic，不受影响）

- [ ] **Step 5: 自检日志与注释**

两个拒绝分支 Warn 带完整上下文 ✔；「为什么可以硬拒」注释 ✔。

- [ ] **Step 6: Commit**

```bash
git add internal/rpc/receive.go internal/rpc/receive_test.go
git commit -m "feat(rpc): ReceiveMessage 校验 topic 存在性与 queueID 边界

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: SendMessage 只读预检（B6）

**Files:**
- Modify: `internal/rpc/send.go:36`（第一遍之前插入预检块）
- Test: `internal/rpc/send_test.go`

**Interfaces:**
- Consumes: `meta.ValidateName(s string) error`、`meta.GetTopic`、`s.cfg.AutoCreateTopic`（均已有）。

- [ ] **Step 1: 写失败的测试**

**前置修整**：`newTestEnv`（server_test.go:78-81）只把 `autoCreate` 传给 `meta.New`，
`cfg.AutoCreateTopic` 仍是 Load("") 的默认 true——本任务的预检读的是 cfg，两者必须
对齐，在 `config.Load` 之后补一行：

```go
	cfg.AutoCreateTopic = autoCreate // cfg 与 meta 的 autoCreate 必须一致，预检（send.go 第零遍）读的是 cfg
```

`internal/rpc/send_test.go` 追加（发送模式照 `TestSendMessageNormal`；「零落盘」直接
扫 `env.st` 的 msg 键区间，键构造用 `store.MsgKey`）：

```go
// sendReq 本文件小 helper：单/多条消息的 SendMessageRequest。
func sendReq(topics ...string) *pb.SendMessageRequest {
	req := &pb.SendMessageRequest{}
	for _, tp := range topics {
		req.Messages = append(req.Messages, &pb.Message{
			Topic:            &pb.Resource{Name: tp},
			SystemProperties: &pb.SystemProperties{MessageType: pb.MessageType_NORMAL},
			Body:             []byte("x"),
		})
	}
	return req
}

// countTopicMsgs 扫 [MsgKey(topic,0,0), MsgKey(topic,queues,0)) 计数（默认 4 队列）。
func countTopicMsgs(t *testing.T, st *store.Store, topic string) int {
	t.Helper()
	n := 0
	err := st.Scan(store.MsgKey(topic, 0, 0), store.MsgKey(topic, 4, 0), 0,
		func(k, v []byte) (bool, error) { n++; return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSendRejectsMixedTopicBatchWithoutPersisting(t *testing.T) {
	env := newTestEnv(t, true)
	resp, err := env.client.SendMessage(context.Background(), sendReq("t-mix-a", "t-mix-b"))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_BAD_REQUEST {
		t.Fatalf("混 topic 批次应拒 BAD_REQUEST，实际 %v", resp.GetStatus())
	}
	// 关键断言：第一条也没落盘（B6 的幽灵消息就是这么产生的）
	if n := countTopicMsgs(t, env.st, "t-mix-a") + countTopicMsgs(t, env.st, "t-mix-b"); n != 0 {
		t.Fatalf("拒绝批次却落盘了 %d 条", n)
	}
}

func TestSendRejectsUnknownTopicWhenAutoCreateOff(t *testing.T) {
	env := newTestEnv(t, false)
	resp, err := env.client.SendMessage(context.Background(), sendReq("t-nocreate"))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_TOPIC_NOT_FOUND {
		t.Fatalf("未建 topic 应拒 TOPIC_NOT_FOUND，实际 %v", resp.GetStatus())
	}
	if n := countTopicMsgs(t, env.st, "t-nocreate"); n != 0 {
		t.Fatalf("拒绝批次却落盘了 %d 条", n)
	}
}

func TestSendRejectsIllegalTopicName(t *testing.T) {
	env := newTestEnv(t, true)
	resp, err := env.client.SendMessage(context.Background(), sendReq("bad/name"))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.GetStatus().GetCode() != pb.Code_ILLEGAL_TOPIC {
		t.Fatalf("非法名应在预检即拒 ILLEGAL_TOPIC，实际 %v", resp.GetStatus())
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/rpc/ -run 'TestSendRejects' -v`
Expected: 混 topic 与非法名两条 FAIL（当前第二遍才发现，首条已落盘/错误码不同）

- [ ] **Step 3: 实现**

`internal/rpc/send.go`，插在「第一遍」循环之前（37 行前）：

```go
	// 第零遍：topic 级只读预检（spec 鉴权收尾 §6 / backlog B6）。
	// 必须完全无副作用——EnsureTopic（会创建 topic）绝不能出现在这里，
	// 否则"整批任一条失败即什么都没发生"的承诺被打破。
	if len(req.GetMessages()) > 0 {
		topic := req.GetMessages()[0].GetTopic().GetName()
		for _, pm := range req.GetMessages()[1:] {
			if pm.GetTopic().GetName() != topic {
				// 官方 Go/Java SDK 客户端侧即拒绝混 topic 批次（producer.go:276），
				// 能到这里的只有手写客户端；若不拒绝，第二遍逐条 Append 会在部分
				// 落盘后因 topic 错误返回不可重试错误，已落盘条目成为幽灵消息
				s.logger.Warn("SendMessage 拒绝：批内 topic 不一致", "topic", topic, "other", pm.GetTopic().GetName())
				return &pb.SendMessageResponse{Status: errStatus(pb.Code_BAD_REQUEST,
					"批内所有消息的 topic 必须一致")}, nil
			}
		}
		if err := meta.ValidateName(topic); err != nil {
			s.logger.Warn("SendMessage 拒绝：topic 名字非法", "topic", topic, "err", err)
			return &pb.SendMessageResponse{Status: errStatus(pb.Code_ILLEGAL_TOPIC, err.Error())}, nil
		}
		if !s.cfg.AutoCreateTopic {
			if _, ok := s.mt.GetTopic(topic); !ok {
				s.logger.Warn("SendMessage 拒绝：topic 不存在且未开启自动创建", "topic", topic)
				return &pb.SendMessageResponse{Status: errStatus(pb.Code_TOPIC_NOT_FOUND,
					fmt.Sprintf("topic %q 不存在（auto_create_topic 已关闭）", topic))}, nil
			}
		}
	}
```

（send.go 补 import `"github.com/xushixin/sq/internal/core/meta"`。SendMessage 顶部
doc comment 补一段「第零遍」说明。）

- [ ] **Step 4: 跑测试**

Run: `go test ./internal/rpc/ -count=1`
Expected: PASS

- [ ] **Step 5: 自检日志与注释**

三个拒绝分支 Warn 带 topic 上下文 ✔；「为什么 EnsureTopic 不能提前」注释 ✔。

- [ ] **Step 6: Commit**

```bash
git add internal/rpc/send.go internal/rpc/send_test.go
git commit -m "feat(rpc): SendMessage 只读预检——混 topic/非法名/未建 topic 整批拒、零落盘

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: e2e 鉴权用例扩多凭据 + 全量 e2e

**Files:**
- Modify: `test/e2e/sdk_auth_test.go`

**Interfaces:**
- Consumes: Task 1 的 `config.Credential`。

- [ ] **Step 1: 扩用例**

现有单凭据用例已在 Task 1 改为列表写法。追加：broker 配两条凭据
（`{Name:"e2e", AccessKey:"e2e-ak", SecretKey:"e2e-sk"}, {AccessKey:"e2e-ak2", SecretKey:"e2e-sk2"}`），
用**第二条**凭据完成发送→接收→ack 全链路（证明命中非首条）；再用
`AccessKey:"e2e-ak2", AccessSecret:"e2e-sk"`（交叉错配）断言被拒。断言与重试写法
照抄文件内既有的错误凭据用例。

- [ ] **Step 2: 跑鉴权 e2e**

Run: `go test -tags e2e -count=1 -run TestOfficialGoSDKAuth ./test/e2e/`（按文件内实际测试名调整 -run）
Expected: PASS

- [ ] **Step 3: 跑全量 e2e（批①的回归门）**

Run: `go test -tags e2e -count=1 ./test/e2e/`
Expected: 全部 PASS。重点观察：handle 加签后所有消费链路（普通/延时/FIFO/事务/DLQ
转发）照常，Receive 校验未误伤 SDK 正常轮询（SDK 先 QueryRoute 建 topic 再 Receive，
见 Task 3 注释的推理——此步是该推理的实证）。

- [ ] **Step 4: Commit**

```bash
git add test/e2e/sdk_auth_test.go
git commit -m "test(e2e): 多凭据鉴权全链路——第二条凭据收发 ack、交叉错配被拒

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: B5 两条承重测试（改锁前钉住旧行为）

**Files:**
- Modify: `internal/store/store_test.go`（Scan limit<=0）
- Modify: `internal/core/produce/produce_test.go`（并发 Append）

**Interfaces:**
- Consumes: `store.Scan(lower, upper []byte, limit int, fn func(k, v []byte) (bool, error)) error`、`produce.Producer.Append`（均已有）。
- Produces: `TestAppendConcurrentNoDupNoHole`——Task 8 改锁后必须原样通过（等价重构的证明）。

- [ ] **Step 1: 写 Scan 语义测试**

`internal/store/store_test.go` 追加（沿用文件内开临时 store 的 helper）：

```go
func TestScanNonPositiveLimitMeansUnlimited(t *testing.T) {
	// limit<=0 == 不限量：该语义被 deliver 阶段 2 的跳过逻辑直接依赖（B5）
	st := openTestStore(t, t.TempDir()) // 文件内既有 helper（store_test.go:19）
	const n = 10
	for i := 0; i < n; i++ {
		b := st.NewBatch()
		b.Set([]byte(fmt.Sprintf("scan-t/%03d", i)), []byte("v"), nil)
		if err := st.Apply(b); err != nil {
			t.Fatal(err)
		}
	}
	for _, limit := range []int{0, -1} {
		got := 0
		err := st.Scan([]byte("scan-t/"), []byte("scan-t0"), limit, func(k, v []byte) (bool, error) {
			got++
			return true, nil
		})
		if err != nil || got != n {
			t.Fatalf("limit=%d: got=%d err=%v（期望全量 %d）", limit, got, err, n)
		}
	}
}
```

- [ ] **Step 2: 写并发 Append 测试**

`internal/core/produce/produce_test.go` 追加（沿用文件内 fixture）：

```go
func TestAppendConcurrentNoDupNoHole(t *testing.T) {
	// Producer 类型注释声称「并发安全」，此用例在 -race 下钉住它，并断言
	// offset 分配无重复无空洞、alloc 计数器与消息数严格一致。
	// Task 8 改队列粒度锁后本用例必须原样通过（等价重构的证明）。
	p, st := newTestProducer(t, t.TempDir()) // 既有 fixture：sync store + autoCreate(4 队列)
	const goroutines, perG = 8, 50
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perG)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				m := &core.Message{Topic: "t-conc", Body: []byte("x")}
				if _, err := p.Append(m); err != nil {
					errs <- err
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// 逐队列扫描 msg/ 前缀：offset 必须恰为 0..count-1（无重复无空洞），
	// 且 alloc 计数器 == count；四队列 count 总和 == goroutines*perG
	total := 0
	for q := uint32(0); q < 4; q++ {
		count := 0
		prev := int64(-1)
		// 队列区间 = [MsgKey(t,q,0), MsgKey(t,q+1,0))：MsgKey 的 queueID/offset
		// 均为定长大端编码，区间边界成立（执行时打开 keys.go:65 复核一眼）
		err := st.Scan(store.MsgKey("t-conc", q, 0), store.MsgKey("t-conc", q+1, 0), 0,
			func(k, v []byte) (bool, error) {
				_, _, off, perr := store.ParseMsgKey(k)
				if perr != nil {
					return false, perr
				}
				if int64(off) != prev+1 {
					t.Fatalf("q%d offset 不连续: prev=%d got=%d", q, prev, off)
				}
				prev = int64(off)
				count++
				return true, nil
			})
		if err != nil {
			t.Fatal(err)
		}
		v, ok, _ := st.Get(store.AllocKey("t-conc", q))
		if count > 0 && (!ok || store.GetU64(v) != uint64(count)) {
			t.Fatalf("q%d alloc 计数器与消息数不一致", q)
		}
		total += count
	}
	if total != goroutines*perG {
		t.Fatalf("总量 = %d, 期望 %d", total, goroutines*perG)
	}
}
```

- [ ] **Step 3: 跑测试（-race）**

Run: `go test -race -count=1 ./internal/store/ ./internal/core/produce/`
Expected: PASS（旧全局锁下天然串行，先钉住行为）

- [ ] **Step 4: Commit**

```bash
git add internal/store/store_test.go internal/core/produce/produce_test.go
git commit -m "test: 补两条承重测试——Scan(limit<=0) 全量语义与并发 Append 的 -race 钉板

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: B1 基准先行（旧代码上记录数字）

**Files:**
- Create: `internal/core/produce/bench_test.go`

**Interfaces:**
- Produces: `BenchmarkAppendParallel`——Task 8 改后复跑同一基准对比。

- [ ] **Step 1: 写基准**

```go
// 并发写吞吐基准：真实 store（fsync=sync）+ 多队列 topic + RunParallel。
// 意义：量化"全局锁跨 fsync 导致 group commit 失效"（produce.go 锁注释记录的
// 瓶颈）。必须先在旧代码上跑出基线，改锁后复跑对比——没有前后两组数字，
// "变快了"只是主张不是事实。
func BenchmarkAppendParallel(b *testing.B) {
	// fixture 照 newTestProducer（produce_test.go:25）改造：*testing.B、
	// store.Open(dir, true /*syncWrites——基准量的就是 fsync 合并*/, ...)、
	// meta.New(st, true, 16 /*16 队列，给并发留出跨队列并行度*/, 16, ...)
	p, _ := newBenchProducer(b, b.TempDir())
	body := []byte("benchmark-payload-256B........................................")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := &core.Message{Topic: "t-bench", Body: body}
			if _, err := p.Append(m); err != nil {
				b.Fatal(err)
			}
		}
	})
}
```

fixture（同文件）：

```go
// newBenchProducer 基准专用 fixture：sync 写盘（基准量的就是 fsync 合并），
// 16 队列 topic 给并发留出跨队列并行度。
func newBenchProducer(b *testing.B, dir string) (*Producer, *store.Store) {
	b.Helper()
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		b.Fatalf("store: %v", err)
	}
	b.Cleanup(func() { st.Close() })
	mt, err := meta.New(st, true, 16, 16, slog.Default())
	if err != nil {
		b.Fatalf("meta: %v", err)
	}
	return New(st, mt, slog.Default()), st
}
```

- [ ] **Step 2: 跑基准并记录**

Run: `go test -bench BenchmarkAppendParallel -benchtime 3s -run '^$' ./internal/core/produce/ | tee /tmp/bench-before.txt`
Expected: 输出 ns/op；换算 msg/s 记下（这是旧代码基线）。

- [ ] **Step 3: Commit（基准数字写进提交信息）**

```bash
git add internal/core/produce/bench_test.go
git commit -m "bench(produce): 并发 Append 基准——改锁前基线 <N> ns/op ≈ <M> msg/s

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: B1 队列粒度锁实施

**Files:**
- Modify: `internal/core/produce/produce.go`

**Interfaces:**
- Consumes: Task 6 的 `TestAppendConcurrentNoDupNoHole`、Task 7 的基准。
- Produces: `Producer` 对外方法签名全部不变（Append/AppendWith/AppendDelay/Subscribe）——调用方零改动。

- [ ] **Step 1: 实现队列粒度锁**

`internal/core/produce/produce.go` 结构体与锁结构改为：

```go
// queueState 单个 (topic, queue) 的写入状态。qs.mu 串行化同队列的
// offset 分配与落盘；不同队列各持各锁，Apply 得以并发进入 Pebble，
// group commit 才能把并发 fsync 合并（B1 的全部意义所在）。
type queueState struct {
	mu     sync.Mutex
	next   uint64 // 下一 offset（懒加载自 alloc/ key）
	loaded bool
}

type Producer struct {
	st     *store.Store
	mt     *meta.Meta
	logger *slog.Logger

	// mu 只护共享 map（qstates/rr/wakers）——临界区内不再有任何 I/O。
	// 旧实现的单一全局锁跨越 store.Apply（fsync 在内），使所有队列的写入
	// 全局串行、group commit 失效，见 git 历史中原锁注释的完整推导。
	mu      sync.Mutex
	qstates map[string]*queueState
	rr      map[string]uint32
	wakers  map[string]chan struct{}

	// delayMu 护延时暂存区的 seq 分配与落盘（单一全局计数器，天然串行；
	// 独立成锁是为了不与普通写入互相阻塞）。
	delayMu     sync.Mutex
	delayNext   uint64
	delayLoaded bool
}
```

`New` 初始化 `qstates: map[string]*queueState{}`（去掉 `next` map）。

`AppendWith` 锁段改为三段式（校验/EnsureTopic/ID/时间戳等锁外逻辑不变）：

```go
	// 段 1（p.mu）：队列选择 + 取/建 queueState。纯内存，不含 I/O。
	p.mu.Lock()
	if m.MessageGroup != "" {
		h := fnv.New32a()
		h.Write([]byte(m.MessageGroup))
		m.QueueID = h.Sum32() % tc.Queues
	} else {
		m.QueueID = p.rr[m.Topic] % tc.Queues
		p.rr[m.Topic]++
	}
	k := qkey(m.Topic, m.QueueID)
	qs, ok := p.qstates[k]
	if !ok {
		qs = &queueState{}
		p.qstates[k] = qs
	}
	p.mu.Unlock()

	// 段 2（qs.mu）：offset 分配 + 编码 + 落盘。同队列串行（offset 顺序 ==
	// 落盘顺序，FIFO 根基），跨队列并行（group commit 合并 fsync）。
	qs.mu.Lock()
	off, err := qs.nextOffsetLocked(p.st, m.Topic, m.QueueID)
	if err != nil {
		qs.mu.Unlock()
		return nil, err
	}
	m.Offset = off
	raw, err := core.EncodeMessage(m)
	if err != nil {
		qs.mu.Unlock()
		return nil, err
	}
	b := p.st.NewBatch()
	// ……batch 组装与现状逐字一致（MsgKey/AllocKey/KeyIdxKey/extra）……
	if err := p.st.Apply(b); err != nil {
		qs.mu.Unlock()
		return nil, fmt.Errorf("写入消息 %s (topic=%s q=%d off=%d): %w", m.ID, m.Topic, m.QueueID, m.Offset, err)
	}
	// Apply 成功才推进（失败的写不能烧 offset，原注释原样保留）
	qs.next = off + 1
	qs.loaded = true
	qs.mu.Unlock()

	// 段 3（p.mu）：唤醒长轮询。必须在落盘成功之后——被唤醒的订阅者读 store
	// 必能看到这条消息。
	p.mu.Lock()
	p.wakeLocked(m.Topic)
	p.mu.Unlock()
	p.logger.Debug("消息已写入", "topic", m.Topic, "queue", m.QueueID, "offset", m.Offset, "msg_id", m.ID, "keys", len(m.Keys))
	return m, nil
```

`nextOffsetLocked` 迁为 `queueState` 方法（调用方持 qs.mu）：

```go
// nextOffsetLocked 取该队列下一 offset，懒加载盘上 alloc/ 计数器。
// 调用方必须持有 qs.mu。
func (qs *queueState) nextOffsetLocked(st *store.Store, topic string, q uint32) (uint64, error) {
	if qs.loaded {
		return qs.next, nil
	}
	v, ok, err := st.Get(store.AllocKey(topic, q))
	if err != nil {
		return 0, fmt.Errorf("读取 offset 计数器 %s/%d: %w", topic, q, err)
	}
	if !ok {
		return 0, nil
	}
	return store.GetU64(v), nil
}
```

注意懒加载后要 `qs.loaded = true`——首次加载路径里 `qs.next` 仍由 Apply 成功后的
`qs.next = off + 1; qs.loaded = true` 统一设置（miss 时读到的值只作返回，不提前写缓存，
保持「Apply 成功才推进」的不变量）。

`AppendDelay`：`p.mu` 全部换成 `p.delayMu`（seq 分配 + Apply + 缓存推进），逻辑不变。
`nextDelaySeqLocked` 注释改为「调用方必须持有 p.delayMu」。
`Subscribe`/`wakeLocked` 不变（p.mu）。
`AppendWith` doc comment 的重入约束改为「extra 内不得再调用本 Producer 的任何方法
（p.mu 与 queueState.mu 均不可重入）」。
包头注释补一行边界：「锁结构：p.mu 护共享 map；每 (topic,queue) 一把锁护写入；
delayMu 护延时暂存」。

- [ ] **Step 2: 跑 produce 全量 + 承重测试（-race）**

Run: `go test -race -count=1 ./internal/core/produce/ ./internal/core/...`
Expected: 全 PASS，尤其 `TestAppendConcurrentNoDupNoHole` 原样通过（等价重构证明）。

- [ ] **Step 3: 复跑基准对比**

Run: `go test -bench BenchmarkAppendParallel -benchtime 3s -run '^$' ./internal/core/produce/ | tee /tmp/bench-after.txt`
Expected: 相对 Task 7 基线显著提升（group commit 生效的直接证据），换算 msg/s ≥ 5000。
若未达标：不要急于调参糊弄——先确认 fixture 确实 `fsync=sync`、队列数 ≥ GOMAXPROCS，
仍不达标则如实记录数字并在最终汇报中说明（达标线是 spec §10 的声明门槛，不是本
任务的合并门槛）。

- [ ] **Step 4: 全仓测试**

Run: `go build ./... && go vet ./... && go test -race -count=1 ./internal/...`
Expected: 全 PASS

- [ ] **Step 5: 自检日志与注释**

- 锁结构三段注释各自说明「为什么」✔（Step 1）
- 错误路径 wrap 上下文原样保留 ✔
- Debug 成功日志保留 ✔

- [ ] **Step 6: Commit（前后数字都写进提交信息）**

```bash
git add internal/core/produce/produce.go
git commit -m "perf(produce): 锁按队列拆分——group commit 生效，并发写 <before> → <after> msg/s

旧 p.mu 跨越 store.Apply（fsync 在内），所有队列全局串行、group commit 失效。
拆为 p.mu（共享 map）+ 每队列锁（offset 分配与落盘）+ delayMu（延时暂存），
同队列 offset 顺序==落盘顺序不变，跨队列 fsync 得以合并。

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: B4 test/e2e 拆独立 Go module

**Files:**
- Create: `test/e2e/go.mod`（及 tidy 生成的 go.sum）
- Modify: `go.mod` / `go.sum`（根模块 tidy 掉 SDK）
- Modify: `Makefile`（e2e 目标）

**Interfaces:**
- Produces: e2e 子模块，import 路径全部不变（包名与文件零改动）。

- [ ] **Step 1: 建子模块**

`test/e2e/go.mod`：

```
module github.com/xushixin/sq/test/e2e

go 1.26.1

require (
	github.com/apache/rocketmq-clients/golang/v5 v5.1.4
	github.com/xushixin/sq v0.0.0
)

replace github.com/xushixin/sq => ../..
```

Run: `cd test/e2e && go mod tidy`
（`go mod tidy` 视所有 build tag 为满足，`//go:build e2e` 文件的 import 会被收进来。）

- [ ] **Step 2: 根模块甩掉 SDK**

Run: `go mod tidy`（仓库根）
Expected: `go.mod` 不再含 `rocketmq-clients`，间接依赖大幅缩减（opencensus/zap/
validator/google.golang.org/api 等消失）。
Run: `grep -c rocketmq go.mod` → 0

- [ ] **Step 3: 改 Makefile**

```makefile
e2e:
	cd test/e2e && go test -tags e2e -count=1 ./...
```

- [ ] **Step 4: 验证两侧**

Run: `go build ./... && go vet ./... && go test -count=1 ./internal/...`
Run: `cd test/e2e && go vet -tags e2e ./... && go test -tags e2e -count=1 -run TestOfficialGoSDKAuth ./...`
Expected: 均 PASS（全量 e2e 留给 Task 10 统一跑）

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum test/e2e/go.mod test/e2e/go.sum Makefile
git commit -m "build: test/e2e 拆独立模块——主模块图甩掉官方 SDK 及其约 20 个间接依赖

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: 收尾——README 同步 + 全量验证门

**Files:**
- Modify: `README.md`（鉴权段多凭据写法、handle 升级注意、e2e 模块说明）

- [ ] **Step 1: README 同步**

三处：①鉴权配置示例改 `credentials` 列表写法（含 name 说明与「每接入方一对、
可单独吊销”一句）；②「升级注意」处加一条：本版本起 receipt handle 带签名，升级
重启后在途旧 handle 失效、消息按不可见窗口到期重投（一次性）；配置 `access_key`/
`secret_key` 标量改为 `credentials` 列表（迁移示例一段）；③开发者说明处注明 e2e
为独立模块（`make e2e` 或 `cd test/e2e && go test -tags e2e ./...`）。措辞总校
留给 B7.2，此处只保证不撒谎。

- [ ] **Step 2: instrumenting-code 终检**

逐项过：每个错误分支带上下文日志 ✔；外部调用（store 读写）前后可追溯 ✔；成功路径
不静默（Info/Debug）✔；无 fmt.Printf ✔；新文件头注释 ✔；导出函数 doc comment ✔。

- [ ] **Step 3: 全量验证门**

```bash
go build ./... && go vet ./... && go test -race -count=1 ./...
cd test/e2e && go test -tags e2e -count=1 ./...
cd web && npm run test
```

Expected: 全绿（web 无改动，vitest 是回归确认）。e2e 全量是 handle 加签 + Receive
校验 + 锁拆分三者的最终回归门。

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: README 同步多凭据鉴权、handle 升级注意与 e2e 独立模块

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## 完成定义

- 10 个 task 全部提交，工作树干净。
- 主模块 `go test -race ./...` 全绿；e2e 子模块全量全绿；vitest 全绿。
- 基准改前/改后两组数字落在 Task 7/8 提交信息中。
- 汇报时附：基准对比数字、e2e 全量耗时、README 变更点清单。
