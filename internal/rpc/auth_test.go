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
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Xsxdot/sq/internal/config"
)

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
	u, _ := NewAuthInterceptors([]config.Credential{{AccessKey: "ak1", SecretKey: "sk1"}}, slog.Default())
	if err := callUnary(t, u, signedCtx("ak1", "sk1", "20260806T120000Z")); err != nil {
		t.Fatalf("合法签名应放行: %v", err)
	}
}

func TestAuthUnaryRejects(t *testing.T) {
	u, _ := NewAuthInterceptors([]config.Credential{{AccessKey: "ak1", SecretKey: "sk1"}}, slog.Default())
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
	_, s := NewAuthInterceptors([]config.Credential{{AccessKey: "ak1", SecretKey: "sk1"}}, slog.Default())
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

// TestAuthAcceptsAllOfficialSDKHeaderShapes 钉住五个官方 SDK 的头形状差异。
//
// 官方实现自己并不统一：Credential 段 Go/C++ 带 "//Rocketmq"、Java/Python/C#
// 是裸 AK；签名十六进制 Go/Java/Python 小写、C#/C++ 大写。任何一种形状被拒，
// 对应语言的客户端在开启鉴权后就是 100% 连不上。
func TestAuthAcceptsAllOfficialSDKHeaderShapes(t *testing.T) {
	u, _ := NewAuthInterceptors([]config.Credential{{AccessKey: "ak1", SecretKey: "sk1"}}, slog.Default())
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
	u, _ := NewAuthInterceptors([]config.Credential{{AccessKey: "ak1", SecretKey: "sk1"}}, slog.Default())
	for _, upper := range []bool{false, true} {
		ctx := sdkCtx("ak1", "wrong", "20260807T120000Z", upper)
		if status.Code(callUnary(t, u, ctx)) != codes.Unauthenticated {
			t.Fatalf("密钥错误必须拒绝（upper=%v）", upper)
		}
	}
}

// TestAuthMultiCredential 多凭据下的命中/拒绝路径：非首条凭据必须放行；
// 「命中但签名错」与「AK 不存在（dummy 路径）」两类失败的信息必须一致，
// 防止 AK 枚举探针。
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

// TestAuthSignatureCache 钉住签名缓存的三条行为契约：
//  1. 同一 date 的第二次校验（缓存命中路径）仍正确放行——缓存值本身必须对；
//  2. date 变化后缓存正确失效：新 date 用旧 date 的签名必须被拒、
//     用新 date 的正确签名必须放行；
//  3. 缓存命中路径下错签名仍被拒——缓存的是期望值，不是"放行"结论。
func TestAuthSignatureCache(t *testing.T) {
	u, _ := NewAuthInterceptors([]config.Credential{{AccessKey: "ak1", SecretKey: "sk1"}}, slog.Default())
	d1, d2 := "20260812T120000Z", "20260812T120001Z"

	// 第一次校验：填充缓存
	if err := callUnary(t, u, signedCtx("ak1", "sk1", d1)); err != nil {
		t.Fatalf("首次校验应放行: %v", err)
	}
	// 同 date 二次校验：缓存命中路径仍正确
	if err := callUnary(t, u, signedCtx("ak1", "sk1", d1)); err != nil {
		t.Fatalf("同 date 二次校验（缓存命中）应放行: %v", err)
	}
	// 缓存命中路径下错签名必须被拒（拿错误 secret 签同一 date）
	if status.Code(callUnary(t, u, signedCtx("ak1", "wrong", d1))) != codes.Unauthenticated {
		t.Fatal("缓存命中路径下错签名必须被拒")
	}
	// date 变化：拿旧 date 的签名冒充新 date 必须被拒（缓存必须失效重算）
	staleSig := sdkCtx("ak1", "sk1", d1, false) // 先按 d1 算签名……
	md, _ := metadata.FromIncomingContext(staleSig)
	md = md.Copy()
	md.Set("x-mq-date-time", d2) // ……再把 datetime 换成 d2，签名与 date 不配套
	if status.Code(callUnary(t, u, metadata.NewIncomingContext(context.Background(), md))) != codes.Unauthenticated {
		t.Fatal("date 变化后沿用旧签名必须被拒")
	}
	// 新 date 的正确签名放行（缓存重算后写回）
	if err := callUnary(t, u, signedCtx("ak1", "sk1", d2)); err != nil {
		t.Fatalf("新 date 正确签名应放行: %v", err)
	}
	// 回到旧 date：缓存已被 d2 覆盖，需重算——依然正确
	if err := callUnary(t, u, signedCtx("ak1", "sk1", d1)); err != nil {
		t.Fatalf("回到旧 date 应重算并放行: %v", err)
	}
}

// fakeServerStream 只提供 Context()，其余方法不会被拦截器触碰。
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }
