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
