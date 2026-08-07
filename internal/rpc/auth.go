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
//   - 签名算法与头格式以官方 SDK 为准，且必须容忍各语言实现之间的差异：
//     Credential 段可能是 "{ak}//Rocketmq"（Go/C++）也可能是裸 "{ak}"
//     （Java/Python/C#）；签名十六进制可能小写（Go/Java/Python）也可能大写
//     （C#/C++）。两种差异都在 auth_test.go 的 SDK 形状表里钉住
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
	// 官方 SDK 五个语言实现的十六进制大小写并不统一：Go/Java/Python 输出小写
	// （hex.EncodeToString / encodeHexString(...,false) / hexlify），C#/C++ 输出
	// 大写（BitConverter.ToString / MixAll::hex 的 'A'-'F' 字典）。服务端统一
	// 折成小写再比 —— 否则 C#/C++ 客户端开启鉴权后 100% 认证失败，且错误信息
	// 刻意不区分原因，几乎无法自助排查。
	// 对客户端送来的串做小写化不泄露密钥的任何信息，常数时间比较的性质不变。
	sigOK := subtle.ConstantTimeCompare([]byte(strings.ToLower(sig)), []byte(expect)) == 1
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
