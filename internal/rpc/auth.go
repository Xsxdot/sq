// auth.go: gRPC 静态 AK/SK 认证（spec §6「可选静态 AK/SK（Signature 头校验），默认关闭」）。
//
// 职责：
//   - 校验官方 SDK 的 MQv2-HMAC-SHA1 签名头（unary 与 stream 两个拦截器）
//   - 多凭据：按 AK 查表，未命中走 dummy 路径抹平时序（见 dummyCred）
//   - 每凭据单条目签名缓存：SDK datetime 秒粒度，同秒内期望签名不变，
//     缓存命中即免去每 RPC 一次 HMAC（见 credInfo.cache 与 dummyCred 的
//     时序抹平论证）
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
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/xushixin/sq/internal/config"
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

// sigCache 一份「datetime → 期望签名」的缓存快照（期望值为小写 hex）。
// 整体以不可变快照存取（atomic.Pointer 换指针），date 与 sig 永远配套，
// 无撕裂读。
type sigCache struct {
	date string
	sig  string
}

// credInfo 一条凭据的校验材料（由 config.Credential 构建，包内私有形态）。
//
// 必须以指针形态放入 byAK：cache 是 atomic.Pointer，值拷贝会破坏原子性
// 语义（vet copylocks 同款问题），且缓存写回需要落到共享的那一份上。
type credInfo struct {
	secret string
	name   string

	// cache 单条目签名缓存。期望签名 = HMAC-SHA1(secret, x-mq-date-time)，
	// 而 SDK 的 datetime 是秒粒度——同一 AK 同一秒内的所有 RPC 期望值相同，
	// 缓存命中即可跳过每 RPC 一次的 HMAC 计算与 hex 编码分配（三机 pprof
	// 实测 auth 拦截器占 6.8% CPU）。并发下重复计算无害：同一 date 算出的
	// 值恒等，谁后写回都幂等。
	cache atomic.Pointer[sigCache]
}

// expectedSig 返回该凭据对 date 的期望签名（小写 hex）：缓存的 date 一致
// 时直接复用，否则计算后原子写回。valid-AK 与 dummy 两条路径都经由本方法，
// 计算形状（缓存查找 + date 变化才算 HMAC + 写回）完全一致——时序抹平的
// 论证见 dummyCred 注释。
func (c *credInfo) expectedSig(date string) string {
	if s := c.cache.Load(); s != nil && s.date == date {
		return s.sig
	}
	h := hmac.New(sha1.New, []byte(c.secret))
	h.Write([]byte(date))
	sig := hex.EncodeToString(h.Sum(nil))
	c.cache.Store(&sigCache{date: date, sig: sig})
	return sig
}

// dummyCred 未命中 AK 时用于计算期望签名的占位凭据。它的作用只是让
// "AK 不存在"与"AK 存在但签名错"走完全相同的计算路径，抹平时序差；
// 占位密钥不承担保密职责——未命中时无论签名比对结果如何，found=false
// 都会强制拒绝，攻击者即使预先算出 dummy 的"正确"签名也无法通过。
//
// 时序抹平在缓存引入后依然成立的论证：若只给真实凭据挂缓存，valid-AK
// 的请求会因命中缓存而显著快于 unknown-AK（后者每次全量 HMAC），响应
// 时序即泄露"该 AK 是否存在"。因此 dummy 路径挂**同结构**的全局缓存
// 条目（密钥固定，期望值只依赖 date），两条路径都是「缓存查找 + date
// 变化时才算一次 HMAC + 常数时间比较」——同一秒内二者同为缓存命中，
// 跨秒首个请求二者同为全量计算，时序重新对齐。dummy 缓存为全局共享
// 单条目（而非 per-unknown-AK）：期望值不依赖 AK，一条即够。
var dummyCred = &credInfo{secret: "sq-dummy-secret-for-timing-equalization"}

// verifyAuth 校验 ctx metadata 中的签名。所有失败路径统一返回 Unauthenticated，
// 错误信息不区分"AK 错"与"签名错"——认证错误细节是给攻击者的探针，不外泄。
func verifyAuth(ctx context.Context, byAK map[string]*credInfo, logger *slog.Logger, method string) error {
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
	info, found := byAK[ak]
	if !found {
		info = dummyCred // 走同形状的占位路径抹平时序（见 dummyCred 注释）
	}
	expect := info.expectedSig(dates[0])
	// 官方 SDK 五个语言实现的十六进制大小写并不统一：Go/Java/Python 输出小写
	// （hex.EncodeToString / encodeHexString(...,false) / hexlify），C#/C++ 输出
	// 大写（BitConverter.ToString / MixAll::hex 的 'A'-'F' 字典）。服务端统一
	// 折成小写再比 —— 否则 C#/C++ 客户端开启鉴权后 100% 认证失败，且错误信息
	// 刻意不区分原因，几乎无法自助排查。
	// 对客户端送来的串做小写化不泄露密钥的任何信息，常数时间比较的性质不变。
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
	byAK := make(map[string]*credInfo, len(creds))
	for _, c := range creds {
		byAK[c.AccessKey] = &credInfo{secret: c.SecretKey, name: c.Name}
	}
	l := logger.With("mod", "rpc.auth")
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := verifyAuth(ctx, byAK, l, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := verifyAuth(ss.Context(), byAK, l, info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
	return unary, stream
}
