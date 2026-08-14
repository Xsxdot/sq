// 客户端标识提取：从 gRPC metadata 取 x-mq-client-id。
//
// 职责：
//   - 把「这个请求来自哪个客户端实例」这一事实从传输层取出来，供自动续租
//     判定持有者存活使用
//
// 边界：
//   - 不校验、不拒绝：取不到就是取不到，由调用方决定退化行为。手写客户端
//     不带这个头是合法的，它只是享受不到自动续租，不该被拒绝服务
//   - 不做缓存：metadata 取值是 map 查找，成本远低于一次缓存失效判断
package rpc

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// clientIDHeaderKey 客户端实例标识头。这是协议内置约定而非自造：官方 SDK 在
// 每个出站请求（含 Telemetry 流、含未开鉴权时）都附带它，protobuf 里还有
// 专门的错误码注释「Request is rejected due to missing of x-mq-client-id header」。
const clientIDHeaderKey = "x-mq-client-id"

// clientIDFrom 从入站 ctx 的 metadata 取客户端标识。
//
// 取不到（无 metadata / 无该头 / 头值为空）一律返回空串，调用方据此退化为
// 「不启用续租」。返回空串不是错误，不打日志——ReceiveMessage 是热路径，
// 手写客户端每次轮询都会命中这条分支，打日志即刷屏。
func clientIDFrom(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vs := md.Get(clientIDHeaderKey)
	if len(vs) == 0 {
		return ""
	}
	return vs[0]
}
