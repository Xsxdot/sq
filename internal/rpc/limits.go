// limits.go 定义 gRPC 传输层的消息大小上限。
//
// 职责：
//   - 推导并导出 MaxGRPCMessageSize：cmd/sq/main.go 用它配置
//     grpc.MaxRecvMsgSize/MaxSendMsgSize；Task 13 的端到端测试用它验证
//     「消息体上限 4MB」这条 spec 约束在真实传输链路上确实可达
//
// 边界：
//   - 只定义常量，不做任何运行时逻辑
package rpc

import "github.com/xushixin/sq/internal/core/produce"

// grpcFramingSlack 是 gRPC 消息体在 produce.MaxBodySize 之外额外预留的字节数，
// 用来覆盖 protobuf 帧头（字段 tag + 长度 varint）以及 SystemProperties
// （MessageId/Tag/Keys/BornTimestamp/MessageGroup/ReceiptHandle 等）、
// UserProperties 等系统与用户属性的序列化开销——这些字段都是在 Body 之外
// 额外挂在同一条 gRPC 消息（SendMessageRequest/ReceiveMessageResponse）上的。
//
// 取值 64KiB：真实流量下这些属性字段的编码大小通常在几十到几百字节量级，
// 64KiB 已留出两个数量级以上的余量（对齐 RocketMQ-common 里"帧头/属性开销"
// 惯用的几十到几百 KB 量级），同时相对 4MB 的 MaxBodySize 而言足够小，
// 不会实质性放宽"消息体上限 4MB"这条硬约束——它仍然是决定性因素。
const grpcFramingSlack = 64 * 1024 // 64 KiB

// MaxGRPCMessageSize 是 gRPC Server 端 MaxRecvMsgSize/MaxSendMsgSize 应使用的上限，
// 由 cmd/sq/main.go 在构造 grpc.NewServer 时应用。
//
// 为什么不能用 gRPC-go 的默认值：gRPC-go 默认 MaxRecvMsgSize 是 4 MiB
// （4,194,304 字节），与 produce.MaxBodySize 数值相同。但一条
// SendMessageRequest 序列化后的 gRPC 消息大小 = protobuf 帧开销 +
// Topic/SystemProperties/UserProperties 等字段 + Body；即便 Body 恰好等于
// MaxBodySize，整条 gRPC 消息也必然大于 MaxBodySize（哪怕只是最小化的请求，
// 帧开销也有 ≥24 字节；带上 MessageId/Tag/Keys/BornTimestamp/UserProperties
// 等真实字段后开销更大）。若沿用 gRPC 默认值，一个 body 恰好在文档宣称的
// 4MB 上限的客户端请求，会在到达 SendMessage 应用层校验之前就被 gRPC
// 传输层以 ResourceExhausted 拒绝——没有 pb.Status，也没有
// Code_MESSAGE_BODY_TOO_LARGE，「消息体上限 4MB」这条约束因此实际不可达。
//
// 本常量必须严格大于 produce.MaxBodySize，且余量来自命名的 grpcFramingSlack
// 常量（而不是散落各处的裸魔法数字 4*1024*1024），今后如需调整只改一处。
//
// ReceiveMessage 会把同样大小的 body 流式发回给消费者，所以 MaxSendMsgSize
// 也要用这个值，不能只调大接收方向的 MaxRecvMsgSize。
const MaxGRPCMessageSize = produce.MaxBodySize + grpcFramingSlack
