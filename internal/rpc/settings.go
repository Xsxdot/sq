// Telemetry Settings 协商：由客户端上报的 Settings 构造服务端下发的 Settings。
//
// 职责：
//   - 填充「必须由服务端告知客户端」的字段（发布端消息体上限、重试退避策略、
//     订阅端长轮询上限等），客户端自报的字段（topics/subscriptions/UA）原样带回
//   - 按客户端类型选择 PubSub 分支：发布端必须收到 Publishing，订阅端必须收到
//     Subscription，否则官方 SDK 直接判为 "[bug] Issued settings not match with
//     the client type" 并让握手失败
//
// 边界：
//   - 不做任何鉴权/配额决策，只是把服务端已有的静态能力上限翻译成协议字段
//   - 不下发 Metric（不启用客户端指标上报）：sq M1 没有指标采集端点，
//     下发一个指向空地址的采集配置只会让客户端反复重连失败
//
// 为什么不能像最初实现那样把客户端的 Settings 原样回发：
// 官方 Go SDK 的 producerSettings.applySettingsCommand 会无条件执行
// `maxBodySizeBytes.Store(v.Publishing.GetMaxBodySize())`，而客户端自己上报的
// Publishing 里根本没有 max_body_size（那是服务端字段，客户端留空）。原样回发
// 等于把客户端的消息体上限改写成 0，此后每一次 Send 都在客户端本地就被拒绝：
// "message body size exceeds the threshold, max size=0 bytes"，请求根本到不了
// 服务端。这个缺陷只有真实 SDK 能暴露——裸 protobuf stub 不会执行这段协商逻辑。
package rpc

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/xushixin/sq/internal/core/produce"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

const (
	// backoffInitial 首次重试前的等待时长。
	backoffInitial = 100 * time.Millisecond
	// backoffMax 重试等待时长的上限（指数退避封顶）。
	backoffMax = time.Second
	// backoffMultiplier 每次重试的退避倍率。
	backoffMultiplier = 2.0
	// backoffMaxAttempts 单次请求最多尝试次数（含首次）。
	// 与官方 SDK 的默认值一致，避免协商后反而放大/收紧客户端的重试面。
	backoffMaxAttempts = 3

	// backoffMaxCluster 集群档的退避封顶与尝试次数。
	// 依据：选举窗口实测 1.5s 量级，单机档默认的 1s×3 次恰好可能全部落在
	// 窗口内——每次重试都被 HA_NOT_AVAILABLE 弹回，3 次烧完还没等到 leader；
	// 3s 封顶 × 5 次（100ms→200ms→400ms→800ms→3s）累计覆盖约 4.5s，
	// 足够跨过窗口。
	backoffMaxCluster         = 3 * time.Second
	backoffMaxAttemptsCluster = 5

	// pushReceiveBatchSize 下发给 push 消费者的单次取件条数建议值。
	// M1 只验证 SimpleConsumer（批量大小由它自己在请求里带），这个字段是
	// push 消费者才会读的，给一个与 ReceiveMessage 默认批量同量级的值即可。
	pushReceiveBatchSize int32 = 32
)

// backoffPolicy 服务端下发的重试退避策略，按部署形态分档：
//   - 单机档：100ms 起步、每次 ×2、封顶 1s，最多尝试 3 次
//   - 集群档：同上倍率，封顶 3s、最多 5 次——选举窗口实测 1.5s 量级，
//     单机档的 1s×3 次恰好可能全部落在窗口内（见常量注释）
//
// 发布端把它用于发送失败重试，订阅端把它用于消费失败后的重投间隔。
func (s *Server) backoffPolicy() *pb.RetryPolicy {
	max, attempts := backoffMax, backoffMaxAttempts
	if s.cfg.ClusterEnabled() {
		max, attempts = backoffMaxCluster, backoffMaxAttemptsCluster
	}
	return &pb.RetryPolicy{
		MaxAttempts: int32(attempts),
		Strategy: &pb.RetryPolicy_ExponentialBackoff{
			ExponentialBackoff: &pb.ExponentialBackoff{
				Initial:    durationpb.New(backoffInitial),
				Max:        durationpb.New(max),
				Multiplier: backoffMultiplier,
			},
		},
	}
}

// negotiateSettings 由客户端上报的 Settings 构造服务端下发的 Settings。
//
// 参数：
//   - client: 客户端在 Telemetry 流上报的 Settings，可能为 nil
//
// 返回：
//   - 下发给该客户端的 Settings。PubSub 分支与客户端类型严格对应；
//     客户端未上报 PubSub（或上报了本版本不认识的分支）时，返回的 PubSub
//     也保持为空——此时协商注定失败，但失败原因应当由客户端按自己的规则
//     报出来，服务端不能凭空捏造一个分支去掩盖它。
func (s *Server) negotiateSettings(client *pb.Settings) *pb.Settings {
	ct := client.GetClientType()
	out := &pb.Settings{
		ClientType:     &ct,
		AccessPoint:    s.endpoints(),
		BackoffPolicy:  s.backoffPolicy(),
		RequestTimeout: client.GetRequestTimeout(),
		UserAgent:      client.GetUserAgent(),
	}
	switch ps := client.GetPubSub().(type) {
	case *pb.Settings_Publishing:
		out.PubSub = &pb.Settings_Publishing{Publishing: &pb.Publishing{
			// Topics 是客户端自报字段，原样带回：客户端不读它，但回包出现在
			// 日志/抓包里时能一眼看出这次协商对应哪个发布端。
			Topics:      ps.Publishing.GetTopics(),
			MaxBodySize: produce.MaxBodySize,
			// 开启客户端侧消息类型校验：sq 的 QueryRoute 通告
			// AcceptMessageTypes=[NORMAL, DELAY, FIFO, TRANSACTION]（M6 起
			// 全类型开放），ValidateMessageType 仍开启以拦截未知类型
			ValidateMessageType: true,
		}}
	case *pb.Settings_Subscription:
		fifo := false
		batch := pushReceiveBatchSize
		out.PubSub = &pb.Settings_Subscription{Subscription: &pb.Subscription{
			Group:         ps.Subscription.GetGroup(),
			Subscriptions: ps.Subscription.GetSubscriptions(),
			// 显式下发 false，且这是终态，不是待翻转的临时值（B13.2 已验）。
			//
			// 三条理由，缺一不可：
			//  1. 顺序安全不依赖此标志：M4 起队列级顺序锁保证每队列至多一条
			//     未终结的顺序 inflight（deliver.go 顺序锁）。e2e
			//     TestOfficialGoSDKPushFIFOOrderLock 用 4 线程 push 消费 20 条
			//     同组消息，实测 listener 并发峰值恒为 1。
			//  2. 翻成 true 会夺走归属权：客户端会改建 FiFoConsumeService，消费
			//     失败转为【客户端本地循环重投 listener】，不回 broker——重试
			//     计数与死信判定都会从 broker 挪到客户端，与 sq 的设计相反。
			//     e2e TestOfficialGoSDKPushRetryOwnedByBroker /
			//     TestOfficialGoSDKPushDLQOwnedByBroker 证的就是当前归属在 broker。
			//  3. 翻成 true 本身就不保证被可靠观测：SDK 的 Start() 读
			//     pcSettings.isFifo（push_consumer.go:379）与 telemetry 回调
			//     applySettingsCommand 写同一字段（push_consumer_options.go:226）
			//     是 v5.1.4 内部的真实竞态，两者并发、无同步。即使下发 true，
			//     消费服务选型（Standard vs FiFo）也会因时序变得不确定。
			//
			// 仍必须显式下发而不是留空：留空让客户端去猜，协商结果不确定。
			Fifo:             &fifo,
			ReceiveBatchSize: &batch,
			// 下发服务端真实的长轮询上限，而不是回显客户端自报的值：
			// ReceiveMessage 端最长就等 defaultLongPolling，客户端按更大的值
			// 设置请求超时只会白等。
			LongPollingTimeout: durationpb.New(defaultLongPolling),
		}}
	}
	return out
}
