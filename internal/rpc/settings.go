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

	// pushReceiveBatchSize 下发给 push 消费者的单次取件条数建议值。
	// M1 只验证 SimpleConsumer（批量大小由它自己在请求里带），这个字段是
	// push 消费者才会读的，给一个与 ReceiveMessage 默认批量同量级的值即可。
	pushReceiveBatchSize int32 = 32
)

// defaultBackoffPolicy 服务端下发的重试退避策略：100ms 起步、每次 ×2、封顶 1s，
// 最多尝试 3 次。发布端把它用于发送失败重试，订阅端把它用于消费失败后的重投间隔。
func defaultBackoffPolicy() *pb.RetryPolicy {
	return &pb.RetryPolicy{
		MaxAttempts: backoffMaxAttempts,
		Strategy: &pb.RetryPolicy_ExponentialBackoff{
			ExponentialBackoff: &pb.ExponentialBackoff{
				Initial:    durationpb.New(backoffInitial),
				Max:        durationpb.New(backoffMax),
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
		BackoffPolicy:  defaultBackoffPolicy(),
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
			// AcceptMessageTypes=[NORMAL, DELAY]（M3 起），让客户端在本地就
			// 拒掉顺序/事务消息，比让它发出去再收一个
			// MESSAGE_PROPERTY_CONFLICT_WITH_TYPE 更早、更清楚。
			ValidateMessageType: true,
		}}
	case *pb.Settings_Subscription:
		fifo := false
		batch := pushReceiveBatchSize
		out.PubSub = &pb.Settings_Subscription{Subscription: &pb.Subscription{
			Group:         ps.Subscription.GetGroup(),
			Subscriptions: ps.Subscription.GetSubscriptions(),
			// M1 不支持顺序消费（属 M4），必须显式下发 false，
			// 不能留空让客户端去猜。
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
