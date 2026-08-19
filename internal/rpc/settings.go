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

	"github.com/Xsxdot/sq/internal/core/produce"
	pb "github.com/Xsxdot/sq/internal/rpc/pb/apache/rocketmq/v2"
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

// publishBackoffPolicy 下发给**发布端**的重试策略。
//
// SDK 用它做 RPC 级的发送失败重试（producer_options.go 读
// Settings.BackoffPolicy，producer 发送失败时按它退避重发）。按部署形态分档：
//   - 单机档：100ms 起步、每次 ×2、封顶 1s，最多尝试 3 次
//   - 集群档：同上倍率，封顶 3s、最多 5 次——选举窗口实测 1.5s 量级，
//     单机档的 1s×3 次恰好可能全部落在窗口内（见常量注释）
//
// 值与拆分前的 backoffPolicy 逐字段相同：发布端行为不得因本次拆分有任何变化。
func (s *Server) publishBackoffPolicy() *pb.RetryPolicy {
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

// subscribeBackoffPolicy 下发给**订阅端**的重试策略。
//
// 与发布端同名字段、完全不同的语义：SDK 用它做消息级处理——
// push_consumer_options.go 读 Settings.BackoffPolicy 存进 pc.retryPolicy，
// nackMessage 用它算重投的不可见时长、eraseFifoMessage 用它判死信。
// 两个用途分属两类客户端，而 Settings 是逐客户端协商的，所以同一个字段本来
// 就可以取不同值——这正是本函数存在的理由。
//
// 参数：
//   - group: 该订阅端的消费组名（可能为空串：客户端未上报组）
//
// **本函数与 publishBackoffPolicy 只差 MaxAttempts 一个字段。**
// 退避三要素（Initial/Max/Multiplier）刻意与发布端逐字段相同，**不是可以合并
// 掉的重复代码**：
//   - 曾有一版设计主张把它们改成 broker 的 retryBackoff（10s / 5min），理由是
//     「客户端算出的不可见时长反正会被 broker 的退避下限覆盖」。**那个前提是
//     错的**：SDK 消费失败走 ChangeInvisibleDuration，broker 侧
//     deliver.ChangeInvisible 是 ExpireAtMs = now + invisible，原样透传、一处
//     下限都没有；Receive 路径那条 max(客户端要求, 退避下限) 只用于投递时回填
//     InvisibleDuration 字段，且判据带 MessageGroup == ""（顺序消息被排除）。
//   - 真改了的后果：push 消费失败的重投从 100ms 变成 10s；顺序消息在反复失败时
//     最坏把队列队头阻塞 5 分钟（顺序锁下每队列至多一条未终结 inflight）。
//   - 详见 spec §2.2 与 §6-A4。settings_test.go 的 T2 把这条钉成红灯。
//
// 注意：本函数只读，不得建组。协商是协议握手，不该有持久化副作用；
// 真正建组由 ReceiveMessage 路径的 EnsureGroup 负责。
func (s *Server) subscribeBackoffPolicy(group string) *pb.RetryPolicy {
	// 回退值取配置而不是 meta.DefaultMaxAttempts：后者是 meta 包的兜底常量
	// （16），而组的实际默认值由 main 从 cfg.DefaultMaxAttempts 传进 meta.New。
	// 用包常量会在用户改过配置时下发一个谁也没配过的 16。
	attempts := s.cfg.DefaultMaxAttempts
	if gc, ok := s.mt.GetGroup(group); ok {
		attempts = gc.EffectiveMaxAttempts()
	} else {
		// 组还没注册（首次连接，还没走到 ReceiveMessage）时下发配置默认值。
		// 这条是「为什么这个消费者拿到的是默认值而不是它组里配的值」唯一的
		// 排查线索，故必打；用 Debug 是因为每个消费者首次连接都会命中一次，
		// Info 会把它变成刷屏噪声。
		s.logger.Debug("协商退避策略：消费组尚未注册，回退配置默认 maxAttempts",
			"group", group, "max_attempts", attempts)
	}
	max := backoffMax
	if s.cfg.ClusterEnabled() {
		max = backoffMaxCluster
	}
	return &pb.RetryPolicy{
		MaxAttempts: attempts,
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
		RequestTimeout: client.GetRequestTimeout(),
		UserAgent:      client.GetUserAgent(),
	}
	// BackoffPolicy 不在公共段赋值：它的语义由客户端类型决定（发布端是 RPC
	// 级发送重试次数，订阅端是消息级死信判据），放在公共段等于给两类客户端
	// 下发同一份逐字节相同的值，其中 MaxAttempts 必有一份是错的。
	// 客户端未上报可识别的 PubSub 时它也一并留空——与下面 PubSub 留空同理：
	// 连客户端类型都判不出来，退避策略的语义就无从选择，服务端不凭空捏造。
	switch ps := client.GetPubSub().(type) {
	case *pb.Settings_Publishing:
		out.BackoffPolicy = s.publishBackoffPolicy()
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
		out.BackoffPolicy = s.subscribeBackoffPolicy(ps.Subscription.GetGroup().GetName())
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
