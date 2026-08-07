// sessions_test.go 覆盖 Telemetry 会话注册表（sessions.go）的纯注册表逻辑：
// 生命周期计数、pickProducer 的严格 topic 匹配与轮转。不启 gRPC。
//
// 职责：
//   - 验证会话 add/remove/count 的生命周期口径
//   - 验证 pickProducer 只匹配发布过该 topic 的 producer，绝不降级
//   - 验证多 producer 场景下按轮转选择（不永远打同一个）
//
// 边界：
//   - 不覆盖 stream 写入本身（send 的串行化由 handler 侧 + race 测试覆盖）
//   - 不覆盖 Telemetry handler 的注册/注销挂接（那是 server_test.go 的职责）
package rpc

import (
	"testing"

	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

func producerSession(topics ...string) *session {
	ts := map[string]bool{}
	for _, t := range topics {
		ts[t] = true
	}
	return &session{clientType: pb.ClientType_PRODUCER, topics: ts}
}

func TestSessionsCountAndRemove(t *testing.T) {
	ss := newSessions()
	a, b := producerSession("t1"), &session{clientType: pb.ClientType_SIMPLE_CONSUMER}
	ss.add(a)
	ss.add(b)
	if ss.count() != 2 {
		t.Fatalf("count = %d", ss.count())
	}
	ss.remove(a)
	if ss.count() != 1 {
		t.Fatalf("remove 后 count = %d", ss.count())
	}
}

func TestPickProducerMatchesTopicStrictly(t *testing.T) {
	ss := newSessions()
	ss.add(producerSession("t1"))
	ss.add(&session{clientType: pb.ClientType_SIMPLE_CONSUMER, topics: map[string]bool{"t2": true}})
	if got := ss.pickProducer("t1"); got == nil {
		t.Fatal("发布 t1 的 producer 应被选中")
	}
	// 只有 consumer 声明过 t2：不能退而求其次拿 consumer 或不相关 producer——
	// 回查命令发给没发布过该 topic 的客户端，其 checker 会对陌生事务做出决断
	if got := ss.pickProducer("t2"); got != nil {
		t.Fatal("无匹配 producer 时必须返回 nil")
	}
}

func TestPickProducerRotates(t *testing.T) {
	ss := newSessions()
	a, b := producerSession("t1"), producerSession("t1")
	ss.add(a)
	ss.add(b)
	first := ss.pickProducer("t1")
	second := ss.pickProducer("t1")
	if first == second {
		t.Fatal("多 producer 时应轮转，不能永远打同一个")
	}
	// wrap-around：注册序排序 + 游标取模，第三次调用必须回到第一个会话。
	// 若排序键不稳定（如按指针地址），无法保证这个「回到起点」的确定性
	third := ss.pickProducer("t1")
	if third != first {
		t.Fatalf("轮转应 wrap-around 回到第一个会话，得到 %p（first=%p）", third, first)
	}
}
