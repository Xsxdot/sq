// Telemetry 会话注册表：记录完成 Settings 协商的双向流，为事务回查提供
// 下发通道，为 metrics/控制台提供连接数（M5 递延项）。
//
// 职责：
//   - 会话生命周期：Settings 协商成功即注册，流结束即注销
//   - pickProducer：按 topic 严格匹配发布方会话（轮转均衡）
//   - session.send：同一条流上的并发写序列化（grpc-go 禁止并发 SendMsg）
//
// 边界：
//   - 不理解回查语义（txn 的事），只是「找到一条能写的流并写进去」
//   - 不做会话保活/超时（流断开由 gRPC 通知，Telemetry handler 退出时注销）
package rpc

import (
	"sort"
	"sync"

	pb "github.com/Xsxdot/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// session 一条已完成 Settings 协商的 Telemetry 流。
type session struct {
	id int // 注册序，pickProducer 轮转的稳定排序键
	// 为什么是自增 id 而不是指针地址：指针地址只是恰好稳定（当前 GC 不移动
	// 堆对象），不是语言规范承诺的——自增 id 与地址一样单调，且语义明确
	stream pb.MessagingService_TelemetryServer
	// sendMu 串行化本流上的所有服务端写：Settings 回包（handler goroutine）
	// 与回查命令（checker goroutine）可能并发，grpc-go 明确禁止同一流并发
	// SendMsg，漏了这把锁就是数据竞争加流损坏
	sendMu     sync.Mutex
	clientType pb.ClientType
	// clientID 客户端实例标识（x-mq-client-id）。自动续租靠它判定
	// 「持有这条 inflight 的客户端是否还活着」。可能为空（手写客户端未带头），
	// 空值不入 byClient 索引。
	clientID string
	topics   map[string]bool // producer 声明发布的 topics（来自 Settings.Publishing.Topics）
}

// send 向该会话写一条命令（并发安全）。
func (se *session) send(cmd *pb.TelemetryCommand) error {
	se.sendMu.Lock()
	defer se.sendMu.Unlock()
	return se.stream.Send(cmd)
}

// sessions 会话注册表。并发安全。
type sessions struct {
	mu     sync.Mutex
	all    map[*session]struct{}
	next   int // pickProducer 轮转游标
	nextID int // 下一注册序（add 时递增，见 session.id）

	// byClient 客户端标识 → 该标识当前活跃的会话数。
	//
	// 用计数而不是 map[string]*session：同一个 client id 理论上可以并存多条
	// Telemetry 流（客户端重连时新旧流在一小段窗口内共存）。用指针会让后注册
	// 的覆盖先注册的，随后先注册的那条注销时把整个条目删掉——客户端明明活着
	// 却被判为已死，它手上正在处理的消息会被立刻重投。
	byClient map[string]int
}

func newSessions() *sessions {
	return &sessions{all: map[*session]struct{}{}, byClient: map[string]int{}}
}

func (ss *sessions) add(se *session) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	se.id = ss.nextID
	ss.nextID++
	ss.all[se] = struct{}{}
	if se.clientID != "" {
		// 空 id 不入索引：所有未带头的客户端会挤在同一个桶里，
		// 任一条流活着就让全体被判为存活，等于让续租判据失效
		ss.byClient[se.clientID]++
	}
}

func (ss *sessions) remove(se *session) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.all, se)
	if se.clientID != "" {
		if n := ss.byClient[se.clientID] - 1; n > 0 {
			ss.byClient[se.clientID] = n
		} else {
			delete(ss.byClient, se.clientID)
		}
	}
}

func (ss *sessions) count() int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return len(ss.all)
}

// aliveClient 判定某客户端标识当前是否仍有活跃的 Telemetry 会话。
//
// 供自动续租的判据 4 使用（见 auto-renew spec §6）。空 id 恒为 false。
//
// 固有窗口：Telemetry 流断开到重连之间会判为 false，于是消息被重投——
// 这恰好是改造前的无条件行为，不是回归，只是那一刻少续了一次租。反向误判
// （进程已死但 gRPC 尚未感知流断）由 InflightState.RenewUntilMs 硬上限兜底。
func (ss *sessions) aliveClient(id string) bool {
	if id == "" {
		return false
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.byClient[id] > 0
}

// updateTopics 全量替换某 producer 会话的 topics 声明（SDK 周期性重发
// Settings，topic 列表会增长）。
//
// 必须在 sessions 锁内进行：handler goroutine 在这里写 topics，checker
// goroutine 的 pickProducer 同时读它——Go map 非原子，就算整体替换 map
// 引用也防不住读写并发，二者必须经 ss.mu 互斥。
func (ss *sessions) updateTopics(se *session, fresh map[string]bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	se.topics = fresh
}

// pickProducer 挑一条发布过 topic 的 producer 会话（多条时轮转）。
//
// 严格按 topic 匹配、绝不降级到任意 producer：回查是把消息交给客户端的
// TransactionChecker 做决断，发给没发布过该 topic 的进程，它的 checker 面对
// 陌生事务多半回 ROLLBACK/UNKNOWN——等于让无关方替业务做了错误决定。
// 找不到匹配会话时返回 nil，由调度器改期后下轮再试（producer 重连即恢复）。
//
// 轮转必须是「稳定顺序上的轮转」：matches 若按 map 随机迭代序收集，两次
// 调用得到相同顺序的概率约 1/2，此时 next 游标前进也照样返回同一个会话，
// 轮转形同虚设。按注册序（add 时分配的 id）排序后再走游标，才保证相邻两次
// 调用一定不同。
func (ss *sessions) pickProducer(topic string) *session {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	var matches []*session
	for se := range ss.all {
		if se.clientType == pb.ClientType_PRODUCER && se.topics[topic] {
			matches = append(matches, se)
		}
	}
	if len(matches) == 0 {
		return nil
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].id < matches[j].id
	})
	se := matches[ss.next%len(matches)]
	ss.next++
	return se
}
