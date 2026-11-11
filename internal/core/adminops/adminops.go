// Package adminops 提供管理面的成片数据清理（Admin API 删除类操作的 store 落地）。
//
// 职责：
//   - topic 删除后的 msg/alloc/keyidx 区间清理
//   - 订阅组删除后的 cursor/inflight 区间清理
//   - 集群档按 raft 组拆批：每批只含同组键族，本节点 leader 的组本地
//     rep.Apply 提交，非 leader 的组经 fwd 转发给该组 leader 提案；消费状态
//     （cursor/、inflight/）批次的本地提交改经 deliver 的管理入口
//     （ConsumeStateAdmin），先失效 leader 内存态再提交（卡 SQ4.1）
//
// 边界：
//   - 跨组拆批无批间原子性：任一队列/桶先清、另一队列/桶后清，中间崩溃留下
//     「部分已清」的残留态。清理操作幂等（DeleteRange/Delete 重放无害），
//     残留由重复执行（重发删除请求）清净——不引入跨组事务
//   - 不在消息热路径；不做并发防护——删除期间仍有生产/消费流量时的行为未定义
//     （alloc 计数器被删后并发 Append 会从 0 重新计数）。「删除前先停对应流量」
//     是运维契约，README 与 Admin API 文档写明，代码不为这种边界加锁
//   - 不动注册表（meta 的事）；调用顺序契约见各函数注释
package adminops

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

// ConsumeStateAdmin 消费状态管理批次的提交入口（deliver.Deliverer 实现；
// 使用方定义的接口，本包不 import deliver 具体类型）。批次触及的队列内存态
// 先失效再提交，见 deliver.ApplyAdminBatch。
type ConsumeStateAdmin interface {
	ApplyAdminBatch(ctx context.Context, g uint32, b *store.Batch) error
}

// TopicLister 注册表里的全部 topic（*meta.Meta 实现）。
type TopicLister interface {
	Topics() []meta.TopicConfig
}

// PurgeTopicData 清理 topic 的全部消息数据：各队列 msg/ 区间、alloc 计数器、
// keyidx/ 索引。logger 为注入式应用日志器（本包内部 With("mod", "adminops")）。
//
// 集群档按队列拆批：每个队列一个批次（该队列 msg/ DeleteRange + alloc/
// Delete，同组键族同批原子），keyidx 段按 key 内 queueID 分桶、每桶一个批次。
// 批次的「本节点 leader ? 本地 rep.Apply : 转发给 leader」分派与回收统一走
// replication.ApplyOrForward——topic 的所有数据都必须清掉，转发不是可选项。
//
// 调用顺序契约：先 Purge 再 meta.DeleteTopic。崩溃在两步之间的中间态是
// 「注册表还在、数据已空」——等价于一个空 topic，无害且可重试；反过来会留下
// 永远没人清理的孤儿数据（注册表没了，retention 不再扫它）。
func PurgeTopicData(ctx context.Context, rep replication.Replicator, rt replication.Router,
	fwd replication.Forwarder, st *store.Store, tc meta.TopicConfig, logger *slog.Logger) error {
	logger = logger.With("mod", "adminops")
	for q := uint32(0); q < tc.Queues; q++ {
		b := st.NewBatch()
		mp := store.MsgQueuePrefix(tc.Name, q)
		b.DeleteRange(mp, store.PrefixUpperBound(mp))
		b.Delete(store.AllocKey(tc.Name, q))
		g := rt.GroupForQueue(tc.Name, q)
		if err := replication.ApplyOrForward(ctx, rep, rt, fwd, g, b, logger); err != nil {
			return fmt.Errorf("清理 topic %s q%d 数据: %w", tc.Name, q, err)
		}
	}
	// keyidx 段：同 retention 的分桶语义，但非 leader 桶也必须清（转发）。
	buckets := map[uint32]*store.Batch{}
	kp := store.KeyIdxTopicPrefix(tc.Name)
	if err := st.Scan(kp, store.PrefixUpperBound(kp), 0, func(k, v []byte) (bool, error) {
		qid, perr := store.ParseKeyIdxQueueID(k)
		if perr != nil {
			return false, perr
		}
		b, ok := buckets[qid]
		if !ok {
			b = st.NewBatch()
			buckets[qid] = b
		}
		b.Delete(k) // Batch 编码时即拷贝 key，回调切片可直接用
		return true, nil
	}); err != nil {
		for _, b := range buckets {
			b.Close()
		}
		return fmt.Errorf("扫描 keyidx %s: %w", tc.Name, err)
	}
	submitted := map[uint32]bool{}
	for qid, b := range buckets {
		if err := replication.ApplyOrForward(ctx, rep, rt, fwd, rt.GroupForQueue(tc.Name, qid), b, logger); err != nil {
			// 错误路径回收：只 Close 未提交的桶。已提交的桶被
			// ApplyOrForward 消费并 Close（Replicator 契约负责其生命周期），
			// 再 Close 一次是与 pebble 批次池别名同类的手续疏漏（今天
			// ErrClosed 被忽略故无害，是隐患——I6）。当前失败桶按 store
			// 契约丢给 GC（Apply 失败不得 Close，见 store.Apply 注释）。
			for oq, ob := range buckets {
				if oq != qid && !submitted[oq] {
					ob.Close()
				}
			}
			return fmt.Errorf("清理 keyidx %s q%d: %w", tc.Name, qid, err)
		}
		submitted[qid] = true
	}
	logger.Info("topic 数据已清理", "topic", tc.Name, "queues", tc.Queues, "idx_buckets", len(buckets))
	return nil
}

// PurgeGroupData 清理订阅组的 cursor/ 与 inflight/ 全部记录。
// logger 为注入式应用日志器（同 PurgeTopicData）。
//
// 枚举 (topic,qid)：pebble 的 cursor/{group}/ 与 inflight/{group}/ 两族扫描
// （只扫 cursor 会漏掉「有 inflight 无 cursor」的队列）∪ 注册表全部 topic×队列
// （消费状态的权威在 leader 内存，可能有尚未落盘的队列，卡 SQ4.1）。逐
// (topic,qid) 一个批次（该队列两个前缀的 DeleteRange，同组键族同批）。
//
// 提交（计划 P2）：消费状态批次必须经 deliver 的管理入口——本节点是该组
// leader 时 cs.ApplyAdminBatch（取队列锁、失效内存态、同步提交）；否则转发给
// 该组 leader，leader 侧控制处理器同样改走 ApplyAdminBatch。直接 rep.Apply
// 会被未失效的内存态在下一次提交时写回。
//
// 调用顺序契约与 PurgeTopicData 同理：先 Purge 再 meta.DeleteGroup。
func PurgeGroupData(ctx context.Context, rep replication.Replicator, rt replication.Router,
	fwd replication.Forwarder, st *store.Store, cs ConsumeStateAdmin, tl TopicLister, group string, logger *slog.Logger) error {
	logger = logger.With("mod", "adminops")
	type tq struct {
		topic string
		qid   uint32
	}
	set := map[tq]struct{}{}
	collect := func(prefix []byte, parse func(k []byte) (string, uint32, error)) error {
		return st.Scan(prefix, store.PrefixUpperBound(prefix), 0, func(k, v []byte) (bool, error) {
			topic, qid, perr := parse(k)
			if perr != nil {
				return false, perr
			}
			set[tq{topic, qid}] = struct{}{}
			return true, nil
		})
	}
	cp := store.CursorGroupPrefix(group)
	if err := collect(cp, func(k []byte) (string, uint32, error) {
		return store.ParseCursorTopicQueue(k)
	}); err != nil {
		return fmt.Errorf("扫描 group %s 位点: %w", group, err)
	}
	ip := store.InflightGroupPrefix(group)
	if err := collect(ip, func(k []byte) (string, uint32, error) {
		_, topic, qid, _, perr := store.ParseInflightKey(k)
		return topic, qid, perr
	}); err != nil {
		return fmt.Errorf("扫描 group %s inflight: %w", group, err)
	}
	onDisk := len(set)
	for _, tc := range tl.Topics() {
		for q := uint32(0); q < tc.Queues; q++ {
			set[tq{tc.Name, q}] = struct{}{}
		}
	}
	for k := range set {
		b := st.NewBatch()
		ck := store.CursorKey(group, k.topic, k.qid)
		b.DeleteRange(ck, store.PrefixUpperBound(ck))
		ik := store.InflightPrefix(group, k.topic, k.qid)
		b.DeleteRange(ik, store.PrefixUpperBound(ik))
		g := rt.GroupForQueue(k.topic, k.qid)
		var err error
		if rt.IsLeader(g) {
			err = cs.ApplyAdminBatch(ctx, g, b)
		} else {
			err = forwardBatch(ctx, fwd, g, b, logger)
		}
		if err != nil {
			return fmt.Errorf("清理 group %s %s/q%d 数据: %w", group, k.topic, k.qid, err)
		}
	}
	logger.Info("消费组数据已清理", "group", group, "queues", len(set), "queues_on_disk", onDisk)
	return nil
}

// forwardBatch 把批次转发给 g 组 leader 提案（批次随即回收）。与
// replication.ApplyOrForward 的转发分支同款；不复用它是因为它在本节点为
// leader 时会走 rep.Apply 直接提交，绕过消费状态管理入口。
func forwardBatch(ctx context.Context, fwd replication.Forwarder, g uint32, b *store.Batch, logger *slog.Logger) error {
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil {
		logger.Warn("转发前回收批次失败（字节已取出，不阻断转发）", "g", g, "err", err)
	}
	if err := fwd.ForwardApply(ctx, g, repr); err != nil {
		logger.Warn("转发消费状态管理批次失败", "g", g, "bytes", len(repr), "err", err)
		return err
	}
	logger.Info("跨节点转发消费状态管理批次", "g", g, "bytes", len(repr))
	return nil
}
