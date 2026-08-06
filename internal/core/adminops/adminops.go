// Package adminops 提供管理面的成片数据清理（Admin API 删除类操作的 store 落地）。
//
// 职责：
//   - topic 删除后的 msg/alloc/keyidx 区间清理
//   - 订阅组删除后的 cursor/inflight 区间清理
//
// 边界：
//   - 不在消息热路径；不做并发防护——删除期间仍有生产/消费流量时的行为未定义
//     （alloc 计数器被删后并发 Append 会从 0 重新计数）。「删除前先停对应流量」
//     是运维契约，README 与 Admin API 文档写明，代码不为这种边界加锁
//   - 不动注册表（meta 的事）；调用顺序契约见各函数注释
package adminops

import (
	"fmt"
	"log/slog"

	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/store"
)

// PurgeTopicData 清理 topic 的全部消息数据：各队列 msg/ 区间、alloc 计数器、
// keyidx/ 索引，单批原子提交。
//
// 调用顺序契约：先 Purge 再 meta.DeleteTopic。崩溃在两步之间的中间态是
// 「注册表还在、数据已空」——等价于一个空 topic，无害且可重试；反过来会留下
// 永远没人清理的孤儿数据（注册表没了，retention 不再扫它）。
func PurgeTopicData(st *store.Store, tc meta.TopicConfig) error {
	b := st.NewBatch()
	for q := uint32(0); q < tc.Queues; q++ {
		mp := store.MsgQueuePrefix(tc.Name, q)
		b.DeleteRange(mp, store.PrefixUpperBound(mp), nil)
		b.Delete(store.AllocKey(tc.Name, q), nil)
	}
	kp := store.KeyIdxTopicPrefix(tc.Name)
	b.DeleteRange(kp, store.PrefixUpperBound(kp), nil)
	if err := st.Apply(b); err != nil {
		return fmt.Errorf("清理 topic %s 数据: %w", tc.Name, err)
	}
	slog.Default().Info("topic 数据已清理", "mod", "adminops", "topic", tc.Name, "queues", tc.Queues)
	return nil
}

// PurgeGroupData 清理订阅组的 cursor/ 与 inflight/ 全部记录，单批原子提交。
// 调用顺序契约与 PurgeTopicData 同理：先 Purge 再 meta.DeleteGroup。
func PurgeGroupData(st *store.Store, group string) error {
	b := st.NewBatch()
	cp := store.CursorGroupPrefix(group)
	b.DeleteRange(cp, store.PrefixUpperBound(cp), nil)
	ip := store.InflightGroupPrefix(group)
	b.DeleteRange(ip, store.PrefixUpperBound(ip), nil)
	if err := st.Apply(b); err != nil {
		return fmt.Errorf("清理 group %s 数据: %w", group, err)
	}
	slog.Default().Info("消费组数据已清理", "mod", "adminops", "group", group)
	return nil
}
