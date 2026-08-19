// Package query 提供消息检索只读路径（Keys 业务索引查询）。
//
// 职责：
//   - 按 (topic, key) 从 keyidx/ 找回消息，供 M5 Admin API / 控制台复用
//
// 边界：
//   - 只读，不修改任何状态
//   - 索引与消息可能因 retention 清理竞态短暂不一致：消息缺失即跳过，不算错
package query

import (
	"fmt"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/store"
)

// defaultLimit 未指定 limit 时的返回上限（控制台单页量级）。
const defaultLimit = 64

// ByKey 按业务 key 查询 topic 下的消息，按写入时间升序，最多 limit 条（<=0 用 defaultLimit）。
func ByKey(st *store.Store, topic, key string, limit int) ([]*core.Message, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	pfx := store.KeyIdxKeyPrefix(topic, key)
	var out []*core.Message
	err := st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		_, pk, _, q, off, perr := store.ParseKeyIdxKey(k)
		if perr != nil || pk != key {
			// 结构不符或 key 不等值 = 命中了「以本 key 为路径前缀」的其他 key
			//（如查 "a" 扫到 "a/b" 的条目），跳过即可，不是数据损坏
			return true, nil
		}
		raw, ok, gerr := st.Get(store.MsgKey(topic, q, off))
		if gerr != nil {
			return false, gerr
		}
		if !ok {
			return true, nil // 消息已被 retention 清走而索引未清（清理竞态），跳过
		}
		m, derr := core.DecodeMessage(raw)
		if derr != nil {
			return false, derr
		}
		out = append(out, m)
		return len(out) < limit, nil
	})
	if err != nil {
		return nil, fmt.Errorf("按 key 查询 (topic=%s key=%q): %w", topic, key, err)
	}
	return out, nil
}

// Browse 按 (topic, queueID) 从 fromOffset 起顺序读取至多 limit 条消息
// （<=0 用 defaultLimit）。控制台"按 topic 浏览"与 DLQ 查看共用此路径。
// 越界/空队列返回空切片不报错——翻页到底是正常形态，不是错误。
func Browse(st *store.Store, topic string, queueID uint32, fromOffset uint64, limit int) ([]*core.Message, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	lower := store.MsgKey(topic, queueID, fromOffset)
	upper := store.PrefixUpperBound(store.MsgQueuePrefix(topic, queueID))
	var out []*core.Message
	err := st.Scan(lower, upper, limit, func(k, v []byte) (bool, error) {
		m, derr := core.DecodeMessage(v)
		if derr != nil {
			return false, derr
		}
		out = append(out, m)
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("浏览队列 (topic=%s q=%d from=%d): %w", topic, queueID, fromOffset, err)
	}
	return out, nil
}
