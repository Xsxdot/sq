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

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/store"
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
