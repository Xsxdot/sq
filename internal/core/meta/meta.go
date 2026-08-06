// Package meta 维护 topic 与订阅组注册表。
//
// 职责：
//   - topic/group 配置的创建、查询、启动时从 store 加载全量内存缓存
//   - 名字合法性校验（key 编码安全的第一道门）
//
// 边界：
//   - 不管队列内容与位点（produce/deliver 的事）
//   - M1 无删除与配置修改（M5 Admin API 再加）
package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/xushixin/sq/internal/store"
)

// ErrTopicNotFound topic 不存在且未开自动创建。
var ErrTopicNotFound = errors.New("topic 不存在")

// ErrBadName 名字不符合 ^[A-Za-z0-9_%\-]{1,127}$。
var ErrBadName = errors.New("名字含非法字符或长度超限")

// ErrGroupNotFound 订阅组不存在（Admin API 删除/查询用）。
var ErrGroupNotFound = errors.New("订阅组不存在")

// DefaultMaxAttempts 订阅组默认最大投递次数（超过即转 DLQ），与 RocketMQ 默认一致。
const DefaultMaxAttempts = 16

// DefaultRetentionMs 默认消息保留时长：3 天（spec §5 流程 7）。
const DefaultRetentionMs int64 = 3 * 24 * 60 * 60 * 1000

const dlqPrefix = "%DLQ%"

// DLQTopicName 消费组对应的死信 topic 名。'%' 在名字合法字符集内，
// 死信 topic 是普通 topic（可用 SDK 直接消费、控制台可查可重发）。
func DLQTopicName(group string) string { return dlqPrefix + group }

var nameRe = regexp.MustCompile(`^[A-Za-z0-9_%\-]{1,127}$`)

// ValidateName 校验 topic/group 名。合法字符不含 '/'，保证 key 编码分隔安全。
func ValidateName(s string) error {
	if !nameRe.MatchString(s) {
		return fmt.Errorf("%w: %q", ErrBadName, s)
	}
	return nil
}

// TopicConfig topic 配置。
type TopicConfig struct {
	Name        string `json:"name"`
	Queues      uint32 `json:"queues"`
	CreatedAtMs int64  `json:"created_at_ms"`
	RetentionMs int64  `json:"retention_ms,omitempty"`
}

// EffectiveRetention 生效的消息保留时长。0 表示 M1 旧配置，回退包默认。
func (t TopicConfig) EffectiveRetention() time.Duration {
	if t.RetentionMs <= 0 {
		return time.Duration(DefaultRetentionMs) * time.Millisecond
	}
	return time.Duration(t.RetentionMs) * time.Millisecond
}

// GroupConfig 订阅组配置。M1 仅注册身份；M2 增 maxAttempts（0 = M1 旧数据，回退包默认）。
type GroupConfig struct {
	Name        string `json:"name"`
	CreatedAtMs int64  `json:"created_at_ms"`
	MaxAttempts int32  `json:"max_attempts,omitempty"`
}

// EffectiveMaxAttempts 生效的最大投递次数。0 表示 M1 时期落盘的旧配置，回退包默认。
func (g GroupConfig) EffectiveMaxAttempts() int32 {
	if g.MaxAttempts <= 0 {
		return DefaultMaxAttempts
	}
	return g.MaxAttempts
}

// Meta topic/group 注册表。读多写少，读走内存缓存，写穿透到 store。
type Meta struct {
	st                 *store.Store
	autoCreate         bool
	defaultQueues      uint32
	defaultMaxAttempts int32
	logger             *slog.Logger

	mu     sync.RWMutex
	topics map[string]TopicConfig
	groups map[string]GroupConfig
}

// New 构造并从 store 加载全部已有配置。
// defaultMaxAttempts<=0 时使用 DefaultMaxAttempts（防御配置层漏校验）。
func New(st *store.Store, autoCreate bool, defaultQueues uint32, defaultMaxAttempts int32, logger *slog.Logger) (*Meta, error) {
	if defaultMaxAttempts <= 0 {
		defaultMaxAttempts = DefaultMaxAttempts
	}
	m := &Meta{
		st: st, autoCreate: autoCreate, defaultQueues: defaultQueues, defaultMaxAttempts: defaultMaxAttempts,
		logger: logger.With("mod", "meta"),
		topics: map[string]TopicConfig{}, groups: map[string]GroupConfig{},
	}
	err := st.Scan([]byte(store.TopicMetaPrefix), store.PrefixUpperBound([]byte(store.TopicMetaPrefix)), 0,
		func(k, v []byte) (bool, error) {
			var tc TopicConfig
			if err := json.Unmarshal(v, &tc); err != nil {
				return false, fmt.Errorf("加载 topic 配置 %q: %w", k, err)
			}
			m.topics[tc.Name] = tc
			return true, nil
		})
	if err != nil {
		return nil, err
	}
	err = st.Scan([]byte(store.GroupMetaPrefix), store.PrefixUpperBound([]byte(store.GroupMetaPrefix)), 0,
		func(k, v []byte) (bool, error) {
			var gc GroupConfig
			if err := json.Unmarshal(v, &gc); err != nil {
				return false, fmt.Errorf("加载 group 配置 %q: %w", k, err)
			}
			m.groups[gc.Name] = gc
			return true, nil
		})
	if err != nil {
		return nil, err
	}
	m.logger.Info("meta 加载完成", "topics", len(m.topics), "groups", len(m.groups))
	return m, nil
}

// GetTopic 查询 topic 配置。
func (m *Meta) GetTopic(name string) (TopicConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tc, ok := m.topics[name]
	return tc, ok
}

// EnsureTopic 获取 topic；不存在且开启自动创建时按默认队列数创建。
// 名字合法性校验优先于其他逻辑，确保无效名字返回 ErrBadName 而非 ErrTopicNotFound。
func (m *Meta) EnsureTopic(name string) (TopicConfig, error) {
	if err := ValidateName(name); err != nil {
		return TopicConfig{}, err
	}
	if tc, ok := m.GetTopic(name); ok {
		return tc, nil
	}
	if !m.autoCreate {
		return TopicConfig{}, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}
	return m.CreateTopic(name, m.defaultQueues)
}

// CreateTopic 创建 topic；已存在时幂等返回现有配置（不改队列数）。
func (m *Meta) CreateTopic(name string, queues uint32) (TopicConfig, error) {
	if err := ValidateName(name); err != nil {
		return TopicConfig{}, err
	}
	if queues == 0 {
		return TopicConfig{}, fmt.Errorf("queues 必须 >0")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if tc, ok := m.topics[name]; ok {
		return tc, nil
	}
	tc := TopicConfig{Name: name, Queues: queues, CreatedAtMs: time.Now().UnixMilli(), RetentionMs: DefaultRetentionMs}
	raw, _ := json.Marshal(tc)
	b := m.st.NewBatch()
	b.Set(store.TopicMetaKey(name), raw, nil)
	if err := m.st.Apply(b); err != nil {
		return TopicConfig{}, fmt.Errorf("持久化 topic %s: %w", name, err)
	}
	m.topics[name] = tc
	m.logger.Info("topic 已创建", "topic", name, "queues", queues, "retention_ms", tc.RetentionMs)
	return tc, nil
}

// EnsureGroup 获取订阅组，不存在则注册（消费组首次出现即注册，不受 autoCreate 开关限制）。
func (m *Meta) EnsureGroup(name string) (GroupConfig, error) {
	m.mu.RLock()
	gc, ok := m.groups[name]
	m.mu.RUnlock()
	if ok {
		return gc, nil
	}
	if err := ValidateName(name); err != nil {
		return GroupConfig{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if gc, ok := m.groups[name]; ok {
		return gc, nil
	}
	gc = GroupConfig{Name: name, CreatedAtMs: time.Now().UnixMilli(), MaxAttempts: m.defaultMaxAttempts}
	raw, _ := json.Marshal(gc)
	b := m.st.NewBatch()
	b.Set(store.GroupMetaKey(name), raw, nil)
	if err := m.st.Apply(b); err != nil {
		return GroupConfig{}, fmt.Errorf("持久化 group %s: %w", name, err)
	}
	m.groups[name] = gc
	m.logger.Info("消费组已注册", "group", name, "max_attempts", gc.MaxAttempts)
	return gc, nil
}

// Topics 返回全部 topic 配置快照（retention 后台扫描用；乱序）。
func (m *Meta) Topics() []TopicConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]TopicConfig, 0, len(m.topics))
	for _, tc := range m.topics {
		out = append(out, tc)
	}
	return out
}

// GetGroup 查询订阅组配置（只读，不注册——与 EnsureGroup 的区别）。
func (m *Meta) GetGroup(name string) (GroupConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	gc, ok := m.groups[name]
	return gc, ok
}

// Groups 返回全部订阅组配置快照（Admin API/metrics 用；乱序）。
func (m *Meta) Groups() []GroupConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]GroupConfig, 0, len(m.groups))
	for _, gc := range m.groups {
		out = append(out, gc)
	}
	return out
}

// UpdateTopicRetention 修改 topic 保留时长并持久化。retentionMs 必须 >0：
// 0 在 TopicConfig 里是"M1 旧数据回退默认"的哨兵值，允许写入会让两种语义混淆。
func (m *Meta) UpdateTopicRetention(name string, retentionMs int64) (TopicConfig, error) {
	if retentionMs <= 0 {
		return TopicConfig{}, fmt.Errorf("retention_ms 必须 >0，得到 %d", retentionMs)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.topics[name]
	if !ok {
		return TopicConfig{}, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}
	tc.RetentionMs = retentionMs
	raw, _ := json.Marshal(tc)
	b := m.st.NewBatch()
	b.Set(store.TopicMetaKey(name), raw, nil)
	if err := m.st.Apply(b); err != nil {
		return TopicConfig{}, fmt.Errorf("持久化 topic %s: %w", name, err)
	}
	m.topics[name] = tc
	m.logger.Info("topic retention 已更新", "topic", name, "retention_ms", retentionMs)
	return tc, nil
}

// DeleteTopic 删除 topic 注册表条目。只删注册表——msg/keyidx/alloc 等数据清理
// 是 adminops.PurgeTopicData 的职责（本包边界：不管队列内容）。不存在返回
// ErrTopicNotFound，让 Admin API 能区分 404 与 500。
func (m *Meta) DeleteTopic(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.topics[name]; !ok {
		return fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}
	b := m.st.NewBatch()
	b.Delete(store.TopicMetaKey(name), nil)
	if err := m.st.Apply(b); err != nil {
		return fmt.Errorf("删除 topic %s: %w", name, err)
	}
	delete(m.topics, name)
	m.logger.Info("topic 已删除", "topic", name)
	return nil
}

// DeleteGroup 删除订阅组注册表条目（cursor/inflight 清理归 adminops.PurgeGroupData）。
func (m *Meta) DeleteGroup(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.groups[name]; !ok {
		return fmt.Errorf("%w: %s", ErrGroupNotFound, name)
	}
	b := m.st.NewBatch()
	b.Delete(store.GroupMetaKey(name), nil)
	if err := m.st.Apply(b); err != nil {
		return fmt.Errorf("删除 group %s: %w", name, err)
	}
	delete(m.groups, name)
	m.logger.Info("消费组已删除", "group", name)
	return nil
}
