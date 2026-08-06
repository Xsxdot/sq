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
}

// GroupConfig 订阅组配置。M1 仅注册身份；maxAttempts 等属 M2。
type GroupConfig struct {
	Name        string `json:"name"`
	CreatedAtMs int64  `json:"created_at_ms"`
}

// Meta topic/group 注册表。读多写少，读走内存缓存，写穿透到 store。
type Meta struct {
	st            *store.Store
	autoCreate    bool
	defaultQueues uint32
	logger        *slog.Logger

	mu     sync.RWMutex
	topics map[string]TopicConfig
	groups map[string]GroupConfig
}

// New 构造并从 store 加载全部已有配置。
func New(st *store.Store, autoCreate bool, defaultQueues uint32, logger *slog.Logger) (*Meta, error) {
	m := &Meta{
		st: st, autoCreate: autoCreate, defaultQueues: defaultQueues,
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
func (m *Meta) EnsureTopic(name string) (TopicConfig, error) {
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
	tc := TopicConfig{Name: name, Queues: queues, CreatedAtMs: time.Now().UnixMilli()}
	raw, _ := json.Marshal(tc)
	b := m.st.NewBatch()
	b.Set(store.TopicMetaKey(name), raw, nil)
	if err := m.st.Apply(b); err != nil {
		return TopicConfig{}, fmt.Errorf("持久化 topic %s: %w", name, err)
	}
	m.topics[name] = tc
	m.logger.Info("topic 已创建", "topic", name, "queues", queues)
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
	gc = GroupConfig{Name: name, CreatedAtMs: time.Now().UnixMilli()}
	raw, _ := json.Marshal(gc)
	b := m.st.NewBatch()
	b.Set(store.GroupMetaKey(name), raw, nil)
	if err := m.st.Apply(b); err != nil {
		return GroupConfig{}, fmt.Errorf("持久化 group %s: %w", name, err)
	}
	m.groups[name] = gc
	m.logger.Info("消费组已注册", "group", name)
	return gc, nil
}
