// Package meta 维护 topic 与订阅组注册表。
//
// 职责（测试文件）：
//   - 验证 topic/group 配置的创建、查询、持久化恢复
//   - 验证名字合法性校验逻辑
//   - 验证 autoCreate 开关对 topic 的控制
//
// 边界：
//   - 仅测试 meta.Meta 及其导出方法的行为
//   - 不测试 store 内部实现
package meta

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

func newTestMeta(t *testing.T, dir string, autoCreate bool) (*Meta, *store.Store) {
	t.Helper()
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	m, err := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, autoCreate, 4, 16, slog.Default())
	if err != nil {
		t.Fatalf("New meta: %v", err)
	}
	return m, st
}

func TestEnsureTopicAutoCreateAndPersist(t *testing.T) {
	dir := t.TempDir()
	m, st := newTestMeta(t, dir, true)
	tc, err := m.EnsureTopic(context.Background(), "orders")
	if err != nil || tc.Queues != 4 {
		t.Fatalf("EnsureTopic: %+v %v", tc, err)
	}
	st.Close()
	// 重开：配置必须从盘上恢复
	m2, st2 := newTestMeta(t, dir, false)
	defer st2.Close()
	got, ok := m2.GetTopic("orders")
	if !ok || got.Queues != 4 {
		t.Fatalf("重开后丢失: %+v %v", got, ok)
	}
}

func TestEnsureTopicNotFoundWhenAutoCreateOff(t *testing.T) {
	m, st := newTestMeta(t, t.TempDir(), false)
	defer st.Close()
	if _, err := m.EnsureTopic(context.Background(), "nope"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("期望 ErrTopicNotFound，得到 %v", err)
	}
}

func TestEnsureTopicBadNameIndependentOfAutoCreate(t *testing.T) {
	// 无效名字应返回 ErrBadName，与 autoCreate 标志无关
	invalidName := "has/slash"

	// 情形1: autoCreate=false，无效名字返回 ErrBadName（不是 ErrTopicNotFound）
	m1, st1 := newTestMeta(t, t.TempDir(), false)
	defer st1.Close()
	if _, err := m1.EnsureTopic(context.Background(), invalidName); !errors.Is(err, ErrBadName) {
		t.Fatalf("autoCreate=false: 期望 ErrBadName，得到 %v", err)
	}

	// 情形2: autoCreate=true，无效名字返回 ErrBadName（不创建 topic）
	m2, st2 := newTestMeta(t, t.TempDir(), true)
	defer st2.Close()
	if _, err := m2.EnsureTopic(context.Background(), invalidName); !errors.Is(err, ErrBadName) {
		t.Fatalf("autoCreate=true: 期望 ErrBadName，得到 %v", err)
	}
	// 确保没有创建
	if _, ok := m2.GetTopic(invalidName); ok {
		t.Fatalf("无效名字不应被创建")
	}
}

func TestValidateName(t *testing.T) {
	if err := ValidateName("ok_Topic-1%"); err != nil {
		t.Fatalf("合法名被拒: %v", err)
	}
	for _, bad := range []string{"", "has/slash", "汉字", string(make([]byte, 200))} {
		if err := ValidateName(bad); err == nil {
			t.Fatalf("非法名未拒: %q", bad)
		}
	}
}

func TestEnsureGroup(t *testing.T) {
	m, st := newTestMeta(t, t.TempDir(), false) // group 不受 autoCreate 开关限制
	defer st.Close()
	g, err := m.EnsureGroup(context.Background(), "g1")
	if err != nil || g.Name != "g1" {
		t.Fatalf("EnsureGroup: %+v %v", g, err)
	}
}

// TestGroupMaxAttempts 新组按 broker 默认注册；旧数据（0 值）回退包默认。
func TestGroupMaxAttempts(t *testing.T) {
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	m, err := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 4, 2, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	gc, err := m.EnsureGroup(context.Background(), "g1")
	if err != nil || gc.EffectiveMaxAttempts() != 2 {
		t.Fatalf("新组 maxAttempts: %d %v", gc.EffectiveMaxAttempts(), err)
	}
	// M1 落盘的旧组无 max_attempts 字段（解码为 0）→ 回退 DefaultMaxAttempts
	if got := (GroupConfig{}).EffectiveMaxAttempts(); got != DefaultMaxAttempts {
		t.Fatalf("零值回退: %d", got)
	}
}

func TestTopicRetentionDefault(t *testing.T) {
	st, _ := store.Open(t.TempDir(), true, slog.Default())
	t.Cleanup(func() { st.Close() })
	m, _ := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 4, 16, slog.Default())
	tc, err := m.CreateTopic(context.Background(), "t", 4)
	if err != nil || tc.EffectiveRetention() != 72*time.Hour {
		t.Fatalf("默认 retention: %v %v", tc.EffectiveRetention(), err)
	}
	if got := (TopicConfig{}).EffectiveRetention(); got != 72*time.Hour {
		t.Fatalf("零值回退: %v", got)
	}
}

func TestDLQTopicName(t *testing.T) {
	name := DLQTopicName("orders-g")
	if name != "%DLQ%orders-g" {
		t.Fatalf("DLQ 名: %s", name)
	}
	if err := ValidateName(name); err != nil {
		t.Fatalf("DLQ 名必须通过名字校验（'%%' 在合法字符集内）: %v", err)
	}
}

func TestTopicsSnapshot(t *testing.T) {
	st, _ := store.Open(t.TempDir(), true, slog.Default())
	t.Cleanup(func() { st.Close() })
	m, _ := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 4, 16, slog.Default())
	m.CreateTopic(context.Background(), "a", 1)
	m.CreateTopic(context.Background(), "b", 2)
	if got := len(m.Topics()); got != 2 {
		t.Fatalf("Topics 快照: %d", got)
	}
}

// TestTopicUpdateAndDelete 修改 retention、删除 topic 与错误路径。
func TestTopicUpdateAndDelete(t *testing.T) {
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	m, err := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 4, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic(context.Background(), "t1", 2); err != nil {
		t.Fatal(err)
	}
	tc, err := m.UpdateTopicRetention(context.Background(), "t1", 1000)
	if err != nil || tc.RetentionMs != 1000 {
		t.Fatalf("更新 retention 失败: %+v %v", tc, err)
	}
	// 持久化必须生效：重开 meta 后新值仍在
	m2, err := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 4, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if tc, _ := m2.GetTopic("t1"); tc.RetentionMs != 1000 {
		t.Fatalf("retention 未持久化: %+v", tc)
	}
	if _, err := m.UpdateTopicRetention(context.Background(), "t1", 0); err == nil {
		t.Fatal("retention<=0 应拒绝")
	}
	if _, err := m.UpdateTopicRetention(context.Background(), "nope", 1000); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("不存在的 topic 应返回 ErrTopicNotFound: %v", err)
	}
	if err := m.DeleteTopic(context.Background(), "t1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.GetTopic("t1"); ok {
		t.Fatal("删除后不应可见")
	}
	if err := m.DeleteTopic(context.Background(), "t1"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("重复删除应返回 ErrTopicNotFound: %v", err)
	}
}

// TestGroupAccessorsAndDelete GetGroup/Groups/DeleteGroup。
func TestGroupAccessorsAndDelete(t *testing.T) {
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	m, err := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, true, 4, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.EnsureGroup(context.Background(), "g1"); err != nil {
		t.Fatal(err)
	}
	if gc, ok := m.GetGroup("g1"); !ok || gc.Name != "g1" {
		t.Fatalf("GetGroup 不符: %+v %v", gc, ok)
	}
	if _, ok := m.GetGroup("nope"); ok {
		t.Fatal("不存在的组不应命中")
	}
	if gs := m.Groups(); len(gs) != 1 || gs[0].Name != "g1" {
		t.Fatalf("Groups 快照不符: %+v", gs)
	}
	if err := m.DeleteGroup(context.Background(), "g1"); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteGroup(context.Background(), "g1"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("重复删除应返回 ErrGroupNotFound: %v", err)
	}
}

// writeTopicRaw 绕过 meta 缓存直写 store——模拟另一节点对同一底层库的盲 apply。
// 编码用 meta 包自身的 TopicConfig JSON，保证与真实写路径字节一致。
func writeTopicRaw(t *testing.T, st *store.Store, name string, queues uint32) {
	t.Helper()
	tc := TopicConfig{Name: name, Queues: queues, CreatedAtMs: time.Now().UnixMilli(), RetentionMs: DefaultRetentionMs}
	raw, err := json.Marshal(tc)
	if err != nil {
		t.Fatal(err)
	}
	b := st.NewBatch()
	b.Set(store.TopicMetaKey(name), raw)
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}
}

// TestMetaReloadPicksUpBlindApply 绕过缓存直写 store 模拟 follower 盲 apply，
// Reload 后读到新 topic——集群档缓存一致性的最小锚点。
func TestMetaReloadPicksUpBlindApply(t *testing.T) {
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	mt, err := New(replication.NewStandalone(st), replication.StandaloneRouter{}, st, false, 4, 16, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	// 模拟另一节点的盲 apply：直接 store 写一条 topic 配置
	writeTopicRaw(t, st, "ghost", 4)
	if _, ok := mt.GetTopic("ghost"); ok {
		t.Fatal("Reload 前不应可见（缓存未失效）")
	}
	if err := mt.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, ok := mt.GetTopic("ghost"); !ok {
		t.Fatal("Reload 后应可见")
	}
}
