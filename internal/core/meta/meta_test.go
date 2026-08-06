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
	"errors"
	"log/slog"
	"testing"

	"github.com/xushixin/sq/internal/store"
)

func newTestMeta(t *testing.T, dir string, autoCreate bool) (*Meta, *store.Store) {
	t.Helper()
	st, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	m, err := New(st, autoCreate, 4, slog.Default())
	if err != nil {
		t.Fatalf("New meta: %v", err)
	}
	return m, st
}

func TestEnsureTopicAutoCreateAndPersist(t *testing.T) {
	dir := t.TempDir()
	m, st := newTestMeta(t, dir, true)
	tc, err := m.EnsureTopic("orders")
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
	if _, err := m.EnsureTopic("nope"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("期望 ErrTopicNotFound，得到 %v", err)
	}
}

func TestEnsureTopicBadNameIndependentOfAutoCreate(t *testing.T) {
	// 无效名字应返回 ErrBadName，与 autoCreate 标志无关
	invalidName := "has/slash"

	// 情形1: autoCreate=false，无效名字返回 ErrBadName（不是 ErrTopicNotFound）
	m1, st1 := newTestMeta(t, t.TempDir(), false)
	defer st1.Close()
	if _, err := m1.EnsureTopic(invalidName); !errors.Is(err, ErrBadName) {
		t.Fatalf("autoCreate=false: 期望 ErrBadName，得到 %v", err)
	}

	// 情形2: autoCreate=true，无效名字返回 ErrBadName（不创建 topic）
	m2, st2 := newTestMeta(t, t.TempDir(), true)
	defer st2.Close()
	if _, err := m2.EnsureTopic(invalidName); !errors.Is(err, ErrBadName) {
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
	g, err := m.EnsureGroup("g1")
	if err != nil || g.Name != "g1" {
		t.Fatalf("EnsureGroup: %+v %v", g, err)
	}
}
