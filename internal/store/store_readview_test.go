// ReadView 一致性只读视图的测试。
//
// 职责：
//   - 验证视图建立后对后续写入不可见（隔离语义）
//   - 验证 Close 幂等，且 Close 之后读取必须报错
//   - 验证视图与主库互不影响
//
// 边界：
//   - 以外部测试包（package store_test）书写：本文件内的用例引用
//     store.NewReadView 等导出 API，与内部包的既有测试（store_test.go）
//     互不干扰
//   - 不测试快照注册表的 TTL 回收（那是集群层的职责）
package store_test

import (
	"log/slog"
	"testing"

	"github.com/xushixin/sq/internal/store"
)

// testSlog 返回写往测试输出的 slog，便于失败时直接看到 store 日志。
// 与 internal/cluster/raftstore_test.go 的版本保持一致。
func testSlog(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

func TestReadViewIsolatesLaterWrites(t *testing.T) {
	st, err := store.Open(t.TempDir(), false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	b := st.NewBatch()
	if err := b.Set([]byte("k/1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b); err != nil {
		t.Fatal(err)
	}

	view := st.NewReadView()
	defer view.Close()

	// 视图建立之后的写入不得被视图看见
	b2 := st.NewBatch()
	if err := b2.Set([]byte("k/2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := b2.Set([]byte("k/1"), []byte("v1-new")); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(b2); err != nil {
		t.Fatal(err)
	}

	got, ok, err := view.Get([]byte("k/1"))
	if err != nil || !ok || string(got) != "v1" {
		t.Fatalf("视图内 k/1 = %q ok=%v err=%v; want \"v1\"", got, ok, err)
	}
	if _, ok, _ := view.Get([]byte("k/2")); ok {
		t.Fatal("视图不得看见建立之后写入的 k/2")
	}
	var keys []string
	if err := view.Scan([]byte("k/"), store.PrefixUpperBound([]byte("k/")), 0,
		func(k, _ []byte) (bool, error) { keys = append(keys, string(k)); return true, nil }); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "k/1" {
		t.Fatalf("视图 Scan = %v; want [k/1]", keys)
	}
	// 主库读到的是最新值（视图不影响主库）
	got, _, _ = st.Get([]byte("k/1"))
	if string(got) != "v1-new" {
		t.Fatalf("主库 k/1 = %q; want v1-new", got)
	}
}

func TestReadViewCloseIsIdempotent(t *testing.T) {
	st, err := store.Open(t.TempDir(), false, testSlog(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	view := st.NewReadView()
	if err := view.Close(); err != nil {
		t.Fatalf("首次 Close: %v", err)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("重复 Close 必须无害（安装失败路径会 defer Close + 显式 Close）: %v", err)
	}
	if _, _, err := view.Get([]byte("k")); err == nil {
		t.Fatal("Close 后读取必须报错，不得静默返回空")
	}
}
