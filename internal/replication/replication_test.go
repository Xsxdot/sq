// 复制层测试：单机后端直通语义与编译期接口满足。
//
// 职责：
//   - 验证 Standalone.Apply 与 store.Apply 等价（apply 即可读，忽略 group）
//   - 编译期断言两个后端都满足 Replicator 接口
//
// 边界：
//   - 不测集群后端行为——三节点集成测试在 Task 7（单测起整套集群不划算）
package replication

import (
	"context"
	"log/slog"
	"testing"

	"github.com/xushixin/sq/internal/store"
)

func openReplTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestStandaloneApplyPassthrough 单机后端 = 今天的路径：apply 即可读，
// 忽略 group 参数。
func TestStandaloneApplyPassthrough(t *testing.T) {
	st := openReplTestStore(t)
	r := NewStandalone(st)
	b := st.NewBatch()
	_ = b.Set([]byte("k"), []byte("v"))
	if err := r.Apply(context.Background(), 42, b); err != nil { // group 随便传
		t.Fatal(err)
	}
	if _, ok, _ := st.Get([]byte("k")); !ok {
		t.Fatal("单机后端 apply 后不可读")
	}
}

// 编译期接口满足：任一后端漏方法都会在此处报错。
var _ Replicator = (*Standalone)(nil)
var _ Replicator = (*Cluster)(nil)
