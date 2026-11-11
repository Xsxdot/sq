// handle_secret_test.go 验证 handle 签名密钥的生成与持久化。
//
// 职责：
//   - 首次启动生成 32 字节密钥并落盘
//   - 关闭重开 store 后密钥原样加载（重启不换钥，在途 handle 跨重启仍有效）
//   - 集群档装载（Replicated 变体）在单机装配下与单机路径行为一致
//
// 边界：
//   - 不测随机性本身（crypto/rand 的强度不在本包职责）
//   - 不测非 leader 轮询路径（需多节点复制事实，属集群集成层职责）
package rpc

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
)

func TestHandleSecretPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	st1, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	k1, err := LoadOrCreateHandleSecret(st1, slog.Default())
	if err != nil || len(k1) != 32 {
		t.Fatalf("首次生成: %v len=%d", err, len(k1))
	}
	st1.Close()
	st2, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	k2, err := LoadOrCreateHandleSecret(st2, slog.Default())
	if err != nil || !bytes.Equal(k1, k2) {
		t.Fatalf("重开后密钥变了: %v", err)
	}
}

// TestLoadOrCreateHandleSecretReplicatedLeaderPath 单机装配（Standalone 后端 +
// StandaloneRouter 恒为 leader）下，Replicated 变体行为与单机路径一致：
// 首启生成、重载原样读回。
func TestLoadOrCreateHandleSecretReplicatedLeaderPath(t *testing.T) {
	dir := t.TempDir()
	st1, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	k1, err := LoadOrCreateHandleSecretReplicated(context.Background(), st1,
		replication.NewStandalone(st1), replication.StandaloneRouter{}, slog.Default())
	if err != nil || len(k1) != 32 {
		t.Fatalf("首启生成: %v len=%d", err, len(k1))
	}
	st1.Close()
	st2, err := store.Open(dir, true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	k2, err := LoadOrCreateHandleSecretReplicated(context.Background(), st2,
		replication.NewStandalone(st2), replication.StandaloneRouter{}, slog.Default())
	if err != nil || !bytes.Equal(k1, k2) {
		t.Fatalf("重开后密钥变了: %v", err)
	}
}

// epochRecRouter 单机路由 + 固定可写门 epoch（集群档 leader 的替身）。
type epochRecRouter struct {
	replication.StandaloneRouter
	epoch uint64
}

func (r epochRecRouter) WaitWritable(context.Context, uint32) (uint64, error) { return r.epoch, nil }

// epochRecReplicator 记录生成路径走的是哪个提交入口及其 epoch；提交直通 store。
type epochRecReplicator struct {
	st         *store.Store
	applyCalls int
	atCalls    int
	atEpoch    uint64
}

func (r *epochRecReplicator) Apply(_ context.Context, _ uint32, b *store.Batch) error {
	r.applyCalls++
	return r.st.Apply(b)
}

func (r *epochRecReplicator) ApplyAsync(ctx context.Context, g uint32, b *store.Batch) (replication.Pending, error) {
	return replication.NewStandalone(r.st).ApplyAsync(ctx, g, b)
}

func (r *epochRecReplicator) ApplyAsyncAt(ctx context.Context, g uint32, b *store.Batch, epoch uint64) (replication.Pending, error) {
	r.atCalls++
	r.atEpoch = epoch
	return replication.NewStandalone(r.st).ApplyAsync(ctx, g, b)
}

// TestHandleSecretCreateUsesGateEpoch 卡 SQ1 修订 R1.1（审查 M-2）：meta leader
// 生成密钥的写入必须按过门 epoch 提案（ApplyAsyncAt），不得走按入队时刻绑定
// 任期的 Apply——否则门放行后失主又复任时，旧判定（「缺失」）会进新任期日志，
// 覆盖期间他人已写入的密钥。
func TestHandleSecretCreateUsesGateEpoch(t *testing.T) {
	st, err := store.Open(t.TempDir(), true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rep := &epochRecReplicator{st: st}
	k, err := LoadOrCreateHandleSecretReplicated(context.Background(), st, rep, epochRecRouter{epoch: 7}, slog.Default())
	if err != nil || len(k) != 32 {
		t.Fatalf("生成: %v len=%d", err, len(k))
	}
	if rep.applyCalls != 0 || rep.atCalls != 1 || rep.atEpoch != 7 {
		t.Fatalf("生成路径提交入口: Apply×%d ApplyAsyncAt×%d(epoch=%d)，期望只走 ApplyAsyncAt 且 epoch=7（过门 epoch）",
			rep.applyCalls, rep.atCalls, rep.atEpoch)
	}
}
