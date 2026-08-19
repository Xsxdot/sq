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
