// receipt handle 签名密钥的加载与生成。
//
// 职责：
//   - 首次启动生成 32 字节随机密钥并持久化到 meta/handle_secret
//   - 此后每次启动原样加载——重启不换钥，在途 handle 跨重启仍有效
//
// 边界：
//   - 与 AK/SK 鉴权配置无关：鉴权关闭时 handle 防伪造同样生效
//   - 只在 main 装配期调用一次，不做缓存失效或轮换
package rpc

import (
	"crypto/rand"
	"fmt"
	"log/slog"

	"github.com/xushixin/sq/internal/store"
)

// LoadOrCreateHandleSecret 加载或生成 handle 签名密钥。
func LoadOrCreateHandleSecret(st *store.Store, logger *slog.Logger) ([]byte, error) {
	v, ok, err := st.Get(store.HandleSecretKey())
	if err != nil {
		return nil, fmt.Errorf("读取 handle 签名密钥: %w", err)
	}
	if ok {
		logger.Info("receipt handle 签名密钥已加载")
		return v, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("生成 handle 签名密钥: %w", err)
	}
	b := st.NewBatch()
	b.Set(store.HandleSecretKey(), key, nil)
	if err := st.Apply(b); err != nil {
		return nil, fmt.Errorf("持久化 handle 签名密钥: %w", err)
	}
	logger.Info("receipt handle 签名密钥已生成并持久化")
	return key, nil
}
