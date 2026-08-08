// receipt handle 签名密钥的加载与生成。
//
// 职责：
//   - 首次启动生成 32 字节随机密钥并持久化到 meta/handle_secret
//   - 此后每次启动原样加载——重启不换钥，在途 handle 跨重启仍有效
//
// 边界：
//   - 与 AK/SK 鉴权配置无关：鉴权关闭时 handle 防伪造同样生效
//   - 只在 main 装配期调用一次，不做缓存失效或轮换
//   - 集群档三节点必须持有同一密钥（客户端凭据在节点间轮询时 handle
//     要在任一节点验签）——密钥经 MetaGroup 复制，任何节点读到的都是
//     多数派事实；本包只在「生成并写入」时区分 leader/follower
package rpc

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"time"

	"github.com/xushixin/sq/internal/cluster"
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/store"
)

// LoadOrCreateHandleSecret 加载或生成 handle 签名密钥（单机档路径）。
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
	b.Set(store.HandleSecretKey(), key)
	if err := st.Apply(b); err != nil {
		return nil, fmt.Errorf("持久化 handle 签名密钥: %w", err)
	}
	logger.Info("receipt handle 签名密钥已生成并持久化")
	return key, nil
}

// handleSecretPollInterval 非 leader 节点的重读间隔。200ms 相对「leader
// 选举 + 首笔提案」的秒级耗时足够激进，又不至于让启动路径空转。
const handleSecretPollInterval = 200 * time.Millisecond

// LoadOrCreateHandleSecretReplicated 集群档的 handle 密钥装载。
//
// 读 meta/handle_secret：密钥经 MetaGroup 复制，三节点同值，直接读即可；
// 缺失（首次启动尚无节点写过）时——本节点是 MetaGroup leader 则生成并
// 经 rep.Apply 写入；不是 leader 则轮询重读直到密钥出现或 ctx 超时
// （leader 会写，复制会送达）。轮询每 5s 打一次 Info——等 leader 写入是
// 正常慢路径，静默会像卡死。
func LoadOrCreateHandleSecretReplicated(ctx context.Context, st *store.Store,
	rep replication.Replicator, rt replication.Router, logger *slog.Logger) ([]byte, error) {
	v, ok, err := st.Get(store.HandleSecretKey())
	if err != nil {
		return nil, fmt.Errorf("读取 handle 签名密钥: %w", err)
	}
	if ok {
		logger.Info("receipt handle 签名密钥已加载")
		return v, nil
	}
	if rt.IsLeader(cluster.MetaGroup) {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("生成 handle 签名密钥: %w", err)
		}
		b := st.NewBatch()
		b.Set(store.HandleSecretKey(), key)
		if err := rep.Apply(ctx, cluster.MetaGroup, b); err != nil {
			return nil, fmt.Errorf("复制持久化 handle 签名密钥: %w", err)
		}
		logger.Info("receipt handle 签名密钥已生成并经复制持久化")
		return key, nil
	}
	// 非 leader：轮询等 leader 写入。逐次计间隔数，每满 5s 打一次 Info。
	polls := 0
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("等待 handle 签名密钥写入超时: %w", ctx.Err())
		case <-time.After(handleSecretPollInterval):
			polls++
			if polls%25 == 0 {
				logger.Info("等待 leader 写入 handle 签名密钥", "waited_ms", int64(polls)*handleSecretPollInterval.Milliseconds())
			}
			v, ok, err := st.Get(store.HandleSecretKey())
			if err != nil {
				return nil, fmt.Errorf("读取 handle 签名密钥: %w", err)
			}
			if ok {
				logger.Info("receipt handle 签名密钥已加载")
				return v, nil
			}
		}
	}
}
