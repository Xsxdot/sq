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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Xsxdot/sq/internal/cluster"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
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
// 缺失（首次启动尚无节点写过）时——本节点是 MetaGroup leader 则先过可写门
// （卡 SQ1：新 leader apply 追平本任期首条之前，「读不到」可能只是上任期
// 已提交的密钥尚未 apply，此时生成会覆盖已被其他节点加载的密钥，在途
// handle 全部验签失败），门开后重读一次，仍缺失才生成并经 rep.Apply 写入；
// 不是 leader（或门未开）则轮询重读，每轮重新判定，直到密钥出现或 ctx
// 超时。轮询每 5s 打一次 Info——等 leader 写入是正常慢路径，静默会像卡死。
func LoadOrCreateHandleSecretReplicated(ctx context.Context, st *store.Store,
	rep replication.Replicator, rt replication.Router, logger *slog.Logger) ([]byte, error) {
	polls := 0
	for {
		v, ok, err := st.Get(store.HandleSecretKey())
		if err != nil {
			return nil, fmt.Errorf("读取 handle 签名密钥: %w", err)
		}
		if ok {
			logger.Info("receipt handle 签名密钥已加载")
			return v, nil
		}
		if rt.IsLeader(cluster.MetaGroup) {
			key, done, err := createHandleSecretAsLeader(ctx, st, rep, rt, logger)
			if done || err != nil {
				return key, err
			}
		}
		// 轮询等 leader 写入（或本节点门开）。逐次计间隔数，每满 5s 打一次 Info。
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("等待 handle 签名密钥写入超时: %w", ctx.Err())
		case <-time.After(handleSecretPollInterval):
			polls++
			if polls%25 == 0 {
				logger.Info("等待 leader 写入 handle 签名密钥", "waited_ms", int64(polls)*handleSecretPollInterval.Milliseconds())
			}
		}
	}
}

// createHandleSecretAsLeader meta leader 上的「缺失即生成」：过可写门 →
// 重读 → 仍缺失才生成并复制写入。
//
// 返回：done=true 时 key/err 即最终结果（已读到或已生成，或写入失败）；
// done=false 且 err==nil 表示门未开（apply 未追平 / 失去领导权），调用方
// 继续轮询——不让启动因一次换届失败。
func createHandleSecretAsLeader(ctx context.Context, st *store.Store,
	rep replication.Replicator, rt replication.Router, logger *slog.Logger) ([]byte, bool, error) {
	epoch, err := rt.WaitWritable(ctx, cluster.MetaGroup)
	if err != nil {
		logger.Warn("meta 可写门未开，暂不生成 handle 签名密钥，继续轮询", "err", err)
		return nil, false, nil
	}
	v, ok, err := st.Get(store.HandleSecretKey())
	if err != nil {
		return nil, true, fmt.Errorf("读取 handle 签名密钥: %w", err)
	}
	if ok {
		logger.Info("receipt handle 签名密钥已加载（可写门后重读）")
		return v, true, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, true, fmt.Errorf("生成 handle 签名密钥: %w", err)
	}
	// 按过门 epoch 提案（卡 SQ1 修订 R1.1）：「缺失」是在 epoch 任期内读到
	// 的判定，门放行后若本节点失主又复任，期间他人可能已写入密钥——按入队
	// 时刻绑任期的 Apply 会把旧判定写进新任期日志、覆盖它。任期不符即
	// ErrNotLeader，回轮询重判。
	b := st.NewBatch()
	b.Set(store.HandleSecretKey(), key)
	pending, err := rep.ApplyAsyncAt(ctx, cluster.MetaGroup, b, epoch)
	if err == nil {
		err = pending.Wait()
	}
	if errors.Is(err, replication.ErrNotLeader) {
		logger.Warn("生成 handle 签名密钥期间 meta 换届，回轮询重判", "epoch", epoch, "err", err)
		return nil, false, nil
	}
	if err != nil {
		return nil, true, fmt.Errorf("复制持久化 handle 签名密钥: %w", err)
	}
	logger.Info("receipt handle 签名密钥已生成并经复制持久化")
	return key, true, nil
}
