// walsync.go: 单机档 fsync=async 的周期 WAL 刷盘循环（SQ4.11）。
//
// 职责：
//   - 按固定周期调用刷盘函数（生产装配传 (*Store).SyncWAL），给 async 档的
//     掉电丢失窗口一个上界（AsyncWALSyncInterval）
//
// 边界：
//   - 只在单机档 async 装配（cmd/sq/main.go）；集群档由 cluster.Manager 的
//     flusher 按「先日志段、后 FSM WAL」顺序刷盘，sync 档每次写都 fsync，
//     两者都不挂本循环
//   - 不持有 Store：刷盘函数由调用方注入，生命周期（先停循环再关 store）
//     由调用方保证
package store

import (
	"context"
	"log/slog"
	"time"
)

// AsyncWALSyncInterval 单机 async 档的周期刷盘间隔：掉电最多丢这么久的写入。
// 与集群 quorum-mem 档 flusher 的 200ms 尾部风险取同一值。
const AsyncWALSyncInterval = 200 * time.Millisecond

// RunWALSyncLoop 每隔 interval 调用一次 sync，直到 ctx 取消后返回。
//
// 参数：
//   - ctx: 取消即退出；返回后不会再调用 sync
//   - interval: 刷盘周期，必须 > 0
//   - sync: 刷盘函数，生产传 (*Store).SyncWAL
//   - logger: 结构化日志
//
// 注意：
//   - 阻塞调用，调用方自行起 goroutine 并在关闭 store 之前取消 ctx、等它返回
//   - sync 失败只记 Error 不退出：单次失败不代表后续都失败，停刷只会让
//     丢失窗口回到无上界；若 WAL 真进入不可恢复态，写路径自己会报错
func RunWALSyncLoop(ctx context.Context, interval time.Duration, sync func() error, logger *slog.Logger) {
	logger = logger.With("mod", "store") // 与 store.Open 内的绑定一致
	logger.Info("async 档周期刷盘已启动", "interval", interval.String())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("async 档周期刷盘已停止")
			return
		case <-ticker.C:
			if err := sync(); err != nil {
				logger.Error("async 档周期刷盘失败", "interval", interval.String(), "err", err)
			}
		}
	}
}
