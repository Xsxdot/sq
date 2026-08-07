// sq 主入口。装配 config/store/core/rpc 并托管进程生命周期。
// 边界：只做装配与启停，不含业务逻辑；退出码非 0 表示启动失败。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xushixin/sq/internal/admin"
	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/delay"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/core/retention"
	"github.com/xushixin/sq/internal/metrics"
	"github.com/xushixin/sq/internal/rpc"
	"github.com/xushixin/sq/internal/store"
	"github.com/xushixin/sq/internal/sysinfo"
)

func main() {
	if err := run(); err != nil {
		slog.Error("sq 启动失败", "err", err)
		os.Exit(1)
	}
}

// run 装配全部模块并托管进程生命周期，直到收到退出信号或 gRPC 服务异常退出。
//
// 装配顺序（严格自底向上，后者依赖前者）：
//
//	config → store → meta → produce → deliver → rpc.Server → grpc.Server
//
// 生命周期收尾：defer st.Close() 在函数声明处即挂好——store.Open 成功后
// 任何后续失败路径（包括 net.Listen 失败）都会经由 defer 正常关闭 store，
// 不会泄漏底层 Pebble 句柄。两条退出路径在 defer 执行前都先让 gRPC server
// 停止接收/处理请求，保证 store 关闭时不会再有 handler goroutine 在读写它：
// 正常路径（收到信号）在 defer 之前调用 gracefulStop()，在有上限的前提下等
// 在途 RPC 自然结束；异常路径（gs.Serve 提前返回）调用 gs.Stop() 立即中断
// 在途 RPC（理由见下方 errCh 分支注释），两者殊途同归，只是收尾姿态不同。
func run() error {
	cfgPath := flag.String("config", "", "配置文件路径（可选）")
	flag.Parse()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	config.SetupSlog(cfg.LogLevel)
	logger := slog.Default()

	st, err := store.Open(cfg.DataDir, cfg.Fsync == "sync", logger)
	if err != nil {
		return err
	}
	defer st.Close()
	mt, err := meta.New(st, cfg.AutoCreateTopic, cfg.DefaultQueueNums, cfg.DefaultMaxAttempts, logger)
	if err != nil {
		return err
	}
	pr := produce.New(st, mt, logger)
	dl := deliver.New(st, mt, pr, logger)

	// writeBlocked 由 retention 每趟探测磁盘后更新，rpc.SendMessage 据此拒写。
	// 必须先于 metrics registry 创建：registry 里的系统 Collector 要拿着
	// sysinfo.Reporter，而 Reporter 持有的正是这个开关的指针。
	writeBlocked := &atomic.Bool{}
	// sysinfo 采集器：retention 的水位判定、/metrics 的 sq_disk_* 与控制台的
	// /admin/system 三方共用它，保证看到的是同一份磁盘事实。
	sys := sysinfo.New(cfg.DataDir, cfg.DiskWatermarkPercent, writeBlocked, logger)

	// metrics registry 必须先于任何后台 goroutine 装配：NewRegistry 会写包级
	// 钩子 store.OnApplyObserve，其契约是「装配阶段设置一次、之后只读」——
	// retention/delay 的 goroutine 启动即可能走 store.Apply 读这个钩子，
	// 放到它们之后装配就是契约禁止的无同步并发读写。admin_listen 为空 =
	// 不装配（钩子保持 nil，Apply 路径零开销）。
	var reg *prometheus.Registry
	var sp *metrics.Sampler
	if cfg.AdminListen != "" {
		reg = metrics.NewRegistry(st, mt, sys, logger)

		// 时序采样器。停机顺序与 retention/delay 同理：本 defer 注册在
		// st.Close 的 defer 之后（LIFO 先执行），保证不会在 store 关闭后落库。
		serCtx, serCancel := context.WithCancel(context.Background())
		var serWG sync.WaitGroup
		sp = metrics.NewSampler(st, mt,
			time.Duration(cfg.MetricsRetentionHours)*time.Hour, logger)
		serWG.Add(1)
		go func() { defer serWG.Done(); sp.Run(serCtx) }()
		defer func() { serCancel(); serWG.Wait() }()
	}

	// retention 后台清理。停机顺序关键：先取消并等待清理 goroutine 退出，
	// 再让 defer 关闭 store——否则可能在 store 关闭后提交清理批次（panic）。
	// defer 为 LIFO：本 defer 注册在 st.Close 的 defer 之后，故先执行。
	retCtx, retCancel := context.WithCancel(context.Background())
	var retWG sync.WaitGroup
	rm := retention.New(st, mt, cfg.RetentionInterval(), cfg.DataDir, cfg.DiskWatermarkPercent, writeBlocked, logger)
	retWG.Add(1)
	go func() { defer retWG.Done(); rm.Run(retCtx) }()
	defer func() { retCancel(); retWG.Wait() }()

	// delay 调度器：到期延时消息移入正常队列。停机顺序与 retention 同理——
	// defer LIFO 保证先取消并等待调度 goroutine 退出，再轮到 st.Close 的 defer，
	// 不会在 store 关闭后提交搬运批次。
	dlyCtx, dlyCancel := context.WithCancel(context.Background())
	var dlyWG sync.WaitGroup
	ds := delay.New(st, pr, logger)
	dlyWG.Add(1)
	go func() { defer dlyWG.Done(); ds.Run(dlyCtx) }()
	defer func() { dlyCancel(); dlyWG.Wait() }()

	// Admin HTTP（含 /metrics）。admin_listen 为空 = 关闭。停机顺序：本 defer
	// 注册在 st.Close 的 defer 之后（LIFO 先执行），保证 handler 不会在 store
	// 关闭后还在读写它。
	if cfg.AdminListen != "" {
		adm := admin.New(st, mt, pr, dl, cfg.AdminUsername, cfg.AdminPassword, sys, sp, reg, logger)
		aln, err := net.Listen("tcp", cfg.AdminListen)
		if err != nil {
			return fmt.Errorf("admin HTTP 监听 %s: %w", cfg.AdminListen, err)
		}
		go func() {
			// 运行期 Serve 异常只记日志不退进程：admin 是辅助面，它挂掉不该
			// 连累消息主链路；启动期端口占用则已在上面 fail-fast
			if err := adm.Serve(aln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("admin HTTP 异常退出", "err", err)
			}
		}()
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := adm.Shutdown(sctx); err != nil {
				logger.Warn("admin HTTP 停机超时", "err", err)
			}
		}()
		logger.Info("admin HTTP 已启动", "listen", cfg.AdminListen,
			"login_required", cfg.AdminUsername != "")
	}

	lis, err := net.Listen("tcp", cfg.GRPCListen)
	if err != nil {
		return err
	}
	// MaxRecvMsgSize/MaxSendMsgSize 必须显式设为 rpc.MaxGRPCMessageSize，不能用
	// gRPC-go 默认的 4MiB：默认值与 produce.MaxBodySize 数值相同，但一条真实
	// 请求/响应在 Body 之外还带 protobuf 帧开销与 SystemProperties/
	// UserProperties，会让恰好达到文档宣称的 4MB 上限的消息在到达应用层校验
	// 之前就被传输层拒绝（见 internal/rpc/limits.go 的详细推导）。
	// ReceiveMessage 会把同样大小的 body 流式发回，所以发送方向也要同步放宽。
	gopts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(rpc.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(rpc.MaxGRPCMessageSize),
	}
	// AK/SK 认证按配置装配（spec §6 默认关闭）。拦截器必须 unary+stream 成对装：
	// 只装 unary 会让 ReceiveMessage/Telemetry 两条流绕过认证。
	if cfg.AccessKey != "" {
		au, as := rpc.NewAuthInterceptors(cfg.AccessKey, cfg.SecretKey, logger)
		gopts = append(gopts, grpc.ChainUnaryInterceptor(au), grpc.ChainStreamInterceptor(as))
		logger.Info("gRPC AK/SK 认证已启用", "access_key", cfg.AccessKey)
	}
	gs := grpc.NewServer(gopts...)
	srv := rpc.New(cfg, mt, pr, dl, writeBlocked, logger)
	srv.Register(gs)

	// signal.Notify 必须先于 gs.Serve 的 goroutine 注册：如果反过来，
	// Serve 启动后、Notify 生效前这段窗口期内到达的 SIGTERM 会命中 Go 的
	// 默认处置（直接杀进程），defer st.Close() 不会执行，「先排空 RPC、
	// 再关 store」的停机契约在这条路径上根本不会被触发——这不是"极少数情况
	// 才会踩中"的边缘分支，而是每次进程刚启动就存在的真实竞态窗口。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(lis) }()
	logger.Info("sq 已启动", "grpc_listen", cfg.GRPCListen,
		"advertise", cfg.AdvertiseHost, "data_dir", cfg.DataDir, "fsync", cfg.Fsync)

	select {
	case sig := <-sigCh:
		logger.Info("收到退出信号，优雅停机", "signal", sig.String())
		// 顺序不能反：先让协议适配层收尾没有自然终点的长流（Telemetry），
		// 再等在途 RPC 排空。反过来的话 GracefulStop 会先把自己挂在那条
		// 永不结束的流上，Shutdown 根本没有机会被调用（见 rpc.Server.Shutdown）。
		srv.Shutdown()
		gracefulStop(gs, logger) // 等待在途 RPC 结束（有上限）；store 由 defer 关闭
		logger.Info("sq 已停止")
		return nil
	case err := <-errCh:
		// gs.Serve 提前返回（如监听 socket 被意外关闭），属于运行期故障，
		// 而不是「用户主动要求退出」——沿用 run() 统一的错误返回路径，
		// 让 main() 按非 0 退出码上报，不能被外部监控当成正常停机。
		//
		// 这里用 Stop() 而不是 GracefulStop()：Serve 本身已经失败（监听
		// 层出了问题），继续接受/处理新请求已不再可能，此时"礼貌地等待
		// 在途 RPC 自然结束"既没有对应的新流量场景去验证，也可能因为某个
		// 长轮询 RPC（如 ReceiveMessage，默认可挂到 20s）迟迟不返回而拖长
		// 一个本该立刻上报的启动/运行期故障的退出时间。Stop() 立即中断
		// 所有在途 RPC 再返回，牺牲这些 RPC 的优雅收尾换取故障退出的及时性，
		// 这里的取舍方向与用户主动触发的正常停机（SIGTERM 分支用
		// GracefulStop）刻意不同。此后 defer st.Close() 才会执行，Stop()
		// 已经保证不会再有 handler goroutine 在读写 store。
		gs.Stop()
		return err
	}
}

// gracefulStopTimeout 优雅停机的等待上限。
//
// 取值依据：在途 RPC 里最长的是 ReceiveMessage 的长轮询，服务端侧上限是
// internal/rpc 的 defaultLongPolling（20s），再留出写回响应的余量，30s 足以
// 让所有「有正常终点」的请求自己走完。超过它还没结束的，只可能是没有正常
// 终点的流（见下方 gracefulStop 的说明）。
const gracefulStopTimeout = 30 * time.Second

// gracefulStop 带上限地优雅停机：先让在途 RPC 自然结束，超时则强制中断。
//
// 为什么不能直接裸调 gs.GracefulStop()：它会一直等到最后一个在途 RPC 结束为止，
// 没有任何超时。正常情况下 rpc.Server.Shutdown() 已经把唯一一条没有自然终点的
// 长流（Telemetry）收掉了，剩下的都是有终点的请求，这里等一等就能排空；但
// 「服务端能不能停下来」这件事不该建立在「所有 handler 都行为良好」这个假设上——
// 只要有一个 RPC 因为 bug 或对端异常挂住，裸 GracefulStop 就是一次无限期阻塞，
// 对外表现为 systemctl stop 卡死、容器被超时 SIGKILL。这个上限就是那道兜底。
//
// 用官方 SDK 实测的数据（在接上 rpc.Server.Shutdown 之前）说明了这类阻塞有多
// 真实：无客户端时停机 0.03s；接一个 producer 时 9.5s（靠客户端自己的 GOAWAY
// 恢复逻辑碰巧断开）；再加一个 SimpleConsumer 之后就再也没有停下来过，只能靠
// 这里的强制中断兜底。
//
// 超时后调用 Stop() 是有意为之：Stop 会立即中断所有在途 RPC，并让阻塞中的
// GracefulStop 返回。被中断的 ReceiveMessage 不会造成消息丢失——那些消息的
// inflight 记录已经落盘，消费者没收到就不会 ack，不可见窗口一过即重投，
// 正是 at-least-once 语义覆盖的情形。
func gracefulStop(gs *grpc.Server, logger *slog.Logger) {
	done := make(chan struct{})
	go func() {
		gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(gracefulStopTimeout):
		logger.Warn("优雅停机超时，强制中断在途 RPC",
			"timeout", gracefulStopTimeout, "reason", "存在没有自然终点的长流（如 Telemetry）或长轮询未结束")
		gs.Stop()
		<-done // Stop 会让上面阻塞中的 GracefulStop 返回，这里等它真正收尾
	}
}
