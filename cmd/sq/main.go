// sq 主入口。装配 config/store/core/rpc 并托管进程生命周期。
// 边界：只做装配与启停，不含业务逻辑；退出码非 0 表示启动失败。
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/rpc"
	"github.com/xushixin/sq/internal/store"
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
// 正常路径（收到信号）在 defer 之前调用 gs.GracefulStop()，等在途 RPC
// 自然结束；异常路径（gs.Serve 提前返回）调用 gs.Stop() 立即中断在途 RPC
// （理由见下方 errCh 分支注释），两者殊途同归，只是收尾姿态不同。
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
	mt, err := meta.New(st, cfg.AutoCreateTopic, cfg.DefaultQueueNums, logger)
	if err != nil {
		return err
	}
	pr := produce.New(st, mt, logger)
	dl := deliver.New(st, mt, pr, logger)

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
	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(rpc.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(rpc.MaxGRPCMessageSize),
	)
	rpc.New(cfg, mt, pr, dl, logger).Register(gs)

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
		gs.GracefulStop() // 等待在途 RPC 结束；store 由 defer 关闭
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
