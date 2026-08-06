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
// 不会泄漏底层 Pebble 句柄；正常路径下 defer 在 gs.GracefulStop() 返回之后
// 才执行，确保「先等在途 RPC 结束、再关 store」这个顺序（RPC handler 仍可能
// 在关闭过程中访问 store，store 必须比 gRPC server 活得更久）。
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

	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(lis) }()
	logger.Info("sq 已启动", "grpc_listen", cfg.GRPCListen,
		"advertise", cfg.AdvertiseHost, "data_dir", cfg.DataDir, "fsync", cfg.Fsync)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
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
		return err
	}
}
