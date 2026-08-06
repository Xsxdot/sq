// sq 主入口。装配 config/store/core/rpc 并托管进程生命周期。
// 边界：只做装配与启停，不含业务逻辑。
package main

import (
	"flag"
	"log/slog"

	"github.com/xushixin/sq/internal/config"
)

func main() {
	cfgPath := flag.String("config", "", "配置文件路径（可选）")
	flag.Parse()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("加载配置失败", "err", err)
		return
	}
	config.SetupSlog(cfg.LogLevel)
	slog.Info("sq 启动（M1 骨架）", "grpc_listen", cfg.GRPCListen, "data_dir", cfg.DataDir)
}
