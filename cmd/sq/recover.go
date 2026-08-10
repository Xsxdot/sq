// recover.go 实现 `sq recover` 子命令：不干净关机后的现场报告与运维签字。
//
// 职责：
//   - 只读地报告本节点的恢复判定与各组现场，让运维在签字前看得见代价
//   - 在 --grant 时写入一次性本地恢复许可
//
// 边界：
//   - 不启动 broker、不碰网络、不做任何恢复动作——恢复由下次正常启动完成
//   - 判定不自己实现，一律走 cluster.InspectRecovery（与进程同源，
//     否则会出现「命令说你不用签字、进程说你要签字」）
//   - 报告走 stdout 而非 slog：它是**给人看的命令输出**，不是事件。
//     事件（授予许可）已经在 slog 里（Error 级，见 raftstore.SaveRecoverPermit），
//     事后审计看 broker/命令日志即可，这条区分避免同一次授予打两份重复日志
//
// 为什么要有这个命令：quorum-mem 档下机器真掉电后，本地日志尾可能残缺，
// 带着它恢复可能静默丢掉已确认的消息。这个决定必须由人做，而人做决定
// 之前得先看见数字。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/xushixin/sq/internal/cluster"
	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/store"
)

// runRecover 执行 `sq recover` 子命令。
//
// 参数：
//   - args: `recover` 之后的全部参数
//
// 返回：错误（nil 表示成功；调用方据此决定退出码）
func runRecover(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "配置文件路径")
	grant := fs.Bool("grant", false, "写入一次性本地恢复许可（默认只报告不写入）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	config.SetupSlog(cfg.LogLevel)
	logger := slog.Default()
	if !cfg.ClusterEnabled() {
		return fmt.Errorf("本配置未启用集群档，不存在不干净关机的重入编排问题，无需 recover")
	}

	// Pebble 独占锁天然把「服务运行中误签」这条路堵死：broker 还在跑时
	// Open 必然失败。这是白送的一道互斥，只需把提示写清楚。
	st, err := store.Open(cfg.DataDir, cfg.Fsync == "sync", logger)
	if err != nil {
		return fmt.Errorf("打开数据目录 %s 失败（若本节点 broker 仍在运行，请先停止它再执行本命令）: %w", cfg.DataDir, err)
	}
	defer st.Close()

	mode := cluster.AckQuorumMem
	if cfg.Cluster.Ack == "quorum-fsync" {
		mode = cluster.AckQuorumFsync
	}
	rep, err := cluster.InspectRecovery(st, cfg.Cluster.DataGroups, mode, nil, logger)
	if err != nil {
		return err
	}
	printRecoveryReport(os.Stdout, rep)

	if !*grant {
		return nil
	}
	if !rep.NeedsPermit {
		fmt.Fprintln(os.Stdout, "\n本节点不需要许可，直接启动即可自动恢复——未写入任何内容。")
		return nil
	}
	if err := cluster.GrantRecoverPermit(st, time.Now(), nil, logger); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "\n已写入一次性本地恢复许可。现在启动本节点即可带着本地日志恢复；"+
		"该许可只对本次事故有效，机器再重启一次即自动失效。")
	return nil
}

// printRecoveryReport 渲染报告：先结论、再判据、最后各组现场。
//
// 顺序是刻意的——运维最需要的是「我要不要签字、签了可能丢什么」，
// 各组的 index 是给需要深挖的人看的素材，放最后。
func printRecoveryReport(w *os.File, rep cluster.RecoveryReport) {
	fmt.Fprintf(w, "恢复判定：%s\n", rep.Path)
	fmt.Fprintf(w, "  理由：%s\n", rep.Reason)
	if rep.NeedsPermit {
		fmt.Fprintf(w, "  结论：**需要运维签字**。quorum-mem 档的后台刷盘周期为 200ms，\n"+
			"        带着本地日志恢复最多可能丢失最后 200ms 内已确认的写入。\n"+
			"        确认接受后执行：sq recover -config <配置> --grant\n")
	} else {
		fmt.Fprintf(w, "  结论：无需许可，直接启动本节点即可。\n")
	}
	fmt.Fprintf(w, "\n判据：\n")
	fmt.Fprintf(w, "  确认档位     : %s\n", rep.Mode.String())
	fmt.Fprintf(w, "  当前机器世代 : %s\n", genText(rep.GenNow, rep.GenNowOK))
	fmt.Fprintf(w, "  盘上机器世代 : %s\n", genText(rep.GenStored, rep.GenStoredOK))
	if rep.HasPermit {
		fmt.Fprintf(w, "  已有许可     : 是（绑定世代 %s）\n", rep.PermitGen)
	} else {
		fmt.Fprintf(w, "  已有许可     : 否\n")
	}
	fmt.Fprintf(w, "\n各组现场：\n")
	fmt.Fprintf(w, "  %-4s %-10s %-10s %-9s %-10s %-6s %-8s\n", "组", "applied", "日志尾", "尾 term", "快照锚点", "任期", "commit")
	for _, g := range rep.Groups {
		fmt.Fprintf(w, "  %-4d %-10d %-10d %-9d %-10d %-6d %-8d\n",
			g.Group, g.Applied, g.LastIndex, g.LastTerm, g.SnapIndex, g.Term, g.Commit)
	}
}

// genText 把「世代 + 是否可用」渲染成一句人话。
func genText(v string, ok bool) string {
	if !ok {
		return "（读不到——按机器可能已重启保守处理）"
	}
	return v
}
