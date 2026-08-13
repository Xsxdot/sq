// sq 主入口。装配 config/store/core/rpc（集群档含 cluster.Manager）并
// 托管进程生命周期。
//
// 职责：
//   - 单机/集群二选一装配复制层：cfg.ClusterEnabled()==false 时走
//     Standalone + StandaloneRouter + StaticRouteView（v1 逐字节行为）；
//     集群档按下方装配序组装 cluster.Manager 并注入全部复制后端
//   - 不干净关机的分级恢复（B10 + B11）：能证明本地日志完好时（机器世代
//     未变，或 quorum-fsync 档）直接以原身份从本地日志恢复；证明不了时
//     走 cluster.Rejoin——**先求集群接纳、拿到接纳才清空数据目录**，
//     求不到接纳则数据分毫不动、进程拒启，并在日志里给出
//     `sq recover --grant` 的签字出口
//
// 边界：只做装配与启停，不含业务逻辑；退出码非 0 表示启动失败。
package main

import (
	"context"
	"encoding/binary"
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
	"github.com/xushixin/sq/internal/cluster"
	"github.com/xushixin/sq/internal/config"
	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/core/delay"
	"github.com/xushixin/sq/internal/core/deliver"
	"github.com/xushixin/sq/internal/core/meta"
	"github.com/xushixin/sq/internal/core/produce"
	"github.com/xushixin/sq/internal/core/retention"
	"github.com/xushixin/sq/internal/core/txn"
	"github.com/xushixin/sq/internal/metrics"
	"github.com/xushixin/sq/internal/replication"
	"github.com/xushixin/sq/internal/rpc"
	"github.com/xushixin/sq/internal/store"
	"github.com/xushixin/sq/internal/sysinfo"
)

func main() {
	// 子命令分流：只认第一个位置参数，其余一律走原有的 run()。
	// 这样 `sq -config x.yaml` 与既有 systemd 单元完全不受影响。
	if len(os.Args) > 1 && os.Args[1] == "recover" {
		if err := runRecover(os.Args[2:]); err != nil {
			slog.Error("recover 失败", "err", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		slog.Error("sq 启动失败", "err", err)
		os.Exit(1)
	}
}

// run 装配全部模块并托管进程生命周期，直到收到退出信号或 gRPC 服务异常退出。
//
// 装配顺序（严格自底向上，后者依赖前者）：
//
//	config → store → [集群档] cluster.NewManager → meta → produce → deliver → rpc.Server → grpc.Server
//
// 集群档装配序（plan Task 11，硬性要求）：
//
//	config → store.Open → cluster.NewManager（含日志回放；ErrUncleanShutdown
//	→ 打 Error 日志 → st.Close → cluster.Rejoin 自愈）→ Manager.Start → 等 meta 组
//	出 leader（60s 超时）→ rep/rt/fwd 构造 → meta.New（此时 FSM 完整）→ produce/deliver/
//	txn/delay/retention 注入 → secret（集群档走 Replicated 变体）→ rpc.New（RouteView）
//	→ OnApplied/OnLeaderChange 钩子闭包接线（meta.Reload / produce.InvalidateCounters
//	——闭包内 go 出去，钩子契约不阻塞）→ ControlHandler 注册（ForwardAppend 落
//	pr.Append、ForwardApply 落 rep.Apply）→ 其余照旧。停机：gRPC 排空 → 定时器退出
//	→ Manager.StopClean → st.Close（defer LIFO 序对齐现有注释风格）
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

	// 编码档位必须在任何编解码发生之前装配：它是进程级一次性开关。
	// 非法值 fail-stop——config.Load 已挡过一道，这里是 core 侧边界的
	// 二次确认，两处都便宜且都在启动期。
	if err := core.SetEncoding(cfg.MessageEncoding); err != nil {
		return err
	}
	// 档位是"盘上为什么出现二进制数据"的唯一线索，无条件打一条：
	// 「已启动」日志在 recover 子命令与启动期失败时不会出现。
	logger.Info("消息编码档位已装配", "message_encoding", cfg.MessageEncoding)

	st, err := store.Open(cfg.DataDir, cfg.Fsync == "sync", logger)
	if err != nil {
		return err
	}
	// 闭包而非裸 st.Close()：集群档 Rejoin 会换掉 st（内部重开 store），
	// defer 必须关「退出时指向的那个实例」——裸调用在声明处就绑死了
	// 旧实例，重入路径会漏关新 store（见下方集群分支）。
	defer func() { st.Close() }()

	// 集群运行 ctx：跨整个进程生命周期（Manager.Start/Rejoin 共用，
	// Rejoin 的 ctx 兼作重启后 Manager 的 run ctx——Task 10 审查注记
	// 不得传短超时；LoadOrCreateHandleSecretReplicated 的轮询也用它）。
	// 单机档不用，但声明无副作用。
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	// 复制层装配：单机档零额外开销直通；集群档把全部写路径接进 raft 组。
	//
	// 为什么 rep/rt/fwd 先声明后赋值：集群档的装配钩子（OnApplied/
	// OnLeaderChange/ControlHandler）要在 cluster.NewManager 的 Options
	// 里以闭包形式注入，而这些闭包引用 mt/pr/rep——它们在 NewManager
	// 之后才构造。闭包捕获变量而非值：声明在前、赋值在后，钩子触发
	// 时（Start 之后）变量已就位。
	//
	// 钩子闭包读这些变量与主 goroutine 的赋值并发（装配窗口语义：
	// 依赖未构造时跳过）——裸变量捕获是数据竞争（cmd 无 -race 覆盖），
	// 统一改 atomic.Pointer 装载：闭包内 Load() 判 nil 跳过，主流程
	// 赋值点 Store()。装配窗口语义不变，只是读是原子的。
	var (
		rep replication.Replicator
		rt  replication.Router
		fwd replication.Forwarder
		rv  rpc.RouteView
		mt  *meta.Meta
		pr  *produce.Producer
		m   *cluster.Manager // 集群档非 nil；单机档保持 nil
	)
	// 钩子闭包的原子装载（见上方注释）：mt/pr 装配于 Start 之后、st 在
	// Rejoin 后换新实例、rep 在 NewCluster 后才有——闭包内一律 Load()。
	var (
		mtPtr  atomic.Pointer[meta.Meta]
		prPtr  atomic.Pointer[produce.Producer]
		stPtr  atomic.Pointer[store.Store]
		repPtr atomic.Pointer[replication.Cluster]
	)
	stPtr.Store(st)
	if cfg.ClusterEnabled() {
		cc := cfg.Cluster
		// 确认档位映射：config 已校验合法值
		mode := cluster.AckQuorumMem
		if cc.Ack == "quorum-fsync" {
			mode = cluster.AckQuorumFsync
		}
		// 装配钩子（Options 注入，Ready 循环内同步触发、闭包内 go 出去）。
		// OnApplied 触发 meta.Reload：follower 盲 apply 不碰缓存，靠钩子
		// 重载；但只有触及 meta 键族的批次才值得整表重载（消息批次触发
		// Reload 是纯浪费），用 store.BatchTouchesPrefix 判定。空条目
		// （选举等）repr 为 nil，直接跳过（Task 2 审查注记）。
		// OnLeaderChange 触发 produce.InvalidateCounters：重新当选 leader
		// 后 offset 缓存可能滞后多数派事实，必须失效。
		//
		// 闭包经 atomic.Pointer（mtPtr/prPtr/stPtr/repPtr）读 mt/pr/st/rep：
		// 装配序把 Start 放在 meta.New 之前，Start 后立即触发的领导权事件
		//（单节点即刻当选）可能先于变量赋值——钩子触发时若依赖尚未构造，
		// Load() 得 nil 跳过该次（produce 未装配就没有缓存可失效，语义
		// 等价）。两个闭包在 NewManager 与 Rejoin 间复用：重入后的新
		// Manager 同样需要这两条装配线。
		onApplied := func(g uint32, repr []byte) {
			// 空条目（选举/成员变更）repr 为 nil：无 FSM 数据，跳过
			if len(repr) == 0 {
				return
			}
			mtv := mtPtr.Load()
			if mtv == nil {
				return // 装配窗口（Start→meta.New）内无缓存可重载
			}
			touches, terr := store.BatchTouchesPrefix(repr, []byte("meta/"))
			if terr != nil {
				// 批次解析失败只在坏字节时发生，apply 路径已经验过一遍，
				// 这里防御性跳过，不影响主链路
				logger.Warn("OnApplied 批次解析失败，跳过缓存重载", "g", g, "err", terr)
				return
			}
			if !touches {
				return // 不触及 meta 键族（纯消息批次），无需重载
			}
			go func() {
				// Reload 失败不致命（部分态）：下一条 applied 条目会
				// 再次触发重载，最终收敛（Task 6 审查注记）
				if rerr := mtv.Reload(); rerr != nil {
					logger.Error("meta 缓存重载失败（下一条 applied 条目会重试）", "g", g, "err", rerr)
				}
			}()
		}
		onLeaderChange := func(g uint32, leader uint64, isSelf bool) {
			if !isSelf {
				return
			}
			prv := prPtr.Load()
			if prv == nil {
				return // 装配窗口（Start→produce.New）内无缓存可失效
			}
			// 同步调用（评审 I4）：失而复得瞬间到 goroutine 执行之间，
			// 并发 Append 会用陈旧 offset 覆写已 quorum 提交的消息——
			// 失效必须在下一次 Append 前完成。只翻布尔锁内操作，不违反
			// 钩子「不得阻塞」契约（重活才 dispatch）。
			prv.InvalidateCounters()
		}
		// 控制处理器：本节点自己的 PrepareJoin（op=3）由 Manager 自装
		// 处理，这里只注册转发两个 op——见 cluster.Options 注释。
		// ForwardAppend 分支：解码消息 → pr.Append（本节点必为目标组
		// leader——发起方按 Leader(g) 寻址；错发时 Append 内的 propose
		// 自然报 ErrNotLeader 随控制帧回传）。ForwardApply 分支：重建
		// 批次 → cl.Apply 提案（构造无关批次，跨节点重放安全）。
		//
		// 注意：装配窗口（m.Start 之后、repPtr/stPtr 未 Store）内必须
		// 在此拦下——handleForwardApply 已改收具体指针（内部 cl == nil
		// 判空真实生效，评审 m2），但 stPtr 的 typed-nil 若放行会在
		// NewBatchFromRepr 处 nil 解引用 panic、无 recover、整进程崩。
		// 窗口真实：m.Start 之后、repPtr.Store(cl) 之前（等 meta leader
		// 期间）本节点可当选任意数据组 leader，I3 修复后 meta 写全走
		// ForwardApply，滚动重启对端流量完全可能打进窗口。
		// 装配窗口语义：转发失败 → 调用方（对端 producer）重试或报错，
		// 与「装配中」原有语义一致。
		controlHandler := func(op byte, payload []byte) ([]byte, error) {
			switch op {
			case cluster.OpForwardAppend:
				return handleForwardAppend(payload, prPtr.Load())
			case cluster.OpForwardApply:
				cl := repPtr.Load()
				st := stPtr.Load()
				if cl == nil || st == nil {
					return nil, errors.New("转发处理器未就绪（装配中）")
				}
				return handleForwardApply(payload, st, cl)
			default:
				return nil, fmt.Errorf("未知控制 op %d", op)
			}
		}
		opts := cluster.Options{
			NodeID:                 cc.NodeID,
			Peers:                  cc.PeerRaftAddrs(),
			ListenAddr:             cc.RaftListen, // 绑定地址（与 Peers[NodeID] 的通告地址分离，见 Options 注释）
			DataGroups:             cc.DataGroups,
			Mode:                   mode,
			Store:                  st,
			Logger:                 logger,
			LeaderBalancerInterval: leaderBalanceInterval,
			AutoPromoteLearners:    true, // 重入自愈的最后一环：追平后自动升 voter
			RetainEntries:          cc.LogRetainEntries,
			TruncateInterval:       cc.TruncateInterval,
			SnapshotChunkBytes:     cc.SnapshotChunkBytes,
			SnapshotViewTTL:        cc.SnapshotViewTTL,
			ReadBarrier:            cc.ReadBarrier,
			ReadBarrierTimeout:     cc.ReadBarrierTimeout,
			OnApplied:              onApplied,
			OnLeaderChange:         onLeaderChange,
			ControlHandler:         controlHandler,
		}
		m, err = cluster.NewManager(opts)
		if errors.Is(err, cluster.ErrUncleanShutdown) {
			// 无人值守自愈是集群模式默认行为：本节点先向存活 leader 求得
			// 接纳（PrepareJoin），拿到之后才清空数据目录以 learner 重入。
			// 求不到接纳时 Rejoin 报错且**不清空**，本进程随即拒启——此时
			// 本地那份数据是集群最后的兜底，宁可停机等人工恢复多数派，也
			// 不能先毁了它（顺序红线见 cluster.Rejoin 文档注释）。
			logger.Error("检测到不干净关机，开始重入编排（先求集群接纳，成功后才清空数据目录）", "dir", cfg.DataDir)
			if cerr := st.Close(); cerr != nil {
				return fmt.Errorf("关闭旧 store 后重入: %w", cerr)
			}
			opts.Store = nil // Rejoin 忽略 Store/Listener，内部按 dataDir 重开
			m, err = cluster.Rejoin(runCtx, opts, cfg.DataDir)
			if err != nil {
				// 拒启而非续跑：编排失败时数据目录未被清空，这份本地数据
				// 是恢复的唯一依据。运维恢复多数派后重启本节点即可自愈。
				logger.Error("集群重入编排失败，进程拒启；本地数据目录未被清空，恢复多数派后重启本节点即可重试",
					"dir", cfg.DataDir, "err", err)
				return fmt.Errorf("集群重入失败（本地数据未清空，可待多数派恢复后重启）: %w", err)
			}
			st = m.Store() // Rejoin 内部重开了 store，后续装配用新实例
			stPtr.Store(st)
			// Rejoin 内部已 Start（第 5 步），不重复 Start
		} else if err != nil {
			return err
		} else {
			m.Start(runCtx)
		}
		// 等 meta 组出 leader（60s 超时）：meta 组 leader 悬空时全部
		// meta 写路径（建 topic/group、事务暂存、handle secret）都会
		// 失败，半残启动比拒绝启动更糟——超时报错退出。
		//
		// 超时退出前必须显式 StopClean：StopClean 的 defer 注册在下面
		// （早于任何定时器 defer，晚于本调用），这条错误路径不会经过它
		// ——Manager 已在 Start 运行，裸 return 会留下「无干净关机标记」
		// 的盘面，下次启动误判不干净、Wipe + 重入，把升级前的存量数据
		// 清掉（单节点升级形态实测踩坑：首次集群启动慢 + 重启 = 数据
		// 丢失）。StopClean 超时兜底，失败仅记日志（进程即将退出）。
		if err := waitMetaLeader(m, logger, metaLeaderWaitTimeout); err != nil {
			sctx, cancel := context.WithTimeout(context.Background(), stopCleanTimeout)
			if cerr := m.StopClean(sctx); cerr != nil {
				logger.Error("等 meta leader 超时退出前清理失败（下次启动将走 learner 重入）", "err", cerr)
			}
			cancel()
			return err
		}
		// 集群后端三合一：*replication.Cluster 同时实现 Replicator、
		// Router 与 Forwarder（rt 断言取得 fwd，见 txn.New/delay.New）
		cl := replication.NewCluster(m)
		rep, rt, fwd = cl, cl, cl
		repPtr.Store(cl) // 钩子闭包（ControlHandler 的 ForwardApply 分支）原子读
		rv = clusterRouteView{m: m, cc: cc}
		// 停机顺序：gRPC 排空（上面 select 分支）→ 定时器退出（各自 defer）
		// → Manager.StopClean → st.Close。本 defer 注册在 st.Close 的 defer
		// 之后（LIFO 先执行），且晚于各定时器 defer（先执行），顺序正好
		// 是「定时器退出 → StopClean → st.Close」。
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), stopCleanTimeout)
			defer cancel()
			if err := m.StopClean(sctx); err != nil {
				logger.Error("集群干净停机失败（下次启动将走 learner 重入）", "err", err)
			}
		}()
	} else {
		rep = replication.NewStandalone(st)
		rt = replication.StandaloneRouter{}
		rv = rpc.StaticRouteView(cfg)
	}

	mt, err = meta.New(rep, rt, st, cfg.AutoCreateTopic, cfg.DefaultQueueNums, cfg.DefaultMaxAttempts, logger)
	if err != nil {
		return err
	}
	mtPtr.Store(mt) // OnApplied 钩子闭包原子读
	pr = produce.New(rep, rt, st, mt, logger)
	prPtr.Store(pr) // OnLeaderChange 钩子闭包原子读
	dl := deliver.New(rep, rt, st, mt, pr, logger)

	// 事务管理器。构造顺序有讲究：rpc.Server 要拿它处理 Send/EndTransaction，
	// 回查调度器又要拿 rpc.Server 当 Notifier（下发回查命令）——先建 Manager、
	// 再建 Server、最后起调度 goroutine，依赖环在构造期就被拆开。
	tx := txn.New(rep, rt, st, pr, mt, cfg.TxnInterval(), cfg.TxnMaxChecks, logger)

	// writeBlocked 由 retention 每趟探测磁盘后更新，rpc.SendMessage 据此拒写。
	// 必须先于 metrics registry 创建：registry 里的系统 Collector 要拿着
	// sysinfo.Reporter，而 Reporter 持有的正是这个开关的指针。
	writeBlocked := &atomic.Bool{}
	// sysinfo 采集器：retention 的水位判定、/metrics 的 sq_disk_* 与控制台的
	// /admin/system 三方共用它，保证看到的是同一份磁盘事实。
	sys := sysinfo.New(cfg.DataDir, cfg.DiskWatermarkPercent, writeBlocked, logger)
	// receipt handle 签名密钥：首次启动生成并持久化，此后原样加载（重启不换钥，
	// 在途 handle 跨重启仍有效）。必须在 rpc.New 之前加载——Server 用它给
	// 每个 handle 加签/验签，无密钥则全部 ack/改不可见时长请求会被拒。
	// 集群档走 Replicated 变体：密钥经 MetaGroup 复制，三节点同值——客户端
	// 在节点间轮询时任一节点都能验签；非 leader 节点轮询等 leader 写入
	//（leader 首次启动会生成，见 rpc/handle_secret.go）。
	var handleSecret []byte
	if cfg.ClusterEnabled() {
		handleSecret, err = rpc.LoadOrCreateHandleSecretReplicated(runCtx, st, rep, rt, logger)
	} else {
		handleSecret, err = rpc.LoadOrCreateHandleSecret(st, logger)
	}
	if err != nil {
		return err
	}
	// rpc.Server 需在 metrics 块之前构造：metrics（/metrics 的
	// sq_connections）与 admin 控制台都要拿 srv.ConnectionCount。rpc.New
	// 无副作用，上移不改变任何行为。
	// 路由视图：单机形态恒指向本节点（StaticRouteView）；集群形态指向
	// 各队列所属组的当前 leader（clusterRouteView，见 clusterview.go）
	srv := rpc.New(cfg, rv, mt, pr, dl, tx, writeBlocked, handleSecret, logger)

	// metrics registry 必须先于任何后台 goroutine 装配：NewRegistry 会写包级
	// 钩子 store.OnApplyObserve，其契约是「装配阶段设置一次、之后只读」——
	// retention/delay 的 goroutine 启动即可能走 store.Apply 读这个钩子，
	// 放到它们之后装配就是契约禁止的无同步并发读写。admin_listen 为空 =
	// 不装配（钩子保持 nil，Apply 路径零开销）。
	var reg *prometheus.Registry
	var sp *metrics.Sampler
	if cfg.AdminListen != "" {
		reg = metrics.NewRegistry(st, mt, sys, tx, srv, dl, logger)

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
	rm := retention.New(rep, rt, st, mt, cfg.RetentionInterval(), cfg.DataDir, cfg.DiskWatermarkPercent, writeBlocked, logger)
	retWG.Add(1)
	go func() { defer retWG.Done(); rm.Run(retCtx) }()
	defer func() { retCancel(); retWG.Wait() }()

	// delay 调度器：到期延时消息移入正常队列。停机顺序与 retention 同理——
	// defer LIFO 保证先取消并等待调度 goroutine 退出，再轮到 st.Close 的 defer，
	// 不会在 store 关闭后提交搬运批次。
	dlyCtx, dlyCancel := context.WithCancel(context.Background())
	var dlyWG sync.WaitGroup
	ds := delay.New(rep, rt, st, pr, logger)
	dlyWG.Add(1)
	go func() { defer dlyWG.Done(); ds.Run(dlyCtx) }()
	defer func() { dlyCancel(); dlyWG.Wait() }()

	// txn 回查调度器：到期未决半消息经 Telemetry 下发回查。停机顺序同
	// retention/delay——defer LIFO 保证先取消并等待调度 goroutine 退出，
	// 再轮到 st.Close。停机窗口内的回查下发会因流已收尾而写失败，
	// RecoverOrphan 按 Warn 处理、条目已改期，重启后自然继续。
	txnCtx, txnCancel := context.WithCancel(context.Background())
	var txnWG sync.WaitGroup
	txnWG.Add(1)
	go func() { defer txnWG.Done(); tx.RunChecker(txnCtx, srv) }()
	defer func() { txnCancel(); txnWG.Wait() }()

	// Admin HTTP（含 /metrics）。admin_listen 为空 = 关闭。停机顺序：本 defer
	// 注册在 st.Close 的 defer 之后（LIFO 先执行），保证 handler 不会在 store
	// 关闭后还在读写它。
	if cfg.AdminListen != "" {
		// fwd 单机档为 nil：StandaloneRouter 的 IsLeader 恒真，adminops 的
		// 转发分支不可达（nil 不会被解引用）；集群档即 *replication.Cluster，
		// adminops 成片清理跨组时经它转发给目标组 leader。
		// 集群拓扑来源：集群档的 rt（*replication.Cluster）同时实现
		// Topologer；单机档的 StandaloneRouter 也实现（回 enabled=false）。
		topo, _ := rt.(replication.Topologer)
		adm := admin.New(rep, rt, fwd, topo, st, mt, pr, dl, cfg.AdminUsername, cfg.AdminPassword, sys, sp, reg, srv, logger)
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
	// AK/SK 认证按配置装配（spec §6 默认关闭，凭据列表为空即不装）。拦截器必须
	// unary+stream 成对装：只装 unary 会让 ReceiveMessage/Telemetry 两条流绕过认证。
	if len(cfg.Credentials) > 0 {
		au, as := rpc.NewAuthInterceptors(cfg.Credentials, logger)
		gopts = append(gopts, grpc.ChainUnaryInterceptor(au), grpc.ChainStreamInterceptor(as))
		logger.Info("gRPC AK/SK 认证已启用", "credentials", len(cfg.Credentials))
	}
	gs := grpc.NewServer(gopts...)
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
	if cfg.ClusterEnabled() {
		logger.Info("sq 已启动（集群模式）", "grpc_listen", cfg.GRPCListen,
			"advertise", cfg.AdvertiseHost, "data_dir", cfg.DataDir, "fsync", cfg.Fsync,
			"message_encoding", cfg.MessageEncoding,
			"node_id", cfg.Cluster.NodeID, "data_groups", cfg.Cluster.DataGroups,
			"ack", cfg.Cluster.Ack, "peers", len(cfg.Cluster.Peers))
	} else {
		logger.Info("sq 已启动（单机模式）", "grpc_listen", cfg.GRPCListen,
			"advertise", cfg.AdvertiseHost, "data_dir", cfg.DataDir, "fsync", cfg.Fsync,
			"message_encoding", cfg.MessageEncoding,
			"txn_check_interval", cfg.TxnCheckInterval, "txn_max_checks", cfg.TxnMaxChecks)
	}

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

// metaLeaderWaitTimeout 等 meta 组出 leader 的上限。取值 60s：集群首次
// 启动的选举在秒级完成，60s 只为吸收「同机多节点 + 机器慢」的极端抖动；
// 超过它说明集群起不来（无 quorum / 选举故障），按启动失败退出——半残
// 启动比拒绝启动更糟（全部 meta 写路径会以 HA_NOT_AVAILABLE 毒化客户端
// 路由缓存）。
const metaLeaderWaitTimeout = 60 * time.Second

// leaderBalanceInterval 集群档的 leader 摊布/自动升 voter 循环节拍。
// 取 5s：摊布是控制面动作（向 preferred 节点转移领导权），秒级即可，
// 过短会在节点抖动期制造无谓转移；AutoPromoteLearners 与该循环共节拍，
// 重入节点追平后最多一个节拍内被升回 voter。
const leaderBalanceInterval = 5 * time.Second

// stopCleanTimeout Manager.StopClean 的等待上限。StopClean 等待全部组
// run 循环退出并写干净关机标记：组退出是本地动作（取消 run ctx 后
// Ready 循环自然结束），30s 是「异常卡死时的兜底」而非常态预算。
const stopCleanTimeout = 30 * time.Second

// forwardOpTimeout 控制通道转发请求（ForwardAppend/ForwardApply）的单次
// 处理上限。转发落在目标组 leader 的提案路径上，正常毫秒级完成；30s 的
// 余量吸收 quorum 抖动，超时后对端按控制通道错误重试。
const forwardOpTimeout = 30 * time.Second

// waitMetaLeader 轮询直到 meta 组选出 leader，或超时报错。
//
// 为什么必须等：meta 组 leader 悬空时，一切 meta 键族写入（topic/group
// 注册、事务 half、handle secret）都会以 ErrNotLeader 失败——此时继续
// 装配 core 只会得到一个「起了但什么都写不进」的半残节点。等 leader
// 就位再装配，保证对外可见即可用（起不来比半残强）。
func waitMetaLeader(m *cluster.Manager, logger *slog.Logger, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if _, ok := m.Leader(cluster.MetaGroup); ok {
			logger.Info("meta 组 leader 已就绪")
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("meta 组在 %s 内未选出 leader（集群未就绪）", timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// handleForwardAppend 处理 OpForwardAppend 控制请求：把一条已编码消息交给
// 本节点 produce 栈追加（offset 在 leader 侧分配）。
//
// 载荷：payload=[4B BE 目标组][core.EncodeMessage 字节]；响应：
// [4B BE queueID][8B BE offset]（replication.ForwardAppend 的解析契约）。
//
// 注意：本节点必为目标组 leader——发起方按 Leader(g) 寻址；错发（leader
// 换手/寻址过期）时 Append 内的 propose 自然报 ErrNotLeader，随控制帧
// 错误文本回传，调用方据此重试。处理器运行在传输层读循环内（控制通道
// 短连接，一次往返），阻塞只影响本连接，与 handlePrepareJoin 同契约。
func handleForwardAppend(payload []byte, pr *produce.Producer) ([]byte, error) {
	if pr == nil {
		return nil, errors.New("转发处理器未就绪（装配中）")
	}
	if len(payload) < 4 {
		return nil, fmt.Errorf("ForwardAppend 载荷过短: %d B", len(payload))
	}
	m, err := core.DecodeMessage(payload[4:])
	if err != nil {
		return nil, fmt.Errorf("解码转发消息: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), forwardOpTimeout)
	defer cancel()
	stored, err := pr.Append(ctx, m)
	if err != nil {
		return nil, err
	}
	resp := make([]byte, 12)
	binary.BigEndian.PutUint32(resp[:4], stored.QueueID)
	binary.BigEndian.PutUint64(resp[4:], stored.Offset)
	return resp, nil
}

// handleForwardApply 处理 OpForwardApply 控制请求：把构造无关批次
// （纯 Delete/DeleteRange/绝对值 Set）提交到目标组。
//
// 载荷：payload=[4B BE 目标组][store.Batch.Repr 字节]；响应空。
//
// 注意：批次重建失败（坏字节）即拒绝——apply 路径不允许任何静默降级，
// 与 group.applyEntry 的失败哲学一致。提交失败（含 ErrNotLeader）随
// 控制帧错误文本回传调用方。形参收具体指针 *replication.Cluster（与
// handleForwardAppend 的 *produce.Producer 同形）——cl == nil 判空
// 真实生效（评审 m2：接口参数下 typed-nil 会穿透判空，判了等于没判）。
func handleForwardApply(payload []byte, st *store.Store, cl *replication.Cluster) ([]byte, error) {
	if cl == nil {
		return nil, errors.New("转发处理器未就绪（装配中）")
	}
	if len(payload) < 4 {
		return nil, fmt.Errorf("ForwardApply 载荷过短: %d B", len(payload))
	}
	g := binary.BigEndian.Uint32(payload[:4])
	b, err := st.NewBatchFromRepr(payload[4:])
	if err != nil {
		return nil, fmt.Errorf("重建转发批次: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), forwardOpTimeout)
	defer cancel()
	if err := cl.Apply(ctx, g, b); err != nil {
		return nil, err
	}
	return nil, nil
}

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
