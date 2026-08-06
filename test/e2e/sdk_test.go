//go:build e2e

// 官方 rocketmq-clients Go SDK 端到端测试：M1 出口标准。
//
// 职责：
//   - 起一个真实的 sq broker 进程（由 cmd/sq 现场编译，监听真实 TCP 端口），
//     用官方 SDK 的 Producer 发普通消息、SimpleConsumer 收并 ack，
//     断言发出去的每一个 msgId 都被收到
//   - 锁定长轮询空结果的协议表述（MESSAGE_NOT_FOUND）
//   - 作为唯一一处「真实客户端 ↔ sq」互操作验证：其余单测全部用裸 protobuf
//     stub 驱动，走不到 SDK 的握手/协商逻辑，盖不住本文件覆盖的那些约束
//
// 为什么 broker 必须是独立进程，而不是进程内 goroutine：
//
//	sq 自己生成的 protobuf 绑定（internal/rpc/pb）与官方 SDK 自带的
//	protocol/v2 描述的是同一批 .proto 文件（apache/rocketmq/v2/*.proto，
//	proto package 也必须是 apache.rocketmq.v2——gRPC 方法路径就是由它拼出来的，
//	改名等于换一套协议）。protobuf-go 的全局注册表按文件路径与全限定名去重，
//	同一个进程里同时链接这两套绑定会在 init() 阶段直接 panic：
//	  panic: proto: file "apache/rocketmq/v2/definition.proto" is already registered
//	这不是 sq 的缺陷，而是「服务端自建绑定」与「客户端 SDK 自带绑定」共存的
//	固有约束。可以用 GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn 把 panic 降级，
//	但那要求每个调用方都记得带上这个环境变量，`go test -tags e2e ./...` 直接跑
//	就会炸；而且降级之后两套描述符谁赢是不确定的，等于在测试里引入一个说不清的
//	变量。起独立进程把两套绑定彻底隔开，代价只是一次 go build，换来的是
//	「测的就是将要发布的那个二进制」——连 main.go 的装配、配置加载与优雅停机
//	都一并覆盖了，比进程内拼装组件更接近生产。
//
// 边界：
//   - 只验证普通消息链路；延时/顺序/事务的 e2e 属 M3/M4/M6
//   - Tag 过滤只用 SUB_ALL（"*"），真实过滤属 M2
//   - 不验证集群路由/多 broker：sq M1 是单机形态
package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	sdkpb "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"gopkg.in/yaml.v3"

	"github.com/xushixin/sq/internal/config"
)

const (
	// preferredPort 首选监听端口（brief 指定）。被占用时回退到随机空闲端口，
	// 避免因为本机端口冲突把一次真实的协议回归误报成失败。
	preferredPort = 18081

	topicName = "e2e-normal"
	groupName = "e2e-group"
	msgCount  = 10 // 发送并期望收齐的消息条数

	// brokerStartTimeout 等待 broker 进程开始接受连接的上限。
	brokerStartTimeout = 30 * time.Second
	// brokerStopTimeout 发出 SIGTERM 后等待 broker 进程退出的上限，超时则
	// SIGKILL——测试不能因为被测进程不肯退出而挂死。必须明显大于 sq 自己的
	// gracefulStopTimeout（30s），否则测试会在 sq 的强制中断兜底生效之前
	// 就把它杀掉，等于把「sq 能否自己停下来」这件事测掉了。
	brokerStopTimeout = 45 * time.Second

	// brokerForcedStopBackstop 是 cmd/sq/main.go 里 gracefulStopTimeout 的值，
	// 只用于断言失败时的提示文案（那个常量不导出，这里刻意不去引用它——
	// e2e 不该为了一句报错信息去改被测代码的可见性）。
	brokerForcedStopBackstop = 30 * time.Second

	// brokerStopBudget SIGTERM 到进程退出之间允许的最长间隔。
	//
	// 取值依据是实测数据的两端：修好之后，producer + consumer 都在线时停机
	// 耗时 0.04s；退回去（不调 srv.Shutdown）则一定会走满 30s 的强制中断兜底。
	// 5s 落在这两个数量级中间——比正常值宽出两个数量级，不会因为 CI 机器慢、
	// 进程调度抖动而误报；又远低于 30s，回归一旦发生必然被抓住。
	brokerStopBudget = 5 * time.Second
)

// brokerBinary 由 TestMain 编译出来的 sq 可执行文件路径，全部用例共用。
var brokerBinary string

// TestMain 准备两类进程级前置条件，再运行全部用例。
//
// 官方 SDK 侧（两项都必须在任何 SDK 客户端构造之前完成，且只能按进程设置一次）：
//   - 日志：SDK 的 log.go 在 init() 里就按 rocketmq.client.logRoot 建好了
//     writer（默认写 $HOME/logs/rocketmq）。测试进程不该往用户家目录里写文件，
//     所以改指到临时目录后必须调 ResetLogger() 让它重建 writer——只 Setenv
//     是没用的，init() 早就跑完了。
//   - TLS：SDK 的 clientConn 无条件构造 TransportCredentials，是否启用 TLS
//     只看包级变量 EnableSsl（由 init() 从 rocketmq.client.enableSsl 读取，
//     默认 true）。sq 监听的是明文 gRPC，不关掉握手会直接失败。
//
// sq 侧：把 cmd/sq 编译成临时二进制，供各用例起独立 broker 进程使用。
func TestMain(m *testing.M) {
	logRoot, err := os.MkdirTemp("", "sq-e2e-rmq-log-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 SDK 日志临时目录失败: %v\n", err)
		os.Exit(1)
	}
	os.Setenv("rocketmq.client.logRoot", logRoot)
	rmq.ResetLogger()
	rmq.EnableSsl = false

	buildDir, err := os.MkdirTemp("", "sq-e2e-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建构建临时目录失败: %v\n", err)
		os.Exit(1)
	}
	brokerBinary = filepath.Join(buildDir, "sq")
	if out, err := buildBroker(brokerBinary); err != nil {
		fmt.Fprintf(os.Stderr, "编译 sq 失败: %v\n%s\n", err, out)
		os.RemoveAll(buildDir)
		os.RemoveAll(logRoot)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(buildDir)
	os.RemoveAll(logRoot)
	os.Exit(code)
}

// repoRoot 由本测试文件的源码路径反推仓库根目录（test/e2e/ 的上两级）。
// 不依赖进程当前工作目录：`go test` 会把工作目录设成被测包所在目录，
// 但这个约定不该被测试逻辑默默依赖，出错时也没有任何提示。
func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("无法定位测试源码路径")
	}
	root := filepath.Dir(filepath.Dir(filepath.Dir(file)))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("从 %s 反推出的仓库根 %s 下没有 go.mod: %w", file, root, err)
	}
	return root, nil
}

// buildBroker 把 cmd/sq 编译到 out 路径。
//
// 返回：
//   - 编译器输出（失败时用于定位，成功时通常为空）
//   - 错误
func buildBroker(out string) ([]byte, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command("go", "build", "-o", out, "./cmd/sq")
	cmd.Dir = root
	return cmd.CombinedOutput()
}

// pickPort 选一个可用端口：优先 preferredPort，被占用则让内核分配。
//
// 这里 bind 只为探测可用性，随即关闭把端口交给 broker 进程去 bind——
// 中间存在一个理论上的抢占窗口，但真抢上了 broker 会启动失败，
// startBroker 的就绪探测会带着 broker 自己的错误日志报出来，不会静默。
func pickPort(t *testing.T) int {
	t.Helper()
	if l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", preferredPort)); err == nil {
		l.Close()
		return preferredPort
	} else {
		t.Logf("首选端口 %d 不可用（%v），改用内核分配的空闲端口", preferredPort, err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("探测空闲端口失败: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startBroker 起一个独立的 sq broker 进程，返回 SDK 用的接入点地址。
//
// advertise_host/advertise_port 必须与进程实际监听的地址完全一致：
// QueryRoute 返回的 endpoints 就是 SDK 后续做 telemetry/send/receive 的目标，
// 对不上时表现为握手超时而不是「路由错」，很难定位。
//
// 返回：
//   - endpoint: "host:port" 形式的接入点，直接喂给 rmq.Config.Endpoint
func startBroker(t *testing.T) string {
	t.Helper()

	port := pickPort(t)
	dir := t.TempDir()
	cfg := &config.Config{
		GRPCListen:       fmt.Sprintf("127.0.0.1:%d", port),
		AdvertiseHost:    "127.0.0.1",
		AdvertisePort:    port,
		DataDir:          filepath.Join(dir, "data"),
		Fsync:            "sync",
		AutoCreateTopic:  true,
		DefaultQueueNums: 4,
		// debug 级别：broker 侧的投递/确认日志是排查「消息没到」的唯一线索，
		// 失败时由 dumpBrokerLog 打进测试输出。
		LogLevel: "debug",
	}
	// 用 config.Config 结构体序列化而不是手写 YAML 文本：配置项改名时这里
	// 会跟着变，不会悄悄写出一份 broker 读不懂（因而全部走默认值）的配置。
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("序列化 broker 配置失败: %v", err)
	}
	cfgPath := filepath.Join(dir, "sq.yaml")
	if err := os.WriteFile(cfgPath, raw, 0o600); err != nil {
		t.Fatalf("写 broker 配置失败: %v", err)
	}

	logPath := filepath.Join(dir, "broker.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("创建 broker 日志文件失败: %v", err)
	}

	cmd := exec.Command(brokerBinary, "-config", cfgPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("启动 broker 进程失败: %v", err)
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

	t.Cleanup(func() {
		stopBroker(t, cmd, waitDone)
		logFile.Close()
		// 只在失败时把 broker 日志灌进测试输出：成功时它是几百行 debug 噪音，
		// 失败时它是唯一能说明服务端到底做了什么的证据。
		if t.Failed() {
			dumpBrokerLog(t, logPath)
		} else {
			t.Logf("broker 日志: %s（用例通过，未展开）", logPath)
		}
	})

	endpoint := fmt.Sprintf("%s:%d", cfg.AdvertiseHost, cfg.AdvertisePort)
	waitBrokerReady(t, endpoint, waitDone, logPath)
	t.Logf("broker 已就绪，endpoint=%s pid=%d", endpoint, cmd.Process.Pid)
	return endpoint
}

// waitBrokerReady 轮询直到 broker 开始接受 TCP 连接。
// 进程提前退出时立即失败并展开日志，不傻等到超时——「起不来」和「起得慢」
// 是两种不同的故障，混成一条超时信息会让排查凭空多绕一圈。
func waitBrokerReady(t *testing.T, endpoint string, waitDone <-chan error, logPath string) {
	t.Helper()
	deadline := time.Now().Add(brokerStartTimeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-waitDone:
			dumpBrokerLog(t, logPath)
			t.Fatalf("broker 进程在就绪之前退出: %v", err)
		default:
		}
		conn, err := net.DialTimeout("tcp", endpoint, time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	dumpBrokerLog(t, logPath)
	t.Fatalf("broker 在 %v 内未开始监听 %s", brokerStartTimeout, endpoint)
}

// stopBroker 先 SIGTERM 走 main.go 的优雅停机路径，超时再 SIGKILL 兜底，
// 并对停机结果做断言——这是 main.go 停机链路唯一的回归护栏。
//
// 为什么这里必须 Errorf 而不是只 Logf：cmd/sq/main.go 的停机由两步组成，
// 顺序还是承重的——先 srv.Shutdown() 收尾没有自然终点的 Telemetry 长流，
// 再 gracefulStop() 排空在途 RPC。这两步任何一步被删掉或调换顺序，
// GracefulStop 都会挂在那条永不结束的流上，直到 30s 的强制中断兜底才退出。
// 而 30s < brokerStopTimeout(45s)，进程最终仍会以退出码 0 正常退出——
// 也就是说：**光看"能不能停下来"，回归是完全静默的，全部用例照样通过，
// 只是每条慢 30 秒。** rpc 包里的 TestShutdownEndsTelemetryStream 只能覆盖
// Server.Shutdown 本身，看不见 main.go 里的调用与顺序。
//
// 因此这里同时钉住两件事：
//   - 退出码为 0：SIGTERM 被正常处理，不是被信号杀死、也不是启动期就崩了
//   - SIGTERM 到退出的间隔在 brokerStopBudget 之内：证明走的是"主动收尾"
//     这条快路径，而不是掉进 30s 强制中断兜底
func stopBroker(t *testing.T, cmd *exec.Cmd, waitDone <-chan error) {
	t.Helper()
	if cmd.Process == nil {
		return
	}
	select {
	case err := <-waitDone:
		// 用例还没跑完 broker 就自己退出了：无论退出码是什么都是故障
		// （正常情况下它应当一直服务到本函数发出 SIGTERM 为止）。
		t.Errorf("broker 进程在用例结束前就已退出: %v", err)
		return
	default:
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Errorf("向 broker 发送 SIGTERM 失败: %v", err)
		return
	}
	sentAt := time.Now()
	select {
	case err := <-waitDone:
		elapsed := time.Since(sentAt)
		if err != nil {
			t.Errorf("broker 未能干净退出：SIGTERM 后 cmd.Wait 返回 %v（期望退出码 0）", err)
		}
		if elapsed > brokerStopBudget {
			t.Errorf("broker 收到 SIGTERM 后耗时 %v 才退出，超过预算 %v："+
				"说明优雅停机没走主动收尾路径，而是掉进了 cmd/sq/main.go 的 %v 强制中断兜底——"+
				"检查 srv.Shutdown() 是否仍在 gracefulStop() 之前被调用",
				elapsed, brokerStopBudget, brokerForcedStopBackstop)
		}
		t.Logf("broker 收到 SIGTERM 后 %v 退出（预算 %v）", elapsed, brokerStopBudget)
	case <-time.After(brokerStopTimeout):
		t.Errorf("broker 在 %v 内完全没有响应 SIGTERM，强制杀掉", brokerStopTimeout)
		cmd.Process.Kill()
		<-waitDone
	}
}

// dumpBrokerLog 把 broker 进程日志打进测试输出。
func dumpBrokerLog(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Logf("读取 broker 日志 %s 失败: %v", path, err)
		return
	}
	t.Logf("---- broker 日志 (%s) ----\n%s---- broker 日志结束 ----", path, raw)
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

// TestOfficialGoSDKSendAndReceive 官方 SDK 收发普通消息的端到端闭环，
// 也是 M1 的出口标准：发 msgCount 条 → SimpleConsumer 轮询收取并逐条 ack →
// 断言每一个发送回执里的 msgId 都被收到。
func TestOfficialGoSDKSendAndReceive(t *testing.T) {
	endpoint := startBroker(t)

	// ---- 生产 msgCount 条 ----
	producer, err := rmq.NewProducer(&rmq.Config{
		Endpoint:    endpoint,
		Credentials: &credentials.SessionCredentials{},
	}, rmq.WithTopics(topicName))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := producer.Start(); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	defer producer.GracefulStop()

	sent := make(map[string]bool, msgCount)
	for i := 0; i < msgCount; i++ {
		msg := &rmq.Message{Topic: topicName, Body: []byte(fmt.Sprintf("e2e payload #%d", i))}
		msg.SetTag("e2e")
		msg.SetKeys(fmt.Sprintf("k-%d", i))
		recs, err := producer.Send(context.Background(), msg)
		if err != nil {
			t.Fatalf("Send #%d 失败: %v", i, err)
		}
		if len(recs) == 0 {
			t.Fatalf("Send #%d 返回空回执", i)
		}
		t.Logf("已发送 #%d msgId=%s offset=%d", i, recs[0].MessageID, recs[0].Offset)
		sent[recs[0].MessageID] = true
	}
	if len(sent) != msgCount {
		t.Fatalf("发送回执 msgId 不唯一: 期望 %d 个，实际 %d 个", msgCount, len(sent))
	}

	// ---- SimpleConsumer 消费并 ack，直到收齐 ----
	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: groupName,
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(5*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			topicName: rmq.SUB_ALL,
		}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	defer consumer.GracefulStop()

	got := make(map[string]bool, msgCount)
	// SimpleConsumer.Receive 每次只轮询一个队列（SDK 内部按队列轮转），
	// topic 默认 4 个队列，因此必须循环若干轮才可能收齐 msgCount 条。
	deadline := time.Now().Add(60 * time.Second)
	rounds, emptyRounds := 0, 0
	for len(got) < len(sent) && time.Now().Before(deadline) {
		rounds++
		mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
		if err != nil {
			// 空轮询（MESSAGE_NOT_FOUND）在 SDK 里以错误形式返回，属正常，继续下一轮；
			// 但绝不能静默——真实故障（握手失败、路由错、过滤表达式被拒）同样走
			// 这条分支，若不打印，一次失败的 run 只会表现为「60 秒后收不齐」，
			// 无从判断卡在哪一步。
			emptyRounds++
			t.Logf("第 %d 轮 Receive 返回错误（空轮询或故障）: %v", rounds, err)
			continue
		}
		if len(mvs) == 0 {
			emptyRounds++
			t.Logf("第 %d 轮 Receive 返回 0 条", rounds)
			continue
		}
		for _, mv := range mvs {
			got[mv.GetMessageId()] = true
			if err := consumer.Ack(context.Background(), mv); err != nil {
				t.Fatalf("Ack msgId=%s 失败: %v", mv.GetMessageId(), err)
			}
		}
		t.Logf("第 %d 轮收到 %d 条，累计 %d/%d", rounds, len(mvs), len(got), len(sent))
	}

	t.Logf("消费结束：共 %d 轮（其中 %d 轮无结果），收到 %d/%d 条", rounds, emptyRounds, len(got), len(sent))
	for id := range sent {
		if !got[id] {
			t.Errorf("消息 %s 未收到", id)
		}
	}
	if len(got) != len(sent) {
		t.Fatalf("收齐失败: sent=%d got=%d", len(sent), len(got))
	}
}

// TestOfficialGoSDKEmptyPollReportsMessageNotFound 锁定「长轮询到期无消息」的
// 协议表述：服务端必须回 MESSAGE_NOT_FOUND，而不是 OK + 零条消息。
//
// 为什么这值得单独一条 e2e：两种回法 SimpleConsumer 都能跑通（非 OK 状态被它
// 翻译成 error 交回调用方循环），所以任何只看「能不能收到消息」的测试都盖不住
// 这个差异；但官方 SDK 的 push 消费者对这个码有专门分支——只有 MESSAGE_NOT_FOUND
// 才被识别为「没有新消息」并按流控退避重发取件请求，其它任何非 OK 都会被记成
// 真·异常。用 SDK 自己的错误类型断言，等于直接验证「SDK 眼里这是不是一次
// 正常的空轮询」，而不是验证我们自己写的那句中文。
func TestOfficialGoSDKEmptyPollReportsMessageNotFound(t *testing.T) {
	endpoint := startBroker(t)

	consumer, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: "e2e-empty-group",
		Credentials:   &credentials.SessionCredentials{},
	},
		rmq.WithSimpleAwaitDuration(2*time.Second),
		rmq.WithSimpleSubscriptionExpressions(map[string]*rmq.FilterExpression{
			"e2e-empty": rmq.SUB_ALL,
		}),
	)
	if err != nil {
		t.Fatalf("NewSimpleConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	defer consumer.GracefulStop()

	start := time.Now()
	mvs, err := consumer.Receive(context.Background(), 16, 30*time.Second)
	t.Logf("空 topic 上 Receive 返回：len=%d err=%v 耗时=%v", len(mvs), err, time.Since(start))

	if err == nil {
		t.Fatalf("空轮询应以 MESSAGE_NOT_FOUND 状态返回错误，实际返回 %d 条消息且无错误", len(mvs))
	}
	st, ok := rmq.AsErrRpcStatus(err)
	if !ok {
		t.Fatalf("空轮询错误应是 SDK 的 ErrRpcStatus（即由服务端 Status 翻译而来），实际 %T: %v", err, err)
	}
	if st.GetCode() != int32(sdkpb.Code_MESSAGE_NOT_FOUND) {
		t.Fatalf("空轮询状态码应为 MESSAGE_NOT_FOUND(%d)，实际 %d（message=%s）",
			sdkpb.Code_MESSAGE_NOT_FOUND, st.GetCode(), st.GetMessage())
	}
}
