# Roadmap：已知未做与残余

本文件收拢各卡 spec / plan / 审查中「本期不做、落 roadmap」的条目与已知残余，按子系统分组。格式：`- [来源] 条目——一句话说明`；已有缺陷卡的条目在来源里标卡号。来源记号：`spec:批` = `docs/superpowers/specs/2026-11-11-sq4-perf-batch-design.md`，`spec:SQ4.1` = `docs/superpowers/specs/2026-11-11-sq4.1-consume-state-in-memory-design.md`，`plan:X` = `docs/superpowers/plans/X-plan.md`，`卡 X` = 该卡评论，`批级` = SQ4 批级验收（cards/sq4@67e1eb4）。

## 投递 / 消费

- [spec:SQ4.1 §61/§73；spec:批 §201] inflight 二进制编码——内存权威态后热路径不再编解码 inflight，JSON 只剩检查点与装载；改格式需两步升级，另卡做
- [plan:sq4.1 §44；卡 SQ4.1] 集群逐次模式流水化（SQ4.1 方案 (ii)）——逐次复制模式下同队列提案仍受复制往返串行约束，流水化另卡
- [卡 SQ4.1 M7] drop / PrepareTransfer 等队列锁可超预算——取锁等待不受转移前回调 2s 预算约束，极端情况下回调超时
- [plan:sq4.1 §267；卡 SQ4.1] 后台检查点轮次失多数派时占住轮次信号量——`roundBudget = max(间隔, 1s)` 挂满期间转移前检查点取信号量失败、该轮转移放弃；间隔越大越明显
- [卡 SQ4.1] 停机前探测多数派——停机收尾检查点在已失多数派时只能等满预算，先探测可提前判定并给出明确日志
- [卡 SQ4.2 M4] ApplyAdminBatch 同队列可能双唤醒——无害，仅多一次空轮询
- [卡 SQ4.9 Q5；plan:sq4.9 §142；spec:SQ4.1 §73；spec:批 §201] 控制台改读内存态 / 快照——`/admin/overview`、`/admin/ledger`、`/admin/groups/{name}` 仍全扫盘上 inflight（指标部分已由 SQ4.9 承接）
- [卡 SQ4.9] standalone 档 delay / half 写在全局计数锁上串行——交架构师，以基准定是否拆锁
- [卡 SQ9；卡 SQ4.9 m-6] 集群档 SDK 延时发送与 Recall 路由挑错端点——5 次 3 次失败；修后补回 e2e 的 Recall 覆盖（SQ4.9 m-6）
- [卡 SQ5] meta.EnsureTopic 凭陈旧缓存 check-then-write——可能以默认队列数覆盖已存在 topic 的配置（疑似，待复现）

## 集群 / raft

- [spec:批 §51/§201；卡 SQ4.3 Minor-1] RawNode 自驱改造——现结构下 Wait 在无截止的 ctx（如 retention）上可挂到组停止，围栏误判已落日志；自驱是根治动机
- [spec:批 §201] raft 条目格式级的提案合并——SQ4.3 只在保序队列一次提交多条，条目格式不变；改条目格式需两步升级
- [spec:批 §178/§201] 快照拉取流水线化——审计发现 9，现为逐块请求-应答
- [spec:批 §201；批级] 开启 CheckQuorum——未开时 leader 不会自降，全集群停机时最后一个停的节点等满收尾预算（亦是 RecentActive 不衰减、摊布需另做存活探测的原因）
- [spec:批 §201] 调大 MaxCommittedSizePerReady——审计发现，未列入本批
- [卡 SQ4.2] 新 leader 提交前任期条目只靠 100ms tick——当选后无主动推进，前任期尾部可见性最多晚一个 tick
- [卡 SQ1 M-3] 持锁调 pr.Append 新增 ≤3s 门等待——deliver ForwardToDLQ、txn End 在持锁路径上可能多等最多 3s；交架构
- [卡 SQ1 M-4] 可写门活性无兜底告警——门长期不开时没有专门告警
- [卡 SQ4.10 M1] seglog 锁跨 IO——冷读与 raft 主循环争同一把锁；交架构师
- [卡 SQ4.10 M2；plan:sq4.10 §7 残余] follower / 热区未 apply 尾部无上界——热区上界实为「2× 窗口 + 未 apply 尾部」，apply 长期落后时随之增长
- [卡 SQ4.10 M5] follower 侧截断不感知死 peer——follower 只按条数与字节截断，死 peer 宽限期只在 leader 侧生效
- [卡 SQ4.10 M7] 拨号失败丢 MsgSnap——靠停滞上报兜底，快照发送恢复慢
- [卡 SQ4.10] staleMsgAge=1s 与写期限 3s 在高延迟链路可能退化——反复丢 MsgApp，待基准
- [卡 SQ7；卡 SQ4.3 Minor-2] raft panic「message should not be self-addressed」——传输层收消息不校验 To==self，测试端口复用串话可触发；生产上同 ID 陈旧连接同样会 panic
- [卡 SQ6] Rejoin 重开 store 硬编码 sync=false——未沿用 fsync 配置，`fsync: sync` 在重入后静默失效（实质影响小）

## 存储

- [spec:批 §90/§201；plan:sq4.5 §41] 按机器内存自动推算缓存大小——现为固定缺省值，需手配
- [卡 SQ4.10 M6] 冷读每次 Open + 256KiB 分配——追平期间读放大与分配压力，可缓存只读句柄与缓冲
- [卡 SQ4.10] 重启节点 VmHWM 914MiB 根因待测——GC 余量还是存活对象未分辨
- [卡 SQ4.8 M4] 有 key 的 topic 单趟清理上限下降——保留期清理改造后，带 key 的 topic 每趟能清的条数变少

## 指标 / 控制台

- [卡 SQ4.9；批级] 快照安装后的重计数停顿——计数在快照安装后需全扫重建，停顿时长未实测
- （控制台改读内存态见「投递 / 消费」）

## 测试 harness

- [卡 SQ4.1 M6] 批级全量加 -v 留 S2/S3 计数——SQ4 批级验收已按此保留计数行，可关闭
- [plan:sq4.1 §297；卡 SQ4.1；卡 SQ4.2] e2e 的 ack 校验响应码——官方 Go SDK `SimpleConsumer.Ack` 吞非 OK 码，SQ4.1 遗留缺陷因此漏网；凡用 SDK Ack 判成功的 e2e 应改原始 gRPC 或校验码
- [卡 SQ8；卡 SQ4.1 m-4] TestOfficialGoSDKFIFOBlockedUntilAck 对顺序锁无牙——SDK 4 队列 × 5s 轮换，观察窗口 ≥20s
- [卡 SQ4.3 Minor-4] deliver TestSQ2 对批内倒序不敏感——批内条目倒序不会让用例变红
- [卡 SQ4.10] TestProposerTermChangeFailsQueuedEntries 偶发时序竞争——SQ1 用例，未定位
- [卡 SQ1；卡 SQ4.3] e2e 夹具配置含 admin_port，v0.1.0 混跑不可用——旧版严格解析拒绝未知键
- [批级 M-2 排查] 官方 Go SDK 路由 30s 才刷新——故障窗口短于 30s 的 e2e 场景里，存活 leader 收不到被接管组的消费流量；设计故障注入用例时须让窗口跨过一次刷新，否则对「接管期」行为无判别力
- [批级] 未实测或未对账的性能项——SQ4.4 S3 的 p99 与 conc=256 数字；SQ4.11 真实掉电；SQ2 E3 conc=64/256 的「低于 main 90%」疑似本机噪声，均未在安静环境复测定论（SQ4.9 重计数停顿见「指标 / 控制台」）

## 运维 / 部署

- [批级 M-5] 最坏停机预算超过 `deploy/sq.service` 的 TimeoutStopSec=45——各阶段上限叠加可超 45s，systemd 会 SIGKILL，退化为不干净关机走重入路径
