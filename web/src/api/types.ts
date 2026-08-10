/**
 * Admin API 响应类型
 *
 * 职责：
 *   - 把 M5a/M5b 的 Admin API 响应形状固化成 TS 类型，页面据此渲染
 *
 * 边界：
 *   - 字段名一律与后端 JSON tag 逐字一致（snake_case），不在这里改名：
 *     改名会让「页面读到的字段」与「curl 出来的字段」对不上，排查时多一层翻译
 *   - 只描述形状，不含任何派生计算；派生放 lib/derive.ts
 */

/** 总览卡片：一屏的全局计数与当前吞吐 */
export interface Overview {
  topics: number
  groups: number
  delay_depth: number
  /** 待决半消息数（事务消息尚未提交/回滚的暂存条数） */
  half_depth: number
  /** 在线连接数（rpc.Server 的会话计数；无连接时后端回 0） */
  connections: number
  total_written: number
  total_pending: number
  total_inflight: number
  total_dlq: number
  /** 采样器未启用或尚无样本时为 null，表示「不知道」而非「没有流量」 */
  qps: number | null
}

/** 数据目录所在文件系统的容量读数。 */
export interface DiskUsage {
  total_bytes: number
  free_bytes: number
  /** 与 df 同口径的已用百分比 */
  used_percent: number
}

/** 运行态系统读数（GET /admin/system）。 */
export interface SystemInfo {
  /** null = 探测失败或非 unix 平台，不是「磁盘为空」 */
  disk: DiskUsage | null
  /** 拒写水位线，0 = 水位保护关闭 */
  watermark_percent: number
  /** true = 当前拒写保读，生产端写入全部失败 */
  write_blocked: boolean
  /** null = 尚未成功统计过 */
  data_dir_bytes: number | null
  /** Go 运行时口径的堆占用，不是进程 RSS */
  go_heap_inuse_bytes: number
  /** Go 运行时向 OS 申请的总量 */
  go_sys_bytes: number
  goroutines: number
  uptime_seconds: number
}

/** 时序曲线上的一个采样点 */
export interface SeriesPoint {
  ts_ms: number
  qps: number
  pending: number
  dlq: number
  inflight: number
  delay_depth: number
}

/** 时序曲线响应的整体形状 */
export interface TimeSeries {
  range: '1h' | '24h' | '7d'
  granularity_ms: number
  source: 'ring' | 'pebble'
  points: SeriesPoint[]
}

/** 消费总账里某一行的队列级明细 */
export interface LedgerQueue {
  queue_id: number
  cursor: number
  next_offset: number
  inflight: number
}

/** 消费关系总账的一行：一个「组 × 主题」的消费进度 */
export interface LedgerRow {
  group: string
  topic: string
  cursor: number
  next_offset: number
  pending: number
  inflight: number
  /** 死信是消费组维度：同一组的各行显示同一个值 */
  dlq: number
  written_qps: number | null
  /** 0 = 尚未观察到位点推进（刚启动，或该组确实没在消费） */
  last_consume_ms: number
  // 追加：总账行的队列级明细由 queues 提供
  queues: LedgerQueue[]
}

/** 主题列表项 */
export interface Topic {
  name: string
  queues: number
  retention_ms: number
  created_at_ms: number
}

/** 主题详情：在列表项基础上多暴露每个队列的位点 */
export interface TopicDetail extends Topic {
  queues_detail: { queue_id: number; next_offset: number }[]
}

/** 消费组列表项 */
export interface Group {
  name: string
  max_attempts: number
  created_at_ms: number
}

/** 消费组×主题×队列 的进度 */
export interface QueueProgress {
  queue_id: number
  cursor: number
  next_offset: number
  pending: number
  inflight: number
}

/** 消费组详情：按主题分组的队列进度 */
export interface GroupDetail {
  name: string
  max_attempts: number
  topics: { topic: string; queues: QueueProgress[] }[]
}

/** 一条消息（body 为 base64，取值/展示时需要解码） */
export interface Message {
  id: string
  topic: string
  queue_id: number
  offset: number
  tag?: string
  keys?: string[]
  message_group?: string
  properties?: Record<string, string>
  body_base64: string
  born_at_ms: number
  store_at_ms: number
  deliver_at_ms?: number
}

/** 延时队列里的一条到期条目 */
export interface DelayEntry {
  due_ms: number
  msg_id: string
  topic: string
}

/** 事务半消息列表里的一条待决事务（按下次回查时间升序） */
export interface TxnEntry {
  tx_id: string
  msg_id: string
  topic: string
  /** 下一次回查时间（毫秒时间戳） */
  next_check_ms: number
  /** 已经发起过的回查次数 */
  checks: number
  /** 半消息暂存时刻（毫秒时间戳） */
  born_ms: number
}

/** 测试发送的返回：新消息落位信息 */
export interface SendResult {
  msg_id: string
  queue_id: number
  offset: number
  deliver_at_ms: number
  /**
   * true = 本条消息是经本节点转发给组 leader 写入的。
   *
   * 暴露给前端不是为了炫技：用户在 follower 上点发送、消息却写进了别的
   * 节点，这个事实必须可见，否则排查"我发的消息去哪了"时会先怀疑丢消息
   */
  forwarded?: boolean
}

/** 成员表里的一个节点 */
export interface ClusterNode {
  id: number
  raft_addr: string
  self: boolean
}

/** leader 视角下某个 peer 的复制进度。pending_snapshot 非零 = 正在被发快照 */
export interface PeerProgress {
  id: number
  match: number
  next: number
  state: string
  recent_active: boolean
  is_learner: boolean
  pending_snapshot: number
}

/**
 * 一个 raft 组在本节点视角下的状态。
 *
 * commit 是 raft 提交位点（每节点都有，follower 也有）；applied 是本节点
 * 已 apply 到位点。「待 apply = commit − applied」是每节点自己就算得出的
 * 落后量，不需要跨节点减法。
 *
 * peers 只在 is_leader 为 true 时有内容——raft 的复制进度只在 leader 上
 * 维护。渲染时必须按 is_leader 判断，空数组不等于"没有 peer"。
 */
export interface ClusterGroup {
  id: number
  leader: number
  is_leader: boolean
  role: string
  applied: number
  commit: number
  term: number
  peers: PeerProgress[]
}

/** 集群拓扑。enabled=false 表示当前是单机模式，不是故障 */
export interface ClusterView {
  enabled: boolean
  self_id: number
  nodes: ClusterNode[]
  groups: ClusterGroup[]
}