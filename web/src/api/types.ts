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
  total_written: number
  total_pending: number
  total_inflight: number
  total_dlq: number
  /** 采样器未启用或尚无样本时为 null，表示「不知道」而非「没有流量」 */
  qps: number | null
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

/** 测试发送的返回：新消息落位信息 */
export interface SendResult {
  msg_id: string
  queue_id: number
  offset: number
  deliver_at_ms: number
}