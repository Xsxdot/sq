/**
 * 消费关系的派生计算
 *
 * 职责：
 *   - 从 ledger 行推导落后量、全表刻度、行状态
 *   - 折线路径生成（趋势图与迷你折线共用）
 *
 * 边界：
 *   - 纯函数，不碰 DOM、不发请求
 *   - 阈值（落后 500 条算「关注」）与 prototypes/base/shared/mock.js 的
 *     markOf 保持一致；要调阈值就两边一起调
 */
import type { LedgerRow } from '../api/types'

/** 单行落后量 = 写入头 − 位点。位点越过写入头（topic 删后重建）按 0 处理。 */
export const lagOf = (r: LedgerRow): number => Math.max(0, r.next_offset - r.cursor)

/**
 * 全表共用的刻度 = 最大落后量。
 *
 * 这是 offset 带能横向比较的前提：所有行用同一把尺，带子长短才有意义。
 * 下界取 1 是为了全部追平时不除零。
 */
export const maxLag = (rows: LedgerRow[]): number =>
  Math.max(1, ...rows.map(lagOf))

/** 行状态：有死信=异常，落后超阈值=关注，否则正常。死信优先于落后。 */
export function markOf(r: LedgerRow): 'm-ok' | 'm-warn' | 'm-bad' {
  if (r.dlq > 0) return 'm-bad'
  if (lagOf(r) > 500) return 'm-warn'
  return 'm-ok'
}

/**
 * 折线路径。vals 为空返回空串（调用方直接渲染空 path，不会画出 NaN）。
 * 单点序列把 x 固定在左边距，避免除以 (length-1)=0。
 */
export function linePath(vals: number[], w: number, h: number, pad: number): string {
  if (vals.length === 0) return ''
  const max = Math.max(...vals, 1)
  return vals
    .map((v, i) => {
      const x = vals.length === 1 ? pad : pad + (w - pad * 2) * (i / (vals.length - 1))
      const y = h - pad - (h - pad * 2) * (v / max)
      return `${i ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`
    })
    .join('')
}