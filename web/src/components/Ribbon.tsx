/**
 * offset 带（控制台的签名元素）
 *
 * 职责：
 *   - 把「消费位点到写入头之间那段空隙」画成一条带子：
 *     斜纹段是已发出未确认，纯色段是尚未发出，空带子表示已追平
 *
 * 边界：
 *   - scale 由调用方传入且同一视图内必须一致——这是带子能横向比较的全部前提
 *   - 只画不算：落后量、刻度都由 lib/derive.ts 算好再传进来
 *
 * 不要改成按 cursor/head 的比例画：那样已追平的队列是满格、落后一百万条的
 * 也接近满格，恰好把最需要区分的两种情况画成一个样子。
 */
import { fmt } from '../lib/format'

export interface RibbonProps {
  cursor: number
  head: number
  fly: number
  /** 本视图共用的刻度（该视图的最大落后量） */
  scale: number
  /** 紧凑模式：省略下方的位点/落后文字，用于表格内嵌套的队列级明细 */
  compact?: boolean
  /** 加高的带子，用于详情页 */
  tall?: boolean
}

export function Ribbon({ cursor, head, fly, scale, compact, tall }: RibbonProps) {
  const gap = Math.max(0, head - cursor)
  const gapPct = (gap / Math.max(scale || 1, 1)) * 100
  const flyPct = gap ? (fly / gap) * gapPct : 0
  const sev = gap > 500 ? 'rib-warn' : 'rib-ok'
  return (
    <>
      <div className={tall ? 'ribbon tall' : 'ribbon'}>
        <span className="rib-fly" style={{ width: `${flyPct.toFixed(2)}%` }} />
        <span className={sev} style={{ width: `${(gapPct - flyPct).toFixed(2)}%` }} />
        <span className="cursor" />
        {gap === 0 && !compact && <span className="caught">已追平</span>}
      </div>
      {!compact && (
        <div className="prog">
          <span>位点 {fmt(cursor)}</span>
          <span>落后 {fmt(gap)}</span>
        </div>
      )}
    </>
  )
}