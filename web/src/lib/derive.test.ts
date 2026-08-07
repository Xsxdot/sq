import { describe, it, expect } from 'vitest'
import { lagOf, maxLag, markOf, linePath } from './derive'
import type { LedgerRow } from '../api/types'

const row = (o: Partial<LedgerRow>): LedgerRow => ({
  group: 'g', topic: 't', cursor: 0, next_offset: 0, pending: 0, inflight: 0,
  dlq: 0, written_qps: null, last_consume_ms: 0, queues: [], ...o,
})

describe('derive', () => {
  it('落后量 = 写入头 − 位点', () => {
    expect(lagOf(row({ cursor: 100, next_offset: 340 }))).toBe(240)
  })

  it('位点越过写入头时落后量按 0 处理', () => {
    // topic 删后重建会让 alloc 归零而 cursor 还留着旧值，
    // 不兜住就会画出负长度的带子
    expect(lagOf(row({ cursor: 500, next_offset: 10 }))).toBe(0)
  })

  it('maxLag 取全表最大且不小于 1', () => {
    expect(maxLag([row({ next_offset: 30 }), row({ next_offset: 900 })])).toBe(900)
    // 全部追平时刻度不能是 0，否则算百分比会除零
    expect(maxLag([row({}), row({})])).toBe(1)
  })

  it('行状态：有死信=异常，落后超阈值=关注，否则正常', () => {
    expect(markOf(row({ dlq: 1 }))).toBe('m-bad')
    expect(markOf(row({ next_offset: 501 }))).toBe('m-warn')
    expect(markOf(row({ next_offset: 10 }))).toBe('m-ok')
    // 死信优先于落后：有死信的行即使追平了也得是异常
    expect(markOf(row({ dlq: 3, next_offset: 0 }))).toBe('m-bad')
  })

  it('linePath 不产生 NaN', () => {
    // 全零序列、单点序列都是真实会出现的输入（刚启动时）
    expect(linePath([0, 0, 0], 100, 20, 2)).not.toContain('NaN')
    expect(linePath([5], 100, 20, 2)).not.toContain('NaN')
    expect(linePath([], 100, 20, 2)).toBe('')
  })
})