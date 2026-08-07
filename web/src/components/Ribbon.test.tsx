import { describe, it, expect } from 'vitest'
import { render } from '@testing-library/react'
import { Ribbon } from './Ribbon'

function widths(container: HTMLElement) {
  return Array.from(container.querySelectorAll<HTMLElement>('.ribbon > span'))
    .map(s => s.style.width)
    // 位点标记与「已追平」是装饰性 span，不带宽度，只测真正表达落后量的两段
    .filter(Boolean)
    // jsdom 会把 "0.00%" 序列化成 "0%"，字符串比较不可靠，统一转成数值
    .map(w => parseFloat(w))
}

describe('Ribbon', () => {
  it('带子长度表达落后量，不是消费进度百分比', () => {
    // 这是画这类图最容易犯的错：按 cursor/head 的比例画，
    // 已追平的队列会是满格、落后一百万条的也接近满格，两者反而分不出来。
    const caught = render(<Ribbon cursor={1000} head={1000} fly={0} scale={1000} />)
    const behind = render(<Ribbon cursor={0} head={1000} fly={0} scale={1000} />)
    // 追平 = 空带子
    expect(widths(caught.container).every(w => w === 0)).toBe(true)
    // 落后满刻度 = 满带子
    expect(widths(behind.container).some(w => w === 100)).toBe(true)
  })

  it('同一刻度下落后越多带子越长', () => {
    const a = render(<Ribbon cursor={0} head={100} fly={0} scale={1000} />)
    const b = render(<Ribbon cursor={0} head={500} fly={0} scale={1000} />)
    const wa = widths(a.container)[1]
    const wb = widths(b.container)[1]
    expect(wb).toBeGreaterThan(wa)
  })

  it('在途段是落后段的一部分，两段之和等于总落后', () => {
    const { container } = render(<Ribbon cursor={0} head={1000} fly={250} scale={1000} />)
    const [fly, rest] = widths(container)
    expect(fly).toBeCloseTo(25, 1)
    expect(fly + rest).toBeCloseTo(100, 1)
  })

  it('追平时给出「已追平」文字，compact 模式下不给', () => {
    const full = render(<Ribbon cursor={9} head={9} fly={0} scale={100} />)
    expect(full.container.textContent).toContain('已追平')
    const compact = render(<Ribbon cursor={9} head={9} fly={0} scale={100} compact />)
    expect(compact.container.textContent).not.toContain('已追平')
  })
})