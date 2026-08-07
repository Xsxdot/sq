/**
 * 事务（半消息）页回归测试
 *
 * 职责：
 *   - 钉住表格渲染与空态：待决事务条目按列渲染，空列表显示「暂无待决事务」
 *
 * 边界：
 *   - 只测列表展示与空态，不测 limit 切换与轮询节奏（由 usePoll 与其
 *     他页面测试覆盖）
 */
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Transactions from './Transactions'

const txn = {
  tx_id: 'TX1',
  msg_id: 'M1',
  topic: 't',
  next_check_ms: Date.now() + 30000,
  checks: 2,
  born_ms: Date.now() - 1000,
}

function json(v: unknown) {
  return new Response(JSON.stringify(v), { status: 200, headers: { 'content-type': 'application/json' } })
}

describe('Transactions', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('渲染待决事务：事务ID / 消息ID / 已回查次数', async () => {
    const fetchMock = vi.fn(async (input: unknown) => {
      const url = String(input)
      if (url.startsWith('/admin/transactions')) return json([txn])
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    render(<MemoryRouter><Transactions /></MemoryRouter>)
    // 表格渲染出事务ID 与消息ID
    await screen.findByText('TX1')
    expect(screen.getByText('M1')).toBeTruthy()
    // 已回查次数在同一行内
    const row = screen.getByText('TX1').closest('tr')
    expect(row).toBeTruthy()
    expect(within(row!).getByText('2')).toBeTruthy()
    // 请求带默认 limit
    expect(fetchMock.mock.calls.some(c => String(c[0]).includes('/admin/transactions?limit=64'))).toBe(true)
  })

  it('空列表显示空态文案「暂无待决事务」', async () => {
    const fetchMock = vi.fn(async (input: unknown) => {
      const url = String(input)
      if (url.startsWith('/admin/transactions')) return json([])
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    render(<MemoryRouter><Transactions /></MemoryRouter>)
    await screen.findByText('暂无待决事务')
  })
})
