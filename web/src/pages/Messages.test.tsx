/**
 * 消息查询页回归测试
 *
 * 职责：
 *   - 钉住查询入口与 tab 的一致性：走哪个入口由当前 tab 决定，
 *     而不是由某个输入框是否有内容决定
 *
 * 边界：
 *   - 只测查询分流与结果标签，不测消息详情弹窗
 */
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Messages from './Messages'

const topic = { name: 't1', queues: 4, retention_ms: 259200000, created_at_ms: 0 }
const topicDetail = {
  ...topic,
  queues_detail: [{ queue_id: 0, next_offset: 5 }],
}
const msg = {
  id: 'm-1',
  topic: 't1',
  queue_id: 0,
  offset: 3,
  keys: ['ORD-1'],
  body_base64: btoa('{"a":1}'),
  born_at_ms: 0,
  store_at_ms: 0,
}

function json(v: unknown) {
  return new Response(JSON.stringify(v), { status: 200, headers: { 'content-type': 'application/json' } })
}

describe('Messages', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('切到按队列浏览后查询走 queue_id，而不是沿用 Keys 输入框里的内容', async () => {
    const fetchMock = vi.fn(async (input: unknown) => {
      const url = String(input)
      if (url === '/admin/topics') return json([topic])
      if (url === '/admin/topics/t1') return json(topicDetail)
      if (url.startsWith('/admin/messages')) return json([msg])
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    render(<MemoryRouter><Messages /></MemoryRouter>)
    // topic 下拉就绪、队列细节拉到
    await vi.waitFor(() => {
      expect(fetchMock.mock.calls.some(c => String(c[0]) === '/admin/topics/t1')).toBe(true)
    })
    // 先在「按 Keys 检索」里输入内容，再切到「按队列浏览」查询
    fireEvent.change(screen.getByPlaceholderText(/业务 Keys/), { target: { value: 'ORD-1' } })
    fireEvent.click(screen.getByRole('button', { name: '按队列浏览' }))
    fireEvent.click(screen.getByRole('button', { name: '查询' }))
    await screen.findByText(/自 offset/)

    // 请求参数必须按 tab（queue）来，不能带着 Keys 输入框的残留
    const msgCall = fetchMock.mock.calls.map(c => String(c[0])).find(u => u.includes('/admin/messages'))
    expect(msgCall).toBeTruthy()
    expect(msgCall).toContain('queue_id=0')
    expect(msgCall).not.toContain('key=')
    // 结果标签同样按 tab 显示
    expect(screen.getByText(/queue 0 · 自 offset 0 起/)).toBeTruthy()
  })
})
