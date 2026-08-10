/**
 * 发送测试消息页回归测试
 *
 * 职责：
 *   - 钉住 follower 写转发 UX 的两条断言：forwarded=true 时渲染转发说明；
 *     ErrNotLeader 错误渲染带集群页链接的可操作指引
 *
 * 边界：
 *   - 只测发送结果的两条分支，不测表单字段校验（那是交互细节）
 */
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Send from './Send'

function json(v: unknown) {
  return new Response(JSON.stringify(v), { status: 200, headers: { 'content-type': 'application/json' } })
}

/** 构造 fetch mock：topics 恒返回一个 topic，send 的行为由 sendImpl 决定。 */
function mockApi(sendImpl: (init: RequestInit) => Response | Promise<Response>) {
  const fetchMock = vi.fn(async (input: unknown, init?: RequestInit) => {
    const url = String(input)
    if (url.startsWith('/admin/topics')) {
      return json([{ name: 't1', queues: 4, retention_ms: 0, created_at_ms: 0 }])
    }
    if (url.startsWith('/admin/messages/send')) {
      return sendImpl(init ?? {})
    }
    throw new Error(`unexpected fetch: ${url}`)
  })
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

async function submit() {
  // 表单默认选中第一个 topic（t1）；直接点发送
  const btn = await screen.findByRole('button', { name: '发送' })
  fireEvent.click(btn)
}

describe('Send', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('forwarded=true 时渲染「已自动转发给 leader」说明', async () => {
    mockApi(() =>
      json({ msg_id: 'M1', queue_id: 7, offset: 42, deliver_at_ms: 0, forwarded: true }),
    )
    render(<MemoryRouter><Send /></MemoryRouter>)
    submit()
    expect(await screen.findByText(/已自动转发给 leader 节点写入/)).toBeTruthy()
  })

  it('ErrNotLeader 错误渲染带集群页链接的可操作指引', async () => {
    mockApi(() => {
      const err = { error: '本节点不是该组 leader，提案被拒绝: 组 1 当前 leader=3' }
      return new Response(JSON.stringify(err), { status: 500, headers: { 'content-type': 'application/json' } })
    })
    render(<MemoryRouter><Send /></MemoryRouter>)
    submit()
    const guidance = await screen.findByText(/本节点当前不是该组 leader 且转发未成功/)
    expect(guidance).toBeTruthy()
    // 指引附一个跳 /cluster 的链接
    const link = document.querySelector('a[href="/cluster"]')
    expect(link).toBeTruthy()
  })
})
