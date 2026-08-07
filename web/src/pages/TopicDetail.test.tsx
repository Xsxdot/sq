/**
 * Topic 详情页回归测试
 *
 * 职责：
 *   - 钉住成功横幅的 JSX 渲染：Notice 只渲染纯文本 children，
 *     横幅里拼 HTML 字符串会把 <b> 当字面文本显示
 *   - 钉住路由参数切换时 retention 表单跟着新 topic 重填：
 *     表单初值只在数据首达时填一次，切 topic 不重填会把上一个 topic
 *     的 retention 保存到新 topic 头上
 *
 * 边界：
 *   - 只测保存横幅与表单重填，不测 PATCH 的 409 等错误分支
 */
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, Route, Routes, useNavigate } from 'react-router-dom'
import TopicDetail from './TopicDetail'

const t1 = {
  name: 't1', queues: 4, retention_ms: 259200000, created_at_ms: 0,
  queues_detail: [{ queue_id: 0, next_offset: 10 }],
}
const t2 = {
  name: 't2', queues: 4, retention_ms: 7200000, created_at_ms: 0,
  queues_detail: [{ queue_id: 0, next_offset: 3 }],
}

function json(v: unknown) {
  return new Response(JSON.stringify(v), { status: 200, headers: { 'content-type': 'application/json' } })
}

function route() {
  return vi.fn(async (input: unknown, init?: RequestInit) => {
    const url = String(input)
    const method = init?.method ?? 'GET'
    if (method === 'GET' && url === '/admin/ledger') return new Response('[]', { status: 200, headers: { 'content-type': 'application/json' } })
    if (method === 'GET' && url === '/admin/topics/t1') return json(t1)
    if (method === 'GET' && url === '/admin/topics/t2') return json(t2)
    if (method === 'PATCH' && url === '/admin/topics/t1') return new Response(null, { status: 204 })
    throw new Error(`unexpected ${method} ${url}`)
  })
}

function Switcher({ to }: { to: string }) {
  const nav = useNavigate()
  return <button onClick={() => nav(to)}>switch</button>
}

function harness(fetchMock: ReturnType<typeof route>) {
  vi.stubGlobal('fetch', fetchMock)
  return render(
    <MemoryRouter initialEntries={['/topics/t1']}>
      <Switcher to="/topics/t2" />
      <Routes><Route path="/topics/:name" element={<TopicDetail />} /></Routes>
    </MemoryRouter>,
  )
}

describe('TopicDetail', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('保存 retention 的成功横幅用 JSX 渲染，不把 <b> 当字面文本', async () => {
    const { container } = harness(route())
    await screen.findByDisplayValue('72')
    fireEvent.click(screen.getByRole('button', { name: '保存' }))
    await screen.findByText(/已把/)
    expect(container.textContent).toContain('已把 t1 的 retention 改为 72 小时')
    expect(container.textContent).not.toContain('<b>')
  })

  it('路由参数切换时 retention 表单跟着新 topic 重填', async () => {
    harness(route())
    await screen.findByDisplayValue('72')
    fireEvent.click(screen.getByRole('button', { name: 'switch' }))
    // 修复前：表单一直留着上一个 topic 的初值（72），新值 2 永远等不到
    await screen.findByDisplayValue('2')
  })
})
