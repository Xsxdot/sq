/**
 * 消费组详情页回归测试
 *
 * 职责：
 *   - 钉住路由参数切换的即时刷新：浏览器前进/后退在 /groups/:name 间
 *     切换时，必须立刻重拉新组数据，不能把旧组数据继续当新组展示
 *
 * 边界：
 *   - 只测参数切换刷新，不测位点重置（确认框流程由人工验收覆盖）
 */
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, Route, Routes, useNavigate } from 'react-router-dom'
import GroupDetail from './GroupDetail'

const g1 = { name: 'g1', max_attempts: 3, topics: [] }
const g2 = { name: 'g2', max_attempts: 5, topics: [] }

function Switcher({ to }: { to: string }) {
  const nav = useNavigate()
  return <button onClick={() => nav(to)}>switch</button>
}

describe('GroupDetail', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('路由参数切换时立即重拉，不把旧组数据当新组展示', async () => {
    const fetchMock = vi.fn(async (input: unknown) => {
      const url = String(input)
      const data = url === '/admin/groups/g1' ? g1 : url === '/admin/groups/g2' ? g2 : null
      if (data) {
        return new Response(JSON.stringify(data), { status: 200, headers: { 'content-type': 'application/json' } })
      }
      throw new Error(`unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    render(
      <MemoryRouter initialEntries={['/groups/g1']}>
        <Switcher to="/groups/g2" />
        <Routes><Route path="/groups/:name" element={<GroupDetail />} /></Routes>
      </MemoryRouter>,
    )
    await screen.findByText('g1')
    fireEvent.click(screen.getByRole('button', { name: 'switch' }))
    // 修复前：新组数据要等下一个 5s 轮询周期才来，1s 超时即失败
    await screen.findByText('g2')
    expect(screen.queryByText('g1')).toBeNull()
    expect(fetchMock.mock.calls.some(c => String(c[0]) === '/admin/groups/g2')).toBe(true)
  })
})
