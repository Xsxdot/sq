/**
 * 集群页回归测试
 *
 * 职责：
 *   - 钉住三条容易搞错的地方：单机档渲染「单机模式」而不是错误；follower
 *     组不渲染复制进度段（peers 空不等于「集群里只有我一个」）；pending_
 *     snapshot 非零渲染「快照挂起」标记
 *
 * 边界：
 *   - 只测展示分支，不测轮询节奏（由 usePoll 覆盖）
 */
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Cluster from './Cluster'
import type { ClusterView } from '../api/types'

function view(over: Partial<ClusterView> = {}): ClusterView {
  return {
    enabled: true,
    self_id: 2,
    nodes: [
      { id: 1, raft_addr: '127.0.0.1:9081', self: false },
      { id: 2, raft_addr: '127.0.0.1:9082', self: true },
    ],
    groups: [
      { id: 0, leader: 1, is_leader: false, role: 'StateFollower', applied: 100, commit: 105, term: 3, peers: [] },
    ],
    ...over,
  }
}

function json(v: unknown) {
  return new Response(JSON.stringify(v), { status: 200, headers: { 'content-type': 'application/json' } })
}

/** 让 /admin/cluster 返回指定视图；其余 fetch 报错让测试暴露意外请求。 */
function mockCluster(v: ClusterView) {
  const fetchMock = vi.fn(async (input: unknown) => {
    const url = String(input)
    if (url.startsWith('/admin/cluster')) return json(v)
    throw new Error(`unexpected fetch: ${url}`)
  })
  vi.stubGlobal('fetch', fetchMock)
}

describe('Cluster', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('单机档渲染「单机模式」而不是错误', async () => {
    mockCluster(view({ enabled: false, nodes: [], groups: [] }))
    render(<MemoryRouter><Cluster /></MemoryRouter>)
    expect(await screen.findByText(/单机模式/)).toBeTruthy()
  })

  it('渲染节点卡并标出本节点', async () => {
    mockCluster(view())
    render(<MemoryRouter><Cluster /></MemoryRouter>)
    expect(await screen.findByText('127.0.0.1:9082')).toBeTruthy()
    expect(screen.getByText('本节点 · 节点 2')).toBeTruthy()
  })

  it('follower 组不渲染复制进度段（peers 空不等于没有 peer）', async () => {
    mockCluster(view())
    render(<MemoryRouter><Cluster /></MemoryRouter>)
    await screen.findByText('127.0.0.1:9082')
    expect(screen.queryByText(/peer 1：Match/)).toBeNull()
    expect(screen.getAllByText(/复制进度仅在本节点为该组 leader 时可见/).length).toBeGreaterThan(0)
  })

  it('leader 组渲染 peer 进度，快照挂起被标记', async () => {
    mockCluster(view({
      groups: [{
        id: 1, leader: 2, is_leader: true, role: 'StateLeader', applied: 500, commit: 505, term: 3,
        peers: [
          { id: 1, match: 500, next: 501, state: 'StateReplicate', recent_active: true, is_learner: false, pending_snapshot: 0 },
          { id: 3, match: 0, next: 1, state: 'StateSnapshot', recent_active: false, is_learner: false, pending_snapshot: 480 },
        ],
      }],
    }))
    render(<MemoryRouter><Cluster /></MemoryRouter>)
    expect(await screen.findByText(/peer 1：Match/)).toBeTruthy()
    expect(screen.getAllByText(/快照挂起/).length).toBeGreaterThan(0)
  })

  it('待 apply = commit − applied 由本节点数据直接得出', async () => {
    // follower 视角的组也能算出待 apply（不需要 leader 数据）
    mockCluster(view())
    render(<MemoryRouter><Cluster /></MemoryRouter>)
    await screen.findByText('127.0.0.1:9082')
    expect(screen.getByText(/待 apply 5/)).toBeTruthy()
  })
})
