import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { api, UNAUTHORIZED_EVENT, TOKEN_KEY } from './client'

describe('api client', () => {
  beforeEach(() => localStorage.clear())
  afterEach(() => vi.unstubAllGlobals())

  it('有 token 时带上 Authorization 头', async () => {
    localStorage.setItem(TOKEN_KEY, 'abc')
    const fetchMock = vi.fn().mockResolvedValue(
      new Response('{"topics":[]}', { status: 200, headers: { 'content-type': 'application/json' } }),
    )
    vi.stubGlobal('fetch', fetchMock)
    await api.get('/admin/topics')
    const headers = fetchMock.mock.calls[0][1].headers as Headers
    expect(headers.get('Authorization')).toBe('Bearer abc')
  })

  it('401 清 token 并广播未授权事件', async () => {
    localStorage.setItem(TOKEN_KEY, 'stale')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{"error":"token 无效"}', { status: 401 })))
    const spy = vi.fn()
    window.addEventListener(UNAUTHORIZED_EVENT, spy)
    await expect(api.get('/admin/topics')).rejects.toThrow()
    // token 必须当场作废：留着它会让接下来每个请求都白跑一趟再 401
    expect(localStorage.getItem(TOKEN_KEY)).toBeNull()
    expect(spy).toHaveBeenCalled()
  })

  it('错误响应把后端的 error 字段抛出来', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      new Response('{"error":"topic order.x 已存在"}', {
        status: 409, headers: { 'content-type': 'application/json' },
      }),
    ))
    // 前端不该自造错误文案：后端已经写清了原因，翻译一遍只会失真
    await expect(api.post('/admin/topics', { name: 'order.x' })).rejects.toThrow('topic order.x 已存在')
  })

  it('登录失败把后端错误文案透出，且不走失效拦截', async () => {
    // 登录端点的 401 是「用户名或密码错误」，不是会话失效：不能清 token、
    // 不能广播未授权事件，错误文案必须是后端那句（登录页头注释的承诺）
    localStorage.setItem(TOKEN_KEY, 'stale')
    const spy = vi.fn()
    window.addEventListener(UNAUTHORIZED_EVENT, spy)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      new Response('{"error":"用户名或密码错误"}', { status: 401 }),
    ))
    await expect(api.login('root', 'bad')).rejects.toThrow('用户名或密码错误')
    // 一次登录失败不该作废现有 token，也不该触发跳登录页
    expect(localStorage.getItem(TOKEN_KEY)).toBe('stale')
    expect(spy).not.toHaveBeenCalled()
  })

  it('probeAuth 在免登录服务端上返回 false', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      new Response('[]', { status: 200, headers: { 'content-type': 'application/json' } }),
    ))
    expect(await api.probeAuth()).toBe(false)
  })

  it('probeAuth 在需要登录的服务端上返回 true', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{"error":"缺少 Bearer token"}', { status: 401 })))
    expect(await api.probeAuth()).toBe(true)
  })
})