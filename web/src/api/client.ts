/**
 * Admin API 客户端
 *
 * 职责：
 *   - 统一 fetch 封装：注入 Bearer token、解析统一错误形状 {"error": "..."}
 *   - 401 时作废本地 token 并广播 UNAUTHORIZED_EVENT，由 Shell 负责跳登录
 *     （登录端点除外：那里的 401 是凭据错误，只透出后端文案，不走失效拦截）
 *   - 提供 login / logout / probeAuth 三个认证相关动作
 *
 * 边界：
 *   - 不做重试、不做缓存：控制台每 5 秒轮询一次，失败下一轮自然重来；
 *     加重试反而会让「后端挂了」这件事晚 3 个周期才显现
 *   - 不在这里跳路由：客户端不该知道路由表，跳转是 Shell 的职责
 */
import type {
  Overview, TimeSeries, LedgerRow, Topic, TopicDetail,
  Group, GroupDetail, Message, DelayEntry, SendResult, SystemInfo,
} from './types'

/** 本地 token 的存储键。与 index.html 里的主题键同一命名风格。 */
export const TOKEN_KEY = 'sq-token'

/** 401 时在 window 上广播的事件名。 */
export const UNAUTHORIZED_EVENT = 'sq:unauthorized'

/** ApiError 携带 HTTP 状态码，页面可据此区分 404（不存在）与 500（真出错）。 */
export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message)
    this.name = 'ApiError'
  }
}

function readToken(): string | null {
  try {
    return localStorage.getItem(TOKEN_KEY)
  } catch {
    return null
  }
}

function clearToken() {
  try {
    localStorage.removeItem(TOKEN_KEY)
  } catch {
    /* 忽略：存不进去也不影响本次会话 */
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers)
  const token = readToken()
  if (token) headers.set('Authorization', `Bearer ${token}`)
  if (init?.body) headers.set('Content-Type', 'application/json')

  const res = await fetch(path, { ...init, headers })

  // 登录端点的 401 是「用户名或密码错误」，不是会话失效：走失效拦截会把后端
  // 那句错误文案换成自造的「登录已失效」，登录页头注释承诺的「错误文案统一为
  // 后端返回的那一句」就兑现不了。登录请求的 401 直接落到下面的统一错误解析，
  // 把 {"error": ...} 透给页面，也不清 token、不广播未授权事件
  if (res.status === 401 && path !== '/admin/login') {
    // token 必须当场作废：留着它会让接下来每个请求都白跑一趟再 401
    clearToken()
    window.dispatchEvent(new CustomEvent(UNAUTHORIZED_EVENT))
    throw new ApiError(401, '登录已失效，请重新登录')
  }
  if (res.status === 204) return undefined as T
  const text = await res.text()
  if (!res.ok) {
    // 后端统一返回 {"error": "..."}，直接把它抛出去。解析不出来时
    // 退回原始文本——总比一句自造的「请求失败」有信息量
    let msg = text || `HTTP ${res.status}`
    try {
      const j = JSON.parse(text)
      if (typeof j?.error === 'string') msg = j.error
    } catch {
      /* 非 JSON 响应（如反代返回的 HTML 错误页），用原文 */
    }
    throw new ApiError(res.status, msg)
  }
  return (text ? JSON.parse(text) : undefined) as T
}

export const api = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: 'POST', body: body === undefined ? undefined : JSON.stringify(body) }),
  patch: <T>(path: string, body: unknown) =>
    request<T>(path, { method: 'PATCH', body: JSON.stringify(body) }),
  del: (path: string) => request<void>(path, { method: 'DELETE' }),

  /** 登录并保存 token。 */
  async login(username: string, password: string): Promise<void> {
    const { token } = await request<{ token: string }>('/admin/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    })
    try {
      localStorage.setItem(TOKEN_KEY, token)
    } catch {
      /* 存不下就只在本次会话有效，不阻断登录 */
    }
  },

  /** 只清本地 token，通知其他页面自行响应。 */
  logout() {
    clearToken()
  },

  /**
   * 探测服务端是否要求登录。
   *
   * 服务端未配置 admin_username/admin_password 时全部端点直通，此时不该
   * 逼用户看一个没有意义的登录页。用一个最轻的受保护端点探一次：
   * 401 = 要登录，其余 = 免登录。
   */
  async probeAuth(): Promise<boolean> {
    try {
      await request<Topic[]>('/admin/topics')
      return false
    } catch (e) {
      return e instanceof ApiError && e.status === 401
    }
  },

  // —— 具名端点：页面调这些而不是裸路径，路径拼写只在这里出现一次 ——

  /** 总览卡片数据。 */
  overview: () => request<Overview>('/admin/overview'),
  /** 运行态系统读数（磁盘 / 数据目录 / Go 内存 / 拒写状态）。 */
  system: () => request<SystemInfo>('/admin/system'),
  /** 时序曲线，range 决定跨度与粒度。 */
  timeseries: (range: '1h' | '24h' | '7d') => request<TimeSeries>(`/admin/timeseries?range=${range}`),
  /** 消费关系总账（组 × 主题 × 队列）。 */
  ledger: () => request<LedgerRow[]>('/admin/ledger'),
  /** 主题列表。 */
  topics: () => request<Topic[]>('/admin/topics'),
  /** 单个主题详情。 */
  topic: (name: string) => request<TopicDetail>(`/admin/topics/${encodeURIComponent(name)}`),
  /** 消费组列表。 */
  groups: () => request<Group[]>('/admin/groups'),
  /** 单个消费组详情。 */
  group: (name: string) => request<GroupDetail>(`/admin/groups/${encodeURIComponent(name)}`),
  /** 延时队列到期条目，limit 限制条数。 */
  delay: (limit: number) => request<DelayEntry[]>(`/admin/delay?limit=${limit}`),
  /** 测试发送一条消息，body 为消息字段。 */
  send: (body: Record<string, unknown>) => request<SendResult>('/admin/messages/send', {
    method: 'POST', body: JSON.stringify(body),
  }),
  /** 按 key 查消息。 */
  messagesByKey: (topic: string, key: string, limit: number) =>
    request<Message[]>(`/admin/messages?topic=${encodeURIComponent(topic)}&key=${encodeURIComponent(key)}&limit=${limit}`),
  /** 按队列位点范围查消息。 */
  messagesByQueue: (topic: string, queueId: number, fromOffset: number, limit: number) =>
    request<Message[]>(`/admin/messages?topic=${encodeURIComponent(topic)}&queue_id=${queueId}&from_offset=${fromOffset}&limit=${limit}`),
}