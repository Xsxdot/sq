/**
 * 页面外壳：侧栏 + 顶部条
 *
 * 职责：
 *   - 全站导航、当前页高亮、主题切换、退出登录
 *   - 监听 401 事件并跳转登录页
 *   - 拒写保读时在内容区顶部挂全站横幅
 *
 * 边界：
 *   - 不取业务数据：页面自己负责自己的数据。唯一例外是 /admin/system 的
 *     拒写状态——它是全站级运行状态（同「离线提示」一类），放进任何单个
 *     页面都会漏掉用户实际察觉问题的那个页面
 *   - 系统读数取数失败时静默降级，不在外壳亮错误
 *   - 侧栏结构与 prototypes/base/shared/shell.html 逐段一致，class 不改
 *   - 登录页不套本外壳，因此这里的轮询不会在未登录状态下打空转
 */
import { useEffect, useState, type ReactNode } from 'react'
import { NavLink, useNavigate } from 'react-router-dom'
import { api, UNAUTHORIZED_EVENT } from '../api/client'
import { usePoll } from '../hooks/usePoll'

const NAV = [
  { to: '/', label: '总览', end: true },
  { group: '资源' },
  { to: '/topics', label: 'Topic' },
  { to: '/groups', label: '消费组' },
  { to: '/delay', label: '延时队列' },
  { to: '/transactions', label: '事务' },
  { group: '排查' },
  { to: '/messages', label: '消息查询' },
  { to: '/dlq', label: '死信队列' },
  { to: '/send', label: '发送测试消息' },
  { group: '运维' },
  { to: '/cluster', label: '集群' },
] as const

function readTheme(): 'light' | 'dark' {
  return document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light'
}

export interface ShellProps {
  title: ReactNode
  /** 面包屑，详情页用 */
  crumb?: ReactNode
  /** 顶部条右侧的额外操作 */
  actions?: ReactNode
  children: ReactNode
}

export function Shell({ title, crumb, actions, children }: ShellProps) {
  const [theme, setTheme] = useState<'light' | 'dark'>(readTheme)
  const nav = useNavigate()

  // 拒写是全站级的运行状态，不是某个页面的业务数据：用户可能在任何页面
  // 察觉「发不进去」——尤其是发送测试消息页。15 秒一次，与总览页同频。
  // 取数失败在这里刻意静默：外壳不该因为一个辅助读数亮红，页面自己的
  // 错误提示已经足够，多一条会淹没真正的问题
  const sys = usePoll(() => api.system(), 15000)

  useEffect(() => {
    const onUnauthorized = () => nav('/login', { replace: true })
    window.addEventListener(UNAUTHORIZED_EVENT, onUnauthorized)
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, onUnauthorized)
  }, [nav])

  function toggleTheme() {
    const next = theme === 'dark' ? 'light' : 'dark'
    document.documentElement.dataset.theme = next
    try {
      localStorage.setItem('sq-theme', next)
    } catch {
      /* 存不下就只在本次会话有效 */
    }
    setTheme(next)
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand"><b>sq</b><span>SIMPLE QUEUE</span></div>
        <nav>
          {NAV.map((n, i) =>
            'group' in n ? (
              <div className="nav-group" key={i}>{n.group}</div>
            ) : (
              <NavLink key={n.to} to={n.to} end={'end' in n ? n.end : false}
                className={({ isActive }) => (isActive ? 'nav-item active' : 'nav-item')}>
                {n.label}
              </NavLink>
            ),
          )}
        </nav>
      </aside>

      <div className="main">
        <header className="topbar">
          {crumb ? <span className="crumb">{crumb}</span> : <span className="page-title">{title}</span>}
          <span className="spacer" />
          <span className="live"><i className="dot" />5s 自动刷新</span>
          {actions}
          {/* 按钮上写的是「点了会变成什么」，用汉字而不是 ☀/☾：
              符号在部分系统字体里会退化成方框 */}
          <button className="btn theme-toggle" onClick={toggleTheme}
            title={theme === 'dark' ? '切换到明色主题' : '切换到暗色主题'}>
            {theme === 'dark' ? '明色' : '暗色'}
          </button>
          <button className="btn" onClick={() => { api.logout(); nav('/login', { replace: true }) }}>
            退出登录
          </button>
        </header>
        <main className="content">
          {sys.data?.write_blocked && (
            <div className="notice bad">
              <span>
                <b>磁盘水位已触发，服务当前拒写保读。</b>
                {sys.data.disk &&
                  `已用 ${sys.data.disk.used_percent.toFixed(1)}%，水位线 ${sys.data.watermark_percent}%。`}
                生产端会收到写入失败，消费不受影响；清理磁盘或调高 disk_watermark_percent 后自动恢复。
              </span>
            </div>
          )}
          {children}
        </main>
      </div>
    </div>
  )
}