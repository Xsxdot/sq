/**
 * 页面外壳：侧栏 + 顶部条
 *
 * 职责：
 *   - 全站导航、当前页高亮、主题切换、退出登录
 *   - 监听 401 事件并跳转登录页
 *
 * 边界：
 *   - 不取任何业务数据：页面自己负责自己的数据
 *   - 侧栏结构与 prototypes/base/shared/shell.html 逐段一致，class 不改
 */
import { useEffect, useState, type ReactNode } from 'react'
import { NavLink, useNavigate } from 'react-router-dom'
import { api, UNAUTHORIZED_EVENT } from '../api/client'

const NAV = [
  { to: '/', label: '总览', end: true },
  { group: '资源' },
  { to: '/topics', label: 'Topic' },
  { to: '/groups', label: '消费组' },
  { to: '/delay', label: '延时队列' },
  { group: '排查' },
  { to: '/messages', label: '消息查询' },
  { to: '/dlq', label: '死信队列' },
  { to: '/send', label: '发送测试消息' },
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
        <main className="content">{children}</main>
      </div>
    </div>
  )
}