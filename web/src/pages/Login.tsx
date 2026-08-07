/**
 * 登录页
 *
 * 职责：
 *   - 控制台唯一入口：校验用户名密码后进入总览
 *   - 说明凭据来自 broker 配置文件，以及两项留空即免登录
 *
 * 对应 Admin API：POST /admin/login
 *
 * 边界：
 *   - 不套 Shell 外壳：未登录时不该看见侧栏导航
 *   - 错误文案统一为后端返回的那一句，不区分是哪一项错，避免账号枚举
 */
import { useState, type FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api/client'

export default function Login() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const nav = useNavigate()

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    try {
      await api.login(username, password)
      setErr(null)
      nav('/', { replace: true })
    } catch (e) {
      setErr(e instanceof Error ? e.message : '登录失败')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="login-wrap">
      <div className="login-card">
        <div className="brand"><b>sq</b><span>SIMPLE QUEUE</span></div>
        <p className="login-sub">内嵌运维控制台</p>

        <div className="notice bad" hidden={!err}><span>{err}</span></div>

        <form className="form" onSubmit={onSubmit}>
          <div className="field">
            <label>用户名</label>
            <input type="text" autoComplete="username" autoFocus
              value={username} onChange={e => setUsername(e.target.value)} />
          </div>
          <div className="field">
            <label>密码</label>
            <input type="password" autoComplete="current-password"
              value={password} onChange={e => setPassword(e.target.value)} />
          </div>
          <button className="btn primary" type="submit" disabled={busy}
            style={{ width: '100%', padding: '7px' }}>
            {busy ? '登录中…' : '登录'}
          </button>
        </form>

        <p className="dim" style={{ margin: '16px 0 0', fontSize: '11.5px' }}>
          用户名与密码在 broker 配置文件的 <span className="mono">admin_username</span> /{' '}
          <span className="mono">admin_password</span> 中设置；两项都留空时控制台免登录。
        </p>
      </div>
    </div>
  )
}