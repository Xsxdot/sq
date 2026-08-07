/**
 * sq 控制台入口
 *
 * 职责：
 *   - 挂载 React 根、装配路由表
 *   - 启动时探一次是否需要登录，免登录的服务端不逼用户看登录页
 *
 * 边界：
 *   - 不含任何业务逻辑与数据获取；页面各自负责自己的数据
 */
import { StrictMode, useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { api } from './api/client'
import Login from './pages/Login'
import Overview from './pages/Overview'
import Topics from './pages/Topics'
import TopicDetail from './pages/TopicDetail'
import Groups from './pages/Groups'
import GroupDetail from './pages/GroupDetail'
import Messages from './pages/Messages'
import Send from './pages/Send'
import Dlq from './pages/Dlq'
import Delay from './pages/Delay'
import Transactions from './pages/Transactions'
import './styles/app.css'

/** 启动时探一次是否需要登录：免登录的服务端上不该逼用户看登录页。 */
function App() {
  const [ready, setReady] = useState(false)
  useEffect(() => {
    api.probeAuth().then(needLogin => {
      if (needLogin && location.pathname !== '/login') {
        history.replaceState(null, '', '/login')
      }
      setReady(true)
    })
  }, [])
  if (!ready) return null
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route path="/" element={<Overview />} />
        <Route path="/topics" element={<Topics />} />
        <Route path="/topics/:name" element={<TopicDetail />} />
        <Route path="/groups" element={<Groups />} />
        <Route path="/groups/:name" element={<GroupDetail />} />
        <Route path="/messages" element={<Messages />} />
        <Route path="/dlq" element={<Dlq />} />
        <Route path="/delay" element={<Delay />} />
        <Route path="/transactions" element={<Transactions />} />
        <Route path="/send" element={<Send />} />
      </Routes>
    </BrowserRouter>
  )
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)