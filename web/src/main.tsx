/**
 * sq 控制台入口
 *
 * 职责：
 *   - 挂载 React 根、装配路由表
 *   - 启动时探一次是否需要登录，免登录的服务端不逼用户看登录页
 *
 * 边界：
 *   - 不含任何业务逻辑与数据获取；页面各自负责自己的数据
 *   - 未实现的页面先指向占位组件，每完成一个就换掉对应一行
 */
import { StrictMode, useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { api } from './api/client'
import Login from './pages/Login'
import {
  Overview, Topics, TopicDetail, Groups, GroupDetail,
  Messages, Send, Dlq, Delay,
} from './pages/placeholder'
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