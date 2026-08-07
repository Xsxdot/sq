/**
 * sq 控制台入口
 *
 * 职责：
 *   - 挂载 React 根、装配路由表
 *
 * 边界：
 *   - 不含任何业务逻辑与数据获取；页面各自负责自己的数据
 */
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import './styles/app.css'

function Placeholder() {
  return <div className="empty"><p>控制台骨架已就绪</p></div>
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="*" element={<Placeholder />} />
      </Routes>
    </BrowserRouter>
  </StrictMode>,
)