/**
 * sq 控制台构建配置
 *
 * 职责：
 *   - 产物落到 dist/，由 web/embed.go 的 go:embed 打进单二进制
 *   - 开发期把 /admin 与 /metrics 代理到本机 broker，前端可独立热更新
 *
 * 边界：
 *   - 不配任何 CDN / 外部字体：产物必须能在无外网机器上完整运行
 */
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  // 相对基址：控制台永远挂在 broker 的根路径下，但用相对路径可以
  // 兼容将来放在反向代理子路径后面的情况
  base: './',
  build: { outDir: 'dist', emptyOutDir: true },
  server: {
    // 固定端口：SuperDev 注册的 console 服务与 web.url 都按这个地址来。
    // 不用默认 5173——本机 tk/admin 已占该口；不写死时 Vite 静默换口会让面板入口失效。
    host: '127.0.0.1',
    port: 5183,
    strictPort: true,
    proxy: {
      '/admin': 'http://127.0.0.1:8082',
      '/metrics': 'http://127.0.0.1:8082',
    },
  },
  test: { environment: 'jsdom', globals: true },
})