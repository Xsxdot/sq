/**
 * 轮询 hook
 *
 * 职责：
 *   - 按固定间隔重复执行一个异步取数函数，暴露 data/error/loading/refresh
 *   - 页面切到后台时暂停轮询，切回来立刻补一次
 *
 * 边界：
 *   - 只管取数，不管展示：loading/error 怎么呈现由调用方决定
 *   - 不做请求去重与竞态取消之外的任何缓存
 */
import { useCallback, useEffect, useRef, useState } from 'react'

/** usePoll 的返回值：数据、错误、首载 loading 与手动刷新。 */
export interface PollState<T> {
  data: T | null
  error: Error | null
  /** 仅首次取数为 true；轮询刷新时保持 false，避免整页反复闪成骨架 */
  loading: boolean
  refresh: () => void
}

export function usePoll<T>(fn: () => Promise<T>, intervalMs = 5000): PollState<T> {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<Error | null>(null)
  const [loading, setLoading] = useState(true)
  // fn 每次渲染都是新函数，放进依赖会让轮询无限重启；用 ref 固定住
  const fnRef = useRef(fn)
  fnRef.current = fn
  // 迟到的响应不能覆盖新响应：切页面时旧请求可能后到
  const seqRef = useRef(0)

  const run = useCallback(async () => {
    const seq = ++seqRef.current
    try {
      const v = await fnRef.current()
      if (seq !== seqRef.current) return
      setData(v)
      setError(null)
    } catch (e) {
      if (seq !== seqRef.current) return
      setError(e instanceof Error ? e : new Error(String(e)))
    } finally {
      if (seq === seqRef.current) setLoading(false)
    }
  }, [])

  useEffect(() => {
    run()
    let timer = window.setInterval(run, intervalMs)
    // 页面不可见时停掉轮询：后台标签页每 5 秒打一次全表扫描端点纯属浪费
    const onVisible = () => {
      window.clearInterval(timer)
      if (document.visibilityState === 'visible') {
        run()
        timer = window.setInterval(run, intervalMs)
      }
    }
    document.addEventListener('visibilitychange', onVisible)
    return () => {
      window.clearInterval(timer)
      document.removeEventListener('visibilitychange', onVisible)
    }
  }, [run, intervalMs])

  return { data, error, loading, refresh: run }
}