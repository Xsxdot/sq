/**
 * 趋势图（写入速率与落后条数）
 *
 * 职责：
 *   - 画两条线：写入速率（实线 + 面积）与落后条数（虚线）
 *   - 底部给时间轴；hover 时给出该时刻的两个读数
 *
 * 边界：
 *   - 尺寸随容器宽度自适应，靠 ResizeObserver 重算而不是固定像素
 *   - 不做缩放、不做区间选择：那是 Grafana 的活，控制台只负责「一眼看出异常」
 */
import { useEffect, useRef, useState } from 'react'
import type { TimeSeries } from '../api/types'
import { linePath } from '../lib/derive'
import { fmt } from '../lib/format'

const H = 170
const PAD = 12

/** 横轴刻度：跨度超过一天时补上日期，否则只给时分。 */
function axisLabel(ms: number, spanMs: number): string {
  const d = new Date(ms)
  const hm = `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
  return spanMs > 86400000 ? `${d.getMonth() + 1}-${d.getDate()} ${hm}` : hm
}

export function TrendChart({ series }: { series: TimeSeries }) {
  const boxRef = useRef<HTMLDivElement>(null)
  const [w, setW] = useState(900)
  const [hover, setHover] = useState<number | null>(null)

  useEffect(() => {
    const el = boxRef.current
    if (!el) return
    const ro = new ResizeObserver(() => setW(el.clientWidth || 900))
    ro.observe(el)
    setW(el.clientWidth || 900)
    return () => ro.disconnect()
  }, [])

  const pts = series.points
  const qps = pts.map(p => p.qps)
  const lag = pts.map(p => p.pending)
  const qpsPath = linePath(qps, w, H, PAD)
  const lagPath = linePath(lag, w, H, PAD)
  const spanMs = pts.length > 1 ? pts[pts.length - 1].ts_ms - pts[0].ts_ms : 0
  const endMs = pts.length ? pts[pts.length - 1].ts_ms : Date.now()

  // 数据点少于 2 个时线画不出来，给一句说明而不是一张空图——
  // 空图会被读成「没有流量」，而实际情况是「刚启动还没采到样本」
  const tooFew = pts.length < 2

  function onMove(e: React.MouseEvent<SVGSVGElement>) {
    if (tooFew) return
    const rect = e.currentTarget.getBoundingClientRect()
    const ratio = (e.clientX - rect.left - PAD) / Math.max(rect.width - PAD * 2, 1)
    const i = Math.round(ratio * (pts.length - 1))
    setHover(i >= 0 && i < pts.length ? i : null)
  }

  const hp = hover !== null ? pts[hover] : null

  return (
    <div ref={boxRef} style={{ position: 'relative' }}>
      <svg width="100%" height={H} preserveAspectRatio="none"
        onMouseMove={onMove} onMouseLeave={() => setHover(null)}>
        {[0, 0.25, 0.5, 0.75, 1].map(f => {
          const y = (PAD + (H - PAD * 2) * f).toFixed(1)
          return <line key={f} x1="0" x2={w} y1={y} y2={y} stroke="var(--chart-grid)" />
        })}
        {qpsPath && (
          <path d={`${qpsPath} L${w - PAD},${H - PAD} L${PAD},${H - PAD} Z`} fill="var(--chart-1-fp)" />
        )}
        {qpsPath && <path d={qpsPath} fill="none" stroke="var(--chart-1)" strokeWidth="1.4" />}
        {lagPath && (
          <path d={lagPath} fill="none" stroke="var(--chart-2)" strokeWidth="1.4" strokeDasharray="3 2" />
        )}
        {hover !== null && (
          <line x1={PAD + (w - PAD * 2) * (hover / Math.max(pts.length - 1, 1))}
            x2={PAD + (w - PAD * 2) * (hover / Math.max(pts.length - 1, 1))}
            y1={PAD} y2={H - PAD} stroke="var(--text-3)" strokeWidth="1" />
        )}
      </svg>

      <div className="prog">
        {[1, 0.75, 0.5, 0.25, 0].map(f => (
          <span key={f}>{axisLabel(endMs - spanMs * f, spanMs)}</span>
        ))}
      </div>

      <div className="panel-note" style={{ marginTop: 8 }}>
        {tooFew
          ? '样本不足，等待采样器积累数据（每 5 秒一个点）'
          : `${series.range === '1h' ? '近 1 小时 · 5 秒粒度' : series.range === '24h' ? '近 24 小时 · 1 分钟粒度取该分钟峰值' : '近 7 天 · 1 分钟粒度取该分钟峰值'} · 来自${series.source === 'ring' ? '内存环形缓冲' : ' Pebble'}`}
        {hp && `　|　${axisLabel(hp.ts_ms, spanMs)}　写入 ${fmt(Math.round(hp.qps))} msg/s　落后 ${fmt(hp.pending)}`}
      </div>
    </div>
  )
}