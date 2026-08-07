/**
 * 数字与时间的展示格式
 *
 * 职责：
 *   - 千分位、相对时间、绝对时间、时长四类格式化，全站唯一来源
 *
 * 边界：
 *   - 只负责「怎么显示」，不做任何业务派生（那在 derive.ts）
 *   - 与 prototypes/base/shared/mock.js 的同名函数行为逐字一致，
 *     区别仅在原型用固定基准时刻、这里用 Date.now()
 */

/** 千分位。表格里的数字不加千分位时，五位数和六位数一眼分不出来。 */
export const fmt = (n: number): string => Number(n).toLocaleString('en-US')

/**
 * 相对时间。排查时「4 分钟前」比绝对时间戳更快形成判断。
 * ms<=0 表示「没有观察到」，交给调用方显示占位符，这里返回空串。
 */
export function ago(ms: number): string {
  if (!ms) return ''
  const d = Math.max(0, Math.round((Date.now() - ms) / 1000))
  if (d < 5) return '刚刚'
  if (d < 60) return `${d}s 前`
  if (d < 3600) return `${Math.round(d / 60)}m 前`
  if (d < 86400) return `${Math.round(d / 3600)}h 前`
  return `${Math.round(d / 86400)}d 前`
}

/** 距离未来某时刻还有多久（延时队列用）。 */
export function until(ms: number): string {
  const d = Math.max(0, Math.round((ms - Date.now()) / 1000))
  if (d < 60) return `${d}s 后`
  if (d < 3600) return `${Math.round(d / 60)}m 后`
  return `${Math.round(d / 3600)}h 后`
}

/** 绝对时间，24 小时制。 */
export const timeText = (ms: number): string =>
  new Date(ms).toLocaleString('zh-CN', { hour12: false })

/** 时长。整天数说「天」，否则折成小时——「72 小时」不如「3 天」好读。 */
export const dur = (ms: number): string =>
  ms % 86400000 === 0 ? `${ms / 86400000} 天` : `${Math.round(ms / 3600000)} 小时`

/** base64 消息体解码。解不开时原样返回，不吞掉内容。 */
export function decodeBody(b64: string): string {
  try {
    return new TextDecoder().decode(Uint8Array.from(atob(b64), c => c.charCodeAt(0)))
  } catch {
    return b64
  }
}