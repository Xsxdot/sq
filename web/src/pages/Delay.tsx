/**
 * 延时队列页
 *
 * 职责：
 *   - 给出延时队列的整体水位：待投递总数、最近一条到期时间、涉及的 topic 数
 *   - 按到期时间升序列出尚未到期的延时消息（头部 limit 条）
 *
 * 对应 Admin API：GET /admin/delay、GET /admin/timeseries
 *
 * 边界：
 *   - GET /admin/delay 只返回 due_ms/msg_id/topic 三个字段，没有 Keys 与消息体，
 *     因此没有「查看消息体」弹窗：延时消息尚在暂存区，要看内容需等它到期后
 *     去消息查询页按 msgId 对照（面板 note 里写清，原型 delay.html 边界注释同步记录）
 *   - 待投递总数取时序里的 delay_depth 而不是列表长度：列表只是头部 limit 条，
 *     是「样例」不是全量
 *   - 只呈现「未到期」这一段生命周期，不做投递/取消等写操作
 */
import { useEffect, useRef, useState } from 'react'
import { api } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { Spark } from '../components/Spark'
import { fmt, until, timeText } from '../lib/format'

export default function Delay() {
  const [limit, setLimit] = useState(64)
  const list = usePoll(() => api.delay(limit))
  const ts = usePoll(() => api.timeseries('1h'))

  const entries = list.data ?? []
  const depth = ts.data?.points.at(-1)?.delay_depth ?? 0
  const nextDue = entries[0]?.due_ms ?? 0
  const topicCount = new Set(entries.map(e => e.topic)).size

  // 切 limit 立即重拉，不能等下一个 5s 轮询周期——选了 128 却还看着 64 条，读起来像 bug。
  // 与总览切档位同款：首次挂载不算「切换」
  const firstLimit = useRef(true)
  useEffect(() => {
    if (firstLimit.current) {
      firstLimit.current = false
      return
    }
    list.refresh()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [limit])

  const loading = (list.loading && !list.data) || (ts.loading && !ts.data)

  return (
    <Shell title="延时队列">
      {loading ? (
        <div className="empty"><p>加载中…</p></div>
      ) : (
        <>
          {list.error && <Notice kind="bad">{list.error.message}</Notice>}
          {ts.error && <Notice kind="bad">{ts.error.message}</Notice>}

          <div className="strip">
            <div className="stat">
              <div>
                <div className="stat-label">待投递</div>
                {/* delay_depth 是全量深度；列表只是头部 limit 条，不能拿列表长度当总数 */}
                <div className="stat-val">{fmt(depth)}</div>
              </div>
              <Spark values={ts.data?.points.map(p => p.delay_depth) ?? []} color="var(--text-3)" />
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">最近一条到期</div>
                <div className="stat-val">
                  {nextDue ? (
                    <>
                      {until(nextDue)}
                      <small>{timeText(nextDue)}</small>
                    </>
                  ) : <small>无</small>}
                </div>
              </div>
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">涉及 TOPIC</div>
                <div className="stat-val">{topicCount}</div>
              </div>
            </div>
          </div>

          <div className="filters">
            <label className="panel-note" htmlFor="limit">返回条数</label>
            <select id="limit" value={limit} onChange={e => setLimit(Number(e.target.value))}>
              <option value={32}>32</option>
              {/* 默认 64 与后端 queryUint(limit, 64) 一致；原型默认 32，仅默认值不同 */}
              <option value={64}>64</option>
              <option value={128}>128</option>
            </select>
            <span className="count">{entries.length} / {fmt(depth)} 条未到期（上限 {limit}）</span>
          </div>

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">未到期的延时消息</span>
              <span className="panel-note">按到期时间升序，最早到期的排在最前</span>
            </div>

            {entries.length === 0 ? (
              <div className="empty">
                <p>当前没有待投递的延时消息</p>
                <div className="hint">到期后的延时消息会转为普通消息，去「消息查询」按 topic 找</div>
              </div>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>MSGID</th>
                    <th style={{ width: 132 }}>TOPIC</th>
                    <th className="r" style={{ width: 158 }}>到期时间</th>
                    <th className="r" style={{ width: 96 }}>还有多久</th>
                  </tr>
                </thead>
                <tbody>
                  {entries.map(e => {
                    // 十分钟内到期的标黄：排查「为什么还没投递」时，这批是马上就要动的
                    const soon = e.due_ms - Date.now() <= 600000
                    return (
                      <tr key={e.msg_id}>
                        <td className="name">{e.msg_id}</td>
                        <td className="name">{e.topic}</td>
                        <td className="num muted">{timeText(e.due_ms)}</td>
                        <td className="num"><span className={`badge ${soon ? 'warn' : ''}`}>{until(e.due_ms)}</span></td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            )}

            <div className="panel-body">
              <div className="panel-note">
                本页只展示<b>尚未到期投递</b>的延时消息。到期后 broker 会把它写入目标 topic
                的队列，从此就是一条普通消息，不再出现在这里 —— 要追踪已投递的消息请去「消息查询」。
                <br />
                延时消息尚在暂存区，未进入任何队列；/admin/delay 只返回到期时间、msgId 与目标
                topic，不含 Keys 与消息体，要看内容需等它到期后去消息查询页按 msgId 对照。
              </div>
            </div>
          </section>
        </>
      )}
    </Shell>
  )
}
