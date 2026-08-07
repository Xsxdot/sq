/**
 * 事务（半消息）页
 *
 * 职责：
 *   - 列出尚未决断的待决事务：事务ID、消息ID、目标 topic、已回查次数、
 *     下一次回查时间与暂存时刻，供排查「事务卡住 / 回查异常」时定位
 *   - 按下次回查时间升序，最紧迫（最早要回查）的排在最前
 *
 * 对应 Admin API：GET /admin/transactions
 *
 * 边界：
 *   - 列表是「头部 limit 条」样例而不是全量：GET /admin/transactions
 *     返回按 next_check 升序的前 limit 条，总数不承诺，页面上如实标注
 *   - 只读不操作：提交/回滚由生产者 SDK 决断，回查由 broker 调度下发，
 *     本页不提供任何写入口
 *   - 结构与交互逐项镜像延时页（usePoll / limit 切换即刷新 / 空态 / 面板说明），
 *     两个页面的行为保持同构
 */
import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { fmt, until, timeText } from '../lib/format'

/** 事务ID/消息ID 都是长 ID，表格里截断展示，title 兜住全文。 */
const trunc = (s: string): string => (s.length > 24 ? `${s.slice(0, 24)}…` : s)

export default function Transactions() {
  const [limit, setLimit] = useState(64)
  const list = usePoll(() => api.transactions(limit))

  const entries = list.data ?? []
  const nextCheck = entries[0]?.next_check_ms ?? 0
  const topicCount = new Set(entries.map(e => e.topic)).size

  // 切 limit 立即重拉，不能等下一个 5s 轮询周期——选了 128 却还看着 64 条，读起来像 bug。
  // 与延时页同款：首次挂载不算「切换」
  const firstLimit = useRef(true)
  useEffect(() => {
    if (firstLimit.current) {
      firstLimit.current = false
      return
    }
    list.refresh()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [limit])

  const loading = list.loading && !list.data

  return (
    <Shell title="事务">
      {loading ? (
        <div className="empty"><p>加载中…</p></div>
      ) : (
        <>
          {list.error && <Notice kind="bad">{list.error.message}</Notice>}

          <div className="strip">
            <div className="stat">
              <div>
                <div className="stat-label">待决事务</div>
                {/* 列表只是头部 limit 条样例，如实标注「上限」，不冒充全量总数 */}
                <div className="stat-val">{fmt(entries.length)}</div>
                <small>上限 {limit} 条</small>
              </div>
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">最近一次回查</div>
                <div className="stat-val">
                  {nextCheck ? (
                    <>
                      {until(nextCheck)}
                      <small>{timeText(nextCheck)}</small>
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
            <span className="count">{entries.length} 条待决（上限 {limit}）</span>
          </div>

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">待决事务</span>
              <span className="panel-note">按下次回查时间升序，最紧迫的排在最前</span>
            </div>

            {entries.length === 0 ? (
              <div className="empty">
                <p>暂无待决事务</p>
                <div className="hint">半消息完成提交或回滚后即离开待决区；若预期有事务却看不到，去「消息查询」按 topic 找已决断的消息</div>
              </div>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th style={{ width: 220 }}>事务ID</th>
                    <th style={{ width: 220 }}>MSGID</th>
                    <th style={{ width: 132 }}>TOPIC</th>
                    <th className="r" style={{ width: 88 }}>已回查</th>
                    <th className="r" style={{ width: 158 }}>下次回查</th>
                    <th className="r" style={{ width: 158 }}>暂存时刻</th>
                  </tr>
                </thead>
                <tbody>
                  {entries.map(e => {
                    // 十分钟内就要回查的标黄：排查「为什么还没决断」时，这批是马上就要动的
                    const soon = e.next_check_ms - Date.now() <= 600000
                    return (
                      <tr key={e.tx_id}>
                        <td className="mono" title={e.tx_id} style={{ fontSize: 11 }}>{trunc(e.tx_id)}</td>
                        <td className="mono" title={e.msg_id} style={{ fontSize: 11 }}>{trunc(e.msg_id)}</td>
                        <td className="name">
                          <Link to={`/topics/${encodeURIComponent(e.topic)}`}>{e.topic}</Link>
                        </td>
                        <td className="num">{e.checks}</td>
                        <td className="num">
                          <span className={`badge ${soon ? 'warn' : ''}`}>{until(e.next_check_ms)}</span>
                          {/* 与延时页统计卡同款组合展示；.num 里的 small 没有 margin，
                              需要手补一个，否则时间戳贴住徽标 */}
                          <small className="muted" style={{ marginLeft: 4 }}>{timeText(e.next_check_ms)}</small>
                        </td>
                        <td className="num muted">{timeText(e.born_ms)}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            )}

            <div className="panel-body">
              <div className="panel-note">
                本页展示的是<b>尚未决断</b>的半消息：事务消息发送后先以半消息形式暂存，
                待生产者提交或回滚（或回查后由 SDK 决断）才会写入目标 topic 的队列。
                <br />
                回查由 broker 按 next_check 时间调度，超过最大次数仍无决断的半消息将被丢弃；
                topic 列可跳转查看该主题的队列情况，本页不提供任何写操作。
              </div>
            </div>
          </section>
        </>
      )}
    </Shell>
  )
}
