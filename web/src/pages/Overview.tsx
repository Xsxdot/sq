/**
 * 总览页
 *
 * 职责：
 *   - 上半屏给整体信号：六项读数（写入/总落后/在途/延时待投/死信/TOPIC·消费组）+ 写入与落后趋势图（1h/24h/7d）
 *   - 下半屏给全部消费关系总账：全表共用一把刻度画 offset 带，可逐行展开到队列级并就地发起操作
 *
 * 边界：
 *   - 详情页与操作页不在此处实现，本页只按路由出链接
 *   - 数据全部来自轮询（api.overview / api.timeseries / api.ledger），不做本地缓存
 *   - 布局与 class 名与 prototypes/base/index.html 逐段一致，class 名一个字不改
 */
import { Fragment, useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { Spark } from '../components/Spark'
import { Ribbon } from '../components/Ribbon'
import { TrendChart } from '../components/TrendChart'
import { lagOf, maxLag, markOf } from '../lib/derive'
import { fmt, ago } from '../lib/format'
import type { LedgerRow } from '../api/types'

/** 行的唯一键：一个「组 × 主题」就是一行。 */
const key = (r: LedgerRow): string => `${r.group}::${r.topic}`

export default function Overview() {
  const ov = usePoll(() => api.overview())
  const [range, setRange] = useState<'1h' | '24h' | '7d'>('1h')
  const ts = usePoll(() => api.timeseries(range))
  const led = usePoll(() => api.ledger())
  const [filter, setFilter] = useState<'lag' | 'dlq' | null>(null)
  const [open, setOpen] = useState<Set<string>>(new Set())

  const rows = (led.data ?? []).filter(r =>
    filter === 'lag' ? lagOf(r) > 0 : filter === 'dlq' ? r.dlq > 0 : true)
  // 全表共用一把刻度，带子长短才可横向比较。注意刻度取自过滤后的行：
  // 过滤成「只看落后的」之后，刻度跟着收紧，剩下几行的差异才看得出来
  const scale = maxLag(rows)

  // 六项读数旁的迷你图共用当前档位时序的点，避免为了四条小曲线再打四次请求；切档位时随 ts 一起刷新
  const pts = ts.data?.points ?? []
  const sparks = {
    qps: pts.map(p => p.qps),
    lag: pts.map(p => p.pending),
    fly: pts.map(p => p.inflight),
    delay: pts.map(p => p.delay_depth),
  }

  // 档位切换要立刻拉新请求，不能等下一个 5s 轮询周期——点 24h 却还看着 1h 的曲线，读起来像 bug
  const firstRange = useRef(true)
  useEffect(() => {
    if (firstRange.current) {
      firstRange.current = false
      return
    }
    ts.refresh()
  }, [range])

  const toggle = (k: string) => setOpen(prev => {
    const next = new Set(prev)
    if (next.has(k)) next.delete(k)
    else next.add(k)
    return next
  })

  // 三个数据源首次都为空时整页转「加载中」，之后各自出错各自亮 Notice，不吞错误
  const loading =
    (ov.loading && !ov.data) || (ts.loading && !ts.data) || (led.loading && !led.data)

  return (
    <Shell title="总览">
      {loading ? (
        <div className="empty"><p>加载中…</p></div>
      ) : (
        <>
          {ov.error && <Notice kind="bad">{ov.error.message}</Notice>}
          {ts.error && <Notice kind="bad">{ts.error.message}</Notice>}
          {led.error && <Notice kind="bad">{led.error.message}</Notice>}

          <div className="strip">
            <div className="stat">
              <div>
                <div className="stat-label">写入</div>
                {/* qps 为 null 表示「不知道」而不是「没有流量」，不能显示成 0 */}
                <div className="stat-val">
                  {ov.data?.qps == null ? '—' : fmt(Math.round(ov.data.qps))}
                  <small>msg/s</small>
                </div>
              </div>
              <Spark values={sparks.qps} color="var(--chart-1)" />
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">总落后</div>
                <div className="stat-val">{fmt(ov.data?.total_pending ?? 0)}</div>
              </div>
              <Spark values={sparks.lag} color="var(--chart-2)" />
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">在途</div>
                <div className="stat-val">{fmt(ov.data?.total_inflight ?? 0)}</div>
              </div>
              <Spark values={sparks.fly} color="var(--text-3)" />
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">延时待投</div>
                <div className="stat-val">{fmt(ov.data?.delay_depth ?? 0)}</div>
              </div>
              <Spark values={sparks.delay} color="var(--text-3)" />
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">死信</div>
                <div className="stat-val bad">{fmt(ov.data?.total_dlq ?? 0)}</div>
              </div>
            </div>
            <div className="stat">
              <div>
                <div className="stat-label">TOPIC / 消费组</div>
                <div className="stat-val">{ov.data?.topics ?? 0} / {ov.data?.groups ?? 0}</div>
              </div>
            </div>
          </div>

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">写入速率与落后</span>
              <span className="spacer" />
              <span className="legend">
                <span><i style={{ background: 'var(--chart-1)' }} />写入 msg/s</span>
                <span><i style={{ background: 'var(--chart-2)' }} />落后条数</span>
              </span>
              <div className="range">
                {(['1h', '24h', '7d'] as const).map(rr => (
                  <button key={rr} className={range === rr ? 'active' : ''}
                    onClick={() => setRange(rr)}>{rr}</button>
                ))}
              </div>
            </div>
            <div className="panel-body">
              {ts.data && <TrendChart series={ts.data} />}
            </div>
          </section>

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">消费关系总账</span>
              <span className="panel-note">点任意一行展开到队列级</span>
              <span className="spacer" />
              <button className={`chip ${filter === 'lag' ? 'on' : ''}`}
                onClick={() => setFilter(filter === 'lag' ? null : 'lag')}>只看落后的</button>
              <button className={`chip ${filter === 'dlq' ? 'on' : ''}`}
                onClick={() => setFilter(filter === 'dlq' ? null : 'dlq')}>只看有死信</button>
              <span className="count">{rows.length} / {led.data?.length ?? 0} 条消费关系</span>
            </div>
            <table>
              <thead>
                <tr>
                  <th style={{ width: 3 }} />
                  <th>消费组 / TOPIC</th>
                  <th style={{ width: 132 }}>写入</th>
                  <th style={{ width: 220 }}>落后（同一刻度）</th>
                  <th className="r" style={{ width: 88 }}>落后</th>
                  <th className="r" style={{ width: 64 }}>在途</th>
                  {/* 死信是消费组维度的数：同组各行是同一个值，表头必须说清楚，
                      否则会被读成每个 topic 各自的死信数 */}
                  <th className="r" style={{ width: 64 }}>死信（组）</th>
                  <th className="r" style={{ width: 92 }}>最后消费</th>
                </tr>
              </thead>
              <tbody>
                {rows.map(r => {
                  // 展开行的刻度用本行自己的最大落后：队列间几十条的差异在全表刻度下看不见
                  const qScale = Math.max(1, ...r.queues.map(q => q.next_offset - q.cursor))
                  return (
                    <Fragment key={key(r)}>
                      <tr className="row" onClick={e => {
                        // 行内链接不应触发展开，否则点组名会同时跳页并展开
                        if ((e.target as HTMLElement).closest('a')) return
                        toggle(key(r))
                      }}>
                        <td className={`mark ${markOf(r)}`} />
                        <td className="name">
                          <Link to={`/groups/${encodeURIComponent(r.group)}`}>{r.group}</Link>
                          <small>{r.topic}</small>
                        </td>
                        <td>
                          <div className="rate">
                            <span>{r.written_qps === null ? '—' : fmt(Math.round(r.written_qps))}</span>
                            <Spark values={sparks.qps} color={r.written_qps ? 'var(--chart-1)' : 'var(--text-3)'} />
                          </div>
                        </td>
                        {/* 主表 offset 带非 compact，下方渲染「位点 X / 落后 Y」字幕，与原型 index.html:190 一致；
                            队列详情行才用 compact（index.html:207） */}
                        <td><Ribbon cursor={r.cursor} head={r.next_offset} fly={r.inflight} scale={scale} /></td>
                        <td className={`num ${lagOf(r) > 500 ? 'bad' : lagOf(r) === 0 ? 'zero' : ''}`}>{fmt(lagOf(r))}</td>
                        <td className={`num ${r.inflight ? '' : 'zero'}`}>{r.inflight}</td>
                        <td className={`num ${r.dlq ? 'bad' : 'zero'}`}>{r.dlq}</td>
                        {/* last_consume_ms=0 表示「尚未观察到位点推进」，与「很久以前消费过」
                            是两件事，用占位符区分，不要显示成一个假的时间 */}
                        <td className="num muted">{r.last_consume_ms ? ago(r.last_consume_ms) : '—'}</td>
                      </tr>
                      {open.has(key(r)) && (
                        <tr className="detail">
                          <td colSpan={8}>
                            <div className="detail-head">
                              {r.queues.length} 个队列 · 刻度为本组内最大落后 {fmt(qScale)} 条
                            </div>
                            <div className="qgrid">
                              {r.queues.map(q => (
                                <div className="qrow" key={q.queue_id}>
                                  <span>queue {q.queue_id}</span>
                                  <Ribbon cursor={q.cursor} head={q.next_offset} fly={q.inflight} scale={qScale} compact />
                                  <span style={{ textAlign: 'right' }}>落后 {fmt(q.next_offset - q.cursor)}</span>
                                </div>
                              ))}
                            </div>
                            <div className="acts">
                              <Link className="btn" to={`/groups/${encodeURIComponent(r.group)}`}>重置位点</Link>
                              <Link className="btn" to={`/dlq?group=${encodeURIComponent(r.group)}`}>查看死信</Link>
                              <Link className="btn" to={`/messages?topic=${encodeURIComponent(r.topic)}`}>查询消息</Link>
                            </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
              </tbody>
            </table>
          </section>
        </>
      )}
    </Shell>
  )
}