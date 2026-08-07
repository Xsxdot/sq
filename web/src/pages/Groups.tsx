/**
 * 消费组列表页
 *
 * 职责：
 *   - 列出全部消费组，把该组在所有 topic 上的消费关系聚合成一行读数：
 *     订阅数、总落后、总在途、死信、最后消费时间
 *   - 行首状态条给一眼可判的健康度；提供删除消费组入口，删除走二次确认
 *
 * 对应 Admin API：GET /admin/groups、DELETE /admin/groups/{name}
 *
 * 边界：
 *   - 只做聚合视图，不画 offset 带：不同 topic 的落后量量纲不同，
 *     混在一条带子里会误导；逐 topic 的带子在消费组详情页看
 *   - 死信是组维度：同一组的各行显示同一个值，聚合时取任意一行即可
 *   - 消费组由消费者首次订阅时自动创建，本页只做查看与清理
 */
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { lagOf, markOf } from '../lib/derive'
import { fmt, ago } from '../lib/format'
import type { Group } from '../api/types'

export default function Groups() {
  const list = usePoll(() => api.groups())
  const led = usePoll(() => api.ledger())

  // 把一个组在所有 topic 上的消费关系聚合成一行读数。
  // 死信是组维度，取任意一行即可（各行相同），无行时为 0。
  function aggregate(name: string) {
    const rows = (led.data ?? []).filter(r => r.group === name)
    return {
      subs: rows.length,
      lag: rows.reduce((s, r) => s + lagOf(r), 0),
      inflight: rows.reduce((s, r) => s + r.inflight, 0),
      dlq: rows[0]?.dlq ?? 0,
      // 组级「最后消费」取各订阅里最近的一次，只要有一条在动就说明组是活的
      lastMs: Math.max(0, ...rows.map(r => r.last_consume_ms)),
      // 聚合行的状态：任一 topic 异常，整组就算异常
      mark: rows.some(r => markOf(r) === 'm-bad') ? 'm-bad'
        : rows.some(r => markOf(r) === 'm-warn') ? 'm-warn' : 'm-ok',
      topics: rows.map(r => r.topic),
    }
  }

  const [kw, setKw] = useState('')
  const groups = list.data ?? []
  const rows = groups.filter(g => g.name.toLowerCase().includes(kw.trim().toLowerCase()))

  // —— 删除消费组：位点与在途会一并清掉，后果比删 topic 更容易被低估，必须二次确认 —— //
  const [pending, setPending] = useState<Group | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [delErr, setDelErr] = useState<string | null>(null)
  const [delOk, setDelOk] = useState<string | null>(null)

  async function onDelete() {
    if (!pending) return
    setDeleting(true)
    setDelErr(null)
    try {
      await api.del(`/admin/groups/${encodeURIComponent(pending.name)}`)
      setDelOk(`已删除消费组 <b>${pending.name}</b>，其消费位点与在途记录一并清除`)
      setPending(null)
      // 列表与总账都要刷新：聚合读数来自总账，只刷列表会残留旧数字
      list.refresh()
      led.refresh()
    } catch (err) {
      setDelErr(err instanceof Error ? err.message : '删除失败')
    } finally {
      setDeleting(false)
    }
  }

  const loading = (list.loading && !list.data) || (led.loading && !led.data)

  return (
    <Shell title="消费组">
      {loading ? (
        <div className="empty"><p>加载中…</p></div>
      ) : (
        <>
          {list.error && <Notice kind="bad">{list.error.message}</Notice>}
          {led.error && <Notice kind="bad">{led.error.message}</Notice>}
          {delOk && <Notice kind="ok" onClose={() => setDelOk(null)}>{delOk}</Notice>}

          <div className="filters">
            <input className="search" type="text" placeholder="按组名过滤，如 notify"
              value={kw} onChange={e => setKw(e.target.value)} />
            <span className="count">{rows.length} / {groups.length} 个消费组</span>
          </div>

          <table>
            <thead>
              <tr>
                <th style={{ width: 3 }} />
                <th>组名</th>
                <th className="r" style={{ width: 88 }}>订阅 TOPIC</th>
                <th className="r" style={{ width: 112 }}>MAX ATTEMPTS</th>
                <th className="r" style={{ width: 96 }}>总落后</th>
                <th className="r" style={{ width: 72 }}>总在途</th>
                <th className="r" style={{ width: 72 }}>死信</th>
                <th className="r" style={{ width: 96 }}>最后消费</th>
                <th className="r" style={{ width: 72 }}>操作</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(g => {
                const a = aggregate(g.name)
                return (
                  <tr key={g.name}>
                    <td className={`mark ${a.mark}`} />
                    <td className="name">
                      <Link to={`/groups/${encodeURIComponent(g.name)}`}>{g.name}</Link>
                      <small>{a.subs ? a.topics.join(' · ') : '未订阅任何 topic'}</small>
                    </td>
                    <td className={`num ${a.subs ? '' : 'zero'}`}>{a.subs}</td>
                    <td className="num muted">{g.max_attempts}</td>
                    <td className={`num ${a.lag > 500 ? 'bad' : a.lag === 0 ? 'zero' : ''}`}>{fmt(a.lag)}</td>
                    <td className={`num ${a.inflight ? '' : 'zero'}`}>{fmt(a.inflight)}</td>
                    <td className={`num ${a.dlq ? 'bad' : 'zero'}`}>{fmt(a.dlq)}</td>
                    {/* lastMs=0 表示「尚未观察到位点推进」，与「很久以前消费过」是两件事，用占位符区分 */}
                    <td className="num muted">{a.lastMs ? ago(a.lastMs) : '—'}</td>
                    <td className="num">
                      <button className="btn danger" onClick={() => { setDelErr(null); setPending(g) }}>删除</button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>

          {rows.length === 0 && (
            <div className="empty">
              <p>没有匹配的消费组</p>
              <div className="hint">消费组由消费者首次订阅时自动创建，这里只做查看与清理</div>
            </div>
          )}
        </>
      )}

      <ConfirmDialog open={pending !== null} title="删除消费组" confirmText="确认删除"
        danger busy={deleting}
        onCancel={() => { setPending(null); setDelErr(null) }}
        onConfirm={onDelete}>
        {pending && (() => {
          const a = aggregate(pending.name)
          return (
            <>
              {delErr && <Notice kind="bad">{delErr}</Notice>}
              <div className="notice bad">
                <span>即将删除消费组 <b>{pending.name}</b>。该操作<b>不可恢复</b>。</span>
              </div>
              <div className="kv">
                <dt>订阅关系</dt>
                <dd>{a.subs ? `${a.subs} 个 topic：${a.topics.join('、')}` : '当前无订阅关系'}</dd>
                <dt>丢弃进度</dt>
                <dd>落后 {fmt(a.lag)} 条 · 在途 {fmt(a.inflight)} 条 · 死信 {fmt(a.dlq)} 条</dd>
              </div>
              <p className="muted">删除会清除该组的所有消费位点与在途记录；正在运行的消费者一旦重连，
                会按各 topic 的最早可用位点<b>从头开始消费</b>，历史消息将被重复投递。
                该组的死信条目也会一并失去归属。请先停掉消费者再执行。</p>
            </>
          )
        })()}
      </ConfirmDialog>
    </Shell>
  )
}