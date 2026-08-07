/**
 * Topic 详情页
 *
 * 职责：
 *   - 展示单个 topic 的属性、每个队列的写入头，以及谁在订阅它、追得上不上
 *   - 提供 retention 就地修改入口（topic 唯一可改的属性）
 *
 * 对应 Admin API：GET /admin/topics/{name}、PATCH /admin/topics/{name}
 *
 * 边界：
 *   - topic 名来自路由参数，取不到 / 不存在即 404 空态，不做原型的「退回 order.created」兜底
 *   - 队列数不可改（改了会打乱 hash 分区与既有位点），页面只读展示；retention 修改走 PATCH
 *   - 消费进度只做只读概览，重置位点等操作在消费组详情页做
 */
import { useEffect, useRef, useState, type FormEvent } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api, ApiError } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { Ribbon } from '../components/Ribbon'
import { lagOf, markOf, maxLag } from '../lib/derive'
import { fmt, ago, dur, timeText } from '../lib/format'

export default function TopicDetail() {
  const { name = '' } = useParams()
  const detail = usePoll(() => api.topic(name))
  const led = usePoll(() => api.ledger())
  const subs = (led.data ?? []).filter(r => r.topic === name)
  // 本表共用一把刻度：带子长短才能横向比较，取本页最大落后量
  const scale = maxLag(subs)
  // 累计写入 = 各队列写入头之和。topic 维度没有单一 offset，只能按队列求和。
  const written = (detail.data?.queues_detail ?? []).reduce((s, q) => s + q.next_offset, 0)

  const [retentionH, setRetentionH] = useState('')
  const [saving, setSaving] = useState(false)
  const [patchErr, setPatchErr] = useState<string | null>(null)
  const [patchOk, setPatchOk] = useState<string | null>(null)
  // 表单初值只在数据首次到达时填一次；之后轮询刷新不覆盖手头正在改的输入
  const first = useRef(true)
  useEffect(() => {
    if (detail.data && first.current) {
      first.current = false
      setRetentionH(String(Math.round(detail.data.retention_ms / 3600000)))
    }
  }, [detail.data])

  async function onSave(e: FormEvent) {
    e.preventDefault()
    const h = Math.round(Number(retentionH))
    if (!h || h <= 0) return
    setSaving(true)
    setPatchErr(null)
    setPatchOk(null)
    try {
      // 表单填的是小时，接口要毫秒
      await api.patch(`/admin/topics/${encodeURIComponent(name)}`, { retention_ms: h * 3600000 })
      setRetentionH(String(h))
      setPatchOk(`已把 <b>${name}</b> 的 retention 改为 ${h} 小时，下一轮清理生效`)
      detail.refresh()
    } catch (err) {
      setPatchErr(err instanceof Error ? err.message : '保存失败')
    } finally {
      setSaving(false)
    }
  }

  const loading = (detail.loading && !detail.data) || (led.loading && !led.data)
  // 后端 404（topic 不存在或名字拼错）给空态而不是报错；其余错误走 Notice
  const notFound = detail.error instanceof ApiError && detail.error.status === 404

  return (
    <Shell title={`Topic ${detail.data?.name ?? name}`}
      crumb={<Link to="/topics">← 返回 Topic 列表</Link>}>
      {loading ? (
        <div className="empty"><p>加载中…</p></div>
      ) : notFound ? (
        <div className="empty"><p>topic 不存在</p></div>
      ) : detail.error ? (
        <Notice kind="bad">{detail.error.message}</Notice>
      ) : detail.data ? (
        <>
          {led.error && <Notice kind="bad">{led.error.message}</Notice>}
          {patchOk && <Notice kind="ok" onClose={() => setPatchOk(null)}>{patchOk}</Notice>}
          {patchErr && <Notice kind="bad" onClose={() => setPatchErr(null)}>{patchErr}</Notice>}

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">基本信息</span>
              <span className="panel-note">队列数创建后不可改，只有 retention 可调</span>
            </div>
            <div className="panel-body">
              <dl className="kv">
                <dt>名称</dt><dd>{detail.data.name}</dd>
                <dt>队列数</dt><dd>{detail.data.queues}</dd>
                <dt>RETENTION</dt>
                <dd>{dur(detail.data.retention_ms)}（{Math.round(detail.data.retention_ms / 3600000)} 小时）</dd>
                <dt>创建时间</dt>
                <dd>{timeText(detail.data.created_at_ms)} · {ago(detail.data.created_at_ms)}</dd>
                <dt>累计写入</dt><dd>{fmt(written)} 条</dd>
              </dl>

              <form className="form" style={{ marginTop: 16 }} onSubmit={onSave}>
                <div className="field">
                  <label>修改 RETENTION（小时）</label>
                  <input type="number" min={1} required value={retentionH}
                    onChange={e => setRetentionH(e.target.value)} />
                  <div className="hint">调小会让超期消息在下一轮清理时被删除，包括尚未被消费的消息</div>
                </div>
                <div className="btn-row">
                  <button className="btn primary" type="submit" disabled={saving}>
                    {saving ? '保存中…' : '保存'}
                  </button>
                  <span className="panel-note">PATCH /admin/topics/{name}</span>
                </div>
              </form>
            </div>
          </section>

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">队列</span>
              <span className="panel-note">{detail.data.queues_detail.length} 个队列 · 累计写入 {fmt(written)} 条</span>
            </div>
            <table>
              <thead>
                <tr>
                  <th>QUEUE ID</th>
                  <th className="r" style={{ width: 160 }}>NEXT OFFSET（写入头）</th>
                  <th className="r" style={{ width: 160 }}>占累计写入</th>
                </tr>
              </thead>
              <tbody>
                {detail.data.queues_detail.map(qd => (
                  <tr key={qd.queue_id}>
                    <td className="name">queue {qd.queue_id}</td>
                    <td className="num">{fmt(qd.next_offset)}</td>
                    {/* 全 0（新 topic 还没写过）时不做除法，避免渲染成 NaN% */}
                    <td className="num muted">{written ? `${(qd.next_offset / written * 100).toFixed(1)}%` : '0.0%'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">订阅此 Topic 的消费组</span>
              <span className="panel-note">带子共用同一把刻度：本表最大落后量</span>
            </div>
            {subs.length > 0 ? (
              <table>
                <thead>
                  <tr>
                    <th style={{ width: 3 }} />
                    <th>消费组</th>
                    <th style={{ width: 240 }}>落后（同一刻度）</th>
                    <th className="r" style={{ width: 96 }}>落后</th>
                    <th className="r" style={{ width: 72 }}>在途</th>
                    <th className="r" style={{ width: 72 }}>死信</th>
                    <th className="r" style={{ width: 96 }}>最后消费</th>
                  </tr>
                </thead>
                <tbody>
                  {subs.map(c => {
                    const lag = lagOf(c)
                    return (
                      <tr key={c.group}>
                        <td className={`mark ${markOf(c)}`} />
                        <td className="name">
                          <Link to={`/groups/${encodeURIComponent(c.group)}`}>{c.group}</Link>
                        </td>
                        <td><Ribbon cursor={c.cursor} head={c.next_offset} fly={c.inflight} scale={scale} /></td>
                        <td className={`num ${lag > 500 ? 'bad' : lag === 0 ? 'zero' : ''}`}>{fmt(lag)}</td>
                        <td className={`num ${c.inflight ? '' : 'zero'}`}>{c.inflight}</td>
                        <td className={`num ${c.dlq ? 'bad' : 'zero'}`}>{c.dlq}</td>
                        {/* 0 = 尚未观察到位点推进，与「很久以前消费过」是两件事，用占位符区分 */}
                        <td className="num muted">{c.last_consume_ms ? ago(c.last_consume_ms) : '—'}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            ) : (
              <div className="empty">
                <p>还没有消费组订阅这个 topic</p>
                <div className="hint">消息会一直堆到 retention 到期被清理，注意确认是否漏配了消费者</div>
              </div>
            )}
          </section>
        </>
      ) : null}
    </Shell>
  )
}