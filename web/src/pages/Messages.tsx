/**
 * 消息查询页
 *
 * 职责：
 *   - 提供两种排查入口：按业务 Keys 检索（keyidx）、按队列 + 起始 offset 顺序浏览
 *   - 列出命中的消息摘要，并可展开单条消息的全部字段与解码后的消息体
 *
 * 对应 Admin API：GET /admin/messages
 *
 * 边界：
 *   - 本页不轮询：查询是用户主动发起的动作，5 秒重查会换掉用户正在看的结果；
 *     topic 下拉来自变化很慢的 topic 列表，单独用 30s 轮询
 *   - 只查不改：不在此页提供重投/删除入口（重投属于死信页）
 *   - 不支持按 msgId 精确查询（需要独立全局索引，与 retention 按 offset 区间
 *     整段删除的存储模型冲突），页面上用 panel-note 明确标注该边界
 */
import { useEffect, useRef, useState, type FormEvent } from 'react'
import { useSearchParams } from 'react-router-dom'
import { api } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { fmt, ago, until, timeText, decodeBody } from '../lib/format'
import type { Message, TopicDetail } from '../api/types'

/** 消息类型不在 API 字段里，按语义推导：有 message_group 是顺序，有 deliver_at_ms 是延时。 */
function typeOf(m: Message): string {
  if (m.message_group) return 'FIFO'
  if (m.deliver_at_ms) return 'DELAY'
  return 'NORMAL'
}

export default function Messages() {
  const topics = usePoll(() => api.topics(), 30000)
  const [searchParams] = useSearchParams()
  // 总览表的「查询消息」链接带 ?topic= 过来说明要看哪个 topic，预填到下拉里
  const preset = searchParams.get('topic')

  const [topic, setTopic] = useState('')
  const [mode, setMode] = useState<'keys' | 'queue'>('keys')
  const [keys, setKeys] = useState('')
  const [queueId, setQueueId] = useState('')
  const [fromOffset, setFromOffset] = useState('0')
  const [limit, setLimit] = useState('32')
  const [queues, setQueues] = useState<TopicDetail['queues_detail']>([])

  const [rows, setRows] = useState<Message[] | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  // topic 下拉首次就绪时初始化当前值：URL 带 ?topic= 则预选，否则选第一个。
  // 只初始化一次，之后轮询刷新不覆盖用户手头正在选的值。
  useEffect(() => {
    if (topic || !topics.data?.length) return
    const names = topics.data.map(t => t.name)
    setTopic(preset && names.includes(preset) ? preset : names[0])
  }, [topics.data, topic, preset])

  // 队列下拉随 topic 变化：不同 topic 的队列数不同，切换后旧 queue_id 可能越界。
  // 不复用轮询——队列只在该 topic 被选中时才需要，且查询是主动动作。
  useEffect(() => {
    if (!topic) return
    let alive = true
    api.topic(topic)
      .then(d => {
        if (!alive) return
        setQueues(d.queues_detail)
        // 当前选的 queue_id 在新 topic 里不存在时，落到第一个队列
        if (!d.queues_detail.some(q => String(q.queue_id) === queueId)) {
          setQueueId(String(d.queues_detail[0]?.queue_id ?? ''))
        }
      })
      .catch(() => setQueues([])) // 拉不到队列细节时保持空，不打断查询流程
    return () => { alive = false }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topic])

  async function onQuery(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    setErr(null)
    try {
      // 两条路径与后端一致：填了 Keys 走 keyidx，否则 queue_id 必填走顺序浏览
      setRows(keys.trim()
        ? await api.messagesByKey(topic, keys.trim(), Math.max(1, Number(limit) || 32))
        : await api.messagesByQueue(topic, Number(queueId), Number(fromOffset) || 0,
            Math.max(1, Number(limit) || 32)))
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  const resultNote = rows === null ? '' : mode === 'keys'
    ? `topic=${topic} · 按 Keys 检索 · 最多 ${limit} 条`
    : `topic=${topic} · queue ${queueId} · 自 offset ${fromOffset} 起 · 最多 ${limit} 条`

  // 单条详情弹窗：原生 <dialog>，与新建 topic 弹窗同款用法
  const detailRef = useRef<HTMLDialogElement>(null)
  const [detail, setDetail] = useState<Message | null>(null)
  useEffect(() => {
    const d = detailRef.current
    if (!d) return
    if (detail && !d.open) d.showModal()
    if (!detail && d.open) d.close()
  }, [detail])

  return (
    <Shell title="消息查询">
      {topics.loading && !topics.data ? (
        <div className="empty"><p>加载中…</p></div>
      ) : (
        <>
          {topics.error && <Notice kind="bad">{topics.error.message}</Notice>}
          {err && <Notice kind="bad" onClose={() => setErr(null)}>{err}</Notice>}

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">查询条件</span>
              <span className="panel-note">GET /admin/messages</span>
            </div>
            <div className="panel-body">
              <div className="tabs">
                <button className={mode === 'keys' ? 'tab active' : 'tab'}
                  onClick={() => setMode('keys')}>按 Keys 检索</button>
                <button className={mode === 'queue' ? 'tab active' : 'tab'}
                  onClick={() => setMode('queue')}>按队列浏览</button>
              </div>

              <form onSubmit={onQuery}>
                <div className="filters">
                  <select value={topic} onChange={e => setTopic(e.target.value)} title="topic">
                    {(topics.data ?? []).map(t => (
                      <option key={t.name} value={t.name}>{t.name}</option>
                    ))}
                  </select>

                  <input type="text" className="search" placeholder="业务 Keys，如 ORD-20260806-8842"
                    value={keys} onChange={e => setKeys(e.target.value)}
                    hidden={mode !== 'keys'} />

                  <select value={queueId} onChange={e => setQueueId(e.target.value)}
                    title="queue_id" hidden={mode !== 'queue'}>
                    {queues.map(q => (
                      <option key={q.queue_id} value={q.queue_id}>
                        queue {q.queue_id} · 写入头 {fmt(q.next_offset)}
                      </option>
                    ))}
                  </select>
                  <input type="number" min={0} value={fromOffset}
                    onChange={e => setFromOffset(e.target.value)}
                    title="起始 offset" hidden={mode !== 'queue'} style={{ width: 120 }} />

                  <input type="number" min={1} max={200} value={limit}
                    onChange={e => setLimit(e.target.value)}
                    title="条数 limit" style={{ width: 78 }} />
                  <button type="submit" className="btn primary" disabled={busy}>
                    {busy ? '查询中…' : '查询'}
                  </button>
                  <span className="count">{rows ? `命中 ${rows.length} 条` : ''}</span>
                </div>
              </form>

              <div className="panel-note" style={{ marginTop: 4 }}>
                边界：M5a <b>不支持按 msgId 精确查询</b> —— 那需要额外维护一份全局 msgId 索引，
                与 retention 按 offset 区间整段删除的存储模型冲突。排查请用 Keys 检索，
                或已知坐标时按队列 + 起始 offset 顺序浏览。
              </div>
            </div>
          </section>

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">查询结果</span>
              <span className="panel-note">{resultNote}</span>
            </div>
            <table hidden={!rows || rows.length === 0}>
              <thead>
                <tr>
                  <th style={{ width: 250 }}>MSG_ID</th>
                  <th style={{ width: 120 }}>TOPIC</th>
                  <th style={{ width: 110 }}>QUEUE / OFFSET</th>
                  <th>KEYS</th>
                  <th style={{ width: 80 }}>TAG</th>
                  <th style={{ width: 78 }}>类型</th>
                  <th style={{ width: 160 }}>写入时间</th>
                  <th className="r" style={{ width: 70 }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {(rows ?? []).map(m => (
                  <tr key={m.id}>
                    <td className="mono" style={{ fontSize: 11 }}>{m.id}</td>
                    <td className="name">{m.topic}</td>
                    <td className="mono muted">{m.queue_id} / {fmt(m.offset)}</td>
                    <td className="mono">{m.keys?.length
                      ? m.keys.join(', ')
                      : <span className="dim">—</span>}</td>
                    <td className="mono muted">{m.tag || <span className="dim">—</span>}</td>
                    <td><span className="badge type">{typeOf(m)}</span></td>
                    <td className="mono muted">{timeText(m.store_at_ms)}</td>
                    <td style={{ textAlign: 'right' }}>
                      <button className="btn" onClick={() => setDetail(m)}>查看</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {(!rows || rows.length === 0) && (
              <div className="empty">
                <p>没有命中任何消息</p>
                <div className="hint">换个 Keys，或改用「按队列浏览」从已知 offset 附近翻</div>
              </div>
            )}
          </section>
        </>
      )}

      {/* 单条消息详情：全字段 + 解码后的原始消息体 */}
      <dialog ref={detailRef} onCancel={e => { e.preventDefault(); setDetail(null) }}>
        <h3>消息详情</h3>
        <div className="dialog-body">
          {detail && (
            <>
              <dl className="kv">
                <dt>MSG_ID</dt><dd>{detail.id}</dd>
                <dt>TOPIC</dt><dd>{detail.topic}</dd>
                <dt>QUEUE</dt><dd>{detail.queue_id}</dd>
                <dt>OFFSET</dt><dd>{fmt(detail.offset)}</dd>
                <dt>KEYS</dt><dd>{detail.keys?.length ? detail.keys.join(', ') : '—'}</dd>
                <dt>TAG</dt><dd>{detail.tag || '—'}</dd>
                <dt>类型</dt><dd><span className="badge type">{typeOf(detail)}</span></dd>
                <dt>生产时间</dt><dd>{timeText(detail.born_at_ms)}</dd>
                <dt>写入时间</dt>
                <dd>{timeText(detail.store_at_ms)} <span className="dim">({ago(detail.store_at_ms)})</span></dd>
                {/* deliver_at_ms 存在才显示：普通消息没有「投递时间」这一说 */}
                {detail.deliver_at_ms ? (
                  <>
                    <dt>投递时间</dt>
                    <dd>{timeText(detail.deliver_at_ms)} <span className="dim">({until(detail.deliver_at_ms)})</span></dd>
                  </>
                ) : null}
                {detail.message_group ? (
                  <>
                    <dt>顺序分组</dt>
                    <dd>{detail.message_group}</dd>
                  </>
                ) : null}
                {detail.properties && Object.keys(detail.properties).length > 0 ? (
                  <>
                    <dt>PROPERTIES</dt>
                    <dd>{JSON.stringify(detail.properties)}</dd>
                  </>
                ) : null}
              </dl>
              <div className="detail-head" style={{ marginTop: 16 }}>消息体 BODY</div>
              <div className="mono" style={{ wordBreak: 'break-all' }}>
                {decodeBody(detail.body_base64)}
              </div>
            </>
          )}
        </div>
        <div className="dialog-foot">
          <button className="btn" onClick={() => setDetail(null)}>关闭</button>
        </div>
      </dialog>
    </Shell>
  )
}