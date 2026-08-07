/**
 * Topic 列表页
 *
 * 职责：
 *   - 列出全部 topic 的配置属性（队列数 / retention / 创建时间）与两个派生量：
 *     累计写入（总账里该 topic 各消费组共同看到的写入头）、订阅它的消费组数
 *   - 承载 topic 的创建与删除入口，删除走二次确认
 *
 * 对应 Admin API：GET /admin/topics、POST /admin/topics、DELETE /admin/topics/{name}
 *
 * 边界：
 *   - 不展示消费进度细节（那是消费组视角），只给「有几个组订阅」的计数，具体进度点进详情页
 *   - 死信 topic（%DLQ% 前缀）是系统自建的，不进管理列表：用户没建过它，也不能删它
 *   - 队列数与 retention 建好后不可随意改，表单里给出默认值提示
 */
import { useEffect, useRef, useState, type FormEvent, type ReactNode } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { fmt, ago, dur } from '../lib/format'
import type { Topic } from '../api/types'

export default function Topics() {
  const list = usePoll(() => api.topics())
  const led = usePoll(() => api.ledger())
  // 死信 topic 是系统自建的，不该出现在「Topic 管理」列表里——
  // 用户没建过它，也不能删它（删了死信就没了）
  const topics = (list.data ?? []).filter(t => !t.name.startsWith('%DLQ%'))
  // 「累计写入」在列表接口里没有（GET /admin/topics 只给配置），
  // 用总账里该 topic 各组共同看到的写入头；没有任何组订阅时显示 —
  const writtenOf = (name: string): number | null =>
    (led.data ?? []).find(r => r.topic === name)?.next_offset ?? null
  const subsOf = (name: string) => (led.data ?? []).filter(r => r.topic === name)

  const [kw, setKw] = useState('')
  const rows = topics.filter(t => t.name.toLowerCase().includes(kw.trim().toLowerCase()))

  // —— 新建 topic 弹窗 —— //
  const [showNew, setShowNew] = useState(false)
  const [newName, setNewName] = useState('')
  const [newQueues, setNewQueues] = useState('4')
  const [newRetention, setNewRetention] = useState('')
  const [createErr, setCreateErr] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)

  // —— 删除 topic：破坏性且不可逆，必须二次确认，确认框写清连带影响 —— //
  const [pending, setPending] = useState<Topic | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [delErr, setDelErr] = useState<string | null>(null)
  const [delOk, setDelOk] = useState<ReactNode | null>(null)

  async function onCreate(e: FormEvent) {
    e.preventDefault()
    setCreating(true)
    setCreateErr(null)
    try {
      // retention 表单填的是小时，接口要毫秒，乘 3600000；留空则不传字段，交给服务端默认值
      const body: Record<string, unknown> = { name: newName.trim(), queues: Number(newQueues) }
      if (newRetention) body.retention_ms = Number(newRetention) * 3600000
      await api.post('/admin/topics', body)
      setShowNew(false)
      setNewName('')
      setNewRetention('')
      list.refresh()
    } catch (err) {
      // 409 等错误原样透出后端文案（如「topic X 已存在」），不自己造句
      setCreateErr(err instanceof Error ? err.message : '创建失败')
    } finally {
      setCreating(false)
    }
  }

  async function onDelete() {
    if (!pending) return
    setDeleting(true)
    setDelErr(null)
    try {
      await api.del(`/admin/topics/${encodeURIComponent(pending.name)}`)
      // 横幅是 JSX 不是字符串：Notice 只渲染纯文本 children，
      // 拼 HTML 字符串会把 <b> 当字面文本显示出来
      setDelOk(<>已删除 topic <b>{pending.name}</b>，其下所有消息数据一并清空</>)
      setPending(null)
      list.refresh()
    } catch (err) {
      setDelErr(err instanceof Error ? err.message : '删除失败')
    } finally {
      setDeleting(false)
    }
  }

  const openNew = () => {
    setCreateErr(null)
    setShowNew(true)
  }

  // 新建 topic 用 modal 承载：焦点陷阱、Esc 关闭、背景 inert 交给原生 <dialog>，
  // 与删除确认框（ConfirmDialog）保持一致，防止背景可点击造成误操作
  const newDialogRef = useRef<HTMLDialogElement>(null)
  useEffect(() => {
    const d = newDialogRef.current
    if (!d) return
    if (showNew && !d.open) d.showModal()
    if (!showNew && d.open) d.close()
  }, [showNew])

  const loading = (list.loading && !list.data) || (led.loading && !led.data)

  return (
    <Shell title="Topic">
      {loading ? (
        <div className="empty"><p>加载中…</p></div>
      ) : (
        <>
          {list.error && <Notice kind="bad">{list.error.message}</Notice>}
          {led.error && <Notice kind="bad">{led.error.message}</Notice>}
          {delOk && <Notice kind="ok" onClose={() => setDelOk(null)}>{delOk}</Notice>}

          <div className="filters">
            <input className="search" type="text" placeholder="按名称过滤，如 order"
              value={kw} onChange={e => setKw(e.target.value)} />
            <span className="count">{rows.length} / {topics.length} 个 topic</span>
            <button className="btn primary" onClick={openNew}>新建 Topic</button>
          </div>

          <table>
            <thead>
              <tr>
                <th>名称</th>
                <th className="r" style={{ width: 74 }}>队列数</th>
                <th className="r" style={{ width: 96 }}>RETENTION</th>
                <th className="r" style={{ width: 112 }}>累计写入</th>
                <th className="r" style={{ width: 96 }}>订阅组数</th>
                <th className="r" style={{ width: 96 }}>创建时间</th>
                <th className="r" style={{ width: 72 }}>操作</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(t => {
                const subs = subsOf(t.name)
                const written = writtenOf(t.name)
                return (
                  <tr key={t.name}>
                    <td className="name">
                      <Link to={`/topics/${encodeURIComponent(t.name)}`}>{t.name}</Link>
                    </td>
                    <td className="num">{t.queues}</td>
                    <td className="num muted">{dur(t.retention_ms)}</td>
                    <td className="num">{written == null ? '—' : fmt(written)}</td>
                    <td className={`num ${subs.length ? '' : 'zero'}`}>{subs.length}</td>
                    <td className="num muted">{ago(t.created_at_ms)}</td>
                    <td className="num">
                      <button className="btn danger" onClick={() => { setDelErr(null); setPending(t) }}>删除</button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>

          {rows.length === 0 && (
            <div className="empty">
              <p>没有匹配的 topic</p>
              <div className="hint">换个关键字，或点右上角「新建 Topic」</div>
            </div>
          )}
        </>
      )}

      {/* 新建 topic：队列数与 retention 建好后不可随意改，表单里给出默认值提示 */}
      <dialog ref={newDialogRef} onCancel={e => { e.preventDefault(); setShowNew(false) }}>
        <h3>新建 Topic</h3>
        <form onSubmit={onCreate}>
          <div className="dialog-body">
            {createErr && <Notice kind="bad">{createErr}</Notice>}
            <div className="form">
              <div className="field">
                <label>名称</label>
                <input type="text" placeholder="order.created" required autoFocus
                  value={newName} onChange={e => setNewName(e.target.value)} />
                <div className="hint">建议用 <span className="mono">业务.动作</span> 形式，创建后不可重命名</div>
              </div>
              <div className="field">
                <label>队列数</label>
                <input type="number" min={1} max={64}
                  value={newQueues} onChange={e => setNewQueues(e.target.value)} />
                <div className="hint">决定同一 topic 的最大并行消费度，创建后不可缩减</div>
              </div>
              <div className="field">
                <label>RETENTION（小时）</label>
                <input type="number" min={1} placeholder="留空 = 使用服务端默认 72 小时"
                  value={newRetention} onChange={e => setNewRetention(e.target.value)} />
                <div className="hint">超过该时长的消息会被后台清理，无论是否已被消费</div>
              </div>
            </div>
          </div>
          <div className="dialog-foot">
            <button className="btn" type="button" onClick={() => setShowNew(false)}>取消</button>
            <button className="btn primary" type="submit" disabled={creating}>
              {creating ? '创建中…' : '创建'}
            </button>
          </div>
        </form>
      </dialog>

      <ConfirmDialog open={pending !== null} title="删除 Topic" confirmText="确认删除"
        danger busy={deleting}
        onCancel={() => { setPending(null); setDelErr(null) }}
        onConfirm={onDelete}>
        {pending && (
          <>
            {delErr && <Notice kind="bad">{delErr}</Notice>}
            <div className="notice bad">
              <span>即将删除 <b>{pending.name}</b>。该操作<b>不可恢复</b>。</span>
            </div>
            <div className="kv">
              <dt>连带清空</dt>
              <dd>{pending.queues} 个队列 · 约 {fmt(writtenOf(pending.name) ?? 0)} 条累计写入</dd>
              <dt>受影响组</dt>
              <dd>{subsOf(pending.name).length
                ? subsOf(pending.name).map(c => c.group).join('、')
                : '当前无消费组订阅'}</dd>
            </div>
            <p className="muted">删除会同时清空该 topic 下所有队列的消息数据与写入位点，且不可恢复；
              仍在订阅它的消费者会立刻开始报错。请先确认没有生产者与消费者在使用该 topic。</p>
          </>
        )}
      </ConfirmDialog>
    </Shell>
  )
}