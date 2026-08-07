/**
 * 死信队列页
 *
 * 职责：
 *   - 按消费组浏览死信消息，重点呈现来源坐标、投递次数与最后一次失败原因
 *   - 提供单条「查看」（全量错误与消息体）与「重发」（危险操作，二次确认）
 *
 * 对应 Admin API：GET /admin/messages（读 %DLQ%{group} topic）、POST /admin/dlq/{group}/resend
 *
 * 边界：
 *   - 死信没有专门的列表端点——它就是名为 %DLQ%{group} 的普通 topic，本页用消息
 *     浏览（queue 0，自 offset 0 取前 200 条）读它；% 符号经 encodeURIComponent
 *     编码成 %25 走 query 参数
 *   - 不提供删除/清空死信：死信是审计凭证，M5a 只允许读与重发
 *   - 重发后死信条目保留（可再次重发），确认框里写清这一点
 */
import { useEffect, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { api } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { fmt, ago, timeText, decodeBody } from '../lib/format'
import type { Message } from '../api/types'

/** 表格里错误原文只留一行，超长截断，title 兜住全文，详情框看完整内容。 */
const CUT = 34
const trunc = (s: string): string => (s.length > CUT ? `${s.slice(0, CUT)}…` : s)

/** 取死信消息 Properties 里的一个属性；老死信没有时返回占位符「—」，
 *  不要显示成 0——0 会被读成「一次都没试过」。 */
function propOf(m: Message, k: string): string {
  const v = m.properties?.[k]
  return v || '—'
}

export default function Dlq() {
  const groups = usePoll(() => api.groups(), 30000)
  // 总览表的「查看死信」链接带 ?group= 过来说明要看哪个组，预填到下拉里
  const [searchParams] = useSearchParams()
  const preset = searchParams.get('group')

  const [group, setGroup] = useState('')
  const [rows, setRows] = useState<Message[] | null>(null)
  const [err, setErr] = useState<string | null>(null)

  async function load(g: string) {
    setErr(null)
    try {
      // %DLQ% 里的 % 必须编码；query 参数走 encodeURIComponent，
      // api.messagesByQueue 内部已经做了
      setRows(await api.messagesByQueue(`%DLQ%${g}`, 0, 0, 200))
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    }
  }

  // 组下拉首次就绪时初始化当前值：URL 带 ?group= 则预选，否则选第一个。
  // 只初始化一次，之后 30s 轮询刷新不覆盖用户手头正在选的值。
  useEffect(() => {
    if (group || !groups.data?.length) return
    const names = groups.data.map(g => g.name)
    setGroup(preset && names.includes(preset) ? preset : names[0])
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [groups.data])

  // 切组立即换一批死信；加载完成前旧列表先留着，避免整表闪白
  useEffect(() => {
    if (group) load(group)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [group])

  // 单条详情弹窗：原生 <dialog>，与消息查询页同款用法
  const detailRef = useRef<HTMLDialogElement>(null)
  const [detail, setDetail] = useState<Message | null>(null)
  useEffect(() => {
    const d = detailRef.current
    if (!d) return
    if (detail && !d.open) d.showModal()
    if (!detail && d.open) d.close()
  }, [detail])

  // —— 重发：危险操作，二次确认；确认框写清坐标语义与死信记录去留 —— //
  const [pending, setPending] = useState<Message | null>(null)
  const [resending, setResending] = useState(false)
  const [resendErr, setResendErr] = useState<string | null>(null)
  const [resendOk, setResendOk] = useState<string | null>(null)

  async function onResend() {
    if (!pending) return
    setResending(true)
    setResendErr(null)
    try {
      await api.post(`/admin/dlq/${encodeURIComponent(group)}/resend`, {
        queue_id: pending.queue_id,
        offset: pending.offset,
      })
      setResendOk(`已提交重发 <b>${pending.id}</b> → ${propOf(pending, 'sq-origin-topic')}（死信条目保留）`)
      setPending(null)
      load(group)
    } catch (e) {
      setResendErr(e instanceof Error ? e.message : '重发失败')
    } finally {
      setResending(false)
    }
  }

  const noGroups = !!groups.data && groups.data.length === 0

  return (
    <Shell title="死信队列">
      {groups.loading && !groups.data ? (
        <div className="empty"><p>加载中…</p></div>
      ) : (
        <>
          {groups.error && <Notice kind="bad">{groups.error.message}</Notice>}
          {err && <Notice kind="bad" onClose={() => setErr(null)}>{err}</Notice>}
          {resendOk && <Notice kind="ok" onClose={() => setResendOk(null)}>{resendOk}</Notice>}

          <div className="filters">
            <select value={group} onChange={e => setGroup(e.target.value)} title="消费组">
              {(groups.data ?? []).map(g => (
                <option key={g.name} value={g.name}>{g.name}</option>
              ))}
            </select>
            {group ? (
              <Link className="btn" to={`/groups/${encodeURIComponent(group)}`}>查看该组详情</Link>
            ) : (
              <span className="btn" style={{ opacity: 0.5, pointerEvents: 'none' }}>查看该组详情</span>
            )}
            <span className="count">{group && rows ? `${group} · ${rows.length} 条死信` : ''}</span>
          </div>

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">死信消息</span>
              <span className="panel-note">超过 max_attempts 后转入 %DLQ%{group}，保留来源坐标以便原样重发</span>
            </div>

            {noGroups ? (
              <div className="empty">
                <p>当前没有任何消费组</p>
                <div className="hint">消费组由消费者首次订阅时自动创建</div>
              </div>
            ) : rows === null ? (
              <div className="empty"><p>加载中…</p></div>
            ) : rows.length === 0 ? (
              <div className="empty">
                <p>该消费组当前没有死信</p>
                <div className="hint">消费失败次数达到该组的 max_attempts 后，消息才会转入死信队列</div>
              </div>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th style={{ width: 250 }}>MSG_ID</th>
                    <th style={{ width: 170 }}>来源</th>
                    <th style={{ width: 160 }}>KEYS</th>
                    <th className="r" style={{ width: 70 }}>投递次数</th>
                    <th className="r" style={{ width: 88 }}>入库</th>
                    <th>最后错误</th>
                    <th className="r" style={{ width: 120 }}>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map(m => {
                    const off = m.properties?.['sq-origin-offset']
                    return (
                      <tr key={m.id}>
                        <td className="mono" style={{ fontSize: 11 }}>{m.id}</td>
                        <td className="name">
                          {propOf(m, 'sq-origin-topic')}
                          <small>queue {propOf(m, 'sq-origin-queue')} · offset {off ? fmt(Number(off)) : '—'}</small>
                        </td>
                        <td className="mono">{m.keys?.length
                          ? m.keys.join(', ')
                          : <span className="dim">—</span>}</td>
                        <td className="num bad">{propOf(m, 'sq-dlq-attempts')}</td>
                        <td className="num muted">{ago(m.store_at_ms)}</td>
                        <td className="muted" title={propOf(m, 'sq-dlq-reason')}>
                          {trunc(propOf(m, 'sq-dlq-reason'))}
                        </td>
                        <td style={{ textAlign: 'right' }}>
                          <span className="btn-row" style={{ justifyContent: 'flex-end' }}>
                            <button className="btn" onClick={() => setDetail(m)}>查看</button>
                            <button className="btn danger"
                              onClick={() => { setPending(m); setResendErr(null) }}>重发</button>
                          </span>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            )}
          </section>
        </>
      )}

      {/* 查看：完整错误原文 + 消息体 */}
      <dialog ref={detailRef} onCancel={e => { e.preventDefault(); setDetail(null) }}>
        <h3>死信详情</h3>
        <div className="dialog-body">
          {detail && (
            <>
              <dl className="kv">
                <dt>MSG_ID</dt><dd>{detail.id}</dd>
                <dt>消费组</dt><dd>{group}</dd>
                <dt>死信坐标</dt><dd>queue {detail.queue_id} / offset {fmt(detail.offset)}</dd>
                <dt>来源 TOPIC</dt><dd>{propOf(detail, 'sq-origin-topic')}</dd>
                <dt>来源坐标</dt>
                <dd>queue {propOf(detail, 'sq-origin-queue')} / offset {detail.properties?.['sq-origin-offset']
                  ? fmt(Number(detail.properties['sq-origin-offset'])) : '—'}</dd>
                <dt>KEYS</dt><dd>{detail.keys?.length ? detail.keys.join(', ') : '—'}</dd>
                <dt>投递次数</dt><dd>{propOf(detail, 'sq-dlq-attempts')}</dd>
                <dt>入库时间</dt>
                <dd>{timeText(detail.store_at_ms)} <span className="dim">({ago(detail.store_at_ms)})</span></dd>
              </dl>
              <div className="detail-head" style={{ marginTop: 16 }}>最后错误 LAST_ERROR</div>
              <div className="mono" style={{ wordBreak: 'break-all' }}>
                {propOf(detail, 'sq-dlq-reason')}
              </div>
              <div className="detail-head" style={{ marginTop: 14 }}>消息体 BODY</div>
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

      <ConfirmDialog open={pending !== null} title="重发死信消息" confirmText="确认重发"
        danger busy={resending}
        onCancel={() => { setPending(null); setResendErr(null) }}
        onConfirm={onResend}>
        {pending && (
          <>
            {resendErr && <Notice kind="bad">{resendErr}</Notice>}
            <div className="notice warn">
              <span>
                将按来源坐标重新投递回原 topic <b>{propOf(pending, 'sq-origin-topic')}</b>，
                <b>消息 ID 保持不变</b>，下游若不幂等会看到同一条消息再次到达。
                <b>死信条目会保留</b>（审计凭证），可以再次重发。
              </span>
            </div>
            <dl className="kv">
              <dt>MSG_ID</dt><dd>{pending.id}</dd>
              <dt>目标 TOPIC</dt><dd>{propOf(pending, 'sq-origin-topic')}</dd>
              <dt>来源坐标</dt>
              <dd>queue {propOf(pending, 'sq-origin-queue')} / offset {pending.properties?.['sq-origin-offset']
                ? fmt(Number(pending.properties['sq-origin-offset'])) : '—'}</dd>
              <dt>KEYS</dt><dd>{pending.keys?.length ? pending.keys.join(', ') : '—'}</dd>
              <dt>已投递</dt><dd>{propOf(pending, 'sq-dlq-attempts')} 次</dd>
            </dl>
          </>
        )}
      </ConfirmDialog>
    </Shell>
  )
}
