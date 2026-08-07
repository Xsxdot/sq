/**
 * 发送测试消息页
 *
 * 职责：
 *   - 提供运维排查用的手工发送入口：选 topic、选消息类型、填 Keys/Tag/消息体
 *   - 按消息类型追加类型专属字段（延时 → 延迟多久投递；顺序 → MessageGroup）
 *   - 发送后给出 msgId 并引导到「消息查询」验证消息确实落库
 *
 * 对应 Admin API：POST /admin/messages/send
 *
 * 边界：
 *   - 只负责「造一条消息」，不负责验证消费结果；消费进度看总览与消费组页
 *   - 这是真实写入路径，不是 dry-run：真实环境下发出去的消息会被消费者消费，
 *     因此页面顶部常驻生产环境告警
 *   - MessageGroup 不做前端必填校验——留给后端报错，避免前端校验与后端规则漂移
 */
import { useEffect, useState, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'

const DEFAULT_BODY = `{
  "orderId": "ORD-20260806-9001",
  "userId": 90218,
  "amount": 29900,
  "items": 3
}`

/** 延迟时长单位 → 毫秒。与原型选项（秒/分钟/小时）一一对应。 */
const UNIT_MS = { s: 1000, m: 60000, h: 3600000 } as const

export default function Send() {
  const topics = usePoll(() => api.topics())

  const [topic, setTopic] = useState('')
  const [newTopic, setNewTopic] = useState('')
  const [type, setType] = useState<'NORMAL' | 'DELAY' | 'FIFO'>('NORMAL')
  const [delayVal, setDelayVal] = useState('30')
  const [delayUnit, setDelayUnit] = useState<'s' | 'm' | 'h'>('m')
  const [messageGroup, setMessageGroup] = useState('')
  const [keys, setKeys] = useState('')
  const [tag, setTag] = useState('')
  const [text, setText] = useState(DEFAULT_BODY)

  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [sent, setSent] = useState<{ msgId: string; topic: string } | null>(null)

  // topic 下拉只在数据首次到达时填一次初值，之后轮询刷新不覆盖用户正在选的
  useEffect(() => {
    if (topic || !topics.data?.length) return
    setTopic(topics.data[0].name)
  }, [topics.data, topic])

  async function onSend(e: FormEvent) {
    e.preventDefault()
    // 「手动输入新 topic」逃生口：选的是 __new__ 时用文本框里的名字
    const realTopic = topic === '__new__' ? newTopic.trim() : topic
    if (!realTopic) {
      setErr('请先填写新 topic 的名称')
      return
    }
    setBusy(true)
    setErr(null)
    setSent(null)
    try {
      const body: Record<string, unknown> = { topic: realTopic, body: text, tag, keys: keys ? [keys] : [] }
      if (type === 'DELAY') body.delay_ms = (Number(delayVal) || 0) * UNIT_MS[delayUnit]
      if (type === 'FIFO') body.message_group = messageGroup
      const r = await api.send(body)
      setSent({ msgId: r.msg_id, topic: realTopic })
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  function onReset() {
    setNewTopic('')
    setDelayVal('30')
    setDelayUnit('m')
    setMessageGroup('')
    setKeys('')
    setTag('')
    setText(DEFAULT_BODY)
    setErr(null)
    setSent(null)
  }

  return (
    <Shell title="发送测试消息">
      <div className="notice warn">
        <span>这是<b>运维排查用</b>的手工发送入口：消息会真实写入队列并被在线消费者消费，
          不可撤回。<b>请勿在生产环境对业务 topic 使用</b>，验证请走专用的测试 topic。</span>
      </div>

      {topics.loading && !topics.data ? (
        <div className="empty"><p>加载中…</p></div>
      ) : (
        <>
          {topics.error && <Notice kind="bad">{topics.error.message}</Notice>}
          {err && <Notice kind="bad" onClose={() => setErr(null)}>{err}</Notice>}
          {sent && (
            <Notice kind="ok" onClose={() => setSent(null)}>
              已发送到 <b>{sent.topic}</b>。msgId <span className="mono">{sent.msgId}</span>
              {' '}· <Link to={`/messages?topic=${encodeURIComponent(sent.topic)}`}>到消息查询里看看 →</Link>
            </Notice>
          )}

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">构造消息</span>
              <span className="panel-note">字段与 SDK 的 Producer.Send 一一对应</span>
            </div>
            <div className="panel-body">
              <form className="form" onSubmit={onSend} onReset={onReset}>
                <div className="field">
                  <label>TOPIC</label>
                  <select value={topic} onChange={e => setTopic(e.target.value)}>
                    {(topics.data ?? []).map(t => (
                      <option key={t.name} value={t.name}>{t.name}（{t.queues} 队列）</option>
                    ))}
                    <option value="__new__">手动输入新 topic…</option>
                  </select>
                </div>

                {/* 选「手动输入新 topic」时才出现：broker 允许发往尚未显式创建的 topic */}
                <div className="field" hidden={topic !== '__new__'}>
                  <label>新 TOPIC 名称</label>
                  <input type="text" value={newTopic} onChange={e => setNewTopic(e.target.value)}
                    placeholder="test.playground" />
                  <div className="hint">若该 topic 尚不存在，broker 会按默认队列数自动创建</div>
                </div>

                <div className="field">
                  <label>消息类型</label>
                  <select value={type} onChange={e => setType(e.target.value as 'NORMAL' | 'DELAY' | 'FIFO')}>
                    <option value="NORMAL">普通</option>
                    <option value="DELAY">延时</option>
                    <option value="FIFO">顺序</option>
                  </select>
                </div>

                {/* 延时专属：延迟时长在提交时换算成毫秒 delay_ms，绝不用 hidden 之外的条件渲染
                    做显隐——hidden 已被 app.css 的 [hidden]{display:none!important} 兜住 */}
                <div className="field" hidden={type !== 'DELAY'}>
                  <label>延迟多久投递</label>
                  <div className="btn-row">
                    <input type="number" value={delayVal} onChange={e => setDelayVal(e.target.value)}
                      min={1} style={{ width: 110 }} />
                    <select value={delayUnit} onChange={e => setDelayUnit(e.target.value as 's' | 'm' | 'h')}
                      style={{ width: 96 }}>
                      <option value="s">秒</option>
                      <option value="m">分钟</option>
                      <option value="h">小时</option>
                    </select>
                  </div>
                  <div className="hint">到期前消息停在延时队列里，可在「延时队列」页看到它</div>
                </div>

                {/* 顺序专属：消息组决定分区，同组内严格有序 */}
                <div className="field" hidden={type !== 'FIFO'}>
                  <label>消息组 MESSAGEGROUP</label>
                  <input type="text" value={messageGroup} onChange={e => setMessageGroup(e.target.value)}
                    placeholder="ORD-20260806-8839" />
                  <div className="hint">同一消息组内的消息严格有序：投递到同一队列、一次只放一条，
                    前一条未 ack 之前后面的不会投递</div>
                </div>

                <div className="field">
                  <label>KEYS</label>
                  <input type="text" value={keys} onChange={e => setKeys(e.target.value)}
                    placeholder="ORD-20260806-8842" />
                  <div className="hint">用于之后在「消息查询」里按 Keys 检索，可留空</div>
                </div>

                <div className="field">
                  <label>TAG</label>
                  <input type="text" value={tag} onChange={e => setTag(e.target.value)}
                    placeholder="created" />
                  <div className="hint">消费端按 Tag 过滤，只订阅自己关心的那部分消息，可留空</div>
                </div>

                <div className="field">
                  <label>消息体</label>
                  <textarea value={text} onChange={e => setText(e.target.value)} spellCheck={false} />
                  <div className="hint">按原始字节存储，broker 不校验格式；JSON 只是这里的示例</div>
                </div>

                <div className="btn-row">
                  <button className="btn primary" type="submit" disabled={busy}>
                    {busy ? '发送中…' : '发送'}
                  </button>
                  <button className="btn" type="reset">重置</button>
                </div>
              </form>
            </div>
          </section>
        </>
      )}
    </Shell>
  )
}