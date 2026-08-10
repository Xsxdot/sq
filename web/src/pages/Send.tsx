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
 *   - 延时/顺序的专属字段在提交前做前端必填校验：空值/0 会静默降级成普通消息
 *     立即投递，后端无法区分，必须在前端拦下；其余规则留给后端报错
 */
import { useEffect, useState, type FormEvent, type ReactNode } from 'react'
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
  const [err, setErr] = useState<ReactNode | null>(null)
  const [sent, setSent] = useState<{ msgId: string; topic: string } | null>(null)
  const [forwarded, setForwarded] = useState(false)

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
    // 延时：空值/0/垃圾输入直接拒绝——delay_ms 为 0 时后端「>0 才是延时」的判定
    // 会把它当作普通消息立即投递，这条真实写入路径上属于静默的类型降级
    const delayNum = Number(delayVal)
    if (type === 'DELAY' && (!Number.isInteger(delayNum) || delayNum <= 0)) {
      setErr('延时消息需要填写大于 0 的时长')
      return
    }
    // 顺序：MessageGroup 为空时后端无法与普通消息区分，会静默降级成立即投递
    const group = messageGroup.trim()
    if (type === 'FIFO' && !group) {
      setErr('顺序消息需要填写 MessageGroup')
      return
    }
    setBusy(true)
    setErr(null)
    setSent(null)
    setForwarded(false)
    try {
      const body: Record<string, unknown> = { topic: realTopic, body: text, tag, keys: keys ? [keys] : [] }
      if (type === 'DELAY') body.delay_ms = delayNum * UNIT_MS[delayUnit]
      if (type === 'FIFO') body.message_group = group
      const r = await api.send(body)
      setSent({ msgId: r.msg_id, topic: realTopic })
      setForwarded(r.forwarded === true)
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e)
      // 集群档的 ErrNotLeader：给可操作指引而不是一句底层错误文本。
      // 控制台的地址是运维随手挑的节点，本节点当前不是该组 leader 且
      // 转发未成功（可能是 leader 悬空或转发链故障）——告知用户去哪看
      if (msg.includes('不是该组 leader') || msg.includes('本节点不是该组 leader')) {
        setErr(
          <>
            {msg}。本节点当前不是该组 leader 且转发未成功，
            请到 <Link to="/cluster">集群页</Link> 查看当前 leader。
          </>,
        )
      } else {
        setErr(msg)
      }
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
    setForwarded(false)
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
          {forwarded && sent && (
            <Notice kind="ok" onClose={() => setForwarded(false)}>
              本节点不是该 topic 所属组的 leader，消息已自动转发给 leader 节点写入。
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