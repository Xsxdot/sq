/**
 * 消费组详情页
 *
 * 职责：
 *   - 展示单个消费组的全景：基本信息 + 按 topic 分组的队列级消费进度
 *   - 提供队列粒度的「重置消费位点」入口（危险操作，二次确认后才执行）
 *
 * 对应 Admin API：GET /admin/groups/{name}、POST /admin/groups/{name}/reset-cursor
 *
 * 边界：
 *   - 数据来自 GET /admin/groups/{name} 而不是总账：它直接给出按 topic
 *     分组的队列级进度，不需要再从 ledger 聚合（brief 明示）
 *   - 每个 topic 的带子用本 topic 内最大落后做刻度：跨 topic 的落后量
 *     量纲不同，用同一把全局尺会让小落后的 topic 的差异被大 topic 淹没
 *     （与列表页不画带子是同一条理由）
 *   - 位点重置会清空该队列全部 inflight，已投递未确认的消息会被重新投递
 *     或永久跳过，必须在确认框里写清后果
 *   - 创建/删除消费组不在此页，属于消费组列表页
 */
import { useEffect, useRef, useState, type ReactNode } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api, ApiError } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { Ribbon } from '../components/Ribbon'
import { fmt } from '../lib/format'
import type { QueueProgress } from '../api/types'

export default function GroupDetail() {
  const { name = '' } = useParams()
  const detail = usePoll(() => api.group(name))

  // 浏览器前进/后退在 /groups/:name 之间切换时组件不卸载：usePoll 的 interval
  // 只在挂载时建立，fn 虽已换成新组，首次取数仍要等下一个 5s 周期——切换后
  // 最多有 5 秒在拿旧组的数据冒充新组。与总览切档位同款：依赖变了立刻 refresh
  const firstMount = useRef(true)
  useEffect(() => {
    if (firstMount.current) {
      firstMount.current = false
      return
    }
    detail.refresh()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name])

  // —— 位点重置：危险且不可逆，必须二次确认，确认框写清 inflight 清空与重放影响 —— //
  const [pendingReset, setPendingReset] = useState<{
    topic: string
    queue: QueueProgress
  } | null>(null)
  const [offset, setOffset] = useState('')
  const [resetting, setResetting] = useState(false)
  const [resetErr, setResetErr] = useState<string | null>(null)
  const [resetOk, setResetOk] = useState<ReactNode | null>(null)

  function openReset(topic: string, q: QueueProgress) {
    setResetErr(null)
    // 默认填当前位点，等同于「不动」；用户改成别的值才是真正要重置到的地方
    setOffset(String(q.cursor))
    setPendingReset({ topic, queue: q })
  }

  async function onReset() {
    if (!pendingReset) return
    // 空输入（用户清空后直接确认）绝不能当成 0：重置到 0 会把整条队列从头重新投递，
    // 是不可逆的破坏性操作，必须显式报错而不是静默放行（Number('') === 0 会绕过整数校验）
    const trimmed = offset.trim()
    if (trimmed === '') {
      setResetErr('请填写目标位点')
      return
    }
    const target = Number(trimmed)
    if (!Number.isInteger(target) || target < 0) {
      setResetErr('目标 offset 必须是不小于 0 的整数')
      return
    }
    setResetting(true)
    setResetErr(null)
    try {
      await api.post(`/admin/groups/${encodeURIComponent(name)}/reset-cursor`, {
        topic: pendingReset.topic,
        queue_id: pendingReset.queue.queue_id,
        offset: target,
      })
      // 横幅是 JSX 不是字符串：Notice 只渲染纯文本 children，
      // 拼 HTML 字符串会把 <b> 当字面文本显示出来
      setResetOk(
        <>已重置 <b>{name}</b> / {pendingReset.topic} / queue {pendingReset.queue.queue_id}
          {' '}→ offset <b>{fmt(target)}</b></>,
      )
      setPendingReset(null)
      detail.refresh()
    } catch (err) {
      setResetErr(err instanceof Error ? err.message : '重置失败')
    } finally {
      setResetting(false)
    }
  }

  const loading = detail.loading && !detail.data
  // 后端 404（组不存在或名字拼错）给空态而不是报错；其余错误走 Notice
  const notFound = detail.error instanceof ApiError && detail.error.status === 404

  return (
    <Shell title="消费组"
      crumb={<Link to="/groups">← 消费组列表</Link>}>
      {loading ? (
        <div className="empty"><p>加载中…</p></div>
      ) : notFound ? (
        <div className="empty"><p>消费组不存在</p></div>
      ) : detail.error ? (
        <Notice kind="bad">{detail.error.message}</Notice>
      ) : detail.data ? (
        <>
          {resetOk && <Notice kind="ok" onClose={() => setResetOk(null)}>{resetOk}</Notice>}

          <section className="panel">
            <div className="panel-head">
              <span className="panel-title">基本信息</span>
              <span className="panel-note">GET /admin/groups/{name}</span>
            </div>
            <div className="panel-body">
              <dl className="kv">
                <dt>组名</dt><dd>{detail.data.name}</dd>
                <dt>MAX_ATTEMPTS</dt><dd>{detail.data.max_attempts}</dd>
                <dt>订阅 TOPIC</dt>
                <dd>{detail.data.topics.length}</dd>
                <dt>总落后</dt>
                <dd>{fmt(detail.data.topics.reduce((s, t) => s + t.queues.reduce((q, x) => q + Math.max(0, x.next_offset - x.cursor), 0), 0))}</dd>
                <dt>总在途</dt>
                <dd>{fmt(detail.data.topics.reduce((s, t) => s + t.queues.reduce((q, x) => q + x.inflight, 0), 0))}</dd>
              </dl>
            </div>
          </section>

          {detail.data.topics.length > 0 ? (
            detail.data.topics.map(t => {
              // 本 topic 共用一把刻度（本 topic 内最大队列落后），
              // 带子长短才能在本 topic 内横向比较；跨 topic 不可比用全局尺
              const scale = Math.max(1, ...t.queues.map(q => q.next_offset - q.cursor))
              return (
                <section className="panel" key={t.topic}>
                  <div className="panel-head">
                    <span className="panel-title">{t.topic}</span>
                    <span className="panel-note">{t.queues.length} 个队列 · 刻度为本 topic 最大落后 {fmt(scale)} 条</span>
                    <span className="spacer" />
                    <Link className="btn" to={`/messages?topic=${encodeURIComponent(t.topic)}`}>查询消息</Link>
                  </div>
                  <table>
                    <thead>
                      <tr>
                        <th style={{ width: 80 }}>QUEUE</th>
                        <th className="r" style={{ width: 110 }}>位点 CURSOR</th>
                        <th className="r" style={{ width: 110 }}>写入头 HEAD</th>
                        <th className="r" style={{ width: 88 }}>落后</th>
                        <th style={{ width: 200 }}>落后（同一刻度）</th>
                        <th className="r" style={{ width: 96 }}>操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      {t.queues.map(q => {
                        const lag = Math.max(0, q.next_offset - q.cursor)
                        return (
                          <tr key={q.queue_id}>
                            <td className="name">queue {q.queue_id}</td>
                            <td className="num">{fmt(q.cursor)}</td>
                            <td className="num muted">{fmt(q.next_offset)}</td>
                            <td className={`num ${lag > 500 ? 'bad' : lag === 0 ? 'zero' : ''}`}>{fmt(lag)}</td>
                            {/* compact：位点/落后已在独立列里，带子只画形状，不重复出字幕 */}
                            <td>
                              <Ribbon cursor={q.cursor} head={q.next_offset} fly={q.inflight} scale={scale} compact />
                            </td>
                            <td style={{ textAlign: 'right' }}>
                              <button className="btn danger" onClick={() => openReset(t.topic, q)}>重置位点</button>
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </section>
              )
            })
          ) : (
            <div className="panel">
              <div className="empty">
                <p>该消费组还没有订阅任何 topic</p>
                <div className="hint">消费者首次拉取时会自动建立消费关系</div>
              </div>
            </div>
          )}

          <div className="panel-note" style={{ marginTop: 14 }}>
            位点重置在服务端会持该队列的队列锁执行，与正在进行的投递互斥排队，
            避免重置与投递同时改写同一队列的位点。
          </div>
        </>
      ) : null}

      <ConfirmDialog open={pendingReset !== null} title="重置消费位点" confirmText="确认重置"
        danger busy={resetting}
        onCancel={() => { setPendingReset(null); setResetErr(null) }}
        onConfirm={onReset}>
        {pendingReset && (
          <>
            {resetErr && <Notice kind="bad">{resetErr}</Notice>}
            <div className="notice warn">
              <span>
                <b>重置会让该队列从新位点重新投递，并清空该队列全部 inflight 记录。</b>
                已投递未确认的消息会被重新投递或永久跳过。
                位点前移会把已消费但未过期的消息再投一遍（消费者需自行幂等），
                位点后移会永久跳过区间内尚未消费的消息。
                <b>请先停掉该组的消费者再操作</b>，否则新位点会与在途投递交叉。
              </span>
            </div>
            <div className="form">
              <div className="field">
                <label>TOPIC</label>
                <div className="mono">{pendingReset.topic}</div>
              </div>
              <div className="field">
                <label>QUEUE_ID</label>
                <div className="mono">queue {pendingReset.queue.queue_id}</div>
              </div>
              <div className="field">
                <label>目标 OFFSET</label>
                <input type="number" min={0} value={offset} required
                  onChange={e => { setOffset(e.target.value); setResetErr(null) }} />
                <div className="hint">
                  当前位点 {fmt(pendingReset.queue.cursor)}，写入头 {fmt(pendingReset.queue.next_offset)}；
                  可填 0 ~ {fmt(pendingReset.queue.next_offset)}。
                </div>
              </div>
            </div>
          </>
        )}
      </ConfirmDialog>
    </Shell>
  )
}