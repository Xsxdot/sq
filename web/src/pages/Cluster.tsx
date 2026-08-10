/**
 * 集群页：成员表、每组角色与待 apply、leader 侧复制进度
 *
 * 职责：
 *   - 展示 GET /admin/cluster 的拓扑快照，5 秒轮询
 *   - 落后量用 offset 带表现（沿用全站既有的位点差语汇），刻度 = 本视图
 *     内 commit−applied 的最大值
 *   - 快照挂起（pending_snapshot 非零）醒目标记
 *
 * 边界：
 *   - 单机档（enabled=false）渲染「当前为单机模式」，不当成错误
 *   - 只读：领导权转移、成员变更不在本页（管理动作要独立确认流程）
 *   - 复制进度只在本节点是该组 leader 时有数据——peers 为空不等于
 *     「没有 peer」，必须按 is_leader 判断，否则会把 follower 视角
 *     误显示成「集群里只有我一个」
 */
import { api } from '../api/client'
import { usePoll } from '../hooks/usePoll'
import { Shell } from '../components/Shell'
import { Notice } from '../components/Notice'
import { fmt } from '../lib/format'
import type { ClusterGroup } from '../api/types'

/** 角色徽标的可读名（去掉 raft 内核的 State 前缀）。 */
function roleLabel(role: string): string {
  if (role.startsWith('State')) return role.slice('State'.length)
  return role
}

/** 「待 apply」= commit − applied：每节点自己就算得出的落后量。 */
const pendingApply = (g: ClusterGroup): number => Math.max(0, g.commit - g.applied)

/** 本视图共用一把刻度的最大值：同视图内 offset 带才能横向比较。 */
function maxPendingApply(groups: ClusterGroup[]): number {
  return Math.max(1, ...groups.map(pendingApply))
}

export default function Cluster() {
  const view = usePoll(() => api.cluster(), 5000)

  // 取数失败就是整页内容都没了，必须显式报错而不是静默降级
  if (view.error && !view.data) {
    return (
      <Shell title="集群">
        <Notice kind="bad">{view.error.message}</Notice>
      </Shell>
    )
  }
  if (view.loading && !view.data) {
    return <Shell title="集群"><div className="empty"><p>加载中…</p></div></Shell>
  }

  const v = view.data
  if (!v) return <Shell title="集群"><div className="empty"><p>暂无数据</p></div></Shell>

  // 单机档（enabled=false）：控制台在单机部署下也会打开这个页面，那不是
  // 故障——显式渲染「当前为单机模式」而不是报错
  if (!v.enabled) {
    return (
      <Shell title="集群">
        <div className="notice">
          <span>当前为<b>单机模式</b>，未启用集群复制——没有 raft 组与成员表可展示。</span>
        </div>
      </Shell>
    )
  }

  // 本节点不是任何组 leader：挂一条中性提示（承接写转发 UX），不是警告
  const anyLeader = v.groups.some(g => g.is_leader)
  const scale = maxPendingApply(v.groups)

  return (
    <Shell title="集群">
      {!anyLeader && (
        <div className="notice">
          <span>本节点当前<b>不是任何 raft 组的 leader</b>，写请求会被自动转发到对应组的 leader 节点。</span>
        </div>
      )}

      <section className="panel">
        <div className="panel-head">
          <span className="panel-title">节点</span>
          <span className="panel-note">本页面视角所在节点高亮</span>
        </div>
        <div className="strip">
          {v.nodes.map(n => (
            <div className="stat" key={n.id}>
              <div>
                <div className="stat-label">{n.self ? '本节点 · ' : ''}节点 {n.id}</div>
                <div className="stat-val" style={{ fontSize: 17 }}>{n.self ? '本节点' : 'peer'}</div>
                <small className="muted" style={{ fontFamily: 'var(--mono)' }}>{n.raft_addr}</small>
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="panel">
        <div className="panel-head">
          <span className="panel-title">Raft 组</span>
          <span className="panel-note">
            「待 apply」= commit − applied，applier 卡住立刻显形；offset 带同视图共用一把刻度
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th style={{ width: 70 }}>组号</th>
              <th style={{ width: 110 }}>LEADER</th>
              <th style={{ width: 110 }}>本节点角色</th>
              <th className="r" style={{ width: 100 }}>APPLIED</th>
              <th className="r" style={{ width: 100 }}>COMMIT</th>
              <th className="r" style={{ width: 96 }}>任期</th>
              <th>待 apply（commit−applied）</th>
              <th style={{ width: 220 }}>复制进度</th>
            </tr>
          </thead>
          <tbody>
            {v.groups.map(g => {
              const lag = pendingApply(g)
              const pct = Math.min(100, (lag / scale) * 100)
              const sev = lag > 500 ? 'rib-warn' : 'rib-ok'
              return (
                <tr key={g.id}>
                  <td className="num">{g.id}{g.id === 0 && <small className="muted"> meta</small>}</td>
                  <td className="name">{g.leader ? `节点 ${g.leader}` : '—'}</td>
                  <td>
                    {g.is_leader
                      ? <span className="badge type">leader</span>
                      : <span className={`badge ${g.role !== 'StateFollower' ? 'warn' : ''}`}>{roleLabel(g.role)}</span>}
                  </td>
                  <td className="num">{fmt(g.applied)}</td>
                  <td className="num">{fmt(g.commit)}</td>
                  <td className="num">{g.term}</td>
                  <td>
                    <div className="ribbon">
                      <span className={sev} style={{ width: `${pct.toFixed(1)}%` }} />
                      <span className="cursor" />
                      {lag === 0 && <span className="caught">已追平</span>}
                    </div>
                    <div className="prog"><span>待 apply {fmt(lag)}</span></div>
                  </td>
                  <td>
                    {g.is_leader ? (
                      g.peers.map(p => {
                        const peerLag = Math.max(0, g.commit - p.match)
                        return (
                          <div className="prog" key={p.id} style={{ margin: '2px 0' }}>
                            peer {p.id}：Match {fmt(p.match)} · 落后 {fmt(peerLag)}
                            {p.pending_snapshot > 0 &&
                              <span className="badge warn" title="该 peer 正在被发快照，长期非零即「快照卡住」现场">快照挂起</span>}
                            {!p.recent_active && <span className="badge warn">不活跃</span>}
                            {p.is_learner && <span className="badge">learner</span>}
                          </div>
                        )
                      })
                    ) : (
                      <span className="panel-note">复制进度仅在本节点为该组 leader 时可见</span>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
        <div className="panel-body">
          <div className="panel-note">
            复制进度只在本节点是该组 leader 时可见——raft 的 tracker 只在 leader
            上维护复制位点，follower 视角拿不到 peer 数据，不是「集群里只有我一个」。
            <b>快照挂起（pending_snapshot 非零）</b>表示该 peer 正在被发快照，长期非零即
            「快照卡住」现场，应当排查该 peer 的拉取是否在前进。
          </div>
        </div>
      </section>
    </Shell>
  )
}
