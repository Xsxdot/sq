/**
 * 页内反馈条
 *
 * 职责：
 *   - 在内容区顶部显示一条操作结果（成功 / 关注 / 失败）
 *
 * 边界：
 *   - 不自动消失：操作结果里常带 msgId 这类需要复制的信息，自动消失会来不及看
 */
export interface NoticeProps {
  kind: 'ok' | 'warn' | 'bad'
  children: React.ReactNode
  onClose?: () => void
}

export function Notice({ kind, children, onClose }: NoticeProps) {
  return (
    <div className={`notice ${kind}`}>
      <span>{children}</span>
      {onClose && <button className="btn" onClick={onClose} style={{ marginLeft: 'auto' }}>关闭</button>}
    </div>
  )
}