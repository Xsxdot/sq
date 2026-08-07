/**
 * 危险操作二次确认
 *
 * 职责：
 *   - 用原生 <dialog> 承载确认框，body 里写清后果与影响范围
 *
 * 边界：
 *   - 只负责「问一句」，实际动作由 onConfirm 回调执行
 *   - 用原生 <dialog> 而不是自造遮罩：焦点陷阱、Esc 关闭、
 *     背景 inert 都是浏览器已经做好的，自己实现只会做得更差
 */
import { useEffect, useRef } from 'react'

export interface ConfirmDialogProps {
  open: boolean
  title: string
  /** 确认按钮文案 */
  confirmText: string
  danger?: boolean
  busy?: boolean
  onCancel: () => void
  onConfirm: () => void
  children: React.ReactNode
}

export function ConfirmDialog({
  open, title, confirmText, danger, busy, onCancel, onConfirm, children,
}: ConfirmDialogProps) {
  const ref = useRef<HTMLDialogElement>(null)

  useEffect(() => {
    const d = ref.current
    if (!d) return
    if (open && !d.open) d.showModal()
    if (!open && d.open) d.close()
  }, [open])

  return (
    <dialog ref={ref} onCancel={e => { e.preventDefault(); onCancel() }}>
      <h3>{title}</h3>
      <div className="dialog-body">{children}</div>
      <div className="dialog-foot">
        <button className="btn" type="button" onClick={onCancel}>取消</button>
        <button className={danger ? 'btn danger' : 'btn primary'} type="button"
          disabled={busy} onClick={onConfirm}>
          {busy ? '执行中…' : confirmText}
        </button>
      </div>
    </dialog>
  )
}