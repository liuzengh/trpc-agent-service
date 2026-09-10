import { useEffect, useRef } from 'react'
import { createPortal } from 'react-dom'
import { AlertIcon, CheckCircleIcon, XIcon } from './Icons'

export type ToastTone = 'success' | 'error' | 'warning'

export function Toast({
  message,
  tone = 'success',
  onClose,
  duration = 2800,
}: {
  message: string
  tone?: ToastTone
  onClose: () => void
  duration?: number
}) {
  const onCloseRef = useRef(onClose)

  useEffect(() => {
    onCloseRef.current = onClose
  }, [onClose])

  useEffect(() => {
    if (!message) return
    const timer = window.setTimeout(() => onCloseRef.current(), duration)
    return () => window.clearTimeout(timer)
  }, [duration, message])

  const icon = tone === 'success'
    ? <CheckCircleIcon size={18} />
    : <AlertIcon size={18} />

  return createPortal(
    <div className="toast-viewport" aria-live="polite" aria-atomic="true">
      <div className={`toast toast-${tone}`} role={tone === 'error' ? 'alert' : 'status'}>
        <span className="toast-icon" aria-hidden="true">{icon}</span>
        <span className="toast-message">{message}</span>
        <button type="button" className="toast-close" aria-label="关闭通知" onClick={onClose}>
          <XIcon size={14} />
        </button>
      </div>
    </div>,
    document.body,
  )
}
