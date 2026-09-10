import * as Dialog from '@radix-ui/react-dialog'
import { AlertIcon, XIcon } from './Icons'

export function ConfirmDialog({
  open,
  title,
  description,
  confirmLabel = '确认删除',
  busy = false,
  onConfirm,
  onOpenChange,
}: {
  open: boolean
  title: string
  description: string
  confirmLabel?: string
  busy?: boolean
  onConfirm: () => void
  onOpenChange: (open: boolean) => void
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="modal-backdrop" />
        <Dialog.Content className="confirm-dialog">
          <div className="confirm-dialog-icon" aria-hidden="true"><AlertIcon size={20} /></div>
          <div className="confirm-dialog-copy">
            <Dialog.Title>{title}</Dialog.Title>
            <Dialog.Description>{description}</Dialog.Description>
          </div>
          <Dialog.Close asChild>
            <button type="button" className="icon-btn confirm-dialog-close" aria-label="关闭确认弹窗">
              <XIcon size={16} />
            </button>
          </Dialog.Close>
          <div className="confirm-dialog-actions">
            <Dialog.Close asChild>
              <button type="button" className="secondary" disabled={busy}>取消</button>
            </Dialog.Close>
            <button type="button" className="danger-button" disabled={busy} onClick={onConfirm}>
              {busy ? '处理中…' : confirmLabel}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
