import * as Dialog from '@radix-ui/react-dialog'

import { XIcon } from './Icons'

export function DialogHeader({
  title,
  description,
  descriptionClassName,
  className = 'modal-head',
  closeClassName = '',
  closeLabel = '关闭弹窗',
}: {
  title: string
  description?: string
  descriptionClassName?: string
  className?: string
  closeClassName?: string
  closeLabel?: string
}) {
  return (
    <div className={className}>
      <div>
        <Dialog.Title>{title}</Dialog.Title>
        {description && <Dialog.Description className={descriptionClassName}>{description}</Dialog.Description>}
      </div>
      <Dialog.Close asChild>
        <button type="button" className={`icon-btn ${closeClassName}`.trim()} aria-label={closeLabel}>
          <XIcon size={16} />
        </button>
      </Dialog.Close>
    </div>
  )
}
