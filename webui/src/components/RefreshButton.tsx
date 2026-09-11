import { RefreshIcon } from './PageIcons'

export function RefreshButton({
  onClick,
  disabled = false,
  loading = false,
  label = '刷新',
  className = '',
}: {
  onClick: () => void
  disabled?: boolean
  loading?: boolean
  label?: string
  className?: string
}) {
  return (
    <button
      type="button"
      className={`icon-btn refresh-button${loading ? ' is-loading' : ''}${className ? ` ${className}` : ''}`}
      aria-label={label}
      title={label}
      disabled={disabled || loading}
      onClick={onClick}
    >
      <RefreshIcon size={15} />
    </button>
  )
}
