export function LoadingState({
  label = '正在加载…',
  compact = false,
}: {
  label?: string
  compact?: boolean
}) {
  return (
    <div className={`loading-state ${compact ? 'is-compact' : ''}`} role="status" aria-live="polite">
      <span className="loading-state-spinner" aria-hidden="true" />
      <span>{label}</span>
    </div>
  )
}
