import type { ReactNode } from 'react'

export type StatusTone = 'success' | 'danger' | 'info' | 'neutral' | 'warning'

export function StatusIndicator({
  tone = 'neutral',
  appearance = 'text',
  dot = true,
  children,
  className = '',
}: {
  tone?: StatusTone
  appearance?: 'text' | 'pill'
  dot?: boolean
  children: ReactNode
  className?: string
}) {
  return (
    <span className={`status-indicator tone-${tone} appearance-${appearance} ${className}`.trim()}>
      {dot && <span className="status-indicator-dot" aria-hidden="true" />}
      {children}
    </span>
  )
}
