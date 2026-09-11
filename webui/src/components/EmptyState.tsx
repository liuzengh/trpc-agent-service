import type { ReactNode } from 'react'

export function EmptyState({
  icon,
  title,
  description,
}: {
  icon?: ReactNode
  title: string
  description: string
}) {
  return (
    <div className="data-empty-state">
      {icon && (
        <div className="data-empty-visual" aria-hidden="true">
          <span className="data-empty-halo" />
          <span className="data-empty-icon">{icon}</span>
        </div>
      )}
      <strong>{title}</strong>
      <p>{description}</p>
    </div>
  )
}
