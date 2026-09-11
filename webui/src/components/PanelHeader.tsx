import type { ReactNode } from 'react'

export function PanelHeader({
  icon,
  title,
  description,
  actions,
  level = 2,
  className = '',
}: {
  icon?: ReactNode
  title: string
  description?: string
  actions?: ReactNode
  level?: 2 | 3
  className?: string
}) {
  const Heading = level === 3 ? 'h3' : 'h2'
  return (
    <div className={`panel-header ${className}`.trim()}>
      <div className="panel-header-main">
        {icon && <span className="panel-header-icon" aria-hidden="true">{icon}</span>}
        <div className="panel-header-copy">
          <Heading>{title}</Heading>
          {description && <p>{description}</p>}
        </div>
      </div>
      {actions && <div className="panel-header-actions">{actions}</div>}
    </div>
  )
}
