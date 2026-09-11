import type { ReactNode } from 'react'
import type { InteractiveCard } from '../types'

export function ChatCard({
  card,
  body,
  onAction,
}: {
  card: InteractiveCard
  body?: ReactNode
  onAction?: (actionID: string) => void
}) {
  const linkActions = (card.actions ?? []).flatMap((action) => {
    const href = safeHTTPSURL(action.url)
    return href ? [{ ...action, href }] : []
  })
  const interactiveActions = (card.actions ?? []).filter((action) => action.action_id && !action.url)
  const hasActions = linkActions.length > 0 || interactiveActions.length > 0
  return (
    <section className={`chat-card${card.state ? ` is-${card.state}` : ''}`} aria-label={card.title || '回答卡片'}>
      {card.title && <h4 className="chat-card-title">{card.title}</h4>}
      <div className="chat-card-body">{body ?? card.body}</div>
      {hasActions && (
        <div className="chat-card-actions">
          {linkActions.map((action) => (
            <a
              key={`${action.label}:${action.href}`}
              className={`chat-card-action${action.style === 'primary' ? ' is-primary' : ''}`}
              href={action.href}
              target="_blank"
              rel="noopener noreferrer"
            >
              {action.label}
            </a>
          ))}
          {interactiveActions.map((action) => (
            <button
              key={`${action.label}:${action.action_id}`}
              type="button"
              className={`chat-card-action${action.style === 'primary' ? ' is-primary' : ''}`}
              onClick={() => action.action_id && onAction?.(action.action_id)}
              disabled={!onAction || card.state !== 'pending'}
            >
              {action.label}
            </button>
          ))}
        </div>
      )}
    </section>
  )
}

function safeHTTPSURL(value?: string): string | null {
  if (!value) return null
  try {
    const parsed = new URL(value)
    return parsed.protocol === 'https:' ? parsed.href : null
  } catch {
    return null
  }
}
