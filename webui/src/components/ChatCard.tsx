import type { ReactNode } from 'react'
import type { InteractiveCard } from '../types'

export function ChatCard({ card, body }: { card: InteractiveCard; body?: ReactNode }) {
  const actions = (card.actions ?? []).flatMap((action) => {
    const href = safeHTTPSURL(action.url)
    return href ? [{ ...action, href }] : []
  })
  return (
    <section className={`chat-card${card.state ? ` is-${card.state}` : ''}`} aria-label={card.title || '回答卡片'}>
      {card.title && <h4 className="chat-card-title">{card.title}</h4>}
      <div className="chat-card-body">{body ?? card.body}</div>
      {actions.length > 0 && (
        <div className="chat-card-actions">
          {actions.map((action) => (
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
