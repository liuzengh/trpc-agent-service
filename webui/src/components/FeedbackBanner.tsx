import type { ReactNode } from 'react'

export function FeedbackBanner({
  tone,
  children,
  className = '',
}: {
  tone: 'error' | 'success'
  children: ReactNode
  className?: string
}) {
  const role = tone === 'error' ? 'alert' : 'status'
  return (
    <div className={`${tone}-banner ${className}`.trim()} role={role}>
      {children}
    </div>
  )
}
