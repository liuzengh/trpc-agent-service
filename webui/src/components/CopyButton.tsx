import { useEffect, useRef, useState } from 'react'

import { CheckCircleIcon } from './Icons'
import { CopyIcon } from './PageIcons'

export function CopyButton({
  value,
  label = '复制',
  copiedLabel = '已复制',
  className = 'secondary',
  iconOnly = false,
  iconSize = 14,
  onCopied,
  onError,
}: {
  value: string
  label?: string
  copiedLabel?: string
  className?: string
  iconOnly?: boolean
  iconSize?: number
  onCopied?: () => void
  onError?: () => void
}) {
  const [copied, setCopied] = useState(false)
  const resetTimer = useRef<number | null>(null)

  useEffect(() => () => {
    if (resetTimer.current !== null) window.clearTimeout(resetTimer.current)
  }, [])

  const copy = async () => {
    try {
      if (!navigator.clipboard) throw new Error('clipboard unavailable')
      await navigator.clipboard.writeText(value)
      setCopied(true)
      onCopied?.()
      if (resetTimer.current !== null) window.clearTimeout(resetTimer.current)
      resetTimer.current = window.setTimeout(() => setCopied(false), 1600)
    } catch {
      onError?.()
    }
  }

  const currentLabel = copied ? copiedLabel : label
  return (
    <button
      type="button"
      className={className}
      aria-label={currentLabel}
      title={currentLabel}
      onClick={() => void copy()}
    >
      {copied ? <CheckCircleIcon size={iconSize} /> : <CopyIcon size={iconSize} />}
      {!iconOnly && currentLabel}
    </button>
  )
}
