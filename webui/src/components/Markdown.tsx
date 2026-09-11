import { useMemo } from 'react'
import DOMPurify from 'dompurify'
import { marked } from 'marked'

const SAFE_LINK_PROTOCOLS = new Set(['http:', 'https:', 'mailto:'])

function escapeAttribute(value: string) {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('"', '&quot;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
}

function escapeText(value: string) {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;')
}

function safeHref(href: string) {
  const trimmed = href.trim()
  if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('/') || trimmed.startsWith('./') || trimmed.startsWith('../')) return trimmed
  try {
    const url = new URL(trimmed)
    return SAFE_LINK_PROTOCOLS.has(url.protocol) ? trimmed : ''
  } catch {
    return ''
  }
}

marked.use({
  breaks: true,
  gfm: true,
  renderer: {
    link({ href, title, text }) {
      const safe = safeHref(href)
      if (!safe) return text
      const titleAttr = title ? ` title="${escapeAttribute(title)}"` : ''
      return `<a href="${escapeAttribute(safe)}"${titleAttr} target="_blank" rel="noopener noreferrer">${text}</a>`
    },
    html() {
      // 模型/会话内容不需要原始 HTML。直接丢弃，避免把 HTML 作为 Markdown 扩展面暴露出来。
      return ''
    },
  },
})

export function Markdown({ content, className = '' }: { content: string; className?: string }) {
  const html = useMemo(() => {
    if (!content) return ''
    try {
      const rendered = marked.parse(content, { async: false }) as string
      return typeof window === 'undefined' ? rendered : DOMPurify.sanitize(rendered)
    } catch {
      return escapeText(content)
    }
  }, [content])

  return (
    <div
      className={`markdown-content ${className}`.trim()}
      // biome-ignore lint/security/noDangerouslySetInnerHtml: marked raw HTML is disabled and browser output is sanitized with DOMPurify; parse failures are escaped as text.
      dangerouslySetInnerHTML={{ __html: html }}
    />
  )
}
