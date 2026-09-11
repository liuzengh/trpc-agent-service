import type { ToolInfo } from '../types'

const BUILT_IN_TOOL_PRESENTATION: Record<string, { name: string; description: string }> = {
  duckduckgo_search: {
    name: '网络搜索',
    description: '搜索公开网页并返回结果摘要。',
  },
  'platform.present_card': {
    name: '结果卡片',
    description: '向用户展示简洁的结果卡片，可附带 HTTPS 链接。',
  },
}

function conciseChineseLabel(description: string) {
  const value = description.trim()
  if (!value || value.length > 14) return ''
  if (!/[\u3400-\u9fff]/u.test(value)) return ''
  if (/[，。！？；：,.!?;:]/u.test(value)) return ''
  return value
}

export function toolDisplayName(tool: ToolInfo | string) {
  const name = typeof tool === 'string' ? tool : tool.name
  const builtIn = BUILT_IN_TOOL_PRESENTATION[name]
  if (builtIn) return builtIn.name
  if (typeof tool !== 'string') {
    const descriptionLabel = conciseChineseLabel(tool.description ?? '')
    if (descriptionLabel) return descriptionLabel
  }
  return name
}

export function toolDisplayDescription(tool: ToolInfo) {
  const builtIn = BUILT_IN_TOOL_PRESENTATION[tool.name]
  if (builtIn) return builtIn.description
  const description = tool.description?.trim()
  if (!description) return '平台提供的可授权工具。'
  if (description === toolDisplayName(tool)) return '平台提供的可授权工具。'
  return description
}
