export const CHANNEL_OPTIONS = [
  { value: 'telegram', label: 'Telegram' },
  { value: 'wecom', label: '企业微信' },
  { value: 'feishu', label: '飞书' },
] as const

const CHANNEL_LABELS: Record<string, string> = {
  ...Object.fromEntries(CHANNEL_OPTIONS.map((entry) => [entry.value, entry.label])),
  web: '网页',
}

export function channelLabel(channel: string) {
  return CHANNEL_LABELS[channel] ?? channel
}

export function channelBindingPlaceholder(channel: string) {
  switch (channel) {
    case 'telegram': return '机器人账号标识，例如 support_bot'
    case 'wecom': return '企业微信机器人标识，例如 customer-service'
    case 'feishu': return '飞书机器人标识，例如 service-assistant'
    default: return '机器人账号标识'
  }
}

export function channelCredentialLabel(reference: string) {
  const name = reference.replace(/^env:/, '').trim()
  return name || reference
}
