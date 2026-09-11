export function chatFailureMessage(error: unknown): string {
  const message = error instanceof Error ? error.message.trim() : ''
  if (!message) return '请求处理失败，请稍后重试。'
  if (/[㐀-鿿]/u.test(message)) return message

  const normalized = message.toLowerCase()
  if (normalized.includes('model') || normalized.includes('provider')) {
    return '模型服务暂时不可用，请稍后重试。'
  }
  if (normalized.includes('429') || normalized.includes('rate limit')) {
    return '请求过于频繁，请稍后再试。'
  }
  if (normalized.includes('timed out') || normalized.includes('timeout') || normalized.includes('reconnect')) {
    return '响应超时，请稍后重试。'
  }
  if (normalized.includes('http 5') || normalized.includes('stream unavailable') || normalized.includes('network') || normalized.includes('fetch')) {
    return '服务暂时不可用，请稍后重试。'
  }
  return '请求处理失败，请稍后重试。'
}
