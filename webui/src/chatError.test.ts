import { describe, expect, it } from 'vitest'

import { chatFailureMessage } from './chatError'

describe('chatFailureMessage', () => {
  it('preserves user-safe Chinese terminal messages', () => {
    expect(chatFailureMessage(new Error('模型服务暂时不可用，请稍后重试。'))).toBe('模型服务暂时不可用，请稍后重试。')
  })

  it('maps technical failures to concise Chinese feedback', () => {
    expect(chatFailureMessage(new Error('HTTP 503'))).toBe('服务暂时不可用，请稍后重试。')
    expect(chatFailureMessage(new Error('chat stream reconnect limit reached'))).toBe('响应超时，请稍后重试。')
    expect(chatFailureMessage(new Error('model provider failed'))).toBe('模型服务暂时不可用，请稍后重试。')
  })
})
