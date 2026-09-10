import { describe, expect, it, vi } from 'vitest'
import { isFeishuQRMessageAccepted } from './useFeishuQRLogin'

describe('Feishu QR postMessage validation', () => {
  it('accepts a message only when both SDK validators approve it', () => {
    const instance = {
      matchOrigin: vi.fn(() => true),
      matchData: vi.fn(() => true),
    }
    const event = { origin: 'https://passport.feishu.cn', data: { tmp_code: 'tmp-1' } }

    expect(isFeishuQRMessageAccepted(instance, event)).toBe(true)
    expect(instance.matchOrigin).toHaveBeenCalledWith(event.origin)
    expect(instance.matchData).toHaveBeenCalledWith(event.data)
  })

  it('fails closed when SDK validators are absent or reject the message', () => {
    const event = { origin: 'https://attacker.example', data: { tmp_code: 'forged' } }

    expect(isFeishuQRMessageAccepted({}, event)).toBe(false)
    expect(isFeishuQRMessageAccepted({ matchOrigin: () => false, matchData: () => true }, event)).toBe(false)
    expect(isFeishuQRMessageAccepted({ matchOrigin: () => true, matchData: () => false }, event)).toBe(false)
  })
})
