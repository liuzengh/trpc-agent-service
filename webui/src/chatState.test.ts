import { describe, expect, it } from 'vitest'

import {
  conversationIdFromSessionKey,
  mergeChatThreads,
  messagesFromTranscript,
  threadFromSession,
  type ChatThread,
} from './chatState'

function thread(partial: Partial<ChatThread> & Pick<ChatThread, 'id' | 'conversationId'>): ChatThread {
  return {
    createdAt: 1,
    updatedAt: 1,
    pending: false,
    messages: [],
    ...partial,
  }
}

describe('web chat session restore', () => {
  it('reads the conversation id from the durable session key', () => {
    expect(conversationIdFromSessionKey('acme/support/web/conversation-1')).toBe('conversation-1')
  })

  it('maps a listed session into a thread without inventing messages', () => {
    const mapped = threadFromSession({
      SessionKey: 'acme/support/web/conversation-1',
      Preview: '订单到哪了',
      UpdatedAt: '2026-09-09T09:00:00Z',
    })
    expect(mapped.id).toBe('acme/support/web/conversation-1')
    expect(mapped.conversationId).toBe('conversation-1')
    expect(mapped.preview).toBe('订单到哪了')
    expect(mapped.messages).toEqual([])
  })

  it('keeps local in-progress threads and restores remote transcripts by conversation id', () => {
    const local = [
      thread({
        id: 'local-new',
        conversationId: 'web-new',
        messages: [],
      }),
      thread({
        id: 'local-live',
        conversationId: 'conversation-1',
        sessionKey: 'acme/support/web/conversation-1',
        pending: true,
        messages: [{ id: 'u1', role: 'user', content: '还在吗', time: '12:00' }],
      }),
    ]
    const remote = [
      threadFromSession({
        SessionKey: 'acme/support/web/conversation-1',
        Preview: '订单到哪了',
        UpdatedAt: '2026-09-09T09:00:00Z',
      }),
      threadFromSession({
        SessionKey: 'acme/support/web/conversation-2',
        Preview: '退款进度',
        UpdatedAt: '2026-09-08T09:00:00Z',
      }),
    ]
    const merged = mergeChatThreads(local, remote)
    expect(merged.map((item) => item.conversationId)).toEqual(['web-new', 'conversation-1', 'conversation-2'])
    expect(merged[1].id).toBe('local-live')
    expect(merged[1].pending).toBe(true)
    expect(merged[1].messages).toHaveLength(1)
    expect(merged[1].preview).toBe('订单到哪了')
  })

  it('projects only user and assistant transcript text', () => {
    expect(messagesFromTranscript([
      { id: 'u1', role: 'user', content: '你好', time: '2026-09-09T09:00:00Z' },
      { id: 'a1', role: 'assistant', content: '你好，我是助手', time: '2026-09-09T09:00:01Z' },
    ]).map((message) => ({ id: message.id, role: message.role, content: message.content }))).toEqual([
      { id: 'u1', role: 'user', content: '你好' },
      { id: 'a1', role: 'assistant', content: '你好，我是助手' },
    ])
  })
})
