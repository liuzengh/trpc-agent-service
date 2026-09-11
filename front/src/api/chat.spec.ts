import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FatalStreamError, openChatStream, parseSSEFrames, type ChatMessageEvent } from './chat'

describe('parseSSEFrames', () => {
  it('decodes a complete frame and keeps the remainder buffered', () => {
    const { frames, rest } = parseSSEFrames('id: 7-0\nevent: message\ndata: {"text":"hi"}\n\ndata: {"text":"par')
    expect(frames).toHaveLength(1)
    expect(frames[0]).toEqual({ id: '7-0', event: 'message', data: '{"text":"hi"}' })
    expect(rest).toBe('data: {"text":"par')
  })

  // A reply arriving in two chunks is the normal case on a slow model, so a
  // parser that only handled whole frames would drop half the answers.
  it('reassembles a frame split across two chunks', () => {
    const first = parseSSEFrames('event: message\ndata: {"te')
    expect(first.frames).toHaveLength(0)
    const second = parseSSEFrames(first.rest + 'xt":"hello"}\n\n')
    expect(second.frames).toHaveLength(1)
    expect(second.frames[0].data).toBe('{"text":"hello"}')
  })

  it('ignores keep-alive comments and joins multi-line data', () => {
    const { frames } = parseSSEFrames(': connected\n\nevent: message\ndata: one\ndata: two\n\n')
    expect(frames).toHaveLength(2)
    expect(frames[0].data).toBe('')
    expect(frames[1].data).toBe('one\ntwo')
  })
})

// The stream is the only path that renders a reply, so its transport details
// (Bearer header, resume cursor, reconnect policy) are pinned here.
describe('openChatStream', () => {
  beforeEach(() => {
    const values = new Map<string, string>([['auth_token', 'tok-1']])
    Object.defineProperty(globalThis, 'localStorage', {
      configurable: true,
      value: {
        getItem: (key: string) => values.get(key) ?? null,
        setItem: (key: string, value: string) => values.set(key, value),
        removeItem: (key: string) => values.delete(key),
        clear: () => values.clear(),
      },
    })
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
    const encoder = new TextEncoder()
    return new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
        controller.close()
      },
    })
  }

  it('sends the bearer token and reports parsed replies', async () => {
    const seen: ChatMessageEvent[] = []
    const calls: { url: string; auth?: string }[] = []
    vi.stubGlobal('fetch', async (url: string, init: RequestInit) => {
      calls.push({ url, auth: (init.headers as Record<string, string>).Authorization })
      return new Response(
        streamOf([': connected\n\n', 'id: 9-0\nevent: message\ndata: {"text":"你好","message_id":"m1","at":1}\n\n']),
        { status: 200, headers: { 'Content-Type': 'text/event-stream' } },
      )
    })

    const handle = openChatStream('s-1', { onMessage: (ev) => seen.push(ev) })
    await vi.waitFor(() => expect(seen).toHaveLength(1))
    handle.close()

    expect(seen[0].text).toBe('你好')
    expect(calls[0].url).toContain('/chat/stream?session_id=s-1&cursor=%24')
    // EventSource could not do this — that was the bug.
    expect(calls[0].auth).toBe('Bearer tok-1')
  })

  it('treats a 401 as fatal instead of retrying forever', async () => {
    const errors: unknown[] = []
    let calls = 0
    vi.stubGlobal('fetch', async () => {
      calls++
      return new Response(null, { status: 401 })
    })

    const handle = openChatStream('s-2', { onMessage: () => {}, onError: (e) => errors.push(e) })
    await vi.waitFor(() => expect(errors).toHaveLength(1))
    await new Promise((r) => setTimeout(r, 700))
    handle.close()

    expect(errors[0]).toBeInstanceOf(FatalStreamError)
    expect(calls).toBe(1)
  })
})
