import api from './index'

export interface ChatSendInput {
  tenant_id: string
  agent_id?: string
  session_id?: string
  user_id?: string
  text: string
}

export interface ChatSendResult {
  message_id: string
  session_id: string
  agent_id: string
}

export interface ChatMessageEvent {
  text: string
  message_id: string
  at: number
}

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'

export async function sendChat(input: ChatSendInput): Promise<ChatSendResult> {
  const { data } = await api.post<ChatSendResult>('/chat', input)
  return data
}

/** One decoded Server-Sent-Events frame. */
export interface SseFrame {
  id: string
  event: string
  data: string
}

/**
 * Splits a raw SSE buffer into complete frames and returns the trailing
 * incomplete remainder to keep buffering.
 *
 * Exported because this parser is the part of the reply path that must not
 * silently drop a message: a reply split across two network chunks is the
 * normal case, not an edge case. Comment lines (`: keep-alive`) carry no data
 * and are returned as empty frames for the caller to ignore.
 */
export function parseSSEFrames(buffer: string): { frames: SseFrame[]; rest: string } {
  const frames: SseFrame[] = []
  let rest = buffer
  for (;;) {
    const end = rest.indexOf('\n\n')
    if (end < 0) break
    const raw = rest.slice(0, end)
    rest = rest.slice(end + 2)
    const frame: SseFrame = { id: '', event: '', data: '' }
    const data: string[] = []
    for (const line of raw.split('\n')) {
      if (!line || line.startsWith(':')) continue
      const colon = line.indexOf(':')
      const field = colon < 0 ? line : line.slice(0, colon)
      const value = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '')
      if (field === 'id') frame.id = value
      else if (field === 'event') frame.event = value
      else if (field === 'data') data.push(value)
    }
    if (data.length) frame.data = data.join('\n')
    frames.push(frame)
  }
  return { frames, rest }
}

/** The stream was refused in a way that retrying cannot fix (401/403). */
export class FatalStreamError extends Error {}

export interface ChatStreamHandlers {
  /** Called once per successfully opened connection (reconnects fire again). */
  onOpen?: () => void
  onMessage: (ev: ChatMessageEvent) => void
  /** Called for a dropped connection or a fatal refusal. */
  onError?: (err: unknown) => void
}

export interface ChatStreamHandle {
  close(): void
}

/** Header mirror of the axios interceptor: the stream needs the same Bearer. */
function authHeaders(): Record<string, string> {
  const token = typeof localStorage !== 'undefined' ? localStorage.getItem('auth_token') : null
  return token ? { Authorization: `Bearer ${token}` } : {}
}

/** sleep that resolves immediately when the handle is closed. */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms)
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer)
        resolve()
      },
      { once: true },
    )
  })
}

/**
 * Opens the admin chat reply stream.
 *
 * This uses fetch + a streaming body rather than `EventSource` on purpose:
 * `EventSource` cannot attach an `Authorization` header, and the endpoint sits
 * behind the platform's JWT middleware — the browser's stream was rejected with
 * 401 and the chat page therefore never showed a reply. A Bearer-carrying fetch
 * keeps the auth model uniform (no token in a URL, where it would leak into
 * access logs, history and Referer).
 *
 * The connection is re-established with exponential backoff, resuming from the
 * last stream position we saw so a reply produced during a reconnect window is
 * still delivered. A 401/403 is fatal: retrying an expired session would spin
 * forever, so it is reported to the caller instead.
 */
export function openChatStream(sessionId: string, handlers: ChatStreamHandlers): ChatStreamHandle {
  const controller = new AbortController()
  let closed = false
  let cursor = '$'
  let delay = 500

  const connect = async (): Promise<void> => {
    const url =
      `${baseURL}/chat/stream?session_id=${encodeURIComponent(sessionId)}` +
      `&cursor=${encodeURIComponent(cursor)}`
    const res = await fetch(url, {
      headers: { Accept: 'text/event-stream', ...authHeaders() },
      signal: controller.signal,
    })
    if (res.status === 401 || res.status === 403) {
      throw new FatalStreamError(`chat stream rejected: ${res.status}`)
    }
    if (!res.ok || !res.body) {
      throw new Error(`chat stream failed: ${res.status}`)
    }
    handlers.onOpen?.()
    delay = 500 // a working connection resets the backoff

    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    for (;;) {
      const { value, done } = await reader.read()
      if (done) return
      buffer += decoder.decode(value, { stream: true })
      const { frames, rest } = parseSSEFrames(buffer)
      buffer = rest
      for (const frame of frames) {
        if (frame.id) cursor = frame.id
        if (frame.event !== 'message' || !frame.data) continue
        try {
          handlers.onMessage(JSON.parse(frame.data) as ChatMessageEvent)
        } catch {
          /* ignore a malformed frame rather than tearing the stream down */
        }
      }
    }
  }

  const loop = async (): Promise<void> => {
    while (!closed) {
      try {
        await connect()
      } catch (err) {
        if (closed) return
        handlers.onError?.(err)
        if (err instanceof FatalStreamError) return
      }
      if (closed) return
      await sleep(delay, controller.signal)
      delay = Math.min(delay * 2, 10_000)
    }
  }

  void loop()
  return {
    close() {
      closed = true
      controller.abort()
    },
  }
}
