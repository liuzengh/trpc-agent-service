import axios from 'axios'

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
const client = axios.create({ baseURL })

export async function sendChat(input: ChatSendInput): Promise<ChatSendResult> {
  const { data } = await client.post<ChatSendResult>('/chat', input)
  return data
}

/**
 * Opens a Server-Sent-Events stream that forwards this session's admin
 * replies. cursor '$' means only messages arriving after the connection.
 * Returns a disposable EventSource; the caller owns the lifecycle.
 */
export function openChatStream(
  sessionId: string,
  onMessage: (ev: ChatMessageEvent) => void,
  onError: (err: Event) => void,
): EventSource {
  const url = `${baseURL}/chat/stream?session_id=${encodeURIComponent(sessionId)}&cursor=$`
  const source = new EventSource(url)
  source.addEventListener('message', (e) => {
    try {
      onMessage(JSON.parse((e as MessageEvent).data) as ChatMessageEvent)
    } catch {
      /* ignore malformed frames */
    }
  })
  source.addEventListener('error', (e) => onError(e))
  return source
}
