import { createContext, useCallback, useContext, useMemo, useState } from 'react'
import { generateUUID } from './api'
import type { InteractiveCard } from './types'

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant' | 'error'
  content: string
  time: string
  attachments?: ChatAttachment[]
  card?: InteractiveCard
}

export interface ChatAttachment {
  name: string
  size?: number
  type?: string
  filename?: string
  version?: number
  mime_type?: string
}

export function visibleUserMessage(content: string, attachments?: ChatAttachment[]) {
  if (attachments?.length) return { content, attachments }
  const marker = '附件「'
  const first = content.indexOf(marker)
  if (first < 0 || (first > 0 && !content.slice(0, first).endsWith('\n\n'))) {
    return { content, attachments }
  }
  const visibleContent = content.slice(0, first).trim()
  let rest = content.slice(first)
  const restored: ChatAttachment[] = []
  while (rest.startsWith(marker)) {
    const suffix = '」内容：\n'
    const nameEndRelative = rest.slice(marker.length).indexOf(suffix)
    if (nameEndRelative < 0) break
    const nameEnd = marker.length + nameEndRelative
    const name = rest.slice(marker.length, nameEnd).trim()
    if (!name) break
    restored.push({ name })
    const bodyStart = nameEnd + suffix.length
    const next = rest.slice(bodyStart).indexOf(`\n\n${marker}`)
    if (next < 0) break
    rest = rest.slice(bodyStart + next + 2)
  }
  return restored.length > 0
    ? { content: visibleContent, attachments: restored }
    : { content, attachments }
}

export interface ChatThread {
  id: string
  conversationId: string
  sessionKey?: string
  preview?: string
  transcriptLoaded?: boolean
  createdAt: number
  updatedAt: number
  pending: boolean
  messages: ChatMessage[]
}

interface ChatWorkspaceState {
  threadsByApp: Record<string, ChatThread[]>
  activeByApp: Record<string, string>
}

interface ChatWorkspaceValue {
  threadsFor: (appKey: string) => ChatThread[]
  activeThreadFor: (appKey: string) => ChatThread | null
  createThread: (appKey: string) => string
  removeThread: (appKey: string, threadId: string) => void
  selectThread: (appKey: string, threadId: string) => void
  appendMessage: (appKey: string, threadId: string, message: ChatMessage) => void
  prependMessages: (appKey: string, threadId: string, messages: ChatMessage[]) => void
  updateMessage: (appKey: string, threadId: string, messageId: string, update: (message: ChatMessage) => ChatMessage) => void
  removeMessage: (appKey: string, threadId: string, messageId: string) => void
  setPending: (appKey: string, threadId: string, pending: boolean) => void
  replaceThreads: (appKey: string, threads: ChatThread[], activeId?: string) => void
  setThreadMessages: (appKey: string, threadId: string, messages: ChatMessage[]) => void
  setThreadSession: (appKey: string, threadId: string, sessionKey: string) => void
}

const ChatWorkspaceContext = createContext<ChatWorkspaceValue | null>(null)

export function conversationIdFromSessionKey(sessionKey: string): string {
  const parts = sessionKey.split('/')
  return parts.length >= 4 ? parts[parts.length - 1] : sessionKey
}

export function formatChatClock(value: string | number): string {
  const date = typeof value === 'number' ? new Date(value) : new Date(value)
  if (Number.isNaN(date.getTime())) return typeof value === 'string' ? value : ''
  return date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })
}

export function formatChatListTime(value: number): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return ''
  const now = new Date()
  const sameDay = date.getFullYear() === now.getFullYear() && date.getMonth() === now.getMonth() && date.getDate() === now.getDate()
  if (sameDay) return date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })
  return date.toLocaleDateString('zh-CN', { month: 'numeric', day: 'numeric' })
}

export function threadFromSession(session: { SessionKey: string; preview?: string; summary?: string; UpdatedAt: string }): ChatThread {
  const updatedAt = Date.parse(session.UpdatedAt)
  const stamp = Number.isNaN(updatedAt) ? Date.now() : updatedAt
  return {
    id: session.SessionKey,
    conversationId: conversationIdFromSessionKey(session.SessionKey),
    sessionKey: session.SessionKey,
    preview: session.preview?.trim() || session.summary?.trim() || '',
    createdAt: stamp,
    updatedAt: stamp,
    pending: false,
    messages: [],
  }
}

export function messagesFromTranscript(messages: Array<{ id: string; role: 'user' | 'assistant'; content: string; time: string; attachments?: ChatAttachment[]; card?: InteractiveCard }>): ChatMessage[] {
  return messages.map((message) => {
    const visible = message.role === 'user'
      ? visibleUserMessage(message.content, message.attachments)
      : { content: message.content, attachments: message.attachments }
    return {
      id: message.id,
      role: message.role,
      content: visible.content,
      time: formatChatClock(message.time),
      attachments: visible.attachments,
      card: message.card,
    }
  })
}

export function mergeChatThreads(local: ChatThread[], remote: ChatThread[]): ChatThread[] {
  const localByConversation = new Map(local.map((thread) => [thread.conversationId, thread]))
  const mergedRemote: ChatThread[] = []
  for (const remoteThread of remote) {
    const existing = localByConversation.get(remoteThread.conversationId)
    if (!existing) {
      mergedRemote.push(remoteThread)
      continue
    }
    localByConversation.delete(remoteThread.conversationId)
    mergedRemote.push({
      ...remoteThread,
      id: existing.id,
      pending: existing.pending,
      messages: existing.messages.length > 0 ? existing.messages : remoteThread.messages,
      preview: existing.preview || remoteThread.preview,
      transcriptLoaded: existing.transcriptLoaded || remoteThread.transcriptLoaded || existing.messages.length > 0,
      updatedAt: Math.max(existing.updatedAt, remoteThread.updatedAt),
    })
  }
  const unsaved = [...localByConversation.values()].filter((thread) => (
    thread.pending || thread.messages.length > 0 || !thread.sessionKey
  ))
  return [...unsaved, ...mergedRemote]
}

function newThread(): ChatThread {
  const id = generateUUID()
  const now = Date.now()
  return {
    id,
    conversationId: `web-${id.slice(0, 8)}`,
    createdAt: now,
    updatedAt: now,
    pending: false,
    messages: [],
  }
}

export function ChatWorkspaceProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<ChatWorkspaceState>({ threadsByApp: {}, activeByApp: {} })

  const createThread = useCallback((appKey: string) => {
    const thread = newThread()
    setState((current) => ({
      threadsByApp: {
        ...current.threadsByApp,
        [appKey]: [thread, ...(current.threadsByApp[appKey] ?? [])],
      },
      activeByApp: { ...current.activeByApp, [appKey]: thread.id },
    }))
    return thread.id
  }, [])

  const selectThread = useCallback((appKey: string, threadId: string) => {
    setState((current) => ({
      ...current,
      activeByApp: { ...current.activeByApp, [appKey]: threadId },
    }))
  }, [])

  const removeThread = useCallback((appKey: string, threadId: string) => {
    setState((current) => {
      const remaining = (current.threadsByApp[appKey] ?? []).filter((thread) => thread.id !== threadId)
      const currentActive = current.activeByApp[appKey]
      const nextActive = currentActive === threadId ? (remaining[0]?.id ?? '') : currentActive
      return {
        threadsByApp: { ...current.threadsByApp, [appKey]: remaining },
        activeByApp: { ...current.activeByApp, [appKey]: nextActive },
      }
    })
  }, [])

  const changeThread = useCallback((appKey: string, threadId: string, change: (thread: ChatThread) => ChatThread) => {
    setState((current) => ({
      ...current,
      threadsByApp: {
        ...current.threadsByApp,
        [appKey]: (current.threadsByApp[appKey] ?? []).map((thread) => thread.id === threadId ? change(thread) : thread),
      },
    }))
  }, [])

  const appendMessage = useCallback((appKey: string, threadId: string, message: ChatMessage) => {
    changeThread(appKey, threadId, (thread) => ({
      ...thread,
      messages: [...thread.messages, message],
      preview: thread.preview || (message.role === 'user' ? message.content.trim() : ''),
      updatedAt: Date.now(),
    }))
  }, [changeThread])

  const prependMessages = useCallback((appKey: string, threadId: string, messages: ChatMessage[]) => {
    changeThread(appKey, threadId, (thread) => {
      const existing = new Set(thread.messages.map((message) => message.id))
      const older = messages.filter((message) => !existing.has(message.id))
      return { ...thread, messages: [...older, ...thread.messages], transcriptLoaded: true }
    })
  }, [changeThread])

  const updateMessage = useCallback((appKey: string, threadId: string, messageId: string, update: (message: ChatMessage) => ChatMessage) => {
    changeThread(appKey, threadId, (thread) => ({
      ...thread,
      messages: thread.messages.map((message) => message.id === messageId ? update(message) : message),
      updatedAt: Date.now(),
    }))
  }, [changeThread])

  const removeMessage = useCallback((appKey: string, threadId: string, messageId: string) => {
    changeThread(appKey, threadId, (thread) => ({
      ...thread,
      messages: thread.messages.filter((message) => message.id !== messageId),
      updatedAt: Date.now(),
    }))
  }, [changeThread])

  const setPending = useCallback((appKey: string, threadId: string, pending: boolean) => {
    changeThread(appKey, threadId, (thread) => ({ ...thread, pending, updatedAt: Date.now() }))
  }, [changeThread])

  const replaceThreads = useCallback((appKey: string, threads: ChatThread[], activeId?: string) => {
    setState((current) => {
      const merged = mergeChatThreads(current.threadsByApp[appKey] ?? [], threads)
      const nextActive = activeId && merged.some((thread) => thread.id === activeId)
        ? activeId
        : (current.activeByApp[appKey] && merged.some((thread) => thread.id === current.activeByApp[appKey])
          ? current.activeByApp[appKey]
          : merged[0]?.id ?? '')
      return {
        threadsByApp: { ...current.threadsByApp, [appKey]: merged },
        activeByApp: { ...current.activeByApp, [appKey]: nextActive },
      }
    })
  }, [])

  const setThreadMessages = useCallback((appKey: string, threadId: string, messages: ChatMessage[]) => {
    changeThread(appKey, threadId, (thread) => ({ ...thread, messages, transcriptLoaded: true, updatedAt: Date.now() }))
  }, [changeThread])

  const setThreadSession = useCallback((appKey: string, threadId: string, sessionKey: string) => {
    changeThread(appKey, threadId, (thread) => ({ ...thread, sessionKey, updatedAt: Date.now() }))
  }, [changeThread])

  const value = useMemo<ChatWorkspaceValue>(() => ({
    threadsFor: (appKey) => state.threadsByApp[appKey] ?? [],
    activeThreadFor: (appKey) => {
      const threads = state.threadsByApp[appKey] ?? []
      const activeID = state.activeByApp[appKey]
      return threads.find((thread) => thread.id === activeID) ?? threads[0] ?? null
    },
    createThread,
    removeThread,
    selectThread,
    appendMessage,
    prependMessages,
    updateMessage,
    removeMessage,
    setPending,
    replaceThreads,
    setThreadMessages,
    setThreadSession,
  }), [appendMessage, createThread, prependMessages, removeMessage, removeThread, replaceThreads, selectThread, setPending, setThreadMessages, setThreadSession, state, updateMessage])

  return <ChatWorkspaceContext.Provider value={value}>{children}</ChatWorkspaceContext.Provider>
}

export function useChatWorkspace(): ChatWorkspaceValue {
  const value = useContext(ChatWorkspaceContext)
  if (!value) throw new Error('useChatWorkspace must be used inside ChatWorkspaceProvider')
  return value
}
