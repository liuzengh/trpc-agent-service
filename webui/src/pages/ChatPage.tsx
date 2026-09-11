import { lazy, memo, Suspense, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { useInfiniteQuery, useQuery } from '@tanstack/react-query'
import { deleteSession, generateUUID, getSessionMessages, listSessions, postChat } from '../api'
import { useAppContext } from '../context'
import {
  formatChatListTime,
  messagesFromTranscript,
  threadFromSession,
  visibleUserMessage,
  useChatWorkspace,
  type ChatMessage,
  type ChatThread,
} from '../chatState'
import type { ChatRequest } from '../types'
import { AlertIcon, BotIcon, ChevronDownIcon, FileTextIcon, PaperclipIcon, SendIcon, TrashIcon, XIcon } from '../components/Icons'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { ChatCard } from '../components/ChatCard'
import { Toast, type ToastTone } from '../components/Toast'
import { useDismissibleLayer } from '../hooks/useDismissibleLayer'

const Markdown = lazy(() => import('../components/Markdown').then((module) => ({ default: module.Markdown })))
const loadConfirmDialog = () => import('../components/ConfirmDialog')
const ConfirmDialog = lazy(() => loadConfirmDialog().then((module) => ({ default: module.ConfirmDialog })))
const MAX_CHAT_FILES = 4
const MAX_CHAT_FILE_BYTES = 16 * 1024 * 1024

export function ChatPage() {
  const { apps, tenant, activeAppKey, user } = useAppContext()
  const chat = useChatWorkspace()
  const { createThread, removeThread, selectThread, appendMessage, prependMessages, updateMessage, removeMessage, setPending, replaceThreads, setThreadMessages, setThreadSession } = chat
  const appliedTranscriptPages = useRef(new Map<string, number>())
  const activeApps = useMemo(
    () => apps.filter((app) => app.Config.tenant_id === tenant && app.Config.status === 'active'),
    [apps, tenant],
  )
  const selected = useMemo(
    () =>
      activeApps.find(
        (app) => `${app.Config.tenant_id}/${app.Config.app_code}` === activeAppKey || app.Config.app_code === activeAppKey,
      ) ?? activeApps[0],
    [activeApps, activeAppKey],
  )
  const tenantId = selected?.Config.tenant_id ?? ''
  const appCode = selected?.Config.app_code ?? ''
  const userId = user?.platform_user_id ?? ''
  const appKey = tenantId && appCode ? `${tenantId}/${appCode}` : ''
  const workspaceKey = appKey
  const hydrateKey = tenantId && appCode && userId ? `${tenantId}/${appCode}/${userId}` : ''
  const threads = workspaceKey ? chat.threadsFor(workspaceKey) : []
  const activeThread = workspaceKey ? chat.activeThreadFor(workspaceKey) : null
  const [sessionMenuOpen, setSessionMenuOpen] = useState(false)
  const sessionMenuTriggerRef = useRef<HTMLButtonElement>(null)
  const [threadToDelete, setThreadToDelete] = useState<ChatThread | null>(null)
  const [deletingThread, setDeletingThread] = useState(false)
  const [toast, setToast] = useState<{ message: string; tone: ToastTone } | null>(null)

  const [text, setText] = useState('')
  const [files, setFiles] = useState<File[]>([])
  const [fileError, setFileError] = useState('')
  const activeChatAbort = useRef(new Map<string, AbortController>())
  const logRef = useRef<HTMLDivElement>(null)
  const pendingScrollRestore = useRef<{ threadId: string; height: number; top: number } | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const fileIDsRef = useRef(new WeakMap<File, string>())
  const nextFileIDRef = useRef(0)
  const messages = activeThread?.messages ?? []
  const streaming = activeThread?.pending ?? false
  const sessionMenuRef = useDismissibleLayer<HTMLDivElement>({
    enabled: sessionMenuOpen,
    onDismiss: () => setSessionMenuOpen(false),
    restoreFocus: () => sessionMenuTriggerRef.current?.focus(),
  })

  const sessionsQuery = useQuery({
    queryKey: ['console', 'chat-sessions', tenantId, appCode, userId],
    queryFn: ({ signal }) => listSessions(tenantId, {
      app: appCode,
      channel: 'web',
      status: 'active',
    }, 'mine', signal),
    enabled: Boolean(hydrateKey),
    staleTime: 30_000,
  })
  const hydrated = !hydrateKey || sessionsQuery.isSuccess || sessionsQuery.isError
  const hydrateError = sessionsQuery.error instanceof Error ? sessionsQuery.error.message : ''

  useEffect(() => {
    if (!workspaceKey || !sessionsQuery.data) return
    replaceThreads(workspaceKey, sessionsQuery.data.map(threadFromSession))
  }, [replaceThreads, sessionsQuery.data, workspaceKey])

  useEffect(() => {
    if (!workspaceKey || !hydrated) return
    if (threads.length === 0 && (sessionsQuery.data?.length ?? 0) === 0) createThread(workspaceKey)
  }, [workspaceKey, createThread, hydrated, sessionsQuery.data?.length, threads.length])

  const activeSessionKey = activeThread?.sessionKey ?? ''
  const activeThreadId = activeThread?.id ?? ''
  const transcriptReady = Boolean(activeThread?.transcriptLoaded || (activeThread?.messages.length ?? 0) > 0 || activeThread?.pending)
  const transcriptQuery = useInfiniteQuery({
    queryKey: ['console', 'chat-transcript', tenantId, activeSessionKey],
    queryFn: ({ signal, pageParam }) => getSessionMessages(tenantId, activeSessionKey, { before: pageParam, limit: 50 }, signal),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.next_cursor || undefined,
    enabled: Boolean(tenantId && activeSessionKey && activeThreadId && !transcriptReady),
    staleTime: 30_000,
  })
  const transcriptError = transcriptQuery.error instanceof Error ? transcriptQuery.error.message : ''

  const loadEarlierTranscript = async () => {
    const log = logRef.current
    if (log) {
      pendingScrollRestore.current = { threadId: activeThreadId, height: log.scrollHeight, top: log.scrollTop }
    }
    const result = await transcriptQuery.fetchNextPage()
    if (result.isError) pendingScrollRestore.current = null
  }

  useEffect(() => {
    if (!activeThreadId || !activeSessionKey || !transcriptQuery.data?.pages.length) return
    const cacheKey = `${workspaceKey}:${activeThreadId}:${activeSessionKey}`
    const pages = transcriptQuery.data.pages
    let applied = appliedTranscriptPages.current.get(cacheKey) ?? 0
    if (applied === 0) {
      if (activeThread?.transcriptLoaded) {
        appliedTranscriptPages.current.set(cacheKey, pages.length)
        return
      }
      setThreadMessages(workspaceKey, activeThreadId, messagesFromTranscript(pages[0].messages))
      applied = 1
    }
    for (let index = applied; index < pages.length; index += 1) {
      prependMessages(workspaceKey, activeThreadId, messagesFromTranscript(pages[index].messages))
    }
    appliedTranscriptPages.current.set(cacheKey, pages.length)
  }, [activeSessionKey, activeThread?.transcriptLoaded, activeThreadId, prependMessages, setThreadMessages, transcriptQuery.data?.pages, workspaceKey])

  // Preserve the reader's viewport when older history is prepended; ordinary new messages still follow the conversation to the bottom.
  // biome-ignore lint/correctness/useExhaustiveDependencies: message identity/content changes are the intentional trigger for scroll anchoring even though the effect only reads the rendered scroll container.
  useLayoutEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      const log = logRef.current
      if (!log) return
      const pending = pendingScrollRestore.current
      if (pending?.threadId === activeThreadId) {
        log.scrollTop = pending.top + (log.scrollHeight - pending.height)
        pendingScrollRestore.current = null
        return
      }
      pendingScrollRestore.current = null
      log.scrollTo({ top: log.scrollHeight })
    })
    return () => window.cancelAnimationFrame(frame)
  }, [activeThreadId, messages])

  // biome-ignore lint/correctness/useExhaustiveDependencies: attachments are scoped to the active thread.
  useEffect(() => {
    setFiles([])
    setFileError('')
  }, [activeThread?.id])

  useEffect(() => {
    return () => {
      for (const controller of activeChatAbort.current.values()) controller.abort()
      activeChatAbort.current.clear()
    }
  }, [])

  const requestBody = useCallback((): ChatRequest | null => {
    if (!selected || !activeThread) return null
    return {
      tenant_id: selected.Config.tenant_id,
      app_code: selected.Config.app_code,
      conversation_id: activeThread.conversationId,
      text: text.trim(),
      request_id: generateUUID(),
    }
  }, [selected, activeThread, text])

  const stamp = () => new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })

  const runDirect = async () => {
    const body = requestBody()
    if (!body || !activeThread || streaming || (!body.text && files.length === 0)) return
    const threadId = activeThread.id
    const pendingFiles = files
    setText('')
    setFiles([])
    setFileError('')
    appendMessage(workspaceKey, threadId, {
      id: generateUUID(), role: 'user', content: body.text, time: stamp(),
      attachments: pendingFiles.map((file) => ({ name: file.name, size: file.size, type: file.type })),
    })
    const assistantId = generateUUID()
    appendMessage(workspaceKey, threadId, { id: assistantId, role: 'assistant', content: '', time: stamp() })
    setPending(workspaceKey, threadId, true)
    const controller = new AbortController()
    activeChatAbort.current.set(threadId, controller)
    const requestId = body.request_id ?? generateUUID()
    const request = { ...body, request_id: requestId }
    let streamBuffer = ''
    let streamTimer = 0
    const flushStream = () => {
      streamTimer = 0
      if (!streamBuffer) return
      const delta = streamBuffer
      streamBuffer = ''
      updateMessage(workspaceKey, threadId, assistantId, (message) => ({ ...message, content: message.content + delta }))
    }
    const queueStreamDelta = (delta: string) => {
      streamBuffer += delta
      if (!streamTimer) streamTimer = window.setTimeout(flushStream, 32)
    }
    const cancelBufferedStream = () => {
      if (streamTimer) window.clearTimeout(streamTimer)
      streamTimer = 0
      streamBuffer = ''
    }
    try {
      const result = await postChat(
        request,
        queueStreamDelta,
        controller.signal,
        pendingFiles,
      )
      if (streamTimer) window.clearTimeout(streamTimer)
      flushStream()
      updateMessage(workspaceKey, threadId, assistantId, (message) => ({ ...message, content: result.reply || message.content, card: result.card }))
      if (result.sessionKey) setThreadSession(workspaceKey, threadId, result.sessionKey)
    } catch (error) {
      cancelBufferedStream()
      removeMessage(workspaceKey, threadId, assistantId)
      if ((error as Error).name !== 'AbortError') {
        appendMessage(workspaceKey, threadId, { id: generateUUID(), role: 'error', content: (error as Error).message, time: stamp() })
      }
    } finally {
      if (streamTimer) window.clearTimeout(streamTimer)
      if (activeChatAbort.current.get(threadId) === controller) activeChatAbort.current.delete(threadId)
      setPending(workspaceKey, threadId, false)
    }
  }

  const startNewConversation = () => {
    if (!selected || !workspaceKey) return
    const blank = threads.find((thread) => !thread.sessionKey && thread.messages.length === 0 && !thread.pending)
    if (blank) selectThread(workspaceKey, blank.id)
    else createThread(workspaceKey)
    setSessionMenuOpen(false)
    setText('')
    setFiles([])
    setFileError('')
  }

  const confirmDeleteThread = async () => {
    if (!threadToDelete || !workspaceKey) return
    try {
      setDeletingThread(true)
      if (threadToDelete.sessionKey) {
        await deleteSession(tenantId, threadToDelete.sessionKey)
      }
      removeThread(workspaceKey, threadToDelete.id)
      void sessionsQuery.refetch()
      setThreadToDelete(null)
      setSessionMenuOpen(false)
      setToast({ message: '会话已删除', tone: 'success' })
    } catch (error) {
      setToast({ message: (error as Error).message, tone: 'error' })
    } finally {
      setDeletingThread(false)
    }
  }

  const addFiles = (selectedFiles: FileList | null) => {
    if (!selectedFiles) return
    const next = [...files, ...Array.from(selectedFiles)]
    if (next.length > MAX_CHAT_FILES) {
      setFileError(`最多添加 ${MAX_CHAT_FILES} 个文件`)
      return
    }
    if (next.reduce((total, file) => total + file.size, 0) > MAX_CHAT_FILE_BYTES) {
      setFileError('文件总大小不能超过 16 MB')
      return
    }
    setFiles(next)
    setFileError('')
  }

  const removeFile = (index: number) => {
    setFiles((current) => current.filter((_, fileIndex) => fileIndex !== index))
    setFileError('')
  }

  const fileKey = (file: File) => {
    const existing = fileIDsRef.current.get(file)
    if (existing) return existing
    nextFileIDRef.current += 1
    const id = `chat-file-${nextFileIDRef.current}`
    fileIDsRef.current.set(file, id)
    return id
  }

  const currentThreadLabel = activeThread ? threadLabel(activeThread, 1) : '新会话'

  return (
    <div className="page-stack">
      <div className="chat-workspace">
      <section className="thread">
        <header className="thread-head">
          <div className="thread-bot">
            <span className="avatar avatar-agent">
              <BotIcon size={14} />
            </span>
            <div>
              <div className="thread-bot-name">
                {selected ? selected.Config.app_code : '未选择机器人'}
                {selected && <span className="chat-availability">可用</span>}
              </div>
              <div className="thread-bot-meta">
                {selected ? `${selected.Config.model?.name || '未配置模型'} · v${selected.Config.config_version}` : ''}
              </div>
            </div>
          </div>
          <div className="thread-session" ref={sessionMenuRef}>
            <button
              ref={sessionMenuTriggerRef}
              type="button"
              className={`session-switch-btn ${sessionMenuOpen ? 'is-open' : ''}`}
              aria-haspopup="dialog"
              aria-expanded={sessionMenuOpen}
              disabled={!selected}
              onClick={() => {
                void loadConfirmDialog()
                setSessionMenuOpen((open) => !open)
              }}
            >
              <span className="session-switch-label">{currentThreadLabel}</span>
              <ChevronDownIcon size={14} />
            </button>
            <button type="button" className="text-button" aria-label="新建会话" onClick={startNewConversation} disabled={!selected}>新建</button>
            {sessionMenuOpen && (
              <div className="session-popover" role="dialog" aria-label="当前机器人会话">
                {hydrateError && <FeedbackBanner tone="error">{hydrateError}</FeedbackBanner>}
                {!hydrated && threads.length === 0 && <p className="hint">正在加载会话…</p>}
                <ThreadList
                  threads={threads}
                  activeThreadID={activeThread?.id}
                  onSelect={(thread) => {
                    selectThread(workspaceKey, thread.id)
                    setSessionMenuOpen(false)
                  }}
                  onDelete={setThreadToDelete}
                />
              </div>
            )}
          </div>
        </header>

        <div className="chat-log" ref={logRef}>
          {hydrateError && <FeedbackBanner tone="error">{hydrateError}</FeedbackBanner>}
          {transcriptError && <FeedbackBanner tone="error">{transcriptError}</FeedbackBanner>}
          {transcriptQuery.hasNextPage && (
            <button
              type="button"
              className="secondary chat-load-earlier"
              disabled={transcriptQuery.isFetchingNextPage}
              onClick={() => void loadEarlierTranscript()}
            >
              {transcriptQuery.isFetchingNextPage ? '正在加载…' : '加载更早消息'}
            </button>
          )}
          {messages.length === 0 && activeThread?.sessionKey && !activeThread.transcriptLoaded && (
            <p className="hint">正在加载对话…</p>
          )}
          {messages.length === 0 && !(activeThread?.sessionKey && !activeThread.transcriptLoaded) && (
            <div className="chat-empty">
              <div className="chat-empty-visual" aria-hidden="true">
                <span className="empty-bubble empty-bubble-primary"><i /><i /><i /></span>
                <span className="empty-bubble empty-bubble-secondary"><i /><i /></span>
                <span className="empty-sparkle empty-sparkle-large" />
                <span className="empty-sparkle empty-sparkle-small" />
              </div>
              <h2 className="chat-empty-title">{selected ? `与 ${selected.Config.app_code} 开始对话` : '请选择机器人'}</h2>
              <p className="chat-empty-subtitle">{selected ? '发送第一条消息开始对话。' : '请先选择一个机器人。'}</p>
            </div>
          )}
          {messages.map((message) => (
            <ChatMessageRow
              key={message.id}
              message={message}
              assistantName={selected?.Config.app_code ?? '机器人'}
              streaming={streaming}
            />
          ))}
        </div>

        <div className="composer-box">
          {files.length > 0 && (
            <ul className="composer-files" aria-label="待发送文件">
              {files.map((file, index) => (
                <li key={fileKey(file)} className="composer-file">
                  <FileTextIcon size={13} />
                  <span>{file.name}</span>
                  <button type="button" aria-label={`移除 ${file.name}`} onClick={() => removeFile(index)}><XIcon size={12} /></button>
                </li>
              ))}
            </ul>
          )}
          <div className="composer-row">
            <input
              ref={fileInputRef}
              type="file"
              multiple
              hidden
              onChange={(event) => {
                addFiles(event.target.files)
                event.target.value = ''
              }}
            />
            <button
              type="button"
              className="icon-btn composer-attach"
              aria-label="添加文件"
              title="添加文件"
              disabled={streaming || !selected}
              onClick={() => fileInputRef.current?.click()}
            >
              <PaperclipIcon size={17} />
            </button>
            <textarea
              rows={1}
              placeholder={selected ? '输入消息…' : '请先在左侧选择机器人'}
              value={text}
              disabled={streaming || !selected}
              onChange={(event) => setText(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter' && !event.shiftKey) {
                  event.preventDefault()
                  void runDirect()
                }
              }}
            />
            <button
              type="button"
              className="primary icon-btn send-btn"
              aria-label="发送"
              onClick={() => void runDirect()}
              disabled={streaming || !selected || (!text.trim() && files.length === 0)}
              title="发送"
            >
              <SendIcon size={16} />
            </button>
          </div>
        </div>
        {fileError && <p className="composer-error" role="alert">{fileError}</p>}
        <p className="composer-hint">Enter 发送 · Shift + Enter 换行</p>
      </section>
      </div>
      {toast && <Toast key={`${toast.tone}:${toast.message}`} message={toast.message} tone={toast.tone} onClose={() => setToast(null)} />}
      {threadToDelete && (
        <Suspense fallback={null}>
          <ConfirmDialog
            open
            title="删除会话？"
            description="删除后会从当前会话列表移除，不会影响其他会话。"
            confirmLabel="删除会话"
            busy={deletingThread}
            onConfirm={() => void confirmDeleteThread()}
            onOpenChange={(open) => { if (!open && !deletingThread) setThreadToDelete(null) }}
          />
        </Suspense>
      )}
    </div>
  )
}

const ChatMessageRow = memo(function ChatMessageRow({
  message,
  assistantName,
  streaming,
}: {
  message: ChatMessage
  assistantName: string
  streaming: boolean
}) {
  const visible = message.role === 'user'
    ? visibleUserMessage(message.content, message.attachments)
    : { content: message.content, attachments: message.attachments }
  return (
    <div className={`msg ${message.role}`}>
      {message.role === 'user' ? (
        <span className="avatar avatar-user">你</span>
      ) : message.role === 'assistant' ? (
        <span className="avatar avatar-agent"><BotIcon size={14} /></span>
      ) : (
        <span className="avatar avatar-error"><AlertIcon size={14} /></span>
      )}
      <div className="msg-body">
        <div className="msg-meta">
          <span className="msg-name">{message.role === 'user' ? '你' : message.role === 'assistant' ? assistantName : '执行错误'}</span>
          <span className="msg-time">{message.time}</span>
        </div>
        {visible.attachments && visible.attachments.length > 0 && (
          <div className="message-attachments">
            {visible.attachments.map((attachment) => (
              <span key={`${attachment.name}-${attachment.size}`} className="message-attachment">
                <FileTextIcon size={13} />
                <span>{attachment.name}</span>
              </span>
            ))}
          </div>
        )}
        {visible.content ? (
          <div className={`msg-text ${message.role === 'assistant' ? 'is-markdown' : ''}`}>
            {message.role === 'assistant' ? (
              <Suspense fallback={<span>{visible.content}</span>}>
                <Markdown content={visible.content} />
              </Suspense>
            ) : visible.content}
          </div>
        ) : message.role === 'assistant' && streaming ? (
          <div className="msg-text"><span className="thinking">正在思考…</span></div>
        ) : null}
        {message.role === 'assistant' && message.card && (
          <ChatCard
            card={message.card}
            body={(
              <Suspense fallback={<span>{message.card.body}</span>}>
                <Markdown content={message.card.body} />
              </Suspense>
            )}
          />
        )}
      </div>
    </div>
  )
})

function ThreadList({
  threads,
  activeThreadID,
  onSelect,
  onDelete,
}: {
  threads: ChatThread[]
  activeThreadID?: string
  onSelect: (thread: ChatThread) => void
  onDelete: (thread: ChatThread) => void
}) {
  return (
    <div className="chat-thread-list">
      {threads.map((thread, index) => (
        <div key={thread.id} className={`chat-thread-row ${activeThreadID === thread.id ? 'is-active' : ''}`}>
          <button
            type="button"
            data-chat-thread={thread.id}
            className="chat-thread-option"
            aria-pressed={activeThreadID === thread.id}
            onClick={() => onSelect(thread)}
          >
            <span className="chat-thread-name">{threadLabel(thread, index + 1)}</span>
            <span className="chat-thread-meta">{thread.messages.length > 0 ? `${thread.messages.length} 条消息 · ` : ''}{formatChatListTime(thread.updatedAt)}</span>
          </button>
          <button
            type="button"
            className="chat-thread-delete"
            aria-label={`删除会话 ${threadLabel(thread, index + 1)}`}
            title="删除会话"
            disabled={thread.pending}
            onClick={() => onDelete(thread)}
          >
            <TrashIcon size={13} />
          </button>
        </div>
      ))}
    </div>
  )
}

function threadLabel(thread: ChatThread, fallbackNumber: number) {
  const preview = thread.preview?.trim()
  if (preview) return truncateLabel(preview)
  const firstUserMessage = thread.messages.find((message) => message.role === 'user' && message.content.trim())?.content.trim()
  if (firstUserMessage) return truncateLabel(firstUserMessage)
  const firstAttachment = thread.messages.find((message) => message.role === 'user' && message.attachments?.length)?.attachments?.[0]
  return firstAttachment ? `附件 · ${firstAttachment.name}` : `新会话 ${fallbackNumber}`
}

function truncateLabel(value: string) {
  const characters = [...value]
  return characters.length > 18 ? `${characters.slice(0, 18).join('')}…` : value
}
