// Console page: conversation list + live chat panel backed by POST /v1/chat.
import { useEffect, useMemo, useRef, useState } from 'react';
import { Link, useOutletContext } from 'react-router-dom';
import { sendChat } from '../api/chat';
import { useAuthStore } from '../stores/auth';
import { newId, useChatStore } from '../stores/chat';
import type { ChatMessage, Conversation } from '../types';
import { formatCost, formatRelativeTime, formatTokens } from '../utils/format';
import '../styles/console.css';

interface OutletContext {
  sidebarOpen: boolean;
  closeSidebar: () => void;
}

type Filter = 'all' | 'unread';
type SortKey = 'recent' | 'unread' | 'name';

const AVATAR_COLORS = [
  'hsl(210, 62%, 55%)',
  'hsl(160, 50%, 45%)',
  'hsl(270, 58%, 58%)',
  'hsl(35, 68%, 55%)',
  'hsl(0, 62%, 55%)',
  'hsl(190, 58%, 48%)',
];

function avatarColor(id: string): string {
  let hash = 0;
  for (let i = 0; i < id.length; i++) hash = (hash * 31 + id.charCodeAt(i)) | 0;
  return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length];
}

function conversationPreview(conv: Conversation): string {
  const last = conv.messages[conv.messages.length - 1];
  if (!last) return '暂无消息';
  const prefix = last.role === 'user' ? '我：' : 'A：';
  return prefix + last.text.slice(0, 40);
}

function conversationUnread(conv: Conversation): number {
  const lastReadAt = conv.lastReadAt ?? 0;
  return conv.messages.filter((message) => message.role === 'agent' && message.createdAt > lastReadAt).length;
}

export default function ConsolePage() {
  const { sidebarOpen, closeSidebar } = useOutletContext<OutletContext>();
  const tenant = useAuthStore((s) => s.tenant);
  const userId = useAuthStore((s) => s.userId);

  const conversations = useChatStore((s) => s.conversations);
  const activeId = useChatStore((s) => s.activeId);
  const select = useChatStore((s) => s.select);
  const createConversation = useChatStore((s) => s.createConversation);
  const appendMessage = useChatStore((s) => s.appendMessage);
  const clearConversation = useChatStore((s) => s.clearConversation);
  const renameConversation = useChatStore((s) => s.renameConversation);
  const setConversationScope = useChatStore((s) => s.setConversationScope);

  const [filter, setFilter] = useState<Filter>('all');
  const [sortKey, setSortKey] = useState<SortKey>('recent');
  const [search, setSearch] = useState('');
  const [scope, setScope] = useState<'direct' | 'group'>('direct');
  const [draft, setDraft] = useState('');
  const [sendingConversationId, setSendingConversationId] = useState<string | null>(null);
  const [renaming, setRenaming] = useState(false);
  const [renameDraft, setRenameDraft] = useState('');
  const [sendError, setSendError] = useState<{ conversationId: string; message: string } | null>(null);

  const messagesRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const active = useMemo(
    () => conversations.find((c) => c.id === activeId) ?? null,
    [conversations, activeId],
  );
  const sending = sendingConversationId !== null;

  const visibleConversations = useMemo(() => {
    let list = conversations;
    if (filter === 'unread') list = list.filter((c) => conversationUnread(c) > 0);
    const q = search.trim();
    if (q) {
      list = list.filter(
        (c) => c.title.includes(q) || c.messages.some((m) => m.text.includes(q)),
      );
    }
    const sorted = [...list];
    if (sortKey === 'name') sorted.sort((a, b) => a.title.localeCompare(b.title, 'zh-CN'));
    else if (sortKey === 'unread') {
      sorted.sort((a, b) => conversationUnread(b) - conversationUnread(a) || b.updatedAt - a.updatedAt);
    } else sorted.sort((a, b) => b.updatedAt - a.updatedAt);
    return sorted;
  }, [conversations, filter, search, sortKey]);

  const totalUnread = conversations.reduce((sum, c) => sum + conversationUnread(c), 0);

  useEffect(() => {
    const el = messagesRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [active?.messages.length, sending]);

  useEffect(() => {
    const ta = textareaRef.current;
    if (!ta) return;
    ta.style.height = 'auto';
    ta.style.height = Math.min(ta.scrollHeight, 120) + 'px';
  }, [draft]);

  useEffect(() => {
    if (active) setScope(active.scope);
  }, [active?.id, active?.scope]);

  if (!tenant) return null;
  const tenantId = tenant.tenant_id;

  function handleNewConversation() {
    const n = conversations.length + 1;
    createConversation(`新会话 ${n}`, scope);
    closeSidebar();
  }

  async function handleSend() {
    const text = draft.trim();
    if (!text || !active || sending) return;
    const conversationId = active.id;
    setSendError(null);
    setSendingConversationId(conversationId);
    setDraft('');

    const userMessage: ChatMessage = { id: newId('msg'), role: 'user', text, createdAt: Date.now() };
    const request = {
      message_id: userMessage.id,
      user_id: userId || 'console-user',
      conversation_id: active.id,
      scope: active.scope,
      text,
    };
    userMessage.request = request;
    appendMessage(conversationId, userMessage);

    try {
      const result = await sendChat(tenantId, request);
      appendMessage(conversationId, {
        id: newId('msg'),
        role: 'agent',
        text: result.text,
        createdAt: Date.now(),
        result,
        request,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setSendError({ conversationId, message: `发送失败：${message}` });
      appendMessage(conversationId, {
        id: newId('msg'),
        role: 'agent',
        text: `⚠ 请求失败：${message}`,
        createdAt: Date.now(),
        error: message,
      });
    } finally {
      setSendingConversationId(null);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      void handleSend();
    }
  }

  function startRename() {
    if (!active) return;
    setRenameDraft(active.title);
    setRenaming(true);
  }

  function commitRename() {
    if (active && renameDraft.trim()) renameConversation(active.id, renameDraft.trim());
    setRenaming(false);
  }

  return (
    <>
      <aside
        className={sidebarOpen ? 'im-sidebar open' : 'im-sidebar'}
        id="imSidebar"
      >
        <div className="im-sidebar__top">
          <button className="btn btn-primary btn-md im-new-conv" type="button" onClick={handleNewConversation}>
            <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
              <path d="M8 3v10M3 8h10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
            新会话
          </button>
          <input
            className="ui-input input-md im-search-input"
            type="text"
            placeholder="搜索会话名称或消息…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <div className="im-sidebar__toolbar">
            <div className="othertabs">
              <button
                className={filter === 'all' ? 'othertabs-item active' : 'othertabs-item'}
                type="button"
                onClick={() => setFilter('all')}
              >
                全部
              </button>
              <button
                className={filter === 'unread' ? 'othertabs-item active' : 'othertabs-item'}
                type="button"
                onClick={() => setFilter('unread')}
              >
                未读
              </button>
            </div>
            <select
              className="im-sort-select-native"
              value={sortKey}
              onChange={(e) => setSortKey(e.target.value as SortKey)}
            >
              <option value="recent">按最近消息</option>
              <option value="unread">按未读消息</option>
              <option value="name">按名称</option>
            </select>
          </div>
        </div>

        <div className="im-conv-list">
          {visibleConversations.map((conv) => {
            const unread = conversationUnread(conv);
            return (
              <div
                key={conv.id}
                className={conv.id === activeId ? 'im-conv-item active' : 'im-conv-item'}
                data-unread={unread}
                onClick={() => {
                  select(conv.id);
                  closeSidebar();
                }}
              >
                <div className="im-conv-avatar" style={{ background: avatarColor(conv.id) }}>
                  {conv.title.slice(0, 1)}
                </div>
                <div className="im-conv-body">
                  <div className="im-conv-row1">
                    <span className="im-conv-name">{conv.title}</span>
                    <span className="im-conv-time">{formatRelativeTime(conv.updatedAt)}</span>
                  </div>
                  <div className="im-conv-row2">
                    <span className="im-conv-preview">{conversationPreview(conv)}</span>
                    {unread > 0 && <span className="im-unread">{unread}</span>}
                  </div>
                </div>
              </div>
            );
          })}
          {visibleConversations.length === 0 && (
            <div className="im-conv-empty">暂无会话，点击「新会话」开始</div>
          )}
        </div>

        <div className="im-sidebar__footer">
          <span>
            {conversations.length} 个会话 · {totalUnread} 条未读 · 用户 {userId || '-'}
          </span>
        </div>
      </aside>

      <section className="im-chat">
        {active ? (
          <>
            <div className="im-chat__header">
              <div className="im-chat__title">
                {renaming ? (
                  <input
                    className="ui-input input-md"
                    value={renameDraft}
                    autoFocus
                    onChange={(e) => setRenameDraft(e.target.value)}
                    onBlur={commitRename}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') commitRename();
                      if (e.key === 'Escape') setRenaming(false);
                    }}
                  />
                ) : (
                  <h2 onClick={startRename} title="点击重命名">
                    {active.title}
                  </h2>
                )}
                <span className="im-chat__meta">
                  conversation_id={active.id} · scope={active.scope === 'direct' ? '单聊' : '群聊'}
                </span>
              </div>
              <div className="im-chat__actions">
                <div className="othertabs">
                  <button
                    className={active.scope === 'direct' ? 'othertabs-item active' : 'othertabs-item'}
                    type="button"
                    onClick={() => {
                      setScope('direct');
                      setConversationScope(active.id, 'direct');
                    }}
                  >
                    单聊
                  </button>
                  <button
                    className={active.scope === 'group' ? 'othertabs-item active' : 'othertabs-item'}
                    type="button"
                    onClick={() => {
                      setScope('group');
                      setConversationScope(active.id, 'group');
                    }}
                  >
                    群聊
                  </button>
                </div>
                <button className="btn btn-text btn-sm" type="button" onClick={startRename}>
                  重命名
                </button>
                <button
                  className="btn btn-text btn-sm im-danger-ghost"
                  type="button"
                  onClick={() => {
                    if (window.confirm('确认清空该会话的所有消息？')) clearConversation(active.id);
                  }}
                >
                  清空
                </button>
              </div>
            </div>

            <div className="im-chat__messages" ref={messagesRef}>
              <div className="im-date-divider">今天</div>
              {active.messages.map((message) => (
                <MessageRow key={message.id} message={message} conversationId={active.id} />
              ))}
              {sendingConversationId === active.id && (
                <div className="im-typing">
                  <div className="im-msg-avatar im-msg-avatar--agent">A</div>
                  <div className="im-typing__dots">
                    <span className="im-typing__dot" />
                    <span className="im-typing__dot" />
                    <span className="im-typing__dot" />
                  </div>
                </div>
              )}
            </div>

            {sendError?.conversationId === active.id && (
              <div className="im-chat__error">{sendError.message}</div>
            )}

            <div className="im-chat__input">
              <textarea
                className="im-chat__textarea"
                ref={textareaRef}
                rows={1}
                placeholder="输入消息，Enter 发送，Shift+Enter 换行"
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={handleKeyDown}
                disabled={sending}
              />
              <button
                className="btn btn-primary btn-md"
                type="button"
                onClick={() => void handleSend()}
                disabled={sending || !draft.trim()}
              >
                <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
                  <path
                    d="M2 8l12-5-5 12-2-5-5-2z"
                    stroke="currentColor"
                    strokeWidth="1.3"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
                发送
              </button>
            </div>
          </>
        ) : (
          <div className="im-chat__empty">
            <p>暂无会话</p>
            <button className="btn btn-primary btn-md" type="button" onClick={handleNewConversation}>
              创建第一个会话
            </button>
          </div>
        )}
      </section>
    </>
  );
}

function MessageRow({ message }: { message: ChatMessage; conversationId: string }) {
  if (message.role === 'user') {
    return (
      <div className="im-msg im-msg--user">
        <div className="im-msg-avatar im-msg-avatar--user">我</div>
        <div className="im-msg-content">
          <div className="im-msg-bubble">{message.text}</div>
        </div>
      </div>
    );
  }
  const result = message.result;
  return (
    <div className="im-msg im-msg--agent">
      <div className="im-msg-avatar im-msg-avatar--agent">A</div>
      <div className="im-msg-content">
        <div className={message.error ? 'im-msg-bubble im-msg-bubble--error' : 'im-msg-bubble'}>
          {message.text}
        </div>
        {result && (
          <div className="im-msg-meta">
            <span className="im-chip">{result.latency_ms} ms</span>
            <span className="im-chip">
              {formatTokens(result.prompt_tokens + result.completion_tokens)} tok
            </span>
            <span className="im-chip">{formatCost(result.cost_usd)}</span>
            {result.cache_hit && <span className="im-chip im-chip--success">缓存命中</span>}
            {result.duplicate && <span className="im-chip">重复消息</span>}
            {message.error ? (
              <span className="im-chip im-chip--danger">失败</span>
            ) : (
              <Link className="im-chip im-chip--link" to={`/message/${message.id}`}>
                详情 / Trace
              </Link>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
