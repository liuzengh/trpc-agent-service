// Chat state: conversations with their messages, persisted per tenant +
// simulated user so the console survives reloads without cross-user leakage.
// All data here originates from real API calls.
import { create } from 'zustand';
import type { ChatMessage, Conversation } from '../types';
import { useAuthStore } from './auth';

const STORAGE_KEY_PREFIX = 'im-console.chat.v2.';
const LEGACY_STORAGE_KEY_PREFIX = 'im-console.chat.v1.';

interface ChatState {
  conversations: Conversation[];
  activeId: string | null;
  load: (tenantId: string, userId: string) => void;
  select: (id: string) => void;
  createConversation: (title: string, scope: 'direct' | 'group') => string;
  appendMessage: (conversationId: string, message: ChatMessage) => void;
  clearConversation: (id: string) => void;
  renameConversation: (id: string, title: string) => void;
  setConversationScope: (id: string, scope: 'direct' | 'group') => void;
}

function storageKey(tenantId: string, userId: string) {
  return STORAGE_KEY_PREFIX + encodeURIComponent(tenantId) + '.' + encodeURIComponent(userId);
}

function persist(key: string, conversations: Conversation[]) {
  try {
    localStorage.setItem(key, JSON.stringify(conversations));
  } catch {
    /* storage full: keep the in-memory state */
  }
}

function newId(prefix: string) {
  const random = typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID().replace(/-/g, '')
    : Math.random().toString(36).slice(2, 18);
  return `${prefix}_${random}`;
}

function isConversation(value: unknown): value is Conversation {
  if (!value || typeof value !== 'object') return false;
  const conversation = value as Partial<Conversation>;
  return (
    typeof conversation.id === 'string' &&
    typeof conversation.title === 'string' &&
    (conversation.scope === 'direct' || conversation.scope === 'group') &&
    typeof conversation.createdAt === 'number' &&
    typeof conversation.updatedAt === 'number' &&
    Array.isArray(conversation.messages)
  );
}

function activeStorageKey(): string | null {
  const auth = useAuthStore.getState();
  if (!auth.tenant || !auth.userId) return null;
  return storageKey(auth.tenant.tenant_id, auth.userId);
}

export const useChatStore = create<ChatState>((set) => ({
  conversations: [],
  activeId: null,

  load: (tenantId, userId) => {
    let conversations: Conversation[] = [];
    const key = storageKey(tenantId, userId);
    try {
      let raw = localStorage.getItem(key);
      // Assign the old tenant-only history to the first user who opens it, then
      // remove the ambiguous key so it cannot leak into every simulated user.
      if (!raw) {
        const legacyKey = LEGACY_STORAGE_KEY_PREFIX + tenantId;
        raw = localStorage.getItem(legacyKey);
        if (raw) {
          localStorage.setItem(key, raw);
          localStorage.removeItem(legacyKey);
        }
      }
      if (raw) {
        const parsed = JSON.parse(raw) as unknown;
        if (Array.isArray(parsed)) conversations = parsed.filter(isConversation);
      }
    } catch {
      /* corrupted storage: start fresh */
    }
    const activeId = conversations[0]?.id ?? null;
    if (activeId) {
      const now = Date.now();
      conversations = conversations.map((conversation) =>
        conversation.id === activeId ? { ...conversation, lastReadAt: now } : conversation,
      );
      persist(key, conversations);
    }
    set({ conversations, activeId });
  },

  select: (id) => {
    const key = activeStorageKey();
    set((state) => {
      const now = Date.now();
      const conversations = state.conversations.map((conversation) =>
        conversation.id === id ? { ...conversation, lastReadAt: now } : conversation,
      );
      if (key) persist(key, conversations);
      return { activeId: id, conversations };
    });
  },

  createConversation: (title, scope) => {
    const id = newId('conv');
    const now = Date.now();
    const conversation: Conversation = {
      id,
      title,
      scope,
      createdAt: now,
      updatedAt: now,
      lastReadAt: now,
      messages: [],
    };
    const key = activeStorageKey();
    set((state) => {
      const conversations = [conversation, ...state.conversations];
      if (key) persist(key, conversations);
      return { conversations, activeId: id };
    });
    return id;
  },

  appendMessage: (conversationId, message) => {
    const key = activeStorageKey();
    set((state) => {
      const conversations = state.conversations.map((conv) => {
        if (conv.id !== conversationId) return conv;
        const isVisibleReply = message.role === 'agent' && state.activeId === conversationId;
        return {
          ...conv,
          messages: [...conv.messages, message],
          updatedAt: Date.now(),
          lastReadAt: isVisibleReply ? message.createdAt : conv.lastReadAt,
        };
      });
      // Keep most recently updated conversations on top.
      conversations.sort((a, b) => b.updatedAt - a.updatedAt);
      if (key) persist(key, conversations);
      return { conversations };
    });
  },

  clearConversation: (id) => {
    const key = activeStorageKey();
    set((state) => {
      const conversations = state.conversations.map((conv) =>
        conv.id === id
          ? { ...conv, messages: [], updatedAt: Date.now(), lastReadAt: Date.now() }
          : conv,
      );
      if (key) persist(key, conversations);
      return { conversations };
    });
  },

  renameConversation: (id, title) => {
    const key = activeStorageKey();
    set((state) => {
      const conversations = state.conversations.map((conv) =>
        conv.id === id ? { ...conv, title, updatedAt: Date.now() } : conv,
      );
      if (key) persist(key, conversations);
      return { conversations };
    });
  },

  setConversationScope: (id, scope) => {
    const key = activeStorageKey();
    set((state) => {
      const conversations = state.conversations.map((conversation) =>
        conversation.id === id ? { ...conversation, scope } : conversation,
      );
      if (key) persist(key, conversations);
      return { conversations };
    });
  },
}));

/** Finds a message across all conversations (used by the detail page route). */
export function findMessage(
  conversations: Conversation[],
  messageId: string,
): { conversation: Conversation; message: ChatMessage } | null {
  for (const conv of conversations) {
    const message = conv.messages.find((m) => m.id === messageId);
    if (message) return { conversation: conv, message };
  }
  return null;
}

export { newId };
