// Chat API: direct agent invocation through the admin gateway.
import { apiJSON } from './client';
import type { ChatRequest, ChatResult } from '../types';

export function sendChat(tenant: string, request: ChatRequest): Promise<ChatResult> {
  return apiJSON<ChatResult>(`/v1/chat/${encodeURIComponent(tenant)}`, {
    method: 'POST',
    body: JSON.stringify(request),
  });
}
