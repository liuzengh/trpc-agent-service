// Minimal fetch wrapper: attaches the admin Bearer token and normalizes errors.
import { useAuthStore } from '../stores/auth';

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public code?: string,
    public requestId?: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export async function apiFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const token = useAuthStore.getState().token;
  const headers = new Headers(init.headers);
  if (token) headers.set('Authorization', `Bearer ${token}`);
  if (init.body && !headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
  const res = await fetch(path, { ...init, headers });
  if (res.status === 401) {
    // Token rejected by the server: drop the session so the router bounces
    // back to the login page instead of looping failed requests.
    useAuthStore.getState().logout();
  }
  return res;
}

export async function apiJSON<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await apiFetch(path, init);
  if (!res.ok) {
    const raw = await res.text();
    let detail = raw.trim() || `HTTP ${res.status}`;
    let code: string | undefined;
    let requestId: string | undefined;
    if (raw) {
      try {
        const body = JSON.parse(raw) as { error?: string; code?: string; request_id?: string };
        detail = body.error ?? detail;
        code = body.code;
        requestId = body.request_id;
      } catch {
        /* Plain-text http.Error response: use the text above. */
      }
    }
    if (requestId) detail += `（请求 ID：${requestId}）`;
    throw new ApiError(res.status, detail, code, requestId);
  }
  return (await res.json()) as T;
}
