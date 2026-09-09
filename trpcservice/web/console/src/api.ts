import type { Principal, Validation } from "./types";

let csrf = "";
const active = new Set<AbortController>();
export class APIError extends Error {
  constructor(
    message: string,
    public status: number,
    public validation?: Validation,
    public code?: string,
  ) {
    super(message);
  }
}
export function cancelRequests() {
  for (const controller of active) controller.abort();
  active.clear();
}
export async function api<T>(
  path: string,
  data?: unknown,
  method = "POST",
): Promise<T> {
  const controller = new AbortController();
  active.add(controller);
  const timer = setTimeout(() => controller.abort(), 25_000);
  try {
    const response = await fetch("/admin/" + path, {
      method,
      credentials: "same-origin",
      cache: "no-store",
      redirect: "error",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        ...(csrf ? { "X-CSRF-Token": csrf } : {}),
      },
      body: method === "GET" ? undefined : JSON.stringify(data ?? {}),
    });
    const text = await response.text();
    if (text.length > 8 * 1024 * 1024)
      throw new APIError("响应过大，请缩小查询范围", 413);
    let result;
    try {
      result = JSON.parse(text);
    } catch {
      throw new APIError("服务未返回有效数据，请检查服务状态", response.status);
    }
    if (!response.ok) {
      if (response.status === 401 && path !== "login") {
        csrf = "";
        window.dispatchEvent(new Event("session-expired"));
      }
      throw new APIError(
        result.error || `请求失败（${response.status}）`,
        response.status,
        result.validation,
        result.code,
      );
    }
    return result as T;
  } finally {
    clearTimeout(timer);
    active.delete(controller);
  }
}
export async function restoreSession() {
  const p = await api<Principal>("session", undefined, "GET");
  csrf = p.csrf_token;
  return p;
}
export async function login(token: string) {
  const p = await api<Principal>("login", { token });
  csrf = p.csrf_token;
  return p;
}
export async function logout() {
  await api("logout");
  csrf = "";
  cancelRequests();
}
export function errorText(error: unknown) {
  return error instanceof DOMException && error.name === "AbortError"
    ? "请求已取消或超时。写操作请刷新核对结果，不要直接重复提交。"
    : error instanceof Error
      ? error.message
      : "操作未完成，请稍后检查";
}

export async function streamState(
  data: unknown,
  signal: AbortSignal,
  onState: (value: any, cursor: string) => void,
) {
  const response = await fetch("/admin/debug/events", {
    method: "POST",
    credentials: "same-origin",
    cache: "no-store",
    signal,
    headers: { "Content-Type": "application/json", "X-CSRF-Token": csrf },
    body: JSON.stringify(data),
  });
  if (!response.ok) {
    if (response.status === 401)
      window.dispatchEvent(new Event("session-expired"));
    throw new APIError(
      "调试状态连接未完成，请检查服务或登录状态",
      response.status,
    );
  }
  if (!response.body) throw new Error("浏览器不支持响应流");
  const reader = response.body.getReader(),
    decoder = new TextDecoder();
  let buffer = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      if (buffer.length > 2 * 1024 * 1024)
        throw new Error("状态响应超过大小限制");
      let end;
      while ((end = buffer.indexOf("\n\n")) >= 0) {
        const event = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        const lines = event.split("\n");
        const payload = lines.find((line) => line.startsWith("data: "));
        if (payload) {
          const cursor =
            lines.find((line) => line.startsWith("id: "))?.slice(4) || "";
          onState(JSON.parse(payload.slice(6)), cursor);
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}
