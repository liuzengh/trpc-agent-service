/** Tenant-visible catalog only: never contains physical targets or credentials. */
export const BACKEND_ROLES = ["session", "memory", "artifact", "knowledge"] as const;
export type BackendRole = (typeof BACKEND_ROLES)[number];
export type BackendKind = "postgresql" | "redis" | "s3" | "qdrant";
export type RuntimeBackend = { id: string; revision: number; label: string; kind: BackendKind; roles: BackendRole[]; available: boolean };
export type BackendSelection = { backend_id: string; backend_revision: number };
export const BACKEND_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$/;
export class BackendDirectoryError extends Error {
  constructor(public readonly code: string, public readonly status = 0) { super(code); this.name = "BackendDirectoryError"; }
}
function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function supports(kind: string, role: string) {
  return (kind === "postgresql" || kind === "redis") ? role === "session" || role === "memory"
    : kind === "s3" ? role === "artifact" : kind === "qdrant" && role === "knowledge";
}
export function decodeBackendDirectory(value: unknown): RuntimeBackend[] {
  const invalid = () => { throw new BackendDirectoryError("INVALID_BACKEND_DIRECTORY"); };
  if (!record(value) || Object.keys(value).some((key) => key !== "items") || !Array.isArray(value.items)) return invalid();
  const ids = new Set<string>();
  return value.items.map((item: unknown) => {
    if (!record(item) || Object.keys(item).some((key) => !["id", "revision", "label", "kind", "roles", "available"].includes(key))
      || typeof item.id !== "string" || !BACKEND_ID_PATTERN.test(item.id) || ids.has(item.id)
      || typeof item.revision !== "number" || !Number.isSafeInteger(item.revision) || item.revision < 1
      || typeof item.label !== "string" || !item.label.length || typeof item.available !== "boolean"
      || typeof item.kind !== "string" || !["postgresql", "redis", "s3", "qdrant"].includes(item.kind)
      || !Array.isArray(item.roles) || !item.roles.length || new Set(item.roles).size !== item.roles.length
      || !item.roles.every((role) => typeof role === "string" && supports(item.kind as string, role))) return invalid();
    ids.add(item.id);
    return { id: item.id, revision: item.revision, label: item.label, kind: item.kind as BackendKind, roles: [...item.roles] as BackendRole[], available: item.available };
  });
}
export function selectableBackend(item: RuntimeBackend, role: BackendRole) { return item.available && item.roles.includes(role) && supports(item.kind, role); }
export async function listRuntimeBackends(tenant: string, signal?: AbortSignal): Promise<RuntimeBackend[]> {
  if (!tenant) throw new BackendDirectoryError("TENANT_REQUIRED");
  const controller = new AbortController();
  const abort = () => controller.abort();
  if (signal?.aborted) abort(); else signal?.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(abort, 10000);
  try {
    const response = await fetch(`/api/control/v1/tenants/${encodeURIComponent(tenant)}/runtime-backends`, { credentials: "include", cache: "no-store", signal: controller.signal });
    if (!response.ok) throw new BackendDirectoryError(response.status === 404 ? "BACKEND_DIRECTORY_NOT_FOUND" : response.status === 401 ? "UNAUTHENTICATED" : response.status === 403 ? "BACKEND_DIRECTORY_FORBIDDEN" : "BACKEND_DIRECTORY_UNAVAILABLE", response.status);
    let payload: unknown;
    try { payload = await response.json(); } catch { throw new BackendDirectoryError("INVALID_BACKEND_DIRECTORY"); }
    return decodeBackendDirectory(payload);
  } catch (error) {
    if (controller.signal.aborted) throw new BackendDirectoryError("BACKEND_DIRECTORY_ABORTED");
    if (error instanceof BackendDirectoryError) throw error;
    throw new BackendDirectoryError("BACKEND_DIRECTORY_UNAVAILABLE");
  } finally { clearTimeout(timeout); signal?.removeEventListener("abort", abort); }
}
export function backendDirectoryMessage(error: unknown): string {
  if (error instanceof BackendDirectoryError) {
    switch (error.code) {
      case "BACKEND_DIRECTORY_NOT_FOUND": return "后端目录暂不可用（HTTP 404），当前不能选择 managed 后端。";
      case "UNAUTHENTICATED": return "登录已失效，请重新登录后读取后端目录。";
      case "BACKEND_DIRECTORY_FORBIDDEN": return "当前身份不能读取后端目录，请检查工作区权限或首次改密状态。";
      case "INVALID_BACKEND_DIRECTORY": return "后端目录响应不符合契约，已停止选择；请联系平台管理员。";
      case "BACKEND_DIRECTORY_ABORTED": return "后端目录请求已取消或超时，请重试。";
    }
  }
  return "后端目录暂不可用，已保留当前绑定；不会自动切换到旧版资源。";
}
