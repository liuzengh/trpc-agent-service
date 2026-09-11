/** Explicit HTTP transport limit; a backend may impose a smaller published limit. */
export const ARTIFACT_MAX_BYTES = 16 * 1024 * 1024;
export type ArtifactScope = { tenantId: string; deploymentId: string; revisionNumber: number; runId: string };
export type ArtifactReceipt = { name: string; version: number; ref: string; mime_type: string; size_bytes: number; sha256: string };
export function validArtifactName(name: string) {
  return name.trim().length > 0 && name !== "." && name !== ".." && !/[\/\\\u0000-\u001f\u007f]/.test(name);
}
function artifactPath(scope: ArtifactScope, name: string, version?: number) {
  if (!validArtifactName(name) || !scope.runId.trim() || !Number.isSafeInteger(scope.revisionNumber) || scope.revisionNumber < 1
    || (version !== undefined && (!Number.isSafeInteger(version) || version < 0))) throw new Error("请填写正式 Run ID、文件名及非负整数版本。");
  const query = new URLSearchParams({ run_id: scope.runId });
  if (version !== undefined) query.set("version", String(version));
  return `/api/control/v1/tenants/${encodeURIComponent(scope.tenantId)}/deployments/${encodeURIComponent(scope.deploymentId)}/revisions/${scope.revisionNumber}/artifacts/${encodeURIComponent(name)}?${query}`;
}
async function request<T>(url: string, write: boolean, init: RequestInit, consume: (response: Response) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  const caller = init.signal;
  const cancel = () => controller.abort();
  if (caller?.aborted) cancel(); else caller?.addEventListener("abort", cancel, { once: true });
  const timeout = setTimeout(cancel, 30_000);
  try {
    const response = await fetch(url, { ...init, credentials: "include", cache: "no-store", redirect: "error", signal: controller.signal });
    if (!response.ok) {
      await response.body?.cancel();
      if (write && response.status >= 500) throw new ArtifactResponseError(`HTTP ${response.status}：上传结果尚未确认，版本可能已生成；请先核对或读取，不会自动重试上传。`);
      const descriptions: Record<number, string> = { 401: "请重新登录", 403: "当前身份没有此 Artifact 的操作权限", 404: "正式 Run、固定发布或文件版本不存在", 409: "状态或固定来源不匹配", 413: "文件超过传输或后端限额", 503: "存储或授权依赖暂不可用" };
      throw new ArtifactResponseError(`HTTP ${response.status}：${descriptions[response.status] ?? "服务端未完成操作"}。${write ? "未自动重试。" : ""}`);
    }
    return await consume(response);
  } catch (error) {
    if (error instanceof ArtifactResponseError) throw error;
    throw new Error(write ? "上传结果尚未确认，版本可能已生成；请先核对或读取，不会自动重试上传。" : "读取未完成，请核对连接后重试。");
  } finally { clearTimeout(timeout); caller?.removeEventListener("abort", cancel); }
}
class ArtifactResponseError extends Error {}
export const artifactApi = {
  async upload(scope: ArtifactScope, name: string, file: Blob, signal?: AbortSignal): Promise<ArtifactReceipt> {
    const url = artifactPath(scope, name);
    if (file.size > ARTIFACT_MAX_BYTES) throw new Error("文件超过明确的 16 MiB HTTP 传输上限；后端还可能有更小限额。");
    return request(url, true, { method: "PUT", headers: { "Content-Type": file.type || "application/octet-stream" }, body: file, signal }, async (response) => {
      const value: unknown = await response.json();
      if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("invalid receipt");
      const item = value as Record<string, unknown>;
      if (Object.keys(item).length !== 6 || !Object.keys(item).every((key) => ["name", "version", "ref", "mime_type", "size_bytes", "sha256"].includes(key))
        || item.name !== name || !Number.isSafeInteger(item.version) || (item.version as number) < 0 || item.size_bytes !== file.size
        || typeof item.ref !== "string" || !item.ref || typeof item.mime_type !== "string" || !item.mime_type
        || typeof item.sha256 !== "string" || !/^(sha256:)?[a-f0-9]{64}$/.test(item.sha256)) throw new Error("invalid receipt");
      return item as ArtifactReceipt;
    });
  },
  async read(scope: ArtifactScope, name: string, version: number, signal?: AbortSignal) {
    return request(artifactPath(scope, name, version), false, { method: "GET", signal }, async (response) => ({ bytes: await response.arrayBuffer(), mimeType: response.headers.get("content-type") ?? "application/octet-stream" }));
  },
};
