/** Explicit transport limits, independent of the selected backend's smaller limit. */
export const KNOWLEDGE_MAX_TEXT_BYTES = 1024 * 1024;
export const KNOWLEDGE_MAX_REQUEST_BYTES = 2 * 1024 * 1024;
export type KnowledgeScope = { tenantId: string; deploymentId: string; revisionNumber: number; resource: string };
export function knowledgeInputError(name: string, text: string): string {
  const encoder = new TextEncoder();
  if (!name.trim() || name !== name.trim() || name === "." || name === ".." || /[\/\\\u0000-\u001f\u007f]/.test(name) || encoder.encode(name).length > 255) return "请填写不含路径或控制字符、最多 255 UTF-8 bytes 的文档名。";
  if (!text.trim() || text.includes("\0") || new TextDecoder().decode(encoder.encode(text)) !== text) return "请填写有效 UTF-8 纯文本，不能包含 NUL；不支持 PDF 或二进制文档。";
  if (encoder.encode(text).length > KNOWLEDGE_MAX_TEXT_BYTES) return "文本超过 1 MiB 上限；后端还可能有更小限额。";
  if (encoder.encode(JSON.stringify({ name, text })).length > KNOWLEDGE_MAX_REQUEST_BYTES) return "JSON 请求超过 2 MiB HTTP 上限。";
  return "";
}
const uncertain = "导入未确认完整完成，部分文本 chunks 可能已写入；请先核对检索结果，不会自动重试。";
class KnowledgeResponseError extends Error {}
export const knowledgeApi = {
  async importText(scope: KnowledgeScope, name: string, text: string, signal?: AbortSignal): Promise<{ documents: number }> {
    const error = knowledgeInputError(name, text);
    if (error) throw new Error(error);
    if (!scope.tenantId || !scope.deploymentId || !Number.isSafeInteger(scope.revisionNumber) || scope.revisionNumber < 1 || !/^[a-z][a-z0-9_-]{0,63}$/.test(scope.resource)) throw new Error("请选择本发布快照中明确配置的 Knowledge 资源。");
    const url = `/api/control/v1/tenants/${encodeURIComponent(scope.tenantId)}/deployments/${encodeURIComponent(scope.deploymentId)}/revisions/${scope.revisionNumber}/knowledge/${encodeURIComponent(scope.resource)}/import`;
    const controller = new AbortController();
    const cancel = () => controller.abort();
    if (signal?.aborted) cancel(); else signal?.addEventListener("abort", cancel, { once: true });
    const timeout = setTimeout(cancel, 30_000);
    try {
      const response = await fetch(url, { method: "POST", body: JSON.stringify({ name, text }), headers: { "content-type": "application/json" }, credentials: "include", cache: "no-store", redirect: "error", signal: controller.signal });
      if (!response.ok) { await response.body?.cancel(); throw new KnowledgeResponseError(`HTTP ${response.status}：${uncertain}`); }
      const value: unknown = await response.json();
      if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).length !== 1 || !("documents" in value) || !Number.isSafeInteger(value.documents) || (value.documents as number) < 0) throw new Error("invalid receipt");
      return { documents: value.documents as number };
    } catch (error) { if (error instanceof KnowledgeResponseError) throw error; throw new Error(uncertain); }
    finally { clearTimeout(timeout); signal?.removeEventListener("abort", cancel); }
  },
};
