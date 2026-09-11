/** Deployment's closed public protocol. Never use Profile/Agent draft DTOs here. */
export type DeploymentInput = {
  schema_version: "v1";
  agent: { agent_id: string; version_number: number };
  profile: { profile_id: string; revision_number: number };
};
export type Deployment = {
  id: string; tenant_id: string; name: string; description: string;
  metadata_revision: number; latest_revision_number: number | null;
  created_by: string; created_at: string; updated_at: string;
};
export type DeploymentDiagnostic = {
  code: string; severity: "error" | "warning"; source: "input" | "agent" | "profile" | "platform";
  path: string; category: string | null; name: string | null; node_id: string | null; message: string;
};
export type DeploymentReport = {
  valid: boolean; compiler_version: string; platform_contract_digest: string; diagnostics: DeploymentDiagnostic[];
};
export type DeploymentRevisionSummary = {
  id: string; tenant_id: string; deployment_id: string; revision_number: number; schema_version: string;
  agent_id: string; agent_version_number: number; profile_id: string; profile_revision_number: number;
  input_digest: string; manifest_id: string; manifest_digest: string; published_by: string; published_at: string;
};
export type JSONValue = string | number | boolean | null | JSONValue[] | { [key: string]: JSONValue };
export type ManifestView = {
  schema_version: string; compiler_version: string; runtime_contract_version: string;
  platform_contract: { version: string; digest: string }; tenant_id: string;
  sources: { agent: { agent_id: string; version_number: number; digest: string }; profile: { profile_id: string; revision_number: number; digest: string } };
  agent_plan: { root: string; nodes: Record<string, {
    kind: string; model_resource?: string; tool_resources?: string[]; knowledge_resources?: string[]; callable_entries?: string[]; artifact?: { enabled: boolean; resource: string };
  }> };
  resources: Record<string, Record<string, JSONValue>>;
  resolved_requirements: Record<string, Record<string, string>>;
  storage_roles: Record<string, string>;
  execution: { backend: string; allowed_endpoint_hosts: string[]; max_run_seconds: number; max_tool_calls: number; max_output_tokens: number };
};
export type DeploymentRevision = DeploymentRevisionSummary & { input: DeploymentInput; manifest_view: ManifestView };
export type PublishDeploymentInput = { expected_latest_revision_number: number | null; input: DeploymentInput };
export type DeploymentPublication = { revision: DeploymentRevision; validation: DeploymentReport };
export type BackendMigrationInput = PublishDeploymentInput & { source_revision_number: number };
export type BackendMigrationPublication = { memory_scopes_copied: number; publication: DeploymentPublication };
export type DeploymentPage = { deployments: Deployment[]; offset: number; limit: number; total: number };
export type DeploymentRevisionPage = { revisions: DeploymentRevisionSummary[]; offset: number; limit: number; total: number };
export class DeploymentApiError extends Error {
  constructor(public readonly status: number, public readonly code: string, message: string, public readonly validation?: DeploymentReport) {
    super(message); this.name = "DeploymentApiError";
  }
}
export const DEPLOYMENT_TIMEOUT_MS = 10_000;
/** Also bounds existing owner-query clients; callers must ignore obsolete results. */
export async function deploymentRead<T>(operation: Promise<T>): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([operation, new Promise<never>((_, reject) => {
      timeout = setTimeout(() => reject(new DeploymentApiError(0, "REQUEST_TIMEOUT", "读取超过 10 秒，请重试。")), DEPLOYMENT_TIMEOUT_MS);
    })]);
  } finally { clearTimeout(timeout); }
}
async function request<T>(path: string, init: RequestInit = {}, timeoutMs = DEPLOYMENT_TIMEOUT_MS): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`/api/control${path}`, { ...init, credentials: "include", cache: "no-store", signal: controller.signal });
    const body = await response.json().catch(() => {
      throw new DeploymentApiError(response.status, "INVALID_RESPONSE", `服务端返回了非 JSON 响应（HTTP ${response.status}）。写操作结果待确认。`);
    });
    if (!response.ok) throw new DeploymentApiError(response.status, body?.error?.code ?? "HTTP_ERROR", body?.error?.message ?? `HTTP ${response.status}`, body?.validation);
    return body as T;
  } catch (error) {
    if (controller.signal.aborted) throw new DeploymentApiError(0, "REQUEST_TIMEOUT", "请求超过 10 秒，操作结果尚未确认。请保留本次请求并重试确认。");
    throw error;
  } finally { clearTimeout(timer); }
}
const path = (tenant: string, id?: string) => `/v1/tenants/${encodeURIComponent(tenant)}/deployments${id ? `/${encodeURIComponent(id)}` : ""}`;
const page = (offset: number, limit: number) => `?${new URLSearchParams({ offset: String(offset), limit: String(limit) })}`;
const json = (method: string, body: unknown, key?: string): RequestInit => ({ method, headers: { "Content-Type": "application/json", ...(key ? { "Idempotency-Key": key } : {}) }, body: JSON.stringify(body) });
export const deploymentApi = {
  create(tenant: string, input: { name: string; description: string }, key: string) { return request<{ deployment: Deployment }>(path(tenant), json("POST", input, key)); },
  list(tenant: string, offset = 0, limit = 20) { return request<DeploymentPage>(path(tenant) + page(offset, limit)); },
  get(tenant: string, id: string) { return request<Deployment>(path(tenant, id)); },
  update(tenant: string, id: string, input: { expected_metadata_revision: number; name?: string; description?: string }) { return request<Deployment>(path(tenant, id), json("PATCH", input)); },
  validate(tenant: string, id: string, input: DeploymentInput) { return request<DeploymentReport>(`${path(tenant, id)}/validate`, json("POST", input)); },
  publish(tenant: string, id: string, input: PublishDeploymentInput, key: string) { return request<DeploymentPublication>(`${path(tenant, id)}/revisions`, json("POST", input, key)); },
  migrateAndPublish(tenant: string, id: string, input: BackendMigrationInput, key: string) { return request<BackendMigrationPublication>(`${path(tenant, id)}/backend-migrations`, json("POST", input, key), 75_000); },
  listRevisions(tenant: string, id: string, offset = 0, limit = 20) { return request<DeploymentRevisionPage>(`${path(tenant, id)}/revisions${page(offset, limit)}`); },
  getRevision(tenant: string, id: string, n: number) { return request<DeploymentRevision>(`${path(tenant, id)}/revisions/${n}`); },
};
export function deploymentError(error: unknown): string {
  if (error instanceof DeploymentApiError) {
    const messages: Record<string, string> = {
      DEPLOYMENT_LATEST_REVISION_CONFLICT: "其他人已发布新版本。请读取最新历史并确认新的发布基线。",
      DEPLOYMENT_METADATA_REVISION_CONFLICT: "名称或描述已被更新。你的输入已保留，请对比当前信息后再次确认。",
      IDEMPOTENCY_CONFLICT: "本次请求标识已用于不同内容。请核实历史记录后再开始新的操作。",
      DEPENDENCY_UNAVAILABLE: "凭据检查依赖暂时异常，请重试；无需重新填写凭据。",
      BACKEND_MIGRATION_BUSY: "来源版本仍有运行中任务，请等待完成后重试迁移。",
      BACKEND_MIGRATION_INVALID: "来源和目标必须启用不同的 Memory 后端，且使用同一个 Agent。",
      BACKEND_MIGRATION_UNAVAILABLE: "迁移 Worker 暂时不可用，请使用同一请求标识重试。",
      BACKEND_MIGRATION_FAILED: "目标后端已有冲突数据或迁移回读校验失败；未发布新版本。",
    };
    if (messages[error.code]) return messages[error.code];
    if (error.status === 401) return "会话已过期，请重新登录；待发布选择已保留在当前浏览器。";
    if (error.status === 403) return "当前账户没有执行此操作的权限。发布部署版本需要租户 OWNER。";
    if (error.status === 404) return "部署或所选来源版本不存在，请核对租户与固定版本。";
    if (error.status === 422) return "发布校验未通过，请处理下方诊断后重新校验。";
    return error.message;
  }
  return error instanceof Error ? error.message : "请求失败，请重试。";
}
