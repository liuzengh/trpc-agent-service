export type TelegramEndpointProfile = "official" | "test";
/** Closed public Channel V1 DTOs; internal workload/credential-resolve APIs are not browser APIs. */
export type ChannelProvider = "telegram" | "wecom";
export type TelegramReceiveMode = "long_polling" | "webhook";
export const isTelegramReceiveMode = (value: unknown): value is TelegramReceiveMode => value === "long_polling" || value === "webhook";
export const receiveModeLabel = (value: unknown) => value === "long_polling" ? "长轮询" : value === "webhook" ? "Webhook" : "接收方式未确认";
export type CredentialPurpose = "telegram.bot_token" | "telegram.webhook_secret" | "wecom.bot_secret";
export type ChannelTarget = { deployment_id: string; revision_number: number };
export type ChannelPublishedTarget = ChannelTarget & {
  tenant_id: string; deployment_revision_id: string; manifest_ref: string; manifest_digest: string;
};
export type ChannelCredentialStatus = { purpose: CredentialPurpose; credential_version: number; configured: boolean };
export type ChannelAccount = {
  tenant_id: string; account_id: string; provider: ChannelProvider; provider_account_id: string;
  name: string; description: string; account_revision: number; connection_revision: number;
  min_route_generation: number; enabled: boolean;
  config: { webhook_path?: string; bot_id?: string; receive_mode?: TelegramReceiveMode; endpoint_profile?: TelegramEndpointProfile };
  credentials: ChannelCredentialStatus[]; created_by: string; created_at: string; updated_at: string;
};
export type ChannelBinding = {
  tenant_id: string; binding_id: string; account_id: string; binding_revision: number; enabled: boolean;
  target: ChannelPublishedTarget; created_by: string; created_at: string; updated_at: string;
};
export type ChannelDistribution = "NOT_EMITTED" | "PENDING" | "IN_FLIGHT" | "PUBLISHED" | "FAILED";
/** The public observation protocol permits future strings; views must provide an unknown-state fallback. */
export type ChannelObservation = {
  connection_revision: number; instance_id: string; instance_epoch: string; report_sequence: number;
  state: string; reason_code: string; observed_at: string; owner_epoch?: number;
  received_at: string; effective_state: string; receive_mode?: TelegramReceiveMode;
};
export type ChannelAccountDetails = {
  account: ChannelAccount; binding?: ChannelBinding; route_generation: number; route_event_id?: string;
  distribution: ChannelDistribution; gateway_application: "UNKNOWN"; observations: ChannelObservation[];
};
export type ChannelBindingDetails = {
  binding: ChannelBinding; route_generation: number; route_event_id?: string;
  distribution: ChannelDistribution; gateway_application: "UNKNOWN";
};
export type ChannelCommandResult = {
  account?: ChannelAccount; binding?: ChannelBinding; route_generation: number; event_id?: string;
  distribution: ChannelDistribution;
};
export type ChannelAccountPage = { accounts: ChannelAccount[]; next_cursor?: string };
export type ChannelBindingPage = { bindings: ChannelBinding[]; next_cursor?: string };
export type CredentialEdit = { action: "replace"; value: string } | { action: "keep" | "clear"; value?: never };
export type CreateAccountInput = {
  provider: ChannelProvider; provider_account_id: string; name: string; description?: string;
  config?: { receive_mode: TelegramReceiveMode; endpoint_profile?: TelegramEndpointProfile };
  credentials: Partial<Record<CredentialPurpose, { action: "replace"; value: string }>>;
};
export type UpdateAccountInput = { expected_account_revision: number; name?: string; description?: string; config?: { receive_mode: TelegramReceiveMode; endpoint_profile?: TelegramEndpointProfile } };
export type UpdateCredentialInput = { expected_account_revision: number; expected_credential_version: number } & CredentialEdit;
export type AccountEnabledInput = { expected_account_revision: number; enabled: boolean };
export type CreateBindingInput = { account_id: string; target: ChannelTarget };
export type BindingTargetInput = { expected_binding_revision: number; target: ChannelTarget };
export type BindingEnabledInput = { expected_binding_revision: number; enabled: boolean };
// Explicit aliases make the ownership clear at cross-feature import sites.
export type ChannelCreateAccountInput = CreateAccountInput;
export type ChannelUpdateAccountInput = UpdateAccountInput;
export type ChannelUpdateCredentialInput = UpdateCredentialInput;

export class ChannelApiError extends Error {
  constructor(public readonly status: number, public readonly code: string, message: string, public readonly field?: string) {
    super(message); this.name = "ChannelApiError";
  }
}
export const CHANNEL_TIMEOUT_MS = 10_000;
/** Bounds existing owner-query clients; the caller must still ignore superseded results. */
export async function channelRead<T>(operation: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([operation, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new ChannelApiError(0, "REQUEST_TIMEOUT", "读取超过 10 秒，请重试。")), CHANNEL_TIMEOUT_MS);
    })]);
  } finally { clearTimeout(timer); }
}
function object(value: unknown): value is Record<string, unknown> { return !!value && typeof value === "object" && !Array.isArray(value); }
function positive(value: unknown): boolean { return typeof value === "number" && Number.isSafeInteger(value) && value > 0; }
function textFields(value: Record<string, unknown>, fields: string[]): boolean { return fields.every((key) => typeof value[key] === "string"); }
function accountShape(value: unknown, legacy = false): boolean {
  if (!object(value) || !textFields(value, ["account_id", "tenant_id", "provider_account_id", "name", "description", "created_by", "created_at", "updated_at"]) || !value.account_id) return false;
  if ((value.provider !== "telegram" && value.provider !== "wecom") || !positive(value.account_revision) || !positive(value.connection_revision) || typeof value.enabled !== "boolean" || !object(value.config)) return false;
  if (typeof value.min_route_generation !== "number" || !Number.isSafeInteger(value.min_route_generation) || value.min_route_generation < 0) return false;
  if (value.provider === "telegram" ? typeof value.config.webhook_path !== "string" : typeof value.config.bot_id !== "string") return false;
  if (value.provider === "telegram" ? !(legacy ? value.config.receive_mode === undefined : isTelegramReceiveMode(value.config.receive_mode)) : value.config.receive_mode !== undefined) return false;
  if (value.config.endpoint_profile !== undefined && (value.provider !== "telegram" || !["official", "test"].includes(String(value.config.endpoint_profile)))) return false;
  return Array.isArray(value.credentials) && value.credentials.every((item) => object(item) && typeof item.purpose === "string" && positive(item.credential_version) && typeof item.configured === "boolean");
}
function bindingShape(value: unknown): boolean {
  return object(value) && textFields(value, ["tenant_id", "binding_id", "account_id", "created_by", "created_at", "updated_at"])
    && !!value.binding_id && positive(value.binding_revision) && typeof value.enabled === "boolean" && object(value.target)
    && textFields(value.target, ["tenant_id", "deployment_id", "deployment_revision_id", "manifest_ref", "manifest_digest"]) && positive(value.target.revision_number);
}
function routeShape(value: Record<string, unknown>): boolean {
  return typeof value.route_generation === "number" && Number.isSafeInteger(value.route_generation) && value.route_generation >= 0
    && typeof value.distribution === "string" && ["NOT_EMITTED", "PENDING", "IN_FLIGHT", "PUBLISHED", "FAILED"].includes(value.distribution);
}
function observationShape(value: unknown): boolean {
  return object(value) && textFields(value, ["instance_id", "instance_epoch", "state", "reason_code", "observed_at", "received_at", "effective_state"])
    && (value.receive_mode === undefined || isTelegramReceiveMode(value.receive_mode)) && positive(value.connection_revision) && positive(value.report_sequence) && (value.owner_epoch === undefined || positive(value.owner_epoch));
}
/** Guard the shapes components consume, including successful-but-malformed responses before clearing a write intent. */
function responseShape(path: string, init: RequestInit, body: unknown, legacy = false): boolean {
  if (!object(body)) return false;
  const route = path.split("?")[0];
  const account = /^\/v1\/tenants\/[^/]+\/channel-accounts(?:\/|$)/.test(route);
  if (init.method === "POST" || init.method === "PATCH") {
    return routeShape(body) && (body.account === undefined || accountShape(body.account, legacy)) && (body.binding === undefined || bindingShape(body.binding))
      && (account ? accountShape(body.account, legacy) : bindingShape(body.binding));
  }
  if (route.endsWith("/channel-accounts") || route.endsWith("/channel-bindings")) {
    const items = account ? body.accounts : body.bindings;
    return Array.isArray(items) && items.every((item) => account ? accountShape(item) : bindingShape(item))
      && (body.next_cursor === undefined || (typeof body.next_cursor === "string" && body.next_cursor.length > 0));
  }
  if (!routeShape(body) || typeof body.gateway_application !== "string") return false;
  if (!account) return bindingShape(body.binding);
  return accountShape(body.account) && (body.binding === undefined || bindingShape(body.binding)) && Array.isArray(body.observations) && body.observations.every(observationShape);
}
async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const operation = (async () => {
      const response = await fetch(`/api/control${path}`, { ...init, credentials: "include", cache: "no-store", signal: controller.signal });
      const body: unknown = await response.json().catch(() => {
        if (response.status === 404) throw new ChannelApiError(404, "CHANNEL_MODULE_UNAVAILABLE", "当前后端未提供此渠道管理接口，请检查运行版本与 Channel 配置；这不是空账户列表。");
        if (response.status === 401) throw new ChannelApiError(401, "UNAUTHENTICATED", "会话已过期，请重新登录。");
        if (response.status === 403) throw new ChannelApiError(403, "CHANNEL_PERMISSION_DENIED", "当前身份没有此渠道操作权限。");
        throw new ChannelApiError(response.status, "INVALID_RESPONSE", `渠道服务返回非 JSON 响应（HTTP ${response.status}），写操作结果待确认。`);
      });
      if (!response.ok) {
        const data = body && typeof body === "object" && "error" in body ? body.error : null;
        const error = data && typeof data === "object" ? data as Record<string, unknown> : {};
        throw new ChannelApiError(response.status, typeof error.code === "string" ? error.code : "HTTP_ERROR", `渠道请求未完成（HTTP ${response.status}）。`, typeof error.field === "string" ? error.field : undefined);
      }
      const contract = response.headers.get("X-Channel-Result-Contract");
      if ((contract !== null && contract !== "receive-modes-v1" && contract !== "webhook-v1") || !responseShape(path, init, body, contract === "webhook-v1")) throw new ChannelApiError(response.status, "INVALID_RESPONSE", `渠道响应结构不完整（HTTP ${response.status}），请保留写请求并确认结果。`);
      return body as T;
    })();
    return await Promise.race([operation, new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new ChannelApiError(0, "REQUEST_TIMEOUT", "请求超过 10 秒，操作结果待确认。请保留本次请求，使用原标识重试确认。"));
      }, CHANNEL_TIMEOUT_MS);
    })]);
  } catch (error) {
    if (controller.signal.aborted) throw new ChannelApiError(0, "REQUEST_TIMEOUT", "请求超过 10 秒，操作结果待确认。请保留本次请求，使用原标识重试确认。");
    if (error instanceof ChannelApiError) throw error;
    throw new ChannelApiError(0, "NETWORK_ERROR", "网络请求未完成，操作结果待确认；请使用原请求确认，不要重复创建或轮换。");
  } finally { clearTimeout(timer); }
}
const path = (tenant: string, kind: "channel-accounts" | "channel-bindings", id?: string) => `/v1/tenants/${encodeURIComponent(tenant)}/${kind}${id === undefined ? "" : `/${encodeURIComponent(id)}`}`;
function pagination(cursor?: string, pageSize?: number): string {
  const query = new URLSearchParams();
  if (cursor !== undefined && cursor !== "") query.set("cursor", cursor);
  if (pageSize !== undefined) query.set("page_size", String(pageSize));
  return query.size ? `?${query}` : "";
}
function json(method: "POST" | "PATCH", body: unknown, key: string): RequestInit {
  if (typeof key !== "string" || key.length < 1 || key.length > 128 || /[^\x21-\x7e]/.test(key)) throw new ChannelApiError(400, "CHANNEL_INPUT_INVALID", "请求标识必须是 1～128 位可见 ASCII 字符。", "/Idempotency-Key");
  return { method, headers: { "Content-Type": "application/json", "Idempotency-Key": key }, body: JSON.stringify(body) };
}
const targetBody = (target: ChannelTarget) => ({ deployment_id: target.deployment_id, revision_number: target.revision_number });
const editBody = (edit: CredentialEdit) => edit.action === "replace" ? { action: edit.action, value: edit.value } : { action: edit.action };
/** Every mutation constructs a fresh allowlisted body. Read-only config/IDs never leak from an edited Account. */
export const channelApi = {
  listAccounts(tenant: string, cursor?: string, pageSize?: number) { return request<ChannelAccountPage>(path(tenant, "channel-accounts") + pagination(cursor, pageSize)); },
  getAccount(tenant: string, id: string) { return request<ChannelAccountDetails>(path(tenant, "channel-accounts", id)); },
  createAccount(tenant: string, input: CreateAccountInput, key: string, contract?: "webhook-v1") {
    if (contract && (input.provider !== "telegram" || input.config !== undefined || !input.credentials["telegram.bot_token"] || !input.credentials["telegram.webhook_secret"])) throw new ChannelApiError(400, "CHANNEL_INPUT_INVALID", "旧创建契约仅用于无 config 的原 Telegram 两项凭据请求。");
    const credentials: CreateAccountInput["credentials"] = {};
    for (const purpose of credentialPurposes(input.provider)) {
      const edit = input.credentials[purpose];
      if (edit) credentials[purpose] = { action: edit.action, value: edit.value };
    }
    const init = json("POST", {
      provider: input.provider, provider_account_id: input.provider_account_id, name: input.name,
      ...(input.provider === "telegram" && input.config?.receive_mode !== undefined ? { config: { receive_mode: input.config.receive_mode, ...(input.config.endpoint_profile !== undefined ? {endpoint_profile: input.config.endpoint_profile} : {}) } } : {}),
      ...(input.description !== undefined ? { description: input.description } : {}), credentials,
    }, key);
    if (contract) init.headers = { ...init.headers, "X-Channel-Create-Contract": contract };
    return request<ChannelCommandResult>(path(tenant, "channel-accounts"), init);
  },
  updateAccount(tenant: string, id: string, input: UpdateAccountInput, key: string) {
    return request<ChannelCommandResult>(path(tenant, "channel-accounts", id), json("PATCH", {
      expected_account_revision: input.expected_account_revision,
      ...(input.config?.receive_mode !== undefined ? { config: { receive_mode: input.config.receive_mode, ...(input.config.endpoint_profile !== undefined ? {endpoint_profile: input.config.endpoint_profile} : {}) } } : {}),
      ...(input.name !== undefined ? { name: input.name } : {}), ...(input.description !== undefined ? { description: input.description } : {}),
    }, key));
  },
  updateCredential(tenant: string, id: string, purpose: CredentialPurpose, input: UpdateCredentialInput, key: string) {
    return request<ChannelCommandResult>(`${path(tenant, "channel-accounts", id)}/credentials/${encodeURIComponent(purpose)}/update`, json("POST", {
      expected_account_revision: input.expected_account_revision, expected_credential_version: input.expected_credential_version, ...editBody(input),
    }, key));
  },
  setAccountEnabled(tenant: string, id: string, input: AccountEnabledInput, key: string) {
    return request<ChannelCommandResult>(`${path(tenant, "channel-accounts", id)}/enabled`, json("POST", { expected_account_revision: input.expected_account_revision, enabled: input.enabled }, key));
  },
  listBindings(tenant: string, cursor?: string, pageSize?: number) { return request<ChannelBindingPage>(path(tenant, "channel-bindings") + pagination(cursor, pageSize)); },
  getBinding(tenant: string, id: string) { return request<ChannelBindingDetails>(path(tenant, "channel-bindings", id)); },
  createBinding(tenant: string, input: CreateBindingInput, key: string) {
    return request<ChannelCommandResult>(path(tenant, "channel-bindings"), json("POST", { account_id: input.account_id, target: targetBody(input.target) }, key));
  },
  setBindingTarget(tenant: string, id: string, input: BindingTargetInput, key: string) {
    return request<ChannelCommandResult>(`${path(tenant, "channel-bindings", id)}/target`, json("POST", { expected_binding_revision: input.expected_binding_revision, target: targetBody(input.target) }, key));
  },
  setBindingEnabled(tenant: string, id: string, input: BindingEnabledInput, key: string) {
    return request<ChannelCommandResult>(`${path(tenant, "channel-bindings", id)}/enabled`, json("POST", { expected_binding_revision: input.expected_binding_revision, enabled: input.enabled }, key));
  },
};
export function isChannelUncertain(error: unknown): boolean {
  if (!(error instanceof ChannelApiError)) return error instanceof TypeError || (error instanceof Error && error.name === "AbortError");
  return error.status === 0 || error.status >= 500 || (error.code === "INVALID_RESPONSE" && error.status >= 200 && error.status < 300);
}
export function channelError(error: unknown): string {
  if (!(error instanceof ChannelApiError)) return "渠道请求未完成，请重试或重新读取当前状态。";
  const messages: Record<string, string> = {
    CHANNEL_MODULE_UNAVAILABLE: "当前后端未提供渠道管理接口，请检查运行版本与 Channel 配置；这不是空账户列表。",
    CHANNEL_INPUT_INVALID: "请求字段不符合渠道接口约定，请检查标注字段、版本和凭据用途。",
    CHANNEL_BINDING_INPUT_INVALID: "绑定输入无效，请选择本租户的已发布部署版本，不使用 Draft 或 latest。",
    CHANNEL_ACCOUNT_NOT_FOUND: "此渠道账户在当前租户中不可见，请刷新列表。",
    CHANNEL_BINDING_NOT_FOUND: "此消息路由在当前租户中不可见，请重新读取账户详情。",
    CHANNEL_TARGET_NOT_FOUND: "所选部署版本在当前租户中不可见，请重新选择已发布版本。",
    CHANNEL_ACCOUNT_ALREADY_BOUND: "此账户已有消息路由，请重新读取账户并更改现有目标，不要重复创建。",
    CHANNEL_ACCOUNT_IDENTITY_CONFLICT: "此 Provider 和机器人身份已登记，请核对物理 ID 或已有渠道账户。",
    CHANNEL_REVISION_CONFLICT: "账户已被修改。请读取最新版本并对比你的输入，再明确确认新操作。",
    CHANNEL_BINDING_REVISION_CONFLICT: "消息路由或目标已被修改。请读取最新版本，不要覆盖旧确认基线。",
    CHANNEL_CREDENTIAL_VERSION_CONFLICT: "此凭据版本已更新。请核对最新状态，不要自动重复轮换。",
    CHANNEL_IDEMPOTENCY_CONFLICT: "本次请求标识已对应不同内容，请核对原操作；新编辑意图需使用新标识。",
    CHANNEL_ACCOUNT_DISABLED: "请先显式启用本平台接入，再开启消息路由。",
    CHANNEL_CREDENTIAL_REQUIRED: "此账户缺少必需凭据，请先在凭据区补齐。",
    CHANNEL_ACCOUNT_MUST_BE_DISABLED: "更改接收方式或清除凭据前请先停用本平台接入；停用会影响消息接纳和回复资格。",
    CHANNEL_VERSION_EXHAUSTED: "账户或绑定版本已达到上限，请联系平台处理，不要重置版本。",
    CHANNEL_ROUTE_GENERATION_EXHAUSTED: "路由代数已达到上限，请联系平台处理，不要回退计数。",
    CHANNEL_SOURCE_INTEGRITY: "账户状态完整性检查未通过，请保留现状并联系平台处理。",
    CHANNEL_TARGET_INTEGRITY: "固定部署目标完整性检查未通过，请保留原目标，不要自动改用最新版本。",
    CHANNEL_DEPENDENCY_UNAVAILABLE: "渠道依赖暂时异常。请保留原操作标识，重新读取或用原请求确认结果。",
  };
  if (error.status === 401) return "会话已过期，请重新登录；秘密输入不会跨刷新保存。";
  if (error.status === 403) return "当前身份没有此渠道操作权限；修改账户、凭据或路由需要租户 OWNER。";
  if (error.code === "CHANNEL_LIMIT_EXCEEDED") return error.status === 413 ? "请求超过大小上限，请缩减输入。" : "账户数量或配置规模已达到平台上限，请联系平台处理。";
  if (messages[error.code]) return messages[error.code];
  return error.message;
}
export function credentialPurposes(provider: ChannelProvider): CredentialPurpose[] {
  return provider === "telegram" ? ["telegram.bot_token", "telegram.webhook_secret"] : provider === "wecom" ? ["wecom.bot_secret"] : [];
}
/** Allowed purposes remain stable; only the current mode's readiness requirements vary. */
export function requiredCredentialPurposes(provider: ChannelProvider, mode?: TelegramReceiveMode): CredentialPurpose[] {
  return provider === "telegram" && mode === "long_polling" ? ["telegram.bot_token"] : credentialPurposes(provider);
}
const trimMetadata = (value: string) => value.replace(/^\p{White_Space}+|\p{White_Space}+$/gu, "");
function validUnicode(value: string): boolean {
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit >= 0xd800 && unit <= 0xdbff) { const next = value.charCodeAt(++i); if (!(next >= 0xdc00 && next <= 0xdfff)) return false; }
    else if (unit >= 0xdc00 && unit <= 0xdfff) return false;
  }
  return true;
}
export function validateCredential(purpose: CredentialPurpose, value: string): string {
  if (!["telegram.bot_token", "telegram.webhook_secret", "wecom.bot_secret"].includes(purpose)) return "凭据用途不受支持。";
  if (typeof value !== "string" || !value.length || !validUnicode(value) || new TextEncoder().encode(value).length > 16 * 1024) return "凭据必须是 1～16384 UTF-8 字节的非空有效文本。";
  if (purpose === "telegram.webhook_secret" && (value.length > 256 || /[^A-Za-z0-9_-]/.test(value))) return "Webhook Secret 需为 1～256 位 ASCII 字母、数字、下划线或连字符，不接受空格和换行。";
  return "";
}
export function validateChannelAccount(input: CreateAccountInput): Record<string, string> {
  const errors: Record<string, string> = {};
  const purposes = credentialPurposes(input.provider);
  const mode = input.config?.receive_mode ?? "long_polling";
  if (input.provider === "telegram" && !isTelegramReceiveMode(mode)) errors.config = "请选择明确的接收方式。";
  if (input.provider === "wecom" && input.config !== undefined) errors.config = "企业微信不使用 Telegram 接收方式。";
  const required = requiredCredentialPurposes(input.provider, mode);
  if (!purposes.length) errors.provider = "请选择 Telegram 或企业微信。";
  const physical = input.provider_account_id;
  if (typeof physical !== "string" || !physical.length || !validUnicode(physical) || new TextEncoder().encode(physical).length > 1024 || /[\p{White_Space}\p{Cc}]/u.test(physical)) errors.provider_account_id = "机器人身份需为 1～1024 UTF-8 字节，不接受空白或控制字符。";
  else if (input.provider === "telegram" && (!/^[0-9]+$/.test(physical) || !/[1-9]/.test(physical))) errors.provider_account_id = "Telegram Bot ID 需为非零十进制数字字符串，不是 @username。";
  if (typeof input.name !== "string" || !validUnicode(input.name) || Array.from(trimMetadata(input.name)).length < 1 || Array.from(trimMetadata(input.name)).length > 128) errors.name = "名称去除首尾空白后需为 1～128 个字符。";
  if (input.description !== undefined && (typeof input.description !== "string" || !validUnicode(input.description) || Array.from(input.description).length > 4096)) errors.description = "说明最多 4096 个字符。";
  for (const purpose of purposes) {
    const edit = input.credentials?.[purpose];
    if (!edit && !required.includes(purpose)) continue;
    const error = !edit || edit.action !== "replace" ? "创建账户时请完整填写此项凭据。" : validateCredential(purpose, edit.value);
    if (error) errors[`credentials.${purpose}`] = error;
  }
  if (input.credentials && Object.keys(input.credentials).some((purpose) => !purposes.includes(purpose as CredentialPurpose))) errors.credentials = "凭据用途需与所选 Provider 完全对应。";
  return errors;
}
