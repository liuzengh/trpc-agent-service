import { isTelegramReceiveMode, type TelegramReceiveMode } from "./channel-api";
/** Separate public preflight protocol: never reuse Channel route-state DTO guards. */
type PreflightVersions = { expected_account_revision: number; expected_connection_revision: number };
export type ChannelPreflightInput = PreflightVersions & (
  | { expected_bot_token_version: number; expected_bot_secret_version?: never; allow_connection_probe?: never }
  | { expected_bot_secret_version: number; allow_connection_probe: true; expected_bot_token_version?: never }
);
export type ChannelPreflightReceipt = { preflight_id: string; tenant_id: string; account_id: string; requested_at: string; job_deadline_at: string; status_url: string };
export const PREFLIGHT_CHECK_IDS = ["credential_configuration", "bot_identity", "public_origin", "webhook_registration", "pending_updates", "delivery_errors", "recovery_materials", "delivery_verification"] as const;
export const WECOM_PREFLIGHT_CHECK_IDS = ["credential_configuration", "connection_authentication", "delivery_verification"] as const;
type TelegramPreflightCheckId = typeof PREFLIGHT_CHECK_IDS[number];
export type PreflightCheckId = TelegramPreflightCheckId | typeof WECOM_PREFLIGHT_CHECK_IDS[number];
export type PreflightCheckStatus = "PASS" | "WARN" | "FAIL" | "UNKNOWN" | "SKIPPED" | "NOT_APPLICABLE";
export type PreflightCheck = { id: PreflightCheckId; status: PreflightCheckStatus; code: string; details: Record<string, string | boolean | number | null> };
type PreflightResultBase = Omit<ChannelPreflightReceipt, "status_url"> & {
  effective_config_digest?: string;
  provider_account_id: string; requested_by: string; account_revision: number; connection_revision: number;
  state: "QUEUED" | "RUNNING" | "COMPLETED" | "TIMED_OUT" | "STALE"; outcome: "PASS" | "WARN" | "FAIL" | "UNKNOWN";
  reason_code: string; freshness: "NOT_CHECKED" | "CURRENT" | "STALE" | "EXPIRED"; metadata_changed: boolean;
  started_at: string | null; checked_at: string | null; expires_at: string | null; gateway_config_digest: string | null;
  gateway_config_freshness: "UNCONFIRMED" | null; expected_public_origin: string | null; checks: PreflightCheck[];
};
export type TelegramPreflightResult = PreflightResultBase & {
  provider: "telegram"; endpoint_profile?: "official" | "test"; receive_mode?: TelegramReceiveMode; diagnostic_policy?: "telegram-receive-modes-v1"; bot_token_version: number;
};
export type WeComPreflightResult = PreflightResultBase & {
  provider: "wecom"; receive_mode: "long_connection"; diagnostic_policy: "wecom_long_connection_v1";
  bot_secret_version: number; allow_connection_probe: true; expected_public_origin: null;
};
export type ChannelPreflightResult = TelegramPreflightResult | WeComPreflightResult;
export class ChannelPreflightApiError extends Error {
  constructor(public readonly status: number, public readonly code: string, public readonly retryAfterSeconds?: number) { super(code); this.name = "ChannelPreflightApiError"; }
}
export const PREFLIGHT_TIMEOUT_MS = 10_000;
const object = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const fields = (v: Record<string, unknown>, keys: string[]) => keys.length === Object.keys(v).length && keys.every((k) => Object.hasOwn(v, k));
export const validPreflightId = (v: unknown): v is string => typeof v === "string" && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(v);
const version = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v > 0;
const date = (v: unknown): v is string => typeof v === "string" && v.length <= 40 && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z$/.test(v) && Number.isFinite(Date.parse(v));
const nullableDate = (v: unknown) => v === null || date(v);
const nullableBool = (v: unknown) => v === null || typeof v === "boolean";
const nullableCount = (v: unknown) => v === null || (typeof v === "number" && Number.isSafeInteger(v) && v >= 0);
const list = (v: unknown, choices: readonly string[]) => typeof v === "string" && choices.includes(v);
const collection = (t: string, a: string) => `/v1/tenants/${encodeURIComponent(t)}/channel-accounts/${encodeURIComponent(a)}/preflights`;
export const preflightResultHref = (t: string, a: string, id: string) => `/tenants/${encodeURIComponent(t)}/channels/${encodeURIComponent(a)}?${new URLSearchParams({ preflight: id })}`;
export function validPreflightInput(v: unknown): v is ChannelPreflightInput {
  if (!object(v) || !version(v.expected_account_revision) || !version(v.expected_connection_revision)) return false;
  return fields(v, ["expected_account_revision", "expected_connection_revision", "expected_bot_token_version"]) && version(v.expected_bot_token_version)
    || fields(v, ["expected_account_revision", "expected_connection_revision", "expected_bot_secret_version", "allow_connection_probe"]) && version(v.expected_bot_secret_version) && v.allow_connection_probe === true;
}
const receiptKeys = ["preflight_id", "tenant_id", "account_id", "requested_at", "job_deadline_at", "status_url"];
function receipt(v: unknown, t: string, a: string): v is ChannelPreflightReceipt {
  return object(v) && fields(v, receiptKeys) && validPreflightId(v.preflight_id) && v.tenant_id === t && v.account_id === a && date(v.requested_at) && date(v.job_deadline_at)
    && Date.parse(v.job_deadline_at) > Date.parse(v.requested_at) && v.status_url === `${collection(t, a)}/${encodeURIComponent(v.preflight_id)}`;
}
const providerCodes = { PROVIDER_NETWORK: "UNKNOWN", PROVIDER_TIMEOUT: "UNKNOWN", PROVIDER_RATE_LIMITED: "UNKNOWN", PROVIDER_UNAVAILABLE: "UNKNOWN", PROVIDER_RESPONSE_INVALID: "UNKNOWN" };
const checkCodes: Record<TelegramPreflightCheckId, Record<string, string>> = {
  credential_configuration: { CREDENTIALS_CONFIGURED: "PASS", BOT_TOKEN_MISSING: "FAIL", WEBHOOK_SECRET_MISSING: "FAIL" },
  bot_identity: { BOT_IDENTITY_MATCH: "PASS", BOT_IDENTITY_MISMATCH: "FAIL", TOKEN_REJECTED: "FAIL", ...providerCodes, NOT_EXECUTED: "SKIPPED" },
  public_origin: { PUBLIC_ORIGIN_STATIC_VALID: "PASS", PUBLIC_ORIGIN_INVALID: "FAIL", PUBLIC_ORIGIN_NOT_PUBLIC: "FAIL" },
  webhook_registration: { WEBHOOK_MATCH: "PASS", WEBHOOK_DIFFERENT: "WARN", WEBHOOK_NONE: "WARN", WEBHOOK_COMPARISON_UNAVAILABLE: "UNKNOWN", TOKEN_REJECTED: "FAIL", ...providerCodes, NOT_EXECUTED: "SKIPPED" },
  pending_updates: { PENDING_UPDATES_ZERO: "PASS", PENDING_UPDATES_PRESENT: "WARN", NOT_EXECUTED: "SKIPPED" },
  delivery_errors: { DELIVERY_ERROR_NOT_REPORTED: "PASS", DELIVERY_ERROR_REPORTED: "WARN", NOT_EXECUTED: "SKIPPED" },
  recovery_materials: { RECOVERY_MATERIALS_UNAVAILABLE: "UNKNOWN" },
  delivery_verification: { DELIVERY_NOT_TESTED: "UNKNOWN" },
};
const notApplicableCodes: Partial<Record<TelegramPreflightCheckId, string>> = { public_origin: "PUBLIC_ORIGIN_NOT_APPLICABLE", delivery_errors: "DELIVERY_ERRORS_NOT_APPLICABLE", recovery_materials: "RECOVERY_MATERIALS_NOT_APPLICABLE" };
function check(v: unknown, id: TelegramPreflightCheckId, mode: TelegramReceiveMode): v is PreflightCheck {
  if (mode === "long_polling" && notApplicableCodes[id]) return object(v) && fields(v, ["id", "status", "code", "details"]) && v.id === id && v.status === "NOT_APPLICABLE" && v.code === notApplicableCodes[id] && object(v.details) && fields(v.details, ["applicability"]) && v.details.applicability === "NOT_APPLICABLE";
  const codes = mode === "long_polling" && id === "webhook_registration" ? { WEBHOOK_NONE: "PASS", WEBHOOK_BLOCKS_LONG_POLLING: "FAIL", TOKEN_REJECTED: "FAIL", ...providerCodes, NOT_EXECUTED: "SKIPPED" } : mode === "long_polling" && id === "credential_configuration" ? { CREDENTIALS_CONFIGURED: "PASS", BOT_TOKEN_MISSING: "FAIL" } : checkCodes[id];
  if (!object(v) || !fields(v, ["id", "status", "code", "details"]) || v.id !== id || typeof v.code !== "string" || (codes as Record<string, string>)[v.code] !== v.status || !object(v.details)) return false;
  const d = v.details; const code = v.code;
  switch (id) {
    case "credential_configuration": return fields(d, ["bot_token_configured", "webhook_secret_configured"]) && typeof d.bot_token_configured === "boolean" && typeof d.webhook_secret_configured === "boolean" && (code === "CREDENTIALS_CONFIGURED" ? d.bot_token_configured && (mode === "long_polling" || d.webhook_secret_configured) : code === "BOT_TOKEN_MISSING" ? !d.bot_token_configured : !d.webhook_secret_configured);
    case "bot_identity": return fields(d, ["identity_match"]) && nullableBool(d.identity_match) && (code === "BOT_IDENTITY_MATCH" ? d.identity_match === true : code === "BOT_IDENTITY_MISMATCH" ? d.identity_match === false : d.identity_match === null);
    case "public_origin": return fields(d, ["validation"]) && d.validation === "STATIC_ONLY";
    case "webhook_registration": return fields(d, ["presence", "relation"]) && nullableBool(d.presence) && list(d.relation, ["MATCH", "DIFFERENT", "NONE", "UNKNOWN"]) && (code === "WEBHOOK_MATCH" ? d.presence === true && d.relation === "MATCH" : (code === "WEBHOOK_DIFFERENT" || code === "WEBHOOK_BLOCKS_LONG_POLLING") ? d.presence === true && d.relation === "DIFFERENT" : code === "WEBHOOK_NONE" ? d.presence === false && d.relation === "NONE" : code === "WEBHOOK_COMPARISON_UNAVAILABLE" ? d.presence === true && d.relation === "UNKNOWN" : d.presence === null && d.relation === "UNKNOWN");
    case "pending_updates": return fields(d, ["pending_update_count"]) && nullableCount(d.pending_update_count) && (code === "PENDING_UPDATES_ZERO" ? d.pending_update_count === 0 : code === "PENDING_UPDATES_PRESENT" ? typeof d.pending_update_count === "number" && d.pending_update_count > 0 : d.pending_update_count === null);
    case "delivery_errors": return fields(d, ["has_last_error", "last_error_at"]) && nullableBool(d.has_last_error) && nullableDate(d.last_error_at) && (code === "DELIVERY_ERROR_NOT_REPORTED" ? d.has_last_error === false && d.last_error_at === null : code === "DELIVERY_ERROR_REPORTED" ? d.has_last_error === true : d.has_last_error === null && d.last_error_at === null);
    case "recovery_materials": return fields(d, ["secret_token_readable", "restore_available"]) && d.secret_token_readable === false && d.restore_available === false;
    case "delivery_verification": return fields(d, ["verification"]) && d.verification === "NOT_TESTED";
  }
}
const resultKeys = [...receiptKeys.filter((k) => k !== "status_url"), "provider", "provider_account_id", "requested_by", "account_revision", "connection_revision", "bot_token_version", "state", "outcome", "reason_code", "freshness", "metadata_changed", "started_at", "checked_at", "expires_at", "gateway_config_digest", "gateway_config_freshness", "expected_public_origin", "checks"];
function publicOrigin(v: unknown): boolean {
  if (v === null) return true;
  if (typeof v !== "string" || v.length > 2048) return false;
  try { const u = new URL(v); return u.protocol === "https:" && !u.username && !u.password && !u.search && !u.hash && u.pathname === "/" && u.origin === v; } catch { return false; }
}
function wecomResult(v: Record<string, unknown>, t: string, a: string, id: string): v is Record<string, unknown> & WeComPreflightResult {
  const digest = (value: unknown) => typeof value === "string" && /^sha256:[a-f0-9]{64}$/.test(value);
  const keys = [...resultKeys.filter((k) => k !== "bot_token_version"), "bot_secret_version", "allow_connection_probe", "receive_mode", "diagnostic_policy", ...(Object.hasOwn(v, "effective_config_digest") ? ["effective_config_digest"] : [])];
  if (!fields(v, keys) || v.preflight_id !== id || v.tenant_id !== t || v.account_id !== a || v.provider !== "wecom" || typeof v.provider_account_id !== "string" || !v.provider_account_id.length || v.provider_account_id.length > 1024 || /\s/.test(v.provider_account_id) || !validPreflightId(v.requested_by)) return false;
  if (v.receive_mode !== "long_connection" || v.diagnostic_policy !== "wecom_long_connection_v1" || v.allow_connection_probe !== true || v.expected_public_origin !== null) return false;
  if (![v.account_revision, v.connection_revision, v.bot_secret_version].every(version) || !list(v.state, ["QUEUED", "RUNNING", "COMPLETED", "TIMED_OUT", "STALE"]) || !list(v.outcome, ["PASS", "WARN", "FAIL", "UNKNOWN"]) || !list(v.freshness, ["NOT_CHECKED", "CURRENT", "STALE", "EXPIRED"]) || typeof v.metadata_changed !== "boolean" || typeof v.reason_code !== "string" || !/^CHANNEL_[A-Z_]{1,100}$/.test(v.reason_code)) return false;
  if (!date(v.requested_at) || !date(v.job_deadline_at) || Date.parse(v.job_deadline_at) <= Date.parse(v.requested_at) || ![v.started_at, v.checked_at, v.expires_at].every(nullableDate) || !Array.isArray(v.checks)) return false;
  if (v.gateway_config_digest === null ? v.gateway_config_freshness !== null || Object.hasOwn(v, "effective_config_digest") : !digest(v.gateway_config_digest) || v.gateway_config_freshness !== "UNCONFIRMED" || !digest(v.effective_config_digest)) return false;
  if (v.state !== "COMPLETED") {
    if (v.checks.length || v.outcome !== "UNKNOWN" || v.checked_at !== null || v.expires_at !== null) return false;
    if (v.state === "QUEUED") return v.started_at === null && v.gateway_config_digest === null;
    if (v.state === "RUNNING") return date(v.started_at) && v.gateway_config_digest !== null;
    return (v.started_at === null) === (v.gateway_config_digest === null);
  }
  if (!date(v.started_at) || !date(v.checked_at) || !date(v.expires_at) || v.gateway_config_digest === null || v.checks.length !== 3) return false;
  if (!v.checks.every((c, i) => object(c) && fields(c, ["id", "status", "code", "details"]) && c.id === WECOM_PREFLIGHT_CHECK_IDS[i] && object(c.details))) return false;
  const [credential, authentication, delivery] = v.checks;
  if (!fields(credential.details, ["bot_secret_configured"]) || !(credential.code === "CREDENTIALS_CONFIGURED" && credential.status === "PASS" && credential.details.bot_secret_configured === true || credential.code === "BOT_SECRET_MISSING" && credential.status === "FAIL" && credential.details.bot_secret_configured === false)) return false;
  const authCodes: Record<string, string> = { WECOM_AUTHENTICATED: "PASS", WECOM_AUTH_REJECTED: "FAIL", NOT_EXECUTED: "SKIPPED", PROVIDER_NETWORK: "UNKNOWN", PROVIDER_TIMEOUT: "UNKNOWN", PROVIDER_RESPONSE_INVALID: "UNKNOWN", PROVIDER_UNAVAILABLE: "UNKNOWN", WECOM_CONNECTION_REPLACED: "UNKNOWN" };
  if (!fields(authentication.details, ["authenticated"]) || authCodes[authentication.code] !== authentication.status || authentication.details.authenticated !== (authentication.code === "WECOM_AUTHENTICATED" ? true : authentication.code === "WECOM_AUTH_REJECTED" ? false : null)) return false;
  if ((credential.code === "BOT_SECRET_MISSING") !== (authentication.code === "NOT_EXECUTED")) return false;
  if (!fields(delivery.details, ["verification"]) || delivery.code !== "DELIVERY_NOT_TESTED" || delivery.status !== "UNKNOWN" || delivery.details.verification !== "NOT_TESTED") return false;
  const statuses = [credential.status, authentication.status];
  return v.outcome === (statuses.includes("FAIL") ? "FAIL" : statuses.includes("UNKNOWN") || statuses.includes("SKIPPED") ? "UNKNOWN" : "PASS");
}
function result(v: unknown, t: string, a: string, id: string): v is ChannelPreflightResult {
  if (!object(v)) return false;
  if (v.provider === "wecom") return wecomResult(v, t, a, id);
  const modern = Object.hasOwn(v, "diagnostic_policy");
  const mode = modern ? v.receive_mode : "webhook";
  const digest = (value: unknown) => typeof value === "string" && /^sha256:[a-f0-9]{64}$/.test(value);
  if (v.endpoint_profile !== undefined && !["official", "test"].includes(String(v.endpoint_profile))) return false;
  const extraKeys = modern ? [...(Object.hasOwn(v, "endpoint_profile") ? ["endpoint_profile"] : []), "receive_mode", "diagnostic_policy", ...(Object.hasOwn(v, "effective_config_digest") ? ["effective_config_digest"] : [])] : [];
  if (!isTelegramReceiveMode(mode) || modern && (v.diagnostic_policy !== "telegram-receive-modes-v1" || (v.gateway_config_digest === null ? Object.hasOwn(v, "effective_config_digest") : !digest(v.effective_config_digest)))) return false;
  if (!fields(v, [...resultKeys, ...extraKeys]) || v.preflight_id !== id || v.tenant_id !== t || v.account_id !== a || v.provider !== "telegram" || typeof v.provider_account_id !== "string" || !/^[0-9]{1,1024}$/.test(v.provider_account_id) || !validPreflightId(v.requested_by)) return false;
  if (![v.account_revision, v.connection_revision, v.bot_token_version].every(version) || !list(v.state, ["QUEUED", "RUNNING", "COMPLETED", "TIMED_OUT", "STALE"]) || !list(v.outcome, ["PASS", "WARN", "FAIL", "UNKNOWN"]) || !list(v.freshness, ["NOT_CHECKED", "CURRENT", "STALE", "EXPIRED"]) || typeof v.metadata_changed !== "boolean" || typeof v.reason_code !== "string" || !/^CHANNEL_[A-Z_]{1,100}$/.test(v.reason_code)) return false;
  if (!date(v.requested_at) || !date(v.job_deadline_at) || Date.parse(v.job_deadline_at) <= Date.parse(v.requested_at) || ![v.started_at, v.checked_at, v.expires_at].every(nullableDate) || !publicOrigin(v.expected_public_origin) || !Array.isArray(v.checks)) return false;
  if (!(v.gateway_config_digest === null || typeof v.gateway_config_digest === "string" && /^sha256:[a-f0-9]{64}$/.test(v.gateway_config_digest)) || !(v.gateway_config_freshness === null || v.gateway_config_freshness === "UNCONFIRMED")) return false;
  if ((v.gateway_config_digest === null) !== (v.gateway_config_freshness === null)) return false;
  if (v.state !== "COMPLETED") {
    if (v.checks.length !== 0 || v.outcome !== "UNKNOWN" || v.checked_at !== null || v.expires_at !== null) return false;
    if (v.state === "QUEUED") return v.started_at === null && v.gateway_config_digest === null && v.expected_public_origin === null;
    if (v.state === "RUNNING") return date(v.started_at) && v.gateway_config_digest !== null;
    return (v.started_at === null) === (v.gateway_config_digest === null);
  }
  if (!date(v.started_at) || !date(v.checked_at) || !date(v.expires_at) || v.gateway_config_digest === null || v.checks.length !== 8 || !v.checks.every((c, i) => check(c, PREFLIGHT_CHECK_IDS[i], mode))) return false;
  if (modern) {
    const codes = v.checks.map((c: PreflightCheck) => c.code);
    // Same execution dependencies as Control's shared validator: no fabricated remote facts.
    if ((codes[0] === "BOT_TOKEN_MISSING") !== (codes[1] === "NOT_EXECUTED") || (codes[1] === "BOT_IDENTITY_MATCH") === (codes[3] === "NOT_EXECUTED")) return false;
    const read = (mode === "long_polling" ? ["WEBHOOK_NONE", "WEBHOOK_BLOCKS_LONG_POLLING"] : ["WEBHOOK_MATCH", "WEBHOOK_DIFFERENT", "WEBHOOK_NONE", "WEBHOOK_COMPARISON_UNAVAILABLE"]).includes(codes[3]);
    if (read === (codes[4] === "NOT_EXECUTED")) return false;
    if (mode === "webhook" && (read === (codes[5] === "NOT_EXECUTED") || (codes[2] === "PUBLIC_ORIGIN_STATIC_VALID" ? codes[3] === "WEBHOOK_COMPARISON_UNAVAILABLE" : ["WEBHOOK_MATCH", "WEBHOOK_DIFFERENT"].includes(codes[3])))) return false;
  }
  const statuses = v.checks.slice(0, 6).map((c: PreflightCheck) => c.status);
  const outcome = statuses.includes("FAIL") ? "FAIL" : statuses.some((s: string) => s === "UNKNOWN" || s === "SKIPPED") ? "UNKNOWN" : statuses.includes("WARN") ? "WARN" : "PASS";
  return v.outcome === outcome;
}
async function responseJSON(response: Response): Promise<unknown> {
  if (!response.body) return null;
  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let total = 0;
  try {
    for (;;) {
      const next = await reader.read(); if (next.done) break;
      total += next.value.byteLength;
      if (total > 65_536) { await reader.cancel(); return null; }
      chunks.push(next.value);
    }
    const bytes = new Uint8Array(total); let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
    try { return JSON.parse(new TextDecoder().decode(bytes)); } catch { return null; }
  } finally { reader.releaseLock(); }
}
async function request<T>(path: string, init: RequestInit, expected: number, validate: (v: unknown) => v is T, signal?: AbortSignal): Promise<T> {
  const controller = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined;
  const abort = () => controller.abort(); signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) controller.abort();
  try {
    return await Promise.race([
      (async () => {
        if (controller.signal.aborted) throw new ChannelPreflightApiError(0, "REQUEST_ABORTED");
        const response = await fetch(`/api/control${path}`, { ...init, signal: controller.signal, credentials: "include", cache: "no-store" });
        const body = await responseJSON(response);
        if (!response.ok) {
          const code = object(body) && object(body.error) && typeof body.error.code === "string" && /^CHANNEL_[A-Z_]{1,100}$/.test(body.error.code) ? body.error.code : "HTTP_ERROR";
          const retry = Number(response.headers.get("Retry-After"));
          throw new ChannelPreflightApiError(response.status, code, Number.isFinite(retry) && retry > 0 && retry <= 120 ? retry : undefined);
        }
        if (response.status !== expected || !validate(body)) throw new ChannelPreflightApiError(response.status, "INVALID_RESPONSE");
        return body;
      })(),
      new Promise<never>((_, reject) => { timer = setTimeout(() => { controller.abort(); reject(new ChannelPreflightApiError(0, "REQUEST_TIMEOUT")); }, PREFLIGHT_TIMEOUT_MS); }),
    ]);
  } catch (error) {
    if (error instanceof ChannelPreflightApiError) throw error;
    throw new ChannelPreflightApiError(0, signal?.aborted ? "REQUEST_ABORTED" : "NETWORK_ERROR");
  } finally { clearTimeout(timer); signal?.removeEventListener("abort", abort); }
}
export const channelPreflightApi = {
  create(tenant: string, account: string, input: ChannelPreflightInput, key: string, signal?: AbortSignal) {
    const wecom = Object.hasOwn(input, "expected_bot_secret_version");
    if (wecom && Object.hasOwn(input, "expected_bot_token_version") || !wecom && Object.hasOwn(input, "allow_connection_probe")) return Promise.reject(new ChannelPreflightApiError(400, "CHANNEL_INPUT_INVALID"));
    const versions = { expected_account_revision: input.expected_account_revision, expected_connection_revision: input.expected_connection_revision };
    const body = wecom ? { ...versions, expected_bot_secret_version: input.expected_bot_secret_version, allow_connection_probe: input.allow_connection_probe } : { ...versions, expected_bot_token_version: input.expected_bot_token_version };
    if (!validPreflightId(tenant) || !validPreflightId(account) || !validPreflightInput(body) || !/^[\x21-\x7e]{1,128}$/.test(key)) return Promise.reject(new ChannelPreflightApiError(400, "CHANNEL_INPUT_INVALID"));
    return request(collection(tenant, account), { method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": key }, body: JSON.stringify(body) }, 202, (v): v is ChannelPreflightReceipt => receipt(v, tenant, account), signal);
  },
  get(tenant: string, account: string, id: string, signal?: AbortSignal) {
    if (![tenant, account, id].every(validPreflightId)) return Promise.reject(new ChannelPreflightApiError(400, "CHANNEL_INPUT_INVALID"));
    return request(`${collection(tenant, account)}/${encodeURIComponent(id)}`, {}, 200, (v): v is ChannelPreflightResult => result(v, tenant, account, id), signal);
  },
};
export function isPreflightUncertain(error: unknown) { return !(error instanceof ChannelPreflightApiError) || error.status === 0 || error.status >= 500 || error.code === "INVALID_RESPONSE"; }
export function preflightError(error: unknown): string {
  if (!(error instanceof ChannelPreflightApiError)) return "请求未完成，请保留本次请求并重试确认。";
  if (error.status === 401) return "会话已过期，请重新登录；本次预检记录已保留。";
  if (error.status === 403) return "当前账户没有发起此预检的权限，写操作已关闭。";
  if (error.status === 404) return error.code === "CHANNEL_PREFLIGHT_NOT_FOUND" || error.code === "CHANNEL_ACCOUNT_NOT_FOUND" ? "预检任务或账户不存在、已清理或不属于当前租户。" : "当前后端尚未提供预检接口，请更新后端后重新读取；这不是空结果。";
  if (error.status === 429) return `检查过于频繁，请${error.retryAfterSeconds ? `在 ${error.retryAfterSeconds} 秒后` : "稍后"}重试原请求。`;
  const messages: Record<string, string> = {
    CHANNEL_PREFLIGHT_ALREADY_RUNNING: "此账户已有进行中的预检；可使用发起者分享的结果链接查看，或等待该任务结束后重新检查。",
    CHANNEL_ACCOUNT_MUST_BE_DISABLED: "预检要求账户停用。请在账户接入操作中单独确认停用，不会自动停用。",
    CHANNEL_REVISION_CONFLICT: "保存的账户版本已变化，请重新读取账户后再准备检查。",
    CHANNEL_CREDENTIAL_VERSION_CONFLICT: "凭据版本已变化，请重新读取账户后再准备检查。",
    CHANNEL_IDEMPOTENCY_CONFLICT: "原请求标识与内容发生冲突，请核实已有任务后再开始新检查。",
    CHANNEL_PREFLIGHT_PROVIDER_UNSUPPORTED: "当前后端尚未支持此渠道的接入诊断，请更新后端后重试。",
    CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED: "企微连接诊断需要明确确认连接替换影响，请重新打开确认窗口。",
    INVALID_RESPONSE: "服务端预检响应格式异常，写入结果待确认；请保留原请求。",
    REQUEST_TIMEOUT: "请求超过 10 秒，结果待确认；请保留原请求重试。",
  };
  return messages[error.code] ?? (error.status >= 500 ? "预检服务暂时异常，结果待确认；请保留原请求稍后重试。" : "预检请求未完成，请核实当前任务并重试确认。");
}
export const PREFLIGHT_STORAGE_PREFIX = "channel-preflight:v1:";
export const PREFLIGHT_IDENTITY_KEY = "channel-preflight-user:v1";
export function clearChannelPreflightPreparations(storage: Storage) {
  const keys: string[] = [];
  try { for (let i = 0; i < storage.length; i++) { const k = storage.key(i); if (k?.startsWith(PREFLIGHT_STORAGE_PREFIX)) keys.push(k); } for (const k of keys) storage.removeItem(k); storage.removeItem(PREFLIGHT_IDENTITY_KEY); } catch { /* Logout must still complete when browser storage is unavailable. */ }
}
export function establishChannelPreflightIdentity(storage: Storage, userId: string): boolean {
  try {
    if (!userId) { clearChannelPreflightPreparations(storage); return false; }
    if (storage.getItem(PREFLIGHT_IDENTITY_KEY) !== userId) clearChannelPreflightPreparations(storage);
    storage.setItem(PREFLIGHT_IDENTITY_KEY, userId); return true;
  } catch { return false; }
}
