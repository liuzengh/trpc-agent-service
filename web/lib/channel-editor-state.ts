import { isTelegramReceiveMode, type ChannelTarget } from "./channel-api";

export type ChannelPending = {
  operation: string; key: string; input: Record<string, unknown>; secret: boolean; createdAt: string;
};
export type PendingChannelOperation = ChannelPending;
export const CHANNEL_PENDING_PREFIX = "channel-pending:v1:";
export const CHANNEL_IDENTITY_KEY = "channel-user:v1";
const id = (value: unknown): value is string => typeof value === "string" && value.length >= 1 && value.length <= 128 && /^[A-Za-z0-9]/.test(value) && !/[^A-Za-z0-9._:-]/.test(value);
const version = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value > 0;
const keyValue = (value: unknown): value is string => typeof value === "string" && value.length >= 1 && value.length <= 128 && !/[^\x21-\x7e]/.test(value);
function record(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value) && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
}
function fields(value: Record<string, unknown>, required: string[], optional: string[] = []): boolean {
  return required.every((key) => Object.hasOwn(value, key)) && Object.keys(value).every((key) => required.includes(key) || optional.includes(key));
}
function target(value: unknown): value is ChannelTarget {
  return record(value) && fields(value, ["deployment_id", "revision_number"]) && id(value.deployment_id) && version(value.revision_number);
}
export const channelHref = (tenant: string, accountId?: string) => `/tenants/${encodeURIComponent(tenant)}/channels${accountId ? `/${encodeURIComponent(accountId)}` : ""}`;
export function channelTargetQuery(value: ChannelTarget): string {
  if (!id(value.deployment_id) || !version(value.revision_number)) return "";
  return new URLSearchParams({ deployment_id: value.deployment_id, revision_number: String(value.revision_number) }).toString();
}
/** Duplicate or partial selectors are invalid; never silently pick one value or coerce latest/exponents. */
export function readChannelTarget(value: URLSearchParams | string): ChannelTarget | null {
  const query = typeof value === "string" ? new URLSearchParams(value) : value;
  const ids = query.getAll("deployment_id"); const versions = query.getAll("revision_number");
  if (ids.length !== 1 || versions.length !== 1 || !id(ids[0]) || (!/^[1-9][0-9]*$/.test(versions[0]) || /[^0-9]/.test(versions[0]))) return null;
  const number = Number(versions[0]);
  return version(number) ? { deployment_id: ids[0], revision_number: number } : null;
}
export const channelPendingKey = (userId: string, tenantId: string, objectId = "new") => CHANNEL_PENDING_PREFIX + [userId, tenantId, objectId].map(encodeURIComponent).join(":");
const purpose = (value: unknown) => typeof value === "string" && ["telegram.bot_token", "telegram.webhook_secret", "wecom.bot_secret"].includes(value);
/** The closed shape below is the primary guard; this recursive check also rejects nested secret-bearing aliases. */
function containsSecretField(value: unknown, depth = 0): boolean {
  if (depth > 8) return true;
  if (Array.isArray(value)) return value.some((item) => containsSecretField(item, depth + 1));
  if (!record(value)) return false;
  return Object.entries(value).some(([key, item]) => /^(?:credentials?|values?|secret|token|password|authorization|ciphertext|(?:telegram\.)?bot_token|(?:telegram\.)?webhook_secret|(?:wecom\.)?bot_secret)$/i.test(key) || containsSecretField(item, depth + 1));
}
function metadata(input: Record<string, unknown>): boolean {
  const string = (value: unknown, max: number) => typeof value === "string" && value.length <= max;
  return (!Object.hasOwn(input, "name") || string(input.name, 512)) && (!Object.hasOwn(input, "description") || string(input.description, 8192));
}
function credentialMarker(input: Record<string, unknown>, secret: boolean): boolean {
  return fields(input, ["purpose", "expected_account_revision", "expected_credential_version", "action"])
    && purpose(input.purpose) && version(input.expected_account_revision) && version(input.expected_credential_version)
    && (secret ? input.action === "replace" : input.action === "keep" || input.action === "clear");
}
function modeConfig(value: unknown): boolean {
  return record(value) && fields(value, ["receive_mode"], ["endpoint_profile"]) && isTelegramReceiveMode(value.receive_mode) && (value.endpoint_profile === undefined || value.endpoint_profile === "official" || value.endpoint_profile === "test");
}
function createModeMarker(input: Record<string, unknown>): boolean {
  // An old marker remains intact and distinguishable; the form locks legacy Telegram recovery.
  if (!Object.hasOwn(input, "config") && !Object.hasOwn(input, "supplied_purposes")) return true;
  if (input.provider !== "telegram" || !modeConfig(input.config) || !Array.isArray(input.supplied_purposes)) return false;
  const purposes = input.supplied_purposes;
  return (purposes.length === 1 || purposes.length === 2) && purposes[0] === "telegram.bot_token"
    && (purposes.length === 1 || purposes[1] === "telegram.webhook_secret")
    && ((input.config as Record<string, unknown>).receive_mode !== "webhook" || purposes.length === 2);
}
function validInput(pending: ChannelPending): boolean {
  const input = pending.input;
  if (!record(input) || containsSecretField(input)) return false;
  if (pending.secret) {
    if (pending.operation === "updateCredential") return credentialMarker(input, true);
    return pending.operation === "createAccount"
      && fields(input, ["provider", "provider_account_id", "name"], ["description", "config", "supplied_purposes"])
      && createModeMarker(input)
      && (input.provider === "telegram" || input.provider === "wecom")
      && typeof input.provider_account_id === "string" && input.provider_account_id.length > 0 && new TextEncoder().encode(input.provider_account_id).length <= 1024
      && typeof input.name === "string" && input.name.trim().length > 0 && metadata(input);
  }
  switch (pending.operation) {
    case "updateAccount": return fields(input, ["expected_account_revision"], ["name", "description", "config"])
      && version(input.expected_account_revision) && (Object.hasOwn(input, "name") || Object.hasOwn(input, "description") || Object.hasOwn(input, "config")) && metadata(input)
      && (!Object.hasOwn(input, "config") || modeConfig(input.config));
    case "setAccountEnabled": return fields(input, ["expected_account_revision", "enabled"]) && version(input.expected_account_revision) && typeof input.enabled === "boolean";
    case "createBinding": return fields(input, ["account_id", "target"]) && id(input.account_id) && target(input.target);
    case "setBindingTarget": return fields(input, ["expected_binding_revision", "target"]) && version(input.expected_binding_revision) && target(input.target);
    case "setBindingEnabled": return fields(input, ["expected_binding_revision", "enabled"]) && version(input.expected_binding_revision) && typeof input.enabled === "boolean";
    case "updateCredential": return credentialMarker(input, false);
    default: return false;
  }
}
function validPending(value: unknown): value is ChannelPending {
  if (!record(value) || !fields(value, ["operation", "key", "input", "secret", "createdAt"])) return false;
  return typeof value.operation === "string" && keyValue(value.key) && typeof value.secret === "boolean"
    && typeof value.createdAt === "string" && value.createdAt.length <= 64 && Number.isFinite(Date.parse(value.createdAt))
    && validInput(value as ChannelPending);
}
function storageKey(key: string): boolean {
  if (!key.startsWith(CHANNEL_PENDING_PREFIX) || key.length > 2048) return false;
  const parts = key.slice(CHANNEL_PENDING_PREFIX.length).split(":");
  try { return parts.length === 3 && parts.every((part) => part.length > 0 && decodeURIComponent(part).length <= 256); }
  catch { return false; }
}
/** secret=true is a non-secret receipt-recovery marker, not permission to persist a credential-bearing body. */
export function saveChannelPending(storage: Storage, key: string, pending: ChannelPending): boolean {
  try {
    if (!storageKey(key) || !validPending(pending)) return false;
    const serialized = JSON.stringify(pending);
    // Revalidate the JSON representation too; toJSON must not introduce fields after validation.
    if (serialized.length > 20_000 || !validPending(JSON.parse(serialized))) return false;
    storage.setItem(key, serialized); return true;
  } catch { return false; }
}
export function loadChannelPending(storage: Storage, key: string): ChannelPending | null {
  try {
    if (!storageKey(key)) return null;
    const serialized = storage.getItem(key);
    if (serialized === null) return null;
    const pending: unknown = serialized.length <= 20_000 ? JSON.parse(serialized) : null;
    if (!validPending(pending)) { storage.removeItem(key); return null; }
    return pending;
  } catch {
    try { storage.removeItem(key); } catch { /* A blocked browser store must not prevent entering the console. */ }
    return null;
  }
}
export function clearChannelPending(storage: Storage, key: string): void {
  try { if (storageKey(key)) storage.removeItem(key); } catch { /* Storage may be unavailable. */ }
}
export function clearChannelPreparations(storage: Storage): void {
  try {
    const keys: string[] = [];
    for (let i = 0; i < storage.length; i++) { const key = storage.key(i); if (key?.startsWith(CHANNEL_PENDING_PREFIX)) keys.push(key); }
    for (const key of keys) storage.removeItem(key);
    storage.removeItem(CHANNEL_IDENTITY_KEY);
  } catch { /* Logout must still finish when storage is blocked. */ }
}
export function establishChannelIdentity(storage: Storage, userId: string): boolean {
  try {
    if (!userId) { clearChannelPreparations(storage); return false; }
    if (storage.getItem(CHANNEL_IDENTITY_KEY) !== userId) clearChannelPreparations(storage);
    storage.setItem(CHANNEL_IDENTITY_KEY, userId); return true;
  } catch { return false; }
}
