import type { ChannelPreflightReceipt, TelegramPreflightResult } from "../lib/channel-preflight-api";
export const preflightReceipt: ChannelPreflightReceipt = { preflight_id: "cpf_test", tenant_id: "t", account_id: "cha_test", requested_at: "2026-09-06T12:00:00Z", job_deadline_at: "2026-09-06T12:02:00Z", status_url: "/v1/tenants/t/channel-accounts/cha_test/preflights/cpf_test" };
export const preflightResult: TelegramPreflightResult = {
  preflight_id: preflightReceipt.preflight_id, tenant_id: preflightReceipt.tenant_id, account_id: preflightReceipt.account_id, requested_at: preflightReceipt.requested_at, job_deadline_at: preflightReceipt.job_deadline_at,
  provider: "telegram", provider_account_id: "123456", requested_by: "u", account_revision: 3, connection_revision: 2, bot_token_version: 1,
  state: "COMPLETED", outcome: "WARN", reason_code: "CHANNEL_PREFLIGHT_COMPLETED", freshness: "CURRENT", metadata_changed: false,
  started_at: "2026-09-06T12:00:02Z", checked_at: "2026-09-06T12:00:06Z", expires_at: "2026-09-06T12:05:06Z", gateway_config_digest: `sha256:${"a".repeat(64)}`, gateway_config_freshness: "UNCONFIRMED", expected_public_origin: "https://gateway.example.com",
  checks: [
    { id: "credential_configuration", status: "PASS", code: "CREDENTIALS_CONFIGURED", details: { bot_token_configured: true, webhook_secret_configured: true } },
    { id: "bot_identity", status: "PASS", code: "BOT_IDENTITY_MATCH", details: { identity_match: true } },
    { id: "public_origin", status: "PASS", code: "PUBLIC_ORIGIN_STATIC_VALID", details: { validation: "STATIC_ONLY" } },
    { id: "webhook_registration", status: "WARN", code: "WEBHOOK_NONE", details: { presence: false, relation: "NONE" } },
    { id: "pending_updates", status: "PASS", code: "PENDING_UPDATES_ZERO", details: { pending_update_count: 0 } },
    { id: "delivery_errors", status: "PASS", code: "DELIVERY_ERROR_NOT_REPORTED", details: { has_last_error: false, last_error_at: null } },
    { id: "recovery_materials", status: "UNKNOWN", code: "RECOVERY_MATERIALS_UNAVAILABLE", details: { secret_token_readable: false, restore_available: false } },
    { id: "delivery_verification", status: "UNKNOWN", code: "DELIVERY_NOT_TESTED", details: { verification: "NOT_TESTED" } },
  ],
};
export const queuedPreflight: TelegramPreflightResult = { ...preflightResult, state: "QUEUED", outcome: "UNKNOWN", reason_code: "CHANNEL_PREFLIGHT_QUEUED", freshness: "NOT_CHECKED", started_at: null, checked_at: null, expires_at: null, gateway_config_digest: null, gateway_config_freshness: null, expected_public_origin: null, checks: [] };

/** Consumer fixture of Control's telegram-receive-modes-v1 fixed eight-check matrix. */
export const longPollingPreflight: TelegramPreflightResult = {
  ...preflightResult, receive_mode: "long_polling", diagnostic_policy: "telegram-receive-modes-v1", effective_config_digest: `sha256:${"b".repeat(64)}`,
  outcome: "PASS", expected_public_origin: null,
  checks: preflightResult.checks.map((check) => {
    if (check.id === "credential_configuration") return { ...check, details: { bot_token_configured: true, webhook_secret_configured: false } };
    if (check.id === "webhook_registration") return { ...check, status: "PASS" };
    const codes: Partial<Record<typeof check.id, string>> = { public_origin: "PUBLIC_ORIGIN_NOT_APPLICABLE", delivery_errors: "DELIVERY_ERRORS_NOT_APPLICABLE", recovery_materials: "RECOVERY_MATERIALS_NOT_APPLICABLE" };
    return codes[check.id] ? { ...check, status: "NOT_APPLICABLE", code: codes[check.id]!, details: { applicability: "NOT_APPLICABLE" } } : check;
  }),
};
