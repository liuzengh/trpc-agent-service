import type { ChannelAccount } from "../lib/channel-api";
import type { WeComPreflightResult } from "../lib/channel-preflight-api";
import { sampleChannelAccount } from "./channel-fixtures";
import { preflightResult } from "./channel-preflight-fixtures";

export const wecomAccount: ChannelAccount = {
  ...sampleChannelAccount, provider: "wecom", provider_account_id: "aibot_test", enabled: false,
  config: { bot_id: "aibot_test" }, credentials: [{ purpose: "wecom.bot_secret", credential_version: 4, configured: true }],
};
const { bot_token_version: _token, ...common } = preflightResult;
export const wecomPreflight: WeComPreflightResult = {
  ...common, provider: "wecom", provider_account_id: "aibot_test", receive_mode: "long_connection",
  diagnostic_policy: "wecom_long_connection_v1", bot_secret_version: 4, allow_connection_probe: true,
  effective_config_digest: `sha256:${"b".repeat(64)}`, expected_public_origin: null, outcome: "PASS",
  checks: [
    { id: "credential_configuration", status: "PASS", code: "CREDENTIALS_CONFIGURED", details: { bot_secret_configured: true } },
    { id: "connection_authentication", status: "PASS", code: "WECOM_AUTHENTICATED", details: { authenticated: true } },
    { id: "delivery_verification", status: "UNKNOWN", code: "DELIVERY_NOT_TESTED", details: { verification: "NOT_TESTED" } },
  ],
};
const { effective_config_digest: _digest, ...withoutDigest } = wecomPreflight;
export const queuedWecomPreflight: WeComPreflightResult = {
  ...withoutDigest, state: "QUEUED", outcome: "UNKNOWN", reason_code: "CHANNEL_PREFLIGHT_QUEUED", freshness: "NOT_CHECKED",
  started_at: null, checked_at: null, expires_at: null, gateway_config_digest: null, gateway_config_freshness: null, checks: [],
};
