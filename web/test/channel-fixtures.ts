import type { ChannelAccount, ChannelAccountDetails, ChannelBinding, CreateAccountInput } from "../lib/channel-api";
export const sampleChannelAccount: ChannelAccount = {
  tenant_id: "t", account_id: "cha_test", provider: "telegram", provider_account_id: "123456",
  name: "研究机器人", description: "渠道测试账户", account_revision: 3, connection_revision: 2,
  min_route_generation: 1, enabled: true, config: { webhook_path: "/v1/telegram/cha_test", receive_mode: "webhook" },
  credentials: [{ purpose: "telegram.bot_token", credential_version: 1, configured: true }, { purpose: "telegram.webhook_secret", credential_version: 1, configured: true }],
  created_by: "u", created_at: "2026-09-06T10:00:00Z", updated_at: "2026-09-06T10:10:00Z",
};
export const sampleChannelBinding: ChannelBinding = {
  tenant_id: "t", binding_id: "chb_test", account_id: "cha_test", binding_revision: 2, enabled: false,
  target: { tenant_id: "t", deployment_id: "d", revision_number: 1, deployment_revision_id: "dpr_test", manifest_ref: "rmf_test", manifest_digest: `sha256:${"a".repeat(64)}` },
  created_by: "u", created_at: "2026-09-06T10:00:00Z", updated_at: "2026-09-06T10:10:00Z",
};
export const sampleChannelDetails: ChannelAccountDetails = {
  account: sampleChannelAccount, binding: sampleChannelBinding, route_generation: 1, route_event_id: "evt_test",
  distribution: "PUBLISHED", gateway_application: "UNKNOWN", observations: [],
};
/** Synthetic values only; real credentials must never be written to fixtures. */
export const sampleChannelCreate: CreateAccountInput = {
  provider: "telegram", provider_account_id: "00123456", name: "研究机器人", description: "测试",
  config: { receive_mode: "webhook" },
  credentials: { "telegram.bot_token": { action: "replace", value: "synthetic-token" }, "telegram.webhook_secret": { action: "replace", value: "synthetic_webhook_secret" } },
};
