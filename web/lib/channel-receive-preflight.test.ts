import { afterEach, describe, expect, it, vi } from "vitest";
import { channelPreflightApi, type ChannelPreflightResult } from "./channel-preflight-api";
import { longPollingPreflight, preflightResult, queuedPreflight } from "../test/channel-preflight-fixtures";

afterEach(() => vi.restoreAllMocks());
const get = (body: unknown) => { vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(body)); return channelPreflightApi.get("t", "cha_test", "cpf_test"); };
describe("Control receive-mode preflight variants", () => {
  it("accepts LP PASS with no Secret/public origin and keeps the last check untested", async () => {
    const value = await get(longPollingPreflight);
    expect(value.checks.filter((c) => c.status === "NOT_APPLICABLE").map((c) => c.id)).toEqual(["public_origin", "delivery_errors", "recovery_materials"]);
    expect(value.checks[7]).toMatchObject({ status: "UNKNOWN", code: "DELIVERY_NOT_TESTED" });
    expect(value.effective_config_digest).toBe(longPollingPreflight.effective_config_digest);
  });
  it("keeps legacy Webhook results byte-shape compatible and new Webhook policy distinct", async () => {
    expect(await get(preflightResult)).toEqual(preflightResult);
    expect(await get({ ...preflightResult, receive_mode: "webhook", diagnostic_policy: "telegram-receive-modes-v1", effective_config_digest: longPollingPreflight.effective_config_digest })).toHaveProperty("receive_mode", "webhook");
  });
  it("accepts queued mode/policy without a digest and requires the per-task digest after claim", async () => {
    const queued = { ...queuedPreflight, receive_mode: "long_polling", diagnostic_policy: "telegram-receive-modes-v1" };
    expect(await get(queued)).toEqual(queued);
    await expect(get({ ...queued, effective_config_digest: longPollingPreflight.effective_config_digest })).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
    const { effective_config_digest: _, ...missing } = longPollingPreflight;
    await expect(get(missing)).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
  });
  it("represents an existing Webhook as FAIL, never as implicit takeover or polling success", async () => {
    const value = structuredClone(longPollingPreflight);
    value.outcome = "FAIL"; value.checks[3] = { id: "webhook_registration", code: "WEBHOOK_BLOCKS_LONG_POLLING", status: "FAIL", details: { presence: true, relation: "DIFFERENT" } };
    expect(await get(value)).toEqual(value);
    await expect(get({ ...value, outcome: "PASS" })).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
  });
  it("preserves genuine provider uncertainty while keeping only the three mode-inapplicable checks N/A", async () => {
    const value = structuredClone(longPollingPreflight); value.outcome = "UNKNOWN";
    value.checks[1] = { id: "bot_identity", status: "UNKNOWN", code: "PROVIDER_TIMEOUT", details: { identity_match: null } };
    value.checks[3] = { id: "webhook_registration", status: "SKIPPED", code: "NOT_EXECUTED", details: { presence: null, relation: "UNKNOWN" } };
    value.checks[4] = { id: "pending_updates", status: "SKIPPED", code: "NOT_EXECUTED", details: { pending_update_count: null } };
    expect(await get(value)).toEqual(value);
  });
  it.each([
    ["unknown policy", (r: Record<string, unknown>) => { r.diagnostic_policy = "future"; }],
    ["unknown mode", (r: Record<string, unknown>) => { r.receive_mode = "future"; }],
    ["missing mode", (r: Record<string, unknown>) => { delete r.receive_mode; }],
    ["missing policy", (r: Record<string, unknown>) => { delete r.diagnostic_policy; }],
    ["null effective digest", (r: Record<string, unknown>) => { r.effective_config_digest = null; }],
  ])("rejects %s rather than falling back to legacy", async (_name, change) => {
    const value = { ...longPollingPreflight }; change(value);
    await expect(get(value)).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
  });
  it.each([
    ["N/A used as PASS", (r: ChannelPreflightResult) => { r.checks[2].status = "PASS"; }],
    ["N/A on identity", (r: ChannelPreflightResult) => { r.checks[1] = { ...r.checks[2], id: "bot_identity" }; }],
    ["extra details", (r: ChannelPreflightResult) => { r.checks[2].details.secret = "never-accepted"; }],
    ["LP old webhook warning", (r: ChannelPreflightResult) => { r.checks[3].status = "WARN"; }],
    ["fabricated identity without token", (r: ChannelPreflightResult) => { r.checks[0] = { id: "credential_configuration", status: "FAIL", code: "BOT_TOKEN_MISSING", details: { bot_token_configured: false, webhook_secret_configured: false } }; r.outcome = "FAIL"; }],
    ["fabricated pending after failed remote read", (r: ChannelPreflightResult) => { r.checks[3] = { id: "webhook_registration", status: "UNKNOWN", code: "PROVIDER_TIMEOUT", details: { presence: null, relation: "UNKNOWN" } }; r.outcome = "UNKNOWN"; }],
  ])("rejects %s against the shared mode matrix", async (_name, change) => {
    const value = structuredClone(longPollingPreflight); change(value);
    await expect(get(value)).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
  });
});

// The API owner owns these fixtures. A consumer change must pass their exact bytes,
// not only locally written component examples. Requires shared commit 4939e1f.
describe("API-owned receive-mode fixtures", () => {
  it.each(["preflight-polling-view-valid.json", "preflight-polling-queued-valid.json"])("accepts %s without rewriting its shape", async (filename) => {
    const { readFileSync } = await import("node:fs");
    const { resolve } = await import("node:path");
    const fixture = JSON.parse(readFileSync(resolve(process.cwd(), "../api/schemas/channel/v1/fixtures", filename), "utf8"));
    const view = fixture.document;
    vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(view));
    expect(await channelPreflightApi.get(view.tenant_id, view.account_id, view.preflight_id)).toEqual(view);
  });
  it("matches the API owner's complete polling checks exactly, including N/A details", async () => {
    const { readFileSync } = await import("node:fs");
    const { resolve } = await import("node:path");
    const fixture = JSON.parse(readFileSync(resolve(process.cwd(), "../api/schemas/channel/v1/fixtures/preflight-polling-view-valid.json"), "utf8"));
    expect(longPollingPreflight.checks).toEqual(fixture.document.checks);
  });
});
