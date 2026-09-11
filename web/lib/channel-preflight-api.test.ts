import { afterEach, describe, expect, it, vi } from "vitest";
import { channelPreflightApi, ChannelPreflightApiError, preflightError, preflightResultHref, establishChannelPreflightIdentity, clearChannelPreflightPreparations, PREFLIGHT_STORAGE_PREFIX, PREFLIGHT_IDENTITY_KEY } from "./channel-preflight-api";
import { preflightReceipt, preflightResult, queuedPreflight } from "../test/channel-preflight-fixtures";
const input = { expected_account_revision: 3, expected_connection_revision: 2, expected_bot_token_version: 1 };
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });
function response(body: unknown, status = 200) { return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } }); }
describe("Channel preflight public transport", () => {
  it("uses a separate 202 contract and an explicit original key with a closed three-version body", async () => {
    const fetch = vi.fn().mockResolvedValue(response(preflightReceipt, 202)); vi.stubGlobal("fetch", fetch);
    await expect(channelPreflightApi.create("t", "cha_test", { ...input, token: "not-to-send" } as typeof input, "original-key")).resolves.toEqual(preflightReceipt);
    const [url, init] = fetch.mock.calls[0]; expect(url).toBe("/api/control/v1/tenants/t/channel-accounts/cha_test/preflights");
    expect(JSON.parse(init.body)).toEqual(input); expect(init.headers["Idempotency-Key"]).toBe("original-key"); expect(init.credentials).toBe("include"); expect(init.cache).toBe("no-store");
  });
  it("reads an exact tenant/account/task and builds only a local result link", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(preflightResult)));
    await expect(channelPreflightApi.get("t", "cha_test", "cpf_test")).resolves.toEqual(preflightResult);
    expect(preflightResultHref("t", "cha_test", "cpf_test")).toBe("/tenants/t/channels/cha_test?preflight=cpf_test");
  });
  it.each([{}, { ...preflightReceipt, tenant_id: "other" }, { ...preflightReceipt, status_url: "https://elsewhere.invalid" }])("keeps malformed/cross-scope creation success uncertain: %j", async (body) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(body, 202)));
    await expect(channelPreflightApi.create("t", "cha_test", input, "key")).rejects.toMatchObject({ code: "INVALID_RESPONSE", status: 202 });
  });
  it.each([
    { ...preflightResult, account_id: "other" }, { ...preflightResult, checks: [] },
    { ...preflightResult, checks: preflightResult.checks.map((c, i) => i === 1 ? { ...c, status: "PASS", code: "PROVIDER_TIMEOUT" } : c) },
    { ...preflightResult, checks: preflightResult.checks.map((c, i) => i === 2 ? { ...c, details: { ...c.details, raw_url: "secret" } } : c) },
    { ...preflightResult, gateway_config_freshness: "CURRENT" },
  ])("rejects an invalid result instead of showing green status: %j", async (body) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(body)));
    await expect(channelPreflightApi.get("t", "cha_test", "cpf_test")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
  });
  it("classifies a non-JSON old backend response without exposing raw content", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("raw secret token", { status: 404 })));
    try { await channelPreflightApi.get("t", "cha_test", "cpf_test"); } catch (error) { expect(preflightError(error)).toContain("当前后端"); expect(preflightError(error)).not.toContain("raw secret"); }
  });
  it("does not echo an upstream error or fetch exception containing a token", async () => {
    const secret = "botToken-never-render"; vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error(secret)));
    try { await channelPreflightApi.create("t", "cha_test", input, "key"); } catch (error) { expect(preflightError(error)).not.toContain(secret); expect(error).toBeInstanceOf(ChannelPreflightApiError); }
  });
  it("bounds stalled fetches even when fetch ignores abort", async () => {
    vi.useFakeTimers(); vi.stubGlobal("fetch", vi.fn().mockReturnValue(new Promise(() => {})));
    const attempt = channelPreflightApi.get("t", "cha_test", "cpf_test"); const check = expect(attempt).rejects.toMatchObject({ code: "REQUEST_TIMEOUT" });
    await vi.advanceTimersByTimeAsync(10_001); await check;
  });
  it("accepts an unclaimed timeout with null Gateway metadata, not a false completed result", async () => {
    const body = { ...queuedPreflight, state: "TIMED_OUT", reason_code: "CHANNEL_PREFLIGHT_NO_EXECUTOR" };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(body)));
    await expect(channelPreflightApi.get("t", "cha_test", "cpf_test")).resolves.toEqual(body);
  });
  it("accepts Telegram token rejection specifically during the webhook read as FAIL", async () => {
    const body = { ...preflightResult, outcome: "FAIL", checks: preflightResult.checks.map((c, i) => i === 3 ? { ...c, status: "FAIL", code: "TOKEN_REJECTED", details: { presence: null, relation: "UNKNOWN" } } : i === 4 ? { ...c, status: "SKIPPED", code: "NOT_EXECUTED", details: { pending_update_count: null } } : i === 5 ? { ...c, status: "SKIPPED", code: "NOT_EXECUTED", details: { has_last_error: null, last_error_at: null } } : c) };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(body)));
    await expect(channelPreflightApi.get("t", "cha_test", "cpf_test")).resolves.toEqual(body);
  });
  it("accepts an invalid expected origin without misrepresenting an existing webhook as different", async () => {
    const body = { ...preflightResult, outcome: "FAIL", expected_public_origin: null, checks: preflightResult.checks.map((c, i) => i === 2 ? { ...c, status: "FAIL", code: "PUBLIC_ORIGIN_INVALID" } : i === 3 ? { ...c, status: "UNKNOWN", code: "WEBHOOK_COMPARISON_UNAVAILABLE", details: { presence: true, relation: "UNKNOWN" } } : c) };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(body)));
    await expect(channelPreflightApi.get("t", "cha_test", "cpf_test")).resolves.toEqual(body);
  });
  it("clears only preflight recovery on identity change and logout", () => {
    sessionStorage.clear(); sessionStorage.setItem(PREFLIGHT_STORAGE_PREFIX + "old:t:a", "old"); sessionStorage.setItem("unrelated", "keep");
    expect(establishChannelPreflightIdentity(sessionStorage, "new")).toBe(true); expect(sessionStorage.getItem(PREFLIGHT_STORAGE_PREFIX + "old:t:a")).toBeNull(); expect(sessionStorage.getItem(PREFLIGHT_IDENTITY_KEY)).toBe("new");
    sessionStorage.setItem(PREFLIGHT_STORAGE_PREFIX + "new:t:a", "new"); establishChannelPreflightIdentity(sessionStorage, "new"); expect(sessionStorage.getItem(PREFLIGHT_STORAGE_PREFIX + "new:t:a")).toBe("new");
    clearChannelPreflightPreparations(sessionStorage); expect(sessionStorage.getItem(PREFLIGHT_IDENTITY_KEY)).toBeNull(); expect(sessionStorage.getItem("unrelated")).toBe("keep"); sessionStorage.clear();
  });
  it("rejects an oversized successful response with no retained raw provider content", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response({ ...preflightResult, raw_error: "secret".repeat(12000) })));
    await expect(channelPreflightApi.get("t", "cha_test", "cpf_test")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
  });
  it("keeps invalid queued/running time projections out of the panel", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response({ ...queuedPreflight, state: "RUNNING" })));
    await expect(channelPreflightApi.get("t", "cha_test", "cpf_test")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
  });
});
