import { afterEach, describe, expect, it, vi } from "vitest";
import { channelPreflightApi, preflightError, validPreflightInput, type ChannelPreflightInput } from "./channel-preflight-api";
import { preflightReceipt } from "../test/channel-preflight-fixtures";
import { queuedWecomPreflight, wecomPreflight } from "../test/wecom-preflight-fixtures";
const input: ChannelPreflightInput = { expected_account_revision: 3, expected_connection_revision: 2, expected_bot_secret_version: 4, allow_connection_probe: true };
afterEach(() => vi.unstubAllGlobals());
const serve = (value: unknown, status = 200) => { const fetch = vi.fn().mockImplementation(async () => Response.json(value, { status })); vi.stubGlobal("fetch", fetch); return fetch; };
const read = () => channelPreflightApi.get("t", "cha_test", "cpf_test");
describe("WeCom preflight frozen public contract", () => {
  it("sends only the three original versions and explicit true confirmation", async () => {
    const fetch = serve(preflightReceipt, 202);
    await channelPreflightApi.create("t", "cha_test", { ...input, secret: "not-for-browser-transport" } as ChannelPreflightInput, "original-wecom-key");
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual(input);
    expect(fetch.mock.calls[0][1].headers["Idempotency-Key"]).toBe("original-wecom-key");
    expect(fetch.mock.calls[0][1].body).not.toContain("bot_token");
  });
  it.each([
    { expected_account_revision: 3, expected_connection_revision: 2, expected_bot_secret_version: 4 },
    { ...input, allow_connection_probe: false }, { ...input, allow_connection_probe: "true" },
    { ...input, expected_bot_secret_version: 0 }, { ...input, expected_bot_token_version: 1 },
    { expected_account_revision: 3, expected_connection_revision: 2, expected_bot_token_version: 1, allow_connection_probe: true },
  ])("rejects missing, false or cross-provider consent without fetch: %j", async (body) => {
    const fetch = serve(preflightReceipt, 202); expect(validPreflightInput(body)).toBe(false);
    await expect(channelPreflightApi.create("t", "cha_test", body as ChannelPreflightInput, "key")).rejects.toMatchObject({ code: "CHANNEL_INPUT_INVALID" });
    expect(fetch).not.toHaveBeenCalled();
  });
  it("accepts the exact three-check result without promoting delivery to PASS", async () => {
    serve(wecomPreflight); expect(await read()).toEqual(wecomPreflight); expect((await read()).checks[2].status).toBe("UNKNOWN");
  });
  it.each(["PROVIDER_NETWORK", "PROVIDER_TIMEOUT", "PROVIDER_RESPONSE_INVALID", "PROVIDER_UNAVAILABLE", "WECOM_CONNECTION_REPLACED"])("preserves %s as genuine uncertainty", async (code) => {
    const value = { ...wecomPreflight, outcome: "UNKNOWN", checks: wecomPreflight.checks.map((c, i) => i === 1 ? { ...c, status: "UNKNOWN", code, details: { authenticated: null } } : c) };
    serve(value); expect(await read()).toEqual(value);
  });
  it("accepts a rejected Secret as failed authentication, not identity mismatch", async () => {
    const value = { ...wecomPreflight, outcome: "FAIL", checks: wecomPreflight.checks.map((c, i) => i === 1 ? { ...c, status: "FAIL", code: "WECOM_AUTH_REJECTED", details: { authenticated: false } } : c) };
    serve(value); expect(await read()).toEqual(value);
  });
  it("skips authentication only when the Secret is missing", async () => {
    const value = { ...wecomPreflight, outcome: "FAIL", checks: [
      { id: "credential_configuration", status: "FAIL", code: "BOT_SECRET_MISSING", details: { bot_secret_configured: false } },
      { id: "connection_authentication", status: "SKIPPED", code: "NOT_EXECUTED", details: { authenticated: null } }, wecomPreflight.checks[2],
    ] }; serve(value); expect(await read()).toEqual(value);
  });
  it.each([
    { ...wecomPreflight, bot_token_version: 4 }, { ...wecomPreflight, expected_public_origin: "https://gateway.example" },
    { ...wecomPreflight, receive_mode: "long_polling" }, { ...wecomPreflight, diagnostic_policy: "telegram-receive-modes-v1" },
    { ...wecomPreflight, allow_connection_probe: false }, { ...wecomPreflight, effective_config_digest: undefined },
    { ...wecomPreflight, checks: [...wecomPreflight.checks].reverse() }, { ...wecomPreflight, checks: wecomPreflight.checks.slice(0, 2) },
    { ...wecomPreflight, checks: wecomPreflight.checks.map((c, i) => i === 2 ? { ...c, status: "PASS" } : c) },
    { ...wecomPreflight, checks: wecomPreflight.checks.map((c, i) => i === 1 ? { ...c, details: { authenticated: false } } : c) },
    { ...wecomPreflight, checks: wecomPreflight.checks.map((c, i) => i === 1 ? { ...c, status: "UNKNOWN", code: "PROVIDER_TIMEOUT", details: { authenticated: null } } : c) },
    { ...wecomPreflight, checks: wecomPreflight.checks.map((c, i) => i === 1 ? { ...c, status: "SKIPPED", code: "NOT_EXECUTED", details: { authenticated: null } } : c) },
    { ...wecomPreflight, checks: wecomPreflight.checks.map((c, i) => i === 0 ? { ...c, details: { bot_secret_configured: true, raw_secret: "must-not-render" } } : c) },
    { ...wecomPreflight, tenant_id: "other" }, { ...wecomPreflight, account_id: "other" },
  ])("rejects mixed providers, fake success or raw secret fields: %j", async (value) => {
    serve(value); await expect(read()).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
  });
  it("preserves queued and unclaimed timeout nulls", async () => {
    serve(queuedWecomPreflight); expect(await read()).toEqual(queuedWecomPreflight);
    const timedOut = { ...queuedWecomPreflight, state: "TIMED_OUT", reason_code: "CHANNEL_PREFLIGHT_NO_EXECUTOR" };
    serve(timedOut); expect(await read()).toEqual(timedOut);
  });
  it("consumes the API owner fixture unchanged and matches the fixed checks", async () => {
    const { readFileSync } = await import("node:fs"); const { resolve } = await import("node:path");
    const fixture = JSON.parse(readFileSync(resolve(process.cwd(), "../api/schemas/channel/v1/fixtures/preflight-wecom-view-valid.json"), "utf8")).document;
    serve(fixture);
    expect(await channelPreflightApi.get(fixture.tenant_id, fixture.account_id, fixture.preflight_id)).toEqual(fixture);
    expect(wecomPreflight.checks).toEqual(fixture.checks);
  });
  it("explains required consent without exposing backend payloads", async () => {
    serve({ error: { code: "CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED", message: "raw-secret" } }, 422);
    await read().catch((error) => { expect(preflightError(error)).toContain("明确确认连接替换影响"); expect(preflightError(error)).not.toContain("raw-secret"); });
  });
});
