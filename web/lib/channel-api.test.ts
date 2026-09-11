import { afterEach, describe, expect, it, vi } from "vitest";
import {
  channelApi, channelError, channelRead, ChannelApiError, CHANNEL_TIMEOUT_MS, credentialPurposes,
  isChannelUncertain, validateChannelAccount, validateCredential, type CreateAccountInput, type UpdateCredentialInput,
} from "./channel-api";
import { sampleChannelAccount as account, sampleChannelBinding as binding, sampleChannelDetails as details, sampleChannelCreate as create } from "../test/channel-fixtures";
const command = { account, binding, route_generation: 1, event_id: "evt_test", distribution: "PENDING" };
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
function routeMock(url: RequestInfo | URL, init?: RequestInit): Response {
  const path = String(url).split("?")[0];
  if (init?.method) return Response.json(command, { status: path.endsWith("channel-accounts") || path.endsWith("channel-bindings") ? 201 : 200 });
  if (path.endsWith("channel-accounts")) return Response.json({ accounts: [account], next_cursor: account.account_id });
  if (path.endsWith("channel-bindings")) return Response.json({ bindings: [binding] });
  return Response.json(path.includes("channel-accounts") ? details : { binding, route_generation: 1, distribution: "PUBLISHED", gateway_application: "UNKNOWN" });
}
describe("Channel public API", () => {
  it("maps all eleven routes, seven keyed writes, cursor paging and no-store transport", async () => {
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (url, init) => routeMock(url, init));
    const tenant = "tenant/a", id = "account b", bid = "binding c";
    const page = await channelApi.listAccounts(tenant, "cursor/d", 25); expect(page.accounts[0].account_id).toBe(account.account_id);
    await channelApi.getAccount(tenant, id); await channelApi.createAccount(tenant, create, "create-original");
    await channelApi.updateAccount(tenant, id, { expected_account_revision: 3, name: "n" }, "metadata-original");
    await channelApi.updateCredential(tenant, id, "telegram.bot_token", { expected_account_revision: 3, expected_credential_version: 1, action: "replace", value: "synthetic" }, "credential-original");
    await channelApi.setAccountEnabled(tenant, id, { expected_account_revision: 3, enabled: false }, "account-original");
    await channelApi.listBindings(tenant); await channelApi.getBinding(tenant, bid);
    await channelApi.createBinding(tenant, { account_id: id, target: binding.target }, "binding-original");
    await channelApi.setBindingTarget(tenant, bid, { expected_binding_revision: 2, target: binding.target }, "target-original");
    await channelApi.setBindingEnabled(tenant, bid, { expected_binding_revision: 2, enabled: true }, "enabled-original");
    const base = "/api/control/v1/tenants/tenant%2Fa";
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      `${base}/channel-accounts?cursor=cursor%2Fd&page_size=25`, `${base}/channel-accounts/account%20b`, `${base}/channel-accounts`,
      `${base}/channel-accounts/account%20b`, `${base}/channel-accounts/account%20b/credentials/telegram.bot_token/update`, `${base}/channel-accounts/account%20b/enabled`,
      `${base}/channel-bindings`, `${base}/channel-bindings/binding%20c`, `${base}/channel-bindings`, `${base}/channel-bindings/binding%20c/target`, `${base}/channel-bindings/binding%20c/enabled`,
    ]);
    for (const [, init] of fetch.mock.calls) expect(init).toMatchObject({ credentials: "include", cache: "no-store", signal: expect.any(AbortSignal) });
    const writes = fetch.mock.calls.filter(([, init]) => init?.method);
    expect(writes).toHaveLength(7);
    expect(writes.map(([, init]) => new Headers(init?.headers).get("Idempotency-Key"))).toEqual(["create-original", "metadata-original", "credential-original", "account-original", "binding-original", "target-original", "enabled-original"]);
    expect(writes[1][1]?.method).toBe("PATCH");
    for (const [url] of writes) expect(String(url)).not.toContain("?");
  });
  it("constructs fresh allowlisted mutation bodies and never copies read-only config or nested target internals", async () => {
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async () => Response.json(command));
    const extra = { config: { receive_mode: "webhook" as const, bot_id: "not-editable" }, enabled: true, tenant_id: "wrong", credentials: { ...create.credentials, "wecom.bot_secret": { action: "replace" as const, value: "other-provider-secret" } } };
    await channelApi.createAccount("t", { ...create, ...extra }, "k1");
    await channelApi.updateAccount("t", "a", { expected_account_revision: 3, name: "n", ...extra }, "k2");
    await channelApi.setAccountEnabled("t", "a", { ...extra, expected_account_revision: 3, enabled: false }, "k3");
    const injectedTarget = { ...binding.target, latest: true, secret: "ignored" };
    await channelApi.createBinding("t", { account_id: "a", target: injectedTarget, ...extra }, "k4");
    await channelApi.setBindingTarget("t", "b", { expected_binding_revision: 2, target: injectedTarget }, "k5");
    await channelApi.setBindingEnabled("t", "b", { ...extra, expected_binding_revision: 2, enabled: true }, "k6");
    const bodies = fetch.mock.calls.map(([, init]) => JSON.parse(String(init?.body)));
    expect(bodies[0]).toEqual(create);
    expect(bodies[1]).toEqual({ expected_account_revision: 3, name: "n", config: { receive_mode: "webhook" } });
    expect(bodies[2]).toEqual({ expected_account_revision: 3, enabled: false });
    expect(bodies[3]).toEqual({ account_id: "a", target: { deployment_id: "d", revision_number: 1 } });
    expect(bodies[4]).toEqual({ expected_binding_revision: 2, target: { deployment_id: "d", revision_number: 1 } });
    expect(bodies[5]).toEqual({ expected_binding_revision: 2, enabled: true });
  });
  it.each(["keep", "clear"] as const)("never sends a value with credential %s", async (action) => {
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async () => Response.json(command));
    const malformed = { expected_account_revision: 3, expected_credential_version: 1, action, value: "synthetic-must-not-send", config: {} } as unknown as UpdateCredentialInput;
    await channelApi.updateCredential("t", "a", "telegram.bot_token", malformed, "k");
    expect(JSON.parse(String(fetch.mock.calls[0][1]?.body))).toEqual({ expected_account_revision: 3, expected_credential_version: 1, action });
  });
  it.each(["", "line\n", "contains space", "x".repeat(129)])("rejects invalid idempotency keys before fetch", (key) => {
    const fetch = vi.spyOn(globalThis, "fetch");
    expect(() => channelApi.createAccount("t", create, key)).toThrow(ChannelApiError); expect(fetch).not.toHaveBeenCalled();
  });
  it.each([200, 201])("accepts status %s without assuming replay or generating another key", async (status) => {
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async () => Response.json(command, { status }));
    expect((await channelApi.createAccount("t", create, "original")).account?.account_id).toBe(account.account_id);
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it.each([401, 403, 409, 422, 500, 503])("retains HTTP %s and sanitized code/field without leaking server message", async (status) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ error: { code: "CHANNEL_REVISION_CONFLICT", field: "/expected_account_revision", message: "SERVER_SECRET_VALUE" } }, { status }));
    let failure: unknown;
    try { await channelApi.updateAccount("t", "a", { expected_account_revision: 3, name: "n" }, "k"); } catch (error) { failure = error; }
    expect(failure).toMatchObject({ status, code: "CHANNEL_REVISION_CONFLICT", field: "/expected_account_revision" });
    expect(channelError(failure)).not.toContain("SERVER_SECRET_VALUE");
    if (status === 401) expect(channelError(failure)).toContain("重新登录");
    if (status === 403) expect(channelError(failure)).toContain("OWNER");
  });
  it("distinguishes unregistered non-JSON 404 from a structured object-not-found response", async () => {
    const fetch = vi.spyOn(globalThis, "fetch");
    fetch.mockResolvedValueOnce(new Response("404 page not found", { status: 404 }));
    await expect(channelApi.listAccounts("t")).rejects.toMatchObject({ status: 404, code: "CHANNEL_MODULE_UNAVAILABLE" });
    fetch.mockResolvedValueOnce(Response.json({ error: { code: "CHANNEL_ACCOUNT_NOT_FOUND" } }, { status: 404 }));
    await expect(channelApi.getAccount("t", "a")).rejects.toMatchObject({ status: 404, code: "CHANNEL_ACCOUNT_NOT_FOUND" });
  });
  it.each([{}, null, { accounts: {} }, { accounts: [null] }, { accounts: [{ account_id: "a" }] }])("rejects malformed successful account pages", async (body) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(body));
    await expect(channelApi.listAccounts("t")).rejects.toMatchObject({ status: 200, code: "INVALID_RESPONSE" });
  });
  it("rejects malformed binding pages, detail observations and successful command receipts", async () => {
    const fetch = vi.spyOn(globalThis, "fetch");
    fetch.mockResolvedValueOnce(Response.json({ bindings: {} }));
    await expect(channelApi.listBindings("t")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
    fetch.mockResolvedValueOnce(Response.json({ ...details, observations: null }));
    await expect(channelApi.getAccount("t", "a")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
    fetch.mockResolvedValueOnce(Response.json({ ...details, account: {} }));
    await expect(channelApi.getAccount("t", "a")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
    fetch.mockResolvedValueOnce(Response.json({ ...command, route_generation: -1 }, { status: 201 }));
    await expect(channelApi.createAccount("t", create, "original")).rejects.toMatchObject({ status: 201, code: "INVALID_RESPONSE" });
    fetch.mockResolvedValueOnce(Response.json({ route_generation: 1, distribution: "PENDING" }, { status: 201 }));
    await expect(channelApi.createBinding("t", { account_id: "a", target: binding.target }, "original")).rejects.toMatchObject({ status: 201, code: "INVALID_RESPONSE" });
  });
  it("bounds a body that never finishes, aborts its transport, and never retries", async () => {
    vi.useFakeTimers(); let signal: AbortSignal | null | undefined;
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => { signal = init?.signal; return { ok: true, status: 201, json: () => new Promise(() => {}) } as Response; });
    const assertion = expect(channelApi.createAccount("t", create, "original")).rejects.toMatchObject({ status: 0, code: "REQUEST_TIMEOUT" });
    await vi.advanceTimersByTimeAsync(CHANNEL_TIMEOUT_MS); await assertion;
    expect(signal?.aborted).toBe(true); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it("bounds owner queries independently and clears timers for settled reads", async () => {
    vi.useFakeTimers(); const assertion = expect(channelRead(new Promise(() => {}))).rejects.toMatchObject({ code: "REQUEST_TIMEOUT" });
    await vi.advanceTimersByTimeAsync(CHANNEL_TIMEOUT_MS); await assertion;
    expect(await channelRead(Promise.resolve(42))).toBe(42); expect(vi.getTimerCount()).toBe(0);
  });
  it("classifies uncertain failures without treating known 4xx rejection as replayable success", async () => {
    for (const error of [new ChannelApiError(0, "REQUEST_TIMEOUT", ""), new ChannelApiError(503, "CHANNEL_DEPENDENCY_UNAVAILABLE", ""), new ChannelApiError(201, "INVALID_RESPONSE", ""), new TypeError("fetch failed")]) expect(isChannelUncertain(error)).toBe(true);
    for (const status of [400, 401, 403, 404, 409, 422]) expect(isChannelUncertain(new ChannelApiError(status, "REJECTED", ""))).toBe(false);
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new TypeError("secret-like browser error"));
    await expect(channelApi.getAccount("t", "a")).rejects.toMatchObject({ code: "NETWORK_ERROR" });
  });
});
describe("Channel provider validation", () => {
  it("accepts complete provider-specific inputs and never normalizes secret values", () => {
    expect(validateChannelAccount(create)).toEqual({});
    expect(validateChannelAccount({ provider: "wecom", provider_account_id: "bot-01", name: "企微", credentials: { "wecom.bot_secret": { action: "replace", value: " surrounding spaces " } } })).toEqual({});
    // Go strings.TrimSpace / unicode.IsSpace use Unicode White_Space, not JavaScript BOM trimming.
    expect(validateChannelAccount({ provider: "wecom", provider_account_id: "bot\ufeff01", name: "\ufeff", credentials: { "wecom.bot_secret": { action: "replace", value: "synthetic" } } })).toEqual({});
    expect(credentialPurposes("telegram")).toEqual(["telegram.bot_token", "telegram.webhook_secret"]);
    const purposes = credentialPurposes("wecom"); purposes.push("telegram.bot_token"); expect(credentialPurposes("wecom")).toEqual(["wecom.bot_secret"]);
  });
  it.each(["", "@username", "000", "123\n", "1 2", "a123"])("rejects invalid Telegram physical identity %j", (provider_account_id) => {
    expect(validateChannelAccount({ ...create, provider_account_id })).toHaveProperty("provider_account_id");
  });
  it.each(["", "a\n", "中文", "a b", "a/b", "a".repeat(257)])("rejects invalid webhook secret %j", (value) => expect(validateCredential("telegram.webhook_secret", value)).not.toBe(""));
  it("checks UTF-8 byte bounds and code-point metadata lengths", () => {
    expect(validateCredential("telegram.webhook_secret", "a".repeat(256))).toBe("");
    expect(validateCredential("wecom.bot_secret", "中".repeat(5461))).toBe("");
    expect(validateCredential("wecom.bot_secret", "中".repeat(5462))).not.toBe("");
    expect(validateCredential("wecom.bot_secret", "\ud800")).not.toBe("");
    expect(validateChannelAccount({ ...create, name: "😀".repeat(128) })).not.toHaveProperty("name");
    expect(validateChannelAccount({ ...create, name: "😀".repeat(129) })).toHaveProperty("name");
    expect(validateChannelAccount({ ...create, description: "中".repeat(4097) })).toHaveProperty("description");
    const bad = { ...create, provider: "wecom", provider_account_id: "中".repeat(342), credentials: {} } as CreateAccountInput;
    expect(validateChannelAccount(bad)).toHaveProperty("provider_account_id");
    expect(validateChannelAccount(bad)).toHaveProperty("credentials.wecom.bot_secret");
  });
  it("reports missing and mismatched provider purposes", () => {
    expect(validateChannelAccount({ ...create, credentials: {} })["credentials.telegram.bot_token"]).toBeTruthy();
    expect(validateChannelAccount({ ...create, credentials: { ...create.credentials, "wecom.bot_secret": { action: "replace", value: "synthetic" } } }).credentials).toBeTruthy();
  });
});
