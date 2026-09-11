import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { channelApi, validateChannelAccount } from "./channel-api";
import { channelPendingKey, loadChannelPending, saveChannelPending } from "./channel-editor-state";
import { sampleChannelAccount, sampleChannelCreate } from "../test/channel-fixtures";

beforeEach(() => sessionStorage.clear());
afterEach(() => vi.restoreAllMocks());
const key = channelPendingKey("u", "t", "new");
describe("Telegram receive mode contract and recovery", () => {
  it("sends only editable mode config for create and PATCH, retaining the original key", async () => {
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async () => Response.json({ account: { ...sampleChannelAccount, config: { ...sampleChannelAccount.config, receive_mode: "long_polling" } }, route_generation: 0, distribution: "NOT_EMITTED" }));
    const config = { receive_mode: "long_polling" as const, webhook_path: "/not-editable", bot_id: "not-editable" };
    await channelApi.createAccount("t", { ...sampleChannelCreate, config }, "original-create");
    await channelApi.updateAccount("t", "a", { expected_account_revision: 3, config }, "original-mode");
    expect(JSON.parse(String(fetch.mock.calls[0][1]?.body)).config).toEqual({ receive_mode: "long_polling" });
    expect(JSON.parse(String(fetch.mock.calls[1][1]?.body))).toEqual({ expected_account_revision: 3, config: { receive_mode: "long_polling" } });
    expect(new Headers(fetch.mock.calls[1][1]?.headers).get("Idempotency-Key")).toBe("original-mode");
  });
  it("requires only Token for LP, both for Webhook, and still validates a supplied optional Secret", () => {
    const input = { ...sampleChannelCreate, config: { receive_mode: "long_polling" as const }, credentials: { "telegram.bot_token": { action: "replace" as const, value: "synthetic-token" } } };
    expect(validateChannelAccount(input)).toEqual({});
    expect(validateChannelAccount({ ...input, config: { receive_mode: "webhook" } })).toHaveProperty("credentials.telegram.webhook_secret");
    expect(validateChannelAccount({ ...input, credentials: { ...input.credentials, "telegram.webhook_secret": { action: "replace", value: "invalid secret" } } })).toHaveProperty("credentials.telegram.webhook_secret");
  });
  it("persists exact mode and optional-purpose presence without values, while retaining legacy markers", () => {
    const legacy = { operation: "createAccount", key: "original-key", secret: true, createdAt: "2026-09-07T00:00:00Z", input: { provider: "telegram", provider_account_id: "123", name: "legacy" } };
    expect(saveChannelPending(sessionStorage, key, legacy)).toBe(true);
    expect(loadChannelPending(sessionStorage, key)).toEqual(legacy);
    const modern = { ...legacy, input: { ...legacy.input, config: { receive_mode: "long_polling" }, supplied_purposes: ["telegram.bot_token"] } };
    expect(saveChannelPending(sessionStorage, key, modern)).toBe(true);
    expect(loadChannelPending(sessionStorage, key)).toEqual(modern);
    const mode = { ...legacy, operation: "updateAccount", secret: false, input: { expected_account_revision: 3, config: { receive_mode: "webhook" } } };
    expect(saveChannelPending(sessionStorage, key, mode)).toBe(true);
    expect(loadChannelPending(sessionStorage, key)).toEqual(mode);
    expect(saveChannelPending(sessionStorage, key, { ...modern, input: { ...modern.input, config: { receive_mode: "future" } } })).toBe(false);
  });
});

it("accepts missing mode only in an owner-marked historical command receipt, never current reads", async () => {
  const old = { ...sampleChannelAccount, config: { webhook_path: "/v1/telegram/a" } };
  const fetch = vi.spyOn(globalThis, "fetch");
  const body = { account: old, distribution: "NOT_EMITTED", route_generation: 0 };
  fetch.mockResolvedValueOnce(Response.json(body, { headers: { "X-Channel-Result-Contract": "webhook-v1" } }));
  expect((await channelApi.updateAccount("t", "a", { expected_account_revision: 3, name: "original" }, "old-key")).account?.account_id).toBe(old.account_id);
  fetch.mockResolvedValueOnce(Response.json(body));
  await expect(channelApi.updateAccount("t", "a", { expected_account_revision: 3, name: "original" }, "old-key")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
  fetch.mockResolvedValueOnce(Response.json({ accounts: [old] }, { headers: { "X-Channel-Result-Contract": "webhook-v1" } }));
  await expect(channelApi.listAccounts("t")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
});

it("sends legacy create with the exact original shape and rejects misuse of the interpreter", async () => {
  const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ account: sampleChannelAccount, route_generation: 0, distribution: "NOT_EMITTED" }, { headers: { "X-Channel-Result-Contract": "receive-modes-v1" } }));
  const { config: _config, ...oldInput } = sampleChannelCreate;
  await channelApi.createAccount("t", oldInput, "old-create", "webhook-v1");
  expect(JSON.parse(String(fetch.mock.calls[0][1]?.body))).toEqual(oldInput);
  expect(new Headers(fetch.mock.calls[0][1]?.headers).get("X-Channel-Create-Contract")).toBe("webhook-v1");
  expect(() => channelApi.createAccount("t", sampleChannelCreate, "old-create", "webhook-v1")).toThrow();
  expect(fetch).toHaveBeenCalledOnce();
});

it.each([undefined, "future", "", null])("rejects unconfirmed current Telegram mode %s rather than inventing LP", async (mode) => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ accounts: [{ ...sampleChannelAccount, config: { ...sampleChannelAccount.config, receive_mode: mode } }] }));
  await expect(channelApi.listAccounts("t")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
});

it("rejects a historical result header attached to a modern LP account", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ account: { ...sampleChannelAccount, config: { ...sampleChannelAccount.config, receive_mode: "long_polling" } }, distribution: "NOT_EMITTED", route_generation: 0 }, { headers: { "X-Channel-Result-Contract": "webhook-v1" } }));
  await expect(channelApi.updateAccount("t", "a", { expected_account_revision: 3, name: "n" }, "k")).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
});
