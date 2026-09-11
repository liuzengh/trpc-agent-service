import { beforeEach, describe, expect, it } from "vitest";
import {
  channelHref, channelTargetQuery, readChannelTarget, channelPendingKey, saveChannelPending, loadChannelPending,
  clearChannelPending, clearChannelPreparations, establishChannelIdentity, CHANNEL_PENDING_PREFIX, CHANNEL_IDENTITY_KEY, type ChannelPending,
} from "./channel-editor-state";
const createdAt = "2026-09-06T12:00:00.000Z";
const pending = (operation: string, input: Record<string, unknown>, secret = false): ChannelPending => ({ operation, key: "original-key", input, secret, createdAt });
const key = channelPendingKey("u", "t", "a");
beforeEach(() => sessionStorage.clear());
describe("Channel fixed target URLs", () => {
  it("encodes identities and roundtrips only a fixed selector", () => {
    expect(channelHref("tenant/a", "account b")).toBe("/tenants/tenant%2Fa/channels/account%20b");
    expect(channelHref("t")).toBe("/tenants/t/channels");
    expect(channelTargetQuery({ deployment_id: "d", revision_number: 3 })).toBe("deployment_id=d&revision_number=3");
    expect(readChannelTarget("?deployment_id=d&revision_number=3")).toEqual({ deployment_id: "d", revision_number: 3 });
  });
  it.each(["", "deployment_id=d", "revision_number=1", "deployment_id=d&revision_number=0", "deployment_id=d&revision_number=-1", "deployment_id=d&revision_number=1.2", "deployment_id=d&revision_number=1e2", "deployment_id=d&revision_number=01", "deployment_id=d&revision_number=latest", "deployment_id=d&revision_number=9007199254740992", "deployment_id=d&revision_number=1&revision_number=1", "deployment_id=d&deployment_id=x&revision_number=1", "deployment_id=d%0A&revision_number=1", "deployment_id=d&revision_number=1%0A"])("rejects partial, duplicate, unsafe or malformed selectors %s", (query) => expect(readChannelTarget(new URLSearchParams(query))).toBeNull());
  it("does not coerce an invalid target into a navigable query", () => expect(channelTargetQuery({ deployment_id: "d", revision_number: NaN })).toBe(""));
});
describe("Channel write-intent storage", () => {
  it.each([
    pending("updateAccount", { expected_account_revision: 3, name: " new name " }),
    pending("setAccountEnabled", { expected_account_revision: 3, enabled: false }),
    pending("createBinding", { account_id: "a", target: { deployment_id: "d", revision_number: 2 } }),
    pending("setBindingTarget", { expected_binding_revision: 2, target: { deployment_id: "d", revision_number: 1 } }),
    pending("setBindingEnabled", { expected_binding_revision: 2, enabled: true }),
    pending("updateCredential", { purpose: "telegram.bot_token", expected_account_revision: 3, expected_credential_version: 1, action: "keep" }),
    pending("updateCredential", { purpose: "wecom.bot_secret", expected_account_revision: 3, expected_credential_version: 1, action: "clear" }),
  ])("roundtrips a closed non-secret $operation intent without changing the original key or Expected", (state) => {
    expect(saveChannelPending(sessionStorage, key, state)).toBe(true); expect(loadChannelPending(sessionStorage, key)).toEqual(state);
  });
  it.each([
    pending("createAccount", { provider: "telegram", provider_account_id: "00123", name: "原名称", description: "原描述" }, true),
    pending("updateCredential", { purpose: "telegram.bot_token", expected_account_revision: 3, expected_credential_version: 1, action: "replace" }, true),
  ])("stores only a non-secret $operation recovery marker", (state) => {
    expect(saveChannelPending(sessionStorage, key, state)).toBe(true); expect(loadChannelPending(sessionStorage, key)).toEqual(state);
  });
  it.each([
    pending("createAccount", { provider: "telegram", provider_account_id: "123", name: "name", credentials: { "telegram.bot_token": { action: "replace", value: "DO_NOT_PERSIST" } } }, true),
    pending("updateCredential", { purpose: "telegram.bot_token", expected_account_revision: 1, expected_credential_version: 1, action: "replace", value: "DO_NOT_PERSIST" }, true),
    pending("setBindingTarget", { expected_binding_revision: 1, target: { deployment_id: "d", revision_number: 1, value: "DO_NOT_PERSIST" } }),
    pending("setAccountEnabled", { expected_account_revision: 1, enabled: false, diagnostics: { token: "DO_NOT_PERSIST" } }),
    pending("updateAccount", { expected_account_revision: 1, name: { value: "DO_NOT_PERSIST" } }),
    pending("createAccount", { provider: "telegram", provider_account_id: "123", name: "name" }, false),
    pending("updateCredential", { purpose: "telegram.bot_token", expected_account_revision: 1, expected_credential_version: 1, action: "replace" }, false),
  ])("rejects secret or mismatched $operation persistence even when secret=true", (state) => {
    expect(saveChannelPending(sessionStorage, key, state)).toBe(false); expect(sessionStorage.getItem(key)).toBeNull();
    sessionStorage.setItem(key, JSON.stringify(state)); expect(loadChannelPending(sessionStorage, key)).toBeNull(); expect(sessionStorage.getItem(key)).toBeNull();
  });
  it.each([
    pending("unknownOperation", {}), pending("setAccountEnabled", { expected_account_revision: 0, enabled: false }),
    pending("setBindingTarget", { expected_binding_revision: 1, target: { deployment_id: "d", revision_number: 9007199254740992 } }),
    { ...pending("setAccountEnabled", { expected_account_revision: 1, enabled: false }), key: "bad\n" },
    { ...pending("setAccountEnabled", { expected_account_revision: 1, enabled: false }), createdAt: "invalid" },
  ])("rejects malformed $operation metadata", (state) => expect(saveChannelPending(sessionStorage, key, state)).toBe(false));
  it("does not overwrite an existing valid pending operation when another input is rejected", () => {
    const state = pending("setAccountEnabled", { expected_account_revision: 3, enabled: true });
    expect(saveChannelPending(sessionStorage, key, state)).toBe(true);
    expect(saveChannelPending(sessionStorage, key, pending("setAccountEnabled", { value: "secret" }))).toBe(false);
    expect(loadChannelPending(sessionStorage, key)).toEqual(state);
  });
  it("clears only channel data on identity change and logout, leaving other feature state", () => {
    const state = pending("setAccountEnabled", { expected_account_revision: 3, enabled: true });
    expect(establishChannelIdentity(sessionStorage, "u")).toBe(true); saveChannelPending(sessionStorage, key, state);
    sessionStorage.setItem("deployment-preparation:v1:u:t:d", "preserve");
    expect(establishChannelIdentity(sessionStorage, "u")).toBe(true); expect(loadChannelPending(sessionStorage, key)).toEqual(state);
    establishChannelIdentity(sessionStorage, "different"); expect(loadChannelPending(sessionStorage, key)).toBeNull();
    expect(sessionStorage.getItem(CHANNEL_IDENTITY_KEY)).toBe("different");
    saveChannelPending(sessionStorage, channelPendingKey("different", "t", "a"), state); clearChannelPreparations(sessionStorage);
    expect(sessionStorage.getItem(CHANNEL_IDENTITY_KEY)).toBeNull(); expect(Object.keys(sessionStorage).some((key) => key.startsWith(CHANNEL_PENDING_PREFIX))).toBe(false);
    expect(sessionStorage.getItem("deployment-preparation:v1:u:t:d")).toBe("preserve");
  });
  it("isolates user, tenant and object key components and only clears a valid namespace key", () => {
    expect(channelPendingKey("u:x", "t", "a")).not.toBe(channelPendingKey("u", "x:t", "a"));
    const state = pending("setAccountEnabled", { expected_account_revision: 1, enabled: true });
    expect(saveChannelPending(sessionStorage, "outside", state)).toBe(false);
    sessionStorage.setItem("outside", "preserve"); clearChannelPending(sessionStorage, "outside"); expect(sessionStorage.getItem("outside")).toBe("preserve");
    saveChannelPending(sessionStorage, key, state); clearChannelPending(sessionStorage, key); expect(loadChannelPending(sessionStorage, key)).toBeNull();
  });
  it("handles corrupted, oversized or blocked browser storage without trapping the console", () => {
    sessionStorage.setItem(key, "{not-json"); expect(loadChannelPending(sessionStorage, key)).toBeNull();
    sessionStorage.setItem(key, "x".repeat(21_000)); expect(loadChannelPending(sessionStorage, key)).toBeNull();
    const denied = new Proxy({} as Storage, { get() { throw new Error("storage blocked"); } });
    expect(establishChannelIdentity(denied, "u")).toBe(false);
    expect(saveChannelPending(denied, key, pending("setAccountEnabled", { expected_account_revision: 1, enabled: true }))).toBe(false);
    expect(loadChannelPending(denied, key)).toBeNull(); expect(() => clearChannelPreparations(denied)).not.toThrow();
  });
});
