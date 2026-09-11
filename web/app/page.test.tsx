import { beforeEach, describe, expect, it, vi } from "vitest";
import Home from "./page";
const state = vi.hoisted(() => ({ get: vi.fn(), redirect: vi.fn((url: string) => { throw new Error(`REDIRECT ${url}`); }) }));
vi.mock("next/headers", () => ({ cookies: async () => ({ get: state.get }) }));
vi.mock("next/navigation", () => ({ redirect: state.redirect }));
beforeEach(() => { vi.clearAllMocks(); vi.unstubAllEnvs(); state.get.mockReturnValue(undefined); });
describe("management service entry", () => {
  it("opens login immediately without calling the backend when no session cookie exists", async () => {
    await expect(Home()).rejects.toThrow("REDIRECT /login");
    expect(state.get).toHaveBeenCalledWith("control_session");
  });
  it("delegates an existing cookie to the authoritative role-aware session check", async () => {
    state.get.mockReturnValue({ value: "opaque-fixture" });
    await expect(Home()).rejects.toThrow("REDIRECT /console");
  });
  it("treats an empty cookie as signed out", async () => {
    state.get.mockReturnValue({ value: "" }); await expect(Home()).rejects.toThrow("REDIRECT /login");
  });
  it("supports the configured backend cookie name", async () => {
    vi.stubEnv("CONTROL_SESSION_COOKIE_NAME", "custom_session");
    await expect(Home()).rejects.toThrow("REDIRECT /login");
    expect(state.get).toHaveBeenCalledWith("custom_session");
  });
});
