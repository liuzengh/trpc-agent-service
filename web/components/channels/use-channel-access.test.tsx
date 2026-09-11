import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Session, Tenant } from "../../lib/control-api";
import { ChannelApiError, CHANNEL_TIMEOUT_MS } from "../../lib/channel-api";
import { useChannelAccess } from "./use-channel-access";

const api = vi.hoisted(() => ({ getMe: vi.fn(), getTenant: vi.fn() }));
vi.mock("../../lib/control-api", () => ({ controlApi: api }));
const session: Session = { user: { id: "u", username: "owner", display_name: "Owner", status: "ACTIVE" }, password_change_required: false };
const tenant: Tenant = { id: "t", name: "Tenant", slug: "tenant", role: "OWNER", status: "ACTIVE", created_at: "", updated_at: "" };
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>((done) => { resolve = done; }); return { promise, resolve }; }
beforeEach(() => { vi.resetAllMocks(); sessionStorage.clear(); api.getMe.mockResolvedValue(session); api.getTenant.mockResolvedValue(tenant); });
afterEach(() => { cleanup(); vi.useRealTimers(); });

describe("Channel access gate", () => {
  it("allows the real successful /me projection without an account status for an ACTIVE OWNER tenant", async () => {
    const authenticatedSession: Session = { user: { id: "u", username: "owner", display_name: "Owner" }, password_change_required: false };
    api.getMe.mockResolvedValue(authenticatedSession);
    const { result } = renderHook(() => useChannelAccess("t"));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current).toMatchObject({ userId: "u", canWrite: true, error: "" });
  });
  it.each(["DISABLED", "UNKNOWN"])("keeps explicit %s account states read-only even for an OWNER", async (status) => {
    api.getMe.mockResolvedValue({ ...session, user: { ...session.user, status } });
    const { result } = renderHook(() => useChannelAccess("t"));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.canWrite).toBe(false);
  });
  it("still denies a minimal /me projection when the tenant is only MEMBER", async () => {
    api.getMe.mockResolvedValue({ user: { id: "u", username: "member", display_name: "Member" }, password_change_required: false });
    api.getTenant.mockResolvedValue({ ...tenant, role: "MEMBER" });
    const { result } = renderHook(() => useChannelAccess("t"));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.canWrite).toBe(false);
  });
  it("keeps writes closed until both identity and tenant authority resolve", async () => {
    const read = deferred<Tenant>(); api.getTenant.mockReturnValue(read.promise);
    const { result } = renderHook(() => useChannelAccess("t"));
    expect(result.current.canWrite).toBe(false); expect(result.current.loading).toBe(true);
    await act(async () => read.resolve(tenant));
    expect(result.current).toMatchObject({ userId: "u", canWrite: true, loading: false, error: "" });
    expect(sessionStorage.getItem("channel-user:v1")).toBe("u");
  });
  it.each([
    { role: "MEMBER", status: "ACTIVE", password: false },
    { role: "OWNER", status: "SUSPENDED", password: false },
    { role: "OWNER", status: "ACTIVE", password: true },
    { role: undefined, status: "ACTIVE", password: false },
  ])("denies write access for %j", async ({ role, status, password }) => {
    api.getMe.mockResolvedValue({ ...session, password_change_required: password }); api.getTenant.mockResolvedValue({ ...tenant, role, status });
    const { result } = renderHook(() => useChannelAccess("t"));
    await waitFor(() => expect(result.current.loading).toBe(false)); expect(result.current.canWrite).toBe(false);
  });
  it.each(["identity", "tenant"])("closes writes when %s lookup fails", async (failing) => {
    (failing === "identity" ? api.getMe : api.getTenant).mockRejectedValue(new ChannelApiError(403, "FORBIDDEN", "denied"));
    const { result } = renderHook(() => useChannelAccess("t"));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.canWrite).toBe(false); expect(result.current.userId).toBe(""); expect(result.current.error).toContain("没有此渠道操作权限");
  });
  it("ignores a late OWNER lookup after changing to a MEMBER tenant", async () => {
    const old = deferred<Tenant>(); api.getTenant.mockReturnValueOnce(old.promise).mockResolvedValueOnce({ ...tenant, id: "other", role: "MEMBER" });
    const { result, rerender } = renderHook(({ id }) => useChannelAccess(id), { initialProps: { id: "t" } });
    rerender({ id: "other" }); await waitFor(() => expect(result.current.loading).toBe(false)); await act(async () => old.resolve(tenant));
    expect(result.current.canWrite).toBe(false); expect(api.getTenant).toHaveBeenLastCalledWith("other");
  });
  it("closes write controls while explicitly rechecking a previous OWNER", async () => {
    const next = deferred<Tenant>(); api.getTenant.mockResolvedValueOnce(tenant).mockReturnValueOnce(next.promise);
    const { result } = renderHook(() => useChannelAccess("t")); await waitFor(() => expect(result.current.canWrite).toBe(true));
    act(() => result.current.reload()); expect(result.current.canWrite).toBe(false); expect(result.current.loading).toBe(true);
    await act(async () => next.resolve({ ...tenant, role: "MEMBER" })); expect(result.current.canWrite).toBe(false);
  });
  it("immediately closes writes after a denied command", async () => {
    const { result } = renderHook(() => useChannelAccess("t")); await waitFor(() => expect(result.current.canWrite).toBe(true));
    act(() => result.current.denyWrites()); expect(result.current.canWrite).toBe(false);
  });
  it("does not let an earlier role lookup override a newer denied write", async () => {
    const earlier = deferred<Tenant>(); api.getTenant.mockReturnValue(earlier.promise);
    const { result } = renderHook(() => useChannelAccess("t"));
    act(() => result.current.denyWrites()); await act(async () => earlier.resolve(tenant));
    expect(result.current.canWrite).toBe(false); expect(result.current.loading).toBe(false);
    api.getTenant.mockResolvedValue(tenant); act(() => result.current.reload()); await waitFor(() => expect(result.current.canWrite).toBe(true));
  });
  it("requires explicit ACTIVE tenant status instead of permitting an unknown status", async () => {
    api.getTenant.mockResolvedValue({ ...tenant, status: "UNKNOWN" }); const { result } = renderHook(() => useChannelAccess("t"));
    await waitFor(() => expect(result.current.loading).toBe(false)); expect(result.current.canWrite).toBe(false);
  });
  it("ends a stalled authority lookup after the bounded read timeout", async () => {
    vi.useFakeTimers(); api.getTenant.mockReturnValue(new Promise(() => {})); const { result } = renderHook(() => useChannelAccess("t"));
    await act(async () => { await vi.advanceTimersByTimeAsync(CHANNEL_TIMEOUT_MS + 1); });
    expect(result.current.canWrite).toBe(false); expect(result.current.loading).toBe(false); expect(result.current.error).not.toBe("");
  });
});
