import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChannelPreflightApiError, PREFLIGHT_STORAGE_PREFIX } from "../../lib/channel-preflight-api";
import { preflightReceipt, preflightResult } from "../../test/channel-preflight-fixtures";
import { queuedWecomPreflight, wecomAccount, wecomPreflight } from "../../test/wecom-preflight-fixtures";
import { ChannelPreflightPanel } from "./preflight-panel";
const api = vi.hoisted(() => ({ create: vi.fn(), get: vi.fn() }));
vi.mock("../../lib/channel-preflight-api", async (original) => ({ ...await original<typeof import("../../lib/channel-preflight-api")>(), channelPreflightApi: api }));
const props = { account: wecomAccount, userId: "u", canWrite: true, denyWrites: vi.fn() };
const storageKey = PREFLIGHT_STORAGE_PREFIX + "u:t:cha_test";
const iso = (offset: number) => new Date(Date.now() + offset).toISOString();
const completed = () => ({ ...wecomPreflight, requested_at: iso(-2000), job_deadline_at: iso(118000), started_at: iso(-1500), checked_at: iso(-1000), expires_at: iso(299000) });
const queued = () => ({ ...queuedWecomPreflight, requested_at: iso(-1000), job_deadline_at: iso(119000) });
const originalInput = { expected_account_revision: 3, expected_connection_revision: 2, expected_bot_secret_version: 4, allow_connection_probe: true };
async function prepare() { fireEvent.click(await screen.findByRole("button", { name: "开始连接诊断" })); return screen.getByRole("dialog"); }
function confirm() { const dialog = within(screen.getByRole("dialog")); fireEvent.click(dialog.getByRole("checkbox")); fireEvent.click(dialog.getByRole("button", { name: /同意影响/ })); }
beforeEach(() => { vi.resetAllMocks(); sessionStorage.clear(); api.create.mockResolvedValue({ ...preflightReceipt, requested_at: iso(-1000), job_deadline_at: iso(119000) }); api.get.mockResolvedValue(completed()); });
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers(); });
describe("WeCom short connection diagnostic", () => {
  it("shows Secret version and disruptive semantics without making requests", async () => {
    render(<ChannelPreflightPanel {...props} />);
    expect(await screen.findByRole("button", { name: "开始连接诊断" })).toBeEnabled();
    expect(screen.getByText("v4 · 已配置")).toBeInTheDocument(); expect(screen.getByText(/这不是只读查询/)).toBeInTheDocument();
    expect(screen.queryByText("Telegram 接入预检")).not.toBeInTheDocument(); expect(api.create).not.toHaveBeenCalled(); expect(api.get).not.toHaveBeenCalled();
  });
  it("requires an unchecked explicit confirmation and allows cancellation without a pending request", async () => {
    render(<ChannelPreflightPanel {...props} />); const dialog = within(await prepare());
    expect(dialog.getByRole("checkbox")).not.toBeChecked(); expect(dialog.getByRole("button", { name: "同意影响并开始诊断" })).toBeDisabled();
    expect(dialog.getByText(/账户在本平台停用不代表其他客户端已断开/)).toBeInTheDocument();
    expect(dialog.getByText(/不产生 Run/)).toBeInTheDocument(); expect(api.create).not.toHaveBeenCalled(); expect(sessionStorage.getItem(storageKey)).toBeNull();
    fireEvent.click(dialog.getByRole("button", { name: "取消" })); expect(screen.queryByRole("dialog")).not.toBeInTheDocument(); expect(api.create).not.toHaveBeenCalled();
  });
  it("sends true consent with frozen versions only after confirmation and separates three facts", async () => {
    render(<ChannelPreflightPanel {...props} />); await prepare(); confirm();
    await screen.findByText("诊断完成 · 连接认证通过");
    expect(api.create).toHaveBeenCalledWith("t", "cha_test", originalInput, expect.any(String), expect.any(AbortSignal)); expect(api.create).toHaveBeenCalledTimes(1);
    expect(screen.getAllByTestId("preflight-check")).toHaveLength(3); expect(screen.getByText("真实消息与 Agent 回复")).toBeInTheDocument();
    expect(screen.getByText(/不代表持续在线、收到真实消息或 Agent 已回复/)).toBeInTheDocument();
    expect(screen.getByText(/COMPLETED 只代表诊断任务结束/)).toBeInTheDocument(); expect(screen.queryByText("检查完成 · 配置检查通过")).not.toBeInTheDocument();
    expect(screen.getByText("r2 · Bot Secret v4")).toBeInTheDocument();
  });
  it("does not auto-disable an enabled account", async () => {
    render(<ChannelPreflightPanel {...props} account={{ ...wecomAccount, enabled: true }} />);
    expect(await screen.findByRole("button", { name: "开始连接诊断" })).toBeDisabled(); expect(screen.getByText(/请先在账户接入操作中单独确认停用/)).toBeInTheDocument(); expect(api.create).not.toHaveBeenCalled();
  });
  it("lets a MEMBER read the shared task without any probe action", async () => {
    render(<ChannelPreflightPanel {...props} canWrite={false} initialPreflightId="cpf_test" />); await screen.findByText("诊断完成 · 连接认证通过");
    expect(screen.queryByRole("button", { name: /开始连接诊断|重新诊断连接/ })).not.toBeInTheDocument(); expect(api.create).not.toHaveBeenCalled(); expect(api.get).toHaveBeenCalledTimes(1);
  });
  it.each(["revision", "credential", "enabled", "owner", "blocked"])("invalidates an open confirmation when %s changes", async (change) => {
    const view = render(<ChannelPreflightPanel {...props} />); await prepare(); fireEvent.click(screen.getByRole("checkbox"));
    const next = { ...props, account: { ...wecomAccount } };
    if (change === "revision") next.account.account_revision++;
    if (change === "credential") next.account.credentials = [{ purpose: "wecom.bot_secret", configured: true, credential_version: 5 }];
    if (change === "enabled") next.account.enabled = true;
    if (change === "owner") next.canWrite = false;
    view.rerender(<ChannelPreflightPanel {...next} blocked={change === "blocked"} />);
    expect(screen.getByRole("button", { name: "同意影响并开始诊断" })).toBeDisabled(); expect(screen.getByText(/操作资格已变化/)).toBeInTheDocument(); expect(api.create).not.toHaveBeenCalled();
  });
  it("reconfirms an uncertain POST but preserves the original key, Secret version and consent", async () => {
    api.create.mockRejectedValueOnce(new ChannelPreflightApiError(0, "REQUEST_TIMEOUT")); const view = render(<ChannelPreflightPanel {...props} />); await prepare(); confirm();
    await screen.findByRole("button", { name: "使用原请求重试确认" }); const first = api.create.mock.calls[0];
    view.rerender(<ChannelPreflightPanel {...props} account={{ ...wecomAccount, account_revision: 5 }} />);
    fireEvent.click(screen.getByRole("button", { name: "使用原请求重试确认" })); expect(api.create).toHaveBeenCalledTimes(1); expect(screen.getByRole("checkbox")).not.toBeChecked();
    confirm(); await screen.findByText("诊断完成 · 连接认证通过"); expect(api.create.mock.calls[1].slice(0, 4)).toEqual(first.slice(0, 4));
  });
  it("restores a confirmed pending marker without automatically probing", async () => {
    sessionStorage.setItem(storageKey, JSON.stringify({ kind: "pending", key: "original", input: originalInput, createdAt: iso(-1000) }));
    render(<ChannelPreflightPanel {...props} />); fireEvent.click(await screen.findByRole("button", { name: "使用原请求重试确认" })); expect(api.create).not.toHaveBeenCalled();
    confirm(); await screen.findByText("诊断完成 · 连接认证通过"); expect(api.create.mock.calls[0].slice(0, 4)).toEqual(["t", "cha_test", originalInput, "original"]);
  });
  it.each([{ ...originalInput, allow_connection_probe: false }, { expected_account_revision: 3, expected_connection_revision: 2, expected_bot_token_version: 1 }])("blocks malformed or cross-provider recovery instead of inventing consent", async (input) => {
    sessionStorage.setItem(storageKey, JSON.stringify({ kind: "pending", key: "original", input, createdAt: iso(-1000) })); render(<ChannelPreflightPanel {...props} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("恢复记录损坏"); expect(screen.getByRole("button", { name: "开始连接诊断" })).toBeDisabled(); expect(api.create).not.toHaveBeenCalled();
  });
  it("only repeats GET after acceptance, even if the initial read fails", async () => {
    api.get.mockRejectedValueOnce(new ChannelPreflightApiError(503, "CHANNEL_DEPENDENCY_UNAVAILABLE")); render(<ChannelPreflightPanel {...props} />); await prepare(); confirm();
    const reread = await screen.findByRole("button", { name: "重新读取预检结果" }); await act(async () => {}); fireEvent.click(reread);
    await screen.findByText("诊断完成 · 连接认证通过"); expect(api.create).toHaveBeenCalledTimes(1); expect(api.get).toHaveBeenCalledTimes(2);
  });
  it("marks Secret rotation stale without rewriting the historical result", async () => {
    const view = render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />); await screen.findByText("诊断完成 · 连接认证通过");
    view.rerender(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" account={{ ...wecomAccount, credentials: [{ purpose: "wecom.bot_secret", configured: true, credential_version: 5 }] }} />);
    expect(screen.getByText("结果已失效")).toBeInTheDocument(); expect(screen.getByText("r2 · Bot Secret v4")).toBeInTheDocument(); expect(api.get).toHaveBeenCalledTimes(1);
  });
  it("renders replaced connections as unknown, never Agent success", async () => {
    api.get.mockResolvedValue({ ...completed(), outcome: "UNKNOWN", checks: wecomPreflight.checks.map((c, i) => i === 1 ? { ...c, status: "UNKNOWN", code: "WECOM_CONNECTION_REPLACED", details: { authenticated: null } } : c) });
    render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />); await screen.findByText(/诊断连接被同一 Bot 的其他连接替换/); expect(screen.queryByText("诊断完成 · 连接认证通过")).not.toBeInTheDocument();
  });
  it("rejects a cross-provider result before rendering any success", async () => {
    api.get.mockResolvedValue(preflightResult); render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />);
    expect(await screen.findByRole("alert")).toHaveTextContent("响应格式异常"); expect(screen.queryByTestId("preflight-check")).not.toBeInTheDocument();
  });
  it("stops polling on completion and aborts on unmount", async () => {
    vi.useFakeTimers(); api.get.mockResolvedValueOnce(queued()).mockResolvedValueOnce(completed()); const view = render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />);
    await act(async () => {}); await act(async () => { await vi.advanceTimersByTimeAsync(2000); }); expect(api.get).toHaveBeenCalledTimes(2);
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); }); expect(api.get).toHaveBeenCalledTimes(2); view.unmount(); expect(api.get.mock.calls[1][3].aborted).toBe(true);
  });
  it("stops automatic polling at 120 seconds without faking completion or issuing a new probe", async () => {
    vi.useFakeTimers(); api.get.mockResolvedValue({ ...queued(), job_deadline_at: iso(999999) }); render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />); await act(async () => {});
    await act(async () => { await vi.advanceTimersByTimeAsync(121000); }); const count = api.get.mock.calls.length; expect(count).toBeLessThanOrEqual(61);
    await act(async () => { await vi.advanceTimersByTimeAsync(120000); }); expect(api.get).toHaveBeenCalledTimes(count); expect(api.create).not.toHaveBeenCalled(); expect(screen.getByText(/自动轮询已停止/)).toBeInTheDocument();
  });
});
