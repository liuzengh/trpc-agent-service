import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChannelPreflightApiError, PREFLIGHT_STORAGE_PREFIX } from "../../lib/channel-preflight-api";
import { sampleChannelAccount } from "../../test/channel-fixtures";
import { preflightReceipt, preflightResult, queuedPreflight, longPollingPreflight } from "../../test/channel-preflight-fixtures";
import { ChannelPreflightPanel } from "./preflight-panel";
const api = vi.hoisted(() => ({ create: vi.fn(), get: vi.fn() }));
vi.mock("../../lib/channel-preflight-api", async (original) => ({ ...await original<typeof import("../../lib/channel-preflight-api")>(), channelPreflightApi: api }));
const denyWrites = vi.fn(); const account = { ...sampleChannelAccount, enabled: false };
const props = { account, userId: "u", canWrite: true, denyWrites };
const storageKey = PREFLIGHT_STORAGE_PREFIX + "u:t:cha_test";
const iso = (offset: number) => new Date(Date.now() + offset).toISOString();
const receipt = () => ({ ...preflightReceipt, requested_at: iso(-1000), job_deadline_at: iso(119000) });
const completed = () => ({ ...preflightResult, requested_at: iso(-2000), job_deadline_at: iso(118000), started_at: iso(-1500), checked_at: iso(-1000), expires_at: iso(299000) });
const queued = () => ({ ...queuedPreflight, requested_at: iso(-1000), job_deadline_at: iso(119000) });
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>((done) => { resolve = done; }); return { promise, resolve }; }
beforeEach(() => { vi.resetAllMocks(); sessionStorage.clear(); api.create.mockResolvedValue(receipt()); api.get.mockResolvedValue(completed()); });
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers(); });
describe("Telegram preflight panel", () => {
  it("does not show a Telegram preflight for WeCom", () => { render(<ChannelPreflightPanel {...props} account={{ ...account, provider: "wecom" }} />); expect(screen.queryByText("检查接入条件")).not.toBeInTheDocument(); expect(api.get).not.toHaveBeenCalled(); });
  it("explains a separately confirmed stop for enabled accounts and never toggles or creates", async () => {
    render(<ChannelPreflightPanel {...props} account={{ ...account, enabled: true }} />);
    expect(await screen.findByRole("button", { name: "检查接入条件" })).toBeDisabled(); expect(screen.getByText(/请先在账户接入操作中单独确认停用/)).toBeInTheDocument(); expect(api.create).not.toHaveBeenCalled();
  });
  it("lets MEMBER read an exact linked task and renders all eight facts and limits", async () => {
    render(<ChannelPreflightPanel {...props} canWrite={false} initialPreflightId="cpf_test" />);
    await screen.findByText("检查完成 · 需要留意"); expect(api.get).toHaveBeenCalledWith("t", "cha_test", "cpf_test", expect.any(AbortSignal));
    expect(screen.getAllByTestId("preflight-check")).toHaveLength(8); expect(screen.queryByRole("button", { name: "检查接入条件" })).not.toBeInTheDocument();
    expect(screen.getByText(/Telegram 实际投递：未验证/)).toBeInTheDocument(); expect(screen.getByText(/Gateway 当前配置：尚未确认/)).toBeInTheDocument(); expect(screen.getByText(/仅静态格式检查/)).toBeInTheDocument();
  });
  it("creates with explicit three CAS versions and never requires a Binding", async () => {
    render(<ChannelPreflightPanel {...props} />); const start = await screen.findByRole("button", { name: "检查接入条件" }); expect(start).toHaveClass("primary"); fireEvent.click(start);
    await screen.findByText("检查完成 · 需要留意"); expect(api.create).toHaveBeenCalledWith("t", "cha_test", { expected_account_revision: 3, expected_connection_revision: 2, expected_bot_token_version: 1 }, expect.any(String), expect.any(AbortSignal));
    expect(screen.getByRole("link", { name: "此预检的固定链接" })).toHaveAttribute("href", "/tenants/t/channels/cha_test?preflight=cpf_test");
    expect(sessionStorage.getItem(storageKey)).not.toContain("credential_id");
  });
  it("replays an uncertain POST with exactly the original key/body even after account metadata changes", async () => {
    api.create.mockRejectedValueOnce(new ChannelPreflightApiError(0, "REQUEST_TIMEOUT")); const view = render(<ChannelPreflightPanel {...props} />);
    fireEvent.click(await screen.findByRole("button", { name: "检查接入条件" })); await screen.findByRole("button", { name: "使用原请求重试确认" }); const original = api.create.mock.calls[0];
    view.rerender(<ChannelPreflightPanel {...props} account={{ ...account, account_revision: 4 }} />);
    fireEvent.click(screen.getByRole("button", { name: "使用原请求重试确认" })); await screen.findByText("检查完成 · 需要留意");
    expect(api.create.mock.calls[1].slice(0, 4)).toEqual(original.slice(0, 4));
  });
  it("re-reads only GET after a 202 acknowledgement followed by read failure", async () => {
    api.get.mockRejectedValueOnce(new ChannelPreflightApiError(503, "CHANNEL_DEPENDENCY_UNAVAILABLE")); render(<ChannelPreflightPanel {...props} />);
    fireEvent.click(await screen.findByRole("button", { name: "检查接入条件" })); fireEvent.click(await screen.findByRole("button", { name: "重新读取预检结果" }));
    await screen.findByText("检查完成 · 需要留意"); expect(api.create).toHaveBeenCalledTimes(1); expect(api.get).toHaveBeenCalledTimes(2);
  });
  it("restores an unknown POST without auto-submission and preserves the original versions/key", async () => {
    const input = { expected_account_revision: 1, expected_connection_revision: 1, expected_bot_token_version: 1 };
    sessionStorage.setItem(storageKey, JSON.stringify({ kind: "pending", key: "original", input, createdAt: iso(-1000) })); render(<ChannelPreflightPanel {...props} />);
    const retry = await screen.findByRole("button", { name: "使用原请求重试确认" }); expect(api.create).not.toHaveBeenCalled(); fireEvent.click(retry);
    await screen.findByText("检查完成 · 需要留意"); expect(api.create.mock.calls[0].slice(0, 4)).toEqual(["t", "cha_test", input, "original"]);
  });
  it("blocks corrupt recovery storage instead of silently starting a new job", async () => {
    sessionStorage.setItem(storageKey, '{"token":"must-not-render"'); render(<ChannelPreflightPanel {...props} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("恢复记录损坏"); expect(screen.getByRole("button", { name: "检查接入条件" })).toBeDisabled(); expect(screen.queryByText("must-not-render")).not.toBeInTheDocument(); expect(api.create).not.toHaveBeenCalled();
  });
  it("blocks a new POST when recovery storage is unavailable", async () => {
    vi.stubGlobal("sessionStorage", { getItem: () => { throw new Error("blocked"); } }); render(<ChannelPreflightPanel {...props} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("浏览器恢复存储不可用"); expect(screen.getByRole("button", { name: "检查接入条件" })).toBeDisabled();
  });
  it("shows an old backend 404 as missing capability, not an empty successful result", async () => {
    api.get.mockRejectedValue(new ChannelPreflightApiError(404, "HTTP_ERROR")); render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />);
    expect(await screen.findByRole("alert")).toHaveTextContent("当前后端尚未提供预检接口"); expect(screen.queryByText("检查完成 · 配置检查通过")).not.toBeInTheDocument();
  });
  it.each([401, 403])("closes writes on HTTP %s", async (status) => {
    api.create.mockRejectedValue(new ChannelPreflightApiError(status, "CHANNEL_PERMISSION_DENIED")); render(<ChannelPreflightPanel {...props} />);
    fireEvent.click(await screen.findByRole("button", { name: "检查接入条件" })); await waitFor(() => expect(denyWrites).toHaveBeenCalled()); expect(screen.getByRole("button", { name: "使用原请求重试确认" })).toBeDisabled();
  });
  it("preserves MEMBER reads after an OWNER-to-MEMBER POST 403 without suggesting re-login", async () => {
    api.create.mockRejectedValue(new ChannelPreflightApiError(403, "CHANNEL_PERMISSION_DENIED"));
    const view = render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />);
    await screen.findByText("检查完成 · 需要留意"); fireEvent.click(screen.getByRole("button", { name: "重新检查接入条件" }));
    await waitFor(() => expect(denyWrites).toHaveBeenCalled());
    const originalKey = api.create.mock.calls[0][3];
    view.rerender(<ChannelPreflightPanel {...props} canWrite={false} initialPreflightId="cpf_test" />);
    const read = screen.getByRole("button", { name: "重新读取预检结果" }); expect(read).toBeEnabled(); fireEvent.click(read);
    await waitFor(() => expect(api.get).toHaveBeenCalledTimes(2));
    expect(screen.getByRole("link", { name: "此预检的固定链接" })).toHaveAttribute("href", "/tenants/t/channels/cha_test?preflight=cpf_test");
    expect(screen.queryByRole("link", { name: "重新登录后继续核实" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "使用原请求重试确认" })).toBeDisabled();
    expect(JSON.parse(sessionStorage.getItem(storageKey)!)).toMatchObject({ kind: "pending", key: originalKey }); expect(api.create).toHaveBeenCalledTimes(1);
  });
  it.each([401, 403])("closes result reads only when the GET itself returns HTTP %s", async (status) => {
    api.get.mockRejectedValue(new ChannelPreflightApiError(status, "CHANNEL_PERMISSION_DENIED"));
    render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />);
    await waitFor(() => expect(denyWrites).toHaveBeenCalled()); expect(screen.getByRole("button", { name: "重新读取预检结果" })).toBeDisabled();
    if (status === 401) expect(screen.getByRole("link", { name: "重新登录后继续核实" })).toBeInTheDocument();
    else { expect(screen.queryByRole("link", { name: "重新登录后继续核实" })).not.toBeInTheDocument(); expect(screen.getByRole("alert")).toHaveTextContent("读取此预检结果"); }
  });
  it("closes historical result reads when a new POST proves the session expired with 401", async () => {
    api.create.mockRejectedValue(new ChannelPreflightApiError(401, "CHANNEL_PERMISSION_DENIED"));
    render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />);
    await screen.findByText("检查完成 · 需要留意"); fireEvent.click(screen.getByRole("button", { name: "重新检查接入条件" }));
    await waitFor(() => expect(denyWrites).toHaveBeenCalled()); expect(screen.getByRole("button", { name: "重新读取预检结果" })).toBeDisabled();
    expect(screen.getByRole("link", { name: "重新登录后继续核实" })).toBeInTheDocument();
  });
  it("ignores a late result after switching account/user and aborts the old request", async () => {
    const old = deferred<ReturnType<typeof completed>>(); api.get.mockReturnValueOnce(old.promise); const view = render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />);
    await waitFor(() => expect(api.get).toHaveBeenCalled()); const signal = api.get.mock.calls[0][3];
    view.rerender(<ChannelPreflightPanel {...props} userId="other" account={{ ...account, account_id: "cha_other" }} />); await act(async () => old.resolve(completed()));
    expect(signal.aborted).toBe(true); expect(screen.queryByText("检查完成 · 需要留意")).not.toBeInTheDocument();
  });
  it("marks a completed check stale immediately on connection change but only notes metadata edits", async () => {
    const view = render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />); await screen.findByText("检查完成 · 需要留意");
    view.rerender(<ChannelPreflightPanel {...props} account={{ ...account, account_revision: 4 }} initialPreflightId="cpf_test" />); expect(screen.getByText(/名称或说明已变更，连接检查仍适用/)).toBeInTheDocument();
    view.rerender(<ChannelPreflightPanel {...props} account={{ ...account, connection_revision: 3 }} initialPreflightId="cpf_test" />); expect(screen.getByText(/配置已变化，此结果已失效/)).toBeInTheDocument();
  });
  it("rejects a malformed URL task id without making a GET", async () => {
    render(<ChannelPreflightPanel {...props} initialPreflightId="../../other" />); expect(await screen.findByRole("alert")).toHaveTextContent("预检链接参数无效"); expect(api.get).not.toHaveBeenCalled();
  });
  it("prevents duplicate clicks from submitting two jobs", async () => {
    const pending = deferred<ReturnType<typeof receipt>>(); api.create.mockReturnValue(pending.promise); render(<ChannelPreflightPanel {...props} />);
    const button = await screen.findByRole("button", { name: "检查接入条件" }); fireEvent.click(button); fireEvent.click(button); expect(api.create).toHaveBeenCalledTimes(1); await act(async () => pending.resolve(receipt()));
  });
  it("polls queued work every two seconds, stops on completion and aborts on unmount", async () => {
    vi.useFakeTimers(); api.get.mockResolvedValueOnce(queued()).mockResolvedValueOnce(completed()); const view = render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />);
    await act(async () => {}); expect(api.get).toHaveBeenCalledTimes(1); await act(async () => { await vi.advanceTimersByTimeAsync(2000); }); expect(api.get).toHaveBeenCalledTimes(2);
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); }); expect(api.get).toHaveBeenCalledTimes(2); view.unmount(); expect(api.get.mock.calls[1][3].aborted).toBe(true);
  });
  it("stops automatic polling at a hard 120-second limit without inventing a backend terminal state", async () => {
    vi.useFakeTimers(); api.get.mockResolvedValue({ ...queued(), job_deadline_at: iso(999999) }); render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />); await act(async () => {});
    await act(async () => { await vi.advanceTimersByTimeAsync(121000); }); expect(api.get.mock.calls.length).toBeLessThanOrEqual(61); const count = api.get.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(120000); }); expect(api.get).toHaveBeenCalledTimes(count); expect(screen.getByText(/自动轮询已停止/)).toBeInTheDocument(); expect(screen.queryByText("检查完成 · 配置检查通过")).not.toBeInTheDocument();
  });
  it("does not poll while hidden, then resumes visibly within the original budget", async () => {
    vi.useFakeTimers(); let visibility = "hidden"; vi.spyOn(document, "visibilityState", "get").mockImplementation(() => visibility as DocumentVisibilityState);
    api.get.mockResolvedValue(queued()); render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />); await act(async () => {}); expect(api.get).not.toHaveBeenCalled();
    visibility = "visible"; await act(async () => { document.dispatchEvent(new Event("visibilitychange")); }); expect(api.get).toHaveBeenCalledTimes(1);
    visibility = "hidden"; await act(async () => { document.dispatchEvent(new Event("visibilitychange")); await vi.advanceTimersByTimeAsync(6000); }); expect(api.get).toHaveBeenCalledTimes(1);
  });
  it("aborts a stalled poll at the overall deadline and ignores a late success", async () => {
    vi.useFakeTimers(); const late = deferred<ReturnType<typeof completed>>(); api.get.mockReturnValue(late.promise);
    render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />); await act(async () => {});
    const signal = api.get.mock.calls[0][3]; await act(async () => { await vi.advanceTimersByTimeAsync(120000); }); expect(signal.aborted).toBe(true);
    await act(async () => late.resolve(completed())); expect(screen.queryByText("检查完成 · 需要留意")).not.toBeInTheDocument(); expect(screen.getByText(/自动轮询已停止/)).toBeInTheDocument();
  });
  it("turns CURRENT into an expired presentation when the server TTL elapses", async () => {
    vi.useFakeTimers(); api.get.mockResolvedValue({ ...completed(), expires_at: iso(1000) });
    render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />); await act(async () => {}); await act(async () => { await vi.advanceTimersByTimeAsync(1001); });
    expect(screen.getByText(/检查结果已超过有效期/)).toBeInTheDocument(); expect(api.get).toHaveBeenCalledTimes(1);
  });
  it("keeps a PASS scoped to configuration and keeps both unverified boundaries visible", async () => {
    api.get.mockResolvedValue({ ...completed(), outcome: "PASS", checks: preflightResult.checks.map((c) => c.id === "webhook_registration" ? { ...c, status: "PASS", code: "WEBHOOK_MATCH", details: { presence: true, relation: "MATCH" } } : c) });
    render(<ChannelPreflightPanel {...props} initialPreflightId="cpf_test" />); await screen.findByText("检查完成 · 配置检查通过");
    expect(screen.getByText(/配置检查通过不等于上线成功/)).toBeInTheDocument(); expect(screen.getByText(/Telegram 实际投递：未验证/)).toBeInTheDocument();
  });
  it("uses a new key only for an explicit new check after the previous task completed", async () => {
    render(<ChannelPreflightPanel {...props} />); fireEvent.click(await screen.findByRole("button", { name: "检查接入条件" })); await screen.findByText("检查完成 · 需要留意");
    fireEvent.click(screen.getByRole("button", { name: "重新检查接入条件" })); await waitFor(() => expect(api.create).toHaveBeenCalledTimes(2)); expect(api.create.mock.calls[0][3]).not.toBe(api.create.mock.calls[1][3]);
  });
});


it("renders mode-bound LP N/A separately from success, with no fake polling or public-origin evidence", async () => {
  api.get.mockResolvedValue({ ...completed(), ...longPollingPreflight, expires_at: iso(299000) });
  render(<ChannelPreflightPanel {...props} account={{ ...account, config: { ...account.config, receive_mode: "long_polling" } }} initialPreflightId="cpf_test" />);
  await screen.findByText("检查完成 · 配置检查通过");
  expect(screen.getAllByText("不适用")).toHaveLength(3);
  expect(screen.getByText("不适用（长轮询）")).toBeInTheDocument();
  expect(screen.getByText(/本次没有发送测试消息/)).toBeInTheDocument();
  expect(screen.getByText(/本任务有效配置/)).toHaveTextContent(longPollingPreflight.effective_config_digest!);
  expect(api.create).not.toHaveBeenCalled();
});

it("retains old Webhook failure after the current Account changes to LP", async () => {
  const historical = completed(); historical.outcome = "FAIL";
  historical.checks = historical.checks.map((c) => c.id === "public_origin" ? { ...c, status: "FAIL", code: "PUBLIC_ORIGIN_NOT_PUBLIC" } : c);
  api.get.mockResolvedValue(historical);
  render(<ChannelPreflightPanel {...props} account={{ ...account, config: { ...account.config, receive_mode: "long_polling" } }} initialPreflightId="cpf_test" />);
  await screen.findByText("检查完成 · 存在问题");
  expect(screen.getByText("Webhook（旧版预检）")).toBeInTheDocument();
  expect(screen.getByText(/下列结果仍按检查时的模式解释/)).toBeInTheDocument();
  expect(screen.getByText("失败")).toBeInTheDocument();
  expect(screen.queryByText("不适用")).not.toBeInTheDocument();
});
