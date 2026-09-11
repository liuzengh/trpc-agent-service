import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChannelApiError, type ChannelCommandResult } from "../../lib/channel-api";
import { channelPendingKey, loadChannelPending, saveChannelPending } from "../../lib/channel-editor-state";
import { sampleChannelAccount, sampleChannelDetails } from "../../test/channel-fixtures";
import { AccountCreate } from "./account-create";

const mocks = vi.hoisted(() => ({ create: vi.fn(), get: vi.fn(), replace: vi.fn(), reload: vi.fn(), deny: vi.fn(), access: { userId: "u", canWrite: true, loading: false, error: "" } }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ replace: mocks.replace }) }));
vi.mock("../../lib/channel-api", async (original) => ({ ...await original<typeof import("../../lib/channel-api")>(), channelApi: { createAccount: mocks.create, getAccount: mocks.get } }));
vi.mock("./use-channel-access", () => ({ useChannelAccess: () => ({ ...mocks.access, reload: mocks.reload, denyWrites: mocks.deny }) }));

const created = { ...sampleChannelAccount, enabled: false, account_revision: 1, connection_revision: 1, min_route_generation: 0 };
const result: ChannelCommandResult = { account: created, distribution: "NOT_EMITTED", route_generation: 0 };
const inputValue = (label: string | RegExp, value: string) => fireEvent.change(screen.getByLabelText(label), { target: { value } });
async function fillTelegram(id = "00123456") {
  await screen.findByRole("button", { name: "检查并创建账户" });
  inputValue(/Telegram 数字 Bot ID/, id); inputValue(/账户名称/, "研究机器人");
  inputValue("Bot Token", "form-only-token"); inputValue(/Webhook Secret/, "form_only_webhook");
}
function openConfirmation(retry = false) { fireEvent.click(screen.getByRole("button", { name: retry ? "重试确认原创建请求" : "检查并创建账户" })); }
function confirmCreate(retry = false) {
  const dialog = screen.getByRole("dialog");
  fireEvent.click(within(dialog).getByRole("checkbox"));
  fireEvent.click(within(dialog).getByRole("button", { name: retry ? "使用原请求标识确认" : "确认创建并保持停用" }));
}

beforeEach(() => {
  vi.resetAllMocks(); window.sessionStorage.clear();
  // Node's optional Web Storage global is not the jsdom Storage implementation.
  vi.stubGlobal("localStorage", { setItem: vi.fn(), getItem: vi.fn(), removeItem: vi.fn(), clear: vi.fn(), key: vi.fn(), length: 0 });
  mocks.access = { userId: "u", canWrite: true, loading: false, error: "" };
  mocks.create.mockResolvedValue(result); mocks.get.mockResolvedValue({ ...sampleChannelDetails, account: created });
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe("Channel account create", () => {
  it("saves the test endpoint selected in the existing account form", async () => {
    render(<AccountCreate tenantId="t" />); await fillTelegram();
    inputValue("接入环境", "test"); openConfirmation(); confirmCreate();
    await waitFor(() => expect(mocks.create).toHaveBeenCalledOnce());
    expect(mocks.create.mock.calls[0][1].config).toEqual({receive_mode:"long_polling",endpoint_profile:"test"});
  });

  it("requires all fields and credentials before creating or opening confirmation", async () => {
    render(<AccountCreate tenantId="t" />);
    fireEvent.click(await screen.findByRole("button", { name: "检查并创建账户" }));
    expect(screen.getByRole("alert")).toHaveTextContent("本次尚未提交");
    expect(screen.getByLabelText(/Telegram 数字 Bot ID/)).toHaveAttribute("aria-invalid", "true");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument(); expect(mocks.create).not.toHaveBeenCalled();
  });

  it("clears provider-specific secrets on switch and sends only WeCom replace credentials", async () => {
    render(<AccountCreate tenantId="t" />); await fillTelegram();
    fireEvent.change(screen.getByRole("combobox", { name: "渠道类型" }), { target: { value: "wecom" } });
    expect(screen.queryByLabelText("Bot Token")).not.toBeInTheDocument();
    inputValue(/企业微信 Bot ID/, "wecom-bot"); inputValue("Bot Secret", "wecom-form-only");
    openConfirmation(); confirmCreate();
    await waitFor(() => expect(mocks.create).toHaveBeenCalledOnce());
    expect(mocks.create.mock.calls[0][1]).toMatchObject({ provider: "wecom", provider_account_id: "wecom-bot", credentials: { "wecom.bot_secret": { action: "replace", value: "wecom-form-only" } } });
    expect(Object.keys(mocks.create.mock.calls[0][1].credentials)).toEqual(["wecom.bot_secret"]);
  });

  it("keeps a large numeric Bot ID as a string, requires confirmation and creates disabled", async () => {
    render(<AccountCreate tenantId="t" query="deployment_id=d&revision_number=3" />); await fillTelegram("009007199254740993123456789");
    openConfirmation();
    expect(mocks.create).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "确认创建并保持停用" })).toBeDisabled();
    expect(within(screen.getByRole("dialog")).getByText("9007199254740993123456789")).toBeInTheDocument();
    confirmCreate();
    await waitFor(() => expect(mocks.replace).toHaveBeenCalledWith("/tenants/t/channels/cha_test?deployment_id=d&revision_number=3"));
    expect(mocks.create.mock.calls[0][1].provider_account_id).toBe("9007199254740993123456789");
    expect(mocks.create.mock.calls[0][1]).not.toHaveProperty("enabled");
    expect(mocks.create.mock.calls[0][1].config).toEqual({ receive_mode: "long_polling" });
    expect(mocks.get).toHaveBeenCalledWith("t", "cha_test");
  });

  it("cancels the confirmation without creating an account", async () => {
    render(<AccountCreate tenantId="t" />); await fillTelegram(); openConfirmation();
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "取消" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument(); expect(mocks.create).not.toHaveBeenCalled();
    expect(window.sessionStorage.length).toBe(0);
    expect(screen.getByRole("link", { name: "取消" })).toHaveAttribute("href", "/tenants/t/channels");
  });

  it("blocks double submission and persists only the non-secret marker", async () => {
    let finish!: (value: ChannelCommandResult) => void;
    mocks.create.mockReturnValue(new Promise((resolve) => { finish = resolve; }));
    const storage = vi.spyOn(Object.getPrototypeOf(window.sessionStorage), "setItem");
    render(<AccountCreate tenantId="t" />); await fillTelegram(); openConfirmation();
    const dialog = screen.getByRole("dialog"); fireEvent.click(within(dialog).getByRole("checkbox"));
    const button = within(dialog).getByRole("button", { name: "确认创建并保持停用" });
    fireEvent.click(button); fireEvent.click(button);
    expect(mocks.create).toHaveBeenCalledOnce();
    const marker = loadChannelPending(window.sessionStorage, channelPendingKey("u", "t", "new"));
    expect(marker).toMatchObject({ operation: "createAccount", secret: true, input: { provider: "telegram", provider_account_id: "123456" } });
    const stored = JSON.stringify(storage.mock.calls);
    expect(stored).not.toContain("form-only-token"); expect(stored).not.toContain("form_only_webhook");
    expect(marker?.input).not.toHaveProperty("credentials"); expect(window.localStorage.setItem).not.toHaveBeenCalled();
    await act(async () => finish(result));
    await waitFor(() => expect(mocks.replace).toHaveBeenCalledOnce());
    expect(loadChannelPending(window.sessionStorage, channelPendingKey("u", "t", "new"))).toBeNull();
  });

  it("retries an uncertain same-page create with the identical body and idempotency key", async () => {
    mocks.create.mockRejectedValueOnce(new ChannelApiError(0, "REQUEST_TIMEOUT", "请求超时，结果待确认")).mockResolvedValueOnce(result);
    render(<AccountCreate tenantId="t" />); await fillTelegram(); openConfirmation(); confirmCreate();
    await screen.findByText("上一次创建结果待确认");
    expect(screen.getByLabelText("Bot Token")).toBeDisabled();
    openConfirmation(true); confirmCreate(true);
    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(2));
    expect(mocks.create.mock.calls[1]).toEqual(mocks.create.mock.calls[0]);
  });

  it("restores non-secret original fields after refresh but waits for original secrets and an explicit same-key retry", async () => {
    const marker = { operation: "createAccount", key: "original-recovery-key", secret: true, createdAt: new Date().toISOString(), input: { provider: "telegram", provider_account_id: "123456", name: "待确认机器人", description: "original-description" } };
    expect(saveChannelPending(window.sessionStorage, channelPendingKey("u", "t", "new"), marker)).toBe(true);
    render(<AccountCreate tenantId="t" />);
    await screen.findByText("上一次创建结果待确认");
    expect(screen.getByLabelText("Bot Token")).toHaveValue(""); expect(screen.getByLabelText("Bot Token")).not.toBeDisabled();
    expect(screen.getByLabelText(/账户名称/)).toHaveValue("待确认机器人"); expect(screen.getByLabelText(/账户名称/)).toBeDisabled();
    expect(mocks.create).not.toHaveBeenCalled();
    inputValue("Bot Token", "original-token-reentered"); inputValue(/Webhook Secret/, "original_webhook");
    openConfirmation(true); expect(screen.getByLabelText(/我重新输入了上一次请求的原凭据/)).toBeInTheDocument(); confirmCreate(true);
    await waitFor(() => expect(mocks.create).toHaveBeenCalledOnce());
    expect(mocks.create.mock.calls[0][2]).toBe("original-recovery-key");
    expect(mocks.create.mock.calls[0][1]).toMatchObject({ name: "待确认机器人", description: "original-description", credentials: { "telegram.bot_token": { value: "original-token-reentered" } } });
  });

  it("navigates to the known created account if the follow-up read fails and never creates twice", async () => {
    mocks.get.mockRejectedValue(new ChannelApiError(503, "CHANNEL_DEPENDENCY_UNAVAILABLE", "read unavailable"));
    render(<AccountCreate tenantId="t" />); await fillTelegram(); openConfirmation(); confirmCreate();
    await waitFor(() => expect(mocks.replace).toHaveBeenCalledWith("/tenants/t/channels/cha_test"));
    expect(mocks.create).toHaveBeenCalledOnce();
    expect(screen.getByRole("link", { name: "打开已创建账户 →" })).toHaveAttribute("href", "/tenants/t/channels/cha_test");
    expect(screen.queryByRole("button", { name: "检查并创建账户" })).not.toBeInTheDocument();
    expect(loadChannelPending(window.sessionStorage, channelPendingKey("u", "t", "new"))).toBeNull();
  });

  it("shows MEMBER only the read-only explanation and no secret controls", async () => {
    mocks.access.canWrite = false;
    render(<AccountCreate tenantId="t" />);
    await screen.findByText("当前为只读访问");
    expect(screen.queryByLabelText("Bot Token")).not.toBeInTheDocument(); expect(mocks.create).not.toHaveBeenCalled();
  });

  it("generates a provider-valid webhook secret without storage writes", async () => {
    const storage = vi.spyOn(Object.getPrototypeOf(window.sessionStorage), "setItem");
    render(<AccountCreate tenantId="t" />);
    fireEvent.click(await screen.findByRole("button", { name: "生成随机值" }));
    expect(screen.getByLabelText<HTMLInputElement>(/Webhook Secret/).value).toMatch(/^[A-Za-z0-9_-]{64}$/);
    expect(storage).not.toHaveBeenCalled(); expect(mocks.create).not.toHaveBeenCalled();
  });

  it("announces invalid or repeated fixed target input and never auto-binds", async () => {
    render(<AccountCreate tenantId="t" query="deployment_id=d&revision_number=latest" />);
    expect(screen.getByRole("alert")).toHaveTextContent("固定部署参数无效或重复");
    await screen.findByRole("button", { name: "检查并创建账户" }); expect(mocks.create).not.toHaveBeenCalled();
  });

  it("clears visible secrets when access is revoked", async () => {
    const view = render(<AccountCreate tenantId="t" />); await fillTelegram();
    mocks.access.canWrite = false; view.rerender(<AccountCreate tenantId="t" />);
    await screen.findByText("当前为只读访问");
    mocks.access.canWrite = true; view.rerender(<AccountCreate tenantId="t" />);
    expect(await screen.findByLabelText("Bot Token")).toHaveValue("");
    expect(screen.getByLabelText(/Webhook Secret/)).toHaveValue("");
  });

  it("locks a refreshed recovery body in memory after its first retry also times out", async () => {
    saveChannelPending(window.sessionStorage, channelPendingKey("u", "t", "new"), { operation: "createAccount", key: "recovered-key", secret: true, createdAt: new Date().toISOString(), input: { provider: "telegram", provider_account_id: "123456", name: "待确认机器人" } });
    mocks.create.mockRejectedValueOnce(new ChannelApiError(0, "REQUEST_TIMEOUT", "timeout"));
    render(<AccountCreate tenantId="t" />); await screen.findByText("上一次创建结果待确认");
    inputValue("Bot Token", "original-token"); inputValue(/Webhook Secret/, "original_webhook");
    openConfirmation(true); confirmCreate(true);
    await waitFor(() => expect(screen.getByRole("button", { name: "重试确认原创建请求" })).not.toBeDisabled());
    expect(screen.getByLabelText("Bot Token")).toBeDisabled();
    openConfirmation(true); confirmCreate(true);
    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(2));
    expect(mocks.create.mock.calls[1]).toEqual(mocks.create.mock.calls[0]);
  });

  it("retains the original key after a recovery conflict and permits explicit re-entry of the actual original credentials", async () => {
    saveChannelPending(window.sessionStorage, channelPendingKey("u", "t", "new"), { operation: "createAccount", key: "recovered-key", secret: true, createdAt: new Date().toISOString(), input: { provider: "telegram", provider_account_id: "123456", name: "待确认机器人" } });
    mocks.create.mockRejectedValueOnce(new ChannelApiError(409, "CHANNEL_IDEMPOTENCY_CONFLICT", "conflict"));
    render(<AccountCreate tenantId="t" />); await screen.findByText("上一次创建结果待确认");
    inputValue("Bot Token", "wrong-reentry"); inputValue(/Webhook Secret/, "wrong_webhook");
    openConfirmation(true); confirmCreate(true);
    expect(await screen.findByRole("alert")).toHaveTextContent("不要以新标识重复创建");
    expect(screen.getByLabelText("Bot Token")).not.toBeDisabled(); expect(screen.getByLabelText("Bot Token")).toHaveValue("");
    inputValue("Bot Token", "actual-original-token"); inputValue(/Webhook Secret/, "actual_original_webhook");
    openConfirmation(true); confirmCreate(true);
    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(2));
    expect(mocks.create.mock.calls[0][2]).toBe("recovered-key"); expect(mocks.create.mock.calls[1][2]).toBe("recovered-key");
    expect(mocks.create.mock.calls[1][1].credentials["telegram.bot_token"].value).toBe("actual-original-token");
  });

  it("blocks new submissions while pending storage cannot be read, then explicitly reloads", async () => {
    const get = vi.spyOn(Object.getPrototypeOf(window.sessionStorage), "getItem").mockImplementationOnce(() => { throw new Error("storage unavailable"); });
    render(<AccountCreate tenantId="t" />);
    expect(await screen.findByRole("alert")).toHaveTextContent("恢复记录读取失败");
    expect(screen.queryByRole("button", { name: "检查并创建账户" })).not.toBeInTheDocument();
    expect(mocks.create).not.toHaveBeenCalled(); get.mockRestore();
    fireEvent.click(screen.getByRole("button", { name: "重新读取恢复记录" }));
    await screen.findByRole("button", { name: "检查并创建账户" });
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("does not send a create when its non-secret recovery marker cannot be saved", async () => {
    render(<AccountCreate tenantId="t" />); await fillTelegram(); openConfirmation();
    vi.spyOn(Object.getPrototypeOf(window.sessionStorage), "setItem").mockImplementation(() => { throw new Error("storage unavailable"); });
    confirmCreate(); expect(await screen.findByRole("alert")).toHaveTextContent("本次尚未提交");
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("does not follow a late create response after leaving the form and retains the recovery marker", async () => {
    let finish!: (value: ChannelCommandResult) => void;
    mocks.create.mockReturnValue(new Promise((resolve) => { finish = resolve; }));
    const view = render(<AccountCreate tenantId="t" />); await fillTelegram(); openConfirmation(); confirmCreate();
    view.unmount(); await act(async () => finish(result));
    expect(mocks.get).not.toHaveBeenCalled(); expect(mocks.replace).not.toHaveBeenCalled();
    expect(loadChannelPending(window.sessionStorage, channelPendingKey("u", "t", "new"))?.operation).toBe("createAccount");
  });

  it("clears in-memory credentials when the authenticated identity changes", async () => {
    const view = render(<AccountCreate tenantId="t" />); await fillTelegram();
    mocks.access.userId = "another-user"; view.rerender(<AccountCreate tenantId="t" />);
    expect(await screen.findByLabelText("Bot Token")).toHaveValue("");
    expect(screen.getByLabelText(/Webhook Secret/)).toHaveValue("");
    expect(screen.getByLabelText(/账户名称/)).toHaveValue(""); expect(mocks.create).not.toHaveBeenCalled();
  });
});


describe("Receive mode creation", () => {
  it("defaults to LP and omits the optional Secret rather than sending an empty replace", async () => {
    render(<AccountCreate tenantId="t" />);
    await screen.findByRole("button", { name: "检查并创建账户" });
    expect(screen.getByRole("radio", { name: /^长轮询/ })).toBeChecked();
    inputValue(/Telegram 数字 Bot ID/, "123456"); inputValue(/账户名称/, "LP bot"); inputValue("Bot Token", "lp-token");
    openConfirmation(); expect(screen.getByRole("dialog")).toHaveTextContent("长轮询"); confirmCreate();
    await waitFor(() => expect(mocks.create).toHaveBeenCalledOnce());
    expect(mocks.create.mock.calls[0][1]).toMatchObject({ config: { receive_mode: "long_polling" }, credentials: { "telegram.bot_token": { action: "replace", value: "lp-token" } } });
    expect(Object.keys(mocks.create.mock.calls[0][1].credentials)).toEqual(["telegram.bot_token"]);
    expect(mocks.create.mock.calls[0]).toHaveLength(3);
  });
  it("requires Secret in Webhook and preserves typed optional Secret across mode choices", async () => {
    render(<AccountCreate tenantId="t" />); await fillTelegram();
    fireEvent.click(screen.getByRole("radio", { name: /^Webhook/ }));
    expect(screen.getByLabelText(/Webhook Secret/)).toHaveValue("form_only_webhook");
    inputValue(/Webhook Secret/, ""); openConfirmation();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByLabelText(/Webhook Secret/)).toHaveAttribute("aria-invalid", "true");
    inputValue(/Webhook Secret/, "preserved"); fireEvent.click(screen.getByRole("radio", { name: /^长轮询/ }));
    expect(screen.getByLabelText(/Webhook Secret/)).toHaveValue("preserved");
    openConfirmation(); confirmCreate(); await waitFor(() => expect(mocks.create).toHaveBeenCalledOnce());
    expect(mocks.create.mock.calls[0][1].credentials["telegram.webhook_secret"].value).toBe("preserved");
  });
  it("restores modern LP with the exact omitted-purpose set and no implicit Secret", async () => {
    const marker = { operation: "createAccount", key: "modern-key", secret: true, createdAt: new Date().toISOString(), input: { provider: "telegram", provider_account_id: "123", name: "LP pending", config: { receive_mode: "long_polling" }, supplied_purposes: ["telegram.bot_token"] } };
    expect(saveChannelPending(sessionStorage, channelPendingKey("u", "t", "new"), marker)).toBe(true);
    render(<AccountCreate tenantId="t" />); await screen.findByText("上一次创建结果待确认");
    expect(screen.getByRole("radio", { name: /^长轮询/ })).toBeDisabled();
    expect(screen.getByLabelText(/Webhook Secret/)).toBeDisabled();
    inputValue("Bot Token", "original-lp-token"); openConfirmation(true); confirmCreate(true);
    await waitFor(() => expect(mocks.create).toHaveBeenCalledOnce());
    expect(mocks.create.mock.calls[0][2]).toBe("modern-key");
    expect(mocks.create.mock.calls[0][1].config).toEqual({ receive_mode: "long_polling" });
    expect(Object.keys(mocks.create.mock.calls[0][1].credentials)).toEqual(["telegram.bot_token"]);
    expect(mocks.create.mock.calls[0][1]).not.toHaveProperty("supplied_purposes");
  });
  it("uses only the explicit legacy contract for an old marker, without new mode or a new key", async () => {
    saveChannelPending(sessionStorage, channelPendingKey("u", "t", "new"), { operation: "createAccount", key: "legacy-key", secret: true, createdAt: new Date().toISOString(), input: { provider: "telegram", provider_account_id: "123", name: "old webhook" } });
    render(<AccountCreate tenantId="t" />); await screen.findByText("上一次创建结果待确认");
    expect(screen.queryByRole("radio")).not.toBeInTheDocument(); expect(mocks.create).not.toHaveBeenCalled();
    inputValue("Bot Token", "original-token"); inputValue(/Webhook Secret/, "original_secret"); openConfirmation(true); confirmCreate(true);
    await waitFor(() => expect(mocks.create).toHaveBeenCalledOnce());
    expect(mocks.create.mock.calls[0][2]).toBe("legacy-key"); expect(mocks.create.mock.calls[0][3]).toBe("webhook-v1");
    expect(mocks.create.mock.calls[0][1]).not.toHaveProperty("config");
    expect(Object.keys(mocks.create.mock.calls[0][1].credentials)).toEqual(["telegram.bot_token", "telegram.webhook_secret"]);
  });
});
