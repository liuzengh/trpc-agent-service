import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChannelApiError, type ChannelAccountDetails } from "../../lib/channel-api";
import { channelPendingKey, loadChannelPending, saveChannelPending } from "../../lib/channel-editor-state";
import { sampleChannelDetails } from "../../test/channel-fixtures";
import { deployment, revision } from "../../test/deployment-fixtures";
import { ChannelWorkspace } from "./account-workspace";

const mocks = vi.hoisted(() => ({
  get: vi.fn(), setAccountEnabled: vi.fn(), setBindingEnabled: vi.fn(), updateAccount: vi.fn(), updateCredential: vi.fn(), createBinding: vi.fn(), setBindingTarget: vi.fn(), getRevision: vi.fn(), getDeployment: vi.fn(), deny: vi.fn(), reload: vi.fn(), access: { userId: "u", canWrite: true, loading: false, error: "" },
}));
vi.mock("../../lib/channel-api", async (original) => ({ ...await original<typeof import("../../lib/channel-api")>(), channelApi: { getAccount: mocks.get, setAccountEnabled: mocks.setAccountEnabled, setBindingEnabled: mocks.setBindingEnabled, updateAccount: mocks.updateAccount, updateCredential: mocks.updateCredential, createBinding: mocks.createBinding, setBindingTarget: mocks.setBindingTarget } }));
vi.mock("../../lib/deployment-api", async (original) => ({ ...await original<typeof import("../../lib/deployment-api")>(), deploymentApi: { getRevision: mocks.getRevision, get: mocks.getDeployment } }));
vi.mock("./use-channel-access", () => ({ useChannelAccess: () => ({ ...mocks.access, denyWrites: mocks.deny, reload: mocks.reload }) }));
vi.mock("./preflight-panel", () => ({ ChannelPreflightPanel: ({ account, initialPreflightId, canWrite, blocked }: { account: { account_id: string }; initialPreflightId?: string; canWrite: boolean; blocked: boolean }) => <div data-testid="preflight-panel" data-account={account.account_id} data-task={initialPreflightId} data-owner={canWrite} data-blocked={blocked}>预检结果测试面板</div> }));
vi.mock("./target-selector", () => ({ ChannelTargetSelector: ({ disabled, onChange }: { disabled: boolean; onChange: (target: unknown, revision: unknown) => void }) => <button disabled={disabled} onClick={() => onChange({ deployment_id: "d", revision_number: 2 }, { ...revision, revision_number: 2 })}>选择固定 r2</button> }));
let current: ChannelAccountDetails;
const markerKey = channelPendingKey("u", "t", "cha_test");
beforeEach(() => {
  vi.resetAllMocks(); sessionStorage.clear(); current = structuredClone(sampleChannelDetails);
  mocks.access = { userId: "u", canWrite: true, loading: false, error: "" };
  mocks.get.mockImplementation(async () => structuredClone(current)); mocks.getRevision.mockResolvedValue(revision); mocks.getDeployment.mockResolvedValue(deployment);
  for (const command of [mocks.setAccountEnabled, mocks.setBindingEnabled, mocks.updateAccount, mocks.updateCredential, mocks.createBinding, mocks.setBindingTarget]) command.mockResolvedValue({ account: current.account, binding: current.binding, route_generation: 1, distribution: "PENDING" });
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); });
async function open(query = "") { render(<ChannelWorkspace tenantId="t" accountId="cha_test" query={query} />); await screen.findByRole("heading", { name: "研究机器人" }); }
async function prepare(name: string) { fireEvent.click(screen.getByRole("button", { name })); return screen.findByRole("dialog"); }
function confirm() { fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "确认此操作" })); }
function settings() { fireEvent.click(screen.getByRole("tab", { name: "凭据与设置" })); }

describe("Channel workspace command semantics", () => {
  it("opens a shared preflight in diagnostics for a member without a Binding", async () => {
    current.binding = undefined; mocks.access.canWrite = false;
    render(<ChannelWorkspace tenantId="t" accountId="cha_test" initialPreflightId="cpf_shared" />);
    await screen.findByRole("heading", { name: "研究机器人" });
    const panel = await screen.findByTestId("preflight-panel");
    expect(screen.getByRole("tab", { name: "连接诊断" })).toHaveAttribute("aria-selected", "true");
    expect(panel).toHaveAttribute("data-task", "cpf_shared");
    expect(panel).toHaveAttribute("data-account", "cha_test");
    expect(panel).toHaveAttribute("data-owner", "false");
    expect(panel).toHaveAttribute("data-blocked", "false");
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled(); expect(mocks.createBinding).not.toHaveBeenCalled();
  });
  it("navigates to the preflight panel without enabling the account or routing", async () => {
    current.account.enabled = false; await open();
    fireEvent.click(screen.getByRole("button", { name: "查看接入预检" }));
    expect(await screen.findByTestId("preflight-panel")).toBeInTheDocument();
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled(); expect(mocks.setBindingEnabled).not.toHaveBeenCalled();
  });
  it("opens the distinct WeCom diagnostic without changing account or routing", async () => {
    current.account.provider = "wecom"; current.account.config = { bot_id: "wecom-bot" }; await open();
    expect(screen.queryByRole("button", { name: "查看接入预检" })).not.toBeInTheDocument();
    expect(screen.getByText(/可能替换同一 Bot 的其他客户端/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "查看企微接入诊断" }));
    expect(screen.getByTestId("preflight-panel")).toBeInTheDocument();
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled(); expect(mocks.setBindingEnabled).not.toHaveBeenCalled();
  });
  it("shows the saved deployment name with its fixed revision and keeps its ID in technical details", async () => {
    current.binding!.target.deployment_id = "dpl_long_technical_identity";
    mocks.getDeployment.mockResolvedValue({ ...deployment, id: "dpl_long_technical_identity", name: "复杂多工具研究部署" });
    mocks.getRevision.mockResolvedValue({ ...revision, deployment_id: "dpl_long_technical_identity" });
    await open();
    expect(await screen.findByText("复杂多工具研究部署 · r1")).toBeInTheDocument();
    expect(mocks.getDeployment).toHaveBeenCalledOnce();
    expect(mocks.getDeployment).toHaveBeenCalledWith("t", "dpl_long_technical_identity");
    fireEvent.click(screen.getByRole("tab", { name: "运行目标" }));
    const target = screen.getByRole("tabpanel", { name: "运行目标" });
    expect(within(target).getByText("复杂多工具研究部署 · r1")).toBeInTheDocument();
    const technical = within(target).getByText("部署技术标识").closest("details")!;
    expect(technical).not.toHaveAttribute("open"); expect(technical).toHaveTextContent("dpl_long_technical_identity");
    expect(within(target).getByRole("link", { name: "查看当前部署快照 →" })).toHaveAttribute("href", "/tenants/t/deployments/dpl_long_technical_identity/revisions/1");
    fireEvent.focus(window);
    await waitFor(() => expect(mocks.get).toHaveBeenCalledTimes(2));
    expect(mocks.getDeployment).toHaveBeenCalledOnce();
  });
  it("keeps the fixed target usable when its display name fails and retries without a mutation", async () => {
    mocks.getDeployment.mockRejectedValueOnce(new Error("temporary"));
    await open(); fireEvent.click(screen.getByRole("tab", { name: "运行目标" }));
    const target = screen.getByRole("tabpanel", { name: "运行目标" });
    expect(await within(target).findByText(/部署名称暂时读取失败/)).toBeInTheDocument();
    expect(within(target).getByText("已保存部署 · r1")).toBeInTheDocument();
    expect(within(target).getByRole("link", { name: "查看当前部署快照 →" })).toHaveAttribute("href", "/tenants/t/deployments/d/revisions/1");
    expect(within(target).getByText("部署技术标识").closest("details")).toHaveTextContent("d");
    fireEvent.click(within(target).getByRole("button", { name: "重试读取当前来源" }));
    expect(await within(target).findByText("研究助手部署 · r1")).toBeInTheDocument();
    expect(mocks.getDeployment).toHaveBeenCalledTimes(2);
    expect(mocks.setBindingTarget).not.toHaveBeenCalled();
  });
  it("ignores a late deployment name from a previous target and fetches only the new detail", async () => {
    let oldName!: (value: typeof deployment) => void;
    mocks.getDeployment.mockImplementation((_tenant: string, id: string) => id === "d" ? new Promise((resolve) => { oldName = resolve; }) : Promise.resolve({ ...deployment, id: "new-deployment", name: "新的运行部署" }));
    await open(); current.binding!.target.deployment_id = "new-deployment";
    fireEvent.focus(window); expect(await screen.findByText("新的运行部署 · r1")).toBeInTheDocument();
    await act(async () => oldName({ ...deployment, name: "旧目标的迟到名称" }));
    expect(screen.queryByText(/旧目标的迟到名称/)).not.toBeInTheDocument();
    expect(screen.getByText("新的运行部署 · r1")).toBeInTheDocument();
    expect(mocks.getDeployment).toHaveBeenCalledTimes(2);
  });
  it("reuses one deployment name when only the saved fixed revision changes", async () => {
    await open(); expect(await screen.findByText("研究助手部署 · r1")).toBeInTheDocument();
    current.binding!.target.revision_number = 2;
    fireEvent.focus(window); expect(await screen.findByText("研究助手部署 · r2")).toBeInTheDocument();
    expect(mocks.getDeployment).toHaveBeenCalledOnce();
    // The new label commits before the dependent revision-read effect runs.
    await waitFor(() => expect(mocks.getRevision).toHaveBeenCalledWith("t", "d", 2));
  });
  it("does not read deployment details for an unbound account", async () => {
    current.binding = undefined; await open();
    expect(mocks.getDeployment).not.toHaveBeenCalled(); expect(mocks.getRevision).not.toHaveBeenCalled();
    expect(screen.getByText("未配置")).toBeInTheDocument();
  });
  it("reads again before confirmation and preserves binding intent when stopping account", async () => {
    current.binding!.enabled = true; await open(); current.account.account_revision = 7;
    const dialog = await prepare("停用本平台接入");
    expect(dialog).toHaveTextContent("消息路由的开启意图将保留"); expect(dialog).toHaveTextContent("不承诺删除 Telegram 远端 Webhook"); expect(dialog).toHaveTextContent("不保证恢复本框显示的旧目标");
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled(); confirm();
    await waitFor(() => expect(mocks.get).toHaveBeenCalledTimes(3));
    expect(mocks.setAccountEnabled).toHaveBeenCalledWith("t", "cha_test", { enabled: false, expected_account_revision: 7 }, expect.any(String));
    expect(mocks.setBindingEnabled).not.toHaveBeenCalled();
  });
  it("explains restart against current server target and Telegram registration side effects", async () => {
    current.account.enabled = false; current.binding!.enabled = true; await open();
    expect(screen.getByText(/接入已停用，但路由开启意图保留/)).toBeInTheDocument();
    const dialog = await prepare("启用本平台接入");
    expect(dialog).toHaveTextContent("setWebhook"); expect(dialog).toHaveTextContent("服务端当时保存的目标"); expect(dialog).toHaveTextContent("其他 OWNER 修改 Binding 不推进账户版本");
    fireEvent.click(within(dialog).getByRole("button", { name: "取消" })); expect(mocks.setAccountEnabled).not.toHaveBeenCalled();
  });
  it("pauses routing independently and never requires READY to enable it", async () => {
    current.binding!.enabled = true; await open(); const dialog = await prepare("暂停消息路由");
    expect(dialog).toHaveTextContent("保留目标不等于保留开启意图"); confirm();
    await waitFor(() => expect(mocks.setBindingEnabled).toHaveBeenCalledWith("t", "chb_test", { expected_binding_revision: 2, enabled: false }, expect.any(String)));
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled();
    current.binding!.enabled = false; fireEvent.focus(window); await screen.findByRole("button", { name: "开启消息路由" });
    expect(await prepare("开启消息路由")).toHaveTextContent("不把 READY 作为额外启用门槛");
  });
  it("disables route enabling while account disabled and rejects newly missing credentials", async () => {
    current.account.enabled = false; await open(); expect(screen.getByRole("button", { name: "开启消息路由" })).toBeDisabled();
    current.account.enabled = true; fireEvent.focus(window); await waitFor(() => expect(screen.getByRole("button", { name: "开启消息路由" })).toBeEnabled());
    current.account.credentials[0].configured = false;
    fireEvent.click(screen.getByRole("button", { name: "开启消息路由" }));
    await screen.findByText(/请先启用本平台接入并配置全部必需凭据/); expect(screen.queryByRole("dialog")).not.toBeInTheDocument(); expect(mocks.setBindingEnabled).not.toHaveBeenCalled();
  });
  it("invalidates an open confirmation after newer account or binding facts arrive", async () => {
    await open(); await prepare("停用本平台接入"); current.binding!.binding_revision = 4;
    fireEvent.focus(window); await screen.findByText(/读取到了较新的账户或 Binding 版本/);
    expect(within(screen.getByRole("dialog")).getByRole("button", { name: "确认此操作" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "重新读取并核对影响" }));
    await waitFor(() => expect(within(screen.getByRole("dialog")).getByRole("button", { name: "确认此操作" })).toBeEnabled());
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled();
  });
  it("saves metadata once without impact confirmation and retains the edit CAS during refresh", async () => {
    await open(); settings(); fireEvent.click(screen.getByRole("button", { name: "编辑名称与描述" }));
    fireEvent.change(screen.getByLabelText("渠道名称"), { target: { value: "新的名称" } });
    fireEvent.change(screen.getByLabelText("渠道描述"), { target: { value: "新的描述" } });
    current.account.name = "另一位 OWNER 的修改"; current.account.account_revision = 8;
    fireEvent.focus(window); await waitFor(() => expect(screen.getByRole("heading", { level: 1 })).toHaveTextContent("另一位 OWNER 的修改"));
    expect(screen.getByLabelText("渠道名称")).toHaveValue("新的名称");
    fireEvent.click(screen.getByRole("button", { name: "保存基本信息" }));
    await waitFor(() => expect(mocks.updateAccount).toHaveBeenCalledWith("t", "cha_test", { name: "新的名称", description: "新的描述", expected_account_revision: 3 }, expect.any(String)));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(mocks.updateAccount).toHaveBeenCalledOnce();
    expect(await screen.findByRole("status")).toHaveTextContent("名称与描述已保存。");
    expect(screen.queryByRole("textbox", { name: /config|Webhook 路径/ })).not.toBeInTheDocument();
  });
  it("retains uncertain metadata content and original key after refresh, without a new confirmation", async () => {
    mocks.updateAccount.mockRejectedValueOnce(new ChannelApiError(0, "REQUEST_TIMEOUT", "timeout"));
    await open(); settings(); fireEvent.click(screen.getByRole("button", { name: "编辑名称与描述" }));
    fireEvent.change(screen.getByLabelText("渠道名称"), { target: { value: "保留这次编辑" } });
    fireEvent.click(screen.getByRole("button", { name: "保存基本信息" }));
    await screen.findByRole("region", { name: "待确认渠道操作" });
    const original = mocks.updateAccount.mock.calls[0];
    expect(loadChannelPending(sessionStorage, markerKey)).toMatchObject({ operation: "updateAccount", secret: false, input: { expected_account_revision: 3, name: "保留这次编辑" } });
    current.account.account_revision = 9;
    fireEvent.click(screen.getByRole("button", { name: "只读取当前事实" }));
    await waitFor(() => expect(mocks.get).toHaveBeenCalledTimes(2));
    fireEvent.click(screen.getByRole("button", { name: "以原请求重试确认" }));
    await waitFor(() => expect(mocks.updateAccount).toHaveBeenCalledTimes(2));
    expect(mocks.updateAccount.mock.calls[1]).toEqual(original);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });
  it("only rereads after metadata save succeeds but its GET fails", async () => {
    await open(); settings(); fireEvent.click(screen.getByRole("button", { name: "编辑名称与描述" }));
    fireEvent.change(screen.getByLabelText("渠道名称"), { target: { value: "已经保存" } });
    mocks.get.mockRejectedValueOnce(new ChannelApiError(503, "UNAVAILABLE", "temporary"));
    fireEvent.click(screen.getByRole("button", { name: "保存基本信息" }));
    await screen.findByText("操作已接受，最新状态读取失败。请仅重试读取，不要再次提交。");
    expect(screen.getByRole("button", { name: "编辑名称与描述" })).toBeDisabled();
    expect(loadChannelPending(sessionStorage, markerKey)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "重新读取账户状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "编辑名称与描述" })).toBeEnabled());
    expect(mocks.updateAccount).toHaveBeenCalledOnce();
  });
  it("preserves a rejected metadata edit and uses a new CAS only after explicitly ending the old request", async () => {
    mocks.updateAccount.mockRejectedValueOnce(new ChannelApiError(409, "CHANNEL_REVISION_CONFLICT", "conflict"));
    await open(); settings(); fireEvent.click(screen.getByRole("button", { name: "编辑名称与描述" }));
    fireEvent.change(screen.getByLabelText("渠道名称"), { target: { value: "我的编辑" } });
    current.account.account_revision = 8;
    fireEvent.click(screen.getByRole("button", { name: "保存基本信息" }));
    await screen.findByRole("region", { name: "待确认渠道操作" });
    expect(mocks.updateAccount.mock.calls[0][2].expected_account_revision).toBe(3);
    expect(screen.getByLabelText("渠道名称")).toHaveValue("我的编辑");
    expect(screen.getByRole("button", { name: "以原请求重试确认" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "已核实状态，结束原请求并重新准备" }));
    await waitFor(() => expect(screen.queryByRole("region", { name: "待确认渠道操作" })).not.toBeInTheDocument());
    expect(mocks.updateAccount).toHaveBeenCalledOnce();
    fireEvent.click(screen.getByRole("button", { name: "保存基本信息" }));
    await waitFor(() => expect(mocks.updateAccount).toHaveBeenCalledTimes(2));
    expect(mocks.updateAccount.mock.calls[1][2]).toMatchObject({ expected_account_revision: 8, name: "我的编辑" });
    expect(mocks.updateAccount.mock.calls[1][3]).not.toBe(mocks.updateAccount.mock.calls[0][3]);
  });
  it("denies further metadata writes on 403 and keeps duplicate saves single flight", async () => {
    let reject!: (error: ChannelApiError) => void;
    mocks.updateAccount.mockImplementation(() => new Promise((_, fail) => { reject = fail; }));
    await open(); settings(); fireEvent.click(screen.getByRole("button", { name: "编辑名称与描述" }));
    const save = screen.getByRole("button", { name: "保存基本信息" }); fireEvent.click(save); fireEvent.click(save);
    expect(mocks.updateAccount).toHaveBeenCalledOnce();
    await act(async () => reject(new ChannelApiError(403, "CHANNEL_PERMISSION_DENIED", "denied")));
    expect(mocks.deny).toHaveBeenCalledOnce();
    expect(screen.getByRole("button", { name: "以原请求重试确认" })).toBeDisabled();
  });
  it("does not duplicate accepted writes when the following GET fails", async () => {
    await open(); await prepare("停用本平台接入"); mocks.get.mockRejectedValueOnce(new ChannelApiError(503, "UNAVAILABLE", "temporary")); confirm();
    await screen.findByText("操作已接受，最新状态读取失败。请仅重试读取，不要再次提交。");
    expect(screen.getByRole("button", { name: "停用本平台接入" })).toBeDisabled(); expect(loadChannelPending(sessionStorage, markerKey)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "重新读取账户状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "停用本平台接入" })).toBeEnabled()); expect(mocks.setAccountEnabled).toHaveBeenCalledOnce();
  });
  it("replays uncertain command with exactly the original key and CAS, never latest facts", async () => {
    mocks.setAccountEnabled.mockRejectedValueOnce(new ChannelApiError(0, "REQUEST_TIMEOUT", "timeout")); await open(); await prepare("停用本平台接入"); confirm();
    await screen.findByRole("region", { name: "待确认渠道操作" }); const original = mocks.setAccountEnabled.mock.calls[0];
    current.account.account_revision = 99; fireEvent.click(screen.getByRole("button", { name: "只读取当前事实" }));
    await waitFor(() => expect(mocks.get).toHaveBeenCalledTimes(3)); fireEvent.click(screen.getByRole("button", { name: "以原请求重试确认" }));
    await waitFor(() => expect(mocks.setAccountEnabled).toHaveBeenCalledTimes(2)); expect(mocks.setAccountEnabled.mock.calls[1]).toEqual(original);
  });
  it("blocks duplicate confirm clicks while a command is in flight", async () => {
    let finish!: () => void; mocks.setAccountEnabled.mockImplementation(() => new Promise<void>((resolve) => { finish = resolve; }));
    await open(); const dialog = await prepare("停用本平台接入"); const button = within(dialog).getByRole("button", { name: "确认此操作" });
    fireEvent.click(button); fireEvent.click(button); expect(mocks.setAccountEnabled).toHaveBeenCalledOnce(); await act(async () => finish());
  });
  it("keeps credential value in memory only and replays one purpose with original versions", async () => {
    mocks.updateCredential.mockRejectedValueOnce(new ChannelApiError(502, "UNAVAILABLE", "unavailable")); const storage = vi.spyOn(Object.getPrototypeOf(sessionStorage), "setItem");
    await open(); settings(); expect(screen.getByRole("button", { name: "清除 Bot Token" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "替换 Bot Token" })); fireEvent.change(screen.getByLabelText("新的 Bot Token"), { target: { value: "synthetic-private-value" } });
    const dialog = await prepare("确认替换 Bot Token"); expect(dialog).not.toHaveTextContent("synthetic-private-value"); confirm();
    await screen.findByRole("region", { name: "待确认渠道操作" }); expect(JSON.stringify(storage.mock.calls)).not.toContain("synthetic-private-value");
    expect(loadChannelPending(sessionStorage, markerKey)).toMatchObject({ secret: true, input: { purpose: "telegram.bot_token", expected_account_revision: 3, expected_credential_version: 1, action: "replace" } });
    fireEvent.click(screen.getByRole("button", { name: "以原请求重试确认" })); await waitFor(() => expect(mocks.updateCredential).toHaveBeenCalledTimes(2));
    expect(mocks.updateCredential.mock.calls[1]).toEqual(mocks.updateCredential.mock.calls[0]); expect(mocks.updateCredential.mock.calls[0][3]).toEqual({ expected_account_revision: 3, expected_credential_version: 1, action: "replace", value: "synthetic-private-value" });
  });
  it("requires original secret reentry after refresh and never automatically replays", async () => {
    saveChannelPending(sessionStorage, markerKey, { key: "original-key", createdAt: new Date().toISOString(), operation: "updateCredential", secret: true, input: { purpose: "telegram.bot_token", expected_account_revision: 2, expected_credential_version: 1, action: "replace" } });
    await open(); expect(mocks.updateCredential).not.toHaveBeenCalled(); expect(screen.getByRole("button", { name: "以原请求重试确认" })).toBeDisabled();
    fireEvent.change(screen.getByLabelText("重新输入原凭据值"), { target: { value: "synthetic-original" } }); fireEvent.click(screen.getByRole("button", { name: "以原请求重试确认" }));
    await waitFor(() => expect(mocks.updateCredential).toHaveBeenCalledWith("t", "cha_test", "telegram.bot_token", { expected_account_revision: 2, expected_credential_version: 1, action: "replace", value: "synthetic-original" }, "original-key"));
  });
  it("makes CAS conflict explicit without automatic new-key retry and denies writes on 403", async () => {
    mocks.setAccountEnabled.mockRejectedValueOnce(new ChannelApiError(409, "CHANNEL_ACCOUNT_REVISION_CONFLICT", "conflict"));
    await open(); await prepare("停用本平台接入"); confirm(); await screen.findByRole("region", { name: "待确认渠道操作" });
    expect(screen.getByRole("button", { name: "以原请求重试确认" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "已核实状态，结束原请求并重新准备" })); await waitFor(() => expect(screen.queryByRole("region", { name: "待确认渠道操作" })).not.toBeInTheDocument());
    mocks.setAccountEnabled.mockRejectedValueOnce(new ChannelApiError(403, "FORBIDDEN", "forbidden")); await prepare("停用本平台接入"); confirm(); await waitFor(() => expect(mocks.deny).toHaveBeenCalledOnce());
  });
  it("renders MEMBER read-only with separate publication, application and stale connection states", async () => {
    mocks.access.canWrite = false; current.observations = [{ connection_revision: 1, instance_id: "gateway-a", instance_epoch: "epoch-a", report_sequence: 8, state: "READY", reason_code: "", observed_at: "2026-09-06T10:00:00Z", received_at: "2026-09-06T10:01:00Z", effective_state: "STALE" }];
    await open(); expect(screen.queryByRole("button", { name: "停用本平台接入" })).not.toBeInTheDocument(); settings(); expect(screen.queryByRole("button", { name: "编辑名称与描述" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "连接诊断" })); expect(screen.getByText("事件已发布到消息流")).toBeInTheDocument(); expect(screen.getByText("尚无路由应用确认")).toBeInTheDocument(); expect(screen.getByText("观测已过期")).toBeInTheDocument(); expect(screen.queryByText("机器人在线")).not.toBeInTheDocument();
  });
  it("creates a disabled Binding separately from account enablement and passes no manifest", async () => {
    current.binding = undefined; current.account.enabled = false; await open("deployment_id=d&revision_number=2");
    fireEvent.click(screen.getByRole("button", { name: "选择固定 r2" })); const dialog = await prepare("保存固定目标"); expect(dialog).toHaveTextContent("首次保存将创建暂停状态的 Binding"); confirm();
    await waitFor(() => expect(mocks.createBinding).toHaveBeenCalledWith("t", { account_id: "cha_test", target: { deployment_id: "d", revision_number: 2 } }, expect.any(String)));
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled(); expect(mocks.setBindingEnabled).not.toHaveBeenCalled();
  });
  it("rejects invalid query preselection and keeps target switch independent of route enablement", async () => {
    await open("deployment_id=d&revision_number=latest"); expect(screen.getByText(/参数无效／重复/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "选择固定 r2" })); const dialog = await prepare("切换固定目标"); expect(dialog).toHaveTextContent("保留当前路由启停意图"); confirm();
    await waitFor(() => expect(mocks.setBindingTarget).toHaveBeenCalledWith("t", "chb_test", { expected_binding_revision: 2, target: { deployment_id: "d", revision_number: 2 } }, expect.any(String)));
    expect(mocks.setBindingEnabled).not.toHaveBeenCalled();
  });

  it("retries a refreshed secret marker with the same key after correcting an idempotency-conflicting original value", async () => {
    saveChannelPending(sessionStorage, markerKey, {
      key: "original-secret-key",
      createdAt: new Date().toISOString(),
      operation: "updateCredential",
      secret: true,
      input: {
        purpose: "telegram.bot_token",
        expected_account_revision: 2,
        expected_credential_version: 1,
        action: "replace",
      },
    });
    mocks.updateCredential.mockRejectedValueOnce(
      new ChannelApiError(409, "CHANNEL_IDEMPOTENCY_CONFLICT", "conflict"),
    );
    await open();
    fireEvent.change(screen.getByLabelText("重新输入原凭据值"), {
      target: { value: "incorrect-reentered-value" },
    });
    fireEvent.click(screen.getByRole("button", { name: "以原请求重试确认" }));

    await waitFor(() => {
      expect(screen.getByLabelText("重新输入原凭据值")).toHaveValue("");
      expect(screen.getByRole("button", { name: "以原请求重试确认" })).toBeDisabled();
    });
    expect(loadChannelPending(sessionStorage, markerKey)?.key).toBe("original-secret-key");
    fireEvent.change(screen.getByLabelText("重新输入原凭据值"), {
      target: { value: "actual-original-value" },
    });
    fireEvent.click(screen.getByRole("button", { name: "以原请求重试确认" }));

    await waitFor(() => expect(mocks.updateCredential).toHaveBeenCalledTimes(2));
    expect(mocks.updateCredential.mock.calls[0][4]).toBe("original-secret-key");
    expect(mocks.updateCredential.mock.calls[1]).toEqual([
      "t", "cha_test", "telegram.bot_token",
      { expected_account_revision: 2, expected_credential_version: 1, action: "replace", value: "actual-original-value" },
      "original-secret-key",
    ]);
    await waitFor(() => expect(loadChannelPending(sessionStorage, markerKey)).toBeNull());
  });

  it("blocks new writes when recovery storage cannot be read and resumes only after an explicit successful reread", async () => {
    const get = vi.spyOn(Object.getPrototypeOf(sessionStorage), "getItem")
      .mockImplementationOnce(() => { throw new Error("storage unavailable"); });
    await open();
    expect(screen.getByRole("alert")).toHaveTextContent("恢复记录暂时读取失败");
    expect(screen.getByRole("button", { name: "停用本平台接入" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "停用本平台接入" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled();

    get.mockRestore();
    fireEvent.click(screen.getByRole("button", { name: "重试读取恢复记录" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "停用本平台接入" })).toBeEnabled());
    await prepare("停用本平台接入");
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled();
    confirm();
    await waitFor(() => expect(mocks.setAccountEnabled).toHaveBeenCalledOnce());
  });

  it("does not issue a mutation when its pending marker cannot be saved", async () => {
    await open();
    await prepare("停用本平台接入");
    vi.spyOn(Object.getPrototypeOf(sessionStorage), "setItem")
      .mockImplementation(() => { throw new Error("storage unavailable"); });
    confirm();

    await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent(/保存失败|写入失败/));
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled();
    expect(loadChannelPending(sessionStorage, markerKey)).toBeNull();
  });

  it("clears editable, prepared and pending in-memory secrets when write access is revoked", async () => {
    const view = render(<ChannelWorkspace tenantId="t" accountId="cha_test" />);
    await screen.findByRole("heading", { name: "研究机器人" });
    settings();
    fireEvent.click(screen.getByRole("button", { name: "替换 Bot Token" }));
    fireEvent.change(screen.getByLabelText("新的 Bot Token"), {
      target: { value: "unsent-in-memory-value" },
    });
    await prepare("确认替换 Bot Token");
    mocks.access.canWrite = false;
    view.rerender(<ChannelWorkspace tenantId="t" accountId="cha_test" />);
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.queryByDisplayValue("unsent-in-memory-value")).not.toBeInTheDocument();
    expect(mocks.updateCredential).not.toHaveBeenCalled();

    mocks.access.canWrite = true;
    view.rerender(<ChannelWorkspace tenantId="t" accountId="cha_test" />);
    fireEvent.click(screen.getByRole("button", { name: "替换 Bot Token" }));
    expect(screen.getByLabelText("新的 Bot Token")).toHaveValue("");
    fireEvent.change(screen.getByLabelText("新的 Bot Token"), {
      target: { value: "uncertain-in-memory-value" },
    });
    mocks.updateCredential.mockRejectedValueOnce(new ChannelApiError(0, "REQUEST_TIMEOUT", "timeout"));
    await prepare("确认替换 Bot Token");
    confirm();
    await screen.findByRole("region", { name: "待确认渠道操作" });
    await waitFor(() => expect(screen.getByRole("button", { name: "以原请求重试确认" })).toBeEnabled());

    mocks.access.canWrite = false;
    view.rerender(<ChannelWorkspace tenantId="t" accountId="cha_test" />);
    expect(screen.queryByDisplayValue("uncertain-in-memory-value")).not.toBeInTheDocument();
    mocks.access.canWrite = true;
    view.rerender(<ChannelWorkspace tenantId="t" accountId="cha_test" />);
    expect(await screen.findByLabelText("重新输入原凭据值")).toHaveValue("");
    expect(screen.getByRole("button", { name: "以原请求重试确认" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "以原请求重试确认" }));
    expect(mocks.updateCredential).toHaveBeenCalledOnce();
  });
});


describe("Receive mode workspace", () => {
  function lp() { current.account.enabled = false; current.account.config.receive_mode = "long_polling"; current.account.credentials[1].configured = false; }
  it("only offers a separate disable action when running", async () => {
    await open(); settings();
    expect(screen.queryByRole("button", { name: "更改接收方式" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "先停用接入" })).toBeInTheDocument();
    expect(mocks.updateAccount).not.toHaveBeenCalled();
  });
  it("saves incomplete Webhook config independently while retaining the route intent", async () => {
    lp(); current.binding!.enabled = true;
    mocks.updateAccount.mockImplementation(async () => { current.account.config.receive_mode = "webhook"; current.account.account_revision += 1; current.account.connection_revision += 1; return { account: current.account, distribution: "PUBLISHED", route_generation: 1 }; });
    await open(); settings(); fireEvent.click(screen.getByRole("button", { name: "更改接收方式" }));
    expect(screen.getByRole("button", { name: "检查并保存接收方式" })).toBeDisabled();
    fireEvent.click(screen.getByRole("radio", { name: /^Webhook/ })); await prepare("检查并保存接收方式");
    expect(screen.getByRole("dialog")).toHaveTextContent("账户仍停用"); expect(screen.getByRole("dialog")).toHaveTextContent("补齐 Webhook Secret");
    confirm(); await screen.findByText(/接收方式已保存，账户仍保持停用/);
    expect(mocks.updateAccount.mock.calls[0][2]).toEqual({ expected_account_revision: 3, config: { receive_mode: "webhook" } });
    expect(mocks.setAccountEnabled).not.toHaveBeenCalled(); expect(mocks.setBindingEnabled).not.toHaveBeenCalled(); expect(mocks.updateCredential).not.toHaveBeenCalled();
    expect(current.binding!.enabled).toBe(true); expect(current.account.credentials[1].credential_version).toBe(1);
  });
  it("permits LP enable without optional Secret, but requires it for Webhook enable", async () => {
    lp(); await open(); await prepare("启用本平台接入");
    expect(screen.getByRole("dialog")).toHaveTextContent("启用长轮询");
    expect(screen.getByRole("dialog")).toHaveTextContent("未知外部 Webhook 保持冲突");
    expect(screen.getByRole("dialog")).toHaveTextContent("不主动丢弃积压");
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "取消" }));
    current.account.config.receive_mode = "webhook";
    fireEvent.click(screen.getByRole("button", { name: "启用本平台接入" }));
    await screen.findByText(/当前接收方式缺少必需凭据/);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument(); expect(mocks.setAccountEnabled).not.toHaveBeenCalled();
  });
  it("retries uncertain mode PATCH using the exact original mode, CAS and key", async () => {
    lp(); mocks.updateAccount.mockRejectedValueOnce(new ChannelApiError(0, "REQUEST_TIMEOUT", "timeout"));
    await open(); settings(); fireEvent.click(screen.getByRole("button", { name: "更改接收方式" }));
    fireEvent.click(screen.getByRole("radio", { name: /^Webhook/ })); await prepare("检查并保存接收方式"); confirm();
    await waitFor(() => expect(mocks.updateAccount).toHaveBeenCalledOnce());
    const stored = loadChannelPending(sessionStorage, markerKey);
    expect(stored?.input).toEqual({ expected_account_revision: 3, config: { receive_mode: "webhook" } });
    expect(stored?.key).toBe(mocks.updateAccount.mock.calls[0][3]);
    fireEvent.click(screen.getByRole("button", { name: "以原请求重试确认" }));
    await waitFor(() => expect(mocks.updateAccount).toHaveBeenCalledTimes(2));
    expect(mocks.updateAccount.mock.calls[1]).toEqual(mocks.updateAccount.mock.calls[0]);
  });
  it("keeps MEMBER read-only and does not invent the initial credential version", async () => {
    lp(); mocks.access.canWrite = false; await open(); settings();
    expect(screen.queryByRole("button", { name: "更改接收方式" })).not.toBeInTheDocument();
    expect(screen.getByText(/可选；已保存的值保留/)).toBeInTheDocument();
    expect(screen.getByText("telegram.webhook_secret · v1")).toBeInTheDocument();
  });
});

it("displays receiver mode/reason without making an old owner's READY current", async () => {
  current.account.config.receive_mode = "long_polling";
  current.observations = [{ connection_revision: 1, instance_id: "old", instance_epoch: "epoch", report_sequence: 1, state: "READY", reason_code: "RECEIVER_DRAINING", effective_state: "STALE", observed_at: "2026-09-07T00:00:00Z", received_at: "2026-09-07T00:00:01Z", receive_mode: "webhook", owner_epoch: 1 }];
  await open(); fireEvent.click(screen.getByRole("tab", { name: "连接诊断" }));
  expect(screen.getByText("观测已过期")).toBeInTheDocument();
  expect(screen.getByText(/历史连接观测（非当前配置）/)).toBeInTheDocument();
  expect(screen.getByText(/正在等待旧接收退出/)).toHaveTextContent("报告接收方式：Webhook");
});

it("revalidates mode-save eligibility after the page has become stale", async () => {
  current.account.enabled = false; await open(); settings();
  fireEvent.click(screen.getByRole("button", { name: "更改接收方式" }));
  fireEvent.click(screen.getByRole("radio", { name: /^长轮询/ }));
  current.account.enabled = true; current.account.account_revision += 1;
  fireEvent.click(screen.getByRole("button", { name: "检查并保存接收方式" }));
  await screen.findByText(/请先停用接入并读取明确的接收方式/);
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument(); expect(mocks.updateAccount).not.toHaveBeenCalled();
});
