import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChannelApiError } from "../../lib/channel-api";
import { sampleChannelAccount } from "../../test/channel-fixtures";
import { AccountList } from "./account-list";

const mocks = vi.hoisted(() => ({ list: vi.fn(), get: vi.fn(), reload: vi.fn(), deny: vi.fn(), access: { userId: "u", canWrite: true, loading: false, error: "" } }));
vi.mock("../../lib/channel-api", async (original) => ({ ...await original<typeof import("../../lib/channel-api")>(), channelApi: { listAccounts: mocks.list, getAccount: mocks.get } }));
vi.mock("./use-channel-access", () => ({ useChannelAccess: () => ({ ...mocks.access, reload: mocks.reload, denyWrites: mocks.deny }) }));
beforeEach(() => {
  vi.resetAllMocks(); mocks.access = { userId: "u", canWrite: true, loading: false, error: "" };
  mocks.list.mockResolvedValue({ accounts: [sampleChannelAccount] });
});
afterEach(cleanup);

describe("Channel account list", () => {
  it("shows stored metadata and cursor pagination without N+1 or invented online state", async () => {
    mocks.list.mockResolvedValueOnce({ accounts: [sampleChannelAccount], next_cursor: "cha_test" }).mockResolvedValueOnce({ accounts: [{ ...sampleChannelAccount, account_id: "cha_second", name: "第二个账户" }] });
    render(<AccountList tenantId="t" />);
    await screen.findByText("研究机器人");
    expect(mocks.list).toHaveBeenCalledWith("t", undefined, 50);
    expect(screen.getByText("配置已启用")).toBeInTheDocument();
    expect(screen.queryByText("在线")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "加载更多" }));
    await screen.findByText("第二个账户");
    expect(mocks.list).toHaveBeenLastCalledWith("t", "cha_test", 50);
    expect(screen.getByText("已加载 2 个账户")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "加载更多" })).not.toBeInTheDocument();
    expect(mocks.get).not.toHaveBeenCalled();
  });

  it("distinguishes raw module 404 from an empty page and retries", async () => {
    mocks.list.mockRejectedValueOnce(new ChannelApiError(404, "CHANNEL_MODULE_UNAVAILABLE", "raw 404")).mockResolvedValueOnce({ accounts: [] });
    render(<AccountList tenantId="t" />);
    expect(await screen.findByRole("alert")).toHaveTextContent("当前后端未提供渠道管理接口");
    expect(screen.queryByText("还没有渠道账户")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "重新读取渠道账户" }));
    await screen.findByText("还没有渠道账户");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("shows MEMBER a read-only list without a create action", async () => {
    mocks.access.canWrite = false;
    render(<AccountList tenantId="t" />); await screen.findByText("研究机器人");
    expect(screen.getByText(/当前为只读访问/)).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /添加渠道账户/ })).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "打开工作台 →" })).toHaveAttribute("href", "/tenants/t/channels/cha_test");
  });

  it("preserves only a validated exact deployment preselection and never writes", async () => {
    render(<AccountList tenantId="t" query="deployment_id=d&revision_number=3&returnTo=https://example.invalid" />);
    const link = await screen.findByRole("link", { name: "选择此账户 →" });
    expect(link).toHaveAttribute("href", "/tenants/t/channels/cha_test?deployment_id=d&revision_number=3");
    expect(screen.getByRole("link", { name: /添加渠道账户/ })).toHaveAttribute("href", "/tenants/t/channels/new?deployment_id=d&revision_number=3");
    expect(mocks.get).not.toHaveBeenCalled();
  });

  it("announces duplicate or invalid target parameters instead of treating them as a normal selection", async () => {
    render(<AccountList tenantId="t" query="deployment_id=d&revision_number=1&revision_number=2" />);
    expect(screen.getByRole("alert")).toHaveTextContent("固定部署参数无效或重复");
    await screen.findByText("研究机器人");
    expect(screen.queryByRole("link", { name: "选择此账户 →" })).not.toBeInTheDocument();
  });

  it("retains already-loaded rows if another page fails and retries the same cursor", async () => {
    mocks.list.mockResolvedValueOnce({ accounts: [sampleChannelAccount], next_cursor: "cha_test" }).mockRejectedValueOnce(new ChannelApiError(503, "CHANNEL_DEPENDENCY_UNAVAILABLE", "unavailable")).mockResolvedValueOnce({ accounts: [] });
    render(<AccountList tenantId="t" />); fireEvent.click(await screen.findByRole("button", { name: "加载更多" }));
    await screen.findByRole("alert"); expect(screen.getByText("研究机器人")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "重新读取渠道账户" }));
    await waitFor(() => expect(mocks.list).toHaveBeenCalledTimes(3));
    expect(mocks.list).toHaveBeenLastCalledWith("t", "cha_test", 50);
  });

  it("ignores an old tenant response after navigating to another tenant", async () => {
    let resolveOld!: (value: { accounts: typeof sampleChannelAccount[] }) => void;
    mocks.list.mockReturnValueOnce(new Promise((resolve) => { resolveOld = resolve; })).mockResolvedValueOnce({ accounts: [{ ...sampleChannelAccount, tenant_id: "other", name: "其他租户账户" }] });
    const view = render(<AccountList tenantId="t" />);
    view.rerender(<AccountList tenantId="other" />); await screen.findByText("其他租户账户");
    await act(async () => { resolveOld({ accounts: [sampleChannelAccount] }); });
    expect(screen.queryByText("研究机器人")).not.toBeInTheDocument();
  });

  it("does not issue account reads before access has resolved", () => {
    mocks.access = { ...mocks.access, userId: "", canWrite: false, loading: true };
    render(<AccountList tenantId="t" />);
    expect(screen.getByRole("status")).toHaveTextContent("正在读取渠道账户");
    expect(mocks.list).not.toHaveBeenCalled();
  });
});


it("shows LP mode and complete required credentials despite an unconfigured optional Secret", async () => {
  mocks.list.mockResolvedValue({ accounts: [{ ...sampleChannelAccount, config: { ...sampleChannelAccount.config, receive_mode: "long_polling" }, credentials: sampleChannelAccount.credentials.map((c) => ({ ...c, configured: c.purpose === "telegram.bot_token" })) }] });
  render(<AccountList tenantId="t" />); await screen.findByText("研究机器人");
  expect(screen.getByText("长轮询")).toBeInTheDocument(); expect(screen.getByText("必需 1 / 1 项已配置")).toBeInTheDocument();
  expect(mocks.get).not.toHaveBeenCalled();
});
