import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ProfileList } from "./profile-list";
const mocks = vi.hoisted(() => ({ listProfiles: vi.fn(), createProfile: vi.fn(), push: vi.fn() }));
vi.mock("../../lib/runtime-profile-api", () => ({ runtimeProfileApi: mocks }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: mocks.push }) }));
beforeEach(() => { vi.clearAllMocks(); mocks.listProfiles.mockReset().mockResolvedValue({ runtime_profiles: [], total: 0, offset: 0, limit: 20 }); mocks.createProfile.mockReset().mockResolvedValue({ profile: { id: "created-profile" } }); });
afterEach(cleanup);
describe("Runtime Profile list", () => {
  it("creates metadata and enters the new draft using encoded tenant paths", async () => {
    const user = userEvent.setup(); render(<ProfileList tenantId="tenant/one" />); await screen.findByText("还没有运行配置");
    await user.click(screen.getByRole("button", { name: "新建运行配置" })); const dialog = screen.getByRole("dialog");
    expect(within(dialog).getByRole("textbox", { name: "配置名称" })).toHaveFocus();
    expect(within(dialog).getByRole("button", { name: "创建配置" })).toBeDisabled();
    await user.type(within(dialog).getByRole("textbox", { name: "配置名称" }), "研究配置");
    await user.click(within(dialog).getByRole("button", { name: "创建配置" }));
    expect(mocks.createProfile).toHaveBeenCalledWith("tenant/one", { name: "研究配置", description: "" });
    expect(mocks.push).toHaveBeenCalledWith("/tenants/tenant%2Fone/runtime-profiles/created-profile");
  });
  it("does not claim empty state on errors and offers a read-only retry", async () => {
    mocks.listProfiles.mockRejectedValueOnce(new Error("读取失败")); const user = userEvent.setup(); render(<ProfileList tenantId="t" />);
    await screen.findByRole("alert"); expect(screen.queryByText("还没有运行配置")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "重新读取列表" })); await screen.findByText("还没有运行配置");
  });
  it("uses offset pagination and metadata without unsupported counts or searches", async () => {
    mocks.listProfiles.mockResolvedValue({ runtime_profiles: [{ id: "p", name: "配置A", description: "", latest_revision_number: 2, created_by: "u", updated_at: "2026-09-05" }], total: 21, offset: 0, limit: 20 });
    const user = userEvent.setup(); render(<ProfileList tenantId="t" />); expect(await screen.findByText("配置A")).toBeInTheDocument();
    expect(screen.queryByRole("searchbox")).not.toBeInTheDocument(); await user.click(screen.getByRole("button", { name: "下一页" }));
    expect(mocks.listProfiles).toHaveBeenLastCalledWith("t", { offset: 20, limit: 20 });
    await screen.findByText("第 21–21 条，共 21 条"); expect(screen.getByRole("button", { name: "下一页" })).toBeDisabled();
  });
  it("closes with Escape and returns keyboard focus to the opener", async () => {
    const user = userEvent.setup(); render(<ProfileList tenantId="t" />); await screen.findByText("还没有运行配置"); const opener = screen.getByRole("button", { name: "新建运行配置" });
    await user.click(opener); await user.keyboard("{Escape}"); expect(screen.queryByRole("dialog")).not.toBeInTheDocument(); expect(opener).toHaveFocus();
  });

  it.each([400, 401, 403, 409, 422])("treats HTTP %s as a known rejection and never reflects server details", async (status) => {
    mocks.createProfile.mockRejectedValueOnce({ status, message: "do-not-reflect-private-server-message" });
    const user = userEvent.setup(); render(<ProfileList tenantId="t" />); await screen.findByText("还没有运行配置");
    await user.click(screen.getByRole("button", { name: "新建运行配置" }));
    const dialog = screen.getByRole("dialog");
    await user.type(within(dialog).getByRole("textbox", { name: "配置名称" }), "待更正配置");
    await user.click(within(dialog).getByRole("button", { name: "创建配置" }));
    const alert = await within(dialog).findByRole("alert");
    expect(alert).not.toHaveTextContent("结果未确认");
    expect(alert).not.toHaveTextContent("do-not-reflect");
    expect(within(dialog).getByRole("textbox", { name: "配置名称" })).toHaveValue("待更正配置");
    expect(within(dialog).getByRole("button", { name: "创建配置" })).toBeEnabled();
    expect(within(dialog).queryByRole("button", { name: "关闭并刷新列表核实" })).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "创建配置" }));
    expect(mocks.createProfile).toHaveBeenCalledTimes(2);
  });

  it.each([new TypeError("response lost"), { status: 503 }, { status: 408 }])("blocks repeated creation until refreshed and explicitly reviewed after an uncertain result", async (failure) => {
    mocks.createProfile.mockRejectedValueOnce(failure);
    const user = userEvent.setup(); render(<ProfileList tenantId="t" />); await screen.findByText("还没有运行配置");
    await user.click(screen.getByRole("button", { name: "新建运行配置" }));
    const dialog = screen.getByRole("dialog");
    await user.type(within(dialog).getByRole("textbox", { name: "配置名称" }), "重试配置");
    await user.type(within(dialog).getByRole("textbox", { name: "描述" }), "保留元信息");
    await user.click(within(dialog).getByRole("button", { name: "创建配置" }));
    await within(dialog).findByText(/已暂停再次提交/);
    expect(within(dialog).getByRole("button", { name: "创建配置" })).toBeDisabled();
    fireEvent.submit(dialog.querySelector("form")!);
    expect(mocks.createProfile).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "新建运行配置" })).toBeDisabled();
    await user.click(within(dialog).getByRole("button", { name: "关闭并刷新列表核实" }));
    await screen.findByRole("region", { name: "核实创建结果" });
    await waitFor(() => expect(screen.getByRole("button", { name: "已核实列表，仍要重新创建" })).toBeEnabled());
    expect(mocks.listProfiles).toHaveBeenCalledTimes(2);
    expect(mocks.createProfile).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "已核实列表，仍要重新创建" }));
    const reopened = screen.getByRole("dialog");
    expect(within(reopened).getByRole("textbox", { name: "配置名称" })).toHaveValue("重试配置");
    expect(within(reopened).getByRole("textbox", { name: "描述" })).toHaveValue("保留元信息");
    expect(mocks.createProfile).toHaveBeenCalledTimes(1);
    await user.click(within(reopened).getByRole("button", { name: "创建配置" }));
    expect(mocks.createProfile).toHaveBeenCalledTimes(2);
    expect(mocks.push).toHaveBeenCalledWith("/tenants/t/runtime-profiles/created-profile");
  });

  it("keeps the uncertain-result gate after Escape and a failed verification read", async () => {
    mocks.createProfile.mockRejectedValueOnce(new TypeError("response lost"));
    const user = userEvent.setup(); render(<ProfileList tenantId="t" />); await screen.findByText("还没有运行配置");
    await user.click(screen.getByRole("button", { name: "新建运行配置" }));
    await user.type(screen.getByRole("textbox", { name: "配置名称" }), "结果待核实");
    await user.click(screen.getByRole("button", { name: "创建配置" }));
    await screen.findByText(/已暂停再次提交/);
    await user.keyboard("{Escape}");
    expect(screen.getByRole("button", { name: "新建运行配置" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "已核实列表，仍要重新创建" })).toBeDisabled();
    mocks.listProfiles.mockRejectedValueOnce(new Error("读取失败"));
    await user.click(screen.getByRole("button", { name: "刷新列表核实创建结果" }));
    await screen.findByRole("alert");
    expect(screen.getByRole("button", { name: "已核实列表，仍要重新创建" })).toBeDisabled();
    expect(mocks.createProfile).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "刷新列表核实创建结果" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "已找到配置，结束核实" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "已找到配置，结束核实" }));
    expect(screen.queryByRole("region", { name: "核实创建结果" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "新建运行配置" })).toBeEnabled();
    expect(mocks.createProfile).toHaveBeenCalledTimes(1);
  });
});
