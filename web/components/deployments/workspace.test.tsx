import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DeploymentWorkspace } from "./workspace";
import { DeploymentMetadataDialog } from "./metadata-dialog";
import { DeploymentApiError } from "../../lib/deployment-api";
import { preparationKey, loadPreparation, savePreparation, selectionFrom } from "../../lib/deployment-editor-state";
import { agentVersion, deployment, input, invalidReport, profileRevision, revision, validReport } from "../../test/deployment-fixtures";

const mocks = vi.hoisted(() => ({
  api: { create: vi.fn(), get: vi.fn(), update: vi.fn(), validate: vi.fn(), publish: vi.fn(), list: vi.fn(), listRevisions: vi.fn(), getRevision: vi.fn() },
  control: { getMe: vi.fn(), getTenant: vi.fn(), getAgentVersion: vi.fn(), listAgents: vi.fn(), listAgentVersions: vi.fn() },
  profile: { listProfiles: vi.fn(), listRevisions: vi.fn(), getRevision: vi.fn() }, push: vi.fn(), replace: vi.fn(),
}));
vi.mock("../../lib/deployment-api", async (original) => ({ ...await original<typeof import("../../lib/deployment-api")>(), deploymentApi: mocks.api }));
vi.mock("../../lib/control-api", () => ({ controlApi: mocks.control }));
vi.mock("../../lib/runtime-profile-api", () => ({ runtimeProfileApi: mocks.profile }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: mocks.push, replace: mocks.replace }) }));
const query = "prepare=1&agent=a&version=3&profile=p&revision=2";
beforeEach(() => {
  vi.resetAllMocks(); sessionStorage.clear(); window.history.replaceState({}, "", "/");
  mocks.api.get.mockResolvedValue(deployment); mocks.api.create.mockResolvedValue({ deployment }); mocks.api.validate.mockResolvedValue(validReport); mocks.api.publish.mockResolvedValue({ revision, validation: validReport }); mocks.api.getRevision.mockResolvedValue(revision);
  mocks.api.list.mockResolvedValue({ deployments: [deployment], total: 1, offset: 0, limit: 20 }); mocks.api.listRevisions.mockResolvedValue({ revisions: [revision], total: 1, offset: 0, limit: 20 });
  mocks.control.getMe.mockResolvedValue({ user: { id: "u" } }); mocks.control.getTenant.mockResolvedValue({ role: "OWNER" }); mocks.control.getAgentVersion.mockResolvedValue(agentVersion);
  mocks.control.listAgents.mockResolvedValue({ agents: [{ id: "a", name: "研究流程", latest_version_number: 4 }], total: 1 });
  mocks.control.listAgentVersions.mockResolvedValue({ versions: [agentVersion, { ...agentVersion, version_number: 4 }], total: 2 });
  mocks.profile.listProfiles.mockResolvedValue({ runtime_profiles: [{ id: "p", name: "研究资源", latest_revision_number: 2 }], total: 1 });
  mocks.profile.listRevisions.mockResolvedValue({ revisions: [profileRevision], total: 1 }); mocks.profile.getRevision.mockResolvedValue(profileRevision);
});
afterEach(cleanup);
async function setup(existing = true, customQuery = query) {
  const result = render(<DeploymentWorkspace tenantId="t" deploymentId={existing ? "d" : undefined} query={customQuery} />);
  await screen.findByRole("heading", { name: existing ? deployment.name : "新建部署" });
  await screen.findByText("未声明 · 不启用");
  if (existing && !screen.queryByRole("button", { name: "重试确认本次发布" })) await waitFor(() => expect(screen.getByRole("button", { name: "重新校验" })).toBeEnabled());
  return result;
}
async function validate() {
  fireEvent.click(screen.getByRole("button", { name: "重新校验" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "发布部署版本" })).toBeEnabled());
}
async function publish() {
  fireEvent.click(screen.getByRole("button", { name: "发布部署版本" }));
  fireEvent.click(await screen.findByRole("button", { name: "确认发布" }));
}
describe("Deployment publication workspace", () => {
  it("defers identity creation until explicit action and carries fixed sources to validation", async () => {
    await setup(false); expect(mocks.api.create).not.toHaveBeenCalled();
    fireEvent.change(screen.getByRole("textbox", { name: "部署名称" }), { target: { value: " 新部署 " } });
    fireEvent.click(screen.getByRole("button", { name: "创建部署并校验" }));
    await waitFor(() => expect(mocks.replace).toHaveBeenCalledWith("/tenants/t/deployments/d?prepare=1&validate=1&agent=a&version=3&profile=p&revision=2"));
    expect(mocks.api.create).toHaveBeenCalledWith("t", { name: "新部署", description: "" }, expect.any(String));
    expect(mocks.api.validate).not.toHaveBeenCalled();
    expect(loadPreparation(sessionStorage, preparationKey("u", "t", "d"))?.pending).toBeNull();
  });
  it("automatically validates after an acknowledged creation without creating again", async () => {
    await setup(true, query + "&validate=1");
    await waitFor(() => expect(mocks.api.validate).toHaveBeenCalledWith("t", "d", input));
    expect(mocks.api.create).not.toHaveBeenCalled();
  });
  it("validates raw fixed input then publishes with null latest and navigates returned revision", async () => {
    await setup(); await validate(); await publish();
    await waitFor(() => expect(mocks.push).toHaveBeenCalledWith("/tenants/t/deployments/d/revisions/1"));
    expect(mocks.api.validate).toHaveBeenCalledWith("t", "d", input);
    expect(mocks.api.publish).toHaveBeenCalledWith("t", "d", { expected_latest_revision_number: null, input }, expect.any(String));
  });
  it("retains a failed validation and routes missing resource diagnostics to Profile, not Agent", async () => {
    mocks.api.validate.mockResolvedValue(invalidReport); await setup();
    fireEvent.click(screen.getByRole("button", { name: "重新校验" }));
    expect(await screen.findByText(/校验未通过 · 1 错误/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "发布部署版本" })).toBeDisabled();
    expect(screen.getByRole("link", { name: /去 Profile 草稿修复/ })).toHaveAttribute("href", expect.stringContaining("/runtime-profiles/p?returnTo="));
    expect(mocks.api.create).not.toHaveBeenCalled();
  });
  it("allows a warning-only report but invalidates it on source selection change", async () => {
    mocks.api.validate.mockResolvedValue({ ...validReport, diagnostics: [{ ...invalidReport.diagnostics[0], severity: "warning", code: "DEPLOYMENT_UNUSED_REQUIREMENT" }] });
    await setup(); await validate();
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Agent 固定版本" })).toBeEnabled());
    fireEvent.change(screen.getByRole("combobox", { name: "Agent 固定版本" }), { target: { value: "4" } });
    expect(screen.getByRole("button", { name: "发布部署版本" })).toBeDisabled(); expect(screen.queryByText(/校验通过 ·/)).not.toBeInTheDocument();
  });
  it("restores edited selections on refresh instead of overwriting with the original entry URL", async () => {
    const view = await setup();
    fireEvent.change(screen.getByRole("combobox", { name: "Agent 固定版本" }), { target: { value: "4" } });
    view.unmount(); await setup();
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Agent 固定版本" })).toHaveValue("4"));
    expect(screen.getByRole("button", { name: "发布部署版本" })).toBeDisabled();
  });
  it("ignores an old validation response after returning from a source repair", async () => {
    let resolve!: (value: typeof validReport) => void;
    mocks.api.validate.mockReturnValue(new Promise((r) => { resolve = r; }));
    await setup(); fireEvent.click(screen.getByRole("button", { name: "重新校验" })); fireEvent(window, new Event("focus"));
    await act(async () => resolve(validReport));
    expect(screen.getByRole("button", { name: "发布部署版本" })).toBeDisabled(); expect(screen.queryByText(/校验通过 ·/)).not.toBeInTheDocument();
  });
  it("lets MEMBER validate but never publish", async () => {
    mocks.control.getTenant.mockResolvedValue({ role: "MEMBER" }); await setup();
    fireEvent.click(screen.getByRole("button", { name: "重新校验" })); await screen.findByText(/校验通过 ·/);
    expect(screen.getByRole("button", { name: "发布部署版本" })).toBeDisabled(); expect(mocks.api.publish).not.toHaveBeenCalled();
    expect(screen.getByText(/MEMBER 可准备和校验/)).toBeInTheDocument();
  });
  it("retries a timed-out publish after refresh using the exact original key and body", async () => {
    mocks.api.publish.mockRejectedValueOnce(new TypeError("response lost")); const view = await setup(); await validate(); await publish();
    const retry = await screen.findByRole("button", { name: "重试确认本次发布" }); expect(retry).toBeEnabled();
    const original = mocks.api.publish.mock.calls[0]; expect(loadPreparation(sessionStorage, preparationKey("u", "t", "d"))?.pending?.key).toBe(original[3]);
    view.unmount(); await setup(true, "prepare=1&agent=a&version=4&profile=p&revision=2");
    fireEvent.click(await screen.findByRole("button", { name: "重试确认本次发布" }));
    await waitFor(() => expect(mocks.api.publish).toHaveBeenCalledTimes(2)); expect(mocks.api.publish.mock.calls[1]).toEqual(original);
    await waitFor(() => expect(mocks.push).toHaveBeenCalled());
  });
  it("retries a timed-out create without another key or a second body", async () => {
    mocks.api.create.mockRejectedValueOnce(new TypeError("response lost")); await setup(false);
    fireEvent.change(screen.getByRole("textbox", { name: "部署名称" }), { target: { value: "创建一次" } });
    fireEvent.click(screen.getByRole("button", { name: "创建部署并校验" }));
    fireEvent.click(await screen.findByRole("button", { name: "重试确认本次创建" }));
    await waitFor(() => expect(mocks.api.create).toHaveBeenCalledTimes(2)); expect(mocks.api.create.mock.calls[1]).toEqual(mocks.api.create.mock.calls[0]);
  });
  it("requires history review and revalidation before a new CAS baseline and key", async () => {
    mocks.api.publish.mockRejectedValueOnce(new DeploymentApiError(409, "DEPLOYMENT_LATEST_REVISION_CONFLICT", "conflict"));
    await setup(); await validate(); await publish();
    await screen.findByText("请求冲突，等待确认"); expect(screen.getByRole("button", { name: "重试确认本次发布" })).toBeDisabled();
    mocks.api.get.mockResolvedValue({ ...deployment, latest_revision_number: 2 });
    fireEvent.click(screen.getByRole("button", { name: "读取最新发布记录" }));
    fireEvent.click(await screen.findByRole("button", { name: "已核实记录，结束原请求并重新准备" }));
    expect(screen.getByRole("button", { name: "发布部署版本" })).toBeDisabled(); await validate(); await publish();
    await waitFor(() => expect(mocks.api.publish).toHaveBeenCalledTimes(2));
    expect(mocks.api.publish.mock.calls[1][2]).toEqual({ expected_latest_revision_number: 2, input }); expect(mocks.api.publish.mock.calls[1][3]).not.toBe(mocks.api.publish.mock.calls[0][3]);
  });
  it("renders publish-time 422 diagnostics and retains the selected sources", async () => {
    mocks.api.publish.mockRejectedValue(new DeploymentApiError(422, "DEPLOYMENT_REVISION_INVALID", "invalid", invalidReport));
    await setup(); await validate(); await publish();
    await screen.findByText(/校验未通过 · 1 错误/); expect(screen.getByRole("button", { name: "发布部署版本" })).toBeDisabled();
    expect(loadPreparation(sessionStorage, preparationKey("u", "t", "d"))).toMatchObject({ selection: selectionFrom(input), pending: null });
  });
  it("revokes publication UI after server 403", async () => {
    mocks.api.publish.mockRejectedValue(new DeploymentApiError(403, "TENANT_FORBIDDEN", "forbidden"));
    await setup(); await validate(); await publish(); await screen.findByText(/MEMBER 可准备和校验/);
    expect(screen.getByRole("button", { name: "发布部署版本" })).toBeDisabled();
  });
  it("keeps source read errors recoverable and avoids validating missing snapshots", async () => {
    mocks.profile.getRevision.mockRejectedValueOnce(new Error("source unavailable"));
    render(<DeploymentWorkspace tenantId="t" deploymentId="d" query={query} />);
    fireEvent.click(await screen.findByRole("button", { name: "重试读取固定来源" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "重新校验" })).toBeEnabled()); expect(mocks.api.validate).not.toHaveBeenCalled();
  });
  it("loads the exact requested historical input for comparison", async () => {
    await setup(true, "prepare=1&from=1"); expect(mocks.api.getRevision).toHaveBeenCalledWith("t", "d", 1);
    expect(screen.getByRole("heading", { name: "相对 r1 的来源变化" })).toBeInTheDocument(); expect(mocks.api.publish).not.toHaveBeenCalled();
  });
  it("shares only fixed source identifiers, not profile data or credentials", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined); Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    await setup(); fireEvent.click(screen.getByRole("button", { name: "复制准备链接" }));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(expect.stringContaining("/tenants/t/deployments/d?prepare=1&agent=a&version=3&profile=p&revision=2")));
    expect(writeText.mock.calls[0][0]).not.toContain("config");
  });
});

describe("Deployment metadata CAS", () => {
  it("preserves local text on conflict and only uses a refreshed baseline after confirmation", async () => {
    const onSaved = vi.fn(); mocks.api.update.mockRejectedValueOnce(new DeploymentApiError(409, "DEPLOYMENT_METADATA_REVISION_CONFLICT", "conflict")).mockResolvedValue({ ...deployment, name: "local", metadata_revision: 3 });
    render(<DeploymentMetadataDialog tenantId="t" deployment={deployment} onClose={vi.fn()} onSaved={onSaved} />);
    fireEvent.change(screen.getByRole("textbox", { name: "部署名称" }), { target: { value: "local" } }); fireEvent.click(screen.getByRole("button", { name: "保存信息" }));
    await screen.findByRole("button", { name: "读取当前部署信息" }); expect(screen.getByRole("textbox", { name: "部署名称" })).toHaveValue("local");
    mocks.api.get.mockResolvedValue({ ...deployment, name: "remote", metadata_revision: 2 }); fireEvent.click(screen.getByRole("button", { name: "读取当前部署信息" }));
    fireEvent.click(await screen.findByRole("button", { name: "确认以当前信息为基线" })); fireEvent.click(screen.getByRole("button", { name: "保存信息" }));
    await waitFor(() => expect(onSaved).toHaveBeenCalled()); expect(mocks.api.update.mock.calls[1][2]).toMatchObject({ name: "local", expected_metadata_revision: 2 });
  });
});
