import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ProfileWorkspace, diagnosticTarget } from "./profile-workspace";
import { RuntimeProfileApiError, type ProfileDraft, type RuntimeProfile } from "../../lib/runtime-profile-api";

const mocks = vi.hoisted(() => ({ api: { getProfile: vi.fn(), getDraft: vi.fn(), saveDraft: vi.fn(), validateDraft: vi.fn(), publishRevision: vi.fn(), updateProfile: vi.fn(), listRevisions: vi.fn() }, getTenant: vi.fn(), push: vi.fn() }));
vi.mock("../../lib/runtime-profile-api", async (original) => ({ ...await original<typeof import("../../lib/runtime-profile-api")>(), runtimeProfileApi: mocks.api }));
vi.mock("../../lib/control-api", () => ({ controlApi: { getTenant: mocks.getTenant } }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: mocks.push }) }));
const profile: RuntimeProfile = { id: "p", tenant_id: "t", name: "研究配置", description: "两个资源", latest_revision_number: null, created_by: "u", created_at: "2026-09-05", updated_at: "2026-09-05" };
const draft: ProfileDraft = { profile_id: "p", tenant_id: "t", draft_revision: 3, schema_version: "v1", credential_protocol_version: "v1", updated_by: "u", updated_at: "2026-09-05", config: { models: { primary: { kind: "openai_compatible", model: "before", base_url: "https://example.com/v1", capabilities: ["chat"] } }, tools: {}, knowledge: {}, storage: {} }, credential_states: { models: { primary: { api_key: { configured: true, status: "active", credential_revision: 1, association_token: "association-internal" } } } } };
const changedDraft = (): ProfileDraft => ({ ...draft, draft_revision: 4, config: { ...draft.config, models: { primary: { ...draft.config.models.primary, model: "after" } } } });
const valid = { valid: true, schema_version: "v1", draft_revision: 4, diagnostics: [] };
async function setup() { const user = userEvent.setup(); render(<ProfileWorkspace tenantId="t" profileId="p" />); await screen.findByRole("heading", { name: "研究配置" }); return user; }
async function editModel(user: ReturnType<typeof userEvent.setup>, name = "after") { const input = screen.getByRole("textbox", { name: "模型名称" }); await user.clear(input); await user.type(input, name); }
beforeEach(() => {
  vi.clearAllMocks(); mocks.api.getProfile.mockResolvedValue(profile); mocks.getTenant.mockResolvedValue({ role: "OWNER" });
  mocks.api.getDraft.mockReset().mockResolvedValue(draft); mocks.api.saveDraft.mockReset().mockResolvedValue({ profile_id: "p", draft_revision: 4, updated_at: "2026-09-05" });
  mocks.api.validateDraft.mockReset().mockResolvedValue(valid); mocks.api.publishRevision.mockReset().mockResolvedValue({ revision: { revision_number: 1 } });
  mocks.api.listRevisions.mockResolvedValue({ revisions: [], total: 0, offset: 0, limit: 20 }); mocks.api.updateProfile.mockResolvedValue({ ...profile, name: "修改后的配置" });
});
afterEach(cleanup);

describe("Runtime Profile workspace", () => {
  it("saves full config, reads receipt state, validates exact returned revision then publishes", async () => {
    const user = await setup(); await editModel(user); mocks.api.getDraft.mockResolvedValue(changedDraft());
    const order: string[] = [];
    mocks.api.saveDraft.mockImplementation(async () => { order.push("save"); return { profile_id: "p", draft_revision: 4, updated_at: "now" }; });
    mocks.api.getDraft.mockImplementation(async () => { order.push("get"); return changedDraft(); });
    mocks.api.validateDraft.mockImplementation(async () => { order.push("validate"); return valid; });
    mocks.api.publishRevision.mockImplementation(async () => { order.push("publish"); return { revision: { revision_number: 1 } }; });
    await user.click(screen.getByRole("button", { name: "校验并发布" }));
    expect(await screen.findByRole("link", { name: "查看版本 r1 →" })).toHaveAttribute("href", "/tenants/t/runtime-profiles/p/revisions/1");
    expect(order).toEqual(["save", "get", "validate", "publish"]);
    expect(mocks.api.saveDraft).toHaveBeenCalledWith("t", "p", { expected_draft_revision: 3, credential_protocol_version: "v1", config: changedDraft().config }, expect.any(String));
    expect(mocks.api.validateDraft).toHaveBeenCalledWith("t", "p", { expected_revision: 4 });
    expect(mocks.api.publishRevision).toHaveBeenCalledWith("t", "p", { expected_revision: 4 });
  });
  it("stops publication when another writer advances the draft after our receipt", async () => {
    const user = await setup(); await editModel(user);
    mocks.api.getDraft.mockResolvedValue({ ...changedDraft(), draft_revision: 5, config: { ...draft.config, models: { primary: { ...draft.config.models.primary, model: "someone-else" } } } });
    await user.click(screen.getByRole("button", { name: "校验并发布" }));
    await screen.findByText(/服务器已更新到 r5/);
    expect(screen.getByRole("textbox", { name: "模型名称" })).toHaveValue("after");
    expect(mocks.api.validateDraft).not.toHaveBeenCalled(); expect(mocks.api.publishRevision).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "校验并发布" })).toBeDisabled();
  });
  it("does not resave an unchanged draft before validation", async () => {
    const user = await setup(); await user.click(screen.getByRole("button", { name: "校验" }));
    await screen.findByText(/静态校验通过/); expect(mocks.api.saveDraft).not.toHaveBeenCalled();
    expect(mocks.api.validateDraft).toHaveBeenCalledWith("t", "p", { expected_revision: 3 });
  });
  it("does not publish invalid drafts and diagnostics focus the named model field", async () => {
    mocks.api.validateDraft.mockResolvedValue({ ...valid, valid: false, draft_revision: 3, diagnostics: [{ code: "MISSING_MODEL", severity: "error", resource_kind: "model", resource_key: "primary", pointer: "/models/primary/model", message: "请填写模型" }] });
    const user = await setup(); await user.click(screen.getByRole("button", { name: "校验并发布" }));
    await user.click(await screen.findByRole("button", { name: /请填写模型/ }));
    expect(screen.getByRole("textbox", { name: "模型名称" })).toHaveFocus(); expect(mocks.api.publishRevision).not.toHaveBeenCalled();
  });
  it("retries an uncertain save using exactly the same body and idempotency key", async () => {
    mocks.api.saveDraft.mockRejectedValueOnce(new TypeError("Network unavailable"));
    const user = await setup(); await editModel(user); mocks.api.getDraft.mockResolvedValue(changedDraft());
    await user.click(screen.getByRole("button", { name: "保存草稿" })); await screen.findByRole("alert");
    await user.click(screen.getByRole("button", { name: "保存草稿" })); await screen.findByText("草稿 r4 已保存。");
    expect(mocks.api.saveDraft.mock.calls[1]).toEqual(mocks.api.saveDraft.mock.calls[0]);
  });
  it("uses a new idempotency key after the user changes an uncertain request", async () => {
    mocks.api.saveDraft.mockRejectedValueOnce(new RuntimeProfileApiError(503, "UNAVAILABLE", "暂时失败"));
    const user = await setup(); await editModel(user); await user.click(screen.getByRole("button", { name: "保存草稿" })); await screen.findByRole("alert");
    await editModel(user, "different"); await user.click(screen.getByRole("button", { name: "保存草稿" }));
    await waitFor(() => expect(mocks.api.saveDraft).toHaveBeenCalledTimes(2));
    expect(mocks.api.saveDraft.mock.calls[0][3]).not.toBe(mocks.api.saveDraft.mock.calls[1][3]);
  });
  it("treats a successful receipt plus failed GET as saved, then retries only GET", async () => {
    const user = await setup(); await editModel(user); mocks.api.getDraft.mockRejectedValueOnce(new Error("read failed")).mockResolvedValue(changedDraft());
    await user.click(screen.getByRole("button", { name: "校验并发布" }));
    expect(await screen.findByText(/草稿已保存；最新凭证状态读取失败/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "校验并发布" })).toBeDisabled();
    expect(mocks.api.validateDraft).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "重新读取已保存状态" }));
    await screen.findByText("已读取最新草稿 r4。"); expect(mocks.api.saveDraft).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "保存草稿" })).toBeDisabled();
  });
  it("preserves non-secret edits on 409, clears pending secrets and requires explicit reload", async () => {
    mocks.api.saveDraft.mockRejectedValue(new RuntimeProfileApiError(409, "REVISION_CONFLICT", "冲突"));
    const user = await setup(); await editModel(user);
    await user.selectOptions(screen.getByRole("combobox", { name: "API Key 操作" }), "replace");
    await user.type(screen.getByLabelText("API Key 新值"), "test-only-sensitive-value");
    await user.click(screen.getByRole("button", { name: "保存草稿" }));
    await screen.findByText(/本地非密钥配置已保留/);
    expect(screen.getByRole("textbox", { name: "模型名称" })).toHaveValue("after"); expect(screen.queryByLabelText("API Key 新值")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "保存草稿" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "重新读取最新草稿" }));
    expect(mocks.api.getDraft).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "保留本地编辑" })); expect(screen.getByRole("textbox", { name: "模型名称" })).toHaveValue("after");
  });
  it("never shows write-only credentials or association tokens in config JSON", async () => {
    const user = await setup(); await user.selectOptions(screen.getByRole("combobox", { name: "API Key 操作" }), "replace");
    await user.type(screen.getByLabelText("API Key 新值"), "synthetic-private-value");
    await user.click(screen.getByRole("button", { name: "查看非密钥配置 JSON" }));
    const json = screen.getByRole("dialog"); expect(json.textContent).toContain("openai_compatible");
    expect(json.textContent).not.toContain("synthetic-private-value"); expect(json.textContent).not.toContain("association-internal");
  });
  it("warns before internal navigation and before browser unload with dirty config", async () => {
    const user = await setup(); await editModel(user); const event = new Event("beforeunload", { cancelable: true }); window.dispatchEvent(event); expect(event.defaultPrevented).toBe(true);
    await user.click(screen.getByRole("link", { name: "← 运行配置列表" }));
    expect(await screen.findByRole("dialog", { name: "离开未保存的配置？" })).toBeInTheDocument(); expect(mocks.push).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "继续编辑" })); expect(screen.getByRole("textbox", { name: "模型名称" })).toHaveValue("after");
  });
  it("allows MEMBER authoring and publishing but offers no credential controls", async () => {
    mocks.getTenant.mockResolvedValue({ role: "MEMBER" }); const user = await setup();
    expect(screen.queryByRole("combobox", { name: "API Key 操作" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "校验并发布" })); expect(await screen.findByRole("link", { name: "查看版本 r1 →" })).toBeInTheDocument();
  });
  it("edits metadata and reads version summaries only when that tab is selected", async () => {
    const user = await setup(); expect(mocks.api.listRevisions).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "编辑信息" })); const dialog = screen.getByRole("dialog");
    const name = within(dialog).getByRole("textbox", { name: "配置名称" }); await user.clear(name); await user.type(name, "修改后的配置");
    await user.click(within(dialog).getByRole("button", { name: "保存信息" })); await screen.findByRole("heading", { name: "修改后的配置" });
    await user.click(screen.getByRole("tab", { name: "版本记录" })); await screen.findByText("尚无发布版本");
    expect(mocks.api.listRevisions).toHaveBeenCalledWith("t", "p", { offset: 0, limit: 20 });
  });
  it("maps singular kinds and JSON pointer escapes without a guessed category", () => {
    const base = { code: "INVALID", severity: "error" as const, pointer: "/config/tools/search/server_url", resource_key: null, resource_kind: null, message: "bad" };
    expect(diagnosticTarget(base)).toEqual({ category: "tools", name: "search", pointer: base.pointer });
    expect(diagnosticTarget({ ...base, pointer: "/tools/a~1b~0c/server_url" })?.name).toBe("a/b~c");
    expect(diagnosticTarget({ ...base, pointer: "/schema_version" })).toBeNull();
  });
});

it("routes executor diagnostics to the existing Profile editor category", () => {
  const diagnostic = { code: "INVALID", severity: "error" as const, pointer: "/executors/sandbox/kind", resource_key: "sandbox", resource_kind: "executor" as const, message: "bad" };
  expect(diagnosticTarget(diagnostic)).toEqual({ category: "executors", name: "sandbox", pointer: diagnostic.pointer });
  expect(diagnosticTarget({ ...diagnostic, resource_kind: null, resource_key: null, pointer: "/config/executors/sandbox/kind" })?.category).toBe("executors");
});
