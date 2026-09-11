import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { ProfileConfig, ProfileRevision } from "../../lib/runtime-profile-api";
import { ProfileRevisionDetail } from "./profile-revision";

const api = vi.hoisted(() => ({ getProfile: vi.fn(), getRevision: vi.fn(), updateCredential: vi.fn(), getTenant: vi.fn() }));
vi.mock("../../lib/control-api", () => ({ controlApi: { getTenant: api.getTenant } }));
vi.mock("../../lib/runtime-profile-api", () => ({ runtimeProfileApi: api }));
vi.mock("./profile-resource-editor", () => ({
  ProfileResourceEditor: ({ config, credentials, readOnly }: { config: ProfileConfig; credentials: unknown; readOnly: boolean }) => <output data-testid="resource-editor" data-readonly={String(readOnly)} data-credentials={JSON.stringify(credentials)}>{JSON.stringify(config)}</output>,
}));

const token = "a".repeat(64);
const config = {
  models: { primary: { kind: "openai_compatible", model: "chat-model", base_url: "https://models.example.com/v1", capabilities: ["chat", "tool_call"] } },
  tools: { search: { kind: "mcp_streamable_http", server_url: "https://tools.example.com/mcp", toolset_name: "search", tool_name: "web_search", auth: { kind: "bearer" }, capability: "web.search" } },
  knowledge: { docs: { kind: "qdrant_openai", host: "knowledge.example.com", port: 6334, tls: true, collection: "docs", embedding: { model: "embedding-model", base_url: "https://models.example.com/v1", dimensions: 1536 } } },
  storage: { sessions: { kind: "postgres_state", destination: { host: "postgres.example.com", port: 5432, database: "agents", username: "app", sslmode: "require" } } },
};
const revision: ProfileRevision = {
  id: "revision-3", tenant_id: "tenant-1", profile_id: "profile-1", revision_number: 3, source_draft_revision: 7,
  schema_version: "v1", credential_protocol_version: "v1", spec_digest: `sha256:${"1".repeat(64)}`,
  published_by: "publisher-1", published_at: "2026-09-05T00:00:00Z", config,
  credential_states: { models: { primary: { api_key: { status: "active", configured: true, credential_revision: 2, association_token: token } } } },
};
const profile = { id: "profile-1", tenant_id: "tenant-1", name: "生产运行配置", description: "完整模型和工具", latest_revision_number: 3, created_by: "owner-1", created_at: "2026-09-01T00:00:00Z", updated_at: "2026-09-05T00:00:00Z" };

beforeEach(() => {
  vi.resetAllMocks();
  api.getTenant.mockResolvedValue({ role: "OWNER" });
  api.getProfile.mockResolvedValue(profile);
  api.getRevision.mockResolvedValue(revision);
  api.updateCredential.mockResolvedValue({ status: "active", credential_revision: 3 });
});
afterEach(cleanup);

async function openUpdate() {
  const user = userEvent.setup();
  render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={3} />);
  await user.click(await screen.findByRole("button", { name: "更新 Models / primary / api_key" }));
  return { user, dialog: screen.getByRole("dialog", { name: "更新凭证" }) };
}

describe("ProfileRevisionDetail", () => {
  it("separates immutable metadata, all four resource snapshots, and current credential state without exposing tokens", async () => {
    render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={3} />);
    expect(screen.getByRole("status")).toHaveTextContent("正在读取");
    expect(await screen.findByRole("heading", { name: "版本 r3" })).toBeInTheDocument();
    expect(screen.getByText("Draft r7")).toBeInTheDocument();
    expect(screen.getByText(revision.spec_digest)).toBeInTheDocument();
    expect(screen.getByText("publisher-1")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "当前凭证状态" })).toBeInTheDocument();
    expect(screen.getByTestId("resource-editor")).toHaveAttribute("data-readonly", "true");
    expect(screen.getByTestId("resource-editor")).toHaveAttribute("data-credentials", "{}");
    expect(screen.getByTestId("resource-editor")).toHaveTextContent(JSON.stringify(config));
    expect(document.body.textContent).not.toContain(token);
    expect(screen.getByText("查看脱敏配置 JSON").parentElement?.querySelector("pre")?.textContent).toBe(JSON.stringify(config, null, 2));
    expect(document.querySelector('input[type="password"]')).not.toBeInTheDocument();
  });

  it("allows MEMBER to browse configuration but hides live update actions", async () => {
    api.getTenant.mockResolvedValue({ role: "MEMBER" });
    render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={3} />);
    await screen.findByText("生产运行配置");
    expect(screen.getByText("当前角色只读；更新已发布关联的凭证需要 OWNER。")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /更新 Models/ })).not.toBeInTheDocument();
    expect(api.updateCredential).not.toHaveBeenCalled();
  });

  it("never offers live restoration of cleared or unconfigured associations", async () => {
    api.getRevision.mockResolvedValue({ ...revision, credential_states: { models: {
      primary: { api_key: { status: "cleared", configured: false, credential_revision: 3, association_token: token } },
      backup: { api_key: { status: "unconfigured", configured: false, credential_revision: 0 } },
    } } });
    render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={3} />);
    await screen.findByText("需新 Draft 关联");
    expect(screen.getByText("未关联")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /更新 Models/ })).not.toBeInTheDocument();
  });

  it("shows empty credential state and load authorization errors independently", async () => {
    api.getRevision.mockResolvedValue({ ...revision, credential_states: undefined });
    const page = render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={3} />);
    expect(await screen.findByText("没有凭证状态")).toBeInTheDocument();
    page.unmount();
    api.getRevision.mockRejectedValue({ status: 403 });
    render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={3} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("当前账户没有查看此运行配置的权限。");
    expect(screen.queryByTestId("resource-editor")).not.toBeInTheDocument();
  });

  it("rejects invalid revision numbers without calling an API", async () => {
    render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={NaN} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("版本号无效");
    expect(api.getRevision).not.toHaveBeenCalled();
  });

  it("submits the revision association with credential CAS, clears secret input, and refreshes on success", async () => {
    const { user, dialog } = await openUpdate();
    const input = within(dialog).getByLabelText("新凭证值");
    expect(input).toHaveValue("");
    expect(within(dialog).getByRole("button", { name: "确认更新凭证" })).toBeDisabled();
    await user.type(input, "test-only-replacement");
    await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
    await screen.findByText("凭证已更新，配置 Revision 与 digest 未改变。");
    expect(api.updateCredential).toHaveBeenCalledExactlyOnceWith("tenant-1", "profile-1", {
      target: { profile_revision_number: 3, category: "models", resource_name: "primary", purpose_field: "api_key", association_token: token },
      action: "replace", expected_credential_revision: 2, value: "test-only-replacement",
    }, expect.any(String));
    expect(api.getRevision).toHaveBeenCalledTimes(2);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(document.body.textContent).not.toContain("test-only-replacement");
    await user.click(screen.getByRole("button", { name: "更新 Models / primary / api_key" }));
    expect(screen.getByLabelText("新凭证值")).toHaveValue("");
  });

  it("retries an uncertain network result using the same idempotency key and identical body", async () => {
    api.updateCredential.mockRejectedValueOnce(new TypeError("network failure")).mockResolvedValueOnce({ status: "active", credential_revision: 3 });
    const { user, dialog } = await openUpdate();
    await user.type(within(dialog).getByLabelText("新凭证值"), "retry-test-value");
    await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
    await user.click(await within(dialog).findByRole("button", { name: "重试同一更新" }));
    await waitFor(() => expect(api.updateCredential).toHaveBeenCalledTimes(2));
    expect(api.updateCredential.mock.calls[1]).toEqual(api.updateCredential.mock.calls[0]);
    expect(api.updateCredential.mock.calls[1][2]).toBe(api.updateCredential.mock.calls[0][2]);
  });

  it("uses a new key after input changes instead of reusing a key with different payload", async () => {
    api.updateCredential.mockRejectedValueOnce({ status: 503, message: "must not display test-only-secret" }).mockResolvedValueOnce({ status: "active", credential_revision: 3 });
    const { user, dialog } = await openUpdate();
    const input = within(dialog).getByLabelText("新凭证值");
    await user.type(input, "first-test-value");
    await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
    await within(dialog).findByRole("button", { name: "重试同一更新" });
    expect(document.body.textContent).not.toContain("must not display");
    await user.clear(input);
    await user.type(input, "second-test-value");
    await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
    await waitFor(() => expect(api.updateCredential).toHaveBeenCalledTimes(2));
    expect(api.updateCredential.mock.calls[1][3]).not.toBe(api.updateCredential.mock.calls[0][3]);
    expect(api.updateCredential.mock.calls[1][2].value).toBe("second-test-value");
  });

  it("requires explicit impact confirmation to clear and never includes value in clear requests", async () => {
    const { user, dialog } = await openUpdate();
    await user.click(within(dialog).getByRole("radio", { name: "清除" }));
    const submit = within(dialog).getByRole("button", { name: "确认清除凭证" });
    expect(submit).toBeDisabled();
    expect(within(dialog).queryByLabelText("新凭证值")).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("checkbox", { name: /我确认清除将使共享该关联/ }));
    await user.click(submit);
    await waitFor(() => expect(api.updateCredential).toHaveBeenCalledTimes(1));
    expect(api.updateCredential.mock.calls[0][2]).toMatchObject({ action: "clear", expected_credential_revision: 2 });
    expect(api.updateCredential.mock.calls[0][2]).not.toHaveProperty("value");
  });

  it("refreshes a 409 but requires explicit confirmation and a new value before using the new target", async () => {
    api.getRevision.mockResolvedValueOnce(revision).mockResolvedValue({ ...revision, credential_states: { models: { primary: { api_key: { status: "active", configured: true, credential_revision: 4, association_token: "b".repeat(64) } } } } });
    api.updateCredential.mockRejectedValueOnce({ status: 409 }).mockResolvedValueOnce({ status: "active", credential_revision: 5 });
    const { user, dialog } = await openUpdate();
    await user.type(within(dialog).getByLabelText("新凭证值"), "first-test-value");
    await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
    await within(dialog).findByText(/最新状态：已配置 · v4/);
    expect(api.updateCredential).toHaveBeenCalledTimes(1);
    expect(within(dialog).getByLabelText("新凭证值")).toHaveValue("");
    expect(within(dialog).getByRole("button", { name: "确认更新凭证" })).toBeDisabled();
    await user.click(within(dialog).getByRole("button", { name: "确认使用最新状态" }));
    expect(within(dialog).getByRole("button", { name: "确认更新凭证" })).toBeDisabled();
    await user.type(within(dialog).getByLabelText("新凭证值"), "confirmed-test-value");
    await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
    await waitFor(() => expect(api.updateCredential).toHaveBeenCalledTimes(2));
    expect(api.updateCredential.mock.calls[1][2]).toMatchObject({ expected_credential_revision: 4, target: { association_token: "b".repeat(64) } });
    expect(api.updateCredential.mock.calls[1][3]).not.toBe(api.updateCredential.mock.calls[0][3]);
  });

  it("does not offer a live retry when conflict refresh says the association was cleared", async () => {
    api.getRevision.mockResolvedValueOnce(revision).mockResolvedValue({ ...revision, credential_states: { models: { primary: { api_key: { status: "cleared", configured: false, credential_revision: 3, association_token: token } } } } });
    api.updateCredential.mockRejectedValue({ status: 409 });
    const { user, dialog } = await openUpdate();
    await user.type(within(dialog).getByLabelText("新凭证值"), "test-value");
    await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
    await within(dialog).findByText("请返回 Draft 建立新关联并发布。");
    expect(within(dialog).queryByRole("button", { name: "确认使用最新状态" })).not.toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "确认更新凭证" })).toBeDisabled();
    expect(api.updateCredential).toHaveBeenCalledTimes(1);
  });

  it("keeps a successful mutation successful if the following GET fails and disables updates until refreshed", async () => {
    api.getRevision.mockResolvedValueOnce(revision).mockRejectedValueOnce(new TypeError("read failed")).mockResolvedValue(revision);
    const { user, dialog } = await openUpdate();
    await user.type(within(dialog).getByLabelText("新凭证值"), "test-value");
    await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
    await screen.findByText(/当前凭证状态刷新失败/);
    expect(screen.getByText("凭证已更新，配置 Revision 与 digest 未改变。")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "更新 Models / primary / api_key" })).toBeDisabled();
    expect(api.updateCredential).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "刷新状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "更新 Models / primary / api_key" })).toBeEnabled());
    expect(api.updateCredential).toHaveBeenCalledTimes(1);
  });

  it("clears credentials on Escape and restores focus to the invoking control", async () => {
    const { user, dialog } = await openUpdate();
    await user.type(within(dialog).getByLabelText("新凭证值"), "temporary-test-value");
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "更新 Models / primary / api_key" })).toHaveFocus();
    await user.click(screen.getByRole("button", { name: "更新 Models / primary / api_key" }));
    expect(screen.getByLabelText("新凭证值")).toHaveValue("");
    expect(api.updateCredential).not.toHaveBeenCalled();
  });

  it("revokes live editing and clears the value if OWNER permission was removed during the operation", async () => {
    api.updateCredential.mockRejectedValue({ status: 403, message: "test-only-secret-reflection" });
    const { user, dialog } = await openUpdate();
    await user.type(within(dialog).getByLabelText("新凭证值"), "test-only-secret");
    await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
    expect(await within(dialog).findByRole("alert")).toHaveTextContent("更新凭证需要当前租户 OWNER 权限。");
    expect(within(dialog).getByLabelText("新凭证值")).toHaveValue("");
    expect(within(dialog).getByLabelText("新凭证值")).toBeDisabled();
    expect(within(dialog).getByRole("button", { name: "确认更新凭证" })).toBeDisabled();
    expect(document.body.textContent).not.toContain("test-only-secret");
    expect(screen.queryByRole("button", { name: /更新 Models/ })).not.toBeInTheDocument();
  });

  it("blocks whitespace-only replacements without submitting a masked keep command", async () => {
    const { user, dialog } = await openUpdate();
    await user.type(within(dialog).getByLabelText("新凭证值"), "   ");
    expect(within(dialog).getByRole("button", { name: "确认更新凭证" })).toBeDisabled();
    expect(within(dialog).getByText("请输入非空且不含换行的凭证值。")).toBeInTheDocument();
    expect(api.updateCredential).not.toHaveBeenCalled();
  });
});

it.each([
  { resource: "memory", label: "Memory", backendId: "retired-pg" },
  { resource: "memory", label: "Memory", backendId: "retired-redis" },
  { resource: "session", label: "Session", backendId: "retired-redis-session" },
])("rotates fixed-revision $resource/$backendId password using token/CAS without directory lookup", async ({ resource, label, backendId }) => {
  const managedRevision = { ...revision, config: { ...config, storage: { [resource]: { kind: `managed_${resource}`, backend_id: backendId, backend_revision: 1 } } }, credential_states: { storage: { [resource]: { dsn_password: { configured: true, status: "active" as const, credential_revision: 2, association_token: token } } } } };
  api.getRevision.mockResolvedValue(managedRevision);
  const fetcher = vi.spyOn(globalThis, "fetch");
  render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={3} />);
  const user = userEvent.setup();
  await user.click(await screen.findByRole("button", { name: `更新 Storage / ${resource} / dsn_password` }));
  const dialog = await screen.findByRole("dialog");
  expect(within(dialog).getByLabelText(`新 ${label} 后端密码`)).toHaveValue("");
  expect(within(dialog).getByLabelText(`新 ${label} 后端密码`)).toHaveAttribute("type", "password");
  await user.type(within(dialog).getByLabelText(`新 ${label} 后端密码`), "test-raw-password");
  await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
  await waitFor(() => expect(api.updateCredential).toHaveBeenCalled());
  expect(api.updateCredential.mock.calls[0][2]).toEqual({ target: { profile_revision_number: 3, category: "storage", resource_name: resource, purpose_field: "dsn_password", association_token: token }, expected_credential_revision: 2, action: "replace", value: "test-raw-password" });
  await screen.findByText("凭证已更新，配置 Revision 与 digest 未改变。");
  expect(screen.getByTestId("resource-editor")).toHaveTextContent(JSON.stringify(managedRevision.config));
  expect(document.body.textContent).not.toContain("test-raw-password");
  expect(fetcher).not.toHaveBeenCalled();fetcher.mockRestore();
});

it("clears a published Redis Session password only with explicit confirmation and fixed association CAS", async () => {
  api.getRevision.mockResolvedValue({ ...revision, config: { ...config, storage: { session: { kind: "managed_session", backend_id: "retired-redis-session", backend_revision: 1 } } }, credential_states: { storage: { session: { dsn_password: { configured: true, status: "active", credential_revision: 2, association_token: token } } } } });
  const user = userEvent.setup();
  render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={3} />);
  await user.click(await screen.findByRole("button", { name: "更新 Storage / session / dsn_password" }));
  const dialog = await screen.findByRole("dialog");
  await user.type(within(dialog).getByLabelText("新 Session 后端密码"), "discarded-replacement");
  await user.click(within(dialog).getByRole("radio", { name: "清除" }));
  expect(within(dialog).queryByLabelText("新 Session 后端密码")).toBeNull();
  const submit = within(dialog).getByRole("button", { name: "确认清除凭证" });
  expect(submit).toBeDisabled();
  await user.click(within(dialog).getByRole("checkbox", { name: /我确认清除将使共享该关联/ }));
  await user.click(submit);
  await waitFor(() => expect(api.updateCredential).toHaveBeenCalledTimes(1));
  expect(api.updateCredential.mock.calls[0][2]).toEqual({ target: { profile_revision_number: 3, category: "storage", resource_name: "session", purpose_field: "dsn_password", association_token: token }, expected_credential_revision: 2, action: "clear" });
});

it.each([
  { purpose: "access_key_id", label: "新 S3 Access Key ID" },
  { purpose: "secret_access_key", label: "新 S3 Secret Access Key" },
])("rotates the published S3 $purpose with the original fixed association and no catalog lookup", async ({ purpose, label }) => {
  const artifactRevision = { ...revision, config: { ...config, storage: { artifact: { kind: "managed_artifact", backend_id: "retired-s3", backend_revision: 1 } } }, credential_states: { storage: { artifact: { [purpose]: { configured: true, status: "active", credential_revision: 2, association_token: token } } } } };
  api.getRevision.mockResolvedValue(artifactRevision);
  const fetcher = vi.spyOn(globalThis, "fetch");const user = userEvent.setup();
  render(<ProfileRevisionDetail tenantId="tenant-1" profileId="profile-1" revisionNumber={3} />);
  await user.click(await screen.findByRole("button", { name: `更新 Storage / artifact / ${purpose}` }));
  const dialog = await screen.findByRole("dialog");
  expect(within(dialog).getByLabelText(label)).toHaveValue("");
  expect(within(dialog).getByLabelText(label)).toHaveAttribute("type", "password");
  await user.type(within(dialog).getByLabelText(label), "replacement-test-value");
  await user.click(within(dialog).getByRole("button", { name: "确认更新凭证" }));
  await screen.findByText("凭证已更新，配置 Revision 与 digest 未改变。");
  expect(api.updateCredential.mock.calls[0][2]).toEqual({ target: { profile_revision_number: 3, category: "storage", resource_name: "artifact", purpose_field: purpose, association_token: token }, expected_credential_revision: 2, action: "replace", value: "replacement-test-value" });
  expect(screen.getByTestId("resource-editor")).toHaveTextContent(JSON.stringify(artifactRevision.config));
  expect(document.body.textContent).not.toContain("replacement-test-value");
  expect(fetcher).not.toHaveBeenCalled();fetcher.mockRestore();
});
