import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ProfileResourceEditor } from "./profile-resource-editor";
import type { CredentialActions, ProfileConfig } from "../../lib/runtime-profile-api";
import type { RuntimeBackend } from "../../lib/runtime-backend-api";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });
function Harness({ configured = true, readOnly = false, isOwner = true, disabled = false, backendId = "redis-session" }: { configured?: boolean; readOnly?: boolean; isOwner?: boolean; disabled?: boolean; backendId?: string }) {
  const [config, setConfig] = useState<ProfileConfig>({ models: {}, tools: {}, knowledge: {}, storage: { session: { kind: "managed_session", backend_id: backendId, backend_revision: 1 } } });
  const [credentials, setCredentials] = useState<CredentialActions>({});
  return <><ProfileResourceEditor tenantId="t" config={config} credentials={credentials} credentialStates={configured ? { storage: { session: { dsn_password: { configured: true, status: "active", credential_revision: 2 } } } } : {}} isOwner={isOwner} disabled={disabled} readOnly={readOnly} onChange={(next, actions) => { setConfig(next); setCredentials(actions); }} /><output data-testid="payload">{JSON.stringify({ config, credentials })}</output></>;
}
const backend = (overrides: Partial<RuntimeBackend> = {}): RuntimeBackend => ({ id: "redis-session", revision: 1, label: "Session", kind: "redis", roles: ["session"], available: true, ...overrides });
const catalog = (items: RuntimeBackend[]) => vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ items }));
const payload = () => JSON.parse(screen.getByTestId("payload").textContent!);

it("writes Redis Session password through the existing write-only keep/replace/clear actions, never config", async () => {
  catalog([backend()]);render(<Harness />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  const action = await screen.findByLabelText("Session 后端密码 操作");
  expect(payload().credentials).toEqual({});
  expect(screen.queryByLabelText("PostgreSQL DSN 操作")).toBeNull();
  fireEvent.change(action, { target: { value: "replace" } });
  const input = screen.getByLabelText("Session 后端密码 新值");
  expect(input).toHaveValue("");expect(input).toHaveAttribute("type", "password");
  fireEvent.change(input, { target: { value: "test-raw-session-password" } });
  expect(payload().config.storage.session).toEqual({ kind: "managed_session", backend_id: "redis-session", backend_revision: 1 });
  expect(JSON.stringify(payload().config)).not.toContain("test-raw-session-password");
  expect(payload().credentials).toEqual({ storage: { session: { dsn_password: { action: "replace", value: "test-raw-session-password" } } } });
  fireEvent.change(action, { target: { value: "clear" } });
  expect(payload().credentials.storage.session.dsn_password).toEqual({ action: "clear" });
  expect(screen.queryByLabelText("Session 后端密码 新值")).toBeNull();
  expect(screen.getByText(/不撤销已发布版本的凭证/)).toBeInTheDocument();
  fireEvent.change(action, { target: { value: "keep" } });
  expect(payload().credentials).toEqual({});
});

it("selects a fixed Redis Session revision without creating a password or physical destination", async () => {
  catalog([backend({ id: "pg-session", kind: "postgresql" }), backend({ revision: 3 })]);
  render(<Harness configured={false} backendId="pg-session" />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByRole("option", { name: /redis · r3/ });
  expect(screen.queryByLabelText("Session 后端密码 操作")).toBeNull();
  fireEvent.change(screen.getByLabelText("平台后端（session）"), { target: { value: "redis-session@3" } });
  expect(payload().config.storage.session).toEqual({ kind: "managed_session", backend_id: "redis-session", backend_revision: 3 });
  expect(payload().credentials).toEqual({});
  fireEvent.change(screen.getByLabelText("Session 后端密码 操作"), { target: { value: "replace" } });
  expect(screen.getByLabelText("Session 后端密码 新值")).toHaveValue("");
});

it.each([
  { name: "unavailable Redis", item: backend({ available: false }) },
  { name: "different revision", item: backend({ revision: 2 }) },
  { name: "different ID", item: backend({ id: "other-redis" }) },
  { name: "Memory role only", item: backend({ roles: ["memory"] }) },
  { name: "PostgreSQL Session", item: backend({ kind: "postgresql" }) },
  { name: "other backend kind", item: backend({ kind: "s3", roles: ["artifact"] }) },
])("preserves status but pauses password editing for $name", async ({ item }) => {
  const fetcher = catalog([item]);render(<Harness />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByRole("button", { name: "刷新后端目录" });
  await screen.findByText(/凭证 r2/);
  // Waiting for the directory request alone is insufficient; ready enables refresh.
  await vi.waitFor(() => expect(screen.getByRole("button", { name: "刷新后端目录" })).toBeEnabled());
  expect(fetcher).toHaveBeenCalled();
  expect(screen.getByRole("region", { name: "Session 后端密码" })).toHaveTextContent("凭证 r2");
  expect(screen.queryByLabelText("Session 后端密码 操作")).toBeNull();
  expect(screen.queryByLabelText("Session 后端密码 新值")).toBeNull();
  expect(payload().credentials).toEqual({});
  expect(payload().config.storage.session).toEqual({ kind: "managed_session", backend_id: "redis-session", backend_revision: 1 });
});

it("keeps PostgreSQL managed Session without any password field when it has no association", async () => {
  catalog([backend({ kind: "postgresql" })]);render(<Harness configured={false} />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByRole("option", { name: /postgresql/ });
  expect(screen.queryByRole("region", { name: "Session 后端密码" })).toBeNull();
  expect(screen.queryByLabelText("PostgreSQL DSN 操作")).toBeNull();
});

it("preserves status without a password editor when the catalog request fails", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("", { status: 503 }));
  render(<Harness />);fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByRole("alert");
  expect(screen.getByRole("region", { name: "Session 后端密码" })).toHaveTextContent("凭证 r2");
  expect(screen.queryByLabelText("Session 后端密码 操作")).toBeNull();
  expect(payload().credentials).toEqual({});
});

it("shows immutable Session password status without reading the current catalog", () => {
  const fetcher = vi.spyOn(globalThis, "fetch");
  render(<Harness readOnly backendId="retired-session" />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  expect(screen.getByRole("region", { name: "Session 后端密码" })).toHaveTextContent("凭证 r2");
  expect(screen.queryByLabelText("Session 后端密码 操作")).toBeNull();
  expect(fetcher).not.toHaveBeenCalled();
});

it("requires OWNER for Redis Session password actions", async () => {
  catalog([backend()]);render(<Harness isOwner={false} />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByRole("option", { name: /redis/ });
  expect(screen.getByText(/凭证变更需要 OWNER/)).toBeInTheDocument();
  expect(screen.queryByLabelText("Session 后端密码 操作")).toBeNull();
  expect(payload().credentials).toEqual({});
});

it("focuses a managed Session credential diagnostic on the public write-only action", async () => {
  catalog([backend()]);
  const config: ProfileConfig = { models: {}, tools: {}, knowledge: {}, storage: { session: { kind: "managed_session", backend_id: "redis-session", backend_revision: 1 } } };
  const props = { tenantId: "t", config, credentials: {}, credentialStates: {}, onChange: vi.fn(), isOwner: true };
  const view = render(<ProfileResourceEditor {...props} />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByLabelText("Session 后端密码 操作");
  view.rerender(<ProfileResourceEditor {...props} focusTarget={{ category: "storage", name: "session", pointer: "/storage/session/dsn_credential_id" }} />);
  expect(screen.getByLabelText("Session 后端密码 操作")).toHaveFocus();
  expect(props.onChange).not.toHaveBeenCalled();
});

it("preserves staged input on a backend change instead of silently discarding it; unsupported pairs remain server-validated", async () => {
  catalog([backend(), backend({ id: "pg-session", kind: "postgresql" })]);render(<Harness />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  fireEvent.change(await screen.findByLabelText("Session 后端密码 操作"), { target: { value: "replace" } });
  fireEvent.change(screen.getByLabelText("Session 后端密码 新值"), { target: { value: "pending-test-password" } });
  fireEvent.change(screen.getByLabelText("平台后端（session）"), { target: { value: "pg-session@1" } });
  expect(screen.queryByLabelText("Session 后端密码 操作")).toBeNull();
  expect(screen.queryByLabelText("Session 后端密码 新值")).toBeNull();
  expect(screen.getByText(/草稿密码编辑暂停/)).toBeInTheDocument();
  expect(payload().config.storage.session).toEqual({ kind: "managed_session", backend_id: "pg-session", backend_revision: 1 });
  expect(payload().credentials.storage.session.dsn_password).toEqual({ action: "replace", value: "pending-test-password" });
  fireEvent.change(screen.getByLabelText("平台后端（session）"), { target: { value: "redis-session@1" } });
  expect(screen.getByLabelText("Session 后端密码 新值")).toHaveValue("pending-test-password");
});
