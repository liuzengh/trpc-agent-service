import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ProfileResourceEditor } from "./profile-resource-editor";
import type { ProfileConfig, CredentialActions } from "../../lib/runtime-profile-api";
import type { RuntimeBackend } from "../../lib/runtime-backend-api";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });
function Harness({ configured = false, readOnly = false, isOwner = true, backendId = "pg" }: { configured?: boolean; readOnly?: boolean; isOwner?: boolean; backendId?: string }) {
  const [config, set] = useState<ProfileConfig>({ models: {}, tools: {}, knowledge: {}, storage: { memory: { kind: "managed_memory", backend_id: backendId, backend_revision: 1 } } });
  const [credentials, setCredentials] = useState<CredentialActions>({});
  return <><ProfileResourceEditor tenantId="t" config={config} credentials={credentials} credentialStates={configured ? { storage: { memory: { dsn_password: { configured: true, status: "active", credential_revision: 2 } } } } : {}} isOwner={isOwner} readOnly={readOnly} onChange={(next, actions) => { set(next); setCredentials(actions); }} /><output data-testid="payload">{JSON.stringify({ config, credentials })}</output></>;
}
const backend = (kind: "postgresql" | "redis", overrides: Partial<RuntimeBackend> = {}): RuntimeBackend => ({ id: kind === "redis" ? "redis-memory" : "pg", revision: 1, label: "Memory", kind, roles: ["memory"], available: true, ...overrides });
const catalog = (items: RuntimeBackend[]) => vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ items }));
const payload = () => JSON.parse(screen.getByTestId("payload").textContent!);

it.each(["postgresql", "redis"] as const)("writes a raw %s Memory password only in credential actions", async (kind) => {
  const selected = backend(kind);
  catalog([selected]);render(<Harness configured backendId={selected.id} />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  const action = await screen.findByLabelText("Memory 后端密码 操作");
  expect(payload().credentials).toEqual({});
  fireEvent.change(action, { target: { value: "replace" } });
  const input = screen.getByLabelText("Memory 后端密码 新值");
  expect(input).toHaveValue("");expect(input).toHaveAttribute("type", "password");
  expect(screen.getByText(/只填写原始密码，不是完整 DSN/)).toBeInTheDocument();
  fireEvent.change(input, { target: { value: "test-raw-password" } });
  expect(payload().config.storage.memory).toEqual({ kind: "managed_memory", backend_id: selected.id, backend_revision: 1 });
  expect(JSON.stringify(payload().config)).not.toContain("test-raw-password");
  expect(payload().credentials.storage.memory.dsn_password).toEqual({ action: "replace", value: "test-raw-password" });
  fireEvent.change(action, { target: { value: "clear" } });
  expect(payload().credentials.storage.memory.dsn_password).toEqual({ action: "clear" });
  expect(screen.queryByLabelText("Memory 后端密码 新值")).toBeNull();
  fireEvent.change(action, { target: { value: "keep" } });
  expect(payload().credentials).toEqual({});
});

it("selects an exact Redis backend revision without copying a destination or password into config", async () => {
  catalog([backend("postgresql"), backend("redis", { revision: 3 })]);
  render(<Harness configured />);fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByRole("option", { name: /redis · r3/ });
  fireEvent.change(screen.getByLabelText("平台后端（memory）"), { target: { value: "redis-memory@3" } });
  expect(payload().config.storage.memory).toEqual({ kind: "managed_memory", backend_id: "redis-memory", backend_revision: 3 });
  expect(payload().credentials).toEqual({});
  expect(screen.getByText(/更换 backend 或 revision 后，请选择替换凭证/)).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("Memory 后端密码 操作"), { target: { value: "replace" } });
  expect(screen.getByLabelText("Memory 后端密码 新值")).toHaveValue("");
});

it.each([
  { name: "unavailable backend", item: backend("redis", { available: false }) },
  { name: "changed revision", item: backend("redis", { revision: 2 }) },
  { name: "different runtime role", item: backend("redis", { roles: ["session"] }) },
])("preserves credential state without editing for $name", async ({ item }) => {
  catalog([item]);render(<Harness configured backendId="redis-memory" />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByText(/没有支持 memory 的可选后端|当前绑定不在可选目录中或修订已变化/);
  expect(screen.getByRole("region", { name: "Memory 后端密码" })).toHaveTextContent("凭证 r2");
  expect(screen.queryByLabelText("Memory 后端密码 操作")).toBeNull();
  expect(payload().credentials).toEqual({});
  expect(payload().config.storage.memory).toEqual({ kind: "managed_memory", backend_id: "redis-memory", backend_revision: 1 });
});

it("preserves configured state but pauses draft editing when directory is unavailable", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("", { status: 503 }));
  render(<Harness configured />);fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByRole("alert");expect(screen.getByRole("region", { name: "Memory 后端密码" })).toBeInTheDocument();
  expect(screen.queryByLabelText("Memory 后端密码 操作")).toBeNull();expect(payload().credentials).toEqual({});
});

it("shows immutable credential status without fetching current directory", () => {
  const fetcher = vi.spyOn(globalThis, "fetch");
  render(<Harness configured readOnly backendId="retired-memory" />);fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  expect(screen.getByRole("region", { name: "Memory 后端密码" })).toHaveTextContent("凭证 r2");
  expect(screen.queryByLabelText("Memory 后端密码 操作")).toBeNull();expect(fetcher).not.toHaveBeenCalled();
});

it("keeps Redis password actions unavailable to non-OWNER users", async () => {
  catalog([backend("redis")]);render(<Harness configured isOwner={false} backendId="redis-memory" />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByRole("option", { name: /redis/ });
  expect(screen.getByText(/凭证变更需要 OWNER/)).toBeInTheDocument();
  expect(screen.queryByLabelText("Memory 后端密码 操作")).toBeNull();
  expect(screen.queryByLabelText("Memory 后端密码 新值")).toBeNull();expect(payload().credentials).toEqual({});
});
