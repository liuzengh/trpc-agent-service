import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ProfileResourceEditor } from "./profile-resource-editor";
import type { CredentialActions, ProfileConfig } from "../../lib/runtime-profile-api";
import type { RuntimeBackend } from "../../lib/runtime-backend-api";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });
const purposes = ["access_key_id", "secret_access_key"] as const;
function Harness({ configured = false, isOwner = true, readOnly = false }: { configured?: boolean; isOwner?: boolean; readOnly?: boolean }) {
  const [config, setConfig] = useState<ProfileConfig>({ models: {}, tools: {}, knowledge: {}, storage: { artifact: { kind: "managed_artifact", backend_id: "s3-artifact", backend_revision: 1 } } });
  const [credentials, setCredentials] = useState<CredentialActions>({});
  return <><ProfileResourceEditor tenantId="t" config={config} credentials={credentials} credentialStates={configured ? { storage: { artifact: Object.fromEntries(purposes.map((purpose) => [purpose, { configured: true, status: "active", credential_revision: 2 }])) } } : {}} isOwner={isOwner} readOnly={readOnly} onChange={(next, actions) => { setConfig(next); setCredentials(actions); }} /><output data-testid="payload">{JSON.stringify({ config, credentials })}</output></>;
}
const backend = (overrides: Partial<RuntimeBackend> = {}): RuntimeBackend => ({ id: "s3-artifact", revision: 1, label: "Artifacts", kind: "s3", roles: ["artifact"], available: true, ...overrides });
const catalog = (items: RuntimeBackend[]) => vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ items }));
const payload = () => JSON.parse(screen.getByTestId("payload").textContent!);

it("uses two independent S3 write-only actions and leaves only the fixed backend in Profile config", async () => {
  catalog([backend()]);render(<Harness configured />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  const accessAction = await screen.findByLabelText("S3 Access Key ID 操作");
  const secretAction = screen.getByLabelText("S3 Secret Access Key 操作");
  expect(payload().credentials).toEqual({});
  fireEvent.change(accessAction, { target: { value: "replace" } });
  fireEvent.change(secretAction, { target: { value: "replace" } });
  for (const label of ["S3 Access Key ID 新值", "S3 Secret Access Key 新值"]) {
    expect(screen.getByLabelText(label)).toHaveValue("");
    expect(screen.getByLabelText(label)).toHaveAttribute("type", "password");
  }
  fireEvent.change(screen.getByLabelText("S3 Access Key ID 新值"), { target: { value: "test-access-key" } });
  fireEvent.change(screen.getByLabelText("S3 Secret Access Key 新值"), { target: { value: "test-secret-key" } });
  expect(payload().config.storage.artifact).toEqual({ kind: "managed_artifact", backend_id: "s3-artifact", backend_revision: 1 });
  expect(payload().credentials).toEqual({ storage: { artifact: { access_key_id: { action: "replace", value: "test-access-key" }, secret_access_key: { action: "replace", value: "test-secret-key" } } } });
  fireEvent.change(accessAction, { target: { value: "keep" } });
  expect(payload().credentials.storage.artifact).toEqual({ secret_access_key: { action: "replace", value: "test-secret-key" } });
  fireEvent.change(secretAction, { target: { value: "clear" } });
  expect(payload().credentials.storage.artifact).toEqual({ secret_access_key: { action: "clear" } });
  expect(screen.queryByLabelText("S3 Secret Access Key 新值")).toBeNull();
  expect(screen.queryByLabelText("PostgreSQL DSN 操作")).toBeNull();
});

it.each([
  { name: "different revision", item: backend({ revision: 2 }) },
  { name: "unavailable", item: backend({ available: false }) },
  { name: "wrong kind and role", item: backend({ kind: "redis", roles: ["session"] }) },
])("preserves both statuses without editing when directory is $name", async ({ item }) => {
  catalog([item]);render(<Harness configured />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await vi.waitFor(() => expect(screen.getByRole("button", { name: "刷新后端目录" })).toBeEnabled());
  for (const label of ["S3 Access Key ID", "S3 Secret Access Key"]) {
    expect(screen.getByRole("region", { name: label })).toHaveTextContent("凭证 r2");
    expect(screen.queryByLabelText(`${label} 操作`)).toBeNull();
  }
  expect(payload().credentials).toEqual({});
});

it("displays immutable S3 credential state without consulting a current catalog", () => {
  const fetcher = vi.spyOn(globalThis, "fetch");render(<Harness configured readOnly />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  expect(screen.getByRole("region", { name: "S3 Access Key ID" })).toHaveTextContent("凭证 r2");
  expect(screen.getByRole("region", { name: "S3 Secret Access Key" })).toHaveTextContent("凭证 r2");
  expect(screen.queryByLabelText("S3 Secret Access Key 操作")).toBeNull();
  expect(fetcher).not.toHaveBeenCalled();
});

it("keeps both S3 credential actions OWNER-only", async () => {
  catalog([backend()]);render(<Harness configured isOwner={false} />);
  fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  await screen.findByRole("option", { name: /s3/ });
  expect(screen.queryByLabelText("S3 Access Key ID 操作")).toBeNull();
  expect(screen.queryByLabelText("S3 Secret Access Key 操作")).toBeNull();
  expect(payload().credentials).toEqual({});
});
