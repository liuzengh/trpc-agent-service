import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ProfileResourceEditor } from "./profile-resource-editor";
import type { ProfileConfig, CredentialActions } from "../../lib/runtime-profile-api";
afterEach(() => { cleanup(); vi.restoreAllMocks(); });
function Editor() {
  const [config, set] = useState<ProfileConfig>({ models: {}, tools: {}, knowledge: {}, storage: {} });
  const [credentials, setCredentials] = useState<CredentialActions>({});
  return <><ProfileResourceEditor tenantId="t" config={config} credentials={credentials} credentialStates={{}} isOwner onChange={(next, actions) => { set(next); setCredentials(actions); }} /><output data-testid="config">{JSON.stringify(config)}</output></>;
}
it("adds managed artifact only with a catalog-backed binding and no DSN fields", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ items: [{ id: "objects", revision: 1, label: "Artifacts", kind: "s3", roles: ["artifact"], available: true }] }));
  render(<Editor />);fireEvent.click(screen.getByRole("button", { name: /^Storage ·/ }));
  fireEvent.click(screen.getByLabelText("新增平台托管资源（managed）"));fireEvent.change(screen.getByLabelText("新资源名称"), { target: { value: "artifact" } });
  fireEvent.click(screen.getByRole("button", { name: "添加资源" }));
  await screen.findByRole("option", { name: /Artifacts/ });
  fireEvent.change(screen.getByLabelText("平台后端（artifact）"), { target: { value: "objects@1" } });
  expect(JSON.parse(screen.getByTestId("config").textContent!).storage.artifact).toEqual({ kind: "managed_artifact", backend_id: "objects", backend_revision: 1 });
  expect(screen.queryByLabelText("数据库主机")).toBeNull();expect(screen.queryByText("PostgreSQL DSN")).toBeNull();
  expect(screen.queryByRole("button", { name: "使用 postgres_state" })).toBeNull();
});
it("managed knowledge retains only embedding credential controls and owner guards", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ items: [] }));
  render(<ProfileResourceEditor tenantId="t" config={{ models: {}, tools: {}, storage: {}, knowledge: { docs: { kind: "managed_knowledge", backend_id: "vectors", backend_revision: 1, embedding: { model: "embed", base_url: "https://embed.test", dimensions: 3 } } } }} credentials={{}} credentialStates={{ knowledge: { docs: { embedding_api_key: { configured: true, status: "active", credential_revision: 1 } } } }} isOwner={false} onChange={vi.fn()} />);
  fireEvent.click(screen.getByRole("button", { name: /^Knowledge ·/ }));
  await screen.findByText(/没有支持 knowledge 的可选后端/);
  expect(screen.queryByLabelText("Qdrant 主机")).toBeNull();expect(screen.queryByText("Qdrant API Key")).toBeNull();
  expect(screen.getByLabelText("Embedding Base URL")).toBeDisabled();expect(screen.getByText("Embedding API Key")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "删除资源 docs" })).toBeDisabled();
});
