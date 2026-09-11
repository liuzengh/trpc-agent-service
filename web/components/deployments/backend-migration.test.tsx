import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { revision, deployment, validReport, agentVersion, profileRevision } from "../../test/deployment-fixtures";
import { BackendMigration } from "./backend-migration";

const mocks = vi.hoisted(() => ({
  deployment: { getRevision: vi.fn(), get: vi.fn(), migrateAndPublish: vi.fn() },
  channel: { getBinding: vi.fn(), setBindingTarget: vi.fn() },
  control: { listAgents: vi.fn(), listAgentVersions: vi.fn(), getAgent: vi.fn() },
  profile: { listProfiles: vi.fn(), listRevisions: vi.fn(), getProfile: vi.fn() },
}));
vi.mock("../../lib/deployment-api", async (original) => ({ ...await original<typeof import("../../lib/deployment-api")>(), deploymentApi: mocks.deployment }));
vi.mock("../../lib/channel-api", async (original) => ({ ...await original<typeof import("../../lib/channel-api")>(), channelApi: mocks.channel }));
vi.mock("../../lib/control-api", () => ({ controlApi: mocks.control }));
vi.mock("../../lib/runtime-profile-api", () => ({ runtimeProfileApi: mocks.profile }));

const binding = { tenant_id: "t", binding_id: "b", account_id: "a", binding_revision: 4, enabled: true, target: { tenant_id: "t", deployment_id: "d", revision_number: 1, deployment_revision_id: "dpr-1", manifest_ref: "rmf-1", manifest_digest: "sha256:x" }, created_by: "u", created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z" };

beforeEach(() => {
  vi.resetAllMocks();
  Object.defineProperty(globalThis, "crypto", { configurable: true, value: { randomUUID: vi.fn().mockReturnValueOnce("migration-key").mockReturnValueOnce("switch-key").mockReturnValue("retry-key") } });
  mocks.deployment.getRevision.mockResolvedValue(revision);
  mocks.deployment.get.mockResolvedValue({ ...deployment, latest_revision_number: 1 });
  mocks.deployment.migrateAndPublish.mockResolvedValue({ memory_scopes_copied: 3, publication: { revision: { ...revision, revision_number: 2 }, validation: validReport } });
  mocks.channel.getBinding.mockResolvedValue({ binding, route_generation: 1, distribution: "PUBLISHED", gateway_application: "UNKNOWN" });
  mocks.channel.setBindingTarget.mockResolvedValue({ binding: { ...binding, binding_revision: 5, target: { ...binding.target, revision_number: 2 } }, route_generation: 2, distribution: "PENDING" });
  mocks.control.listAgents.mockResolvedValue({ agents: [{ id: revision.agent_id, name: "Agent", latest_version_number: 3 }], total: 1 });
  mocks.control.listAgentVersions.mockResolvedValue({ versions: [agentVersion], total: 1 });
  mocks.control.getAgent.mockResolvedValue({ name: "Agent" });
  mocks.profile.listProfiles.mockResolvedValue({ runtime_profiles: [{ id: revision.profile_id, name: "Profile", latest_revision_number: 2 }], total: 1 });
  mocks.profile.listRevisions.mockResolvedValue({ revisions: [profileRevision], total: 1 });
  mocks.profile.getProfile.mockResolvedValue({ name: "Profile" });
});
afterEach(cleanup);

describe("Deployment backend migration", () => {
  it("migrates, publishes, then switches the binding with CAS", async () => {
    render(<BackendMigration tenantId="t" deploymentId="d" sourceRevision={1} initialBindingId="b" />);
    fireEvent.click(await screen.findByRole("button", { name: "迁移、发布并切换" }));
    await screen.findByText(/迁移完成：复制 3 个 Memory Scope/);
    expect(mocks.deployment.migrateAndPublish).toHaveBeenCalledWith("t", "d", expect.objectContaining({ source_revision_number: 1, expected_latest_revision_number: 1 }), "migration-key");
    expect(mocks.channel.setBindingTarget).toHaveBeenCalledWith("t", "b", { expected_binding_revision: 4, target: { deployment_id: "d", revision_number: 2 } }, "switch-key");
  });

  it("retries only the binding switch after publication succeeds", async () => {
    mocks.channel.setBindingTarget.mockRejectedValueOnce(new Error("switch failed"));
    render(<BackendMigration tenantId="t" deploymentId="d" sourceRevision={1} initialBindingId="b" />);
    fireEvent.click(await screen.findByRole("button", { name: "迁移、发布并切换" }));
    fireEvent.click(await screen.findByRole("button", { name: "重试切换渠道" }));
    await waitFor(() => expect(mocks.channel.setBindingTarget).toHaveBeenCalledTimes(2));
    expect(mocks.deployment.migrateAndPublish).toHaveBeenCalledTimes(1);
  });
});
