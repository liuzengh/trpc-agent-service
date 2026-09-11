import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { AgentSpecDocument, ValidationDiagnostic } from "../../lib/control-api";
import { ControlApiError } from "../../lib/control-api";
import { AgentWorkspace } from "./agent-workspace";

const api = vi.hoisted(() => ({
  getAgent: vi.fn(),
  getAgentDraft: vi.fn(),
  listAgentVersions: vi.fn(),
  saveAgentDraft: vi.fn(),
  validateAgentDraft: vi.fn(),
  publishAgentVersion: vi.fn(),
  updateAgent: vi.fn(),
}));

vi.mock("../../lib/control-api", async () => {
  const actual = await vi.importActual<typeof import("../../lib/control-api")>("../../lib/control-api");
  return { ...actual, controlApi: api };
});

vi.mock("./agent-spec-editor", () => ({
  AgentSpecEditor: ({
    value,
    onChange,
    diagnostics,
  }: {
    value: AgentSpecDocument;
    onChange(next: AgentSpecDocument): void;
    diagnostics?: readonly ValidationDiagnostic[];
  }) => (
    <div>
      <output data-testid="editor-value">{JSON.stringify(value)}</output>
      <output data-testid="editor-diagnostics">{diagnostics?.length ?? 0}</output>
      <button
        onClick={() => onChange({
          schema_version: "v1",
          root: "assistant",
          requirements: { models: { primary: { capabilities: ["chat"] } }, tools: {}, knowledge: {} },
          nodes: { assistant: { kind: "llm", instruction: "Local canvas change", model_slot: "primary", tool_slots: [], knowledge_slots: [] } },
        })}
        type="button"
      >
        编辑画布
      </button>
    </div>
  ),
}));

const agent = {
  id: "agent-1",
  tenant_id: "tenant-1",
  name: "Research Agent",
  description: "Researches topics",
  latest_version_number: null,
  created_by: "user-1",
  created_at: "2026-09-01T00:00:00Z",
  updated_at: "2026-09-01T00:00:00Z",
};

const initialDraft = {
  agent_id: "agent-1",
  tenant_id: "tenant-1",
  revision: 1,
  spec: {},
  updated_by: "user-1",
  updated_at: "2026-09-01T00:00:00Z",
};

const validReport = {
  valid: true,
  schema_version: "v1",
  draft_revision: 2,
  diagnostics: [],
};

beforeEach(() => {
  vi.clearAllMocks();
  api.getAgent.mockResolvedValue(agent);
  api.getAgentDraft.mockResolvedValue(initialDraft);
  api.listAgentVersions.mockResolvedValue({ versions: [], offset: 0, limit: 20, total: 0 });
  api.saveAgentDraft.mockImplementation(async (_tenantId, _agentId, input) => ({
    ...initialDraft,
    revision: input.expected_revision + 1,
    spec: input.spec,
  }));
  api.validateAgentDraft.mockResolvedValue(validReport);
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: { writeText: vi.fn().mockResolvedValue(undefined) },
  });
});

afterEach(() => cleanup());

describe("AgentWorkspace Draft state machine", () => {
  it("saves a canvas working copy with the loaded expected revision", async () => {
    const user = userEvent.setup();
    render(<AgentWorkspace agentId="agent-1" tenantId="tenant-1" />);

    await screen.findByText("Research Agent");
    await user.click(screen.getByRole("button", { name: "编辑画布" }));
    expect(screen.getByText("有未保存修改")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "保存 Draft" }));

    await waitFor(() => expect(api.saveAgentDraft).toHaveBeenCalledWith(
      "tenant-1",
      "agent-1",
      expect.objectContaining({ expected_revision: 1 }),
    ));
    expect(await screen.findByText("Draft revision 2 已保存到 Control API。")).toBeInTheDocument();
    expect(screen.getByText("与服务端 Draft 一致")).toBeInTheDocument();
  });

  it("saves first and validates the exact returned revision", async () => {
    const user = userEvent.setup();
    render(<AgentWorkspace agentId="agent-1" tenantId="tenant-1" />);

    await screen.findByText("Research Agent");
    await user.click(screen.getByRole("button", { name: "编辑画布" }));
    await user.click(screen.getByRole("button", { name: "保存并校验" }));

    await waitFor(() => expect(api.validateAgentDraft).toHaveBeenCalledWith(
      "tenant-1",
      "agent-1",
      { expected_revision: 2 },
    ));
    expect(api.saveAgentDraft.mock.invocationCallOrder[0]).toBeLessThan(
      api.validateAgentDraft.mock.invocationCallOrder[0],
    );
    expect(await screen.findByText("Draft revision 2 通过服务端发布校验。")).toBeInTheDocument();
  });

  it("preserves the local working copy and exposes the latest server Draft on a 409", async () => {
    const user = userEvent.setup();
    api.getAgentDraft
      .mockResolvedValueOnce(initialDraft)
      .mockResolvedValueOnce({ ...initialDraft, revision: 2, spec: { server: "newer" } });
    api.saveAgentDraft.mockRejectedValue(new ControlApiError(
      409,
      "AGENT_DRAFT_REVISION_CONFLICT",
      "draft revision is stale",
    ));
    render(<AgentWorkspace agentId="agent-1" tenantId="tenant-1" />);

    await screen.findByText("Research Agent");
    await user.click(screen.getByRole("button", { name: "编辑画布" }));
    await user.click(screen.getByRole("button", { name: "保存 Draft" }));

    expect(await screen.findByText("Draft revision 已发生冲突")).toBeInTheDocument();
    expect(screen.getByText(/服务端当前为 revision 2/)).toBeInTheDocument();
    expect(screen.getByTestId("editor-value")).toHaveTextContent("Local canvas change");
    expect(screen.getByRole("button", { name: "保存 Draft" })).toBeDisabled();
  });

  it("does not reload the Draft or erase dirty canvas state when version pagination changes", async () => {
    const user = userEvent.setup();
    api.listAgentVersions
      .mockResolvedValueOnce({ versions: [], offset: 0, limit: 20, total: 21 })
      .mockResolvedValueOnce({ versions: [], offset: 20, limit: 20, total: 21 });
    render(<AgentWorkspace agentId="agent-1" tenantId="tenant-1" />);

    await screen.findByText("Research Agent");
    await user.click(screen.getByRole("button", { name: "编辑画布" }));
    await user.click(await screen.findByRole("button", { name: "下一页" }));

    await waitFor(() => expect(api.listAgentVersions).toHaveBeenCalledTimes(2));
    expect(api.getAgentDraft).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("editor-value")).toHaveTextContent("Local canvas change");
    expect(screen.getByText("有未保存修改")).toBeInTheDocument();
  });

  it("passes 422 service diagnostics into the editor without changing the Draft", async () => {
    const user = userEvent.setup();
    const invalidReport = {
      valid: false,
      schema_version: "v1",
      draft_revision: 1,
      diagnostics: [{
        code: "AGENT_SPEC_ROOT_NOT_FOUND",
        severity: "error" as const,
        pointer: "/root",
        node_id: null,
        message: "root does not identify a node",
      }],
    };
    api.validateAgentDraft.mockRejectedValue(new ControlApiError(
      422,
      "AGENT_SPEC_INVALID",
      "AgentSpec validation failed",
      invalidReport,
    ));
    render(<AgentWorkspace agentId="agent-1" tenantId="tenant-1" />);

    await screen.findByText("Research Agent");
    await user.click(screen.getByRole("button", { name: "校验 Draft" }));

    expect(await screen.findByText("AGENT_SPEC_ROOT_NOT_FOUND")).toBeInTheDocument();
    expect(screen.getByTestId("editor-diagnostics")).toHaveTextContent("1");
    expect(api.saveAgentDraft).not.toHaveBeenCalled();
  });

  it("does not publish when the backend reports publication errors", async () => {
    const user = userEvent.setup();
    api.validateAgentDraft.mockResolvedValue({
      valid: false,
      schema_version: "v1",
      draft_revision: 1,
      diagnostics: [{ code: "AGENT_SPEC_REQUIRED", severity: "error", pointer: "/root", node_id: null, message: "root is required" }],
    });
    render(<AgentWorkspace agentId="agent-1" tenantId="tenant-1" />);

    await screen.findByText("Research Agent");
    await user.click(screen.getByRole("button", { name: "校验并发布" }));

    expect(await screen.findByText("Draft revision 1 未通过服务端校验，未执行发布。")).toBeInTheDocument();
    expect(api.publishAgentVersion).not.toHaveBeenCalled();
  });
});
