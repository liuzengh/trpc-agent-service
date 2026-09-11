import { describe, expect, it } from "vitest";
import { createSingleLLMAgentSpec, isRenderableAgentSpecV1, validateAgentSpecShape, type LLMNodeV1 } from "./agent-spec-v1";
import { agentEditorReducer, createAgentEditorState, validateAgentSpecLocally } from "./agent-editor-state";
function configured() {
  const spec = createSingleLLMAgentSpec();
  spec.requirements.models.primary.capabilities.push("tool_call");
  spec.requirements.executors = { sandbox: { capability: "workspace" } };
  const node = spec.nodes.assistant as LLMNodeV1;
  node.workspace = { executor_slot: "sandbox", tools: ["workspace_exec", "workspace_save_artifact"] };
  node.artifact = { enabled: true };
  return spec;
}
describe("Workspace contract", () => {
  it("preserves optional omission and valid declarations through visual editing", () => {
    expect(validateAgentSpecShape(createSingleLLMAgentSpec())).toEqual([]);
    const spec = configured(); expect(isRenderableAgentSpecV1(spec)).toBe(true);
    expect(validateAgentSpecLocally(spec).filter((d) => d.severity === "error")).toEqual([]);
    const state = agentEditorReducer(createAgentEditorState(spec), { type: "requirement.capability.set", kind: "executors", slot: "other", capability: "workspace" });
    expect(state.spec.nodes.assistant).toEqual(spec.nodes.assistant);
    expect(state.spec.requirements.executors?.other).toEqual({ capability: "workspace" });
  });
  it.each([[], ["workspace_exec", "workspace_exec"], ["shell"], ["workspace_exec", "workspace_save_artifact", "workspace_exec"]].map((tools) => ({ tools })))("rejects invalid tool selection $tools", ({ tools }) => {
    const spec = configured(); const node = spec.nodes.assistant as LLMNodeV1;
    const value = { ...spec, nodes: { assistant: { ...node, workspace: { executor_slot: "sandbox", tools } } } };
    expect(validateAgentSpecShape(value).some((d) => d.pointer.startsWith("/nodes/assistant/workspace/tools"))).toBe(true);
  });
  it("keeps incomplete known fields editable but leaves unknown workspace options in JSON", () => {
    const spec = configured(); const node = spec.nodes.assistant as LLMNodeV1;
    node.workspace = { executor_slot: "", tools: [] };
    expect(isRenderableAgentSpecV1(spec)).toBe(true);
    expect(validateAgentSpecShape(spec).length).toBeGreaterThan(0);
    expect(isRenderableAgentSpecV1({ ...spec, nodes: { assistant: { ...node, workspace: { ...node.workspace, host_path: "/host" } } } })).toBe(false);
  });
  it("diagnoses removed executors, Artifact dependency without changing the source", () => {
    const spec = configured(); delete spec.requirements.executors;
    delete (spec.nodes.assistant as LLMNodeV1).artifact;
    spec.requirements.models.primary.capabilities = ["chat"];
    const before = JSON.stringify(spec);
    const pointers = validateAgentSpecLocally(spec).map((d) => d.pointer);
    expect(pointers).toContain("/nodes/assistant/workspace/executor_slot");
    expect(pointers).toContain("/nodes/assistant/workspace/tools");
    expect(pointers).not.toContain("/nodes/assistant/model_slot"); // Actual model capability is checked at Deployment publication.
    expect(JSON.stringify(spec)).toBe(before);
  });
  it("rejects wrong executor capability, excess map size and workspace on non-LLM", () => {
    const spec = configured(); spec.requirements.executors = { sandbox: { capability: "shell" } };
    expect(validateAgentSpecShape(spec).some((d) => d.pointer.endsWith("/capability"))).toBe(true);
    spec.requirements.executors = Object.fromEntries(Array.from({ length: 17 }, (_, i) => [`slot${i}`, { capability: "workspace" }]));
    expect(validateAgentSpecShape(spec).some((d) => d.pointer === "/requirements/executors")).toBe(true);
    expect(validateAgentSpecShape({ ...configured(), nodes: { assistant: { kind: "sequence", children: [], workspace: { executor_slot: "sandbox", tools: ["workspace_exec"] } } } }).some((d) => d.pointer === "/nodes/assistant/workspace")).toBe(true);
  });
});
