import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { createSingleLLMAgentSpec, isRenderableAgentSpecV1, validateAgentSpecShape, type AgentSpecV1 } from "./agent-spec-v1";
import { agentEditorReducer, createAgentEditorState, validateAgentSpecLocally } from "./agent-editor-state";

const fixture = (name: string): AgentSpecV1 => JSON.parse(readFileSync(resolve(process.cwd(), `../api/schemas/agentspec/v1/examples/valid/${name}.json`), "utf8"));
function candidate(memory: unknown) {
  const spec = createSingleLLMAgentSpec();
  return { ...spec, nodes: { assistant: { ...spec.nodes.assistant, memory } } };
}
describe("P0a runtime data source contract", () => {
  it.each(["runtime-data-capabilities", "runtime-data-disabled"])("consumes real Control fixture %s without losing source fields", (name) => {
    const spec = fixture(name);
    expect(isRenderableAgentSpecV1(spec)).toBe(true);
    expect(validateAgentSpecLocally(spec).filter((d) => d.severity === "error")).toEqual([]);
    const state = agentEditorReducer(createAgentEditorState(spec), { type: "node.select", nodeID: "assistant" });
    expect(JSON.parse(JSON.stringify(state.spec))).toEqual(spec);
    if (spec.runtime?.summary?.enabled) expect(validateAgentSpecLocally(spec).some((d) => d.code === "AGENT_SPEC_UNUSED_MODEL_SLOT")).toBe(false);
  });
  it("matches Control's unsupported-tool diagnostic", () => {
    expect(validateAgentSpecShape(candidate({ tools: ["memory_unknown"] }))).toEqual(expect.arrayContaining([expect.objectContaining({ code: "AGENT_SPEC_MEMORY_TOOL_UNSUPPORTED", pointer: "/nodes/assistant/memory/tools/0" })]));
  });
  it.each([-1, 0, 1, Number.MAX_SAFE_INTEGER])("accepts preload %s", (preload_limit) => {
    expect(validateAgentSpecShape(candidate({ tools: [], preload_limit }))).toEqual([]);
  });
  it.each([-2, 0.5, Number.MAX_SAFE_INTEGER + 1, null, "1"])("rejects preload %s at exact pointer", (preload_limit) => {
    expect(validateAgentSpecShape(candidate({ tools: [], preload_limit }))).toEqual(expect.arrayContaining([expect.objectContaining({ pointer: "/nodes/assistant/memory/preload_limit", severity: "error" })]));
  });
  it.each([{}, null, { tools: ["memory_unknown"] }, { tools: ["memory_load", "memory_load"] }, { tools: [], subject: "spoof" }])("rejects invalid Memory %j", (memory) => {
    expect(validateAgentSpecShape(candidate(memory)).length).toBeGreaterThan(0);
  });
  it.each([1, Number.MAX_SAFE_INTEGER])("accepts summary threshold %s", (event_threshold) => {
    const spec = fixture("runtime-data-capabilities"); spec.runtime!.summary!.event_threshold = event_threshold;
    expect(validateAgentSpecShape(spec)).toEqual([]);
  });
  it.each([0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])("rejects summary threshold %s", (event_threshold) => {
    const spec = fixture("runtime-data-capabilities"); spec.runtime!.summary!.event_threshold = event_threshold;
    expect(validateAgentSpecShape(spec)).toEqual(expect.arrayContaining([expect.objectContaining({ pointer: "/runtime/summary/event_threshold" })]));
  });
  it("preserves empty runtime and keeps absent legacy fields absent", () => {
    const spec = createSingleLLMAgentSpec(); const before = JSON.stringify(spec);
    expect(JSON.stringify(createAgentEditorState(spec).spec)).toBe(before);
    expect(validateAgentSpecShape({ ...spec, runtime: {} })).toEqual([]);
    expect(isRenderableAgentSpecV1({ ...spec, runtime: {} })).toBe(true);
    expect(spec).not.toHaveProperty("runtime");
  });
  it("keeps incomplete Summary on canvas but diagnoses missing required fields", () => {
    const spec = { ...createSingleLLMAgentSpec(), runtime: { summary: { enabled: true } } };
    expect(isRenderableAgentSpecV1(spec)).toBe(true);
    expect(validateAgentSpecShape(spec).map((d) => d.pointer)).toEqual(expect.arrayContaining(["/runtime/summary/model_slot", "/runtime/summary/event_threshold"]));
  });
  it("rejects disabled Summary parameters without erasing source on read", () => {
    const spec = fixture("runtime-data-capabilities"); spec.runtime!.summary!.enabled = false;
    expect(validateAgentSpecShape(spec).map((d) => d.pointer)).toContain("/runtime/summary/model_slot");
    expect(createAgentEditorState(spec).spec.runtime).toEqual(spec.runtime);
  });
  it("requires declared chat summary model and counts that slot as used", () => {
    const spec = fixture("runtime-data-capabilities");
    spec.requirements.models.summary.capabilities = ["embedding"];
    expect(validateAgentSpecLocally(spec).map((d) => d.code)).toContain("AGENT_SPEC_SUMMARY_MODEL_CAPABILITY");
    delete spec.requirements.models.summary;
    expect(validateAgentSpecLocally(spec).map((d) => d.pointer)).toContain("/runtime/summary/model_slot");
  });
  it("does not silently clear node consumption when summary is disabled", () => {
    const initial = createAgentEditorState(fixture("runtime-data-capabilities"));
    const state = agentEditorReducer(initial, { type: "runtime.set", runtime: { summary: { enabled: false } } });
    expect(validateAgentSpecLocally(state.spec).map((d) => d.code)).toContain("AGENT_SPEC_SUMMARY_NOT_ENABLED");
    expect(initial.spec.runtime!.summary!.enabled).toBe(true);
    expect(state.spec.nodes.assistant).toHaveProperty("add_session_summary", true);
  });
  it.each(["memory", "artifact", "add_session_summary"])("rejects %s on structural node", (key) => {
    const spec = { ...createSingleLLMAgentSpec(), root: "root" };
    const value = { ...spec, nodes: { ...spec.nodes, root: { kind: "sequence", children: ["assistant"], [key]: key === "memory" ? { tools: [] } : key === "artifact" ? { enabled: false } : false } } };
    expect(isRenderableAgentSpecV1(value)).toBe(false);
    expect(validateAgentSpecShape(value).map((d) => d.code)).toContain("AGENT_SPEC_UNKNOWN_FIELD");
  });
  it.each([{ artifact: {} }, { artifact: null }, { artifact: { enabled: "true" } }, { add_session_summary: 1 }])("rejects invalid typed capabilities %j", (fields) => {
    const spec = createSingleLLMAgentSpec();
    expect(validateAgentSpecShape({ ...spec, nodes: { assistant: { ...spec.nodes.assistant, ...fields } } }).length).toBeGreaterThan(0);
  });
});

describe("Summary model own-property lookup", () => {
  it("diagnoses undeclared constructor instead of reading Object.prototype", () => {
    const spec = createSingleLLMAgentSpec();
    spec.runtime = { summary: { enabled: true, model_slot: "constructor", event_threshold: 1 } };
    expect(validateAgentSpecShape(spec)).toEqual([]);
    expect(() => validateAgentSpecLocally(spec)).not.toThrow();
    expect(validateAgentSpecLocally(spec)).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "AGENT_SPEC_MODEL_SLOT_NOT_FOUND", pointer: "/runtime/summary/model_slot" }),
    ]));
  });
  it("accepts an explicitly declared constructor slot with chat capability", () => {
    const spec = createSingleLLMAgentSpec();
    spec.requirements.models = { ...spec.requirements.models, constructor: { capabilities: ["chat"] } };
    spec.runtime = { summary: { enabled: true, model_slot: "constructor", event_threshold: 1 } };
    expect(validateAgentSpecShape(spec)).toEqual([]);
    expect(validateAgentSpecLocally(spec)).toEqual([]);
  });
  it.each(["toString", "valueOf", "hasOwnProperty"])("rejects inherited mixed-case name %s at the schema boundary without throwing", (model_slot) => {
    const spec = createSingleLLMAgentSpec();
    spec.runtime = { summary: { enabled: true, model_slot, event_threshold: 1 } };
    expect(() => validateAgentSpecLocally(spec)).not.toThrow();
    expect(validateAgentSpecLocally(spec)).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "AGENT_SPEC_INVALID_IDENTIFIER", pointer: "/runtime/summary/model_slot" }),
    ]));
  });
});
