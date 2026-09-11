import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import {
  createSingleLLMAgentSpec,
  isAgentSpecV1,
  isRenderableAgentSpecV1,
  validateAgentSpecShape,
  type AgentSpecV1,
} from "./agent-spec-v1";

const fixture = (group: "valid" | "invalid", name: string): unknown =>
  JSON.parse(readFileSync(resolve(process.cwd(), `../api/schemas/agentspec/v1/examples/${group}/${name}`), "utf8"));

describe("AgentSpec V1 contract", () => {
  it.each(["single-llm.json", "sequence-parallel.json", "loop.json"])(
    "accepts backend valid fixture %s without rewriting it",
    (name) => {
      const value = fixture("valid", name);
      expect(isAgentSpecV1(value)).toBe(true);
      expect(JSON.parse(JSON.stringify(value))).toEqual(value);
    },
  );

  it("creates a strict single-LLM document", () => {
    const spec = createSingleLLMAgentSpec();
    expect(isAgentSpecV1(spec)).toBe(true);
    expect(spec.root).toBe("assistant");
    expect(JSON.stringify(spec)).not.toMatch(/graph|chain|cycle|edges|editor_metadata|runtime_compatibility|model_ref/);
  });

  it("rejects backend fixtures containing editor state or a legacy model_ref", () => {
    expect(validateAgentSpecShape(fixture("invalid", "editor-state.json"))).toEqual(
      expect.arrayContaining([expect.objectContaining({ code: "AGENT_SPEC_UNKNOWN_FIELD", pointer: "/editor_state" })]),
    );
    expect(validateAgentSpecShape(fixture("invalid", "unknown-field.json"))).toEqual(
      expect.arrayContaining([expect.objectContaining({ code: "AGENT_SPEC_UNKNOWN_FIELD", pointer: "/nodes/assistant/model_ref" })]),
    );
  });

  it("uses the same duplicate codes as the Go schema validator", () => {
    const slots = createSingleLLMAgentSpec();
    slots.nodes.assistant = {
      ...(slots.nodes.assistant as Extract<AgentSpecV1["nodes"][string], { kind: "llm" }>),
      tool_slots: ["search", "search"],
    };
    slots.requirements.tools.search = { capability: "web.search" };
    expect(validateAgentSpecShape(slots)).toEqual(
      expect.arrayContaining([expect.objectContaining({ code: "AGENT_SPEC_LIMIT_EXCEEDED", pointer: "/nodes/assistant/tool_slots/1" })]),
    );

    const children = fixture("valid", "sequence-parallel.json") as AgentSpecV1;
    const main = children.nodes.main;
    if (main.kind !== "sequence") throw new Error("fixture changed");
    main.children = [main.children[0], main.children[0]];
    expect(validateAgentSpecShape(children)).toEqual(
      expect.arrayContaining([expect.objectContaining({ code: "AGENT_SPEC_DUPLICATE_CHILD", pointer: "/nodes/main/children/1" })]),
    );
  });
});

describe("AgentSpec V1 renderable editing state", () => {
  it.each(["single-llm.json", "sequence-parallel.json", "loop.json"])(
    "renders backend valid fixture %s without rewriting it",
    (name) => {
      const value = fixture("valid", name);
      const before = JSON.stringify(value);
      expect(isRenderableAgentSpecV1(value)).toBe(true);
      expect(JSON.stringify(value)).toBe(before);
    },
  );

  const editingStates: [string, (spec: AgentSpecV1) => void][] = [
    ["empty instruction", (spec) => {
      const node = spec.nodes.assistant;
      if (node.kind === "llm") node.instruction = "";
    }],
    ["empty model slot", (spec) => {
      const node = spec.nodes.assistant;
      if (node.kind === "llm") node.model_slot = "";
    }],
    ["empty model capabilities", (spec) => { spec.requirements.models.primary.capabilities = []; }],
    ["empty tool capability", (spec) => { spec.requirements.tools.search = { capability: "" }; }],
    ["empty knowledge capability", (spec) => { spec.requirements.knowledge.docs = { capability: "" }; }],
    ["empty sequence children", (spec) => { spec.nodes.main = { kind: "sequence", children: [] }; }],
    ["empty parallel children", (spec) => { spec.nodes.main = { kind: "parallel", children: [] }; }],
    ["empty loop body", (spec) => { spec.nodes.main = { kind: "loop", body: "", max_iterations: 3 }; }],
    ["zero loop iterations", (spec) => { spec.nodes.main = { kind: "loop", body: "assistant", max_iterations: 0 }; }],
    ["out-of-range and fractional loop iterations", (spec) => {
      spec.nodes.main = { kind: "loop", body: "assistant", max_iterations: 33.5 };
    }],
    ["out-of-range generation options", (spec) => {
      const node = spec.nodes.assistant;
      if (node.kind === "llm") node.generation = { temperature: 4, max_output_tokens: 0.5 };
    }],
    ["temporarily invalid identifiers", (spec) => {
      spec.root = "Not a valid ID";
      spec.nodes["New Node"] = { kind: "parallel", children: ["missing node"] };
      spec.requirements.models["New Slot"] = { capabilities: ["Bad Capability"] };
    }],
    ["empty root and nodes", (spec) => { spec.root = ""; spec.nodes = {}; }],
  ];

  it.each(editingStates)("keeps %s on the canvas without relaxing publish validation", (_name, edit) => {
    const spec = createSingleLLMAgentSpec();
    edit(spec);
    const before = JSON.stringify(spec);
    expect(isRenderableAgentSpecV1(spec)).toBe(true);
    expect(isAgentSpecV1(spec)).toBe(false);
    expect(validateAgentSpecShape(spec).length).toBeGreaterThan(0);
    expect(JSON.stringify(spec)).toBe(before);
  });

  it.each([
    ["root", (value: Record<string, unknown>) => { delete value.root; }],
    ["requirements", (value: Record<string, unknown>) => { delete value.requirements; }],
    ["nodes", (value: Record<string, unknown>) => { delete value.nodes; }],
    ["schema version", (value: Record<string, unknown>) => { value.schema_version = "v2"; }],
    ["root type", (value: Record<string, unknown>) => { value.root = 0; }],
    ["node map type", (value: Record<string, unknown>) => { value.nodes = []; }],
    ["missing requirement group", (value: Record<string, unknown>) => {
      value.requirements = { models: {}, tools: {} };
    }],
    ["model capability array type", (value: Record<string, unknown>) => {
      value.requirements = { models: { primary: { capabilities: "chat" } }, tools: {}, knowledge: {} };
    }],
    ["tool capability type", (value: Record<string, unknown>) => {
      value.requirements = { models: {}, tools: { search: { capability: null } }, knowledge: {} };
    }],
    ["missing node kind", (value: Record<string, unknown>) => { value.nodes = { main: { children: [] } }; }],
    ["unknown node kind", (value: Record<string, unknown>) => { value.nodes = { main: { kind: "router", children: [] } }; }],
    ["children array type", (value: Record<string, unknown>) => { value.nodes = { main: { kind: "sequence", children: "assistant" } }; }],
    ["children element type", (value: Record<string, unknown>) => { value.nodes = { main: { kind: "parallel", children: [1] } }; }],
    ["loop iteration type", (value: Record<string, unknown>) => { value.nodes = { main: { kind: "loop", body: "", max_iterations: "3" } }; }],
    ["non-finite loop iterations", (value: Record<string, unknown>) => { value.nodes = { main: { kind: "loop", body: "", max_iterations: Infinity } }; }],
  ])("leaves incomplete or mistyped %s in JSON mode", (_name, edit) => {
    const value: Record<string, unknown> = { ...createSingleLLMAgentSpec() };
    edit(value);
    expect(isRenderableAgentSpecV1(value)).toBe(false);
  });

  it("rejects incomplete LLM fields, mistyped references, and non-finite generation values", () => {
    for (const patch of [
      { instruction: null }, { model_slot: 1 }, { tool_slots: null }, { knowledge_slots: [false] },
      { generation: { temperature: NaN } }, { generation: { max_output_tokens: Infinity } },
    ]) {
      const spec = createSingleLLMAgentSpec();
      const value = { ...spec, nodes: { assistant: { ...spec.nodes.assistant, ...patch } } };
      expect(isRenderableAgentSpecV1(value)).toBe(false);
    }
    const spec = createSingleLLMAgentSpec();
    const node: Record<string, unknown> = { ...spec.nodes.assistant };
    delete node.instruction;
    expect(isRenderableAgentSpecV1({ ...spec, nodes: { assistant: node } })).toBe(false);
  });

  it("retains unknown fields in JSON instead of loading a lossy visual projection", () => {
    const spec = createSingleLLMAgentSpec();
    const node = spec.nodes.assistant;
    const values = [
      { ...spec, editor_state: {} },
      { ...spec, requirements: { ...spec.requirements, secrets: {} } },
      { ...spec, requirements: { ...spec.requirements, models: { primary: { capabilities: [], model_ref: "concrete" } } } },
      { ...spec, nodes: { assistant: { ...node, model_ref: "concrete" } } },
      { ...spec, nodes: { assistant: { ...node, generation: { seed: 1 } } } },
    ];
    for (const value of values) expect(isRenderableAgentSpecV1(value)).toBe(false);
  });

  it("preserves schema-compatible constructor and prototype slot names", () => {
    for (const key of ["constructor", "prototype"]) {
      const spec = createSingleLLMAgentSpec();
      spec.requirements.models[key] = { capabilities: ["chat"] };
      spec.requirements.tools[key] = { capability: "web.search" };
      spec.requirements.knowledge[key] = { capability: "knowledge.query" };
      const node = spec.nodes.assistant;
      if (node.kind !== "llm") throw new Error("template changed");
      node.model_slot = key;
      node.tool_slots = [key];
      node.knowledge_slots = [key];
      expect(isAgentSpecV1(spec)).toBe(true);
      expect(isRenderableAgentSpecV1(spec)).toBe(true);
    }
  });

  it("rejects inherited document structures instead of accepting missing own fields", () => {
    expect(isRenderableAgentSpecV1(Object.create(createSingleLLMAgentSpec()))).toBe(false);
  });
});
