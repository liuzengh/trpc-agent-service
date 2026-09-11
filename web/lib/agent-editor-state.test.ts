import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { createSingleLLMAgentSpec, type AgentSpecV1 } from "./agent-spec-v1";
import {
  agentEditorReducer,
  createAgentEditorState,
  parseEditorPositions,
  serializeEditorPositions,
  validateAgentSpecLocally,
} from "./agent-editor-state";

const validFixture = (name: string): AgentSpecV1 =>
  JSON.parse(readFileSync(resolve(process.cwd(), `../api/schemas/agentspec/v1/examples/valid/${name}`), "utf8")) as AgentSpecV1;

describe("Agent editor state", () => {
  it.each(["single-llm.json", "sequence-parallel.json", "loop.json"])(
    "lays out and locally validates %s",
    (name) => {
      const spec = validFixture(name);
      const state = createAgentEditorState(spec);
      expect(Object.keys(state.positions).sort()).toEqual(Object.keys(spec.nodes).sort());
      expect(validateAgentSpecLocally(spec).filter((item) => item.severity === "error")).toEqual([]);
    },
  );

  it("keeps canvas coordinates outside AgentSpec", () => {
    const initial = createAgentEditorState(createSingleLLMAgentSpec());
    const moved = agentEditorReducer(initial, { type: "node.move", nodeID: "assistant", position: { x: 333, y: 222 } });
    expect(moved.positions.assistant).toEqual({ x: 333, y: 222 });
    expect(moved.spec).toBe(initial.spec);
    expect(JSON.stringify(moved.spec)).not.toContain("positions");
    expect(parseEditorPositions(serializeEditorPositions(moved.positions), ["assistant"]).assistant).toEqual({ x: 333, y: 222 });
  });

  it("adds all V1 node kinds and preserves a structurally editable tree", () => {
    let state = createAgentEditorState(createSingleLLMAgentSpec());
    state = agentEditorReducer(state, { type: "node.add", nodeID: "steps", kind: "sequence" });
    expect(state.spec.root).toBe("steps");
    expect(state.spec.nodes.steps).toEqual(expect.objectContaining({ kind: "sequence", children: ["assistant"] }));
    state = agentEditorReducer(state, { type: "node.add", nodeID: "review", kind: "llm", parentID: "steps" });
    expect(state.spec.nodes.steps).toEqual(expect.objectContaining({ children: ["assistant", "review"] }));
    state = agentEditorReducer(state, { type: "node.add", nodeID: "parallel", kind: "parallel" });
    state = agentEditorReducer(state, { type: "node.add", nodeID: "loop", kind: "loop" });
    expect(Object.values(state.spec.nodes).map((node) => node.kind)).toEqual(
      expect.arrayContaining(["llm", "sequence", "parallel", "loop"]),
    );
  });

  it("reports a node pointer for a missing reference and slot", () => {
    const spec = validFixture("single-llm.json");
    const assistant = spec.nodes.assistant;
    if (assistant.kind !== "llm") throw new Error("fixture changed");
    assistant.model_slot = "missing";
    spec.nodes.main = { kind: "sequence", children: ["assistant", "gone"] };
    spec.root = "main";
    expect(validateAgentSpecLocally(spec)).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "AGENT_SPEC_MODEL_SLOT_NOT_FOUND", node_id: "assistant", pointer: "/nodes/assistant/model_slot" }),
      expect.objectContaining({ code: "AGENT_SPEC_NODE_REFERENCE_NOT_FOUND", node_id: "main", pointer: "/nodes/main/children/1" }),
    ]));
  });

  it("adds nested containers explicitly without wrapping Root or leaving orphan nodes", () => {
    let state = createAgentEditorState(createSingleLLMAgentSpec());
    state = agentEditorReducer(state, { type: "node.add", nodeID: "flow", kind: "sequence", mode: "wrap-root" });
    state = agentEditorReducer(state, { type: "node.add", nodeID: "fanout", kind: "parallel", mode: "child", parentID: "flow" });
    expect(state.spec.root).toBe("flow");
    expect(state.spec.nodes.flow).toMatchObject({ children: ["assistant", "fanout"] });
    expect(state.spec.nodes.fanout).toMatchObject({ children: [] });
    const before = state;
    expect(agentEditorReducer(state, { type: "node.add", nodeID: "orphan", kind: "llm", mode: "child", parentID: "assistant" })).toBe(before);
    expect(agentEditorReducer(state, { type: "node.add", nodeID: "retry", kind: "loop", mode: "child", parentID: "flow" })).toBe(before);
  });

  it("wraps a selected nested node in place and preserves the parent's child order", () => {
    let state = createAgentEditorState(createSingleLLMAgentSpec());
    state = agentEditorReducer(state, { type: "node.add", nodeID: "flow", kind: "sequence", mode: "wrap-root" });
    state = agentEditorReducer(state, { type: "node.add", nodeID: "review", kind: "llm", mode: "child", parentID: "flow" });
    state = agentEditorReducer(state, { type: "node.add", nodeID: "retry", kind: "loop", mode: "wrap-selected", targetID: "assistant" });
    expect(state.spec.root).toBe("flow");
    expect(state.spec.nodes.flow).toMatchObject({ children: ["retry", "review"] });
    expect(state.spec.nodes.retry).toMatchObject({ body: "assistant" });
    expect(validateAgentSpecLocally(state.spec).filter((item) => item.severity === "error")).toEqual([]);
  });

  it("reparents nodes atomically while rejecting cycles, root moves and occupied loops", () => {
    let state = createAgentEditorState(createSingleLLMAgentSpec());
    state = agentEditorReducer(state, { type: "node.add", nodeID: "flow", kind: "sequence", mode: "wrap-root" });
    state = agentEditorReducer(state, { type: "node.add", nodeID: "fanout", kind: "parallel", mode: "child", parentID: "flow" });
    state = agentEditorReducer(state, { type: "node.reparent", nodeID: "assistant", parentID: "fanout" });
    expect(state.spec.nodes.flow).toMatchObject({ children: ["fanout"] });
    expect(state.spec.nodes.fanout).toMatchObject({ children: ["assistant"] });
    expect(agentEditorReducer(state, { type: "node.reparent", nodeID: "flow", parentID: "fanout" })).toBe(state);
    expect(agentEditorReducer(state, { type: "node.reparent", nodeID: "fanout", parentID: "assistant" })).toBe(state);
    state = agentEditorReducer(state, { type: "node.add", nodeID: "retry", kind: "loop", mode: "wrap-selected", targetID: "assistant" });
    expect(agentEditorReducer(state, { type: "node.reparent", nodeID: "fanout", parentID: "retry" })).toBe(state);
    state = agentEditorReducer(state, { type: "node.add", nodeID: "review", kind: "llm", mode: "child", parentID: "flow" });
    expect(agentEditorReducer(state, { type: "node.reparent", nodeID: "review", parentID: "retry" })).toBe(state);
    expect(validateAgentSpecLocally(state.spec).filter((item) => item.severity === "error")).toEqual([]);
  });

  it("reorders children and resets canvas layout without changing the Spec", () => {
    let state = createAgentEditorState(createSingleLLMAgentSpec());
    state = agentEditorReducer(state, { type: "node.add", nodeID: "flow", kind: "sequence", mode: "wrap-root" });
    state = agentEditorReducer(state, { type: "node.add", nodeID: "review", kind: "llm", mode: "child", parentID: "flow" });
    state = agentEditorReducer(state, { type: "child.move", parentID: "flow", from: 1, to: 0 });
    expect(state.spec.nodes.flow).toMatchObject({ children: ["review", "assistant"] });
    state = agentEditorReducer(state, { type: "node.move", nodeID: "assistant", position: { x: 999, y: 999 } });
    const laidOut = agentEditorReducer(state, { type: "layout.reset" });
    expect(laidOut.spec).toBe(state.spec);
    expect(laidOut.positions.assistant).not.toEqual({ x: 999, y: 999 });
    expect(laidOut.positions.review.x).toBeLessThan(laidOut.positions.assistant.x);
  });

  it("keeps wrapping inside a loop and moves its body without creating extra parents", () => {
    let state = createAgentEditorState(createSingleLLMAgentSpec());
    state = agentEditorReducer(state, { type: "node.add", nodeID: "flow", kind: "sequence", mode: "wrap-root" });
    state = agentEditorReducer(state, { type: "node.add", nodeID: "retry", kind: "loop", mode: "wrap-selected", targetID: "assistant" });
    state = agentEditorReducer(state, { type: "node.add", nodeID: "nested", kind: "sequence", mode: "wrap-selected", targetID: "assistant" });
    expect(state.spec.nodes.retry).toMatchObject({ body: "nested" });
    expect(state.spec.nodes.nested).toMatchObject({ children: ["assistant"] });
    state = agentEditorReducer(state, { type: "node.reparent", nodeID: "nested", parentID: "flow" });
    expect(state.spec.nodes.retry).toMatchObject({ body: "" });
    state = agentEditorReducer(state, { type: "node.reparent", nodeID: "assistant", parentID: "retry" });
    expect(state.spec.nodes.retry).toMatchObject({ body: "assistant" });
    expect(state.spec.nodes.nested).toMatchObject({ children: [] });
    expect(state.spec.nodes.flow).toMatchObject({ children: ["retry", "nested"] });
  });

  it("rejects invalid IDs, duplicate IDs, invalid wrapping and full parents without mutating state", () => {
    let state = createAgentEditorState(createSingleLLMAgentSpec());
    state = agentEditorReducer(state, { type: "node.add", nodeID: "flow", kind: "sequence", mode: "wrap-root" });
    for (const nodeID of ["Bad ID", "assistant"]) {
      expect(agentEditorReducer(state, { type: "node.add", nodeID, kind: "llm", mode: "child", parentID: "flow" })).toBe(state);
    }
    expect(agentEditorReducer(state, { type: "node.add", nodeID: "wrapper", kind: "llm", mode: "wrap-root" })).toBe(state);
    expect(agentEditorReducer(state, { type: "node.add", nodeID: "wrapper", kind: "sequence", mode: "wrap-selected", targetID: "missing" })).toBe(state);
    for (let index = 1; index < 64; index++) {
      state = agentEditorReducer(state, { type: "node.add", nodeID: `child_${index}`, kind: "llm", mode: "child", parentID: "flow" });
    }
    expect(agentEditorReducer(state, { type: "node.add", nodeID: "overflow", kind: "llm", mode: "child", parentID: "flow" })).toBe(state);
  });
});
