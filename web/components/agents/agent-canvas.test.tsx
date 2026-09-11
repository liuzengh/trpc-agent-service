import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { agentEditorReducer, createAgentEditorState } from "../../lib/agent-editor-state";
import { createSingleLLMAgentSpec } from "../../lib/agent-spec-v1";
import { AgentCanvas } from "./agent-canvas";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("AgentCanvas selection visibility", () => {
  it("scrolls newly added and diagnostic-selected nodes into view without scrolling while dragging", () => {
    const scroll = vi.fn();
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", { configurable: true, value: scroll });
    let state = createAgentEditorState(createSingleLLMAgentSpec());
    const onAction = vi.fn();
    const { rerender } = render(<AgentCanvas diagnostics={[]} onAction={onAction} state={state} />);
    scroll.mockClear();
    state = agentEditorReducer(state, { type: "node.add", nodeID: "flow", kind: "sequence", mode: "wrap-root" });
    rerender(<AgentCanvas diagnostics={[]} onAction={onAction} state={state} />);
    expect(scroll).toHaveBeenCalledWith({ block: "nearest", inline: "nearest" });
    scroll.mockClear();
    state = agentEditorReducer(state, { type: "node.select", nodeID: "assistant" });
    rerender(<AgentCanvas diagnostics={[]} onAction={onAction} state={state} />);
    expect(scroll).toHaveBeenCalledOnce();
    scroll.mockClear();
    const assistant = state.spec.nodes.assistant;
    if (assistant.kind !== "llm") throw new Error("fixture changed");
    state = agentEditorReducer(state, { type: "node.replace", nodeID: "assistant", node: { ...assistant, instruction: "editing" } });
    state = agentEditorReducer(state, { type: "spec.replace", spec: state.spec });
    rerender(<AgentCanvas diagnostics={[]} onAction={onAction} state={state} />);
    expect(scroll).not.toHaveBeenCalled();
    state = agentEditorReducer(state, { type: "node.select", nodeID: "assistant" });
    rerender(<AgentCanvas diagnostics={[]} onAction={onAction} state={state} />);
    expect(scroll).toHaveBeenCalledOnce();
    scroll.mockClear();
    state = agentEditorReducer(state, { type: "layout.reset" });
    rerender(<AgentCanvas diagnostics={[]} onAction={onAction} state={state} />);
    expect(scroll).toHaveBeenCalledOnce();
    fireEvent.pointerDown(screen.getByLabelText("节点 assistant"), { button: 0, clientX: 40, clientY: 40 });
    scroll.mockClear();
    state = agentEditorReducer(state, { type: "node.move", nodeID: "assistant", position: { x: 999, y: 999 } });
    rerender(<AgentCanvas diagnostics={[]} onAction={onAction} state={state} />);
    expect(scroll).not.toHaveBeenCalled();
  });
});
