import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useReducer } from "react";
import { afterEach, describe, expect, it } from "vitest";

import { agentEditorReducer, createAgentEditorState } from "../../lib/agent-editor-state";
import { createSingleLLMAgentSpec } from "../../lib/agent-spec-v1";
import { NodeStructureFields, NodeToolbar } from "./node-structure-editor";

afterEach(cleanup);

function Harness() {
  const [state, dispatch] = useReducer(agentEditorReducer, createSingleLLMAgentSpec(), createAgentEditorState);
  return <>
    <NodeToolbar disabled={false} dispatch={dispatch} state={state} />
    {state.selectedNodeID && <NodeStructureFields disabled={false} dispatch={dispatch} nodeID={state.selectedNodeID} state={state} />}
    <output data-testid="spec">{JSON.stringify(state.spec)}</output>
    {Object.keys(state.spec.nodes).map((id) => <button key={id} onClick={() => dispatch({ type: "node.select", nodeID: id })}>{`选中 ${id}`}</button>)}
  </>;
}

describe("Node structure editor", () => {
  it("makes wrapping explicit, keeps the chosen parent while adding siblings, and orders children", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.type(screen.getByLabelText("新节点 ID"), "flow");
    await user.selectOptions(screen.getByLabelText("节点操作"), "wrap-root");
    await user.selectOptions(screen.getByLabelText("新节点类型"), "sequence");
    await user.click(screen.getByRole("button", { name: "添加节点" }));
    await user.selectOptions(screen.getByLabelText("节点操作"), "child");
    await user.selectOptions(screen.getByLabelText("新节点父节点"), "flow");
    await user.type(screen.getByLabelText("新节点 ID"), "fanout");
    await user.selectOptions(screen.getByLabelText("新节点类型"), "parallel");
    await user.click(screen.getByRole("button", { name: "添加节点" }));
    expect(screen.getByLabelText("新节点父节点")).toHaveValue("flow");
    await user.type(screen.getByLabelText("新节点 ID"), "review");
    await user.selectOptions(screen.getByLabelText("新节点类型"), "llm");
    await user.click(screen.getByRole("button", { name: "添加节点" }));
    const spec = JSON.parse(screen.getByTestId("spec").textContent!);
    expect(spec.root).toBe("flow");
    expect(spec.nodes.flow.children).toEqual(["assistant", "fanout", "review"]);
    expect(spec.nodes.fanout.children).toEqual([]);
    await user.click(screen.getByRole("button", { name: "选中 flow" }));
    await user.click(screen.getByRole("button", { name: "上移 review" }));
    expect(JSON.parse(screen.getByTestId("spec").textContent!).nodes.flow.children).toEqual(["assistant", "review", "fanout"]);
  });

  it("reattaches an existing node using a selector instead of CSV and preserves an empty loop input", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.type(screen.getByLabelText("新节点 ID"), "flow");
    await user.selectOptions(screen.getByLabelText("节点操作"), "wrap-root");
    await user.click(screen.getByRole("button", { name: "添加节点" }));
    await user.selectOptions(screen.getByLabelText("节点操作"), "child");
    await user.type(screen.getByLabelText("新节点 ID"), "nested");
    await user.click(screen.getByRole("button", { name: "添加节点" }));
    await user.selectOptions(screen.getByLabelText("连接已有子节点"), "assistant");
    await user.click(screen.getByRole("button", { name: "连接子节点" }));
    expect(JSON.parse(screen.getByTestId("spec").textContent!).nodes.flow.children).toEqual(["nested"]);
    await user.selectOptions(screen.getByLabelText("节点操作"), "wrap-selected");
    await user.selectOptions(screen.getByLabelText("新节点类型"), "loop");
    await user.type(screen.getByLabelText("新节点 ID"), "retry");
    await user.click(screen.getByRole("button", { name: "添加节点" }));
    await user.clear(screen.getByLabelText("最大迭代次数"));
    expect(screen.getByLabelText("最大迭代次数")).toHaveDisplayValue("");
    await user.type(screen.getByLabelText("最大迭代次数"), "4");
    expect(JSON.parse(screen.getByTestId("spec").textContent!).nodes.retry.max_iterations).toBe(4);
  });
});
