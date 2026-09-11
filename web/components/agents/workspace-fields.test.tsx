import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { createSingleLLMAgentSpec, type LLMNodeV1 } from "../../lib/agent-spec-v1";
import { WorkspaceFields } from "./workspace-fields";
afterEach(cleanup);
function Harness({ disabled = false, initial = createSingleLLMAgentSpec().nodes.assistant as LLMNodeV1 }: { disabled?: boolean; initial?: LLMNodeV1 }) {
  const [node, set] = useState(initial);
  return <><WorkspaceFields node={node} executors={{ sandbox: { capability: "workspace" } }} replace={set} disabled={disabled} /><output data-testid="node">{JSON.stringify(node)}</output></>;
}
const node = () => JSON.parse(screen.getByTestId("node").textContent!);
it("requires explicit slot and per-tool selection, removes workspace when last tool is cleared", () => {
  render(<Harness />);
  expect(node()).not.toHaveProperty("workspace");
  expect(screen.getByLabelText("workspace_exec")).toBeDisabled();
  fireEvent.change(screen.getByLabelText("Workspace Executor Slot"), { target: { value: "sandbox" } });
  expect(node()).not.toHaveProperty("workspace");
  fireEvent.click(screen.getByLabelText("workspace_exec"));
  expect(node().workspace).toEqual({ executor_slot: "sandbox", tools: ["workspace_exec"] });
  fireEvent.click(screen.getByLabelText("workspace_save_artifact"));
  expect(node().workspace.tools).toEqual(["workspace_exec", "workspace_save_artifact"]);
  expect(node()).not.toHaveProperty("artifact");expect(screen.getByRole("alert")).toHaveTextContent("不会自动开启");
  fireEvent.click(screen.getByLabelText("workspace_exec"));
  expect(node().workspace.tools).toEqual(["workspace_save_artifact"]);
  fireEvent.click(screen.getByLabelText("workspace_save_artifact"));
  expect(node()).not.toHaveProperty("workspace");
});
it("preserves stale executor selection until explicit closure", () => {
  const initial = createSingleLLMAgentSpec().nodes.assistant as LLMNodeV1;
  initial.workspace = { executor_slot: "removed", tools: ["workspace_exec"] };
  render(<Harness initial={initial} />);
  expect(screen.getByRole("option", { name: "removed · 未声明" })).toBeInTheDocument();
  expect(node().workspace.executor_slot).toBe("removed");
  fireEvent.click(screen.getByRole("button", { name: "关闭 Workspace" }));
  expect(node()).not.toHaveProperty("workspace");
});
it("locks every workspace control in readonly mode", () => {
  const initial = createSingleLLMAgentSpec().nodes.assistant as LLMNodeV1;
  initial.workspace = { executor_slot: "sandbox", tools: ["workspace_exec"] };
  render(<Harness initial={initial} disabled />);
  expect(screen.getByLabelText("Workspace Executor Slot")).toBeDisabled();
  expect(screen.getByLabelText("workspace_exec")).toBeDisabled();
  expect(screen.getByRole("button", { name: "关闭 Workspace" })).toBeDisabled();
});
