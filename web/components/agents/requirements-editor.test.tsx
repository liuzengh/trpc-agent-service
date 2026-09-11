import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useReducer } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createSingleLLMAgentSpec, type AgentSpecV1 } from "../../lib/agent-spec-v1";
import { agentEditorReducer, createAgentEditorState, type RequirementKind } from "../../lib/agent-editor-state";
import { RequirementsEditor } from "./requirements-editor";

afterEach(cleanup);

function setup(spec = createSingleLLMAgentSpec(), disabled = false) {
  const dispatch = vi.fn();
  render(<RequirementsEditor state={createAgentEditorState(spec)} dispatch={dispatch} disabled={disabled} />);
  return dispatch;
}

function enterSlot(group: string, slot: string, capability: string) {
  fireEvent.change(screen.getByLabelText(`${group} 新 Slot ID`), { target: { value: slot } });
  fireEvent.change(screen.getByLabelText(`${group} 新 Capability`), { target: { value: capability } });
}

function StatefulEditor({ spec = createSingleLLMAgentSpec() }: { spec?: AgentSpecV1 }) {
  const [state, dispatch] = useReducer(agentEditorReducer, spec, createAgentEditorState);
  return <><RequirementsEditor state={state} dispatch={dispatch} disabled={false} /><output data-testid="spec">{JSON.stringify(state.spec)}</output></>;
}

describe("RequirementsEditor", () => {
  it("rejects invalid slot IDs before dispatch instead of silently clearing the input", () => {
    const dispatch = setup();
    enterSlot("Tools", "Bad Slot", "web.search");
    expect(screen.getByRole("button", { name: "添加 Tools Slot" })).toBeDisabled();
    expect(screen.getByLabelText("Tools 新 Slot ID")).toHaveAccessibleDescription(/Slot ID/);
    fireEvent.click(screen.getByRole("button", { name: "添加 Tools Slot" }));
    expect(dispatch).not.toHaveBeenCalled();
    expect(screen.getByLabelText("Tools 新 Slot ID")).toHaveValue("Bad Slot");
    expect(screen.getByLabelText("Tools 新 Capability")).toHaveValue("web.search");
  });

  it("blocks duplicate slot IDs instead of replacing the existing capability", () => {
    const spec = createSingleLLMAgentSpec();
    spec.requirements.tools.search = { capability: "web.search" };
    const dispatch = setup(spec);
    enterSlot("Tools", "search", "http.request");
    expect(screen.getByRole("button", { name: "添加 Tools Slot" })).toBeDisabled();
    expect(screen.getByLabelText("Tools 新 Slot ID")).toHaveAccessibleDescription(/已存在/);
    fireEvent.click(screen.getByRole("button", { name: "添加 Tools Slot" }));
    expect(dispatch).not.toHaveBeenCalled();
    expect(screen.getByLabelText("search capability")).toHaveValue("web.search");
    expect(screen.getByLabelText("Tools 新 Slot ID")).toHaveValue("search");
  });

  it.each([
    ["Tools", "search", "", /必填/],
    ["Knowledge", "docs", "Knowledge Query", /小写字母/],
    ["Models", "research", "chat, BadValue", /小写字母/],
    ["Models", "research", "chat,chat", /不可重复/],
    ["Models", "research", Array.from({ length: 17 }, (_, index) => `cap${index}`).join(","), /最多声明 16/],
  ])("validates %s new capabilities before adding %s", (group, slot, capability, expectedError) => {
    const dispatch = setup();
    enterSlot(group, slot, capability);
    expect(screen.getByRole("button", { name: `添加 ${group} Slot` })).toBeDisabled();
    expect(screen.getByLabelText(`${group} 新 Capability`)).toHaveAccessibleDescription(expectedError);
    expect(screen.getByLabelText(`${group} 新 Capability`)).toHaveValue(capability);
    expect(dispatch).not.toHaveBeenCalled();
  });

  it.each<[RequirementKind, string, number]>([
    ["models", "Models", 16], ["tools", "Tools", 64], ["knowledge", "Knowledge", 32],
  ])("enforces the %s slot limit without dropping new input", (kind, title, limit) => {
    const spec = createSingleLLMAgentSpec();
    const entries = Array.from({ length: limit }, (_, index) => [`slot${index}`, kind === "models" ? { capabilities: ["chat"] } : { capability: "knowledge.query" }]);
    spec.requirements = { ...spec.requirements, [kind]: Object.fromEntries(entries) };
    const dispatch = setup(spec);
    enterSlot(title, "overflow", "chat");
    expect(screen.getByRole("button", { name: `添加 ${title} Slot` })).toBeDisabled();
    expect(screen.getByLabelText(`${title} 新 Slot ID`)).toHaveAccessibleDescription(new RegExp(`最多声明 ${limit}`));
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("clears new inputs only after a valid slot is dispatched", () => {
    const dispatch = setup();
    enterSlot("Tools", "search", "web.search");
    fireEvent.click(screen.getByRole("button", { name: "添加 Tools Slot" }));
    expect(dispatch).toHaveBeenCalledExactlyOnceWith({ type: "requirement.capability.set", kind: "tools", slot: "search", capability: "web.search" });
    expect(screen.getByLabelText("Tools 新 Slot ID")).toHaveValue("");
    expect(screen.getByLabelText("Tools 新 Capability")).toHaveValue("");
  });

  it("allows the same ID in separate Requirement groups and normalizes new Model capabilities", () => {
    const dispatch = setup();
    enterSlot("Tools", "primary", "web.search");
    expect(screen.getByRole("button", { name: "添加 Tools Slot" })).toBeEnabled();
    enterSlot("Models", "reviewer", "chat, tool_call");
    fireEvent.click(screen.getByRole("button", { name: "添加 Models Slot" }));
    expect(dispatch).toHaveBeenLastCalledWith({ type: "requirement.model.set", slot: "reviewer", capabilities: ["chat", "tool_call"] });
    expect(screen.getByLabelText("Models 新 Capability")).toHaveValue("chat");
  });

  it("keeps existing Models editable through clear and sequential comma typing", async () => {
    const user = userEvent.setup();
    render(<StatefulEditor />);
    const input = screen.getByLabelText("primary capability");
    await user.clear(input);
    expect(input).toHaveValue("");
    expect(input).toHaveAccessibleDescription(/必填/);
    await user.type(input, "chat,tool_call");
    expect(input).toHaveValue("chat,tool_call");
    expect(JSON.parse(screen.getByTestId("spec").textContent ?? "{}").requirements.models.primary.capabilities).toEqual(["chat", "tool_call"]);
    await user.tab();
    expect(input).toHaveValue("chat, tool_call");
  });

  it("keeps existing Tools editable through an invalid empty intermediate value", async () => {
    const user = userEvent.setup();
    const spec = createSingleLLMAgentSpec();
    spec.requirements.tools.search = { capability: "web.search" };
    render(<StatefulEditor spec={spec} />);
    const input = screen.getByLabelText("search capability");
    await user.clear(input);
    expect(input).toHaveValue("");
    expect(input).toHaveAccessibleDescription(/必填/);
    await user.type(input, "http.request");
    expect(input).toHaveValue("http.request");
    expect(input).not.toHaveAttribute("aria-describedby");
  });

  it("disables additions, edits, and deletion when read-only", () => {
    setup(createSingleLLMAgentSpec(), true);
    for (const input of screen.getAllByRole("textbox")) expect(input).toBeDisabled();
    for (const button of screen.getAllByRole("button")) expect(button).toBeDisabled();
  });
});

it("adds fixed workspace Executor requirements without enabling a node", () => {
  render(<StatefulEditor />);
  expect(screen.getByLabelText("Executors 新 Capability")).toHaveValue("workspace");
  expect(screen.getByLabelText("Executors 新 Capability")).toHaveAttribute("readonly");
  fireEvent.change(screen.getByLabelText("Executors 新 Slot ID"), { target: { value: "sandbox" } });
  fireEvent.click(screen.getByRole("button", { name: "添加 Executors Slot" }));
  const spec = JSON.parse(screen.getByTestId("spec").textContent!);
  expect(spec.requirements.executors).toEqual({ sandbox: { capability: "workspace" } });
  expect(spec.nodes.assistant).not.toHaveProperty("workspace");
  fireEvent.click(screen.getByRole("button", { name: "删除 sandbox" }));
  expect(JSON.parse(screen.getByTestId("spec").textContent!).requirements.executors).toEqual({});
});
