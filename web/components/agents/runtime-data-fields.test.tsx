import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { AgentSpecEditor } from "./agent-spec-editor";
import type { AgentSpecDocument } from "../../lib/control-api";
import { NodeDataFields, RuntimeSummaryFields } from "./runtime-data-fields";
import { createSingleLLMAgentSpec, type LLMNodeV1 } from "../../lib/agent-spec-v1";
import { agentEditorReducer, createAgentEditorState } from "../../lib/agent-editor-state";
afterEach(cleanup);
function Harness({ disabled = false }: { disabled?: boolean }) {
  const [state, set] = useState(() => createAgentEditorState(createSingleLLMAgentSpec()));
  return <><RuntimeSummaryFields state={state} disabled={disabled} dispatch={(action) => set((s) => agentEditorReducer(s, action))} />
    <NodeDataFields node={state.spec.nodes.assistant as LLMNodeV1} disabled={disabled} replace={(node) => set((s) => agentEditorReducer(s, { type: "node.replace", nodeID: "assistant", node }))} />
    <output data-testid="spec">{JSON.stringify(state.spec)}</output></>;
}
const spec = () => JSON.parse(screen.getByTestId("spec").textContent!);
describe("runtime capability controls", () => {
  it("starts absent, edits explicit memory values and removes declarations", () => {
    render(<Harness />); expect(spec().nodes.assistant.memory).toBeUndefined();
    fireEvent.click(screen.getByLabelText("声明 Memory 配置"));
    expect(spec().nodes.assistant.memory).toEqual({ tools: [] });
    fireEvent.click(screen.getByLabelText("memory_search"));
    fireEvent.change(screen.getByLabelText("Memory 预加载条目"), { target: { value: "-1" } });
    expect(spec().nodes.assistant.memory).toEqual({ tools: ["memory_search"], preload_limit: -1 });
    fireEvent.change(screen.getByLabelText("Memory 预加载条目"), { target: { value: "0" } });
    expect(spec().nodes.assistant.memory.preload_limit).toBe(0);
    fireEvent.change(screen.getByLabelText("Memory 预加载条目"), { target: { value: "" } });
    expect(spec().nodes.assistant.memory).not.toHaveProperty("preload_limit");
    fireEvent.click(screen.getByLabelText("声明 Memory 配置"));
    expect(spec().nodes.assistant).not.toHaveProperty("memory");
  });
  it("requires explicit summary settings and strips only forbidden disabled fields", () => {
    render(<Harness />);
    fireEvent.change(screen.getByLabelText("生成会话摘要"), { target: { value: "true" } });
    expect(spec().runtime.summary).not.toHaveProperty("event_threshold");
    fireEvent.change(screen.getByLabelText("Summary 模型槽"), { target: { value: "primary" } });
    fireEvent.change(screen.getByLabelText("摘要事件触发阈值"), { target: { value: "4" } });
    fireEvent.change(screen.getByLabelText("消费会话摘要"), { target: { value: "true" } });
    expect(spec().runtime.summary).toEqual({ enabled: true, model_slot: "primary", event_threshold: 4 });
    fireEvent.change(screen.getByLabelText("生成会话摘要"), { target: { value: "false" } });
    expect(spec().runtime.summary).toEqual({ enabled: false });
    expect(spec().nodes.assistant.add_session_summary).toBe(true);
    fireEvent.click(screen.getByText("移除 runtime 声明")); expect(spec()).not.toHaveProperty("runtime");
  });
  it("distinguishes absent and explicit disabled Artifact without injecting tools", () => {
    render(<Harness />);
    fireEvent.change(screen.getByLabelText("Artifact 服务"), { target: { value: "false" } });
    expect(spec().nodes.assistant.artifact).toEqual({ enabled: false });
    fireEvent.change(screen.getByLabelText("Artifact 服务"), { target: { value: "true" } });
    expect(spec().nodes.assistant.tool_slots).toEqual([]);
    fireEvent.change(screen.getByLabelText("Artifact 服务"), { target: { value: "absent" } });
    expect(spec().nodes.assistant).not.toHaveProperty("artifact");
    expect(screen.getByText(/不是 SDK 内建工具/, { selector: "[role='tooltip']" })).toHaveTextContent("artifact_save、artifact_load、artifact_list、artifact_delete");
    expect(screen.getByText(/工具保存立即持久化/, { selector: "[role='tooltip']" })).toHaveTextContent("不随失败 Run 回滚");
  });
  it("disables controls and distinguishes supported Summary execution from cross-publication history migration", () => {
    render(<Harness disabled />);
    expect(screen.getByLabelText("声明 Memory 配置")).toBeDisabled();
    expect(screen.getByLabelText("生成会话摘要")).toBeDisabled();
    expect(screen.getByLabelText("Artifact 服务")).toBeDisabled();
    expect(screen.getByText(/Session Summary 已支持配置、发布和 Worker 执行/, { selector: "[role='tooltip']" })).toBeInTheDocument();
    expect(screen.getByText(/新的 DeploymentRevision 对应新的 Session/, { selector: "[role='tooltip']" })).toHaveTextContent("不继承旧版本的会话历史或摘要");
    expect(screen.getByText(/内部 overlay 保留摘要元数据/, { selector: "[role='tooltip']" })).toHaveTextContent("不等于跨发布版本的历史迁移");
    expect(screen.queryByText(/P0a|含新增运行声明的 Deployment 暂不支持发布/)).toBeNull();
  });
});

function EditorHarness() {
  const [value, set] = useState<AgentSpecDocument>(createSingleLLMAgentSpec());
  return <><AgentSpecEditor value={value} onChange={set} /><output data-testid="source">{JSON.stringify(value)}</output></>;
}
it("keeps runtime edits on the real controlled canvas and round-trips through JSON", () => {
  render(<EditorHarness />);
  fireEvent.change(screen.getByLabelText("生成会话摘要"), { target: { value: "true" } });
  expect(screen.getByTestId("agent-canvas")).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("Summary 模型槽"), { target: { value: "primary" } });
  fireEvent.change(screen.getByLabelText("摘要事件触发阈值"), { target: { value: "3" } });
  fireEvent.click(screen.getByLabelText("声明 Memory 配置"));
  fireEvent.click(screen.getByLabelText("memory_load"));
  const before = JSON.parse(screen.getByTestId("source").textContent!);
  fireEvent.click(screen.getByRole("button", { name: "JSON" }));
  expect(JSON.parse((screen.getByLabelText("AgentSpec JSON") as HTMLTextAreaElement).value)).toEqual(before);
  fireEvent.click(screen.getByRole("button", { name: "画布" }));
  expect(screen.getByLabelText("memory_load")).toBeChecked();
  expect(screen.getByLabelText("摘要事件触发阈值")).toHaveValue(3);
});

it("round-trips explicit workspace selection through the actual Agent canvas and JSON", () => {
  render(<EditorHarness />);
  fireEvent.change(screen.getByLabelText("Executors 新 Slot ID"), { target: { value: "sandbox" } });
  fireEvent.click(screen.getByRole("button", { name: "添加 Executors Slot" }));
  fireEvent.change(screen.getByLabelText("Workspace Executor Slot"), { target: { value: "sandbox" } });
  fireEvent.click(screen.getByLabelText("workspace_exec"));
  const before = JSON.parse(screen.getByTestId("source").textContent!);
  expect(before.requirements.executors).toEqual({ sandbox: { capability: "workspace" } });
  expect(before.nodes.assistant.workspace).toEqual({ executor_slot: "sandbox", tools: ["workspace_exec"] });
  expect(before.nodes.assistant.artifact).toBeUndefined();
  fireEvent.click(screen.getByRole("button", { name: "JSON" }));
  expect(JSON.parse((screen.getByLabelText("AgentSpec JSON") as HTMLTextAreaElement).value)).toEqual(before);
  fireEvent.click(screen.getByRole("button", { name: "画布" }));
  expect(screen.getByLabelText("workspace_exec")).toBeChecked();
  expect(screen.getByLabelText("workspace_save_artifact")).not.toBeChecked();
});
