import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { AgentSpecDocument } from "../../lib/control-api";
import { createSingleLLMAgentSpec, isAgentSpecV1, type AgentSpecV1 } from "../../lib/agent-spec-v1";
import { validateAgentSpecLocally } from "../../lib/agent-editor-state";
import { AgentSpecEditor } from "./agent-spec-editor";

beforeEach(() => {
  const values = new Map<string, string>();
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: {
      clear: () => values.clear(),
      getItem: (key: string) => values.get(key) ?? null,
      key: (index: number) => [...values.keys()][index] ?? null,
      get length() { return values.size; },
      removeItem: (key: string) => values.delete(key),
      setItem: (key: string, value: string) => values.set(key, String(value)),
    },
  });
});

afterEach(() => cleanup());

describe("AgentSpecEditor", () => {
  it("initializes an empty backend Draft only after the user selects the template", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn<(next: AgentSpecDocument) => void>();
    render(<AgentSpecEditor onChange={onChange} value={{}} />);

    expect(onChange).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: /使用 Single LLM 模板初始化画布/ }));

    const generated = onChange.mock.calls.at(-1)?.[0];
    expect(isAgentSpecV1(generated)).toBe(true);
    expect(JSON.stringify(generated)).not.toMatch(/editor_(state|metadata)|graph|model_ref/);
    expect(screen.getByTestId("agent-canvas")).toBeInTheDocument();
  });

  it("round-trips a parseable incomplete JSON object for L0 Draft saving", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn<(next: AgentSpecDocument) => void>();
    render(<AgentSpecEditor onChange={onChange} value={{}} />);

    await user.click(screen.getByRole("button", { name: /直接编辑 JSON/ }));
    const input = screen.getByLabelText("AgentSpec JSON");
    fireEvent.change(input, { target: { value: '{"root":"draft_root"}' } });
    await user.click(screen.getByRole("button", { name: "应用 JSON 到工作副本" }));

    expect(onChange).toHaveBeenLastCalledWith({ root: "draft_root" });
    expect(screen.getByText(/不完整但可解析的 Object/)).toBeInTheDocument();
  });

  it("keeps raw unknown fields saveable at Draft L0 and rejects sensitive fields", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn<(next: AgentSpecDocument) => void>();
    render(<AgentSpecEditor onChange={onChange} value={{}} />);
    await user.click(screen.getByRole("button", { name: /直接编辑 JSON/ }));
    const input = screen.getByLabelText("AgentSpec JSON");
    fireEvent.change(input, { target: { value: '{"editor_metadata":{"x":1}}' } });
    await user.click(screen.getByRole("button", { name: "应用 JSON 到工作副本" }));
    expect(onChange).toHaveBeenLastCalledWith({ editor_metadata: { x: 1 } });

    fireEvent.change(input, { target: { value: '{"nodes":{"draft":{"token":"secret"}}}' } });
    await user.click(screen.getByRole("button", { name: "应用 JSON 到工作副本" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("敏感字段 /nodes/draft/token");
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it("adds a sequence node, draws its structure, and stores drag coordinates outside the Spec", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn<(next: AgentSpecDocument) => void>();
    render(<AgentSpecEditor onChange={onChange} storageKey="agent-editor:test" value={createSingleLLMAgentSpec()} />);

    await user.type(screen.getByLabelText("新节点 ID"), "workflow");
    await user.selectOptions(screen.getByLabelText("新节点类型"), "sequence");
    await user.click(screen.getByRole("button", { name: /添加节点/ }));

    const generated = onChange.mock.calls.at(-1)?.[0] as AgentSpecV1;
    expect(generated.root).toBe("workflow");
    expect(generated.nodes.workflow).toEqual(expect.objectContaining({ kind: "sequence", children: ["assistant"] }));
    expect(screen.getByLabelText("节点 workflow")).toBeInTheDocument();
    expect(document.querySelectorAll("svg path[marker-end]").length).toBeGreaterThan(0);

    const assistant = screen.getByLabelText("节点 assistant");
    fireEvent.pointerDown(assistant, { button: 0, clientX: 40, clientY: 40 });
    fireEvent.pointerMove(window, { clientX: 240, clientY: 180 });
    fireEvent.pointerUp(window);
    await waitFor(() => expect(window.localStorage.getItem("agent-editor:test")).toContain("positions"));
    expect(JSON.stringify(generated)).not.toContain("positions");
  });

  it("builds a publishable all-kind AgentSpec with every Requirement group through the UI", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn<(next: AgentSpecDocument) => void>();
    render(<AgentSpecEditor onChange={onChange} value={createSingleLLMAgentSpec()} />);

    async function addNode(nodeID: string, kind: "llm" | "sequence" | "parallel" | "loop") {
      await user.selectOptions(screen.getByLabelText("节点操作"), kind === "llm" ? "child" : "wrap-root");
      if (nodeID === "worker") await user.selectOptions(screen.getByLabelText("新节点父节点"), "fanout");
      await user.type(screen.getByLabelText("新节点 ID"), nodeID);
      await user.selectOptions(screen.getByLabelText("新节点类型"), kind);
      await user.click(screen.getByRole("button", { name: /添加节点/ }));
    }

    await addNode("flow", "sequence");
    await addNode("review", "llm");
    await addNode("fanout", "parallel");
    await addNode("worker", "llm");
    await addNode("retry", "loop");

    await user.type(screen.getByLabelText("Tools 新 Slot ID"), "search");
    await user.type(screen.getByLabelText("Tools 新 Capability"), "web.search");
    await user.click(screen.getByRole("button", { name: "添加 Tools Slot" }));
    await user.type(screen.getByLabelText("Knowledge 新 Slot ID"), "docs");
    await user.type(screen.getByLabelText("Knowledge 新 Capability"), "knowledge.query");
    await user.click(screen.getByRole("button", { name: "添加 Knowledge Slot" }));

    await user.click(screen.getByLabelText("节点 assistant"));
    await user.click(screen.getByLabelText("search"));
    await user.click(screen.getByLabelText("docs"));

    const generated = onChange.mock.calls.at(-1)?.[0];
    expect(isAgentSpecV1(generated)).toBe(true);
    const spec = generated as AgentSpecV1;
    expect(Object.keys(spec.nodes).sort()).toEqual(["assistant", "fanout", "flow", "retry", "review", "worker"]);
    expect(spec.nodes.flow).toEqual(expect.objectContaining({ children: ["assistant", "review"] }));
    expect(spec.nodes.fanout).toEqual(expect.objectContaining({ children: ["flow", "worker"] }));
    expect(Object.values(spec.nodes).map((node) => node.kind)).toEqual(
      expect.arrayContaining(["llm", "sequence", "parallel", "loop"]),
    );
    expect(spec.requirements).toEqual(expect.objectContaining({
      models: { primary: { capabilities: ["chat"] } },
      tools: { search: { capability: "web.search" } },
      knowledge: { docs: { capability: "knowledge.query" } },
    }));
    expect(validateAgentSpecLocally(spec).filter((item) => item.severity === "error")).toEqual([]);
    expect(JSON.stringify(spec)).not.toMatch(/editor_(state|metadata)|positions/);
  });
});
