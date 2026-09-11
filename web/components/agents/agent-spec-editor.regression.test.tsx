import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import { createSingleLLMAgentSpec, type AgentSpecV1 } from "../../lib/agent-spec-v1";
import type { AgentSpecDocument } from "../../lib/control-api";
import { AgentSpecEditor } from "./agent-spec-editor";

afterEach(cleanup);

function Harness({ initial = createSingleLLMAgentSpec() }: { initial?: AgentSpecV1 }) {
  const [spec, setSpec] = useState<AgentSpecDocument>(initial);
  return <><AgentSpecEditor onChange={setSpec} value={spec} /><output data-testid="current-spec">{JSON.stringify(spec)}</output></>;
}

function current(): AgentSpecV1 {
  return JSON.parse(screen.getByTestId("current-spec").textContent || "{}");
}

describe("AgentSpec controlled editing regressions", () => {
  it("selects node Tool and Knowledge slots from the declared requirements only", async () => {
    const user = userEvent.setup();
    const initial = createSingleLLMAgentSpec();
    initial.requirements.tools = { search: { capability: "web.search" } };
    initial.requirements.knowledge = { docs: { capability: "knowledge.query" } };
    const assistant = initial.nodes.assistant;
    if (assistant.kind === "llm") assistant.tool_slots = ["stale"];
    render(<Harness initial={initial} />);

    // A reference whose declaration was removed stays visible instead of silently vanishing.
    expect(screen.getByLabelText("stale · 未声明")).toBeChecked();
    await user.click(screen.getByLabelText("search"));
    await user.click(screen.getByLabelText("docs"));
    expect(current().nodes.assistant).toEqual(expect.objectContaining({ tool_slots: ["stale", "search"], knowledge_slots: ["docs"] }));
    await user.click(screen.getByLabelText("search"));
    await user.click(screen.getByLabelText("stale · 未声明"));
    expect(current().nodes.assistant).toEqual(expect.objectContaining({ tool_slots: [], knowledge_slots: ["docs"] }));
  });

  it("keeps the canvas and focus when Instruction is cleared and retyped", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const instruction = screen.getByLabelText("Instruction");
    await user.clear(instruction);
    expect(screen.getByTestId("agent-canvas")).toBeInTheDocument();
    expect(instruction).toHaveFocus();
    expect(instruction).toHaveValue("");
    expect(current().nodes.assistant).toEqual(expect.objectContaining({ instruction: "" }));
    expect(screen.getByText(/AGENT_SPEC_LIMIT_EXCEEDED/)).toBeInTheDocument();
    await user.type(instruction, "重新输入指令");
    expect(current().nodes.assistant).toEqual(expect.objectContaining({ instruction: "重新输入指令" }));
    expect(screen.queryByText(/AGENT_SPEC_LIMIT_EXCEEDED/)).not.toBeInTheDocument();
  });

  it("keeps multiple model capabilities editable across empty intermediate states", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const input = screen.getByLabelText("primary capability");
    await user.clear(input);
    expect(screen.getByTestId("agent-canvas")).toBeInTheDocument();
    expect(input).toHaveValue("");
    await user.type(input, "chat,tool_call");
    expect(current().requirements.models.primary.capabilities).toEqual(["chat", "tool_call"]);
  });

  it("retains the canvas for out-of-range generation values and lets the user correct them", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const temperature = screen.getByLabelText("Temperature");
    await user.type(temperature, "3");
    expect(screen.getByTestId("agent-canvas")).toBeInTheDocument();
    expect(screen.getByText(/AGENT_SPEC_LIMIT_EXCEEDED/)).toBeInTheDocument();
    await user.clear(temperature);
    await user.type(temperature, "0.2");
    expect(current().nodes.assistant).toEqual(expect.objectContaining({ generation: { temperature: 0.2 } }));
  });

  it("keeps an empty composition editable and retains incomplete raw JSON support", async () => {
    const user = userEvent.setup();
    const spec = createSingleLLMAgentSpec();
    spec.nodes.flow = { kind: "sequence", children: [] };
    spec.root = "flow";
    render(<Harness initial={spec} />);
    expect(screen.getByTestId("agent-canvas")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /^JSON$/ }));
    fireEvent.change(screen.getByLabelText("AgentSpec JSON"), { target: { value: '{"root":"incomplete"}' } });
    await user.click(screen.getByRole("button", { name: "应用 JSON 到工作副本" }));
    expect(screen.queryByTestId("agent-canvas")).not.toBeInTheDocument();
    expect(current()).toEqual({ root: "incomplete" });
  });
});
