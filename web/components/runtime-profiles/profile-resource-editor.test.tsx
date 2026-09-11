import { useState } from "react";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { CredentialActions, CredentialStates, ProfileConfig } from "../../lib/runtime-profile-api";
import { ProfileResourceEditor } from "./profile-resource-editor";

afterEach(cleanup);
const initial: ProfileConfig = {
  models: { primary: { kind: "openai_compatible", model: "example-chat", base_url: "https://model.example.com/v1", capabilities: ["chat"] } },
  tools: { search: { kind: "mcp_streamable_http", server_url: "https://mcp.example.com", toolset_name: "search", tool_name: "web", auth: { kind: "bearer" }, capability: "web.search" } },
  knowledge: { docs: { kind: "qdrant_openai", host: "qdrant.example.com", port: 6334, tls: true, collection: "docs", embedding: { model: "example-embedding", base_url: "https://embedding.example.com/v1", dimensions: 1536 } } },
  storage: { memory: { kind: "postgres_state", destination: { host: "db.example.com", port: 5432, database: "app", username: "agent", sslmode: "require" } } },
};
const states: CredentialStates = { models: { primary: { api_key: { configured: true, status: "active", credential_revision: 2, association_token: "association-never-visible" } } } };
function Harness({ config = initial, credentialStates = {}, isOwner = true, readOnly = false, changed = () => {} }: { config?: ProfileConfig; credentialStates?: CredentialStates; isOwner?: boolean; readOnly?: boolean; changed?(config: ProfileConfig, credentials: CredentialActions): void }) {
  const [value, setValue] = useState(config);
  const [credentials, setCredentials] = useState<CredentialActions>({});
  return <ProfileResourceEditor config={value} credentials={credentials} credentialStates={credentialStates} isOwner={isOwner} readOnly={readOnly} onChange={(next, actions) => { setValue(next); setCredentials(actions); changed(next, actions); }} />;
}
async function selectCategory(name: string) {
  await userEvent.click(screen.getByRole("button", { name: new RegExp(`^${name} ·`) }));
}

describe("Runtime Profile resource forms", () => {
  it("keeps category labels and selection independent of their visual color", async () => {
    render(<Harness />);
    for (const [label, category] of [["Models", "models"], ["Tools", "tools"], ["Knowledge", "knowledge"], ["Storage", "storage"]]) {
      const button = screen.getByRole("button", { name: new RegExp(`^${label} ·`) });
      expect(button.parentElement).toHaveAttribute("data-resource-category", category);
      await userEvent.click(button);
      expect(button).toHaveAttribute("aria-expanded", "true");
    }
  });
  it("edits model fields and fixed capabilities without losing empty input", async () => {
    const user = userEvent.setup(); const changed = vi.fn();
    render(<Harness changed={changed} />);
    const input = screen.getByRole("textbox", { name: "模型名称" });
    await user.clear(input); expect(input).toHaveValue(""); expect(input).toHaveFocus();
    await user.type(input, "new-chat"); await user.click(screen.getByRole("checkbox", { name: "tool_call" }));
    expect(changed.mock.lastCall?.[0].models.primary).toMatchObject({ model: "new-chat", capabilities: ["chat", "tool_call"] });
    expect(screen.getByText("openai_compatible")).toBeVisible();
  });
  it("shows MCP fields and bearer-only credentials, removes only staged token when switching to none", async () => {
    const user = userEvent.setup(); const changed = vi.fn(); render(<Harness changed={changed} />);
    await selectCategory("Tools");
    expect(screen.getByRole("textbox", { name: "Server URL" })).toHaveValue("https://mcp.example.com");
    await user.selectOptions(screen.getByLabelText("Bearer Token 操作"), "replace");
    await user.type(screen.getByLabelText("Bearer Token 新值"), "test-value");
    expect(changed.mock.lastCall?.[1].tools.search.bearer_token).toEqual({ action: "replace", value: "test-value" });
    await user.selectOptions(screen.getByLabelText("认证方式"), "none");
    expect(screen.queryByLabelText("Bearer Token 新值")).not.toBeInTheDocument();
    expect(changed.mock.lastCall?.[1].tools?.search).toBeUndefined();
    expect(changed.mock.lastCall?.[0].tools.search.auth.kind).toBe("none");
  });
  it("edits Qdrant and embedding numeric fields while preserving cleared values", async () => {
    const user = userEvent.setup(); const changed = vi.fn(); render(<Harness changed={changed} />);
    await selectCategory("Knowledge");
    expect(screen.getByRole("textbox", { name: "Collection" })).toHaveValue("docs");
    const port = screen.getByRole("spinbutton", { name: "Qdrant 端口" });
    await user.clear(port); expect(port).toHaveValue(null);
    expect(changed.mock.lastCall?.[0].knowledge.docs.port).toBeUndefined();
    await user.type(port, "6333"); expect(port).toHaveValue(6333);
    await user.clear(screen.getByRole("spinbutton", { name: "Embedding 维度" }));
    expect(changed.mock.lastCall?.[0].knowledge.docs.embedding.dimensions).toBeUndefined();
  });
  it("edits every non-secret PostgreSQL destination and uses a write-only DSN", async () => {
    const user = userEvent.setup(); const changed = vi.fn(); render(<Harness changed={changed} />);
    await selectCategory("Storage");
    expect(screen.getByLabelText("数据库主机")).toHaveValue("db.example.com");
    expect(screen.getByLabelText("数据库端口")).toHaveValue(5432);
    expect(screen.getByLabelText("数据库名称")).toHaveValue("app");
    expect(screen.getByLabelText("数据库用户名")).toHaveValue("agent");
    await user.selectOptions(screen.getByLabelText("SSL 模式"), "verify-full");
    await user.selectOptions(screen.getByLabelText("PostgreSQL DSN 操作"), "replace");
    const dsn = screen.getByLabelText("PostgreSQL DSN 新值"); expect(dsn).toHaveAttribute("type", "password");
    await user.type(dsn, "postgresql://agent:test@db.example.com:5432/app?sslmode=verify-full");
    expect(changed.mock.lastCall?.[0].storage.memory.destination.sslmode).toBe("verify-full");
    expect(changed.mock.lastCall?.[0].storage.memory).not.toHaveProperty("dsn");
  });
  it("keeps credentials omitted by default, replaces from an empty password, and clears with no value", async () => {
    const user = userEvent.setup(); const changed = vi.fn(); render(<Harness changed={changed} credentialStates={states} />);
    expect(screen.getByText("已配置 · 凭证 r2")).toBeVisible();
    expect(screen.queryByText("association-never-visible")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("API Key 新值")).not.toBeInTheDocument();
    await user.type(screen.getByLabelText("模型名称"), "2"); expect(changed.mock.lastCall?.[1]).toEqual({});
    await user.selectOptions(screen.getByLabelText("API Key 操作"), "replace");
    expect(screen.getByLabelText("API Key 新值")).toHaveValue("");
    await user.type(screen.getByLabelText("API Key 新值"), "example-value");
    await user.selectOptions(screen.getByLabelText("API Key 操作"), "clear");
    expect(changed.mock.lastCall?.[1].models.primary.api_key).toEqual({ action: "clear" });
    await user.selectOptions(screen.getByLabelText("API Key 操作"), "keep"); expect(changed.mock.lastCall?.[1]).toEqual({});
  });
  it("keeps Qdrant and embedding credential actions independent", async () => {
    const user = userEvent.setup(); const changed = vi.fn(); render(<Harness changed={changed} />);
    await selectCategory("Knowledge");
    await user.selectOptions(screen.getByLabelText("Qdrant API Key 操作"), "replace");
    await user.type(screen.getByLabelText("Qdrant API Key 新值"), "qdrant-value");
    await user.selectOptions(screen.getByLabelText("Embedding API Key 操作"), "clear");
    expect(changed.mock.lastCall?.[1].knowledge.docs).toEqual({ qdrant_api_key: { action: "replace", value: "qdrant-value" }, embedding_api_key: { action: "clear" } });
    await selectCategory("Models"); await selectCategory("Knowledge");
    expect(screen.getByLabelText("Qdrant API Key 新值")).toHaveValue("qdrant-value");
  });
  it("rejects invalid and duplicate names, permits same name across categories and safe own keys", async () => {
    const user = userEvent.setup(); const changed = vi.fn(); render(<Harness changed={changed} />);
    const name = screen.getByLabelText("新资源名称");
    await user.type(name, "Bad name"); expect(screen.getByRole("button", { name: "添加资源" })).toBeDisabled();
    await user.clear(name); await user.type(name, "primary"); expect(screen.getByText("当前分类中已存在同名资源")).toBeVisible();
    expect(screen.getByRole("button", { name: "添加资源" })).toBeDisabled();
    await user.clear(name); await user.type(name, "constructor"); await user.click(screen.getByRole("button", { name: "添加资源" }));
    expect(Object.hasOwn(changed.mock.lastCall?.[0].models, "constructor")).toBe(true);
    await selectCategory("Tools"); await user.type(screen.getByLabelText("新资源名称"), "primary"); await user.click(screen.getByRole("button", { name: "添加资源" }));
    expect(changed.mock.lastCall?.[0].tools.primary.kind).toBe("mcp_streamable_http");
    expect(changed.mock.lastCall?.[0].models.primary.model).toBe("example-chat");
  });
  it("enforces resource count limits and preserves the attempted name", async () => {
    const user = userEvent.setup(); const config = { ...initial, models: Object.fromEntries(Array.from({ length: 16 }, (_, i) => [`model_${i}`, initial.models.primary])) };
    render(<Harness config={config} />); await user.type(screen.getByLabelText("新资源名称"), "extra");
    expect(screen.getByRole("button", { name: "添加资源" })).toBeDisabled(); expect(screen.getByLabelText("新资源名称")).toHaveValue("extra");
    expect(screen.getByText("Models 最多 16 个资源")).toBeVisible();
  });
  it("requires confirmation and deletes only the currently selected resource and its pending credentials", async () => {
    const user = userEvent.setup(); const changed = vi.fn(); render(<Harness changed={changed} />);
    await user.selectOptions(screen.getByLabelText("API Key 操作"), "replace");
    await user.click(screen.getByRole("button", { name: "删除资源 primary" }));
    expect(changed.mock.lastCall?.[0].models).toHaveProperty("primary");
    const confirm = screen.getByRole("dialog", { name: "删除资源 primary" });
    await user.click(within(confirm).getByRole("button", { name: "确认删除" }));
    expect(changed.mock.lastCall?.[0].models).toEqual({});
    expect(changed.mock.lastCall?.[0].tools.search).toEqual(initial.tools.search);
    expect(changed.mock.lastCall?.[1]).toEqual({});
  });
  it("allows MEMBER non-secret edits but protects existing credential associations", async () => {
    const user = userEvent.setup(); const changed = vi.fn(); render(<Harness changed={changed} credentialStates={states} isOwner={false} />);
    expect(screen.queryByLabelText("API Key 操作")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "删除资源 primary" })).toBeDisabled();
    expect(screen.getByLabelText("Base URL")).toBeDisabled();
    await user.type(screen.getByLabelText("模型名称"), "2");
    expect(changed.mock.lastCall?.[0].models.primary.model).toBe("example-chat2");
    expect(changed.mock.lastCall?.[1]).toEqual({});
    await selectCategory("Storage"); expect(screen.getByRole("button", { name: "删除资源 memory" })).not.toBeDisabled();
  });
  it("also protects a cleared credential association and MCP auth changes for MEMBER", async () => {
    const cleared: CredentialStates = { tools: { search: { bearer_token: { configured: false, status: "cleared", credential_revision: 3 } } } };
    render(<Harness credentialStates={cleared} isOwner={false} />); await selectCategory("Tools");
    expect(screen.getByRole("button", { name: "删除资源 search" })).toBeDisabled();
    expect(screen.getByLabelText("认证方式")).toBeDisabled(); expect(screen.getByLabelText("Server URL")).toBeDisabled();
    expect(screen.getByLabelText("Tool Name")).not.toBeDisabled();
  });
  it("lets read-only users browse all resource forms without editing or credential actions", async () => {
    render(<Harness readOnly credentialStates={states} />);
    expect(screen.queryByLabelText("新资源名称")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "删除资源 primary" })).not.toBeInTheDocument();
    expect(screen.getByLabelText("模型名称")).toHaveAttribute("readonly");
    await selectCategory("Knowledge"); expect(screen.getByLabelText("Collection")).toHaveAttribute("readonly");
    expect(screen.queryByLabelText("Qdrant API Key 操作")).not.toBeInTheDocument();
  });
  it("targets the diagnosed category, resource and field without calling onChange", () => {
    const changed = vi.fn(); const { rerender } = render(<ProfileResourceEditor config={initial} credentials={{}} credentialStates={{}} onChange={changed} isOwner />);
    rerender(<ProfileResourceEditor config={initial} credentials={{}} credentialStates={{}} onChange={changed} isOwner focusTarget={{ category: "knowledge", name: "docs", pointer: "/knowledge/docs/embedding/dimensions" }} />);
    expect(screen.getByLabelText("Embedding 维度")).toHaveFocus(); expect(changed).not.toHaveBeenCalled();
  });
  it("tolerates absent categories and incomplete resource bodies without remounting inputs", async () => {
    const user = userEvent.setup(); const changed = vi.fn();
    const config = { models: { primary: {} }, tools: {}, knowledge: {}, storage: {} };
    render(<Harness config={config} changed={changed} />);
    fireEvent.change(screen.getByLabelText("模型名称"), { target: { value: "m" } });
    await user.click(screen.getByLabelText("模型名称")); await user.keyboard("x");
    expect(screen.getByLabelText("模型名称")).toHaveValue("mx");
  });
  it("keeps busy forms unchanged while preserving category navigation", async () => {
    const changed = vi.fn();
    render(<ProfileResourceEditor config={initial} credentials={{}} credentialStates={states} onChange={changed} isOwner disabled />);
    expect(screen.getByLabelText("模型名称")).toBeDisabled();
    expect(screen.getByLabelText("API Key 操作")).toBeDisabled();
    expect(screen.getByLabelText("新资源名称")).toBeDisabled();
    await selectCategory("Tools"); expect(screen.getByLabelText("Tool Name")).toBeDisabled();
    expect(changed).not.toHaveBeenCalled();
  });
  it("shows unsupported draft values faithfully and permits explicit kind repair and free capability editing", async () => {
    const changed = vi.fn(); const user = userEvent.setup();
    const config = { ...initial, models: { primary: { ...initial.models.primary, kind: "unsupported_kind", capabilities: ["chat", "unsupported_cap"] } }, tools: { search: { ...initial.tools.search, capability: "other.action", auth: { kind: "custom_auth" } } } };
    render(<Harness config={config} changed={changed} />);
    expect(screen.getByText("unsupported_kind")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "使用 openai_compatible" }));
    expect(changed.mock.lastCall?.[0].models.primary.kind).toBe("openai_compatible");
    await user.click(screen.getByRole("checkbox", { name: "unsupported_cap · 不支持" }));
    expect(changed.mock.lastCall?.[0].models.primary.capabilities).toEqual(["chat"]);
    await selectCategory("Tools");
    expect(screen.getByLabelText("认证方式")).toHaveValue("custom_auth");
    expect(screen.getByLabelText("Capability")).toHaveValue("other.action");
    expect(screen.queryByRole("button", { name: "使用 web.search" })).toBeNull();
    await user.clear(screen.getByLabelText("Capability"));
    await user.type(screen.getByLabelText("Capability"), "mcp.search");
    expect(changed.mock.lastCall?.[0].tools.search.capability).toBe("mcp.search");
  });
  it("validates the existing Agent capability grammar without rewriting invalid draft input", async () => {
    const changed = vi.fn(); render(<Harness changed={changed} />); await selectCategory("Tools");
    const field = screen.getByLabelText("Capability");
    for (const capability of ["mcp.search", "a" + "x".repeat(127), "internal.read_v2-checked"]) {
      fireEvent.change(field, { target: { value: capability } });
      expect(changed.mock.lastCall?.[0].tools.search.capability).toBe(capability);
      expect(screen.queryByText(/Capability 须以小写字母开头/)).toBeNull();
    }
    for (const capability of ["", "Upper.read", "has space", "a".repeat(129)]) {
      fireEvent.change(field, { target: { value: capability } });
      expect(field).toHaveValue(capability);
      expect(screen.getByText(/Capability 须以小写字母开头/)).toBeInTheDocument();
    }
    expect(changed.mock.lastCall?.[0].tools.search.server_url).toBe(initial.tools.search.server_url);
    expect(changed.mock.lastCall?.[1]).toEqual({});
  });
  it("keeps immutable MCP capability read-only", async () => {
    const changed = vi.fn();render(<Harness readOnly changed={changed} />);await selectCategory("Tools");
    expect(screen.getByLabelText("Capability")).toHaveAttribute("readonly");expect(changed).not.toHaveBeenCalled();
  });
  it("focuses capability-array and credential diagnostics without exposing internal associations", () => {
    const changed = vi.fn();
    const view = render(<ProfileResourceEditor config={initial} credentials={{}} credentialStates={states} onChange={changed} isOwner focusTarget={{ category: "models", name: "primary", pointer: "/models/primary/capabilities/0" }} />);
    expect(screen.getByRole("group", { name: "Capabilities" })).toHaveFocus();
    view.rerender(<ProfileResourceEditor config={initial} credentials={{}} credentialStates={states} onChange={changed} isOwner focusTarget={{ category: "models", name: "primary", pointer: "/models/primary/api_key_credential_id" }} />);
    expect(screen.getByLabelText("API Key 操作")).toHaveFocus();
    expect(changed).not.toHaveBeenCalled();
  });
  it("handles absent maps and empty nested configuration without inventing values on read", async () => {
    const changed = vi.fn();
    const config = JSON.parse('{"models":{"primary":{}}}');
    render(<ProfileResourceEditor config={config} credentials={{}} credentialStates={{}} onChange={changed} isOwner readOnly />);
    expect(screen.getByLabelText("模型名称")).toHaveValue("");
    await selectCategory("Knowledge"); expect(screen.getByRole("heading", { name: "尚无 Knowledge 资源" })).toBeVisible();
    expect(changed).not.toHaveBeenCalled();
  });

});

it("adds only sdk_sandbox executors without connection or credential fields", async () => {
  const changed = vi.fn();render(<Harness changed={changed} />);
  await selectCategory("Executors");
  fireEvent.change(screen.getByLabelText("新资源名称"), { target: { value: "sandbox" } });
  fireEvent.click(screen.getByRole("button", { name: "添加资源" }));
  expect(changed.mock.lastCall?.[0].executors).toEqual({ sandbox: { kind: "sdk_sandbox" } });
  expect(changed.mock.lastCall?.[1]).toEqual({});
  expect(screen.queryByLabelText("Server URL")).toBeNull();
  expect(screen.queryByLabelText(/Token 新值/)).toBeNull();
  expect(screen.getByText(/添加资源不会自动启用任何节点能力/)).toBeInTheDocument();
});
it("shows immutable executor declarations without editable settings", async () => {
  const changed = vi.fn();render(<Harness config={{ ...initial, executors: { sandbox: { kind: "sdk_sandbox" } } }} readOnly changed={changed} />);
  await selectCategory("Executors");
  expect(screen.getByText("sdk_sandbox")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "添加资源" })).toBeNull();
  expect(screen.queryByRole("button", { name: "删除资源 sandbox" })).toBeNull();
  expect(changed).not.toHaveBeenCalled();
});
