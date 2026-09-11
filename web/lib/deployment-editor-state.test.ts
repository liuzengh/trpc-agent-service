import { beforeEach, describe, expect, it } from "vitest";
import { clearDeploymentPreparations, establishPreparationIdentity, inputFrom, loadPreparation, pairingRows, preparationKey, safeDeploymentReturn, savePreparation, selectionFrom, selectionFromQuery, selectionQuery, type Preparation } from "./deployment-editor-state";
import { agentVersion, input, profileRevision } from "../test/deployment-fixtures";
beforeEach(() => sessionStorage.clear());
const state = (): Preparation => ({ version: 1, selection: selectionFrom(input), name: "研究", description: "", pending: { kind: "publish", key: "original-key", body: { expected_latest_revision_number: null, input } } });
describe("Deployment preparation state", () => {
  it("roundtrips fixed sources without Draft/latest selectors", () => {
    expect(inputFrom(selectionFromQuery(new URLSearchParams(selectionQuery(selectionFrom(input)))))).toEqual(input);
    expect(inputFrom({ ...selectionFrom(input), agentVersion: 0 })).toBeNull();
    expect(inputFrom({ ...selectionFrom(input), profileRevision: 1.5 })).toBeNull();
    expect(inputFrom(selectionFromQuery(new URLSearchParams("agent=a&version=latest&profile=p&revision=2")))).toBeNull();
  });
  it("stores the exact pending request, isolates tenants and users, strips unrelated data", () => {
    const key = preparationKey("u", "t", "d");
    sessionStorage.setItem(key, JSON.stringify({ ...state(), config: { credential: "not-retained" }, selection: { ...state().selection, other: "discard" } }));
    expect(loadPreparation(sessionStorage, key)).toEqual(state());
    expect(loadPreparation(sessionStorage, preparationKey("other", "t", "d"))).toBeNull();
    expect(loadPreparation(sessionStorage, preparationKey("u", "other", "d"))).toBeNull();
  });
  it("restores locked pending input rather than a different saved UI selection", () => {
    savePreparation(sessionStorage, "p", { ...state(), selection: { ...state().selection, agentVersion: 100 } });
    expect(loadPreparation(sessionStorage, "p")?.selection.agentVersion).toBe(3);
  });
  it.each(["{broken", "null", JSON.stringify({ version: 2 }), JSON.stringify({ ...state(), pending: { kind: "publish", key: "k", body: {} } }), JSON.stringify({ ...state(), pending: { ...state().pending, key: "line\nbreak" } })])("rejects malformed preparation %s", (raw) => {
    sessionStorage.setItem("p", raw); expect(loadPreparation(sessionStorage, "p")).toBeNull();
  });
  it("clears preparation on identity switch and logout without touching unrelated state", () => {
    establishPreparationIdentity(sessionStorage, "u"); savePreparation(sessionStorage, preparationKey("u", "t"), state()); sessionStorage.setItem("unrelated", "keep");
    establishPreparationIdentity(sessionStorage, "other"); expect(loadPreparation(sessionStorage, preparationKey("u", "t"))).toBeNull();
    savePreparation(sessionStorage, preparationKey("other", "t"), state()); clearDeploymentPreparations(sessionStorage); expect(sessionStorage.getItem("unrelated")).toBe("keep");
  });
  it("matches exact category/key, not remote MCP tool_name, and includes unused declarations", () => {
    const profile = structuredClone(profileRevision); profile.config.tools = { web_search: { tool_name: "search" } };
    const agent = structuredClone(agentVersion); agent.spec.requirements.tools.unused = { capability: "web.search" };
    const rows = pairingRows(agent, profile);
    expect(rows.find((r) => r.name === "search")).toMatchObject({ present: false, nodes: ["researcher"] });
    expect(rows.find((r) => r.name === "unused")).toMatchObject({ present: false, nodes: [] });
    expect(rows.some((r) => r.name === "web_search")).toBe(false);
  });
  it("requires session but not memory and ignores other storage names", () => {
    const profile = structuredClone(profileRevision); profile.config.storage = { conversation_state: { kind: "postgres_state" } };
    const rows = pairingRows(agentVersion, profile);
    expect(rows.find((r) => r.name === "session")?.present).toBe(false);
    expect(rows.find((r) => r.name === "memory")).toMatchObject({ present: true, note: "未声明 · 不启用" });
    expect(rows.some((r) => r.name === "conversation_state")).toBe(false);
  });
  it.each(["https://elsewhere.test/", "//elsewhere.test/", "/tenants/other/deployments/d", "/tenants/t/deployments/../members", "/tenants/t/deployments/%2e%2e/members", "/tenants/t/deployments/d\\..", "/tenants/t/deployments\n"]) ("rejects an unsafe or cross-tenant return link %s", (url) => expect(safeDeploymentReturn(url, "t")).toBeNull());
  it("preserves a same-tenant fixed-source return link", () => expect(safeDeploymentReturn("/tenants/t/deployments/d?prepare=1&profile=p&revision=3", "t")).toBe("/tenants/t/deployments/d?prepare=1&profile=p&revision=3"));
});
