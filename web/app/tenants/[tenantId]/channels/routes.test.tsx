import { describe, expect, it, vi } from "vitest";
import List from "./page";
import New from "./new/page";
import Workspace from "./[accountId]/page";
import { channelPageQuery } from "./target-query";

vi.mock("../../../../components/channels/account-list", () => ({ AccountList: () => null }));
vi.mock("../../../../components/channels/account-create", () => ({ AccountCreate: () => null }));
vi.mock("../../../../components/channels/account-workspace", () => ({ ChannelWorkspace: () => null }));

const target = { deployment_id: "dep_123", revision_number: "2" };
const route = { tenantId: "tenant-1", accountId: "account-1" };

describe("Channel App Router pages", () => {
  it("awaits route and search promises for all three page roles", async () => {
    const list = await List({ params: Promise.resolve(route), searchParams: Promise.resolve(target) });
    expect(list.props).toEqual({ tenantId: "tenant-1", query: "deployment_id=dep_123&revision_number=2" });
    const create = await New({ params: Promise.resolve(route), searchParams: Promise.resolve(target) });
    expect(create.props).toEqual({ tenantId: "tenant-1", query: "deployment_id=dep_123&revision_number=2" });
    const detail = await Workspace({ params: Promise.resolve(route), searchParams: Promise.resolve(target) });
    expect(detail.props).toEqual({ ...route, query: "deployment_id=dep_123&revision_number=2" });
  });

  it("whitelists the fixed target and excludes arbitrary query data", () => {
    const query = channelPageQuery({ ...target, token: "not-forwarded", returnTo: "https://unrelated.test", manifest_id: "not-input", enabled: "true" });
    expect(query).toBe("deployment_id=dep_123&revision_number=2");
    expect(channelPageQuery({})).toBe("");
  });

  it("opens a shared preflight independently from the fixed deployment target", async () => {
    const page = await Workspace({ params: Promise.resolve(route), searchParams: Promise.resolve({ preflight: "cpf_check_1" }) });
    expect(page.props).toEqual({ ...route, query: "", initialPreflightId: "cpf_check_1" });
    const next = await Workspace({ params: Promise.resolve(route), searchParams: Promise.resolve({ preflight: "cpf_check_2" }) });
    expect(next.key).not.toBe(page.key);
  });

  it.each(["", "https://external.test/task", "../cpf_other", ["cpf_one", "cpf_two"], []])("rejects ambiguous or non-identifier shared preflight input: %j", async (preflight) => {
    const page = await Workspace({ params: Promise.resolve(route), searchParams: Promise.resolve({ preflight }) });
    expect(page.props.initialPreflightId).toBeUndefined();
    expect(page.props.invalidPreflightQuery).toBe(true);
    expect(page.props.query).toBe("");
  });

  it.each([
    { deployment_id: ["dep_123", "dep_456"], revision_number: "2" },
    { deployment_id: "dep_123", revision_number: ["2", "3"] },
    { deployment_id: ["dep_123", "dep_123"], revision_number: ["2", "2"] },
  ])("preserves duplicate target parameters for explicit rejection: %j", async (query) => {
    const page = await New({ params: Promise.resolve(route), searchParams: Promise.resolve(query) });
    const serialized = new URLSearchParams(page.props.query);
    for (const key of ["deployment_id", "revision_number"] as const) {
      expect(serialized.getAll(key)).toEqual(Array.isArray(query[key]) ? query[key] : [query[key]]);
    }
  });

  it.each(["0", "-1", "1.2", "1e2", "9007199254740992", "", " 2 "])("preserves invalid revision %j without coercing it into a fixed target", async (revision_number) => {
    const page = await Workspace({ params: Promise.resolve(route), searchParams: Promise.resolve({ ...target, revision_number }) });
    expect(new URLSearchParams(page.props.query).get("revision_number")).toBe(revision_number);
  });

  it("keeps missing and empty target input distinguishable from no target", () => {
    expect(channelPageQuery({ deployment_id: "dep_123" })).toBe("deployment_id=dep_123");
    expect(channelPageQuery({ deployment_id: [], revision_number: "2" })).toBe("deployment_id=&revision_number=2");
  });

  it("keys every view by tenant, account identity and target query", async () => {
    for (const page of [List, New]) {
      const first = await page({ params: Promise.resolve(route), searchParams: Promise.resolve(target) });
      const tenant = await page({ params: Promise.resolve({ tenantId: "tenant-2" }), searchParams: Promise.resolve(target) });
      const query = await page({ params: Promise.resolve(route), searchParams: Promise.resolve({ ...target, revision_number: "3" }) });
      expect(first.key).not.toBe(tenant.key);
      expect(first.key).not.toBe(query.key);
    }
    const first = await Workspace({ params: Promise.resolve(route), searchParams: Promise.resolve(target) });
    for (const changed of [{ tenantId: "tenant-2", accountId: route.accountId }, { tenantId: route.tenantId, accountId: "account-2" }]) {
      const next = await Workspace({ params: Promise.resolve(changed), searchParams: Promise.resolve(target) });
      expect(first.key).not.toBe(next.key);
    }
    const next = await Workspace({ params: Promise.resolve(route), searchParams: Promise.resolve({ ...target, revision_number: "3" }) });
    expect(first.key).not.toBe(next.key);
  });
});
