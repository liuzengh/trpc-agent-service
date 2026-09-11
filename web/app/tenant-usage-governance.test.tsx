import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import UsageGovernancePage from "./tenants/[tenantId]/usage-governance/page";

const mocks = vi.hoisted(() => ({
  getTenant: vi.fn(),
  getUsagePolicy: vi.fn(),
  getUsageSummary: vi.fn(),
  replaceUsagePolicy: vi.fn(),
  replace: vi.fn(),
}));

vi.mock("next/navigation", () => ({
  useParams: () => ({ tenantId: "tenant-1" }),
  useRouter: () => ({ replace: mocks.replace }),
}));
vi.mock("../lib/control-api", () => ({ controlApi: mocks }));

const policy = {
  schema_version: 1 as const,
  tenant_id: "tenant-1",
  revision: 4,
  enabled: true,
  im: { allow_all: true, rules: [] },
  requests: { tenant_per_minute: 100, user_per_minute: 10 },
  execution: { max_concurrent_runs: 3 },
  tokens: {
    period_seconds: 3600,
    limit: 10000,
    reservation_per_run: 100,
    input_micros_per_million_tokens: 100,
    output_micros_per_million_tokens: 400,
  },
};

beforeEach(() => {
  mocks.getTenant
    .mockReset()
    .mockResolvedValue({ id: "tenant-1", name: "Alpha", role: "OWNER" });
  mocks.getUsagePolicy.mockReset().mockResolvedValue(policy);
  mocks.getUsageSummary.mockReset().mockResolvedValue({
    tenant_id: "tenant-1",
    policy_revision: 4,
    period_seconds: 3600,
    token_limit: 10000,
    used_tokens: 250,
    reserved_tokens: 100,
    unknown_usage_count: 1,
    pending_usage_count: 0,
    estimated_cost_micros: 750000,
  });
  mocks.replaceUsagePolicy
    .mockReset()
    .mockResolvedValue({ ...policy, revision: 5 });
  mocks.replace.mockReset();
});
afterEach(cleanup);

describe("tenant usage governance", () => {
  it("shows known, reserved, unknown and estimated usage", async () => {
    render(<UsageGovernancePage />);
    expect(await screen.findByText("Alpha · 使用治理")).toBeInTheDocument();
    expect(screen.getByText("250")).toBeInTheDocument();
    expect(screen.getByText("100")).toBeInTheDocument();
    expect(screen.getByText(/未知 1，待结算 0/)).toBeInTheDocument();
    expect(screen.getByText(/不是供应商账单/)).toBeInTheDocument();
  });

  it("uses CAS and an idempotency key when saving", async () => {
    render(<UsageGovernancePage />);
    await screen.findByText("Alpha · 使用治理");
    fireEvent.click(screen.getByRole("button", { name: "保存使用策略" }));
    await waitFor(() =>
      expect(mocks.replaceUsagePolicy).toHaveBeenCalledTimes(1),
    );
    const [tenant, revision, submitted, key] =
      mocks.replaceUsagePolicy.mock.calls[0];
    expect(tenant).toBe("tenant-1");
    expect(revision).toBe(4);
    expect(submitted.revision).toBe(0);
    expect(key).toMatch(/^[0-9a-f-]{36}$/);
  });

  it("does not disguise an unavailable usage projection as zero", async () => {
    mocks.getUsageSummary.mockRejectedValueOnce(new Error("offline"));
    render(<UsageGovernancePage />);
    expect(await screen.findByText(/用量投影当前不可用/)).toBeInTheDocument();
  });
});
