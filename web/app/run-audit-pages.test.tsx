import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import AuditPage from "./tenants/[tenantId]/audit/page";
import RunDetailPage from "./tenants/[tenantId]/runs/[runId]/page";
import RunsPage from "./tenants/[tenantId]/runs/page";

const mocks = vi.hoisted(() => ({ listRuns: vi.fn(), getRun: vi.fn(), listAuditEvents: vi.fn() }));
vi.mock("next/navigation", () => ({ useParams: () => ({ tenantId: "tenant-1", runId: "run-1" }) }));
vi.mock("../lib/control-api", () => ({ controlApi: mocks }));

afterEach(cleanup);

beforeEach(() => {
  mocks.listRuns.mockReset().mockResolvedValue({ runs: [{ run_id: "run-1", session_id: "session-1", status: "RUNNING", stage: "EXECUTION", attempts: 1, usage_status: "PARTIAL", input_tokens: 12, output_tokens: 4, total_tokens: 16, reply_status: "NOT_CREATED", accepted_at: "2026-09-09T00:00:00Z" }], offset: 0, limit: 25, total: 1 });
  mocks.getRun.mockReset().mockResolvedValue({ run_id: "run-1", session_id: "session-1", status: "FAILED", stage: "FAILED", attempts: 1, usage_status: "PARTIAL", input_tokens: 12, output_tokens: 4, total_tokens: 16, reply_status: "NOT_CREATED", accepted_at: "2026-09-09T00:00:00Z", admission_id: "admission-1", attempt_log: [], timeline: [{ source: "worker", category: "ATTEMPT_ENDED", status: "FAILED", reason: "MODEL_UNAVAILABLE", occurred_at: "2026-09-09T00:01:00Z" }], coverage: ["worker.run", "worker.attempt"] });
  mocks.listAuditEvents.mockReset().mockResolvedValue({ events: [{ event_id: "deployment:1", source: "control", category: "CONFIG", action: "PUBLISH", outcome: "SUCCEEDED", actor_id: "user-1", resource_type: "deployment_revision", resource_id: "revision-1", occurred_at: "2026-09-09T00:00:00Z" }], offset: 0, limit: 25, total: 1 });
});

describe("run management pages", () => {
  it("shows current business stage and opens a Run", async () => {
    render(<RunsPage />);
    expect(await screen.findByText("run-1")).toBeInTheDocument();
    expect(screen.getByText("EXECUTION")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /查看过程/ })).toHaveAttribute("href", "/tenants/tenant-1/runs/run-1");
  });
  it("explains a persisted failure without claiming provider delivery", async () => {
    render(<RunDetailPage />);
    expect(await screen.findByText("MODEL_UNAVAILABLE")).toBeInTheDocument();
    expect(screen.getByText(/不等于渠道已送达/)).toBeInTheDocument();
  });
  it("shows source and actor for business audit", async () => {
    render(<AuditPage />);
    await waitFor(() => expect(screen.getByText("revision-1")).toBeInTheDocument());
    expect(screen.getByText("control")).toBeInTheDocument();
    expect(screen.getByText("user-1")).toBeInTheDocument();
  });
  it("shows unavailable usage as not collected rather than zero", async () => {
    mocks.listRuns.mockResolvedValueOnce({ runs: [{ run_id: "run-no-usage", session_id: "session-1", status: "SUCCEEDED", stage: "COMPLETED", attempts: 1, usage_status: "UNAVAILABLE", input_tokens: 0, output_tokens: 0, total_tokens: 0, reply_status: "HANDED_OFF", accepted_at: "2026-09-09T00:00:00Z" }], offset: 0, limit: 25, total: 1 });
    render(<RunsPage />);
    expect(await screen.findByText("未采集")).toBeInTheDocument();
  });
  it("keeps the last successful run page number when the next request fails", async () => {
    mocks.listRuns.mockResolvedValueOnce({ runs: [{ run_id: "run-first-page", session_id: "session-1", status: "RUNNING", stage: "EXECUTION", attempts: 1, usage_status: "UNAVAILABLE", input_tokens: 0, output_tokens: 0, total_tokens: 0, reply_status: "NOT_CREATED", accepted_at: "2026-09-09T00:00:00Z" }], offset: 0, limit: 25, total: 50 });
    render(<RunsPage />);
    expect(await screen.findByText("run-first-page")).toBeInTheDocument();
    mocks.listRuns.mockRejectedValueOnce(new Error("next page failed"));
    fireEvent.click(screen.getByRole("button", { name: /下一页/ }));
    await waitFor(() => expect(mocks.listRuns).toHaveBeenCalledTimes(2));
    expect(await screen.findByRole("alert")).toHaveTextContent("next page failed");
    expect(await screen.findByText("run-first-page")).toBeInTheDocument();
    expect(screen.getByText(/第 1–25 条/)).toBeInTheDocument();
    expect(screen.queryByText(/第 26–50 条/)).not.toBeInTheDocument();
  });
  it("keeps the last successful audit page number when the next request fails", async () => {
    mocks.listAuditEvents.mockResolvedValueOnce({ events: [{ event_id: "first", source: "control", category: "CONFIG", action: "CREATE", outcome: "SUCCEEDED", resource_type: "agent", resource_id: "agent-first-page", occurred_at: "2026-09-09T00:00:00Z" }], offset: 0, limit: 25, total: 50 });
    render(<AuditPage />);
    expect(await screen.findByText("agent-first-page")).toBeInTheDocument();
    mocks.listAuditEvents.mockRejectedValueOnce(new Error("next page failed"));
    fireEvent.click(screen.getByRole("button", { name: /下一页/ }));
    await waitFor(() => expect(mocks.listAuditEvents).toHaveBeenCalledTimes(2));
    expect(await screen.findByRole("alert")).toHaveTextContent("next page failed");
    expect(await screen.findByText("agent-first-page")).toBeInTheDocument();
    expect(screen.getByText(/第 1–25 条/)).toBeInTheDocument();
    expect(screen.queryByText(/第 26–50 条/)).not.toBeInTheDocument();
  });
  it("does not present a failed initial request as an empty result", async () => {
    mocks.listRuns.mockRejectedValueOnce(new Error("initial request failed"));
    render(<RunsPage />);
    await waitFor(() => expect(mocks.listRuns).toHaveBeenCalledTimes(1));
    expect(await screen.findByRole("alert")).toHaveTextContent("initial request failed");
    expect(screen.queryByText("暂无运行记录")).not.toBeInTheDocument();
  });
});
