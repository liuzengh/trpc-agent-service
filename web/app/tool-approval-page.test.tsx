import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import ToolApprovalsPage from "./tenants/[tenantId]/approvals/page";

const mocks = vi.hoisted(() => ({ listToolApprovals: vi.fn(), decideToolApproval: vi.fn(), getTenant: vi.fn() }));
vi.mock("next/navigation", () => ({ useParams: () => ({ tenantId: "tenant-1" }) }));
vi.mock("../lib/control-api", () => ({ controlApi: mocks }));

afterEach(cleanup);
beforeEach(() => {
  mocks.getTenant.mockReset().mockResolvedValue({ id: "tenant-1", role: "OWNER" });
  mocks.listToolApprovals.mockReset().mockResolvedValue({ operations: [{ operation_id: "tap_1", tenant_id: "tenant-1", run_id: "run-1", attempt_id: "attempt-1", node_id: "assistant", tool_name: "mcp_ticket", tool_resource: "ticket", capability: "test.ticket.status.update", target: "TEST-42", parameter_summary: "将测试工单 TEST-42 的状态修改为 resolved", arguments_digest: `sha256:${"a".repeat(64)}`, status: "PENDING", requested_at: "2026-09-10T00:00:00Z", expires_at: "2026-09-10T00:05:00Z" }], offset: 0, limit: 50, total: 1 });
  mocks.decideToolApproval.mockReset().mockResolvedValue({ outcome: "DECIDED" });
});

describe("tool approval page", () => {
  it("shows exact target and submits an owner decision with the displayed digest", async () => {
    render(<ToolApprovalsPage />);
    expect(await screen.findByText("TEST-42")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "批准" }));
    expect(screen.getByRole("dialog", { name: "确认批准此操作" })).toHaveTextContent("将测试工单 TEST-42 的状态修改为 resolved");
    fireEvent.change(screen.getByLabelText("决定说明（可选）"), { target: { value: "confirmed target" } });
    fireEvent.click(screen.getByRole("button", { name: "批准并允许执行" }));
    await waitFor(() => expect(mocks.decideToolApproval).toHaveBeenCalledWith("tenant-1", "tap_1", { action: "approve", reason: "confirmed target", expected_arguments_digest: `sha256:${"a".repeat(64)}` }));
  });
  it("lets members inspect but not decide", async () => {
    mocks.getTenant.mockResolvedValueOnce({ id: "tenant-1", role: "MEMBER" });
    render(<ToolApprovalsPage />);
    expect(await screen.findByText("仅 OWNER 可决定")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "批准" })).not.toBeInTheDocument();
  });
});
