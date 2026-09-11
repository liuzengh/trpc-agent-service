import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ControlApiError } from "../../lib/control-api";
import Home from "./page";

const navigation = vi.hoisted(() => ({ replace: vi.fn() }));
const api = vi.hoisted(() => ({
  getMe: vi.fn(),
  getCapabilities: vi.fn(),
  listMyTenants: vi.fn(),
}));

vi.mock("next/navigation", () => ({
  useRouter: () => navigation,
}));

vi.mock("../../lib/control-api", async () => {
  const actual = await vi.importActual<typeof import("../../lib/control-api")>("../../lib/control-api");
  return { ...actual, controlApi: api };
});

beforeEach(() => {
  vi.clearAllMocks();
  api.getMe.mockResolvedValue({
    user: { id: "user-1", username: "alice", display_name: "Alice" },
    password_change_required: false,
  });
  api.getCapabilities.mockResolvedValue({ capabilities: ["users:manage"] });
  api.listMyTenants.mockResolvedValue({ tenants: [] });
});

afterEach(() => {
  vi.useRealTimers();
  cleanup();
});

describe("role-aware console landing", () => {
  it("lands a Platform Operator on Admin", async () => {
    render(<Home />);
    await waitFor(() => expect(navigation.replace).toHaveBeenCalledWith("/admin"));
    expect(api.listMyTenants).not.toHaveBeenCalled();
  });

  it.each(["OWNER", "MEMBER"])("lands a single-Tenant %s on the Tenant chooser", async (role) => {
    api.getCapabilities.mockRejectedValue(new ControlApiError(403, "PLATFORM_FORBIDDEN", "forbidden"));
    api.listMyTenants.mockResolvedValue({ tenants: [{
      id: "tenant/a", slug: "team-a", name: "Team A", status: "ACTIVE", role,
      created_at: "", updated_at: "",
    }] });

    render(<Home />);

    await waitFor(() => expect(navigation.replace).toHaveBeenCalledWith("/tenants"));
    expect(api.listMyTenants).toHaveBeenCalledTimes(1);
  });

  it("uses the Tenant chooser when the user has multiple memberships", async () => {
    api.getCapabilities.mockRejectedValue(new ControlApiError(403, "PLATFORM_FORBIDDEN", "forbidden"));
    api.listMyTenants.mockResolvedValue({ tenants: [
      { id: "tenant-1", role: "OWNER" },
      { id: "tenant-2", role: "MEMBER" },
    ] });

    render(<Home />);

    await waitFor(() => expect(navigation.replace).toHaveBeenCalledWith("/tenants"));
  });

  it("shows an operational error instead of misclassifying a server failure as logout", async () => {
    api.getCapabilities.mockRejectedValue(new ControlApiError(500, "INTERNAL_ERROR", "request failed"));

    render(<Home />);

    expect(await screen.findByText("控制台暂时不可用")).toBeInTheDocument();
    expect(screen.getByText("request failed")).toBeInTheDocument();
    expect(navigation.replace).not.toHaveBeenCalledWith("/login");
  });

  it("stops waiting and exposes retry when session restoration never settles", async () => {
    vi.useFakeTimers();
    api.getMe.mockImplementation(() => new Promise(() => undefined));

    render(<Home />);
    await act(async () => vi.advanceTimersByTimeAsync(10_000));

    expect(screen.getByText("控制台连接超时")).toBeInTheDocument();
    expect(screen.getByText("恢复会话超过 10 秒，请确认 Web 服务与 Control API 均可访问后重试。")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "重试" })).toBeInTheDocument();
  });
});

describe("console authentication entry", () => {
  it("sends an unauthenticated visitor to login", async () => {
    api.getMe.mockRejectedValue(new ControlApiError(401, "UNAUTHORIZED", "login required"));
    render(<Home />);
    await waitFor(() => expect(navigation.replace).toHaveBeenCalledWith("/login"));
  });
  it("preserves the required password change flow", async () => {
    api.getMe.mockResolvedValue({ password_change_required: true });
    render(<Home />);
    await waitFor(() => expect(navigation.replace).toHaveBeenCalledWith("/change-password"));
    expect(api.getCapabilities).not.toHaveBeenCalled();
  });
});
