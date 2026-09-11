import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AppShell } from "./app-shell";

const navigation = vi.hoisted(() => ({ pathname: "/admin/users" }));
const access = vi.hoisted(() => ({ role: "MEMBER" as "OWNER" | "MEMBER" }));
const api = vi.hoisted(() => ({ getTenant: vi.fn(), logout: vi.fn() }));

vi.mock("../lib/control-api", () => ({ controlApi: api }));

vi.mock("next/navigation", () => ({
  usePathname: () => navigation.pathname,
  useRouter: () => ({ replace: vi.fn(), refresh: vi.fn() }),
}));

afterEach(() => cleanup());

describe("dashboard-01 application shell", () => {
  beforeEach(() => {
    sessionStorage.clear();
    api.logout.mockResolvedValue(undefined);
    navigation.pathname = "/admin/users";
    access.role = "MEMBER";
    api.getTenant.mockResolvedValue({
      id: "tenant-1", slug: "team-a", name: "Team A", status: "ACTIVE",
      role: access.role, created_at: "", updated_at: "",
    });
  });

  it.each(["/admin/users", "/tenants", "/tenants/tenant-1/channels"])("keeps help available without replacing the active page at %s", (pathname) => {
    navigation.pathname = pathname;
    render(<AppShell user={{ id: "user-1", username: "member", display_name: "Member" }}>content</AppShell>);
    const help = screen.getByRole("link", { name: "帮助文档（新标签页打开）" });
    expect(help).toHaveAttribute("href", "/help");
    expect(help).toHaveAttribute("target", "_blank");
    expect(help).toHaveAttribute("rel", "noopener noreferrer");
  });

  it("shows only navigation backed by the currently implemented APIs", () => {
    render(
      <AppShell
        user={{ id: "user-1", username: "admin", display_name: "Platform Admin" }}
        capabilities={["users:manage", "operators:manage", "tenants:manage"]}
      >
        <div>page content</div>
      </AppShell>,
    );

    expect(screen.getByRole("link", { name: "平台用户" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "平台管理员" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "租户" })).toBeInTheDocument();
    expect(screen.queryByText("Agent 应用")).not.toBeInTheDocument();
    expect(screen.queryByText("部署")).not.toBeInTheDocument();
  });

  it("labels the authenticated session without claiming runtime or provider health", () => {
    render(<AppShell user={{ id: "user-1", username: "owner", display_name: "Owner" }}>content</AppShell>);
    expect(screen.getByText("已登录")).toBeInTheDocument();
    expect(screen.queryByText("服务正常")).not.toBeInTheDocument();
  });

  it("shows Agent authoring but not member management to a Tenant MEMBER", async () => {
    navigation.pathname = "/tenants/tenant-1/agents/agent-1";
    render(
      <AppShell user={{ id: "user-1", username: "member", display_name: "Tenant Member" }}>
        <div>agent content</div>
      </AppShell>,
    );

    expect(screen.getByRole("link", { name: "Agent 工作台" })).toHaveAttribute(
      "href",
      "/tenants/tenant-1/agents",
    );
    expect(await screen.findByRole("link", { name: "切换租户" })).toHaveAttribute("href", "/tenants");
    expect(screen.getByRole("link", { name: "部署" })).toHaveAttribute("href", "/tenants/tenant-1/deployments");
    expect(screen.getByRole("link", { name: "运行配置" })).toHaveAttribute("href", "/tenants/tenant-1/runtime-profiles");
    expect(screen.getByRole("link", { name: "渠道接入" })).toHaveAttribute("href", "/tenants/tenant-1/channels");
    expect(screen.queryByRole("link", { name: "成员管理" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "平台用户" })).not.toBeInTheDocument();
  });

  it("labels the non-operator tenant selection chrome as a tenant space", () => {
    navigation.pathname = "/tenants";
    render(
      <AppShell user={{ id: "user-1", username: "member", display_name: "Tenant Member" }}>
        <div>tenant selection</div>
      </AppShell>,
    );

    expect(screen.getByRole("navigation", { name: "租户空间" })).toBeInTheDocument();
    expect(screen.getByText("租户空间")).toBeInTheDocument();
    expect(screen.queryByText("平台管理")).not.toBeInTheDocument();
  });

  it("shows member management only after the active Tenant resolves as OWNER", async () => {
    navigation.pathname = "/tenants/tenant-1/agents";
    access.role = "OWNER";
    api.getTenant.mockResolvedValue({
      id: "tenant-1", slug: "team-a", name: "Team A", status: "ACTIVE",
      role: "OWNER", created_at: "", updated_at: "",
    });

    render(
      <AppShell user={{ id: "user-1", username: "owner", display_name: "Tenant Owner" }}>
        <div>agent content</div>
      </AppShell>,
    );

    expect(await screen.findByRole("link", { name: "成员管理" })).toHaveAttribute(
      "href",
      "/tenants/tenant-1/members",
    );
    expect(screen.getByText("Team A")).toBeInTheDocument();
  });
  it("clears local Deployment preparations on logout without deleting unrelated state", async () => {
    render(<AppShell user={{ id: "user-1", username: "owner", display_name: "Owner" }}>content</AppShell>);
    sessionStorage.setItem("deployment-preparation:v1:user-1:tenant-1:d", "{}"); sessionStorage.setItem("unrelated", "keep");
    fireEvent.click(screen.getByRole("button", { name: "退出登录" }));
    await waitFor(() => expect(api.logout).toHaveBeenCalled());
    expect(sessionStorage.getItem("deployment-preparation:v1:user-1:tenant-1:d")).toBeNull(); expect(sessionStorage.getItem("unrelated")).toBe("keep");
  });

  it.each([
    ["/tenants/tenant-1/channels", "渠道接入"],
    ["/tenants/tenant-1/channels/new", "新增渠道"],
    ["/tenants/tenant-1/channels/account-1", "渠道账户详情"],
  ])("marks Channel navigation active and labels %s", async (pathname, label) => {
    navigation.pathname = pathname;
    render(<AppShell user={{ id: "user-1", username: "member", display_name: "Member" }}>channel content</AppShell>);
    expect(screen.getByRole("link", { name: "渠道接入" })).toHaveClass("active");
    expect(document.querySelector(".workspace-header strong")).toHaveTextContent(label);
    expect(screen.queryByRole("link", { name: "成员管理" })).not.toBeInTheDocument();
    await screen.findByText("Team A");
  });

  it("establishes the Channel identity and clears another user's pending commands", () => {
    sessionStorage.setItem("channel-user:v1", "previous-user");
    sessionStorage.setItem("channel-pending:v1:previous-user:tenant-1:account-1", "{}");
    sessionStorage.setItem("unrelated", "keep");
    const { rerender } = render(<AppShell user={{ id: "user-1", username: "owner", display_name: "Owner" }}>content</AppShell>);
    expect(sessionStorage.getItem("channel-user:v1")).toBe("user-1");
    expect(sessionStorage.getItem("channel-pending:v1:previous-user:tenant-1:account-1")).toBeNull();
    sessionStorage.setItem("channel-pending:v1:user-1:tenant-1:account-1", "{}");
    rerender(<AppShell user={{ id: "user-1", username: "owner", display_name: "Updated name" }}>content</AppShell>);
    expect(sessionStorage.getItem("channel-pending:v1:user-1:tenant-1:account-1")).toBe("{}");
    rerender(<AppShell user={{ id: "user-2", username: "other", display_name: "Other" }}>content</AppShell>);
    expect(sessionStorage.getItem("channel-user:v1")).toBe("user-2");
    expect(sessionStorage.getItem("channel-pending:v1:user-1:tenant-1:account-1")).toBeNull();
    expect(sessionStorage.getItem("unrelated")).toBe("keep");
  });

  it("clears Channel pending state before logout even if the logout request fails", async () => {
    api.logout.mockRejectedValueOnce(new Error("network unavailable"));
    render(<AppShell user={{ id: "user-1", username: "owner", display_name: "Owner" }}>content</AppShell>);
    sessionStorage.setItem("channel-pending:v1:user-1:tenant-1:account-1", "{}");
    sessionStorage.setItem("unrelated", "keep");
    fireEvent.click(screen.getByRole("button", { name: "退出登录" }));
    expect(sessionStorage.getItem("channel-pending:v1:user-1:tenant-1:account-1")).toBeNull();
    await waitFor(() => expect(api.logout).toHaveBeenCalled());
    expect(sessionStorage.getItem("unrelated")).toBe("keep");
  });

  it("clears preflight preparations on identity change but keeps a same-user result", () => {
    const { rerender } = render(<AppShell user={{ id: "user-1", username: "owner", display_name: "Owner" }}>content</AppShell>);
    sessionStorage.setItem("channel-preflight:v1:user-1:tenant-1:account-1", "{}");
    sessionStorage.setItem("unrelated", "keep");
    rerender(<AppShell user={{ id: "user-1", username: "owner", display_name: "Updated name" }}>content</AppShell>);
    expect(sessionStorage.getItem("channel-preflight:v1:user-1:tenant-1:account-1")).toBe("{}");
    rerender(<AppShell user={{ id: "user-2", username: "other", display_name: "Other" }}>content</AppShell>);
    expect(sessionStorage.getItem("channel-preflight:v1:user-1:tenant-1:account-1")).toBeNull();
    expect(sessionStorage.getItem("unrelated")).toBe("keep");
  });

  it("clears preflight recovery before a failed logout request", async () => {
    api.logout.mockRejectedValueOnce(new Error("offline"));
    render(<AppShell user={{ id: "user-1", username: "owner", display_name: "Owner" }}>content</AppShell>);
    sessionStorage.setItem("channel-preflight:v1:user-1:tenant-1:account-1", "{}");
    sessionStorage.setItem("unrelated", "keep");
    fireEvent.click(screen.getByRole("button", { name: "退出登录" }));
    expect(sessionStorage.getItem("channel-preflight:v1:user-1:tenant-1:account-1")).toBeNull();
    await waitFor(() => expect(api.logout).toHaveBeenCalled());
    expect(sessionStorage.getItem("unrelated")).toBe("keep");
  });

});
