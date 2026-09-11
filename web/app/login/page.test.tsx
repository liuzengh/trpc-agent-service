import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import LoginPage from "./page";
import ChangePasswordPage from "../change-password/page";
const state = vi.hoisted(() => ({ replace: vi.fn(), login: vi.fn(), changePassword: vi.fn(), next: null as string | null }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ replace: state.replace }), useSearchParams: () => ({ get: () => state.next }) }));
vi.mock("../../lib/control-api", () => ({ controlApi: state, ControlApiError: class extends Error {} }));
beforeEach(() => { vi.clearAllMocks(); state.next = null; state.login.mockResolvedValue({ password_change_required: false }); state.changePassword.mockResolvedValue({}); });
afterEach(cleanup);
function submitLogin() {
  fireEvent.change(screen.getByLabelText("用户名"), { target: { value: "example" } });
  fireEvent.change(screen.getByLabelText("密码"), { target: { value: "example-password" } });
  fireEvent.click(screen.getByRole("button", { name: "登录" }));
}
describe("post-authentication destinations", () => {
  it("keeps accessible credentials and a separate documentation entry in the redesigned login", () => {
    render(<LoginPage />);
    expect(screen.getByRole("heading", { level: 1, name: "登录控制台" })).toBeVisible();
    expect(screen.getByLabelText("用户名")).toHaveAttribute("autocomplete", "username");
    expect(screen.getByLabelText("密码")).toHaveAttribute("type", "password");
    expect(screen.getByRole("link", { name: /帮助文档/ })).toHaveAttribute("target", "_blank");
    expect(screen.getByText("Tools")).toHaveAttribute("data-resource-category", "tools");
  });
  it("shows authentication failures without losing the entered username", async () => {
    state.login.mockRejectedValueOnce(new Error("登录失败，请重试"));
    render(<LoginPage />); submitLogin();
    expect(await screen.findByRole("alert")).toHaveTextContent("登录失败，请重试");
    expect(screen.getByLabelText("用户名")).toHaveValue("example");
    expect(screen.getByRole("button", { name: "登录" })).toBeEnabled();
  });
  it.each([null, "https://example.org", "//example.org"])("uses the console entry for next=%s", async (next) => {
    state.next = next; render(<LoginPage />); submitLogin();
    await waitFor(() => expect(state.replace).toHaveBeenCalledWith("/console"));
  });
  it("preserves an explicit in-app destination", async () => {
    state.next = "/tenants/team-a/channels"; render(<LoginPage />); submitLogin();
    await waitFor(() => expect(state.replace).toHaveBeenCalledWith(state.next));
  });
  it("requires initial password change before console entry", async () => {
    state.login.mockResolvedValue({ password_change_required: true }); render(<LoginPage />); submitLogin();
    await waitFor(() => expect(state.replace).toHaveBeenCalledWith("/change-password"));
  });
  it("continues to the console after changing the password", async () => {
    render(<ChangePasswordPage />);
    for (const label of ["当前密码", "新密码", "确认新密码"]) fireEvent.change(screen.getByLabelText(new RegExp(`^${label}`)), { target: { value: "example-password" } });
    fireEvent.click(screen.getByRole("button", { name: "保存并继续" }));
    await waitFor(() => expect(state.replace).toHaveBeenCalledWith("/console"));
  });
});
