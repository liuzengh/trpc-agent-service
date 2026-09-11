import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ProfileDialog } from "./profile-dialog";

afterEach(cleanup);
describe("Profile dialog keyboard behavior", () => {
  it("traps Tab in both directions, locks scroll and restores focus on close", async () => {
    function Harness() { const [open, setOpen] = useState(false); return <><button onClick={() => setOpen(true)}>打开</button>{open && <ProfileDialog title="设置" description="配置说明" onClose={() => setOpen(false)} footer={<button onClick={() => setOpen(false)}>完成</button>}><label>名称<input /></label></ProfileDialog>}</>; }
    const user = userEvent.setup(); render(<Harness />); const opener = screen.getByRole("button", { name: "打开" }); await user.click(opener);
    expect(screen.getByRole("textbox", { name: "名称" })).toHaveFocus(); expect(document.body.style.overflow).toBe("hidden");
    await user.tab({ shift: true }); expect(screen.getByRole("button", { name: "关闭对话框" })).toHaveFocus();
    await user.tab({ shift: true }); expect(screen.getByRole("button", { name: "完成" })).toHaveFocus();
    await user.tab(); expect(screen.getByRole("button", { name: "关闭对话框" })).toHaveFocus();
    await user.keyboard("{Escape}"); expect(screen.queryByRole("dialog")).not.toBeInTheDocument(); expect(opener).toHaveFocus(); expect(document.body.style.overflow).toBe("");
  });
  it("keeps a busy submission open when Escape or close is attempted", async () => {
    const close = vi.fn(); const user = userEvent.setup(); render(<ProfileDialog title="提交" onClose={close} busy><input aria-label="值" /></ProfileDialog>);
    await user.keyboard("{Escape}"); expect(close).not.toHaveBeenCalled(); expect(screen.getByRole("button", { name: "关闭对话框" })).toBeDisabled();
  });
});
