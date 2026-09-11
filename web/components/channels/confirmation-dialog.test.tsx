import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChannelDialog } from "./confirmation-dialog";

afterEach(() => cleanup());
const props = () => ({ title: "确认停用接入", busy: false, confirmLabel: "确认停用", onConfirm: vi.fn(), onClose: vi.fn() });

describe("Channel confirmation dialog accessibility", () => {
  it("names the modal and initially focuses its container rather than a destructive action", () => {
    render(<ChannelDialog {...props()}><p>停用保留路由意图。</p></ChannelDialog>);
    const dialog = screen.getByRole("dialog", { name: "确认停用接入" });
    expect(dialog).toHaveAttribute("aria-modal", "true"); expect(dialog).toHaveFocus();
    expect(screen.getByRole("button", { name: "确认停用" })).not.toHaveFocus();
  });
  it("traps Tab and Shift+Tab inside enabled dialog controls", async () => {
    const user = userEvent.setup();
    render(<><button>背景按钮</button><ChannelDialog {...props()}><label><input type="checkbox" />我已确认影响</label></ChannelDialog></>);
    await user.tab(); expect(screen.getByRole("checkbox")).toHaveFocus();
    await user.tab(); expect(screen.getByRole("button", { name: "取消" })).toHaveFocus();
    await user.tab(); expect(screen.getByRole("button", { name: "确认停用" })).toHaveFocus();
    await user.tab(); expect(screen.getByRole("checkbox")).toHaveFocus();
    await user.tab({ shift: true }); expect(screen.getByRole("button", { name: "确认停用" })).toHaveFocus();
    expect(screen.getByRole("button", { name: "背景按钮" })).not.toHaveFocus();
  });
  it("skips a disabled confirm button and blocks its click", async () => {
    const user = userEvent.setup(); const options = props();
    render(<ChannelDialog {...options} disabled><p>请先核对影响。</p></ChannelDialog>);
    await user.tab(); expect(screen.getByRole("button", { name: "取消" })).toHaveFocus();
    await user.tab(); expect(screen.getByRole("button", { name: "取消" })).toHaveFocus();
    fireEvent.click(screen.getByRole("button", { name: "确认停用" })); expect(options.onConfirm).not.toHaveBeenCalled();
  });
  it("keeps focus inside and ignores Escape while submitting", async () => {
    const user = userEvent.setup(); const options = props();
    render(<><button>背景按钮</button><ChannelDialog {...options} busy><p>提交中。</p></ChannelDialog></>);
    await user.tab(); expect(screen.getByRole("dialog")).toHaveFocus();
    await user.tab({ shift: true }); expect(screen.getByRole("dialog")).toHaveFocus();
    await user.keyboard("{Escape}"); expect(options.onClose).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "取消" })).toBeDisabled(); expect(screen.getByRole("button", { name: "正在提交…" })).toBeDisabled();
  });
  it("uses the current Escape handler after busy and callbacks change", async () => {
    const user = userEvent.setup(); const first = props(); const close = vi.fn();
    const view = render(<ChannelDialog {...first} busy>影响说明</ChannelDialog>);
    view.rerender(<ChannelDialog {...first} busy={false} onClose={close}>影响说明</ChannelDialog>);
    await user.keyboard("{Escape}"); expect(close).toHaveBeenCalledOnce(); expect(first.onClose).not.toHaveBeenCalled();
  });
  it("restores the invoking control on close and removes the keyboard handler", () => {
    const options = props(); const view = render(<button>打开确认框</button>);
    const trigger = screen.getByRole("button", { name: "打开确认框" }); trigger.focus();
    view.rerender(<><button>打开确认框</button><ChannelDialog {...options}>影响说明</ChannelDialog></>);
    expect(screen.getByRole("dialog")).toHaveFocus();
    view.rerender(<button>打开确认框</button>);
    expect(screen.getByRole("button", { name: "打开确认框" })).toHaveFocus();
    fireEvent.keyDown(document, { key: "Escape" }); expect(options.onClose).not.toHaveBeenCalled();
  });
});
