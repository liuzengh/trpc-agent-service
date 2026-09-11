import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ManagedBackendSelect } from "./managed-backend-select";
const item = { id: "pg", revision: 2, label: "Primary PG", kind: "postgresql", roles: ["memory"], available: true };
afterEach(() => { cleanup(); vi.restoreAllMocks(); });
describe("managed selector (component tests, not live HTTP acceptance)", () => {
  it("requires explicit selection and emits only the id/revision pair", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ items: [item] }));
    const changed = vi.fn(); render(<ManagedBackendSelect tenantId="t" role="memory" value={{}} onChange={changed} />);
    await screen.findByRole("option", { name: /Primary PG/ });expect(changed).not.toHaveBeenCalled();
    fireEvent.change(screen.getByLabelText("平台后端（memory）"), { target: { value: "pg@2" } });
    expect(changed).toHaveBeenCalledWith({ backend_id: "pg", backend_revision: 2 });
  });
  it.each([404, 503])("keeps binding and blocks selection on HTTP %s", async (status) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("", { status }));
    const changed = vi.fn();render(<ManagedBackendSelect tenantId="t" role="memory" value={{ backend_id: "old", backend_revision: 1 }} onChange={changed} />);
    await screen.findByRole("alert");expect(screen.getByText("old")).toBeInTheDocument();
    expect(screen.getByLabelText("平台后端（memory）")).toBeDisabled();expect(changed).not.toHaveBeenCalled();
  });
  it("retains stale revision until explicit reselection and never writes on refresh", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async () => Response.json({ items: [item] }));
    const changed = vi.fn();render(<ManagedBackendSelect tenantId="t" role="memory" value={{ backend_id: "pg", backend_revision: 1 }} onChange={changed} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("修订已变化");
    fireEvent.click(screen.getByText("刷新后端目录"));await screen.findByRole("option", { name: /Primary PG/ });
    expect(changed).not.toHaveBeenCalled();
  });
  it("shows honest empty state for unavailable or wrong-role catalog entries", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ items: [{ ...item, available: false }] }));
    render(<ManagedBackendSelect tenantId="t" role="memory" value={{}} onChange={vi.fn()} />);
    expect(await screen.findByText(/没有支持 memory 的可选后端/)).toBeInTheDocument();
    expect(screen.getByLabelText("平台后端（memory）")).toBeDisabled();
  });
  it("can retry after failure without fallback or automatic selection", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response("", { status: 503 })).mockResolvedValueOnce(Response.json({ items: [item] }));
    const changed = vi.fn();render(<ManagedBackendSelect tenantId="t" role="memory" value={{}} onChange={changed} />);
    await screen.findByRole("alert");fireEvent.click(screen.getByText("刷新后端目录"));
    await screen.findByRole("option", { name: /Primary PG/ });expect(changed).not.toHaveBeenCalled();
  });
  it("does not fetch directory for immutable read-only revision", () => {
    const fetcher = vi.spyOn(globalThis, "fetch");render(<ManagedBackendSelect tenantId="t" role="memory" value={{ backend_id: "pg", backend_revision: 1 }} onChange={vi.fn()} readOnly />);
    expect(fetcher).not.toHaveBeenCalled();expect(screen.queryByRole("combobox")).toBeNull();
  });
  it("does not display prior tenant options after tenant change", async () => {
    let resolveOld!: (response: Response) => void;
    vi.spyOn(globalThis, "fetch").mockImplementationOnce(() => new Promise((resolve) => { resolveOld = resolve; })).mockResolvedValueOnce(Response.json({ items: [] }));
    const changed = vi.fn();const view = render(<ManagedBackendSelect tenantId="old" role="memory" value={{}} onChange={changed} />);
    view.rerender(<ManagedBackendSelect tenantId="new" role="memory" value={{}} onChange={changed} />);
    await screen.findByText(/没有支持 memory 的可选后端/);resolveOld(Response.json({ items: [item] }));
    await waitFor(() => expect(screen.queryByRole("option", { name: /Primary PG/ })).toBeNull());expect(changed).not.toHaveBeenCalled();
  });
});
