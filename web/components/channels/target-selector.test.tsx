import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { DeploymentPage, DeploymentRevision, DeploymentRevisionPage } from "../../lib/deployment-api";
import { deployment, revision } from "../../test/deployment-fixtures";
import { ChannelTargetSelector } from "./target-selector";

const api = vi.hoisted(() => ({ list: vi.fn(), listRevisions: vi.fn(), getRevision: vi.fn() }));
vi.mock("../../lib/deployment-api", async (original) => ({ ...await original<typeof import("../../lib/deployment-api")>(), deploymentApi: api }));
const depPage = (offset = 0, total = 1): DeploymentPage => ({ deployments: [{ ...deployment, latest_revision_number: 9 }], offset, limit: 50, total });
const revPage = (offset = 0, total = 1): DeploymentRevisionPage => ({ revisions: [revision], offset, limit: 50, total });
const target = { deployment_id: "d", revision_number: 1 };
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>((done) => { resolve = done; }); return { promise, resolve }; }

beforeEach(() => {
  vi.resetAllMocks();
  api.list.mockResolvedValue(depPage()); api.listRevisions.mockResolvedValue(revPage()); api.getRevision.mockResolvedValue(revision);
});
afterEach(() => cleanup());

describe("Channel fixed Deployment target selector", () => {
  it("loads an exact rN even when latest and the listed revisions are newer", async () => {
    api.listRevisions.mockResolvedValue({ ...revPage(), revisions: [{ ...revision, revision_number: 9 }] });
    const change = vi.fn(); render(<ChannelTargetSelector tenantId="t" initialTarget={target} disabled={false} onChange={change} />);
    await screen.findByText("待保存目标 · r1");
    expect(api.getRevision).toHaveBeenCalledExactlyOnceWith("t", "d", 1);
    expect(screen.getByRole("combobox", { name: "渠道固定部署版本" })).toHaveValue("1");
    expect(screen.getByRole("option", { name: "r1（指定版本）" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "查看部署快照" })).toHaveAttribute("href", "/tenants/t/deployments/d/revisions/1");
    expect(change).toHaveBeenLastCalledWith(target, revision);
  });

  it.each(["versions", "deployments"])("keeps the verified target and preview while paging %s", async (kind) => {
    const next = deferred<DeploymentPage | DeploymentRevisionPage>();
    if (kind === "versions") api.listRevisions.mockResolvedValueOnce(revPage(0, 51)).mockReturnValueOnce(next.promise);
    else api.list.mockResolvedValueOnce(depPage(0, 51)).mockReturnValueOnce(next.promise);
    const change = vi.fn(); render(<ChannelTargetSelector tenantId="t" initialTarget={target} disabled={false} onChange={change} />);
    await screen.findByText("待保存目标 · r1"); const count = change.mock.calls.length;
    fireEvent.click(screen.getByRole("button", { name: kind === "versions" ? "加载更多版本" : "加载更多部署" }));
    expect(screen.getByText("待保存目标 · r1")).toBeInTheDocument(); expect(change).toHaveBeenCalledTimes(count);
    await act(async () => next.resolve(kind === "versions" ? { ...revPage(50, 51), revisions: [{ ...revision, revision_number: 2 }] } : { ...depPage(50, 51), deployments: [{ ...deployment, id: "other", name: "其他部署", latest_revision_number: 1 }] }));
    expect(screen.getByText("待保存目标 · r1")).toBeInTheDocument(); expect(api.getRevision).toHaveBeenCalledTimes(1);
    expect(change).toHaveBeenCalledTimes(count); expect(screen.getByRole("combobox", { name: "渠道固定部署版本" })).toHaveValue("1");
  });

  it("clears a changed deployment and ignores its late snapshot response", async () => {
    const old = deferred<DeploymentRevision>(); const change = vi.fn();
    api.list.mockResolvedValue({ ...depPage(), deployments: [...depPage().deployments, { ...deployment, id: "b", name: "新部署", latest_revision_number: 2 }] });
    api.listRevisions.mockImplementation((_tenant, id) => Promise.resolve(id === "b" ? { ...revPage(), revisions: [{ ...revision, deployment_id: "b", revision_number: 2 }] } : revPage()));
    api.getRevision.mockReturnValueOnce(old.promise).mockResolvedValueOnce({ ...revision, deployment_id: "b", revision_number: 2 });
    render(<ChannelTargetSelector tenantId="t" initialTarget={target} disabled={false} onChange={change} />);
    await waitFor(() => expect(screen.getByRole("combobox", { name: "渠道运行部署" })).toBeEnabled());
    fireEvent.change(screen.getByRole("combobox", { name: "渠道运行部署" }), { target: { value: "b" } });
    expect(change).toHaveBeenLastCalledWith(null, null);
    expect(screen.queryByText("待保存目标 · r1")).not.toBeInTheDocument(); expect(screen.queryByRole("status")).not.toBeInTheDocument();
    await screen.findByRole("option", { name: /r2 · Agent/ });
    fireEvent.change(screen.getByRole("combobox", { name: "渠道固定部署版本" }), { target: { value: "2" } });
    await screen.findByText("待保存目标 · r2");
    await act(async () => old.resolve(revision));
    expect(screen.queryByText("待保存目标 · r1")).not.toBeInTheDocument();
    expect(change).toHaveBeenLastCalledWith({ deployment_id: "b", revision_number: 2 }, expect.objectContaining({ deployment_id: "b", revision_number: 2 }));
  });

  it("ignores a late revision list for a previously selected deployment", async () => {
    const old = deferred<DeploymentRevisionPage>();
    api.list.mockResolvedValue({ ...depPage(), deployments: [...depPage().deployments, { ...deployment, id: "b", name: "新部署", latest_revision_number: 2 }] });
    api.listRevisions.mockReturnValueOnce(old.promise).mockResolvedValueOnce({ ...revPage(), revisions: [{ ...revision, deployment_id: "b", revision_number: 2 }] });
    render(<ChannelTargetSelector tenantId="t" initialTarget={target} disabled={false} onChange={vi.fn()} />);
    await waitFor(() => expect(screen.getByRole("combobox", { name: "渠道运行部署" })).toBeEnabled());
    fireEvent.change(screen.getByRole("combobox", { name: "渠道运行部署" }), { target: { value: "b" } });
    await screen.findByRole("option", { name: /r2 · Agent/ }); await act(async () => old.resolve(revPage()));
    expect(screen.queryByRole("option", { name: /r1 · Agent/ })).not.toBeInTheDocument();
    expect(screen.getByRole("option", { name: /r2 · Agent/ })).toBeInTheDocument();
  });

  it("removes snapshot loading when a pending target is explicitly cleared", async () => {
    api.getRevision.mockReturnValue(new Promise(() => {}));
    render(<ChannelTargetSelector tenantId="t" initialTarget={target} disabled={false} onChange={vi.fn()} />);
    await waitFor(() => expect(screen.getByRole("combobox", { name: "渠道运行部署" })).toBeEnabled());
    expect(screen.getByRole("status")).toBeInTheDocument();
    fireEvent.change(screen.getByRole("combobox", { name: "渠道运行部署" }), { target: { value: "" } });
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "渠道固定部署版本" })).toBeDisabled();
  });

  it.each(["versions", "deployments"])("does not skip an unread page after %s pagination fails", async (kind) => {
    if (kind === "versions") api.listRevisions.mockResolvedValueOnce(revPage(0, 101)).mockRejectedValueOnce(new Error("page unavailable")).mockResolvedValue(revPage(50, 101));
    else api.list.mockResolvedValueOnce(depPage(0, 101)).mockRejectedValueOnce(new Error("page unavailable")).mockResolvedValue(depPage(50, 101));
    render(<ChannelTargetSelector tenantId="t" initialTarget={target} disabled={false} onChange={vi.fn()} />);
    await screen.findByText("待保存目标 · r1");
    const more = screen.getByRole("button", { name: kind === "versions" ? "加载更多版本" : "加载更多部署" });
    fireEvent.click(more); await screen.findByRole("alert");
    expect(more).toBeDisabled();
    expect(screen.getByText("待保存目标 · r1")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "重试读取目标" }));
    if (kind === "versions") await waitFor(() => expect(api.listRevisions).toHaveBeenLastCalledWith("t", "d", 50, 50));
    else await waitFor(() => expect(api.list).toHaveBeenLastCalledWith("t", 50, 50));
  });

  it("does not select latest or fetch a snapshot just because deployments exist", async () => {
    render(<ChannelTargetSelector tenantId="t" disabled={false} onChange={vi.fn()} />);
    await waitFor(() => expect(screen.getByRole("combobox", { name: "渠道运行部署" })).toBeEnabled());
    expect(api.getRevision).not.toHaveBeenCalled(); expect(screen.queryByText(/待保存目标/)).not.toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "渠道运行部署" })).toHaveValue("");
  });

  it("does not clear a failed deployment-page guard when a different revision is selected", async () => {
    api.list.mockResolvedValueOnce(depPage(0, 101)).mockRejectedValueOnce(new Error("page unavailable"));
    api.listRevisions.mockResolvedValue({ ...revPage(), revisions: [revision, { ...revision, revision_number: 2 }] });
    api.getRevision.mockResolvedValueOnce(revision).mockResolvedValueOnce({ ...revision, revision_number: 2 });
    render(<ChannelTargetSelector tenantId="t" initialTarget={target} disabled={false} onChange={vi.fn()} />);
    await screen.findByText("待保存目标 · r1");
    fireEvent.click(screen.getByRole("button", { name: "加载更多部署" })); await screen.findByRole("alert");
    fireEvent.change(screen.getByRole("combobox", { name: "渠道固定部署版本" }), { target: { value: "2" } });
    await screen.findByText("待保存目标 · r2");
    expect(screen.getByRole("button", { name: "加载更多部署" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "重试读取目标" })).toBeInTheDocument();
  });
});
