import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ArtifactPanel } from "./artifact-panel";
import { ARTIFACT_MAX_BYTES } from "../../lib/artifact-api";

beforeEach(() => {
  vi.stubGlobal("URL", class extends URL { static createObjectURL = vi.fn(() => "blob:owned-download"); static revokeObjectURL = vi.fn(); });
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });
const props = { tenantId: "t", deploymentId: "d", revisionNumber: 2 };
const tenant = () => Response.json({ id: "t", role: "OWNER" });
const receipt = { name: "report.txt", version: 0, ref: "artifact://opaque", mime_type: "text/plain", size_bytes: 3, sha256: "a".repeat(64) };
async function open() {
  const view = render(<ArtifactPanel {...props} />);
  await waitFor(() => expect(screen.getByLabelText("正式 Run ID")).toBeEnabled());
  fireEvent.change(screen.getByLabelText("正式 Run ID"), { target: { value: "run_1" } });
  return view;
}
function pick(file = new File(["abc"], "report.txt", { type: "text/plain" })) {
  fireEvent.change(screen.getByLabelText("Artifact 上传文件"), { target: { files: [file] } });
  return file;
}
it("uploads, displays real receipt version zero, reads exact version and releases the local download URL", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(tenant()).mockResolvedValueOnce(Response.json(receipt, { status: 201 })).mockResolvedValueOnce(new Response("abc", { headers: { "content-type": "text/plain" } }));
  const view = await open();const file = pick();
  fireEvent.click(screen.getByRole("button", { name: "上传新版本" }));
  await screen.findByText("已保存版本 0");
  expect(fetcher.mock.calls[1][1]).toMatchObject({ method: "PUT", body: file });
  expect(screen.getByLabelText("Artifact 读取版本")).toHaveValue(0);
  expect(screen.getByText("artifact://opaque")).not.toHaveAttribute("href");
  fireEvent.click(screen.getByRole("button", { name: "读取指定版本" }));
  expect(await screen.findByRole("link", { name: "下载已读取文件" })).toHaveAttribute("download", "report.txt");
  expect(fetcher.mock.calls[2][0]).toBe("/api/control/v1/tenants/t/deployments/d/revisions/2/artifacts/report.txt?run_id=run_1&version=0");
  expect(screen.getByText("abc")).toBeInTheDocument();
  expect(URL.createObjectURL).toHaveBeenCalledTimes(1);
  view.unmount();expect(URL.revokeObjectURL).toHaveBeenCalledExactlyOnceWith("blob:owned-download");
});
it("never automatically resubmits a PUT after a lost response", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(tenant()).mockRejectedValueOnce(new TypeError("secret-detail"));
  await open();pick();fireEvent.click(screen.getByRole("button", { name: "上传新版本" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("上传结果尚未确认");
  expect(fetcher).toHaveBeenCalledTimes(2);
  expect(screen.queryByText(/已保存版本/)).toBeNull();
  expect(document.body.textContent).not.toContain("secret-detail");
});
it("keeps Artifact operations unavailable to a MEMBER", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ id: "t", role: "MEMBER" }));
  render(<ArtifactPanel {...props} />);
  await screen.findByText(/需要当前租户 OWNER/);
  expect(screen.getByRole("button", { name: "上传新版本" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "读取指定版本" })).toBeDisabled();
  expect(fetcher).toHaveBeenCalledTimes(1);
});
it("rejects path-like names and explicit transport overflow before upload", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(tenant());await open();pick();
  fireEvent.change(screen.getByLabelText("Artifact 文件名"), { target: { value: "../report.txt" } });
  expect(screen.getByRole("button", { name: "上传新版本" })).toBeDisabled();
  const file = new File(["abc"], "report.txt");Object.defineProperty(file, "size", { value: ARTIFACT_MAX_BYTES + 1 });pick(file);
  expect(screen.getByRole("alert")).toHaveTextContent("16 MiB");
  expect(screen.getByRole("button", { name: "上传新版本" })).toBeDisabled();
  expect(fetcher).toHaveBeenCalledTimes(1);
});
it("renders HTML-like contents as text, never as markup, and clears previous scope results", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(tenant()).mockResolvedValueOnce(new Response("<img src=x onerror=alert(1)>", { headers: { "content-type": "text/html" } }));
  await open();fireEvent.change(screen.getByLabelText("Artifact 文件名"), { target: { value: "report.html" } });
  fireEvent.change(screen.getByLabelText("Artifact 读取版本"), { target: { value: "0" } });
  fireEvent.click(screen.getByRole("button", { name: "读取指定版本" }));
  await screen.findByRole("link", { name: "下载已读取文件" });
  expect(document.querySelector("img")).toBeNull();expect(screen.getByText("<img src=x onerror=alert(1)>")).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("正式 Run ID"), { target: { value: "run_2" } });
  expect(screen.queryByRole("link", { name: "下载已读取文件" })).toBeNull();
  expect(URL.revokeObjectURL).toHaveBeenCalledExactlyOnceWith("blob:owned-download");
});
it("aborts an outstanding Artifact read when the fixed revision panel unmounts", async () => {
  let signal: AbortSignal | undefined;
  vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(tenant()).mockImplementationOnce((_url, init) => new Promise((_resolve, reject) => {
    signal = init?.signal ?? undefined;signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
  }));
  const view = await open();fireEvent.change(screen.getByLabelText("Artifact 文件名"), { target: { value: "report.txt" } });
  fireEvent.change(screen.getByLabelText("Artifact 读取版本"), { target: { value: "0" } });
  fireEvent.click(screen.getByRole("button", { name: "读取指定版本" }));
  await waitFor(() => expect(signal).toBeDefined());view.unmount();expect(signal?.aborted).toBe(true);
  expect(URL.createObjectURL).not.toHaveBeenCalled();
});
