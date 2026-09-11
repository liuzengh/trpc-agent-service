import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { KnowledgeImportPanel } from "./knowledge-import-panel";
afterEach(()=>{cleanup();vi.restoreAllMocks();});
const props={tenantId:"t",deploymentId:"d",revisionNumber:2,resources:["docs"]};
const tenant=()=>Response.json({id:"t",role:"OWNER"});
async function open(){const view=render(<KnowledgeImportPanel {...props}/>);await waitFor(()=>expect(screen.getByLabelText("Knowledge 文档名")).toBeEnabled());return view;}
function text(){fireEvent.change(screen.getByLabelText("Knowledge 文档名"),{target:{value:"note.txt"}});fireEvent.change(screen.getByLabelText("Knowledge 文本内容"),{target:{value:"knowledge canary"}});}
it("imports explicit text without Run/session/path fields and clears receipt on edit",async()=>{
 const f=vi.spyOn(globalThis,"fetch").mockResolvedValueOnce(tenant()).mockResolvedValueOnce(Response.json({documents:2}));await open();text();fireEvent.click(screen.getByRole("button",{name:"同步导入文本"}));expect(await screen.findByText(/已导入 2 个 documents/)).toBeInTheDocument();expect(JSON.parse(f.mock.calls[1][1]?.body as string)).toEqual({name:"note.txt",text:"knowledge canary"});expect(screen.queryByLabelText("正式 Run ID")).toBeNull();fireEvent.change(screen.getByLabelText("Knowledge 文本内容"),{target:{value:"changed"}});expect(screen.queryByText(/已导入/)).toBeNull();
});
it("loads actual local UTF8 text and rejects PDF without forwarding it",async()=>{
 const f=vi.spyOn(globalThis,"fetch").mockResolvedValueOnce(tenant());await open();fireEvent.change(screen.getByLabelText("Knowledge 文本文件"),{target:{files:[new File(["hello 世界"],"text.txt",{type:"text/plain"})]}});await waitFor(()=>expect(screen.getByLabelText("Knowledge 文本内容")).toHaveValue("hello 世界"));expect(screen.getByLabelText("Knowledge 文档名")).toHaveValue("text.txt");fireEvent.change(screen.getByLabelText("Knowledge 文本文件"),{target:{files:[new File(["%PDF"],"data.pdf",{type:"application/pdf"})]}});expect(screen.getByRole("alert")).toHaveTextContent("不解析 PDF");expect(screen.getByRole("button",{name:"同步导入文本"})).toBeDisabled();expect(f).toHaveBeenCalledTimes(1);
});
it("does not turn a failed or lost write response into success and never retries",async()=>{
 const f=vi.spyOn(globalThis,"fetch").mockResolvedValueOnce(tenant()).mockResolvedValueOnce(Response.json({code:"KNOWLEDGE_UNAVAILABLE"},{status:503}));await open();text();fireEvent.click(screen.getByRole("button",{name:"同步导入文本"}));expect(await screen.findByRole("alert")).toHaveTextContent(/部分.*已写入/);expect(f).toHaveBeenCalledTimes(2);expect(screen.queryByText(/已导入/)).toBeNull();
});
it("keeps imports OWNER-only",async()=>{
 const f=vi.spyOn(globalThis,"fetch").mockResolvedValue(Response.json({id:"t",role:"MEMBER"}));render(<KnowledgeImportPanel {...props}/>);await screen.findByText(/需要当前租户 OWNER/);expect(screen.getByRole("button",{name:"同步导入文本"})).toBeDisabled();expect(f).toHaveBeenCalledTimes(1);
});
it("aborts an in-flight import on revision unmount",async()=>{
 let signal:AbortSignal|undefined;vi.spyOn(globalThis,"fetch").mockResolvedValueOnce(tenant()).mockImplementationOnce((_u,i)=>new Promise((_r,reject)=>{signal=i?.signal??undefined;signal?.addEventListener("abort",()=>reject(new Error("stop")));}));const v=await open();text();fireEvent.click(screen.getByRole("button",{name:"同步导入文本"}));await waitFor(()=>expect(signal).toBeDefined());v.unmount();expect(signal?.aborted).toBe(true);
});
