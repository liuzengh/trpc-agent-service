import { afterEach, expect, it, vi } from "vitest";
import { knowledgeApi, KNOWLEDGE_MAX_TEXT_BYTES } from "./knowledge-api";
afterEach(()=>vi.restoreAllMocks());
const scope={tenantId:"t/a",deploymentId:"d ?",revisionNumber:2,resource:"docs"};
it("posts exact text and name through the existing authenticated Control path without Session or namespace",async()=>{
 const f=vi.spyOn(globalThis,"fetch").mockResolvedValue(Response.json({documents:2}));expect(await knowledgeApi.importText(scope,"note.txt","hello 世界")).toEqual({documents:2});
 expect(f).toHaveBeenCalledTimes(1);expect(f.mock.calls[0][0]).toBe("/api/control/v1/tenants/t%2Fa/deployments/d%20%3F/revisions/2/knowledge/docs/import");expect(f.mock.calls[0][1]).toMatchObject({method:"POST",body:JSON.stringify({name:"note.txt",text:"hello 世界"}),credentials:"include",cache:"no-store",redirect:"error"});
});
it.each(["../a.txt","a/b","a\\b",".","x\u0000","a".repeat(256)])("rejects invalid name %s before network",async(name)=>{
 const f=vi.spyOn(globalThis,"fetch");await expect(knowledgeApi.importText(scope,name,"hello")).rejects.toThrow();expect(f).not.toHaveBeenCalled();
});
it("counts UTF8 text and full JSON bytes independently",async()=>{
 const f=vi.spyOn(globalThis,"fetch");await expect(knowledgeApi.importText(scope,"a.txt","中".repeat(KNOWLEDGE_MAX_TEXT_BYTES/3+1))).rejects.toThrow("1 MiB");await expect(knowledgeApi.importText(scope,"a.txt","\u0001".repeat(400000))).rejects.toThrow("2 MiB");expect(f).not.toHaveBeenCalled();
});
it.each(["", " ", "\n\t", "hello\0world", "\ud800"])("rejects empty, NUL or malformed Unicode text",async(text)=>{
 const f=vi.spyOn(globalThis,"fetch");await expect(knowledgeApi.importText(scope,"a.txt",text)).rejects.toThrow();expect(f).not.toHaveBeenCalled();
});
it.each([Response.json({documents:1},{status:503}),Response.json({documents:-1}),Response.json({documents:1,namespace:"unexpected"})])("never equates an error or invalid receipt to complete import",async(response)=>{
 const f=vi.spyOn(globalThis,"fetch").mockResolvedValue(response);await expect(knowledgeApi.importText(scope,"a.txt","x")).rejects.toThrow(/部分.*已写入/);expect(f).toHaveBeenCalledTimes(1);
});
it("treats a lost response as partial/uncertain, hides raw errors, and does not retry",async()=>{
 const f=vi.spyOn(globalThis,"fetch").mockRejectedValue(new TypeError("secret-internal"));await expect(knowledgeApi.importText(scope,"a.txt","x")).rejects.toThrow("不会自动重试");expect(f).toHaveBeenCalledTimes(1);
});
it("propagates caller cancellation",async()=>{
 const caller=new AbortController();let signal:AbortSignal|undefined;vi.spyOn(globalThis,"fetch").mockImplementation((_u,i)=>new Promise((_r,reject)=>{signal=i?.signal??undefined;signal?.addEventListener("abort",()=>reject(new Error("abort")));}));const pending=knowledgeApi.importText(scope,"a.txt","x",caller.signal);caller.abort();await expect(pending).rejects.toThrow(/部分.*已写入/);expect(signal?.aborted).toBe(true);
});

it("accepts the actual 255-byte public basename boundary",async()=>{
 const f=vi.spyOn(globalThis,"fetch").mockResolvedValue(Response.json({documents:1}));expect(await knowledgeApi.importText(scope,"a".repeat(255),"x")).toEqual({documents:1});expect(f).toHaveBeenCalledTimes(1);
});
