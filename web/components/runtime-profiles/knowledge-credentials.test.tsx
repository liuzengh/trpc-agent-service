import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ProfileResourceEditor } from "./profile-resource-editor";
import type { CredentialActions, ProfileConfig } from "../../lib/runtime-profile-api";
import type { RuntimeBackend } from "../../lib/runtime-backend-api";
afterEach(() => { cleanup(); vi.restoreAllMocks(); });
function Harness({ isOwner = true, readOnly = false }: { isOwner?: boolean; readOnly?: boolean }) {
 const [config,setConfig] = useState<ProfileConfig>({models:{},tools:{},storage:{},knowledge:{docs:{kind:"managed_knowledge",backend_id:"qdrant-docs",backend_revision:1,embedding:{model:"embed",base_url:"https://embed.example/v1",dimensions:3}}}});
 const [credentials,setCredentials] = useState<CredentialActions>({});
 return <><ProfileResourceEditor tenantId="t" config={config} credentials={credentials} credentialStates={{knowledge:{docs:{qdrant_api_key:{configured:true,status:"active",credential_revision:2},embedding_api_key:{configured:true,status:"active",credential_revision:3}}}}} onChange={(c,a)=>{setConfig(c);setCredentials(a);}} isOwner={isOwner} readOnly={readOnly}/><output data-testid="payload">{JSON.stringify({config,credentials})}</output></>;
}
const backend=(overrides:Partial<RuntimeBackend>={}):RuntimeBackend=>({id:"qdrant-docs",revision:1,label:"Docs",kind:"qdrant",roles:["knowledge"],available:true,...overrides});
it("edits managed Qdrant and embedding secrets independently, never placing targets or secrets in config",async()=>{
 vi.spyOn(globalThis,"fetch").mockResolvedValue(Response.json({items:[backend()]}));render(<Harness/>);
 fireEvent.click(screen.getByRole("button",{name:/^Knowledge ·/}));
 const q=await screen.findByLabelText("Qdrant API Key 操作");fireEvent.change(q,{target:{value:"replace"}});
 expect(screen.getByLabelText("Qdrant API Key 新值")).toHaveValue("");expect(screen.getByLabelText("Qdrant API Key 新值")).toHaveAttribute("type","password");
 fireEvent.change(screen.getByLabelText("Qdrant API Key 新值"),{target:{value:"test-qdrant"}});
 fireEvent.change(screen.getByLabelText("Embedding API Key 操作"),{target:{value:"replace"}});fireEvent.change(screen.getByLabelText("Embedding API Key 新值"),{target:{value:"test-embedding"}});
 let p=JSON.parse(screen.getByTestId("payload").textContent!);expect(p.credentials).toEqual({knowledge:{docs:{qdrant_api_key:{action:"replace",value:"test-qdrant"},embedding_api_key:{action:"replace",value:"test-embedding"}}}});
 expect(p.config.knowledge.docs).toEqual({kind:"managed_knowledge",backend_id:"qdrant-docs",backend_revision:1,embedding:{model:"embed",base_url:"https://embed.example/v1",dimensions:3}});expect(screen.queryByLabelText("Qdrant 主机")).toBeNull();
 fireEvent.change(q,{target:{value:"keep"}});p=JSON.parse(screen.getByTestId("payload").textContent!);expect(p.credentials.knowledge.docs.qdrant_api_key).toBeUndefined();
 fireEvent.change(q,{target:{value:"clear"}});expect(JSON.parse(screen.getByTestId("payload").textContent!).credentials.knowledge.docs.qdrant_api_key).toEqual({action:"clear"});
});
it.each([backend({revision:2}),backend({available:false}),backend({kind:"redis",roles:["memory"]})])("preserves current status when catalog does not confirm exact Qdrant revision",async(item)=>{
 vi.spyOn(globalThis,"fetch").mockResolvedValue(Response.json({items:[item]}));render(<Harness/>);fireEvent.click(screen.getByRole("button",{name:/^Knowledge ·/}));await vi.waitFor(()=>expect(screen.getByRole("button",{name:"刷新后端目录"})).toBeEnabled());
 expect(screen.getByRole("region",{name:"Qdrant API Key"})).toHaveTextContent("凭证 r2");expect(screen.queryByLabelText("Qdrant API Key 操作")).toBeNull();expect(screen.getByLabelText("Embedding API Key 操作")).toBeEnabled();
});
it("keeps immutable and MEMBER credentials read-only",async()=>{
 const fetcher=vi.spyOn(globalThis,"fetch");render(<Harness readOnly/>);fireEvent.click(screen.getByRole("button",{name:/^Knowledge ·/}));expect(screen.getByRole("region",{name:"Qdrant API Key"})).toHaveTextContent("凭证 r2");expect(screen.queryByLabelText("Qdrant API Key 操作")).toBeNull();expect(fetcher).not.toHaveBeenCalled();
 cleanup();fetcher.mockResolvedValue(Response.json({items:[backend()]}));render(<Harness isOwner={false}/>);fireEvent.click(screen.getByRole("button",{name:/^Knowledge ·/}));await screen.findByRole("option",{name:/qdrant/});expect(screen.queryByLabelText("Qdrant API Key 操作")).toBeNull();expect(screen.queryByLabelText("Embedding API Key 操作")).toBeNull();
});
