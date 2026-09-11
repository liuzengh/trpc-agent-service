import { afterEach, expect, it, vi } from "vitest";
import { channelApi } from "./channel-api";
import { sampleChannelAccount, sampleChannelCreate } from "../test/channel-fixtures";
import { saveChannelPending, loadChannelPending, channelPendingKey } from "./channel-editor-state";
afterEach(() => vi.restoreAllMocks());
it("preserves the selected test endpoint in create and update requests", async () => {
 const f=vi.spyOn(globalThis,"fetch").mockImplementation(async () => Response.json({account:{...sampleChannelAccount,config:{...sampleChannelAccount.config,endpoint_profile:"test"}},route_generation:0,distribution:"NOT_EMITTED"}));
 const config={receive_mode:"long_polling" as const,endpoint_profile:"test" as const};
 await channelApi.createAccount("t",{...sampleChannelCreate,config},"create");
 await channelApi.updateAccount("t","a",{expected_account_revision:3,config},"update");
 for(const [, init] of f.mock.calls) expect(JSON.parse(String(init?.body)).config).toEqual(config);
});
it("retains the endpoint when restoring an uncertain update",()=>{
 const marker={operation:"updateAccount",key:"endpoint-key",secret:false,createdAt:new Date().toISOString(),input:{expected_account_revision:3,config:{receive_mode:"long_polling",endpoint_profile:"test"}}};
 expect(saveChannelPending(window.sessionStorage,channelPendingKey("u","t","a"),marker)).toBe(true);
 expect(loadChannelPending(window.sessionStorage,channelPendingKey("u","t","a"))?.input.config).toEqual(marker.input.config);
});
