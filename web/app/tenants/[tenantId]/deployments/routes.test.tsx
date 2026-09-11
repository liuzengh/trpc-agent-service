import { describe, expect, it } from "vitest";
import List from "./page";
import New from "./new/page";
import Workspace from "./[deploymentId]/page";
import Revision from "./[deploymentId]/revisions/[revisionNumber]/page";
describe("Deployment App Router pages", () => {
  it("awaits promised route params for all four page roles", async () => {
    expect((await List({ params: Promise.resolve({ tenantId: "t" }) })).props.tenantId).toBe("t");
    const query = { profile: "p", revision: "2" };
    const initial = await New({ params: Promise.resolve({ tenantId: "t" }), searchParams: Promise.resolve(query) });
    expect(initial.props).toMatchObject({ tenantId: "t", query: "profile=p&revision=2" });
    const current = await Workspace({ params: Promise.resolve({ tenantId: "t", deploymentId: "d" }), searchParams: Promise.resolve({ from: "1" }) });
    expect(current.props).toMatchObject({ tenantId: "t", deploymentId: "d", query: "from=1" });
    expect(current.key).toContain("from=1");
    const revision = await Revision({ params: Promise.resolve({ tenantId: "t", deploymentId: "d", revisionNumber: "3" }) }); expect(revision.props).toMatchObject({ deploymentId: "d", revisionNumber: 3 });
  });
});
