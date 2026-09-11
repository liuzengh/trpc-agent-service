import { redirect } from "next/navigation";

export default async function TenantRootPage({
  params,
}: {
  params: Promise<{ tenantId: string }>;
}) {
  const { tenantId } = await params;
  return redirect(`/tenants/${encodeURIComponent(tenantId)}/agents`);
}
