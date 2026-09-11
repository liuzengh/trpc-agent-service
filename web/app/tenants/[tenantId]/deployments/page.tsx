import { DeploymentList } from "../../../../components/deployments/list";
export default async function DeploymentsPage({ params }: { params: Promise<{ tenantId: string }> }) {
  const { tenantId } = await params;
  return <DeploymentList key={tenantId} tenantId={tenantId} />;
}
