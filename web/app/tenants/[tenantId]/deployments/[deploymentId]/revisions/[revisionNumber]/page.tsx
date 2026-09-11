import { DeploymentRevisionDetail } from "../../../../../../../components/deployments/revision";
export default async function DeploymentRevisionPage({ params }: { params: Promise<{ tenantId: string; deploymentId: string; revisionNumber: string }> }) {
  const { tenantId, deploymentId, revisionNumber } = await params;
  return <DeploymentRevisionDetail key={`${tenantId}/${deploymentId}/${revisionNumber}`} tenantId={tenantId} deploymentId={deploymentId} revisionNumber={Number(revisionNumber)} />;
}
