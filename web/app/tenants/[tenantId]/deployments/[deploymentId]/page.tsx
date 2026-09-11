import { DeploymentWorkspace } from "../../../../../components/deployments/workspace";
export default async function DeploymentPage({ params, searchParams }: {
  params: Promise<{ tenantId: string; deploymentId: string; }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const route = await params; const search = await searchParams;
  const query = new URLSearchParams(Object.entries(search).flatMap(([key, value]) => typeof value === "string" ? [[key, value]] : [])).toString();
  return <DeploymentWorkspace key={`${route.tenantId}/${route.deploymentId}/${query}`} tenantId={route.tenantId} deploymentId={route.deploymentId} query={query} />;
}
