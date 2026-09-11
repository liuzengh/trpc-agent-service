import { BackendMigration } from "../../../../../../components/deployments/backend-migration";

export default async function BackendMigrationPage({ params, searchParams }: { params: Promise<{ tenantId: string; deploymentId: string }>; searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  const { tenantId, deploymentId } = await params;
  const search = await searchParams;
  const source = typeof search.source === "string" ? Number(search.source) : 0;
  const binding = typeof search.binding === "string" ? search.binding : "";
  return <BackendMigration tenantId={tenantId} deploymentId={deploymentId} sourceRevision={source} initialBindingId={binding} />;
}
