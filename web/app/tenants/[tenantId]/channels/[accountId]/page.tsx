import { ChannelWorkspace } from "../../../../../components/channels/account-workspace";
import { channelPageQuery, type ChannelSearchParams } from "../target-query";

export default async function ChannelAccountPage({ params, searchParams }: {
  params: Promise<{ tenantId: string; accountId: string }>;
  searchParams: Promise<ChannelSearchParams>;
}) {
  const { tenantId, accountId } = await params;
  const search = await searchParams;
  const query = channelPageQuery(search);
  const raw = search.preflight;
  // This is a resource identifier, never an external status URL or a target selector.
  const valid = typeof raw === "string" && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(raw);
  const preflight = raw === undefined ? {} : valid ? { initialPreflightId: raw } : { invalidPreflightQuery: true };
  return <ChannelWorkspace key={JSON.stringify([tenantId, accountId, query, raw])} tenantId={tenantId} accountId={accountId} query={query} {...preflight} />;
}
