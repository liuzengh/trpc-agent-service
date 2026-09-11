import { AccountCreate } from "../../../../../components/channels/account-create";
import { channelPageQuery, type ChannelSearchParams } from "../target-query";

export default async function NewChannelPage({ params, searchParams }: {
  params: Promise<{ tenantId: string }>;
  searchParams: Promise<ChannelSearchParams>;
}) {
  const { tenantId } = await params;
  const query = channelPageQuery(await searchParams);
  return <AccountCreate key={JSON.stringify([tenantId, "new", query])} tenantId={tenantId} query={query} />;
}
