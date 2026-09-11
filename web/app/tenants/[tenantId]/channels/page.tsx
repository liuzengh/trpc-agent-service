import { AccountList } from "../../../../components/channels/account-list";
import { channelPageQuery, type ChannelSearchParams } from "./target-query";

export default async function ChannelsPage({ params, searchParams }: {
  params: Promise<{ tenantId: string }>;
  searchParams: Promise<ChannelSearchParams>;
}) {
  const { tenantId } = await params;
  const query = channelPageQuery(await searchParams);
  return <AccountList key={JSON.stringify([tenantId, "list", query])} tenantId={tenantId} query={query} />;
}
