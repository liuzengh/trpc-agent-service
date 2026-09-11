export type ChannelSearchParams = Record<string, string | string[] | undefined>;

/** Keep only the target contract, but preserve duplicates so validation can reject them. */
export function channelPageQuery(search: ChannelSearchParams): string {
  const query = new URLSearchParams();
  for (const key of ["deployment_id", "revision_number"]) {
    const value = search[key];
    if (typeof value === "string") query.append(key, value);
    else if (Array.isArray(value)) {
      if (!value.length) query.append(key, "");
      else for (const item of value) query.append(key, item);
    }
  }
  return query.toString();
}
