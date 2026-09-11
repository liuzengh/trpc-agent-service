import type { ComponentProps } from "react";
import { siteUrl } from "../../lib/urls.mjs";
export function SiteLink({ href = "", ...props }: ComponentProps<"a">) {
  return <a {...props} href={siteUrl(href)} />;
}
